"""Service driver images: one digest per bind, checked when it moves (D5).

A service-plane driver runs from an image reference on its registration. Each
new binding (a credential lease issued for a service driver's connector)
resolves that reference to a digest, records the resolution in
``connector_driver_images`` and stamps the digest on the lease, which keys the
connector's shared service pod. A re-delivered lease is no new bind: it keeps
the digest it was issued with, so a moved tag starts a new pod for new
bindings while the old pod serves the existing ones.

* **Pin or follow.** A digest reference is exact; any tag is looked up
  (:mod:`shared.connectors.images`). Lookups are cached for a few seconds per
  reference and bounded by a deadline, since a bind may run inside its
  caller's transaction.
* **Registry unreachable.** A reference reuses the last digest it resolved
  to, marked stale in the log; a reference that never resolved fails the
  bind.
* **A moved tag** (a digest new for the connector) is checked against the
  image's ``io.srw.driver.spec`` label when it has one: same driver and
  protocol major, no credential slot gone, the stored config still valid. An
  incompatible image refuses the bind with a reason the connector shows.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Driver
versions".
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from orchestrator.services.connector_credential_leases import (
    LeaseDeliveryError,
    LeaseOwner,
    record_lease_event,
)
from shared.connectors.contract import PROTOCOL_VERSION, DriverSpec, protocol_major
from shared.connectors.images import (
    ImageReference,
    SpecContract,
    compatibility_problems,
    label_spec,
    refusal_message,
    spec_hash,
)

logger = logging.getLogger(__name__)

DEFAULT_CACHE_SECONDS = 60.0
DEFAULT_TIMEOUT_SECONDS = 10.0
#: The most schema errors a refusal lists.
_MAX_CONFIG_ERRORS = 3


class ServiceImageUnavailable(LeaseDeliveryError):
    """The driver's image could not be resolved and was never resolved."""


class ServiceImageRefused(LeaseDeliveryError):
    """The image behind the reference changed its contract under the tag."""


@dataclass(frozen=True)
class ServiceImageSettings:
    """Which image each service driver runs, and how it is resolved.

    ``references`` maps a driver name to its image reference (the
    registration's, D6; the development echo driver's from the chart).
    ``resolver`` is a :class:`shared.oci_registry.RegistryResolver`.
    """

    references: Mapping[str, str] = field(default_factory=dict)
    resolver: Any = None
    cache_seconds: float = DEFAULT_CACHE_SECONDS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS


@dataclass(frozen=True)
class BoundImage:
    """What one bind records: ``{reference, digest, resolved_at, spec_hash,
    protocol_version}``, plus the image's runtime fields for the pod."""

    driver: str
    reference: str
    digest: str
    resolved_at: datetime | None
    spec: Mapping[str, Any] | None
    spec_hash: str | None
    protocol_version: str
    entrypoint: tuple[str, ...] = ()
    cmd: tuple[str, ...] = ()
    stale: bool = False

    def record(self) -> dict[str, Any]:
        return {
            "reference": self.reference,
            "digest": self.digest,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "spec_hash": self.spec_hash,
            "protocol_version": self.protocol_version,
        }


#: The process's image settings, set when the application is built (as
#: ``connector_credential_leases.configure_lease_window`` does), and the
#: resolution cache: reference -> (monotonic expiry, resolved image).
_state: dict[str, Any] = {"settings": ServiceImageSettings()}
_cache: dict[tuple[str, str], tuple[float, Any]] = {}


def configure_service_images(settings: ServiceImageSettings) -> None:
    """Install the image settings and forget every cached resolution."""
    _state["settings"] = settings
    _cache.clear()


def service_image_settings() -> ServiceImageSettings:
    return _state["settings"]


def image_reference_for(driver: str) -> str | None:
    """The image reference a service driver runs, if one is configured."""
    return service_image_settings().references.get(driver)


def _json_list(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        value = json.loads(value)
    return tuple(str(item) for item in value or ())


def _json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value) if isinstance(value, Mapping) else None


def _bound_from_row(row: Mapping[str, Any], *, stale: bool) -> BoundImage:
    return BoundImage(
        driver=str(row["driver"]),
        reference=str(row["reference"]),
        digest=str(row["digest"]),
        resolved_at=row["resolved_at"],
        spec=_json_object(row["spec"]),
        spec_hash=row["spec_hash"],
        protocol_version=str(row["protocol_version"]),
        entrypoint=_json_list(row["entrypoint"]),
        cmd=_json_list(row["cmd"]),
        stale=stale,
    )


_ROW = (
    "driver, reference, digest, entrypoint, cmd, spec, spec_hash, "
    "protocol_version, resolved_at"
)


async def _record(
    conn: Any, *, driver: str, reference: str, resolved: Any, spec: Any
) -> BoundImage:
    """Upsert one resolution; returns it as recorded."""
    protocol = spec.get("protocol_version") if spec else PROTOCOL_VERSION
    row = await conn.fetchrow(
        f"""
        INSERT INTO connector_driver_images
            (driver, reference, digest, entrypoint, cmd, spec, spec_hash,
             protocol_version)
        VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7, $8)
        ON CONFLICT (driver, reference, digest) DO UPDATE
           SET resolved_at = now(),
               entrypoint = EXCLUDED.entrypoint,
               cmd = EXCLUDED.cmd,
               spec = EXCLUDED.spec,
               spec_hash = EXCLUDED.spec_hash,
               protocol_version = EXCLUDED.protocol_version
        RETURNING {_ROW}
        """,
        driver,
        reference,
        resolved.digest,
        json.dumps(list(resolved.entrypoint)),
        json.dumps(list(resolved.cmd)),
        json.dumps(spec) if spec is not None else None,
        spec_hash(spec),
        protocol,
    )
    return _bound_from_row(row, stale=False)


async def _last_resolution(
    conn: Any, *, driver: str, reference: str, digest: str | None
) -> BoundImage | None:
    if digest is not None:
        row = await conn.fetchrow(
            f"SELECT {_ROW} FROM connector_driver_images "
            "WHERE driver = $1 AND reference = $2 AND digest = $3",
            driver,
            reference,
            digest,
        )
    else:
        row = await conn.fetchrow(
            f"SELECT {_ROW} FROM connector_driver_images "
            "WHERE driver = $1 AND reference = $2 "
            "ORDER BY resolved_at DESC LIMIT 1",
            driver,
            reference,
        )
    return _bound_from_row(row, stale=True) if row is not None else None


async def resolve_driver_image(
    conn: Any,
    *,
    driver: str,
    reference: str,
    clock: Callable[[], float] = time.monotonic,
) -> BoundImage:
    """Resolve ``reference`` for ``driver`` now (or from the brief cache).

    Raises :class:`ServiceImageUnavailable` when the registry cannot answer
    and this reference never resolved before.
    """
    settings = service_image_settings()
    parsed = ImageReference.parse(reference)
    key = (driver, reference)
    cached = _cache.get(key)
    now = clock()
    if cached is not None and cached[0] > now:
        return cached[1]
    resolved = None
    if settings.resolver is None:
        problem = "no image resolver is configured"
    else:
        try:
            resolved = await asyncio.wait_for(
                settings.resolver.resolve_image(parsed.lookup()),
                timeout=settings.timeout_seconds,
            )
        except (TimeoutError, asyncio.TimeoutError):
            problem = "the registry did not answer in time"
        except Exception as exc:  # resolution, transport, protocol
            problem = str(exc) or type(exc).__name__
    if resolved is None:
        last = await _last_resolution(
            conn, driver=driver, reference=reference, digest=parsed.digest
        )
        if last is None:
            raise ServiceImageUnavailable(
                f"The image {reference} cannot be resolved ({problem})"
            )
        logger.warning(
            "Driver %s image %s did not resolve (%s); reusing digest %s "
            "resolved at %s (stale)",
            driver,
            reference,
            problem,
            last.digest,
            last.resolved_at,
        )
        return last
    try:
        spec = label_spec(resolved.labels)
        if (
            spec is not None
            and protocol_major(str(spec.get("protocol_version") or "")) is None
        ):
            raise ValueError("its protocol_version is not MAJOR.MINOR")
    except ValueError as exc:
        raise ServiceImageRefused(
            refusal_message(reference, [f"its spec label is unreadable: {exc}"])
        ) from exc
    bound = await _record(
        conn, driver=driver, reference=reference, resolved=resolved, spec=spec
    )
    _cache[key] = (now + max(0.0, settings.cache_seconds), bound)
    return bound


async def image_for_digest(conn: Any, *, driver: str, digest: str) -> BoundImage | None:
    """The newest recorded resolution of ``digest`` for ``driver``."""
    row = await conn.fetchrow(
        f"SELECT {_ROW} FROM connector_driver_images "
        "WHERE driver = $1 AND digest = $2 ORDER BY resolved_at DESC LIMIT 1",
        driver,
        digest,
    )
    return _bound_from_row(row, stale=False) if row is not None else None


async def ensure_image(
    conn: Any, *, driver: str, reference: str, digest: str
) -> BoundImage:
    """The recorded image of ``digest``, resolving it by digest if no row
    holds it (a bind whose transaction rolled back recorded nothing)."""
    image = await image_for_digest(conn, driver=driver, digest=digest)
    if image is not None:
        return image
    settings = service_image_settings()
    if settings.resolver is None:
        raise ServiceImageUnavailable("no image resolver is configured")
    pinned = ImageReference.parse(reference).at(digest)
    try:
        resolved = await asyncio.wait_for(
            settings.resolver.resolve_image(pinned),
            timeout=settings.timeout_seconds,
        )
        spec = label_spec(resolved.labels)
    except Exception as exc:
        raise ServiceImageUnavailable(
            f"The image {pinned} cannot be resolved ({exc or type(exc).__name__})"
        ) from exc
    if resolved.digest != digest:
        raise ServiceImageUnavailable(
            f"The registry answered {pinned} with another digest"
        )
    return await _record(
        conn, driver=driver, reference=reference, resolved=resolved, spec=spec
    )


def config_errors(schema: Mapping[str, Any], config: Any) -> list[str]:
    """The stored config's errors against an image's config schema."""
    from jsonschema import Draft202012Validator
    from jsonschema.exceptions import SchemaError

    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        return [f"the image's config schema is invalid ({exc.message})"]
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(config), key=lambda e: list(e.path))
    return [
        (f"/{'/'.join(str(p) for p in error.path)}: " if error.path else "")
        + error.message
        for error in errors[:_MAX_CONFIG_ERRORS]
    ]


def _contract(spec: DriverSpec, label: Mapping[str, Any] | None) -> SpecContract:
    return SpecContract.of_label(label) if label else SpecContract.of_driver(spec)


async def _previous_digest(conn: Any, connector_id: UUID) -> str | None:
    """The digest the connector's newest binding was issued with."""
    return await conn.fetchval(
        """
        SELECT image_digest FROM connector_credential_leases
         WHERE connector_id = $1 AND image_digest IS NOT NULL
         ORDER BY issued_at DESC LIMIT 1
        """,
        connector_id,
    )


async def _stored_config(conn: Any, connector_id: UUID) -> dict[str, Any]:
    raw = await conn.fetchval(
        "SELECT config FROM datasources WHERE id = $1", connector_id
    )
    return _json_object(raw) or {}


async def check_moved_image(
    conn: Any,
    *,
    spec: DriverSpec,
    connector_id: str,
    image: BoundImage,
) -> list[str]:
    """Why ``image`` cannot serve the connector, when its digest is new to it.

    Empty when the digest is the one the connector's newest binding used, or
    when the new image keeps the contract. The previous contract is the
    previous image's label spec, else the installed spec; an image without a
    label keeps the installed spec, so it is compatible by definition.
    """
    connector_uuid = UUID(str(connector_id))
    previous_digest = await _previous_digest(conn, connector_uuid)
    if previous_digest == image.digest or image.spec is None:
        return []
    previous_label = None
    if previous_digest is not None:
        previous = await image_for_digest(
            conn, driver=spec.name, digest=previous_digest
        )
        previous_label = previous.spec if previous is not None else None
    try:
        previous_contract = _contract(spec, previous_label)
    except ValueError:
        previous_contract = SpecContract.of_driver(spec)
    try:
        new_contract = SpecContract.of_label(image.spec)
    except ValueError as exc:
        return [f"its spec label is malformed ({exc})"]
    config = await _stored_config(conn, connector_uuid)
    return compatibility_problems(
        previous_contract,
        new_contract,
        config_errors=config_errors(new_contract.config_schema, config),
    )


async def bind_service_image(
    conn: Any,
    *,
    spec: DriverSpec,
    connector_id: str,
    owner: LeaseOwner,
) -> str:
    """The digest a service driver's binding of ``owner`` runs on.

    A live lease already holds one (re-delivery is no new bind). Otherwise the
    driver's reference is resolved and, when the digest is new for the
    connector, checked; an incompatible image refuses the bind
    (:class:`ServiceImageRefused`) and is audited.
    """
    reference = image_reference_for(spec.name)
    if not reference:
        raise ServiceImageUnavailable(
            f"No image is configured for the service driver {spec.name}"
        )
    connector_uuid = UUID(str(connector_id))
    held = await conn.fetchval(
        f"""
        SELECT image_digest FROM connector_credential_leases
         WHERE {owner.column} = $1 AND connector_id = $2
           AND revoked_at IS NULL AND expires_at > now()
           AND image_digest IS NOT NULL
        """,
        UUID(owner.id),
        connector_uuid,
    )
    if held:
        return str(held)
    image = await resolve_driver_image(conn, driver=spec.name, reference=reference)
    problems = await check_moved_image(
        conn, spec=spec, connector_id=connector_id, image=image
    )
    if problems:
        message = refusal_message(reference, problems)
        await record_lease_event(
            conn,
            event_type="connector_driver_image_refused",
            resource_type="connector",
            resource_id=str(connector_uuid),
            detail=(
                f"driver={spec.name} reference={reference} digest={image.digest} "
                f"owner={owner.kind}:{owner.id} problems={'; '.join(problems)}"
            ),
        )
        raise ServiceImageRefused(message)
    return image.digest


__all__ = [
    "BoundImage",
    "ServiceImageRefused",
    "ServiceImageSettings",
    "ServiceImageUnavailable",
    "bind_service_image",
    "check_moved_image",
    "config_errors",
    "configure_service_images",
    "image_for_digest",
    "image_reference_for",
    "resolve_driver_image",
    "service_image_settings",
]
