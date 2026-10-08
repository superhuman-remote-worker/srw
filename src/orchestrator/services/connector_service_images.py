"""Service driver images: one digest per bind, checked when it moves (D5).

A service-plane driver runs from an image reference on its registration. Each
new binding (a credential lease issued for a service driver's connector)
resolves that reference to a digest, records the resolution in
``connector_driver_images`` and stamps the digest on the lease, which keys the
connector's shared service pod. A re-delivered lease is no new bind: it keeps
the digest it was issued with, so a moved tag starts a new pod for new
bindings while the old pod serves the existing ones.

* **Pin or follow.** A digest reference is exact; any tag is looked up
  (:mod:`shared.connectors.images`). A lookup, failed or not, is reused for
  the cache window.
* **Never inside a caller's transaction.** A bind runs inside its caller's
  transaction and under its locks (the stateless claim, the thread
  datasource lock, pinned dispatch). Callers that can call
  :func:`prepare_service_images` before they open it: the registry lookup,
  the image row and a refusal audit happen there, on the store's own
  connections, and the bind applies the remembered decision. A bind with no
  decision makes one on the store's own connections too, capped at
  ``bind_timeout_seconds``; it only reads on the caller's connection, so
  two claims never wait on each other's image rows.
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
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeVar
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
#: The longest a bind waits on a registry it has no answer for: a bind may
#: run inside its caller's transaction and under its locks.
DEFAULT_BIND_TIMEOUT_SECONDS = 2.0
#: Of a bind's cap, what the registry lookup leaves for the fallback to the
#: last resolved digest (a read) and the moved-tag check: at most this much,
#: at most half the cap.
BIND_FALLBACK_SECONDS = 0.5
#: How long a lookup that failed under a bind's short cap is remembered. A
#: full-deadline failure is remembered for the cache window; a capped one
#: only spares the next binds the same wait.
BRIEF_FAILURE_SECONDS = 5.0
#: The most schema errors a refusal lists.
_MAX_CONFIG_ERRORS = 3

T = TypeVar("T")


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
    ``store`` is the application's database: image rows and refusal audits
    are written on its connections, in their own short transactions, never in
    a caller's. ``bind_timeout_seconds`` caps a bind that finds no answer
    prepared before its transaction. ``service_namespace`` is where service
    pods and their endpoint Services run: a managed MCP binding carries its
    endpoint's URL there (empty while hosting is off).
    ``service_start_seconds`` is how long a new binding's pod may take to
    serve (the reconciler's interval plus its start timeout): a git swap
    binding's first clone waits that long for its driver.
    """

    references: Mapping[str, str] = field(default_factory=dict)
    resolver: Any = None
    cache_seconds: float = DEFAULT_CACHE_SECONDS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    bind_timeout_seconds: float = DEFAULT_BIND_TIMEOUT_SECONDS
    store: Any = None
    service_namespace: str = ""
    service_start_seconds: float = 210.0


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


@dataclass(frozen=True)
class _Failure:
    """A reference that did not resolve, remembered for the cache window."""


@dataclass(frozen=True)
class _Decision:
    """What a bind of one connector gets: a digest, or why not."""

    digest: str | None = None
    refusal: str | None = None
    unavailable: str | None = None


#: The process's image settings, set when the application is built (as
#: ``connector_credential_leases.configure_lease_window`` does); the
#: resolution cache, (driver, reference) -> (monotonic expiry, image or
#: failure); and the bind decisions, (driver, reference, connector) ->
#: (monotonic expiry, decision).
_state: dict[str, Any] = {"settings": ServiceImageSettings()}
_cache: dict[tuple[str, str], tuple[float, Any]] = {}
_decisions: dict[tuple[str, str, str], tuple[float, _Decision]] = {}


def configure_service_images(settings: ServiceImageSettings) -> None:
    """Install the image settings and forget every cached answer."""
    _state["settings"] = settings
    _cache.clear()
    _decisions.clear()


def service_image_settings() -> ServiceImageSettings:
    return _state["settings"]


def image_reference_for(driver: str) -> str | None:
    """The image reference a service driver runs, if one is configured."""
    return service_image_settings().references.get(driver)


def _unavailable(driver: str) -> str:
    # Generic on purpose: the registry's own answer can describe internal
    # services, and this message reaches the connector's readers.
    return f"The image of the service driver {driver} cannot be resolved"


async def _apart(work: Callable[[], Awaitable[T]], *, timeout: float) -> T:
    """Run ``work`` in a task of its own, bounded by ``timeout``.

    A child task never shares its parent's ``transaction_scope`` connection,
    so every store connection ``work`` acquires is a new one: what it writes
    commits on its own, and it holds no lock of the caller's transaction.
    """
    return await asyncio.wait_for(asyncio.create_task(work()), timeout)


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
    store: Any,
    *,
    driver: str,
    reference: str,
    timeout: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> BoundImage:
    """Resolve ``reference`` for ``driver`` now (or from the brief cache).

    The registry is asked at most once per cache window, a failure included;
    the resolution is recorded on a connection of ``store``. Raises
    :class:`ServiceImageUnavailable` when the registry cannot answer and this
    reference never resolved before. Call it outside any transaction, or
    through :func:`_apart`.
    """
    settings = service_image_settings()
    parsed = ImageReference.parse(reference)
    key = (driver, reference)
    window = max(0.0, settings.cache_seconds)
    deadline = timeout or settings.timeout_seconds
    capped = deadline < settings.timeout_seconds
    cached = _cache.get(key)
    now = clock()
    if cached is not None and cached[0] > now:
        if isinstance(cached[1], _Failure):
            raise ServiceImageUnavailable(_unavailable(driver))
        return cached[1]
    resolved = None
    if settings.resolver is None:
        problem = "no image resolver is configured"
    else:
        try:
            resolved = await asyncio.wait_for(
                settings.resolver.resolve_image(parsed.lookup()),
                timeout=deadline,
            )
        except (TimeoutError, asyncio.TimeoutError):
            problem = "the registry did not answer in time"
        except Exception as exc:  # resolution, transport, protocol
            problem = str(exc) or type(exc).__name__
    if resolved is None:
        async with store.acquire() as conn:
            last = await _last_resolution(
                conn, driver=driver, reference=reference, digest=parsed.digest
            )
        logger.warning(
            "Driver %s image %s did not resolve (%s)%s",
            driver,
            reference,
            problem,
            f"; reusing digest {last.digest} resolved at {last.resolved_at} (stale)"
            if last is not None
            else "",
        )
        if last is None:
            # A lookup cut short by a bind's cap proves less than one that
            # had its whole deadline: remember it only briefly.
            failure_window = min(window, BRIEF_FAILURE_SECONDS) if capped else window
            _cache[key] = (now + failure_window, _Failure())
            raise ServiceImageUnavailable(_unavailable(driver))
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
    async with store.acquire() as conn:
        bound = await _record(
            conn, driver=driver, reference=reference, resolved=resolved, spec=spec
        )
    _cache[key] = (now + window, bound)
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
    holds it. For the reconciler, which holds no caller's transaction."""
    image = await image_for_digest(conn, driver=driver, digest=digest)
    if image is not None:
        return image
    settings = service_image_settings()
    if settings.resolver is None:
        raise ServiceImageUnavailable(_unavailable(driver))
    pinned = ImageReference.parse(reference).at(digest)
    try:
        resolved = await asyncio.wait_for(
            settings.resolver.resolve_image(pinned),
            timeout=settings.timeout_seconds,
        )
        spec = label_spec(resolved.labels)
    except Exception as exc:
        logger.warning("Driver %s image %s did not resolve: %s", driver, pinned, exc)
        raise ServiceImageUnavailable(_unavailable(driver)) from exc
    if resolved.digest != digest:
        logger.warning("The registry answered %s with another digest", pinned)
        raise ServiceImageUnavailable(_unavailable(driver))
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
    previous_digest = await _previous_digest(conn, UUID(str(connector_id)))
    return await moved_image_problems(
        conn,
        spec=spec,
        connector_id=connector_id,
        image=image,
        previous_digest=previous_digest,
    )


async def moved_image_problems(
    conn: Any,
    *,
    spec: DriverSpec,
    connector_id: str,
    image: BoundImage,
    previous_digest: str | None,
) -> list[str]:
    """:func:`check_moved_image` against a known previous digest (a
    bind-time binding's, D6, rather than a lease's)."""
    connector_uuid = UUID(str(connector_id))
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


_HELD = """
SELECT image_digest FROM connector_credential_leases
 WHERE {column} = $1 AND connector_id = $2
   AND revoked_at IS NULL AND expires_at > now()
   AND image_digest IS NOT NULL
"""


async def _held_digest(conn: Any, owner: LeaseOwner, connector_id: str) -> str | None:
    """The digest a live lease of ``owner`` already binds the connector at."""
    held = await conn.fetchval(
        _HELD.format(column=owner.column), UUID(owner.id), UUID(str(connector_id))
    )
    return str(held) if held else None


async def _decide(
    store: Any,
    *,
    spec: DriverSpec,
    reference: str,
    connector_id: str,
    owner: LeaseOwner,
    timeout: float,
) -> _Decision:
    """Resolve, check and audit one bind on ``store``'s own connections."""
    try:
        image = await resolve_driver_image(
            store, driver=spec.name, reference=reference, timeout=timeout
        )
    except ServiceImageRefused as exc:
        return _Decision(refusal=str(exc))
    except ServiceImageUnavailable as exc:
        return _Decision(unavailable=str(exc))
    async with store.acquire() as conn:
        problems = await check_moved_image(
            conn, spec=spec, connector_id=connector_id, image=image
        )
        if not problems:
            return _Decision(digest=image.digest)
        # Audited on this connection, so the record survives a caller that
        # rolls back after the refusal.
        await record_lease_event(
            conn,
            event_type="connector_driver_image_refused",
            resource_type="connector",
            resource_id=str(UUID(str(connector_id))),
            detail=(
                f"driver={spec.name} reference={reference} digest={image.digest} "
                f"owner={owner.kind}:{owner.id} problems={'; '.join(problems)}"
            ),
        )
    return _Decision(refusal=refusal_message(reference, problems))


def _remember(
    key: tuple[str, str, str],
    decision: _Decision,
    *,
    window: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    full = max(0.0, service_image_settings().cache_seconds)
    held = full if window is None else min(full, window)
    _decisions[key] = (clock() + held, decision)


def _recalled(
    key: tuple[str, str, str], *, clock: Callable[[], float] = time.monotonic
) -> _Decision | None:
    found = _decisions.get(key)
    return found[1] if found is not None and found[0] > clock() else None


def _service_lease_entries(entries: Any) -> list[tuple[DriverSpec, str]]:
    """``(spec, connector id)`` of every service-driver lease entry."""
    from orchestrator.services.connector_credential_leases import lease_spec

    found = []
    for entry in entries or ():
        spec = lease_spec(entry) if isinstance(entry, Mapping) else None
        if spec is None or spec.plane != "service":
            continue
        connector_id = str(entry.get("datasource_id") or "")
        try:
            UUID(connector_id)
        except ValueError:
            continue
        found.append((spec, connector_id))
    return found


async def prepare_service_images(
    entries: Any, *, owner: LeaseOwner, store: Any
) -> None:
    """Decide the image of every new service-driver binding in ``entries``,
    before the caller opens its transaction.

    The registry lookup (with its full deadline), the image row and a refusal
    audit happen here, each on ``store``'s own connections; the decision is
    remembered for the cache window, so the bind inside the caller's
    transaction reads it and does no network or write of its own. A binding a
    live lease already holds needs no decision. Never raises: the bind
    applies what was decided.
    """
    settings = service_image_settings()
    for spec, connector_id in _service_lease_entries(entries):
        reference = image_reference_for(spec.name)
        if not reference:
            continue
        try:
            async with store.acquire() as conn:
                if await _held_digest(conn, owner, connector_id):
                    continue
            decision = await _apart(
                lambda spec=spec, reference=reference, connector_id=connector_id: (
                    _decide(
                        store,
                        spec=spec,
                        reference=reference,
                        connector_id=connector_id,
                        owner=owner,
                        timeout=settings.timeout_seconds,
                    )
                ),
                timeout=settings.timeout_seconds + 5,
            )
        except (TimeoutError, asyncio.TimeoutError):
            decision = _Decision(unavailable=_unavailable(spec.name))
        except Exception:
            # The bind decides again under its own cap.
            logger.warning(
                "Preparing the image of %s for connector %s failed",
                spec.name,
                connector_id,
                exc_info=True,
            )
            continue
        _remember((spec.name, reference, connector_id), decision)


async def bind_service_image(
    conn: Any,
    *,
    spec: DriverSpec,
    connector_id: str,
    owner: LeaseOwner,
) -> str:
    """The digest a service driver's binding of ``owner`` runs on.

    A live lease already holds one (re-delivery is no new bind). Otherwise the
    decision :func:`prepare_service_images` made before the caller's
    transaction applies. Without one, the bind decides now on the store's own
    connections, capped at ``bind_timeout_seconds``; ``conn`` is only read.
    An incompatible image refuses the bind (:class:`ServiceImageRefused`,
    audited); an image that cannot be resolved fails it
    (:class:`ServiceImageUnavailable`). Both are remembered for the cache
    window.
    """
    settings = service_image_settings()
    reference = image_reference_for(spec.name)
    if not reference:
        raise ServiceImageUnavailable(
            f"No image is configured for the service driver {spec.name}"
        )
    held = await _held_digest(conn, owner, connector_id)
    if held:
        return held
    key = (spec.name, reference, str(connector_id))
    decision = _recalled(key)
    if decision is None:
        if settings.store is None:
            raise ServiceImageUnavailable(_unavailable(spec.name))
        cap = settings.bind_timeout_seconds
        # The lookup gets less than the cap, so a registry that hangs still
        # leaves time to fall back to the last digest this reference resolved.
        lookup = cap - min(BIND_FALLBACK_SECONDS, cap / 2)
        try:
            decision = await _apart(
                lambda: _decide(
                    settings.store,
                    spec=spec,
                    reference=reference,
                    connector_id=connector_id,
                    owner=owner,
                    timeout=lookup,
                ),
                timeout=cap,
            )
        except (TimeoutError, asyncio.TimeoutError):
            decision = _Decision(unavailable=_unavailable(spec.name))
        if decision.unavailable is None:
            _remember(key, decision)
        else:
            # Decided under the bind's short cap: spare the next binds the
            # same wait, briefly; a prepared decision is remembered in full.
            _remember(key, decision, window=BRIEF_FAILURE_SECONDS)
    if decision.refusal is not None:
        raise ServiceImageRefused(decision.refusal)
    if decision.digest is None:
        raise ServiceImageUnavailable(decision.unavailable or _unavailable(spec.name))
    return decision.digest


__all__ = [
    "BoundImage",
    "ServiceImageRefused",
    "ServiceImageSettings",
    "ServiceImageUnavailable",
    "bind_service_image",
    "check_moved_image",
    "config_errors",
    "configure_service_images",
    "ensure_image",
    "image_for_digest",
    "image_reference_for",
    "moved_image_problems",
    "prepare_service_images",
    "resolve_driver_image",
    "service_image_settings",
]
