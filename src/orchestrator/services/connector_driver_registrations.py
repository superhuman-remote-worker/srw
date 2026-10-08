"""Registering connector driver images (connector drivers D6).

Any image may be a driver image (decision 3, the workspace-template image
policy): the control is privilege, never admission.

**Who registers where** follows the manifest scopes
(``ManifestAuthority.scope(write=True)``, decision 5): the caller in their own
Account, editors and up in a Project, unrestricted administrators in the
shared Catalog. The same authority disables, enables and deletes. Reading
follows the scopes too: an Account's registrations are its user's, a
Project's are its members', the Catalog's are everyone's. An administrator
may also read any Account's registration *by id* (the list never shows
another user's): support needs to see what a connector of that user runs,
and a manifest scope's administrator reads an Account's resources the same
way.

**The spec** comes from the image's ``io.srw.driver.spec`` label, read from
the registry without running the image. An image without one has its
``spec`` operation run in a driver pod with no secret and no egress but the
result route (``connector_bind_time.run_spec_operation``), when the
installation runs driver pods. The spec must satisfy
``shared.connectors.registration.custom_driver_problems``; the in-pod plane
needs a trusted repository (``connectors.drivers.trustedRepositories``, a path
boundary match) or ``connectors.customDrivers.privileged``.

**Names.** ``srw.*`` is SRW's own. One name per scope. A registration in an
Account or a Project may not reuse a name the shared Catalog has, so nobody
shadows what administrators curated. An administrator may still register a
Catalog name that Accounts or Projects already use.

**Which registration a connector runs.** A connector pins its registration
by id when it is created (``connector_driver_assignments``), so a
registration added later under the same name never moves an existing
connector to another image. A connector created by driver *name* takes the
Catalog's registration of that name; without one, the name must resolve to
exactly one registration in the Project it is created for and the creator's
Account, else the request is refused as ambiguous and names the ids to
pick from.

**Disable and delete.** A disabled registration binds nothing new: its
connectors show "registration disabled", and its live bindings are revoked
(``connector_bind_time.registration_disabled``). It is deleted only when
none of its bindings is unrevoked (a revoke needs the binding's own
inputs, not the registration) and no connector uses it, or it is disabled.
Delete and connector creation lock the registration's row, so neither can
slip past the other.

**Versions.** The registration keeps its image reference as written; SRW
never rewrites it. Each bind resolves it to a digest and checks a moved tag
against the stored connector (``connector_bind_time``).

Design: knowledge-base/knowledge/features/connector_drivers.md, "Trust and
registration", "Driver and connector" and slice D6.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import HTTPException

from orchestrator.security.access import (
    _role_satisfies,
    log_security_event,
    mcp_scope_project_id,
)
from orchestrator.services.connector_drivers.registered import (
    CheckRunner,
    SupportsDriverRegistration,
)
from orchestrator.services.manifest_authority import ManifestAuthority
from shared.connectors.contract import DriverSpec
from shared.connectors.images import ImageReference, label_spec, spec_hash
from shared.connectors.registration import (
    custom_driver_problems,
    declared_env_names,
    repository_trusted,
    reserved_name,
    spec_from_json,
)

logger = logging.getLogger(__name__)

SCOPE_KINDS = ("Account", "Project", "Catalog")
CATALOG = "shared"
_NAME_LOCK = "srw-connector-driver-name:"
_ROW = (
    "id, name, scope_kind, owner_id, project_id, title, description, "
    "image_reference, image_digest, spec, spec_hash, spec_source, "
    "protocol_version, plane, source_document, created_by, created_at, "
    "updated_at, disabled_at, disabled_by"
)
#: Resolves one image reference for registration: the registry's answer
#: (digest, labels, entrypoint, command), ``shared.oci_registry.ResolvedImage``.
ResolveImage = Callable[[str], Awaitable[Any]]
#: Runs an unlabelled image's ``spec`` operation in a sandboxed pod:
#: ``(reference, resolved image) -> the spec JSON``.
RunSpec = Callable[..., Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class DriverTrustPolicy:
    """The operator's trust in driver images (``connectors.drivers`` and
    ``connectors.customDrivers`` in the chart)."""

    trusted_repositories: tuple[str, ...] = ()
    custom_drivers_privileged: bool = False

    def trusted(self, reference: str) -> bool:
        return repository_trusted(reference, self.trusted_repositories)

    def privileged(self, reference: str) -> bool:
        """Whether the image may have privilege (the in-pod plane)."""
        return self.trusted(reference) or self.custom_drivers_privileged

    def trust(self, reference: str) -> dict[str, Any]:
        """The matrix's trust block for a registered image."""
        trusted = self.trusted(reference)
        return {
            "tier": "trusted" if trusted else "custom",
            "trusted": trusted,
            "privileged": self.privileged(reference),
            "image": reference,
            # SRW does not verify foreign images, as for workspace images.
            "claims_declared_by_author": not trusted,
        }


@dataclass(frozen=True)
class Registration:
    """One registered driver image."""

    id: str
    name: str
    scope_kind: str
    owner_id: str | None
    project_id: str | None
    title: str
    description: str | None
    image_reference: str
    image_digest: str
    spec_json: Mapping[str, Any] = field(repr=False)
    spec: DriverSpec = field(repr=False)
    spec_hash: str
    spec_source: str
    protocol_version: str
    plane: str
    created_by: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    source_document: Mapping[str, Any] | None = field(default=None, repr=False)
    disabled_at: datetime | None = None
    disabled_by: str | None = None

    @property
    def disabled(self) -> bool:
        return self.disabled_at is not None

    @property
    def env_names(self) -> tuple[str, ...]:
        """The variable names its spec declares a bind may return."""
        return declared_env_names(self.spec_json)

    @property
    def scope(self) -> dict[str, str]:
        name = {
            "Account": self.owner_id,
            "Project": self.project_id,
            "Catalog": CATALOG,
        }[self.scope_kind]
        return {"kind": self.scope_kind, "name": str(name)}

    def view(self, policy: DriverTrustPolicy) -> dict[str, Any]:
        """The API's answer: never a secret (a spec holds none)."""
        return {
            "id": self.id,
            "name": self.name,
            "title": self.title,
            "description": self.description,
            "scope": self.scope,
            "image_reference": self.image_reference,
            "image_digest": self.image_digest,
            "spec_source": self.spec_source,
            "spec_hash": self.spec_hash,
            "protocol_version": self.protocol_version,
            "plane": self.plane,
            "env_names": list(self.env_names),
            "trust": policy.trust(self.image_reference),
            "disabled": self.disabled,
            "disabled_at": self.disabled_at.isoformat() if self.disabled_at else None,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def registration_from_row(row: Mapping[str, Any]) -> Registration:
    """A stored row as a registration; ``ValueError`` if its spec is unreadable."""
    spec_json = _json(row["spec"]) or {}
    return Registration(
        id=str(row["id"]),
        name=str(row["name"]),
        scope_kind=str(row["scope_kind"]),
        owner_id=str(row["owner_id"]) if row["owner_id"] else None,
        project_id=str(row["project_id"]) if row["project_id"] else None,
        title=str(row["title"]),
        description=row["description"],
        image_reference=str(row["image_reference"]),
        image_digest=str(row["image_digest"]),
        spec_json=spec_json,
        spec=spec_from_json(spec_json),
        spec_hash=str(row["spec_hash"]),
        spec_source=str(row["spec_source"]),
        protocol_version=str(row["protocol_version"]),
        plane=str(row["plane"]),
        created_by=str(row["created_by"]) if row["created_by"] else None,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        source_document=_json(row["source_document"]),
        disabled_at=row["disabled_at"],
        disabled_by=str(row["disabled_by"]) if row["disabled_by"] else None,
    )


def _readable(rows: Iterable[Mapping[str, Any]]) -> list[Registration]:
    found = []
    for row in rows:
        try:
            found.append(registration_from_row(row))
        except ValueError as exc:
            logger.error(
                "Driver registration %s has an unreadable spec: %s", row["id"], exc
            )
    return found


# =============================================================================
# Visibility
# =============================================================================


async def _member_project_ids(db: Any, user: Mapping[str, Any]) -> list[str]:
    """The projects whose registrations the caller may read: every member
    project, or only its token's project."""
    token_project = mcp_scope_project_id(dict(user))
    projects = await db.get_projects_for_user(str(user["id"]), limit=1000)
    ids = [str(project["id"]) for project in projects]
    if token_project is not None:
        return [pid for pid in ids if pid == str(token_project)]
    return ids


async def list_visible_registrations(
    db: Any, user: Mapping[str, Any]
) -> list[Registration]:
    """The Catalog's registrations, the caller's Account's and their
    Projects'. A project-scoped token sees its project's and the Catalog's."""
    project_ids = await _member_project_ids(db, user)
    account = None if mcp_scope_project_id(dict(user)) else UUID(str(user["id"]))
    rows = await db.fetch(
        f"""
        SELECT {_ROW} FROM connector_driver_registrations
         WHERE scope_kind = 'Catalog'
            OR (scope_kind = 'Account' AND owner_id = $1)
            OR (scope_kind = 'Project' AND project_id = ANY($2::uuid[]))
         ORDER BY name, scope_kind, created_at
        """,
        account,
        [UUID(pid) for pid in project_ids],
    )
    return _readable(rows)


async def _can_read(
    db: Any, user: Mapping[str, Any], registration: Registration
) -> bool:
    if registration.scope_kind == "Catalog":
        return True
    token_project = mcp_scope_project_id(dict(user))
    if registration.scope_kind == "Account":
        # An administrator reads another user's by id on purpose (the
        # module docstring); a project-scoped token reads no Account's.
        return token_project is None and (
            registration.owner_id == str(user["id"]) or bool(user.get("is_admin"))
        )
    if token_project is not None and str(token_project) != registration.project_id:
        return False
    if user.get("is_admin"):
        return True
    role = await db.get_user_role_in_project(registration.project_id, str(user["id"]))
    return _role_satisfies(role, "viewer")


async def _load(db: Any, registration_id: Any) -> Registration | None:
    try:
        uid = UUID(str(registration_id))
    except ValueError:
        return None
    row = await db.fetchrow(
        f"SELECT {_ROW} FROM connector_driver_registrations WHERE id = $1", uid
    )
    if row is None:
        return None
    found = _readable([row])
    return found[0] if found else None


def _not_found() -> HTTPException:
    # The same answer whether it does not exist or the caller may not see it.
    return HTTPException(status_code=404, detail="Driver registration not found")


async def get_visible_registration(
    db: Any, user: Mapping[str, Any], registration_id: Any
) -> Registration:
    registration = await _load(db, registration_id)
    if registration is None or not await _can_read(db, user, registration):
        raise _not_found()
    return registration


# =============================================================================
# Registration
# =============================================================================


def _problems_detail(problems: list[str]) -> str:
    return "The driver image's spec is refused: " + "; ".join(problems[:5])


async def _spec_of_image(
    reference: str,
    resolved: Any,
    *,
    run_spec: RunSpec | None,
    requested_by: str,
) -> tuple[dict[str, Any], str]:
    """The spec JSON an image declares and where it came from."""
    try:
        labelled = label_spec(getattr(resolved, "labels", None))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    if labelled is not None:
        return labelled, "label"
    if run_spec is None:
        raise HTTPException(
            status_code=422,
            detail=(
                "The image has no io.srw.driver.spec label, and this installation "
                "runs no driver pod to ask it for its spec "
                "(connectors.servicePods.enabled)"
            ),
        )
    return (
        await run_spec(reference, resolved, requested_by=requested_by),
        "spec_operation",
    )


async def register_driver(
    db: Any,
    user: Mapping[str, Any],
    *,
    scope: Mapping[str, Any] | None,
    image_reference: str,
    policy: DriverTrustPolicy,
    resolve_image: ResolveImage,
    run_spec: RunSpec | None = None,
    name: str | None = None,
    title: str | None = None,
    description: str | None = None,
    spec_json: Mapping[str, Any] | None = None,
    spec_source: str | None = None,
    source_document: Mapping[str, Any] | None = None,
    request: Any = None,
) -> Registration:
    """Register ``image_reference`` in ``scope`` (the caller's Account when
    omitted). ``spec_json`` replaces the image's own spec (a ``server.json``
    import); ``name``, when given, must be the name the spec declares."""
    authority = ManifestAuthority(db, dict(user), request=request)
    scope = await authority.scope(dict(scope) if scope else None, write=True)
    try:
        reference = str(ImageReference.parse(image_reference))
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"Invalid image reference: {exc}"
        ) from None
    try:
        resolved = await resolve_image(ImageReference.parse(reference).lookup())
    except HTTPException:
        raise
    except Exception as exc:
        # The registry's own words can describe internal services: log them.
        logger.warning("Driver image %s did not resolve: %s", reference, exc)
        raise HTTPException(
            status_code=422,
            detail=f"The image {reference} could not be resolved in its registry",
        ) from None
    if spec_json is None:
        spec_json, spec_source = await _spec_of_image(
            reference, resolved, run_spec=run_spec, requested_by=str(user["id"])
        )
    try:
        spec = spec_from_json(spec_json)
        env_names = declared_env_names(spec_json)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=_problems_detail([str(exc)])
        ) from None
    problems = custom_driver_problems(
        spec, privileged=policy.privileged(reference), env_names=env_names
    )
    if problems:
        raise HTTPException(status_code=422, detail=_problems_detail(problems))
    if name is not None and name != spec.name:
        raise HTTPException(
            status_code=422,
            detail=f"The image declares the driver {spec.name}, not {name}",
        )
    kind = scope["kind"]
    owner_id = UUID(scope["name"]) if kind == "Account" else None
    project_id = UUID(scope["name"]) if kind == "Project" else None
    async with db.transaction_scope():
        await db.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            _NAME_LOCK + spec.name,
        )
        if kind != "Catalog" and await db.fetchval(
            "SELECT 1 FROM connector_driver_registrations "
            "WHERE scope_kind = 'Catalog' AND name = $1",
            spec.name,
        ):
            raise HTTPException(
                status_code=409,
                detail=(
                    f"The shared Catalog has a driver named {spec.name}; a "
                    "registration in an Account or Project cannot shadow it"
                ),
            )
        taken = await db.fetchval(
            """
            SELECT 1 FROM connector_driver_registrations
             WHERE name = $1 AND scope_kind = $2
               AND owner_id IS NOT DISTINCT FROM $3
               AND project_id IS NOT DISTINCT FROM $4
            """,
            spec.name,
            kind,
            owner_id,
            project_id,
        )
        if taken:
            raise HTTPException(
                status_code=409,
                detail=f"{spec.name} is already registered in this scope",
            )
        row = await db.fetchrow(
            f"""
            INSERT INTO connector_driver_registrations
                (name, scope_kind, owner_id, project_id, title, description,
                 image_reference, image_digest, spec, spec_hash, spec_source,
                 protocol_version, plane, source_document, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12,
                    $13, $14::jsonb, $15)
            RETURNING {_ROW}
            """,
            spec.name,
            kind,
            owner_id,
            project_id,
            (title or spec.title)[:200],
            description,
            reference,
            resolved.digest,
            json.dumps(spec_json),
            spec_hash(spec_json),
            spec_source,
            spec.protocol_version,
            spec.plane,
            json.dumps(source_document) if source_document is not None else None,
            UUID(str(user["id"])),
        )
    registration = registration_from_row(row)
    await log_security_event(
        db,
        resource_type="connector_driver",
        event_type="connector_driver_registered",
        user=dict(user),
        resource_id=registration.id,
        detail=(
            f"name={registration.name} scope={kind}:{scope['name']} "
            f"image={reference} digest={resolved.digest} source={spec_source} "
            f"tier={policy.trust(reference)['tier']}"
        ),
        request=request,
    )
    return registration


async def _writable(
    db: Any, user: Mapping[str, Any], registration_id: Any, request: Any
) -> Registration:
    """A registration the caller may change: visible, and in a scope the
    caller may write (an administrator for the Catalog, the owner for an
    Account, editors and up for a Project)."""
    registration = await get_visible_registration(db, user, registration_id)
    authority = ManifestAuthority(db, dict(user), request=request)
    await authority.scope(registration.scope, write=True)
    return registration


async def delete_registration(
    db: Any, user: Mapping[str, Any], registration_id: Any, *, request: Any = None
) -> None:
    """Delete a registration whose every binding was revoked, and which no
    connector uses or which is disabled (409 otherwise). Its row is locked
    against a connector being created on it meanwhile."""
    registration = await _writable(db, user, registration_id, request)
    async with db.transaction_scope():
        locked = await db.fetchrow(
            "SELECT disabled_at FROM connector_driver_registrations "
            "WHERE id = $1 FOR UPDATE",
            UUID(registration.id),
        )
        if locked is None:
            raise _not_found()
        live = await db.fetchval(
            "SELECT count(*) FROM connector_bind_time_bindings "
            "WHERE registration_id = $1 AND status IN ('pending', 'bound', 'revoking')",
            UUID(registration.id),
        )
        if live:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{live} binding(s) of this driver are not revoked yet. "
                    "Disable it (POST /api/connector-drivers/"
                    f"{registration.id}/disable), let SRW revoke them, then delete it"
                ),
            )
        used = await db.fetchval(
            "SELECT count(*) FROM connector_driver_assignments "
            "WHERE registration_id = $1",
            UUID(registration.id),
        )
        if used and locked["disabled_at"] is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{used} connector(s) use this driver. Delete them, or disable "
                    "the driver first (POST /api/connector-drivers/"
                    f"{registration.id}/disable): its connectors then stay, "
                    "without a driver"
                ),
            )
        await db.execute(
            "DELETE FROM connector_driver_registrations WHERE id = $1",
            UUID(registration.id),
        )
    await log_security_event(
        db,
        resource_type="connector_driver",
        event_type="connector_driver_deleted",
        user=dict(user),
        resource_id=registration.id,
        detail=f"name={registration.name} scope={registration.scope_kind}",
        request=request,
    )


async def set_registration_disabled(
    db: Any,
    user: Mapping[str, Any],
    registration_id: Any,
    *,
    disabled: bool,
    request: Any = None,
) -> Registration:
    """Disable (or enable again) a registration: the kill switch. Disabled,
    it binds nothing new and every live binding of it is revoked; enabled
    again, its connectors' failed binds get a fresh try."""
    from orchestrator.services import connector_bind_time

    registration = await _writable(db, user, registration_id, request)
    async with db.transaction_scope():
        row = await db.fetchrow(
            f"""
            UPDATE connector_driver_registrations
               SET disabled_at = CASE WHEN $2 THEN COALESCE(disabled_at, now())
                                      ELSE NULL END,
                   disabled_by = CASE WHEN $2 THEN COALESCE(disabled_by, $3)
                                      ELSE NULL END,
                   updated_at = now()
             WHERE id = $1
            RETURNING {_ROW}
            """,
            UUID(registration.id),
            disabled,
            UUID(str(user["id"])),
        )
        if row is None:
            raise _not_found()
        async with db.acquire() as conn:
            if disabled:
                revoked = await connector_bind_time.registration_disabled(
                    conn, registration.id
                )
            else:
                revoked = 0
                await connector_bind_time.registration_enabled(conn, registration.id)
    await log_security_event(
        db,
        resource_type="connector_driver",
        event_type=(
            "connector_driver_disabled" if disabled else "connector_driver_enabled"
        ),
        user=dict(user),
        resource_id=registration.id,
        detail=(
            f"name={registration.name} scope={registration.scope_kind} "
            f"revoked_bindings={revoked}"
        ),
        request=request,
    )
    return registration_from_row(row)


# =============================================================================
# Connectors and their registration
# =============================================================================


async def resolve_registration_for_use(
    db: Any,
    user: Mapping[str, Any],
    *,
    registration_id: Any = None,
    name: str | None = None,
    project_id: str | None = None,
) -> Registration:
    """The registration a new connector of ``user`` runs.

    By id: any registration the caller may read. By name: Catalog, then
    ``project_id``'s (a project the caller may read), then the caller's
    Account. Only a bind-time registration serves a connector this way.
    """
    if registration_id is not None:
        registration = await get_visible_registration(db, user, registration_id)
    elif name:
        if reserved_name(name):
            raise HTTPException(
                status_code=400,
                detail=f"{name} is a driver SRW ships; pick it by its type",
            )
        candidates = [
            item
            for item in await list_visible_registrations(db, user)
            if item.name == name
            and (item.scope_kind != "Project" or item.project_id == project_id)
        ]
        catalog = [item for item in candidates if item.scope_kind == "Catalog"]
        if catalog:
            candidates = catalog
        if not candidates:
            raise HTTPException(
                status_code=404, detail=f"No driver named {name} is visible to you"
            )
        if len(candidates) > 1:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": (
                        f"Ambiguous driver name {name}: pick the registration "
                        "(driver_registration_id)"
                    ),
                    "registrations": [
                        {"id": item.id, "scope": item.scope_kind} for item in candidates
                    ],
                },
            )
        registration = candidates[0]
    else:
        raise HTTPException(
            status_code=400,
            detail=(
                "A registered driver's connector names its driver: "
                "driver_registration_id or driver"
            ),
        )
    if registration.plane != "bind_time":
        raise HTTPException(
            status_code=422,
            detail=(
                f"{registration.name} runs on the {registration.plane} plane; "
                "registered connectors are bind-time drivers in this release"
            ),
        )
    if registration.disabled:
        raise HTTPException(
            status_code=409,
            detail=f"The driver registration {registration.name} is disabled",
        )
    return registration


async def registration_by_id(db: Any, registration_id: Any) -> Registration | None:
    """A registration by id, whoever may see it (SRW's own reads: a revoke
    after its connector is gone)."""
    return await _load(db, registration_id) if registration_id else None


async def registration_for_connector(db: Any, connector_id: Any) -> Registration | None:
    """The registration a connector runs, if it runs one (and it still exists)."""
    try:
        uid = UUID(str(connector_id))
    except ValueError:
        return None
    row = await db.fetchrow(
        f"""
        SELECT {", ".join("r." + column for column in _ROW.split(", "))}
          FROM connector_driver_assignments a
          JOIN connector_driver_registrations r ON r.id = a.registration_id
         WHERE a.connector_id = $1
        """,
        uid,
    )
    if row is None:
        return None
    found = _readable([row])
    return found[0] if found else None


_BINDINGS = """
SELECT owner_kind, owner_id, status, attempt, image_reference, image_digest,
       image_stale, resolved_at, spec_hash, protocol_version, error_class,
       error_message, retry_at, created_at, bound_at, failed_at, revoked_at,
       revoke_reason, revoke_error
  FROM connector_bind_time_bindings
 WHERE connector_id = $1
 ORDER BY created_at DESC
 LIMIT $2
"""


def _iso(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def _binding_view(row: Mapping[str, Any], *, with_owner: bool) -> dict[str, Any]:
    view = {
        "status": row["status"],
        # What the bind recorded (connector_drivers.md, "Driver versions").
        "reference": row["image_reference"],
        "digest": row["image_digest"],
        "resolved_at": _iso(row["resolved_at"]),
        "spec_hash": row["spec_hash"],
        "protocol_version": row["protocol_version"],
        # The registry was unreachable: the bind reused the last digest.
        "stale": bool(row["image_stale"]),
        "attempt": row["attempt"],
        "error_class": row["error_class"],
        "message": row["error_message"],
        "retry_at": _iso(row["retry_at"]),
        "created_at": _iso(row["created_at"]),
        "bound_at": _iso(row["bound_at"]),
        "failed_at": _iso(row["failed_at"]),
        "revoked_at": _iso(row["revoked_at"]),
        "revoke_reason": row["revoke_reason"],
        "revoke_error": row["revoke_error"],
    }
    if with_owner:
        view["owner"] = {"kind": row["owner_kind"], "id": str(row["owner_id"])}
    return view


async def connector_driver_status(
    db: Any, connector_id: Any, *, with_bindings: bool = False
) -> dict[str, Any]:
    """Which registration a connector runs and how its binds went.

    ``last_bind`` is the newest bind of any execution, a refusal included:
    what a reader of the connector sees when a moved tag broke it, a bind
    failed for good (a session skips the connector then) or the registry was
    unreachable (``stale``). ``bindings`` (the connector's owner and
    administrators only: they name other users' executions) lists recent
    binds with the digest each used, and only they see an Account
    registration's owner.
    """
    registration = await registration_for_connector(db, connector_id)
    rows = await db.fetch(_BINDINGS, UUID(str(connector_id)), 20)
    scope = registration.scope if registration is not None else None
    if scope is not None and scope["kind"] == "Account" and not with_bindings:
        scope = {"kind": "Account"}
    status: dict[str, Any] = {
        "registration": (
            {
                "id": registration.id,
                "name": registration.name,
                "title": registration.title,
                "scope": scope,
                "image_reference": registration.image_reference,
                "plane": registration.plane,
                "disabled": registration.disabled,
            }
            if registration is not None
            else None
        ),
        "notice": (
            "registration disabled"
            if registration is not None and registration.disabled
            else "registration gone"
            if registration is None
            else None
        ),
        "last_bind": _binding_view(rows[0], with_owner=False) if rows else None,
    }
    if with_bindings:
        status["bindings"] = [_binding_view(row, with_owner=True) for row in rows]
    return status


async def driver_for_row(
    db: Any,
    registry: Any,
    row: Mapping[str, Any],
    *,
    check_runner: CheckRunner | None = None,
) -> Any:
    """The driver that serves a stored connector row.

    The type's driver, except that a type whose connectors each run a
    registration answers with that registration's driver (the type's own,
    which refuses writes, when the registration is gone).
    """
    driver = registry.for_type(row.get("type"))
    if not isinstance(driver, SupportsDriverRegistration) or not row.get("id"):
        return driver
    registration = await registration_for_connector(db, row["id"])
    if registration is None:
        return driver
    return driver.for_registration(registration, check_runner=check_runner)


__all__ = [
    "CATALOG",
    "SCOPE_KINDS",
    "DriverTrustPolicy",
    "Registration",
    "connector_driver_status",
    "delete_registration",
    "set_registration_disabled",
    "driver_for_row",
    "get_visible_registration",
    "list_visible_registrations",
    "register_driver",
    "registration_by_id",
    "registration_for_connector",
    "registration_from_row",
    "resolve_registration_for_use",
]
