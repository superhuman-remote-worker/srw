"""Connector (datasource) CRUD, catalogue, eligibility and connectivity tests.

Extracted verbatim from ``orchestrator.main`` (R1.B03 lane D). Three
properties are load-bearing and moved unchanged:

* **Credentials never leave through a read.** Every response goes through
  ``redact_datasource``/``redact_datasources``; the loaded row keeps its raw
  credentials only so ``test_datasource`` can probe without a second
  round-trip.
* **Authorization order.** Some routes validate the request body *before*
  authenticating (an unknown connector type is a 400 even for an
  unauthenticated caller), and the per-project owner gate fires in the middle
  of create/update rather than at the top. The gates therefore arrive as
  callables the router binds to ``request``, so this module controls *when*
  they fire without owning *what* they are.
* **KB side effects follow the row.** A KB create/update marks the watermark
  pending and schedules a full rebuild; a KB delete goes through the
  advisory-claim fence in :mod:`orchestrator.services.knowledge_index` rather
  than the plain row delete.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from fastapi import HTTPException, Request

from orchestrator.database.postgres import (
    DatasourceCatalogCursorError,
    DatasourcePolicyConflictError,
    DatasourcePolicyValidationError,
    DatasourceProjectAuthorizationError,
    DatasourceScopeAuthorizationError,
)
from orchestrator.schemas.datasources import (
    DatasourceCreate,
    DatasourceUpdate,
    SSHKeyGenerateRequest,
    SSHKeyGenerateResponse,
)
from orchestrator.security.access import (
    filter_visible_datasources,
    log_security_event,
    mcp_scope_project_id,
    redact_datasource,
    redact_datasources,
    user_visible_project_ids,
)
from orchestrator.services import (
    connector_bind_time,
    connector_driver_registrations,
    connector_minted_credentials,
    knowledge_index,
)
from orchestrator.services.connector_drivers import ConnectorDriverRegistry
from orchestrator.services.connector_drivers.base import (
    CheckContext,
    ConnectorDraft,
    DatasourceDriver,
    DeploymentGates,
    DriverEnvironment,
    NormalizedConnector,
    SupportsIndexOperations,
    SupportsTestOverrides,
    SupportsWriteEffects,
    ValidationContext,
)
from orchestrator.services.connector_drivers.registered import (
    CheckRunner,
    SupportsDriverRegistration,
)
from shared.credential_connectors import CredentialConnectorAttachedError
from shared.connectors.platform import platform_owned
from shared.runtime.utils.ssh_key import (
    generate_ed25519_keypair as _generate_ed25519_keypair,
)

logger = logging.getLogger(__name__)

#: Authenticate the caller. Bound by the router to ``require_approved_user``.
ApproveCaller = Callable[[], Awaitable[dict[str, Any]]]
#: Owner gate for one project id. Bound to ``require_project_owner``.
RequireProjectOwner = Callable[[str], Awaitable[Any]]
#: Membership gate for one project id. Bound to ``require_project_member``.
RequireProjectMember = Callable[[str], Awaitable[Any]]
#: Resolve ``(user, datasource)``. Bound to ``require_datasource_owner``.
ResolveDatasourceOwner = Callable[[], Awaitable[tuple[dict[str, Any], dict[str, Any]]]]


@dataclass(frozen=True)
class DatasourceDependencies:
    """Collaborators for one datasource operation, resolved per invocation.

    ``store``, ``vector_db`` and everything reachable through
    ``knowledge_index`` are rebound during ``lifespan``; the application
    rebuilds this dataclass per request rather than capturing it at import.
    """

    store: Any
    vector_db: Any
    knowledge_index: knowledge_index.KnowledgeIndexDependencies
    mcp_datasources_enabled: Callable[[], bool]
    validate_mcp_datasource: Callable[[str | None, dict[str, Any]], None]
    #: The application's installed connector drivers.
    connector_drivers: ConnectorDriverRegistry
    #: Reads a connector's credentials from its Connector's resource secret,
    #: in place (``connector_secrets.read_connector_credentials``); ``None``
    #: tests with the row's own.
    connector_credentials: Callable[..., Awaitable[None]] | None = None
    #: Runs a registered driver's ``check`` in a driver pod (D6); ``None``
    #: where this installation runs no driver pods.
    driver_check_runner: CheckRunner | None = None

    async def driver_for(self, row: dict[str, Any]) -> DatasourceDriver | None:
        """The driver of a stored row: its type's, or for a registered
        driver's connector, its registration's."""
        return await connector_driver_registrations.driver_for_row(
            self.store,
            self.connector_drivers,
            row,
            check_runner=self.driver_check_runner,
        )

    def driver_environment(self) -> DriverEnvironment:
        return DriverEnvironment(
            gates=DeploymentGates(
                mcp_datasources_enabled=self.mcp_datasources_enabled,
            ),
            validate_mcp_datasource=self.validate_mcp_datasource,
        )


# =============================================================================
# SSH keypair generation
# =============================================================================


def generate_datasource_ssh_key(
    *, body: SSHKeyGenerateRequest | None
) -> SSHKeyGenerateResponse:
    """Generate a fresh ed25519 SSH keypair for the user to paste into the form.

    **P4e** — gated to approved users. The keypair is ephemeral (no DB
    write); the gate just blocks anonymous CPU-burn from key generation.

    The private half is returned in OpenSSH PEM format (already normalized
    with a trailing newline so it round-trips through validation) and the
    public half is returned in the single-line authorized_keys format the
    user pastes into their provider's deploy-keys UI. The server does not
    persist the keypair — storage happens when the user submits the
    datasource form, which re-validates the private key via the same
    ssh_key path as a hand-pasted key.
    """
    comment = (body.comment if body else None) or ""
    keypair = _generate_ed25519_keypair(comment=comment)
    return SSHKeyGenerateResponse(
        private_key=keypair.private_key,
        public_key=keypair.public_key,
    )


# =============================================================================
# Reads
# =============================================================================


async def list_datasources(
    *,
    user: dict[str, Any],
    job_id: str | None,
    ds_type: str | None,
    limit: int,
    dependencies: DatasourceDependencies,
) -> list[dict[str, Any]]:
    """List connectors visible to the caller.

    F3: each row is scoped (admin / creator / project member) and the
    `credentials` field is stripped from every row.
    """
    try:
        rows = await dependencies.store.list_datasources(
            job_id=job_id, ds_type=ds_type, limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    # Batch the visibility filter: one project-link fetch + one membership
    # resolution for the whole page (was a per-row N x (1 + M) fan-out).
    visible = await filter_visible_datasources(user, dependencies.store, rows)
    await connector_driver_registrations.annotate_driver_env_names(
        dependencies.store, visible
    )
    return redact_datasources(visible)


async def list_datasource_catalog(
    *,
    user: dict[str, Any],
    q: str | None,
    ds_type: str | None,
    project_id: str | None,
    scope_mode: str | None,
    auto_attach: bool | None,
    visibility: str | None,
    ownership: str | None,
    availability: str | None,
    limit: int,
    cursor: str | None,
    dependencies: DatasourceDependencies,
) -> dict[str, Any]:
    """Cursor-paginated connector management catalog.

    Authorization and every filter are applied before the stable cursor/limit;
    this avoids the legacy newest-100 raw-row cap hiding older owned rows.
    """
    scope_project_id = mcp_scope_project_id(user)
    if scope_project_id and project_id and str(project_id) != str(scope_project_id):
        raise HTTPException(status_code=403, detail="Access denied by MCP token scope")

    visible = await user_visible_project_ids(user, dependencies.store)
    visible_project_ids = [] if visible == "all" else [str(value) for value in visible]
    if scope_project_id:
        visible_project_ids = [str(scope_project_id)]
    try:
        result = await dependencies.store.list_datasource_catalog(
            str(user["id"]),
            visible_project_ids,
            is_admin=bool(user.get("is_admin")),
            restrict_to_projects=bool(scope_project_id),
            q=q,
            ds_type=ds_type,
            project_id=project_id,
            scope_mode=scope_mode,
            auto_attach=auto_attach,
            visibility=visibility,
            ownership=ownership,
            availability=availability,
            limit=limit,
            cursor=cursor,
        )
    except (DatasourcePolicyValidationError, DatasourceCatalogCursorError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    items = result.get("items") or []
    await connector_driver_registrations.annotate_driver_env_names(
        dependencies.store, items
    )
    result["items"] = redact_datasources(items)
    return result


async def list_linkable_datasource_targets(
    *,
    user: dict[str, Any],
    datasource_id: str | None,
    q: str | None,
    limit: int,
    cursor: str | None,
    dependencies: DatasourceDependencies,
) -> dict[str, Any]:
    """Projects addable to a connector policy, plus retained current links."""
    try:
        result = await dependencies.store.list_linkable_datasource_targets(
            str(user["id"]),
            datasource_id=datasource_id,
            is_admin=bool(user.get("is_admin")),
            restrict_project_id=(
                str(mcp_scope_project_id(user)) if mcp_scope_project_id(user) else None
            ),
            q=q,
            limit=limit,
            cursor=cursor,
        )
    except DatasourceCatalogCursorError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return result


async def list_eligible_datasources(
    *,
    user: dict[str, Any],
    project_id: list[str] | None,
    require_project_member: RequireProjectMember,
    dependencies: DatasourceDependencies,
) -> list[dict[str, Any]]:
    """Connectors the caller may pre-select for a job/session (the picker
    source of truth).

    Returns the union of: connectors the caller created, public connectors,
    and connectors linked to any supplied project. Credentials are stripped.
    Membership is required for each supplied project (403 otherwise). Used to
    seed the create-job / create-session connector picker; with explicit-only
    resolution the picker is the only way a job gets connectors.
    """
    project_ids = list(dict.fromkeys(str(value) for value in (project_id or [])))
    scope_project_id = mcp_scope_project_id(user)
    if scope_project_id is not None:
        scoped_id = str(scope_project_id)
        if project_ids and project_ids != [scoped_id]:
            raise HTTPException(
                status_code=403,
                detail="Access denied by MCP token scope",
            )
        # Omission never widens a project-scoped principal to the projectless
        # catalog: the token's project is the authoritative target context.
        project_ids = [scoped_id]
    for pid in project_ids:
        await require_project_member(pid)
    try:
        rows = await dependencies.store.list_eligible_datasources(
            str(user["id"]),
            project_ids,
            is_admin=bool(user.get("is_admin")),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    await connector_driver_registrations.annotate_driver_env_names(
        dependencies.store, rows
    )
    return redact_datasources(rows)


async def get_datasource(
    *,
    user: dict[str, Any],
    ds: dict[str, Any],
    datasource_id: str,
    dependencies: DatasourceDependencies,
) -> dict[str, Any]:
    """Get a single connector by ID. F3: gated + credentials redacted."""
    if user.get("is_admin") or str(ds.get("created_by") or "") == str(user["id"]):
        project_ids = await dependencies.store.list_datasource_projects(datasource_id)
        scope_project_id = mcp_scope_project_id(user)
        ds["project_ids"] = (
            [str(scope_project_id)]
            if scope_project_id
            and str(scope_project_id) in {str(value) for value in project_ids}
            else ([] if scope_project_id else project_ids)
        )
    driver = dependencies.connector_drivers.for_type(ds.get("type"))
    if isinstance(driver, SupportsDriverRegistration):
        # A registered driver's connector shows which image it runs and how
        # its last bind went (a refused moved tag included).
        ds[
            "driver_status"
        ] = await connector_driver_registrations.connector_driver_status(
            dependencies.store,
            datasource_id,
            with_bindings=bool(user.get("is_admin"))
            or str(ds.get("created_by") or "") == str(user["id"]),
        )
        await connector_driver_registrations.annotate_driver_env_names(
            dependencies.store, [ds]
        )
    return redact_datasource(ds)


# =============================================================================
# Writes
# =============================================================================


async def require_scoped_datasource_mutation(
    user: dict[str, Any],
    datasource: dict[str, Any],
    datasource_id: str,
    *,
    dependencies: DatasourceDependencies,
) -> set[str]:
    """Keep a project-scoped token from mutating a cross-scope connector."""
    scope_project_id = mcp_scope_project_id(user)
    if scope_project_id is None:
        return set()
    project_ids = {
        str(value)
        for value in await dependencies.store.list_datasource_projects(datasource_id)
    }
    if datasource.get("scope_mode") != "projects" or project_ids != {
        str(scope_project_id)
    }:
        raise HTTPException(
            status_code=403,
            detail="Access denied by MCP token scope",
        )
    return project_ids


async def create_datasource(
    *,
    body: DatasourceCreate,
    require_approved_user: ApproveCaller,
    require_project_owner: RequireProjectOwner,
    dependencies: DatasourceDependencies,
) -> dict[str, Any]:
    """Create a new connector owned by the current user.

    The connector's driver validates and normalizes what is stored; this
    function owns the order around it: type and pre-authentication checks,
    authentication, project and publish authority, then the driver.
    """
    driver = dependencies.connector_drivers.for_type(body.type)
    if driver is None:
        valid_types = dependencies.connector_drivers.type_ids()
        raise HTTPException(
            status_code=400,
            detail=f"Invalid type '{body.type}'. Must be one of: {', '.join(sorted(valid_types))}",
        )
    if body.job_id is not None:
        raise HTTPException(
            status_code=400,
            detail=(
                "New connectors cannot set job_id; attach them through the "
                "job's explicit connector selection"
            ),
        )
    environment = dependencies.driver_environment()
    draft = ConnectorDraft.from_body(body)
    await driver.prevalidate(draft, environment)

    user = await require_approved_user()
    user_id = str(user["id"])

    # Project scope is an execution boundary, so UUID knowledge or ordinary
    # membership is insufficient to add a link. Creation needs management
    # authority for every selected target before the datasource transaction.
    project_ids = list(dict.fromkeys(str(value) for value in body.project_ids or []))
    registration = None
    if isinstance(driver, SupportsDriverRegistration):
        # A registered driver's connector runs the registration it names, one
        # the caller may read (D6), pinned for the connector's life.
        registration = (
            await connector_driver_registrations.resolve_registration_for_use(
                dependencies.store,
                user,
                registration_id=body.driver_registration_id,
                name=body.driver,
                project_id=project_ids[0] if len(project_ids) == 1 else None,
            )
        )
        driver = driver.for_registration(
            registration, check_runner=dependencies.driver_check_runner
        )
    elif body.driver_registration_id is not None or body.driver is not None:
        raise HTTPException(
            status_code=400,
            detail="Only a registered driver's connector names a driver",
        )
    scope_project_id = mcp_scope_project_id(user)
    if scope_project_id and (
        body.scope_mode != "projects" or set(project_ids) != {str(scope_project_id)}
    ):
        raise HTTPException(
            status_code=403,
            detail="Access denied by MCP token scope",
        )
    for project_id in project_ids:
        await require_project_owner(project_id)

    # Publish gate — is_global hands the publisher's stored credentials to
    # every user's agents (knowledge-base/knowledge/features/public_datasources.md).
    if body.is_global and not await dependencies.store.user_can_publish_datasource(
        user
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                "Publishing public connectors requires the "
                "'public_datasources' capability"
            ),
        )
    read_only = body.read_only
    if body.is_global and read_only is None:
        read_only = True  # invariant: public ⇒ read_only set (RO default)

    normalized = await driver.validate(
        draft,
        existing=None,
        ctx=_validation_context(user, environment, dependencies),
    )

    try:
        created = await dependencies.store.create_datasource(
            name=body.name,
            ds_type=body.type,
            connection_url=normalized.connection_url,
            description=body.description,
            credentials=normalized.credentials,
            job_id=body.job_id,
            cli_hint=body.cli_hint,
            default_branch=body.default_branch,
            config=normalized.config,
            created_by=user_id,
            is_global=body.is_global,
            read_only=read_only,
            scope_mode=body.scope_mode,
            auto_attach=body.auto_attach,
            project_ids=project_ids,
            authority_user_id=user_id,
            authority_is_admin=bool(user.get("is_admin")),
            **(
                {"driver_registration_id": registration.id}
                if registration is not None
                else {}
            ),
        )
    except DatasourceProjectAuthorizationError as exc:
        raise HTTPException(
            status_code=403,
            detail="Not authorized to add one or more project links",
        ) from exc
    except DatasourcePolicyValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except DatasourcePolicyConflictError as exc:
        # The registration was deleted or disabled meanwhile (D6).
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        if "unique" in error_msg.lower() or "duplicate" in error_msg.lower():
            raise HTTPException(
                status_code=409,
                detail=f"A connector named '{body.name}' of type '{body.type}' already exists",
            ) from e
        raise HTTPException(status_code=500, detail=error_msg) from e
    created["project_ids"] = project_ids

    if isinstance(driver, SupportsWriteEffects):
        await driver.after_write(
            str(created["id"]),
            normalized,
            created=True,
            knowledge_index=dependencies.knowledge_index,
        )
    return redact_datasource(created)


def _validation_context(
    user: dict[str, Any],
    environment: DriverEnvironment,
    dependencies: DatasourceDependencies,
) -> ValidationContext:
    async def can_autonomous_send() -> bool:
        return bool(await dependencies.store.user_can_autonomous_send(user))

    return ValidationContext(
        environment=environment, can_autonomous_send=can_autonomous_send
    )


def minting_inputs_changed(
    existing_ds: dict[str, Any], normalized: NormalizedConnector
) -> bool:
    """Whether an update of a connector that mints at a provider (C5)
    changed what it mints with: its URL, config or credentials."""
    if connector_minted_credentials.row_provider(existing_ds) is None:
        return False
    return (
        normalized.config is not None
        or normalized.credentials is not None
        or normalized.connection_url_set
        or normalized.connection_url is not None
    )


async def revoke_minted_after_update(
    store: Any,
    datasource_id: str,
    existing_ds: dict[str, Any],
    normalized: NormalizedConnector,
) -> int:
    """Right after the update committed: what SRW minted with the old
    minting inputs is revoked, and each execution's next delivery mints
    afresh (C5). Returns how many credentials were asked to go."""
    if not minting_inputs_changed(existing_ds, normalized):
        return 0
    async with store.acquire() as conn:
        return await connector_minted_credentials.connector_changed(conn, datasource_id)


async def update_datasource(
    *,
    request: Request,
    datasource_id: str,
    body: DatasourceUpdate,
    user: dict[str, Any],
    existing_ds: dict[str, Any],
    require_project_owner: RequireProjectOwner,
    dependencies: DatasourceDependencies,
) -> dict[str, Any]:
    """Update a connector. F3: creator/admin only; credentials are preserved.

    Authority comes first (MCP token scope, the driver's deployment gate, the
    publish gate, the native-KB lock, owner authority over new project
    links); the connector's driver then validates the content it changes.
    """
    scope_project_id = mcp_scope_project_id(user)
    scoped_current_project_ids = await require_scoped_datasource_mutation(
        user, existing_ds, datasource_id, dependencies=dependencies
    )
    environment = dependencies.driver_environment()
    driver = await dependencies.driver_for(existing_ds)
    if driver is not None:
        driver.require_enabled(environment.gates)
    # Publish gate (spec: knowledge-base/knowledge/features/public_datasources.md). Only the
    # false→true transition needs the capability; unpublishing must always
    # work for creator/admin (a revoked grant must not trap a public row).
    if body.is_global is True and not existing_ds.get("is_global"):
        if not await dependencies.store.user_can_publish_datasource(user):
            raise HTTPException(
                status_code=403,
                detail=(
                    "Publishing public connectors requires the "
                    "'public_datasources' capability"
                ),
            )
    read_only = body.read_only
    effective_global = (
        body.is_global
        if body.is_global is not None
        else bool(existing_ds.get("is_global"))
    )
    if effective_global and read_only is None and existing_ds.get("read_only") is None:
        read_only = True  # invariant: public ⇒ read_only set

    policy_fields = {"scope_mode", "project_ids", "auto_attach"}
    policy_changed = bool(policy_fields.intersection(body.model_fields_set))
    if platform_owned(existing_ds) and policy_changed:
        raise HTTPException(
            status_code=409,
            detail="The native project knowledge connector policy is managed by its project",
        )

    # A connector-owner may always revoke an existing link, but every newly
    # added target requires current project-owner authority. This check is
    # deliberately based on the desired-set diff: retained-only projects can
    # survive an edit without being re-authorized or losing their overrides.
    existing_project_ids: set[str] = set()
    if policy_changed:
        existing_project_ids = (
            scoped_current_project_ids
            if scope_project_id
            else set(await dependencies.store.list_datasource_projects(datasource_id))
        )
    if "project_ids" in body.model_fields_set:
        desired_project_ids = set(str(value) for value in body.project_ids or [])
        if scope_project_id and desired_project_ids != {str(scope_project_id)}:
            raise HTTPException(
                status_code=403,
                detail="Access denied by MCP token scope",
            )
        for project_id in sorted(desired_project_ids - existing_project_ids):
            await require_project_owner(project_id)
    if (
        scope_project_id
        and "scope_mode" in body.model_fields_set
        and body.scope_mode != "projects"
    ):
        raise HTTPException(
            status_code=403,
            detail="Access denied by MCP token scope",
        )

    draft = ConnectorDraft.from_body(body)
    if driver is None:
        # A stored type no driver serves: only the type-free rules apply.
        credentials = DatasourceDriver.stored_credentials(draft, existing_ds)
        normalized = NormalizedConnector(
            draft.connection_url,
            DatasourceDriver.no_config(draft, existing_ds),
            credentials,
        )
    else:
        normalized = await driver.validate(
            draft,
            existing=existing_ds,
            ctx=_validation_context(user, environment, dependencies),
        )
    connection_url = normalized.connection_url
    datasource_config = normalized.config
    credentials = normalized.credentials
    connection_url_set = normalized.connection_url_set
    try:
        policy_result: dict[str, Any] | None = None
        content_fields = {
            "name",
            "description",
            "connection_url",
            "credentials",
            "cli_hint",
            "default_branch",
            "config",
            "is_global",
            "read_only",
        }
        content_changed = bool(content_fields.intersection(body.model_fields_set))
        if policy_changed:
            policy_result = await dependencies.store.update_datasource_with_policy(
                datasource_id,
                expected_policy_revision=int(body.policy_revision or 0),
                scope_mode=(
                    body.scope_mode if "scope_mode" in body.model_fields_set else None
                ),
                auto_attach=(
                    body.auto_attach if "auto_attach" in body.model_fields_set else None
                ),
                project_ids=(
                    body.project_ids if "project_ids" in body.model_fields_set else None
                ),
                name=body.name,
                description=body.description,
                connection_url=connection_url,
                credentials=credentials,
                cli_hint=body.cli_hint,
                default_branch=body.default_branch,
                config=datasource_config,
                is_global=body.is_global,
                read_only=read_only,
                connection_url_set=connection_url_set,
                authority_user_id=str(user["id"]),
                authority_is_admin=bool(user.get("is_admin")),
                authority_project_scope_id=(
                    str(scope_project_id) if scope_project_id else None
                ),
            )
            if policy_result is None:
                raise HTTPException(
                    status_code=404, detail=f"Connector '{datasource_id}' not found"
                )

        if content_changed and not policy_changed:
            update_kwargs = dict(
                datasource_id=datasource_id,
                name=body.name,
                description=body.description,
                connection_url=connection_url,
                credentials=credentials,
                cli_hint=body.cli_hint,
                default_branch=body.default_branch,
                config=datasource_config,
                is_global=body.is_global,
                read_only=read_only,
            )
            if connection_url_set:
                update_kwargs["connection_url_set"] = True
            if scope_project_id:
                update_kwargs["authority_project_scope_id"] = str(scope_project_id)
            success = await dependencies.store.update_datasource(**update_kwargs)
            if not success:
                raise HTTPException(
                    status_code=404, detail=f"Connector '{datasource_id}' not found"
                )

        # Right after the update committed, before anything else that may
        # fail: what SRW minted with the old minting inputs goes (C5).
        await revoke_minted_after_update(
            dependencies.store, datasource_id, existing_ds, normalized
        )
        if isinstance(driver, SupportsWriteEffects):
            await driver.after_write(
                datasource_id,
                normalized,
                created=False,
                knowledge_index=dependencies.knowledge_index,
            )
        if isinstance(driver, SupportsDriverRegistration) and (
            normalized.config is not None or normalized.credentials is not None
        ):
            # Its bindings ran on the old config or credentials: revoke them;
            # each execution's next delivery binds afresh (D6).
            async with dependencies.store.acquire() as conn:
                await connector_bind_time.connector_changed(conn, datasource_id)

        updated_ds = await dependencies.store.get_datasource(datasource_id)
        if not updated_ds:
            raise HTTPException(
                status_code=404, detail=f"Connector '{datasource_id}' not found"
            )
        updated_project_ids = (
            policy_result.get("project_ids", [])
            if policy_result is not None
            else await dependencies.store.list_datasource_projects(datasource_id)
        )
        updated_ds["project_ids"] = (
            [str(scope_project_id)]
            if scope_project_id
            and str(scope_project_id) in {str(value) for value in updated_project_ids}
            else ([] if scope_project_id else updated_project_ids)
        )
        if policy_result is not None:
            await log_security_event(
                dependencies.store,
                resource_type="datasource",
                event_type="datasource_policy_updated",
                user=user,
                resource_id=datasource_id,
                detail=(
                    f"scope={existing_ds.get('scope_mode', 'all')}->"
                    f"{updated_ds.get('scope_mode', 'all')} "
                    f"auto={bool(existing_ds.get('auto_attach', False))}->"
                    f"{bool(updated_ds.get('auto_attach', False))} "
                    f"projects={len(existing_project_ids)}->"
                    f"{len(updated_ds['project_ids'])} "
                    f"revision={updated_ds.get('policy_revision')}"
                ),
                request=request,
            )
        return redact_datasource(updated_ds)
    except DatasourceScopeAuthorizationError as exc:
        raise HTTPException(
            status_code=403,
            detail="Access denied by MCP token scope",
        ) from exc
    except DatasourceProjectAuthorizationError as exc:
        raise HTTPException(
            status_code=403,
            detail="Not authorized to add one or more project links",
        ) from exc
    except DatasourcePolicyConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except DatasourcePolicyValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def delete_datasource(
    *,
    user: dict[str, Any],
    datasource: dict[str, Any],
    datasource_id: str,
    dependencies: DatasourceDependencies,
) -> dict[str, str]:
    """Delete a connector. F3: creator/admin only.

    A connector SRW indexes is deleted through its driver, behind the index
    fence; every other connector is a plain row delete.
    """
    if platform_owned(datasource):
        raise HTTPException(
            status_code=409,
            detail="The project knowledge connector is managed by its project",
        )
    await require_scoped_datasource_mutation(
        user, datasource, datasource_id, dependencies=dependencies
    )
    scope_project_id = mcp_scope_project_id(user)
    driver = dependencies.connector_drivers.for_type(datasource.get("type"))
    try:
        if isinstance(driver, SupportsIndexOperations):
            success = await driver.delete_with_index(
                datasource_id,
                authority_project_scope_id=(
                    str(scope_project_id) if scope_project_id else None
                ),
                deleted_by=str(user["id"]),
                knowledge_index=dependencies.knowledge_index,
            )
        else:
            success = await dependencies.store.delete_datasource(
                datasource_id,
                authority_project_scope_id=(
                    str(scope_project_id) if scope_project_id else None
                ),
                deleted_by=str(user["id"]),
            )
        if not success:
            raise HTTPException(
                status_code=404, detail=f"Connector '{datasource_id}' not found"
            )
        return {"status": "deleted"}
    except CredentialConnectorAttachedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except HTTPException:
        raise
    except DatasourceScopeAuthorizationError as exc:
        raise HTTPException(
            status_code=403,
            detail="Access denied by MCP token scope",
        ) from exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def get_job_datasources(
    *, job_id: str, dependencies: DatasourceDependencies
) -> list[dict[str, Any]]:
    """Get resolved connectors for a job.

    F3: gated by `require_job_access`; credentials redacted in the
    response (the agent process gets them via internal dispatch, not via
    this endpoint).
    """
    try:
        rows = await dependencies.store.resolve_datasources_for_job(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return redact_datasources(rows)


# =============================================================================
# KB index reads/commands on a connector
# =============================================================================


def _index_driver(
    datasource: dict[str, Any], dependencies: DatasourceDependencies
) -> SupportsIndexOperations:
    driver = dependencies.connector_drivers.for_type(datasource.get("type"))
    if not isinstance(driver, SupportsIndexOperations):
        raise HTTPException(
            status_code=400, detail="Connector is not an OKF Knowledge Base"
        )
    return driver


async def get_datasource_index_status(
    *,
    datasource: dict[str, Any],
    datasource_id: str,
    dependencies: DatasourceDependencies,
) -> dict[str, Any]:
    """Return credential-free indexing state for an OKF KB connector."""
    driver = _index_driver(datasource, dependencies)
    try:
        return await driver.index_status(
            datasource, datasource_id, vector_db=dependencies.vector_db
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def reindex_datasource_knowledge(
    *,
    datasource: dict[str, Any],
    full: bool,
    dependencies: DatasourceDependencies,
) -> dict[str, Any]:
    """Incrementally refresh an external OKF KB; owner/admin only."""
    driver = _index_driver(datasource, dependencies)
    try:
        return await driver.reindex(
            datasource, full=full, knowledge_index=dependencies.knowledge_index
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# =============================================================================
# Connectivity probes
# =============================================================================


async def test_datasource(
    *,
    resolve_datasource: ResolveDatasourceOwner,
    dependencies: DatasourceDependencies,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Test connectivity to a connector.

    Attempts to connect using the stored connection details and returns
    the result. Does not modify any data. F3: creator/admin only (test
    uses live credentials and probes the target). ``overrides`` is the
    endpoint a connector form is editing; a driver that can test an edit
    validates it like an update, and every other driver ignores it.
    The credentials come from the Connector's resource secret, which the
    owner may always use, or from the row where the resource has none yet.
    """
    try:
        user, ds = await resolve_datasource()
        if dependencies.connector_credentials is not None:
            await dependencies.connector_credentials([ds], authorized=[str(ds["id"])])
        driver = await dependencies.driver_for(ds)
        if overrides and isinstance(driver, SupportsTestOverrides):
            ds = driver.apply_test_overrides(ds, overrides)
        ds_type = ds["type"]
        creds = ds.get("credentials") or {}
        if isinstance(creds, str):
            creds = json.loads(creds)
        if driver is None:
            return {"status": "error", "message": f"Unknown connector type: {ds_type}"}
        environment = dependencies.driver_environment()
        driver.require_enabled(environment.gates)
        return await driver.check(
            ds,
            creds,
            ctx=CheckContext(
                environment,
                requester=str(user.get("id")) if isinstance(user, dict) else None,
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        error_ref = uuid4().hex[:12]
        logger.exception("Connector test failed (error_ref=%s)", error_ref)
        raise HTTPException(
            status_code=500,
            detail=f"Connector test failed (error_ref={error_ref})",
        ) from e
