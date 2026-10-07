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

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import asyncpg
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
from orchestrator.security.credential_files import (
    CREDENTIAL_FILE_TYPES,
    CredentialFileValidationError,
    normalize_credential_files,
)
from orchestrator.services import knowledge_index
from orchestrator.services.datasource_config import (
    normalize_datasource_credentials,
    normalize_kb_config,
    normalize_repository_config,
    validate_kb_repository_auth,
    validate_kb_repository_url,
)
from orchestrator.services.email_datasource import (
    probe_email_connection,
    validate_email_config,
    validate_email_credentials,
)
from orchestrator.services.workspace_ssh_connector import (
    WorkspaceSshConnectorError,
    probe_workspace_ssh_connector,
    repository_uses_ssh_key,
    validate_workspace_ssh_connector,
)
from shared.runtime.core.datasource_catalog import DATASOURCE_TYPES
from shared.credential_connectors import (
    CredentialConnectorAttachedError,
    normalize_credential_env,
)
from shared.runtime.utils.ssh_key import (
    generate_ed25519_keypair as _generate_ed25519_keypair,
)

logger = logging.getLogger(__name__)

#: Connector types whose SSH key reaches a workspace ``ssh-agent`` (C1).
WORKSPACE_SSH_CONNECTOR_TYPES = frozenset({"repository", "ssh_key"})

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
    result["items"] = redact_datasources(result.get("items") or [])
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
    """Create a new connector owned by the current user."""
    valid_types = DATASOURCE_TYPES
    if body.type not in valid_types:
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
    if body.type == "mcp":
        if not dependencies.mcp_datasources_enabled():
            raise HTTPException(
                status_code=403,
                detail="MCP connectors are disabled on this deployment",
            )
        dependencies.validate_mcp_datasource(
            body.connection_url, body.credentials or {}
        )

    user = await require_approved_user()
    user_id = str(user["id"])

    # Project scope is an execution boundary, so UUID knowledge or ordinary
    # membership is insufficient to add a link. Creation needs management
    # authority for every selected target before the datasource transaction.
    project_ids = list(dict.fromkeys(str(value) for value in body.project_ids or []))
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
    # Mailboxes are never published — a public email datasource would hand the
    # owner's IMAP/SMTP credentials to every user's agents, so no capability
    # can allow it (knowledge-base/knowledge/features/email_datasource.md).
    if body.type == "email" and body.is_global:
        raise HTTPException(
            status_code=400,
            detail="Email connectors cannot be published (is_global)",
        )
    read_only = body.read_only
    if body.type == "kb":
        if read_only is False:
            raise HTTPException(
                status_code=400,
                detail="Knowledge-base connectors are always read-only",
            )
        if body.is_global:
            read_only = True
    elif body.is_global and read_only is None:
        read_only = True  # invariant: public ⇒ read_only set (RO default)

    connection_url = body.connection_url
    if body.type == "kb":
        connection_url = validate_kb_repository_url(connection_url)
        datasource_config = normalize_kb_config(body.config)
    elif body.type == "email":
        # The grant is only consulted when unattended_send is requested, so
        # the common draft-tier create skips the grant-resolution round-trip.
        wants_unattended = bool((body.config or {}).get("unattended_send"))
        owner_has_send_grant = (
            await dependencies.store.user_can_autonomous_send(user)
            if wants_unattended
            else False
        )
        try:
            datasource_config = validate_email_config(body.config, owner_has_send_grant)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    elif body.type == "mcp":
        if (body.credentials or {}).get("transport", "http").lower() == "stdio":
            connection_url = None
        if body.config:
            raise HTTPException(
                status_code=400,
                detail="Connector config is not supported for MCP connectors",
            )
        datasource_config = {}
    elif body.type == "repository":
        datasource_config = normalize_repository_config(
            body.config, body.connection_url
        )
    elif body.type == "ssh_key":
        # Host, user, port and pinned host keys; validated with the key below.
        datasource_config = dict(body.config or {})
    else:
        if body.config:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Connector config is only supported for OKF Knowledge "
                    "Bases and email connectors"
                ),
            )
        datasource_config = dict(body.config or {})

    credentials = normalize_datasource_credentials(body.credentials)
    if body.type == "credentials":
        if body.is_global:
            raise HTTPException(
                status_code=400, detail="Credential connectors cannot be published"
            )
        try:
            credentials = {
                "env_vars": normalize_credential_env(
                    (credentials or {}).get("env_vars", {}), required=True
                )
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        credentials = normalize_credential_files(body.type, body.name, credentials)
    except CredentialFileValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if body.type in WORKSPACE_SSH_CONNECTOR_TYPES:
        try:
            datasource_config = validate_workspace_ssh_connector(
                body.type,
                connection_url=connection_url,
                config=datasource_config,
                credentials=credentials,
            )
        except WorkspaceSshConnectorError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    if body.type == "kb":
        validate_kb_repository_auth(connection_url, credentials)
    if body.type == "email":
        # Shape check runs BEFORE encryption at rest (create_datasource
        # encrypts transparently); smtp block required only for access='send'.
        try:
            credentials = validate_email_credentials(
                credentials, access=datasource_config["access"]
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        created = await dependencies.store.create_datasource(
            name=body.name,
            ds_type=body.type,
            connection_url=connection_url,
            description=body.description,
            credentials=credentials,
            job_id=body.job_id,
            cli_hint=body.cli_hint,
            default_branch=body.default_branch,
            config=datasource_config,
            created_by=user_id,
            is_global=body.is_global,
            read_only=read_only,
            scope_mode=body.scope_mode,
            auto_attach=body.auto_attach,
            project_ids=project_ids,
            authority_user_id=user_id,
            authority_is_admin=bool(user.get("is_admin")),
        )
    except DatasourceProjectAuthorizationError as exc:
        raise HTTPException(
            status_code=403,
            detail="Not authorized to add one or more project links",
        ) from exc
    except DatasourcePolicyValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
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

    if body.type == "kb":
        await knowledge_index.mark_kb_datasource_pending(
            str(created["id"]), dependencies=dependencies.knowledge_index
        )
        knowledge_index.schedule_kb_datasource_reindex(
            str(created["id"]),
            force_full=True,
            dependencies=dependencies.knowledge_index,
        )
    return redact_datasource(created)


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
    """Update a connector. F3: creator/admin only; credentials are preserved."""
    scope_project_id = mcp_scope_project_id(user)
    scoped_current_project_ids = await require_scoped_datasource_mutation(
        user, existing_ds, datasource_id, dependencies=dependencies
    )
    if existing_ds.get("type") == "mcp" and not dependencies.mcp_datasources_enabled():
        raise HTTPException(
            status_code=403,
            detail="MCP connectors are disabled on this deployment",
        )
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
    # Mailboxes are never published — a public email datasource would hand the
    # owner's IMAP/SMTP credentials to every user's agents, so no capability
    # can allow it (knowledge-base/knowledge/features/email_datasource.md).
    if existing_ds.get("type") == "email" and body.is_global is True:
        raise HTTPException(
            status_code=400,
            detail="Email connectors cannot be published (is_global)",
        )
    read_only = body.read_only
    if existing_ds.get("type") == "kb" and read_only is False:
        raise HTTPException(
            status_code=400,
            detail="Knowledge-base connectors are always read-only",
        )
    effective_global = (
        body.is_global
        if body.is_global is not None
        else bool(existing_ds.get("is_global"))
    )
    if effective_global and read_only is None and existing_ds.get("read_only") is None:
        read_only = True  # invariant: public ⇒ read_only set
    # F3: if body.credentials is None or {}, do NOT touch the stored value.
    # The cockpit's edit form sends an empty creds dict when the user
    # didn't re-enter; passing that through would clobber the secret.
    raw_creds = normalize_datasource_credentials(body.credentials)
    credentials = raw_creds if raw_creds else None
    if existing_ds.get("type") == "credentials":
        if body.is_global is True:
            raise HTTPException(
                status_code=400, detail="Credential connectors cannot be published"
            )
        if credentials is not None:
            try:
                values = normalize_credential_env(
                    credentials.get("env_vars", {}), required=True
                )
                previous = (existing_ds.get("credentials") or {}).get("env_vars", {})
                credentials = {
                    "env_vars": normalize_credential_env(
                        {**previous, **values}, required=True
                    )
                }
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
    if credentials is not None and existing_ds.get("type") in CREDENTIAL_FILE_TYPES:
        try:
            credentials = normalize_credential_files(
                existing_ds["type"],
                body.name or existing_ds.get("name", ""),
                credentials,
            )
        except CredentialFileValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    from orchestrator.services.kb_datasources import (
        NATIVE_PROJECT_CONFIG_KEY,
        native_kb_project_id,
    )

    connection_url = body.connection_url
    datasource_config = body.config
    mcp_connection_url_set = False
    reindex_required = False
    native_project = native_kb_project_id(existing_ds)
    policy_fields = {"scope_mode", "project_ids", "auto_attach"}
    policy_changed = bool(policy_fields.intersection(body.model_fields_set))
    if native_project and policy_changed:
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
    if existing_ds.get("type") == "kb" and native_project:
        # A project's own KB row is a management surface, not a remote source:
        # it has no repository URL or credentials to validate, and it is never
        # indexed under its datasource id, so no edit here can require a
        # rebuild. Config still goes through the normalizer (unknown keys stay
        # rejected) and the server-owned marker is re-attached, so editing the
        # root path cannot quietly promote the vault into the external sweep
        # and index every note a second time.
        if datasource_config is not None:
            datasource_config = normalize_kb_config(datasource_config)
            datasource_config[NATIVE_PROJECT_CONFIG_KEY] = native_project
        connection_url = None  # nothing to point at; leave the column alone
    elif existing_ds.get("type") == "kb":
        if connection_url is not None:
            connection_url = validate_kb_repository_url(connection_url)
            reindex_required = (
                connection_url != str(existing_ds.get("connection_url") or "").strip()
            )
        if datasource_config is not None:
            datasource_config = normalize_kb_config(datasource_config)
            existing_config = normalize_kb_config(existing_ds.get("config"))
            reindex_required = reindex_required or (
                datasource_config != existing_config
            )
        if credentials is not None:
            reindex_required = reindex_required or (
                credentials != (existing_ds.get("credentials") or {})
            )
        if body.default_branch is not None:
            reindex_required = reindex_required or (
                (body.default_branch or None)
                != (existing_ds.get("default_branch") or None)
            )
        effective_url = connection_url or str(existing_ds.get("connection_url") or "")
        effective_credentials = (
            credentials
            if credentials is not None
            else (existing_ds.get("credentials") or {})
        )
        validate_kb_repository_auth(effective_url, effective_credentials)
    elif existing_ds.get("type") == "email":
        if datasource_config is not None:
            wants_unattended = bool(datasource_config.get("unattended_send"))
            owner_has_send_grant = (
                await dependencies.store.user_can_autonomous_send(user)
                if wants_unattended
                else False
            )
            try:
                datasource_config = validate_email_config(
                    datasource_config, owner_has_send_grant
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        # Validate the EFFECTIVE credential shape against the EFFECTIVE access
        # tier (e.g. flipping access to 'send' without a stored smtp block must
        # 400 here, not fail at first use). Runs BEFORE encryption at rest;
        # preserved (None) credentials are checked but not rewritten.
        effective_email_conf = (
            datasource_config
            if datasource_config is not None
            else (existing_ds.get("config") or {})
        )
        effective_credentials = (
            credentials
            if credentials is not None
            else (existing_ds.get("credentials") or {})
        )
        try:
            checked_credentials = validate_email_credentials(
                effective_credentials,
                access=effective_email_conf.get("access", "draft"),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if credentials is not None:
            credentials = checked_credentials
    elif existing_ds.get("type") == "mcp":
        if datasource_config:
            raise HTTPException(
                status_code=400,
                detail="Connector config is not supported for MCP connectors",
            )
        effective_credentials = (
            credentials
            if credentials is not None
            else (existing_ds.get("credentials") or {})
        )
        url_was_supplied = "connection_url" in body.model_fields_set
        effective_url = (
            body.connection_url
            if url_was_supplied
            else existing_ds.get("connection_url")
        )
        dependencies.validate_mcp_datasource(effective_url, effective_credentials)
        transport = (effective_credentials.get("transport") or "http").lower()
        if transport == "stdio":
            connection_url = None
            mcp_connection_url_set = bool(
                url_was_supplied or existing_ds.get("connection_url") is not None
            )
    elif (existing_ds.get("type") or "") == "repository":
        if datasource_config is not None:
            effective_url = body.connection_url or existing_ds.get("connection_url")
            datasource_config = normalize_repository_config(
                datasource_config, effective_url
            )
    elif existing_ds.get("type") == "ssh_key":
        pass  # host/user/port/known_hosts, validated with the key below
    elif datasource_config:
        raise HTTPException(
            status_code=400,
            detail=(
                "Connector config is only supported for OKF Knowledge Bases "
                "and email connectors"
            ),
        )
    if existing_ds.get("type") in WORKSPACE_SSH_CONNECTOR_TYPES and (
        datasource_config is not None
        or credentials is not None
        or "connection_url" in body.model_fields_set
    ):
        # Validate the EFFECTIVE endpoint: a URL-only edit of an SSH-key
        # repository moves the host its stored key and pins are used for. A
        # preserved (None) key was checked when it was stored.
        effective_config = datasource_config
        if effective_config is None:
            effective_config = existing_ds.get("config") or {}
            if isinstance(effective_config, str):
                try:
                    effective_config = json.loads(effective_config)
                except ValueError:
                    effective_config = {}
        effective_credentials = (
            credentials
            if credentials is not None
            else (existing_ds.get("credentials") or {})
        )
        if (
            datasource_config is None
            and existing_ds.get("type") == "repository"
            and not repository_uses_ssh_key(effective_credentials)
        ):
            # A switch to token auth leaves a stored pin unread, not invalid.
            effective_config = {
                key: value
                for key, value in effective_config.items()
                if key != "known_hosts"
            }
        try:
            checked_config = validate_workspace_ssh_connector(
                existing_ds["type"],
                connection_url=body.connection_url or existing_ds.get("connection_url"),
                config=effective_config,
                credentials=effective_credentials,
                check_key=credentials is not None,
            )
        except WorkspaceSshConnectorError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if datasource_config is not None:
            datasource_config = checked_config
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
                connection_url_set=mcp_connection_url_set,
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
            if mcp_connection_url_set:
                update_kwargs["connection_url_set"] = True
            if scope_project_id:
                update_kwargs["authority_project_scope_id"] = str(scope_project_id)
            success = await dependencies.store.update_datasource(**update_kwargs)
            if not success:
                raise HTTPException(
                    status_code=404, detail=f"Connector '{datasource_id}' not found"
                )

        if existing_ds.get("type") == "kb" and reindex_required:
            await knowledge_index.mark_kb_datasource_pending(
                datasource_id, dependencies=dependencies.knowledge_index
            )
            knowledge_index.schedule_kb_datasource_reindex(
                datasource_id,
                force_full=True,
                dependencies=dependencies.knowledge_index,
            )

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
    """Delete a connector. F3: creator/admin only."""
    from orchestrator.services.kb_datasources import native_kb_project_id

    if native_kb_project_id(datasource):
        raise HTTPException(
            status_code=409,
            detail="The project knowledge connector is managed by its project",
        )
    await require_scoped_datasource_mutation(
        user, datasource, datasource_id, dependencies=dependencies
    )
    scope_project_id = mcp_scope_project_id(user)
    try:
        if datasource.get("type") == "kb":
            success = await knowledge_index.delete_kb_datasource_with_index(
                datasource_id,
                authority_project_scope_id=(
                    str(scope_project_id) if scope_project_id else None
                ),
                deleted_by=str(user["id"]),
                dependencies=dependencies.knowledge_index,
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


async def get_datasource_index_status(
    *,
    datasource: dict[str, Any],
    datasource_id: str,
    dependencies: DatasourceDependencies,
) -> dict[str, Any]:
    """Return credential-free indexing state for an OKF KB connector."""
    if datasource.get("type") != "kb":
        raise HTTPException(
            status_code=400, detail="Connector is not an OKF Knowledge Base"
        )
    try:
        from shared.runtime.services.knowledge_store import KnowledgeStore

        from orchestrator.services.kb_datasources import (
            index_status_payload,
            native_kb_project_id,
        )

        watermark_id = native_kb_project_id(datasource) or datasource_id
        watermark = await KnowledgeStore(
            db=dependencies.vector_db, embedding_service=None
        ).get_watermark(UUID(watermark_id))
        return index_status_payload(datasource_id, watermark)
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
    from orchestrator.services.kb_datasources import native_kb_project_id

    if datasource.get("type") != "kb":
        raise HTTPException(
            status_code=400, detail="Connector is not an OKF Knowledge Base"
        )
    if native_kb_project_id(datasource):
        # Indexing it here would write its project's notes a second time under
        # this datasource's id and duplicate every search hit.
        raise HTTPException(
            status_code=400,
            detail=(
                "This connector mirrors the project's own knowledge base; "
                "reindex it from the project instead"
            ),
        )
    try:
        return await knowledge_index.reindex_kb_datasource_now(
            datasource, force_full=full, dependencies=dependencies.knowledge_index
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# =============================================================================
# Connectivity probes
# =============================================================================


async def test_mcp_datasource(
    connection_url: str | None,
    credentials: dict[str, Any],
) -> dict[str, Any]:
    """Connect and list MCP tools with a ten-second overall bound."""
    import shutil
    from contextlib import AsyncExitStack

    transport = str(credentials.get("transport") or "http").lower()
    if transport == "stdio" and not shutil.which(credentials.get("command") or ""):
        return {
            "status": "ok",
            "message": (
                "stdio server untested here (runtime not on the orchestrator); "
                "it will resolve on the agent at job start"
            ),
        }

    async def _probe() -> dict[str, Any]:
        from shared.mcp_sdk import ensure_mcp_sdk

        ensure_mcp_sdk()
        async with AsyncExitStack() as stack:
            if transport == "stdio":
                from mcp import StdioServerParameters
                from mcp.client.stdio import get_default_environment, stdio_client

                parameters = StdioServerParameters(
                    command=credentials["command"],
                    args=credentials.get("args") or [],
                    env={
                        **get_default_environment(),
                        **dict(credentials.get("env") or {}),
                    },
                )
                # Never forward third-party stderr: a server may print its
                # credential-bearing environment.
                error_sink = stack.enter_context(open(os.devnull, "w"))
                read, write = await stack.enter_async_context(
                    stdio_client(parameters, errlog=error_sink)
                )
            else:
                headers: dict[str, str] = {}
                auth = credentials.get("auth") or {}
                if auth.get("type") == "bearer":
                    headers["Authorization"] = f"Bearer {auth['token']}"
                elif auth.get("type") == "headers":
                    headers.update(auth.get("headers") or {})

                if transport == "sse":
                    from mcp.client.sse import sse_client

                    read, write = await stack.enter_async_context(
                        sse_client(connection_url, headers=headers or None)
                    )
                else:
                    from mcp.client import streamable_http

                    http_transport = getattr(
                        streamable_http,
                        "streamable_http_client",
                        None,
                    )
                    if http_transport is not None:
                        from mcp.shared._httpx_utils import create_mcp_http_client

                        http_client = await stack.enter_async_context(
                            create_mcp_http_client(headers=headers or None)
                        )
                        transport_context = http_transport(
                            connection_url,
                            http_client=http_client,
                        )
                    else:
                        transport_context = streamable_http.streamablehttp_client(
                            connection_url,
                            headers=headers or None,
                        )
                    read, write, _ = await stack.enter_async_context(transport_context)

            from mcp import ClientSession

            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            listing = await session.list_tools()
            names = [tool.name for tool in listing.tools]
            preview = ", ".join(names[:8])
            if len(names) > 8:
                preview += ", …"
            suffix = f" ({preview})" if preview else ""
            return {
                "status": "ok",
                "message": f"Connected: {len(names)} tools{suffix}",
            }

    try:
        return await asyncio.wait_for(_probe(), timeout=10)
    except TimeoutError:
        return {"status": "error", "message": "MCP connect timed out after 10s"}
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # Transport exceptions can contain URLs/headers. Report only the class.
        return {
            "status": "error",
            "message": f"MCP connection failed ({type(exc).__name__})",
        }


async def test_repository_datasource(
    ds: dict[str, Any], url: str | None, creds: dict[str, Any]
) -> dict[str, Any]:
    """Probe a token-authenticated repository connector without exposing the token.

    Reports the principal the agent will act as, its permission on the
    repository, the token class (GitHub only), and the repository's default
    branch, with warnings for the two configurations that silently defeat
    the guardrails: an administrator token (bypasses branch rules) and a
    connector that is not read-only but cannot push. SSH-key connectors have
    no API to ask: Test reaches their SSH endpoint and reports its host key
    (``workspace_ssh_connector.probe_workspace_ssh_connector``); the clone at
    job start proves the key.
    """
    from shared.runtime.services.forge import (  # noqa: PLC0415
        ForgeError,
        ForgeRepo,
        parse_owner_repo,
        probe_repository_access,
        resolve_api_base,
    )

    token = str(creds.get("token") or "")
    auth_method = str(creds.get("auth_method") or "").lower()
    if not auth_method:
        auth_method = "ssh" if creds.get("ssh_key") else ("token" if token else "")
    if auth_method == "ssh":
        # No forge API takes a deploy key; reach the SSH endpoint and report
        # the host key the connector form offers to pin.
        probed = await probe_workspace_ssh_connector({**ds, "credentials": creds})
        if probed is not None:
            return probed
    if auth_method != "token" or not token:
        return {
            "status": "ok",
            "message": (
                "No API probe for SSH-key repository connectors; "
                "the clone at job start is the test"
            ),
        }

    config = ds.get("config") or {}
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except ValueError:
            config = {}
    try:
        forge = normalize_repository_config(config, url)["forge"]
        owner, repo = parse_owner_repo(url or "")
        target = ForgeRepo(
            forge=forge,
            api_base=resolve_api_base(url or "", forge),
            owner=owner,
            repo=repo,
            token=token,
        )
    except HTTPException as exc:
        return {"status": "error", "message": str(exc.detail)}
    except ForgeError as exc:
        return {"status": "error", "message": str(exc)}

    try:
        facts = await asyncio.wait_for(probe_repository_access(target), timeout=15)
    except asyncio.TimeoutError:
        return {"status": "error", "message": "Repository probe timed out after 15s"}
    except ForgeError as exc:
        return {"status": "error", "message": str(exc)}

    warnings = list(facts.get("warnings") or [])
    if not facts["can_write"] and not ds.get("read_only"):
        warnings.append(
            f"{facts['principal'] or 'the token'} cannot push to {owner}/{repo} "
            "but the connector is not marked read-only"
        )
    configured_branch = str(ds.get("default_branch") or "")
    repo_default = facts.get("default_branch")
    branch_note = ""
    if repo_default:
        branch_note = f"; repository default branch {repo_default}"
        if configured_branch and configured_branch != repo_default:
            branch_note += f" (connector targets {configured_branch})"

    token_label = (
        f"{facts['token_class']} token"
        if facts["token_class"] != "unknown"
        else "token"
    )
    access = "write" if facts["can_write"] else "read-only"
    message = (
        f"Authenticated as {facts['principal'] or 'unknown principal'} "
        f"({token_label}); {access} access to {owner}/{repo}{branch_note}"
    )
    if warnings:
        message += " — WARNING: " + "; ".join(warnings)
    return {
        "status": "ok",
        "message": message,
        "details": {**facts, "warnings": warnings},
    }


def _probe_failure(message: str, ds_type: str) -> dict[str, Any]:
    """Report a failed connectivity probe without disclosing the exception.

    A driver's exception text routinely carries the connection URL (with its
    password), internal hostnames and ports, so it is logged rather than
    returned. ``error_ref`` — the same 12-hex-char shape the request-id
    middleware uses — is what an operator quotes to find that log line.
    """
    error_ref = uuid4().hex[:12]
    logger.exception(
        "Connectivity probe failed for a %s connector (error_ref=%s)",
        ds_type,
        error_ref,
    )
    return {"status": "error", "message": message, "error_ref": error_ref}


async def test_datasource(
    *,
    resolve_datasource: ResolveDatasourceOwner,
    dependencies: DatasourceDependencies,
) -> dict[str, Any]:
    """Test connectivity to a connector.

    Attempts to connect using the stored connection details and returns
    the result. Does not modify any data. F3: creator/admin only (test
    uses live credentials and probes the target).
    """
    try:
        _, ds = await resolve_datasource()
        ds_type = ds["type"]
        url = ds["connection_url"]
        creds = ds.get("credentials") or {}
        if isinstance(creds, str):
            creds = json.loads(creds)

        if ds_type == "kb":
            from orchestrator.services.kb_datasources import (
                test_kb_datasource as _test_kb,
            )

            try:
                return await _test_kb(ds)
            except Exception:
                return _probe_failure("Knowledge base probe failed", ds_type)

        if ds_type == "mcp":
            if not dependencies.mcp_datasources_enabled():
                raise HTTPException(
                    status_code=403,
                    detail="MCP connectors are disabled on this deployment",
                )
            dependencies.validate_mcp_datasource(url, creds)
            return await test_mcp_datasource(url, creds)

        if ds_type == "postgresql":
            try:
                conn = await asyncpg.connect(url, timeout=10)
                version = await conn.fetchval("SELECT version()")
                await conn.close()
                return {"status": "ok", "message": f"Connected: {version[:80]}"}
            except Exception:
                return _probe_failure("PostgreSQL connection failed", ds_type)

        elif ds_type == "neo4j":
            try:
                from neo4j import GraphDatabase

                username = creds.get("username", "neo4j")
                password = creds.get("password", "")
                driver = GraphDatabase.driver(url, auth=(username, password))
                driver.verify_connectivity()
                driver.close()
                return {"status": "ok", "message": "Connected to Neo4j"}
            except Exception:
                return _probe_failure("Neo4j connection failed", ds_type)

        elif ds_type == "mongodb":
            try:
                from pymongo import MongoClient

                client = MongoClient(url, serverSelectionTimeoutMS=5000)
                client.server_info()
                client.close()
                return {"status": "ok", "message": "Connected to MongoDB"}
            except Exception:
                return _probe_failure("MongoDB connection failed", ds_type)

        elif ds_type == "webdav":
            try:
                from webdav3.client import Client as WebDAVClient

                client = WebDAVClient(
                    {
                        "webdav_hostname": url,
                        "webdav_login": creds.get("username"),
                        "webdav_password": creds.get("password"),
                    }
                )
                client.list("/")
                return {"status": "ok", "message": "Connected to WebDAV"}
            except Exception:
                return _probe_failure("WebDAV connection failed", ds_type)

        elif ds_type == "email":
            # Blocking imaplib/smtplib probe runs off the event loop (the
            # sync sibling branches above block it — don't copy that).
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(
                        probe_email_connection, creds, ds.get("config") or {}
                    ),
                    timeout=10,
                )
            except asyncio.TimeoutError:
                return {
                    "status": "error",
                    "message": "IMAP/SMTP connectivity test timed out after 10s",
                }
            except Exception:
                return _probe_failure("IMAP/SMTP connection failed", ds_type)

        elif ds_type == "repository":
            return await test_repository_datasource(ds, url, creds)

        elif ds_type == "credentials":
            normalize_credential_env(creds.get("env_vars", {}), required=True)
            return {
                "status": "ok",
                "message": "Credential variables are valid; provider access is tested in the workspace",
            }
        elif ds_type == "generic":
            return {
                "status": "ok",
                "message": "No connectivity test for generic connectors",
            }
        elif (
            ds_type == "ssh_key"
            and (probed := await probe_workspace_ssh_connector(ds)) is not None
        ):
            # Only a declared host has an endpoint to test.
            return probed

        else:
            return {"status": "error", "message": f"Unknown connector type: {ds_type}"}

    except HTTPException:
        raise
    except Exception as e:
        error_ref = uuid4().hex[:12]
        logger.exception("Connector test failed (error_ref=%s)", error_ref)
        raise HTTPException(
            status_code=500,
            detail=f"Connector test failed (error_ref={error_ref})",
        ) from e
