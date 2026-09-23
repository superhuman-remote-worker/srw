"""MCP server exposing debug cockpit tools.

Provides tools to inspect agent jobs, audit trails, todos, and graph changes
via the Model Context Protocol using FastMCP.
"""

from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
import functools
import hashlib
from importlib.metadata import PackageNotFoundError, version
import json
import os
from pathlib import Path
import platform
from typing import Any, Literal
from uuid import UUID

from fastmcp import FastMCP
from starlette.responses import JSONResponse

# Shared-surface imports resolve through the installed package in every environment.
from shared.expert_reference import (
    ExpertReferenceConflict,
    resolve_expert_selection,
)
from shared.orch_surface import formatters as fmt
from shared.orch_surface.client import AsyncCockpitClient, MutationOutcomeUnknown
from shared.orch_surface.jobs import AUTH_CONTEXT_FAILURE_NOTICE, CallerCtx
from shared.sudo_command_line import render_sudo_command_line

DatasourceType = Literal[
    "generic",
    "repository",
    "kb",
    "postgresql",
    "neo4j",
    "mongodb",
    "webdav",
    "email",
    "mcp",
    "kubeconfig",
    "ssh_key",
    "generic_file",
]
DatasourceScopeMode = Literal["all", "projects"]
DatasourceVisibility = Literal["public", "private"]
DatasourceOwnership = Literal["mine", "shared"]
DatasourceAvailability = Literal["all", "projects", "unavailable"]

from mcp_server.capabilities import TOOL_CAPABILITIES  # noqa: E402

# Conditional auth: HTTP transport uses token verification, stdio skips it
_transport = os.environ.get("MCP_TRANSPORT", "http").lower()
_auth = None
if _transport == "http":
    from mcp_server.auth import McpTokenVerifier

    _token_verifier = McpTokenVerifier()

    if os.environ.get("MCP_OAUTH_ENABLED", "").lower() == "true":
        from mcp_server.oauth_bridge import SRWOAuthProxy

        _base_url = os.environ.get("MCP_BASE_URL", "http://localhost:8055")
        _auth = SRWOAuthProxy(
            config_url=os.environ.get(
                "MCP_OIDC_CONFIG_URL",
                "http://keycloak:8080/realms/srw/.well-known/openid-configuration",
            ),
            client_id=os.environ["MCP_OIDC_CLIENT_ID"],
            client_secret=os.environ["MCP_OIDC_CLIENT_SECRET"],
            base_url=_base_url,
            # issuer_url = base_url (the proxy IS the authorization server
            # from the client's perspective; Keycloak is only used internally
            # via config_url for OIDC discovery)
            mcp_verifier=_token_verifier,
            verify_id_token=True,  # Keycloak access tokens may be opaque
        )
    else:
        _auth = _token_verifier

# Create the MCP server instance
mcp = FastMCP("cockpit-debug", auth=_auth)


def mcp_tool(function):
    """Register a tool using its authoritative capability contract."""
    contract = TOOL_CAPABILITIES.get(function.__name__)
    if contract is None:
        raise RuntimeError(
            f"MCP tool {function.__name__!r} has no capability contract entry"
        )

    @functools.wraps(function)
    async def scoped_invocation(*args: Any, **kwargs: Any):
        client = _get_client()
        caller = _get_mcp_caller_ctx()
        scope_manager = (
            client.invocation_scope(
                user_id=caller.user_id,
                scope=caller.scope_header,
                unauthenticated=caller.auth_failed,
            )
            if isinstance(client, AsyncCockpitClient)
            else nullcontext()
        )
        with scope_manager:
            if not caller.auth_failed:
                return await function(*args, **kwargs)
            # http-mode auth context failed: the binding above carries no
            # identity headers at all (never the internal key), so guarded
            # endpoints 401. Lead the tool result with the real cause.
            try:
                outcome = await function(*args, **kwargs)
            except Exception as error:
                outcome = f"{type(error).__name__}: {error}"
            return f"{AUTH_CONTEXT_FAILURE_NOTICE}\n{outcome}"

    return mcp.tool(
        scoped_invocation,
        annotations=contract.annotations,
        meta={"io.srw.capability": contract.metadata()},
    )


# Global client instance (initialized lazily)
_client: AsyncCockpitClient | None = None


def _get_client() -> AsyncCockpitClient:
    """Get or create the process-wide async client instance."""
    global _client
    if _client is None:
        _client = AsyncCockpitClient()
    return _client


def _get_mcp_caller_ctx() -> CallerCtx:
    """Translate the authenticated token into trusted hidden caller context.

    Two deliberately different anonymous shapes:

    * stdio transport is the documented internal mode (docker/Dockerfile.mcp):
      there is no token middleware, and requests authenticate with
      ``MCP_INTERNAL_KEY`` alone. That is its contract and it stays.
    * http transport has exactly one identity source — the verified bearer
      token. Any failure to resolve it (middleware error OR missing token)
      yields ``auth_failed=True``: the invocation is bound with NO identity
      headers (the internal key is never an error fallback), guarded
      orchestrator endpoints 401 — the pre-unification fail-closed
      behavior — and the tool result names the auth context failure.
    """
    if _transport != "http":
        return CallerCtx(kind="mcp")
    try:
        from mcp.server.auth.middleware.auth_context import get_access_token

        token = get_access_token()
        if not token:
            return CallerCtx(kind="mcp", auth_failed=True)
        scopes = tuple(str(scope) for scope in (token.scopes or ()) if scope)
        project_ids = tuple(
            dict.fromkeys(
                scope.split(":", 1)[1]
                for scope in scopes
                if scope.startswith("project:") and scope.split(":", 1)[1]
            )
        )
        explicit_scope = scopes[0] if len(scopes) == 1 else None
        lineage_project_id = project_ids[0] if len(project_ids) == 1 else None
        return CallerCtx(
            kind="mcp",
            user_id=str(token.client_id),
            project_ids=project_ids,
            lineage_project_id=lineage_project_id,
            explicit_scope=explicit_scope,
        )
    except Exception:
        return CallerCtx(kind="mcp", auth_failed=True)


def _format_action_error(action: str, target: str, error: Exception) -> str:
    """Keep an ambiguous mutation distinct from a confirmed failure."""
    if isinstance(error, MutationOutcomeUnknown):
        return f"Action '{action}' has an unknown outcome for {target}:\n{error}"
    return fmt.format_action_error(action, target, error)


# =============================================================================
# Health Check Endpoint
# =============================================================================

# Bump when an MCP tool signature or meaning changes in a way that callers need
# to distinguish from a cached/deployed schema. Build provenance says which
# source produced the pod; this small contract revision says which tool surface
# that source promises.
# "9": officer_supervision_surface E1-E4 — three job_evidence tools,
# caller-aware get_stuck_jobs threshold, liveness-backed progress output.
# "10": officer_message_routing M3 — reply_to_job_message,
# escalate_job_message, acknowledge_job_message (officer inbox actions).
# "11": officer_legate_channel — list_officers, get_project_officer,
# send_officer_note (the Legate's side), plus newest_first on
# get_persistent_thread_messages.
# "12": one expert selector — create_job / create_project_job take `expert`
# (a bundled expert id or a DB expert UUID, exactly as list_experts prints
# it); config_name and expert_id stay as deprecated single-store aliases.
MCP_TOOL_SCHEMA_REVISION = "13"
_tool_schema_cache: tuple[list[dict[str, Any]], str] | None = None


async def canonical_tool_schema() -> tuple[list[dict[str, Any]], str]:
    """Return canonical raw ``tools/list`` material and its SHA-256 digest.

    The middleware chain runs because it is part of what a client receives:
    FastMCP enables ``DereferenceRefsMiddleware`` by default, which inlines
    ``$defs``/``$ref`` on the way out for clients that cannot follow them.
    Bypassing it was invisible while every tool returned a plain ``str`` and
    no schema carried a ``$ref``; the first descriptor with a model in its
    return type (``read_job_evidence`` -> ``JobToolResult``) made this artifact
    disagree with two fresh clients and failed the image smoke at build time.
    """
    global _tool_schema_cache
    if _tool_schema_cache is None:
        registered = await mcp.list_tools()
        tools = [
            tool.to_mcp_tool(name=tool.name).model_dump(
                mode="json", by_alias=True, exclude_none=True
            )
            for tool in registered
        ]
        tools.sort(key=lambda tool: tool["name"])
        canonical = json.dumps(
            {"tools": tools},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        _tool_schema_cache = (tools, f"sha256:{hashlib.sha256(canonical).hexdigest()}")
    return _tool_schema_cache


async def _mcp_build_info() -> dict[str, str | int]:
    tools, schema_digest = await canonical_tool_schema()
    try:
        fastmcp_version = version("fastmcp")
    except PackageNotFoundError:
        fastmcp_version = "unavailable"
    try:
        mcp_sdk_version = version("mcp")
    except PackageNotFoundError:
        mcp_sdk_version = "unavailable"
    schema_artifact_path = os.environ.get("MCP_SCHEMA_ARTIFACT")
    schema_artifact_digest = "unavailable"
    schema_artifact_status = "not_configured"
    if schema_artifact_path:
        try:
            artifact = json.loads(
                Path(schema_artifact_path).read_text(encoding="utf-8")
            )
            schema_artifact_digest = str(artifact.get("digest") or "unavailable")
            schema_artifact_status = (
                "match" if artifact.get("tools") == tools else "mismatch"
            )
        except (OSError, ValueError, TypeError):
            schema_artifact_status = "invalid_or_missing"
    return {
        "tool_schema_revision": MCP_TOOL_SCHEMA_REVISION,
        "tool_schema_digest": schema_digest,
        "tool_count": len(tools),
        "source_revision": os.environ.get("SRW_SOURCE_REVISION") or "unavailable",
        "release_version": os.environ.get("SRW_RELEASE_VERSION") or "development",
        "artifact_digest": os.environ.get("SRW_ARTIFACT_DIGEST") or "unavailable",
        "schema_artifact_digest": schema_artifact_digest,
        "schema_artifact_status": schema_artifact_status,
        "python_version": platform.python_version(),
        "fastmcp_version": fastmcp_version,
        "mcp_sdk_version": mcp_sdk_version,
    }


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request):
    """Kubernetes health probe endpoint."""
    build_info = await _mcp_build_info()
    if build_info["schema_artifact_status"] not in {"match", "not_configured"}:
        return JSONResponse(
            {
                "status": "degraded",
                "error": "runtime tools/list does not match the image schema artifact",
                **build_info,
            },
            status_code=503,
        )
    try:
        client = _get_client()
        await client.health_check()
        return JSONResponse({"status": "healthy", "backend": "connected", **build_info})
    except Exception as e:
        return JSONResponse(
            {"status": "degraded", "error": str(e), **build_info},
            status_code=503,
        )


# =============================================================================
# MCP Tools
# =============================================================================

# Job operations are the only descriptor-backed slice. Every other MCP tool in
# this module remains hand-written and behaviorally unchanged.
try:
    from mcp_server.job_adapter import register_job_tools
except ImportError:
    from job_adapter import register_job_tools  # type: ignore[no-redef]

_REGISTERED_JOB_TOOLS = register_job_tools(
    mcp,
    client_provider=lambda: _get_client(),
    caller_provider=lambda: _get_mcp_caller_ctx(),
    capabilities=TOOL_CAPABILITIES,
)
globals().update(_REGISTERED_JOB_TOOLS)


@mcp_tool
async def get_graph_changes(job_id: str) -> str:
    """Get timeline of Neo4j graph mutations for a job.

    Returns parsed Cypher queries showing nodes/relationships
    created, modified, and deleted. Includes summary statistics.

    Args:
        job_id: Job UUID to get graph changes for

    Returns:
        Formatted graph changes timeline with summary
    """
    client = _get_client()
    changes = await client.get_graph_changes(job_id)
    return fmt.format_graph_changes(changes)


# =============================================================================
# Action & Operations Tools (Category A)
# =============================================================================


@mcp_tool
async def test_datasource(datasource_id: str) -> str:
    """Test connectivity to a connector.

    Attempts to connect to the connector using stored connection details.
    Supports PostgreSQL, Neo4j, and MongoDB. Does not modify any data.

    Args:
        datasource_id: Connector UUID to test

    Returns:
        Test result with status and connection details
    """
    client = _get_client()
    try:
        result = await client.test_datasource(datasource_id)
        return fmt.format_datasource_test(datasource_id, result)
    except Exception as e:
        return _format_action_error("test_datasource", datasource_id, e)


# =============================================================================
# Git History Tools (Category B)
# =============================================================================


@mcp_tool
async def list_job_tags(job_id: str) -> str:
    """List phase tags to understand the job's phase history.

    Shows tags like phase_1_start, phase_1_end, phase_2_start, etc.
    Use these tag names as refs in other git tools.

    Args:
        job_id: Job UUID

    Returns:
        List of tags with name and commit SHA, sorted chronologically
    """
    client = _get_client()
    try:
        tags = await client.list_job_tags(job_id)
        return fmt.format_tags(job_id, tags)
    except Exception as e:
        return fmt.format_git_error("list tags", job_id, e)


# =============================================================================
# Workspace & Job Context Tools (Category C)
# =============================================================================


@mcp_tool
async def get_workspace_file(job_id: str, path: str) -> str:
    """Read a file from the job's workspace repo (Gitea-backed).

    Returns committed state as of the worker's last phase-boundary push —
    workers push at every phase boundary, freeze, and finalize, so mid-phase
    edits are not visible yet. Reads the job branch head; use get_job_file
    with a ref to read a phase tag instead.

    Args:
        job_id: Job UUID
        path: Relative path within the workspace repo (e.g., "plan.md",
              "notes/decisions.md", "archive/phase_1_retrospective.md")

    Returns:
        File content as text
    """
    client = _get_client()
    try:
        result = await client.get_workspace_file(job_id, path)
        content = result.get("content", "")
        return f"Workspace file: {path} (job {job_id})\n---\n{content}"
    except Exception as e:
        return fmt.format_workspace_error(f"read workspace file '{path}'", job_id, e)


# =============================================================================
# System Monitoring Tools (Category D)
# =============================================================================


@mcp_tool
async def get_job_stats() -> str:
    """Get job queue statistics with counts by status.

    Returns:
        Total jobs and counts per status (created, processing, completed, etc.)
    """
    client = _get_client()
    try:
        data = await client.get_job_stats()
        return fmt.format_job_stats(data)
    except Exception as e:
        return fmt.format_monitoring_error("get job stats", e)


@mcp_tool
async def get_agent_stats() -> str:
    """Get agent workforce summary with counts by status.

    Returns:
        Total agents and counts per status (ready, working, offline, etc.)
    """
    client = _get_client()
    try:
        data = await client.get_agent_stats()
        return fmt.format_agent_stats(data)
    except Exception as e:
        return fmt.format_monitoring_error("get agent stats", e)


@mcp_tool
async def list_agents(status: str | None = None) -> str:
    """List registered agents with status and current assignment.

    Args:
        status: Filter by status (booting, ready, working, completed, failed, offline)

    Returns:
        Agent list with ID, config, hostname, status, current job, last heartbeat
    """
    client = _get_client()
    try:
        agents = await client.list_agents(status=status)
        return fmt.format_agents(agents, status_filter=status)
    except Exception as e:
        return fmt.format_monitoring_error("list agents", e)


@mcp_tool
async def list_experts() -> str:
    """List available expert/agent configurations.

    Returns:
        Expert configs with ID, display name, description, and tags
    """
    client = _get_client()
    try:
        experts = await client.list_experts()
        return fmt.format_experts(experts)
    except Exception as e:
        return fmt.format_monitoring_error("list experts", e)


@mcp_tool
async def get_expert(expert_id: str) -> str:
    """Get full detail for an expert config including merged config and instructions.

    Args:
        expert_id: Expert config ID (e.g., "general-worker", "researcher")

    Returns:
        Full config detail with system prompt, tool list, and instructions
    """
    client = _get_client()
    try:
        data = await client.get_expert(expert_id)
        return fmt.format_expert_detail(expert_id, data)
    except Exception as e:
        return fmt.format_monitoring_error(f"get expert '{expert_id}'", e)


def _manifest_scope(scope_kind, scope_name):
    if (scope_kind is None) != (scope_name is None):
        raise ValueError("scope_kind and scope_name must be supplied together")
    return {"kind": scope_kind, "name": scope_name} if scope_kind else None


@mcp_tool
async def manifest_validate(
    source: str, format: Literal["yaml", "json"] = "yaml"
) -> str:
    """Validate SRW manifest JSON/YAML without creating resources or work.

    Expert, WorkspaceTemplate, Connector, Project and Job use the SRW resource
    schema. Opaque harness configuration and literal nulls are preserved.
    """
    try:
        return json.dumps(
            await _get_client().manifest_validate(source, format=format),
            indent=2,
            ensure_ascii=False,
        )
    except Exception as error:
        return fmt.format_monitoring_error("validate manifests", error)


@mcp_tool
async def manifest_preview(
    source: str,
    format: Literal["yaml", "json"] = "yaml",
    scope_kind: Literal["Account", "Project", "Catalog"] | None = None,
    scope_name: str | None = None,
    resolution: Literal["stored", "bundle"] = "stored",
) -> str:
    """Preview manifests with authorized stored references or only this bundle.

    Stored preview returns planRevision for an optional later apply precondition.
    Preview has no resource/execution side effects; pending admission checks are
    reported explicitly. Scope names never grant access by themselves.
    """
    try:
        result = await _get_client().manifest_preview(
            source,
            format=format,
            default_scope=_manifest_scope(scope_kind, scope_name),
            resolution=resolution,
        )
        return json.dumps(result, indent=2, ensure_ascii=False)
    except Exception as error:
        return fmt.format_monitoring_error("preview manifests", error)


@mcp_tool
async def manifest_apply(
    source: str,
    format: Literal["yaml", "json"] = "yaml",
    scope_kind: Literal["Account", "Project", "Catalog"] | None = None,
    scope_name: str | None = None,
    expected_versions: dict[str, int] | None = None,
    plan_revision: str | None = None,
    idempotency_key: str | None = None,
) -> str:
    """Create/update authorized resources and admit Jobs in a manifest bundle.

    Applying a Job may start its selected image and external work. Existing
    resource updates require observed versions keyed by Kind/ScopeKind/ScopeName/name.
    plan_revision can bind a reviewed stored preview; idempotency_key can recover
    an identical apply request after a lost response. Mutations are never retried
    by this client. Read current resources after an unknown outcome.
    """
    try:
        result = await _get_client().manifest_apply(
            source,
            format=format,
            default_scope=_manifest_scope(scope_kind, scope_name),
            expected_versions=expected_versions,
            plan_revision=plan_revision,
            idempotency_key=idempotency_key,
        )
        return json.dumps(result, indent=2, ensure_ascii=False)
    except Exception as error:
        return fmt.format_monitoring_error("apply manifests", error)


@mcp_tool
async def manifest_list(
    scope_kind: Literal["Account", "Project", "Catalog"] = "Account",
    scope_name: str = "me",
    kind: Literal["Expert", "WorkspaceTemplate", "Connector", "Project", "Job"]
    | None = None,
) -> str:
    """List canonical manifest resources in an authorized account/project/catalog scope.

    Results include resource UID, authored configuration, and resourceVersion for
    optimistic updates. Stored credential values are never part of the response.
    """
    try:
        result = await _get_client().list_manifest_resources(
            scope_kind=scope_kind, scope_name=scope_name, kind=kind
        )
        return json.dumps(result, indent=2, ensure_ascii=False)
    except Exception as error:
        return fmt.format_monitoring_error("list manifest resources", error)


@mcp_tool
async def manifest_get(resource_id: str) -> str:
    """Get an authorized resource by UID, including authored spec and observed version/status."""
    try:
        return json.dumps(
            await _get_client().get_manifest_resource(resource_id),
            indent=2,
            ensure_ascii=False,
        )
    except Exception as error:
        return fmt.format_monitoring_error("get manifest resource", error)


@mcp_tool
async def manifest_export(
    resource_id: str, output_format: Literal["yaml", "json"] = "yaml"
) -> str:
    """Export one stored resource as portable authored JSON/YAML, preserving private config and refs."""
    try:
        result = await _get_client().export_manifest_resource(
            resource_id, output_format=output_format
        )
        return result["source"]
    except Exception as error:
        return fmt.format_monitoring_error("export manifest resource", error)


@mcp_tool
async def manifest_delete(resource_id: str, expected_version: int) -> str:
    """Delete an authorized resource only at the observed resourceVersion.

    Active execution dependencies and project ownership can prevent deletion.
    This is a resource operation; Job cancellation uses the existing cancel tool.
    """
    try:
        result = await _get_client().delete_manifest_resource(
            resource_id, expected_version=expected_version
        )
        return json.dumps(result, indent=2, ensure_ascii=False)
    except Exception as error:
        return fmt.format_monitoring_error("delete manifest resource", error)


@mcp_tool
async def list_skills() -> str:
    """List available agent skills (the catalog the agent selects from).

    Returns:
        Skills with id, name, description, and tags.
    """
    client = _get_client()
    try:
        skills = await client.list_skills()
        return fmt.format_skills(skills)
    except Exception as e:
        return fmt.format_monitoring_error("list skills", e)


@mcp_tool
async def get_skill(skill_id: str) -> str:
    """Get full detail for a skill including its SKILL.md body and file list.

    Args:
        skill_id: Skill id (bundled name or DB UUID).

    Returns:
        The skill's metadata, body, and bundled file paths.
    """
    client = _get_client()
    try:
        data = await client.get_skill(skill_id)
        return fmt.format_skill_detail(skill_id, data)
    except Exception as e:
        return fmt.format_monitoring_error(f"get skill '{skill_id}'", e)


@mcp_tool
async def reload_skills() -> str:
    """Force reload of bundled skills from disk.

    Returns:
        Reload confirmation with skill count.
    """
    client = _get_client()
    result = await client.reload_skills()
    return f"Skills reloaded ({result.get('count', 0)} bundled skills loaded)."


@mcp_tool
async def list_models() -> str:
    """List available AI models grouped by provider.

    Returns model IDs for use in create_job's config_override parameter,
    e.g. config_override={"llm": {"model": "<model_id>"}}.
    Also includes preset pairs (strategic + tactical models).

    Returns:
        Models grouped by provider with availability status, plus presets
    """
    client = _get_client()
    try:
        data = await client.list_models()
        return fmt.format_models(data)
    except Exception as e:
        return fmt.format_monitoring_error("list models", e)


@mcp_tool
async def list_datasources(
    ds_type: DatasourceType | None = None,
    q: str | None = None,
    project_id: str | None = None,
    scope_mode: DatasourceScopeMode = None,  # type: ignore[assignment]
    auto_attach: bool = None,  # type: ignore[assignment]
    visibility: DatasourceVisibility = None,  # type: ignore[assignment]
    ownership: DatasourceOwnership = None,  # type: ignore[assignment]
    availability: DatasourceAvailability = None,  # type: ignore[assignment]
    limit: int = 50,
    cursor: str | None = None,
) -> str:
    """List the authorized connector management catalog.

    Results contain complete connector and project UUIDs plus the current
    policy revision needed for safe scope/default updates. Pagination is
    cursor-based; pass the reported next cursor unchanged to continue.

    Args:
        ds_type: Filter by canonical connector type
        q: Search connector name or description
        project_id: Filter to connectors linked to this authorized project
        scope_mode: Filter by availability scope (all or projects)
        auto_attach: Filter by the owner's automatic-default preference
        visibility: Filter to public or private connectors
        ownership: Filter to connectors owned by the caller or shared with them
        availability: Filter by effective scope shape (all, projects, unavailable)
        limit: Page size from 1 to 100 (default 50)
        cursor: Opaque next cursor from the previous page

    Returns:
        Connector list with full IDs, policy revisions, and next cursor
    """
    client = _get_client()
    try:
        page = await client.list_datasources(
            ds_type=ds_type,
            q=q,
            project_id=project_id,
            scope_mode=scope_mode,
            auto_attach=auto_attach,
            visibility=visibility,
            ownership=ownership,
            availability=availability,
            limit=limit,
            cursor=cursor,
        )
        rendered = fmt.format_datasources(page.get("items") or [], type_filter=ds_type)
        return f"{rendered}\n\nNext cursor: {page.get('next_cursor') or 'none'}"
    except Exception as e:
        return fmt.format_monitoring_error("list connectors", e)


@mcp_tool
async def get_datasource(datasource_id: str) -> str:
    """Get one connector's exact management state.

    Use this before update_datasource to load the complete connector UUID,
    project scope, and current policy_revision. Credentials are always redacted
    by the orchestrator.

    Args:
        datasource_id: Complete connector UUID

    Returns:
        Connector detail with full IDs and current policy revision
    """
    client = _get_client()
    try:
        datasource = await client.get_datasource(datasource_id)
        return fmt.format_datasource_detail(datasource)
    except Exception as e:
        return fmt.format_monitoring_error("get connector", e)


# =============================================================================
# Database Inspection Tools (Category E)
# =============================================================================


@mcp_tool
async def list_tables() -> str:
    """List all database tables with row counts.

    Returns:
        Table names with row counts
    """
    client = _get_client()
    try:
        tables = await client.list_tables()
        return fmt.format_tables(tables)
    except Exception as e:
        return fmt.format_monitoring_error("list tables", e)


@mcp_tool
async def query_table(
    table_name: str,
    limit: int = 50,
    offset: int = 0,
) -> str:
    """Get paginated data from a database table.

    Args:
        table_name: Table name (e.g., jobs, requirements, citations)
        limit: Rows per page (default: 50, max: 500)
        offset: Pagination offset (default: 0)

    Returns:
        Row data with column names
    """
    if limit < 1:
        limit = 1
    elif limit > 500:
        limit = 500

    # Convert offset to page number (1-indexed)
    page = (offset // limit) + 1

    client = _get_client()
    try:
        data = await client.get_table_data(table_name, page=page, page_size=limit)
        return fmt.format_table_data(table_name, data)
    except Exception as e:
        return fmt.format_monitoring_error(f"query table '{table_name}'", e)


@mcp_tool
async def get_table_schema(table_name: str) -> str:
    """Get column definitions for a database table.

    Args:
        table_name: Table name

    Returns:
        Column names, types, nullable flags, and defaults
    """
    client = _get_client()
    try:
        columns = await client.get_table_schema(table_name)
        return fmt.format_table_schema(table_name, columns)
    except Exception as e:
        return fmt.format_monitoring_error(f"get schema for '{table_name}'", e)


# =============================================================================
# Citation & Source Library Tools (Category F)
# =============================================================================


@mcp_tool
async def list_job_sources(
    job_id: str | None = None,
    source_type: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> str:
    """List sources registered by a job (documents, websites, databases, custom artifacts).

    When job_id is omitted, returns sources across all jobs with their linked job IDs.

    Args:
        job_id: Job UUID (omit to query across all jobs)
        source_type: Filter by type (document, website, database, custom)
        limit: Max results (default: 50)
        offset: Pagination offset (default: 0)

    Returns:
        Source list with ID, type, name, identifier, and content preview
    """
    if limit < 1:
        limit = 1
    elif limit > 500:
        limit = 500

    client = _get_client()
    try:
        data = await client.list_job_sources(
            job_id=job_id, source_type=source_type, limit=limit, offset=offset
        )
        return fmt.format_sources(data, job_id=job_id, type_filter=source_type)
    except Exception as e:
        return fmt.format_citation_error("list sources", e, job_id=job_id)


@mcp_tool
async def get_source_detail(
    source_id: int,
    content_limit: int = 2000,
) -> str:
    """Get full detail for a single source including content, metadata, and content hash.

    Args:
        source_id: Source ID (integer)
        content_limit: Max characters of content to return (default: 2000, 0 for full)

    Returns:
        Source record with type, identifier, name, content, and metadata
    """
    client = _get_client()
    try:
        data = await client.get_source_detail(source_id, content_limit=content_limit)
        return fmt.format_source_detail(data)
    except Exception as e:
        return fmt.format_citation_error(f"get source {source_id}", e)


@mcp_tool
async def list_job_citations(
    job_id: str,
    source_id: int | None = None,
    verification_status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> str:
    """List all citations for a job with verification status.

    Args:
        job_id: Job UUID
        source_id: Filter by source ID
        verification_status: Filter by status (pending, verified, failed, unverified)
        limit: Max results (default: 50)
        offset: Pagination offset (default: 0)

    Returns:
        Citation list with claim, source, verification status, and confidence
    """
    if limit < 1:
        limit = 1
    elif limit > 500:
        limit = 500

    client = _get_client()
    try:
        data = await client.list_job_citations(
            job_id,
            source_id=source_id,
            verification_status=verification_status,
            limit=limit,
            offset=offset,
        )
        return fmt.format_citations(job_id, data, status_filter=verification_status)
    except Exception as e:
        return fmt.format_citation_error("list citations", e, job_id=job_id)


@mcp_tool
async def get_citation_detail(citation_id: int) -> str:
    """Get full citation record with source info, verification details, and locator.

    Args:
        citation_id: Citation ID (integer)

    Returns:
        Full citation with claim, quote, source, locator, verification, and reasoning
    """
    client = _get_client()
    try:
        data = await client.get_citation_detail(citation_id)
        return fmt.format_citation_detail(data)
    except Exception as e:
        return fmt.format_citation_error(f"get citation {citation_id}", e)


@mcp_tool
async def search_job_sources(
    job_id: str,
    query: str,
    mode: str = "keyword",
    source_type: str | None = None,
    tags: str | None = None,
    top_k: int = 10,
) -> str:
    """Search a job's source library using keyword search with evidence labels.

    Uses PostgreSQL full-text search to find matching source content.

    Args:
        job_id: Job UUID
        query: Natural language query or keywords
        mode: Search mode (currently only "keyword" supported)
        source_type: Filter by source type
        tags: Comma-separated tags (AND logic)
        top_k: Max results (default: 10)

    Returns:
        Search results with evidence labels (HIGH/MEDIUM/LOW) and snippets
    """
    if top_k < 1:
        top_k = 1
    elif top_k > 50:
        top_k = 50

    client = _get_client()
    try:
        data = await client.search_job_sources(
            job_id,
            query=query,
            mode=mode,
            source_type=source_type,
            tags=tags,
            top_k=top_k,
        )
        return fmt.format_source_search(data)
    except Exception as e:
        return fmt.format_citation_error("search sources", e, job_id=job_id)


@mcp_tool
async def get_source_annotations(
    job_id: str,
    source_id: int,
    annotation_type: str | None = None,
) -> str:
    """Get annotations (notes, highlights, summaries, questions, critiques) for a source.

    Args:
        job_id: Job UUID
        source_id: Source ID
        annotation_type: Filter by type (note, highlight, summary, question, critique)

    Returns:
        List of annotations with type, content, and page reference
    """
    client = _get_client()
    try:
        annotations = await client.get_source_annotations(
            job_id,
            source_id,
            annotation_type=annotation_type,
        )
        return fmt.format_annotations(
            job_id, source_id, annotations, type_filter=annotation_type
        )
    except Exception as e:
        return fmt.format_citation_error(
            f"get annotations for source {source_id}", e, job_id=job_id
        )


@mcp_tool
async def get_source_tags(job_id: str, source_id: int) -> str:
    """Get tags assigned to a source within a job.

    Args:
        job_id: Job UUID
        source_id: Source ID

    Returns:
        List of tag strings
    """
    client = _get_client()
    try:
        tags = await client.get_source_tags(job_id, source_id)
        return fmt.format_source_tags(job_id, source_id, tags)
    except Exception as e:
        return fmt.format_citation_error(
            f"get tags for source {source_id}", e, job_id=job_id
        )


@mcp_tool
async def get_citation_stats(job_id: str) -> str:
    """Get citation statistics for a job — counts by verification status, source type, confidence.

    Args:
        job_id: Job UUID

    Returns:
        Statistics overview with source and citation breakdowns
    """
    client = _get_client()
    try:
        data = await client.get_citation_stats(job_id)
        return fmt.format_citation_stats(job_id, data)
    except Exception as e:
        return fmt.format_citation_error("get citation stats", e, job_id=job_id)


@mcp_tool
async def get_memory_stats(job_id: str) -> str:
    """Get memory statistics for a job — counts by type, source channel, tokens, and access patterns.

    Args:
        job_id: Job UUID

    Returns:
        Formatted memory statistics overview
    """
    client = _get_client()
    try:
        data = await client.get_memory_stats(job_id)
        return fmt.format_memory_stats(job_id, data)
    except Exception as e:
        return fmt.format_citation_error("get memory stats", e, job_id=job_id)


@mcp_tool
async def get_agent_system_info(agent_id: str) -> str:
    """Get system information from an agent's container.

    Returns CPU, memory, disk usage, listening ports, top processes by memory,
    established network connections, and current agent state. Useful for
    monitoring what the agent is doing inside its container.

    Args:
        agent_id: Agent UUID

    Returns:
        Formatted system info with resource usage and process details
    """
    client = _get_client()
    try:
        data = await client.get_agent_system_info(agent_id)
        return fmt.format_system_info(agent_id, data)
    except Exception as e:
        error_msg = str(e)
        if hasattr(e, "response"):
            try:
                detail = e.response.json().get("detail", error_msg)  # type: ignore[union-attr]
                error_msg = detail
            except Exception:
                error_msg = f"HTTP {e.response.status_code}: {error_msg}"  # type: ignore[union-attr]
        return f"Failed to get system info for agent {agent_id}:\n{error_msg}"


@mcp_tool
async def get_daily_stats(days: int = 7) -> str:
    """Get daily job statistics for the past N days.

    Shows jobs created, completed, failed, and cancelled per day.
    Useful for tracking system activity trends.

    Args:
        days: Number of days to look back (1-90, default 7)

    Returns:
        Daily statistics table
    """
    client = _get_client()
    data = await client.get_daily_stats(days=days)
    return fmt.format_daily_stats(data, days)


@mcp_tool
async def reload_experts() -> str:
    """Force reload of expert configurations from disk.

    Use after modifying expert YAML files to pick up changes
    without restarting the orchestrator.

    Returns:
        Reload confirmation with expert count
    """
    client = _get_client()
    result = await client.reload_experts()
    count = result.get("count", 0)
    return f"Expert configurations reloaded ({count} experts loaded)."


@mcp_tool
async def deregister_agent(agent_id: str) -> str:
    """Remove an agent from the system.

    Use for cleaning up agents that are offline or no longer needed.
    Returns 404 if the agent doesn't exist.

    Args:
        agent_id: Agent UUID to deregister

    Returns:
        Deregistration confirmation
    """
    client = _get_client()
    result = await client.deregister_agent(agent_id)
    s = result.get("status", "unknown")
    return f"Agent {agent_id} deregistered ({s})."


# =============================================================================
# Logs, LLM Requests & Shell State Tools
# =============================================================================


@mcp_tool
async def get_thread_log(
    thread_id: str,
    lines: int = 100,
    grep: str | None = None,
    level: str | None = None,
) -> str:
    """Read the archived agent-pod log for a persistent session.

    Post-mortem debugging for sessions whose agent pod has been torn down
    (idle timeout, session end, crash + reap): the pod's full log is archived
    to S3 at deletion and served here, scoped to this thread's lines. Returns
    404 while the pod is still alive — the log is only archived at teardown.

    Args:
        thread_id: Thread UUID
        lines: Number of tail lines to return (max 1000, default 100)
        grep: Case-insensitive substring filter
        level: Log level filter (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Formatted log output with line count and filter info
    """
    if lines < 1:
        lines = 1
    elif lines > 1000:
        lines = 1000

    client = _get_client()
    try:
        data = await client.get_thread_logs(
            thread_id=thread_id, lines=lines, grep=grep, level=level
        )
        return fmt.format_thread_log(thread_id, data)
    except Exception as e:
        return fmt.format_workspace_error("get thread log", thread_id, e)


# =============================================================================
# Todo Archives & Current Todos
# =============================================================================


# =============================================================================
# Knowledge Base
# =============================================================================


@mcp_tool
async def get_knowledge_summary(project_id: str) -> str:
    """Get knowledge base summary statistics for a project.

    Shows total notes, counts by type and status, and the 5 most
    recently modified notes.

    Args:
        project_id: Project UUID

    Returns:
        Formatted knowledge base summary
    """
    client = _get_client()
    data = await client.get_knowledge_summary(project_id)
    return fmt.format_knowledge_summary(project_id, data)


@mcp_tool
async def list_knowledge_notes(
    project_id: str,
    note_type: str | None = None,
    status: str | None = None,
    tag: str | None = None,
    job_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> str:
    """List knowledge notes for a project with optional filters.

    Shows note previews with type, status, tags, and confidence scores.
    Use get_knowledge_note to read full content of a specific note.

    Args:
        project_id: Project UUID
        note_type: Filter by type (insight, decision, pattern, issue, etc.)
        status: Filter by status (active, resolved, superseded, archived)
        tag: Filter by tag
        job_id: Filter by originating job UUID
        limit: Max results (1-200, default 50)
        offset: Pagination offset (default 0)

    Returns:
        Formatted note list with previews
    """
    client = _get_client()
    data = await client.list_knowledge_notes(
        project_id=project_id,
        note_type=note_type,
        status=status,
        tag=tag,
        job_id=job_id,
        limit=limit,
        offset=offset,
    )
    return fmt.format_knowledge_notes(data)


@mcp_tool
async def get_knowledge_note(project_id: str, note_id: str) -> str:
    """Get a single knowledge note with full content and relationships.

    Returns the complete note including content, metadata, tags, keywords,
    confidence score, and Neo4j graph relationships.

    Args:
        project_id: Project UUID
        note_id: Note ID

    Returns:
        Full note detail with content and relationships
    """
    client = _get_client()
    data = await client.get_knowledge_note(project_id, note_id)
    return fmt.format_knowledge_note_detail(data)


@mcp_tool
async def search_knowledge(
    project_id: str,
    query: str,
    limit: int = 10,
) -> str:
    """Search the project knowledge base using hybrid search.

    Uses dense vector + sparse keyword search when embeddings are
    available, falls back to keyword-only search otherwise. Use this
    to find relevant knowledge from previous jobs.

    Args:
        project_id: Project UUID
        query: Search query text
        limit: Max results (1-50, default 10)

    Returns:
        Ranked search results with note previews
    """
    client = _get_client()
    data = await client.search_knowledge(
        project_id=project_id,
        query=query,
        limit=limit,
    )
    return fmt.format_knowledge_search(data)


# =============================================================================
# Projects
# =============================================================================


@mcp_tool
async def list_projects(
    user_id: str | None = None, include_archived: bool = False
) -> str:
    """List ACTIVE projects, optionally filtered by user membership.

    Shows project name, status, goal, and last update time. Archived
    projects are excluded by default — they are a historical record, and
    dispatching work or commissioning an officer against one is almost
    always a mistake. The count of hidden archives is reported in the
    footer, so nothing disappears silently.

    Args:
        user_id: Filter to projects this user belongs to (optional)
        include_archived: Also list archived projects, each marked
            [ARCHIVED]. Use when looking for historical context, never to
            pick a target for new work.

    Returns:
        Formatted list of projects
    """
    client = _get_client()
    # The server now default-excludes archived rows, so the archives have to be
    # requested explicitly — otherwise `include_archived=True` marks nothing,
    # because there is nothing left to mark. The formatter still owns the
    # hide-and-report behaviour; this only widens what it is given.
    statuses = ["active", "archived"] if include_archived else None
    projects = await client.list_projects(user_id=user_id, statuses=statuses)
    return fmt.format_projects(projects, include_archived=include_archived)


@mcp_tool
async def get_project(project_id: str) -> str:
    """Get full details for a specific project.

    Returns name, description, goal, default config, and timestamps.

    Args:
        project_id: Project UUID

    Returns:
        Formatted project details
    """
    client = _get_client()
    project = await client.get_project(project_id)
    return fmt.format_project_detail(project)


@mcp_tool
async def create_project(
    name: str,
    user_id: str,
    description: str | None = None,
    goal: str | None = None,
    default_config_name: str | None = None,
    default_config_override: dict[str, Any] | None = None,
) -> str:
    """Create a new project.

    Projects organize related jobs, accumulate knowledge across jobs,
    and can have team members with different roles.

    Args:
        name: Project name
        user_id: Owner user UUID
        description: Project description (optional)
        goal: Project goal statement (optional)
        default_config_name: Default agent config for new jobs (optional)
        default_config_override: Default per-job config overrides (optional)

    Returns:
        Created project summary with ID
    """
    client = _get_client()
    result = await client.create_project(
        name=name,
        user_id=user_id,
        description=description,
        goal=goal,
        default_config_name=default_config_name,
        default_config_override=default_config_override,
    )
    return fmt.format_created_project(result)


@mcp_tool
async def list_project_jobs(
    project_id: str,
    status: str | None = None,
    limit: int = 100,
) -> str:
    """List jobs belonging to a project.

    Shows job status, config, timestamps, and audit entry counts.

    Args:
        project_id: Project UUID
        status: Filter by status (created, processing, completed, failed, etc.)
        limit: Max results (1-500, default 100)

    Returns:
        Formatted job list
    """
    client = _get_client()
    jobs = await client.list_project_jobs(
        project_id=project_id,
        status=status,
        limit=limit,
    )
    return fmt.format_jobs(jobs)


@mcp_tool
async def create_project_job(
    project_id: str,
    description: str,
    expert: str | None = None,
    config_name: str = "worker_base",
    expert_id: str | None = None,
    instructions: str | None = None,
    kickoff_message: str | None = None,
    config_override: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    datasource_ids: list[str] = None,  # type: ignore[assignment]
    priority: int = 5,
    required_deliverables: list[str] | None = None,
) -> str:
    """Create a job within a project context.

    Uses the project's default config if not overridden. The job is
    automatically linked to the project, gets a Gitea branch, and is queued for
    workspace provisioning and automatic agent assignment.

    Args:
        project_id: Project UUID
        description: Task description
        expert: Which expert runs this job — the worker profile that decides
            its tools, prompts and model. Takes either form the catalogue
            prints: a bundled expert id ("developer") or a database expert
            UUID. Use the `list_experts` tool to discover valid values and
            `get_expert` to inspect one. Omit to accept the project's default.
        config_name: DEPRECATED alias for `expert`, bundled experts only.
        expert_id: DEPRECATED alias for `expert`, database experts only.
        instructions: Additional inline instructions (optional)
        kickoff_message: Opening task brief sent to the agent (optional)
        config_override: Per-job config overrides as JSON (optional). To set
            the model, use {"llm": {"model": "<model_id>"}} — e.g.
            {"llm": {"model": "codex/gpt-5.3-codex-spark"}}.
            Use the list_models tool to discover available model IDs.
        datasource_ids: Connector selection. Omit to use the project's
            automatic defaults; pass [] to attach none; pass IDs to request
            exactly those connectors.
        context: Additional context dictionary (optional)
        priority: Dispatch priority from 0 (low) to 10 (high), default 5
        required_deliverables: Deliverable contract — workspace-relative
            artifact paths (e.g. "output/report.md") or "kb:<slug>" note
            slugs that must exist before a completion claiming success may
            seal. Shown to the worker at dispatch; missing deliverables
            bounce the seal back to the worker with the precise list.

    Returns:
        Created job summary with ID
    """
    # Same resolver the shared create_job descriptor and the REST funnel use;
    # see src/shared/expert_reference.py.
    try:
        choice = resolve_expert_selection(
            expert=expert, config_name=config_name, expert_id=expert_id
        )
    except ExpertReferenceConflict as conflict:
        return f"Refusing to create job: {conflict}"
    client = _get_client()
    result = await client.create_project_job(
        project_id=project_id,
        description=description,
        config_name=choice.config_name,
        expert_id=choice.expert_id,
        instructions=instructions,
        kickoff_message=kickoff_message,
        config_override=config_override,
        context=context,
        datasource_ids=datasource_ids,
        priority=priority,
        required_deliverables=required_deliverables,
    )
    return fmt.format_created_job(result, choice.config_name, expert=choice.reference)


# =============================================================================
# Project Management (Extended)
# =============================================================================


@mcp_tool
async def update_project(
    project_id: str,
    name: str | None = None,
    description: str | None = None,
    goal: str | None = None,
    status: str | None = None,
    default_config_name: str | None = None,
    default_config_override: dict[str, Any] | None = None,
) -> str:
    """Update a project's metadata or default config.

    Only provided fields are updated; others remain unchanged.

    Args:
        project_id: Project UUID
        name: New project name (optional)
        description: New description (optional)
        goal: New goal statement (optional)
        status: New status (optional)
        default_config_name: Default agent config for new jobs (optional)
        default_config_override: Default per-job config overrides (optional)

    Returns:
        Update confirmation
    """
    client = _get_client()
    result = await client.update_project(
        project_id=project_id,
        name=name,
        description=description,
        goal=goal,
        status=status,
        default_config_name=default_config_name,
        default_config_override=default_config_override,
    )
    s = result.get("status", "unknown")
    message = f"Project {project_id} updated ({s})."
    dropped = result.get("dropped_hidden_keys") or []
    if dropped:
        # get_project serves the override redacted; a write that changed an
        # endpoint (or a section) kept none of these stored secrets.
        message += (
            " Stored secrets NOT kept (send them again to restore): "
            + ", ".join(dropped)
            + "."
        )
    return message


@mcp_tool
async def delete_project(project_id: str) -> str:
    """Permanently delete a project and its associated data.

    Cannot delete default projects. This is irreversible — all project
    data including repos and knowledge base entries will be removed.

    Args:
        project_id: Project UUID

    Returns:
        Deletion confirmation
    """
    client = _get_client()
    result = await client.delete_project(project_id)
    s = result.get("status", "unknown")
    return f"Project {project_id} deleted ({s})."


@mcp_tool
async def list_project_members(project_id: str) -> str:
    """List all members of a project with their roles.

    Shows display name, email, role (owner/editor/viewer), and
    when they were added.

    Args:
        project_id: Project UUID

    Returns:
        Formatted member list with roles
    """
    client = _get_client()
    members = await client.list_project_members(project_id)
    return fmt.format_project_members(project_id, members)


@mcp_tool
async def add_project_member(
    project_id: str,
    user_id: str,
    role: str = "editor",
) -> str:
    """Add a user to a project with a specified role.

    Returns 409 if the user is already a member.

    Args:
        project_id: Project UUID
        user_id: User UUID to add
        role: Member role — owner, editor, or viewer (default: editor)

    Returns:
        Confirmation with member name and role
    """
    client = _get_client()
    result = await client.add_project_member(
        project_id=project_id,
        user_id=user_id,
        role=role,
    )
    name = result.get("display_name", user_id)
    actual_role = result.get("role", role)
    return f"Added {name} to project {project_id} as {actual_role}."


@mcp_tool
async def update_project_member(
    project_id: str,
    user_id: str,
    role: str,
) -> str:
    """Change a project member's role.

    Valid roles: owner, editor, viewer.

    Args:
        project_id: Project UUID
        user_id: User UUID of the member to update
        role: New role — owner, editor, or viewer

    Returns:
        Update confirmation
    """
    client = _get_client()
    result = await client.update_project_member(
        project_id=project_id,
        user_id=user_id,
        role=role,
    )
    s = result.get("status", "unknown")
    return f"Updated member {user_id} role to {role} in project {project_id} ({s})."


@mcp_tool
async def remove_project_member(
    project_id: str,
    user_id: str,
) -> str:
    """Remove a member from a project.

    Cannot remove the last owner of a project.

    Args:
        project_id: Project UUID
        user_id: User UUID to remove

    Returns:
        Removal confirmation
    """
    client = _get_client()
    result = await client.remove_project_member(
        project_id=project_id,
        user_id=user_id,
    )
    s = result.get("status", "unknown")
    return f"Removed member {user_id} from project {project_id} ({s})."


@mcp_tool
async def list_project_experts(project_id: str) -> str:
    """List project-specific expert configurations.

    These are experts stored in the project's Gitea repository
    under the experts/ directory, separate from global experts.

    Args:
        project_id: Project UUID

    Returns:
        Formatted list of project experts with descriptions and tags
    """
    client = _get_client()
    experts = await client.list_project_experts(project_id)
    return fmt.format_project_experts(project_id, experts)


@mcp_tool
async def get_project_expert(
    project_id: str,
    expert_name: str,
) -> str:
    """Get detailed configuration for a project-specific expert.

    Returns the expert's merged config (defaults + overrides)
    and the full instructions.md content.

    Args:
        project_id: Project UUID
        expert_name: Expert config name (e.g. 'scholar', 'developer')

    Returns:
        Expert detail with config and instructions
    """
    client = _get_client()
    data = await client.get_project_expert(project_id, expert_name)
    return fmt.format_project_expert_detail(project_id, data)


# =============================================================================
# Datasource CRUD
# =============================================================================


@mcp_tool
async def create_datasource(
    name: str,
    type: DatasourceType,
    connection_url: str | None = None,
    description: str | None = None,
    credentials: dict[str, Any] | None = None,
    cli_hint: str | None = None,
    default_branch: str | None = None,
    config: dict[str, Any] | None = None,
    is_global: bool = False,
    read_only: bool | None = None,
    scope_mode: DatasourceScopeMode = "all",
    project_ids: list[str] = None,  # type: ignore[assignment]
    auto_attach: bool = False,
) -> str:
    """Create a new connector.

    Supports generic, repository, kb, PostgreSQL, Neo4j, MongoDB, WebDAV,
    email, MCP, and credential-file connectors. New connectors are canonical
    user-owned resources; job-scoped connector creation is no longer
    supported. Global publication is controlled separately by is_global and
    requires the server-side public_datasources capability.

    Args:
        name: User-provided label
        type: Canonical connector type
        connection_url: Connection string (optional for generic)
        description: What this connector contains (optional)
        credentials: Auth details (optional). Stored values are never returned.
            Credential-file types accept credentials.files; SSH-key generation
            and interactive OAuth onboarding deliberately remain REST/UI-only.
        cli_hint: Suggested CLI command (optional, for generic type)
        default_branch: Branch to clone (optional, for repository type)
        config: Non-secret type-specific config. Includes kb root_path; email
            access/folders/drafts_folder/from_address/recipient_allowlist/
            unattended_send; and repository options. MCP connectors reject it.
        is_global: Publish to all users (capability-gated; default false)
        read_only: Declarative read-only setting (public defaults true; kb always true)
        scope_mode: Availability scope. ``all`` allows every otherwise-authorized
            work context; ``projects`` restricts use to project_ids.
        project_ids: Full initial project scope. Omit for all scope; projects
            mode requires a nonempty list. Explicit null is invalid.
        auto_attach: Preselect for the creator's new work when available.
            This is a default only and never force-attaches the connector.

    Returns:
        Created connector summary with masked URL
    """
    client = _get_client()
    result = await client.create_datasource(
        name=name,
        ds_type=type,
        connection_url=connection_url,
        description=description,
        credentials=credentials,
        cli_hint=cli_hint,
        default_branch=default_branch,
        config=config,
        is_global=is_global,
        read_only=read_only,
        scope_mode=scope_mode,
        project_ids=project_ids,
        auto_attach=auto_attach,
    )
    return fmt.format_created_datasource(result)


@mcp_tool
async def update_datasource(
    datasource_id: str,
    name: str | None = None,
    description: str | None = None,
    connection_url: str | None = None,
    credentials: dict[str, Any] | None = None,
    cli_hint: str | None = None,
    default_branch: str | None = None,
    config: dict[str, Any] | None = None,
    is_global: bool | None = None,
    read_only: bool | None = None,
    scope_mode: DatasourceScopeMode = None,  # type: ignore[assignment]
    project_ids: list[str] = None,  # type: ignore[assignment]
    auto_attach: bool = None,  # type: ignore[assignment]
    policy_revision: int = None,  # type: ignore[assignment]
) -> str:
    """Update an existing connector's connection details or metadata.

    Only provided fields are updated; others remain unchanged.

    Args:
        datasource_id: Connector UUID
        name: New label (optional)
        description: New description (optional)
        connection_url: New connection string (optional)
        credentials: New auth details (optional; omit to preserve stored secrets)
        cli_hint: New CLI hint (optional)
        default_branch: New default branch (optional)
        config: New non-secret type-specific config (optional)
        is_global: Publish/unpublish. Publishing is capability-gated (optional)
        read_only: New declarative read-only setting (optional)
        scope_mode: New availability scope; omit to preserve it.
        project_ids: Desired full project set. Omit to preserve links; pass []
            to remove all links (valid only with resulting all scope).
            Explicit null is invalid.
        auto_attach: New owner-only default-selection preference
        policy_revision: Revision returned by the management API. Required
            whenever scope_mode, project_ids, or auto_attach is changed.

    Returns:
        Update confirmation
    """
    client = _get_client()
    result = await client.update_datasource(
        datasource_id=datasource_id,
        name=name,
        description=description,
        connection_url=connection_url,
        credentials=credentials,
        cli_hint=cli_hint,
        default_branch=default_branch,
        config=config,
        is_global=is_global,
        read_only=read_only,
        scope_mode=scope_mode,
        project_ids=project_ids,
        auto_attach=auto_attach,
        policy_revision=policy_revision,
    )
    status = result.get("status") or "updated"
    lines = [f"Connector {datasource_id} updated ({status})."]
    if result.get("policy_revision") is not None:
        lines.append(f"Policy revision: {result['policy_revision']}")
    if result.get("scope_mode") is not None:
        lines.append(f"Availability scope: {result['scope_mode']}")
    if result.get("auto_attach") is not None:
        lines.append(f"Auto-attach default: {bool(result['auto_attach'])}")
    if result.get("project_ids") is not None:
        projects = ", ".join(str(value) for value in result["project_ids"]) or "none"
        lines.append(f"Projects: {projects}")
    return "\n".join(lines)


@mcp_tool
async def delete_datasource(datasource_id: str) -> str:
    """Permanently delete a connector.

    This does not affect jobs that have already cloned it.

    Args:
        datasource_id: Connector UUID

    Returns:
        Deletion confirmation
    """
    client = _get_client()
    result = await client.delete_datasource(datasource_id)
    status = result.get("status", "unknown")
    return f"Connector {datasource_id} deleted ({status})."


# =============================================================================
# Project ↔ Datasource (N:M)
# =============================================================================


@mcp_tool
async def list_project_datasources(project_id: str) -> str:
    """List connectors linked to a project.

    Args:
        project_id: Project UUID

    Returns:
        Formatted connector list
    """
    client = _get_client()
    datasources = await client.list_project_datasources(project_id)
    if not datasources:
        return f"No connectors linked to project {project_id}."
    return fmt.format_datasources(datasources)


@mcp_tool
async def link_datasource_to_project(project_id: str, datasource_id: str) -> str:
    """Link a connector to a project.

    Creates a knowledge entry so agents can discover the connector
    through the project's knowledge base.

    Args:
        project_id: Project UUID
        datasource_id: Connector UUID

    Returns:
        Link confirmation
    """
    client = _get_client()
    result = await client.link_datasource_to_project(project_id, datasource_id)
    status = result.get("status", "unknown")
    return f"Connector {datasource_id} linked to project {project_id} ({status})."


@mcp_tool
async def unlink_datasource_from_project(project_id: str, datasource_id: str) -> str:
    """Unlink a connector from a project.

    Removes the corresponding knowledge entry.

    Args:
        project_id: Project UUID
        datasource_id: Connector UUID

    Returns:
        Unlink confirmation
    """
    client = _get_client()
    result = await client.unlink_datasource_from_project(project_id, datasource_id)
    status = result.get("status", "unknown")
    return f"Connector {datasource_id} unlinked from project {project_id} ({status})."


# =============================================================================
# Knowledge Mutations
# =============================================================================


@mcp_tool
async def update_knowledge_note(
    project_id: str,
    note_id: str,
    status: str | None = None,
    add_tags: list[str] | None = None,
    remove_tags: list[str] | None = None,
) -> str:
    """Update a knowledge note's status or tags.

    Use to mark notes as resolved, superseded, or archived,
    or to organize notes with tags.

    Args:
        project_id: Project UUID
        note_id: Note ID
        status: New status (active, resolved, superseded, archived)
        add_tags: Tags to add (optional)
        remove_tags: Tags to remove (optional)

    Returns:
        Update confirmation
    """
    client = _get_client()
    result = await client.update_knowledge_note(
        project_id=project_id,
        note_id=note_id,
        status=status,
        add_tags=add_tags,
        remove_tags=remove_tags,
    )
    s = result.get("status", "unknown")
    return f"Knowledge note {note_id} updated ({s})."


@mcp_tool
async def delete_knowledge_note(project_id: str, note_id: str) -> str:
    """Permanently delete a knowledge note from both PostgreSQL and Neo4j.

    This is irreversible.

    Args:
        project_id: Project UUID
        note_id: Note ID

    Returns:
        Deletion confirmation
    """
    client = _get_client()
    result = await client.delete_knowledge_note(project_id, note_id)
    s = result.get("status", "unknown")
    return f"Knowledge note {note_id} deleted ({s})."


@mcp_tool
async def export_knowledge(project_id: str) -> str:
    """Export a project's knowledge base as Obsidian-compatible markdown.

    Each note becomes a .md file with YAML frontmatter and wikilink
    relationships. Requires Neo4j to be available.

    Args:
        project_id: Project UUID

    Returns:
        Export summary with path and note count
    """
    client = _get_client()
    result = await client.export_knowledge(project_id)
    path = result.get("path", "unknown")
    count = result.get("note_count", 0)
    name = result.get("project_name", "")
    return (
        f"Knowledge base exported for project '{name}'.\n"
        f"  Notes exported: {count}\n"
        f"  Export path: {path}\n"
        f"  Format: Obsidian-compatible markdown with YAML frontmatter"
    )


@mcp_tool
async def reindex_knowledge(project_id: str, full: bool = False) -> str:
    """Rebuild/refresh a project KB's chunk index from its vault repo.

    The `kb reindex --full` operator hatch (OKF KB slice 3): incremental by
    default (only notes whose git blob changed re-embed, via the per-KB commit
    watermark); `full=True` re-embeds the whole vault — use after an embedding
    model/chunker change or to recover a corrupt index.

    Args:
        project_id: Project UUID
        full: Re-embed every note instead of only changed blobs

    Returns:
        Reindex summary (status, commit,
        upserted/deleted/skipped/skipped_duplicates/errors)
    """
    client = _get_client()
    result = await client.reindex_knowledge(project_id, full=full)
    status = result.get("status", "unknown")
    commit = (result.get("indexed_commit") or "")[:12]
    # `skipped_duplicates` is the reindexer's own counter for notes it declined
    # to index because another path already holds the id. Omitting it made a
    # partially-indexed vault read as a clean run: the notes are simply absent
    # from search with nothing in the summary to say why. `.get` with a default
    # so an older endpoint payload still renders.
    return (
        f"KB reindex: {status} (commit {commit or 'n/a'}, "
        f"full={result.get('full', False)}).\n"
        f"  Upserted: {result.get('upserted', 0)}, "
        f"deleted: {result.get('deleted', 0)}, "
        f"skipped: {result.get('skipped', 0)}, "
        f"skipped_duplicates: {result.get('skipped_duplicates', 0)}, "
        f"errors: {result.get('errors', 0)}"
    )


# =============================================================================
# Job Promotion
# =============================================================================


# =============================================================================
# Internal Async Helpers (depend on client, not extracted to formatters)
# =============================================================================


# =============================================================================
# Sudo Approval Gate
# =============================================================================


@mcp_tool
async def list_sudo_requests(
    job_id: str | None = None,
    status: str | None = None,
    limit: int = 20,
) -> str:
    """List sudo approval requests from agent VMs.

    Shows pending, approved, denied, and expired sudo requests.
    Use this to see what privileged commands agents are requesting.

    Args:
        job_id: Filter by job ID
        status: Filter by status (pending, approved, denied, expired, auto_approved, auto_denied)
        limit: Maximum requests to return (1-100, default 20)

    Returns:
        Formatted list of sudo approval requests
    """
    if limit < 1:
        limit = 1
    elif limit > 100:
        limit = 100

    client = _get_client()
    requests = await client.list_sudo_requests(
        job_id=job_id, status=status, limit=limit
    )

    if not requests:
        return "No sudo approval requests found."

    lines = [f"Found {len(requests)} sudo request(s):\n"]
    for req in requests:
        rid = req.get("id", "?")
        jid = req.get("job_id", "?")[:8]
        cmd_str = render_sudo_command_line(req.get("command"), req.get("arguments"))
        st = req.get("status", "?")
        user = req.get("requesting_user", "?")
        target = req.get("target_user", "root")
        vm = req.get("vm_name", "?")
        ts = req.get("requested_at", "?")

        status_icon = {
            "pending": "⏳",
            "approved": "✅",
            "denied": "❌",
            "expired": "⏰",
        }.get(st, "•")
        lines.append(f"{status_icon} [{rid[:8]}] {st.upper()} — id: {rid}")
        lines.append(f"  Command: {cmd_str}")
        lines.append(f"  User: {user} → {target} | VM: {vm} | Job: {jid}")
        lines.append(f"  Requested: {ts}")
        if req.get("expires_at"):
            lines.append(f"  Expires: {req['expires_at']}")
        if req.get("decided_by"):
            lines.append(
                f"  Decided by: {req['decided_by']} — {req.get('decision_reason', '')}"
            )
        lines.append("")

    return "\n".join(lines)


class _SudoIdError(ValueError):
    """The caller's id matched no request, or more than one."""


async def _resolve_sudo_request_id(client: Any, request_id: str) -> str:
    """Accept the short id the listing prints as well as the full UUID.

    ``list_sudo_requests`` shows 8-character ids, so an approver who reads the
    listing has a prefix and not a UUID. A full UUID is used as-is; anything
    shorter is matched against the ids of recent requests and must identify
    exactly one.
    """
    candidate = request_id.strip().lower() if isinstance(request_id, str) else ""
    try:
        return str(UUID(candidate))
    except (AttributeError, TypeError, ValueError):
        pass

    # A prefix must be a nonempty beginning of a canonical UUID. In particular,
    # startswith("") matches every row and can silently decide the sole request.
    shape = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    if not 1 <= len(candidate) < len(shape) or any(
        char != "-" if shape[index] == "-" else char not in "0123456789abcdef"
        for index, char in enumerate(candidate)
    ):
        raise _SudoIdError(
            "invalid sudo request id — provide a UUID or its short prefix"
        )

    requests = await client.list_sudo_requests(limit=100)
    matches = sorted(
        {
            str(req["id"])
            for req in requests
            if str(req.get("id", "")).startswith(candidate)
        }
    )
    if not matches:
        raise _SudoIdError(
            f"no sudo request id starts with '{candidate}' — "
            "run list_sudo_requests to see the current ids"
        )
    if len(matches) > 1:
        raise _SudoIdError(
            f"'{candidate}' is ambiguous, it matches {len(matches)} requests: "
            + ", ".join(matches)
        )
    return matches[0]


@mcp_tool
async def approve_sudo_request(
    request_id: str,
    reason: str = "",
) -> str:
    """Approve a pending sudo approval request.

    This sends the approval to the agent's VM, allowing the sudo command
    to execute. The agent's run_command call will unblock and return output.

    Args:
        request_id: UUID of the sudo request to approve
        reason: Optional reason for approval

    Returns:
        Confirmation message
    """
    client = _get_client()
    try:
        resolved = await _resolve_sudo_request_id(client, request_id)
    except _SudoIdError as e:
        return f"Failed to approve: {e}"
    try:
        result = await client.approve_sudo_request(resolved, reason=reason)
        return f"Approved sudo request {resolved}: {result.get('status', 'ok')}"
    except Exception as e:
        return f"Failed to approve: {e}"


@mcp_tool
async def deny_sudo_request(
    request_id: str,
    reason: str,
) -> str:
    """Deny a pending sudo approval request.

    This sends a denial to the agent's VM. The sudo command will be rejected
    and the agent will see 'sudo request denied by operator' in stderr.

    Args:
        request_id: UUID of the sudo request to deny
        reason: Reason for denial (required)

    Returns:
        Confirmation message
    """
    client = _get_client()
    try:
        resolved = await _resolve_sudo_request_id(client, request_id)
    except _SudoIdError as e:
        return f"Failed to deny: {e}"
    try:
        result = await client.deny_sudo_request(resolved, reason=reason)
        return f"Denied sudo request {resolved}: {result.get('status', 'ok')}"
    except Exception as e:
        return f"Failed to deny: {e}"


# =============================================================================
# Messaging Tools (Live Communication)
# =============================================================================


# =============================================================================
# Officers — the Legate's side (knowledge-base/knowledge/features/officer_legate_channel.md)
# =============================================================================


@mcp_tool
async def list_officers() -> str:
    """List every project officer (centurion) you can see, vacant posts included.

    The roster answers "which of my projects has an officer, and is anything
    waiting on him" in one call: commissioned / vacant / held, when he next
    wakes, jobs in flight, events pending on him, pages he sent today.

    Returns:
        One block per post, or a note that no project you can see has one.
    """
    client = _get_client()
    try:
        data = await client.list_officers()
    except Exception as e:
        return _format_action_error("list_officers", "N/A", e)
    return fmt.format_officer_roster(data)


@mcp_tool
async def get_project_officer(project_id: str, recent: int = 10) -> str:
    """Get one project's officer: his post, his capacity, and his recent log.

    Reads the post card (commission state, hold, kit utilization with pool
    depth, next wake, page budget, digest ring, open conference) and, when the
    post is filled, the tail of his session log — so you can see what he has
    actually been doing, not only what he is configured to do.

    Args:
        project_id: Project UUID
        recent: How many trailing log messages to include (0 disables)

    Returns:
        The post as a briefing, with a recent-log section when available.
    """
    client = _get_client()
    try:
        post = await client.get_project_officer(project_id)
    except Exception as e:
        return _format_action_error("get_project_officer", project_id, e)

    thread_id = (post.get("officer") or {}).get("thread_id")
    tail = None
    footnote = None
    if post.get("commissioned") and thread_id and recent > 0:
        try:
            tail = await client.get_persistent_thread_messages(
                thread_id,
                limit=max(2, min(int(recent), 100)),
                before=datetime.now(timezone.utc).isoformat(),
            )
        except Exception as e:
            # His post is the answer; the log is the bonus. Say which half is
            # missing instead of failing the whole read.
            footnote = f"(recent log unavailable: {type(e).__name__})"
    rendered = fmt.format_officer_post(post, recent=tail)
    return f"{rendered}\n{footnote}" if footnote else rendered


@mcp_tool
async def send_officer_note(project_id: str, message: str) -> str:
    """Send the project's officer a one-way note from the Legate.

    MUTATION: the note reaches a running agent. Use it to give direction,
    answer something he paged about, or redirect his backlog — he treats a
    Legate note as top authority. It is stamped with who sent it, so a note
    you compose does not read as words the user typed.

    There is no reply channel here: his answer shows up in his log, in his
    digest, or as a page. The result states which delivery happened — reaching
    his input queue, queuing durably for his next wake, or waiting behind a
    hold — so never report a note as read until it actually is.

    Args:
        project_id: Project UUID whose officer should receive the note
        message: The direction itself (max 8000 chars)

    Returns:
        How the note landed, including when he is expected to read it.
    """
    client = _get_client()
    try:
        result = await client.send_officer_note(project_id, message)
    except Exception as e:
        return _format_action_error("send_officer_note", project_id, e)
    return fmt.format_officer_note_result(result)


# =============================================================================
# Persistent Session/Thread Management
# =============================================================================


@mcp_tool
async def create_persistent_thread(
    title: str = "Untitled Session",
    config_name: str = "session_base",
    permission_mode: Literal["supervised", "auto_accept", "autonomous"] = "supervised",
    project_id: str | None = None,
    project_ids: list[str] | None = None,
    datasource_ids: list[str] = None,  # type: ignore[assignment]
    model: str | None = None,
    temperature: float | None = None,
) -> str:
    """Create a new persistent thread (interactive agent session).

    MUTATION: This creates a persistent thread, provisions a workspace
    container and agent pod. The thread starts in 'created' status and
    waits for an agent to connect.

    **Deliberately exposes no ``config_override``, and adding one is a
    security decision, not a convenience.** Session create accepts any tool
    name the registry knows in its own category, including the
    ``grant: "explicit"`` control-plane writes (``set_expert_bundle`` and
    friends) — safe today only because no *model-authored* path reaches that
    parameter. This tool is the one that would open it: an MCP caller is an
    LLM, and a prompt-injected fragment would arrive as a legitimate request.
    Pinned by ``tests/test_tool_override_boundary.py``
    ``TestNoModelAuthoredPathReachesSessionCreate``. If you add the parameter,
    that test fails and you must first decide what a model may name.

    Args:
        title: Human-readable session title
        config_name: Agent config to use (default: "session_base")
        permission_mode: Tool approval mode (supervised, auto_accept, autonomous)
        project_id: Single project UUID to scope (legacy)
        project_ids: List of project UUIDs to scope
        datasource_ids: Connector selection. Omit to use automatic defaults;
            pass [] for none, or IDs for exactly that authorized selection.
        model: LLM model override (e.g. "RedHatAI/gemma-4-31B-it-FP8-Dynamic")
        temperature: Temperature override

    Returns:
        Created thread ID and status
    """
    client = _get_client()
    try:
        result = await client.create_persistent_thread(
            config_name=config_name,
            title=title,
            permission_mode=permission_mode,
            project_id=project_id,
            project_ids=project_ids,
            datasource_ids=datasource_ids,
            model=model,
            temperature=temperature,
        )
        return fmt.format_created_thread(result, config_name, title)
    except Exception as e:
        return _format_action_error("create_thread", "N/A", e)


@mcp_tool
async def list_persistent_threads(
    project_id: str | None = None,
    status: Literal["created", "active", "ended"] | None = None,
) -> str:
    """List persistent threads for the authenticated user.

    Returns all persistent sessions with status, config, and activity info.
    Use filters to narrow results.

    Args:
        project_id: Filter by project UUID
        status: Filter by thread status (created, active, ended)

    Returns:
        Formatted list of persistent threads
    """
    client = _get_client()
    data = await client.list_persistent_threads(project_id=project_id, status=status)
    return fmt.format_persistent_threads(data.get("threads", []))


@mcp_tool
async def get_persistent_thread(thread_id: str) -> str:
    """Get detailed information about a specific persistent thread.

    Returns full thread details including status, config, workspace state,
    and metadata.

    Args:
        thread_id: Thread UUID to retrieve

    Returns:
        Formatted thread details
    """
    client = _get_client()
    thread = await client.get_persistent_thread(thread_id)
    return fmt.format_persistent_thread_detail(thread)


@mcp_tool
async def end_persistent_thread(thread_id: str, permanent: bool = False) -> str:
    """End or permanently delete a persistent thread.

    MUTATION: If permanent=False (default), the thread is soft-ended and
    can be resumed later. If permanent=True, the thread and ALL its
    messages are permanently deleted along with workspace containers,
    agent pods, and VMs.

    Args:
        thread_id: Thread UUID to end or delete
        permanent: If true, permanently delete instead of soft-end

    Returns:
        Action result with status
    """
    client = _get_client()
    try:
        result = await client.end_persistent_thread(thread_id, permanent=permanent)
        return fmt.format_thread_action_result(
            "delete_thread" if permanent else "end_thread", thread_id, result
        )
    except Exception as e:
        return _format_action_error("end_thread", thread_id, e)


@mcp_tool
async def resume_persistent_thread(
    thread_id: str, acknowledge: list[str] | None = None
) -> str:
    """Resume an ended persistent thread.

    MUTATION: Resets the thread status to 'created', clears the stale
    agent binding, and re-provisions the agent pod. The thread must be
    in 'ended' status.

    If the session's config has drifted (deleted connector, revoked
    project, withdrawn grant), this returns the drifted items and their
    ids; call again with `acknowledge` set to those ids to resume without
    them.

    Args:
        thread_id: Thread UUID to resume
        acknowledge: Drift item ids to resume without (optional)

    Returns:
        Action result with new status
    """
    client = _get_client()
    try:
        result = await client.resume_persistent_thread(
            thread_id, acknowledge=acknowledge
        )
        return fmt.format_thread_action_result("resume_thread", thread_id, result)
    except Exception as e:
        return _format_action_error("resume_thread", thread_id, e)


@mcp_tool
async def get_persistent_thread_messages(
    thread_id: str,
    limit: int = 50,
    offset: int = 0,
    full_content: bool = False,
    newest_first: bool = False,
) -> str:
    """Get message history for a persistent thread session.

    Returns conversation messages in chronological order with role,
    content preview, and tool call info. Paginated.

    Args:
        thread_id: Thread UUID to get messages for
        limit: Maximum messages to return (1-500, default 50)
        offset: Pagination offset (default 0), ignored when newest_first
        full_content: If True, emit each message's content in full instead of
            the default 500-char preview. Pair with a small ``limit`` — full
            bodies can be very large.
        newest_first: Read the END of the log — the newest ``limit`` messages,
            still printed oldest-first within that window. Use this on long
            sessions (an officer runs hundreds of turns); paging ``offset``
            from zero to reach the current state is the slow way there.

    Returns:
        Formatted message history
    """
    if limit < 1:
        limit = 1
    elif limit > 500:
        limit = 500

    client = _get_client()
    if newest_first:
        data = await client.get_persistent_thread_messages(
            thread_id,
            limit=limit,
            before=datetime.now(timezone.utc).isoformat(),
        )
    else:
        data = await client.get_persistent_thread_messages(
            thread_id, limit=limit, offset=offset
        )
    return fmt.format_persistent_thread_messages(
        data, full_content=full_content, tail=newest_first
    )


@mcp_tool
async def get_persistent_thread_ide(thread_id: str) -> str:
    """Get IDE/workspace session status for a persistent thread.

    Returns the workspace container or VM status with a code-server URL
    when the workspace is ready.

    Args:
        thread_id: Thread UUID to check IDE status for

    Returns:
        IDE session status with URLs
    """
    client = _get_client()
    data = await client.get_persistent_thread_ide(thread_id)
    return fmt.format_persistent_thread_ide(data)
