"""Orchestrator job management tools for persistent agents.

These tools call the orchestrator REST API to create, monitor, and manage
worker jobs. They enable the persistent agent to delegate heavy work to
the autonomous worker pool.

The orchestrator URL is read from the ORCHESTRATOR_URL environment variable
(same as the worker's orchestrator_client.py).
"""

import logging
import os
from uuid import UUID
from typing import Any, Dict, List, Optional

import httpx
from langchain_core.tools import tool

from shared.orch_surface.client import AsyncCockpitClient
from shared.orch_surface.jobs import (
    CallerCtx,
    JOB_DESCRIPTORS,
    JobToolResult,
    make_bound_handler,
)
from agent.services.image_content import IMAGE_DATA_TAG_TEMPLATE

from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    ORCHESTRATOR_TOOLS_METADATA as ORCHESTRATOR_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)


def _get_orchestrator_url() -> str:
    """Get orchestrator base URL from environment."""
    url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8085")
    return url.rstrip("/")


def _get_client(*, user_id: Optional[str] = None) -> httpx.AsyncClient:
    """Create an httpx client for orchestrator calls.

    Attaches ``X-Internal-Key`` when ``MCP_INTERNAL_KEY`` is set so the
    orchestrator's Track B (P4b) gates accept agent-tool calls. When
    ``user_id`` is supplied the client also sends ``X-MCP-User-Id`` so
    the orchestrator's ``_get_user_from_mcp_headers`` path can resolve
    the originating user — required by ``GET /api/jobs``,
    ``GET /api/jobs/{id}`` and any other endpoint guarded by
    ``require_approved_user`` / ``require_job_access``. Without the
    user header those endpoints 401 even with a valid internal key.

    Worker-mode callers (no session, no user identity) pass
    ``user_id=None`` and continue to authenticate as anonymous internal
    against the dual-callable / require_internal endpoints.
    """
    headers: dict[str, str] = {}
    internal_key = os.getenv("MCP_INTERNAL_KEY", "")
    if internal_key:
        headers["X-Internal-Key"] = internal_key
    if user_id:
        headers["X-MCP-User-Id"] = user_id
    return httpx.AsyncClient(timeout=30.0, headers=headers)


_surface_client: AsyncCockpitClient | None = None


def _get_surface_client() -> AsyncCockpitClient:
    """Return the shared pooled client used by descriptor-backed tools."""
    global _surface_client
    if _surface_client is None:
        _surface_client = AsyncCockpitClient(base_url=_get_orchestrator_url())
    return _surface_client


def _caller_ctx(context: ToolContext) -> CallerCtx:
    """Translate trusted ToolContext lineage without adding public arguments.

    Officer detection comes only from the server-derived runtime actor. Parsed
    config still controls the tool ceiling, but never grants actor authority.
    """
    parent_job_id: str | None = None
    candidate = context._job_metadata.get("job_id")
    try:
        if candidate:
            parent_job_id = str(UUID(str(candidate)))
    except (TypeError, ValueError, AttributeError):
        pass

    project_ids = tuple(str(project_id) for project_id in context.project_ids)
    runtime_actor = getattr(context, "runtime_actor", None)
    officer_session = bool(
        runtime_actor is not None and runtime_actor.caller_kind == "officer"
    )
    return CallerCtx(
        kind="officer" if officer_session else "session",
        user_id=context.user_id,
        project_ids=project_ids,
        lineage_project_id=context.project_id,
        thread_id=context.thread_id,
        parent_job_id=parent_job_id,
        resolve_job_id_prefixes=True,
        runtime_actor=runtime_actor,
        supports_multimodal=context.get_phase_multimodal(),
    )


def _agent_tool_result(result: str | JobToolResult) -> str:
    """Bridge a typed shared result into the existing transient image tag.

    The graph/persistent-graph post-processor strips this tag before the
    ToolMessage is checkpointed and creates the provider image block. Plain
    text never contains base64 after that boundary.
    """
    if isinstance(result, str):
        return result
    if result.image is None:
        return result.text
    return (
        result.text
        + "\n"
        + IMAGE_DATA_TAG_TEMPLATE.format(
            mime=result.image.media_type,
            b64=result.image.base64_data,
        )
    )


# F15: the local _short_id/_truncate/_compact_dict_value/_format_freeze_data/
# _format_job_list_item helpers were re-homed into
# src/shared/orch_surface/formatters.py (format_jobs/format_job_detail) so
# every lane renders the same decision-grade output.


def _format_grants(capabilities: Dict[str, Any]) -> List[str]:
    """Render the caller's effective grants for the session-context report.

    Shape comes from ``GET /api/users/me/capabilities``: admins get
    ``grants: None`` (unrestricted). Only the capabilities that decide whether a
    ``create_job`` override will be accepted are surfaced — the full
    catalog would bloat the tool result for no decision value.
    """
    if capabilities.get("is_admin"):
        return ["  Grants: admin (unrestricted)"]
    grants = capabilities.get("grants")
    if not isinstance(grants, dict):
        return []
    lines = ["  Grants:"]
    for key in ("vm_workspace", "shell_tools", "delegation", "datasource_tools"):
        if key in grants:
            lines.append(f"    {key}: {grants[key]}")
    allowed_models = grants.get("model_selection")
    if allowed_models is not None:
        lines.append(
            f"    model_selection: {', '.join(str(m) for m in allowed_models)}"
        )
    return lines


def _format_session_context(
    context: ToolContext,
    *,
    chat_models: Optional[List[str]] = None,
    capabilities: Optional[Dict[str, Any]] = None,
) -> str:
    workspace = context.workspace_manager
    backend = getattr(workspace, "backend", None) if workspace else None
    cloud_mount = context.config.get("cloud_mount") or {}
    datasource_keys = sorted(context.datasources.keys())

    lines = [
        "Session context:",
        f"  Thread ID: {context.thread_id or 'none'}",
        f"  User ID: {context.user_id or 'none'}",
        f"  Primary project ID: {context.project_id or 'none'}",
        f"  Project IDs: {', '.join(context.project_ids) if context.project_ids else 'none'}",
        f"  Job/context ID: {context.job_id or 'none'}",
        f"  Workspace available: {bool(workspace)}",
    ]
    if workspace:
        lines.extend(
            [
                f"  Workspace path: {workspace.path}",
                f"  Workspace backend: {type(backend).__name__ if backend else 'unknown'}",
                f"  Supports shell: {bool(getattr(backend, 'supports_shell', False))}",
                f"  Git available: {context.has_git()}",
            ]
        )
    lines.extend(
        [
            f"  Shell manager available: {context.has_shell()}",
            f"  Knowledge available: {context.has_knowledge()}",
            f"  Connectors: {', '.join(datasource_keys) if datasource_keys else 'none'}",
            f"  Cloud mount active: {bool(cloud_mount.get('active'))}",
        ]
    )
    if cloud_mount.get("active"):
        lines.append(
            f"  Cloud workspace entry: {cloud_mount.get('workspace_entry', 'cloud')}"
        )
    # Everything a create_job config_override needs to be written without
    # guessing: the model IDs this deployment actually routes, and the grants
    # that decide whether an override is accepted. Both are omitted (not faked)
    # when the lookup failed — see get_session_context.
    if chat_models:
        lines.append(f"  Available chat models: {', '.join(chat_models)}")
    if capabilities:
        lines.extend(_format_grants(capabilities))
    return "\n".join(lines)


def create_orchestrator_tools(context: ToolContext) -> List[Any]:
    """Create the session-context tool and every descriptor-backed job tool."""
    base_url = _get_orchestrator_url()

    @tool
    async def get_session_context() -> str:
        """Show the current session, project, workspace, and capability context.

        Use this before project/job/repository actions when you need to know
        which thread, user, project, and workspace backend this session is using.
        Also reports the chat models this deployment routes and your effective
        grants — read both before pinning a model or workspace backend in a
        create_job config_override.
        """
        chat_models: Optional[List[str]] = None
        capabilities: Optional[Dict[str, Any]] = None
        try:
            async with _get_client(user_id=context.user_id) as client:
                try:
                    resp = await client.get(f"{base_url}/api/models")
                    resp.raise_for_status()
                    groups = resp.json().get("groups") or []
                    models = [
                        str(model_id)
                        for group in groups
                        for model_id in (group.get("models") or [])
                    ]
                    chat_models = sorted(dict.fromkeys(models)) or None
                except Exception as error:
                    logger.debug("get_session_context: model lookup failed: %s", error)
                try:
                    resp = await client.get(f"{base_url}/api/users/me/capabilities")
                    resp.raise_for_status()
                    payload = resp.json()
                    if isinstance(payload, dict):
                        capabilities = payload
                except Exception as error:
                    logger.debug(
                        "get_session_context: capability lookup failed: %s", error
                    )
        except Exception as error:
            logger.debug("get_session_context: orchestrator unreachable: %s", error)

        return _format_session_context(
            context, chat_models=chat_models, capabilities=capabilities
        )

    job_tools = [
        tool(
            make_bound_handler(
                item,
                client_provider=_get_surface_client,
                caller_provider=lambda context=context: _caller_ctx(context),
                result_adapter=_agent_tool_result,
            )
        )
        for item in JOB_DESCRIPTORS
    ]
    return [get_session_context, *job_tools]
