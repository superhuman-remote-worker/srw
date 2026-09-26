"""Session tool-group visibility and creation-form prediction (R1.B10).

Two reads answer "which tools does this session hold?": the live pane asks the
bound agent and falls back to a config prediction; the New Session / job
creation form can only ever predict. Both share one categorised view
(:func:`shared.runtime.core.tool_report.compose_tool_view`) so one renderer
serves both surfaces.

Every collaborator that the application composes (experts gates backed by the
store, the toolset probe, grant resolution, roster prefetch and the session
configuration dependencies) arrives through :class:`SessionToolViewDependencies`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException, Request

from orchestrator.services import session_config_resolution
from orchestrator.services.agent_toolset_probe import origin_fields, unmeasured
from orchestrator.services.config_overrides import (
    looks_like_uuid,
    refuse_execution_owned_workspace_keys,
)
from orchestrator.services.deployment_gates import is_experts_db_enabled
from orchestrator.services.session_tool_policy import (
    legacy_session_tool_policy,
    merged_session_tool_policy,
)
from shared.runtime.core.loader import canonical_config_name
from shared.runtime.core.tool_policy import enumerate_only_members
from shared.runtime.core.tool_report import compose_tool_view, tool_groups_from_view

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SessionToolViewDependencies:
    """Application-composed collaborators for one tool-view read."""

    store: Any
    user_experts_enabled: Callable[..., Awaitable[bool]]
    resolve_runner_grants: Callable[..., Awaitable[dict[str, Any] | None]]
    acknowledged_grant_strip: Callable[..., Awaitable[Any]]
    prefetch_roster_refs: Callable[..., Awaitable[Any]]
    agent_toolset_measurement: Callable[[dict[str, Any]], Awaitable[Any]]
    session_config_dependencies: Callable[
        [], session_config_resolution.SessionConfigDependencies
    ]


async def session_tool_grants(
    thread: dict[str, Any], *, dependencies: SessionToolViewDependencies
) -> dict[str, Any] | None:
    """The owner's capability grants, for explaining an ``unavailable``.

    ``None`` means "impose no grant-based restriction" — both for an admin
    (``_resolve_runner_grants`` returns ``None``) and for a lookup failure. A
    read surface must never INVENT a denial: the PDP at attach and dispatch is
    the enforcement, this is only the explanation, and a fabricated
    "unavailable — needs the shell_tools grant" is its own D1 violation.
    """
    try:
        project_ids = [str(thread["project_id"])] if thread.get("project_id") else []
        return await dependencies.resolve_runner_grants(
            runner_user_id=str(thread.get("user_id"))
            if thread.get("user_id")
            else None,
            project_ids=project_ids,
        )
    except Exception:
        logger.warning(
            "Tool-group grant lookup failed for thread %s; reporting no "
            "grant-based restrictions",
            thread.get("id"),
        )
        return None


async def thread_tool_groups(
    thread_id: str,
    thread: dict[str, Any],
    *,
    dependencies: SessionToolViewDependencies,
) -> dict[str, Any]:
    """Measured-or-predicted toolset of one owner-authorized session thread.

    See ``GET /api/persistent/threads/{thread_id}/tool-groups`` for the
    ``origin`` / ``source`` contract; this is its body after authorization.
    """
    store = dependencies.store
    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    request_override = metadata.get("config_override") or None

    base = canonical_config_name(thread.get("config_name") or "session_base")
    if looks_like_uuid(base):
        # Sentinel / cockpit-conflated expert UUID → the real session base.
        base = "session_base"

    m = await dependencies.agent_toolset_measurement(thread)
    grants = await session_tool_grants(thread, dependencies=dependencies)

    from shared.runtime.core.subagent_roster import roster_summary

    source = "resolved"
    configured: dict[str, Any] = {}
    provenance: dict[str, str] = {}
    # What a Delegation tick reaches: the expert's materialised roster, or an
    # empty one the pane can name as such. None only when the resolve failed.
    roster: dict[str, Any] | None = None

    if not is_experts_db_enabled() or not await dependencies.user_experts_enabled():
        source = "legacy"
        configured, provenance = await asyncio.to_thread(
            legacy_session_tool_policy, base, request_override
        )
        # The legacy path merges a public base only; none carries a roster.
        roster = roster_summary(None)
    else:
        try:
            expert_id = metadata.get("expert_id")
            expert_row = (
                await store.get_expert_by_id(str(expert_id)) if expert_id else None
            )
            project_id = str(thread["project_id"]) if thread.get("project_id") else None
            project_overrides = None
            if project_id and expert_id:
                link = await store.get_project_expert_link(
                    project_id=project_id, expert_id=str(expert_id)
                )
                if link:
                    project_overrides = link.get("config_override") or None
                    if isinstance(project_overrides, str):
                        project_overrides = json.loads(project_overrides)
            # Owner-correct, same as session_tool_grants above: an admin
            # viewing another user's thread must see THAT owner's
            # acknowledged grants, not their own (see _acknowledged_grant_strip
            # and the resume-time owner-vs-caller fix it mirrors).
            grant_strip = await dependencies.acknowledged_grant_strip(
                metadata,
                user_id=str(thread["user_id"]) if thread.get("user_id") else None,
                project_id=project_id,
            )
            # The same roster rows the attach prefetches: a DB `$ref` entry
            # the resolve cannot see is dropped, and the pane would then
            # report a roster the agent does bind as missing.
            db_refs = await dependencies.prefetch_roster_refs(
                expert_row=expert_row,
                overrides=[project_overrides, request_override],
                user_id=str(thread["user_id"]) if thread.get("user_id") else None,
                project_ids=[project_id] if project_id else [],
            )
            capture: dict[str, Any] = {}
            configured, provenance = await asyncio.to_thread(
                merged_session_tool_policy,
                base_config_name=base,
                expert_row=expert_row,
                project_overrides=project_overrides,
                request_override=request_override,
                grant_strip=grant_strip,
                db_refs=db_refs,
                capture=capture,
            )
            roster = roster_summary(
                (capture.get("merged_fragment") or {}).get("subagents")
            )
        except Exception:
            logger.exception("Tool-group resolve failed for thread %s", thread_id)
            source = "error"
            if m.categories is None:
                # No measurement AND no resolve: there is nothing honest to
                # report. A resolve error REFUSES the attach (fail closed), so
                # there is no agent answer either.
                return {
                    "thread_id": thread_id,
                    "source": "error",
                    **origin_fields(m),
                    "tool_groups": None,
                    "categories": None,
                    "subagents": None,
                }

    # Only a MEASURED answer carries backend capabilities: they come from the
    # agent's own report. A prediction has no provisioned workspace to inspect,
    # which is one of the three reasons it over-reports (the live gate saw it
    # over-report by 14 execution tools on a no-shell tier).
    view = compose_tool_view(
        measured=m.categories,
        configured=configured,
        provenance=provenance,
        backend_caps=m.backend,
        grants=grants,
    )
    return {
        "thread_id": thread_id,
        "source": source,
        **origin_fields(m),
        "enumerate_only": enumerate_only_members(),
        "tool_groups": tool_groups_from_view(view),
        "categories": view,
        "subagents": roster,
    }


async def preview_tool_groups(
    body: Any,
    user: dict[str, Any],
    *,
    request: Request,
    dependencies: SessionToolViewDependencies,
) -> dict[str, Any]:
    """The creation form's prediction for an approved user's proposed config.

    ``body`` is a ``ToolGroupPreviewRequest``. ``request`` is the caller's own
    request, handed to workspace selection unchanged. See
    ``POST /api/persistent/tool-groups/preview`` for the contract.
    """
    # The form preview answers for the same raw override admission will see,
    # so it refuses the container side door with admission's 422 instead of
    # letting workspace binding drop it with a warning.
    refuse_execution_owned_workspace_keys(body.config_override)
    store = dependencies.store
    is_worker = body.expert_type == "worker"
    default_base = "worker_base" if is_worker else "session_base"
    base = canonical_config_name(body.config_name or default_base)
    if looks_like_uuid(base):
        base = default_base

    expert_row = None
    project_overrides = None
    legacy = (
        not is_experts_db_enabled() or not await dependencies.user_experts_enabled()
    )
    try:
        if body.expert_id and not legacy:
            expert_row = await store.get_expert_by_id(str(body.expert_id))
            if body.project_id:
                link = await store.get_project_expert_link(
                    project_id=str(body.project_id), expert_id=str(body.expert_id)
                )
                if link:
                    project_overrides = link.get("config_override") or None
                    if isinstance(project_overrides, str):
                        project_overrides = json.loads(project_overrides)
    except Exception:
        logger.warning("Tool-group preview could not load the expert/project layer")

    from orchestrator.services.manifest_workspace_selection import (
        select_execution_workspace,
    )
    from shared.runtime.core.workspace_selection import bind_execution_workspace

    account = (
        await session_config_resolution.resolve_session_account_defaults(
            str(user["id"]), dependencies=dependencies.session_config_dependencies()
        )
        if not is_worker
        else {}
    )
    workspace_config, workspace_selection = await select_execution_workspace(
        store,
        user,
        project_id=body.project_id,
        role=body.expert_type,
        workspace=body.workspace,
        supplied="workspace" in body.model_fields_set,
        config_override=body.config_override,
        account_defaults=account,
        request=request,
    )
    workspace_source = (
        "project"
        if workspace_selection and workspace_selection.get("project_revision")
        else "request"
        if "workspace" in body.model_fields_set
        or "backend" in ((body.config_override or {}).get("workspace") or {})
        else "default"
    )
    # A creation client can ask to preview its proposed recommendation. It must
    # materialize that choice in the submitted execution; admission never reads it.
    if workspace_source == "default" and body.workspace_preference is not None:
        workspace_config["backend"] = body.workspace_preference
        workspace_source = "recommendation"
    preview_override = bind_execution_workspace(
        body.config_override or {}, workspace_config
    )
    preview_workspace = {
        "backend": workspace_config["backend"],
        "source": workspace_source,
        "binding": workspace_selection["document"]
        if workspace_selection
        else (
            None
            if workspace_config["backend"] == "none"
            else {"template": {"inline": {"backend": workspace_config["backend"]}}}
        ),
    }

    # The legacy branch models ONE agent's behaviour: persistent_session's
    # re-adding of the closed group lists when no disable marker is present.
    # Worker jobs have no such step, so on the worker surface "experts off" only
    # means there is no expert layer to merge — the resolved path already answers
    # that correctly. Routing a worker preview through the session legacy policy
    # would predict appended session groups for a job that cannot hold them.
    use_legacy = legacy and not is_worker
    from shared.runtime.core.subagent_roster import roster_summary

    roster: dict[str, Any] = roster_summary(None)
    try:
        if use_legacy:
            configured, provenance = await asyncio.to_thread(
                legacy_session_tool_policy, base, preview_override
            )
        else:
            # No grant_strip here: this is a not-yet-created session, so
            # there is no thread and no metadata.config_drift_ack to have
            # acknowledged anything against — unlike the thread endpoint
            # above, omitting it is not a gap to close, it is the correct
            # answer for a config that cannot yet have drifted.
            db_refs = await dependencies.prefetch_roster_refs(
                expert_row=expert_row,
                overrides=[project_overrides, body.config_override],
                user_id=str(user["id"]),
                project_ids=[str(body.project_id)] if body.project_id else [],
            )
            capture: dict[str, Any] = {}
            configured, provenance = await asyncio.to_thread(
                merged_session_tool_policy,
                base_config_name=base,
                expert_row=expert_row,
                project_overrides=project_overrides,
                request_override=preview_override,
                expert_type=body.expert_type,
                db_refs=db_refs,
                capture=capture,
            )
            roster = roster_summary(
                (capture.get("merged_fragment") or {}).get("subagents")
            )
    except Exception:
        logger.exception("Tool-group preview resolve failed")
        raise HTTPException(
            status_code=422,
            detail="This configuration cannot be resolved, so its toolset "
            "cannot be predicted.",
        )

    try:
        grants = await dependencies.resolve_runner_grants(
            runner_user_id=str(user["id"]),
            project_ids=[str(body.project_id)] if body.project_id else [],
        )
    except Exception:
        logger.warning("Tool-group preview grant lookup failed")
        grants = None

    view = compose_tool_view(
        measured=None,
        configured=configured,
        provenance=provenance,
        backend_caps={
            "supports_shell": workspace_config["backend"] in ("sandbox", "vm"),
            "supports_file_tools": workspace_config["backend"] != "none",
            "supports_canvas_presentation": workspace_config["backend"] != "none",
        },
        grants=grants,
    )
    return {
        "workspace": preview_workspace,
        "source": "legacy" if use_legacy else "resolved",
        **origin_fields(
            unmeasured(
                "no agent exists for an unsaved job"
                if is_worker
                else "no agent exists for an unsaved session"
            )
        ),
        "enumerate_only": enumerate_only_members(),
        "tool_groups": tool_groups_from_view(view),
        "categories": view,
        "subagents": roster,
    }


__all__ = [
    "SessionToolViewDependencies",
    "preview_tool_groups",
    "session_tool_grants",
    "thread_tool_groups",
]
