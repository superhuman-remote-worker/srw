"""Agent-facing thread creation, message persistence and subagent children.

Extracted verbatim from ``orchestrator.main`` (R1.B06, lane C, census group
``S_CHILD``). Thirteen routes over three surfaces that share one transport
(``X-Internal-Key``) and nothing else:

* ``POST /api/agents/threads`` **provisions a session** — it resolves the
  application session expert, creates the thread, mints a scoped Gitea
  repository and schedules a workspace pod.
* The job-scoped and session-scoped ``…/subagents…`` routes **provision
  nothing**. A child runs inside its parent's workspace, so these routes only
  write the ``threads`` row. The contrast with the route above is deliberate
  and is the reason they are not merged.
* ``POST /api/agents/threads/{thread_id}/messages`` is a fire-and-forget
  transcript append.

Four properties are load-bearing and moved unchanged:

* **The internal key authenticates the transport, not the body it carries.**
  ``agent_create_thread`` runs the same ``config_name`` write boundary as the
  user-facing funnel — validate, canonicalise, then refuse — *before* any
  insert. A hostile config name is a 422 with nothing written.
* **The child row is derived from the parent, never from the body.** For a
  worker child ``user_id``/``project_id`` come from ``jobs``; for a session
  child they come from the parent thread. That is what keeps a child's
  transcript readable by the job owner and off every other user's sessions
  page.
* **Parent authority refusal is a 409 with the refusal's own detail** (§P7).
  ``ParentExecutionAuthorityRefused`` and ``SessionParentAuthorityRefused``
  carry the shape; a ``ValueError`` is a 400; a missing parent is a 404; and a
  result whose ``result`` key is not in the allowed set is a 409 carrying the
  whole result. ``InputDeliveryConflict`` keeps its distinct
  ``subagent_delivery_conflict`` body.
* **Terminalize is idempotent, and its idempotency key is the delivery id**
  (§P9). ``applied``/``idempotent`` (plus ``already_delivered`` on the session
  side) are successes; anything else is a refusal. Creation is idempotent per
  ``subagent_id`` only while the parent remains open — once a completion
  decision is journaled, even an exact retry is refused, so completion cannot
  race a child revival.

Collaborators arrive through :class:`AgentChildThreadDependencies`, rebuilt per
invocation by the application.

**The access gate belongs to the caller, not to these functions.** Each was the
body of a route whose first statement was ``require_internal``; that statement
now lives in ``orchestrator.routers.agent_child_threads`` (and in the
application's compatibility wrapper), which is where the endpoint-inventory gate
scanner reads it. Every function below assumes it has already run. Do not call
one from an ungated path.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable
from uuid import UUID

from fastapi import HTTPException, Request

from orchestrator.schemas.agent_child_threads import (
    AgentSessionSubagentByCallRequest,
    AgentSessionSubagentCreateRequest,
    AgentSessionSubagentQueryRequest,
    AgentSessionSubagentReopenRequest,
    AgentSessionSubagentTerminalRequest,
    AgentSubagentThreadCreateRequest,
    AgentSubagentThreadQueryRequest,
    AgentSubagentThreadReopenRequest,
    AgentSubagentThreadTerminalRequest,
    AgentThreadCreateRequest,
    AgentThreadMessageRequest,
)
from orchestrator.services.config_overrides import (
    validated_config_name as _validated_config_name,
)
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
    create_managed_repository,
    ensure_managed_repository_authority,
    revoke_and_delete_managed_repository,
)
from orchestrator.services.manifest_runtime_ownership import (
    require_srw_expert_configuration,
)
from orchestrator.services.subagent_projection import (
    subagent_thread_payload as _subagent_thread_payload,
)
from shared.backend_kinds import LITE_BACKENDS
from shared.persistent_input_delivery import InputDeliveryConflict
from shared.runtime.core.loader import canonical_config_name
from shared.session_subagent_authority import (
    SessionParentAuthority as AgentSessionSubagentAuthority,
)
from shared.session_subagent_authority import SessionParentAuthorityRefused
from shared.subagent_parent_authority import ParentExecutionAuthorityRefused

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgentChildThreadDependencies:
    """Collaborators for one agent-facing thread/child request, per invocation.

    Nothing is captured at import. ``store``, ``gitea_client`` and
    ``container_provisioner`` are rebound during ``lifespan`` and by tests;
    ``require_internal``, the three session-config seams and
    ``backend_from_override`` are names tests patch on the application module,
    and importing them here would resolve a different object than the one a
    caller patched.
    """

    store: Any
    gitea_client: Any
    container_provisioner: Any
    logger: logging.Logger

    # Guard (orchestrator.security.access, bound by main).
    require_internal: Callable[[Request], Awaitable[None]]

    # Import-time gate as a callable (port contract §P1).
    is_experts_db_enabled: Callable[[], bool]

    # Session configuration resolution seams the application owns.
    resolve_config: Callable[..., Any]
    prefetch_roster_refs: Callable[..., Awaitable[Any]]
    resolve_session_account_defaults: Callable[..., Awaitable[Any]]
    backend_from_override: Callable[[dict[str, Any]], Any]


async def agent_create_thread(
    request: Request,
    body: AgentThreadCreateRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """Agent creates its own thread on startup. **Internal** (P4b) —
    requires ``X-Internal-Key``. Ingress strips this path.

    Used by persistent agents starting with ORCHESTRATOR_URL set.
    Creates a thread with user_id=NULL (visible to all cockpit users).
    """
    postgres_db = dependencies.store
    gitea_client = dependencies.gitea_client
    container_provisioner = dependencies.container_provisioner

    try:
        # Same write boundary as the user-facing funnel: the internal key
        # authenticates the transport, not the body it carries.
        config_name = canonical_config_name(
            _validated_config_name(body.config_name) or "session_base"
        )
        selected_expert = None
        if dependencies.is_experts_db_enabled() and config_name == "session_base":
            selected_expert = await postgres_db.get_application_expert_default(
                "session"
            )
            if not selected_expert:
                raise HTTPException(
                    status_code=503,
                    detail="No application session expert default is configured",
                )

        require_srw_expert_configuration(selected_expert, interactive=True)
        create_capture: dict[str, Any] = {}
        dependencies.resolve_config(
            base_config_name=config_name,
            base_defaults=await dependencies.resolve_session_account_defaults(None),
            expert_row=selected_expert,
            expert_type="session",
            capture=create_capture,
            db_refs=await dependencies.prefetch_roster_refs(expert_row=selected_expert),
        )
        effective_backend = dependencies.backend_from_override(
            create_capture["merged_fragment"]
        )
        effective_narration_mode = (
            create_capture["merged_fragment"].get("interactive") or {}
        ).get("narration_mode") or "auto"
        config_override: dict[str, Any] = {}
        if effective_backend:
            config_override = {"workspace": {"backend": effective_backend}}

        metadata_patch: dict[str, Any] = {"config_override": config_override}
        if selected_expert:
            metadata_patch.update(
                {
                    "expert_id": str(selected_expert["id"]),
                    "expert_selection_source": "application",
                }
            )

        thread_id = await postgres_db.create_thread(
            user_id=None,
            config_name=config_name,
            permission_mode=body.permission_mode,
            narration_mode=effective_narration_mode,
            title=body.title,
            initial_metadata=metadata_patch,
            datasource_ids=[],
            datasource_selection_provenance={
                "origin": "system_empty",
                "creation_path": "internal_agent_thread",
                "effective_work_owner_id": None,
                "initiating_actor_id": None,
                "project_ids": [],
                "datasource_ids": [],
                "policy_revisions": {},
                "materialized_at": datetime.now(timezone.utc).isoformat(),
            },
        )

        # Create Gitea repo for workspace versioning
        if not gitea_client.is_initialized and gitea_client.is_configured:
            await gitea_client.ensure_initialized()
        if gitea_client.is_initialized:
            repo_name = f"thread-{thread_id[:8]}"
            try:
                git_remote_url, creation_intent = await create_managed_repository(
                    postgres_db,
                    gitea_client,
                    repo_name=repo_name,
                    authority_kind="thread",
                    authority_id=thread_id,
                    project_id=None,
                    access_mode="write",
                )
                if git_remote_url:
                    repository_authority = await ensure_managed_repository_authority(
                        postgres_db,
                        gitea_client,
                        repo_name=repo_name,
                        authority_kind="thread",
                        authority_id=thread_id,
                        access_mode="write",
                        creation_intent_id=str(creation_intent["id"]),
                    )
                if not await postgres_db.bind_thread_managed_repository(
                    thread_id,
                    repo_name=repo_name,
                    clean_url=str(repository_authority["clean_repo_url"]),
                ):
                    await revoke_and_delete_managed_repository(
                        postgres_db, gitea_client, repo_name
                    )
                    raise HTTPException(
                        status_code=503,
                        detail="Scoped workspace repository binding failed",
                    )
            except ManagedRepositoryAuthorityError as exc:
                await revoke_and_delete_managed_repository(
                    postgres_db, gitea_client, repo_name
                )
                raise HTTPException(
                    status_code=503,
                    detail="Scoped workspace repository authority unavailable",
                ) from exc

        # Provision workspace container in background if K8s is available
        # (in-cluster only) — unless this is a lite (virtual/none) session, which
        # runs with no workspace pod at all (no_workspace_agent_mode.md §4).
        if (
            container_provisioner.is_available
            and container_provisioner.in_cluster
            and dependencies.backend_from_override(config_override) not in LITE_BACKENDS
        ):
            asyncio.create_task(
                container_provisioner.create_pinned_thread_workspace(thread_id)
            )

        return {"thread_id": thread_id, "status": "created"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def agent_create_subagent_thread(
    request: Request,
    job_id: str,
    body: AgentSubagentThreadCreateRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """Create the ``threads`` row of a subagent child of a job (U3 B.1).
    **Internal** — requires ``X-Internal-Key``. Ingress strips this path.

    The orchestrator owns thread creation: the row is derived from the JOB
    (``user_id`` / ``project_id`` come from ``jobs``, never from the body),
    which is what makes the child's transcript readable by the job owner
    through the ordinary thread endpoints and keeps it off every other
    user's sessions page. Nothing is provisioned — no repository, no
    workspace, no pod: a child runs inside its parent's. Compare
    ``POST /api/agents/threads``, which provisions a session.

    Idempotent per ``subagent_id`` while the parent remains open: a retried
    create returns the same id. Once a completion decision is journaled even
    an exact retry is refused, so completion cannot race a child revival.
    404 when the job does not exist (the FK would refuse the row anyway).
    """
    postgres_db = dependencies.store

    try:
        created = await postgres_db.create_subagent_thread(
            parent_job_id=job_id,
            parent_authority=body.parent_authority,
            thread_id=str(body.subagent_id) if body.subagent_id else None,
            handle=body.handle,
            subagent_type=body.subagent_type,
            parent_tool_call_id=body.parent_tool_call_id,
            isolation=body.isolation,
            write_policy=body.write_policy,
            owned_paths=body.owned_paths,
            brief_description=body.brief_description,
            parent_iteration=body.parent_iteration,
            fork=body.fork,
            run_in_background=body.run_in_background,
            initial_status=body.initial_status,
        )
    except ParentExecutionAuthorityRefused as e:
        raise HTTPException(status_code=409, detail=e.detail()) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    if created is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return {**created, "status": "created"}


async def agent_list_live_subagent_threads(
    request: Request,
    job_id: str,
    body: AgentSubagentThreadQueryRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """List generation-bearing queued/running children. Internal only."""
    postgres_db = dependencies.store

    try:
        rows = await postgres_db.list_live_subagent_threads(
            job_id, parent_authority=body.parent_authority
        )
    except ParentExecutionAuthorityRefused as e:
        raise HTTPException(status_code=409, detail=e.detail()) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return {
        "job_id": job_id,
        "count": len(rows),
        "subagents": [_subagent_thread_payload(row) for row in rows],
    }


async def agent_get_subagent_thread(
    request: Request,
    job_id: str,
    thread_id: UUID,
    body: AgentSubagentThreadQueryRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """Read one exact worker child, including its generation. Internal only."""
    postgres_db = dependencies.store

    try:
        row = await postgres_db.get_subagent_thread(
            job_id,
            str(thread_id),
            parent_authority=body.parent_authority,
        )
    except ParentExecutionAuthorityRefused as e:
        raise HTTPException(status_code=409, detail=e.detail()) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    if row is None:
        raise HTTPException(status_code=404, detail="Subagent thread not found")
    return _subagent_thread_payload(row)


async def agent_reopen_subagent_thread(
    request: Request,
    job_id: str,
    thread_id: UUID,
    body: AgentSubagentThreadReopenRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """Rotate an ended child to a queued generation. Internal only."""
    postgres_db = dependencies.store

    try:
        result = await postgres_db.reopen_subagent_thread(
            parent_job_id=job_id,
            thread_id=str(thread_id),
            runtime_generation=str(body.runtime_generation),
            parent_authority=body.parent_authority,
        )
    except ParentExecutionAuthorityRefused as e:
        raise HTTPException(status_code=409, detail=e.detail()) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    if result is None:
        raise HTTPException(status_code=404, detail="Job or subagent thread not found")
    if result.get("result") != "reopened":
        raise HTTPException(status_code=409, detail=result)
    return result


async def agent_terminalize_subagent_thread(
    request: Request,
    job_id: str,
    thread_id: UUID,
    body: AgentSubagentThreadTerminalRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """Atomically terminalize one run and enqueue its stable report."""
    postgres_db = dependencies.store

    try:
        result = await postgres_db.terminalize_subagent_thread_and_enqueue(
            parent_job_id=job_id,
            thread_id=str(thread_id),
            runtime_generation=str(body.runtime_generation),
            parent_authority=body.parent_authority,
            delivery_id=str(body.delivery_id),
            message=body.message,
            timestamp=body.timestamp,
            subagent_status=body.subagent_status,
            outcome=body.outcome,
            turns=body.turns,
            tokens=body.tokens,
            report_path=body.report_path,
            error=body.error,
        )
    except ParentExecutionAuthorityRefused as e:
        raise HTTPException(status_code=409, detail=e.detail()) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    if result is None:
        raise HTTPException(status_code=404, detail="Job or subagent thread not found")
    if result.get("result") not in {"applied", "idempotent"}:
        raise HTTPException(status_code=409, detail=result)
    return result


def session_subagent_authority_wire(
    authority: AgentSessionSubagentAuthority,
) -> dict[str, Any]:
    return authority.model_dump(mode="json")


async def agent_create_session_subagent_thread(
    request: Request,
    parent_thread_id: str,
    body: AgentSessionSubagentCreateRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """Create a child of one exact persistent-session runtime. Internal only."""
    postgres_db = dependencies.store

    try:
        created = await postgres_db.create_session_subagent_thread(
            parent_thread_id=parent_thread_id,
            parent_authority=session_subagent_authority_wire(body.parent_authority),
            thread_id=str(body.subagent_id) if body.subagent_id else None,
            handle=body.handle,
            subagent_type=body.subagent_type,
            parent_tool_call_id=body.parent_tool_call_id,
            parent_input_message_id=(
                str(body.parent_input_message_id)
                if body.parent_input_message_id is not None
                else None
            ),
            parent_ai_message_id=(
                str(body.parent_ai_message_id)
                if body.parent_ai_message_id is not None
                else None
            ),
            isolation=body.isolation,
            write_policy=body.write_policy,
            owned_paths=body.owned_paths,
            brief_description=body.brief_description,
            parent_iteration=body.parent_iteration,
            fork=body.fork,
            run_in_background=body.run_in_background,
            initial_status=body.initial_status,
        )
    except SessionParentAuthorityRefused as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if created is None:
        raise HTTPException(status_code=404, detail="Parent session not found")
    return {**created, "status": "created"}


async def agent_list_live_session_subagent_threads(
    request: Request,
    parent_thread_id: str,
    body: AgentSessionSubagentQueryRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """List session child recovery candidates under exact authority."""
    postgres_db = dependencies.store

    try:
        rows = await postgres_db.list_live_session_subagent_threads(
            parent_thread_id,
            parent_authority=session_subagent_authority_wire(body.parent_authority),
        )
    except SessionParentAuthorityRefused as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {
        "parent_thread_id": parent_thread_id,
        "count": len(rows),
        "subagents": [_subagent_thread_payload(row) for row in rows],
    }


async def agent_get_session_subagent_thread_by_call(
    request: Request,
    parent_thread_id: str,
    body: AgentSessionSubagentByCallRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """Resolve a replayed session delegation call. Internal only."""
    postgres_db = dependencies.store

    try:
        row = await postgres_db.get_session_subagent_thread_by_call(
            parent_thread_id,
            body.parent_tool_call_id,
            parent_authority=session_subagent_authority_wire(body.parent_authority),
        )
    except SessionParentAuthorityRefused as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if row is None:
        raise HTTPException(status_code=404, detail="Subagent thread not found")
    return _subagent_thread_payload(row)


async def agent_get_session_subagent_thread(
    request: Request,
    parent_thread_id: str,
    thread_id: UUID,
    body: AgentSessionSubagentQueryRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """Read one session child and its generation. Internal only."""
    postgres_db = dependencies.store

    try:
        row = await postgres_db.get_session_subagent_thread(
            parent_thread_id,
            str(thread_id),
            parent_authority=session_subagent_authority_wire(body.parent_authority),
        )
    except SessionParentAuthorityRefused as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if row is None:
        raise HTTPException(status_code=404, detail="Subagent thread not found")
    return _subagent_thread_payload(row)


async def agent_reopen_session_subagent_thread(
    request: Request,
    parent_thread_id: str,
    thread_id: UUID,
    body: AgentSessionSubagentReopenRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """Rotate one terminal session child generation. Internal only."""
    postgres_db = dependencies.store

    try:
        result = await postgres_db.reopen_session_subagent_thread(
            parent_thread_id=parent_thread_id,
            thread_id=str(thread_id),
            runtime_generation=str(body.runtime_generation),
            parent_authority=session_subagent_authority_wire(body.parent_authority),
        )
    except SessionParentAuthorityRefused as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Session or child not found")
    if result.get("result") != "reopened":
        raise HTTPException(status_code=409, detail=result)
    return result


async def agent_terminalize_session_subagent_thread(
    request: Request,
    parent_thread_id: str,
    thread_id: UUID,
    body: AgentSessionSubagentTerminalRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """End a session child and atomically persist a background report event."""
    postgres_db = dependencies.store

    try:
        result = await postgres_db.terminalize_session_subagent_thread(
            parent_thread_id=parent_thread_id,
            thread_id=str(thread_id),
            runtime_generation=str(body.runtime_generation),
            parent_authority=session_subagent_authority_wire(body.parent_authority),
            subagent_status=body.subagent_status,
            delivery_id=str(body.delivery_id) if body.delivery_id else None,
            message=body.message,
            outcome=body.outcome,
            turns=body.turns,
            tokens=body.tokens,
            report_path=body.report_path,
            error=body.error,
            foreground_orphan_recovery=body.foreground_orphan_recovery,
        )
    except SessionParentAuthorityRefused as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc
    except InputDeliveryConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "subagent_delivery_conflict", "message": str(exc)},
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Session or child not found")
    if result.get("result") not in {
        "applied",
        "idempotent",
        "already_delivered",
    }:
        raise HTTPException(status_code=409, detail=result)
    return result


async def agent_save_message(
    request: Request,
    thread_id: str,
    body: AgentThreadMessageRequest,
    *,
    dependencies: AgentChildThreadDependencies,
) -> dict[str, Any]:
    """Agent saves a message to thread history. **Internal** (P4b) —
    requires ``X-Internal-Key``. Ingress strips this path.

    Fire-and-forget safe — agents call this after each turn.
    """
    postgres_db = dependencies.store

    try:
        message_id = await postgres_db.save_thread_message(
            thread_id=thread_id,
            role=body.role,
            content=body.content,
            tool_calls=body.tool_calls,
            turn_number=body.turn_number,
            metrics=body.metrics,
            tool_call_id=body.tool_call_id,
            thinking=body.thinking,
            reasoning=body.reasoning,
            tool_results=body.tool_results,
            provider=body.provider,
            provider_raw=body.provider_raw,
            additional_kwargs=body.additional_kwargs,
            response_metadata=body.response_metadata,
        )
        return {"message_id": message_id, "status": "saved"}
    except RuntimeError as e:
        if "legacy message writer is unavailable for stateless threads" in str(e):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "stateless_legacy_writer_refused",
                    "message": str(e),
                },
            ) from e
        raise HTTPException(status_code=500, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


__all__ = [
    "AgentChildThreadDependencies",
    "agent_create_session_subagent_thread",
    "agent_create_subagent_thread",
    "agent_create_thread",
    "agent_get_session_subagent_thread",
    "agent_get_session_subagent_thread_by_call",
    "agent_get_subagent_thread",
    "agent_list_live_session_subagent_threads",
    "agent_list_live_subagent_threads",
    "agent_reopen_session_subagent_thread",
    "agent_reopen_subagent_thread",
    "agent_save_message",
    "agent_terminalize_session_subagent_thread",
    "agent_terminalize_subagent_thread",
    "session_subagent_authority_wire",
]
