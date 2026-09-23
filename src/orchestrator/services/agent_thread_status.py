"""Thread status, suspend and attach release — the agent-facing lifecycle edges.

R1.B06, root lane. These are the writes that decide whether a pinned session's
runtime still owns its thread, so they are the batch's highest-risk nodes and
they stayed with the integrator rather than going to a lane.

Three properties travelled with them:

* **Terminal state is fenced, and a retry is a reconciliation channel.** An
  exact final-status retry — the complete ``(agent, generation, attach token,
  retirement token, disposition, permanent)`` tuple — is answered from the
  append-only retirement outcome, which survives suspended generation
  rotation, a quick Resume and permanent row deletion. A *generic* missing row
  or a changed generation is deliberately **not** treated as success.
* **A pinned write must prove reciprocal ownership.** Where the lane is pinned
  and either protected cloud is engaged or the deployment gate demands it, the
  caller must present agent id, runtime generation and attach token, and an
  ``ending``/``ended`` edge must also present its disposition. Missing identity
  is a refusal, never a downgrade to the unfenced path.
* **Release is quiescence-proofed.** Clearing ``threads.agent_id`` requires the
  exact process-zero protocol and the registered Pod identity; the successor is
  only scheduled once the release actually released.

Transport stays in ``routers/agent_thread_status.py``: ``require_internal`` and
the raw JSON read are the router's. Everything that decides an outcome —
including the 400 on a missing ``agent_id`` — is here, so it can be tested
without a request.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from collections.abc import Mapping
from typing import Any, Awaitable, Callable
from uuid import UUID, uuid4

from fastapi import HTTPException

from orchestrator.schemas.agent_thread_status import AgentThreadStatusRequest
from orchestrator.services.deployment_gates import require_pinned_status_identity
from orchestrator.services.officer_metadata import (
    officer_meta_enabled,
    thread_officer_meta,
)
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
    thread_runtime_refusal_detail,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.services.vm_remote_operation import (
    VMRemoteOperationUnavailable,
    _identity_from_row,
)
from shared.workspace_idle_policy import RuntimeIdentity
from shared.workspace_idle_store import apply_idle_transition_on_conn

logger = logging.getLogger(__name__)

__all__ = [
    "AgentThreadStatusDependencies",
    "AgentThreadStatusRequest",
    "release_thread_agent",
    "suspend_thread",
    "update_thread_status",
]


@dataclass(frozen=True)
class AgentThreadStatusDependencies:
    """Collaborators resolved per invocation from the handling application."""

    db: Any
    persistent_thread_recycler: Any
    #: The router's transport guard, resolved from the handling app.
    require_internal: Callable[..., Awaitable[Any]]
    #: B06 lane A — the attach binding surface.
    thread_accepts_runtime: Callable[..., Any]
    release_session_attach_binding: Callable[..., Awaitable[Any]]
    acknowledge_retiring_failed_attach: Callable[..., Awaitable[Any]]
    schedule_attach_abort_successor: Callable[..., Any]
    #: B09 owns retirement; B06 only asks for it.
    begin_pinned_thread_retirement: Callable[..., Awaitable[Any]]
    end_thread_flow: Callable[..., Awaitable[Any]]
    suspend_thread_resources: Callable[..., Awaitable[Any]]
    #: B07 owns Officer conferences.
    conclude_conference_if_any: Callable[..., Awaitable[Any]]


async def update_thread_status(
    thread_id: str,
    body: AgentThreadStatusRequest,
    *,
    dependencies: AgentThreadStatusDependencies,
) -> dict[str, Any]:
    """Update thread status. **Internal** (P4b) — requires ``X-Internal-Key``.
    Ingress strips this path.

    Lifecycle transitions:
      created → active, active → ended (existing).
      active → awaiting_user (Phase 5: agent reached natural pause, no WS
        subscriber). Idempotent — repeated awaiting_user writes preserve
        the original awaiting_user_since so the attention-sleep watchdog's
        clock keeps ticking.
      awaiting_user → active (Phase 5: subscriber reattached). Clears
        awaiting_user_since and extend_count.

    'suspended' is reserved for the attention-sleep watchdog and is not
    writable from agent path — would create a race where an agent flips
    the thread back to active while the orchestrator is mid-suspend.
    """
    valid_statuses = {"active", "ending", "ended", "awaiting_user"}
    if body.status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Status must be one of: {valid_statuses}",
        )
    lane_thread = await dependencies.db.get_thread(thread_id)
    if (
        body.status == "ended"
        and body.agent_id is not None
        and body.session_runtime_generation is not None
        and body.session_runtime_attach_token is not None
        and body.session_runtime_retirement_token is not None
        and body.retirement_disposition is not None
    ):
        # Exact final-status retries are also the lost-200 reconciliation
        # channel. The append-only outcome survives suspended generation
        # rotation, quick Resume and permanent row deletion, and is keyed by
        # the complete old process/T tuple. A generic missing row or G change
        # is deliberately not treated as success.
        exact_outcome = await dependencies.db.get_pinned_thread_retirement_outcome(
            thread_id,
            runtime_generation=str(body.session_runtime_generation),
            retirement_token=str(body.session_runtime_retirement_token),
            agent_id=str(body.agent_id),
            runtime_attach_token=str(body.session_runtime_attach_token),
            disposition=str(body.retirement_disposition),
            permanent=bool(body.retirement_permanent),
        )
        if exact_outcome is not None:
            return exact_outcome
        if lane_thread is None:
            raise HTTPException(
                status_code=409,
                detail={"code": "pinned_retirement_outcome_unproven"},
            )
    lane_metadata = thread_metadata_object(lane_thread)
    pinned_identity_required = bool(
        lane_thread is not None
        and lane_thread.get("execution_lane") == "pinned"
        and (
            protected_cloud_marker_state(lane_metadata) != "off"
            or require_pinned_status_identity()
        )
    )
    if (
        lane_thread is not None
        and lane_thread.get("execution_lane") == "pinned"
        and (
            body.agent_id is None
            or body.session_runtime_generation is None
            or body.session_runtime_attach_token is None
            or (
                body.status in {"ending", "ended"}
                and body.retirement_disposition is None
            )
            or (
                body.status == "ended" and body.session_runtime_retirement_token is None
            )
        )
        and pinned_identity_required
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "pinned_status_identity_required",
                "message": (
                    "Pinned session status updates require agent, runtime "
                    "generation, and process attach identity."
                ),
            },
        )
    if body.status == "ending" and (
        body.agent_id is None
        or body.session_runtime_generation is None
        or body.session_runtime_attach_token is None
        or body.retirement_disposition is None
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "pinned_status_identity_required"},
        )
    if body.status == "ending" and body.retirement_permanent:
        raise HTTPException(
            status_code=409,
            detail={"code": "agent_permanent_retirement_forbidden"},
        )
    if (
        lane_thread is not None
        and lane_thread.get("execution_lane") == "pinned"
        and body.session_runtime_generation is not None
        and str(lane_thread.get("runtime_generation") or "")
        != str(body.session_runtime_generation)
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "pinned_runtime_generation_mismatch"},
        )
    if (
        body.status == "ended"
        and body.agent_id is None
        and dependencies.persistent_thread_recycler is not None
    ):
        recycle_boundary = (
            await dependencies.persistent_thread_recycler.acknowledge_parked_boundary(
                thread_id=thread_id,
                agent_id=None,
            )
        )
        if recycle_boundary.acknowledged:
            return {"status": "suspended"}
        if recycle_boundary.active_generation:
            raise HTTPException(
                status_code=409,
                detail="Persistent recycle owns this thread transition",
            )
    if body.status in {
        "active",
        "awaiting_user",
    } and not dependencies.thread_accepts_runtime(lane_thread):
        raise HTTPException(
            status_code=409,
            detail=thread_runtime_refusal_detail(lane_thread),
        )
    if (
        lane_thread is not None
        and lane_thread.get("execution_lane") == "stateless"
        and body.agent_id is None
    ):
        # Queue-served lifecycle/presence writes carry an exact lease and use
        # src.shared.session_retirement. A generic pod request has no owner
        # credential and must never resurrect an ended/retiring thread.
        raise HTTPException(
            status_code=409,
            detail="Stateless status updates require exact lease authority",
        )
    if body.agent_id is not None:
        agent_id = body.agent_id
        async with dependencies.db.acquire() as conn:
            async with conn.transaction():
                thread_record = await conn.fetchrow(
                    "SELECT id, agent_id, execution_lane, status, metadata, "
                    "project_id, title, runtime_generation, "
                    "runtime_attach_token, runtime_retirement_token, "
                    "workspace_idle_revision, workspace_idle_episode "
                    "FROM threads WHERE id = $1::uuid FOR UPDATE",
                    thread_id,
                )
                if (
                    thread_record is None
                    or str(thread_record["execution_lane"] or "") != "pinned"
                    or thread_record["agent_id"] != agent_id
                    or (
                        body.session_runtime_generation is not None
                        and str(thread_record["runtime_generation"])
                        != str(body.session_runtime_generation)
                    )
                    or (
                        thread_record["runtime_retirement_token"] is not None
                        and body.status not in {"ending", "ended"}
                    )
                    or str(thread_record["runtime_attach_token"] or "")
                    != str(body.session_runtime_attach_token or "")
                ):
                    raise HTTPException(
                        status_code=409,
                        detail="Pinned session ownership changed before teardown",
                    )
                if str(thread_record["status"] or "") == "ended":
                    return {"status": "ended"}
                reciprocal = await conn.fetchrow(
                    "SELECT pod_uid, metadata FROM agents WHERE id = $1::uuid "
                    "AND thread_id = $2::uuid FOR SHARE",
                    agent_id,
                    thread_id,
                )
                reciprocal_metadata = (
                    thread_metadata_object({"metadata": reciprocal["metadata"]})
                    if reciprocal is not None
                    else {}
                )
                if (
                    reciprocal is None
                    or (str(reciprocal["pod_uid"] or "") or None)
                    != (str(body.pod_uid or "") or None)
                    or str(reciprocal_metadata.get("dispatch_process_generation") or "")
                    != str(body.process_generation or "")
                ):
                    raise HTTPException(
                        status_code=409,
                        detail="Pinned session process ownership changed",
                    )

                thread_row = dict(thread_record)
                officer = officer_meta_enabled(thread_officer_meta(thread_row))
                if (
                    body.status == "ended"
                    and officer
                    and body.retirement_disposition not in {None, "suspended"}
                ):
                    raise HTTPException(
                        status_code=409,
                        detail={"code": "pinned_retirement_disposition_mismatch"},
                    )
                if body.status == "ending":
                    updated = thread_record["id"]
                    result_status = "begin_retirement"
                elif (
                    body.status == "ended"
                    and body.retirement_disposition == "suspended"
                ):
                    # Graceful Officer shutdown is a runtime suspension, not
                    # Post retirement. It still must close admission before
                    # cleanup and rotate generation at settlement; a direct
                    # suspended write reopened the watchdog while G1 cleanup
                    # was deleting deterministic resource names.
                    updated = thread_record["id"]
                    result_status = "retiring_suspended"
                elif body.status == "ended":
                    # The dedicated pinned retirement funnel owns the first
                    # lifecycle write. Directly setting ended here would
                    # reopen the historical End->Resume cleanup ABA window.
                    updated = thread_record["id"]
                    result_status = "retiring"
                elif body.status == "awaiting_user":
                    updated = await conn.fetchval(
                        "UPDATE threads "
                        "SET status = 'awaiting_user', "
                        "    awaiting_user_since = CASE "
                        "        WHEN status = 'awaiting_user' "
                        "             THEN awaiting_user_since "
                        "        ELSE now() "
                        "    END, "
                        "    extend_count = CASE "
                        "        WHEN status = 'awaiting_user' THEN extend_count "
                        "        ELSE 0 "
                        "    END, "
                        "    last_activity = CURRENT_TIMESTAMP "
                        "WHERE id = $1::uuid AND agent_id = $2::uuid "
                        "AND status <> 'ended' "
                        "AND ($3::uuid IS NULL OR runtime_generation=$3::uuid) "
                        "AND runtime_attach_token IS NOT DISTINCT FROM $4::uuid "
                        "AND runtime_retirement_token IS NULL RETURNING id",
                        thread_id,
                        agent_id,
                        body.session_runtime_generation,
                        body.session_runtime_attach_token,
                    )
                    result_status = "awaiting_user"
                else:
                    updated = await conn.fetchval(
                        "UPDATE threads "
                        "SET status = 'active', "
                        "    awaiting_user_since = NULL, "
                        "    extend_count = 0, "
                        "    last_activity = CURRENT_TIMESTAMP "
                        "WHERE id = $1::uuid AND agent_id = $2::uuid "
                        "AND status <> 'ended' "
                        "AND ($3::uuid IS NULL OR runtime_generation=$3::uuid) "
                        "AND runtime_attach_token IS NOT DISTINCT FROM $4::uuid "
                        "AND runtime_retirement_token IS NULL RETURNING id",
                        thread_id,
                        agent_id,
                        body.session_runtime_generation,
                        body.session_runtime_attach_token,
                    )
                    result_status = "active"
                if updated is None:
                    raise HTTPException(
                        status_code=409,
                        detail="Pinned session ownership changed before teardown",
                    )
                if (
                    result_status == "awaiting_user"
                    and thread_record["status"] == "active"
                    and thread_record["workspace_idle_episode"] is None
                ):
                    metadata = thread_metadata_object(thread_row)
                    vm = metadata.get("vm")
                    try:
                        authority = _identity_from_row(
                            thread_row, owner_kind="thread",
                            owner_id=str(thread_id), operation_kind="idle_policy",
                        )
                        if (
                            not isinstance(vm, dict)
                            or vm.get("status") != "ready"
                            or str(UUID(str(vm.get("vm_uid")))) != authority.vm_uid
                            or str(UUID(str(vm.get("vmi_uid")))) != vm.get("vmi_uid")
                            or str(UUID(str(vm.get("active_pod_uid"))))
                                != authority.launcher_pod_uid
                            or str(UUID(str(vm.get("rootdisk_pvc_uid"))))
                                != vm.get("rootdisk_pvc_uid")
                        ):
                            raise ValueError("unproven VM tuple")
                    except (VMRemoteOperationUnavailable, ValueError, TypeError):
                        pass
                    else:
                        await apply_idle_transition_on_conn(
                            conn,
                            runtime=RuntimeIdentity(
                                "thread", str(thread_id), "vm",
                                authority.workspace_generation, authority.vm_uid,
                            ),
                            event="enter",
                            expected_revision=thread_record["workspace_idle_revision"],
                            expected_episode_id=None,
                            wait_kind="natural_pause", wait_key=str(uuid4()),
                        )

        if result_status == "begin_retirement":
            retirement = await dependencies.begin_pinned_thread_retirement(
                thread_id,
                permanent=bool(body.retirement_permanent),
                settle_status=str(body.retirement_disposition),
                initiator="agent",
                expected_runtime_generation=str(body.session_runtime_generation),
                expected_agent_id=str(agent_id),
                expected_attach_token=str(body.session_runtime_attach_token),
                authorize_immediately=True,
            )
            if (
                retirement.get("state") != "pending"
                or retirement.get("authorized_at") is None
            ):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "pinned_retirement_conflict",
                        "reason": retirement.get("reason") or retirement.get("state"),
                    },
                )
            return {
                "status": "ending",
                "retirement_disposition": str(body.retirement_disposition),
                "retirement_permanent": bool(retirement.get("permanent")),
                "session_runtime_retirement_token": str(retirement["token"]),
            }
        if result_status in {"retiring", "retiring_suspended"}:
            if body.retirement_disposition is not None and (
                (result_status == "retiring_suspended")
                != (body.retirement_disposition == "suspended")
            ):
                raise HTTPException(
                    status_code=409,
                    detail={"code": "pinned_retirement_disposition_mismatch"},
                )
            settle_disposition = (
                str(body.retirement_disposition)
                if body.retirement_disposition is not None
                else ("suspended" if result_status == "retiring_suspended" else "ended")
            )
            if not body.local_runtime_quiesced:
                raise HTTPException(
                    status_code=409,
                    detail={"code": "pinned_local_quiescence_required"},
                )
            if body.session_runtime_retirement_token is None:
                raise HTTPException(
                    status_code=409,
                    detail={"code": "pinned_retirement_token_required"},
                )
            # A direct final request may race or replace the begin-only call.
            # Install/reuse the immutable marker first, then append the exact
            # local proof under that token before any staging or cleanup.
            retirement = await dependencies.begin_pinned_thread_retirement(
                thread_id,
                permanent=bool(body.retirement_permanent),
                settle_status=settle_disposition,
                initiator="agent",
                expected_runtime_generation=str(body.session_runtime_generation),
                expected_agent_id=str(agent_id),
                expected_attach_token=str(body.session_runtime_attach_token),
                expected_retirement_token=str(body.session_runtime_retirement_token),
            )
            if retirement.get("state") != "pending":
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "pinned_retirement_conflict",
                        "reason": retirement.get("reason") or retirement.get("state"),
                    },
                )
            local_receipt = (
                await dependencies.db.acknowledge_pinned_thread_local_quiescence(
                    thread_id,
                    expected_runtime_generation=str(body.session_runtime_generation),
                    expected_retirement_token=str(
                        body.session_runtime_retirement_token
                    ),
                    expected_agent_id=str(agent_id),
                    expected_attach_token=str(body.session_runtime_attach_token),
                    expected_settle_status=settle_disposition,
                    expected_quiescence_protocol=str(
                        body.local_quiescence_protocol or ""
                    ),
                    expected_workspace_generation=(
                        str(body.workspace_generation)
                        if body.workspace_generation is not None
                        else None
                    ),
                    expected_workspace_runtime_incarnation=(
                        str(body.workspace_runtime_incarnation)
                        if body.workspace_runtime_incarnation is not None
                        else None
                    ),
                )
            )
            if local_receipt is None:
                raise HTTPException(
                    status_code=409,
                    detail={"code": "pinned_local_quiescence_refused"},
                )
            retired = await dependencies.end_thread_flow(
                thread_id,
                thread_row,
                permanent=bool(body.retirement_permanent),
                force=True,
                expected_runtime_generation=(
                    str(body.session_runtime_generation)
                    if body.session_runtime_generation is not None
                    else None
                ),
                expected_agent_id=str(agent_id),
                expected_attach_token=(
                    str(body.session_runtime_attach_token)
                    if body.session_runtime_attach_token is not None
                    else None
                ),
                settle_status=settle_disposition,
                local_runtime_quiesced=True,
                retiring_agent_response_pending=bool(body.retirement_permanent),
            )
            return retired
        # Terminal exact-owner writes return through the retirement funnel
        # above. Reaching this point means the pinned runtime only acknowledged
        # a live presence transition; tearing down its resources here would
        # revoke the authority that the successful status write just proved.
        return {"status": result_status}
    try:
        if body.status == "ended":
            # Officer sessions (centurion.md §4): an agent-side 'ended' is a
            # GRACEFUL termination — pod delete, node drain, deploy rollout,
            # or the shutdown handler racing a suspend. For an officer that
            # must map to 'suspended', the designed routine down-state the
            # watchdog respawns from; writing 'ended' here permanently kills
            # the officer with his flag still raised (observed on the k3d
            # smoke: kubectl delete pod → shutdown handler → 'ended' → no
            # respawn). Deliberate retirement goes through end_thread, which
            # also lowers officer.enabled. True crashes (SIGKILL) never reach
            # this endpoint and take the watchdog's dead-pod path instead.
            thread_row = await dependencies.db.get_thread(thread_id)
            if thread_row is not None and officer_meta_enabled(
                thread_officer_meta(thread_row)
            ):
                async with dependencies.db.acquire() as conn:
                    updated = await conn.fetchval(
                        "UPDATE threads "
                        "SET status = 'suspended', agent_id = NULL, "
                        "    control_admission_agent_id = NULL, "
                        "    awaiting_user_since = NULL "
                        "WHERE id = $1 AND status <> 'ended' RETURNING id",
                        thread_id,
                    )
                if updated is None:
                    raise HTTPException(
                        status_code=409,
                        detail=thread_runtime_refusal_detail(
                            await dependencies.db.get_thread(thread_id)
                        ),
                    )
                logger.info(
                    "Officer thread %s: agent-side 'ended' mapped to "
                    "'suspended' (watchdog will respawn)",
                    thread_id[:8],
                )
                return {"status": "suspended"}
            # Guarded end (mirrors end_thread, which stays unguarded for
            # user-intent call sites): a late agent-side 'ended' — e.g. the
            # SIGTERM shutdown handler of a pod deleted mid-suspend, or the
            # drain-suspend fallback racing a lost suspend response — must
            # never clobber an orchestrator-driven 'suspended' thread.
            async with dependencies.db.acquire() as conn:
                updated = await conn.fetchval(
                    "UPDATE threads "
                    "SET status = 'ended', ended_at = CURRENT_TIMESTAMP, "
                    "    control_admission_agent_id = NULL "
                    "WHERE id = $1 AND status <> 'suspended' "
                    "RETURNING id",
                    thread_id,
                )
            if updated:
                # Agent-initiated `ended` (idle timeout, watchdog, WS
                # disconnect) is almost always recoverable, not a user-intent
                # delete — preserve the workspace via S3 snapshot so /resume
                # can restore it. The user-facing DELETE handler still uses
                # _release_thread_resources for true destruction.
                # See knowledge-base/knowledge/issues/persistent_session_permission_check_race.md.
                asyncio.create_task(dependencies.suspend_thread_resources(thread_id))
                # A conference ending by idle-archive concludes the meeting
                # exactly like a deliberate end: release the officer's hold
                # + brief wake (centurion.md §4). thread_row was loaded above.
                if thread_row is not None:
                    await dependencies.conclude_conference_if_any(thread_row)
            else:
                logger.info(
                    "Ignored agent 'ended' for thread %s — already suspended",
                    thread_id,
                )
        elif body.status == "awaiting_user":
            # Idempotent: preserve awaiting_user_since on repeated writes
            # (the agent's loop calls this on every untethered turn-complete
            # in eager mode; resetting the timestamp would let the
            # attention-sleep watchdog never fire). extend_count is also
            # preserved across repeated writes within the same session;
            # only the active→awaiting_user transition resets it.
            async with dependencies.db.acquire() as conn:
                updated = await conn.fetchval(
                    "UPDATE threads "
                    "SET status = 'awaiting_user', "
                    "    awaiting_user_since = CASE "
                    "        WHEN status = 'awaiting_user' "
                    "             THEN awaiting_user_since "
                    "        ELSE now() "
                    "    END, "
                    "    extend_count = CASE "
                    "        WHEN status = 'awaiting_user' THEN extend_count "
                    "        ELSE 0 "
                    "    END, "
                    "    last_activity = CURRENT_TIMESTAMP "
                    "WHERE id = $1 AND status <> 'ended' RETURNING id",
                    thread_id,
                )
            if updated is None:
                raise HTTPException(
                    status_code=409,
                    detail=thread_runtime_refusal_detail(
                        await dependencies.db.get_thread(thread_id)
                    ),
                )
        else:  # active
            # On revert from awaiting_user (or any other source), clear the
            # attention-sleep timer fields so the watchdog re-arms cleanly
            # on the next natural-pause transition.
            async with dependencies.db.acquire() as conn:
                updated = await conn.fetchval(
                    "UPDATE threads "
                    "SET status = $2, "
                    "    awaiting_user_since = NULL, "
                    "    extend_count = 0, "
                    "    last_activity = CURRENT_TIMESTAMP "
                    "WHERE id = $1 AND status <> 'ended' RETURNING id",
                    thread_id,
                    body.status,
                )
            if updated is None:
                raise HTTPException(
                    status_code=409,
                    detail=thread_runtime_refusal_detail(
                        await dependencies.db.get_thread(thread_id)
                    ),
                )
        return {"status": body.status}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def suspend_thread(
    thread_id: str,
    *,
    headers: Mapping[str, str],
    dependencies: AgentThreadStatusDependencies,
) -> dict[str, Any]:
    """Clean drain-suspend requested by the thread's own agent. **Internal**
    (P4b) — requires ``X-Internal-Key``. Ingress strips this path.

    Called by a persistent agent that received ``intents.should_drain``
    while its loop is parked between turns. An active dedicated-pod recycle
    generation derives the exact old agent/UID/drain intent under the durable
    locks and owns this request before any workspace action; this accepts the
    headerless shape sent by pre-change runtimes. Without an active recycle,
    the historical path converges on the attention-sleep state by snapshotting
    the workspace and clearing the agent binding.

    Returns ``{"suspended": bool, "status": <thread status>}``. The agent
    falls back to the legacy 'ended' detach when ``suspended`` is false —
    e.g. suspension service disabled, snapshot failure, or a thread already
    past the point of suspending.
    """
    requesting_agent_id = headers.get("X-Agent-ID", "").strip()
    requesting_generation = headers.get("X-Session-Runtime-Generation", "").strip()
    requesting_attach_token = headers.get("X-Session-Runtime-Attach-Token", "").strip()
    requesting_retirement_token = headers.get(
        "X-Session-Runtime-Retirement-Token", ""
    ).strip()
    local_quiesced = (
        headers.get("X-Session-Local-Quiesced", "").strip().lower() == "true"
    )
    local_quiescence_protocol = headers.get(
        "X-Session-Local-Quiescence-Protocol", ""
    ).strip()
    workspace_generation = headers.get("X-Workspace-Generation", "").strip()
    workspace_runtime_incarnation = headers.get(
        "X-Workspace-Runtime-Incarnation", ""
    ).strip()
    identity_thread = await dependencies.db.get_thread(thread_id)
    if not identity_thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    pinned_thread = str(identity_thread.get("execution_lane") or "") == "pinned"
    if not pinned_thread and dependencies.persistent_thread_recycler is not None:
        acknowledgement = (
            await dependencies.persistent_thread_recycler.acknowledge_parked_boundary(
                thread_id=thread_id,
                agent_id=requesting_agent_id or None,
            )
        )
        if acknowledgement.acknowledged:
            logger.info(
                "Persistent recycle parked boundary acknowledged for thread %s",
                thread_id,
            )
            return {
                "suspended": True,
                "status": "suspended",
                "reason": "persistent_recycle",
            }
        if acknowledgement.active_generation:
            # An active generation owns suspension exclusively. A mismatching
            # optional agent header is a failed consistency assertion and can
            # never fall into the workspace snapshot/delete legacy path.
            raise HTTPException(
                status_code=409,
                detail="Persistent recycle boundary authority did not match",
            )
    thread = await dependencies.db.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    status = thread.get("status")
    if thread.get("execution_lane") == "stateless":
        return {
            "suspended": False,
            "status": status,
            "reason": "stateless_terminal_protocol_required",
        }
    if status == "suspended":
        # Idempotent — a retried call after a lost response must not fail.
        return {"suspended": True, "status": "suspended"}
    if status not in ("created", "active", "awaiting_user"):
        return {"suspended": False, "status": status}
    if not local_quiesced:
        raise HTTPException(
            status_code=409,
            detail={"code": "pinned_local_quiescence_required"},
        )
    try:
        requesting_retirement_token = str(UUID(requesting_retirement_token))
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "pinned_retirement_token_required"},
        ) from exc
    retirement = await dependencies.begin_pinned_thread_retirement(
        thread_id,
        permanent=False,
        settle_status="suspended",
        expected_runtime_generation=requesting_generation,
        expected_agent_id=requesting_agent_id,
        expected_attach_token=requesting_attach_token,
        expected_retirement_token=requesting_retirement_token,
    )
    if retirement.get("state") != "pending":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "pinned_retirement_conflict",
                "reason": retirement.get("reason") or retirement.get("state"),
            },
        )
    if (
        await dependencies.db.acknowledge_pinned_thread_local_quiescence(
            thread_id,
            expected_runtime_generation=requesting_generation,
            expected_retirement_token=requesting_retirement_token,
            expected_agent_id=requesting_agent_id,
            expected_attach_token=requesting_attach_token,
            expected_settle_status="suspended",
            expected_quiescence_protocol=local_quiescence_protocol,
            expected_workspace_generation=workspace_generation or None,
            expected_workspace_runtime_incarnation=(
                workspace_runtime_incarnation or None
            ),
        )
    ) is None:
        raise HTTPException(
            status_code=409,
            detail={"code": "pinned_local_quiescence_refused"},
        )
    # Suspend is a resumable retirement, not a status write followed by
    # name-based cleanup. The shared funnel closes admission first, captures
    # exact physical identities, snapshots/deletes only those identities, and
    # rotates the generation as it settles ``suspended``. Resume/prepare and a
    # delayed G1 suspend therefore cannot overlap G2.
    retired = await dependencies.end_thread_flow(
        thread_id,
        thread,
        permanent=False,
        force=True,
        expected_runtime_generation=requesting_generation or None,
        expected_agent_id=requesting_agent_id or None,
        expected_attach_token=requesting_attach_token or None,
        settle_status="suspended",
        local_runtime_quiesced=True,
    )
    return {"suspended": True, "status": retired.get("status", "suspended")}


async def release_thread_agent(
    thread_id: str,
    body: Any,
    *,
    dependencies: AgentThreadStatusDependencies,
) -> dict[str, str]:
    """Clear threads.agent_id. **Internal** (P4b) — requires
    ``X-Internal-Key``. Ingress strips this path.

    Called by an agent whose /session/attach background task failed (e.g.
    workspace SSH polling timed out before the workspace pod's image pull
    completed). Without this, the thread stays bound to a session-less agent
    and the next WS reconnect re-targets the same broken agent.
    """
    agent_id = body.get("agent_id") if isinstance(body, dict) else None
    if not isinstance(agent_id, str) or not agent_id:
        raise HTTPException(status_code=400, detail="agent_id is required")
    runtime_generation = (
        body.get("session_runtime_generation") if isinstance(body, dict) else None
    )
    attach_token = (
        body.get("session_runtime_attach_token") if isinstance(body, dict) else None
    )
    agent_pod_uid = body.get("agent_pod_uid") if isinstance(body, dict) else None
    local_runtime_quiesced = (
        body.get("local_runtime_quiesced") if isinstance(body, dict) else None
    )
    local_quiescence_protocol = (
        body.get("local_quiescence_protocol") if isinstance(body, dict) else None
    )
    workspace_generation = (
        body.get("workspace_generation") if isinstance(body, dict) else None
    )
    workspace_runtime_incarnation = (
        body.get("workspace_runtime_incarnation") if isinstance(body, dict) else None
    )
    try:
        agent_id = str(UUID(agent_id))
        runtime_generation = str(UUID(str(runtime_generation)))
        attach_token = str(UUID(str(attach_token)))
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "pinned_runtime_identity_required",
                "message": "Attach release requires its exact runtime identity.",
            },
        ) from exc
    if (
        not isinstance(agent_pod_uid, str)
        or not agent_pod_uid.strip()
        or local_runtime_quiesced is not True
        or local_quiescence_protocol
        not in {
            "workspace_process_zero_v1",
            "agent_runtime_zero_v1",
            "agent_attach_not_started_v1",
        }
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "pinned_attach_quiescence_required",
                "message": (
                    "Attach release requires exact process-zero proof and "
                    "the registered agent Pod identity."
                ),
            },
        )
    try:
        workspace_generation = (
            str(UUID(str(workspace_generation)))
            if workspace_generation not in {None, ""}
            else None
        )
        workspace_runtime_incarnation = (
            str(UUID(str(workspace_runtime_incarnation)))
            if workspace_runtime_incarnation not in {None, ""}
            else None
        )
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "pinned_workspace_identity_invalid"},
        ) from exc
    outcome = await dependencies.release_session_attach_binding(
        agent_id,
        thread_id,
        expected_runtime_generation=runtime_generation,
        expected_attach_token=attach_token,
        expected_agent_pod_uid=agent_pod_uid.strip(),
        local_runtime_quiesced=True,
        local_quiescence_protocol=local_quiescence_protocol,
        workspace_generation=workspace_generation,
        workspace_runtime_incarnation=workspace_runtime_incarnation,
    )
    if outcome == "unsafe" and await dependencies.acknowledge_retiring_failed_attach(
        agent_id,
        thread_id,
        expected_runtime_generation=runtime_generation,
        expected_attach_token=attach_token,
        expected_agent_pod_uid=agent_pod_uid.strip(),
        local_quiescence_protocol=local_quiescence_protocol,
        workspace_generation=workspace_generation,
        workspace_runtime_incarnation=workspace_runtime_incarnation,
    ):
        outcome = "retirement_acknowledged"
    if outcome in {"released", "already_detached"}:
        dependencies.schedule_attach_abort_successor(
            thread_id,
            retired_runtime_generation=runtime_generation,
            retired_attach_token=attach_token,
            retired_agent_id=agent_id,
        )
    return {"status": outcome}
