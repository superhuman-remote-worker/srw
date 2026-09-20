"""Legacy completion workflow with explicit application-owned collaborators.

The operation below intentionally preserves the historical statement and
effect order.  Its size is recorded extraction debt; decomposing it while
moving the authority would make replay and transaction review needlessly
risky.  The dependency ports expose the former application globals without
giving this service a reference to the FastAPI application or to
``orchestrator.main``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from typing import Any
from uuid import UUID

from fastapi import HTTPException, Request

from orchestrator.schemas.job_runtime import JobCompleteRequest
from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
    WorkspaceCleanupOutcome,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from orchestrator.services.vm_workspace_recovery_store import (
    acquire_vm_cleanup_permit,
    completed_cleanup_outcome,
    complete_vm_cleanup_permit,
)


CompletionCallback = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class LegacyPersistenceDependencies:
    store: Any
    vector_store: Any
    forge: Any


@dataclass(frozen=True, slots=True)
class LegacyWorkspaceDependencies:
    container_provisioner: Any
    vm_provisioner: Any
    recovery_store: Any
    cloud_router: Any
    sudo_gate: Any
    get_container_context: CompletionCallback
    get_vm_context: CompletionCallback
    get_infra_transient_context: CompletionCallback
    job_needs_vm: CompletionCallback
    check_vm_permission: CompletionCallback
    capture_freeze_snapshot: CompletionCallback
    unmerged_pr_gate_reason: CompletionCallback


@dataclass(frozen=True, slots=True)
class LegacyVerificationDependencies:
    handle_critic_verdict: CompletionCallback
    materialize_critic_verdict: CompletionCallback
    run_critic_verdict_followups: CompletionCallback
    trigger_verification: CompletionCallback
    materialize_verification_critic: CompletionCallback
    run_verification_critic_handoff: CompletionCallback
    verification_rounds: CompletionCallback


@dataclass(frozen=True, slots=True)
class LegacySubjobDependencies:
    graft_completed_subjob: CompletionCallback
    handle_scholar_completion: CompletionCallback
    handle_delegation_completion: CompletionCallback


@dataclass(frozen=True, slots=True)
class LegacyPostCommitDependencies:
    internal_resume_job: CompletionCallback
    resume_job_without_vm: CompletionCallback
    notify_operator_freeze: CompletionCallback
    trigger_curation_final_pass: CompletionCallback
    advance_project_loop: CompletionCallback
    prepare_project_loop_advance: CompletionCallback
    materialize_project_loop_advance: CompletionCallback
    execute_project_loop_handoff: CompletionCallback
    project_loop_handoff_error_output: CompletionCallback
    maybe_wake_session: CompletionCallback
    trigger_dispatch: CompletionCallback
    kick_session_wake_drain: CompletionCallback


@dataclass(frozen=True, slots=True)
class LegacyCompletionEffectOperations:
    run: CompletionCallback
    run_workspace_teardown: CompletionCallback
    dedup_key: CompletionCallback


@dataclass(frozen=True, slots=True)
class LegacyCompletionDependencies:
    """The explicit ports consumed by the intact completion workflow."""

    persistence: LegacyPersistenceDependencies
    workspace: LegacyWorkspaceDependencies
    verification: LegacyVerificationDependencies
    subjobs: LegacySubjobDependencies
    post_commit: LegacyPostCommitDependencies
    effects: LegacyCompletionEffectOperations
    require_internal: Callable[[Request], Awaitable[Any]]
    require_srw_runtime: CompletionCallback
    completion_commands_enabled: Callable[[], bool]
    logger: logging.Logger


async def complete_job_legacy(
    request: Request,
    job_id: str,
    body: JobCompleteRequest,
    *,
    dependencies: LegacyCompletionDependencies,
    _authorized: bool = False,
    _effect_runner: Any | None = None,
) -> dict[str, Any]:
    """Handle job completion reported by the agent. **Internal** (P4b) —
    requires ``X-Internal-Key``. Ingress strips this path.

    The agent calls this after the graph finishes. The orchestrator handles
    all post-completion logic: status determination, critic verdict handling,
    verification job spawning, curation final pass, and dispatch.

    This replaces the agent-side ``_update_job_status_from_result``,
    ``_handle_critic_verdict``, and ``_maybe_trigger_verification`` functions.
    """
    postgres_db = dependencies.persistence.store
    vector_db = dependencies.persistence.vector_store
    gitea_client = dependencies.persistence.forge
    container_provisioner = dependencies.workspace.container_provisioner
    vm_provisioner = dependencies.workspace.vm_provisioner
    main_cloud_router = dependencies.workspace.cloud_router
    sudo_gate = dependencies.workspace.sudo_gate
    _get_container_context = dependencies.workspace.get_container_context
    _get_vm_context = dependencies.workspace.get_vm_context
    _get_infra_transient_context = dependencies.workspace.get_infra_transient_context
    _job_needs_vm = dependencies.workspace.job_needs_vm
    _check_vm_permission = dependencies.workspace.check_vm_permission
    _capture_workspace_snapshot_for_freeze = (
        dependencies.workspace.capture_freeze_snapshot
    )
    _unmerged_pr_gate_reason = dependencies.workspace.unmerged_pr_gate_reason
    _handle_critic_verdict_on_complete = dependencies.verification.handle_critic_verdict
    _materialize_critic_verdict_transactional = (
        dependencies.verification.materialize_critic_verdict
    )
    _run_critic_verdict_followups = (
        dependencies.verification.run_critic_verdict_followups
    )
    _trigger_verification_on_complete = dependencies.verification.trigger_verification
    _materialize_verification_critic_transactional = (
        dependencies.verification.materialize_verification_critic
    )
    _run_verification_critic_handoff = (
        dependencies.verification.run_verification_critic_handoff
    )
    _verification_rounds = dependencies.verification.verification_rounds
    _maybe_graft_completed_subjob = dependencies.subjobs.graft_completed_subjob
    _handle_scholar_completion = dependencies.subjobs.handle_scholar_completion
    _handle_delegation_child_completion = (
        dependencies.subjobs.handle_delegation_completion
    )
    _internal_resume_job = dependencies.post_commit.internal_resume_job
    _resume_job_without_vm_internal = dependencies.post_commit.resume_job_without_vm
    _notify_operator_freeze = dependencies.post_commit.notify_operator_freeze
    _trigger_curation_final_pass = dependencies.post_commit.trigger_curation_final_pass
    _advance_project_loop = dependencies.post_commit.advance_project_loop
    _prepare_atomic_project_loop_advance = (
        dependencies.post_commit.prepare_project_loop_advance
    )
    _materialize_prepared_project_loop_advance = (
        dependencies.post_commit.materialize_project_loop_advance
    )
    _execute_persisted_project_loop_handoff = (
        dependencies.post_commit.execute_project_loop_handoff
    )
    _project_loop_handoff_error_output = (
        dependencies.post_commit.project_loop_handoff_error_output
    )
    maybe_wake_session = dependencies.post_commit.maybe_wake_session
    _trigger_dispatch = dependencies.post_commit.trigger_dispatch
    _kick_session_wake_drain = dependencies.post_commit.kick_session_wake_drain
    _run_completion_effect = dependencies.effects.run
    _run_completion_workspace_teardown = dependencies.effects.run_workspace_teardown
    _completion_effect_dedup_key = dependencies.effects.dedup_key
    require_internal = dependencies.require_internal
    require_srw_runtime = dependencies.require_srw_runtime
    COMPLETION_COMMANDS_ENABLED = dependencies.completion_commands_enabled()
    logger = dependencies.logger

    if not _authorized:
        await require_internal(request)
    from orchestrator.services.completion import (
        determine_job_status,
        handle_pod_workspace_recovery,
        is_curation_enabled,
        is_late_completion_report,
        is_verification_enabled,
        should_persist_completion_freeze,
        should_reset_recovery_counter,
        unmerged_pr_seal_status,
    )

    try:
        job = await postgres_db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        require_srw_runtime(job)
        completion_entry_status = str(job.get("status") or "")
        if _effect_runner is not None:
            resolved_entry_status = str(
                getattr(_effect_runner, "command", {}).get("resolved_entry_status", "")
                or ""
            ).strip()
            if resolved_entry_status:
                completion_entry_status = resolved_entry_status
        stateless_completion = job.get("execution_lane", "pinned") == "stateless"
        legacy_pinned_completion = _effect_runner is None and not stateless_completion
        completion_result = body.model_dump(
            exclude={"lease_token", "agent_id", "client_report_id"},
        )

        # A retry must not re-run a pure disposition decision against context
        # already advanced by this command (memory/LLM/infra counters are the
        # sharp case at their retry ceilings).  Resolve the parent snapshot and
        # initial status while S1 still sees the accepted command's entry row;
        # the journal stores only fixed-cardinality decision inputs/outputs.
        entry_parent_status: str | None = None
        if _effect_runner is not None and job.get("parent_job_id"):
            entry_parent = await postgres_db.get_job(str(job["parent_job_id"]))
            entry_parent_status = (
                str(entry_parent.get("status")) if entry_parent else None
            )

        entry_context = job.get("context") or {}
        if isinstance(entry_context, str):
            try:
                entry_context = json.loads(entry_context)
            except (json.JSONDecodeError, TypeError):
                entry_context = {}
        if not isinstance(entry_context, Mapping):
            entry_context = {}
        entry_llm_outage = entry_context.get("llm_outage")
        entry_llm_outage = (
            entry_llm_outage if isinstance(entry_llm_outage, Mapping) else {}
        )

        async def _raise_completion_control_race(
            observed_status: str | None = None,
            *,
            legacy_detail: str = (
                "Completion report lost an out-of-band job control race"
            ),
        ) -> None:
            current_status = str(observed_status or "").strip()
            if not current_status:
                current = await postgres_db.get_job(job_id)
                current_status = str((current or {}).get("status") or "unknown")
            if _effect_runner is None:
                raise HTTPException(
                    status_code=409,
                    detail=legacy_detail,
                )
            logger.warning(
                "Completion disposition lost control race "
                "job=%s lease_token=%s entry_status=%s current_status=%s",
                job_id,
                body.lease_token,
                completion_entry_status,
                current_status,
            )
            from orchestrator.services.completion_finalizer import (
                CompletionDispositionSuperseded,
            )

            raise CompletionDispositionSuperseded(
                observed_status=current_status,
                expected_statuses=(completion_entry_status,),
            )

        # Thin S3 entry fence. Rotation never reaches this route; a genuine
        # terminal stateless report must prove the exact live worker lease.
        # Keep the check before the terminal-status early return and every
        # mutation/side effect. Pinned callers remain tokenless.
        if stateless_completion and _effect_runner is None:
            from shared.worker_queue import worker_lease_is_current

            lease_current = False
            if body.lease_token is not None:
                async with postgres_db.acquire() as conn:
                    lease_current = await worker_lease_is_current(
                        conn,
                        job_id=job_id,
                        lease_token=body.lease_token,
                    )
            if not lease_current:
                logger.warning(
                    "Stateless completion fence rejected job=%s lease_token=%s",
                    job_id,
                    body.lease_token,
                )
                raise HTTPException(
                    status_code=409,
                    detail="Completion report does not hold the current worker lease",
                )

        async def _evaluate_late_callback_guard() -> dict[str, Any]:
            if _effect_runner is None:
                # Keep the dark path byte-for-byte equivalent to the legacy S1
                # status guard. Durable-only replay inputs deliberately avoid
                # parsing historical context/config values here: old rows may
                # contain shapes that no legacy completion branch ever read.
                return {
                    "entry_status": completion_entry_status,
                    "matched": completion_entry_status
                    in ("completed", "reviewing", "pending_review"),
                }
            entry_resolution = None
            entry_resolution, _entry_error = determine_job_status(
                job,
                completion_result,
                parent_status=entry_parent_status,
            )
            return {
                "entry_status": completion_entry_status,
                "entry_assigned_agent_id": (
                    str(job["assigned_agent_id"])
                    if job.get("assigned_agent_id") is not None
                    else None
                ),
                "entry_updated_at": (
                    job["updated_at"].isoformat()
                    if isinstance(job.get("updated_at"), datetime)
                    else job.get("updated_at")
                ),
                "matched": completion_entry_status
                in ("completed", "reviewing", "pending_review"),
                "entry_needs_vm": _job_needs_vm(job),
                "entry_parent_status": entry_parent_status,
                "entry_resolution": entry_resolution,
                "entry_infra_transient_attempts": int(
                    (entry_context.get("infra_transient") or {}).get("attempts") or 0
                )
                if isinstance(entry_context.get("infra_transient"), Mapping)
                else 0,
                "entry_memory_retry_count": int(
                    entry_context.get("memory_retry_count") or 0
                ),
                "entry_llm_outage": {
                    "attempt": int(entry_llm_outage.get("attempt") or 0),
                    "first_failed_at": entry_llm_outage.get("first_failed_at"),
                    "last_failed_at": entry_llm_outage.get("last_failed_at"),
                    "next_retry_at": entry_llm_outage.get("next_retry_at"),
                    "fingerprint": (
                        str(entry_llm_outage["fingerprint"])[:512]
                        if entry_llm_outage.get("fingerprint") is not None
                        else None
                    ),
                    "repeat_key": (
                        str(entry_llm_outage["repeat_key"])[:512]
                        if entry_llm_outage.get("repeat_key") is not None
                        else None
                    ),
                    "repeats": int(entry_llm_outage.get("repeats") or 0),
                    "shape_nudge_attempted": bool(
                        entry_llm_outage.get("shape_nudge_attempted")
                    ),
                },
            }

        late_guard = await _run_completion_effect(
            _effect_runner,
            "late_callback_guard",
            "entry",
            _evaluate_late_callback_guard,
        )
        # On finalizer resume the jobs row may already carry S17's disposition.
        # The journaled entry status reconstructs the original branch decision,
        # so S1 cannot turn a resumable command into a false late callback.
        completion_entry_status = str(late_guard["entry_status"])
        if _effect_runner is not None:
            entry_updated_at = late_guard.get("entry_updated_at")
            if isinstance(entry_updated_at, str):
                try:
                    entry_updated_at = datetime.fromisoformat(entry_updated_at)
                except ValueError:
                    entry_updated_at = None
            job["updated_at"] = entry_updated_at
        completion_current_status = str(job.get("status") or "")
        if completion_current_status != completion_entry_status:
            # A durable callback is allowed to observe a jobs-row disposition
            # written by this *same* command only when the corresponding effect
            # marker committed with it.  Matching status alone is not proof: a
            # concurrent cancel/pause or human writer can legitimately reach
            # the same value.  Postgres-only disposition effects use
            # run_transactional(), so their domain write and marker are one
            # commit; external recovery/gate effects may resume only after
            # their completed output names the exact status they produced.
            owned_disposition = False
            if _effect_runner is not None:
                disposition_effects = (
                    "infra_transient_give_up",
                    "infra_transient_pause",
                    "pod_workspace_recovery",
                    "vm_workspace_recovery",
                    "memory_kb_retry_pause",
                    "llm_outage_retry_pause",
                    "deliverable_contract_gate",
                    "main_status_write",
                    "auto_deny_resume",
                )
                for effect_name in disposition_effects:
                    if not await _effect_runner.has_completed(effect_name):
                        continue
                    effect_output = await _effect_runner.completed_detail(effect_name)
                    if not isinstance(effect_output, Mapping):
                        continue
                    effect_status = effect_output.get("new_status")
                    if effect_name in {
                        "memory_kb_retry_pause",
                        "llm_outage_retry_pause",
                    }:
                        effect_status = (
                            "paused" if effect_output.get("paused") else None
                        )
                    elif effect_name == "deliverable_contract_gate":
                        effect_status = (
                            "paused" if effect_output.get("bounced") else effect_status
                        )
                    elif effect_name == "auto_deny_resume":
                        effect_status = (
                            "paused" if effect_output.get("auto_denied") else None
                        )
                    if effect_status == completion_current_status:
                        owned_disposition = True
                        break
                if not owned_disposition and await _effect_runner.has_started(
                    "pod_workspace_recovery"
                ):
                    recovery_context = _get_container_context(job)
                    recovery_outcome = recovery_context.get(
                        "recovery_completion_outcome"
                    )
                    if (
                        recovery_context.get("recovery_completion_command_id")
                        == _effect_runner.command_id
                        and isinstance(recovery_outcome, Mapping)
                        and recovery_outcome.get("new_status")
                        == completion_current_status
                    ):
                        # S7 contains external probe/delete work, so it cannot
                        # run inside the jobs/effect transaction.  Its final
                        # processing disposition instead carries this exact
                        # command key in the same jobs-row UPDATE; that domain
                        # marker is the reconcile proof after a marker crash.
                        owned_disposition = True
            if not owned_disposition:
                await _raise_completion_control_race(
                    completion_current_status,
                    legacy_detail=(
                        "Completion finalization lost an out-of-band job control race"
                    ),
                )
            # Keep the database snapshot intact and use a logical copy for the
            # pre-S17 decision path. Completed callbacks replay stored results;
            # the exact command-owned marker above is the only authority for
            # bypassing S1 after a prior disposition commit.
            job = {
                **job,
                "status": completion_entry_status,
                "assigned_agent_id": late_guard.get("entry_assigned_agent_id"),
            }
            # Class A may already have atomically stashed and cleared an
            # auto-redispatch freeze.  Rehydrate it from the durable context so
            # the resumed S20/S22-S25 tail observes the same payload without
            # putting an unbounded freeze blob in completion_effects.detail.
            if not job.get("freeze_data"):
                replay_context = job.get("context") or {}
                if isinstance(replay_context, str):
                    try:
                        replay_context = json.loads(replay_context)
                    except (json.JSONDecodeError, TypeError):
                        replay_context = {}
                if isinstance(replay_context, Mapping) and isinstance(
                    replay_context.get("last_freeze_data"), Mapping
                ):
                    job["freeze_data"] = dict(replay_context["last_freeze_data"])

        # Post-execution handoff states are monotonic.  The agent that reported
        # one may still be unwinding while this handler archives its workspace
        # or starts verification; that process can race us with a trailing
        # pause/outage callback.  Do not let such a callback overwrite the
        # completion freeze, downgrade the row to ``paused``, or put an
        # already-delivered/review-gated job back in the dispatch queue.
        # Explicit approve/reject/resume endpoints own transitions out of the
        # review states. Failed rows deliberately retain the narrow late
        # completion re-resolution path below.
        #
        # Reproduced on k3d: a loop diff reached Nextcloud and wrote its change
        # record, then the old agent's llm_unavailable callback arrived 15s
        # later and changed ``completed`` -> ``paused``.
        if late_guard["matched"]:
            logger.info(
                "Job %s: ignoring late completion callback while status is %s",
                job_id,
                job["status"],
            )
            late_actions = [f"late callback ignored; job already {job['status']}"]
            if _effect_runner is not None:
                # Ordered reports normally make a trailing crash/error report a
                # terminal no-op. If the lower report deferred S36 after seeing
                # this report's HWM, the no-op must nevertheless run *only* the
                # teardown tail. Continuing through the full legacy body would
                # repeat unrelated Class B/C effects.
                handoff = await _effect_runner.workspace_teardown_handoff()
                if handoff.required:
                    workspace_cleanup = await _run_completion_workspace_teardown(
                        job_id,
                        _effect_runner,
                    )
                    late_actions.extend(workspace_cleanup["actions"])
            return {
                "status": "handled",
                "job_id": job_id,
                "new_status": job["status"],
                "actions": late_actions,
            }

        # Stateless END checkpoints are intentionally re-reported after an
        # ambiguous HTTP failure. Several human/tool paths publish their final
        # status before the report (waiting/waiting_for_reply), and the handler
        # itself may have committed paused/failed/cancelled before its response
        # was lost. The exact queue token above makes these benign callbacks
        # safe; return 2xx so the holder can close the queue instead of
        # release/re-report looping forever. Pinned behavior stays unchanged.
        if stateless_completion and job["status"] in (
            "paused",
            "failed",
            "cancelled",
            "waiting",
            "waiting_for_reply",
        ):
            logger.info(
                "Job %s: accepting exact-token stateless terminal retry while "
                "status is %s",
                job_id,
                job["status"],
            )
            return {
                "status": "handled",
                "job_id": job_id,
                "new_status": job["status"],
                "actions": [f"exact-token terminal retry; job already {job['status']}"],
            }

        result = completion_result
        actions: list[str] = []

        if job["status"] not in (
            "processing",
            "reviewing",
            "pending_review",
            "completed",
        ):
            # Narrow re-resolve: a job that genuinely finished, whose completion
            # freeze arrived after something failed it out-of-band. Without this
            # the report is rejected before anything inspects it, and a finished
            # job stays 'failed' forever — job e1192a9d had to be repaired by
            # hand. See is_late_completion_report for why this is failed-only,
            # completion-freeze-only, and never re-opens a job for re-dispatch.
            if is_late_completion_report(job, result):
                logger.warning(
                    "Job %s: late job_complete freeze accepted on a terminal job "
                    "— re-resolving. It was failed out-of-band while the agent "
                    "was still finishing (prior error: %r).",
                    job_id,
                    job.get("error_message"),
                )

                async def _clear_stale_failure() -> bool:
                    kwargs: dict[str, Any] = {}
                    if _effect_runner is not None:
                        kwargs = {
                            "expected_updated_at": job.get("updated_at"),
                            "completion_command_id": _effect_runner.command_id,
                            "completion_finalizing_by": _effect_runner.owner,
                        }
                    return bool(await postgres_db.clear_job_failure(job_id, **kwargs))

                await _run_completion_effect(
                    _effect_runner,
                    "clear_stale_failure",
                    "entry",
                    _clear_stale_failure,
                    transactional=True,
                )
                job["error_message"] = None
                job["error_details"] = None
                actions.append("late completion freeze re-resolved a terminal job")
            else:
                # Everything else stays rejected — but LOUDLY. This silence is
                # why the gate hid through two incidents: a VALID
                # workspace_unavailable recovery request vanished into a 400
                # that only the agent ever saw, so the recovery arm 47 lines
                # below was never reached.
                err = (
                    result.get("error") if isinstance(result.get("error"), dict) else {}
                )
                logger.warning(
                    "Job %s: DISCARDING completion report on terminal job "
                    "(status=%s, error_type=%s, recoverable=%s, has_freeze=%s). "
                    "A recoverable failure reported here never reaches its "
                    "recovery arm.",
                    job_id,
                    job["status"],
                    err.get("type"),
                    err.get("recoverable"),
                    bool(result.get("freeze_data")),
                )
                raise HTTPException(
                    status_code=400,
                    detail=f"Job cannot be completed (status: {job['status']})",
                )

        # Write freeze_data from the completion report.
        # The orchestrator is the single authority for DB writes — agents
        # report freeze_data in the completion payload, we persist it.
        # EXCEPT on a workspace_unavailable completion: an agent that died
        # before its graph ran echoes the job's PREVIOUS freeze back at us,
        # and persisting that stale blob before the recovery arm's pause left
        # the job paused-but-invisible to the dispatcher
        # (knowledge-base/knowledge/issues/recovery_pause_repersists_stale_freeze_invisible_job.md).
        if result.get("freeze_data"):
            if should_persist_completion_freeze(result):
                job["freeze_data"] = result["freeze_data"]

                async def _persist_reported_freeze() -> dict[str, Any]:
                    try:
                        async with postgres_db.acquire() as conn:
                            await conn.execute(
                                "UPDATE jobs SET freeze_data = $1::jsonb "
                                "WHERE id = $2::uuid",
                                json.dumps(result["freeze_data"]),
                                job_id,
                            )
                    except Exception as exc:
                        logger.warning(
                            f"Failed to write freeze_data for {job_id}: {exc}"
                        )
                        return {"persisted": False, "error": str(exc)}
                    return {"persisted": True}

                await _run_completion_effect(
                    _effect_runner,
                    "persist_reported_freeze",
                    "entry",
                    _persist_reported_freeze,
                    transactional=True,
                )
            else:
                logger.info(
                    "Job %s: skipping freeze_data persist on "
                    "workspace_unavailable completion (echoed stale freeze)",
                    job_id,
                )

        # Clear any remaining queued_replies from job context on completion.
        # The agent may have consumed them during phase transitions.
        if result.get("should_stop") and not stateless_completion:

            async def _drop_queued_replies() -> dict[str, Any]:
                try:
                    async with postgres_db.acquire() as conn:
                        await conn.execute(
                            "UPDATE jobs SET context = context - 'queued_replies' "
                            "WHERE id = $1::uuid AND context ? 'queued_replies'",
                            job_id,
                        )
                except Exception as exc:
                    logger.warning(
                        f"Failed to clear queued_replies for {job_id}: {exc}"
                    )
                    return {"cleared": False, "error": str(exc)}
                return {"cleared": True}

            await _run_completion_effect(
                _effect_runner,
                "drop_queued_replies",
                "entry",
                _drop_queued_replies,
                transactional=True,
            )

        # 0. Workspace-unavailable recovery: the agent's remote workspace went
        #    unreachable mid-run. Recover by BACKEND TYPE — a pod-backed job must
        #    not be routed into the VM arm (that was the wedge in
        #    knowledge-base/knowledge/issues/loop_job_workspace_lost_wedged_in_recovery.md).
        error = result.get("error") or {}

        # 0a. Transient infrastructure failure (a backing service blipped, not a
        #     job fault). Pause with a backoff freeze and KEEP the workspace —
        #     the re-dispatched agent reattaches the surviving VM and resumes
        #     from checkpoint. Checkpoints survive automatically: the prune only
        #     fires on terminal status, and this is 'paused'.
        #
        #     On 2026-07-27 a dropped Postgres connection took this path's place
        #     as a terminal `job_error`, killing three multi-day jobs and
        #     destroying two workspaces.
        #     knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md
        if isinstance(error, dict) and error.get("type") == "infra_transient":
            from orchestrator.services.completion import (
                INFRA_TRANSIENT_MAX_ATTEMPTS,
                infra_transient_backoff_seconds,
            )

            _prev = _get_infra_transient_context(job)
            if "entry_infra_transient_attempts" in late_guard:
                _prev = {
                    **_prev,
                    "attempts": int(late_guard["entry_infra_transient_attempts"]),
                }
            _attempt = int(_prev.get("attempts") or 0) + 1
            _msg = str(error.get("message") or "transient infrastructure failure")

            if _attempt > INFRA_TRANSIENT_MAX_ATTEMPTS:
                # Ceiling. Fail terminally, but NAME the infra cause so this is
                # never mistaken for a job defect in triage.
                _detail = (
                    f"Transient infrastructure failure did not clear after "
                    f"{INFRA_TRANSIENT_MAX_ATTEMPTS} retries: {_msg}"
                )
                logger.error("Job %s: %s", job_id, _detail)

                async def _give_up_infra_transient() -> dict[str, Any]:
                    update_kwargs: dict[str, Any] = {}
                    if legacy_pinned_completion:
                        update_kwargs["expected_status"] = completion_entry_status
                    if _effect_runner is not None:
                        update_kwargs = {
                            "expected_status": completion_entry_status,
                            "completion_command_id": _effect_runner.command_id,
                            "completion_finalizing_by": _effect_runner.owner,
                        }
                    disposition_updated = await postgres_db.update_job_status(
                        job_id,
                        status="failed",
                        error_message=_detail,
                        error_details={
                            "type": "infra_transient",
                            "message": _msg,
                            "recoverable": False,
                            "attempts": _attempt - 1,
                        },
                        **update_kwargs,
                    )
                    if (
                        legacy_pinned_completion or _effect_runner is not None
                    ) and not disposition_updated:
                        await _raise_completion_control_race()
                    return {
                        "status": "handled",
                        "job_id": job_id,
                        "new_status": "failed",
                        "actions": [
                            f"infra_transient: give-up after "
                            f"{INFRA_TRANSIENT_MAX_ATTEMPTS} attempts"
                        ],
                    }

                return await _run_completion_effect(
                    _effect_runner,
                    "infra_transient_give_up",
                    "recovery",
                    _give_up_infra_transient,
                    transactional=True,
                )

            _delay = infra_transient_backoff_seconds(_attempt)
            _next = datetime.now(timezone.utc) + timedelta(seconds=_delay)
            _freeze = {
                "freeze_type": "infra_transient",
                "next_retry_at": _next.isoformat(),
                "attempts": _attempt,
                "last_error": _msg[:500],
            }

            async def _pause_infra_transient() -> dict[str, Any] | None:
                if _effect_runner is not None:
                    if not await postgres_db.pause_job(
                        job_id, completion_commands_enabled=True
                    ):
                        await _raise_completion_control_race()
                try:
                    # Durable attempt counter first — it must survive the sweeper
                    # clearing freeze_data, or the ceiling is unreachable.
                    await postgres_db.merge_job_context(
                        job_id,
                        {
                            "infra_transient": {
                                "attempts": _attempt,
                                "last_error": _msg[:500],
                                "next_retry_at": _next.isoformat(),
                            }
                        },
                    )
                    async with postgres_db.acquire() as conn:
                        await conn.execute(
                            "UPDATE jobs SET freeze_data = $1::jsonb "
                            "WHERE id = $2::uuid",
                            json.dumps(_freeze),
                            job_id,
                        )
                except Exception as exc:
                    if _effect_runner is not None:
                        raise
                    # Without the freeze the sweeper cannot find the job again,
                    # so preserve the legacy fall-through disposition.
                    logger.error(
                        "Job %s: failed to write infra_transient freeze (%s) — "
                        "not pausing, falling through to normal resolution",
                        job_id,
                        exc,
                    )
                    return None
                if _effect_runner is None and not await postgres_db.pause_job(job_id):
                    if legacy_pinned_completion:
                        await _raise_completion_control_race()
                    return None
                logger.warning(
                    "Job %s: paused for transient infrastructure failure "
                    "(attempt %d/%d, retry in %.0fs, workspace KEPT): %s",
                    job_id,
                    _attempt,
                    INFRA_TRANSIENT_MAX_ATTEMPTS,
                    _delay,
                    _msg[:200],
                )
                return {
                    "status": "handled",
                    "job_id": job_id,
                    "new_status": "paused",
                    "actions": [
                        f"infra_transient: paused for retry "
                        f"(attempt {_attempt}/{INFRA_TRANSIENT_MAX_ATTEMPTS}, "
                        f"next retry in {_delay:.0f}s, workspace kept)"
                    ],
                }

            infra_pause_outcome = await _run_completion_effect(
                _effect_runner,
                "infra_transient_pause",
                "recovery",
                _pause_infra_transient,
                transactional=True,
            )
            if infra_pause_outcome is not None:
                return infra_pause_outcome

        if isinstance(error, dict) and error.get("type") == "workspace_unavailable":
            # Decide on the ORIGINAL job (before any stamp): a pod/sandbox job has
            # no vm.requested, so _job_needs_vm is False and it recovers via PVC
            # reattach; only a true VM job takes the legacy VM path below.
            entry_needs_vm = bool(late_guard.get("entry_needs_vm", _job_needs_vm(job)))
            if not entry_needs_vm:
                # --- G1: pod (sandbox/PVC) recovery -------------------------------
                # Extracted to services.completion for testability. Probes the
                # workspace sshd before any delete (a live pod is kept warm),
                # bounds attempts at the cap, and tears the last pod down on
                # fail-loud so it cannot leak.
                # See knowledge-base/knowledge/features/workspace_pvc_branch_a_implementation.md (G1)
                # and knowledge-base/knowledge/issues/maxsessions_parallel_tools_false_workspace_death.md.
                async def _delete_pod(jid: str) -> bool:
                    owner = WorkspaceOwner.job(jid)
                    runtime_incarnation = _get_container_context(job).get(
                        WORKSPACE_RUNTIME_INCARNATION_KEY
                    )
                    try:
                        runtime_incarnation = str(UUID(str(runtime_incarnation)))
                    except (TypeError, ValueError):
                        # Name-only deletion can consume a replacement at the
                        # deterministic Pod name. Legacy recovery now fails
                        # closed on rows that cannot identify runtime A.
                        return False

                    intent = (
                        await container_provisioner.prepare_workspace_cleanup_intent(
                            owner,
                            expected_runtime_incarnation=runtime_incarnation,
                            target_disposition="deleted",
                            reclaim_shared_resources=False,
                        )
                    )
                    if not isinstance(intent, dict):
                        return False
                    cleanup = (
                        await container_provisioner.reconcile_workspace_cleanup_intent(
                            owner,
                            expected_runtime_incarnation=runtime_incarnation,
                            intent_generation=int(intent["intent_generation"]),
                        )
                    )
                    if not isinstance(cleanup, WorkspaceCleanupOutcome):
                        return False
                    if cleanup.superseded:
                        # A reached process-zero and disappeared. A successor B
                        # owns the current context and must not be projected
                        # back to A's recovery state.
                        return True
                    return cleanup.settled

                async def _recover_pod_workspace() -> dict[str, Any]:
                    return await handle_pod_workspace_recovery(
                        job,
                        job_id,
                        error,
                        db=postgres_db,
                        delete_workspace=_delete_pod,
                        trigger_dispatch=_trigger_dispatch,
                        completion_command_id=(
                            _effect_runner.command_id
                            if _effect_runner is not None
                            else None
                        ),
                        completion_finalizing_by=(
                            _effect_runner.owner if _effect_runner is not None else None
                        ),
                        **(
                            {"expected_status": completion_entry_status}
                            if legacy_pinned_completion
                            else {}
                        ),
                    )

                pod_recovery = await _run_completion_effect(
                    _effect_runner,
                    "pod_workspace_recovery",
                    "recovery",
                    _recover_pod_workspace,
                )
                if (
                    legacy_pinned_completion or _effect_runner is not None
                ) and not pod_recovery.get("paused", True):
                    await _raise_completion_control_race()
                return pod_recovery

            # --- VM recovery (legacy path) ------------------------------------
            # VM finalization is explicitly outside this Gate-3 milestone. Keep
            # the historical duplicate guard and best-effort retirement. Publish
            # its pause before external I/O so an already-cancelled job cannot
            # enter recovery. This branch remains unjournaled.
            vm_ctx = _get_vm_context(job)
            if vm_ctx and vm_ctx.get("recovering"):
                logger.info(
                    f"Job {job_id}: VM recovery already in progress, skipping duplicate"
                )
                return {
                    "status": "handled",
                    "job_id": job_id,
                    "new_status": "paused",
                    "actions": ["vm recovery: duplicate skipped"],
                }

            async def _recover_vm_workspace() -> dict[str, Any]:
                logger.warning(
                    f"Job {job_id}: workspace unavailable — attempting VM recovery"
                )
                if COMPLETION_COMMANDS_ENABLED or legacy_pinned_completion:
                    paused = await postgres_db.pause_job(
                        job_id,
                        **(
                            {"completion_commands_enabled": True}
                            if COMPLETION_COMMANDS_ENABLED
                            else {}
                        ),
                    )
                    if not paused:
                        await _raise_completion_control_race()
                # Retire the exact credential-capable runtime before replacing
                # its authority-bearing VM context.  The old flow published a
                # small ``recovering`` object first and thereby erased the UID,
                # SSH host-key fingerprint, and endpoint that the process-zero
                # protocol needs.  The retirement claim itself is the absorbing
                # pre-I/O fence; only a completed captured release may publish
                # the fresh-provision marker.
                vm_deleted = True
                if vm_ctx:
                    try:
                        identity = await vm_provisioner.capture_vm_teardown_identity(
                            job_id,
                            entity_type="job",
                        )
                        cleanup = await acquire_vm_cleanup_permit(
                            dependencies.workspace.recovery_store,
                            owner_kind="job",
                            owner_id=job_id,
                            identity=identity,
                            source="legacy_vm_recovery_release",
                            purge_disk=False,
                        )
                        if not cleanup.allowed:
                            raise RuntimeError(
                                "legacy VM recovery held for workspace recovery"
                            )
                        disposition = completed_cleanup_outcome(cleanup)
                        if disposition is None:
                            release = await vm_provisioner.release_vm_captured(
                                job_id,
                                identity,
                                purge_disk=False,
                                entity_type="job",
                                capture_snapshot=False,
                            )
                            disposition = release.disposition
                            if disposition in {
                                "completed",
                                "identity_superseded",
                            }:
                                await complete_vm_cleanup_permit(
                                    dependencies.workspace.recovery_store,
                                    cleanup,
                                    outcome=disposition,
                                )
                        vm_deleted = disposition == "completed"
                    except Exception:
                        vm_deleted = False
                        logger.exception(
                            "VM recovery for job %s: exact retirement failed",
                            job_id,
                        )
                    if not vm_deleted:
                        logger.error(
                            "VM recovery for job %s: delete was refused; the "
                            "stale cloud-init Secret will fail the recreate",
                            job_id,
                        )
                if vm_deleted:
                    # Replace context.vm only after exact retirement.  The
                    # controller keeps the deterministic rootdisk; the next
                    # dispatch mints a new provision generation.
                    await postgres_db.merge_job_context(
                        job_id,
                        {
                            "vm": {
                                "requested": True,
                                "recovering": True,
                                "previous_error": "workspace_unavailable",
                                "rootdisk": "kept",
                            }
                        },
                    )
                if not COMPLETION_COMMANDS_ENABLED and not legacy_pinned_completion:
                    await postgres_db.pause_job(job_id)
                if vm_deleted:
                    _trigger_dispatch()
                return {
                    "status": "handled",
                    "job_id": job_id,
                    "new_status": "paused",
                    "actions": [
                        (
                            "vm recovery: old VM deleted, new VM will be "
                            "provisioned, job re-queued"
                        )
                        if vm_deleted
                        else (
                            "vm recovery: old VM delete REFUSED — recreate will "
                            "likely fail on the stale cloud-init Secret"
                        )
                    ],
                }

            return await _recover_vm_workspace()

        # Any other handled completion proves the workspace connection works —
        # clear a lingering recovery strike so an old blip cannot make a later,
        # unrelated one exhaust the cap early.
        # knowledge-base/knowledge/issues/maxsessions_parallel_tools_false_workspace_death.md (D).
        if should_reset_recovery_counter(_get_container_context(job), error):

            async def _reset_recovery_strikes() -> dict[str, Any]:
                try:
                    await postgres_db.merge_workspace_container_context(
                        job_id, {"recovery_attempts": 0, "previous_error": None}
                    )
                except Exception as exc:
                    logger.warning(
                        f"Failed to reset workspace recovery counter for {job_id}"
                    )
                    return {"reset": False, "error": str(exc)}
                return {"reset": True}

            await _run_completion_effect(
                _effect_runner,
                "reset_recovery_strikes",
                "recovery",
                _reset_recovery_strikes,
                transactional=True,
            )

        # 1. Determine and set the new job status. For a subjob, pass the parent's
        # current status so a drain-frozen subjob resolves terminally instead of
        # pausing into a cascade-guard wedge under a permanently-failed parent.
        # knowledge-history/done/coincident_infra_error_overrides_reported_job_outcome.md
        _parent_status = late_guard.get("entry_parent_status")
        if _effect_runner is None and job.get("parent_job_id"):
            _parent = await postgres_db.get_job(str(job["parent_job_id"]))
            _parent_status = _parent.get("status") if _parent else None
        decision_job = job
        if _effect_runner is not None:
            decision_context = job.get("context") or {}
            if isinstance(decision_context, str):
                try:
                    decision_context = json.loads(decision_context)
                except (json.JSONDecodeError, TypeError):
                    decision_context = {}
            decision_context = (
                dict(decision_context) if isinstance(decision_context, Mapping) else {}
            )
            if "entry_memory_retry_count" in late_guard:
                decision_context["memory_retry_count"] = int(
                    late_guard["entry_memory_retry_count"]
                )
            if "entry_llm_outage" in late_guard:
                decision_context["llm_outage"] = dict(
                    late_guard.get("entry_llm_outage") or {}
                )
            decision_job = {**job, "context": decision_context}
        new_status, error_message = determine_job_status(
            decision_job, result, parent_status=_parent_status
        )
        if _effect_runner is not None and "entry_resolution" in late_guard:
            entry_resolution = late_guard.get("entry_resolution")
            if entry_resolution != new_status:
                # The only expected divergence is a counter/time decision that
                # this same command advanced before its marker was replayed.
                # Preserve S1's accepted-entry result, including a None result.
                new_status = entry_resolution
                if new_status != "failed":
                    error_message = None

        # 1·mem. Memory/KB-unavailable bounded retry. determine_job_status has
        # already enforced the cap (paused under MEMORY_RETRY_CAP, failed at it).
        # For the pause we must FREE the agent so the dispatcher re-dispatches the
        # SAME job on a fresh pod — pause_job() does that, but only while the row
        # is still 'processing', so it has to run before the generic status write
        # below. The loop-advance hook is correctly skipped because the job never
        # reaches a terminal status here.
        # knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md
        if new_status == "paused":
            _mfd = result.get("freeze_data")
            if isinstance(_mfd, str):
                try:
                    _mfd = json.loads(_mfd)
                except (ValueError, TypeError):
                    _mfd = {}
            if isinstance(_mfd, dict) and _mfd.get("freeze_type") in (
                "memory_unavailable",
                "kb_unavailable",
            ):

                async def _pause_for_memory_retry() -> dict[str, Any]:
                    # Atomic increment (race-proof) — a duplicate re-dispatch of
                    # the same paused job must not stall the counter.
                    if _effect_runner is not None:
                        paused = bool(
                            await postgres_db.pause_job(
                                job_id, completion_commands_enabled=True
                            )
                        )
                        if not paused:
                            await _raise_completion_control_race()
                    retry_count = await postgres_db.increment_job_memory_retry(job_id)
                    if _effect_runner is None:
                        paused = bool(await postgres_db.pause_job(job_id))
                        if legacy_pinned_completion and not paused:
                            await _raise_completion_control_race()
                    if paused and _effect_runner is None:
                        _trigger_dispatch()
                    return {"paused": paused, "retry_count": retry_count}

                memory_retry = await _run_completion_effect(
                    _effect_runner,
                    "memory_kb_retry_pause",
                    "recovery",
                    _pause_for_memory_retry,
                    transactional=True,
                )
                if memory_retry["paused"]:
                    # The durable callback's DB writes and effect marker are
                    # committed before this task is scheduled.  A child task
                    # must not inherit/use the transaction-scoped connection.
                    if _effect_runner is not None:
                        _trigger_dispatch()
                    if completion_current_status not in (
                        completion_entry_status,
                        "paused",
                    ):
                        await _raise_completion_control_race(
                            completion_current_status,
                            legacy_detail=(
                                "Completion finalization lost an out-of-band job "
                                "control race"
                            ),
                        )
                    _mn = int(memory_retry["retry_count"])
                    actions.append(
                        f"memory_unavailable: re-queued for retry "
                        f"(memory_retry_count -> {_mn})"
                    )
                    job["status"] = "paused"
                    new_status = None  # generic write + loop-advance must not re-handle

        # 1·llm. LLM-outage pause + backoff re-dispatch. determine_job_status has
        # already made the pause-vs-fail call (paused under the 24h/attempts
        # ceiling, failed at it). On a PAUSE: atomically advance the attempt
        # counter, compute the Full-Jittered next_retry_at, persist it into
        # freeze_data, and free the agent via pause_job — but do NOT
        # _trigger_dispatch(): the outage sweeper owns re-dispatch when the timer
        # is due (freeze_data IS NULL would block the dispatcher anyway). On a
        # terminal FAIL (ceiling tripped): alert the operator (dead-letter +
        # alert, not silent give-up); the generic write below sets status=failed
        # and the loop-advance hook counts it once.
        # knowledge-base/knowledge/features/llm_outage_pause_and_backoff_redispatch.md
        _lfd = result.get("freeze_data")
        if isinstance(_lfd, str):
            try:
                _lfd = json.loads(_lfd)
            except (ValueError, TypeError):
                _lfd = {}
        if isinstance(_lfd, dict) and _lfd.get("freeze_type") == "llm_unavailable":
            if new_status == "paused":
                from orchestrator.services.completion import (
                    LLM_OUTAGE_REPEAT_CEILING,
                    LLM_OUTAGE_RESET_WINDOW_SECONDS,
                    LLM_OUTAGE_SHAPE_NUDGE,
                    llm_outage_backoff_seconds,
                    llm_outage_fingerprint,
                    llm_outage_repeat_key,
                )

                async def _pause_for_llm_outage() -> dict[str, Any]:
                    if _effect_runner is not None:
                        paused = bool(
                            await postgres_db.pause_job(
                                job_id, completion_commands_enabled=True
                            )
                        )
                        if not paused:
                            await _raise_completion_control_race()
                    now = datetime.now(timezone.utc)
                    advanced = await postgres_db.increment_job_llm_outage_attempt(
                        job_id,
                        now=now,
                        reset_window_seconds=LLM_OUTAGE_RESET_WINDOW_SECONDS,
                        fingerprint=llm_outage_fingerprint(_lfd),
                        repeat_key=llm_outage_repeat_key(_lfd),
                        nudge_at_repeats=(
                            LLM_OUTAGE_REPEAT_CEILING
                            if LLM_OUTAGE_SHAPE_NUDGE
                            else None
                        ),
                    )
                    attempt = int(advanced["attempt"])
                    retry_after = _lfd.get("retry_after_seconds")
                    try:
                        retry_after = (
                            float(retry_after) if retry_after is not None else None
                        )
                    except (ValueError, TypeError):
                        retry_after = None
                    delay = llm_outage_backoff_seconds(
                        attempt, retry_after_seconds=retry_after
                    )
                    next_retry = now + timedelta(seconds=delay)
                    out_freeze = dict(_lfd)
                    out_freeze["next_retry_at"] = next_retry.isoformat()
                    out_freeze["attempt"] = attempt
                    try:
                        async with postgres_db.acquire() as conn:
                            await conn.execute(
                                """
                                UPDATE jobs
                                   SET freeze_data = $1::jsonb,
                                       context = jsonb_set(
                                           COALESCE(context, '{}'::jsonb),
                                           '{llm_outage,next_retry_at}',
                                           to_jsonb($3::text),
                                           true
                                       )
                                 WHERE id = $2::uuid
                                """,
                                json.dumps(out_freeze),
                                job_id,
                                next_retry.isoformat(),
                            )
                    except Exception as exc:
                        if _effect_runner is not None:
                            raise
                        logger.warning(
                            "Failed to write llm_outage next_retry_at for %s: %s",
                            job_id,
                            exc,
                        )
                    if _effect_runner is None:
                        paused = bool(await postgres_db.pause_job(job_id))
                        if legacy_pinned_completion and not paused:
                            await _raise_completion_control_race()
                    return {
                        "paused": paused,
                        "attempt": attempt,
                        "delay": delay,
                        "next_retry_at": next_retry.isoformat(),
                    }

                llm_retry = await _run_completion_effect(
                    _effect_runner,
                    "llm_outage_retry_pause",
                    "recovery",
                    _pause_for_llm_outage,
                    transactional=True,
                )
                if llm_retry["paused"]:
                    if completion_current_status not in (
                        completion_entry_status,
                        "paused",
                    ):
                        await _raise_completion_control_race(
                            completion_current_status,
                            legacy_detail=(
                                "Completion finalization lost an out-of-band job "
                                "control race"
                            ),
                        )
                    _attempt = int(llm_retry["attempt"])
                    _delay = float(llm_retry["delay"])
                    actions.append(
                        f"llm_unavailable: paused for backoff re-dispatch "
                        f"(attempt {_attempt}, next retry in {_delay:.0f}s)"
                    )
                    logger.warning(
                        f"Job {job_id} paused for LLM outage — attempt {_attempt}, "
                        f"next_retry_at={llm_retry['next_retry_at']} "
                        f"(classification={_lfd.get('classification')}, "
                        f"model={_lfd.get('model')})"
                    )
                    job["status"] = "paused"
                    # No _trigger_dispatch() — the outage sweeper re-dispatches.
                    new_status = None  # generic write + loop-advance must not re-handle
            elif new_status == "failed":
                logger.error(
                    f"Job {job_id} FAILED after LLM-outage give-up ceiling: "
                    f"{error_message}"
                )

                async def _alert_llm_give_up() -> dict[str, Any]:
                    try:
                        await _notify_operator_freeze(
                            job,
                            job_id,
                            "llm_unavailable",
                            _lfd,
                            dedup_key=_completion_effect_dedup_key(
                                _effect_runner, "llm_give_up_operator_alert", job_id
                            ),
                        )
                    except Exception as exc:
                        logger.warning(
                            "Failed to send llm_unavailable give-up alert for %s: %s",
                            job_id,
                            exc,
                        )
                        return {"sent": False, "error": str(exc)}
                    return {"sent": True}

                llm_alert = await _run_completion_effect(
                    _effect_runner,
                    "llm_give_up_operator_alert",
                    "llm_give_up_alert",
                    _alert_llm_give_up,
                    retry_if=lambda output: not bool(output.get("sent")),
                )
                if llm_alert["sent"]:
                    actions.append("operator alerted (llm_unavailable give-up)")

        # 1·gate. Deliverable-contract gate (P1-C): a completion that CLAIMS
        # done-ness must have every context.required_deliverables artifact
        # present at the job branch HEAD (Gitea) before it may seal — or spawn
        # critic/curator work. Missing → bounce back through the P1-A
        # resume-with-feedback lane with the precise missing/present listing
        # (bounded by the gate's cap). At the cap an explicit publication
        # promise becomes terminal blocked/undelivered; ordinary in-repo
        # manifests retain their historical review behavior. Forge failure
        # fails closed for PR contracts and remains fail-open only for ordinary
        # in-repo evidence. Logic in services/deliverable_gate.py.
        # knowledge-base/knowledge/issues/officer_blind_reads_and_worker_bureaucracy.md §4 P1-C.
        from orchestrator.services.completion import apply_deliverable_gate

        legacy_resume_control_lost = False

        async def _queue_deliverable_gate_resume(
            resume_job_id: str,
            feedback: str,
            reason: str | None = None,
        ) -> None:
            nonlocal legacy_resume_control_lost
            resumed = await _internal_resume_job(
                resume_job_id,
                feedback,
                reason,
                expected_status=completion_entry_status,
                completion_owner_command_id=(
                    str(_effect_runner.command_id)
                    if _effect_runner is not None
                    else None
                ),
                completion_owner=(
                    str(_effect_runner.owner) if _effect_runner is not None else None
                ),
            )
            if legacy_pinned_completion and not resumed:
                legacy_resume_control_lost = True
                await _raise_completion_control_race()

        async def _apply_completion_deliverable_gate() -> dict[str, Any]:
            gate_decision = await apply_deliverable_gate(
                job,
                result,
                new_status,
                db=postgres_db,
                gitea=gitea_client,
                queue_resume=_queue_deliverable_gate_resume,
                vector_db=vector_db,
            )
            # The gate catches queue failures to retain its historical fallback
            # policy. A cancelled legacy job must stop outside that catch before
            # evidence, delivery, or any further completion disposition.
            if legacy_resume_control_lost:
                await _raise_completion_control_race()
            status, gate_actions, bounced = gate_decision
            return {
                "new_status": status,
                "actions": list(gate_actions),
                "bounced": bool(bounced),
                # Tests and a rolling in-process collaborator may still
                # return the historical three-tuple. Absence means the old
                # ordinary outcome, never an inferred blocked result.
                "outcome_kind": getattr(gate_decision, "outcome_kind", None),
            }

        gate_result = await _run_completion_effect(
            _effect_runner,
            "deliverable_contract_gate",
            "delivery_gate",
            _apply_completion_deliverable_gate,
        )
        new_status = gate_result["new_status"]
        _gate_actions = list(gate_result["actions"])
        _gate_bounced = bool(gate_result["bounced"])
        completion_outcome_kind = gate_result.get("outcome_kind")
        actions.extend(_gate_actions)
        if _gate_bounced:
            # Refused seal: the job is already parked paused with
            # queued_feedback (+ queued_feedback_reason) and the dispatcher
            # triggered. Skip the status write, notifications, subjob graft,
            # critic/curator spawns and loop advance — none may act on a
            # bounced seal.
            return {
                "status": "handled",
                "job_id": job_id,
                "new_status": "paused",
                "actions": actions,
            }

        if completion_outcome_kind == "blocked_undelivered":
            error_message = (
                "Delivery contract could not be satisfied; work ended "
                "blocked/undelivered without a verified pull request."
            )

        # 1·evidence (E4, officer_supervision_surface §3.3): a completion
        # CLAIM that survived the gate gets its typed evidence manifest —
        # server-created completion-report + deliverable-check entries plus
        # resolved worker-declared entries, pinned to the completion
        # revision — recorded in jobs.context.evidence_manifest. Best-effort:
        # a manifest failure must never block a seal.
        if new_status in ("completed", "pending_review", "reviewing"):

            async def _record_evidence_manifest() -> dict[str, Any]:
                from orchestrator.services.job_evidence import build_evidence_manifest

                try:
                    evidence_job = await postgres_db.get_job(job_id) or job
                    manifest = await build_evidence_manifest(
                        evidence_job,
                        result,
                        db=postgres_db,
                        gitea=gitea_client,
                    )
                    await postgres_db.merge_job_context(
                        job_id, {"evidence_manifest": manifest}
                    )
                    return {
                        "recorded": True,
                        "entry_count": len(manifest.get("entries") or []),
                    }
                except Exception:  # noqa: BLE001 — never block the seal
                    logger.warning(
                        "Evidence manifest recording failed safely for job %s",
                        job_id,
                    )
                    return {
                        "recorded": False,
                        "error": "evidence_manifest_record_failed",
                    }

            evidence_effect = await _run_completion_effect(
                _effect_runner,
                "evidence_manifest_record",
                "delivery_gate",
                _record_evidence_manifest,
            )
            if evidence_effect.get("recorded"):
                actions.append(
                    f"evidence manifest recorded "
                    f"({evidence_effect.get('entry_count', 0)} entr(y/ies))"
                )

        # A reordered command owns one durable jobs-row control marker before
        # any product delivery starts.  Cancel/pause/resume/admission paths
        # already honor this reserved marker, so the check survives the gap
        # between a jobs-row preflight and external WebDAV/Gitea I/O.  A retry
        # adopts the command-id marker; a stale process still loses on its
        # ephemeral command term.  Once S17 is already journaled, skip this
        # pre-status phase entirely and resume only its durable tail.
        from orchestrator.services.completion_effect_policy import (
            completion_status_order,
        )

        pre_s15_status_order = completion_status_order(
            getattr(_effect_runner, "command", None),
            new_status,
        )
        main_status_already_completed = False
        delivery_control_claim_id: str | None = None
        if pre_s15_status_order.reordered:
            main_status_already_completed = await _effect_runner.has_completed(
                "main_status_write"
            )
            if not main_status_already_completed:
                delivery_control_claim_id = (
                    await _effect_runner.acquire_delivery_control(
                        completion_entry_status
                    )
                )

        # 1a. Project-cloud delivery. Every project job receives a cloud
        # baseline in its isolated repo. Ordinary jobs retain the human
        # accept/reject workflow. Loop jobs auto-apply only a completely
        # readable, conflict-free diff; any conflict, backend failure, or
        # partial write parks the member at pending_review, so its loop barrier
        # cannot rotate past an unresolved durable-file state.
        from orchestrator.services.project_loops import job_loop_id

        _completion_loop_id = job_loop_id(job)
        if _completion_loop_id and new_status == "completed":

            async def _deliver_loop_project_cloud() -> dict[str, Any]:
                if delivery_control_claim_id is not None:
                    await _effect_runner.assert_delivery_control(
                        completion_entry_status
                    )
                try:
                    from orchestrator.services.job_cloud_baseline import (
                        deliver_loop_diff_to_cloud,
                    )

                    delivery_project = (
                        await postgres_db.get_project(str(job["project_id"]))
                        if job.get("project_id")
                        else None
                    )
                    if not delivery_project:
                        loop_delivery = {
                            "delivery_status": "cloud-unavailable",
                            "needs_review": True,
                            "delivery_sha": None,
                            "notes": ["project row is unavailable"],
                        }
                    else:
                        loop_delivery = await deliver_loop_diff_to_cloud(
                            job=job,
                            project=delivery_project,
                            postgres_db=postgres_db,
                            gitea_client=gitea_client,
                            main_cloud_router=main_cloud_router,
                            completion_command_id=getattr(
                                _effect_runner, "command_id", None
                            ),
                        )
                    delivery_status = str(loop_delivery["delivery_status"])
                    await postgres_db.update_job_merge_status(
                        job_id, merge_status=delivery_status
                    )
                    await postgres_db.merge_job_context(
                        job_id, {"loop_cloud_delivery": loop_delivery}
                    )
                    status = (
                        "pending_review"
                        if loop_delivery.get("needs_review")
                        else "completed"
                    )
                    action = (
                        f"loop cloud delivery {delivery_status} -> pending_review"
                        if status == "pending_review"
                        else f"loop cloud delivery -> {delivery_status}"
                    )
                    result = {
                        "new_status": status,
                        "delivery_status": delivery_status,
                        "action": action,
                    }
                    if _effect_runner is None:
                        result["legacy_loop_delivery"] = loop_delivery
                    return result
                except Exception as exc:
                    # Fail closed for loops. Advancing here would strand the only
                    # durable copy of this turn's project-file contribution.
                    logger.exception(
                        "Loop cloud delivery failed for job %s; parking for review",
                        job_id,
                    )
                    try:
                        await postgres_db.update_job_merge_status(
                            job_id, merge_status="cloud-unavailable"
                        )
                        await postgres_db.merge_job_context(
                            job_id,
                            {
                                "loop_cloud_delivery": {
                                    "delivery_status": "cloud-unavailable",
                                    "needs_review": True,
                                    "notes": [str(exc)],
                                }
                            },
                        )
                    except Exception:
                        logger.warning(
                            "Failed to persist loop cloud delivery failure for %s",
                            job_id,
                            exc_info=True,
                        )
                    return {
                        "new_status": "pending_review",
                        "delivery_status": "cloud-unavailable",
                        "action": "loop cloud delivery failed -> pending_review",
                    }

            loop_delivery_result = await _run_completion_effect(
                _effect_runner,
                "loop_project_cloud_delivery",
                "delivery",
                _deliver_loop_project_cloud,
            )
            new_status = str(loop_delivery_result["new_status"])
            job["merge_status"] = loop_delivery_result["delivery_status"]
            # S15's potentially unbounded per-file inventory already lives in
            # context.loop_cloud_delivery. Never copy it into the 8 KiB effect
            # detail row. Durable replay reloads the domain record; the dark
            # path preserves the historical in-memory merge and extra-read
            # count exactly.
            if _effect_runner is not None:
                refreshed_loop_job = await postgres_db.get_job(job_id)
                if refreshed_loop_job is not None:
                    job["context"] = refreshed_loop_job.get("context") or job.get(
                        "context"
                    )
            else:
                legacy_loop_delivery = loop_delivery_result.get("legacy_loop_delivery")
                if legacy_loop_delivery is not None:
                    loop_context = job.get("context") or {}
                    if isinstance(loop_context, str):
                        try:
                            loop_context = json.loads(loop_context)
                        except (json.JSONDecodeError, TypeError):
                            loop_context = {}
                    if not isinstance(loop_context, dict):
                        loop_context = {}
                    loop_context["loop_cloud_delivery"] = legacy_loop_delivery
                    job["context"] = loop_context
            actions.append(str(loop_delivery_result["action"]))
        elif (
            job.get("cloud_diff_baseline_commit")
            and new_status in ("completed", "pending_review")
            and gitea_client.is_initialized
            and not _completion_loop_id
        ):

            async def _capture_mode_a_diff() -> dict[str, Any]:
                try:
                    from orchestrator.services.job_cloud_baseline import (
                        capture_diff_for_mode_a_job,
                    )

                    captured = await capture_diff_for_mode_a_job(
                        job=job,
                        postgres_db=postgres_db,
                        gitea_client=gitea_client,
                    )
                except Exception as exc:
                    logger.warning(
                        "Mode A: diff capture failed for job %s (%s); "
                        "proceeding with original status",
                        job_id,
                        exc,
                    )
                    return {"captured": False, "error": str(exc)}
                return {"captured": bool(captured)}

            mode_a_capture = await _run_completion_effect(
                _effect_runner,
                "mode_a_diff_capture",
                "delivery",
                _capture_mode_a_diff,
            )
            if mode_a_capture["captured"] and new_status == "completed":
                new_status = "pending_review"
                actions.append("mode A diff captured -> pending_review")

        # A job whose deliverable is a pull request is not done while that PR is
        # open. Routed to human review rather than refused, because refusing a
        # self-sealing job would strand it. Sibling of the mode A downgrade
        # above; both are deliberate exceptions to `full` autonomy being
        # terminal (knowledge-base/knowledge/issues/full_autonomy_is_not_actually_terminal.md).
        if new_status == "completed":
            new_status, unmerged_pr_action = unmerged_pr_seal_status(
                new_status,
                loop_id=_completion_loop_id,
                reason=await _unmerged_pr_gate_reason({**job, "id": job_id}, user=None),
            )
            if unmerged_pr_action:
                actions.append(unmerged_pr_action)

        # Step 4 is selected only by the immutable value captured on this
        # command at admission. A process-global flag must never reinterpret a
        # stranded command after a rollback/redeploy, and non-terminal paths
        # retain their historical order even when that captured bit is true.
        status_order = completion_status_order(
            getattr(_effect_runner, "command", None),
            new_status,
        )
        pre_status_critic_verdict: dict[str, Any] | None = None
        pre_status_subjob_actions: list[str] = []
        pre_status_terminal_actions: list[str] = []

        async def _run_subjob_output_graft_effect() -> list[str]:
            if not job.get("parent_job_id"):
                return []

            async def _graft_subjob_output() -> dict[str, Any]:
                if delivery_control_claim_id is not None:
                    await _effect_runner.assert_delivery_control(
                        completion_entry_status
                    )
                graft = await _maybe_graft_completed_subjob(
                    job,
                    completion_command_id=getattr(_effect_runner, "command_id", None),
                )
                return {"graft_result": graft}

            graft_effect = await _run_completion_effect(
                _effect_runner,
                "subjob_output_graft",
                "subjob_graft",
                _graft_subjob_output,
                retry_on_error=True,
                error_output=lambda exc: {
                    "graft_result": {
                        "status": "error",
                        "reason": str(exc),
                    }
                },
            )
            graft_result = graft_effect["graft_result"]
            if graft_result and graft_result.get("status") == "grafted":
                return [f"subjob output grafted to {graft_result['output_path']}"]
            return []

        async def _run_terminal_delivery_effect() -> list[str]:
            # S33 preserves its historical applicability. ``cancelled`` is in
            # the reordered terminal set but has no merge/change-record work.
            if new_status not in ("completed", "failed") and not (
                new_status == "cancelled"
                and completion_outcome_kind == "blocked_undelivered"
            ):
                return []

            async def _apply_terminal_merge_and_record() -> dict[str, Any]:
                if delivery_control_claim_id is not None:
                    await _effect_runner.assert_delivery_control(
                        completion_entry_status
                    )
                try:
                    from orchestrator.services.completion import (
                        apply_terminal_job_side_effects,
                    )

                    durable_merge_kwargs: dict[str, Any] = {}
                    if _effect_runner is not None:
                        durable_merge_kwargs = {
                            "completion_command_id": _effect_runner.command_id,
                            "load_merge_intent": lambda: _effect_runner.capture_intent(
                                "terminal_merge_change_record"
                            ),
                            "store_merge_intent": lambda detail: (
                                _effect_runner.capture_intent(
                                    "terminal_merge_change_record", detail
                                )
                            ),
                        }
                    side_effects = await apply_terminal_job_side_effects(
                        job,
                        new_status,
                        gitea=gitea_client,
                        db=postgres_db,
                        vector_db=vector_db,
                        error=error_message,
                        outcome_kind=completion_outcome_kind,
                        **durable_merge_kwargs,
                    )
                except Exception as exc:
                    logger.warning(
                        f"Job {job_id}: terminal side effects failed (non-fatal)",
                        exc_info=True,
                    )
                    return {"actions": [], "error": str(exc)}
                return {"actions": list(side_effects["actions"])}

            terminal_effects = await _run_completion_effect(
                _effect_runner,
                "terminal_merge_change_record",
                "terminal_delivery",
                _apply_terminal_merge_and_record,
                retry_if=lambda output: bool(output.get("error")),
            )
            return list(terminal_effects["actions"])

        if status_order.reordered and not main_status_already_completed:
            # S27's DB-only core is Class B. It must observe the disposition
            # this command is about to publish, not the still-processing jobs
            # row. Its external follow-up remains in the post-status tail.
            if "critic_verdict" in status_order.pre_status_class_b_effects:
                logical_terminal_job = {**job, "status": new_status}

                async def _materialize_pre_status_critic_verdict() -> dict[str, Any]:
                    return await _materialize_critic_verdict_transactional(
                        logical_terminal_job
                    )

                pre_status_critic_verdict = await _run_completion_effect(
                    _effect_runner,
                    "critic_verdict",
                    "critic_verdict",
                    _materialize_pre_status_critic_verdict,
                    transactional=True,
                    supersede_if=lambda output: (
                        output.get("applicable") is True
                        and output.get("world_cas_won") is False
                    ),
                )

            if "subjob_output_graft" in status_order.pre_status_delivery_effects:
                pre_status_subjob_actions = await _run_subjob_output_graft_effect()
            if (
                "terminal_merge_change_record"
                in status_order.pre_status_delivery_effects
            ):
                pre_status_terminal_actions = await _run_terminal_delivery_effect()

            # Run every independent delivery first, then withhold S17 if any
            # group scheduled a retry. CompletionFinalizer owns release/park;
            # returning here is what prevents a pending delivery from becoming
            # user-visible terminal state.
            for gated_group in status_order.gated_groups:
                if await _effect_runner.has_pending_group(gated_group):
                    return {
                        "status": "handled",
                        "job_id": job_id,
                        "new_status": job["status"],
                        "actions": actions,
                    }

        if new_status:
            kwargs: dict[str, Any] = {"status": new_status}
            if completion_outcome_kind is not None:
                kwargs["completion_outcome_kind"] = completion_outcome_kind
            had_assigned_agent = False
            fd_row: dict[str, Any] | None = None
            stash_and_clear_freeze = False

            if error_message:
                kwargs["error_message"] = error_message
            # Persist the agent's structured error alongside the failure so the
            # loop-advance heal path (which re-runs with result={}) can read
            # classification/reset_at back off the row. Rides the SAME UPDATE
            # as status='failed', so a sibling barrier winner never sees one
            # without the other. knowledge-base/knowledge/issues/loop_advances_into_active_model_cooldown.md
            if new_status == "failed" and isinstance(result.get("error"), dict):
                kwargs["error_details"] = result["error"]

            # Class A: every field that defines this jobs-row disposition rides
            # one UPDATE. ``None`` means "omit" to update_job_status, so the
            # established empty-string sentinel is required to clear the agent.
            if new_status == "paused":
                had_assigned_agent = bool(job.get("assigned_agent_id"))
                kwargs["assigned_agent_id"] = ""

                raw_fd = job.get("freeze_data")
                if isinstance(raw_fd, str):
                    try:
                        raw_fd = json.loads(raw_fd)
                    except (json.JSONDecodeError, ValueError):
                        raw_fd = None
                if isinstance(raw_fd, dict):
                    fd_row = raw_fd
                    from shared.job_freeze_types import (
                        AUTO_REDISPATCH_FREEZE_TYPES,
                    )

                    stash_and_clear_freeze = (
                        fd_row.get("freeze_type") in AUTO_REDISPATCH_FREEZE_TYPES
                    )
                    if stash_and_clear_freeze:
                        kwargs["stash_and_clear_freeze"] = True
                        kwargs["freeze_data"] = fd_row

            # A pinned cancel now publishes terminal authority before external
            # cleanup in either mode. An already-running legacy callback must
            # lose the same status CAS as a stateless/durable completion, or it
            # could resurrect the job while cancellation retires its workspace.
            kwargs["expected_status"] = completion_entry_status
            if _effect_runner is not None:
                kwargs["completion_command_id"] = _effect_runner.command_id
                kwargs["completion_finalizing_by"] = _effect_runner.owner
                if new_status in {"completed", "failed", "cancelled"}:
                    from orchestrator.services.job_completion_commands import (
                        accepted_completion_decision_tool_call_id,
                    )

                    accepted_decision_id = accepted_completion_decision_tool_call_id(
                        _effect_runner.command.get("payload")
                    )
                    if accepted_decision_id is not None:
                        kwargs["consume_completion_decision_tool_call_id"] = (
                            accepted_decision_id
                        )
                if delivery_control_claim_id is not None:
                    kwargs["completion_control_claim_id"] = delivery_control_claim_id

            async def _write_main_status() -> dict[str, Any]:
                disposition_updated = await postgres_db.update_job_status(
                    job_id, **kwargs
                )
                if not disposition_updated:
                    current = await postgres_db.get_job(job_id)
                    current_status = str((current or {}).get("status") or "unknown")
                    logger.warning(
                        "Completion disposition lost control race "
                        "job=%s lease_token=%s entry_status=%s current_status=%s",
                        job_id,
                        body.lease_token,
                        completion_entry_status,
                        current_status,
                    )
                    await _raise_completion_control_race(current_status)
                return {
                    "new_status": new_status,
                    "had_assigned_agent": had_assigned_agent,
                    "stash_and_clear_freeze": stash_and_clear_freeze,
                }

            status_effect = await _run_completion_effect(
                _effect_runner,
                "main_status_write",
                "job_disposition",
                _write_main_status,
                transactional=True,
            )
            new_status = status_effect["new_status"]
            had_assigned_agent = bool(status_effect["had_assigned_agent"])
            stash_and_clear_freeze = bool(status_effect["stash_and_clear_freeze"])
            # The freeze payload can contain an unbounded command/result blob.
            # It belongs on jobs.freeze_data/context.last_freeze_data, never in
            # completion_effects.detail. Rehydrate it from that domain record
            # after replaying S17's fixed-cardinality decision summary.
            replay_fd = job.get("freeze_data")
            if isinstance(replay_fd, str):
                try:
                    replay_fd = json.loads(replay_fd)
                except (json.JSONDecodeError, TypeError):
                    replay_fd = None
            fd_row = dict(replay_fd) if isinstance(replay_fd, Mapping) else None
            actions.append(f"status -> {new_status}")
            logger.info(f"Job {job_id} status set to '{new_status}'")

            # 'paused' means unassigned + dispatchable (pause_job semantics), but
            # the freeze→paused paths (version_upgrade drain, vm/workspace
            # upgrade, memory_unavailable) land here with the dying agent still
            # attached — and the dispatcher only picks up paused jobs with
            # assigned_agent_id IS NULL, so without this clear they wedge until
            # gc_offline_agents' 24h FK cascade frees them.
            if new_status == "paused":
                job["assigned_agent_id"] = None
                if had_assigned_agent:
                    actions.append("cleared agent on paused job (re-dispatchable)")

            async def _record_assigned_agent_clear() -> dict[str, Any]:
                return {
                    "applied_in_main_status_write": new_status == "paused",
                    "had_assigned_agent": had_assigned_agent,
                }

            await _run_completion_effect(
                _effect_runner,
                "clear_assigned_agent_on_pause",
                "job_disposition",
                _record_assigned_agent_clear,
            )

            # Auto-redispatch pauses must ALSO shed the row-level freeze blob:
            # get_dispatchable_jobs requires ``freeze_data IS NULL`` (partial
            # index, 0046), so a kept freeze makes the paused job invisible to
            # the dispatcher forever. Stash it in context for observability —
            # resume state itself lives in the checkpoint + pushed branch, not
            # here. Pauses awaiting explicit action (vm_upgrade_required,
            # user-feedback freezes) keep their freeze_data untouched.
            if new_status == "paused" and stash_and_clear_freeze:
                job["freeze_data"] = None
                actions.append("freeze stashed to context (auto-redispatch)")

                async def _record_freeze_stash() -> dict[str, Any]:
                    return {"applied_in_main_status_write": True}

                await _run_completion_effect(
                    _effect_runner,
                    "stash_and_clear_freeze",
                    "job_disposition",
                    _record_freeze_stash,
                )

                # Progress-aware drain backstop (defense-in-depth for the
                # version_upgrade drain livelock,
                # knowledge-base/knowledge/issues/version_upgrade_drain_livelock.md). Detects a
                # re-dispatch loop that is NOT advancing (freeze phase_number
                # stuck) and alerts, rather than letting it churn invisibly.
                # Pure decision in services.completion; I/O stays here.
                async def _update_drain_stall_counter() -> dict[str, Any]:
                    try:
                        from orchestrator.services.completion import (
                            auto_continue_drain_update,
                        )

                        ctx = job.get("context") or {}
                        if isinstance(ctx, str):
                            ctx = json.loads(ctx)
                        cap = int(os.environ.get("AUTO_CONTINUE_DRAIN_ALERT_CAP", "10"))
                        drains, last_phase, should_alert = auto_continue_drain_update(
                            ctx or {}, fd_row, cap=cap
                        )
                        merged = await postgres_db.merge_job_context(
                            job_id,
                            {
                                "auto_continue_drains": drains,
                                "auto_continue_last_phase": last_phase,
                            },
                        )
                        if _effect_runner is not None and not merged:
                            raise RuntimeError(
                                "drain-stall counter update did not commit"
                            )
                        return {
                            "drains": drains,
                            "last_phase": last_phase,
                            "alerted": should_alert,
                        }
                    except Exception as exc:
                        if _effect_runner is not None:
                            raise
                        logger.warning(
                            "Failed to update auto-continue drain counter for %s: %s",
                            job_id,
                            exc,
                        )
                        return {"error": str(exc)}

                drain_stall = await _run_completion_effect(
                    _effect_runner,
                    "drain_stall_counter_alert",
                    "drain_stall_alert",
                    _update_drain_stall_counter,
                    transactional=True,
                )
                if drain_stall.get("alerted"):
                    logger.error(
                        f"Job {job_id}: {drain_stall['drains']} consecutive "
                        f"{fd_row.get('freeze_type')} re-dispatches with NO "
                        f"phase progress (stuck at {drain_stall['last_phase']}) — "
                        f"the agent-side resume-clear may be failing; alerting "
                        f"operator."
                    )

                    async def _send_drain_stall_alert() -> dict[str, Any]:
                        try:
                            await _notify_operator_freeze(
                                job,
                                job_id,
                                fd_row.get("freeze_type"),
                                fd_row,
                                dedup_key=_completion_effect_dedup_key(
                                    _effect_runner, "drain_stall_operator_alert", job_id
                                ),
                            )
                        except Exception as exc:
                            logger.warning(
                                "Failed to alert on drain-stall for %s: %s",
                                job_id,
                                exc,
                            )
                            return {"sent": False, "error": str(exc)}
                        return {"sent": True}

                    await _run_completion_effect(
                        _effect_runner,
                        "drain_stall_operator_alert",
                        "drain_stall_notification",
                        _send_drain_stall_alert,
                        retry_if=lambda output: bool(output.get("error")),
                    )

            async def _record_completed_at() -> dict[str, Any]:
                return {"applied_in_main_status_write": new_status == "completed"}

            await _run_completion_effect(
                _effect_runner,
                "completed_at",
                "job_disposition",
                _record_completed_at,
            )

            # Update job dict with new status for downstream checks
            job["status"] = new_status
            if completion_outcome_kind is not None:
                job["completion_outcome_kind"] = completion_outcome_kind

        # 1b. Notify operator for freeze events that require human action
        _NOTIFIABLE_FREEZE_TYPES = {
            "vm_upgrade_required",
            "job_complete",
            "budget_exceeded",
        }
        if new_status in ("pending_review", "paused") and result.get("freeze_data"):
            fd = result["freeze_data"]
            if isinstance(fd, str):
                fd = json.loads(fd)
            ft = fd.get("freeze_type")
            if ft in _NOTIFIABLE_FREEZE_TYPES:
                sudo_request_id = None
                auto_denied = False

                # For vm_upgrade freezes, create a sudo_approval_requests record
                # so the operator can approve/deny from the Cockpit Sudo tab.
                if ft == "vm_upgrade_required":
                    # Auto-deny an approval the job owner can never satisfy
                    # (Teleport / GCP-PAM model: unsatisfiable requests are
                    # rejected at creation). Raising it anyway would park the
                    # job for 24 h on a decision no human is entitled to make.
                    # An auto_denied row is still written for audit parity.
                    denial_detail: str | None = None
                    try:
                        owner = (
                            await postgres_db.get_user(str(job["user_id"]))
                            if job.get("user_id")
                            else None
                        )
                        try:
                            await _check_vm_permission(owner, job_needs_vm=True)
                        except HTTPException as he:
                            denial_detail = str(he.detail)
                    except Exception:
                        # Infra failure — don't guess; raise the approval normally.
                        logger.exception(
                            f"VM permission pre-check failed for {job_id}; "
                            "raising the approval request normally"
                        )

                    async def _create_sudo_approval_request() -> dict[str, Any]:
                        try:
                            request_id = await sudo_gate.insert_vm_upgrade_request(
                                job_id=job_id,
                                command=fd.get("command", "unknown"),
                                reason=fd.get("reason", ""),
                                config_name=job.get("config_name", ""),
                                status="auto_denied" if denial_detail else "pending",
                                decision_reason=denial_detail or "",
                            )
                        except Exception as exc:
                            logger.warning(
                                "Failed to create sudo request for %s: %s",
                                job_id,
                                exc,
                            )
                            return {
                                "request_id": None,
                                "denial_detail": denial_detail,
                                "error": str(exc),
                            }
                        return {
                            "request_id": request_id,
                            "denial_detail": denial_detail,
                        }

                    sudo_effect = await _run_completion_effect(
                        _effect_runner,
                        "sudo_approval_request",
                        "sudo_request",
                        _create_sudo_approval_request,
                        retry_if=lambda output: bool(output.get("error")),
                    )
                    sudo_request_id = sudo_effect["request_id"]
                    denial_detail = sudo_effect["denial_detail"]
                    if sudo_request_id:
                        actions.append(
                            f"sudo request created ({sudo_request_id[:8]})"
                            + (" [auto-denied]" if denial_detail else "")
                        )

                    if denial_detail:

                        async def _auto_deny_vm_upgrade() -> dict[str, Any]:
                            try:
                                await _resume_job_without_vm_internal(
                                    job_id,
                                    decided_by="system",
                                    reason=denial_detail,
                                    denied=True,
                                    completion_owner_command_id=(
                                        _effect_runner.command_id
                                        if _effect_runner is not None
                                        else None
                                    ),
                                    completion_owner=(
                                        _effect_runner.owner
                                        if _effect_runner is not None
                                        else None
                                    ),
                                )
                            except Exception as exc:
                                # Preserve the legacy fallback to manual review.
                                logger.exception(
                                    "Auto-deny resume failed for %s; leaving the "
                                    "job paused for a manual decision",
                                    job_id,
                                )
                                return {"auto_denied": False, "error": str(exc)}
                            return {"auto_denied": True}

                        auto_deny_effect = await _run_completion_effect(
                            _effect_runner,
                            "auto_deny_resume",
                            "auto_deny_resume",
                            _auto_deny_vm_upgrade,
                            retry_if=lambda output: bool(output.get("error")),
                        )
                        auto_denied = bool(auto_deny_effect["auto_denied"])
                        if auto_denied:
                            actions.append(
                                "vm upgrade auto-denied — job continues on its "
                                "original tier"
                            )

                if not auto_denied:
                    if ft == "vm_upgrade_required":
                        # Durable capture while the workspace is certainly
                        # alive — the job now parks on a 24h human decision and
                        # the workspace only stays warm for the reap grace.
                        async def _schedule_freeze_snapshot() -> dict[str, Any]:
                            if _effect_runner is None:
                                # Historical latency contract while the durable
                                # path is dark: schedule and return immediately.
                                asyncio.create_task(
                                    _capture_workspace_snapshot_for_freeze(job, job_id),
                                    name=f"freeze-capture-{job_id[:8]}",
                                )
                            else:
                                # A durable effect cannot mark "scheduled" as
                                # done: an orchestrator crash would lose the
                                # detached task permanently. Class D may lag,
                                # but it remains at-least-once, so the flagged
                                # finalizer awaits the capture attempt before
                                # committing its marker.
                                captured = await _capture_workspace_snapshot_for_freeze(
                                    job, job_id
                                )
                                return {"scheduled": True, "captured": captured}
                            return {"scheduled": True}

                        await _run_completion_effect(
                            _effect_runner,
                            "freeze_workspace_snapshot",
                            "workspace_snapshot",
                            _schedule_freeze_snapshot,
                            retry_if=lambda output: output.get("captured") is False,
                        )

                    async def _send_freeze_notification() -> dict[str, Any]:
                        try:
                            recorded = await _notify_operator_freeze(
                                job,
                                job_id,
                                ft,
                                fd,
                                sudo_request_id=sudo_request_id,
                                dedup_key=_completion_effect_dedup_key(
                                    _effect_runner, "freeze_notification", job_id
                                ),
                            )
                        except Exception as exc:
                            logger.warning(
                                "Failed to send freeze notification for %s: %s",
                                job_id,
                                exc,
                            )
                            return {"sent": False, "error": str(exc)}
                        return {
                            "sent": True,
                            "notification_id": (
                                recorded.notification_id if recorded else None
                            ),
                            "inserted": bool(recorded and recorded.inserted),
                        }

                    freeze_notification = await _run_completion_effect(
                        _effect_runner,
                        "freeze_notification",
                        "freeze_notification",
                        _send_freeze_notification,
                        retry_if=lambda output: bool(output.get("error")),
                    )
                    if freeze_notification["sent"]:
                        actions.append(f"notification sent ({ft})")

        # A legitimate control writer may win after S17 commits. Revalidate
        # that exact command-owned disposition immediately before the first
        # Class C effect; a miss supersedes the whole command before graft,
        # spawn, merge, parent-unblock, or teardown can begin.
        if _effect_runner is not None:
            await _effect_runner.assert_disposition_authority()

        # 2. Subjob output graft (uniform for all subjob types; critic skipped
        # inside). Reordered terminal commands already ran this delivery before
        # S17; every other command reaches the historical tail call here.
        if main_status_already_completed or (
            "subjob_output_graft" not in status_order.pre_status_delivery_effects
        ):
            pre_status_subjob_actions = await _run_subjob_output_graft_effect()
        # Effects move, response presentation does not: emit S26's stored
        # action at its historical tail location after S17.
        actions.extend(pre_status_subjob_actions)

        # 3. Handle critic verdict (if this is a critic job). The flag-off arm
        # remains the historical callback-direct path. Durable S27 publishes
        # the target transition and its effect marker in one transaction;
        # only that winner may run dispatch/wake/notification follow-ups.
        if _effect_runner is None:

            async def _apply_critic_verdict() -> dict[str, Any]:
                effect_actions: list[str] = []
                try:
                    await _handle_critic_verdict_on_complete(job, effect_actions)
                except Exception as exc:
                    logger.error(
                        f"Error handling critic verdict for {job_id}: {exc}",
                        exc_info=True,
                    )
                    return {"actions": effect_actions, "error": str(exc)}
                return {"actions": effect_actions}

            critic_verdict = await _run_completion_effect(
                None,
                "critic_verdict",
                "critic_verdict",
                _apply_critic_verdict,
                retry_if=lambda output: bool(output.get("error")),
            )
            actions.extend(critic_verdict["actions"])
        else:
            if pre_status_critic_verdict is not None:
                critic_verdict = pre_status_critic_verdict
            else:

                async def _materialize_critic_verdict() -> dict[str, Any]:
                    return await _materialize_critic_verdict_transactional(job)

                critic_verdict = await _run_completion_effect(
                    _effect_runner,
                    "critic_verdict",
                    "critic_verdict",
                    _materialize_critic_verdict,
                    transactional=True,
                    supersede_if=lambda output: (
                        output.get("applicable") is True
                        and output.get("world_cas_won") is False
                    ),
                )
            # A command that crossed S27 before M3 replays its legacy output;
            # its side effects already ran and must not be synthesized again.
            if "world_cas_won" not in critic_verdict:
                actions.extend(critic_verdict.get("actions") or [])
            elif critic_verdict.get("world_cas_won"):

                async def _critic_verdict_followup() -> dict[str, Any]:
                    return await _run_critic_verdict_followups(
                        critic_verdict,
                        completion_command_id=_effect_runner.command_id,
                    )

                critic_followup = await _run_completion_effect(
                    _effect_runner,
                    "critic_verdict_followup",
                    "critic_verdict_followup",
                    _critic_verdict_followup,
                )
                actions.extend(critic_followup["actions"])

        # 3b. Handle scholar completion (unblock parent job)
        async def _unblock_scholar_parent() -> dict[str, Any]:
            effect_actions: list[str] = []
            try:
                await _handle_scholar_completion(job, effect_actions)
            except Exception as exc:
                logger.error(
                    f"Error handling scholar completion for {job_id}: {exc}",
                    exc_info=True,
                )
                return {"actions": effect_actions, "error": str(exc)}
            return {"actions": effect_actions}

        scholar_unblock = await _run_completion_effect(
            _effect_runner,
            "scholar_parent_unblock",
            "scholar_unblock",
            _unblock_scholar_parent,
            retry_if=lambda output: bool(output.get("error")),
            retry_on_error=True,
            error_output=lambda exc: {"actions": [], "error": str(exc)},
            depends_on_groups=("subjob_graft",),
        )
        actions.extend(scholar_unblock["actions"])

        # 3c. Handle delegation child completion (resume parent when all siblings done)
        async def _unblock_delegation_parent() -> dict[str, Any]:
            effect_actions: list[str] = []
            try:
                await _handle_delegation_child_completion(job, effect_actions)
            except Exception as exc:
                logger.error(
                    f"Error handling delegation child completion for {job_id}: {exc}",
                    exc_info=True,
                )
                return {"actions": effect_actions, "error": str(exc)}
            return {"actions": effect_actions}

        delegation_unblock = await _run_completion_effect(
            _effect_runner,
            "delegation_parent_unblock",
            "delegation_unblock",
            _unblock_delegation_parent,
            retry_if=lambda output: bool(output.get("error")),
            retry_on_error=True,
            error_output=lambda exc: {"actions": [], "error": str(exc)},
            depends_on_groups=("subjob_graft",),
        )
        actions.extend(delegation_unblock["actions"])

        # 4. Trigger verification (if this is a main job that completed).
        # Durable S30 owns only DB materialization in its first effect; branch
        # creation and dispatch happen after commit in a separate effect.
        if _effect_runner is None:

            async def _spawn_verification_critic() -> dict[str, Any]:
                effect_actions: list[str] = []
                if completion_outcome_kind == "blocked_undelivered":
                    return {"actions": effect_actions}
                try:
                    await _trigger_verification_on_complete(
                        job,
                        result,
                        effect_actions,
                    )
                except Exception as exc:
                    logger.error(
                        f"Error triggering verification for {job_id}: {exc}",
                        exc_info=True,
                    )
                    return {"actions": effect_actions, "error": str(exc)}
                return {"actions": effect_actions}

            verification_spawn = await _run_completion_effect(
                None,
                "verification_critic_spawn",
                "verification",
                _spawn_verification_critic,
                retry_if=lambda output: bool(output.get("error")),
            )
            actions.extend(verification_spawn["actions"])
        else:
            expected_verification_round = len(_verification_rounds(job))

            async def _materialize_verification_critic() -> dict[str, Any]:
                if completion_outcome_kind == "blocked_undelivered":
                    return {
                        "applicable": False,
                        "world_cas_won": True,
                        "action": "noop",
                        "actions": [],
                    }
                return await _materialize_verification_critic_transactional(
                    job,
                    result,
                    expected_round=expected_verification_round,
                )

            verification_spawn = await _run_completion_effect(
                _effect_runner,
                "verification_critic_spawn",
                "verification",
                _materialize_verification_critic,
                transactional=True,
                supersede_if=lambda output: (
                    output.get("applicable") is True
                    and output.get("world_cas_won") is False
                ),
            )
            if "world_cas_won" not in verification_spawn:
                # Pre-M3 completed effect: its embedded external handoff
                # already ran, so replay only its stored actions.
                actions.extend(verification_spawn.get("actions") or [])
            elif (
                verification_spawn.get("world_cas_won")
                and verification_spawn.get("action") != "noop"
            ):

                async def _handoff_verification_critic() -> dict[str, Any]:
                    return await _run_verification_critic_handoff(verification_spawn)

                verification_handoff = await _run_completion_effect(
                    _effect_runner,
                    "verification_critic_handoff",
                    "verification_handoff",
                    _handoff_verification_critic,
                    depends_on_groups=("verification",),
                )
                actions.extend(verification_handoff["actions"])

        # 5. Curation final pass (if no verification but curation enabled, and goal achieved)
        if (
            not is_verification_enabled(job)
            and is_curation_enabled(job)
            and result.get("should_stop")
            and result.get("goal_achieved")
            and completion_outcome_kind != "blocked_undelivered"
        ):

            async def _start_curation_final_pass() -> dict[str, Any]:
                try:
                    curation_kwargs = (
                        {"completion_command_id": _effect_runner.command_id}
                        if _effect_runner is not None
                        else {}
                    )
                    await _trigger_curation_final_pass(
                        job_id,
                        job,
                        **curation_kwargs,
                    )
                except Exception as exc:
                    logger.error(
                        f"Error triggering curation for {job_id}: {exc}",
                        exc_info=True,
                    )
                    return {"triggered": False, "error": str(exc)}
                return {"triggered": True}

            curation_pass = await _run_completion_effect(
                _effect_runner,
                "curation_final_pass",
                "curation",
                _start_curation_final_pass,
                retry_if=lambda output: bool(output.get("error")),
            )
            if curation_pass["triggered"]:
                actions.append("curation final pass triggered (no verification)")

        # 5d. Advance project self-improvement loop (if this job belongs to one).
        # Loop jobs run bare, so this is the only completion hook that fires for
        # them; it spawns the next role's job or stops the loop on budget.
        # Only a TERMINAL outcome advances the loop: a paused job (e.g. the
        # memory_unavailable bounded-retry) is re-dispatched as the SAME job, so
        # the loop must keep waiting on it rather than rotate to the next role.
        # knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md
        if _effect_runner is None:

            async def _advance_completion_project_loop() -> dict[str, Any]:
                effect_actions: list[str] = []
                try:
                    if job.get("status") in ("completed", "failed", "cancelled"):
                        await _advance_project_loop(job, result, effect_actions)
                except Exception as exc:
                    logger.error(
                        f"Error advancing project loop for {job_id}: {exc}",
                        exc_info=True,
                    )
                    return {"actions": effect_actions, "error": str(exc)}
                return {"actions": effect_actions}

            loop_advance = await _run_completion_effect(
                _effect_runner,
                "project_loop_advance",
                "project_loop",
                _advance_completion_project_loop,
                retry_if=lambda output: bool(output.get("error")),
            )
            actions.extend(loop_advance["actions"])
        else:
            # S32's barrier claim is part of the same DB transaction as the
            # successor INSERTs and loop pointer/counter/campaign writeback.
            # Preparing kickoffs may read vector/history stores, so do it
            # before opening the transaction. A replayed terminal effect skips
            # that planning entirely and uses its persisted output.
            loop_advance = await _effect_runner.terminal_detail("project_loop_advance")
            legacy_loop_replay = (
                isinstance(loop_advance, Mapping) and "applicable" not in loop_advance
            )
            if loop_advance is None:
                prepared_loop_advance = None
                if job.get("status") in ("completed", "failed", "cancelled"):
                    prepared_loop_advance = await _prepare_atomic_project_loop_advance(
                        job,
                        result,
                        completion_command_id=_effect_runner.command_id,
                    )

                async def _materialize_completion_project_loop() -> dict[str, Any]:
                    return await _materialize_prepared_project_loop_advance(
                        prepared_loop_advance,
                        job,
                    )

                loop_advance = await _run_completion_effect(
                    _effect_runner,
                    "project_loop_advance",
                    "project_loop",
                    _materialize_completion_project_loop,
                    transactional=True,
                    supersede_if=lambda output: (
                        bool(output.get("applicable")) and not bool(output.get("won"))
                    ),
                )
                legacy_loop_replay = False

            if legacy_loop_replay:
                # Pre-M3 S32 ran all DB and external work inside the one effect.
                # Preserve its terminal output exactly; synthesizing a new
                # handoff would duplicate those already-executed consequences.
                actions.extend(loop_advance.get("actions") or [])
            else:
                # External provisioning, cloud baseline, KB/vector consequences,
                # notifications, officer wake and dispatch are independently
                # journaled. A crash after the DB commit replays S32's IDs then
                # resumes this handoff; it never re-enters the materializer.
                async def _handoff_completion_project_loop() -> dict[str, Any]:
                    return await _execute_persisted_project_loop_handoff(
                        job,
                        loop_advance,
                    )

                loop_handoff = await _run_completion_effect(
                    _effect_runner,
                    "project_loop_advance_handoff",
                    "project_loop_handoff",
                    _handoff_completion_project_loop,
                    retry_on_error=True,
                    error_output=_project_loop_handoff_error_output,
                    depends_on_groups=("project_loop",),
                )
                actions.extend(loop_handoff["actions"])

        # 5d2. Structured terminal history (project_jobs_repo_retirement.md).
        # New jobs write one database record; no history file is committed into
        # their execution repo. Loop jobs are skipped inside this generic hook
        # because the advance above owns their delivery-aware record. The same
        # call runs in approve_job, where a review-autonomy job transitions.
        # A narrow legacy merge path remains for in-flight jobs that were
        # already attached to a shared project repo before this migration.
        # Best-effort: a failure here never blocks completion handling.
        if main_status_already_completed or (
            "terminal_merge_change_record"
            not in status_order.pre_status_delivery_effects
        ):
            pre_status_terminal_actions = await _run_terminal_delivery_effect()
        # As above, S33's user-visible action retains the pre-step-4 ordering.
        actions.extend(pre_status_terminal_actions)

        # 5e. Wake the session that created this job, if any. Must sit BEFORE
        # the workspace archive below: that call tears the workspace down, and
        # a wake that raced it would point the session at a workspace being
        # deleted underneath it. Enqueue-only — the actual send happens after
        # this request commits (see kick_drain at the end).
        #
        # Keyed on new_status, deliberately NOT falling back to job['status']:
        # new_status is the outcome of THIS completion, and None means nothing
        # terminal happened here (the loop-advance suppression path). The stale
        # entry-time status would enqueue a wake for a transition that did not
        # occur. Anything genuinely terminal that this call misses is picked up
        # by the sweeper, which reads the row's real status.
        # knowledge-base/knowledge/features/session_wake_on_job_completion.md
        if new_status:

            async def _enqueue_session_wake() -> dict[str, Any]:
                await maybe_wake_session(postgres_db, job_id, new_status)
                return {"enqueued": True}

            await _run_completion_effect(
                _effect_runner,
                "session_wake_enqueue",
                "session_wake_enqueue",
                _enqueue_session_wake,
                retry_on_error=True,
                error_output=lambda exc: {
                    "enqueued": False,
                    "error": str(exc),
                },
            )

        # 6. Trigger dispatch (freed agent can pick up queued work)
        async def _kick_dispatch() -> dict[str, Any]:
            _trigger_dispatch()
            return {"triggered": True}

        await _run_completion_effect(
            _effect_runner,
            "dispatch_trigger",
            "dispatch",
            _kick_dispatch,
        )

        # 7. Archive workspace (snapshot to S3) and clean up VM/container
        if job.get("status") in ("completed", "failed") or (
            job.get("status") == "cancelled"
            and completion_outcome_kind == "blocked_undelivered"
        ):
            workspace_cleanup = await _run_completion_workspace_teardown(
                job_id,
                _effect_runner,
            )
            actions.extend(workspace_cleanup["actions"])

        # Fast path for the wake enqueued above. Every statement here
        # autocommits, so the terminal status is already durable; this only
        # skips the sweeper's tick. Fire-and-forget by design — losing it is
        # harmless because the claim, not this call, is the mechanism.
        async def _kick_wake_drain() -> dict[str, Any]:
            _kick_session_wake_drain(postgres_db)
            return {"triggered": True}

        await _run_completion_effect(
            _effect_runner,
            "session_wake_drain_kick",
            "session_wake_kick",
            _kick_wake_drain,
        )

        return {
            "status": "handled",
            "job_id": job_id,
            "new_status": new_status or job["status"],
            "actions": actions,
        }

    except HTTPException:
        raise
    except Exception as e:
        if _effect_runner is not None:
            # Keep the default-off route from importing the finalizer. Durable
            # status races are command state-machine signals, not HTTP-500
            # failures, and must reach CompletionFinalizer's supersede handler.
            from orchestrator.services.completion_finalizer import (
                CompletionDispositionSuperseded,
            )

            if isinstance(e, CompletionDispositionSuperseded):
                raise
        logger.exception(f"Failed to handle completion for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


__all__ = [
    "LegacyCompletionDependencies",
    "LegacyCompletionEffectOperations",
    "LegacyPersistenceDependencies",
    "LegacyPostCommitDependencies",
    "LegacySubjobDependencies",
    "LegacyVerificationDependencies",
    "LegacyWorkspaceDependencies",
    "complete_job_legacy",
]
