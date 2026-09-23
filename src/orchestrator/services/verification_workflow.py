"""Verification critics, durable verdicts, and completion-decision journals.

This module owns the B08 verification boundary.  Jobs-row mutations happen in
the transaction functions; dispatch, notification, repository handoff, and
session wake work happen only in the corresponding follow-up functions.  The
split is required by the durable completion effect runner: a retry may repeat
an idempotent external effect, but it must never infer a verdict from that
effect or make an external call from inside the jobs transaction.

The verification ledger remains the authority for critic outcomes.  A missing
or malformed verdict fails closed and can only escalate the target.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import HTTPException

from orchestrator.database.postgres import (
    CompletionDecisionBlocked,
    DatasourcePolicyConflictError,
    _stateless_resume_context,
)
from orchestrator.services.completion import (
    _parse_freeze_data,
    format_verification_instructions,
    get_autonomy_level,
    get_verification_config,
    is_curation_enabled,
    is_job_completion_freeze,
    is_verification_enabled,
)
from orchestrator.services.completion_control import completion_control_claim_active
from orchestrator.services.manifest_runtime_ownership import require_srw_runtime
from orchestrator.services.project_loops import job_loop_id
from orchestrator.services.verification_ledger import (
    assign_ids,
    compute_verdict,
    escalation_status,
    fold_open_findings,
    render_prior_findings,
    validate_dispositions,
    validate_verdict_call,
)
from shared.run_queue import unpark_unit
from shared.pinned_job_delivery import stamp_pinned_resume_input_ids
from shared.worker_queue import enqueue_worker_batch_wake, reset_worker_batch_attempts

logger = logging.getLogger(__name__)

_CRITIC_TERMINAL_OK = {"completed"}
_CRITIC_ACTIONABLE_STATUSES = {"completed", "failed", "cancelled", "pending_review"}
_CRITIC_DIAGNOSTIC_LIMIT_BYTES = 1024
_MAX_VERDICT_REJECTIONS = 3


@dataclass(frozen=True)
class VerificationTransactionPorts:
    """Ports used while deciding or materializing durable verification state."""

    revalidate_datasource_selection: Callable[
        [dict[str, Any]], Awaitable[tuple[list[str], dict[str, Any]]]
    ]
    datasource_selection_provenance: Callable[..., Awaitable[dict[str, Any]]]
    resolve_workspace_contract: Callable[[dict[str, Any]], Any]
    deep_merge_dicts: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]
    is_lite_config_override: Callable[[Any], bool]
    enqueue_worker_batch_wake: Callable[..., Awaitable[Any]] = enqueue_worker_batch_wake
    reset_worker_batch_attempts: Callable[..., Awaitable[Any]] = (
        reset_worker_batch_attempts
    )
    unpark_unit: Callable[..., Awaitable[Any]] = unpark_unit
    stateless_resume_context: Callable[[dict[str, Any]], dict[str, Any]] = (
        _stateless_resume_context
    )


@dataclass(frozen=True)
class VerificationEffectPorts:
    """Ports that may perform work after a durable verification decision."""

    forge: Any
    notifier: Any
    prepare_job_repository_authority: Callable[..., Awaitable[dict[str, Any] | None]]
    trigger_dispatch: Callable[[], Any]
    maybe_wake_session: Callable[..., Awaitable[Any]]
    kick_session_wake_drain: Callable[[Any], Any]
    trigger_curation_final_pass: Callable[..., Awaitable[Any]]

    # Legacy completion callbacks retained until the command-only path is the
    # sole completion implementation. Their owners are injected; this module
    # does not create a second lifecycle authority.
    set_target_to_autonomy_status: Callable[[str], Awaitable[str]]
    escalate_target: Callable[[str, dict[str, Any], str], Awaitable[str]]
    internal_resume_job: Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class VerificationDependencies:
    """Per-application verification collaborators.

    ``store`` is intentionally shared by the transaction and effect groups.
    The groups make the retry boundary visible without introducing an
    application-sized dependency container.
    """

    store: Any
    transaction: VerificationTransactionPorts
    effects: VerificationEffectPorts


def verification_rounds(job: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Read ``context.verification_rounds`` through one defensive JSONB parse."""

    if not job:
        return []
    context = job.get("context")
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, ValueError):
            return []
    if not isinstance(context, dict):
        return []
    rounds = context.get("verification_rounds")
    return rounds if isinstance(rounds, list) else []


def is_verification_critic(job: dict[str, Any]) -> bool:
    """Return true only for a child explicitly assigned a verification target."""

    context = job.get("context")
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, ValueError):
            return False
    return bool(isinstance(context, dict) and context.get("verification_target"))


def resolve_critic_outcome(
    critic_job_id: str, critic_status: str, rounds: list[dict[str, Any]]
) -> tuple[str, str]:
    """Resolve a critic outcome from the target ledger, failing closed."""

    if critic_status not in _CRITIC_TERMINAL_OK:
        return (
            "escalate",
            f"Critic {critic_job_id} ended in status {critic_status!r}; "
            "no trustworthy verdict.",
        )
    for round_record in rounds:
        if round_record.get("critic_job_id") == critic_job_id:
            return (round_record.get("verdict", "returned"), "")
    return (
        "escalate",
        f"Critic {critic_job_id} finished with no verdict recorded on the "
        "verification ledger.",
    )


class CriticWorldCASMiss(RuntimeError):
    """Roll back tentative domain writes before superseding a synthesizer."""

    def __init__(self, observed_status: str) -> None:
        self.observed_status = observed_status
        super().__init__(f"critic target world CAS lost ({observed_status})")


def bounded_critic_text(
    value: Any, *, limit_bytes: int = _CRITIC_DIAGNOSTIC_LIMIT_BYTES
) -> str:
    """Bound critic diagnostics before persistence, logs, or notifications."""

    text = str(value or "")
    encoded = text.encode("utf-8")
    if len(encoded) <= limit_bytes:
        return text
    suffix = "…"
    budget = max(0, limit_bytes - len(suffix.encode("utf-8")))
    return encoded[:budget].decode("utf-8", errors="ignore") + suffix


def critic_verdict_transition(
    critic_job: dict[str, Any], target_job: dict[str, Any]
) -> dict[str, Any]:
    """Build the bounded, database-only transition from one locked target."""

    critic_job_id = str(critic_job["id"])
    target_job_id = str(target_job["id"])
    rounds = verification_rounds(target_job)
    outcome, reason = resolve_critic_outcome(
        critic_job_id, str(critic_job.get("status") or ""), rounds
    )
    reason = bounded_critic_text(reason)
    transition: dict[str, Any] = {
        "outcome": outcome,
        "reason": reason,
        "target_job_id": target_job_id,
        "critic_job_id": critic_job_id,
        "is_loop_job": bool(job_loop_id(target_job)),
        "curation_enabled": bool(is_curation_enabled(target_job)),
    }
    if outcome == "approved":
        transition["new_status"] = (
            "completed"
            if get_autonomy_level(target_job) == "full"
            else "pending_review"
        )
    elif outcome == "returned":
        open_findings = fold_open_findings(rounds)
        feedback_lines = ["## Open findings", ""]
        for finding in sorted(open_findings, key=lambda value: value.get("id", "")):
            feedback_lines.append(
                f"- **{finding['id']}** "
                f"[{finding.get('severity', 'unknown')}]: "
                f"{finding.get('claim', '')}"
            )
        transition.update(
            new_status="paused",
            feedback="\n".join(feedback_lines),
            feedback_reason=(
                "The critic reviewed the completed work and returned it with "
                "open findings; address them."
            ),
            open_finding_count=len(open_findings),
        )
    else:
        transition["new_status"] = escalation_status(
            is_loop_job=transition["is_loop_job"]
        )
    return transition


async def materialize_critic_verdict_transactional(
    critic_job: dict[str, Any], *, dependencies: VerificationDependencies
) -> dict[str, Any]:
    """Apply a critic verdict under the target's ``reviewing`` row lock."""

    if not is_verification_critic(critic_job):
        return {"applicable": False, "world_cas_won": True, "actions": []}
    critic_status = str(critic_job.get("status") or "")
    if critic_status not in _CRITIC_ACTIONABLE_STATUSES:
        return {"applicable": False, "world_cas_won": True, "actions": []}

    critic_context = critic_job.get("context") or {}
    if isinstance(critic_context, str):
        try:
            critic_context = json.loads(critic_context)
        except (TypeError, ValueError):
            critic_context = {}
    target_job_id = str((critic_context or {}).get("verification_target") or "")
    try:
        target_uuid = UUID(target_job_id)
    except (TypeError, ValueError):
        return {
            "applicable": True,
            "world_cas_won": False,
            "observed_status": "missing",
            "target_job_id": target_job_id,
            "actions": [],
        }

    store = dependencies.store
    async with store.acquire() as conn:
        hint = await conn.fetchrow(
            "SELECT jobs.*, "
            "extract(epoch FROM clock_timestamp())::float8 AS db_now_epoch "
            "FROM jobs WHERE id=$1::uuid",
            target_uuid,
        )
        if hint is None:
            return {
                "applicable": True,
                "world_cas_won": False,
                "observed_status": "missing",
                "target_job_id": target_job_id,
                "actions": [],
            }
        hinted_job = dict(hint)
        hinted_transition = critic_verdict_transition(critic_job, hinted_job)
        hinted_lane = str(hinted_job.get("execution_lane") or "pinned")
        queue_first = (
            hinted_lane == "stateless" and hinted_transition["outcome"] == "returned"
        )

        try:
            async with conn.transaction():
                if queue_first:
                    admitted = await dependencies.transaction.enqueue_worker_batch_wake(
                        conn,
                        job_id=target_uuid,
                        fair_key=(
                            str(hinted_job["user_id"])
                            if hinted_job.get("user_id")
                            else None
                        ),
                        priority=int(hinted_job.get("priority") or 0),
                    )

                locked = await conn.fetchrow(
                    "SELECT jobs.*, "
                    "extract(epoch FROM clock_timestamp())::float8 AS db_now_epoch "
                    "FROM jobs WHERE id=$1::uuid FOR UPDATE",
                    target_uuid,
                )
                if locked is None:
                    raise CriticWorldCASMiss("missing")
                target_job = dict(locked)
                observed_status = str(target_job.get("status") or "")
                if observed_status != "reviewing":
                    raise CriticWorldCASMiss(observed_status)
                if completion_control_claim_active(
                    target_job.get("context"),
                    now_epoch=float(target_job["db_now_epoch"]),
                ):
                    raise CriticWorldCASMiss("reviewing:control_claimed")

                transition = critic_verdict_transition(critic_job, target_job)
                lane = str(target_job.get("execution_lane") or "pinned")
                if queue_first != (
                    lane == "stateless" and transition["outcome"] == "returned"
                ):
                    raise RuntimeError(
                        "critic verdict transition changed across queue-first admission"
                    )

                if transition["outcome"] == "returned":
                    resume_values = {
                        "queued_feedback": transition["feedback"],
                        "queued_feedback_reason": transition["feedback_reason"],
                    }
                    resume_context = (
                        dependencies.transaction.stateless_resume_context(resume_values)
                        if queue_first
                        else stamp_pinned_resume_input_ids(resume_values)
                    )
                    if queue_first:
                        if (
                            await dependencies.transaction.reset_worker_batch_attempts(
                                conn, job_id=target_uuid
                            )
                            is None
                        ):
                            raise RuntimeError(
                                "critic return lost the worker queue row"
                            )
                        if (
                            admitted.state == "parked"
                            and not await dependencies.transaction.unpark_unit(
                                conn, unit_id=target_uuid
                            )
                        ):
                            raise RuntimeError(
                                "critic return could not unpark worker queue"
                            )
                    result = await conn.execute(
                        "UPDATE jobs SET "
                        "context=(COALESCE(context, '{}'::jsonb) "
                        "- 'completion_decision') || $2::jsonb || "
                        "CASE WHEN freeze_data IS NULL THEN '{}'::jsonb "
                        "ELSE jsonb_build_object('last_freeze_data', freeze_data) END, "
                        "status='paused', assigned_agent_id=NULL, freeze_data=NULL, "
                        "updated_at=CURRENT_TIMESTAMP "
                        "WHERE id=$1::uuid AND status='reviewing' "
                        "AND execution_lane=$3::text",
                        target_uuid,
                        json.dumps(resume_context),
                        lane,
                    )
                else:
                    new_status = str(transition["new_status"])
                    result = await conn.execute(
                        "UPDATE jobs SET status=$2::text, "
                        "context=CASE WHEN $2::text IN "
                        "('completed','failed','cancelled') "
                        "THEN COALESCE(context, '{}'::jsonb) "
                        "- 'completion_decision' ELSE context END, "
                        "completed_at=CASE WHEN $2::text='completed' "
                        "THEN COALESCE(completed_at, CURRENT_TIMESTAMP) "
                        "ELSE completed_at END, "
                        "error_message=CASE WHEN $3::text='' THEN error_message "
                        "ELSE $3::text END, updated_at=CURRENT_TIMESTAMP "
                        "WHERE id=$1::uuid AND status='reviewing'",
                        target_uuid,
                        new_status,
                        str(transition.get("reason") or ""),
                    )
                if result != "UPDATE 1":
                    raise CriticWorldCASMiss(observed_status)
        except CriticWorldCASMiss as miss:
            return {
                "applicable": True,
                "world_cas_won": False,
                "observed_status": miss.observed_status,
                "target_job_id": target_job_id,
                "critic_job_id": str(critic_job["id"]),
                "actions": [],
            }

    persisted_transition = {
        key: transition[key]
        for key in (
            "outcome",
            "target_job_id",
            "critic_job_id",
            "new_status",
            "open_finding_count",
        )
        if key in transition
    }
    return {
        "applicable": True,
        "world_cas_won": True,
        **persisted_transition,
        "actions": [],
    }


async def run_critic_verdict_followups(
    plan: Mapping[str, Any],
    *,
    completion_command_id: str,
    dependencies: VerificationDependencies,
) -> dict[str, Any]:
    """Run only external/idempotent consequences of a winning verdict CAS."""

    if not plan.get("applicable") or not plan.get("world_cas_won"):
        return {"actions": []}
    store = dependencies.store
    effects = dependencies.effects
    target_job_id = str(plan["target_job_id"])
    critic_job_id = str(plan["critic_job_id"])
    outcome = str(plan["outcome"])
    new_status = str(plan["new_status"])
    actions: list[str] = []
    target_job = await store.get_job(target_job_id)

    if outcome == "approved":
        logger.info("Critic %s approved target %s", critic_job_id, target_job_id)
        await effects.maybe_wake_session(store, target_job_id, new_status)
        effects.kick_session_wake_drain(store)
        actions.append(f"target {target_job_id} set to '{new_status}' (approved)")
        if target_job and is_curation_enabled(target_job):
            await effects.trigger_curation_final_pass(
                target_job_id,
                completion_command_id=completion_command_id,
            )
            actions.append(f"curation final pass triggered for {target_job_id}")
    elif outcome == "returned":
        logger.info(
            "Critic %s returned target %s (%s open finding(s))",
            critic_job_id,
            target_job_id,
            int(plan.get("open_finding_count") or 0),
        )
        effects.trigger_dispatch()
        actions.append(
            f"target {target_job_id} resumed with feedback from critic {critic_job_id}"
        )
    else:
        reason = bounded_critic_text((target_job or {}).get("error_message") or "")
        logger.warning(
            "Verification escalated target %s to %s: %s",
            target_job_id,
            new_status,
            reason,
        )
        try:
            await effects.maybe_wake_session(store, target_job_id, new_status)
            effects.kick_session_wake_drain(store)
        except Exception:
            logger.exception(
                "Session wake for escalated target %s failed (non-fatal)",
                target_job_id,
            )
        user_id = (target_job or {}).get("user_id")
        if target_job and not job_loop_id(target_job) and user_id:
            try:
                await effects.notifier.record_review_returned(
                    user_id=str(user_id),
                    job_id=target_job_id,
                    config_name=str(target_job.get("config_name") or ""),
                    reason=reason,
                )
            except Exception:
                logger.exception(
                    "Failed to notify owner of escalated target %s (non-fatal)",
                    target_job_id,
                )
        actions.append(
            bounded_critic_text(
                f"target {target_job_id} escalated to '{new_status}': {reason}"
            )
        )
    return {"actions": actions}


async def handle_critic_verdict_on_complete(
    job: dict[str, Any],
    actions: list[str],
    *,
    dependencies: VerificationDependencies,
) -> None:
    """Apply the legacy completion path's ledger-driven critic outcome."""

    if not is_verification_critic(job):
        return
    job_id = str(job["id"])
    context = job.get("context")
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, ValueError):
            context = {}
    target_job_id = str((context or {}).get("verification_target"))
    critic_status = job.get("status")
    if critic_status not in _CRITIC_ACTIONABLE_STATUSES:
        logger.debug(
            "Critic %s in non-actionable status %r — target %s left untouched",
            job_id,
            critic_status,
            target_job_id,
        )
        return

    target_job = await dependencies.store.get_job(target_job_id)
    if not target_job:
        logger.warning("Critic %s: target job %s not found", job_id, target_job_id)
        return
    rounds = verification_rounds(target_job)
    outcome, reason = resolve_critic_outcome(job_id, critic_status, rounds)
    effects = dependencies.effects
    if outcome == "approved":
        logger.info("Critic %s approved target %s", job_id, target_job_id)
        new_status = await effects.set_target_to_autonomy_status(target_job_id)
        actions.append(f"target {target_job_id} set to '{new_status}' (approved)")
        if is_curation_enabled(target_job):
            await effects.trigger_curation_final_pass(target_job_id, target_job)
            actions.append(f"curation final pass triggered for {target_job_id}")
    elif outcome == "returned":
        open_findings = fold_open_findings(rounds)
        feedback_lines = ["## Open findings", ""]
        for finding in sorted(open_findings, key=lambda value: value.get("id", "")):
            feedback_lines.append(
                f"- **{finding['id']}** "
                f"[{finding.get('severity', 'unknown')}]: "
                f"{finding.get('claim', '')}"
            )
        logger.info(
            "Critic %s returned target %s (%s open finding(s))",
            job_id,
            target_job_id,
            len(open_findings),
        )
        await effects.internal_resume_job(
            target_job_id,
            feedback="\n".join(feedback_lines),
            reason=(
                "The critic reviewed the completed work and returned it with "
                "open findings; address them."
            ),
        )
        actions.append(
            f"target {target_job_id} resumed with feedback from critic {job_id}"
        )
    else:
        status = await effects.escalate_target(target_job_id, target_job, reason)
        actions.append(f"target {target_job_id} escalated to '{status}': {reason}")


def verification_gate_decision(
    rounds: list[dict[str, Any]],
    content_tree: str | None,
    max_rounds: int,
) -> tuple[str, str]:
    """Decide whether to spawn another critic or escalate for human review."""

    if not rounds:
        return ("spawn", "")
    open_findings = fold_open_findings(rounds)
    if not open_findings:
        return ("spawn", "")
    open_ids = ", ".join(
        finding["id"] for finding in open_findings if finding.get("id")
    )
    previous_tree = rounds[-1].get("content_tree")
    if content_tree and previous_tree and content_tree == previous_tree:
        return (
            "escalate",
            f"No progress since round {len(rounds)}: the deliverable is unchanged "
            f"(content {content_tree[:8]}) while {len(open_findings)} finding(s) "
            f"remain open ({open_ids}).",
        )
    if max_rounds > 0 and len(rounds) >= max_rounds:
        return (
            "escalate",
            f"Round limit reached ({max_rounds}) with {len(open_findings)} "
            f"finding(s) still open ({open_ids}).",
        )
    return ("spawn", "")


def critic_config_override(parent_llm: dict[str, Any] | None) -> dict[str, Any]:
    """Build the constrained configuration stamped on verification critics."""

    override: dict[str, Any] = {
        "autonomy": "full",
        "tools": {
            "evaluation": ["approve_job_verdict", "return_job_with_feedback"],
            "job_inspection": True,
            "core": ["next_phase_todos", "todo_complete", "todo_list", "todo_rewind"],
            "communication": [],
        },
    }
    if parent_llm is not None:
        override["llm"] = parent_llm
    return override


async def setup_verification_critic_workspace(
    target_job: dict[str, Any],
    critic_job: dict[str, Any],
    critic_config: str,
    *,
    dependencies: VerificationDependencies,
    durable_reconcile: bool = False,
) -> None:
    """Finish a critic's idempotent repository and jobs-row handoff."""

    effects = dependencies.effects
    if not effects.forge.is_initialized:
        return

    critic_job_id = str(critic_job["id"])
    short_id = critic_job_id[:8]
    effective_config = str(critic_job.get("config_name") or critic_config)
    from_branch = target_job.get("branch_name") or "main"
    branch_name = f"subjob/{short_id}/{effective_config}"
    try:
        parent_authority = await effects.prepare_job_repository_authority(
            dependencies.store, effects.forge, target_job
        )
        if parent_authority is None:
            raise RuntimeError("Target repository authority is unavailable")
        parent_repo_name = str(parent_authority["repo_name"])
        branch_ok = await effects.forge.create_branch(
            parent_repo_name, branch_name, from_branch=from_branch
        )
        if not branch_ok:
            message = (
                f"Failed to create branch '{branch_name}' from '{from_branch}' "
                f"in '{parent_repo_name}' for critic {critic_job_id}"
            )
            logger.error(message)
            if durable_reconcile:
                raise RuntimeError(message)

        context_updated = await dependencies.store.bind_job_managed_repository(
            critic_job_id,
            repo_name=parent_repo_name,
            clean_url=str(parent_authority["clean_repo_url"]),
        )
        if durable_reconcile and not context_updated:
            raise RuntimeError(
                f"Critic {critic_job_id} disappeared during context handoff"
            )

        critic_context = critic_job.get("context") or {}
        if isinstance(critic_context, str):
            try:
                critic_context = json.loads(critic_context)
            except (json.JSONDecodeError, ValueError):
                critic_context = {}
        worktree_path = None
        if isinstance(critic_context, dict) and critic_context.get(
            "inherits_parent_workspace"
        ):
            worktree_path = (
                f"/home/agent-host/workspace/worktrees/{short_id}-{effective_config}"
            )

        async with dependencies.store.acquire() as conn:
            update_result = await conn.execute(
                "UPDATE jobs SET branch_name = $1, worktree_path = $2 "
                "WHERE id = $3::uuid",
                branch_name,
                worktree_path,
                critic_job_id,
            )
        if durable_reconcile and update_result != "UPDATE 1":
            raise RuntimeError(
                f"Critic {critic_job_id} disappeared during branch handoff"
            )
    except Exception as exc:
        logger.warning(
            "Failed to create Gitea branch for critic %s: %s", critic_job_id, exc
        )
        if durable_reconcile:
            raise


async def trigger_verification_on_complete(
    job: dict[str, Any],
    result: dict[str, Any],
    actions: list[str],
    *,
    dependencies: VerificationDependencies,
    reconcile_existing_critic: bool = False,
) -> None:
    """Legacy path: create a fresh critic or fail closed to human review."""

    store = dependencies.store
    transaction = dependencies.transaction
    effects = dependencies.effects
    job_id = str(job["id"])
    if result.get("error") or not result.get("should_stop", False):
        return
    if job.get("parent_job_id") is not None:
        logger.debug("Skipping verification for %s — it is a sub-job", job_id)
        return
    if transaction.is_lite_config_override(job.get("config_override")):
        logger.info(
            "Critic skipped for job %s: lite workspace backend has no git "
            "workspace for the verification subjob handoff",
            job_id,
        )
        return
    if not is_verification_enabled(job):
        logger.debug("Verification not enabled for job %s", job_id)
        return
    if not is_job_completion_freeze(job) and job.get("status") != "reviewing":
        logger.debug(
            "Skipping verification for %s — not a job completion freeze", job_id
        )
        return

    verification_config = get_verification_config(job)
    freeze_data = _parse_freeze_data(job) or {}
    rounds = verification_rounds(job)
    max_rounds = verification_config.get("max_rounds", 3)
    content_tree = freeze_data.get("content_tree")

    if freeze_data.get("delivery_failed"):
        reason = (
            freeze_data.get("delivery_error")
            or "The job-ending git push failed; deliverables were not delivered."
        )
        reason = f"Verification skipped — {reason}"
        await effects.escalate_target(job_id, job, reason)
        actions.append(f"target {job_id} escalated: {reason}")
        return

    action, reason = verification_gate_decision(rounds, content_tree, max_rounds)
    if action == "escalate":
        await effects.escalate_target(job_id, job, reason)
        actions.append(f"target {job_id} escalated: {reason}")
        return

    critic_config = verification_config.get("critic_config", "critic")
    if reconcile_existing_critic:
        existing_critic = await store.get_verification_critic_for_round(
            job_id, len(rounds)
        )
        if existing_critic is not None:
            await setup_verification_critic_workspace(
                job,
                existing_critic,
                critic_config,
                dependencies=dependencies,
                durable_reconcile=True,
            )
            critic_job_id = str(existing_critic["id"])
            effects.trigger_dispatch()
            actions.append(f"critic job {critic_job_id} reconciled")
            logger.info(
                "Verification job %s reconciled for job %s", critic_job_id, job_id
            )
            return

    if await store.has_live_verification_critic(job_id):
        logger.info(
            "Critic skipped for job %s: one is already in flight "
            "(duplicate /complete for round %s)",
            job_id,
            len(rounds) + 1,
        )
        actions.append(f"critic already in flight for {job_id} — spawn skipped")
        return

    config_name = job.get("config_name", "unknown")
    instructions = format_verification_instructions(
        job_id=job_id,
        description=job.get("description", ""),
        freeze_data=freeze_data,
        config_name=config_name,
        prior_findings=render_prior_findings(fold_open_findings(rounds), len(rounds)),
    )
    if not instructions:
        logger.error("Failed to format verification instructions for job %s", job_id)
        return
    verification_description = (
        f"Verify deliverables of job {job_id} ({config_name}). "
        "Review output against original requirements and either approve or "
        "return with feedback."
    )
    context: dict[str, Any] = {
        "verification_target": job_id,
        "instructions": instructions,
        "original_description": job.get("description", ""),
        "original_config": config_name,
        "deliverables": freeze_data.get("deliverables", []),
        "summary": freeze_data.get("summary", ""),
        "confidence": freeze_data.get("confidence", 0),
        "verification_round": len(rounds),
        "max_verification_rounds": max_rounds,
    }
    parent_context = job.get("context") or {}
    if isinstance(parent_context, str):
        try:
            parent_context = json.loads(parent_context)
        except (json.JSONDecodeError, ValueError):
            parent_context = {}
    parent_workspace_backend = transaction.resolve_workspace_contract(
        job
    ).assigned_backend
    if parent_workspace_backend == "vm" and parent_context.get("vm"):
        context["inherits_parent_workspace"] = True
    elif parent_workspace_backend == "sandbox" and parent_context.get(
        "workspace_container"
    ):
        context["inherits_parent_workspace"] = True

    parent_override = job.get("config_override")
    if isinstance(parent_override, str):
        try:
            parent_override = json.loads(parent_override)
        except (json.JSONDecodeError, ValueError):
            parent_override = None
    parent_llm = None
    if isinstance(parent_override, dict) and isinstance(
        parent_override.get("llm"), dict
    ):
        parent_llm = parent_override["llm"]
    config_override = transaction.deep_merge_dicts(
        critic_config_override(parent_llm),
        {"workspace": {"backend": parent_workspace_backend}},
    )
    project_id = str(job["project_id"]) if job.get("project_id") else None

    logger.info(
        "Creating verification job for %s (critic_config=%s, round=%s, max_rounds=%s)",
        job_id,
        critic_config,
        len(rounds),
        max_rounds,
    )
    try:
        (
            critic_datasource_ids,
            critic_datasource_revisions,
        ) = await transaction.revalidate_datasource_selection(job)
    except HTTPException as exc:
        if exc.status_code != 403:
            raise
        reason = (
            "Verification could not start because the target's connector "
            "selection is no longer authorized."
        )
        await effects.escalate_target(job_id, job, reason)
        actions.append(f"target {job_id} escalated: connector access changed")
        return
    critic_owner_id = str(job["user_id"]) if job.get("user_id") else None
    critic_actor = await store.get_user(critic_owner_id) if critic_owner_id else None
    critic_datasource_provenance = await transaction.datasource_selection_provenance(
        datasource_ids=critic_datasource_ids,
        policy_revisions=critic_datasource_revisions,
        origin="inherited",
        effective_work_owner_id=critic_owner_id,
        actor=critic_actor,
        project_ids=[project_id] if project_id else [],
        creation_path="critic_lifecycle",
    )

    critic_was_reconciled = False
    try:
        critic_job = await store.create_job(
            origin="subjob",
            description=verification_description,
            config_name=critic_config,
            config_override=config_override,
            context=context,
            parent_job_id=job_id,
            project_id=project_id,
            priority=10,
            user_id=critic_owner_id,
            runner_kind="lifecycle",
            datasource_ids=critic_datasource_ids,
            datasource_selection_provenance=critic_datasource_provenance,
            datasource_policy_revisions=critic_datasource_revisions,
            authority_user_id=critic_owner_id,
            authority_project_ids=(
                [project_id] if critic_owner_id and project_id else []
            ),
            requested_workspace_backend=None,
            workspace_assignment_source="parent_inheritance",
        )
    except asyncpg.UniqueViolationError as exc:
        if getattr(exc, "constraint_name", None) != "jobs_verification_uniq":
            raise
        logger.info(
            "Critic skipped for job %s: round %d already has a critic",
            job_id,
            len(rounds),
        )
        if not reconcile_existing_critic:
            actions.append(
                f"critic round {len(rounds)} already exists for {job_id} — "
                "spawn skipped"
            )
            return
        critic_job = await store.get_verification_critic_for_round(job_id, len(rounds))
        if critic_job is None:
            raise RuntimeError(
                f"Verification critic index winner for {job_id} round "
                f"{len(rounds)} could not be resolved"
            ) from exc
        critic_was_reconciled = True
    except DatasourcePolicyConflictError:
        reason = (
            "Verification could not start because the target's connector "
            "policy changed concurrently."
        )
        await effects.escalate_target(job_id, job, reason)
        actions.append(f"target {job_id} escalated: connector policy changed")
        return

    critic_job_id = str(critic_job["id"])
    await setup_verification_critic_workspace(
        job,
        critic_job,
        critic_config,
        dependencies=dependencies,
        durable_reconcile=reconcile_existing_critic,
    )
    effects.trigger_dispatch()
    if critic_was_reconciled:
        actions.append(f"critic job {critic_job_id} reconciled")
        logger.info("Verification job %s reconciled for job %s", critic_job_id, job_id)
    else:
        actions.append(f"critic job {critic_job_id} created")
        logger.info("Verification job %s created for job %s", critic_job_id, job_id)


async def materialize_verification_critic_transactional(
    job: dict[str, Any],
    result: dict[str, Any],
    *,
    expected_round: int,
    dependencies: VerificationDependencies,
) -> dict[str, Any]:
    """Materialize a critic or escalation under the locked reviewing row."""

    transaction = dependencies.transaction
    store = dependencies.store
    job_id = str(job["id"])
    if (
        result.get("error")
        or not result.get("should_stop", False)
        or job.get("parent_job_id") is not None
        or transaction.is_lite_config_override(job.get("config_override"))
        or (
            not is_job_completion_freeze(job)
            and str(job.get("status") or "") != "reviewing"
        )
    ):
        return {
            "applicable": False,
            "world_cas_won": True,
            "action": "noop",
            "actions": [],
        }

    try:
        job_uuid = UUID(job_id)
    except (TypeError, ValueError):
        return {
            "applicable": True,
            "world_cas_won": False,
            "observed_status": "missing",
            "target_job_id": job_id,
            "actions": [],
        }

    async with store.acquire() as conn:
        parent_row = await conn.fetchrow(
            "SELECT jobs.*, "
            "extract(epoch FROM clock_timestamp())::float8 AS db_now_epoch "
            "FROM jobs WHERE id=$1::uuid FOR UPDATE",
            job_uuid,
        )
        if parent_row is None:
            return {
                "applicable": True,
                "world_cas_won": False,
                "observed_status": "missing",
                "target_job_id": job_id,
                "actions": [],
            }
        parent = dict(parent_row)
        if not is_verification_enabled(parent):
            return {
                "applicable": False,
                "world_cas_won": True,
                "action": "noop",
                "actions": [],
            }
        observed_status = str(parent.get("status") or "")
        if observed_status != "reviewing" or completion_control_claim_active(
            parent.get("context"), now_epoch=float(parent["db_now_epoch"])
        ):
            return {
                "applicable": True,
                "world_cas_won": False,
                "observed_status": (
                    observed_status
                    if observed_status != "reviewing"
                    else "reviewing:control_claimed"
                ),
                "target_job_id": job_id,
                "actions": [],
            }

        rounds = verification_rounds(parent)
        natural_round = len(rounds)
        if natural_round != int(expected_round):
            return {
                "applicable": True,
                "world_cas_won": False,
                "observed_status": f"reviewing:round-{natural_round}",
                "target_job_id": job_id,
                "expected_round": int(expected_round),
                "actions": [],
            }

        verification_config = get_verification_config(parent)
        max_rounds = int(verification_config.get("max_rounds", 3))
        freeze_data = _parse_freeze_data(job) or _parse_freeze_data(parent) or {}
        content_tree = freeze_data.get("content_tree")

        async def publish_escalation(reason: str, action_code: str) -> dict[str, Any]:
            bounded_reason = bounded_critic_text(reason)
            status = escalation_status(is_loop_job=bool(job_loop_id(parent)))
            updated = await conn.execute(
                "UPDATE jobs SET status=$2::text, error_message=$3::text, "
                "completed_at=CASE WHEN $2::text='completed' "
                "THEN COALESCE(completed_at, CURRENT_TIMESTAMP) "
                "ELSE completed_at END, updated_at=CURRENT_TIMESTAMP "
                "WHERE id=$1::uuid AND status='reviewing'",
                job_uuid,
                status,
                bounded_reason,
            )
            if updated != "UPDATE 1":
                return {
                    "applicable": True,
                    "world_cas_won": False,
                    "observed_status": observed_status,
                    "target_job_id": job_id,
                    "actions": [],
                }
            return {
                "applicable": True,
                "world_cas_won": True,
                "action": "escalate",
                "target_job_id": job_id,
                "new_status": status,
                "action_code": action_code,
                "actions": [],
            }

        escalation_reason = ""
        escalation_code = ""
        if freeze_data.get("delivery_failed"):
            escalation_reason = (
                freeze_data.get("delivery_error")
                or "The job-ending git push failed; deliverables were not delivered."
            )
            escalation_reason = f"Verification skipped — {escalation_reason}"
            escalation_code = "delivery_failed"
        else:
            gate_action, gate_reason = verification_gate_decision(
                rounds, content_tree, max_rounds
            )
            if gate_action == "escalate":
                escalation_reason = gate_reason
                escalation_code = "verification_gate"
        if escalation_reason:
            return await publish_escalation(escalation_reason, escalation_code)

        existing_critic = await store.get_verification_critic_for_round(
            job_id, natural_round
        )
        critic_config = str(verification_config.get("critic_config", "critic"))
        if existing_critic is not None:
            return {
                "applicable": True,
                "world_cas_won": True,
                "action": "handoff",
                "target_job_id": job_id,
                "critic_job_id": str(existing_critic["id"]),
                "verification_round": natural_round,
                "reconciled": True,
                "actions": [],
            }
        if await store.has_live_verification_critic(job_id):
            return {
                "applicable": True,
                "world_cas_won": True,
                "action": "noop",
                "target_job_id": job_id,
                "verification_round": natural_round,
                "actions": [],
            }

        config_name = str(parent.get("config_name") or "unknown")
        instructions = format_verification_instructions(
            job_id=job_id,
            description=str(parent.get("description") or ""),
            freeze_data=freeze_data,
            config_name=config_name,
            prior_findings=render_prior_findings(
                fold_open_findings(rounds), natural_round
            ),
        )
        if not instructions:
            raise RuntimeError(
                f"failed to format verification instructions for job {job_id}"
            )
        verification_description = (
            f"Verify deliverables of job {job_id} ({config_name}). "
            "Review output against original requirements and either approve "
            "or return with feedback."
        )
        critic_context: dict[str, Any] = {
            "verification_target": job_id,
            "instructions": instructions,
            "original_description": str(parent.get("description") or ""),
            "original_config": config_name,
            "deliverables": freeze_data.get("deliverables", []),
            "summary": freeze_data.get("summary", ""),
            "confidence": freeze_data.get("confidence", 0),
            "verification_round": natural_round,
            "max_verification_rounds": max_rounds,
        }
        parent_context = parent.get("context") or {}
        if isinstance(parent_context, str):
            try:
                parent_context = json.loads(parent_context)
            except (TypeError, ValueError):
                parent_context = {}
        parent_workspace_backend = transaction.resolve_workspace_contract(
            parent
        ).assigned_backend
        if parent_workspace_backend == "vm" and parent_context.get("vm"):
            critic_context["inherits_parent_workspace"] = True
        elif parent_workspace_backend == "sandbox" and parent_context.get(
            "workspace_container"
        ):
            critic_context["inherits_parent_workspace"] = True

        parent_override = parent.get("config_override")
        if isinstance(parent_override, str):
            try:
                parent_override = json.loads(parent_override)
            except (TypeError, ValueError):
                parent_override = None
        parent_llm = (
            parent_override.get("llm")
            if isinstance(parent_override, dict)
            and isinstance(parent_override.get("llm"), dict)
            else None
        )
        critic_override = transaction.deep_merge_dicts(
            critic_config_override(parent_llm),
            {"workspace": {"backend": parent_workspace_backend}},
        )
        project_id = str(parent["project_id"]) if parent.get("project_id") else None

        try:
            (
                critic_datasource_ids,
                critic_datasource_revisions,
            ) = await transaction.revalidate_datasource_selection(parent)
        except HTTPException as exc:
            if exc.status_code != 403:
                raise
            reason = (
                "Verification could not start because the target's connector "
                "selection is no longer authorized."
            )
            return await publish_escalation(reason, "connector_access_changed")

        critic_owner_id = str(parent["user_id"]) if parent.get("user_id") else None
        critic_actor = (
            await store.get_user(critic_owner_id) if critic_owner_id else None
        )
        critic_datasource_provenance = (
            await transaction.datasource_selection_provenance(
                datasource_ids=critic_datasource_ids,
                policy_revisions=critic_datasource_revisions,
                origin="inherited",
                effective_work_owner_id=critic_owner_id,
                actor=critic_actor,
                project_ids=[project_id] if project_id else [],
                creation_path="critic_lifecycle",
            )
        )

        critic_was_reconciled = False
        try:
            async with conn.transaction():
                critic_job = await store.create_job(
                    origin="subjob",
                    description=verification_description,
                    config_name=critic_config,
                    config_override=critic_override,
                    context=critic_context,
                    parent_job_id=job_id,
                    project_id=project_id,
                    priority=10,
                    user_id=critic_owner_id,
                    runner_kind="lifecycle",
                    datasource_ids=critic_datasource_ids,
                    datasource_selection_provenance=critic_datasource_provenance,
                    datasource_policy_revisions=critic_datasource_revisions,
                    authority_user_id=critic_owner_id,
                    authority_project_ids=(
                        [project_id] if critic_owner_id and project_id else []
                    ),
                    requested_workspace_backend=None,
                    workspace_assignment_source="parent_inheritance",
                )
        except asyncpg.UniqueViolationError as exc:
            if getattr(exc, "constraint_name", None) != "jobs_verification_uniq":
                raise
            critic_job = await store.get_verification_critic_for_round(
                job_id, natural_round
            )
            if critic_job is None:
                raise RuntimeError(
                    f"verification critic index winner for {job_id} round "
                    f"{natural_round} could not be resolved"
                ) from exc
            critic_was_reconciled = True
        except DatasourcePolicyConflictError:
            reason = (
                "Verification could not start because the target's connector "
                "policy changed concurrently."
            )
            return await publish_escalation(reason, "connector_policy_changed")

    return {
        "applicable": True,
        "world_cas_won": True,
        "action": "handoff",
        "target_job_id": job_id,
        "critic_job_id": str(critic_job["id"]),
        "verification_round": natural_round,
        "reconciled": critic_was_reconciled,
        "actions": [],
    }


async def run_verification_critic_handoff(
    plan: Mapping[str, Any], *, dependencies: VerificationDependencies
) -> dict[str, Any]:
    """Run repository, dispatch, wake, and notification work after materialization."""

    if not plan.get("applicable") or not plan.get("world_cas_won"):
        return {"actions": []}
    action = str(plan.get("action") or "noop")
    target_job_id = str(plan.get("target_job_id") or "")
    if action == "noop":
        return {"actions": []}

    store = dependencies.store
    effects = dependencies.effects
    if action == "handoff":
        critic_job_id = str(plan["critic_job_id"])
        target_job = await store.get_job(target_job_id)
        critic_job = await store.get_job(critic_job_id)
        if target_job is None or critic_job is None:
            raise RuntimeError("verification critic handoff lost a materialized job")
        await setup_verification_critic_workspace(
            target_job,
            critic_job,
            str(critic_job.get("config_name") or "critic"),
            dependencies=dependencies,
            durable_reconcile=True,
        )
        effects.trigger_dispatch()
        reconciled = bool(plan.get("reconciled"))
        verb = "reconciled" if reconciled else "created"
        logger.info(
            "Verification job %s %s for job %s",
            critic_job_id,
            verb,
            target_job_id,
        )
        return {"actions": [f"critic job {critic_job_id} {verb}"]}

    if action != "escalate":
        raise RuntimeError(f"unknown verification materialization action {action!r}")
    target_job = await store.get_job(target_job_id)
    if target_job is None:
        raise RuntimeError("verification escalation handoff lost its target job")
    status = str(target_job.get("status") or plan["new_status"])
    reason = bounded_critic_text(target_job.get("error_message") or "")
    logger.warning(
        "Verification escalated target %s to %s: %s",
        target_job_id,
        status,
        reason,
    )
    try:
        await effects.maybe_wake_session(store, target_job_id, status)
        effects.kick_session_wake_drain(store)
    except Exception:
        logger.exception(
            "Session wake for escalated target %s failed (non-fatal)", target_job_id
        )
    user_id = target_job.get("user_id")
    if not job_loop_id(target_job) and user_id:
        try:
            await effects.notifier.record_review_returned(
                user_id=str(user_id),
                job_id=target_job_id,
                config_name=str(target_job.get("config_name") or ""),
                reason=reason,
            )
        except Exception:
            logger.exception(
                "Failed to notify owner of escalated target %s (non-fatal)",
                target_job_id,
            )
    action_code = str(plan.get("action_code") or "")
    if action_code == "connector_access_changed":
        action_text = f"target {target_job_id} escalated: connector access changed"
    elif action_code == "connector_policy_changed":
        action_text = f"target {target_job_id} escalated: connector policy changed"
    else:
        action_text = bounded_critic_text(f"target {target_job_id} escalated: {reason}")
    return {"actions": [action_text]}


async def record_verification_round(
    *,
    target_job_id: str,
    critic_job_id: str,
    asserted_verdict: str,
    opened: list[dict[str, Any]],
    dispositions: list[dict[str, Any]],
    head_commit: str | None,
    content_tree: str | None = None,
    dependencies: VerificationDependencies,
) -> dict[str, Any]:
    """Validate, compute, and durably append one verification round."""

    store = dependencies.store
    if not critic_job_id:
        raise HTTPException(status_code=400, detail="critic_job_id is required")

    target = await store.get_job(target_job_id)
    if not target:
        raise HTTPException(status_code=404, detail=f"Job {target_job_id} not found")

    critic = await store.get_job(critic_job_id)
    critic_context = (critic or {}).get("context")
    if isinstance(critic_context, str):
        try:
            critic_context = json.loads(critic_context)
        except (json.JSONDecodeError, ValueError):
            critic_context = {}
    if not isinstance(critic_context, dict):
        critic_context = {}
    claimed_target = critic_context.get("verification_target")
    if not critic or str(claimed_target or "") != str(target_job_id):
        logger.warning(
            "Rejected verification round: critic %s is not the critic for target "
            "%s (its verification_target is %r)",
            critic_job_id,
            target_job_id,
            claimed_target,
        )
        raise HTTPException(
            status_code=403,
            detail=(
                f"Job {critic_job_id} is not the verification critic for "
                f"{target_job_id}. Record your verdict against the job you were "
                "asked to review."
            ),
        )

    target_freeze = _parse_freeze_data(target) or {}
    head_commit = target_freeze.get("head_commit") or head_commit
    content_tree = target_freeze.get("content_tree") or content_tree
    rounds = verification_rounds(target)

    for existing in rounds:
        if existing.get("critic_job_id") == critic_job_id:
            return {
                "verdict": existing.get("verdict"),
                "round": existing.get("round"),
                "assigned": existing.get("opened", []),
                "open_findings": fold_open_findings(rounds),
            }

    open_before = fold_open_findings(rounds)
    errors = validate_verdict_call(asserted_verdict, opened, open_before)
    errors += validate_dispositions(dispositions, open_before)
    if errors:
        rejections = await store.increment_verdict_rejections(critic_job_id)
        if rejections >= _MAX_VERDICT_REJECTIONS:
            reason = (
                f"Critic {critic_job_id} failed to render a valid verdict "
                f"after {rejections} rejected submissions; sent to manual "
                "review. Last rejection: " + "; ".join(errors)
            )
            await dependencies.effects.escalate_target(target_job_id, target, reason)
            raise HTTPException(
                status_code=409, detail={"errors": errors, "escalated": True}
            )
        raise HTTPException(status_code=409, detail={"errors": errors})

    assigned = assign_ids(opened, rounds)
    record = {
        "round": len(rounds) + 1,
        "critic_job_id": critic_job_id,
        "head_commit": head_commit,
        "content_tree": content_tree,
        "asserted_verdict": str(asserted_verdict).lower(),
        "opened": assigned,
        "dispositions": dispositions,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    open_after = fold_open_findings(rounds + [record])
    record["verdict"] = compute_verdict(record["asserted_verdict"], open_after)

    if record["verdict"] != record["asserted_verdict"]:
        logger.warning(
            "Verification verdict divergence for target %s (critic %s): "
            "model asserted %r, computed %r from %d open finding(s)",
            target_job_id,
            critic_job_id,
            record["asserted_verdict"],
            record["verdict"],
            len(open_after),
        )

    appended = await store.append_verification_round(target_job_id, record)
    if appended == 0:
        stored = await store.get_job(target_job_id)
        stored_rounds = verification_rounds(stored)
        for existing in stored_rounds:
            if existing.get("critic_job_id") == critic_job_id:
                return {
                    "verdict": existing.get("verdict"),
                    "round": existing.get("round"),
                    "assigned": existing.get("opened", []),
                    "open_findings": fold_open_findings(stored_rounds),
                }
        raise HTTPException(status_code=500, detail="Ledger append failed")

    return {
        "verdict": record["verdict"],
        "round": record["round"],
        "assigned": assigned,
        "open_findings": open_after,
    }


def parse_completion_decision(job: dict[str, Any] | None) -> dict[str, Any] | None:
    """Extract ``context.completion_decision`` with a defensive JSONB parse."""

    context = (job or {}).get("context")
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, ValueError):
            context = {}
    if not isinstance(context, dict):
        return None
    decision = context.get("completion_decision")
    return decision if isinstance(decision, dict) else None


async def record_completion_decision(
    *,
    job_id: str,
    tool_call_id: str,
    summary: str,
    deliverables: list[Any],
    confidence: float,
    notes: str | None,
    dependencies: VerificationDependencies,
) -> dict[str, Any]:
    """Validate and durably journal one worker completion decision."""

    if not tool_call_id:
        raise HTTPException(status_code=400, detail="tool_call_id is required")
    store = dependencies.store
    job = await store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    require_srw_runtime(job)
    if job.get("status") in ("completed", "failed", "cancelled"):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Job {job_id} is already terminal ({job.get('status')}); "
                "refusing to journal a completion decision for it."
            ),
        )

    existing = parse_completion_decision(job)
    if existing and existing.get("tool_call_id") == tool_call_id:
        return {"recorded": True, "replay": True, "decision": existing}

    decision = {
        "tool_call_id": tool_call_id,
        "summary": str(summary or ""),
        "deliverables": [str(value) for value in (deliverables or [])],
        "confidence": max(0.0, min(1.0, float(confidence))),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "job_id": str(job_id),
    }
    if notes:
        decision["notes"] = str(notes)

    try:
        journaled = await store.set_completion_decision(job_id, decision)
    except CompletionDecisionBlocked as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc
    if not journaled:
        raise HTTPException(
            status_code=409,
            detail=f"Job {job_id} changed state while journaling the decision",
        )

    logger.info(
        "Journaled completion decision for job %s "
        "(tool_call_id=%s, confidence=%s, %d deliverable(s))",
        job_id,
        tool_call_id,
        decision["confidence"],
        len(decision["deliverables"]),
    )
    return {"recorded": True, "replay": False, "decision": decision}


async def get_completion_decision(
    job_id: str, *, dependencies: VerificationDependencies
) -> dict[str, Any]:
    """Read back the durable completion decision for resume hydration."""

    job = await dependencies.store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return {"decision": parse_completion_decision(job)}


__all__ = [
    "CriticWorldCASMiss",
    "VerificationDependencies",
    "VerificationEffectPorts",
    "VerificationTransactionPorts",
    "bounded_critic_text",
    "critic_config_override",
    "critic_verdict_transition",
    "get_completion_decision",
    "handle_critic_verdict_on_complete",
    "is_verification_critic",
    "materialize_critic_verdict_transactional",
    "materialize_verification_critic_transactional",
    "parse_completion_decision",
    "record_completion_decision",
    "record_verification_round",
    "resolve_critic_outcome",
    "run_critic_verdict_followups",
    "run_verification_critic_handoff",
    "setup_verification_critic_workspace",
    "trigger_verification_on_complete",
    "verification_gate_decision",
    "verification_rounds",
]
