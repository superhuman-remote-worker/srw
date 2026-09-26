"""Pure logic functions for job completion handling.

Extracted from agent-side post-completion code (src/agent/api/app.py) so the
orchestrator can make verification/curation decisions without depending
on agent state.  All functions read config from the job dict's
``resolved_config`` JSONB — no live agent config needed.

Also provides lightweight disk-based config readers for decisions that
must be made at job *creation* time (before resolved_config exists),
such as scholar spawning.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
from collections.abc import Awaitable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from shared.job_freeze_types import (
    CONTINUE_AS_NEW_FREEZE_TYPES,
    ERROR_IMMUNE_FREEZE_TYPES,
    SUBJOB_REDISPATCH_FREEZE_TYPES,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Repo root for template resolution
# ---------------------------------------------------------------------------
def _find_repo_root() -> Path:
    """Walk up from this file to find the directory containing ``config/``."""
    anchor = Path(__file__).resolve().parent
    for _ in range(5):
        if (anchor / "config" / "expert_base.yaml").is_file():
            return anchor
        anchor = anchor.parent
    # Last resort: assume working directory (WORKDIR /app in Docker)
    return Path.cwd()


_REPO_ROOT = _find_repo_root()


# ---------------------------------------------------------------------------
# Config helpers — read from resolved_config JSONB
# ---------------------------------------------------------------------------


def _parse_resolved_config(job: dict[str, Any]) -> dict[str, Any]:
    """Parse resolved_config from a job dict (handles str or dict)."""
    rc = job.get("resolved_config")
    if rc is None:
        return {}
    if isinstance(rc, str):
        try:
            return json.loads(rc)
        except (json.JSONDecodeError, ValueError):
            return {}
    return rc if isinstance(rc, dict) else {}


def _get_subjob_config(job: dict[str, Any], key: str) -> dict[str, Any]:
    """Extract a subjob config section (verification/curator/scholar) from resolved_config.

    Resolution order:
    1. ``resolved_config.agent.{key}`` — standard path after flatten
    2. ``resolved_config.agent.extra.{key}`` — fallback if extra wasn't flattened
    3. ``resolved_config.{key}`` — direct top-level path
    """
    rc = _parse_resolved_config(job)

    agent_block = rc.get("agent")
    if isinstance(agent_block, dict):
        # Primary: flattened into agent dict
        val = agent_block.get(key)
        if isinstance(val, dict):
            return val
        # Fallback: still nested in extra
        extra = agent_block.get("extra")
        if isinstance(extra, dict):
            val = extra.get(key)
            if isinstance(val, dict):
                return val

    # Top-level fallback
    val = rc.get(key)
    if isinstance(val, dict):
        return val

    return {}


def get_verification_config(job: dict[str, Any]) -> dict[str, Any]:
    """Extract verification config from resolved_config."""
    return _get_subjob_config(job, "verification")


def is_verification_enabled(job: dict[str, Any]) -> bool:
    """Check if verification is enabled for a job.

    Falls back to reading from disk if resolved_config is NULL.
    """
    cfg = get_verification_config(job)
    if cfg:
        return bool(cfg.get("enabled", False))
    # Disk fallback when resolved_config is missing
    config_name = job.get("config_name") or "worker_base"
    config_override = job.get("config_override")
    if isinstance(config_override, str):
        try:
            config_override = json.loads(config_override)
        except (json.JSONDecodeError, ValueError):
            config_override = None
    return bool(
        _resolve_config_section_from_disk(
            "verification", config_name, config_override
        ).get("enabled", False)
    )


def get_curation_config(job: dict[str, Any]) -> dict[str, Any]:
    """Extract curation config from resolved_config."""
    return _get_subjob_config(job, "curator")


def is_curation_enabled(job: dict[str, Any]) -> bool:
    """Check if curation is enabled for a job."""
    return bool(get_curation_config(job).get("enabled", False))


def get_scholar_config(job: dict[str, Any]) -> dict[str, Any]:
    """Extract scholar config from resolved_config."""
    return _get_subjob_config(job, "scholar")


def is_scholar_enabled(job: dict[str, Any]) -> bool:
    """Check if scholar is enabled for a job."""
    return bool(get_scholar_config(job).get("enabled", False))


# ---------------------------------------------------------------------------
# Disk-based config readers (for creation-time decisions)
# ---------------------------------------------------------------------------


def _resolve_config_section_from_disk(
    section: str,
    config_name: str,
    config_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Lightweight YAML reader for a config section at creation/completion time.

    Reads just the ``section`` key from:

    1. the worker role base (``config/expert_base.yaml`` + the worker overlay —
       ``load_role_base("worker")``; the sections read here are worker-only,
       so a raw read of either file alone would miss them)
    2. ``config/experts/{config_name}/config.yaml`` or
       ``config/{config_name}.yaml`` (expert override)
    3. ``config_override[section]`` (per-job override)

    The expert leaf is still read raw — no full resolution, no matrix.
    """
    result: dict[str, Any] = {}

    # 1. Read defaults
    try:
        from shared.runtime.core.loader import load_role_base

        defaults = load_role_base("worker")
        if isinstance(defaults.get(section), dict):
            result.update(defaults[section])
    except Exception as e:
        logger.warning("Failed to read the worker base for %s config: %s", section, e)

    # 2. Read expert config (overrides defaults)
    expert_paths = [
        _REPO_ROOT / "config" / "experts" / config_name / "config.yaml",
        _REPO_ROOT / "config" / f"{config_name}.yaml",
    ]
    for path in expert_paths:
        if path.exists():
            try:
                from shared.runtime.core.srw_manifest_config import read_srw_config

                expert = read_srw_config(path)
                if isinstance(expert.get(section), dict):
                    result.update(expert[section])
            except Exception as e:
                logger.warning("Failed to read %s for %s config: %s", path, section, e)
            break

    # 3. Apply per-job config_override
    if config_override and isinstance(config_override.get(section), dict):
        result.update(config_override[section])

    return result


def resolve_scholar_config_from_disk(
    config_name: str,
    config_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Lightweight YAML reader for scholar config at job creation time."""
    return _resolve_config_section_from_disk("scholar", config_name, config_override)


def get_autonomy_level(job: dict[str, Any]) -> str:
    """Read the autonomy level from resolved_config (default: 'review')."""
    rc = _parse_resolved_config(job)
    agent_block = rc.get("agent")
    if isinstance(agent_block, dict):
        return agent_block.get("autonomy", "review")
    return rc.get("autonomy", "review")


# ---------------------------------------------------------------------------
# Freeze data helpers
# ---------------------------------------------------------------------------


def _parse_freeze_data(job: dict[str, Any]) -> dict[str, Any] | None:
    """Parse freeze_data from a job dict (handles str or dict)."""
    fd = job.get("freeze_data")
    if fd is None:
        return None
    if isinstance(fd, str):
        try:
            return json.loads(fd)
        except (json.JSONDecodeError, ValueError):
            return None
    return fd if isinstance(fd, dict) else None


def is_job_completion_freeze(job: dict[str, Any]) -> bool:
    """Check if a job's freeze_data indicates job completion (not phase boundary).

    Two formats exist depending on autonomy level:
      - review/partial: ``freeze_type="job_complete"``
      - full: ``status="job_completed"`` (no freeze_type field)
    """
    freeze_data = _parse_freeze_data(job)
    if not freeze_data:
        return False
    freeze_type = freeze_data.get("freeze_type")
    return freeze_type == "job_complete" or freeze_data.get("status") == "job_completed"


# Transient-infrastructure retry policy (Defect 1). Mirrors the shape of the
# LLM-outage backoff: a fixed escalating ladder, capped, with a hard attempt
# ceiling after which the job fails terminally with the infra cause named.
#
# The ceiling is what makes a MISCLASSIFICATION safe: if a genuinely permanent
# error is ever matched by is_transient_infra_error, it costs five paused
# retries over ~1.8h and then fails — it cannot retry forever.
INFRA_TRANSIENT_MAX_ATTEMPTS = 5
_INFRA_TRANSIENT_BACKOFF_SECONDS = (60, 300, 900, 1800, 3600)


def infra_transient_backoff_seconds(attempt: int) -> float:
    """Seconds to wait before re-dispatching after transient-infra attempt ``n``.

    ``attempt`` is 1-based (the first pause waits 60s). Past the ladder the cap
    (1h) repeats, which also bounds how long the reaper carve-out can pin a VM.
    """
    index = max(1, int(attempt)) - 1
    if index >= len(_INFRA_TRANSIENT_BACKOFF_SECONDS):
        return float(_INFRA_TRANSIENT_BACKOFF_SECONDS[-1])
    return float(_INFRA_TRANSIENT_BACKOFF_SECONDS[index])


def is_late_completion_report(job: dict[str, Any], result: dict[str, Any]) -> bool:
    """True when a report on an already-``failed`` job says the job finished.

    The ``/complete`` gate rejects any report on a terminal job *before*
    inspecting it, which strands two real cases:

      * a job that genuinely completed, whose ``job_complete`` freeze arrives
        after something failed it out-of-band — it stays ``failed`` forever, and
        ``determine_job_status``' carve-outs never run because they sit
        downstream of the gate (see
        knowledge-history/done/coincident_infra_error_overrides_reported_job_outcome.md);
      * a recoverable ``workspace_unavailable`` report, whose recovery arm is
        47 lines further down and therefore unreachable.

    This predicate authorises ONLY the first: a completion freeze re-resolves
    the job. It is deliberately narrow.

      * ``failed`` only. ``cancelled`` is explicit human intent and is never
        overridden by a late machine report.
      * The freeze must come from the REQUEST BODY. A failed job's DB
        ``freeze_data`` is NULL (the freeze write is what did not happen), so
        reading the row would always say no.
      * Re-resolve, never re-open: this returns True only for a completion
        freeze, so a recoverable error report cannot use it to re-dispatch a
        job that already ran — that is the hazard that made us reconstruct job
        c6dd288d by hand rather than resume it.

    knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md (Defect 2)
    """
    if job.get("status") != "failed":
        return False
    freeze = result.get("freeze_data")
    if isinstance(freeze, str):
        try:
            freeze = json.loads(freeze)
        except (ValueError, TypeError):
            return False
    if not isinstance(freeze, dict):
        return False
    return (
        freeze.get("freeze_type") == "job_complete"
        or freeze.get("status") == "job_completed"
    )


# ---------------------------------------------------------------------------
# Status determination
# ---------------------------------------------------------------------------


# Bounded re-dispatch cap for memory/KB-unavailable pauses. After this many
# pause+retry cycles a memory-required job is failed instead of looping forever
# (knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md).
MEMORY_RETRY_CAP = 2


# Parent statuses that PERMANENTLY block a subjob's re-dispatch. The dispatcher's
# cascade guard (get_dispatchable_jobs, postgres.py) also blocks on a 'paused'
# ancestor, but that is temporary — the subjob re-dispatches once the parent
# resumes — so only the permanent terminals wedge a paused subjob forever. A
# drain-frozen subjob under such a parent must resolve terminally, not pause.
# knowledge-history/done/coincident_infra_error_overrides_reported_job_outcome.md
_PARENT_TERMINAL_BLOCKING: frozenset[str] = frozenset({"failed", "cancelled"})

# Freeze types whose subjob short-circuit routes to the shared re-dispatch
# handling instead of the pending_review fallback: the drain freeze pauses
# directly, and the outage freezes (memory/kb/llm) fall through to their
# type-specific branches — whose retry caps and duration ceilings are
# row-scoped (context.memory_retry_count / context.llm_outage on the subjob's
# own row), so they apply per-subjob unchanged. All are guarded by
# _PARENT_TERMINAL_BLOCKING first: a paused subjob under a permanently-dead
# parent is a silent cascade-guard wedge.
# knowledge-base/knowledge/features/llm_outage_subjob_resilience.md. Membership is centralized in
# src.shared.job_freeze_types so independently deployed consumers cannot drift.

# Substrings that mark an error as a workspace/VM *teardown* (connectivity) blip
# rather than a genuine mid-run failure. On completion the VM is reaped, and a
# trailing SSH/SFTP/stat op can time out against the gone workspace; that trailing
# error must not override an outcome the agent already reported as complete.
# Kept deliberately narrow (connectivity/teardown only) — widen on evidence, not
# on suspicion (the "cleanup hiccup vs real failure" line, signed off 2026-07-12).
# knowledge-history/done/coincident_infra_error_overrides_reported_job_outcome.md
_TEARDOWN_ERROR_PATTERNS: tuple[str, ...] = (
    "failed to connect to workspace",
    "workspace i/o timed out",
    "workspace is unavailable",
    "key-exchange timed out",
    "waiting for key negotiation",
    "[gone]",
)


def is_teardown_infra_error(message: str | None) -> bool:
    """True if ``message`` looks like a workspace/VM teardown-connectivity blip.

    Used to tell a "finished the work, then a teardown hiccup" completion report
    apart from a genuine mid-run crash: only the former may keep its reported
    (successful) outcome when a coincident error rides the same report.
    """
    if not message:
        return False
    low = message.lower()
    return any(pattern in low for pattern in _TEARDOWN_ERROR_PATTERNS)


async def probe_workspace_ssh(host: str, port: int, timeout: float = 3.0) -> bool:
    """TCP-probe the workspace sshd; True means something accepted the connect.

    Used by the recovery arm to distinguish "pod is gone" from "agent
    misclassified a live pod" (e.g. an sshd MaxSessions refusal) before doing
    anything destructive. Any failure — refused, unroutable, timeout — is
    False; a False probe degrades safely to the old delete-and-reprovision
    path. knowledge-base/knowledge/issues/maxsessions_parallel_tools_false_workspace_death.md
    """
    import asyncio

    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout
        )
    except Exception:
        return False
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
    return True


def should_reset_recovery_counter(container_ctx: dict[str, Any], error: Any) -> bool:
    """True when a handled completion should zero ``recovery_attempts``.

    The counter only ever incremented before, so a single recovered blip
    stayed on the job forever and made a later, unrelated blip fail faster.
    Any completion the agent managed to report that is NOT another
    ``workspace_unavailable`` proves the workspace connection works — reset.
    """
    if int(container_ctx.get("recovery_attempts") or 0) <= 0:
        return False
    if isinstance(error, dict) and error.get("type") == "workspace_unavailable":
        return False
    return True


def should_persist_completion_freeze(result: dict[str, Any]) -> bool:
    """False when a completion report's freeze_data must NOT be persisted.

    An agent that dies before its graph runs (``workspace_unavailable``) can
    only ECHO the job's previous freeze back in its completion payload — it
    froze nothing new. Persisting that echo before the recovery arm's pause
    re-wedges the job: paused + freeze_data set is invisible to
    ``get_dispatchable_jobs`` (partial-index contract, migration 0046).
    Deliberately narrow — an errored completion with a genuinely new freeze
    (e.g. an llm_outage backoff freeze) must keep persisting.
    knowledge-base/knowledge/issues/recovery_pause_repersists_stale_freeze_invisible_job.md
    """
    error = result.get("error")
    return not (
        isinstance(error, dict) and error.get("type") == "workspace_unavailable"
    )


async def handle_pod_workspace_recovery(
    job: dict[str, Any],
    job_id: str,
    error: dict[str, Any],
    *,
    db: Any,
    delete_workspace: Callable[[str], Any],
    trigger_dispatch: Callable[[], None],
    probe: Callable[..., Any] = probe_workspace_ssh,
    completion_command_id: str | None = None,
    completion_finalizing_by: str | None = None,
    expected_status: str | None = None,
) -> dict[str, Any]:
    """G1 pod (sandbox/PVC) workspace recovery — the ``workspace_unavailable``
    arm of the completion endpoint, extracted for testability.

    Re-dispatch through the POD arm: ensure_workspace → _create →
    create_workspace 409-reuses the deterministic PVC (= reattach), and the
    agent resumes from the Postgres checkpoint on the intact files. At the
    retry cap, retain the workspace and checkpoints under an explicit Resume
    hold. Exhaustion alone never authorizes deleting user work.

    Probe-before-punch: a TCP probe of the workspace sshd guards the delete.
    A live pod is kept warm (its context stays ``ready`` so the re-dispatch
    adopts it); only a dead probe tears the pod down. The attempts counter
    increments either way so a pathological report-loop stays bounded.
    See knowledge-base/knowledge/features/workspace_pvc_branch_a_implementation.md (G1) and
    knowledge-base/knowledge/issues/maxsessions_parallel_tools_false_workspace_death.md (D).

    ``expected_status`` fences legacy pinned completion against cancellation
    after its entry read. A lost disposition returns ``paused=False`` so the
    caller stops before reporting recovery or continuing completion effects.
    """
    container_ctx = _get_ctx(job)

    async def _delete_for_recovery() -> None:
        if completion_command_id is not None and not container_ctx.get(
            "_runtime_incarnation"
        ):
            # A replay after deleting a name-only Pod could otherwise observe
            # and delete a same-name replacement. Durable Kubernetes cleanup
            # requires the immutable UID captured in workspace context.
            raise RuntimeError(
                "durable workspace recovery is missing the captured Pod UID"
            )
        deleted = await delete_workspace(job_id)
        if completion_command_id is not None and deleted is not True:
            # The Kubernetes provisioner reports probe/precondition/delete
            # failures as ``False``.  Treat that as an incomplete external
            # effect when a durable owner exists; legacy callers keep their
            # historical best-effort contract.
            raise RuntimeError("durable workspace recovery delete did not complete")

    if completion_command_id is not None and (
        container_ctx.get("recovery_completion_command_id") == completion_command_id
    ):
        stored_outcome = container_ctx.get("recovery_completion_outcome")
        if (
            isinstance(stored_outcome, dict)
            and stored_outcome.get("held_for_resume") is True
        ):
            # Re-read the committed receipt. Replay has no probe, dispatch or
            # cleanup effect, including after an explicit Resume or Cancel.
            replay = await db.hold_exhausted_workspace_recovery(
                job_id,
                expected_workspace=container_ctx,
                expected_agent_id=job.get("assigned_agent_id"),
                recovery_cap=int(
                    os.environ.get("WORKSPACE_RECOVERY_MAX_ATTEMPTS", "3")
                ),
                error_detail=error.get("message") or "workspace_unavailable",
                completion_command_id=completion_command_id,
                completion_finalizing_by=completion_finalizing_by,
            )
            if replay is None:
                raise RuntimeError("workspace recovery lost its committed hold outcome")
            return replay
        if isinstance(stored_outcome, dict):
            # The processing→paused/failed disposition and this exact-command
            # marker are one jobs-row UPDATE.  A crash before the generic
            # effect marker therefore reconciles here without consuming the
            # recovery counter or repeating the probe.  Re-kick dispatch (or
            # the idempotent failed cleanup) because either may have been the
            # instruction immediately following that durable UPDATE.
            if stored_outcome.get("new_status") == "failed":
                try:
                    await _delete_for_recovery()
                except Exception:
                    logger.exception(
                        "Job %s: failed to reconcile exhausted workspace cleanup",
                        job_id,
                    )
                    # The exact-command jobs-row marker proves that the failed
                    # disposition committed, but it does not prove the
                    # external delete did.  Keep the durable effect pending so
                    # a later owner retries the UID-fenced cleanup.
                    raise
            else:
                if container_ctx.get("recovery_delete_pending") is True:
                    await _delete_for_recovery()
                    cleared = await db.merge_workspace_container_context(
                        job_id,
                        {"recovery_delete_pending": False},
                        completion_command_id=completion_command_id,
                        completion_finalizing_by=completion_finalizing_by,
                    )
                    if not cleared:
                        raise RuntimeError(
                            "workspace recovery lost its delete-reconcile term"
                        )
                trigger_dispatch()
            return dict(stored_outcome)

    same_command_attempt = completion_command_id is not None and (
        container_ctx.get("recovery_attempt_command_id") == completion_command_id
    )
    attempts = int(container_ctx.get("recovery_attempts") or 0) + (
        0 if same_command_attempt else 1
    )
    cap = int(os.environ.get("WORKSPACE_RECOVERY_MAX_ATTEMPTS", "3"))
    if attempts > cap:
        outcome = await db.hold_exhausted_workspace_recovery(
            job_id,
            expected_workspace=container_ctx,
            expected_agent_id=job.get("assigned_agent_id"),
            recovery_cap=cap,
            error_detail=error.get("message") or "workspace_unavailable",
            completion_command_id=completion_command_id,
            completion_finalizing_by=completion_finalizing_by,
        )
        if outcome is None:
            if completion_command_id is not None:
                raise RuntimeError(
                    "workspace recovery lost its finalizer disposition term"
                )
            return {
                "status": "handled",
                "job_id": job_id,
                "new_status": "paused",
                "actions": [],
                "paused": False,
            }
        return outcome

    host = container_ctx.get("host")
    port = int(container_ctx.get("port") or 30022)
    pod_alive = bool(host) and await probe(host, port)

    if completion_command_id is not None or expected_status is not None:
        if (
            completion_command_id is not None
            and not pod_alive
            and not container_ctx.get("_runtime_incarnation")
        ):
            raise RuntimeError(
                "durable workspace recovery is missing the captured Pod UID"
            )
        if pod_alive:
            logger.warning(
                f"Job {job_id}: workspace reported unavailable but sshd probe on "
                f"{host}:{port} succeeded — keeping pod, re-dispatch "
                f"(attempt {attempts}/{cap})"
            )
            container_updates = {
                "recovery_attempts": attempts,
                "previous_error": error.get("message") or "workspace_unavailable",
            }
            action = (
                f"workspace recovery: pod alive on probe — kept, re-dispatch "
                f"(attempt {attempts}/{cap})"
            )
        else:
            logger.warning(
                f"Job {job_id}: workspace unavailable — pod recovery "
                f"attempt {attempts}/{cap} (PVC reattach)"
            )
            container_updates = {
                "status": "deleted",
                "pod_ip": None,
                "recovery_attempts": attempts,
                "previous_error": error.get("message") or "workspace_unavailable",
            }
            action = (
                f"workspace recovery: dead pod deleted (PVC kept), "
                f"re-dispatch for reattach (attempt {attempts}/{cap})"
            )

        outcome = {
            "status": "handled",
            "job_id": job_id,
            "new_status": "paused",
            "actions": [action],
            "paused": True,
        }
        pause_kwargs: dict[str, Any] = {}
        if completion_command_id is not None:
            container_updates.update(
                {
                    "recovery_attempt_command_id": completion_command_id,
                    "recovery_delete_pending": not pod_alive,
                    "recovery_completion_command_id": completion_command_id,
                    "recovery_completion_outcome": outcome,
                }
            )
            pause_kwargs = {
                "completion_command_id": completion_command_id,
                "completion_finalizing_by": completion_finalizing_by,
            }
        # The same processing->paused CAS must win before either mode can
        # invalidate runtime context or retire the captured pod. The legacy
        # completion has no durable command and must not write its markers.
        paused = await db.pause_job_shed_freeze(
            job_id,
            workspace_context_updates=container_updates,
            **pause_kwargs,
        )
        if not paused:
            outcome["paused"] = False
            return outcome

        if not pod_alive:
            try:
                await _delete_for_recovery()
            except Exception:
                logger.exception("Job %s: error deleting dead workspace pod", job_id)
                if completion_command_id is not None:
                    raise
            if completion_command_id is not None:
                cleared = await db.merge_workspace_container_context(
                    job_id,
                    {"recovery_delete_pending": False},
                    completion_command_id=completion_command_id,
                    completion_finalizing_by=completion_finalizing_by,
                )
                if not cleared:
                    raise RuntimeError(
                        "workspace recovery lost its delete-complete term"
                    )
        trigger_dispatch()
        if completion_command_id is None:
            outcome.pop("paused")
        return outcome

    if pod_alive:
        # The workspace answers — the report was a misclassification or a
        # transient. Keep the warm pod (context stays ready → the re-dispatch
        # adopts it); only record the attempt + discriminating cause.
        logger.warning(
            f"Job {job_id}: workspace reported unavailable but sshd probe on "
            f"{host}:{port} succeeded — keeping pod, re-dispatch "
            f"(attempt {attempts}/{cap})"
        )
        container_updates = {
            "recovery_attempts": attempts,
            "previous_error": error.get("message") or "workspace_unavailable",
        }
        if completion_command_id is not None:
            container_updates["recovery_attempt_command_id"] = completion_command_id
        merge_kwargs = (
            {
                "completion_command_id": completion_command_id,
                "completion_finalizing_by": completion_finalizing_by,
            }
            if completion_command_id is not None
            else {}
        )
        merged = await db.merge_workspace_container_context(
            job_id, container_updates, **merge_kwargs
        )
        if completion_command_id is not None and not merged:
            raise RuntimeError("workspace recovery lost its counter-update term")
        action = (
            f"workspace recovery: pod alive on probe — kept, re-dispatch "
            f"(attempt {attempts}/{cap})"
        )
    else:
        logger.warning(
            f"Job {job_id}: workspace unavailable — pod recovery "
            f"attempt {attempts}/{cap} (PVC reattach)"
        )
        # Invalidate the stale container so the re-dispatch RECREATES it
        # (and reattaches the PVC) instead of reusing the dead pod:
        #   * status="deleted" — `_job_needs_sandbox` returns False while
        #     status=="ready", short-circuiting before ensure_workspace's
        #     drift probe; and delete_workspace's 404/"already deleted"
        #     branch does NOT set the status, so we must set it here.
        #   * pod_ip=None — drop the dead IP the resume path would dial.
        container_updates = {
            "status": "deleted",
            "pod_ip": None,
            "recovery_attempts": attempts,
            "previous_error": error.get("message") or "workspace_unavailable",
        }
        if completion_command_id is not None:
            container_updates["recovery_attempt_command_id"] = completion_command_id
        merge_kwargs = (
            {
                "completion_command_id": completion_command_id,
                "completion_finalizing_by": completion_finalizing_by,
            }
            if completion_command_id is not None
            else {}
        )
        merged = await db.merge_workspace_container_context(
            job_id, container_updates, **merge_kwargs
        )
        if completion_command_id is not None and not merged:
            raise RuntimeError("workspace recovery lost its counter-update term")
        # Delete the dead pod so create_workspace does not ADOPT the Failed
        # tombstone (restartPolicy:Never → not "terminating"). The PVC is
        # retained (delete_workspace never touches it) and reattaches by
        # name on recreate.
        try:
            await _delete_for_recovery()
        except Exception:
            logger.exception(
                f"Job {job_id}: error deleting dead workspace pod "
                f"(continuing to re-dispatch)"
            )
            if completion_command_id is not None:
                # Re-dispatching before the exact pod is gone can make the
                # deterministic workspace name adopt a failed tombstone.  A
                # durable command must retry the fenced delete instead.
                raise
        action = (
            f"workspace recovery: dead pod deleted (PVC kept), "
            f"re-dispatch for reattach (attempt {attempts}/{cap})"
        )

    # The pause clears the agent + flips to paused (→ resume=True on
    # re-dispatch) AND sheds any row-level freeze into
    # context.last_freeze_data: paused + freeze_data set is invisible to
    # get_dispatchable_jobs, so a freeze surviving this transition would park
    # the job forever
    # (knowledge-base/knowledge/issues/recovery_pause_repersists_stale_freeze_invisible_job.md).
    # Gate the dispatch on the processing→paused transition so a duplicate
    # completion can't double-dispatch.
    outcome: dict[str, Any] = {
        "status": "handled",
        "job_id": job_id,
        "new_status": "paused",
        "actions": [action],
    }
    if completion_command_id is not None:
        # Internal replay discriminator only.  Keeping it off the legacy arm
        # preserves the exact pre-flag response contract.
        outcome["paused"] = True
    pause_kwargs: dict[str, Any] = {}
    if completion_command_id is not None or completion_finalizing_by is not None:
        pause_kwargs = {
            "completion_command_id": completion_command_id,
            "completion_finalizing_by": completion_finalizing_by,
            "workspace_context_updates": {
                "recovery_completion_command_id": completion_command_id,
                "recovery_completion_outcome": outcome,
            },
        }
    paused = await db.pause_job_shed_freeze(job_id, **pause_kwargs)
    if paused:
        trigger_dispatch()
    if completion_command_id is not None and not paused:
        outcome["paused"] = False
    return outcome


async def apply_deliverable_gate(
    job: dict[str, Any],
    result: dict[str, Any],
    new_status: str | None,
    *,
    db: Any,
    gitea: Any,
    queue_resume: Callable[..., Any],
    vector_db: Any = None,
) -> Any:
    """P1-C deliverable-contract arm of the completion decision.

    Sits between ``determine_job_status`` and the status write in the
    /complete handler: a completion that CLAIMS done-ness while
    ``context.required_deliverables`` artifacts are absent at the job branch
    HEAD is bounced back through the P1-A resume-with-feedback lane instead
    of sealing (bounded by the module's bounce cap; Gitea-down fails open).

    Thin hook by design — all gate logic lives in
    ``services.deliverable_gate`` (same extraction pattern as
    ``handle_pod_workspace_recovery`` above). Returns
    A backward-compatible three-value iterable plus ``outcome_kind``; on
    ``bounced=True`` the caller must early-return without sealing or spawning
    critic/curator subjobs.
    """
    from orchestrator.services.deliverable_gate import run_deliverable_gate

    return await run_deliverable_gate(
        job,
        result,
        new_status,
        db=db,
        gitea=gitea,
        queue_resume=queue_resume,
        vector_db=vector_db,
    )


async def write_job_change_record(
    job: dict[str, Any],
    new_status: str,
    *,
    db: Any,
    vector_db: Any = None,
    error: str | None = None,
    merge_status: str | None = None,
    merged_sha: str | None = None,
    outcome_kind: str | None = None,
) -> bool:
    """General per-job change record — the completion-path hook (§6.5).

    Every job leaves exactly one structured database record on reaching a
    terminal disposition, including blocked/undelivered work. The latter is
    written under its presentation outcome rather than as cancellation or a
    worker failure.

    Loop jobs are skipped here: the loop advance records them itself
    (``_record_loop_job_outcome`` → ``write_loop_retro``) with the cloud
    delivery outcome — detected via the same loop-context stamp the
    loop service keys off (``context.loop_id``, ``job_loop_id``). The
    ``job_change_records.job_id`` primary key makes retries idempotent.

    ``merge_status`` / ``merged_sha`` carry a legacy merge or current delivery
    outcome; omitted, the writer falls back to whatever the job row carries.
    ``outcome_kind`` is server-owned and selects the truthful record status.

    Thin hook by design — ``changes`` derivation and the insert live in
    ``services.job_records`` (same extraction
    pattern as ``apply_deliverable_gate`` above). Best-effort: the writer
    logs and swallows its own failures; a record must never block
    completion handling.
    """
    from orchestrator.services.job_records import write_job_record
    from orchestrator.services.project_loops import job_loop_id

    if job_loop_id(job):
        return False
    record_status = (
        "blocked_undelivered" if outcome_kind == "blocked_undelivered" else new_status
    )
    return await write_job_record(
        db,
        job,
        status=record_status,
        error=error,
        vector_db=vector_db,
        merge_status=merge_status,
        merged_sha=merged_sha,
    )


# ---------------------------------------------------------------------------
# Terminal-transition side effects (§6.6)
# knowledge-base/knowledge/features/workspace_and_change_records.md
# ---------------------------------------------------------------------------

# Delivery outcomes that mean a second terminal callback must not attempt the
# legacy project-repo merge compatibility path.
ALREADY_MERGED_STATUSES = (
    "merged",
    "curated",
    "cloud-applied",
    "cloud-rejected",
    "no-changes",
)


def job_has_file_contract(job: dict[str, Any]) -> bool:
    """True when ``context.required_deliverables`` names at least one FILE.

    Deliberately THE predicate ``merge_loop_job_contribution`` dispatches on
    (imported, not re-derived): the two must never disagree, because a job
    the merge considers contract-less takes its FULL squash-merge path —
    which for an ad-hoc job would land the entire scratchpad (``plan.md``,
    ``todos.yaml``, phase archives, probe files) on ``main``, the exact
    accumulation §6.4 exists to prevent. ``kb:`` entries are store-backed
    and never count.
    """
    from orchestrator.services.project_loops import contracted_file_deliverables

    return bool(contracted_file_deliverables(job))


def unmerged_pr_seal_status(
    new_status: str, *, loop_id: Any, reason: str | None
) -> tuple[str, str | None]:
    """Pure gate: should a self-sealing completion be routed to human review?

    Returns ``(status, action)``, where ``action`` is ``None`` when nothing
    changed. The I/O that produces ``reason`` lives in
    ``main._unmerged_pr_gate_reason``; this is only the decision, mirroring the
    cloud-diff downgrade it sits beside.

    Exclusions, in order:

    * anything but a successful ``completed`` — a job already heading for review
      or failure is not sealing itself, so there is nothing to downgrade;
    * no reason — the pull request is merged, absent, or the caller holds
      ``complete_unmerged_pr``;
    * **loop jobs** — the loop owns its own delivery and advance, and a loop
      parked in ``pending_review`` produces nothing while nobody is watching to
      unstick it. The cloud-diff downgrade excludes them for the same reason
      (``not _completion_loop_id``).

    Spec: knowledge-base/knowledge/features/merged_pr_completion_grant.md §5b.
    """
    if new_status != "completed" or reason is None:
        return new_status, None
    if loop_id:
        return new_status, None
    return "pending_review", f"unmerged pull request -> pending_review ({reason})"


def should_merge_job_contribution(
    job: dict[str, Any], new_status: str
) -> tuple[bool, str]:
    """Pure gate: may this terminal transition land the job's branch on ``main``?

    Returns ``(should_merge, reason)``; the reason is logged and asserted on
    in tests. The exclusions, in order (§6.6 "Invariants to preserve"):

    * anything but a successful ``completed`` — a failed job's partial
      deliverables must not reach a destination;
    * **subjobs** — they branch ``subjob/<id>/<role>`` from the PARENT and
      land through the delegation graft, never into project ``main``;
    * **loop jobs** — their isolated diff uses project-cloud delivery and the
      loop advance owns their structured record;
    * a job whose row already carries a terminal Git or cloud-delivery outcome
      — the double-delivery backstop for a second terminal call;
    * no project, no project branch, or a per-job ``job-<id>`` repo (the
      agent worked directly on that repo's ``main``: nothing to merge);
    * **no file contract** — §6.6 policy: record only, the branch stays.
    """
    from orchestrator.services.project_loops import job_loop_id

    if new_status != "completed":
        return False, f"status {new_status!r} is not a successful completion"
    if job.get("parent_job_id"):
        return False, "subjob (the delegation graft owns its merge)"
    if job_loop_id(job):
        return False, "loop job (the loop advance owns its merge)"
    row_merge_status = str(job.get("merge_status") or "")
    if row_merge_status in ALREADY_MERGED_STATUSES:
        return False, f"already {row_merge_status} (double-merge backstop)"
    if not job.get("project_id"):
        return False, "no project (nothing to merge into)"
    repo_name = job.get("repo_name")
    branch = job.get("branch_name")
    if not repo_name or not branch or branch == "main":
        return False, "no project branch to merge"
    if repo_name == f"job-{str(job.get('id'))[:8]}":
        return False, "per-job repo (the agent worked on its own main)"
    if not job_has_file_contract(job):
        return False, "no file contract (§6.6: record only, branch stays)"
    return True, "file contract"


async def apply_terminal_job_side_effects(
    job: dict[str, Any],
    new_status: str,
    *,
    gitea: Any,
    db: Any = None,
    vector_db: Any = None,
    error: str | None = None,
    load_merge_intent: Callable[[], Awaitable[dict[str, Any] | None]] | None = None,
    store_merge_intent: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
    | None = None,
    completion_command_id: str | None = None,
    outcome_kind: str | None = None,
) -> dict[str, Any]:
    """Run terminal compatibility delivery and persist structured history.

    ONE function behind BOTH terminal paths — the ``/complete`` handler and
    ``approve_job`` — so an approved job and an autonomy-``full`` job that
    sealed itself leave identical traces. Hooking the *transition* rather
    than "approval" is deliberate: ``review`` autonomy makes approval the
    transition (the human gate stays where it was asked for), while ``full``
    autonomy transitions unattended and the merge rides along.

    Two side effects, in order:

    1. **Legacy merge compatibility**, only for a pre-migration job already
       attached to a shared project repo. New roots are isolated and always
       fail this gate; loop files use project-cloud delivery earlier in the
       completion path.
    2. **Change record** via :func:`write_job_change_record`, carrying the
       delivery outcome this call observed. Loop jobs are skipped inside the
       writer because their advance owns the record.

    Best-effort by contract, like the record hook it replaces: every failure
    is logged and swallowed, so a Gitea outage can never fail an approval or
    a legacy completion.  Durable completion callers may supply both intent
    callbacks; that arm persists the exact PR number before merge and raises
    ambiguous/transient merge failures so its effect group can retry safely.
    Returns the observable outcome —
    ``{merge_status, merged_sha, merge_notes, merge_skipped_reason,
    record_written, actions}`` — which the callers surface in their action
    log and the tests compare across the two paths.
    """
    outcome: dict[str, Any] = {
        "merge_status": None,
        "merged_sha": None,
        "merge_notes": [],
        "merge_skipped_reason": None,
        "record_written": False,
        "actions": [],
    }
    blocked = new_status == "cancelled" and outcome_kind == "blocked_undelivered"
    if new_status not in ("completed", "failed") and not blocked:
        return outcome

    job_id = str(job.get("id"))
    should_merge, reason = should_merge_job_contribution(job, new_status)
    if not should_merge:
        # Logged at info, not debug: "why is my work still on the branch?" is
        # the first question the §6.6 no-contract policy provokes.
        outcome["merge_skipped_reason"] = reason
        logger.info("Job %s: no terminal merge — %s", job_id[:8], reason)
    else:
        # A raise is the same outcome as a returned merge-failed — the legacy
        # branch kept everything either way.
        status, sha, notes = "merge-failed", None, []
        try:
            from orchestrator.services.project_loops import merge_loop_job_contribution

            status, sha, notes = await merge_loop_job_contribution(
                gitea,
                job,
                load_merge_intent=load_merge_intent,
                store_merge_intent=store_merge_intent,
                completion_command_id=completion_command_id,
            )
            logger.info(
                "Job %s: terminal merge of %s -> %s (%s)",
                job_id[:8],
                job.get("branch_name"),
                status,
                (sha or "")[:8] or "-",
            )
        except Exception:
            if (
                load_merge_intent is not None
                or store_merge_intent is not None
                or completion_command_id is not None
            ):
                raise
            logger.warning(
                "Job %s: terminal merge raised (non-fatal)", job_id, exc_info=True
            )
        outcome["merge_status"] = status
        outcome["merged_sha"] = sha
        outcome["merge_notes"] = list(notes or [])
        outcome["actions"].append(f"branch merge -> {status}")
        # Stamp the outcome on the row AND on the caller's dict: the row is
        # what the double-merge backstop reads on a LATER call, the dict is
        # what it reads within this one.
        if status != "skipped":
            job["merge_status"] = status
            if db is not None:
                try:
                    await db.update_job_merge_status(job_id, merge_status=status)
                except Exception:
                    logger.warning(
                        "Job %s: recording merge_status=%s failed (non-fatal)",
                        job_id[:8],
                        status,
                        exc_info=True,
                    )

    try:
        if await write_job_change_record(
            job,
            new_status,
            db=db,
            vector_db=vector_db,
            error=error,
            merge_status=outcome["merge_status"],
            merged_sha=outcome["merged_sha"],
            outcome_kind=outcome_kind,
        ):
            outcome["record_written"] = True
            outcome["actions"].append("job change record written to database")
    except Exception:
        logger.warning(
            "Job %s: change-record write failed (non-fatal)", job_id, exc_info=True
        )
    return outcome


def _get_ctx(job: dict[str, Any]) -> dict[str, Any]:
    """workspace_container sub-dict from a job row's context JSONB."""
    ctx = job.get("context") or {}
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, TypeError):
            ctx = {}
    if not isinstance(ctx, dict):
        return {}
    return ctx.get("workspace_container", {}) or {}


def classify_workspace_death(
    termination: dict[str, Any] | None,
) -> tuple[str, bool]:
    """Human cause + is-resource-kill flag from a workspace pod's terminated
    state (the dict from ``ContainerProvisioner.get_last_termination``).

    The bool is the escalation signal: a resource kill (OOM / node-pressure
    eviction) means "retry bigger", not "re-dispatch into the same grave". We
    only assert a resource kill on positive evidence — a pod already gone before
    we could read it stays ``False`` (no evidence), so we never fabricate an OOM.
    See knowledge-base/knowledge/features/workspace_resource_pressure_resilience.md.
    """
    if not termination:
        return (
            "workspace pod gone before its termination cause could be read",
            False,
        )
    container_reason = termination.get("container_reason") or ""
    pod_reason = termination.get("pod_reason") or ""
    exit_code = termination.get("exit_code")
    if container_reason == "OOMKilled":
        return ("workspace ran out of memory (OOMKilled)", True)
    if pod_reason == "Evicted":
        return ("workspace evicted under node resource pressure", True)
    if exit_code == 137:
        # 137 = 128 + SIGKILL. A workspace pod has no liveness probe to SIGKILL
        # it, so a bare 137 without an OOMKilled reason is still almost always
        # memory pressure (the kernel can drop the reason when the node is under
        # duress). Flag as a likely resource kill — hedged wording, not a claim.
        return (
            "workspace process killed (SIGKILL/137) — likely memory pressure",
            True,
        )
    if container_reason:
        detail = f"workspace container terminated: {container_reason}"
        if exit_code is not None:
            detail += f" (exit {exit_code})"
        return (detail, False)
    return ("workspace became unavailable (no terminated-container record)", False)


def auto_continue_drain_update(
    context: dict[str, Any], freeze_data: dict[str, Any], *, cap: int
) -> tuple[int, Any, bool]:
    """Progress-aware drain counter for auto-continue re-dispatches (backstop).

    Defense-in-depth for the version_upgrade drain livelock
    (knowledge-base/knowledge/issues/version_upgrade_drain_livelock.md). The agent-side resume-clear
    (src/agent/agent.py) guarantees a re-dispatched auto-continue job re-enters the
    graph and advances a phase each cycle, so a subsequent re-freeze carries a
    NEW ``phase_number``. If the freeze ``phase_number`` STOPS changing across
    re-dispatches, that guarantee has broken and the job is spinning with no
    progress — which this counter detects so the caller can alert loudly.

    Pure decision logic (the caller performs the context write + the alert I/O),
    mirroring the ``dispatch_guards`` extraction pattern.

    Args:
        context: the job's ``context`` dict (reads prior counter + last phase).
        freeze_data: the freeze blob being processed (reads ``phase_number``).
        cap: alert threshold — ``should_alert`` is True once drains reach it.

    Returns:
        ``(drains, last_phase, should_alert)`` — the new consecutive-no-progress
        count, the phase to remember, and whether to alert. A changed or absent
        ``phase_number`` resets the count to 0 (progress / no signal).
    """
    cur_phase = freeze_data.get("phase_number")
    last_phase = context.get("auto_continue_last_phase")
    drains = int(context.get("auto_continue_drains", 0) or 0)
    if cur_phase is not None and cur_phase == last_phase:
        drains += 1
    else:
        drains = 0
    return drains, cur_phase, drains >= cap


# ---------------------------------------------------------------------------
# LLM-outage pause + backoff re-dispatch
# (knowledge-base/knowledge/features/llm_outage_pause_and_backoff_redispatch.md)
#
# A transient LLM outage exhausts the worker's Tier-1 in-process retries and
# freezes the job (freeze_type=llm_unavailable). Rather than fail it, the
# orchestrator pauses it and an outage sweeper re-dispatches it on an
# exponential, Full-Jittered backoff so an overnight loop survives a
# multi-minute/hour provider outage and resumes from its checkpoint. Two
# ceilings guarantee a broken config can't park an iteration forever.
# ---------------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    """Read an int from env with a fallback (blank/garbage → default)."""
    try:
        return int((os.getenv(name) or "").strip() or default)
    except (ValueError, TypeError):
        return default


# 12h continuous-outage duration ceiling (primary; the Temporal
# scheduleToCloseTimeout model). After this the job fails loudly. Doubles as the
# cooldown pause-vs-fail-fast cutoff (knowledge-base/knowledge/features/llm_cooldown_pause_and_resume.md):
# a quota cooldown whose provider-stated reset exceeds this fails fast instead of
# pausing. Keep the 43_200 default in sync with src/graph.py's
# _COOLDOWN_MAX_PAUSE_SECONDS (a different pod reads the same env var).
LLM_OUTAGE_CEILING_SECONDS = _env_int("LLM_OUTAGE_CEILING_SECONDS", 43_200)
# Attempts backstop — defends against pathological fast re-fail loops whose
# short Full-Jitter draws could rack up attempts without much wall-clock.
LLM_OUTAGE_MAX_ATTEMPTS = _env_int("LLM_OUTAGE_MAX_ATTEMPTS", 60)
# Gap since the last failure that resets the attempt counter + duration ceiling
# (the job ran fine in between → treat the next failure as a brand-new outage).
# MUST exceed LLM_OUTAGE_BACKOFF_CAP_SECONDS: during a single sustained outage,
# consecutive failures are ~one backoff apart (up to the 60-min cap), so a
# window <= the cap would misread that idle backoff wait as "ran fine" and
# spuriously reset first_failed_at every long cycle — letting a 24h outage park
# a loop forever. 2h > 1h cap leaves headroom while still resetting on a genuine
# multi-hour productive stretch between unrelated outages.
LLM_OUTAGE_RESET_WINDOW_SECONDS = _env_int("LLM_OUTAGE_RESET_WINDOW_SECONDS", 7_200)
# Backoff envelope: min(cap, base * 2**(attempt-1)), coefficient 2.
LLM_OUTAGE_BACKOFF_BASE_SECONDS = _env_int("LLM_OUTAGE_BACKOFF_BASE_SECONDS", 30)
LLM_OUTAGE_BACKOFF_CAP_SECONDS = _env_int("LLM_OUTAGE_BACKOFF_CAP_SECONDS", 3_600)
# Jitter strategy: "full" (uniform[0, envelope], the thundering-herd default) or
# "equal" (envelope/2 + uniform[0, envelope/2], a >=50% floor).
LLM_OUTAGE_JITTER = (os.getenv("LLM_OUTAGE_JITTER") or "full").strip().lower() or "full"
# Consecutive pauses carrying a byte-identical (normalized) error before we stop
# auto-redispatching a NON-4xx failure. 4xx request rejections are deterministic
# on sight and give up after 2 (llm_outage_fingerprint below); a 5xx/transport
# error repeats identically during a GENUINE provider outage too, so it needs a
# higher bar. At the 60-min backoff cap 4 pauses is still ~2-3h of outage
# tolerance, versus the 12h duration ceiling spent achieving nothing.
# Incident: 2026-07-29 job d251e513 burned 13 attempts on an identical
# `503 auth_unavailable` that failed on attempt 1 of every single re-dispatch.
LLM_OUTAGE_REPEAT_CEILING = _env_int("LLM_OUTAGE_REPEAT_CEILING", 4)
# >>> TEMPORARY QUICKFIX (2026-07-30) — knowledge-history/done/codex_stream_disconnect_shape_nudge.md
# Spend one extra backoff cycle on a request-SHAPE change before the repeat
# give-up fires. Exists only because OpenAI's `stream disconnected before
# completion` (openai/codex#9995, still OPEN) is deterministic per payload: the
# identical request keeps failing, a slightly different one usually does not.
# DELETE this flag and its two call sites once that upstream bug is fixed —
# set LLM_OUTAGE_SHAPE_NUDGE=0 to disable without a deploy.
LLM_OUTAGE_SHAPE_NUDGE = (
    os.getenv("LLM_OUTAGE_SHAPE_NUDGE") or "1"
).strip().lower() not in ("0", "false", "no", "off")


def _parse_context(job: dict[str, Any]) -> dict[str, Any]:
    """Parse the job's ``context`` JSONB (handles str or dict)."""
    ctx = job.get("context")
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, ValueError):
            return {}
    return ctx if isinstance(ctx, dict) else {}


def _parse_ts(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp string into an aware UTC datetime, or None."""
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def llm_outage_fingerprint(fd: dict[str, Any]) -> str | None:
    """Fingerprint a deterministic request-rejection error, or None.

    Feeds the fail-fast in ``determine_job_status``: an ``llm_unavailable``
    pause whose normalized error text is IDENTICAL to the previous pause's is
    deterministic — retrying cannot fix the input (the 2026-07-11 `6a186c76`
    poisoned-tool-call incident burned 12 requests + would have burned the
    whole give-up ceiling; see knowledge-base/knowledge/features/outbound_message_hygiene.md).

    Deliberately narrow: only request-shaped 4xx rejections qualify. A genuine
    endpoint outage (timeouts, connection errors, 5xx) repeats an identical
    generic message the whole time the provider is down and MUST keep
    pausing/retrying — that is the outage feature's entire purpose. 429/rate
    texts are excluded too (they have their own handling upstream).

    Normalization strips long ids (request ids, tool_call ids, uuids) and all
    digits so two cycles of the same rejection hash identically.

    ``deterministic_exempt`` (set by the agent when the failing response was
    edge-shaped — a non-API body such as an nginx 404 page) opts the freeze
    out entirely: an identical edge page across pause cycles means the
    provider's gateway is still down, not that the request is deterministic,
    so the job must keep riding the outage ceilings like a 5xx outage. See
    knowledge-base/knowledge/issues/llm_infra_404_misclassified_permanent_kills_jobs.md.
    """
    if fd.get("deterministic_exempt"):
        return None
    src = str(fd.get("error_summary") or fd.get("reason") or "")
    if not src:
        return None
    low = src.lower()
    if "429" in low or "rate limit" in low or "too many requests" in low:
        return None
    if not re.search(r"\b4\d\d\b", low):
        return None
    return _normalize_llm_error(low)


def _normalize_llm_error(low: str) -> str:
    """Hash an error string with volatile ids/numbers stripped.

    ``{20,}`` not shorter: ids (uuids, request ids, tool_call ids) are 20+ chars
    while semantic tokens like ``bad_request_error`` (17) must survive so
    different error TYPES keep distinct fingerprints.
    """
    norm = re.sub(r"[a-z0-9_-]{20,}", "<tok>", low)
    norm = re.sub(r"\d+", "<n>", norm)
    return hashlib.sha256(norm[:500].encode("utf-8")).hexdigest()[:16]


def llm_outage_repeat_key(fd: dict[str, Any]) -> str | None:
    """Signature of ANY outage error, for the consecutive-repeat give-up.

    The sibling :func:`llm_outage_fingerprint` only claims request-shaped 4xx,
    because those are deterministic on sight and must die after two cycles. But
    a 5xx / transport error can ALSO be deterministic — the 2026-07-29
    ``d251e513`` incident failed on attempt 1 of thirteen consecutive
    re-dispatches with a byte-identical ``503 auth_unavailable``, each separated
    by a fully-idle backoff hour, and rode the ceiling for nothing.

    So this fingerprints everything the strict function skips, and the caller
    pairs it with the much higher :data:`LLM_OUTAGE_REPEAT_CEILING` so a genuine
    multi-hour provider outage still gets to ride the pause+backoff path — which
    is the entire point of the feature. Same exclusions as the strict function:
    ``deterministic_exempt`` (provider gateway still down) and rate-limit texts
    (they have their own cooldown handling) never count toward the streak.
    """
    if fd.get("deterministic_exempt"):
        return None
    src = str(fd.get("error_summary") or fd.get("reason") or "")
    if not src:
        return None
    low = src.lower()
    if "429" in low or "rate limit" in low or "too many requests" in low:
        return None
    return _normalize_llm_error(low)


def llm_outage_nudge_state(
    prior: dict[str, Any], repeats: int, nudge_at_repeats: int | None
) -> tuple[bool, bool]:
    """One-shot arming for the request-shape nudge → ``(pending, attempted)``.

    >>> TEMPORARY QUICKFIX — knowledge-history/done/codex_stream_disconnect_shape_nudge.md

    Pure so it can be tested without a database;
    :meth:`PostgresDB.increment_job_llm_outage_attempt` is its only caller and
    persists both flags into ``context.llm_outage``.

    The latch is the whole safety property. ``attempted`` must stay True for the
    rest of a streak so the NEXT identical failure falls through to the real
    give-up — otherwise every cycle re-arms and the job nudges forever, which is
    the exact grinding this was built to stop. It must also reset when the streak
    does (``repeats == 1``), so an unrelated future outage gets its own nudge.

    Args:
        prior: the ``llm_outage`` object as it was BEFORE this pause.
        repeats: consecutive-identical count INCLUDING this pause (1 = fresh).
        nudge_at_repeats: arm at this count; None disables the quickfix.

    Returns:
        ``(pending, attempted)`` — ``pending`` is consumed by the agent on the
        next resume; ``attempted`` persists for the life of the streak.
    """
    streak_continues = repeats > 1
    attempted = bool(streak_continues and prior.get("shape_nudge_attempted"))
    if nudge_at_repeats is not None and repeats >= nudge_at_repeats and not attempted:
        return True, True
    return False, attempted


def evaluate_llm_outage(ctx: dict[str, Any], now: datetime) -> dict[str, Any]:
    """Decide an ``llm_unavailable`` freeze: apply the auto-reset + give-up ceiling.

    Pure function of the job's parsed ``context`` and an aware UTC ``now`` — no
    I/O — so the ``/complete`` handler and the sweeper backstop reach the same
    verdict. Returns the post-reset, PRE-increment outage state:

    * ``attempt``          — pauses so far for THIS outage (0 after a reset)
    * ``first_failed_at``  — outage start (``now`` on a fresh/reset outage)
    * ``elapsed_seconds``  — outage duration so far
    * ``reset``            — the 30-min gap reset fired
    * ``over_ceiling``     — the duration OR attempts ceiling has tripped
    * ``ceiling_reason``   — ``"duration"`` | ``"attempts"`` | ``None``
    """
    outage = ctx.get("llm_outage")
    outage = outage if isinstance(outage, dict) else {}
    attempt = int(outage.get("attempt", 0) or 0)
    first_failed_at = _parse_ts(outage.get("first_failed_at"))
    last_failed_at = _parse_ts(outage.get("last_failed_at"))
    next_retry_at = _parse_ts(outage.get("next_retry_at"))

    # Measure idle time from when the job was free to run again — the END of any
    # scheduled wait we imposed (next_retry_at) — not from the last failure. A
    # long cooldown pause (retry_after floored to hours) is us deliberately
    # sleeping, not the job running fine; anchoring on last_failed_at would
    # misread it as a productive gap, spuriously reset the ceiling, and let a
    # never-clearing cooldown park the loop forever. next_retry_at is absent on
    # legacy state → fall back to last_failed_at (byte-for-byte today's behavior).
    # knowledge-base/knowledge/features/llm_cooldown_pause_and_resume.md §Design decision
    anchor = last_failed_at
    if next_retry_at is not None and (anchor is None or next_retry_at > anchor):
        anchor = next_retry_at
    reset = (
        anchor is not None
        and (now - anchor).total_seconds() > LLM_OUTAGE_RESET_WINDOW_SECONDS
    )
    if reset:
        attempt = 0
        first_failed_at = None
    if first_failed_at is None:
        first_failed_at = now

    elapsed = (now - first_failed_at).total_seconds()
    ceiling_reason: str | None = None
    if elapsed >= LLM_OUTAGE_CEILING_SECONDS:
        ceiling_reason = "duration"
    elif attempt >= LLM_OUTAGE_MAX_ATTEMPTS:
        ceiling_reason = "attempts"

    return {
        "attempt": attempt,
        "first_failed_at": first_failed_at,
        "elapsed_seconds": elapsed,
        "reset": reset,
        "over_ceiling": ceiling_reason is not None,
        "ceiling_reason": ceiling_reason,
    }


def llm_outage_backoff_seconds(
    attempt: int,
    *,
    base: int | None = None,
    cap: int | None = None,
    jitter: str | None = None,
    retry_after_seconds: float | None = None,
    rng: Callable[[float, float], float] | None = None,
) -> float:
    """Seconds to wait before the next re-dispatch for outage ``attempt`` (1-indexed).

    Deterministic envelope ``min(cap, base * 2**(attempt-1))`` (coefficient 2 —
    the universal default), then jitter:

    * ``full``  — ``uniform(0, envelope)`` (AWS Full Jitter; flattens a
      fleet-wide thundering herd across the whole window)
    * ``equal`` — ``envelope/2 + uniform(0, envelope/2)`` (>=50% floor)

    A server-directed ``retry_after_seconds`` (Retry-After /
    ``anthropic-ratelimit-*-reset``) floors the result. ``rng`` is injectable
    (signature ``uniform(a, b)``) for deterministic tests.
    """
    base = LLM_OUTAGE_BACKOFF_BASE_SECONDS if base is None else base
    cap = LLM_OUTAGE_BACKOFF_CAP_SECONDS if cap is None else cap
    jitter = (LLM_OUTAGE_JITTER if jitter is None else jitter).lower()
    _uniform = rng or random.uniform

    n = max(1, int(attempt))
    # Cap the exponent so an absurd attempt can't overflow the shift (the
    # envelope caps the value anyway).
    envelope = min(float(cap), float(base) * (2 ** min(n - 1, 30)))
    if jitter == "equal":
        delay = envelope / 2 + _uniform(0, envelope / 2)
    else:  # full jitter (default)
        delay = _uniform(0, envelope)
    if retry_after_seconds is not None:
        delay = max(delay, float(retry_after_seconds))
    return delay


def determine_job_status(
    job: dict[str, Any],
    result: dict[str, Any],
    *,
    parent_status: str | None = None,
) -> tuple[str | None, str | None]:
    """Determine the new job status from the graph execution result.

    The orchestrator is the single authority for job status. Agents report
    facts (should_stop, goal_achieved, freeze_data) and this function
    determines the DB status.

    Args:
        parent_status: For a subjob (``parent_job_id`` set), the parent's current
            status. Lets a drain-frozen subjob resolve terminally instead of
            pausing into a cascade-guard wedge when the parent has permanently
            failed. ``None`` for top-level jobs (or when unknown).

    Returns:
        ``(new_status, error_message)`` — either or both may be ``None``
        to indicate no change is needed for that field.
    """
    error = result.get("error")
    should_stop = result.get("should_stop", False)
    goal_achieved = result.get("goal_achieved", False)

    # Resolve freeze_data (DB row preferred, request-body fallback) BEFORE the
    # error short-circuit so it can consult an auto-redispatch / outage freeze.
    fd = _parse_freeze_data(job)
    if not fd:
        fd = result.get("freeze_data")
        if not isinstance(fd, dict):
            fd = {}
    freeze_type = fd.get("freeze_type")

    # Did the agent's report DECLARE a genuine completion? Resolved up-front so the
    # error short-circuit can tell a "finished the work, then a teardown blip"
    # report apart from a mid-run crash — the former keeps its completed outcome.
    is_completion = (
        goal_achieved
        or freeze_type == "job_complete"
        or fd.get("status") == "job_completed"
    )

    if error:
        error_msg = (
            error.get("message", str(error)) if isinstance(error, dict) else str(error)
        )
        # Terminal-status idempotency backstop: a job that ALREADY reached success
        # must never be downgraded to 'failed' by a late/duplicate report or a
        # post-completion teardown error — a second /complete, or a trailing
        # SSH/IO timeout after the completion side-effects already merged the work.
        # (Carve-out (2) below handles the single-report dual-payload case; this
        # covers the already-persisted row regardless of which trailing op raised
        # the error.) Return "no change" so the successful status stands.
        # ``curated`` (§6.4) is the same evidence as ``merged``: the job's
        # contribution is already on ``main``.
        # knowledge-history/done/coincident_infra_error_overrides_reported_job_outcome.md
        if job.get("status") == "completed" or job.get("merge_status") in (
            "merged",
            "curated",
        ):
            logger.warning(
                "Job %s: ignoring a coincident error on an already-successful job "
                "(status=%s, merge_status=%s): %s",
                job.get("id"),
                job.get("status"),
                job.get("merge_status"),
                error_msg[:200],
            )
            return (None, None)
        # A coincident error must not mask an outcome the agent already reported.
        # Two carve-outs let the report's own branch below own the decision:
        #  (1) a clean-boundary auto-redispatch / outage freeze (drain
        #      Continue-as-New, memory/KB retry caps, LLM-outage ceiling) — the
        #      work is checkpointed and re-dispatch resumes it. For a SUBJOB the
        #      carve-out additionally requires a live parent and one of the
        #      subjob-redispatch freeze types (a dead parent means the pause
        #      could never resume — fail on the error as before). A stale row
        #      freeze can't shield a crashed run (should_stop is False there →
        #      falls through to failed).
        #      knowledge-history/done/version_upgrade_drain_masked_by_coincident_error.md
        #      knowledge-base/knowledge/features/llm_outage_subjob_resilience.md
        #  (2) a genuine completion whose only error is a post-completion
        #      workspace/VM *teardown* blip: the VM is reaped on completion and a
        #      trailing SSH/IO op then times out against the gone workspace. The
        #      deliverables already landed (merge/graft ran); the completion branch
        #      below resolves it exactly as it would with no error. A NON-teardown
        #      error (real mid-run crash) still fails, even on a completion report.
        #      knowledge-history/done/coincident_infra_error_overrides_reported_job_outcome.md
        redispatchable = should_stop and (
            (
                job.get("parent_job_id") is None
                and freeze_type in ERROR_IMMUNE_FREEZE_TYPES
            )
            or (
                job.get("parent_job_id") is not None
                and freeze_type in SUBJOB_REDISPATCH_FREEZE_TYPES
                and parent_status not in _PARENT_TERMINAL_BLOCKING
            )
        )
        completed_despite_teardown = (
            should_stop and is_completion and is_teardown_infra_error(error_msg)
        )
        if not (redispatchable or completed_despite_teardown):
            return ("failed", error_msg)
        logger.warning(
            "Job %s: routing the reported outcome over a coincident error "
            "(freeze_type=%r, is_completion=%s, teardown=%s): %s",
            job.get("id"),
            freeze_type,
            is_completion,
            completed_despite_teardown,
            error_msg[:200],
        )

    if not should_stop:
        return (None, None)  # Still running — leave as processing

    # Critic jobs (have parent_job_id): read status from freeze_data if the
    # agent supplied an explicit one. As of Task 8, neither approve_job_verdict nor
    # return_job_with_feedback writes a "status" key any more — the verdict
    # lives on the TARGET's durable ledger, not on the critic's own freeze —
    # so this branch normally falls through to "infer from goal_achieved"
    # below, and BOTH verdicts resolve the critic's own job to "completed".
    # The target's own status is a separate decision made later, from the
    # ledger, in _handle_critic_verdict_on_complete (orchestrator/main.py).
    if job.get("parent_job_id") is not None:
        fd_status = fd.get("status")
        if fd_status:
            # Normalize synonyms
            if fd_status == "job_completed":
                fd_status = "completed"
            logger.debug(
                "Job %s is a sub-job — setting status from freeze_data: %s",
                job.get("id"),
                fd_status,
            )
            return (fd_status, None)
        # A drain (version_upgrade) or outage (memory/kb/llm) freeze on a subjob
        # is re-dispatchable exactly like a top-level job — this short-circuit
        # historically routed them to pending_review because the freeze table
        # below was unreachable for subjobs. Guard the parent-terminal case
        # first: if the parent has permanently failed, the dispatcher's cascade
        # guard will never re-dispatch the subjob (a silent paused wedge,
        # strictly worse than the visible pending_review), so resolve terminally
        # instead. The drain freeze pauses here; the outage freezes fall THROUGH
        # to the shared type-specific branches below — their retry caps and
        # duration ceilings live on the subjob's own row
        # (context.memory_retry_count / context.llm_outage), so they apply
        # per-subjob unchanged.
        # knowledge-history/done/coincident_infra_error_overrides_reported_job_outcome.md
        # knowledge-base/knowledge/features/llm_outage_subjob_resilience.md
        if freeze_type in SUBJOB_REDISPATCH_FREEZE_TYPES:
            if parent_status in _PARENT_TERMINAL_BLOCKING:
                return ("completed" if goal_achieved else "cancelled", None)
            if freeze_type in CONTINUE_AS_NEW_FREEZE_TYPES:
                return ("paused", None)
            # memory/kb/llm: shared branches below own pause-vs-fail.
        else:
            # No explicit status in freeze_data — infer from goal_achieved
            return ("completed" if goal_achieved else "pending_review", None)

    # Unattended project-loop jobs must never land on the human-review gate:
    # the loop's advance hook fires only on TERMINAL statuses, so a
    # pending_review loop job wedges the whole loop forever (the same
    # invisible-wedge class as the Mode-A diff gate, which is exempted
    # narrowly in the /complete handler). Quality judgment for loop work
    # belongs to the retro + the next critic, and lost work is already
    # flagged by the empty-merge (F29) check — so a loop job that stops
    # without declaring completion maps to `completed` (weak, but honest in
    # its retro), never `pending_review`. Found live: a campaign member that
    # finished without freeze_data wedged the P1 smoke loop.
    # knowledge-base/knowledge/features/loop_campaign_scheduling.md.
    from orchestrator.services.project_loops import job_loop_id

    is_loop_job = bool(job_loop_id(job))

    # Job completion (any autonomy level)
    if is_completion:
        if is_verification_enabled(job):
            return ("reviewing", None)
        elif goal_achieved or is_loop_job:
            return ("completed", None)
        else:
            # Non-full autonomy, no verification — keep pending_review
            return ("pending_review", None)

    # Phase boundary freeze or other non-completion stop
    if freeze_type == "delegation":
        return ("waiting", None)
    if freeze_type == "vm_upgrade_required":
        return ("paused", None)
    if freeze_type == "workspace_upgrade_required":
        # A worker job requested an in-process sandbox upgrade, but the agent's
        # upgrade attempt failed (provision/seed/grant) and surfaced the freeze
        # instead of swapping in place (workspace_tier_upgrade.md §4.3 W1). Pause
        # so the dispatcher can re-attempt, mirroring the vm_upgrade fallback. On
        # the happy path this freeze never reaches here — the agent handles it
        # in-process and the job continues without ever reporting the freeze.
        return ("paused", None)
    if freeze_type in CONTINUE_AS_NEW_FREEZE_TYPES:
        # Continue-as-New: the agent stopped at a durable checkpoint boundary.
        # version_upgrade goes to a fresh-version pinned agent; batch_boundary
        # goes back through the stateless run queue. Both preserve the same
        # logical job and therefore must remain non-terminal.
        return ("paused", None)
    if freeze_type in ("memory_unavailable", "kb_unavailable"):
        # A memory-required job (e.g. the self-improvement loop) lost its
        # embedding-backed stores at startup. Pause for bounded re-dispatch so a
        # transient dispatch-time credential miss self-heals on the next pod;
        # after MEMORY_RETRY_CAP attempts fail loudly instead of looping. The
        # /complete handler bumps context.memory_retry_count on each pause.
        ctx = job.get("context") or {}
        if isinstance(ctx, str):
            try:
                ctx = json.loads(ctx)
            except (json.JSONDecodeError, ValueError):
                ctx = {}
        retries = int((ctx or {}).get("memory_retry_count", 0) or 0)
        if retries < MEMORY_RETRY_CAP:
            return ("paused", None)
        reason = fd.get("reason") or "memory/KB unavailable at startup."
        return (
            "failed",
            f"{reason} Still unavailable after {retries} re-dispatch attempt(s) "
            f"— check the embedding model/endpoint (Admin → Models).",
        )
    if freeze_type == "llm_unavailable":
        # A transient LLM outage exhausted the worker's Tier-1 in-process
        # retries. Pause (non-terminal) so the outage sweeper re-dispatches the
        # SAME job on a backoff and it resumes from its checkpoint when the
        # endpoint recovers. After the duration ceiling (12h default,
        # LLM_OUTAGE_CEILING_SECONDS) or the attempts
        # backstop, fail loudly — a broken config must not park an overnight
        # loop iteration forever. The /complete handler owns the increment +
        # next_retry_at; here we only make the pause-vs-fail call.
        # knowledge-base/knowledge/features/llm_outage_pause_and_backoff_redispatch.md
        ev = evaluate_llm_outage(_parse_context(job), datetime.now(timezone.utc))
        if ev["over_ceiling"]:
            summary = (
                fd.get("error_summary")
                or fd.get("reason")
                or "LLM endpoint unavailable"
            )
            model = fd.get("model")
            if ev["ceiling_reason"] == "duration":
                detail = (
                    f"still unavailable after ~{LLM_OUTAGE_CEILING_SECONDS / 3600:.0f}h "
                    f"of continuous outage"
                )
            else:
                detail = f"still unavailable after {ev['attempt']} re-dispatch attempts"
            return (
                "failed",
                f"LLM endpoint unavailable — {detail}. Giving up. Last error: "
                f"{str(summary)[:200]}"
                + (f" (model '{model}')" if model else "")
                + ". Check the model endpoint/provider (Admin → Models).",
            )
        # Determinism fail-fast: a request-shaped 4xx whose normalized text is
        # identical to the PREVIOUS pause's is deterministic — a third, fourth,
        # ... cycle can only burn the give-up ceiling. Genuine outages never
        # enter here (llm_outage_fingerprint returns None for connection/5xx/
        # rate errors). knowledge-base/knowledge/features/outbound_message_hygiene.md (layer 3).
        fp = llm_outage_fingerprint(fd)
        prior = _parse_context(job).get("llm_outage")
        prior = prior if isinstance(prior, dict) else {}
        if fp is not None:
            if prior.get("fingerprint") == fp:
                summary = fd.get("error_summary") or fd.get("reason") or "LLM error"
                return (
                    "failed",
                    "Deterministic LLM request rejection — the identical "
                    "(normalized) error failed two consecutive backoff cycles; "
                    f"retrying cannot fix this input. Last error: "
                    f"{str(summary)[:200]}. The request/history is likely "
                    "poisoned or the model config is wrong — resume with "
                    "feedback (compacts context) or fix the model "
                    "(Admin → Models).",
                )
        # Repeat give-up: the same error (5xx/transport included) surviving
        # LLM_OUTAGE_REPEAT_CEILING consecutive pauses is not an outage we are
        # waiting out — each re-dispatch got a fresh pod, a fresh connection and
        # an idle endpoint, and still failed identically. Higher bar than the
        # 4xx path above precisely so a real multi-hour outage still rides the
        # backoff. The initial error is preferred in the message: the retry
        # storm's LAST error is often a downstream symptom of the FIRST one
        # (d251e513: a 408 stream drop, reported as five 503s).
        rk = llm_outage_repeat_key(fd)
        if rk is not None and prior.get("repeat_key") == rk:
            repeats = int(prior.get("repeats", 0) or 0) + 1
            if repeats >= LLM_OUTAGE_REPEAT_CEILING:
                # >>> TEMPORARY QUICKFIX — remove when the upstream 408 is fixed.
                # knowledge-history/done/codex_stream_disconnect_shape_nudge.md
                # Spend ONE more cycle on a request-shape change before giving
                # up. A byte-identical replay of a payload upstream has already
                # rejected N times cannot succeed, but appending a short turn
                # changes the payload and usually can — that is precisely what a
                # human does in Codex CLI when its 5 stream retries run out
                # ("type retry and it usually works again", openai/codex#10378).
                # We are autonomous, so nobody types it; this does.
                # The increment (postgres.increment_job_llm_outage_attempt) sets
                # pending_shape_nudge, the agent injects the turn on resume, and
                # shape_nudge_attempted makes it strictly one-shot per streak.
                if LLM_OUTAGE_SHAPE_NUDGE and not prior.get("shape_nudge_attempted"):
                    return ("paused", None)
                summary = (
                    fd.get("initial_error_summary")
                    or fd.get("error_summary")
                    or fd.get("reason")
                    or "LLM error"
                )
                return (
                    "failed",
                    f"LLM call failed identically on {repeats} consecutive "
                    "backoff cycles — each re-dispatch hit a fresh pod and an "
                    "idle endpoint, so this is not an outage that waiting can "
                    f"clear. Giving up instead of burning the "
                    f"{LLM_OUTAGE_CEILING_SECONDS / 3600:.0f}h ceiling. First "
                    f"error of the last attempt: {str(summary)[:200]}. Resume "
                    "with feedback (compacts context) or check the model "
                    "endpoint/provider (Admin → Models)."
                    + (
                        " A request-shape nudge was already tried and did not clear it."
                        if prior.get("shape_nudge_attempted")
                        else ""
                    ),
                )
        return ("paused", None)
    if is_loop_job and freeze_type is None:
        # Non-completion stop with no recognized freeze on an unattended loop
        # job (agent stopped without declaring job_complete — weak-model
        # finishes do this). `completed` keeps the loop advancing; the retro
        # records the actual (possibly poor) state and the empty-merge check
        # flags lost work. See the loop-wedge rationale above.
        logger.warning(
            "Loop job %s stopped without a completion declaration "
            "(freeze_type=%r) — mapping to 'completed' so the loop advances",
            job.get("id"),
            freeze_type,
        )
        return ("completed", None)
    if freeze_type is not None:
        # Fail visibly on version skew or a misspelled/new freeze type. Mapping
        # an unknown declared boundary to completed is data corruption for loop
        # jobs: the RSI loop merges partial work as if the unit had finished.
        logger.error(
            "Job %s stopped with unknown freeze_type=%r; routing to "
            "pending_review rather than guessing a terminal outcome",
            job.get("id"),
            freeze_type,
        )
    return ("pending_review", None)


# ---------------------------------------------------------------------------
# Template formatting
# ---------------------------------------------------------------------------


def format_verification_instructions(
    job_id: str,
    description: str,
    freeze_data: dict[str, Any],
    config_name: str,
    prior_findings: str = "",
) -> str | None:
    """Load and format the verification instructions template.

    Moved from ``OrchestratorClient._format_verification_instructions``.
    Template is loaded from ``config/experts/critic/verification_instructions.md``
    with fallback to ``config/templates/verification_instructions.md``.

    ``prior_findings`` defaults to ``""`` rather than being required: this is
    formatted with ``str.format``, which raises ``KeyError`` for a missing
    key, and the ``except KeyError`` branch below returns ``None`` — which
    would abort critic creation entirely at the caller. Callers should pass
    ``render_prior_findings(fold_open_findings(rounds), len(rounds))`` (see
    ``services.verification_ledger``); the fallback text below only covers
    callers that don't.

    That fallback deliberately does NOT claim "this is a first review": this
    function has no ledger and cannot tell a genuine first round from a later
    one whose findings were all resolved. Asserting the stronger claim without
    the evidence for it is what the caller-side fix removes.
    """
    search_paths = [
        _REPO_ROOT / "config" / "experts" / "critic" / "verification_instructions.md",
        _REPO_ROOT / "config" / "templates" / "verification_instructions.md",
    ]

    template_path = None
    for path in search_paths:
        if path.exists():
            template_path = path
            break

    if not template_path:
        logger.error(
            "Verification instructions template not found. Searched: %s",
            [str(p) for p in search_paths],
        )
        return None

    try:
        template = template_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error("Failed to read verification template %s: %s", template_path, e)
        return None

    # Format deliverables as a bulleted list
    deliverables = freeze_data.get("deliverables", [])
    if deliverables:
        deliverables_list = "\n".join(f"- `{d}`" for d in deliverables)
    else:
        deliverables_list = "- *(no deliverables listed)*"

    confidence = freeze_data.get("confidence", 0)
    confidence_str = (
        f"{confidence:.0%}" if isinstance(confidence, (int, float)) else str(confidence)
    )

    try:
        return template.format(
            target_job_id=job_id,
            target_config=config_name,
            target_description=description,
            deliverables_list=deliverables_list,
            agent_summary=freeze_data.get("summary", "*(no summary provided)*"),
            agent_confidence=confidence_str,
            prior_findings=prior_findings
            or "No open findings from previous rounds were supplied.",
        )
    except KeyError as e:
        logger.error("Verification template has unknown placeholder: %s", e)
        return None


def format_curation_instructions(
    job_id: str,
    description: str,
    config_name: str,
    phase_data: str,
    curation_mode: str = "incremental",
    curation_phase: str = "initial",
) -> str | None:
    """Load and format the curation instructions template.

    Moved from ``OrchestratorClient._format_curation_instructions``.
    """
    search_paths = [
        _REPO_ROOT / "config" / "experts" / "curator" / "curation_instructions.md",
        _REPO_ROOT / "config" / "templates" / "curation_instructions.md",
    ]

    template_path = None
    for path in search_paths:
        if path.exists():
            template_path = path
            break

    if not template_path:
        logger.error(
            "Curation instructions template not found. Searched: %s",
            [str(p) for p in search_paths],
        )
        return None

    try:
        template = template_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error("Failed to read curation template %s: %s", template_path, e)
        return None

    if curation_mode == "incremental":
        task_instructions = (
            "Read the phase artifacts below and extract knowledge notes. "
            "Search existing knowledge with `kb_search` before writing to avoid duplicates. "
            "Write notes via `kb_write`, then call `job_complete` with a summary."
        )
    else:
        task_instructions = (
            "This is the FINAL curation pass. Read memories, output/, and the final workspace.md. "
            "Promote valuable memories to knowledge notes. Write a `state` note summarizing "
            "what changed in the project. Check for open questions and unresolved items. "
            "Link all notes to related existing knowledge. Call `job_complete` when done."
        )

    try:
        return template.format(
            target_job_id=job_id,
            target_config=config_name,
            target_description=description,
            curation_phase=curation_phase,
            curation_mode=curation_mode,
            phase_context=phase_data,
            task_instructions=task_instructions,
        )
    except KeyError as e:
        logger.error("Curation template has unknown placeholder: %s", e)
        return None


def format_scholar_instructions(
    parent_job_id: str,
    description: str,
    config_name: str,
    instructions: str | None = None,
    output_dir: str = "research",
) -> str | None:
    """Load and format the scholar subjob instructions template.

    Template is loaded from ``config/experts/scholar/scholar_subjob_instructions.md``
    with fallback to ``config/templates/scholar_subjob_instructions.md``.
    """
    search_paths = [
        _REPO_ROOT
        / "config"
        / "experts"
        / "scholar"
        / "scholar_subjob_instructions.md",
        _REPO_ROOT / "config" / "templates" / "scholar_subjob_instructions.md",
    ]

    template_path = None
    for path in search_paths:
        if path.exists():
            template_path = path
            break

    if not template_path:
        logger.error(
            "Scholar instructions template not found. Searched: %s",
            [str(p) for p in search_paths],
        )
        return None

    try:
        template = template_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error("Failed to read scholar template %s: %s", template_path, e)
        return None

    if instructions:
        parent_instructions_section = f"## Additional Instructions\n\n{instructions}"
    else:
        parent_instructions_section = ""

    try:
        return template.format(
            parent_job_id=parent_job_id,
            parent_config=config_name,
            parent_description=description,
            parent_instructions_section=parent_instructions_section,
            output_dir=output_dir,
        )
    except KeyError as e:
        logger.error("Scholar template has unknown placeholder: %s", e)
        return None
