"""Exact completed phase source for stateless VM idle release and approval."""

import json
from uuid import UUID

from orchestrator.services.workspace_idle_completion_events import (
    ACCEPTED_IDLE_WAIT_SOURCE_KEY,
)
from shared.workspace_idle_completion import classify_completion_wait
from shared.workspace_idle_policy import IdlePolicyError, read_episode


def _object(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return value if isinstance(value, dict) else {}


def approval_source_snapshot(job):
    """Freeze the route-loaded phase A identity before any control await."""
    if (
        not isinstance(job, dict)
        or job.get("status") != "pending_review"
        or job.get("execution_lane") != "stateless"
    ):
        return None
    freeze = _object(job.get("freeze_data"))
    if freeze.get("freeze_type") != "phase_boundary":
        return None
    try:
        episode = read_episode(
            _object(job.get("workspace_idle_episode")),
            revision=job.get("workspace_idle_revision"),
        )
        if episode is None or episode.wait_kind != "human_approval":
            return None
        command_id = str(UUID(episode.wait_key))
    except (IdlePolicyError, TypeError, ValueError):
        return None
    return {
        "command_id": command_id,
        "episode_id": episode.episode_id,
        "episode_revision": episode.revision,
        "freeze_type": "phase_boundary",
        "phase_type": freeze.get("phase_type"),
        "phase_number": freeze.get("phase_number"),
        "runtime_generation": episode.runtime_identity.runtime_generation,
        "runtime_uid": episode.runtime_identity.runtime_uid,
    }


def review_source_snapshot(job):
    """Freeze the route-loaded final-review source before any asynchronous gate."""
    if (
        not isinstance(job, dict)
        or job.get("status") != "pending_review"
        or job.get("execution_lane") != "stateless"
    ):
        return None
    freeze = _object(job.get("freeze_data"))
    if freeze.get("freeze_type") != "job_complete":
        return None
    try:
        episode = read_episode(
            _object(job.get("workspace_idle_episode")),
            revision=job.get("workspace_idle_revision"),
        )
        if episode is None or episode.wait_kind != "human_review":
            return None
        command_id = str(UUID(episode.wait_key))
    except (IdlePolicyError, TypeError, ValueError):
        return None
    return {
        "command_id": command_id,
        "episode_id": episode.episode_id,
        "episode_revision": episode.revision,
        "freeze_type": "job_complete",
        "runtime_generation": episode.runtime_identity.runtime_generation,
        "runtime_uid": episode.runtime_identity.runtime_uid,
    }


async def finalized_phase_source(
    conn,
    *,
    job,
    episode,
    generation,
    vm_uid,
    launcher_uid,
):
    """Read the S17-completed source, never infer approval from a display state."""
    if (
        episode is None
        or episode.wait_kind != "human_approval"
        or job["status"] != "pending_review"
    ):
        return None
    try:
        command_id = UUID(episode.wait_key)
    except (TypeError, ValueError):
        return None
    command = await conn.fetchrow(
        "SELECT payload,report_seq,state,finalized_at FROM job_completion_commands "
        "WHERE id=$1 AND job_id=$2",
        command_id,
        job["id"],
    )
    if (
        command is None
        or command["state"] != "done"
        or command["finalized_at"] is None
        or command["report_seq"] != job["completion_seq_hwm"]
    ):
        return None
    effect_done = await conn.fetchval(
        "SELECT 1 FROM completion_effects WHERE producer_kind='job_completion' "
        "AND producer_id=$1 AND scope_id=$2 AND effect_name='main_status_write' "
        "AND state='done' AND completed_at IS NOT NULL",
        command_id,
        job["id"],
    )
    if effect_done is None:
        return None
    payload = _object(command["payload"])
    source = _object(payload.get(ACCEPTED_IDLE_WAIT_SOURCE_KEY))
    semantics = _object(source.get("semantics"))
    runtime = _object(source.get("runtime_identity"))
    current = classify_completion_wait(
        job=dict(job),
        report={**payload, "freeze_data": _object(job["freeze_data"])},
    )
    if (
        source.get("version") != 1
        or current is None
        or current != semantics
        or current.get("branch") != "phase_approval"
        or current.get("wait_kind") != "human_approval"
        or runtime
        != {
            "owner_kind": "job",
            "owner_id": str(job["id"]),
            "backend": "vm",
            "runtime_generation": str(generation),
            "runtime_uid": str(vm_uid),
        }
        or source.get("launcher_uid") != str(launcher_uid)
    ):
        return None
    return {"command_id": str(command_id), "semantics": semantics}


async def finalized_review_source(
    conn, *, job, episode, generation, vm_uid, launcher_uid, pvc_uid,
):
    """Require the exact finalized S17 final-review decision and effect."""
    if (
        episode is None
        or episode.wait_kind != "human_review"
        or job["status"] != "pending_review"
        or _object(job["freeze_data"]).get("freeze_type") != "job_complete"
    ):
        return None
    try:
        command_id = UUID(episode.wait_key)
    except (TypeError, ValueError):
        return None
    command = await conn.fetchrow(
        "SELECT payload,report_seq,state,finalized_at FROM job_completion_commands "
        "WHERE id=$1 AND job_id=$2", command_id, job["id"],
    )
    if (
        command is None or command["state"] != "done"
        or command["finalized_at"] is None
        or command["report_seq"] != job["completion_seq_hwm"]
    ):
        return None
    if await conn.fetchval(
        "SELECT 1 FROM completion_effects WHERE producer_kind='job_completion' "
        "AND producer_id=$1 AND scope_id=$2 AND effect_name='main_status_write' "
        "AND state='done' AND completed_at IS NOT NULL", command_id, job["id"],
    ) is None:
        return None
    payload = _object(command["payload"])
    source = _object(payload.get(ACCEPTED_IDLE_WAIT_SOURCE_KEY))
    semantics = _object(source.get("semantics"))
    context = _object(job["context"])
    decision = _object(context.get("completion_decision"))
    current = classify_completion_wait(
        job=dict(job),
        report={**payload, "freeze_data": _object(job["freeze_data"])},
        decision_tool_call_id=semantics.get("decision_tool_call_id"),
    )
    if (
        source.get("version") != 1 or current is None or current != semantics
        or current.get("branch") != "final_human_review"
        or current.get("wait_kind") != "human_review"
        or decision.get("tool_call_id") != semantics.get("decision_tool_call_id")
        or _object(source.get("runtime_identity")) != {
            "owner_kind": "job", "owner_id": str(job["id"]), "backend": "vm",
            "runtime_generation": str(generation), "runtime_uid": str(vm_uid),
        }
        or source.get("launcher_uid") != str(launcher_uid)
        or source.get("rootdisk_pvc_uid") != str(pvc_uid)
    ):
        return None
    return {"command_id": str(command_id), "semantics": semantics}
