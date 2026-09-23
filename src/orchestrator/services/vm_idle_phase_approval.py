"""Exact completed phase source for stateless VM idle release and approval."""

import json
from uuid import UUID

from orchestrator.services.workspace_idle_completion_events import (
    ACCEPTED_IDLE_WAIT_SOURCE_KEY,
)
from shared.workspace_idle_completion import classify_completion_wait


def _object(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return value if isinstance(value, dict) else {}


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
