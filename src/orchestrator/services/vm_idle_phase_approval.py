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


async def _source_runtime_continuity(
    conn, *, job, episode, source, generation, vm_uid, vmi_uid,
    launcher_uid, pvc_uid,
):
    """Check the immutable S17 origin and inductively verified current link.

    The 0273 trigger checks each link against its immediate predecessor before
    commit, and prevents later edit/deletion. Thus root and latest checks are
    constant-size regardless of how many access-only wakes occurred.
    """
    original = _object(source.get("runtime_identity"))
    if original.get("owner_kind") != "job" or original.get("owner_id") != str(job["id"]):
        return False
    if original.get("backend") != "vm":
        return False
    predecessor = {
        "generation": original.get("runtime_generation"),
        "vm_uid": original.get("runtime_uid"),
        "launcher_uid": source.get("launcher_uid"),
        "pvc_uid": source.get("rootdisk_pvc_uid"),
    }
    if any(not isinstance(value, str) or not value for value in predecessor.values()):
        return False
    current = {
        "generation": str(generation), "vm_uid": str(vm_uid),
        "launcher_uid": str(launcher_uid), "pvc_uid": str(pvc_uid),
    }
    if (
        episode.runtime_identity.runtime_generation != current["generation"]
        or episode.runtime_identity.runtime_uid != current["vm_uid"]
    ):
        return False
    latest = await conn.fetchrow(
        "SELECT * FROM vm_idle_operations WHERE owner_kind='job' AND owner_id=$1 "
        "AND episode_id=$2 AND episode_revision<$3 "
        "ORDER BY episode_revision DESC,id DESC LIMIT 1",
        job["id"], UUID(episode.episode_id), episode.revision,
    )
    if latest is None:
        return predecessor == current
    latest_proof = _object(latest["access_rebind_proof"])
    try:
        root_id = UUID(latest_proof["root_operation_id"])
    except (KeyError, TypeError, ValueError):
        return False
    root = await conn.fetchrow(
        "SELECT * FROM vm_idle_operations WHERE id=$1 AND owner_kind='job' "
        "AND owner_id=$2 AND episode_id=$3",
        root_id, job["id"], UUID(episode.episode_id),
    )
    if root is None or await conn.fetchval(
        "SELECT 1 FROM vm_idle_operations WHERE owner_kind='job' AND owner_id=$1 "
        "AND episode_id=$2 AND episode_revision<=$3 AND id<>$4 LIMIT 1",
        job["id"], UUID(episode.episode_id), root["episode_revision"], root["id"],
    ) is not None:
        return False

    def valid_link(row, proof):
        return (
            row["phase"] == "ready" and row["closed_at"] is not None
            and row["stop_verified_at"] is not None
            and row["wake_ready_at"] is not None
            and not row["wake_execution_requested"]
            and not row["post_ready_resume_requested"]
            and row["terminal_source_command_id"] is None
            and proof.get("version") == 1
            and proof.get("operation_id") == str(row["id"])
            and proof.get("episode_id") == episode.episode_id
            and proof.get("wait_kind") == episode.wait_kind
            and proof.get("wait_key") == episode.wait_key
            and proof.get("entered_at") == episode.entered_at.isoformat()
            and proof.get("from_revision") == row["episode_revision"]
            and proof.get("to_revision") == row["episode_revision"] + 1
            and proof.get("wake_id") == str(row["wake_id"])
            and proof.get("stop_verified_at") == row["stop_verified_at"].isoformat()
            and proof.get("ready_at") == row["wake_ready_at"].isoformat()
            and _object(proof.get("predecessor")) == {
                "generation": str(row["provision_generation"]),
                "vm_uid": str(row["vm_uid"]),
                "vmi_uid": str(row["vmi_uid"]),
                "launcher_uid": str(row["launcher_uid"]),
                "pvc_uid": str(row["pvc_uid"]),
            }
            and _object(proof.get("successor")).get("generation")
                == str(row["wake_generation"])
            and _object(proof.get("successor")).get("pvc_uid")
                == str(row["pvc_uid"])
        )

    root_proof = _object(root["access_rebind_proof"])
    if (
        not valid_link(root, root_proof)
        or not valid_link(latest, latest_proof)
        or root_proof.get("root_operation_id") != str(root["id"])
        or root_proof.get("root_revision") != root["episode_revision"]
        or root_proof.get("chain_length") != 1
        or latest_proof.get("root_operation_id") != str(root["id"])
        or latest_proof.get("root_revision") != root["episode_revision"]
        or latest_proof.get("chain_length")
            != latest["episode_revision"] - root["episode_revision"] + 1
        or latest["episode_revision"] + 1 != episode.revision
        or {key: _object(root_proof.get("predecessor")).get(key) for key in predecessor}
            != predecessor
        or {key: _object(latest_proof.get("successor")).get(key) for key in current}
            != current
        or _object(latest_proof.get("successor")).get("vmi_uid") != str(vmi_uid)
    ):
        return False
    return True


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
    vmi_uid=None,
    pvc_uid=None,
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
    ):
        return None
    vm = _object(_object(job["context"]).get("vm"))
    if not await _source_runtime_continuity(
        conn, job=job, episode=episode, source=source,
        generation=generation, vm_uid=vm_uid,
        vmi_uid=vmi_uid if vmi_uid is not None else vm.get("vmi_uid"),
        launcher_uid=launcher_uid,
        pvc_uid=pvc_uid if pvc_uid is not None else vm.get("rootdisk_pvc_uid"),
    ):
        return None
    return {"command_id": str(command_id), "semantics": semantics}


async def finalized_review_source(
    conn, *, job, episode, generation, vm_uid, launcher_uid, pvc_uid,
    vmi_uid=None,
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
    ):
        return None
    if not await _source_runtime_continuity(
        conn, job=job, episode=episode, source=source,
        generation=generation, vm_uid=vm_uid,
        vmi_uid=vmi_uid if vmi_uid is not None else _object(
            _object(job["context"]).get("vm")
        ).get("vmi_uid"),
        launcher_uid=launcher_uid, pvc_uid=pvc_uid,
    ):
        return None
    return {"command_id": str(command_id), "semantics": semantics}
