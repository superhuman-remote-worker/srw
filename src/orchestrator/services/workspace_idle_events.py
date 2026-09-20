"""Semantic human-wait publication inside existing authorized Job transactions.

Only the reviewed VM Job route adapter is present. This records metadata; no
lane acquires idle-release capability or a physical suspension loop here.
"""

import json
import os
from uuid import UUID

from orchestrator.services.vm_remote_operation import (
    VMRemoteOperationUnavailable,
    _identity_from_row,
)
from orchestrator.services.vm_workspace_recovery_store import _job_workspace_owner
from shared.workspace_idle_policy import RuntimeIdentity, read_episode
from shared.workspace_idle_store import apply_idle_transition_on_conn


async def record_human_route_wait_on_conn(conn, *, job_id, route_id):
    """Caller already proved source claimant and committed this exact freeze.

    Unknown runtime identity leaves the lane ineligible; this helper must not
    bootstrap runtime authority from a name or assume a child's shared owner.
    """
    if os.getenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false").lower() != "true":
        return False
    row = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1 FOR UPDATE", job_id)
    if row is None or row["status"] != "waiting_for_reply":
        return False
    row = dict(row)
    route = await conn.fetchrow(
        "SELECT job_id,project_id,blocking,state FROM job_message_routes WHERE route_id=$1",
        UUID(str(route_id)),
    )
    if (
        route is None
        or route["job_id"] != row["id"]
        or route["project_id"] != row["project_id"]
        or not route["blocking"]
        or route["state"] not in {"user_direct", "pending_both", "escalated_to_user"}
    ):
        return False
    for key in ("context", "config_override", "freeze_data", "workspace_idle_episode"):
        if isinstance(row[key], str):
            row[key] = json.loads(row[key])
    freeze = row["freeze_data"]
    if not isinstance(freeze, dict) or freeze.get("route_id") != str(route_id):
        return False
    owner, ambiguous = _job_workspace_owner(UUID(str(job_id)), row)
    if ambiguous or owner != UUID(str(job_id)):
        return False
    vm = row["context"].get("vm")
    if not isinstance(vm, dict) or vm.get("status") != "ready":
        return False
    try:
        identity = _identity_from_row(
            row, owner_kind="job", owner_id=str(job_id), operation_kind="idle_policy"
        )
    except VMRemoteOperationUnavailable:
        return False
    # Older remote-operation callers accept opaque VM identifiers. Idle policy
    # supports only the exact Kubernetes UID, never a legacy name fallback.
    try:
        if str(UUID(identity.vm_uid)) != identity.vm_uid:
            return False
    except ValueError:
        return False
    prior = read_episode(
        row["workspace_idle_episode"], revision=row["workspace_idle_revision"]
    )
    await apply_idle_transition_on_conn(
        conn,
        runtime=RuntimeIdentity(
            "job", str(job_id), "vm", identity.workspace_generation, identity.vm_uid
        ),
        event="enter",
        expected_revision=row["workspace_idle_revision"],
        expected_episode_id=prior.episode_id if prior else None,
        wait_kind="human_message",
        wait_key=str(route_id),
    )
    return True
