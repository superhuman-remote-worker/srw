"""Stateless → pinned lane switch for a VM workspace upgrade (C2).

The owner control route calls this after ``check_stateless_vm_upgrade`` and
while it holds ``stateless_session_workspace_ensure_lock`` (the lock stateless
End, Resume, uploads and the ensure scheduler hold). One transaction, locking
``threads`` then ``run_queue`` like every stateless writer:

* proves the row is a quiet root lite stateless Session (spec §2 table);
* records the request as a pending control with no agent and no queue wake
  (no ``record_control_seq``);
* closes the session queue the way End does (token + 1, ``done``), so no claim
  can start and a stale lease fails its fence;
* bumps the journal epoch and appends an unlinked ``session.lane_changed``
  frame (nothing holds the epoch: the queue is closed and no pinned agent is
  bound — the writer-exclusion rule in ``shared.event_journal``);
* flips the lane to ``pinned`` and drops ``_stateless_active_claim``.

The first pinned owner adopts the request (``_ADOPT_PINNED_SQL``) and runs the
VM hot swap. Every pinned path re-checks the lane under its own lock, so the
pinned ``thread_advisory_lock`` is not taken here.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from orchestrator.services.stateless_workspace_gate import (
    stateless_session_workspace_check,
    thread_metadata_object,
)
from orchestrator.services.stateless_workspace_upgrade import (
    VM_UPGRADE_NOT_POSSIBLE,
    is_root_session,
)
from orchestrator.services.thread_control_inbox import (
    AdmittedControl,
    ControlAdmissionError,
    ControlAdmissionNotReady,
)
from shared.backend_kinds import LITE_BACKENDS
from shared.event_journal import append_system_frame, bump_epoch
from shared.run_queue import (
    LANE_STATELESS,
    STATE_DONE,
    STATE_LEASED,
    STATE_PARKED,
    STATE_QUEUED,
    UNIT_KIND_SESSION_TURN,
)
from shared.session_retirement import ACTIVE_CLAIM_KEY, STATELESS_STOP_KEYS
from shared.workspace_upgrade import (
    LANE_CHANGED_FRAME,
    WORKSPACE_UPGRADE_PAYLOAD_KEYS,
    WORKSPACE_UPGRADE_VERB,
    metadata_object,
)

VM_UPGRADE_SESSION_ENDING = (
    "This Session is ending or recovering an executor, so it can't move to a "
    "VM workspace now."
)
VM_UPGRADE_TURN_RUNNING = "Session is completing a turn; retry the VM upgrade"
VM_UPGRADE_INPUT_PENDING = (
    "Session has pending input; retry the VM upgrade after that turn completes"
)
VM_UPGRADE_BACKGROUND_WORK = (
    "Session has background work in progress; retry the VM upgrade"
)
_STRANDED_PINNED_CONTROL = "Session has a control awaiting its pinned owner"
_QUEUE_PARKED = "Session queue is parked; unpark it before sending controls"

_THREAD_SQL = """
SELECT id, user_id, status, kind, execution_lane, agent_id, runtime_generation,
       runtime_attach_token, runtime_retirement_token, control_admission_agent_id,
       control_seq_hwm, metadata
FROM threads WHERE id = $1 FOR UPDATE
"""
_EXISTING_SQL = """
SELECT id, request_seq, client_request_id, verb, payload, outcome, runtime_generation
FROM thread_control_requests WHERE thread_id = $1 AND client_request_id = $2
"""
_QUEUE_SQL = """
SELECT unit_kind, state, lease_token, input_seq, consumed_seq,
       control_input_seq, control_consumed_seq
FROM run_queue WHERE unit_id = $1 FOR UPDATE
"""
# A stranded pinned request (accepted_agent_id set) sorts first.
_PENDING_CONTROL_SQL = """
SELECT accepted_agent_id IS NOT NULL AS stranded
FROM thread_control_requests
WHERE thread_id = $1 AND outcome IS NULL
ORDER BY (accepted_agent_id IS NOT NULL) DESC
LIMIT 1
"""
# End's interrupt and permission rules (begin_stateless_thread_workspace_retirement
# in postgres.py), the 0191 delivery states, live child subagents, and an
# unacknowledged cloud push.
_BACKGROUND_WORK_SQL = """
SELECT EXISTS (SELECT 1 FROM thread_interrupt_requests
                WHERE thread_id = $1
                  AND (outcome IS NULL OR (outcome = 'applied' AND NOT
                       (COALESCE(result, '{}'::jsonb) ? 'consumed_input_seq'))))
    OR EXISTS (SELECT 1 FROM thread_permission_requests
                WHERE thread_id = $1 AND status = 'pending')
    OR EXISTS (SELECT 1 FROM thread_input_deliveries
                WHERE thread_id = $1
                  AND state IN ('persisted', 'owned', 'queued', 'deferred'))
    OR EXISTS (SELECT 1 FROM threads
                WHERE parent_thread_id = $1 AND kind = 'subagent'
                  AND status IN ('created', 'active'))
    OR EXISTS (SELECT 1 FROM thread_cloud_sync_generations
                WHERE thread_id = $1
                  AND acknowledged_generation < required_generation)
"""
# 0185 reciprocity: a pinned row without an agent has no agents row.
_AGENT_ROW_SQL = "SELECT EXISTS (SELECT 1 FROM agents WHERE thread_id = $1)"
_BUMP_HWM_SQL = """
UPDATE threads SET control_seq_hwm = $2, last_activity = now()
WHERE id = $1 AND runtime_generation = $3 AND runtime_retirement_token IS NULL
RETURNING id
"""
_INSERT_REQUEST_SQL = """
INSERT INTO thread_control_requests (
    thread_id, request_seq, client_request_id, verb, payload, requested_by,
    accepted_agent_id, runtime_generation
) VALUES ($1, $2, $3, $4, $5::jsonb, $6, NULL, $7)
RETURNING id
"""
# End's close (begin_stateless_thread_workspace_retirement) without the
# interrupt reconcile: the row is not leased, so there is no claimant to settle.
_CLOSE_QUEUE_SQL = """
UPDATE run_queue
   SET lease_token = $2::bigint + 1, state = 'done', attempts_since_completion = 0,
       leased_by = NULL, last_leased_by = NULL, leased_until = NULL,
       interrupt_admission_lease_token = NULL, interrupt_admission_turn_id = NULL,
       queued_at = now(), run_after = now()
 WHERE unit_id = $1 AND unit_kind = 'session_turn' AND lease_token = $2::bigint
   AND state IN ('queued', 'done') AND leased_by IS NULL
RETURNING lease_token
"""
# A natural pause keeps its status, but its timer restarts: the pinned
# attention-sleep sweep (session_attention.py) skips only stateless rows, so an
# old stateless timer would suspend the Session before its first subscriber.
_FLIP_LANE_SQL = """
UPDATE threads
   SET execution_lane = 'pinned', metadata = metadata - $3::text,
       last_activity = now(),
       awaiting_user_since = CASE WHEN status = 'awaiting_user' THEN now()
                                  ELSE awaiting_user_since END
 WHERE id = $1 AND execution_lane = 'stateless' AND runtime_generation = $2
   AND agent_id IS NULL
RETURNING id
"""


def _payload_generation(payload: Mapping[str, Any]) -> UUID:
    if not set(payload) <= WORKSPACE_UPGRADE_PAYLOAD_KEYS or payload.get(
        "target_tier"
    ) not in (None, "vm"):
        raise ControlAdmissionError(
            "upgrade-to-workspace accepts only target_tier, template and "
            "runtime_generation"
        )
    try:
        return UUID(str(payload["runtime_generation"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise ControlAdmissionError(
            "upgrade-to-workspace requires session_runtime_generation"
        ) from exc


def require_switchable_row(
    thread: Mapping[str, Any] | None,
    *,
    owner_user_id: UUID | None,
    generation: UUID,
) -> None:
    """The row facts End and control admission require (spec §2, row 1-2)."""
    if thread is None or thread["user_id"] != owner_user_id:
        raise ControlAdmissionError("Thread is unavailable")
    metadata = thread_metadata_object(thread)
    if any(key in metadata for key in STATELESS_STOP_KEYS):
        raise ControlAdmissionError(VM_UPGRADE_SESSION_ENDING)
    backend, refusal = stateless_session_workspace_check(dict(thread))
    if (
        thread.get("execution_lane") != LANE_STATELESS
        or not is_root_session(thread)
        or str(thread.get("status") or "") not in {"created", "active", "awaiting_user"}
        or thread.get("runtime_generation") != generation
        or thread.get("agent_id") is not None
        or thread.get("runtime_attach_token") is not None
        or thread.get("control_admission_agent_id") is not None
        or thread.get("runtime_retirement_token") is not None
        or refusal is not None
        or backend not in LITE_BACKENDS
    ):
        raise ControlAdmissionError(VM_UPGRADE_NOT_POSSIBLE)


def _require_quiet_queue(queue: Mapping[str, Any] | None) -> None:
    if queue is None:
        return
    if queue["unit_kind"] != UNIT_KIND_SESSION_TURN:
        raise ControlAdmissionError("Session queue identity is incompatible")
    state = str(queue["state"] or "")
    if state == STATE_LEASED:
        raise ControlAdmissionNotReady(VM_UPGRADE_TURN_RUNNING)
    if state == STATE_PARKED:
        raise ControlAdmissionError(_QUEUE_PARKED)
    if state not in (STATE_QUEUED, STATE_DONE):
        raise ControlAdmissionError("Session queue state is unavailable")
    input_seq, consumed_seq = queue["input_seq"], queue["consumed_seq"]
    if input_seq is not None and (
        consumed_seq is None or int(input_seq) > int(consumed_seq)
    ):
        raise ControlAdmissionNotReady(VM_UPGRADE_INPUT_PENDING)
    if int(queue["control_input_seq"] or 0) > int(queue["control_consumed_seq"] or 0):
        raise ControlAdmissionNotReady(VM_UPGRADE_BACKGROUND_WORK)


async def begin_stateless_vm_upgrade_handoff(
    db: Any,
    *,
    thread_id: UUID | str,
    owner_user_id: UUID | str | None,
    client_request_id: UUID,
    payload: dict[str, Any],
    requested_by: str,
) -> AdmittedControl:
    """Switch one idle stateless lite Session to the pinned lane (spec §2)."""
    tid = UUID(str(thread_id))
    uid = UUID(str(owner_user_id)) if owner_user_id is not None else None
    generation = _payload_generation(payload)
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))

    async with db.acquire() as conn:
        async with conn.transaction():
            thread = await conn.fetchrow(_THREAD_SQL, tid)
            if thread is None or thread["user_id"] != uid:
                raise ControlAdmissionError("Thread is unavailable")
            existing = await conn.fetchrow(_EXISTING_SQL, tid, client_request_id)
            if existing is not None:
                if (
                    existing["verb"] != WORKSPACE_UPGRADE_VERB
                    or metadata_object(existing["payload"]) != payload
                ):
                    raise ControlAdmissionError(
                        "client_request_id was already used for a different control"
                    )
                return AdmittedControl(
                    id=existing["id"],
                    request_seq=int(existing["request_seq"]),
                    client_request_id=existing["client_request_id"],
                    verb=WORKSPACE_UPGRADE_VERB,
                    state=str(existing["outcome"] or "pending"),
                    duplicate=True,
                    runtime_generation=existing["runtime_generation"],
                )
            require_switchable_row(thread, owner_user_id=uid, generation=generation)
            queue = await conn.fetchrow(_QUEUE_SQL, tid)
            _require_quiet_queue(queue)
            pending = await conn.fetchrow(_PENDING_CONTROL_SQL, tid)
            if pending is not None:
                if pending["stranded"]:
                    raise ControlAdmissionError(_STRANDED_PINNED_CONTROL)
                raise ControlAdmissionNotReady(VM_UPGRADE_BACKGROUND_WORK)
            if await conn.fetchval(_BACKGROUND_WORK_SQL, tid):
                raise ControlAdmissionNotReady(VM_UPGRADE_BACKGROUND_WORK)
            if await conn.fetchval(_AGENT_ROW_SQL, tid):
                raise ControlAdmissionError(VM_UPGRADE_NOT_POSSIBLE)

            request_seq = int(thread["control_seq_hwm"] or 0) + 1
            if await conn.fetchval(_BUMP_HWM_SQL, tid, request_seq, generation) is None:
                raise ControlAdmissionError(VM_UPGRADE_NOT_POSSIBLE)
            request_id = await conn.fetchval(
                _INSERT_REQUEST_SQL,
                tid,
                request_seq,
                client_request_id,
                WORKSPACE_UPGRADE_VERB,
                payload_json,
                requested_by,
                generation,
            )
            if queue is not None and (
                await conn.fetchval(
                    _CLOSE_QUEUE_SQL, tid, int(queue["lease_token"] or 0)
                )
                is None
            ):
                raise ControlAdmissionError(VM_UPGRADE_NOT_POSSIBLE)
            await bump_epoch(conn, thread_id=str(tid))
            await append_system_frame(
                conn,
                thread_id=str(tid),
                kind=LANE_CHANGED_FRAME,
                payload={
                    "from": "stateless",
                    "to": "pinned",
                    "reason": "vm_upgrade",
                    "control_request_id": str(request_id),
                    "client_request_id": str(client_request_id),
                    "session_runtime_generation": str(generation),
                },
            )
            if (
                await conn.fetchval(_FLIP_LANE_SQL, tid, generation, ACTIVE_CLAIM_KEY)
                is None
            ):
                raise ControlAdmissionError(VM_UPGRADE_NOT_POSSIBLE)

    return AdmittedControl(
        id=request_id,
        request_seq=request_seq,
        client_request_id=client_request_id,
        verb=WORKSPACE_UPGRADE_VERB,
        state="pending",
        duplicate=False,
        runtime_generation=generation,
    )


__all__ = [
    "VM_UPGRADE_BACKGROUND_WORK",
    "VM_UPGRADE_INPUT_PENDING",
    "VM_UPGRADE_SESSION_ENDING",
    "VM_UPGRADE_TURN_RUNNING",
    "begin_stateless_vm_upgrade_handoff",
    "require_switchable_row",
]
