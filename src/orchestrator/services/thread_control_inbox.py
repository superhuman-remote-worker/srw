"""Transactional admission for the durable session-control inbox.

The browser supplies a client request UUID. Admission serializes on the thread
row, making ``request_seq`` commit ordered, inserts the request, and wakes the
stateless queue in one transaction. Admission deliberately does not persist the
desired scalar: doing so would let a REST snapshot expose a control before the
serving owner applied and journalled it. The owner makes the scalar visible only
while finalizing its durable journal receipt. Workspace undo is a
stateless-sandbox-only effect: admission requires an idle queue so it cannot
race a live turn, and the claimant records an idempotency marker in Git before
writing the same owner-fenced journal receipt. Manual ``compact`` follows
the same idle-only rule: the claimant summarizes the attached conversation
and its fenced checkpoint row plus journal receipt are the effect.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from shared.run_queue import (
    LANE_PINNED,
    LANE_STATELESS,
    STATE_DONE,
    STATE_LEASED,
    STATE_PARKED,
    STATE_QUEUED,
    UNIT_KIND_SESSION_TURN,
    record_control_seq,
)
from orchestrator.services.stateless_workspace_gate import (
    stateless_session_workspace_check,
)


@dataclass(frozen=True, slots=True)
class AdmittedControl:
    id: UUID
    request_seq: int
    client_request_id: UUID
    verb: str
    state: str
    duplicate: bool
    runtime_generation: UUID | None = None


class ControlAdmissionError(RuntimeError):
    """A safe, public-facing admission refusal."""


class ControlAdmissionNotReady(ControlAdmissionError):
    """The exact serving owner is not ready yet; a same-UUID retry is safe."""


WORKSPACE_UNDO_VERB = "workspace.undo"
COMPACT_VERB = "compact"
#: Effectful stateless-only controls: they change the workspace tree or the
#: conversation the next turn runs on, so they are admitted only while no turn
#: holds the lease and no committed human input is waiting ahead of them.
IDLE_ONLY_VERBS = frozenset({WORKSPACE_UNDO_VERB, COMPACT_VERB})
_COMPACT_PAYLOAD_KEYS = frozenset({"focus", "boundary_message_id"})


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


async def find_existing_thread_control(
    db: Any,
    *,
    thread_id: UUID | str,
    owner_user_id: UUID | str | None,
    client_request_id: UUID,
    verb: str,
    payload: dict[str, Any],
) -> AdmittedControl | None:
    """Return a matching committed request without rerunning mutable policy.

    This read is an idempotency preflight only. ``admit_thread_control`` still
    performs the locked ownership check before returning the duplicate. A
    UUID reused for different content fails loudly instead of disclosing the
    original request.
    """

    tid = UUID(str(thread_id))
    uid = UUID(str(owner_user_id)) if owner_user_id is not None else None
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT request.id, request.request_seq, "
            "request.client_request_id, request.verb, request.payload, "
            "request.outcome, request.runtime_generation "
            "FROM thread_control_requests request "
            "JOIN threads thread ON thread.id = request.thread_id "
            "WHERE request.thread_id = $1 "
            "AND request.client_request_id = $2 "
            "AND thread.user_id IS NOT DISTINCT FROM $3::uuid",
            tid,
            client_request_id,
            uid,
        )
    if row is None:
        return None
    if row["verb"] != verb or _json_object(row["payload"]) != payload:
        raise ControlAdmissionError(
            "client_request_id was already used for a different control"
        )
    return AdmittedControl(
        id=row["id"],
        request_seq=int(row["request_seq"]),
        client_request_id=row["client_request_id"],
        verb=str(row["verb"]),
        state=str(row["outcome"] or "pending"),
        duplicate=True,
        runtime_generation=row.get("runtime_generation"),
    )


async def admit_thread_control(
    db: Any,
    *,
    thread_id: UUID | str,
    owner_user_id: UUID | str | None,
    client_request_id: UUID,
    verb: str,
    payload: dict[str, Any],
    requested_by: str,
    expected_runtime_generation: UUID | str | None = None,
    require_pinned_runtime_generation: bool = False,
) -> AdmittedControl:
    """Admit one already-authorized session-control request.

    The caller owns schema validation and the permission-mode grant check. This
    function owns the atomicity/fencing boundary and rechecks owner, lane,
    lifecycle and reciprocal pinned binding under the thread row lock.
    ``workspace.undo`` is deliberately absent from the pinned REST lane: the
    existing live WebSocket behavior remains authoritative there.
    """
    tid = UUID(str(thread_id))
    uid = UUID(str(owner_user_id)) if owner_user_id is not None else None
    expected_generation = (
        UUID(str(expected_runtime_generation))
        if expected_runtime_generation is not None
        else None
    )
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))

    async with db.acquire() as conn:
        async with conn.transaction():
            thread = await conn.fetchrow(
                "SELECT id, user_id, agent_id, status, execution_lane, "
                "       control_seq_hwm, control_admission_agent_id, metadata, "
                "       runtime_generation, runtime_retirement_token, "
                "       runtime_attach_token "
                "FROM threads WHERE id = $1 FOR UPDATE",
                tid,
            )
            if thread is None or thread["user_id"] != uid:
                raise ControlAdmissionError("Thread is unavailable")

            existing = await conn.fetchrow(
                "SELECT id, request_seq, client_request_id, verb, payload, "
                "       outcome, runtime_generation "
                "FROM thread_control_requests "
                "WHERE thread_id = $1 AND client_request_id = $2",
                tid,
                client_request_id,
            )
            if existing is not None:
                if (
                    existing["verb"] != verb
                    or _json_object(existing["payload"]) != payload
                ):
                    raise ControlAdmissionError(
                        "client_request_id was already used for a different control"
                    )
                if (
                    expected_generation is not None
                    and existing.get("runtime_generation") != expected_generation
                ):
                    raise ControlAdmissionError(
                        "client_request_id belongs to a different session runtime"
                    )
                return AdmittedControl(
                    id=existing["id"],
                    request_seq=int(existing["request_seq"]),
                    client_request_id=existing["client_request_id"],
                    verb=str(existing["verb"]),
                    state=str(existing["outcome"] or "pending"),
                    duplicate=True,
                    runtime_generation=existing.get("runtime_generation"),
                )

            if thread["status"] not in {"created", "active", "awaiting_user"}:
                raise ControlAdmissionError(
                    "Session is not currently able to consume controls"
                )
            if verb == WORKSPACE_UNDO_VERB and payload != {}:
                # Route validation is not an authorization boundary. Keeping
                # this effect payload empty also makes same-UUID equivalence
                # exact and leaves the Git marker as its only mutable input.
                raise ControlAdmissionError(
                    "workspace.undo does not accept a control payload"
                )
            if verb == COMPACT_VERB and (
                not set(payload) <= _COMPACT_PAYLOAD_KEYS
                or not all(
                    isinstance(value, str) and value for value in payload.values()
                )
            ):
                raise ControlAdmissionError(
                    "compact accepts only a non-empty focus and boundary_message_id"
                )

            lane = str(thread["execution_lane"] or "")
            current_generation = thread.get("runtime_generation")
            if thread.get("runtime_retirement_token") is not None:
                raise ControlAdmissionError(
                    "Session runtime retirement is still in progress"
                )
            if (
                expected_generation is not None
                and expected_generation != current_generation
            ):
                raise ControlAdmissionError(
                    "Session runtime changed before control admission"
                )
            if lane == LANE_PINNED:
                if require_pinned_runtime_generation and expected_generation is None:
                    raise ControlAdmissionError(
                        "Pinned controls require the current session runtime generation"
                    )
            if verb == WORKSPACE_UNDO_VERB and lane != LANE_STATELESS:
                raise ControlAdmissionError(
                    "Workspace undo uses the live session transport on the "
                    "pinned execution lane"
                )
            if verb == COMPACT_VERB and lane != LANE_STATELESS:
                raise ControlAdmissionError(
                    "Compaction uses the live session transport on the pinned "
                    "execution lane"
                )
            accepted_agent_id: UUID | None
            if lane == LANE_PINNED:
                accepted_agent_id = thread["agent_id"]
                if accepted_agent_id is None:
                    raise ControlAdmissionNotReady(
                        "Session is not ready to accept controls"
                    )
                if thread["control_admission_agent_id"] != accepted_agent_id:
                    raise ControlAdmissionNotReady(
                        "Session is not ready to accept controls"
                    )
                reciprocal = await conn.fetchval(
                    "SELECT 1 FROM agents WHERE id = $1 AND thread_id = $2 FOR SHARE",
                    accepted_agent_id,
                    tid,
                )
                if reciprocal is None:
                    raise ControlAdmissionNotReady(
                        "Session is not ready to accept controls"
                    )
            elif lane == LANE_STATELESS:
                backend, workspace_refusal = stateless_session_workspace_check(
                    dict(thread)
                )
                if workspace_refusal is not None:
                    raise ControlAdmissionError(
                        "Stateless execution does not support this session's "
                        f"workspace binding ({workspace_refusal})"
                    )
                if thread["agent_id"] is not None:
                    raise ControlAdmissionError(
                        "Stateless session has an incompatible agent binding"
                    )
                if verb == WORKSPACE_UNDO_VERB and backend != "sandbox":
                    raise ControlAdmissionError(
                        "Workspace undo is supported only for sandbox sessions; "
                        "this workspace tier fails closed"
                    )
                accepted_agent_id = None
                queue = await conn.fetchrow(
                    "SELECT unit_kind, state, input_seq, consumed_seq "
                    "FROM run_queue "
                    "WHERE unit_id = $1 FOR UPDATE",
                    tid,
                )
                queue_state = str(queue["state"]) if queue is not None else None
                if queue is not None and queue["unit_kind"] != UNIT_KIND_SESSION_TURN:
                    raise ControlAdmissionError(
                        "Session queue identity is incompatible"
                    )
                if queue_state == STATE_PARKED:
                    raise ControlAdmissionError(
                        "Session queue is parked; unpark it before sending controls"
                    )
                effect_label = (
                    "workspace undo" if verb == WORKSPACE_UNDO_VERB else "compaction"
                )
                if verb in IDLE_ONLY_VERBS and queue_state == STATE_LEASED:
                    # Unlike scalar controls, undo changes the workspace tree
                    # and compact rewrites the conversation the turn runs on.
                    # Never wake the mid-turn watcher and race an active turn;
                    # 425 directs the caller to retry this UUID once the current
                    # owner has released the turn lease.
                    raise ControlAdmissionNotReady(
                        f"Session is completing a turn; retry {effect_label}"
                    )
                if verb in IDLE_ONLY_VERBS and queue is not None:
                    input_seq = queue["input_seq"]
                    consumed_seq = queue["consumed_seq"]
                    if input_seq is not None and (
                        consumed_seq is None or int(input_seq) > int(consumed_seq)
                    ):
                        # Controls drain before the human turn.  An effectful
                        # undo admitted here would otherwise overtake an input
                        # that was committed first.  A queued row with equal
                        # watermarks is control-only and remains admissible.
                        raise ControlAdmissionNotReady(
                            f"Session has pending input; retry {effect_label} "
                            "after that turn completes"
                        )
                if queue_state not in {
                    None,
                    STATE_QUEUED,
                    STATE_LEASED,
                    STATE_DONE,
                }:
                    raise ControlAdmissionError("Session queue state is unavailable")
                stranded_pinned = await conn.fetchval(
                    "SELECT 1 FROM thread_control_requests "
                    "WHERE thread_id = $1 AND outcome IS NULL "
                    "AND accepted_agent_id IS NOT NULL LIMIT 1",
                    tid,
                )
                if stranded_pinned is not None:
                    raise ControlAdmissionError(
                        "Session has a control awaiting its pinned owner"
                    )
            else:
                raise ControlAdmissionError("Session execution lane is unavailable")

            request_seq = int(thread["control_seq_hwm"] or 0) + 1
            if verb not in {
                "mode.set",
                "narration.set",
                WORKSPACE_UNDO_VERB,
                COMPACT_VERB,
            }:
                # Caller validation is not an authorization boundary.
                raise ControlAdmissionError("Unsupported control verb")
            await conn.execute(
                "UPDATE threads SET control_seq_hwm = $2, "
                "last_activity = now() WHERE id = $1 "
                "AND runtime_generation = $3::uuid "
                "AND runtime_retirement_token IS NULL",
                tid,
                request_seq,
                current_generation,
            )

            request_id = await conn.fetchval(
                "INSERT INTO thread_control_requests ("
                "thread_id, request_seq, client_request_id, verb, payload, "
                "requested_by, accepted_agent_id, runtime_generation"
                ") VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8) RETURNING id",
                tid,
                request_seq,
                client_request_id,
                verb,
                payload_json,
                requested_by,
                accepted_agent_id,
                current_generation,
            )

            state = "pending"
            if lane == LANE_STATELESS:
                baseline_input_seq = int(
                    await conn.fetchval(
                        "SELECT COALESCE(MAX(seq), 0) FROM thread_messages "
                        "WHERE thread_id = $1 AND role = 'human' "
                        "AND rewound_at IS NULL",
                        tid,
                    )
                    or 0
                )
                queue_state_after = await record_control_seq(
                    conn,
                    unit_id=tid,
                    unit_kind=UNIT_KIND_SESSION_TURN,
                    control_seq=request_seq,
                    baseline_input_seq=baseline_input_seq,
                    fair_key=str(uid),
                )
                if queue_state_after == STATE_PARKED:
                    # Defensive against a state change between the earlier read
                    # and helper call (the same row lock should make it
                    # impossible). Raising keeps the request + watermark one
                    # all-or-nothing commit.
                    raise ControlAdmissionError(
                        "Session queue is parked; unpark it before sending controls"
                    )

            return AdmittedControl(
                id=request_id,
                request_seq=request_seq,
                client_request_id=client_request_id,
                verb=verb,
                state=state,
                duplicate=False,
                runtime_generation=current_generation,
            )
