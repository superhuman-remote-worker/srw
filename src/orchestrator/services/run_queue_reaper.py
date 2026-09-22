"""Leader-gated run_queue lease reaper (stateless-agents S1, doc §5.2).

Steals expired leases via the shared per-row CAS in ``src/shared/run_queue``
and, for session units, writes the post-steal journal signal so the user never
gets silent dead air: one transaction per stolen unit bumps
``threads.events_epoch`` (fencing any zombie journal writer out — the client
re-anchors via ``gone_beyond_horizon``), reconciles recoverable interrupt rows
that belong to the stolen token, and appends a ``turn.interrupted`` /
``turn.parked`` system frame. An unreceipted admitted interrupt is durable stop
intent: owner loss settles it as ``applied``/``hard`` without signalling a
successor's RAM, consumes its exact human input, and writes a linked ack. An
already committed receipt is authoritative and is terminalized without
duplication. Post-steal is a
sanctioned writer-exclusion context for ``append_system_frame``: the steal's
``lease_token`` bump already fenced the previous holder's persists, and the
epoch bump retires its in-memory seq counter (knowledge-base/knowledge/features/stateless_agents.md
§5.3.2, system-writer class).

Leadership is a session-scoped advisory lock (``RUN_QUEUE_REAPER_ID``,
``src/orchestrator/database/lock_ids.py``) held on the same pooled connection the
sweep runs on, mirroring ``services/leader_election.py``'s shape: lock dies
with the session, a follower takes over within ~one interval. The lock only
avoids duplicate sweep WORK — correctness under the documented dual-leader
window comes from ``reap_expired``'s per-row CAS (each steal is returned to
exactly one caller, and the journal write is keyed off that returned record).

Error containment mirrors ``thread_events_prune_sweeper``: per-row failures
are logged and skipped, per-cycle failures drop leadership and re-contend,
and the loop itself never dies before shutdown.

Non-session unit kinds (``worker_batch``, ``bg_task``) are reaped only — no
journal writes; workers have no ``thread_events`` journal until S3 (doc §5.5).
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from orchestrator.database.lock_ids import RUN_QUEUE_REAPER_ID
from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from shared.workspace_recovery import workspace_recovery_enabled
from shared.run_queue.queries import _REAP_STEAL_SQL
from shared.event_journal import append_system_frame, bump_epoch
from shared.run_queue import (
    REAPER_GRACE_SECONDS,
    REAPER_INTERVAL_SECONDS,
    STATE_QUEUED,
    UNIT_KIND_SESSION_TURN,
    StolenUnit,
    reap_expired,
)
from shared.session_retirement import (
    ACTIVE_CLAIM_KEY,
    CLAIM_LOSS_HOLD_KEY,
    CLAIM_LOSS_LEDGER_KEY,
    ClaimantAuthority,
    active_claim_authority,
    unresolved_claim_losses,
)
from shared.thread_interrupts import (
    consume_applied_interrupt_input_idle,
    interrupt_receipt_result,
)

from shared.session_permission_retirement import retire_stale_stateless_permissions

logger = logging.getLogger(__name__)


STALE_INTERRUPT_RETRY_MAX_THREADS = 25
STALE_PERMISSION_RETRY_MAX_THREADS = 25
CLAIM_LOSS_RECONCILE_MAX_THREADS = 25
RETAINED_EXECUTOR_RELEASE_MAX_PODS = 50

# (thread, token, pod uid) debts already reported as absent-without-proof, so a
# wedged hold is loud once per orchestrator process rather than every tick.
_ABSENT_CLAIMANT_ANOMALIES: set[tuple[str, int, str]] = set()
_ABSENT_CLAIMANT_ANOMALY_LIMIT = 4096


class JournalStealResult(str, Enum):
    """Typed outcome of the post-steal system-writer transaction."""

    WRITTEN = "written"
    SKIPPED_SUCCESSOR_CLAIMED = "skipped_successor_claimed"
    SKIPPED_QUEUE_CHANGED = "skipped_queue_changed"


@dataclass(frozen=True, slots=True)
class InterruptReconcileResult:
    """Durable rows settled by one exact writer-fenced transaction."""

    count: int
    target_turn_ids: tuple[int, ...]
    request_ids: tuple[str, ...]
    settled_queue_state: str | None = None
    requests_by_target: tuple[tuple[int, tuple[str, ...]], ...] = ()


_LOCK_THREAD_SQL = """
SELECT execution_lane, agent_id, metadata
FROM threads
WHERE id = $1::uuid
FOR UPDATE
"""

_LOCK_QUEUE_UNIT_SQL = """
SELECT unit_kind, state, lease_token, leased_by, last_leased_by,
       leased_until, max_attempts, attempts_since_completion,
       queued_at, run_after,
       input_seq, consumed_seq,
       control_input_seq, control_consumed_seq
       , interrupt_admission_lease_token, interrupt_admission_turn_id
FROM run_queue
WHERE unit_id = $1::uuid
FOR UPDATE
"""


_STEAL_LOCKED_SESSION_SQL = """
UPDATE run_queue SET
    lease_token = lease_token + 1,
    state = CASE WHEN attempts_since_completion >= max_attempts
                 THEN 'parked' ELSE 'queued' END,
    park_reason = CASE WHEN attempts_since_completion >= max_attempts
                       THEN 'reaper_max_attempts' ELSE park_reason END,
    parked_at = CASE WHEN attempts_since_completion >= max_attempts
                     THEN now() ELSE parked_at END,
    leased_by = NULL,
    last_leased_by = NULL,
    leased_until = NULL,
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    queued_at = now(),
    run_after = now() + make_interval(secs => $3::float8)
WHERE unit_id = $1::uuid
  AND unit_kind = 'session_turn'
  AND state = 'leased'
  AND lease_token = $2::bigint
  AND leased_until < now() - make_interval(secs => $4::float8)
RETURNING unit_id, unit_kind, state, attempts_since_completion, lease_token
"""


_STORE_CLAIM_LOSS_HOLD_SQL = """
UPDATE threads
SET metadata = $2::jsonb
WHERE id = $1::uuid
  AND execution_lane = 'stateless'
  AND agent_id IS NULL
RETURNING id
"""


_PARK_CLAIM_LOSS_HOLD_SQL = """
UPDATE run_queue
SET state = 'parked',
    park_reason = 'claim_loss_hold',
    parked_at = now(),
    leased_by = NULL, last_leased_by = NULL, leased_until = NULL
WHERE unit_id = $1::uuid
  AND unit_kind = 'session_turn'
  AND lease_token = $2::bigint
  AND leased_by IS NULL
  AND state IN ('queued', 'parked', 'done')
RETURNING unit_id
"""


_PENDING_STOLEN_INTERRUPTS_SQL = """
SELECT request.id, request.client_request_id, request.target_turn_id,
       request.accepted_lease_token, request.accepted_leased_by,
       request.outcome AS request_outcome, request.result AS request_result,
       receipt.epoch AS receipt_epoch, receipt.seq AS receipt_seq,
       receipt.kind AS receipt_kind, receipt.payload AS receipt_payload
FROM thread_interrupt_requests request
LEFT JOIN thread_events receipt
  ON receipt.thread_id = request.thread_id
 AND receipt.interrupt_request_id = request.id
WHERE request.thread_id = $1::uuid
  AND request.accepted_lease_token = $2::bigint
  AND (request.outcome IS NULL
       OR (request.outcome = 'applied'
           AND NOT (COALESCE(request.result, '{}'::jsonb)
                    ? 'consumed_input_seq')))
ORDER BY request.requested_at, request.id
FOR UPDATE OF request
"""

_STALE_INTERRUPT_RETRY_CANDIDATES_SQL = """
SELECT request.thread_id, min(request.requested_at) AS oldest_request
FROM thread_interrupt_requests request
JOIN threads thread ON thread.id = request.thread_id
JOIN run_queue queue ON queue.unit_id = request.thread_id
WHERE (request.outcome IS NULL
       OR (request.outcome = 'applied'
           AND NOT (COALESCE(request.result, '{}'::jsonb)
                    ? 'consumed_input_seq')))
  AND thread.execution_lane = 'stateless'
  AND thread.agent_id IS NULL
  AND NOT (COALESCE(thread.metadata, '{}'::jsonb)
           ? '_stateless_claim_losses')
  AND queue.unit_kind = 'session_turn'
  AND queue.lease_token > request.accepted_lease_token
  AND queue.leased_by IS NULL
  AND queue.state = 'parked'
GROUP BY request.thread_id
ORDER BY min(request.requested_at), request.thread_id
LIMIT $1::integer
"""

_DONE_PERMISSION_RECOVERY_PROOF = """
EXISTS (
    SELECT 1
    FROM thread_interrupt_requests proof_request
    JOIN thread_events proof_receipt
      ON proof_receipt.thread_id = proof_request.thread_id
     AND proof_receipt.interrupt_request_id = proof_request.id
     AND proof_receipt.epoch = proof_request.journal_epoch
     AND proof_receipt.seq = proof_request.journal_seq
    WHERE proof_request.thread_id = request.thread_id
      AND proof_request.outcome = 'applied'
      AND proof_request.accepted_lease_token > 0
      AND proof_request.accepted_lease_token = queue.lease_token - 1
      AND proof_request.applied_lease_token =
          proof_request.accepted_lease_token
      AND proof_request.result->>'input_settlement' = 'lease_recovery'
      AND proof_request.result->'input_settled_by_lease_token' =
          to_jsonb(queue.lease_token)
      AND proof_receipt.kind = 'interrupt.ack'
      AND proof_receipt.payload @> jsonb_build_object(
            'request_id', proof_request.id::text,
            'client_request_id', proof_request.client_request_id::text,
            'target_turn_id', proof_request.target_turn_id,
            'applied', TRUE
          )
      AND proof_receipt.payload->>'mode' IN ('hard', 'graceful')
)
"""

_STALE_PERMISSION_RETRY_CANDIDATES_SQL = f"""
SELECT request.thread_id, min(request.requested_at) AS oldest_request
FROM thread_permission_requests request
JOIN threads thread ON thread.id = request.thread_id
JOIN run_queue queue ON queue.unit_id = request.thread_id
WHERE request.status = 'pending'
  AND (request.accepted_lease_token IS NULL
       OR (request.accepted_lease_token > 0
           AND request.accepted_lease_token < queue.lease_token))
  AND thread.execution_lane = 'stateless'
  AND thread.agent_id IS NULL
  AND queue.unit_kind = 'session_turn'
  AND queue.lease_token > 0
  AND (queue.state = 'parked'
       OR (queue.state = 'done'
           AND {_DONE_PERMISSION_RECOVERY_PROOF}))
  AND queue.leased_by IS NULL
  AND queue.last_leased_by IS NULL
  AND NOT (COALESCE(thread.metadata, '{{}}'::jsonb)
           ? '_stateless_active_claim')
  AND NOT (COALESCE(thread.metadata, '{{}}'::jsonb)
           ? '_stateless_claim_losses')
  AND NOT (COALESCE(thread.metadata, '{{}}'::jsonb)
           ? '_stateless_claim_loss_hold')
GROUP BY request.thread_id
ORDER BY min(request.requested_at), request.thread_id
LIMIT $1::integer
"""

_LOCK_DONE_PERMISSION_RECOVERY_PROOF_SQL = """
SELECT proof_request.id
FROM thread_interrupt_requests proof_request
JOIN thread_events proof_receipt
  ON proof_receipt.thread_id = proof_request.thread_id
 AND proof_receipt.interrupt_request_id = proof_request.id
 AND proof_receipt.epoch = proof_request.journal_epoch
 AND proof_receipt.seq = proof_request.journal_seq
WHERE proof_request.thread_id = $1::uuid
  AND proof_request.outcome = 'applied'
  AND proof_request.accepted_lease_token > 0
  AND proof_request.accepted_lease_token = $2::bigint - 1
  AND proof_request.applied_lease_token = proof_request.accepted_lease_token
  AND proof_request.result->>'input_settlement' = 'lease_recovery'
  AND proof_request.result->'input_settled_by_lease_token' =
      to_jsonb($2::bigint)
  AND proof_receipt.kind = 'interrupt.ack'
  AND proof_receipt.payload @> jsonb_build_object(
        'request_id', proof_request.id::text,
        'client_request_id', proof_request.client_request_id::text,
        'target_turn_id', proof_request.target_turn_id,
        'applied', TRUE
      )
  AND proof_receipt.payload->>'mode' IN ('hard', 'graceful')
ORDER BY proof_request.accepted_lease_token, proof_request.requested_at,
         proof_request.id
LIMIT 1
FOR SHARE OF proof_request, proof_receipt
"""

_PENDING_STALE_INTERRUPT_RETRY_SQL = """
SELECT request.id, request.client_request_id, request.target_turn_id,
       request.accepted_lease_token, request.accepted_leased_by,
       request.outcome AS request_outcome, request.result AS request_result,
       receipt.epoch AS receipt_epoch, receipt.seq AS receipt_seq,
       receipt.kind AS receipt_kind, receipt.payload AS receipt_payload
FROM thread_interrupt_requests request
JOIN threads thread ON thread.id = request.thread_id
LEFT JOIN thread_events receipt
  ON receipt.thread_id = request.thread_id
 AND receipt.interrupt_request_id = request.id
WHERE request.thread_id = $1::uuid
  AND (request.outcome IS NULL
       OR (request.outcome = 'applied'
           AND NOT (COALESCE(request.result, '{}'::jsonb)
                    ? 'consumed_input_seq')))
  AND request.accepted_lease_token < $2::bigint
  AND thread.execution_lane = 'stateless'
  AND thread.agent_id IS NULL
ORDER BY request.requested_at, request.id
FOR UPDATE OF request
"""


_CLAIM_LOSS_HOLD_CANDIDATES_SQL = """
SELECT id, metadata
FROM threads
WHERE execution_lane = 'stateless'
  AND agent_id IS NULL
  AND COALESCE(metadata, '{}'::jsonb) ? '_stateless_claim_losses'
ORDER BY last_activity, id
LIMIT $1::integer
"""

# One statement, one snapshot. The steal and End convert "leased by pod" into
# "ledger names uid" atomically, so reading the two predicates in separate
# statements could observe neither. A malformed ledger that merely mentions the
# UID still blocks release: unparseable authority is never absence. Each source
# is scanned once however many candidates a rollout produces.
_UNREFERENCED_EXECUTOR_UIDS_SQL = """
WITH leased AS MATERIALIZED (
    SELECT DISTINCT leased_by
    FROM run_queue
    WHERE unit_kind = 'session_turn'
      AND leased_by IS NOT NULL
), ledgers AS MATERIALIZED (
    SELECT (metadata -> '_stateless_claim_losses')::text AS ledger
    FROM threads
    WHERE COALESCE(metadata, '{}'::jsonb) ? '_stateless_claim_losses'
)
SELECT candidate.pod_uid
FROM unnest($1::text[], $2::text[]) AS candidate(pod_uid, pod_name)
WHERE NOT EXISTS (
        SELECT 1 FROM leased WHERE leased.leased_by = candidate.pod_name
      )
  AND NOT EXISTS (
        SELECT 1 FROM ledgers WHERE strpos(ledgers.ledger, candidate.pod_uid) > 0
      )
"""

_TERMINALIZE_STOLEN_INTERRUPT_SQL = """
UPDATE thread_interrupt_requests
SET outcome = $4::text,
    result = $5::jsonb,
    applied_mode = $6::text,
    applied_at = now(),
    applied_lease_token = $3::bigint,
    journal_epoch = $7::integer,
    journal_seq = $8::bigint,
    acknowledged_at = now(),
    error_code = $9::text
WHERE id = $1::uuid
  AND thread_id = $2::uuid
  AND accepted_lease_token = $3::bigint
  AND outcome IS NULL
RETURNING id
"""


async def _sleep_or_shutdown(seconds: float, shutdown_event: asyncio.Event) -> None:
    """Sleep up to ``seconds``, waking immediately on shutdown."""
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return


def _receipt_payload(value: Any) -> dict[str, Any]:
    """Return one receipt payload as an object or fail the reconciliation."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("interrupt receipt payload is not valid JSON") from exc
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError("interrupt receipt payload is not an object")


async def _reconcile_stolen_interrupts(
    conn: Any,
    *,
    thread_id: str,
    unit: StolenUnit,
) -> InterruptReconcileResult:
    """Settle recoverable requests owned by the lease that was just stolen.

    The run_queue CAS has already bumped the token before this function is
    reachable. A missing old token means an older/mock ``StolenUnit`` that
    cannot prove which inbox generation it retired, so it is left untouched.
    A non-consecutive token pair is a corrupt boundary and fails the enclosing
    transaction rather than rejecting somebody else's requests.

    A valid pre-existing receipt wins: the former owner may have committed its
    journal frame and crashed before finalization. Otherwise exact-turn
    admission itself is authoritative stop intent: the post-steal writer emits
    a correlated ``applied``/``hard`` ack, consumes that target input, and never
    signals a successor's RAM. Every terminal update uses the immutable old
    token and exact event cursor.
    """
    previous_token = unit.previous_lease_token
    if previous_token is None:
        return InterruptReconcileResult(0, (), ())
    previous_token = int(previous_token)
    if previous_token <= 0 or int(unit.lease_token) != previous_token + 1:
        raise RuntimeError(
            "stolen session unit has a non-consecutive lease-token boundary: "
            f"{previous_token}->{unit.lease_token}"
        )

    pending = await conn.fetch(
        _PENDING_STOLEN_INTERRUPTS_SQL,
        thread_id,
        previous_token,
    )
    return await _reconcile_interrupt_rows(
        conn,
        thread_id=thread_id,
        current_lease_token=int(unit.lease_token),
        pending=pending,
    )


async def _reconcile_interrupt_rows(
    conn: Any,
    *,
    thread_id: str,
    current_lease_token: int,
    pending: Any,
    owner_loss_reason: str = "lease_expired",
    terminal_queue: bool = False,
) -> InterruptReconcileResult:
    """Reconcile already-locked request rows from their durable receipts."""
    reconciled = 0
    target_turn_ids: list[int] = []
    request_ids: list[str] = []
    requests_by_target: dict[int, list[str]] = {}
    applied_groups: list[tuple[int, int, str]] = []
    settled_queue_state: str | None = None
    for request in pending:
        request_id = request["id"]
        client_request_id = request["client_request_id"]
        target_turn_id = int(request["target_turn_id"])
        accepted_lease_token = int(request["accepted_lease_token"])
        request_outcome = request["request_outcome"]
        if request["receipt_epoch"] is not None:
            receipt_result = interrupt_receipt_result(
                request_id=request_id,
                client_request_id=client_request_id,
                target_turn_id=target_turn_id,
                event_kind=str(request["receipt_kind"]),
                event_payload=request["receipt_payload"],
            )
            if receipt_result is None:
                raise RuntimeError(
                    f"stolen interrupt has an invalid durable receipt: {request_id}"
                )
            outcome, mode, error_code = receipt_result
            payload = _receipt_payload(request["receipt_payload"])
            epoch = int(request["receipt_epoch"])
            seq = int(request["receipt_seq"])
            if request_outcome is not None and request_outcome != outcome:
                raise RuntimeError(
                    "terminal interrupt outcome disagrees with durable receipt: "
                    f"{request_id}"
                )
        else:
            if request_outcome is not None:
                raise RuntimeError(
                    f"terminal interrupt is missing its durable receipt: {request_id}"
                )
            outcome = "applied"
            mode = "hard"
            error_code = None
            payload = {
                "request_id": str(request_id),
                "client_request_id": str(client_request_id),
                "target_turn_id": target_turn_id,
                "applied": True,
                "mode": mode,
                "reason": "owner_lost",
                "owner_loss_reason": owner_loss_reason,
            }
            receipt = await append_system_frame(
                conn,
                thread_id=thread_id,
                kind="interrupt.ack",
                payload=payload,
                interrupt_request_id=str(request_id),
            )
            if receipt is None:
                raise RuntimeError(
                    f"thread disappeared while settling interrupt {request_id}"
                )
            epoch, seq = receipt

        if request_outcome is None:
            updated = await conn.fetchval(
                _TERMINALIZE_STOLEN_INTERRUPT_SQL,
                request_id,
                thread_id,
                accepted_lease_token,
                outcome,
                json.dumps(payload),
                mode,
                epoch,
                seq,
                error_code,
            )
            if updated is None:
                raise RuntimeError(
                    f"stolen interrupt terminalization lost exact row {request_id}"
                )
        if outcome == "applied":
            group = (accepted_lease_token, target_turn_id, str(request_id))
            if not any(group[:2] == existing[:2] for existing in applied_groups):
                applied_groups.append(group)
        reconciled += 1
        if target_turn_id not in target_turn_ids:
            target_turn_ids.append(target_turn_id)
        request_ids.append(str(request_id))
        requests_by_target.setdefault(target_turn_id, []).append(str(request_id))

    # Terminalize the whole locked batch first. The group helper then sees and
    # stamps every applied sibling before advancing exactly one human input.
    # Distinct old lease/turn groups are settled in durable request order; a
    # queued result exposes the next human input to the following group.
    for accepted_lease_token, target_turn_id, request_id in applied_groups:
        consumed = await consume_applied_interrupt_input_idle(
            conn,
            thread_id=thread_id,
            current_lease_token=int(current_lease_token),
            accepted_lease_token=accepted_lease_token,
            target_turn_id=target_turn_id,
            request_id=request_id,
            terminal=terminal_queue,
        )
        if consumed is None:
            raise RuntimeError(
                "applied stolen interrupt lost its exact idle queue owner: "
                f"{request_id}"
            )
        settled_queue_state = consumed.state
    return InterruptReconcileResult(
        reconciled,
        tuple(target_turn_ids),
        tuple(request_ids),
        settled_queue_state,
        tuple(
            (target_turn_id, tuple(ids))
            for target_turn_id, ids in requests_by_target.items()
        ),
    )


def _turn_frame_payload(
    *,
    attempts: int,
    stolen_from: str | None,
    interrupts: InterruptReconcileResult,
    fallback_target_turn_id: int | None = None,
    reason: str = "lease_expired",
) -> dict[str, Any]:
    """Correlate a steal frame with the exact interrupted turn when known."""
    payload: dict[str, Any] = {
        "reason": reason,
        "attempts": int(attempts),
        "stolen_from": stolen_from,
    }
    if len(interrupts.target_turn_ids) == 1:
        target_turn_id = interrupts.target_turn_ids[0]
        payload["target_turn_id"] = target_turn_id
        payload["turn_id"] = target_turn_id
    elif interrupts.target_turn_ids:
        payload["target_turn_ids"] = list(interrupts.target_turn_ids)
    elif fallback_target_turn_id is not None:
        target_turn_id = int(fallback_target_turn_id)
        payload["target_turn_id"] = target_turn_id
        payload["turn_id"] = target_turn_id
    if len(interrupts.request_ids) == 1:
        payload["interrupt_request_id"] = interrupts.request_ids[0]
    if interrupts.request_ids:
        payload["interrupt_request_ids"] = list(interrupts.request_ids)
    return payload


def _turn_frame_slices(
    interrupts: InterruptReconcileResult,
) -> tuple[InterruptReconcileResult, ...]:
    """One exact-target lifecycle slice per interrupted turn."""
    if not interrupts.requests_by_target:
        return (interrupts,)
    return tuple(
        InterruptReconcileResult(
            count=len(request_ids),
            target_turn_ids=(target_turn_id,),
            request_ids=request_ids,
            settled_queue_state=interrupts.settled_queue_state,
            requests_by_target=((target_turn_id, request_ids),),
        )
        for target_turn_id, request_ids in interrupts.requests_by_target
    )


async def _append_turn_frames(
    conn: Any,
    *,
    thread_id: str,
    kind: str,
    attempts: int,
    stolen_from: str | None,
    interrupts: InterruptReconcileResult,
    fallback_target_turn_id: int | None = None,
    reason: str = "lease_expired",
) -> None:
    """Append exact-target terminal frames, failing the enclosing transaction."""
    for frame_interrupts in _turn_frame_slices(interrupts):
        frame = await append_system_frame(
            conn,
            thread_id=thread_id,
            kind=kind,
            payload=_turn_frame_payload(
                attempts=attempts,
                stolen_from=stolen_from,
                interrupts=frame_interrupts,
                fallback_target_turn_id=fallback_target_turn_id,
                reason=reason,
            ),
        )
        if frame is None:
            raise RuntimeError(
                f"thread disappeared while journaling lifecycle {thread_id}"
            )


async def reconcile_terminal_interrupts(
    conn: Any,
    *,
    thread_id: str,
    previous_lease_token: int,
    terminal_lease_token: int,
    leased_by: str | None,
    attempts: int,
    active_turn_id: int | None = None,
    reason: str = "force_end",
    epoch_already_bumped: bool = False,
) -> InterruptReconcileResult:
    """Settle exact old-lease interrupts after public End steals ownership.

    The caller already holds ``threads -> run_queue`` locks in an explicit
    transaction and has moved the queue to a writer-free ``queued``/``parked``
    state at ``terminal_lease_token``. Reuse the same durable receipt recovery,
    hard-stop acknowledgement, and exact human-input settlement as the expiry
    reaper.  All unresolved generations below the terminal token are included:
    an earlier reaper may have published a parked token and then crashed before
    its journal transaction.

    When End fenced a turn with an open admission window, ``active_turn_id``
    is the durable identity of the input that may already have workspace side
    effects.  Settle that one exact human row even if no explicit interrupt
    request existed.  Inputs admitted after it remain pending for Resume.
    """

    old_token = int(previous_lease_token)
    terminal_token = int(terminal_lease_token)
    if old_token <= 0 or terminal_token != old_token + 1:
        raise ValueError("terminal interrupt recovery requires one exact token bump")
    if reason not in {"force_end", "user_end"}:
        raise ValueError("terminal interrupt reason is invalid")
    pending = await conn.fetch(
        _PENDING_STALE_INTERRUPT_RETRY_SQL,
        thread_id,
        terminal_token,
    )
    turn_id = int(active_turn_id) if active_turn_id is not None else None
    if turn_id is not None and turn_id <= 0:
        raise ValueError("active terminal turn id must be positive")
    if not pending and turn_id is None:
        return InterruptReconcileResult(0, (), ())

    # This is also the fence for an old claimant's journal allocator.  One
    # bump covers every stale receipt and the synthetic force-End frame in the
    # enclosing transaction.
    if not epoch_already_bumped:
        await bump_epoch(conn, thread_id=thread_id)
    interrupts = await _reconcile_interrupt_rows(
        conn,
        thread_id=thread_id,
        current_lease_token=terminal_token,
        pending=pending,
        owner_loss_reason=reason,
        terminal_queue=True,
    )
    if pending:
        await _append_turn_frames(
            conn,
            thread_id=thread_id,
            kind="turn.interrupted",
            attempts=int(attempts),
            stolen_from=leased_by,
            interrupts=interrupts,
            reason=reason,
        )

    if turn_id is not None:
        humans = await conn.fetch(
            "SELECT seq FROM thread_messages "
            "WHERE thread_id = $1::uuid AND role = 'human' "
            "AND rewound_at IS NULL AND turn_number = $2::integer "
            "ORDER BY seq FOR SHARE",
            thread_id,
            turn_id,
        )
        if len(humans) != 1:
            raise RuntimeError(
                "terminal active turn must map to exactly one live human input"
            )
        active_input_seq = int(humans[0]["seq"])
        queue = await conn.fetchrow(
            "SELECT consumed_seq FROM run_queue "
            "WHERE unit_id = $1::uuid AND unit_kind = 'session_turn' "
            "AND lease_token = $2::bigint AND state IN ('queued', 'parked') "
            "AND leased_by IS NULL AND leased_until IS NULL FOR UPDATE",
            thread_id,
            terminal_token,
        )
        if queue is None:
            raise RuntimeError("terminal active-input settlement lost queue owner")
        consumed = queue["consumed_seq"]
        needs_settlement = consumed is None or int(consumed) < active_input_seq
        if needs_settlement:
            next_unanswered = await conn.fetchval(
                "SELECT min(seq) FROM thread_messages "
                "WHERE thread_id = $1::uuid AND role = 'human' "
                "AND rewound_at IS NULL "
                "AND seq > COALESCE($2::bigint, -1)",
                thread_id,
                consumed,
            )
            if next_unanswered is None or int(next_unanswered) != active_input_seq:
                raise RuntimeError(
                    "terminal active turn is not the next unanswered human input"
                )
            updated = await conn.fetchval(
                "UPDATE run_queue "
                "SET consumed_seq = GREATEST(COALESCE(consumed_seq, -1), $3::bigint), "
                "    attempts_since_completion = 0 "
                "WHERE unit_id = $1::uuid AND unit_kind = 'session_turn' "
                "AND lease_token = $2::bigint AND state IN ('queued', 'parked') "
                "AND leased_by IS NULL AND leased_until IS NULL "
                "RETURNING consumed_seq",
                thread_id,
                terminal_token,
                active_input_seq,
            )
            if updated is None:
                raise RuntimeError("terminal active-input settlement lost its fence")
        if turn_id not in interrupts.target_turn_ids:
            # The active gate is lifecycle evidence even when a racing live
            # interrupt finalizer already consumed the target and therefore
            # left no unresolved request in ``pending``. End's epoch bump must
            # still publish the exact terminal edge.
            await _append_turn_frames(
                conn,
                thread_id=thread_id,
                kind="turn.interrupted",
                attempts=int(attempts),
                stolen_from=leased_by,
                interrupts=InterruptReconcileResult(0, (), ()),
                fallback_target_turn_id=turn_id,
                reason=reason,
            )
    return interrupts


def _post_steal_queue_result(
    thread: Any,
    queue: Any,
    unit: StolenUnit,
) -> JournalStealResult:
    """Validate the locked queue row before entering system-writer context."""
    if (
        queue is not None
        and str(queue["state"]) == "leased"
        and int(queue["lease_token"]) > int(unit.lease_token)
    ):
        return JournalStealResult.SKIPPED_SUCCESSOR_CLAIMED
    if (
        thread is None
        or str(thread["execution_lane"] or "") != "stateless"
        or thread["agent_id"] is not None
        or queue is None
        or str(queue["unit_kind"]) != UNIT_KIND_SESSION_TURN
        or str(queue["state"]) not in {"queued", "parked"}
        or str(queue["state"]) != str(unit.state)
        or int(queue["lease_token"]) != int(unit.lease_token)
        or queue["leased_by"] is not None
    ):
        return JournalStealResult.SKIPPED_QUEUE_CHANGED
    return JournalStealResult.WRITTEN


def _json_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _metadata_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return dict(parsed)
    raise RuntimeError("stateless thread metadata is malformed")


async def _steal_session_with_claim_loss(
    conn: Any,
    *,
    candidate: Any,
    backoff_seconds: float,
    grace_seconds: float,
) -> StolenUnit | None:
    """Atomically fence one expired session claimant and record its I/O debt.

    A queue token prevents new durable writes, but it cannot cancel a Paramiko
    SFTP call that already passed the old claimant's local admission lock.  The
    unresolved-only thread ledger therefore commits in the same
    ``threads -> run_queue`` transaction as the token bump.  End must drain
    every ledger entry before touching workspace bytes; the exact old claimant
    (or the reconciler observing its finalizer-retained Pod UID with every
    container terminated) removes its own entry later.
    """

    thread_id = str(candidate["unit_id"])
    previous_token = int(candidate["lease_token"])
    previous_owner = str(candidate["leased_by"] or "").strip()
    if previous_token <= 0 or not previous_owner:
        raise RuntimeError("expired session lease lacks exact claimant authority")

    async with conn.transaction():
        thread = await conn.fetchrow(_LOCK_THREAD_SQL, thread_id)
        if (
            thread is None
            or str(thread["execution_lane"] or "") != "stateless"
            or thread["agent_id"] is not None
        ):
            return None
        # Validate every already-unresolved generation and the credential-bound
        # claimant before advancing the queue.  A claim that never reached the
        # final bundle boundary has no workspace credentials and therefore
        # creates no local-I/O debt when fenced.
        losses = unresolved_claim_losses(thread["metadata"])
        if previous_token in losses:
            raise RuntimeError("expired session token already has claimant-loss debt")
        active_claim = active_claim_authority(thread["metadata"])
        current_authority: ClaimantAuthority | None = None
        if active_claim is not None:
            active_token, active_authority = active_claim
            if active_token > previous_token:
                raise RuntimeError("active claimant generation is ahead of its lease")
            if active_token == previous_token:
                if active_authority.pod != previous_owner:
                    raise RuntimeError("active claimant disagrees with queue owner")
                current_authority = active_authority

        queue = await conn.fetchrow(_LOCK_QUEUE_UNIT_SQL, thread_id)
        if (
            queue is None
            or str(queue["unit_kind"] or "") != UNIT_KIND_SESSION_TURN
            or str(queue["state"] or "") != "leased"
            or int(queue["lease_token"] or 0) != previous_token
            or str(queue["leased_by"] or "") != previous_owner
        ):
            return None

        updated = await conn.fetchrow(
            _STEAL_LOCKED_SESSION_SQL,
            thread_id,
            previous_token,
            float(backoff_seconds),
            float(grace_seconds),
        )
        if updated is None:
            # A heartbeat renewed the row after candidate discovery. No thread
            # metadata has changed, so the transaction may commit as a no-op.
            return None
        admission_turn = None
        if (
            queue["interrupt_admission_lease_token"] is not None
            and int(queue["interrupt_admission_lease_token"]) == previous_token
            and queue["interrupt_admission_turn_id"] is not None
        ):
            admission_turn = int(queue["interrupt_admission_turn_id"])
        unit = StolenUnit(
            unit_id=updated["unit_id"],
            unit_kind=updated["unit_kind"],
            state=updated["state"],
            attempts_since_completion=int(updated["attempts_since_completion"]),
            leased_by=previous_owner,
            lease_token=int(updated["lease_token"]),
            previous_lease_token=previous_token,
            interrupt_admission_turn_id=admission_turn,
            lifecycle_journaled=True,
        )
        # The epoch edge, interrupt receipts, input settlement, and claimant
        # loss provenance are one atomic owner-loss fact.  Publishing only the
        # queue bump would leave an unrecoverable claim-loss gap after a crash.
        await bump_epoch(conn, thread_id=thread_id)
        await retire_stale_stateless_permissions(
            conn,
            thread_id=thread_id,
            retired_lease_token=previous_token,
            successor_lease_token=int(unit.lease_token),
            reason="lease_expired",
            epoch_already_bumped=True,
        )
        interrupts = await _reconcile_stolen_interrupts(
            conn,
            thread_id=thread_id,
            unit=unit,
        )
        kind = (
            "turn.interrupted"
            if unit.state == STATE_QUEUED
            or interrupts.settled_queue_state in {"done", "queued"}
            else "turn.parked"
        )
        await _append_turn_frames(
            conn,
            thread_id=thread_id,
            kind=kind,
            attempts=unit.attempts_since_completion,
            stolen_from=unit.leased_by,
            interrupts=interrupts,
            fallback_target_turn_id=unit.interrupt_admission_turn_id,
        )

        # Interrupt settlement may have changed queued -> done (or preserved
        # max-attempt parked). Capture that authoritative post-settlement state
        # before installing the physical quiescence hold.
        settled_queue = await conn.fetchrow(_LOCK_QUEUE_UNIT_SQL, thread_id)
        if (
            settled_queue is None
            or int(settled_queue["lease_token"] or 0) != int(unit.lease_token)
            or settled_queue["leased_by"] is not None
            or str(settled_queue["state"] or "") not in {"queued", "parked", "done"}
        ):
            raise RuntimeError("session steal lost its post-settlement queue row")

        metadata = _metadata_object(thread["metadata"] or {})
        metadata.pop(ACTIVE_CLAIM_KEY, None)
        raw_ledger = dict(metadata.get(CLAIM_LOSS_LEDGER_KEY) or {})
        if current_authority is not None:
            raw_ledger[str(previous_token)] = {
                "pod": current_authority.pod,
                "pod_uid": current_authority.pod_uid,
                "quiesced": False,
            }
        if raw_ledger:
            metadata[CLAIM_LOSS_LEDGER_KEY] = raw_ledger
            metadata[CLAIM_LOSS_HOLD_KEY] = {
                "lease_token": int(unit.lease_token),
                "intended_state": str(settled_queue["state"]),
                "attempts_since_completion": int(
                    settled_queue["attempts_since_completion"] or 0
                ),
                "queued_at": _json_timestamp(settled_queue["queued_at"]),
                "run_after": _json_timestamp(settled_queue["run_after"]),
            }
        else:
            metadata.pop(CLAIM_LOSS_LEDGER_KEY, None)
            metadata.pop(CLAIM_LOSS_HOLD_KEY, None)

        stored = await conn.fetchval(
            _STORE_CLAIM_LOSS_HOLD_SQL,
            thread_id,
            json.dumps(metadata, sort_keys=True, separators=(",", ":")),
        )
        if stored is None:
            raise RuntimeError("session steal failed to store claimant authority")
        if raw_ledger:
            held = await conn.fetchrow(
                _PARK_CLAIM_LOSS_HOLD_SQL,
                thread_id,
                int(unit.lease_token),
            )
            if held is None:
                raise RuntimeError("session steal failed to park claimant-loss debt")
        return unit


async def _try_steal_session_with_claim_loss(
    conn: Any,
    *,
    candidate: Any,
    backoff_seconds: float,
    grace_seconds: float,
) -> StolenUnit | None:
    """Contain one corrupt/session-journal failure without aborting the pass."""

    try:
        return await _steal_session_with_claim_loss(
            conn,
            candidate=candidate,
            backoff_seconds=backoff_seconds,
            grace_seconds=grace_seconds,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "run_queue reaper: atomic session steal failed for unit %s "
            "(contained; lease left unchanged)",
            candidate["unit_id"],
        )
        return None


async def _journal_steal(conn: Any, unit: StolenUnit) -> JournalStealResult:
    """Post-steal journal write for ONE session unit — one transaction.

    ``bump_epoch`` first (fences the zombie's journal writer and resets the
    seq high-water mark), then stale interrupt reconciliation, then the turn
    frame. All share one transaction so a crash cannot expose a partial set of
    receipts or a bumped epoch with no explanatory frame. ``unit.state`` is
    the post-steal state:
    ``'queued'`` → the turn will be retried (``turn.interrupted``);
    ``'parked'`` → attempts exhausted, operator unpark required
    (``turn.parked``).
    """
    thread_id = str(unit.unit_id)
    async with conn.transaction():
        # Global per-thread lock order is threads -> run_queue. REST interrupt
        # admission uses the same order, so neither side can form a lock cycle.
        thread = await conn.fetchrow(_LOCK_THREAD_SQL, thread_id)
        queue = await conn.fetchrow(_LOCK_QUEUE_UNIT_SQL, thread_id)
        queue_result = _post_steal_queue_result(thread, queue, unit)
        if queue_result is not JournalStealResult.WRITTEN:
            return queue_result
        await bump_epoch(conn, thread_id=thread_id)
        await retire_stale_stateless_permissions(
            conn,
            thread_id=thread_id,
            retired_lease_token=int(unit.previous_lease_token),
            successor_lease_token=int(unit.lease_token),
            reason="lease_expired",
            epoch_already_bumped=True,
        )
        interrupts = await _reconcile_stolen_interrupts(
            conn,
            thread_id=thread_id,
            unit=unit,
        )
        # Applied settlement consumes the stopped input and can override a
        # post-steal parked row to done/queued. Its lifecycle edge is an
        # interruption, not a poison-unit park; choose only after settlement.
        kind = (
            "turn.interrupted"
            if unit.state == STATE_QUEUED
            or interrupts.settled_queue_state in {"done", "queued"}
            else "turn.parked"
        )
        await _append_turn_frames(
            conn,
            thread_id=thread_id,
            kind=kind,
            attempts=unit.attempts_since_completion,
            stolen_from=unit.leased_by,
            interrupts=interrupts,
            fallback_target_turn_id=unit.interrupt_admission_turn_id,
        )
    return JournalStealResult.WRITTEN


def _queue_allows_stale_retry(queue: Any) -> bool:
    """True only for a provably writer-free post-steal parked row.

    A voluntarily released queued row can keep an attached warm journal
    allocator even after ``last_leased_by`` is cleared. Queued repair is thus
    delegated to its next live claimant; parked post-steal rows are the only
    safe periodic system-writer context.
    """
    return bool(
        queue is not None
        and str(queue["unit_kind"]) == UNIT_KIND_SESSION_TURN
        and str(queue["state"]) == "parked"
        and queue["leased_by"] is None
    )


def _thread_allows_parked_permission_retry(thread: Any) -> bool:
    """Prove no credential-bearing claimant or resident writer remains."""

    if (
        thread is None
        or str(thread["execution_lane"] or "") != "stateless"
        or thread["agent_id"] is not None
    ):
        return False
    try:
        metadata = _metadata_object(thread["metadata"] or {})
    except RuntimeError:
        return False
    return not any(
        key in metadata
        for key in (
            ACTIVE_CLAIM_KEY,
            CLAIM_LOSS_LEDGER_KEY,
            CLAIM_LOSS_HOLD_KEY,
        )
    )


async def _retry_stale_permission_thread(
    conn: Any,
    *,
    thread_id: str,
) -> int:
    """Retire rolling-skew rows only at a proven writer-free boundary."""

    async with conn.transaction():
        thread = await conn.fetchrow(_LOCK_THREAD_SQL, thread_id)
        if not _thread_allows_parked_permission_retry(thread):
            return 0
        queue = await conn.fetchrow(_LOCK_QUEUE_UNIT_SQL, thread_id)
        if (
            queue is None
            or str(queue["unit_kind"]) != UNIT_KIND_SESSION_TURN
            or str(queue["state"]) not in {"parked", "done"}
            or queue["leased_by"] is not None
        ):
            return 0
        queue_token = int(queue["lease_token"] or 0)
        if queue_token <= 0:
            return 0
        if queue["last_leased_by"] is not None:
            # Reaper steals clear this hint. A non-NULL holder on parked is a
            # malformed/legacy state, not proof that a warm allocator is gone.
            return 0
        if str(queue["state"]) == "done":
            # A plain completed queue may still have a warm allocator or may
            # simply be old history. The only recovery-grade done state is one
            # whose terminal interrupt row and immutable linked receipt both
            # prove that this exact queue token performed lease-recovery input
            # settlement. Lock that provenance through the retirement txn.
            proof = await conn.fetchrow(
                _LOCK_DONE_PERMISSION_RECOVERY_PROOF_SQL,
                thread_id,
                queue_token,
            )
            if proof is None:
                return 0
        result = await retire_stale_stateless_permissions(
            conn,
            thread_id=thread_id,
            retired_lease_token=queue_token - 1,
            successor_lease_token=queue_token,
            reason="lease_expired",
            epoch_already_bumped=False,
        )
        return result.count


async def retry_stale_permission_requests(
    conn: Any,
    *,
    max_threads: int = STALE_PERMISSION_RETRY_MAX_THREADS,
) -> int:
    """Bounded repair for rows skipped by an older rolling reaper.

    Candidate discovery is advisory. Each thread is rechecked in the global
    ``threads -> run_queue`` lock order. A parked row with no holder and no
    active/claim-loss authority is a system-writer context. A done row is
    eligible only with an exact terminal interrupt request + linked receipt
    proving this queue token performed lease-recovery input settlement.
    Ordinary done/queued rows can retain a warm allocator, and leased rows
    belong to their exact successor, so none is touched without that proof.
    """

    if max_threads <= 0:
        return 0
    candidates = await conn.fetch(
        _STALE_PERMISSION_RETRY_CANDIDATES_SQL,
        int(max_threads),
    )
    retired = 0
    for candidate in candidates:
        thread_id = str(candidate["thread_id"])
        try:
            retired += await _retry_stale_permission_thread(
                conn,
                thread_id=thread_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "run_queue reaper: stale permission retry failed for thread %s "
                "(contained)",
                thread_id,
            )
    return retired


async def _retry_stale_interrupt_thread(
    conn: Any,
    *,
    thread_id: str,
) -> int:
    """Reconstruct one failed parked post-steal journal transaction."""
    async with conn.transaction():
        # Match interrupt admission's threads -> run_queue lock order.
        thread = await conn.fetchrow(_LOCK_THREAD_SQL, thread_id)
        if (
            thread is None
            or str(thread["execution_lane"] or "") != "stateless"
            or thread["agent_id"] is not None
        ):
            return 0
        if unresolved_claim_losses(thread["metadata"]):
            return 0
        queue = await conn.fetchrow(_LOCK_QUEUE_UNIT_SQL, thread_id)
        if not _queue_allows_stale_retry(queue):
            return 0
        queue_token = int(queue["lease_token"])
        pending = await conn.fetch(
            _PENDING_STALE_INTERRUPT_RETRY_SQL,
            thread_id,
            queue_token,
        )
        if not pending:
            return 0
        # The original journal-steal transaction failed as one unit, so retry
        # reconstructs the whole boundary even when every ack receipt predates
        # it: epoch bump, row reconciliation, and turn.parked are one commit.
        await bump_epoch(conn, thread_id=thread_id)
        await retire_stale_stateless_permissions(
            conn,
            thread_id=thread_id,
            retired_lease_token=queue_token - 1,
            successor_lease_token=queue_token,
            reason="lease_expired",
            epoch_already_bumped=True,
        )
        interrupts = await _reconcile_interrupt_rows(
            conn,
            thread_id=thread_id,
            current_lease_token=queue_token,
            pending=pending,
        )
        owners = {
            str(request["accepted_leased_by"])
            for request in pending
            if request["accepted_leased_by"] is not None
        }
        await _append_turn_frames(
            conn,
            thread_id=thread_id,
            kind=(
                "turn.interrupted"
                if interrupts.settled_queue_state in {"done", "queued"}
                else "turn.parked"
            ),
            attempts=int(queue["attempts_since_completion"]),
            stolen_from=next(iter(owners)) if len(owners) == 1 else None,
            interrupts=interrupts,
        )
        return interrupts.count


async def retry_stale_interrupt_requests(
    conn: Any,
    *,
    max_threads: int = STALE_INTERRUPT_RETRY_MAX_THREADS,
) -> int:
    """Bounded periodic repair for failed post-steal interrupt transactions.

    Candidate discovery is advisory. Each thread is rechecked while holding
    its exact queue row ``FOR UPDATE``. A claim that commits first makes the
    row leased and is skipped; a retry that locks first excludes claims until
    its receipt/finalization transaction commits. Per-thread errors are
    contained so a corrupt receipt cannot starve unrelated repairs.
    """
    if max_threads <= 0:
        return 0
    candidates = await conn.fetch(
        _STALE_INTERRUPT_RETRY_CANDIDATES_SQL,
        int(max_threads),
    )
    reconciled = 0
    for candidate in candidates:
        thread_id = str(candidate["thread_id"])
        try:
            reconciled += await _retry_stale_interrupt_thread(
                conn,
                thread_id=thread_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "run_queue reaper: stale interrupt retry failed for thread %s "
                "(contained)",
                thread_id,
            )
    return reconciled


async def reconcile_claim_loss_holds(
    conn: Any,
    *,
    max_threads: int = CLAIM_LOSS_RECONCILE_MAX_THREADS,
) -> int:
    """Evict/settle immutable claimant Pods before restoring held work.

    Candidate discovery holds no DB lock while Kubernetes is contacted.  The
    exact ACK then reacquires ``threads -> run_queue`` and removes only the
    same ``(token, pod, uid)`` debt. A same-name replacement proves the old UID
    gone but is never itself deleted.
    """

    if max_threads <= 0:
        return 0
    from orchestrator.services.agent_provisioner import agent_provisioner
    from shared.session_retirement import (
        acknowledge_session_claim_quiesced,
        mark_session_claim_eviction_requested,
    )

    candidates = await conn.fetch(_CLAIM_LOSS_HOLD_CANDIDATES_SQL, int(max_threads))
    settled = 0
    for candidate in candidates:
        thread_id = str(candidate["id"])
        try:
            losses = unresolved_claim_losses(candidate["metadata"])
            for token, authority in sorted(losses.items()):
                pod_state = await agent_provisioner.agent_pod_authority(
                    authority.pod,
                    expected_pod_uid=authority.pod_uid,
                )
                if pod_state == "exact_live":
                    if await mark_session_claim_eviction_requested(
                        conn,
                        thread_id=thread_id,
                        previous_lease_token=token,
                        leased_by=authority.pod,
                        pod_uid=authority.pod_uid,
                    ):
                        await agent_provisioner.delete_agent_pod_exact(
                            authority.pod,
                            expected_pod_uid=authority.pod_uid,
                        )
                    continue
                if pod_state != "exact_terminal":
                    # Kubernetes API-object absence (including a same-name
                    # replacement) cannot prove that a partitioned kubelet
                    # stopped the old credential-bearing process.  Only the
                    # claimant's local drain ACK or an observed exact UID with
                    # all containers terminated may clear this debt.
                    if pod_state in {"exact_absent", "replacement"}:
                        _report_absent_claimant(thread_id, token, authority, pod_state)
                    continue
                # The executor's process-zero finalizer keeps this exact
                # terminal object readable; the ACK and its receipt commit
                # before release_retained_executor_pods may let it go.
                if await acknowledge_session_claim_quiesced(
                    conn,
                    thread_id=thread_id,
                    previous_lease_token=token,
                    leased_by=authority.pod,
                    pod_uid=authority.pod_uid,
                    quiesced_by="pod_terminal",
                    receipt={"evidence": "exact_terminal"},
                ):
                    settled += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "run_queue reaper: claimant-loss reconciliation failed for %s "
                "(contained)",
                thread_id,
            )
    return settled


def _report_absent_claimant(
    thread_id: str, token: int, authority: ClaimantAuthority, pod_state: str
) -> None:
    """Log, once, a claimant debt whose exact Pod vanished without proof.

    With the executor process-zero finalizer the object outlives its
    containers, so a 404 means a force delete, node GC or a pre-finalizer
    Pod, never the steady state. Only an operator can settle it.
    """

    key = (thread_id, int(token), authority.pod_uid)
    if key in _ABSENT_CLAIMANT_ANOMALIES:
        return
    if len(_ABSENT_CLAIMANT_ANOMALIES) >= _ABSENT_CLAIMANT_ANOMALY_LIMIT:
        _ABSENT_CLAIMANT_ANOMALIES.clear()
    _ABSENT_CLAIMANT_ANOMALIES.add(key)
    logger.error(
        "run_queue reaper: claimant pod %s uid=%s of unit %s (token %d) is %s "
        "without process-zero proof; the claim-loss hold cannot settle "
        "automatically. Once the process is confirmed gone: POST "
        "/api/admin/run-queue/%s/attest-claimant-gone",
        authority.pod,
        authority.pod_uid,
        thread_id,
        int(token),
        pod_state,
        thread_id,
    )


async def unreferenced_executor_uids(conn: Any, pods: dict[str, str]) -> set[str]:
    """Return the ``{uid: name}`` candidates no claimant authority names.

    Callers must observe the Pods BEFORE this read. A durable reference is
    created only by a live claimant process holding a session lease (the
    claim-bundle stamp, then the steal/End conversion into ledger debt), so a
    process already observed stopped cannot create one after this snapshot.
    """

    if not pods:
        return set()
    uids = sorted(pods)
    rows = await conn.fetch(
        _UNREFERENCED_EXECUTOR_UIDS_SQL,
        uids,
        [pods[uid] for uid in uids],
    )
    return {str(row["pod_uid"]) for row in rows}


async def release_retained_executor_pods(
    conn: Any,
    *,
    max_pods: int = RETAINED_EXECUTOR_RELEASE_MAX_PODS,
) -> int:
    """Release the process-zero finalizer from settled executor Pods.

    Every pool executor is born with the chart's retention finalizer so the
    claimant-loss reconciler can observe its exact terminal UID. Release
    requires, in order: one Kubernetes LIST (process-zero evaluated on that
    snapshot), one database snapshot proving no session lease is held by the
    Pod's name and no claim-loss ledger names its UID, then a
    UID/resourceVersion-tested patch. Rollouts, scale-downs, crash
    replacements and never-claimed Pods therefore leave nothing behind, while
    a Pod whose debt is unsettled stays readable. Stateless by design: an
    orchestrator restart or a missed tick defers release to the next pass.
    """

    from orchestrator.services.agent_provisioner import (
        agent_provisioner,
        stateless_executor_process_zero,
    )

    try:
        retained = await agent_provisioner.list_retained_stateless_executor_pods()
        if not retained:
            return 0
        candidates: dict[str, Any] = {}
        names: dict[str, str] = {}
        for pod in retained:
            if len(candidates) >= max_pods:
                break
            if not stateless_executor_process_zero(pod):
                continue
            uid = str(pod.metadata.uid)
            candidates[uid] = pod
            names[uid] = str(pod.metadata.name)
        released = 0
        for uid in sorted(await unreferenced_executor_uids(conn, names)):
            if await agent_provisioner.release_stateless_executor_finalizer_exact(
                candidates[uid]
            ):
                released += 1
        return released
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "run_queue reaper: retained executor finalizer release failed "
            "(contained; retried next cycle)"
        )
        return 0


async def _try_steal_worker_with_recovery(
    conn: Any, *, candidate: Any, backoff_seconds: float, grace_seconds: float
) -> StolenUnit | None:
    """Let exact recovery evidence decide before the ordinary exhaustion CAS."""
    try:
        store = VMWorkspaceRecoveryStore(conn)
        disposition = await store.get_attempt_disposition(
            conn, job_id=candidate["unit_id"], lease_token=candidate["lease_token"]
        )
        # Admission owns canonical workspace -> queue -> job locking and
        # revalidates both expiry and attempt evidence after those locks.
        if await store.admit_hold_from_reaper(
            conn,
            disposition=disposition,
            job_id=candidate["unit_id"],
            lease_token=candidate["lease_token"],
            grace_seconds=grace_seconds,
        ):
            return None
        # Non-VM workers retain the queue-only steal and exhaustion behavior.
        row = await conn.fetchrow(
            _REAP_STEAL_SQL,
            candidate["unit_id"],
            candidate["lease_token"],
            backoff_seconds,
            grace_seconds,
        )
        if row is None:
            return None
        return StolenUnit(
            unit_id=row["unit_id"],
            unit_kind=row["unit_kind"],
            state=row["state"],
            attempts_since_completion=row["attempts_since_completion"],
            leased_by=candidate["leased_by"],
            lease_token=row["lease_token"],
            previous_lease_token=candidate["lease_token"],
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "run_queue reaper: worker recovery reconciliation failed for %s (exact lease left unchanged)",
            candidate["unit_id"],
        )
        return None


async def reap_cycle(
    conn: Any,
    *,
    grace_seconds: float = REAPER_GRACE_SECONDS,
) -> int:
    """One sweep: steal expired leases, journal the session ones. Returns the
    steal count.

    Runs OUTSIDE any enclosing transaction (``reap_expired``'s contract: each
    per-row CAS steal commits independently); only the per-unit journal write
    opens its own transaction. Per-row errors are contained so one bad unit
    never blocks the rest of the pass.
    """
    recovery_options = (
        {"worker_steal": _try_steal_worker_with_recovery}
        if workspace_recovery_enabled()
        else {}
    )
    stolen = await reap_expired(
        conn,
        grace_seconds=grace_seconds,
        session_steal=_try_steal_session_with_claim_loss,
        **recovery_options,
    )
    for unit in stolen:
        # Greppable ops line — one per steal (M6 fault-injection anchors on it).
        logger.info(
            "run_queue steal: unit=%s kind=%s pod=%s attempts=%d new_state=%s",
            unit.unit_id,
            unit.unit_kind,
            unit.leased_by,
            unit.attempts_since_completion,
            unit.state,
        )
        if unit.unit_kind != UNIT_KIND_SESSION_TURN:
            # Reap only — no journal for non-session kinds (S3).
            continue
        if unit.lifecycle_journaled:
            continue
        try:
            result = await _journal_steal(conn, unit)
            if result is not JournalStealResult.WRITTEN:
                logger.info(
                    "run_queue reaper: post-steal journal skipped for unit %s (%s)",
                    unit.unit_id,
                    result.value,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Contained: the steal itself already committed (lease fenced);
            # only the user-visible journal signal was lost for this unit.
            logger.exception(
                "run_queue reaper: journal write failed for unit %s (contained)",
                unit.unit_id,
            )
    await retry_stale_interrupt_requests(conn)
    await retry_stale_permission_requests(conn)
    await reconcile_claim_loss_holds(conn)
    # After settlement, so a UID the reconciler just proved terminal is
    # released in the same cycle and never before its receipt is durable.
    await release_retained_executor_pods(conn)
    return len(stolen)


async def run_queue_reaper_loop(
    db: Any,
    shutdown_event: asyncio.Event,
    *,
    interval_seconds: float = float(REAPER_INTERVAL_SECONDS),
    grace_seconds: float = float(REAPER_GRACE_SECONDS),
) -> None:
    """Contend for the reaper advisory lock and sweep while holding it.

    ``db`` is the orchestrator's ``PostgresDB`` (uses ``db._pool`` directly:
    the advisory lock is session-scoped, so lock and sweep must share one
    dedicated connection whose lifetime IS the leadership tenure). Followers
    re-contend every ``interval_seconds``. Wired in ``main.py``'s lifespan
    beside ``thread_events_prune_sweeper`` and stopped via the same
    ``shutdown_event``.
    """
    logger.info(
        "run_queue reaper started (interval=%.0fs grace=%.0fs)",
        interval_seconds,
        grace_seconds,
    )
    while not shutdown_event.is_set():
        pool = getattr(db, "_pool", None)
        if pool is None:
            # Orchestrator still booting; retry next interval.
            await _sleep_or_shutdown(interval_seconds, shutdown_event)
            continue
        conn = None
        try:
            conn = await pool.acquire()
            got = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1)", RUN_QUEUE_REAPER_ID
            )
            if not got:
                await _sleep_or_shutdown(interval_seconds, shutdown_event)
                continue
            logger.info("run_queue reaper: leadership acquired")
            try:
                while not shutdown_event.is_set():
                    await reap_cycle(conn, grace_seconds=grace_seconds)
                    await _sleep_or_shutdown(interval_seconds, shutdown_event)
            finally:
                try:
                    await conn.execute(
                        "SELECT pg_advisory_unlock($1)", RUN_QUEUE_REAPER_ID
                    )
                except Exception:
                    # Best-effort: the lock auto-releases with the session.
                    pass
                logger.info("run_queue reaper: leadership released")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Connection lost / DB blip / cycle-level failure: drop leadership
            # (lock releases with the session) and re-contend after a beat.
            logger.warning("run_queue reaper error (non-fatal, retrying): %s", e)
            await _sleep_or_shutdown(interval_seconds, shutdown_event)
        finally:
            if conn is not None:
                try:
                    await pool.release(conn)
                except Exception:
                    pass
    logger.info("run_queue reaper stopped")
