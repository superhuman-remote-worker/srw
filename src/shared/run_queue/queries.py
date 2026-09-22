"""run_queue SQL contract — claim, lease, fence, completion (§5.1/§5.2).

The work-queue + recorded-lease substrate of the stateless-agents design
(knowledge-base/knowledge/features/stateless_agents.md). Both the orchestrator (enqueue paths,
reaper, read models) and the agent executor (claim, heartbeat, fence,
complete, release) import THIS module, so the semantics live in exactly one
place. Pure functions over an asyncpg connection or pool; SQL as module
constants; no engine, no ORM.

Contract invariants (the point of this module — do not weaken):

* **The lease is recorded state, never a held lock.** ``FOR UPDATE SKIP
  LOCKED`` appears only inside the single-statement claim; nothing holds a
  row or advisory lock across an executing turn (pgmq/SQS visibility-timeout
  model — advisory locks pin a backend and break under transaction pooling;
  row locks held for minutes trip idle-in-transaction timeouts).
* **``lease_token`` is a Kleppmann fencing token**: monotonic per unit,
  bumped on every claim and every reaper steal, NEVER reset by enqueue,
  complete, or release. Rows are durable per unit for exactly this reason —
  delete-and-reinsert would reset the token to 0 and break fencing.
* **Every persist transaction opens with** :func:`fence_lease`; zero rows
  means the lease is lost and the transaction must abort. ``FOR SHARE``
  blocks a concurrent steal until the persist commits, so check-then-write
  cannot interleave with a steal.
* **Watermarks close the fence's blind spot.** A steal landing between the
  final persist and completion leaves a *valid* new lease, so fencing alone
  cannot stop a double answer — the claim returns ``input_seq`` /
  ``consumed_seq`` and the executor skips already-answered input (§5.1
  skip-if-answered).
* **Input during a leased turn touches ``input_seq`` only.** Flipping a
  leased row's state would break the lease; completion re-queues instead
  (``input_seq > consumed_seq`` at complete time ⇒ ``state='queued'``).
* **Interrupt admission is an exact lease/turn window, not a watermark.** The
  serving executor opens the nullable admission pair only after its consumer
  is ready, then closes it before the final drain. Admission and closure lock
  the same queue row, so a request either commits before closure and is
  drained by that owner or observes the closed window and is refused. A
  successor must never consume an interrupt aimed at an earlier turn.
* **Attempts reset only on full completion** (and explicit unpark), never on
  release or partial progress — a unit that dies at the same tool call every
  cycle parks at ``max_attempts`` instead of hot-looping LLM spend.
* **Dedup is queued-only.** One pending and one running collapsible task may
  coexist; a signal arriving mid-run is never swallowed.
* **Layering:** this module mutates ONLY ``run_queue`` and reads unresolved
  recovery participants to fence worker claims, completion and reaping. Epoch bumps, system
  frames (``turn.interrupted`` / ``turn.parked``), and job-row CASes belong
  to the callers (reaper loop / executor), keyed off the returned records.

Timestamps are compared in the database (``now()``), never in Python, so
skewed executor clocks cannot corrupt lease arithmetic.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any, Awaitable, Callable
from uuid import UUID

from shared.run_queue.types import (
    ClaimedUnit,
    EnqueueResult,
    QueueWatermarks,
    StolenUnit,
)

if TYPE_CHECKING:  # pragma: no cover - typing only; keeps runtime deps stdlib
    from collections.abc import Sequence
    from datetime import datetime

    import asyncpg

    Executor = asyncpg.Connection | asyncpg.Pool
else:
    Executor = Any

# --- Doc-appendix parameters (§5.2 / Appendix) -------------------------------

LEASE_TTL_SECONDS = 60
HEARTBEAT_INTERVAL_SECONDS = 20  # TTL/3: ~2 missed beats tolerated
REAPER_GRACE_SECONDS = 30  # steal at expiry + grace ⇒ ≥1.5 missed beats
REAPER_INTERVAL_SECONDS = 15

# Warm-pod affinity grace (§5.3.4). A freshly queued unit is claimable only by
# its last holder for this long. Sized to cover one idle poll of a warm pod
# (0.5s ± 20% jitter) with slack for scheduling and DB latency; the executor
# additionally refuses to enter poll backoff while it holds a warm session, so
# the warm pod is always in the fast poll cadence. The cost when the last
# holder is gone is exactly this window, once.
AFFINITY_GRACE_SECONDS = 2.0

# --- Unit-kind / lane vocabulary (app-validated; no CHECK constraints) -------

UNIT_KIND_SESSION_TURN = "session_turn"
UNIT_KIND_WORKER_BATCH = "worker_batch"
UNIT_KIND_BG_TASK = "bg_task"

LANE_PINNED = "pinned"  # threads.execution_lane default
LANE_STATELESS = "stateless"

STATE_QUEUED = "queued"
STATE_LEASED = "leased"
STATE_DONE = "done"
STATE_PARKED = "parked"

# --- Enqueue outcome vocabulary ----------------------------------------------

ENQUEUE_INSERTED = "inserted"  # no row existed — fresh queued row
ENQUEUE_REQUEUED = "requeued"  # done → queued (unit woke up again)
ENQUEUE_UPDATED = "updated"  # queued row merged (priority/run_after/input)
ENQUEUE_INPUT_RECORDED = "input_recorded"  # leased row: input watermark only
ENQUEUE_PARKED = "parked"  # parked row: input recorded, stays parked
ENQUEUE_DEDUPED = "deduped"  # collapsed into an existing queued dedup row

_OLD_STATE_TO_STATUS = {
    None: ENQUEUE_INSERTED,
    STATE_DONE: ENQUEUE_REQUEUED,
    STATE_QUEUED: ENQUEUE_UPDATED,
    STATE_LEASED: ENQUEUE_INPUT_RECORDED,
    STATE_PARKED: ENQUEUE_PARKED,
}

_RELEASE_BACKOFF_BASE_SECONDS = 5.0

# Park lifecycle (stateless_turn_resilience.md step 2). `park_reason` names the
# disposition an operator or owner sees; the retryable set is the one an owner
# may revive through /queue/retry without an operator (no unledgered effect
# debt is implied by any of them — attach never crossed a tool boundary, a
# shutdown/CAS park is a completion that was cut, a reaper park is exhausted
# claims). `claim_loss_hold` and free-form executor reasons stay admin-only.
PARK_REASON_ATTACH_FAILED = "attach_failed"
PARK_REASON_SHUTDOWN_CANCELLED = "shutdown_cancelled"
PARK_REASON_COMPLETION_CAS_FAILED = "completion_cas_failed"
PARK_REASON_REAPER_MAX_ATTEMPTS = "reaper_max_attempts"
# A pre-effect error release that reached the row's own max_attempts. The
# release paths that use it never crossed a tool boundary (post-effect
# failures park fail-closed instead), so an owner retry replays nothing.
PARK_REASON_RETRY_EXHAUSTED = "retry_exhausted"
PARK_REASON_CLAIM_LOSS_HOLD = "claim_loss_hold"
RETRYABLE_PARK_REASONS = frozenset(
    {
        PARK_REASON_ATTACH_FAILED,
        PARK_REASON_SHUTDOWN_CANCELLED,
        PARK_REASON_COMPLETION_CAS_FAILED,
        PARK_REASON_REAPER_MAX_ATTEMPTS,
        PARK_REASON_RETRY_EXHAUSTED,
    }
)
# Attach-failure backoff: indexed by the claim's own attempt count (the claim
# already counted this attempt), capped. A same-signature failure parks on the
# third consecutive occurrence regardless of max_attempts.
ATTACH_FAILURE_BACKOFF_SECONDS: tuple[float, ...] = (5.0, 15.0, 45.0, 135.0)
ATTACH_FAILURE_BACKOFF_CAP_SECONDS = 300.0
ATTACH_FAILURE_SIGNATURE_PARK_THRESHOLD = 3


def attach_failure_backoff_seconds(
    attempts: int,
    *,
    schedule: tuple[float, ...] = ATTACH_FAILURE_BACKOFF_SECONDS,
    cap: float = ATTACH_FAILURE_BACKOFF_CAP_SECONDS,
) -> float:
    """Backoff before the next claim after the ``attempts``-th failed attach.

    ``attempts`` is ``attempts_since_completion`` as the claim recorded it
    (1 on the first attempt). Walks the schedule, holds its last step, caps.
    """
    if not schedule:
        return min(cap, 0.0)
    index = max(0, min(int(attempts) - 1, len(schedule) - 1))
    return float(min(schedule[index], cap))


# Error releases split by whether a retry can succeed without a change
# (voluntary_session_release_bypasses_retry_budget). A deterministic failure
# counts toward the park budget under this failure-record signature; a
# transient one (DB, transport, an orchestrator restarting mid-deploy) never
# parks and backs off exponentially instead — capped, so a recovered
# orchestrator is retried within a minute. (Input admission records a
# watermark only; it does not shorten either wait.)
DETERMINISTIC_RELEASE_SIGNATURE = "release:deterministic"
TRANSIENT_RELEASE_BACKOFF_BASE_SECONDS = 5.0
TRANSIENT_RELEASE_BACKOFF_CAP_SECONDS = 60.0


def transient_release_backoff_seconds(
    attempts: int,
    *,
    base: float = TRANSIENT_RELEASE_BACKOFF_BASE_SECONDS,
    cap: float = TRANSIENT_RELEASE_BACKOFF_CAP_SECONDS,
) -> float:
    """Backoff before re-claiming after a transient error release.

    ``attempts`` is the claim's ``attempts_since_completion`` (1 on the first
    claim): ``base × 2^(attempts-1)``, capped. It grows through an outage and
    resets with the unit's next completion.
    """
    exponent = max(0, int(attempts) - 1)
    if exponent >= 32:
        return float(cap)
    return float(min(cap, base * (2**exponent)))


# =============================================================================
# SQL
# =============================================================================

# Admission upsert for durable units (dedup_key IS NULL). One statement:
#   * row absent          → INSERT state='queued' (ON CONFLICT(unit_id) covers
#                           the concurrent-insert race with a benign merge).
#   * 'done'              → flip to 'queued', fresh queued_at/run_after.
#   * 'queued'            → monotonic merge: priority GREATEST, run_after
#                           LEAST (an enqueue may make a unit MORE runnable,
#                           never less — new input must cut an error backoff),
#                           input_seq GREATEST. queued_at is NOT touched: a
#                           re-enqueue must not move the unit in the FIFO.
#   * 'leased'            → input_seq GREATEST only (never touch the lease).
#   * 'parked'            → input_seq GREATEST only (unpark is explicit).
# lease_token is never written by any branch.
_ENQUEUE_SQL = """
WITH cur AS (
    SELECT unit_id, state AS old_state
    FROM run_queue
    WHERE unit_id = $1::uuid
    FOR UPDATE
),
upd AS (
    UPDATE run_queue r SET
        state     = CASE WHEN cur.old_state = 'done' THEN 'queued'
                         ELSE r.state END,
        queued_at = CASE WHEN cur.old_state = 'done' THEN now()
                         ELSE r.queued_at END,
        priority  = CASE WHEN cur.old_state IN ('done', 'queued')
                         THEN GREATEST(r.priority, $4::int)
                         ELSE r.priority END,
        run_after = CASE WHEN cur.old_state = 'done'
                         THEN COALESCE($5::timestamptz, now())
                         WHEN cur.old_state = 'queued'
                         THEN LEAST(r.run_after, COALESCE($5::timestamptz, now()))
                         ELSE r.run_after END,
        input_seq = GREATEST(r.input_seq, $6::bigint),
        fair_key  = CASE WHEN cur.old_state IN ('done', 'queued')
                         THEN COALESCE($3::text, r.fair_key)
                         ELSE r.fair_key END
    FROM cur
    WHERE r.unit_id = cur.unit_id
    RETURNING cur.old_state, r.state AS new_state
),
ins AS (
    -- Row creation initializes consumed_seq to input_seq - 1: the unit's
    -- first queue signal is the lane-flip boundary — everything persisted
    -- BEFORE it belongs to the pre-queue (pinned) life and must never be
    -- re-answered. A NULL floor here made the first claim treat the entire
    -- thread history as pending (observed live: duplicate re-answer of an
    -- already-answered message on a flipped thread).
    INSERT INTO run_queue (unit_id, unit_kind, fair_key, priority, run_after,
                           input_seq, consumed_seq)
    SELECT $1::uuid, $2::text, $3::text, $4::int,
           COALESCE($5::timestamptz, now()), $6::bigint,
           $6::bigint - 1
    WHERE NOT EXISTS (SELECT 1 FROM cur)
    ON CONFLICT (unit_id) DO UPDATE SET
        priority  = GREATEST(run_queue.priority, EXCLUDED.priority),
        run_after = LEAST(run_queue.run_after, EXCLUDED.run_after),
        input_seq = GREATEST(run_queue.input_seq, EXCLUDED.input_seq),
        fair_key  = COALESCE(EXCLUDED.fair_key, run_queue.fair_key)
    RETURNING NULL::text AS old_state, state AS new_state
)
SELECT old_state, new_state FROM upd
UNION ALL
SELECT old_state, new_state FROM ins
"""

# Admission for collapsible tasks (dedup_key IS NOT NULL): rely on the partial
# unique dedup index — queued-only, so a running instance never swallows a new
# signal. Requires a FRESH unit_id per signal (the PK is not an arbiter here).
_ENQUEUE_DEDUP_SQL = """
INSERT INTO run_queue (unit_id, unit_kind, dedup_key, fair_key, priority, run_after,
                       input_seq, consumed_seq)
VALUES ($1::uuid, $2::text, $3::text, $4::text, $5::int,
        COALESCE($6::timestamptz, now()), $7::bigint,
        $7::bigint - 1)
ON CONFLICT (unit_kind, dedup_key) WHERE dedup_key IS NOT NULL AND state = 'queued'
DO NOTHING
RETURNING state
"""

# §5.2 claim CTE, verbatim, plus the enqueue_ord tiebreak so equal-timestamp
# claims are deterministic FIFO. The only place SKIP LOCKED appears; the
# statement claims-and-marks atomically (closes the READ COMMITTED
# double-dispatch race) and commits immediately — no lock survives the call.
_CLAIM_UPDATE_TAIL = """
UPDATE run_queue r SET
    state = 'leased',
    lease_token = lease_token + 1,
    input_delivery_capable_lease_token = CASE
        WHEN r.unit_kind = 'session_turn' THEN lease_token + 1
        ELSE r.input_delivery_capable_lease_token
    END,
    leased_by = $2::text,
    last_leased_by = $2::text,
    leased_until = now() + make_interval(secs => $3::float8),
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    attempts_since_completion = attempts_since_completion + 1
FROM c
WHERE r.unit_id = c.unit_id
RETURNING r.unit_id, r.unit_kind, r.fair_key, r.lease_token, r.input_seq,
          r.consumed_seq, r.control_input_seq, r.control_consumed_seq,
          r.attempts_since_completion, r.leased_until
"""

# General claim. The affinity grace ($4) hides a freshly queued unit from
# every pod EXCEPT the one that last held it, for a bounded window (§5.3.4):
# the last holder may still have the thread's session attached in-process,
# and a cold winner pays a full re-attach the warm one skips. Purely an
# optimization — after the grace any pod claims normally, so a dead holder
# costs at most one grace window, and `last_leased_by IS NULL` (never-claimed
# units) is unaffected.
_CLAIM_SQL = (
    """
WITH c AS (
    SELECT unit_id FROM run_queue
    WHERE state = 'queued' AND unit_kind = $1::text AND run_after <= now()
      AND NOT EXISTS (
          SELECT 1 FROM vm_workspace_recovery_jobs AS recovery_job
          WHERE recovery_job.job_id = run_queue.unit_id
            AND recovery_job.resolved_at IS NULL
      )
      AND (last_leased_by IS NULL
           OR last_leased_by = $2::text
           OR queued_at <= now() - make_interval(secs => $4::float8))
    ORDER BY priority DESC, queued_at, enqueue_ord
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
"""
    + _CLAIM_UPDATE_TAIL
)

# Affinity variant: same statement constrained to one unit (soft affinity —
# the pod continuing its own thread). Tried FIRST, before the general claim.
# No grace predicate: this IS the warm holder claiming its own unit.
_CLAIM_PREFER_SQL = (
    """
WITH c AS (
    SELECT unit_id FROM run_queue
    WHERE unit_id = $4::uuid
      AND state = 'queued' AND unit_kind = $1::text AND run_after <= now()
      AND NOT EXISTS (
          SELECT 1 FROM vm_workspace_recovery_jobs AS recovery_job
          WHERE recovery_job.job_id = run_queue.unit_id
            AND recovery_job.resolved_at IS NULL
      )
    FOR UPDATE SKIP LOCKED
)
"""
    + _CLAIM_UPDATE_TAIL
)

_HEARTBEAT_SQL = """
UPDATE run_queue SET
    leased_until = now() + make_interval(secs => $3::float8)
WHERE unit_id = $1::uuid AND lease_token = $2::bigint AND state = 'leased'
RETURNING leased_until
"""

# The pair's nullable state is the admission bit. Open and close serialize with
# REST admission on the same run_queue row; unlike a watermark, the pair is
# deliberately tied to one lease and one concrete turn and never transfers to
# a successor. Repeating the same operation is idempotent, while an attempt to
# replace a different open turn fails closed.
_OPEN_INTERRUPT_ADMISSION_SQL = """
UPDATE run_queue SET
    interrupt_admission_lease_token = $2::bigint,
    interrupt_admission_turn_id = $3::integer
WHERE unit_id = $1::uuid
  AND unit_kind = 'session_turn'
  AND state = 'leased'
  AND lease_token = $2::bigint
  AND (
      (interrupt_admission_lease_token IS NULL
       AND interrupt_admission_turn_id IS NULL)
      OR
      (interrupt_admission_lease_token = $2::bigint
       AND interrupt_admission_turn_id = $3::integer)
  )
RETURNING 1
"""

_CLOSE_INTERRUPT_ADMISSION_SQL = """
UPDATE run_queue AS queue SET
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    consumed_seq = CASE
        WHEN $4::bigint IS NULL THEN queue.consumed_seq
        ELSE GREATEST(COALESCE(queue.consumed_seq, -1), $4::bigint)
    END,
    attempts_since_completion = CASE
        WHEN $4::bigint IS NULL THEN queue.attempts_since_completion
        ELSE 0
    END
WHERE queue.unit_id = $1::uuid
  AND queue.unit_kind = 'session_turn'
  AND queue.state = 'leased'
  AND queue.lease_token = $2::bigint
  AND (
      (
          queue.interrupt_admission_lease_token = $2::bigint
          AND queue.interrupt_admission_turn_id = $3::integer
          AND (
              $4::bigint IS NULL
              OR (
                  EXISTS (
                      SELECT 1
                      FROM thread_messages AS target
                      WHERE target.thread_id = queue.unit_id
                        AND target.seq = $4::bigint
                        AND target.rewound_at IS NULL
                        AND target.turn_number = $3::integer
                        AND (
                            target.role = 'human'
                            OR (
                                target.role = 'event'
                                AND EXISTS (
                                    SELECT 1
                                    FROM thread_input_deliveries AS delivery
                                    WHERE delivery.message_id = target.id
                                      AND delivery.thread_id = queue.unit_id
                                      AND delivery.execution_lane = 'stateless'
                                      AND delivery.state IN ('admitted', 'settled')
                                      AND delivery.owner_run_queue_lease_token
                                            = $2::bigint
                                )
                            )
                        )
                  )
                  AND (
                      COALESCE(queue.consumed_seq, -1) >= $4::bigint
                      OR EXISTS (
                          SELECT 1
                          FROM thread_messages AS event_target
                          JOIN thread_input_deliveries AS event_delivery
                            ON event_delivery.message_id = event_target.id
                          WHERE event_target.thread_id = queue.unit_id
                            AND event_target.seq = $4::bigint
                            AND event_target.role = 'event'
                            AND event_delivery.execution_lane = 'stateless'
                            AND event_delivery.state IN ('admitted', 'settled')
                            AND event_delivery.owner_run_queue_lease_token
                                  = $2::bigint
                      )
                      OR (
                          queue.input_seq IS NOT NULL
                          AND $4::bigint <= queue.input_seq
                          AND $4::bigint = (
                              SELECT min(message.seq)
                              FROM thread_messages AS message
                              WHERE message.thread_id = queue.unit_id
                                AND message.role = 'human'
                                AND message.rewound_at IS NULL
                                AND message.seq
                                    > COALESCE(queue.consumed_seq, -1)
                          )
                      )
                  )
              )
          )
      )
      OR (
          queue.interrupt_admission_lease_token IS NULL
          AND queue.interrupt_admission_turn_id IS NULL
          AND (
              $4::bigint IS NULL
              OR COALESCE(queue.consumed_seq, -1) >= $4::bigint
          )
      )
  )
RETURNING 1
"""

# §5.1 completion half, one fenced statement. NULL-safe watermark CASE: any
# recorded input is "ahead" of a NULL consumed watermark, so input that
# arrived during a turn whose claim carried no watermark still re-queues.
# A control committed during the lease is an independent reason to requeue:
# without this second watermark a control-only edge can be stranded in a row
# that completion just changed to ``done``.
_COMPLETE_SQL = """
UPDATE run_queue SET
    consumed_seq = CASE
        WHEN $3::bigint IS NULL THEN consumed_seq
        ELSE GREATEST(COALESCE(consumed_seq, $3::bigint), $3::bigint)
    END,
    attempts_since_completion = 0,
    attach_failures = 0,
    last_error = NULL,
    last_error_signature = NULL,
    queued_at = now(),
    state = CASE WHEN (input_seq IS NOT NULL
                       AND (
                           $3::bigint IS NULL
                           OR input_seq > GREATEST(
                               COALESCE(consumed_seq, $3::bigint), $3::bigint
                           )
                       ))
                      OR control_input_seq > control_consumed_seq
                      OR EXISTS (
                          SELECT 1
                          FROM thread_input_deliveries AS delivery
                          JOIN thread_messages AS message
                            ON message.id = delivery.message_id
                          WHERE delivery.thread_id = run_queue.unit_id
                            AND delivery.execution_lane = 'stateless'
                            AND delivery.state IN (
                                'persisted', 'owned', 'queued', 'deferred'
                            )
                            AND message.rewound_at IS NULL
                      )
                 THEN 'queued' ELSE 'done' END,
    leased_by = NULL,
    leased_until = NULL,
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    run_after = now()
WHERE unit_id = $1::uuid AND lease_token = $2::bigint AND state = 'leased'
  AND NOT EXISTS (
      SELECT 1 FROM vm_workspace_recovery_jobs AS recovery_job
      WHERE recovery_job.job_id = run_queue.unit_id
        AND recovery_job.resolved_at IS NULL
  )
RETURNING state
"""

_RELEASE_SQL = """
UPDATE run_queue SET
    state = 'queued',
    leased_by = NULL,
    -- A release is the pod saying "I cannot serve this" (bundle refused,
    -- attach failed, loop died, shutting down). Keeping it as the affinity
    -- holder would make every other pod wait out the grace before failing
    -- over to a pod that can. Same reasoning as the reaper steal.
    last_leased_by = NULL,
    leased_until = NULL,
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    queued_at = now(),
    run_after = now() + make_interval(secs =>
        CASE WHEN $4::boolean AND $3::float8 <= 0
             THEN $5::float8 * attempts_since_completion
             ELSE $3::float8 END)
WHERE unit_id = $1::uuid AND lease_token = $2::bigint AND state = 'leased'
RETURNING state
"""

# Deterministic error release (voluntary_session_release_bypasses_retry_budget).
# The reaper cannot bound a release loop — a voluntarily released row is never
# an expired lease — so this statement is the only budget check such a loop
# meets. It counts DETERMINISTIC failures only, in the unit's existing failure
# record (last_error_signature / attach_failures; completion and unpark reset
# it): while the signature $8 repeats the count advances, and another failure
# class in between (an attach failure) restarts it at 1. Transient releases and
# shutdown / stop-boundary hand-backs use _RELEASE_SQL and never touch the
# record, so they neither consume this budget nor reset it — attempts_since_
# completion, which every claim bumps, is deliberately not the measure here.
# Parks with reason $6 once the count reaches the row's OWN max_attempts;
# otherwise backs off on the attach-failure schedule, $5 x 3^(failures-1)
# capped at $9 (5 / 15 / 45 / 135 s), unless an explicit $3 is given. The
# spacing matters: the claim bundle answers 409 both for a genuine refusal
# and for a fail-closed exception behind it (DB, config resolution), so a
# budgeted streak must outlast a dependency blip before it parks. $7 is the
# operator-facing reason (NULL keeps the previous one).
_RELEASE_DETERMINISTIC_SQL = """
WITH cur AS (
    SELECT unit_id, max_attempts,
           CASE WHEN last_error_signature IS NOT DISTINCT FROM $8::text
                THEN attach_failures + 1 ELSE 1 END AS failures
    FROM run_queue
    WHERE unit_id = $1::uuid AND lease_token = $2::bigint AND state = 'leased'
    FOR UPDATE
)
UPDATE run_queue AS queue SET
    attach_failures = cur.failures,
    last_error_signature = $8::text,
    last_error = COALESCE($7::text, queue.last_error),
    state = CASE WHEN cur.failures >= cur.max_attempts
                 THEN 'parked' ELSE 'queued' END,
    park_reason = CASE WHEN cur.failures >= cur.max_attempts
                       THEN $6::text ELSE queue.park_reason END,
    parked_at = CASE WHEN cur.failures >= cur.max_attempts
                     THEN now() ELSE queue.parked_at END,
    leased_by = NULL,
    last_leased_by = NULL,
    leased_until = NULL,
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    queued_at = now(),
    run_after = now() + make_interval(secs =>
        CASE WHEN $4::boolean AND $3::float8 <= 0
             THEN LEAST($9::float8,
                        $5::float8 * power(3::float8, cur.failures - 1))
             ELSE $3::float8 END)
FROM cur
WHERE queue.unit_id = cur.unit_id
RETURNING queue.state
"""

# Fail-closed disposition for a leased unit whose executor crossed an
# external-effect boundary but could not reach normal completion. Unlike an
# error release, this MUST NOT make the unit runnable: retrying an input after
# an unledgered tool effect can duplicate that effect. The exact-token/state
# guard is the same ownership CAS as complete/release; watermarks and attempts
# are deliberately untouched so an operator can inspect and explicitly
# unpark the original debt.
_PARK_SQL = """
UPDATE run_queue SET
    state = 'parked',
    park_reason = COALESCE($3::text, park_reason),
    parked_at = now(),
    leased_by = NULL,
    last_leased_by = NULL,
    leased_until = NULL,
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    queued_at = now(),
    run_after = now()
WHERE unit_id = $1::uuid AND lease_token = $2::bigint AND state = 'leased'
RETURNING state
"""

_RECORD_INPUT_SQL = """
WITH cur AS (
    SELECT unit_id, state AS old_state
    FROM run_queue
    WHERE unit_id = $1::uuid
    FOR UPDATE
),
upd AS (
    UPDATE run_queue r SET
        input_seq = GREATEST(r.input_seq, $3::bigint),
        state     = CASE WHEN cur.old_state = 'done' THEN 'queued'
                         ELSE r.state END,
        queued_at = CASE WHEN cur.old_state = 'done' THEN now()
                         ELSE r.queued_at END,
        run_after = CASE WHEN cur.old_state = 'done' THEN now()
                         ELSE r.run_after END,
        fair_key  = CASE WHEN cur.old_state IN ('done', 'queued')
                         THEN COALESCE($4::text, r.fair_key)
                         ELSE r.fair_key END
    FROM cur
    WHERE r.unit_id = cur.unit_id
    RETURNING r.state AS new_state
),
ins AS (
    -- consumed_seq = input_seq - 1 at creation: the lane-flip boundary (see
    -- the identical initialization in _ENQUEUE_SQL for the full rationale).
    INSERT INTO run_queue (unit_id, unit_kind, fair_key, input_seq, consumed_seq)
    SELECT $1::uuid, $2::text, $4::text, $3::bigint, $3::bigint - 1
    WHERE NOT EXISTS (SELECT 1 FROM cur)
    ON CONFLICT (unit_id) DO UPDATE SET
        input_seq = GREATEST(run_queue.input_seq, EXCLUDED.input_seq)
    RETURNING state AS new_state
)
SELECT new_state FROM upd
UNION ALL
SELECT new_state FROM ins
"""

# Control admission mirrors record_input_seq, but does not disturb the human
# watermark. ``baseline_input_seq`` initializes human history only when a queue
# row is first created. The control baseline also advances whenever the row has
# no pending controls, because request_seq is thread-global and can include
# terminal controls from an earlier pinned generation. A parked unit remains
# parked; the public admission transaction rejects that outcome and rolls its
# request insert back rather than silently stranding a command.
_RECORD_CONTROL_SQL = """
WITH cur AS (
    SELECT unit_id, state AS old_state,
           control_input_seq AS old_control_input_seq,
           control_consumed_seq AS old_control_consumed_seq
    FROM run_queue
    WHERE unit_id = $1::uuid
    FOR UPDATE
),
upd AS (
    UPDATE run_queue r SET
        control_input_seq = GREATEST(r.control_input_seq, $3::bigint),
        -- request_seq is thread-global across lane changes. When this queue
        -- has no pending control, earlier terminal pinned requests are the
        -- stateless baseline rather than a gap this lane must consume.
        control_consumed_seq = CASE
            WHEN cur.old_control_input_seq = cur.old_control_consumed_seq
            THEN GREATEST(r.control_consumed_seq, $3::bigint - 1)
            ELSE r.control_consumed_seq
        END,
        state     = CASE WHEN cur.old_state = 'done' THEN 'queued'
                         ELSE r.state END,
        queued_at = CASE WHEN cur.old_state = 'done' THEN now()
                         ELSE r.queued_at END,
        run_after = CASE WHEN cur.old_state = 'done' THEN now()
                         ELSE r.run_after END,
        fair_key  = CASE WHEN cur.old_state IN ('done', 'queued')
                         THEN COALESCE($4::text, r.fair_key)
                         ELSE r.fair_key END
    FROM cur
    WHERE r.unit_id = cur.unit_id
    RETURNING r.state AS new_state
),
ins AS (
    INSERT INTO run_queue (
        unit_id, unit_kind, fair_key, input_seq, consumed_seq,
        control_input_seq, control_consumed_seq
    )
    SELECT $1::uuid, $2::text, $4::text, $5::bigint, $5::bigint,
           $3::bigint, $3::bigint - 1
    WHERE NOT EXISTS (SELECT 1 FROM cur)
    ON CONFLICT (unit_id) DO UPDATE SET
        control_input_seq = GREATEST(
            run_queue.control_input_seq, EXCLUDED.control_input_seq
        ),
        control_consumed_seq = CASE
            WHEN run_queue.control_input_seq = run_queue.control_consumed_seq
            THEN GREATEST(
                run_queue.control_consumed_seq,
                EXCLUDED.control_input_seq - 1
            )
            ELSE run_queue.control_consumed_seq
        END,
        fair_key = COALESCE(EXCLUDED.fair_key, run_queue.fair_key)
    RETURNING state AS new_state
)
SELECT new_state FROM upd
UNION ALL
SELECT new_state FROM ins
"""

_FENCE_SQL = """
SELECT 1 FROM run_queue
WHERE unit_id = $1::uuid AND lease_token = $2::bigint AND state = 'leased'
FOR SHARE
"""

# Reaper candidate scan. FOR SHARE SKIP LOCKED is deliberate and asymmetric:
#   * rows exclusively locked (an in-flight steal/claim/wedged transaction)
#     are SKIPPED — one wedged row never stalls the sweep (§5.2 per-row form);
#   * rows share-locked by an in-flight persist fence ARE returned — the
#     per-row steal below then blocks until that persist commits, preserving
#     the fence's check-then-write atomicity.
# Share locks from this statement last only until the statement ends (the
# reaper never holds them across steals).
_REAP_CANDIDATES_SQL = """
SELECT unit_id, unit_kind, leased_by, lease_token,
       attempts_since_completion, max_attempts
FROM run_queue
WHERE state = 'leased'
  AND leased_until < now() - make_interval(secs => $1::float8)
  AND NOT EXISTS (
      SELECT 1 FROM vm_workspace_recovery_jobs AS recovery_job
      WHERE recovery_job.job_id = run_queue.unit_id
        AND recovery_job.resolved_at IS NULL
  )
  AND ($2::text IS NULL OR unit_kind = $2::text)
ORDER BY leased_until
LIMIT $3::int
FOR SHARE SKIP LOCKED
"""

# Per-row steal: a single-statement CAS. Guarded by the candidate's
# lease_token AND a re-check of state + expiry, so a heartbeat renewal, a
# voluntary release, or a competing reaper between candidate-read and steal
# makes this a no-op (zero rows) instead of a double steal. Blocks behind an
# in-flight persist's FOR SHARE fence and re-evaluates the guard on wake.
_REAP_STEAL_SQL = """
WITH previous AS (
    SELECT unit_id, interrupt_admission_lease_token,
           interrupt_admission_turn_id
    FROM run_queue
    WHERE unit_id = $1::uuid
      AND lease_token = $2::bigint
      AND state = 'leased'
      AND leased_until < now() - make_interval(secs => $4::float8)
      AND NOT EXISTS (
          SELECT 1 FROM vm_workspace_recovery_jobs AS recovery_job
          WHERE recovery_job.job_id = run_queue.unit_id
            AND recovery_job.resolved_at IS NULL
      )
    FOR UPDATE
)
UPDATE run_queue AS queue SET
    lease_token = queue.lease_token + 1,
    state = CASE WHEN queue.attempts_since_completion >= queue.max_attempts
                 THEN 'parked' ELSE 'queued' END,
    park_reason = CASE WHEN queue.attempts_since_completion >= queue.max_attempts
                       THEN 'reaper_max_attempts' ELSE queue.park_reason END,
    parked_at = CASE WHEN queue.attempts_since_completion >= queue.max_attempts
                     THEN now() ELSE queue.parked_at END,
    leased_by = NULL,
    -- The holder missed its heartbeats: it is dead, wedged, or partitioned.
    -- Clearing the affinity hint keeps the grace window from delaying the
    -- successor's claim on behalf of a pod that will never come back.
    last_leased_by = NULL,
    leased_until = NULL,
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    queued_at = now(),
    run_after = now() + make_interval(secs => $3::float8)
FROM previous
WHERE queue.unit_id = previous.unit_id
RETURNING queue.unit_id, queue.unit_kind, queue.state,
          queue.attempts_since_completion, queue.lease_token,
          previous.interrupt_admission_lease_token,
          previous.interrupt_admission_turn_id
"""

_UNPARK_SQL = """
UPDATE run_queue SET
    state = 'queued',
    attempts_since_completion = 0,
    attach_failures = 0,
    park_reason = NULL,
    parked_at = NULL,
    last_error = NULL,
    last_error_signature = NULL,
    run_after = now(),
    queued_at = now()
WHERE unit_id = $1::uuid AND state = 'parked'
  AND NOT EXISTS (
      SELECT 1 FROM vm_workspace_recovery_jobs AS recovery_job
      WHERE recovery_job.job_id = run_queue.unit_id
        AND recovery_job.resolved_at IS NULL
  )
RETURNING state
"""

# Attach-failed release (stateless_turn_resilience.md step 2). The claim
# already counted this attempt in attempts_since_completion, so this does not
# increment it again (same convention as _RELEASE_SQL); it records the failure,
# advances the consecutive same-signature counter, applies the executor-chosen
# backoff, and parks — with a reason — when the claim count reached
# max_attempts or the same signature is failing for the $6-th time in a row.
# One fenced CAS: a stale token or a stolen lease changes nothing.
_RECORD_ATTACH_FAILURE_SQL = """
WITH cur AS (
    SELECT unit_id,
           attempts_since_completion AS attempts,
           max_attempts,
           CASE WHEN last_error_signature IS NOT DISTINCT FROM $3::text
                THEN attach_failures + 1 ELSE 1 END AS next_attach_failures
    FROM run_queue
    WHERE unit_id = $1::uuid AND lease_token = $2::bigint AND state = 'leased'
    FOR UPDATE
), verdict AS (
    SELECT unit_id, attempts, next_attach_failures,
           (attempts >= max_attempts OR next_attach_failures >= $6::int) AS will_park
    FROM cur
)
UPDATE run_queue AS queue SET
    attach_failures = verdict.next_attach_failures,
    last_error = $4::text,
    last_error_signature = $3::text,
    state = CASE WHEN verdict.will_park THEN 'parked' ELSE 'queued' END,
    park_reason = CASE WHEN verdict.will_park THEN 'attach_failed'
                       ELSE queue.park_reason END,
    parked_at = CASE WHEN verdict.will_park THEN now() ELSE queue.parked_at END,
    leased_by = NULL,
    last_leased_by = NULL,
    leased_until = NULL,
    interrupt_admission_lease_token = NULL,
    interrupt_admission_turn_id = NULL,
    queued_at = now(),
    run_after = now() + make_interval(secs => $5::float8)
FROM verdict
WHERE queue.unit_id = verdict.unit_id
RETURNING queue.state, queue.attempts_since_completion, queue.attach_failures,
          queue.park_reason, queue.run_after
"""

_QUEUE_STATE_SQL = """
SELECT state, park_reason, parked_at, last_error,
       attempts_since_completion, max_attempts, attach_failures,
       input_seq, consumed_seq, run_after
FROM run_queue WHERE unit_id = $1::uuid
"""

# Operator worklist: parked units newest first, with the owning thread (session
# units only; worker_batch rows carry no thread) for the admin capacity page.
_LIST_PARKED_DETAIL_SQL = """
SELECT queue.unit_id, queue.unit_kind, queue.park_reason, queue.parked_at,
       queue.attempts_since_completion, queue.max_attempts,
       queue.attach_failures, queue.last_error, queue.queued_at,
       (queue.input_seq IS NOT NULL
        AND (queue.consumed_seq IS NULL OR queue.input_seq > queue.consumed_seq))
           AS pending_input,
       thread.id AS thread_id, thread.title, thread.user_id,
       owner.preferred_username AS owner
FROM run_queue AS queue
LEFT JOIN run_queue_bg_tasks AS task
       ON task.unit_id = queue.unit_id AND queue.unit_kind = 'bg_task'
LEFT JOIN threads AS thread
       ON (thread.id = queue.unit_id AND queue.unit_kind = 'session_turn')
       OR (thread.id = task.thread_id AND queue.unit_kind = 'bg_task')
LEFT JOIN users AS owner ON owner.id = thread.user_id
WHERE queue.state = 'parked'
ORDER BY COALESCE(queue.parked_at, queue.queued_at) DESC, queue.unit_id
LIMIT $1::int
"""

_LIST_LEASED_SQL = """
SELECT unit_id, unit_kind, fair_key, leased_by, leased_until, lease_token,
       attempts_since_completion, max_attempts, queued_at,
       input_seq, consumed_seq, control_input_seq, control_consumed_seq,
       EXTRACT(EPOCH FROM (leased_until - now()))::float8 AS lease_remaining_seconds
FROM run_queue
WHERE state = 'leased' AND ($1::text IS NULL OR unit_kind = $1::text)
ORDER BY leased_until
"""

_LIST_PARKED_SQL = """
SELECT unit_id, unit_kind, fair_key, lease_token,
       attempts_since_completion, max_attempts, queued_at, run_after,
       input_seq, consumed_seq, control_input_seq, control_consumed_seq
FROM run_queue
WHERE state = 'parked' AND ($1::text IS NULL OR unit_kind = $1::text)
ORDER BY queued_at
"""

_WATERMARKS_SQL = """
SELECT state, input_seq, consumed_seq, control_input_seq, control_consumed_seq
FROM run_queue WHERE unit_id = $1::uuid
"""


def _uuid(value: UUID | str) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


# =============================================================================
# Admission
# =============================================================================


async def enqueue_unit(
    conn: Executor,
    *,
    unit_id: UUID | str,
    unit_kind: str,
    fair_key: str | None = None,
    dedup_key: str | None = None,
    priority: int = 0,
    run_after: datetime | None = None,
    input_seq: int | None = None,
) -> EnqueueResult:
    """Admit a unit of work (§5.1 admission), one statement, state-aware.

    Durable units (``dedup_key is None``) upsert on ``unit_id``:

    * absent → fresh ``'queued'`` row (:data:`ENQUEUE_INSERTED`)
    * ``'done'`` → re-queued with fresh ``queued_at`` (:data:`ENQUEUE_REQUEUED`)
    * ``'queued'`` → monotonic merge — ``priority`` GREATEST, ``run_after``
      LEAST (an enqueue may only make the unit *more* runnable; fresh input
      cuts an error backoff short), ``input_seq`` GREATEST; ``queued_at``
      untouched so the FIFO position is preserved (:data:`ENQUEUE_UPDATED`)
    * ``'leased'`` → ``input_seq`` GREATEST **only** — flipping a leased row
      breaks the lease; completion re-queues via the watermark
      (:data:`ENQUEUE_INPUT_RECORDED`)
    * ``'parked'`` → ``input_seq`` GREATEST only; the unit STAYS parked —
      re-queueing a poison unit requires an explicit :func:`unpark_unit`
      (:data:`ENQUEUE_PARKED`)

    ``lease_token`` is never written by any enqueue path.

    Collapsible tasks (``dedup_key`` given) instead INSERT against the
    queued-only partial dedup index (``ON CONFLICT DO NOTHING``): while one
    instance is *queued*, further signals collapse (:data:`ENQUEUE_DEDUPED`);
    once it is claimed, the next signal inserts a fresh pending row — one
    running + one pending coexist by design. Each signal must carry a FRESH
    ``unit_id`` (raises ``ValueError`` on primary-key reuse).
    """
    if dedup_key is not None:
        try:
            row = await conn.fetchrow(
                _ENQUEUE_DEDUP_SQL,
                _uuid(unit_id),
                unit_kind,
                dedup_key,
                fair_key,
                priority,
                run_after,
                input_seq,
            )
        except Exception as exc:
            if getattr(exc, "sqlstate", None) == "23505":
                raise ValueError(
                    "dedup-keyed enqueue reused an existing unit_id "
                    f"({unit_id}); collapsible tasks must enqueue a fresh "
                    "unit_id per signal"
                ) from exc
            raise
        if row is None:
            return EnqueueResult(status=ENQUEUE_DEDUPED, state=STATE_QUEUED)
        return EnqueueResult(status=ENQUEUE_INSERTED, state=row["state"])

    row = await conn.fetchrow(
        _ENQUEUE_SQL,
        _uuid(unit_id),
        unit_kind,
        fair_key,
        priority,
        run_after,
        input_seq,
    )
    return EnqueueResult(
        status=_OLD_STATE_TO_STATUS[row["old_state"]], state=row["new_state"]
    )


async def record_input_seq(
    conn: Executor,
    *,
    unit_id: UUID | str,
    unit_kind: str,
    input_seq: int,
    fair_key: str | None = None,
) -> str:
    """Record new input for a unit whatever it is currently doing (§5.3.1).

    The input-during-anything path — call it in the same transaction as the
    ``thread_messages`` insert so "message durable ⟺ watermark advanced":

    * row missing → fresh ``'queued'`` row carrying the watermark, with
      ``consumed_seq`` initialized to ``input_seq - 1`` (the lane-flip
      boundary: pre-queue history is never re-answered)
    * ``'done'`` → revived to ``'queued'`` (``queued_at``/``run_after`` reset)
    * ``'queued'`` → watermark merged, queue position untouched
    * ``'leased'`` → watermark merged ONLY; the running turn's completion sees
      ``input_seq > consumed_seq`` and re-queues (never touches the lease)
    * ``'parked'`` → watermark merged; STAYS parked (explicit unpark only)

    ``input_seq`` merges with GREATEST — the watermark never regresses.
    Returns the resulting state.
    """
    return await conn.fetchval(
        _RECORD_INPUT_SQL, _uuid(unit_id), unit_kind, input_seq, fair_key
    )


async def record_control_seq(
    conn: Executor,
    *,
    unit_id: UUID | str,
    unit_kind: str,
    control_seq: int,
    baseline_input_seq: int,
    fair_key: str | None = None,
) -> str:
    """Record a stateless control admission in the queue transaction.

    A missing/done row becomes runnable; a queued row keeps its FIFO position;
    a leased row advances only the control watermark so completion performs the
    requeue; and a parked row stays parked. ``lease_token`` is never changed.

    Call this in the same transaction as ``thread_control_requests`` INSERT so
    request durability and wake durability are one commit. Public admission
    rejects the returned ``parked`` state by raising inside that transaction.
    """
    return await conn.fetchval(
        _RECORD_CONTROL_SQL,
        _uuid(unit_id),
        unit_kind,
        int(control_seq),
        fair_key,
        int(baseline_input_seq),
    )


# =============================================================================
# Claim / lease lifecycle
# =============================================================================


async def claim_unit(
    conn: Executor,
    *,
    unit_kind: str,
    pod_name: str,
    prefer_unit_id: UUID | str | None = None,
    lease_ttl_seconds: float = LEASE_TTL_SECONDS,
    affinity_grace_seconds: float = AFFINITY_GRACE_SECONDS,
) -> ClaimedUnit | None:
    """Claim the next runnable unit (§5.2 claim CTE) — or None if idle.

    Single statement, ``FOR UPDATE SKIP LOCKED`` inside the CTE only:
    claim-and-mark is atomic (no READ COMMITTED double dispatch) and no lock
    outlives the call. Order: ``priority DESC, queued_at, enqueue_ord`` —
    ``queued_at`` is reset on completion/release/steal, which is what makes
    this round-robin within a priority class; ``enqueue_ord`` makes
    equal-timestamp claims deterministic FIFO. Rows with ``run_after`` in the
    future are invisible.

    Affinity is TWO cooperating mechanisms, both soft (§5.3.4):

    * ``prefer_unit_id`` is tried FIRST regardless of queue order — the pod
      asking for the thread it just served. On miss (not queued / backed off
      / locked) the general claim runs.
    * ``affinity_grace_seconds`` then hides a freshly queued unit from OTHER
      pods for that window, so the last holder's next poll wins even when a
      cold pod polls first. Without it the prefer-path lost the 0.5s poll
      race about half the time on k3d, and every loss paid a full re-attach.

    Correctness never depends on either: after the grace any pod claims the
    unit, and the rebuild-from-Postgres path is what makes a cold winner
    correct anyway.

    The claim bumps ``lease_token`` (fencing: same-pod re-claims get a new
    token, invalidating stragglers of the previous claim) and increments
    ``attempts_since_completion`` — the claim is the ONLY place attempts are
    counted. Compare the returned watermarks before invoking the LLM
    (skip-if-answered, §5.1).
    """
    if prefer_unit_id is not None:
        row = await conn.fetchrow(
            _CLAIM_PREFER_SQL,
            unit_kind,
            pod_name,
            lease_ttl_seconds,
            _uuid(prefer_unit_id),
        )
        if row is not None:
            return ClaimedUnit(**dict(row))
    row = await conn.fetchrow(
        _CLAIM_SQL, unit_kind, pod_name, lease_ttl_seconds, affinity_grace_seconds
    )
    if row is None:
        return None
    return ClaimedUnit(**dict(row))


async def heartbeat_unit(
    conn: Executor,
    *,
    unit_id: UUID | str,
    lease_token: int,
    lease_ttl_seconds: float = LEASE_TTL_SECONDS,
) -> datetime | None:
    """Renew the lease; ``None`` means the lease is LOST (§5.2).

    Run from an async task independent of the graph loop and tool calls
    (every :data:`HEARTBEAT_INTERVAL_SECONDS` = TTL/3), so a 10-minute tool
    call needs nothing special. On ``None`` the executor must abort the turn,
    discard un-persisted work, and attempt no further fenced persists.
    """
    return await conn.fetchval(
        _HEARTBEAT_SQL, _uuid(unit_id), lease_token, lease_ttl_seconds
    )


async def open_interrupt_admission(
    conn: Executor,
    *,
    unit_id: UUID | str,
    lease_token: int,
    turn_id: int,
) -> bool:
    """Open the stateless interrupt inbox for one exact lease and turn.

    The serving executor calls this only after the concrete input turn is
    known and its consumer is ready. The update is fenced by the current
    ``session_turn`` lease and refuses to replace a different open turn. A
    repeated open of the same pair is idempotent.

    Returns ``False`` when the lease is stale, the unit is not a leased
    session turn, or a different turn is already open. Positive tokens and
    turn ids are required because zero is not a real claim/turn generation.
    """
    if int(lease_token) <= 0:
        raise ValueError("lease_token must be positive")
    if int(turn_id) <= 0:
        raise ValueError("turn_id must be positive")
    opened = await conn.fetchval(
        _OPEN_INTERRUPT_ADMISSION_SQL,
        _uuid(unit_id),
        int(lease_token),
        int(turn_id),
    )
    return opened is not None


async def close_interrupt_admission(
    conn: Executor,
    *,
    unit_id: UUID | str,
    lease_token: int,
    turn_id: int,
    completed_input_seq: int | None = None,
) -> bool:
    """Close one exact stateless interrupt admission window.

    Closure and REST admission update/lock the same queue row, providing the
    boundary for the executor's final drain. The expected turn prevents stale
    cleanup from closing a newer window. After full settlement and cloud-push
    completion, the executor also checkpoints the exact next unanswered human
    row in this same statement. Thus no durable state can expose a NULL gate
    while leaving the settled input replayable.

    Repeating a close after the pair is already NULL is idempotent while the
    exact lease remains current. A repeated checkpoint succeeds only when its
    human seq is already consumed; it can never advance a later row.

    Returns ``False`` for a stale lease, a non-session unit, or a different
    open turn. The caller must treat ``False`` as lost ownership unless it can
    independently prove another lifecycle path already cleared the lease.
    """
    if int(lease_token) <= 0:
        raise ValueError("lease_token must be positive")
    if int(turn_id) <= 0:
        raise ValueError("turn_id must be positive")
    completed_seq = (
        int(completed_input_seq) if completed_input_seq is not None else None
    )
    if completed_seq is not None and completed_seq <= 0:
        raise ValueError("completed_input_seq must be positive")
    closed = await conn.fetchval(
        _CLOSE_INTERRUPT_ADMISSION_SQL,
        _uuid(unit_id),
        int(lease_token),
        int(turn_id),
        completed_seq,
    )
    return closed is not None


async def complete_unit(
    conn: Executor,
    *,
    unit_id: UUID | str,
    lease_token: int,
    consumed_seq: int | None,
) -> str | None:
    """Complete a turn (§5.1 Complete) — one fenced statement.

    ``consumed_seq`` is the input watermark the finished turn actually
    answered — normally the claim's ``input_seq`` (or newer, if the executor
    drained later input mid-turn). The statement, atomically:

    * records the consumed watermark and resets
      ``attempts_since_completion`` (the ONLY attempt reset besides unpark),
    * resets ``queued_at`` (fairness: the unit re-enters at the back of its
      priority class) and clears lease fields + ``run_after`` backoff,
    * re-queues iff newer input is already recorded (``input_seq`` ahead of
      the consumed watermark, NULL-safe: any input beats a NULL watermark) —
      the re-queue-on-completion half of the double-texting policy; else
      ``'done'``.

    Returns the resulting state, or ``None`` if fenced out (stale token /
    stolen lease) — the caller must treat the turn as LOST: its persists
    either landed before the steal or were rejected by the fence, and the
    successor's claim decides what still needs answering via the watermarks.
    ``lease_token`` is not touched — completion never resets fencing.
    """
    return await conn.fetchval(_COMPLETE_SQL, _uuid(unit_id), lease_token, consumed_seq)


async def release_unit(
    conn: Executor,
    *,
    unit_id: UUID | str,
    lease_token: int,
    backoff_seconds: float = 0.0,
    error: bool = False,
    park_reason: str | None = None,
    last_error: str | None = None,
) -> str | None:
    """Voluntarily release a lease back to ``'queued'`` (§5.1 error release).

    Distinct from :func:`complete_unit`: ``consumed_seq`` is untouched (the
    turn did NOT answer its input) and ``attempts_since_completion`` is NOT
    reset — the claim already counted this attempt.

    The reaper never sees a voluntarily released row (it is no longer an
    expired lease), so a plain release cannot bound a unit that fails the
    same way on every claim. A caller releasing for a DETERMINISTIC failure
    passes ``park_reason``: the failure is counted in the unit's failure
    record (see ``_RELEASE_DETERMINISTIC_SQL``) and, once that count reaches
    the row's own ``max_attempts``, the release parks with that reason
    instead of re-queueing (recording ``last_error``); its default backoff is
    the attach-failure schedule by that count. A plain release — transient
    failures with their
    :func:`transient_release_backoff_seconds`, shutdown hand-backs — never
    parks and never touches that record. Parking is only the queue half of
    the disposition; a session caller journals the visible outcome in the
    same transaction.

    ``run_after`` moves to ``now() + backoff_seconds``. With ``error=True``
    and no explicit backoff, a linear default backoff is applied instead
    (``5s × attempts_since_completion``, mirroring the reaper's scale).
    ``queued_at`` is reset — a released unit re-enters at the back of its
    priority class (§5.1 fairness).

    Returns the resulting state, or ``None`` if fenced out (the lease was
    already stolen — nothing was changed).
    """
    if park_reason is None:
        return await conn.fetchval(
            _RELEASE_SQL,
            _uuid(unit_id),
            lease_token,
            float(backoff_seconds),
            bool(error),
            _RELEASE_BACKOFF_BASE_SECONDS,
        )
    return await conn.fetchval(
        _RELEASE_DETERMINISTIC_SQL,
        _uuid(unit_id),
        lease_token,
        float(backoff_seconds),
        bool(error),
        _RELEASE_BACKOFF_BASE_SECONDS,
        str(park_reason),
        last_error,
        DETERMINISTIC_RELEASE_SIGNATURE,
        ATTACH_FAILURE_BACKOFF_CAP_SECONDS,
    )


async def park_unit(
    conn: Executor,
    *,
    unit_id: UUID | str,
    lease_token: int,
    reason: str | None = None,
) -> str | None:
    """Fail closed: exact leased claim -> ``'parked'`` without consumption.

    This is the non-retry disposition for a fully quiesced claimant that may
    already have produced an external side effect but cannot truthfully call
    :func:`complete_unit`. It preserves input/consumed watermarks, attempt
    counters, and the fencing token; clears the old physical/admission owner;
    and remains non-claimable until an operator calls :func:`unpark_unit`.

    Returns ``'parked'`` on success or ``None`` when the exact token no longer
    owns a leased row. Callers must quiesce every local writer before calling
    this function, just as they must before release/complete.

    ``reason`` is recorded as ``park_reason`` (with ``parked_at``) so an owner
    or operator can see why; ``None`` keeps whatever reason the row carries.
    """
    return await conn.fetchval(_PARK_SQL, _uuid(unit_id), lease_token, reason)


async def record_attach_failure(
    conn: Executor,
    *,
    unit_id: UUID | str,
    lease_token: int,
    error: str,
    signature: str,
    backoff_seconds: float,
    signature_park_threshold: int = ATTACH_FAILURE_SIGNATURE_PARK_THRESHOLD,
) -> dict[str, Any] | None:
    """Release after a failed attach: count it, back off, park when bounded.

    The claim already counted this attempt in ``attempts_since_completion``
    (same convention as :func:`release_unit`), so the statement does not
    increment it again. It records ``last_error`` / ``last_error_signature``,
    advances ``attach_failures`` (reset to 1 when the signature changes), moves
    ``run_after`` by ``backoff_seconds``, and parks with
    ``park_reason='attach_failed'`` when the claim count already reached
    ``max_attempts`` or this is the ``signature_park_threshold``-th consecutive
    failure with the same signature. Returns the resulting row slice
    (``state``, ``attempts_since_completion``, ``attach_failures``,
    ``park_reason``, ``run_after``) or ``None`` when fenced out.
    """
    row = await conn.fetchrow(
        _RECORD_ATTACH_FAILURE_SQL,
        _uuid(unit_id),
        lease_token,
        signature,
        error,
        float(backoff_seconds),
        int(signature_park_threshold),
    )
    return dict(row) if row is not None else None


async def fence_lease(
    conn: Executor,
    *,
    unit_id: UUID | str,
    lease_token: int,
) -> bool:
    """§5.2 persist fence. MUST run INSIDE an open transaction.

    Contract (verbatim from the design — every fenced store depends on it):

    * Call as the FIRST statement of every persist transaction
      (``thread_messages`` inserts, checkpoint puts, completion CASes).
    * ``False`` (zero rows) ⇒ the lease is lost ⇒ ABORT the transaction —
      roll back, persist nothing, stop.
    * ``FOR SHARE`` holds the queue row against a concurrent steal until this
      transaction commits: the reaper's steal UPDATE blocks behind it, so
      "check token then write" can never interleave with a steal.

    The connection MUST be an ``asyncpg.Connection`` inside an explicit
    ``conn.transaction()`` block. In autocommit (or on a pool proxy) the
    share lock evaporates at statement end and the fence protects nothing —
    that is a caller bug this function cannot detect.

    The fence protects Postgres state only: a zombie's in-flight SSH/tmux
    side effect is out of reach (§5.2 fencing boundary), and it is a
    correctness boundary among cooperating pods, not a security boundary
    (§5.6).
    """
    row = await conn.fetchrow(_FENCE_SQL, _uuid(unit_id), lease_token)
    return row is not None


# =============================================================================
# Reaper / operator verbs
# =============================================================================


async def reap_expired(
    conn: Executor,
    *,
    unit_kind: str | None = None,
    grace_seconds: float = REAPER_GRACE_SECONDS,
    max_rows: int = 50,
    backoff_base_seconds: float = 5.0,
    jitter: float = 0.2,
    session_steal: Callable[..., Awaitable[StolenUnit | None]] | None = None,
    worker_steal: Callable[..., Awaitable[StolenUnit | None]] | None = None,
) -> list[StolenUnit]:
    """Steal expired leases, per-row (§5.2 reaper) — never one bulk UPDATE.

    Two-step per pass:

    1. Candidate scan: ``FOR SHARE SKIP LOCKED`` over rows whose
       ``leased_until`` passed more than ``grace_seconds`` ago. Rows
       exclusively locked (an in-flight competing steal or claim) are
       skipped — one wedged row never stalls the sweep; rows share-locked by
       an in-flight persist fence ARE candidates.
    2. Per row, its own single-statement CAS steal: ``lease_token + 1``
       (fences the zombie even before any re-claim), ``state = 'parked'`` at
       ``max_attempts`` else ``'queued'``, backoff-with-jitter on
       ``run_after`` (``base × attempts × (1 + U(0, jitter))``), lease fields
       cleared, ``queued_at`` reset. The guard re-checks token + state +
       expiry, so a heartbeat renewal or competing reaper turns the steal
       into a no-op. A steal blocks behind an in-flight persist's ``FOR
       SHARE`` and re-evaluates on wake — the §5.2 fence contract.

    Each steal commits independently (run this OUTSIDE any transaction): a
    crash mid-pass keeps the steals already made.

    ``session_steal`` and ``worker_steal`` are optional layering seams. Each
    owns the exact per-row transition for its unit kind and returns a
    :class:`StolenUnit` or None. The orchestrator uses the session hook to lock
    ``threads -> run_queue`` and record old-claim I/O-quiescence provenance in
    the same transaction as the token bump. The worker hook reconciles durable
    disposition before retry/exhaustion. Without a matching hook, the existing
    queue-only statement below remains authoritative.

    Layering: without that callback this module touches only ``run_queue``. The CALLER (the
    leader-gated reaper loop — advisory lock ``RUN_QUEUE_REAPER_ID`` in
    ``src/orchestrator/database/lock_ids.py``) bumps ``threads.events_epoch`` for
    session units and writes the ``turn.interrupted`` / ``turn.parked``
    system frames from the returned records. Safe under a dual-leader window
    by the per-row CAS, not by the election.
    """
    candidates: Sequence[Any] = await conn.fetch(
        _REAP_CANDIDATES_SQL, float(grace_seconds), unit_kind, int(max_rows)
    )
    stolen: list[StolenUnit] = []
    for cand in candidates:
        attempts = cand["attempts_since_completion"]
        backoff = backoff_base_seconds * attempts * (1.0 + random.uniform(0.0, jitter))
        steal = (
            session_steal
            if cand["unit_kind"] == UNIT_KIND_SESSION_TURN
            else worker_steal
            if cand["unit_kind"] == UNIT_KIND_WORKER_BATCH
            else None
        )
        if steal is not None:
            unit = await steal(
                conn,
                candidate=cand,
                backoff_seconds=backoff,
                grace_seconds=float(grace_seconds),
            )
            if unit is not None:
                stolen.append(unit)
            continue
        row = await conn.fetchrow(
            _REAP_STEAL_SQL,
            cand["unit_id"],
            cand["lease_token"],
            backoff,
            float(grace_seconds),
        )
        if row is None:
            continue  # renewed, released, or stolen by a competitor meanwhile
        stolen.append(
            StolenUnit(
                unit_id=row["unit_id"],
                unit_kind=row["unit_kind"],
                state=row["state"],
                attempts_since_completion=row["attempts_since_completion"],
                leased_by=cand["leased_by"],
                lease_token=row["lease_token"],
                previous_lease_token=cand["lease_token"],
                interrupt_admission_turn_id=(
                    int(row["interrupt_admission_turn_id"])
                    if row["interrupt_admission_turn_id"] is not None
                    and row["interrupt_admission_lease_token"] == cand["lease_token"]
                    else None
                ),
            )
        )
    return stolen


async def unpark_unit(conn: Executor, *, unit_id: UUID | str) -> bool:
    """Operator verb: parked → queued, attempts reset, runnable now.

    The ONLY path out of ``'parked'`` — neither enqueue nor input recording
    revives a parked unit. Returns ``False`` if the unit is not parked.
    """
    state = await conn.fetchval(_UNPARK_SQL, _uuid(unit_id))
    return state is not None


# =============================================================================
# Read models
# =============================================================================


async def list_active(
    conn: Executor, *, unit_kind: str | None = None
) -> dict[str, list[dict[str, Any]]]:
    """Operator read model: current leases + parked units, as plain dicts.

    ``leased`` rows carry ``lease_remaining_seconds`` (negative = expired,
    awaiting the reaper) for renewal-health displays; ``parked`` rows are the
    unpark worklist. Read-only; values are diagnostics, never inputs to
    correctness decisions.
    """
    leased = await conn.fetch(_LIST_LEASED_SQL, unit_kind)
    parked = await conn.fetch(_LIST_PARKED_SQL, unit_kind)
    return {
        "leased": [dict(row) for row in leased],
        "parked": [dict(row) for row in parked],
    }


async def queue_depth_for(
    conn: Executor, *, unit_id: UUID | str
) -> QueueWatermarks | None:
    """The unit's watermarks + an honest pending flag; ``None`` if no row.

    ``has_pending_input`` and ``has_pending_control`` are independent watermark
    comparisons. The former is NOT a message count — seqs are not dense per
    unit, so callers needing an exact depth must count ``thread_messages`` rows
    above the consumed watermark themselves.
    """
    row = await conn.fetchrow(_WATERMARKS_SQL, _uuid(unit_id))
    if row is None:
        return None
    input_seq = row["input_seq"]
    consumed_seq = row["consumed_seq"]
    has_pending = input_seq is not None and (
        consumed_seq is None or input_seq > consumed_seq
    )
    control_input_seq = int(row["control_input_seq"] or 0)
    control_consumed_seq = int(row["control_consumed_seq"] or 0)
    return QueueWatermarks(
        state=row["state"],
        input_seq=input_seq,
        consumed_seq=consumed_seq,
        has_pending_input=has_pending,
        control_input_seq=control_input_seq,
        control_consumed_seq=control_consumed_seq,
        has_pending_control=control_input_seq > control_consumed_seq,
    )


async def queue_state_for(
    conn: Executor, *, unit_id: UUID | str
) -> dict[str, Any] | None:
    """Owner/operator read model of one unit's lifecycle; ``None`` if no row.

    Plain dict: ``state``, ``park_reason``, ``parked_at`` (datetime or None),
    ``last_error``, ``attempts``, ``max_attempts``, ``attach_failures``,
    ``pending_input`` (the same NULL-safe watermark comparison as
    :func:`queue_depth_for`), ``run_after``. Whether a park is *retryable* is
    decided by the orchestrator (it needs the thread's stop markers), not here.
    """
    row = await conn.fetchrow(_QUEUE_STATE_SQL, _uuid(unit_id))
    if row is None:
        return None
    input_seq = row["input_seq"]
    consumed_seq = row["consumed_seq"]
    return {
        "state": row["state"],
        "park_reason": row["park_reason"],
        "parked_at": row["parked_at"],
        "last_error": row["last_error"],
        "attempts": int(row["attempts_since_completion"] or 0),
        "max_attempts": int(row["max_attempts"] or 0),
        "attach_failures": int(row["attach_failures"] or 0),
        "pending_input": input_seq is not None
        and (consumed_seq is None or input_seq > consumed_seq),
        "run_after": row["run_after"],
    }


async def list_parked(conn: Executor, *, limit: int = 50) -> list[dict[str, Any]]:
    """Parked units newest first, joined to their session thread and owner.

    The admin capacity page's unpark worklist. Diagnostics only.
    """
    bounded = max(1, min(int(limit), 500))
    rows = await conn.fetch(_LIST_PARKED_DETAIL_SQL, bounded)
    return [dict(row) for row in rows]
