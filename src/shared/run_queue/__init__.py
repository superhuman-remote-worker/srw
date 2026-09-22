"""Shared run_queue substrate — work queue + recorded lease + fencing (S1).

The single home of the claim/heartbeat/fence/complete semantics from
knowledge-base/knowledge/features/stateless_agents.md §5.1/§5.2, imported by BOTH the
orchestrator (enqueue paths, reaper loop, read models) and the agent executor
(claim, heartbeat, fence, complete, release) — the ``orch_surface``
precedent. Schema: ``orchestrator/database/migrations/app/0115_run_queue.sql``.

Contract summary (full invariants on the functions in ``queries.py``):

* the lease is recorded row state (``leased_until`` + ``lease_token``), never
  a held lock — SKIP LOCKED exists only inside the one-statement claim;
* ``lease_token`` is the monotonic fencing token (bumped on every claim and
  steal, never reset) and :func:`fence_lease` must open every persist
  transaction;
* watermarks (``input_seq`` / ``consumed_seq``) carry skip-if-answered and
  re-queue-on-completion; input during a leased turn bumps the watermark
  only;
* attempts reset only on completion/unpark; the reaper parks at
  ``max_attempts``; dedup is queued-only; parked units revive only via
  :func:`unpark_unit`.
"""

from shared.run_queue.queries import (
    AFFINITY_GRACE_SECONDS,
    ATTACH_FAILURE_BACKOFF_CAP_SECONDS,
    ATTACH_FAILURE_BACKOFF_SECONDS,
    ATTACH_FAILURE_SIGNATURE_PARK_THRESHOLD,
    DETERMINISTIC_RELEASE_SIGNATURE,
    ENQUEUE_DEDUPED,
    ENQUEUE_INPUT_RECORDED,
    ENQUEUE_INSERTED,
    ENQUEUE_PARKED,
    ENQUEUE_REQUEUED,
    ENQUEUE_UPDATED,
    HEARTBEAT_INTERVAL_SECONDS,
    LANE_PINNED,
    LANE_STATELESS,
    LEASE_TTL_SECONDS,
    PARK_REASON_ATTACH_FAILED,
    PARK_REASON_CLAIM_LOSS_HOLD,
    PARK_REASON_COMPLETION_CAS_FAILED,
    PARK_REASON_REAPER_MAX_ATTEMPTS,
    PARK_REASON_RETRY_EXHAUSTED,
    PARK_REASON_SHUTDOWN_CANCELLED,
    REAPER_GRACE_SECONDS,
    REAPER_INTERVAL_SECONDS,
    STATE_DONE,
    STATE_LEASED,
    STATE_PARKED,
    STATE_QUEUED,
    UNIT_KIND_BG_TASK,
    UNIT_KIND_SESSION_TURN,
    RETRYABLE_PARK_REASONS,
    TRANSIENT_RELEASE_BACKOFF_BASE_SECONDS,
    TRANSIENT_RELEASE_BACKOFF_CAP_SECONDS,
    UNIT_KIND_WORKER_BATCH,
    attach_failure_backoff_seconds,
    claim_unit,
    close_interrupt_admission,
    complete_unit,
    enqueue_unit,
    fence_lease,
    heartbeat_unit,
    list_active,
    list_parked,
    open_interrupt_admission,
    park_unit,
    queue_depth_for,
    queue_state_for,
    reap_expired,
    transient_release_backoff_seconds,
    record_attach_failure,
    record_control_seq,
    record_input_seq,
    release_unit,
    unpark_unit,
)
from shared.run_queue.types import (
    ClaimedUnit,
    EnqueueResult,
    QueueWatermarks,
    StolenUnit,
)

__all__ = [
    "AFFINITY_GRACE_SECONDS",
    "ATTACH_FAILURE_BACKOFF_CAP_SECONDS",
    "ATTACH_FAILURE_BACKOFF_SECONDS",
    "ATTACH_FAILURE_SIGNATURE_PARK_THRESHOLD",
    "DETERMINISTIC_RELEASE_SIGNATURE",
    "TRANSIENT_RELEASE_BACKOFF_BASE_SECONDS",
    "TRANSIENT_RELEASE_BACKOFF_CAP_SECONDS",
    "transient_release_backoff_seconds",
    "PARK_REASON_ATTACH_FAILED",
    "PARK_REASON_CLAIM_LOSS_HOLD",
    "PARK_REASON_COMPLETION_CAS_FAILED",
    "PARK_REASON_REAPER_MAX_ATTEMPTS",
    "PARK_REASON_RETRY_EXHAUSTED",
    "PARK_REASON_SHUTDOWN_CANCELLED",
    "RETRYABLE_PARK_REASONS",
    "attach_failure_backoff_seconds",
    "list_parked",
    "queue_state_for",
    "record_attach_failure",
    "ENQUEUE_DEDUPED",
    "ENQUEUE_INPUT_RECORDED",
    "ENQUEUE_INSERTED",
    "ENQUEUE_PARKED",
    "ENQUEUE_REQUEUED",
    "ENQUEUE_UPDATED",
    "HEARTBEAT_INTERVAL_SECONDS",
    "LANE_PINNED",
    "LANE_STATELESS",
    "LEASE_TTL_SECONDS",
    "REAPER_GRACE_SECONDS",
    "REAPER_INTERVAL_SECONDS",
    "STATE_DONE",
    "STATE_LEASED",
    "STATE_PARKED",
    "STATE_QUEUED",
    "UNIT_KIND_BG_TASK",
    "UNIT_KIND_SESSION_TURN",
    "UNIT_KIND_WORKER_BATCH",
    "ClaimedUnit",
    "EnqueueResult",
    "QueueWatermarks",
    "StolenUnit",
    "claim_unit",
    "close_interrupt_admission",
    "complete_unit",
    "enqueue_unit",
    "fence_lease",
    "heartbeat_unit",
    "list_active",
    "open_interrupt_admission",
    "park_unit",
    "queue_depth_for",
    "reap_expired",
    "record_control_seq",
    "record_input_seq",
    "release_unit",
    "unpark_unit",
]
