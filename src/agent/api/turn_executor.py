"""Stateless turn executor — the M3 claim loop (stateless_agents.md §5.1–§5.3).

One asyncio loop per pod: claim a ``session_turn`` unit from the shared
``run_queue`` (``src/shared/run_queue``), replay the thread through the
EXISTING persistent_app attach/restore machinery, drive exactly ONE turn by
injecting the oldest unanswered ``thread_messages`` row into the running
loop's user queue, and complete the unit with the consumed watermark. This
module is a DRIVER over ``persistent_app`` — it owns no session runtime of
its own (doc decision 4: maximum reuse of the pool attach/detach path).

Flow per claim (§5.1/§5.3.1):

1.  **Skip-if-answered** — ``consumed_seq >= input_seq`` at claim time means a
    predecessor answered but died before completing; complete immediately,
    never re-invoke the LLM.
2.  **Heartbeat** — an independent task renews the lease every
    ``HEARTBEAT_INTERVAL_SECONDS``; never an astream hook (a 10-minute tool
    call must not starve renewal). A failed renewal marks the shared
    :class:`~agent.api.lease_context.LeaseHandle` lost.
3.  **Claim bundle** — resolved config + attach payload delivered only
    against proof of the current lease (§5.6;
    ``GET /internal/units/{unit_id}/claim-bundle?lease_token=N``).
4.  **Attach with affinity** — same thread + same attach fingerprint reuses
    the live session; anything else detaches, scrubs process residue
    (§5.6 scrub-on-claim), and attaches fresh.
5.  **Inject** — the oldest unanswered ``role='human'`` row (LangChain role
    vocabulary — implementation log M0b) goes onto the loop's user queue with
    its DB row id, so every later persist upserts onto the existing row.
6.  **Complete** — after the turn-complete hook fires (and the turn-end cloud
    push, if any, is awaited under the lease — §5.3.5 option (i)),
    ``complete_unit`` records the consumed watermark; 'queued' means more
    input already arrived and the unit re-enters the queue.

Torn-turn invariant (§5.2, stated where the doc requires it): sessions
rebuild from ``thread_messages`` + the consumed watermark alone; a
checkpoint-ahead-of-messages tear is converged by the next claim's rebuild,
and skip-if-answered prevents the double-answer.

Greppable log contract (M6 fault-injection greps for these):

* ``run_queue claim``      — a lease was obtained
* ``run_queue complete``   — a turn (or skip) completed a unit
* ``run_queue release``    — a claim was voluntarily released (error path)
* ``lease lost``           — heartbeat/fence/completion found the lease gone
* ``fence rejected``       — a fenced persist/flush was refused
  (emitted by ``postgres_db`` and the journal writer)

S1 acceptance — zero in-process claim state: between loop iterations the ONLY
state this executor carries is (a) the soft-affinity hint
(``_prefer_unit_id`` + the attach fingerprint of the cached session) and
(b) the attached-session cache inside persistent_app itself. Neither is
load-bearing: a pod restart forgets both and the next claim rebuilds
everything from Postgres (thread_messages + run_queue watermarks).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import random
import re
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)
from uuid import UUID, uuid4

import httpx

from agent.api.lease_context import LeaseHandle, LeaseLostError, current_lease
from agent.api.models import JobStartRequest
from agent.api.orchestrator_client import (
    ClaimBundleError,
    CompletionNonTerminalReportError,
)
from shared.event_journal import append_system_frame, bump_epoch
from shared.job_freeze_types import (
    AUTO_CONTINUE_FREEZE_TYPES,
    FREEZE_TYPE_BATCH_BOUNDARY,
)
from shared.run_queue import (
    HEARTBEAT_INTERVAL_SECONDS,
    LANE_STATELESS,
    PARK_REASON_ATTACH_FAILED,
    PARK_REASON_COMPLETION_CAS_FAILED,
    PARK_REASON_RETRY_EXHAUSTED,
    PARK_REASON_SHUTDOWN_CANCELLED,
    RETRYABLE_PARK_REASONS,
    STATE_PARKED,
    STATE_QUEUED,
    UNIT_KIND_SESSION_TURN,
    ClaimedUnit,
    attach_failure_backoff_seconds,
    claim_unit,
    close_interrupt_admission,
    complete_unit,
    heartbeat_unit,
    open_interrupt_admission,
    park_unit,
    record_attach_failure,
    release_unit,
    transient_release_backoff_seconds,
)
from shared.session_permission_retirement import retire_stale_stateless_permissions
from shared.session_retirement import (
    acknowledge_session_claim_quiesced,
    active_claim_authority,
)
from shared.cloud_push_tasks import (
    PushAdoptionDeferred,
    claim_bg_task,
    complete_bg_task,
    enabled as cloud_push_recovery_enabled,
    fail_bg_task,
)
from shared.subagent_lifecycle import SubagentLifecycleError
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from shared.workspace_recovery import (
    WorkspaceRecoveryCode,
    WorkspaceRecoveryDisposition,
    workspace_recovery_enabled,
)
from shared.worker_queue import (
    WorkerClaim,
    WorkerCompletionAcceptance,
    WorkerRenewal,
    claim_worker_batch,
    complete_worker_batch,
    get_worker_completion_acceptance,
    renew_worker_batch,
    release_worker_batch,
    rotate_worker_batch,
)

logger = logging.getLogger(__name__)

_COMPLETION_REPORT_PAYLOAD_FIELDS = (
    "should_stop",
    "goal_achieved",
    "error",
    "freeze_data",
)

# --- Tunables (env-overridable where deployment cares) -----------------------

IDLE_POLL_SECONDS = 0.5
IDLE_POLL_BACKOFF_SECONDS = 2.0
IDLE_POLLS_BEFORE_BACKOFF = 30
POLL_JITTER = 0.2  # ±20%
# How long a pod keeps an idle thread's session attached, hoping for the next
# turn (§5.3.4). The win it buys is the whole attach (measured 7.4s on k3d);
# the cost is one process's worth of session state — LLM clients, workspace
# backend, knowledge stores — held for a thread nobody is talking to. Well
# under the reaper's steal horizon, so an expired warm cache never masks a
# lease problem.
WARM_SESSION_IDLE_TTL_SECONDS = 300.0
CLOUD_PUSH_WAIT_SECONDS = 60.0  # §5.3.5 option (i): the lease covers the push
# Commit-then-effects (stateless_turn_resilience.md step 4a): the unit only
# waits for the push to be STAGED (workspace read, temp files written) —
# transmit continues off-slot under its own fence.
CLOUD_PUSH_STAGE_WAIT_SECONDS = 120.0
TURN_ABORT_GRACE_SECONDS = 15.0  # polite-unwind budget after an interrupt
COMPLETE_RETRY_ATTEMPTS = 3
# Step 4a: a settled turn's completion CAS is re-attempted from the shutdown
# dispose path; the process is on its way out, so the attempt is bounded and
# a hang falls through to the (checkpoint-protected) release.
SHUTDOWN_COMPLETE_TIMEOUT_SECONDS = 5.0
PENDING_ROWS_LIMIT = 50
WORKER_FINALIZATION_POLL_SECONDS = 1.0
WORKER_RECOVERY_QUIESCE_SECONDS = 15.0


@dataclass(frozen=True)
class WorkspaceRecoveryHandoff:
    """Exact durable receipt, or a report whose commit remains unknown.

    This signal revokes local admission. It never proves a remote command
    stopped; the durable recovery controller retains that obligation.
    """

    disposition: WorkspaceRecoveryDisposition | None


class _ClaimQuiescenceError(RuntimeError):
    """A claimant could not retire every local consumer before disposition."""


class _PostEffectParkError(_ClaimQuiescenceError):
    """Exact post-effect claim could not reach its fail-closed queue state."""


_AUDIT_WRITER_UNSET = object()

_WORKER_PRESERVE_SHELL_STATUSES = frozenset(
    {"paused", "pending_review", "reviewing", "waiting", "waiting_for_reply"}
)
_WORKER_TERMINAL_JOB_STATUSES = frozenset({"completed", "failed", "cancelled"})
_WORKER_UNFINISHED_COMMAND_STATES = frozenset({"pending", "finalizing"})
_WORKER_FINALIZED_COMMAND_STATES = frozenset({"done", "superseded", "force_resolved"})


def _enabled_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# Oldest-unanswered query (§5.1 watermarks + M0b role vocabulary): LangChain
# roles ('human'/'ai'); rewound rows are dead timelines and must not be
# answered. ``seq > COALESCE(consumed_seq, -1)`` — a NULL consumed watermark
# means nothing was ever answered, so the oldest human row qualifies.
_PENDING_INPUT_SQL = """
    SELECT message.id, message.seq, message.content, message.turn_number,
           message.role, delivery.delivery_id, delivery.supersedes_input_seq
    FROM thread_messages AS message
    LEFT JOIN thread_input_deliveries AS delivery
      ON delivery.message_id = message.id
     AND delivery.thread_id = message.thread_id
    WHERE message.thread_id = $1
      AND message.rewound_at IS NULL
      AND (
          (message.role = 'human' AND message.seq > $2)
          OR
          (
              message.role = 'event'
              AND delivery.execution_lane = 'stateless'
              AND delivery.state IN ('persisted', 'queued', 'deferred')
          )
      )
    ORDER BY
        CASE
            WHEN delivery.source = 'subagent'
             AND delivery.supersedes_input_seq IS NOT NULL
            THEN 0
            ELSE 1
        END,
        seq ASC
    LIMIT $3
"""

_EXACT_CONSUMED_SEQ_AFTER_ATTACH_SQL = """
SELECT GREATEST(COALESCE(consumed_seq, -1), $4::bigint)
  FROM run_queue
 WHERE unit_id = $1
   AND unit_kind = 'session_turn'
   AND state = 'leased'
   AND lease_token = $2
   AND leased_by = $3
   AND input_delivery_capable_lease_token = $2
"""

_PENDING_EVENT_EXISTS_SQL = """
SELECT EXISTS (
    SELECT 1
      FROM thread_input_deliveries AS delivery
      JOIN thread_messages AS message ON message.id = delivery.message_id
     WHERE delivery.thread_id = $1
       AND delivery.execution_lane = 'stateless'
       AND delivery.state IN ('persisted', 'queued', 'deferred')
       AND message.rewound_at IS NULL
)
"""

# Transcript-truth answered check (skip-if-answered's second leg). The
# watermark leg catches a predecessor that persisted AND completed; this one
# catches a predecessor whose turn ran to its final answer but whose
# settlement never advanced ``consumed_seq`` (the loop died in the
# turn-complete hook — e.g. the pre-fix compaction crash in
# knowledge-base/knowledge/issues/stateless_turn_settlement_crashes_after_midturn_compaction.md).
# Without it, an operator unpark re-injects the already-answered human row
# and the thread answers it twice. Returns the seq of the oldest pending
# human row that is followed by a final assistant answer — content, no tool
# calls (the loop ends a turn exactly there) — with no newer human input in
# between; NULL when the oldest pending input is genuinely unanswered.
_ANSWERED_BY_TRANSCRIPT_SQL = """
SELECT input.seq
  FROM thread_messages AS input
 WHERE input.thread_id = $1
   AND input.role = 'human'
   AND input.rewound_at IS NULL
   AND input.seq > $2::bigint
   AND input.seq <= $3::bigint
   AND EXISTS (
       SELECT 1
         FROM thread_messages AS answer
        WHERE answer.thread_id = input.thread_id
          AND answer.role = 'ai'
          AND answer.rewound_at IS NULL
          AND answer.seq > input.seq
          AND COALESCE(answer.content, '') <> ''
          AND (
              answer.tool_calls IS NULL
              OR jsonb_typeof(answer.tool_calls) <> 'array'
              OR jsonb_array_length(answer.tool_calls) = 0
              -- A turn whose last assistant message carried a tool call
              -- (answer text + write_file in one message) has no zero-tool
              -- final row; its own turn.completed frame is the settled
              -- boundary instead (stateless_turn_resilience.md step 3 → 4a).
              OR EXISTS (
                  SELECT 1
                    FROM thread_events AS frame
                   WHERE frame.thread_id = input.thread_id
                     AND frame.kind = 'turn.completed'
                     AND frame.payload ->> 'turn_id' = input.turn_number::text
                     AND frame.created_at >= answer.created_at
              )
          )
          AND NOT EXISTS (
              SELECT 1
                FROM thread_messages AS later_input
               WHERE later_input.thread_id = input.thread_id
                 AND later_input.role = 'human'
                 AND later_input.rewound_at IS NULL
                 AND later_input.seq > input.seq
                 AND later_input.seq < answer.seq
          )
   )
 ORDER BY input.seq ASC
 LIMIT 1
"""

# Shutdown classification (step 4a): a cancelled turn that crossed a tool
# effect is re-queued — not parked — when every tool call the transcript
# already holds has its durable ToolMessage. Only an in-flight tool (a
# persisted call with no result row) is ambiguous enough to park.
_TOOL_EFFECTS_DURABLE_SQL = """
SELECT NOT EXISTS (
    SELECT 1
      FROM thread_messages AS call_row
     CROSS JOIN LATERAL jsonb_array_elements(
         CASE WHEN jsonb_typeof(call_row.tool_calls) = 'array'
              THEN call_row.tool_calls ELSE '[]'::jsonb END
     ) AS tool_call
     WHERE call_row.thread_id = $1
       AND call_row.role = 'ai'
       AND call_row.rewound_at IS NULL
       AND call_row.seq > $2::bigint
       AND NOT EXISTS (
           SELECT 1
             FROM thread_messages AS result_row
            WHERE result_row.thread_id = call_row.thread_id
              AND result_row.role = 'tool'
              AND result_row.rewound_at IS NULL
              AND result_row.seq > call_row.seq
              AND result_row.tool_call_id = tool_call ->> 'id'
       )
)
"""


def _pa():
    """The persistent_app module, imported lazily (import-cycle guard)."""
    import agent.api.persistent_app as pa

    return pa


def _message_text(msg: Any) -> str:
    """Best-effort text of a message's content (list content is flattened
    exactly like ``_serialize_message_row`` does at persist time, so a
    restored row's stored content compares equal)."""
    content = getattr(msg, "content", None)
    if isinstance(content, list):
        return " ".join(
            b.get("text", "") if isinstance(b, dict) else str(b) for b in content
        )
    return content if isinstance(content, str) else str(content or "")


def attach_fingerprint(attach: Dict[str, Any]) -> str:
    """Stable, cheap fingerprint of a claim bundle's attach object (§5.3.4).

    sha256 over canonical JSON — any change in resolved config / datasources /
    project scope forces a re-attach; identical bundles reuse the live
    session. ``default=str`` keeps exotic values (UUIDs, datetimes) stable
    rather than raising.

    Two classes of delivery-only values are excluded before hashing:

    * ``resolved_config.resolved_at`` is a wall-clock stamp minted by every
      resolver call and consumed by nothing.
    * ``runtime_actor`` bearer credentials and expiries rotate on every claim.
      The warm session keeps its still-live actor (and refresh token); its
      stable caller/user/project/thread identity remains in the hash.
    * ``interactive.permission_mode`` / ``narration_mode`` are first-class
      control-inbox scalars. A cold attach must receive them for crash/handoff
      convergence, but a warm owner applies their ordered pending request in
      place. Hashing them forced a full detach/attach before that drain (9–11s
      measured on k3d for a scalar whose journal write took ~10ms).

    Every other config-content change (model, prompts, tools, datasources and
    other interactive settings) still changes the hash and forces the attach
    it should.
    """

    def _without_control_scalars(config: Any) -> Any:
        if not isinstance(config, dict):
            return config
        interactive = config.get("interactive")
        if not isinstance(interactive, dict):
            return config
        filtered = {
            key: value
            for key, value in interactive.items()
            if key not in {"permission_mode", "narration_mode"}
        }
        result = dict(config)
        if filtered:
            result["interactive"] = filtered
        else:
            result.pop("interactive", None)
        return result

    rc = attach.get("resolved_config")
    override = attach.get("config_override")
    if isinstance(rc, dict):
        rc = dict(rc)
        rc.pop("resolved_at", None)
        agent = rc.get("agent")
        if isinstance(agent, dict):
            rc["agent"] = _without_control_scalars(agent)
        attach = {
            **attach,
            "resolved_config": rc,
        }
    if isinstance(override, dict):
        attach = {
            **attach,
            "config_override": _without_control_scalars(override),
        }
    runtime_actor = attach.get("runtime_actor")
    if isinstance(runtime_actor, dict):
        attach = {
            **attach,
            "runtime_actor": {
                key: value
                for key, value in runtime_actor.items()
                if key
                not in {
                    "access_credential",
                    "refresh_credential",
                    "access_expires_at",
                    "refresh_expires_at",
                }
            },
        }
    canonical = json.dumps(attach, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fingerprint_diff_paths(
    old: Any, new: Any, *, max_depth: int = 3, max_paths: int = 20
) -> List[str]:
    """Dotted paths (KEYS ONLY — never values; bundles hold secrets) where two
    attach payloads differ. Diagnostic for affinity misses: answers *which*
    part of the bundle was volatile without ever logging credential material.
    """
    paths: List[str] = []

    def _canon(v: Any) -> str:
        return json.dumps(v, sort_keys=True, separators=(",", ":"), default=str)

    def _walk(a: Any, b: Any, prefix: str, depth: int) -> None:
        if len(paths) >= max_paths:
            return
        if isinstance(a, dict) and isinstance(b, dict) and depth < max_depth:
            for key in sorted(set(a) | set(b)):
                pa_, pb_ = a.get(key), b.get(key)
                if _canon(pa_) != _canon(pb_):
                    _walk(
                        pa_, pb_, f"{prefix}.{key}" if prefix else str(key), depth + 1
                    )
        elif isinstance(a, list) and isinstance(b, list) and depth < max_depth:
            if len(a) != len(b):
                paths.append(f"{prefix}[len {len(a)}->{len(b)}]")
                return
            for i, (ia, ib) in enumerate(zip(a, b)):
                if _canon(ia) != _canon(ib):
                    _walk(ia, ib, f"{prefix}[{i}]", depth + 1)
        else:
            paths.append(prefix or "<root>")

    _walk(old, new, "", 0)
    return paths


def strip_restored_pending_humans(
    messages: List[Any], pending_rows: List[Dict[str, Any]]
) -> int:
    """Drop restored copies of not-yet-consumed human rows from the tail.

    ``_restore_session_messages`` loads ALL live rows — including pending
    unanswered ones (rows whose ``seq`` is past the consumed watermark).
    Those re-enter properly via the loop injection (id-based upsert makes the
    DB write idempotent); without this strip the turn would see the pending
    message twice (once as restored history, once as the injected input).

    Matching strategy: **by message id when the restored message carries the
    DB row id** (future-proof — today's restore deliberately mints fresh
    uuid4 ids and does not even select the id column, so id matches never
    occur), **else by exact content equality matched tail-to-tail** (the
    pending rows are the newest rows, so their restored copies are the last
    messages; both sequences are seq-ordered and compared from the end).

    Stops at the first trailing message that is not a matching HumanMessage:
    a non-trailing human is history and stays; an unanswered ``role='event'``
    row (never enqueued by the orchestrator, so never in ``pending_rows``)
    legitimately remains in context as history — matching today's documented
    behavior for accepted-but-unconsumed notices.

    Mutates ``messages`` in place; returns the number of messages removed.
    """
    if not messages or not pending_rows:
        return 0
    pending_ids = {
        str(row["id"]): row for row in pending_rows if row.get("id") is not None
    }
    # Event deliveries are deliberately excluded by transcript restore until
    # provider admission. Keep only rows that restore actually loaded, or an
    # event after a human row would stop the tail matcher and duplicate the
    # human on a fresh attach.
    remaining = [row for row in pending_rows if row.get("role", "human") == "human"]
    removed = 0
    while messages and remaining:
        msg = messages[-1]
        if getattr(msg, "type", None) != "human":
            break
        msg_id = getattr(msg, "id", None)
        row = pending_ids.get(str(msg_id)) if msg_id is not None else None
        if row is not None and row in remaining:
            remaining.remove(row)
        elif _message_text(msg) == (remaining[-1].get("content") or ""):
            remaining.pop()
        else:
            break
        messages.pop()
        removed += 1
    return removed


_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_HEX_RE = re.compile(r"\b[0-9a-f]{12,}\b")
_DIGITS_RE = re.compile(r"\d+")
_ERROR_SIGNATURE_MESSAGE_CHARS = 80
_LAST_ERROR_CHARS = 2000


def _error_signature(exc: BaseException) -> str:
    """Stable class+message signature of an attach failure.

    The signature is what parks a poison attach (the same failure three times
    in a row), so it must not change between retries just because an id or a
    counter in the message did: UUIDs, long hex tokens and digit runs are
    folded, whitespace collapsed, then the message is cut to 80 chars.
    """
    message = " ".join(str(exc).split())
    message = _UUID_RE.sub("<uuid>", message)
    message = _HEX_RE.sub("<hex>", message)
    message = _DIGITS_RE.sub("#", message)
    return f"{type(exc).__name__}:{message[:_ERROR_SIGNATURE_MESSAGE_CHARS]}"


# Executor-internal park reasons → the recorded park_reason vocabulary
# (stateless_turn_resilience.md step 2). Anything not named here is recorded
# verbatim: it is still a reason, just not an owner-retryable one.
_PARK_REASON_MAP = {
    "uncooperative_shutdown": PARK_REASON_SHUTDOWN_CANCELLED,
    "shutdown_cancelled": PARK_REASON_SHUTDOWN_CANCELLED,
    "completion_cas_failed": PARK_REASON_COMPLETION_CAS_FAILED,
    "completion_cas_failed_pre_effect": PARK_REASON_COMPLETION_CAS_FAILED,
}


def _park_reason_for(executor_reason: str) -> str:
    return _PARK_REASON_MAP.get(executor_reason, executor_reason)


# --- Exhausted error release -------------------------------------------------
# knowledge-base/knowledge/issues/voluntary_session_release_bypasses_retry_budget.md.
# A voluntarily released row is never an expired lease, so the reaper's
# max_attempts check cannot bound a release loop; the release itself must. When
# it parks, the park is only half of the disposition: Cockpit derives the
# visible lifecycle from the journal, so the epoch edge, the settled claimant
# authority and ``turn.parked`` commit with the queue CAS or not at all.
# Global per-thread lock order is threads -> run_queue (REST admission, End and
# the reaper take the same order); the CAS takes the second lock.

# Every ``_release`` reason, classified. Only a DETERMINISTIC pre-effect
# failure — one a retry cannot fix without something changing — counts toward
# the park budget. A TRANSIENT one (DB, transport, the orchestrator restarting
# mid-deploy, a lost race, shutdown or drain) re-queues without limit under a
# capped exponential backoff: such claims always recovered on their own, and
# parking them turns every deploy blackout into manual Retries. When a site
# cannot tell the two apart it is transient. tests/test_run_queue_park_
# lifecycle.py fails on a release site whose reason is in neither table.
_DETERMINISTIC_RELEASE_REASONS = frozenset(
    {
        # The durable input row cannot be run as it stands.
        "empty_event_input",
        "pending_turn_identity_invalid",
        # The persistent loop ended on its own while this executor was
        # healthy — a crash on this input, or the strict-pairing halt that
        # exists to stop repeated provider spend. Shutdown never lands here:
        # stop() marks the lease lost before aborting, and a cancelled claim
        # takes the shutdown release. A DB blip can end the loop too, which is
        # why the budget is max_attempts such failures, not one.
        "loop_died",
    }
)
_TRANSIENT_RELEASE_REASONS = frozenset(
    {
        "bundle_error",  # transport: the orchestrator is unreachable
        "pending_query_failed",  # DB
        "pending_event_query_failed",  # DB
        "event_delivery_claim_failed",  # DB
        "event_delivery_claim_lost",  # lost a race for the delivery
        "control_inbox_failed",  # DB / owner fence
        # DB or lease loss; a corrupt receipt cannot be told apart here.
        "stale_interrupt_recovery_failed",
        # Warm reuse only: the detach makes the next claim's attach fresh.
        "pending_turn_identity_mismatch",
        "loop_not_ready",  # includes runtime admission closed (shutdown/drain)
        # An unclassified exception: a bug and a DB error look the same.
        "serve_crash",
        "completion_cas_failed",  # the answer is durable and checkpointed
    }
)
# Claim-bundle HTTP refusals: a 4xx is the orchestrator refusing THIS claim
# (409 attach assembly refused, 403 lease validation, 401/400/422), so it is
# deterministic; a 5xx, a timeout or a rate limit is the orchestrator unwell.
_TRANSIENT_BUNDLE_STATUSES = frozenset({408, 425, 429})


def _release_is_deterministic(reason: str) -> bool:
    status = reason.removeprefix("bundle_")
    if status != reason and status.isdigit():
        code = int(status)
        return 400 <= code < 500 and code not in _TRANSIENT_BUNDLE_STATUSES
    if reason in _DETERMINISTIC_RELEASE_REASONS:
        return True
    if reason not in _TRANSIENT_RELEASE_REASONS:
        logger.warning("unclassified release reason %r treated as transient", reason)
    return False


_LOCK_RELEASE_THREAD_SQL = """
SELECT execution_lane, agent_id, metadata
  FROM threads
 WHERE id = $1::uuid
   FOR UPDATE
"""

_RELEASED_QUEUE_ROW_SQL = """
SELECT state, lease_token, leased_by, park_reason,
       attempts_since_completion, max_attempts, attach_failures
  FROM run_queue
 WHERE unit_id = $1::uuid
"""

_CLEAR_ACTIVE_CLAIM_SQL = """
UPDATE threads
   SET metadata = metadata - '_stateless_active_claim'
 WHERE id = $1::uuid
RETURNING id
"""


@dataclass(frozen=True)
class _ReleaseDisposition:
    """The committed queue outcome of one exact error release."""

    state: str
    attempts: int
    journaled: bool = False
    # A retry found this claim's own earlier commit (its response was lost).
    replayed: bool = False


@contextlib.asynccontextmanager
async def _db_transaction(db: Any) -> AsyncIterator[Any]:
    """One explicit transaction on a pool wrapper or a bare connection."""

    acquire = getattr(db, "acquire", None)
    if acquire is None:
        async with db.transaction():
            yield db
        return
    async with acquire() as conn:
        async with conn.transaction():
            yield conn


async def _retire_parked_claim_authority(
    conn: Any,
    *,
    unit_id: str,
    metadata: Any,
    lease_token: int,
    pod_name: str,
    pod_uid: str,
) -> None:
    """Clear the credential-bound claimant record a quiesced park retires.

    The caller has drained every local writer, so the claimant's own
    quiescence is the proof; nothing about the workspace is asserted. A record
    for this exact token must name this pod (and UID, when known). An older
    token is a leftover of an earlier voluntary disposition — a steal or End
    would already have removed it. Anything else is not this claimant's to
    clear and is left, logged, for the operator.
    """

    if metadata is None:
        return
    try:
        active = active_claim_authority(metadata)
    except RuntimeError:
        logger.warning(
            "parked release left a malformed active-claim record: unit=%s", unit_id
        )
        return
    if active is None:
        return
    active_token, authority = active
    if active_token > lease_token or (
        active_token == lease_token
        and (
            authority.pod != pod_name
            or (bool(pod_uid) and authority.pod_uid != pod_uid)
        )
    ):
        logger.warning(
            "parked release left a foreign active claim: unit=%s token=%d "
            "active_token=%d active_pod=%s",
            unit_id,
            lease_token,
            active_token,
            authority.pod,
        )
        return
    if await conn.fetchval(_CLEAR_ACTIVE_CLAIM_SQL, unit_id) is None:
        raise RuntimeError("parked release lost its locked thread row")


async def _settle_session_release(
    db: Any,
    claim: ClaimedUnit,
    *,
    cas: Callable[[Any], Awaitable[Optional[str]]],
    pod_name: str,
    pod_uid: str,
    park_reason: str,
    release_reason: str,
    error: Optional[str] = None,
) -> Optional[_ReleaseDisposition]:
    """Run one exact error-release CAS and, when it parks, journal it atomically.

    ``cas(conn)`` is the fenced queue statement (a bounded release or the
    attach-failure record) and returns the resulting state, ``None`` when
    fenced out. The caller must have quiesced the claim. Returns ``None`` when
    the lease belongs to someone else.
    """

    unit_id = str(claim.unit_id)
    token = int(claim.lease_token)
    session_unit = str(claim.unit_kind) == UNIT_KIND_SESSION_TURN
    async with _db_transaction(db) as conn:
        thread = (
            await conn.fetchrow(_LOCK_RELEASE_THREAD_SQL, unit_id)
            if session_unit
            else None
        )
        state = await cas(conn)
        queue = await conn.fetchrow(_RELEASED_QUEUE_ROW_SQL, unit_id)
        attempts = (
            int(queue["attempts_since_completion"])
            if queue is not None
            else int(claim.attempts_since_completion)
        )
        if state is None:
            # Claims, steals and End all advance the token; only this claimant
            # takes token N off 'leased' without doing so. A writer-free row
            # still at N is therefore its own earlier commit whose response
            # was lost: report it again, journal nothing twice.
            if (
                queue is not None
                and int(queue["lease_token"] or 0) == token
                and str(queue["state"] or "") in {STATE_QUEUED, STATE_PARKED}
                and queue["leased_by"] is None
            ):
                return _ReleaseDisposition(
                    state=str(queue["state"]), attempts=attempts, replayed=True
                )
            return None
        if (
            str(state) != STATE_PARKED
            or thread is None
            or str(thread["execution_lane"] or "") != LANE_STATELESS
            or thread["agent_id"] is not None
        ):
            return _ReleaseDisposition(state=str(state), attempts=attempts)

        reason = str(
            (queue["park_reason"] if queue is not None else None) or park_reason
        )
        await _retire_parked_claim_authority(
            conn,
            unit_id=unit_id,
            metadata=thread["metadata"],
            lease_token=token,
            pod_name=pod_name,
            pod_uid=pod_uid,
        )
        # Fences any warm allocator still holding this epoch, exactly like a
        # reaper park; the frames below are the new epoch's first facts.
        await bump_epoch(conn, thread_id=unit_id)
        # A loop that died while a tool awaited approval leaves that prompt
        # pending under this token, and nothing retires it while parked (the
        # reaper's parked-row sweep only reaches older tokens). The quiesced
        # claimant cannot answer it, and N + 1 is the generation the next
        # claim mints — the same boundary its attach-time recovery retires.
        # Interrupts need no such step: pre-injection releases never opened
        # admission, and loop death closes it with a final owner drain.
        await retire_stale_stateless_permissions(
            conn,
            thread_id=unit_id,
            retired_lease_token=token,
            successor_lease_token=token + 1,
            reason="lease_expired",
            epoch_already_bumped=True,
        )
        # No target turn id: bundle, attach and pre-injection failures never
        # reached turn.started, and when a loop died mid-turn the client's
        # active-turn fallback for an uncorrelated frame is exactly that turn.
        # Either way the queue block the client re-anchors to shows the park;
        # this frame names why.
        payload: dict[str, Any] = {
            "reason": reason,
            "release_reason": release_reason,
            "attempts": attempts,
            "retryable": reason in RETRYABLE_PARK_REASONS,
            "parked_by": pod_name,
            "lease_token": token,
        }
        if queue is not None:
            # The budget counts failures, not claims (hand-backs and
            # transient releases bump attempts too), so name both.
            payload["failures"] = int(queue["attach_failures"])
            payload["max_attempts"] = int(queue["max_attempts"])
        if error:
            payload["error"] = error[: _ERROR_SIGNATURE_MESSAGE_CHARS * 4]
        frame = await append_system_frame(
            conn, thread_id=unit_id, kind="turn.parked", payload=payload
        )
        if frame is None:
            raise RuntimeError("thread disappeared while journaling a parked release")
        return _ReleaseDisposition(
            state=STATE_PARKED, attempts=attempts, journaled=True
        )


class StatelessTurnExecutor:
    """The M3 claim loop. One instance per stateless pod (see module docstring)."""

    def __init__(
        self,
        *,
        pod_name: Optional[str] = None,
        pod_uid: Optional[str] = None,
        idle_poll_seconds: float = IDLE_POLL_SECONDS,
        idle_backoff_seconds: float = IDLE_POLL_BACKOFF_SECONDS,
        idle_polls_before_backoff: int = IDLE_POLLS_BEFORE_BACKOFF,
        jitter: float = POLL_JITTER,
        cloud_push_wait_seconds: float = CLOUD_PUSH_WAIT_SECONDS,
        abort_grace_seconds: float = TURN_ABORT_GRACE_SECONDS,
        warm_session_idle_ttl_seconds: float = WARM_SESSION_IDLE_TTL_SECONDS,
        worker_enabled: Optional[bool] = None,
        bg_task_enabled: Optional[bool] = None,
        completion_commands_enabled: Optional[bool] = None,
        audit_writer: Any = _AUDIT_WRITER_UNSET,
    ) -> None:
        self._pod_name = (
            pod_name or os.getenv("POD_NAME") or socket.gethostname() or "agent"
        )
        self._pod_uid = str(pod_uid or os.getenv("POD_UID") or "").strip()
        self._idle_poll_seconds = idle_poll_seconds
        self._idle_backoff_seconds = idle_backoff_seconds
        self._idle_polls_before_backoff = idle_polls_before_backoff
        self._jitter = jitter
        self._cloud_push_wait_seconds = cloud_push_wait_seconds
        self._abort_grace_seconds = abort_grace_seconds
        self._warm_session_idle_ttl = warm_session_idle_ttl_seconds
        self._warm_since: Optional[float] = None
        self._worker_enabled = (
            _enabled_env("STATELESS_EXECUTOR", False)
            if worker_enabled is None
            else bool(worker_enabled)
        )
        self._completion_commands_enabled = (
            _enabled_env("COMPLETION_COMMANDS_ENABLED", False)
            if completion_commands_enabled is None
            else bool(completion_commands_enabled)
        )
        self._bg_task_enabled = (
            cloud_push_recovery_enabled()
            if bg_task_enabled is None
            else bool(bg_task_enabled)
        )
        self._bg_claim: ClaimedUnit | None = None
        self._worker_preempted = asyncio.Event()
        self._worker_preempt_status: Optional[str] = None
        self._worker_terminal_report_generation: tuple[str, int] | None = None
        self._worker_completion_accepted_generation: tuple[str, int] | None = None
        self._worker_workspace_recovery: WorkspaceRecoveryHandoff | None = None
        self._worker_workspace_recovery_code: WorkspaceRecoveryCode | None = None
        self._worker_workspace_backend: str | None = None
        self._worker_quarantined = False
        self._worker_runtime_quiesced = False
        self._worker_retirement_lock = asyncio.Lock()
        self._worker_recovery_handoff_done = False
        self._worker_lease_disposition_uncertain = False
        self._worker_disposition_lookup_failed = False
        self._worker_active_claim: WorkerClaim | None = None
        self._worker_heartbeat_task: asyncio.Task | None = None
        self._claim_audit_unavailable_logged = False
        if audit_writer is _AUDIT_WRITER_UNSET:
            try:
                # Reuse the process-wide archiver's SyncAuditWriter. Creating a
                # second writer would mean a second private event loop/pool;
                # resolving it once here also keeps writer construction out of
                # the per-claim path.
                from agent.core.archiver import get_archiver

                archiver = get_archiver()
                self._claim_audit_writer = (
                    getattr(archiver, "_writer", None) if archiver is not None else None
                )
            except Exception:
                logger.warning(
                    "worker claim timing audit initialization failed; "
                    "claims will continue without timing rows",
                    exc_info=True,
                )
                self._claim_audit_writer = None
                self._claim_audit_unavailable_logged = True
        else:
            self._claim_audit_writer = audit_writer

        # S1 acceptance (zero in-process claim state): everything below is
        # either the soft-affinity hint or plumbing. Correctness never
        # depends on any of it surviving a restart.
        self._prefer_unit_id: Optional[Any] = None  # last thread served
        self._attached_fingerprint: Optional[str] = None
        # Previous attach payload, kept ONLY for affinity-miss path diffing
        # (fingerprint_diff_paths — key paths, never values). Same process
        # that holds the live session's credentials, so no new exposure.
        self._attached_bundle: Optional[Dict[str, Any]] = None
        self._lease = LeaseHandle()
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        # Full-settle close evidence survives cancellation into the claim's
        # finally block. Cleanup must never downgrade an atomic
        # close+checkpoint retry into a gate-only close.
        self._pending_settled_close: tuple[str, int, int, int] | None = None
        # Monotonic executor-owned copy of the exact external-effect seam.
        # PersistentApp session globals are intentionally cleared by a
        # physical detach, which can precede the queue completion CAS.
        self._tool_effect_identity: tuple[str, int, int] | None = None
        # (unit_id, token) of the claim whose turn settled (turn_done observed):
        # a cancellation after this point completes or releases, never parks.
        self._settled_claim: Optional[tuple[str, int]] = None
        # A local shutdown abort invalidates the in-process writer before
        # signaling the loop, but still owes a fenced queue release after
        # quiescence. Track it separately from an actual reaper/End steal.
        self._shutdown_retry_claim: Optional[tuple[str, int]] = None

    # ------------------------------------------------------------------
    # Plumbing
    # ------------------------------------------------------------------

    @property
    def _db(self):
        """The agent's app-DB pool wrapper (PostgresDB) — the same pool
        ``_session.postgres_conn`` uses. PostgresDB exposes fetch/fetchrow/
        fetchval with asyncpg-compatible signatures, which is all the
        run_queue query functions need."""
        pa = _pa()
        agent = pa._agent
        db = getattr(agent, "postgres_conn", None) if agent is not None else None
        if db is None:
            raise RuntimeError(
                "stateless executor requires the agent's Postgres pool "
                "(UniversalAgent.initialize must run first, with "
                "connections.postgres enabled)"
            )
        return db

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self._worker_quarantined:
            raise _ClaimQuiescenceError("quarantined executor cannot restart")
        if self.running:
            raise RuntimeError("stateless executor already running")
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self.run(), name="stateless-turn-executor")

    def request_stop(self) -> None:
        """SIGTERM/preStop: stop claiming (mid-turn work finishes first)."""
        self._stop.set()

    async def stop(self, timeout: Optional[float] = None) -> None:
        """Stop claiming, let a mid-flight turn finish (bounded), then return.

        The turn keeps heartbeating while it finishes, so the lease covers the
        whole grace window. On timeout, escalate: politely interrupt the turn,
        then cancel the loop task as the last resort. A fully quiesced claim
        that crossed a tool-effect boundary is parked for manual
        reconciliation; a pre-effect claim remains retryable.
        """
        if timeout is None:
            timeout = float(os.getenv("STATELESS_SHUTDOWN_TIMEOUT_S", "120"))
        self.request_stop()
        task = self._task
        if task is None:
            await self._await_background_pushes(timeout)
            return
        started = time.monotonic()
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout)
            # The claim loop is out. Handed-off pushes hold no slot; give them
            # the rest of the budget, then exit — progress is durable and a
            # stale heartbeat lets the thread's next claim adopt what is left.
            await self._await_background_pushes(
                max(0.0, timeout - (time.monotonic() - started))
            )
        except asyncio.TimeoutError:
            if self._bg_claim is not None:
                # Cloud work has its own durable fence/progress. Cancellation
                # retires that writer and requeues only the background unit.
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
                self._task = None
                return
            logger.warning(
                "stateless executor did not finish within %.0fs — "
                "interrupting the in-flight turn",
                timeout,
            )
            pa = _pa()
            target_turn_id = self._owned_abort_target(pa)
            tool_execution_started = self._claim_crossed_tool_effect(pa)
            if not tool_execution_started:
                # This turn has crossed no external-effect boundary. Abandon
                # the exact local claim before signaling so a cooperative turn
                # close cannot race through complete_unit and consume an
                # unanswered input. Even a cooperative unwind must release
                # the DB lease before the pod disappears; otherwise the reaper
                # creates claimant-loss debt instead of a normal retry.
                if self._lease.unit_id is not None:
                    self._shutdown_retry_claim = (
                        str(self._lease.unit_id),
                        int(self._lease.lease_token),
                    )
                self._lease.mark_lost()
            self._abort_turn_politely(
                pa,
                target_turn_id=target_turn_id,
                force_graceful=tool_execution_started,
            )
            try:
                await asyncio.wait_for(asyncio.shield(task), self._abort_grace_seconds)
            except asyncio.TimeoutError:
                logger.error(
                    "stateless executor still running after interrupt — "
                    "cancelling; a quiesced post-effect claim will be parked "
                    "instead of retried"
                )
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        except Exception:
            logger.exception("stateless executor task ended with an error")
        self._task = None

    async def _await_background_pushes(self, timeout: float) -> None:
        """Wait (bounded) for pushes this pod handed off before exiting."""
        pa = _pa()
        waiter = getattr(pa, "_await_background_cloud_pushes", None)
        if waiter is None:
            return
        try:
            await waiter(timeout)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("background cloud push drain failed", exc_info=True)

    async def _sleep_interruptible(self, delay: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=max(0.0, delay))

    def _idle_delay(self, idle_polls: int) -> float:
        # A pod holding a warm session never enters backoff: the DB-side
        # affinity grace (AFFINITY_GRACE_SECONDS) is sized for the FAST poll
        # cadence, and a backed-off warm pod would sleep straight through its
        # own head start and hand the thread to a cold pod.
        backed_off = (
            idle_polls >= self._idle_polls_before_backoff
            and self._attached_fingerprint is None
        )
        base = self._idle_backoff_seconds if backed_off else self._idle_poll_seconds
        return base * (1.0 + random.uniform(-self._jitter, self._jitter))

    def _mark_warm(self, claim: ClaimedUnit) -> None:
        """Record this claim as the affinity hint and (re)start the warm clock.

        Called only where a claim finished cleanly, so the cached session (if
        any) matches the thread this pod should keep preferring.
        """
        session = _pa()._session
        if session is None or not session.stateless_warm_reuse_safe:
            # Physical workspace state is retired under the claim before the
            # queue transition. Only lite sessions may remain resident/warm.
            self._prefer_unit_id = None
            self._warm_since = None
            return
        self._prefer_unit_id = claim.unit_id
        self._warm_since = time.monotonic()

    async def _expire_warm_session(self) -> None:
        """Drop a warm session nobody has come back to (see the TTL constant).

        Only ever runs on the idle path — between claims — so it cannot race
        a turn, and the next claim for that thread simply attaches fresh.
        """
        if self._warm_since is None or self._attached_fingerprint is None:
            return
        idle_for = time.monotonic() - self._warm_since
        if idle_for < self._warm_session_idle_ttl:
            return
        logger.info(
            "warm session expired after %.0fs idle (unit=%s) — detaching",
            idle_for,
            self._prefer_unit_id,
        )
        await self._detach_cached_session("warm_idle_ttl")

    def _new_worker_claim_timing(
        self, claim: WorkerClaim
    ) -> tuple[dict[str, Any], datetime, float]:
        """Create the one claim-local payload and its monotonic wall clock."""

        claimed_at = datetime.now(timezone.utc)
        timing: dict[str, Any] = {
            "bundle": 0.0,
            "preflight": 0.0,
            "agent_start": 0.0,
            "stream": 0.0,
            "finish": 0.0,
            "claimed_at": claimed_at.isoformat().replace("+00:00", "Z"),
            "released_at": None,
            "outcome": "error",
            "lease_token": int(claim.lease_token),
            "pod_name": self._pod_name,
            "mcp_attached": False,
        }
        return timing, claimed_at, time.perf_counter()

    @staticmethod
    def _add_worker_finish_timing(
        timing: dict[str, Any] | None, started_at: float
    ) -> None:
        if timing is not None:
            timing["finish"] = float(timing.get("finish") or 0.0) + max(
                0.0, time.perf_counter() - started_at
            )

    def _record_worker_claim_timing(
        self,
        claim: WorkerClaim,
        timing: dict[str, Any],
        *,
        claimed_at: datetime,
        started_at: float,
    ) -> None:
        """Best-effort append of the claim's sole ``claim_timing`` row."""

        released_at = datetime.now(timezone.utc)
        timing["released_at"] = released_at.isoformat().replace("+00:00", "Z")
        elapsed = max(0.0, time.perf_counter() - started_at)
        logger.info(
            "worker claim timing: unit=%s token=%d outcome=%s pod=%s "
            "mcp_attached=%s bundle=%.3fs preflight=%.3fs "
            "agent_start=%.3fs stream=%.3fs finish=%.3fs total=%.3fs",
            claim.unit_id,
            claim.lease_token,
            timing["outcome"],
            self._pod_name,
            timing["mcp_attached"],
            timing["bundle"],
            timing["preflight"],
            timing["agent_start"],
            timing["stream"],
            timing["finish"],
            elapsed,
        )

        writer = self._claim_audit_writer
        if writer is None:
            if not self._claim_audit_unavailable_logged:
                logger.warning(
                    "worker claim timing audit unavailable; claims continue "
                    "without agent_audit timing rows"
                )
                self._claim_audit_unavailable_logged = True
            return
        try:
            writer.insert_audit_pre(
                {
                    "job_id": str(claim.unit_id),
                    "agent_type": "worker",
                    "iteration": claim.unit.attempts_since_completion,
                    "step_type": "claim_timing",
                    "node_name": "worker_claim",
                    # Claim time, not insert time, is the stable ordering key
                    # when a fenced predecessor finishes after its successor.
                    "timestamp": claimed_at,
                    "latency_ms": round(elapsed * 1000),
                    "payload": dict(timing),
                    "metadata": None,
                }
            )
        except Exception:
            # SyncAuditWriter already converts its own readiness/write failures
            # to a warning + None. This belt protects injected/alternate sinks
            # without ever changing the queue disposition.
            logger.warning(
                "worker claim timing audit write failed; claim disposition "
                "is unchanged (unit=%s token=%d)",
                claim.unit_id,
                claim.lease_token,
                exc_info=True,
            )

    @staticmethod
    def _worker_mcp_attached(request: JobStartRequest) -> bool:
        """Whether resolved claim inputs will construct an MCP manager."""

        return any(
            isinstance(datasource, dict) and datasource.get("type") == "mcp"
            for datasource in (request.datasources or ())
        )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        # Install the lease handle in THIS context before any task is spawned:
        # the persistent loop, the event writer, and every per-turn helper
        # inherit the same mutable handle (see lease_context.py for why a
        # plain immutable ContextVar value cannot work across claims).
        current_lease.set(self._lease)
        logger.info(
            "stateless turn executor started: pod=%s idle_poll=%.2fs "
            "backoff=%.2fs/%d heartbeat=%ss worker_enabled=%s",
            self._pod_name,
            self._idle_poll_seconds,
            self._idle_backoff_seconds,
            self._idle_polls_before_backoff,
            HEARTBEAT_INTERVAL_SECONDS,
            self._worker_enabled,
        )
        idle_polls = 0
        while not self._stop.is_set():
            try:
                claim = await claim_unit(
                    self._db,
                    unit_kind=UNIT_KIND_SESSION_TURN,
                    pod_name=self._pod_name,
                    prefer_unit_id=self._prefer_unit_id,
                )
            except Exception as e:
                logger.warning("run_queue claim poll failed (transient): %s", e)
                await self._sleep_interruptible(self._idle_backoff_seconds)
                continue
            worker_claim: Optional[WorkerClaim] = None
            if claim is None and self._worker_enabled:
                try:
                    worker_claim = await claim_worker_batch(
                        self._db,
                        pod_name=self._pod_name,
                        completion_commands_enabled=(self._completion_commands_enabled),
                    )
                except Exception as e:
                    logger.warning("worker_batch claim poll failed (transient): %s", e)
                    await self._sleep_interruptible(self._idle_backoff_seconds)
                    continue

            bg_claim = None
            if claim is None and worker_claim is None and self._bg_task_enabled:
                try:
                    bg_claim = await claim_bg_task(self._db, pod_name=self._pod_name)
                except Exception:
                    logger.warning("background claim poll failed", exc_info=True)
                    await self._sleep_interruptible(self._idle_backoff_seconds)
                    continue
            if bg_claim is not None:
                if self._stop.is_set():
                    await release_unit(
                        self._db,
                        unit_id=bg_claim.unit_id,
                        lease_token=bg_claim.lease_token,
                    )
                    break
                idle_polls = 0
                try:
                    await self._serve_bg_task_claim(bg_claim)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "background queue transition failed; lease left for reaper"
                    )
                continue

            if claim is None and worker_claim is None:
                idle_polls += 1
                await self._expire_warm_session()
                await self._sleep_interruptible(self._idle_delay(idle_polls))
                continue
            idle_polls = 0
            if self._stop.is_set():
                # Claimed on the stop boundary — hand it straight back for
                # another pod (no backoff, not an error).
                stop_claim = worker_claim.unit if worker_claim is not None else claim
                stop_timing: dict[str, Any] | None = None
                stop_claimed_at: datetime | None = None
                stop_started_at: float | None = None
                if worker_claim is not None:
                    (
                        stop_timing,
                        stop_claimed_at,
                        stop_started_at,
                    ) = self._new_worker_claim_timing(worker_claim)
                try:
                    if worker_claim is None:
                        # The pod may still hold an idle warm session from its
                        # previous claim. Retire it before publishing this
                        # newly leased session unit back to the queue.
                        await self._quiesce_claim_before_transition(
                            _pa(),
                            reason="release_shutting_down",
                        )
                    finish_started_at = time.perf_counter()
                    try:
                        state = await release_unit(
                            self._db,
                            unit_id=stop_claim.unit_id,
                            lease_token=stop_claim.lease_token,
                            backoff_seconds=0.0,
                        )
                    finally:
                        self._add_worker_finish_timing(
                            stop_timing,
                            finish_started_at,
                        )
                    logger.info(
                        "run_queue release: unit=%s token=%d reason=shutting_down "
                        "state=%s",
                        stop_claim.unit_id,
                        stop_claim.lease_token,
                        state,
                    )
                except _ClaimQuiescenceError:
                    logger.critical(
                        "shutdown-boundary claim could not be quiesced; "
                        "leaving exact lease unreleased (unit=%s token=%d)",
                        stop_claim.unit_id,
                        stop_claim.lease_token,
                        exc_info=True,
                    )
                except Exception:
                    pass
                finally:
                    if (
                        worker_claim is not None
                        and stop_timing is not None
                        and stop_claimed_at is not None
                        and stop_started_at is not None
                    ):
                        stop_timing["outcome"] = "released:shutting_down"
                        self._record_worker_claim_timing(
                            worker_claim,
                            stop_timing,
                            claimed_at=stop_claimed_at,
                            started_at=stop_started_at,
                        )
                break
            if worker_claim is not None:
                try:
                    await self._serve_worker_claim(worker_claim)
                except asyncio.CancelledError:
                    raise
                except (SubagentLifecycleError, _ClaimQuiescenceError):
                    # Publishing this claim could overlap the old ToolContext
                    # and child generation.  Leave the exact lease unresolved
                    # for expiry/operator recovery and stop taking new work.
                    logger.critical(
                        "worker child lifecycle could not be settled; stopping "
                        "executor without release (unit=%s token=%d)",
                        worker_claim.unit_id,
                        worker_claim.lease_token,
                        exc_info=True,
                    )
                    self.request_stop()
                except Exception:
                    logger.exception(
                        "unhandled error serving worker unit %s — releasing and "
                        "continuing",
                        worker_claim.unit_id,
                    )
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await self._cleanup_worker_runtime(preserve_shell=True)
                    await self._release_worker_claim(
                        worker_claim,
                        reason="serve_crash",
                    )
                continue
            try:
                assert claim is not None
                await self._serve_claim(claim)
            except asyncio.CancelledError:
                raise
            except _PostEffectParkError:
                # Never route this through the generic release belt: the
                # claim may already have produced an unledgered external tool
                # effect. Stop claiming and leave the exact lease visibly
                # unresolved rather than making the input runnable.
                logger.critical(
                    "post-effect claim could not be parked; stopping executor "
                    "without release (unit=%s token=%d)",
                    claim.unit_id,
                    claim.lease_token,
                    exc_info=True,
                )
                self.request_stop()
            except _ClaimQuiescenceError:
                # A retryable claim is not safe to publish while its old warm
                # loop/session may still be alive. Stop and leave the exact
                # lease for operator/reaper reconciliation.
                logger.critical(
                    "claim could not be fully quiesced; stopping executor "
                    "without release (unit=%s token=%d)",
                    claim.unit_id,
                    claim.lease_token,
                    exc_info=True,
                )
                self.request_stop()
            except Exception:
                # This outermost belt must classify the exact claim before it
                # hands anything back. An unexpected exception can happen
                # after a tool has crossed its effect boundary; releasing that
                # input would replay the tool on a successor.
                if self._claim_crossed_tool_effect(_pa(), claim=claim):
                    logger.exception(
                        "unhandled post-effect error serving unit %s — "
                        "quiescing and parking",
                        claim.unit_id,
                    )
                    try:
                        state = await self._quiesce_and_park_post_effect_claim(
                            _pa(),
                            claim,
                            reason="serve_crash_after_tool_effect",
                        )
                    except _PostEffectParkError:
                        logger.critical(
                            "post-effect serve crash could not be parked; "
                            "stopping executor without release "
                            "(unit=%s token=%d)",
                            claim.unit_id,
                            claim.lease_token,
                            exc_info=True,
                        )
                        self.request_stop()
                    else:
                        if state is None:
                            self.request_stop()
                    continue
                # No external-effect boundary was crossed: the original
                # retryable serve-crash disposition remains correct.
                logger.exception(
                    "unhandled pre-effect error serving unit %s — "
                    "releasing and continuing",
                    claim.unit_id,
                )
                await self._release(claim, reason="serve_crash")
        logger.info("stateless turn executor stopped: pod=%s", self._pod_name)

    # ------------------------------------------------------------------
    # One claim
    # ------------------------------------------------------------------

    async def _serve_bg_task_claim(self, claim: ClaimedUnit) -> None:
        """Serve cloud-only work, with an independent recorded queue lease."""
        from agent.api.cloud_push_task import run_adopted_cloud_push

        self._bg_claim = claim

        async def execute():
            await self._detach_cached_session("background_cloud_push")
            bundle = await self._fetch_bundle(str(claim.unit_id), claim.lease_token)
            if bundle.get("deferred"):
                raise PushAdoptionDeferred("background push temporarily deferred")
            await run_adopted_cloud_push(
                self._db, claim, bundle, pod_name=self._pod_name, pod_uid=self._pod_uid
            )

        work = asyncio.create_task(execute(), name=f"cloud-push-{claim.unit_id}")

        async def heartbeat():
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                try:
                    alive = await heartbeat_unit(
                        self._db, unit_id=claim.unit_id, lease_token=claim.lease_token
                    )
                except Exception:
                    # Uncertain authority stops this writer; never keep writing
                    # on an optimistic heartbeat after a database outage.
                    alive = None
                if alive is None:
                    work.cancel()
                    return

        renewal = asyncio.create_task(heartbeat())
        try:
            await work
            state = await complete_bg_task(self._db, claim)
            logger.info(
                "run_queue complete: background unit=%s state=%s", claim.unit_id, state
            )
        except PushAdoptionDeferred:
            await fail_bg_task(
                self._db,
                claim,
                error="background push temporarily deferred",
                deferred=True,
            )
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await fail_bg_task(self._db, claim, error="background push interrupted")
            if asyncio.current_task().cancelling():
                raise
        except Exception as exc:
            # No credential-bearing exception text is persisted to the task.
            await fail_bg_task(self._db, claim, error=type(exc).__name__)
            logger.warning(
                "background cloud push failed: unit=%s error=%s",
                claim.unit_id,
                type(exc).__name__,
            )
        finally:
            renewal.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renewal
            self._bg_claim = None

    async def _serve_worker_claim(self, claim: WorkerClaim) -> None:
        """Drive one worker batch under its immutable queue lease.

        This is deliberately a separate lifecycle from the persistent-session
        driver below.  Worker rotation is queue-only; the completion API is
        reserved for genuine terminal/human-facing graph stops.
        """

        if self._worker_quarantined:
            raise _ClaimQuiescenceError("quarantined executor cannot accept a claim")
        unit = claim.unit
        unit_id = str(unit.unit_id)
        token = unit.lease_token
        timing, claimed_at, claim_started_at = self._new_worker_claim_timing(claim)
        logger.info(
            "run_queue claim: unit=%s kind=%s token=%d attempts=%d "
            "input_seq=%s consumed_seq=%s prior_job_status=%s pod=%s",
            unit_id,
            unit.unit_kind,
            token,
            unit.attempts_since_completion,
            unit.input_seq,
            unit.consumed_seq,
            claim.prior_job_status,
            self._pod_name,
        )
        self._worker_preempted = asyncio.Event()
        self._worker_preempt_status = None
        self._worker_terminal_report_generation = None
        self._worker_completion_accepted_generation = None
        self._worker_workspace_recovery = None
        self._worker_workspace_recovery_code = None
        self._worker_workspace_backend = None
        self._worker_runtime_quiesced = False
        self._worker_recovery_handoff_done = False
        self._worker_lease_disposition_uncertain = False
        self._worker_disposition_lookup_failed = False
        self._worker_active_claim = claim
        heartbeat_task: asyncio.Task | None = None
        try:
            heartbeat_task = asyncio.create_task(
                self._worker_heartbeat_loop(claim),
                name=f"worker-lease-heartbeat-{unit_id[:8]}",
            )
            self._worker_heartbeat_task = heartbeat_task
            pa = _pa()
            # A shared pod can still hold a warm interactive session when the
            # next durable claim is a worker.  Perform the same physical claim
            # switch even for the no-work ``attempts > max`` give-up path:
            # terminal cleanup must never clear the singleton agent underneath
            # an attached cached session or inherit its tenant residue.
            if pa._session is not None:
                await self._detach_cached_session("worker_claim_switch")
            self._scrub_process_residue()
            self._activate_lease(unit_id, token)

            timing["outcome"] = await self._serve_worker_claim_inner(
                claim,
                timing=timing,
                retry_exhausted=(unit.attempts_since_completion > claim.max_attempts),
            )
        except asyncio.CancelledError:
            # Hard executor shutdown: close local admission first.  A best-effort
            # release is intentionally left to the outer shutdown/reaper path if
            # cancellation prevents the DB call from completing.
            with contextlib.suppress(asyncio.CancelledError, Exception):
                if not self._worker_quarantined:
                    if await self._resolve_workspace_recovery(claim):
                        await self._handoff_workspace_recovery(claim=claim)
                    else:
                        await self._cleanup_worker_runtime(preserve_shell=True)
            raise
        except _ClaimQuiescenceError:
            self._worker_quarantined = True
            self.request_stop()
            timing["outcome"] = f"quarantined:{self._worker_handoff_reason()}"
            return
        except SubagentLifecycleError as exc:
            if await self._resolve_workspace_recovery(claim, error=exc):
                timing["outcome"] = await self._handoff_workspace_recovery(claim=claim)
                return
            # Retry the lifecycle gate exactly once through the normal cleanup
            # belt.  Only a fully successful cleanup permits retry publication;
            # a second failure escapes to run(), which stops without release.
            logger.error(
                "worker child lifecycle failed before disposition: "
                "unit=%s token=%d type=%s",
                unit_id,
                token,
                type(exc).__name__,
            )
            try:
                await self._cleanup_worker_runtime(preserve_shell=True)
            except asyncio.CancelledError:
                raise
            except Exception as retry_exc:
                raise SubagentLifecycleError(
                    "worker child lifecycle retry did not fully clean the claim"
                ) from retry_exc
            if self._lease.lost.is_set():
                timing["outcome"] = "closed:child_lifecycle_after_lease_loss"
                return
            await self._release_worker_claim(
                claim,
                reason="child_lifecycle_failed",
                park_on_exhaustion=False,
                timing=timing,
            )
            timing["outcome"] = "released:child_lifecycle_failed"
            return
        except Exception as exc:
            if self._worker_quarantined:
                self.request_stop()
                timing["outcome"] = f"quarantined:{self._worker_handoff_reason()}"
                return
            if await self._resolve_workspace_recovery(claim, error=exc):
                timing["outcome"] = await self._handoff_workspace_recovery(claim=claim)
                return
            if self._worker_lease_disposition_uncertain:
                timing["outcome"] = await self._handoff_worker_disposition(claim=claim)
                return
            logger.exception(
                "worker_batch failed before a disposition: unit=%s token=%d",
                unit_id,
                token,
            )
            report_started = self._worker_terminal_report_generation == (
                unit_id,
                int(token),
            )
            if report_started:
                # Once an HTTP report has begun, correction 8 owns every
                # ambiguous tail failure. Never issue a second report from
                # this generation and never park it: preserve runtime state,
                # release with backoff, and let a successor consume or
                # benignly re-report the durable END checkpoint.
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._cleanup_worker_runtime(preserve_shell=True)
                if self._worker_completion_accepted_generation == (
                    unit_id,
                    int(token),
                ):
                    logger.warning(
                        "worker accepted completion hold failed; durable command "
                        "backstop retains ownership: unit=%s token=%d",
                        unit_id,
                        token,
                    )
                    return
                await self._release_worker_claim(
                    claim,
                    reason="terminal_report_failed",
                    park_on_exhaustion=False,
                    timing=timing,
                )
                timing["outcome"] = "released:terminal_report_failed"
                return
            if str(self._lease.unit_id or "") != unit_id or int(
                self._lease.lease_token
            ) != int(token):
                # Failure may precede the normal publication point (for
                # example, while detaching a warm session). The queue claim is
                # nevertheless authoritative; publish this generation before
                # its retry/give-up disposition rather than inheriting the old
                # handle's lost bit.
                self._activate_lease(unit_id, token)
            if (
                unit.attempts_since_completion > claim.max_attempts
                and not self._lease.lost.is_set()
            ):
                # Above the cap, bundle/setup exists only to inspect the
                # canonical checkpoint. If that inspection is temporarily
                # unavailable, do not overwrite a potentially successful or
                # human-facing END with an invented failure. Retry without
                # parking until the checkpoint can decide the outcome.
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._cleanup_worker_runtime(preserve_shell=True)
                await self._release_worker_claim(
                    claim,
                    reason="exhausted_checkpoint_probe_failed",
                    park_on_exhaustion=False,
                    timing=timing,
                )
                timing["outcome"] = "released:exhausted_checkpoint_probe_failed"
                return
            if (
                unit.attempts_since_completion == claim.max_attempts
                and not self._lease.lost.is_set()
            ):
                final_state = self._worker_retry_exhausted_state(
                    self._worker_driver_error_state(str(exc), job_id=unit_id),
                    attempts=unit.attempts_since_completion,
                    max_attempts=claim.max_attempts,
                )
                logger.error(
                    "worker_batch driver retry exhausted: unit=%s token=%d "
                    "attempts=%d/%d — reporting terminal give-up",
                    unit_id,
                    token,
                    unit.attempts_since_completion,
                    claim.max_attempts,
                )
                try:
                    timing["outcome"] = await self._report_worker_terminal(
                        claim,
                        final_state,
                        timing=timing,
                    )
                    return
                except Exception:
                    # The report path itself is retriable forever (correction
                    # 8): never turn an unavailable completion handler into an
                    # invisible parked processing job.
                    logger.exception(
                        "worker_batch exhausted give-up report failed before a "
                        "disposition: unit=%s token=%d",
                        unit_id,
                        token,
                    )
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await self._cleanup_worker_runtime(preserve_shell=True)
                    await self._release_worker_claim(
                        claim,
                        reason="terminal_report_failed",
                        park_on_exhaustion=False,
                        timing=timing,
                    )
                    timing["outcome"] = "released:terminal_report_failed"
                    return
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._cleanup_worker_runtime(preserve_shell=True)
            await self._release_worker_claim(
                claim,
                reason="driver_error",
                timing=timing,
            )
            timing["outcome"] = "released:driver_error"
        finally:
            if heartbeat_task is not None:
                if self._worker_quarantined:
                    # A bounded join may already have failed. Do not wait
                    # again for a publisher that cannot relinquish this slot.
                    heartbeat_task.cancel()
                    heartbeat_task.add_done_callback(
                        lambda done: done.exception() if not done.cancelled() else None
                    )
                else:
                    try:
                        await self._stop_worker_heartbeat()
                    except (Exception, asyncio.CancelledError):
                        self._worker_quarantined = True
                        self.request_stop()
                        timing["outcome"] = (
                            f"quarantined:{self._worker_handoff_reason()}"
                        )
            # A heartbeat can observe the committed hold while ordinary
            # cleanup or a fenced queue CAS is awaiting. It publishes evidence;
            # this driver alone serializes retirement and finishes the handoff.
            if (
                (
                    self._worker_workspace_recovery is not None
                    or self._worker_lease_disposition_uncertain
                )
                and not self._worker_recovery_handoff_done
                and not self._worker_quarantined
            ):
                timing["outcome"] = await self._handoff_worker_disposition(claim=claim)
            self._worker_active_claim = None
            self._record_worker_claim_timing(
                claim,
                timing,
                claimed_at=claimed_at,
                started_at=claim_started_at,
            )

    async def _serve_worker_claim_inner(
        self,
        claim: WorkerClaim,
        *,
        timing: dict[str, Any],
        retry_exhausted: bool = False,
    ) -> str:
        pa = _pa()
        unit = claim.unit
        job_id = str(unit.unit_id)
        token = unit.lease_token

        started_at = time.perf_counter()
        try:
            bundle = await self._fetch_bundle(job_id, token)
        finally:
            timing["bundle"] = max(0.0, time.perf_counter() - started_at)
        request, batch = self._parse_worker_bundle(bundle, claim)
        self._worker_workspace_backend = (request.workspace_runtime or {}).get(
            "assigned_backend"
        )
        timing["mcp_attached"] = self._worker_mcp_attached(request)
        metadata = self._worker_job_metadata(request)
        context = request.context or {}
        self._seed_worker_inboxes(
            job_id,
            context.get("pending_guidance"),
            context.get("queued_replies"),
        )

        # Close the claim→bundle race and get the authoritative control state
        # before creating a workspace or invoking the graph.
        started_at = time.perf_counter()
        try:
            renewal = await renew_worker_batch(
                self._db,
                unit_id=unit.unit_id,
                lease_token=token,
            )
        finally:
            timing["preflight"] = max(0.0, time.perf_counter() - started_at)
        if renewal is None:
            if await self._resolve_workspace_recovery(claim, lease_rejected=True):
                return await self._handoff_workspace_recovery(claim=claim)
            self._lease.mark_lost()
            logger.warning(
                "lease lost: worker unit=%s token=%d before graph start",
                job_id,
                token,
            )
            return "error"
        self._observe_worker_renewal(job_id, renewal)
        if self._worker_preempted.is_set():
            await self._finish_external_worker_preempt(claim, timing=timing)
            return "preempted"

        agent = pa._agent
        client = pa._orchestrator_client
        if agent is None or client is None:
            raise RuntimeError("worker claim requires an initialized agent and client")
        agent._orchestrator_client = client

        from shared.job_steering import CheckpointSteeringAcker

        steering_acker = CheckpointSteeringAcker(job_id, client)

        streaming_gen: Optional[AsyncIterator[Dict[str, Any]]] = None
        agent_start_started_at = time.perf_counter()
        timing["agent_start"] = None
        try:
            streaming_gen = await agent.process_job(
                job_id,
                metadata,
                stream=True,
                resume=claim.resume,
                feedback=context.get("queued_feedback"),
                feedback_reason=context.get("queued_feedback_reason"),
                original_config_name=request.config_name,
                previous_status=claim.prior_job_status,
                worker_lease_token=token,
                worker_batch_target_wall_seconds=batch["target_wall_seconds"],
                worker_batch_min_wall_seconds=batch.get("min_wall_seconds"),
                worker_batch_iteration_cap=batch.get("iteration_cap"),
                worker_resume_id=claim.resume_id,
                worker_retry_exhausted=retry_exhausted,
                defer_cleanup=True,
                worker_checkpoint_post_commit=steering_acker,
            )
            outcome, final_state = await self._consume_worker_stream(
                streaming_gen,
                timing=timing,
                agent_start_started_at=agent_start_started_at,
            )
        finally:
            # ``agent_start`` ends at the first superstep yielded by the graph,
            # not at process_job() returning its async iterator. It therefore
            # intentionally includes workspace SSH, datasource/MCP setup,
            # checkpoint/Todo hydration, and any work inside that first step.
            if timing["agent_start"] is None:
                timing["agent_start"] = max(
                    0.0, time.perf_counter() - agent_start_started_at
                )
            if streaming_gen is not None:
                await self._join_worker_local_task(
                    asyncio.create_task(streaming_gen.aclose())
                )

        if await self._resolve_workspace_recovery(claim, final_state=final_state):
            return await self._handoff_workspace_recovery(claim=claim)

        if outcome == "lease_lost":
            logger.warning(
                "lease lost: worker unit=%s token=%d — no report/release/complete",
                job_id,
                token,
            )
            await self._cleanup_worker_runtime(preserve_shell=True)
            return "error"
        if outcome == "preempted":
            await self._finish_external_worker_preempt(claim, timing=timing)
            return "preempted"
        # A renewal can commit between the stream's StopAsyncIteration and the
        # disposition branch.  Recheck both signals so an external control is
        # still guaranteed to make zero HTTP completion reports.
        if self._lease.lost.is_set():
            await self._cleanup_worker_runtime(preserve_shell=True)
            return "error"
        if self._worker_preempted.is_set():
            await self._finish_external_worker_preempt(claim, timing=timing)
            return "preempted"
        if outcome != "graph_end" or final_state is None:
            if unit.attempts_since_completion >= claim.max_attempts:
                exhausted = self._worker_retry_exhausted_state(
                    self._worker_driver_error_state(
                        "worker graph stream ended without a durable terminal state",
                        job_id=job_id,
                    ),
                    attempts=unit.attempts_since_completion,
                    max_attempts=claim.max_attempts,
                )
                logger.error(
                    "worker_batch empty-stream retry exhausted: unit=%s token=%d "
                    "attempts=%d/%d — reporting terminal give-up",
                    job_id,
                    token,
                    unit.attempts_since_completion,
                    claim.max_attempts,
                )
                return await self._report_worker_terminal(
                    claim,
                    exhausted,
                    client=client,
                    timing=timing,
                )
            await self._cleanup_worker_runtime(preserve_shell=True)
            await self._release_worker_claim(
                claim,
                reason="graph_stream_ended_empty",
                timing=timing,
            )
            return "released:graph_stream_ended_empty"

        freeze = final_state.get("freeze_data") or {}
        freeze_type = freeze.get("freeze_type") if isinstance(freeze, dict) else None
        if freeze_type == FREEZE_TYPE_BATCH_BOUNDARY:
            await self._cleanup_worker_runtime(preserve_shell=True)
            if await self._resolve_workspace_recovery(claim):
                return await self._handoff_workspace_recovery(claim=claim)
            if self._worker_lease_disposition_uncertain:
                return await self._handoff_worker_disposition(claim=claim)
            started_at = time.perf_counter()
            try:
                rotation = await rotate_worker_batch(
                    self._db,
                    unit_id=unit.unit_id,
                    lease_token=token,
                    input_seq=unit.input_seq,
                    fair_key=unit.fair_key,
                )
            finally:
                self._add_worker_finish_timing(timing, started_at)
            if rotation is None:
                if await self._resolve_workspace_recovery(claim, lease_rejected=True):
                    return await self._handoff_workspace_recovery(claim=claim)
                self._lease.mark_lost()
                logger.warning(
                    "lease lost: worker rotation fenced out unit=%s token=%d",
                    job_id,
                    token,
                )
                return "error"
            logger.info(
                "worker_batch rotate: unit=%s token=%d "
                "queue_verb=complete_and_requeue queue_state=%s "
                "input_seq=%s next_input_seq=%d complete_calls=0 "
                "http_complete_calls=0",
                job_id,
                token,
                rotation.state,
                rotation.prior_input_seq,
                rotation.next_input_seq,
            )
            return "rotated"

        if self._worker_stop_is_recoverable(final_state, freeze_type):
            if unit.attempts_since_completion >= claim.max_attempts:
                final_state = self._worker_retry_exhausted_state(
                    final_state,
                    attempts=unit.attempts_since_completion,
                    max_attempts=claim.max_attempts,
                )
                freeze_type = "worker_retry_exhausted"
                logger.error(
                    "worker_batch retry exhausted: unit=%s token=%d "
                    "attempts=%d/%d — reporting terminal give-up",
                    job_id,
                    token,
                    unit.attempts_since_completion,
                    claim.max_attempts,
                )
            else:
                await self._cleanup_worker_runtime(preserve_shell=True)
                await self._release_worker_claim(
                    claim,
                    reason="recoverable_stop",
                    timing=timing,
                )
                logger.info(
                    "worker_batch recoverable release: unit=%s token=%d "
                    "freeze=%s attempts=%d/%d complete_calls=0 "
                    "http_complete_calls=0",
                    job_id,
                    token,
                    freeze_type,
                    unit.attempts_since_completion,
                    claim.max_attempts,
                )
                return "released:recoverable_stop"

        return await self._report_worker_terminal(
            claim,
            final_state,
            client=client,
            timing=timing,
        )

    async def _report_worker_terminal(
        self,
        claim: WorkerClaim,
        final_state: Dict[str, Any],
        *,
        client: Any | None = None,
        timing: dict[str, Any] | None = None,
    ) -> str:
        """Report one genuine/give-up stop, then fence the queue disposition."""

        if await self._resolve_workspace_recovery(claim, final_state=final_state):
            return await self._handoff_workspace_recovery(claim=claim)
        if self._worker_lease_disposition_uncertain:
            return await self._handoff_worker_disposition(claim=claim)

        unit = claim.unit
        job_id = str(unit.unit_id)
        token = unit.lease_token
        wire_payload, payload_source = self._worker_completion_wire_payload(final_state)
        if wire_payload.get("should_stop") is not True:
            # Fail closed before marking this generation as report-started.
            # A continue-shaped stateless payload is a driver bug, not a
            # completion attempt: preserve the remote shell and return the
            # claim through ordinary queue backoff/default exhaustion parking.
            logger.error(
                "worker terminal report blocked locally: unit=%s token=%d "
                "payload_source=%s effective_should_stop_not_true — preserving "
                "shell and releasing without /complete",
                job_id,
                token,
                payload_source,
            )
            await self._cleanup_worker_runtime(preserve_shell=True)
            await self._release_worker_claim(
                claim,
                reason="nonterminal_completion_report_blocked",
                timing=timing,
            )
            return "released:nonterminal_completion_report_blocked"
        if client is None:
            client = _pa()._orchestrator_client
        if client is None:
            raise RuntimeError("worker terminal report requires orchestrator client")

        # Genuine terminal/human-facing stop: report exactly once while the
        # queue lease and renewal task remain alive. Only success or exact B4
        # acceptance proof permits finalization hold/queue closure. Ambiguous
        # failures preserve tmux and re-report; the exact coded pre-write 422
        # below instead follows ordinary bounded retry/parking semantics.
        self._worker_terminal_report_generation = (job_id, int(token))
        started_at = time.perf_counter()
        try:
            reported = await client.report_completion(
                job_id,
                final_state,
                lease_token=token,
            )
        except CompletionNonTerminalReportError as exc:
            # This exact coded 422 is a definitive pre-write refusal. Clear
            # the report-started marker before cleanup so even a cleanup fault
            # follows ordinary retry/parking semantics rather than the
            # ambiguous-HTTP no-park path.
            self._worker_terminal_report_generation = None
            logger.error(
                "worker completion definitively refused: unit=%s token=%d code=%s",
                job_id,
                token,
                exc.code,
            )
            try:
                await self._cleanup_worker_runtime(preserve_shell=True)
            except Exception:
                # Cleanup is best-effort but the definitive pre-write result
                # must remain definitive. Letting this escape could enter the
                # driver-exhaustion handler and issue a second /complete at
                # the retry cap. Keep the diagnostic bounded and continue to
                # the exact token-fenced ordinary release.
                logger.error(
                    "worker completion refusal cleanup failed: "
                    "unit=%s token=%d code=%s",
                    job_id,
                    token,
                    exc.code,
                )
            await self._release_worker_claim(
                claim,
                reason=exc.code,
                timing=timing,
            )
            return f"released:{exc.code}"
        finally:
            self._add_worker_finish_timing(timing, started_at)
        if await self._resolve_workspace_recovery(claim):
            return await self._handoff_workspace_recovery(claim=claim)
        if not reported:
            # A pause/cancel may win after the handler's thin entry fence.  The
            # handler's jobs-row disposition CAS then rejects the report. Read
            # the authoritative status once immediately (rather than waiting
            # for the next heartbeat) and honor the external control with zero
            # queue error-release. Never cancel an in-flight report: Starlette
            # cancellation can strand the existing multi-write handler.
            failed_report_status: str | None = None
            accepted_completion = None
            if not self._lease.lost.is_set():
                renewal = await renew_worker_batch(
                    self._db,
                    unit_id=unit.unit_id,
                    lease_token=token,
                )
                if renewal is None:
                    if await self._resolve_workspace_recovery(
                        claim, lease_rejected=True
                    ):
                        return await self._handoff_workspace_recovery(claim=claim)
                    accepted_completion = await self._accepted_worker_completion(claim)
                    if accepted_completion is None:
                        self._lease.mark_lost()
                    else:
                        failed_report_status = accepted_completion.job_status
                else:
                    failed_report_status = renewal.job_status
                    self._observe_worker_renewal(job_id, renewal)
            if accepted_completion is not None:
                logger.info(
                    "worker completion already accepted: unit=%s token=%d "
                    "command=%s state=%s after ambiguous HTTP result",
                    job_id,
                    token,
                    accepted_completion.command_id,
                    accepted_completion.command_state,
                )
                resolved_status = await self._finish_accepted_worker_completion(
                    claim,
                    accepted_completion,
                    http_result_ambiguous=True,
                )
                return self._worker_terminal_timing_outcome(
                    final_state,
                    observed_status=resolved_status or accepted_completion.job_status,
                )
            if self._lease.lost.is_set():
                await self._cleanup_worker_runtime(preserve_shell=True)
                return "error"
            if self._worker_preempted.is_set():
                await self._finish_external_worker_preempt(
                    claim,
                    http_complete_calls=1,
                    timing=timing,
                )
                return "preempted"
            await self._cleanup_worker_runtime(
                preserve_shell=failed_report_status
                not in {"completed", "failed", "cancelled"}
            )
            await self._release_worker_claim(
                claim,
                reason="terminal_report_failed",
                park_on_exhaustion=False,
                timing=timing,
            )
            return "released:terminal_report_failed"

        # Do not rely on the heartbeat event observed before/during the HTTP
        # call. Re-read the exact token and authoritative job status after the
        # handler returns so a same-window control transition cannot be missed
        # before queue closure. Report-authored paused/failed states use the
        # same safe terminal closure; their cleanup disposition is status-based.
        post_report_status: str | None = None
        accepted_completion = None
        if not self._lease.lost.is_set():
            renewal = await renew_worker_batch(
                self._db,
                unit_id=unit.unit_id,
                lease_token=token,
            )
            if renewal is None:
                if await self._resolve_workspace_recovery(claim, lease_rejected=True):
                    return await self._handoff_workspace_recovery(claim=claim)
                accepted_completion = await self._accepted_worker_completion(claim)
                if accepted_completion is None:
                    self._lease.mark_lost()
                else:
                    post_report_status = accepted_completion.job_status
            else:
                post_report_status = renewal.job_status
                self._observe_worker_renewal(job_id, renewal)
        if self._lease.lost.is_set():
            await self._cleanup_worker_runtime(preserve_shell=True)
            logger.warning(
                "lease lost: worker unit=%s token=%d after accepted terminal "
                "report — successor owns queue closure",
                job_id,
                token,
            )
            return self._worker_terminal_timing_outcome(final_state)
        if self._worker_preempted.is_set():
            await self._finish_external_worker_preempt(
                claim,
                http_complete_calls=1,
                timing=timing,
            )
            return "preempted"
        if accepted_completion is not None:
            resolved_status = await self._finish_accepted_worker_completion(
                claim,
                accepted_completion,
                http_result_ambiguous=False,
            )
            return self._worker_terminal_timing_outcome(
                final_state,
                observed_status=resolved_status or accepted_completion.job_status,
            )
        await self._cleanup_worker_runtime(
            preserve_shell=post_report_status in _WORKER_PRESERVE_SHELL_STATUSES
        )
        if (
            self._worker_workspace_recovery is not None
            or self._worker_lease_disposition_uncertain
        ):
            return await self._handoff_worker_disposition(claim=claim)
        state = await complete_worker_batch(
            self._db,
            unit_id=unit.unit_id,
            lease_token=token,
            consumed_seq=unit.input_seq,
        )
        if state is None:
            self._lease.mark_lost()
            logger.warning(
                "lease lost: worker terminal queue closure fenced out unit=%s token=%d",
                job_id,
                token,
            )
            return self._worker_terminal_timing_outcome(
                final_state,
                observed_status=post_report_status,
            )
        logger.info(
            "worker_batch terminal: unit=%s token=%d queue_state=%s "
            "complete_calls=1 http_complete_calls=1",
            job_id,
            token,
            state,
        )
        return self._worker_terminal_timing_outcome(
            final_state,
            observed_status=post_report_status,
        )

    @staticmethod
    def _worker_completion_wire_payload(
        final_state: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], str]:
        """Return the exact payload source ``report_completion`` will use."""

        checkpointed = final_state.get("completion_report_payload")
        if isinstance(checkpointed, dict) and set(checkpointed) == set(
            _COMPLETION_REPORT_PAYLOAD_FIELDS
        ):
            return checkpointed, "checkpoint_envelope"
        return final_state, "live_state"

    @classmethod
    def _worker_terminal_timing_outcome(
        cls,
        final_state: Dict[str, Any],
        *,
        observed_status: str | None = None,
    ) -> str:
        """Label telemetry without making an orchestrator status decision."""

        normalized = str(observed_status or "").strip()
        if normalized and normalized not in {"created", "processing"}:
            return f"terminal:{normalized}"
        payload, _ = cls._worker_completion_wire_payload(final_state)
        freeze = payload.get("freeze_data")
        if isinstance(freeze, dict):
            reported_status = str(freeze.get("status") or "").strip()
            if reported_status:
                if reported_status == "job_completed":
                    reported_status = "completed"
                return f"terminal:{reported_status}"
        if payload.get("goal_achieved") is True:
            return "terminal:completed"
        if payload.get("error"):
            return "terminal:failed"
        return "terminal:unknown"

    @staticmethod
    def _parse_worker_bundle(
        bundle: Dict[str, Any], claim: WorkerClaim
    ) -> Tuple[JobStartRequest, Dict[str, Any]]:
        job_id = str(claim.unit_id)
        if (
            str(bundle.get("unit_id")) != job_id
            or str(bundle.get("job_id")) != job_id
            or bundle.get("unit_kind") != "worker_batch"
            or bundle.get("execution_lane") != "stateless"
        ):
            raise ValueError("claim bundle does not describe the leased worker unit")
        request = JobStartRequest.model_validate(bundle.get("job") or {})
        if request.job_id != job_id:
            raise ValueError("worker claim bundle job payload id mismatch")
        from shared.workspace_contract import validate_worker_workspace_projection

        validate_worker_workspace_projection(
            config_override=request.config_override,
            resolved_config=request.resolved_config,
            workspace_runtime=request.workspace_runtime,
        )
        authority = {
            "workspace_generation": request.workspace_generation,
            "workspace_runtime_incarnation": request.workspace_runtime_incarnation,
            "workspace_ssh_host_key_fingerprint": (
                request.workspace_ssh_host_key_fingerprint
            ),
            "workspace_owner_kind": request.workspace_owner_kind,
            "workspace_owner_id": request.workspace_owner_id,
        }
        if any(
            not isinstance(value, str) or not value.strip()
            for value in authority.values()
        ):
            raise ValueError("worker claim bundle is missing workspace authority")
        if request.workspace_owner_kind != "job":
            raise ValueError("worker claim workspace owner kind is invalid")
        for field in (
            "workspace_generation",
            "workspace_runtime_incarnation",
            "workspace_owner_id",
        ):
            value = str(authority[field])
            try:
                canonical = str(UUID(value))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"worker claim {field} is invalid") from exc
            if value != canonical:
                raise ValueError(f"worker claim {field} is invalid")
        if (
            re.fullmatch(
                r"SHA256:[A-Za-z0-9+/]{43}",
                str(request.workspace_ssh_host_key_fingerprint),
            )
            is None
        ):
            raise ValueError("worker claim SSH host identity is invalid")
        batch = bundle.get("batch")
        if not isinstance(batch, dict):
            raise ValueError("worker claim bundle is missing its batch envelope")
        return request, dict(batch)

    @staticmethod
    def _worker_job_metadata(request: JobStartRequest) -> Dict[str, Any]:
        """Build the exact metadata shape used by the pinned start path."""

        metadata: Dict[str, Any] = {"description": request.description}
        for field, key in (
            ("upload_id", "upload_id"),
            ("config_upload_id", "config_upload_id"),
            ("instructions_upload_id", "instructions_upload_id"),
            ("document_path", "document_path"),
            ("document_dir", "document_dir"),
        ):
            value = getattr(request, field)
            if value:
                metadata[key] = value
        if request.context:
            metadata.update(request.context)
        if request.instructions:
            metadata["instructions"] = request.instructions
        if request.config_name and request.config_name != "worker_base":
            metadata["config_name"] = request.config_name
        for field, key in (
            ("expert_id", "expert_id"),
            ("config_override", "config_override"),
            ("resolved_config", "resolved_config"),
            ("git_remote_url", "git_remote_url"),
            ("datasources", "datasources"),
            ("repositories", "repositories"),
            (
                "managed_repository_credentials",
                "managed_repository_credentials",
            ),
            ("branch_name", "branch_name"),
            ("project_id", "project_id"),
            ("runtime_actor", "runtime_actor"),
            ("workspace_runtime", "workspace_runtime"),
            ("workspace_provisioner", "workspace_provisioner"),
            ("workspace_generation", "workspace_generation"),
            (
                "workspace_runtime_incarnation",
                "workspace_runtime_incarnation",
            ),
            (
                "workspace_ssh_host_key_fingerprint",
                "workspace_ssh_host_key_fingerprint",
            ),
            ("workspace_owner_kind", "workspace_owner_kind"),
            ("workspace_owner_id", "workspace_owner_id"),
        ):
            value = getattr(request, field)
            if value:
                metadata[key] = value
        return metadata

    async def _resolve_workspace_recovery(
        self,
        claim: WorkerClaim,
        *,
        error: BaseException | None = None,
        final_state: dict[str, Any] | None = None,
        lease_rejected: bool = False,
    ) -> bool:
        """Resolve only typed workspace causes and exact accepted receipts."""
        self._worker_disposition_lookup_failed = False
        if self._worker_workspace_recovery is not None:
            return True
        receipt = error.recovery if isinstance(error, ClaimBundleError) else None
        if isinstance(receipt, WorkspaceRecoveryDisposition):
            if receipt.accepted_lease_token == claim.lease_token:
                self._worker_workspace_recovery = WorkspaceRecoveryHandoff(receipt)
                return True
        code = (
            error.code
            if isinstance(error, ClaimBundleError)
            else self._worker_workspace_recovery_code
        )
        if isinstance(error, WorkspaceUnavailableError):
            code = WorkspaceRecoveryCode.TRANSPORT_UNAVAILABLE
        state_error = (final_state or {}).get("error")
        if isinstance(state_error, dict):
            if state_error.get("type") == "workspace_unavailable":
                code = WorkspaceRecoveryCode.TRANSPORT_UNAVAILABLE
            else:
                with contextlib.suppress(ValueError, TypeError):
                    code = WorkspaceRecoveryCode(state_error.get("type"))
        eligible = workspace_recovery_enabled() and self._worker_workspace_backend in {
            None,
            "vm",
        }
        ambiguous_bundle = self._worker_workspace_backend is None and isinstance(
            error, (TimeoutError, ConnectionError, httpx.TransportError)
        )
        lookup_failed = False
        client = _pa()._orchestrator_client
        lookup = getattr(client, "get_workspace_recovery_disposition", None)
        if not callable(lookup):
            self._worker_disposition_lookup_failed = True
            return False
        try:
            receipt = await lookup(str(claim.unit_id), claim.lease_token)
        except Exception:
            # Endpoint availability alone says nothing about the workspace.
            # Bundle/lease uncertainty is separate from workspace recovery:
            # no workspace hold is implied without exact or typed evidence.
            self._worker_disposition_lookup_failed = True
            lookup_failed = True
            receipt = None
        if isinstance(receipt, WorkspaceRecoveryDisposition):
            if receipt.accepted_lease_token == claim.lease_token:
                self._worker_workspace_recovery = WorkspaceRecoveryHandoff(receipt)
                return True
        if not isinstance(code, WorkspaceRecoveryCode):
            if (
                lookup_failed
                and (ambiguous_bundle or lease_rejected)
                and self._worker_workspace_backend in {None, "vm"}
            ):
                self._worker_lease_disposition_uncertain = True
                self._lease.mark_lost()
            return False
        if not eligible:
            return False
        # Publish pending before awaiting: heartbeat observes the same local
        # admission fence and cannot issue a duplicate recovery request.
        self._worker_workspace_recovery = WorkspaceRecoveryHandoff(None)
        try:
            receipt = await client.report_workspace_recovery(
                str(claim.unit_id), claim.lease_token, code=code, request_id=uuid4()
            )
        except Exception:
            try:
                receipt = await lookup(str(claim.unit_id), claim.lease_token)
            except Exception:
                receipt = None
        if (
            isinstance(receipt, WorkspaceRecoveryDisposition)
            and receipt.accepted_lease_token == claim.lease_token
        ):
            self._worker_workspace_recovery = WorkspaceRecoveryHandoff(receipt)
        return True

    async def _join_worker_local_task(self, task: asyncio.Task) -> None:
        """Bound local retirement without mistaking cancellation for a join."""
        try:
            done, _ = await asyncio.wait(
                {task}, timeout=WORKER_RECOVERY_QUIESCE_SECONDS
            )
        except asyncio.CancelledError:
            self._worker_quarantined = True
            self.request_stop()
            task.add_done_callback(
                lambda completed: completed.exception()
                if not completed.cancelled()
                else None
            )
            raise
        if task not in done:
            self._worker_quarantined = True
            self.request_stop()
            # Do not cancel a to_thread retirement: its synchronous call is
            # still live. Keep the executor unusable until process replacement.
            task.add_done_callback(
                lambda completed: completed.exception()
                if not completed.cancelled()
                else None
            )
            raise _ClaimQuiescenceError("worker local retirement did not quiesce")
        await task

    async def _handoff_workspace_recovery(self, *, claim: WorkerClaim) -> str:
        """Retire local consumers, preserving tmux and the shared saver pool."""
        return await self._handoff_worker_disposition(claim=claim)

    def _worker_handoff_reason(self) -> str:
        if self._worker_workspace_recovery is not None:
            return "workspace_recovery"
        if self._worker_lease_disposition_uncertain:
            return "lease_disposition_uncertain"
        return "local_retirement"

    async def _handoff_worker_disposition(self, *, claim: WorkerClaim) -> str:
        """Join local work; leave an uncertain lease to exact reaper disposition."""
        reason = self._worker_handoff_reason()
        self._lease.mark_lost()
        runtime = _pa()._agent
        try:
            async with self._worker_retirement_lock:
                if runtime is not None and not self._worker_runtime_quiesced:
                    retire = getattr(runtime, "quiesce_worker_workspace_recovery", None)
                    if not callable(retire):
                        raise _ClaimQuiescenceError(
                            "worker runtime has no recovery quiescence gate"
                        )
                    await self._join_worker_local_task(asyncio.create_task(retire()))
                    self._worker_runtime_quiesced = True
        except (Exception, asyncio.CancelledError):
            self._worker_quarantined = True
            self.request_stop()
            logger.error(
                "worker local quiescence failed: unit=%s token=%d reason=%s",
                claim.unit_id,
                claim.lease_token,
                reason,
                exc_info=True,
            )
            return f"quarantined:{reason}"
        self._worker_recovery_handoff_done = True
        # No release/complete/refund: an accepted receipt owns disposition.
        # Without one, uncertainty leaves the exact lease to expiry/reaper;
        # local retirement alone does not establish a durable recovery hold.
        return reason

    async def _consume_worker_stream(
        self,
        stream: AsyncIterator[Dict[str, Any]],
        *,
        timing: dict[str, Any],
        agent_start_started_at: float,
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Race each graph step against exact-lease loss and external stop."""

        final_state: Optional[Dict[str, Any]] = None
        stream_started_at: float | None = None
        lost_waiter = asyncio.create_task(self._lease.lost.wait())
        preempt_waiter = asyncio.create_task(self._worker_preempted.wait())
        try:
            while True:
                next_state = asyncio.create_task(anext(stream))
                try:
                    await asyncio.wait(
                        {next_state, lost_waiter, preempt_waiter},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    # Ownership/control always wins a same-tick graph result.
                    if self._lease.lost.is_set():
                        next_state.cancel()
                        with contextlib.suppress(asyncio.CancelledError, Exception):
                            await self._join_worker_local_task(next_state)
                        return "lease_lost", final_state
                    if self._worker_preempted.is_set():
                        next_state.cancel()
                        with contextlib.suppress(asyncio.CancelledError, Exception):
                            await next_state
                        return "preempted", final_state
                    try:
                        state = next_state.result()
                    except StopAsyncIteration:
                        return "graph_end", final_state
                    if stream_started_at is None:
                        stream_started_at = time.perf_counter()
                        timing["agent_start"] = max(
                            0.0, stream_started_at - agent_start_started_at
                        )
                    if isinstance(state, dict):
                        final_state = state
                        error = state.get("error")
                        if isinstance(error, dict):
                            if error.get("type") == "workspace_unavailable":
                                self._worker_workspace_recovery_code = (
                                    WorkspaceRecoveryCode.TRANSPORT_UNAVAILABLE
                                )
                            else:
                                with contextlib.suppress(ValueError, TypeError):
                                    self._worker_workspace_recovery_code = (
                                        WorkspaceRecoveryCode(error.get("type"))
                                    )
                        if self._worker_workspace_recovery_code is not None:
                            if (
                                workspace_recovery_enabled()
                                and self._worker_workspace_backend == "vm"
                            ):
                                self._lease.mark_lost()
                                return "workspace_recovery", final_state
                finally:
                    if not next_state.done():
                        next_state.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await self._join_worker_local_task(next_state)
        finally:
            finished_at = time.perf_counter()
            if timing["agent_start"] is None:
                timing["agent_start"] = max(0.0, finished_at - agent_start_started_at)
            timing["stream"] = (
                max(0.0, finished_at - stream_started_at)
                if stream_started_at is not None
                else 0.0
            )
            for waiter in (lost_waiter, preempt_waiter):
                if not waiter.done():
                    waiter.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await waiter

    async def _worker_heartbeat_loop(self, claim: WorkerClaim) -> None:
        unit = claim.unit
        job_id = str(unit.unit_id)
        token = unit.lease_token
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            try:
                renewal = await renew_worker_batch(
                    self._db,
                    unit_id=unit.unit_id,
                    lease_token=token,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if await self._resolve_workspace_recovery(claim, error=e):
                    self._lease.mark_lost()
                    return
                if (
                    self._worker_workspace_backend == "vm"
                    and self._ambiguous_worker_renewal(e)
                ):
                    self._worker_lease_disposition_uncertain = True
                    self._lease.mark_lost()
                    return
                logger.warning(
                    "worker lease heartbeat failed for unit %s (transient): %s",
                    job_id,
                    e,
                )
                continue
            if renewal is None:
                if await self._resolve_workspace_recovery(claim, lease_rejected=True):
                    self._lease.mark_lost()
                    return
                accepted_completion = await self._accepted_worker_completion(claim)
                if accepted_completion is not None:
                    logger.info(
                        "worker completion accepted during heartbeat: unit=%s "
                        "token=%d command=%s state=%s",
                        job_id,
                        token,
                        accepted_completion.command_id,
                        accepted_completion.command_state,
                    )
                    return
                self._lease.mark_lost()
                logger.warning(
                    "lease lost: worker unit=%s token=%d (renewal rejected)",
                    job_id,
                    token,
                )
                return
            self._observe_worker_renewal(job_id, renewal)

    @staticmethod
    def _ambiguous_worker_renewal(error: BaseException) -> bool:
        """Connection loss/timeout cannot establish whether renewal committed."""
        if isinstance(error, (TimeoutError, ConnectionError, httpx.TransportError)):
            return True
        from asyncpg import InterfaceError, PostgresConnectionError

        return isinstance(error, (InterfaceError, PostgresConnectionError))

    async def _accepted_worker_completion(
        self,
        claim: WorkerClaim,
        *,
        command_id: Any | None = None,
    ) -> WorkerCompletionAcceptance | None:
        """Return an exact B4 accept only while the shared rollout gate is on."""

        if not self._completion_commands_enabled:
            return None
        try:
            kwargs: dict[str, Any] = {
                "unit_id": claim.unit_id,
                "lease_token": claim.lease_token,
            }
            if command_id is not None:
                kwargs["command_id"] = command_id
            return await get_worker_completion_acceptance(self._db, **kwargs)
        except Exception as exc:
            logger.warning(
                "worker completion-acceptance lookup failed for unit %s "
                "(treating renewal rejection as lease loss): %s",
                claim.unit_id,
                exc,
            )
            return None

    @staticmethod
    def _stored_worker_completion_status(
        accepted: WorkerCompletionAcceptance,
    ) -> str | None:
        """Resolve shell disposition from the finalized command's own result."""

        outcome = accepted.command_outcome
        state = accepted.command_state
        value: Any = None
        if state == "done":
            value = outcome.get("new_status")
        elif state == "superseded":
            value = outcome.get("observed_status") or outcome.get("observed_job_status")
        elif state == "force_resolved":
            value = outcome.get("terminal_status")
        normalized = str(value or "").strip()
        return normalized or None

    @staticmethod
    def _worker_finalization_poll_delay(
        accepted: WorkerCompletionAcceptance,
    ) -> float:
        """Bound the next observation by DB-clock run/lease/deadline horizons."""

        horizons = [
            WORKER_FINALIZATION_POLL_SECONDS,
            accepted.deadline_remaining_seconds,
        ]
        if accepted.command_state == "pending":
            horizons.append(accepted.run_after_remaining_seconds)
        elif (
            accepted.command_state == "finalizing"
            and accepted.lease_remaining_seconds is not None
        ):
            horizons.append(accepted.lease_remaining_seconds)
        positive = [value for value in horizons if value > 0]
        return max(0.05, min(positive or [WORKER_FINALIZATION_POLL_SECONDS]))

    async def _sleep_worker_finalization_poll(self, seconds: float) -> None:
        """Sleep seam kept separate from the worker lease heartbeat in tests."""

        await asyncio.sleep(seconds)

    async def _finish_accepted_worker_completion(
        self,
        claim: WorkerClaim,
        accepted: WorkerCompletionAcceptance,
        *,
        http_result_ambiguous: bool,
    ) -> str | None:
        """Hold an accepted worker shell until its exact command resolves.

        B4 already changed the queue row to ``done``; releasing or completing
        it here would create a second execution owner.  Pending/finalizing work
        instead retires local shell admission and scrubs claim-local state,
        then observes the same command through the established B4 lookup.  Its
        PostgreSQL deadline is the absolute local wait bound.  Parked, lookup
        loss, deadline, or cancellation hands the still-live remote shell to
        the command/lifecycle backstop without requeueing.  Only an explicit
        terminal status in the stored finalized outcome destroys tmux.
        """

        current = accepted
        self._worker_completion_accepted_generation = (
            str(claim.unit_id),
            int(claim.lease_token),
        )
        held = False
        while (
            current.command_state in _WORKER_UNFINISHED_COMMAND_STATES
            and not current.deadline_expired
        ):
            if not held:
                agent = _pa()._agent
                if agent is not None:
                    await agent.hold_worker_finalization()
                held = True
                logger.info(
                    "worker finalization-pending hold: unit=%s token=%d "
                    "command=%s state=%s deadline_in=%.3fs",
                    claim.unit_id,
                    claim.lease_token,
                    current.command_id,
                    current.command_state,
                    current.deadline_remaining_seconds,
                )
            await self._sleep_worker_finalization_poll(
                self._worker_finalization_poll_delay(current)
            )
            observed = await self._accepted_worker_completion(
                claim,
                command_id=current.command_id,
            )
            if observed is None:
                await self._cleanup_worker_runtime(preserve_shell=True)
                logger.warning(
                    "worker finalization hold handed off after lookup loss: "
                    "unit=%s token=%d command=%s",
                    claim.unit_id,
                    claim.lease_token,
                    current.command_id,
                )
                return None
            current = observed

        resolved_status = (
            self._stored_worker_completion_status(current)
            if current.command_state in _WORKER_FINALIZED_COMMAND_STATES
            else None
        )
        preserve_shell = resolved_status not in _WORKER_TERMINAL_JOB_STATUSES
        await self._cleanup_worker_runtime(preserve_shell=preserve_shell)
        logger.info(
            "worker_batch completion handoff settled: unit=%s token=%d "
            "queue_state=%s command=%s command_state=%s outcome_status=%s "
            "shell=%s ambiguous_http=%s complete_calls=0 http_complete_calls=1",
            claim.unit_id,
            claim.lease_token,
            current.queue_state,
            current.command_id,
            current.command_state,
            resolved_status,
            "preserved" if preserve_shell else "retired",
            http_result_ambiguous,
        )
        return resolved_status or current.job_status

    def _observe_worker_renewal(self, job_id: str, renewal: WorkerRenewal) -> None:
        self._seed_worker_inboxes(
            job_id,
            list(renewal.pending_guidance),
            list(renewal.queued_replies),
        )
        if renewal.preempted:
            if not self._worker_preempted.is_set():
                logger.info(
                    "worker_batch preempt discovered: unit=%s status=%s",
                    job_id,
                    renewal.job_status,
                )
            self._worker_preempt_status = renewal.job_status
            self._worker_preempted.set()

    @staticmethod
    def _seed_worker_inboxes(
        job_id: str,
        pending_guidance: Any,
        queued_replies: Any,
    ) -> None:
        try:
            import agent.api.dual_app as dual_app

            dual_app._replace_inbox(
                dual_app._guidance_inbox,
                job_id,
                pending_guidance,
                "Supervisor guidance",
            )
            dual_app._replace_inbox(
                dual_app._reply_inbox,
                job_id,
                queued_replies,
                "Queued replies",
            )
        except Exception:
            logger.debug("Worker steering inbox refresh failed", exc_info=True)

    @staticmethod
    def _worker_stop_is_recoverable(
        final_state: Dict[str, Any], freeze_type: Any
    ) -> bool:
        # An explicit human-facing freeze wins over a coincident retryable
        # error.  Those stops must remain visible/actionable through the
        # completion handler (condition 3 of the governing scope correction).
        if freeze_type and freeze_type not in AUTO_CONTINUE_FREEZE_TYPES:
            return False
        error = final_state.get("error")
        if isinstance(error, dict) and error.get("recoverable") is True:
            return True
        return bool(
            freeze_type in AUTO_CONTINUE_FREEZE_TYPES
            and freeze_type != FREEZE_TYPE_BATCH_BOUNDARY
        )

    @staticmethod
    def _worker_driver_error_state(
        message: str,
        *,
        job_id: str | None = None,
    ) -> Dict[str, Any]:
        return {
            "job_id": job_id,
            "should_stop": True,
            "goal_achieved": False,
            "error": {
                "type": "worker_driver_error",
                "recoverable": True,
                "message": str(message),
            },
        }

    @staticmethod
    def _worker_retry_exhausted_state(
        final_state: Dict[str, Any],
        *,
        attempts: int,
        max_attempts: int,
    ) -> Dict[str, Any]:
        """Turn the queue's last recoverable attempt into a visible give-up.

        The queue owns retry accounting, but the orchestrator remains the sole
        job-status authority. The final holder therefore reports a factual,
        non-recoverable terminal envelope while it still owns the exact token;
        it never writes ``jobs.status`` directly.
        """

        exhausted = dict(final_state)
        prior_error = final_state.get("error")
        error = dict(prior_error) if isinstance(prior_error, dict) else {}
        prior_freeze = final_state.get("freeze_data")
        reason = error.get("message")
        if not reason and isinstance(prior_freeze, dict):
            reason = prior_freeze.get("reason") or prior_freeze.get("error_summary")
        error.update(
            {
                "type": "worker_retry_exhausted",
                "recoverable": False,
                "message": (
                    f"Stateless worker recovery exhausted {attempts}/{max_attempts} "
                    f"queue attempts" + (f": {reason}" if reason else "")
                ),
            }
        )
        exhausted.update(
            {
                "should_stop": True,
                "goal_achieved": False,
                "error": error,
                "freeze_data": {
                    "freeze_type": "worker_retry_exhausted",
                    "reason": error["message"],
                    "attempts": attempts,
                    "max_attempts": max_attempts,
                    "prior_freeze": prior_freeze,
                },
            }
        )
        return exhausted

    async def _finish_external_worker_preempt(
        self,
        claim: WorkerClaim,
        *,
        http_complete_calls: int = 0,
        timing: dict[str, Any] | None = None,
    ) -> None:
        status = self._worker_preempt_status or "unknown"
        preserve_shell = status in _WORKER_PRESERVE_SHELL_STATUSES
        await self._cleanup_worker_runtime(preserve_shell=preserve_shell)
        if (
            self._worker_workspace_recovery is not None
            or self._worker_lease_disposition_uncertain
        ):
            return
        started_at = time.perf_counter()
        try:
            state = await complete_worker_batch(
                self._db,
                unit_id=claim.unit_id,
                lease_token=claim.lease_token,
                consumed_seq=claim.unit.input_seq,
            )
        finally:
            self._add_worker_finish_timing(timing, started_at)
        logger.info(
            "worker_batch external stop: unit=%s token=%d status=%s "
            "queue_state=%s complete_calls=0 http_complete_calls=%d",
            claim.unit_id,
            claim.lease_token,
            status,
            state,
            http_complete_calls,
        )

    async def _cleanup_worker_runtime(self, *, preserve_shell: bool) -> None:
        async with self._worker_retirement_lock:
            await self._cleanup_worker_runtime_locked(preserve_shell=preserve_shell)

    async def _stop_worker_heartbeat(self) -> None:
        """Close the concurrent receipt publisher before a shell decision."""
        heartbeat = self._worker_heartbeat_task
        if heartbeat is None:
            return
        heartbeat.cancel()

        async def join() -> None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await heartbeat

        await self._join_worker_local_task(asyncio.create_task(join()))
        self._worker_heartbeat_task = None

    async def _cleanup_worker_runtime_locked(self, *, preserve_shell: bool) -> None:
        agent = _pa()._agent
        if agent is not None:
            if self._lease.lost.is_set():
                # Local lease loss is already sufficient to revoke effect
                # authority.  Do not ask quiesce/its DB probe to rediscover the
                # steal after a race; abandon first guarantees zero further
                # child writes or provider/tool effects before ToolContext is
                # scrubbed.
                abandon = getattr(agent, "abandon_worker_subagents", None)
                if callable(abandon):
                    await abandon("worker lease authority lost")
            if self._worker_workspace_backend == "vm":
                # Every VM retirement may race a recovery acceptance. Obtain
                # positive local quiescence BEFORE ordinary cleanup can scrub
                # the original backend/graph/child handles. The hold retains
                # only an immutable terminal-cleanup capability afterward.
                try:
                    if not self._worker_runtime_quiesced:
                        await self._join_worker_local_task(
                            asyncio.create_task(
                                agent.hold_worker_finalization(strict=True)
                            )
                        )
                        self._worker_runtime_quiesced = True
                except (Exception, asyncio.CancelledError) as exc:
                    self._worker_quarantined = True
                    self.request_stop()
                    if isinstance(exc, asyncio.CancelledError):
                        raise
                    raise _ClaimQuiescenceError(
                        "VM worker retirement did not quiesce"
                    ) from exc
            if not preserve_shell and self._worker_active_claim is not None:
                # No publisher may commit a receipt after this decision. Join
                # it, then replay the exact disposition under the retirement
                # lock. An unreadable disposition preserves tmux without
                # inventing recovery or changing ordinary queue completion.
                await self._stop_worker_heartbeat()
                if await self._resolve_workspace_recovery(self._worker_active_claim):
                    self._lease.mark_lost()
                preserve_shell = self._worker_disposition_lookup_failed
            await agent.cleanup_worker_claim(
                preserve_shell=preserve_shell
                or self._worker_workspace_recovery is not None
                or self._worker_lease_disposition_uncertain
            )

    async def _release_worker_claim(
        self,
        claim: WorkerClaim,
        *,
        reason: str,
        park_on_exhaustion: bool = True,
        timing: dict[str, Any] | None = None,
    ) -> None:
        if self._lease.lost.is_set() or self._worker_quarantined:
            logger.info(
                "run_queue release: worker unit=%s token=%d reason=%s "
                "skipped after local ownership loss",
                claim.unit_id,
                claim.lease_token,
                reason,
            )
            return
        started_at = time.perf_counter()
        try:
            state = await release_worker_batch(
                self._db,
                unit_id=claim.unit_id,
                lease_token=claim.lease_token,
                park_on_exhaustion=park_on_exhaustion,
            )
        except Exception:
            logger.warning(
                "run_queue release failed for worker unit %s (reason=%s) — "
                "the lease will expire instead",
                claim.unit_id,
                reason,
                exc_info=True,
            )
            return
        finally:
            self._add_worker_finish_timing(timing, started_at)
        logger.info(
            "run_queue release: worker unit=%s token=%d reason=%s state=%s",
            claim.unit_id,
            claim.lease_token,
            reason,
            state,
        )

    async def _serve_claim(self, claim: ClaimedUnit) -> None:
        pa = _pa()
        unit_id = str(claim.unit_id)
        token = claim.lease_token
        # A newly claimed generation cannot inherit an effect marker from a
        # durably disposed predecessor. The PA copy remains exact-token
        # scoped until attach/turn start resets its session-local diagnostics.
        self._tool_effect_identity = None
        logger.info(
            "run_queue claim: unit=%s kind=%s token=%d attempts=%d "
            "input_seq=%s consumed_seq=%s control_input_seq=%s "
            "control_consumed_seq=%s pod=%s",
            unit_id,
            claim.unit_kind,
            token,
            claim.attempts_since_completion,
            claim.input_seq,
            claim.consumed_seq,
            claim.control_input_seq,
            claim.control_consumed_seq,
            self._pod_name,
        )

        # (a) Skip-if-answered (§5.1): a steal can land between a
        # predecessor's final persist and its completion — the fence cannot
        # catch that (our lease is VALID), only the watermark can. No LLM.
        watermarks_answered = (
            claim.consumed_seq is not None
            and claim.input_seq is not None
            and claim.consumed_seq >= claim.input_seq
            and claim.control_consumed_seq >= claim.control_input_seq
        )
        pending_event = False
        if watermarks_answered:
            try:
                fetchval = getattr(self._db, "fetchval", None)
                if fetchval is not None:
                    pending_event = bool(
                        await fetchval(_PENDING_EVENT_EXISTS_SQL, claim.unit_id)
                    )
            except Exception:
                logger.warning(
                    "pending-event authority query failed for unit %s; "
                    "releasing instead of skipping",
                    unit_id,
                    exc_info=True,
                )
                await self._release(claim, reason="pending_event_query_failed")
                return
        if watermarks_answered and not pending_event:
            await self._detach_physical_before_transition("skip_if_answered")
            state = await complete_unit(
                self._db,
                unit_id=claim.unit_id,
                lease_token=token,
                consumed_seq=claim.consumed_seq,
            )
            logger.info(
                "run_queue complete: unit=%s consumed_seq=%s state=%s "
                "(skip-if-answered)",
                unit_id,
                claim.consumed_seq,
                state,
            )
            if state is None:
                await self._ack_terminal_claim_loss(claim)
            self._mark_warm(claim)
            return

        # (b) Skip-if-answered, transcript leg: the oldest pending input
        # already has its final answer in thread_messages (a predecessor's
        # settlement died after the answer landed). Advance the watermark to
        # that input instead of answering it again; complete_unit re-queues
        # the unit when newer input is waiting behind it. A pending event
        # delivery keeps the ordinary path (the claim must run it).
        if (
            not watermarks_answered
            and claim.input_seq is not None
            and claim.control_consumed_seq >= claim.control_input_seq
        ):
            answered_seq = await self._transcript_answered_seq(claim)
            if answered_seq is not None:
                try:
                    fetchval = getattr(self._db, "fetchval", None)
                    pending_event = bool(
                        fetchval is not None
                        and await fetchval(_PENDING_EVENT_EXISTS_SQL, claim.unit_id)
                    )
                except Exception:
                    logger.warning(
                        "pending-event authority query failed for unit %s; "
                        "releasing instead of skipping (transcript leg)",
                        unit_id,
                        exc_info=True,
                    )
                    await self._release(claim, reason="pending_event_query_failed")
                    return
                if not pending_event:
                    await self._detach_physical_before_transition(
                        "skip_if_answered_by_transcript"
                    )
                    state = await complete_unit(
                        self._db,
                        unit_id=claim.unit_id,
                        lease_token=token,
                        consumed_seq=answered_seq,
                    )
                    logger.info(
                        "run_queue complete: unit=%s consumed_seq=%s state=%s "
                        "(skip-if-answered: transcript holds the final answer "
                        "for input seq %s; watermark was %s)",
                        unit_id,
                        answered_seq,
                        state,
                        answered_seq,
                        claim.consumed_seq,
                    )
                    if state is None:
                        await self._ack_terminal_claim_loss(claim)
                    self._mark_warm(claim)
                    return

        # (c) Independent heartbeat — spawned BEFORE the bundle fetch/attach,
        # which can themselves outlast the 60s lease TTL (MCP connect_all,
        # message-tail restore). Never an astream hook.
        claim_lost = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(claim, claim_lost),
            name=f"lease-heartbeat-{unit_id[:8]}",
        )
        cancelled = False
        try:
            await self._serve_claim_inner(pa, claim, unit_id, token, claim_lost)
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            # Belt for every exception/cancellation path. Normal completion
            # and release paths stop it earlier, before mutating the queue;
            # this prevents a lease-scoped consumer leaking into warm idle.
            interrupt_turn = (
                pa._interrupt_owner_turn_id
                if pa._interrupt_owner_lease_token == token
                else None
            )
            if interrupt_turn is not None:
                try:
                    await self._close_interrupt_window(
                        pa,
                        claim,
                        target_turn_id=int(interrupt_turn),
                    )
                except (asyncio.CancelledError, Exception):
                    self._lease.mark_lost()
                    logger.warning(
                        "interrupt window cleanup failed; lease will not be "
                        "released (unit=%s token=%d turn=%d)",
                        unit_id,
                        token,
                        interrupt_turn,
                        exc_info=True,
                    )
            else:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await pa._stop_thread_interrupt_watcher()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await pa._stop_thread_control_watcher()
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await heartbeat_task
            pa._turn_start_external_hook = None
            pa._turn_complete_external_hook = None
            pa._turn_tool_execution_external_hook = None
            settled = self._settled_claim == (str(claim.unit_id), int(token))
            self._settled_claim = None
            shutdown_retry = self._shutdown_retry_claim == (unit_id, int(token))
            if shutdown_retry:
                self._shutdown_retry_claim = None
            if cancelled or shutdown_retry:
                # SIGTERM's hard-cancel is still an ownership transition. Do
                # not leave a live exact claim to expire after this Pod object
                # disappears: first drain every local/SFTP/background writer,
                # then disposition the exact token while this process is
                # alive. Pre-effect work is released for retry; a claim that
                # crossed a tool-effect boundary is parked so no successor can
                # automatically replay an ambiguous external side effect.
                await self._shutdown_dispose_cancelled_claim(claim, settled=settled)
            if claim_lost.is_set() or self._exact_claim_handle_lost(claim):
                if not await self._ack_terminal_claim_loss(claim):
                    # A pod that cannot durably settle its exact claimant debt
                    # must never return to the claim loop.  The reaper's
                    # UID-preconditioned eviction/absence path is the only
                    # safe successor owner from here.
                    self.request_stop()

    async def _shutdown_dispose_cancelled_claim(
        self, claim: ClaimedUnit, *, settled: bool = False
    ) -> None:
        """Quiesce and durably dispose one SIGTERM-cancelled session claim.

        Commit-then-effects (stateless_turn_resilience.md step 4a), the four
        branches: (1) the turn settled — its answer is durable and the
        interrupt close in ``_serve_claim``'s finally already checkpointed
        ``consumed_seq`` — so complete the unit (idempotent CAS) and leave the
        push, if any, handed off; a failed CAS releases, never parks. (2) not
        settled, no tool effect crossed — release with attempts++. (3) not
        settled, effect crossed, but every persisted tool call has its
        durable ToolMessage — release: the successor continues from the
        transcript. (4) an in-flight tool with no result row — the only
        remaining park.
        """

        pa = _pa()
        # Capture only an exact claim identity before physical detach may
        # clear process-local session state. This classification is monotonic
        # for the remainder of this claim's cancellation cleanup.
        tool_execution_started = self._claim_crossed_tool_effect(pa, claim=claim)

        if settled:
            await self._quiesce_claim_before_transition(
                pa,
                reason="shutdown_after_settle",
                claim=claim,
            )
            if not self._lease.lost.is_set():
                try:
                    state = await asyncio.wait_for(
                        self._complete_with_retry(claim, consumed_seq=None),
                        timeout=SHUTDOWN_COMPLETE_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        "run_queue complete hung for %.0fs during shutdown: "
                        "unit=%s token=%d — releasing under the checkpoint",
                        SHUTDOWN_COMPLETE_TIMEOUT_SECONDS,
                        claim.unit_id,
                        claim.lease_token,
                    )
                    state = "error"
                if state is not None and state != "error":
                    self._clear_claim_tool_effect(pa, claim)
                    logger.info(
                        "run_queue complete: unit=%s token=%d state=%s "
                        "(shutdown after settle; push handed off)",
                        claim.unit_id,
                        claim.lease_token,
                        state,
                    )
                    return
                if state is None:
                    await self._ack_terminal_claim_loss(claim)
                    self._clear_claim_tool_effect(pa, claim)
                    return
            # Completion could not be made durable: the checkpoint already
            # protects the answer, so a plain release is safe (skip-if-answered).
            tool_execution_started = False
        elif tool_execution_started:
            if await self._tool_effects_durable(claim):
                logger.warning(
                    "shutdown mid-turn after tool effects, all results durable: "
                    "releasing unit=%s token=%d for a successor to continue",
                    claim.unit_id,
                    claim.lease_token,
                )
                tool_execution_started = False
            else:
                await self._quiesce_and_park_post_effect_claim(
                    pa,
                    claim,
                    reason="uncooperative_shutdown",
                )
                return

        if not settled:
            await self._quiesce_claim_before_transition(
                pa,
                reason="shutdown_cancelled_claim",
                claim=claim,
            )

        last_error: BaseException | None = None
        for attempt in range(1, COMPLETE_RETRY_ATTEMPTS + 1):
            try:
                state = await release_unit(
                    self._db,
                    unit_id=claim.unit_id,
                    lease_token=claim.lease_token,
                    error=True,
                )
                if state is not None:
                    self._clear_claim_tool_effect(pa, claim)
                    logger.info(
                        "run_queue release: unit=%s token=%d "
                        "reason=shutdown_cancelled state=%s",
                        claim.unit_id,
                        claim.lease_token,
                        state,
                    )
                    return
                # End/reaper won. Its thread->queue transaction publishes any
                # credential-bearing claimant debt before this SELECT can see
                # the advanced token; exact post-detach ACK is therefore safe.
                await self._ack_terminal_claim_loss(claim)
                self._clear_claim_tool_effect(pa, claim)
                return
            except asyncio.CancelledError:
                # The outer task is already cancelled; cleanup itself must be
                # cancellation-resistant until it has a durable disposition.
                continue
            except BaseException as exc:
                last_error = exc
                if attempt < COMPLETE_RETRY_ATTEMPTS:
                    await asyncio.sleep(0.5 * attempt)
        raise RuntimeError(
            "shutdown could not durably release its exact stateless claim"
        ) from last_error

    async def _serve_claim_inner(
        self,
        pa: Any,
        claim: ClaimedUnit,
        unit_id: str,
        token: int,
        claim_lost: asyncio.Event,
    ) -> None:
        # (d) Claim bundle — the pinned contract. On failure the token-guarded
        # release below is a no-op when the lease is genuinely gone (401/403 =
        # "treat as lease lost: release nothing" happens naturally via the
        # WHERE lease_token=$2 AND state='leased' guard in release_unit).
        timing: Dict[str, float] = {}
        t0 = time.perf_counter()
        try:
            bundle = await self._fetch_bundle(unit_id, token)
        except ClaimBundleError as e:
            if e.status_code == 404:
                logger.warning(
                    "claim bundle 404 for unit %s — unit vanished, dropping claim",
                    unit_id,
                )
                return
            logger.warning(
                "claim bundle %d for unit %s — releasing (token-guarded: a "
                "genuinely lost lease makes this a no-op): %s",
                e.status_code,
                unit_id,
                e.detail[:200] if e.detail else "",
            )
            await self._release(claim, reason=f"bundle_{e.status_code}")
            return
        except Exception as e:
            logger.warning("claim bundle fetch failed for unit %s: %s", unit_id, e)
            await self._release(claim, reason="bundle_error")
            return

        # The heartbeat starts before the slow credential bundle.  Its loss
        # signal is claim-local because the shared LeaseHandle may still point
        # at the previous warm session until that session is safely detached.
        # Never erase a loss by installing a fresh handle/event afterwards.
        if claim_lost.is_set():
            logger.warning(
                "lease lost during claim bundle: unit=%s token=%d; "
                "discarding credentials without attach or release",
                unit_id,
                token,
            )
            return

        timing["bundle"] = time.perf_counter() - t0
        attach = bundle.get("attach") or {}
        # Watermarks: the claim's values were read atomically inside the claim
        # statement — they are the authority; the bundle's copy is diagnostics.
        consumed_seq = claim.consumed_seq

        # (e) Attach with affinity + (b/D) scrub-on-claim.
        fingerprint = attach_fingerprint(attach)
        fresh_attach = False
        reuse = (
            pa._session is not None
            and pa._thread_id == unit_id
            and self._attached_fingerprint == fingerprint
            and pa._session.stateless_warm_reuse_safe
        )
        t0 = time.perf_counter()
        if reuse:
            # Same lite thread, unchanged config: reuse the live session. The
            # mutable LeaseHandle repoints every fenced writer (message
            # persists, journal flushes) at THIS claim's token in place.
            # Physical sessions deliberately miss this path: detach retires
            # their old backend before a fresh object receives the new token.
            self._activate_lease(unit_id, token)
            if claim_lost.is_set():
                self._lease.mark_lost()
                return
            logger.info(
                "session reuse (affinity): unit=%s fingerprint=%s",
                unit_id,
                fingerprint[:12],
            )
        else:
            if (
                pa._session is not None
                and pa._thread_id == unit_id
                and self._attached_bundle is not None
            ):
                # Same thread, changed fingerprint: name WHICH bundle paths
                # were volatile (§5.3.4 affinity-miss diagnosis; paths only,
                # never values).
                changed = fingerprint_diff_paths(self._attached_bundle, attach)
                logger.info("affinity miss: unit=%s changed_paths=%s", unit_id, changed)
            if pa._session is not None:
                # Detach BEFORE repointing the lease handle: the old writer's
                # close/drain must flush under the OLD unit's lease identity,
                # not this claim's.
                await self._detach_cached_session("claim_switch")
            timing["detach"] = time.perf_counter() - t0
            self._scrub_process_residue()
            self._activate_lease(unit_id, token)
            if claim_lost.is_set():
                self._lease.mark_lost()
                return
            t0 = time.perf_counter()
            try:
                await pa._attach_session(**attach)
            except Exception as e:
                logger.warning(
                    "attach failed for unit %s: %s", unit_id, e, exc_info=True
                )
                await self._detach_cached_session("attach_failed")
                await self._release_attach_failure(claim, e)
                return
            self._attached_fingerprint = fingerprint
            self._attached_bundle = attach
            fresh_attach = True
        if claim_lost.is_set():
            self._lease.mark_lost()
            await self._detach_cached_session("lease_lost_during_attach")
            return
        timing["attach"] = time.perf_counter() - t0

        # Fresh attach performs foreground-child recovery before publishing
        # readiness. That transaction may advance ``consumed_seq`` without
        # enqueueing a replacement event when the parent's final AI response
        # is already durable. The claim snapshot predates attach, so re-read
        # the exact still-leased row before selecting pending input; otherwise
        # this executor can inject the superseded input and pay the provider a
        # second time. The identity predicate also serves as the post-recovery
        # lease fence.
        refreshed_consumed_seq = await self._db.fetchval(
            _EXACT_CONSUMED_SEQ_AFTER_ATTACH_SQL,
            unit_id,
            token,
            self._pod_name,
            consumed_seq if consumed_seq is not None else -1,
        )
        if refreshed_consumed_seq is None:
            self._lease.mark_lost()
            await self._detach_cached_session("lease_lost_after_attach_recovery")
            return
        consumed_seq = int(refreshed_consumed_seq)

        # Bind every remote tmux mutation to this monotonic queue token before
        # controls or user input can start tool work. Reuse invalidates the
        # previous claim's local tab cache; fresh attach records the token for
        # the first lazy shell initialization.
        if pa._session is not None:
            pa._session.set_shell_owner_token(token)

        # A claim may beat the reaper's post-steal journal transaction. Close
        # that abandoned generation and settle its exact interrupted input
        # before controls or pending-input selection can expose successor
        # output. The returned watermark is newer than the claim snapshot
        # precisely when an applied old-turn receipt consumed its target.
        t0 = time.perf_counter()
        try:
            (
                stale_count,
                recovered_consumed_seq,
            ) = await pa._reconcile_stale_thread_interrupts(lease_token=token)
        except Exception as e:
            logger.warning(
                "stale-interrupt recovery failed for unit %s; no successor "
                "input will be injected: %s",
                unit_id,
                e,
                exc_info=True,
            )
            await self._detach_cached_session("stale_interrupt_recovery_failed")
            await self._release(claim, reason="stale_interrupt_recovery_failed")
            return
        timing["interrupt_recovery"] = time.perf_counter() - t0
        if recovered_consumed_seq is not None:
            consumed_seq = max(
                consumed_seq if consumed_seq is not None else -1,
                int(recovered_consumed_seq),
            )
        if stale_count:
            logger.info(
                "session-interrupt claim recovery: unit=%s token=%d count=%d "
                "consumed_seq=%s total=%.3fs",
                unit_id,
                token,
                stale_count,
                consumed_seq,
                timing["interrupt_recovery"],
            )

        # Controls are consumed only by the exact serving owner. The initial
        # drain is synchronous so a control-only claim cannot take either
        # no-input completion edge; the watcher then stays live for mid-turn
        # mode changes until we stop it immediately before complete/release.
        t0 = time.perf_counter()
        try:
            drained_controls = await pa._start_thread_control_watcher(lease_token=token)
        except Exception as e:
            logger.warning(
                "control-inbox attach failed for unit %s; request remains "
                "pending for retry: %s",
                unit_id,
                e,
                exc_info=True,
            )
            # A strict journal fence can terminally close the attached writer.
            # Never leave that dead writer in the affinity cache for the next
            # claim; detach while the handle still carries this claim's token.
            await self._detach_cached_session("control_inbox_failed")
            await self._release(claim, reason="control_inbox_failed")
            return
        timing["controls"] = time.perf_counter() - t0
        if drained_controls:
            logger.info(
                "session-control claim drain: unit=%s token=%d count=%d total=%.3fs",
                unit_id,
                token,
                drained_controls,
                timing["controls"],
            )

        # (f) Oldest unanswered input.
        t0 = time.perf_counter()
        try:
            pending = await self._fetch_pending_rows(unit_id, consumed_seq)
        except Exception as e:
            logger.warning("pending-input query failed for unit %s: %s", unit_id, e)
            await self._release(claim, reason="pending_query_failed")
            return
        if not pending:
            # Enqueue without input (possible race) — nothing to answer.
            fallback = (
                claim.input_seq
                if claim.input_seq is not None
                else (consumed_seq if consumed_seq is not None else 0)
            )
            await pa._stop_thread_control_watcher()
            await self._detach_physical_before_transition("no_pending_complete")
            state = await complete_unit(
                self._db,
                unit_id=claim.unit_id,
                lease_token=token,
                consumed_seq=fallback,
            )
            logger.info(
                "run_queue complete: unit=%s consumed_seq=%s state=%s "
                "(no-pending-input)",
                unit_id,
                fallback,
                state,
            )
            if state is None:
                await self._ack_terminal_claim_loss(claim)
            self._mark_warm(claim)
            return

        target = pending[0]

        # (g) Strip restored pending copies — only a fresh attach ran the
        # restore; a reused session's memory holds no unanswered copies
        # (inputs land in the DB via the orchestrator, never in this pod's
        # memory outside a claim).
        if fresh_attach and pa._session is not None:
            removed = strip_restored_pending_humans(pa._session.messages, pending)
            if removed:
                logger.info(
                    "stripped %d restored pending message(s) before injection "
                    "(unit=%s)",
                    removed,
                    unit_id,
                )

        if not target["content"]:
            if target.get("delivery_id") is not None:
                logger.error(
                    "durable event input is empty; refusing to consume it "
                    "without provider admission (unit=%s seq=%s)",
                    unit_id,
                    target["seq"],
                )
                await pa._stop_thread_control_watcher()
                await self._detach_cached_session("empty_event_input")
                await self._release(claim, reason="empty_event_input")
                return
            # An empty row can never produce a turn (the loop skips empty
            # input, and the completion hook would never fire) — consume it.
            await pa._stop_thread_control_watcher()
            await self._detach_physical_before_transition("empty_input_complete")
            state = await complete_unit(
                self._db,
                unit_id=claim.unit_id,
                lease_token=token,
                consumed_seq=target["seq"],
            )
            logger.info(
                "run_queue complete: unit=%s consumed_seq=%s state=%s "
                "(empty-input row)",
                unit_id,
                target["seq"],
                state,
            )
            if state is None:
                await self._ack_terminal_claim_loss(claim)
            self._mark_warm(claim)
            return

        target_turn_id = target.get("turn_number")
        if (
            isinstance(target_turn_id, bool)
            or not isinstance(target_turn_id, int)
            or target_turn_id <= 0
            or pa._session is None
        ):
            logger.error(
                "pending input lacks an exact durable turn identity; refusing "
                "injection (unit=%s seq=%s turn=%r)",
                unit_id,
                target.get("seq"),
                target_turn_id,
            )
            await pa._stop_thread_control_watcher()
            await self._detach_cached_session("pending_turn_identity_invalid")
            await self._release(claim, reason="pending_turn_identity_invalid")
            return

        expected_previous_turn = int(target_turn_id) - 1
        if fresh_attach:
            # Restore includes unanswered human rows and therefore seeds
            # turn_count to the newest pending row. We just stripped those
            # copies; rewind the in-process counter to the predecessor of the
            # OLDEST pending row so on_turn_start opens admission for the
            # turn_number already durable on that exact human row. This must
            # happen before queue injection: persist_message runs only after
            # on_turn_start and cannot repair a crash in between.
            pa._session.turn_count = expected_previous_turn
        elif int(pa._session.turn_count) != expected_previous_turn:
            logger.error(
                "warm pending turn identity diverged; refusing injection "
                "(unit=%s session_turn=%s target_turn=%d)",
                unit_id,
                pa._session.turn_count,
                target_turn_id,
            )
            await pa._stop_thread_control_watcher()
            await self._detach_cached_session("pending_turn_identity_mismatch")
            await self._release(claim, reason="pending_turn_identity_mismatch")
            return

        timing["pending"] = time.perf_counter() - t0

        # (h) Inject — the row already exists (accept-time persist is
        # orchestrator-side on this lane), so ONLY the queue put + loop
        # arming from _accept_user_input are reproduced here; its persist is
        # deliberately not. The id makes the loop's own turn-start persist an
        # idempotent upsert onto the same row.
        turn_done = asyncio.Event()
        pa._turn_start_external_hook = lambda turn_id: self._arm_interrupt_window(
            pa,
            claim,
            target_turn_id=turn_id,
        )
        pa._turn_complete_external_hook = lambda _turn_id: turn_done.set()
        pa._turn_tool_execution_external_hook = (
            lambda identity: self._record_claim_tool_effect(claim, identity)
        )
        if not pa._ensure_persistent_loop_started("stateless_claim"):
            await self._detach_cached_session("loop_not_ready")
            await self._release(claim, reason="loop_not_ready")
            return
        delivery_id = target.get("delivery_id")
        delivery_claim_generation: int | None = None
        if delivery_id is not None:
            try:
                claimed_delivery = await self._db.claim_stateless_input_delivery(
                    thread_id=unit_id,
                    delivery_id=str(delivery_id),
                    lease_token=token,
                    executor_id=self._pod_name,
                    pod_uid=self._pod_uid,
                )
            except Exception:
                logger.warning(
                    "stateless event-delivery claim failed for unit %s token=%d",
                    unit_id,
                    token,
                    exc_info=True,
                )
                await self._detach_cached_session("event_delivery_claim_failed")
                await self._release(claim, reason="event_delivery_claim_failed")
                return
            if (
                claimed_delivery is None
                or int(claimed_delivery.get("seq") or -1) != int(target["seq"])
                or str(claimed_delivery.get("message_id") or "") != str(target["id"])
            ):
                logger.warning(
                    "stateless event-delivery authority changed before injection "
                    "(unit=%s token=%d)",
                    unit_id,
                    token,
                )
                await self._detach_cached_session("event_delivery_claim_lost")
                await self._release(claim, reason="event_delivery_claim_lost")
                return
            delivery_claim_generation = int(claimed_delivery["claim_generation"])
        loop_task = pa._loop_task
        queue_item = {"content": target["content"], "id": target["id"]}
        if delivery_id is not None and delivery_claim_generation is not None:
            queue_item.update(
                {
                    "role": "event",
                    "delivery_id": str(delivery_id),
                    "claim_generation": delivery_claim_generation,
                }
            )
            if target.get("supersedes_input_seq") is not None:
                queue_item["supersedes_input_seq"] = int(target["supersedes_input_seq"])
        recovery_context = (
            getattr(pa._session, "tool_context", None)
            if pa._session is not None
            else None
        )
        is_subagent_recovery = target.get("supersedes_input_seq") is not None
        if recovery_context is not None:
            recovery_context._stateless_subagent_recovery_active = is_subagent_recovery
        try:
            await pa._loop_user_queue.put(queue_item)

            # (i) Wait for the full-turn settlement hook (event, not a poll),
            # the lease-lost signal, or the loop dying under us. PersistentApp
            # publishes it only after transcript persistence and Git push/turn-
            # ledger mapping, so detach cannot cancel a half-recorded workspace
            # turn.
            t0 = time.perf_counter()
            outcome = await self._await_turn(turn_done, loop_task)
            timing["turn"] = time.perf_counter() - t0
        finally:
            if recovery_context is not None:
                recovery_context._stateless_subagent_recovery_active = False

        if outcome == "turn_done":
            interrupt_turn_id = pa._interrupt_owner_turn_id
            if pa._interrupt_owner_lease_token != token or interrupt_turn_id is None:
                self._lease.mark_lost()
                logger.error(
                    "interrupt window identity missing at turn completion; "
                    "leaving lease to expire (unit=%s token=%d)",
                    unit_id,
                    token,
                )
                await self._detach_cached_session("interrupt_identity_missing")
                return
            tool_execution_started = self._claim_crossed_tool_effect(
                pa,
                claim=claim,
            )
            self._settled_claim = (str(claim.unit_id), int(token))
            # Commit-then-effects (stateless_turn_resilience.md step 4a): the
            # transcript, memory, Git mapping and workspace effects have
            # settled. The turn-end push only has to be STAGED (workspace read,
            # temp files written) before the workspace may be detached; its
            # transmit is handed its own fence below and continues off-slot.
            t0 = time.perf_counter()
            await self._await_cloud_push_staged(pa)
            timing["stage"] = time.perf_counter() - t0
            superseded_input_seq = target.get("supersedes_input_seq")
            completed_input_seq = max(
                int(
                    superseded_input_seq
                    if superseded_input_seq is not None
                    else target["seq"]
                ),
                int(claim.consumed_seq) if claim.consumed_seq is not None else -1,
            )
            self._pending_settled_close = (
                str(claim.unit_id),
                int(token),
                int(interrupt_turn_id),
                completed_input_seq,
            )
            try:
                interrupt_closed = await self._close_interrupt_window(
                    pa,
                    claim,
                    target_turn_id=int(interrupt_turn_id),
                    completed_input_seq=completed_input_seq,
                )
            except Exception:
                if tool_execution_started:
                    await self._quiesce_and_park_post_effect_claim(
                        pa,
                        claim,
                        reason=(
                            "turn_complete_interrupt_close_failed_after_tool_effect"
                        ),
                    )
                    return
                self._lease.mark_lost()
                logger.error(
                    "atomic input checkpoint/final interrupt drain failed after "
                    "turn completion; "
                    "leaving lease to expire (unit=%s token=%d)",
                    unit_id,
                    token,
                    exc_info=True,
                )
                await self._detach_cached_session("interrupt_final_drain_failed")
                return
            if not interrupt_closed:
                logger.warning(
                    "lease lost: unit=%s token=%d while closing interrupt "
                    "window — successor owns completion",
                    unit_id,
                    token,
                )
                if tool_execution_started:
                    await self._quiesce_and_park_post_effect_claim(
                        pa,
                        claim,
                        reason="turn_complete_after_tool_effect_lost_authority",
                    )
                else:
                    await self._detach_cached_session("interrupt_close_lost_lease")
                    await self._ack_terminal_claim_loss(claim)
                return
            # Close the owner-consumption window before completion. A control
            # committed after this point advances control_input_seq; the
            # completion statement observes it and requeues atomically.
            await pa._stop_thread_control_watcher()
            # Hand the pending push its own fence (push_owner_token) under the
            # still-live lease, BEFORE the slot is released: every write after
            # complete_unit is then checked against that token, never the
            # lease. If the hand-off cannot be made durable, fall back to the
            # pre-4a contract and wait the push out under the lease.
            t0 = time.perf_counter()
            await self._hand_off_cloud_push(pa, claim)
            timing["handoff"] = time.perf_counter() - t0
            # Snapshot the exact effect identity before a non-warm physical
            # detach may clear process-local session state. If the completion
            # CAS itself later exhausts retries, this determines whether the
            # input can safely auto-retry or must be parked.
            t0 = time.perf_counter()
            await self._detach_physical_before_transition("turn_complete")
            timing["detach_final"] = time.perf_counter() - t0
            t0 = time.perf_counter()
            state = await self._complete_with_retry(
                claim, consumed_seq=completed_input_seq
            )
            timing["complete"] = time.perf_counter() - t0
            if state is None:
                # Fenced out at completion: a steal beat us after the final
                # persist. The successor's claim decides what still needs
                # answering via the watermarks (skip-if-answered). Discard
                # the cached session — its in-memory tail may diverge from
                # what the successor persists.
                logger.warning(
                    "lease lost: unit=%s token=%d at completion (fenced out) — "
                    "successor decides via watermarks",
                    unit_id,
                    token,
                )
                await self._detach_cached_session("lease_lost_completion")
                await self._ack_terminal_claim_loss(claim)
                self._clear_claim_tool_effect(pa, claim)
                return
            if state == "error":
                # Close already checkpointed consumed_seq after settlement.
                # Even a turn with tool effects is safe to retry from that
                # boundary: its successor skips the durable answer. Never
                # park it merely because the final queue-state CAS failed.
                await self._release(claim, reason="completion_cas_failed")
                self.request_stop()
                return
            logger.info(
                "run_queue complete: unit=%s consumed_seq=%d state=%s",
                unit_id,
                completed_input_seq,
                state,
            )
            logger.info(
                "turn timing: unit=%s mode=%s bundle=%.2fs detach=%.2fs "
                "attach=%.2fs controls=%.2fs pending=%.2fs turn=%.2fs stage=%.2fs "
                "handoff=%.2fs detach_final=%.2fs complete=%.2fs total=%.2fs",
                unit_id,
                "reuse" if reuse else "fresh",
                timing.get("bundle", 0.0),
                timing.get("detach", 0.0),
                timing.get("attach", 0.0),
                timing.get("controls", 0.0),
                timing.get("pending", 0.0),
                timing.get("turn", 0.0),
                timing.get("stage", 0.0),
                timing.get("handoff", 0.0),
                timing.get("detach_final", 0.0),
                timing.get("complete", 0.0),
                sum(timing.values()),
            )
            if state != "error":
                self._clear_claim_tool_effect(pa, claim)
                self._mark_warm(claim)  # lite-only affinity + warm-TTL clock
        elif outcome == "lease_lost":
            # Polite fast path (§5.2): stop the turn the way the graceful
            # interrupt does; further fenced persists would only die at the
            # fence anyway. NO release_unit — the lease is not ours anymore.
            logger.warning(
                "lease lost: unit=%s token=%d — aborting turn politely "
                "(no release; no completion)",
                unit_id,
                token,
            )
            turn_id = self._owned_abort_target(pa)
            # Signal before interrupt-watcher close/drain: those are remote/DB
            # joins and must not delay the process-local abort edge.
            self._abort_turn_politely(pa, target_turn_id=turn_id)
            if turn_id is not None:
                await self._close_interrupt_window(
                    pa,
                    claim,
                    target_turn_id=int(turn_id),
                )
            else:
                await pa._stop_thread_interrupt_watcher()
            await pa._stop_thread_control_watcher()
            await self._wait_turn_unwind(turn_done, loop_task)
            # The aborted turn's in-memory tail may hold messages the fence
            # rejected — a later affinity reuse would diverge from the DB.
            # Discard; the next claim rebuilds from thread_messages (§5.2
            # torn-turn invariant).
            await self._detach_cached_session("lease_lost")
            # A fenced message/event/interrupt persist can signal the shared
            # LeaseHandle before the heartbeat observes the stolen row. ACK
            # directly after full unwind+detach; the exact marker matcher makes
            # this a no-op for ordinary reaper loss.
            await self._ack_terminal_claim_loss(claim)
        else:  # loop_died
            tool_execution_started = self._claim_crossed_tool_effect(
                pa,
                claim=claim,
            )
            disposition = "parking" if tool_execution_started else "releasing"
            logger.warning(
                "persistent loop ended mid-turn for unit %s (%s) — %s",
                unit_id,
                outcome,
                disposition,
            )
            turn_id = (
                pa._interrupt_owner_turn_id
                if pa._interrupt_owner_lease_token == token
                else None
            )
            if turn_id is not None:
                try:
                    interrupt_closed = await self._close_interrupt_window(
                        pa,
                        claim,
                        target_turn_id=int(turn_id),
                    )
                except Exception:
                    if tool_execution_started:
                        await self._quiesce_and_park_post_effect_claim(
                            pa,
                            claim,
                            reason="loop_died_after_tool_effect_interrupt_close_failed",
                        )
                        return
                    self._lease.mark_lost()
                    logger.error(
                        "interrupt final drain failed after loop death; "
                        "leaving lease to expire (unit=%s token=%d)",
                        unit_id,
                        token,
                        exc_info=True,
                    )
                    await self._detach_cached_session(
                        "loop_died_interrupt_drain_failed"
                    )
                    return
                if not interrupt_closed:
                    if tool_execution_started:
                        await self._quiesce_and_park_post_effect_claim(
                            pa,
                            claim,
                            reason="loop_died_after_tool_effect_lost_authority",
                        )
                    else:
                        await self._detach_cached_session("loop_died_lost_lease")
                    return
            if tool_execution_started:
                await self._quiesce_and_park_post_effect_claim(
                    pa,
                    claim,
                    reason="loop_died_after_tool_effect",
                )
                return
            await self._release(claim, reason="loop_died")

    # ------------------------------------------------------------------
    # Pieces
    # ------------------------------------------------------------------

    async def _arm_interrupt_window(
        self,
        pa: Any,
        claim: ClaimedUnit,
        *,
        target_turn_id: int,
    ) -> None:
        """Arm the consumer, then publish exact-turn admission.

        The watcher starts before the public gate opens. A synchronous drain
        after opening closes the LISTEN-registration window; only then may the
        persistent loop emit ``turn.started``.
        """

        token = claim.lease_token
        opened = False
        try:
            await pa._start_thread_interrupt_watcher(
                lease_token=token,
                target_turn_id=int(target_turn_id),
            )
            opened = await open_interrupt_admission(
                self._db,
                unit_id=claim.unit_id,
                lease_token=token,
                turn_id=int(target_turn_id),
            )
            if not opened:
                self._lease.mark_lost()
                raise LeaseLostError(
                    "interrupt admission rejected stale lease/turn: "
                    f"{claim.unit_id}/{token}/{target_turn_id}"
                )
            await pa._drain_thread_interrupts(
                lease_token=token,
                target_turn_id=int(target_turn_id),
            )
        except BaseException:
            closed = False
            if opened:
                try:
                    closed = await close_interrupt_admission(
                        self._db,
                        unit_id=claim.unit_id,
                        lease_token=token,
                        turn_id=int(target_turn_id),
                    )
                except BaseException:
                    self._lease.mark_lost()
            try:
                await pa._stop_thread_interrupt_watcher()
            except BaseException:
                # A consumer that cannot be joined must never survive a queue
                # transition, even when the public gate did not open.
                self._lease.mark_lost()
            if opened and closed:
                try:
                    await pa._drain_thread_interrupts(
                        lease_token=token,
                        target_turn_id=int(target_turn_id),
                    )
                except BaseException:
                    # Never release a queue row after closing a window whose
                    # committed admission tail could not be settled.
                    self._lease.mark_lost()
            elif opened:
                self._lease.mark_lost()
            raise

    async def _close_interrupt_window(
        self,
        pa: Any,
        claim: ClaimedUnit,
        *,
        target_turn_id: int,
        completed_input_seq: int | None = None,
    ) -> bool:
        """Close admission, optionally checkpoint, then drain committed tail."""

        token = claim.lease_token
        pending_close = self._pending_settled_close
        if (
            completed_input_seq is None
            and pending_close is not None
            and pending_close[:3]
            == (str(claim.unit_id), int(token), int(target_turn_id))
        ):
            completed_input_seq = pending_close[3]
        attempts = 0
        try:
            while True:
                try:
                    closed = await close_interrupt_admission(
                        self._db,
                        unit_id=claim.unit_id,
                        lease_token=token,
                        turn_id=int(target_turn_id),
                        completed_input_seq=completed_input_seq,
                    )
                    break
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if completed_input_seq is None:
                        raise
                    attempts += 1
                    logger.warning(
                        "atomic completed-input checkpoint failed; retrying "
                        "under live lease (unit=%s token=%d turn=%d attempt=%d)",
                        claim.unit_id,
                        token,
                        target_turn_id,
                        attempts,
                        exc_info=attempts <= COMPLETE_RETRY_ATTEMPTS,
                    )
                    # A transport exception may be an ambiguous commit. The
                    # SQL's already-consumed arm makes every retry idempotent;
                    # do not detach or expose a successor merely because a
                    # finite retry budget elapsed while heartbeat still owns
                    # this row.
                    await asyncio.sleep(0.5 * min(attempts, COMPLETE_RETRY_ATTEMPTS))
        except BaseException:
            self._lease.mark_lost()
            with contextlib.suppress(BaseException):
                await pa._stop_thread_interrupt_watcher()
            raise
        if pending_close is not None and pending_close[:3] == (
            str(claim.unit_id),
            int(token),
            int(target_turn_id),
        ):
            self._pending_settled_close = None
        await pa._stop_thread_interrupt_watcher()
        if not closed:
            self._lease.mark_lost()
            return False
        try:
            await pa._drain_thread_interrupts(
                lease_token=token,
                target_turn_id=int(target_turn_id),
            )
        except BaseException:
            self._lease.mark_lost()
            raise
        return True

    async def _transcript_answered_seq(self, claim: ClaimedUnit) -> Optional[int]:
        """Seq of the oldest pending input the transcript already answers, or None.

        Read-only and fail-open to None: a query failure means "not proven
        answered", and the ordinary claim path decides. See
        ``_ANSWERED_BY_TRANSCRIPT_SQL``.
        """
        fetchval = getattr(self._db, "fetchval", None)
        if fetchval is None or claim.input_seq is None:
            return None
        try:
            value = await fetchval(
                _ANSWERED_BY_TRANSCRIPT_SQL,
                claim.unit_id,
                claim.consumed_seq if claim.consumed_seq is not None else -1,
                int(claim.input_seq),
            )
        except Exception:
            logger.warning(
                "transcript answered-check failed for unit %s; treating as unanswered",
                claim.unit_id,
                exc_info=True,
            )
            return None
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    async def _fetch_bundle(self, unit_id: str, token: int) -> Dict[str, Any]:
        client = _pa()._orchestrator_client
        if client is None:
            raise RuntimeError("no orchestrator client — cannot fetch the claim bundle")
        return await client.get_claim_bundle(unit_id, token)

    async def _fetch_pending_rows(
        self, thread_id: str, consumed_seq: Optional[int]
    ) -> List[Dict[str, Any]]:
        rows = await self._db.fetch(
            _PENDING_INPUT_SQL,
            thread_id,
            consumed_seq if consumed_seq is not None else -1,
            PENDING_ROWS_LIMIT,
        )
        return [
            {
                "id": str(r["id"]),
                "seq": r["seq"],
                "content": r["content"] or "",
                "turn_number": r["turn_number"],
                "role": str(r.get("role") or "human"),
                "delivery_id": (
                    str(r.get("delivery_id"))
                    if r.get("delivery_id") is not None
                    else None
                ),
                "supersedes_input_seq": (
                    int(r.get("supersedes_input_seq"))
                    if r.get("supersedes_input_seq") is not None
                    else None
                ),
            }
            for r in rows
        ]

    async def _heartbeat_loop(
        self, claim: ClaimedUnit, claim_lost: asyncio.Event | None = None
    ) -> None:
        """Renew every HEARTBEAT_INTERVAL_SECONDS; on a lost lease, signal the
        driver via the shared handle. Independent of the graph loop by
        construction (its own task — a long tool call cannot starve it)."""
        unit_id = claim.unit_id
        token = claim.lease_token
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
            try:
                renewed = await heartbeat_unit(
                    self._db, unit_id=unit_id, lease_token=token
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Transient DB trouble: keep trying — the lease has TTL/3
                # slack, and if the DB stays down the reaper takes over.
                logger.warning(
                    "lease heartbeat failed for unit %s (transient): %s",
                    unit_id,
                    e,
                )
                continue
            if renewed is None:
                logger.warning(
                    "lease lost: unit=%s token=%d (heartbeat renewal found no "
                    "leased row)",
                    unit_id,
                    token,
                )
                if claim_lost is not None:
                    claim_lost.set()
                # Do not poison a previous warm claim's handle while its
                # teardown still runs. Once this identity is installed, signal
                # both the shared writers and _await_turn directly.
                if self._lease.unit_id == str(
                    unit_id
                ) and self._lease.lease_token == int(token):
                    self._lease.mark_lost()
                return

    async def _await_turn(
        self, turn_done: asyncio.Event, loop_task: Optional[asyncio.Task]
    ) -> str:
        """First of: turn completed | lease lost | loop died. Never cancels
        the loop task itself."""
        lost_event = self._lease.lost
        done_waiter = asyncio.create_task(turn_done.wait())
        lost_waiter = asyncio.create_task(lost_event.wait())
        waiters = {done_waiter, lost_waiter}
        if loop_task is not None:
            waiters.add(loop_task)
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in (done_waiter, lost_waiter):
                if not waiter.done():
                    waiter.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await waiter
        # Lease loss wins a simultaneous turn close. Besides a rejected DB
        # renewal this event represents shutdown's intentional abandonment of
        # a pre-effect turn; letting turn_done win there would complete the
        # still-unanswered input under a DB lease that remains valid until TTL.
        if lost_event.is_set():
            return "lease_lost"
        if turn_done.is_set():
            return "turn_done"
        return "loop_died"

    def _activate_lease(self, unit_id: Any, lease_token: int) -> None:
        """Install a new exact claimant and retire prior effect evidence."""

        self._tool_effect_identity = None
        self._lease.update(
            unit_id,
            lease_token,
            executor_id=self._pod_name,
            pod_uid=self._pod_uid,
        )

    def _record_claim_tool_effect(
        self,
        claim: ClaimedUnit,
        identity: tuple[str, int, int],
    ) -> None:
        """Synchronously copy PA's exact pre-invoke identity into the owner."""

        expected = (str(claim.unit_id), int(claim.lease_token))
        if (
            not isinstance(identity, tuple)
            or len(identity) != 3
            or identity[:2] != expected
            or isinstance(identity[2], bool)
            or not isinstance(identity[2], int)
            or identity[2] <= 0
            or self._lease.unit_id != expected[0]
            or self._lease.lease_token != expected[1]
            or self._lease.lost.is_set()
        ):
            self._lease.mark_lost()
            raise RuntimeError(
                "tool execution boundary does not match the active queue claim"
            )
        current = self._tool_effect_identity
        if current is not None and current != identity:
            # One queue claim is one durable input turn. Keep the first effect
            # monotonic and fail rather than silently relabeling it.
            self._lease.mark_lost()
            raise RuntimeError("queue claim crossed multiple tool-turn identities")
        self._tool_effect_identity = identity

    def _owned_abort_target(self, pa: Any) -> Optional[int]:
        """Return the exact active turn owned by this executor claim."""

        unit_id = self._lease.unit_id
        lease_token = self._lease.lease_token
        target_turn_id = getattr(pa, "_interrupt_owner_turn_id", None)
        if (
            not unit_id
            or str(getattr(pa, "_thread_id", "") or "") != str(unit_id)
            or getattr(pa, "_interrupt_owner_lease_token", None) != lease_token
            or isinstance(target_turn_id, bool)
            or not isinstance(target_turn_id, int)
            or target_turn_id <= 0
        ):
            return None
        return target_turn_id

    def _claim_crossed_tool_effect(
        self,
        pa: Any,
        *,
        claim: Optional[ClaimedUnit] = None,
    ) -> bool:
        """Read the exact claim-scoped external-effect identity.

        This deliberately does not depend on interrupt-watcher globals: those
        close at the turn-settled hook, before the run_queue completion CAS.
        """

        unit_id = str(claim.unit_id) if claim is not None else self._lease.unit_id
        lease_token = (
            int(claim.lease_token)
            if claim is not None
            else int(self._lease.lease_token)
        )
        if not unit_id or lease_token <= 0:
            return False
        local_identity = self._tool_effect_identity
        if local_identity is not None and local_identity[:2] == (
            str(unit_id),
            lease_token,
        ):
            return True
        try:
            identity = pa._turn_tool_execution_identity_for_claim(
                thread_id=str(unit_id),
                lease_token=lease_token,
            )
            if identity is None:
                return False
            # Copy before any caller can physically detach PersistentApp and
            # erase its diagnostic global.
            self._tool_effect_identity = identity
            return True
        except Exception:
            # Classification is a safety boundary. Never downgrade a broken
            # exact-identity reader to the pre-effect auto-retry path.
            logger.exception(
                "tool-effect identity lookup failed closed (unit=%s token=%d)",
                unit_id,
                lease_token,
            )
            return True

    def _clear_claim_tool_effect(self, pa: Any, claim: ClaimedUnit) -> None:
        """Forget only the identity whose queue disposition is now durable."""

        expected = (str(claim.unit_id), int(claim.lease_token))
        if (
            self._tool_effect_identity is not None
            and self._tool_effect_identity[:2] == expected
        ):
            self._tool_effect_identity = None

        try:
            pa._clear_turn_tool_execution_identity(
                thread_id=str(claim.unit_id),
                lease_token=int(claim.lease_token),
            )
        except Exception:
            # A stale marker can only make a later shutdown more conservative
            # because all lookups are exact-token scoped. Do not turn a durable
            # queue disposition into an executor failure for local cleanup.
            logger.warning(
                "failed to clear local tool-effect identity (unit=%s token=%d)",
                claim.unit_id,
                claim.lease_token,
                exc_info=True,
            )

    async def _quiesce_and_park_post_effect_claim(
        self,
        pa: Any,
        claim: ClaimedUnit,
        *,
        reason: str,
    ) -> Optional[str]:
        """Retire every physical writer, then park an ambiguous effect claim."""

        try:
            await self._quiesce_claim_before_transition(
                pa,
                reason=f"park_{reason}",
                claim=claim,
            )
        except _ClaimQuiescenceError as exc:
            raise _PostEffectParkError(
                "post-effect claimant could not be fully quiesced before park"
            ) from exc
        return await self._park_post_effect_claim(
            pa,
            claim,
            reason=reason,
        )

    async def _quiesce_claim_before_transition(
        self,
        pa: Any,
        *,
        reason: str,
        claim: Optional[ClaimedUnit] = None,
    ) -> None:
        """Retire all warm/physical consumers before a queue state change.

        A pending turn-end push is no longer awaited here (step 4a): with the
        exact claim known it is handed its own fence and continues off-slot;
        without one (legacy callers) the pre-4a wait applies.
        """

        try:
            if claim is not None:
                await self._hand_off_cloud_push(pa, claim)
            else:
                # No queued successor may observe the claim while a turn-end
                # cloud writer or cached persistent loop can still mutate state.
                await self._await_cloud_push(pa)
            if (
                pa._pending_cloud_push_task is not None
                and pa._pending_cloud_push_task.done()
            ):
                pa._pending_cloud_push_task = None
            await self._detach_cached_session(reason)
        except BaseException as exc:
            raise _ClaimQuiescenceError(
                "claimant could not be fully quiesced before queue transition"
            ) from exc

    async def _hand_off_cloud_push(self, pa: Any, claim: ClaimedUnit) -> bool:
        """Give this claim's pending push its own fence; True when handed off.

        Falls back to the pre-4a contract (wait the push out under the lease)
        when the hand-off cannot be made durable, so completion never leaves an
        unfenced external writer behind.
        """

        task = getattr(pa, "_pending_cloud_push_task", None)
        if task is None:
            return False
        hand_off = getattr(pa, "_hand_off_cloud_push", None)
        if hand_off is None:
            await self._await_cloud_push(pa)
            return False
        try:
            handed = await hand_off(
                self._db,
                thread_id=str(claim.unit_id),
                lease_token=int(claim.lease_token),
                pod_name=self._pod_name,
                pod_uid=self._pod_uid,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "cloud push hand-off failed for unit %s token=%d; waiting the "
                "push out under the lease instead",
                claim.unit_id,
                claim.lease_token,
                exc_info=True,
            )
            await self._await_cloud_push(pa)
            return False
        if not handed:
            await self._await_cloud_push(pa)
        return bool(handed)

    async def _await_cloud_push_staged(self, pa: Any) -> None:
        """Wait until the turn-end push has read the workspace (or ended)."""

        task = pa._pending_cloud_push_task
        if task is None:
            return
        staged = getattr(pa, "_pending_cloud_push_staged", None)
        waiters: set = {asyncio.ensure_future(asyncio.shield(task))}
        if staged is not None:
            waiters.add(asyncio.create_task(staged.wait()))
        started = time.monotonic()
        try:
            await asyncio.wait(
                waiters,
                timeout=CLOUD_PUSH_STAGE_WAIT_SECONDS,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for waiter in waiters:
                if not waiter.done():
                    waiter.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await waiter
        if task.done() and not task.cancelled() and task.exception() is not None:
            logger.warning(
                "turn-end cloud push failed before hand-off; durable generation "
                "remains pending for successor recovery",
                exc_info=task.exception(),
            )
        logger.info(
            "turn-end cloud push staged in %.1fs (transmit continues off-slot)",
            time.monotonic() - started,
        )

    async def _tool_effects_durable(self, claim: ClaimedUnit) -> bool:
        """Every persisted tool call of the pending turn has its result row."""

        fetchval = getattr(self._db, "fetchval", None)
        if fetchval is None:
            return False
        try:
            value = await fetchval(
                _TOOL_EFFECTS_DURABLE_SQL,
                claim.unit_id,
                claim.consumed_seq if claim.consumed_seq is not None else -1,
            )
        except Exception:
            logger.warning(
                "tool-effect durability check failed for unit %s; parking",
                claim.unit_id,
                exc_info=True,
            )
            return False
        return bool(value)

    async def _park_post_effect_claim(
        self,
        pa: Any,
        claim: ClaimedUnit,
        *,
        reason: str,
    ) -> Optional[str]:
        """CAS an already-quiesced post-effect claim to manual recovery."""

        last_error: BaseException | None = None
        park_reason = _park_reason_for(reason)
        for attempt in range(1, COMPLETE_RETRY_ATTEMPTS + 1):
            try:
                state = await park_unit(
                    self._db,
                    unit_id=claim.unit_id,
                    lease_token=claim.lease_token,
                    reason=park_reason,
                )
                if state is None:
                    logger.critical(
                        "post-effect park lost exact queue authority: "
                        "unit=%s token=%d reason=%s; successor disposition "
                        "requires reconciliation",
                        claim.unit_id,
                        claim.lease_token,
                        reason,
                    )
                    await self._ack_terminal_claim_loss(claim)
                    self._clear_claim_tool_effect(pa, claim)
                    return None
                self._clear_claim_tool_effect(pa, claim)
                logger.critical(
                    "run_queue parked after post-effect claim could not "
                    "complete: unit=%s token=%d reason=%s park_reason=%s "
                    "state=%s; reconciliation and explicit unpark required",
                    claim.unit_id,
                    claim.lease_token,
                    reason,
                    park_reason,
                    state,
                )
                await self._journal_parked(
                    claim,
                    reason=park_reason,
                    error=None,
                    attempts=claim.attempts_since_completion,
                )
                return state
            except asyncio.CancelledError:
                # Cancellation cleanup must reach a durable queue disposition;
                # retry it just like the release path below.
                continue
            except BaseException as exc:
                last_error = exc
                if attempt < COMPLETE_RETRY_ATTEMPTS:
                    await asyncio.sleep(0.5 * attempt)
        raise _PostEffectParkError(
            "post-effect claim could not be parked without enabling replay"
        ) from last_error

    def _abort_turn_politely(
        self,
        pa: Any,
        *,
        target_turn_id: Optional[int],
        force_graceful: bool = False,
    ) -> bool:
        """Interrupt the loop the way the interrupt verb does: graceful while
        a tool call is mid-invoke, hard otherwise (cancels a blocked LLM
        stream immediately).

        Stateless lease loss may race the end of a turn. Never synthesize an
        unscoped flag: the persistent-loop helper atomically verifies that the
        exact durable turn is still active before mutating interrupt state.
        """
        if (
            isinstance(target_turn_id, bool)
            or not isinstance(target_turn_id, int)
            or target_turn_id <= 0
        ):
            logger.warning(
                "refusing unscoped stateless turn abort: no exact active turn"
            )
            return False
        try:
            mode = pa._signal_interrupt_for_turn(
                target_turn_id,
                force_graceful=force_graceful,
            )
        except Exception:
            logger.warning("failed to signal turn abort", exc_info=True)
            return False
        if mode is None:
            logger.warning(
                "refusing stale stateless turn abort: target_turn=%d is no "
                "longer active",
                target_turn_id,
            )
            return False
        logger.info(
            "turn abort requested (mode=%s target_turn=%d)",
            mode,
            target_turn_id,
        )
        return True

    async def _wait_turn_unwind(
        self, turn_done: asyncio.Event, loop_task: Optional[asyncio.Task]
    ) -> None:
        """Bounded wait for the interrupted turn to unwind. On timeout the
        detach path's loop-task cancel finishes the job."""
        done_waiter = asyncio.create_task(turn_done.wait())
        waiters: set = {done_waiter}
        if loop_task is not None:
            waiters.add(loop_task)
        try:
            await asyncio.wait(
                waiters,
                timeout=self._abort_grace_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            if not done_waiter.done():
                done_waiter.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await done_waiter

    async def _await_cloud_push(self, pa: Any) -> None:
        """Keep the lease until the turn-end push reaches a terminal outcome.

        A live task after ``complete_unit`` has no durable ownership and can
        race the next claimant's recovery/pull. The DB generation stays
        pending when a finished task reports failure, so the successor can
        safely replay; there is no safe equivalent for a still-running PUT.
        """
        task = pa._pending_cloud_push_task
        if task is None:
            return
        started = time.monotonic()
        try:
            await task
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "turn-end cloud push reached failure under the lease; "
                "durable generation remains pending for successor recovery",
                exc_info=True,
            )
        logger.info(
            "turn-end cloud push reached a terminal outcome in %.1fs under the lease",
            time.monotonic() - started,
        )

    async def _complete_with_retry(
        self, claim: ClaimedUnit, *, consumed_seq: int
    ) -> Optional[str]:
        """complete_unit with bounded retries. Returns the resulting state,
        None when fenced out, or the sentinel 'error' after exhausted retries.

        NEVER falls back to release_unit here: the answer is already persisted
        and the preceding atomic interrupt close already checkpointed
        ``consumed_seq``. A release would publish that already-settled
        answer/effect for another pod despite the watermark. Leaving the queue
        state leased is the lesser evil — loud in the log and bounded by the
        reaper (§5.2 torn-turn invariant)."""
        last_exc: Optional[BaseException] = None
        for attempt in range(1, COMPLETE_RETRY_ATTEMPTS + 1):
            try:
                return await complete_unit(
                    self._db,
                    unit_id=claim.unit_id,
                    lease_token=claim.lease_token,
                    consumed_seq=consumed_seq,
                )
            except Exception as e:
                last_exc = e
                if attempt < COMPLETE_RETRY_ATTEMPTS:
                    await asyncio.sleep(0.5 * attempt)
        logger.error(
            "run_queue complete FAILED after %d attempts for unit %s "
            "(consumed_seq=%s) — leaving the lease to expire: %s",
            COMPLETE_RETRY_ATTEMPTS,
            claim.unit_id,
            consumed_seq,
            last_exc,
        )
        return "error"

    async def _settle_release(
        self,
        claim: ClaimedUnit,
        *,
        cas: Callable[[Any], Awaitable[Optional[str]]],
        park_reason: str,
        release_reason: str,
        error: Optional[str] = None,
    ) -> Optional[_ReleaseDisposition]:
        """:func:`_settle_session_release` with bounded retries.

        A lost commit response replays as this claim's own disposition, so a
        retry can neither journal twice nor abandon an already-settled lease
        to the reaper (whose claimant-loss hold would then need Pod proof).
        Raises the last error once every attempt failed; the lease is then
        still exactly this claim's and expires instead.
        """

        last_error: Optional[Exception] = None
        for attempt in range(1, COMPLETE_RETRY_ATTEMPTS + 1):
            try:
                return await _settle_session_release(
                    self._db,
                    claim,
                    cas=cas,
                    pod_name=self._pod_name,
                    pod_uid=self._pod_uid,
                    park_reason=park_reason,
                    release_reason=release_reason,
                    error=error,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt < COMPLETE_RETRY_ATTEMPTS:
                    await asyncio.sleep(0.5 * attempt)
        assert last_error is not None
        raise last_error

    async def _release(self, claim: ClaimedUnit, *, reason: str) -> None:
        """Voluntary error release (§5.1): default linear backoff, token-
        guarded (a genuinely lost lease makes this a recorded no-op).

        Every caller is pre-effect (post-effect failures park fail-closed).
        A deterministic reason (``_release_is_deterministic``) is counted
        against the row's retry budget: the failure that reaches
        ``max_attempts`` parks as ``retry_exhausted`` — owner-retryable — and
        journals ``turn.parked`` in the same commit, instead of re-queueing
        forever. A transient reason never parks and backs off exponentially
        (capped), so an orchestrator or DB outage heals by itself."""
        pa = _pa()
        # Warm affinity is valid only after successful completion. Every
        # leased->queued error transition retires the cached loop/session so a
        # successor can never overlap this claimant's consumer or writers.
        await self._quiesce_claim_before_transition(
            pa,
            reason=f"release_{reason}",
            claim=claim,
        )
        if self._lease.lost.is_set():
            logger.info(
                "run_queue release: unit=%s token=%d reason=%s "
                "skipped after local ownership loss",
                claim.unit_id,
                claim.lease_token,
                reason,
            )
            if self._exact_claim_handle_lost(claim):
                await self._ack_terminal_claim_loss(claim)
            return

        deterministic = _release_is_deterministic(reason)

        async def error_release(conn: Any) -> Optional[str]:
            if deterministic:
                return await release_unit(
                    conn,
                    unit_id=claim.unit_id,
                    lease_token=claim.lease_token,
                    error=True,
                    park_reason=PARK_REASON_RETRY_EXHAUSTED,
                    last_error=reason,
                )
            return await release_unit(
                conn,
                unit_id=claim.unit_id,
                lease_token=claim.lease_token,
                backoff_seconds=transient_release_backoff_seconds(
                    claim.attempts_since_completion
                ),
                error=True,
            )

        try:
            outcome = await self._settle_release(
                claim,
                cas=error_release,
                park_reason=PARK_REASON_RETRY_EXHAUSTED,
                release_reason=reason,
            )
        except Exception:
            logger.warning(
                "run_queue release failed for unit %s (reason=%s) — the lease "
                "will expire instead",
                claim.unit_id,
                reason,
                exc_info=True,
            )
            return
        if outcome is None:
            logger.info(
                "run_queue release: unit=%s token=%d reason=%s "
                "(already fenced out — nothing to release)",
                claim.unit_id,
                claim.lease_token,
                reason,
            )
            await self._ack_terminal_claim_loss(claim)
            self._clear_claim_tool_effect(pa, claim)
        else:
            log = logger.warning if outcome.state == STATE_PARKED else logger.info
            log(
                "run_queue release: unit=%s token=%d reason=%s (%s) state=%s "
                "attempts=%d%s",
                claim.unit_id,
                claim.lease_token,
                reason,
                "deterministic" if deterministic else "transient",
                outcome.state,
                outcome.attempts,
                " (retry budget exhausted)" if outcome.state == STATE_PARKED else "",
            )
            self._clear_claim_tool_effect(pa, claim)

    async def _release_attach_failure(
        self, claim: ClaimedUnit, exc: BaseException
    ) -> None:
        """Attach failed: count it, back off, park when bounded, tell the user.

        Replaces the plain error release for the attach path
        (stateless_turn_resilience.md step 2). The claim already counted this
        attempt; ``record_attach_failure`` records the failure signature and
        either re-queues with a growing backoff or parks with
        ``park_reason='attach_failed'`` — and a park commits with its
        ``turn.parked`` frame so a queued message never turns into silence.
        """
        pa = _pa()
        await self._quiesce_claim_before_transition(
            pa,
            reason="release_attach_failed",
            claim=claim,
        )
        if self._lease.lost.is_set():
            logger.info(
                "run_queue release: unit=%s token=%d reason=attach_failed "
                "skipped after local ownership loss",
                claim.unit_id,
                claim.lease_token,
            )
            if self._exact_claim_handle_lost(claim):
                await self._ack_terminal_claim_loss(claim)
            return
        signature = _error_signature(exc)
        error_text = str(exc)[:_LAST_ERROR_CHARS]
        backoff = attach_failure_backoff_seconds(claim.attempts_since_completion)
        recorded: dict[str, Any] = {}

        async def attach_failure_release(conn: Any) -> Optional[str]:
            row = await record_attach_failure(
                conn,
                unit_id=claim.unit_id,
                lease_token=claim.lease_token,
                error=error_text,
                signature=signature,
                backoff_seconds=backoff,
            )
            recorded.clear()
            if row is None:
                return None
            recorded.update(row)
            return str(row.get("state") or "")

        try:
            outcome = await self._settle_release(
                claim,
                cas=attach_failure_release,
                park_reason=PARK_REASON_ATTACH_FAILED,
                release_reason="attach_failed",
                error=error_text,
            )
        except Exception:
            logger.warning(
                "run_queue attach-failure release failed for unit %s — the "
                "lease will expire instead",
                claim.unit_id,
                exc_info=True,
            )
            return
        if outcome is None:
            logger.info(
                "run_queue release: unit=%s token=%d reason=attach_failed "
                "(already fenced out — nothing to release)",
                claim.unit_id,
                claim.lease_token,
            )
            await self._ack_terminal_claim_loss(claim)
            self._clear_claim_tool_effect(pa, claim)
            return
        self._clear_claim_tool_effect(pa, claim)
        logger.warning(
            "run_queue release: unit=%s token=%d reason=attach_failed state=%s "
            "attempts=%d attach_failures=%s backoff=%.0fs signature=%s",
            claim.unit_id,
            claim.lease_token,
            outcome.state,
            outcome.attempts,
            recorded.get("attach_failures", "?"),
            backoff,
            signature,
        )

    async def _journal_parked(
        self,
        claim: ClaimedUnit,
        *,
        reason: str,
        error: Optional[str],
        attempts: int,
    ) -> None:
        """Append the ``turn.parked`` system frame for a park this pod made.

        Post-park is a sanctioned system-writer context
        (``shared.event_journal.append_system_frame``): the local journal
        writer was detached by the quiescence that precedes every park. Only
        session units have a journal; a failure to write it is logged, never
        raised — the queue disposition is already durable.
        """
        if str(claim.unit_kind) != UNIT_KIND_SESSION_TURN:
            return
        payload: dict[str, Any] = {
            "reason": reason,
            "attempts": int(attempts),
            "retryable": reason in RETRYABLE_PARK_REASONS,
            "parked_by": self._pod_name,
        }
        if error:
            payload["error"] = error[: _ERROR_SIGNATURE_MESSAGE_CHARS * 4]
        try:
            await append_system_frame(
                self._db,
                thread_id=str(claim.unit_id),
                kind="turn.parked",
                payload=payload,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "turn.parked journal frame failed for unit %s (reason=%s)",
                claim.unit_id,
                reason,
                exc_info=True,
            )

    async def _detach_physical_before_transition(self, reason: str) -> None:
        """Retire physical owner state while this claim is still exclusive."""
        session = _pa()._session
        if session is None or session.stateless_warm_reuse_safe:
            return
        await self._detach_cached_session(reason)

    def _exact_claim_handle_lost(self, claim: ClaimedUnit) -> bool:
        """Bind the shared mutable loss event to the claim being unwound."""

        return bool(
            self._lease.unit_id == str(claim.unit_id)
            and self._lease.lease_token == int(claim.lease_token)
            and self._lease.lost.is_set()
        )

    async def _ack_terminal_claim_loss(
        self,
        claim: ClaimedUnit,
    ) -> bool:
        """Acknowledge public-End fencing only after local I/O is quiescent.

        Reaper steals and ordinary lifecycle races reach this method too; the
        shared DB helper returns False unless an exact End marker names this
        old token and pod.  Physical sessions are detached once more as an
        idempotent belt so attach/bundle loss paths receive the same guarantee
        as the normal mid-turn unwind path.
        """

        pa = _pa()
        if pa._thread_id == str(claim.unit_id) and pa._session is not None:
            await self._detach_cached_session("terminal_claim_fenced")
        try:
            acknowledged = await acknowledge_session_claim_quiesced(
                self._db,
                thread_id=claim.unit_id,
                previous_lease_token=claim.lease_token,
                leased_by=self._pod_name,
                pod_uid=self._pod_uid,
            )
        except Exception:
            logger.warning(
                "terminal claimant-quiescence acknowledgement failed: "
                "unit=%s token=%d pod=%s",
                claim.unit_id,
                claim.lease_token,
                self._pod_name,
                exc_info=True,
            )
            return False
        if acknowledged:
            logger.info(
                "terminal claimant quiesced: unit=%s token=%d pod=%s",
                claim.unit_id,
                claim.lease_token,
                self._pod_name,
            )
        return bool(acknowledged)

    async def _detach_cached_session(self, reason: str) -> None:
        """Drop the cached session (and the affinity that pointed at it).

        Uses the same teardown /session/detach uses (_terminate_session) but
        NEVER marks the thread — on the stateless lane thread lifecycle is
        orchestrator-owned, and a pod-side 'ended' would force an epoch bump
        (client cache-wipe cascade) on the next claim's attach."""
        pa = _pa()
        self._attached_fingerprint = None
        self._attached_bundle = None
        self._prefer_unit_id = None
        self._warm_since = None
        pa._turn_complete_external_hook = None
        pa._turn_start_external_hook = None
        pa._turn_tool_execution_external_hook = None
        if pa._session is None:
            # A failed attach can leave _thread_id set with no session
            # (dual_app precedent) — clear it so the next claim starts clean.
            pa._thread_id = None
            return
        # Queue-claim detach retires only this Python/SFTP owner. Workspace
        # rclone/overlay residents are durable handoff state and may still be
        # flushing VFS bytes; destroying them on every claim boundary can lose
        # writeback. Public End owns a separate exact remote resident-retirement
        # acknowledgement before any emptyDir snapshot.
        preserve_workspace_daemons = True
        await pa._terminate_session(
            reason,
            mark_thread=False,
            preserve_shell=True,
            preserve_workspace_daemons=preserve_workspace_daemons,
        )
        if pa._session is not None:
            raise RuntimeError(
                "physical stateless session detach did not retire its owner"
            )

    def _scrub_process_residue(self) -> None:
        """§5.6 scrub-on-claim, executor half (deliverable D).

        _terminate_session already clears the session-scoped state (loop
        primitives, canvas awareness, subscribers, journal cursor, writer,
        ToolContext via session.cleanup). What it does NOT touch is the
        env/singleton-shaped process residue — exactly what a warm pod leaks
        across sequential tenants:

        * memory-embedding env keys + singleton, KB profile keys + singleton
          (also scrubbed pop-first inside _attach_session's
          _apply_session_embedding_env — this claim-time pass covers claims
          whose attach then fails before reaching that block);
        * the dual-mode guidance/reply inboxes (worker-plane; a stateless pod
          never runs jobs, but they are process-global dicts, so clear them).
        """
        try:
            pa = _pa()
            pa._apply_session_embedding_env(None)
        except Exception:
            logger.warning("embedding scrub failed (non-fatal)", exc_info=True)
        try:
            import agent.api.dual_app as dual_app

            dual_app._guidance_inbox.clear()
            dual_app._reply_inbox.clear()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Module-level lifecycle (used by persistent_app's stateless lifespan branch)
# ---------------------------------------------------------------------------

_executor: Optional[StatelessTurnExecutor] = None


async def start_stateless_executor() -> StatelessTurnExecutor:
    """Create + start the singleton executor (idempotent)."""
    global _executor
    if _executor is not None and _executor._worker_quarantined:
        raise _ClaimQuiescenceError(
            "quarantined worker runtime requires process replacement"
        )
    if _executor is not None and _executor.running:
        return _executor
    executor = StatelessTurnExecutor()
    # Fail fast when the pool is missing — a stateless pod without its app-DB
    # pool can never serve a claim, and /ready must go 503, not lie.
    executor._db
    executor.start()
    _executor = executor
    return executor


async def stop_stateless_executor(timeout: Optional[float] = None) -> None:
    global _executor
    try:
        if _executor is not None:
            await _executor.stop(timeout)
            _executor = None
    finally:
        # Worker savers share one process pool.  It is lifespan-owned, never
        # closed at a batch boundary, and must still close when startup failed
        # after opening it but before publishing the executor singleton.
        from agent.core.fenced_checkpointer import close_fenced_checkpointer_pool

        await close_fenced_checkpointer_pool()


def executor_running() -> bool:
    return _executor is not None and _executor.running
