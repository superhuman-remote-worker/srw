"""Durable execution authority for persistent-session input.

``thread_messages`` is the transcript, not an inbox: restore presents those
rows as context and never schedules them.  This module keeps the smallest
separate state needed to make a persisted input reclaimable and to prove when
its one paid turn crossed provider admission.

Pinned mutators use ``thread -> agent -> delivery``; stateless mutators use
``thread -> run_queue -> delivery``. Runtime identities and queue leases are
server-issued observations; no model-visible tool schema contains them.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid5


_THREAD_MESSAGE_ID_NAMESPACE = UUID("4b9d8f7e-2c3a-5d6b-8e1f-0a1b2c3d4e5f")

# Bound on repeated recovery on the pinned lane (parallel_subagents.md §5.5,
# §14.1): an input whose recovery chain reached this many provider admissions
# is parked instead of served again, as the stateless lane parks its unit at
# ``run_queue.max_attempts`` (default 5). The chain is the input plus every
# input it supersedes (``supersedes_input_seq``): a recovery turn may delegate
# again (D3), and each death then supersedes the last continuation with a new
# one, so a count per delivery alone would never reach the bound.
PINNED_RECOVERY_ADMISSION_LIMIT = 5
# ``deferred_reason`` of a parked input. The pinned claim never takes it;
# the owner's retry (``retry_parked_pinned_inputs``) re-arms it.
PINNED_RECOVERY_PARK_REASON = "max_attempts"
PINNED_RECOVERY_RETRY_REASON = "owner_retry"
PINNED_RECOVERY_PARK_NOTICE = (
    "This input was stopped after {attempts} attempts. Each attempt was cut "
    "short because the process running this session was replaced, so it is "
    "not run again automatically. Changes from those attempts may be "
    "incomplete. Retry it to run it once more."
)


class InputDeliveryAuthorityLost(RuntimeError):
    """The caller is not the exact current delivery authority."""


class InputDeliveryConflict(RuntimeError):
    """A stable delivery identity was reused for different input."""


def raw_message_id(delivery_id: str | UUID) -> str:
    return f"msg_delivery_{UUID(str(delivery_id)).hex}"


def message_row_id(delivery_id: str | UUID) -> UUID:
    return uuid5(_THREAD_MESSAGE_ID_NAMESPACE, raw_message_id(delivery_id))


def _dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _with_idle_exit(update_sql: str) -> str:
    """Close an old wait only with the successful execution-admission CAS.

    Caller already holds thread/runtime authority in the usual lock order.
    No flag gates exit: disabling new tracking must not strand an old episode.
    Presence, queued input, replay and settlement do not reach this statement.
    """
    return (
        "WITH admitted_delivery AS (" + update_sql + "), idle_exit AS ("
        "UPDATE threads SET workspace_idle_episode=NULL, "
        "workspace_idle_revision=workspace_idle_revision+1 "
        "FROM admitted_delivery WHERE threads.id=admitted_delivery.thread_id "
        "AND threads.workspace_idle_episode IS NOT NULL RETURNING threads.id) "
        "SELECT delivery_id FROM admitted_delivery"
    )


def stale_admission_answered_sql(
    *, delivery: str = "delivery", message: str = "message"
) -> str:
    """SQL predicate: the admitted event's own turn reached a durable end.

    The same two proofs as skip-if-answered for a human input
    (``_ANSWERED_BY_TRANSCRIPT_SQL`` in ``agent.api.turn_executor``): the
    turn's authoritative final reconcile minted ``turn_execution_id`` on this
    input row (the loop runs it at every turn end, an error included), or the
    loop's incremental writer already persisted the turn's final answer
    (content, no tool calls) after the input. The turn is the one the
    admission recorded.
    """

    return f"""(
        {message}.turn_execution_id IS NOT NULL
        OR EXISTS (
            SELECT 1
              FROM thread_messages AS answer
             WHERE answer.thread_id = {message}.thread_id
               AND answer.role = 'ai'
               AND answer.rewound_at IS NULL
               AND answer.turn_number = COALESCE(
                   {delivery}.admitted_turn_number, {message}.turn_number
               )
               AND answer.seq > {message}.seq
               AND COALESCE(answer.content, '') <> ''
               AND (
                   answer.tool_calls IS NULL
                   OR jsonb_typeof(answer.tool_calls) <> 'array'
                   OR jsonb_array_length(answer.tool_calls) = 0
               )
        )
    )"""


def stale_stateless_admission_sql(
    *,
    lease_token: str,
    watermark: str,
    delivery: str = "delivery",
    message: str = "message",
) -> str:
    """SQL predicate: an event admitted under an older lease that never settled.

    A stateless event input is served under the ``run_queue`` lease that
    claimed it (``owner_run_queue_lease_token``); a human input is owed while
    its seq is above the watermark. When the executor that admitted the event
    dies before the loop settles it, the delivery stays ``admitted`` and no
    later claim would serve it again (parallel_subagents.md §8, "Recovery turn
    killed", found live as K4). Such an admission is owed to the next lease:
    served again when its turn left no durable end, settled when it did
    (``stale_admission_answered_sql``). The guards:

    * ``owner_run_queue_lease_token < lease_token``: every claim and every
      reaper steal advances the unit's token, so only an admission of an older
      lease qualifies. The current lease's own admission (a turn in flight) is
      never selected again.
    * no live ``subagent`` continuation supersedes the event: a recovery turn
      that delegated and died is settled against the event instead
      (parallel_subagents.md D3). That settle also settles this delivery; the
      guard keeps a superseded event from being served even if it did not.
    * an unanswered admission only while its seq is above the watermark. The
      watermark never moves past one (the pending query selects it before any
      later input, and the no-pending completion runs only when nothing is
      owed), so this excludes only history stranded before this rule, which
      must not be replayed into a newer conversation. An answered admission is
      settled wherever it sits: its own checkpoint may have passed it when the
      loop's settle lost the race with the completion, and an unsettled
      delivery blocks rewind.

    ``lease_token`` and ``watermark`` are SQL expressions (a parameter or a
    ``run_queue`` column); the aliases name the caller's delivery and message
    rows.
    """

    answered = stale_admission_answered_sql(delivery=delivery, message=message)
    return f"""(
        {delivery}.execution_lane = 'stateless'
        AND {delivery}.state = 'admitted'
        AND {delivery}.owner_run_queue_lease_token < {lease_token}
        AND ({message}.seq > COALESCE({watermark}, -1) OR {answered})
        AND NOT EXISTS (
            SELECT 1
              FROM thread_input_deliveries AS successor
              JOIN thread_messages AS successor_message
                ON successor_message.id = successor.message_id
             WHERE successor.thread_id = {delivery}.thread_id
               AND successor.source = 'subagent'
               AND successor.supersedes_input_seq = {message}.seq
               AND successor_message.rewound_at IS NULL
        )
    )"""


def turn_completed_frame_sql(*, thread: str, turn_id: str, after: str) -> str:
    """SQL body: the loop's ``turn.completed`` frame for one turn of a thread.

    ``SELECT 1 ...``, for the caller to wrap in ``EXISTS``. The frame proves
    the turn ended, not that its rows are durable: the pinned loop broadcasts
    it before its best-effort transcript reconcile (parallel_subagents.md
    §14.1), and a final message that carries a tool call has no answer row.
    ``thread``, ``turn_id`` (text) and ``after`` (the earliest the frame may
    be journaled) are SQL expressions. The batch settle anchors it after the
    delegating AI row; the pinned re-serve after the admission.
    """

    return f"""
SELECT 1
  FROM thread_events AS frame
 WHERE frame.thread_id = {thread}
   AND frame.kind = 'turn.completed'
   AND (frame.payload->>'turn_id') = {turn_id}
   AND frame.created_at >= {after}
"""


def pinned_admission_answered_sql(
    *, delivery: str = "delivery", message: str = "message"
) -> str:
    """SQL predicate: a pinned admission's own turn reached its end.

    ``stale_admission_answered_sql`` (its final answer row), or the loop's
    ``turn.completed`` frame for the admitted turn journaled after the
    admission: the same proof the batch settle accepts. The stateless lane
    keeps the narrower predicate; its loop journals the frame only after the
    authoritative reconcile that the first proof reads.
    """

    answered = stale_admission_answered_sql(delivery=delivery, message=message)
    frame = turn_completed_frame_sql(
        thread=f"{delivery}.thread_id",
        turn_id=f"{delivery}.admitted_turn_number::text",
        after=f"{delivery}.admitted_at",
    )
    return f"({answered} OR EXISTS ({frame}))"


def later_pinned_admission_sql(*, delivery: str = "delivery") -> str:
    """SQL predicate: another input of the thread was admitted after this one.

    The pinned loop admits one input per turn, so a later admission means the
    conversation went on without this turn. Read it before a hand-back: the
    hand-back clears the later row's own ``admitted_at``.
    """

    return f"""EXISTS (
        SELECT 1
          FROM thread_input_deliveries AS later
         WHERE later.thread_id = {delivery}.thread_id
           AND later.delivery_id <> {delivery}.delivery_id
           AND later.admitted_at > {delivery}.admitted_at
    )"""


def stale_pinned_admission_sql(
    *,
    process_generation: str,
    delivery: str = "delivery",
    message: str = "message",
) -> str:
    """SQL predicate: a pinned event admitted by a process that is gone.

    The pinned twin of ``stale_stateless_admission_sql`` (parallel_subagents.md
    §14.1, "an admitted but unsettled delivery is never served again"). A
    pinned claim takes only unadmitted rows, so an event whose runtime died
    after provider admission stayed ``admitted`` and no successor served it.
    ``reserve_stale_pinned_admissions`` resolves every such row: settled when
    its turn reached its end (``pinned_admission_answered_sql``) or when a
    later admission made it history (``later_pinned_admission_sql``: replaying
    it into a newer conversation would be wrong, and an unsettled delivery
    blocks rewind and idle release), otherwise served again. The guards:

    * events only. A ``direct_human`` partial turn keeps its immutable
      admission receipt and is never replayed (R3.3c, migration 0313).
    * ``owner_runtime_generation`` differs from the attaching process's
      generation. A generation is minted per attach in one process, so only an
      admission of an earlier process qualifies; the current process's own
      admission (a turn in flight) is never selected.
    * no live ``subagent`` continuation supersedes the event: a turn that
      delegated and died is settled against its input by the batch recovery,
      which runs before this (parallel_subagents.md D3).

    ``process_generation`` is an SQL expression (a parameter); the aliases
    name the caller's delivery and message rows.
    """

    return f"""(
        {delivery}.execution_lane = 'pinned'
        AND {delivery}.state = 'admitted'
        AND {delivery}.source <> 'direct_human'
        AND {message}.role = 'event'
        AND {delivery}.owner_runtime_generation IS DISTINCT FROM {process_generation}
        AND NOT EXISTS (
            SELECT 1
              FROM thread_input_deliveries AS successor
              JOIN thread_messages AS successor_message
                ON successor_message.id = successor.message_id
             WHERE successor.thread_id = {delivery}.thread_id
               AND successor.source = 'subagent'
               AND successor.supersedes_input_seq = {message}.seq
               AND successor_message.rewound_at IS NULL
        )
    )"""


# The recovery chain of one delivery: the delivery, the input it supersedes,
# that input's own superseded input, and so on. A continuation always
# supersedes an earlier row, so ``seq`` strictly decreases along the chain;
# the depth cap only guards against a corrupt cycle.
_RECOVERY_CHAIN_SQL = """
    WITH RECURSIVE chain AS (
        SELECT delivery.delivery_id, delivery.thread_id, message.seq,
               delivery.supersedes_input_seq, delivery.admission_count,
               1 AS depth
          FROM thread_input_deliveries AS delivery
          JOIN thread_messages AS message ON message.id = delivery.message_id
         WHERE delivery.delivery_id = $1
        UNION ALL
        SELECT source.delivery_id, source.thread_id, source_message.seq,
               source.supersedes_input_seq, source.admission_count,
               chain.depth + 1
          FROM chain
          JOIN thread_messages AS source_message
            ON source_message.thread_id = chain.thread_id
           AND source_message.seq = chain.supersedes_input_seq
          JOIN thread_input_deliveries AS source
            ON source.message_id = source_message.id
         WHERE chain.supersedes_input_seq < chain.seq
           AND chain.depth < 64
    )
    SELECT COALESCE(sum(admission_count), 0)::bigint AS admissions,
           array_agg(delivery_id ORDER BY depth) AS members
      FROM chain
"""


async def recovery_chain_admissions(
    conn: Any, *, delivery_id: str | UUID
) -> tuple[int, list[UUID]]:
    """Provider admissions summed along one delivery's recovery chain."""

    row = await conn.fetchrow(_RECOVERY_CHAIN_SQL, UUID(str(delivery_id)))
    if row is None:
        return 0, []
    return int(row["admissions"] or 0), list(row["members"] or [])


def _may_reach_recovery_bound(row: dict[str, Any]) -> bool:
    """Only an event that was admitted before, or continues one, can count."""

    return (
        str(row.get("role") or "") == "event"
        and str(row.get("source") or "") != "direct_human"
        and (
            int(row.get("admission_count") or 0) > 0
            or row.get("supersedes_input_seq") is not None
        )
    )


def recovery_park_notice_id(delivery_id: str | UUID, claim_generation: int) -> UUID:
    """The one transcript notice of one park (its claim generation)."""

    return uuid5(
        _THREAD_MESSAGE_ID_NAMESPACE,
        f"input_recovery_park:{UUID(str(delivery_id)).hex}:{int(claim_generation)}",
    )


async def _park_at_recovery_bound(
    conn: Any,
    *,
    delivery_id: Any,
    agent_id: UUID,
    pod_uid: str,
    runtime_generation: UUID,
    admissions: int,
) -> dict[str, Any] | None:
    """Park one input at the recovery bound and tell the person once.

    The current runtime takes the row (so a later abrupt-exit receipt of this
    life counts it as its own leftover, migration 0335) and leaves it
    ``deferred`` with ``max_attempts``, which no pinned claim takes. The
    ``error`` row is the transcript line the cockpit shows; restore never
    loads ``error`` rows, so the model does not see it. Caller holds the
    thread lock and the delivery row.
    """

    parked = await conn.fetchrow(
        """
        UPDATE thread_input_deliveries
           SET state = 'deferred', claim_generation = claim_generation + 1,
               owner_agent_id = $2, owner_pod_uid = $3,
               owner_runtime_generation = $4,
               owned_at = statement_timestamp(), queued_at = NULL,
               admitted_at = NULL, admitted_turn_number = NULL,
               deferred_reason = $5, deferred_at = statement_timestamp(),
               updated_at = statement_timestamp()
         WHERE delivery_id = $1
           AND execution_lane = 'pinned'
           AND state IN ('persisted', 'owned', 'queued', 'deferred', 'admitted')
        RETURNING *
        """,
        delivery_id,
        agent_id,
        str(pod_uid),
        runtime_generation,
        PINNED_RECOVERY_PARK_REASON,
    )
    if parked is None:
        return None
    message = await conn.fetchrow(
        "SELECT thread_id, turn_number FROM thread_messages WHERE id = $1",
        parked["message_id"],
    )
    await conn.execute(
        "INSERT INTO thread_messages (id, thread_id, role, content, turn_number) "
        "VALUES ($1, $2, 'error', $3, $4) ON CONFLICT (id) DO NOTHING",
        recovery_park_notice_id(parked["delivery_id"], parked["claim_generation"]),
        parked["thread_id"],
        PINNED_RECOVERY_PARK_NOTICE.format(attempts=int(admissions)),
        message["turn_number"] if message is not None else None,
    )
    return _dict(parked)


async def lock_runtime_authority(
    conn: Any,
    *,
    thread_id: str | UUID,
    agent_id: str | UUID,
    pod_uid: str,
    session_runtime_generation: str | UUID,
    runtime_attach_token: str | UUID,
) -> dict[str, Any]:
    """Lock and prove exact reciprocal thread/agent/pod authority."""

    thread_uuid = UUID(str(thread_id))
    agent_uuid = UUID(str(agent_id))
    runtime_uuid = UUID(str(session_runtime_generation))
    attach_uuid = UUID(str(runtime_attach_token))
    pod = str(pod_uid or "").strip()
    if not pod:
        raise InputDeliveryAuthorityLost("runtime pod identity is unavailable")

    thread = await conn.fetchrow(
        "SELECT id, agent_id, status, execution_lane, runtime_generation, "
        "runtime_attach_token, runtime_retirement_token, user_id, total_turns, "
        "conversation_revision "
        "FROM threads "
        "WHERE id = $1 FOR UPDATE",
        thread_uuid,
    )
    if (
        thread is None
        or str(thread["agent_id"] or "") != str(agent_uuid)
        or str(thread["execution_lane"] or "pinned") == "stateless"
        or str(thread["status"] or "") in {"ended", "suspended"}
        or str(thread["runtime_generation"] or "") != str(runtime_uuid)
        or str(thread["runtime_attach_token"] or "") != str(attach_uuid)
        or thread["runtime_retirement_token"] is not None
    ):
        raise InputDeliveryAuthorityLost("thread runtime authority was lost")

    agent = await conn.fetchrow(
        "SELECT id, thread_id, pod_uid, status FROM agents WHERE id = $1 FOR SHARE",
        agent_uuid,
    )
    if (
        agent is None
        or str(agent["thread_id"] or "") != str(thread_uuid)
        or str(agent["pod_uid"] or "") != pod
        or str(agent["status"] or "") in {"offline", "deleted"}
    ):
        raise InputDeliveryAuthorityLost("agent runtime authority was lost")
    return _dict(thread)


async def _lock_stateless_runtime_authority(
    conn: Any,
    *,
    thread_id: str | UUID,
    lease_token: int,
    executor_id: str,
    pod_uid: str,
    for_update: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Lock and prove one exact stateless ``session_turn`` claimant.

    Lock order is ``thread -> run_queue``. The delivery row is locked by the
    caller only after this helper returns, matching orchestrator-side event
    admission (``thread -> run_queue -> delivery``).
    """

    thread_uuid = UUID(str(thread_id))
    executor = str(executor_id or "").strip()
    pod = str(pod_uid or "").strip()
    if int(lease_token) <= 0 or not executor or not pod:
        raise InputDeliveryAuthorityLost("stateless runtime identity is incomplete")
    lock_clause = "FOR UPDATE" if for_update else "FOR SHARE"
    thread = await conn.fetchrow(
        "SELECT id, agent_id, status, execution_lane, user_id, total_turns, "
        "metadata, conversation_revision "
        f"FROM threads WHERE id = $1 {lock_clause}",
        thread_uuid,
    )
    if (
        thread is None
        or str(thread["execution_lane"] or "") != "stateless"
        or thread["agent_id"] is not None
        or str(thread["status"] or "") in {"ended", "suspended"}
    ):
        raise InputDeliveryAuthorityLost("stateless thread authority was lost")
    # Queue rows identify a pod by its reusable name.  The credential bundle
    # additionally stamps the immutable Kubernetes UID in thread metadata;
    # require the exact three-part claim so a restarted pod with the same name
    # cannot inherit another incarnation's DB mutation authority.
    from shared.session_retirement import active_claim_authority

    try:
        active_claim = active_claim_authority(thread["metadata"])
    except RuntimeError as exc:
        raise InputDeliveryAuthorityLost("stateless active claim is malformed") from exc
    if (
        active_claim is None
        or int(active_claim[0]) != int(lease_token)
        or active_claim[1].pod != executor
        or active_claim[1].pod_uid != pod
    ):
        raise InputDeliveryAuthorityLost("stateless pod incarnation was lost")
    queue = await conn.fetchrow(
        "SELECT unit_id, unit_kind, state, lease_token, leased_by, "
        "input_delivery_capable_lease_token, consumed_seq FROM run_queue "
        f"WHERE unit_id = $1 {lock_clause}",
        thread_uuid,
    )
    if (
        queue is None
        or str(queue["unit_kind"] or "") != "session_turn"
        or str(queue["state"] or "") != "leased"
        or int(queue["lease_token"] or 0) != int(lease_token)
        or str(queue["leased_by"] or "") != executor
        or int(queue["input_delivery_capable_lease_token"] or 0) != int(lease_token)
    ):
        raise InputDeliveryAuthorityLost("stateless queue lease was lost")
    return _dict(thread), _dict(queue)


async def persist_input_delivery(
    conn: Any,
    *,
    thread_id: str | UUID,
    delivery_id: str | UUID,
    role: str,
    content: str,
    source: str,
    turn_number: int | None,
    agent_id: str | UUID | None = None,
    pod_uid: str | None = None,
    runtime_generation: str | UUID | None = None,
    session_runtime_generation: str | UUID | None = None,
    runtime_attach_token: str | UUID | None = None,
    allow_stateless_subagent_event: bool = False,
    supersedes_input_seq: int | None = None,
    turn_number_hint: int | None = None,
) -> dict[str, Any]:
    """Atomically persist one transcript row and optionally claim execution.

    With no runtime identity this is an orchestrator-side durable persist only.
    With all three identity fields it also establishes/recovers the exact
    process claim.  Partial identity is always rejected.

    ``turn_number`` asserts the input's turn: a terminal receipt of another
    turn is a conflict. ``turn_number_hint`` only numbers a transcript row this
    call creates (a runtime's guess at its next turn); it never identifies an
    existing row or receipt, so a retry that arrives after the session's turn
    counter moved on still resolves to its receipt. Pass at most one.
    """

    delivery_uuid = UUID(str(delivery_id))
    thread_uuid = UUID(str(thread_id))
    row_id = message_row_id(delivery_uuid)
    source_value = str(source or "unknown")[:80]
    if isinstance(supersedes_input_seq, bool) or (
        supersedes_input_seq is not None
        and (not isinstance(supersedes_input_seq, int) or supersedes_input_seq <= 0)
    ):
        raise InputDeliveryConflict("superseded input sequence is invalid")
    if turn_number is not None and turn_number_hint is not None:
        raise ValueError("pass either an asserted turn number or a hint, not both")
    session_runtime_generation = session_runtime_generation or runtime_generation
    identity = (
        agent_id,
        pod_uid,
        runtime_generation,
        session_runtime_generation,
        runtime_attach_token,
    )
    has_identity = all(value is not None for value in identity)
    if any(value is not None for value in identity) and not has_identity:
        raise InputDeliveryAuthorityLost("incomplete runtime identity")

    # Lock the parent before the delivery row in every path.  Do not require
    # live runtime ownership yet: an admitted/settled delivery is an immutable
    # historical receipt whose response may have been lost immediately before
    # End cleared the live binding.
    thread_row = await conn.fetchrow(
        "SELECT id, agent_id, status, execution_lane, runtime_generation, "
        "runtime_attach_token, runtime_retirement_token, user_id, total_turns, "
        "conversation_revision "
        "FROM threads WHERE id = $1 FOR UPDATE",
        thread_uuid,
    )
    if thread_row is None:
        raise InputDeliveryAuthorityLost("thread no longer exists")
    thread = _dict(thread_row)

    execution_lane = str(thread.get("execution_lane") or "pinned")
    if execution_lane not in {"pinned", "stateless"}:
        raise InputDeliveryAuthorityLost("thread execution lane is unsupported")

    # A provider-admitted delivery is an immutable execution receipt. Its
    # outbox response may have been lost and the thread may legitimately move
    # lanes before the stable retry arrives. Resolve that retry against the
    # historical ledger lane/identity, not the thread's new lane, and return
    # without touching either queue. Pending rows deliberately stay on the
    # current-lane path below so a lane mismatch cannot re-arm or launder them.
    terminal_replay = await conn.fetchrow(
        "SELECT delivery.*, message.seq, message.thread_id AS message_thread_id, "
        "message.role, message.content, message.turn_number, message.rewound_at "
        "FROM thread_input_deliveries AS delivery "
        "JOIN thread_messages AS message ON message.id=delivery.message_id "
        "WHERE delivery.delivery_id=$1 "
        "AND delivery.state IN ('admitted','settled') "
        "FOR UPDATE OF delivery",
        delivery_uuid,
    )
    if terminal_replay is not None:
        if (
            str(terminal_replay["thread_id"]) != str(thread_uuid)
            or str(terminal_replay["message_thread_id"]) != str(thread_uuid)
            or str(terminal_replay["message_id"]) != str(row_id)
            or str(terminal_replay["source"]) != source_value
            or str(terminal_replay["role"]) != str(role)
            or str(terminal_replay["execution_lane"] or "")
            not in {"pinned", "stateless"}
            or terminal_replay.get("supersedes_input_seq") != supersedes_input_seq
            or (
                turn_number is not None
                and terminal_replay.get("turn_number") != turn_number
            )
        ):
            raise InputDeliveryConflict(
                "stable input identity conflicts with terminal delivery"
            )
        stored_content = str(terminal_replay["content"] or "")
        if stored_content != str(content) and source_value != "officer_wake":
            raise InputDeliveryConflict(
                "stable input identity conflicts with transcript"
            )
        if has_identity and (
            str(terminal_replay["execution_lane"] or "") != "pinned"
            or str(terminal_replay["owner_agent_id"] or "") != str(agent_id)
            or str(terminal_replay["owner_pod_uid"] or "") != str(pod_uid)
            or str(terminal_replay["owner_runtime_generation"] or "")
            != str(runtime_generation)
        ):
            raise InputDeliveryAuthorityLost(
                "terminal delivery belongs to another runtime"
            )
        result = _dict(terminal_replay)
        result.update(
            {
                "message_id": str(row_id),
                "message_row_id": str(row_id),
                "seq": int(terminal_replay["seq"]),
                "transcript_inserted": False,
                "content": stored_content,
                "role": str(terminal_replay["role"]),
                "turn_number": terminal_replay["turn_number"],
                # This is provenance, not the thread's newly selected lane.
                "execution_lane": str(terminal_replay["execution_lane"]),
                "queue_state": None,
                "execution_disposition": (
                    "historical"
                    if terminal_replay["rewound_at"] is not None
                    else "current"
                ),
            }
        )
        return result

    historical = await conn.fetchrow(
        "SELECT delivery.*, message.seq, message.thread_id AS message_thread_id, "
        "message.role, message.content, message.turn_number, message.rewound_at "
        "FROM thread_input_deliveries AS delivery "
        "JOIN thread_messages AS message ON message.id=delivery.message_id "
        "WHERE delivery.delivery_id=$1 FOR UPDATE OF delivery",
        delivery_uuid,
    )
    if historical is not None and historical["rewound_at"] is not None:
        if (
            str(historical["thread_id"]) != str(thread_uuid)
            or str(historical["message_thread_id"]) != str(thread_uuid)
            or str(historical["message_id"]) != str(row_id)
            or str(historical["source"]) != source_value
            or str(historical["role"]) != str(role)
            or historical.get("supersedes_input_seq") != supersedes_input_seq
        ):
            raise InputDeliveryConflict(
                "stable input identity conflicts with historical delivery"
            )
        stored_content = str(historical["content"] or "")
        if stored_content != str(content) and source_value != "officer_wake":
            raise InputDeliveryConflict(
                "stable input identity conflicts with historical transcript"
            )
        result = _dict(historical)
        result.update(
            {
                "message_id": str(row_id),
                "message_row_id": str(row_id),
                "seq": int(historical["seq"]),
                "transcript_inserted": False,
                "content": stored_content,
                "role": str(historical["role"]),
                "turn_number": historical["turn_number"],
                "execution_lane": str(historical["execution_lane"]),
                "queue_state": None,
                "execution_disposition": "superseded",
            }
        )
        return result

    if has_identity:
        # New work still needs the current reciprocal binding, attach token,
        # open generation, and live agent row.  This second read reuses the
        # parent lock already held above and keeps the standalone helper strict.
        thread = await lock_runtime_authority(
            conn,
            thread_id=thread_uuid,
            agent_id=str(agent_id),
            pod_uid=str(pod_uid),
            session_runtime_generation=str(session_runtime_generation),
            runtime_attach_token=str(runtime_attach_token),
        )

    if execution_lane == "pinned" and (
        thread.get("runtime_retirement_token") is not None
        or str(thread.get("status") or "") in {"ending", "ended", "suspended"}
    ):
        # A terminal replay above remains observable after End. New input does
        # not: End and persistence serialize on the same thread row, so the
        # winner is the only truthful durable outcome.
        raise InputDeliveryAuthorityLost("pinned thread retirement owns input")

    if has_identity and execution_lane != "pinned":
        raise InputDeliveryAuthorityLost("pinned runtime cannot claim stateless input")
    if not has_identity and execution_lane == "stateless":
        accepted_source = source_value == "officer_wake" or (
            allow_stateless_subagent_event and source_value == "subagent"
        )
        if str(role) != "event" or not accepted_source:
            raise InputDeliveryAuthorityLost(
                "stateless durable input is reserved for server events"
            )
        if thread.get("agent_id") is not None:
            raise InputDeliveryAuthorityLost("stateless thread is unexpectedly bound")

    # Stable delivery identity owns the immutable transcript row. A retry may
    # be rendered after the session's turn counter advanced, or may carry a
    # newly derived turn hint; neither is allowed to reinterpret that row.
    existing_turn_number = await conn.fetchval(
        "SELECT turn_number FROM thread_messages WHERE id=$1 AND thread_id=$2",
        row_id,
        thread_uuid,
    )
    effective_turn_number = (
        int(existing_turn_number)
        if existing_turn_number is not None
        else turn_number
        if turn_number is not None
        else turn_number_hint
    )
    if execution_lane == "stateless" and (
        isinstance(effective_turn_number, bool)
        or not isinstance(effective_turn_number, int)
        or effective_turn_number <= 0
    ):
        # The first stateless writer derives its turn exactly once. Concurrent
        # retries then take the committed branch above.
        effective_turn_number = int(thread.get("total_turns") or 0) + 1

    inserted = await conn.fetchrow(
        """
        INSERT INTO thread_messages
            (id, thread_id, role, content, turn_number)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (id) DO NOTHING
        RETURNING id, seq, thread_id, role, content, turn_number
        """,
        row_id,
        thread_uuid,
        str(role),
        str(content),
        effective_turn_number,
    )
    transcript_inserted = inserted is not None
    message = inserted or await conn.fetchrow(
        "SELECT id, seq, thread_id, role, content, turn_number "
        "FROM thread_messages WHERE id = $1",
        row_id,
    )
    if (
        message is None
        or str(message["thread_id"]) != str(thread_uuid)
        or str(message["role"]) != str(role)
        or message.get("turn_number") != effective_turn_number
    ):
        raise InputDeliveryConflict("stable input identity conflicts with transcript")

    if transcript_inserted:
        await conn.execute(
            "UPDATE threads SET last_activity = CURRENT_TIMESTAMP, "
            "total_turns = GREATEST(total_turns, COALESCE($2, 0)) WHERE id = $1",
            thread_uuid,
            effective_turn_number,
        )

    existing_delivery = await conn.fetchrow(
        "SELECT * FROM thread_input_deliveries WHERE delivery_id = $1",
        delivery_uuid,
    )
    existing_state = (
        str(existing_delivery["state"] or "") if existing_delivery is not None else ""
    )

    queue_state: str | None = None
    queue_deferred_reason: str | None = None
    if (
        not has_identity
        and execution_lane == "stateless"
        and existing_state not in {"admitted", "settled"}
    ):
        status = str(thread.get("status") or "")
        if status == "suspended":
            woke = await conn.fetchval(
                "UPDATE threads SET status = 'created', agent_id = NULL, "
                "control_admission_agent_id = NULL, awaiting_user_since = NULL, "
                "extend_count = 0 WHERE id = $1 AND execution_lane = 'stateless' "
                "AND status = 'suspended' RETURNING id",
                thread_uuid,
            )
            if woke is None:
                raise InputDeliveryAuthorityLost(
                    "stateless suspended-input wake lost thread authority"
                )
            status = "created"
        if status in {"created", "active", "awaiting_user"}:
            queue = await conn.fetchrow(
                "SELECT unit_kind, state, lease_token, "
                "input_delivery_capable_lease_token FROM run_queue "
                "WHERE unit_id = $1 FOR UPDATE",
                thread_uuid,
            )
            # A rolling-old executor that already owns this unit cannot see an
            # event row. Leave the durable input unqueued until that exact
            # claim finishes; the outbox's stable retry then admits it without
            # disturbing the old turn's watermark.
            if queue is not None and str(queue["unit_kind"] or "") != "session_turn":
                raise InputDeliveryAuthorityLost("run_queue unit kind is incompatible")
            if (
                queue is not None
                and str(queue["state"] or "") == "leased"
                and int(queue["input_delivery_capable_lease_token"] or 0)
                != int(queue["lease_token"] or 0)
            ):
                queue_deferred_reason = "rolling_old_executor"
            else:
                from shared.run_queue import (
                    UNIT_KIND_SESSION_TURN,
                    record_input_seq,
                )

                queue_state = await record_input_seq(
                    conn,
                    unit_id=thread_uuid,
                    unit_kind=UNIT_KIND_SESSION_TURN,
                    input_seq=int(message["seq"]),
                    fair_key=(
                        str(thread["user_id"])
                        if thread.get("user_id") is not None
                        else None
                    ),
                )
                if queue_state == "parked":
                    queue_deferred_reason = "run_queue_parked"
        elif status != "ended":
            raise InputDeliveryAuthorityLost(
                f"stateless thread does not accept event input ({status or 'unknown'})"
            )

    await conn.execute(
        """
        INSERT INTO thread_input_deliveries
            (delivery_id, thread_id, message_id, source, execution_lane,
             supersedes_input_seq, conversation_revision)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (delivery_id) DO NOTHING
        """,
        delivery_uuid,
        thread_uuid,
        row_id,
        source_value,
        execution_lane,
        supersedes_input_seq,
        int(thread.get("conversation_revision") or 0),
    )
    delivery = await conn.fetchrow(
        "SELECT * FROM thread_input_deliveries WHERE delivery_id = $1 FOR UPDATE",
        delivery_uuid,
    )
    if (
        delivery is None
        or str(delivery["thread_id"]) != str(thread_uuid)
        or str(delivery["message_id"]) != str(row_id)
        or str(delivery["source"]) != source_value
        or str(delivery["execution_lane"] or "") != execution_lane
        or delivery.get("supersedes_input_seq") != supersedes_input_seq
    ):
        raise InputDeliveryConflict("stable input identity conflicts with delivery")

    # A wake renderer may observe newer project state on an ambiguous-response
    # retry. The first committed transcript payload is canonical for that
    # server-owned delivery identity; replay it rather than rejecting the retry
    # or queueing different text under one identity. Human delivery identities
    # retain the stricter invariant so internal misuse cannot alias two inputs.
    stored_content = str(message["content"] or "")
    if stored_content != str(content) and source_value != "officer_wake":
        raise InputDeliveryConflict("stable input identity conflicts with transcript")

    if (
        not has_identity
        and execution_lane == "stateless"
        and str(delivery["state"]) not in {"admitted", "settled", "cancelled"}
    ):
        if queue_deferred_reason is not None:
            delivery = await conn.fetchrow(
                "UPDATE thread_input_deliveries SET state = 'deferred', "
                "deferred_reason = $2, deferred_at = statement_timestamp(), "
                "queued_at = NULL, updated_at = statement_timestamp() "
                "WHERE delivery_id = $1 AND execution_lane = 'stateless' "
                "AND state IN ('persisted', 'queued', 'deferred') RETURNING *",
                delivery_uuid,
                queue_deferred_reason,
            )
        elif queue_state is not None:
            delivery = await conn.fetchrow(
                "UPDATE thread_input_deliveries SET state = 'queued', "
                "queued_at = COALESCE(queued_at, statement_timestamp()), "
                "deferred_reason = NULL, deferred_at = NULL, "
                "updated_at = statement_timestamp() "
                "WHERE delivery_id = $1 AND execution_lane = 'stateless' "
                "AND state IN ('persisted', 'queued', 'deferred') RETURNING *",
                delivery_uuid,
            )
        if delivery is None:
            raise InputDeliveryAuthorityLost("stateless input admission was lost")

    if has_identity and str(delivery["state"]) not in {
        "admitted",
        "settled",
        "cancelled",
    }:
        same_runtime = (
            str(delivery["owner_agent_id"] or "") == str(agent_id)
            and str(delivery["owner_pod_uid"] or "") == str(pod_uid)
            and str(delivery["owner_runtime_generation"] or "")
            == str(runtime_generation)
        )
        # A stable-identity retry (an outbox wake, a settle's continuation)
        # leaves an input parked at the recovery bound parked; only its
        # owner's retry re-arms it.
        parked = (
            str(delivery["state"]) == "deferred"
            and delivery.get("deferred_reason") == PINNED_RECOVERY_PARK_REASON
        )
        if not parked and (not same_runtime or str(delivery["state"]) == "deferred"):
            delivery = await conn.fetchrow(
                """
                UPDATE thread_input_deliveries
                   SET state = 'owned',
                       claim_generation = claim_generation + 1,
                       owner_agent_id = $2,
                       owner_pod_uid = $3,
                       owner_runtime_generation = $4,
                       owned_at = statement_timestamp(),
                       queued_at = NULL,
                       deferred_reason = NULL,
                       deferred_at = NULL,
                       updated_at = statement_timestamp()
                 WHERE delivery_id = $1
                   AND state IN ('persisted', 'owned', 'queued', 'deferred')
                   AND NOT (state = 'deferred'
                            AND deferred_reason IS NOT DISTINCT FROM $5)
                RETURNING *
                """,
                delivery_uuid,
                UUID(str(agent_id)),
                str(pod_uid),
                UUID(str(runtime_generation)),
                PINNED_RECOVERY_PARK_REASON,
            )
            if delivery is None:
                raise InputDeliveryAuthorityLost("delivery claim was lost")

    result = _dict(delivery)
    result.update(
        {
            "message_id": str(row_id),
            "message_row_id": str(row_id),
            "seq": int(message["seq"]),
            "transcript_inserted": transcript_inserted,
            "content": stored_content,
            "role": str(role),
            "turn_number": message.get("turn_number"),
            "execution_lane": execution_lane,
            "queue_state": queue_state,
            "execution_disposition": "current",
        }
    )
    return result


async def claim_stateless_input_delivery(
    conn: Any,
    *,
    thread_id: str | UUID,
    delivery_id: str | UUID,
    lease_token: int,
    executor_id: str,
    pod_uid: str,
) -> dict[str, Any] | None:
    """Bind one pending event to the exact current stateless queue lease.

    A pending event is ``persisted``, ``queued`` or ``deferred``, or an
    admission of an older lease that never settled and whose turn left no
    durable answer (``stale_stateless_admission_sql``): its executor died in
    the turn, so it is owed again. Such an admission returns to ``queued``
    under this lease with a new claim generation, and the caller admits it
    again. An answered one is refused here; the executor settles it instead
    (``settle_answered_stateless_admission``).
    """

    thread_uuid = UUID(str(thread_id))
    delivery_uuid = UUID(str(delivery_id))
    thread, queue = await _lock_stateless_runtime_authority(
        conn,
        thread_id=thread_uuid,
        lease_token=lease_token,
        executor_id=executor_id,
        pod_uid=pod_uid,
    )
    stale = stale_stateless_admission_sql(
        lease_token="$4::bigint", watermark="$5::bigint"
    )
    answered = stale_admission_answered_sql()
    row = await conn.fetchrow(
        "SELECT delivery.*, message.seq, message.role, message.content, "
        "message.turn_number, message.rewound_at, "
        f"{stale} AS stale_admission, {answered} AS admission_answered "
        "FROM thread_input_deliveries AS delivery "
        "JOIN thread_messages AS message ON message.id = delivery.message_id "
        "WHERE delivery.delivery_id = $1 AND delivery.thread_id = $2 "
        "AND message.rewound_at IS NULL "
        "AND (delivery.conversation_revision=$3 OR "
        "(delivery.conversation_revision IS NULL AND $3=0)) "
        "FOR UPDATE OF delivery",
        delivery_uuid,
        thread_uuid,
        int(thread.get("conversation_revision") or 0),
        int(lease_token),
        queue.get("consumed_seq"),
    )
    owed_admission = (
        row is not None
        and bool(row["stale_admission"])
        and not bool(row["admission_answered"])
    )
    if (
        row is None
        or str(row["execution_lane"] or "") != "stateless"
        or str(row["role"] or "") != "event"
        or (
            str(row["state"] or "") not in {"persisted", "queued", "deferred"}
            and not owed_admission
        )
    ):
        return None
    same_claim = (
        int(row["owner_run_queue_lease_token"] or 0) == int(lease_token)
        and str(row["owner_executor"] or "") == str(executor_id)
        and str(row["owner_executor_pod_uid"] or "") == str(pod_uid)
    )
    # An owed admission clears its admission receipt (the row shape requires
    # it off 'admitted'); the next admission records the turn that runs.
    claimed = await conn.fetchrow(
        "UPDATE thread_input_deliveries SET state = 'queued', "
        "claim_generation = claim_generation + $2::bigint, "
        "owner_run_queue_lease_token = $3, owner_executor = $4, "
        "owner_executor_pod_uid = $5, owned_at = statement_timestamp(), "
        "queued_at = COALESCE(queued_at, statement_timestamp()), "
        "admitted_at = NULL, admitted_turn_number = NULL, "
        "deferred_reason = NULL, deferred_at = NULL, "
        "updated_at = statement_timestamp() WHERE delivery_id = $1 "
        "AND execution_lane = 'stateless' "
        "AND (state IN ('persisted', 'queued', 'deferred') "
        "OR (state = 'admitted' AND $6::boolean)) RETURNING *",
        delivery_uuid,
        0 if same_claim else 1,
        int(lease_token),
        str(executor_id),
        str(pod_uid),
        owed_admission,
    )
    if claimed is None:
        return None
    result = _dict(claimed)
    result.update(
        {
            "message_id": str(row["message_id"]),
            "seq": int(row["seq"]),
            "role": str(row["role"]),
            "content": str(row["content"] or ""),
            "turn_number": row["turn_number"],
        }
    )
    return result


async def settle_answered_stateless_admission(
    conn: Any,
    *,
    thread_id: str | UUID,
    delivery_id: str | UUID,
    lease_token: int,
    executor_id: str,
    pod_uid: str,
) -> str | None:
    """Settle an older lease's admission whose turn already reached its end.

    The executor that admitted the event died after the turn's durable end
    (its final answer or its final reconcile) and before the loop settled the
    delivery, or its settle lost a race with the queue completion. Serving it
    again would answer it twice, so the current exact claimant settles it.

    Returns ``"settled"``; ``"owed"`` when the admission is stale but its turn
    has no durable end (the caller serves it again through
    ``claim_stateless_input_delivery``); ``None`` when the row is not a stale
    admission of this unit. Raises ``InputDeliveryAuthorityLost`` when the
    caller is not the exact current claimant.
    """

    thread_uuid = UUID(str(thread_id))
    delivery_uuid = UUID(str(delivery_id))
    thread, queue = await _lock_stateless_runtime_authority(
        conn,
        thread_id=thread_uuid,
        lease_token=lease_token,
        executor_id=executor_id,
        pod_uid=pod_uid,
    )
    stale = stale_stateless_admission_sql(
        lease_token="$4::bigint", watermark="$5::bigint"
    )
    answered = stale_admission_answered_sql()
    row = await conn.fetchrow(
        f"SELECT {answered} AS admission_answered "
        "FROM thread_input_deliveries AS delivery "
        "JOIN thread_messages AS message ON message.id = delivery.message_id "
        "WHERE delivery.delivery_id = $1 AND delivery.thread_id = $2 "
        "AND message.role = 'event' AND message.rewound_at IS NULL "
        "AND (delivery.conversation_revision=$3 OR "
        "(delivery.conversation_revision IS NULL AND $3=0)) "
        f"AND {stale} "
        "FOR UPDATE OF delivery",
        delivery_uuid,
        thread_uuid,
        int(thread.get("conversation_revision") or 0),
        int(lease_token),
        queue.get("consumed_seq"),
    )
    if row is None:
        return None
    if not bool(row["admission_answered"]):
        return "owed"
    settled = await conn.fetchval(
        "UPDATE thread_input_deliveries SET state = 'settled', "
        "settled_at = statement_timestamp(), updated_at = statement_timestamp() "
        "WHERE delivery_id = $1 AND execution_lane = 'stateless' "
        "AND state = 'admitted' RETURNING delivery_id",
        delivery_uuid,
    )
    return "settled" if settled is not None else None


async def transition_stateless_input_delivery(
    conn: Any,
    *,
    thread_id: str | UUID,
    delivery_id: str | UUID,
    lease_token: int,
    executor_id: str,
    pod_uid: str,
    claim_generation: int,
    transition: str,
    turn_number: int | None = None,
    reason: str | None = None,
) -> bool:
    """CAS one stateless event through provider admission and settlement."""

    await _lock_stateless_runtime_authority(
        conn,
        thread_id=thread_id,
        lease_token=lease_token,
        executor_id=executor_id,
        pod_uid=pod_uid,
        for_update=transition == "admitted",
    )
    if transition == "admitted":
        if (
            isinstance(turn_number, bool)
            or not isinstance(turn_number, int)
            or turn_number <= 0
        ):
            raise ValueError("admitted input requires a positive turn number")
        states = ("queued",)
        assignments = (
            "state = 'admitted', admitted_at = statement_timestamp(), "
            "admitted_turn_number = input_params.turn_number, "
            "deferred_reason = NULL, "
            "deferred_at = NULL, updated_at = statement_timestamp()"
        )
    elif transition == "settled":
        states = ("admitted",)
        assignments = (
            "state = 'settled', settled_at = statement_timestamp(), "
            "updated_at = statement_timestamp()"
        )
    elif transition == "deferred":
        states = ("queued",)
        assignments = (
            "state = 'deferred', deferred_reason = "
            "LEFT(COALESCE(input_params.reason, 'retryable'), 120), "
            "deferred_at = statement_timestamp(), queued_at = NULL, "
            "updated_at = statement_timestamp()"
        )
    elif transition == "unadmit":
        states = ("admitted",)
        assignments = (
            "state = 'deferred', admitted_at = NULL, admitted_turn_number = NULL, "
            "deferred_reason = LEFT(COALESCE(input_params.reason, "
            "'provider_not_started'), 120), "
            "deferred_at = statement_timestamp(), queued_at = NULL, "
            "updated_at = statement_timestamp()"
        )
    else:  # pragma: no cover - caller contract
        raise ValueError(f"unsupported input delivery transition: {transition}")
    update_sql = (
        "WITH input_params AS ("
        "SELECT $7::bigint AS turn_number, $8::text AS reason) "
        f"UPDATE thread_input_deliveries SET {assignments} FROM input_params "
        "WHERE delivery_id = $1 AND thread_id = $2 "
        "AND execution_lane = 'stateless' AND state = ANY($3::text[]) "
        "AND claim_generation = $4 "
        "AND owner_run_queue_lease_token = $5 "
        "AND owner_executor = $6 AND owner_executor_pod_uid = $9 "
        "RETURNING delivery_id, thread_id"
    )
    updated = await conn.fetchval(
        _with_idle_exit(update_sql) if transition == "admitted" else update_sql,
        UUID(str(delivery_id)),
        UUID(str(thread_id)),
        list(states),
        int(claim_generation),
        int(lease_token),
        str(executor_id),
        turn_number,
        reason,
        str(pod_uid),
    )
    return updated is not None


async def claim_pending_input_deliveries(
    conn: Any,
    *,
    thread_id: str | UUID,
    agent_id: str | UUID,
    pod_uid: str,
    runtime_generation: str | UUID,
    session_runtime_generation: str | UUID | None = None,
    runtime_attach_token: str | UUID,
) -> list[dict[str, Any]]:
    """Claim every persisted/unadmitted input for one attached runtime.

    An input parked at the recovery bound is not claimed. An event whose
    recovery chain reached ``PINNED_RECOVERY_ADMISSION_LIMIT`` admissions is
    parked here instead of claimed (``_park_at_recovery_bound``): a fresh
    continuation of a turn that died again, or an event served again by
    ``reserve_stale_pinned_admissions`` and stranded once more.
    """

    thread_uuid = UUID(str(thread_id))
    agent_uuid = UUID(str(agent_id))
    runtime_uuid = UUID(str(runtime_generation))
    attach_uuid = UUID(str(runtime_attach_token))
    session_runtime = session_runtime_generation or runtime_generation
    thread = await lock_runtime_authority(
        conn,
        thread_id=thread_uuid,
        agent_id=agent_uuid,
        pod_uid=pod_uid,
        session_runtime_generation=session_runtime,
        runtime_attach_token=attach_uuid,
    )
    rows = await conn.fetch(
        """
        SELECT delivery.*, message.seq, message.role, message.content
          FROM thread_input_deliveries AS delivery
          JOIN thread_messages AS message ON message.id = delivery.message_id
         WHERE delivery.thread_id = $1
           AND delivery.state IN ('persisted', 'owned', 'queued', 'deferred')
           AND NOT (delivery.state = 'deferred'
                    AND delivery.deferred_reason IS NOT DISTINCT FROM $3)
           AND message.rewound_at IS NULL
           AND (delivery.conversation_revision=$2 OR
                (delivery.conversation_revision IS NULL AND $2=0))
         ORDER BY
             CASE WHEN delivery.source = 'subagent'
                        AND delivery.supersedes_input_seq IS NOT NULL
                  THEN 0 ELSE 1 END,
             message.seq,
             delivery.delivery_id
         FOR UPDATE OF delivery
        """,
        thread_uuid,
        int(thread.get("conversation_revision") or 0),
        PINNED_RECOVERY_PARK_REASON,
    )
    result: list[dict[str, Any]] = []
    for raw in rows:
        row = _dict(raw)
        same_runtime = (
            str(row.get("owner_agent_id") or "") == str(agent_uuid)
            and str(row.get("owner_pod_uid") or "") == str(pod_uid)
            and str(row.get("owner_runtime_generation") or "") == str(runtime_uuid)
        )
        # A row this process already published (``queued``) was counted when
        # it was claimed; its chain cannot grow before its own admission.
        if not (same_runtime and str(row.get("state")) == "queued"):
            if _may_reach_recovery_bound(row):
                admissions, _ = await recovery_chain_admissions(
                    conn, delivery_id=row["delivery_id"]
                )
                if admissions >= PINNED_RECOVERY_ADMISSION_LIMIT:
                    await _park_at_recovery_bound(
                        conn,
                        delivery_id=row["delivery_id"],
                        agent_id=agent_uuid,
                        pod_uid=str(pod_uid),
                        runtime_generation=runtime_uuid,
                        admissions=admissions,
                    )
                    continue
        if not same_runtime or str(row.get("state")) == "deferred":
            updated = await conn.fetchrow(
                """
                UPDATE thread_input_deliveries
                   SET state = 'owned', claim_generation = claim_generation + 1,
                       owner_agent_id = $2, owner_pod_uid = $3,
                       owner_runtime_generation = $4,
                       owned_at = statement_timestamp(), queued_at = NULL,
                       deferred_reason = NULL, deferred_at = NULL,
                       updated_at = statement_timestamp()
                 WHERE delivery_id = $1
                   AND state IN ('persisted', 'owned', 'queued', 'deferred')
                RETURNING *
                """,
                row["delivery_id"],
                agent_uuid,
                str(pod_uid),
                runtime_uuid,
            )
            if updated is None:
                continue
            preserved = {
                "seq": row["seq"],
                "role": row["role"],
                "content": row["content"],
            }
            row = {**_dict(updated), **preserved}
        row["message_id"] = str(row["message_id"])
        result.append(row)
    return result


async def reserve_stale_pinned_admissions(
    conn: Any,
    *,
    thread_id: str | UUID,
    agent_id: str | UUID,
    pod_uid: str,
    runtime_generation: str | UUID,
    session_runtime_generation: str | UUID | None = None,
    runtime_attach_token: str | UUID,
) -> dict[str, list[str]]:
    """Serve again, settle or park each event an earlier process admitted.

    Attach runs this after the subagent recovery (whose batch settle
    supersedes an input that delegated) and before restore, so restore's
    exclusion of unadmitted rows strips the transcript copy of every input
    handed back here and the loop sees it once, as new input
    (parallel_subagents.md §14.2, P3). For each ``stale_pinned_admission_sql``
    row:

    * its turn reached its end (``pinned_admission_answered_sql``): settled,
      never answered twice;
    * otherwise, when another input was admitted after it
      (``later_pinned_admission_sql``): settled as history, now, while that
      evidence exists (a hand-back below clears the later row's admission);
    * otherwise, when its recovery chain already holds
      ``PINNED_RECOVERY_ADMISSION_LIMIT`` admissions: parked with one notice;
    * otherwise: handed back to ``owned`` by this exact runtime with a new
      claim generation and no admission receipt, so the ordinary claim serves
      it and the next admission records the turn that runs it.

    Returns the delivery ids by outcome. Raises
    ``InputDeliveryAuthorityLost`` when the caller is not the exact current
    runtime.
    """

    thread_uuid = UUID(str(thread_id))
    agent_uuid = UUID(str(agent_id))
    runtime_uuid = UUID(str(runtime_generation))
    thread = await lock_runtime_authority(
        conn,
        thread_id=thread_uuid,
        agent_id=agent_uuid,
        pod_uid=pod_uid,
        session_runtime_generation=session_runtime_generation or runtime_generation,
        runtime_attach_token=UUID(str(runtime_attach_token)),
    )
    stale = stale_pinned_admission_sql(process_generation="$3::uuid")
    answered = pinned_admission_answered_sql()
    later = later_pinned_admission_sql()
    rows = await conn.fetch(
        "SELECT delivery.*, message.seq, message.role, "
        f"{answered} AS admission_answered, {later} AS later_admission "
        "FROM thread_input_deliveries AS delivery "
        "JOIN thread_messages AS message ON message.id = delivery.message_id "
        "WHERE delivery.thread_id = $1 AND message.rewound_at IS NULL "
        "AND (delivery.conversation_revision=$2 OR "
        "(delivery.conversation_revision IS NULL AND $2=0)) "
        f"AND {stale} "
        "ORDER BY message.seq, delivery.delivery_id "
        "FOR UPDATE OF delivery",
        thread_uuid,
        int(thread.get("conversation_revision") or 0),
        runtime_uuid,
    )
    outcome: dict[str, list[str]] = {
        "settled": [],
        "history": [],
        "reserved": [],
        "parked": [],
    }
    for row in rows:
        delivery_id = row["delivery_id"]
        if bool(row["admission_answered"]) or bool(row["later_admission"]):
            settled = await conn.fetchval(
                "UPDATE thread_input_deliveries SET state = 'settled', "
                "settled_at = statement_timestamp(), "
                "updated_at = statement_timestamp() "
                "WHERE delivery_id = $1 AND execution_lane = 'pinned' "
                "AND state = 'admitted' RETURNING delivery_id",
                delivery_id,
            )
            if settled is not None:
                key = "settled" if bool(row["admission_answered"]) else "history"
                outcome[key].append(str(delivery_id))
            continue
        admissions, _ = await recovery_chain_admissions(conn, delivery_id=delivery_id)
        if admissions >= PINNED_RECOVERY_ADMISSION_LIMIT:
            if (
                await _park_at_recovery_bound(
                    conn,
                    delivery_id=delivery_id,
                    agent_id=agent_uuid,
                    pod_uid=str(pod_uid),
                    runtime_generation=runtime_uuid,
                    admissions=admissions,
                )
                is not None
            ):
                outcome["parked"].append(str(delivery_id))
            continue
        # The row shape requires the receipt off a non-admitted row; the
        # admission count stays, so the bound sees every earlier attempt.
        reserved = await conn.fetchval(
            """
            UPDATE thread_input_deliveries
               SET state = 'owned', claim_generation = claim_generation + 1,
                   owner_agent_id = $2, owner_pod_uid = $3,
                   owner_runtime_generation = $4,
                   owned_at = statement_timestamp(), queued_at = NULL,
                   admitted_at = NULL, admitted_turn_number = NULL,
                   deferred_reason = NULL, deferred_at = NULL,
                   updated_at = statement_timestamp()
             WHERE delivery_id = $1 AND execution_lane = 'pinned'
               AND state = 'admitted'
            RETURNING delivery_id
            """,
            delivery_id,
            agent_uuid,
            str(pod_uid),
            runtime_uuid,
        )
        if reserved is not None:
            outcome["reserved"].append(str(delivery_id))
    return outcome


_PINNED_PARKED_INPUTS_SQL = """
    SELECT delivery.delivery_id, delivery.deferred_at, message.seq
      FROM thread_input_deliveries AS delivery
      JOIN thread_messages AS message ON message.id = delivery.message_id
     WHERE delivery.thread_id = $1
       AND delivery.execution_lane = 'pinned'
       AND delivery.state = 'deferred'
       AND delivery.deferred_reason = $2
       AND message.rewound_at IS NULL
     ORDER BY message.seq, delivery.delivery_id
"""


async def retry_parked_pinned_inputs(conn: Any, *, thread_id: str | UUID) -> list[str]:
    """Owner retry: re-arm every pinned input parked at the recovery bound.

    Each parked row leaves the park (``deferred_reason`` becomes
    ``owner_retry``, still ``deferred``, so the next pinned claim takes it)
    and its whole recovery chain counts from zero again, so the retried turn
    gets the full bound. A live runtime's inbox poll serves it within its
    poll interval; an ended or suspended session serves it at its next attach.
    The caller holds the thread row lock (``thread -> delivery``).
    """

    rows = await conn.fetch(
        _PINNED_PARKED_INPUTS_SQL + " FOR UPDATE OF delivery",
        UUID(str(thread_id)),
        PINNED_RECOVERY_PARK_REASON,
    )
    retried: list[str] = []
    for row in rows:
        _, members = await recovery_chain_admissions(
            conn, delivery_id=row["delivery_id"]
        )
        await conn.execute(
            "UPDATE thread_input_deliveries SET admission_count = 0, "
            "updated_at = statement_timestamp() "
            "WHERE delivery_id = ANY($1::uuid[]) AND admission_count <> 0",
            members,
        )
        updated = await conn.fetchval(
            "UPDATE thread_input_deliveries SET deferred_reason = $2, "
            "deferred_at = statement_timestamp(), "
            "updated_at = statement_timestamp() "
            "WHERE delivery_id = $1 AND execution_lane = 'pinned' "
            "AND state = 'deferred' AND deferred_reason = $3 "
            "RETURNING delivery_id",
            row["delivery_id"],
            PINNED_RECOVERY_RETRY_REASON,
            PINNED_RECOVERY_PARK_REASON,
        )
        if updated is not None:
            retried.append(str(updated))
    return retried


async def mark_input_delivery_queued(
    conn: Any,
    *,
    delivery_id: str | UUID,
    agent_id: str | UUID,
    pod_uid: str,
    runtime_generation: str | UUID,
    session_runtime_generation: str | UUID | None = None,
    runtime_attach_token: str | UUID,
    claim_generation: int,
) -> bool:
    session_runtime = session_runtime_generation or runtime_generation
    row = await conn.fetchrow(
        """
        UPDATE thread_input_deliveries delivery
           SET state = 'queued', queued_at = COALESCE(queued_at, statement_timestamp()),
               updated_at = statement_timestamp()
          FROM threads thread, agents agent
         WHERE delivery.delivery_id = $1
           AND delivery.state IN ('owned', 'queued')
           AND delivery.claim_generation = $2
           AND delivery.owner_agent_id = $3
           AND delivery.owner_pod_uid = $4
           AND delivery.owner_runtime_generation = $5
           AND thread.id = delivery.thread_id
           AND thread.agent_id = delivery.owner_agent_id
           AND thread.runtime_generation = $6
           AND thread.runtime_attach_token = $7
           AND thread.runtime_retirement_token IS NULL
           AND agent.id = delivery.owner_agent_id
           AND agent.thread_id = thread.id
           AND agent.pod_uid = delivery.owner_pod_uid
           AND agent.status NOT IN ('offline', 'deleted')
        RETURNING delivery.delivery_id
        """,
        UUID(str(delivery_id)),
        int(claim_generation),
        UUID(str(agent_id)),
        str(pod_uid),
        UUID(str(runtime_generation)),
        UUID(str(session_runtime)),
        UUID(str(runtime_attach_token)),
    )
    return row is not None


async def transition_input_delivery(
    conn: Any,
    *,
    delivery_id: str | UUID,
    agent_id: str | UUID,
    pod_uid: str,
    runtime_generation: str | UUID,
    session_runtime_generation: str | UUID | None = None,
    runtime_attach_token: str | UUID,
    claim_generation: int,
    transition: str,
    turn_number: int | None = None,
    reason: str | None = None,
) -> bool:
    """CAS one exact owner through admitted, settled, deferred, or cancelled.

    Admission counts one provider admission (``admission_count``, the
    recovery bound's unit); an unadmit before the provider call takes it back.
    """

    session_runtime = session_runtime_generation or runtime_generation
    if transition == "admitted":
        states = ("owned", "queued")
        assignments = (
            "state = 'admitted', admitted_at = statement_timestamp(), "
            "admitted_turn_number = $6, deferred_reason = NULL, "
            "admission_count = delivery.admission_count + 1, "
            "deferred_at = NULL, updated_at = statement_timestamp()"
        )
    elif transition == "settled":
        states = ("admitted",)
        assignments = (
            "state = 'settled', settled_at = statement_timestamp(), "
            "updated_at = statement_timestamp()"
        )
    elif transition == "deferred":
        states = ("owned", "queued")
        assignments = (
            "state = 'deferred', deferred_reason = LEFT(COALESCE($7, 'retryable'), 120), "
            "deferred_at = statement_timestamp(), queued_at = NULL, "
            "updated_at = statement_timestamp()"
        )
    elif transition == "unadmit":
        states = ("admitted",)
        assignments = (
            "state = 'deferred', admitted_at = NULL, admitted_turn_number = NULL, "
            "admission_count = GREATEST(delivery.admission_count - 1, 0), "
            "deferred_reason = LEFT(COALESCE($7, 'provider_not_started'), 120), "
            "deferred_at = statement_timestamp(), queued_at = NULL, "
            "updated_at = statement_timestamp()"
        )
    elif transition == "cancelled":
        states = ("owned", "queued")
        assignments = (
            "state = 'cancelled', cancelled_at = statement_timestamp(), "
            "cancelled_turn_number = $6, "
            "cancelled_reason = LEFT(COALESCE($7, 'human_stop_before_provider'), 120), "
            "queued_at = NULL, deferred_reason = NULL, deferred_at = NULL, "
            "updated_at = statement_timestamp()"
        )
    else:  # pragma: no cover - caller contract
        raise ValueError(f"unsupported input delivery transition: {transition}")

    source_fence = (
        "AND delivery.source = 'direct_human'" if transition == "cancelled" else ""
    )

    update_sql = f"""
        UPDATE thread_input_deliveries delivery
           SET {assignments}
          FROM threads thread, agents agent
         WHERE delivery.delivery_id = $1
           AND delivery.state = ANY($2::text[])
           AND delivery.claim_generation = $3
           AND delivery.owner_agent_id = $4
           AND delivery.owner_pod_uid = $5
           AND delivery.owner_runtime_generation = $8
           {source_fence}
           AND thread.id = delivery.thread_id
           AND thread.agent_id = delivery.owner_agent_id
           AND thread.runtime_generation = $9
           AND thread.runtime_attach_token = $10
           AND thread.runtime_retirement_token IS NULL
           AND agent.id = delivery.owner_agent_id
           AND agent.thread_id = thread.id
           AND agent.pod_uid = delivery.owner_pod_uid
           AND agent.status NOT IN ('offline', 'deleted')
           AND ($6::bigint IS NULL OR TRUE)
           AND ($7::text IS NULL OR TRUE)
        RETURNING delivery.delivery_id, delivery.thread_id
        """
    row = await conn.fetchrow(
        _with_idle_exit(update_sql) if transition == "admitted" else update_sql,
        UUID(str(delivery_id)),
        list(states),
        int(claim_generation),
        UUID(str(agent_id)),
        str(pod_uid),
        turn_number,
        reason,
        UUID(str(runtime_generation)),
        UUID(str(session_runtime)),
        UUID(str(runtime_attach_token)),
    )
    return row is not None


async def get_input_delivery(
    conn: Any, delivery_id: str | UUID
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT delivery.*, message.rewound_at, "
        "thread.conversation_revision AS current_conversation_revision "
        "FROM thread_input_deliveries AS delivery "
        "JOIN thread_messages AS message ON message.id=delivery.message_id "
        "JOIN threads AS thread ON thread.id=delivery.thread_id "
        "WHERE delivery.delivery_id = $1",
        UUID(str(delivery_id)),
    )
    if row is None:
        return None
    result = _dict(row)
    if row["rewound_at"] is not None:
        result["execution_disposition"] = (
            "historical"
            if str(row["state"] or "") in {"admitted", "settled"}
            else "superseded"
        )
    else:
        result["execution_disposition"] = "current"
    return result
