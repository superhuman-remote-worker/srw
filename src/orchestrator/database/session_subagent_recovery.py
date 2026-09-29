"""Session subagent recovery: the facts of the parent turn, and the batch settle.

When the executor running a session dies while children it delegated to are
running or finished, its successor recovers them before the session resumes.
Supersession of the abandoned input, the stateless input watermark and the
pinned source delivery are facts of the parent TURN, not of one child. The
helpers here compute and apply them once, inside the caller's transaction and
after the caller proved the parent authority, so every lock follows the order
parent, then run queue (stateless) or agent (pinned), then children.

Two callers share them:

* ``PostgresDB.terminalize_session_subagent_thread`` recovers one child
  (``foreground_orphan_recovery``). That single-member path is unchanged.
* :func:`settle_session_subagent_batch` settles a whole delegation turn in one
  transaction: one tool result per undelivered ``delegate_agent`` call, in
  provider order, and one continuation that supersedes the abandoned input.

Design: knowledge-base/knowledge/features/parallel_subagents.md §5.1–§5.6.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from uuid import UUID

from shared.session_subagent_authority import (
    SessionParentAuthority,
    SessionParentAuthorityRefused,
    require_session_parent_authority,
)
from shared.session_subagent_batch import (
    CALL_DECLINED,
    CALL_DELIVERED,
    CALL_ENDED,
    CALL_LIVE,
    CALL_NOT_STARTED,
    CALL_RETIRED,
    DECLINED_RESULT_TEXT,
    RESULT_CLASS_BY_CALL_CLASS,
    batch_continuation_text,
    continuation_metrics,
    not_started_result_text,
    result_metrics,
    retired_result_text,
    session_subagent_batch_delivery_id,
    session_subagent_batch_result_id,
)

_RETIRED_OUTCOME = "cancelled:parent_retired"
_PARENT_RESTART_STATUS = "interrupted"
_PARENT_RESTART_OUTCOME = "interrupted:parent_restart"


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _json_value(value: Any) -> Any:
    if isinstance(value, (str, bytes)):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return None
    return value


# ---------------------------------------------------------------------------
# Turn-level facts, shared with the single-member recovery path
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecoveryParentInput:
    """The input a recovered turn answered, and whether it is already consumed."""

    message_id: UUID
    seq: int
    role: str
    delivery_id: UUID | None
    delivery_state: str | None
    queue_consumed: int | None
    source_already_complete: bool
    # Stateless recovery reuses the abandoned turn's number for what it writes.
    recovery_turn_number: int | None


async def load_recovery_parent_input(
    conn: Any,
    *,
    execution_lane: str,
    parent_thread_id: UUID,
    parent_input_message_id: UUID,
    parent_iteration: int,
) -> RecoveryParentInput:
    """The exact live input of the recovered turn; refuse a rewound or foreign one."""

    parent_input = await conn.fetchrow(
        """
        SELECT message.seq, message.role, delivery.delivery_id,
               delivery.state AS delivery_state
          FROM thread_messages AS message
          LEFT JOIN thread_input_deliveries AS delivery
            ON delivery.thread_id = message.thread_id
           AND delivery.message_id = message.id
         WHERE message.id = $1
           AND message.thread_id = $2
           AND message.turn_number = $3
           AND message.rewound_at IS NULL
           AND (
               message.role = 'human'
               OR (
                   message.role = 'event'
                   AND delivery.state IN ('admitted', 'settled')
               )
         )
        """,
        parent_input_message_id,
        parent_thread_id,
        parent_iteration,
    )
    if parent_input is None:
        raise ValueError("foreground recovery parent input is missing")
    supersedes_input_seq = int(parent_input["seq"])
    raw_source_delivery_id = parent_input.get("delivery_id")
    source_delivery_id = (
        UUID(str(raw_source_delivery_id))
        if raw_source_delivery_id is not None
        else None
    )
    source_delivery_state = str(parent_input.get("delivery_state") or "") or None
    if parent_input.get("role") == "event":
        if source_delivery_id is None:
            raise ValueError("foreground recovery event input has no delivery")
    if execution_lane == "stateless":
        queue_consumed = await conn.fetchval(
            "SELECT consumed_seq FROM run_queue WHERE unit_id=$1",
            parent_thread_id,
        )
        source_already_complete = (
            queue_consumed is not None and int(queue_consumed) >= supersedes_input_seq
        )
    else:
        queue_consumed = None
        source_already_complete = source_delivery_state == "settled"
    return RecoveryParentInput(
        message_id=parent_input_message_id,
        seq=supersedes_input_seq,
        role=str(parent_input.get("role") or ""),
        delivery_id=source_delivery_id,
        delivery_state=source_delivery_state,
        queue_consumed=int(queue_consumed) if queue_consumed is not None else None,
        source_already_complete=source_already_complete,
        recovery_turn_number=(
            parent_iteration if execution_lane == "stateless" else None
        ),
    )


async def parent_turn_completed(
    conn: Any,
    *,
    parent_thread_id: UUID,
    parent_iteration: int,
    parent_ai_message_id: UUID,
    parent_ai_seq: int,
) -> bool:
    """Whether the turn provably answered its input after the delegating call.

    Two durable proofs: the loop's own ``turn.completed`` frame for that turn,
    journaled after the delegating AI row (the only evidence when the final
    answer carries its own tool call — ad7eb761), or the turn's own final
    answer row — same turn_number, after the delegating call, no tool calls.
    """

    return bool(
        await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM thread_events AS frame
                 WHERE frame.thread_id = $1
                   AND frame.kind = 'turn.completed'
                   AND (frame.payload->>'turn_id') = $2::text
                   AND frame.created_at >= (
                       SELECT created_at
                         FROM thread_messages
                        WHERE id = $3
                   )
            )
            OR EXISTS (
                SELECT 1
                  FROM thread_messages AS answer
                 WHERE answer.thread_id = $1
                   AND answer.role = 'ai'
                   AND answer.turn_number = $5
                   AND answer.seq > $4
                   AND answer.rewound_at IS NULL
                   AND jsonb_array_length(
                       COALESCE(answer.tool_calls, '[]'::jsonb)
                   ) = 0
            )
            """,
            parent_thread_id,
            str(int(parent_iteration)),
            parent_ai_message_id,
            int(parent_ai_seq),
            int(parent_iteration),
        )
    )


async def advance_recovery_watermark(
    conn: Any,
    authority: SessionParentAuthority,
    *,
    parent_thread_id: UUID,
    parent_input: RecoveryParentInput,
) -> None:
    """Consume the abandoned stateless input exactly once and never out of order."""

    oldest_pending = await conn.fetchval(
        """
        SELECT min(message.seq)
          FROM thread_messages AS message
          LEFT JOIN thread_input_deliveries AS delivery
            ON delivery.message_id = message.id
           AND delivery.thread_id = message.thread_id
         WHERE message.thread_id = $1
           AND message.seq > COALESCE($2, -1)
           AND message.rewound_at IS NULL
           AND (
               message.seq = $3
               OR
               message.role = 'human'
               OR (
                   message.role = 'event'
                   AND delivery.execution_lane = 'stateless'
                   AND delivery.state IN (
                       'persisted', 'queued', 'deferred'
                   )
               )
           )
        """,
        parent_thread_id,
        parent_input.queue_consumed,
        parent_input.seq,
    )
    if oldest_pending != parent_input.seq:
        raise ValueError("stateless foreground recovery would skip another input")
    advanced = await conn.fetchval(
        """
        UPDATE run_queue
           SET consumed_seq = GREATEST(
                   COALESCE(consumed_seq, -1), $2
               )
         WHERE unit_id = $1
           AND unit_kind = 'session_turn'
           AND state = 'leased'
           AND lease_token = $3
           AND leased_by = $4
           AND input_delivery_capable_lease_token = $3
        RETURNING consumed_seq
        """,
        parent_thread_id,
        parent_input.seq,
        int(authority.lease_token or 0),
        str(authority.executor_id),
    )
    if advanced is None:
        raise SessionParentAuthorityRefused("stateless_parent_not_current")


async def settle_recovery_source(
    conn: Any,
    *,
    parent_thread_id: UUID,
    parent_input: RecoveryParentInput,
) -> None:
    """Settle the admitted source delivery of the abandoned input."""

    settled_source = await conn.fetchval(
        """
        UPDATE thread_input_deliveries
           SET state = 'settled',
               settled_at = COALESCE(
                   settled_at, CURRENT_TIMESTAMP
               ),
               updated_at = statement_timestamp()
         WHERE delivery_id = $1
           AND thread_id = $2
           AND message_id = (
               SELECT id FROM thread_messages
                WHERE thread_id = $2 AND seq = $3
           )
           AND state = 'admitted'
        RETURNING delivery_id
        """,
        parent_input.delivery_id,
        parent_thread_id,
        parent_input.seq,
    )
    if settled_source is None:
        raise ValueError("foreground recovery lost source input authority")


async def final_parent_response_seq(
    conn: Any,
    *,
    execution_lane: str,
    parent_thread_id: UUID,
    parent_input: RecoveryParentInput,
    parent_iteration: int,
    after_seq: int,
    before_seq: int | None = None,
) -> int | None:
    """The turn's final answer after the delegating call, if one is durable.

    A final answer counts only behind an authoritative turn boundary: never
    declare a turn finished on a half-written answer. ``before_seq`` ends the
    search at the continuation that supersedes this input, whose recovery
    turn reuses the turn number on the stateless lane.
    """

    stateless_finalized_end_seq: int | None = None
    stateless_completion_effect_present = False
    if execution_lane == "stateless":
        stateless_completion_effect_present = bool(
            await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                      FROM thread_messages AS source
                      JOIN completion_effects AS effect
                        ON effect.producer_kind = 'session_turn'
                       AND effect.producer_id =
                               source.turn_execution_id
                       AND effect.effect_name =
                               'final_memory_extraction'
                     WHERE source.id = $2
                       AND source.thread_id = $1
                )
                """,
                parent_thread_id,
                parent_input.message_id,
            )
        )
        stateless_finalized_end_seq = await conn.fetchval(
            """
            SELECT CASE WHEN count(*) = 1 THEN min(
                       (effect.detail->>'end_seq')::bigint
                   ) END
              FROM thread_messages AS source
              JOIN completion_effects AS effect
                ON effect.producer_kind = 'session_turn'
               AND effect.producer_id = source.turn_execution_id
               AND effect.effect_name = 'final_memory_extraction'
               AND effect.scope_id = $1
               AND effect.effect_group = 'memory_extraction'
             WHERE source.id = $2
               AND source.thread_id = $1
               AND source.seq = $3
               AND source.turn_execution_id IS NOT NULL
               AND effect.detail @> jsonb_build_object(
                   'input_message_id', source.id,
                   'turn_number', $4::integer
               )
               AND jsonb_typeof(
                   effect.detail->'boundary_seq'
               ) = 'number'
               AND jsonb_typeof(
                   effect.detail->'end_seq'
               ) = 'number'
               AND (effect.detail->>'boundary_seq')::bigint =
                       source.seq
            """,
            parent_thread_id,
            parent_input.message_id,
            parent_input.seq,
            parent_iteration,
        )
    final_seq = await conn.fetchval(
        """
                SELECT min(response.seq)
                  FROM thread_messages AS response
                 WHERE response.thread_id = $1
                   AND response.role = 'ai'
                   AND response.turn_number = $2
                   AND response.seq > $3
                   AND ($4::bigint IS NULL OR response.seq < $4::bigint)
                   AND response.rewound_at IS NULL
                   AND jsonb_array_length(
                       COALESCE(response.tool_calls, '[]'::jsonb)
                   ) = 0
        """,
        parent_thread_id,
        parent_iteration,
        int(after_seq),
        before_seq,
    )
    if final_seq is not None and not parent_input.source_already_complete:
        finalized_turn_boundary = False
        if execution_lane == "stateless":
            if stateless_completion_effect_present:
                finalized_turn_boundary = bool(
                    stateless_finalized_end_seq is not None
                    and int(stateless_finalized_end_seq) >= int(final_seq)
                )
            else:
                # Incremental final-AI persistence can win just before the
                # batch reconcile that creates the memory effect. The exact
                # live lease, oldest source, immutable parent call, and unique
                # final row are sufficient to checkpoint the response without
                # a second provider turn.
                finalized_turn_boundary = True
        else:
            # Pinned delivery settlement is the authoritative
            # no-second-provider boundary.  Memory/Git effects run after the
            # final AI row and are healable; a crash between them must not
            # replay the provider.
            finalized_turn_boundary = parent_input.delivery_state in {
                "admitted",
                "settled",
            }
        if not finalized_turn_boundary:
            raise ValueError(
                "foreground recovery found a final parent response "
                "without an authoritative settled turn boundary"
            )
    return int(final_seq) if final_seq is not None else None


async def end_session_child(
    conn: Any,
    *,
    child_id: UUID,
    parent_thread_id: UUID,
    runtime_generation: UUID,
    subagent_status: str,
    outcome: str | None,
    turns: int | None,
    tokens: int | None,
    report_path: str | None,
    error: str | None,
) -> bool:
    """End one live child generation; ``False`` when it was not live."""

    ended = await conn.fetchval(
        """
        UPDATE threads
           SET status = 'ended',
               subagent_status = $4,
               subagent_outcome = COALESCE($5, subagent_outcome),
               total_turns = COALESCE($6, total_turns),
               total_tokens = COALESCE($7, total_tokens),
               report_path = COALESCE($8, report_path),
               subagent_error = COALESCE($9, subagent_error),
               ended_at = COALESCE(ended_at, CURRENT_TIMESTAMP),
               last_activity = CURRENT_TIMESTAMP
         WHERE id = $1
           AND kind = 'subagent'
           AND parent_job_id IS NULL
           AND parent_thread_id = $2
           AND runtime_generation = $3
           AND status <> 'ended'
        RETURNING id
        """,
        child_id,
        parent_thread_id,
        runtime_generation,
        subagent_status,
        outcome,
        turns,
        tokens,
        report_path,
        error,
    )
    return ended is not None


async def stamp_recovered_child(
    conn: Any, child_id: UUID, runtime_generation: UUID | str
) -> None:
    """Close one child generation for recovery; it leaves the live list."""

    await conn.execute(
        """
        UPDATE threads
           SET metadata = jsonb_set(
               COALESCE(metadata, '{}'::jsonb),
               '{subagent_foreground_recovery_generation}',
               to_jsonb($2::text),
               true
           )
         WHERE id = $1
        """,
        child_id,
        str(runtime_generation),
    )


def delivery_disposition(delivery: Mapping[str, Any]) -> str:
    """Whether a stored delivery still runs, or a rewind made it history."""

    if delivery.get("execution_disposition"):
        return str(delivery["execution_disposition"])
    if delivery.get("rewound_at") is not None:
        if str(delivery.get("state") or "") in {"admitted", "settled"}:
            return "historical"
        return "superseded"
    return "current"


# ---------------------------------------------------------------------------
# The delegation manifest of one turn
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DelegationCall:
    """One ``delegate_agent`` call of the recovered turn and its class (§5.2)."""

    index: int
    tool_call_id: str
    parent_ai_message_id: UUID
    parent_ai_seq: int
    args: Mapping[str, Any]
    call_class: str
    child: Mapping[str, Any] | None
    permission_status: str | None

    @property
    def child_live(self) -> bool:
        return self.child is not None and str(self.child["status"]) != "ended"

    @property
    def needs_entry(self) -> bool:
        """The successor names this child: it is exactly a live-list candidate."""

        return self.child is not None and (
            self.child_live or self.call_class == CALL_ENDED
        )

    @property
    def needs_message(self) -> bool:
        """The successor supplies the text of this call's result."""

        return self.call_class in {CALL_ENDED, CALL_LIVE}

    def view(self) -> dict[str, Any]:
        child = self.child or {}
        subagent_type = child.get("subagent_type") or self.args.get("subagent_type")
        description = self.args.get("description")
        return {
            "index": self.index,
            "tool_call_id": self.tool_call_id,
            "parent_ai_message_id": str(self.parent_ai_message_id),
            "class": self.call_class,
            "subagent_type": str(subagent_type) if subagent_type else None,
            "description": str(description) if description else None,
            "thread_id": str(child["id"]) if child else None,
            "runtime_generation": (str(child["runtime_generation"]) if child else None),
            "handle": child.get("subagent_handle") if child else None,
            "subagent_status": child.get("subagent_status") if child else None,
            "outcome": child.get("subagent_outcome") if child else None,
            "needs_entry": self.needs_entry,
            "needs_message": self.needs_message,
        }


# Where the rows of one input end: at the continuation that supersedes it,
# and nowhere else. A stateless recovery turn reuses the abandoned turn's
# number, so its rows follow that continuation under the same number. Another
# input row of the same number is not a boundary: the pinned runtime numbers
# input queued before a turn as ``turn_count + 1`` until it runs, so a message
# typed before turn T began carries T while turn T runs.
_INPUT_END_SQL = """
SELECT min(continuation.seq)
  FROM thread_messages AS continuation
  JOIN thread_input_deliveries AS delivery
    ON delivery.message_id = continuation.id
   AND delivery.thread_id = continuation.thread_id
 WHERE continuation.thread_id = $1
   AND continuation.seq > $2
   AND continuation.rewound_at IS NULL
   AND delivery.source = 'subagent'
   AND delivery.supersedes_input_seq = $2
"""


async def load_delegation_manifest(
    conn: Any,
    *,
    parent_thread_id: UUID,
    parent_input: RecoveryParentInput,
    parent_iteration: int,
    lock_children: bool,
) -> list[DelegationCall]:
    """Every ``delegate_agent`` call of the turn's durable AI rows, classified.

    The turn is the input's AI rows of the same turn number, up to the
    continuation that supersedes the input, if one exists (``_INPUT_END_SQL``).
    Its calls are the manifest, in provider order; a
    call queued behind the cap never got a child row, so membership comes
    from the calls and not from the children (§5.1, F4). Children join
    through the ``(parent_thread_id, parent_tool_call_id)`` unique index and
    are locked in spawn order, the order retirement uses.
    """

    input_end_seq = await conn.fetchval(
        _INPUT_END_SQL, parent_thread_id, parent_input.seq
    )
    rows = await conn.fetch(
        """
        SELECT call_row.id AS ai_id, call_row.seq AS ai_seq,
               tool_call.value AS tool_call
          FROM thread_messages AS call_row
          CROSS JOIN LATERAL jsonb_array_elements(
              CASE WHEN jsonb_typeof(call_row.tool_calls) = 'array'
                   THEN call_row.tool_calls ELSE '[]'::jsonb END
          ) WITH ORDINALITY AS tool_call(value, ordinality)
         WHERE call_row.thread_id = $1
           AND call_row.role = 'ai'
           AND call_row.turn_number = $2
           AND call_row.seq > $3
           AND ($4::bigint IS NULL OR call_row.seq < $4::bigint)
           AND call_row.rewound_at IS NULL
           AND tool_call.value->>'name' = 'delegate_agent'
         ORDER BY call_row.seq, tool_call.ordinality
        """,
        parent_thread_id,
        parent_iteration,
        parent_input.seq,
        input_end_seq,
    )
    manifest: list[tuple[str, UUID, int, dict[str, Any]]] = []
    for row in rows:
        tool_call = _json_object(row["tool_call"])
        call_id = tool_call.get("id")
        if not isinstance(call_id, str) or not call_id.strip():
            raise ValueError("a delegation call of the recovered turn has no id")
        args = tool_call.get("args")
        manifest.append(
            (
                call_id,
                UUID(str(row["ai_id"])),
                int(row["ai_seq"]),
                args if isinstance(args, dict) else {},
            )
        )
    call_ids = [call_id for call_id, *_ in manifest]
    if len(set(call_ids)) != len(call_ids):
        raise ValueError("the recovered turn repeats a delegation call id")
    if not call_ids:
        return []

    delivered = {
        str(value)
        for value in await conn.fetchval(
            """
            SELECT COALESCE(array_agg(DISTINCT result.tool_call_id), '{}')
              FROM thread_messages AS result
             WHERE result.thread_id = $1
               AND result.role = 'tool'
               AND result.rewound_at IS NULL
               AND result.tool_call_id = ANY($2::text[])
            """,
            parent_thread_id,
            call_ids,
        )
    }
    child_rows = await conn.fetch(
        f"""
        SELECT id, runtime_generation, status, subagent_status,
               subagent_outcome, subagent_error, report_path, total_turns,
               total_tokens, subagent_handle, subagent_type,
               parent_tool_call_id, metadata
          FROM threads
         WHERE kind = 'subagent'
           AND parent_job_id IS NULL
           AND parent_thread_id = $1
           AND parent_tool_call_id = ANY($2::text[])
         ORDER BY created_at, id
         {"FOR UPDATE" if lock_children else ""}
        """,
        parent_thread_id,
        call_ids,
    )
    permissions = {
        str(row["tool_call_id"]): str(row["status"] or "")
        for row in await conn.fetch(
            """
            SELECT DISTINCT ON (tool_call_id) tool_call_id, status
              FROM thread_permission_requests
             WHERE thread_id = $1
               AND tool_call_id = ANY($2::text[])
             ORDER BY tool_call_id, requested_at DESC, id DESC
            """,
            parent_thread_id,
            call_ids,
        )
    }
    children: dict[str, dict[str, Any]] = {}
    for raw in child_rows:
        child = dict(raw)
        metadata = _json_object(child.get("metadata"))
        spawn = metadata.get("subagent")
        spawn = spawn if isinstance(spawn, dict) else {}
        if spawn.get("run_in_background") is True:
            raise ValueError("a batch settle covers foreground children only")
        if (
            str(spawn.get("parent_input_message_id") or "")
            != str(parent_input.message_id)
            or spawn.get("parent_iteration") != parent_iteration
        ):
            raise ValueError("a delegation child names another parent turn")
        status = str(child.get("status") or "")
        subagent_status = str(child.get("subagent_status") or "")
        if status != "ended" and not (
            status in {"created", "active"} and subagent_status in {"queued", "running"}
        ):
            raise ValueError("a delegation child has an inconsistent lifecycle")
        child["recovery_stamp"] = metadata.get(
            "subagent_foreground_recovery_generation"
        )
        children[str(child["parent_tool_call_id"])] = child

    calls: list[DelegationCall] = []
    for index, (call_id, ai_id, ai_seq, args) in enumerate(manifest):
        child = children.get(call_id)
        if call_id in delivered:
            call_class = CALL_DELIVERED
        elif child is None:
            call_class = (
                CALL_DECLINED
                if permissions.get(call_id) == "denied"
                else CALL_NOT_STARTED
            )
        elif str(child["status"]) != "ended":
            call_class = CALL_LIVE
        elif child.get("subagent_outcome") == _RETIRED_OUTCOME:
            call_class = CALL_RETIRED
        elif str(child.get("recovery_stamp") or "") == str(child["runtime_generation"]):
            # An earlier recovery already closed this generation.
            call_class = CALL_DELIVERED
        else:
            call_class = CALL_ENDED
        calls.append(
            DelegationCall(
                index=index,
                tool_call_id=call_id,
                parent_ai_message_id=ai_id,
                parent_ai_seq=ai_seq,
                args=args,
                call_class=call_class,
                child=child,
                permission_status=permissions.get(call_id),
            )
        )
    return calls


def _parent_turn_identity(row: Mapping[str, Any]) -> tuple[str, int] | None:
    spawn = _json_object(row.get("metadata")).get("subagent")
    if not isinstance(spawn, dict):
        return None
    iteration = spawn.get("parent_iteration")
    if isinstance(iteration, bool) or not isinstance(iteration, int) or iteration <= 0:
        return None
    try:
        input_id = str(UUID(str(spawn.get("parent_input_message_id"))))
    except (TypeError, ValueError, AttributeError):
        return None
    return input_id, iteration


async def plan_recovery_turns(
    conn: Any,
    authority: SessionParentAuthority,
    *,
    parent_thread_id: UUID,
    candidates: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """One plan per turn that owns a live-list candidate (§6.1).

    A plan names the turn, the continuation a settle would write, and every
    delegation call of the turn with its class, so the successor knows which
    members it must name and which need its text. A turn whose evidence does
    not add up carries ``error`` and no calls; the listing itself never fails
    because of a plan, since agents that predate plans only read the rows.
    """

    turns: dict[tuple[str, int], None] = {}
    for row in candidates:
        identity = _parent_turn_identity(row)
        if identity is not None:
            turns.setdefault(identity)
    plans: list[dict[str, Any]] = []
    for input_id, iteration in turns:
        plan: dict[str, Any] = {
            "parent_input_message_id": input_id,
            "parent_iteration": iteration,
            "delivery_id": str(
                session_subagent_batch_delivery_id(parent_thread_id, input_id)
            ),
            "supersedes_input_seq": None,
            "calls": [],
        }
        try:
            parent_input = await load_recovery_parent_input(
                conn,
                execution_lane=authority.execution_lane,
                parent_thread_id=parent_thread_id,
                parent_input_message_id=UUID(input_id),
                parent_iteration=iteration,
            )
            calls = await load_delegation_manifest(
                conn,
                parent_thread_id=parent_thread_id,
                parent_input=parent_input,
                parent_iteration=iteration,
                lock_children=False,
            )
        except ValueError as exc:
            plan["error"] = str(exc)
        else:
            plan["supersedes_input_seq"] = parent_input.seq
            plan["calls"] = [call.view() for call in calls]
        plans.append(plan)
    return plans


# ---------------------------------------------------------------------------
# The batch settle
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BatchMember:
    """What the successor states about one child it names (a request entry)."""

    thread_id: UUID
    runtime_generation: UUID
    subagent_status: str
    outcome: str | None
    turns: int | None
    tokens: int | None
    report_path: str | None
    error: str | None
    message: str | None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "BatchMember":
        try:
            thread_id = UUID(str(value.get("thread_id")))
            generation = UUID(str(value.get("runtime_generation")))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("a batch member needs an exact child generation") from exc
        status = str(value.get("subagent_status") or "").strip()
        if not status or status in {"queued", "running"}:
            raise ValueError("terminal subagent status must not be queued or running")
        return cls(
            thread_id=thread_id,
            runtime_generation=generation,
            subagent_status=status,
            outcome=value.get("outcome"),
            turns=value.get("turns"),
            tokens=value.get("tokens"),
            report_path=value.get("report_path"),
            error=value.get("error"),
            message=value.get("message"),
        )


def _validate_members(
    calls: Sequence[DelegationCall], members: Mapping[UUID, BatchMember]
) -> None:
    """The per-member checks of §5.3 step 8, all before the first write."""

    for call in calls:
        if not call.needs_entry:
            continue
        child = call.child or {}
        member = members[UUID(str(child["id"]))]
        if call.child_live:
            if (
                member.subagent_status != _PARENT_RESTART_STATUS
                or str(member.outcome or "") != _PARENT_RESTART_OUTCOME
            ):
                raise ValueError(
                    "live foreground orphan recovery requires an interrupted "
                    "parent-restart outcome"
                )
        else:
            if str(child.get("subagent_status") or "") != member.subagent_status:
                raise ValueError(
                    "terminal session child retry changed its terminal status"
                )
            for field, supplied, stored in (
                ("outcome", member.outcome, child.get("subagent_outcome")),
                ("turns", member.turns, child.get("total_turns")),
                ("tokens", member.tokens, child.get("total_tokens")),
                ("report_path", member.report_path, child.get("report_path")),
                ("error", member.error, child.get("subagent_error")),
            ):
                if supplied is not None and supplied != stored:
                    raise ValueError(
                        "terminal session child retry changed its " + field
                    )
        if call.needs_message:
            if not str(member.message or "").strip():
                raise ValueError("a delivered session child needs a message")
        elif member.message is not None:
            raise ValueError(
                "a session child whose result is durable must not carry a message"
            )


def _delivery_view(delivery: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": str(delivery["delivery_id"]),
        "source": "subagent",
        "role": "event",
        "message_id": str(delivery.get("message_id") or ""),
        "state": str(delivery.get("state") or ""),
        "execution_disposition": delivery_disposition(delivery),
    }


async def settle_session_subagent_batch(
    conn: Any,
    authority: SessionParentAuthority,
    *,
    parent_thread_id: UUID,
    parent_input_message_id: UUID,
    parent_iteration: int,
    members: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Settle one delegation turn of a dead executor, inside one transaction.

    The step order is §5.3's. Nothing is written until every check passed; a
    refusal after a write can only be an exception, which rolls the whole
    transaction back. No provider or network call happens here.

    Results: ``applied`` (results and continuation written now),
    ``idempotent`` (a continuation for this input already exists; it is
    returned and nothing is touched), ``already_delivered`` (the turn has its
    answer, or every call its result: members are closed, nothing is queued),
    ``nothing_to_recover`` (no call of the turn ever got a child), and
    ``stale`` (the request does not name exactly the server's members; the
    server's view is returned and nothing is written).
    """

    if (
        isinstance(parent_iteration, bool)
        or not isinstance(parent_iteration, int)
        or parent_iteration <= 0
    ):
        raise ValueError("foreground recovery has no exact parent turn")
    parsed_members = [BatchMember.from_mapping(member) for member in members]
    by_child: dict[UUID, BatchMember] = {}
    for member in parsed_members:
        if member.thread_id in by_child:
            raise ValueError("a batch settle names one child twice")
        by_child[member.thread_id] = member
    lane = authority.execution_lane
    delivery_id = session_subagent_batch_delivery_id(
        parent_thread_id, parent_input_message_id
    )
    identity = {
        "parent_thread_id": str(parent_thread_id),
        "parent_input_message_id": str(parent_input_message_id),
        "parent_iteration": parent_iteration,
    }

    # 1. Authority, with the delivery-event locks: the parent row, then the
    #    run queue (stateless) or the agent row (pinned).
    await require_session_parent_authority(
        conn, authority, parent_thread_id=parent_thread_id, delivery_event=True
    )

    # 2. The turn is the idempotency key: at most one continuation per
    #    superseded input, whoever wrote it, rewound or not.
    input_seq = await conn.fetchval(
        "SELECT seq FROM thread_messages WHERE id=$1 AND thread_id=$2",
        parent_input_message_id,
        parent_thread_id,
    )
    if input_seq is None:
        raise ValueError("foreground recovery parent input is missing")
    existing = await conn.fetchrow(
        """
        SELECT delivery.*, message.rewound_at
          FROM thread_input_deliveries AS delivery
          JOIN thread_messages AS message ON message.id = delivery.message_id
         WHERE delivery.thread_id = $1
           AND delivery.source = 'subagent'
           AND delivery.supersedes_input_seq = $2
         ORDER BY message.seq, delivery.delivery_id
         LIMIT 1
        """,
        parent_thread_id,
        int(input_seq),
    )
    if existing is not None:
        view = _delivery_view(existing)
        return {
            "result": "idempotent",
            **identity,
            "supersedes_input_seq": int(input_seq),
            "delivery_id": view["id"],
            "delivery_state": view["state"],
            "execution_disposition": view["execution_disposition"],
            "delivery": view,
            "calls": [],
        }

    # 3.–5. The manifest, its children locked in spawn order, every call
    #    classified from durable facts.
    parent_input = await load_recovery_parent_input(
        conn,
        execution_lane=lane,
        parent_thread_id=parent_thread_id,
        parent_input_message_id=parent_input_message_id,
        parent_iteration=parent_iteration,
    )
    calls = await load_delegation_manifest(
        conn,
        parent_thread_id=parent_thread_id,
        parent_input=parent_input,
        parent_iteration=parent_iteration,
        lock_children=True,
    )
    identity["supersedes_input_seq"] = parent_input.seq

    # 6. The request must name exactly the members the server would settle,
    #    each at its current generation. Any difference writes nothing.
    expected = {
        UUID(str(call.child["id"])): UUID(str(call.child["runtime_generation"]))
        for call in calls
        if call.needs_entry and call.child is not None
    }
    named = {
        thread_id: member.runtime_generation for thread_id, member in by_child.items()
    }
    if expected != named:
        return {
            "result": "stale",
            "reason": (
                "members_differ"
                if set(expected) != set(named)
                else "generation_differs"
            ),
            **identity,
            "delivery_id": None,
            "calls": [call.view() for call in calls],
        }
    _validate_members(calls, by_child)

    if not any(call.child is not None for call in calls):
        # Nothing was spent on children; the input replays as a normal turn.
        return {
            "result": "nothing_to_recover",
            **identity,
            "delivery_id": None,
            "calls": [call.view() for call in calls],
        }

    # 7. The turn-level facts, once for the whole turn.
    first = calls[0]
    input_end_seq = await conn.fetchval(
        _INPUT_END_SQL, parent_thread_id, parent_input.seq
    )
    answered = (
        await final_parent_response_seq(
            conn,
            execution_lane=lane,
            parent_thread_id=parent_thread_id,
            parent_input=parent_input,
            parent_iteration=parent_iteration,
            after_seq=first.parent_ai_seq,
            before_seq=input_end_seq,
        )
        is not None
    ) or await parent_turn_completed(
        conn,
        parent_thread_id=parent_thread_id,
        parent_iteration=parent_iteration,
        parent_ai_message_id=first.parent_ai_message_id,
        parent_ai_seq=first.parent_ai_seq,
    )
    undelivered = [call for call in calls if call.call_class != CALL_DELIVERED]
    settle = not answered and bool(undelivered)

    # 8. Apply. End every live member as interrupted by the restart.
    for call in calls:
        if not (call.needs_entry and call.child_live and call.child is not None):
            continue
        member = by_child[UUID(str(call.child["id"]))]
        if not await end_session_child(
            conn,
            child_id=UUID(str(call.child["id"])),
            parent_thread_id=parent_thread_id,
            runtime_generation=member.runtime_generation,
            subagent_status=member.subagent_status,
            outcome=member.outcome,
            turns=member.turns,
            tokens=member.tokens,
            report_path=member.report_path,
            error=member.error,
        ):
            raise RuntimeError("batch settle lost a locked child generation")

    written: dict[str, UUID] = {}
    delivery: Mapping[str, Any] | None = None
    if settle:
        from shared.persistent_input_delivery import (
            message_row_id,
            persist_input_delivery,
        )

        # What the parent will see: one result per undelivered call, in
        # provider order, each stamped with the abandoned turn's number so a
        # restore places it behind its call and ahead of input typed during
        # the batch (invariant 12).
        interrupted = not_started = declined = retired = 0
        for call in calls:
            if call.permission_status == "denied" and call.call_class in {
                CALL_DELIVERED,
                CALL_DECLINED,
            }:
                declined += 1
            if call.call_class == CALL_DELIVERED:
                continue
            child = call.child or {}
            member = by_child.get(UUID(str(child["id"]))) if child else None
            subagent_status = child.get("subagent_status") if child else None
            report_path = child.get("report_path") if child else None
            if call.call_class in {CALL_ENDED, CALL_LIVE}:
                if member is None:  # step 6 proved every such child is named
                    raise RuntimeError("batch settle lost a named member")
                content = str(member.message)
                if call.call_class == CALL_LIVE:
                    interrupted += 1
                    subagent_status = member.subagent_status
                    report_path = member.report_path or report_path
            elif call.call_class == CALL_RETIRED:
                retired += 1
                content = retired_result_text(
                    handle=str(child.get("subagent_handle") or ""),
                    subagent_type=str(child.get("subagent_type") or ""),
                    turns=int(child.get("total_turns") or 0),
                    tokens=int(child.get("total_tokens") or 0),
                )
            elif call.call_class == CALL_DECLINED:
                content = DECLINED_RESULT_TEXT
            else:
                not_started += 1
                content = not_started_result_text()
            view = call.view()
            row_id = session_subagent_batch_result_id(
                parent_thread_id, parent_input_message_id, call.tool_call_id
            )
            await conn.execute(
                """
                INSERT INTO thread_messages
                    (id, thread_id, role, content, tool_call_id, turn_number,
                     metrics)
                VALUES ($1, $2, 'tool', $3, $4, $5, $6::jsonb)
                """,
                row_id,
                parent_thread_id,
                content,
                call.tool_call_id,
                parent_iteration,
                json.dumps(
                    result_metrics(
                        result_class=RESULT_CLASS_BY_CALL_CLASS[call.call_class],
                        tool_call_id=call.tool_call_id,
                        delivery_id=delivery_id,
                        thread_id=view["thread_id"],
                        handle=view["handle"],
                        subagent_type=view["subagent_type"],
                        subagent_status=subagent_status,
                        report_path=report_path,
                    )
                ),
            )
            written[call.tool_call_id] = row_id

        # Then the one continuation that supersedes the abandoned input. It
        # keeps ``source='subagent'`` and ``supersedes_input_seq``, so the
        # stateless executor serves it before input typed during the batch,
        # and it carries the abandoned turn's number on both lanes.
        pinned = lane == "pinned"
        delivery = await persist_input_delivery(
            conn,
            thread_id=parent_thread_id,
            delivery_id=delivery_id,
            role="event",
            content=batch_continuation_text(
                calls=len(calls),
                interrupted=interrupted,
                not_started=not_started,
                declined=declined,
                retired=retired,
            ),
            source="subagent",
            turn_number=parent_iteration,
            agent_id=authority.agent_id if pinned else None,
            pod_uid=authority.pod_uid if pinned else None,
            runtime_generation=(
                authority.session_runtime_generation if pinned else None
            ),
            session_runtime_generation=(
                authority.session_runtime_generation if pinned else None
            ),
            runtime_attach_token=authority.runtime_attach_token if pinned else None,
            allow_stateless_subagent_event=True,
            supersedes_input_seq=parent_input.seq,
        )
        await conn.execute(
            "UPDATE thread_messages SET metrics = $3::jsonb "
            "WHERE id = $1 AND thread_id = $2",
            message_row_id(delivery_id),
            parent_thread_id,
            json.dumps(
                continuation_metrics(
                    supersedes_input_seq=parent_input.seq,
                    calls=len(calls),
                    interrupted=interrupted,
                    not_started=not_started,
                    declined=declined,
                    retired=retired,
                )
            ),
        )

    if settle or answered:
        # The input is consumed with the continuation that supersedes it, or
        # because the turn answered it; never otherwise, so a turn whose calls
        # all have results replays with them in the transcript. The stateless
        # watermark counts human input only. An event input (the continuation
        # of an earlier settle, whose recovery turn delegated again) is
        # consumed by settling its own delivery below; moving the watermark
        # to its seq would skip input the user typed during the first batch,
        # which the executor serves after the continuation.
        if (
            lane == "stateless"
            and parent_input.role == "human"
            and not parent_input.source_already_complete
        ):
            await advance_recovery_watermark(
                conn,
                authority,
                parent_thread_id=parent_thread_id,
                parent_input=parent_input,
            )
        if parent_input.delivery_state == "admitted":
            await settle_recovery_source(
                conn, parent_thread_id=parent_thread_id, parent_input=parent_input
            )
    for call in calls:
        if call.needs_entry and call.child is not None:
            await stamp_recovered_child(
                conn, UUID(str(call.child["id"])), call.child["runtime_generation"]
            )

    # 9. The result, the delivery and the disposition of every call.
    call_views = []
    for call in calls:
        view = call.view()
        view["result_message_id"] = (
            str(written[call.tool_call_id]) if call.tool_call_id in written else None
        )
        call_views.append(view)
    if delivery is None:
        return {
            "result": "already_delivered",
            **identity,
            "delivery_id": None,
            "delivery_state": None,
            "execution_disposition": None,
            "calls": call_views,
        }
    stored = dict(delivery)
    stored["delivery_id"] = delivery_id
    view = _delivery_view(stored)
    return {
        "result": "applied",
        **identity,
        "delivery_id": view["id"],
        "delivery_state": view["state"],
        "execution_disposition": view["execution_disposition"],
        "delivery": view,
        "calls": call_views,
    }


__all__ = [
    "BatchMember",
    "DelegationCall",
    "RecoveryParentInput",
    "advance_recovery_watermark",
    "delivery_disposition",
    "end_session_child",
    "final_parent_response_seq",
    "load_delegation_manifest",
    "load_recovery_parent_input",
    "parent_turn_completed",
    "plan_recovery_turns",
    "settle_recovery_source",
    "settle_session_subagent_batch",
    "stamp_recovered_child",
]
