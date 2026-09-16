"""Idle, conversation-only rewind for stateless persistent sessions.

The effect and its replayable receipt commit in one PostgreSQL transaction.
No worker, queue transition, workspace operation, or model call participates.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Callable
from uuid import UUID, uuid4

import asyncpg

from orchestrator.schemas.thread_rewind import (
    RewindExpected,
    StatelessRewindPreview,
    StatelessRewindRequest,
    StatelessRewindResult,
)
from shared.event_journal import append_system_frame, bump_epoch
from shared.session_retirement import STATELESS_STOP_KEYS
from shared.thread_rewind import (
    LIVE_SESSION_CHILD_EXISTS_SQL,
    LIVE_USER_TARGET_SQL,
    SESSION_MEMORY_REWIND_GUARD_SQL,
)


@dataclass(slots=True)
class RewindFailure(Exception):
    status_code: int
    code: str
    reason: str

    def __str__(self) -> str:
        return f"{self.code}: {self.reason}"


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _strict_json_object(value: Any) -> dict[str, Any] | None:
    """Parse a JSON object without laundering malformed metadata into empty state."""

    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return None
        return dict(parsed) if isinstance(parsed, dict) else None
    return None


def _decimal(value: Any) -> str | None:
    return None if value is None else str(int(value))


def _canonical_request(body: StatelessRewindRequest) -> dict[str, Any]:
    return body.model_dump(mode="json")


def _result_from_payload(value: Any) -> StatelessRewindResult:
    return StatelessRewindResult.model_validate(_json_object(value))


def _refuse(code: str, reason: str, *, status_code: int = 409) -> None:
    raise RewindFailure(status_code=status_code, code=code, reason=reason)


async def _load_target(conn: Any, thread_id: UUID, message_id: UUID) -> Any:
    return await conn.fetchrow(LIVE_USER_TARGET_SQL, thread_id, message_id)


async def _load_queue(conn: Any, thread_id: UUID, *, lock: bool) -> Any:
    suffix = " FOR UPDATE" if lock else ""
    return await conn.fetchrow(
        "SELECT unit_id, unit_kind, state, lease_token, leased_by, leased_until, "
        "input_seq, consumed_seq, control_input_seq, control_consumed_seq, "
        "interrupt_admission_lease_token, interrupt_admission_turn_id, park_reason "
        "FROM run_queue WHERE unit_id=$1 AND unit_kind='session_turn'" + suffix,
        thread_id,
    )


async def _unsettled_reason(
    conn: Any,
    *,
    thread_id: UUID,
    from_seq: int,
    lock: bool,
) -> str | None:
    if await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM thread_control_requests "
        "WHERE thread_id=$1 AND outcome IS NULL)",
        thread_id,
    ):
        return "pending_control"
    if await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM thread_interrupt_requests "
        "WHERE thread_id=$1 AND (outcome IS NULL OR "
        "(outcome='applied' AND NOT (COALESCE(result, '{}'::jsonb) "
        "? 'consumed_input_seq'))))",
        thread_id,
    ):
        return "pending_interrupt"
    if await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM thread_permission_requests "
        "WHERE thread_id=$1 AND status='pending')",
        thread_id,
    ):
        return "pending_permission"
    if await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM thread_input_deliveries "
        "WHERE thread_id=$1 AND state NOT IN ('settled','cancelled'))",
        thread_id,
    ):
        return "pending_input_delivery"

    jobs_lock = " FOR UPDATE" if lock else ""
    jobs = await conn.fetch(
        "SELECT id, status, wake_state, wake_notified_status "
        "FROM jobs WHERE created_by_thread_id=$1 AND wake_on_complete "
        "ORDER BY id" + jobs_lock,
        thread_id,
    )
    terminal = {"completed", "failed", "cancelled", "pending_review"}
    for job in jobs:
        status = str(job["status"] or "")
        wake_state = str(job["wake_state"] or "")
        if status not in terminal:
            return "active_job"
        if wake_state in {"pending", "sending"}:
            return "pending_job_wake"
        if wake_state == "none" and job["wake_notified_status"] != status:
            return "pending_job_wake"
        if wake_state not in {"none", "sent", "dead", "undeliverable"}:
            return "unknown_job_wake"

    if await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM session_wake_events "
        "WHERE thread_id=$1 AND state IN ('pending','sending'))",
        thread_id,
    ):
        return "pending_scheduled_wake"
    if await conn.fetchval(LIVE_SESSION_CHILD_EXISTS_SQL, thread_id):
        return "pending_child"

    memory_sql = SESSION_MEMORY_REWIND_GUARD_SQL
    if lock:
        memory_sql += " FOR UPDATE OF effect"
    if await conn.fetchrow(memory_sql, thread_id, from_seq):
        return "pending_final_memory"
    return None


def _base_refusal(thread: Any, queue: Any, gate_enabled: bool) -> str | None:
    if not gate_enabled:
        return "feature_disabled"
    if str(thread["kind"] or "") != "session":
        return "unsupported_session_class"
    if thread["parent_job_id"] is not None or thread["parent_thread_id"] is not None:
        return "unsupported_session_class"
    if str(thread["execution_lane"] or "") != "stateless":
        return "unsupported_session_class"
    if thread["agent_id"] is not None:
        return "incompatible_agent_binding"
    if str(thread["status"] or "") not in {"active", "awaiting_user"}:
        return "unsupported_lifecycle"
    if thread["runtime_retirement_token"] is not None:
        return "runtime_transition"
    if thread["runtime_attach_token"] is not None:
        return "runtime_transition"
    metadata = _strict_json_object(thread["metadata"])
    if metadata is None:
        return "malformed_thread_metadata"
    if any(key in metadata for key in STATELESS_STOP_KEYS):
        return "runtime_transition"
    config_override = metadata.get("config_override")
    officer = (
        config_override.get("officer") if isinstance(config_override, dict) else None
    )
    if isinstance(officer, dict) and officer.get("enabled") is True:
        return "unsupported_session_class"
    if queue is None or str(queue["unit_kind"] or "") != "session_turn":
        return "missing_queue"
    if str(queue["state"] or "") != "done":
        return "queue_not_idle"
    if queue["leased_by"] is not None or queue["leased_until"] is not None:
        return "queue_leased"
    if queue["interrupt_admission_lease_token"] is not None:
        return "interrupt_admission_open"
    if queue["interrupt_admission_turn_id"] is not None:
        return "interrupt_admission_open"
    if queue["park_reason"] is not None:
        return "queue_parked"
    if queue["input_seq"] != queue["consumed_seq"]:
        return "input_pending"
    if int(queue["control_input_seq"] or 0) != int(queue["control_consumed_seq"] or 0):
        return "control_pending"
    return None


async def _evaluate(
    conn: Any,
    *,
    thread: Any,
    thread_id: UUID,
    message_id: UUID,
    gate_enabled: bool,
    lock: bool,
) -> tuple[Any | None, Any | None, str | None, RewindExpected | None, int]:
    queue = await _load_queue(conn, thread_id, lock=lock)
    refusal = _base_refusal(thread, queue, gate_enabled)
    if refusal is None and await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM project_officers WHERE thread_id=$1)",
        thread_id,
    ):
        refusal = "unsupported_session_class"

    target = await _load_target(conn, thread_id, message_id)
    if target is None:
        return queue, None, "target_invalid", None, 0

    swept_count = int(
        await conn.fetchval(
            "SELECT COUNT(*) FROM thread_messages WHERE thread_id=$1 "
            "AND seq >= $2 AND rewound_at IS NULL",
            thread_id,
            target["seq"],
        )
        or 0
    )
    if refusal is None:
        refusal = await _unsettled_reason(
            conn,
            thread_id=thread_id,
            from_seq=int(target["seq"]),
            lock=lock,
        )
    if refusal is not None or queue is None:
        return queue, target, refusal, None, swept_count

    transcript_tail = await conn.fetchval(
        "SELECT MAX(seq) FROM thread_messages WHERE thread_id=$1", thread_id
    )
    if transcript_tail is None:
        return queue, target, "target_invalid", None, swept_count
    expected = RewindExpected(
        session_runtime_generation=thread["runtime_generation"],
        conversation_revision=int(thread["conversation_revision"] or 0),
        events_epoch=int(thread["events_epoch"] or 0),
        transcript_tail_seq=str(int(transcript_tail)),
        input_seq=_decimal(queue["input_seq"]),
        consumed_seq=_decimal(queue["consumed_seq"]),
    )
    return queue, target, None, expected, swept_count


_THREAD_COLUMNS = """
id, user_id, kind, parent_job_id, parent_thread_id, execution_lane, agent_id,
status, metadata, runtime_generation, runtime_retirement_token,
runtime_attach_token,
conversation_revision, events_epoch, total_turns
"""


class ThreadRewindService:
    def __init__(self, store: Any, gate: Callable[[], bool]) -> None:
        self._store = store
        self._gate = gate

    @staticmethod
    def _authorize(thread: Any, user: dict[str, Any]) -> None:
        if thread is None:
            _refuse("rewind_not_found", "thread_not_found", status_code=404)
        if not user.get("is_admin") and str(thread["user_id"] or "") != str(user["id"]):
            _refuse("rewind_forbidden", "not_thread_owner", status_code=403)

    async def preview(
        self, thread_id: str, message_id: UUID, user: dict[str, Any]
    ) -> StatelessRewindPreview:
        tid = UUID(str(thread_id))
        async with self._store.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                thread = await conn.fetchrow(
                    f"SELECT {_THREAD_COLUMNS} FROM threads WHERE id=$1", tid
                )
                self._authorize(thread, user)
                _queue, target, refusal, expected, swept = await _evaluate(
                    conn,
                    thread=thread,
                    thread_id=tid,
                    message_id=message_id,
                    gate_enabled=self._gate(),
                    lock=False,
                )
        return StatelessRewindPreview(
            message_id=message_id,
            prompt=str(target["content"] or "") if target is not None else "",
            eligible=refusal is None,
            refusal_code=refusal,
            swept_count=swept,
            expected=expected,
        )

    async def receipt(
        self, thread_id: str, client_request_id: UUID, user: dict[str, Any]
    ) -> StatelessRewindResult:
        tid = UUID(str(thread_id))
        async with self._store.acquire() as conn:
            thread = await conn.fetchrow(
                f"SELECT {_THREAD_COLUMNS} FROM threads WHERE id=$1", tid
            )
            self._authorize(thread, user)
            row = await conn.fetchrow(
                "SELECT result_payload FROM thread_rewinds "
                "WHERE thread_id=$1 AND client_request_id=$2",
                tid,
                client_request_id,
            )
        if row is None:
            _refuse("rewind_receipt_not_found", "receipt_not_found", status_code=404)
        return _result_from_payload(row["result_payload"])

    async def apply(
        self, thread_id: str, body: StatelessRewindRequest, user: dict[str, Any]
    ) -> tuple[StatelessRewindResult, bool]:
        retryable = (
            asyncpg.exceptions.DeadlockDetectedError,
            asyncpg.exceptions.SerializationError,
        )
        for attempt in range(2):
            try:
                return await self._apply_once(thread_id, body, user)
            except retryable:
                if attempt:
                    break
                await asyncio.sleep(0)
            except (
                asyncpg.exceptions.LockNotAvailableError,
                asyncpg.exceptions.QueryCanceledError,
            ) as exc:
                raise RewindFailure(
                    503, "rewind_temporarily_unavailable", "database_timeout"
                ) from exc
        raise RewindFailure(
            503, "rewind_temporarily_unavailable", "transaction_retry_exhausted"
        )

    async def _apply_once(
        self, thread_id: str, body: StatelessRewindRequest, user: dict[str, Any]
    ) -> tuple[StatelessRewindResult, bool]:
        tid = UUID(str(thread_id))
        canonical = _canonical_request(body)
        async with self._store.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL lock_timeout = '2s'")
                await conn.execute("SET LOCAL statement_timeout = '10s'")
                thread = await conn.fetchrow(
                    f"SELECT {_THREAD_COLUMNS} FROM threads WHERE id=$1 FOR UPDATE",
                    tid,
                )
                self._authorize(thread, user)

                duplicate = await conn.fetchrow(
                    "SELECT request_payload, result_payload FROM thread_rewinds "
                    "WHERE thread_id=$1 AND client_request_id=$2",
                    tid,
                    body.client_request_id,
                )
                if duplicate is not None:
                    if _json_object(duplicate["request_payload"]) != canonical:
                        _refuse(
                            "rewind_idempotency_conflict",
                            "client_request_reused",
                        )
                    return _result_from_payload(duplicate["result_payload"]), True

                queue, target, refusal, actual, swept_count = await _evaluate(
                    conn,
                    thread=thread,
                    thread_id=tid,
                    message_id=body.message_id,
                    gate_enabled=self._gate(),
                    lock=True,
                )
                if refusal == "target_invalid":
                    # A concurrent rewind can hide this target while this request
                    # waits for the thread lock.  Report the changed boundary,
                    # rather than misclassifying a formerly valid target.
                    if (
                        thread["runtime_generation"]
                        != body.expected.session_runtime_generation
                        or int(thread["conversation_revision"] or 0)
                        != body.expected.conversation_revision
                        or int(thread["events_epoch"] or 0)
                        != body.expected.events_epoch
                    ):
                        _refuse("rewind_stale", "preview_changed")
                    _refuse("rewind_target_invalid", refusal)
                if refusal in {
                    "feature_disabled",
                    "unsupported_session_class",
                    "unsupported_lifecycle",
                    "incompatible_agent_binding",
                    "missing_queue",
                }:
                    _refuse("rewind_unavailable", refusal)
                if refusal is not None:
                    _refuse("rewind_busy", refusal)
                if actual is None or target is None or queue is None:
                    _refuse("rewind_unavailable", "eligibility_unavailable")
                if actual != body.expected:
                    _refuse("rewind_stale", "preview_changed")

                rewind_id = uuid4()
                swept = int(
                    await conn.fetchval(
                        """
                        WITH swept AS (
                            UPDATE thread_messages
                            SET rewound_at=now()
                            WHERE thread_id=$1 AND seq >= $2
                              AND rewound_at IS NULL
                            RETURNING 1
                        )
                        SELECT COUNT(*) FROM swept
                        """,
                        tid,
                        target["seq"],
                    )
                    or 0
                )
                if swept != swept_count or swept <= 0:
                    raise RuntimeError("rewind sweep changed after locked validation")
                surviving_turn = int(
                    await conn.fetchval(
                        "SELECT COALESCE(MAX(turn_number), 0) FROM thread_messages "
                        "WHERE thread_id=$1 AND rewound_at IS NULL "
                        "AND role NOT IN ('summary','error')",
                        tid,
                    )
                    or 0
                )
                revision = int(
                    await conn.fetchval(
                        "UPDATE threads SET total_turns=$2, "
                        "conversation_revision=conversation_revision+1 "
                        "WHERE id=$1 RETURNING conversation_revision",
                        tid,
                        surviving_turn,
                    )
                )
                await conn.execute(
                    "UPDATE thread_session_runtime_state "
                    "SET memory_extraction_turn=LEAST(memory_extraction_turn,$2), "
                    "updated_at=now() WHERE thread_id=$1",
                    tid,
                    surviving_turn,
                )
                await bump_epoch(conn, thread_id=str(tid))
                event = await append_system_frame(
                    conn,
                    thread_id=str(tid),
                    kind="rewind.done",
                    payload={
                        "rewind_id": str(rewind_id),
                        "client_request_id": str(body.client_request_id),
                        "message_id": str(body.message_id),
                        "mode": "conversation",
                        "conversation_revision": revision,
                    },
                )
                if event is None:
                    raise RuntimeError("rewind event allocation lost thread")
                event_epoch, event_seq = event
                result = StatelessRewindResult(
                    rewind_id=rewind_id,
                    client_request_id=body.client_request_id,
                    message_id=body.message_id,
                    prompt=str(target["content"] or ""),
                    swept_count=swept,
                    surviving_turn=surviving_turn,
                    conversation_revision=revision,
                    events_epoch=event_epoch,
                    event_seq=str(event_seq),
                )
                await conn.execute(
                    """
                    INSERT INTO thread_rewinds (
                        id, thread_id, from_seq, mode, actor, swept_count,
                        client_request_id, request_payload, result_payload,
                        runtime_generation, before_conversation_revision,
                        after_conversation_revision, event_epoch, event_seq
                    ) VALUES (
                        $1, $2, $3, 'conversation', $4, $5, $6,
                        $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13
                    )
                    """,
                    rewind_id,
                    tid,
                    target["seq"],
                    str(user["id"]),
                    swept,
                    body.client_request_id,
                    json.dumps(canonical, sort_keys=True, separators=(",", ":")),
                    result.model_dump_json(),
                    thread["runtime_generation"],
                    int(thread["conversation_revision"] or 0),
                    revision,
                    event_epoch,
                    event_seq,
                )
                return result, False


__all__ = ["RewindFailure", "ThreadRewindService"]
