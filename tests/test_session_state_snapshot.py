from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.services.session_state_snapshot import (
    build_session_state_snapshot,
)


class _AsyncContext:
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _SnapshotConn:
    def __init__(
        self,
        *,
        thread: dict | None,
        lifecycle: dict | None = None,
        running: dict | None = None,
        permissions: list[dict] | None = None,
        tasks: list[dict] | None = None,
        control_receipts: list[dict] | None = None,
        message_count: int = 0,
        live_turn_count: int = 0,
        usage_events: list[dict] | None = None,
    ) -> None:
        self.thread = thread
        self.lifecycle = lifecycle
        self.running = running
        self.permissions = permissions or []
        self.tasks = tasks or []
        self.control_receipts = control_receipts or []
        self.message_count = message_count
        self.live_turn_count = live_turn_count
        # Rows the production ``usage.updated`` SELECT would return: the
        # frames of the newest turn carrying usage, oldest first. Each entry is
        # ``{"seq": int, "payload": <dict or json str>}``; ``seq`` is unused by
        # the builder and only present so fixtures read like the journal.
        self.usage_events = usage_events or []
        self.calls: list[tuple[str, tuple]] = []

    def transaction(self, **kwargs):
        assert kwargs == {"isolation": "repeatable_read", "readonly": True}
        return _AsyncContext()

    async def fetchrow(self, sql: str, *args):
        self.calls.append((sql, args))
        if "FROM threads" in sql:
            return self.thread
        if "AS latest_kind" in sql:
            return self.lifecycle
        if "SELECT started.payload" in sql:
            return self.running
        raise AssertionError(f"unexpected fetchrow query: {sql}")

    async def fetchval(self, sql: str, *args):
        self.calls.append((sql, args))
        if "COUNT(*) FROM thread_messages" in sql:
            return self.message_count
        if "MAX(turn_number)" in sql:
            return self.live_turn_count
        raise AssertionError(f"unexpected fetchval query: {sql}")

    async def fetch(self, sql: str, *args):
        self.calls.append((sql, args))
        if "FROM thread_permission_requests" in sql:
            return self.permissions
        if "FROM thread_session_tasks" in sql:
            assert "ORDER BY task_number ASC" in sql
            return self.tasks
        if "'usage.updated'" in sql:
            # Pin the two predicates the double-count contract rests on: the
            # read is bounded by the same hwm that becomes ``event_cursor``
            # (so the Cockpit's ``coveredBySnapshot`` partitions the journal
            # on the identical seq), and it selects one turn, not one frame.
            assert "event.seq <= $3" in sql
            assert "IS NOT DISTINCT FROM latest.turn" in sql
            assert args[2] == int(self.thread["events_seq_hwm"] or 0)
            return self.usage_events
        if "FROM thread_control_requests" in sql:
            # Model the query's partial read rather than handing finalized
            # history to the snapshot builder. Keeping ``outcome`` on these
            # fixture rows also lets tests pin the SQL predicate explicitly.
            assert "request.outcome IS NULL" in sql
            return [
                receipt
                for receipt in self.control_receipts
                if receipt.get("outcome") is None
            ]
        raise AssertionError(f"unexpected fetch query: {sql}")


class _SnapshotDB:
    def __init__(self, conn: _SnapshotConn) -> None:
        self.conn = conn

    def acquire(self):
        return _AsyncContext(self.conn)


def _thread(**overrides):
    row = {
        "id": "thread-1",
        "permission_mode": "supervised",
        "metadata": {},
        "status": "active",
        "execution_lane": "pinned",
        "events_epoch": 3,
        "events_seq_hwm": 41,
        "queue_state": None,
    }
    row.update(overrides)
    return row


def _control_receipt(
    *,
    request_id: str,
    request_seq: int,
    verb: str,
    mode: str,
    event_kind: str | None = None,
    event_mode: str | None = None,
    event_method: str | None = None,
    event_epoch: int = 2,
    outcome: str | None = None,
) -> dict:
    client_request_id = f"00000000-0000-4000-8000-{request_seq:012d}"
    expected_kind = {
        "mode.set": "mode.changed",
        "narration.set": "narration.changed",
    }.get(verb, "control.rejected")
    return {
        "request_id": request_id,
        "client_request_id": client_request_id,
        "request_seq": request_seq,
        "verb": verb,
        "request_payload": {"mode": mode},
        "event_kind": event_kind or expected_kind,
        "event_payload": {
            "request_id": request_id,
            "client_request_id": client_request_id,
            "request_seq": request_seq,
            "method": event_method or verb,
            "mode": event_mode or mode,
        },
        # The production SELECT intentionally does not constrain event epoch:
        # a receipt can survive the owner's crash and a later epoch bump.
        "event_epoch": event_epoch,
        "outcome": outcome,
    }


@pytest.mark.asyncio
async def test_snapshot_has_full_lane_free_shape_and_normalizes_durable_rows():
    conn = _SnapshotConn(
        thread=_thread(
            conversation_revision=6,
            metadata=json.dumps(
                {
                    "config_override": {
                        "interactive": {"narration_mode": "silent"},
                        "llm": {"model": "stored-model", "api_key": "never-return"},
                    },
                    "execution_lane": "must-not-return",
                }
            ),
        ),
        lifecycle={
            "latest_kind": "turn.started",
            "latest_turn_id": "7",
            "latest_turn_start_seq": 39,
        },
        running={
            "payload": json.dumps(
                {"id": "tool-7", "tool": "run_command", "args": {"cmd": "ls"}}
            )
        },
        permissions=[
            {
                "id": "approval-1",
                "tool_call_id": "tool-8",
                "tool_name": "write_file",
                "tool_args": '{"path":"notes.md"}',
            },
            {
                "id": "approval-2",
                "tool_call_id": "tool-9",
                "tool_name": "run_command",
                "tool_args": "not-json",
            },
        ],
        message_count=12,
        live_turn_count=7,
    )

    result = await build_session_state_snapshot(
        _SnapshotDB(conn),
        "thread-1",
        resolved_config={
            "agent": {
                "interactive": {
                    "permission_mode": "autonomous",
                    "narration_mode": "verbose",
                },
                "llm": {
                    "model": "effective-model",
                    "temperature": Decimal("0.25"),
                    "api_key": "also-never-return",
                },
            },
        },
    )

    assert result == {
        "thread_id": "thread-1",
        "conversation_revision": 6,
        # The first-class thread column is the durable permission authority.
        "permission_mode": "supervised",
        "narration_mode": "verbose",
        "turn_count": 7,
        "turn_in_flight": True,
        "message_count": 12,
        "model": "effective-model",
        "temperature": 0.25,
        "running_tool": {
            "id": "tool-7",
            "tool": "run_command",
            "args": {"cmd": "ls"},
        },
        "pending_permissions": [
            {
                "id": "tool-8",
                "approval_id": "approval-1",
                "tool": "write_file",
                "args": {"path": "notes.md"},
            },
            {
                "id": "tool-9",
                "approval_id": "approval-2",
                "tool": "run_command",
                "args": {},
            },
        ],
        "tasks": [],
        # Explicit null, not an omitted key: presence is authoritative on the
        # Cockpit side, so a thread with no usage history actively clears the
        # panel rather than leaving the previous session's numbers on screen.
        "usage": None,
        "event_cursor": {"epoch": 3, "seq": 41},
        "replay_cursor": {"epoch": 3, "seq": 38},
        "snapshot_source": "durable_journal",
    }
    encoded = json.dumps(result)
    assert "execution_lane" not in encoded
    assert "api_key" not in encoded
    assert "never-return" not in encoded

    lifecycle_call = next(call for call in conn.calls if "AS latest_kind" in call[0])
    assert lifecycle_call[1][1] == 3
    assert lifecycle_call[1][2] == [
        "turn.started",
        "turn.completed",
        "turn.error",
        "turn.interrupted",
        "turn.parked",
        "ready",
    ]


@pytest.mark.asyncio
async def test_snapshot_hydrates_tasks_older_than_the_replay_floor():
    created = datetime(2026, 8, 10, 8, 30, tzinfo=timezone.utc)
    completed = datetime(2026, 8, 10, 9, 15, tzinfo=timezone.utc)
    conn = _SnapshotConn(
        thread=_thread(events_seq_hwm=81, execution_lane="stateless"),
        lifecycle={
            "latest_kind": "turn.completed",
            "latest_turn_id": "6",
            "latest_turn_start_seq": 78,
        },
        tasks=[
            {
                "task_number": 2,
                "description": "Survive the next pod",
                "status": "in_progress",
                "priority": "high",
                "notes": "claimed on pod A",
                "created_at": created,
                "completed_at": None,
            },
            {
                "task_number": 7,
                "description": "Verify the handoff",
                "status": "completed",
                "priority": "medium",
                "notes": "observed on pod B",
                "created_at": created,
                "completed_at": completed,
            },
        ],
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["replay_cursor"] == {"epoch": 3, "seq": 77}
    assert result["tasks"] == [
        {
            "id": "task_2",
            "description": "Survive the next pod",
            "status": "in_progress",
            "priority": "high",
            "notes": "claimed on pod A",
            "created_at": "2026-08-10T08:30:00+00:00",
            "completed_at": None,
        },
        {
            "id": "task_7",
            "description": "Verify the handoff",
            "status": "completed",
            "priority": "medium",
            "notes": "observed on pod B",
            "created_at": "2026-08-10T08:30:00+00:00",
            "completed_at": "2026-08-10T09:15:00+00:00",
        },
    ]


@pytest.mark.asyncio
async def test_valid_pending_prior_epoch_receipt_overlays_old_thread_scalar():
    receipt = _control_receipt(
        request_id="11111111-1111-4111-8111-111111111111",
        request_seq=7,
        verb="mode.set",
        mode="autonomous",
        event_epoch=2,
    )
    conn = _SnapshotConn(
        thread=_thread(
            permission_mode="supervised",
            narration_mode="auto",
            events_epoch=3,
        ),
        control_receipts=[receipt],
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["permission_mode"] == "autonomous"
    assert result["narration_mode"] == "auto"
    assert result["event_cursor"]["epoch"] == 3
    control_sql = next(
        sql for sql, _args in conn.calls if "FROM thread_control_requests" in sql
    )
    assert "event.epoch =" not in " ".join(control_sql.split())


@pytest.mark.asyncio
async def test_finalized_historical_receipt_does_not_overlay_newer_thread_scalar():
    historical = _control_receipt(
        request_id="22222222-2222-4222-8222-222222222222",
        request_seq=4,
        verb="mode.set",
        mode="autonomous",
        event_epoch=1,
        outcome="applied",
    )
    conn = _SnapshotConn(
        thread=_thread(
            permission_mode="supervised",
            narration_mode="verbose",
        ),
        control_receipts=[historical],
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["permission_mode"] == "supervised"
    assert result["narration_mode"] == "verbose"
    control_sql = next(
        sql for sql, _args in conn.calls if "FROM thread_control_requests" in sql
    )
    assert "request.outcome IS NULL" in control_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt_override",
    [
        pytest.param(
            {"event_kind": "narration.changed"},
            id="mismatched-kind",
        ),
        pytest.param(
            {"event_mode": "supervised"},
            id="mismatched-mode",
        ),
        pytest.param(
            {"event_method": "narration.set"},
            id="mismatched-method",
        ),
    ],
)
async def test_mismatched_pending_control_receipt_is_ignored(receipt_override):
    receipt = _control_receipt(
        request_id="33333333-3333-4333-8333-333333333333",
        request_seq=9,
        verb="mode.set",
        mode="autonomous",
        **receipt_override,
    )
    conn = _SnapshotConn(
        thread=_thread(permission_mode="supervised", narration_mode="auto"),
        control_receipts=[receipt],
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["permission_mode"] == "supervised"
    assert result["narration_mode"] == "auto"


@pytest.mark.asyncio
async def test_pending_permission_and_narration_receipts_overlay_independently():
    narration = _control_receipt(
        request_id="44444444-4444-4444-8444-444444444444",
        request_seq=10,
        verb="narration.set",
        mode="silent",
    )
    permission = _control_receipt(
        request_id="55555555-5555-4555-8555-555555555555",
        request_seq=11,
        verb="mode.set",
        mode="auto_accept",
    )
    conn = _SnapshotConn(
        thread=_thread(permission_mode="supervised", narration_mode="verbose"),
        control_receipts=[narration, permission],
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["permission_mode"] == "auto_accept"
    assert result["narration_mode"] == "silent"


@pytest.mark.asyncio
async def test_snapshot_explicitly_clears_idle_runtime_and_pending_state():
    conn = _SnapshotConn(
        thread=_thread(metadata={"config_override": {}}),
        lifecycle={
            "latest_kind": "turn.completed",
            "latest_turn_id": "7",
            "latest_turn_start_seq": 35,
        },
        running=None,
        permissions=[],
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["turn_in_flight"] is False
    assert result["running_tool"] is None
    assert result["pending_permissions"] == []
    assert result["narration_mode"] == "auto"
    assert result["model"] is None
    assert result["temperature"] is None


def _usage_event(seq: int, **payload) -> dict:
    return {"seq": seq, "payload": payload}


@pytest.mark.asyncio
async def test_usage_accumulates_the_turns_calls_not_just_its_last_frame():
    """A tool-using turn makes several LLM calls; the panel shows their sum.

    Restoring only the newest frame would under-report OUTPUT by every call but
    the last, which is the number the user watches to see what a turn cost.
    """

    conn = _SnapshotConn(
        thread=_thread(),
        lifecycle={
            "latest_kind": "turn.completed",
            "latest_turn_id": "6",
            "latest_turn_start_seq": 30,
        },
        usage_events=[
            _usage_event(
                31,
                turn=6,
                input_tokens=40_000,
                output_tokens=500,
                reasoning_tokens=120,
                ctx_limit_tokens=200_000,
                compaction_threshold_tokens=160_000,
            ),
            _usage_event(35, turn=6, input_tokens=44_000, output_tokens=210),
            _usage_event(
                39, turn=6, input_tokens=47_500, output_tokens=90, reasoning_tokens=30
            ),
        ],
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["usage"] == {
        "turn": 6,
        # Latest wins: the newest call's prompt is the current context fill.
        "input_tokens": 47_500,
        # Accumulated across the turn's calls.
        "output_tokens": 800,
        "reasoning_tokens": 150,
        "reasoning_estimated": False,
        # Sticky: later frames omit the limits, so the first frame's survive.
        "ctx_limit_tokens": 200_000,
        "compaction_threshold_tokens": 160_000,
    }


@pytest.mark.asyncio
async def test_usage_is_sticky_true_once_any_call_estimated_reasoning():
    conn = _SnapshotConn(
        thread=_thread(),
        usage_events=[
            _usage_event(
                20,
                turn=2,
                input_tokens=9_000,
                output_tokens=81,
                reasoning_tokens=70,
                reasoning_estimated=True,
            ),
            # Provider reported a real count on the follow-up call; the turn
            # as a whole is still partly derived, so the panel keeps the ~.
            _usage_event(24, turn=2, input_tokens=9_500, output_tokens=40),
        ],
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["usage"]["reasoning_estimated"] is True
    assert result["usage"]["reasoning_tokens"] == 70


@pytest.mark.asyncio
async def test_usage_tolerates_json_encoded_payloads_and_missing_fields():
    """asyncpg may hand back jsonb as text, and old frames predate fields."""

    conn = _SnapshotConn(
        thread=_thread(),
        usage_events=[
            {"seq": 10, "payload": json.dumps({"turn": 1, "input_tokens": 5_000})},
            {"seq": 11, "payload": json.dumps({"turn": 1, "output_tokens": 7})},
        ],
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["usage"] == {
        "turn": 1,
        # The newest frame omits input entirely — the previous one still holds.
        "input_tokens": 5_000,
        "output_tokens": 7,
        "reasoning_tokens": 0,
        "reasoning_estimated": False,
        "ctx_limit_tokens": None,
        "compaction_threshold_tokens": None,
    }


@pytest.mark.asyncio
async def test_usage_is_none_when_the_thread_never_reported_any():
    conn = _SnapshotConn(thread=_thread(), usage_events=[])

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["usage"] is None


@pytest.mark.asyncio
async def test_snapshot_returns_none_if_thread_disappears_inside_read():
    conn = _SnapshotConn(thread=None)
    assert await build_session_state_snapshot(_SnapshotDB(conn), "thread-gone") is None
    assert len(conn.calls) == 1


@pytest.mark.asyncio
async def test_snapshot_uses_live_turn_after_rewind_and_queue_done_clears_stale_edge():
    conn = _SnapshotConn(
        thread=_thread(execution_lane="stateless", queue_state="done"),
        lifecycle={
            "latest_kind": "turn.started",
            "latest_turn_id": "4",
            "latest_turn_start_seq": 41,
        },
        running={"payload": {"id": "stale-tool", "tool": "run_command", "args": {}}},
        live_turn_count=4,
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["turn_count"] == 4
    assert result["turn_in_flight"] is False
    assert result["running_tool"] is None
    # Do not replay the stale start when the queue proves its terminal edge
    # was the journal write that went missing.
    assert result["replay_cursor"] == {"epoch": 3, "seq": 41}


@pytest.mark.asyncio
async def test_stale_queue_row_never_clears_a_pinned_runtime_snapshot():
    conn = _SnapshotConn(
        thread=_thread(execution_lane="pinned", queue_state="done"),
        lifecycle={
            "latest_kind": "turn.started",
            "latest_turn_id": "8",
            "latest_turn_start_seq": 41,
        },
        running={"payload": {"id": "live-tool", "tool": "run_command", "args": {}}},
        live_turn_count=8,
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["turn_in_flight"] is True
    assert result["running_tool"] == {
        "id": "live-tool",
        "tool": "run_command",
        "args": {},
    }


@pytest.mark.asyncio
async def test_unknown_stateless_queue_state_never_claims_runtime_is_idle():
    conn = _SnapshotConn(
        thread=_thread(execution_lane="stateless", queue_state="future-state"),
        lifecycle={
            "latest_kind": "turn.started",
            "latest_turn_id": "8",
            "latest_turn_start_seq": 41,
        },
        running={"payload": {"id": "live-tool", "tool": "run_command", "args": {}}},
        live_turn_count=8,
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["turn_in_flight"] is True
    assert result["running_tool"] is not None


@pytest.mark.asyncio
async def test_ready_is_a_durable_terminal_observation_for_turn_and_tool_state():
    conn = _SnapshotConn(
        thread=_thread(execution_lane="pinned", queue_state=None),
        lifecycle={
            "latest_kind": "ready",
            "latest_turn_id": "8",
            "latest_turn_start_seq": 39,
        },
        running=None,
        live_turn_count=8,
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["turn_in_flight"] is False


@pytest.mark.asyncio
async def test_queued_future_inputs_do_not_advance_the_active_runtime_turn():
    conn = _SnapshotConn(
        thread=_thread(execution_lane="stateless", queue_state="leased"),
        lifecycle={
            "latest_kind": "turn.started",
            "latest_turn_id": "8",
            "latest_turn_start_seq": 36,
        },
        # Durable admission has already numbered two future human rows 9/10.
        live_turn_count=10,
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["turn_count"] == 8
    assert result["turn_in_flight"] is True
    assert result["replay_cursor"] == {"epoch": 3, "seq": 35}


@pytest.mark.asyncio
async def test_turn_count_fallback_uses_only_execution_produced_rows():
    conn = _SnapshotConn(
        thread=_thread(events_seq_hwm=77),
        lifecycle=None,
        live_turn_count=6,
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["turn_count"] == 6
    assert result["replay_cursor"] == {"epoch": 3, "seq": 77}
    fallback_sql = next(sql for sql, _args in conn.calls if "MAX(turn_number)" in sql)
    assert "role NOT IN ('human', 'event')" in fallback_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["ended", "suspended", "idle", "future-status"])
async def test_terminal_thread_status_clears_stale_runtime_edges(status):
    conn = _SnapshotConn(
        thread=_thread(status=status),
        lifecycle={
            "latest_kind": "turn.started",
            "latest_turn_id": "9",
            "latest_turn_start_seq": 40,
        },
        running={"payload": {"id": "stale", "tool": "run_command", "args": {}}},
    )

    result = await build_session_state_snapshot(_SnapshotDB(conn), "thread-1")

    assert result is not None
    assert result["turn_in_flight"] is False
    assert result["running_tool"] is None


@pytest.mark.asyncio
async def test_config_resolver_receives_the_row_captured_with_the_event_cursor():
    conn = _SnapshotConn(
        thread=_thread(
            metadata={"config_override": {"llm": {"model": "captured"}}},
            events_seq_hwm=52,
        ),
        lifecycle=None,
    )
    resolver = AsyncMock(
        return_value={"agent": {"llm": {"model": "captured", "api_key": "secret"}}}
    )

    result = await build_session_state_snapshot(
        _SnapshotDB(conn), "thread-1", config_resolver=resolver
    )

    assert result is not None
    resolver.assert_awaited_once_with(
        {
            **_thread(
                metadata={"config_override": {"llm": {"model": "captured"}}},
                events_seq_hwm=52,
            )
        },
        {"config_override": {"llm": {"model": "captured"}}},
    )
    assert result["model"] == "captured"
    assert result["event_cursor"] == {"epoch": 3, "seq": 52}
    assert "secret" not in json.dumps(result)


@pytest.mark.asyncio
async def test_owner_gated_route_resolves_only_safe_display_config():
    import orchestrator.main as orchestrator_main

    thread = _thread(
        id="thread-1",
        user_id="user-1",
        metadata={"config_override": {}},
    )
    snapshot = {
        "thread_id": "thread-1",
        "pending_permissions": [],
        "event_cursor": {"epoch": 3, "seq": 41},
    }
    owner = AsyncMock(return_value=({"id": "user-1"}, thread))
    resolve = AsyncMock(
        return_value={"agent": {"llm": {"model": "effective", "api_key": "secret"}}}
    )

    async def _build_with_captured_resolver(_db, _thread_id, *, config_resolver):
        captured = {
            **thread,
            "metadata": {"config_override": {"llm": {"model": "captured"}}},
        }
        resolved = await config_resolver(captured, captured["metadata"])
        assert resolved == {
            "agent": {"llm": {"model": "effective", "api_key": "secret"}}
        }
        return snapshot

    build = AsyncMock(side_effect=_build_with_captured_resolver)

    with (
        patch.object(orchestrator_main, "require_thread_owner", owner),
        patch.object(orchestrator_main, "_resolve_session_config", resolve),
        patch.object(orchestrator_main, "build_session_state_snapshot", build),
    ):
        response = MagicMock()
        response.headers = {}
        result = await orchestrator_main.get_thread_session_state(
            "thread-1", MagicMock(), response
        )

    assert result is snapshot
    assert response.headers["Cache-Control"] == "private, no-store"
    owner.assert_awaited_once()
    resolve.assert_awaited_once_with(
        {**thread, "metadata": {"config_override": {"llm": {"model": "captured"}}}},
        {"config_override": {"llm": {"model": "captured"}}},
    )
    build.assert_awaited_once_with(
        orchestrator_main.postgres_db,
        "thread-1",
        config_resolver=ANY,
    )


@pytest.mark.asyncio
async def test_owner_gated_route_returns_404_if_thread_vanishes_after_auth():
    import orchestrator.main as orchestrator_main

    with (
        patch.object(
            orchestrator_main,
            "require_thread_owner",
            AsyncMock(return_value=({"id": "user-1"}, _thread())),
        ),
        patch.object(
            orchestrator_main,
            "_resolve_session_config",
            AsyncMock(return_value=None),
        ),
        patch.object(
            orchestrator_main,
            "build_session_state_snapshot",
            AsyncMock(return_value=None),
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await orchestrator_main.get_thread_session_state(
                "thread-gone", MagicMock(), MagicMock()
            )
    assert exc.value.status_code == 404
