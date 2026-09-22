"""Park lifecycle (stateless_turn_resilience.md step 2): the queue side.

Pure rules (backoff schedule, error signature, retryable verdict), the SQL
contracts of the new statements (asserted verbatim — memory: strict asyncpg
seams break on new SQL), the read models on a fake connection, and the
executor's attach-failure path: it must route through
``record_attach_failure`` and journal ``turn.parked`` when the row parks.
The lock-manager semantics (CAS under a stolen lease, real ``run_after``)
are proven in ``tests/test_run_queue.py`` against a scratch Postgres.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.api import turn_executor as te
from orchestrator.services import stateless_queue_state as sqs
from shared.run_queue import (
    ATTACH_FAILURE_BACKOFF_CAP_SECONDS,
    ATTACH_FAILURE_SIGNATURE_PARK_THRESHOLD,
    PARK_REASON_ATTACH_FAILED,
    PARK_REASON_CLAIM_LOSS_HOLD,
    PARK_REASON_COMPLETION_CAS_FAILED,
    PARK_REASON_REAPER_MAX_ATTEMPTS,
    PARK_REASON_RETRY_EXHAUSTED,
    PARK_REASON_SHUTDOWN_CANCELLED,
    RETRYABLE_PARK_REASONS,
    ClaimedUnit,
    attach_failure_backoff_seconds,
    list_parked,
    queue_state_for,
    record_attach_failure,
    release_unit,
)
from shared.run_queue import queries as q
from shared.session_retirement import (
    CLAIM_LOSS_HOLD_KEY,
    CLAIM_RETIREMENT_KEY,
)

UNIT = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
NOW = datetime(2026, 9, 8, 15, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Pure rules
# ---------------------------------------------------------------------------


def test_backoff_schedule_walks_holds_and_caps():
    assert [attach_failure_backoff_seconds(n) for n in (1, 2, 3, 4)] == [
        5.0,
        15.0,
        45.0,
        135.0,
    ]
    assert attach_failure_backoff_seconds(9) == 135.0  # holds the last step
    assert attach_failure_backoff_seconds(0) == 5.0  # never negative-indexes
    assert (
        attach_failure_backoff_seconds(2, schedule=(1.0, 900.0))
        == ATTACH_FAILURE_BACKOFF_CAP_SECONDS
    )
    assert attach_failure_backoff_seconds(3, schedule=()) == 0.0


def test_error_signature_is_stable_across_ids_and_counters():
    a = ValueError(
        "attach of 7d5c417a-e7c3-4da0-b8a3-26b502e5defa failed (HTTP 400) at 12:34:56"
    )
    b = ValueError(
        "attach of 0e15f5e8-3f70-4425-8d23-3ddff6d8d5d8 failed (HTTP 400) at 12:35:10"
    )
    assert te._error_signature(a) == te._error_signature(b)
    assert te._error_signature(a).startswith("ValueError:")
    assert "<uuid>" in te._error_signature(a)
    # A different class or message is a different signature.
    assert te._error_signature(
        RuntimeError("attach failed (HTTP 400)")
    ) != te._error_signature(a)
    assert te._error_signature(
        ValueError("attach failed (HTTP 500)")
    ) == te._error_signature(
        ValueError("attach failed (HTTP 400)")
    )  # digits fold: the class of failure, not the code, is the signature
    assert len(te._error_signature(ValueError("x" * 500))) <= len("ValueError:") + 80


def test_park_reason_mapping_normalises_executor_reasons():
    assert (
        te._park_reason_for("uncooperative_shutdown") == PARK_REASON_SHUTDOWN_CANCELLED
    )
    assert (
        te._park_reason_for("completion_cas_failed")
        == PARK_REASON_COMPLETION_CAS_FAILED
    )
    assert (
        te._park_reason_for("completion_cas_failed_pre_effect")
        == PARK_REASON_COMPLETION_CAS_FAILED
    )
    assert te._park_reason_for("loop_died_interrupt_drain_failed") == (
        "loop_died_interrupt_drain_failed"
    )


def test_retryable_set_and_refusal_order():
    assert RETRYABLE_PARK_REASONS == {
        PARK_REASON_ATTACH_FAILED,
        PARK_REASON_SHUTDOWN_CANCELLED,
        PARK_REASON_COMPLETION_CAS_FAILED,
        PARK_REASON_REAPER_MAX_ATTEMPTS,
        PARK_REASON_RETRY_EXHAUSTED,
    }
    assert sqs.park_retry_refusal(PARK_REASON_ATTACH_FAILED, {}) is None
    assert sqs.park_retry_refusal(PARK_REASON_RETRY_EXHAUSTED, {}) is None
    assert sqs.park_retry_refusal(PARK_REASON_ATTACH_FAILED, None) is None
    assert (
        sqs.park_retry_refusal("some_executor_reason", {})
        == sqs.RETRY_REFUSAL_NOT_RETRYABLE
    )
    assert sqs.park_retry_refusal(PARK_REASON_CLAIM_LOSS_HOLD, {}) == (
        sqs.RETRY_REFUSAL_CLAIM_LOSS_HOLD
    )
    # A hold marker wins over a retryable reason; any other stop marker too.
    assert sqs.park_retry_refusal(
        PARK_REASON_ATTACH_FAILED, {CLAIM_LOSS_HOLD_KEY: {}}
    ) == (sqs.RETRY_REFUSAL_CLAIM_LOSS_HOLD)
    assert sqs.park_retry_refusal(
        PARK_REASON_ATTACH_FAILED, {CLAIM_RETIREMENT_KEY: {}}
    ) == (sqs.RETRY_REFUSAL_STOP_MARKERS)
    # JSON-encoded metadata (asyncpg may hand back text) is parsed.
    assert (
        sqs.park_retry_refusal(
            PARK_REASON_ATTACH_FAILED, '{"%s": {}}' % CLAIM_RETIREMENT_KEY
        )
        == sqs.RETRY_REFUSAL_STOP_MARKERS
    )
    # A malformed root fails closed; SQL NULL is simply "no markers".
    assert (
        sqs.park_retry_refusal(PARK_REASON_ATTACH_FAILED, "not json")
        == sqs.RETRY_REFUSAL_STOP_MARKERS
    )
    assert sqs.park_retry_refusal(PARK_REASON_ATTACH_FAILED, "[1, 2]") == (
        sqs.RETRY_REFUSAL_STOP_MARKERS
    )


def test_queue_block_shapes_parked_and_idle():
    parked = {
        "state": "parked",
        "park_reason": PARK_REASON_ATTACH_FAILED,
        "parked_at": NOW,
        "attempts": 3,
        "pending_input": True,
    }
    block = sqs.queue_block(parked, {})
    assert block == {
        "state": "parked",
        "park_reason": PARK_REASON_ATTACH_FAILED,
        "parked_at": NOW.isoformat(),
        "retryable": True,
        "attempts": 3,
        "pending_input": True,
    }
    held = sqs.queue_block(parked, {CLAIM_LOSS_HOLD_KEY: {}})
    assert held["retryable"] is False
    idle = sqs.queue_block({"state": "done", "park_reason": "stale", "attempts": 0}, {})
    assert idle["park_reason"] is None and idle["retryable"] is False
    assert sqs.queue_block(None, {})["state"] == "none"


# ---------------------------------------------------------------------------
# SQL contracts
# ---------------------------------------------------------------------------


def test_record_attach_failure_sql_contract():
    sql = q._RECORD_ATTACH_FAILURE_SQL
    # Fenced on the exact leased claim, like every other disposition CAS.
    assert "lease_token = $2::bigint AND state = 'leased'" in sql
    # The claim already counted the attempt: never incremented here.
    assert "attempts_since_completion + 1" not in sql
    # Consecutive same-signature counter resets when the signature changes.
    assert "last_error_signature IS NOT DISTINCT FROM $3::text" in sql
    assert "THEN attach_failures + 1 ELSE 1 END" in sql
    # Park on the claim count reaching max_attempts OR the signature threshold.
    assert "attempts >= max_attempts OR next_attach_failures >= $6::int" in sql
    assert "park_reason = CASE WHEN verdict.will_park THEN 'attach_failed'" in sql
    assert "run_after = now() + make_interval(secs => $5::float8)" in sql
    assert "RETURNING queue.state, queue.attempts_since_completion" in sql


def test_bounded_release_sql_parks_at_the_rows_own_budget():
    sql = q._RELEASE_BOUNDED_SQL
    # Same exact-lease fence as every other disposition CAS.
    assert "lease_token = $2::bigint AND state = 'leased'" in sql
    # The claim already counted the attempt; the row's own budget decides.
    assert "attempts_since_completion + 1" not in sql
    assert sql.count("WHEN attempts_since_completion >= max_attempts") == 3
    assert "THEN 'parked' ELSE 'queued' END" in sql
    assert "THEN $6::text ELSE park_reason END" in sql
    assert "last_error = COALESCE($7::text, last_error)" in sql
    # The ordinary error backoff is unchanged.
    assert "THEN $5::float8 * attempts_since_completion" in sql
    # The unbounded statement stays for shutdown / stop-boundary hand-backs.
    assert "max_attempts" not in q._RELEASE_SQL


@pytest.mark.asyncio
async def test_release_unit_routes_only_budgeted_callers_to_the_bounded_sql():
    class _ValConn:
        def __init__(self):
            self.calls = []

        async def fetchval(self, query, *args):
            self.calls.append((query, args))
            return "queued"

    conn = _ValConn()
    await release_unit(conn, unit_id=UNIT, lease_token=7, error=True)
    await release_unit(
        conn,
        unit_id=UNIT,
        lease_token=7,
        error=True,
        park_reason=PARK_REASON_RETRY_EXHAUSTED,
        last_error="bundle_409",
    )
    assert conn.calls[0] == (q._RELEASE_SQL, (UNIT, 7, 0.0, True, 5.0))
    assert conn.calls[1] == (
        q._RELEASE_BOUNDED_SQL,
        (UNIT, 7, 0.0, True, 5.0, PARK_REASON_RETRY_EXHAUSTED, "bundle_409"),
    )


def test_park_unpark_complete_and_reaper_sql_carry_the_lifecycle():
    assert "park_reason = COALESCE($3::text, park_reason)" in q._PARK_SQL
    assert "parked_at = now()" in q._PARK_SQL
    for cleared in (
        "attach_failures = 0",
        "park_reason = NULL",
        "parked_at = NULL",
        "last_error = NULL",
        "last_error_signature = NULL",
    ):
        assert cleared in q._UNPARK_SQL, cleared
    # A completed turn wipes the failure record but keeps park fields untouched
    # (a completing claim is by definition not parked).
    assert "attach_failures = 0" in q._COMPLETE_SQL
    assert "last_error_signature = NULL" in q._COMPLETE_SQL
    assert "'reaper_max_attempts'" in q._REAP_STEAL_SQL
    from orchestrator.services import run_queue_reaper as reaper

    assert "'reaper_max_attempts'" in reaper._STEAL_LOCKED_SESSION_SQL
    assert "park_reason = 'claim_loss_hold'" in reaper._PARK_CLAIM_LOSS_HOLD_SQL


def test_list_parked_sql_joins_thread_and_owner_newest_first():
    sql = q._LIST_PARKED_DETAIL_SQL
    assert "WHERE queue.state = 'parked'" in sql
    assert (
        "LEFT JOIN threads AS thread" in sql
        and "queue.unit_kind = 'session_turn'" in sql
    )
    assert "owner.preferred_username AS owner" in sql
    assert "ORDER BY COALESCE(queue.parked_at, queue.queued_at) DESC" in sql
    assert "LIMIT $1::int" in sql


# ---------------------------------------------------------------------------
# Read models on a fake connection
# ---------------------------------------------------------------------------


class _Conn:
    def __init__(self, row=None, rows=()):
        self.row = row
        self.rows = list(rows)
        self.calls: list[tuple[str, tuple]] = []

    async def fetchrow(self, query, *args):
        self.calls.append((query, args))
        return self.row

    async def fetch(self, query, *args):
        self.calls.append((query, args))
        return self.rows


@pytest.mark.asyncio
async def test_queue_state_for_reports_pending_input_null_safe():
    conn = _Conn(
        row={
            "state": "parked",
            "park_reason": PARK_REASON_ATTACH_FAILED,
            "parked_at": NOW,
            "last_error": "boom",
            "attempts_since_completion": 5,
            "max_attempts": 5,
            "attach_failures": 3,
            "input_seq": 74747,
            "consumed_seq": 74498,
            "run_after": NOW,
        }
    )
    state = await queue_state_for(conn, unit_id=UNIT)
    assert state["state"] == "parked" and state["pending_input"] is True
    assert state["attempts"] == 5 and state["attach_failures"] == 3
    assert conn.calls[0][0] == q._QUEUE_STATE_SQL and conn.calls[0][1] == (UNIT,)
    conn.row["consumed_seq"] = None
    assert (await queue_state_for(conn, unit_id=str(UNIT)))["pending_input"] is True
    conn.row["input_seq"] = None
    assert (await queue_state_for(conn, unit_id=UNIT))["pending_input"] is False
    assert await queue_state_for(_Conn(row=None), unit_id=UNIT) is None


@pytest.mark.asyncio
async def test_record_attach_failure_passes_the_contract_arguments():
    conn = _Conn(
        row={
            "state": "queued",
            "attempts_since_completion": 1,
            "attach_failures": 1,
            "park_reason": None,
            "run_after": NOW,
        }
    )
    row = await record_attach_failure(
        conn,
        unit_id=UNIT,
        lease_token=7,
        error="boom",
        signature="ValueError:boom",
        backoff_seconds=15.0,
    )
    assert row["state"] == "queued"
    query, args = conn.calls[0]
    assert query == q._RECORD_ATTACH_FAILURE_SQL
    assert args == (
        UNIT,
        7,
        "ValueError:boom",
        "boom",
        15.0,
        ATTACH_FAILURE_SIGNATURE_PARK_THRESHOLD,
    )
    assert (
        await record_attach_failure(
            _Conn(row=None),
            unit_id=UNIT,
            lease_token=7,
            error="e",
            signature="s",
            backoff_seconds=1.0,
        )
        is None
    )


@pytest.mark.asyncio
async def test_list_parked_bounds_the_limit():
    conn = _Conn(rows=[{"unit_id": UNIT, "unit_kind": "session_turn"}])
    rows = await list_parked(conn, limit=5000)
    assert rows == [{"unit_id": UNIT, "unit_kind": "session_turn"}]
    assert conn.calls[0] == (q._LIST_PARKED_DETAIL_SQL, (500,))
    await list_parked(conn, limit=0)
    assert conn.calls[1][1] == (1,)


# ---------------------------------------------------------------------------
# Executor: attach failure → record_attach_failure → journal on park
# ---------------------------------------------------------------------------


def _claim(
    attempts: int = 1, token: int = 7, kind: str = "session_turn"
) -> ClaimedUnit:
    return ClaimedUnit(
        unit_id=UNIT,
        unit_kind=kind,
        fair_key=None,
        lease_token=token,
        input_seq=74747,
        consumed_seq=74498,
        attempts_since_completion=attempts,
        leased_until=NOW,
    )


class _TxConn:
    """A bare connection for the thread-locked release transaction.

    The queue statements and the frame append are patched per test; this
    answers only the thread lock (a stateless thread) and the epoch bump.
    """

    def __init__(self):
        self.transactions = 0
        self.bumped = 0

    def transaction(self):
        conn = self

        class _Tx:
            async def __aenter__(self):
                conn.transactions += 1

            async def __aexit__(self, *exc):
                return False

        return _Tx()

    async def fetchrow(self, query, *args):
        if query == te._LOCK_RELEASE_THREAD_SQL:
            return {"execution_lane": "stateless", "agent_id": None, "metadata": {}}
        if "SET events_epoch = events_epoch + 1" in query:
            self.bumped += 1
            return {"events_epoch": 4}
        return None

    async def fetch(self, query, *args):
        return []  # no permission prompt pending under the parked token


@pytest.fixture
def executor(monkeypatch):
    ex = te.StatelessTurnExecutor(pod_name="test-pod")
    # ``_db`` resolves the agent's pool through the persistent app; the queue
    # statements are patched below, so a bare transaction-capable stand-in
    # will do (real-Postgres proofs: test_session_release_budget_real_postgres).
    ex.test_conn = _TxConn()
    monkeypatch.setattr(
        te.StatelessTurnExecutor, "_db", property(lambda self: self.test_conn)
    )
    monkeypatch.setattr(te, "_pa", lambda: MagicMock())
    monkeypatch.setattr(ex, "_quiesce_claim_before_transition", AsyncMock())
    monkeypatch.setattr(ex, "_ack_terminal_claim_loss", AsyncMock())
    monkeypatch.setattr(ex, "_clear_claim_tool_effect", MagicMock())
    monkeypatch.setattr(ex, "_exact_claim_handle_lost", MagicMock(return_value=True))
    return ex


@pytest.mark.asyncio
async def test_attach_failure_requeues_with_backoff_and_no_journal(
    executor, monkeypatch
):
    record = AsyncMock(
        return_value={
            "state": "queued",
            "attempts_since_completion": 2,
            "attach_failures": 1,
            "park_reason": None,
            "run_after": NOW,
        }
    )
    journal = AsyncMock()
    monkeypatch.setattr(te, "record_attach_failure", record)
    monkeypatch.setattr(te, "append_system_frame", journal)

    await executor._release_attach_failure(
        _claim(attempts=2), RuntimeError("subagent 400")
    )

    record.assert_awaited_once()
    kwargs = record.await_args.kwargs
    assert kwargs["unit_id"] == UNIT and kwargs["lease_token"] == 7
    assert kwargs["signature"] == te._error_signature(RuntimeError("subagent 400"))
    assert kwargs["error"] == "subagent 400"
    assert kwargs["backoff_seconds"] == attach_failure_backoff_seconds(2) == 15.0
    journal.assert_not_awaited()
    executor._quiesce_claim_before_transition.assert_awaited_once()
    executor._clear_claim_tool_effect.assert_called_once()


@pytest.mark.asyncio
async def test_attach_failure_park_journals_turn_parked(executor, monkeypatch):
    record = AsyncMock(
        return_value={
            "state": "parked",
            "attempts_since_completion": 3,
            "attach_failures": 3,
            "park_reason": PARK_REASON_ATTACH_FAILED,
            "run_after": NOW,
        }
    )
    journal = AsyncMock(return_value=(0, 500))
    monkeypatch.setattr(te, "record_attach_failure", record)
    monkeypatch.setattr(te, "append_system_frame", journal)

    await executor._release_attach_failure(
        _claim(attempts=3), RuntimeError("subagent 400")
    )

    journal.assert_awaited_once()
    kwargs = journal.await_args.kwargs
    assert kwargs["thread_id"] == str(UNIT) and kwargs["kind"] == "turn.parked"
    assert kwargs["payload"]["reason"] == PARK_REASON_ATTACH_FAILED
    assert kwargs["payload"]["attempts"] == 3
    assert kwargs["payload"]["retryable"] is True
    assert kwargs["payload"]["error"] == "subagent 400"
    assert kwargs["payload"]["parked_by"] == "test-pod"
    # Same transaction as the CAS, on a fresh epoch edge.
    assert journal.await_args.args == (executor.test_conn,)
    assert record.await_args.args == (executor.test_conn,)
    assert executor.test_conn.transactions == 1
    assert executor.test_conn.bumped == 1


@pytest.mark.asyncio
async def test_exhausted_error_release_parks_and_journals_retry_exhausted(
    executor, monkeypatch
):
    release = AsyncMock(return_value="parked")
    journal = AsyncMock(return_value=(4, 1))
    monkeypatch.setattr(te, "release_unit", release)
    monkeypatch.setattr(te, "append_system_frame", journal)

    await executor._release(_claim(attempts=5), reason="bundle_409")

    kwargs = release.await_args.kwargs
    assert kwargs["park_reason"] == PARK_REASON_RETRY_EXHAUSTED
    assert kwargs["last_error"] == "bundle_409" and kwargs["error"] is True
    payload = journal.await_args.kwargs["payload"]
    assert payload == {
        "reason": PARK_REASON_RETRY_EXHAUSTED,
        "release_reason": "bundle_409",
        "attempts": 5,
        "retryable": True,
        "parked_by": "test-pod",
        "lease_token": 7,
    }
    assert executor.test_conn.transactions == 1
    assert executor.test_conn.bumped == 1
    executor._ack_terminal_claim_loss.assert_not_awaited()


@pytest.mark.asyncio
async def test_under_budget_error_release_requeues_silently(executor, monkeypatch):
    monkeypatch.setattr(te, "release_unit", AsyncMock(return_value="queued"))
    journal = AsyncMock()
    monkeypatch.setattr(te, "append_system_frame", journal)

    await executor._release(_claim(attempts=2), reason="loop_not_ready")

    journal.assert_not_awaited()
    assert executor.test_conn.bumped == 0
    executor._clear_claim_tool_effect.assert_called_once()


@pytest.mark.asyncio
async def test_attach_failure_fenced_out_acks_loss_without_journal(
    executor, monkeypatch
):
    monkeypatch.setattr(te, "record_attach_failure", AsyncMock(return_value=None))
    journal = AsyncMock()
    monkeypatch.setattr(te, "append_system_frame", journal)

    await executor._release_attach_failure(_claim(), RuntimeError("x"))

    executor._ack_terminal_claim_loss.assert_awaited_once()
    journal.assert_not_awaited()


@pytest.mark.asyncio
async def test_attach_failure_after_local_loss_never_touches_the_row(
    executor, monkeypatch
):
    record = AsyncMock()
    monkeypatch.setattr(te, "record_attach_failure", record)
    executor._lease.mark_lost()

    await executor._release_attach_failure(_claim(), RuntimeError("x"))

    record.assert_not_awaited()
    executor._ack_terminal_claim_loss.assert_awaited_once()


@pytest.mark.asyncio
async def test_journal_failure_is_contained_and_worker_units_skip_it(
    executor, monkeypatch
):
    journal = AsyncMock(side_effect=RuntimeError("journal down"))
    monkeypatch.setattr(te, "append_system_frame", journal)
    await executor._journal_parked(
        _claim(), reason="attach_failed", error=None, attempts=1
    )
    journal.assert_awaited_once()
    journal.reset_mock()
    await executor._journal_parked(
        _claim(kind="worker_batch"), reason="attach_failed", error=None, attempts=1
    )
    journal.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_effect_park_records_reason_and_journals(executor, monkeypatch):
    park = AsyncMock(return_value="parked")
    journal = AsyncMock()
    monkeypatch.setattr(te, "park_unit", park)
    monkeypatch.setattr(te, "append_system_frame", journal)

    state = await executor._park_post_effect_claim(
        MagicMock(), _claim(attempts=2), reason="uncooperative_shutdown"
    )

    assert state == "parked"
    assert park.await_args.kwargs["reason"] == PARK_REASON_SHUTDOWN_CANCELLED
    payload = journal.await_args.kwargs["payload"]
    assert (
        payload["reason"] == PARK_REASON_SHUTDOWN_CANCELLED
        and payload["retryable"] is True
    )
    assert payload["attempts"] == 2
