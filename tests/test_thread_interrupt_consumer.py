"""Focused runtime tests for the exact-turn durable interrupt inbox."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

import agent.api.persistent_app as pa
import agent.api.turn_executor as te
from agent.api.lease_context import LeaseHandle, current_lease
from shared.thread_interrupts import (
    InterruptInputConsumption,
    InterruptReceipt,
    InterruptRequest,
    consume_applied_interrupt_input_live,
    fetch_stale_interrupt_requests,
    finalize_interrupt_request,
    interrupt_receipt_result,
    owner_fence_current,
)


THREAD_ID = UUID("11111111-1111-4111-8111-111111111111")
REQUEST_ID = UUID("22222222-2222-4222-8222-222222222222")
CLIENT_REQUEST_ID = UUID("33333333-3333-4333-8333-333333333333")


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _pool(conn=None):
    return SimpleNamespace(acquire=lambda: _Acquire(conn or MagicMock()))


def _request() -> InterruptRequest:
    return InterruptRequest(
        id=REQUEST_ID,
        thread_id=THREAD_ID,
        client_request_id=CLIENT_REQUEST_ID,
        target_turn_id=4,
        accepted_lease_token=9,
        accepted_leased_by="executor-a",
    )


def _receipt(*, applied: bool = True) -> InterruptReceipt:
    payload = {
        "request_id": str(REQUEST_ID),
        "client_request_id": str(CLIENT_REQUEST_ID),
        "target_turn_id": 4,
        "applied": applied,
    }
    if applied:
        payload["mode"] = "hard"
    else:
        payload["error_code"] = "target_turn_not_active"
    return InterruptReceipt(epoch=3, seq=12, kind="interrupt.ack", payload=payload)


@pytest.mark.asyncio
async def test_stale_fetch_has_no_old_batch_limit():
    rows = [
        {
            "id": uuid4(),
            "thread_id": THREAD_ID,
            "client_request_id": uuid4(),
            "target_turn_id": 4,
            "accepted_lease_token": 9,
            "accepted_leased_by": "executor-a",
            "outcome": None,
            "result": None,
        }
        for _ in range(1001)
    ]
    conn = SimpleNamespace(fetch=AsyncMock(return_value=rows))

    requests = await fetch_stale_interrupt_requests(
        conn,
        thread_id=THREAD_ID,
        current_lease_token=10,
    )

    assert len(requests) == 1001
    sql = conn.fetch.await_args.args[0]
    assert "LIMIT" not in sql
    assert conn.fetch.await_args.args[1:] == (THREAD_ID, 10)


@pytest.mark.asyncio
async def test_owner_fence_locks_thread_before_exact_queue():
    conn = SimpleNamespace(fetchrow=AsyncMock(side_effect=[{"one": 1}, {"one": 1}]))

    assert await owner_fence_current(conn, thread_id=THREAD_ID, lease_token=9)

    thread_call, queue_call = conn.fetchrow.await_args_list
    assert "FROM threads" in thread_call.args[0]
    assert "FOR SHARE" in thread_call.args[0]
    assert "FROM run_queue" in queue_call.args[0]
    assert "FOR SHARE" in queue_call.args[0]
    assert queue_call.args[1:] == (THREAD_ID, 9)


@pytest.fixture
def stateless_owner(monkeypatch):
    session = SimpleNamespace(
        postgres_conn=_pool(),
        turn_count=4,
    )
    monkeypatch.setattr(pa, "_session", session)
    monkeypatch.setattr(pa, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa, "_turn_event_open", True)
    monkeypatch.setattr(pa._session_input, "_interrupt_mode", None)
    monkeypatch.setattr(pa._session_input, "_hard_interrupt_event", asyncio.Event())
    monkeypatch.setattr(pa, "_tool_inflight", False)
    handle = LeaseHandle()
    handle.update(str(THREAD_ID), 9)
    token = current_lease.set(handle)
    try:
        yield session, handle
    finally:
        current_lease.reset(token)


@pytest.mark.asyncio
async def test_signal_precedes_receipt_and_finalization(stateless_owner):
    request = _request()
    order: list[str] = []

    def signal(turn_id):
        order.append(f"signal:{turn_id}")
        return "hard"

    async def journal(_kind, _params, **_kwargs):
        order.append("journal")
        return 3, 12

    async def finalize(_request, **_kwargs):
        order.append("finalize")
        return "applied"

    with (
        patch(
            "shared.thread_interrupts.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.fetch_next_interrupt_request",
            AsyncMock(side_effect=[request, None]),
        ),
        patch(
            "shared.thread_interrupts.fetch_interrupt_receipt",
            AsyncMock(return_value=None),
        ),
        patch.object(
            pa._session_input, "signal_interrupt_for_turn", side_effect=signal
        ),
        patch.object(pa, "_broadcast_interrupt_durable", side_effect=journal) as write,
        patch.object(pa, "_finalize_durable_interrupt", side_effect=finalize) as finish,
    ):
        assert (
            await pa._drain_thread_interrupts(
                lease_token=9,
                target_turn_id=4,
            )
            == 1
        )

    assert order == ["signal:4", "journal", "finalize"]
    write.assert_awaited_once_with(
        "interrupt.ack",
        {
            "request_id": str(REQUEST_ID),
            "client_request_id": str(CLIENT_REQUEST_ID),
            "target_turn_id": 4,
            "applied": True,
            "mode": "hard",
        },
        interrupt_request_id=str(REQUEST_ID),
        lease_token=9,
    )
    finish.assert_awaited_once_with(
        request,
        lease_token=9,
        outcome="applied",
        mode="hard",
        error_code=None,
    )


@pytest.mark.asyncio
async def test_receipt_recovery_never_resignals_or_rejournals(stateless_owner):
    request = _request()
    signal = MagicMock()
    journal = AsyncMock()
    finalize = AsyncMock(return_value="applied")

    with (
        patch(
            "shared.thread_interrupts.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.fetch_next_interrupt_request",
            AsyncMock(side_effect=[request, None]),
        ),
        patch(
            "shared.thread_interrupts.fetch_interrupt_receipt",
            AsyncMock(return_value=_receipt()),
        ),
        patch.object(pa._session_input, "signal_interrupt_for_turn", signal),
        patch.object(pa, "_broadcast_interrupt_durable", journal),
        patch.object(pa, "_finalize_durable_interrupt", finalize),
    ):
        assert (
            await pa._drain_thread_interrupts(
                lease_token=9,
                target_turn_id=4,
            )
            == 1
        )

    signal.assert_not_called()
    journal.assert_not_awaited()
    finalize.assert_awaited_once_with(
        request,
        lease_token=9,
        outcome="applied",
        mode="hard",
        error_code=None,
    )


@pytest.mark.asyncio
async def test_stop_joins_slow_receipt_without_cancel_or_duplicate(
    stateless_owner, monkeypatch
):
    """Closing past the old 1s timeout cannot detach a live receipt wait."""

    request = _request()
    receipt_started = asyncio.Event()
    receipt_commit = asyncio.Event()
    journal_cancelled = False

    async def slow_journal(_kind, _params, **_kwargs):
        nonlocal journal_cancelled
        receipt_started.set()
        try:
            await receipt_commit.wait()
        except asyncio.CancelledError:
            journal_cancelled = True
            raise
        return 3, 12

    signal = MagicMock(return_value="hard")
    journal = AsyncMock(side_effect=slow_journal)
    finalize = AsyncMock(return_value="applied")
    fetch_next = AsyncMock(side_effect=[request, None, None])

    with (
        patch(
            "shared.thread_interrupts.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.fetch_next_interrupt_request",
            fetch_next,
        ),
        patch(
            "shared.thread_interrupts.fetch_interrupt_receipt",
            AsyncMock(return_value=None),
        ),
        patch.object(pa._session_input, "signal_interrupt_for_turn", signal),
        patch.object(pa, "_broadcast_interrupt_durable", journal),
        patch.object(pa, "_finalize_durable_interrupt", finalize),
    ):
        watcher = asyncio.create_task(
            pa._drain_thread_interrupts(lease_token=9, target_turn_id=4)
        )
        await receipt_started.wait()
        stop = asyncio.Event()
        monkeypatch.setattr(pa, "_interrupt_watcher_task", watcher)
        monkeypatch.setattr(pa, "_interrupt_watcher_stop", stop)
        monkeypatch.setattr(pa, "_interrupt_owner_lease_token", 9)
        monkeypatch.setattr(pa, "_interrupt_owner_turn_id", 4)

        monkeypatch.setattr(pa, "_agent", SimpleNamespace(postgres_conn=object()))
        executor = te.StatelessTurnExecutor(pod_name="executor-a")
        executor._lease.update(str(THREAD_ID), 9)
        claim = SimpleNamespace(unit_id=THREAD_ID, lease_token=9)
        close_admission = AsyncMock(return_value=True)

        with patch.object(te, "close_interrupt_admission", close_admission):
            closing = asyncio.create_task(
                executor._close_interrupt_window(
                    pa,
                    claim,
                    target_turn_id=4,
                )
            )
            await asyncio.sleep(0)
            close_admission.assert_awaited_once()
            # Hold the simulated writer beyond the removed one-second grace.
            # Close must still await the original consumer, never cancel it.
            await asyncio.sleep(1.05)
            assert not closing.done()
            assert not watcher.done()
            assert not journal_cancelled
            signal.assert_called_once_with(4)
            journal.assert_awaited_once()
            finalize.assert_not_awaited()

            receipt_commit.set()
            assert await closing is True
            assert await watcher == 1

    assert stop.is_set()
    assert not journal_cancelled
    signal.assert_called_once_with(4)
    journal.assert_awaited_once()
    finalize.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_racing_start_waits_for_atomic_task_publication(
    stateless_owner, monkeypatch
):
    """Stop cannot clear owner globals while start awaits its initial drain."""

    drain_started = asyncio.Event()
    allow_drain = asyncio.Event()
    watcher_tasks: list[asyncio.Task] = []
    watcher_bindings: list[tuple[int, int]] = []

    async def slow_initial_drain(*, lease_token, target_turn_id):
        assert (lease_token, target_turn_id) == (9, 4)
        drain_started.set()
        await allow_drain.wait()
        return 0

    async def fake_watcher_loop(
        *, postgres_conn, thread_id, stop, lease_token, target_turn_id
    ):
        del postgres_conn, thread_id
        watcher_tasks.append(asyncio.current_task())
        watcher_bindings.append((lease_token, target_turn_id))
        await stop.wait()

    monkeypatch.setattr(pa, "_interrupt_watcher_lifecycle_lock", asyncio.Lock())
    monkeypatch.setattr(pa, "_interrupt_watcher_task", None)
    monkeypatch.setattr(pa, "_interrupt_watcher_stop", None)
    monkeypatch.setattr(pa, "_interrupt_owner_lease_token", None)
    monkeypatch.setattr(pa, "_interrupt_owner_turn_id", None)

    with (
        patch.object(pa, "_drain_thread_interrupts", side_effect=slow_initial_drain),
        patch.object(pa, "_interrupt_watcher_loop", side_effect=fake_watcher_loop),
    ):
        starting = asyncio.create_task(
            pa._start_thread_interrupt_watcher(
                lease_token=9,
                target_turn_id=4,
            )
        )
        await drain_started.wait()
        assert pa._interrupt_owner_lease_token == 9
        assert pa._interrupt_owner_turn_id == 4
        assert pa._interrupt_watcher_task is None

        stopping = asyncio.create_task(pa._stop_thread_interrupt_watcher())
        await asyncio.sleep(0)
        assert not stopping.done()
        # The lifecycle lock keeps stop from clearing this unpublished start.
        assert pa._interrupt_owner_lease_token == 9
        assert pa._interrupt_owner_turn_id == 4

        allow_drain.set()
        assert await starting == 0
        await stopping

    assert watcher_bindings == [(9, 4)]
    assert len(watcher_tasks) == 1
    assert watcher_tasks[0] is not None and watcher_tasks[0].done()
    assert pa._interrupt_watcher_task is None
    assert pa._interrupt_watcher_stop is None
    assert pa._interrupt_owner_lease_token is None
    assert pa._interrupt_owner_turn_id is None


@pytest.mark.asyncio
async def test_terminal_edge_is_durably_rejected_without_ram_signal(
    stateless_owner, monkeypatch
):
    request = _request()
    monkeypatch.setattr(pa, "_turn_event_open", False)
    journal = AsyncMock(return_value=(3, 12))
    finalize = AsyncMock(return_value="rejected")

    with (
        patch(
            "shared.thread_interrupts.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.fetch_next_interrupt_request",
            AsyncMock(side_effect=[request, None]),
        ),
        patch(
            "shared.thread_interrupts.fetch_interrupt_receipt",
            AsyncMock(return_value=None),
        ),
        patch.object(pa, "_broadcast_interrupt_durable", journal),
        patch.object(pa, "_finalize_durable_interrupt", finalize),
    ):
        assert (
            await pa._drain_thread_interrupts(
                lease_token=9,
                target_turn_id=4,
            )
            == 1
        )

    assert pa._session_input.interrupt_mode is None
    params = journal.await_args.args[1]
    assert params == {
        "request_id": str(REQUEST_ID),
        "client_request_id": str(CLIENT_REQUEST_ID),
        "target_turn_id": 4,
        "applied": False,
        "error_code": "target_turn_not_active",
    }
    finalize.assert_awaited_once_with(
        request,
        lease_token=9,
        outcome="rejected",
        mode=None,
        error_code="target_turn_not_active",
    )


@pytest.mark.asyncio
async def test_journal_failure_leaves_request_unfinalized(stateless_owner):
    request = _request()
    finalize = AsyncMock()
    with (
        patch(
            "shared.thread_interrupts.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.fetch_next_interrupt_request",
            AsyncMock(return_value=request),
        ),
        patch(
            "shared.thread_interrupts.fetch_interrupt_receipt",
            AsyncMock(return_value=None),
        ),
        patch.object(
            pa,
            "_broadcast_interrupt_durable",
            AsyncMock(side_effect=pa.EventJournalUnavailable("writer failed")),
        ),
        patch.object(pa, "_finalize_durable_interrupt", finalize),
    ):
        with pytest.raises(pa.InterruptInboxBlocked):
            await pa._drain_thread_interrupts(lease_token=9, target_turn_id=4)
    finalize.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_local_handle_never_reads_or_signals(stateless_owner):
    _session, handle = stateless_owner
    handle.update(str(THREAD_ID), 10)
    fetch = AsyncMock()
    signal = MagicMock()
    with (
        patch("shared.thread_interrupts.fetch_next_interrupt_request", fetch),
        patch.object(pa._session_input, "signal_interrupt_for_turn", signal),
    ):
        with pytest.raises(pa.InterruptInboxBlocked):
            await pa._drain_thread_interrupts(lease_token=9, target_turn_id=4)
    assert handle.lost.is_set()
    fetch.assert_not_awaited()
    signal.assert_not_called()


class _ReconcileConn:
    def __init__(self, consumed_seq=5, *, stale_permissions=False):
        self.consumed_seq = consumed_seq
        self.stale_permissions = stale_permissions

    def transaction(self):
        return _Transaction()

    async def fetchrow(self, sql, *args):
        if "SELECT consumed_seq FROM run_queue" in sql:
            return {"consumed_seq": self.consumed_seq}
        raise AssertionError(sql)

    async def fetchval(self, sql, *args):
        if "thread_permission_requests" in sql:
            return self.stale_permissions
        raise AssertionError(sql)


@pytest.mark.asyncio
async def test_permission_only_successor_recovery_rotates_after_lock_release(
    stateless_owner,
):
    session, handle = stateless_owner
    handle.update(str(THREAD_ID), 10)
    order: list[str] = []

    class TrackingTxn:
        async def __aenter__(self):
            order.append("txn_enter")
            return self

        async def __aexit__(self, *_args):
            order.append("txn_exit")
            return False

    conn = _ReconcileConn(consumed_seq=5, stale_permissions=True)
    conn.transaction = TrackingTxn
    session.postgres_conn = _pool(conn)

    async def rotate(**_kwargs):
        order.append("rotate")
        return 4

    with (
        patch(
            "shared.thread_interrupts.owner_fence_current_for_update",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.fetch_stale_interrupt_requests",
            AsyncMock(return_value=[]),
        ),
        patch.object(pa, "_rotate_thread_interrupt_recovery_epoch", rotate),
    ):
        result = await asyncio.wait_for(
            pa._reconcile_stale_thread_interrupts(lease_token=10), timeout=1
        )

    assert result == (0, 5)
    assert order == ["txn_enter", "txn_exit", "rotate"]


@pytest.mark.asyncio
async def test_stale_permission_probe_includes_legacy_null(stateless_owner):
    session, handle = stateless_owner
    handle.update(str(THREAD_ID), 10)
    conn = _ReconcileConn(consumed_seq=5, stale_permissions=False)
    session.postgres_conn = _pool(conn)

    with (
        patch(
            "shared.thread_interrupts.owner_fence_current_for_update",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.fetch_stale_interrupt_requests",
            AsyncMock(return_value=[]),
        ),
    ):
        assert await pa._reconcile_stale_thread_interrupts(lease_token=10) == (0, 5)

    assert "accepted_lease_token IS NULL" in pa._STALE_PERMISSION_EXISTS_SQL


@pytest.mark.asyncio
async def test_owner_crash_before_receipt_applies_stop_without_successor_signal(
    stateless_owner,
):
    session, handle = stateless_owner
    handle.update(str(THREAD_ID), 10)
    conn = _ReconcileConn(consumed_seq=1)
    session.postgres_conn = _pool(conn)
    request = _request()
    order: list[str] = []

    async def rotate(**_kwargs):
        order.append("rotate")
        return 4

    async def ack(*_args, **_kwargs):
        order.append("ack")
        return 4, 1

    async def boundary(*_args, **_kwargs):
        order.append("boundary")
        return 4, 2

    async def finalize(*_args, **_kwargs):
        order.append("finalize")
        conn.consumed_seq = 5
        return "applied"

    with (
        patch(
            "shared.thread_interrupts.owner_fence_current_for_update",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.fetch_stale_interrupt_requests",
            AsyncMock(return_value=[request]),
        ) as fetch_stale,
        patch(
            "shared.thread_interrupts.fetch_interrupt_receipt",
            AsyncMock(return_value=None),
        ),
        patch.object(pa, "_rotate_thread_interrupt_recovery_epoch", rotate),
        patch.object(pa, "_broadcast_interrupt_durable", side_effect=ack) as write,
        patch.object(pa, "_broadcast_event_durable", side_effect=boundary) as edge,
        patch.object(pa, "_finalize_durable_interrupt", side_effect=finalize) as finish,
        patch.object(pa._session_input, "signal_interrupt_for_turn") as signal,
    ):
        assert await pa._reconcile_stale_thread_interrupts(lease_token=10) == (
            1,
            5,
        )

    assert order == ["rotate", "ack", "boundary", "finalize"]
    fetch_stale.assert_awaited_once()
    signal.assert_not_called()
    write.assert_awaited_once_with(
        "interrupt.ack",
        {
            "request_id": str(REQUEST_ID),
            "client_request_id": str(CLIENT_REQUEST_ID),
            "target_turn_id": 4,
            "applied": True,
            "mode": "hard",
            "reason": "owner_lost",
            "owner_loss_reason": "lease_expired",
        },
        interrupt_request_id=str(REQUEST_ID),
        lease_token=10,
        accepted_lease_token=9,
        stale_recovery=True,
    )
    assert edge.await_args.args[0] == "turn.interrupted"
    assert edge.await_args.args[1]["target_turn_id"] == 4
    assert "target_turn_ids" not in edge.await_args.args[1]
    finish.assert_awaited_once_with(
        request,
        lease_token=10,
        outcome="applied",
        mode="hard",
        error_code=None,
        accepted_lease_token=9,
        stale_recovery=True,
    )


@pytest.mark.asyncio
async def test_owner_crash_after_receipt_finalizes_without_successor_signal(
    stateless_owner,
):
    session, handle = stateless_owner
    handle.update(str(THREAD_ID), 10)
    conn = _ReconcileConn(consumed_seq=1)
    session.postgres_conn = _pool(conn)
    request = _request()

    async def finalize(*_args, **_kwargs):
        conn.consumed_seq = 5
        return "applied"

    with (
        patch(
            "shared.thread_interrupts.owner_fence_current_for_update",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.fetch_stale_interrupt_requests",
            AsyncMock(return_value=[request]),
        ),
        patch(
            "shared.thread_interrupts.fetch_interrupt_receipt",
            AsyncMock(return_value=_receipt()),
        ),
        patch.object(
            pa, "_rotate_thread_interrupt_recovery_epoch", AsyncMock(return_value=4)
        ),
        patch.object(
            pa, "_broadcast_event_durable", AsyncMock(return_value=(4, 1))
        ) as edge,
        patch.object(pa, "_broadcast_interrupt_durable") as write,
        patch.object(pa, "_finalize_durable_interrupt", side_effect=finalize) as finish,
        patch.object(pa._session_input, "signal_interrupt_for_turn") as signal,
    ):
        assert await pa._reconcile_stale_thread_interrupts(lease_token=10) == (
            1,
            5,
        )

    write.assert_not_awaited()
    signal.assert_not_called()
    finish.assert_awaited_once_with(
        request,
        lease_token=10,
        outcome="applied",
        mode="hard",
        error_code=None,
        accepted_lease_token=9,
        stale_recovery=True,
    )
    assert edge.await_args.args[0] == "turn.interrupted"


@pytest.mark.asyncio
async def test_stale_recovery_emits_one_singular_boundary_per_exact_target(
    stateless_owner,
):
    session, handle = stateless_owner
    handle.update(str(THREAD_ID), 10)
    conn = _ReconcileConn(consumed_seq=6)
    session.postgres_conn = _pool(conn)
    second_request_id = UUID("55555555-5555-4555-8555-555555555555")
    second_client_id = UUID("66666666-6666-4666-8666-666666666666")
    requests = [
        _request(),
        InterruptRequest(
            id=second_request_id,
            thread_id=THREAD_ID,
            client_request_id=second_client_id,
            target_turn_id=5,
            accepted_lease_token=9,
            accepted_leased_by="executor-a",
        ),
    ]
    second_receipt = InterruptReceipt(
        epoch=3,
        seq=13,
        kind="interrupt.ack",
        payload={
            "request_id": str(second_request_id),
            "client_request_id": str(second_client_id),
            "target_turn_id": 5,
            "applied": True,
            "mode": "hard",
        },
    )
    boundaries = AsyncMock(side_effect=[(4, 1), (4, 2)])

    with (
        patch(
            "shared.thread_interrupts.owner_fence_current_for_update",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.fetch_stale_interrupt_requests",
            AsyncMock(return_value=requests),
        ),
        patch(
            "shared.thread_interrupts.fetch_interrupt_receipt",
            AsyncMock(side_effect=[_receipt(), second_receipt]),
        ),
        patch.object(
            pa, "_rotate_thread_interrupt_recovery_epoch", AsyncMock(return_value=4)
        ) as rotate,
        patch.object(pa, "_broadcast_event_durable", boundaries),
        patch.object(
            pa, "_finalize_durable_interrupt", AsyncMock(return_value="applied")
        ),
        patch.object(pa._session_input, "signal_interrupt_for_turn") as signal,
    ):
        assert await pa._reconcile_stale_thread_interrupts(lease_token=10) == (
            2,
            6,
        )

    rotate.assert_awaited_once_with(lease_token=10)
    assert [call.args[1]["target_turn_id"] for call in boundaries.await_args_list] == [
        4,
        5,
    ]
    assert all(
        "target_turn_ids" not in call.args[1] for call in boundaries.await_args_list
    )
    signal.assert_not_called()


@pytest.mark.asyncio
async def test_terminal_applied_stale_row_settles_and_makes_progress_without_loop(
    stateless_owner,
):
    session, handle = stateless_owner
    handle.update(str(THREAD_ID), 10)
    conn = _ReconcileConn(consumed_seq=5)
    session.postgres_conn = _pool(conn)
    request = InterruptRequest(
        id=REQUEST_ID,
        thread_id=THREAD_ID,
        client_request_id=CLIENT_REQUEST_ID,
        target_turn_id=4,
        accepted_lease_token=9,
        accepted_leased_by="executor-a",
        outcome="applied",
        result={"applied": True},
    )
    consumption = InterruptInputConsumption(5, "leased", True, False, True)
    fetch_stale = AsyncMock(return_value=[request])
    consume = AsyncMock(return_value=consumption)

    with (
        patch(
            "shared.thread_interrupts.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.owner_fence_current_for_update",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_interrupts.fetch_stale_interrupt_requests",
            fetch_stale,
        ),
        patch(
            "shared.thread_interrupts.fetch_interrupt_receipt",
            AsyncMock(return_value=_receipt()),
        ),
        patch(
            "shared.thread_interrupts.consume_applied_interrupt_input_live",
            consume,
        ),
        patch.object(
            pa, "_rotate_thread_interrupt_recovery_epoch", AsyncMock(return_value=4)
        ),
        patch.object(pa, "_broadcast_event_durable", AsyncMock(return_value=(4, 1))),
        patch.object(pa, "_broadcast_interrupt_durable") as ack,
        patch.object(pa, "_finalize_durable_interrupt") as finalize,
        patch.object(pa._session_input, "signal_interrupt_for_turn") as signal,
    ):
        assert await pa._reconcile_stale_thread_interrupts(lease_token=10) == (
            1,
            5,
        )

    fetch_stale.assert_awaited_once()
    consume.assert_awaited_once_with(
        conn,
        thread_id=str(THREAD_ID),
        current_lease_token=10,
        accepted_lease_token=9,
        target_turn_id=4,
        request_id=REQUEST_ID,
    )
    ack.assert_not_awaited()
    finalize.assert_not_awaited()
    signal.assert_not_called()


@pytest.mark.asyncio
async def test_recovery_epoch_closes_old_writer_before_exact_fenced_bump(
    stateless_owner, monkeypatch
):
    session, handle = stateless_owner
    handle.update(str(THREAD_ID), 10)
    order: list[str] = []

    class EpochConn:
        def transaction(self):
            return _Transaction()

        async def fetchrow(self, sql, *args):
            if "FROM threads" in sql:
                order.append("lock_thread")
            elif "FROM run_queue" in sql:
                order.append("lock_queue")
            else:
                raise AssertionError(sql)
            return {"one": 1}

    async def close_old():
        order.append("close_old")

    async def bump(_conn, *, thread_id):
        assert thread_id == str(THREAD_ID)
        order.append("bump")
        return 4

    old_writer = SimpleNamespace(thread_id=str(THREAD_ID), close=close_old)
    new_writer = MagicMock()
    new_writer.start.side_effect = lambda: order.append("start_new")
    session.postgres_conn = _pool(EpochConn())
    monkeypatch.setattr(pa, "_event_writer", old_writer)
    monkeypatch.setattr(pa, "_events_epoch", 3)
    monkeypatch.setattr(pa, "_next_seq", 99)

    retirement = SimpleNamespace(epoch_bumped=False, receipts=(), count=0)
    with (
        patch.object(pa._event_journal, "bump_epoch", side_effect=bump),
        patch(
            "shared.session_permission_retirement.retire_stale_stateless_permissions",
            AsyncMock(return_value=retirement),
        ),
        patch.object(pa, "_OrderedPersistentEventWriter", return_value=new_writer),
    ):
        assert await pa._rotate_thread_interrupt_recovery_epoch(lease_token=10) == 4

    assert order == ["close_old", "lock_thread", "lock_queue", "bump", "start_new"]
    assert pa._events_epoch == 4
    assert pa._next_seq == 0
    assert pa._event_writer is new_writer


@pytest.mark.asyncio
async def test_recovery_epoch_seeds_after_multiple_permission_receipts(
    stateless_owner, monkeypatch
):
    session, handle = stateless_owner
    handle.update(str(THREAD_ID), 10)

    class EpochConn:
        def transaction(self):
            return _Transaction()

        async def fetchrow(self, sql, *_args):
            if "FROM threads" in sql or "FROM run_queue" in sql:
                return {"one": 1}
            raise AssertionError(sql)

    async def close_old():
        return None

    receipts = (
        SimpleNamespace(epoch=4, seq=1),
        SimpleNamespace(epoch=4, seq=2),
    )
    retirement = SimpleNamespace(epoch_bumped=True, receipts=receipts, count=2)
    old_writer = SimpleNamespace(thread_id=str(THREAD_ID), close=close_old)
    new_writer = MagicMock()
    new_writer.thread_id = str(THREAD_ID)
    session.postgres_conn = _pool(EpochConn())
    monkeypatch.setattr(pa, "_event_writer", old_writer)
    monkeypatch.setattr(pa, "_events_epoch", 3)
    monkeypatch.setattr(pa, "_next_seq", 99)

    with (
        patch(
            "shared.session_permission_retirement.retire_stale_stateless_permissions",
            AsyncMock(return_value=retirement),
        ),
        patch.object(pa._event_journal, "bump_epoch") as bump,
        patch.object(pa, "_OrderedPersistentEventWriter", return_value=new_writer),
    ):
        assert await pa._rotate_thread_interrupt_recovery_epoch(lease_token=10) == 4

    bump.assert_not_awaited()
    assert pa._events_epoch == 4
    assert pa._next_seq == 2
    pa._broadcast_frame("turn.started", {}, durable_receipt=False)
    assert pa._next_seq == 3
    queued = new_writer.enqueue.call_args.args[0]
    assert (queued.epoch, queued.seq) == (4, 3)


class _RecordingConn:
    def __init__(self):
        self.calls: list[tuple[str, tuple]] = []

    def transaction(self):
        return _Transaction()

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        return 1


@pytest.mark.asyncio
async def test_writer_uses_interrupt_events_immutable_lease_token():
    conn = _RecordingConn()
    warm_handle = LeaseHandle()
    warm_handle.update(str(THREAD_ID), 10)
    writer = pa._OrderedPersistentEventWriter(
        postgres_conn=_pool(conn),
        thread_id=str(THREAD_ID),
        epoch=2,
        on_terminal_failure=lambda _events, _reason: None,
        lease=warm_handle,
    )
    event = pa._QueuedPersistentEvent(
        epoch=2,
        seq=8,
        kind="interrupt.ack",
        payload={},
        interrupt_request_id=str(REQUEST_ID),
        interrupt_lease_token=9,
    )

    assert await writer._write_batch([event]) == 1
    thread_fence_sql, _thread_fence_args = conn.calls[0]
    queue_fence_sql, queue_fence_args = conn.calls[1]
    request_lock_sql, request_lock_args = conn.calls[2]
    assert "FOR NO KEY UPDATE" in thread_fence_sql
    assert "state = 'leased'" in queue_fence_sql
    assert queue_fence_args == (str(THREAD_ID), 9)
    assert "accepted_lease_token = $3::bigint" in request_lock_sql
    assert request_lock_args == ([str(REQUEST_ID)], str(THREAD_ID), 9)
    assert queue_fence_args[1] != warm_handle.lease_token


@pytest.mark.asyncio
async def test_stale_recovery_writer_fences_current_but_locks_old_request():
    conn = _RecordingConn()
    handle = LeaseHandle()
    handle.update(str(THREAD_ID), 10)
    writer = pa._OrderedPersistentEventWriter(
        postgres_conn=_pool(conn),
        thread_id=str(THREAD_ID),
        epoch=4,
        on_terminal_failure=lambda _events, _reason: None,
        lease=handle,
    )
    event = pa._QueuedPersistentEvent(
        epoch=4,
        seq=1,
        kind="interrupt.ack",
        payload={"applied": False},
        interrupt_request_id=str(REQUEST_ID),
        interrupt_lease_token=10,
        interrupt_accepted_lease_token=9,
        interrupt_stale_recovery=True,
    )

    assert await writer._write_batch([event]) == 1
    _queue_sql, queue_args = conn.calls[1]
    _request_sql, request_args = conn.calls[2]
    assert queue_args == (str(THREAD_ID), 10)
    assert request_args == ([str(REQUEST_ID)], str(THREAD_ID), 9)


@pytest.mark.asyncio
async def test_writer_isolates_interrupt_receipt_from_ordinary_frames():
    writer = pa._OrderedPersistentEventWriter(
        postgres_conn=_pool(),
        thread_id=str(THREAD_ID),
        epoch=2,
        on_terminal_failure=lambda _events, _reason: None,
    )
    write = AsyncMock(return_value=None)
    writer._write_with_retry = write
    writer.start()
    assert writer.enqueue(pa._QueuedPersistentEvent(2, 1, "assistant.delta", {}))
    assert writer.enqueue(
        pa._QueuedPersistentEvent(
            2,
            2,
            "interrupt.ack",
            {},
            interrupt_request_id=str(REQUEST_ID),
            interrupt_lease_token=9,
        )
    )
    assert writer.enqueue(pa._QueuedPersistentEvent(2, 3, "assistant.delta", {}))
    await writer._queue.join()
    await writer.close()

    batches = [item.args[0] for item in write.await_args_list]
    assert [[event.seq for event in batch] for batch in batches] == [[1], [2], [3]]


def test_receipt_validation_requires_full_correlation():
    receipt = _receipt()
    assert interrupt_receipt_result(
        request_id=REQUEST_ID,
        client_request_id=CLIENT_REQUEST_ID,
        target_turn_id=4,
        event_kind=receipt.kind,
        event_payload=receipt.payload,
    ) == ("applied", "hard", None)
    corrupt = dict(receipt.payload)
    corrupt["target_turn_id"] = 5
    assert (
        interrupt_receipt_result(
            request_id=REQUEST_ID,
            client_request_id=CLIENT_REQUEST_ID,
            target_turn_id=4,
            event_kind=receipt.kind,
            event_payload=corrupt,
        )
        is None
    )


class _FinalizeConn:
    def __init__(self, *, applied=True):
        self.calls: list[tuple[str, tuple]] = []
        self.rows = [
            {"owned": 1},
            {"owned": 1},
            {
                "id": REQUEST_ID,
                "thread_id": THREAD_ID,
                "client_request_id": CLIENT_REQUEST_ID,
                "target_turn_id": 4,
                "accepted_lease_token": 9,
                "outcome": None,
            },
            {
                "epoch": 3,
                "seq": 12,
                "kind": "interrupt.ack",
                "payload": _receipt(applied=applied).payload,
            },
            {"id": REQUEST_ID},
        ]

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        return self.rows.pop(0)


@pytest.mark.asyncio
async def test_finalizer_rechecks_owner_then_request_and_receipt():
    conn = _FinalizeConn()

    consumption = InterruptInputConsumption(5, "leased", False, False, True)
    with patch(
        "shared.thread_interrupts.queries.consume_applied_interrupt_input_live",
        AsyncMock(return_value=consumption),
    ) as consume:
        result = await finalize_interrupt_request(
            conn,
            request_id=REQUEST_ID,
            thread_id=THREAD_ID,
            lease_token=9,
            target_turn_id=4,
            outcome="applied",
            mode="hard",
            error_code=None,
        )

    assert result == "applied"
    assert "FROM threads" in conn.calls[0][0]
    assert "FROM run_queue" in conn.calls[1][0]
    assert "FOR UPDATE" in conn.calls[1][0]
    assert "thread_interrupt_requests" in conn.calls[2][0]
    assert "interrupt_request_id" in conn.calls[3][0]
    assert "UPDATE thread_interrupt_requests" in conn.calls[4][0]
    assert conn.calls[4][1][4] == 9
    assert conn.calls[4][1][5:7] == (3, 12)
    consume.assert_awaited_once_with(
        conn,
        thread_id=THREAD_ID,
        current_lease_token=9,
        accepted_lease_token=9,
        target_turn_id=4,
        request_id=REQUEST_ID,
    )


@pytest.mark.asyncio
async def test_rejected_finalizer_never_consumes_input():
    conn = _FinalizeConn(applied=False)
    consume = AsyncMock()
    with patch(
        "shared.thread_interrupts.queries.consume_applied_interrupt_input_live",
        consume,
    ):
        result = await finalize_interrupt_request(
            conn,
            request_id=REQUEST_ID,
            thread_id=THREAD_ID,
            lease_token=9,
            target_turn_id=4,
            outcome="rejected",
            mode=None,
            error_code="target_turn_not_active",
        )

    assert result == "rejected"
    consume.assert_not_awaited()


class _ConsumptionConn:
    def __init__(self, *, consumed_seq=1, input_seq=9, humans=None, applied=None):
        self.queue = {
            "input_seq": input_seq,
            "consumed_seq": consumed_seq,
            "control_input_seq": 4,
            "control_consumed_seq": 3,
            "state": "leased",
            "attempts_since_completion": 5,
        }
        self.humans = list(humans or [])
        self.applied = list(
            applied
            or [
                {
                    "id": REQUEST_ID,
                    "result": {
                        "request_id": str(REQUEST_ID),
                        "applied": True,
                    },
                }
            ]
        )
        self.calls: list[tuple[str, tuple]] = []
        self.marker = None

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        if "SELECT 1 FROM threads" in sql:
            return {"one": 1}
        if "FROM run_queue" in sql and "SELECT input_seq" in sql:
            return dict(self.queue)
        if "UPDATE run_queue" in sql:
            target = int(args[2])
            old = self.queue["consumed_seq"]
            self.queue["consumed_seq"] = target if old is None else max(old, target)
            if "attempts_since_completion = 0" in sql:
                self.queue["attempts_since_completion"] = 0
            return dict(self.queue)
        raise AssertionError(sql)

    async def fetch(self, sql, *args):
        self.calls.append((sql, args))
        if "FROM thread_interrupt_requests" in sql:
            return list(self.applied)
        if "FROM thread_messages" in sql:
            return [{"seq": seq} for seq in self.humans]
        raise AssertionError(sql)

    async def execute(self, sql, *args):
        self.calls.append((sql, args))
        self.marker = args[3]
        return "UPDATE 1"


@pytest.mark.asyncio
async def test_applied_group_resets_max_attempts_and_preserves_newer_work():
    conn = _ConsumptionConn(humans=[5])

    result = await consume_applied_interrupt_input_live(
        conn,
        thread_id=THREAD_ID,
        current_lease_token=10,
        accepted_lease_token=9,
        target_turn_id=4,
        request_id=REQUEST_ID,
    )

    assert result == InterruptInputConsumption(5, "leased", True, True, True)
    human_sql = next(sql for sql, _ in conn.calls if "thread_messages" in sql)
    assert "turn_number = $2::integer" in human_sql
    assert "seq >" not in human_sql
    assert '"consumed_input_seq": 5' in conn.marker
    assert conn.queue["input_seq"] == 9
    assert conn.queue["control_input_seq"] == 4
    assert conn.queue["control_consumed_seq"] == 3
    # Applied acknowledgement is a semantic completion. A crash immediately
    # after this transaction must not let the reaper park the consumed unit at
    # the old max-attempt counter before a successor can take skip-if-answered.
    assert conn.queue["attempts_since_completion"] == 0
    live_update_sql = next(sql for sql, _ in conn.calls if "UPDATE run_queue" in sql)
    assert "attempts_since_completion = 0" in live_update_sql
    assert "SET state" not in live_update_sql


@pytest.mark.asyncio
async def test_applied_sibling_marker_prevents_second_input_consumption():
    sibling = UUID("44444444-4444-4444-8444-444444444444")
    conn = _ConsumptionConn(
        consumed_seq=5,
        humans=[5],
        applied=[
            {"id": REQUEST_ID, "result": {"applied": True}},
            {
                "id": sibling,
                "result": {"applied": True, "consumed_input_seq": 5},
            },
        ],
    )

    result = await consume_applied_interrupt_input_live(
        conn,
        thread_id=THREAD_ID,
        current_lease_token=10,
        accepted_lease_token=9,
        target_turn_id=4,
        request_id=REQUEST_ID,
    )

    assert result is not None and result.consumed_seq == 5
    assert result.advanced is False
    assert not any("thread_messages" in sql for sql, _ in conn.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("humans", [[], [5, 6]])
async def test_applied_target_mapping_fails_closed_when_not_exact(humans):
    conn = _ConsumptionConn(humans=humans)
    with pytest.raises(RuntimeError, match="exactly one live human"):
        await consume_applied_interrupt_input_live(
            conn,
            thread_id=THREAD_ID,
            current_lease_token=10,
            accepted_lease_token=9,
            target_turn_id=4,
            request_id=REQUEST_ID,
        )


@pytest.mark.asyncio
async def test_old_token_already_consumed_target_is_stamped_without_advancing():
    conn = _ConsumptionConn(consumed_seq=9, input_seq=12, humans=[5])

    result = await consume_applied_interrupt_input_live(
        conn,
        thread_id=THREAD_ID,
        current_lease_token=10,
        accepted_lease_token=9,
        target_turn_id=4,
        request_id=REQUEST_ID,
    )

    assert result is not None
    assert result.consumed_seq == 9
    assert result.advanced is False
    assert '"consumed_input_seq": 5' in conn.marker
