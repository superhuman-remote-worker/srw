"""Focused owner-side tests for the durable session-control inbox.

Admission is covered separately.  These tests pin the consumer's critical
ordering: describe without side effects, durably journal under the immutable
owner credential, finalize the request/scalar, and only then converge RAM.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch
from uuid import UUID

import pytest

import agent.api.persistent_app as pa
from agent.api.lease_context import LeaseHandle, current_lease
from shared.thread_controls import ControlReceipt, ControlRequest


THREAD_ID = UUID("11111111-1111-4111-8111-111111111111")
REQUEST_1 = UUID("22222222-2222-4222-8222-222222222222")
REQUEST_2 = UUID("33333333-3333-4333-8333-333333333333")
PINNED_AGENT_ID = UUID("44444444-4444-4444-8444-444444444444")
RUNTIME_GENERATION = UUID("55555555-5555-4555-8555-555555555555")
RUNTIME_ATTACH_TOKEN = UUID("66666666-6666-4666-8666-666666666666")


@pytest.fixture(autouse=True)
def isolated_legacy_status_contract(monkeypatch):
    # Every test starts before advertisement; modern cases explicitly install
    # their captured life. A prior module's bound G must not turn these legacy
    # arrangements into an incomplete/stale runtime contract.
    monkeypatch.setattr(pa._session_identity, "_status_contract", False)
    monkeypatch.setattr(pa._session_identity, "_runtime_contract", False)
    monkeypatch.setattr(pa._session_identity, "_session_generation", None)
    monkeypatch.setattr(pa._session_identity, "_attach_token", None)


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


def _pool():
    conn = MagicMock()
    return SimpleNamespace(acquire=lambda: _Acquire(conn))


def _request(
    request_id: UUID,
    seq: int,
    verb: str,
    mode: str,
    *,
    accepted_agent_id: UUID | None = None,
) -> ControlRequest:
    return ControlRequest(
        id=request_id,
        thread_id=THREAD_ID,
        request_seq=seq,
        client_request_id=UUID(int=seq),
        verb=verb,
        payload={"mode": mode},
        accepted_agent_id=accepted_agent_id,
        runtime_generation=RUNTIME_GENERATION,
    )


def _receipt(
    request: ControlRequest,
    *,
    method: str | None = None,
    mode: str | None = None,
) -> ControlReceipt:
    return ControlReceipt(
        epoch=6,
        seq=44,
        kind=("mode.changed" if request.verb == "mode.set" else "narration.changed"),
        payload={
            "request_id": str(request.id),
            "client_request_id": str(request.client_request_id),
            "request_seq": request.request_seq,
            "method": method or request.verb,
            "mode": mode or str(request.payload["mode"]),
        },
    )


def _undo_request(request_id: UUID = REQUEST_1, seq: int = 1) -> ControlRequest:
    return ControlRequest(
        id=request_id,
        thread_id=THREAD_ID,
        request_seq=seq,
        client_request_id=UUID(int=seq),
        verb="workspace.undo",
        payload={},
        accepted_agent_id=None,
        runtime_generation=RUNTIME_GENERATION,
    )


def _undo_receipt(request: ControlRequest) -> ControlReceipt:
    return ControlReceipt(
        epoch=6,
        seq=44,
        kind="files.restored",
        payload={
            "request_id": str(request.id),
            "client_request_id": str(request.client_request_id),
            "request_seq": request.request_seq,
            "method": "workspace.undo",
            "paths": ["kept.txt", "new.txt"],
            "restored_to_sha": "1" * 40,
            "restore_commit_sha": "2" * 40,
        },
    )


@pytest.fixture
def stateless_owner(monkeypatch):
    session = SimpleNamespace(
        postgres_conn=_pool(),
        permission_mode="supervised",
        narration_mode="auto",
        undo_turn=AsyncMock(),
    )
    monkeypatch.setattr(pa, "_session", session)
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    handle = LeaseHandle()
    handle.update(str(THREAD_ID), 9)
    token = current_lease.set(handle)
    try:
        yield session
    finally:
        current_lease.reset(token)


@pytest.mark.asyncio
async def test_drain_journals_and_finalizes_before_synchronous_ram_apply(
    stateless_owner,
):
    first = _request(REQUEST_1, 1, "mode.set", "autonomous")
    second = _request(REQUEST_2, 2, "narration.set", "verbose")
    fetch_next = AsyncMock(side_effect=[first, second, None])
    fetch_receipt = AsyncMock(return_value=None)
    owner_fence = AsyncMock(return_value=True)
    adopt = AsyncMock(return_value=False)
    order: list[str] = []

    def describe(request):
        order.append(f"describe:{request.request_seq}")
        return (
            ("mode.changed", "applied", None)
            if request.verb == "mode.set"
            else ("narration.changed", "applied", None)
        )

    async def journal(_kind, _params, **_kwargs):
        seq = len([item for item in order if item.startswith("journal:")]) + 1
        order.append(f"journal:{seq}")
        return 4, 9 + seq

    async def finalize(request, **_kwargs):
        order.append(f"finalize:{request.request_seq}")
        return "applied"

    def apply(request):
        order.append(f"apply:{request.request_seq}")
        return describe_result[request.request_seq]

    describe_result = {
        1: ("mode.changed", "applied", None),
        2: ("narration.changed", "applied", None),
    }
    describe_mock = MagicMock(side_effect=describe)
    journal_mock = AsyncMock(side_effect=journal)
    finalize_mock = AsyncMock(side_effect=finalize)
    apply_mock = MagicMock(side_effect=apply)

    with (
        patch("shared.thread_controls.owner_fence_current", owner_fence),
        patch("shared.thread_controls.adopt_next_pinned_control_request", adopt),
        patch("shared.thread_controls.fetch_next_control_request", fetch_next),
        patch("shared.thread_controls.fetch_control_receipt", fetch_receipt),
        patch.object(pa, "_describe_control_request", describe_mock),
        patch.object(pa, "_broadcast_durable", journal_mock),
        patch.object(pa, "_finalize_durable_control", finalize_mock),
        patch.object(pa, "_apply_control_request", apply_mock),
    ):
        assert await pa._drain_thread_controls(lease_token=9) == 2

    assert order == [
        "describe:1",
        "journal:1",
        "finalize:1",
        "apply:1",
        "describe:2",
        "journal:2",
        "finalize:2",
        "apply:2",
    ]
    assert [
        (
            call.kwargs["control_request_id"],
            call.kwargs["lease_token"],
            call.kwargs["agent_id"],
        )
        for call in journal_mock.await_args_list
    ] == [(str(REQUEST_1), 9, None), (str(REQUEST_2), 9, None)]
    assert [call.args[0].request_seq for call in apply_mock.call_args_list] == [1, 2]
    adopt.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_undo_effect_precedes_owner_fenced_journal_and_finalize(
    stateless_owner,
):
    request = _undo_request()
    fetch_next = AsyncMock(side_effect=[request, None])
    order: list[str] = []

    async def undo(**_kwargs):
        order.append("undo")
        return {
            "paths": ["kept.txt", "new.txt"],
            "restored_to_sha": "1" * 40,
            "restore_commit_sha": "2" * 40,
        }

    async def journal(*_args, **_kwargs):
        order.append("journal")
        return 6, 44

    async def finalize(*_args, **_kwargs):
        order.append("finalize")
        return "applied"

    stateless_owner.undo_turn.side_effect = undo
    journal_mock = AsyncMock(side_effect=journal)
    finalize_mock = AsyncMock(side_effect=finalize)
    with (
        patch(
            "shared.thread_controls.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_controls.adopt_next_pinned_control_request",
            AsyncMock(return_value=False),
        ),
        patch("shared.thread_controls.fetch_next_control_request", fetch_next),
        patch(
            "shared.thread_controls.fetch_control_receipt",
            AsyncMock(return_value=None),
        ),
        patch.object(pa, "_broadcast_durable", journal_mock),
        patch.object(pa, "_finalize_durable_control", finalize_mock),
    ):
        assert await pa._drain_thread_controls(lease_token=9) == 1

    assert order == ["undo", "journal", "finalize"]
    stateless_owner.undo_turn.assert_awaited_once_with(
        control_request_id=str(REQUEST_1)
    )
    journal_mock.assert_awaited_once_with(
        "files.restored",
        {
            "request_id": str(REQUEST_1),
            "client_request_id": str(request.client_request_id),
            "request_seq": 1,
            "method": "workspace.undo",
            "paths": ["kept.txt", "new.txt"],
            "restored_to_sha": "1" * 40,
            "restore_commit_sha": "2" * 40,
        },
        control_request_id=str(REQUEST_1),
        lease_token=9,
        agent_id=None,
    )
    finalize_mock.assert_awaited_once_with(
        request,
        lease_token=9,
        agent_id=None,
        outcome="applied",
        error_code=None,
    )


@pytest.mark.asyncio
async def test_workspace_undo_receipt_recovery_never_repeats_git_effect(
    stateless_owner,
):
    request = _undo_request()
    receipt = _undo_receipt(request)
    with (
        patch(
            "shared.thread_controls.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_controls.adopt_next_pinned_control_request",
            AsyncMock(return_value=False),
        ),
        patch(
            "shared.thread_controls.fetch_next_control_request",
            AsyncMock(side_effect=[request, None]),
        ),
        patch(
            "shared.thread_controls.fetch_control_receipt",
            AsyncMock(return_value=receipt),
        ),
        patch.object(pa, "_broadcast_durable", AsyncMock()) as journal,
        patch.object(
            pa, "_finalize_durable_control", AsyncMock(return_value="applied")
        ),
    ):
        assert await pa._drain_thread_controls(lease_token=9) == 1

    stateless_owner.undo_turn.assert_not_awaited()
    journal.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_undo_retryable_effect_is_not_journaled(stateless_owner):
    request = _undo_request()
    stateless_owner.undo_turn.side_effect = pa.WorkspaceUndoRetryable(
        "push is ambiguous"
    )
    journal = AsyncMock()
    with (
        patch(
            "shared.thread_controls.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_controls.adopt_next_pinned_control_request",
            AsyncMock(return_value=False),
        ),
        patch(
            "shared.thread_controls.fetch_next_control_request",
            AsyncMock(return_value=request),
        ),
        patch(
            "shared.thread_controls.fetch_control_receipt",
            AsyncMock(return_value=None),
        ),
        patch.object(pa, "_broadcast_durable", journal),
    ):
        with pytest.raises(pa.ControlInboxBlocked, match="remains retryable"):
            await pa._drain_thread_controls(lease_token=9)

    journal.assert_not_awaited()


@pytest.mark.asyncio
async def test_journal_failure_blocks_without_ram_or_permission_side_effects(
    stateless_owner,
):
    request = _request(REQUEST_1, 1, "mode.set", "autonomous")
    apply_request = MagicMock()
    retire_permissions = AsyncMock()
    finalize = AsyncMock()

    with (
        patch(
            "shared.thread_controls.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_controls.adopt_next_pinned_control_request",
            AsyncMock(return_value=False),
        ),
        patch(
            "shared.thread_controls.fetch_next_control_request",
            AsyncMock(return_value=request),
        ),
        patch(
            "shared.thread_controls.fetch_control_receipt",
            AsyncMock(return_value=None),
        ),
        patch.object(
            pa,
            "_broadcast_durable",
            AsyncMock(side_effect=pa.EventJournalUnavailable("write failed")),
        ),
        patch.object(pa, "_finalize_durable_control", finalize),
        patch.object(pa, "_apply_control_request", apply_request),
        patch.object(pa, "_retire_announced_permission_rows", retire_permissions),
    ):
        with pytest.raises(pa.ControlInboxBlocked):
            await pa._drain_thread_controls(lease_token=9)

    assert stateless_owner.permission_mode == "supervised"
    apply_request.assert_not_called()
    retire_permissions.assert_not_awaited()
    finalize.assert_not_awaited()


@pytest.mark.asyncio
async def test_finalize_failure_blocks_without_ram_or_permission_side_effects(
    stateless_owner,
):
    request = _request(REQUEST_1, 1, "mode.set", "autonomous")
    apply_request = MagicMock()
    retire_permissions = AsyncMock()

    with (
        patch(
            "shared.thread_controls.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_controls.adopt_next_pinned_control_request",
            AsyncMock(return_value=False),
        ),
        patch(
            "shared.thread_controls.fetch_next_control_request",
            AsyncMock(return_value=request),
        ),
        patch(
            "shared.thread_controls.fetch_control_receipt",
            AsyncMock(return_value=None),
        ),
        patch.object(pa, "_broadcast_durable", AsyncMock(return_value=(4, 10))),
        patch.object(
            pa,
            "_finalize_durable_control",
            AsyncMock(return_value="watermark_gap"),
        ),
        patch.object(pa, "_apply_control_request", apply_request),
        patch.object(pa, "_retire_announced_permission_rows", retire_permissions),
    ):
        with pytest.raises(pa.ControlInboxBlocked, match="watermark_gap"):
            await pa._drain_thread_controls(lease_token=9)

    assert stateless_owner.permission_mode == "supervised"
    apply_request.assert_not_called()
    retire_permissions.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("receipt_method", "receipt_mode"),
    [
        ("mode.set", "supervised"),
        ("narration.set", "autonomous"),
    ],
)
async def test_full_receipt_mismatch_blocks_before_finalize_or_apply(
    stateless_owner,
    receipt_method,
    receipt_mode,
):
    request = _request(REQUEST_1, 1, "mode.set", "autonomous")
    receipt = _receipt(request, method=receipt_method, mode=receipt_mode)
    finalize = AsyncMock()
    journal = AsyncMock()
    apply_request = MagicMock()

    with (
        patch(
            "shared.thread_controls.owner_fence_current",
            AsyncMock(return_value=True),
        ),
        patch(
            "shared.thread_controls.adopt_next_pinned_control_request",
            AsyncMock(return_value=False),
        ),
        patch(
            "shared.thread_controls.fetch_next_control_request",
            AsyncMock(return_value=request),
        ),
        patch(
            "shared.thread_controls.fetch_control_receipt",
            AsyncMock(return_value=receipt),
        ),
        patch.object(pa, "_broadcast_durable", journal),
        patch.object(pa, "_finalize_durable_control", finalize),
        patch.object(pa, "_apply_control_request", apply_request),
    ):
        with pytest.raises(pa.ControlInboxBlocked, match="does not match"):
            await pa._drain_thread_controls(lease_token=9)

    assert stateless_owner.permission_mode == "supervised"
    journal.assert_not_awaited()
    finalize.assert_not_awaited()
    apply_request.assert_not_called()


@pytest.mark.asyncio
async def test_receipt_recovery_finalizes_then_converges_ram_without_rejournal(
    stateless_owner,
):
    request = _request(REQUEST_1, 1, "mode.set", "autonomous")
    receipt = _receipt(request)
    fetch_next = AsyncMock(side_effect=[request, None])
    owner_fence = AsyncMock(return_value=True)
    journal = AsyncMock()
    order: list[str] = []
    real_apply = pa._apply_control_request

    async def finalize(_request, **_kwargs):
        order.append("finalize")
        return "applied"

    def converge(request_to_apply):
        order.append("apply")
        return real_apply(request_to_apply)

    finalize_mock = AsyncMock(side_effect=finalize)
    apply_mock = MagicMock(side_effect=converge)
    with (
        patch("shared.thread_controls.owner_fence_current", owner_fence),
        patch(
            "shared.thread_controls.adopt_next_pinned_control_request",
            AsyncMock(return_value=False),
        ),
        patch("shared.thread_controls.fetch_next_control_request", fetch_next),
        patch(
            "shared.thread_controls.fetch_control_receipt",
            AsyncMock(return_value=receipt),
        ),
        patch.object(pa, "_broadcast_durable", journal),
        patch.object(pa, "_finalize_durable_control", finalize_mock),
        patch.object(pa, "_apply_control_request", apply_mock),
    ):
        assert await pa._drain_thread_controls(lease_token=9) == 1

    assert order == ["finalize", "apply"]
    assert stateless_owner.permission_mode == "autonomous"
    journal.assert_not_awaited()
    finalize_mock.assert_awaited_once_with(
        request,
        lease_token=9,
        agent_id=None,
        outcome="applied",
        error_code=None,
    )
    apply_mock.assert_called_once_with(request)


def test_ram_convergence_is_scalar_only(stateless_owner):
    request = _request(REQUEST_1, 1, "mode.set", "auto_accept")
    retire_permissions = AsyncMock()
    with patch.object(pa, "_retire_announced_permission_rows", retire_permissions):
        assert pa._apply_control_request(request) == (
            "mode.changed",
            "applied",
            None,
        )

    assert stateless_owner.permission_mode == "auto_accept"
    retire_permissions.assert_not_awaited()


@pytest.mark.asyncio
async def test_pinned_drain_adopts_then_passes_immutable_agent_owner(
    stateless_owner,
):
    request = _request(
        REQUEST_1,
        1,
        "narration.set",
        "silent",
        accepted_agent_id=PINNED_AGENT_ID,
    )
    fetch_next = AsyncMock(side_effect=[request, None])
    owner_fence = AsyncMock(return_value=True)
    adopt = AsyncMock(return_value=True)
    journal = AsyncMock(return_value=(3, 8))
    finalize = AsyncMock(return_value="applied")
    apply_request = MagicMock(return_value=("narration.changed", "applied", None))

    with (
        patch.object(
            pa._session_identity, "_session_generation", str(RUNTIME_GENERATION)
        ),
        patch.object(pa._session_identity, "_attach_token", str(RUNTIME_ATTACH_TOKEN)),
        patch("shared.thread_controls.owner_fence_current", owner_fence),
        patch("shared.thread_controls.adopt_next_pinned_control_request", adopt),
        patch("shared.thread_controls.fetch_next_control_request", fetch_next),
        patch(
            "shared.thread_controls.fetch_control_receipt",
            AsyncMock(return_value=None),
        ),
        patch.object(pa, "_broadcast_durable", journal),
        patch.object(pa, "_finalize_durable_control", finalize),
        patch.object(pa, "_apply_control_request", apply_request),
    ):
        assert await pa._drain_thread_controls(agent_id=str(PINNED_AGENT_ID)) == 1

    assert adopt.await_count == 2
    assert adopt.await_args_list[0].kwargs == {
        "thread_id": str(THREAD_ID),
        "agent_id": str(PINNED_AGENT_ID),
        "runtime_generation": str(RUNTIME_GENERATION),
        "runtime_attach_token": str(RUNTIME_ATTACH_TOKEN),
    }
    journal.assert_awaited_once_with(
        "narration.changed",
        {
            "request_id": str(REQUEST_1),
            "client_request_id": str(request.client_request_id),
            "request_seq": 1,
            "method": "narration.set",
            "mode": "silent",
        },
        control_request_id=str(REQUEST_1),
        lease_token=None,
        agent_id=str(PINNED_AGENT_ID),
    )
    finalize.assert_awaited_once_with(
        request,
        lease_token=None,
        agent_id=str(PINNED_AGENT_ID),
        outcome="applied",
        error_code=None,
    )
    apply_request.assert_called_once_with(request)


class _RecordingConn:
    def __init__(self):
        self.calls: list[tuple[str, tuple]] = []

    def transaction(self):
        return _Transaction()

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        return 1


@pytest.mark.asyncio
async def test_writer_fences_stateless_control_on_event_immutable_lease_token():
    conn = _RecordingConn()
    warm_handle = LeaseHandle()
    warm_handle.update(str(THREAD_ID), 10)
    writer = pa._OrderedPersistentEventWriter(
        postgres_conn=SimpleNamespace(acquire=lambda: _Acquire(conn)),
        thread_id=str(THREAD_ID),
        epoch=2,
        on_terminal_failure=lambda _events, _reason: None,
        lease=warm_handle,
    )
    event = pa._QueuedPersistentEvent(
        epoch=2,
        seq=8,
        kind="mode.changed",
        payload={},
        control_request_id=str(REQUEST_1),
        control_lease_token=9,
    )

    assert await writer._write_batch([event]) == 1
    thread_fence_sql, _thread_fence_args = conn.calls[0]
    queue_fence_sql, queue_fence_args = conn.calls[1]
    assert "FOR NO KEY UPDATE" in thread_fence_sql
    assert "state = 'leased'" in queue_fence_sql
    assert queue_fence_args == (str(THREAD_ID), 9)
    assert queue_fence_args[1] != warm_handle.lease_token


@pytest.mark.asyncio
async def test_writer_fences_pinned_control_on_event_immutable_agent_id():
    conn = _RecordingConn()
    agent_id = str(PINNED_AGENT_ID)
    writer = pa._OrderedPersistentEventWriter(
        postgres_conn=SimpleNamespace(acquire=lambda: _Acquire(conn)),
        thread_id=str(THREAD_ID),
        epoch=2,
        on_terminal_failure=lambda _events, _reason: None,
        pinned_agent_id=agent_id,
        pinned_runtime_generation=str(RUNTIME_GENERATION),
        pinned_runtime_attach_token=str(RUNTIME_ATTACH_TOKEN),
    )
    event = pa._QueuedPersistentEvent(
        epoch=2,
        seq=8,
        kind="mode.changed",
        payload={},
        control_request_id=str(REQUEST_1),
        control_agent_id=agent_id,
    )

    assert await writer._write_batch([event]) == 1
    thread_fence_sql, thread_fence_args = conn.calls[0]
    agent_fence_sql, agent_fence_args = conn.calls[1]
    request_lock_sql, request_lock_args = conn.calls[2]
    assert "execution_lane = 'pinned'" in thread_fence_sql
    assert "FOR NO KEY UPDATE" in thread_fence_sql
    assert thread_fence_args == (
        str(THREAD_ID),
        agent_id,
        str(RUNTIME_GENERATION),
        str(RUNTIME_ATTACH_TOKEN),
    )
    assert "thread_id = $2::uuid" in agent_fence_sql
    assert agent_fence_args == (agent_id, str(THREAD_ID))
    assert "accepted_agent_id IS NOT DISTINCT FROM $3::uuid" in request_lock_sql
    assert request_lock_args[2] == agent_id
    assert request_lock_args[3] == str(RUNTIME_GENERATION)


@pytest.mark.asyncio
async def test_writer_never_mixes_control_and_ordinary_frames_in_one_batch():
    writer = pa._OrderedPersistentEventWriter(
        postgres_conn=_pool(),
        thread_id=str(THREAD_ID),
        epoch=2,
        on_terminal_failure=lambda _events, _reason: None,
    )
    write = AsyncMock(return_value=None)
    writer._write_with_retry = write
    writer.start()
    ordinary_before = pa._QueuedPersistentEvent(2, 1, "assistant.delta", {})
    control = pa._QueuedPersistentEvent(
        2,
        2,
        "mode.changed",
        {},
        control_request_id=str(REQUEST_1),
        control_agent_id=str(PINNED_AGENT_ID),
    )
    ordinary_after = pa._QueuedPersistentEvent(2, 3, "assistant.delta", {})
    assert writer.enqueue(ordinary_before)
    assert writer.enqueue(control)
    assert writer.enqueue(ordinary_after)
    await writer._queue.join()
    await writer.close()

    batches = [call.args[0] for call in write.await_args_list]
    assert [[event.seq for event in batch] for batch in batches] == [[1], [2], [3]]
    assert all(
        len(batch) == 1
        for batch in batches
        if any(event.control_request_id is not None for event in batch)
    )


@pytest.mark.asyncio
async def test_control_fence_loss_does_not_drop_already_queued_ordinary_frame():
    writer = pa._OrderedPersistentEventWriter(
        postgres_conn=_pool(),
        thread_id=str(THREAD_ID),
        epoch=2,
        on_terminal_failure=lambda _events, _reason: None,
    )
    calls: list[list[int]] = []

    async def write(batch):
        calls.append([event.seq for event in batch])
        if batch[0].control_request_id is not None:
            writer._closing = True
            return "epoch_fenced"
        return None

    writer._write_with_retry = AsyncMock(side_effect=write)
    writer.start()
    assert writer.enqueue(
        pa._QueuedPersistentEvent(
            2,
            1,
            "mode.changed",
            {},
            control_request_id=str(REQUEST_1),
            control_agent_id=str(PINNED_AGENT_ID),
        )
    )
    assert writer.enqueue(pa._QueuedPersistentEvent(2, 2, "assistant.delta", {}))
    await writer._queue.join()
    await writer.close()

    assert calls == [[1], [2]]


@pytest.mark.asyncio
async def test_pinned_watcher_closes_gate_before_stop_and_after_first_drain_failure(
    monkeypatch,
):
    order: list[str] = []
    monkeypatch.setattr(pa, "_session", SimpleNamespace(postgres_conn=object()))
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa, "_control_owner_lease_token", None)
    monkeypatch.setattr(pa, "_control_owner_agent_id", "prior-owner")

    async def set_gate(*, agent_id, open_for_admission):
        order.append(f"gate:{agent_id}:{open_for_admission}")
        return True

    async def stop():
        order.append("stop")

    async def drain():
        order.append("drain")
        raise RuntimeError("receipt unavailable")

    with (
        patch.object(pa, "_set_pinned_control_admission", side_effect=set_gate),
        patch.object(pa, "_stop_thread_control_watcher", side_effect=stop),
        patch.object(pa, "_drain_current_thread_controls", side_effect=drain),
    ):
        with pytest.raises(RuntimeError, match="receipt unavailable"):
            await pa._start_thread_control_watcher(agent_id=str(PINNED_AGENT_ID))

    assert order == [
        f"gate:{PINNED_AGENT_ID}:False",
        "stop",
        "drain",
        f"gate:{PINNED_AGENT_ID}:False",
    ]
    assert pa._control_owner_agent_id is None
    assert pa._control_owner_lease_token is None


@pytest.mark.asyncio
async def test_pinned_watcher_cancellation_after_open_recloses_capability(monkeypatch):
    order: list[str] = []
    monkeypatch.setattr(pa, "_session", SimpleNamespace(postgres_conn=object()))
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa, "_control_owner_lease_token", None)
    monkeypatch.setattr(pa, "_control_owner_agent_id", None)

    async def set_gate(*, agent_id, open_for_admission):
        order.append(f"gate:{open_for_admission}")
        return True

    drain_count = 0

    async def drain():
        nonlocal drain_count
        drain_count += 1
        order.append(f"drain:{drain_count}")
        if drain_count == 2:
            raise asyncio.CancelledError
        return 1

    with (
        patch.object(pa, "_set_pinned_control_admission", side_effect=set_gate),
        patch.object(pa, "_stop_thread_control_watcher", new=AsyncMock()),
        patch.object(pa, "_drain_current_thread_controls", side_effect=drain),
    ):
        with pytest.raises(asyncio.CancelledError):
            await pa._start_thread_control_watcher(agent_id=str(PINNED_AGENT_ID))

    assert order == [
        "gate:False",
        "drain:1",
        "gate:True",
        "drain:2",
        "gate:False",
    ]
    assert pa._control_owner_agent_id is None
    assert pa._control_owner_lease_token is None


@pytest.mark.asyncio
async def test_control_watcher_stop_wakes_listener_without_task_cancellation(
    monkeypatch,
):
    listener = SimpleNamespace(
        add_listener=AsyncMock(),
        remove_listener=AsyncMock(),
    )
    pool = SimpleNamespace(acquire=lambda: _Acquire(listener))
    stop = asyncio.Event()
    drain = AsyncMock(return_value=0)
    monkeypatch.setattr(pa, "_drain_thread_controls", drain)

    task = asyncio.create_task(
        pa._control_watcher_loop(
            postgres_conn=pool,
            thread_id=str(THREAD_ID),
            stop=stop,
            lease_token=9,
            agent_id=None,
        )
    )
    for _ in range(10):
        if listener.add_listener.await_count:
            break
        await asyncio.sleep(0)
    assert listener.add_listener.await_count == 1

    stop.set()
    await asyncio.wait_for(task, timeout=0.5)

    assert not task.cancelled()
    listener.remove_listener.assert_awaited_once()
    drain.assert_awaited_once_with(lease_token=9, agent_id=None)


@pytest.mark.asyncio
async def test_stop_control_watcher_prefers_cooperative_shutdown(monkeypatch):
    stop = asyncio.Event()
    cancelled = False

    async def watcher():
        nonlocal cancelled
        try:
            await stop.wait()
        except asyncio.CancelledError:
            cancelled = True
            raise

    task = asyncio.create_task(watcher())
    monkeypatch.setattr(pa, "_control_watcher_task", task)
    monkeypatch.setattr(pa, "_control_watcher_stop", stop)
    monkeypatch.setattr(pa, "_control_owner_agent_id", None)
    monkeypatch.setattr(pa, "_control_owner_lease_token", 9)

    await pa._stop_thread_control_watcher()

    assert not cancelled
    assert task.done()
    assert pa._control_watcher_task is None
    assert pa._control_watcher_stop is None
    assert pa._control_owner_lease_token is None


@pytest.mark.asyncio
async def test_final_pinned_drain_continues_cleanup_when_binding_moves(monkeypatch):
    gate = AsyncMock(side_effect=[True, False])
    drain = AsyncMock(
        side_effect=pa.ControlInboxBlocked("control owner lost pinned binding")
    )
    monkeypatch.setattr(pa, "_set_pinned_control_admission", gate)
    monkeypatch.setattr(pa, "_drain_thread_controls", drain)

    assert not await pa._close_pinned_control_inbox(agent_id=str(PINNED_AGENT_ID))
    assert gate.await_args_list == [
        call(agent_id=str(PINNED_AGENT_ID), open_for_admission=False),
        call(agent_id=str(PINNED_AGENT_ID), open_for_admission=False),
    ]
    drain.assert_awaited_once_with(agent_id=str(PINNED_AGENT_ID))


@pytest.mark.asyncio
async def test_final_pinned_drain_failure_stays_fatal_for_current_owner(monkeypatch):
    gate = AsyncMock(side_effect=[True, True])
    drain = AsyncMock(side_effect=pa.ControlInboxBlocked("watermark_gap"))
    monkeypatch.setattr(pa, "_set_pinned_control_admission", gate)
    monkeypatch.setattr(pa, "_drain_thread_controls", drain)

    with pytest.raises(pa.ControlInboxBlocked, match="watermark_gap"):
        await pa._close_pinned_control_inbox(agent_id=str(PINNED_AGENT_ID))

    assert gate.await_count == 2
    drain.assert_awaited_once_with(agent_id=str(PINNED_AGENT_ID))


class _PinnedStatusConn:
    def __init__(
        self,
        *,
        agent_id=PINNED_AGENT_ID,
        runtime_generation="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        runtime_attach_token=None,
        reciprocal=1,
        updated=THREAD_ID,
    ):
        self.agent_id = agent_id
        self.runtime_generation = runtime_generation
        self.runtime_attach_token = runtime_attach_token
        self.reciprocal = reciprocal
        self.updated = updated
        self.calls: list[tuple[str, tuple]] = []

    def transaction(self):
        return _Transaction()

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        return {
            "execution_lane": "pinned",
            "agent_id": self.agent_id,
            "runtime_generation": self.runtime_generation,
            "runtime_attach_token": self.runtime_attach_token,
        }

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        if "FROM agents" in sql:
            return self.reciprocal
        if "UPDATE threads" in sql:
            return self.updated
        raise AssertionError(sql)


@pytest.mark.asyncio
async def test_exact_pinned_ended_status_rest_false_never_bypasses_retirement(
    monkeypatch,
):
    conn = _PinnedStatusConn()
    client = SimpleNamespace(update_thread_status=AsyncMock(return_value=False))
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(postgres_conn=SimpleNamespace(acquire=lambda: _Acquire(conn))),
    )

    assert not await pa._update_thread_status(
        "ended", pinned_agent_id=str(PINNED_AGENT_ID)
    )
    client.update_thread_status.assert_awaited_once_with(
        str(THREAD_ID),
        "ended",
        pinned_agent_id=str(PINNED_AGENT_ID),
    )
    assert not any("UPDATE threads" in sql for sql, _args in conn.calls)


@pytest.mark.asyncio
async def test_exact_pinned_status_refuses_moved_binding_without_update(monkeypatch):
    successor = UUID("55555555-5555-4555-8555-555555555555")
    conn = _PinnedStatusConn(agent_id=successor)
    monkeypatch.setattr(
        pa,
        "_orchestrator_client",
        SimpleNamespace(update_thread_status=AsyncMock(return_value=False)),
    )
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(postgres_conn=SimpleNamespace(acquire=lambda: _Acquire(conn))),
    )

    assert not await pa._update_thread_status(
        "ended", pinned_agent_id=str(PINNED_AGENT_ID)
    )
    assert not any("UPDATE threads" in sql for sql, _args in conn.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["active", "awaiting_user"])
async def test_advertised_pinned_identity_fences_every_status_write(
    monkeypatch, status
):
    conn = _PinnedStatusConn()
    client = SimpleNamespace(
        agent_id=str(PINNED_AGENT_ID),
        update_thread_status=AsyncMock(return_value=False),
    )
    monkeypatch.setattr(pa._session_identity, "_status_contract", True)
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(postgres_conn=SimpleNamespace(acquire=lambda: _Acquire(conn))),
    )

    assert await pa._update_thread_status(status)
    client.update_thread_status.assert_awaited_once_with(
        str(THREAD_ID),
        status,
        pinned_agent_id=str(PINNED_AGENT_ID),
    )
    assert any(
        "agent_id = $2::uuid" in sql and "UPDATE threads" in sql
        for sql, _args in conn.calls
    )


@pytest.mark.asyncio
async def test_old_server_compatibility_omits_identity_until_advertised(monkeypatch):
    client = SimpleNamespace(
        agent_id=str(PINNED_AGENT_ID),
        update_thread_status=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(pa._session_identity, "_status_contract", False)
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa, "_session", None)

    assert await pa._update_thread_status("active")
    client.update_thread_status.assert_awaited_once_with(str(THREAD_ID), "active")


@pytest.mark.asyncio
async def test_runtime_generation_fences_rest_and_direct_database_status_write(
    monkeypatch,
):
    generation = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    attach_token = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    conn = _PinnedStatusConn(
        runtime_generation=generation,
        runtime_attach_token=attach_token,
    )
    client = SimpleNamespace(
        agent_id=str(PINNED_AGENT_ID),
        update_thread_status=AsyncMock(return_value=False),
    )
    monkeypatch.setattr(pa._session_identity, "_status_contract", True)
    monkeypatch.setattr(pa._session_identity, "_runtime_contract", True)
    monkeypatch.setattr(pa._session_identity, "_session_generation", generation)
    monkeypatch.setattr(pa._session_identity, "_attach_token", attach_token)
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(postgres_conn=SimpleNamespace(acquire=lambda: _Acquire(conn))),
    )

    assert await pa._update_thread_status("active")
    client.update_thread_status.assert_awaited_once_with(
        str(THREAD_ID),
        "active",
        pinned_agent_id=str(PINNED_AGENT_ID),
        session_runtime_generation=generation,
        session_runtime_attach_token=attach_token,
    )
    update_sql, update_args = next(
        (sql, args) for sql, args in conn.calls if "UPDATE threads" in sql
    )
    assert "runtime_generation = $3::uuid" in update_sql
    assert "runtime_attach_token IS NOT DISTINCT FROM $4::uuid" in update_sql
    assert update_args == (
        str(THREAD_ID),
        str(PINNED_AGENT_ID),
        generation,
        attach_token,
    )


@pytest.mark.asyncio
async def test_stale_runtime_generation_cannot_use_direct_database_fallback(
    monkeypatch,
):
    generation_a = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    generation_b = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    conn = _PinnedStatusConn(runtime_generation=generation_b)
    client = SimpleNamespace(
        agent_id=str(PINNED_AGENT_ID),
        update_thread_status=AsyncMock(return_value=False),
    )
    monkeypatch.setattr(pa._session_identity, "_status_contract", True)
    monkeypatch.setattr(pa._session_identity, "_runtime_contract", True)
    monkeypatch.setattr(pa._session_identity, "_session_generation", generation_a)
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(postgres_conn=SimpleNamespace(acquire=lambda: _Acquire(conn))),
    )

    assert not await pa._update_thread_status("active")
    assert not any("UPDATE threads" in sql for sql, _args in conn.calls)
