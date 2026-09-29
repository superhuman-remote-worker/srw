"""Stateless manual ``/compact`` over the durable control inbox, plus the
pinned mid-turn guard it shares a core with.

Design: knowledge-base/knowledge/features/session_slash_commands_and_stateless_compact.md
§3-§4. The claimant's drain runs the lane-neutral compaction core, writes the
fenced checkpoint row stamped with the request id, and journals
``context.compacted`` as the durable receipt. Only failures a successor must
retry block the inbox; a summarizer failure is a durable rejection, because a
blocked control is requeued forever and would starve the session's input.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

import agent.api.persistent_app as pa
from agent.api.lease_context import LeaseHandle, current_lease
from shared.thread_controls import ControlRequest, control_receipt_result


THREAD_ID = UUID("11111111-1111-4111-8111-111111111111")
REQUEST_ID = UUID("22222222-2222-4222-8222-222222222222")
CLIENT_REQUEST_ID = UUID(int=1)
RUNTIME_GENERATION = UUID("55555555-5555-4555-8555-555555555555")
LEASE = 9


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _compact_request(payload: dict | None = None) -> ControlRequest:
    return ControlRequest(
        id=REQUEST_ID,
        thread_id=THREAD_ID,
        request_seq=1,
        client_request_id=CLIENT_REQUEST_ID,
        verb="compact",
        payload={} if payload is None else payload,
        accepted_agent_id=None,
        runtime_generation=RUNTIME_GENERATION,
    )


def _envelope() -> dict:
    return {
        "request_id": str(REQUEST_ID),
        "client_request_id": str(CLIENT_REQUEST_ID),
        "request_seq": 1,
        "method": "compact",
    }


@pytest.fixture
def stateless_owner(monkeypatch):
    conn = MagicMock()
    pool = SimpleNamespace(
        acquire=lambda: _Acquire(conn),
        get_latest_compaction_checkpoint=AsyncMock(return_value=None),
    )
    session = SimpleNamespace(
        postgres_conn=pool,
        permission_mode="supervised",
        narration_mode="auto",
        turn_count=12,
        context_manager=SimpleNamespace(_last_summarization_stats=None),
    )
    monkeypatch.setattr(pa, "_session", session)
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa, "_resume_compaction_receipt", None)
    handle = LeaseHandle()
    handle.update(str(THREAD_ID), LEASE)
    token = current_lease.set(handle)
    try:
        yield session
    finally:
        current_lease.reset(token)


async def _drain(request, **patches):
    """Drain exactly ``request`` under lease 9; returns (journal, finalize)."""
    journal = patches.pop("journal", AsyncMock(return_value=(6, 44)))
    # The finalizer answers with the outcome it terminalized.
    finalize = patches.pop(
        "finalize",
        AsyncMock(side_effect=lambda _request, **kwargs: kwargs["outcome"]),
    )
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
            AsyncMock(return_value=None),
        ),
        patch.object(pa, "_broadcast_durable", journal),
        patch.object(pa, "_finalize_durable_control", finalize),
    ):
        applied = await pa._drain_thread_controls(lease_token=LEASE)
    return applied, journal, finalize


# ── Drain applier ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_compaction_persists_its_stamped_checkpoint_before_the_receipt(
    stateless_owner,
):
    request = _compact_request({"focus": "pricing", "boundary_message_id": "m-7"})
    order: list[str] = []

    async def compact(focus, boundary):
        order.append(f"compact:{focus}:{boundary}")
        return "compacted", "the summary", 40, 6

    async def persist(*args, **kwargs):
        order.append("persist")
        return True

    async def journal(*_args, **_kwargs):
        order.append("journal")
        return 6, 44

    async def finalize(*_args, **_kwargs):
        order.append("finalize")
        return "applied"

    persist_mock = AsyncMock(side_effect=persist)
    with (
        patch.object(pa, "_compact_session_manually", AsyncMock(side_effect=compact)),
        patch.object(pa, "_persist_compaction_checkpoint", persist_mock),
    ):
        applied, journal_mock, finalize_mock = await _drain(
            request,
            journal=AsyncMock(side_effect=journal),
            finalize=AsyncMock(side_effect=finalize),
        )

    assert applied == 1
    assert order == ["compact:pricing:m-7", "persist", "journal", "finalize"]
    persist_mock.assert_awaited_once_with(
        "the summary", 40, 6, "manual", control_request_id=str(REQUEST_ID)
    )
    kind, params = journal_mock.await_args.args
    assert kind == "context.compacted"
    assert params == {
        **_envelope(),
        "before": 40,
        "after": 6,
        "trigger": "manual",
        "summary": "the summary",
        "turn": 12,
    }
    assert journal_mock.await_args.kwargs == {
        "control_request_id": str(REQUEST_ID),
        "lease_token": LEASE,
        "agent_id": None,
    }
    assert finalize_mock.await_args.kwargs["outcome"] == "applied"
    # The journaled receipt is exactly what the shared validator accepts.
    assert control_receipt_result(
        request_id=REQUEST_ID,
        client_request_id=CLIENT_REQUEST_ID,
        request_seq=1,
        verb="compact",
        request_payload=request.payload,
        event_kind=kind,
        event_payload=params,
    ) == ("applied", None, None)


@pytest.mark.asyncio
async def test_nothing_to_fold_is_a_summaryless_receipt_without_checkpoint(
    stateless_owner,
):
    persist = AsyncMock()
    with (
        patch.object(
            pa,
            "_compact_session_manually",
            AsyncMock(return_value=("noop", None, 5, 5)),
        ),
        patch.object(pa, "_persist_compaction_checkpoint", persist),
    ):
        _applied, journal, finalize = await _drain(_compact_request())

    persist.assert_not_awaited()
    kind, params = journal.await_args.args
    assert kind == "context.compacted"
    assert params["summary"] is None
    assert params["trigger"] == "manual"
    assert finalize.await_args.kwargs["outcome"] == "applied"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "compaction",
    [
        AsyncMock(return_value=("failed", None, 40, 40)),
        AsyncMock(side_effect=RuntimeError("aux provider exploded")),
    ],
)
async def test_summarizer_failure_is_a_durable_rejection_not_a_block(
    stateless_owner, compaction
):
    persist = AsyncMock()
    with (
        patch.object(pa, "_compact_session_manually", compaction),
        patch.object(pa, "_persist_compaction_checkpoint", persist),
    ):
        applied, journal, finalize = await _drain(_compact_request())

    assert applied == 1
    persist.assert_not_awaited()
    kind, params = journal.await_args.args
    assert kind == "control.rejected"
    assert params == {**_envelope(), "error_code": "compaction_failed"}
    assert finalize.await_args.kwargs == {
        "lease_token": LEASE,
        "agent_id": None,
        "outcome": "rejected",
        "error_code": "compaction_failed",
    }


@pytest.mark.asyncio
async def test_boundary_no_longer_resident_is_rejected(stateless_owner):
    refusal = pa._ManualCompactionRefused("boundary_not_in_context", "gone")
    with patch.object(pa, "_compact_session_manually", AsyncMock(side_effect=refusal)):
        _applied, journal, _finalize = await _drain(
            _compact_request({"boundary_message_id": "m-1"})
        )

    kind, params = journal.await_args.args
    assert kind == "control.rejected"
    assert params["error_code"] == "boundary_not_in_context"


@pytest.mark.asyncio
async def test_terminating_runtime_leaves_the_request_pending(stateless_owner):
    refusal = pa._ManualCompactionRefused("runtime_terminating", "closing")
    journal = AsyncMock()
    with patch.object(pa, "_compact_session_manually", AsyncMock(side_effect=refusal)):
        with pytest.raises(pa.ControlInboxBlocked, match="runtime_terminating"):
            await _drain(_compact_request(), journal=journal)
    journal.assert_not_awaited()


@pytest.mark.asyncio
async def test_unwritten_checkpoint_blocks_instead_of_receipting_a_lost_compaction(
    stateless_owner,
):
    journal = AsyncMock()
    with (
        patch.object(
            pa,
            "_compact_session_manually",
            AsyncMock(return_value=("compacted", "summary", 40, 6)),
        ),
        patch.object(
            pa,
            "_persist_compaction_checkpoint",
            AsyncMock(side_effect=RuntimeError("db down")),
        ),
    ):
        with pytest.raises(pa.ControlInboxBlocked, match="persist failed"):
            await _drain(_compact_request(), journal=journal)
    journal.assert_not_awaited()


@pytest.mark.asyncio
async def test_crash_after_checkpoint_is_answered_from_that_row(stateless_owner):
    stateless_owner.postgres_conn.get_latest_compaction_checkpoint.return_value = {
        "summary": "already folded",
        "before": 40,
        "after": 6,
        "trigger": "manual",
        "turn_number": 11,
        "control_request_id": str(REQUEST_ID),
    }
    compaction = AsyncMock()
    with patch.object(pa, "_compact_session_manually", compaction):
        _applied, journal, finalize = await _drain(_compact_request())

    compaction.assert_not_awaited()
    kind, params = journal.await_args.args
    assert kind == "context.compacted"
    assert params == {
        **_envelope(),
        "before": 40,
        "after": 6,
        "trigger": "manual",
        "summary": "already folded",
        "turn": 11,
    }
    assert finalize.await_args.kwargs["outcome"] == "applied"


@pytest.mark.asyncio
async def test_checkpoint_of_another_request_does_not_short_circuit(stateless_owner):
    stateless_owner.postgres_conn.get_latest_compaction_checkpoint.return_value = {
        "summary": "older",
        "control_request_id": str(UUID(int=99)),
    }
    compaction = AsyncMock(return_value=("noop", None, 5, 5))
    with patch.object(pa, "_compact_session_manually", compaction):
        await _drain(_compact_request())
    compaction.assert_awaited_once()


@pytest.mark.asyncio
async def test_resume_compaction_under_this_lease_satisfies_the_request(
    stateless_owner, monkeypatch
):
    monkeypatch.setattr(
        pa,
        "_resume_compaction_receipt",
        (
            LEASE,
            {
                "before": 300,
                "after": 20,
                "trigger": "resume",
                "summary": "resume summary",
                "turn": 12,
            },
        ),
    )
    compaction = AsyncMock()
    with patch.object(pa, "_compact_session_manually", compaction):
        _applied, journal, _finalize = await _drain(_compact_request())

    compaction.assert_not_awaited()
    kind, params = journal.await_args.args
    assert kind == "context.compacted"
    # Same banner turn as the resume frame, so the Cockpit replaces it.
    assert params["turn"] == 12
    assert params["summary"] == "resume summary"
    assert params["trigger"] == "manual"
    assert params["satisfied_by"] == "resume"


@pytest.mark.asyncio
async def test_resume_compaction_of_an_earlier_claim_does_not_satisfy(
    stateless_owner, monkeypatch
):
    monkeypatch.setattr(
        pa, "_resume_compaction_receipt", (LEASE - 1, {"summary": "stale"})
    )
    compaction = AsyncMock(return_value=("noop", None, 5, 5))
    with patch.object(pa, "_compact_session_manually", compaction):
        await _drain(_compact_request())
    compaction.assert_awaited_once()


@pytest.mark.parametrize(
    "payload",
    [{"mode": "auto"}, {"focus": ""}, {"boundary_message_id": 7}],
)
def test_malformed_compact_payload_is_described_unsupported(payload):
    assert pa._describe_control_request(_compact_request(payload)) == (
        "control.rejected",
        "rejected",
        "unsupported_control",
    )


def test_ram_convergence_is_a_no_op_for_compact(stateless_owner):
    assert pa._apply_control_request(_compact_request()) == (
        "context.compacted",
        "applied",
        None,
    )
    assert stateless_owner.permission_mode == "supervised"


@pytest.mark.asyncio
async def test_compact_on_the_pinned_owner_credential_is_rejected():
    # Admission keeps compact off the pinned inbox; the applier still refuses
    # rather than compacting without a lease fence.
    assert await pa._apply_compact_control(_compact_request(), lease_token=None) == (
        "control.rejected",
        "rejected",
        "unsupported_control",
        {},
    )


# ── Shared receipt contract ──────────────────────────────────────────────────


def _receipt(kind: str = "context.compacted", request_payload=None, **overrides):
    payload = {
        **_envelope(),
        "before": 40,
        "after": 6,
        "trigger": "manual",
        "summary": "s",
        "turn": 3,
    }
    payload.update(overrides)
    return control_receipt_result(
        request_id=REQUEST_ID,
        client_request_id=CLIENT_REQUEST_ID,
        request_seq=1,
        verb="compact",
        request_payload={} if request_payload is None else request_payload,
        event_kind=kind,
        event_payload=payload,
    )


def test_compact_receipts_are_applied_for_folded_and_empty_results():
    assert _receipt() == ("applied", None, None)
    assert _receipt(summary=None) == ("applied", None, None)
    assert _receipt(request_payload={"focus": "x"}) == ("applied", None, None)


@pytest.mark.parametrize(
    "overrides",
    [
        {"trigger": "auto"},
        {"summary": ""},
        {"method": "workspace.undo"},
        {"request_seq": 2},
    ],
)
def test_compact_receipt_rejects_mismatches(overrides):
    assert _receipt(**overrides) is None


def test_compact_rejection_receipt_is_valid():
    assert control_receipt_result(
        request_id=REQUEST_ID,
        client_request_id=CLIENT_REQUEST_ID,
        request_seq=1,
        verb="compact",
        request_payload={},
        event_kind="control.rejected",
        event_payload={**_envelope(), "error_code": "compaction_failed"},
    ) == ("rejected", "compaction_failed", None)


# ── Resume marker ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_resume_checkpoint_is_remembered_under_the_current_lease(
    stateless_owner, monkeypatch
):
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")
    monkeypatch.setattr(pa, "_broadcast", MagicMock())
    stateless_owner.postgres_conn.save_thread_message = AsyncMock()

    await pa._record_compaction("resume summary", 300, 20, trigger="resume")

    lease, params = pa._resume_compaction_receipt
    assert lease == LEASE
    assert params["summary"] == "resume summary"
    assert params["turn"] == 12


@pytest.mark.asyncio
async def test_unpersisted_resume_compaction_is_not_remembered(
    stateless_owner, monkeypatch
):
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")
    monkeypatch.setattr(pa, "_broadcast", MagicMock())
    stateless_owner.postgres_conn.save_thread_message = AsyncMock(
        side_effect=RuntimeError("fence lost")
    )

    await pa._record_compaction("resume summary", 300, 20, trigger="resume")

    assert pa._resume_compaction_receipt is None


# ── Shared core + pinned guard ───────────────────────────────────────────────


def _core_session(ctx_mgr):
    return SimpleNamespace(
        messages=[MagicMock(id="a"), MagicMock(id="b")],
        context_manager=ctx_mgr,
        auxiliary_llm=MagicMock(),
        config=SimpleNamespace(
            context_management=SimpleNamespace(max_summary_length=100)
        ),
        turn_count=3,
    )


@pytest.mark.asyncio
async def test_core_reports_a_summarizer_failure_distinctly(monkeypatch):
    ctx_mgr = SimpleNamespace(
        compaction_runs=2,
        last_compaction_failed=True,
        summarize_and_compact=AsyncMock(side_effect=lambda **kw: kw["messages"]),
        set_progress_callback=MagicMock(),
    )
    monkeypatch.setattr(pa, "_session", _core_session(ctx_mgr))
    monkeypatch.setattr(pa, "_runtime_admission_closed", lambda: False)

    outcome, summary, before, after = await pa._compact_session_manually()

    assert (outcome, summary, before, after) == ("failed", None, 2, 2)
    ctx_mgr.set_progress_callback.assert_called_once_with(pa._loop_compaction_progress)


@pytest.mark.asyncio
async def test_core_refuses_a_boundary_that_is_not_resident(monkeypatch):
    ctx_mgr = SimpleNamespace(
        compaction_runs=0,
        summarize_and_compact=AsyncMock(),
        set_progress_callback=MagicMock(),
    )
    monkeypatch.setattr(pa, "_session", _core_session(ctx_mgr))

    with pytest.raises(pa._ManualCompactionRefused) as refused:
        await pa._compact_session_manually(
            boundary_message_id="99999999-9999-4999-8999-999999999999"
        )

    assert refused.value.code == "boundary_not_in_context"
    ctx_mgr.summarize_and_compact.assert_not_awaited()


@pytest.mark.asyncio
async def test_pinned_compact_never_runs_beside_a_turn(monkeypatch):
    sent: list[tuple[str, dict]] = []

    async def _ws_send(_ws, method, params):
        sent.append((method, params))

    compaction = AsyncMock()
    monkeypatch.setattr(pa, "_ws_send", _ws_send)
    monkeypatch.setattr(pa, "_runtime_admission_closed", lambda: False)
    monkeypatch.setattr(pa, "_turn_in_flight", lambda: True)
    monkeypatch.setattr(pa, "_MANUAL_COMPACT_IDLE_WAIT_S", 0.05)
    monkeypatch.setattr(pa, "_MANUAL_COMPACT_IDLE_POLL_S", 0.01)
    monkeypatch.setattr(pa, "_compact_session_manually", compaction)

    await pa._handle_compact(MagicMock(), "")

    compaction.assert_not_awaited()
    assert sent and sent[0][0] == "error"
    assert "still running" in sent[0][1]["message"]


@pytest.mark.asyncio
async def test_pinned_compact_waits_for_the_turn_to_park(monkeypatch):
    busy = iter([True, True, False])
    compaction = AsyncMock(return_value=("noop", None, 4, 4))
    sent: list[tuple[str, dict]] = []

    async def _ws_send(_ws, method, params):
        sent.append((method, params))

    monkeypatch.setattr(pa, "_ws_send", _ws_send)
    monkeypatch.setattr(pa, "_runtime_admission_closed", lambda: False)
    monkeypatch.setattr(pa, "_turn_in_flight", lambda: next(busy, False))
    monkeypatch.setattr(pa, "_MANUAL_COMPACT_IDLE_POLL_S", 0.001)
    monkeypatch.setattr(pa, "_compact_session_manually", compaction)
    monkeypatch.setattr(
        pa, "_session", SimpleNamespace(turn_count=4, workspace_manager=None)
    )

    await pa._handle_compact(MagicMock(), "focus")

    compaction.assert_awaited_once_with("focus", None)
    assert sent == [
        (
            "context.compacted",
            {"before": 4, "after": 4, "trigger": "manual", "summary": None, "turn": 4},
        )
    ]


@pytest.mark.asyncio
async def test_pinned_failed_compaction_does_not_claim_nothing_to_compact(
    monkeypatch,
):
    sent: list[tuple[str, dict]] = []

    async def _ws_send(_ws, method, params):
        sent.append((method, params))

    monkeypatch.setattr(pa, "_ws_send", _ws_send)
    monkeypatch.setattr(pa, "_runtime_admission_closed", lambda: False)
    monkeypatch.setattr(pa, "_turn_in_flight", lambda: False)
    monkeypatch.setattr(
        pa,
        "_compact_session_manually",
        AsyncMock(return_value=("failed", None, 4, 4)),
    )
    monkeypatch.setattr(
        pa, "_session", SimpleNamespace(turn_count=4, workspace_manager=None)
    )

    await pa._handle_compact(MagicMock(), "")

    # compaction.failed (journaled by the engine) is the only notice.
    assert sent == []
