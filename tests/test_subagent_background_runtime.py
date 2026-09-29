"""U4-B background runtime: durable admission, delivery, controls and teardown."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage, AIMessageChunk

import agent.subagents.runtime as runtime_mod
from agent.subagents import NullLedger, RecordingLedger
from agent.subagents.persistence import RestoredSubagentTranscript
from tests._fake_chat_model import HANG, FakeChatModel, text_turn
from tests.test_subagent_runtime import call, make_parent, runtime_for


class GateModel(FakeChatModel):
    """Pause the first provider response until a control wins the race."""

    def __init__(self, final: str):
        super().__init__([])
        self.started = asyncio.Event()
        self.release_first = asyncio.Event()
        self.final = final

    async def astream(self, messages, **kwargs):
        del kwargs
        self.calls.append(list(messages))
        if len(self.calls) == 1:
            self.started.set()
            await self.release_first.wait()
            yield AIMessageChunk(content="unfinished")
            return
        for chunk in text_turn(self.final):
            await asyncio.sleep(0)
            yield chunk


class StrictLedger(RecordingLedger):
    def __init__(self) -> None:
        super().__init__()
        self.open_started = asyncio.Event()
        self.allow_open = asyncio.Event()
        self.allow_open.set()
        self.terminal = asyncio.Event()
        self.terminal_calls: list[tuple[str, dict[str, Any]]] = []
        self.foreground_terminal_calls: list[tuple[str, dict[str, Any]]] = []
        self.live: list[dict[str, Any]] = []
        self.refuse_open = False
        self.generations: dict[str, str] = {}
        self.reopen_calls: list[str] = []

    async def open(self, subagent_id: str, **fields: Any) -> dict[str, str] | None:
        self.open_started.set()
        await self.allow_open.wait()
        self.opened.append((subagent_id, dict(fields)))
        if self.refuse_open:
            return None
        generation = "aaaaaaaa-1111-4222-8333-444444444444"
        self.generations[subagent_id] = generation
        return {
            "thread_id": subagent_id,
            "runtime_generation": generation,
        }

    async def load_messages(self, subagent_id: str) -> RestoredSubagentTranscript:
        rows = [
            (message, turn)
            for child, message, turn in self.messages
            if child == subagent_id
        ]
        return RestoredSubagentTranscript(
            messages=[message for message, _turn in rows],
            turn_number=max((turn for _message, turn in rows), default=0),
        )

    async def reopen(self, subagent_id: str) -> dict[str, Any]:
        self.reopen_calls.append(subagent_id)
        generation = "bbbbbbbb-1111-4222-8333-444444444444"
        self.generations[subagent_id] = generation
        return {
            "result": "reopened",
            "thread_id": subagent_id,
            "runtime_generation": generation,
        }

    async def terminalize_and_enqueue(
        self, subagent_id: str, **fields: Any
    ) -> dict[str, Any]:
        self.terminal_calls.append((subagent_id, dict(fields)))
        opened = next(data for child, data in self.opened if child == subagent_id)
        delivery = {
            "id": fields["delivery_id"],
            "source": "subagent",
            "thread_id": subagent_id,
            "handle": opened["handle"],
            "run_generation": self.generations.get(
                subagent_id, "aaaaaaaa-1111-4222-8333-444444444444"
            ),
            "message": fields["message"],
            "timestamp": fields["timestamp"],
        }
        self.terminal.set()
        return {
            "result": "applied",
            "delivery_state": "queued",
            "delivery": delivery,
        }

    async def terminalize_foreground_orphan_and_enqueue(
        self, subagent_id: str, **fields: Any
    ) -> dict[str, Any]:
        self.foreground_terminal_calls.append((subagent_id, dict(fields)))
        return {
            "result": "applied",
            "delivery_state": "queued",
            "delivery": {
                "id": fields["delivery_id"],
                "source": "subagent",
                "role": "event",
            },
        }

    async def list_live(self, parent_job_id: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self.live]


class DurableStateLedger(StrictLedger):
    """Expose any by-call row, like the production worker ledger."""

    def __init__(self, row: dict[str, Any] | None = None) -> None:
        super().__init__()
        self.lookup_row = row
        self.fail_lookup = False

    async def lookup(
        self, parent_job_id: str, parent_tool_call_id: str
    ) -> dict[str, Any] | None:
        self.lookups.append((str(parent_job_id), str(parent_tool_call_id)))
        if self.fail_lookup:
            raise RuntimeError("lookup unavailable")
        return dict(self.lookup_row) if self.lookup_row is not None else None


@pytest.mark.asyncio
async def test_empty_recovered_runtime_quiesces_after_parent_end(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    runtime = runtime_for(ctx, ledger=ledger)
    assert await runtime.recover_orphans() == []
    runtime.host.settlement_authority_fn = AsyncMock(return_value=False)
    runtime.host.effect_authority_fn = AsyncMock(return_value=False)
    runtime._notify_changed = AsyncMock()

    await runtime.quiesce("parent End already authorized")

    assert runtime._accepting is False
    assert not ledger.opened and not ledger.terminal_calls
    runtime._notify_changed.assert_not_awaited()
    with pytest.raises(RuntimeError, match="exact parent authority"):
        await runtime.resume()


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_state", ["unrecovered", "reserved_handle"])
async def test_empty_quiescence_never_hides_unresolved_child_authority(
    tmp_path, prior_state
):
    ctx, _ = make_parent(tmp_path)
    runtime = runtime_for(ctx, ledger=StrictLedger())
    if prior_state == "reserved_handle":
        assert await runtime.recover_orphans() == []
        # Handles are reserved before a foreground build or durable create can
        # suspend, and remain recorded even after its active entry is removed.
        runtime.mint_handle("explorer")
    runtime.host.settlement_authority_fn = AsyncMock(return_value=False)

    with pytest.raises(RuntimeError, match="exact settlement authority"):
        await runtime.quiesce("parent End already authorized")
    assert runtime._accepting is False


@pytest.mark.asyncio
async def test_quiesced_runtime_resumes_only_with_exact_authority(tmp_path):
    ctx, _ = make_parent(tmp_path)
    runtime = runtime_for(ctx)
    authority = AsyncMock(return_value=True)
    runtime.host.effect_authority_fn = authority

    await runtime.quiesce("retirement preflight")
    assert runtime._accepting is False
    await runtime.resume()

    assert runtime._accepting is True
    assert authority.await_count == 2


@pytest.mark.asyncio
async def test_quiesced_runtime_refuses_resume_after_authority_loss(tmp_path):
    ctx, _ = make_parent(tmp_path)
    runtime = runtime_for(ctx)
    runtime.host.effect_authority_fn = AsyncMock(return_value=True)
    await runtime.quiesce("retirement preflight")
    runtime.host.effect_authority_fn = AsyncMock(return_value=False)

    with pytest.raises(RuntimeError, match="exact parent authority"):
        await runtime.resume()

    assert runtime._accepting is False


@pytest.mark.asyncio
async def test_provider_is_not_constructed_before_durable_receipt(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    ledger.allow_open.clear()
    factory_called = asyncio.Event()

    def factory(config, limits):
        factory_called.set()
        return FakeChatModel([text_turn("done")])

    runtime = runtime_for(ctx, factory=factory, ledger=ledger)
    spawn = asyncio.create_task(
        runtime.run_background(call(run_in_background=True, description="durable"))
    )
    await ledger.open_started.wait()
    await asyncio.sleep(0)
    assert not factory_called.is_set()
    ledger.allow_open.set()
    receipt = await spawn
    assert "· queued]" in receipt
    assert "aaaaaaaa-1111-4222-8333-444444444444" in receipt
    await asyncio.wait_for(factory_called.wait(), 5)
    await asyncio.wait_for(ledger.terminal.wait(), 5)
    await runtime.close()


@pytest.mark.asyncio
async def test_preempted_parent_terminal_receipt_never_creates_lane_b_backlog(tmp_path):
    class SuppressedTerminalLedger(StrictLedger):
        async def terminalize_and_enqueue(
            self, subagent_id: str, **fields: Any
        ) -> dict[str, Any]:
            self.terminal_calls.append((subagent_id, dict(fields)))
            self.terminal.set()
            return {"result": "applied", "delivery_state": "suppressed"}

    ctx, _ = make_parent(tmp_path)
    ledger = SuppressedTerminalLedger()
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: FakeChatModel([text_turn("done")]),
        ledger=ledger,
    )

    receipt = await runtime.run_background(call(run_in_background=True))
    handle = receipt.split()[1]
    await asyncio.wait_for(ledger.terminal.wait(), 5)
    tasks = list(runtime._background_tasks.values())
    if tasks:
        await asyncio.gather(*tasks)

    assert runtime._background[handle].status == "completed"
    assert runtime._background[handle].delivery_pending is False
    assert runtime.drain_local_deliveries() == []
    assert not runtime.has_completion_blockers()
    await runtime.close()


@pytest.mark.asyncio
async def test_create_refusal_never_schedules_provider(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    ledger.refuse_open = True
    made: list[FakeChatModel] = []
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: made.append(FakeChatModel([text_turn("never")])),
        ledger=ledger,
    )
    result = await runtime.run_background(call(run_in_background=True))
    assert result.startswith("Error: background subagent")
    assert made == []
    assert await runtime.list_agents() == []
    assert not runtime.has_completion_blockers()


@pytest.mark.asyncio
async def test_null_ledger_refuses_before_build_or_provider_task(tmp_path):
    ctx, _ = make_parent(tmp_path)
    made: list[Any] = []
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: made.append(object()),
        ledger=NullLedger(),
    )
    result = await runtime.run_background(call(run_in_background=True))
    assert "returned no create receipt" in result
    assert made == []
    assert runtime.active == {}
    assert await runtime.list_agents() == []


@pytest.mark.parametrize("status", ["queued", "running"])
@pytest.mark.asyncio
async def test_worker_background_live_durable_call_refuses_before_create(
    tmp_path, status: str
):
    ctx, _ = make_parent(tmp_path)
    ledger = DurableStateLedger(
        {
            "id": "bbbbbbbb-1111-4222-8333-444444444444",
            "parent_job_id": "parent-job",
            "parent_tool_call_id": "c1",
            "subagent_status": status,
        }
    )
    made: list[Any] = []
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: made.append(object()),
        ledger=ledger,
    )

    with pytest.raises(RuntimeError, match="already has a live durable child"):
        await runtime.run_background(call(run_in_background=True))

    assert ledger.opened == []
    assert made == []


@pytest.mark.asyncio
async def test_worker_background_lookup_failure_is_strict_before_create(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = DurableStateLedger()
    ledger.fail_lookup = True
    runtime = runtime_for(ctx, ledger=ledger)

    with pytest.raises(RuntimeError, match="idempotency lookup failed"):
        await runtime.run_background(call(run_in_background=True))

    assert ledger.opened == []


@pytest.mark.asyncio
async def test_worker_background_cold_terminal_replays_without_create(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = DurableStateLedger(
        {
            "id": "bbbbbbbb-1111-4222-8333-444444444444",
            "parent_job_id": "parent-job",
            "parent_tool_call_id": "c1",
            "subagent_handle": "explorer-dead",
            "subagent_type": "explorer",
            "subagent_status": "completed",
            "subagent_outcome": "completed",
            "status": "ended",
            "total_turns": 2,
            "total_tokens": 50,
        }
    )
    made: list[Any] = []
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: made.append(object()),
        ledger=ledger,
    )

    replay = await runtime.run_background(call(run_in_background=True))

    assert "Replayed: this child already ran for tool call c1" in replay
    assert ledger.opened == []
    assert made == []


@pytest.mark.asyncio
async def test_terminal_commit_precedes_local_publish_and_drain_releases_backpressure(
    tmp_path,
):
    ctx, _ = make_parent(tmp_path, max_concurrent=1)
    ledger = StrictLedger()
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: FakeChatModel([text_turn("evidence")]),
        ledger=ledger,
    )
    await runtime.run_background(call("one", run_in_background=True))
    await asyncio.wait_for(ledger.terminal.wait(), 5)
    assert runtime.has_completion_blockers()
    refused = await runtime.run_background(call("two", run_in_background=True))
    assert "backlog is full (1)" in refused
    deliveries = runtime.drain_local_deliveries()
    assert len(deliveries) == 1
    assert deliveries[0]["source"] == "subagent"
    assert deliveries[0]["message"].startswith("[subagent explorer-")
    assert not runtime.has_completion_blockers()
    ledger.terminal.clear()
    accepted = await runtime.run_background(call("three", run_in_background=True))
    assert "· queued]" in accepted
    await asyncio.wait_for(ledger.terminal.wait(), 5)
    await runtime.close()


@pytest.mark.asyncio
async def test_wait_uses_condition_and_wakes_on_terminal_commit(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    fake = FakeChatModel([HANG])
    runtime = runtime_for(ctx, factory=lambda config, limits: fake, ledger=ledger)
    receipt = await runtime.run_background(call(run_in_background=True))
    handle = receipt.split()[1]
    waiter = asyncio.create_task(runtime.wait_agent(handle, 10))
    await asyncio.wait_for(fake.hang_started.wait(), 5)
    await runtime.stop_agent(handle, 0.1)
    result = await asyncio.wait_for(waiter, 5)
    assert result["result"] == "ready"
    assert result["timed_out"] is False
    assert result["status"] in {"interrupted", "cancelled"}
    await runtime.close()


@pytest.mark.asyncio
async def test_steer_wins_before_finish_and_is_included_before_terminalization(
    tmp_path,
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    model = GateModel("steered evidence")
    runtime = runtime_for(ctx, factory=lambda config, limits: model, ledger=ledger)
    receipt = await runtime.run_background(call(run_in_background=True))
    handle = receipt.split()[1]
    await asyncio.wait_for(model.started.wait(), 5)
    assert (await runtime.message_agent(handle, "new evidence"))["result"] == "accepted"
    model.release_first.set()
    await asyncio.wait_for(ledger.terminal.wait(), 5)
    assert "steered evidence" in ledger.terminal_calls[0][1]["message"]
    assert (await runtime.message_agent(handle, "too late"))["result"] == (
        "report_pending"
    )
    assert len(ledger.terminal_calls) == 1
    await runtime.close()


@pytest.mark.asyncio
async def test_terminal_message_rotates_generation_and_resumes_durable_history(
    tmp_path,
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    first = FakeChatModel([text_turn("first report")])
    second = FakeChatModel([text_turn("second report")])
    models = [first, second]
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: models.pop(0),
        ledger=ledger,
    )

    receipt = await runtime.run_background(call(run_in_background=True))
    handle = receipt.split()[1]
    first_task = runtime._background[handle].task
    assert first_task is not None
    await asyncio.wait_for(first_task, 5)
    first_delivery = ledger.terminal_calls[0][1]["delivery_id"]
    first_turn = max(turn for _child, _message, turn in ledger.messages)
    assert runtime.drain_local_deliveries()

    revived = await runtime.message_agent(handle, "check the new evidence")
    assert revived == {
        "result": "revived",
        "handle": handle,
        "status": "queued",
        "runtime_generation": "bbbbbbbb-1111-4222-8333-444444444444",
    }
    second_task = runtime._background[handle].task
    assert second_task is not None and second_task is not first_task
    await asyncio.wait_for(second_task, 5)

    assert ledger.reopen_calls == [runtime._background[handle].subagent_id]
    assert len(first.calls) == 1
    assert len(second.calls) == 1
    resumed = second.calls[0]
    assert any(getattr(message, "content", "") == "first report" for message in resumed)
    assert any(
        getattr(message, "content", "") == "check the new evidence"
        for message in resumed
    )
    assert max(turn for _child, _message, turn in ledger.messages) > first_turn
    assert len(ledger.terminal_calls) == 2
    assert ledger.terminal_calls[1][1]["delivery_id"] != first_delivery
    await runtime.close()


@pytest.mark.asyncio
async def test_terminal_revival_reconciles_a_timed_out_reopen(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    class LostAckLedger(StrictLedger):
        async def reopen(self, subagent_id: str) -> dict[str, Any]:
            self.reopen_calls.append(subagent_id)
            generation = "bbbbbbbb-1111-4222-8333-444444444444"
            self.generations[subagent_id] = generation
            if len(self.reopen_calls) == 1:
                # The durable rotation committed, but its response never
                # reached the caller before the local ledger deadline.
                await asyncio.sleep(1)
            return {
                "result": "reopened",
                "thread_id": subagent_id,
                "runtime_generation": generation,
                "reconciled": len(self.reopen_calls) > 1,
            }

    monkeypatch.setattr(runtime_mod, "_LEDGER_TIMEOUT_S", 0.01)
    ctx, _ = make_parent(tmp_path)
    ledger = LostAckLedger()
    models = [
        FakeChatModel([text_turn("first report")]),
        FakeChatModel([text_turn("second report")]),
    ]
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: models.pop(0),
        ledger=ledger,
    )
    receipt = await runtime.run_background(call(run_in_background=True))
    handle = receipt.split()[1]
    first_task = runtime._background[handle].task
    assert first_task is not None
    await asyncio.wait_for(first_task, 5)
    runtime.drain_local_deliveries()

    revived = await runtime.message_agent(handle, "resume after lost ack")

    assert revived["result"] == "revived"
    assert len(ledger.reopen_calls) == 2
    assert runtime._background[handle].runtime_generation == (
        "bbbbbbbb-1111-4222-8333-444444444444"
    )
    await runtime.close()


@pytest.mark.asyncio
async def test_multiple_live_steers_remain_in_one_tracked_brief(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    model = GateModel("steered evidence")
    runtime = runtime_for(ctx, factory=lambda config, limits: model, ledger=ledger)
    receipt = await runtime.run_background(call(run_in_background=True))
    handle = receipt.split()[1]
    await asyncio.wait_for(model.started.wait(), 5)
    assert (await runtime.message_agent(handle, "first steer"))["result"] == "accepted"
    assert (await runtime.message_agent(handle, "second steer"))["result"] == (
        "accepted"
    )
    model.release_first.set()
    await asyncio.wait_for(ledger.terminal.wait(), 5)
    assert len(model.calls) == 3
    # The interrupted first stream still reached the provider and is now
    # truthfully audited; both accepted steer continuations remain inside the
    # same single terminalized brief.
    assert ledger.terminal_calls[0][1]["turns"] == 3
    tasks = list(runtime._background_tasks.values())
    if tasks:
        await asyncio.gather(*tasks)
        await asyncio.sleep(0)
    assert runtime._background_tasks == {}
    await runtime.close()


@pytest.mark.asyncio
async def test_stop_attempts_one_toolless_partial_before_terminal_delivery(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    model = GateModel("partial before stop")
    runtime = runtime_for(ctx, factory=lambda config, limits: model, ledger=ledger)
    receipt = await runtime.run_background(call(run_in_background=True))
    handle = receipt.split()[1]
    await asyncio.wait_for(model.started.wait(), 5)
    stopping = asyncio.create_task(runtime.stop_agent(handle, 2))
    await asyncio.sleep(0)
    model.release_first.set()
    stopped = await asyncio.wait_for(stopping, 5)
    assert stopped["result"] == "stopped"
    await asyncio.wait_for(ledger.terminal.wait(), 5)
    terminal = ledger.terminal_calls[0][1]
    assert terminal["outcome"] == "interrupted:stopped"
    assert "partial before stop" in terminal["message"]
    assert len(model.calls) == 2
    await runtime.close()


@pytest.mark.asyncio
async def test_stop_before_driver_start_never_reaches_provider(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    model = FakeChatModel([text_turn("must not run")])
    runtime = runtime_for(ctx, factory=lambda config, limits: model, ledger=ledger)
    published = asyncio.Event()
    release_notification = asyncio.Event()
    original_notify = runtime._notify_changed

    async def gated_notify():
        if runtime.active and not ledger.terminal.is_set():
            published.set()
            await release_notification.wait()
        await original_notify()

    runtime._notify_changed = gated_notify
    spawning = asyncio.create_task(runtime.run_background(call(run_in_background=True)))
    await asyncio.wait_for(published.wait(), 5)
    agents = await runtime.list_agents()
    handle = agents[0]["handle"]
    stopping = asyncio.create_task(runtime.stop_agent(handle, 0.1))
    await asyncio.sleep(0)
    release_notification.set()
    receipt = await asyncio.wait_for(spawning, 5)
    assert handle in receipt
    await asyncio.wait_for(stopping, 5)
    await asyncio.wait_for(ledger.terminal.wait(), 5)
    assert model.calls == []
    assert ledger.terminal_calls[0][1]["outcome"] == "interrupted:stopped"
    await runtime.close()


@pytest.mark.asyncio
async def test_stop_discards_queued_steer_before_single_synthesis(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    model = GateModel("partial before stop")
    runtime = runtime_for(ctx, factory=lambda config, limits: model, ledger=ledger)
    receipt = await runtime.run_background(call(run_in_background=True))
    handle = receipt.split()[1]
    await asyncio.wait_for(model.started.wait(), 5)
    assert (await runtime.message_agent(handle, "stale queued steer"))["result"] == (
        "accepted"
    )
    stopping = asyncio.create_task(runtime.stop_agent(handle, 2))
    await asyncio.sleep(0)
    model.release_first.set()
    await asyncio.wait_for(stopping, 5)
    await asyncio.wait_for(ledger.terminal.wait(), 5)
    assert len(model.calls) == 2
    assert "stale queued steer" not in str(model.calls[1])
    assert "Do NOT call any more tools" in str(model.calls[1])
    assert ledger.terminal_calls[0][1]["outcome"] == "interrupted:stopped"
    await runtime.close()


@pytest.mark.asyncio
async def test_abandon_cancels_local_work_without_terminal_or_delivery(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    fake = FakeChatModel([HANG])
    runtime = runtime_for(ctx, factory=lambda config, limits: fake, ledger=ledger)
    await runtime.run_background(call(run_in_background=True))
    await asyncio.wait_for(fake.hang_started.wait(), 5)
    await runtime.abandon("lease stolen")
    assert ledger.terminal_calls == []
    assert runtime.drain_local_deliveries() == []
    assert runtime.active == {}


@pytest.mark.asyncio
async def test_orphan_recovery_delivers_background_once_and_never_calls_provider(
    tmp_path,
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    child = "bbbbbbbb-1111-4222-8333-444444444444"
    ledger.opened.append(
        (
            child,
            {"handle": "explorer-dead", "subagent_type": "explorer"},
        )
    )
    ledger.live = [
        {
            "thread_id": child,
            "runtime_generation": "aaaaaaaa-1111-4222-8333-444444444444",
            "parent_job_id": "parent-job",
            "handle": "explorer-dead",
            "subagent_type": "explorer",
            "parent_tool_call_id": "old-call",
            "run_in_background": True,
            "total_turns": 2,
            "total_tokens": 50,
        }
    ]
    made: list[Any] = []
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: made.append(object()),
        ledger=ledger,
    )
    recovered = await runtime.recover_orphans()
    assert recovered[0]["status"] == "interrupted"
    assert made == []
    assert len(ledger.terminal_calls) == 1
    assert "interrupted:parent_restart" in ledger.terminal_calls[0][1]["message"]
    assert await runtime.recover_orphans() == []
    assert len(ledger.terminal_calls) == 1
    replay = await runtime.run_background(call("old-call", run_in_background=True))
    assert child in replay
    assert "interrupted:parent_restart" in replay
    assert len(ledger.opened) == 1
    assert made == []


@pytest.mark.asyncio
async def test_session_foreground_orphan_becomes_durable_partial_without_provider(
    tmp_path,
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    child = "cccccccc-1111-4222-8333-444444444444"
    ledger.live = [
        {
            "thread_id": child,
            "runtime_generation": "aaaaaaaa-1111-4222-8333-444444444444",
            "parent_thread_id": "parent-job",
            "handle": "explorer-dead",
            "subagent_type": "explorer",
            "parent_tool_call_id": "old-call",
            "run_in_background": False,
            "total_turns": 2,
            "total_tokens": 50,
        }
    ]
    ledger.messages.append((child, AIMessage(content="the latest durable finding"), 2))
    made: list[Any] = []
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: made.append(object()),
        ledger=ledger,
    )
    runtime.host.delivery_channel = "event"

    recovered = await runtime.recover_orphans()

    assert recovered == [
        {
            "handle": "explorer-dead",
            "thread_id": child,
            "status": "interrupted",
            "run_in_background": False,
            "delivery_id": runtime._delivery_id(
                child, "aaaaaaaa-1111-4222-8333-444444444444"
            ),
            "supersedes_input_seq": None,
        }
    ]
    assert made == []
    assert len(ledger.foreground_terminal_calls) == 1
    _, fields = ledger.foreground_terminal_calls[0]
    assert fields["outcome"] == "interrupted:parent_restart"
    assert "the latest durable finding" in fields["message"]
    assert "durable partial transcript" in fields["message"]
    assert runtime.host.events == [fields["message"]]
    assert await runtime.recover_orphans() == []
    assert len(ledger.foreground_terminal_calls) == 1


@pytest.mark.asyncio
async def test_terminal_foreground_gap_preserves_child_outcome(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    child = "dddddddd-1111-4222-8333-444444444444"
    ledger.live = [
        {
            "thread_id": child,
            "runtime_generation": "aaaaaaaa-1111-4222-8333-444444444444",
            "parent_thread_id": "parent-job",
            "handle": "reviewer-done",
            "subagent_type": "reviewer",
            "parent_tool_call_id": "completed-call",
            "run_in_background": False,
            "thread_status": "ended",
            "status": "completed",
            "outcome": "completed",
            "recovery_kind": "terminal_foreground",
            "total_turns": 3,
            "total_tokens": 75,
        }
    ]
    ledger.messages.append((child, AIMessage(content="final durable review"), 3))
    runtime = runtime_for(
        ctx, factory=lambda *_: pytest.fail("provider ran"), ledger=ledger
    )
    runtime.host.delivery_channel = "event"

    recovered = await runtime.recover_orphans()

    assert recovered[0]["status"] == "completed"
    _, fields = ledger.foreground_terminal_calls[0]
    assert fields["status"] == "completed"
    assert fields["outcome"] == "completed"
    assert "final durable review" in fields["message"]


@pytest.mark.asyncio
async def test_terminal_foreground_row_sends_the_child_status_not_the_thread_status(
    tmp_path,
):
    """The durable listing row (``_SUBAGENT_THREAD_COLUMNS``) carries the
    THREAD status ``ended`` under ``status`` and the child's own terminal
    status under ``subagent_status``. Recovery must send the latter: the
    orchestrator refuses a terminal retry whose status differs from the stored
    one, which is what looped dev thread ``ad7eb761`` on every attach."""

    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    child = "dddddddd-1111-4222-8333-555555555555"
    ledger.live = [
        {
            "id": child,
            "thread_id": child,
            "runtime_generation": "aaaaaaaa-1111-4222-8333-555555555555",
            "parent_thread_id": "parent-job",
            "handle": "reader-307f",
            "subagent_type": "reader",
            "parent_tool_call_id": "completed-call",
            "run_in_background": False,
            "status": "ended",
            "subagent_status": "completed",
            "subagent_outcome": "completed",
            "subagent_error": None,
            "report_path": ".subagents/reader-307f/report.md",
            "recovery_kind": "terminal_foreground",
            "total_turns": 8,
            "total_tokens": 61474,
        }
    ]
    ledger.messages.append((child, AIMessage(content="final durable report"), 8))
    runtime = runtime_for(
        ctx, factory=lambda *_: pytest.fail("provider ran"), ledger=ledger
    )
    runtime.host.delivery_channel = "event"

    recovered = await runtime.recover_orphans()

    assert recovered[0]["status"] == "completed"
    _, fields = ledger.foreground_terminal_calls[0]
    assert fields["status"] == "completed"
    assert fields["outcome"] == "completed"
    assert fields["report_path"] == ".subagents/reader-307f/report.md"
    assert fields["error"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("thread_status", "child_status", "recovery_kind", "sent_status"),
    [
        ("ended", "completed", "terminal_foreground", "completed"),
        ("active", "running", None, "interrupted"),
    ],
)
async def test_session_orphan_counters_survive_the_roster_payload_shape(
    tmp_path, thread_status, child_status, recovery_kind, sent_status
):
    """A session ledger lists the orchestrator's ROSTER PAYLOAD, not database
    rows: ``subagent_thread_payload`` publishes the counters as ``turns`` and
    ``tokens``. Recovery read ``total_turns``/``total_tokens`` and sent zeros,
    which the orchestrator refuses for an ended child ("terminal session child
    retry changed its turns") and writes over a live child's real counters.

    The row is built by the production projection so the test cannot drift
    from the wire shape."""

    from orchestrator.services.subagent_projection import subagent_thread_payload

    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    child = "eeeeeeee-1111-4222-8333-555555555555"
    row = subagent_thread_payload(
        {
            "id": child,
            "parent_job_id": None,
            "parent_thread_id": "parent-job",
            "runtime_generation": "aaaaaaaa-1111-4222-8333-555555555555",
            "subagent_handle": "reader-307f",
            "subagent_type": "reader",
            "status": thread_status,
            "subagent_status": child_status,
            "subagent_outcome": child_status if recovery_kind else None,
            "subagent_error": None,
            "total_turns": 8,
            "total_tokens": 61474,
            "report_path": ".subagents/reader-307f/report.md",
            "parent_tool_call_id": "counted-call",
            "metadata": {"subagent": {"run_in_background": False}},
            "recovery_kind": recovery_kind,
        }
    )
    assert "total_turns" not in row and "total_tokens" not in row
    ledger.live = [row]
    ledger.messages.append((child, AIMessage(content="durable finding"), 8))
    runtime = runtime_for(
        ctx, factory=lambda *_: pytest.fail("provider ran"), ledger=ledger
    )
    runtime.host.delivery_channel = "event"

    recovered = await runtime.recover_orphans()

    assert recovered[0]["status"] == sent_status
    _, fields = ledger.foreground_terminal_calls[0]
    assert fields["status"] == sent_status
    assert fields["turns"] == 8
    assert fields["tokens"] == 61474


@pytest.mark.asyncio
async def test_quiesce_adopts_inflight_durable_create_and_settles_without_provider(
    tmp_path,
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    ledger.allow_open.clear()
    made: list[Any] = []
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: made.append(
            FakeChatModel([text_turn("must not run")])
        ),
        ledger=ledger,
    )
    spawning = asyncio.create_task(runtime.run_background(call(run_in_background=True)))
    await ledger.open_started.wait()
    quiescing = asyncio.create_task(runtime.quiesce("rotation"))
    await asyncio.sleep(0)
    assert not quiescing.done()
    ledger.allow_open.set()
    result = await asyncio.wait_for(spawning, 5)
    await asyncio.wait_for(quiescing, 5)
    assert "fenced before provider start" in result
    assert made == []
    assert len(ledger.terminal_calls) == 1
    assert ledger.terminal_calls[0][1]["outcome"] == "interrupted:stopped"


@pytest.mark.asyncio
async def test_quiesce_awaits_terminal_delivery(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    fake = FakeChatModel([HANG])
    runtime = runtime_for(ctx, factory=lambda config, limits: fake, ledger=ledger)
    await runtime.run_background(call(run_in_background=True))
    await asyncio.wait_for(fake.hang_started.wait(), 5)
    await runtime.quiesce("rotation")
    assert ledger.terminal.is_set()
    assert runtime.active == {}
    assert "no new work accepted" in await runtime.run_background(
        call("later", run_in_background=True)
    )


@pytest.mark.asyncio
async def test_quiesce_fails_closed_when_terminal_delivery_cannot_commit(tmp_path):
    class RefusingTerminalLedger(StrictLedger):
        def __init__(self):
            super().__init__()
            self.attempted = asyncio.Event()

        async def terminalize_and_enqueue(self, subagent_id: str, **fields: Any):
            del subagent_id, fields
            self.attempted.set()
            raise RuntimeError("transport unavailable")

    ctx, _ = make_parent(tmp_path)
    ledger = RefusingTerminalLedger()
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: FakeChatModel([text_turn("evidence")]),
        ledger=ledger,
    )
    await runtime.run_background(call(run_in_background=True))
    await asyncio.wait_for(ledger.attempted.wait(), 5)
    with pytest.raises(RuntimeError, match="could not commit terminal delivery"):
        await runtime.quiesce("rotation")


@pytest.mark.asyncio
async def test_quiesce_joins_delivery_pending_task_before_any_retry(tmp_path):
    class GatedTerminalLedger(StrictLedger):
        def __init__(self):
            super().__init__()
            self.commit_started = asyncio.Event()
            self.allow_commit = asyncio.Event()
            self.commit_calls = 0

        async def terminalize_and_enqueue(self, subagent_id: str, **fields: Any):
            self.commit_calls += 1
            self.commit_started.set()
            await self.allow_commit.wait()
            return await super().terminalize_and_enqueue(subagent_id, **fields)

    ctx, _ = make_parent(tmp_path)
    ledger = GatedTerminalLedger()
    runtime = runtime_for(
        ctx,
        factory=lambda config, limits: FakeChatModel([text_turn("evidence")]),
        ledger=ledger,
    )
    await runtime.run_background(call(run_in_background=True))
    await asyncio.wait_for(ledger.commit_started.wait(), 5)
    quiescing = asyncio.create_task(runtime.quiesce("rotation"))
    await asyncio.sleep(0)
    assert not quiescing.done()
    assert ledger.commit_calls == 1
    ledger.allow_commit.set()
    await asyncio.wait_for(quiescing, 5)
    assert ledger.commit_calls == 1
    assert runtime._background_tasks == {}
