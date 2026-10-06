"""Fail-closed persistence rules for a session-owned subagent runtime.

Worker-job delegation retains its historical best-effort ledger behavior.
Once a persistent session opens a child thread, however, an unavailable
idempotency read or lifecycle write must never be presented as a clean child
result.  These tests keep that distinction at the runtime boundary rather
than depending on the HTTP/database implementations beneath it.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage

import agent.subagents.runtime as runtime_mod
from agent.subagents import SessionHost, SubagentRuntime
from agent.subagents.fork import FORK_NOTICE
from tests._fake_chat_model import HANG, FakeChatModel, text_turn
from tests.test_subagent_runtime import call, make_parent


PARENT_THREAD_ID = "11111111-2222-4333-8444-555555555555"
RUNTIME_GENERATION = "aaaaaaaa-1111-4222-8333-444444444444"


@pytest.mark.asyncio
async def test_retirement_quiesce_keeps_exact_settlement_authority(tmp_path):
    """Closing new admission must not abandon already-admitted child writes."""

    ctx, _ = make_parent(tmp_path)
    admission = {"open": False}
    exact_effect = AsyncMock(return_value=True)
    exact_settlement = AsyncMock(return_value=True)
    host = SessionHost(
        thread_id=PARENT_THREAD_ID,
        agent_type="persistent",
        tool_context=ctx,
        admission_fn=lambda: admission["open"],
        effect_authority_fn=exact_effect,
        settlement_authority_fn=exact_settlement,
    )
    runtime = SubagentRuntime.from_context(
        ctx,
        host,
        ledger=StrictSessionLedger(),
        llm_factory=lambda config, limits: FakeChatModel([text_turn("unused")]),
    )

    await runtime.quiesce("retirement preflight")
    assert runtime._persistence_abandoned is False
    assert exact_settlement.await_count == 1
    assert exact_effect.await_count == 0

    admission["open"] = True
    await runtime.resume()
    assert runtime._accepting is True
    assert exact_effect.await_count == 1


@pytest.mark.asyncio
async def test_transient_settlement_probe_can_retry_without_abandoning(tmp_path):
    ctx, _ = make_parent(tmp_path)
    admission = {"open": False}
    exact_settlement = AsyncMock(side_effect=[RuntimeError("db unavailable"), True])
    host = SessionHost(
        thread_id=PARENT_THREAD_ID,
        agent_type="persistent",
        tool_context=ctx,
        admission_fn=lambda: admission["open"],
        effect_authority_fn=lambda: True,
        settlement_authority_fn=exact_settlement,
    )
    runtime = SubagentRuntime.from_context(
        ctx,
        host,
        ledger=StrictSessionLedger(),
        llm_factory=lambda config, limits: FakeChatModel([text_turn("unused")]),
    )

    with pytest.raises(RuntimeError, match="settlement authority"):
        await runtime.quiesce("retirement preflight")
    assert runtime._persistence_abandoned is False
    assert runtime._accepting is False

    await runtime.quiesce("retirement retry")
    admission["open"] = True
    await runtime.resume()
    assert runtime._accepting is True


class LookupFailure(RuntimeError):
    pass


class OpenFailure(RuntimeError):
    pass


class TranscriptFailure(RuntimeError):
    pass


class TerminalFailure(RuntimeError):
    pass


class StrictSessionLedger:
    """Small exact-receipt ledger with independently injectable failures."""

    def __init__(self) -> None:
        self.fail_lookup = False
        self.lookup_row: dict[str, Any] | None = None
        self.open_mode = "receipt"
        self.fail_transcript = False
        self.fail_terminal = False
        self.opened: list[tuple[str, dict[str, Any]]] = []
        self.messages: list[tuple[str, Any, int]] = []
        self.seeds: list[tuple[str, list[Any]]] = []
        self.updates: list[tuple[str, dict[str, Any]]] = []

    async def lookup(
        self, parent_thread_id: str, parent_tool_call_id: str
    ) -> dict[str, Any] | None:
        del parent_thread_id, parent_tool_call_id
        if self.fail_lookup:
            raise LookupFailure("durable replay lookup unavailable")
        return dict(self.lookup_row) if self.lookup_row is not None else None

    async def open(self, subagent_id: str, **fields: Any) -> dict[str, str] | None:
        self.opened.append((subagent_id, dict(fields)))
        if self.open_mode == "none":
            return None
        if self.open_mode == "error":
            raise OpenFailure("durable child create unavailable")
        return {
            "thread_id": subagent_id,
            "runtime_generation": RUNTIME_GENERATION,
        }

    async def persist_seed(self, subagent_id: str, messages: list[Any]) -> bool:
        self.seeds.append((subagent_id, list(messages)))
        return True

    async def persist_message(
        self, subagent_id: str, message: Any, turn_number: int
    ) -> None:
        self.messages.append((subagent_id, message, turn_number))
        if self.fail_transcript:
            raise TranscriptFailure("durable transcript unavailable")

    async def update(self, subagent_id: str, **fields: Any) -> None:
        self.updates.append((subagent_id, dict(fields)))
        if self.fail_terminal and str(fields.get("status") or "") not in {
            "queued",
            "running",
        }:
            raise TerminalFailure("durable terminal lifecycle unavailable")


def _runtime(
    ctx: Any,
    ledger: StrictSessionLedger,
    factory: Any,
) -> SubagentRuntime:
    host = SessionHost(
        thread_id=PARENT_THREAD_ID,
        agent_type="persistent",
        tool_context=ctx,
        admission_fn=lambda: True,
        effect_authority_fn=lambda: True,
    )
    runtime = SubagentRuntime.from_context(
        ctx,
        host,
        ledger=ledger,
        llm_factory=factory,
        driver_kwargs={
            "watcher_poll_interval": 0.01,
            "archiver": None,
            "archive_fn": lambda **kwargs: None,
        },
    )
    ctx._parent_host = host
    ctx.subagent_runtime = runtime
    return runtime


def _capture_builds(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    builds: list[Any] = []
    real_build_child = runtime_mod.build_child

    async def capture(*args: Any, **kwargs: Any) -> Any:
        build = await real_build_child(*args, **kwargs)
        builds.append(build)
        return build

    monkeypatch.setattr(runtime_mod, "build_child", capture)
    return builds


@pytest.mark.asyncio
async def test_lookup_failure_refuses_before_child_construction_or_provider(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictSessionLedger()
    ledger.fail_lookup = True
    models: list[FakeChatModel] = []
    runtime = _runtime(
        ctx,
        ledger,
        lambda config, limits: models.append(FakeChatModel([text_turn("never")])),
    )

    with pytest.raises(RuntimeError, match="idempotency lookup failed"):
        await runtime.run_foreground(call())

    assert models == []
    assert ledger.opened == []
    assert runtime.active == {}
    await runtime.close()


@pytest.mark.parametrize("status", ["queued", "running"])
@pytest.mark.asyncio
async def test_live_durable_call_refuses_foreground_before_construction_or_create(
    tmp_path, status: str
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictSessionLedger()
    ledger.lookup_row = {
        "id": "bbbbbbbb-1111-4222-8333-444444444444",
        "parent_thread_id": PARENT_THREAD_ID,
        "parent_tool_call_id": "c1",
        "subagent_status": status,
    }
    models: list[FakeChatModel] = []
    runtime = _runtime(
        ctx,
        ledger,
        lambda config, limits: models.append(FakeChatModel([text_turn("never")]))
        or models[-1],
    )

    with pytest.raises(RuntimeError, match="already has a live durable child"):
        await runtime.run_foreground(call())

    assert models == []
    assert ledger.opened == []
    await runtime.close()


@pytest.mark.asyncio
async def test_live_durable_call_refuses_background_before_create_or_provider(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictSessionLedger()
    ledger.lookup_row = {
        "id": "bbbbbbbb-1111-4222-8333-444444444444",
        "parent_thread_id": PARENT_THREAD_ID,
        "parent_tool_call_id": "c1",
        "subagent_status": "queued",
    }
    models: list[FakeChatModel] = []
    runtime = _runtime(
        ctx,
        ledger,
        lambda config, limits: models.append(FakeChatModel([text_turn("never")]))
        or models[-1],
    )

    with pytest.raises(RuntimeError, match="already has a live durable child"):
        await runtime.run_background(call(run_in_background=True))

    assert models == []
    assert ledger.opened == []
    await runtime.close()


@pytest.mark.asyncio
async def test_process_local_background_receipt_precedes_live_durable_refusal(tmp_path):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictSessionLedger()
    ledger.lookup_row = {
        "id": "bbbbbbbb-1111-4222-8333-444444444444",
        "parent_thread_id": PARENT_THREAD_ID,
        "parent_tool_call_id": "c1",
        "subagent_status": "running",
    }
    runtime = _runtime(
        ctx,
        ledger,
        lambda config, limits: FakeChatModel([text_turn("never")]),
    )
    key = (PARENT_THREAD_ID, "c1")
    runtime._background_keys[key] = "explorer-live"
    runtime._background["explorer-live"] = SimpleNamespace(
        receipt="same receipt", status="running", envelope=None
    )

    assert await runtime.run_background(call(run_in_background=True)) == "same receipt"
    assert ledger.opened == []
    runtime._background.clear()
    runtime._background_keys.clear()
    await runtime.close()


@pytest.mark.asyncio
async def test_background_cold_terminal_replay_never_creates_or_calls_provider(
    tmp_path,
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictSessionLedger()
    ledger.lookup_row = {
        "id": "bbbbbbbb-1111-4222-8333-444444444444",
        "parent_thread_id": PARENT_THREAD_ID,
        "parent_tool_call_id": "c1",
        "subagent_handle": "explorer-dead",
        "subagent_type": "explorer",
        "subagent_status": "completed",
        "subagent_outcome": "completed",
        "status": "ended",
        "total_turns": 2,
        "total_tokens": 50,
    }
    models: list[FakeChatModel] = []
    runtime = _runtime(
        ctx,
        ledger,
        lambda config, limits: models.append(FakeChatModel([text_turn("never")]))
        or models[-1],
    )

    replay = await runtime.run_background(call(run_in_background=True))

    assert "Replayed: this child already ran for tool call c1" in replay
    assert "no new child was spawned" in replay
    assert models == []
    assert ledger.opened == []
    assert runtime.records[(PARENT_THREAD_ID, "c1")].replayed is True
    await runtime.close()


@pytest.mark.parametrize("open_mode", ["none", "error"])
@pytest.mark.asyncio
async def test_open_failure_refuses_provider_and_releases_the_built_child(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    open_mode: str,
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictSessionLedger()
    ledger.open_mode = open_mode
    models: list[FakeChatModel] = []
    builds = _capture_builds(monkeypatch)
    runtime = _runtime(
        ctx,
        ledger,
        lambda config, limits: models.append(FakeChatModel([text_turn("never")]))
        or models[-1],
    )

    expected = OpenFailure if open_mode == "error" else RuntimeError
    with pytest.raises(expected):
        await runtime.run_foreground(call())

    assert len(models) == 1
    assert models[0].calls == []
    assert len(builds) == 1 and builds[0].released is True
    assert runtime.active == {}
    await runtime.close()


@pytest.mark.asyncio
async def test_transcript_failure_surfaces_as_error_not_a_clean_envelope(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictSessionLedger()
    ledger.fail_transcript = True
    builds = _capture_builds(monkeypatch)
    model = FakeChatModel([text_turn("must not become a clean result")])
    runtime = _runtime(ctx, ledger, lambda config, limits: model)

    result = await runtime.run_foreground(call())

    assert result.startswith("[subagent ")
    assert "· error ·" in result
    assert "TranscriptFailure: durable transcript unavailable" in result
    assert "must not become a clean result" not in result
    assert ledger.updates[-1][1]["status"] == "error"
    assert len(runtime.records) == 1
    record = next(iter(runtime.records.values()))
    assert record.status == "error"
    assert record.envelope == result
    assert len(builds) == 1 and builds[0].released is True
    await runtime.close()


@pytest.mark.asyncio
async def test_terminal_update_failure_propagates_instead_of_returning_envelope(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictSessionLedger()
    ledger.fail_terminal = True
    builds = _capture_builds(monkeypatch)
    runtime = _runtime(
        ctx,
        ledger,
        lambda config, limits: FakeChatModel([text_turn("finished evidence")]),
    )

    with pytest.raises(TerminalFailure, match="terminal lifecycle unavailable"):
        await runtime.run_foreground(call())

    assert ledger.updates[-1][1]["status"] == "completed"
    assert runtime.records == {}
    assert len(builds) == 1 and builds[0].released is True
    await runtime.close()


@pytest.mark.asyncio
async def test_session_fork_persists_a_reminted_parent_history_before_provider(
    tmp_path,
):
    ctx, _ = make_parent(tmp_path)
    parent_history = [
        HumanMessage(content="earlier question", id="parent-human"),
        AIMessage(content="earlier answer", id="parent-ai"),
    ]
    ctx._fork_source = parent_history
    ledger = StrictSessionLedger()
    model = FakeChatModel([text_turn("forked evidence")])
    runtime = _runtime(ctx, ledger, lambda config, limits: model)

    result = await runtime.run_foreground(call(fork=True))

    assert "forked evidence" in result
    assert ledger.opened[0][1]["fork"] is True
    assert ledger.opened[0][1]["parent_thread_id"] == PARENT_THREAD_ID
    assert ledger.opened[0][1]["parent_job_id"] is None
    assert len(ledger.seeds) == 1
    child_id, seed = ledger.seeds[0]
    assert child_id == ledger.opened[0][0]
    seed_contents = [message.content for message in seed]
    assert seed_contents.index("earlier question") < seed_contents.index(
        "earlier answer"
    )
    assert seed_contents[-1] == FORK_NOTICE
    assert all(message.id not in {"parent-human", "parent-ai"} for message in seed)
    assert model.calls, "the provider starts only after the seed receipt"
    await runtime.close()


@pytest.mark.asyncio
async def test_cancellation_propagates_after_its_terminal_write_commits(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictSessionLedger()
    builds = _capture_builds(monkeypatch)
    model = FakeChatModel([HANG])
    runtime = _runtime(ctx, ledger, lambda config, limits: model)
    running = asyncio.create_task(runtime.run_foreground(call()))
    await asyncio.wait_for(model.hang_started.wait(), 5)

    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running

    assert ledger.updates[-1][1]["status"] == "cancelled"
    assert len(builds) == 1 and builds[0].released is True
    await runtime.close()


@pytest.mark.asyncio
async def test_cancellation_surfaces_a_failed_strict_terminal_write(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    """An opened durable row must not be reported as cleanly cancelled.

    Cancellation is re-raised when the terminal write commits (the preceding
    test).  When that write itself fails, the persistence error is the safer
    outcome: parent teardown must see an unsettled durable child and fail
    closed, leaving the exact generation for recovery instead of treating the
    cancellation as a completed lifecycle transition.
    """

    ctx, _ = make_parent(tmp_path)
    ledger = StrictSessionLedger()
    ledger.fail_terminal = True
    builds = _capture_builds(monkeypatch)
    model = FakeChatModel([HANG])
    runtime = _runtime(ctx, ledger, lambda config, limits: model)
    running = asyncio.create_task(runtime.run_foreground(call()))
    await asyncio.wait_for(model.hang_started.wait(), 5)

    running.cancel()
    with pytest.raises(TerminalFailure, match="terminal lifecycle unavailable"):
        await running

    assert ledger.updates[-1][1]["status"] == "cancelled"
    assert len(builds) == 1 and builds[0].released is True
    await runtime.close()


@pytest.mark.asyncio
async def test_quiesce_retries_failed_foreground_terminal_receipt(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    ctx, _ = make_parent(tmp_path)
    ledger = StrictSessionLedger()
    ledger.fail_terminal = True
    _capture_builds(monkeypatch)
    runtime = _runtime(
        ctx,
        ledger,
        lambda config, limits: FakeChatModel([text_turn("finished")]),
    )

    with pytest.raises(TerminalFailure, match="terminal lifecycle unavailable"):
        await runtime.run_foreground(call())
    assert runtime._foreground_terminal_pending

    with pytest.raises(RuntimeError, match="foreground terminal state"):
        await runtime.quiesce("retirement preflight")
    assert runtime._foreground_terminal_pending

    ledger.fail_terminal = False
    await runtime.quiesce("retirement retry")
    assert runtime._foreground_terminal_pending == {}
    await runtime.resume()
    assert runtime._accepting is True


class _ListingSessionLedger(StrictSessionLedger):
    """Adds the empty durable live-list that orphan recovery needs."""

    async def list_live(self, parent_id: str) -> list[dict[str, Any]]:
        del parent_id
        return []


def _revoke_parent_authority(runtime: SubagentRuntime) -> None:
    """Public End revoked the parent's authority before the local watchdog."""
    runtime.host.settlement_authority_fn = AsyncMock(return_value=False)
    runtime.host.effect_authority_fn = AsyncMock(return_value=False)


@pytest.mark.asyncio
async def test_owner_end_quiesces_a_runtime_whose_child_settled(tmp_path):
    """A session that delegated once must still close after owner End.

    k3d 2026-10-06: an ended session that had run one foreground child reached
    the warm idle TTL detach; quiesce raised "cannot prove exact settlement
    authority" because the spent handle was still recorded, the termination
    never completed and the stateless pod stayed unready until deleted.
    """
    ctx, _ = make_parent(tmp_path)
    ledger = _ListingSessionLedger()
    runtime = _runtime(
        ctx,
        ledger,
        lambda config, limits: FakeChatModel([text_turn("finished evidence")]),
    )
    assert await runtime.recover_orphans() == []
    assert "finished evidence" in await runtime.run_foreground(call())
    writes = len(ledger.updates)
    _revoke_parent_authority(runtime)
    runtime._notify_changed = AsyncMock()

    await runtime.quiesce("owner End already authorized")

    assert runtime._accepting is False
    assert len(ledger.updates) == writes
    runtime._notify_changed.assert_not_awaited()
    with pytest.raises(RuntimeError, match="exact parent authority"):
        await runtime.resume()


@pytest.mark.asyncio
async def test_owner_end_quiesces_after_a_child_failed_before_its_row(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    ctx, _ = make_parent(tmp_path)
    ledger = _ListingSessionLedger()

    async def fail_build(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("environment unavailable")

    monkeypatch.setattr(runtime_mod, "build_child", fail_build)
    runtime = _runtime(
        ctx, ledger, lambda config, limits: FakeChatModel([text_turn("unused")])
    )
    assert await runtime.recover_orphans() == []
    assert "could not be started" in await runtime.run_foreground(call())
    assert ledger.opened == []
    _revoke_parent_authority(runtime)

    await runtime.quiesce("owner End already authorized")

    assert runtime._accepting is False


@pytest.mark.asyncio
async def test_owner_end_never_hides_an_uncommitted_terminal_receipt(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    ctx, _ = make_parent(tmp_path)
    ledger = _ListingSessionLedger()
    ledger.fail_terminal = True
    _capture_builds(monkeypatch)
    runtime = _runtime(
        ctx, ledger, lambda config, limits: FakeChatModel([text_turn("finished")])
    )
    assert await runtime.recover_orphans() == []
    with pytest.raises(TerminalFailure, match="terminal lifecycle unavailable"):
        await runtime.run_foreground(call())
    _revoke_parent_authority(runtime)

    with pytest.raises(RuntimeError, match="exact settlement authority"):
        await runtime.quiesce("owner End already authorized")
    assert runtime._foreground_terminal_pending
