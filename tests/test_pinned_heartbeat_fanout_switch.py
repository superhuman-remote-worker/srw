"""A running pinned session takes the fan-out switch from its heartbeat (P5).

parallel_subagents.md §14.1 (first item), §14.2 P5. A pinned runtime claims
its inputs straight from Postgres, so no orchestrator response accompanies a
turn; before P5 it learned the operator's switch only at attach. The
orchestrator now puts the pinned attach body's two keys on every heartbeat
response of a pinned session. The agent:

* holds the newest pair from each heartbeat response (persistent and dual
  heartbeat callbacks), and applies nothing there;
* applies the held pair once, at the next turn start, through
  ``apply_subagent_advertisement`` — before the turn reads its tool binding,
  so a value that arrives mid-turn reaches the next turn and the running turn
  keeps the binding and the gate it started with;
* treats an absent pair (an older orchestrator, a non-pinned agent) as no
  change, never as off; the stateless lane never takes a heartbeat value;
* drops a held value at a new attach, whose own advertisement is newer.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

import agent.api.persistent_app as pa
from agent.api.persistent_session import PersistentSession
from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop
from shared.session_subagent_batch import (
    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY,
    SESSION_SUBAGENT_FANOUT_KEY,
)
from tests.test_session_delegation_fanout_config import (
    _attach_until_construction,
    _lite_workspace,
)

pytestmark = pytest.mark.asyncio


def _beat(fanout: Any, settle: Any = 1, **extra: Any) -> dict[str, Any]:
    return {
        "status": "ok",
        "intents": {},
        SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY: settle,
        SESSION_SUBAGENT_FANOUT_KEY: fanout,
        **extra,
    }


class _Llm:
    """One bound model; scripted responses, records each provider call."""

    reasoning = None

    def __init__(self, name: str, responses: list[AIMessage], log: list):
        self.name = name
        self._responses = list(responses)
        self._log = log
        self.session: Any = None

    async def astream(self, _messages, **_kwargs):
        context = self.session.tool_context
        self._log.append((self.name, context._session_subagent_fanout))
        yield self._responses.pop(0)


class _Session:
    """A pinned session with the real advertisement method.

    ``apply_subagent_fanout_advertisement`` is ``PersistentSession``'s own:
    it publishes both flags on the tool context and, on a change, rebuilds
    the tool binding (here: the next scripted model).
    """

    apply_subagent_fanout_advertisement = (
        PersistentSession.apply_subagent_fanout_advertisement
    )

    def __init__(self, *, fanout: bool = True, rebuilt: list | None = None):
        self.subagent_batch_settle_contract = True
        self.subagent_fanout = fanout
        self.tool_context = SimpleNamespace(
            _session_subagent_batch_settle_contract=True,
            _session_subagent_fanout=fanout,
        )
        self.tools: list = []
        self.llm_with_tools: Any = object()
        self._rebuilt = list(rebuilt or [])
        self.rebuilds = 0

    def refresh_delegation_description(self) -> bool:
        self.rebuilds += 1
        if self._rebuilt:
            self.llm_with_tools = self._rebuilt.pop(0)
        return True


@pytest.fixture
def pinned(monkeypatch):
    """Persistent-app module state for one attached pinned session."""

    monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
    monkeypatch.setattr(pa._session_attach, "_heartbeat_subagent_advertisement", None)
    intents = AsyncMock()
    monkeypatch.setattr(pa._session_termination, "handle_heartbeat_intents", intents)
    session = _Session()
    monkeypatch.setattr(pa, "_session", session)
    return SimpleNamespace(session=session, intents=intents)


def _flags(session) -> tuple[bool, bool]:
    return (
        session.tool_context._session_subagent_batch_settle_contract,
        session.tool_context._session_subagent_fanout,
    )


async def test_a_heartbeat_value_is_held_until_the_next_turn_start(pinned, caplog):
    response = _beat(False)
    await pa._on_heartbeat_response(response)

    # The heartbeat applies nothing; the termination coordinator still gets
    # the response for its drain intent.
    pinned.intents.assert_awaited_once_with(response)
    assert _flags(pinned.session) == (True, True)
    assert pinned.session.rebuilds == 0

    with caplog.at_level(logging.INFO, logger="agent.api.persistent_app"):
        llm, tools = pa._loop_current_tools()
    assert _flags(pinned.session) == (True, False)
    assert pinned.session.rebuilds == 1
    assert (llm, tools) == (pinned.session.llm_with_tools, pinned.session.tools)
    reapplied = [r for r in caplog.records if "re-applied" in r.getMessage()]
    assert len(reapplied) == 1
    assert "fanout=False" in reapplied[0].getMessage()

    # Applied once: the next turn start has nothing to apply.
    assert pa._session_attach.apply_heartbeat_subagent_advertisement() is False
    pa._loop_current_tools()
    assert pinned.session.rebuilds == 1


async def test_turning_the_lane_back_on_also_arrives_at_a_turn_start(pinned):
    pinned.session.apply_subagent_fanout_advertisement(
        batch_settle_contract=True, fanout=False
    )
    await pa._on_heartbeat_response(_beat(True))
    assert _flags(pinned.session) == (True, False)
    pa._loop_current_tools()
    assert _flags(pinned.session) == (True, True)


@pytest.mark.parametrize(
    "response",
    [
        # An orchestrator that predates P5, or an agent with no pinned thread.
        {"status": "ok", "intents": {}},
        # Half a pair is not an advertisement.
        {"status": "ok", SESSION_SUBAGENT_FANOUT_KEY: False},
        {"status": "ok", SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY: 1},
        None,
    ],
)
async def test_an_absent_advertisement_changes_nothing(pinned, response):
    pa._session_attach.hold_heartbeat_subagent_advertisement(response)
    pa._loop_current_tools()
    assert _flags(pinned.session) == (True, True)
    assert pinned.session.rebuilds == 0


async def test_an_absent_pair_does_not_erase_a_held_one(pinned):
    await pa._on_heartbeat_response(_beat(False))
    await pa._on_heartbeat_response({"status": "ok", "intents": {}})
    pa._loop_current_tools()
    assert _flags(pinned.session) == (True, False)


async def test_the_newest_heartbeat_wins(pinned):
    await pa._on_heartbeat_response(_beat(False))
    await pa._on_heartbeat_response(_beat(True))
    pa._loop_current_tools()
    assert _flags(pinned.session) == (True, True)
    assert pinned.session.rebuilds == 0


async def test_the_values_are_judged_like_the_attach_body(pinned):
    """Only int 1 and a literal ``true`` count, as at attach."""
    await pa._on_heartbeat_response(_beat("true"))
    pa._loop_current_tools()
    assert _flags(pinned.session) == (True, False)
    await pa._on_heartbeat_response(_beat(True, settle=True))
    pa._loop_current_tools()
    assert _flags(pinned.session) == (False, True)


async def test_the_stateless_lane_never_takes_a_heartbeat_value(pinned, monkeypatch):
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")
    pa._session_attach.hold_heartbeat_subagent_advertisement(_beat(False))
    pa._loop_current_tools()
    assert _flags(pinned.session) == (True, True)


async def test_a_held_value_waits_for_a_session(pinned, monkeypatch):
    monkeypatch.setattr(pa, "_session", None)
    pa._session_attach.hold_heartbeat_subagent_advertisement(_beat(False))
    assert pa._session_attach.apply_heartbeat_subagent_advertisement() is False
    monkeypatch.setattr(pa, "_session", pinned.session)
    assert pa._session_attach.apply_heartbeat_subagent_advertisement() is True
    assert _flags(pinned.session) == (True, False)


async def test_a_failed_apply_keeps_the_turn_running(pinned, monkeypatch):
    def _boom(**_kwargs):
        raise RuntimeError("rebuild")

    monkeypatch.setattr(pinned.session, "apply_subagent_fanout_advertisement", _boom)
    await pa._on_heartbeat_response(_beat(False))
    llm, _tools = pa._loop_current_tools()
    assert llm is pinned.session.llm_with_tools


async def test_an_attach_drops_a_value_held_for_an_earlier_binding(pinned):
    pa._session_attach.hold_heartbeat_subagent_advertisement(_beat(False))
    seen = await _attach_until_construction(
        _lite_workspace(),
        session_subagent_batch_settle_contract=1,
        session_subagent_fanout=True,
    )
    assert seen["subagent_fanout"] is True
    assert pa._session_attach._heartbeat_subagent_advertisement is None


async def test_the_dual_heartbeat_callback_holds_the_value_too(pinned, monkeypatch):
    """Dual pods host pushed pinned sessions on persistent_app's state."""
    import agent.api.dual_app as dual_app

    monkeypatch.setattr(dual_app, "_check_job_preempted", MagicMock())
    monkeypatch.setattr(dual_app, "_update_guidance_inbox", MagicMock())
    await dual_app._handle_heartbeat_intents(_beat(False))
    assert _flags(pinned.session) == (True, True)
    pa._loop_current_tools()
    assert _flags(pinned.session) == (True, False)


async def test_the_runtime_wires_both_ends():
    """The loop reads its tools through ``_loop_current_tools``, and the
    persistent heartbeat loop reports to ``_on_heartbeat_response``."""
    import inspect

    captured: dict = {}

    def _fake_run(**kwargs):
        captured.update(kwargs)

        async def _noop():
            return None

        return _noop()

    session = MagicMock()
    session.postgres_conn = None
    with (
        patch.object(pa, "_session", session),
        patch.object(pa._session_identity, "_thread_id", "tid"),
        patch.object(pa, "_loop_task", None),
        patch.object(pa, "_session_ready", lambda: True),
        patch.object(pa, "run_persistent_loop", _fake_run),
    ):
        pa._ensure_persistent_loop_started("test")
        await asyncio.sleep(0)
    assert captured["get_current_tools"] is pa._loop_current_tools
    assert "on_response=_on_heartbeat_response" in inspect.getsource(pa)


# ---------------------------------------------------------------------------
# The loop: a switch that arrives mid-turn reaches the next turn only.
# ---------------------------------------------------------------------------


def _config() -> MagicMock:
    config = MagicMock()
    config.extra = {}
    config.llm.timeout = 600
    config.memory.enabled = False
    config.memory.observer_interval = 5
    config.context_management.max_summary_length = 10_000
    return config


def _context_manager() -> MagicMock:
    manager = MagicMock()
    manager.ensure_within_limits = AsyncMock(
        side_effect=lambda messages, *_args, **_kwargs: messages
    )
    return manager


def _callbacks(inputs: list[str]) -> PersistentLoopCallbacks:
    queue = iter(inputs)

    async def _input():
        try:
            return next(queue)
        except StopIteration:
            raise asyncio.CancelledError from None

    return PersistentLoopCallbacks(
        get_user_input=_input,
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=AsyncMock(),
        on_error=AsyncMock(),
        check_interrupt=MagicMock(return_value=False),
        persist_message=AsyncMock(),
        on_turn_settled=AsyncMock(),
    )


async def test_a_switch_that_arrives_mid_turn_reaches_the_next_turn_only(pinned):
    calls: list = []
    turn_one = _Llm(
        "boot",
        [
            AIMessage(
                content="",
                tool_calls=[{"name": "probe", "args": {}, "id": "call-1"}],
            ),
            AIMessage(content="turn one done"),
        ],
        calls,
    )
    turn_two = _Llm("rebuilt", [AIMessage(content="turn two done")], calls)
    session = _Session(rebuilt=[turn_two])
    session.llm_with_tools = turn_one
    turn_one.session = turn_two.session = session
    pa._session = session  # restored by the fixture's monkeypatch

    seen_in_tool: list = []

    async def _probe(*_args, **_kwargs):
        # The operator turns the lane off while this turn runs a tool.
        await pa._on_heartbeat_response(_beat(False))
        seen_in_tool.append(session.tool_context._session_subagent_fanout)
        return "probed"

    probe = MagicMock()
    probe.name = "probe"
    probe.args_schema = None
    probe.ainvoke = AsyncMock(side_effect=_probe)
    session.tools = [probe]

    await run_persistent_loop(
        llm_with_tools=turn_one,
        tools=[probe],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=_callbacks(["first", "second"]),
        messages=[],
        get_current_tools=pa._loop_current_tools,
    )

    # Turn one kept its binding and its gate after the heartbeat; turn two
    # started on the rebuilt binding with fan-out off.
    assert seen_in_tool == [True]
    assert calls == [("boot", True), ("boot", True), ("rebuilt", False)]
    assert session.rebuilds == 1
