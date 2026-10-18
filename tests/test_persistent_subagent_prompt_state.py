"""Persistent-session visibility and loop-guard semantics for U5 children."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from agent.core.context_injection import format_active_subagents_none
from shared.runtime.core.context_entries import (
    FOLDED_KEY,
    entry_body,
    entry_kind,
    is_context_entry,
)
from shared.runtime.core.message_markers import PERSIST_ROLE_CONTEXT, PERSIST_ROLE_KEY
from agent.persistent_graph import (
    PersistentLoopCallbacks,
    TurnResult,
    _execute_turn,
    run_persistent_loop,
)


def _callbacks(**overrides) -> PersistentLoopCallbacks:
    values = {
        "get_user_input": AsyncMock(),
        "on_token": AsyncMock(),
        "on_thinking": AsyncMock(),
        "on_tool_start": AsyncMock(),
        "on_tool_result": AsyncMock(),
        "permission_check": AsyncMock(return_value=True),
        "on_turn_start": AsyncMock(),
        "on_turn_complete": AsyncMock(),
        "on_error": AsyncMock(),
        "check_interrupt": MagicMock(return_value=False),
        "persist_message": AsyncMock(),
    }
    values.update(overrides)
    return PersistentLoopCallbacks(**values)


def _config(*, officer: bool = False, max_actions: int = 100) -> MagicMock:
    config = MagicMock()
    config.extra = {}
    config.llm.timeout = 600
    config.llm.model = "test-model"
    config.memory.enabled = False
    config.memory.observer_interval = 5
    config.memory.query = None
    config.context_management.max_summary_length = 10_000
    config.officer.enabled = officer
    config.officer.conference = False
    config.officer.max_actions_per_wake = max_actions
    return config


def _context_manager() -> AsyncMock:
    manager = AsyncMock()
    manager.should_summarize = MagicMock(return_value=False)
    manager.ensure_within_limits = AsyncMock(
        side_effect=lambda messages, *_args, **_kwargs: messages
    )
    return manager


def _tool(name: str, result: str = "ok") -> MagicMock:
    tool = MagicMock()
    tool.name = name
    tool.args_schema = None
    tool.ainvoke = AsyncMock(return_value=result)
    return tool


def _tool_call(name: str, call_id: str) -> dict:
    return {"name": name, "args": {}, "id": call_id}


def _streaming_llm(responses: list[AIMessage], captured: list[list]) -> MagicMock:
    response_iter = iter(responses)

    async def _astream(provider_input, **_kwargs):
        captured.append(list(provider_input))
        yield next(response_iter)

    llm = MagicMock()
    llm.reasoning = None
    llm.astream = _astream
    return llm


@pytest.mark.asyncio
async def test_persistent_loop_stamps_parent_turn_before_execution(monkeypatch):
    tool_context = SimpleNamespace()
    observed_turns: list[int] = []

    async def _execute_stub(**kwargs):
        observed_turns.append(kwargs["tool_context"]._current_turn_count)
        return TurnResult(turn_id=0, messages_added=0, tool_calls_made=0)

    monkeypatch.setattr("agent.persistent_graph._execute_turn", _execute_stub)
    callbacks = _callbacks(
        get_user_input=AsyncMock(
            side_effect=["delegate this", asyncio.CancelledError()]
        )
    )

    await run_persistent_loop(
        llm_with_tools=MagicMock(),
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=callbacks,
        messages=[],
        tool_context=tool_context,
        initial_turn_count=6,
    )

    assert observed_turns == [7]
    assert tool_context._current_turn_count == 7


def _persisted(callbacks: PersistentLoopCallbacks) -> list:
    return [call.args[0] for call in callbacks.persist_message.await_args_list]


@pytest.mark.asyncio
async def test_active_child_state_precedes_the_final_product_boundary():
    from shared.runtime.core.skill_resolution import (
        APP_GUIDE_LOADER_TOOL,
        add_persistent_system_skills,
    )

    active_block = "<active_subagents>one child</active_subagents>"
    tool_context = SimpleNamespace(
        knowledge_bindings=[],
        citation_engine=None,
        subagent_runtime=SimpleNamespace(active_subagents_block=lambda: active_block),
    )
    config = _config()
    config.extra = {"_resolved_skills": add_persistent_system_skills({})}
    captured: list[list] = []
    callbacks = _callbacks()
    durable_messages = [SystemMessage(content="sys"), HumanMessage(content="question")]

    await _execute_turn(
        llm_with_tools=_streaming_llm([AIMessage(content="done")], captured),
        tool_map={APP_GUIDE_LOADER_TOOL: _tool(APP_GUIDE_LOADER_TOOL)},
        context_manager=_context_manager(),
        messages=durable_messages,
        callbacks=callbacks,
        llm_timeout=600,
        auxiliary_llm=None,
        config=config,
        tool_context=tool_context,
        turn_id=5,
    )

    entries = [m for m in durable_messages if is_context_entry(m)]
    assert [entry_kind(m) for m in entries] == ["subagents", "turn_boundary"]
    assert entry_body(entries[0]) == active_block
    assert "<managed_product_guide_turn_boundary" in entry_body(entries[1])
    assert all(
        m.additional_kwargs[PERSIST_ROLE_KEY] == PERSIST_ROLE_CONTEXT for m in entries
    )
    assert _persisted(callbacks)[:2] == entries
    # The question carries the child state, then the boundary as the last text.
    carrier = captured[0][-1]
    assert isinstance(carrier, HumanMessage)
    assert carrier.additional_kwargs[FOLDED_KEY] == ["subagents", "turn_boundary"]
    assert carrier.content.index(active_block) < carrier.content.index(
        "<managed_product_guide_turn_boundary"
    )
    assert carrier.content.endswith(str(entries[1].content))


@pytest.mark.asyncio
async def test_active_child_state_is_read_per_call_and_appended_when_it_changes():
    """The status is read before every provider call and appended as a
    ``subagents`` entry only when it changed; each entry is durable history
    (persisted as a context row) and the next request starts with the
    previous one. Design change from the per-call tail, which rebuilt the
    block for every call and never persisted it."""
    active_block = "<active_subagents>reviewer is running</active_subagents>"
    runtime = MagicMock()
    runtime.active_subagents_block = MagicMock(side_effect=[active_block, ""])
    tool_context = SimpleNamespace(
        knowledge_bindings=[],
        citation_engine=None,
        subagent_runtime=runtime,
    )
    captured: list[list] = []
    llm = _streaming_llm(
        [
            AIMessage(content="", tool_calls=[_tool_call("noop", "call-1")]),
            AIMessage(content="done"),
        ],
        captured,
    )
    callbacks = _callbacks()
    durable_messages = [
        SystemMessage(content="sys"),
        HumanMessage(content="continue"),
    ]

    await _execute_turn(
        llm_with_tools=llm,
        tool_map={"noop": _tool("noop")},
        context_manager=_context_manager(),
        messages=durable_messages,
        callbacks=callbacks,
        llm_timeout=600,
        auxiliary_llm=None,
        config=_config(),
        tool_context=tool_context,
    )

    assert runtime.active_subagents_block.call_count == 2
    cleared = format_active_subagents_none("test-model")
    entries = [m for m in durable_messages if is_context_entry(m)]
    assert [entry_kind(m) for m in entries] == ["subagents", "subagents"]
    assert [entry_body(m) for m in entries] == [active_block, cleared.strip()]
    persisted_entries = [m for m in _persisted(callbacks) if is_context_entry(m)]
    assert persisted_entries == entries
    assert all(
        m.additional_kwargs[PERSIST_ROLE_KEY] == PERSIST_ROLE_CONTEXT for m in entries
    )

    # Call 1: the running child rides the user's message.
    assert len(captured) == 2
    first_carrier = captured[0][-1]
    assert first_carrier.additional_kwargs[FOLDED_KEY] == ["subagents"]
    assert active_block in first_carrier.content
    # Call 2 starts with request 1 unchanged; the cleared status rides the
    # tool result, the old status is never re-sent.
    assert captured[1][: len(captured[0])] == captured[0]
    second_carrier = captured[1][-1]
    assert second_carrier.type == "tool"
    assert second_carrier.additional_kwargs[FOLDED_KEY] == ["subagents"]
    assert second_carrier.content.endswith(str(entries[1].content))
    assert active_block not in second_carrier.content


@pytest.mark.asyncio
async def test_empty_retry_archives_each_response_with_its_provider_input():
    """An empty-response retry is archived and billed as its own attempt.

    The retry re-sends the request the planner built for this provider call:
    the status is read once, before the first attempt (there is no per-call
    tail to rebuild for the retry).
    """
    first_block = "<active_subagents>reviewer is running</active_subagents>"
    retry_block = "<active_subagents>reviewer completed</active_subagents>"
    runtime = MagicMock()
    runtime.active_subagents_block = MagicMock(side_effect=[first_block, retry_block])
    tool_context = SimpleNamespace(
        knowledge_bindings=[],
        citation_engine=None,
        subagent_runtime=runtime,
    )
    streamed = AIMessage(
        content="",
        response_metadata={
            "model_name": "empty-attempt",
            "token_usage": {"input_tokens": 11, "output_tokens": 0},
        },
    )
    retried = AIMessage(
        content="recovered",
        response_metadata={
            "model_name": "retry-attempt",
            "token_usage": {"input_tokens": 13, "output_tokens": 2},
        },
    )
    provider_inputs: list[list] = []

    async def _astream(provider_input, **_kwargs):
        provider_inputs.append(list(provider_input))
        yield streamed

    async def _ainvoke(provider_input, **_kwargs):
        provider_inputs.append(list(provider_input))
        return retried

    llm = MagicMock()
    llm.reasoning = True
    llm.astream = _astream
    llm.ainvoke = AsyncMock(side_effect=_ainvoke)
    archive = MagicMock()
    usage = AsyncMock()
    callbacks = _callbacks(archive_llm_call=archive, on_usage=usage)
    context_manager = _context_manager()
    context_manager.record_provider_usage = MagicMock()

    result = await _execute_turn(
        llm_with_tools=llm,
        tool_map={},
        context_manager=context_manager,
        messages=[SystemMessage(content="sys"), HumanMessage(content="continue")],
        callbacks=callbacks,
        llm_timeout=600,
        auxiliary_llm=None,
        config=_config(),
        tool_context=tool_context,
    )

    assert runtime.active_subagents_block.call_count == 1
    assert len(provider_inputs) == 2
    assert archive.call_count == 2
    first_input, first_response, first_metrics = archive.call_args_list[0].args
    retry_input, retry_response, retry_metrics = archive.call_args_list[1].args
    assert first_response is streamed
    assert retry_response is retried
    assert first_input == provider_inputs[0]
    assert retry_input == provider_inputs[1]
    assert first_metrics["model"] == "empty-attempt"
    assert retry_metrics["model"] == "retry-attempt"
    assert [
        call.args[0] for call in context_manager.record_provider_usage.call_args_list
    ] == [11, 13]
    assert [call.args[0]["model"] for call in usage.await_args_list] == [
        "empty-attempt",
        "retry-attempt",
    ]
    assert result.metrics["model"] == "retry-attempt"
    assert retry_input == first_input
    assert first_input[-1].additional_kwargs[FOLDED_KEY] == ["subagents"]
    assert first_block in first_input[-1].content
    assert all(retry_block not in str(message.content) for message in retry_input)


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ["list_agents", "wait_agent"])
async def test_observation_controls_do_not_trip_repeated_action_guard(tool_name):
    captured: list[list] = []
    repeated_calls = [
        AIMessage(content="", tool_calls=[_tool_call(tool_name, f"call-{index}")])
        for index in range(5)
    ]
    llm = _streaming_llm([*repeated_calls, AIMessage(content="done")], captured)
    callbacks = _callbacks()
    durable_messages = [SystemMessage(content="sys"), HumanMessage(content="watch")]

    result = await _execute_turn(
        llm_with_tools=llm,
        tool_map={tool_name: _tool(tool_name)},
        context_manager=_context_manager(),
        messages=durable_messages,
        callbacks=callbacks,
        llm_timeout=600,
        auxiliary_llm=None,
        config=_config(officer=True),
    )

    # Five identical ordinary actions force-end an officer wake. These
    # observation controls instead reach the sixth provider call normally,
    # while remaining counted in the overall action total.
    assert len(captured) == 6
    assert result.tool_calls_made == 5
    assert not any(
        isinstance(message, HumanMessage)
        and str(message.content).startswith("[guard] Wake force-ended")
        for message in durable_messages
    )
