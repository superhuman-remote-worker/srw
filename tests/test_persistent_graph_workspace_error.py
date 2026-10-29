"""Chat turn loop must surface WorkspaceUnavailableError cleanly, not flatten it
into a retryable ToolMessage. knowledge-base/knowledge/issues/agent_fast_freeze_on_dead_workspace.md."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import tool

from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from agent.persistent_graph import (
    PersistentLoopCallbacks,
    _user_facing_turn_error,
    run_persistent_loop,
)


def test_user_facing_message_for_workspace_unavailable():
    msg = _user_facing_turn_error(WorkspaceUnavailableError("gone"))
    assert "workspace" in msg.lower()
    assert "gone" not in msg  # actionable copy, not the raw exception text


def test_user_facing_message_for_wrapped_workspace_unavailable():
    """The turn handler often sees an exception whose __cause__ is the WUE."""
    try:
        try:
            raise WorkspaceUnavailableError("gone")
        except WorkspaceUnavailableError as inner:
            raise RuntimeError("turn failed") from inner
    except RuntimeError as e:
        msg = _user_facing_turn_error(e)
    assert "workspace" in msg.lower()


def _make_callbacks(**overrides) -> PersistentLoopCallbacks:
    defaults = dict(
        get_user_input=AsyncMock(return_value="hello"),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=AsyncMock(),
        on_error=AsyncMock(),
        check_interrupt=MagicMock(return_value=False),
        on_vm_upgrade_needed=None,
    )
    defaults.update(overrides)
    return PersistentLoopCallbacks(**defaults)


def _tool_calling_llm():
    """Fake LLM whose single streamed response is one tool call to `boom`."""
    llm = AsyncMock()
    llm.reasoning = None
    response = AIMessage(
        content="", tool_calls=[{"name": "boom", "args": {}, "id": "c1"}]
    )

    async def _astream(messages, **kw):
        yield response

    llm.astream = _astream
    llm.ainvoke = AsyncMock(return_value=response)
    return llm


def _loop_config() -> MagicMock:
    cfg = MagicMock()
    cfg.llm.timeout = 600
    cfg.context_management.max_summary_length = 10000
    return cfg


@pytest.mark.asyncio
async def test_workspace_error_surfaces_via_on_error():
    """End-to-end: a tool raising WorkspaceUnavailableError mid-turn propagates
    out of _execute_turn to the turn handler → on_error (clean recovery message),
    instead of being flattened into a retryable ToolMessage."""

    @tool
    def boom() -> str:
        """dead workspace"""
        raise WorkspaceUnavailableError("workspace gone")

    on_error = AsyncMock()

    calls = 0

    async def _input():
        nonlocal calls
        calls += 1
        if calls == 1:
            return "do it"
        raise asyncio.CancelledError

    await run_persistent_loop(
        llm_with_tools=_tool_calling_llm(),
        tools=[boom],
        context_manager=AsyncMock(
            ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
        ),
        config=_loop_config(),
        system_prompt="sys",
        callbacks=_make_callbacks(get_user_input=_input, on_error=on_error),
        messages=[],
    )

    assert on_error.called, "turn error was not surfaced via on_error"
    surfaced = str(on_error.call_args.args[0]) if on_error.call_args.args else ""
    assert "workspace" in surfaced.lower()


@pytest.mark.asyncio
async def test_workspace_loss_leaves_no_call_without_a_result():
    """A call left without a result row is tool debt: the Session's next message
    would park until someone repairs the history
    (session_unknown_tool_result_needs_admin.md). The running call's result is
    unknown and says so; the call after it never started and says that."""

    @tool
    def boom() -> str:
        """dead workspace"""
        raise WorkspaceUnavailableError("workspace gone")

    @tool
    def later() -> str:
        """never reached"""
        raise AssertionError("must not run after the workspace was lost")

    llm = AsyncMock()
    llm.reasoning = None
    response = AIMessage(
        content="",
        tool_calls=[
            {"name": "boom", "args": {}, "id": "c1"},
            {"name": "later", "args": {}, "id": "c2"},
        ],
    )

    async def _astream(messages, **kw):
        yield response

    llm.astream = _astream
    llm.ainvoke = AsyncMock(return_value=response)

    persisted = []

    async def _persist(message):
        persisted.append(message)
        return True

    calls = 0

    async def _input():
        nonlocal calls
        calls += 1
        if calls == 1:
            return "do it"
        raise asyncio.CancelledError

    on_tool_result = AsyncMock()
    on_error = AsyncMock()
    callbacks = _make_callbacks(
        get_user_input=_input,
        on_error=on_error,
        on_tool_result=on_tool_result,
    )
    callbacks.persist_message = _persist
    messages = []
    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[boom, later],
        context_manager=AsyncMock(
            ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
        ),
        config=_loop_config(),
        system_prompt="sys",
        callbacks=callbacks,
        messages=messages,
    )

    results = {
        m.tool_call_id: m.content for m in persisted if isinstance(m, ToolMessage)
    }
    assert results["c1"].startswith("Result unknown:")
    assert "workspace gone" in results["c1"]
    assert results["c2"].startswith("Not run:")
    # The started call's card closes as failed; the unstarted one never opened.
    on_tool_result.assert_awaited_once()
    assert on_tool_result.await_args.args[:2] == ("boom", results["c1"])
    assert on_tool_result.await_args.kwargs == {"is_error": True}
    assert on_error.called
