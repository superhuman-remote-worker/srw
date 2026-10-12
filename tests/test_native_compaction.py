"""Native compaction: the working model writes its own summary (compaction WP5/WP6).

The conversation keeps its last main request as sent; a native compaction
sends that request unchanged, the messages added since and the recipe's
instruction through the same client. Anything that keeps it from running
falls back to the auxiliary fold, with the reason on the fold's events.
See knowledge-base/knowledge/features/compaction_refactor_fidelity_and_fork_strategy.md
(§6 WP5, WP6, design pass decisions 2026-10-07).
"""

import asyncio
from types import SimpleNamespace
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    RemoveMessage,
    SystemMessage,
    ToolMessage,
)

from agent.core.context import ContextConfig, ContextManager, summary_text
from agent.core.native_compaction import (
    DEFAULT_OUTPUT_RESERVE,
    LastRequest,
    build_fork,
    compaction_settings,
    messages_since,
    output_cap,
    recipe_instruction,
    summary_from_reply,
)
from shared.runtime.core.context_entries import make_context_entry
from shared.runtime.core.message_markers import is_compaction_summary
from shared.runtime.services.auxiliary import AuxiliaryLLM

NATIVE = {"strategy": "native", "recipe": "codex"}
SYSTEM = SystemMessage(content="You are the agent.", id="sys")
NATIVE_SUMMARY = "Progress: the exporter is written.\nNext: run the tests."
FOLD_SUMMARY = "## Objective\n- Ship the CSV export.\n\n## Work State\n- Tests pass."
CODEX_HEAD = "You are performing a CONTEXT CHECKPOINT COMPACTION."


class ForkLLM:
    """The main client: answers the fork, records every request."""

    def __init__(
        self,
        reply: Optional[AIMessage] = None,
        error: Optional[BaseException] = None,
        max_tokens: Optional[int] = 4096,
    ) -> None:
        self.reply = reply or AIMessage(
            content=NATIVE_SUMMARY, response_metadata={"finish_reason": "stop"}
        )
        self.error = error
        self.max_tokens = max_tokens
        self.model_name = "gpt-6-sol"
        self.calls: List[List[BaseMessage]] = []

    async def ainvoke(self, messages, **_kwargs):
        self.calls.append(list(messages))
        if self.error is not None:
            raise self.error
        return self.reply


def _aux(archiver: Any = None) -> AuxiliaryLLM:
    llm = MagicMock()
    llm.ainvoke = AsyncMock(
        return_value=AIMessage(
            content=FOLD_SUMMARY, response_metadata={"finish_reason": "stop"}
        )
    )
    return AuxiliaryLLM(
        llm=llm,
        max_context_tokens=50_000,
        archiver=archiver,
        job_id="job-1" if archiver is not None else None,
        agent_type="worker",
    )


def _manager(compaction: Optional[dict] = NATIVE, window: int = 200_000):
    manager = ContextManager(
        config=ContextConfig(
            compaction_threshold_tokens=1000,
            summarization_threshold_tokens=1000,
            message_count_threshold=10,
            message_count_min_tokens=500,
            keep_recent_messages=3,
            model_max_context_tokens=window,
            compaction=compaction,
        ),
        model="gpt-4",
    )
    events: List[tuple] = []

    async def _cb(event: str, params: dict) -> None:
        events.append((event, params))

    manager.set_progress_callback(_cb)
    return manager, events


def _history(n: int = 12) -> List[BaseMessage]:
    return [
        HumanMessage(content=f"user {i} " + "x" * 400, id=f"h{i}")
        if i % 2 == 0
        else AIMessage(content=f"assistant {i} " + "y" * 400, id=f"a{i}")
        for i in range(n)
    ]


def _sent(history: List[BaseMessage]) -> List[BaseMessage]:
    return [SYSTEM, *history]


def _kept(result: list) -> list:
    return [m for m in result if not isinstance(m, RemoveMessage)]


def _started(events: list) -> List[dict]:
    return [p for e, p in events if e == "compaction.started"]


class TestSettings:
    def test_native_needs_the_strategy(self):
        assert compaction_settings(NATIVE).native
        assert compaction_settings(NATIVE).recipe == "codex"
        for raw in (None, {}, {"strategy": "auxiliary"}, "native", {"recipe": "codex"}):
            assert not compaction_settings(raw).native

    def test_codex_recipe_is_codex_prompt_plus_the_no_tools_line(self):
        text = recipe_instruction("codex")
        assert text.startswith(CODEX_HEAD)
        assert text.endswith("Do not call any tools. Reply with the summary only.")

    def test_claude_recipe_is_anthropics_client_side_instruction(self):
        text = recipe_instruction("claude")
        assert text.startswith(
            "Summarize the transcript inside <summary></summary> tags."
        )
        assert "(6) specific details that would be hard to reconstruct" in text
        assert text.endswith("Do not call any tools.")

    def test_unknown_recipe_names_never_become_paths(self):
        for name in ("gemini", "../codex", None, ""):
            assert recipe_instruction(name) is None

    def test_family_matrix_routes_compaction_to_llm(self):
        from shared.runtime.core.loader import _apply_settings_matrix

        for model, expected in (
            ("gpt-6-sol", {"strategy": "native", "recipe": "codex"}),
            ("gpt-6.1-sol", {"strategy": "native", "recipe": "codex"}),
            ("gpt-5-mini", {"strategy": "native", "recipe": "codex"}),
            ("claude-opus-5-5", {"strategy": "native", "recipe": "claude"}),
            ("claude-sonnet-5-5", {"strategy": "native", "recipe": "claude"}),
            ("claude-fable-5-1", {"strategy": "native", "recipe": "claude"}),
            ("claude-haiku-4-5-20251001", {"strategy": "native", "recipe": "claude"}),
            ("muse-spark-1.3-contributor", {"strategy": "auxiliary"}),
            ("MiniMax-M3", {"strategy": "auxiliary"}),
        ):
            data = {"llm": {"model": model}}
            _apply_settings_matrix(data, set())
            assert data["llm"]["compaction"] == expected, model

    def test_an_expert_setting_wins_over_the_family(self):
        from shared.runtime.core.loader import _apply_settings_matrix

        data = {"llm": {"model": "gpt-6-sol", "compaction": {"strategy": "auxiliary"}}}
        _apply_settings_matrix(data, {"compaction"})
        assert data["llm"]["compaction"] == {"strategy": "auxiliary"}

    def test_llm_config_carries_it_through_an_override(self):
        from shared.runtime.core.loader import (
            LLMConfig,
            PhaseLLMOverride,
            _parse_llm_config,
        )

        config = _parse_llm_config({"model": "gpt-6-sol", "compaction": NATIVE})
        assert isinstance(config, LLMConfig) and config.compaction == NATIVE
        assert config.with_override(PhaseLLMOverride(model="x")).compaction == NATIVE


class TestHelpers:
    def test_output_cap_reads_through_bindings(self):
        assert output_cap(SimpleNamespace(max_tokens=8192)) == 8192
        bound = SimpleNamespace(
            kwargs={"tools": []}, bound=SimpleNamespace(max_tokens=512)
        )
        assert output_cap(bound) == 512
        assert output_cap(SimpleNamespace(kwargs={"max_tokens": 1000})) == 1000
        assert output_cap(MagicMock()) is None
        assert output_cap(SimpleNamespace(max_tokens=True)) is None

    def test_messages_since_is_the_suffix(self):
        history = _history(4)
        last = LastRequest(messages=_sent(history), llm=None, history=_sent(history))
        reply = AIMessage(content="r", id="r1")
        added, reason = messages_since(last, [*history, reply])
        assert reason is None and added == [reply]

    def test_a_rewritten_history_is_not_a_suffix(self):
        history = _history(4)
        last = LastRequest(messages=_sent(history), llm=None, history=_sent(history))
        inserted = HumanMessage(content="inserted", id="new")
        changed = [history[0], inserted, *history[1:], AIMessage(content="r")]
        assert messages_since(last, changed) == (None, "history_changed")
        assert messages_since(last, [HumanMessage(content="unrelated")]) == (
            None,
            "history_changed",
        )

    def test_a_call_waiting_for_its_result_cannot_be_followed(self):
        history = _history(4)
        last = LastRequest(messages=_sent(history), llm=None, history=_sent(history))
        call = AIMessage(
            content="", tool_calls=[{"name": "t", "args": {}, "id": "c1"}], id="c"
        )
        assert messages_since(last, [*history, call]) == (None, "pending_tool_calls")
        result = ToolMessage(content="ok", tool_call_id="c1")
        assert messages_since(last, [*history, call, result]) == ([call, result], None)

    def test_entries_the_request_carried_are_not_sent_twice(self):
        """append-only: an entry folded into the request went to the state."""
        history = _history(4)
        entry = make_context_entry("memory", "[m:1] fact", section="memory")
        unfolded = [SYSTEM, *history, entry]
        last = LastRequest(
            messages=[SYSTEM, *history[:-1], HumanMessage(content="folded")],
            llm=None,
            history=unfolded,
        )
        reply = AIMessage(content="r", id="r1")
        added, reason = messages_since(last, [*history, entry, reply])
        assert reason is None and added == [reply]

    def test_the_fork_keeps_the_previous_request_object_for_object(self):
        history = _history(4)
        sent = _sent(history)
        last = LastRequest(messages=sent, llm=None, history=sent)
        reply = AIMessage(content="r")
        fork = build_fork(last, [reply], "INSTRUCTION")
        assert all(a is b for a, b in zip(fork, sent))
        assert fork[len(sent)] is reply
        assert isinstance(fork[-1], HumanMessage) and fork[-1].content == "INSTRUCTION"

    def test_a_new_entry_without_carrier_stands_alone(self):
        history = _history(4)
        last = LastRequest(messages=_sent(history), llm=None, history=_sent(history))
        entry = make_context_entry("memory", "[m:2] new fact", section="memory")
        fork = build_fork(last, [entry], "INSTRUCTION")
        assert fork[: len(last.messages)] == last.messages
        assert isinstance(fork[-2], HumanMessage) and "new fact" in fork[-2].content

    @pytest.mark.parametrize(
        "reply,reason",
        [
            (
                AIMessage(
                    content="", tool_calls=[{"name": "t", "args": {}, "id": "1"}]
                ),
                "tool_call",
            ),
            (AIMessage(content="", additional_kwargs={"refusal": "no"}), "refusal"),
            (
                AIMessage(
                    content="", response_metadata={"finish_reason": "content_filter"}
                ),
                "refusal",
            ),
            (AIMessage(content="   "), "empty"),
            (
                AIMessage(
                    content="half", response_metadata={"finish_reason": "length"}
                ),
                "truncated",
            ),
        ],
    )
    def test_unusable_replies(self, reply, reason):
        assert summary_from_reply(reply) == (None, reason)

    def test_claude_summary_is_the_last_summary_block(self):
        reply = AIMessage(
            content=[
                {"type": "thinking", "thinking": "plan"},
                {
                    "type": "text",
                    "text": "<analysis>notes</analysis>\n<summary>draft</summary>\n"
                    "<summary>\n## State\n- done\n</summary>",
                },
            ]
        )
        assert summary_from_reply(reply, "claude") == ("## State\n- done", None)

    @pytest.mark.parametrize(
        "text", ["Here is where we are: done.", "<summary>  </summary>"]
    )
    def test_a_claude_reply_without_a_summary_block_is_rejected(self, text):
        assert summary_from_reply(AIMessage(content=text), "claude") == (
            None,
            "no_summary",
        )
        assert summary_from_reply(AIMessage(content=text), "codex")[1] in (
            None,
            "empty",
        )

    def test_reasoning_blocks_are_not_the_summary(self):
        reply = AIMessage(
            content=[
                {"type": "reasoning", "text": "thinking"},
                {"type": "text", "text": NATIVE_SUMMARY},
            ]
        )
        assert summary_from_reply(reply) == (NATIVE_SUMMARY, None)


class TestContextManagerNative:
    @pytest.mark.asyncio
    async def test_the_model_summarizes_its_own_request(self):
        manager, events = _manager()
        history = _history()
        sent = _sent(history)
        llm = ForkLLM()
        manager.record_main_request(
            sent, llm, history=sent, input_tokens=5000, timeout=30
        )
        reply = AIMessage(content="assistant reply", id="r1")
        aux = _aux(archiver=MagicMock())

        result = await manager.summarize_and_compact(
            [*history, reply], aux, allow_native=True
        )

        fork = llm.calls[0]
        assert all(a is b for a, b in zip(fork, sent)) and len(fork) == len(sent) + 2
        assert fork[len(sent)] is reply
        assert fork[-1].content.startswith(CODEX_HEAD)
        aux.llm.ainvoke.assert_not_awaited()
        summary = next(m for m in _kept(result) if is_compaction_summary(m))
        assert isinstance(summary, HumanMessage)
        assert summary_text(summary).startswith(NATIVE_SUMMARY)
        assert _started(events) == [
            {
                "trigger": "auto",
                "strategy": "native",
                "recipe": "codex",
                "total_tokens": _started(events)[0]["total_tokens"],
                "ctx_used_tokens": _started(events)[0]["total_tokens"],
                "ctx_limit_tokens": 200_000,
                "ctx_used_pct": _started(events)[0]["ctx_used_pct"],
                "n_passes": 1,
            }
        ]
        assert _started(events)[0]["total_tokens"] > 5000
        assert manager._last_summarization_stats["strategy"] == "native"
        assert manager.compaction_runs == 1
        assert manager._last_request is None, "the kept request is stale now"
        archived = aux._archiver.archive.call_args.kwargs
        assert archived["call_type"] == "summarization"
        assert archived["messages"] == fork
        assert archived["model"] == "gpt-6-sol"
        assert archived["auxiliary_metadata"] == {
            "task_class": "SummarizeTask",
            "strategy": "native",
            "recipe": "codex",
        }

    @pytest.mark.asyncio
    async def test_a_manual_focus_goes_into_the_instruction(self):
        manager, _ = _manager()
        history = _history()
        llm = ForkLLM()
        manager.record_main_request(_sent(history), llm, history=_sent(history))
        await manager.summarize_and_compact(
            history, _aux(), allow_native=True, trigger="manual", focus="the tests"
        )
        assert llm.calls[0][-1].content.endswith(
            "The user asked this summary to focus on: the tests\n"
            "Give that topic the most detail."
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "setup,reason",
        [
            ("unrecorded", "no_last_request"),
            ("huge", "does_not_fit"),
            ("tool_call", "tool_call"),
            ("truncated", "truncated"),
            ("error", "provider_error"),
            ("timeout", "timeout"),
            ("inserted", "history_changed"),
        ],
    )
    async def test_fallbacks_fold_and_say_why(self, setup, reason):
        manager, events = _manager()
        history = _history()
        reply = None
        error = None
        if setup == "tool_call":
            reply = AIMessage(
                content="", tool_calls=[{"name": "t", "args": {}, "id": "1"}]
            )
        elif setup == "truncated":
            reply = AIMessage(
                content="cut", response_metadata={"finish_reason": "length"}
            )
        elif setup == "error":
            error = RuntimeError("upstream 500")
        elif setup == "timeout":
            error = asyncio.TimeoutError()
        llm = ForkLLM(reply=reply, error=error)
        if setup != "unrecorded":
            manager.record_main_request(
                _sent(history),
                llm,
                history=_sent(history),
                input_tokens=199_000 if setup == "huge" else 5000,
            )
        state = list(history)
        if setup == "inserted":
            state.insert(3, HumanMessage(content="rewritten", id="x"))
        aux = _aux(archiver=MagicMock())

        result = await manager.summarize_and_compact(state, aux, allow_native=True)

        aux.llm.ainvoke.assert_awaited()
        summary = next(m for m in _kept(result) if is_compaction_summary(m))
        assert summary_text(summary).startswith("## Objective")
        fold = _started(events)[-1]
        assert fold["strategy"] == "auxiliary" and fold["fallback_reason"] == reason
        assert manager._last_summarization_stats["fallback_reason"] == reason
        assert not any(e == "compaction.failed" for e, _ in events)
        if setup in ("tool_call", "truncated"):
            meta = aux._archiver.archive.call_args_list[0].kwargs["auxiliary_metadata"]
            assert meta["strategy"] == "native" and meta["rejected"] == reason
        if setup in ("error", "timeout"):
            meta = aux._archiver.archive_error.call_args.kwargs["auxiliary_metadata"]
            assert meta["strategy"] == "native"

    @pytest.mark.asyncio
    async def test_not_allowed_here_is_a_plain_fold(self):
        manager, events = _manager()
        history = _history()
        llm = ForkLLM()
        manager.record_main_request(_sent(history), llm, history=_sent(history))
        await manager.summarize_and_compact(history, _aux(), allow_native=False)
        assert llm.calls == []
        assert "fallback_reason" not in _started(events)[-1]

    @pytest.mark.asyncio
    async def test_an_auxiliary_family_keeps_nothing_and_folds(self):
        manager, events = _manager(compaction={"strategy": "auxiliary"})
        history = _history()
        llm = ForkLLM()
        manager.record_main_request(_sent(history), llm, history=_sent(history))
        assert manager._last_request is None
        await manager.summarize_and_compact(history, _aux(), allow_native=True)
        assert llm.calls == []
        assert _started(events)[-1]["strategy"] == "auxiliary"
        assert "fallback_reason" not in _started(events)[-1]

    def test_a_model_switch_forgets_the_request(self):
        manager, _ = _manager()
        history = _history()
        manager.record_main_request(_sent(history), ForkLLM(), history=_sent(history))
        assert manager._last_request is not None
        manager.update_limits(manager.config, "gpt-5")
        assert manager._last_request is None

    @pytest.mark.asyncio
    async def test_ensure_within_limits_passes_it_on(self):
        manager, _ = _manager()
        history = _history(30)
        llm = ForkLLM()
        manager.record_main_request(_sent(history), llm, history=_sent(history))
        await manager.ensure_within_limits(history, _aux(), allow_native=True)
        assert len(llm.calls) == 1

    def test_the_default_output_reserve_applies_without_a_cap(self):
        assert output_cap(ForkLLM(max_tokens=None)) is None
        assert DEFAULT_OUTPUT_RESERVE == 16_384


# ---------------------------------------------------------------------------
# The session loop: records each answered request, compacts natively mid-turn
# ---------------------------------------------------------------------------

THREAD_ID = "11111111-2222-3333-4444-555555555555"
TOOL_RESULT = "row-data " * 80


@pytest.fixture
def _fast_summarizer_backoff(monkeypatch):
    monkeypatch.setattr("agent.core.summarizer.BACKOFF_SECONDS", (0.0, 0.0))


class _SessionLLM:
    """Streams four tool calls, then the answer; answers the fork."""

    def __init__(self) -> None:
        self.reasoning = None
        self.streamed: List[List[BaseMessage]] = []
        self.forks: List[List[BaseMessage]] = []
        replies = [
            AIMessage(
                content="",
                tool_calls=[{"name": "read_rows", "args": {"page": i}, "id": f"tc{i}"}],
            )
            for i in range(4)
        ]
        replies.append(AIMessage(content="Final answer: all rows read."))
        self._queue = iter(replies)

    async def astream(self, messages, **_kwargs):
        self.streamed.append(messages)
        yield next(self._queue)

    async def ainvoke(self, messages, **_kwargs):
        self.forks.append(list(messages))
        return AIMessage(
            content=NATIVE_SUMMARY, response_metadata={"finish_reason": "stop"}
        )


@pytest.mark.asyncio
@pytest.mark.usefixtures("_fast_summarizer_backoff")
async def test_the_session_loop_forks_its_last_request():
    from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop

    manager = ContextManager(
        config=ContextConfig(
            compaction_threshold_tokens=1_000_000,
            summarization_threshold_tokens=1_000_000,
            message_count_threshold=8,
            message_count_min_tokens=0,
            keep_recent_messages=4,
            keep_window_max_tool_result_chars=200,
            model_max_context_tokens=200_000,
            compaction=NATIVE,
        ),
        model="gpt-4",
        preserve_message_identity=True,
    )
    llm = _SessionLLM()
    aux = _aux()
    tool = MagicMock()
    tool.name = "read_rows"
    tool.ainvoke = AsyncMock(return_value=TOOL_RESULT)
    config = MagicMock()
    config.llm.timeout = 30
    config.memory.enabled = False
    config.memory.observer_interval = 5
    config.context_management.max_summary_length = 10_000
    config.officer.enabled = False
    messages: list = []
    queue = iter(["Read every row, then answer."])

    async def _input():
        try:
            return {"content": next(queue)}
        except StopIteration:
            raise asyncio.CancelledError from None

    callbacks = PersistentLoopCallbacks(
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
    )
    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[tool],
        context_manager=manager,
        config=config,
        system_prompt="system",
        callbacks=callbacks,
        messages=messages,
        auxiliary_llm=aux,
        memory_thread_id=THREAD_ID,
    )

    callbacks.on_error.assert_not_awaited()
    assert manager.compaction_runs == 1
    assert len(llm.forks) == 1, "the working model wrote the summary"
    aux.llm.ainvoke.assert_not_awaited()
    fork = llm.forks[0]
    # The compaction runs before the fifth call, after the fourth tool result:
    # the kept request is the fourth one streamed.
    assert len(llm.streamed) == 5
    previous = llm.streamed[3]
    assert len(fork) == len(previous) + 3
    assert all(a is b for a, b in zip(fork, previous)), "previous request unchanged"
    added = fork[len(previous) : -1]
    assert [type(m).__name__ for m in added] == ["AIMessage", "ToolMessage"]
    assert fork[-1].content.startswith(CODEX_HEAD)
    summary = next(m for m in messages if is_compaction_summary(m))
    assert summary_text(summary).startswith(NATIVE_SUMMARY)
    assert messages[-1].content == "Final answer: all rows read."


# ---------------------------------------------------------------------------
# The worker's execute node: records each answered request, then a threshold
# compaction forks it (harness: tests/test_execute_prepared_layout.py)
# ---------------------------------------------------------------------------

from tests.test_execute_prepared_layout import (  # noqa: E402
    CapturingLLM,
    _apply_turn,
    _make_node,
    _run,
    _state,
    env,  # noqa: F401  (fixture)
)


async def _two_worker_turns(env):  # noqa: F811
    manager = ContextManager(
        config=ContextConfig(
            compaction_threshold_tokens=1_000_000,
            summarization_threshold_tokens=1_000_000,
            message_count_threshold=1_000,
            message_count_min_tokens=0,
            keep_recent_messages=2,
            model_max_context_tokens=200_000,
            compaction=NATIVE,
        ),
        model="gpt-4",
    )
    env["context"] = manager
    call = AIMessage(
        content="",
        tool_calls=[{"name": "read_file", "args": {"path": "b.md"}, "id": "c2"}],
    )
    fork_reply = AIMessage(
        content=NATIVE_SUMMARY, response_metadata={"finish_reason": "stop"}
    )
    llm = CapturingLLM([call, fork_reply, AIMessage(content="done")])
    aux = MagicMock()
    node = _make_node(env, llm, auxiliary_llm=aux)

    state = _state()
    _apply_turn(state, await _run(node, state))
    assert manager._last_request is not None, "turn 1 kept its request"
    state["messages"] = state["messages"] + [
        ToolMessage(content="contents of b.md", tool_call_id="c2", name="read_file")
    ]
    manager.config.message_count_threshold = 3  # turn 2 crosses the threshold
    result = await _run(node, state)
    return llm, aux, manager, result


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["legacy", "append_only"])
async def test_the_worker_forks_its_last_request(env, mode):  # noqa: F811
    env["config"].context_management.injection_mode = mode
    llm, aux, manager, result = await _two_worker_turns(env)

    assert len(llm.requests) == 3, "turn 1, the fork, turn 2"
    first, fork, second = llm.requests
    assert all(a is b for a, b in zip(fork, first)), "previous request unchanged"
    added = fork[len(first) : -1]
    assert [type(m).__name__ for m in added] == ["AIMessage", "ToolMessage"]
    assert added[0].tool_calls[0]["id"] == "c2"
    assert fork[-1].content.startswith(CODEX_HEAD)
    assert manager.compaction_runs == 1
    summaries = [m for m in second if is_compaction_summary(m)]
    assert len(summaries) == 1 and isinstance(summaries[0], HumanMessage)
    assert summary_text(summaries[0]).startswith(NATIVE_SUMMARY)
    meta = aux.archive_native_compaction.call_args.kwargs["metadata"]
    assert meta == {"strategy": "native", "recipe": "codex"}
    # The harness's history has no ids (the reducer would assign them), so
    # the suffix was found by object identity. Turn 2 kept its own request.
    assert manager._last_request.messages == second
