"""The session loop in ``append_only`` mode (append-only context injection).

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D1, D3, D4, D21, D22, D27, D28); spec:
knowledge-base/knowledge/plans/append_only_context_injection_wp2_spec.md
§C, §E.5, §F.5, §F.6 and §K row 2.5.

The session loop (``persistent_graph``) reads
``context_management.injection_mode`` per turn. In ``append_only`` mode the
charter, memory, knowledge, citation feedback, the subagent status and the
App Guide turn boundary are not rebuilt at the tail of every provider call.
Each provider call plans what is new or changed against the compacted
history (turn-start retrieval, unchanged), and every planned entry is
stamped with the turn, appended to the durable list and persisted before
the provider call, after the input was admitted. The request is the history
with its entries folded into their carrier. These tests drive the real loop
with a scripted chat model; the legacy pins live in test_persistent_graph.py,
test_memory_persistent_equivalence.py, test_officer_charter.py,
test_app_guide_compaction.py and test_persistent_subagent_prompt_state.py.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from agent.api.persistent_app import _select_turn_messages
from agent.core.context_injection import format_active_subagents_none
from agent.persistent_graph import (
    PersistentLoopCallbacks,
    _execute_turn,
    run_persistent_loop,
)
from shared.runtime.core.context_entries import (
    FOLDED_KEY,
    entry_body,
    entry_kind,
    entry_meta,
    is_context_entry,
    make_context_entry,
)
from shared.runtime.core.message_markers import (
    stamp_turn_membership,
    turn_membership,
)
from tests import _prompt_cache_prefix_harness as harness
from tests._fake_chat_model import FakeChatModel, text_turn, tool_turn
from tests._memory_fixtures import (
    BrokenScorer,
    GatedRetriever,
    make_async_manager,
    settle_retrieval,
)

MODEL = "gpt-5.6-sol"
SYSTEM_PROMPT = "You are the SRW session under test."
TURN_START_KINDS = ["charter", "memory", "knowledge", "subagents", "turn_boundary"]
SUBAGENTS_A = (
    "<active_subagents>\n- scout-01 (explorer): running\n"
    "Reports push automatically as evidence; do not poll.\n</active_subagents>"
)
SUBAGENTS_B = (
    "<active_subagents>\n- scout-02 (explorer): queued\n"
    "Reports push automatically as evidence; do not poll.\n</active_subagents>"
)
LEGACY_PAIR_PREFIXES = ("memory_inject_", "knowledge_inject_", "charter_inject_")


class _LoggingModel(FakeChatModel):
    """FakeChatModel that logs each provider call into the shared event log."""

    def __init__(self, script: List[Any], events: List[tuple]) -> None:
        super().__init__(script)
        self._events = events

    async def astream(self, messages, **kw):
        self._events.append(("provider", len(self.calls)))
        async for chunk in super().astream(messages, **kw):
            yield chunk


def _context_manager(
    compact: Optional[Callable[[int, List[BaseMessage]], Any]] = None,
) -> AsyncMock:
    """A pass-through context manager; ``compact(call, messages)`` may return
    a compacted history, which the loop adopts (``compaction_runs`` bumps)."""
    manager = AsyncMock()
    manager.should_summarize = MagicMock(return_value=False)
    manager.compaction_runs = 0
    manager.config.keep_recent_messages = 10
    manager.record_provider_usage = MagicMock()
    calls = {"n": 0}

    async def ensure(messages, *_args, **_kwargs):
        calls["n"] += 1
        if compact is not None:
            result = compact(calls["n"], list(messages))
            if result is not None:
                manager.compaction_runs += 1
                return result
        return messages

    manager.ensure_within_limits = ensure
    return manager


class Session:
    """One session loop with every append_only source live.

    Charter (officer conference), memory and knowledge (the recording
    MemoryManager), the subagent status (``subagents`` returns the block per
    call; None = no subagent runtime) and the App Guide turn boundary (the
    managed guide and its reader are bound).
    """

    def __init__(
        self,
        script: List[Any],
        *,
        mode: str = "append_only",
        subagents: Optional[Callable[[], str]] = lambda: SUBAGENTS_A,
        compact: Optional[Callable[[int, List[BaseMessage]], Any]] = None,
    ) -> None:
        self.events: List[tuple] = []
        self.llm = _LoggingModel(script, self.events)
        self.config = harness._session_config(
            injections=True, model=MODEL, injection_mode=mode
        )
        self.context_manager = _context_manager(compact)
        self.tools = [
            harness._session_tool(t["function"]["name"]) for t in harness.SESSION_TOOLS
        ]
        self.tool_context = SimpleNamespace(knowledge_bindings=[], citation_engine=None)
        if subagents is not None:
            self.tool_context.subagent_runtime = SimpleNamespace(
                active_subagents_block=subagents
            )
        self.knowledge_store = MagicMock()
        self.knowledge_store.get_charter_note = AsyncMock(return_value=harness.CHARTER)
        self.memory_service = harness.RecordingMemoryManager()
        self.messages: List[BaseMessage] = []
        self.persisted: List[BaseMessage] = []
        self.archived: List[tuple] = []
        self.errors: List[Any] = []

    async def _persist(self, msg: Any) -> bool:
        self.events.append(("persist", entry_kind(msg) or msg.type))
        self.persisted.append(msg)
        return True

    def _archive(self, prepared, response, metrics, *args, **kwargs) -> None:
        self.archived.append((list(prepared), args, dict(kwargs)))

    async def _on_error(self, *args: Any, **kwargs: Any) -> None:
        self.errors.append((args, kwargs))

    def callbacks(self, inputs: tuple = ()) -> PersistentLoopCallbacks:
        return PersistentLoopCallbacks(
            get_user_input=AsyncMock(side_effect=[*inputs, asyncio.CancelledError()]),
            on_token=AsyncMock(),
            on_thinking=AsyncMock(),
            on_tool_start=AsyncMock(),
            on_tool_result=AsyncMock(),
            permission_check=AsyncMock(return_value=True),
            on_turn_start=AsyncMock(),
            on_turn_complete=AsyncMock(),
            on_error=self._on_error,
            check_interrupt=MagicMock(return_value=None),
            persist_message=self._persist,
            archive_llm_call=self._archive,
        )

    async def run(
        self, *inputs: str, get_current_context: Optional[Callable] = None
    ) -> None:
        await run_persistent_loop(
            llm_with_tools=self.llm,
            tools=self.tools,
            context_manager=self.context_manager,
            config=self.config,
            system_prompt=SYSTEM_PROMPT,
            callbacks=self.callbacks(inputs),
            messages=self.messages,
            knowledge_store=self.knowledge_store,
            project_ids=[harness.PROJECT_ID],
            tool_context=self.tool_context,
            memory_service=self.memory_service,
            get_current_context=get_current_context,
        )
        assert self.errors == []

    def entries(self) -> List[HumanMessage]:
        return [m for m in self.messages if is_context_entry(m)]

    def persisted_between(self, call: int) -> List[str]:
        """What was persisted after provider call ``call - 1`` and before ``call``."""
        start = 0
        if call > 0:
            start = self.events.index(("provider", call - 1)) + 1
        end = self.events.index(("provider", call))
        return [kind for event, *rest in self.events[start:end] for kind in rest]


def _texts(provider_input: List[BaseMessage]) -> str:
    return "\n".join(str(m.content) for m in provider_input)


def _srw(kind: str) -> str:
    return f'<srw_context kind="{kind}">'


def _view(provider_input: List[BaseMessage]) -> List[tuple]:
    return [(type(m).__name__, str(m.content)) for m in provider_input]


# ---------------------------------------------------------------------------
# Write first: admitted, persisted, then the provider call (§F.5)
# ---------------------------------------------------------------------------


class TestWriteFirst:
    async def _turn(self, session: Session, admit) -> Any:
        user = stamp_turn_membership(HumanMessage(content="Hello", id="input-3"), 3)
        session.messages[:] = [SystemMessage(content=SYSTEM_PROMPT), user]
        return await _execute_turn(
            llm_with_tools=session.llm,
            tool_map={tool.name: tool for tool in session.tools},
            context_manager=session.context_manager,
            messages=session.messages,
            callbacks=session.callbacks(),
            llm_timeout=600,
            auxiliary_llm=None,
            config=session.config,
            knowledge_store=session.knowledge_store,
            project_ids=[harness.PROJECT_ID],
            tool_context=session.tool_context,
            memory_service=session.memory_service,
            before_first_provider_admission=admit,
            turn_id=3,
        )

    @pytest.mark.asyncio
    async def test_entries_are_persisted_after_admission_before_the_provider(self):
        session = Session([text_turn("done")])

        async def admit() -> bool:
            session.events.append(("admit",))
            return True

        result = await self._turn(session, admit)

        assert result.error is None and not result.admission_closed
        assert session.events == [
            ("admit",),
            *[("persist", kind) for kind in TURN_START_KINDS],
            ("provider", 0),
            ("persist", "ai"),
        ]
        entries = session.entries()
        assert [entry_kind(e) for e in entries] == TURN_START_KINDS
        # The persisted objects ARE the history entries: stamped with the
        # turn, given a stable id, right after the input they ride on.
        assert session.persisted[: len(entries)] == entries
        assert session.messages[1].id == "input-3"
        assert session.messages[2 : 2 + len(entries)] == entries
        for entry in entries:
            assert entry.id
            assert turn_membership(entry) == 3
            assert entry_meta(entry)["turn"] == 3
        assert entry_meta(entries[-1])["section"] == "turn_boundary:3"
        assert result.messages_added == len(entries) + 1

        # The request: the input with the entries folded in, nothing after it.
        (sent,) = session.llm.calls
        assert [type(m) for m in sent] == [SystemMessage, HumanMessage]
        assert sent[1].content == "\n\n".join(
            ["Hello"] + [str(e.content) for e in entries]
        )
        assert sent[1].additional_kwargs[FOLDED_KEY] == TURN_START_KINDS

    @pytest.mark.asyncio
    async def test_a_refused_admission_appends_nothing(self):
        session = Session([text_turn("never sent")])

        async def admit() -> bool:
            session.events.append(("admit",))
            return False

        result = await self._turn(session, admit)

        assert result.admission_closed
        assert session.events == [("admit",)]
        assert session.entries() == []
        assert session.llm.calls == []
        assert len(session.messages) == 2


# ---------------------------------------------------------------------------
# Once per conversation / turn / change (D21, D22)
# ---------------------------------------------------------------------------


class TestAppendOnChange:
    @pytest.mark.asyncio
    async def test_two_turns_boundary_per_turn_the_rest_once(self):
        session = Session(
            [
                tool_turn("read_file", {"path": "notes.md"}, "c1"),
                text_turn("Answer one."),
                text_turn("Answer two."),
            ]
        )

        await session.run("Turn one?", "Turn two?")

        entries = session.entries()
        assert [entry_kind(e) for e in entries] == TURN_START_KINDS + ["turn_boundary"]
        assert [
            entry_meta(e)["section"]
            for e in entries
            if entry_kind(e) == "turn_boundary"
        ] == ["turn_boundary:1", "turn_boundary:2"]
        # Each turn's entries sit right after its input (D22).
        humans = [
            i
            for i, m in enumerate(session.messages)
            if isinstance(m, HumanMessage) and not is_context_entry(m)
        ]
        first, second = humans
        assert session.messages[first].content == "Turn one?"
        assert session.messages[first + 1 : first + 6] == entries[:5]
        assert session.messages[second].content == "Turn two?"
        assert session.messages[second + 1] is entries[5]

        # The call after the tool result re-sent nothing; turn two appended
        # only its own boundary.
        # The loop persists each input at turn start, then its entries.
        assert session.persisted_between(0) == ["human"] + TURN_START_KINDS
        assert session.persisted_between(1) == ["ai", "tool"]
        assert session.persisted_between(2) == ["ai", "human", "turn_boundary"]

        calls = session.llm.calls
        assert [_texts(c).count(_srw("turn_boundary")) for c in calls] == [1, 1, 2]
        for call in calls:
            for kind in ("charter", "memory", "knowledge", "subagents"):
                assert _texts(call).count(_srw(kind)) == 1, kind
            assert _texts(call).count(harness.CHARTER["content"]) == 1
        # Append-only: every request starts with the previous one.
        for prev, nxt in zip(calls, calls[1:]):
            assert _view(nxt)[: len(prev)] == _view(prev)

    @pytest.mark.asyncio
    async def test_subagent_status_on_change_cleared_and_quiet(self):
        states = iter([SUBAGENTS_A, SUBAGENTS_A, "", "", SUBAGENTS_B])
        session = Session(
            [tool_turn("read_file", {"path": f"f{i}"}, f"c{i}") for i in range(4)]
            + [text_turn("done")],
            subagents=lambda: next(states),
        )

        await session.run("Work, please.")

        subagent_entries = [
            e for e in session.entries() if entry_kind(e) == "subagents"
        ]
        assert [entry_body(e) for e in subagent_entries] == [
            SUBAGENTS_A,
            format_active_subagents_none(MODEL),
            SUBAGENTS_B,
        ]
        assert [k for k in session.persisted_between(1) if k != "ai"] == ["tool"]
        assert "subagents" in session.persisted_between(2)
        assert "subagents" not in session.persisted_between(3)
        assert "subagents" in session.persisted_between(4)
        # Never a standalone status message: each rides a tool result.
        for call in session.llm.calls:
            assert not any(
                isinstance(m, HumanMessage)
                and str(m.content).startswith("<active_subagents>")
                for m in call
            )

    @pytest.mark.asyncio
    async def test_no_subagent_runtime_appends_no_status(self):
        session = Session([text_turn("done")], subagents=None)

        await session.run("Hi")

        assert [entry_kind(e) for e in session.entries()] == [
            "charter",
            "memory",
            "knowledge",
            "turn_boundary",
        ]

    @pytest.mark.asyncio
    async def test_compaction_evicts_entries_and_the_next_call_re_appends(self):
        def compact(call: int, messages: List[BaseMessage]):
            if call != 2:
                return None
            # The summary replaces everything before the last tool batch,
            # turn-start entries included (D4).
            return [
                messages[0],
                SystemMessage(content="[Summary of prior work]\nrecap"),
                *messages[-2:],
            ]

        session = Session(
            [
                tool_turn("read_file", {"path": "a"}, "c1"),
                tool_turn("read_file", {"path": "b"}, "c2"),
                text_turn("done"),
            ],
            compact=compact,
        )

        await session.run("Read both.")

        assert session.persisted_between(0) == ["human"] + TURN_START_KINDS
        # Call 2 runs on the compacted history: every entry is absent there
        # and goes in again, after the newest tool result, persisted first.
        assert session.persisted_between(1) == ["ai", "tool"] + TURN_START_KINDS
        assert session.persisted_between(2) == ["ai", "tool"]
        second = session.llm.calls[1]
        assert isinstance(second[-1], ToolMessage)
        assert second[-1].additional_kwargs[FOLDED_KEY] == TURN_START_KINDS
        assert [type(m) for m in session.messages[:4]] == [
            SystemMessage,
            SystemMessage,
            AIMessage,
            ToolMessage,
        ]
        assert [entry_kind(m) for m in session.messages[4:9]] == TURN_START_KINDS
        third = session.llm.calls[2]
        assert _view(third)[: len(second)] == _view(second)


# ---------------------------------------------------------------------------
# The provider input and the archive (§F.6)
# ---------------------------------------------------------------------------


def _legacy_pieces(provider_input: List[BaseMessage]) -> Dict[str, int]:
    pairs = sum(
        1
        for m in provider_input
        if isinstance(m, ToolMessage)
        and str(m.tool_call_id).startswith(LEGACY_PAIR_PREFIXES)
    )
    tails = sum(
        1
        for m in provider_input
        if isinstance(m, HumanMessage)
        and str(m.content).startswith(
            ("<active_subagents>", "<managed_product_guide_turn_boundary")
        )
    )
    return {"pairs": pairs, "tails": tails}


class TestProviderInput:
    @pytest.mark.asyncio
    async def test_append_only_skips_the_legacy_pairs_and_tail(self):
        session = Session(
            [tool_turn("read_file", {"path": "a"}, "c1"), text_turn("done")]
        )

        await session.run("Go.")

        for call in session.llm.calls:
            assert _legacy_pieces(call) == {"pairs": 0, "tails": 0}
            assert not any(is_context_entry(m) for m in call)
            assert any(FOLDED_KEY in m.additional_kwargs for m in call)

    @pytest.mark.asyncio
    async def test_legacy_keeps_the_per_call_tail(self):
        """The same session in legacy mode: the tail is rebuilt per call and
        never enters the history (the discriminating control)."""
        session = Session(
            [tool_turn("read_file", {"path": "a"}, "c1"), text_turn("done")],
            mode="legacy",
        )

        await session.run("Go.")

        assert session.entries() == []
        for call in session.llm.calls:
            # charter + memory + knowledge pairs; subagents + boundary tails
            assert _legacy_pieces(call) == {"pairs": 3, "tails": 2}
            assert not any(FOLDED_KEY in m.additional_kwargs for m in call)

    @pytest.mark.asyncio
    async def test_the_archive_gets_the_unfolded_history(self):
        session = Session(
            [tool_turn("read_file", {"path": "a"}, "c1"), text_turn("done")]
        )

        await session.run("Go.")

        assert len(session.archived) == 2
        for (prepared, args, kwargs), sent in zip(session.archived, session.llm.calls):
            assert args == ()
            assert _view(prepared) == _view(sent)
            # llm_requests keeps the folded request; the chat delta reads the
            # history, where each entry is its own message.
            history = kwargs["history_messages"]
            assert [entry_kind(m) for m in history if is_context_entry(m)] == (
                TURN_START_KINDS
            )
            assert not any(is_context_entry(m) for m in prepared)
            assert len(history) == len(prepared) + len(TURN_START_KINDS)

    @pytest.mark.asyncio
    async def test_legacy_archive_call_is_unchanged(self):
        session = Session([text_turn("done")], mode="legacy")

        await session.run("Go.")

        assert [(args, kwargs) for _p, args, kwargs in session.archived] == [((), {})]


# ---------------------------------------------------------------------------
# The mode is read per turn (sessions hot-swap their config)
# ---------------------------------------------------------------------------


class TestModeIsReadPerTurn:
    @pytest.mark.asyncio
    async def test_a_swap_to_append_only_takes_effect_at_the_next_turn(self):
        session = Session([text_turn("Answer one."), text_turn("Answer two.")])
        legacy = harness._session_config(
            injections=True, model=MODEL, injection_mode="legacy"
        )
        configs = iter([legacy, session.config])

        await session.run(
            "Turn one?",
            "Turn two?",
            get_current_context=lambda: (session.context_manager, next(configs), None),
        )

        first, second = session.llm.calls
        assert _legacy_pieces(first) == {"pairs": 3, "tails": 2}
        assert _legacy_pieces(second) == {"pairs": 0, "tails": 0}
        entries = session.entries()
        assert [entry_kind(e) for e in entries] == TURN_START_KINDS
        assert {turn_membership(e) for e in entries} == {2}
        assert entry_meta(entries[-1])["section"] == "turn_boundary:2"


# ---------------------------------------------------------------------------
# The pinned turn-end reconcile keeps the turn's entries
# ---------------------------------------------------------------------------


class TestPinnedReconcileWalk:
    def test_an_entry_is_not_the_turn_boundary(self):
        """The pinned walk (unstamped history) stops at the turn's input, not
        at the first HumanMessage-typed entry, so a mid-turn entry and the
        turn-start entries are re-saved with the turn's rows."""

        def entry(kind: str) -> HumanMessage:
            msg = make_context_entry(kind, f"{kind} body", section=kind)
            msg.id = f"{kind}-row"
            return msg

        previous = AIMessage(content="earlier answer", id="a0")
        user = HumanMessage(content="question", id="in-2")
        turn = [
            entry("memory"),
            AIMessage(
                content="",
                tool_calls=[{"name": "t", "args": {}, "id": "c1"}],
                id="a1",
            ),
            ToolMessage(content="out", tool_call_id="c1", id="t1"),
            entry("subagents"),
            AIMessage(content="answer", id="a2"),
        ]

        selected = _select_turn_messages(
            [SystemMessage(content="sys"), previous, user, *turn],
            2,
            authoritative_turn_boundary=False,
            turn_input_message_id=None,
        )

        assert selected == turn


# ---------------------------------------------------------------------------
# Retrieval off the request path (WP3; D6-D8, D13)
# ---------------------------------------------------------------------------


class TestAsyncRetrieval:
    """The turn starts its retrieval without awaiting it; each provider call
    takes in the latest finished result before it plans. A real
    MemoryManager with a gated retriever decides when a retrieval finishes."""

    @staticmethod
    def _release_in_tool(session: Session, retriever: GatedRetriever, index: int):
        """The read_file tool finishes retrieval ``index`` while it runs."""

        async def run(args: Any) -> str:
            retriever.release(index)
            await settle_retrieval(session.memory_service)
            return "ok: read_file"

        session.tools[0].ainvoke = AsyncMock(side_effect=run)

    @pytest.mark.asyncio
    async def test_the_turn_start_retrieval_is_not_awaited_and_lands_mid_turn(
        self,
    ):
        retriever = GatedRetriever()
        session = Session(
            [tool_turn("read_file", {"path": "notes.md"}, "c1"), text_turn("One.")]
        )
        session.memory_service = make_async_manager(retriever)
        self._release_in_tool(session, retriever, 0)

        await session.run("Turn one?")

        assert retriever.queries == ["Turn one?"]
        first, second = session.llm.calls
        # The first call went out while the retrieval ran (D6) ...
        assert _texts(first).count(_srw("memory")) == 0
        assert _texts(first).count(_srw("charter")) == 1
        # ... and the next call of the turn took the result in (D8, D26).
        assert _texts(second).count(_srw("memory")) == 1
        assert _view(second)[: len(first)] == _view(first)
        assert [entry_kind(e) for e in session.entries()].count("memory") == 1
        assert session.memory_service.retrieval_stats()["drained"] == 1

    @pytest.mark.asyncio
    async def test_a_retrieval_still_in_flight_defers_the_next_turns_start(self):
        """Single flight across turns: turn two's retrieval starts at the
        first call that finds none in flight, after taking turn one's in."""
        retriever = GatedRetriever()
        session = Session(
            [
                text_turn("One."),
                tool_turn("read_file", {"path": "plan.md"}, "c2"),
                text_turn("Two."),
            ]
        )
        session.memory_service = make_async_manager(retriever)
        self._release_in_tool(session, retriever, 0)

        await session.run("Turn one?", "Turn two?")

        # Turn two found turn one's retrieval running: no second start then.
        # The call after the tool took turn one's result in and started turn
        # two's retrieval with turn two's query.
        assert retriever.queries == ["Turn one?", "Turn two?"]
        calls = session.llm.calls
        assert [_texts(c).count(_srw("memory")) for c in calls] == [0, 0, 1]
        for prev, nxt in zip(calls, calls[1:]):
            assert _view(nxt)[: len(prev)] == _view(prev)
        stats = session.memory_service.retrieval_stats()
        # Skipped at turn two's start and at its first call, both while turn
        # one's retrieval still ran.
        assert stats["skipped_in_flight"] == 2
        assert stats["started"] == 2

        await session.memory_service.close_background()
        assert retriever.cancelled == 1

    @pytest.mark.asyncio
    async def test_a_structural_reranker_failure_leaves_the_turn_alive(self):
        session = Session([text_turn("One."), text_turn("Two.")])
        session.memory_service = make_async_manager(
            GatedRetriever(auto=True),
            scorer=BrokenScorer(ValueError("rerank route returned HTML")),
            job_id="thread-1",
        )
        archiver = MagicMock()

        with patch("agent.core.archiver.get_archiver", return_value=archiver):
            await session.run("Turn one?", "Turn two?")

        assert len(session.llm.calls) == 2
        assert all(_texts(c).count(_srw("memory")) == 0 for c in session.llm.calls)
        assert session.memory_service.retrieval_stats()["degraded"] == 2
        steps = [c.kwargs["step_type"] for c in archiver.audit_step.call_args_list]
        assert steps == ["memory_pipeline_degraded"]

    @pytest.mark.asyncio
    async def test_legacy_mode_still_fails_the_turn(self):
        session = Session([text_turn("One.")], mode="legacy")
        session.memory_service = make_async_manager(
            GatedRetriever(auto=True),
            scorer=BrokenScorer(ValueError("rerank route returned HTML")),
        )

        with pytest.raises(AssertionError):
            await session.run("Turn one?")

        assert session.errors
        assert session.llm.calls == []
        assert session.memory_service.retrieval_stats()["started"] == 0
