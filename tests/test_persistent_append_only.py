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
import json
import time
from dataclasses import replace
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

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
from shared.runtime.services.recall_store import MemoryRecord
from shared.session_pending_memory import SESSION_PENDING_MEMORY_KEY
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


# ---------------------------------------------------------------------------
# Idle-time prefetch and the durable pending set (WP4; D24, D32, D33, B8, B9)
# ---------------------------------------------------------------------------

PREFETCHED = MemoryRecord(
    id=UUID(int=101),
    content="The deploy window is Friday 18:00 CET.",
    importance=0.7,
    token_count=9,
)


class ExchangeRetriever:
    """Memory retriever that tells the idle-time prefetch from a turn's own
    retrieval by the prefetch's request flags (no retries, no TTL tick).

    ``gate_turn`` / ``gate_prefetch`` hold that kind of call until it is
    cancelled. Records every request and each cancellation.
    """

    def __init__(
        self,
        *,
        prefetch=(PREFETCHED,),
        turn=(),
        gate_turn: bool = False,
        gate_prefetch: bool = False,
    ) -> None:
        self.prefetch = list(prefetch)
        self.turn = list(turn)
        self.gate_turn = gate_turn
        self.gate_prefetch = gate_prefetch
        self.requests: List[Any] = []
        self.cancelled = 0

    async def retrieve(self, req):
        from agent.services.memory import Candidate

        self.requests.append(req)
        is_prefetch = req.retries is False
        assert req.ttl_tick is not is_prefetch
        if self.gate_prefetch if is_prefetch else self.gate_turn:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled += 1
                raise
        records = self.prefetch if is_prefetch else self.turn
        return [
            Candidate(kind="memory", text=m.content, token_count=9, record=m)
            for m in records
        ]

    @property
    def prefetch_queries(self) -> List[str]:
        return [r.query_text for r in self.requests if r.retries is False]


class PrefetchSession(Session):
    """A session whose transport wires the idle-time prefetch.

    ``metadata`` stands in for ``threads.metadata``; ``save_pending_memory``
    writes the set as JSON (what the column holds) or clears it by id.
    ``input_arrives(n)`` answers the n-th new-input check.
    """

    def __init__(
        self,
        script: List[Any],
        *,
        retriever: ExchangeRetriever,
        budget: float = 1.0,
        input_arrives: Callable[[int], bool] = lambda n: False,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(script, **kwargs)
        self.retriever = retriever
        self.memory_service = make_async_manager(retriever, job_id="thread-1")
        self.config.memory.prefetch_budget_s = budget
        self.metadata: Dict[str, Any] = dict(metadata or {})
        self.input_arrives = input_arrives
        self.input_checks = 0
        self.clear_error: Optional[Exception] = None
        self.stop_turn = False

    async def _save(self, payload, expected_id) -> bool:
        if payload is not None:
            self.events.append(("save", "write"))
            self.metadata[SESSION_PENDING_MEMORY_KEY] = json.loads(json.dumps(payload))
            return True
        self.events.append(("save", "clear"))
        if self.clear_error is not None:
            raise self.clear_error
        stored = self.metadata.get(SESSION_PENDING_MEMORY_KEY)
        if stored is not None and stored.get("id") == expected_id:
            del self.metadata[SESSION_PENDING_MEMORY_KEY]
        return True

    async def _idle_input(self) -> bool:
        self.input_checks += 1
        return bool(self.input_arrives(self.input_checks))

    async def _settled(self, turn_id: int) -> None:
        self.events.append(("settled", turn_id))

    def callbacks(self, inputs: tuple = ()) -> PersistentLoopCallbacks:
        callbacks = replace(
            super().callbacks(inputs),
            idle_input_arrived=self._idle_input,
            save_pending_memory=self._save,
            on_turn_settled=self._settled,
        )
        if self.stop_turn:
            callbacks.check_interrupt = MagicMock(return_value="hard")
        return callbacks

    def take_bundle(self, bundle: Dict[str, Any]) -> bool:
        """A claim: the executor hands the bundle's set to the session."""
        from agent.api.persistent_session import PersistentSession
        from agent.api.turn_executor import claim_bundle_pending_memory

        session = SimpleNamespace(
            memory_service=self.memory_service, config=self.config, thread_id="t"
        )
        return PersistentSession.apply_pending_memory(
            session, claim_bundle_pending_memory(bundle)
        )

    async def run(  # type: ignore[override]
        self, *inputs: Any, stateless: bool = True, initial_turn_count: int = 0
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
            initial_turn_count=initial_turn_count,
            defer_memory_extraction_to_outbox=stateless,
        )
        assert self.errors == []

    def memory_entries(self) -> List[HumanMessage]:
        return [e for e in self.entries() if entry_kind(e) == "memory"]


def _bundle(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """A claim bundle: the stored set rides beside ``attach`` (B9)."""
    bundle: Dict[str, Any] = {"attach": {"thread_id": "t"}}
    if SESSION_PENDING_MEMORY_KEY in metadata:
        bundle[SESSION_PENDING_MEMORY_KEY] = metadata[SESSION_PENDING_MEMORY_KEY]
    return bundle


class TestIdlePrefetch:
    @pytest.mark.asyncio
    async def test_pod_handoff_the_next_turns_first_request_gets_the_prefetch(self):
        """Turn N on process A prefetches and saves the set; turn N+1 on
        process B (a fresh attach whose bundle carries it) sends it with its
        first request, while B's own turn retrieval has not finished."""
        pod_a = PrefetchSession([text_turn("One.")], retriever=ExchangeRetriever())
        await pod_a.run("Turn one?")

        assert pod_a.retriever.prefetch_queries == ["Turn one?\n\nOne."]
        stored = pod_a.metadata[SESSION_PENDING_MEMORY_KEY]
        assert [m["content"] for m in stored["memory"]] == [PREFETCHED.content]
        assert stored["turn"] == 1
        # Stateless: saved before the settled edge, i.e. before the executor
        # closes the interrupt window, detaches and completes the unit (B8).
        assert pod_a.events.index(("save", "write")) < pod_a.events.index(
            ("settled", 1)
        )
        assert pod_a.memory_entries() == []

        pod_b = PrefetchSession(
            [text_turn("Two.")],
            retriever=ExchangeRetriever(prefetch=(), gate_turn=True),
            metadata=pod_a.metadata,
        )
        pod_b.messages = list(pod_a.messages)  # restored history
        assert pod_b.take_bundle(_bundle(pod_b.metadata)) is True

        await pod_b.run("Turn two?", initial_turn_count=1)

        (first,) = pod_b.llm.calls
        assert _texts(first).count(_srw("memory")) == 1
        assert _texts(first).count(PREFETCHED.content) == 1
        assert len(pod_b.memory_entries()) == 1
        # The memory went out as a persisted context row (write first) ...
        assert "memory" in [entry_kind(m) for m in pod_b.persisted]
        # ... and B's turn end cleared the drained set (clear second): its
        # own turn retrieval was still running (cancelled, single flight)
        # and its prefetch found nothing new.
        assert pod_b.retriever.cancelled == 1
        assert SESSION_PENDING_MEMORY_KEY not in pod_b.metadata
        assert pod_b.events.index(("save", "clear")) < pod_b.events.index(
            ("settled", 2)
        )

    @pytest.mark.asyncio
    async def test_a_replay_after_a_crash_between_write_and_clear_appends_nothing(
        self,
    ):
        """The turn that drained the set dies before its clear lands; the
        next process gets the same set again and the history already holds
        it (D3), so nothing is appended twice."""
        pod_a = PrefetchSession([text_turn("One.")], retriever=ExchangeRetriever())
        await pod_a.run("Turn one?")
        pod_b = PrefetchSession(
            [text_turn("Two.")],
            retriever=ExchangeRetriever(prefetch=(), gate_turn=True),
            metadata=pod_a.metadata,
        )
        pod_b.messages = list(pod_a.messages)
        pod_b.take_bundle(_bundle(pod_b.metadata))
        pod_b.clear_error = ConnectionError("pod died before the clear")
        await pod_b.run("Turn two?", initial_turn_count=1)
        assert SESSION_PENDING_MEMORY_KEY in pod_b.metadata  # the clear failed
        assert len(pod_b.memory_entries()) == 1

        pod_c = PrefetchSession(
            [text_turn("Three.")],
            retriever=ExchangeRetriever(prefetch=(), gate_turn=True),
            metadata=pod_b.metadata,
        )
        pod_c.messages = list(pod_b.messages)
        assert pod_c.take_bundle(_bundle(pod_c.metadata)) is True

        await pod_c.run("Turn three?", initial_turn_count=2)

        (first,) = pod_c.llm.calls
        assert _texts(first).count(_srw("memory")) == 1  # B's entry only
        assert _texts(first).count(PREFETCHED.content) == 1
        assert len(pod_c.memory_entries()) == 1
        assert pod_c.memory_service.retrieval_stats()["prefetch_taken"] == 1
        # C's turn end clears the replayed set.
        assert SESSION_PENDING_MEMORY_KEY not in pod_c.metadata

    @pytest.mark.asyncio
    async def test_a_warm_reuse_takes_its_own_set_in_once(self):
        session = PrefetchSession(
            [text_turn("One."), text_turn("Two.")],
            retriever=ExchangeRetriever(),
        )
        await session.run("Turn one?")
        assert SESSION_PENDING_MEMORY_KEY in session.metadata

        # The next claim reuses the warm session: the bundle carries the set
        # it already holds, which is not taken a second time.
        assert session.take_bundle(_bundle(session.metadata)) is False
        await session.run("Turn two?", initial_turn_count=1)

        first_of_turn_two = session.llm.calls[1]
        assert _texts(first_of_turn_two).count(PREFETCHED.content) == 1
        assert len(session.memory_entries()) == 1
        # Turn two's prefetch found the same memory, already in the history:
        # nothing new to store, and the drained set is cleared.
        assert session.retriever.prefetch_queries == [
            "Turn one?\n\nOne.",
            "Turn two?\n\nTwo.",
        ]
        assert SESSION_PENDING_MEMORY_KEY not in session.metadata
        stats = session.memory_service.retrieval_stats()
        assert (stats["prefetch_ok"], stats["prefetch_empty"]) == (1, 1)
        for prev, nxt in zip(session.llm.calls, session.llm.calls[1:]):
            assert _view(nxt)[: len(prev)] == _view(prev)

    @pytest.mark.asyncio
    async def test_the_budget_ends_the_turn_without_a_prefetch(self):
        session = PrefetchSession(
            [text_turn("One.")],
            retriever=ExchangeRetriever(gate_prefetch=True),
            budget=0.1,
        )

        started = time.monotonic()
        await session.run("Turn one?")

        assert time.monotonic() - started < 1.0
        assert session.retriever.cancelled == 1
        assert session.metadata == {}
        assert ("save", "write") not in session.events
        assert session.memory_service.retrieval_stats()["prefetch_timed_out"] == 1

    @pytest.mark.asyncio
    async def test_new_input_cancels_the_prefetch(self):
        session = PrefetchSession(
            [text_turn("One.")],
            retriever=ExchangeRetriever(gate_prefetch=True),
            budget=30.0,
            input_arrives=lambda n: n >= 2,
        )

        started = time.monotonic()
        await session.run("Turn one?")

        # One check before the start, one at the first poll (0.25 s).
        assert session.input_checks == 2
        assert time.monotonic() - started < 2.0
        assert session.retriever.cancelled == 1
        assert session.metadata == {}
        assert session.memory_service.retrieval_stats()["prefetch_cancelled"] == 1

    @pytest.mark.asyncio
    async def test_pinned_runs_it_after_the_turn_settled(self):
        session = PrefetchSession([text_turn("One.")], retriever=ExchangeRetriever())

        await session.run("Turn one?", stateless=False)

        assert session.events.index(("settled", 1)) < session.events.index(
            ("save", "write")
        )
        assert SESSION_PENDING_MEMORY_KEY in session.metadata

    @pytest.mark.asyncio
    async def test_legacy_mode_neither_prefetches_nor_stores(self):
        session = PrefetchSession(
            [text_turn("One.")], retriever=ExchangeRetriever(), mode="legacy"
        )

        await session.run("Turn one?")

        assert session.retriever.prefetch_queries == []
        assert session.metadata == {}
        assert session.input_checks == 0
        assert not any(event == "save" for event, *_ in session.events)

    @pytest.mark.asyncio
    async def test_a_stopped_turn_ends_without_a_prefetch(self):
        session = PrefetchSession([text_turn("One.")], retriever=ExchangeRetriever())
        session.stop_turn = True

        await session.run("Turn one?")

        assert session.retriever.prefetch_queries == []
        assert session.metadata == {}
