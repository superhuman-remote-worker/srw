"""Compaction semantics for typed context entries (append-only injection).

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(D4, B3); spec:
knowledge-base/knowledge/plans/append_only_context_injection_wp2_spec.md §E,
sub-step 2.2. Nothing produces entries yet, so the histories here build them
directly with ``make_context_entry``.

- Entries are history: the summarized region evicts them (the worker with a
  ``RemoveMessage`` each, the session by dropping the region), the kept window
  keeps them with their carriers, kwargs intact.
- The keep window counts non-entries (``keep_window_start``, O7), and so do
  the message-count rule of ``should_summarize`` and the "nothing to
  summarize" check.
- ``find_safe_slice_start`` never starts on an entry and looks past an entry
  stored between the results of one batch.
- The summarizer reads no entry except supervisor guidance (O1); the legacy
  transient pieces are still filtered before anything else.
- WP1's ``restate_after_summary`` still seats its restatement right after the
  summary, ahead of the kept window and its entries.
"""

from __future__ import annotations

from typing import List
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    RemoveMessage,
    ToolMessage,
)

from agent.core.context import (
    ContextConfig,
    ContextManager,
    find_safe_slice_start,
    keep_window_start,
)
from agent.llm.response_guards import strip_removal_markers
from agent.managers.todo import TODO_LIST_RESTATEMENT_LEAD, TodoManager
from shared.runtime.core.context_entries import (
    FOLDED_KEY,
    entry_meta,
    fold_context_entries,
    is_context_entry,
    make_context_entry,
)
from shared.runtime.core.injection_markers import MEMORY_TOOL_CALL_ID_PREFIX
from shared.runtime.core.message_markers import is_compaction_summary

MEMORY_BODY = "[m:3f9a2c] The brief lives in docs/brief.md."
KNOWLEDGE_BODY = "Note: summaries are capped at 300 words."
GUIDANCE_BODY = "Prefer the newer brief over the old draft."
LATE_MEMORY_BODY = "[m:77aa01] The owner reviews summaries on Fridays."
SUMMARY = "## Objective\n- Summarize the brief."


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr("agent.core.summarizer.BACKOFF_SECONDS", (0.0, 0.0))


def _entry(kind: str, body: str, entry_id: str) -> HumanMessage:
    items = (
        [{"key": entry_id, "hash": "0" * 16, "handle": None}]
        if kind in ("memory", "knowledge", "guidance")
        else []
    )
    entry = make_context_entry(kind, body, section=kind, items=items)
    entry.id = entry_id
    return entry


def _round(n: int, content: str) -> List[BaseMessage]:
    return [
        AIMessage(
            content="",
            tool_calls=[{"name": "read_file", "args": {"n": n}, "id": f"c{n}"}],
            id=f"ai{n}",
        ),
        ToolMessage(
            content=content, tool_call_id=f"c{n}", name="read_file", id=f"t{n}"
        ),
    ]


def _history() -> List[BaseMessage]:
    """A worker-shaped history (ids as LangGraph assigns them) with entries
    after tool results, a legacy memory pair, and an entry in the keep
    window."""
    history: List[BaseMessage] = [HumanMessage(content="Start the task.", id="h0")]
    history += _round(1, "line 1\n" * 600)
    history.append(_entry("memory", MEMORY_BODY, "e1"))
    history += _round(2, "line 2\n" * 600)
    history.append(_entry("guidance", GUIDANCE_BODY, "e2"))
    history += _round(3, "line 3\n" * 600)
    history.append(_entry("knowledge", KNOWLEDGE_BODY, "e3"))
    history.append(_entry("citation", "No failed citations.", "e4"))
    # A legacy transient pair: never durable, filtered before compaction.
    legacy_id = f"{MEMORY_TOOL_CALL_ID_PREFIX}abc"
    history += [
        AIMessage(
            content="",
            tool_calls=[{"name": "recall_memories", "args": {}, "id": legacy_id}],
            id="legacy-ai",
        ),
        ToolMessage(content="legacy memory", tool_call_id=legacy_id, id="legacy-t"),
    ]
    history += _round(4, "short result 4")
    history.append(_entry("memory", LATE_MEMORY_BODY, "e5"))
    return history


def _summarizer() -> tuple:
    from shared.runtime.services.auxiliary import AuxiliaryLLM

    llm = MagicMock()
    llm.ainvoke = AsyncMock(
        return_value=AIMessage(
            content=SUMMARY, response_metadata={"finish_reason": "stop"}
        )
    )
    return AuxiliaryLLM(llm=llm, max_context_tokens=15_000), llm


def _manager(*, keep: int = 2, preserve_identity: bool = False) -> ContextManager:
    return ContextManager(
        config=ContextConfig(
            compaction_threshold_tokens=1_000,
            summarization_threshold_tokens=1_000,
            keep_recent_messages=keep,
            model_max_context_tokens=200_000,
        ),
        model="gpt-4",
        preserve_message_identity=preserve_identity,
    )


def _summarizer_text(llm: MagicMock) -> str:
    parts = []
    for call in llm.ainvoke.await_args_list:
        for message in call.args[0]:
            parts.append(str(getattr(message, "content", message)))
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# keep_window_start / find_safe_slice_start
# ---------------------------------------------------------------------------


class TestKeepWindowStart:
    def test_without_entries_it_is_the_plain_tail_slice(self):
        conversation = [HumanMessage(content=str(i)) for i in range(7)]
        for keep in range(1, 7):
            assert keep_window_start(conversation, keep) == len(conversation) - keep

    def test_entries_ride_with_their_carriers_and_are_not_counted(self):
        h = HumanMessage(content="go")
        a1, t1 = _round(1, "r1")
        e1 = _entry("memory", MEMORY_BODY, "e1")
        a2, t2 = _round(2, "r2")
        e2 = _entry("memory", LATE_MEMORY_BODY, "e2")
        e3 = _entry("citation", "none", "e3")
        conversation = [h, a1, t1, e1, a2, t2, e2, e3]

        start = keep_window_start(conversation, 2)

        assert conversation[start:] == [a2, t2, e2, e3]
        # e1 belongs to t1, which is outside the window, so it is too.
        assert e1 not in conversation[start:]
        assert keep_window_start(conversation, 3) == 2  # t1, then its e1

    def test_fewer_non_entries_than_the_window_keeps_everything(self):
        conversation = [
            HumanMessage(content="go"),
            _entry("memory", MEMORY_BODY, "e1"),
            AIMessage(content="ok"),
        ]
        assert keep_window_start(conversation, 2) == 0
        assert keep_window_start(conversation, 5) == 0

    def test_a_non_positive_window_keeps_nothing(self):
        conversation = [HumanMessage(content="go"), _entry("memory", "x", "e1")]
        assert keep_window_start(conversation, 0) == len(conversation)


class TestFindSafeSliceStart:
    def test_a_start_on_an_entry_steps_back_to_its_carrier(self):
        a1, t1 = _round(1, "r1")
        conversation = [
            HumanMessage(content="go"),
            a1,
            t1,
            _entry("memory", MEMORY_BODY, "e1"),
            AIMessage(content="done"),
        ]
        # e1's carrier is t1, whose call is a1: the window starts there.
        assert find_safe_slice_start(conversation, 3) == 1

    def test_a_start_on_an_entry_after_a_human_carrier(self):
        conversation = [
            AIMessage(content="earlier answer"),
            HumanMessage(content="next question"),
            _entry("turn_boundary", "guide", "e1"),
            AIMessage(content="answer"),
        ]
        assert find_safe_slice_start(conversation, 2) == 1

    def test_an_entry_between_one_batchs_results_is_not_a_boundary(self):
        call = AIMessage(
            content="",
            tool_calls=[
                {"name": "read_file", "args": {}, "id": "c1"},
                {"name": "read_file", "args": {}, "id": "c2"},
            ],
        )
        conversation = [
            HumanMessage(content="go"),
            call,
            ToolMessage(content="r1", tool_call_id="c1"),
            _entry("memory", MEMORY_BODY, "e1"),
            ToolMessage(content="r2", tool_call_id="c2"),
            AIMessage(content="done"),
        ]
        assert find_safe_slice_start(conversation, 4) == 1


# ---------------------------------------------------------------------------
# Message-count rules count non-entries (O7)
# ---------------------------------------------------------------------------


class TestMessageCountRules:
    def _count_manager(self) -> ContextManager:
        return ContextManager(
            config=ContextConfig(
                compaction_threshold_tokens=10_000_000,
                summarization_threshold_tokens=10_000_000,
                message_count_threshold=5,
                message_count_min_tokens=0,
            ),
            model="gpt-4",
        )

    def test_should_summarize_ignores_entries(self):
        manager = self._count_manager()
        conversation: List[BaseMessage] = [HumanMessage(content="go")]
        conversation += [_entry("memory", f"fact {i}", f"e{i}") for i in range(20)]
        conversation += [AIMessage(content="a"), HumanMessage(content="b")]

        assert not manager.should_summarize(conversation)  # 3 non-entries
        conversation += [AIMessage(content="c"), HumanMessage(content="d")]
        conversation += [AIMessage(content="e")]
        assert manager.should_summarize(conversation)  # 6 non-entries

    @pytest.mark.asyncio
    async def test_entries_never_push_a_short_history_into_a_summary(self):
        """Two non-entries and a keep window of two: nothing to summarize,
        however many entries ride along."""
        aux, llm = _summarizer()
        conversation = [
            HumanMessage(content="go", id="h"),
            _entry("memory", MEMORY_BODY, "e1"),
            _entry("knowledge", KNOWLEDGE_BODY, "e2"),
            AIMessage(content="ok", id="a"),
            _entry("citation", "none", "e3"),
        ]

        result = await _manager(keep=2).summarize_and_compact(conversation, aux)

        llm.ainvoke.assert_not_awaited()
        assert result == conversation


# ---------------------------------------------------------------------------
# Worker: the summarized region's entries are evicted, the window's are copied
# ---------------------------------------------------------------------------


class TestWorkerCompaction:
    @pytest.mark.asyncio
    async def test_summarized_entries_get_a_remove_marker(self):
        history = _history()
        aux, _llm = _summarizer()

        result = await _manager().summarize_and_compact(history, aux)

        removed = {m.id for m in result if isinstance(m, RemoveMessage)}
        # Every durable message is evicted (the window comes back as copies),
        # the summarized entries included. The legacy pair was never state.
        assert {"e1", "e2", "e3", "e4", "e5"} <= removed
        assert {"h0", "ai1", "t1", "ai4", "t4"} <= removed
        assert not {"legacy-ai", "legacy-t"} & removed

    @pytest.mark.asyncio
    async def test_kept_entries_are_copied_with_their_kwargs(self):
        history = _history()
        late = history[-1]
        aux, _llm = _summarizer()

        result = await _manager().summarize_and_compact(history, aux)

        kept = [m for m in result if not isinstance(m, RemoveMessage)]
        assert is_compaction_summary(kept[0])
        assert [type(m) for m in kept[1:]] == [AIMessage, ToolMessage, HumanMessage]
        copy = kept[-1]
        assert is_context_entry(copy) and copy.id is None  # appended fresh
        assert copy.content == late.content
        assert copy.additional_kwargs == late.additional_kwargs
        assert copy.additional_kwargs is not late.additional_kwargs
        # Only the window's entry survives; the summarized ones are gone.
        assert [entry_meta(m)["kind"] for m in kept if is_context_entry(m)] == [
            "memory"
        ]
        assert not any("legacy memory" in str(m.content) for m in kept)

    @pytest.mark.asyncio
    async def test_the_kept_entry_still_folds_into_its_carrier(self):
        aux, _llm = _summarizer()
        result = await _manager().summarize_and_compact(_history(), aux)
        kept = [m for m in result if not isinstance(m, RemoveMessage)]

        folded = fold_context_entries(kept)

        assert len(folded) == len(kept) - 1
        carrier = folded[-1]
        assert isinstance(carrier, ToolMessage)
        assert carrier.content.startswith("short result 4\n\n<srw_context")
        assert carrier.additional_kwargs[FOLDED_KEY] == ["memory"]


# ---------------------------------------------------------------------------
# Session: identity preserved, summarized entries dropped with their region
# ---------------------------------------------------------------------------


class TestSessionCompaction:
    @pytest.mark.asyncio
    async def test_summarized_entries_are_dropped(self):
        history = _history()
        late = history[-1]
        aux, _llm = _summarizer()

        result = await _manager(preserve_identity=True).summarize_and_compact(
            history, aux
        )

        removed = {m.id for m in result if isinstance(m, RemoveMessage)}
        assert {"e1", "e2", "e3", "e4"} <= removed
        # The window keeps its identity: no marker for it, same objects.
        assert not {"ai4", "t4", "e5"} & removed
        bounded = strip_removal_markers(result)
        entries = [m for m in bounded if is_context_entry(m)]
        assert entries == [late] and entries[0] is late
        assert bounded[-3:] == history[-3:]


# ---------------------------------------------------------------------------
# The summarizer reads no entry but guidance (D4, O1)
# ---------------------------------------------------------------------------


class TestSummarizerInput:
    @pytest.mark.asyncio
    async def test_only_guidance_entries_reach_the_summarizer(self):
        aux, llm = _summarizer()

        await _manager().summarize_and_compact(_history(), aux)

        text = _summarizer_text(llm)
        assert f"[Supervisor guidance]: {GUIDANCE_BODY}" in text
        for body in (MEMORY_BODY, KNOWLEDGE_BODY, LATE_MEMORY_BODY):
            assert body not in text
        assert "<srw_context" not in text
        assert "legacy memory" not in text
        assert "line 1" in text  # the conversation itself is summarized


# ---------------------------------------------------------------------------
# WP1's restatement still works with entries present (spec §E.4)
# ---------------------------------------------------------------------------


class TestRestateAfterSummaryWithEntries:
    def _todos(self) -> TodoManager:
        workspace = MagicMock()
        workspace.git_manager = None
        manager = TodoManager(workspace, min_todos=2)
        manager.is_strategic_phase = False
        manager.phase_number = 2
        manager.add("Read the brief")
        manager.add("Write the summary")
        return manager

    @pytest.mark.asyncio
    async def test_restatement_sits_after_the_summary_ahead_of_the_window(self):
        todos = self._todos()
        seen: list = []

        def hook(retained):
            seen.append(list(retained))
            text = todos.list_restatement(retained)
            return [HumanMessage(content=text)] if text else []

        aux, _llm = _summarizer()
        result = await _manager().summarize_and_compact(
            _history(), aux, restate_after_summary=hook
        )

        kept = [m for m in result if not isinstance(m, RemoveMessage)]
        assert is_compaction_summary(kept[0])
        restated = kept[1]
        assert restated.content.startswith(TODO_LIST_RESTATEMENT_LEAD)
        assert not is_context_entry(restated)  # never an entry, never folded
        assert [type(m) for m in kept[2:]] == [AIMessage, ToolMessage, HumanMessage]
        assert is_context_entry(kept[-1])
        # The hook saw the retained history, the window's entry included.
        assert any(is_context_entry(m) for m in seen[0])

        folded = fold_context_entries(kept)
        assert folded[1] is restated  # the restatement keeps its own bytes
        assert folded[-1].additional_kwargs[FOLDED_KEY] == ["memory"]

    @pytest.mark.asyncio
    async def test_an_entry_never_counts_as_the_list(self):
        """An entry's text never carries the todo rendering, so it cannot
        make the restatement think the list is still present."""
        todos = self._todos()
        history = _history()
        assert all(
            todos.format_for_injection() not in str(m.content)
            for m in history
            if is_context_entry(m)
        )

        aux, _llm = _summarizer()
        result = await _manager().summarize_and_compact(
            history,
            aux,
            restate_after_summary=lambda retained: (
                [HumanMessage(content=todos.list_restatement(retained))]
            ),
        )

        kept = [m for m in result if not isinstance(m, RemoveMessage)]
        assert sum(TODO_LIST_RESTATEMENT_LEAD in str(m.content) for m in kept) == 1
