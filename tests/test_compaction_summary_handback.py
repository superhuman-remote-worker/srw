"""The compaction summary goes back as a marked user message (compaction WP9).

One Claude-style wrapper for every family (lead-in, summary, continue line),
summary first, then the kept messages. It is runtime-authored, so it must
never pass for what the user said: memory queries, memory extraction and the
session's turn reconcile skip it. Histories from before the change hold a
``[Summary of prior work]`` SystemMessage; those still restore and re-compact.
See knowledge-base/knowledge/features/compaction_refactor_fidelity_and_fork_strategy.md
(§6 WP9).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    RemoveMessage,
    SystemMessage,
)

from agent.api.persistent_app import _db_rows_to_lc_messages, _select_turn_messages
from agent.core.context import (
    SUMMARY_CONTINUE,
    SUMMARY_LEAD_IN,
    ContextConfig,
    ContextManager,
    _conversation_count,
    extract_summary_text,
    make_summary_message,
    summary_text,
)
from agent.core.thread_messages import _serialize_message_row
from agent.services.memory.extraction_engine import MemoryExtractionEngine
from agent.services.memory.query import build_exchange_query_text
from shared.runtime.core.context_entries import (
    fold_context_entries,
    last_user_text,
    make_context_entry,
)
from shared.runtime.core.message_markers import (
    COMPACTION_SUMMARY_KEY,
    PERSIST_ROLE_EVENT,
    PERSIST_ROLE_KEY,
    is_compaction_summary,
)
from shared.runtime.services.auxiliary import (
    AuxiliaryLLM,
    _format_messages_for_extraction,
)

CHECKPOINT = "## Objective\n- Ship the CSV export.\n\n## Work State\n- Tests pass."


def _aux() -> AuxiliaryLLM:
    llm = MagicMock()
    llm.ainvoke = AsyncMock(
        return_value=AIMessage(
            content=CHECKPOINT, response_metadata={"finish_reason": "stop"}
        )
    )
    return AuxiliaryLLM(llm=llm, max_context_tokens=15_000)


def _manager() -> ContextManager:
    return ContextManager(
        config=ContextConfig(
            compaction_threshold_tokens=1000,
            summarization_threshold_tokens=1000,
            message_count_threshold=10,
            message_count_min_tokens=500,
            keep_recent_messages=3,
            model_max_context_tokens=2000,
        ),
        model="gpt-4",
    )


def _history(n: int = 12) -> list:
    return [
        HumanMessage(content=f"user {i} " + "x" * 400, id=f"h{i}")
        if i % 2 == 0
        else AIMessage(content=f"assistant {i} " + "y" * 400, id=f"a{i}")
        for i in range(n)
    ]


def _kept(result: list) -> list:
    return [m for m in result if not isinstance(m, RemoveMessage)]


class TestTheHandBack:
    def test_wrapper_and_markers(self):
        msg = make_summary_message("## Objective\n- X", message_id="s1")
        assert isinstance(msg, HumanMessage)
        assert msg.id == "s1"
        assert (
            msg.content
            == f"{SUMMARY_LEAD_IN}\n\n## Objective\n- X\n\n{SUMMARY_CONTINUE}"
        )
        assert msg.additional_kwargs[COMPACTION_SUMMARY_KEY] is True
        assert msg.additional_kwargs[PERSIST_ROLE_KEY] == PERSIST_ROLE_EVENT
        assert is_compaction_summary(msg)
        assert summary_text(msg) == "## Objective\n- X"

    def test_legacy_form_is_still_a_summary(self):
        legacy = SystemMessage(content="[Summary of prior work]\nold recap")
        assert is_compaction_summary(legacy)
        assert summary_text(legacy) == "old recap"
        assert extract_summary_text([legacy]) == "old recap"

    def test_plain_messages_are_not_summaries(self):
        assert not is_compaction_summary(HumanMessage(content=SUMMARY_LEAD_IN))
        assert not is_compaction_summary(SystemMessage(content="You are an agent."))
        assert not is_compaction_summary(
            HumanMessage(content="[Summary of prior work]\ntyped by a user")
        )

    def test_folded_request_copy_keeps_marker_and_text(self):
        """A context entry after the summary folds into its request copy."""
        summary = make_summary_message("S")
        entry = make_context_entry("memory", "[m:1] fact", section="memory")
        folded = fold_context_entries([SystemMessage(content="prompt"), summary, entry])
        assert len(folded) == 2
        copy = folded[1]
        assert copy is not summary and summary.content.endswith(SUMMARY_CONTINUE)
        assert is_compaction_summary(copy)
        assert summary_text(copy) == "S"


class TestCompaction:
    @pytest.mark.asyncio
    async def test_summary_is_a_user_message_ahead_of_the_kept_window(self):
        result = await _manager().summarize_and_compact(_history(), _aux())
        kept = _kept(result)
        assert isinstance(kept[0], HumanMessage)
        assert is_compaction_summary(kept[0])
        assert summary_text(kept[0]) == CHECKPOINT
        assert not any(isinstance(m, SystemMessage) for m in kept)
        assert [m.content[:6] for m in kept[1:]] == ["assist", "user 1", "assist"]

    @pytest.mark.asyncio
    async def test_legacy_summary_merges_and_is_replaced(self):
        """An old checkpoint's SystemMessage summary re-compacts correctly."""
        legacy = SystemMessage(content="[Summary of prior work]\nOLD RECAP", id="old")
        aux = _aux()
        result = await _manager().summarize_and_compact([legacy, *_history()], aux)

        assert RemoveMessage(id="old") in result
        summaries = [m for m in _kept(result) if is_compaction_summary(m)]
        assert len(summaries) == 1 and isinstance(summaries[0], HumanMessage)
        # The legacy summary seeded the fold under the merge contract.
        sent = aux.llm.ainvoke.await_args.args[0]
        assert "OLD RECAP" in str(sent[-1].content)

    @pytest.mark.asyncio
    async def test_second_compaction_merges_the_first(self):
        manager, aux = _manager(), _aux()
        first = _kept(await manager.summarize_and_compact(_history(), aux))
        first[0].id = "sum-1"  # a reducer would have given it one
        grown = first + _history(8)
        result = await manager.summarize_and_compact(grown, aux)
        assert RemoveMessage(id="sum-1") in result
        assert sum(is_compaction_summary(m) for m in _kept(result)) == 1

    def test_summary_is_not_counted_as_conversation(self):
        summary = make_summary_message("S")
        assert _conversation_count([summary, HumanMessage("q"), AIMessage("a")]) == 2


class TestNotTheUserSpeaking:
    def test_last_user_text_skips_the_summary(self):
        """A worker compacting mid-phase: the summary is the newest Human."""
        messages = [HumanMessage(content="the brief"), make_summary_message("S")]
        assert last_user_text(messages) == "the brief"
        assert last_user_text([make_summary_message("S")]) == ""

    def test_exchange_query_skips_the_summary(self):
        messages = [
            HumanMessage(content="what is the deploy tool?"),
            AIMessage(content="Fleet."),
            make_summary_message("S"),
        ]
        assert build_exchange_query_text(messages) == (
            "what is the deploy tool?\n\nFleet."
        )

    def test_memory_extraction_never_sees_the_summary(self):
        messages = [make_summary_message("S"), HumanMessage(content="I prefer tabs")]
        text = _format_messages_for_extraction(messages)
        assert text == "[User] I prefer tabs"
        engine = MemoryExtractionEngine.__new__(MemoryExtractionEngine)
        assert engine._format(messages) == ["[User] I prefer tabs"]


class TestSessionPersistence:
    def test_a_stray_persist_is_an_event_row_not_a_user_bubble(self):
        row = _serialize_message_row(make_summary_message("S", message_id="s1"), 3)
        assert row["role"] == "event"

    def test_turn_reconcile_walk_skips_the_summary(self):
        """Pinned walk: the summary is not the turn boundary and is not saved."""
        user = HumanMessage(content="question", id="u1")
        answer = AIMessage(content="answer", id="a1")
        messages = [user, make_summary_message("S", message_id="s1"), answer]
        saved = _select_turn_messages(
            messages,
            4,
            authoritative_turn_boundary=False,
            turn_input_message_id=None,
        )
        assert saved == [answer]

    def test_restored_event_row_gets_its_marker_back(self):
        """A forked child's seed saved the summary as an 'event' row."""
        summary = make_summary_message("S")
        rows = [
            {"id": "s1", "role": "event", "content": summary.content},
            {"id": "e1", "role": "event", "content": "Job 7 finished."},
        ]
        restored = _db_rows_to_lc_messages(rows)
        assert is_compaction_summary(restored[0]) and restored[0].id == "s1"
        assert summary_text(restored[0]) == "S"
        assert not is_compaction_summary(restored[1])
