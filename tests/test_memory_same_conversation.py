"""Same-conversation memories at the take-in (append-only context injection P4).

Design: the deferred test in the vault,
knowledge-base/knowledge/tests/aoci_same_conversation_memory_reinjection_test.md.
``memory.retrieval.same_conversation`` is its switch: ``allow`` (arm A, the
default) keeps a memory the observer extracted from the current job or
session eligible; ``skip_in_context`` (arm B) drops it while its source turns
are still in context and lets it in again once a compaction removed them.

Pinned here: the rule (``agent.services.memory.same_conversation``), the
manager's take-in (every result passes there: request retrieval, idle
prefetch, a pending set read back from the thread), the config key through
the loader, and where a session's compaction boundary comes from (the
checkpoint writer and both restore paths). The graph wiring is pinned in
test_execute_prepared_layout.py (worker) and test_persistent_append_only.py
(session).
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from agent.services.memory import (
    AssembleRequest,
    InjectionBlock,
    MemoryPayload,
    RetrievalResult,
)
from agent.services.memory.pending_set import (
    deserialize_pending_set,
    serialize_pending_set,
)
from agent.services.memory.same_conversation import (
    ConversationTurns,
    conversation_key,
    same_conversation_mode,
    session_conversation_turns,
    skipped_count,
    source_in_context,
    worker_conversation_turns,
)
from shared.runtime.core.context_entries import make_context_entry
from shared.runtime.core.loader import (
    SAME_CONVERSATION_ALLOW,
    SAME_CONVERSATION_SKIP_IN_CONTEXT,
    MemoryConfig,
    MemoryRetrievalConfig,
    _parse_memory_config,
    deep_merge,
    load_agent_config,
    load_agent_config_from_dict,
    load_config_from_resolved,
    load_role_base,
    serialize_resolved_config,
)
from shared.runtime.services.recall_store import MemoryRecord
from tests._memory_fixtures import GatedRetriever, make_async_manager, settle_retrieval

REPO_ROOT = Path(__file__).resolve().parent.parent
THREAD = "0f0e0d0c-0b0a-4908-8706-050403020100"
OTHER = "11111111-2222-4333-8444-555555555555"


def _memory(
    n: int, *, job: Any = THREAD, start: Any = None, end: Any = None
) -> MemoryRecord:
    return MemoryRecord(
        id=UUID(int=n),
        job_id=UUID(job) if isinstance(job, str) else job,
        content=f"memory {n}",
        importance=0.7,
        token_count=5,
        source_turn_start=start,
        source_turn_end=end,
    )


#: Turn 10 of a conversation whose latest compaction removed turns 1-6.
TURNS = ConversationTurns(compacted_through=6, current=10)
IN_CONTEXT = _memory(1, start=5, end=8)
EVICTED = _memory(2, start=1, end=6)
OTHER_JOB = _memory(3, job=OTHER, start=5, end=8)
UNSTAMPED = _memory(4)


def _arm_b() -> MemoryConfig:
    return MemoryConfig(
        retrieval=MemoryRetrievalConfig(
            same_conversation=SAME_CONVERSATION_SKIP_IN_CONTEXT
        )
    )


def _manager(memories: List[MemoryRecord], memory_config: Any = None):
    manager = make_async_manager(GatedRetriever(memories, auto=True), job_id=THREAD)
    manager.runtime.memory_config = memory_config
    return manager


async def _finished(manager) -> None:
    manager.start_retrieval(AssembleRequest(query_text="q"))
    await settle_retrieval(manager)


def _ids(result: RetrievalResult) -> List[UUID]:
    return [record.id for record in result.records("memory")]


# ---------------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------------


class TestSourceInContext:
    def test_turns_after_the_boundary_are_in_context(self):
        assert source_in_context(IN_CONTEXT, TURNS) is True

    def test_turns_a_compaction_removed_are_not(self):
        assert source_in_context(EVICTED, TURNS) is False
        assert source_in_context(_memory(5, end=7), TURNS) is True

    def test_the_end_turn_decides_and_the_start_stands_in_for_it(self):
        # The extraction window ends at the turn it ran on.
        assert source_in_context(_memory(5, start=2, end=7), TURNS) is True
        assert source_in_context(_memory(5, start=7), TURNS) is True
        assert source_in_context(_memory(5, start=6), TURNS) is False

    def test_nothing_compacted_keeps_every_turn_in_context(self):
        turns = ConversationTurns(compacted_through=0, current=10)
        assert source_in_context(EVICTED, turns) is True
        assert source_in_context(_memory(5, end=1), turns) is True

    def test_a_turn_past_the_current_one_is_an_earlier_run(self):
        # A rewind or a restart without the checkpoint: not in this history.
        assert source_in_context(_memory(5, end=12), TURNS) is False
        unknown = ConversationTurns(compacted_through=6, current=None)
        assert source_in_context(_memory(5, end=12), unknown) is True

    def test_an_unstamped_memory_follows_the_first_compaction(self):
        assert source_in_context(UNSTAMPED, ConversationTurns(0, 10)) is True
        assert source_in_context(UNSTAMPED, TURNS) is False


class TestConversationTurns:
    def test_worker_boundary_is_turns_minus_the_responses_left(self):
        history = [
            HumanMessage(content="summary of turns 1-7"),
            AIMessage(content="", tool_calls=[{"name": "t", "args": {}, "id": "a"}]),
            ToolMessage(content="ok", tool_call_id="a"),
            AIMessage(content="done"),
            # Injected context is no response.
            make_context_entry("memory", "x", section="memory"),
            AIMessage(
                content="",
                tool_calls=[{"name": "m", "args": {}, "id": "memory_inject_1"}],
            ),
        ]
        assert worker_conversation_turns(history, 9) == ConversationTurns(7, 9)

    def test_worker_without_compaction_starts_at_zero(self):
        history = [AIMessage(content=str(i)) for i in range(3)]
        assert worker_conversation_turns(history, 3) == ConversationTurns(0, 3)
        # A synthetic "Phase complete" note on top never goes negative.
        history.append(AIMessage(content="Phase complete."))
        assert worker_conversation_turns(history, 3) == ConversationTurns(0, 3)

    def test_session_reads_the_boundary_the_transport_recorded(self):
        ctx = SimpleNamespace(compaction_boundary_turn=4)
        assert session_conversation_turns(ctx, 6) == ConversationTurns(4, 6)
        assert session_conversation_turns(
            SimpleNamespace(compaction_boundary_turn=None), 6
        ) == ConversationTurns(0, 6)
        # A mock context manager (no recorded boundary) and an unknown turn.
        assert session_conversation_turns(MagicMock(), 0) == ConversationTurns(0, None)

    def test_conversation_key_matches_uuid_and_text(self):
        assert conversation_key(UUID(THREAD)) == conversation_key(THREAD.upper())
        assert conversation_key("layout-job-1") == "layout-job-1"
        assert conversation_key(None) is None


class TestMode:
    def test_default_and_unknown_values_are_allow(self):
        assert same_conversation_mode(MemoryConfig()) == SAME_CONVERSATION_ALLOW
        assert same_conversation_mode(None) == SAME_CONVERSATION_ALLOW
        assert same_conversation_mode(MagicMock()) == SAME_CONVERSATION_ALLOW
        assert same_conversation_mode(_arm_b()) == SAME_CONVERSATION_SKIP_IN_CONTEXT


# ---------------------------------------------------------------------------
# The manager's take-in
# ---------------------------------------------------------------------------


class TestTakeIn:
    @pytest.mark.asyncio
    async def test_arm_a_hands_out_every_memory_unchanged(self):
        manager = _manager([IN_CONTEXT, EVICTED, OTHER_JOB, UNSTAMPED], MemoryConfig())
        await _finished(manager)

        result = manager.take_retrieval(TURNS)

        assert _ids(result) == [
            m.id for m in (IN_CONTEXT, EVICTED, OTHER_JOB, UNSTAMPED)
        ]
        assert "same_conversation" not in result.payload.stats.extra
        assert manager.retrieval_stats()["same_conversation_skipped"] == 0

    @pytest.mark.asyncio
    async def test_arm_b_skips_this_conversations_memory_still_in_context(self):
        manager = _manager([IN_CONTEXT, EVICTED, OTHER_JOB, UNSTAMPED], _arm_b())
        await _finished(manager)

        result = manager.take_retrieval(TURNS)

        # Removed by compaction, other conversations, and an unstamped one
        # after the first compaction stay; the in-context one goes.
        assert _ids(result) == [m.id for m in (EVICTED, OTHER_JOB, UNSTAMPED)]
        (block,) = result.payload.blocks
        assert [item["record_id"] for item in block.items] == [
            str(m.id) for m in (EVICTED, OTHER_JOB, UNSTAMPED)
        ]
        assert block.token_count == 15
        assert result.payload.stats.extra["same_conversation"] == {
            "mode": "skip_in_context",
            "compacted_through": 6,
            "current_turn": 10,
            "same_conversation": 3,
            "skipped": 1,
        }
        assert skipped_count(result.payload.stats) == 1
        assert manager.retrieval_stats()["same_conversation_skipped"] == 1

    @pytest.mark.asyncio
    async def test_arm_b_without_a_position_counts_nothing_as_compacted(self):
        manager = _manager([IN_CONTEXT, EVICTED, OTHER_JOB, UNSTAMPED], _arm_b())
        await _finished(manager)

        result = manager.take_retrieval()

        assert _ids(result) == [OTHER_JOB.id]
        assert skipped_count(result.payload.stats) == 3

    def test_arm_b_leaves_knowledge_alone_and_reports_a_zero_skip(self):
        manager = _manager([OTHER_JOB], _arm_b())
        note = SimpleNamespace(note_id="n1", project_id=None, content="note")
        manager._pending_retrieval = RetrievalResult(
            payload=MemoryPayload(
                blocks=[
                    InjectionBlock(kind="memory", records=[OTHER_JOB]),
                    InjectionBlock(kind="knowledge", records=[note]),
                ]
            )
        )

        result = manager.take_retrieval(TURNS)

        assert result.records("knowledge") == [note]
        assert _ids(result) == [OTHER_JOB.id]
        assert result.payload.stats.extra["same_conversation"]["skipped"] == 0

    @pytest.mark.asyncio
    async def test_a_stored_config_without_the_key_behaves_as_arm_a(self):
        stored = dataclasses.asdict(MemoryConfig())
        del stored["retrieval"]
        manager = _manager([IN_CONTEXT], _parse_memory_config(stored))
        await _finished(manager)

        assert _ids(manager.take_retrieval(TURNS)) == [IN_CONTEXT.id]

    @pytest.mark.asyncio
    async def test_a_failing_filter_hands_the_result_out_unfiltered(self, caplog):
        manager = _manager([IN_CONTEXT], _arm_b())
        await _finished(manager)

        with patch(
            "agent.services.memory.manager.skip_in_context",
            side_effect=RuntimeError("boom"),
        ):
            result = manager.take_retrieval(TURNS)

        assert _ids(result) == [IN_CONTEXT.id]
        assert "Same-conversation memory filter failed" in caplog.text

    @pytest.mark.asyncio
    async def test_the_idle_prefetch_is_filtered_where_it_is_taken_in(self):
        manager = _manager([IN_CONTEXT, EVICTED, OTHER_JOB], _arm_b())

        outcome = await manager.prefetch(
            AssembleRequest(query_text="exchange", retries=False), budget_s=5.0
        )
        assert outcome.memories == 3  # the pending set is unfiltered

        result = manager.take_retrieval(TURNS)
        assert result.source == "prefetch"
        assert _ids(result) == [EVICTED.id, OTHER_JOB.id]

    def test_a_pending_set_read_back_keeps_its_provenance(self):
        """The stateless lane takes the set in from the thread in another
        process: job id and source turns survive the stored form."""
        manager = _manager([], _arm_b())
        stored = serialize_pending_set(
            RetrievalResult(
                payload=MemoryPayload(
                    blocks=[
                        InjectionBlock(
                            kind="memory", records=[IN_CONTEXT, EVICTED, OTHER_JOB]
                        )
                    ]
                ),
                source="prefetch",
                pending_id="set-1",
            ),
            turn=9,
        )
        stored = json.loads(json.dumps(stored))
        assert stored["memory"][0]["job_id"] == THREAD
        assert stored["memory"][0]["source_turn_end"] == 8

        assert manager.seed_prefetch(deserialize_pending_set(stored)) is True
        result = manager.take_retrieval(TURNS)

        assert _ids(result) == [EVICTED.id, OTHER_JOB.id]
        assert result.records("memory")[0].job_id == UUID(THREAD)


# ---------------------------------------------------------------------------
# The config key through the loader
# ---------------------------------------------------------------------------


class TestConfigKey:
    def test_the_role_bases_default_to_allow(self):
        for name in ("worker_base", "session_base"):
            config = load_agent_config(str(REPO_ROOT / "config" / f"{name}.yaml"))
            assert config.memory.retrieval.same_conversation == "allow"

    def test_an_override_selects_arm_b_and_survives_the_frozen_config(self):
        data = deep_merge(
            load_role_base("worker"),
            {"memory": {"retrieval": {"same_conversation": "skip_in_context"}}},
        )
        config = load_agent_config_from_dict(data)
        assert config.memory.retrieval.same_conversation == "skip_in_context"

        # Dispatch round-trips a live config through asdict + re-parse.
        again = _parse_memory_config(dataclasses.asdict(config.memory))
        assert again.retrieval == config.memory.retrieval

        with (
            patch("shared.runtime.core.loader.PromptMatrixResolver") as prompts,
            patch("shared.runtime.core.loader.InstructionMatrixResolver") as instr,
        ):
            prompts.return_value.load.return_value = "prompt"
            instr.return_value.load.return_value = "instruction"
            resolved = serialize_resolved_config(config, model="gpt-4o")
        resolved = json.loads(json.dumps(resolved, default=str))
        assert resolved["agent"]["memory"]["retrieval"] == {
            "same_conversation": "skip_in_context"
        }
        restored = load_config_from_resolved(resolved)
        assert restored.memory.retrieval.same_conversation == "skip_in_context"

    def test_a_frozen_config_from_before_the_key_is_arm_a(self):
        assert _parse_memory_config({}).retrieval.same_conversation == "allow"
        assert (
            _parse_memory_config({"retrieval": None}).retrieval.same_conversation
            == "allow"
        )
        assert (
            _parse_memory_config(
                {"retrieval": {"same_conversation": None}}
            ).retrieval.same_conversation
            == "allow"
        )

    @pytest.mark.parametrize(
        "value", [{"same_conversation": "skip"}, {"same_conversation": True}, "allow"]
    )
    def test_an_unknown_value_fails_loud(self, value):
        with pytest.raises(ValueError, match="memory.retrieval"):
            _parse_memory_config({"retrieval": value})


# ---------------------------------------------------------------------------
# A session's boundary: the checkpoint writer and the restore paths
# ---------------------------------------------------------------------------


class TestSessionBoundary:
    @pytest.mark.asyncio
    async def test_the_checkpoint_writer_records_its_boundary_turn(self):
        from agent.api import persistent_app as pa

        session = MagicMock()
        session.turn_count = 5
        session.context_manager = SimpleNamespace(
            _last_compaction_boundary_id=None, compaction_boundary_turn=None
        )
        session.postgres_conn.save_thread_message = AsyncMock(return_value={})
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            await pa._record_compaction("summary", 50, 8, trigger="auto", ws=None)

        metrics = session.postgres_conn.save_thread_message.call_args.kwargs["metrics"]
        assert metrics["boundary_turn"] == 4
        assert session.context_manager.compaction_boundary_turn == 4

    async def _restore(self, ckpt, rows):
        from agent.api import persistent_app as pa

        session = MagicMock()
        session.messages = []
        session.auxiliary_llm = None  # no re-bound pass
        session.context_manager = SimpleNamespace(compaction_boundary_turn=None)
        agent = MagicMock()
        agent.postgres_conn.get_latest_compaction_checkpoint = AsyncMock(
            return_value=ckpt
        )
        agent.postgres_conn.get_thread_messages_history = AsyncMock(return_value=rows)
        with (
            patch.object(pa, "_session", session),
            patch.object(pa, "_agent", agent),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            await pa._restore_session_messages()
        return session.context_manager.compaction_boundary_turn

    @staticmethod
    def _rows(*turns: int) -> List[dict]:
        return [
            {
                "role": "user",
                "content": f"turn {t}",
                "tool_calls": None,
                "turn_number": t,
            }
            for t in turns
        ]

    @pytest.mark.asyncio
    async def test_path_a_takes_the_checkpoints_boundary(self):
        ckpt = {"summary": "S", "boundary_turn": 5, "boundary_seq": 70}
        assert await self._restore(ckpt, self._rows(5, 6)) == 5

    @pytest.mark.asyncio
    async def test_a_floored_tail_moves_the_boundary_to_its_first_row(self):
        ckpt = {"summary": "S", "boundary_turn": 5, "boundary_seq": 70}
        assert await self._restore(ckpt, self._rows(9, 10)) == 8

    @pytest.mark.asyncio
    async def test_path_b_full_log_is_uncompacted(self):
        assert await self._restore(None, self._rows(1, 2, 3)) == 0
        # The resume floor dropped turns 1-3.
        assert await self._restore(None, self._rows(4, 5)) == 3
