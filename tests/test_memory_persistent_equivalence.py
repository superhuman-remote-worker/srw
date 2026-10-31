"""The session's memory read path through the MemoryManager.

The `recall_hybrid` + `kb_notes` retrievers reproduce the store calls of the
session's pre-manager read path (persistent_graph.py's turn start): the
latest HumanMessage as the query, the same arguments, the same per-call 5 s
guard and containment. The manager no longer renders blocks: each kind's
block carries its store records, which the append-only planner renders as
context entries (agent.core.context_injection).
"""

import asyncio
import uuid as uuid_module

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from shared.runtime.core.loader import MemoryConfig, MemoryPipelineConfig
from agent.services.memory import AssembleRequest, MemoryManager, MemoryRuntime
from agent.services.memory.plugins.legacy import (
    _bounded,
    build_persistent_query_text,
)
from tests._memory_fixtures import (
    PROJECT_ID,
    make_kb_mock,
    make_memories,
    make_notes,
    make_recall_mock,
)

PROJECT_ID_2 = "87654321-4321-8765-4321-876543218765"

# Fast-but-safe stand-ins for the 5 s per-call guard in timeout tests
SHORT_TIMEOUT = 0.05
HANG = 5.0


def make_conversation():
    return [
        SystemMessage(content="You are a persistent agent."),
        HumanMessage(content="Set up the dev cluster."),
        AIMessage(content="Done."),
        HumanMessage(content="Now check the rclone mounts."),
        AIMessage(content="", tool_calls=[{"name": "run", "args": {}, "id": "t1"}]),
        ToolMessage(content="ok", tool_call_id="t1"),
    ]


def slow(result, delay=HANG):
    async def _slow(*args, **kwargs):
        await asyncio.sleep(delay)
        return result

    return _slow


def build_persistent_manager(
    recall_store=None,
    knowledge_store=None,
    project_id=None,
    project_ids=None,
    timeout=5.0,
):
    cfg = MemoryConfig(
        enabled=True,
        pipeline=MemoryPipelineConfig(retrievers=["recall_hybrid", "kb_notes"]),
    )
    runtime = MemoryRuntime(
        recall_store=recall_store,
        knowledge_store=knowledge_store,
        project_id=project_id,
        project_ids=list(project_ids or []),
        retrieval_timeout=timeout,
    )
    return MemoryManager.from_config(cfg, runtime)


def persistent_request(messages):
    return AssembleRequest(
        query_text=build_persistent_query_text(messages),
        task_frame=None,
        budget_tokens=10000,
    )


class TestPersistentQueryFormation:
    def test_latest_human_message_wins(self):
        assert (
            build_persistent_query_text(make_conversation())
            == "Now check the rclone mounts."
        )

    def test_multimodal_content_str_coerced(self):
        content = [{"type": "text", "text": "look at this"}]
        messages = [HumanMessage(content=content)]
        assert build_persistent_query_text(messages) == str(content)

    def test_no_human_message_returns_empty(self):
        assert build_persistent_query_text([]) == ""
        assert build_persistent_query_text([AIMessage(content="hi")]) == ""


class TestPersistentReadPath:
    @pytest.mark.asyncio
    async def test_blocks_carry_the_store_records_in_rank_order(self):
        memories, notes = make_memories(), make_notes()
        manager = build_persistent_manager(
            recall_store=make_recall_mock(memories),
            knowledge_store=make_kb_mock(notes),
            project_ids=[PROJECT_ID],
        )
        payload = await manager.assemble(persistent_request(make_conversation()))

        assert {b.kind: b.records for b in payload.blocks} == {
            "memory": memories,
            "knowledge": notes,
        }
        assert payload.stats.errors == []

    @pytest.mark.asyncio
    async def test_store_calls(self):
        recall = make_recall_mock(make_memories())
        kb = make_kb_mock(make_notes())
        manager = build_persistent_manager(
            recall_store=recall,
            knowledge_store=kb,
            project_ids=[PROJECT_ID, PROJECT_ID_2],
        )
        await manager.assemble(persistent_request(make_conversation()))

        recall.retrieve.assert_awaited_once_with("Now check the rclone mounts.")
        # multi-project goes through as the full UUID list
        kb.hybrid_search.assert_awaited_once_with(
            project_ids=[uuid_module.UUID(PROJECT_ID), uuid_module.UUID(PROJECT_ID_2)],
            query="Now check the rclone mounts.",
            match_count=5,
        )

    @pytest.mark.asyncio
    async def test_single_project_via_project_ids_list(self):
        """Persistent passes project_ids=[one] — the worker's singular
        project_id= lands on the same store-internal normalization."""
        kb = make_kb_mock(make_notes())
        manager = build_persistent_manager(knowledge_store=kb, project_ids=[PROJECT_ID])
        await manager.assemble(persistent_request(make_conversation()))
        kb.hybrid_search.assert_awaited_once_with(
            project_ids=[uuid_module.UUID(PROJECT_ID)],
            query="Now check the rclone mounts.",
            match_count=5,
        )

    @pytest.mark.asyncio
    async def test_empty_query_still_retrieves(self):
        """No HumanMessage → retrieve with "" rather than skipping."""
        recall = make_recall_mock(make_memories())
        manager = build_persistent_manager(recall_store=recall)
        payload = await manager.assemble(
            persistent_request([AIMessage(content="solo")])
        )

        recall.retrieve.assert_awaited_once_with("")
        assert [b.kind for b in payload.blocks] == ["memory"]


class TestTimeouts:
    @pytest.mark.asyncio
    async def test_memory_timeout_skips_memory_kb_proceeds(self):
        recall = make_recall_mock()
        recall.retrieve.side_effect = slow(make_memories())
        manager = build_persistent_manager(
            recall_store=recall,
            knowledge_store=make_kb_mock(make_notes()),
            project_ids=[PROJECT_ID],
            timeout=SHORT_TIMEOUT,
        )
        payload = await manager.assemble(persistent_request(make_conversation()))

        assert [b.kind for b in payload.blocks] == ["knowledge"]
        assert payload.stats.errors == ["retriever:recall_hybrid: TimeoutError: "]

    @pytest.mark.asyncio
    async def test_kb_timeout_skips_kb_memory_proceeds(self):
        kb = make_kb_mock()
        kb.hybrid_search.side_effect = slow(make_notes())
        manager = build_persistent_manager(
            recall_store=make_recall_mock(make_memories()),
            knowledge_store=kb,
            project_ids=[PROJECT_ID],
            timeout=SHORT_TIMEOUT,
        )
        payload = await manager.assemble(persistent_request(make_conversation()))

        assert [b.kind for b in payload.blocks] == ["memory"]
        assert payload.stats.errors == ["retriever:kb_notes: TimeoutError: "]

    @pytest.mark.asyncio
    async def test_bounded_none_applies_no_timeout(self, monkeypatch):
        """Worker mode (retrieval_timeout=None) must not touch wait_for."""

        def _forbidden(*args, **kwargs):  # pragma: no cover - guard
            raise AssertionError("wait_for must not be called when timeout is None")

        monkeypatch.setattr(asyncio, "wait_for", _forbidden)

        async def _value():
            return 42

        assert await _bounded(_value(), None) == 42
