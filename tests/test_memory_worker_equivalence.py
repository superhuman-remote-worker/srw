"""The worker's memory read path through the MemoryManager.

The `recall_hybrid` + `kb_notes` retrievers reproduce the store calls of the
worker's pre-manager read path (graph.py's execute node): the same query, the
same arguments, the same containment. The manager no longer renders blocks:
each kind's block carries its store records, which the append-only planner
renders as context entries (agent.core.context_injection).
"""

import uuid as uuid_module
from unittest.mock import call

import pytest

from shared.runtime.core.loader import MemoryConfig, MemoryPipelineConfig
from agent.services.memory import (
    AssembleRequest,
    MemoryManager,
    MemoryRuntime,
    TaskFrame,
    available_memory_plugins,
)
from agent.services.memory.plugins.legacy import build_worker_query_text
from tests._memory_fixtures import (
    PROJECT_ID,
    make_kb_mock,
    make_memories,
    make_notes,
    make_recall_mock,
)


def build_manager(
    recall_store=None, knowledge_store=None, project_id=None, project_ids=None
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
    )
    return MemoryManager.from_config(cfg, runtime)


def worker_request(
    top_todo="Implement the config parser",
    phase_number=3,
    is_strategic=False,
):
    frame = TaskFrame(
        top_todo=top_todo, phase_number=phase_number, is_strategic=is_strategic
    )
    return AssembleRequest(
        query_text=build_worker_query_text(frame),
        task_frame=frame,
        budget_tokens=10000,
    )


def records_by_kind(payload):
    return {block.kind: block.records for block in payload.blocks}


class TestQueryFormation:
    def test_tactical_with_todo(self):
        frame = TaskFrame(
            top_todo="Implement the config parser", phase_number=3, is_strategic=False
        )
        assert (
            build_worker_query_text(frame)
            == "Implement the config parser phase 3 tactical"
        )

    def test_strategic_without_todo(self):
        frame = TaskFrame(top_todo=None, phase_number=5, is_strategic=True)
        assert build_worker_query_text(frame) == "phase 5 strategic"


class TestRegistration:
    def test_legacy_retrievers_registered_on_package_import(self):
        retrievers = available_memory_plugins("retriever")["retriever"]
        assert "recall_hybrid" in retrievers
        assert "recall_two_tier" in retrievers  # the retired name, an alias
        assert "kb_notes" in retrievers


class TestWorkerReadPath:
    @pytest.mark.asyncio
    async def test_blocks_carry_the_store_records_in_rank_order(self):
        memories, notes = make_memories(), make_notes()
        manager = build_manager(
            recall_store=make_recall_mock(memories),
            knowledge_store=make_kb_mock(notes),
            project_id=PROJECT_ID,
        )
        payload = await manager.assemble(worker_request())

        assert [b.kind for b in payload.blocks] == ["memory", "knowledge"]
        assert records_by_kind(payload) == {"memory": memories, "knowledge": notes}
        assert payload.stats.errors == []

    @pytest.mark.asyncio
    async def test_store_call_signatures(self):
        recall = make_recall_mock(make_memories())
        kb = make_kb_mock(make_notes())
        manager = build_manager(
            recall_store=recall, knowledge_store=kb, project_id=PROJECT_ID
        )
        await manager.assemble(worker_request())

        # retrieve only (no TTL tick, D36), positional with no budget kwarg
        assert recall.mock_calls == [
            call.retrieve("Implement the config parser phase 3 tactical"),
        ]
        # hybrid_search normalizes project_ids=[single] to the same SQL
        # path as the legacy worker's project_id=single call
        kb.hybrid_search.assert_awaited_once_with(
            project_ids=[uuid_module.UUID(PROJECT_ID)],
            query="Implement the config parser phase 3 tactical",
            match_count=5,
        )


class TestConditionalBlocks:
    @pytest.mark.asyncio
    async def test_no_recall_store_yields_kb_only(self):
        manager = build_manager(
            recall_store=None,
            knowledge_store=make_kb_mock(make_notes()),
            project_id=PROJECT_ID,
        )
        payload = await manager.assemble(worker_request())
        assert [b.kind for b in payload.blocks] == ["knowledge"]

    @pytest.mark.asyncio
    async def test_no_project_scope_yields_memory_only(self):
        kb = make_kb_mock(make_notes())
        manager = build_manager(
            recall_store=make_recall_mock(make_memories()),
            knowledge_store=kb,
            project_id=None,
        )
        payload = await manager.assemble(worker_request())
        assert [b.kind for b in payload.blocks] == ["memory"]
        kb.hybrid_search.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_results_yield_no_blocks(self):
        manager = build_manager(
            recall_store=make_recall_mock([]),
            knowledge_store=make_kb_mock([]),
            project_id=PROJECT_ID,
        )
        payload = await manager.assemble(worker_request())
        assert payload.blocks == []

    @pytest.mark.asyncio
    async def test_retrieval_failure_contained_kb_proceeds(self):
        manager = build_manager(
            recall_store=make_recall_mock(
                retrieve_error=RuntimeError("embedding down")
            ),
            knowledge_store=make_kb_mock(make_notes()),
            project_id=PROJECT_ID,
        )
        payload = await manager.assemble(worker_request())
        assert [b.kind for b in payload.blocks] == ["knowledge"]
        assert payload.stats.errors == [
            "retriever:recall_hybrid: RuntimeError: embedding down"
        ]


class TestStats:
    @pytest.mark.asyncio
    async def test_counts_and_tokens(self):
        manager = build_manager(
            recall_store=make_recall_mock(make_memories()),
            knowledge_store=make_kb_mock(make_notes()),
            project_id=PROJECT_ID,
        )
        payload = await manager.assemble(worker_request())
        stats = payload.stats
        assert stats.candidates_total == 4
        assert stats.per_retriever == {"recall_hybrid": 2, "kb_notes": 2}
        assert stats.blocks == 2
        assert stats.injected_total == 4
        # KB candidates carry token_count=0 (the legacy KB block is
        # uncapped/uncounted — B5, unified budget arrives in Phase 3)
        assert stats.tokens_injected == 26
