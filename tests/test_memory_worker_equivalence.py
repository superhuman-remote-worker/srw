"""Phase-1 slice-2 equivalence: MemoryManager payload ≡ legacy worker read path.

The legacy memory/KB injection sequence stays live in graph.py's execute
node until the Phase-1 cutover; these fixtures pin the transplanted
pipeline (`recall_hybrid` + `kb_notes` retrievers + the manager's
render mechanics) against it byte-for-byte. `run_legacy_worker_sequence`
below reproduces graph.py:888-1037 — if that block changes, this file
must change with it (it shouldn't: the old code is frozen until cutover,
see agent_memory_overhaul.md §5 Phase 1).

The synthetic tool_call_ids carry a random suffix, so comparisons
normalize ids down to their stable prefixes while separately asserting
pair-internal id consistency.
"""

import uuid as uuid_module
from unittest.mock import call

import pytest
from langchain_core.messages import ToolMessage

from agent.core.knowledge_injection import (
    KNOWLEDGE_TOOL_CALL_ID_PREFIX,
    create_knowledge_injection_messages,
)
from shared.runtime.core.loader import MemoryConfig, MemoryPipelineConfig
from agent.core.memory_injection import (
    MEMORY_TOOL_CALL_ID_PREFIX,
    create_memory_injection_messages,
)
from shared.runtime.services.knowledge_store import KnowledgeStore
from agent.services.memory import (
    AssembleRequest,
    MemoryManager,
    MemoryRuntime,
    TaskFrame,
    available_memory_plugins,
)
from agent.services.memory.plugins.legacy import build_worker_query_text
from shared.runtime.services.recall_store import RecallStore
from tests._memory_fixtures import (
    GOLDEN_KNOWLEDGE_BLOCK,
    GOLDEN_MEMORY_BLOCK,
    PROJECT_ID,
    make_kb_mock,
    make_memories,
    make_notes,
    make_recall_mock,
    normalize,
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
    model=None,
):
    frame = TaskFrame(
        top_todo=top_todo, phase_number=phase_number, is_strategic=is_strategic
    )
    return AssembleRequest(
        query_text=build_worker_query_text(frame),
        task_frame=frame,
        budget_tokens=10000,
        model=model,
    )


async def run_legacy_worker_sequence(
    recall_store,
    knowledge_store,
    project_id,
    *,
    top_todo="Implement the config parser",
    phase_number=3,
    is_strategic=False,
    model=None,
):
    """The pre-refactor read path, reproduced from graph.py:888-1037.

    Same calls, same arguments, same containment, same gating — only the
    surrounding graph plumbing (todo manager, archiver audit) stripped.
    """
    memory_block = ""
    if recall_store:
        try:
            context_parts = []
            if top_todo:
                context_parts.append(top_todo)
            context_parts.append(
                f"phase {phase_number} {'strategic' if is_strategic else 'tactical'}"
            )
            context_text = " ".join(context_parts)
            memories = await recall_store.retrieve(context_text)
            if memories:
                memory_block = RecallStore.assemble_memory_block(memories, model=model)
        except Exception:
            pass

    knowledge_block = ""
    if knowledge_store and project_id:
        try:
            project_uuid = (
                uuid_module.UUID(project_id)
                if isinstance(project_id, str)
                else project_id
            )
            kb_context_parts = []
            if top_todo:
                kb_context_parts.append(top_todo)
            kb_context_parts.append(
                f"phase {phase_number} {'strategic' if is_strategic else 'tactical'}"
            )
            kb_context_text = " ".join(kb_context_parts)
            kb_notes = await knowledge_store.hybrid_search(
                project_id=project_uuid,
                query=kb_context_text,
                match_count=5,
            )
            if kb_notes:
                knowledge_block = KnowledgeStore.assemble_knowledge_block(
                    kb_notes, model=model
                )
        except Exception:
            pass

    injected = []
    if memory_block:
        mem_ai, mem_tool = create_memory_injection_messages(memory_block)
        injected.extend([mem_ai, mem_tool])
    if knowledge_block:
        kb_ai, kb_tool = create_knowledge_injection_messages(knowledge_block)
        injected.extend([kb_ai, kb_tool])
    return injected


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


class TestWorkerEquivalence:
    @pytest.mark.asyncio
    async def test_full_payload_equals_legacy_sequence(self):
        legacy_messages = await run_legacy_worker_sequence(
            make_recall_mock(make_memories()), make_kb_mock(make_notes()), PROJECT_ID
        )
        manager = build_manager(
            recall_store=make_recall_mock(make_memories()),
            knowledge_store=make_kb_mock(make_notes()),
            project_id=PROJECT_ID,
        )
        payload = await manager.assemble(worker_request())

        assert normalize(payload.messages()) == normalize(legacy_messages)
        # The ToolMessage contents are the actual injected bytes
        legacy_contents = [
            m.content for m in legacy_messages if isinstance(m, ToolMessage)
        ]
        payload_contents = [
            m.content for m in payload.messages() if isinstance(m, ToolMessage)
        ]
        assert payload_contents == legacy_contents
        assert payload.stats.errors == []

    @pytest.mark.asyncio
    async def test_pair_ids_consistent_and_prefixed(self):
        manager = build_manager(
            recall_store=make_recall_mock(make_memories()),
            knowledge_store=make_kb_mock(make_notes()),
            project_id=PROJECT_ID,
        )
        payload = await manager.assemble(worker_request())
        mem_ai, mem_tool, kb_ai, kb_tool = payload.messages()

        assert mem_ai.tool_calls[0]["id"] == mem_tool.tool_call_id
        assert mem_tool.tool_call_id.startswith(MEMORY_TOOL_CALL_ID_PREFIX)
        assert mem_ai.tool_calls[0]["name"] == "recall_memories"

        assert kb_ai.tool_calls[0]["id"] == kb_tool.tool_call_id
        assert kb_tool.tool_call_id.startswith(KNOWLEDGE_TOOL_CALL_ID_PREFIX)
        assert kb_ai.tool_calls[0]["name"] == "kb_search"

    @pytest.mark.asyncio
    async def test_golden_block_snapshots(self):
        manager = build_manager(
            recall_store=make_recall_mock(make_memories()),
            knowledge_store=make_kb_mock(make_notes()),
            project_id=PROJECT_ID,
        )
        payload = await manager.assemble(worker_request())
        by_kind = {b.kind: b for b in payload.blocks}
        assert by_kind["memory"].content == GOLDEN_MEMORY_BLOCK
        assert by_kind["knowledge"].content == GOLDEN_KNOWLEDGE_BLOCK

    @pytest.mark.asyncio
    async def test_store_call_signatures_match_legacy(self):
        recall = make_recall_mock(make_memories())
        kb = make_kb_mock(make_notes())
        manager = build_manager(
            recall_store=recall, knowledge_store=kb, project_id=PROJECT_ID
        )
        req = worker_request()
        await manager.assemble(req)

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

    @pytest.mark.asyncio
    async def test_model_threaded_to_both_renderers(self, monkeypatch):
        seen = {}
        real_mem = RecallStore.assemble_memory_block.__func__
        real_kb = KnowledgeStore.assemble_knowledge_block.__func__

        def spy_mem(cls, records, budget_tokens=10000, model=None):
            seen["memory_model"] = model
            return real_mem(cls, records, budget_tokens=budget_tokens, model=model)

        def spy_kb(cls, records, model=None):
            seen["kb_model"] = model
            return real_kb(cls, records, model=model)

        monkeypatch.setattr(RecallStore, "assemble_memory_block", classmethod(spy_mem))
        monkeypatch.setattr(
            KnowledgeStore, "assemble_knowledge_block", classmethod(spy_kb)
        )

        manager = build_manager(
            recall_store=make_recall_mock(make_memories()),
            knowledge_store=make_kb_mock(make_notes()),
            project_id=PROJECT_ID,
        )
        await manager.assemble(worker_request(model="gpt-test-model"))
        assert seen == {"memory_model": "gpt-test-model", "kb_model": "gpt-test-model"}


class TestConditionalParity:
    @pytest.mark.asyncio
    async def test_no_recall_store_yields_kb_only(self):
        legacy_messages = await run_legacy_worker_sequence(
            None, make_kb_mock(make_notes()), PROJECT_ID
        )
        manager = build_manager(
            recall_store=None,
            knowledge_store=make_kb_mock(make_notes()),
            project_id=PROJECT_ID,
        )
        payload = await manager.assemble(worker_request())
        assert normalize(payload.messages()) == normalize(legacy_messages)
        assert [b.kind for b in payload.blocks] == ["knowledge"]

    @pytest.mark.asyncio
    async def test_no_project_scope_yields_memory_only(self):
        legacy_messages = await run_legacy_worker_sequence(
            make_recall_mock(make_memories()), make_kb_mock(make_notes()), None
        )
        manager = build_manager(
            recall_store=make_recall_mock(make_memories()),
            knowledge_store=make_kb_mock(make_notes()),
            project_id=None,
        )
        payload = await manager.assemble(worker_request())
        assert normalize(payload.messages()) == normalize(legacy_messages)
        assert [b.kind for b in payload.blocks] == ["memory"]

    @pytest.mark.asyncio
    async def test_empty_results_inject_nothing(self):
        legacy_messages = await run_legacy_worker_sequence(
            make_recall_mock([]), make_kb_mock([]), PROJECT_ID
        )
        manager = build_manager(
            recall_store=make_recall_mock([]),
            knowledge_store=make_kb_mock([]),
            project_id=PROJECT_ID,
        )
        payload = await manager.assemble(worker_request())
        assert legacy_messages == []
        assert payload.messages() == []
        assert payload.blocks == []

    @pytest.mark.asyncio
    async def test_retrieval_failure_contained_kb_proceeds(self):
        legacy_messages = await run_legacy_worker_sequence(
            make_recall_mock(retrieve_error=RuntimeError("embedding down")),
            make_kb_mock(make_notes()),
            PROJECT_ID,
        )
        manager = build_manager(
            recall_store=make_recall_mock(
                retrieve_error=RuntimeError("embedding down")
            ),
            knowledge_store=make_kb_mock(make_notes()),
            project_id=PROJECT_ID,
        )
        payload = await manager.assemble(worker_request())
        assert normalize(payload.messages()) == normalize(legacy_messages)
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
