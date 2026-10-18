"""Phase-1 slice-3 equivalence: MemoryManager payload ≡ legacy persistent read path.

`run_legacy_persistent_sequence` below reproduces the memory/KB retrieval
block of persistent_graph.py:527-659 (one retrieval per turn, before the
inner loop) — same calls, same per-call 5 s wait_for guards, same
containment, same gating. The legacy path stays live until the Phase-1
cutover; if that block changes, this file must change with it.

Out of scope here (call-site mechanics, position-agnostic payload):
where the pairs get *inserted* (persistent: after the first HumanMessage;
worker: appended) and the per-inner-iteration re-creation of the pairs —
within one turn the pair content is identical, only the synthetic id
suffix differs, and nothing consumes those ids beyond prefix checks.
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

from agent.core.knowledge_injection import create_knowledge_injection_messages
from shared.runtime.core.loader import MemoryConfig, MemoryPipelineConfig
from agent.core.memory_injection import create_memory_injection_messages
from shared.runtime.services.knowledge_store import KnowledgeStore
from agent.services.memory import AssembleRequest, MemoryManager, MemoryRuntime
from agent.services.memory.plugins.legacy import (
    _bounded,
    build_persistent_query_text,
)
from shared.runtime.services.recall_store import RecallStore
from tests._memory_fixtures import (
    PROJECT_ID,
    make_kb_mock,
    make_memories,
    make_notes,
    make_recall_mock,
    normalize,
)

PROJECT_ID_2 = "87654321-4321-8765-4321-876543218765"

# Fast-but-safe stand-ins for the legacy 5 s guard in timeout tests
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


def persistent_request(messages, model=None):
    return AssembleRequest(
        query_text=build_persistent_query_text(messages),
        task_frame=None,
        budget_tokens=10000,
        model=model,
    )


async def run_legacy_persistent_sequence(
    recall_store,
    knowledge_store,
    project_id,
    project_ids,
    messages,
    *,
    model=None,
    retrieval_timeout=5.0,
):
    """The pre-refactor persistent read path (persistent_graph.py:527-659).

    Returns one inner-loop iteration's injection pair set built from the
    per-turn blocks (the loop re-creates the pairs each iteration with a
    fresh id suffix; content is invariant within the turn).
    """
    memory_block = ""
    knowledge_block = ""

    if recall_store:
        try:
            context_text = ""
            for msg in reversed(messages):
                if isinstance(msg, HumanMessage):
                    context_text = (
                        msg.content
                        if isinstance(msg.content, str)
                        else str(msg.content)
                    )
                    break

            memories = await asyncio.wait_for(
                recall_store.retrieve(context_text), timeout=retrieval_timeout
            )
            if memories:
                memory_block = RecallStore.assemble_memory_block(memories, model=model)
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass

    effective_pids = project_ids or ([project_id] if project_id else [])
    if knowledge_store and effective_pids:
        try:
            kb_context = ""
            for msg in reversed(messages):
                if isinstance(msg, HumanMessage):
                    kb_context = (
                        msg.content
                        if isinstance(msg.content, str)
                        else str(msg.content)
                    )
                    break

            kb_notes = await asyncio.wait_for(
                knowledge_store.hybrid_search(
                    project_ids=[uuid_module.UUID(p) for p in effective_pids],
                    query=kb_context,
                    match_count=5,
                ),
                timeout=retrieval_timeout,
            )
            if kb_notes:
                knowledge_block = KnowledgeStore.assemble_knowledge_block(
                    kb_notes, model=model
                )
        except asyncio.TimeoutError:
            pass
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


class TestPersistentEquivalence:
    @pytest.mark.asyncio
    async def test_full_payload_equals_legacy_sequence(self):
        messages = make_conversation()
        legacy_messages = await run_legacy_persistent_sequence(
            make_recall_mock(make_memories()),
            make_kb_mock(make_notes()),
            None,
            [PROJECT_ID],
            messages,
        )
        manager = build_persistent_manager(
            recall_store=make_recall_mock(make_memories()),
            knowledge_store=make_kb_mock(make_notes()),
            project_ids=[PROJECT_ID],
        )
        payload = await manager.assemble(persistent_request(messages))

        assert normalize(payload.messages()) == normalize(legacy_messages)
        legacy_contents = [
            m.content for m in legacy_messages if isinstance(m, ToolMessage)
        ]
        payload_contents = [
            m.content for m in payload.messages() if isinstance(m, ToolMessage)
        ]
        assert payload_contents == legacy_contents
        assert payload.stats.errors == []

    @pytest.mark.asyncio
    async def test_store_calls_identical_to_legacy(self):
        """Both paths must hit the stores with the same await signatures."""
        messages = make_conversation()
        legacy_recall = make_recall_mock(make_memories())
        legacy_kb = make_kb_mock(make_notes())
        await run_legacy_persistent_sequence(
            legacy_recall, legacy_kb, None, [PROJECT_ID, PROJECT_ID_2], messages
        )

        manager_recall = make_recall_mock(make_memories())
        manager_kb = make_kb_mock(make_notes())
        manager = build_persistent_manager(
            recall_store=manager_recall,
            knowledge_store=manager_kb,
            project_ids=[PROJECT_ID, PROJECT_ID_2],
        )
        await manager.assemble(persistent_request(messages))

        assert manager_recall.retrieve.await_args == legacy_recall.retrieve.await_args
        assert manager_kb.hybrid_search.await_args == legacy_kb.hybrid_search.await_args
        # multi-project goes through as the full UUID list
        assert manager_kb.hybrid_search.await_args.kwargs["project_ids"] == [
            uuid_module.UUID(PROJECT_ID),
            uuid_module.UUID(PROJECT_ID_2),
        ]

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
        """No HumanMessage → legacy retrieves with "" rather than skipping."""
        messages = [AIMessage(content="solo")]
        legacy_recall = make_recall_mock(make_memories())
        await run_legacy_persistent_sequence(legacy_recall, None, None, [], messages)
        manager_recall = make_recall_mock(make_memories())
        manager = build_persistent_manager(recall_store=manager_recall)
        payload = await manager.assemble(persistent_request(messages))

        legacy_recall.retrieve.assert_awaited_once_with("")
        manager_recall.retrieve.assert_awaited_once_with("")
        assert [b.kind for b in payload.blocks] == ["memory"]


class TestTimeoutParity:
    @pytest.mark.asyncio
    async def test_memory_timeout_skips_memory_kb_proceeds(self):
        messages = make_conversation()
        legacy_recall = make_recall_mock()
        legacy_recall.retrieve.side_effect = slow(make_memories())
        legacy_messages = await run_legacy_persistent_sequence(
            legacy_recall,
            make_kb_mock(make_notes()),
            None,
            [PROJECT_ID],
            messages,
            retrieval_timeout=SHORT_TIMEOUT,
        )

        manager_recall = make_recall_mock()
        manager_recall.retrieve.side_effect = slow(make_memories())
        manager = build_persistent_manager(
            recall_store=manager_recall,
            knowledge_store=make_kb_mock(make_notes()),
            project_ids=[PROJECT_ID],
            timeout=SHORT_TIMEOUT,
        )
        payload = await manager.assemble(persistent_request(messages))

        assert normalize(payload.messages()) == normalize(legacy_messages)
        assert [b.kind for b in payload.blocks] == ["knowledge"]
        assert payload.stats.errors == ["retriever:recall_hybrid: TimeoutError: "]

    @pytest.mark.asyncio
    async def test_kb_timeout_skips_kb_memory_proceeds(self):
        messages = make_conversation()
        legacy_kb = make_kb_mock()
        legacy_kb.hybrid_search.side_effect = slow(make_notes())
        legacy_messages = await run_legacy_persistent_sequence(
            make_recall_mock(make_memories()),
            legacy_kb,
            None,
            [PROJECT_ID],
            messages,
            retrieval_timeout=SHORT_TIMEOUT,
        )

        manager_kb = make_kb_mock()
        manager_kb.hybrid_search.side_effect = slow(make_notes())
        manager = build_persistent_manager(
            recall_store=make_recall_mock(make_memories()),
            knowledge_store=manager_kb,
            project_ids=[PROJECT_ID],
            timeout=SHORT_TIMEOUT,
        )
        payload = await manager.assemble(persistent_request(messages))

        assert normalize(payload.messages()) == normalize(legacy_messages)
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
