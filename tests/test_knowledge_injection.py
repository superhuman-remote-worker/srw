"""Tests for src/agent/core/knowledge_injection.py: bound knowledge retrieval.

The legacy ``kb_search`` pair creator and predicate left with the per-request
tail (append-only context injection WP6); the knowledge entry is the
planner's (tests/test_context_injection_planner.py).
"""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.core.knowledge_injection import retrieve_bound_knowledge
from agent.services.knowledge.bindings import KnowledgeBinding
from shared.runtime.services.knowledge_store import KnowledgeRecord, KnowledgeStore


def _binding(alias: str, *, native: bool = False) -> KnowledgeBinding:
    return KnowledgeBinding(
        kb_id=uuid.uuid4(),
        alias=alias,
        name=alias.title(),
        kind="native" if native else "datasource",
        writable=native,
        indexed_commit=None if native else f"{alias}-commit",
    )


def _record(binding: KnowledgeBinding, note_id: str) -> KnowledgeRecord:
    return KnowledgeRecord(
        note_id=note_id,
        kb_id=binding.kb_id,
        project_id=binding.kb_id if binding.is_native else None,
        title=note_id.title(),
        note_type="learning",
        content=f"body {note_id}",
    )


def _store(results_by_kb, *, failures=()):
    store = MagicMock()
    store.embedding_service.model = "test-embed"
    store.embedding_service.expected_dimensions = 16

    async def search_chunks(**kwargs):
        kb_ids = kwargs["kb_ids"]
        if any(kb_id in failures for kb_id in kb_ids):
            raise RuntimeError("one KB unavailable")
        if len(kb_ids) > 1:
            records = []
            for kb_id in kb_ids:
                records.extend(results_by_kb.get(kb_id, []))
            return records
        return results_by_kb.get(kb_ids[0], [])

    async def get_watermark(kb_id):
        return SimpleNamespace(indexed_commit=f"wm-{str(kb_id)[:8]}")

    store.search_chunks = AsyncMock(side_effect=search_chunks)
    store.get_watermark = AsyncMock(side_effect=get_watermark)
    return store


class TestBoundKnowledgeRetrieval:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("native", [False, True])
    async def test_embedding_failure_still_injects_attributed_lexical_hits(
        self, native
    ):
        binding = _binding("docs", native=native)
        note_row = uuid.uuid4()
        db = AsyncMock()
        db.fetch.side_effect = [
            [{"note_row": note_row, "arms": ["sparse", "recency"]}],
            [
                {
                    "id": note_row,
                    "kb_id": binding.kb_id,
                    "note_id": "deploy",
                    "title": "Deployment",
                    "note_type": "learning",
                    "content": "deploy safely",
                }
            ],
        ]
        embedder = SimpleNamespace(
            model="missing-model",
            expected_dimensions=16,
            embed=AsyncMock(side_effect=ConnectionError("offline")),
        )
        store = KnowledgeStore(db=db, embedding_service=embedder)
        store.get_watermark = AsyncMock(return_value=None)

        selection = await retrieve_bound_knowledge(store, [binding], "deploy safely")
        block = KnowledgeStore.assemble_knowledge_block(
            selection.notes,
            bindings=selection.bindings,
        )

        assert [note.note_id for note in selection.notes] == ["deploy"]
        assert "deploy safely" in block
        assert "⟨sparse+recency⟩" in block
        assert "dense" not in block

    @pytest.mark.asyncio
    async def test_protects_three_native_and_two_external_slots(self):
        native = _binding("project", native=True)
        external = _binding("docs")
        store = _store(
            {
                native.kb_id: [_record(native, f"n{i}") for i in range(5)],
                external.kb_id: [_record(external, f"e{i}") for i in range(5)],
            }
        )

        selection = await retrieve_bound_knowledge(
            store, [native, external], "current work"
        )

        assert [note.note_id for note in selection.notes] == [
            "n0",
            "n1",
            "n2",
            "e0",
            "e1",
        ]
        assert selection.counts_by_binding == {"project": 3, "docs": 2}

    @pytest.mark.asyncio
    async def test_unused_native_slots_spill_to_external(self):
        native = _binding("project", native=True)
        external = _binding("docs")
        store = _store(
            {
                native.kb_id: [_record(native, "native")],
                external.kb_id: [_record(external, f"e{i}") for i in range(5)],
            }
        )

        selection = await retrieve_bound_knowledge(store, [native, external], "q")

        assert selection.counts_by_binding == {"project": 1, "docs": 4}
        assert len(selection.notes) == 5

    @pytest.mark.asyncio
    async def test_unused_external_slots_spill_to_native(self):
        native = _binding("project", native=True)
        external = _binding("docs")
        store = _store(
            {
                native.kb_id: [_record(native, f"n{i}") for i in range(5)],
                external.kb_id: [],
            }
        )

        selection = await retrieve_bound_knowledge(store, [native, external], "q")

        assert selection.counts_by_binding == {"project": 5}
        assert len(selection.notes) == 5

    @pytest.mark.asyncio
    async def test_external_only_round_robins_up_to_five(self):
        docs = _binding("docs")
        runbooks = _binding("runbooks")
        store = _store(
            {
                docs.kb_id: [_record(docs, f"d{i}") for i in range(4)],
                runbooks.kb_id: [_record(runbooks, f"r{i}") for i in range(4)],
            }
        )

        selection = await retrieve_bound_knowledge(store, [docs, runbooks], "q")

        assert [note.note_id for note in selection.notes] == [
            "d0",
            "r0",
            "d1",
            "r1",
            "d2",
        ]
        assert selection.counts_by_binding == {"docs": 3, "runbooks": 2}

    @pytest.mark.asyncio
    async def test_external_failure_keeps_native_and_other_external_results(self):
        native = _binding("project", native=True)
        broken = _binding("broken")
        docs = _binding("docs")
        store = _store(
            {
                native.kb_id: [_record(native, f"n{i}") for i in range(3)],
                docs.kb_id: [_record(docs, "d0"), _record(docs, "d1")],
            },
            failures={broken.kb_id},
        )

        selection = await retrieve_bound_knowledge(store, [native, broken, docs], "q")

        assert selection.counts_by_binding == {"project": 3, "docs": 2}
        assert len(selection.notes) == 5

    @pytest.mark.asyncio
    async def test_deduplicates_by_kb_and_note_id_and_uses_chunk_version(self):
        docs = _binding("docs")
        duplicate = _record(docs, "same")
        store = _store({docs.kb_id: [duplicate, duplicate, _record(docs, "different")]})
        store.embedding_service.profile_fingerprint = "pf-effective-profile"

        selection = await retrieve_bound_knowledge(store, [docs], "q")

        assert [note.note_id for note in selection.notes] == ["same", "different"]
        kwargs = store.search_chunks.await_args.kwargs
        assert kwargs["embedding_version"] == ("test-embed:16:c1:pf-effective-profile")
        assert kwargs["match_count"] == 5

    @pytest.mark.asyncio
    async def test_source_labels_and_external_watermark_are_rendered(self):
        docs = _binding("docs")
        store = _store({docs.kb_id: [_record(docs, "deployments")]})

        selection = await retrieve_bound_knowledge(store, [docs], "deploy")
        block = KnowledgeStore.assemble_knowledge_block(
            selection.notes,
            bindings=selection.bindings,
            external_watermarks=selection.external_watermarks,
        )

        assert "[docs] docs:deployments" in block
        assert "External snapshots (as of): [docs] wm-" in block

    @pytest.mark.asyncio
    async def test_partial_external_injection_discloses_mixed_convergence(self):
        docs = _binding("docs")
        store = _store({docs.kb_id: [_record(docs, "deployments")]})
        store.get_watermark = AsyncMock(
            return_value=SimpleNamespace(
                indexed_commit="a" * 40,
                source_head="b" * 40,
                status="partial",
            )
        )

        selection = await retrieve_bound_knowledge(store, [docs], "deploy")
        block = KnowledgeStore.assemble_knowledge_block(
            selection.notes,
            bindings=selection.bindings,
            external_watermarks=selection.external_watermarks,
        )

        assert "partial — last clean @ " in block
        assert "source @ " in block

    @pytest.mark.asyncio
    async def test_zero_contribution_external_still_discloses_indexing(self):
        # An external KB that returned no records but is still indexing must
        # remain visible in the block — otherwise a mid-convergence KB is
        # silently dropped and reads as "nothing here".
        docs = _binding("docs")
        pending = _binding("pending")
        store = _store({docs.kb_id: [_record(docs, "deployments")]})

        async def get_watermark(kb_id):
            if kb_id == pending.kb_id:
                return SimpleNamespace(
                    indexed_commit=None,
                    source_head="b" * 40,
                    status="indexing",
                )
            return SimpleNamespace(indexed_commit=f"wm-{str(kb_id)[:8]}")

        store.get_watermark = AsyncMock(side_effect=get_watermark)

        selection = await retrieve_bound_knowledge(store, [docs, pending], "deploy")
        block = KnowledgeStore.assemble_knowledge_block(
            selection.notes,
            bindings=selection.bindings,
            external_watermarks=selection.external_watermarks,
        )

        assert "pending" in selection.external_watermarks
        assert "[pending]" in block
        assert "source @ " + "b" * 40 in block
