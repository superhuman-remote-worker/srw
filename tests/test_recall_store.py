"""Unit and integration tests for RecallStore.

Unit tests use mocked db/embedding and run everywhere.
Integration tests require DATABASE_URL and are skipped otherwise.
"""

import os
import uuid
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

import shared.runtime.services.recall_store as recall_store_module
from shared.runtime.services.recall_store import (
    MemoryRecord,
    RecallStore,
    memory_health,
)


# =============================================================================
# Fixtures
# =============================================================================


@dataclass
class MockMemoryConfig:
    """Minimal config matching MemoryConfig fields."""

    enabled: bool = True
    budget_tokens: int = 5000
    max_memories_per_injection: int = 10
    importance_threshold: float = 0.3
    dedup_threshold: float = 0.92


@pytest.fixture
def job_id():
    return uuid.uuid4()


@pytest.fixture
def mock_embedding_service():
    service = MagicMock()
    service.embed = AsyncMock(return_value=[0.1] * 1536)
    service.embed_batch = AsyncMock(return_value=[[0.1] * 1536])
    return service


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.fetchval = AsyncMock()
    db.fetchrow = AsyncMock()
    db.fetch = AsyncMock(return_value=[])
    db.execute = AsyncMock()
    return db


@pytest.fixture
def config():
    return MockMemoryConfig()


@pytest.fixture
def store(mock_db, mock_embedding_service, job_id, config):
    return RecallStore(
        db=mock_db,
        embedding_service=mock_embedding_service,
        job_id=job_id,
        config=config,
    )


# =============================================================================
# MemoryRecord Tests
# =============================================================================


class TestMemoryRecord:
    """Test MemoryRecord dataclass."""

    def test_from_row(self):
        """from_row creates a record from a dict."""
        row = {
            "id": uuid.uuid4(),
            "job_id": uuid.uuid4(),
            "content": "test content",
            "summary": "test",
            "memory_type": "procedural",
            "source": "todo",
            "keywords": ["a", "b"],
            "importance": 0.8,
            "token_count": 42,
            "access_count": 3,
            "source_phase": 2,
        }
        record = MemoryRecord.from_row(row)
        assert record.content == "test content"
        assert record.memory_type == "procedural"
        assert record.source == "todo"
        assert record.keywords == ["a", "b"]
        assert record.importance == 0.8
        assert record.token_count == 42

    def test_from_row_defaults(self):
        """from_row handles missing fields gracefully."""
        record = MemoryRecord.from_row({})
        assert record.content == ""
        assert record.memory_type == "factual"
        assert record.source == "observer"
        assert record.keywords == []
        assert record.importance == 0.5

    def test_from_row_none_keywords(self):
        """from_row converts None keywords to empty list."""
        record = MemoryRecord.from_row({"keywords": None})
        assert record.keywords == []

    def test_from_row_bitemporal_fields(self):
        """from_row populates the Phase-4 bi-temporal supersede columns."""
        from datetime import datetime, timezone

        new_id = uuid.uuid4()
        vf = datetime(2026, 6, 1, tzinfo=timezone.utc)
        vt = datetime(2026, 6, 14, tzinfo=timezone.utc)
        record = MemoryRecord.from_row(
            {
                "content": "stale fact",
                "valid_from": vf,
                "valid_to": vt,
                "superseded_at": vt,
                "superseded_by": new_id,
            }
        )
        assert record.valid_from == vf
        assert record.valid_to == vt
        assert record.superseded_at == vt
        assert record.superseded_by == new_id

    def test_from_row_bitemporal_defaults_none(self):
        """A currently-valid row has valid_to / superseded_* unset (None)."""
        record = MemoryRecord.from_row({"content": "live fact"})
        assert record.valid_from is None
        assert record.valid_to is None
        assert record.superseded_at is None
        assert record.superseded_by is None


# =============================================================================
# Storage Tests
# =============================================================================


class TestStore:
    """Test RecallStore.store()."""

    @pytest.mark.asyncio
    async def test_store_basic(self, store, mock_db, mock_embedding_service):
        """store() embeds content and inserts into database."""
        mock_db.fetchrow.return_value = None  # No duplicates
        mock_db.fetchval.return_value = uuid.uuid4()

        mem_id = await store.store(
            content="The API uses JWT tokens for auth.",
            summary="API uses JWT auth",
            keywords=["jwt", "auth", "api"],
            importance=0.8,
        )

        assert mem_id is not None
        mock_embedding_service.embed.assert_awaited_once_with(
            "The API uses JWT tokens for auth."
        )
        mock_db.fetchval.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_store_below_threshold(self, store, mock_embedding_service):
        """store() skips memories below importance threshold."""
        result = await store.store(
            content="Unimportant note",
            importance=0.1,  # Below 0.3 threshold
        )

        assert result is None
        mock_embedding_service.embed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_store_dedup_updates_existing(
        self, store, mock_db, mock_embedding_service
    ):
        """store() updates existing memory when duplicate found."""
        existing_id = uuid.uuid4()
        # find_similar returns a match
        mock_db.fetchrow.return_value = {
            "id": existing_id,
            "content": "Similar content",
            "similarity": 0.95,
            "job_id": store.job_id,
            "memory_type": "factual",
            "source": "observer",
            "importance": 0.7,
            "token_count": 10,
            "access_count": 1,
        }

        result = await store.store(
            content="Similar content rephrased",
            importance=0.9,
        )

        assert result == existing_id
        # Should update existing, not insert
        mock_db.execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_store_estimates_tokens(self, store, mock_db):
        """store() estimates token count when not provided."""
        mock_db.fetchrow.return_value = None  # No duplicates
        mock_db.fetchval.return_value = uuid.uuid4()

        await store.store(
            content="A" * 400,  # ~100 tokens
            importance=0.5,
        )

        # Check the token_count arg (15th positional arg, $15 with project_id added)
        call_args = mock_db.fetchval.call_args
        token_count_arg = call_args[0][15]  # $15 = token_count
        assert token_count_arg == 100  # 400 chars / 4

    @pytest.mark.asyncio
    async def test_store_truncates_summary(self, store, mock_db):
        """store() truncates summary to 500 chars."""
        mock_db.fetchrow.return_value = None
        mock_db.fetchval.return_value = uuid.uuid4()

        await store.store(
            content="content",
            summary="x" * 600,
            importance=0.5,
        )

        call_args = mock_db.fetchval.call_args
        summary_arg = call_args[0][5]  # $5 = summary (shifted by project_id)
        assert len(summary_arg) == 500

    @pytest.mark.asyncio
    async def test_retrieval_subwrite_failure_is_legacy_best_effort(
        self, store, mock_db, mock_embedding_service
    ):
        memory_id = uuid.uuid4()
        mock_db.fetchrow.return_value = None
        mock_db.fetchval.return_value = memory_id
        mock_db.execute.side_effect = RuntimeError("retrieval write failed")

        result = await store.store(
            content="Durable parent with a best-effort trigger.",
            importance=0.8,
            retrieval_messages=["When is the durable parent useful?"],
        )

        assert result == memory_id
        mock_embedding_service.embed_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_retrieval_subwrite_failure_propagates_in_strict_mode(
        self, mock_db, mock_embedding_service, job_id, config
    ):
        strict = RecallStore(
            db=mock_db,
            embedding_service=mock_embedding_service,
            job_id=job_id,
            config=config,
            strict_writes=True,
        )
        mock_db.fetchrow.return_value = None
        mock_db.fetchval.return_value = uuid.uuid4()
        mock_db.execute.side_effect = RuntimeError("retrieval write failed")

        with pytest.raises(RuntimeError, match="retrieval write failed"):
            await strict.store(
                content="Durable parent needs an atomic trigger.",
                importance=0.8,
                retrieval_messages=["When is the atomic trigger useful?"],
            )

    @pytest.mark.asyncio
    async def test_retrieval_embedding_count_mismatch_propagates_in_strict_mode(
        self, mock_db, mock_embedding_service, job_id, config
    ):
        strict = RecallStore(
            db=mock_db,
            embedding_service=mock_embedding_service,
            job_id=job_id,
            config=config,
            strict_writes=True,
        )
        mock_db.fetchrow.return_value = None
        mock_db.fetchval.return_value = uuid.uuid4()
        mock_embedding_service.embed_batch.return_value = []

        with pytest.raises(RuntimeError, match="embedding count"):
            await strict.store(
                content="The provider response must be complete.",
                importance=0.8,
                retrieval_messages=["When do complete embeddings matter?"],
            )


# =============================================================================
# Retrieval Tests
# =============================================================================


class TestRetrieval:
    """Test RecallStore retrieval methods."""

    @pytest.mark.asyncio
    async def test_retrieve_respects_budget(
        self, store, mock_db, mock_embedding_service
    ):
        """retrieve() stops adding memories when budget is exhausted."""
        # Return 3 candidates with 2000 tokens each
        mock_db.fetch.return_value = [
            {
                "id": uuid.uuid4(),
                "job_id": store.job_id,
                "content": f"Memory {i}",
                "memory_type": "factual",
                "source": "observer",
                "importance": 0.9 - i * 0.1,
                "token_count": 2000,
                "access_count": 0,
                "keywords": [],
            }
            for i in range(3)
        ]

        store.budget_tokens = 5000
        results = await store.retrieve("test context")

        # Should fit 2 memories (4000 tokens), not 3 (6000 would exceed 5000)
        assert len(results) == 2


# =============================================================================
# Bi-temporal supersede (overhaul Phase 4) — default retrieval excludes
# retired (valid_to IS NOT NULL) rows. Behaviour-preserving until a writer
# actually retires something; these pin the filter is present in every
# agent-side read path.
# =============================================================================


class TestBitemporalRetrievalFilter:
    """Every default read path filters `valid_to IS NULL` (currently-valid)."""

    @pytest.mark.asyncio
    async def test_find_similar_filters_valid(self, store, mock_db):
        """find_similar only adjudicates against currently-valid memories."""
        mock_db.fetchrow.return_value = None
        await store.find_similar([0.1] * 1536)
        sql = mock_db.fetchrow.call_args[0][0]
        assert "valid_to IS NULL" in sql

    @pytest.mark.asyncio
    async def test_get_ttl_active_filters_valid(self, store, mock_db):
        """A retired pinned memory is not 'active' — get_ttl_active excludes it."""
        mock_db.fetch.return_value = []
        await store.get_ttl_active()
        sql = mock_db.fetch.call_args[0][0]
        assert "valid_to IS NULL" in sql

    @pytest.mark.asyncio
    async def test_decrement_ttl_filters_valid(self, store, mock_db):
        """decrement_ttl does not tick TTL on retired rows."""
        mock_db.fetchval.return_value = 0
        await store.decrement_ttl()
        sql = mock_db.fetchval.call_args[0][0]
        assert "valid_to IS NULL" in sql

    @pytest.mark.asyncio
    async def test_get_stats_reports_current_and_superseded(self, store, mock_db):
        """get_stats surfaces the live vs retired split."""
        mock_db.fetchrow.return_value = {
            "total": 5,
            "current": 4,
            "superseded": 1,
            "ttl_active": 2,
        }
        stats = await store.get_stats()
        sql = mock_db.fetchrow.call_args[0][0]
        assert "FILTER (WHERE valid_to IS NULL)" in sql
        assert "FILTER (WHERE valid_to IS NOT NULL)" in sql
        assert stats["current"] == 4
        assert stats["superseded"] == 1


# =============================================================================
# Ingestion verdicts + supersede (overhaul Phase 4)
# =============================================================================


class _FakeVerdict:
    """Stand-in for auxiliary.IngestionVerdict (duck-typed by the store)."""

    def __init__(self, action, target_indices=None, merged_content=None, reason="r"):
        self.action = action
        self.target_indices = target_indices or []
        self.merged_content = merged_content
        self.reason = reason


class _FakeVerdictService:
    """Stand-in for IngestionVerdictService injected onto the store."""

    def __init__(self, verdict, top_k=5, review_floor=0.6):
        self.verdict = verdict
        self.top_k = top_k
        self.review_floor = review_floor
        self.calls = 0
        self.last_kwargs = None

    async def adjudicate(self, **kwargs):
        self.calls += 1
        self.last_kwargs = kwargs
        return self.verdict


def _neighbour_row(content="old fact", sim=0.8):
    return {
        "id": uuid.uuid4(),
        "content": content,
        "memory_type": "factual",
        "source": "observer",
        "importance": 0.7,
        "token_count": 10,
        "access_count": 1,
        "keywords": [],
        "similarity": sim,
    }


class TestIngestionVerdict:
    """store() write-path adjudication when an ingestion verdict is wired."""

    @pytest.mark.asyncio
    async def test_cost_guard_no_neighbour_adds_without_llm(
        self, store, mock_db, mock_embedding_service
    ):
        """No near-duplicate → straight ADD, zero verdict calls."""
        svc = _FakeVerdictService(_FakeVerdict("NOOP"))  # would NOOP if asked
        store.ingestion_verdict = svc
        mock_db.fetch.return_value = []  # find_similar_many: nothing close
        new_id = uuid.uuid4()
        mock_db.fetchval.return_value = new_id

        result = await store.store(content="a brand new fact", importance=0.8)

        assert result == new_id
        assert svc.calls == 0  # cost guard: adjudicator never consulted
        mock_db.fetchval.assert_awaited_once()  # inserted

    @pytest.mark.asyncio
    async def test_verdict_add_inserts_new(self, store, mock_db):
        """ADD with neighbours present → adjudicate, then insert."""
        store.ingestion_verdict = _FakeVerdictService(_FakeVerdict("ADD"))
        mock_db.fetch.return_value = [_neighbour_row()]
        new_id = uuid.uuid4()
        mock_db.fetchval.return_value = new_id

        result = await store.store(content="related but new", importance=0.8)

        assert result == new_id
        assert store.ingestion_verdict.calls == 1
        mock_db.fetchval.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_verdict_noop_bumps_existing(self, store, mock_db):
        """NOOP → bump the duplicate in place, never insert."""
        row = _neighbour_row()
        store.ingestion_verdict = _FakeVerdictService(
            _FakeVerdict("NOOP", target_indices=[1])
        )
        mock_db.fetch.return_value = [row]

        result = await store.store(content="dup", importance=0.9)

        assert result == row["id"]
        mock_db.fetchval.assert_not_awaited()  # no insert
        mock_db.execute.assert_awaited()  # bump UPDATE

    @pytest.mark.asyncio
    async def test_verdict_update_inserts_and_supersedes(self, store, mock_db):
        """UPDATE → insert the new fact AND retire the stale neighbour."""
        row = _neighbour_row(content="works out one hour per day")
        store.ingestion_verdict = _FakeVerdictService(
            _FakeVerdict("UPDATE", target_indices=[1])
        )
        mock_db.fetch.return_value = [row]
        new_id = uuid.uuid4()
        mock_db.fetchval.return_value = new_id
        mock_db.execute.return_value = "UPDATE 1"

        result = await store.store(
            content="works out two hours per day", importance=0.8
        )

        assert result == new_id
        mock_db.fetchval.assert_awaited_once()  # inserted new
        # supersede UPDATE ran against the stale neighbour id
        execute_sqls = [c.args[0] for c in mock_db.execute.await_args_list]
        assert any("valid_to = CURRENT_TIMESTAMP" in s for s in execute_sqls)
        retire_call = next(
            c for c in mock_db.execute.await_args_list if "valid_to" in c.args[0]
        )
        assert retire_call.args[1] == [row["id"]]  # old id
        assert retire_call.args[2] == new_id  # superseded_by

    @pytest.mark.asyncio
    async def test_verdict_merge_inserts_merged_and_supersedes(
        self, store, mock_db, mock_embedding_service
    ):
        """MERGE → insert merged_content (re-embedded) and retire neighbours."""
        row = _neighbour_row(content="the bookshelf is oak")
        store.ingestion_verdict = _FakeVerdictService(
            _FakeVerdict(
                "MERGE",
                target_indices=[1],
                merged_content="the oak bookshelf was assembled in May",
            )
        )
        mock_db.fetch.return_value = [row]
        new_id = uuid.uuid4()
        mock_db.fetchval.return_value = new_id
        mock_db.execute.return_value = "UPDATE 1"

        result = await store.store(
            content="assembled the bookshelf in May", importance=0.8
        )

        assert result == new_id
        # candidate embed + merged-content re-embed
        assert mock_embedding_service.embed.await_count == 2
        inserted_content = mock_db.fetchval.await_args.args[4]  # $4 = content
        assert inserted_content == "the oak bookshelf was assembled in May"

    @pytest.mark.asyncio
    async def test_verdict_update_without_targets_degrades_to_add(self, store, mock_db):
        """Malformed UPDATE (no target_indices) → conservative ADD, no retire."""
        store.ingestion_verdict = _FakeVerdictService(
            _FakeVerdict("UPDATE", target_indices=[])
        )
        mock_db.fetch.return_value = [_neighbour_row()]
        new_id = uuid.uuid4()
        mock_db.fetchval.return_value = new_id

        result = await store.store(content="ambiguous", importance=0.8)

        assert result == new_id
        mock_db.fetchval.assert_awaited_once()  # inserted
        # no supersede UPDATE ran
        execute_sqls = [c.args[0] for c in mock_db.execute.await_args_list]
        assert not any("valid_to = CURRENT_TIMESTAMP" in s for s in execute_sqls)

    @pytest.mark.asyncio
    async def test_legacy_path_when_no_verdict_service(self, store, mock_db):
        """ingestion_verdict None → legacy cosine dedup, find_similar_many unused."""
        assert store.ingestion_verdict is None
        mock_db.fetchrow.return_value = None  # find_similar: no dup
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.store(content="x", importance=0.8)
        # legacy path uses fetchrow (find_similar), not fetch (find_similar_many)
        mock_db.fetchrow.assert_awaited()


class TestPreInsertDedupRecheck:
    """Slice 0 — the dual-trigger dedup race guard in _store_with_verdict (§4.7).

    Every ADD terminal re-runs find_similar_many at ``recheck_threshold`` right
    before inserting, reusing the candidate embedding. A twin that a concurrent
    writer committed *between* the first check and the insert bumps instead of
    inserting a duplicate; a non-racing write sees the same state twice and
    behaves exactly as before. Driven by ``mock_db.fetch.side_effect`` = the
    [first-lookup, re-check] result pair.
    """

    @pytest.mark.asyncio
    async def test_racing_twin_bumps_instead_of_inserting(self, store, mock_db):
        """A twin committed between the cost-guard check and the insert → bump."""
        twin = _neighbour_row(content="same fact", sim=0.95)
        store.ingestion_verdict = _FakeVerdictService(_FakeVerdict("ADD"))
        # 1st find_similar_many (cost guard): nothing close → straight-ADD path.
        # 2nd find_similar_many (pre-insert re-check): a twin has appeared.
        mock_db.fetch.side_effect = [[], [twin]]

        result = await store.store(content="same fact", importance=0.8)

        assert result == twin["id"]  # bumped the twin
        mock_db.fetchval.assert_not_awaited()  # never inserted a duplicate
        mock_db.execute.assert_awaited()  # bump UPDATE ran

    @pytest.mark.asyncio
    async def test_non_racing_write_inserts_normally(self, store, mock_db):
        """No twin appears between the checks → ordinary ADD, unchanged behaviour."""
        store.ingestion_verdict = _FakeVerdictService(_FakeVerdict("ADD"))
        mock_db.fetch.side_effect = [[], []]  # empty both times
        new_id = uuid.uuid4()
        mock_db.fetchval.return_value = new_id

        result = await store.store(content="genuinely new", importance=0.8)

        assert result == new_id
        mock_db.fetchval.assert_awaited_once()  # inserted once

    @pytest.mark.asyncio
    async def test_recheck_excludes_already_adjudicated_neighbour(self, store, mock_db):
        """A ≥0.9 neighbour the verdict already cleared for ADD must not be re-bumped."""
        neighbour = _neighbour_row(content="close but distinct", sim=0.93)
        store.ingestion_verdict = _FakeVerdictService(_FakeVerdict("ADD"))
        # Same high-similarity neighbour on both the first lookup and the
        # re-check: it is in the adjudicated (excluded) set, so no bump — the
        # explicit ADD verdict stands.
        mock_db.fetch.side_effect = [[neighbour], [neighbour]]
        new_id = uuid.uuid4()
        mock_db.fetchval.return_value = new_id

        result = await store.store(content="close but distinct v2", importance=0.8)

        assert result == new_id
        mock_db.fetchval.assert_awaited_once()  # ADD honoured, not downgraded

    @pytest.mark.asyncio
    async def test_recheck_lookup_failure_falls_back_to_add(self, store, mock_db):
        """A failed re-check SELECT must not lose the write — proceed with ADD."""
        store.ingestion_verdict = _FakeVerdictService(_FakeVerdict("ADD"))
        mock_db.fetch.side_effect = [[], RuntimeError("db blip")]
        new_id = uuid.uuid4()
        mock_db.fetchval.return_value = new_id

        result = await store.store(content="new despite blip", importance=0.8)

        assert result == new_id
        mock_db.fetchval.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_initial_verdict_lookup_failure_propagates_when_strict(
        self, store, mock_db
    ):
        store.strict_writes = True
        store.ingestion_verdict = _FakeVerdictService(_FakeVerdict("ADD"))
        mock_db.fetch.side_effect = RuntimeError("initial lookup failed")

        with pytest.raises(RuntimeError, match="initial lookup failed"):
            await store.store(content="strict candidate", importance=0.8)

    @pytest.mark.asyncio
    async def test_recheck_lookup_failure_propagates_when_strict(self, store, mock_db):
        store.strict_writes = True
        store.ingestion_verdict = _FakeVerdictService(_FakeVerdict("ADD"))
        mock_db.fetch.side_effect = [[], RuntimeError("recheck failed")]

        with pytest.raises(RuntimeError, match="recheck failed"):
            await store.store(content="strict candidate", importance=0.8)


class TestFindSimilarManyAndSupersede:
    """The two new write-path primitives."""

    @pytest.mark.asyncio
    async def test_find_similar_many_filters_and_limits(self, store, mock_db):
        mock_db.fetch.return_value = []
        await store.find_similar_many([0.1] * 1536, k=7, min_similarity=0.55)
        sql = mock_db.fetch.call_args[0][0]
        assert "valid_to IS NULL" in sql
        assert "LIMIT $4" in sql
        # params: (sql, embedding $1, scope_val $2, min_similarity $3, k $4)
        assert mock_db.fetch.call_args[0][3] == 0.55  # min_similarity
        assert mock_db.fetch.call_args[0][4] == 7  # k

    @pytest.mark.asyncio
    async def test_find_similar_many_attaches_similarity(self, store, mock_db):
        mock_db.fetch.return_value = [_neighbour_row(sim=0.91)]
        out = await store.find_similar_many([0.1] * 1536)
        assert len(out) == 1
        assert out[0].similarity == 0.91

    @pytest.mark.asyncio
    async def test_supersede_sets_markers_and_counts(self, store, mock_db):
        mock_db.execute.return_value = "UPDATE 2"
        old = [uuid.uuid4(), uuid.uuid4()]
        new = uuid.uuid4()
        count = await store.supersede(old, new)
        assert count == 2
        sql = mock_db.execute.call_args[0][0]
        assert "valid_to = CURRENT_TIMESTAMP" in sql
        assert "superseded_by = $2" in sql
        assert "valid_to IS NULL" in sql  # idempotent: skip already-retired
        assert mock_db.execute.call_args[0][1] == old
        assert mock_db.execute.call_args[0][2] == new

    @pytest.mark.asyncio
    async def test_supersede_empty_is_noop(self, store, mock_db):
        assert await store.supersede([], uuid.uuid4()) == 0
        mock_db.execute.assert_not_awaited()


class TestWriteGate:
    """memory.extraction.write_gate (overhaul Phase 4) — completeness toggle."""

    @pytest.mark.asyncio
    async def test_gate_on_skips_low_importance(self, store, mock_embedding_service):
        assert store.write_gate is True  # default
        result = await store.store(content="x", importance=0.1)
        assert result is None
        mock_embedding_service.embed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_gate_off_stores_low_importance(
        self, mock_db, mock_embedding_service, job_id
    ):
        from types import SimpleNamespace

        cfg = SimpleNamespace(
            project_scoped=False,
            dedup_threshold=0.92,
            importance_threshold=0.3,
            budget_tokens=5000,
            max_memories_per_injection=10,
            retrieval_importance_floor=0.4,
            default_ttl=10,
            extraction=SimpleNamespace(write_gate=False),
        )
        s = RecallStore(
            db=mock_db,
            embedding_service=mock_embedding_service,
            job_id=job_id,
            config=cfg,
        )
        assert s.write_gate is False
        mock_db.fetchrow.return_value = None
        mock_db.fetchval.return_value = uuid.uuid4()

        result = await s.store(content="a low-value note", importance=0.05)

        assert result is not None  # stored despite importance < threshold
        mock_embedding_service.embed.assert_awaited()


# =============================================================================
# Assembly Tests
# =============================================================================


class TestAssembly:
    """Test memory block assembly."""

    def test_format_memory_basic(self):
        """format_memory() creates readable output."""
        memory = MemoryRecord(
            content="The users table uses soft deletes.",
            importance=0.9,
            source_phase=2,
        )

        result = RecallStore.format_memory(memory, 1)
        assert "[1]" in result
        assert "importance: 0.9" in result
        assert "phase 2" in result
        assert "The users table uses soft deletes." in result

    def test_format_memory_with_type(self):
        """format_memory() shows non-default memory type."""
        memory = MemoryRecord(
            content="Always use async/await.",
            importance=0.7,
            memory_type="procedural",
        )

        result = RecallStore.format_memory(memory, 1)
        assert "procedural" in result

    def test_format_memory_factual_type_hidden(self):
        """format_memory() hides default 'factual' type."""
        memory = MemoryRecord(
            content="Some fact.",
            importance=0.5,
            memory_type="factual",
        )

        result = RecallStore.format_memory(memory, 1)
        assert "factual" not in result

    def test_assemble_memory_block(self):
        """assemble_memory_block() creates full block with header/footer."""
        memories = [
            MemoryRecord(content="Memory one", importance=0.9, token_count=100),
            MemoryRecord(content="Memory two", importance=0.7, token_count=50),
        ]

        block = RecallStore.assemble_memory_block(memories)
        assert "--- Retrieved Memories (relevance-ranked) ---" in block
        assert (
            "--- End Memories (2 items: 0 pinned + 2 retrieved, ~150 tokens) ---"
            in block
        )
        assert "[1]" in block
        assert "[2]" in block
        assert "Memory one" in block
        assert "Memory two" in block

    def test_assemble_memory_block_empty(self):
        """assemble_memory_block() returns empty string for no memories."""
        assert RecallStore.assemble_memory_block([]) == ""

    def test_format_memory_pinned(self):
        """format_memory() carries no per-turn TTL text (D11).

        A pinned memory renders like any other: the "pinned, N turns left"
        clause went with append-only context injection 2e.
        """
        memory = MemoryRecord(
            content="Important pinned memory.",
            importance=0.9,
            remaining_turns=7,
        )

        result = RecallStore.format_memory(memory, 1)
        assert "turns left" not in result
        assert result == "[1] (importance: 0.9)\nImportant pinned memory."

    def test_assemble_memory_block_mixed(self):
        """assemble_memory_block() separates pinned from retrieved."""
        memories = [
            MemoryRecord(
                content="Pinned one", importance=0.9, token_count=50, remaining_turns=5
            ),
            MemoryRecord(
                content="Retrieved one",
                importance=0.7,
                token_count=50,
                remaining_turns=0,
            ),
            MemoryRecord(content="Retrieved two", importance=0.5, token_count=50),
        ]

        block = RecallStore.assemble_memory_block(memories)
        assert "--- Pinned Memories (TTL-active) ---" in block
        assert "--- Retrieved Memories (relevance-ranked) ---" in block
        assert "1 pinned + 2 retrieved" in block
        assert "Pinned one" in block
        assert "Retrieved one" in block
        assert "Retrieved two" in block

    def test_memory_record_remaining_turns(self):
        """MemoryRecord includes remaining_turns from row."""
        row = {
            "content": "Test memory",
            "remaining_turns": 5,
        }
        record = MemoryRecord.from_row(row)
        assert record.remaining_turns == 5

    def test_memory_record_remaining_turns_null(self):
        """MemoryRecord handles None remaining_turns."""
        row = {"content": "Test memory"}
        record = MemoryRecord.from_row(row)
        assert record.remaining_turns is None


# =============================================================================
# Stats Tests
# =============================================================================


class TestStats:
    """Test get_stats()."""

    @pytest.mark.asyncio
    async def test_get_stats(self, store, mock_db):
        """get_stats() returns aggregated statistics."""
        mock_db.fetchrow.return_value = {
            "total": 25,
            "total_tokens": 12500,
            "total_accesses": 80,
            "factual": 10,
            "procedural": 5,
            "error_solution": 3,
            "vocabulary": 2,
            "relational": 5,
            "from_observer": 12,
            "from_todo": 8,
            "from_compaction": 3,
            "from_phase_archive": 1,
            "from_tool_error": 1,
            "avg_importance": 0.65,
        }

        stats = await store.get_stats()
        assert stats["total"] == 25
        assert stats["total_tokens"] == 12500
        assert stats["from_observer"] == 12
        assert stats["avg_importance"] == 0.65

    @pytest.mark.asyncio
    async def test_get_stats_empty(self, store, mock_db):
        """get_stats() handles empty memory store."""
        mock_db.fetchrow.return_value = None

        stats = await store.get_stats()
        assert stats["total"] == 0


# =============================================================================
# Deadlock containment (knowledge-base/knowledge/issues/project_scoped_memory_deadlocks_under_
# parallel_jobs.md) — deterministic id-ordered locking, bounded retry of the
# access-stat write, containment, and MemoryHealth counters.
# =============================================================================


class DeadlockDetectedError(Exception):
    """Stands in for asyncpg's DeadlockDetectedError.

    The production code matches on ``type(e).__name__`` so it does not have to
    import asyncpg's exception tree, so this stub must carry the real name.
    """


class TestDeadlockContainment:
    """Ordered locking + contained/retried/counted concurrency failures."""

    @pytest.fixture(autouse=True)
    def _reset_health(self, monkeypatch):
        monkeypatch.setattr(recall_store_module, "_ACCESS_STAT_RETRY_DELAYS", (0, 0))
        memory_health.reset()
        yield
        memory_health.reset()

    @pytest.mark.asyncio
    async def test_decrement_ttl_locks_in_id_order(self, store, mock_db):
        """decrement_ttl acquires row locks via an id-ordered FOR UPDATE CTE."""
        mock_db.fetchval.return_value = 4
        count = await store.decrement_ttl()
        sql = mock_db.fetchval.call_args[0][0]
        assert "ORDER BY id" in sql
        assert "FOR UPDATE" in sql
        assert "valid_to IS NULL" in sql
        assert count == 4

    @pytest.mark.asyncio
    async def test_decrement_ttl_counts_deadlock_and_reraises(self, store, mock_db):
        """A TTL-decrement deadlock is counted, then re-raised (callers contain)."""
        mock_db.fetchval.side_effect = DeadlockDetectedError("deadlock detected")
        with pytest.raises(DeadlockDetectedError):
            await store.decrement_ttl()
        assert memory_health.snapshot() == {"ttl_decrement_deadlock": 1}

    @pytest.mark.asyncio
    async def test_access_stats_sorts_ids_and_locks_in_order(self, store, mock_db):
        """The access-stat write sorts ids and locks via an ordered CTE."""
        ids = sorted([uuid.uuid4() for _ in range(3)], reverse=True)
        await store._record_access_stats(ids)
        sql = mock_db.execute.call_args[0][0]
        assert "ORDER BY id" in sql
        assert "FOR UPDATE" in sql
        assert mock_db.execute.call_args[0][1] == sorted(ids)

    @pytest.mark.asyncio
    async def test_access_stats_retries_deadlock_then_succeeds(self, store, mock_db):
        """Two deadlocks then success → three attempts, no raise, counted."""
        mock_db.execute.side_effect = [
            DeadlockDetectedError("deadlock detected"),
            DeadlockDetectedError("deadlock detected"),
            None,
        ]
        await store._record_access_stats([uuid.uuid4()])
        assert mock_db.execute.await_count == 3
        assert memory_health.snapshot() == {"access_stats_deadlock": 2}

    @pytest.mark.asyncio
    async def test_access_stats_failure_never_aborts_retrieval(self, store, mock_db):
        """hybrid_search still returns the fetched rows when every access-stat
        attempt deadlocks."""
        rows = [
            {"id": uuid.uuid4(), "content": f"m{i}", "token_count": 5} for i in range(3)
        ]
        mock_db.fetch.return_value = rows
        mock_db.execute.side_effect = DeadlockDetectedError("deadlock detected")

        results = await store.hybrid_search("query", [0.1] * 1536)

        assert len(results) == 3
        assert mock_db.execute.await_count == 3  # bounded: initial + 2 retries
        assert memory_health.snapshot() == {"access_stats_deadlock": 3}

    @pytest.mark.asyncio
    async def test_access_stats_non_deadlock_contained_without_retry(
        self, store, mock_db
    ):
        """A non-deadlock error is contained on the first attempt, no retry."""
        mock_db.execute.side_effect = ValueError("boom")
        await store._record_access_stats([uuid.uuid4()])
        assert mock_db.execute.await_count == 1
        assert memory_health.snapshot() == {"access_stats_error": 1}

    @pytest.mark.asyncio
    async def test_hybrid_search_counts_retrieval_deadlock(self, store, mock_db):
        """A deadlocked RRF read is counted, then re-raised (outer containment)."""
        mock_db.fetch.side_effect = DeadlockDetectedError("deadlock detected")
        with pytest.raises(DeadlockDetectedError):
            await store.hybrid_search("query", [0.1] * 1536)
        assert memory_health.snapshot() == {"retrieval_deadlock": 1}

    def test_snapshot_none_when_all_zero(self):
        """Healthy store reports no payload at all."""
        assert memory_health.snapshot() is None
        memory_health.increment("retrieval_deadlock")
        assert memory_health.snapshot() == {"retrieval_deadlock": 1}
        memory_health.reset()
        assert memory_health.snapshot() is None


# =============================================================================
# Integration Tests (require DATABASE_URL)
# =============================================================================


@pytest.mark.skipif(
    not os.getenv("RUN_INTEGRATION_TESTS"),
    reason="Set RUN_INTEGRATION_TESTS=1 to run integration tests",
)
class TestIntegration:
    """Integration tests requiring a live PostgreSQL database with pgvector."""

    @pytest_asyncio.fixture
    async def live_db(self):
        """Create a live database connection."""
        from agent.database.postgres_db import PostgresDB

        db = PostgresDB()
        await db.connect()

        # Ensure schema exists
        async with db.acquire() as conn:
            db_dir = (
                __import__("pathlib").Path(__file__).parent.parent
                / "src"
                / "orchestrator"
                / "database"
            )
            await conn.execute((db_dir / "schema.sql").read_text())
            await conn.execute((db_dir / "vector_schema.sql").read_text())

        yield db
        await db.close()

    @pytest_asyncio.fixture
    async def live_store(self, live_db):
        """Create a RecallStore with live database."""
        from shared.runtime.services.embedding_service import get_embedding_service

        job_id = uuid.uuid4()

        # Create a test job first
        await live_db.execute(
            "INSERT INTO jobs (id, description, status) VALUES ($1, $2, $3)",
            job_id,
            "Integration test job",
            "created",
        )

        store = RecallStore(
            db=live_db,
            embedding_service=get_embedding_service(),
            job_id=job_id,
            config=MockMemoryConfig(),
        )

        yield store

        # Cleanup
        await live_db.execute("DELETE FROM memories WHERE job_id = $1", job_id)
        await live_db.execute("DELETE FROM jobs WHERE id = $1", job_id)

    @pytest.mark.asyncio
    async def test_integration_store_and_retrieve(self, live_store):
        """Store a memory and retrieve it via hybrid search."""
        mem_id = await live_store.store(
            content="The config system uses YAML with $extends inheritance.",
            summary="Config uses YAML inheritance",
            keywords=["config", "yaml", "extends", "inheritance"],
            importance=0.8,
            memory_type="factual",
            source="observer",
        )

        assert mem_id is not None

        # Retrieve via hybrid search
        memories = await live_store.retrieve("How does config inheritance work?")
        assert len(memories) > 0
        assert any("config" in m.content.lower() for m in memories)

    @pytest.mark.asyncio
    async def test_integration_dedup(self, live_store):
        """Dedup prevents storing identical memories."""
        id1 = await live_store.store(
            content="PostgreSQL uses asyncpg for async connections.",
            importance=0.7,
        )
        id2 = await live_store.store(
            content="PostgreSQL uses asyncpg for async connections.",
            importance=0.8,
        )

        # Both should return the same ID (dedup)
        assert id1 == id2

    @pytest.mark.asyncio
    async def test_integration_stats(self, live_store):
        """Stats correctly count stored memories."""
        await live_store.store(
            content="Fact one about the system.",
            importance=0.6,
            memory_type="factual",
            source="todo",
        )
        await live_store.store(
            content="How to configure the agent properly.",
            importance=0.7,
            memory_type="procedural",
            source="observer",
        )

        stats = await live_store.get_stats()
        assert stats["total"] >= 2
