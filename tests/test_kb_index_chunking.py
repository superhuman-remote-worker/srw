"""Tests for OKF KB slice 3 PR1 — chunk-granular index store surface.

Design: knowledge-base/knowledge/features/okf_knowledge_base.md §5.1 / §11 slice-3 PR1.

PR1 is the SCHEMA + STORE surface, inert: it adds the persistence primitives the
git-tree reindexer (PR3) writes and the retrieval cutover (PR4) reads, but nothing
in the running system calls them yet. These tests pin the SQL each primitive
composes and the shapes it returns, against the 0008_kb_index_chunking.sql schema.

Covers:
  - KbWatermark / KnowledgeChunk dataclasses (from_row mapping)
  - KnowledgeStore watermark CRUD: get_watermark, upsert_watermark
  - KnowledgeStore.get_indexed_blob_shas — {path: blob_sha} for tree-diff self-heal
  - KnowledgeStore.upsert_kb_note — note-level row keyed by (kb_id, path)
  - KnowledgeStore.replace_note_chunks — atomic delete+insert of a note's chunks
  - KnowledgeStore.delete_kb_note — remove a note (cascade reaps its chunks)
"""

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.runtime.services.knowledge_store import (
    NOTE_ID_MAX,
    KbWatermark,
    KnowledgeChunk,
    KnowledgeStore,
)


STATUS_MIGRATION = (
    Path(__file__).parents[1]
    / "src/orchestrator/database/migrations/vector/0011_kb_watermark_status.sql"
)


def _make_store():
    """KnowledgeStore with mocked async db + embedding service."""
    mock_db = AsyncMock()
    mock_embed = AsyncMock()
    mock_embed.embed = AsyncMock(return_value=[0.1, 0.2, 0.3])
    return KnowledgeStore(db=mock_db, embedding_service=mock_embed), mock_db, mock_embed


# =============================================================================
# KbWatermark dataclass
# =============================================================================


class TestKbWatermarkFromRow:
    def test_status_migration_adds_operational_fields_and_constraint(self):
        sql = STATUS_MIGRATION.read_text()
        for field in (
            "source_head",
            "status",
            "last_attempt_at",
            "last_success_at",
            "last_error",
        ):
            assert field in sql
        assert "pending" in sql
        assert "indexing" in sql
        assert "partial" in sql
        assert "failed" in sql

    def test_maps_all_fields(self):
        kb = uuid.uuid4()
        wm = KbWatermark.from_row(
            {
                "kb_id": kb,
                "repo_name": "project-68137e29-jobs",
                "branch": "main",
                "indexed_commit": "abc123",
                "pipeline_version": "qwen3-8b:4096:v1",
                "updated_at": "2026-07-05T00:00:00Z",
            }
        )
        assert wm.kb_id == kb
        assert wm.repo_name == "project-68137e29-jobs"
        assert wm.branch == "main"
        assert wm.indexed_commit == "abc123"
        assert wm.pipeline_version == "qwen3-8b:4096:v1"
        assert wm.updated_at == "2026-07-05T00:00:00Z"

    def test_missing_keys_default_to_none(self):
        wm = KbWatermark.from_row({"kb_id": uuid.uuid4()})
        assert wm.repo_name is None
        assert wm.branch is None
        assert wm.indexed_commit is None
        assert wm.pipeline_version is None
        assert wm.updated_at is None


# =============================================================================
# get_watermark
# =============================================================================


class TestGetWatermark:
    @pytest.mark.asyncio
    async def test_selects_by_kb_id_and_parses(self):
        store, mock_db, _ = _make_store()
        kb = uuid.uuid4()
        mock_db.fetchrow.return_value = {
            "kb_id": kb,
            "repo_name": "r",
            "branch": "main",
            "indexed_commit": "deadbeef",
            "pipeline_version": "v1",
            "updated_at": None,
        }
        wm = await store.get_watermark(kb)
        query, *params = mock_db.fetchrow.call_args[0]
        assert "kb_index_watermark" in query
        assert "kb_id = $1" in query
        assert params == [kb]
        assert wm.indexed_commit == "deadbeef"

    @pytest.mark.asyncio
    async def test_returns_none_when_absent(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = None
        assert await store.get_watermark(uuid.uuid4()) is None


class TestReindexAdvisoryLock:
    @pytest.mark.asyncio
    async def test_nonblocking_claim_is_released_on_exit(self):
        conn = AsyncMock()
        conn.fetchval.side_effect = [True, True]
        db = MagicMock()

        @asynccontextmanager
        async def acquire():
            yield conn

        db.acquire = acquire
        store = KnowledgeStore(db=db, embedding_service=AsyncMock())
        kb_id = uuid.uuid4()

        async with store.try_reindex_lock(kb_id) as claimed:
            assert claimed is True

        assert "pg_try_advisory_lock" in conn.fetchval.await_args_list[0].args[0]
        assert "pg_advisory_unlock" in conn.fetchval.await_args_list[1].args[0]
        assert (
            conn.fetchval.await_args_list[0].args[1]
            == (conn.fetchval.await_args_list[1].args[1])
        )

    @pytest.mark.asyncio
    async def test_blocking_delete_claim_uses_the_same_key_and_releases(self):
        conn = AsyncMock()
        transaction = AsyncMock()
        transaction.__aenter__.return_value = transaction
        transaction.__aexit__.return_value = False
        conn.transaction = MagicMock(return_value=transaction)
        db = MagicMock()
        acquire_count = 0

        @asynccontextmanager
        async def acquire():
            nonlocal acquire_count
            acquire_count += 1
            assert acquire_count == 1, "nested pool acquisition would deadlock"
            yield conn

        db.acquire = acquire
        store = KnowledgeStore(db=db, embedding_service=None)
        kb_id = uuid.uuid4()

        async with store.reindex_lock(kb_id) as lock_conn:
            await store.delete_kb_index(kb_id, conn=lock_conn)

        acquire_call, release_call = conn.fetchval.await_args_list
        assert acquire_count == 1
        assert "pg_advisory_lock" in acquire_call.args[0]
        assert "pg_try_advisory_lock" not in acquire_call.args[0]
        assert "pg_advisory_unlock" in release_call.args[0]
        assert acquire_call.args[1] == release_call.args[1]
        assert conn.execute.await_count == 2


# =============================================================================
# upsert_watermark
# =============================================================================


class TestUpsertWatermark:
    @pytest.mark.asyncio
    async def test_inserts_on_conflict_updates(self):
        store, mock_db, _ = _make_store()
        kb = uuid.uuid4()
        await store.upsert_watermark(
            kb_id=kb,
            repo_name="project-68137e29-jobs",
            branch="main",
            indexed_commit="abc123",
            pipeline_version="qwen3-8b:4096:v1",
        )
        query, *params = mock_db.execute.call_args[0]
        assert "INSERT INTO kb_index_watermark" in query
        assert "ON CONFLICT (kb_id) DO UPDATE" in query
        # indexed_commit + pipeline_version must be in both the VALUES and the SET
        assert query.count("indexed_commit") >= 2
        assert query.count("pipeline_version") >= 2
        assert params[0] == kb
        assert "abc123" in params
        assert "qwen3-8b:4096:v1" in params

    @pytest.mark.asyncio
    async def test_stamps_updated_at_via_now(self):
        store, mock_db, _ = _make_store()
        await store.upsert_watermark(
            kb_id=uuid.uuid4(),
            repo_name="r",
            branch="main",
            indexed_commit="c",
            pipeline_version="v",
        )
        query = mock_db.execute.call_args[0][0]
        assert "NOW()" in query

    @pytest.mark.asyncio
    async def test_status_update_does_not_advance_indexed_commit(self):
        store, mock_db, _ = _make_store()
        kb = uuid.uuid4()

        await store.set_watermark_status(
            kb,
            "partial",
            source_head="f" * 40,
            last_error="one note failed",
            repo_name=f"datasource:{kb}",
            branch="main",
        )

        query, *params = mock_db.execute.await_args.args
        assert "source_head" in query
        assert "status" in query
        assert "indexed_commit" not in query
        assert "partial" in params
        assert "one note failed" in params


class TestDeleteKbIndex:
    @pytest.mark.asyncio
    async def test_deletes_only_one_kb_notes_and_watermark(self):
        conn = AsyncMock()
        transaction = AsyncMock()
        transaction.__aenter__.return_value = transaction
        transaction.__aexit__.return_value = False
        conn.transaction = MagicMock(return_value=transaction)
        db = MagicMock()

        @asynccontextmanager
        async def acquire():
            yield conn

        db.acquire = acquire
        store = KnowledgeStore(db=db, embedding_service=None)
        kb = uuid.uuid4()

        await store.delete_kb_index(kb)

        first, second = conn.execute.await_args_list
        assert "knowledge_index WHERE kb_id = $1" in first.args[0]
        assert "kb_index_watermark WHERE kb_id = $1" in second.args[0]
        assert first.args[1] == kb
        assert second.args[1] == kb


# =============================================================================
# get_indexed_blob_shas
# =============================================================================


class TestGetIndexedBlobShas:
    @pytest.mark.asyncio
    async def test_returns_path_to_blob_sha_map(self):
        store, mock_db, _ = _make_store()
        kb = uuid.uuid4()
        mock_db.fetch.return_value = [
            {"path": "knowledge/a.md", "blob_sha": "sha_a"},
            {"path": "knowledge/b.md", "blob_sha": "sha_b"},
        ]
        result = await store.get_indexed_blob_shas(kb)
        assert result == {"knowledge/a.md": "sha_a", "knowledge/b.md": "sha_b"}
        query, *params = mock_db.fetch.call_args[0]
        assert "knowledge_index" in query
        assert "kb_id = $1" in query
        assert "path IS NOT NULL" in query
        assert params == [kb]

    @pytest.mark.asyncio
    async def test_empty_when_nothing_indexed(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        assert await store.get_indexed_blob_shas(uuid.uuid4()) == {}


# =============================================================================
# clear_note_stamps — batched invalidation for a resumable rebuild
# =============================================================================


class TestClearNoteStamps:
    """Batching is the whole point, not an implementation detail.

    ``knowledge_index`` carries an HNSW index plus two GIN indexes, so this
    UPDATE costs ~9ms/row. Shipped as one statement it ran past the pool's 60s
    ``command_timeout`` on a large KB and asyncpg cancelled it — with an
    empty-message ``asyncio.TimeoutError``, so the caller's fallback logged
    nothing useful and the rebuild silently stayed non-resumable.
    """

    @pytest.mark.asyncio
    async def test_keeps_paging_until_a_short_batch(self):
        store, mock_db, _ = _make_store()
        kb = uuid.uuid4()
        mock_db.fetch.side_effect = [
            [{"id": uuid.uuid4()} for _ in range(200)],
            [{"id": uuid.uuid4()} for _ in range(200)],
            [{"id": uuid.uuid4()} for _ in range(37)],
        ]

        assert await store.clear_note_stamps(kb, batch_size=200) == 437
        assert mock_db.fetch.await_count == 3
        query, *params = mock_db.fetch.call_args[0]
        assert "LIMIT $3" in query
        assert params == [kb, None, 200]

    @pytest.mark.asyncio
    async def test_single_short_batch_is_one_statement(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [{"id": uuid.uuid4()}]
        assert await store.clear_note_stamps(uuid.uuid4(), batch_size=200) == 1
        assert mock_db.fetch.await_count == 1

    @pytest.mark.asyncio
    async def test_embedding_version_narrows_to_other_versions(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        assert await store.clear_note_stamps(uuid.uuid4(), embedding_version="v2") == 0
        query, *params = mock_db.fetch.call_args[0]
        assert "embedding_version IS DISTINCT FROM" in query
        assert params[1] == "v2"


# =============================================================================
# KnowledgeChunk dataclass
# =============================================================================


class TestKnowledgeChunkFromRow:
    def test_maps_all_fields(self):
        note_row = uuid.uuid4()
        kb = uuid.uuid4()
        ch = KnowledgeChunk.from_row(
            {
                "id": None,
                "note_row": note_row,
                "kb_id": kb,
                "chunk_ix": 3,
                "heading_path": "Design > Storage",
                "content": "chunk body",
                "embedding_version": "qwen3-8b:4096:v1",
                "created_at": None,
            }
        )
        assert ch.note_row == note_row
        assert ch.kb_id == kb
        assert ch.chunk_ix == 3
        assert ch.heading_path == "Design > Storage"
        assert ch.content == "chunk body"
        assert ch.embedding_version == "qwen3-8b:4096:v1"

    def test_defaults_on_missing(self):
        ch = KnowledgeChunk.from_row({})
        assert ch.chunk_ix == 0
        assert ch.content == ""
        assert ch.heading_path is None
        assert ch.embedding_version is None


# =============================================================================
# upsert_kb_note — note-level row keyed by (kb_id, path)
# =============================================================================


class TestUpsertKbNote:
    @pytest.mark.asyncio
    async def test_populates_search_doc_for_the_sparse_arm(self):
        # The note-level sparse index. upsert_note (the agent-write path)
        # computes it at write time; without the same column here, every note
        # that arrives through the reindexer — i.e. every note in a vault that
        # was seeded by pushing files — is invisible to the note-level search
        # the orchestrator's /knowledge/search endpoint runs.
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="seeded-note",
            path="knowledge/features/seeded-note.md",
            title="Seeded Note",
            note_type="learning",
            content="body text worth finding",
            blob_sha="blob123",
            embedding_version="qwen3-8b:4096:v1",
        )
        query, *params = mock_db.fetchval.call_args[0]
        assert "search_doc" in query
        assert "to_tsvector" in query
        assert "search_doc = EXCLUDED.search_doc" in query

    @pytest.mark.asyncio
    async def test_inserts_keyed_by_kb_path_returning_id(self):
        store, mock_db, _ = _make_store()
        row_id = uuid.uuid4()
        mock_db.fetchval.return_value = row_id
        kb = uuid.uuid4()
        result = await store.upsert_kb_note(
            kb_id=kb,
            note_id="chose-jwt-over-oauth",
            path="knowledge/chose-jwt.md",
            title="Chose JWT",
            note_type="decision",
            content="body",
            blob_sha="blob123",
            embedding_version="qwen3-8b:4096:v1",
        )
        assert result == row_id
        query, *params = mock_db.fetchval.call_args[0]
        assert "INSERT INTO knowledge_index" in query
        assert "ON CONFLICT (kb_id, path)" in query
        assert "DO UPDATE" in query
        assert "RETURNING id" in query
        # the reindex-critical columns must be written
        assert kb in params
        assert "knowledge/chose-jwt.md" in params
        assert "blob123" in params
        assert "qwen3-8b:4096:v1" in params

    @pytest.mark.asyncio
    async def test_note_row_carries_no_dense_vector(self):
        # In the chunk model the embedding lives on knowledge_chunks, never the
        # note row — the note INSERT must not touch the `embedding` column.
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
        )
        query = mock_db.fetchval.call_args[0][0]
        # `embedding_version` is fine; a bare `embedding` column is not.
        assert "embedding" not in query.replace("embedding_version", "")

    @pytest.mark.asyncio
    async def test_backfills_project_id_from_kb_for_legacy_filters(self):
        # Legacy queries (get_summary, hybrid_search) still filter project_id;
        # a project-scoped KB's id IS its project_id, so the new row carries it.
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        kb = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=kb,
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
        )
        query, *params = mock_db.fetchval.call_args[0]
        # Both columns are written, and both bind to the same $1 (the kb id) —
        # project_id = kb_id, so the legacy project-scoped filters see the row.
        col_list = query.split("VALUES")[0]
        assert "kb_id" in col_list and "project_id" in col_list
        assert "($1, $1," in query
        assert kb in params

    @pytest.mark.asyncio
    async def test_conflict_update_preserves_created_at(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
        )
        query = mock_db.fetchval.call_args[0][0]
        assert "created_at" not in query.split("DO UPDATE")[1]

    @pytest.mark.asyncio
    async def test_binds_priority_at_its_own_position(self):
        # Mutation-tested (project-backlog-pipeline task 2, fix round 1
        # finding 2): colliding priority's DO-UPDATE placeholder with
        # modified_at's ($20 instead of $21) left this path untested before —
        # pin both the query's SET clause and the bound value's position.
        # No longer the FINAL position: B2 appended $22 (ready_at), and the
        # whole point of this test is that a placeholder collision is caught,
        # which a [-1] pin stops doing the moment something else lands last.
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            priority=0,
        )
        query, *params = mock_db.fetchval.call_args[0]
        # Fix round 2 (Finding 3): the raw SET is now COALESCE($21, ...) --
        # see TestUpsertKbNotePriorityCoalesceSentinel for the sentinel
        # semantics themselves; this test only pins the position.
        assert "priority = COALESCE($21, knowledge_index.priority)" in query
        assert params[20] == 0
        assert "ready_at = COALESCE($22, knowledge_index.ready_at)" in query
        assert params[21] is None


# =============================================================================
# upsert_kb_note seeds the convergence TTL (Slice A task 5): after Slice A
# the file-canonical write is the only writer for a new note, so it must seed
# remaining_cycles the same way upsert_note (the agent-write path) already
# does -- and must never reset it on a later reindex.
# =============================================================================


class TestUpsertKbNoteSeedsTtl:
    """A file-canonical write must seed the convergence TTL, because after
    Slice A nothing else does — and must never reset it on a later reindex."""

    @pytest.mark.asyncio
    async def test_seeds_the_note_types_ttl_on_insert(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="ttl-seed",
            path="knowledge/ttl-seed.md",
            title="TTL seed",
            note_type="state",  # KB_TTL_BY_NOTE_TYPE['state'] == 2
            content="body",
            blob_sha=None,
            embedding_version=None,
        )
        query, *params = mock_db.fetchval.call_args[0]
        # Fix round: pin the column list AND the bound value's own ordinal
        # ($23, params[22]) -- "remaining_cycles" in query / "2 in params"
        # would each pass even if the value landed at the wrong position
        # (swapped with a neighbour) or the identifier only appeared in a
        # comment. The $21/$22 tests above constrain renumbering below this
        # point; this is the tripwire for $23 itself.
        assert "priority, ready_at, remaining_cycles, description)" in query
        assert params[22] == 2

    @pytest.mark.asyncio
    async def test_a_durable_note_type_stays_null(self):
        # KB_TTL_DEFAULT is None: 'decision' notes never count down. Coercing
        # that to a number would put durable knowledge on a countdown.
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="durable",
            path="knowledge/durable.md",
            title="Durable",
            note_type="decision",
            content="body",
            blob_sha=None,
            embedding_version=None,
        )
        _query, *params = mock_db.fetchval.call_args[0]
        # Fix round: `2 not in params` / `3 not in params` waved through the
        # most dangerous coercion -- None becoming 0. Per
        # 0007_knowledge_index_ttl.sql:42-46 the expiry sweep is
        # `WHERE remaining_cycles <= 0 AND status = 'active'`, so a stray 0
        # here would mark every durable note in the vault as already
        # expired. Pin the exact ordinal ($23 / params[22]) to its exact
        # expected value instead of merely excluding two unrelated numbers.
        assert params[22] is None

    @pytest.mark.asyncio
    async def test_on_conflict_never_touches_remaining_cycles(self):
        # The mechanism that preserves a decremented TTL across reindexes: the
        # column is seeded on INSERT and absent from DO UPDATE. If it appeared
        # there, every sweep would re-arm every note's countdown.
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="ttl-keep",
            path="knowledge/ttl-keep.md",
            title="TTL keep",
            note_type="state",
            content="body",
            blob_sha=None,
            embedding_version=None,
        )
        query, *_params = mock_db.fetchval.call_args[0]
        _insert_half, conflict_half = query.split("DO UPDATE", 1)
        assert "remaining_cycles" not in conflict_half


# =============================================================================
# upsert_kb_note retrieval_messages sentinel (Slice A task 5): None must mean
# "leave the stored value alone", the same COALESCE contract
# TestUpsertKbNotePriorityCoalesceSentinel (test_knowledge_store.py) pins for
# priority/ready_at. OKF frontmatter carries no retrieval_messages field, so a
# reindex has no opinion of its own -- without this, a value set through the
# inline materialize call would be wiped back to empty by the very next sweep.
# =============================================================================


class TestUpsertKbNoteDescription:
    """The file's description lands on the row, and a file without one clears
    it: unlike priority or job_id there is no other writer to protect."""

    async def _upsert(self, **kw):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha=None,
            embedding_version=None,
            **kw,
        )
        query, *params = mock_db.fetchval.call_args[0]
        return query, params

    @pytest.mark.asyncio
    async def test_binds_the_description_as_the_last_parameter(self):
        query, params = await self._upsert(description="Why JWT.")
        assert "remaining_cycles, description)" in query
        assert "$23, $24)" in query
        assert params[23] == "Why JWT."

    @pytest.mark.asyncio
    async def test_overwrites_on_conflict_without_a_sentinel(self):
        query, params = await self._upsert()
        assert "description = $24" in query
        assert "COALESCE($24" not in query
        assert params[23] is None


class TestUpsertKbNoteRetrievalMessagesCoalesceSentinel:
    @pytest.mark.asyncio
    async def test_coalesces_none_against_existing_row_on_conflict(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            retrieval_messages=None,
        )
        query, *params = mock_db.fetchval.call_args[0]
        assert (
            "retrieval_messages = COALESCE($11::text[], "
            "knowledge_index.retrieval_messages)" in query
        )
        # This layer must never turn None into [] itself -- that decision
        # belongs entirely to the SQL COALESCE against the live row.
        assert params[10] is None

    @pytest.mark.asyncio
    async def test_coalesces_none_to_empty_array_for_a_fresh_row(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            retrieval_messages=None,
        )
        query = mock_db.fetchval.call_args[0][0]
        assert "COALESCE($11::text[], '{}'::text[])" in query

    @pytest.mark.asyncio
    async def test_explicit_retrieval_messages_still_win(self):
        """Regression guard: the sentinel must not interfere with a real,
        caller-supplied value."""
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            retrieval_messages=["what auth approach?"],
        )
        _query, *params = mock_db.fetchval.call_args[0]
        assert params[10] == ["what auth approach?"]


# =============================================================================
# upsert_kb_note job_id / phase sentinels (Slice A task 6 fix round): the same
# COALESCE contract as priority/ready_at/retrieval_messages above. Both columns
# were written unconditionally, so the sweep -- which knows no job and no phase
# -- NULLed them on its next pass over any note. job_id backs a shipped filter
# (list_notes' `job_id = $n` clause, behind kb_list(job_id=...)), so a NULL
# there is the difference between "which notes did job X write?" answering and
# answering nothing.
# =============================================================================


class TestUpsertKbNoteProvenanceCoalesceSentinel:
    @pytest.mark.asyncio
    async def test_job_id_coalesces_against_the_existing_row_on_conflict(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            job_id=None,
        )
        query, *params = mock_db.fetchval.call_args[0]
        assert "job_id = COALESCE($16, knowledge_index.job_id)" in query
        # Pin the ordinal, not just the identifier: $16 is mid-statement, so a
        # renumbering slip would bind job_id into invalidated_at or phase and
        # a presence-only assertion would stay green.
        assert params[15] is None

    @pytest.mark.asyncio
    async def test_phase_coalesces_against_the_existing_row_on_conflict(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            phase=None,
        )
        query, *params = mock_db.fetchval.call_args[0]
        assert "phase = COALESCE($17, knowledge_index.phase)" in query
        assert params[16] is None

    @pytest.mark.asyncio
    async def test_explicit_provenance_still_wins(self):
        """Regression guard: the sentinel must not swallow a real value."""
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        writing_job = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            job_id=writing_job,
            phase=3,
        )
        _query, *params = mock_db.fetchval.call_args[0]
        assert params[15] == writing_job
        assert params[16] == 3

    @pytest.mark.asyncio
    async def test_a_fresh_row_binds_them_raw(self):
        # The INSERT half must stay raw: a new row has no prior value to
        # preserve, and `knowledge_index.job_id` is not even addressable in a
        # VALUES list. This pins the asymmetry so a later "tidy-up" cannot
        # make both halves match and break the statement.
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
        )
        query = mock_db.fetchval.call_args[0][0]
        insert_half, _conflict_half = query.split("ON CONFLICT", 1)
        assert "$16, $17," in insert_half
        assert "COALESCE($16" not in insert_half
        assert "COALESCE($17" not in insert_half

    @pytest.mark.asyncio
    async def test_modified_at_coalesces_against_the_existing_row_on_conflict(self):
        # modified_at orders the search function's recency arm — a bare
        # ORDER BY ki.modified_at DESC, with no NULLS LAST. A file with no
        # `modified:` line carries no opinion, so a replay must not blank the
        # stored one; Postgres sorts NULLs FIRST under DESC, so blanking it
        # would hoist the row to the top of that arm rather than bury it.
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            modified_at=None,
        )
        query, *params = mock_db.fetchval.call_args[0]
        assert "modified_at = COALESCE($20, knowledge_index.modified_at)" in query
        assert params[19] is None

    @pytest.mark.asyncio
    async def test_an_explicit_modified_at_still_wins(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        stamp = datetime(2026, 2, 20, 9, 0, tzinfo=timezone.utc)
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="learning",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            modified_at=stamp,
        )
        _query, *params = mock_db.fetchval.call_args[0]
        assert params[19] == stamp


# =============================================================================
# replace_note_chunks — atomic delete + insert of a note's chunks
# =============================================================================


class TestReplaceNoteChunks:
    @pytest.mark.asyncio
    async def test_deletes_then_inserts_each_chunk(self):
        store, mock_db, _ = _make_store()
        note_row = uuid.uuid4()
        kb = uuid.uuid4()
        chunks = [
            {
                "chunk_ix": 0,
                "heading_path": "Intro",
                "content": "first",
                "embedding": [0.1, 0.2],
            },
            {
                "chunk_ix": 1,
                "heading_path": "Design > Storage",
                "content": "second",
                "embedding": [0.3, 0.4],
            },
        ]
        n = await store.replace_note_chunks(
            note_row=note_row,
            kb_id=kb,
            chunks=chunks,
            embedding_version="v1",
        )
        assert n == 2
        calls = mock_db.execute.call_args_list
        # first call clears the note's existing chunks
        assert "DELETE FROM knowledge_chunks" in calls[0][0][0]
        assert calls[0][0][1] == note_row
        # then one INSERT per chunk
        insert_calls = [c for c in calls if "INSERT INTO knowledge_chunks" in c[0][0]]
        assert len(insert_calls) == 2
        first_insert = insert_calls[0]
        assert "to_tsvector" in first_insert[0][0]  # search_doc populated at write
        assert note_row in first_insert[0]
        assert kb in first_insert[0]
        assert "first" in first_insert[0]
        assert "v1" in first_insert[0]

    @pytest.mark.asyncio
    async def test_empty_chunks_clears_and_returns_zero(self):
        store, mock_db, _ = _make_store()
        note_row = uuid.uuid4()
        n = await store.replace_note_chunks(
            note_row=note_row,
            kb_id=uuid.uuid4(),
            chunks=[],
            embedding_version="v1",
        )
        assert n == 0
        calls = mock_db.execute.call_args_list
        assert len(calls) == 1
        assert "DELETE FROM knowledge_chunks" in calls[0][0][0]

    @pytest.mark.asyncio
    async def test_embedding_passed_through_prepare(self):
        # A legacy string embedding must be normalized to a list before the codec.
        store, mock_db, _ = _make_store()
        await store.replace_note_chunks(
            note_row=uuid.uuid4(),
            kb_id=uuid.uuid4(),
            chunks=[{"chunk_ix": 0, "content": "c", "embedding": "[0.1,0.2]"}],
            embedding_version="v1",
        )
        insert_call = [
            c
            for c in mock_db.execute.call_args_list
            if "INSERT INTO knowledge_chunks" in c[0][0]
        ][0]
        assert [0.1, 0.2] in insert_call[0]


# =============================================================================
# adopt_legacy_row — claim a legacy row or move a stable id after a Git rename
# =============================================================================


class TestAdoptLegacyRow:
    @pytest.mark.asyncio
    async def test_claims_pathless_row_and_returns_id(self):
        # A legacy slice-1/2 row has (project_id, note_id) but path IS NULL.
        # Adopting it sets path (and kb_id) so upsert_kb_note's (kb_id, path)
        # INSERT can't unique-violate on uq_knowledge_project_note.
        store, mock_db, _ = _make_store()
        row_id = uuid.uuid4()
        mock_db.fetchval.return_value = row_id
        kb = uuid.uuid4()
        result = await store.adopt_legacy_row(kb, "chose-jwt", "knowledge/chose-jwt.md")
        assert result == row_id
        query, *params = mock_db.fetchval.call_args[0]
        assert "UPDATE knowledge_index" in query
        assert "path IS DISTINCT FROM $3" in query
        assert "note.path IS NULL OR note.path = ANY($4::text[])" in query
        assert "project_id = $1" in query
        assert "note_id = $2" in query
        assert "RETURNING note.id" in query
        assert "destination.path = $3" in query  # never steals another row
        # sets BOTH kb_id and path on the claimed row
        set_clause = query.split("SET")[1].split("WHERE")[0]
        assert "kb_id" in set_clause and "path" in set_clause
        assert params == [kb, "chose-jwt", "knowledge/chose-jwt.md", []]

    @pytest.mark.asyncio
    async def test_moves_existing_same_id_row_to_renamed_path(self):
        """A stable frontmatter id is the identity across a Git path rename."""
        store, mock_db, _ = _make_store()
        row_id = uuid.uuid4()
        mock_db.fetchval.return_value = row_id
        kb = uuid.uuid4()

        result = await store.adopt_legacy_row(
            kb,
            "stable-id",
            "knowledge/new-name.md",
            movable_paths=["knowledge/old-name.md"],
        )

        assert result == row_id
        query, *params = mock_db.fetchval.call_args[0]
        assert "note.path IS DISTINCT FROM $3" in query
        assert "note.path IS NULL OR note.path = ANY($4::text[])" in query
        assert "NOT EXISTS" in query
        assert params == [
            kb,
            "stable-id",
            "knowledge/new-name.md",
            ["knowledge/old-name.md"],
        ]

    @pytest.mark.asyncio
    async def test_duplicate_id_cannot_move_a_still_canonical_path(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = None
        kb = uuid.uuid4()

        await store.adopt_legacy_row(
            kb,
            "duplicate-id",
            "knowledge/second.md",
            movable_paths=[],
        )

        query, *params = mock_db.fetchval.call_args[0]
        assert "note.path IS NULL OR note.path = ANY($4::text[])" in query
        assert params[-1] == []

    @pytest.mark.asyncio
    async def test_returns_none_when_no_legacy_row(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = None
        assert await store.adopt_legacy_row(uuid.uuid4(), "n", "knowledge/n.md") is None


# =============================================================================
# delete_kb_note — remove a note (cascade reaps its chunks)
# =============================================================================


class TestDeleteKbNote:
    @pytest.mark.asyncio
    async def test_deletes_by_kb_and_path(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        kb = uuid.uuid4()
        assert await store.delete_kb_note(kb, "knowledge/n.md") is True
        query, *params = mock_db.fetchval.call_args[0]
        assert "DELETE FROM knowledge_index" in query
        assert "kb_id = $1" in query
        assert "path = $2" in query
        assert "RETURNING id" in query
        assert params == [kb, "knowledge/n.md"]

    @pytest.mark.asyncio
    async def test_returns_false_when_absent(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = None
        assert await store.delete_kb_note(uuid.uuid4(), "knowledge/x.md") is False


# =============================================================================
# KnowledgeStore.stamp_note_indexed — the chunks-are-durable stamp (PR3.1)
# =============================================================================


class TestStampNoteIndexed:
    """blob_sha/embedding_version mean "this note's chunks are fully written".
    The reindexer upserts the note UNSTAMPED, writes chunks, then stamps — so a
    chunk-write failure leaves blob_sha NULL and the next diff retries the note
    (live gap 2026-07-05: notes stamped, zero chunks, self-heal defeated)."""

    @pytest.mark.asyncio
    async def test_stamps_blob_sha_and_version_by_row_id(self):
        store, db, _ = _make_store()
        row_id = uuid.uuid4()
        await store.stamp_note_indexed(row_id, "abc123", "m:4096:c1")
        query = db.execute.await_args[0][0]
        params = db.execute.await_args[0][1:]
        assert "UPDATE knowledge_index" in query
        assert "blob_sha" in query
        assert "embedding_version" in query
        assert "WHERE id = $1" in query
        assert params == (row_id, "abc123", "m:4096:c1")

    @pytest.mark.asyncio
    async def test_no_centroid_leaves_embedding_untouched(self):
        # Backward-compatible: without a centroid the stamp must not clobber the
        # note row's embedding column (an empty-body note has no centroid).
        store, db, _ = _make_store()
        await store.stamp_note_indexed(uuid.uuid4(), "abc123", "m:4096:c1")
        query = db.execute.await_args[0][0]
        assert "embedding =" not in query

    @pytest.mark.asyncio
    async def test_centroid_written_to_embedding_column(self):
        # PR4d: the reindexer's whole-note centroid lands on the note row so
        # find_near_duplicate_pairs (which filters embedding IS NOT NULL) sees
        # reindexed notes again. Atomic with the stamp — same UPDATE.
        store, db, _ = _make_store()
        row_id = uuid.uuid4()
        centroid = [0.1, 0.2, 0.3]
        await store.stamp_note_indexed(row_id, "abc123", "m:4096:c1", centroid=centroid)
        query = db.execute.await_args[0][0]
        params = db.execute.await_args[0][1:]
        assert "UPDATE knowledge_index" in query
        assert "embedding = $4" in query
        assert params == (row_id, "abc123", "m:4096:c1", centroid)


# =============================================================================
# KnowledgeStore.replace_note_links — the body-link edge set for the 1-hop
# graph-tool degrade when Neo4j is absent (slice-3 PR4c)
# =============================================================================


class TestReplaceNoteLinks:
    """Body markdown links become rows the reindexer rewrites per note (delete +
    re-insert, mirroring replace_note_chunks). Targets are stored as slugs and
    resolved to note rows at read time — so a link to a not-yet-indexed note is
    kept, not dropped (dead links fall out of the read-time join naturally)."""

    @pytest.mark.asyncio
    async def test_deletes_then_inserts_each_target(self):
        store, mock_db, _ = _make_store()
        note_row = uuid.uuid4()
        kb = uuid.uuid4()
        n = await store.replace_note_links(
            source_note_row=note_row,
            kb_id=kb,
            source_id="note-a",
            targets=["note-b", "note-c"],
        )
        assert n == 2
        calls = mock_db.execute.call_args_list
        assert "DELETE FROM knowledge_links" in calls[0][0][0]
        assert calls[0][0][1] == note_row
        insert_calls = [c for c in calls if "INSERT INTO knowledge_links" in c[0][0]]
        assert len(insert_calls) == 2
        first = insert_calls[0][0]
        assert note_row in first
        assert kb in first
        assert "note-a" in first  # source_id denormalized for query
        assert "note-b" in first  # target_id (a slug, resolved at read time)

    @pytest.mark.asyncio
    async def test_empty_targets_clears_and_returns_zero(self):
        store, mock_db, _ = _make_store()
        note_row = uuid.uuid4()
        n = await store.replace_note_links(
            source_note_row=note_row, kb_id=uuid.uuid4(), source_id="a", targets=[]
        )
        assert n == 0
        calls = mock_db.execute.call_args_list
        assert len(calls) == 1
        assert "DELETE FROM knowledge_links" in calls[0][0][0]

    @pytest.mark.asyncio
    async def test_dedupes_repeated_targets(self):
        store, mock_db, _ = _make_store()
        n = await store.replace_note_links(
            source_note_row=uuid.uuid4(),
            kb_id=uuid.uuid4(),
            source_id="a",
            targets=["b", "b", "c"],
        )
        assert n == 2  # one edge per distinct target
        insert_calls = [
            c
            for c in mock_db.execute.call_args_list
            if "INSERT INTO knowledge_links" in c[0][0]
        ]
        assert len(insert_calls) == 2

    @pytest.mark.asyncio
    async def test_default_rel_type_is_references(self):
        store, mock_db, _ = _make_store()
        await store.replace_note_links(
            source_note_row=uuid.uuid4(),
            kb_id=uuid.uuid4(),
            source_id="a",
            targets=["b"],
        )
        insert = [
            c
            for c in mock_db.execute.call_args_list
            if "INSERT INTO knowledge_links" in c[0][0]
        ][0][0]
        assert "references" in insert

    @pytest.mark.asyncio
    async def test_over_long_target_is_clamped_to_the_column_bound(self):
        """Live wedge: 17 archive notes could never finish indexing.

        source_id/target_id are VARCHAR(100). An unclamped long `[[wikilink]]`
        raised "value too long for type character varying(100)" here, and since
        links are written before stamp_note_indexed the note stayed unstamped —
        so every later sweep retried it and failed the same way, pinning the
        whole KB at `partial`.
        """
        store, mock_db, _ = _make_store()
        long_target = "x" * 250
        n = await store.replace_note_links(
            source_note_row=uuid.uuid4(),
            kb_id=uuid.uuid4(),
            source_id="y" * 250,
            targets=[long_target],
        )
        assert n == 1
        insert = [
            c
            for c in mock_db.execute.call_args_list
            if "INSERT INTO knowledge_links" in c[0][0]
        ][0][0]
        source_id, target_id = insert[3], insert[4]
        assert len(target_id) == NOTE_ID_MAX
        assert target_id == long_target[:NOTE_ID_MAX]
        assert len(source_id) == NOTE_ID_MAX

    @pytest.mark.asyncio
    async def test_targets_differing_past_the_bound_collapse_to_one_edge(self):
        """Clamp before dedupe, not after.

        Both of these resolve to the same note, so writing two edges would be
        redundant — and the dedupe is the only thing keeping them apart.
        """
        store, mock_db, _ = _make_store()
        prefix = "z" * NOTE_ID_MAX
        n = await store.replace_note_links(
            source_note_row=uuid.uuid4(),
            kb_id=uuid.uuid4(),
            source_id="a",
            targets=[prefix + "-one", prefix + "-two"],
        )
        assert n == 1

    @pytest.mark.asyncio
    async def test_target_at_the_bound_is_untouched(self):
        store, mock_db, _ = _make_store()
        exact = "q" * NOTE_ID_MAX
        await store.replace_note_links(
            source_note_row=uuid.uuid4(),
            kb_id=uuid.uuid4(),
            source_id="a",
            targets=[exact],
        )
        insert = [
            c
            for c in mock_db.execute.call_args_list
            if "INSERT INTO knowledge_links" in c[0][0]
        ][0][0]
        assert insert[4] == exact


# =============================================================================
# KnowledgeStore.get_related_notes — 1-hop link neighbours (the kg-less
# kb_related backend), resolved and active-only at read time (slice-3 PR4c)
# =============================================================================


class TestGetRelatedNotes:
    @pytest.mark.asyncio
    async def test_returns_one_hop_neighbours(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [
            {"id": "note-b", "title": "B", "type": "decision", "status": "active"},
        ]
        result = await store.get_related_notes(kb_id=uuid.uuid4(), note_id="note-a")
        assert len(result) == 1
        assert result[0]["id"] == "note-b"
        assert result[0]["distance"] == 1
        assert result[0]["rel_types"] == ["references"]

    @pytest.mark.asyncio
    async def test_query_is_bidirectional_active_only_kb_scoped(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        kb = uuid.uuid4()
        await store.get_related_notes(kb_id=kb, note_id="note-a", limit=7)
        sql, *params = mock_db.fetch.call_args[0]
        # Neighbours where note is the source (outbound) OR the target (inbound).
        assert "source_id" in sql and "target_id" in sql
        # Joined to knowledge_index for title/type/status, active-only.
        assert "knowledge_index" in sql
        assert "status = 'active'" in sql
        assert kb in params
        assert "note-a" in params
        assert 7 in params

    @pytest.mark.asyncio
    async def test_empty_when_no_links(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        assert await store.get_related_notes(uuid.uuid4(), "x") == []


# =============================================================================
# KnowledgeStore.get_note_by_slug — the kg-less kb_read backend (slice-3 PR4c).
# Reads a full note from the pgvector index by (kb_id, note_id), returning the
# same dict shape kg.read_note does so the tool formats both worlds identically.
# =============================================================================


class TestGetNoteBySlug:
    @pytest.mark.asyncio
    async def test_returns_note_dict_shaped_like_kg(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = {
            "note_id": "chose-jwt",
            "title": "Chose JWT",
            "note_type": "decision",
            "status": "active",
            "content": "body",
            "confidence": "high",
            "tags": ["auth"],
            "keywords": ["jwt"],
            "job_id": None,
            "phase": 2,
            "created_at": None,
            "modified_at": None,
        }
        result = await store.get_note_by_slug(kb_id=uuid.uuid4(), note_id="chose-jwt")
        # kg.read_note keys: id / type (not note_id / note_type)
        assert result["id"] == "chose-jwt"
        assert result["type"] == "decision"
        assert result["title"] == "Chose JWT"
        assert result["content"] == "body"
        assert result["tags"] == ["auth"]
        assert result["confidence"] == "high"

    @pytest.mark.asyncio
    async def test_scopes_by_kb_and_note_status_agnostic(self):
        # kb_read must find a note of ANY status (superseded/archived included).
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = None
        kb = uuid.uuid4()
        await store.get_note_by_slug(kb_id=kb, note_id="n1")
        sql, *params = mock_db.fetchrow.call_args[0]
        assert "knowledge_index" in sql
        assert "kb_id = $1" in sql
        assert "note_id = $2" in sql
        assert "'active'" not in sql  # no active-only filter on a direct read
        assert kb in params
        assert "n1" in params

    @pytest.mark.asyncio
    async def test_returns_none_when_absent(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = None
        assert await store.get_note_by_slug(uuid.uuid4(), "missing") is None


# =============================================================================
# KnowledgeStore.list_notes — the kg-less kb_list backend (slice-3 PR4c).
# =============================================================================


class TestListNotes:
    @pytest.mark.asyncio
    async def test_returns_summary_dicts_shaped_like_kg(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [
            {
                "note_id": "n1",
                "title": "T",
                "note_type": "decision",
                "status": "active",
                "confidence": "high",
            }
        ]
        result = await store.list_notes(kb_id=uuid.uuid4())
        assert result[0]["id"] == "n1"
        assert result[0]["type"] == "decision"
        assert result[0]["confidence"] == "high"

    @pytest.mark.asyncio
    async def test_applies_type_status_job_and_tag_filters(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        kb = uuid.uuid4()
        await store.list_notes(
            kb_id=kb,
            note_type="decision",
            tag="auth",
            status="active",
            job_id="job-1",
        )
        sql, *params = mock_db.fetch.call_args[0]
        assert "kb_id" in sql
        assert "note_type =" in sql
        assert "status =" in sql
        assert "job_id =" in sql
        assert "ANY(tags)" in sql  # tag membership against the array column
        assert kb in params
        assert "decision" in params
        assert "active" in params
        assert "auth" in params

    @pytest.mark.asyncio
    async def test_no_filters_lists_all_in_kb(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        kb = uuid.uuid4()
        await store.list_notes(kb_id=kb)
        sql, *params = mock_db.fetch.call_args[0]
        assert "kb_id = $1" in sql
        assert "note_type =" not in sql  # no type filter clause when unfiltered
        assert kb in params


# =============================================================================
# Duplicate note id across two paths — detection + one-line diagnostic
# =============================================================================


class TestFindNoteIdOwner:
    """The lookup that names which path the index is actually holding."""

    @pytest.mark.asyncio
    async def test_returns_incumbent_row(self):
        store, mock_db, _ = _make_store()
        kb = uuid.uuid4()
        row_id = uuid.uuid4()
        mock_db.fetchrow.return_value = {
            "id": row_id,
            "path": "knowledge/plan.md/dup.md",
            "status": "active",
            "indexed_at": None,
        }

        result = await store.find_note_id_owner(kb, "dup")

        assert result["path"] == "knowledge/plan.md/dup.md"
        assert result["id"] == row_id
        sql, *params = mock_db.fetchrow.call_args[0]
        # Keyed on project_id (not kb_id): the constraint that fires is
        # uq_knowledge_project_note, and a pathless legacy row has no kb_id.
        assert "project_id = $1" in sql
        assert "note_id = $2" in sql
        assert params == [kb, "dup"]

    @pytest.mark.asyncio
    async def test_returns_none_when_nothing_owns_the_id(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = None
        assert await store.find_note_id_owner(uuid.uuid4(), "ghost") is None


class TestDuplicateNoteIdDiagnostic:
    """`_log_duplicate_note_id` — the readable form of the constraint error.

    The raw Postgres error names only the note id, so the live symptom read as
    "the reindexer keeps retrying a write" when the truth is that two files
    claim one identity and the INSERT is impossible. One log line must carry
    the id, BOTH paths, and which one the index holds.
    """

    def _violation(self):
        exc = Exception(
            'duplicate key value violates unique constraint "uq_knowledge_project_note"'
        )
        exc.constraint_name = "uq_knowledge_project_note"
        return exc

    @pytest.mark.asyncio
    async def test_names_both_paths_and_the_winner(self, caplog):
        from orchestrator.services import kb_reindex

        store = AsyncMock()
        store.find_note_id_owner.return_value = {
            "id": uuid.uuid4(),
            "path": "knowledge/iter-33-plan.md/dup-note.md",
            "status": "active",
            "indexed_at": None,
        }
        kb = uuid.uuid4()

        with caplog.at_level(logging.ERROR):
            handled = await kb_reindex._log_duplicate_note_id(
                store, kb, "knowledge/dup-note.md", "dup-note", self._violation()
            )

        assert handled is True
        assert len(caplog.records) == 1
        line = caplog.records[0].getMessage()
        assert "dup-note" in line
        assert "knowledge/dup-note.md" in line  # the path being indexed
        assert "knowledge/iter-33-plan.md/dup-note.md" in line  # the incumbent
        assert "two paths" in line
        store.find_note_id_owner.assert_awaited_once_with(kb, "dup-note")

    @pytest.mark.asyncio
    async def test_reports_a_pathless_legacy_incumbent(self, caplog):
        from orchestrator.services import kb_reindex

        store = AsyncMock()
        store.find_note_id_owner.return_value = {
            "id": uuid.uuid4(),
            "path": None,
            "status": "active",
            "indexed_at": None,
        }

        with caplog.at_level(logging.ERROR):
            handled = await kb_reindex._log_duplicate_note_id(
                store, uuid.uuid4(), "knowledge/n.md", "n", self._violation()
            )

        assert handled is True
        assert "pathless" in caplog.records[0].getMessage()

    @pytest.mark.asyncio
    async def test_ignores_unrelated_errors(self):
        from orchestrator.services import kb_reindex

        store = AsyncMock()
        handled = await kb_reindex._log_duplicate_note_id(
            store, uuid.uuid4(), "knowledge/n.md", "n", Exception("connection reset")
        )
        # False => the caller keeps its generic error line; no DB round-trip
        # on every unrelated failure.
        assert handled is False
        store.find_note_id_owner.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_detects_via_message_when_driver_attr_is_absent(self, caplog):
        # A wrapping layer may re-raise with only the rendered message.
        from orchestrator.services import kb_reindex

        store = AsyncMock()
        store.find_note_id_owner.return_value = {"path": "knowledge/a/n.md"}
        with caplog.at_level(logging.ERROR):
            handled = await kb_reindex._log_duplicate_note_id(
                store,
                uuid.uuid4(),
                "knowledge/n.md",
                "n",
                RuntimeError('violates unique constraint "uq_knowledge_project_note"'),
            )
        assert handled is True

    @pytest.mark.asyncio
    async def test_lookup_failure_still_logs(self, caplog):
        # The diagnostic runs inside an except block: failing to explain an
        # error must never replace it with a new one.
        from orchestrator.services import kb_reindex

        store = AsyncMock()
        store.find_note_id_owner.side_effect = Exception("vector db down")

        with caplog.at_level(logging.ERROR):
            handled = await kb_reindex._log_duplicate_note_id(
                store, uuid.uuid4(), "knowledge/n.md", "n", self._violation()
            )

        assert handled is True
        assert "n" in caplog.records[-1].getMessage()
