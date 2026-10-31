"""Tests for src/services/knowledge_store.py.

Covers section 11 of persistent_agent_tests.md:
  11.1  KnowledgeRecord dataclass
  11.2  KnowledgeStore._content_hash()
  11.3  KnowledgeStore._prepare_embedding()
  11.4  upsert_note() — metadata-only update
  11.5  upsert_note() — content changed
  11.6  delete_note()
  11.7  hybrid_search()
  11.8  get_summary()
  11.9  rebuild_from_notes()
  11.10 format_note() (static)
  11.11 assemble_knowledge_block() (classmethod)
"""

import hashlib
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.runtime.services.knowledge_store import KnowledgeRecord, KnowledgeStore


# =============================================================================
# Helpers
# =============================================================================


def _make_store():
    """Create a KnowledgeStore with mocked db and embedding_service."""
    mock_db = AsyncMock()
    mock_embed = AsyncMock()
    mock_embed.embed = AsyncMock(return_value=[0.1, 0.2, 0.3])
    store = KnowledgeStore(db=mock_db, embedding_service=mock_embed)
    return store, mock_db, mock_embed


def _make_record(**overrides):
    """Create a KnowledgeRecord with sensible defaults."""
    defaults = {
        "id": uuid.uuid4(),
        "note_id": "test-note",
        "project_id": uuid.uuid4(),
        "title": "Test Note",
        "note_type": "decision",
        "status": "active",
        "content": "Test content",
    }
    defaults.update(overrides)
    return KnowledgeRecord(**defaults)


# =============================================================================
# 11.1: KnowledgeRecord dataclass
# =============================================================================


class TestKnowledgeRecord:
    """Tests for KnowledgeRecord dataclass."""

    def test_can_construct_empty(self):
        r = KnowledgeRecord()
        assert r.note_id == ""
        assert r.title == ""

    def test_status_defaults_to_active(self):
        r = KnowledgeRecord()
        assert r.status == "active"

    def test_tags_defaults_to_empty_list(self):
        r = KnowledgeRecord()
        assert r.tags == []

    def test_keywords_defaults_to_empty_list(self):
        r = KnowledgeRecord()
        assert r.keywords == []

    def test_retrieval_messages_defaults_to_empty_list(self):
        r = KnowledgeRecord()
        assert r.retrieval_messages == []

    def test_from_row_creates_record(self):
        row = {
            "id": uuid.uuid4(),
            "note_id": "my-note",
            "title": "My Note",
            "note_type": "decision",
            "status": "active",
            "content": "body text",
        }
        r = KnowledgeRecord.from_row(row)
        assert r.note_id == "my-note"
        assert r.title == "My Note"

    def test_from_row_handles_missing_keys(self):
        r = KnowledgeRecord.from_row({})
        assert r.note_id == ""
        assert r.status == "active"
        assert r.tags == []

    def test_from_row_none_tags_become_empty_list(self):
        r = KnowledgeRecord.from_row({"tags": None})
        assert r.tags == []

    def test_from_row_none_keywords_become_empty_list(self):
        r = KnowledgeRecord.from_row({"keywords": None})
        assert r.keywords == []

    def test_from_row_none_retrieval_messages_become_empty_list(self):
        r = KnowledgeRecord.from_row({"retrieval_messages": None})
        assert r.retrieval_messages == []


# =============================================================================
# 11.2: KnowledgeStore._content_hash()
# =============================================================================


class TestContentHash:
    """Tests for _content_hash static method."""

    def test_returns_sha256_hex(self):
        expected = hashlib.sha256("test".encode()).hexdigest()
        assert KnowledgeStore._content_hash("test") == expected

    def test_is_static_method(self):
        # Can be called without instance
        result = KnowledgeStore._content_hash("hello")
        assert isinstance(result, str)
        assert len(result) == 64

    def test_consistent_hashing(self):
        h1 = KnowledgeStore._content_hash("same")
        h2 = KnowledgeStore._content_hash("same")
        assert h1 == h2


# =============================================================================
# 11.3: KnowledgeStore._prepare_embedding()
# =============================================================================


class TestPrepareEmbedding:
    """Tests for _prepare_embedding static method."""

    def test_list_unchanged(self):
        data = [0.1, 0.2, 0.3]
        assert KnowledgeStore._prepare_embedding(data) is data

    def test_parses_string_format(self):
        result = KnowledgeStore._prepare_embedding("[0.1,0.2,0.3]")
        assert result == [0.1, 0.2, 0.3]

    def test_empty_string_brackets(self):
        result = KnowledgeStore._prepare_embedding("[]")
        assert result == []

    def test_other_iterable_fallback(self):
        result = KnowledgeStore._prepare_embedding(tuple([1.0, 2.0]))
        assert result == [1.0, 2.0]


# =============================================================================
# 11.4: upsert_note() — metadata-only update
# =============================================================================


class TestUpsertMetadataOnly:
    """Tests for upsert_note when content hash matches (metadata-only)."""

    @pytest.mark.asyncio
    async def test_skips_embedding_when_hash_matches(self):
        store, mock_db, mock_embed = _make_store()
        content = "existing content"
        content_hash = KnowledgeStore._content_hash(content)
        row_id = uuid.uuid4()

        mock_db.fetchval.side_effect = [content_hash, row_id]

        result = await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content=content,
        )
        assert result == row_id
        mock_embed.embed.assert_not_called()

    @pytest.mark.asyncio
    async def test_updates_metadata_fields(self):
        store, mock_db, mock_embed = _make_store()
        content = "body"
        content_hash = KnowledgeStore._content_hash(content)
        row_id = uuid.uuid4()

        mock_db.fetchval.side_effect = [content_hash, row_id]

        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="Updated Title",
            note_type="learning",
            content=content,
            status="resolved",
            confidence="high",
            tags=["a"],
            keywords=["b"],
            retrieval_messages=["q"],
        )

        # Second fetchval call is the UPDATE
        update_call = mock_db.fetchval.call_args_list[1]
        query = update_call[0][0]
        assert "UPDATE knowledge_index" in query
        assert "RETURNING id" in query

    @pytest.mark.asyncio
    async def test_returns_row_uuid(self):
        store, mock_db, _ = _make_store()
        content = "body"
        row_id = uuid.uuid4()
        mock_db.fetchval.side_effect = [KnowledgeStore._content_hash(content), row_id]

        result = await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content=content,
        )
        assert result == row_id


# =============================================================================
# 11.5: upsert_note() — content changed
# =============================================================================


class TestUpsertContentChanged:
    """Tests for upsert_note when content hash differs."""

    @pytest.mark.asyncio
    async def test_generates_embedding_when_hash_differs(self):
        store, mock_db, mock_embed = _make_store()
        mock_db.fetchval.side_effect = ["old-hash", uuid.uuid4()]

        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content="new content",
        )
        mock_embed.embed.assert_called_once()

    @pytest.mark.asyncio
    async def test_generates_embedding_when_no_existing(self):
        store, mock_db, mock_embed = _make_store()
        mock_db.fetchval.side_effect = [None, uuid.uuid4()]

        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content="content",
        )
        mock_embed.embed.assert_called_once()

    @pytest.mark.asyncio
    async def test_embed_text_includes_retrieval_messages(self):
        store, mock_db, mock_embed = _make_store()
        mock_db.fetchval.side_effect = [None, uuid.uuid4()]

        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content="body",
            retrieval_messages=["What is X?", "How does Y work?"],
        )
        embed_text = mock_embed.embed.call_args[0][0]
        assert "body" in embed_text
        assert "What is X?" in embed_text
        assert "How does Y work?" in embed_text

    @pytest.mark.asyncio
    async def test_embed_text_without_retrieval_messages(self):
        store, mock_db, mock_embed = _make_store()
        mock_db.fetchval.side_effect = [None, uuid.uuid4()]

        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content="just content",
        )
        embed_text = mock_embed.embed.call_args[0][0]
        assert embed_text == "just content"

    @pytest.mark.asyncio
    async def test_uses_on_conflict_upsert(self):
        store, mock_db, mock_embed = _make_store()
        mock_db.fetchval.side_effect = [None, uuid.uuid4()]

        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content="body",
        )
        # Second fetchval is the INSERT ... ON CONFLICT
        query = mock_db.fetchval.call_args_list[1][0][0]
        assert "ON CONFLICT (project_id, note_id) DO UPDATE" in query

    @pytest.mark.asyncio
    async def test_returns_row_uuid(self):
        store, mock_db, _ = _make_store()
        row_id = uuid.uuid4()
        mock_db.fetchval.side_effect = [None, row_id]

        result = await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content="body",
        )
        assert result == row_id

    @pytest.mark.asyncio
    async def test_none_tags_normalized_to_empty_list(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.side_effect = [None, uuid.uuid4()]

        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="decision",
            content="body",
            tags=None,
            keywords=None,
            retrieval_messages=None,
        )
        # Should not raise — None normalized to []
        mock_db.fetchval.assert_called()


# =============================================================================
# upsert_note priority sentinel (project-backlog-pipeline task 3, fix round 1,
# Finding 1): priority=None means "unknown, leave it as-is" -- both branches
# must COALESCE against the row's existing value, not silently default to 1
# and clobber a real priority. Mutation-tested: reverting either COALESCE to
# a bare `priority`/`EXCLUDED.priority` reference fails these.
# =============================================================================


class TestUpsertNotePriorityCoalesceSentinel:
    @pytest.mark.asyncio
    async def test_metadata_only_branch_coalesces_none_against_existing_row(self):
        store, mock_db, _ = _make_store()
        existing_hash = KnowledgeStore._content_hash("body")
        mock_db.fetchval.side_effect = [existing_hash, uuid.uuid4()]
        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="feature",
            content="body",
            priority=None,
        )
        update_call = mock_db.fetchval.call_args_list[1]
        query = update_call[0][0]
        assert "priority = COALESCE($13, priority)" in query
        # This layer must never turn None into a concrete int itself -- that
        # decision belongs entirely to the SQL COALESCE against the live row.
        assert update_call[0][-1] is None

    @pytest.mark.asyncio
    async def test_insert_branch_coalesces_none_to_default_for_a_fresh_row(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.side_effect = [None, uuid.uuid4()]  # no existing -> INSERT
        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="feature",
            content="body",
            priority=None,
        )
        insert_call = mock_db.fetchval.call_args_list[1]
        assert "COALESCE($19, 1)" in insert_call[0][0]
        # $19 is still bound as None -- the VALUES-list COALESCE (not Python)
        # is what turns it into 1 for the genuinely-new row.
        assert insert_call[0][-1] is None

    @pytest.mark.asyncio
    async def test_insert_branch_on_conflict_preserves_existing_not_excluded(self):
        """The ON CONFLICT branch must reference the raw bound parameter
        ($19), never EXCLUDED.priority -- EXCLUDED.priority is already the
        VALUES-list COALESCE(_, 1) result and would never be NULL, so
        referencing it here would silently reintroduce the clobber bug on
        every conflicting upsert with priority=None."""
        store, mock_db, _ = _make_store()
        mock_db.fetchval.side_effect = [None, uuid.uuid4()]
        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="feature",
            content="body",
            priority=None,
        )
        query = mock_db.fetchval.call_args_list[1][0][0]
        conflict_clause = query.split("ON CONFLICT")[1]
        assert "priority = COALESCE($19, knowledge_index.priority)" in conflict_clause
        assert "EXCLUDED.priority" not in conflict_clause

    @pytest.mark.asyncio
    async def test_explicit_priority_still_wins_on_metadata_only_branch(self):
        """Regression guard: the sentinel must not interfere with a real,
        caller-supplied value (Task 3's original contract, fix round 0)."""
        store, mock_db, _ = _make_store()
        existing_hash = KnowledgeStore._content_hash("body")
        mock_db.fetchval.side_effect = [existing_hash, uuid.uuid4()]
        await store.upsert_note(
            note_id="n1",
            project_id=uuid.uuid4(),
            title="T",
            note_type="feature",
            content="body",
            priority=0,
        )
        update_call = mock_db.fetchval.call_args_list[1]
        # Positional, not [-1]: args[0] is the query, so $13 (priority) is
        # index 13. B2 appended $14 (ready) after it, and a trailing-slot pin
        # would silently start asserting against whatever lands last next.
        assert update_call[0][13] == 0
        assert update_call[0][14] is None  # this write said nothing about ready


# =============================================================================
# upsert_kb_note priority sentinel (project-backlog-pipeline task 3, fix
# round 2, Finding 3): priority=None means "the file has no priority: line,
# leave the stored rank as-is" -- the reindex-path counterpart to
# TestUpsertNotePriorityCoalesceSentinel above (the agent-write path).
# Mutation-tested: reverting either COALESCE to a bare `$21`/
# `EXCLUDED.priority` reference fails these.
# =============================================================================


class TestUpsertKbNotePriorityCoalesceSentinel:
    @pytest.mark.asyncio
    async def test_coalesces_none_against_existing_row_on_conflict(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="feature",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            priority=None,
        )
        query, *params = mock_db.fetchval.call_args[0]
        assert "priority = COALESCE($21, knowledge_index.priority)" in query
        # This layer must never turn None into a concrete int itself -- that
        # decision belongs entirely to the SQL COALESCE against the live row.
        assert params[-1] is None

    @pytest.mark.asyncio
    async def test_coalesces_none_to_default_for_a_fresh_row(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="feature",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            priority=None,
        )
        query = mock_db.fetchval.call_args[0][0]
        assert "COALESCE($21, 1)" in query

    @pytest.mark.asyncio
    async def test_on_conflict_preserves_existing_not_excluded(self):
        """Never EXCLUDED.priority here -- it's already the VALUES-list's
        COALESCE(_, 1) result and would never be NULL, so referencing it
        would silently reintroduce the clobber bug on every conflicting
        upsert with priority=None (i.e. every reindex of an existing note
        whose file has no priority: line)."""
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="feature",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            priority=None,
        )
        query = mock_db.fetchval.call_args[0][0]
        conflict_clause = query.split("ON CONFLICT")[1]
        assert "priority = COALESCE($21, knowledge_index.priority)" in conflict_clause
        assert "EXCLUDED.priority" not in conflict_clause

    @pytest.mark.asyncio
    async def test_explicit_priority_still_wins(self):
        """Regression guard: the sentinel must not interfere with a real,
        caller-supplied value (Task 2's original contract)."""
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        await store.upsert_kb_note(
            kb_id=uuid.uuid4(),
            note_id="n",
            path="knowledge/n.md",
            title="T",
            note_type="feature",
            content="body",
            blob_sha="b",
            embedding_version="v1",
            priority=0,
        )
        params = mock_db.fetchval.call_args[0][1:]
        # $21 (priority) by position, not [-1]: B2 appended $22 (ready_at)
        # after it, and a trailing-slot pin would follow whatever lands last.
        assert params[20] == 0
        assert params[21] is None  # no ready_at: line in this note's frontmatter


# =============================================================================
# 11.6: delete_note()
# =============================================================================


class TestDeleteNote:
    """Tests for delete_note() method."""

    @pytest.mark.asyncio
    async def test_returns_true_when_deleted(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        assert await store.delete_note(uuid.uuid4(), "n1") is True

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = None
        assert await store.delete_note(uuid.uuid4(), "n1") is False


# =============================================================================
# 11.7: hybrid_search()
# =============================================================================


class TestHybridSearch:
    """Tests for hybrid_search() method."""

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_project(self):
        store, mock_db, _ = _make_store()
        result = await store.hybrid_search(query="test")
        assert result == []
        mock_db.fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_single_project_uses_single_function(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []

        await store.hybrid_search(project_id=uuid.uuid4(), query="test")
        query = mock_db.fetch.call_args[0][0]
        assert "knowledge_hybrid_search" in query

    @pytest.mark.asyncio
    async def test_multi_project_uses_multi_function(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []

        await store.hybrid_search(
            project_ids=[uuid.uuid4(), uuid.uuid4()], query="test"
        )
        query = mock_db.fetch.call_args[0][0]
        assert "knowledge_multi_project_hybrid_search" in query

    @pytest.mark.asyncio
    async def test_project_ids_takes_priority(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []

        pid = uuid.uuid4()
        pids = [uuid.uuid4(), uuid.uuid4()]
        await store.hybrid_search(project_id=pid, project_ids=pids, query="test")
        query = mock_db.fetch.call_args[0][0]
        assert "multi_project" in query

    @pytest.mark.asyncio
    async def test_calls_embed_for_query(self):
        store, mock_db, mock_embed = _make_store()
        mock_db.fetch.return_value = []

        await store.hybrid_search(project_id=uuid.uuid4(), query="search term")
        mock_embed.embed.assert_called_once_with("search term")

    @pytest.mark.asyncio
    async def test_returns_knowledge_records(self):
        store, mock_db, _ = _make_store()
        mock_row = MagicMock()
        mock_row.__iter__ = MagicMock(return_value=iter([]))
        # dict(row) must work
        mock_row_dict = {
            "note_id": "n1",
            "title": "A",
            "note_type": "decision",
            "status": "active",
            "content": "body",
        }
        mock_db.fetch.return_value = [mock_row_dict]

        result = await store.hybrid_search(project_id=uuid.uuid4(), query="q")
        assert len(result) == 1
        assert isinstance(result[0], KnowledgeRecord)
        assert result[0].note_id == "n1"

    @pytest.mark.asyncio
    async def test_default_weights(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []

        await store.hybrid_search(project_id=uuid.uuid4(), query="q")
        call_args = mock_db.fetch.call_args[0]
        # Args: sql, query, embedding, scope, match_count, dense, sparse, recency
        assert call_args[5] == 0.6  # dense
        assert call_args[6] == 0.3  # sparse
        assert call_args[7] == 0.1  # recency


# =============================================================================
# find_similar_many() — neighbour fetch for the ingestion verdict (slice 2 PR2)
# =============================================================================


class TestFindSimilarMany:
    """Tests for find_similar_many() — the KB analog of RecallStore's version."""

    @pytest.mark.asyncio
    async def test_returns_records_with_similarity(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [
            {"note_id": "n1", "title": "A", "content": "body", "similarity": 0.91},
        ]

        result = await store.find_similar_many(
            project_id=uuid.uuid4(), embedding=[0.1, 0.2, 0.3]
        )
        assert len(result) == 1
        assert isinstance(result[0], KnowledgeRecord)
        assert result[0].note_id == "n1"
        assert result[0].similarity == 0.91

    @pytest.mark.asyncio
    async def test_query_scopes_to_project_active_and_floor(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        pid = uuid.uuid4()

        await store.find_similar_many(
            project_id=pid, embedding=[0.1, 0.2, 0.3], k=7, min_similarity=0.75
        )
        sql = mock_db.fetch.call_args[0][0]
        args = mock_db.fetch.call_args[0][1:]
        assert "project_id" in sql
        assert "status = 'active'" in sql
        assert "embedding <=>" in sql
        # args: embedding, project_id, min_similarity, k
        assert args[1] == pid
        assert args[2] == 0.75
        assert args[3] == 7

    @pytest.mark.asyncio
    async def test_empty_when_no_neighbours(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []

        result = await store.find_similar_many(
            project_id=uuid.uuid4(), embedding=[0.1, 0.2, 0.3]
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_accepts_string_embedding(self):
        # Legacy string embeddings must be normalized (mirrors _prepare_embedding).
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []

        await store.find_similar_many(
            project_id=uuid.uuid4(), embedding="[0.1,0.2,0.3]"
        )
        passed = mock_db.fetch.call_args[0][1]
        assert passed == [0.1, 0.2, 0.3]


# =============================================================================
# 11.8: get_summary()
# =============================================================================


class TestGetSummary:
    """Tests for get_summary() method."""

    @pytest.mark.asyncio
    async def test_returns_zero_total_when_no_project(self):
        store, mock_db, _ = _make_store()
        result = await store.get_summary()
        assert result == {"total": 0}

    @pytest.mark.asyncio
    async def test_single_project_uses_equals_clause(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = {"total": 5}
        mock_db.fetch.return_value = []

        await store.get_summary(project_id=uuid.uuid4())
        query = mock_db.fetchrow.call_args[0][0]
        assert "project_id = $1" in query

    @pytest.mark.asyncio
    async def test_multi_project_uses_any_clause(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = {"total": 10}
        mock_db.fetch.return_value = []

        await store.get_summary(project_ids=[uuid.uuid4(), uuid.uuid4()])
        query = mock_db.fetchrow.call_args[0][0]
        assert "project_id = ANY($1)" in query

    @pytest.mark.asyncio
    async def test_returns_expected_count_fields(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = {
            "total": 10,
            "active": 8,
            "decisions": 3,
            "learnings": 2,
            "open_questions": 1,
            "goals": 1,
            "code_notes": 2,
            "state_notes": 1,
            "last_modified": None,
        }
        mock_db.fetch.return_value = []

        result = await store.get_summary(project_id=uuid.uuid4())
        assert result["total"] == 10
        assert result["active"] == 8
        assert result["decisions"] == 3

    @pytest.mark.asyncio
    async def test_includes_recent_notes(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = {"total": 2}
        mock_db.fetch.return_value = [
            {
                "note_id": "n1",
                "title": "A",
                "note_type": "decision",
                "status": "active",
                "modified_at": None,
            },
        ]

        result = await store.get_summary(project_id=uuid.uuid4())
        assert "recent_notes" in result
        assert len(result["recent_notes"]) == 1


# =============================================================================
# 11.9: rebuild_from_notes()
# =============================================================================


class TestRebuildFromNotes:
    """Tests for rebuild_from_notes() method."""

    @pytest.mark.asyncio
    async def test_deletes_existing_entries_first(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        pid = uuid.uuid4()

        await store.rebuild_from_notes(pid, [])
        mock_db.execute.assert_called_once()
        query = mock_db.execute.call_args[0][0]
        assert "DELETE FROM knowledge_index" in query

    @pytest.mark.asyncio
    async def test_upserts_each_note(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        pid = uuid.uuid4()

        notes = [
            {"id": "n1", "title": "A", "type": "decision", "content": "body1"},
            {"id": "n2", "title": "B", "type": "learning", "content": "body2"},
        ]
        count = await store.rebuild_from_notes(pid, notes)
        assert count == 2

    @pytest.mark.asyncio
    async def test_handles_neo4j_datetime(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()

        mock_dt = MagicMock()
        mock_dt.to_native.return_value = "2026-01-01T00:00:00"

        notes = [
            {
                "id": "n1",
                "type": "decision",
                "content": "body",
                "created": mock_dt,
                "modified": mock_dt,
            }
        ]
        count = await store.rebuild_from_notes(uuid.uuid4(), notes)
        assert count == 1
        mock_dt.to_native.assert_called()

    @pytest.mark.asyncio
    async def test_converts_string_job_id(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchval.return_value = uuid.uuid4()
        job_uuid = str(uuid.uuid4())

        notes = [{"id": "n1", "type": "decision", "content": "x", "job_id": job_uuid}]
        count = await store.rebuild_from_notes(uuid.uuid4(), notes)
        assert count == 1

    @pytest.mark.asyncio
    async def test_individual_failure_is_non_fatal(self):
        store, mock_db, _ = _make_store()
        # First note succeeds, second fails, third succeeds
        mock_db.fetchval.side_effect = [
            uuid.uuid4(),  # hash check for n1 (content changed)
            uuid.uuid4(),  # upsert result for n1
            Exception("bad data"),  # n2 fails
            uuid.uuid4(),  # hash check for n3
            uuid.uuid4(),  # upsert result for n3
        ]

        notes = [
            {"id": "n1", "type": "decision", "content": "ok1"},
            {"id": "n2", "type": "decision", "content": "bad"},
            {"id": "n3", "type": "decision", "content": "ok3"},
        ]
        count = await store.rebuild_from_notes(uuid.uuid4(), notes)
        assert count == 2  # n1 + n3

    @pytest.mark.asyncio
    async def test_returns_zero_for_empty_notes(self):
        store, mock_db, _ = _make_store()
        count = await store.rebuild_from_notes(uuid.uuid4(), [])
        assert count == 0


# =============================================================================
# 11.10: format_note() (static)
# =============================================================================


class TestFormatNote:
    """Tests for format_note() static method."""

    def test_includes_note_type(self):
        note = _make_record(note_type="decision")
        result = KnowledgeStore.format_note(note, 1)
        assert "decision" in result

    def test_includes_confidence_when_present(self):
        note = _make_record(confidence="high")
        result = KnowledgeStore.format_note(note, 1)
        assert "high confidence" in result

    def test_excludes_confidence_when_absent(self):
        note = _make_record(confidence=None)
        result = KnowledgeStore.format_note(note, 1)
        assert "confidence" not in result

    def test_includes_phase_when_not_none(self):
        note = _make_record(phase=3)
        result = KnowledgeStore.format_note(note, 1)
        assert "phase 3" in result

    def test_excludes_phase_when_none(self):
        note = _make_record(phase=None)
        result = KnowledgeStore.format_note(note, 1)
        assert "phase" not in result

    def test_includes_tags_when_present(self):
        note = _make_record(tags=["auth", "security"])
        result = KnowledgeStore.format_note(note, 1)
        assert "Tags: auth, security" in result

    def test_excludes_tags_when_empty(self):
        note = _make_record(tags=[])
        result = KnowledgeStore.format_note(note, 1)
        assert "Tags:" not in result

    def test_truncates_long_content(self):
        note = _make_record(content="x" * 600)
        result = KnowledgeStore.format_note(note, 1)
        assert result.endswith("...")
        # Content part should be 497 + 3 = 500 chars
        content_line = result.split("\n", 1)[1]
        assert len(content_line) == 500

    def test_does_not_truncate_short_content(self):
        note = _make_record(content="short")
        result = KnowledgeStore.format_note(note, 1)
        assert "..." not in result

    def test_uses_1_based_index(self):
        note = _make_record()
        result = KnowledgeStore.format_note(note, 5)
        assert result.startswith("[5]")


# =============================================================================
# 11.11: assemble_knowledge_block() (classmethod)
# =============================================================================


class TestAssembleKnowledgeBlock:
    """Tests for assemble_knowledge_block() classmethod."""

    def test_returns_empty_string_for_empty_list(self):
        assert KnowledgeStore.assemble_knowledge_block([]) == ""

    def test_wraps_with_header_and_footer(self):
        notes = [_make_record(content="body")]
        result = KnowledgeStore.assemble_knowledge_block(notes)
        assert result.startswith("--- Project Knowledge ---")
        assert "--- End Knowledge" in result

    def test_footer_includes_note_count(self):
        notes = [_make_record(content="a"), _make_record(content="b")]
        result = KnowledgeStore.assemble_knowledge_block(notes)
        assert "2 notes" in result

    def test_footer_includes_token_estimate(self):
        notes = [_make_record(content="a" * 400)]
        result = KnowledgeStore.assemble_knowledge_block(notes)
        # 400 // 4 = 100 tokens
        assert "100 tokens" in result

    def test_each_note_formatted_with_1_based_index(self):
        notes = [_make_record(content="first"), _make_record(content="second")]
        result = KnowledgeStore.assemble_knowledge_block(notes)
        assert "[1]" in result
        assert "[2]" in result


# =============================================================================
# find_near_duplicate_pairs() — the kb_lint near-duplicate fetch
# =============================================================================


class TestFindNearDuplicatePairs:
    @pytest.mark.asyncio
    async def test_returns_pair_tuples_from_rows(self):
        store, mock_db, _ = _make_store()
        pid = uuid.uuid4()
        mock_db.fetch.return_value = [
            {"note_a": "n1", "note_b": "n2", "similarity": 0.94},
            {"note_a": "n3", "note_b": "n4", "similarity": 0.91},
        ]
        pairs = await store.find_near_duplicate_pairs(pid)
        assert pairs == [("n1", "n2", 0.94), ("n3", "n4", 0.91)]

    @pytest.mark.asyncio
    async def test_query_is_active_only_self_join_with_knobs(self):
        store, mock_db, _ = _make_store()
        pid = uuid.uuid4()
        mock_db.fetch.return_value = []
        await store.find_near_duplicate_pairs(pid, min_similarity=0.88, limit=25)
        args = mock_db.fetch.call_args[0]
        sql = args[0]
        # Active-only on both sides, each pair once, embeddings required.
        assert sql.count("status = 'active'") == 2
        assert "a.note_id < b.note_id" in sql
        assert "embedding IS NOT NULL" in sql
        assert pid in args
        assert 0.88 in args
        assert 25 in args

    @pytest.mark.asyncio
    async def test_empty_result(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        assert await store.find_near_duplicate_pairs(uuid.uuid4()) == []

    @pytest.mark.asyncio
    async def test_self_join_guards_embedding_version(self):
        # D-2: cosine-comparing vectors from different embedding models is
        # meaningless (ghost null-version vs qwen3 c1). The self-join must only
        # pair rows that share an embedding_version.
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        await store.find_near_duplicate_pairs(uuid.uuid4())
        sql = mock_db.fetch.call_args[0][0]
        assert "a.embedding_version = b.embedding_version" in sql


# =============================================================================
# get_note_by_slug() — the kg-less kb_read backend
# =============================================================================


class TestGetNoteBySlug:
    @pytest.mark.asyncio
    async def test_excludes_pathless_ghost_rows(self):
        # B-1 (symmetry with list_notes): a direct read must not surface a
        # pathless ghost row either — files-canonical means a file must back it.
        # Status stays unfiltered (superseded/archived files still read).
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = None
        await store.get_note_by_slug(uuid.uuid4(), "some-slug")
        sql = mock_db.fetchrow.call_args[0][0]
        assert "path IS NOT NULL" in sql

    @pytest.mark.asyncio
    async def test_maps_row_to_note_dict(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = {
            "note_id": "n1",
            "title": "N1",
            "note_type": "decision",
            "status": "superseded",
            "content": "body",
            "confidence": "high",
            "tags": ["a"],
            "keywords": ["k"],
            "job_id": None,
            "phase": None,
            "created_at": None,
            "modified_at": None,
        }
        out = await store.get_note_by_slug(uuid.uuid4(), "n1")
        assert out["id"] == "n1"
        assert out["type"] == "decision"
        assert out["status"] == "superseded"
        assert out["tags"] == ["a"]

    @pytest.mark.asyncio
    async def test_returns_none_when_absent(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = None
        assert await store.get_note_by_slug(uuid.uuid4(), "nope") is None

    @pytest.mark.asyncio
    async def test_exposes_the_blob_sha_as_the_cas_token(self):
        # kb_gardening G3: kb_update/kb_delete send this back as
        # expected_blob_sha so a concurrent rewrite fails loudly.
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = {
            "note_id": "n1",
            "title": "N1",
            "note_type": "state",
            "status": "active",
            "content": "body",
            "blob_sha": "a" * 40,
        }
        out = await store.get_note_by_slug(uuid.uuid4(), "n1")
        assert out["blob_sha"] == "a" * 40
        assert "blob_sha" in mock_db.fetchrow.call_args[0][0]


# =============================================================================
# get_inbound_links() — the retirement guard's evidence (kb_gardening G5)
# =============================================================================


class TestGardeningHealth:
    @pytest.mark.asyncio
    async def test_counters_are_scoped_and_typed(self):
        from datetime import timedelta

        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = {
            "active": 120,
            "retired": 40,
            "retired_past_grace": 12,
            "nursery_active": 80,
            "nursery_orphans": 30,
            "oldest_retired_days": 41.7,
        }
        kb = uuid.uuid4()
        out = await store.get_gardening_health(kb, grace=timedelta(days=7))
        sql, *params = mock_db.fetchrow.call_args[0]
        flat = " ".join(sql.split())
        assert "WHERE kb_id = $1 AND path IS NOT NULL" in flat
        assert "status IN ('archived', 'superseded')" in flat
        assert "note_type IN ('learning', 'retrospective', 'state')" in flat
        assert params == [kb, timedelta(days=7)]
        assert out == {
            "active": 120,
            "retired": 40,
            "retired_past_grace": 12,
            "nursery_active": 80,
            "nursery_orphans": 30,
            "oldest_retired_days": 41.7,
            "grace_days": 7,
        }

    @pytest.mark.asyncio
    async def test_empty_kb_is_all_zero(self):
        store, mock_db, _ = _make_store()
        mock_db.fetchrow.return_value = None
        out = await store.get_gardening_health(uuid.uuid4())
        assert out["active"] == 0 and out["retired"] == 0
        assert out["oldest_retired_days"] is None
        assert out["grace_days"] == 14


class TestListUnreachableNursery:
    """kb_gardening G7 R4 — GC mark-and-sweep as one recursive query."""

    @pytest.mark.asyncio
    async def test_query_marks_from_active_roots_through_active_links(self):
        from datetime import timedelta

        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [
            {"note_id": "orphan-retro", "note_type": "retrospective"}
        ]
        kb = uuid.uuid4()
        out = await store.list_unreachable_nursery(
            kb,
            root_types=["decision", "goal"],
            nursery_types=["learning", "retrospective", "state"],
            protected_tags=["pinned", "ready"],
            min_age=timedelta(days=7),
            limit=25,
        )
        sql, *params = mock_db.fetch.call_args[0]
        flat = " ".join(sql.split())
        assert "WITH RECURSIVE roots AS" in flat
        assert "note_type = ANY($2::text[])" in flat  # roots
        assert "JOIN reach r ON r.note_id = kl.source_id" in flat
        assert "src.status = 'active'" in flat  # marking follows active sources only
        assert "ki.note_type = ANY($3::text[])" in flat  # nursery
        assert "&& $4::text[]" in flat  # protected tags
        assert "ki.ready_at IS NULL" in flat
        assert "< now() - $5::interval" in flat
        assert "NOT EXISTS (SELECT 1 FROM reach r WHERE r.note_id = ki.note_id)" in flat
        assert "LIMIT $6" in flat
        assert params == [
            kb,
            ["decision", "goal"],
            ["learning", "retrospective", "state"],
            ["pinned", "ready"],
            timedelta(days=7),
            25,
        ]
        assert out[0]["note_id"] == "orphan-retro"


class TestSetNoteStatus:
    @pytest.mark.asyncio
    async def test_flips_status_and_stamps_invalidated_at(self):
        store, mock_db, _ = _make_store()
        mock_db.execute.return_value = "UPDATE 1"
        kb = uuid.uuid4()
        assert (
            await store.set_note_status(kb, "n1", "archived", invalidated=True) is True
        )
        sql, *params = mock_db.execute.call_args[0]
        flat = " ".join(sql.split())
        assert "SET status = $3" in flat
        assert (
            "invalidated_at = CASE WHEN $4 THEN now() ELSE invalidated_at END" in flat
        )
        assert "AND status <> $3" in flat
        assert params == [kb, "n1", "archived", True]

    @pytest.mark.asyncio
    async def test_no_change_reports_false(self):
        store, mock_db, _ = _make_store()
        mock_db.execute.return_value = "UPDATE 0"
        assert await store.set_note_status(uuid.uuid4(), "n1", "archived") is False


class TestListPurgeCandidates:
    """kb_gardening G2: the three-signal purge rule lives in one query."""

    @pytest.mark.asyncio
    async def test_query_encodes_the_three_signals_and_the_cap(self):
        from datetime import timedelta

        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [
            {
                "note_id": "old",
                "path": "knowledge/old.md",
                "blob_sha": "a" * 40,
                "status": "archived",
                "note_type": "learning",
                "retired_at": None,
            }
        ]
        kb = uuid.uuid4()
        out = await store.list_purge_candidates(
            kb, grace=timedelta(days=14), limit=7, excluded_types=["charter", "issue"]
        )
        sql, *params = mock_db.fetch.call_args[0]
        flat = " ".join(sql.split())
        assert "ki.status IN ('archived', 'superseded')" in flat
        assert "ki.path IS NOT NULL" in flat
        assert "NOT (ki.note_type = ANY($2::text[]))" in flat
        assert (
            "COALESCE(ki.invalidated_at, ki.modified_at, ki.indexed_at) < now() - $3::interval"
            in flat
        )
        assert "NOT EXISTS" in flat and "src.status = 'active'" in flat
        assert "LIMIT $4" in flat
        assert params == [kb, ["charter", "issue"], timedelta(days=14), 7]
        assert out[0]["note_id"] == "old"


class TestTryConvergeLock:
    @pytest.mark.asyncio
    async def test_claims_a_distinct_key_and_releases_it(self):
        store, mock_db, _ = _make_store()
        conn = AsyncMock()
        conn.fetchval = AsyncMock(side_effect=[True, True])
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=conn)
        cm.__aexit__ = AsyncMock(return_value=False)
        mock_db.acquire = MagicMock(return_value=cm)
        kb = uuid.uuid4()
        async with store.try_converge_lock(kb) as claimed:
            assert claimed is True
        calls = [c.args for c in conn.fetchval.await_args_list]
        assert calls[0][0] == "SELECT pg_try_advisory_lock($1)"
        assert calls[1][0] == "SELECT pg_advisory_unlock($1)"
        key = calls[0][1]
        assert key == calls[1][1]
        # never the reindex key: holding that for a minutes-long LLM pass
        # would defer every inline index in the project
        assert key != store._reindex_lock_key(kb)

    @pytest.mark.asyncio
    async def test_lost_claim_does_not_unlock(self):
        store, mock_db, _ = _make_store()
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=False)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=conn)
        cm.__aexit__ = AsyncMock(return_value=False)
        mock_db.acquire = MagicMock(return_value=cm)
        async with store.try_converge_lock(uuid.uuid4()) as claimed:
            assert claimed is False
        assert conn.fetchval.await_count == 1


class TestGetInboundLinks:
    @pytest.mark.asyncio
    async def test_filters_active_sources_of_the_given_types(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [
            {
                "id": "chose-jwt",
                "title": "Chose JWT",
                "type": "decision",
                "status": "active",
            }
        ]
        kb = uuid.uuid4()
        out = await store.get_inbound_links(
            kb, "old-state", active_only=True, note_types=["decision", "goal"]
        )
        sql, *params = mock_db.fetch.call_args[0]
        assert "kl.target_id = $2" in sql
        assert "ki.note_id = kl.source_id" in sql
        assert "ki.status = 'active'" in sql
        assert "ki.note_type = ANY($3::text[])" in sql
        assert params == [kb, "old-state", ["decision", "goal"]]
        assert out == [
            {
                "id": "chose-jwt",
                "title": "Chose JWT",
                "type": "decision",
                "status": "active",
            }
        ]

    @pytest.mark.asyncio
    async def test_unfiltered_variant_has_no_status_or_type_clause(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        await store.get_inbound_links(uuid.uuid4(), "n", active_only=False)
        sql, *params = mock_db.fetch.call_args[0]
        assert "ki.status = 'active'" not in sql
        assert "ANY(" not in sql
        assert len(params) == 2


# =============================================================================
# list_notes() — the kg-less kb_list backend
# =============================================================================


class TestListNotes:
    @pytest.mark.asyncio
    async def test_excludes_pathless_ghost_rows(self):
        # B-1: files-canonical — the store lists what a file backs. Pathless
        # ghost rows (the DELETE dual-write gap) must never surface in kb_list.
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        await store.list_notes(uuid.uuid4())
        sql = mock_db.fetch.call_args[0][0]
        assert "path IS NOT NULL" in sql

    @pytest.mark.asyncio
    async def test_maps_rows_to_summary_dicts(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [
            {
                "note_id": "n1",
                "title": "N1",
                "note_type": "decision",
                "status": "active",
                "confidence": "high",
            }
        ]
        out = await store.list_notes(uuid.uuid4())
        assert out == [
            {
                "id": "n1",
                "title": "N1",
                "type": "decision",
                "status": "active",
                "confidence": "high",
                "priority": 1,
            }
        ]


# =============================================================================
# list_notes_full() — the kb_lint / kb_index gardener backend
# =============================================================================


class TestListNotesFull:
    @pytest.mark.asyncio
    async def test_does_not_filter_on_path(self):
        # The linter's whole job is to see what the read path cannot: a row no
        # file backs is a defect (invisible to kb_read/kb_search), not an
        # absence. Gating it like list_notes would let a KB whose
        # materialisation is broken lint "clean" while reading empty.
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        await store.list_notes_full(uuid.uuid4())
        sql = mock_db.fetch.call_args[0][0]
        assert "path IS NOT NULL" not in sql

    @pytest.mark.asyncio
    async def test_matches_unadopted_rows_through_project_id(self):
        # upsert_note (the agent write-through) sets project_id but never
        # kb_id, so a kb_id-only filter would miss every just-written note.
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        await store.list_notes_full(uuid.uuid4())
        sql = mock_db.fetch.call_args[0][0]
        assert "kb_id IS NULL AND project_id = $1" in sql

    @pytest.mark.asyncio
    async def test_maps_rows_with_path_and_supersede(self):
        store, mock_db, _ = _make_store()
        job_id = uuid.uuid4()
        mock_db.fetch.return_value = [
            {
                "note_id": "n1",
                "path": "knowledge/n1.md",
                "title": "N1",
                "note_type": "decision",
                "status": "superseded",
                "confidence": "high",
                "priority": 0,
                "tags": ["a"],
                "keywords": None,
                "job_id": job_id,
                "phase": 3,
                "content": "body",
                "superseded_by": "n2",
                "created_at": None,
                "modified_at": None,
            }
        ]
        out = await store.list_notes_full(uuid.uuid4())
        assert out == [
            {
                "id": "n1",
                "path": "knowledge/n1.md",
                "title": "N1",
                "type": "decision",
                "status": "superseded",
                "content": "body",
                "confidence": "high",
                "priority": 0,
                "tags": ["a"],
                "keywords": [],
                "job_id": job_id,
                "phase": 3,
                "superseded_by": "n2",
                "created": None,
                "modified": None,
            }
        ]

    @pytest.mark.asyncio
    async def test_passes_the_scan_cap_as_a_limit(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        await store.list_notes_full(uuid.uuid4(), limit=7)
        assert mock_db.fetch.call_args[0][2] == 7


# =============================================================================
# reconcile_orphans() — R-1 ghost reconciliation
# =============================================================================


class TestReconcileOrphans:
    @pytest.mark.asyncio
    async def test_archives_pathless_orphans_keyed_on_project_id(self):
        # R-1: un-adopted ghost rows carry project_id but kb_id IS NULL, so the
        # reconciliation MUST key on project_id — a kb_id filter matches zero.
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [{"id": uuid.uuid4()}, {"id": uuid.uuid4()}]
        pid = uuid.uuid4()
        n = await store.reconcile_orphans(pid, ["keep-a", "keep-b"])
        assert n == 2
        sql = mock_db.fetch.call_args[0][0]
        assert "project_id = $1" in sql
        assert "kb_id" not in sql  # the landmine: ghosts have kb_id NULL
        assert "path IS NULL" in sql
        assert "status = 'active'" in sql  # only reap active rows
        assert "note_id <> ALL" in sql  # slug absent from the tree
        assert "indexed_at <" in sql  # adoption grace
        assert "status = 'archived'" in sql  # soft-archive, not delete
        assert "invalidated_at = now()" in sql
        args = mock_db.fetch.call_args[0]
        assert pid in args
        assert ["keep-a", "keep-b"] in args

    @pytest.mark.asyncio
    async def test_grace_defaults_to_one_hour(self):
        import datetime

        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        await store.reconcile_orphans(uuid.uuid4(), [])
        assert datetime.timedelta(hours=1) in mock_db.fetch.call_args[0]

    @pytest.mark.asyncio
    async def test_generated_connector_notes_are_never_orphans(self):
        # A ds-* connector note is pathless by design and in no tree; reaping
        # it archived every connector note in a project with a vault.
        from shared.runtime.services.knowledge_store import (
            GENERATED_PATHLESS_NOTE_TYPES,
        )

        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        await store.reconcile_orphans(uuid.uuid4(), [])
        assert "note_type <> ALL($4::text[])" in mock_db.fetch.call_args[0][0]
        assert list(GENERATED_PATHLESS_NOTE_TYPES) in mock_db.fetch.call_args[0]
        assert "datasource" in GENERATED_PATHLESS_NOTE_TYPES


# =============================================================================
# search_chunks() — the slice-3 PR4 retrieval cutover (RRF over knowledge_chunks)
# =============================================================================


class TestSearchChunks:
    """Tests for search_chunks() — hybrid retrieval over the chunk index.

    The chunk-granular successor to hybrid_search: after the PR3 reindexer the
    dense vector lives on ``knowledge_chunks`` (the note row's ``embedding`` is
    NULL for reindexed notes), so retrieval must fuse over chunk rows and
    collapse the best chunk back to its note. Returns note-level
    ``KnowledgeRecord``s so the ``kb_search`` tool signature is unchanged.
    """

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_kb_ids(self):
        store, mock_db, _ = _make_store()
        result = await store.search_chunks(kb_ids=[], query="test")
        assert result == []
        mock_db.fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_calls_embed_for_query(self):
        store, mock_db, mock_embed = _make_store()
        mock_db.fetch.return_value = []
        await store.search_chunks(kb_ids=[uuid.uuid4()], query="search term")
        mock_embed.embed.assert_called_once_with("search term")

    @pytest.mark.asyncio
    async def test_uses_chunk_hybrid_search_function(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        await store.search_chunks(kb_ids=[uuid.uuid4()], query="q")
        sql = mock_db.fetch.call_args[0][0]
        assert "knowledge_chunk_hybrid_search" in sql

    @pytest.mark.asyncio
    async def test_passes_kb_ids_as_array_and_version_filter(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        kb_ids = [uuid.uuid4(), uuid.uuid4()]
        await store.search_chunks(
            kb_ids=kb_ids, query="q", embedding_version="m:4096:c1"
        )
        args = mock_db.fetch.call_args[0]
        # kb_ids threaded as a single array param (ANY(...) in the function);
        # embedding_version threaded so mixed-model vectors can't drift.
        assert kb_ids in args
        assert "m:4096:c1" in args

    @pytest.mark.asyncio
    async def test_default_weights_and_rrf_k(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = []
        await store.search_chunks(kb_ids=[uuid.uuid4()], query="q")
        args = mock_db.fetch.call_args[0]
        # RRF weights match §5.1 (0.6/0.3/0.1); rrf_k defaults to 60.
        assert 0.6 in args
        assert 0.3 in args
        assert 0.1 in args
        assert 60 in args

    @pytest.mark.asyncio
    async def test_over_fetches_then_truncates_to_match_count(self):
        store, mock_db, _ = _make_store()
        # The function over-fetches ~over_fetch fused candidates; the method
        # reranks (no-op v1) then truncates to match_count.
        mock_db.fetch.return_value = [
            {"note_id": f"n{i}", "title": "T", "content": "b"} for i in range(50)
        ]
        result = await store.search_chunks(
            kb_ids=[uuid.uuid4()], query="q", match_count=15
        )
        assert len(result) == 15
        # The SQL LIMIT is the over-fetch, strictly larger than match_count.
        args = mock_db.fetch.call_args[0]
        assert any(isinstance(a, int) and a >= 50 for a in args)

    @pytest.mark.asyncio
    async def test_returns_knowledge_records(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [
            {
                "note_id": "n1",
                "title": "A",
                "note_type": "decision",
                "status": "active",
                "content": "body",
            }
        ]
        result = await store.search_chunks(kb_ids=[uuid.uuid4()], query="q")
        assert len(result) == 1
        assert isinstance(result[0], KnowledgeRecord)
        assert result[0].note_id == "n1"

    @pytest.mark.asyncio
    async def test_reranker_slot_preserves_fusion_order_by_default(self):
        store, mock_db, _ = _make_store()
        mock_db.fetch.return_value = [
            {"note_id": "first", "title": "A", "content": "b"},
            {"note_id": "second", "title": "B", "content": "b"},
        ]
        result = await store.search_chunks(kb_ids=[uuid.uuid4()], query="q")
        # The no-op reranker slot must not reorder the RRF ranking.
        assert [r.note_id for r in result] == ["first", "second"]
