"""Tests for orchestrator knowledge endpoints.

Everything above ``TestMaterializeEndpointIndexesInline`` REPLICATES the
endpoint logic locally rather than importing it: the Pydantic models, the lazy
``_get_knowledge_graph`` singleton, ``HTTPException`` and six handler bodies
are all copies kept in this file. They therefore pin the shape those handlers
were written to, not the shipped code — the production owners are
``orchestrator.services.knowledge_operations`` (handlers),
``orchestrator.schemas.knowledge`` (models) and
``orchestrator.services.knowledge_projection.KnowledgeGraphHandle`` (the
singleton), and ``tests/test_knowledge_access.py`` plus
``tests/test_knowledge_router_wire.py`` are what exercise those for real.

``TestMaterializeEndpointIndexesInline`` is the exception: it drives the real
``orchestrator.routers.knowledge.materialize_knowledge_note``.

Covers sections 16.1–16.9 of persistent_agent_tests.md:
  - _get_knowledge_graph() (lazy singleton)
  - KnowledgeSearchRequest model
  - KnowledgeNoteUpdate model
  - GET    /api/projects/{project_id}/knowledge/summary
  - GET    /api/projects/{project_id}/knowledge
  - GET    /api/projects/{project_id}/knowledge/{note_id}
  - POST   /api/projects/{project_id}/knowledge/search
  - PATCH  /api/projects/{project_id}/knowledge/{note_id}
  - DELETE /api/projects/{project_id}/knowledge/{note_id}
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel, Field, ValidationError


# =============================================================================
# Replicated Pydantic models (from orchestrator/main.py)
# =============================================================================


class KnowledgeSearchRequest(BaseModel):
    """Request body for hybrid knowledge search."""

    query: str = Field(..., description="Search query text")
    limit: int = Field(10, ge=1, le=50, description="Max results to return")


class KnowledgeNoteUpdate(BaseModel):
    """Request body for updating a knowledge note."""

    status: str | None = Field(
        None, description="New status: active, resolved, superseded, archived"
    )
    add_tags: list[str] | None = Field(None, description="Tags to add")
    remove_tags: list[str] | None = Field(None, description="Tags to remove")


# =============================================================================
# Replicated lazy singleton (from orchestrator/main.py)
# =============================================================================


_kg_cache: dict = {"instance": None, "tried": False}


def _get_knowledge_graph():
    """Lazily initialise and return the KnowledgeGraphDB singleton.

    Replicates orchestrator/main.py logic for testing.
    """
    global _kg_cache
    if _kg_cache["instance"] is not None:
        return _kg_cache["instance"]
    if _kg_cache["tried"]:
        return None
    _kg_cache["tried"] = True
    try:
        from shared.runtime.services.knowledge_graph import KnowledgeGraphDB

        kg = KnowledgeGraphDB()
        if not kg.connect():
            _kg_cache["instance"] = None
            return None
        _kg_cache["instance"] = kg
        return kg
    except Exception:
        _kg_cache["instance"] = None
        return None


def _reset_kg_cache():
    """Reset the lazy singleton cache between tests."""
    global _kg_cache
    _kg_cache["instance"] = None
    _kg_cache["tried"] = False


# =============================================================================
# Replicated endpoint handlers (from orchestrator/main.py)
# =============================================================================


async def get_knowledge_summary(
    project_id: str,
    postgres_db,
    vector_db,
) -> dict[str, Any]:
    """16.4: GET /api/projects/{project_id}/knowledge/summary"""
    project = await postgres_db.get_project(project_id)
    if not project:
        raise HTTPException(404, f"Project '{project_id}' not found")

    try:
        conn = vector_db
        type_rows = await conn.fetch(
            "SELECT note_type, COUNT(*) as cnt FROM knowledge_index "
            "WHERE project_id = $1 GROUP BY note_type ORDER BY cnt DESC",
            project_id,
        )
        by_type = {r["note_type"]: r["cnt"] for r in type_rows}

        status_rows = await conn.fetch(
            "SELECT status, COUNT(*) as cnt FROM knowledge_index "
            "WHERE project_id = $1 GROUP BY status ORDER BY cnt DESC",
            project_id,
        )
        by_status = {r["status"]: r["cnt"] for r in status_rows}

        total = sum(by_type.values())

        recent_rows = await conn.fetch(
            "SELECT note_id, title, note_type, status, modified_at "
            "FROM knowledge_index WHERE project_id = $1 "
            "ORDER BY modified_at DESC LIMIT 5",
            project_id,
        )
        recent = [dict(r) for r in recent_rows]

        return {
            "total": total,
            "by_type": by_type,
            "by_status": by_status,
            "recent": recent,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e)) from e


async def list_knowledge_notes(
    project_id: str,
    postgres_db,
    vector_db,
    *,
    note_type: str | None = None,
    status: str | None = None,
    tag: str | None = None,
    job_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """16.5: GET /api/projects/{project_id}/knowledge"""
    project = await postgres_db.get_project(project_id)
    if not project:
        raise HTTPException(404, f"Project '{project_id}' not found")

    try:
        conn = vector_db
        conditions = ["project_id = $1"]
        params: list[Any] = [project_id]
        idx = 2

        if note_type:
            conditions.append(f"note_type = ${idx}")
            params.append(note_type)
            idx += 1
        if status:
            conditions.append(f"status = ${idx}")
            params.append(status)
            idx += 1
        if tag:
            conditions.append(f"${idx} = ANY(tags)")
            params.append(tag)
            idx += 1
        if job_id:
            conditions.append(f"job_id = ${idx}::uuid")
            params.append(job_id)
            idx += 1

        where = " AND ".join(conditions)

        count_row = await conn.fetchrow(
            f"SELECT COUNT(*) as cnt FROM knowledge_index WHERE {where}",
            *params,
        )
        total = count_row["cnt"] if count_row else 0

        params.extend([limit, offset])
        rows = await conn.fetch(
            f"SELECT id, note_id, title, note_type, status, confidence, "
            f"tags, keywords, job_id, phase, "
            f"LEFT(content, 300) as content_preview, "
            f"created_at, modified_at "
            f"FROM knowledge_index WHERE {where} "
            f"ORDER BY modified_at DESC "
            f"LIMIT ${idx} OFFSET ${idx + 1}",
            *params,
        )
        notes = [dict(r) for r in rows]

        return {"notes": notes, "total": total, "limit": limit, "offset": offset}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e)) from e


async def get_knowledge_note(
    project_id: str,
    note_id: str,
    vector_db,
    kg_func=None,
) -> dict[str, Any]:
    """16.6: GET /api/projects/{project_id}/knowledge/{note_id}"""
    try:
        conn = vector_db
        row = await conn.fetchrow(
            "SELECT * FROM knowledge_index WHERE project_id = $1 AND note_id = $2",
            project_id,
            note_id,
        )
        if not row:
            raise HTTPException(404, f"Note '{note_id}' not found")

        result = dict(row)
        result.pop("embedding", None)
        result.pop("search_doc", None)

        kg = kg_func() if kg_func else None
        if kg:
            try:
                neo4j_note = kg.read_note(project_id, note_id)
                if neo4j_note:
                    result["relationships"] = neo4j_note.get("relationships", [])
            except Exception:
                result["relationships"] = []
        else:
            result["relationships"] = []

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e)) from e


async def search_knowledge(
    project_id: str,
    body: KnowledgeSearchRequest,
    postgres_db,
    vector_db,
    embedding_func=None,
) -> dict[str, Any]:
    """16.7: POST /api/projects/{project_id}/knowledge/search"""
    project = await postgres_db.get_project(project_id)
    if not project:
        raise HTTPException(404, f"Project '{project_id}' not found")

    try:
        conn = vector_db

        embedding = None
        if embedding_func:
            try:
                embedding = await embedding_func(body.query)
            except Exception:
                pass

        if embedding:
            rows = await conn.fetch(
                "SELECT * FROM knowledge_hybrid_search($1, $2::vector, $3, $4)",
                body.query,
                str(embedding),
                project_id,
                body.limit,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM knowledge_index "
                "WHERE project_id = $1 AND search_doc @@ websearch_to_tsquery($2) "
                "ORDER BY ts_rank_cd(search_doc, websearch_to_tsquery($2)) DESC "
                "LIMIT $3",
                project_id,
                body.query,
                body.limit,
            )

        notes = []
        for r in rows:
            d = dict(r)
            d.pop("embedding", None)
            d.pop("search_doc", None)
            notes.append(d)

        return {"notes": notes, "query": body.query, "total": len(notes)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e)) from e


async def update_knowledge_note(
    project_id: str,
    note_id: str,
    body: KnowledgeNoteUpdate,
    vector_db,
    kg_func=None,
) -> dict[str, str]:
    """16.8: PATCH /api/projects/{project_id}/knowledge/{note_id}"""
    valid_statuses = {"active", "resolved", "superseded", "archived"}
    if body.status and body.status not in valid_statuses:
        raise HTTPException(
            400, f"Invalid status '{body.status}'. Must be one of: {valid_statuses}"
        )

    try:
        conn = vector_db
        row = await conn.fetchrow(
            "SELECT note_id FROM knowledge_index "
            "WHERE project_id = $1 AND note_id = $2",
            project_id,
            note_id,
        )
        if not row:
            raise HTTPException(404, f"Note '{note_id}' not found")

        updates = []
        params: list[Any] = [project_id, note_id]
        idx = 3

        if body.status:
            updates.append(f"status = ${idx}")
            params.append(body.status)
            idx += 1

        if body.add_tags:
            updates.append(f"tags = array_cat(tags, ${idx}::text[])")
            params.append(body.add_tags)
            idx += 1

        if body.remove_tags:
            for rm_tag in body.remove_tags:
                updates.append(f"tags = array_remove(tags, ${idx})")
                params.append(rm_tag)
                idx += 1

        if not updates:
            return {"status": "no_changes"}

        updates.append("modified_at = NOW()")
        set_clause = ", ".join(updates)
        await conn.execute(
            f"UPDATE knowledge_index SET {set_clause} "
            f"WHERE project_id = $1 AND note_id = $2",
            *params,
        )

        kg = kg_func() if kg_func else None
        if kg:
            try:
                update_kwargs: dict[str, Any] = {}
                if body.status:
                    update_kwargs["status"] = body.status
                if body.add_tags:
                    update_kwargs["add_tags"] = body.add_tags
                if update_kwargs:
                    kg.update_note(project_id, note_id, **update_kwargs)
            except Exception:
                pass  # non-fatal

        return {"status": "updated"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e)) from e


async def delete_knowledge_note(
    project_id: str,
    note_id: str,
    vector_db,
    kg_func=None,
) -> dict[str, str]:
    """16.9: DELETE /api/projects/{project_id}/knowledge/{note_id}"""
    try:
        conn = vector_db
        result = await conn.execute(
            "DELETE FROM knowledge_index WHERE project_id = $1 AND note_id = $2",
            project_id,
            note_id,
        )
        if result == "DELETE 0":
            raise HTTPException(404, f"Note '{note_id}' not found")

        kg = kg_func() if kg_func else None
        if kg:
            try:
                kg._db.execute_write(
                    "MATCH (n:Note {project_id: $pid, id: $nid}) DETACH DELETE n",
                    {"pid": project_id, "nid": note_id},
                )
            except Exception:
                pass  # non-fatal

        return {"status": "deleted"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e)) from e


# =============================================================================
# Minimal HTTPException for replicated logic
# =============================================================================


class HTTPException(Exception):
    def __init__(self, status_code: int, detail: str = ""):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


# =============================================================================
# Helpers
# =============================================================================


def _make_conn():
    """Create a mock async DB connection."""
    conn = AsyncMock()
    conn.fetch.return_value = []
    conn.fetchrow.return_value = None
    conn.execute.return_value = "DELETE 1"
    return conn


def _make_postgres_db(project_exists=True):
    """Create a mock postgres_db with get_project()."""
    db = AsyncMock()
    if project_exists:
        db.get_project.return_value = {"id": "proj-1", "name": "Test Project"}
    else:
        db.get_project.return_value = None
    return db


# =============================================================================
# 16.1: _get_knowledge_graph() (lazy singleton)
# =============================================================================


class TestGetKnowledgeGraph:
    """Tests for _get_knowledge_graph() lazy singleton."""

    def setup_method(self):
        _reset_kg_cache()

    def teardown_method(self):
        _reset_kg_cache()

    @patch("shared.runtime.services.knowledge_graph.KnowledgeGraphDB")
    def test_returns_instance_on_successful_connect(self, MockKGDB):
        mock_kg = MagicMock()
        mock_kg.connect.return_value = True
        MockKGDB.return_value = mock_kg
        result = _get_knowledge_graph()
        assert result is mock_kg

    @patch("shared.runtime.services.knowledge_graph.KnowledgeGraphDB")
    def test_returns_cached_on_subsequent_calls(self, MockKGDB):
        mock_kg = MagicMock()
        mock_kg.connect.return_value = True
        MockKGDB.return_value = mock_kg
        first = _get_knowledge_graph()
        second = _get_knowledge_graph()
        assert first is second
        MockKGDB.assert_called_once()

    @patch("shared.runtime.services.knowledge_graph.KnowledgeGraphDB")
    def test_returns_none_on_connect_failure(self, MockKGDB):
        mock_kg = MagicMock()
        mock_kg.connect.return_value = False
        MockKGDB.return_value = mock_kg
        result = _get_knowledge_graph()
        assert result is None

    @patch(
        "shared.runtime.services.knowledge_graph.KnowledgeGraphDB",
        side_effect=Exception("fail"),
    )
    def test_returns_none_on_exception(self, MockKGDB):
        result = _get_knowledge_graph()
        assert result is None

    @patch.dict("sys.modules", {"shared.runtime.services.knowledge_graph": None})
    def test_returns_none_on_import_error(self):
        result = _get_knowledge_graph()
        assert result is None


# =============================================================================
# 16.2: KnowledgeSearchRequest model
# =============================================================================


class TestKnowledgeSearchRequest:
    """Tests for the KnowledgeSearchRequest Pydantic model."""

    def test_query_required(self):
        with pytest.raises(ValidationError):
            KnowledgeSearchRequest()

    def test_query_accepted(self):
        req = KnowledgeSearchRequest(query="test search")
        assert req.query == "test search"

    def test_limit_default_10(self):
        req = KnowledgeSearchRequest(query="test")
        assert req.limit == 10

    def test_limit_custom(self):
        req = KnowledgeSearchRequest(query="test", limit=25)
        assert req.limit == 25

    def test_limit_min_1(self):
        with pytest.raises(ValidationError):
            KnowledgeSearchRequest(query="test", limit=0)

    def test_limit_max_50(self):
        with pytest.raises(ValidationError):
            KnowledgeSearchRequest(query="test", limit=51)

    def test_limit_boundary_1(self):
        req = KnowledgeSearchRequest(query="test", limit=1)
        assert req.limit == 1

    def test_limit_boundary_50(self):
        req = KnowledgeSearchRequest(query="test", limit=50)
        assert req.limit == 50


# =============================================================================
# 16.3: KnowledgeNoteUpdate model
# =============================================================================


class TestKnowledgeNoteUpdate:
    """Tests for the KnowledgeNoteUpdate Pydantic model."""

    def test_all_fields_optional(self):
        update = KnowledgeNoteUpdate()
        assert update.status is None
        assert update.add_tags is None
        assert update.remove_tags is None

    def test_status_set(self):
        update = KnowledgeNoteUpdate(status="active")
        assert update.status == "active"

    def test_add_tags_set(self):
        update = KnowledgeNoteUpdate(add_tags=["foo", "bar"])
        assert update.add_tags == ["foo", "bar"]

    def test_remove_tags_set(self):
        update = KnowledgeNoteUpdate(remove_tags=["baz"])
        assert update.remove_tags == ["baz"]

    def test_all_fields_set(self):
        update = KnowledgeNoteUpdate(
            status="archived",
            add_tags=["new"],
            remove_tags=["old"],
        )
        assert update.status == "archived"
        assert update.add_tags == ["new"]
        assert update.remove_tags == ["old"]


# =============================================================================
# 16.4: GET /api/projects/{project_id}/knowledge/summary
# =============================================================================


class TestGetKnowledgeSummary:
    """Tests for the knowledge summary endpoint."""

    @pytest.mark.asyncio
    async def test_404_when_project_not_found(self):
        db = _make_postgres_db(project_exists=False)
        conn = _make_conn()
        with pytest.raises(HTTPException) as exc_info:
            await get_knowledge_summary("proj-1", db, conn)
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_returns_by_type_counts(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetch.side_effect = [
            # type_rows
            [{"note_type": "learning", "cnt": 5}, {"note_type": "code", "cnt": 3}],
            # status_rows
            [{"note_type": "active", "cnt": 8}],  # reuses note_type key for status
            # recent_rows
            [],
        ]
        # Fix status_rows to use "status" key
        conn.fetch.side_effect = [
            [{"note_type": "learning", "cnt": 5}, {"note_type": "code", "cnt": 3}],
            [{"status": "active", "cnt": 8}],
            [],
        ]
        result = await get_knowledge_summary("proj-1", db, conn)
        assert result["by_type"] == {"learning": 5, "code": 3}

    @pytest.mark.asyncio
    async def test_returns_by_status_counts(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetch.side_effect = [
            [],
            [{"status": "active", "cnt": 5}, {"status": "archived", "cnt": 2}],
            [],
        ]
        result = await get_knowledge_summary("proj-1", db, conn)
        assert result["by_status"] == {"active": 5, "archived": 2}

    @pytest.mark.asyncio
    async def test_total_is_sum_of_by_type(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetch.side_effect = [
            [{"note_type": "learning", "cnt": 5}, {"note_type": "code", "cnt": 3}],
            [],
            [],
        ]
        result = await get_knowledge_summary("proj-1", db, conn)
        assert result["total"] == 8

    @pytest.mark.asyncio
    async def test_returns_recent_notes(self):
        db = _make_postgres_db()
        conn = _make_conn()
        recent = [
            {
                "note_id": "n1",
                "title": "T1",
                "note_type": "learning",
                "status": "active",
                "modified_at": "2026-01-01",
            },
        ]
        conn.fetch.side_effect = [[], [], recent]
        result = await get_knowledge_summary("proj-1", db, conn)
        assert len(result["recent"]) == 1
        assert result["recent"][0]["note_id"] == "n1"

    @pytest.mark.asyncio
    async def test_500_on_database_error(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetch.side_effect = Exception("DB error")
        with pytest.raises(HTTPException) as exc_info:
            await get_knowledge_summary("proj-1", db, conn)
        assert exc_info.value.status_code == 500


# =============================================================================
# 16.5: GET /api/projects/{project_id}/knowledge
# =============================================================================


class TestListKnowledgeNotes:
    """Tests for the knowledge list endpoint."""

    @pytest.mark.asyncio
    async def test_404_when_project_not_found(self):
        db = _make_postgres_db(project_exists=False)
        conn = _make_conn()
        with pytest.raises(HTTPException) as exc_info:
            await list_knowledge_notes("proj-1", db, conn)
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_returns_paginated_result(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetchrow.return_value = {"cnt": 2}
        conn.fetch.return_value = [
            {
                "id": 1,
                "note_id": "n1",
                "title": "T1",
                "note_type": "learning",
                "status": "active",
                "confidence": None,
                "tags": [],
                "keywords": None,
                "job_id": None,
                "phase": None,
                "content_preview": "text",
                "created_at": None,
                "modified_at": None,
            },
        ]
        result = await list_knowledge_notes("proj-1", db, conn)
        assert "notes" in result
        assert "total" in result
        assert "limit" in result
        assert "offset" in result

    @pytest.mark.asyncio
    async def test_default_limit_50(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetchrow.return_value = {"cnt": 0}
        result = await list_knowledge_notes("proj-1", db, conn)
        assert result["limit"] == 50

    @pytest.mark.asyncio
    async def test_filter_by_type(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetchrow.return_value = {"cnt": 0}
        await list_knowledge_notes("proj-1", db, conn, note_type="learning")
        # Verify the filter was included in the query
        count_call = conn.fetchrow.call_args
        query = count_call[0][0]
        assert "note_type = $2" in query

    @pytest.mark.asyncio
    async def test_filter_by_status(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetchrow.return_value = {"cnt": 0}
        await list_knowledge_notes("proj-1", db, conn, status="active")
        query = conn.fetchrow.call_args[0][0]
        assert "status = $2" in query

    @pytest.mark.asyncio
    async def test_filter_by_tag(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetchrow.return_value = {"cnt": 0}
        await list_knowledge_notes("proj-1", db, conn, tag="python")
        query = conn.fetchrow.call_args[0][0]
        assert "ANY(tags)" in query

    @pytest.mark.asyncio
    async def test_filter_by_job_id(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetchrow.return_value = {"cnt": 0}
        await list_knowledge_notes("proj-1", db, conn, job_id="abc-123")
        query = conn.fetchrow.call_args[0][0]
        assert "job_id" in query
        assert "::uuid" in query

    @pytest.mark.asyncio
    async def test_content_preview_in_query(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetchrow.return_value = {"cnt": 0}
        await list_knowledge_notes("proj-1", db, conn)
        fetch_call = conn.fetch.call_args
        query = fetch_call[0][0]
        assert "LEFT(content, 300)" in query

    @pytest.mark.asyncio
    async def test_ordered_by_modified_at_desc(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetchrow.return_value = {"cnt": 0}
        await list_knowledge_notes("proj-1", db, conn)
        query = conn.fetch.call_args[0][0]
        assert "ORDER BY modified_at DESC" in query

    @pytest.mark.asyncio
    async def test_500_on_database_error(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetchrow.side_effect = Exception("DB error")
        with pytest.raises(HTTPException) as exc_info:
            await list_knowledge_notes("proj-1", db, conn)
        assert exc_info.value.status_code == 500


# =============================================================================
# 16.6: GET /api/projects/{project_id}/knowledge/{note_id}
# =============================================================================


class TestGetKnowledgeNote:
    """Tests for the single note endpoint."""

    @pytest.mark.asyncio
    async def test_404_when_not_found(self):
        conn = _make_conn()
        conn.fetchrow.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            await get_knowledge_note("proj-1", "note-1", conn)
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_removes_embedding_field(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {
            "note_id": "n1",
            "content": "text",
            "embedding": [0.1, 0.2],
            "search_doc": "tsvec",
        }
        result = await get_knowledge_note("proj-1", "n1", conn)
        assert "embedding" not in result
        assert "search_doc" not in result

    @pytest.mark.asyncio
    async def test_removes_search_doc_field(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {
            "note_id": "n1",
            "content": "text",
            "search_doc": "tsvec",
        }
        result = await get_knowledge_note("proj-1", "n1", conn)
        assert "search_doc" not in result

    @pytest.mark.asyncio
    async def test_fetches_relationships_from_neo4j(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {"note_id": "n1", "content": "text"}
        mock_kg = MagicMock()
        mock_kg.read_note.return_value = {
            "relationships": [{"type": "REFERENCES", "target": "n2"}]
        }
        result = await get_knowledge_note("proj-1", "n1", conn, kg_func=lambda: mock_kg)
        assert len(result["relationships"]) == 1
        assert result["relationships"][0]["target"] == "n2"

    @pytest.mark.asyncio
    async def test_empty_relationships_when_neo4j_unavailable(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {"note_id": "n1", "content": "text"}
        result = await get_knowledge_note("proj-1", "n1", conn, kg_func=None)
        assert result["relationships"] == []

    @pytest.mark.asyncio
    async def test_empty_relationships_on_neo4j_exception(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {"note_id": "n1", "content": "text"}
        mock_kg = MagicMock()
        mock_kg.read_note.side_effect = Exception("Neo4j down")
        result = await get_knowledge_note("proj-1", "n1", conn, kg_func=lambda: mock_kg)
        assert result["relationships"] == []

    @pytest.mark.asyncio
    async def test_empty_relationships_when_kg_returns_none(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {"note_id": "n1", "content": "text"}
        result = await get_knowledge_note("proj-1", "n1", conn, kg_func=lambda: None)
        assert result["relationships"] == []

    @pytest.mark.asyncio
    async def test_500_on_database_error(self):
        conn = _make_conn()
        conn.fetchrow.side_effect = Exception("DB error")
        with pytest.raises(HTTPException) as exc_info:
            await get_knowledge_note("proj-1", "n1", conn)
        assert exc_info.value.status_code == 500


# =============================================================================
# 16.7: POST /api/projects/{project_id}/knowledge/search
# =============================================================================


class TestSearchKnowledge:
    """Tests for the knowledge search endpoint."""

    @pytest.mark.asyncio
    async def test_404_when_project_not_found(self):
        db = _make_postgres_db(project_exists=False)
        conn = _make_conn()
        body = KnowledgeSearchRequest(query="test")
        with pytest.raises(HTTPException) as exc_info:
            await search_knowledge("proj-1", body, db, conn)
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_dense_sparse_search_with_embedding(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetch.return_value = [
            {"note_id": "n1", "content": "text", "embedding": [0.1], "search_doc": "tv"}
        ]

        async def embed_fn(q):
            return [0.1, 0.2, 0.3]

        body = KnowledgeSearchRequest(query="test")
        await search_knowledge("proj-1", body, db, conn, embedding_func=embed_fn)
        # Should use hybrid search function
        query = conn.fetch.call_args[0][0]
        assert "knowledge_hybrid_search" in query

    @pytest.mark.asyncio
    async def test_sparse_only_fallback_when_no_embedding(self):
        db = _make_postgres_db()
        conn = _make_conn()
        body = KnowledgeSearchRequest(query="test")
        await search_knowledge("proj-1", body, db, conn, embedding_func=None)
        query = conn.fetch.call_args[0][0]
        assert "websearch_to_tsquery" in query

    @pytest.mark.asyncio
    async def test_sparse_fallback_when_embedding_fails(self):
        db = _make_postgres_db()
        conn = _make_conn()

        async def embed_fn(q):
            raise Exception("embedding service down")

        body = KnowledgeSearchRequest(query="test")
        await search_knowledge("proj-1", body, db, conn, embedding_func=embed_fn)
        query = conn.fetch.call_args[0][0]
        assert "websearch_to_tsquery" in query

    @pytest.mark.asyncio
    async def test_removes_embedding_and_search_doc(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetch.return_value = [
            {"note_id": "n1", "content": "text", "embedding": [0.1], "search_doc": "tv"}
        ]
        body = KnowledgeSearchRequest(query="test")
        result = await search_knowledge("proj-1", body, db, conn)
        for note in result["notes"]:
            assert "embedding" not in note
            assert "search_doc" not in note

    @pytest.mark.asyncio
    async def test_returns_query_and_total(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetch.return_value = [
            {"note_id": "n1", "content": "text"},
            {"note_id": "n2", "content": "text2"},
        ]
        body = KnowledgeSearchRequest(query="my search")
        result = await search_knowledge("proj-1", body, db, conn)
        assert result["query"] == "my search"
        assert result["total"] == 2

    @pytest.mark.asyncio
    async def test_500_on_database_error(self):
        db = _make_postgres_db()
        conn = _make_conn()
        conn.fetch.side_effect = Exception("DB error")
        body = KnowledgeSearchRequest(query="test")
        with pytest.raises(HTTPException) as exc_info:
            await search_knowledge("proj-1", body, db, conn)
        assert exc_info.value.status_code == 500


# =============================================================================
# 16.8: PATCH /api/projects/{project_id}/knowledge/{note_id}
# =============================================================================


class TestUpdateKnowledgeNote:
    """Tests for the knowledge update endpoint."""

    @pytest.mark.asyncio
    async def test_400_invalid_status(self):
        conn = _make_conn()
        body = KnowledgeNoteUpdate(status="invalid")
        with pytest.raises(HTTPException) as exc_info:
            await update_knowledge_note("proj-1", "n1", body, conn)
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_404_when_note_not_found(self):
        conn = _make_conn()
        conn.fetchrow.return_value = None
        body = KnowledgeNoteUpdate(status="active")
        with pytest.raises(HTTPException) as exc_info:
            await update_knowledge_note("proj-1", "n1", body, conn)
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_no_changes_when_empty_body(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {"note_id": "n1"}
        body = KnowledgeNoteUpdate()
        result = await update_knowledge_note("proj-1", "n1", body, conn)
        assert result == {"status": "no_changes"}

    @pytest.mark.asyncio
    async def test_updates_status(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {"note_id": "n1"}
        body = KnowledgeNoteUpdate(status="resolved")
        result = await update_knowledge_note("proj-1", "n1", body, conn)
        assert result == {"status": "updated"}
        execute_call = conn.execute.call_args
        query = execute_call[0][0]
        assert "status = $3" in query

    @pytest.mark.asyncio
    async def test_adds_tags(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {"note_id": "n1"}
        body = KnowledgeNoteUpdate(add_tags=["new-tag"])
        result = await update_knowledge_note("proj-1", "n1", body, conn)
        assert result == {"status": "updated"}
        query = conn.execute.call_args[0][0]
        assert "array_cat" in query

    @pytest.mark.asyncio
    async def test_removes_tags_iterates(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {"note_id": "n1"}
        body = KnowledgeNoteUpdate(remove_tags=["tag1", "tag2"])
        await update_knowledge_note("proj-1", "n1", body, conn)
        query = conn.execute.call_args[0][0]
        assert query.count("array_remove") == 2

    @pytest.mark.asyncio
    async def test_updates_modified_at(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {"note_id": "n1"}
        body = KnowledgeNoteUpdate(status="active")
        await update_knowledge_note("proj-1", "n1", body, conn)
        query = conn.execute.call_args[0][0]
        assert "modified_at = NOW()" in query

    @pytest.mark.asyncio
    async def test_updates_neo4j_when_available(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {"note_id": "n1"}
        mock_kg = MagicMock()
        body = KnowledgeNoteUpdate(status="archived")
        await update_knowledge_note("proj-1", "n1", body, conn, kg_func=lambda: mock_kg)
        mock_kg.update_note.assert_called_once()

    @pytest.mark.asyncio
    async def test_neo4j_update_failure_nonfatal(self):
        conn = _make_conn()
        conn.fetchrow.return_value = {"note_id": "n1"}
        mock_kg = MagicMock()
        mock_kg.update_note.side_effect = Exception("Neo4j down")
        body = KnowledgeNoteUpdate(status="active")
        result = await update_knowledge_note(
            "proj-1", "n1", body, conn, kg_func=lambda: mock_kg
        )
        assert result == {"status": "updated"}

    @pytest.mark.asyncio
    async def test_valid_statuses_accepted(self):
        for status in ("active", "resolved", "superseded", "archived"):
            conn = _make_conn()
            conn.fetchrow.return_value = {"note_id": "n1"}
            body = KnowledgeNoteUpdate(status=status)
            result = await update_knowledge_note("proj-1", "n1", body, conn)
            assert result == {"status": "updated"}

    @pytest.mark.asyncio
    async def test_500_on_database_error(self):
        conn = _make_conn()
        conn.fetchrow.side_effect = Exception("DB error")
        body = KnowledgeNoteUpdate(status="active")
        with pytest.raises(HTTPException) as exc_info:
            await update_knowledge_note("proj-1", "n1", body, conn)
        assert exc_info.value.status_code == 500


# =============================================================================
# 16.9: DELETE /api/projects/{project_id}/knowledge/{note_id}
# =============================================================================


class TestDeleteKnowledgeNote:
    """Tests for the knowledge delete endpoint."""

    @pytest.mark.asyncio
    async def test_404_when_delete_affects_zero_rows(self):
        conn = _make_conn()
        conn.execute.return_value = "DELETE 0"
        with pytest.raises(HTTPException) as exc_info:
            await delete_knowledge_note("proj-1", "n1", conn)
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_deletes_from_pgvector(self):
        conn = _make_conn()
        conn.execute.return_value = "DELETE 1"
        result = await delete_knowledge_note("proj-1", "n1", conn)
        assert result == {"status": "deleted"}
        query = conn.execute.call_args[0][0]
        assert "DELETE FROM knowledge_index" in query

    @pytest.mark.asyncio
    async def test_deletes_from_neo4j_when_available(self):
        conn = _make_conn()
        conn.execute.return_value = "DELETE 1"
        mock_kg = MagicMock()
        mock_kg._db = MagicMock()
        await delete_knowledge_note("proj-1", "n1", conn, kg_func=lambda: mock_kg)
        mock_kg._db.execute_write.assert_called_once()
        cypher = mock_kg._db.execute_write.call_args[0][0]
        assert "DETACH DELETE" in cypher

    @pytest.mark.asyncio
    async def test_neo4j_delete_failure_nonfatal(self):
        conn = _make_conn()
        conn.execute.return_value = "DELETE 1"
        mock_kg = MagicMock()
        mock_kg._db = MagicMock()
        mock_kg._db.execute_write.side_effect = Exception("Neo4j down")
        result = await delete_knowledge_note(
            "proj-1", "n1", conn, kg_func=lambda: mock_kg
        )
        assert result == {"status": "deleted"}

    @pytest.mark.asyncio
    async def test_no_neo4j_delete_when_unavailable(self):
        conn = _make_conn()
        conn.execute.return_value = "DELETE 1"
        result = await delete_knowledge_note("proj-1", "n1", conn, kg_func=None)
        assert result == {"status": "deleted"}

    @pytest.mark.asyncio
    async def test_500_on_database_error(self):
        conn = _make_conn()
        conn.execute.side_effect = Exception("DB error")
        with pytest.raises(HTTPException) as exc_info:
            await delete_knowledge_note("proj-1", "n1", conn)
        assert exc_info.value.status_code == 500


def _materialize_deps():
    """Router dependencies for the real materialize endpoint.

    ``require_internal`` is stubbed here for the same reason the old test
    patched ``main.require_internal``: these cases are about the inline
    indexer the endpoint hands the materialiser, not about the
    ``X-Internal-Key`` gate (``tests/test_kb_materialize.py`` owns that).
    """
    from orchestrator.routers.knowledge import KnowledgeDependencies
    from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
    from orchestrator.services.knowledge_index import KnowledgeIndexDependencies
    from orchestrator.services.knowledge_operations import (
        KnowledgeOperationDependencies,
    )

    import logging
    from types import SimpleNamespace

    log = logging.getLogger("tests.knowledge_endpoints")
    store, vector, gitea = AsyncMock(), MagicMock(), MagicMock()
    return KnowledgeDependencies(
        store=store,
        operations=KnowledgeOperationDependencies(
            store=store,
            vector_db=vector,
            gitea_client=gitea,
            logger=log,
            graph=SimpleNamespace(get=lambda: None),
            knowledge_index=KnowledgeIndexDependencies(
                store=store,
                vector_db=vector,
                gitea_client=gitea,
                logger=log,
                tasks=KbDatasourceTaskRegistry(),
                inject_system_kb_embedding_profile=AsyncMock(return_value=None),
            ),
        ),
        require_internal_service=AsyncMock(return_value=None),
    )


class TestMaterializeEndpointIndexesInline:
    """The endpoint must hand the materialiser an indexer, or nothing is
    searchable until the next sweep — the whole point of Slice A."""

    @pytest.mark.asyncio
    async def test_endpoint_passes_a_store_and_embedding_service(self):
        from orchestrator.routers.knowledge import materialize_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeMaterializeRequest

        captured = {}

        async def _fake_materialize(**kwargs):
            captured.update(kwargs)
            return {"status": "committed", "indexed": True, "index_reason": None}

        svc = MagicMock()
        with (
            patch(
                "orchestrator.services.knowledge_index.build_kb_embedding_service",
                AsyncMock(return_value=svc),
            ),
            patch(
                "orchestrator.services.kb_materialize.materialize_knowledge_note",
                _fake_materialize,
            ),
        ):
            result = await materialize_knowledge_note(
                request=MagicMock(),
                project_id="1a387b4d-0000-0000-0000-000000000000",
                body=KnowledgeMaterializeRequest(
                    slug="a-note", content="---\nid: a-note\n---\n# A\n"
                ),
                dependencies=_materialize_deps(),
            )

        assert result["indexed"] is True
        assert captured["store"] is not None
        assert captured["embedding_service"] is svc

    @pytest.mark.asyncio
    async def test_no_embedding_service_still_commits(self):
        from orchestrator.routers.knowledge import materialize_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeMaterializeRequest

        captured = {}

        async def _fake_materialize(**kwargs):
            captured.update(kwargs)
            return {
                "status": "committed",
                "indexed": False,
                "index_reason": "no-indexer",
            }

        with (
            patch(
                "orchestrator.services.knowledge_index.build_kb_embedding_service",
                AsyncMock(return_value=None),
            ),
            patch(
                "orchestrator.services.kb_materialize.materialize_knowledge_note",
                _fake_materialize,
            ),
        ):
            result = await materialize_knowledge_note(
                request=MagicMock(),
                project_id="1a387b4d-0000-0000-0000-000000000000",
                body=KnowledgeMaterializeRequest(
                    slug="a-note", content="---\nid: a-note\n---\n# A\n"
                ),
                dependencies=_materialize_deps(),
            )

        assert result["status"] == "committed"
        assert captured["store"] is None
        assert captured["embedding_service"] is None

    @pytest.mark.asyncio
    async def test_embedding_service_resolution_error_still_commits(self):
        """A misconfigured embedding backend must degrade, not 500 the write."""
        from orchestrator.routers.knowledge import materialize_knowledge_note
        from orchestrator.schemas.knowledge import KnowledgeMaterializeRequest

        captured = {}

        async def _fake_materialize(**kwargs):
            captured.update(kwargs)
            return {
                "status": "committed",
                "indexed": False,
                "index_reason": "no-indexer",
            }

        with (
            patch(
                "orchestrator.services.knowledge_index.build_kb_embedding_service",
                AsyncMock(side_effect=RuntimeError("catalog unavailable")),
            ),
            patch(
                "orchestrator.services.kb_materialize.materialize_knowledge_note",
                _fake_materialize,
            ),
        ):
            result = await materialize_knowledge_note(
                request=MagicMock(),
                project_id="1a387b4d-0000-0000-0000-000000000000",
                body=KnowledgeMaterializeRequest(
                    slug="a-note", content="---\nid: a-note\n---\n# A\n"
                ),
                dependencies=_materialize_deps(),
            )

        assert result["status"] == "committed"
        assert captured["store"] is None
        assert captured["embedding_service"] is None
