"""Wire contracts for the extracted project knowledge-base router.

Before R1.B03 these handlers lived in ``main.py``. The behaviours pinned here
are the ones a handler move can quietly change:

* which routes are member-gated and which are the internal (``X-Internal-Key``)
  agent/operator boundary — a materialize or purge that starts accepting a
  logged-in user would be a privilege change, not a refactor;
* that ``/knowledge/summary`` is still declared *before* ``/knowledge/{note_id}``,
  so the summary is not served as a note lookup for the id ``"summary"``;
* the 404 detail shape for a missing note, which the cockpit matches on;
* that a PATCH refuses an archived project (``allow_archived=False``) while a
  DELETE does not — editing a note is a content mutation, deleting one is
  teardown;
* that every knowledge mutation (reindex, export, PATCH, DELETE) needs editor,
  and that ``ready``/``parallel-safe`` tags and charter notes need the owner;
* that the gate runs before any store work.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from tests._mounted_router import mount_router


PROJECT_ID = "00000000-0000-0000-0000-0000000000b1"
USER = {"id": "00000000-0000-0000-0000-0000000000c1", "is_admin": False}
PROJECT = {"id": PROJECT_ID, "name": "Atlas"}


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


def _pool(conn):
    """Minimal asyncpg-pool stand-in: ``async with pool.acquire() as conn``."""
    return SimpleNamespace(acquire=lambda: _Acquire(conn))


def _conn(**over):
    conn = SimpleNamespace(
        fetch=AsyncMock(return_value=[]),
        fetchrow=AsyncMock(return_value=None),
        fetchval=AsyncMock(return_value=None),
        execute=AsyncMock(return_value=None),
    )
    for key, value in over.items():
        setattr(conn, key, value)
    return conn


def _wire(*, conn=None, member_gate=None, internal_gate=None, graph=None, store=None):
    from orchestrator.routers.knowledge import KnowledgeDependencies as RouteDeps
    from orchestrator.routers.knowledge import router
    from orchestrator.services.knowledge_operations import (
        KnowledgeOperationDependencies as OpDeps,
    )

    connection = conn if conn is not None else _conn()
    vector_db = _pool(connection)
    db = store or SimpleNamespace(
        finish_knowledge_projection=AsyncMock(return_value={"synced": True}),
    )
    calls = SimpleNamespace(member=[], internal=[])

    async def member(request, _store, project_id, **kwargs):
        calls.member.append((project_id, kwargs))
        if member_gate is not None:
            return await member_gate(request, _store, project_id, **kwargs)
        return (USER, PROJECT)

    async def internal(request):
        calls.internal.append(request.url.path)
        if internal_gate is not None:
            return await internal_gate(request)
        return None

    ops = OpDeps(
        store=db,
        vector_db=vector_db,
        gitea_client=MagicMock(),
        logger=MagicMock(),
        graph=graph if graph is not None else SimpleNamespace(get=lambda: None),
        knowledge_index=MagicMock(),
    )
    deps = RouteDeps(
        store=db,
        operations=ops,
        require_project_member=member,
        require_internal_service=internal,
    )
    app = mount_router(
        router, factories={"knowledge_dependencies_factory": lambda: deps}
    )
    return SimpleNamespace(
        client=TestClient(app),
        conn=connection,
        store=db,
        calls=calls,
        operations=ops,
    )


# =============================================================================
# Gate tiers
# =============================================================================


@pytest.mark.parametrize(
    "method,path",
    [
        ("get", f"/api/projects/{PROJECT_ID}/knowledge/summary"),
        ("get", f"/api/projects/{PROJECT_ID}/knowledge"),
        ("get", f"/api/projects/{PROJECT_ID}/knowledge/note-1"),
        ("post", f"/api/projects/{PROJECT_ID}/knowledge/reindex"),
        ("post", f"/api/projects/{PROJECT_ID}/knowledge/search"),
        ("patch", f"/api/projects/{PROJECT_ID}/knowledge/note-1"),
        ("delete", f"/api/projects/{PROJECT_ID}/knowledge/note-1"),
        ("post", f"/api/projects/{PROJECT_ID}/knowledge/export"),
    ],
)
def test_member_routes_refuse_a_non_member_before_touching_the_store(method, path):
    async def deny(*_a, **_k):
        raise HTTPException(status_code=403, detail="Not a member of this project")

    wired = _wire(member_gate=deny)
    kwargs = {"json": {"query": "x"}} if method == "post" else {}
    if method == "patch":
        kwargs = {"json": {"status": "resolved"}}
    response = getattr(wired.client, method)(path, **kwargs)

    assert response.status_code == 403
    assert response.json()["detail"] == "Not a member of this project"
    wired.conn.fetch.assert_not_awaited()
    wired.conn.fetchrow.assert_not_awaited()


@pytest.mark.parametrize(
    "path,body",
    [
        (
            f"/api/projects/{PROJECT_ID}/knowledge/materialize",
            {"slug": "s", "content": "c"},
        ),
        (
            f"/api/projects/{PROJECT_ID}/knowledge/materialize/intent-1/projection",
            {"synced": True},
        ),
        (f"/api/projects/{PROJECT_ID}/knowledge/delete", {"slug": "s"}),
    ],
)
def test_internal_routes_are_the_agent_boundary_not_the_member_gate(path, body):
    """No user gate runs on these; the shared-secret gate is the only one."""

    async def deny(_request):
        raise HTTPException(status_code=403, detail="Internal key required")

    wired = _wire(internal_gate=deny)
    response = wired.client.post(path, json=body)

    assert response.status_code == 403
    assert response.json()["detail"] == "Internal key required"
    assert wired.calls.member == []
    assert wired.calls.internal == [path]


def test_member_gate_does_not_open_the_internal_purge():
    """A member-gated caller cannot reach the purge lane by being a member."""
    wired = _wire()
    wired.client.post(
        f"/api/projects/{PROJECT_ID}/knowledge/delete", json={"slug": "s"}
    )

    assert wired.calls.member == []
    assert wired.calls.internal == [f"/api/projects/{PROJECT_ID}/knowledge/delete"]


# =============================================================================
# Route order: summary must not be served as a note id
# =============================================================================


def test_summary_is_not_swallowed_by_the_note_id_route():
    conn = _conn(fetch=AsyncMock(return_value=[]))
    wired = _wire(conn=conn)

    response = wired.client.get(f"/api/projects/{PROJECT_ID}/knowledge/summary")

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"total", "by_type", "by_status", "recent", "gardening"}
    # The note lookup ("SELECT * FROM knowledge_index ...") never ran.
    conn.fetchrow.assert_not_awaited()


def test_a_real_note_id_still_reaches_the_note_route():
    row = {"note_id": "adr-7", "title": "T", "embedding": [0.1], "search_doc": "x"}
    conn = _conn(fetchrow=AsyncMock(return_value=row))
    wired = _wire(conn=conn)

    response = wired.client.get(f"/api/projects/{PROJECT_ID}/knowledge/adr-7")

    assert response.status_code == 200
    body = response.json()
    assert body["note_id"] == "adr-7"
    # Vector/tsvector columns never leave the process.
    assert "embedding" not in body
    assert "search_doc" not in body
    # Neo4j absent → empty relationships, not a failure.
    assert body["relationships"] == []


# =============================================================================
# Not-found shapes
# =============================================================================


def test_missing_note_reports_the_project_scoped_404_detail():
    wired = _wire(conn=_conn(fetchrow=AsyncMock(return_value=None)))

    response = wired.client.get(f"/api/projects/{PROJECT_ID}/knowledge/nope")

    assert response.status_code == 404
    assert response.json()["detail"] == (
        f"Note 'nope' not found in project '{PROJECT_ID}'"
    )


def test_patch_of_a_missing_note_is_404_before_the_git_boundary():
    wired = _wire(conn=_conn(fetchrow=AsyncMock(return_value=None)))

    response = wired.client.patch(
        f"/api/projects/{PROJECT_ID}/knowledge/nope", json={"status": "resolved"}
    )

    assert response.status_code == 404
    assert response.json()["detail"] == (
        f"Note 'nope' not found in project '{PROJECT_ID}'"
    )


def test_unknown_projection_intent_is_404():
    store = SimpleNamespace(
        finish_knowledge_projection=AsyncMock(return_value=None),
    )
    wired = _wire(store=store)

    response = wired.client.post(
        f"/api/projects/{PROJECT_ID}/knowledge/materialize/intent-x/projection",
        json={"synced": True},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "canonical intent not found"


# =============================================================================
# Command-specific rulings preserved from main
# =============================================================================


def test_patch_refuses_an_archived_project_but_delete_does_not():
    wired = _wire(conn=_conn(fetchrow=AsyncMock(return_value=None)))

    wired.client.patch(
        f"/api/projects/{PROJECT_ID}/knowledge/n", json={"status": "resolved"}
    )
    wired.client.delete(f"/api/projects/{PROJECT_ID}/knowledge/n")

    patch_kwargs = wired.calls.member[0][1]
    delete_kwargs = wired.calls.member[1][1]
    assert patch_kwargs == {"min_role": "editor", "allow_archived": False}
    assert delete_kwargs == {"min_role": "editor"}


def test_patch_rejects_a_status_outside_the_vocabulary():
    wired = _wire()

    response = wired.client.patch(
        f"/api/projects/{PROJECT_ID}/knowledge/n", json={"status": "bogus"}
    )

    assert response.status_code == 400
    assert response.json()["detail"].startswith("Invalid status 'bogus'.")


def test_patch_with_nothing_to_change_is_a_no_op():
    wired = _wire()

    response = wired.client.patch(f"/api/projects/{PROJECT_ID}/knowledge/n", json={})

    assert response.status_code == 200
    assert response.json() == {"status": "no_changes"}
    wired.conn.fetchrow.assert_not_awaited()


def test_export_without_neo4j_is_503():
    wired = _wire(graph=SimpleNamespace(get=lambda: None))

    response = wired.client.post(f"/api/projects/{PROJECT_ID}/knowledge/export")

    assert response.status_code == 503
    assert response.json()["detail"] == (
        "Neo4j not available — cannot export knowledge base"
    )


def test_export_names_the_project_from_the_gate_result(tmp_path):
    graph = SimpleNamespace(
        get=lambda: SimpleNamespace(get_all_notes_for_export=lambda _pid: [])
    )
    wired = _wire(graph=graph)

    response = wired.client.post(f"/api/projects/{PROJECT_ID}/knowledge/export")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "exported"
    assert body["note_count"] == 0
    assert body["project_name"] == "Atlas"


# =============================================================================
# The knowledge_index port (R1.B03 lane D)
# =============================================================================


def _knowledge_index_module(monkeypatch, **members):
    """Stand in for lane D's ``services.knowledge_index`` at its frozen signature."""
    import sys
    import types

    module = types.ModuleType("orchestrator.services.knowledge_index")
    for name, value in members.items():
        setattr(module, name, value)
    monkeypatch.setitem(sys.modules, "orchestrator.services.knowledge_index", module)
    return module


def test_reindex_calls_the_index_port_with_the_nested_dependency_value(monkeypatch):
    reindex = AsyncMock(return_value={"status": "ok", "indexed": 3})
    _knowledge_index_module(monkeypatch, reindex_project_kb=reindex)
    wired = _wire()

    response = wired.client.post(
        f"/api/projects/{PROJECT_ID}/knowledge/reindex", params={"full": "true"}
    )

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "indexed": 3}
    assert reindex.await_args.args == (PROJECT_ID,)
    assert reindex.await_args.kwargs == {
        "force_full": True,
        "dependencies": wired.operations.knowledge_index,
    }


def test_reindex_defaults_to_the_incremental_run(monkeypatch):
    reindex = AsyncMock(return_value={})
    _knowledge_index_module(monkeypatch, reindex_project_kb=reindex)
    wired = _wire()

    wired.client.post(f"/api/projects/{PROJECT_ID}/knowledge/reindex")

    assert reindex.await_args.kwargs["force_full"] is False


def test_a_failed_reindex_is_a_500_carrying_the_reason(monkeypatch):
    _knowledge_index_module(
        monkeypatch,
        reindex_project_kb=AsyncMock(side_effect=RuntimeError("vault unreachable")),
    )
    wired = _wire()

    response = wired.client.post(f"/api/projects/{PROJECT_ID}/knowledge/reindex")

    assert response.status_code == 500
    assert response.json()["detail"] == "vault unreachable"


def test_materialize_without_an_indexer_still_commits_the_note(monkeypatch):
    """No system embedding is a valid degraded deployment, not a failure."""
    _knowledge_index_module(
        monkeypatch, build_kb_embedding_service=AsyncMock(return_value=None)
    )
    materialize = AsyncMock(return_value={"status": "committed", "indexed": False})
    monkeypatch.setattr(
        "orchestrator.services.kb_materialize.materialize_knowledge_note", materialize
    )
    wired = _wire()

    response = wired.client.post(
        f"/api/projects/{PROJECT_ID}/knowledge/materialize",
        json={"slug": "adr-7", "content": "# note"},
    )

    assert response.status_code == 200
    assert response.json() == {"status": "committed", "indexed": False}
    assert materialize.await_args.kwargs["store"] is None
    assert materialize.await_args.kwargs["embedding_service"] is None
    assert wired.operations.logger.info.called


def test_materialize_degrades_when_the_indexer_cannot_be_built(monkeypatch):
    _knowledge_index_module(
        monkeypatch,
        build_kb_embedding_service=AsyncMock(side_effect=RuntimeError("no key")),
    )
    materialize = AsyncMock(return_value={"status": "committed"})
    monkeypatch.setattr(
        "orchestrator.services.kb_materialize.materialize_knowledge_note", materialize
    )
    wired = _wire()

    response = wired.client.post(
        f"/api/projects/{PROJECT_ID}/knowledge/materialize",
        json={"slug": "adr-7", "content": "# note"},
    )

    assert response.status_code == 200
    assert materialize.await_args.kwargs["store"] is None
    assert wired.operations.logger.warning.called


def test_materialize_forwards_the_whole_request_body(monkeypatch):
    _knowledge_index_module(
        monkeypatch, build_kb_embedding_service=AsyncMock(return_value=None)
    )
    materialize = AsyncMock(return_value={"status": "committed"})
    monkeypatch.setattr(
        "orchestrator.services.kb_materialize.materialize_knowledge_note", materialize
    )
    wired = _wire()

    wired.client.post(
        f"/api/projects/{PROJECT_ID}/knowledge/materialize",
        json={
            "slug": "adr-7",
            "content": "# note",
            "job_id": "job-1",
            "expected_blob_sha": "deadbeef",
            "retrieval_messages": ["why adr-7"],
        },
    )

    kwargs = materialize.await_args.kwargs
    assert kwargs["project_id"] == PROJECT_ID
    assert kwargs["slug"] == "adr-7"
    assert kwargs["content"] == "# note"
    assert kwargs["job_id"] == "job-1"
    assert kwargs["expected_blob_sha"] == "deadbeef"
    assert kwargs["retrieval_messages"] == ["why adr-7"]


# =============================================================================
# Query-parameter bounds
# =============================================================================


@pytest.mark.parametrize("limit", [0, 201])
def test_list_notes_rejects_an_out_of_range_limit(limit):
    wired = _wire()

    response = wired.client.get(
        f"/api/projects/{PROJECT_ID}/knowledge", params={"limit": limit}
    )

    assert response.status_code == 422


def test_list_notes_rejects_a_negative_offset():
    wired = _wire()

    response = wired.client.get(
        f"/api/projects/{PROJECT_ID}/knowledge", params={"offset": -1}
    )

    assert response.status_code == 422


def test_list_notes_filters_use_the_type_alias():
    conn = _conn(
        fetch=AsyncMock(return_value=[]),
        fetchrow=AsyncMock(return_value={"cnt": 0}),
    )
    wired = _wire(conn=conn)

    response = wired.client.get(
        f"/api/projects/{PROJECT_ID}/knowledge",
        params={"type": "decision", "status": "active", "tag": "kb", "limit": 5},
    )

    assert response.status_code == 200
    assert response.json() == {"notes": [], "total": 0, "limit": 5, "offset": 0}
    count_sql = conn.fetchrow.await_args.args[0]
    assert "note_type = $2" in count_sql
    assert "status = $3" in count_sql
    assert "$4 = ANY(tags)" in count_sql


# =============================================================================
# Role tiers (G1/G1b, D9): mutations need editor; dispatch tags and the
# charter need the project owner or an admin
# =============================================================================


@pytest.mark.parametrize(
    "method,path",
    [
        ("post", f"/api/projects/{PROJECT_ID}/knowledge/reindex"),
        ("post", f"/api/projects/{PROJECT_ID}/knowledge/export"),
        ("patch", f"/api/projects/{PROJECT_ID}/knowledge/note-1"),
        ("delete", f"/api/projects/{PROJECT_ID}/knowledge/note-1"),
    ],
)
def test_every_knowledge_mutation_asks_the_gate_for_editor(method, path):
    wired = _wire(conn=_conn(fetchrow=AsyncMock(return_value=None)))
    kwargs = {"json": {"status": "resolved"}} if method == "patch" else {}

    getattr(wired.client, method)(path, **kwargs)

    assert wired.calls.member[0][1]["min_role"] == "editor"


@pytest.mark.parametrize(
    "method,path",
    [
        ("get", f"/api/projects/{PROJECT_ID}/knowledge/summary"),
        ("get", f"/api/projects/{PROJECT_ID}/knowledge"),
        ("get", f"/api/projects/{PROJECT_ID}/knowledge/note-1"),
        ("post", f"/api/projects/{PROJECT_ID}/knowledge/search"),
    ],
)
def test_reads_stay_open_to_viewers(method, path):
    wired = _wire()
    kwargs = {"json": {"query": "x"}} if method == "post" else {}

    getattr(wired.client, method)(path, **kwargs)

    assert "min_role" not in wired.calls.member[0][1]


def _role_store(role):
    return SimpleNamespace(
        finish_knowledge_projection=AsyncMock(return_value={"synced": True}),
        get_user_role_in_project=AsyncMock(return_value=role),
    )


@pytest.mark.parametrize("tags", [["ready"], ["Parallel-Safe"]])
@pytest.mark.parametrize("field", ["add_tags", "remove_tags"])
def test_an_editor_cannot_touch_a_dispatch_tag(tags, field):
    conn = _conn(
        fetchrow=AsyncMock(return_value={"note_id": "n", "note_type": "issue"})
    )
    wired = _wire(conn=conn, store=_role_store("editor"))

    response = wired.client.patch(
        f"/api/projects/{PROJECT_ID}/knowledge/n", json={field: tags}
    )

    assert response.status_code == 403
    assert "project owner" in response.json()["detail"]
    conn.fetchrow.assert_not_awaited()


def test_an_editor_keeps_ordinary_tags_and_status():
    conn = _conn(fetchrow=AsyncMock(return_value=None))
    store = _role_store("editor")
    wired = _wire(conn=conn, store=store)

    response = wired.client.patch(
        f"/api/projects/{PROJECT_ID}/knowledge/n",
        json={"status": "resolved", "add_tags": ["subject/notes"]},
    )

    # Past the authority checks: the missing note is the next answer.
    assert response.status_code == 404
    store.get_user_role_in_project.assert_not_awaited()


def test_the_owner_may_arm_ready():
    conn = _conn(fetchrow=AsyncMock(return_value=None))
    wired = _wire(conn=conn, store=_role_store("owner"))

    response = wired.client.patch(
        f"/api/projects/{PROJECT_ID}/knowledge/n", json={"add_tags": ["ready"]}
    )

    assert response.status_code == 404
    conn.fetchrow.assert_awaited_once()


def test_an_admin_may_arm_ready_without_a_membership_role():
    async def admin_gate(request, _store, project_id, **kwargs):
        return ({"id": "admin", "is_admin": True}, PROJECT)

    store = _role_store(None)
    wired = _wire(
        conn=_conn(fetchrow=AsyncMock(return_value=None)),
        member_gate=admin_gate,
        store=store,
    )

    response = wired.client.patch(
        f"/api/projects/{PROJECT_ID}/knowledge/n", json={"add_tags": ["ready"]}
    )

    assert response.status_code == 404
    store.get_user_role_in_project.assert_not_awaited()


def test_an_editor_cannot_change_the_charter():
    conn = _conn(
        fetchrow=AsyncMock(return_value={"note_id": "charter", "note_type": "charter"})
    )
    wired = _wire(conn=conn, store=_role_store("editor"))

    response = wired.client.patch(
        f"/api/projects/{PROJECT_ID}/knowledge/charter", json={"status": "resolved"}
    )

    assert response.status_code == 403
    assert (
        response.json()["detail"]
        == "Only the project owner can change the charter note"
    )


def test_the_owner_passes_the_charter_check():
    conn = _conn(
        fetchrow=AsyncMock(return_value={"note_id": "charter", "note_type": "charter"})
    )
    wired = _wire(conn=conn, store=_role_store("owner"))

    with patch(
        "orchestrator.services.kb_materialize.materialize_knowledge_metadata_update",
        AsyncMock(return_value={"canonical_state": "failed", "reason": "stub"}),
    ) as materialize:
        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}/knowledge/charter", json={"status": "resolved"}
        )

    # Through the authority checks to the git boundary (stubbed to fail).
    materialize.assert_awaited_once()
    assert response.status_code == 409
