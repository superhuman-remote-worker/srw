"""Links to notes not written yet, on the graph tier, against a real Neo4j.

An edge needs both notes, so a forward reference used to reach the file alone:
``kb_update`` re-rendered the file from the graph's edges and dropped it (dev,
11-04), and it never became an edge even once its target was written (dev,
11-06). The graph now keeps such a link pending on its source and makes the
edge when the target is created; an update hands the graph every link of the
file it lacks, which also heals notes written before pending links existed.

The stored row an update reads is made the way the reindexer makes it: from
the file the write sent, through ``note_fields``. Runs ``neo4j:5-community``;
skips without a container runtime or the neo4j driver.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.tools.knowledge.knowledge_tools import create_kb_tools
from orchestrator.services.kb_reindex import note_fields
from shared.runtime.database import neo4j_db
from shared.runtime.knowledge.gardener import parse_note_md
from shared.runtime.services.knowledge_graph import KnowledgeGraphDB

_PASSWORD = "kb-forward-links"


@pytest.fixture(scope="module")
def neo4j_server():
    pytest.importorskip("testcontainers.community.neo4j")
    if neo4j_db.GraphDatabase is None:
        pytest.skip("the neo4j driver is not installed")
    from testcontainers.community.neo4j import Neo4jContainer

    container = Neo4jContainer("neo4j:5-community", password=_PASSWORD)
    container.with_env("NEO4J_server_memory_heap_max__size", "512m")
    try:
        container.start()
    except Exception as exc:  # noqa: BLE001 - any runtime failure means skip
        pytest.skip(f"no container runtime for testcontainers: {exc}")
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture
def kg(neo4j_server):
    graph = KnowledgeGraphDB(
        uri=neo4j_server.get_connection_url(), username="neo4j", password=_PASSWORD
    )
    assert graph.connect()
    try:
        yield graph
    finally:
        graph.close()


def _tools(kg, project_id):
    ctx = MagicMock()
    ctx.project_id = project_id
    ctx.project_ids = [project_id]
    ctx.job_id = str(uuid.uuid4())
    ctx.config = {"current_phase": 2}
    ctx.knowledge_graph = kg
    ctx.knowledge_store = AsyncMock()
    ctx.knowledge_store.get_note_by_slug.return_value = None
    with patch("agent.tools.knowledge.knowledge_tools.asyncio") as mock_asyncio:
        mock_asyncio.get_running_loop.side_effect = RuntimeError("no loop")
        tools = {t.name: t for t in create_kb_tools(ctx)}
    return tools, ctx


def _call(tool, args):
    """Run a tool with the vault POST captured; return (result, files sent)."""
    sent: list = []

    def _post(project_id, slug, content, job_id, retrieval_messages=None, **_kw):
        sent.append(content)
        return {"status": "committed", "path": f"knowledge/{slug}.md"}

    with patch(
        "agent.tools.knowledge.knowledge_tools._post_vault_file", side_effect=_post
    ):
        result = tool.invoke(args)
    return result, sent


def _indexed_row(slug, file_content):
    """What ``get_note_by_slug`` returns once the reindexer read this file."""
    fm, body = parse_note_md(file_content)
    fields = note_fields(f"knowledge/{slug}.md", fm, body)
    return {
        "id": fields["note_id"],
        "title": fields["title"],
        "type": fields["note_type"],
        "status": fields["status"],
        "content": body,
        "description": fields.get("description"),
        "confidence": None,
        "tags": [],
        "keywords": [],
        "job_id": None,
        "phase": None,
        "created": None,
        "modified": None,
        "blob_sha": "0" * 40,
    }


def _edges(kg, project_id, slug):
    rows = kg._db.execute_query(
        "MATCH (n:Note {project_id: $pid, id: $id})-[r]->(m:Note) "
        "RETURN type(r) AS type, m.id AS target",
        {"pid": project_id, "id": slug},
    )
    return {(r["type"], r["target"]) for r in rows}


def test_a_forward_link_survives_an_append_on_the_graph_tier(kg):
    project_id = str(uuid.uuid4())
    tools, ctx = _tools(kg, project_id)

    result, _ = _call(
        tools["kb_write"],
        {"title": "In The Graph", "type": "learning", "content": "Exists."},
    )
    assert result.startswith("Created"), result

    result, sent = _call(
        tools["kb_write"],
        {
            "title": "Auth",
            "type": "decision",
            "content": "We use JWT.",
            "links": [
                {"target": "not-written-yet", "type": "REFERENCES"},
                {"target": "in-the-graph", "type": "SUPPORTS"},
            ],
        },
    )
    assert result.startswith("Created"), result
    written = sent[-1]
    assert "[not-written-yet](not-written-yet.md)" in written
    # The premise: the real graph holds the edge to the note that exists and
    # never wrote the forward reference.
    assert _edges(kg, project_id, "auth") == {("SUPPORTS", "in-the-graph")}

    ctx.knowledge_store.get_note_by_slug.return_value = _indexed_row("auth", written)
    result, sent = _call(
        tools["kb_update"],
        {
            "note": "auth",
            "append": "Rotate keys.",
            "add_links": [{"target": "in-the-graph", "type": "SUPPORTS"}],
        },
    )
    assert result.startswith("Updated **"), result

    _, body = parse_note_md(sent[-1])
    assert body.count("## Relationships") == 1
    assert "**REFERENCES:** [not-written-yet](not-written-yet.md)" in body
    assert body.count("[in-the-graph](in-the-graph.md)") == 1
    assert body.index("Rotate keys.") < body.index("## Relationships")


def _pending(kg, project_id, slug):
    return kg.read_note(project_id, slug)["pending_relationships"]


def test_a_forward_link_becomes_an_edge_once_its_target_is_written(kg):
    project_id = str(uuid.uuid4())
    tools, _ = _tools(kg, project_id)

    result, _ = _call(
        tools["kb_write"],
        {
            "title": "Auth",
            "type": "decision",
            "content": "We use JWT.",
            "links": [{"target": "token-rotation", "type": "REFERENCES"}],
        },
    )
    assert result.startswith("Created"), result
    assert _edges(kg, project_id, "auth") == set()
    assert _pending(kg, project_id, "auth") == [
        {"type": "REFERENCES", "target": "token-rotation"}
    ]
    # The reader still sees the link while its target is missing.
    assert "- **REFERENCES** → [[token-rotation]]" in tools["kb_read"].invoke(
        {"note": "auth"}
    )

    result, _ = _call(
        tools["kb_write"],
        {"title": "Token Rotation", "type": "learning", "content": "Monthly."},
    )
    assert result.startswith("Created"), result
    assert _edges(kg, project_id, "auth") == {("REFERENCES", "token-rotation")}
    assert _pending(kg, project_id, "auth") == []
    read = tools["kb_read"].invoke({"note": "token-rotation"})
    assert "[[auth]] (Auth) **REFERENCES** → this" in read


def test_an_update_makes_the_edge_an_older_write_never_made(kg):
    project_id = str(uuid.uuid4())
    tools, ctx = _tools(kg, project_id)

    _, sent = _call(
        tools["kb_write"],
        {
            "title": "Old Note",
            "type": "decision",
            "content": "Written before pending links.",
            "links": [{"target": "its-target", "type": "DEPENDS_ON"}],
        },
    )
    written = sent[-1]
    # What a graph written before this change holds: no edge, nothing pending.
    kg._db.execute_write(
        "MATCH (a:Note {project_id: $pid, id: 'old-note'}) REMOVE a.pending_links",
        {"pid": project_id},
    )
    _call(
        tools["kb_write"],
        {"title": "Its Target", "type": "learning", "content": "Exists now."},
    )
    assert _edges(kg, project_id, "old-note") == set()

    ctx.knowledge_store.get_note_by_slug.return_value = _indexed_row(
        "old-note", written
    )
    result, _ = _call(tools["kb_update"], {"note": "old-note", "append": "Touched."})
    assert result.startswith("Updated **"), result
    assert _edges(kg, project_id, "old-note") == {("DEPENDS_ON", "its-target")}
