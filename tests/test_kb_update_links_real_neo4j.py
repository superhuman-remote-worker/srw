"""A kb_update on the graph tier keeps a link whose target note is not written yet.

Against a real ``neo4j:5-community``: ``create_note`` MATCHes a link's target
before it writes the edge, so a forward reference never reaches the graph and
lives in the note's file alone. ``kb_update`` re-renders the whole file, and
built its links block from the graph's edges, so the first update dropped the
link (dev, 11-04). The stored row the update reads is made the way the
reindexer makes it: from the file the write sent, through ``note_fields``.

Skips without a container runtime or the neo4j driver.
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
