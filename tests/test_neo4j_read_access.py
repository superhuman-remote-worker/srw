"""Neo4j reads run in read-access sessions; read-only links read only.

The graph tools' keyword filter is a friendly early message, not the
enforcement: ``WITH 1 AS one\\nCREATE ...`` or ``SET\\t`` slip past it. The
server refuses every write in a ``READ_ACCESS`` session
(``Neo.ClientError.Statement.AccessMode``), so ``cypher_query`` and the schema
read always use one, and a connection made for a read-only connector link
opens nothing else.

The unit tests record the session arguments; the container test runs the
real bind -> process_datasources -> create_neo4j_tools path against
``neo4j:5-community`` with auth on, and skips without a container runtime.
"""

from __future__ import annotations

import logging

import pytest

from shared.runtime.database import neo4j_db
from shared.runtime.database.neo4j_db import READ_ACCESS, Neo4jDB

# Statements that got past the keyword filter in review.
WRITES_PAST_THE_FILTER = [
    "WITH 1 AS one\nCREATE (n:Pwned {v: 1}) RETURN n",
    "MATCH (n) SET\tn.x=1",
    "MATCH (n) DELETE(n)",
]


class _Session:
    def __init__(self, log, kwargs):
        self.log, self.kwargs = log, kwargs

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def run(self, query, parameters):
        self.log.append((self.kwargs.get("default_access_mode"), query))
        return []


class _Driver:
    def __init__(self):
        self.log = []

    def session(self, **kwargs):
        return _Session(self.log, kwargs)


def _db(*, read_only: bool) -> tuple[Neo4jDB, _Driver]:
    db = Neo4jDB("bolt://graph:7687", "neo4j", "pw", read_only=read_only)
    db.driver = _Driver()
    return db, db.driver


class TestSessionAccessMode:
    def test_reads_use_read_access_on_any_connection(self):
        db, driver = _db(read_only=False)
        db.execute_read("MATCH (n) RETURN n")
        db.get_schema()
        assert {mode for mode, _ in driver.log} == {READ_ACCESS}

    def test_a_writable_connection_keeps_its_default_session_for_queries(self):
        db, driver = _db(read_only=False)
        db.execute_query("CREATE (n)")
        assert driver.log == [(None, "CREATE (n)")]

    def test_a_read_only_connection_opens_only_read_sessions(self):
        db, driver = _db(read_only=True)
        db.execute_query("CREATE (n)")
        db.execute_read("MATCH (n) RETURN n")
        assert {mode for mode, _ in driver.log} == {READ_ACCESS}

    def test_a_read_only_connection_refuses_write_transactions(self):
        db, _ = _db(read_only=True)
        with pytest.raises(RuntimeError, match="read-only"):
            db.execute_write("CREATE (n)")


@pytest.mark.parametrize("read_only", [False, True])
def test_the_agent_opens_read_only_links_read_only(read_only, monkeypatch):
    from agent.core.datasource_setup import create_datasource_connection

    monkeypatch.setattr(Neo4jDB, "connect", lambda self: True)
    db, _ = create_datasource_connection(
        {
            "type": "neo4j",
            "connection_url": "bolt://graph:7687",
            "credentials": {"username": "graph", "password": "pw"},
            "project_read_only": read_only,
        }
    )
    assert db.read_only is read_only


def test_cypher_query_reads_through_a_read_access_session():
    from agent.tools.context import ToolContext
    from agent.tools.graph.neo4j import create_neo4j_tools

    db, driver = _db(read_only=False)
    tools = {
        t.name: t for t in create_neo4j_tools(ToolContext(datasources={"neo4j": db}))
    }
    tools["cypher_query"].invoke({"query": "MATCH (n) RETURN count(n) AS c"})
    assert driver.log == [(READ_ACCESS, "MATCH (n) RETURN count(n) AS c")]


# =============================================================================
# Against a real server
# =============================================================================


@pytest.fixture(scope="module")
def neo4j_server():
    pytest.importorskip("testcontainers.community.neo4j")
    if neo4j_db.GraphDatabase is None:
        pytest.skip("the neo4j driver is not installed")
    from testcontainers.community.neo4j import Neo4jContainer

    container = Neo4jContainer("neo4j:5-community", password="d1a-read-access")
    container.with_env("NEO4J_server_memory_heap_max__size", "512m")
    try:
        container.start()
    except Exception as exc:  # noqa: BLE001 - any runtime failure means skip
        pytest.skip(f"no container runtime for testcontainers: {exc}")
    try:
        yield container
    finally:
        container.stop()


def _tools_for(server, *, read_only: bool):
    """The real path: the orchestrator's bind, then the agent's setup."""
    from agent.core.datasource_setup import process_datasources
    from agent.tools.context import ToolContext
    from agent.tools.graph.neo4j import create_neo4j_tools
    from orchestrator.services.agent_datasource_payload import (
        DatasourcePayloadDependencies,
        build_datasources_payload,
    )
    from orchestrator.services.connector_drivers import builtin_connector_drivers

    deps = DatasourcePayloadDependencies(
        logger=logging.getLogger("test"),
        mcp_datasources_enabled=lambda: False,
        mcp_stdio_enabled=lambda: False,
        connector_drivers=builtin_connector_drivers(),
        workspace_ssh_known_hosts=lambda: "",
    )
    row = {
        "id": "00000000-0000-0000-0000-0000000000d1",
        "type": "neo4j",
        "name": "Supply Graph",
        "description": None,
        "connection_url": server.get_connection_url(),
        "credentials": {"username": "neo4j", "password": server.password},
        "project_read_only": read_only,
    }
    (entry,) = build_datasources_payload([row], dependencies=deps)
    connections, _, _ = process_datasources([entry])
    db = connections["neo4j"]
    tools = create_neo4j_tools(ToolContext(datasources={"neo4j": db}))
    return db, {t.name: t for t in tools}


def _node_count(server) -> int:
    with server.get_driver() as driver, driver.session() as session:
        return session.run("MATCH (n) RETURN count(n) AS c").single()["c"]


@pytest.mark.parametrize("statement", WRITES_PAST_THE_FILTER)
def test_a_read_only_link_cannot_write_through_any_tool(neo4j_server, statement):
    with neo4j_server.get_driver() as driver, driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()
        session.run("CREATE (:Seed {x: 0})").consume()
    db, tools = _tools_for(neo4j_server, read_only=True)
    try:
        for tool_name, argument in (
            ("cypher_query", "query"),
            ("cypher_execute", "statement"),
        ):
            answer = tools[tool_name].invoke({argument: statement})
            assert answer.startswith("Error"), (tool_name, answer)
        with neo4j_server.get_driver() as driver, driver.session() as session:
            seed = session.run("MATCH (n:Seed) RETURN n.x AS x").single()
            pwned = session.run("MATCH (n:Pwned) RETURN count(n) AS c").single()
        assert seed is not None and seed["x"] == 0
        assert pwned["c"] == 0
        # Reads still work.
        assert "Record 1" in tools["cypher_query"].invoke(
            {"query": "MATCH (n) RETURN count(n) AS c"}
        )
        assert "Seed" in tools["get_database_schema"].invoke({})
    finally:
        db.close()


def test_cypher_query_cannot_write_on_a_writable_link_either(neo4j_server):
    with neo4j_server.get_driver() as driver, driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()
    db, tools = _tools_for(neo4j_server, read_only=False)
    try:
        refused = tools["cypher_query"].invoke(
            {"query": "WITH 1 AS one\nCREATE (n:Pwned {v: 1}) RETURN n"}
        )
        assert "AccessMode" in refused or "Writing in read access mode" in refused
        assert _node_count(neo4j_server) == 0
        # The write tool still writes on a writable link.
        tools["cypher_execute"].invoke({"statement": "CREATE (:Written)"})
        assert _node_count(neo4j_server) == 1
    finally:
        db.close()
