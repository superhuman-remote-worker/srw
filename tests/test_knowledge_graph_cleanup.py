"""A deleted project's knowledge leaves the Neo4j graph.

Nothing cascades from PostgreSQL into Neo4j, and nothing removed the graph
side of a project delete: an agent-written note, its tags and its keywords
outlived their project (dev, 11-06: 82 notes of 12 deleted projects). The
project delete now removes them once the row is gone, and a leader-gated
sweep removes what that could not.

The graph tests run ``neo4j:5-community`` and the sweep's table reads run a
real PostgreSQL; each skips without a container runtime.
"""

from __future__ import annotations

import asyncio
import ast
import inspect
import logging
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from orchestrator.application import background_tasks
from orchestrator.services import knowledge_graph_cleanup as cleanup
from orchestrator.services import projects
from shared.runtime.database import neo4j_db
from shared.runtime.services.knowledge_graph import KnowledgeGraphDB

_PASSWORD = "kb-graph-cleanup"


# =============================================================================
# Real Neo4j + PostgreSQL
# =============================================================================


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
    graph._db.execute_write("MATCH (n) DETACH DELETE n")
    try:
        yield graph
    finally:
        graph.close()


@pytest.fixture(scope="module")
def pg_dsn():
    pytest.importorskip("testcontainers.postgres")
    from testcontainers.postgres import PostgresContainer

    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:  # noqa: BLE001 - any runtime failure means skip
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture
async def projects_table(pg_dsn):
    """A pool whose ``projects`` table holds the ids a test adds."""
    asyncpg = pytest.importorskip("asyncpg")
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS projects")
        await conn.execute("CREATE TABLE projects (id uuid PRIMARY KEY)")
    try:
        yield pool
    finally:
        await pool.close()


def _write(kg, project_id, title, *, tags=(), keywords=()):
    return kg.create_note(
        project_id=project_id,
        title=title,
        note_type="learning",
        content=f"{title}.",
        tags=list(tags),
        keywords=list(keywords),
    )


def _nodes(kg, project_id):
    rows = kg._db.execute_query(
        "MATCH (n {project_id: $pid}) RETURN labels(n)[0] AS label, count(*) AS c",
        {"pid": project_id},
    )
    return {row["label"]: row["c"] for row in rows}


def _handle(kg):
    return SimpleNamespace(get=lambda: kg)


def test_a_project_delete_removes_its_notes_tags_and_keywords_only(kg):
    gone, kept = str(uuid.uuid4()), str(uuid.uuid4())
    _write(kg, gone, "First", tags=["a", "b"], keywords=["k"])
    _write(kg, gone, "Second", tags=["a"])
    _write(kg, kept, "Other", tags=["a"], keywords=["k"])

    # 2 notes + 2 tags + 1 keyword. The old query answered 1 here.
    assert kg.delete_project_knowledge(gone) == 5
    assert _nodes(kg, gone) == {}
    assert _nodes(kg, kept) == {"Note": 1, "Tag": 1, "Keyword": 1}


def test_a_large_project_goes_in_batches(kg):
    project = str(uuid.uuid4())
    for index in range(5):
        _write(kg, project, f"Note {index}", tags=[f"t{index}"])

    assert kg.delete_project_knowledge(project, batch_size=3) == 10
    assert _nodes(kg, project) == {}


def test_the_graph_lists_the_projects_it_holds(kg):
    first, second = str(uuid.uuid4()), str(uuid.uuid4())
    _write(kg, first, "One")
    kg._db.execute_write(
        "CREATE (:Tag {name: 'left-behind', project_id: $pid})", {"pid": second}
    )

    assert sorted(kg.knowledge_project_ids()) == sorted([first, second])


@pytest.mark.asyncio
async def test_the_sweep_removes_only_projects_the_table_no_longer_has(
    kg, projects_table
):
    live, deleted, tags_only = (str(uuid.uuid4()) for _ in range(3))
    _write(kg, live, "Live note", tags=["x"])
    _write(kg, deleted, "Orphan note", tags=["x"], keywords=["y"])
    # A project whose notes went one by one leaves its tags behind.
    kg._db.execute_write(
        "CREATE (:Tag {name: 'x', project_id: $pid})", {"pid": tags_only}
    )
    kg._db.execute_write("CREATE (:Tag {name: 'x', project_id: 'proj-legacy'})")
    async with projects_table.acquire() as conn:
        await conn.execute("INSERT INTO projects VALUES ($1)", uuid.UUID(live))

    swept = await cleanup.sweep_orphaned_graph_knowledge(
        store=projects_table, graph=_handle(kg)
    )

    assert swept == 2
    assert _nodes(kg, deleted) == {} and _nodes(kg, tags_only) == {}
    assert _nodes(kg, live) == {"Note": 1, "Tag": 1}
    # Not a project id, so not the sweep's to judge.
    assert _nodes(kg, "proj-legacy") == {"Tag": 1}


@pytest.mark.asyncio
async def test_an_empty_projects_table_sweeps_nothing(kg, projects_table, caplog):
    project = str(uuid.uuid4())
    _write(kg, project, "Kept")

    with caplog.at_level(logging.WARNING, logger=cleanup.__name__):
        swept = await cleanup.sweep_orphaned_graph_knowledge(
            store=projects_table, graph=_handle(kg)
        )

    assert swept == 0
    assert _nodes(kg, project) == {"Note": 1}
    assert "projects table is empty" in caplog.text


# =============================================================================
# The sweep's order, the loop, and the delete hook
# =============================================================================


class _RecordingGraph:
    def __init__(self, project_ids, events):
        self._ids = project_ids
        self._events = events
        self.deleted: list[str] = []

    def knowledge_project_ids(self):
        self._events.append("graph")
        return list(self._ids)

    def delete_project_knowledge(self, project_id):
        self.deleted.append(project_id)
        return 1


def _table(project_ids, events):
    conn = SimpleNamespace(
        fetchval=AsyncMock(return_value=True),
        fetch=AsyncMock(return_value=[{"id": uuid.UUID(p)} for p in project_ids]),
    )

    @asynccontextmanager
    async def acquire():
        events.append("table")
        yield conn

    return SimpleNamespace(acquire=acquire)


@pytest.mark.asyncio
async def test_the_graph_is_read_before_the_projects_table():
    """A project created between the two reads is in the later one."""
    events: list[str] = []
    project = str(uuid.uuid4())
    graph = _RecordingGraph([project], events)

    await cleanup.sweep_orphaned_graph_knowledge(
        store=_table([project], events), graph=_handle(graph)
    )

    assert events == ["graph", "table"]
    assert graph.deleted == []


@pytest.mark.asyncio
async def test_without_a_graph_the_sweep_reads_nothing():
    store = SimpleNamespace(acquire=MagicMock())
    swept = await cleanup.sweep_orphaned_graph_knowledge(
        store=store, graph=SimpleNamespace(get=lambda: None)
    )
    assert swept == 0
    store.acquire.assert_not_called()


@pytest.mark.asyncio
async def test_the_loop_sweeps_at_once_survives_a_failure_and_stops_on_shutdown(
    monkeypatch,
):
    shutdown = asyncio.Event()
    passes: list[int] = []

    async def sweep(*, store, graph):
        passes.append(1)
        if len(passes) == 1:
            raise RuntimeError("neo4j down")
        shutdown.set()
        return 0

    monkeypatch.setattr(cleanup, "sweep_orphaned_graph_knowledge", sweep)

    await asyncio.wait_for(
        cleanup.orphaned_graph_knowledge_sweep_loop(
            shutdown, store=object(), graph=object(), interval_seconds=0.01
        ),
        timeout=5,
    )
    assert len(passes) == 2


@pytest.mark.asyncio
async def test_a_graph_failure_never_fails_the_project_delete(caplog):
    graph = MagicMock()
    graph.delete_project_knowledge.side_effect = RuntimeError("bolt refused")

    with caplog.at_level(logging.WARNING, logger=cleanup.__name__):
        result = await cleanup.delete_project_graph_knowledge("p", graph=_handle(graph))

    assert result is None
    assert "orphan sweep removes them" in caplog.text


def test_the_sweep_is_leader_gated_and_shut_down_in_order():
    assert "knowledge_graph_orphan_sweep" in (
        background_tasks.BACKGROUND_TASK_SHUTDOWN_ORDER
    )
    source = inspect.getsource(background_tasks.start_background_tasks)
    gated = [
        node.args[0].value
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == "tasks.start_leader_gated"
        and isinstance(node.args[0], ast.Constant)
    ]
    assert "knowledge_graph_orphan_sweep" in gated


# =============================================================================
# projects.delete_project
# =============================================================================

PROJECT_ID = "44444444-4444-4444-8444-444444444444"


def _project_dependencies(store, graph):
    @asynccontextmanager
    async def acquire():
        yield SimpleNamespace(execute=AsyncMock())

    return projects.ProjectDependencies(
        store=store,
        vector_db=SimpleNamespace(acquire=acquire),
        forge=SimpleNamespace(is_initialized=False),
        keycloak_groups=SimpleNamespace(is_initialized=False),
        main_cloud_router=SimpleNamespace(
            for_project_optional=MagicMock(return_value=None)
        ),
        logger=MagicMock(),
        provisioning=MagicMock(),
        graph=graph,
        with_validated_tool_overrides=lambda value: value,
    )


def _project_store(delete):
    @asynccontextmanager
    async def acquire():
        yield SimpleNamespace(execute=AsyncMock())

    return SimpleNamespace(
        get_project_repositories=AsyncMock(return_value=[]),
        acquire=acquire,
        delete_project=delete,
    )


@pytest.mark.asyncio
async def test_a_project_delete_removes_its_graph_knowledge_after_the_row(
    monkeypatch,
):
    monkeypatch.setattr(
        projects.folder_grants, "revoke_folder_grants", AsyncMock(return_value=0)
    )
    order: list[str] = []

    async def delete(_project_id):
        order.append("row")
        return True

    graph = MagicMock()
    graph.delete_project_knowledge.side_effect = lambda pid: order.append(
        f"graph:{pid}"
    )

    await projects.delete_project(
        PROJECT_ID,
        project={"id": PROJECT_ID, "is_default": False},
        dependencies=_project_dependencies(_project_store(delete), _handle(graph)),
    )

    assert order == ["row", f"graph:{PROJECT_ID}"]


@pytest.mark.asyncio
async def test_a_refused_project_delete_leaves_the_graph_alone(monkeypatch):
    from fastapi import HTTPException

    monkeypatch.setattr(
        projects.folder_grants, "revoke_folder_grants", AsyncMock(return_value=0)
    )

    async def refuse(_project_id):
        raise HTTPException(status_code=409, detail="unfinished work")

    graph = MagicMock()

    with pytest.raises(HTTPException):
        await projects.delete_project(
            PROJECT_ID,
            project={"id": PROJECT_ID, "is_default": False},
            dependencies=_project_dependencies(_project_store(refuse), _handle(graph)),
        )

    graph.delete_project_knowledge.assert_not_called()
