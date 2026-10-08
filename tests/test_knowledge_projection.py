"""Connector knowledge projection: note content, and degradation without Neo4j.

The note is what an agent retrieves when it asks "what can I connect to?", so
its content is per-type by design: a repository names the clone path, a
read-write managed connector names the CLI and its env vars, a read-only one
names the tools instead, and a KB says it is read-only and centrally indexed.

The projection has two legs and they fail independently. Neo4j is optional —
absent, the pgvector row is still written and no caller sees an error. Only a
``strict=True`` caller (the lifespan reconciler) is told, and it is told which
legs failed.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services import knowledge_projection as subject


PROJECT_ID = "00000000-0000-0000-0000-0000000000b1"
DS_ID = "1234abcd-0000-0000-0000-000000000001"


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


def _pool(conn):
    return SimpleNamespace(acquire=lambda: _Acquire(conn))


def _deps(*, conn=None, graph=None, logger=None):
    connection = conn if conn is not None else SimpleNamespace(execute=AsyncMock())
    return subject.KnowledgeProjectionDependencies(
        store=_pool(connection),
        logger=logger or MagicMock(),
        graph=graph if graph is not None else SimpleNamespace(get=lambda: None),
    )


def _graph(execute_write=None, **over):
    inner = SimpleNamespace(execute_write=execute_write or MagicMock())
    kg = SimpleNamespace(_db=inner, **over)
    return SimpleNamespace(get=lambda: kg), kg


# =============================================================================
# Note content per datasource type
# =============================================================================


def test_generic_connector_note_lists_env_var_names_but_no_values():
    ds = {
        "type": "generic",
        "name": "Production DB",
        "description": "The prod store",
        "connection_url": "postgres://host/db",
        "cli_hint": "psql $PROD_URL",
        "credentials": {"env_vars": {"PROD_PASSWORD": "hunter2"}},
    }

    content = subject.build_datasource_note_content(ds)

    assert "## Connector: Production DB" in content
    assert "The prod store" in content
    assert "postgres://host/db" in content
    assert "`psql $PROD_URL`" in content
    assert "`PROD_PASSWORD` — available in workspace" in content
    assert "hunter2" not in content


def test_generic_connector_note_tolerates_json_string_credentials():
    ds = {
        "type": "generic",
        "name": "StringCreds",
        "credentials": '{"env_vars": {"TOKEN": "x"}}',
    }

    assert "`TOKEN` — available in workspace" in subject.build_datasource_note_content(
        ds
    )


def test_generic_connector_note_tolerates_unparseable_credentials():
    ds = {"type": "generic", "name": "Broken", "credentials": "not json"}

    content = subject.build_datasource_note_content(ds)

    assert content == "## Connector: Broken"


def test_repository_note_slugs_the_name_into_the_clone_path():
    ds = {
        "type": "repository",
        "name": "My Awesome Repo!!!",
        "default_branch": "trunk",
    }

    content = subject.build_datasource_note_content(ds)

    assert "## Repository: My Awesome Repo!!!" in content
    assert "Cloned to `./repos/my-awesome-repo/`" in content
    assert "- Default branch: `trunk`" in content


def test_kb_note_states_the_release_read_only_rule_and_the_root():
    ds = {
        "type": "kb",
        "name": "Vault",
        "description": "The design vault",
        "config": {"root_path": "knowledge/"},
    }

    content = subject.build_datasource_note_content(ds)

    assert "## OKF Knowledge Base: Vault" in content
    assert "Centrally indexed and read-only to agents in this release." in content
    assert "OKF root: `knowledge/`" in content
    assert "use the `kb_*` tools" in content


@pytest.mark.parametrize(
    "ds_type,write_tool,cli",
    [
        ("postgresql", "sql_execute", "psql"),
        ("neo4j", "cypher_execute", "cypher-shell"),
        ("mongodb", "mongo_insert", "mongosh"),
    ],
)
def test_a_read_write_managed_connector_names_its_write_tools_not_a_cli(
    ds_type, write_tool, cli
):
    ds = {"type": ds_type, "name": "Analytics", "project_read_only": False}

    content = subject.build_datasource_note_content(ds)

    assert "**Access:** read-write (tools)" in content
    assert f"`{write_tool}`" in content
    assert cli not in content
    assert "CLI" not in content


@pytest.mark.parametrize(
    "ds_type,tool",
    [
        ("postgresql", "sql_query"),
        ("neo4j", "cypher_query"),
        ("mongodb", "mongo_query"),
    ],
)
def test_a_read_only_managed_connector_names_tools_and_denies_the_cli(ds_type, tool):
    ds = {"type": ds_type, "name": "Analytics", "project_read_only": True}

    content = subject.build_datasource_note_content(ds)

    assert "**Access:** read-only (tools)" in content
    assert tool in content
    assert "No CLI access or write operations available." in content


def test_a_read_write_webdav_connector_lists_the_write_tools():
    ds = {"type": "webdav", "name": "Files", "project_read_only": False}

    content = subject.build_datasource_note_content(ds)

    assert "**Access:** read-write" in content
    assert "`webdav_write`" in content
    assert "`webdav_delete`" in content


def test_a_read_only_webdav_connector_omits_the_write_tools():
    ds = {"type": "webdav", "name": "Files", "project_read_only": True}

    content = subject.build_datasource_note_content(ds)

    assert "**Access:** read-only" in content
    assert "`webdav_write`" not in content
    assert "`webdav_delete`" not in content


def test_an_unknown_type_still_produces_a_minimal_note():
    ds = {"type": "smoke-signal", "name": "Odd", "description": "who knows"}

    assert subject.build_datasource_note_content(ds) == "## Connector: Odd\nwho knows"


# =============================================================================
# Neo4j-absent degradation
# =============================================================================


def _fake_graph_module(*, connect, monkeypatch):
    import sys
    import types

    module = types.ModuleType("shared.runtime.services.knowledge_graph")
    module.KnowledgeGraphDB = lambda: SimpleNamespace(
        connect=connect, marker="constructed"
    )
    monkeypatch.setitem(sys.modules, "shared.runtime.services.knowledge_graph", module)


def test_the_graph_handle_degrades_to_none_when_neo4j_refuses_the_connection(
    monkeypatch,
):
    _fake_graph_module(connect=lambda: False, monkeypatch=monkeypatch)
    logger = MagicMock()
    handle = subject.KnowledgeGraphHandle(logger=logger)

    assert handle.get() is None
    logger.warning.assert_called_once_with(
        "Could not connect to Neo4j for knowledge base"
    )


def test_the_graph_handle_degrades_to_none_when_the_driver_is_unimportable(
    monkeypatch,
):
    import sys

    monkeypatch.setitem(sys.modules, "shared.runtime.services.knowledge_graph", None)
    logger = MagicMock()
    handle = subject.KnowledgeGraphHandle(logger=logger)

    assert handle.get() is None
    assert "KnowledgeGraphDB not available" in logger.warning.call_args.args[0]


def test_the_graph_handle_constructs_the_driver_only_once(monkeypatch):
    _fake_graph_module(connect=lambda: True, monkeypatch=monkeypatch)
    logger = MagicMock()
    handle = subject.KnowledgeGraphHandle(logger=logger)

    first = handle.get()
    second = handle.get()

    assert first is second
    assert first.marker == "constructed"
    logger.warning.assert_not_called()


@pytest.mark.asyncio
async def test_sync_writes_the_pgvector_row_when_neo4j_is_absent():
    conn = SimpleNamespace(execute=AsyncMock())
    deps = _deps(conn=conn, graph=SimpleNamespace(get=lambda: None))

    await subject.sync_datasource_knowledge(
        PROJECT_ID,
        {"id": DS_ID, "name": "Files", "type": "webdav"},
        dependencies=deps,
    )

    sql, *params = conn.execute.await_args.args
    assert "INSERT INTO knowledge_index" in sql
    assert params[0] == "ds-1234abcd"
    assert params[1] == PROJECT_ID
    assert params[2] == "Connector: Files (webdav)"
    assert params[3] == ["datasource", "webdav"]


@pytest.mark.asyncio
async def test_sync_without_neo4j_is_not_a_strict_failure():
    deps = _deps(graph=SimpleNamespace(get=lambda: None))

    await subject.sync_datasource_knowledge(
        PROJECT_ID,
        {"id": DS_ID, "name": "Files", "type": "webdav"},
        strict=True,
        dependencies=deps,
    )


@pytest.mark.asyncio
async def test_sync_upserts_the_note_and_its_tags_into_neo4j():
    graph, kg = _graph()
    deps = _deps(graph=graph)

    await subject.sync_datasource_knowledge(
        PROJECT_ID,
        {"id": DS_ID, "name": "Repo", "type": "repository"},
        dependencies=deps,
    )

    statements = [call.args[0] for call in kg._db.execute_write.call_args_list]
    assert "MERGE (n:Note {project_id: $pid, id: $nid})" in statements[0]
    assert [
        call.args[1]["tag"] for call in kg._db.execute_write.call_args_list[1:]
    ] == [
        "datasource",
        "repository",
    ]


@pytest.mark.asyncio
async def test_a_neo4j_failure_is_swallowed_unless_strict_and_never_logs_the_error():
    graph, kg = _graph(execute_write=MagicMock(side_effect=RuntimeError("boom-detail")))
    logger = MagicMock()
    deps = _deps(graph=graph, logger=logger)
    datasource = {"id": DS_ID, "name": "Files", "type": "webdav"}

    await subject.sync_datasource_knowledge(PROJECT_ID, datasource, dependencies=deps)

    logged = logger.warning.call_args.args
    assert logged[0] == "Neo4j datasource knowledge sync failed error_class=%s"
    assert logged[1] == "RuntimeError"
    assert "boom-detail" not in str(logged)

    with pytest.raises(
        RuntimeError, match="Datasource knowledge sync failed for neo4j"
    ):
        await subject.sync_datasource_knowledge(
            PROJECT_ID, datasource, strict=True, dependencies=deps
        )


@pytest.mark.asyncio
async def test_strict_sync_names_every_failed_leg():
    graph, _kg = _graph(execute_write=MagicMock(side_effect=RuntimeError("x")))
    conn = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("y")))
    deps = _deps(conn=conn, graph=graph)

    with pytest.raises(RuntimeError) as exc:
        await subject.sync_datasource_knowledge(
            PROJECT_ID,
            {"id": DS_ID, "name": "Files", "type": "webdav"},
            strict=True,
            dependencies=deps,
        )

    assert str(exc.value) == "Datasource knowledge sync failed for neo4j, pgvector"


@pytest.mark.asyncio
async def test_delete_removes_both_legs_by_the_deterministic_note_id():
    graph, kg = _graph()
    conn = SimpleNamespace(execute=AsyncMock())
    deps = _deps(conn=conn, graph=graph)

    await subject.delete_datasource_knowledge(PROJECT_ID, DS_ID, dependencies=deps)

    assert kg._db.execute_write.call_args.args[1] == {
        "pid": PROJECT_ID,
        "nid": "ds-1234abcd",
    }
    sql, *params = conn.execute.await_args.args
    assert "DELETE FROM knowledge_index" in sql
    assert params == [PROJECT_ID, "ds-1234abcd"]


@pytest.mark.asyncio
async def test_delete_without_neo4j_still_removes_the_index_row():
    conn = SimpleNamespace(execute=AsyncMock())
    deps = _deps(conn=conn, graph=SimpleNamespace(get=lambda: None))

    await subject.delete_datasource_knowledge(
        PROJECT_ID, DS_ID, strict=True, dependencies=deps
    )

    assert "DELETE FROM knowledge_index" in conn.execute.await_args.args[0]


@pytest.mark.asyncio
async def test_strict_delete_reports_a_failed_pgvector_leg():
    conn = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("y")))
    deps = _deps(conn=conn, graph=SimpleNamespace(get=lambda: None))

    with pytest.raises(RuntimeError) as exc:
        await subject.delete_datasource_knowledge(
            PROJECT_ID, DS_ID, strict=True, dependencies=deps
        )

    assert str(exc.value) == "Datasource knowledge delete failed for pgvector"


# =============================================================================
# Retrieval messages — what makes the note findable
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ds_type,expected_tail",
    [
        ("repository", "available repositories"),
        ("kb", "available OKF knowledge bases"),
        ("generic", "available connectors"),
        ("postgresql", "What databases are available?"),
    ],
)
async def test_retrieval_messages_are_typed_to_the_connector(ds_type, expected_tail):
    conn = SimpleNamespace(execute=AsyncMock())
    deps = _deps(conn=conn, graph=SimpleNamespace(get=lambda: None))

    await subject.sync_datasource_knowledge(
        PROJECT_ID,
        {"id": DS_ID, "name": "Thing", "type": ds_type},
        dependencies=deps,
    )

    retrieval_messages = conn.execute.await_args.args[6]
    assert retrieval_messages[-1] == expected_tail
