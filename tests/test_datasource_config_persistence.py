"""Datasource config migration and PostgreSQL persistence tests."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.database.postgres import PostgresDB, _datasource_row_to_dict


MIGRATION = (
    Path(__file__).parents[1]
    / "src/orchestrator/database/migrations/app/0055_datasource_config.sql"
)
DATASOURCE_ID = "11111111-1111-1111-1111-111111111111"
PROJECT_ID = "22222222-2222-2222-2222-222222222222"
PROJECT_ID_B = "55555555-5555-5555-5555-555555555555"
JOB_ID = "33333333-3333-3333-3333-333333333333"
USER_ID = "44444444-4444-4444-4444-444444444444"


def _make_db(conn: AsyncMock) -> PostgresDB:
    db = PostgresDB.__new__(PostgresDB)
    db._pool = MagicMock()
    db._connection_string = "test"
    db._queries = {}

    @asynccontextmanager
    async def acquire():
        yield conn

    @asynccontextmanager
    async def transaction():
        yield

    conn.transaction = MagicMock(side_effect=transaction)
    db.acquire = acquire

    # The Connector write-through (tests/test_manifest_connectors*.py) runs
    # around these row writes; here only the row SQL is pinned.
    @asynccontextmanager
    async def transaction_scope():
        yield conn

    db.transaction_scope = transaction_scope
    db._lock_connector_catalog = AsyncMock()
    db._persist_connector_resource = AsyncMock(return_value="unchanged")
    return db


def _assert_projects_config(sql: str, *, qualified: bool = False) -> None:
    projection = "d.config" if qualified else "config"
    assert projection in sql.split("FROM", maxsplit=1)[0]


def test_migration_adds_non_null_jsonb_config_with_empty_object_default():
    sql = MIGRATION.read_text()

    assert "ALTER TABLE datasources" in sql
    assert "ADD COLUMN IF NOT EXISTS config JSONB NOT NULL" in sql
    assert "DEFAULT '{}'::jsonb" in sql


@pytest.mark.parametrize(
    ("raw_config", "expected"),
    [
        ('{"root_path": "docs/knowledge"}', {"root_path": "docs/knowledge"}),
        ({"root_path": "knowledge"}, {"root_path": "knowledge"}),
        (None, {}),
        ("[]", {}),
    ],
)
def test_datasource_rows_return_config_as_an_ordinary_dict(raw_config, expected):
    row = {"id": DATASOURCE_ID, "credentials": "{}", "config": raw_config}

    assert _datasource_row_to_dict(row)["config"] == expected


@pytest.mark.asyncio
async def test_create_datasource_persists_and_returns_config():
    conn = AsyncMock()
    conn.fetchrow.return_value = {
        "id": DATASOURCE_ID,
        "credentials": "{}",
        "config": '{"root_path": "docs/knowledge"}',
    }
    db = _make_db(conn)

    result = await db.create_datasource(
        name="Docs",
        ds_type="kb",
        connection_url="https://example.test/docs.git",
        config={"root_path": "docs/knowledge"},
    )

    sql, *params = conn.fetchrow.await_args.args
    assert "credentials, config, job_id" in sql
    assert "$6::jsonb" in sql
    assert json.loads(params[5]) == {"root_path": "docs/knowledge"}
    assert result["config"] == {"root_path": "docs/knowledge"}


@pytest.mark.asyncio
async def test_update_datasource_persists_explicit_config():
    conn = AsyncMock()
    conn.execute.return_value = "UPDATE 1"
    db = _make_db(conn)

    assert await db.update_datasource(DATASOURCE_ID, config={"root_path": "handbook"})

    sql, *params = conn.execute.await_args_list[0].args
    assert "config = $1::jsonb" in sql
    assert json.loads(params[0]) == {"root_path": "handbook"}
    conn.transaction.assert_called_once()
    assert (
        "datasource_project_reconcile_queue" in conn.execute.await_args_list[1].args[0]
    )


@pytest.mark.asyncio
async def test_update_datasource_omitting_config_preserves_it():
    conn = AsyncMock()
    conn.execute.return_value = "UPDATE 1"
    db = _make_db(conn)

    assert await db.update_datasource(DATASOURCE_ID, name="Renamed")

    sql = conn.execute.await_args_list[0].args[0]
    assert "config =" not in sql


@pytest.mark.asyncio
async def test_update_datasource_visibility_advances_policy_revision():
    conn = AsyncMock()
    conn.execute.return_value = "UPDATE 1"
    db = _make_db(conn)

    assert await db.update_datasource(DATASOURCE_ID, is_global=False)

    sql = conn.execute.await_args.args[0]
    assert "is_global = $1" in sql
    assert "policy_revision = policy_revision + 1" in sql


@pytest.mark.asyncio
async def test_update_datasource_can_explicitly_clear_connection_url():
    conn = AsyncMock()
    conn.execute.return_value = "UPDATE 1"
    db = _make_db(conn)

    assert await db.update_datasource(
        DATASOURCE_ID,
        connection_url=None,
        connection_url_set=True,
    )

    sql, *params = conn.execute.await_args_list[0].args
    assert "connection_url = $1" in sql
    assert params[0] is None


@pytest.mark.asyncio
async def test_crud_list_and_get_queries_return_config():
    conn = AsyncMock()
    conn.fetch.return_value = []
    conn.fetchrow.return_value = None
    db = _make_db(conn)

    await db.list_datasources()
    _assert_projects_config(conn.fetch.await_args.args[0])

    await db.get_datasource(DATASOURCE_ID)
    _assert_projects_config(conn.fetchrow.await_args.args[0])


@pytest.mark.asyncio
async def test_job_query_returns_config_without_legacy_fallback():
    conn = AsyncMock()
    conn.fetch.return_value = []
    db = _make_db(conn)

    await db.resolve_datasources_for_job(JOB_ID, PROJECT_ID)

    conn.fetch.assert_awaited_once()
    explicit_call = conn.fetch.await_args
    _assert_projects_config(explicit_call.args[0], qualified=True)
    assert "job_datasources" in explicit_call.args[0]
    assert "FROM datasources d" not in explicit_call.args[0]


@pytest.mark.asyncio
async def test_thread_eligible_and_project_queries_return_config():
    conn = AsyncMock()
    conn.fetch.return_value = []
    db = _make_db(conn)

    await db.resolve_datasources_for_thread([DATASOURCE_ID], [PROJECT_ID])
    _assert_projects_config(conn.fetch.await_args.args[0], qualified=True)

    await db.list_eligible_datasources(USER_ID, [PROJECT_ID])
    _assert_projects_config(conn.fetch.await_args.args[0], qualified=True)

    await db.list_project_datasources(PROJECT_ID)
    _assert_projects_config(conn.fetch.await_args.args[0], qualified=True)


@pytest.mark.asyncio
async def test_thread_resolution_collapses_multi_project_overrides_conservatively():
    conn = AsyncMock()
    conn.fetch.return_value = [
        {
            "id": DATASOURCE_ID,
            "name": "Application DB",
            "type": "postgresql",
            "credentials": "{}",
            "config": "{}",
            "policy_revision": 7,
            "project_read_only": True,
        }
    ]
    db = _make_db(conn)

    resolved = await db.resolve_datasources_for_thread(
        [DATASOURCE_ID], [PROJECT_ID, PROJECT_ID_B]
    )

    assert len(resolved) == 1
    assert resolved[0]["id"] == DATASOURCE_ID
    assert resolved[0]["policy_revision"] == 7
    assert resolved[0]["project_read_only"] is True
    sql, datasource_ids, project_ids = conn.fetch.await_args.args
    assert "LEFT JOIN LATERAL" in sql
    assert "BOOL_OR(pd.read_only)" in sql
    assert "SELECT DISTINCT" not in sql
    assert len(datasource_ids) == 1
    assert {str(value) for value in project_ids} == {PROJECT_ID, PROJECT_ID_B}


@pytest.mark.asyncio
async def test_default_datasource_upsert_persists_and_returns_config():
    conn = AsyncMock()
    conn.fetchrow.return_value = {
        "id": DATASOURCE_ID,
        "credentials": "{}",
        "config": '{"root_path": ""}',
    }
    db = _make_db(conn)

    result = await db.upsert_default_datasource(
        name="Default KB",
        ds_type="kb",
        connection_url="https://example.test/docs.git",
        config={"root_path": ""},
    )

    sql, *params = conn.fetchrow.await_args.args
    assert "credentials,\n                                         config" in sql
    assert "config = EXCLUDED.config" in sql
    assert "config, created_by" in sql
    assert json.loads(params[4]) == {"root_path": ""}
    assert result["config"] == {"root_path": ""}
