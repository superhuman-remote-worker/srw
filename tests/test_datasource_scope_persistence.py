"""Database contract tests for connector scope/default persistence."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from orchestrator.database.postgres import (
    DatasourceCatalogCursorError,
    DatasourceMaterializationAuthorizationError,
    DatasourcePolicyConflictError,
    DatasourceProjectAuthorizationError,
    DatasourcePolicyValidationError,
    DatasourceScopeAuthorizationError,
    PostgresDB,
    _encode_page_cursor,
)


MIGRATION = (
    Path(__file__).parents[1]
    / "src/orchestrator/database/migrations/app/0083_datasource_scope_auto_attach.sql"
)
DATASOURCE_ID = "11111111-1111-4111-8111-111111111111"
PROJECT_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
PROJECT_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
JOB_ID = "22222222-2222-4222-8222-222222222222"
THREAD_ID = "33333333-3333-4333-8333-333333333333"
LEGACY_JOB_ID = "44444444-4444-4444-8444-444444444444"
USER_ID = "55555555-5555-4555-8555-555555555555"


def _make_db(conn: AsyncMock) -> PostgresDB:
    db = PostgresDB.__new__(PostgresDB)

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


def _datasource_row(*, revision: int = 1, scope: str = "all") -> dict:
    return {
        "id": UUID(DATASOURCE_ID),
        "name": "Application DB",
        "description": None,
        "type": "postgresql",
        "connection_url": None,
        "credentials": "{}",
        "config": "{}",
        "job_id": None,
        "cli_hint": None,
        "default_branch": None,
        "created_by": None,
        "is_global": False,
        "read_only": None,
        "scope_mode": scope,
        "auto_attach": False,
        "policy_revision": revision,
        "created_at": None,
        "updated_at": None,
    }


def test_migration_backfills_and_retires_legacy_runtime_association():
    sql = MIGRATION.read_text()

    assert "ADD COLUMN scope_mode" in sql
    assert "ADD COLUMN auto_attach" in sql
    assert "ADD COLUMN policy_revision" in sql
    assert "INSERT INTO job_datasources" in sql
    assert "WHERE job_id IS NOT NULL" in sql
    assert "materialized_job_selections" in sql
    assert "'datasource_ids', selection.datasource_ids" in sql
    assert "config->>'native_project_id'" in sql
    assert "ON p.id = CASE" in sql
    assert "THEN (d.config->>'native_project_id')::UUID" in sql
    assert "created_by IS NULL" in sql
    assert "AND NOT (\n      d.type = 'kb'" in sql
    assert "datasource_project_reconcile_queue" in sql
    assert "CREATE SEQUENCE datasource_project_reconcile_generation_seq" in sql
    assert "claim_token BIGINT NOT NULL" in sql
    assert "claim_token = nextval(" in sql
    assert "AFTER INSERT OR UPDATE OR DELETE ON project_datasources" in sql


@pytest.mark.asyncio
async def test_create_datasource_and_initial_project_links_are_atomic():
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        _datasource_row(),
        _datasource_row(revision=3, scope="projects"),
    ]
    db = _make_db(conn)

    created = await db.create_datasource(
        name="Application DB",
        ds_type="postgresql",
        scope_mode="projects",
        auto_attach=True,
        project_ids=[PROJECT_A, PROJECT_B],
    )

    conn.transaction.assert_called_once()
    conn.executemany.assert_awaited_once()
    link_rows = conn.executemany.await_args.args[1]
    assert {str(row[0]) for row in link_rows} == {PROJECT_A, PROJECT_B}
    assert created["policy_revision"] == 3


@pytest.mark.asyncio
async def test_create_datasource_rechecks_project_owner_inside_transaction():
    conn = AsyncMock()
    conn.fetchrow.return_value = {"is_admin": False, "is_approved": True}
    conn.fetch.return_value = []
    db = _make_db(conn)

    with pytest.raises(DatasourceProjectAuthorizationError):
        await db.create_datasource(
            name="Application DB",
            ds_type="postgresql",
            scope_mode="projects",
            project_ids=[PROJECT_A],
            authority_user_id=USER_ID,
            authority_is_admin=True,
        )

    conn.transaction.assert_called_once()
    authority_sql = conn.fetch.await_args.args[0]
    assert "FROM project_members" in authority_sql
    assert "role = 'owner'" in authority_sql
    assert "FOR UPDATE" in authority_sql
    actor_sql = conn.fetchrow.await_args.args[0]
    assert "FROM users" in actor_sql
    assert "FOR UPDATE" in actor_sql
    conn.executemany.assert_not_awaited()


@pytest.mark.asyncio
async def test_policy_full_set_diff_preserves_retained_link_overrides():
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        _datasource_row(),
        _datasource_row(revision=3, scope="projects"),
    ]
    conn.execute.return_value = "UPDATE 1"
    conn.fetch.side_effect = [
        [
            {
                "project_id": UUID(PROJECT_A),
                "linked_at": "original",
                "read_only": False,
                "description": "keep me",
            }
        ],
        [
            {"project_id": UUID(PROJECT_A)},
            {"project_id": UUID(PROJECT_B)},
        ],
    ]
    db = _make_db(conn)

    updated = await db.update_datasource_policy(
        DATASOURCE_ID,
        expected_policy_revision=1,
        scope_mode="projects",
        auto_attach=True,
        project_ids=[PROJECT_A, PROJECT_B],
    )

    inserted = conn.executemany.await_args.args[1]
    assert inserted == [(UUID(PROJECT_B), UUID(DATASOURCE_ID), None)]
    update_sql = conn.execute.await_args.args[0]
    assert "UPDATE datasources" in update_sql
    assert updated["project_ids"] == [PROJECT_A, PROJECT_B]


@pytest.mark.asyncio
async def test_stale_policy_revision_fails_before_link_mutation():
    conn = AsyncMock()
    conn.fetchrow.return_value = _datasource_row(revision=4)
    db = _make_db(conn)

    with pytest.raises(DatasourcePolicyConflictError):
        await db.update_datasource_policy(
            DATASOURCE_ID,
            expected_policy_revision=3,
            auto_attach=True,
        )

    conn.fetch.assert_not_awaited()
    conn.executemany.assert_not_awaited()


@pytest.mark.asyncio
async def test_policy_addition_rechecks_project_owner_before_mutation():
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        _datasource_row(),
        {"is_admin": False, "is_approved": True},
    ]
    conn.fetch.side_effect = [[], []]
    db = _make_db(conn)

    with pytest.raises(DatasourceProjectAuthorizationError):
        await db.update_datasource_policy(
            DATASOURCE_ID,
            expected_policy_revision=1,
            scope_mode="projects",
            project_ids=[PROJECT_B],
            authority_user_id=USER_ID,
        )

    authority_sql = conn.fetch.await_args_list[1].args[0]
    assert "FROM project_members" in authority_sql
    assert "FOR UPDATE" in authority_sql
    conn.execute.assert_not_awaited()
    conn.executemany.assert_not_awaited()


@pytest.mark.asyncio
async def test_plain_scoped_mutation_rejects_multi_project_connector_under_lock():
    conn = AsyncMock()
    conn.fetchrow.return_value = {"scope_mode": "projects"}
    conn.fetch.return_value = [
        {"project_id": UUID(PROJECT_A)},
        {"project_id": UUID(PROJECT_B)},
    ]
    db = _make_db(conn)

    with pytest.raises(DatasourceScopeAuthorizationError):
        await db.update_datasource(
            DATASOURCE_ID,
            name="Renamed",
            authority_project_scope_id=PROJECT_A,
        )

    assert "FOR UPDATE" in conn.fetchrow.await_args.args[0]
    assert "FOR UPDATE" in conn.fetch.await_args.args[0]
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_combined_scoped_mutation_must_remain_exactly_one_project():
    conn = AsyncMock()
    conn.fetchrow.return_value = _datasource_row(scope="projects")
    conn.fetch.return_value = [{"project_id": UUID(PROJECT_A)}]
    db = _make_db(conn)

    with pytest.raises(DatasourceScopeAuthorizationError):
        await db.update_datasource_policy(
            DATASOURCE_ID,
            expected_policy_revision=1,
            scope_mode="all",
            project_ids=[PROJECT_A],
            authority_project_scope_id=PROJECT_A,
        )

    conn.execute.assert_not_awaited()
    conn.executemany.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_job_materializes_links_in_job_transaction(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.manifest_execution_snapshot.capture_execution",
        AsyncMock(),
    )
    conn = AsyncMock()
    conn.fetch.side_effect = [
        [{"id": UUID(DATASOURCE_ID), "policy_revision": 7}],
        [{"id": UUID(PROJECT_A)}],
        [{"project_id": UUID(PROJECT_A)}],
    ]
    conn.fetchrow.side_effect = [
        {"is_admin": False, "is_approved": True},
        {"id": UUID(JOB_ID), "status": "created"},
    ]
    db = _make_db(conn)

    result = await db.create_job(
        description="Inspect data",
        user_id=USER_ID,
        project_id=PROJECT_A,
        datasource_ids=[DATASOURCE_ID],
        datasource_selection_provenance={
            "origin": "default",
            "policy_revisions": {DATASOURCE_ID: 7},
        },
        authority_user_id=USER_ID,
        authority_project_ids=[PROJECT_A],
    )

    conn.transaction.assert_called_once()
    insert_call = conn.fetchrow.await_args_list[-1]
    persisted_context = json.loads(insert_call.args[5])
    assert persisted_context["datasource_selection"] == {
        "origin": "default",
        "datasource_ids": [DATASOURCE_ID],
        "policy_revisions": {DATASOURCE_ID: 7},
    }
    assert "FROM users" in conn.fetchrow.await_args_list[0].args[0]
    assert "FOR UPDATE" in conn.fetchrow.await_args_list[0].args[0]
    assert "FROM projects" in conn.fetch.await_args_list[1].args[0]
    assert "FOR KEY SHARE" in conn.fetch.await_args_list[1].args[0]
    assert "FROM project_members" in conn.fetch.await_args_list[2].args[0]
    assert "FOR UPDATE" in conn.fetch.await_args_list[2].args[0]
    assert conn.executemany.await_args.args[1] == [(UUID(JOB_ID), UUID(DATASOURCE_ID))]
    assert str(result["id"]) == JOB_ID


@pytest.mark.asyncio
async def test_create_job_rejects_membership_revoked_after_policy_resolution():
    conn = AsyncMock()
    conn.fetch.side_effect = [
        [{"id": UUID(DATASOURCE_ID), "policy_revision": 7}],
        [{"id": UUID(PROJECT_A)}],
        [],
    ]
    conn.fetchrow.return_value = {"is_admin": False, "is_approved": True}
    db = _make_db(conn)

    with pytest.raises(DatasourceMaterializationAuthorizationError):
        await db.create_job(
            description="Inspect data",
            user_id=USER_ID,
            project_id=PROJECT_A,
            datasource_ids=[DATASOURCE_ID],
            datasource_policy_revisions={DATASOURCE_ID: 7},
            authority_user_id=USER_ID,
            authority_project_ids=[PROJECT_A],
        )

    sql_calls = [call.args[0] for call in conn.fetchrow.await_args_list]
    assert any("FROM users" in sql and "FOR UPDATE" in sql for sql in sql_calls)
    assert not any("INSERT INTO jobs" in sql for sql in sql_calls)
    conn.executemany.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_job_requires_complete_policy_snapshot_before_database_access():
    conn = AsyncMock()
    db = _make_db(conn)

    with pytest.raises(DatasourcePolicyValidationError):
        await db.create_job(
            description="Inspect data",
            datasource_ids=[DATASOURCE_ID],
            datasource_selection_provenance={"origin": "default"},
        )

    conn.fetch.assert_not_awaited()
    conn.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_job_rejects_changed_policy_before_job_insert():
    conn = AsyncMock()
    conn.fetch.return_value = [{"id": UUID(DATASOURCE_ID), "policy_revision": 8}]
    db = _make_db(conn)

    with pytest.raises(DatasourcePolicyConflictError):
        await db.create_job(
            description="Inspect data",
            datasource_ids=[DATASOURCE_ID],
            datasource_policy_revisions={DATASOURCE_ID: 7},
        )

    conn.transaction.assert_called_once()
    conn.fetchrow.assert_not_awaited()
    conn.executemany.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_thread_always_materializes_empty_selection_metadata(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.manifest_execution_snapshot.capture_execution",
        AsyncMock(),
    )
    conn = AsyncMock()
    conn.fetchrow.return_value = {"id": UUID(THREAD_ID)}
    db = _make_db(conn)

    await db.create_thread()

    metadata = json.loads(conn.fetchrow.await_args.args[7])
    assert metadata == {"datasource_ids": []}


@pytest.mark.asyncio
async def test_create_thread_locks_policy_and_persists_revision_snapshot(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.manifest_execution_snapshot.capture_execution",
        AsyncMock(),
    )
    conn = AsyncMock()
    conn.fetch.return_value = [{"id": UUID(DATASOURCE_ID), "policy_revision": 4}]
    conn.fetchrow.return_value = {"id": UUID(THREAD_ID)}
    db = _make_db(conn)

    thread_id = await db.create_thread(
        datasource_ids=[DATASOURCE_ID],
        datasource_policy_revisions={DATASOURCE_ID: 4},
        datasource_selection_provenance={"origin": "explicit"},
    )

    conn.transaction.assert_called_once()
    metadata = json.loads(conn.fetchrow.await_args.args[7])
    assert metadata == {
        "datasource_ids": [DATASOURCE_ID],
        "datasource_selection": {
            "origin": "explicit",
            "policy_revisions": {DATASOURCE_ID: 4},
        },
    }
    assert thread_id == THREAD_ID


@pytest.mark.asyncio
async def test_create_thread_rejects_deleted_connector_before_thread_insert():
    conn = AsyncMock()
    conn.fetch.return_value = []
    db = _make_db(conn)

    with pytest.raises(DatasourcePolicyConflictError):
        await db.create_thread(
            datasource_ids=[DATASOURCE_ID],
            datasource_selection_provenance={"policy_revisions": {DATASOURCE_ID: 4}},
        )

    conn.transaction.assert_called_once()
    conn.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_combined_content_and_policy_update_uses_one_transaction():
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        _datasource_row(),
        {
            **_datasource_row(revision=2, scope="projects"),
            "name": "Renamed",
        },
    ]
    conn.fetch.side_effect = [
        [{"project_id": UUID(PROJECT_A)}],
        [{"project_id": UUID(PROJECT_A)}],
    ]
    conn.execute.return_value = "UPDATE 1"
    db = _make_db(conn)

    result = await db.update_datasource_with_policy(
        DATASOURCE_ID,
        expected_policy_revision=1,
        name="Renamed",
        scope_mode="projects",
        auto_attach=True,
        project_ids=[PROJECT_A],
    )

    conn.transaction.assert_called_once()
    assert conn.execute.await_count == 2  # guarded UPDATE + durable reconcile enqueue
    update_sql = conn.execute.await_args_list[0].args[0]
    assert "name =" in update_sql
    assert "scope_mode =" in update_sql
    assert "policy_revision = policy_revision + 1" in update_sql
    assert result["name"] == "Renamed"


_NOTE_CONTENT_UPDATES = [
    pytest.param({"name": "Renamed"}, id="name"),
    pytest.param({"description": "Runbook"}, id="description"),
    pytest.param({"connection_url": "https://db.example.test"}, id="url"),
    pytest.param(
        {"connection_url": None, "connection_url_set": True},
        id="url-clear",
    ),
    pytest.param(
        {"credentials": {"env_vars": {"DATABASE_TOKEN": "secret"}}},
        id="credential-env-names",
    ),
    pytest.param({"cli_hint": "psql"}, id="cli-hint"),
    pytest.param({"default_branch": "develop"}, id="default-branch"),
    pytest.param({"config": {"root_path": "docs"}}, id="config"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("content_update", _NOTE_CONTENT_UPDATES)
async def test_plain_content_update_enqueues_every_note_dependency(content_update):
    conn = AsyncMock()
    conn.execute.return_value = "UPDATE 1"
    db = _make_db(conn)

    with patch(
        "orchestrator.database.postgres._encrypt_credentials_dict",
        return_value="encrypted",
    ):
        assert await db.update_datasource(DATASOURCE_ID, **content_update)

    assert conn.execute.await_count == 2
    enqueue_sql = conn.execute.await_args_list[1].args[0]
    assert "datasource_project_reconcile_queue" in enqueue_sql
    assert "datasource_project_reconcile_queue.policy_revision," in enqueue_sql
    assert "EXCLUDED.policy_revision" in enqueue_sql
    assert "claim_token = nextval(" in enqueue_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("content_update", _NOTE_CONTENT_UPDATES)
async def test_combined_update_enqueues_every_note_dependency(content_update):
    conn = AsyncMock()
    conn.fetchrow.side_effect = [_datasource_row(), _datasource_row()]
    conn.fetch.side_effect = [
        [{"project_id": UUID(PROJECT_A)}],
        [{"project_id": UUID(PROJECT_A)}],
    ]
    conn.execute.return_value = "UPDATE 1"
    db = _make_db(conn)

    with patch(
        "orchestrator.database.postgres._encrypt_credentials_dict",
        return_value="encrypted",
    ):
        result = await db.update_datasource_with_policy(
            DATASOURCE_ID,
            expected_policy_revision=1,
            **content_update,
        )

    assert result is not None
    assert conn.execute.await_count == 2
    enqueue_sql = conn.execute.await_args_list[1].args[0]
    assert "datasource_project_reconcile_queue" in enqueue_sql
    assert "datasource_project_reconcile_queue.policy_revision," in enqueue_sql
    assert "EXCLUDED.policy_revision" in enqueue_sql
    assert "claim_token = nextval(" in enqueue_sql


@pytest.mark.asyncio
async def test_combined_visibility_change_advances_policy_revision():
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        _datasource_row(),
        {**_datasource_row(revision=2), "is_global": True},
    ]
    conn.fetch.side_effect = [[], []]
    conn.execute.return_value = "UPDATE 1"
    db = _make_db(conn)

    result = await db.update_datasource_with_policy(
        DATASOURCE_ID,
        expected_policy_revision=1,
        is_global=True,
    )

    update_sql = conn.execute.await_args.args[0]
    assert "is_global =" in update_sql
    assert "policy_revision = policy_revision + 1" in update_sql
    assert result["policy_revision"] == 2


@pytest.mark.asyncio
async def test_catalog_filters_authorization_before_cursor_limit():
    conn = AsyncMock()
    first = {
        **_datasource_row(),
        "created_by": UUID(DATASOURCE_ID),
        "created_at": "2026-08-05T10:00:00+00:00",
        "project_ids": [UUID(PROJECT_A)],
    }
    second = {
        **first,
        "id": UUID("77777777-7777-4777-8777-777777777777"),
        "created_at": "2026-08-04T10:00:00+00:00",
    }
    conn.fetch.return_value = [first, second]
    db = _make_db(conn)

    page = await db.list_datasource_catalog(
        DATASOURCE_ID,
        [PROJECT_A],
        q="Application",
        ownership="mine",
        limit=1,
    )

    assert len(page["items"]) == 1
    assert page["next_cursor"]
    sql = conn.fetch.await_args.args[0]
    assert sql.index("d.created_by = $1") < sql.index("LIMIT")
    assert "ILIKE" in sql


@pytest.mark.asyncio
async def test_scoped_catalog_never_enriches_creator_with_hidden_links():
    conn = AsyncMock()
    conn.fetch.return_value = []
    db = _make_db(conn)

    await db.list_datasource_catalog(
        USER_ID,
        [PROJECT_A],
        restrict_to_projects=True,
    )

    sql, owner_id, visible_ids, full_admin, restrict_scope, _ = (
        conn.fetch.await_args.args
    )
    assert owner_id == UUID(USER_ID)
    assert visible_ids == [UUID(PROJECT_A)]
    assert full_admin is False
    assert restrict_scope is True
    assert "NOT $4::boolean AND d.created_by = $1" in sql
    assert "pd.project_id = ANY($2::uuid[])" in sql


@pytest.mark.asyncio
async def test_linkable_targets_mark_retained_only_project():
    conn = AsyncMock()
    retained = {
        "id": UUID(PROJECT_A),
        "name": "Application",
        "is_default": False,
        "user_role": "viewer",
        "linked": True,
        "addable": False,
        "retained_only": True,
    }
    conn.fetch.side_effect = [[], [retained]]
    db = _make_db(conn)

    page = await db.list_linkable_datasource_targets(
        DATASOURCE_ID,
        datasource_id=DATASOURCE_ID,
        restrict_project_id=PROJECT_A,
        q="does-not-match",
    )

    assert page["items"] == []
    assert page["selected_items"] == [retained]
    page_sql = conn.fetch.await_args_list[0].args[0]
    selected_sql = conn.fetch.await_args_list[1].args[0]
    assert "pm.role = 'owner'" in page_sql
    assert "pd.datasource_id IS NOT NULL" in page_sql
    assert "ILIKE" in page_sql
    assert "p.id = $4" in page_sql
    assert "ILIKE" not in selected_sql
    assert "LIMIT" not in selected_sql
    assert "pd.datasource_id = $2" in selected_sql
    assert "p.id = $4" in selected_sql


@pytest.mark.asyncio
async def test_linkable_targets_create_and_invalid_reads_return_empty_selection():
    conn = AsyncMock()
    conn.fetch.return_value = []
    db = _make_db(conn)

    create_page = await db.list_linkable_datasource_targets(DATASOURCE_ID)
    invalid_page = await db.list_linkable_datasource_targets(
        DATASOURCE_ID,
        datasource_id="not-a-uuid",
    )

    assert create_page == {
        "items": [],
        "selected_items": [],
        "next_cursor": None,
    }
    assert invalid_page == create_page
    conn.fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_catalog_rejects_malformed_cursor():
    db = _make_db(AsyncMock())

    with pytest.raises(DatasourceCatalogCursorError):
        await db.list_datasource_catalog(DATASOURCE_ID, cursor="not-a-cursor")


@pytest.mark.asyncio
async def test_catalog_rejects_naive_datetime_cursor_before_database_access():
    conn = AsyncMock()
    db = _make_db(conn)
    cursor = _encode_page_cursor(
        {"created_at": "2026-08-05T10:00:00", "id": DATASOURCE_ID}
    )

    with pytest.raises(DatasourceCatalogCursorError):
        await db.list_datasource_catalog(USER_ID, cursor=cursor)

    conn.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_project_linkable_datasources_are_target_aware_and_paginated():
    conn = AsyncMock()
    first = {
        key: value for key, value in _datasource_row().items() if key != "credentials"
    }
    first["created_by"] = UUID(USER_ID)
    second = {
        **first,
        "id": UUID("77777777-7777-4777-8777-777777777777"),
        "name": "Warehouse",
    }
    conn.fetch.return_value = [first, second]
    db = _make_db(conn)

    result = await db.list_project_linkable_datasources(
        USER_ID,
        PROJECT_A,
        q="data",
        limit=1,
    )

    assert len(result["items"]) == 1
    assert result["next_cursor"]
    sql, owner_id, project_id, is_admin, query, fetch_limit = conn.fetch.await_args.args
    assert owner_id == UUID(USER_ID)
    assert project_id == UUID(PROJECT_A)
    assert is_admin is False
    assert query == "data"
    assert fetch_limit == 2
    assert "d.job_id IS NULL" in sql
    # Platform-owned rows (the managed key, or the native KB marker an older
    # orchestrator wrote) are never linkable.
    assert "NOT (d.managed_key IS NOT NULL OR (d.type = 'kb'" in sql
    assert "d.config->>'native_project_id'" in sql
    assert "NOT EXISTS" in sql
    assert "linked_pd.project_id = $2" in sql
    assert "d.created_by = $1" in sql
    assert "d.is_global = TRUE AND d.scope_mode = 'all'" in sql
    assert "credentials" not in sql.split("FROM datasources", maxsplit=1)[0]
    assert "ORDER BY LOWER(d.name), d.id" in sql


@pytest.mark.asyncio
async def test_standalone_link_rechecks_owner_for_a_new_relationship():
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        {
            "id": UUID(DATASOURCE_ID),
            "created_by": UUID(USER_ID),
            "is_global": False,
            "scope_mode": "projects",
            "is_native": False,
        },
        None,
        {"is_admin": False, "is_approved": True},
    ]
    conn.fetch.return_value = []
    db = _make_db(conn)

    with pytest.raises(DatasourceProjectAuthorizationError):
        await db.link_datasource_to_project(
            PROJECT_A,
            DATASOURCE_ID,
            authority_user_id=USER_ID,
        )

    conn.transaction.assert_called_once()
    parent_sql = conn.fetchrow.await_args_list[0].args[0]
    link_sql = conn.fetchrow.await_args_list[1].args[0]
    assert "FROM datasources" in parent_sql
    assert "FOR UPDATE" in parent_sql
    assert "FROM project_datasources" in link_sql
    assert "FOR UPDATE" in conn.fetch.await_args.args[0]
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_standalone_link_rechecks_connector_authority_under_parent_lock():
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        {
            "id": UUID(DATASOURCE_ID),
            "created_by": UUID(DATASOURCE_ID),
            "is_global": False,
            "scope_mode": "all",
            "is_native": False,
        },
        None,
        {"is_admin": False, "is_approved": True},
    ]
    conn.fetch.return_value = [{"project_id": UUID(PROJECT_A)}]
    db = _make_db(conn)

    with pytest.raises(DatasourceProjectAuthorizationError):
        await db.link_datasource_to_project(
            PROJECT_A,
            DATASOURCE_ID,
            authority_user_id=USER_ID,
        )

    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_existing_link_post_rejects_concurrent_project_owner_demotion():
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        {
            "id": UUID(DATASOURCE_ID),
            "created_by": UUID(USER_ID),
            "is_global": False,
            "scope_mode": "projects",
            "is_native": False,
        },
        {"exists": 1},
        {"is_admin": False, "is_approved": True},
        None,
    ]
    db = _make_db(conn)

    with pytest.raises(DatasourceProjectAuthorizationError):
        await db.link_datasource_to_project(
            PROJECT_A,
            DATASOURCE_ID,
            read_only=True,
            authority_user_id=USER_ID,
            authority_is_admin=False,
        )

    actor_sql = conn.fetchrow.await_args_list[2].args[0]
    membership_sql = conn.fetchrow.await_args_list[3].args[0]
    assert "FROM users" in actor_sql and "FOR UPDATE" in actor_sql
    assert "FROM project_members" in membership_sql
    assert "role = 'owner'" in membership_sql
    assert "FOR UPDATE" in membership_sql
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["update", "unlink"])
async def test_project_link_mutations_lock_datasource_parent_first(operation):
    conn = AsyncMock()
    conn.fetchrow.return_value = {"id": UUID(DATASOURCE_ID)}
    conn.execute.return_value = "UPDATE 1" if operation == "update" else "DELETE 1"
    db = _make_db(conn)

    if operation == "update":
        assert await db.update_project_datasource(
            PROJECT_A,
            DATASOURCE_ID,
            description="Production",
        )
    else:
        assert await db.unlink_datasource_from_project(PROJECT_A, DATASOURCE_ID)

    conn.transaction.assert_called_once()
    parent_sql = conn.fetchrow.await_args.args[0]
    child_sql = conn.execute.await_args.args[0]
    assert "FROM datasources" in parent_sql
    assert "FOR UPDATE" in parent_sql
    assert "project_datasources" in child_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["update", "unlink"])
async def test_project_link_mutation_rejects_concurrent_owner_demotion(operation):
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        {"id": UUID(DATASOURCE_ID), "created_by": UUID(DATASOURCE_ID)},
        {"is_admin": False, "is_approved": True},
        None,
    ]
    db = _make_db(conn)

    with pytest.raises(DatasourceProjectAuthorizationError):
        if operation == "update":
            await db.update_project_datasource(
                PROJECT_A,
                DATASOURCE_ID,
                description="Production",
                authority_user_id=USER_ID,
                authority_is_admin=False,
            )
        else:
            await db.unlink_datasource_from_project(
                PROJECT_A,
                DATASOURCE_ID,
                authority_user_id=USER_ID,
                authority_is_admin=False,
            )

    conn.transaction.assert_called_once()
    parent_sql = conn.fetchrow.await_args_list[0].args[0]
    actor_sql = conn.fetchrow.await_args_list[1].args[0]
    membership_sql = conn.fetchrow.await_args_list[2].args[0]
    assert "FROM datasources" in parent_sql and "FOR UPDATE" in parent_sql
    assert "FROM users" in actor_sql and "FOR UPDATE" in actor_sql
    assert "FROM project_members" in membership_sql
    assert "role = 'owner'" in membership_sql
    assert "FOR UPDATE" in membership_sql
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_owner_can_revoke_link_without_project_owner_role():
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        {"id": UUID(DATASOURCE_ID), "created_by": UUID(USER_ID)},
        {"is_admin": False, "is_approved": True},
    ]
    conn.execute.return_value = "DELETE 1"
    db = _make_db(conn)

    removed = await db.unlink_datasource_from_project(
        PROJECT_A,
        DATASOURCE_ID,
        authority_user_id=USER_ID,
        authority_is_admin=False,
    )

    assert removed is True
    assert len(conn.fetchrow.await_args_list) == 2
    assert "FROM users" in conn.fetchrow.await_args_list[1].args[0]
    assert "DELETE FROM project_datasources" in conn.execute.await_args.args[0]


@pytest.mark.asyncio
async def test_delete_project_removes_native_kb_datasource_before_project_row():
    conn = AsyncMock()
    # This Project has no saved resources, retained workspace or live execution.
    conn.fetch.return_value = []
    conn.fetchval.return_value = False
    conn.execute.return_value = "DELETE 1"
    db = _make_db(conn)

    assert await db.delete_project(PROJECT_A)

    conn.transaction.assert_called_once()
    statements = [call.args[0] for call in conn.execute.await_args_list]
    native_index = next(
        index
        for index, sql in enumerate(statements)
        if "DELETE FROM datasources" in sql
    )
    project_index = next(
        index for index, sql in enumerate(statements) if "DELETE FROM projects" in sql
    )
    assert native_index < project_index
    native_sql, native_project_id = conn.execute.await_args_list[native_index].args
    assert "type = 'kb'" in native_sql
    assert "WHEN config->>'native_project_id' ~*" in native_sql
    assert "THEN (config->>'native_project_id')::uuid" in native_sql
    assert "END = $1" in native_sql
    assert native_project_id == UUID(PROJECT_A)
    parent_lock_sql, parent_lock_project = conn.fetch.await_args.args
    assert "ORDER BY d.id" in parent_lock_sql
    assert "FOR UPDATE OF d" in parent_lock_sql
    assert parent_lock_project == UUID(PROJECT_A)


@pytest.mark.asyncio
async def test_scoped_datasource_delete_rejects_all_scope_connector():
    conn = AsyncMock()
    conn.fetchrow.return_value = {"scope_mode": "all"}
    conn.fetch.return_value = [{"project_id": UUID(PROJECT_A)}]
    db = _make_db(conn)

    with pytest.raises(DatasourceScopeAuthorizationError):
        await db.delete_datasource(
            DATASOURCE_ID,
            authority_project_scope_id=PROJECT_A,
        )

    conn.transaction.assert_called_once()
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_reconciliation_ack_atomically_requeues_current_authority():
    """A newer worker may ACK/delete the row before an older sync finishes."""
    conn = AsyncMock()
    conn.fetchval.return_value = False
    db = _make_db(conn)

    removed = await db.finish_datasource_project_reconciliation(
        PROJECT_A,
        DATASOURCE_ID,
        7,
    )

    assert removed is False
    sql, project_id, datasource_id, claim_token = conn.fetchval.await_args.args
    assert project_id == UUID(PROJECT_A)
    assert datasource_id == UUID(DATASOURCE_ID)
    assert claim_token == 7
    assert "WITH deleted AS" in sql
    assert "AND claim_token = $3" in sql
    assert "WHERE NOT EXISTS (SELECT 1 FROM deleted)" in sql
    assert "INSERT INTO datasource_project_reconcile_queue" in sql
    assert "datasource_project_reconcile_queue.policy_revision," in sql
    assert "EXCLUDED.policy_revision" in sql
    assert "datasource_project_reconcile_generation_seq" in sql


@pytest.mark.asyncio
async def test_each_reconciliation_claim_rotates_never_reused_token():
    conn = AsyncMock()
    conn.fetch.return_value = []
    db = _make_db(conn)

    await db.claim_datasource_project_reconciliations(
        limit=5,
        lease_seconds=30,
    )

    sql = conn.fetch.await_args.args[0]
    assert "claim_token = nextval(" in sql
    assert "datasource_project_reconcile_generation_seq" in sql
    assert "q.claim_token" in sql


@pytest.mark.asyncio
async def test_stale_failed_reconciliation_atomically_requeues_correction():
    conn = AsyncMock()
    conn.fetchval.return_value = False
    db = _make_db(conn)

    retained = await db.retry_datasource_project_reconciliation(
        PROJECT_A,
        DATASOURCE_ID,
        99,
        safe_error="RuntimeError: datasource knowledge sync failed",
        delay_seconds=20,
    )

    assert retained is False
    sql, _, _, claim_token, delay, safe_error = conn.fetchval.await_args.args
    assert claim_token == 99
    assert delay == 20
    assert safe_error == "RuntimeError: datasource knowledge sync failed"
    assert "WITH retained AS" in sql
    assert "AND claim_token = $3" in sql
    assert "WHERE NOT EXISTS (SELECT 1 FROM retained)" in sql
    assert "datasource_project_reconcile_generation_seq" in sql


@pytest.mark.asyncio
async def test_policy_rows_admit_only_the_exact_legacy_job_binding():
    conn = AsyncMock()
    conn.fetch.return_value = [
        {
            "id": UUID(DATASOURCE_ID),
            "type": "repository",
            "created_by": None,
            "is_global": False,
            "config": {},
            "job_id": UUID(LEGACY_JOB_ID),
            "scope_mode": "all",
            "auto_attach": False,
            "policy_revision": 1,
            "project_ids": [],
        }
    ]
    db = _make_db(conn)

    rows = await db.get_datasource_policy_rows(
        [DATASOURCE_ID],
        legacy_job_id=LEGACY_JOB_ID,
    )

    assert rows[0]["job_id"] == UUID(LEGACY_JOB_ID)
    sql, datasource_ids, legacy_job_id = conn.fetch.await_args.args
    assert "d.job_id IS NULL OR d.job_id = $2::uuid" in sql
    assert datasource_ids == [UUID(DATASOURCE_ID)]
    assert legacy_job_id == UUID(LEGACY_JOB_ID)


# =============================================================================
# Type rules come from the connector driver specs (slice D1c)
# =============================================================================


def test_the_store_type_rules_come_from_the_driver_specs():
    from orchestrator.database import postgres

    assert postgres._LIVE_DETACH_REFUSED_TYPES == {"credentials"}
    assert postgres._DELETE_WHILE_ATTACHED_REFUSED_TYPES == {"credentials"}
    assert postgres._FORCED_READ_ONLY_TYPES == {"kb"}
    assert postgres._UNATTENDED_SEND_TYPES == {"email"}


@pytest.mark.asyncio
@pytest.mark.parametrize(("ds_type", "read_only"), [("kb", True), ("postgresql", None)])
async def test_initial_project_links_of_a_forced_read_only_driver_are_read_only(
    ds_type, read_only
):
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        _datasource_row(),
        _datasource_row(revision=2, scope="projects"),
    ]
    db = _make_db(conn)

    await db.create_datasource(
        name="Notes",
        ds_type=ds_type,
        scope_mode="projects",
        project_ids=[PROJECT_A],
    )

    link_rows = conn.executemany.await_args.args[1]
    assert [row[2] for row in link_rows] == [read_only]


@pytest.mark.asyncio
async def test_only_a_driver_with_unattended_send_is_stamped_with_the_grant():
    db = PostgresDB.__new__(PostgresDB)
    db.get_user = AsyncMock(return_value={"id": USER_ID})
    db.user_can_autonomous_send = AsyncMock(return_value=True)
    config = {"unattended_send": True}
    email = {"type": "email", "created_by": USER_ID, "config": dict(config)}
    other = {"type": "webdav", "created_by": USER_ID, "config": dict(config)}

    await db._stamp_email_autonomous_send([email, other])

    assert email["_owner_can_autonomous_send"] is True
    assert "_owner_can_autonomous_send" not in other
    db.user_can_autonomous_send.assert_awaited_once()
