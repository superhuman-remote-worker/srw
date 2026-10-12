"""A protected session's folder on a real PostgreSQL (migration 0524).

* the startup pass copies a protected thread's selected ``thread_mounts`` row
  under the same id, once;
* the selection check (0186's trigger, 0524's lookup) accepts a
  ``thread_protected_sources`` row, still accepts a legacy ``thread_mounts``
  row, and refuses a selected source whose target differs;
* a revoked->engaging replacement works with a carried-over id;
* a protected source is immutable.
"""

from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.cloud.handles import ProjectFolderHandle
from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)
from tests.cloud.test_backend_instance_registry_real_postgres import _authority

_ROOT = Path(__file__).resolve().parents[1]
_SCHEMA = _ROOT / "src/orchestrator/database/schema_current.sql"
_INSTANCE = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_USER = "11111111-1111-4111-8111-111111111111"
_PROJECT = "22222222-2222-4222-8222-222222222222"
_THREAD = "33333333-3333-4333-8333-333333333333"
_GENERATION = "44444444-4444-4444-8444-444444444444"
_MOUNT = "55555555-5555-4555-8555-555555555555"
_SOURCE = "77777777-7777-4777-8777-777777777777"
_OTHER_MOUNT = "88888888-8888-4888-8888-888888888888"
_FOLDER = "99999999-9999-4999-8999-999999999999"
_HANDLE = ProjectFolderHandle(
    backend="nextcloud", native_id="7", vendor_meta={"mountpoint": "Project Alpha"}
)


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local Postgres container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(_SCHEMA.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE cloud_ro_effect_intents, cloud_ro_mounts, "
            "thread_protected_sources, thread_mounts, threads, projects, users, "
            "main_cloud_active_backend, main_cloud_backend_instances CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()


async def _seed(db, *, protected: bool = True) -> None:
    assert await db.install_initial_main_cloud_backend_instance(
        _authority(
            _INSTANCE,
            remote_identity="installation-a",
            base_url="https://a.internal.example",
        )
    )
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id,display_name) VALUES ($1::uuid,'owner')", _USER
        )
        await conn.execute(
            """
            INSERT INTO projects (
                id, name, main_cloud_backend,
                main_cloud_backend_instance_id, main_cloud_folder_handle
            ) VALUES ($1::uuid,'project','nextcloud',$2::uuid,$3)
            """,
            _PROJECT,
            _INSTANCE,
            _HANDLE.to_db(),
        )
        await conn.execute(
            """
            INSERT INTO threads (
                id,user_id,project_id,status,execution_lane,
                runtime_generation,main_cloud_backend,
                main_cloud_backend_instance_id,metadata
            ) VALUES (
                $1::uuid,$2::uuid,$3::uuid,'created','pinned',
                $4::uuid,'nextcloud',$5::uuid,$6::jsonb
            )
            """,
            _THREAD,
            _USER,
            _PROJECT,
            _GENERATION,
            _INSTANCE,
            '{"protected_cloud": true}' if protected else "{}",
        )


async def _thread_mount(db, mount_id: str, target_path: str = "cloud") -> None:
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO thread_mounts (
                id,thread_id,mount_kind,target_path,source_kind,source_ref,
                backend_id,backend_instance_id,cloud_handle,webdav_url
            ) VALUES (
                $1::uuid,$2::uuid,'project',$6,'project_folder',$3::uuid,
                'nextcloud',$4::uuid,$5,'https://ignored.invalid/dav'
            )
            """,
            mount_id,
            _THREAD,
            _PROJECT,
            _INSTANCE,
            _HANDLE.to_db(),
            target_path,
        )


async def _protected_source(db, source_id: str, target_path: str = "cloud") -> None:
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO thread_protected_sources (
                id, thread_id, datasource_id, mount_kind, source_kind,
                source_ref, target_path, backend_id, backend_instance_id,
                cloud_handle
            ) VALUES ($1::uuid,$2::uuid,$3::uuid,'project','project_folder',
                      $4::uuid,$5,'nextcloud',$6::uuid,$7)
            """,
            source_id,
            _THREAD,
            _FOLDER,
            _PROJECT,
            target_path,
            _INSTANCE,
            _HANDLE.to_db(),
        )


def _plan(attempt: str) -> ProtectedNextcloudReaderGrantPlan:
    return ProtectedNextcloudReaderGrantPlan(
        engage_attempt=attempt,
        backend_instance_id=_INSTANCE,
        source=ProtectedMountSourceIdentity(
            backend_instance_id=_INSTANCE,
            source_ref=_PROJECT,
            target_path="cloud",
            native_id="7",
            mountpoint="Project Alpha",
        ),
    )


async def _install(db, selected: str, attempt: str):
    plan = _plan(attempt)
    installed = await db.install_ro_mount_engage_intent(
        thread_id=_THREAD,
        user_id=_USER,
        selected_mount_id=selected,
        expected_runtime_generation=_GENERATION,
        plan=plan,
        credentials="attempt-secret",
        webdav_url="https://a.internal.example/dav/attempt",
    )
    return installed, plan


async def _activate(db, installed, plan, selected: str) -> bool:
    return await db.activate_ro_mount_attempt_with_baseline(
        installed["id"],
        {"a.txt": "etag-a"},
        thread_id=_THREAD,
        user_id=_USER,
        selected_mount_id=selected,
        expected_runtime_generation=_GENERATION,
        plan=plan,
    )


@pytest.mark.asyncio
async def test_the_startup_pass_copies_the_selected_row_under_its_id_once(db):
    await _seed(db)
    await _thread_mount(db, _MOUNT)
    assert await db.backfill_thread_protected_sources() == 1
    assert await db.backfill_thread_protected_sources() == 0
    source = await db.get_thread_protected_source(_THREAD)
    assert str(source["id"]) == _MOUNT
    assert source["datasource_id"] is None
    assert source["target_path"] == "cloud"
    assert source["cloud_handle"] == _HANDLE.to_db()
    assert [
        str(row["id"]) for row in await db.list_protected_selection_rows(_THREAD)
    ] == [_MOUNT]


@pytest.mark.asyncio
async def test_an_unprotected_thread_is_never_copied(db):
    await _seed(db, protected=False)
    await _thread_mount(db, _MOUNT)
    assert await db.backfill_thread_protected_sources() == 0
    assert await db.get_thread_protected_source(_THREAD) is None
    # Its selection still reads its mounts, as before.
    assert [
        str(row["id"]) for row in await db.list_protected_selection_rows(_THREAD)
    ] == [_MOUNT]


@pytest.mark.asyncio
async def test_a_connector_sourced_folder_engages_and_activates(db):
    await _seed(db)
    await _protected_source(db, _SOURCE)
    installed, plan = await _install(
        db, _SOURCE, "66666666-6666-4666-8666-666666666666"
    )
    assert installed is not None
    assert await _activate(db, installed, plan, _SOURCE)
    assert (await db.get_ro_mount_by_thread(_THREAD))["status"] == "active"


@pytest.mark.asyncio
async def test_a_legacy_thread_mounts_selection_still_engages(db):
    await _seed(db)
    await _thread_mount(db, _MOUNT)
    installed, plan = await _install(db, _MOUNT, "66666666-6666-4666-8666-666666666666")
    assert installed is not None
    assert await _activate(db, installed, plan, _MOUNT)


@pytest.mark.asyncio
async def test_a_selected_source_that_differs_or_is_missing_is_refused(db):
    await _seed(db)
    await _protected_source(db, _SOURCE, target_path="elsewhere")
    await _thread_mount(db, _OTHER_MOUNT, target_path="elsewhere")
    # The plan protects "cloud": neither the source row nor the legacy
    # mount row names it, and an id naming nothing is refused too.
    for selected in (_SOURCE, _OTHER_MOUNT, "12121212-1212-4212-8212-121212121212"):
        installed, _plan = await _install(
            db, selected, "66666666-6666-4666-8666-666666666666"
        )
        assert installed is None
    assert await db.get_ro_mount_by_thread(_THREAD) is None


@pytest.mark.asyncio
async def test_a_replacement_engages_under_a_carried_over_id(db):
    await _seed(db)
    await _thread_mount(db, _MOUNT)
    installed, plan = await _install(db, _MOUNT, "66666666-6666-4666-8666-666666666666")
    assert await _activate(db, installed, plan, _MOUNT)
    assert await db.begin_ro_mount_revocation_if_matches(
        installed["id"],
        expected_thread_id=_THREAD,
        expected_runtime_generation=_GENERATION,
        plan=plan,
    )
    assert await db.finish_ro_mount_revocation_if_matches(
        installed["id"],
        expected_thread_id=_THREAD,
        expected_runtime_generation=_GENERATION,
        plan=plan,
    )
    # 0524 (or the startup pass) carries the row over; the old row goes.
    assert await db.backfill_thread_protected_sources() == 1
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM thread_mounts WHERE id=$1::uuid", _MOUNT)
    replaced, replaced_plan = await _install(
        db, _MOUNT, "67676767-6767-4767-8767-676767676767"
    )
    assert replaced is not None
    assert await _activate(db, replaced, replaced_plan, _MOUNT)


@pytest.mark.asyncio
async def test_a_protected_source_is_immutable(db):
    await _seed(db)
    await _protected_source(db, _SOURCE)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as refused:
            await conn.execute(
                "UPDATE thread_protected_sources SET target_path='moved' "
                "WHERE id=$1::uuid",
                _SOURCE,
            )
    assert refused.value.constraint_name == "thread_protected_sources_immutable"
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                """
                INSERT INTO thread_protected_sources (
                    id, thread_id, mount_kind, source_kind, source_ref,
                    target_path, backend_id, backend_instance_id, cloud_handle
                ) VALUES (gen_random_uuid(), $1::uuid, 'project',
                          'project_folder', $2::uuid, 'cloud', 'nextcloud',
                          $3::uuid, 'h')
                """,
                _THREAD,
                _PROJECT,
                _INSTANCE,
            )
