"""Native non-quota End must preserve an exact retained-Resume predecessor."""

import json
from pathlib import Path

import asyncpg
import pytest_asyncio

import pytest
from tests.test_vm_nonquota_readiness_real_postgres import prepared
from testcontainers.postgres import PostgresContainer

from orchestrator.services.vm_provisioner import VMTeardownIdentity
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    acquire_pinned_thread_retirement_cleanup_permit,
    prepare_vm_cleanup_resource,
)
from tests.test_vm_thread_adopted_without_quotas_delete_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    adopted_source,
    cleaned_retirement,
    db,  # noqa: F401
    setup,  # noqa: F401
    thread_schema as _thread_schema,  # noqa: F401 - pytest fixture registration
)


@pytest.fixture(scope="module")
def pg_dsn():
    container = (
        PostgresContainer("postgres:15")
        .with_kwargs(mem_limit="1g", nano_cpus=2_000_000_000, pids_limit=128)
        .with_command(
            "postgres -c fsync=off -c synchronous_commit=off "
            "-c full_page_writes=off -c max_connections=32"
        )
    )
    container.start()
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def thread_schema(pg_dsn, _thread_schema):  # noqa: F811 - fixture dependency
    migration = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0323_nonquota_retained_vm_resume.sql"
    )
    if migration.exists():
        conn = await asyncpg.connect(pg_dsn)
        try:
            if not await conn.fetchval(
                "SELECT to_regprocedure('public.valid_vm_thread_nonquota_creation(public.vm_creation_retries)') IS NOT NULL"
            ):
                await conn.execute(migration.read_text())
        finally:
            await conn.close()


@pytest.mark.asyncio
async def test_nonquota_soft_end_requires_exact_controller_stop_before_completion(
    db,  # noqa: F811 - imported fixture
    setup,  # noqa: F811 - imported fixture
    monkeypatch,  # noqa: F811 - imported PostgreSQL/controller fixtures
):
    current, source = await adopted_source(db, setup, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(
        str(current["id"]), permanent=False
    )
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    vm = retirement["context"]["vm"]
    recovery = VMWorkspaceRecoveryStore(db)
    permit = await acquire_pinned_thread_retirement_cleanup_permit(
        recovery,
        thread_id=current["id"],
        identity=VMTeardownIdentity(
            provision_generation=vm["provision_generation"],
            vm_uid=vm["vm_uid"],
            rootdisk_pvc_uid=vm["rootdisk_pvc_uid"],
        ),
        purge_disk=False,
    )
    assert permit.allowed
    candidate = await prepare_vm_cleanup_resource(recovery, permit)
    assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == 0
    assert await db.fetchval(
        "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
        permit.admission_id,
    )
    assert candidate is not None, (
        "non-quota soft End bypasses exact controller stop capture"
    )
    assert candidate["vm_uid"] == str(source["observed_vm_uid"])
    assert candidate["pvc_uid"] == str(source["observed_pvc_uid"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rotated", [False, True], ids=["original-actor", "confirmed-successor"]
)
async def test_native_nonquota_end_resume_records_retained_operation_without_kept_marker(
    db,  # noqa: F811 - imported fixture
    setup,  # noqa: F811 - imported fixture
    monkeypatch,
    rotated,
):
    current, source, _, _ = await prepared(db, setup, monkeypatch, rotated)
    retirement = await cleaned_retirement(db, current, permanent=False)
    assert await db.settle_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        final_status="ended",
    )
    # The shared fake stops at the CAS's retiring projection. Production's
    # completed VM delete then publishes deleted under the same generation.
    assert await db.merge_thread_vm_context_if_provision_generation(
        str(current["id"]), str(source["provision_generation"]), {"status": "deleted"}
    )
    ended = await db.get_thread(str(current["id"]))
    vm = json.loads(ended["metadata"])["vm"]
    assert vm["status"] == "deleted" and vm.get("rootdisk") is None
    assert await db.resume_thread(str(current["id"]))
    resumed = await db.get_thread(str(current["id"]))
    assert resumed["runtime_generation"] != ended["runtime_generation"]
    assert resumed["agent_id"] is None and resumed["runtime_attach_token"] is None
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source
    )
    assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == 0
    operation = await db.fetchrow(
        "SELECT * FROM vm_thread_retained_resumes WHERE thread_id=$1 AND runtime_generation=$2",
        current["id"],
        resumed["runtime_generation"],
    )
    assert operation is not None, (
        "accepted non-quota Resume has no durable retained-disk operation"
    )
    assert operation["predecessor_runtime_generation"] == ended["runtime_generation"]
