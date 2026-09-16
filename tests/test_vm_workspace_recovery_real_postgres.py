"""Durable VM workspace recovery contracts against PostgreSQL 15."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4
from urllib.parse import urlsplit

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.community.postgres import PostgresContainer

from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from orchestrator.services.vm_workspace_recovery_store import (
    WorkspaceRecoveryControlConflict,
)
from orchestrator.database.postgres import PostgresDB
import shared.worker_queue as worker_queue
from shared.run_queue import reap_expired, unpark_unit
from shared.workspace_recovery import (
    RecoveryAttemptDisposition,
    WorkspaceRecoveryCode,
    WorkspaceRecoveryDisposition,
    workspace_recovery_enabled,
)


SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
OWNER_ID = UUID("00000000-0000-0000-0000-000000000101")


def test_workspace_recovery_disposition_wire_shape() -> None:
    disposition = WorkspaceRecoveryDisposition.hold_committed(
        operation_id=UUID("00000000-0000-0000-0000-000000000001"),
        accepted_lease_token=27,
        hold_lease_token=28,
        code=WorkspaceRecoveryCode.RUNTIME_NOT_READY,
    )

    assert disposition.as_error_detail() == {
        "detail": "VM workspace is temporarily unavailable",
        "code": "workspace_runtime_not_ready",
        "recovery": {
            "version": 1,
            "action": "hold_committed",
            "operation_id": "00000000-0000-0000-0000-000000000001",
            "accepted_lease_token": 27,
            "hold_lease_token": 28,
        },
    }


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_workspace_recovery_feature_flag_accepts_explicit_true_values(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", value)
    assert workspace_recovery_enabled()


def test_workspace_recovery_feature_flag_defaults_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VM_WORKSPACE_RECOVERY_ENABLED", raising=False)
    assert not workspace_recovery_enabled()


@pytest.fixture(scope="module")
def pg_dsn():
    dsn = os.getenv("VM_RECOVERY_TEST_DSN")
    if dsn:
        if "test" not in urlsplit(dsn).path.rsplit("/", 1)[-1].lower():
            pytest.fail("VM_RECOVERY_TEST_DSN must name a disposable test database")
        yield dsn
        return
    try:
        with PostgresContainer("postgres:15") as postgres:
            yield postgres.get_connection_url().replace(
                "postgresql+psycopg2", "postgresql"
            )
    except Exception as exc:
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        if os.getenv("VM_RECOVERY_TEST_DSN"):
            await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def app_pg(pg_dsn: str, _schema_applied: None):
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE vm_workspace_cleanup_admissions, "
            "vm_workspace_recovery_requests, "
            "vm_workspace_recovery_probe_slots, "
            "vm_workspace_recovery_stop_receipts, "
            "vm_workspace_recovery_retention_pins, worker_batch_attempts, "
            "vm_workspace_recovery_jobs, vm_workspace_recoveries, "
            "run_queue, jobs CASCADE"
        )
    try:
        yield pool
    finally:
        await pool.close()


async def insert_recovery(
    app_pg,
    *,
    owner_id: UUID = OWNER_ID,
    phase: str = "recovering",
    first_observed_offset: timedelta = timedelta(0),
    deadline_offset: timedelta = timedelta(minutes=15),
) -> UUID:
    async with app_pg.acquire() as conn:
        first = await conn.fetchval("SELECT clock_timestamp()")
        first += first_observed_offset
        return await conn.fetchval(
            """
            INSERT INTO vm_workspace_recoveries (
                owner_kind, owner_id, workspace_contract_digest,
                provision_generation, cluster_name, namespace, vm_uid,
                prior_vmi_uid, prior_launcher_uid, root_pvc_uid, phase,
                first_observed_at, deadline_at, next_check_at, reason_code
            ) VALUES (
                'job', $1, 'sha256:contract', $2, 'test-cluster', 'workers', $3,
                $4, $5, $6, $7, $8, $9, $8, 'workspace_runtime_not_ready'
            ) RETURNING id
            """,
            owner_id,
            uuid4(),
            uuid4(),
            uuid4(),
            uuid4(),
            uuid4(),
            phase,
            first,
            first + deadline_offset,
        )


async def insert_leased_job(
    app_pg, *, include_attempt: bool = True
) -> tuple[UUID, int]:
    job_id = uuid4()
    lease_token = 27
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane) "
            "VALUES ($1, 'admit recovery', 'processing', 'stateless')",
            job_id,
        )
        await conn.execute(
            """
            INSERT INTO run_queue (
                unit_id, unit_kind, state, lease_token, leased_by, leased_until,
                input_seq, consumed_seq, attempts_since_completion
            ) VALUES ($1, 'worker_batch', 'leased', $2, 'worker-a',
                      clock_timestamp() + interval '1 minute', 4, 3, 2)
            """,
            job_id,
            lease_token,
        )
        if include_attempt:
            await conn.execute(
                "INSERT INTO worker_batch_attempts "
                "(job_id, lease_token, claimed_attempt) VALUES ($1, $2, 2)",
                job_id,
                lease_token,
            )
    return job_id, lease_token


@pytest.mark.asyncio
async def test_recovery_hold_and_cleanup_admission_have_one_winner(app_pg) -> None:
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="test-worker")
    job_id, lease_token = await insert_leased_job(app_pg)
    kwargs = admission_kwargs(job_id, lease_token)
    cleanup_request_id = uuid4()
    cleanup = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=job_id,
        pvc_uid=kwargs["root_pvc_uid"],
        request_id=cleanup_request_id,
        source="terminal_cleanup",
        intent_digest="sha256:terminal-cleanup",
    )
    assert cleanup.allowed
    cleanup_replay = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=job_id,
        pvc_uid=kwargs["root_pvc_uid"],
        request_id=cleanup_request_id,
        source="terminal_cleanup",
        intent_digest="sha256:terminal-cleanup",
    )
    assert cleanup_replay.allowed
    assert cleanup_replay.admission_id == cleanup.admission_id

    competing_cleanup = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=job_id,
        pvc_uid=kwargs["root_pvc_uid"],
        request_id=uuid4(),
        source="terminal_cleanup_retry",
        intent_digest="sha256:terminal-cleanup-retry",
    )
    assert not competing_cleanup.allowed
    assert competing_cleanup.reason == "workspace_cleanup_already_admitted"

    with pytest.raises(
        WorkspaceRecoveryControlConflict,
        match="crossed its admission boundary",
    ):
        await store.admit_hold(**kwargs)

    assert await store.complete_cleanup_permit(
        cleanup.admission_id, outcome="not_started"
    )
    disposition = await store.admit_hold(**kwargs)
    assert disposition.operation_id is not None
    blocked = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=job_id,
        pvc_uid=kwargs["root_pvc_uid"],
        request_id=uuid4(),
        source="terminal_cleanup",
        intent_digest="sha256:terminal-cleanup-after-recovery",
    )
    assert not blocked.allowed
    assert blocked.recovery_id == disposition.operation_id


@pytest.mark.asyncio
async def test_cleanup_replay_rejects_changed_resource_intent(app_pg) -> None:
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="test-worker")
    owner_id = uuid4()
    request_id = uuid4()
    first_pvc = uuid4()
    permit = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=owner_id,
        pvc_uid=first_pvc,
        request_id=request_id,
        source="public_vm_delete",
        intent_digest="sha256:public-vm-delete",
    )
    assert permit.allowed

    with pytest.raises(WorkspaceRecoveryControlConflict) as changed_source:
        await store.acquire_cleanup_permit(
            owner_kind="job",
            owner_id=owner_id,
            pvc_uid=first_pvc,
            request_id=request_id,
            source="completion_workspace_teardown",
            intent_digest="sha256:public-vm-delete",
        )
    assert changed_source.value.code == "cleanup_request_id_reused"

    with pytest.raises(WorkspaceRecoveryControlConflict) as changed_pvc:
        await store.acquire_cleanup_permit(
            owner_kind="job",
            owner_id=owner_id,
            pvc_uid=uuid4(),
            request_id=request_id,
            source="public_vm_delete",
            intent_digest="sha256:public-vm-delete",
        )
    assert changed_pvc.value.code == "cleanup_request_id_reused"


@pytest.mark.asyncio
async def test_cleanup_replay_binds_keep_vs_purge_and_returns_completed_outcome(
    app_pg,
) -> None:
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="test-worker")
    owner_id = uuid4()
    request_id = uuid4()
    pvc_uid = uuid4()
    keep_intent = "sha256:keep-exact-vm-and-pvc"
    permit = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=owner_id,
        pvc_uid=pvc_uid,
        request_id=request_id,
        source="lifecycle_vm_delete",
        intent_digest=keep_intent,
    )
    assert permit.allowed and permit.completed_outcome is None

    with pytest.raises(WorkspaceRecoveryControlConflict) as changed_mode:
        await store.acquire_cleanup_permit(
            owner_kind="job",
            owner_id=owner_id,
            pvc_uid=pvc_uid,
            request_id=request_id,
            source="lifecycle_vm_delete",
            intent_digest="sha256:purge-exact-vm-and-pvc",
        )
    assert changed_mode.value.code == "cleanup_request_id_reused"

    assert await store.complete_cleanup_permit(permit.admission_id, outcome="completed")
    replay = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=owner_id,
        pvc_uid=pvc_uid,
        request_id=request_id,
        source="lifecycle_vm_delete",
        intent_digest=keep_intent,
    )
    assert replay.allowed
    assert replay.admission_id == permit.admission_id
    assert replay.completed_outcome == "completed"
    assert replay.reason == "cleanup_request_already_completed"


@pytest.mark.asyncio
async def test_parent_cleanup_blocks_stale_child_owner_recovery_without_pvc(
    app_pg,
) -> None:
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="test-worker")
    child_id, lease_token = await insert_leased_job(app_pg)
    parent_id = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id,description,status,execution_lane) "
            "VALUES ($1,'canonical owner','paused','pinned')",
            parent_id,
        )
        await conn.execute(
            "UPDATE jobs SET parent_job_id=$2,context=jsonb_build_object("
            "'inherits_parent_workspace',true) WHERE id=$1",
            child_id,
            parent_id,
        )
    cleanup = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=parent_id,
        pvc_uid=None,
        request_id=uuid4(),
        source="parent_cleanup",
        intent_digest="sha256:parent-cleanup",
    )
    assert cleanup.allowed
    kwargs = admission_kwargs(child_id, lease_token)
    kwargs.update(owner_id=child_id, root_pvc_uid=None)

    with pytest.raises(WorkspaceRecoveryControlConflict) as refused:
        await store.admit_hold(**kwargs)

    assert refused.value.code == "workspace_cleanup_already_admitted"


@pytest.mark.asyncio
async def test_retry_transfers_every_hold_and_pin_atomically_and_replays(
    app_pg,
) -> None:
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="test-worker")
    job_id, lease_token = await insert_leased_job(app_pg)
    kwargs = admission_kwargs(job_id, lease_token)
    kwargs["code"] = WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
    original = await store.admit_hold(**kwargs)
    request_id = uuid4()

    first = await store.retry_paused(
        job_id=job_id,
        operation_id=original.operation_id,
        request_id=request_id,
        actor_kind="user",
        actor_id="operator",
    )
    replay = await store.retry_paused(
        job_id=job_id,
        operation_id=original.operation_id,
        request_id=request_id,
        actor_kind="user",
        actor_id="operator",
    )

    assert replay == first
    successor = UUID(first["operation_id"])
    async with app_pg.acquire() as conn:
        old = await conn.fetchrow(
            "SELECT phase,superseded_by FROM vm_workspace_recoveries WHERE id=$1",
            original.operation_id,
        )
        open_rows = await conn.fetch(
            "SELECT recovery_id,hold_lease_token,participation "
            "FROM vm_workspace_recovery_jobs "
            "WHERE job_id=$1 AND resolved_at IS NULL",
            job_id,
        )
        pins = await conn.fetch(
            "SELECT recovery_id,released_at FROM vm_workspace_recovery_retention_pins "
            "WHERE recovery_id=ANY($1::uuid[]) ORDER BY recovery_id",
            [original.operation_id, successor],
        )
    assert old["phase"] == "superseded" and old["superseded_by"] == successor
    assert [
        (row["recovery_id"], row["hold_lease_token"], row["participation"])
        for row in open_rows
    ] == [
        (successor, original.hold_lease_token, "held")
    ]
    assert sum(pin["released_at"] is None for pin in pins) == 1
    assert await store.claim_due(successor) is not None


@pytest.mark.asyncio
async def test_concurrent_exact_retry_rechecks_receipt_after_owner_lock(app_pg) -> None:
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="test-worker")
    job_id, lease_token = await insert_leased_job(app_pg)
    kwargs = admission_kwargs(job_id, lease_token)
    kwargs["code"] = WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
    original = await store.admit_hold(**kwargs)
    request_id = uuid4()

    async with app_pg.acquire() as blocker:
        async with blocker.transaction():
            await blocker.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"workspace-recovery:job:{job_id}",
            )
            first = asyncio.create_task(
                store.retry_paused(
                    job_id=job_id,
                    operation_id=original.operation_id,
                    request_id=request_id,
                    actor_kind="user",
                    actor_id="operator",
                )
            )
            second = asyncio.create_task(
                store.retry_paused(
                    job_id=job_id,
                    operation_id=original.operation_id,
                    request_id=request_id,
                    actor_kind="user",
                    actor_id="operator",
                )
            )
            await asyncio.sleep(0.1)
        results = await asyncio.gather(first, second)

    assert results[0] == results[1]


@pytest.mark.asyncio
async def test_child_only_retry_is_refused_without_changing_hold(app_pg) -> None:
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="test-worker")
    job_id, lease_token = await insert_leased_job(app_pg)
    kwargs = admission_kwargs(job_id, lease_token)
    kwargs["code"] = WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
    original = await store.admit_hold(**kwargs)

    with pytest.raises(WorkspaceRecoveryControlConflict) as refused:
        await store.retry_paused(
            job_id=uuid4(),
            operation_id=original.operation_id,
            request_id=uuid4(),
            actor_kind="user",
            actor_id="child-owner",
        )

    assert refused.value.code == "workspace_recovery_owner_required"
    assert (await store.unresolved_participation(job_id))[
        "operation_id"
    ] == original.operation_id


@pytest.mark.asyncio
async def test_cancel_resolves_only_requesting_recovery_participant(app_pg) -> None:
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="test-worker")
    owner_id, lease_token = await insert_leased_job(app_pg)
    kwargs = admission_kwargs(owner_id, lease_token)
    kwargs["code"] = WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
    recovery = await store.admit_hold(**kwargs)
    child_id = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id,description,status,execution_lane,freeze_data) "
            "VALUES ($1,'shared child','paused','stateless',"
            "jsonb_build_object('freeze_type','workspace_recovery','recovery_id',$2::text,'hold_lease_token',1))",
            child_id,
            str(recovery.operation_id),
        )
        await conn.execute(
            "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token,run_after,park_reason,parked_at) "
            "VALUES ($1,'worker_batch','parked',1,'infinity','workspace_recovery',clock_timestamp())",
            child_id,
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_jobs (recovery_id,job_id,hold_lease_token,"
            "prior_queue_state,prior_job_status,participation) "
            "VALUES ($1,$2,1,'queued','created','attention')",
            recovery.operation_id,
            child_id,
        )
    db = PostgresDB.__new__(PostgresDB)
    db.acquire = app_pg.acquire

    cancelled, _ = await db.cancel_stateless_job(str(owner_id))

    assert cancelled
    async with app_pg.acquire() as conn:
        rows = await conn.fetch(
            "SELECT job_id,participation,resolved_at FROM vm_workspace_recovery_jobs "
            "WHERE recovery_id=$1 ORDER BY job_id",
            recovery.operation_id,
        )
        operation = await conn.fetchrow(
            "SELECT phase,resolved_at FROM vm_workspace_recoveries WHERE id=$1",
            recovery.operation_id,
        )
        pin_released = await conn.fetchval(
            "SELECT released_at IS NOT NULL FROM vm_workspace_recovery_retention_pins "
            "WHERE recovery_id=$1",
            recovery.operation_id,
        )
    by_job = {row["job_id"]: row for row in rows}
    assert by_job[owner_id]["participation"] == "cancelled"
    assert by_job[owner_id]["resolved_at"] is not None
    assert by_job[child_id]["resolved_at"] is None
    assert operation["phase"] == "paused_attention" and operation["resolved_at"] is None
    assert pin_released is False


@pytest.mark.asyncio
async def test_pinned_cancel_resolves_only_requesting_recovery_participant(
    app_pg,
) -> None:
    recovery_id = await insert_recovery(app_pg, phase="paused_attention")
    pinned_id = OWNER_ID
    other_id = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id,description,status,execution_lane,freeze_data) VALUES "
            "($1,'pinned participant','paused','pinned',jsonb_build_object("
            "'freeze_type','workspace_recovery','recovery_id',$3::text)),"
            "($2,'other participant','paused','pinned',jsonb_build_object("
            "'freeze_type','workspace_recovery','recovery_id',$3::text))",
            pinned_id,
            other_id,
            str(recovery_id),
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_jobs "
            "(recovery_id,job_id,prior_queue_state,prior_job_status,participation) VALUES "
            "($1,$2,'non_worker','processing','attention'),"
            "($1,$3,'non_worker','processing','attention')",
            recovery_id,
            pinned_id,
            other_id,
        )
        recovery = await conn.fetchrow(
            "SELECT root_pvc_uid,provision_generation FROM vm_workspace_recoveries WHERE id=$1",
            recovery_id,
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_retention_pins "
            "(recovery_id,pvc_uid,provision_generation) VALUES ($1,$2,$3)",
            recovery_id,
            recovery["root_pvc_uid"],
            recovery["provision_generation"],
        )
    db = PostgresDB.__new__(PostgresDB)
    db.acquire = app_pg.acquire

    assert await db.linearize_pinned_cancel(str(pinned_id), expected_status="paused")

    async with app_pg.acquire() as conn:
        rows = await conn.fetch(
            "SELECT job_id,participation,resolved_at FROM vm_workspace_recovery_jobs "
            "WHERE recovery_id=$1 ORDER BY job_id",
            recovery_id,
        )
        operation = await conn.fetchrow(
            "SELECT phase,resolved_at FROM vm_workspace_recoveries WHERE id=$1",
            recovery_id,
        )
        pin_released = await conn.fetchval(
            "SELECT released_at IS NOT NULL FROM vm_workspace_recovery_retention_pins "
            "WHERE recovery_id=$1",
            recovery_id,
        )
    by_job = {row["job_id"]: row for row in rows}
    assert by_job[pinned_id]["participation"] == "cancelled"
    assert by_job[pinned_id]["resolved_at"] is not None
    assert by_job[other_id]["resolved_at"] is None
    assert operation["phase"] == "paused_attention" and operation["resolved_at"] is None
    assert pin_released is False


@pytest.mark.asyncio
async def test_concurrent_final_pinned_cancellations_resolve_operation_once(
    app_pg,
) -> None:
    recovery_id = await insert_recovery(app_pg, phase="paused_attention")
    first_id = OWNER_ID
    second_id = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id,description,status,execution_lane,freeze_data) VALUES "
            "($1,'first participant','paused','pinned',jsonb_build_object("
            "'freeze_type','workspace_recovery','recovery_id',$3::text)),"
            "($2,'second participant','paused','pinned',jsonb_build_object("
            "'freeze_type','workspace_recovery','recovery_id',$3::text))",
            first_id,
            second_id,
            str(recovery_id),
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_jobs "
            "(recovery_id,job_id,prior_queue_state,prior_job_status,participation) VALUES "
            "($1,$2,'non_worker','processing','attention'),"
            "($1,$3,'non_worker','processing','attention')",
            recovery_id,
            first_id,
            second_id,
        )
        recovery = await conn.fetchrow(
            "SELECT root_pvc_uid,provision_generation FROM vm_workspace_recoveries WHERE id=$1",
            recovery_id,
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_retention_pins "
            "(recovery_id,pvc_uid,provision_generation) VALUES ($1,$2,$3)",
            recovery_id,
            recovery["root_pvc_uid"],
            recovery["provision_generation"],
        )
    remaining_checks = 0
    both_remaining_checks = asyncio.Event()

    class BarrierConnection:
        def __init__(self, conn):
            self._conn = conn

        def __getattr__(self, name):
            return getattr(self._conn, name)

        async def fetchval(self, query, *args):
            nonlocal remaining_checks
            if (
                "SELECT EXISTS" in query
                and "vm_workspace_recovery_jobs" in query
                and "resolved_at IS NULL" in query
            ):
                remaining_checks += 1
                if remaining_checks == 2:
                    both_remaining_checks.set()
                try:
                    await asyncio.wait_for(both_remaining_checks.wait(), timeout=0.25)
                except TimeoutError:
                    # The fixed implementation serializes on the operation row,
                    # so the second transaction cannot reach this query until
                    # the first one commits. Let that serialized caller proceed.
                    pass
            return await self._conn.fetchval(query, *args)

    @asynccontextmanager
    async def acquire():
        async with app_pg.acquire() as conn:
            yield BarrierConnection(conn)

    db = PostgresDB.__new__(PostgresDB)
    db.acquire = acquire

    results = await asyncio.gather(
        db.linearize_pinned_cancel(str(first_id), expected_status="paused"),
        db.linearize_pinned_cancel(str(second_id), expected_status="paused"),
    )

    assert results == [True, True]
    async with app_pg.acquire() as conn:
        operation = await conn.fetchrow(
            "SELECT phase,resolved_at,version FROM vm_workspace_recoveries WHERE id=$1",
            recovery_id,
        )
        unresolved = await conn.fetchval(
            "SELECT count(*) FROM vm_workspace_recovery_jobs "
            "WHERE recovery_id=$1 AND resolved_at IS NULL",
            recovery_id,
        )
        released_pins = await conn.fetchval(
            "SELECT count(*) FROM vm_workspace_recovery_retention_pins "
            "WHERE recovery_id=$1 AND released_at IS NOT NULL",
            recovery_id,
        )
    assert unresolved == 0
    assert operation["phase"] == "cancelled"
    assert operation["resolved_at"] is not None
    assert operation["version"] == 2
    assert released_pins == 1


def admission_kwargs(
    job_id: UUID, lease_token: int, *, request_id: UUID | None = None
) -> dict:
    return {
        "job_id": job_id,
        "accepted_lease_token": lease_token,
        "owner_kind": "job",
        "owner_id": job_id,
        "workspace_contract_digest": "sha256:workspace",
        "provision_generation": uuid4(),
        "cluster_name": "test-cluster",
        "namespace": "workers",
        "vm_uid": uuid4(),
        "prior_vmi_uid": uuid4(),
        "prior_launcher_uid": uuid4(),
        "root_pvc_uid": uuid4(),
        "code": WorkspaceRecoveryCode.RUNTIME_NOT_READY,
        "request_id": request_id or uuid4(),
        "actor_kind": "worker",
        "actor_id": "worker-a",
        "intent_digest": "sha256:intent",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing",
    [
        "prior_vmi_uid",
        "prior_launcher_uid",
        "vm_uid",
        "root_pvc_uid",
        "provision_generation",
        "namespace",
    ],
)
async def test_missing_captured_vmi_identity_commits_attention_hold(app_pg, missing):
    job_id, token = await insert_leased_job(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg)
    admitted = await store.admit_hold(
        **(admission_kwargs(job_id, token) | {missing: None})
    )
    assert admitted.action == "paused_attention"
    assert admitted.code == WorkspaceRecoveryCode.IDENTITY_CONFLICT
    async with app_pg.acquire() as conn:
        assert (
            await conn.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", job_id)
            == "parked"
        )
        operation = await conn.fetchrow(
            "SELECT * FROM vm_workspace_recoveries WHERE id=$1", admitted.operation_id
        )
        assert operation[missing] is None
        assert (
            missing
            in json.loads(operation["latest_diagnostic"])["missing_identity_fields"]
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_workspace_recoveries SET phase='recovering' WHERE id=$1",
                admitted.operation_id,
            )
    assert await store.claim_due(admitted.operation_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_attempt", [False, True])
async def test_reporter_and_active_sibling_replay_exact_committed_hold(
    app_pg, missing_attempt
):
    from types import SimpleNamespace
    from orchestrator.services.unit_claim_bundle import (
        get_workspace_recovery_disposition,
    )

    parent_id, token = await insert_leased_job(
        app_pg, include_attempt=not missing_attempt
    )
    child_id, child_token = await insert_leased_job(
        app_pg, include_attempt=not missing_attempt
    )
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET parent_job_id=$2, context='{"
            + '"inherits_parent_workspace":true'
            + "}'::jsonb WHERE id=$1",
            child_id,
            parent_id,
        )
    store = VMWorkspaceRecoveryStore(app_pg)
    admitted = await store.admit_hold(**admission_kwargs(parent_id, token))
    dependencies = SimpleNamespace(db=app_pg, recovery_store=store)
    for job_id, accepted_token in ((parent_id, token), (child_id, child_token)):
        result = await get_workspace_recovery_disposition(
            unit_id=str(job_id), lease_token=accepted_token, dependencies=dependencies
        )
        assert result is not None
        assert result.operation_id == admitted.operation_id
        assert result.accepted_lease_token == accepted_token
        assert result.hold_lease_token == accepted_token + 1
        assert result.action == (
            "paused_attention" if missing_attempt else "hold_committed"
        )
    async with app_pg.acquire() as conn:
        attempt = await store.get_attempt_disposition(
            conn, job_id=child_id, lease_token=child_token
        )
        if not missing_attempt:
            assert attempt.disposition["action"] == "hold_committed"
        else:
            assert attempt is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        WorkspaceRecoveryCode.IDENTITY_CONFLICT,
        WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN,
        WorkspaceRecoveryCode.CHECKPOINT_UNAVAILABLE,
    ],
)
async def test_worker_uncertainty_codes_require_attention(app_pg, code):
    job_id, token = await insert_leased_job(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg)
    result = await store.admit_hold(
        **(admission_kwargs(job_id, token) | {"code": code})
    )
    assert result.action == "paused_attention"
    assert result.code == code
    async with app_pg.acquire() as conn:
        diagnostic = await conn.fetchval(
            "SELECT latest_diagnostic FROM vm_workspace_recoveries WHERE id=$1",
            result.operation_id,
        )
        assert json.loads(diagnostic)["reason"] == code.value
    assert await store.claim_due(result.operation_id) is None


@pytest.mark.asyncio
async def test_disabled_bundle_then_enabled_hold_never_refunds_executable_attempt(
    app_pg, monkeypatch
):
    import httpx
    from orchestrator import main as orch_main
    from tests.test_claim_bundle import (
        recovery_protocol_app,
        UNIT_ID,
        POD_NAME,
        POD_UID,
    )

    app, db, _ = recovery_protocol_app(monkeypatch, ready=True)
    db.acquire = app_pg.acquire
    store = VMWorkspaceRecoveryStore(app_pg)
    monkeypatch.setattr(orch_main, "VMWorkspaceRecoveryStore", lambda db: store)
    job_id = UUID(UNIT_ID)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id,description,status,execution_lane,context,config_override) VALUES ($1,'disabled bundle','processing','stateless',$2::jsonb,$3::jsonb)",
            job_id,
            json.dumps(db._job["context"]),
            json.dumps(db._job["config_override"]),
        )
        await conn.execute(
            "INSERT INTO run_queue (unit_id,unit_kind,state,lease_token,leased_by,leased_until,attempts_since_completion) VALUES ($1,'worker_batch','leased',7,$2,clock_timestamp()+interval '1 minute',1)",
            job_id,
            POD_NAME,
        )
        await conn.execute(
            "INSERT INTO worker_batch_attempts (job_id,lease_token,claimed_attempt) VALUES ($1,7,1)",
            job_id,
        )
        await conn.execute(
            "INSERT INTO agents (config_name,hostname,pod_uid) VALUES ('test',$1,$2)",
            POD_NAME,
            POD_UID,
        )
    monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "false")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        bundle = await client.get(
            f"/internal/units/{UNIT_ID}/claim-bundle",
            params={"lease_token": 7, "pod_name": POD_NAME, "pod_uid": POD_UID},
        )
        assert bundle.status_code == 200
        monkeypatch.setenv("VM_WORKSPACE_RECOVERY_ENABLED", "true")
        hold = await client.post(
            f"/internal/units/{UNIT_ID}/workspace-recovery",
            json={
                "lease_token": 7,
                "pod_name": POD_NAME,
                "pod_uid": POD_UID,
                "request_id": str(uuid4()),
                "code": "workspace_transport_unavailable",
            },
        )
        assert hold.status_code == 200
        assert hold.json()["recovery"]["action"] == "hold_committed"
    async with app_pg.acquire() as conn:
        attempt = await conn.fetchrow(
            "SELECT bundle_authorized_at,refunded_at FROM worker_batch_attempts WHERE job_id=$1 AND lease_token=7",
            job_id,
        )
        assert attempt["bundle_authorized_at"] is not None
        assert attempt["refunded_at"] is None
        assert (
            await conn.fetchval(
                "SELECT attempts_since_completion FROM run_queue WHERE unit_id=$1",
                job_id,
            )
            == 1
        )


@pytest.mark.asyncio
async def test_schema_allows_only_one_unresolved_recovery_per_owner(app_pg) -> None:
    first = await insert_recovery(app_pg)
    with pytest.raises(asyncpg.UniqueViolationError):
        await insert_recovery(app_pg)

    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET resolved_at=clock_timestamp(), "
            "phase='recovered' WHERE id=$1",
            first,
        )
    assert await insert_recovery(app_pg) != first


@pytest.mark.asyncio
async def test_schema_rejects_unknown_phases_and_extended_deadlines(app_pg) -> None:
    with pytest.raises(asyncpg.CheckViolationError):
        await insert_recovery(app_pg, phase="invented")
    with pytest.raises(asyncpg.CheckViolationError):
        await insert_recovery(app_pg, deadline_offset=timedelta(minutes=16))
    recovery_id = await insert_recovery(app_pg)
    async with app_pg.acquire() as conn:
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE vm_workspace_recoveries "
                "SET deadline_at=deadline_at + interval '1 second' WHERE id=$1",
                recovery_id,
            )


@pytest.mark.asyncio
async def test_claim_due_enforces_four_global_durable_probe_slots(app_pg) -> None:
    recovery_ids = [
        await insert_recovery(app_pg, owner_id=uuid4()) for _ in range(5)
    ]
    stores = [
        VMWorkspaceRecoveryStore(app_pg, worker_id=f"reconciler-{index}")
        for index in range(5)
    ]

    claims = await asyncio.gather(
        *(store.claim_due(operation_id) for store, operation_id in zip(stores, recovery_ids))
    )

    assert sum(item is not None for item in claims) == 4
    async with app_pg.acquire() as conn:
        assert await conn.fetchval(
            "SELECT count(*) FROM vm_workspace_recovery_probe_slots"
        ) == 4
        assert await conn.fetchval(
            "SELECT count(DISTINCT global_slot) FROM vm_workspace_recovery_probe_slots"
        ) == 4


@pytest.mark.asyncio
async def test_claim_due_enforces_one_durable_probe_per_known_node(app_pg) -> None:
    first = await insert_recovery(app_pg, owner_id=uuid4())
    second = await insert_recovery(app_pg, owner_id=uuid4())
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET latest_observation="
            "'{\"successor\":{\"node_uid\":\"node-8\"}}'::jsonb "
            "WHERE id=ANY($1::uuid[])",
            [first, second],
        )

    first_claim, second_claim = await asyncio.gather(
        VMWorkspaceRecoveryStore(app_pg, worker_id="leader-a").claim_due(first),
        VMWorkspaceRecoveryStore(app_pg, worker_id="leader-b").claim_due(second),
    )

    assert (first_claim is None) != (second_claim is None)
    claimed = first_claim or second_claim
    assert claimed is not None and claimed.node_key == "node-8"


@pytest.mark.asyncio
async def test_lost_or_cancelled_claim_cannot_apply_probe_result(app_pg) -> None:
    recovery_id = await insert_recovery(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="leader-a")
    claimed = await store.claim_due(recovery_id)
    assert claimed is not None
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET phase='cancelled', "
            "resolved_at=clock_timestamp(),claimed_by=NULL,claimed_until=NULL,"
            "version=version+1 WHERE id=$1",
            recovery_id,
        )

    assert not await store.claim_is_current(claimed)
    assert not await store.defer_claim(
        operation_id=recovery_id,
        version=claimed.version,
        claim_token=claimed.claim_token,
        phase="waiting_runtime",
        observation={"ready": True},
        next_check_seconds=10,
    )


@pytest.mark.asyncio
async def test_feature_off_pause_preserves_hold_and_clears_probe_slot(app_pg) -> None:
    job_id, lease_token = await insert_leased_job(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="leader-a")
    admitted = await store.admit_hold(**admission_kwargs(job_id, lease_token))
    claimed = await store.claim_due(admitted.operation_id)
    assert claimed is not None

    assert await store.pause_automatic_disabled() == 1

    async with app_pg.acquire() as conn:
        operation = await conn.fetchrow(
            "SELECT phase,resolved_at,latest_diagnostic FROM vm_workspace_recoveries "
            "WHERE id=$1",
            admitted.operation_id,
        )
        queue = await conn.fetchrow(
            "SELECT state,lease_token,park_reason FROM run_queue WHERE unit_id=$1",
            job_id,
        )
        slots = await conn.fetchval(
            "SELECT count(*) FROM vm_workspace_recovery_probe_slots WHERE recovery_id=$1",
            admitted.operation_id,
        )
    diagnostic = operation["latest_diagnostic"]
    if isinstance(diagnostic, str):
        diagnostic = json.loads(diagnostic)
    assert operation["phase"] == "paused_attention"
    assert operation["resolved_at"] is None
    assert diagnostic["reason"] == "automatic_recovery_disabled"
    assert tuple(queue) == ("parked", lease_token + 1, "workspace_recovery")
    assert slots == 0


@pytest.mark.asyncio
async def test_ordinary_readiness_cas_cannot_promote_recovery_owned_job(app_pg) -> None:
    job_id, lease_token = await insert_leased_job(app_pg)
    kwargs = admission_kwargs(job_id, lease_token)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_build_object('vm',jsonb_build_object("
            "'provision_generation',$2::text,'vm_uid',$3::text,"
            "'rootdisk_pvc_uid',$4::text,'status','ssh_pending',"
            "'ssh_registration_id','old-registration')) WHERE id=$1",
            job_id,
            str(kwargs["provision_generation"]),
            str(kwargs["vm_uid"]),
            str(kwargs["root_pvc_uid"]),
        )
    await VMWorkspaceRecoveryStore(app_pg).admit_hold(**kwargs)
    db = PostgresDB.__new__(PostgresDB)
    db.acquire = app_pg.acquire

    assert await db.vm_workspace_recovery_owns_authority("job", str(job_id))
    assert not await db.merge_vm_context_if_current(
        str(job_id),
        "old-registration",
        {"status": "ready", "active_pod_uid": str(uuid4())},
    )
    assert not await db.merge_vm_context_if_provision_generation(
        str(job_id),
        str(kwargs["provision_generation"]),
        {"status": "ready", "active_pod_uid": str(uuid4())},
        require_status_not_ready=True,
    )
    async with app_pg.acquire() as conn:
        assert (
            await conn.fetchval("SELECT context->'vm'->>'status' FROM jobs WHERE id=$1", job_id)
            == "ssh_pending"
        )


@pytest.mark.asyncio
async def test_request_receipts_are_idempotent_per_scope_and_request(app_pg) -> None:
    recovery_id = await insert_recovery(app_pg)
    request_id = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO vm_workspace_recovery_requests
                (scope_kind, scope_id, request_id, actor_kind, actor_id,
                 intent_digest, recovery_id, accepted_result)
            VALUES ('job', $1, $2, 'worker', 'pod-a', 'sha256:first', $3,
                    '{"accepted":true}'::jsonb)
            """,
            OWNER_ID,
            request_id,
            recovery_id,
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                """
                INSERT INTO vm_workspace_recovery_requests
                    (scope_kind, scope_id, request_id, actor_kind, actor_id,
                     intent_digest, recovery_id, accepted_result)
                VALUES ('job', $1, $2, 'worker', 'pod-a', 'sha256:different', $3,
                        '{}'::jsonb)
                """,
                OWNER_ID,
                request_id,
                recovery_id,
            )


@pytest.mark.asyncio
async def test_stop_receipts_are_append_only_and_bind_exact_identity(app_pg) -> None:
    recovery_id = await insert_recovery(app_pg)
    receipt_id = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO vm_workspace_recovery_stop_receipts (
                id, recovery_id, accepted_claim_token, vm_uid, vmi_uid,
                launcher_uid, container_id, root_pvc_uid, controller_identity,
                observed_at, evidence, evidence_digest
            ) VALUES ($1, $2, 1, $3, $4, $5, 'container://old', $6,
                      'controller/test', clock_timestamp(),
                      '{"stopped":true}'::jsonb, 'sha256:evidence')
            """,
            receipt_id,
            recovery_id,
            uuid4(),
            uuid4(),
            uuid4(),
            uuid4(),
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "UPDATE vm_workspace_recovery_stop_receipts "
                "SET evidence_digest='sha256:changed' WHERE id=$1",
                receipt_id,
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await conn.execute(
                "DELETE FROM vm_workspace_recovery_stop_receipts WHERE id=$1",
                receipt_id,
            )


@pytest.mark.asyncio
async def test_probe_slots_and_retention_pins_have_exact_unique_keys(app_pg) -> None:
    recovery_id = await insert_recovery(app_pg)
    global_conflict_recovery_id = await insert_recovery(app_pg, owner_id=uuid4())
    node_conflict_recovery_id = await insert_recovery(app_pg, owner_id=uuid4())
    pvc_uid = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO vm_workspace_recovery_probe_slots
                (recovery_id, global_slot, node_key, claim_token, leased_until)
            VALUES ($1, 1, 'node-a', 1, clock_timestamp() + interval '30 seconds')
            """,
            recovery_id,
        )
        with pytest.raises(asyncpg.UniqueViolationError) as global_conflict:
            await conn.execute(
                """
                INSERT INTO vm_workspace_recovery_probe_slots
                    (recovery_id, global_slot, node_key, claim_token, leased_until)
                VALUES ($1, 1, 'node-b', 1,
                        clock_timestamp() + interval '30 seconds')
                """,
                global_conflict_recovery_id,
            )
        assert (
            global_conflict.value.constraint_name
            == "vm_workspace_recovery_probe_slots_global_slot_key"
        )
        with pytest.raises(asyncpg.UniqueViolationError) as node_conflict:
            await conn.execute(
                """
                INSERT INTO vm_workspace_recovery_probe_slots
                    (recovery_id, global_slot, node_key, claim_token, leased_until)
                VALUES ($1, 2, 'node-a', 1,
                        clock_timestamp() + interval '30 seconds')
                """,
                node_conflict_recovery_id,
            )
        assert (
            node_conflict.value.constraint_name
            == "vm_workspace_recovery_probe_slots_node_key_key"
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_retention_pins "
            "(recovery_id, pvc_uid, provision_generation) VALUES ($1, $2, $3)",
            recovery_id,
            pvc_uid,
            uuid4(),
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "INSERT INTO vm_workspace_recovery_retention_pins "
                "(recovery_id, pvc_uid, provision_generation) VALUES ($1, $2, $3)",
                recovery_id,
                pvc_uid,
                uuid4(),
            )


@pytest.mark.asyncio
async def test_attempt_store_authorization_and_recovery_claim_use_cas(app_pg) -> None:
    recovery_id = await insert_recovery(app_pg)
    async with app_pg.acquire() as conn:
        job_id = await conn.fetchval(
            "INSERT INTO jobs (description, status, execution_lane) "
            "VALUES ('recovery contract', 'processing', 'stateless') RETURNING id"
        )
        await conn.execute(
            """
            INSERT INTO worker_batch_attempts
                (job_id, lease_token, claimed_attempt, recovery_id, disposition)
            VALUES ($1, 27, 3, NULL,
                    '{"code":"workspace_runtime_not_ready", "action":"hold_committed"}'::jsonb)
            """,
            job_id,
        )
        await conn.execute(
            "INSERT INTO run_queue (unit_id, unit_kind, state, lease_token, "
            "leased_by, leased_until) VALUES ($1, 'worker_batch', 'leased', 27, "
            "'pod-a', now()+interval '1 minute')",
            job_id,
        )

    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-a")
    async with app_pg.acquire() as conn:
        assert await store.record_bundle_authorized(
            conn,
            job_id=job_id,
            lease_token=27,
            authority_digest="sha256:authority",
        )
        assert not await store.record_bundle_authorized(
            conn,
            job_id=job_id,
            lease_token=27,
            authority_digest="sha256:other",
        )
        attempt = await store.get_attempt_disposition(
            conn, job_id=job_id, lease_token=27
        )
    assert attempt == RecoveryAttemptDisposition(
        job_id=job_id,
        lease_token=27,
        bundle_authorized=True,
        authority_digest="sha256:authority",
        disposition={
            "code": "workspace_runtime_not_ready",
            "action": "hold_committed",
        },
        recovery_id=None,
        refunded=False,
    )
    assert attempt.requires_recovery_hold

    claim = await store.claim_due(recovery_id, ttl_seconds=30)
    assert claim is not None
    assert claim.operation_id == recovery_id
    assert claim.claim_token == 1
    assert claim.remaining_seconds > 0
    assert not await store.apply_observation(
        operation_id=recovery_id,
        version=claim.version,
        claim_token=claim.claim_token + 1,
        observation={"ready": False},
    )
    assert await store.apply_observation(
        operation_id=recovery_id,
        version=claim.version,
        claim_token=claim.claim_token,
        observation={"ready": False},
    )
    async with app_pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT version, claim_token, latest_observation "
            "FROM vm_workspace_recoveries WHERE id=$1",
            recovery_id,
        )
    assert row["version"] == claim.version + 1
    assert row["claim_token"] == claim.claim_token
    observation = row["latest_observation"]
    if isinstance(observation, str):
        observation = json.loads(observation)
    assert observation == {"ready": False}


@pytest.mark.asyncio
async def test_claim_due_durably_pauses_recovery_found_after_deadline(app_pg) -> None:
    recovery_id = await insert_recovery(
        app_pg, first_observed_offset=-timedelta(minutes=16)
    )
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-after-restart")

    assert await store.claim_due(recovery_id) is None

    async with app_pg.acquire() as conn:
        operation = await conn.fetchrow(
            "SELECT phase, reason_code, claimed_by, claimed_until, resolved_at "
            "FROM vm_workspace_recoveries WHERE id=$1",
            recovery_id,
        )
    assert tuple(operation) == (
        "paused_attention",
        "workspace_recovery_deadline_exceeded",
        None,
        None,
        None,
    )


@pytest.mark.asyncio
async def test_release_after_deadline_retains_hold_and_pauses_attention(app_pg) -> None:
    recovery_id = await insert_recovery(
        app_pg, first_observed_offset=-timedelta(minutes=16)
    )
    job_id = uuid4()
    hold_token = 28
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET claimed_by='reconciler-a', "
            "claimed_until=clock_timestamp()+interval '30 seconds', "
            "claim_token=1, version=2 WHERE id=$1",
            recovery_id,
        )
        await conn.execute(
            """
            INSERT INTO jobs (id, description, status, execution_lane, freeze_data)
            VALUES ($1, 'late probe', 'paused', 'stateless',
                    jsonb_build_object('freeze_type','workspace_recovery',
                                       'recovery_id',$2::text,
                                       'hold_lease_token',$3::bigint))
            """,
            job_id,
            str(recovery_id),
            hold_token,
        )
        await conn.execute(
            """
            INSERT INTO run_queue
                (unit_id, unit_kind, state, lease_token, park_reason, parked_at)
            VALUES ($1, 'worker_batch', 'parked', $2,
                    'workspace_recovery', clock_timestamp())
            """,
            job_id,
            hold_token,
        )
        await conn.execute(
            """
            INSERT INTO vm_workspace_recovery_jobs
                (recovery_id, job_id, accepted_lease_token, hold_lease_token,
                 prior_queue_state, prior_job_status)
            VALUES ($1, $2, 27, $3, 'leased', 'processing')
            """,
            recovery_id,
            job_id,
            hold_token,
        )

    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-a")
    assert not await store.release_recovered(
        operation_id=recovery_id,
        version=2,
        claim_token=1,
        resume_receipt={"kind": "late-ready"},
    )

    async with app_pg.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state, lease_token, park_reason FROM run_queue WHERE unit_id=$1",
            job_id,
        )
        operation = await conn.fetchrow(
            "SELECT phase, reason_code FROM vm_workspace_recoveries WHERE id=$1",
            recovery_id,
        )
    assert tuple(queue) == ("parked", hold_token, "workspace_recovery")
    assert tuple(operation) == (
        "paused_attention",
        "workspace_recovery_deadline_exceeded",
    )


@pytest.mark.asyncio
async def test_admission_without_attempt_evidence_holds_for_attention(app_pg) -> None:
    job_id, lease_token = await insert_leased_job(app_pg, include_attempt=False)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-a")

    disposition = await store.admit_hold(**admission_kwargs(job_id, lease_token))

    assert disposition.action == "paused_attention"
    assert disposition.code is WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
    async with app_pg.acquire() as conn:
        operation = await conn.fetchrow(
            "SELECT phase, reason_code FROM vm_workspace_recoveries WHERE id=$1",
            disposition.operation_id,
        )
        queue = await conn.fetchrow(
            "SELECT state, lease_token, park_reason FROM run_queue WHERE unit_id=$1",
            job_id,
        )
    assert tuple(operation) == ("paused_attention", "tool_outcome_unknown")
    assert tuple(queue) == ("parked", lease_token + 1, "workspace_recovery")


@pytest.mark.parametrize(
    "projection_fault", ["missing", "status", "recovery_id", "hold_token"]
)
@pytest.mark.asyncio
async def test_release_with_changed_job_projection_retains_hold(
    app_pg, projection_fault: str
) -> None:
    job_id, lease_token = await insert_leased_job(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-a")
    admitted = await store.admit_hold(**admission_kwargs(job_id, lease_token))
    claim = await store.claim_due(admitted.operation_id)
    assert claim is not None
    async with app_pg.acquire() as conn:
        if projection_fault == "status":
            await conn.execute("UPDATE jobs SET status='created' WHERE id=$1", job_id)
        elif projection_fault == "missing":
            await conn.execute("UPDATE jobs SET freeze_data=NULL WHERE id=$1", job_id)
        else:
            freeze = {
                "freeze_type": "workspace_recovery",
                "recovery_id": str(admitted.operation_id),
                "hold_lease_token": lease_token + 1,
            }
            if projection_fault == "recovery_id":
                freeze["recovery_id"] = str(uuid4())
            else:
                freeze["hold_lease_token"] = lease_token + 2
            await conn.execute(
                "UPDATE jobs SET freeze_data=$2::jsonb WHERE id=$1",
                job_id,
                json.dumps(freeze),
            )

    assert not await store.release_recovered(
        operation_id=admitted.operation_id,
        version=claim.version,
        claim_token=claim.claim_token,
        resume_receipt={"kind": "same-runtime-ready"},
    )

    async with app_pg.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state, lease_token, park_reason FROM run_queue WHERE unit_id=$1",
            job_id,
        )
        operation = await conn.fetchrow(
            "SELECT phase, reason_code FROM vm_workspace_recoveries WHERE id=$1",
            admitted.operation_id,
        )
        participant = await conn.fetchrow(
            "SELECT participation, resolved_at FROM vm_workspace_recovery_jobs "
            "WHERE recovery_id=$1 AND job_id=$2",
            admitted.operation_id,
            job_id,
        )
    assert tuple(queue) == ("parked", lease_token + 1, "workspace_recovery")
    assert tuple(operation) == ("paused_attention", "workspace_identity_conflict")
    assert tuple(participant) == ("attention", None)


@pytest.mark.asyncio
async def test_concurrent_identical_admissions_return_one_receipt(app_pg) -> None:
    job_id, lease_token = await insert_leased_job(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-a")
    kwargs = admission_kwargs(job_id, lease_token)

    first, second = await asyncio.gather(
        store.admit_hold(**kwargs), store.admit_hold(**kwargs)
    )

    assert first == second
    async with app_pg.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_recovery_requests "
                "WHERE scope_kind='job' AND scope_id=$1 AND request_id=$2",
                job_id,
                kwargs["request_id"],
            )
            == 1
        )


@pytest.mark.asyncio
async def test_store_admits_idempotent_hold_and_releases_exact_queue_token(
    app_pg,
) -> None:
    job_id = uuid4()
    attempt_token = 27
    request_id = uuid4()
    provision_generation = uuid4()
    vm_uid = uuid4()
    vmi_uid = uuid4()
    launcher_uid = uuid4()
    pvc_uid = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane) "
            "VALUES ($1, 'admit recovery', 'processing', 'stateless')",
            job_id,
        )
        await conn.execute(
            """
            INSERT INTO run_queue (
                unit_id, unit_kind, state, lease_token, leased_by, leased_until,
                input_seq, consumed_seq, attempts_since_completion
            ) VALUES ($1, 'worker_batch', 'leased', $2, 'worker-a',
                      clock_timestamp() + interval '1 minute', 4, 3, 2)
            """,
            job_id,
            attempt_token,
        )
        await conn.execute(
            "INSERT INTO worker_batch_attempts "
            "(job_id, lease_token, claimed_attempt) VALUES ($1, $2, 2)",
            job_id,
            attempt_token,
        )

    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-a")
    kwargs = dict(
        job_id=job_id,
        accepted_lease_token=attempt_token,
        owner_kind="job",
        owner_id=job_id,
        workspace_contract_digest="sha256:workspace",
        provision_generation=provision_generation,
        cluster_name="test-cluster",
        namespace="workers",
        vm_uid=vm_uid,
        prior_vmi_uid=vmi_uid,
        prior_launcher_uid=launcher_uid,
        root_pvc_uid=pvc_uid,
        code=WorkspaceRecoveryCode.RUNTIME_NOT_READY,
        request_id=request_id,
        actor_kind="worker",
        actor_id="worker-a",
        intent_digest="sha256:intent",
    )
    admitted = await store.admit_hold(**kwargs)
    assert admitted.accepted_lease_token == attempt_token
    assert admitted.hold_lease_token == attempt_token + 1
    assert await store.admit_hold(**kwargs) == admitted
    with pytest.raises(RuntimeError, match="different intent"):
        await store.admit_hold(**(kwargs | {"intent_digest": "sha256:changed"}))

    async with app_pg.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state, lease_token, attempts_since_completion, input_seq, "
            "consumed_seq, park_reason FROM run_queue WHERE unit_id=$1",
            job_id,
        )
        job = await conn.fetchrow(
            "SELECT status, freeze_data FROM jobs WHERE id=$1", job_id
        )
    assert tuple(queue) == (
        "parked",
        attempt_token + 1,
        1,
        4,
        3,
        "workspace_recovery",
    )
    freeze = job["freeze_data"]
    if isinstance(freeze, str):
        freeze = json.loads(freeze)
    assert job["status"] == "paused"
    assert freeze["recovery_id"] == str(admitted.operation_id)

    claim = await store.claim_due(admitted.operation_id)
    assert claim is not None
    assert await store.release_recovered(
        operation_id=admitted.operation_id,
        version=claim.version,
        claim_token=claim.claim_token,
        resume_receipt={"kind": "same_runtime_ready"},
    )
    async with app_pg.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state, lease_token, attempts_since_completion, input_seq, "
            "consumed_seq, park_reason FROM run_queue WHERE unit_id=$1",
            job_id,
        )
        job = await conn.fetchrow(
            "SELECT status, freeze_data FROM jobs WHERE id=$1", job_id
        )
        operation = await conn.fetchrow(
            "SELECT phase, resolved_at FROM vm_workspace_recoveries WHERE id=$1",
            admitted.operation_id,
        )
    assert tuple(queue) == ("queued", attempt_token + 1, 1, 5, 3, None)
    assert tuple(job) == ("paused", None)
    assert operation["phase"] == "recovered"
    assert operation["resolved_at"] is not None


@pytest.mark.asyncio
async def test_final_recovery_cas_binds_successor_and_releases_once(app_pg) -> None:
    job_id, lease_token = await insert_leased_job(app_pg)
    generation = uuid4()
    vm_uid = uuid4()
    old_vmi_uid = uuid4()
    old_launcher_uid = uuid4()
    pvc_uid = uuid4()
    successor_vmi_uid = uuid4()
    successor_launcher_uid = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_build_object('vm',jsonb_build_object("
            "'provision_generation',$2::text,'vm_uid',$3::text,"
            "'rootdisk_pvc_uid',$4::text,'status','ssh_pending')) WHERE id=$1",
            job_id,
            str(generation),
            str(vm_uid),
            str(pvc_uid),
        )
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="leader-a")
    admitted = await store.admit_hold(
        **(
            admission_kwargs(job_id, lease_token)
            | {
                "provision_generation": generation,
                "vm_uid": vm_uid,
                "prior_vmi_uid": old_vmi_uid,
                "prior_launcher_uid": old_launcher_uid,
                "root_pvc_uid": pvc_uid,
            }
        )
    )
    claimed = await store.claim_due(admitted.operation_id)
    assert claimed is not None
    observation = {
        "ready": True,
        "authenticated": True,
        "owner_kind": "job",
        "owner_id": str(job_id),
        "provision_generation": str(generation),
        "vm_uid": str(vm_uid),
        "root_pvc_uid": str(pvc_uid),
        "prior_runtime": "stopped",
        "stop_receipt_digest": "sha256:exact-stop",
        "remote_operations": "settled",
        "continuation": "safe",
        "successor": {
            "vmi_uid": str(successor_vmi_uid),
            "launcher_uid": str(successor_launcher_uid),
            "node_uid": "node-8",
            "pod_ip": "10.42.0.90",
            "ssh_registration_id": "registration-1",
        },
    }
    staged = await store.stage_observation(
        operation_id=claimed.operation_id,
        version=claimed.version,
        claim_token=claimed.claim_token,
        phase="attesting",
        observation=observation,
    )
    assert staged is not None
    async with app_pg.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO vm_workspace_recovery_stop_receipts (
                recovery_id,accepted_claim_token,vm_uid,vmi_uid,launcher_uid,
                container_id,root_pvc_uid,controller_identity,observed_at,
                evidence,evidence_digest
            ) VALUES ($1,$2,$3,$4,$5,'container://old',$6,'controller/test',
                      clock_timestamp(),'{"stopped":true}'::jsonb,$7)
            """,
            admitted.operation_id,
            staged.claim_token,
            vm_uid,
            old_vmi_uid,
            old_launcher_uid,
            pvc_uid,
            "sha256:exact-stop",
        )

    assert await store.release_recovered(
        operation_id=staged.operation_id,
        version=staged.version,
        claim_token=staged.claim_token,
        initial_observation=observation,
        final_observation=observation.copy(),
        resume_receipt={"kind": "workspace_recovery"},
    )
    async with app_pg.acquire() as conn:
        operation = await conn.fetchrow(
            "SELECT phase,resolved_at FROM vm_workspace_recoveries WHERE id=$1",
            admitted.operation_id,
        )
        queue = await conn.fetchrow(
            "SELECT state,lease_token FROM run_queue WHERE unit_id=$1", job_id
        )
        vm = await conn.fetchval("SELECT context->'vm' FROM jobs WHERE id=$1", job_id)
    if isinstance(vm, str):
        vm = json.loads(vm)
    assert operation["phase"] == "recovered" and operation["resolved_at"] is not None
    assert tuple(queue) == ("queued", lease_token + 1)
    assert vm["active_pod_uid"] == str(successor_launcher_uid)
    assert vm["vmi_uid"] == str(successor_vmi_uid)
    assert vm["pod_ip"] == "10.42.0.90"


@pytest.mark.asyncio
async def test_final_recovery_cas_retains_hold_when_re_attested_generation_changed(
    app_pg,
) -> None:
    job_id, lease_token = await insert_leased_job(app_pg)
    kwargs = admission_kwargs(job_id, lease_token)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_build_object('vm',jsonb_build_object("
            "'provision_generation',$2::text,'vm_uid',$3::text,"
            "'rootdisk_pvc_uid',$4::text)) WHERE id=$1",
            job_id,
            str(kwargs["provision_generation"]),
            str(kwargs["vm_uid"]),
            str(kwargs["root_pvc_uid"]),
        )
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="leader-a")
    admitted = await store.admit_hold(**kwargs)
    claimed = await store.claim_due(admitted.operation_id)
    assert claimed is not None
    observation = {
        "ready": True,
        "authenticated": True,
        "owner_kind": "job",
        "owner_id": str(job_id),
        "provision_generation": str(kwargs["provision_generation"]),
        "vm_uid": str(kwargs["vm_uid"]),
        "root_pvc_uid": str(kwargs["root_pvc_uid"]),
        "prior_runtime": "same_runtime",
        "remote_operations": "settled",
        "continuation": "safe",
        "successor": {
            "vmi_uid": str(kwargs["prior_vmi_uid"]),
            "launcher_uid": str(kwargs["prior_launcher_uid"]),
            "node_uid": "node-8",
            "pod_ip": "10.42.0.91",
            "ssh_registration_id": "registration-2",
        },
    }
    staged = await store.stage_observation(
        operation_id=claimed.operation_id,
        version=claimed.version,
        claim_token=claimed.claim_token,
        phase="attesting",
        observation=observation,
    )
    assert staged is not None
    changed_observation = {
        **observation,
        "provision_generation": str(uuid4()),
    }

    assert not await store.release_recovered(
        operation_id=staged.operation_id,
        version=staged.version,
        claim_token=staged.claim_token,
        initial_observation=observation,
        final_observation=changed_observation,
        resume_receipt={"kind": "workspace_recovery"},
    )
    async with app_pg.acquire() as conn:
        operation = await conn.fetchrow(
            "SELECT phase,reason_code FROM vm_workspace_recoveries WHERE id=$1",
            admitted.operation_id,
        )
        queue = await conn.fetchrow(
            "SELECT state,lease_token,park_reason FROM run_queue WHERE unit_id=$1",
            job_id,
        )
    assert tuple(operation) == ("paused_attention", "workspace_identity_conflict")
    assert tuple(queue) == ("parked", lease_token + 1, "workspace_recovery")


@pytest.mark.asyncio
@pytest.mark.parametrize("commands", [False, True])
async def test_worker_claim_commits_exact_attempt_and_rolls_back_on_ledger_conflict(
    app_pg, commands
) -> None:
    async with app_pg.acquire() as conn:
        job_id = await conn.fetchval(
            "INSERT INTO jobs (description, status, execution_lane) "
            "VALUES ('claim ledger', 'created', 'stateless') RETURNING id"
        )
        await worker_queue.enqueue_worker_batch(conn, job_id=job_id)
    claim = await worker_queue.claim_worker_batch(
        app_pg, pod_name="worker-a", completion_commands_enabled=commands
    )
    assert claim is not None
    async with app_pg.acquire() as conn:
        attempt = await conn.fetchrow(
            "SELECT lease_token, claimed_attempt, bundle_authorized_at "
            "FROM worker_batch_attempts WHERE job_id=$1",
            job_id,
        )
        assert attempt is not None
        assert tuple(attempt) == (1, 1, None)
        await worker_queue.release_worker_batch(
            conn,
            unit_id=job_id,
            lease_token=1,
            park_on_exhaustion=True,
            backoff_base_seconds=0,
        )
        await conn.execute(
            "INSERT INTO worker_batch_attempts (job_id, lease_token, claimed_attempt) "
            "VALUES ($1, 2, 2)",
            job_id,
        )
    with pytest.raises(asyncpg.UniqueViolationError):
        await worker_queue.claim_worker_batch(
            app_pg, pod_name="worker-a", completion_commands_enabled=commands
        )
    async with app_pg.acquire() as conn:
        assert tuple(
            await conn.fetchrow(
                "SELECT state, lease_token, attempts_since_completion FROM run_queue "
                "WHERE unit_id=$1",
                job_id,
            )
        ) == ("queued", 1, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "evidence", ["pre_bundle", "authorized", "missing", "refunded"]
)
async def test_recovery_hold_rotates_token_without_resetting_failures(
    app_pg, evidence
) -> None:
    job_id, token = await insert_leased_job(
        app_pg, include_attempt=evidence != "missing"
    )
    recovery_id = await insert_recovery(app_pg)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET attempts_since_completion=4, input_seq=9, "
            "consumed_seq=8, control_input_seq=7, control_consumed_seq=6, "
            "last_leased_by='worker-a' WHERE unit_id=$1",
            job_id,
        )
        await conn.execute(
            "UPDATE worker_batch_attempts SET claimed_attempt=4 WHERE job_id=$1", job_id
        )
        if evidence == "authorized":
            await conn.execute(
                "UPDATE worker_batch_attempts SET bundle_authorized_at=now(), "
                "authority_digest='sha256:bundle' WHERE job_id=$1",
                job_id,
            )
        elif evidence == "refunded":
            await conn.execute(
                "UPDATE worker_batch_attempts SET refunded_at=now(), "
                "refund_reason='already_refunded' WHERE job_id=$1",
                job_id,
            )
        held = await worker_queue.park_worker_batch_for_workspace_recovery(
            conn, job_id=job_id, accepted_lease_token=27, recovery_id=recovery_id
        )
        assert held.hold_lease_token == 28
        assert (
            await worker_queue.park_worker_batch_for_workspace_recovery(
                conn, job_id=job_id, accepted_lease_token=27, recovery_id=recovery_id
            )
            is None
        )
        row = await conn.fetchrow(
            "SELECT state, lease_token, attempts_since_completion, input_seq, consumed_seq, "
            "control_input_seq, control_consumed_seq, leased_by, last_leased_by, "
            "leased_until, run_after='infinity'::timestamptz AS indefinite "
            "FROM run_queue WHERE unit_id=$1",
            job_id,
        )
        expected_attempts = 3 if evidence == "pre_bundle" else 4
        assert tuple(row) == (
            "parked",
            28,
            expected_attempts,
            9,
            8,
            7,
            6,
            None,
            None,
            None,
            True,
        )
        disposition = await worker_queue.get_worker_attempt_disposition(
            conn, job_id=job_id, lease_token=token
        )
        if evidence == "missing":
            assert disposition is None
        else:
            assert disposition.refunded is (evidence in {"pre_bundle", "refunded"})
        assert not await worker_queue.record_worker_bundle_authorized(
            conn, job_id=job_id, lease_token=token, authority_digest="sha256:late"
        )


@pytest.mark.asyncio
async def test_bundle_authorization_serializes_with_recovery_hold(app_pg) -> None:
    job_id, token = await insert_leased_job(app_pg)
    recovery_id = await insert_recovery(app_pg)
    async with app_pg.acquire() as conn:
        assert await worker_queue.record_worker_bundle_authorized(
            conn, job_id=job_id, lease_token=token, authority_digest="sha256:bundle"
        )
        held = await worker_queue.park_worker_batch_for_workspace_recovery(
            conn, job_id=job_id, accepted_lease_token=token, recovery_id=recovery_id
        )
        assert not held.refunded
        assert (
            await conn.fetchval(
                "SELECT attempts_since_completion FROM run_queue WHERE unit_id=$1",
                job_id,
            )
            == 2
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("commands", [False, True])
async def test_unresolved_recovery_blocks_claim_renew_complete_and_reap(
    app_pg, commands
) -> None:
    job_id, token = await insert_leased_job(app_pg)
    recovery_id = await insert_recovery(app_pg)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_jobs (recovery_id, job_id, "
            "accepted_lease_token, hold_lease_token, prior_queue_state, prior_job_status) "
            "VALUES ($1, $2, 26, 27, 'leased', 'processing')",
            recovery_id,
            job_id,
        )
        assert (
            await worker_queue.renew_worker_batch(
                conn, unit_id=job_id, lease_token=token
            )
            is None
        )
        assert (
            await worker_queue.complete_worker_batch(
                conn, unit_id=job_id, lease_token=token, consumed_seq=4
            )
            is None
        )
        await conn.execute(
            "UPDATE run_queue SET leased_until=now()-interval '1 minute' WHERE unit_id=$1",
            job_id,
        )
        assert await reap_expired(conn, unit_kind="worker_batch", grace_seconds=0) == []
        await conn.execute(
            "UPDATE run_queue SET state='parked', leased_by=NULL, leased_until=NULL "
            "WHERE unit_id=$1",
            job_id,
        )
        assert not await unpark_unit(conn, unit_id=job_id)
        await conn.execute(
            "UPDATE run_queue SET state='queued', last_leased_by=NULL, run_after=now() "
            "WHERE unit_id=$1",
            job_id,
        )
        runnable_id = await conn.fetchval(
            "INSERT INTO jobs (description, status, execution_lane) "
            "VALUES ('not held', 'created', 'stateless') RETURNING id"
        )
        await worker_queue.enqueue_worker_batch(conn, job_id=runnable_id)
    claim = await worker_queue.claim_worker_batch(
        app_pg, pod_name="worker-b", completion_commands_enabled=commands
    )
    assert claim is not None and claim.unit_id == runnable_id
    async with app_pg.acquire() as conn:
        assert tuple(
            await conn.fetchrow(
                "SELECT lease_token, attempts_since_completion FROM run_queue WHERE unit_id=$1",
                job_id,
            )
        ) == (27, 2)


@pytest.mark.asyncio
async def test_recovery_release_cas_advances_input_once_and_preserves_counters(
    app_pg,
) -> None:
    job_id, token = await insert_leased_job(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-a")
    admitted = await store.admit_hold(**admission_kwargs(job_id, token))
    claim = await store.claim_due(admitted.operation_id)
    assert claim is not None
    async with app_pg.acquire() as conn:
        args = dict(
            job_id=job_id,
            recovery_id=admitted.operation_id,
            hold_lease_token=28,
            version=claim.version,
            claim_token=claim.claim_token,
            resume_receipt={"ready": True},
        )
        assert not await worker_queue.release_worker_batch_from_workspace_recovery(
            conn, **(args | {"hold_lease_token": 29})
        )
        assert not await worker_queue.release_worker_batch_from_workspace_recovery(
            conn, **(args | {"claim_token": claim.claim_token + 1})
        )
        assert await worker_queue.release_worker_batch_from_workspace_recovery(
            conn, **args
        )
        assert not await worker_queue.release_worker_batch_from_workspace_recovery(
            conn, **args
        )
        assert tuple(
            await conn.fetchrow(
                "SELECT state, lease_token, attempts_since_completion, input_seq, consumed_seq "
                "FROM run_queue WHERE unit_id=$1",
                job_id,
            )
        ) == ("queued", 28, 1, 5, 3)


def recovery_test_db(pool):
    db = PostgresDB.__new__(PostgresDB)
    db._pool = pool
    return db


@pytest.mark.asyncio
async def test_membership_creation_and_reassignment_refuse_open_recovery(
    app_pg,
) -> None:
    parent_id, token = await insert_leased_job(app_pg)
    child_id = uuid4()
    db = recovery_test_db(app_pg)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, parent_job_id) "
            "VALUES ($1, 'child', 'created', $2)",
            child_id,
            parent_id,
        )
    recovery_id = await insert_recovery(app_pg, owner_id=parent_id)
    with pytest.raises(RuntimeError, match="workspace recovery"):
        async with db.transaction_scope():
            await db.create_job(
                description="late child",
                parent_job_id=str(parent_id),
                context={"inherits_parent_workspace": True},
            )
    assert not await db.merge_job_context(
        str(child_id), {"inherits_parent_workspace": True}
    )
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context='{"
            + '"inherits_parent_workspace":true'
            + "}'::jsonb "
            "WHERE id=$1",
            child_id,
        )
    assert not await db.merge_job_context(
        str(child_id), {"inherits_parent_workspace": False}
    )
    assert not await db.delete_job_context_keys(
        str(child_id), ["inherits_parent_workspace"]
    )
    async with app_pg.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM jobs") == 2
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET phase='cancelled', resolved_at=now() WHERE id=$1",
            recovery_id,
        )
    assert await db.merge_job_context(
        str(child_id), {"inherits_parent_workspace": False}
    )


@pytest.mark.asyncio
async def test_shared_recovery_materializes_and_holds_all_member_queues(app_pg) -> None:
    parent_id, token = await insert_leased_job(app_pg)
    child_id = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, parent_job_id, context) "
            "VALUES ($1, 'never leased child', 'created', 'stateless', $2, "
            "'{\"inherits_parent_workspace\":true}'::jsonb)",
            child_id,
            parent_id,
        )
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-a")
    admitted = await store.admit_hold(**admission_kwargs(parent_id, token))
    async with app_pg.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_recovery_jobs WHERE recovery_id=$1",
                admitted.operation_id,
            )
            == 2
        )
        assert tuple(
            await conn.fetchrow(
                "SELECT state, lease_token, attempts_since_completion FROM run_queue WHERE unit_id=$1",
                child_id,
            )
        ) == ("parked", 1, 0)
        assert tuple(
            await conn.fetchrow(
                "SELECT accepted_lease_token, prior_queue_state FROM vm_workspace_recovery_jobs "
                "WHERE job_id=$1",
                child_id,
            )
        ) == (None, "absent")
    claim = await store.claim_due(admitted.operation_id)
    assert claim is not None
    assert await store.release_recovered(
        operation_id=admitted.operation_id,
        version=claim.version,
        claim_token=claim.claim_token,
        resume_receipt={"ready": True},
    )
    async with app_pg.acquire() as conn:
        assert tuple(
            await conn.fetchrow(
                "SELECT state, lease_token, attempts_since_completion, input_seq FROM run_queue WHERE unit_id=$1",
                child_id,
            )
        ) == ("queued", 1, 0, 1)
        assert (
            await conn.fetchval("SELECT status FROM jobs WHERE id=$1", child_id)
            == "created"
        )


@pytest.mark.asyncio
async def test_child_creation_waits_for_membership_lock_then_observes_hold(
    app_pg,
) -> None:
    parent_id, _ = await insert_leased_job(app_pg)
    db = recovery_test_db(app_pg)

    async def create_child():
        async with db.transaction_scope():
            return await db.create_job(
                description="racing child",
                parent_job_id=str(parent_id),
                context={"inherits_parent_workspace": True},
            )

    task = None
    try:
        async with app_pg.acquire() as owner:
            async with owner.transaction():
                await owner.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                    f"workspace-recovery:job:{parent_id}",
                )
                task = asyncio.create_task(create_child())
                async with app_pg.acquire() as observer:

                    async def waiting():
                        while not task.done():
                            if await observer.fetchval(
                                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                                "WHERE datname=current_database() AND wait_event_type='Lock' "
                                "AND query LIKE '%pg_advisory_xact_lock%')"
                            ):
                                return True
                            await asyncio.sleep(0.01)
                        return False

                    assert await asyncio.wait_for(waiting(), 3)
                await insert_recovery(app_pg, owner_id=parent_id)
            with pytest.raises(RuntimeError, match="workspace recovery"):
                await asyncio.wait_for(task, 3)
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_pinned_shared_writer_forces_attention_without_worker_queue(
    app_pg,
) -> None:
    parent_id, token = await insert_leased_job(app_pg)
    child_id = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, parent_job_id, context) "
            "VALUES ($1, 'pinned writer', 'created', 'pinned', $2, "
            "'{\"inherits_parent_workspace\":true}'::jsonb)",
            child_id,
            parent_id,
        )
    store = VMWorkspaceRecoveryStore(app_pg)
    admitted = await store.admit_hold(**admission_kwargs(parent_id, token))
    assert admitted.action == "paused_attention"
    assert admitted.code is WorkspaceRecoveryCode.SHARED_WRITERS_UNFENCED
    async with app_pg.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT latest_diagnostic->>'reason' FROM vm_workspace_recoveries WHERE id=$1",
                admitted.operation_id,
            )
            == "shared_workspace_writers_unfenced"
        )
        assert tuple(
            await conn.fetchrow(
                "SELECT participation, accepted_lease_token, hold_lease_token, prior_queue_state "
                "FROM vm_workspace_recovery_jobs WHERE recovery_id=$1 AND job_id=$2",
                admitted.operation_id,
                child_id,
            )
        ) == ("attention", None, None, "non_worker")
        assert not await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM run_queue WHERE unit_id=$1)", child_id
        )
        assert (
            await conn.fetchval("SELECT status FROM jobs WHERE id=$1", child_id)
            == "created"
        )
    assert await store.claim_due(admitted.operation_id) is None


@pytest.mark.asyncio
async def test_participant_hold_token_is_exact_or_explicitly_non_worker(app_pg) -> None:
    recovery_id = await insert_recovery(app_pg)
    job_id, _ = await insert_leased_job(app_pg)
    async with app_pg.acquire() as conn:
        insert = (
            "INSERT INTO vm_workspace_recovery_jobs (recovery_id, job_id, "
            "accepted_lease_token, hold_lease_token, prior_queue_state, prior_job_status, participation) "
            "VALUES ($1,$2,$3,$4,$5,'created','attention')"
        )
        for accepted, hold, state in [
            (None, 1, "non_worker"),
            (None, None, "queued"),
            (27, None, "non_worker"),
        ]:
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(insert, recovery_id, job_id, accepted, hold, state)
        await conn.execute(insert, recovery_id, job_id, None, None, "non_worker")


@pytest.mark.asyncio
async def test_owner_reassignment_before_admission_locks_holds_current_workspace(
    app_pg,
) -> None:
    reporter_id, token = await insert_leased_job(app_pg)
    parent_id, sibling_id = uuid4(), uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane) "
            "VALUES ($1,'parent','created','stateless')",
            parent_id,
        )
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, parent_job_id, context) "
            "VALUES ($1,'sibling','created','stateless',$2,'{\"inherits_parent_workspace\":true}')",
            sibling_id,
            parent_id,
        )
        await conn.execute(
            "UPDATE jobs SET parent_job_id=$2 WHERE id=$1", reporter_id, parent_id
        )
    # Runtime/owner selection precedes reassignment. The admission waits on
    # the real membership writer's locks and must re-resolve after its commit.
    kwargs = admission_kwargs(reporter_id, token)
    db = recovery_test_db(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg)
    task = None
    try:
        async with db.transaction_scope():
            assert await db.merge_job_context(
                str(reporter_id), {"inherits_parent_workspace": True}
            )
            task = asyncio.create_task(store.admit_hold(**kwargs))
            async with app_pg.acquire() as observer:

                async def waiting():
                    while not task.done():
                        if await observer.fetchval(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() "
                            "AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock%')"
                        ):
                            return True
                        await asyncio.sleep(0.01)
                    return False

                assert await asyncio.wait_for(waiting(), 3)
        admitted = await asyncio.wait_for(task, 3)
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert admitted.action == "paused_attention"
    assert admitted.code is WorkspaceRecoveryCode.IDENTITY_CONFLICT
    async with app_pg.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT owner_id FROM vm_workspace_recoveries WHERE id=$1",
                admitted.operation_id,
            )
            == parent_id
        )
        participants = await conn.fetch(
            "SELECT job_id, participation FROM vm_workspace_recovery_jobs WHERE recovery_id=$1",
            admitted.operation_id,
        )
        assert {row["job_id"] for row in participants} == {
            reporter_id,
            parent_id,
            sibling_id,
        }
        assert {row["participation"] for row in participants} == {"attention"}
        assert tuple(
            await conn.fetchrow(
                "SELECT state, lease_token FROM run_queue WHERE unit_id=$1", reporter_id
            )
        ) == ("parked", 28)
    assert await store.claim_due(admitted.operation_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "history",
    [
        "queued_legacy",
        "uncertain_park",
        "processing_without_queue",
        "created_with_history",
    ],
)
async def test_nonleased_dependent_requires_positive_initialization_evidence(
    app_pg, history
) -> None:
    parent_id, token = await insert_leased_job(app_pg)
    child_id, child_token = await insert_leased_job(
        app_pg, include_attempt=history in {"uncertain_park", "created_with_history"}
    )
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET parent_job_id=$2, context='{\"inherits_parent_workspace\":true}' WHERE id=$1",
            child_id,
            parent_id,
        )
        if history == "processing_without_queue":
            await conn.execute("DELETE FROM run_queue WHERE unit_id=$1", child_id)
        else:
            await conn.execute(
                "UPDATE run_queue SET state=$2, leased_by=NULL, leased_until=NULL, "
                "park_reason=$3, input_seq=9, consumed_seq=8 WHERE unit_id=$1",
                child_id,
                "parked" if history == "uncertain_park" else "queued",
                "claim_loss_hold" if history == "uncertain_park" else None,
            )
        if history == "uncertain_park":
            await conn.execute(
                "UPDATE worker_batch_attempts SET bundle_authorized_at=now(), authority_digest='sha256:prior' "
                "WHERE job_id=$1",
                child_id,
            )
        elif history == "created_with_history":
            await conn.execute("UPDATE jobs SET status='created' WHERE id=$1", child_id)
            await conn.execute(
                "UPDATE run_queue SET lease_token=0, attempts_since_completion=0 WHERE unit_id=$1",
                child_id,
            )
    store = VMWorkspaceRecoveryStore(app_pg)
    admitted = await store.admit_hold(**admission_kwargs(parent_id, token))
    assert admitted.action == "paused_attention"
    assert admitted.code is WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
    assert await store.claim_due(admitted.operation_id) is None
    async with app_pg.acquire() as conn:
        participant = await conn.fetchrow(
            "SELECT accepted_lease_token, prior_queue_state, prior_control_reference "
            "FROM vm_workspace_recovery_jobs WHERE recovery_id=$1 AND job_id=$2",
            admitted.operation_id,
            child_id,
        )
        reference = participant["prior_control_reference"]
        if isinstance(reference, str):
            reference = json.loads(reference)
        assert reference["never_started"] is False
        if history in {"queued_legacy", "uncertain_park"}:
            assert participant["accepted_lease_token"] == child_token
            assert tuple(
                await conn.fetchrow(
                    "SELECT state, attempts_since_completion, input_seq, consumed_seq FROM run_queue WHERE unit_id=$1",
                    child_id,
                )
            ) == ("parked", 2, 9, 8)
        if history == "uncertain_park":
            assert reference["queue"]["park_reason"] == "claim_loss_hold"
            assert reference["attempt"]["lease_token"] == child_token
            assert reference["attempt"]["authority_digest"] == "sha256:prior"
            assert (
                await conn.fetchval(
                    "SELECT park_reason FROM run_queue WHERE unit_id=$1", child_id
                )
                == "claim_loss_hold"
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("inheritance", [True, "ambiguous"])
async def test_admission_rechecks_canonical_membership_shape(
    app_pg, inheritance
) -> None:
    reporter_id, token = await insert_leased_job(app_pg)
    parent_id = uuid4()
    async with app_pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane) VALUES ($1,'parent','created','stateless')",
            parent_id,
        )
        await conn.execute(
            "UPDATE jobs SET parent_job_id=$2, context=$3::jsonb WHERE id=$1",
            reporter_id,
            parent_id,
            json.dumps({"inherits_parent_workspace": inheritance}),
        )
    kwargs = admission_kwargs(reporter_id, token)
    if inheritance is True:
        kwargs["owner_id"] = parent_id
    admitted = await VMWorkspaceRecoveryStore(app_pg).admit_hold(**kwargs)
    if inheritance is True:
        assert admitted.action == "hold_committed"
    else:
        assert admitted.action == "paused_attention"
        assert admitted.code is WorkspaceRecoveryCode.IDENTITY_CONFLICT
    async with app_pg.acquire() as conn:
        assert tuple(
            await conn.fetchrow(
                "SELECT state, lease_token FROM run_queue WHERE unit_id=$1", reporter_id
            )
        ) == ("parked", 28)
