"""Durable VM workspace recovery contracts against PostgreSQL 15."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
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


class CaptureTelemetry:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def emit(self, **values) -> None:
        self.calls.append(values)


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
    original_cause: dict | None = None,
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
                first_observed_at, deadline_at, next_check_at, reason_code,
                original_cause
            ) VALUES (
                'job', $1, 'sha256:contract', $2, 'test-cluster', 'workers', $3,
                $4, $5, $6, $7, $8, $9, $8, 'workspace_runtime_not_ready',
                $10::jsonb
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
            json.dumps(original_cause if original_cause is not None else {}),
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


async def prepare_checkpoint_rows(
    app_pg, job_id: UUID, *, checkpoint_count: int
) -> None:
    """Create the minimal LangGraph tables needed by checkpoint-prune races."""

    async with app_pg.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                thread_id text NOT NULL,
                checkpoint_ns text NOT NULL DEFAULT '',
                checkpoint_id text NOT NULL,
                PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
            );
            CREATE TABLE IF NOT EXISTS checkpoint_writes (
                thread_id text NOT NULL,
                checkpoint_ns text NOT NULL DEFAULT '',
                checkpoint_id text NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoint_blobs (
                thread_id text NOT NULL,
                checkpoint_ns text NOT NULL DEFAULT '',
                channel text NOT NULL,
                version text NOT NULL
            );
            TRUNCATE checkpoint_writes, checkpoint_blobs, checkpoints;
            """
        )
        await conn.executemany(
            "INSERT INTO checkpoints (thread_id,checkpoint_ns,checkpoint_id) "
            "VALUES ($1,'',$2)",
            [(str(job_id), f"{index:04d}") for index in range(1, checkpoint_count + 1)],
        )
        await conn.executemany(
            "INSERT INTO checkpoint_writes (thread_id,checkpoint_ns,checkpoint_id) "
            "VALUES ($1,'',$2)",
            [(str(job_id), f"{index:04d}") for index in range(1, checkpoint_count + 1)],
        )
        await conn.executemany(
            "INSERT INTO checkpoint_blobs "
            "(thread_id,checkpoint_ns,channel,version) VALUES ($1,'','state',$2)",
            [(str(job_id), f"{index:04d}") for index in range(1, checkpoint_count + 1)],
        )


async def checkpoint_count(app_pg, job_id: UUID) -> int:
    async with app_pg.acquire() as conn:
        return int(
            await conn.fetchval(
                "SELECT count(*) FROM checkpoints WHERE thread_id=$1", str(job_id)
            )
        )


async def checkpoint_row_counts(app_pg, job_id: UUID) -> tuple[int, int, int]:
    async with app_pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT "
            "(SELECT count(*) FROM checkpoint_writes WHERE thread_id=$1) AS writes,"
            "(SELECT count(*) FROM checkpoint_blobs WHERE thread_id=$1) AS blobs,"
            "(SELECT count(*) FROM checkpoints WHERE thread_id=$1) AS checkpoints",
            str(job_id),
        )
    return int(row["writes"]), int(row["blobs"]), int(row["checkpoints"])


async def checkpoint_cleanup_admissions(app_pg, job_id: UUID) -> list[asyncpg.Record]:
    async with app_pg.acquire() as conn:
        return list(
            await conn.fetch(
                "SELECT id,source,intent_digest,completed_at,outcome "
                "FROM vm_workspace_cleanup_admissions "
                "WHERE owner_kind='job' AND owner_id=$1 ORDER BY admitted_at,id",
                job_id,
            )
        )


async def wait_for_checkpoint_cleanup_admission(app_pg, job_id: UUID) -> None:
    for _ in range(100):
        async with app_pg.acquire() as conn:
            admitted = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM vm_workspace_cleanup_admissions "
                "WHERE owner_kind='job' AND owner_id=$1 AND completed_at IS NULL)",
                job_id,
            )
        if admitted:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("checkpoint cleanup did not publish its durable admission")


async def checkpoint_cleanup_outcome(app_pg, job_id: UUID, source: str) -> str | None:
    async with app_pg.acquire() as conn:
        return await conn.fetchval(
            "SELECT outcome FROM vm_workspace_cleanup_admissions "
            "WHERE owner_kind='job' AND owner_id=$1 AND source=$2 "
            "ORDER BY admitted_at DESC LIMIT 1",
            job_id,
            source,
        )


async def hold_checkpoint_row_lock(app_pg, job_id: UUID):
    conn = await app_pg.acquire()
    transaction = conn.transaction()
    await transaction.start()
    await conn.fetchrow(
        "SELECT checkpoint_id FROM checkpoints WHERE thread_id=$1 "
        "ORDER BY checkpoint_id LIMIT 1 FOR UPDATE",
        str(job_id),
    )
    return conn, transaction


def postgres_db(app_pg) -> PostgresDB:
    db = PostgresDB.__new__(PostgresDB)
    db.acquire = app_pg.acquire
    return db


@pytest.mark.asyncio
async def test_recovery_hold_and_cleanup_admission_have_one_winner(app_pg) -> None:
    telemetry = CaptureTelemetry()
    store = VMWorkspaceRecoveryStore(
        app_pg, worker_id="test-worker", telemetry=telemetry
    )
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
    assert [call["event"] for call in telemetry.calls] == [
        "cleanup_blocked",
        "hold",
        "queue_fenced",
        "cleanup_blocked",
    ]
    assert telemetry.calls[2]["accepted_lease_token"] == lease_token
    assert telemetry.calls[2]["hold_lease_token"] == lease_token + 1


@pytest.mark.asyncio
async def test_committed_hold_blocks_controller_cleanup_before_pin_publication(
    app_pg,
) -> None:
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="controller-boundary")
    job_id, lease_token = await insert_leased_job(app_pg)
    kwargs = admission_kwargs(job_id, lease_token)

    admitted = await store.admit_hold(**kwargs)
    async with app_pg.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT controller_pinned_at IS NOT NULL "
            "FROM vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
            admitted.operation_id,
        )

    cleanup = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=job_id,
        pvc_uid=kwargs["root_pvc_uid"],
        request_id=uuid4(),
        source="controller_rootdisk_delete",
        intent_digest="sha256:controller-delete-before-pin",
        revalidate_completed=True,
    )

    assert not cleanup.allowed
    assert cleanup.recovery_id == admitted.operation_id
    assert cleanup.reason == "workspace_recovery_unresolved"


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
async def test_controller_cleanup_reservation_resumes_only_while_open(app_pg) -> None:
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="controller-boundary")
    owner_id = uuid4()
    request_id = uuid4()
    intent_digest = "sha256:failed-dv-recreate-g1-old-dv-old-pvc"
    permit = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=owner_id,
        pvc_uid=uuid4(),
        request_id=request_id,
        source="controller_failed_dv_recreate",
        intent_digest=intent_digest,
    )
    assert permit.allowed and permit.admission_id is not None

    resumed = await store.resume_cleanup_permit(
        permit.admission_id,
        owner_kind="job",
        owner_id=owner_id,
        source="controller_failed_dv_recreate",
        request_id=request_id,
        intent_digest=intent_digest,
    )
    assert resumed.allowed and resumed.admission_id == permit.admission_id

    changed_intent = await store.resume_cleanup_permit(
        permit.admission_id,
        owner_kind="job",
        owner_id=owner_id,
        source="controller_failed_dv_recreate",
        request_id=request_id,
        intent_digest="sha256:failed-dv-recreate-g2-old-dv-old-pvc",
    )
    assert not changed_intent.allowed
    assert changed_intent.reason == "cleanup_reservation_changed"

    assert await store.complete_cleanup_permit(
        permit.admission_id,
        outcome="recreated",
        request_id=request_id,
        intent_digest=intent_digest,
    )
    assert await store.complete_cleanup_permit(
        permit.admission_id,
        outcome="recreated",
        request_id=request_id,
        intent_digest=intent_digest,
    )
    assert not await store.complete_cleanup_permit(
        permit.admission_id,
        outcome="deleted",
        request_id=request_id,
        intent_digest=intent_digest,
    )
    assert not await store.complete_cleanup_permit(
        permit.admission_id,
        outcome="recreated",
        request_id=request_id,
        intent_digest="sha256:failed-dv-recreate-g2-old-dv-old-pvc",
    )
    completed = await store.resume_cleanup_permit(
        permit.admission_id,
        owner_kind="job",
        owner_id=owner_id,
        source="controller_failed_dv_recreate",
        request_id=request_id,
        intent_digest=intent_digest,
    )
    assert not completed.allowed
    assert completed.reason == "cleanup_request_already_completed"
    assert completed.completed_outcome == "recreated"


@pytest.mark.asyncio
async def test_nested_vm_cleanup_child_survives_parent_completion(app_pg) -> None:
    from types import SimpleNamespace
    from orchestrator.services.vm_workspace_recovery_store import (
        acquire_vm_cleanup_permit,
    )

    store = VMWorkspaceRecoveryStore(app_pg)
    owner_id, lease_token = await insert_leased_job(app_pg)
    hold = admission_kwargs(owner_id, lease_token)
    pvc_uid, generation = hold["root_pvc_uid"], hold["provision_generation"]
    identity = SimpleNamespace(
        provision_generation=str(generation),
        vm_uid="vm-original",
        rootdisk_pvc_uid=str(pvc_uid),
    )
    parent = await acquire_vm_cleanup_permit(
        store,
        owner_kind="job",
        owner_id=owner_id,
        identity=identity,
        source="public_vm_delete",
        purge_disk=True,
    )
    boundary = dict(
        owner_kind="job",
        owner_id=owner_id,
        pvc_uid=pvc_uid,
        parent_provision_generation=str(generation),
        expected_vm_uid="vm-original",
        source="controller_rootdisk_delete",
        request_id=uuid4(),
        intent_digest="sha256:exact-child-dv-pvc",
    )
    proof = parent.parent_cleanup
    assert proof
    child = await store.acquire_cleanup_permit(parent_cleanup=proof, **boundary)
    assert child.allowed and child.admission_id != parent.admission_id
    assert (
        await store.acquire_cleanup_permit(parent_cleanup=proof, **boundary)
    ).admission_id == child.admission_id
    for field, value in (
        ("owner_id", uuid4()),
        ("pvc_uid", uuid4()),
        ("parent_provision_generation", str(uuid4())),
        ("expected_vm_uid", "vm-successor"),
        ("source", "controller_rootdisk_adopt"),
    ):
        assert not (
            await store.acquire_cleanup_permit(
                parent_cleanup=proof, **{**boundary, field: value}
            )
        ).allowed
    for field, value in (
        ("source", "lifecycle_vm_delete"),
        ("purge_disk", False),
        ("resource", "checkpoint"),
    ):
        changed = {**proof, "intent": {**proof["intent"], field: value}}
        assert not (
            await store.acquire_cleanup_permit(parent_cleanup=changed, **boundary)
        ).allowed
    assert await store.complete_cleanup_permit(
        parent.admission_id, outcome="identity_superseded"
    )
    # Existing child replay survives parent completion, but no new child does.
    assert (
        await store.acquire_cleanup_permit(parent_cleanup=proof, **boundary)
    ).admission_id == child.admission_id
    assert not (
        await store.acquire_cleanup_permit(
            parent_cleanup=proof, **{**boundary, "request_id": uuid4()}
        )
    ).allowed
    with pytest.raises(
        WorkspaceRecoveryControlConflict, match="crossed its admission boundary"
    ):
        await store.admit_hold(**hold)
    competing = await store.acquire_cleanup_permit(
        owner_kind="job",
        owner_id=owner_id,
        pvc_uid=pvc_uid,
        request_id=uuid4(),
        source="new-root",
        intent_digest="new-root",
    )
    assert not competing.allowed
    assert await store.complete_cleanup_permit(child.admission_id, outcome="deleted")
    assert (await store.admit_hold(**hold)).operation_id is not None


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
    telemetry = CaptureTelemetry()
    store = VMWorkspaceRecoveryStore(
        app_pg, worker_id="test-worker", telemetry=telemetry
    )
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
    ] == [(successor, original.hold_lease_token, "held")]
    assert sum(pin["released_at"] is None for pin in pins) == 1
    assert await store.claim_due(successor) is not None
    assert [call["event"] for call in telemetry.calls].count("retry") == 1


@pytest.mark.asyncio
async def test_retry_activates_successor_pin_before_predecessor_release(app_pg) -> None:
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="controller-sync")
    job_id, lease_token = await insert_leased_job(app_pg)
    kwargs = admission_kwargs(job_id, lease_token)
    kwargs["code"] = WorkspaceRecoveryCode.TOOL_OUTCOME_UNKNOWN
    original = await store.admit_hold(**kwargs)
    original_command = (await store.list_retention_pin_commands())[0]
    assert await store.acknowledge_retention_pin(
        original_command,
        {
            "state": "active",
            "recovery_id": str(original.operation_id),
            "pvc_uid": str(original_command.pvc_uid),
            "provision_generation": str(original_command.provision_generation),
            "pin_uid": "predecessor-pin",
            "resource_version": "1",
        },
    )
    retried = await store.retry_paused(
        job_id=job_id,
        operation_id=original.operation_id,
        request_id=uuid4(),
        actor_kind="user",
        actor_id="operator",
    )
    successor_id = UUID(retried["operation_id"])

    commands = await store.list_retention_pin_commands()
    assert [(command.recovery_id, command.desired_state) for command in commands] == [
        (successor_id, "active")
    ]
    successor_command = commands[0]
    assert await store.acknowledge_retention_pin(
        successor_command,
        {
            "state": "active",
            "recovery_id": str(successor_id),
            "pvc_uid": str(successor_command.pvc_uid),
            "provision_generation": str(successor_command.provision_generation),
            "pin_uid": "successor-pin",
            "resource_version": "2",
        },
    )

    commands = await store.list_retention_pin_commands()
    assert [(command.recovery_id, command.desired_state) for command in commands] == [
        (original.operation_id, "released")
    ]


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
async def test_terminal_checkpoint_prune_serializes_cleanup_before_recovery(
    app_pg, monkeypatch
) -> None:
    """A hold cannot commit after the prune's check but before its delete."""

    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id, lease_token = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=1)
    blocker, blocker_tx = await hold_checkpoint_row_lock(app_pg, job_id)
    db = postgres_db(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="recovery-racer")

    prune_task = asyncio.create_task(db.delete_checkpoint_thread(str(job_id)))
    hold_task = None
    try:
        await wait_for_checkpoint_cleanup_admission(app_pg, job_id)
        hold_task = asyncio.create_task(
            store.admit_hold(**admission_kwargs(job_id, lease_token))
        )
        await asyncio.sleep(0.1)
        assert not hold_task.done(), "recovery crossed an active prune boundary"
        hold_task.cancel()
        with suppress(asyncio.CancelledError):
            await hold_task
    finally:
        await blocker_tx.commit()
        await app_pg.release(blocker)
        if hold_task is not None and not hold_task.done():
            hold_task.cancel()
        if not prune_task.done():
            await prune_task

    assert await prune_task == 3
    assert await checkpoint_count(app_pg, job_id) == 0
    assert (
        await checkpoint_cleanup_outcome(app_pg, job_id, "terminal_checkpoint_prune")
        == "completed"
    )


@pytest.mark.asyncio
async def test_terminal_checkpoint_prune_serializes_recovery_before_cleanup(
    app_pg, monkeypatch
) -> None:
    """A committed hold wins when pruning waited on the same owner lock."""

    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id, lease_token = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=1)
    db = postgres_db(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="recovery-winner")
    conn = await app_pg.acquire()
    transaction = conn.transaction()
    await transaction.start()
    try:
        recovery = await store.admit_hold(
            **admission_kwargs(job_id, lease_token), _conn=conn
        )
        prune_task = asyncio.create_task(
            db.delete_checkpoint_thread(str(job_id), strict=True)
        )
        await asyncio.sleep(0.1)
        assert not prune_task.done(), "prune bypassed the recovery owner lock"
        await transaction.commit()
    except BaseException:
        await transaction.rollback()
        raise
    finally:
        await app_pg.release(conn)

    assert recovery.operation_id is not None
    with pytest.raises(RuntimeError, match="blocked by workspace recovery authority"):
        await prune_task
    assert await checkpoint_count(app_pg, job_id) == 1


@pytest.mark.asyncio
async def test_global_checkpoint_prune_serializes_cleanup_before_recovery(
    app_pg, monkeypatch
) -> None:
    """Global retention publishes authority before waiting on checkpoint I/O."""

    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id, lease_token = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=3)
    blocker, blocker_tx = await hold_checkpoint_row_lock(app_pg, job_id)
    db = postgres_db(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="global-racer")

    prune_task = asyncio.create_task(db.prune_checkpoints_keep_last(1))
    hold_task = None
    try:
        await wait_for_checkpoint_cleanup_admission(app_pg, job_id)
        hold_task = asyncio.create_task(
            store.admit_hold(**admission_kwargs(job_id, lease_token))
        )
        await asyncio.sleep(0.1)
        assert not hold_task.done(), "recovery crossed global retention deletion"
        hold_task.cancel()
        with suppress(asyncio.CancelledError):
            await hold_task
    finally:
        await blocker_tx.commit()
        await app_pg.release(blocker)
        if hold_task is not None and not hold_task.done():
            hold_task.cancel()
        if not prune_task.done():
            await prune_task

    assert await prune_task == 6
    assert await checkpoint_count(app_pg, job_id) == 1
    assert (
        await checkpoint_cleanup_outcome(
            app_pg, job_id, f"checkpoint_retention_prune:v1:thread:{job_id}:keep:1"
        )
        == "completed"
    )


@pytest.mark.asyncio
async def test_global_checkpoint_prune_serializes_recovery_before_cleanup(
    app_pg, monkeypatch
) -> None:
    """Global retention preserves every checkpoint of an unresolved recovery."""

    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id, lease_token = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=3)
    db = postgres_db(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="global-winner")
    conn = await app_pg.acquire()
    transaction = conn.transaction()
    await transaction.start()
    try:
        recovery = await store.admit_hold(
            **admission_kwargs(job_id, lease_token), _conn=conn
        )
        prune_task = asyncio.create_task(db.prune_checkpoints_keep_last(1))
        await asyncio.sleep(0.1)
        assert not prune_task.done(), "global prune bypassed recovery owner lock"
        await transaction.commit()
    except BaseException:
        await transaction.rollback()
        raise
    finally:
        await app_pg.release(conn)

    assert recovery.operation_id is not None
    assert await prune_task == 0
    assert await checkpoint_count(app_pg, job_id) == 3


@pytest.mark.asyncio
async def test_terminal_checkpoint_prune_resumes_cancelled_open_admission(
    app_pg, monkeypatch
) -> None:
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id, _ = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=2)
    blocker, blocker_tx = await hold_checkpoint_row_lock(app_pg, job_id)
    db = postgres_db(app_pg)

    prune_task = asyncio.create_task(db.delete_checkpoint_thread(str(job_id)))
    try:
        await wait_for_checkpoint_cleanup_admission(app_pg, job_id)
        prune_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await prune_task
    finally:
        await blocker_tx.rollback()
        await app_pg.release(blocker)

    assert await checkpoint_row_counts(app_pg, job_id) == (2, 2, 2)
    admissions = await checkpoint_cleanup_admissions(app_pg, job_id)
    assert len(admissions) == 1
    assert admissions[0]["completed_at"] is None

    assert await db.delete_checkpoint_thread(str(job_id), strict=True) == 6
    assert await checkpoint_row_counts(app_pg, job_id) == (0, 0, 0)
    replayed = await checkpoint_cleanup_admissions(app_pg, job_id)
    assert len(replayed) == 1
    assert replayed[0]["id"] == admissions[0]["id"]
    assert replayed[0]["outcome"] == "completed"


@pytest.mark.asyncio
async def test_global_checkpoint_prune_resumes_cancelled_generation_then_advances(
    app_pg, monkeypatch
) -> None:
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id, _ = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=5)
    blocker, blocker_tx = await hold_checkpoint_row_lock(app_pg, job_id)
    db = postgres_db(app_pg)

    prune_task = asyncio.create_task(db.prune_checkpoints_keep_last(3))
    try:
        await wait_for_checkpoint_cleanup_admission(app_pg, job_id)
        prune_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await prune_task
    finally:
        await blocker_tx.rollback()
        await app_pg.release(blocker)

    assert await checkpoint_row_counts(app_pg, job_id) == (5, 5, 5)
    first = await checkpoint_cleanup_admissions(app_pg, job_id)
    assert len(first) == 1 and first[0]["completed_at"] is None

    # The changed policy first completes the durable keep-3 generation, then
    # opens and applies keep-1 as a new generation in the same sweep.
    assert await db.prune_checkpoints_keep_last(1) == 12
    replayed = await checkpoint_cleanup_admissions(app_pg, job_id)
    assert len(replayed) == 2
    assert replayed[0]["id"] == first[0]["id"]
    assert all(row["outcome"] == "completed" for row in replayed)
    assert await checkpoint_row_counts(app_pg, job_id) == (1, 1, 1)

    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=4)
    assert await db.prune_checkpoints_keep_last(1) == 9
    generations = await checkpoint_cleanup_admissions(app_pg, job_id)
    assert len(generations) == 3
    assert all(row["outcome"] == "completed" for row in generations)


@pytest.mark.asyncio
async def test_orphan_uuid_checkpoints_prune_without_stranding_authority(
    app_pg, monkeypatch
):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id = uuid4()
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=3)
    db = postgres_db(app_pg)
    assert await db.prune_checkpoints_keep_last(1) == 6
    assert await db.delete_checkpoint_thread(str(job_id), strict=True) == 3
    assert await checkpoint_row_counts(app_pg, job_id) == (0, 0, 0)
    assert all(
        row["outcome"] == "completed"
        for row in await checkpoint_cleanup_admissions(app_pg, job_id)
    )


@pytest.mark.asyncio
async def test_retention_relaxed_policy_discovers_prior_generation(app_pg, monkeypatch):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id, _ = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=3)
    db = postgres_db(app_pg)
    authority = await db._acquire_checkpoint_prune_authority(
        str(job_id),
        source=f"checkpoint_retention_prune:v1:thread:{job_id}:keep:1",
        intent={"mode": "keep_last", "keep_n": 1},
    )
    assert authority.allowed
    assert await db.prune_checkpoints_keep_last(5) == 6
    assert all(
        row["outcome"] == "completed"
        for row in await checkpoint_cleanup_admissions(app_pg, job_id)
    )


@pytest.mark.asyncio
async def test_checkpoint_prune_refuses_mismatched_open_admission(
    app_pg, monkeypatch
) -> None:
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id, _ = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=3)
    db = postgres_db(app_pg)
    terminal = await db._acquire_checkpoint_prune_authority(
        str(job_id),
        source="terminal_checkpoint_prune",
        intent={"mode": "delete_thread"},
    )
    assert terminal.allowed and terminal.admission_id is not None

    assert await db.prune_checkpoints_keep_last(1) == 0
    assert await checkpoint_row_counts(app_pg, job_id) == (3, 3, 3)
    admissions = await checkpoint_cleanup_admissions(app_pg, job_id)
    assert len(admissions) == 1
    assert admissions[0]["id"] == terminal.admission_id
    assert admissions[0]["completed_at"] is None

    assert await db.delete_checkpoint_thread(str(job_id)) == 9
    assert await checkpoint_row_counts(app_pg, job_id) == (0, 0, 0)


@pytest.mark.parametrize("failure_table", ["checkpoint_writes", "checkpoint_blobs"])
@pytest.mark.asyncio
async def test_terminal_checkpoint_prune_rolls_back_statement_error_and_retries(
    app_pg, monkeypatch, failure_table
) -> None:
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id, _ = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=2)
    db = postgres_db(app_pg)
    async with app_pg.acquire() as conn:
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION fail_checkpoint_prune() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN
                RAISE EXCEPTION 'injected checkpoint prune failure';
            END $$
            """
        )
        await conn.execute(
            f"CREATE TRIGGER fail_checkpoint_prune_trigger BEFORE DELETE "
            f"ON {failure_table} FOR EACH STATEMENT "
            "EXECUTE FUNCTION fail_checkpoint_prune()"
        )

    try:
        assert await db.delete_checkpoint_thread(str(job_id)) == 0
        assert await checkpoint_row_counts(app_pg, job_id) == (2, 2, 2)
        admissions = await checkpoint_cleanup_admissions(app_pg, job_id)
        assert len(admissions) == 1
        assert admissions[0]["completed_at"] is None
    finally:
        async with app_pg.acquire() as conn:
            await conn.execute(
                f"DROP TRIGGER IF EXISTS fail_checkpoint_prune_trigger "
                f"ON {failure_table}"
            )
            await conn.execute("DROP FUNCTION IF EXISTS fail_checkpoint_prune()")

    assert await db.delete_checkpoint_thread(str(job_id)) == 6
    assert await checkpoint_row_counts(app_pg, job_id) == (0, 0, 0)
    replayed = await checkpoint_cleanup_admissions(app_pg, job_id)
    assert len(replayed) == 1
    assert replayed[0]["id"] == admissions[0]["id"]
    assert replayed[0]["outcome"] == "completed"


@pytest.mark.parametrize("failure_table", ["checkpoints", "checkpoint_writes"])
@pytest.mark.asyncio
async def test_global_checkpoint_prune_rolls_back_statement_error_and_retries(
    app_pg, monkeypatch, failure_table
) -> None:
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id, _ = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=3)
    db = postgres_db(app_pg)
    async with app_pg.acquire() as conn:
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION fail_checkpoint_prune() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN
                RAISE EXCEPTION 'injected checkpoint prune failure';
            END $$
            """
        )
        await conn.execute(
            f"CREATE TRIGGER fail_checkpoint_prune_trigger BEFORE DELETE "
            f"ON {failure_table} FOR EACH STATEMENT "
            "EXECUTE FUNCTION fail_checkpoint_prune()"
        )

    try:
        assert await db.prune_checkpoints_keep_last(1) == 0
        assert await checkpoint_row_counts(app_pg, job_id) == (3, 3, 3)
        admissions = await checkpoint_cleanup_admissions(app_pg, job_id)
        assert len(admissions) == 1
        assert admissions[0]["completed_at"] is None
    finally:
        async with app_pg.acquire() as conn:
            await conn.execute(
                f"DROP TRIGGER IF EXISTS fail_checkpoint_prune_trigger "
                f"ON {failure_table}"
            )
            await conn.execute("DROP FUNCTION IF EXISTS fail_checkpoint_prune()")

    assert await db.prune_checkpoints_keep_last(1) == 6
    assert await checkpoint_row_counts(app_pg, job_id) == (1, 1, 1)
    replayed = await checkpoint_cleanup_admissions(app_pg, job_id)
    assert len(replayed) == 1
    assert replayed[0]["id"] == admissions[0]["id"]
    assert replayed[0]["outcome"] == "completed"


@pytest.mark.asyncio
async def test_terminal_checkpoint_prune_missing_table_uses_savepoint(
    app_pg, monkeypatch
) -> None:
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    job_id, _ = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=2)
    db = postgres_db(app_pg)
    async with app_pg.acquire() as conn:
        await conn.execute("DROP TABLE checkpoint_blobs")

    assert await db.delete_checkpoint_thread(str(job_id)) == 4
    async with app_pg.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM checkpoint_writes WHERE thread_id=$1",
                str(job_id),
            )
            == 0
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM checkpoints WHERE thread_id=$1", str(job_id)
            )
            == 0
        )
    admissions = await checkpoint_cleanup_admissions(app_pg, job_id)
    assert len(admissions) == 1
    assert admissions[0]["outcome"] == "completed"


@pytest.mark.asyncio
async def test_terminal_checkpoint_prune_batch_cap_rolls_back_and_retries(
    app_pg, monkeypatch
) -> None:
    import orchestrator.database.postgres as postgres_module

    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    monkeypatch.setattr(postgres_module, "_CHECKPOINT_DELETE_BATCH", 2)
    monkeypatch.setattr(postgres_module, "_CHECKPOINT_DELETE_MAX_BATCHES", 1)
    job_id, _ = await insert_leased_job(app_pg)
    await prepare_checkpoint_rows(app_pg, job_id, checkpoint_count=3)
    db = postgres_db(app_pg)

    assert await db.delete_checkpoint_thread(str(job_id)) == 0
    assert await checkpoint_row_counts(app_pg, job_id) == (3, 3, 3)
    admissions = await checkpoint_cleanup_admissions(app_pg, job_id)
    assert len(admissions) == 1
    assert admissions[0]["completed_at"] is None

    monkeypatch.setattr(postgres_module, "_CHECKPOINT_DELETE_MAX_BATCHES", 2)
    assert await db.delete_checkpoint_thread(str(job_id)) == 9
    assert await checkpoint_row_counts(app_pg, job_id) == (0, 0, 0)
    replayed = await checkpoint_cleanup_admissions(app_pg, job_id)
    assert len(replayed) == 1
    assert replayed[0]["id"] == admissions[0]["id"]
    assert replayed[0]["outcome"] == "completed"


def recovery_guest_network(challenge: str = "fresh-challenge") -> dict:
    return {
        "challenge": challenge,
        "boot_id": "00000000-0000-4000-8000-000000000041",
        "machine_id": "41" * 16,
        "interfaces": [
            {
                "ifname": "eth0",
                "address": "10.0.2.15",
                "mac": "02:00:00:00:00:41",
            }
        ],
        "address": "10.0.2.15",
        "routes": [{"dst": "default", "gateway": "10.0.2.2"}],
        "default_route": {"dst": "default", "gateway": "10.0.2.2"},
        "dns": "nameserver 10.0.2.3",
        "netplan_sha256": {"/etc/netplan/50-cloud-init.yaml": "a" * 64},
        "networkd_sha256": {},
        "cloud_init_instance_id": "iid-datasource-none",
        "cloud_init_cache_identity": "b" * 64,
        "cloud_init_cache_cleaned": False,
    }


def same_runtime_observation(job_id: UUID, values: dict) -> dict:
    return {
        "ready": True,
        "authenticated": True,
        "ambiguous": False,
        "owner_kind": "job",
        "owner_id": str(job_id),
        "provision_generation": str(values["provision_generation"]),
        "vm_uid": str(values["vm_uid"]),
        "root_pvc_uid": str(values["root_pvc_uid"]),
        "prior_runtime": "same_runtime",
        "remote_operations": "settled",
        "continuation": "safe",
        "successor": {
            "vmi_uid": str(values["prior_vmi_uid"]),
            "launcher_uid": str(values["prior_launcher_uid"]),
            "node_uid": "node-test",
            "pod_ip": "10.42.0.90",
            "ssh_registration_id": "51" * 16,
            "guest_boot_id": "00000000-0000-4000-8000-000000000041",
            "guest_machine_id": "41" * 16,
            "interface_mac": "02:00:00:00:00:41",
            "guest_network": recovery_guest_network(),
        },
    }


@pytest.mark.asyncio
async def test_expired_probe_claim_cannot_stage_returned_observation(app_pg) -> None:
    job_id, lease_token = await insert_leased_job(app_pg)
    values = admission_kwargs(job_id, lease_token)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="leader-a")
    admitted = await store.admit_hold(**values)
    claimed = await store.claim_due(admitted.operation_id)
    assert claimed is not None
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET claimed_until=clock_timestamp()-interval '1 second' "
            "WHERE id=$1",
            admitted.operation_id,
        )
        await conn.execute(
            "UPDATE vm_workspace_recovery_probe_slots "
            "SET leased_until=clock_timestamp()-interval '1 second' WHERE recovery_id=$1",
            admitted.operation_id,
        )

    assert (
        await store.stage_observation(
            operation_id=claimed.operation_id,
            version=claimed.version,
            claim_token=claimed.claim_token,
            phase="attesting",
            observation=same_runtime_observation(job_id, values),
        )
        is None
    )
    observation = same_runtime_observation(job_id, values)
    assert not await store.release_recovered(
        operation_id=claimed.operation_id,
        version=claimed.version,
        claim_token=claimed.claim_token,
        initial_observation=observation,
        final_observation=observation,
        resume_receipt={"kind": "expired"},
    )
    async with app_pg.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state,lease_token,park_reason FROM run_queue WHERE unit_id=$1",
            job_id,
        )
        operation = await conn.fetchrow(
            "SELECT phase,resolved_at FROM vm_workspace_recoveries WHERE id=$1",
            admitted.operation_id,
        )
    assert tuple(queue) == ("parked", lease_token + 1, "workspace_recovery")
    assert tuple(operation) == ("observing", None)


@pytest.mark.asyncio
async def test_stale_result_cannot_delete_successor_probe_slot(app_pg) -> None:
    job_id, lease_token = await insert_leased_job(app_pg)
    values = admission_kwargs(job_id, lease_token)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="leader-a")
    admitted = await store.admit_hold(**values)
    claimed = await store.claim_due(admitted.operation_id)
    assert claimed is not None
    staged = await store.stage_observation(
        operation_id=claimed.operation_id,
        version=claimed.version,
        claim_token=claimed.claim_token,
        phase="attesting",
        observation=same_runtime_observation(job_id, values),
    )
    assert staged is not None

    assert not await store.defer_claim(
        operation_id=claimed.operation_id,
        version=claimed.version,
        claim_token=claimed.claim_token,
        phase="waiting_runtime",
        next_check_seconds=1,
    )
    assert await store.claim_is_current(staged)
    async with app_pg.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_recovery_probe_slots "
                "WHERE recovery_id=$1 AND claim_token=$2",
                staged.operation_id,
                staged.claim_token,
            )
            == 1
        )


@pytest.mark.asyncio
async def test_release_requires_complete_attestation_evidence(app_pg) -> None:
    job_id, lease_token = await insert_leased_job(app_pg)
    values = admission_kwargs(job_id, lease_token)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="leader-a")
    admitted = await store.admit_hold(**values)
    claimed = await store.claim_due(admitted.operation_id)
    assert claimed is not None

    assert not await store.release_recovered(
        operation_id=claimed.operation_id,
        version=claimed.version,
        claim_token=claimed.claim_token,
        initial_observation=None,
        final_observation=None,
        resume_receipt={"kind": "invalid"},
    )
    async with app_pg.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state,lease_token,park_reason FROM run_queue WHERE unit_id=$1",
            job_id,
        )
    assert tuple(queue) == ("parked", lease_token + 1, "workspace_recovery")


@pytest.mark.asyncio
async def test_store_rejects_changed_guest_identity_on_final_attestation(
    app_pg,
) -> None:
    job_id, lease_token = await insert_leased_job(app_pg)
    values = admission_kwargs(job_id, lease_token)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_build_object('vm',jsonb_build_object("
            "'provision_generation',$2::text,'vm_uid',$3::text,"
            "'rootdisk_pvc_uid',$4::text)) WHERE id=$1",
            job_id,
            str(values["provision_generation"]),
            str(values["vm_uid"]),
            str(values["root_pvc_uid"]),
        )
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="leader-a")
    admitted = await store.admit_hold(**values)
    claimed = await store.claim_due(admitted.operation_id)
    assert claimed is not None
    initial = same_runtime_observation(job_id, values)
    final = same_runtime_observation(job_id, values)
    final["successor"]["guest_machine_id"] = "42" * 16
    final["successor"]["guest_network"]["machine_id"] = "42" * 16

    assert not await store.release_recovered(
        operation_id=claimed.operation_id,
        version=claimed.version,
        claim_token=claimed.claim_token,
        initial_observation=initial,
        final_observation=final,
        resume_receipt={"kind": "malformed"},
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
        # Live stateless contract: pooled executors never register. The
        # bundle must authorize without an agents row; assert its absence
        # before exercising disabled-then-enabled accounting.
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM agents WHERE hostname=$1 AND pod_uid=$2",
                POD_NAME,
                POD_UID,
            )
            == 0
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
        # No registration was created as a side effect of the repair.
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM agents WHERE hostname=$1 AND pod_uid=$2",
                POD_NAME,
                POD_UID,
            )
            == 0
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
    recovery_ids = [await insert_recovery(app_pg, owner_id=uuid4()) for _ in range(5)]
    async with app_pg.acquire() as conn:
        for index, recovery_id in enumerate(recovery_ids):
            await conn.execute(
                "UPDATE vm_workspace_recoveries SET latest_observation=$2::jsonb "
                "WHERE id=$1",
                recovery_id,
                json.dumps({"successor": {"node_uid": f"node-{index}"}}),
            )
    stores = [
        VMWorkspaceRecoveryStore(app_pg, worker_id=f"reconciler-{index}")
        for index in range(5)
    ]

    claims = await asyncio.gather(
        *(
            store.claim_due(operation_id)
            for store, operation_id in zip(stores, recovery_ids)
        )
    )

    assert sum(item is not None for item in claims) == 4
    async with app_pg.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_recovery_probe_slots"
            )
            == 4
        )
        assert (
            await conn.fetchval(
                "SELECT count(DISTINCT global_slot) FROM vm_workspace_recovery_probe_slots"
            )
            == 4
        )


@pytest.mark.asyncio
async def test_claim_due_enforces_one_durable_probe_per_known_node(app_pg) -> None:
    first = await insert_recovery(app_pg, owner_id=uuid4())
    second = await insert_recovery(app_pg, owner_id=uuid4())
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_recoveries SET latest_observation="
            '\'{"successor":{"node_uid":"node-8"}}\'::jsonb '
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
async def test_claim_due_serializes_all_unknown_node_probes(app_pg) -> None:
    first = await insert_recovery(app_pg, owner_id=uuid4())
    second = await insert_recovery(app_pg, owner_id=uuid4())

    first_claim, second_claim = await asyncio.gather(
        VMWorkspaceRecoveryStore(app_pg, worker_id="leader-a").claim_due(first),
        VMWorkspaceRecoveryStore(app_pg, worker_id="leader-b").claim_due(second),
    )

    assert (first_claim is None) != (second_claim is None)
    claimed = first_claim or second_claim
    assert claimed is not None and claimed.node_key == "unknown"


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
            await conn.fetchval(
                "SELECT context->'vm'->>'status' FROM jobs WHERE id=$1", job_id
            )
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
async def test_historical_stop_receipt_is_accepted_once_and_reused_by_new_term(
    app_pg,
) -> None:
    recovery_id = await insert_recovery(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="receipt-controller")
    first = await store.claim_due(recovery_id)
    assert first is not None
    evidence = {
        "protocol_version": 1,
        "vm_uid": str(first.captured_identity["vm_uid"]),
        "vmi_uid": str(first.captured_identity["prior_vmi_uid"]),
        "launcher_uid": str(first.captured_identity["prior_launcher_uid"]),
        "container_id": "containerd://old-compute",
        "root_pvc_uid": str(first.captured_identity["root_pvc_uid"]),
        "controller_identity": "controller/pod-1",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "containers": [
            {
                "name": "compute",
                "kind": "regular",
                "container_id": "containerd://old-compute",
                "terminated_container_id": "containerd://old-compute",
                "restart_count": 0,
                "state": "terminated",
                "last_state": None,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "reason": "Completed",
            }
        ],
        "declared_containers": {"regular": ["compute"], "init": []},
        "pod_terminal": {"phase": "Succeeded", "restart_policy": "Never"},
    }
    digest = await store.accept_stop_evidence(first, evidence)
    assert digest and digest.startswith("sha256:")
    assert await store.accept_stop_evidence(first, evidence) == digest
    assert await store.defer_claim(
        operation_id=first.operation_id,
        version=first.version,
        claim_token=first.claim_token,
        phase="verifying_stop",
        next_check_seconds=0,
    )
    second = await store.claim_due(recovery_id)
    assert second is not None and second.claim_token > first.claim_token
    assert await store.trusted_stop_receipt(second) == digest


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "missing_compute",
        "unknown_reason",
        "undeclared",
        "duplicate",
        "termination_identity_mismatch",
    ],
)
async def test_stop_receipt_store_rejects_incomplete_container_evidence(
    app_pg, invalid
) -> None:
    recovery_id = await insert_recovery(app_pg)
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="receipt-validator")
    recovery_claim = await store.claim_due(recovery_id)
    assert recovery_claim is not None
    now = datetime.now(timezone.utc).isoformat()
    evidence = {
        "protocol_version": 1,
        "vm_uid": str(recovery_claim.captured_identity["vm_uid"]),
        "vmi_uid": str(recovery_claim.captured_identity["prior_vmi_uid"]),
        "launcher_uid": str(recovery_claim.captured_identity["prior_launcher_uid"]),
        "container_id": "containerd://old-compute",
        "root_pvc_uid": str(recovery_claim.captured_identity["root_pvc_uid"]),
        "controller_identity": "controller/pod-1",
        "observed_at": now,
        "containers": [
            {
                "name": "compute",
                "kind": "regular",
                "container_id": "containerd://old-compute",
                "terminated_container_id": "containerd://old-compute",
                "restart_count": 0,
                "state": "terminated",
                "last_state": None,
                "finished_at": now,
                "reason": "Completed",
            },
            {
                "name": "guest-console-log",
                "kind": "regular",
                "container_id": "containerd://old-log",
                "terminated_container_id": "containerd://old-log",
                "restart_count": 0,
                "state": "terminated",
                "last_state": None,
                "finished_at": now,
                "reason": "Completed",
            },
        ],
        "declared_containers": {
            "regular": ["compute", "guest-console-log"],
            "init": [],
        },
        "pod_terminal": {"phase": "Succeeded", "restart_policy": "Never"},
    }
    if invalid == "missing_compute":
        evidence["containers"].pop(0)
    elif invalid == "unknown_reason":
        evidence["containers"][0]["reason"] = "ContainerStatusUnknown"
    elif invalid == "undeclared":
        evidence["containers"].append(
            {
                "name": "unexpected-sidecar",
                "kind": "regular",
                "container_id": "containerd://unexpected",
                "terminated_container_id": "containerd://unexpected",
                "restart_count": 0,
                "state": "terminated",
                "last_state": None,
                "finished_at": now,
                "reason": "Completed",
            }
        )
    elif invalid == "duplicate":
        evidence["containers"].append(dict(evidence["containers"][0]))
    else:
        evidence["containers"][0]["terminated_container_id"] = (
            "containerd://different-incarnation"
        )

    assert await store.accept_stop_evidence(recovery_claim, evidence) is None


@pytest.mark.asyncio
async def test_controller_pin_desired_and_ack_state_survive_store_restart(
    app_pg,
) -> None:
    recovery_id = await insert_recovery(app_pg)
    async with app_pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT provision_generation,root_pvc_uid FROM vm_workspace_recoveries "
            "WHERE id=$1",
            recovery_id,
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_retention_pins "
            "(recovery_id,pvc_uid,provision_generation) VALUES ($1,$2,$3)",
            recovery_id,
            row["root_pvc_uid"],
            row["provision_generation"],
        )
    first_store = VMWorkspaceRecoveryStore(app_pg, worker_id="controller-sync-a")
    commands = await first_store.list_retention_pin_commands()
    assert len(commands) == 1 and commands[0].desired_state == "active"
    command = commands[0]
    assert await first_store.acknowledge_retention_pin(
        command,
        {
            "state": "active",
            "recovery_id": str(recovery_id),
            "pvc_uid": str(row["root_pvc_uid"]),
            "provision_generation": str(row["provision_generation"]),
            "pin_uid": "controller-pin-uid",
            "resource_version": "11",
        },
    )

    restarted = VMWorkspaceRecoveryStore(app_pg, worker_id="controller-sync-b")
    assert await restarted.list_retention_pin_commands() == []
    async with app_pg.acquire() as conn:
        persisted = await conn.fetchrow(
            "SELECT controller_pinned_at,controller_pin_uid,"
            "controller_pin_resource_version FROM "
            "vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
            recovery_id,
        )
    assert persisted["controller_pinned_at"] is not None
    assert persisted["controller_pin_uid"] == "controller-pin-uid"
    assert persisted["controller_pin_resource_version"] == "11"


@pytest.mark.asyncio
async def test_controller_pin_release_is_exact_and_failed_sync_stays_durable(
    app_pg,
) -> None:
    recovery_id = await insert_recovery(app_pg)
    async with app_pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT provision_generation,root_pvc_uid FROM vm_workspace_recoveries "
            "WHERE id=$1",
            recovery_id,
        )
        await conn.execute(
            "INSERT INTO vm_workspace_recovery_retention_pins "
            "(recovery_id,pvc_uid,provision_generation,controller_pinned_at,"
            "controller_pin_uid,controller_pin_resource_version,released_at) "
            "VALUES ($1,$2,$3,clock_timestamp(),'pin-current','4',clock_timestamp())",
            recovery_id,
            row["root_pvc_uid"],
            row["provision_generation"],
        )
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="controller-sync")
    command = (await store.list_retention_pin_commands())[0]
    await store.defer_retention_pin_command(command, error="controller unavailable")
    async with app_pg.acquire() as conn:
        persisted = await conn.fetchrow(
            "SELECT controller_released_at,controller_sync_error FROM "
            "vm_workspace_recovery_retention_pins WHERE recovery_id=$1",
            recovery_id,
        )
        await conn.execute(
            "UPDATE vm_workspace_recovery_retention_pins "
            "SET controller_sync_after=clock_timestamp() WHERE recovery_id=$1",
            recovery_id,
        )
    assert persisted["controller_released_at"] is None
    assert persisted["controller_sync_error"] == "controller unavailable"
    command = (await store.list_retention_pin_commands())[0]
    assert not await store.acknowledge_retention_pin(
        command,
        {
            "state": "released",
            "recovery_id": str(recovery_id),
            "pvc_uid": str(row["root_pvc_uid"]),
            "provision_generation": str(row["provision_generation"]),
            "pin_uid": "pin-stale",
            "resource_version": "4",
        },
    )
    assert await store.acknowledge_retention_pin(
        command,
        {
            "state": "released",
            "recovery_id": str(recovery_id),
            "pvc_uid": str(row["root_pvc_uid"]),
            "provision_generation": str(row["provision_generation"]),
            "pin_uid": "pin-current",
            "resource_version": "4",
        },
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
            "INSERT INTO vm_workspace_recovery_probe_slots "
            "(recovery_id,global_slot,node_key,claim_token,leased_until) "
            "VALUES ($1,0,'deadline-node',1,clock_timestamp()+interval '30 seconds')",
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
        identity = await conn.fetchrow(
            "SELECT owner_kind,owner_id,provision_generation,vm_uid,"
            "prior_vmi_uid,prior_launcher_uid,root_pvc_uid "
            "FROM vm_workspace_recoveries WHERE id=$1",
            recovery_id,
        )

    observation = {
        "ready": True,
        "authenticated": True,
        "ambiguous": False,
        "owner_kind": identity["owner_kind"],
        "owner_id": str(identity["owner_id"]),
        "provision_generation": str(identity["provision_generation"]),
        "vm_uid": str(identity["vm_uid"]),
        "root_pvc_uid": str(identity["root_pvc_uid"]),
        "prior_runtime": "same_runtime",
        "remote_operations": "settled",
        "continuation": "safe",
        "successor": {
            "vmi_uid": str(identity["prior_vmi_uid"]),
            "launcher_uid": str(identity["prior_launcher_uid"]),
            "node_uid": "deadline-node",
            "pod_ip": "10.42.0.99",
            "ssh_registration_id": "53" * 16,
            "guest_boot_id": "00000000-0000-4000-8000-000000000041",
            "guest_machine_id": "41" * 16,
            "interface_mac": "02:00:00:00:00:41",
            "guest_network": recovery_guest_network(),
        },
    }

    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-a")
    assert not await store.release_recovered(
        operation_id=recovery_id,
        version=2,
        claim_token=1,
        initial_observation=observation,
        final_observation=observation.copy(),
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
    values = admission_kwargs(job_id, lease_token)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_build_object('vm',jsonb_build_object("
            "'provision_generation',$2::text,'vm_uid',$3::text,"
            "'rootdisk_pvc_uid',$4::text)) WHERE id=$1",
            job_id,
            str(values["provision_generation"]),
            str(values["vm_uid"]),
            str(values["root_pvc_uid"]),
        )
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-a")
    admitted = await store.admit_hold(**values)
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
        initial_observation=same_runtime_observation(job_id, values),
        final_observation=same_runtime_observation(job_id, values),
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
    from shared.vm_provisioning_phases import observe_provisioning
    from tests.test_vm_provisioning_phases import running

    job_id = uuid4()
    attempt_token = 27
    request_id = uuid4()
    provision_generation = uuid4()
    vm_uid = uuid4()
    vmi_uid = uuid4()
    launcher_uid = uuid4()
    pvc_uid = uuid4()
    async with app_pg.acquire() as conn:
        phase_now = await conn.fetchval(
            "SELECT extract(epoch FROM clock_timestamp())::double precision"
        )
    phase = observe_provisioning(
        None,
        running(
            owner_id=str(job_id),
            namespace="workers",
            provision_generation=str(provision_generation),
            vm_uid=str(vm_uid),
            vmi_uid=str(vmi_uid),
            rootdisk_pvc_uid=str(pvc_uid),
        ),
        now=phase_now,
    )
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
        await conn.execute(
            "UPDATE jobs SET context=jsonb_build_object('vm',$2::jsonb) WHERE id=$1",
            job_id,
            json.dumps(
                {
                    "provision_generation": str(provision_generation),
                    "vm_uid": str(vm_uid),
                    "vmi_uid": str(vmi_uid),
                    "rootdisk_pvc_uid": str(pvc_uid),
                    "provisioning": phase,
                    "provisioning_revision": 6,
                }
            ),
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
    observation = same_runtime_observation(job_id, kwargs)
    final_observation = same_runtime_observation(job_id, kwargs)
    final_observation["successor"]["ssh_registration_id"] = "52" * 16
    final_observation["successor"]["guest_network"]["challenge"] = "fresh-challenge-two"
    assert await store.release_recovered(
        operation_id=admitted.operation_id,
        version=claim.version,
        claim_token=claim.claim_token,
        initial_observation=observation,
        final_observation=final_observation,
        resume_receipt={"kind": "same_runtime_ready"},
    )
    async with app_pg.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state, lease_token, attempts_since_completion, input_seq, "
            "consumed_seq, park_reason FROM run_queue WHERE unit_id=$1",
            job_id,
        )
        job = await conn.fetchrow(
            "SELECT status, freeze_data, context->'vm' AS vm FROM jobs WHERE id=$1",
            job_id,
        )
        operation = await conn.fetchrow(
            "SELECT phase, resolved_at FROM vm_workspace_recoveries WHERE id=$1",
            admitted.operation_id,
        )
    assert tuple(queue) == ("queued", attempt_token + 1, 1, 5, 3, None)
    assert (job["status"], job["freeze_data"]) == ("paused", None)
    vm = job["vm"] if isinstance(job["vm"], dict) else json.loads(job["vm"])
    assert vm["ssh_registration_id"] == "52" * 16
    assert vm["provisioning"] == phase
    assert vm["provisioning_revision"] == 7
    assert operation["phase"] == "recovered"
    assert operation["resolved_at"] is not None


@pytest.mark.asyncio
async def test_final_recovery_cas_binds_successor_and_releases_once(
    app_pg, monkeypatch
) -> None:
    from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
    from shared.vm_provisioning_phases import observe_provisioning
    from tests.test_vm_provisioning_phase_store import provisioner_with_response, status

    job_id, lease_token = await insert_leased_job(app_pg)
    generation = uuid4()
    vm_uid = uuid4()
    old_vmi_uid = uuid4()
    old_launcher_uid = uuid4()
    pvc_uid = uuid4()
    successor_vmi_uid = uuid4()
    successor_launcher_uid = uuid4()
    predecessor_status = status(
        str(job_id),
        str(generation),
        boot=True,
        namespace="workers",
        vm_uid=str(vm_uid),
        vmi_uid=str(old_vmi_uid),
        rootdisk_pvc_uid=str(pvc_uid),
    )
    async with app_pg.acquire() as conn:
        phase_now = await conn.fetchval(
            "SELECT extract(epoch FROM clock_timestamp())::double precision"
        )
    predecessor_phase = observe_provisioning(
        None, predecessor_status["provisioning"], now=phase_now
    )
    budget = {"boot_attempts": 3, "provision_attempts": 5}
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_build_object('vm',$2::jsonb) WHERE id=$1",
            job_id,
            json.dumps(
                {
                    "provision_generation": str(generation),
                    "vm_uid": str(vm_uid),
                    "vmi_uid": str(old_vmi_uid),
                    "rootdisk_pvc_uid": str(pvc_uid),
                    "vm_name": f"agent-vm-{job_id}",
                    "namespace": "workers",
                    "status": "ssh_pending",
                    "provisioning": predecessor_phase,
                    "provisioning_revision": 4,
                    **budget,
                }
            ),
        )
    db = postgres_db(app_pg)
    phase_store = VMProvisioningPhaseStore(db)
    stale_token = await phase_store.capture(str(job_id), str(generation))
    assert stale_token is not None and stale_token.revision == 4
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
        "ambiguous": False,
        "owner_kind": "job",
        "owner_id": str(job_id),
        "provision_generation": str(generation),
        "vm_uid": str(vm_uid),
        "root_pvc_uid": str(pvc_uid),
        "prior_runtime": "stopped",
        "remote_operations": "settled",
        "continuation": "safe",
        "successor": {
            "vmi_uid": str(successor_vmi_uid),
            "launcher_uid": str(successor_launcher_uid),
            "node_uid": "node-8",
            "pod_ip": "10.42.0.90",
            "ssh_registration_id": "54" * 16,
            "guest_boot_id": "00000000-0000-4000-8000-000000000041",
            "guest_machine_id": "41" * 16,
            "interface_mac": "02:00:00:00:00:41",
            "guest_network": recovery_guest_network(),
        },
    }
    stopped_at = datetime.now(timezone.utc).isoformat()
    receipt = {
        "protocol_version": 1,
        "vm_uid": str(vm_uid),
        "vmi_uid": str(old_vmi_uid),
        "launcher_uid": str(old_launcher_uid),
        "container_id": "containerd://old-compute",
        "root_pvc_uid": str(pvc_uid),
        "controller_identity": "controller/pod-1",
        "observed_at": stopped_at,
        "containers": [
            {
                "name": "compute",
                "kind": "regular",
                "container_id": "containerd://old-compute",
                "terminated_container_id": "containerd://old-compute",
                "restart_count": 0,
                "state": "terminated",
                "last_state": None,
                "finished_at": stopped_at,
                "reason": "Completed",
            }
        ],
        "declared_containers": {"regular": ["compute"], "init": []},
        "pod_terminal": {"phase": "Succeeded", "restart_policy": "Never"},
    }
    digest = await store.accept_stop_evidence(claimed, receipt)
    assert digest is not None
    observation["stop_receipt_digest"] = digest
    staged = await store.stage_observation(
        operation_id=claimed.operation_id,
        version=claimed.version,
        claim_token=claimed.claim_token,
        phase="attesting",
        observation=observation,
    )
    assert staged is not None
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
        pin_released = await conn.fetchval(
            "SELECT released_at IS NOT NULL "
            "FROM vm_workspace_recovery_retention_pins "
            "WHERE recovery_id=$1 AND pvc_uid=$2 AND provision_generation=$3",
            admitted.operation_id,
            pvc_uid,
            generation,
        )
    if isinstance(vm, str):
        vm = json.loads(vm)
    assert operation["phase"] == "recovered" and operation["resolved_at"] is not None
    assert tuple(queue) == ("queued", lease_token + 1)
    assert vm["active_pod_uid"] == str(successor_launcher_uid)
    assert vm["vmi_uid"] == str(successor_vmi_uid)
    assert vm["provisioning"]["identity"]["vmi_uid"] == str(successor_vmi_uid)
    assert vm["provisioning_revision"] == 5
    assert {key: vm[key] for key in budget} == budget
    assert {
        key: value for key, value in vm["provisioning"].items() if key != "identity"
    } == {key: value for key, value in predecessor_phase.items() if key != "identity"}
    assert vm["pod_ip"] == "10.42.0.90"
    assert pin_released is True
    successor_status = status(
        str(job_id),
        str(generation),
        boot=True,
        namespace="workers",
        vm_uid=str(vm_uid),
        vmi_uid=str(successor_vmi_uid),
        rootdisk_pvc_uid=str(pvc_uid),
    )
    successor_status["namespace"] = "workers"
    successor_status["vmi_uid"] = str(successor_vmi_uid)
    assert await phase_store.apply_status(stale_token, successor_status) == "stale"

    async def reply(_request):
        return successor_status

    provisioner = await provisioner_with_response(db, monkeypatch, reply)
    try:
        assert await provisioner.query_status(str(job_id)) is not None
    finally:
        await provisioner._http_client.aclose()
    async with app_pg.acquire() as conn:
        post_query_phase = await conn.fetchval(
            "SELECT context->'vm'->'provisioning' FROM jobs WHERE id=$1", job_id
        )
    if isinstance(post_query_phase, str):
        post_query_phase = json.loads(post_query_phase)
    assert (
        post_query_phase["first_guest_started_at"]
        == predecessor_phase["first_guest_started_at"]
    )
    assert post_query_phase["phase_started_at"] == predecessor_phase["phase_started_at"]

    # A later worker lease can recover the already replaced runtime again.
    # Its predecessor phase is the first successor, not the original VMI.
    async with app_pg.acquire() as conn:
        phase_before_second = await conn.fetchval(
            "SELECT context->'vm'->'provisioning' FROM jobs WHERE id=$1", job_id
        )
        await conn.execute(
            "UPDATE run_queue SET state='leased',lease_token=$2,leased_by='worker-a',"
            "leased_until=clock_timestamp()+interval '1 minute' WHERE unit_id=$1",
            job_id,
            lease_token + 2,
        )
        await conn.execute(
            "INSERT INTO worker_batch_attempts(job_id,lease_token,claimed_attempt) "
            "SELECT unit_id,lease_token,attempts_since_completion FROM run_queue "
            "WHERE unit_id=$1",
            job_id,
        )
    if isinstance(phase_before_second, str):
        phase_before_second = json.loads(phase_before_second)
    second_successor_vmi = uuid4()
    second_successor_launcher = uuid4()
    second_admitted = await store.admit_hold(
        **(
            admission_kwargs(job_id, lease_token + 2)
            | {
                "provision_generation": generation,
                "vm_uid": vm_uid,
                "prior_vmi_uid": successor_vmi_uid,
                "prior_launcher_uid": successor_launcher_uid,
                "root_pvc_uid": pvc_uid,
            }
        )
    )
    second_claim = await store.claim_due(second_admitted.operation_id)
    assert second_claim is not None, second_admitted
    second_receipt = {
        **receipt,
        "vmi_uid": str(successor_vmi_uid),
        "launcher_uid": str(successor_launcher_uid),
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    second_digest = await store.accept_stop_evidence(second_claim, second_receipt)
    assert second_digest is not None
    second_observation = {
        **observation,
        "stop_receipt_digest": second_digest,
        "successor": {
            **observation["successor"],
            "vmi_uid": str(second_successor_vmi),
            "launcher_uid": str(second_successor_launcher),
        },
    }
    second_stage = await store.stage_observation(
        operation_id=second_claim.operation_id,
        version=second_claim.version,
        claim_token=second_claim.claim_token,
        phase="attesting",
        observation=second_observation,
    )
    assert second_stage is not None
    assert await store.release_recovered(
        operation_id=second_stage.operation_id,
        version=second_stage.version,
        claim_token=second_stage.claim_token,
        initial_observation=second_observation,
        final_observation=second_observation.copy(),
        resume_receipt={"kind": "workspace_recovery"},
    )
    async with app_pg.acquire() as conn:
        second_vm = await conn.fetchval(
            "SELECT context->'vm' FROM jobs WHERE id=$1", job_id
        )
    if isinstance(second_vm, str):
        second_vm = json.loads(second_vm)
    assert second_vm["vmi_uid"] == str(second_successor_vmi)
    assert second_vm["provisioning"]["identity"]["vmi_uid"] == str(second_successor_vmi)
    assert {
        key: value
        for key, value in second_vm["provisioning"].items()
        if key != "identity"
    } == {key: value for key, value in phase_before_second.items() if key != "identity"}
    assert second_vm["provisioning_revision"] == vm["provisioning_revision"] + 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    ["wrong_predecessor", "malformed_clock", "invalid_revision", "top_vmi_changed"],
)
async def test_recovery_phase_conflict_keeps_exact_hold(app_pg, invalid) -> None:
    from shared.vm_provisioning_phases import observe_provisioning
    from tests.test_vm_provisioning_phases import running

    job_id, lease_token = await insert_leased_job(app_pg)
    kwargs = admission_kwargs(job_id, lease_token)
    async with app_pg.acquire() as conn:
        phase_now = await conn.fetchval(
            "SELECT extract(epoch FROM clock_timestamp())::double precision"
        )
    phase = observe_provisioning(
        None,
        running(
            owner_id=str(job_id),
            namespace="workers",
            provision_generation=str(kwargs["provision_generation"]),
            vm_uid=str(kwargs["vm_uid"]),
            vmi_uid=str(kwargs["prior_vmi_uid"]),
            rootdisk_pvc_uid=str(kwargs["root_pvc_uid"]),
        ),
        now=phase_now,
    )
    revision = 4
    if invalid == "wrong_predecessor":
        phase["identity"]["vmi_uid"] = str(uuid4())
    elif invalid == "malformed_clock":
        phase["first_guest_started_at"] = -1
    else:
        revision = 2**63 - 2
    vm = {
        "provision_generation": str(kwargs["provision_generation"]),
        "vm_uid": str(kwargs["vm_uid"]),
        "vmi_uid": str(kwargs["prior_vmi_uid"]),
        "rootdisk_pvc_uid": str(kwargs["root_pvc_uid"]),
        "provisioning": phase,
        "provisioning_revision": revision,
    }
    if invalid == "top_vmi_changed":
        vm["vmi_uid"] = str(uuid4())
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_build_object('vm',$2::jsonb) WHERE id=$1",
            job_id,
            json.dumps(vm),
        )
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="leader-a")
    admitted = await store.admit_hold(**kwargs)
    claimed = await store.claim_due(admitted.operation_id)
    assert claimed is not None
    observation = same_runtime_observation(job_id, kwargs)
    assert not await store.release_recovered(
        operation_id=claimed.operation_id,
        version=claimed.version,
        claim_token=claimed.claim_token,
        initial_observation=observation,
        final_observation=observation.copy(),
        resume_receipt={"kind": "same_runtime_ready"},
    )
    async with app_pg.acquire() as conn:
        actual_vm = await conn.fetchval(
            "SELECT context->'vm' FROM jobs WHERE id=$1", job_id
        )
        queue = await conn.fetchrow(
            "SELECT state,lease_token,park_reason FROM run_queue WHERE unit_id=$1",
            job_id,
        )
        operation = await conn.fetchrow(
            "SELECT phase,reason_code,resolved_at FROM vm_workspace_recoveries "
            "WHERE id=$1",
            admitted.operation_id,
        )
    if isinstance(actual_vm, str):
        actual_vm = json.loads(actual_vm)
    assert actual_vm == vm
    assert tuple(queue) == ("parked", admitted.hold_lease_token, "workspace_recovery")
    assert operation["phase"] == "paused_attention"
    assert operation["reason_code"] == WorkspaceRecoveryCode.IDENTITY_CONFLICT.value
    assert operation["resolved_at"] is None


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
        "ambiguous": False,
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
            "ssh_registration_id": "55" * 16,
            "guest_boot_id": "00000000-0000-4000-8000-000000000041",
            "guest_machine_id": "41" * 16,
            "interface_mac": "02:00:00:00:00:41",
            "guest_network": recovery_guest_network(),
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
            claimed_by="reconciler-a",
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
    values = admission_kwargs(parent_id, token)
    async with app_pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_build_object('vm',jsonb_build_object("
            "'provision_generation',$2::text,'vm_uid',$3::text,"
            "'rootdisk_pvc_uid',$4::text)) WHERE id=$1",
            parent_id,
            str(values["provision_generation"]),
            str(values["vm_uid"]),
            str(values["root_pvc_uid"]),
        )
        await conn.execute(
            "INSERT INTO jobs (id, description, status, execution_lane, parent_job_id, context) "
            "VALUES ($1, 'never leased child', 'created', 'stateless', $2, "
            "'{\"inherits_parent_workspace\":true}'::jsonb)",
            child_id,
            parent_id,
        )
    store = VMWorkspaceRecoveryStore(app_pg, worker_id="reconciler-a")
    admitted = await store.admit_hold(**values)
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
    observation = same_runtime_observation(parent_id, values)
    assert await store.release_recovered(
        operation_id=admitted.operation_id,
        version=claim.version,
        claim_token=claim.claim_token,
        initial_observation=observation,
        final_observation=observation.copy(),
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
