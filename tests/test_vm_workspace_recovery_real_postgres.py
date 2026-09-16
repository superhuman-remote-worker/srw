"""Durable VM workspace recovery contracts against PostgreSQL 15."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.community.postgres import PostgresContainer

from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
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
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def app_pg(pg_dsn: str, _schema_applied: None):
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE vm_workspace_recovery_requests, "
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
    deadline_offset: timedelta = timedelta(minutes=15),
) -> UUID:
    async with app_pg.acquire() as conn:
        first = await conn.fetchval("SELECT clock_timestamp()")
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
        for global_slot, node_key in ((1, "node-b"), (2, "node-a")):
            with pytest.raises(asyncpg.UniqueViolationError):
                await conn.execute(
                    """
                    INSERT INTO vm_workspace_recovery_probe_slots
                        (recovery_id, global_slot, node_key, claim_token, leased_until)
                    VALUES ($1, $2, $3, 1,
                            clock_timestamp() + interval '30 seconds')
                    """,
                    recovery_id,
                    global_slot,
                    node_key,
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
            VALUES ($1, 27, 3, $2,
                    '{"code":"workspace_runtime_not_ready", "action":"hold_committed"}'::jsonb)
            """,
            job_id,
            recovery_id,
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
        recovery_id=recovery_id,
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
async def test_store_admits_idempotent_hold_and_releases_exact_queue_token(app_pg) -> None:
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
        2,
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
    assert tuple(queue) == ("queued", attempt_token + 1, 2, 4, 3, None)
    assert tuple(job) == ("paused", None)
    assert operation["phase"] == "recovered"
    assert operation["resolved_at"] is not None
