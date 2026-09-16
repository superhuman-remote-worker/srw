"""Durable VM workspace recovery contracts against PostgreSQL 15."""

from __future__ import annotations

import asyncio
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


async def insert_leased_job(app_pg, *, include_attempt: bool = True) -> tuple[UUID, int]:
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
