"""Committed policy-1 retained stop with an older process-zero receipt."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.services.vm_pre_ssh_stop_store import (
    VMPreSSHStopConflict,
    VMPreSSHStopStore,
)
from orchestrator.services.vm_job_cancel_retention import current_policy1_stop_authority
from tests.test_vm_job_cancel_retention_real_postgres import (
    acquire,
    cancelled,
    preflight,
    terminal_proof,
)
from tests.test_vm_job_initial_ready_retention_real_postgres import (  # noqa: F401
    _base_db,
    _db_fixture,
    _pre_ssh_db,
    _retention_db,
    _resume_db,
    _schema_applied,
    db as _initial_ready_db,
    initial_ready_schema,
    pg_dsn,
    postgres_db_fixture,
    pre_ssh_schema,
    retention_schema,
    resume_schema,
    whole_schema,
)


@pytest_asyncio.fixture(scope="module")
async def held_stop_schema(pg_dsn, initial_ready_schema):  # noqa: F811
    root = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        installed = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='vm_pre_ssh_stop_intents' "
            "AND column_name='held_stop_xact_id')"
        )
        if not installed:
            for name in (
                "0431_vm_never_app_ready_retained_stop.sql",
                "0432_validate_vm_never_app_ready_retained_stop.sql",
            ):
                await conn.execute((root / name).read_text())
        assert (
            await conn.fetchval(
                "SELECT convalidated FROM pg_constraint "
                "WHERE conname='vm_pre_ssh_stop_prior_zero_fk'"
            )
            is True
        )
        yield
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(held_stop_schema, _initial_ready_db):  # noqa: F811
    yield _initial_ready_db


@pytest.fixture(autouse=True)
def enabled(monkeypatch):
    monkeypatch.setenv("VM_JOB_CANCEL_RETENTION_ENABLED", "true")


def frozen_candidate(state, parent):
    return {
        **state["frozen"],
        "kind": "vm_job_never_app_ready_retained_stop_candidate_v1",
        "cleanup_admission_id": parent["admission_id"],
        "cleanup_request_id": parent["request_id"],
        "cleanup_intent_digest": parent["intent_digest"],
        "kube_vm_ready_at_inspection": True,
    }


async def held_state(db, *, zero=True):
    state = await cancelled(db, old=False)
    permit = await acquire(state)
    assert permit.allowed, permit.reason
    parent = permit.parent_cleanup
    state["frozen"] = frozen_candidate(state, parent)
    state["parent"] = parent
    state["preflight"] = preflight(state)
    state["preflight"]["pvc_name"] = f"agent-vm-{state['job_id']}-rootdisk"
    state["zero"] = None
    if zero:
        assert await db.record_managed_repository_workspace_process_zero(
            state["job_id"],
            owner_kind="job",
            scope="vm",
            provisioner="vm",
            runtime_incarnation=state["generation"],
        )
        state["zero"] = await db.fetchrow(
            "SELECT * FROM managed_repository_process_zero_receipts WHERE "
            "owner_kind='job' AND owner_id=$1 AND scope='vm' AND "
            "runtime_incarnation=$2",
            UUID(state["job_id"]),
            state["generation"],
        )
    return state


@pytest.mark.asyncio
async def test_exact_prior_zero_admits_immutable_intent_then_committed_proof(db):
    state = await held_state(db)
    store, parent, frozen, old_zero = (
        state["store"],
        state["parent"],
        state["frozen"],
        state["zero"],
    )
    intent = await store.admit_intent(
        state["job_id"],
        state["generation"],
        parent,
        frozen,
        retention_preflight=state["preflight"],
        prior_zero_receipt_id=str(old_zero["id"]),
    )
    assert intent["frozen"] == frozen
    assert (
        await db.fetchval(
            "SELECT prior_zero_receipt_id FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == old_zero["id"]
    )
    assert (
        await store.committed_proof(state["job_id"], state["generation"], parent)
        is None
    )
    observed = terminal_proof(frozen, intent["frozen_digest"])
    observed["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
    proof = await store.commit_positive_proof(
        state["job_id"], state["generation"], parent, observed
    )
    assert proof["process_zero_receipt_id"] == str(old_zero["id"])
    assert await store.committed_proof(state["job_id"], state["generation"], parent)
    assert dict(
        await db.fetchrow(
            "SELECT * FROM managed_repository_process_zero_receipts WHERE id=$1",
            old_zero["id"],
        )
    ) == dict(old_zero)
    assert (
        await store.admit_intent(
            state["job_id"],
            state["generation"],
            parent,
            frozen,
            retention_preflight=state["preflight"],
            prior_zero_receipt_id=str(old_zero["id"]),
        )
        == intent
    )
    with pytest.raises((asyncpg.CheckViolationError, VMPreSSHStopConflict)):
        await store.admit_intent(
            state["job_id"],
            state["generation"],
            parent,
            frozen,
            retention_preflight=state["preflight"],
            prior_zero_receipt_id=str(uuid4()),
        )


@pytest.mark.asyncio
async def test_authority_is_native_committed_and_exact(db):
    state = await held_state(db)
    args = dict(
        job_id=state["job_id"],
        generation=state["generation"],
        vm_uid=state["frozen"]["vm_uid"],
        pvc_uid=state["frozen"]["pvc_uid"],
    )
    authority = await current_policy1_stop_authority(db, state["parent"], **args)
    assert authority["kind"] == "vm_job_cancel_retention_held_stop_authority_v1"
    assert authority["cleanup_admission_id"] == state["parent"]["admission_id"]
    assert (
        await current_policy1_stop_authority(db, state["parent"], **args) == authority
    )
    assert (
        await current_policy1_stop_authority(
            db, state["parent"], **{**args, "vm_uid": str(uuid4())}
        )
        is None
    )
    changed = {**state["parent"], "request_id": str(uuid4())}
    assert await current_policy1_stop_authority(db, changed, **args) is None
    await db.execute(
        "UPDATE run_queue SET state='queued' WHERE unit_id=$1", UUID(state["job_id"])
    )
    assert await current_policy1_stop_authority(db, state["parent"], **args) is None
    await db.execute(
        "UPDATE run_queue SET state='done' WHERE unit_id=$1", UUID(state["job_id"])
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt", ["missing", "wrong"])
async def test_existing_zero_rejects_missing_or_wrong_link(db, receipt):
    state = await held_state(db)
    with pytest.raises(VMPreSSHStopConflict):
        await state["store"].admit_intent(
            state["job_id"],
            state["generation"],
            state["parent"],
            state["frozen"],
            retention_preflight=state["preflight"],
            prior_zero_receipt_id=None if receipt == "missing" else str(uuid4()),
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_old_kind_cannot_borrow_existing_zero(db):
    state = await held_state(db)
    old_frozen = {
        key: value
        for key, value in state["frozen"].items()
        if key
        not in {
            "cleanup_admission_id",
            "cleanup_request_id",
            "cleanup_intent_digest",
            "kube_vm_ready_at_inspection",
        }
    }
    old_frozen["kind"] = "vm_pre_ssh_stop_candidate_v1"
    with pytest.raises(VMPreSSHStopConflict):
        await state["store"].admit_intent(
            state["job_id"],
            state["generation"],
            state["parent"],
            old_frozen,
            prior_zero_receipt_id=str(state["zero"]["id"]),
        )


@pytest.mark.asyncio
async def test_no_proof_cannot_complete_retaining_parent(db):
    state = await held_state(db)
    await state["store"].admit_intent(
        state["job_id"],
        state["generation"],
        state["parent"],
        state["frozen"],
        retention_preflight=state["preflight"],
        prior_zero_receipt_id=str(state["zero"]["id"]),
    )
    assert (
        await state["store"].committed_proof(
            state["job_id"], state["generation"], state["parent"]
        )
        is None
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
            "outcome='completed' WHERE id=$1",
            UUID(state["parent"]["admission_id"]),
        )
    assert (
        await db.fetchval(
            "SELECT completed_at FROM vm_workspace_cleanup_admissions WHERE id=$1",
            UUID(state["parent"]["admission_id"]),
        )
        is None
    )


@pytest.mark.asyncio
async def test_positive_proof_requires_prior_intent(db):
    state = await held_state(db)
    proof = terminal_proof(state["frozen"], "sha256:" + "a" * 64)
    proof["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
    with pytest.raises(VMPreSSHStopConflict, match="stop_intent_absent"):
        await state["store"].commit_positive_proof(
            state["job_id"], state["generation"], state["parent"], proof
        )


@pytest.mark.asyncio
async def test_fresh_held_stop_intent_does_not_require_prior_zero(db):
    state = await held_state(db, zero=False)
    intent = await state["store"].admit_intent(
        state["job_id"],
        state["generation"],
        state["parent"],
        state["frozen"],
        retention_preflight=state["preflight"],
    )
    assert intent["frozen"] == state["frozen"]
    assert (
        await db.fetchval(
            "SELECT prior_zero_receipt_id FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        is None
    )
    observed = terminal_proof(state["frozen"], intent["frozen_digest"])
    observed["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
    proof = await state["store"].commit_positive_proof(
        state["job_id"], state["generation"], state["parent"], observed
    )
    assert proof["process_zero_receipt_id"]
    assert await state["store"].committed_proof(
        state["job_id"], state["generation"], state["parent"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["owner", "queue", "charge"])
async def test_concurrent_writer_invalidates_authority_after_lock(db, mutation):
    state = await held_state(db)
    args = dict(
        job_id=state["job_id"],
        generation=state["generation"],
        vm_uid=state["frozen"]["vm_uid"],
        pvc_uid=state["frozen"]["pvc_uid"],
    )
    reader = None
    try:
        async with db.acquire() as writer:
            async with writer.transaction():
                if mutation == "owner":
                    await writer.execute(
                        "UPDATE jobs SET execution_lane='pinned' WHERE id=$1",
                        UUID(state["job_id"]),
                    )
                elif mutation == "queue":
                    await writer.execute(
                        "UPDATE run_queue SET state='queued' WHERE unit_id=$1",
                        UUID(state["job_id"]),
                    )
                else:
                    await writer.execute(
                        "UPDATE vm_resource_reservations SET "
                        "observed_cpu_millicores=observed_cpu_millicores+1 WHERE id=$1",
                        UUID(state["reservation_id"]),
                    )
                reader = asyncio.create_task(
                    current_policy1_stop_authority(db, state["parent"], **args)
                )
                await asyncio.sleep(0.05)
                assert not reader.done()
        result = await asyncio.wait_for(reader, 5)
        assert (result is not None) if mutation == "charge" else (result is None)
    finally:
        if reader is not None and not reader.done():
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)


class SameConnectionDB:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


@pytest.mark.asyncio
async def test_savepoint_release_cannot_publish_uncommitted_held_intent(db):
    state = await held_state(db)
    with pytest.raises(RuntimeError, match="roll back outer"):
        async with db.acquire() as conn:
            same = SameConnectionDB(conn)
            store = VMPreSSHStopStore(same)
            async with conn.transaction():
                async with conn.transaction():
                    await store.admit_intent(
                        state["job_id"],
                        state["generation"],
                        state["parent"],
                        state["frozen"],
                        retention_preflight=state["preflight"],
                        prior_zero_receipt_id=str(state["zero"]["id"]),
                    )
                with pytest.raises(VMPreSSHStopConflict, match="uncommitted"):
                    await store.current_intent(
                        state["job_id"], state["generation"], state["parent"]
                    )
                observed = terminal_proof(
                    state["frozen"],
                    await conn.fetchval(
                        "SELECT frozen_digest FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
                        UUID(state["job_id"]),
                    ),
                )
                observed["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
                with pytest.raises(VMPreSSHStopConflict, match="uncommitted"):
                    await store.commit_positive_proof(
                        state["job_id"], state["generation"], state["parent"], observed
                    )
                assert (
                    await current_policy1_stop_authority(
                        same,
                        state["parent"],
                        job_id=state["job_id"],
                        generation=state["generation"],
                        vm_uid=state["frozen"]["vm_uid"],
                        pvc_uid=state["frozen"]["pvc_uid"],
                    )
                    is None
                )
                raise RuntimeError("roll back outer")
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_pre_ssh_stop_intents WHERE job_id=$1",
            UUID(state["job_id"]),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_native_intent_stamp_overwrites_forged_input(db):
    state = await held_state(db, zero=False)
    row = await db.fetchrow(
        "INSERT INTO vm_pre_ssh_stop_intents "
        "(cleanup_admission_id,job_id,provision_generation,creation_request_id,"
        "reservation_id,reservation_revision,vm_uid,vmi_uid,launcher_uid,pvc_uid,node_uid,"
        "cleanup_intent_digest,frozen,frozen_digest,retention_preflight,held_stop_xact_id) "
        "SELECT cleanup_admission_id,job_id,provision_generation,creation_request_id,"
        "reservation_id,reservation_revision,vm_uid,vmi_uid,launcher_uid,pvc_uid,node_uid,"
        "intent_digest,$2::jsonb,'sha256:'||encode(sha256(convert_to($2::jsonb::text,'UTF8')),'hex'),"
        "$3::jsonb,'0'::xid8 FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1 "
        "RETURNING held_stop_xact_id,frozen_digest",
        UUID(state["parent"]["admission_id"]),
        json.dumps(state["frozen"]),
        json.dumps(state["preflight"]),
    )
    assert row["held_stop_xact_id"] is not None
    assert str(row["held_stop_xact_id"]) != "0"
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_pre_ssh_stop_intents SET held_stop_xact_id='0'::xid8 "
            "WHERE cleanup_admission_id=$1",
            UUID(state["parent"]["admission_id"]),
        )
    observed = terminal_proof(state["frozen"], row["frozen_digest"])
    observed["kind"] = "vm_job_never_app_ready_retained_positive_stop_v1"
    proof_stamp = await db.fetchval(
        "INSERT INTO vm_pre_ssh_stop_proofs "
        "(cleanup_admission_id,job_id,provision_generation,frozen_digest,"
        "terminal_evidence,evidence_digest,held_stop_xact_id) "
        "VALUES($1,$2,$3,$4,$5::jsonb,"
        "'sha256:'||encode(sha256(convert_to($5::jsonb::text,'UTF8')),'hex'),'0'::xid8) "
        "RETURNING held_stop_xact_id",
        UUID(state["parent"]["admission_id"]),
        UUID(state["job_id"]),
        UUID(state["generation"]),
        row["frozen_digest"],
        json.dumps(observed),
    )
    assert proof_stamp is not None
    assert str(proof_stamp) != "0"
    assert proof_stamp != row["held_stop_xact_id"]
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_pre_ssh_stop_proofs SET held_stop_xact_id='0'::xid8 "
            "WHERE cleanup_admission_id=$1",
            UUID(state["parent"]["admission_id"]),
        )


@pytest.mark.asyncio
async def test_ready_application_retry_revokes_held_authority(db):
    state = await held_state(db)
    await db.execute(
        "UPDATE vm_creation_retries SET ready_at=clock_timestamp() WHERE job_id=$1",
        UUID(state["job_id"]),
    )
    assert (
        await current_policy1_stop_authority(
            db,
            state["parent"],
            job_id=state["job_id"],
            generation=state["generation"],
            vm_uid=state["frozen"]["vm_uid"],
            pvc_uid=state["frozen"]["pvc_uid"],
        )
        is None
    )
    with pytest.raises(VMPreSSHStopConflict):
        await state["store"].admit_intent(
            state["job_id"],
            state["generation"],
            state["parent"],
            state["frozen"],
            retention_preflight=state["preflight"],
            prior_zero_receipt_id=str(state["zero"]["id"]),
        )


@pytest.mark.asyncio
async def test_stale_generation_revision_and_successor_refuse_authority(db):
    state = await held_state(db)
    args = dict(
        job_id=state["job_id"],
        generation=state["generation"],
        vm_uid=state["frozen"]["vm_uid"],
        pvc_uid=state["frozen"]["pvc_uid"],
    )
    assert (
        await current_policy1_stop_authority(
            db, state["parent"], **{**args, "generation": str(uuid4())}
        )
        is None
    )

    async with db.acquire() as conn:
        old_revision = await conn.fetchval(
            "SELECT revision FROM vm_resource_reservations WHERE id=$1",
            UUID(state["reservation_id"]),
        )
        # Model stale persisted data without making production mutators accept
        # an impossible revision or a successor with no recovery fixture.
        await conn.execute("SET session_replication_role=replica")
        try:
            await conn.execute(
                "UPDATE vm_resource_reservations SET revision=revision+1 WHERE id=$1",
                UUID(state["reservation_id"]),
            )
        finally:
            await conn.execute("SET session_replication_role=origin")
    assert await current_policy1_stop_authority(db, state["parent"], **args) is None
    async with db.acquire() as conn:
        await conn.execute("SET session_replication_role=replica")
        try:
            await conn.execute(
                "UPDATE vm_resource_reservations SET revision=$2 WHERE id=$1",
                UUID(state["reservation_id"]),
                old_revision,
            )
        finally:
            await conn.execute("SET session_replication_role=origin")

    successor_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute("SET session_replication_role=replica")
        try:
            await conn.execute(
                "INSERT INTO vm_resource_recovery_successors "
                "(recovery_id,reservation_id,ordinal,owner_id,provision_generation,"
                "vm_uid,root_pvc_uid,prior_vmi_uid,prior_launcher_uid,"
                "successor_vmi_uid,successor_launcher_uid,stop_receipt_digest,"
                "final_attestation_digest) "
                "VALUES($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11)",
                successor_id,
                UUID(state["reservation_id"]),
                UUID(state["job_id"]),
                UUID(state["generation"]),
                UUID(state["frozen"]["vm_uid"]),
                UUID(state["frozen"]["pvc_uid"]),
                UUID(state["frozen"]["vmi_uid"]),
                UUID(state["frozen"]["launcher_uid"]),
                uuid4(),
                uuid4(),
                "sha256:" + "f" * 64,
            )
        finally:
            await conn.execute("SET session_replication_role=origin")
    try:
        assert await current_policy1_stop_authority(db, state["parent"], **args) is None
    finally:
        async with db.acquire() as conn:
            await conn.execute("SET session_replication_role=replica")
            try:
                await conn.execute(
                    "DELETE FROM vm_resource_recovery_successors WHERE recovery_id=$1",
                    successor_id,
                )
            finally:
                await conn.execute("SET session_replication_role=origin")
