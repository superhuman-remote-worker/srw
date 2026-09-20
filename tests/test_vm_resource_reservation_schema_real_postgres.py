import json
from uuid import UUID, uuid4

import asyncpg
import pytest

from tests.test_vm_creation_retry_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    admitted_job,
    admit,
)
from tests.test_vm_resource_inventory_real_postgres import setup, publish, successor

db = _db_fixture


async def ledger(db):
    store, observation = setup(db, history_limit=1)
    receipt = await publish(store, observation)
    job, generation, proposal = await admitted_job(db)
    request = await admit(db, job, generation, proposal)
    node = observation["nodes"][0]
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO vm_resource_admission_policy(cluster_id,namespace,policy_digest,document,mode) VALUES($1,$2,$3,'{}','off')",
            store.cluster_id,
            store.namespace,
            store.policy_digest,
        )
        await conn.execute(
            "INSERT INTO vm_resource_nodes(cluster_id,node_uid,node_name) VALUES($1,$2,$3)",
            store.cluster_id,
            UUID(node["uid"]),
            node["name"],
        )
        await conn.execute(
            "INSERT INTO vm_resource_waiters(request_id,job_id,provision_generation,cluster_id,policy_digest,owner_key,priority,"
            "request_digest,guest_vcpus,guest_memory_bytes,cpu_millicores,memory_bytes,kvm_devices,placement) "
            "VALUES($1,$2,$3,$4,$5,'system',5,$6,8,17179869184,1000,1073741824,1,'{}')",
            request["request_id"],
            job,
            generation,
            store.cluster_id,
            store.policy_digest,
            request["request_digest"],
        )
    return store, observation, receipt, request


async def reserve(db, setup, *, revision=1, digest=None, cpu=1000):
    store, observation, receipt, request = setup
    node = observation["nodes"][0]
    identity = uuid4()
    await db.execute(
        "INSERT INTO vm_resource_reservations(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,"
        "cpu_millicores,memory_bytes,kvm_devices,snapshot_id,snapshot_digest) "
        "VALUES($1,$2,$3,$4,$5,$6,$7,$10,1073741824,1,$8,$9)",
        identity,
        request["request_id"],
        revision,
        store.cluster_id,
        store.policy_digest,
        UUID(node["uid"]),
        node["name"],
        UUID(observation["snapshot_id"]),
        digest or receipt["digest"],
        cpu,
    )
    return identity


@pytest.mark.asyncio
async def test_inventory_pruning_retains_referenced_evidence_beyond_unreferenced_window(
    db,
):
    fixture = await ledger(db)
    store, first, _, _ = fixture
    await reserve(db, fixture)
    second = successor(first)
    await publish(store, second)
    third = successor(second)
    await publish(store, third)
    rows = await db.fetch(
        "SELECT snapshot_id FROM vm_resource_inventory_snapshots WHERE cluster_id=$1",
        store.cluster_id,
    )
    assert {str(row["snapshot_id"]) for row in rows} == {
        first["snapshot_id"],
        third["snapshot_id"],
    }
    assert (await store.current())["snapshot"]["snapshot_id"] == third["snapshot_id"]


@pytest.mark.asyncio
async def test_same_request_has_one_held_reservation_and_exact_snapshot_digest(db):
    fixture = await ledger(db)
    first = await reserve(db, fixture)
    with pytest.raises(asyncpg.UniqueViolationError):
        await reserve(db, fixture, revision=2)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute("DELETE FROM vm_resource_reservations WHERE id=$1", first)
    await db.execute(
        "UPDATE vm_resource_reservations SET state='released',released_at=clock_timestamp(),release_evidence=$2::jsonb WHERE id=$1",
        first,
        json.dumps({"kind": "never_vm_issued"}),
    )
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await reserve(db, fixture, revision=2, digest="sha256:" + "f" * 64)
    second = await reserve(db, fixture, revision=2)
    assert second != first


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "cpu_millicores=1001",
        "node_name='different'",
        "snapshot_digest='sha256:'||repeat('a',64)",
    ],
)
async def test_reservation_vector_and_identity_are_immutable(db, change):
    fixture = await ledger(db)
    identity = await reserve(db, fixture)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_reservations SET " + change + " WHERE id=$1", identity
        )


@pytest.mark.asyncio
async def test_observed_runtime_identity_is_write_once_and_release_is_terminal(db):
    fixture = await ledger(db)
    identity = await reserve(db, fixture)
    await db.execute(
        "UPDATE vm_resource_reservations SET state='active',vm_uid=$2,vmi_uid=$3,launcher_uid=$4 WHERE id=$1",
        identity,
        uuid4(),
        uuid4(),
        uuid4(),
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_reservations SET vm_uid=$2 WHERE id=$1",
            identity,
            uuid4(),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_reservations SET state='released' WHERE id=$1", identity
        )
    await db.execute(
        "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1", identity
    )
    await db.execute(
        "UPDATE vm_resource_reservations SET state='released',released_at=clock_timestamp(),release_evidence=$2::jsonb WHERE id=$1",
        identity,
        json.dumps({"kind": "exact_compute_absent"}),
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_reservations SET state='reserved',released_at=NULL,release_evidence=NULL WHERE id=$1",
            identity,
        )


@pytest.mark.asyncio
async def test_waiter_cannot_change_owner_generation_or_original_age(db):
    fixture = await ledger(db)
    request = fixture[3]
    for assignment in (
        "owner_key='other'",
        "enqueued_at=clock_timestamp()",
        "priority=99",
        "provision_generation=gen_random_uuid()",
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE vm_resource_waiters SET " + assignment + " WHERE request_id=$1",
                request["request_id"],
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["job_id", "provision_generation", "request_digest"])
async def test_waiter_insert_must_match_immutable_source_without_new_parent_index(
    db, field
):
    fixture = await ledger(db)
    wrong = "sha256:" + "b" * 64 if field == "request_digest" else str(uuid4())
    with pytest.raises(
        asyncpg.ForeignKeyViolationError, match="source identity mismatch"
    ):
        await db.execute(
            "INSERT INTO vm_resource_waiters SELECT (jsonb_populate_record(NULL::vm_resource_waiters,"
            "to_jsonb(w)||jsonb_build_object($2::text,$3::text))).* FROM vm_resource_waiters w WHERE request_id=$1",
            fixture[3]["request_id"],
            field,
            wrong,
        )


@pytest.mark.asyncio
async def test_reservation_cannot_start_with_repriced_demand_or_released_state(db):
    fixture = await ledger(db)
    with pytest.raises(asyncpg.CheckViolationError, match="initial demand"):
        await reserve(db, fixture, cpu=1)
    identity = await reserve(db, fixture)
    with pytest.raises(asyncpg.CheckViolationError, match="initial demand"):
        await db.execute(
            "INSERT INTO vm_resource_reservations SELECT (jsonb_populate_record(NULL::vm_resource_reservations,"
            "to_jsonb(r)||jsonb_build_object('id',gen_random_uuid(),'revision',2,'state','released',"
            "'released_at',clock_timestamp(),'release_evidence',jsonb_build_object('kind','exact_compute_absent')))).* "
            "FROM vm_resource_reservations r WHERE id=$1",
            identity,
        )
