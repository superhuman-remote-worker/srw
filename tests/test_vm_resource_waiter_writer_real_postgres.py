"""Trusted resource-waiter projection on actual PostgreSQL."""

import asyncio
import json
from uuid import uuid4

import pytest

from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)
from shared.vm_resource_admission import ResourceAdmissionError
from tests.test_vm_creation_retry_real_postgres import (
    _schema_applied,  # noqa: F401
    admit,
    admitted_job,
    db as _db_fixture,
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
)
from tests.test_vm_resource_reservation_store_real_postgres import (
    environment,
    resource_configuration,
    wait_for_policy_lock,
)


db = _db_fixture


async def admit_with_writer(
    db, store, inventory, *, placement=None, user_id=None, priority=5
):
    controller_configuration = resource_configuration(store, inventory, placement)
    job, generation, proposal = await admitted_job(
        db, controller_configuration=controller_configuration
    )
    if user_id is not None:
        await db.execute(
            "INSERT INTO users(id,display_name) VALUES($1,'waiter-owner')", user_id
        )
    await db.execute(
        "UPDATE jobs SET user_id=$2,priority=$3 WHERE id=$1",
        job,
        user_id,
        priority,
    )
    async with db.acquire() as conn, conn.transaction():
        retry = await VMCreationRetryStore(
            db, _resource_waiter_writer=store
        ).admit_on_conn(
            conn,
            job_id=str(job),
            expected_generation=str(generation),
            request_id=str(uuid4()),
            proposal=proposal,
        )
    return retry, proposal


@pytest.mark.asyncio
async def test_fresh_retry_inserts_exact_waiter_from_locked_authority(db):
    store, inventory, _ = await environment(db)
    user_id = uuid4()
    retry, _ = await admit_with_writer(
        db, store, inventory, user_id=user_id, priority=17
    )

    row = await db.fetchrow(
        "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
        retry["request_id"],
    )
    assert row["job_id"] == retry["job_id"]
    assert row["provision_generation"] == retry["provision_generation"]
    assert row["cluster_id"] == inventory.cluster_id
    assert row["policy_digest"] == inventory.policy_digest
    assert row["owner_key"] == f"user:{user_id}"
    assert row["project_id"] is None
    assert row["priority"] == 17
    assert row["request_digest"] == retry["request_digest"]
    assert row["guest_vcpus"] == 8
    assert row["guest_memory_bytes"] == 16 * 1024**3
    assert (row["cpu_millicores"], row["memory_bytes"], row["kvm_devices"]) == (
        4000,
        16 * 1024**3,
        1,
    )
    assert json.loads(row["placement"]) == {
        "version": 1,
        "selector": {},
        "tolerations": [],
        "required_affinity": None,
        "storage_class": "local",
        "retained_pvc_uid": None,
    }
    assert row["state"] == "waiting"
    assert row["reason"] is None
    assert row["bypasses"] == 0
    assert row["protected_order"] is None
    assert row["revision"] == 1


@pytest.mark.asyncio
async def test_replay_preserves_frozen_fairness_after_job_metadata_changes(db):
    store, inventory, _ = await environment(db)
    original_user, changed_user = uuid4(), uuid4()
    retry, proposal = await admit_with_writer(
        db, store, inventory, user_id=original_user, priority=17
    )
    before = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
            retry["request_id"],
        )
    )
    await db.execute(
        "INSERT INTO users(id,display_name) VALUES($1,'changed-owner')", changed_user
    )
    await db.execute(
        "UPDATE jobs SET user_id=$2,priority=99 WHERE id=$1",
        retry["job_id"],
        changed_user,
    )

    async with db.acquire() as conn, conn.transaction():
        replay = await VMCreationRetryStore(
            db, _resource_waiter_writer=store
        ).admit_on_conn(
            conn,
            job_id=str(retry["job_id"]),
            expected_generation=str(retry["provision_generation"]),
            request_id=str(uuid4()),
            proposal=proposal,
        )

    after = dict(
        await db.fetchrow(
            "SELECT * FROM vm_resource_waiters WHERE request_id=$1",
            retry["request_id"],
        )
    )
    assert replay["request_id"] == retry["request_id"]
    assert after == before
    assert after["owner_key"] == f"user:{original_user}"
    assert after["priority"] == 17


@pytest.mark.asyncio
async def test_admission_refuses_raw_waiter_with_tampered_frozen_placement(db):
    store, inventory, _ = await environment(db)
    controller_configuration = resource_configuration(store, inventory)
    job, generation, proposal = await admitted_job(
        db, controller_configuration=controller_configuration
    )
    retry = await admit(db, job, generation, proposal)
    tampered = {
        "version": 1,
        "selector": {"zone": "b"},
        "tolerations": [],
        "required_affinity": None,
        "storage_class": "local",
        "retained_pvc_uid": None,
    }
    await db.execute(
        "INSERT INTO vm_resource_waiters(request_id,job_id,provision_generation,cluster_id,policy_digest,owner_key,priority,request_digest,guest_vcpus,guest_memory_bytes,cpu_millicores,memory_bytes,kvm_devices,placement) "
        "VALUES($1,$2,$3,$4,$5,'system',5,$6,8,$7,4000,$7,1,$8::jsonb)",
        retry["request_id"],
        job,
        generation,
        inventory.cluster_id,
        inventory.policy_digest,
        retry["request_digest"],
        16 * 1024**3,
        json.dumps(tampered),
    )

    assert await store.admit(request_id=str(retry["request_id"])) == {
        "action": "unavailable",
        "reason": "resource_waiter_changed",
    }
    assert await db.fetchval(
        "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
        retry["request_id"],
    ) == 0


@pytest.mark.asyncio
async def test_held_replay_refuses_waiter_that_disagrees_with_frozen_v2(db):
    store, inventory, snapshot = await environment(db)
    controller_configuration = resource_configuration(store, inventory)
    job, generation, proposal = await admitted_job(
        db, controller_configuration=controller_configuration
    )
    retry = await admit(db, job, generation, proposal)
    tampered = {
        "version": 1,
        "selector": {"zone": "b"},
        "tolerations": [],
        "required_affinity": None,
        "storage_class": "local",
        "retained_pvc_uid": None,
    }
    await db.execute(
        "INSERT INTO vm_resource_waiters(request_id,job_id,provision_generation,cluster_id,policy_digest,owner_key,priority,request_digest,guest_vcpus,guest_memory_bytes,cpu_millicores,memory_bytes,kvm_devices,placement) "
        "VALUES($1,$2,$3,$4,$5,'system',5,$6,8,$7,4000,$7,1,$8::jsonb)",
        retry["request_id"],
        job,
        generation,
        inventory.cluster_id,
        inventory.policy_digest,
        retry["request_digest"],
        16 * 1024**3,
        json.dumps(tampered),
    )
    node = snapshot["nodes"][0]
    observation = await db.fetchrow(
        "SELECT snapshot_id,digest FROM vm_resource_inventory_snapshots WHERE cluster_id=$1 AND policy_digest=$2",
        inventory.cluster_id,
        inventory.policy_digest,
    )
    reservation_id = uuid4()
    await db.execute(
        "INSERT INTO vm_resource_nodes(cluster_id,node_uid,node_name) VALUES($1,$2,$3)",
        inventory.cluster_id,
        node["uid"],
        node["name"],
    )
    await db.execute(
        "UPDATE vm_resource_waiters SET state='admitted',revision=revision+1 WHERE request_id=$1",
        retry["request_id"],
    )
    await db.execute(
        "INSERT INTO vm_resource_reservations(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,cpu_millicores,memory_bytes,kvm_devices,snapshot_id,snapshot_digest) "
        "VALUES($1,$2,1,$3,$4,$5,$6,4000,$7,1,$8,$9)",
        reservation_id,
        retry["request_id"],
        inventory.cluster_id,
        inventory.policy_digest,
        node["uid"],
        node["name"],
        16 * 1024**3,
        observation["snapshot_id"],
        observation["digest"],
    )

    assert await store.admit(request_id=str(retry["request_id"])) == {
        "action": "unavailable",
        "reason": "resource_waiter_changed",
    }
    held = await db.fetchrow(
        "SELECT id,state FROM vm_resource_reservations WHERE id=$1", reservation_id
    )
    assert dict(held) == {"id": reservation_id, "state": "reserved"}


@pytest.mark.asyncio
async def test_writer_refuses_equivalent_vector_from_different_host_policy(db):
    store, inventory, _ = await environment(db)
    controller_configuration = resource_configuration(store, inventory)
    mapping = controller_configuration["resource_admission"]["host_mapping"]
    mapping["policy"]["cpuMillicoresPerVcpuNumerator"] = 1000
    mapping["policy"]["cpuMillicoresPerVcpuDenominator"] = 2
    job, generation, proposal = await admitted_job(
        db, controller_configuration=controller_configuration
    )

    async with db.acquire() as conn:
        with pytest.raises(
            ResourceAdmissionError, match="^resource_configuration_changed$"
        ):
            async with conn.transaction():
                await VMCreationRetryStore(
                    db, _resource_waiter_writer=store
                ).admit_on_conn(
                    conn,
                    job_id=str(job),
                    expected_generation=str(generation),
                    request_id=str(uuid4()),
                    proposal=proposal,
                )

    assert await db.fetchval(
        "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1", job
    ) == 0
    assert await db.fetchval("SELECT count(*) FROM vm_resource_waiters") == 0


@pytest.mark.asyncio
async def test_existing_v2_retry_without_waiter_is_never_backfilled(db):
    store, inventory, _ = await environment(db)
    controller_configuration = resource_configuration(store, inventory)
    job, generation, proposal = await admitted_job(
        db, controller_configuration=controller_configuration
    )
    retry = await admit(db, job, generation, proposal)

    async with db.acquire() as conn:
        with pytest.raises(ResourceAdmissionError, match="^resource_waiter_missing$"):
            async with conn.transaction():
                await VMCreationRetryStore(
                    db, _resource_waiter_writer=store
                ).admit_on_conn(
                    conn,
                    job_id=str(job),
                    expected_generation=str(generation),
                    request_id=str(uuid4()),
                    proposal=proposal,
                )

    assert await db.fetchval(
        "SELECT count(*) FROM vm_creation_retries WHERE request_id=$1",
        retry["request_id"],
    ) == 1
    assert await db.fetchval("SELECT count(*) FROM vm_resource_waiters") == 0


@pytest.mark.asyncio
async def test_deadline_after_policy_wait_rolls_back_retry_and_waiter(db):
    store, inventory, _ = await environment(db)
    controller_configuration = resource_configuration(store, inventory)
    job, generation, proposal = await admitted_job(
        db, timeout=2, controller_configuration=controller_configuration
    )

    async def attempt():
        async with db.acquire() as conn, conn.transaction():
            return await VMCreationRetryStore(
                db, _resource_waiter_writer=store
            ).admit_on_conn(
                conn,
                job_id=str(job),
                expected_generation=str(generation),
                request_id=str(uuid4()),
                proposal=proposal,
            )

    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow(
            "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
            inventory.cluster_id,
        )
        task = asyncio.create_task(attempt())
        await wait_for_policy_lock(db)
        await blocker.execute("SELECT pg_sleep(2.1)")
    with pytest.raises(VMCreationRetryConflict, match="^job_admission_expired$"):
        await task

    assert await db.fetchval(
        "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1", job
    ) == 0
    assert await db.fetchval("SELECT count(*) FROM vm_resource_waiters") == 0


@pytest.mark.asyncio
async def test_two_retry_replicas_create_one_waiter_and_preserve_snapshot(db):
    store, inventory, _ = await environment(db)
    controller_configuration = resource_configuration(store, inventory)
    job, generation, proposal = await admitted_job(
        db, controller_configuration=controller_configuration
    )
    user = uuid4()
    await db.execute(
        "INSERT INTO users(id,display_name) VALUES($1,'concurrent-owner')", user
    )
    await db.execute(
        "UPDATE jobs SET user_id=$2,priority=23 WHERE id=$1", job, user
    )

    async def attempt():
        async with db.acquire() as conn, conn.transaction():
            return await VMCreationRetryStore(
                db, _resource_waiter_writer=store
            ).admit_on_conn(
                conn,
                job_id=str(job),
                expected_generation=str(generation),
                request_id=str(uuid4()),
                proposal=proposal,
            )

    first, second = await asyncio.gather(attempt(), attempt())
    assert first["request_id"] == second["request_id"]
    rows = await db.fetch(
        "SELECT request_id,owner_key,priority,revision,bypasses,protected_order FROM vm_resource_waiters WHERE job_id=$1",
        job,
    )
    assert [dict(row) for row in rows] == [
        {
            "request_id": first["request_id"],
            "owner_key": "user:" + str(user),
            "priority": 23,
            "revision": 1,
            "bypasses": 0,
            "protected_order": None,
        }
    ]


@pytest.mark.asyncio
async def test_ownerless_fresh_retry_uses_one_literal_system_lane(db):
    store, inventory, _ = await environment(db)
    retry, _ = await admit_with_writer(db, store, inventory)
    assert await db.fetchval(
        "SELECT owner_key FROM vm_resource_waiters WHERE request_id=$1",
        retry["request_id"],
    ) == "system"


@pytest.mark.asyncio
async def test_legacy_configuration_cannot_create_resource_authority(db):
    store, _, _ = await environment(db)
    job, generation, proposal = await admitted_job(db)
    async with db.acquire() as conn:
        with pytest.raises(
            ResourceAdmissionError, match="^resource_configuration_unavailable$"
        ):
            async with conn.transaction():
                await VMCreationRetryStore(
                    db, _resource_waiter_writer=store
                ).admit_on_conn(
                    conn,
                    job_id=str(job),
                    expected_generation=str(generation),
                    request_id=str(uuid4()),
                    proposal=proposal,
                )
    assert await db.fetchval(
        "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1", job
    ) == 0
    assert await db.fetchval("SELECT count(*) FROM vm_resource_waiters") == 0
