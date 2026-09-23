"""Versioned six-resource ledger guards run in real PostgreSQL."""

from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from tests.test_vm_creation_retry_real_postgres import (
    db as _base_db,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    admitted_job,
    admit,
)
from tests.test_vm_resource_inventory_real_postgres import setup, publish


@pytest_asyncio.fixture(scope="module")
async def whole_schema(_schema_applied):  # noqa: F811
    # The regenerated snapshot replays both 0273 and 0274.
    yield


@pytest_asyncio.fixture
async def db(whole_schema, _base_db):  # noqa: F811
    yield _base_db


@pytest.mark.asyncio
async def test_version_two_waiter_reservation_require_exact_six_frozen_fields(db):
    store, snapshot = setup(db)
    receipt = await publish(store, snapshot)
    job, generation, proposal = await admitted_job(db)
    request = await admit(db, job, generation, proposal)
    owner = uuid4()
    await db.execute("INSERT INTO users(id,display_name) VALUES($1,'resource-owner')", owner)
    await db.execute("UPDATE jobs SET user_id=$2 WHERE id=$1", job, owner)
    await db.execute(
        "INSERT INTO vm_resource_admission_policy(cluster_id,namespace,policy_digest,document,mode) VALUES($1,$2,$3,'{}','off')",
        store.cluster_id, store.namespace, store.policy_digest,
    )
    node = snapshot["nodes"][0]
    await db.execute(
        "INSERT INTO vm_resource_nodes(cluster_id,node_uid,node_name) VALUES($1,$2,$3)",
        store.cluster_id, UUID(node["uid"]), node["name"],
    )
    await db.execute(
        "INSERT INTO vm_resource_waiters(request_id,job_id,provision_generation,cluster_id,policy_digest,owner_key,priority,request_digest,guest_vcpus,guest_memory_bytes,cpu_millicores,memory_bytes,kvm_devices,ephemeral_storage_bytes,tun_devices,vhost_net_devices,resource_version,placement) "
        "VALUES($1,$2,$3,$4,$5,$6,5,$7,2,536870912,205,861277888,1,50000000,1,1,2,'{}')",
        request["request_id"], job, generation, store.cluster_id,
        store.policy_digest, "user:" + str(owner), request["request_digest"],
    )
    reservation_id = uuid4()
    await db.execute(
        "INSERT INTO vm_resource_reservations(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,cpu_millicores,memory_bytes,kvm_devices,ephemeral_storage_bytes,tun_devices,vhost_net_devices,resource_version,snapshot_id,snapshot_digest) "
        "VALUES($1,$2,1,$3,$4,$5,$6,205,861277888,1,50000000,1,1,2,$7,$8)",
        reservation_id, request["request_id"], store.cluster_id, store.policy_digest,
        UUID(node["uid"]), node["name"], UUID(snapshot["snapshot_id"]), receipt["digest"],
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_waiters SET tun_devices=2 WHERE request_id=$1",
            request["request_id"],
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_waiters SET resource_version=1 WHERE request_id=$1",
            request["request_id"],
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_reservations SET tun_devices=2 WHERE id=$1", reservation_id
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_reservations SET observed_cpu_millicores=100 WHERE id=$1",
            reservation_id,
        )
    await db.execute(
        "UPDATE vm_resource_reservations SET observed_cpu_millicores=220,observed_memory_bytes=900000000,observed_ephemeral_storage_bytes=60000000,observed_kvm_devices=1,observed_tun_devices=2,observed_vhost_net_devices=1 WHERE id=$1",
        reservation_id,
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_resource_reservations SET observed_tun_devices=1 WHERE id=$1", reservation_id
        )
