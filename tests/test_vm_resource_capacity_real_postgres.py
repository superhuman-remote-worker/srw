"""Admin VM occupancy comes from one read-only durable snapshot."""

from contextlib import asynccontextmanager
from datetime import timedelta
import json
from uuid import uuid4

import pytest

from orchestrator.services.vm_resource_capacity import vm_capacity_snapshot
from tests.test_vm_resource_whole_store_real_postgres import (
    db as _capacity_db,
    whole_schema,  # noqa: F401
    _db_fixture,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    environment,
    waiter,  # noqa: F401
)
from tests.test_vm_resource_inventory_real_postgres import publish, successor

db = _capacity_db


def cluster(result, inventory):
    return next(
        c for c in result["clusters"] if c["cluster_id"] == inventory.cluster_id
    )


class LaterClock:
    """Advance the DB clock read, keeping immutable observations unchanged."""

    def __init__(self, db, seconds=120):
        self.db = db
        self.seconds = seconds

    @asynccontextmanager
    async def acquire(self):
        seconds = self.seconds
        async with self.db.acquire() as conn:

            class Connection:
                def transaction(self, **kwargs):
                    return conn.transaction(**kwargs)

                async def fetchval(self, query, *args):
                    value = await conn.fetchval(query, *args)
                    return (
                        value + timedelta(seconds=seconds)
                        if query == "SELECT clock_timestamp()"
                        else value
                    )

                async def fetch(self, *args):
                    return await conn.fetch(*args)

                async def fetchrow(self, *args):
                    return await conn.fetchrow(*args)

            yield Connection()


@pytest.mark.asyncio
async def test_reserved_charge_and_external_demand_have_separate_vectors(db):
    store, inventory, sample, demand = await environment(db)
    retry = await waiter(db, store, inventory)
    assert (await store.admit(request_id=str(retry["request_id"])))[
        "action"
    ] == "admitted"
    before = await db.fetchval("SELECT count(*) FROM vm_resource_reservations")
    result = await vm_capacity_snapshot(db)
    cluster = next(
        c for c in result["clusters"] if c["cluster_id"] == inventory.cluster_id
    )
    assert cluster["available"] is True
    assert cluster["mode"] == "enforce"
    assert cluster["totals"]["unbound"] == demand.to_six_dict()
    assert cluster["totals"]["external"] == dict.fromkeys(demand.to_six_dict(), 0)
    assert cluster["held"]["unbound"] == demand.to_six_dict()
    assert cluster["count_backstop"]["maximum"] is None
    assert cluster["count_backstop"]["observed"] == 0
    assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == before
    # Neither requests nor raw policy/inventory credentials enter the API.
    assert "document" not in cluster and "placement" not in json.dumps(cluster)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["off", "drain"])
async def test_stale_inventory_keeps_held_demand_when_policy_is_not_enforcing(db, mode):
    store, inventory, sample, demand = await environment(db)
    retry = await waiter(db, store, inventory)
    await store.admit(request_id=str(retry["request_id"]))
    await db.execute(
        "UPDATE vm_resource_admission_policy SET mode=$2 WHERE cluster_id=$1",
        inventory.cluster_id,
        mode,
    )
    value = cluster(await vm_capacity_snapshot(LaterClock(db)), inventory)
    assert value["mode"] == mode
    assert value["available"] is False and value["reason"] == "inventory_stale"
    assert value["totals"] is None and value["nodes"] is None
    assert value["held"]["unbound"] == demand.to_six_dict()
    assert value["count_backstop"]["observed"] is None


@pytest.mark.asyncio
async def test_waiters_keep_original_age_and_fairness_without_queue_rank(db):
    store, inventory, sample, demand = await environment(db)
    retry = await waiter(db, store, inventory)
    await db.execute(
        "UPDATE vm_resource_waiters SET bypasses=2,protected_order=1 WHERE request_id=$1",
        retry["request_id"],
    )
    value = cluster(await vm_capacity_snapshot(db), inventory)
    assert value["waiting"]["count"] == 1
    assert value["waiting"]["protected"] == 1
    assert value["waiting"]["bypasses"] == 2
    assert value["waiting"]["oldest_age_seconds"] >= 0
    assert "rank" not in json.dumps(value)


@pytest.mark.asyncio
async def test_installed_policy_and_inventory_are_one_readonly_snapshot(db):
    store, inventory, sample, demand = await environment(db)
    calls = []

    class Connection:
        def __init__(self, conn):
            self.conn = conn

        def transaction(self, **kwargs):
            calls.append(kwargs)
            return self.conn.transaction(**kwargs)

        async def fetchval(self, *args):
            return await self.conn.fetchval(*args)

        async def fetch(self, query, *args):
            rows = await self.conn.fetch(query, *args)
            if "FROM vm_resource_admission_policy" in query:
                await db.execute(
                    "UPDATE vm_resource_admission_policy SET mode='drain' WHERE cluster_id=$1",
                    inventory.cluster_id,
                )
            return rows

        async def fetchrow(self, *args):
            return await self.conn.fetchrow(*args)

    class Database:
        @asynccontextmanager
        async def acquire(self):
            async with db.acquire() as conn:
                yield Connection(conn)

    result = await vm_capacity_snapshot(Database())
    assert calls == [{"isolation": "repeatable_read", "readonly": True}]
    assert cluster(result, inventory)["mode"] == "enforce"
    assert (
        await db.fetchval(
            "SELECT mode FROM vm_resource_admission_policy WHERE cluster_id=$1",
            inventory.cluster_id,
        )
        == "drain"
    )


@pytest.mark.asyncio
async def test_missing_policy_does_not_mean_zero_capacity(db):
    # The shared fixture retains empty installed policies between test owners.
    # Remove only those with no durable charges or waiting source.
    await db.execute(
        "DELETE FROM vm_resource_nodes n WHERE NOT EXISTS (SELECT 1 FROM vm_resource_reservations r WHERE r.cluster_id=n.cluster_id AND r.node_uid=n.node_uid)"
    )
    await db.execute(
        "DELETE FROM vm_resource_owner_fairness f WHERE NOT EXISTS (SELECT 1 FROM vm_resource_waiters w WHERE w.cluster_id=f.cluster_id)"
    )
    await db.execute(
        "DELETE FROM vm_resource_admission_policy p WHERE NOT EXISTS (SELECT 1 FROM vm_resource_nodes n WHERE n.cluster_id=p.cluster_id) AND NOT EXISTS (SELECT 1 FROM vm_resource_waiters w WHERE w.cluster_id=p.cluster_id)"
    )
    result = await vm_capacity_snapshot(db)
    assert result["clusters"] == []
    assert result["available"] is False
    assert result["reason"] == "resource_policy_missing"


@pytest.mark.asyncio
async def test_incomplete_inventory_does_not_fall_back_to_previous_capacity(db):
    store, inventory, sample, demand = await environment(db)
    retry = await waiter(db, store, inventory)
    await store.admit(request_id=str(retry["request_id"]))
    incomplete = successor(sample, complete=False)
    incomplete["installed_profile"] = None
    await publish(inventory, incomplete)
    value = cluster(await vm_capacity_snapshot(db), inventory)
    assert value["reason"] == "inventory_incomplete"
    assert value["inventory"]["complete"] is False
    assert value["totals"] is None
    assert value["held"]["total"] == demand.to_six_dict()


@pytest.mark.asyncio
async def test_orphaned_charge_keeps_observed_high_water_and_blocks_reused_node_name(
    db,
):
    store, inventory, sample, demand = await environment(db)
    retry = await waiter(db, store, inventory)
    admitted = await store.admit(request_id=str(retry["request_id"]))
    await db.execute(
        "UPDATE vm_resource_reservations SET observed_cpu_millicores=$2,observed_memory_bytes=$3,"
        "observed_kvm_devices=$4,observed_ephemeral_storage_bytes=$5,observed_tun_devices=$6,"
        "observed_vhost_net_devices=$7 WHERE id=$1",
        admitted["reservation_id"],
        demand.cpu_millicores + 7,
        demand.memory_bytes,
        demand.kvm_devices,
        demand.ephemeral_storage_bytes,
        demand.tun_devices,
        demand.vhost_net_devices,
    )
    newer = successor(sample)
    newer["nodes"][0]["uid"] = str(uuid4())
    await publish(inventory, newer)
    value = cluster(await vm_capacity_snapshot(db), inventory)
    expected = {**demand.to_six_dict(), "cpu_millicores": demand.cpu_millicores + 7}
    assert value["held"]["unbound"] == expected
    assert value["orphaned_held"] == {"count": 1, "resources": expected}
    assert value["nodes"][0]["general_exclusion"] == "prior_node_identity_held"
    assert value["totals"]["available"] == dict.fromkeys(expected, 0)


@pytest.mark.asyncio
async def test_external_pending_and_count_backstop_have_distinct_scopes(db):
    store, inventory, sample, demand = await environment(db)
    newer = successor(sample)
    external = {
        "cpu_millicores": 123,
        "memory_bytes": 456,
        "kvm_devices": 0,
        "ephemeral_storage_bytes": 789,
        "tun_devices": 0,
        "vhost_net_devices": 0,
    }
    node = newer["nodes"][0]
    pod = {
        "uid": str(uuid4()),
        "namespace": "workers",
        "name": "other-work",
        "node_uid": node["uid"],
        "node_name": node["name"],
        "terminal": False,
        "deleting": False,
        "requests": external,
        "vmi_uid": None,
        "reservation_id": None,
        "provision_generation": None,
    }
    newer["pods"] = [
        pod,
        {
            **pod,
            "uid": str(uuid4()),
            "name": "pending-work",
            "node_uid": None,
            "node_name": None,
        },
    ]
    newer["vms"] = [
        {
            "uid": str(uuid4()),
            "name": name,
            "owner_kind": None,
            "owner_id": None,
            "provision_generation": None,
            "deleting": deleting,
        }
        for name, deleting in (
            ("agent-vm-example", False),
            ("agent-vm-deleting", True),
            ("agent-vm-golden-image", False),
            ("unrelated-vm", False),
        )
    ]
    await publish(inventory, newer)
    value = cluster(await vm_capacity_snapshot(db), inventory)
    assert value["available"] is True
    assert value["totals"]["external"] == value["pending_external"] == external
    assert value["count_backstop"]["observed"] == 1
    assert value["count_backstop"]["maximum"] is None


@pytest.mark.asyncio
async def test_known_srw_vm_without_reservation_is_unknown_not_external_capacity(db):
    store, inventory, sample, demand = await environment(db)
    newer = successor(sample)
    newer["vms"] = [
        {
            "uid": str(uuid4()),
            "name": "agent-vm-legacy",
            "owner_kind": "thread",
            "owner_id": str(uuid4()),
            "provision_generation": str(uuid4()),
            "deleting": False,
        }
    ]
    await publish(inventory, newer)
    value = cluster(await vm_capacity_snapshot(db), inventory)
    assert value["reason"] == "legacy_occupancy_unclassified"
    assert value["totals"] is None


@pytest.mark.asyncio
async def test_teardown_age_uses_exact_release_progress_and_never_releases(
    db, monkeypatch
):
    from tests.test_vm_resource_job_runtime_real_postgres import charged_idle_wait
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    store, retry, admitted, episode, identity = await charged_idle_wait(db, monkeypatch)
    operation = await VMIdleLifecycleStore(db).admit_release(
        str(retry["job_id"]),
        episode_id=episode.episode_id,
        revision=episode.revision,
        identity=identity,
    )
    assert operation is not None
    value = cluster(await vm_capacity_snapshot(LaterClock(db, 360)), store.inventory)
    assert value["teardown"]["count"] == value["teardown"]["overdue"] == 1
    assert value["teardown"]["unknown_age"] == 0
    assert value["teardown"]["oldest_progress_age_seconds"] >= 360
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            admitted["reservation_id"],
        )
        == "teardown"
    )


@pytest.mark.asyncio
async def test_teardown_without_exact_progress_keeps_unknown_age(db):
    store, inventory, sample, demand = await environment(db)
    retry = await waiter(db, store, inventory)
    admitted = await store.admit(request_id=str(retry["request_id"]))
    await db.execute(
        "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
        admitted["reservation_id"],
    )
    value = cluster(await vm_capacity_snapshot(LaterClock(db, 360)), inventory)
    assert value["teardown"]["unknown_age"] == 1
    assert value["teardown"]["oldest_progress_age_seconds"] is None
    assert value["teardown"]["overdue"] == 0
    assert value["held"]["teardown"] == demand.to_six_dict()
