"""Admission races on actual PG; waiter fixtures stand in for later D4 enqueue."""

import asyncio
from copy import deepcopy
import hashlib
import json
from uuid import UUID, uuid4

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
from tests.test_vm_resource_inventory_settings import configuration

db = _db_fixture
GIB = 1024**3


async def environment(db, *, max_bypasses=1, stale_after_seconds=60):
    from orchestrator.services.vm_resource_reservation_store import (
        VMResourceReservationStore,
    )

    inventory, snapshot = setup(db, stale_after_seconds=stale_after_seconds)
    config = configuration()
    policy = config["policy"]
    policy.update(
        stableClusterId=inventory.cluster_id,
        shadowEnabled=True,
        enforcementEnabled=True,
    )
    policy["inventory"].update(maxItems=100, staleAfterSeconds=stale_after_seconds)
    policy["hostCost"] = {
        "cpuMillicoresPerVcpuNumerator": 500,
        "cpuMillicoresPerVcpuDenominator": 1,
        "launcherCpuOverheadMillicores": 0,
        "fixedMemoryOverheadBytes": 0,
        "perVcpuMemoryOverheadBytes": 0,
        "memoryOverheadBasisPoints": 0,
    }
    policy["nodeHeadroom"] = {"cpuMillicores": 0, "memoryBytes": 0, "kvmDevices": 0}
    policy["fairness"] = {"maxBypasses": max_bypasses, "priorityAgingSeconds": 60}
    digest = (
        "sha256:"
        + hashlib.sha256(
            json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    inventory.policy_digest = snapshot["policy_digest"] = digest
    snapshot["nodes"][0]["allocatable"]["memory_bytes"] = 20 * GIB
    snapshot["nodes"][0]["labels"]["kubernetes.io/hostname"] = "node-a"
    snapshot["storage_classes"] = [
        {
            "uid": str(uuid4()),
            "name": "local",
            "binding_mode": "WaitForFirstConsumer",
            "allowed_topology": None,
        }
    ]
    await publish(inventory, snapshot)
    await db.execute(
        "INSERT INTO vm_resource_admission_policy(cluster_id,namespace,policy_digest,document,mode) VALUES($1,$2,$3,$4::jsonb,'enforce')",
        inventory.cluster_id,
        inventory.namespace,
        digest,
        json.dumps(config),
    )
    return (
        VMResourceReservationStore(
            db, inventory=inventory, policy_document=config, policy_revision=1
        ),
        inventory,
        snapshot,
    )


async def waiter(
    db, inventory, *, owner=None, priority=5, placement=None, conn=None, timeout=3600
):
    # These rows are deliberately inserted only in tests. D4 must derive the
    # projection from authenticated immutable controller configuration.
    job, generation, proposal = await admitted_job(db, timeout=timeout)
    request = await admit(db, job, generation, proposal)
    placement = placement or {
        "version": 1,
        "selector": {},
        "tolerations": [],
        "required_affinity": None,
        "storage_class": "local",
        "retained_pvc_uid": None,
    }
    await (conn or db).execute(
        "INSERT INTO vm_resource_waiters(request_id,job_id,provision_generation,cluster_id,policy_digest,owner_key,priority,request_digest,"
        "guest_vcpus,guest_memory_bytes,cpu_millicores,memory_bytes,kvm_devices,placement) "
        "VALUES($1,$2,$3,$4,$5,$6,$7,$8,8,17179869184,4000,17179869184,1,$9::jsonb)",
        request["request_id"],
        job,
        generation,
        inventory.cluster_id,
        inventory.policy_digest,
        owner or "job:" + str(job),
        priority,
        request["request_digest"],
        json.dumps(placement),
    )
    return request


async def decide(store, request):
    return await store.admit(request_id=str(request["request_id"]))


@pytest.mark.asyncio
async def test_two_replicas_cannot_spend_final_vector_and_replay_keeps_sequence(db):
    store, inventory, snapshot = await environment(db)
    first, second = await waiter(db, inventory), await waiter(db, inventory)
    results = await asyncio.gather(decide(store, first), decide(store, second))
    assert sum(result["action"] == "admitted" for result in results) == 1
    held = await db.fetch(
        "SELECT * FROM vm_resource_reservations WHERE cluster_id=$1",
        inventory.cluster_id,
    )
    assert len(held) == 1 and held[0]["cpu_millicores"] == 4000
    assert str(held[0]["node_uid"]) == snapshot["nodes"][0]["uid"]
    replay = await decide(store, first)
    assert replay["reservation_id"] == str(held[0]["id"])
    assert (
        await db.fetchval(
            "SELECT admission_sequence FROM vm_resource_admission_policy WHERE cluster_id=$1",
            inventory.cluster_id,
        )
        == 1
    )


@pytest.mark.asyncio
async def test_duplicate_parallel_request_reserves_once_even_after_inventory_invalidates(
    db,
):
    store, inventory, snapshot = await environment(db)
    request = await waiter(db, inventory)
    results = await asyncio.gather(decide(store, request), decide(store, request))
    assert results[0] == results[1]
    await publish(inventory, successor(snapshot, complete=False))
    assert await decide(store, request) == results[0]
    await db.execute(
        "UPDATE srw_execution_specs SET revision='changed' WHERE work_id=$1",
        request["job_id"],
    )
    refused = await decide(store, request)
    assert (
        refused["action"] == "unavailable"
        and refused["reason"] == "execution_manifest_changed"
    )


@pytest.mark.asyncio
async def test_newer_incomplete_after_policy_wait_cannot_reserve(db):
    store, inventory, snapshot = await environment(db)
    request = await waiter(db, inventory)
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow(
            "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
            inventory.cluster_id,
        )
        task = asyncio.create_task(decide(store, request))
        await wait_for_policy_lock(db)
        await publish(inventory, successor(snapshot, complete=False))
    result = await asyncio.wait_for(task, 5)
    assert result == {"action": "unavailable", "reason": "inventory_incomplete"}
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE cluster_id=$1",
            inventory.cluster_id,
        )
        == 0
    )


async def wait_for_policy_lock(db):
    for _ in range(100):
        if await db.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT%vm_resource_admission_policy%FOR UPDATE%')"
        ):
            return
        await asyncio.sleep(0.01)
    pytest.fail("admission did not reach the policy row lock")


@pytest.mark.asyncio
async def test_winner_changes_while_waiting_without_locking_foreign_job(db):
    store, inventory, _ = await environment(db)
    first = await waiter(db, inventory, priority=5)
    async with db.acquire() as owner, owner.transaction():
        async with db.acquire() as blocker, blocker.transaction():
            await blocker.fetchrow(
                "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
                inventory.cluster_id,
            )
            task = asyncio.create_task(decide(store, first))
            await wait_for_policy_lock(db)
            later = await waiter(db, inventory, priority=99, conn=blocker)
            await owner.fetchrow(
                "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", later["job_id"]
            )
        result = await asyncio.wait_for(task, 3)
    assert result == {"action": "nominate", "request_id": str(later["request_id"])}
    assert (
        await db.fetchval(
            "SELECT admission_sequence FROM vm_resource_admission_policy WHERE cluster_id=$1",
            inventory.cluster_id,
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["mode='drain'", "revision=2", "document='{}'::jsonb"]
)
async def test_policy_change_refuses_without_modifying_waiter_or_fairness(db, change):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    await db.execute(
        "UPDATE vm_resource_admission_policy SET " + change + " WHERE cluster_id=$1",
        inventory.cluster_id,
    )
    assert (await decide(store, request))["action"] == "unavailable"
    row = await db.fetchrow(
        "SELECT state,bypasses FROM vm_resource_waiters WHERE request_id=$1",
        request["request_id"],
    )
    assert tuple(row.values()) == ("waiting", 0)


@pytest.mark.asyncio
async def test_nonfit_is_reconsidered_on_new_snapshot_without_resetting_age(db):
    store, inventory, snapshot = await environment(db)
    snapshot = successor(snapshot)
    snapshot["nodes"][0]["allocatable"]["cpu_millicores"] = 3999
    await publish(inventory, snapshot)
    request = await waiter(db, inventory)
    assert (await decide(store, request))["action"] == "nonfit"
    original = await db.fetchrow(
        "SELECT enqueued_at,revision FROM vm_resource_waiters WHERE request_id=$1",
        request["request_id"],
    )
    newer = successor(snapshot)
    newer["nodes"][0]["allocatable"]["cpu_millicores"] = 4000
    await publish(inventory, newer)
    assert (await decide(store, request))["action"] == "admitted"
    assert (
        await db.fetchval(
            "SELECT enqueued_at FROM vm_resource_waiters WHERE request_id=$1",
            request["request_id"],
        )
        == original["enqueued_at"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "condition",
    ["cordon", "not_ready", "missing_kvm", "unknown_label", "missing_storage"],
)
async def test_transient_placement_evidence_is_never_permanent_nonfit(db, condition):
    store, inventory, snapshot = await environment(db)
    snapshot = successor(snapshot)
    placement = {
        "version": 1,
        "selector": {},
        "tolerations": [],
        "required_affinity": None,
        "storage_class": "local",
        "retained_pvc_uid": None,
    }
    if condition == "cordon":
        snapshot["nodes"][0]["unschedulable"] = True
    elif condition == "not_ready":
        snapshot["nodes"][0]["ready"] = False
    elif condition == "missing_kvm":
        snapshot["nodes"][0]["allocatable"]["kvm_devices"] = 0
    elif condition == "unknown_label":
        placement["selector"] = {"uncollected": "x"}
    else:
        snapshot["storage_classes"] = []
    await publish(inventory, snapshot)
    request = await waiter(db, inventory, placement=placement)
    assert (await decide(store, request))["action"] in {"wait", "protected"}
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
            request["request_id"],
        )
        == "waiting"
    )


@pytest.mark.asyncio
async def test_bypass_protection_is_committed_once_and_survives_recreated_store(db):
    store, inventory, snapshot = await environment(db)
    blocked = await waiter(
        db,
        inventory,
        priority=10,
        placement={
            "version": 1,
            "selector": {"zone": "b"},
            "tolerations": [],
            "required_affinity": None,
            "storage_class": "local",
            "retained_pvc_uid": None,
        },
    )
    fitting = await waiter(db, inventory)
    result = await decide(store, fitting)
    assert result["action"] == "admitted"
    state = await db.fetchrow(
        "SELECT bypasses,protected_order FROM vm_resource_waiters WHERE request_id=$1",
        blocked["request_id"],
    )
    assert state["bypasses"] == 1 and state["protected_order"] is not None
    assert await decide(store, fitting) == result
    newcomer = await waiter(db, inventory, priority=99)
    result = await decide(store, newcomer)
    assert result == {"action": "nominate", "request_id": str(blocked["request_id"])}
    assert (
        await db.fetchval(
            "SELECT bypasses FROM vm_resource_waiters WHERE request_id=$1",
            blocked["request_id"],
        )
        == 1
    )


@pytest.mark.asyncio
async def test_original_execution_deadline_is_rechecked_after_policy_wait(db):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory, timeout=2)
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow(
            "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
            inventory.cluster_id,
        )
        task = asyncio.create_task(decide(store, request))
        await wait_for_policy_lock(db)
        await blocker.execute(
            "SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM ($1::timestamptz-clock_timestamp())))+0.05)",
            request["admission_deadline"],
        )
    assert await asyncio.wait_for(task, 3) == {
        "action": "unavailable",
        "reason": "job_admission_expired",
    }
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE cluster_id=$1",
            inventory.cluster_id,
        )
        == 0
    )


@pytest.mark.asyncio
async def test_inventory_freshness_is_rechecked_after_policy_wait(db):
    store, inventory, _ = await environment(db, stale_after_seconds=6)
    request = await waiter(db, inventory)
    async with db.acquire() as blocker, blocker.transaction():
        await blocker.fetchrow(
            "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1 FOR UPDATE",
            inventory.cluster_id,
        )
        task = asyncio.create_task(decide(store, request))
        await wait_for_policy_lock(db)
        await blocker.execute("SELECT pg_sleep(1.1)")
    assert await asyncio.wait_for(task, 3) == {
        "action": "unavailable",
        "reason": "inventory_stale",
    }


@pytest.mark.asyncio
async def test_old_policy_charge_survives_new_policy_and_node_name_reuse(db):
    from orchestrator.services.vm_resource_reservation_store import (
        VMResourceReservationStore,
    )

    store, inventory, snapshot = await environment(db)
    request = await waiter(db, inventory)
    first = await decide(store, request)
    config = deepcopy(store.policy_document)
    config["policy"]["fairness"]["priorityAgingSeconds"] = 120
    digest = (
        "sha256:"
        + hashlib.sha256(
            json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    inventory.policy_digest = digest
    snapshot = successor(snapshot)
    snapshot["policy_digest"] = digest
    await publish(inventory, snapshot)
    await db.execute(
        "UPDATE vm_resource_admission_policy SET policy_digest=$2,document=$3::jsonb,revision=2 WHERE cluster_id=$1",
        inventory.cluster_id,
        digest,
        json.dumps(config),
    )
    store = VMResourceReservationStore(
        db, inventory=inventory, policy_document=config, policy_revision=2
    )
    waiting = await waiter(db, inventory)
    assert (await decide(store, waiting))["action"] == "wait"
    snapshot = successor(snapshot)
    snapshot["nodes"][0]["uid"] = str(uuid4())
    await publish(inventory, snapshot)
    assert (await decide(store, waiting))["action"] == "wait"
    held = await db.fetchrow(
        "SELECT state,node_uid,cpu_millicores FROM vm_resource_reservations WHERE id=$1",
        UUID(first["reservation_id"]),
    )
    assert (
        held["state"] == "reserved"
        and str(held["node_uid"]) == first["node_uid"]
        and held["cpu_millicores"] == 4000
    )


@pytest.mark.asyncio
async def test_deleting_external_pod_still_consumes_capacity(db):
    store, inventory, snapshot = await environment(db)
    snapshot = successor(snapshot)
    snapshot["pods"] = [
        {
            "uid": str(uuid4()),
            "name": "external",
            "namespace": "different",
            "node_uid": snapshot["nodes"][0]["uid"],
            "node_name": "node-a",
            "terminal": False,
            "deleting": True,
            "requests": {"cpu_millicores": 1, "memory_bytes": 0, "kvm_devices": 0},
            "vmi_uid": None,
            "reservation_id": None,
            "provision_generation": None,
        }
    ]
    await publish(inventory, snapshot)
    request = await waiter(db, inventory)
    assert (await decide(store, request))["action"] == "wait"
    newer = successor(snapshot)
    newer["pods"][0]["terminal"] = True
    await publish(inventory, newer)
    assert (await decide(store, request))["action"] == "admitted"


@pytest.mark.asyncio
async def test_teardown_reservation_replay_cannot_grant_creation(db):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    first = await decide(store, request)
    await db.execute(
        "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
        UUID(first["reservation_id"]),
    )
    assert await decide(store, request) == {
        "action": "unavailable",
        "reason": "reservation_teardown",
    }


@pytest.mark.asyncio
async def test_locked_policy_document_compares_exact_json_types(db):
    store, inventory, _ = await environment(db)
    request = await waiter(db, inventory)
    await db.execute(
        "UPDATE vm_resource_admission_policy SET document=jsonb_set(document,'{policy,hostCost,cpuMillicoresPerVcpuDenominator}','true') WHERE cluster_id=$1",
        inventory.cluster_id,
    )
    assert await decide(store, request) == {
        "action": "unavailable",
        "reason": "resource_policy_changed",
    }
