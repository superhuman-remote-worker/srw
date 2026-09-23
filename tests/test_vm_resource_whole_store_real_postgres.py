"""Whole-launcher budget decisions on the real locked admission route."""

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from uuid import UUID, uuid4

import pytest

from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from orchestrator.services.vm_resource_inventory_store import VMResourceInventoryStore
from orchestrator.services.vm_resource_reservation_store import VMResourceReservationStore
from shared.vm_launcher_profile import predict_launcher
from tests.test_vm_creation_retry_real_postgres import (
    admit,
    admitted_job,
    db as _base_db,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)
from tests.test_vm_resource_configuration import whole_launcher_configuration
from tests.test_vm_resource_inventory_contract import snapshot as inventory_snapshot
from shared.vm_resource_inventory import INVENTORY_KINDS
from tests.test_vm_resource_inventory_real_postgres import publish
from tests.test_vm_resource_policy import whole_launcher_policy
from tests.test_vm_resource_whole_schema_real_postgres import (
    db as _db_fixture,
    whole_schema,  # noqa: F401
)

db = _db_fixture


def digest(value):
    return "sha256:" + hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


async def environment(
    db, *, installation_count=1, owner_count=1, age_seconds=1,
    node_arch="amd64", node_tun=10,
):
    policy = whole_launcher_policy()
    policy["namespace"] = "workers"
    policy["policy"]["stableClusterId"] = "test-" + str(uuid4())
    policy["policy"].update(shadowEnabled=True, enforcementEnabled=True)
    policy["policy"]["inventory"].update(maxItems=100, maxBytes=100000)
    vector = policy["policy"]["hostCost"]
    from shared.vm_resource_policy import parse_whole_launcher_host_cost_policy

    demand = parse_whole_launcher_host_cost_policy(vector).cost(8, "16Gi")
    for key, count in (("installationBudget", installation_count), ("ownerBudget", owner_count)):
        policy["policy"][key] = {
            "cpuMillicores": demand.cpu_millicores * count,
            "memoryBytes": demand.memory_bytes * count,
            "ephemeralStorageBytes": demand.ephemeral_storage_bytes * count,
            "kvmDevices": demand.kvm_devices * count,
            "tunDevices": demand.tun_devices * count,
            "vhostNetDevices": demand.vhost_net_devices * count,
        }
    settings = policy["policy"]["inventory"]
    value = inventory_snapshot()
    value.update(
        protocol=2, cluster_id=policy["policy"]["stableClusterId"],
        policy_digest=digest(policy),
        label_keys=sorted(settings["nodeLabelKeys"]),
    )
    value["resource_versions"].update(kubevirt="10", limitranges="10")
    value["installed_profile"] = {
        "uid": str(uuid4()), "namespace": "kubevirt", "name": "kubevirt",
        "generation": 2, "observedGeneration": 2,
        "targetVersion": "v1.6.6", "observedVersion": "v1.6.6",
        "targetDeploymentID": "settled", "observedDeploymentID": "settled",
        "profile": deepcopy(policy["policy"]["launcherProfile"]),
    }
    value["nodes"][0]["labels"] = {
        "kubernetes.io/hostname": "node-a", "kubernetes.io/arch": node_arch
    }
    value["nodes"][0]["allocatable"].update(
        cpu_millicores=4000, memory_bytes=64 * 1024**3,
        ephemeral_storage_bytes=10**10, kvm_devices=10,
        tun_devices=node_tun, vhost_net_devices=10,
    )
    value["storage_classes"] = [{
        "uid": str(uuid4()), "name": "local", "binding_mode": "WaitForFirstConsumer",
        "allowed_topology": None,
    }]
    value["started_at"] = value["finished_at"] = (
        datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    ).isoformat()
    inventory = VMResourceInventoryStore(
        db, cluster_id=value["cluster_id"], namespace="workers",
        policy_digest=value["policy_digest"], label_keys=value["label_keys"],
        max_items=100, max_bytes=100000, stale_after_seconds=60,
        history_limit=3, protocol=2, kubevirt_namespace="kubevirt",
        kubevirt_name="kubevirt",
    )
    await publish(inventory, value)
    await db.execute(
        "INSERT INTO vm_resource_admission_policy(cluster_id,namespace,policy_digest,document,mode) "
        "VALUES($1,$2,$3,$4::jsonb,'enforce')",
        inventory.cluster_id, inventory.namespace, inventory.policy_digest,
        json.dumps(policy),
    )
    return VMResourceReservationStore(
        db, inventory=inventory, policy_document=policy, policy_revision=1,
    ), inventory, value, demand


async def waiter(db, store, inventory, *, user_id=None):
    config = whole_launcher_configuration()
    config.update(namespace="workers", storage_class="local")
    resource = config["resource_admission"]
    profile = resource["template_profile"]
    profile.update(storage_class="local", guest_vcpus=8, guest_memory_bytes=16 * 1024**3)
    resource["cluster_id"] = inventory.cluster_id
    resource["policy_digest"] = inventory.policy_digest
    resource["launcher_prediction"]["vector"] = predict_launcher(
        store.launcher_profile, guest_vcpus=8, guest_memory_bytes=16 * 1024**3,
    ).to_six_dict()
    resource["host_mapping"]["vector"] = store.cost.cost(8, "16Gi").to_six_dict()
    job, generation, proposal = await admitted_job(db, controller_configuration=config)
    if user_id is not None:
        await db.execute("INSERT INTO users(id,display_name) VALUES($1,'whole-owner') ON CONFLICT DO NOTHING", user_id)
        await db.execute("UPDATE jobs SET user_id=$2 WHERE id=$1", job, user_id)
    async with db.acquire() as conn, conn.transaction():
        return await VMCreationRetryStore(
            db, _resource_waiter_writer=store
        ).admit_on_conn(
            conn, job_id=str(job), expected_generation=str(generation),
            request_id=str(uuid4()), proposal=proposal,
        )


@pytest.mark.asyncio
async def test_atomic_final_installation_slot_and_exact_replay(db):
    store, inventory, _, demand = await environment(db)
    first = await waiter(db, store, inventory, user_id=uuid4())
    second = await waiter(db, store, inventory, user_id=uuid4())
    result = await asyncio.gather(
        store.admit(request_id=str(first["request_id"])),
        store.admit(request_id=str(second["request_id"])),
    )
    assert sorted(row["action"] for row in result) == ["admitted", "wait"]
    winner = first if result[0]["action"] == "admitted" else second
    assert await store.admit(request_id=str(winner["request_id"])) == next(
        row for row in result if row["action"] == "admitted"
    )
    rows = await db.fetch(
        "SELECT * FROM vm_resource_reservations WHERE cluster_id=$1", inventory.cluster_id
    )
    assert len(rows) == 1
    assert rows[0]["resource_version"] == 2
    assert (rows[0]["cpu_millicores"], rows[0]["ephemeral_storage_bytes"],
            rows[0]["tun_devices"], rows[0]["vhost_net_devices"]) == (
        demand.cpu_millicores, demand.ephemeral_storage_bytes,
        demand.tun_devices, demand.vhost_net_devices,
    )


@pytest.mark.asyncio
async def test_same_owner_parallel_requests_spend_one_owner_slot(db):
    store, inventory, _, _ = await environment(db, installation_count=2)
    owner = uuid4()
    requests = [
        await waiter(db, store, inventory, user_id=owner)
        for _ in range(2)
    ]
    results = await asyncio.gather(*(
        store.admit(request_id=str(row["request_id"])) for row in requests
    ))
    assert sum(result["action"] == "admitted" for result in results) == 1
    losing = next(
        row for row, result in zip(requests, results)
        if result["action"] != "admitted"
    )
    assert await store.admit(request_id=str(losing["request_id"])) == {
        "action": "wait", "reason": "owner_budget",
    }


@pytest.mark.asyncio
async def test_different_owners_parallel_requests_can_use_distinct_owner_slots(db):
    store, inventory, _, _ = await environment(db, installation_count=2)
    requests = [
        await waiter(db, store, inventory, user_id=uuid4())
        for _ in range(2)
    ]
    await asyncio.gather(*(
        store.admit(request_id=str(row["request_id"])) for row in requests
    ))
    results = [
        await store.admit(request_id=str(row["request_id"])) for row in requests
    ]
    assert [result["action"] for result in results] == ["admitted", "admitted"]
    assert await db.fetchval(
        "SELECT count(*) FROM vm_resource_reservations WHERE cluster_id=$1",
        inventory.cluster_id,
    ) == 2


@pytest.mark.asyncio
async def test_owner_budget_and_installed_profile_are_authoritative(db):
    store, inventory, value, _ = await environment(db, installation_count=2)
    owner = uuid4()
    first = await waiter(db, store, inventory, user_id=owner)
    second = await waiter(db, store, inventory, user_id=owner)
    assert (await store.admit(request_id=str(first["request_id"])))["action"] == "admitted"
    assert await store.admit(request_id=str(second["request_id"])) == {
        "action": "wait", "reason": "owner_budget",
    }
    third = await waiter(db, store, inventory, user_id=uuid4())
    changed = deepcopy(value)
    changed["snapshot_id"] = str(uuid4())
    changed["sequence"] += 1
    changed["started_at"] = changed["finished_at"] = datetime.now(timezone.utc).isoformat()
    changed["installed_profile"]["profile"]["cpuAllocationRatio"] = 5
    await publish(inventory, changed)
    assert await store.admit(request_id=str(third["request_id"])) == {
        "action": "unavailable", "reason": "installed_launcher_profile_changed",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("held_state", ["reserved", "active", "warm", "teardown"])
async def test_monotonic_observed_high_water_charges_every_held_phase(db, held_state):
    store, inventory, _, demand = await environment(
        db, installation_count=2, owner_count=2,
    )
    first = await waiter(db, store, inventory, user_id=uuid4())
    assert (await store.admit(request_id=str(first["request_id"])))["action"] == "admitted"
    if held_state in {"active", "warm"}:
        await db.execute(
            "UPDATE vm_resource_reservations SET state='active',vm_uid=$2,vmi_uid=$3,launcher_uid=$4 WHERE request_id=$1",
            first["request_id"], uuid4(), uuid4(), uuid4(),
        )
    if held_state == "warm":
        await db.execute(
            "UPDATE vm_resource_reservations SET state='warm' WHERE request_id=$1",
            first["request_id"],
        )
    if held_state == "teardown":
        await db.execute(
            "UPDATE vm_resource_reservations SET state='teardown' WHERE request_id=$1",
            first["request_id"],
        )
    await db.execute(
        "UPDATE vm_resource_reservations SET observed_cpu_millicores=$2,observed_memory_bytes=$3,observed_ephemeral_storage_bytes=$4,observed_kvm_devices=$5,observed_tun_devices=$6,observed_vhost_net_devices=$7 WHERE request_id=$1",
        first["request_id"], demand.cpu_millicores + 1, demand.memory_bytes,
        demand.ephemeral_storage_bytes, demand.kvm_devices,
        demand.tun_devices, demand.vhost_net_devices,
    )
    second = await waiter(db, store, inventory, user_id=uuid4())
    assert await store.admit(request_id=str(second["request_id"])) == {
        "action": "wait", "reason": "installation_budget",
    }
    assert await db.fetchval(
        "SELECT count(*) FROM vm_resource_reservations WHERE cluster_id=$1",
        inventory.cluster_id,
    ) == 1


async def foreign_held_charge(db, inventory, current_snapshot, demand, *, version, owner):
    """Native historical row on the same installation, with a different policy digest."""
    old = deepcopy(current_snapshot)
    old["policy_digest"] = digest(str(uuid4()))
    old["snapshot_id"] = str(uuid4())
    old["started_at"] = old["finished_at"] = (
        datetime.now(timezone.utc) - timedelta(seconds=1)
    ).isoformat()
    if version == 1:
        old["protocol"] = 1
        del old["installed_profile"]
        old["resource_versions"].pop("kubevirt")
        old["resource_versions"].pop("limitranges")
        for key in ("ephemeral_storage_bytes", "tun_devices", "vhost_net_devices"):
            old["nodes"][0]["allocatable"].pop(key)
    old_store = VMResourceInventoryStore(
        db, cluster_id=inventory.cluster_id, namespace=inventory.namespace,
        policy_digest=old["policy_digest"], label_keys=old["label_keys"],
        max_items=100, max_bytes=100000, stale_after_seconds=60,
        history_limit=3, protocol=version,
        kubevirt_namespace="kubevirt" if version == 2 else None,
        kubevirt_name="kubevirt" if version == 2 else None,
    )
    receipt = await publish(old_store, old)
    job, generation, proposal = await admitted_job(db)
    await db.execute("INSERT INTO users(id,display_name) VALUES($1,'old-owner') ON CONFLICT DO NOTHING", owner)
    await db.execute("UPDATE jobs SET user_id=$2 WHERE id=$1", job, owner)
    retry = await admit(db, job, generation, proposal)
    node = old["nodes"][0]
    await db.execute(
        "INSERT INTO vm_resource_nodes(cluster_id,node_uid,node_name) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
        inventory.cluster_id, node["uid"], node["name"],
    )
    waiter_values = (
        retry["request_id"], job, generation, inventory.cluster_id,
        old["policy_digest"], "user:" + str(owner), retry["request_digest"],
        demand.cpu_millicores, demand.memory_bytes, demand.kvm_devices,
    )
    if version == 2:
        await db.execute(
            "INSERT INTO vm_resource_waiters(request_id,job_id,provision_generation,cluster_id,policy_digest,owner_key,priority,request_digest,guest_vcpus,guest_memory_bytes,cpu_millicores,memory_bytes,kvm_devices,placement,ephemeral_storage_bytes,tun_devices,vhost_net_devices,resource_version) "
            "VALUES($1,$2,$3,$4,$5,$6,5,$7,8,17179869184,$8,$9,$10,'{}',$11,$12,$13,2)",
            *waiter_values, demand.ephemeral_storage_bytes,
            demand.tun_devices, demand.vhost_net_devices,
        )
    else:
        await db.execute(
            "INSERT INTO vm_resource_waiters(request_id,job_id,provision_generation,cluster_id,policy_digest,owner_key,priority,request_digest,guest_vcpus,guest_memory_bytes,cpu_millicores,memory_bytes,kvm_devices,placement) "
            "VALUES($1,$2,$3,$4,$5,$6,5,$7,8,17179869184,$8,$9,$10,'{}')",
            *waiter_values,
        )
    reservation_values = (
        uuid4(), retry["request_id"], inventory.cluster_id,
        old["policy_digest"], UUID(node["uid"]), node["name"],
        demand.cpu_millicores, demand.memory_bytes, demand.kvm_devices,
        UUID(old["snapshot_id"]), receipt["digest"],
    )
    if version == 2:
        await db.execute(
            "INSERT INTO vm_resource_reservations(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,cpu_millicores,memory_bytes,kvm_devices,snapshot_id,snapshot_digest,ephemeral_storage_bytes,tun_devices,vhost_net_devices,resource_version) "
            "VALUES($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,2)",
            *reservation_values, demand.ephemeral_storage_bytes,
            demand.tun_devices, demand.vhost_net_devices,
        )
    else:
        await db.execute(
            "INSERT INTO vm_resource_reservations(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,cpu_millicores,memory_bytes,kvm_devices,snapshot_id,snapshot_digest) "
            "VALUES($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
            *reservation_values,
        )
    await db.execute(
        "UPDATE vm_resource_waiters SET state='admitted' WHERE request_id=$1",
        retry["request_id"],
    )


@pytest.mark.asyncio
async def test_across_policy_digest_charge_and_old_unknown_cutover(db):
    store, inventory, value, demand = await environment(db, installation_count=1)
    await foreign_held_charge(db, inventory, value, demand, version=2, owner=uuid4())
    target = await waiter(db, store, inventory, user_id=uuid4())
    assert await store.admit(request_id=str(target["request_id"])) == {
        "action": "wait", "reason": "installation_budget",
    }


@pytest.mark.asyncio
async def test_legacy_three_field_held_charge_blocks_cutover(db):
    store, inventory, value, demand = await environment(db, installation_count=2)
    await foreign_held_charge(db, inventory, value, demand, version=1, owner=uuid4())
    target = await waiter(db, store, inventory, user_id=uuid4())
    assert await store.admit(request_id=str(target["request_id"])) == {
        "action": "unavailable", "reason": "legacy_occupancy_unclassified",
    }


async def publish_attributable_vm(inventory, value, *, with_pod):
    observed = deepcopy(value)
    observed["snapshot_id"] = str(uuid4())
    observed["sequence"] += 1
    observed["started_at"] = observed["finished_at"] = datetime.now(timezone.utc).isoformat()
    node = observed["nodes"][0]
    vm_uid, vmi_uid = str(uuid4()), str(uuid4())
    observed["vms"] = [{
        "uid": vm_uid, "name": "agent-vm-legacy", "owner_kind": "job",
        "owner_id": str(uuid4()), "provision_generation": str(uuid4()),
        "deleting": False,
    }]
    observed["vmis"] = [{
        "uid": vmi_uid, "name": "agent-vm-legacy", "vm_uid": vm_uid,
        "node_uid": node["uid"], "node_name": node["name"],
        "phase": "Running", "deleting": False,
    }]
    if with_pod:
        observed["pods"] = [{
            "uid": str(uuid4()), "namespace": observed["namespace"],
            "name": "virt-launcher-legacy", "node_uid": node["uid"],
            "node_name": node["name"], "terminal": False, "deleting": False,
            "requests": {
                "cpu_millicores": 205, "memory_bytes": 861277888,
                "ephemeral_storage_bytes": 50000000, "kvm_devices": 1,
                "tun_devices": 1, "vhost_net_devices": 1,
            },
            "vmi_uid": vmi_uid, "reservation_id": None,
            "provision_generation": observed["vms"][0]["provision_generation"],
        }]
    await publish(inventory, observed)


@pytest.mark.asyncio
@pytest.mark.parametrize("with_pod", [True, False])
async def test_live_attributable_vm_without_v2_charge_refuses_budget_cutover(db, with_pod):
    store, inventory, value, _ = await environment(db, installation_count=2)
    await publish_attributable_vm(inventory, value, with_pod=with_pod)
    target = await waiter(db, store, inventory, user_id=uuid4())
    assert await store.admit(request_id=str(target["request_id"])) == {
        "action": "unavailable", "reason": "legacy_occupancy_unclassified",
    }
    assert await db.fetchval(
        "SELECT count(*) FROM vm_resource_reservations WHERE cluster_id=$1",
        inventory.cluster_id,
    ) == 0


@pytest.mark.asyncio
async def test_srw_marked_launcher_without_vm_owner_link_refuses_cutover(db):
    store, inventory, value, _ = await environment(db, installation_count=2)
    observed = deepcopy(value)
    observed["snapshot_id"] = str(uuid4())
    observed["sequence"] += 1
    observed["started_at"] = observed["finished_at"] = datetime.now(timezone.utc).isoformat()
    node = observed["nodes"][0]
    vmi_uid = str(uuid4())
    observed["vmis"] = [{
        "uid": vmi_uid, "name": "legacy-unlinked", "vm_uid": None,
        "node_uid": node["uid"], "node_name": node["name"],
        "phase": "Running", "deleting": False,
    }]
    observed["pods"] = [{
        "uid": str(uuid4()), "namespace": observed["namespace"],
        "name": "virt-launcher-unlinked", "node_uid": node["uid"],
        "node_name": node["name"], "terminal": False, "deleting": False,
        "requests": {
            "cpu_millicores": 205, "memory_bytes": 861277888,
            "ephemeral_storage_bytes": 50000000, "kvm_devices": 1,
            "tun_devices": 1, "vhost_net_devices": 1,
        },
        "vmi_uid": vmi_uid, "reservation_id": str(uuid4()),
        "provision_generation": str(uuid4()),
    }]
    await publish(inventory, observed)
    target = await waiter(db, store, inventory, user_id=uuid4())
    assert await store.admit(request_id=str(target["request_id"])) == {
        "action": "unavailable", "reason": "legacy_occupancy_unclassified",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("marked", [True, False])
async def test_pod_without_vmi_owner_link_only_blocks_when_srw_marked(db, marked):
    store, inventory, value, _ = await environment(db, installation_count=2)
    observed = deepcopy(value)
    observed["snapshot_id"] = str(uuid4())
    observed["sequence"] += 1
    observed["started_at"] = observed["finished_at"] = datetime.now(timezone.utc).isoformat()
    node = observed["nodes"][0]
    observed["pods"] = [{
        "uid": str(uuid4()), "namespace": observed["namespace"],
        "name": "virt-launcher-owner-link-absent", "node_uid": node["uid"],
        "node_name": node["name"], "terminal": False, "deleting": False,
        "requests": {
            "cpu_millicores": 205, "memory_bytes": 861277888,
            "ephemeral_storage_bytes": 50000000, "kvm_devices": 1,
            "tun_devices": 1, "vhost_net_devices": 1,
        },
        "vmi_uid": None,
        "reservation_id": str(uuid4()) if marked else None,
        "provision_generation": str(uuid4()) if marked else None,
    }]
    await publish(inventory, observed)
    target = await waiter(db, store, inventory, user_id=uuid4())
    result = await store.admit(request_id=str(target["request_id"]))
    if marked:
        assert result == {
            "action": "unavailable", "reason": "legacy_occupancy_unclassified",
        }
    else:
        assert result["action"] == "admitted"


@pytest.mark.asyncio
async def test_exact_bound_live_launcher_uses_one_v2_charge(db):
    store, inventory, value, demand = await environment(
        db, installation_count=2, owner_count=2,
    )
    first = await waiter(db, store, inventory, user_id=uuid4())
    admitted = await store.admit(request_id=str(first["request_id"]))
    assert admitted["action"] == "admitted"
    vm_uid, vmi_uid, launcher_uid = uuid4(), uuid4(), uuid4()
    await db.execute(
        "UPDATE vm_resource_reservations SET state='active',vm_uid=$2,"
        "vmi_uid=$3,launcher_uid=$4 WHERE id=$1",
        UUID(admitted["reservation_id"]), vm_uid, vmi_uid, launcher_uid,
    )
    observed = deepcopy(value)
    observed["snapshot_id"] = str(uuid4())
    observed["sequence"] += 1
    observed["started_at"] = observed["finished_at"] = datetime.now(timezone.utc).isoformat()
    node = observed["nodes"][0]
    observed["vms"] = [{
        "uid": str(vm_uid), "name": "agent-vm-bound", "owner_kind": "job",
        "owner_id": str(first["job_id"]),
        "provision_generation": str(first["provision_generation"]),
        "deleting": False,
    }]
    observed["vmis"] = [{
        "uid": str(vmi_uid), "name": "agent-vm-bound", "vm_uid": str(vm_uid),
        "node_uid": node["uid"], "node_name": node["name"],
        "phase": "Running", "deleting": False,
    }]
    observed["pods"] = [{
        "uid": str(launcher_uid), "namespace": observed["namespace"],
        "name": "virt-launcher-bound", "node_uid": node["uid"],
        "node_name": node["name"], "terminal": False, "deleting": False,
        "requests": demand.to_six_dict(), "vmi_uid": str(vmi_uid),
        "reservation_id": admitted["reservation_id"],
        "provision_generation": str(first["provision_generation"]),
    }]
    await publish(inventory, observed)
    second = await waiter(db, store, inventory, user_id=uuid4())
    second_result = await store.admit(request_id=str(second["request_id"]))
    assert second_result["action"] == "admitted"
    assert await db.fetchval(
        "SELECT count(*) FROM vm_resource_reservations WHERE cluster_id=$1",
        inventory.cluster_id,
    ) == 2


@pytest.mark.asyncio
async def test_stale_installed_observation_is_not_capacity(db):
    store, inventory, _, _ = await environment(db, age_seconds=120)
    target = await waiter(db, store, inventory, user_id=uuid4())
    assert await store.admit(request_id=str(target["request_id"])) == {
        "action": "unavailable", "reason": "inventory_stale",
    }


@pytest.mark.asyncio
async def test_missing_installed_observation_invalidates_old_capacity(db):
    store, inventory, value, _ = await environment(db)
    target = await waiter(db, store, inventory, user_id=uuid4())
    missing = deepcopy(value)
    missing.update(
        snapshot_id=str(uuid4()), sequence=value["sequence"] + 1,
        started_at=datetime.now(timezone.utc).isoformat(),
        finished_at=datetime.now(timezone.utc).isoformat(),
        complete=False, reason="collection_failed", resource_versions={},
        installed_profile=None,
    )
    for kind in INVENTORY_KINDS:
        missing[kind] = []
    await publish(inventory, missing)
    assert await store.admit(request_id=str(target["request_id"])) == {
        "action": "unavailable", "reason": "inventory_incomplete",
    }


@pytest.mark.asyncio
async def test_six_dimension_node_fit_and_exact_architecture(db):
    no_tun, inv_a, _, _ = await environment(db, node_tun=0)
    no_tun_request = await waiter(db, no_tun, inv_a, user_id=uuid4())
    assert (await no_tun.admit(request_id=str(no_tun_request["request_id"]))) == {
        "action": "nonfit",
    }
    wrong_arch, inv_b, _, _ = await environment(db, node_arch="arm64")
    wrong_arch_request = await waiter(db, wrong_arch, inv_b, user_id=uuid4())
    assert (await wrong_arch.admit(request_id=str(wrong_arch_request["request_id"]))) == {
        "action": "wait",
    }
