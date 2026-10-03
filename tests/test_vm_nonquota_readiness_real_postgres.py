"""Actual-source non-quota creation keeps every readiness authority guard."""

import hashlib
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services.vm_creation_readiness import _ready
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from orchestrator.services.vm_readiness import VMReadinessService
from orchestrator.services.vm_thread_network import document, verified_source
from tests.test_pinned_vm_failed_initial_end_real_postgres import _release_binding
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent
from tests.test_vm_thread_adopted_without_quotas_delete_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    adopted_source,
    db,  # noqa: F401
    pg_dsn,  # noqa: F401
    setup,  # noqa: F401
    thread_schema,  # noqa: F401
)


async def prepared(store, controller_setup, monkeypatch, rotated):
    current, source = await adopted_source(store, controller_setup, monkeypatch)
    if rotated:
        assert await _release_binding(store, current) == "released"
        assert await store.delete_agent(str(current["agent_id"]))
        current = await _bind_cold_agent(store, current["id"])
    vm = document(current["metadata"])["vm"]
    registration = uuid4().hex
    pending = {
        "status": "ssh_pending",
        "vmi_uid": str(uuid4()),
        "active_pod_uid": str(uuid4()),
        "ssh_registration_id": registration,
        "ssh_host": "10.42.0.42",
        "pod_ip": "10.42.0.42",
        "ssh_port": 22,
        "ssh_ready_source": "provisioner_probe",
        "ssh_verified_at": datetime.now(timezone.utc).isoformat(),
    }
    assert await store.merge_thread_vm_context_if_provision_generation(
        str(current["id"]),
        str(source["provision_generation"]),
        pending,
        require_status_not_ready=True,
    )
    current = await store.get_thread(str(current["id"]))
    vm = document(current["metadata"])["vm"]
    evidence = document(
        await store.fetchval(
            "SELECT evidence FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='vm' AND state='observed'",
            source["request_id"],
        )
    )
    updates = {**pending, "status": "ready"}
    assert _ready(
        vm | updates, source, evidence, datetime.now(timezone.utc).timestamp()
    )
    return current, source, vm, updates


@pytest.mark.asyncio
async def test_verified_nonquota_source_accepts_only_its_actual_frozen_request(
    db,  # noqa: F811 - imported PostgreSQL fixture
    setup,  # noqa: F811 - imported actuator fixture
    monkeypatch,
):
    current, source = await adopted_source(db, setup, monkeypatch)
    assert document(source["controller_configuration"])["version"] == 1
    assert verified_source(
        source,
        thread_id=str(current["id"]),
        generation=str(source["provision_generation"]),
        request_id=str(source["request_id"]),
    ) == document(source["canonical_request"])


@pytest.mark.asyncio
@pytest.mark.parametrize("rotated", [False, True])
async def test_nonquota_current_and_confirmed_successor_reach_actual_ssh_gate(
    db,  # noqa: F811
    setup,  # noqa: F811
    monkeypatch,
    rotated,
):
    current, source, vm, _ = await prepared(db, setup, monkeypatch, rotated)
    row = next(
        row
        for row in await db.list_thread_vm_readiness_candidates()
        if row["entity_id"] == str(current["id"])
    )
    ssh = AsyncMock(return_value=(False, 1, "controlled SSH observation"))
    monkeypatch.setattr("orchestrator.services.vm_readiness.wait_for_agent_ssh", ssh)
    provisioner = SimpleNamespace(
        query_status=AsyncMock(
            return_value={
                "phase": "Running",
                "ready": True,
                "provision_generation": str(source["provision_generation"]),
                "pod_ip": vm["pod_ip"],
                "active_pod_uid": vm["active_pod_uid"],
            }
        ),
        _set_context_if_generation=AsyncMock(return_value=True),
    )
    await VMReadinessService(db, provisioner, trigger_dispatch=lambda: None)._probe(
        "thread",
        str(current["id"]),
        str(source["provision_generation"]),
        vm,
        row,
        False,
    )
    assert ssh.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("rotated", [False, True])
async def test_nonquota_current_and_confirmed_successor_publish_locked_ready(
    db,  # noqa: F811
    setup,  # noqa: F811
    monkeypatch,
    rotated,
):
    current, source, vm, updates = await prepared(db, setup, monkeypatch, rotated)
    assert (
        await VMProvisioningPhaseStore(db).publish_thread_ready(
            str(current["id"]),
            str(source["provision_generation"]),
            vm["ssh_registration_id"],
            vm["vm_uid"],
            updates,
        )
        is True
    )
    after = await db.get_thread(str(current["id"]))
    assert after["runtime_generation"] == current["runtime_generation"]
    assert document(after["metadata"])["vm"]["status"] == "ready"
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["enforcement", "reservation", "waiter"])
async def test_nonquota_ready_refuses_quota_authority_or_obligations(
    db,  # noqa: F811
    setup,  # noqa: F811
    monkeypatch,
    defect,
):
    current, source, vm, updates = await prepared(db, setup, monkeypatch, False)
    if defect == "enforcement":
        monkeypatch.setattr(
            "orchestrator.services.vm_resource_job_runtime.configured_enforcement_policy",
            lambda: object(),
        )
    else:
        # Only damaged history in this disposable database. A v1 source never
        # normally obtains either kind of quota obligation.
        async with db.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            if defect == "reservation":
                await conn.execute(
                    "INSERT INTO vm_resource_reservations "
                    "(id,request_id,revision,cluster_id,policy_digest,node_uid,node_name,"
                    "cpu_millicores,memory_bytes,kvm_devices,snapshot_id,snapshot_digest) "
                    "VALUES ($1,$2,1,'test',$3,$4,'test-node',1000,1024,1,$5,$3)",
                    uuid4(),
                    source["request_id"],
                    "sha256:" + "0" * 64,
                    uuid4(),
                    uuid4(),
                )
            else:
                await conn.execute(
                    "INSERT INTO vm_resource_waiters "
                    "(request_id,provision_generation,cluster_id,policy_digest,owner_key,"
                    "priority,request_digest,guest_vcpus,guest_memory_bytes,cpu_millicores,"
                    "memory_bytes,kvm_devices,placement,owner_kind,thread_id) "
                    "VALUES ($1,$2,'test',$3,'test-owner',0,$4,1,1024,1000,1024,1,'{}','thread',$5)",
                    source["request_id"],
                    source["provision_generation"],
                    "sha256:" + "0" * 64,
                    source["request_digest"],
                    current["id"],
                )
    assert (
        await VMProvisioningPhaseStore(db).publish_thread_ready(
            str(current["id"]),
            str(source["provision_generation"]),
            vm["ssh_registration_id"],
            vm["vm_uid"],
            updates,
        )
        is False
    )
    after = await db.get_thread(str(current["id"]))
    assert document(after["metadata"])["vm"]["status"] == "ssh_pending"
    assert await db.fetchval(
        "SELECT ready_at IS NULL FROM vm_creation_retries WHERE request_id=$1",
        source["request_id"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"version": True},
        {"version": 1.0},
        {"version": 2},
        {"version": 4},
        {"resource_admission": {}},
        {"network_profile_policy": {}},
    ],
)
async def test_nonquota_source_refuses_coherently_digested_invalid_policy(
    db,  # noqa: F811
    setup,  # noqa: F811
    monkeypatch,
    change,
):
    current, source = await adopted_source(db, setup, monkeypatch)
    configuration = document(source["controller_configuration"]) | change
    damaged = source | {
        "controller_configuration": configuration,
        "controller_configuration_digest": "sha256:"
        + hashlib.sha256(
            json.dumps(
                configuration, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode()
        ).hexdigest(),
    }
    assert (
        verified_source(
            damaged,
            thread_id=str(current["id"]),
            generation=str(source["provision_generation"]),
            request_id=str(source["request_id"]),
        )
        is None
    )
