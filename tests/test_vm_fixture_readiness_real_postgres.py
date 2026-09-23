"""Worker-disabled gate fixtures use the real final Ready release writer."""

import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from tests.test_vm_creation_readiness_real_postgres import (  # noqa: F401
    _schema_applied, db, pg_dsn, postgres_db_fixture, ready_creation,
)


async def fixture_ready(db, monkeypatch, *, marker_key, run, profiled=False):  # noqa: F811
    from shared.worker_queue import enqueue_worker_batch, hold_worker_batch_for_preflight

    if profiled:
        from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
        from shared.vm_network_profile import NETWORK_DATA, NETWORK_PROFILE
        from tests import test_vm_creation_retry_real_postgres as creation_tests

        original = creation_tests.build_vm_creation_request

        def profiled_request(**kwargs):
            kwargs["vm_image"] = "registry.example/guest@sha256:" + "a" * 64
            kwargs["network_profile"] = NETWORK_PROFILE
            return original(**kwargs)

        monkeypatch.setattr(creation_tests, "build_vm_creation_request", profiled_request)
        original_observe = VMCreationRetryStore.observe_effect

        async def profiled_observe(self, **kwargs):
            observation = kwargs["observation"]
            if observation.get("object", {}).get("kind") == "VirtualMachine":
                cloud_volume = observation["object"]["spec"]["template"]["spec"]["volumes"][1]
                cloud_volume["cloudInitNoCloud"]["networkData"] = NETWORK_DATA
            return await original_observe(self, **kwargs)

        monkeypatch.setattr(VMCreationRetryStore, "observe_effect", profiled_observe)
    created = await ready_creation(db, monkeypatch)
    row = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1",
        created["request_id"],
    )
    owner = uuid4()
    request = json.loads(row["canonical_request"])
    preflight = {
        "version": 1, "job_id": str(row["job_id"]),
        "request_id": str(row["request_id"]),
        "request": request, "request_digest": row["request_digest"],
        "revision": 0, "attempt": 0, "state": "admitted",
        "execution_id": str(row["execution_id"]),
        "execution_revision": row["execution_revision"],
        "execution_generation": row["execution_generation"],
        "admission_deadline": row["admission_deadline"].isoformat()
        if row["admission_deadline"] else None,
    }
    async with db.acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO users(id,display_name,is_approved,is_admin) "
            "VALUES($1,$2,true,false)", owner, f"fixture {run}",
        )
        await conn.execute(
            "UPDATE jobs SET user_id=$2,execution_lane='stateless',"
            "context=jsonb_set(jsonb_set(context,$3::text[],to_jsonb($4::text)),"
            "'{vm,creation_preflight}',$5::jsonb) WHERE id=$1",
            row["job_id"], owner, [marker_key], run, json.dumps(preflight),
        )
        await enqueue_worker_batch(conn, job_id=row["job_id"])
        await hold_worker_batch_for_preflight(
            conn, job_id=row["job_id"], preserve_attempts=True,
        )
    return row, owner, preflight


def _boot_phase(job_id, vm, vmi_uid):
    from shared.vm_provisioning_phases import observe_provisioning

    return observe_provisioning(None, {
        "version": 1, "owner_kind": "job", "owner_id": str(job_id),
        "namespace": vm["namespace"],
        "provision_generation": vm["provision_generation"],
        "vm_uid": vm["vm_uid"], "vmi_uid": vmi_uid,
        "rootdisk_dv_name": None, "rootdisk_dv_uid": None,
        "rootdisk_pvc_name": f"agent-vm-{job_id}-rootdisk",
        "rootdisk_pvc_uid": vm["rootdisk_pvc_uid"],
        "disk_mode": "clone", "disk_phase": "ready",
        "vmi_phase": "running", "disk_progress": 100,
    }, now=time.time())


@pytest.mark.asyncio
@pytest.mark.parametrize("marker_key", [
    "vm_retained_resume_acceptance_gate",
    "vm_workspace_recovery_acceptance_gate",
])
async def test_owned_paused_fixture_settles_real_ready_without_dispatch(
    db, monkeypatch, marker_key,  # noqa: F811
):
    from orchestrator.operator_cli.vm_fixture_readiness import settle_owned_fixture_ready

    run = "srw-a1-ready-" + uuid4().hex[:8]
    row, owner, preflight = await fixture_ready(
        db, monkeypatch, marker_key=marker_key, run=run,
    )
    job_id = row["job_id"]
    async with db.acquire() as conn:
        before_queue = dict(await conn.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1", job_id,
        ))
        before_request = dict(await conn.fetchrow(
            "SELECT canonical_request,request_digest,admission_deadline,"
            "provision_generation FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        ))
    arguments = dict(
        db=db, job_id=str(job_id), owner_id=str(owner), run_id=run,
        marker_key=marker_key, request_id=str(row["request_id"]),
        generation=str(row["provision_generation"]),
        vm_uid=str(row["observed_vm_uid"]),
        pvc_uid=str(row["observed_pvc_uid"]),
    )
    assert await settle_owned_fixture_ready(**arguments)
    async with db.acquire() as conn:
        retry = await conn.fetchrow(
            "SELECT ready_at,canonical_request,request_digest,admission_deadline,"
            "provision_generation FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        )
        context = json.loads(await conn.fetchval(
            "SELECT context FROM jobs WHERE id=$1", job_id,
        ))
        queue = dict(await conn.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1", job_id,
        ))
        assert retry["ready_at"] is not None
        assert "_vm_creation_pending" not in context
        assert context[marker_key] == run
        assert context["vm"]["creation_preflight"]["request_id"] == preflight["request_id"]
        assert queue == before_queue
        assert dict(retry)["canonical_request"] == before_request["canonical_request"]
        assert retry["request_digest"] == before_request["request_digest"]
        assert retry["admission_deadline"] == before_request["admission_deadline"]
        assert retry["provision_generation"] == before_request["provision_generation"]
        assert await conn.fetchval(
            "SELECT count(*) FROM worker_batch_attempts WHERE job_id=$1", job_id,
        ) == 0
    assert await settle_owned_fixture_ready(**arguments)
    assert await db.fetchval(
        "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1",
        row["request_id"],
    ) == retry["ready_at"]


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong", ["owner", "request", "generation", "marker", "vm", "pvc"])
async def test_fixture_guard_refuses_changed_source_before_real_release(
    db, monkeypatch, wrong,  # noqa: F811
):
    from orchestrator.operator_cli.vm_fixture_readiness import settle_owned_fixture_ready

    run = "srw-a1-refuse-" + uuid4().hex[:8]
    marker = "vm_retained_resume_acceptance_gate"
    row, owner, _ = await fixture_ready(db, monkeypatch, marker_key=marker, run=run)
    values = dict(
        db=db, job_id=str(row["job_id"]), owner_id=str(owner), run_id=run,
        marker_key=marker, request_id=str(row["request_id"]),
        generation=str(row["provision_generation"]),
        vm_uid=str(row["observed_vm_uid"]), pvc_uid=str(row["observed_pvc_uid"]),
    )
    key = {"owner": "owner_id", "request": "request_id", "generation": "generation",
           "marker": "run_id", "vm": "vm_uid", "pvc": "pvc_uid"}[wrong]
    values[key] = str(uuid4()) if key != "run_id" else "srw-a1-foreign-run"
    assert not await settle_owned_fixture_ready(**values)
    assert await db.fetchval(
        "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1",
        row["request_id"],
    ) is None


@pytest.mark.asyncio
async def test_b_wait_adapter_uses_real_release_then_existing_authority(db, monkeypatch):  # noqa: F811
    from orchestrator.operator_cli.vm_workspace_recovery_acceptance import LiveScenario

    run = "srw-b-fixture-" + uuid4().hex[:8]
    row, owner, _ = await fixture_ready(
        db, monkeypatch, marker_key="vm_workspace_recovery_acceptance_gate", run=run,
    )
    context = json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", row["job_id"],
    ))
    vm = context["vm"]
    vmi_uid = str(uuid4())
    vm["provisioning"] = _boot_phase(row["job_id"], vm, vmi_uid)
    context["_workspace_contract"] = {
        "version": 1, "assigned_backend": "vm", "requested_backend": "vm",
        "assignment_source": "fixture",
    }
    request = json.loads(row["canonical_request"])
    image = request["vm_image"]
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb,config_override=$3::jsonb WHERE id=$1",
        row["job_id"], json.dumps(context), json.dumps({
            "workspace": {"backend": "vm", "vm": {
                "image": image, "cpu_cores": 2,
                "memory": "2Gi", "disk_size": "12Gi",
            }},
        }),
    )
    live = {
        "ready": True, "provision_generation": str(row["provision_generation"]),
        "vm_uid": vm["vm_uid"], "rootdisk_pvc_uid": vm["rootdisk_pvc_uid"],
        "active_pod_uid": vm["active_pod_uid"],
        "provisioning": {"vmi_uid": vmi_uid},
    }
    scenario = object.__new__(LiveScenario)
    scenario.db, scenario.run_id = db, run
    scenario.namespace = vm.get("namespace") or "agent-vms"
    scenario.gate_user_id = owner
    scenario.fixture_request_id = str(row["request_id"])
    scenario.fixture_generation = str(row["provision_generation"])
    scenario.fixture_image = image
    scenario.protocol_fixture = True
    scenario.profiled_fixture = False
    scenario.provisioner = SimpleNamespace(query_status=AsyncMock(return_value=live))

    scenario.provisioner.query_status.return_value = {
        **live, "vm_uid": str(uuid4()),
    }
    assert await scenario._fixture_ready_identity(row["job_id"]) is None
    assert await db.fetchval(
        "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1",
        row["request_id"],
    ) is None
    scenario.provisioner.query_status.return_value = live
    identity = await scenario._fixture_ready_identity(row["job_id"])
    assert identity is not None
    assert await db.fetchval(
        "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1",
        row["request_id"],
    ) is not None
    assert await scenario._fixture_ready_identity(row["job_id"]) == identity
    scenario.provisioner.query_status.return_value = {
        **live, "vm_uid": str(uuid4()),
    }
    assert await scenario._fixture_ready_identity(row["job_id"]) is None


@pytest.mark.asyncio
async def test_a1_wait_adapter_uses_real_release_and_profile_proof(db, monkeypatch):  # noqa: F811
    from orchestrator.operator_cli.vm_retained_resume_fixture import probe_ready_fixture
    from shared.vm_network_profile import NETWORK_PROFILE

    run = "srw-a1-fixture-" + uuid4().hex[:8]
    row, owner, _ = await fixture_ready(
        db, monkeypatch, marker_key="vm_retained_resume_acceptance_gate",
        run=run, profiled=True,
    )
    context = json.loads(await db.fetchval(
        "SELECT context FROM jobs WHERE id=$1", row["job_id"],
    ))
    vm = context["vm"]
    vmi_uid = str(uuid4())
    vm["provisioning"] = _boot_phase(row["job_id"], vm, vmi_uid)
    vm["network_profile_evidence"] = {
        "profile": NETWORK_PROFILE,
        "provision_generation": str(row["provision_generation"]),
        "vm_uid": vm["vm_uid"], "pvc_uid": vm["rootdisk_pvc_uid"],
        "vmi_uid": vmi_uid, "launcher_uid": vm["active_pod_uid"],
        "guest_boot_id": str(uuid4()), "cloud_init_instance_id": "i-a1-fixture",
        "cloud_init_cached_instance_id": "i-a1-fixture",
        "network_file_sha256": "a" * 64, "name_only_dhcp": True,
    }
    request = json.loads(row["canonical_request"])
    image = request["vm_image"]
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb,config_override=$3::jsonb WHERE id=$1",
        row["job_id"], json.dumps(context), json.dumps({
            "workspace": {"backend": "vm", "vm": {"image": image}},
        }),
    )
    live = {
        "ready": True, "vm_uid": vm["vm_uid"],
        "rootdisk_pvc_uid": vm["rootdisk_pvc_uid"],
        "active_pod_uid": vm["active_pod_uid"],
        "provisioning": {"vmi_uid": vmi_uid},
    }
    provisioner = SimpleNamespace(query_status=AsyncMock(return_value=live))
    prepared = {
        "job_id": str(row["job_id"]), "owner_id": str(owner),
        "request_id": str(row["request_id"]),
        "provision_generation": str(row["provision_generation"]),
    }
    evidence = vm.pop("network_profile_evidence")
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
        row["job_id"], json.dumps(context),
    )
    assert await probe_ready_fixture(
        db, provisioner, run_id=run, vm_image=image, prepared=prepared,
    ) is None
    assert await db.fetchval(
        "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1",
        row["request_id"],
    ) is None
    vm["network_profile_evidence"] = evidence
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
        row["job_id"], json.dumps(context),
    )
    identity = await probe_ready_fixture(
        db, provisioner, run_id=run, vm_image=image, prepared=prepared,
    )
    assert identity is not None
    assert await db.fetchval(
        "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1",
        row["request_id"],
    ) is not None
    live["vm_uid"] = str(uuid4())
    assert await probe_ready_fixture(
        db, provisioner, run_id=run, vm_image=image, prepared=prepared,
    ) is None
