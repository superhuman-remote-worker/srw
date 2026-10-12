"""Generation admission accounting shares the actual phase/Ready transaction."""

import asyncio
from uuid import uuid4
from datetime import datetime, timezone

import pytest

from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from tests.test_vm_provisioning_phase_store import (
    db as _db_fixture,
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    seed,
    status,
    vm_context,
)  # noqa: F401


db = _db_fixture


async def fresh(db, **changes):
    return await seed(
        db,
        vm_fields={
            "admission_accounting_version": 1,
            "provision_admission": None,
            "provision_attempts": 2,
            **changes,
        },
    )


@pytest.mark.asyncio
async def test_two_observers_count_one_admission_and_no_worker_attempt(db):
    job, generation = await fresh(db)
    store = VMProvisioningPhaseStore(db)
    tokens = await asyncio.gather(
        store.capture(job, generation), store.capture(job, generation)
    )
    observed = status(job, generation)
    results = await asyncio.gather(
        *(store.apply_status(token, observed) for token in tokens)
    )
    assert sorted(results) == ["observed", "stale"]
    vm = await vm_context(db, job)
    assert vm["provision_attempts"] == 3
    assert vm["provision_admission"]["vm_uid"] == observed["vm_uid"]
    assert (
        await store.apply_status(await store.capture(job, generation), observed)
        == "observed"
    )
    assert (await vm_context(db, job))["provision_attempts"] == 3
    assert (
        await db.fetchval("SELECT count(*) FROM run_queue WHERE unit_id=$1::uuid", job)
        == 0
    )


@pytest.mark.asyncio
async def test_missing_phase_does_not_count_and_historical_budget_is_preserved(db):
    store = VMProvisioningPhaseStore(db)
    for fields in ({"admission_accounting_version": 1}, {}):
        job, generation = await seed(db, vm_fields={"provision_attempts": 2, **fields})
        observed = status(job, generation)
        if fields:
            observed.pop("provisioning")
        await store.apply_status(await store.capture(job, generation), observed)
        assert (await vm_context(db, job))["provision_attempts"] == 2


async def ready_fixture(db):
    job, generation = await fresh(db)
    store = VMProvisioningPhaseStore(db)
    observed = status(job, generation)
    await store.apply_status(await store.capture(job, generation), observed)
    registration, launcher = str(uuid4()), str(uuid4())
    await db.merge_vm_context_if_provision_generation(
        job,
        generation,
        {
            "ssh_registration_id": registration,
            "active_pod_uid": launcher,
            "ssh_host": "10.42.0.2",
            "pod_ip": "10.42.0.2",
            "ssh_port": 22,
            "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
        },
    )
    updates = {
        "status": "ready",
        "ssh_registration_id": registration,
        "active_pod_uid": launcher,
        "ssh_host": "10.42.0.2",
        "pod_ip": "10.42.0.2",
        "ssh_port": 22,
        "ssh_ready_source": "provisioner_probe",
        "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
        "ssh_verified_at": datetime.now(timezone.utc).isoformat(),
    }
    return store, job, generation, observed, registration, updates


@pytest.mark.asyncio
async def test_verified_ready_resets_counter_atomically_but_late_query_cannot_recount(
    db,
):
    store, job, generation, observed, registration, updates = await ready_fixture(db)
    assert await store.publish_ready(
        job, generation, registration, observed["vm_uid"], updates
    )
    vm = await vm_context(db, job)
    assert vm["status"] == "ready" and vm["provision_attempts"] == 0
    marker = vm["provision_admission"]
    await store.apply_status(await store.capture(job, generation), observed)
    vm = await vm_context(db, job)
    assert vm["provision_attempts"] == 0 and vm["provision_admission"] == marker


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    ["registration", "vm_uid", "generation", "cleanup", "attention", "launcher"],
)
async def test_stale_ready_does_not_erase_current_budget(db, change):
    store, job, generation, observed, registration, updates = await ready_fixture(db)
    if change == "generation":
        generation = str(uuid4())
    elif change == "vm_uid":
        observed["vm_uid"] = str(uuid4())
    else:
        field, value = {
            "registration": ("ssh_registration_id", str(uuid4())),
            "cleanup": ("retirement_cleanup_pending", True),
            "attention": (
                "provisioning_attention_reason",
                "vm_phase_identity_conflict",
            ),
            "launcher": ("active_pod_uid", str(uuid4())),
        }[change]
        await db.merge_vm_context(job, {field: value})
    assert not await store.publish_ready(
        job, generation, registration, observed["vm_uid"], updates
    )
    assert (await vm_context(db, job))["provision_attempts"] == 3


@pytest.mark.asyncio
async def test_a1_owns_count_before_adoption_after_adoption_and_after_ready(
    db, monkeypatch
):
    from tests.test_vm_creation_effects_real_postgres import observed_creation
    from orchestrator.services.vm_creation_readiness import VMCreationReadinessStore
    from datetime import datetime, timezone

    retry, row, carrier, observations = await observed_creation(db, monkeypatch)
    job, generation = str(row["job_id"]), str(row["provision_generation"])
    await db.merge_vm_context(
        job,
        {
            "status": "starting",
            "admission_accounting_version": 1,
            "provision_admission": None,
            "provision_attempts": 0,
        },
    )
    observed = status(
        job,
        generation,
        vm_uid=observations["vm"]["object"]["metadata"]["uid"],
        rootdisk_pvc_uid=observations["rootdisk"]["pvc"]["metadata"]["uid"],
        namespace="agent-vms",
    )
    observed["namespace"] = "agent-vms"
    phase = VMProvisioningPhaseStore(db)
    assert (
        await phase.apply_status(await phase.capture(job, generation), observed)
        == "observed"
    )
    assert (await vm_context(db, job))["provision_attempts"] == 0
    await retry.settle_adopted(
        request_id=str(row["request_id"]), carrier=carrier, observations=observations
    )
    assert (await vm_context(db, job))["provision_attempts"] == 1
    assert (
        await phase.apply_status(await phase.capture(job, generation), observed)
        == "observed"
    )
    assert (await vm_context(db, job))["provision_attempts"] == 1
    registration, launcher = uuid4().hex, str(uuid4())
    updates = dict(
        status="ready",
        ssh_registration_id=registration,
        active_pod_uid=launcher,
        ssh_host="10.42.1.8",
        pod_ip="10.42.1.8",
        ssh_port=22,
        ssh_host_key_fingerprint="SHA256:" + "A" * 43,
        ssh_ready_source="provisioner_probe",
        ssh_verified_at=datetime.now(timezone.utc).isoformat(),
    )
    await db.merge_vm_context(job, {k: v for k, v in updates.items() if k != "status"})
    assert await phase.publish_ready(
        job, generation, registration, observed["vm_uid"], updates
    )
    assert (await vm_context(db, job))["provision_attempts"] == 1
    assert await VMCreationReadinessStore(db).release(request_id=str(row["request_id"]))
    assert (
        await phase.apply_status(await phase.capture(job, generation), observed)
        == "observed"
    )
    assert (await vm_context(db, job))["provision_attempts"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [True, "2", -1, None])
async def test_invalid_counter_cannot_publish_new_phase_identity(db, invalid):
    job, generation = await fresh(db, provision_attempts=invalid)
    store = VMProvisioningPhaseStore(db)
    assert (
        await store.apply_status(
            await store.capture(job, generation), status(job, generation)
        )
        == "conflict"
    )
    vm = await vm_context(db, job)
    assert vm["provision_attempts"] == invalid
    assert "vm_uid" not in vm and "provisioning" not in vm


@pytest.mark.asyncio
async def test_first_phase_query_after_ready_never_adds_failed_attempt(db):
    job, generation = await fresh(db, status="ready", provision_attempts=0)
    store = VMProvisioningPhaseStore(db)
    observed = status(job, generation)
    assert (
        await store.apply_status(await store.capture(job, generation), observed)
        == "observed"
    )
    vm = await vm_context(db, job)
    assert vm["provision_attempts"] == 0
    assert vm["provision_admission"]["vm_uid"] == observed["vm_uid"]
