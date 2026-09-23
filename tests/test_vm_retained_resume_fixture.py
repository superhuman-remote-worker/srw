"""Real store and preflight contract for the run-owned A1 fixture factory."""

from __future__ import annotations

import json
from uuid import UUID, uuid4

import pytest

from tests.test_non_pinned_workspace_lifecycle_real_postgres import (  # noqa: F401
    _schema_applied,
    db,
    pg_dsn,
)


IMAGE = "registry.example/a1-guest@sha256:" + "a" * 64
RUN = "srw-a1-owned-20260923a"
NAMESPACE = RUN
MODEL = "e2e-vm-a1-20260923a"


async def _model(db, run, model, *, api_key="a1-fixture-test-key-20260923a"):  # noqa: F811
    endpoint = await db.create_system_llm_endpoint(
        label=f"srw-a1-provider-{run}",
        base_url=f"http://srw-a1-provider.{run}.svc.cluster.local:8000/v1",
        api_key=api_key,
        key_prefix=None,
    )
    await db.create_model(
        provider_kind="endpoint", provider_ref=str(endpoint["id"]),
        model_id=model, display_label="A1 deterministic model",
        capabilities=["chat", "auxiliary"], family="e2e",
    )


@pytest.mark.asyncio
async def test_a1_fixture_uses_real_paused_job_snapshot_and_creation_preflight(
    db, monkeypatch,  # noqa: F811
):
    from orchestrator.operator_cli.vm_retained_resume_fixture import (
        prepare_fixture, probe_ready_fixture,
    )
    from orchestrator.services.manifest_execution_snapshot import (
        read_execution, srw_snapshot_config,
    )
    from orchestrator.services.vm_provisioner import VMProvisioner
    from shared.vm_network_profile import NETWORK_PROFILE

    for key, value in {
        "VM_MODE": "same-cluster", "VM_CREATION_RETRY_ENABLED": "true",
        "VM_NETWORK_PROFILE_ENABLED": "true",
        "VM_NETWORK_PROFILE_IMAGE_ALLOWLIST": IMAGE,
        "VM_RETAINED_RESUME_ACCEPTANCE_GATE_ENABLED": "true",
    }.items():
        monkeypatch.setenv(key, value)
    run = f"{RUN}-{uuid4().hex[:8]}"
    model = f"{MODEL}-{uuid4().hex[:8]}"
    await _model(db, run, model)
    provisioner = VMProvisioner()
    provisioner._db = db

    prepared = await prepare_fixture(
        db, provisioner, run_id=run, namespace=run,
        vm_image=IMAGE, model_id=model,
    )
    job_id, owner_id = UUID(prepared["job_id"]), UUID(prepared["owner_id"])
    job = await db.fetchrow(
        "SELECT user_id,status::text AS status,execution_lane,context,expert_id "
        "FROM jobs WHERE id=$1", job_id,
    )
    context = json.loads(job["context"])
    user = await db.fetchrow(
        "SELECT display_name,is_approved,is_admin,can_use_vm FROM users WHERE id=$1",
        owner_id,
    )
    queue = await db.fetchrow(
        "SELECT state,lease_token,leased_by FROM run_queue WHERE unit_id=$1",
        job_id,
    )
    snapshot = await read_execution(db, "Job", str(job_id))
    _, policy = srw_snapshot_config(snapshot)
    assert (job["user_id"], job["status"], job["execution_lane"]) == (
        owner_id, "paused", "stateless",
    )
    assert context["vm_retained_resume_acceptance_gate"] == run
    assert (user["display_name"], user["is_approved"], user["is_admin"],
            user["can_use_vm"]) == (f"A1 retained Resume gate {run}", True, False, True)
    assert (queue["state"], queue["lease_token"], queue["leased_by"]) == (
        "done", 0, None,
    )
    assert policy["workspace"]["vm"]["image"] == IMAGE
    assert policy["llm"]["model"] == model
    preflight = context["vm"]["creation_preflight"]
    assert preflight["request"]["vm_image"] == IMAGE
    assert preflight["request"]["network_profile"] == NETWORK_PROFILE
    assert context["_vm_creation_pending"] == prepared["request_id"]
    assert str(snapshot["id"]) == preflight["execution_id"]
    assert await db.fetchval(
        "SELECT count(*) FROM worker_batch_attempts WHERE job_id=$1", job_id,
    ) == 0
    assert await probe_ready_fixture(
        db, provisioner, run_id=run, vm_image=IMAGE, prepared=prepared,
    ) is None  # Queued preflight is not a first-boot Ready proof.

    from orchestrator.operator_cli.vm_retained_resume_fixture import _expert_config
    expert = await db.get_expert_by_id(str(job["expert_id"]))
    assert expert["config"] == _expert_config(model)

    replayed = await prepare_fixture(
        db, provisioner, run_id=run, namespace=run,
        vm_image=IMAGE, model_id=model,
    )
    assert replayed["job_id"] == prepared["job_id"]
    assert replayed["owner_id"] == prepared["owner_id"]
    assert replayed["request_id"] == prepared["request_id"]
    assert await db.fetchval(
        "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1", job_id,
    ) == 0  # Resolver has not yet consumed the queued preflight.


@pytest.mark.asyncio
async def test_a1_fixture_refuses_unowned_model_and_does_not_create_job(
    db, monkeypatch,  # noqa: F811
):
    from orchestrator.operator_cli.vm_retained_resume_fixture import (
        FixtureRefusal, prepare_fixture,
    )
    from orchestrator.services.vm_provisioner import VMProvisioner

    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_IMAGE_ALLOWLIST", IMAGE)
    monkeypatch.setenv("VM_RETAINED_RESUME_ACCEPTANCE_GATE_ENABLED", "true")
    provisioner = VMProvisioner()
    provisioner._db = db
    missing_run = "srw-a1-missing-model-20260923a"
    with pytest.raises(FixtureRefusal, match="model"):
        await prepare_fixture(
            db, provisioner, run_id=missing_run, namespace=missing_run,
            vm_image=IMAGE, model_id="e2e-vm-missing-20260923a",
        )
    assert await db.fetchval(
        "SELECT count(*) FROM jobs WHERE context->>'vm_retained_resume_acceptance_gate'=$1",
        missing_run,
    ) == 0


@pytest.mark.asyncio
async def test_a1_fixture_default_off_refuses_before_writing_owner_or_job(db, monkeypatch):  # noqa: F811
    from orchestrator.operator_cli.vm_retained_resume_fixture import (
        FixtureRefusal, prepare_fixture,
    )
    from orchestrator.services.vm_provisioner import VMProvisioner

    run = f"srw-a1-disabled-{uuid4().hex[:8]}"
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_IMAGE_ALLOWLIST", IMAGE)
    monkeypatch.delenv("VM_RETAINED_RESUME_ACCEPTANCE_GATE_ENABLED", raising=False)
    provisioner = VMProvisioner()
    provisioner._db = db
    with pytest.raises(FixtureRefusal, match="enabled"):
        await prepare_fixture(
            db, provisioner, run_id=run, namespace=run,
            vm_image=IMAGE, model_id="e2e-vm-disabled-20260923a",
        )
    assert await db.fetchval(
        "SELECT count(*) FROM jobs WHERE context->>'vm_retained_resume_acceptance_gate'=$1",
        run,
    ) == 0


@pytest.mark.asyncio
async def test_a1_fixture_refuses_provider_without_worker_auth_key(db, monkeypatch):  # noqa: F811
    from orchestrator.operator_cli.vm_retained_resume_fixture import (
        FixtureRefusal, prepare_fixture,
    )
    from orchestrator.services.vm_provisioner import VMProvisioner

    run = f"srw-a1-no-key-{uuid4().hex[:8]}"
    model = f"e2e-vm-no-key-{uuid4().hex[:8]}"
    for key, value in {
        "VM_MODE": "same-cluster", "VM_CREATION_RETRY_ENABLED": "true",
        "VM_NETWORK_PROFILE_ENABLED": "true",
        "VM_NETWORK_PROFILE_IMAGE_ALLOWLIST": IMAGE,
        "VM_RETAINED_RESUME_ACCEPTANCE_GATE_ENABLED": "true",
    }.items():
        monkeypatch.setenv(key, value)
    await _model(db, run, model, api_key=None)
    provisioner = VMProvisioner()
    provisioner._db = db
    with pytest.raises(FixtureRefusal, match="endpoint"):
        await prepare_fixture(
            db, provisioner, run_id=run, namespace=run,
            vm_image=IMAGE, model_id=model,
        )
    assert await db.fetchval(
        "SELECT count(*) FROM jobs WHERE context->>'vm_retained_resume_acceptance_gate'=$1",
        run,
    ) == 0
