"""Real transaction gates for exclusive retained VM workspace ownership."""

from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import HTTPException
import pytest

from orchestrator.services.manifest_execution_snapshot import read_execution
from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from orchestrator.services.retained_vm_workspaces import (
    provision_binding,
    reconcile_detached,
    record_created,
    record_detached,
    guest_attachment_is_current,
)
from orchestrator.services.vm_workspace_config import vm_provisioning_options
from tests import test_manifest_native_full_schema as full_schema
from tests.test_manifest_vm_workspaces import template, OPTIONS

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url


@pytest.fixture(autouse=True)
def hosting(monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")


def assignment(workspace=None):
    doc = full_schema.assignment(adapter="srw/v1", mode="Reported")
    recipe = {**template()["spec"], "retention": "Retain"}
    doc["metadata"]["name"] = "retained-" + uuid4().hex
    doc["spec"]["execution"]["workspace"] = workspace or {
        "template": {"inline": recipe}
    }
    return doc


@pytest.mark.asyncio
async def test_recovery_hold_blocks_retained_workspace_detach(monkeypatch):
    from orchestrator.services.vm_workspace_recovery_store import (
        VMWorkspaceRecoveryStore,
    )

    job_id = uuid4()
    generation = str(uuid4())
    pvc_uid = str(uuid4())
    db = MagicMock()
    db.fetch = AsyncMock(
        return_value=[
            {
                "id": job_id,
                "context": {
                    "vm": {
                        "workspace_storage": {"uid": str(uuid4())},
                        "provision_generation": generation,
                    }
                },
            }
        ]
    )
    db.managed_repository_workspace_process_zero_is_current = AsyncMock(
        return_value=True
    )
    provisioner = MagicMock()
    provisioner._probe_vm_teardown_identity = AsyncMock(
        return_value=SimpleNamespace(
            disposition="absent",
            rootdisk_identity_known=True,
            identity=SimpleNamespace(rootdisk_pvc_uid=pvc_uid),
        )
    )
    provisioner._storage_context = AsyncMock(return_value={"pvc_uid": pvc_uid})
    provisioner._record_retained_detach = AsyncMock()
    acquire_cleanup = AsyncMock(
        return_value=SimpleNamespace(
            allowed=False, reason="workspace_recovery_unresolved"
        )
    )
    monkeypatch.setattr(
        VMWorkspaceRecoveryStore, "acquire_cleanup_permit", acquire_cleanup
    )
    complete_cleanup = AsyncMock()
    monkeypatch.setattr(
        VMWorkspaceRecoveryStore, "complete_cleanup_permit", complete_cleanup
    )

    await reconcile_detached(db, provisioner)

    acquire_cleanup.assert_awaited_once()
    complete_cleanup.assert_not_awaited()
    provisioner._record_retained_detach.assert_not_awaited()


async def first(database, actor):
    *_, job_id, snapshot = await full_schema.admit(database, actor, assignment())
    binding = await provision_binding(database, job_id)
    return job_id, snapshot, binding


async def finished(database, job_id, binding):
    uid = str(uuid4())
    await record_created(database, job_id, binding, uid, namespace="test-vms")
    await database.execute(
        "UPDATE jobs SET status='completed' WHERE id=$1::uuid", job_id
    )
    await record_detached(database, job_id, binding)
    return uid


@pytest.mark.asyncio
async def test_second_job_reuses_only_a_released_instance_and_its_frozen_recipe(
    database, actor
):
    job_id, snapshot, binding = await first(database, actor)
    options = await vm_provisioning_options(
        database, "Job", await database.get_job(job_id)
    )
    assert options == {**OPTIONS, "workspace_storage": binding}
    ref = {"instanceRef": {"uid": binding["uid"]}}
    before = await database.fetchval("SELECT count(*) FROM jobs")
    with pytest.raises(HTTPException) as denied:
        await full_schema.admit(database, actor, assignment(ref))
    assert denied.value.status_code == 409
    assert await database.fetchval("SELECT count(*) FROM jobs") == before
    uid = await finished(database, job_id, binding)
    *_, next_job, next_snapshot = await full_schema.admit(
        database, actor, assignment(ref)
    )
    next_binding = await provision_binding(database, next_job)
    assert next_binding == {**binding, "generation": 2, "pvc_uid": uid}
    from orchestrator.services.retained_vm_workspaces import object_value

    row = await database.fetchrow(
        "SELECT backend_state FROM srw_workspace_instances WHERE id=$1::uuid",
        binding["uid"],
    )
    assert object_value(row["backend_state"])["namespace"] == "test-vms"
    assert next_snapshot["resolved"]["spec"]["execution"]["workspace"] == ref
    with pytest.raises(HTTPException, match="no longer owns"):
        await provision_binding(database, job_id)
    assert (await read_execution(database, "Job", job_id))["resolved"] == snapshot[
        "resolved"
    ]
    assert not await guest_attachment_is_current(database, job_id, binding)
    assert await guest_attachment_is_current(database, next_job, next_binding)


@pytest.mark.asyncio
async def test_job_completion_without_fenced_teardown_keeps_ownership(database, actor):
    job_id, snapshot, binding = await first(database, actor)
    await record_created(database, job_id, binding, str(uuid4()))
    # Final status alone is not a disk handoff. Only the provisioner's terminal
    # process-zero + controller-absence path may call record_detached.
    await database.execute(
        "UPDATE jobs SET status='completed' WHERE id=$1::uuid", job_id
    )
    with pytest.raises(HTTPException) as denied:
        await full_schema.admit(
            database, actor, assignment({"instanceRef": {"uid": binding["uid"]}})
        )
    assert denied.value.status_code == 409
    assert await guest_attachment_is_current(database, job_id, binding)


@pytest.mark.asyncio
@pytest.mark.parametrize("initialization_succeeded", [False, True])
async def test_failed_later_attachment_preserves_the_disks_initialization_status(
    database, actor, initialization_succeeded
):
    from orchestrator.services.manifest_workspaces import ManifestWorkspaceService

    job_id, _, binding = await first(database, actor)
    await database.merge_job_context(
        job_id,
        {
            "vm": {
                "initialization_receipt": {
                    "phase": "Succeeded" if initialization_succeeded else "Failed"
                }
            }
        },
    )
    pvc_uid = await finished(database, job_id, binding)
    service = ManifestWorkspaceService(
        database, None, namespace="test-vms", default_image="unused"
    )
    assert (await service.view(binding["uid"], actor))["initialized"] is (
        initialization_succeeded
    )

    ref = {"instanceRef": {"uid": binding["uid"]}}
    *_, next_job, _ = await full_schema.admit(database, actor, assignment(ref))
    next_binding = await provision_binding(database, next_job)
    assert next_binding["pvc_uid"] == pvc_uid
    # The later attachment failed before a guest initialization receipt. This
    # callback follows fenced teardown; it must not erase an earlier success
    # for this same disk or invent one for a disk that was never initialized.
    await database.execute(
        "UPDATE jobs SET status='failed' WHERE id=$1::uuid", next_job
    )
    await record_detached(database, next_job, next_binding)
    state = await service.view(binding["uid"], actor)
    assert state["status"] == "Detached" and state["executionId"] is None
    assert state["generation"] == 2
    assert state["initialized"] is initialization_succeeded

    *_, third_job, _ = await full_schema.admit(database, actor, assignment(ref))
    third_binding = await provision_binding(database, third_job)
    assert third_binding == {**next_binding, "generation": 3}


@pytest.mark.asyncio
async def test_running_job_cannot_be_detached_by_a_cleanup_acknowledgement(
    database, actor
):
    job_id, snapshot, binding = await first(database, actor)
    await record_created(database, job_id, binding, str(uuid4()))
    await record_detached(database, job_id, binding)
    assert (await provision_binding(database, job_id))["generation"] == 1


@pytest.mark.asyncio
async def test_account_boundary_applies_even_to_an_administrator(database, actor):
    job_id, _, binding = await first(database, actor)
    await finished(database, job_id, binding)
    other = dict(
        await database.fetchrow(
            "INSERT INTO users(display_name,is_approved,is_admin) VALUES('Other',TRUE,TRUE) RETURNING *"
        )
    )
    with pytest.raises(HTTPException) as denied:
        await full_schema.admit(
            database, other, assignment({"instanceRef": {"uid": binding["uid"]}})
        )
    assert denied.value.status_code == 403


@pytest.mark.asyncio
async def test_legacy_job_selection_reserves_in_the_same_transaction(database, actor):
    recipe = assignment()["spec"]["execution"]["workspace"]
    config, selection = await select_execution_workspace(
        database,
        actor,
        project_id=None,
        role="worker",
        workspace=recipe,
        supplied=True,
    )
    job = await database.create_job(
        description="Retained compatibility Job",
        user_id=str(actor["id"]),
        authority_user_id=str(actor["id"]),
        requested_workspace_backend="vm",
        config_override={"workspace": config},
        workspace_selection=selection,
    )
    binding = await provision_binding(database, str(job["id"]))
    assert binding["generation"] == 1
    snapshot = await read_execution(database, "Job", str(job["id"]))
    assert snapshot["document"]["spec"]["execution"]["workspace"] == recipe


@pytest.mark.asyncio
async def test_session_retention_rejects_atomically_without_creating_an_owner(
    database, actor
):
    recipe = assignment()["spec"]["execution"]["workspace"]
    config, selection = await select_execution_workspace(
        database,
        actor,
        project_id=None,
        role="persistent",
        workspace=recipe,
        supplied=True,
    )
    with pytest.raises(HTTPException) as denied:
        await database.create_thread(
            user_id=str(actor["id"]),
            authority_user_id=str(actor["id"]),
            initial_metadata={"config_override": {"workspace": config}},
            workspace_selection=selection,
        )
    assert denied.value.status_code == 422
    assert await database.fetchval("SELECT count(*) FROM threads") == 0
    assert await database.fetchval("SELECT count(*) FROM srw_workspace_instances") == 0


@pytest.mark.asyncio
async def test_stale_instance_selection_cannot_attach_a_later_generation(
    database, actor
):
    job_id, _, binding = await first(database, actor)
    await finished(database, job_id, binding)
    ref = {"instanceRef": {"uid": binding["uid"]}}
    config, selection = await select_execution_workspace(
        database,
        actor,
        project_id=None,
        role="worker",
        workspace=ref,
        supplied=True,
    )
    *_, next_job, _ = await full_schema.admit(database, actor, assignment(ref))
    next_binding = await provision_binding(database, next_job)
    await database.execute(
        "UPDATE jobs SET status='completed' WHERE id=$1::uuid", next_job
    )
    await record_detached(database, next_job, next_binding)
    before = await database.fetchval("SELECT count(*) FROM jobs")
    with pytest.raises(HTTPException) as denied:
        await database.create_job(
            description="Stale selection",
            user_id=str(actor["id"]),
            authority_user_id=str(actor["id"]),
            config_override={"workspace": config},
            requested_workspace_backend="vm",
            workspace_selection=deepcopy(selection),
        )
    assert denied.value.status_code == 409
    assert await database.fetchval("SELECT count(*) FROM jobs") == before


@pytest.mark.asyncio
async def test_concurrent_job_admissions_get_exactly_one_attachment(database, actor):
    import asyncio

    job_id, _, binding = await first(database, actor)
    await finished(database, job_id, binding)
    ref = {"instanceRef": {"uid": binding["uid"]}}
    results = await asyncio.gather(
        full_schema.admit(database, actor, assignment(ref)),
        full_schema.admit(database, actor, assignment(ref)),
        return_exceptions=True,
    )
    assert sum(isinstance(result, tuple) for result in results) == 1
    denied = next(result for result in results if isinstance(result, HTTPException))
    assert denied.status_code == 409
    assert (
        await database.fetchval("SELECT count(*) FROM srw_execution_workspace_bindings")
        == 2
    )
    row = await database.fetchrow(
        "SELECT generation,execution_id FROM srw_workspace_instances WHERE id=$1::uuid",
        binding["uid"],
    )
    assert row["generation"] == 2 and row["execution_id"] is not None


@pytest.mark.asyncio
async def test_lost_delete_response_keeps_a_durable_admission_fence(database, actor):
    from unittest.mock import AsyncMock
    from types import SimpleNamespace
    from orchestrator.services.manifest_workspaces import ManifestWorkspaceService

    job_id, _, binding = await first(database, actor)
    await finished(database, job_id, binding)
    provisioner = SimpleNamespace(
        release_workspace_storage=AsyncMock(side_effect=TimeoutError)
    )
    service = ManifestWorkspaceService(
        database,
        None,
        namespace="test",
        default_image="unused",
        vm_provisioner=provisioner,
    )
    with pytest.raises(TimeoutError):
        await service.delete(binding["uid"], actor, expected_generation=1)
    assert (await service.view(binding["uid"], actor))["status"] == "Deleting"
    with pytest.raises(HTTPException) as denied:
        await full_schema.admit(
            database, actor, assignment({"instanceRef": {"uid": binding["uid"]}})
        )
    assert denied.value.status_code == 409
    provisioner.release_workspace_storage.side_effect = None
    provisioner.release_workspace_storage.return_value = True
    assert (await service.delete(binding["uid"], actor, expected_generation=1))[
        "deleted"
    ]
    assert (await service.view(binding["uid"], actor))["status"] == "Released"


@pytest.mark.asyncio
async def test_generic_harness_cannot_reserve_a_retained_vm_instance(database, actor):
    from orchestrator.services.manifest_workspaces import ManifestWorkspaceService

    job_id, _, binding = await first(database, actor)
    await finished(database, job_id, binding)
    service = ManifestWorkspaceService(
        database, None, namespace="test", default_image="unused"
    )
    with pytest.raises(HTTPException) as denied:
        await service.validate({"instanceRef": {"uid": binding["uid"]}}, actor)
    assert denied.value.status_code == 422


@pytest.mark.asyncio
async def test_context_alone_cannot_authorize_a_signed_storage_reference(
    database, actor
):
    from orchestrator.services.vm_provisioner import VMProvisioner

    job_id, _, binding = await first(database, actor)
    victim = await database.create_job(
        description="Unbound Job",
        user_id=str(actor["id"]),
        authority_user_id=str(actor["id"]),
    )
    await database.execute(
        "UPDATE jobs SET context=jsonb_build_object('vm',jsonb_build_object('workspace_storage',$2::jsonb)) WHERE id=$1",
        victim["id"],
        json.dumps(binding),
    )
    provisioner = VMProvisioner()
    provisioner.connect(database)
    with pytest.raises(ValueError, match="reservation authority"):
        await provisioner._storage_context(str(victim["id"]))
    await provisioner.disconnect()


@pytest.mark.asyncio
async def test_transport_uses_the_reserved_pvc_pin_when_context_lags(database, actor):
    from orchestrator.services.vm_provisioner import VMProvisioner

    job_id, _, binding = await first(database, actor)
    pvc_uid = str(uuid4())
    await record_created(database, job_id, binding, pvc_uid, namespace="test-vms")
    await database.execute(
        "UPDATE jobs SET context=jsonb_build_object('vm',jsonb_build_object('workspace_storage',$2::jsonb)) WHERE id=$1::uuid",
        job_id,
        json.dumps(binding),
    )
    provisioner = VMProvisioner()
    provisioner.connect(database)
    assert await provisioner._storage_context(job_id) == {**binding, "pvc_uid": pvc_uid}
    await provisioner.disconnect()


@pytest.mark.asyncio
async def test_prepared_retained_instance_can_be_readmitted_with_preparation_disabled(
    database, actor, monkeypatch
):
    monkeypatch.setenv(
        "VM_LIFECYCLE_HMAC_SECRET", "test-preparation-only-long-auth-secret"
    )
    monkeypatch.setenv("VM_PREPARATION_ENABLED", "true")
    monkeypatch.setenv(
        "VM_PREPARATION_IMAGE", "registry.example/builder@sha256:" + "b" * 64
    )
    monkeypatch.setenv("VM_PREPARATION_REGISTRY_HOSTS", '["registry.example"]')
    manifest = assignment()
    manifest["spec"]["execution"]["workspace"]["template"]["inline"]["environment"][
        "prepare"
    ] = [{"command": ["true"]}]
    *_, job_id, _ = await full_schema.admit(database, actor, manifest)
    binding = await provision_binding(database, job_id)
    uid = await finished(database, job_id, binding)
    monkeypatch.setenv("VM_PREPARATION_ENABLED", "false")
    *_, next_job, _ = await full_schema.admit(
        database, actor, assignment({"instanceRef": {"uid": binding["uid"]}})
    )
    options = await vm_provisioning_options(
        database, "Job", await database.get_job(next_job)
    )
    assert "preparation" not in options
    assert options["workspace_storage"]["pvc_uid"] == uid
