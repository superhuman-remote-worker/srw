"""A selected development VM survives template edits and execution recovery."""

from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import HTTPException
import pytest

from orchestrator.services.manifest_execution_snapshot import (
    prepare_srw_session_patch,
    read_execution,
    srw_snapshot_config,
)
from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.manifest_store import ManifestStore
from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
    srw_workspace_config,
)
from orchestrator.services.vm_workspace_config import vm_provisioning_options
from shared.manifests import preview_documents
from tests import test_manifest_native_full_schema as full_schema

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url

IMAGE = "registry.example/dev-vm@sha256:" + "a" * 64
VM = {"image": IMAGE, "cpu_cores": 12, "memory": "24Gi", "disk_size": "120Gi"}
OPTIONS = {"vm_image": IMAGE, "cpu_cores": 12, "memory": "24Gi", "disk_size": "120Gi"}


def template():
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "WorkspaceTemplate",
        "metadata": {"name": "development"},
        "spec": {
            "backend": "vm",
            "resources": {"cpu": 12, "memory": "24Gi", "storage": "120Gi"},
            "environment": {"image": IMAGE},
        },
    }


def test_resolved_prebuilt_vm_preserves_allocation_without_changing_the_source():
    document = template()
    before = deepcopy(document)
    resolved = preview_documents(
        [document], default_scope={"kind": "Catalog", "name": "shared"}
    )["resolved"][0]
    assert srw_workspace_config({"template": {"inline": resolved["spec"]}}) == {
        "backend": "vm",
        "vm": VM,
    }
    assert document == before


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_initialization_uses_the_frozen_template_after_source_edits(
    database, actor, lane
):
    from shared.workspace_initialization import initialization_request

    service = ManifestResourceService(database)
    document = template()
    steps = [{"command": ["sh", "-c", "mkdir -p toolchain && touch toolchain/ready"]}]
    document["spec"]["initialize"] = steps
    await service.apply(json.dumps(document), actor, format="json")
    job = full_schema.assignment(adapter="srw/v1", mode="Reported")
    job["spec"]["execution"]["workspace"] = {
        "template": {"ref": {"name": "development"}}
    }
    _, _, _, _, work_id, snapshot = await full_schema.admit(database, actor, job)
    await database.execute(
        "UPDATE jobs SET execution_lane=$2 WHERE id=$1::uuid", work_id, lane
    )
    row = await ManifestStore(database).by_name(
        "WorkspaceTemplate",
        {"kind": "Account", "name": str(actor["id"])},
        "development",
    )
    document["spec"]["initialize"] = [{"command": ["false"]}]
    await service.apply(
        json.dumps(document),
        actor,
        format="json",
        expected_versions={
            f"WorkspaceTemplate/Account/{actor['id']}/development": row[
                "resource_version"
            ],
        },
    )
    options = await vm_provisioning_options(
        database, "Job", await database.get_job(work_id)
    )
    assert options == {**OPTIONS, "initialization": initialization_request(steps)}
    reread = await read_execution(database, "Job", work_id)
    assert reread["resolved"] == snapshot["resolved"]


@pytest.mark.parametrize(
    "changes",
    [
        {"resources": {"cpu": 1.5}},
        {"resources": {"cpu": True}},
        {"environment": {"image": "bad\nmanifest: injected"}},
        {"environment": {"image": IMAGE, "prepare": []}},
        {"environment": {"image": IMAGE, "pullPolicy": "Always"}},
        {"environment": {"image": IMAGE, "pullPolicy": "Never"}},
        {"environment": {"image": IMAGE, "cache": "Rebuild"}},
    ],
)
def test_unsupported_vm_recipes_are_refused_instead_of_partially_applied(changes):
    spec = {**template()["spec"], **changes}
    with pytest.raises(HTTPException) as denied:
        srw_workspace_config({"template": {"inline": spec}})
    assert denied.value.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_job_dispatch_keeps_selected_image_and_size_after_template_edit(
    database, actor, lane
):
    service = ManifestResourceService(database)
    document = template()
    await service.apply(json.dumps(document), actor, format="json")
    job = full_schema.assignment(adapter="srw/v1", mode="Reported")
    job["spec"]["execution"]["workspace"] = {
        "template": {"ref": {"name": "development"}}
    }
    _, _, _, _, work_id, snapshot = await full_schema.admit(database, actor, job)
    _, policy = srw_snapshot_config(snapshot)
    assert policy["workspace"]["vm"] == VM
    await database.execute(
        "UPDATE jobs SET execution_lane=$2 WHERE id=$1::uuid", work_id, lane
    )
    row = await ManifestStore(database).by_name(
        "WorkspaceTemplate",
        {"kind": "Account", "name": str(actor["id"])},
        "development",
    )
    document["spec"]["environment"]["image"] = "registry.example/other:v2"
    document["spec"]["resources"]["cpu"] = 2
    await service.apply(
        json.dumps(document),
        actor,
        format="json",
        expected_versions={
            f"WorkspaceTemplate/Account/{actor['id']}/development": row[
                "resource_version"
            ]
        },
    )
    candidates = await (
        database.get_dispatchable_jobs()
        if lane == "pinned"
        else database.get_admittable_stateless_jobs()
    )
    candidate = next(row for row in candidates if str(row["id"]) == work_id)
    assert candidate["execution_harness_adapter"] == "srw/v1"
    assert (
        await vm_provisioning_options(
            database,
            "Job",
            candidate,
            fallback={"workspace": {"vm": {"image": "mutable-projection:v2"}}},
        )
        == OPTIONS
    )
    assert await read_execution(database, "Job", work_id) == snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize("initialize", [False, True])
async def test_session_snapshot_keeps_vm_allocation_across_unrelated_patch(
    database, actor, initialize
):
    from shared.workspace_initialization import initialization_request

    spec = template()["spec"]
    expected_vm, expected_options = deepcopy(VM), deepcopy(OPTIONS)
    if initialize:
        spec["initialize"] = [{"command": ["mkdir", "-p", "project"]}]
        request = initialization_request(spec["initialize"])
        expected_vm["initialization"] = request
        expected_options["initialization"] = request
    workspace, receipt = await select_execution_workspace(
        database,
        actor,
        role="session",
        project_id=None,
        supplied=True,
        workspace={"template": {"inline": spec}},
    )
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        initial_metadata={"config_override": {"workspace": workspace}},
        workspace_selection=receipt,
    )
    current = await read_execution(database, "Session", thread_id)
    thread = await database.get_thread(thread_id)
    metadata = json.loads(thread["metadata"])
    prepared, _ = await prepare_srw_session_patch(
        database,
        current,
        thread,
        metadata,
        [],
        {"llm": {"temperature": 0.2}},
    )
    _, policy = srw_snapshot_config(prepared)
    assert policy["workspace"]["vm"] == expected_vm
    assert prepared["resolved"]["spec"]["execution"]["workspace"] == receipt["resolved"]
    assert (
        await vm_provisioning_options(database, "Session", thread) == expected_options
    )
    with pytest.raises(HTTPException) as denied:
        await prepare_srw_session_patch(
            database,
            current,
            thread,
            metadata,
            [],
            {"workspace": {"vm": {"image": "registry.example/other:v2"}}},
        )
    assert denied.value.status_code == 422
    assert await read_execution(database, "Session", thread_id) == current


@pytest.mark.asyncio
async def test_project_default_selects_vm_and_explicit_none_suppresses_it(
    database, actor
):
    service = ManifestResourceService(database)
    project = {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {"name": "development-team"},
        "spec": {
            "resources": {
                "workspaces": {"development": {"inline": template()["spec"]}}
            },
            "defaults": {"workspace": "development"},
        },
    }
    applied = await service.apply(json.dumps(project), actor, format="json")
    entry = next(x for x in applied["resources"] if x["resource"]["kind"] == "Project")
    row = await ManifestStore(database).by_id(entry["uid"])
    project_id = str(row["linked_id"])
    selected, receipt = await select_execution_workspace(
        database,
        actor,
        role="worker",
        project_id=project_id,
    )
    assert selected == {"backend": "vm", "vm": VM}
    assert receipt["project_revision"] == row["revision"]
    selected, _ = await select_execution_workspace(
        database,
        actor,
        role="worker",
        project_id=project_id,
        supplied=True,
        workspace=None,
    )
    assert selected == {"backend": "none"}


@pytest.mark.asyncio
async def test_marked_execution_never_falls_back_when_snapshot_is_missing():
    db = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    with pytest.raises(HTTPException) as denied:
        await vm_provisioning_options(
            db,
            "Job",
            {
                "id": "11111111-1111-4111-8111-111111111111",
                "execution_harness_adapter": "srw/v1",
            },
            fallback={"workspace": {"vm": VM}},
        )
    assert denied.value.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("admin", [False, True])
async def test_manifest_vm_admission_obeys_the_existing_operator_gate(
    database, actor, monkeypatch, admin
):
    user = {**actor, "is_admin": admin}
    monkeypatch.setattr(database, "user_can_use_vm", AsyncMock(return_value=False))
    monkeypatch.setattr(
        database,
        "get_system_setting",
        AsyncMock(return_value={"value": {"enabled": not admin}}),
    )
    job = full_schema.assignment(adapter="srw/v1", mode="Reported")
    job["spec"]["execution"]["workspace"] = {"template": {"inline": template()["spec"]}}
    with pytest.raises(HTTPException) as denied:
        await full_schema.admit(database, user, job)
    assert denied.value.status_code == 403
    assert await database.fetchval("SELECT count(*) FROM jobs") == 0


@pytest.fixture
def preparation_hosting(monkeypatch):
    for key, value in {
        "VM_MODE": "same-cluster",
        "VM_PERSISTENT_ROOTDISK": "true",
        "VM_LIFECYCLE_HMAC_SECRET": "test-only-preparation-secret-32-bytes-long",
        "VM_PREPARATION_ENABLED": "true",
        "VM_PREPARATION_IMAGE": "registry.example/builder@sha256:" + "b" * 64,
        "VM_PREPARATION_REGISTRY_HOSTS": '["registry.example"]',
    }.items():
        monkeypatch.setenv(key, value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pull,cache", [("IfNotPresent", "Reuse"), ("Always", "Rebuild"), ("Never", "Reuse")]
)
async def test_preparation_is_execution_owned_and_frozen_after_template_edits(
    database, actor, preparation_hosting, pull, cache
):
    document = template()
    environment = {
        **document["spec"]["environment"],
        "pullPolicy": pull,
        "cache": cache,
        "prepare": [{"command": ["sh", "-c", "install-project-tools"]}],
    }
    document["spec"]["environment"] = deepcopy(environment)
    service = ManifestResourceService(database)
    await service.apply(json.dumps(document), actor, format="json")
    job = full_schema.assignment(adapter="srw/v1", mode="Reported")
    job["spec"]["execution"]["workspace"] = {
        "template": {"ref": {"name": "development"}}
    }
    _, _, _, _, job_id, _ = await full_schema.admit(database, actor, job)
    before = await vm_provisioning_options(
        database, "Job", await database.get_job(job_id)
    )
    assert before["preparation"]["scope"] == {
        "kind": "Account",
        "uid": str(actor["id"]),
    }
    assert before["preparation"]["allocationId"] == job_id
    assert before["preparation"]["steps"] == environment["prepare"]
    stored = await ManifestStore(database).by_name(
        "WorkspaceTemplate",
        {"kind": "Account", "name": str(actor["id"])},
        "development",
    )
    document["spec"]["environment"]["prepare"] = [{"command": ["false"]}]
    await service.apply(
        json.dumps(document),
        actor,
        format="json",
        expected_versions={
            f"WorkspaceTemplate/Account/{actor['id']}/development": stored[
                "resource_version"
            ]
        },
    )
    assert (
        await vm_provisioning_options(database, "Job", await database.get_job(job_id))
        == before
    )


@pytest.mark.asyncio
async def test_session_preparation_is_bound_to_runtime_generation(
    database, actor, preparation_hosting
):
    spec = template()["spec"]
    spec["environment"]["prepare"] = [{"command": ["touch", "/opt/ready"]}]
    workspace, receipt = await select_execution_workspace(
        database,
        actor,
        role="session",
        project_id=None,
        supplied=True,
        workspace={"template": {"inline": spec}},
    )
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        initial_metadata={"config_override": {"workspace": workspace}},
        workspace_selection=receipt,
    )
    thread = await database.get_thread(thread_id)
    options = await vm_provisioning_options(database, "Session", thread)
    assert options["preparation"]["runtimeGeneration"] == str(
        thread["runtime_generation"]
    )
    assert options["preparation"]["ownerKind"] == "session"
    assert options["preparation"]["allocationId"] == thread_id


def test_prepared_workspace_default_is_large_enough_to_clone(preparation_hosting):
    spec = {"backend": "vm", "environment": {"image": IMAGE, "prepare": []}}
    assert (
        srw_workspace_config({"template": {"inline": spec}})["vm"]["disk_size"]
        == "30Gi"
    )
    spec["resources"] = {"storage": "20Gi"}
    with pytest.raises(HTTPException, match="smaller"):
        srw_workspace_config({"template": {"inline": spec}})


def test_retained_disk_reference_does_not_require_rebuilding_a_template(monkeypatch):
    monkeypatch.setenv("VM_PREPARATION_ENABLED", "false")
    spec = template()["spec"]
    spec.update(retention="Retain")
    spec["environment"]["prepare"] = [{"command": ["false"]}]
    selected = srw_workspace_config(
        {"instanceRef": {"uid": "11111111-1111-4111-8111-111111111111"}},
        instance_recipe=spec,
    )
    assert "preparation" not in selected["vm"]


@pytest.mark.asyncio
async def test_legacy_override_cannot_supply_an_unadmitted_preparation():
    with pytest.raises(HTTPException, match="admitted manifest"):
        await vm_provisioning_options(
            None,
            "Job",
            {"id": "11111111-1111-4111-8111-111111111111"},
            fallback={
                "workspace": {"vm": {"preparation": {"image": IMAGE, "prepare": []}}}
            },
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_response", [False, True])
async def test_session_cache_stage_does_not_create_vm_until_ready(
    database, actor, preparation_hosting, monkeypatch, lost_response
):
    import httpx
    from orchestrator.services.vm_provisioner import VMProvisioner

    monkeypatch.setenv("VM_CONTROLLER_URL", "http://controller.invalid")
    spec = template()["spec"]
    spec["environment"]["prepare"] = [{"command": ["true"]}]
    workspace, receipt = await select_execution_workspace(
        database,
        actor,
        role="session",
        project_id=None,
        supplied=True,
        workspace={"template": {"inline": spec}},
    )
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        initial_metadata={"config_override": {"workspace": workspace}},
        workspace_selection=receipt,
    )
    thread = await database.get_thread(thread_id)
    options = await vm_provisioning_options(database, "Session", thread)
    provisioner = VMProvisioner()
    provisioner.connect(database)
    responses = [
        httpx.ReadTimeout("lost")
        if lost_response
        else {
            "source": None,
            "waiting": {
                "status": "waiting_preparation",
                "preparation": {"phase": "Building"},
            },
        },
        {"source": {"name": "verified-cache"}, "waiting": None},
    ]
    provisioner.preparation_operation = AsyncMock(side_effect=responses)
    provisioner._create_http = AsyncMock(return_value={"status": "created"})
    try:
        result = await provisioner.create_thread_vm(
            thread_id,
            **options,
            expected_runtime_generation=str(thread["runtime_generation"]),
            expected_agent_id=None,
            expected_attach_token=None,
            expected_vm_context=None,
        )
        assert result["status"] == "waiting_preparation"
        current = await database.get_thread(thread_id)
        meta = (
            json.loads(current["metadata"])
            if isinstance(current["metadata"], str)
            else current["metadata"]
        )
        assert not meta.get("vm")
        stage = meta["workspace_preparation"]
        provisioner._create_http.assert_not_awaited()
        await provisioner.poll_thread_vm(thread_id, stage["provision_generation"])
        current = await database.get_thread(thread_id)
        meta = (
            json.loads(current["metadata"])
            if isinstance(current["metadata"], str)
            else current["metadata"]
        )
        assert "workspace_preparation" not in meta
        assert (
            meta["vm"]["preparation_wait_started_at"]
            == stage["preparation_wait_started_at"]
        )
        assert meta["vm"]["provision_generation"] != stage["provision_generation"]
        provisioner._create_http.assert_awaited_once()
    finally:
        await provisioner.disconnect()
