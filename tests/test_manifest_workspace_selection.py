"""The same Expert works with independently chosen, immutable workspaces."""

from copy import deepcopy
import json
from uuid import uuid4

from fastapi import HTTPException
import pytest

from orchestrator.schemas.job_create import JobCreate
from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.services.config_resolver import resolve_config
from orchestrator.services.manifest_execution_snapshot import (
    read_execution,
    srw_snapshot_config,
)
from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.manifest_store import ManifestStore
from orchestrator.services.manifest_workspace_migration import (
    migrate_workspace_preferences,
)
from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from shared.manifests import preview_documents
from shared.runtime.core.workspace_selection import migrate_expert_workspace_preference
from tests import test_manifest_native_full_schema as full_schema

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url


def binding(backend):
    return None if backend == "none" else {"template": {"inline": {"backend": backend}}}


def expert():
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Expert",
        "metadata": {"name": "independent-helper"},
        "spec": {
            "workspacePreference": {"backend": "sandbox"},
            "runtime": {
                "adapter": "srw/v1",
                "config": {
                    "config_name": "worker_base",
                    "config": {
                        "llm": {"model": "admitted-model"},
                        "workspace": {"backend": "vm", "git_versioning": False},
                    },
                },
            },
        },
    }


@pytest.mark.parametrize("role", ["worker", "session"])
@pytest.mark.parametrize("backend", ["none", "virtual", "sandbox", "vm"])
def test_expert_edits_never_select_or_size_execution_workspace(role, backend):
    row = {
        "config": {
            "workspace": {"backend": "vm", "vm": {"cpu_cores": 99}},
            "llm": {"model": "admitted-model"},
        }
    }
    original = deepcopy(row)
    blob = resolve_config(
        base_config_name=f"{role}_base",
        expert_type=role,
        expert_row=row,
        request_override={"workspace": {"backend": backend}},
    )
    assert blob["agent"]["workspace"]["backend"] == backend
    assert (blob["agent"]["workspace"].get("vm") or {}).get("cpu_cores") != 99
    assert row == original


@pytest.mark.parametrize(
    "role,backend", [("worker", "sandbox"), ("session", "virtual")]
)
def test_managed_role_default_is_independent_of_expert_backend(role, backend):
    blob = resolve_config(
        base_config_name=f"{role}_base",
        expert_type=role,
        expert_row={
            "config": {
                "workspace": {"backend": "vm"},
                "llm": {"model": "admitted-model"},
            }
        },
    )
    assert blob["agent"]["workspace"]["backend"] == backend


@pytest.mark.parametrize(
    "model,required", [(JobCreate, {"description": "Test"}), (ThreadCreateRequest, {})]
)
def test_api_workspace_null_is_distinct_from_omission(model, required):
    assert "workspace" not in model(**required).model_fields_set
    assert "workspace" in model(**required, workspace=None).model_fields_set
    with pytest.raises(ValueError):
        model(
            **required,
            workspace={
                "template": {
                    "inline": {"backend": "sandbox"},
                    "ref": {"name": "ambiguous"},
                }
            },
        )


def test_generic_settings_and_preference_do_not_select_a_workspace():
    doc = full_schema.assignment()
    doc["spec"]["execution"].pop("workspace")
    selected = doc["spec"]["execution"]["expert"]["inline"]
    selected["workspacePreference"] = {"backend": "vm"}
    selected["runtime"]["config"]["workspace"] = {"backend": "private", "vm": None}
    before = deepcopy(doc)
    resolved = preview_documents(
        [doc], default_scope={"kind": "Catalog", "name": "shared"}
    )["resolved"][0]
    assert resolved["spec"]["execution"]["workspace"] is None
    assert (
        resolved["spec"]["execution"]["expert"]["inline"]["runtime"]["config"]
        == selected["runtime"]["config"]
    )
    assert (
        migrate_expert_workspace_preference({**expert(), "spec": selected})["spec"]
        == selected
    )
    assert doc == before


@pytest.mark.asyncio
async def test_one_expert_two_referenced_templates_and_explicit_none(database, actor):
    service = ManifestResourceService(database)
    source = expert()
    await service.apply(json.dumps(source), actor, format="json")
    snapshots = []
    for backend in ("virtual", "sandbox", "none"):
        doc = full_schema.assignment(adapter="srw/v1", mode="Reported")
        doc["metadata"]["name"] = f"selected-{backend}"
        doc["spec"]["execution"]["expert"] = {
            "ref": {"name": source["metadata"]["name"]}
        }
        if backend != "none":
            template = {
                "apiVersion": "srw/v1alpha1",
                "kind": "WorkspaceTemplate",
                "metadata": {"name": backend},
                "spec": {"backend": backend},
            }
            await service.apply(json.dumps(template), actor, format="json")
            doc["spec"]["execution"]["workspace"] = {
                "template": {"ref": {"name": backend}}
            }
        else:
            doc["spec"]["execution"]["workspace"] = None
        _, _, _, _, job_id, snapshot = await full_schema.admit(database, actor, doc)
        blob, _ = srw_snapshot_config(snapshot)
        assert blob["agent"]["workspace"]["backend"] == backend
        snapshots.append((job_id, snapshot))
    row = await ManifestStore(database).by_name(
        "Expert", {"kind": "Account", "name": str(actor["id"])}, "independent-helper"
    )
    source["spec"]["runtime"]["config"]["config"]["workspace"]["backend"] = "none"
    await service.apply(
        json.dumps(source),
        actor,
        format="json",
        expected_versions={
            f"Expert/Account/{actor['id']}/independent-helper": row["resource_version"]
        },
    )
    for job_id, snapshot in snapshots:
        assert await read_execution(database, "Job", job_id) == snapshot


@pytest.mark.asyncio
async def test_session_template_revision_survives_source_edit(database, actor):
    service = ManifestResourceService(database)
    template = {
        "apiVersion": "srw/v1alpha1",
        "kind": "WorkspaceTemplate",
        "metadata": {"name": "selected"},
        "spec": {"backend": "virtual"},
    }
    await service.apply(json.dumps(template), actor, format="json")
    authored = {"template": {"ref": {"name": "selected"}}}
    config, receipt = await select_execution_workspace(
        database,
        actor,
        role="session",
        project_id=None,
        workspace=authored,
        supplied=True,
    )
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        initial_metadata={"config_override": {"workspace": config}},
        workspace_selection=receipt,
    )
    before = await read_execution(database, "Session", thread_id)
    assert before["document"]["spec"]["execution"]["workspace"] == authored
    row = await ManifestStore(database).by_name(
        "WorkspaceTemplate", {"kind": "Account", "name": str(actor["id"])}, "selected"
    )
    template["spec"]["backend"] = "sandbox"
    await service.apply(
        json.dumps(template),
        actor,
        format="json",
        expected_versions={
            f"WorkspaceTemplate/Account/{actor['id']}/selected": row["resource_version"]
        },
    )
    assert await read_execution(database, "Session", thread_id) == before
    newer, _ = await select_execution_workspace(
        database,
        actor,
        role="session",
        project_id=None,
        workspace=authored,
        supplied=True,
    )
    assert newer["backend"] == "sandbox"


@pytest.mark.asyncio
async def test_versioned_migration_preserves_history_and_rejects_stale_plan(
    database, actor
):
    service = ManifestResourceService(database)
    source = expert()
    source["spec"].pop("workspacePreference")
    await service.apply(json.dumps(source), actor, format="json")
    store = ManifestStore(database)
    scope = {"kind": "Account", "name": str(actor["id"])}
    old = await store.by_name("Expert", scope, "independent-helper")
    plan = await migrate_workspace_preferences(database)
    assert len(plan["changes"]) == 1
    with pytest.raises(HTTPException) as stale:
        await migrate_workspace_preferences(database, apply=True, plan_revision="stale")
    assert stale.value.status_code == 409
    assert await store.by_name("Expert", scope, "independent-helper") == old
    await migrate_workspace_preferences(
        database, apply=True, plan_revision=plan["planRevision"]
    )
    current = await store.by_name("Expert", scope, "independent-helper")
    assert current["resource_version"] == old["resource_version"] + 1
    assert current["document"]["spec"]["workspacePreference"] == {"backend": "vm"}
    assert current["document"]["spec"]["runtime"]["config"]["config"]["workspace"] == {
        "git_versioning": False
    }
    historical = await store.by_name(
        "Expert", scope, "independent-helper", revision=old["revision"]
    )
    assert historical["document"] == old["document"]
    assert (await migrate_workspace_preferences(database))["changes"] == []


@pytest.mark.asyncio
async def test_other_account_template_and_unsupported_recipes_fail_before_creation(
    database, actor
):
    with pytest.raises(HTTPException):
        await select_execution_workspace(
            database,
            {**actor, "is_admin": False},
            role="session",
            project_id=None,
            workspace={
                "template": {
                    "ref": {
                        "name": "private",
                        "scope": {"kind": "Account", "name": str(uuid4())},
                    }
                }
            },
            supplied=True,
        )
    with pytest.raises(HTTPException) as unsupported:
        await select_execution_workspace(
            database,
            actor,
            role="worker",
            project_id=None,
            workspace={
                "template": {
                    "inline": {
                        "backend": "sandbox",
                        "initialize": [{"command": ["setup"]}],
                    }
                }
            },
            supplied=True,
        )
    assert unsupported.value.status_code == 422
    assert await database.fetchval("SELECT count(*) FROM jobs") == 0
    assert await database.fetchval("SELECT count(*) FROM threads") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workspace,message",
    [
        (
            {
                "template": {
                    "inline": {
                        "backend": "virtual",
                        "environment": {"image": "registry.example/x:1"},
                    }
                }
            },
            "Virtual workspaces do not support OS images",
        ),
        (
            {"template": {"inline": {"backend": "none", "resources": {"cpu": 1}}}},
            "workspace must be null or a manifest template/instanceRef binding",
        ),
    ],
)
async def test_invalid_template_selection_is_a_client_error(
    database, actor, workspace, message
):
    """The resolver's and the binding schema's refusals are 422s, never 500s."""
    with pytest.raises(HTTPException) as refused:
        await select_execution_workspace(
            database,
            actor,
            role="worker",
            project_id=None,
            workspace=workspace,
            supplied=True,
        )
    assert refused.value.status_code == 422
    assert message in refused.value.detail


@pytest.mark.asyncio
async def test_project_workspace_default_precedence_and_activation_race(
    database, actor
):
    resources = ManifestResourceService(database)
    project = {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {"name": "workspace-team"},
        "spec": {
            "resources": {
                "experts": {"helper": {"inline": expert()["spec"]}},
                "workspaces": {"working": {"inline": {"backend": "virtual"}}},
            },
            "defaults": {"workspace": "working"},
        },
    }
    applied = await resources.apply(json.dumps(project), actor, format="json")
    entry = next(
        item for item in applied["resources"] if item["resource"]["kind"] == "Project"
    )
    row = await ManifestStore(database).by_id(entry["uid"])
    project_id = str(row["linked_id"])
    selected, receipt = await select_execution_workspace(
        database,
        actor,
        project_id=project_id,
        role="session",
        account_defaults={"workspace": {"backend": "sandbox"}},
    )
    assert selected["backend"] == "virtual"
    assert receipt["project_revision"] == row["revision"]
    from orchestrator.services.manifest_workspace_selection import (
        select_project_workspace_default,
    )

    override, background_receipt = await select_project_workspace_default(
        database, str(actor["id"]), project_id, {"autonomy": "full"}
    )
    assert override["workspace"]["backend"] == "virtual"
    background = await database.create_job(
        description="Project background assignment",
        user_id=str(actor["id"]),
        project_id=project_id,
        config_override=override,
        datasource_ids=[],
        workspace_selection=background_receipt,
    )
    captured = await read_execution(database, "Job", str(background["id"]))
    assert (
        captured["resolved"]["spec"]["execution"]["workspace"]
        == background_receipt["resolved"]
    )

    explicit, _ = await select_execution_workspace(
        database,
        actor,
        project_id=project_id,
        role="session",
        workspace=None,
        supplied=True,
    )
    assert explicit["backend"] == "none"
    project["spec"]["resources"]["workspaces"]["working"]["inline"]["backend"] = (
        "sandbox"
    )
    expected = {
        f"Project/Account/{actor['id']}/workspace-team": entry["resourceVersion"]
    }
    await resources.apply(
        json.dumps(project), actor, format="json", expected_versions=expected
    )
    with pytest.raises(HTTPException) as stale:
        await database.create_thread(
            user_id=str(actor["id"]),
            project_id=project_id,
            datasource_ids=[],
            authority_user_id=str(actor["id"]),
            authority_project_ids=[project_id],
            initial_metadata={"config_override": {"workspace": selected}},
            workspace_selection=receipt,
        )
    assert stale.value.status_code == 409
    assert await database.fetchval("SELECT count(*) FROM threads") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["virtual", "none"])
async def test_activity_does_not_invent_workspace_authority_or_block_job_deletion(
    database, actor, backend
):
    job = await database.create_job(
        description="Independent workspace heartbeat",
        user_id=str(actor["id"]),
        config_override={
            "workspace": {"backend": backend},
            "llm": {"model": "admitted-model"},
        },
        datasource_ids=[],
        status="completed",
    )
    changed = await database.merge_workspace_container_context(
        str(job["id"]),
        {"last_activity": "2026-09-10T00:00:00+00:00"},
        existing_only=True,
    )
    assert changed is False
    current = await database.get_job(str(job["id"]))
    assert "workspace_container" not in current["context"]
    assert await database.delete_job(
        str(job["id"]), deletion_actor_user_id=str(actor["id"])
    )
