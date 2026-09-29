"""select_execution_workspace consults the defaults chain (Slice A2b)."""

import json

import pytest
from fastapi import HTTPException

from shared.workspace_defaults import ProjectDefaults
from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from orchestrator.services.project_workspace_defaults import save_settings_defaults
from orchestrator.services.workspace_defaults_resolution import workspace_sources_record
from tests import test_manifest_native_full_schema as full_schema

postgres_url = full_schema.postgres_url
database = full_schema.database
actor = full_schema.actor


async def _project(db) -> str:
    return str(
        await db.fetchval("INSERT INTO projects(name) VALUES('Chain') RETURNING id")
    )


@pytest.fixture(autouse=True)
def no_chart_values(monkeypatch):
    monkeypatch.delenv("WORKSPACE_DEFAULTS", raising=False)
    monkeypatch.delenv("WORKSPACE_BUILTIN_TEMPLATES", raising=False)


@pytest.mark.asyncio
async def test_a_job_without_a_project_gets_the_installation_mode(database, actor):
    config, receipt = await select_execution_workspace(
        database, actor, project_id=None, role="worker"
    )
    assert config == {"backend": "sandbox"}
    assert receipt["sources"] == {"tier": "installation", "template": "builtin"}


@pytest.mark.asyncio
async def test_a_session_without_a_project_gets_virtual(database, actor):
    config, receipt = await select_execution_workspace(
        database, actor, project_id=None, role="session"
    )
    assert config == {"backend": "virtual"}
    assert receipt["sources"]["tier"] == "installation"


@pytest.mark.asyncio
async def test_the_installation_mode_comes_from_the_chart(database, actor, monkeypatch):
    monkeypatch.setenv("WORKSPACE_DEFAULTS", json.dumps({"jobs": "none"}))
    config, receipt = await select_execution_workspace(
        database, actor, project_id=None, role="worker"
    )
    assert config == {"backend": "none"}


@pytest.mark.asyncio
async def test_a_settings_row_supplies_the_mode_and_the_template(database, actor):
    project_id = await _project(database)
    inline = {
        "inline": {"backend": "sandbox", "resources": {"cpu": 1, "memory": "2Gi"}}
    }
    await save_settings_defaults(
        database,
        project_id,
        ProjectDefaults(jobs="container", container=inline),
        actor_id=str(actor["id"]),
    )
    config, receipt = await select_execution_workspace(
        database, actor, project_id=project_id, role="worker"
    )
    assert config["backend"] == "sandbox"
    assert config["sandbox"] == {"cpu": 1, "memory": "2Gi"}
    assert receipt["sources"] == {"tier": "project", "template": "project"}
    assert workspace_sources_record(receipt) == {
        "tier": "project",
        "template": "project",
        "template_name": None,
    }


@pytest.mark.asyncio
async def test_a_deleted_project_template_fails_admission_by_name(database, actor):
    project_id = await _project(database)
    gone = {"ref": {"name": "gone", "scope": {"kind": "Catalog", "name": "shared"}}}
    await save_settings_defaults(
        database,
        project_id,
        ProjectDefaults(jobs="container", container=gone),
        actor_id=str(actor["id"]),
    )
    with pytest.raises(HTTPException) as refused:
        await select_execution_workspace(
            database, actor, project_id=project_id, role="worker"
        )
    assert refused.value.status_code == 409
    assert (
        refused.value.detail
        == "This Project's container template 'gone' no longer exists."
    )


@pytest.mark.asyncio
async def test_invalid_chart_values_name_the_helm_key(database, actor, monkeypatch):
    monkeypatch.setenv("WORKSPACE_DEFAULTS", "not json")
    with pytest.raises(HTTPException) as refused:
        await select_execution_workspace(
            database, actor, project_id=None, role="worker"
        )
    assert refused.value.status_code == 503
    assert refused.value.detail.startswith(
        "The installation's workspace defaults (Helm workspace.defaults) are invalid:"
    )


@pytest.mark.asyncio
async def test_an_explicit_choice_records_explicit_sources(database, actor):
    config, receipt = await select_execution_workspace(
        database, actor, project_id=None, role="worker", workspace=None, supplied=True
    )
    assert config == {"backend": "none"}
    assert receipt["sources"] == {"tier": "explicit", "template": "explicit"}


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


@pytest.mark.asyncio
async def test_a_job_records_the_layers_that_supplied_its_workspace(database, actor):
    config, receipt = await select_execution_workspace(
        database, actor, project_id=None, role="worker"
    )
    job = await database.create_job(
        "chain",
        user_id=str(actor["id"]),
        config_override={"workspace": config},
        workspace_selection=receipt,
    )
    row = await database.get_job(str(job["id"]))
    assert _json(row["context"])["workspace_sources"] == {
        "tier": "installation",
        "template": "builtin",
        "template_name": None,
    }


@pytest.mark.asyncio
async def test_a_session_records_the_layers_that_supplied_its_workspace(
    database, actor
):
    config, receipt = await select_execution_workspace(
        database, actor, project_id=None, role="session"
    )
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        initial_metadata={"config_override": {"workspace": config}},
        workspace_selection=receipt,
    )
    thread = await database.get_thread(thread_id)
    assert _json(thread["metadata"])["workspace_sources"] == {
        "tier": "installation",
        "template": None,
        "template_name": None,
    }
