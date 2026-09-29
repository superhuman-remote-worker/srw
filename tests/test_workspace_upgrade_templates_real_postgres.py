"""Tier upgrades provision the chain's template, or a named one (Slice A2b)."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.services.builtin_workspace_templates import (
    reconcile_builtin_workspace_templates,
)
from orchestrator.services.sandbox_workspace_settings import resolve_sandbox_settings
from orchestrator.services.workspace_access import (
    WorkspaceOperationDependencies,
    provision_job_workspace,
)
from orchestrator.services.workspace_defaults_resolution import (
    render_upgrade_workspace,
    work_owner,
)
from tests import test_manifest_native_full_schema as full_schema

postgres_url = full_schema.postgres_url
database = full_schema.database
actor = full_schema.actor

SHARED = {"kind": "Catalog", "name": "shared"}
# Resolution defaults a template's pullPolicy, as for any work created from it.
PULL = {"pull_policy": "IfNotPresent"}


def template(name, spec):
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "WorkspaceTemplate",
        "metadata": {"name": name, "scope": SHARED},
        "spec": spec,
    }


@pytest.fixture(autouse=True)
def chart(monkeypatch):
    monkeypatch.delenv("WORKSPACE_DEFAULTS", raising=False)
    monkeypatch.delenv("WORKSPACE_BUILTIN_TEMPLATES", raising=False)
    monkeypatch.setenv("VM_MODE", "same-cluster")


async def _catalog(db):
    await reconcile_builtin_workspace_templates(
        db,
        [
            template("container-minimal", {"backend": "sandbox"}),
            template("vm-full", {"backend": "vm"}),
            template(
                "site",
                {"backend": "sandbox", "environment": {"image": "r.example/site:1"}},
            ),
        ],
    )


@pytest.mark.asyncio
async def test_a_session_upgrade_follows_the_chain(database, actor):
    await _catalog(database)
    mode, config, sources = await render_upgrade_workspace(
        database, actor, role="session", project_id=None, current_backend="virtual"
    )
    assert (mode, config["backend"]) == ("container", "sandbox")
    assert sources == {"tier": "upgrade", "template": "builtin", "template_name": None}


@pytest.mark.asyncio
async def test_a_named_template_decides_the_tier(database, actor):
    await _catalog(database)
    mode, config, sources = await render_upgrade_workspace(
        database,
        actor,
        role="session",
        project_id=None,
        current_backend="virtual",
        template_name="vm-full",
    )
    assert (mode, config["backend"]) == ("vm", "vm")
    assert sources == {
        "tier": "explicit",
        "template": "explicit",
        "template_name": "vm-full",
    }


@pytest.mark.asyncio
async def test_a_named_template_must_be_a_higher_tier(database, actor):
    await _catalog(database)
    with pytest.raises(HTTPException) as refused:
        await render_upgrade_workspace(
            database,
            actor,
            role="session",
            project_id=None,
            current_backend="sandbox",
            template_name="container-minimal",
        )
    assert (refused.value.status_code, refused.value.detail) == (
        400,
        "An upgrade must move to a higher tier than the current one.",
    )


@pytest.mark.asyncio
async def test_an_unknown_name_is_404(database, actor):
    with pytest.raises(HTTPException) as refused:
        await render_upgrade_workspace(
            database,
            actor,
            role="session",
            project_id=None,
            current_backend="virtual",
            template_name="nope",
        )
    assert (refused.value.status_code, refused.value.detail) == (
        404,
        "No template named 'nope' is available here.",
    )


@pytest.mark.asyncio
async def test_the_installation_template_renders_its_settings(
    database, actor, monkeypatch
):
    await _catalog(database)
    monkeypatch.setenv("WORKSPACE_DEFAULTS", json.dumps({"container": "site"}))
    mode, config, sources = await render_upgrade_workspace(
        database,
        actor,
        role="worker",
        project_id=None,
        current_backend="none",
        requested_backend="sandbox",
    )
    assert (mode, config) == (
        "container",
        {"backend": "sandbox", "sandbox": {"image": "r.example/site:1", **PULL}},
    )
    assert sources == {
        "tier": "upgrade",
        "template": "installation",
        "template_name": "site",
    }


@pytest.mark.asyncio
async def test_a_chain_template_of_another_tier_fails_closed(
    database, actor, monkeypatch
):
    await _catalog(database)
    monkeypatch.setenv("WORKSPACE_DEFAULTS", json.dumps({"container": "vm-full"}))
    with pytest.raises(HTTPException) as refused:
        await render_upgrade_workspace(
            database, actor, role="session", project_id=None, current_backend="virtual"
        )
    assert (refused.value.status_code, refused.value.detail) == (
        409,
        "The container template must be a container workspace.",
    )


@pytest.mark.asyncio
async def test_a_missing_chain_template_names_its_layer(database, actor, monkeypatch):
    monkeypatch.setenv("WORKSPACE_DEFAULTS", json.dumps({"vm": "gone"}))
    with pytest.raises(HTTPException) as refused:
        await render_upgrade_workspace(
            database,
            actor,
            role="session",
            project_id=None,
            current_backend="sandbox",
            requested_backend="vm",
        )
    assert (refused.value.status_code, refused.value.detail) == (
        409,
        "The installation's vm template 'gone' (Helm workspace.defaults.vm) "
        "no longer exists.",
    )


@pytest.mark.asyncio
async def test_a_missing_owner_fails_cleanly(database):
    for user_id in (None, str(uuid4())):
        with pytest.raises(HTTPException) as refused:
            await work_owner(database, user_id)
        assert (refused.value.status_code, refused.value.detail) == (
            409,
            "The execution owner is unavailable.",
        )


async def _lite_job(db, user_id):
    job = await db.create_job(
        description="lite job that upgrades in place",
        config_override={"workspace": {"backend": "virtual"}},
        requested_workspace_backend="virtual",
        workspace_assignment_source="request",
        user_id=user_id,
    )
    await db.execute("UPDATE jobs SET status='processing' WHERE id=$1", job["id"])
    return str(job["id"])


def _job_dependencies(db):
    return WorkspaceOperationDependencies(
        store=db,
        forge=None,
        container_provisioner=SimpleNamespace(
            is_available=True, in_cluster=True, create_workspace=AsyncMock()
        ),
        enforce_job_workspace_upgrade_grants=AsyncMock(),
    )


def _context(row):
    context = row["context"]
    return json.loads(context) if isinstance(context, str) else context


@pytest.mark.asyncio
async def test_a_job_container_upgrade_provisions_the_chain_template(
    database, actor, monkeypatch
):
    await _catalog(database)
    monkeypatch.setenv("WORKSPACE_DEFAULTS", json.dumps({"container": "site"}))
    job_id = await _lite_job(database, str(actor["id"]))

    result = await provision_job_workspace(
        job_id=job_id, body=None, dependencies=_job_dependencies(database)
    )

    assert result["status"] == "provisioning"
    workspace = _context(await database.get_job(job_id))["workspace_container"]
    assert workspace == {
        "status": "pending",
        "upgrade_config": {
            "image": "r.example/site:1",
            **PULL,
            "sources": {
                "tier": "upgrade",
                "template": "installation",
                "template_name": "site",
            },
        },
    }
    settings = await resolve_sandbox_settings(database, "job", job_id)
    assert (settings.image, settings.pull_policy) == (
        "r.example/site:1",
        "IfNotPresent",
    )


@pytest.mark.asyncio
async def test_a_job_container_upgrade_without_an_owner_changes_nothing(database):
    job_id = await _lite_job(database, None)
    dependencies = _job_dependencies(database)

    with pytest.raises(HTTPException) as refused:
        await provision_job_workspace(
            job_id=job_id, body=None, dependencies=dependencies
        )

    assert refused.value.status_code == 409
    assert "workspace_container" not in _context(await database.get_job(job_id))
    dependencies.container_provisioner.create_workspace.assert_not_called()
