"""Tier upgrades provision the chain's template, or a named one (Slice A2b)."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.services import thread_config_update as tcu
from orchestrator.services.builtin_workspace_templates import (
    reconcile_builtin_workspace_templates,
)
from orchestrator.services.project_workspace_defaults import save_settings_defaults
from orchestrator.services.sandbox_workspace_settings import resolve_sandbox_settings
from orchestrator.services.vm_provisioner import VMProvisioner
from orchestrator.services.workspace_access import (
    WorkspaceOperationDependencies,
    provision_job_workspace,
)
from orchestrator.services.workspace_defaults_resolution import (
    render_upgrade_workspace,
    work_owner,
)
from shared.workspace_defaults import ProjectDefaults
from tests import test_manifest_native_full_schema as full_schema

postgres_url = full_schema.postgres_url
database = full_schema.database
actor = full_schema.actor

SHARED = {"kind": "Catalog", "name": "shared"}
KEPT = "Upgrades can't use a template that keeps its workspace; start new work with this template."
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
            template(
                "vm-big",
                {
                    "backend": "vm",
                    "environment": {"image": "r.example/vm:2"},
                    "resources": {"cpu": 4, "memory": "8Gi"},
                },
            ),
            template("vm-kept", {"backend": "vm", "retention": "Retain"}),
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
async def test_a_named_template_that_keeps_its_workspace_is_refused(database, actor):
    await _catalog(database)
    with pytest.raises(HTTPException) as refused:
        await render_upgrade_workspace(
            database,
            actor,
            role="session",
            project_id=None,
            current_backend="virtual",
            template_name="vm-kept",
        )
    assert (refused.value.status_code, refused.value.detail) == (409, KEPT)


@pytest.mark.asyncio
async def test_a_project_vm_default_that_keeps_its_workspace_is_refused(
    database, actor
):
    project_id = str(
        await database.fetchval(
            "INSERT INTO projects(name) VALUES('Kept') RETURNING id"
        )
    )
    await save_settings_defaults(
        database,
        project_id,
        ProjectDefaults(vm={"inline": {"backend": "vm", "retention": "Retain"}}),
        actor_id=str(actor["id"]),
    )
    with pytest.raises(HTTPException) as refused:
        await render_upgrade_workspace(
            database,
            actor,
            role="worker",
            project_id=project_id,
            current_backend="sandbox",
            requested_backend="vm",
        )
    assert (refused.value.status_code, refused.value.detail) == (409, KEPT)


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
        "upgrade_config": {"image": "r.example/site:1", **PULL},
        "upgrade_sources": {
            "tier": "upgrade",
            "template": "installation",
            "template_name": "site",
        },
    }
    settings = await resolve_sandbox_settings(database, "job", job_id)
    assert (settings.image, settings.pull_policy) == (
        "r.example/site:1",
        "IfNotPresent",
    )


@pytest.mark.asyncio
async def test_an_ownerless_job_container_upgrade_stays_bare(
    database, actor, monkeypatch
):
    # Trusted internal callers and their subjobs create Jobs without an owner:
    # nobody to read templates as, so the upgrade provisions what it always did.
    await _catalog(database)
    monkeypatch.setenv("WORKSPACE_DEFAULTS", json.dumps({"container": "site"}))
    job_id = await _lite_job(database, None)
    dependencies = _job_dependencies(database)

    result = await provision_job_workspace(
        job_id=job_id, body=None, dependencies=dependencies
    )

    assert result["status"] == "provisioning"
    workspace = _context(await database.get_job(job_id))["workspace_container"]
    assert workspace == {
        "status": "pending",
        "upgrade_config": {},
        "upgrade_sources": None,
    }
    assert (await resolve_sandbox_settings(database, "job", job_id)).is_empty()


class CasProvisioner:
    """The VM provisioner up to its first effect: the real provision CAS."""

    is_available = True
    mode = "same-cluster"

    def __init__(self, db):
        self.db, self.options = db, []

    async def create_thread_vm(
        self,
        *,
        thread_id,
        agent_config,
        expected_runtime_generation,
        expected_agent_id,
        expected_attach_token,
        expected_vm_context,
        **options,
    ):
        self.options.append(options)
        context = VMProvisioner._fresh_provision_ctx()
        context["status"] = "provisioning"
        return await self.db.begin_pinned_thread_vm_provisioning(
            thread_id,
            expected_runtime_generation=expected_runtime_generation,
            expected_agent_id=expected_agent_id,
            expected_attach_token=expected_attach_token,
            expected_vm_context=expected_vm_context,
            provision_context=context,
        )


async def _pinned_session(db, user_id):
    thread_id = uuid4()
    await db.execute(
        "INSERT INTO threads (id,user_id,status,execution_lane,config_name,metadata) "
        "VALUES ($1,$2,'active','pinned','session_base',$3::jsonb)",
        thread_id,
        user_id,
        json.dumps({"config_override": {"workspace": {"backend": "virtual"}}}),
    )
    return str(thread_id)


def _session_dependencies(db):
    return tcu.ThreadConfigUpdateDependencies(
        store=db,
        vm_provisioner=CasProvisioner(db),
        container_provisioner=MagicMock(),
        recovery_store=MagicMock(),
        enforce_workspace_upgrade_grants=AsyncMock(),
        require_internal=AsyncMock(),
        require_thread_owner=AsyncMock(),
        thread_project_ids=AsyncMock(),
        authorize_thread_datasource_selection=AsyncMock(),
        build_datasource_tool_override=MagicMock(),
        datasource_selection_provenance=AsyncMock(),
        enforce_session_create_grants=AsyncMock(),
        inject_model_credentials=AsyncMock(),
        log_security_event=AsyncMock(),
    )


def _metadata(row):
    metadata = row["metadata"]
    return json.loads(metadata) if isinstance(metadata, str) else metadata


@pytest.mark.asyncio
async def test_a_session_vm_upgrade_records_its_template_through_the_provision_cas(
    database, actor, monkeypatch
):
    await _catalog(database)
    monkeypatch.setenv("WORKSPACE_DEFAULTS", json.dumps({"vm": "vm-big"}))
    thread_id = await _pinned_session(database, actor["id"])
    dependencies = _session_dependencies(database)

    result = await tcu.agent_upgrade_thread_to_vm(
        MagicMock(), thread_id, dependencies=dependencies
    )

    assert result["status"] == "provisioning"
    assert dependencies.vm_provisioner.options == [
        {"vm_image": "r.example/vm:2", "cpu_cores": 4, "memory": "8Gi"}
    ]
    vm = _metadata(await database.get_thread(thread_id))["vm"]
    assert vm["status"] == "provisioning"
    assert vm["upgrade_config"] == {
        "image": "r.example/vm:2",
        "cpu_cores": 4,
        "memory": "8Gi",
    }
    assert vm["upgrade_sources"] == {
        "tier": "upgrade",
        "template": "installation",
        "template_name": "vm-big",
    }


@pytest.mark.asyncio
async def test_the_provision_cas_refuses_the_context_read_before_the_record(
    database, actor
):
    thread_id = await _pinned_session(database, actor["id"])
    thread = await database.get_thread(thread_id)
    await database.merge_thread_vm_context(
        thread_id, {"upgrade_config": {}, "upgrade_sources": None}
    )
    context = VMProvisioner._fresh_provision_ctx()
    context["status"] = "provisioning"

    assert not await database.begin_pinned_thread_vm_provisioning(
        thread_id,
        expected_runtime_generation=str(thread["runtime_generation"]),
        expected_agent_id=None,
        expected_attach_token=None,
        expected_vm_context=None,
        provision_context=context,
    )


@pytest.mark.asyncio
async def test_a_refused_vm_option_leaves_the_session_untouched(
    database, actor, monkeypatch
):
    await _catalog(database)
    thread_id = await _pinned_session(database, actor["id"])
    monkeypatch.setattr(
        tcu,
        "vm_provisioning_options",
        AsyncMock(side_effect=HTTPException(422, "refused option")),
    )
    dependencies = _session_dependencies(database)

    with pytest.raises(HTTPException) as refused:
        await tcu.agent_upgrade_thread_to_vm(
            MagicMock(), thread_id, dependencies=dependencies
        )

    assert refused.value.status_code == 422
    assert "vm" not in _metadata(await database.get_thread(thread_id))
