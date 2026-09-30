"""Resource concurrency, authorization and activation on real PostgreSQL."""

import asyncio
from copy import deepcopy
import json
from pathlib import Path
import re
from uuid import UUID, uuid4
from unittest.mock import AsyncMock

import asyncpg
from fastapi import HTTPException
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.manifest_store import resource_key, ManifestStore
from orchestrator.services.project_workspace_defaults import read_project_defaults
from shared.workspace_defaults import ProjectDefaults


@pytest.fixture(scope="module")
def pg_url():
    with PostgresContainer("postgres:16") as pg:
        yield re.sub(r"^postgresql\+\w+://", "postgresql://", pg.get_connection_url())


class ResourceDB(PostgresDB):
    async def get_user(self, user_id):
        row = await self.fetchrow("SELECT id FROM users WHERE id=$1", UUID(user_id))
        return {**dict(row), "is_approved": True} if row else None

    async def get_job(self, job_id):
        row = await self.fetchrow("SELECT * FROM jobs WHERE id=$1", UUID(job_id))
        return dict(row) if row else None

    async def update_job_status(
        self, job_id, *, status, expected_status=None, error_message=None
    ):
        result = await self.execute(
            "UPDATE jobs SET status=$2,error_message=$3 WHERE id=$1 AND ($4::text IS NULL OR status=$4)",
            UUID(job_id),
            status,
            error_message,
            expected_status,
        )
        return result == "UPDATE 1"

    async def create_job(self, **kwargs):
        # The production Job insertion/fencing funnel has separate coverage.
        # This fixture exercises native snapshot admission and reconciliation.
        from orchestrator.services.manifest_store import ManifestStore

        async with self.transaction_scope():
            row = await self.fetchrow(
                "INSERT INTO jobs(id) VALUES($1) RETURNING *",
                UUID(str(kwargs["job_id"])),
            )
            await ManifestStore(self).freeze_execution(
                work_kind="Job",
                work_id=str(row["id"]),
                owner_id=kwargs["user_id"],
                project_ids=kwargs["authority_project_ids"],
                **kwargs["execution_manifest"],
            )
            return dict(row)

    async def get_project(self, project_id):
        try:
            return await self.fetchrow(
                "SELECT * FROM projects WHERE id=$1", UUID(project_id)
            )
        except ValueError:
            return None

    async def get_user_role_in_project(self, project_id, user_id):
        return await self.fetchval(
            "SELECT role FROM project_members WHERE project_id=$1 AND user_id=$2",
            UUID(project_id),
            UUID(user_id),
        )

    async def create_project(self, name, description=None, *, manifest_project_id=None):
        return await self.fetchrow(
            "INSERT INTO projects(id,name) VALUES($1,$2) RETURNING *",
            manifest_project_id,
            name,
        )

    async def add_project_member(
        self, project_id, user_id, role="editor", *, defer_manifest=False
    ):
        await self.execute(
            "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,$3)",
            UUID(project_id),
            UUID(user_id),
            role,
        )


@pytest_asyncio.fixture
async def database(pg_url, monkeypatch):
    # Domain Expert visibility/identity has its own real-Postgres suite. This
    # fixture exercises resource transactions without duplicating that schema.
    monkeypatch.setattr(
        "orchestrator.services.manifest_experts.sync_expert_identity", AsyncMock()
    )
    monkeypatch.setattr(
        "orchestrator.services.manifest_projects.sync_project_identity", AsyncMock()
    )
    name = "manifest_" + uuid4().hex
    admin = await asyncpg.connect(pg_url)
    await admin.execute(f'CREATE DATABASE "{name}"')
    await admin.close()
    db = ResourceDB(
        pg_url.rsplit("/", 1)[0] + "/" + name, min_connections=2, max_connections=6
    )
    await db.connect()
    await db.execute("""
        CREATE TABLE users(id uuid PRIMARY KEY);
        CREATE TABLE projects(id uuid PRIMARY KEY, name text, status text DEFAULT 'active');
        CREATE TABLE project_members(project_id uuid, user_id uuid, role text);
        CREATE TABLE experts(id uuid PRIMARY KEY);
        CREATE TABLE jobs(id uuid PRIMARY KEY, status text DEFAULT 'created', error_message text,
            assigned_agent_id uuid, lease_expires_at timestamptz, execution_lane text DEFAULT 'pinned');
        CREATE TABLE threads(id uuid PRIMARY KEY, status text DEFAULT 'created');
    """)
    migration = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0234_manifest_resources.sql"
    )
    await db.execute(migration.read_text())
    await db.execute(
        migration.with_name("0307_installation_managed_resources.sql").read_text()
    )
    await db.execute(
        migration.with_name("0308_project_workspace_defaults.sql").read_text()
    )
    yield db
    await db.disconnect()


@pytest_asyncio.fixture
async def actor(database):
    user = {"id": uuid4(), "is_admin": False, "is_approved": True}
    await database.execute("INSERT INTO users(id) VALUES($1)", user["id"])
    return user


def expert(name="worker", **runtime):
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Expert",
        "metadata": {"name": name},
        "spec": {
            "runtime": {
                "image": "example/worker:v1",
                "config": {"a": None, "tools": ["unknown"]},
                **runtime,
            }
        },
    }


def job(name="assignment", **execution):
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Job",
        "metadata": {"name": name},
        "spec": {
            "task": {"text": "Do the work"},
            "execution": {"expert": {"ref": {"name": "worker"}}, **execution},
        },
    }


async def apply(service, docs, actor, **kwargs):
    return await service.apply(json.dumps(docs), actor, format="json", **kwargs)


def expected(result):
    return {
        resource_key(item["resource"]): item["resourceVersion"]
        for item in result["resources"]
    }


@pytest.mark.asyncio
async def test_resource_versions_and_private_values(database, actor):
    service = ManifestResourceService(database)
    initial = await apply(service, expert(), actor)
    first = initial["resources"][0]
    assert first["resource"]["spec"]["runtime"]["config"] == {
        "a": None,
        "tools": ["unknown"],
    }
    assert first["resourceVersion"] == 1
    repeated = await apply(service, expert(), actor)
    assert repeated["resources"][0]["uid"] == first["uid"]
    assert repeated["resources"][0]["changed"] is False
    changed = expert(image="example/worker:v2")
    with pytest.raises(HTTPException) as caught:
        await apply(service, changed, actor)
    assert caught.value.status_code == 409
    current = await apply(service, changed, actor, expected_versions=expected(initial))
    assert current["resources"][0]["resourceVersion"] == 2
    assert await database.fetchval("SELECT count(*) FROM srw_resource_revisions") == 2
    with pytest.raises(HTTPException) as caught:
        await apply(service, expert(), actor, expected_versions=expected(initial))
    assert caught.value.status_code == 409


@pytest.mark.asyncio
async def test_concurrent_updates_have_one_winner(database, actor):
    service = ManifestResourceService(database)
    initial = await apply(service, expert(), actor)
    results = await asyncio.gather(
        *[
            apply(
                service,
                expert(image=f"example/worker:v{n}"),
                actor,
                expected_versions=expected(initial),
            )
            for n in (2, 3)
        ],
        return_exceptions=True,
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    failure = next(result for result in results if isinstance(result, Exception))
    assert isinstance(failure, HTTPException) and failure.status_code == 409


@pytest.mark.asyncio
async def test_reviewed_dependency_changes_and_pinned_revisions(database, actor):
    service = ManifestResourceService(database)
    initial = await apply(service, expert(), actor)
    old = initial["resources"][0]
    preview = await service.preview(json.dumps(job()), actor, format="json")
    await apply(
        service,
        expert(image="example/new:2"),
        actor,
        expected_versions=expected(initial),
    )
    with pytest.raises(HTTPException) as caught:
        await apply(service, job(), actor, plan_revision=preview["planRevision"])
    assert caught.value.status_code == 409
    pinned = job(expert={"ref": {"name": "worker", "revision": old["revision"]}})
    resolved = await service.preview(json.dumps(pinned), actor, format="json")
    runtime = resolved["resolved"][0]["spec"]["execution"]["expert"]["inline"][
        "runtime"
    ]
    assert runtime["image"] == "example/worker:v1"
    assert (
        await database.fetchval("SELECT count(*) FROM srw_resources WHERE kind='Job'")
        == 0
    )


@pytest.mark.asyncio
async def test_scopes_and_token_restrictions_do_not_follow_authored_names(
    database, actor
):
    service = ManifestResourceService(database)
    other = {**actor, "id": uuid4()}
    await database.execute("INSERT INTO users(id) VALUES($1)", other["id"])
    initial = await apply(service, expert(), actor)
    for who in (other, {**actor, "scopes": [f"project:{uuid4()}"]}):
        doc = expert()
        doc["metadata"]["scope"] = {"kind": "Account", "name": str(actor["id"])}
        with pytest.raises(HTTPException) as caught:
            await apply(service, doc, who)
        assert caught.value.status_code == 403
    with pytest.raises(HTTPException) as caught:
        await service.get(initial["resources"][0]["uid"], other)
    assert caught.value.status_code == 403


@pytest.mark.asyncio
async def test_project_activation_is_atomic_and_defaults_use_pinned_content(
    database, actor
):
    service = ManifestResourceService(database)
    project = {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {"name": "team"},
        "spec": {
            "resources": {"experts": {"worker": {"inline": expert()["spec"]}}},
            "defaults": {"expert": "worker"},
        },
    }
    initial = await apply(service, project, actor)
    project_row = next(
        item for item in initial["resources"] if item["resource"]["kind"] == "Project"
    )
    project_id = await database.fetchval(
        "SELECT linked_id FROM srw_resources WHERE id=$1", UUID(project_row["uid"])
    )
    scoped_job = job()
    scoped_job["metadata"]["scope"] = {"kind": "Project", "name": str(project_id)}
    scoped_job["spec"]["execution"] = {}
    preview = await service.preview(json.dumps(scoped_job), actor, format="json")
    assert (
        preview["resolved"][0]["spec"]["execution"]["expert"]["inline"]["runtime"][
            "image"
        ]
        == "example/worker:v1"
    )
    changed = deepcopy(project)
    changed["spec"]["resources"]["experts"]["worker"]["inline"]["runtime"]["image"] = (
        "example/worker:v2"
    )
    with pytest.raises(HTTPException) as caught:
        await apply(service, changed, actor)
    assert caught.value.status_code == 409
    assert (
        await database.fetchval(
            "SELECT active_revision FROM srw_resources WHERE id=$1",
            UUID(project_row["uid"]),
        )
        == project_row["revision"]
    )
    assert await database.fetchval("SELECT count(*) FROM srw_resource_revisions") == 2
    upgraded = await apply(
        service,
        changed,
        actor,
        expected_versions={
            resource_key(project_row["resource"]): project_row["resourceVersion"]
        },
    )
    assert all(item["resourceVersion"] == 2 for item in upgraded["resources"])


@pytest.mark.asyncio
async def test_project_manifest_owns_and_releases_workspace_defaults(database, actor):
    service = ManifestResourceService(database)
    # A standalone, Account-scoped WorkspaceTemplate referenced by alias: an
    # inline alias would become a managed child of the Project, and a managed
    # child can only be removed by re-applying its owning Project (never by
    # deleting the Project itself), which would confuse this test's own
    # store-delete release path with the activation release path.
    workspace = {
        "apiVersion": "srw/v1alpha1",
        "kind": "WorkspaceTemplate",
        "metadata": {"name": "box"},
        "spec": {"backend": "vm"},
    }
    await apply(service, workspace, actor)
    project = {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {"name": "team"},
        "spec": {
            "resources": {
                "workspaces": {
                    "box": {
                        "ref": {
                            "name": "box",
                            "scope": {"kind": "Account", "name": str(actor["id"])},
                        }
                    }
                }
            },
            "defaults": {"workspace": {"jobs": "vm", "vm": "box"}},
        },
    }
    initial = await apply(service, project, actor)
    project_row = next(
        item for item in initial["resources"] if item["resource"]["kind"] == "Project"
    )
    project_id = await database.fetchval(
        "SELECT linked_id FROM srw_resources WHERE id=$1", UUID(project_row["uid"])
    )
    row = await read_project_defaults(database, project_id)
    assert row.jobs == "vm"
    assert row.source == "manifest"
    assert row.manifest_revision == project_row["revision"]

    store = ManifestStore(database)
    resource = await store.by_link("Project", project_id)
    await store.delete(resource, expected_version=resource["resource_version"])
    assert await read_project_defaults(database, project_id) == ProjectDefaults()


async def manifest_project(database, actor, spec):
    initial = await apply(
        ManifestResourceService(database),
        {
            "apiVersion": "srw/v1alpha1",
            "kind": "Project",
            "metadata": {"name": "team"},
            "spec": spec,
        },
        actor,
    )
    project_row = next(
        item for item in initial["resources"] if item["resource"]["kind"] == "Project"
    )
    return await database.fetchval(
        "SELECT linked_id FROM srw_resources WHERE id=$1", UUID(project_row["uid"])
    )


async def admit_generic_project_job(database, actor, monkeypatch, project_id):
    """Admit a generic-image Job that omits its workspace; record create_job."""
    from orchestrator.services.manifest_execution import ManifestExecutionService
    from orchestrator.services.manifest_execution_snapshot import read_execution
    from orchestrator.services.manifest_workspaces import ManifestWorkspaceService

    created = []
    create_job = database.create_job

    async def recording_create_job(**kwargs):
        created.append(kwargs)
        return await create_job(**kwargs)

    monkeypatch.setattr(database, "create_job", recording_create_job)
    workspaces = ManifestWorkspaceService(
        database,
        WorkspaceProcessRuntime(),
        namespace="ws",
        harness_namespace="agents",
        default_image="workspace:test",
    )
    execution = ManifestExecutionService(
        database,
        runtime=ProcessRuntime(),
        namespace="agents",
        workspace=workspaces,
        native_hosting_enabled=True,
    )
    resources = ManifestResourceService(database, admit_job=execution.admit)
    assignment = job(expert={"inline": expert()["spec"]})
    assignment["metadata"]["scope"] = {"kind": "Project", "name": str(project_id)}
    result = await apply(resources, assignment, actor)
    work_id = next(iter(result["executions"].values()))
    snapshot = await read_execution(database, "Job", work_id)
    # The authored document keeps the omission; admission fills the snapshot.
    assert "workspace" not in snapshot["document"]["spec"]["execution"]
    return snapshot, created[0]


@pytest.mark.asyncio
async def test_generic_job_without_workspace_gets_the_project_container_template(
    database, actor, monkeypatch
):
    from orchestrator.services.workspace_defaults_resolution import (
        workspace_sources_record,
    )

    # The shorthand to a sandbox alias: Jobs mode container plus that template
    # (the shape of the published conformance fixture's generic Job).
    recipe = {
        "backend": "sandbox",
        "environment": {"image": "example/ssh-workspace:1"},
        "initialize": [{"command": ["mkdir", "-p", "project"]}],
        "retention": "Retain",
    }
    project_id = await manifest_project(
        database,
        actor,
        {
            "resources": {"workspaces": {"development": {"inline": recipe}}},
            "defaults": {"workspace": "development"},
        },
    )
    row = await read_project_defaults(database, project_id)
    assert (row.jobs, row.source) == ("container", "manifest")
    assert "inline" in row.container
    snapshot, created = await admit_generic_project_job(
        database, actor, monkeypatch, project_id
    )
    template = snapshot["resolved"]["spec"]["execution"]["workspace"]["template"]
    assert template["inline"]["backend"] == "sandbox"
    assert template["inline"]["retention"] == "Retain"
    assert template["inline"]["environment"]["image"] == "example/ssh-workspace:1"
    assert created["config_override"] == {"workspace": {"backend": "sandbox"}}
    assert workspace_sources_record(created["workspace_selection"]) == {
        "tier": "project",
        "template": "project",
        "template_name": None,
    }
    # Reserved like an authored binding.
    assert (
        await database.fetchval(
            "SELECT count(*) FROM srw_execution_workspace_bindings WHERE execution_id=$1",
            snapshot["id"],
        )
        == 1
    )


@pytest.mark.asyncio
async def test_generic_job_resolves_a_settings_row_template_reference(
    database, actor, monkeypatch
):
    from orchestrator.services.project_workspace_defaults import (
        save_settings_defaults,
    )

    await apply(
        ManifestResourceService(database),
        {
            "apiVersion": "srw/v1alpha1",
            "kind": "WorkspaceTemplate",
            "metadata": {"name": "tools"},
            "spec": {
                "backend": "sandbox",
                "environment": {"image": "example/tools-workspace:2"},
            },
        },
        actor,
    )
    project_id = await manifest_project(database, actor, {"resources": {}})
    reference = {
        "ref": {"name": "tools", "scope": {"kind": "Account", "name": str(actor["id"])}}
    }
    await save_settings_defaults(
        database,
        project_id,
        ProjectDefaults(jobs="container", container=reference),
        actor_id=str(actor["id"]),
    )
    snapshot, created = await admit_generic_project_job(
        database, actor, monkeypatch, project_id
    )
    template = snapshot["resolved"]["spec"]["execution"]["workspace"]["template"]
    assert template["inline"]["environment"]["image"] == "example/tools-workspace:2"
    assert created["config_override"] == {"workspace": {"backend": "sandbox"}}
    assert created["workspace_selection"]["template_name"] == "tools"
    assert any(
        dependency.get("key", "").endswith("/tools")
        for dependency in snapshot["dependencies"]
    )


@pytest.mark.asyncio
async def test_generic_job_names_a_missing_project_container_template(
    database, actor, monkeypatch
):
    from orchestrator.services.project_workspace_defaults import (
        save_settings_defaults,
    )

    project_id = await manifest_project(database, actor, {"resources": {}})
    missing = {
        "ref": {"name": "gone", "scope": {"kind": "Account", "name": str(actor["id"])}}
    }
    await save_settings_defaults(
        database,
        project_id,
        ProjectDefaults(jobs="container", container=missing),
        actor_id=str(actor["id"]),
    )
    with pytest.raises(HTTPException) as refused:
        await admit_generic_project_job(database, actor, monkeypatch, project_id)
    assert refused.value.status_code == 409
    assert refused.value.detail == (
        "This Project's container template 'gone' no longer exists."
    )
    assert await database.fetchval("SELECT count(*) FROM jobs") == 0


@pytest.mark.asyncio
async def test_generic_job_without_workspace_ignores_a_project_vm_default(
    database, actor, monkeypatch
):
    project_id = await manifest_project(
        database,
        actor,
        {
            "resources": {"workspaces": {"box": {"inline": {"backend": "vm"}}}},
            "defaults": {"workspace": {"jobs": "vm", "vm": "box"}},
        },
    )
    assert (await read_project_defaults(database, project_id)).jobs == "vm"
    snapshot, created = await admit_generic_project_job(
        database, actor, monkeypatch, project_id
    )
    # A generic image can't use a VM tier: it keeps "omitted means none".
    assert snapshot["resolved"]["spec"]["execution"]["workspace"] is None
    assert created["config_override"] == {"workspace": {"backend": "none"}}
    assert created["requested_workspace_backend"] == "none"
    assert created["workspace_selection"] is None


@pytest.mark.asyncio
async def test_secrets_are_encrypted_and_plans_never_deliver_them(database, actor):
    service = ManifestResourceService(database)
    secret = await service.put_secret(
        actor, scope=None, name="provider", values={"token": "private-test-sentinel"}
    )
    doc = expert(env={"TOKEN": {"secretRef": {"name": "provider", "key": "token"}}})
    initial = await apply(service, doc, actor)
    preview = await service.preview(json.dumps(doc), actor, format="json")
    stored = await database.fetchval("SELECT ciphertext FROM srw_resource_secrets")
    assert stored.startswith("v1:") and "private-test-sentinel" not in stored
    assert "private-test-sentinel" not in json.dumps([initial, preview, secret])
    await service.put_secret(
        actor,
        scope=None,
        name="provider",
        values={"token": "next"},
        expected_version=secret["resourceVersion"],
    )
    with pytest.raises(HTTPException) as caught:
        await apply(service, doc, actor, plan_revision=preview["planRevision"])
    assert caught.value.status_code == 409


@pytest.mark.asyncio
async def test_failed_admission_rolls_back_resource_and_operation(database, actor):
    async def refuse(*args, **kwargs):
        raise HTTPException(403, "Execution access denied")

    service = ManifestResourceService(database, admit_job=refuse)
    with pytest.raises(HTTPException):
        await apply(service, [expert(), job()], actor, idempotency_key="atomic")
    assert await database.fetchval("SELECT count(*) FROM srw_resources") == 0
    assert await database.fetchval("SELECT count(*) FROM srw_manifest_operations") == 0


@pytest.mark.asyncio
async def test_delete_and_recreate_has_new_identity(database, actor):
    service = ManifestResourceService(database)
    first = (await apply(service, expert(), actor))["resources"][0]
    await service.delete(first["uid"], actor, expected_version=1)
    second = (await apply(service, expert(), actor))["resources"][0]
    assert first["uid"] != second["uid"]
    assert second["resourceVersion"] == 1
    assert await database.fetchval("SELECT count(*) FROM srw_resource_revisions") == 2


class ProcessRuntime:
    def __init__(self):
        self.pods = {}
        self.launches = []
        self.cancelled = []
        self.cleaned = []

    async def observe(self, identity, *, expected_pod_uid=None):
        from orchestrator.services.generic_harness_runtime import GenericPodObservation

        return self.pods.get(
            identity.pod_name, GenericPodObservation(identity.pod_name, None, "Absent")
        )

    async def launch(self, plan):
        from orchestrator.services.generic_harness_runtime import GenericPodObservation

        self.launches.append(plan)
        observed = GenericPodObservation(
            plan.identity.pod_name,
            str(uuid4()),
            "Running",
            image_id="example/worker@sha256:" + "a" * 64,
        )
        self.pods[plan.identity.pod_name] = observed
        return observed

    def exit(self, code=0):
        from dataclasses import replace

        name = self.launches[-1].identity.pod_name
        self.pods[name] = replace(
            self.pods[name],
            phase="Succeeded" if code == 0 else "Failed",
            process_exit_code=code,
            containers_terminal=True,
        )

    async def cancel(self, identity, *, expected_pod_uid):
        self.cancelled.append(identity.pod_name)
        self.exit(143)
        return self.pods[identity.pod_name]

    async def cleanup(self, identity, *, expected_pod_uid):
        self.cleaned.append(identity.pod_name)
        self.pods.pop(identity.pod_name, None)
        return True


async def native_execution(database, actor, *, max_attempts=1, mode="ProcessExit"):
    from orchestrator.services.manifest_execution import ManifestExecutionService

    process = ProcessRuntime()
    execution = ManifestExecutionService(
        database, runtime=process, namespace="test", native_hosting_enabled=True
    )
    resources = ManifestResourceService(database, admit_job=execution.admit)
    assignment = job(expert={"inline": expert()["spec"]})
    assignment["spec"]["retry"] = {"maxAttempts": max_attempts}
    assignment["spec"]["completion"] = {"mode": mode}
    assignment["spec"]["timeoutSeconds"] = 60
    result = await apply(resources, assignment, actor)
    work_id = next(iter(result["executions"].values()))
    execution_id = str(
        await database.fetchval(
            "SELECT id FROM srw_execution_specs WHERE work_id=$1", UUID(work_id)
        )
    )
    return execution, process, resources, assignment, result, work_id, execution_id


@pytest.mark.asyncio
async def test_zero_hook_job_process_exit_and_reapply_never_replays(database, actor):
    (
        execution,
        process,
        resources,
        assignment,
        result,
        work_id,
        execution_id,
    ) = await native_execution(database, actor)
    await execution.reconcile_one(execution_id)
    assert (await database.get_job(work_id))["status"] == "processing"
    container = process.launches[0].pod["spec"]["containers"][0]
    assert "command" not in container and "envFrom" not in container
    assert process.launches[0].pod["spec"]["automountServiceAccountToken"] is False
    process.exit()
    await execution.reconcile_one(execution_id)
    assert (await database.get_job(work_id))["status"] == "completed"
    repeated = await apply(resources, assignment, actor)
    assert repeated["executions"] == result["executions"]
    await execution.reconcile()
    assert len(process.launches) == 1
    assert await database.fetchval("SELECT count(*) FROM srw_execution_specs") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["created", "processing", "completed"])
async def test_job_metadata_edits_preserve_execution_and_require_current_version(
    database, actor, phase
):
    (
        execution,
        process,
        resources,
        assignment,
        first,
        work_id,
        execution_id,
    ) = await native_execution(database, actor)
    if phase != "created":
        await execution.reconcile_one(execution_id)
    if phase == "completed":
        process.exit()
        await execution.reconcile_one(execution_id)
    assert (await database.get_job(work_id))["status"] == phase
    snapshot = await database.fetchval(
        "SELECT row_to_json(s)::text FROM srw_execution_specs s"
    )
    launches = len(process.launches)
    edited = deepcopy(assignment)
    edited["metadata"].update(
        tags=["development", "reviewed"],
        labels={"team": "toolchain"},
        annotations={"example.com/note": "Classification only"},
    )

    with pytest.raises(HTTPException) as caught:
        await apply(resources, edited, actor)
    assert caught.value.status_code == 409

    updated = await apply(resources, edited, actor, expected_versions=expected(first))
    original, current = first["resources"][0], updated["resources"][0]
    assert current["uid"] == original["uid"]
    assert current["resourceVersion"] == original["resourceVersion"] + 1
    assert current["revision"] == original["revision"]
    for field in ("tags", "labels", "annotations"):
        assert current["resource"]["metadata"][field] == edited["metadata"][field]
    assert updated["executions"] == first["executions"]
    assert (await database.get_job(work_id))["status"] == phase
    assert len(process.launches) == launches
    assert (
        await database.fetchval(
            "SELECT row_to_json(s)::text FROM srw_execution_specs s"
        )
        == snapshot
    )

    with pytest.raises(HTTPException) as caught:
        await apply(resources, assignment, actor, expected_versions=expected(first))
    assert caught.value.status_code == 409
    repeated = await apply(
        resources, edited, actor, expected_versions=expected(updated)
    )
    assert not repeated["resources"][0]["changed"]
    assert repeated["executions"] == first["executions"]
    if phase == "completed":
        await execution.reconcile()
        assert len(process.launches) == launches


@pytest.mark.asyncio
async def test_job_metadata_edit_cannot_change_the_admitted_spec(database, actor):
    _, _, resources, assignment, first, _, _ = await native_execution(database, actor)
    original = await database.fetchval(
        "SELECT row_to_json(s)::text FROM srw_execution_specs s"
    )
    edited = deepcopy(assignment)
    edited["metadata"]["tags"] = ["reviewed"]
    edited["spec"]["task"]["text"] = "A different assignment"
    with pytest.raises(HTTPException) as caught:
        await apply(resources, edited, actor, expected_versions=expected(first))
    assert caught.value.status_code == 409
    assert await database.fetchval("SELECT count(*) FROM srw_resource_revisions") == 1
    assert (
        await database.fetchval(
            "SELECT row_to_json(s)::text FROM srw_execution_specs s"
        )
        == original
    )


@pytest.mark.asyncio
async def test_retry_requires_terminal_observation_and_cleanup(database, actor):
    execution, process, _, _, _, work_id, execution_id = await native_execution(
        database, actor, max_attempts=2
    )
    await execution.reconcile_one(execution_id)
    await execution.reconcile_one(execution_id)
    assert len(process.launches) == 1
    process.exit(1)
    await execution.reconcile_one(execution_id)
    assert len(process.cleaned) == 1 and len(process.launches) == 1
    await execution.reconcile_one(execution_id)
    assert len(process.launches) == 2
    assert (
        process.launches[0].identity.execution_id
        == process.launches[1].identity.execution_id
    )
    assert process.launches[1].identity.attempt == 2
    process.exit()
    await execution.reconcile_one(execution_id)
    assert (await database.get_job(work_id))["status"] == "completed"


@pytest.mark.asyncio
async def test_crash_after_cleanup_recovers_recorded_outcome(
    database, actor, monkeypatch
):
    execution, process, _, _, _, work_id, execution_id = await native_execution(
        database, actor
    )
    await execution.reconcile_one(execution_id)
    process.exit()
    settle = execution._settle_attempt
    monkeypatch.setattr(
        execution,
        "_settle_attempt",
        AsyncMock(side_effect=RuntimeError("process restart")),
    )
    with pytest.raises(RuntimeError):
        await execution.reconcile_one(execution_id)
    assert not process.pods
    monkeypatch.setattr(execution, "_settle_attempt", settle)
    await execution.reconcile_one(execution_id)
    assert (await database.get_job(work_id))["status"] == "completed"
    assert len(process.launches) == 1


@pytest.mark.asyncio
async def test_cancel_prevents_retry_and_waits_for_process_fence(database, actor):
    execution, process, _, _, _, work_id, execution_id = await native_execution(
        database, actor, max_attempts=3
    )
    await execution.reconcile_one(execution_id)
    assert await execution.cancel(work_id) is True
    assert process.cancelled and not process.cleaned
    await execution.reconcile_one(execution_id)
    assert process.cleaned and len(process.launches) == 1
    assert (await database.get_job(work_id))["status"] == "cancelled"


@pytest.mark.asyncio
async def test_unproven_pod_loss_cannot_launch_another_attempt(database, actor):
    execution, process, _, _, _, work_id, execution_id = await native_execution(
        database, actor, max_attempts=3
    )
    await execution.reconcile_one(execution_id)
    process.pods.clear()
    await execution.reconcile_one(execution_id)
    await execution.reconcile_one(execution_id)
    assert (await database.get_job(work_id))["status"] == "failed"
    assert not process.cleaned and len(process.launches) == 1


@pytest.mark.asyncio
async def test_manual_exit_waits_for_authorized_current_attempt_outcome(
    database, actor
):
    (
        execution,
        process,
        resources,
        _,
        result,
        work_id,
        execution_id,
    ) = await native_execution(database, actor, mode="Manual", max_attempts=2)
    resource_id = result["resources"][0]["uid"]
    await execution.reconcile_one(execution_id)
    process.exit(2)
    await execution.reconcile_one(execution_id)
    assert (await database.get_job(work_id))["status"] == "pending_review"
    assert len(process.launches) == 1
    status = (await resources.get(resource_id, actor))["status"]
    assert status["attempt"]["exitCode"] == 2
    assert status["attempt"]["cleaned"] is True
    with pytest.raises(HTTPException) as stale:
        await execution.report_outcome(
            resource_id, actor, attempt=2, outcome="Succeeded"
        )
    assert stale.value.status_code == 409
    with pytest.raises(HTTPException) as denied:
        await execution.report_outcome(
            resource_id, {**actor, "id": uuid4()}, attempt=1, outcome="Succeeded"
        )
    assert denied.value.status_code == 403
    await execution.report_outcome(resource_id, actor, attempt=1, outcome="Succeeded")
    await execution.reconcile()
    assert (await database.get_job(work_id))["status"] == "completed"
    assert len(process.launches) == 1
    assert (
        await execution.report_outcome(
            resource_id, actor, attempt=1, outcome="Succeeded"
        )
    )["accepted"]


@pytest.mark.asyncio
async def test_reported_outcome_cannot_skip_process_fencing(database, actor):
    execution, process, _, _, result, work_id, execution_id = await native_execution(
        database, actor, mode="Reported"
    )
    await execution.reconcile_one(execution_id)
    await execution.report_outcome(
        result["resources"][0]["uid"], actor, attempt=1, outcome="Succeeded"
    )
    await execution.reconcile_one(execution_id)
    assert process.cancelled and not process.cleaned
    assert (await database.get_job(work_id))["status"] == "processing"
    await execution.reconcile_one(execution_id)
    assert (await database.get_job(work_id))["status"] == "completed"


@pytest.mark.asyncio
async def test_missing_report_times_out_even_after_successful_process_exit(
    database, actor
):
    execution, process, _, _, _, work_id, execution_id = await native_execution(
        database, actor, mode="Reported"
    )
    await execution.reconcile_one(execution_id)
    process.exit()
    await execution.reconcile_one(execution_id)
    assert (await database.get_job(work_id))["status"] == "processing"
    await database.execute(
        "UPDATE srw_execution_specs SET created_at=now()-interval '2 minutes' WHERE id=$1",
        UUID(execution_id),
    )
    await execution.reconcile_one(execution_id)
    assert (await database.get_job(work_id))["status"] == "failed"
    assert process.cleaned


@pytest.mark.asyncio
async def test_database_requires_native_attempt_authority(database, actor):
    _, _, _, _, _, work_id, _ = await native_execution(database, actor)
    with pytest.raises(asyncpg.CheckViolationError, match="recorded running attempt"):
        await database.update_job_status(work_id, status="processing")
    with pytest.raises(asyncpg.CheckViolationError, match="recorded outcome"):
        await database.update_job_status(work_id, status="completed")


@pytest.mark.asyncio
async def test_unverified_network_profile_blocks_generic_admission_without_effects(
    database, actor
):
    from orchestrator.services.manifest_execution import ManifestExecutionService

    processes = ProcessRuntime()
    execution = ManifestExecutionService(database, runtime=processes, namespace="test")
    resources = ManifestResourceService(database, admit_job=execution.admit)
    with pytest.raises(HTTPException) as unavailable:
        await apply(resources, job(expert={"inline": expert()["spec"]}), actor)
    assert unavailable.value.status_code == 503
    assert unavailable.value.detail["code"] == "HostingCapabilityUnavailable"
    for table in (
        "jobs",
        "srw_resources",
        "srw_execution_specs",
        "srw_manifest_operations",
    ):
        assert await database.fetchval(f"SELECT count(*) FROM {table}") == 0
    assert not processes.launches


@pytest.mark.asyncio
async def test_reapply_job_keeps_original_referenced_generation(database, actor):
    from orchestrator.services.manifest_execution import ManifestExecutionService

    process = ProcessRuntime()
    execution = ManifestExecutionService(
        database, runtime=process, namespace="test", native_hosting_enabled=True
    )
    resources = ManifestResourceService(database, admit_job=execution.admit)
    authored = [expert(), job()]
    first = await apply(resources, authored, actor)
    original = await database.fetchval("SELECT resolved::text FROM srw_execution_specs")
    change = expert(image="example/worker:v2")
    versions = {
        key: value
        for key, value in expected(first).items()
        if key.startswith("Expert/")
    }
    await apply(resources, change, actor, expected_versions=versions)
    repeated = await apply(resources, job(), actor)
    assert repeated["executions"] == first["executions"]
    assert (
        await database.fetchval("SELECT resolved::text FROM srw_execution_specs")
        == original
    )
    classified = job()
    classified["metadata"]["tags"] = ["reviewed"]
    job_versions = {
        key: value for key, value in expected(first).items() if key.startswith("Job/")
    }
    updated = await apply(resources, classified, actor, expected_versions=job_versions)
    assert updated["executions"] == first["executions"]
    current = await resources.store.by_id(updated["resources"][0]["uid"])
    assert (
        current["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"]["image"]
        == expert()["spec"]["runtime"]["image"]
    )
    assert current["resolved"]["metadata"]["tags"] == ["reviewed"]
    assert (
        await database.fetchval("SELECT resolved::text FROM srw_execution_specs")
        == original
    )


class WorkspaceProcessRuntime(ProcessRuntime):
    def __init__(self):
        super().__init__()
        self.volumes = {}
        self.deleted_volumes = []

    async def ensure_volume(self, plan, *, expected_pvc_uid=None):
        uid = self.volumes.setdefault(plan.identity.pvc_name, str(uuid4()))
        assert expected_pvc_uid is None or expected_pvc_uid == uid
        return uid

    async def launch(self, plan):
        from orchestrator.services.manifest_workspace_runtime import (
            WorkspacePodObservation,
        )

        self.launches.append(plan)
        observed = WorkspacePodObservation(
            plan.identity.pod_name,
            str(uuid4()),
            "Running",
            readiness=True,
            pod_ip="10.42.1.23",
            initialization_succeeded=True,
            image_id="example/workspace@sha256:" + "b" * 64,
        )
        self.pods[plan.identity.pod_name] = observed
        return observed

    async def delete_volume(self, identity, *, expected_pvc_uid):
        assert self.volumes.get(identity.pvc_name) == expected_pvc_uid
        self.deleted_volumes.append(expected_pvc_uid)
        self.volumes.pop(identity.pvc_name)
        return True


async def native_workspace_execution(
    database, actor, *, retention="Retain", mode="ProcessExit", attempts=2
):
    from orchestrator.services.manifest_execution import ManifestExecutionService
    from orchestrator.services.manifest_workspaces import ManifestWorkspaceService

    processes, workspace_processes = ProcessRuntime(), WorkspaceProcessRuntime()
    workspaces = ManifestWorkspaceService(
        database,
        workspace_processes,
        namespace="ws",
        harness_namespace="agents",
        default_image="workspace:test",
    )
    executions = ManifestExecutionService(
        database,
        runtime=processes,
        namespace="agents",
        workspace=workspaces,
        native_hosting_enabled=True,
    )
    resources = ManifestResourceService(database, admit_job=executions.admit)
    recipe = {
        "backend": "sandbox",
        "retention": retention,
        "initialize": [{"command": ["seed"]}],
    }
    authored = job(
        expert={"inline": expert()["spec"]}, workspace={"template": {"inline": recipe}}
    )
    authored["spec"].update(retry={"maxAttempts": attempts}, completion={"mode": mode})
    result = await apply(resources, authored, actor)
    work_id = next(iter(result["executions"].values()))
    snapshot = await executions.store.execution("Job", work_id)
    return (
        executions,
        resources,
        processes,
        workspaces,
        workspace_processes,
        snapshot,
        result,
    )


@pytest.mark.asyncio
async def test_retry_fences_workspace_preserves_instance_and_initializes_once(
    database, actor
):
    (
        executions,
        resources,
        processes,
        workspaces,
        ws_processes,
        snapshot,
        _,
    ) = await native_workspace_execution(database, actor)
    execution_id = str(snapshot["id"])
    await executions.reconcile_one(execution_id)
    before = await workspaces._bound(snapshot)
    first_key = before["ssh_ciphertext"]
    assert before["initialized"] and first_key.startswith("v1:")
    ingress = ws_processes.launches[0].runtime.network_policy["spec"]["ingress"][0][
        "from"
    ][0]
    assert (
        ingress["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"]
        == "agents"
    )
    assert ws_processes.launches[0].initialize
    processes.exit(1)
    await executions.reconcile_one(execution_id)
    assert ws_processes.cancelled and len(processes.launches) == 1
    assert (await workspaces._bound(snapshot))["pod_uid"] == before["pod_uid"]
    await executions.reconcile_one(execution_id)
    assert len(processes.launches) == 1
    await executions.reconcile_one(execution_id)
    after = await workspaces._bound(snapshot)
    assert after["id"] == before["id"] and after["pvc_uid"] == before["pvc_uid"]
    assert after["generation"] == 2 and after["ssh_ciphertext"] != first_key
    assert not ws_processes.launches[-1].initialize
    assert not ws_processes.deleted_volumes and len(processes.launches) == 2
    processes.exit()
    await executions.reconcile_one(execution_id)
    await executions.reconcile_one(execution_id)
    retained = await workspaces._bound(snapshot)
    assert retained["status"] == "Detached" and retained["execution_id"] is None
    second = job(
        name="reuse",
        expert={"inline": expert()["spec"]},
        workspace={"instanceRef": {"uid": str(before["id"])}},
    )
    await apply(resources, second, actor)
    with pytest.raises(HTTPException, match="already|still"):
        await apply(
            resources, {**second, "metadata": {"name": "concurrent-reuse"}}, actor
        )


@pytest.mark.asyncio
async def test_manual_workspace_retention_waits_for_decision(database, actor):
    (
        executions,
        _,
        processes,
        workspaces,
        ws_processes,
        snapshot,
        result,
    ) = await native_workspace_execution(
        database, actor, retention="Delete", mode="Manual", attempts=1
    )
    execution_id = str(snapshot["id"])
    await executions.reconcile_one(execution_id)
    processes.exit()
    await executions.reconcile_one(execution_id)
    await executions.reconcile_one(execution_id)
    row = await workspaces._bound(snapshot)
    assert row["status"] == "Detached" and row["execution_id"] == snapshot["id"]
    assert not ws_processes.deleted_volumes
    await executions.report_outcome(
        result["resources"][0]["uid"], actor, attempt=1, outcome="Succeeded"
    )
    await executions.reconcile()
    assert ws_processes.deleted_volumes == [row["pvc_uid"]]
    assert (await database.get_job(str(snapshot["work_id"])))["status"] == "completed"


@pytest.mark.asyncio
async def test_workspace_replacement_cannot_be_cancelled_or_adopted(
    database, actor, monkeypatch
):
    from orchestrator.services.generic_harness_runtime import (
        GenericAttemptIdentity,
        GenericPodObservation,
    )

    (
        executions,
        _,
        _,
        workspaces,
        ws_processes,
        snapshot,
        _,
    ) = await native_workspace_execution(database, actor)
    await executions.reconcile_one(str(snapshot["id"]))
    row = await workspaces._bound(snapshot)
    monkeypatch.setattr(
        ws_processes,
        "observe",
        AsyncMock(
            return_value=GenericPodObservation(
                row["pod_name"], "replacement-uid", "Replaced"
            )
        ),
    )
    assert not await workspaces.detach(
        snapshot, GenericAttemptIdentity(str(snapshot["id"]), 1), final=True
    )
    assert (await workspaces._bound(snapshot))["pod_uid"] == row["pod_uid"]
    assert not ws_processes.cancelled and not ws_processes.cleaned
