"""select_execution_workspace consults the defaults chain (Slice A2b)."""

from copy import deepcopy
import json
from uuid import UUID

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


GONE = {"ref": {"name": "gone", "scope": {"kind": "Catalog", "name": "shared"}}}
GONE_MESSAGE = "This Project's container template 'gone' no longer exists."


async def _cron_automation(db, actor, name, *, project_id=None, minutes_ago):
    return str(
        await db.fetchval(
            """
            INSERT INTO automations
                (owner_id, project_id, name, trigger_type, cron_expr, expert,
                 prompt, next_run_at)
            VALUES ($1, $2, $3, 'cron', '*/5 * * * *', 'worker_base', 'Do it',
                    now() - make_interval(mins => $4))
            RETURNING id
            """,
            actor["id"],
            UUID(project_id) if project_id else None,
            name,
            minutes_ago,
        )
    )


@pytest.mark.asyncio
async def test_a_refused_cron_fire_is_recorded_and_the_next_automation_still_fires(
    database, actor, monkeypatch
):
    from datetime import datetime, timedelta, timezone

    from orchestrator.services import cron_dispatcher

    # The bundled worker_base, so the fire needs no Expert catalogue.
    monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
    # Clock-independent: every advance lands a day ahead, so no row can
    # become due again within this drain whatever the wall clock says.
    monkeypatch.setattr(
        cron_dispatcher,
        "compute_next_run_after",
        lambda *_args: datetime.now(timezone.utc) + timedelta(days=1),
    )

    project_id = await _project(database)
    await save_settings_defaults(
        database,
        project_id,
        ProjectDefaults(jobs="container", container=GONE),
        actor_id=str(actor["id"]),
    )
    # The refused row is due first, so it is claimed first on every tick.
    refused = await _cron_automation(
        database, actor, "refused", project_id=project_id, minutes_ago=2
    )
    fires = await _cron_automation(database, actor, "fires", minutes_ago=1)

    assert await cron_dispatcher._tick(database) == 2

    row = await database.fetchrow(
        "SELECT last_status, next_run_at > now() AS advanced, run_count FROM automations WHERE id=$1",
        UUID(refused),
    )
    assert row["last_status"] == GONE_MESSAGE
    assert row["advanced"] and row["run_count"] == 0
    assert (
        await database.fetchval(
            "SELECT count(*) FROM jobs WHERE context->>'automation_id' = $1", refused
        )
        == 0
    )
    fired = await database.fetchrow(
        "SELECT last_status, run_count, last_job_id FROM automations WHERE id=$1",
        UUID(fires),
    )
    assert fired["run_count"] == 1 and fired["last_status"] is None
    job = await database.get_job(str(fired["last_job_id"]))
    assert _json(job["context"])["automation_id"] == fires


@pytest.mark.asyncio
async def test_a_default_template_of_another_tier_fails_closed(database, actor):
    from orchestrator.services.manifest_resources import ManifestResourceService

    template = {
        "apiVersion": "srw/v1alpha1",
        "kind": "WorkspaceTemplate",
        "metadata": {"name": "edited", "scope": {"kind": "Catalog", "name": "shared"}},
        "spec": {"backend": "vm"},
    }
    await ManifestResourceService(database).apply(
        json.dumps(template), actor, format="json"
    )
    project_id = await _project(database)
    edited = {"ref": {"name": "edited", "scope": {"kind": "Catalog", "name": "shared"}}}
    await save_settings_defaults(
        database,
        project_id,
        ProjectDefaults(jobs="container", container=edited),
        actor_id=str(actor["id"]),
    )
    with pytest.raises(HTTPException) as refused:
        await select_execution_workspace(
            database, actor, project_id=project_id, role="worker"
        )
    assert refused.value.status_code == 409
    assert (
        refused.value.detail == "The container template must be a container workspace."
    )


@pytest.mark.asyncio
async def test_a_caller_cannot_supply_the_workspace_sources(database, actor):
    forged = {"tier": "project", "template": "project", "template_name": "forged"}
    job = await database.create_job(
        "forged",
        user_id=str(actor["id"]),
        context={"workspace_sources": forged, "keep": 1},
    )
    context = _json((await database.get_job(str(job["id"])))["context"])
    assert "workspace_sources" not in context and context["keep"] == 1
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        initial_metadata={"workspace_sources": forged},
    )
    metadata = _json((await database.get_thread(thread_id))["metadata"])
    assert "workspace_sources" not in metadata


CHART_IMAGE = "ghcr.io/superhuman-remote-worker/srw-workspace:1.4.0"
CHART_SIZES = {"cpu": 2, "memory": "4Gi", "requests": {"cpu": 0.5, "memory": "1Gi"}}


def _chart_builtins() -> list[dict]:
    """What helm/templates/_helpers.tpl renders with the shipped values."""
    scope = {"kind": "Catalog", "name": "shared"}

    def template(name, spec):
        return {
            "apiVersion": "srw/v1alpha1",
            "kind": "WorkspaceTemplate",
            "metadata": {"name": name, "scope": scope},
            "spec": spec,
        }

    return [
        template("virtual", {"backend": "virtual"}),
        template(
            "container-minimal",
            {
                "backend": "sandbox",
                "environment": {
                    "image": CHART_IMAGE.replace(
                        "srw-workspace", "srw-workspace-minimal"
                    )
                },
                "resources": CHART_SIZES,
            },
        ),
        template(
            "container-full",
            {
                "backend": "sandbox",
                "environment": {"image": CHART_IMAGE},
                "resources": CHART_SIZES,
            },
        ),
    ]


@pytest.mark.asyncio
async def test_the_shipped_configuration_with_the_chart_builtins(database, monkeypatch):
    from orchestrator.services.builtin_workspace_templates import (
        reconcile_builtin_workspace_templates,
    )

    declared = _chart_builtins()
    await reconcile_builtin_workspace_templates(database, declared)
    monkeypatch.setenv("WORKSPACE_BUILTIN_TEMPLATES", json.dumps(declared))
    user = dict(
        await database.fetchrow(
            "INSERT INTO users(display_name,is_approved,is_admin) VALUES('Member',TRUE,FALSE) RETURNING *"
        )
    )

    config, receipt = await select_execution_workspace(
        database, user, project_id=None, role="worker"
    )
    assert config["backend"] == "sandbox"
    assert config["sandbox"]["image"] == CHART_IMAGE
    assert {key: config["sandbox"][key] for key in CHART_SIZES} == CHART_SIZES
    assert receipt["sources"] == {"tier": "installation", "template": "builtin"}
    assert receipt["template_name"] == "container-full"

    session, _ = await select_execution_workspace(
        database, user, project_id=None, role="session"
    )
    assert session == {"backend": "virtual"}

    # container-full retired (soft-deleted) while the chart still declares it.
    await reconcile_builtin_workspace_templates(
        database,
        [doc for doc in declared if doc["metadata"]["name"] != "container-full"],
    )
    with pytest.raises(HTTPException) as refused:
        await select_execution_workspace(database, user, project_id=None, role="worker")
    assert refused.value.status_code == 409
    assert refused.value.detail == (
        "The built-in container template 'container-full' is missing; see the orchestrator's startup log."
    )


@pytest.mark.asyncio
async def test_the_personal_project_row_decides_a_session(database, actor):
    project_id = await _project(database)
    await save_settings_defaults(
        database,
        project_id,
        ProjectDefaults(sessions="container"),
        actor_id=str(actor["id"]),
    )
    config, receipt = await select_execution_workspace(
        database, actor, project_id=project_id, role="session"
    )
    assert config["backend"] == "sandbox"
    assert receipt["sources"] == {"tier": "project", "template": "builtin"}


# R15: a manifest-owned row holds only while its revision is the Project's
# active one. Every writer that moves the revision syncs it, and a row left
# behind (a missed writer, a pre-A2b pod in a rolling update) heals on read.
BOX = {"backend": "sandbox", "resources": {"cpu": 1, "memory": "2Gi"}}


async def _manifest_project(db, actor) -> str:
    from orchestrator.services.manifest_resources import ManifestResourceService

    document = {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {"name": "managed-team"},
        "spec": {
            "description": "Manifest-owned workspace defaults",
            "resources": {"workspaces": {"box": {"inline": deepcopy(BOX)}}},
            "defaults": {"workspace": {"jobs": "container", "container": "box"}},
        },
    }
    applied = await ManifestResourceService(db).apply(
        json.dumps(document), actor, format="json"
    )
    uid = next(
        item["uid"]
        for item in applied["resources"]
        if item["resource"]["kind"] == "Project"
    )
    return str(
        await db.fetchval("SELECT linked_id FROM srw_resources WHERE id=$1", UUID(uid))
    )


async def _active_revision(db, project_id) -> str:
    from orchestrator.services.manifest_projects import active_project_resource

    return (await active_project_resource(db, project_id))["revision"]


@pytest.mark.asyncio
async def test_an_officer_kit_edit_keeps_the_manifest_defaults_current(database, actor):
    from orchestrator.services.manifest_projects import persist_officer_controller
    from orchestrator.services.project_workspace_defaults import (
        read_project_defaults,
    )

    project_id = await _manifest_project(database, actor)
    before = await _active_revision(database, project_id)
    async with database.transaction_scope():
        await persist_officer_controller(
            database,
            project_id,
            {
                "config_override": {"officer": {"enabled": True}},
                "communication_policy": {},
            },
        )
    active = await _active_revision(database, project_id)
    assert active != before
    # The writer itself moved the row with the revision (no read needed).
    assert (await read_project_defaults(database, project_id)).manifest_revision == (
        active
    )

    config, receipt = await select_execution_workspace(
        database, actor, project_id=project_id, role="worker"
    )
    assert config == {"backend": "sandbox", "sandbox": {"cpu": 1, "memory": "2Gi"}}
    assert receipt["sources"] == {"tier": "project", "template": "project"}
    assert receipt["project_revision"] == active


@pytest.mark.asyncio
async def test_a_stale_manifest_row_heals_on_the_next_selection(database, actor):
    from orchestrator.services.manifest_workspace_selection import (
        select_generic_project_workspace,
    )
    from orchestrator.services.project_workspace_defaults import (
        read_project_defaults,
    )

    project_id = await _manifest_project(database, actor)
    active = await _active_revision(database, project_id)

    async def make_stale():
        await database.execute(
            "UPDATE project_workspace_defaults SET manifest_revision='sha256:stale' "
            "WHERE project_id=$1",
            UUID(project_id),
        )

    await make_stale()
    config, receipt = await select_execution_workspace(
        database, actor, project_id=project_id, role="worker"
    )
    assert config == {"backend": "sandbox", "sandbox": {"cpu": 1, "memory": "2Gi"}}
    assert receipt["project_revision"] == active
    stored = await read_project_defaults(database, project_id)
    assert (stored.source, stored.manifest_revision) == ("manifest", active)

    # The generic-image path heals the same way.
    await make_stale()
    generic = await select_generic_project_workspace(
        database, actor, project_id=project_id
    )
    assert generic["resolved"]["template"]["inline"]["backend"] == "sandbox"
    assert (await read_project_defaults(database, project_id)).manifest_revision == (
        active
    )


@pytest.mark.asyncio
async def test_a_manifest_row_without_its_project_manifest_is_released_on_read(
    database, actor
):
    from orchestrator.services.project_workspace_defaults import (
        read_project_defaults,
    )

    project_id = await _manifest_project(database, actor)
    # The Project resource goes away without releasing the row.
    await database.execute(
        "UPDATE srw_resources SET deleted_at=now() WHERE kind='Project' AND linked_id=$1",
        UUID(project_id),
    )
    config, receipt = await select_execution_workspace(
        database, actor, project_id=project_id, role="worker"
    )
    # The installation's Jobs mode and no Project layer.
    assert config == {"backend": "sandbox"}
    assert receipt["sources"] == {"tier": "installation", "template": "builtin"}
    assert receipt["project_revision"] is None
    released = await read_project_defaults(database, project_id)
    assert (released.source, released.manifest_revision, released.jobs) == (
        "settings",
        None,
        None,
    )
