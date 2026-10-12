"""Project generation fidelity, migration and authorization on isolated PostgreSQL."""

from copy import deepcopy
import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
from fastapi import HTTPException
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.manifest_experts import (
    bundled_expert_for_execution,
    expert_manifest,
    migrate_stored_experts,
    project_expert_resource,
    seed_bundled_expert_manifests,
)
from orchestrator.services.manifest_projects import (
    active_project_resource,
    compose_srw_expert,
    migrate_projects,
    project_document,
    project_expert_for_execution,
    validate_project_activation,
)
from orchestrator.services.manifest_resources import ManifestResourceService
from orchestrator.services.manifest_store import ManifestStore
from orchestrator.services.project_workspace_defaults import read_project_defaults
from shared.manifests import validate_documents
from shared.manifests.resolution import content_revision
from shared.runtime.core.expert_resolution import build_expert_config
from shared.runtime.core.srw_manifest_config import srw_private_config

ROOT = Path(__file__).resolve().parents[1]
OWNER = UUID("00000000-0000-0000-0000-000000000011")
PROJECT = UUID("00000000-0000-0000-0000-000000000012")
EXPERT = UUID("00000000-0000-0000-0000-000000000013")


def expert_source(role="worker", **changes):
    raw = {
        "id": EXPERT,
        "name": "builder",
        "display_name": "Builder",
        "expert_type": role,
        "owner_id": OWNER,
        "is_global": False,
        "icon": "build",
        "color": "#123456",
        "tags": [role],
        "config": {"settings": {"expert": True}},
        "prompts": {"persona": "Keep this persona."},
        "default_for": role,
        "config_override": {"settings": {"last": True}},
        **changes,
    }
    document = expert_manifest(raw, image="trusted/srw:v1")
    return project_expert_resource(
        raw,
        {
            "id": uuid4(),
            "document": document,
            "revision": content_revision(document["spec"]),
            "resource_version": 1,
        },
    )


def test_project_migration_preserves_ordered_deletions_and_account_defaults():
    source = expert_source()
    shared = {"settings": None}
    project = {"id": PROJECT, "name": "My team", "default_config_override": shared}
    document, recipe = project_document(project, owner_id=OWNER, experts=[source])
    validate_documents([document])
    alias = document["spec"]["defaults"]["expert"]
    spec = document["spec"]["resources"]["experts"][alias]["inline"]
    private = spec["runtime"]["config"]
    assert private["layers"] == [shared, source["config_override"]]
    assert private["layers"][0]["settings"] is None
    assert private["prompts"] == source["prompts"]
    for model in ("account-a-model", "account-b-model"):
        result, _ = build_expert_config(
            {"settings": {"base": True}, "llm": {"model": model}},
            {**source, "harness_config_layers": private["layers"]},
        )
        assert result["settings"] == {"last": True}
        assert result["llm"]["model"] == model
    session = recipe["experts"][str(EXPERT)]["aliases"]["session"]
    assert document["spec"]["resources"]["experts"][session]["inline"]["runtime"][
        "config"
    ]["layers"] == [source["config_override"]]


@pytest.mark.parametrize("layers", [{}, [None], ["not a layer"]])
def test_srw_layers_require_an_ordered_list_of_objects(layers):
    document = expert_source()["manifest"]
    document["spec"]["runtime"]["config"]["layers"] = layers
    # Public harness configuration remains opaque; its chosen adapter validates it.
    validate_documents([document])
    with pytest.raises(ValueError, match="array of objects"):
        srw_private_config(document)


def test_generic_project_composition_does_not_merge_private_settings():
    document = expert_source()["manifest"]
    document["spec"]["runtime"].pop("adapter")
    document["spec"]["runtime"]["config"] = {"layers": None, "arbitrary": [None]}
    assert compose_srw_expert(document) == document
    with pytest.raises(HTTPException) as error:
        compose_srw_expert(document, shared={"tools": {"workspace": []}})
    assert error.value.status_code == 422


def test_project_connector_availability_does_not_expand_historical_autoattach():
    project = {"id": PROJECT, "name": "Team"}
    document, _ = project_document(
        project,
        owner_id=OWNER,
        experts=[],
        datasources=[
            {"id": uuid4(), "policy_revision": 7},
            {"id": uuid4(), "policy_revision": 9},
        ],
    )
    validate_documents([document])
    assert len(document["spec"]["resources"]["connectors"]) == 2
    assert "connectors" not in document["spec"].get("defaults", {})


def test_officer_migration_keeps_kit_and_excludes_incarnation_state():
    project = {"id": PROJECT, "name": "Team"}
    kit = {
        "officer": {
            "slots": {"line": {"count": 2, "model": "m", "backend": "sandbox"}},
            "auto_pull": False,
            "hold": {"kind": "maintenance"},
            "last_respawn_at": "now",
        }
    }
    document, _ = project_document(
        project,
        owner_id=OWNER,
        experts=[],
        post={
            "config_override": kit,
            "communication_policy": {"worker_messages": "user_direct"},
            "is_active": True,
            "thread_id": uuid4(),
            "state": {"privateRuntime": True},
        },
    )
    validate_documents([document])
    team = document["spec"]["team"]
    assert team["state"] == "Active"
    assert "jobPolicy" not in team
    private = team["controller"]["config"]
    assert private["config"]["officer"]["slots"] == kit["officer"]["slots"]
    assert "hold" not in private["config"]["officer"]
    assert "last_respawn_at" not in private["config"]["officer"]
    assert "thread_id" not in private and "state" not in private


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:15") as pg:
        yield pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def schema_applied(pg_dsn):
    connection = await asyncpg.connect(pg_dsn)
    try:
        await connection.execute(
            (ROOT / "src/orchestrator/database/schema_current.sql").read_text()
        )
        await connection.execute("SET search_path TO public")
        if not await connection.fetchval("SELECT to_regclass('srw_resources')"):
            await connection.execute(
                (
                    ROOT
                    / "src/orchestrator/database/migrations/app/0234_manifest_resources.sql"
                ).read_text()
            )
    finally:
        await connection.close()


@pytest_asyncio.fixture
async def db(pg_dsn, schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    try:
        await store.execute("TRUNCATE srw_resources,experts,users,projects CASCADE")
        yield store
    finally:
        await store.close()


async def historical_project(db):
    await db.execute(
        """INSERT INTO projects(id,name,default_config_name,default_config_override)
        VALUES($1,'Legacy team','worker_base',$2::jsonb)""",
        PROJECT,
        json.dumps({"settings": None}),
    )
    await db.execute(
        "INSERT INTO users(id,display_name,default_project_id) VALUES($1,'Owner',$2)",
        OWNER,
        PROJECT,
    )
    await db.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'owner')",
        PROJECT,
        OWNER,
    )
    await db.execute("INSERT INTO project_officers(project_id) VALUES($1)", PROJECT)
    await db.execute(
        """INSERT INTO experts(id,name,display_name,expert_type,owner_id,config,prompts)
        VALUES($1,'legacy-helper','Legacy helper','worker',$2,$3::jsonb,$4::jsonb)""",
        EXPERT,
        OWNER,
        json.dumps({"settings": {"expert": True}}),
        json.dumps({"persona": "Frozen persona."}),
    )
    await db.execute(
        """INSERT INTO project_experts(project_id,expert_id,default_for,config_override)
        VALUES($1,$2,'worker',$3::jsonb)""",
        PROJECT,
        EXPERT,
        json.dumps({"settings": {"last": True}}),
    )
    await migrate_stored_experts(db, image="trusted/srw:v1")


@pytest.mark.asyncio
async def test_unclaimed_projects_do_not_block_migration_or_gain_an_owner(db):
    await historical_project(db)
    orphan = uuid4()
    await db.execute(
        "INSERT INTO projects(id,name,is_default,default_config_override) "
        "VALUES($1,'Unclaimed legacy Project',TRUE,$2::jsonb)",
        orphan,
        json.dumps({"settings": {"preserve": True}}),
    )
    before = await db.fetchval("SELECT to_jsonb(p) FROM projects p WHERE id=$1", orphan)

    assert await migrate_projects(db) == {
        "migrated": 1,
        "preserved": 0,
        "deferred": 1,
    }
    assert await active_project_resource(db, PROJECT)
    assert await ManifestStore(db).by_link("Project", orphan) is None
    assert (
        await db.fetchval("SELECT to_jsonb(p) FROM projects p WHERE id=$1", orphan)
        == before
    )
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM project_members WHERE project_id=$1)", orphan
    )
    assert await migrate_projects(db) == {
        "migrated": 0,
        "preserved": 1,
        "deferred": 1,
    }

    # An explicit owner assignment makes this Project eligible, without
    # converting an unrelated member or administrator into its owner.
    await db.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'owner')",
        orphan,
        OWNER,
    )
    assert await migrate_projects(db) == {
        "migrated": 1,
        "preserved": 1,
        "deferred": 0,
    }
    recovered = await active_project_resource(db, orphan)
    assert recovered["owner_id"] == OWNER
    assert (await db.get_project(str(orphan)))["default_config_override"] == {
        "settings": {"preserve": True}
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("reference", ("member", "user-default"))
async def test_claimed_ownerless_project_still_requires_explicit_ownership(
    db, reference
):
    await historical_project(db)
    orphan = uuid4()
    await db.execute(
        "INSERT INTO projects(id,name) VALUES($1,'Needs ownership repair')", orphan
    )
    if reference == "member":
        await db.execute(
            "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'viewer')",
            orphan,
            OWNER,
        )
    else:
        await db.execute(
            "UPDATE users SET default_project_id=$1 WHERE id=$2", orphan, OWNER
        )
    with pytest.raises(HTTPException, match="needs an owner") as error:
        await migrate_projects(db)
    assert error.value.status_code == 409
    assert await ManifestStore(db).by_link("Project", PROJECT) is None
    assert await ManifestStore(db).by_link("Project", orphan) is None
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM project_members WHERE project_id=$1 AND role='owner')",
        orphan,
    )


@pytest.mark.asyncio
async def test_project_migration_is_canonical_repeatable_and_freezes_source_content(db):
    await historical_project(db)
    assert await migrate_projects(db) == {"migrated": 1, "preserved": 0, "deferred": 0}
    before = await active_project_resource(db, PROJECT)
    raw = await db.fetchrow("SELECT * FROM projects WHERE id=$1", PROJECT)
    assert raw["manifest_resource_id"] == before["id"]
    assert raw["default_config_name"] is raw["default_config_override"] is None
    assert (
        await db.fetchval(
            "SELECT config_override FROM project_experts WHERE project_id=$1 AND expert_id=$2",
            PROJECT,
            EXPERT,
        )
        is None
    )
    projected = await db.get_project(str(PROJECT))
    assert projected["default_config_override"] == {"settings": None}
    assert projected["manifest_composed"] is True
    default = await db.get_project_default_expert(
        project_id=str(PROJECT), expert_type="worker"
    )
    assert str(default["id"]) == str(EXPERT)
    assert default["harness_config_layers"] == [
        {"settings": None},
        {"settings": {"last": True}},
    ]
    await db.update_expert(
        str(EXPERT),
        updated_by=str(OWNER),
        config={"settings": {"changed": True}},
        prompts={"persona": "New source persona."},
    )
    frozen = await project_expert_for_execution(db, PROJECT, EXPERT)
    assert frozen["config"] == {"settings": {"expert": True}}
    assert frozen["prompts"] == {"persona": "Frozen persona."}
    assert frozen["project_dependency"]["revision"] == before["revision"]
    await db.update_project(str(PROJECT), name="Renamed team")
    renamed = await project_expert_for_execution(db, PROJECT, EXPERT)
    assert renamed["config"] == frozen["config"]
    assert renamed["prompts"] == frozen["prompts"]
    assert await migrate_projects(db) == {"migrated": 0, "preserved": 1, "deferred": 0}
    assert (await active_project_resource(db, PROJECT))["revision"] == before[
        "revision"
    ]


@pytest.mark.asyncio
async def test_project_default_edit_activates_complete_rebuild_and_clears_shadow_payload(
    db,
):
    await historical_project(db)
    await migrate_projects(db)
    first = await active_project_resource(db, PROJECT)
    await db.set_project_default_expert(
        project_id=str(PROJECT),
        expert_id=str(EXPERT),
        expert_type="worker",
        actor_user_id=str(OWNER),
        config_override={"settings": {"new": True}},
    )
    second = await active_project_resource(db, PROJECT)
    assert second["id"] == first["id"]
    assert second["revision"] != first["revision"]
    assert second["active_revision"] == second["revision"]
    link = await db.get_project_expert_link(
        project_id=str(PROJECT), expert_id=str(EXPERT)
    )
    assert link["config_override"] == {"settings": {"new": True}}
    assert (
        await db.fetchval(
            "SELECT config_override FROM project_experts WHERE project_id=$1 AND expert_id=$2",
            PROJECT,
            EXPERT,
        )
        is None
    )
    await db.update_project(
        str(PROJECT), default_config_override={"settings": {"shared": True}}
    )
    frozen = await project_expert_for_execution(db, PROJECT, EXPERT)
    assert frozen["harness_config_layers"] == [
        {"settings": {"shared": True}},
        {"settings": {"new": True}},
    ]
    await db.clear_project_default_expert(
        project_id=str(PROJECT), expert_type="worker", actor_user_id=str(OWNER)
    )
    assert (
        await db.get_project_default_expert(
            project_id=str(PROJECT), expert_type="worker"
        )
        is None
    )
    assert "expert" not in (await active_project_resource(db, PROJECT))["document"][
        "spec"
    ].get("defaults", {})


@pytest.mark.asyncio
async def test_user_and_project_birth_publish_manifests(db):
    owner, default = await db.create_user_with_default_project("Owner")
    resource = await active_project_resource(db, default["id"])
    assert resource and resource["owner_id"] == owner["id"]
    project = await db.create_project("Another project")
    await db.add_project_member(str(project["id"]), str(owner["id"]), "owner")
    assert await active_project_resource(db, project["id"])
    restored = await db.create_default_project_for_user(str(owner["id"]), "Owner")
    assert await active_project_resource(db, restored["id"])
    jit = await db.upsert_user_from_oidc(
        "test-subject", "test@example.invalid", "OIDC user"
    )
    assert await active_project_resource(db, jit["default_project_id"])


@pytest.mark.asyncio
async def test_project_activation_rejects_editor_officer_privilege_and_unimplemented_commission(
    db,
):
    await historical_project(db)
    await migrate_projects(db)
    resource = await active_project_resource(db, PROJECT)
    document = deepcopy(resource["document"])
    document["spec"]["team"] = {
        "state": "Active",
        "controller": {
            "type": "srw/officer-v1",
            "config": {"config": {}, "communicationPolicy": {}},
        },
    }
    prepared = {"project_id": str(PROJECT), "resolved": document}
    stranger = uuid4()
    await db.execute(
        "INSERT INTO users(id,display_name,default_project_id) VALUES($1,'Editor',$2)",
        stranger,
        PROJECT,
    )
    await db.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'editor')",
        PROJECT,
        stranger,
    )
    before = await db.fetchval("SELECT COUNT(*) FROM srw_resource_revisions")
    with pytest.raises(HTTPException) as denied:
        await validate_project_activation(
            db, prepared, {"id": stranger}, validate_only=True
        )
    assert denied.value.status_code == 403
    # Match the existing post's explicit default communication policy so this
    # specifically exercises the absence of a commissioning lifecycle mapping.
    post = await db.fetchrow(
        "SELECT communication_policy FROM project_officers WHERE project_id=$1", PROJECT
    )
    document["spec"]["team"]["controller"]["config"]["communicationPolicy"] = (
        json.loads(post["communication_policy"])
    )
    with pytest.raises(HTTPException, match="Commission") as unsupported:
        await validate_project_activation(
            db, prepared, {"id": OWNER}, validate_only=True
        )
    assert unsupported.value.status_code == 422
    assert await db.fetchval("SELECT COUNT(*) FROM srw_resource_revisions") == before


@pytest.mark.asyncio
async def test_invalid_project_rebuild_rolls_back_default_pointer_and_audit(db):
    await historical_project(db)
    await migrate_projects(db)
    before = await active_project_resource(db, PROJECT)
    audit_count = await db.fetchval("SELECT COUNT(*) FROM expert_default_audit")
    with pytest.raises(ValueError, match="JSON object"):
        await db.set_project_default_expert(
            project_id=str(PROJECT),
            expert_id=str(EXPERT),
            expert_type="worker",
            actor_user_id=str(OWNER),
            config_override=["invalid"],
        )
    after = await active_project_resource(db, PROJECT)
    assert after["resource_version"] == before["resource_version"]
    assert await db.fetchval("SELECT COUNT(*) FROM expert_default_audit") == audit_count


@pytest.mark.asyncio
async def test_project_cannot_remove_its_controller_by_substituting_empty_held_team(db):
    await historical_project(db)
    await migrate_projects(db)
    resource = await active_project_resource(db, PROJECT)
    assert resource["document"]["spec"]["team"]["controller"]
    document = deepcopy(resource["document"])
    document["spec"]["team"] = {"state": "Held"}
    prepared = {"project_id": str(PROJECT), "resolved": document}
    before = await db.fetchval("SELECT COUNT(*) FROM srw_resource_revisions")
    with pytest.raises(HTTPException, match="Removing a team controller") as refused:
        await validate_project_activation(
            db, prepared, {"id": OWNER}, validate_only=True
        )
    assert refused.value.status_code == 422
    assert await db.fetchval("SELECT COUNT(*) FROM srw_resource_revisions") == before


@pytest.mark.asyncio
async def test_migration_refuses_legacy_link_payload_written_after_canonical_cutover(
    db,
):
    await historical_project(db)
    await migrate_projects(db)
    before = await active_project_resource(db, PROJECT)
    stale = {"llm": {"model": "legacy-writer-value"}}
    await db.execute(
        "UPDATE project_experts SET config_override=$3::jsonb WHERE project_id=$1 AND expert_id=$2",
        PROJECT,
        EXPERT,
        json.dumps(stale),
    )
    with pytest.raises(RuntimeError, match="legacy defaults or link overrides"):
        await migrate_projects(db)
    assert await active_project_resource(db, PROJECT) == before
    assert (
        json.loads(
            await db.fetchval(
                "SELECT config_override FROM project_experts WHERE project_id=$1 AND expert_id=$2",
                PROJECT,
                EXPERT,
            )
        )
        == stale
    )
    assert (
        await db.get_project_expert_link(project_id=str(PROJECT), expert_id=str(EXPERT))
    )["config_override"] == {"settings": {"last": True}}


@pytest.mark.asyncio
async def test_native_project_can_borrow_bundled_catalog_default_without_copying_source(
    db,
):
    owner, _ = await db.create_user_with_default_project("Owner")
    await db.execute("UPDATE users SET is_approved=TRUE WHERE id=$1", owner["id"])
    actor = {"id": owner["id"], "is_approved": True, "is_admin": False}
    await seed_bundled_expert_manifests(db, ROOT / "config", image="trusted/srw:v1")
    source = await ManifestStore(db).by_name(
        "Expert", {"kind": "Catalog", "name": "shared"}, "developer"
    )
    assert source["linked_id"] is None
    original_config = deepcopy(source["document"]["spec"]["runtime"]["config"])
    service = ManifestResourceService(db)
    document = {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {"name": "new-team"},
        "spec": {
            "resources": {
                "experts": {
                    "builder": {
                        "ref": {
                            "name": "developer",
                            "scope": {"kind": "Catalog", "name": "shared"},
                        }
                    }
                }
            },
            "defaults": {"expert": "builder"},
        },
    }
    result = await service.apply(json.dumps([document]), actor, format="json")
    project_resource = await ManifestStore(db).by_id(result["resources"][0]["uid"])
    project_id = project_resource["linked_id"]
    default = await db.get_project_default_expert(
        project_id=str(project_id), expert_type="worker"
    )
    source_after = await ManifestStore(db).by_id(source["id"])
    assert source_after["linked_id"] == UUID(str(default["id"]))
    assert source_after["document"] == source["document"]
    assert source_after["owner_id"] is None
    assert default["is_global"] is True
    assert default["harness_config_name"] == "worker_base"
    assert default["harness_asset_name"] == "developer"
    assert default["config"] == original_config["config"]
    assert (
        await ManifestStore(db).by_name(
            "Expert", {"kind": "Project", "name": str(project_id)}, "builder"
        )
        is None
    )
    assert (
        "ref" in project_resource["document"]["spec"]["resources"]["experts"]["builder"]
    )
    changed = deepcopy(source["document"])
    changed["spec"]["runtime"]["config"]["config"]["manifest_freeze_marker"] = (
        "new source"
    )
    async with db.transaction_scope():
        store = ManifestStore(db)
        await store.lock_catalog()
        await store.lock_identity(changed)
        await store.save(
            changed,
            deepcopy(changed),
            content_revision(changed["spec"]),
            [],
            owner_id=None,
            expected_version=source_after["resource_version"],
        )
    frozen = await project_expert_for_execution(db, project_id, default["id"])
    assert "manifest_freeze_marker" not in frozen["config"]
    assert frozen["manifest_revision"] == source["revision"]
    document["metadata"]["name"] = "second-team"
    await service.apply(json.dumps([document]), actor, format="json")
    assert (await ManifestStore(db).by_id(source["id"]))["linked_id"] == source_after[
        "linked_id"
    ]
    assert (
        await db.fetchval(
            "SELECT COUNT(*) FROM experts WHERE manifest_resource_id=$1", source["id"]
        )
        == 1
    )


@pytest.mark.asyncio
async def test_native_inline_session_default_gets_project_owned_primary_role(db):
    owner, _ = await db.create_user_with_default_project("Owner")
    actor = {"id": owner["id"], "is_approved": True, "is_admin": False}
    document = {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {"name": "session-team"},
        "spec": {
            "resources": {
                "experts": {
                    "assistant": {"inline": {"runtime": {"image": "custom/session:v1"}}}
                }
            },
            "defaults": {"sessionExpert": "assistant"},
        },
    }
    result = await ManifestResourceService(db).apply(
        json.dumps([document]), actor, format="json"
    )
    project = next(
        item for item in result["resources"] if item["resource"]["kind"] == "Project"
    )
    row = await ManifestStore(db).by_id(project["uid"])
    default = await db.get_project_default_expert(
        project_id=str(row["linked_id"]), expert_type="session"
    )
    assert default["expert_type"] == "session"
    assert default["harness_adapter"] is None
    assert default["manifest"]["spec"]["runtime"]["image"] == "custom/session:v1"


@pytest.mark.asyncio
async def test_legacy_officer_post_edit_publishes_project_controller_atomically(db):
    await historical_project(db)
    await migrate_projects(db)
    before = await active_project_resource(db, PROJECT)
    result = await db.update_project_officer_post(
        str(PROJECT), config_updates={"officer": {"max_concurrent_workers": 3}}
    )
    after = await active_project_resource(db, PROJECT)
    assert after["revision"] != before["revision"]
    private = after["resolved"]["spec"]["team"]["controller"]["config"]
    assert private["config"] == result["post"]["config_override"]
    assert private["communicationPolicy"] == result["post"]["communication_policy"]


@pytest.mark.asyncio
async def test_composed_expert_copy_keeps_ordered_private_layers(db):
    owner, _ = await db.create_user_with_default_project("Owner")
    layers = [{"settings": None}, {"settings": {"last": True}}]
    expert = await db.create_expert(
        name="composed",
        display_name="Composed",
        expert_type="worker",
        owner_id=str(owner["id"]),
        config={"settings": {"first": True}},
        prompts={"persona": "Frozen persona."},
        srw_layers=layers,
    )
    from orchestrator.services.expert_catalog import db_expert_to_bundle_src

    source = db_expert_to_bundle_src(expert)
    copied = await db.fork_and_set_user_expert_default(
        user_id=str(owner["id"]), expert_type="worker", source=source
    )
    assert copied["harness_config_layers"] == layers
    resolved, _ = build_expert_config({"settings": {"base": True}}, copied)
    assert resolved["settings"] == {"last": True}


@pytest.mark.asyncio
async def test_expert_resource_tombstone_is_not_resurrected_by_startup_migration(db):
    owner, _ = await db.create_user_with_default_project("Owner")
    expert = await db.create_expert(
        name="temporary",
        display_name="Temporary",
        expert_type="worker",
        owner_id=str(owner["id"]),
        config={"settings": {"keep": None}},
    )
    store = ManifestStore(db)
    resource = await store.by_link("Expert", expert["id"])
    await store.delete(resource, expected_version=resource["resource_version"])
    count = await db.fetchval("SELECT COUNT(*) FROM srw_resources")
    assert await db.get_expert_by_id(str(expert["id"])) is None
    assert await migrate_stored_experts(db) == {"migrated": 0, "preserved": 1}
    assert await db.fetchval("SELECT COUNT(*) FROM srw_resources") == count
    assert await store.by_link("Expert", expert["id"]) is None
    assert await db.get_expert_by_id(str(expert["id"])) is None


@pytest.mark.asyncio
async def test_deleted_bundled_catalog_resource_stays_retired_after_import(db):
    await seed_bundled_expert_manifests(db, ROOT / "config", image="trusted/srw:v1")
    store = ManifestStore(db)
    resource = await store.by_name(
        "Expert", {"kind": "Catalog", "name": "shared"}, "developer"
    )
    await store.delete(resource, expected_version=resource["resource_version"])
    count = await db.fetchval("SELECT COUNT(*) FROM srw_resources")
    result = await seed_bundled_expert_manifests(
        db, ROOT / "config", image="trusted/srw:v2"
    )
    assert result["added"] == 0
    assert await db.fetchval("SELECT COUNT(*) FROM srw_resources") == count
    assert (
        await store.by_name(
            "Expert", {"kind": "Catalog", "name": "shared"}, "developer"
        )
        is None
    )


@pytest.mark.asyncio
async def test_native_resource_delete_cannot_remove_a_personal_default_expert(db):
    owner, _ = await db.create_user_with_default_project("Owner")
    expert = await db.create_expert(
        name="default-helper",
        display_name="Default helper",
        expert_type="worker",
        owner_id=str(owner["id"]),
    )
    await db.set_user_expert_default(
        user_id=str(owner["id"]), expert_type="worker", expert_id=str(expert["id"])
    )
    store = ManifestStore(db)
    resource = await store.by_link("Expert", expert["id"])
    with pytest.raises(HTTPException) as blocked:
        await store.delete(resource, expected_version=resource["resource_version"])
    assert blocked.value.status_code == 409
    assert await store.by_id(resource["id"])
    default = await db.get_user_expert_default(
        user_id=str(owner["id"]), expert_type="worker"
    )
    assert default["id"] == expert["id"]


@pytest.mark.asyncio
async def test_persist_project_resource_early_branch_resyncs_workspace_defaults_revision(
    db,
):
    owner, _ = await db.create_user_with_default_project("Owner")
    service = ManifestResourceService(db)
    workspace = {
        "apiVersion": "srw/v1alpha1",
        "kind": "WorkspaceTemplate",
        "metadata": {"name": "box"},
        "spec": {"backend": "vm"},
    }
    await service.apply(json.dumps(workspace), owner, format="json")
    project_doc = {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {"name": "native-team"},
        "spec": {
            "description": "first",
            "resources": {
                "workspaces": {
                    "box": {
                        "ref": {
                            "name": "box",
                            "scope": {"kind": "Account", "name": str(owner["id"])},
                        }
                    }
                }
            },
            "defaults": {"workspace": {"jobs": "vm", "vm": "box"}},
        },
    }
    applied = await service.apply(json.dumps(project_doc), owner, format="json")
    project_row = next(
        item for item in applied["resources"] if item["resource"]["kind"] == "Project"
    )
    project_id = await db.fetchval(
        "SELECT linked_id FROM srw_resources WHERE id=$1", UUID(project_row["uid"])
    )
    before = await read_project_defaults(db, project_id)
    assert before.manifest_revision == project_row["revision"]

    # A native Project's early rebuild branch: only description/display-name
    # change, but that still hashes into a new active_revision. This is the
    # real production trigger (PostgresDB.update_project()).
    assert await db.update_project(str(project_id), description="second")
    updated = await ManifestStore(db).by_link("Project", project_id)
    assert updated["revision"] != project_row["revision"]
    assert updated["active_revision"] == updated["revision"]

    after = await read_project_defaults(db, project_id)
    assert after.jobs == "vm"
    assert after.manifest_revision == updated["revision"]


@pytest.mark.asyncio
async def test_explicit_bundled_selection_outranks_project_default_and_keeps_shared_layer(
    db,
):
    await historical_project(db)
    await migrate_projects(db)
    await seed_bundled_expert_manifests(db, ROOT / "config", image="trusted/srw:v1")
    chosen = await project_expert_for_execution(
        db, PROJECT, role="worker", config_name="developer"
    )
    source = await ManifestStore(db).by_name(
        "Expert", {"kind": "Catalog", "name": "shared"}, "developer"
    )
    assert chosen["harness_config_name"] == "worker_base"
    assert chosen["harness_asset_name"] == "developer"
    assert chosen["manifest_uid"] == str(source["id"])
    assert chosen["config"] == source["document"]["spec"]["runtime"]["config"]["config"]
    assert chosen["harness_config_layers"] == [{"settings": None}]
    assert chosen["prompts"] != {"persona": "Frozen persona."}


@pytest.mark.asyncio
async def test_base_only_project_selection_retains_shared_layer_without_a_typed_default(
    db,
):
    await historical_project(db)
    await migrate_projects(db)
    await db.clear_project_default_expert(
        project_id=str(PROJECT), expert_type="worker", actor_user_id=str(OWNER)
    )
    chosen = await project_expert_for_execution(
        db, PROJECT, role="worker", config_name="worker_base"
    )
    assert chosen["config"] == {}
    assert chosen["harness_config_name"] == "worker_base"
    assert chosen["harness_config_layers"] == [{"settings": None}]


@pytest.mark.asyncio
async def test_canonical_catalog_edit_is_the_only_leaf_and_copy_keeps_assets(db):
    from orchestrator.services.config_resolver import resolve_config
    from orchestrator.services.expert_authoring import ExpertAuthoringService
    from orchestrator.services.expert_catalog import db_expert_to_bundle_src

    owner, _ = await db.create_user_with_default_project("Owner")
    await seed_bundled_expert_manifests(db, ROOT / "config", image="trusted/srw:v1")
    store = ManifestStore(db)
    resource = await store.by_name(
        "Expert", {"kind": "Catalog", "name": "shared"}, "developer"
    )
    document = deepcopy(resource["document"])
    private = document["spec"]["runtime"]["config"]
    assert private["config"]["llm"].pop("reasoning_level") == "high"
    await ManifestResourceService(db).apply(
        json.dumps(document),
        {"id": owner["id"], "is_admin": True},
        format="json",
        expected_versions={
            "Expert/Catalog/shared/developer": resource["resource_version"]
        },
    )
    selected = await bundled_expert_for_execution(db, "developer")
    captured = {}
    resolved = resolve_config(
        base_config_name="worker_base",
        expert_row=selected,
        expert_type="worker",
        base_defaults={"llm": {"reasoning_level": "low"}},
        capture=captured,
    )
    assert captured["merged_fragment"]["llm"]["reasoning_level"] == "low"
    assert selected["harness_asset_name"] == "developer"
    authoring = ExpertAuthoringService(
        store=db, catalog=None, resolve_default_models=None, prefetch_roster_refs=None
    )
    copied = await authoring.create_forked_expert(
        db_expert_to_bundle_src(selected), str(owner["id"])
    )
    assert copied["harness_config_name"] == "worker_base"
    assert copied["harness_asset_name"] == "developer"
    forked = resolve_config(
        base_config_name="worker_base", expert_row=copied, expert_type="worker"
    )
    assert forked["prompts"] == resolved["prompts"]


@pytest.mark.asyncio
async def test_named_roster_uses_canonical_catalog_and_cannot_restore_a_retired_source(
    db,
):
    from types import SimpleNamespace
    from orchestrator.services.session_config_resolution import prefetch_roster_refs
    from shared.runtime.core.subagent_roster import resolve_subagent_roster

    owner, _ = await db.create_user_with_default_project("Owner")
    await seed_bundled_expert_manifests(db, ROOT / "config", image="trusted/srw:v1")
    store = ManifestStore(db)
    resource = await store.by_name(
        "Expert", {"kind": "Catalog", "name": "shared"}, "subagent-reader"
    )
    document = deepcopy(resource["document"])
    private = document["spec"]["runtime"]["config"]
    private["config"]["llm"]["model"] = "gpt-4o"
    private["prompts"] = {"persona": "The saved Catalog persona."}
    await ManifestResourceService(db).apply(
        json.dumps(document),
        {"id": owner["id"], "is_admin": True},
        format="json",
        expected_versions={
            "Expert/Catalog/shared/subagent-reader": resource["resource_version"]
        },
    )
    layer = {"subagents": {"roster": {"lookup": {"$ref": "reader"}}}}
    refs = await prefetch_roster_refs(
        overrides=[layer],
        user_id=str(owner["id"]),
        dependencies=SimpleNamespace(store=db),
    )
    result = resolve_subagent_roster(
        {"llm": {"model": "claude-opus-4-1"}, **layer}, db_refs=refs
    )
    entry = result["subagents"]["roster"]["lookup"]
    assert entry["llm"]["model"] == "gpt-4o"
    assert entry["prompts"]["persona"] == "The saved Catalog persona."
    assert entry["_deployment_dir"] == "config/subagents/reader"
    current = await store.by_id(resource["id"])
    await store.delete(current, expected_version=current["resource_version"])
    retired = await prefetch_roster_refs(
        overrides=[layer],
        user_id=str(owner["id"]),
        dependencies=SimpleNamespace(store=db),
    )
    assert retired == {"reader": {}}
    result = resolve_subagent_roster(
        {"llm": {"model": "claude-opus-4-1"}, **layer}, db_refs=retired
    )
    assert "lookup" not in result["subagents"]["roster"]
