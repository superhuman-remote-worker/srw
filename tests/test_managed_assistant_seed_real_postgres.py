"""Managed assistant repair is conditional on the live row, not a stale read."""

import asyncio
from copy import deepcopy
import json
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.default_experts import (
    MANAGED_SEEDS,
    load_seed_bundle,
    seed_managed_default_experts,
    upgrade_managed_seed,
)


ROOT = Path(__file__).resolve().parents[1]
SETUP_GUIDANCE_MIGRATION = (
    ROOT / "src/orchestrator/database/migrations/app/0229_assistant_setup_guidance.sql"
)
PREVIOUS_PERSONA = (ROOT / "tests/fixtures/assistant_persona_v3.txt").read_text()
KEY = "application-default-session-seed"
SPEC = next(spec for spec in MANAGED_SEEDS if spec["managed_key"] == KEY)
OLD_ROSTER = {
    "default": "explorer",
    "roster": {
        name: {"$ref": f"subagents/{name}"}
        for name in ("explorer", "reader", "implementer")
    },
}
#: Seed 3's roster: the implementer pinned to no shell. Seed 4 lifts the pin,
#: so the shipped roster is OLD_ROSTER again and only a v3 row is repaired.
V3_ROSTER = deepcopy(OLD_ROSTER)
V3_ROSTER["roster"]["implementer"]["tools"] = {"shell": []}


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:15") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            (ROOT / "src/orchestrator/database/schema_current.sql").read_text()
        )
        await conn.execute("SET search_path TO public")
        if not await conn.fetchval("SELECT to_regclass('srw_resources')"):
            await conn.execute(
                (
                    ROOT
                    / "src/orchestrator/database/migrations/app/0234_manifest_resources.sql"
                ).read_text()
            )
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    try:
        await store.execute("TRUNCATE srw_resources, experts CASCADE")
        yield store
    finally:
        await store.close()


def _bundle():
    return load_seed_bundle(
        ROOT / "config", directory="assistant", expert_type="session"
    )


async def _legacy(db, *, roster=None, seed_version=2, prompts=None):
    bundle = _bundle()
    bundle["config"]["subagents"] = deepcopy(OLD_ROSTER if roster is None else roster)
    bundle["config"]["operator_extra"] = {"enabled": True}
    bundle["prompts"] = (
        {"persona": "Operator-authored persona"} if prompts is None else prompts
    )
    bundle["display_name"] = "Operator label"
    row, created = await db.upsert_managed_expert(
        managed_key=KEY, seed_version=seed_version, **bundle
    )
    assert created
    return row


def _config(row):
    return (
        json.loads(row["config"])
        if isinstance(row["config"], str)
        else deepcopy(row["config"])
    )


class HistoricalExpertStore:
    """Raw pre-manifest rows for exercising migration 0229 in its own era."""

    def __init__(self, db):
        self.db = db

    def __getattr__(self, name):
        return getattr(self.db, name)

    async def upsert_managed_expert(self, **fields):
        row = await self.db.fetchrow(
            """INSERT INTO experts (name,display_name,description,icon,color,tags,expert_type,
            config,prompts,is_global,managed_key,seed_version)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,TRUE,$10,$11) RETURNING *""",
            fields["name"],
            fields["display_name"],
            fields["description"],
            fields["icon"],
            fields["color"],
            fields["tags"],
            fields["expert_type"],
            json.dumps(fields["config"]),
            json.dumps(fields["prompts"]),
            fields["managed_key"],
            fields["seed_version"],
        )
        return dict(row), True

    async def get_expert_by_managed_key(self, key):
        row = await self.db.fetchrow("SELECT * FROM experts WHERE managed_key=$1", key)
        return dict(row) if row else None


@pytest.fixture
def legacy_db(db):
    return HistoricalExpertStore(db)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed_version", [1, 2, 3])
async def test_setup_guidance_updates_unchanged_persona_once(legacy_db, seed_version):
    db = legacy_db
    before = await _legacy(
        db,
        seed_version=seed_version,
        prompts={"persona": PREVIOUS_PERSONA, "instructions": "Keep my instructions"},
    )
    alternative = _bundle()
    alternative["name"] = "operator-choice"
    alternative["prompts"]["persona"] = PREVIOUS_PERSONA
    selected, _ = await db.upsert_managed_expert(
        managed_key="operator-alternative", seed_version=3, **alternative
    )
    await db.ensure_application_expert_default(
        expert_type="session", expert_id=str(selected["id"])
    )

    await db.execute(SETUP_GUIDANCE_MIGRATION.read_text())

    after = await db.get_expert_by_managed_key(KEY)
    assert json.loads(after["prompts"]) == {
        "persona": _bundle()["prompts"]["persona"],
        "instructions": "Keep my instructions",
    }
    assert after["version"] == before["version"] + 1
    for key in before.keys() - {"prompts", "version", "updated_at"}:
        assert after[key] == before[key], key
    assert await db.get_expert_by_managed_key("operator-alternative") == selected
    assert (await db.get_application_expert_default("session"))["id"] == selected["id"]

    await db.execute(SETUP_GUIDANCE_MIGRATION.read_text())
    assert await db.get_expert_by_managed_key(KEY) == after


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prompts",
    [
        {"persona": PREVIOUS_PERSONA + "\n"},
        {"persona": "Operator-authored persona"},
        {"persona": None},
        {"persona": {"text": PREVIOUS_PERSONA}},
        {"instructions": "No persona override"},
    ],
)
async def test_setup_guidance_preserves_operator_and_nontext_personas(
    legacy_db, prompts
):
    db = legacy_db
    before = await _legacy(db, prompts=prompts)

    await db.execute(SETUP_GUIDANCE_MIGRATION.read_text())

    assert await db.get_expert_by_managed_key(KEY) == before


@pytest.mark.asyncio
async def test_setup_guidance_rechecks_concurrent_persona_edit(legacy_db):
    db = legacy_db
    await _legacy(db, prompts={"persona": PREVIOUS_PERSONA})
    async with db.acquire() as editor:
        blocker = await editor.fetchval("SELECT pg_backend_pid()")
        async with editor.transaction():
            await editor.execute(
                """UPDATE experts SET prompts = jsonb_set(
                       prompts, '{persona}', '"Concurrent operator persona"'::jsonb
                   ), version = version + 1 WHERE managed_key = $1""",
                KEY,
            )
            operator_row = dict(
                await editor.fetchrow(
                    "SELECT * FROM experts WHERE managed_key = $1", KEY
                )
            )
            task = asyncio.create_task(db.execute(SETUP_GUIDANCE_MIGRATION.read_text()))
            try:
                async with asyncio.timeout(4):
                    while not await db.fetchval(
                        """SELECT EXISTS (
                               SELECT 1 FROM pg_stat_activity
                               WHERE $1 = ANY(pg_blocking_pids(pid))
                           )""",
                        blocker,
                    ):
                        await asyncio.sleep(0.01)
            except BaseException:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                raise

    await asyncio.wait_for(task, timeout=10)
    assert await db.get_expert_by_managed_key(KEY) == operator_row


@pytest.mark.asyncio
async def test_v2_roster_is_kept_as_seed_4_ships_it_preserving_content_and_pointer(db):
    """Seed 3 pinned a v2 roster's implementer; seed 4 ships the v2 roster
    again, so a v2 row upgrades straight to 4 with its roster untouched."""
    before = await _legacy(db)
    alternative = _bundle()
    alternative["name"] = "operator-choice"
    selected, _ = await db.upsert_managed_expert(
        managed_key="operator-alternative", seed_version=2, **alternative
    )
    await db.ensure_application_expert_default(
        expert_type="session", expert_id=str(selected["id"])
    )

    await seed_managed_default_experts(db, ROOT / "config")

    after = await db.get_expert_by_managed_key(KEY)
    expected_config = _config(before)
    expected_config["subagents"] = _bundle()["config"]["subagents"]
    assert expected_config["subagents"] == OLD_ROSTER
    assert _config(after) == expected_config
    assert after["seed_version"] == 4
    assert after["version"] == before["version"] + 1
    for key in before.keys() - {
        "config",
        "version",
        "seed_version",
        "updated_at",
        "manifest",
        "manifest_revision",
        "manifest_resource_version",
    }:
        assert after[key] == before[key], key
    pointer = await db.get_application_expert_default("session")
    assert pointer["id"] == selected["id"]
    assert await db.get_expert_by_managed_key("operator-alternative") == selected

    await seed_managed_default_experts(db, ROOT / "config")
    assert await db.get_expert_by_managed_key(KEY) == after


@pytest.mark.asyncio
@pytest.mark.parametrize("edit", ["default", "entry", "shell"])
async def test_operator_roster_variants_survive_seed_upgrade(db, edit):
    roster = deepcopy(OLD_ROSTER)
    if edit == "default":
        roster["default"] = "reader"
    elif edit == "entry":
        roster["roster"]["implementer"]["description"] = "My custom writer"
    else:
        roster["roster"]["implementer"]["tools"] = {"shell": ["run_command"]}
    before = await _legacy(db, roster=roster)

    await seed_managed_default_experts(db, ROOT / "config")

    after = await db.get_expert_by_managed_key(KEY)
    assert _config(after) == _config(before)
    assert after["prompts"] == before["prompts"]
    assert after["seed_version"] == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("seed_version", [1, 3, 4])
async def test_repair_requires_exact_seed_version(db, seed_version):
    before = await _legacy(db, seed_version=seed_version)

    updated = await db.upgrade_managed_expert_seed(
        managed_key=KEY,
        seed_version=3,
        config_additions={},
        expected_seed_version=2,
        expected_subagents=OLD_ROSTER,
        replacement_subagents=_bundle()["config"]["subagents"],
    )

    assert updated is None
    assert await db.get_expert_by_managed_key(KEY) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("edit_roster", [True, False])
async def test_waiting_upgrade_rechecks_operator_edit_after_row_lock(db, edit_roster):
    # A v3 row: the one whose roster seed 4 repairs, so a repair is waiting.
    stale_row = await _legacy(db, roster=V3_ROSTER, seed_version=3)
    task = None
    async with db.acquire() as editor:
        blocker = await editor.fetchval("SELECT pg_backend_pid()")
        async with editor.transaction():
            config = _config(stale_row)
            prompts = deepcopy(stale_row["prompts"])
            if edit_roster:
                config["subagents"]["roster"]["implementer"]["description"] = (
                    "Concurrent operator edit"
                )
            else:
                config["concurrent_operator_key"] = "preserve"
                prompts = {"persona": "Concurrent persona"}
            from orchestrator.services.manifest_experts import persist_expert_resource

            async with db.using_connection(editor):
                await db._lock_expert_catalog()
                raw = await editor.fetchrow(
                    "UPDATE experts SET version=version+1 WHERE managed_key=$1 RETURNING *",
                    KEY,
                )
                operator_row = await persist_expert_resource(
                    db, {**dict(raw), "config": config, "prompts": prompts}
                )
            task = asyncio.create_task(
                upgrade_managed_seed(db, spec=SPEC, bundle=_bundle(), row=stale_row)
            )
            try:
                # Prove the repair actually waits on the editor's catalog lock,
                # then commit the operator edit so PostgreSQL must recheck.
                async with asyncio.timeout(10):
                    while not await db.fetchval(
                        """SELECT EXISTS (
                               SELECT 1 FROM pg_stat_activity
                               WHERE $1 = ANY(pg_blocking_pids(pid))
                           )""",
                        blocker,
                    ):
                        await asyncio.sleep(0.01)
            except BaseException:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                raise

    updated = await asyncio.wait_for(task, timeout=10)
    after = await db.get_expert_by_managed_key(KEY)
    if edit_roster:
        assert updated is None
        assert after == operator_row
    else:
        assert updated is not None
        expected = _config(operator_row)
        expected["subagents"] = _bundle()["config"]["subagents"]
        assert _config(after) == expected
        assert after["prompts"] == operator_row["prompts"]
        assert after["seed_version"] == 4
        assert after["version"] == operator_row["version"] + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("scope_kind", ["Account", "Project", "Catalog"])
async def test_native_expert_binding_preserves_scope_visibility(db, scope_kind):
    from orchestrator.services.manifest_experts import sync_expert_identity
    from orchestrator.services.manifest_store import ManifestStore
    from shared.manifests import preview_documents
    from shared.manifests.resolution import content_revision

    project_id = await db.fetchval(
        "INSERT INTO projects(name) VALUES('Native Expert scope') RETURNING id"
    )
    owner = await db.fetchval(
        "INSERT INTO users(display_name,default_project_id) VALUES('Manifest owner',$1) RETURNING id",
        project_id,
    )
    outsider = await db.fetchval(
        "INSERT INTO users(display_name,default_project_id) VALUES('Other user',$1) RETURNING id",
        project_id,
    )
    scope_name = (
        "shared"
        if scope_kind == "Catalog"
        else str(project_id if scope_kind == "Project" else owner)
    )
    document = {
        "apiVersion": "srw/v1alpha1",
        "kind": "Expert",
        "metadata": {
            "name": "generic-" + str(uuid4())[:8],
            "scope": {"kind": scope_kind, "name": scope_name},
            "annotations": {
                "srw.io/bundled-selector": "assistant",
                "is_global": "true",
            },
        },
        "spec": {
            "runtime": {
                "image": "example.invalid/custom:fixed",
                "config": {"tools": {"my-tool": None}},
            }
        },
    }
    store = ManifestStore(db)
    resolved = preview_documents([document])["resolved"][0]
    async with db.transaction_scope():
        await store.lock_catalog()
        await store.lock_identity(document)
        resource, _ = await store.save(
            document,
            resolved,
            content_revision(resolved["spec"]),
            [],
            owner_id=owner,
            project_id=project_id if scope_kind == "Project" else None,
        )
        await sync_expert_identity(db, resource)
    linked = await store.by_id(resource["id"])
    assert linked["linked_id"] is not None
    owned = await db.get_expert_visible_by_id(
        str(linked["linked_id"]), user_id=str(owner)
    )
    assert owned["manifest"] == document
    assert owned["config"] == {} and owned["harness_adapter"] is None
    other = await db.get_expert_visible_by_id(
        str(linked["linked_id"]), user_id=str(outsider)
    )
    assert bool(other) == (scope_kind == "Catalog")
    if scope_kind == "Project":
        linked_project = await db.get_project_linked_expert(
            str(project_id), str(linked["linked_id"])
        )
        assert linked_project["manifest"] == document


# The managed General Worker's seed 2 (2026-11-11): the shell tools and the
# exact v1 description, through the same SQL and manifest write-back.
WORKER_KEY = "application-default-worker-seed"


async def _v1_worker(db, *, description=None, tools=None):
    from orchestrator.services.default_experts import _GENERAL_WORKER_V1_DESCRIPTION

    bundle = load_seed_bundle(
        ROOT / "config", directory="general-worker", expert_type="worker"
    )
    text = _GENERAL_WORKER_V1_DESCRIPTION if description is None else description
    bundle["description"] = text
    bundle["config"]["description"] = text + "\n"
    bundle["config"].pop("tools")
    if tools is not None:
        bundle["config"]["tools"] = tools
    row, created = await db.upsert_managed_expert(
        managed_key=WORKER_KEY, seed_version=1, **bundle
    )
    assert created
    return row


@pytest.mark.asyncio
async def test_v1_general_worker_gains_shell_and_description_once(db):
    before = await _v1_worker(db)
    assert "tools" not in _config(before)

    await seed_managed_default_experts(db, ROOT / "config")

    after = await db.get_expert_by_managed_key(WORKER_KEY)
    shipped = load_seed_bundle(
        ROOT / "config", directory="general-worker", expert_type="worker"
    )
    assert after["seed_version"] == 2
    assert _config(after)["tools"] == {
        "shell": ["run_command", "cancel_command", "shell_read"]
    }
    assert after["description"] == shipped["description"]
    assert _config(after)["description"] == shipped["description"]
    annotations = after["manifest"]["metadata"]["annotations"]
    assert annotations["srw.io/description"] == shipped["description"]
    assert after["version"] == before["version"] + 1

    await seed_managed_default_experts(db, ROOT / "config")
    assert await db.get_expert_by_managed_key(WORKER_KEY) == after


@pytest.mark.asyncio
async def test_v1_general_worker_keeps_operator_description_and_tools(db):
    before = await _v1_worker(db, description="Our house worker.", tools={"shell": []})

    await seed_managed_default_experts(db, ROOT / "config")

    after = await db.get_expert_by_managed_key(WORKER_KEY)
    assert after["seed_version"] == 2
    assert _config(after) == _config(before)
    assert after["description"] == "Our house worker."


async def _v3_assistant(db, *, description=None, tools=None):
    """What a pre-2026-11-11 install holds: seed 3, the pinned roster, no
    `tools` key and the description that listed no shell."""
    from orchestrator.services.default_experts import _ASSISTANT_V3_DESCRIPTION

    bundle = _bundle()
    text = _ASSISTANT_V3_DESCRIPTION if description is None else description
    bundle["description"] = text
    bundle["config"]["description"] = text + "\n"
    bundle["config"]["subagents"] = deepcopy(V3_ROSTER)
    bundle["config"].pop("tools")
    if tools is not None:
        bundle["config"]["tools"] = tools
    row, created = await db.upsert_managed_expert(
        managed_key=KEY, seed_version=3, **bundle
    )
    assert created
    return row


@pytest.mark.asyncio
async def test_v3_assistant_gains_shell_unpinned_roster_and_description_once(db):
    before = await _v3_assistant(db)
    assert "tools" not in _config(before)

    await seed_managed_default_experts(db, ROOT / "config")

    after = await db.get_expert_by_managed_key(KEY)
    shipped = _bundle()
    assert after["seed_version"] == 4
    assert _config(after)["tools"] == {
        "shell": ["run_command", "cancel_command", "shell_read"]
    }
    assert _config(after)["subagents"] == OLD_ROSTER
    assert after["description"] == shipped["description"]
    assert _config(after)["description"] == shipped["description"]
    assert after["version"] == before["version"] + 1

    await seed_managed_default_experts(db, ROOT / "config")
    assert await db.get_expert_by_managed_key(KEY) == after


@pytest.mark.asyncio
async def test_v3_assistant_keeps_operator_description_and_tools(db):
    before = await _v3_assistant(
        db, description="Our house assistant.", tools={"shell": []}
    )

    await seed_managed_default_experts(db, ROOT / "config")

    after = await db.get_expert_by_managed_key(KEY)
    expected = _config(before)
    expected["subagents"] = OLD_ROSTER
    assert after["seed_version"] == 4
    assert _config(after) == expected
    assert after["description"] == "Our house assistant."
