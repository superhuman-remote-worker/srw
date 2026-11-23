"""The one-time startup pass that keeps existing Experts and automations phased.

Before the ``phases`` setting every job planned in phases; now a job does not
unless its configuration says so. The first start after the upgrade writes
``phases: true`` on every stored Expert manifest and every automation that does
not state the setting, as new revisions through the store, and records that it
ran. The shared catalogue's copies of the experts the release ships without
phases, and a Project's frozen copy of one, follow the new default; a user's
own copy of one is marked. An installation that never created a job has
nothing from before the setting: the pass marks nothing there.
"""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from uuid import UUID

import pytest

from orchestrator.services.expert_phases_backfill import (
    MARKER_KEY,
    SHIPPED_WITHOUT_PHASES,
    backfill_existing_phases,
    mark_phases,
)
from orchestrator.services.manifest_experts import (
    migrate_stored_experts,
    seed_bundled_expert_manifests,
)
from orchestrator.services.manifest_projects import persist_project_resource
from orchestrator.services.manifest_store import ManifestStore
from shared.manifests.resolution import content_revision
from tests import test_manifest_native_full_schema as full_schema

database = full_schema.database
postgres_url = full_schema.postgres_url

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
CATALOG = {"kind": "Catalog", "name": "shared"}


def _private(row: dict) -> dict:
    """The SRW private configuration of a stored Expert document or spec."""
    spec = row.get("spec", row)
    return spec["runtime"]["config"]


def _phases_of(value: dict):
    return _private(value).get("config", {}).get("phases", "<absent>")


async def _live(db, sql: str, *args):
    return [dict(row) for row in await db.fetch(sql, *args)]


async def _user(db, name: str = "Owner") -> UUID:
    return await db.fetchval(
        "INSERT INTO users(display_name,is_approved) VALUES($1,TRUE) RETURNING id",
        name,
    )


async def _job_from_before_the_setting(db) -> UUID:
    """A job row: the installation ran work before the setting existed, so the
    pass marks what it finds (with no job it marks nothing)."""
    return await db.fetchval(
        "INSERT INTO jobs(description,status) VALUES('Before phases','completed') "
        "RETURNING id"
    )


async def _legacy_expert(db, name: str, owner: UUID | None, config: dict) -> UUID:
    """An ``experts`` row as an older release stored it; the migration gives it
    its manifest (Account scope with an owner; without one a managed, global
    row in the shared catalogue)."""
    return await db.fetchval(
        """INSERT INTO experts
            (name,display_name,expert_type,owner_id,config,prompts,
             managed_key,is_global)
        VALUES($1,$2,'worker',$3,$4::jsonb,'{}'::jsonb,$5,$6) RETURNING id""",
        name,
        name.title(),
        owner,
        json.dumps(config),
        None if owner else f"test-{name}-seed",
        owner is None,
    )


async def _project_using(db, owner: UUID, expert: UUID) -> UUID:
    project = await db.fetchval(
        "INSERT INTO projects(name) VALUES('Phases backfill') RETURNING id"
    )
    await db.execute(
        "INSERT INTO project_members(project_id,user_id,role) VALUES($1,$2,'owner')",
        project,
        owner,
    )
    await db.execute(
        "INSERT INTO project_experts(project_id,expert_id) VALUES($1,$2)",
        project,
        expert,
    )
    async with db.transaction_scope():
        await persist_project_resource(db, str(project))
    return project


async def _automation(db, owner: UUID, name: str, override: dict) -> UUID:
    return await db.fetchval(
        """INSERT INTO automations
            (owner_id,name,trigger_type,cron_expr,expert,prompt,config_override)
        VALUES($1,$2,'cron','0 * * * *','worker_base','Do it',$3::jsonb)
        RETURNING id""",
        owner,
        name,
        json.dumps(override),
    )


async def _override(db, automation: UUID) -> dict:
    value = await db.fetchval(
        "SELECT config_override FROM automations WHERE id=$1", automation
    )
    return json.loads(value) if isinstance(value, str) else value


# =============================================================================
# mark_phases, the transform
# =============================================================================


def _expert_spec(config=None, *, adapter="srw/v1", **private) -> dict:
    runtime_config = {"config_name": "worker_base", **private}
    if config is not None:
        runtime_config["config"] = config
    return {"runtime": {"adapter": adapter, "config": runtime_config}}


class TestMarkPhases:
    def test_an_expert_without_the_key_is_marked(self):
        spec = _expert_spec({"agent_id": "x"})
        assert mark_phases(spec) is True
        assert _private(spec)["config"] == {"agent_id": "x", "phases": True}
        # Idempotent: a second pass finds the key.
        assert mark_phases(spec) is False

    def test_a_missing_config_dict_is_created(self):
        spec = _expert_spec()
        assert "config" not in _private(spec)
        assert mark_phases(spec) is True
        assert _private(spec)["config"] == {"phases": True}

    def test_a_null_config_is_created(self):
        spec = _expert_spec()
        _private(spec)["config"] = None
        assert mark_phases(spec) is True
        assert _private(spec)["config"] == {"phases": True}

    @pytest.mark.parametrize("value", [True, False, "on", "off", "No"])
    def test_a_stated_value_is_kept(self, value):
        spec = _expert_spec({"phases": value})
        assert mark_phases(spec) is False
        assert _private(spec)["config"] == {"phases": value}

    @pytest.mark.parametrize("value", [None, "", "maybe"])
    def test_a_value_that_states_nothing_is_replaced(self, value):
        spec = _expert_spec({"phases": value})
        assert mark_phases(spec) is True
        assert _private(spec)["config"] == {"phases": True}

    @pytest.mark.parametrize("adapter", ["generic", None, "srw/v2"])
    def test_a_spec_with_another_adapter_is_untouched(self, adapter):
        spec = _expert_spec({"agent_id": "x"}, adapter=adapter)
        before = deepcopy(spec)
        assert mark_phases(spec) is False
        assert spec == before

    def test_a_runtime_without_an_adapter_is_untouched(self):
        spec = {"runtime": {"image": "example/x", "config": {"config": {}}}}
        before = deepcopy(spec)
        assert mark_phases(spec) is False
        assert spec == before

    @pytest.mark.parametrize("asset", SHIPPED_WITHOUT_PHASES)
    def test_with_skip_shipped_a_copy_of_one_shipped_without_phases_is_untouched(
        self, asset
    ):
        # A Project's frozen copy of a catalogue expert the release ships
        # without phases follows the new default.
        spec = _expert_spec({"agent_id": asset}, asset_name=asset)
        before = deepcopy(spec)
        assert mark_phases(spec, skip_shipped=True) is False
        assert spec == before

    @pytest.mark.parametrize("asset", SHIPPED_WITHOUT_PHASES)
    def test_by_default_a_copy_of_one_shipped_without_phases_is_marked(self, asset):
        # A user's own copy of one of them is that user's expert.
        spec = _expert_spec({"agent_id": asset}, asset_name=asset)
        assert mark_phases(spec) is True
        assert _private(spec)["config"] == {"agent_id": asset, "phases": True}

    def test_skip_shipped_marks_every_other_expert(self):
        spec = _expert_spec({}, asset_name="developer")
        assert mark_phases(spec, skip_shipped=True) is True
        assert _private(spec)["config"] == {"phases": True}

    def test_a_copy_of_a_phased_bundled_expert_is_marked(self):
        spec = _expert_spec({}, asset_name="developer")
        assert mark_phases(spec) is True
        assert _private(spec)["config"] == {"phases": True}

    def test_a_config_that_is_no_dict_is_untouched(self):
        spec = _expert_spec()
        _private(spec)["config"] = ["not", "a", "dict"]
        before = deepcopy(spec)
        assert mark_phases(spec) is False
        assert spec == before

    @staticmethod
    def _project_spec() -> dict:
        return {
            "resources": {
                "experts": {
                    "a": {"inline": _expert_spec({"agent_id": "a"})},
                    "b": {
                        "inline": _expert_spec(
                            {"agent_id": "b"}, asset_name="general-worker"
                        )
                    },
                    "c": {"inline": _expert_spec({"phases": False})},
                    "d": {"ref": {"name": "x"}},
                },
                "list": [{"inline": _expert_spec()}],
            },
            "note": {"config": {"agent_id": "not an expert spec"}},
        }

    @pytest.mark.parametrize("skip_shipped", [True, False])
    def test_inline_copies_in_a_project_spec_are_reached(self, skip_shipped):
        spec = self._project_spec()
        assert mark_phases(spec, skip_shipped=skip_shipped) is True
        experts = spec["resources"]["experts"]
        assert _private(experts["a"]["inline"])["config"]["phases"] is True
        shipped = _private(experts["b"]["inline"])["config"]
        if skip_shipped:
            assert "phases" not in shipped
        else:
            assert shipped["phases"] is True
        assert _private(experts["c"]["inline"])["config"]["phases"] is False
        assert experts["d"] == {"ref": {"name": "x"}}
        assert _private(spec["resources"]["list"][0]["inline"])["config"] == {
            "phases": True
        }
        assert spec["note"] == {"config": {"agent_id": "not an expert spec"}}
        assert mark_phases(spec, skip_shipped=skip_shipped) is False

    @pytest.mark.parametrize("value", [None, "text", 3, [], {}])
    def test_values_without_an_expert_spec_are_untouched(self, value):
        before = deepcopy(value)
        assert mark_phases(value) is False
        assert value == before


# =============================================================================
# backfill_existing_phases on a real database
# =============================================================================


@pytest.mark.asyncio
async def test_a_custom_expert_is_marked_in_a_new_revision(database):
    db = database
    await _job_from_before_the_setting(db)
    owner = await _user(db)
    expert = await _legacy_expert(db, "reviewer", owner, {"agent_id": "reviewer"})
    await migrate_stored_experts(db)
    store = ManifestStore(db)
    before = await store.by_link("Expert", expert)
    assert before["document"]["metadata"]["scope"]["kind"] == "Account"
    assert _phases_of(before["document"]) == "<absent>"

    counts = await backfill_existing_phases(db)

    assert counts == {"projects": 0, "experts": 1, "skipped": 0, "automations": 0}
    after = await store.by_link("Expert", expert)
    assert _phases_of(after["document"]) is True
    assert _phases_of(after["resolved"]) is True
    assert _private(after["document"])["config"]["agent_id"] == "reviewer"
    assert after["resource_version"] == before["resource_version"] + 1
    assert after["revision"] == content_revision(after["resolved"]["spec"])
    assert after["revision"] != before["revision"]
    # The new revision is in the history, after the old one.
    history = await _live(
        db,
        "SELECT resource_version,revision FROM srw_resource_revisions "
        "WHERE resource_id=$1 ORDER BY resource_version",
        after["id"],
    )
    assert [h["revision"] for h in history][-2:] == [
        before["revision"],
        after["revision"],
    ]


@pytest.mark.asyncio
async def test_an_expert_that_states_the_setting_is_not_touched(database):
    db = database
    await _job_from_before_the_setting(db)
    owner = await _user(db)
    stated_off = await _legacy_expert(db, "direct", owner, {"phases": False})
    stated_on = await _legacy_expert(db, "planner", owner, {"phases": True})
    await migrate_stored_experts(db)
    store = ManifestStore(db)
    before = {
        uid: await store.by_link("Expert", uid) for uid in (stated_off, stated_on)
    }

    counts = await backfill_existing_phases(db)

    assert counts["experts"] == 0 and counts["skipped"] == 0
    for uid, row in before.items():
        after = await store.by_link("Expert", uid)
        assert after["resource_version"] == row["resource_version"]
        assert after["revision"] == row["revision"]
        assert after["document"] == row["document"]
    assert _phases_of((await store.by_link("Expert", stated_off))["document"]) is False


@pytest.mark.asyncio
async def test_the_catalogue_copies_shipped_without_phases_are_left_alone(database):
    db = database
    await _job_from_before_the_setting(db)
    await seed_bundled_expert_manifests(db, CONFIG_DIR)
    # A catalogue Expert an older release stored without the setting.
    await _legacy_expert(db, "shared-reviewer", None, {"agent_id": "shared"})
    await migrate_stored_experts(db)
    store = ManifestStore(db)
    before = {
        row["document"]["metadata"]["name"]: row
        for row in await store.list_scope(CATALOG, kind="Expert")
    }
    assert set(SHIPPED_WITHOUT_PHASES) | {"developer", "shared-reviewer"} <= set(before)
    for name in SHIPPED_WITHOUT_PHASES:
        assert _phases_of(before[name]["document"]) == "<absent>", name
    assert _phases_of(before["developer"]["document"]) is True
    unstated = {
        name
        for name, row in before.items()
        if _phases_of(row["document"]) == "<absent>"
    }
    expected = unstated - set(SHIPPED_WITHOUT_PHASES)
    assert "shared-reviewer" in expected

    counts = await backfill_existing_phases(db)

    assert counts["skipped"] == 0
    assert counts["experts"] == len(expected)
    after = {
        row["document"]["metadata"]["name"]: row
        for row in await store.list_scope(CATALOG, kind="Expert")
    }
    for name in SHIPPED_WITHOUT_PHASES:
        assert after[name]["resource_version"] == before[name]["resource_version"]
        assert _phases_of(after[name]["document"]) == "<absent>", name
    # Stated by its file: not touched.
    assert (
        after["developer"]["resource_version"]
        == before["developer"]["resource_version"]
    )
    for name in expected:
        assert _phases_of(after[name]["document"]) is True, name
        assert _phases_of(after[name]["resolved"]) is True, name
        assert after[name]["revision"] == content_revision(
            after[name]["resolved"]["spec"]
        )


@pytest.mark.asyncio
async def test_a_project_and_its_managed_children_are_rewritten_together(database):
    db = database
    await _job_from_before_the_setting(db)
    owner = await _user(db)
    expert = await _legacy_expert(db, "reviewer", owner, {"agent_id": "reviewer"})
    await migrate_stored_experts(db)
    project = await _project_using(db, owner, expert)
    store = ManifestStore(db)
    project_before = await store.by_link("Project", project)
    children_before = [
        await store.by_id(c["id"])
        for c in await _live(
            db,
            "SELECT id FROM srw_resources WHERE managed_by=$1 AND deleted_at IS NULL",
            project_before["id"],
        )
    ]
    assert children_before
    for child in children_before:
        assert _phases_of(child["document"]["spec"]) == "<absent>"
    inline_before = [
        entry["inline"]
        for entry in project_before["document"]["spec"]["resources"]["experts"].values()
        if "inline" in entry
    ]
    assert inline_before
    assert all(_phases_of(copy) == "<absent>" for copy in inline_before)

    counts = await backfill_existing_phases(db)

    assert counts == {"projects": 1, "experts": 1, "skipped": 0, "automations": 0}
    project_after = await store.by_link("Project", project)
    assert project_after["resource_version"] == project_before["resource_version"] + 1
    assert project_after["revision"] == content_revision(
        project_after["resolved"]["spec"]
    )
    assert project_after["active_revision"] == project_after["revision"]
    inline = [
        entry["inline"]
        for entry in project_after["document"]["spec"]["resources"]["experts"].values()
        if "inline" in entry
    ]
    assert inline and all(_phases_of(copy) is True for copy in inline)

    children = {
        str(c["id"]): await store.by_id(c["id"])
        for c in await _live(
            db,
            "SELECT id FROM srw_resources WHERE managed_by=$1 AND deleted_at IS NULL",
            project_after["id"],
        )
    }
    assert children
    for child in children.values():
        assert _phases_of(child["document"]["spec"]) is True
        assert child["revision"] == content_revision(child["resolved"]["spec"])
    # The Project pins the new revision of each managed child.
    pinned = {
        d["uid"]: d["revision"]
        for d in project_after["dependencies"]
        if isinstance(d, dict) and d.get("uid") in children
    }
    assert pinned and pinned == {uid: c["revision"] for uid, c in children.items()}

    # The source Expert was marked too, so the Project's own rebuild composes
    # the same copies: its document and its managed children do not change.
    async with db.transaction_scope():
        await persist_project_resource(db, str(project))
    rebuilt = await store.by_link("Project", project)
    assert rebuilt["document"] == project_after["document"]
    for uid, child in children.items():
        again = await store.by_id(uid)
        assert again["resource_version"] == child["resource_version"]


@pytest.mark.asyncio
async def test_automations_without_the_key_are_marked(database):
    db = database
    await _job_from_before_the_setting(db)
    owner = await _user(db)
    bare = await _automation(db, owner, "bare", {})
    other_keys = await _automation(db, owner, "other", {"autonomy": "full"})
    # A null an older editor left there states nothing either.
    null = await _automation(db, owner, "null", {"phases": None, "priority": 3})
    off = await _automation(db, owner, "off", {"phases": False})
    on = await _automation(db, owner, "on", {"phases": True})

    counts = await backfill_existing_phases(db)

    assert counts["automations"] == 3
    assert await _override(db, bare) == {"phases": True}
    assert await _override(db, other_keys) == {"autonomy": "full", "phases": True}
    assert await _override(db, null) == {"phases": True, "priority": 3}
    assert await _override(db, off) == {"phases": False}
    assert await _override(db, on) == {"phases": True}


@pytest.mark.asyncio
async def test_an_installation_without_a_job_marks_nothing(database):
    db = database
    owner = await _user(db)
    expert = await _legacy_expert(db, "reviewer", owner, {"agent_id": "reviewer"})
    await migrate_stored_experts(db)
    await seed_bundled_expert_manifests(db, CONFIG_DIR)
    automation = await _automation(db, owner, "nightly", {})
    store = ManifestStore(db)
    before = {
        row["id"]: row["resource_version"]
        for row in await _live(
            db,
            "SELECT id, resource_version FROM srw_resources "
            "WHERE kind='Expert' AND deleted_at IS NULL",
        )
    }

    counts = await backfill_existing_phases(db)

    zero = {"projects": 0, "experts": 0, "skipped": 0, "automations": 0}
    assert counts == zero
    after = {
        row["id"]: row["resource_version"]
        for row in await _live(
            db,
            "SELECT id, resource_version FROM srw_resources "
            "WHERE kind='Expert' AND deleted_at IS NULL",
        )
    }
    assert after == before
    # Neither the custom Expert nor a bundled one without the key (here the
    # session assistant) is marked.
    assert _phases_of((await store.by_link("Expert", expert))["document"]) == (
        "<absent>"
    )
    assistant = await store.by_name("Expert", CATALOG, "assistant")
    assert _phases_of(assistant["document"]) == "<absent>"
    assert await _override(db, automation) == {}
    # The marker row is written with the zero counts.
    value = await db.fetchval(
        "SELECT value FROM system_settings WHERE key=$1", MARKER_KEY
    )
    assert (json.loads(value) if isinstance(value, str) else value) == zero

    # A job created afterwards does not reopen the pass.
    await _job_from_before_the_setting(db)
    assert await backfill_existing_phases(db) is None
    assert _phases_of((await store.by_link("Expert", expert))["document"]) == (
        "<absent>"
    )
    assert await _override(db, automation) == {}


@pytest.mark.asyncio
async def test_the_pass_runs_once_and_records_its_counts(database):
    db = database
    await _job_from_before_the_setting(db)
    owner = await _user(db)
    await _legacy_expert(db, "reviewer", owner, {"agent_id": "reviewer"})
    await migrate_stored_experts(db)
    await _automation(db, owner, "nightly", {})

    counts = await backfill_existing_phases(db)

    assert counts == {"projects": 0, "experts": 1, "skipped": 0, "automations": 1}
    row = await db.fetchrow(
        "SELECT value, updated_by, source FROM system_settings WHERE key=$1",
        MARKER_KEY,
    )
    assert MARKER_KEY == "jobs.phases_backfill"
    assert row is not None
    value = row["value"]
    assert (json.loads(value) if isinstance(value, str) else value) == counts
    assert row["source"] == "default"
    assert row["updated_by"] == "startup:phases-backfill"

    # Created after the pass, without the key: it follows the default.
    late = await _legacy_expert(db, "late", owner, {"agent_id": "late"})
    await migrate_stored_experts(db)
    late_automation = await _automation(db, owner, "late", {})

    assert await backfill_existing_phases(db) is None

    late_row = await ManifestStore(db).by_link("Expert", late)
    assert _phases_of(late_row["document"]) == "<absent>"
    assert await _override(db, late_automation) == {}
    # The recorded counts are those of the first run.
    value = await db.fetchval(
        "SELECT value FROM system_settings WHERE key=$1", MARKER_KEY
    )
    assert (json.loads(value) if isinstance(value, str) else value) == counts


@pytest.mark.asyncio
async def test_a_revision_that_is_not_its_digest_is_skipped(database, caplog):
    import logging

    db = database
    await _job_from_before_the_setting(db)
    owner = await _user(db)
    expert = await _legacy_expert(db, "reviewer", owner, {"agent_id": "reviewer"})
    await migrate_stored_experts(db)
    row = await ManifestStore(db).by_link("Expert", expert)
    await db.execute(
        "UPDATE srw_resources SET revision='not-a-digest' WHERE id=$1", row["id"]
    )

    with caplog.at_level(logging.WARNING):
        counts = await backfill_existing_phases(db)

    assert counts["experts"] == 0 and counts["skipped"] == 1
    assert "not its content digest" in caplog.text
    untouched = await ManifestStore(db).by_link("Expert", expert)
    assert untouched["resource_version"] == row["resource_version"]
    assert _phases_of(untouched["document"]) == "<absent>"
    # The marker is written anyway: the skipped Expert follows the default.
    assert await backfill_existing_phases(db) is None


async def _account_copy_of(db, owner: UUID, bundled: str) -> dict:
    """An Account-scope copy of a bundled Expert, as the Cockpit's "Copy"
    stores it: the bundled private configuration, ``asset_name`` included."""
    from shared.manifests import parse_documents, preview_documents
    from shared.runtime.core.srw_manifest_config import srw_private_config

    path = CONFIG_DIR / "experts" / bundled / "config.yaml"
    document = parse_documents(path.read_text(encoding="utf-8"))[0]
    document["metadata"]["name"] = f"my-{bundled}"
    document["metadata"]["scope"] = {"kind": "Account", "name": str(owner)}
    srw_private_config(document)
    resolved = preview_documents([document])["resolved"][0]
    store = ManifestStore(db)
    async with db.transaction_scope():
        await store.lock_identity(document)
        await store.save(
            document,
            resolved,
            content_revision(resolved["spec"]),
            [],
            owner_id=owner,
        )
    return await store.by_name("Expert", document["metadata"]["scope"], f"my-{bundled}")


@pytest.mark.asyncio
async def test_an_account_copy_of_an_expert_shipped_without_phases_is_marked(
    database,
):
    db = database
    await _job_from_before_the_setting(db)
    owner = await _user(db)
    before = await _account_copy_of(db, owner, "general-worker")
    assert _private(before["document"])["asset_name"] == "general-worker"
    assert _phases_of(before["document"]) == "<absent>"

    await backfill_existing_phases(db)

    after = await ManifestStore(db).by_id(before["id"])
    assert _phases_of(after["document"]) is True
