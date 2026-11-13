"""Startup strips the retired strategic/tactical prompts from current manifests.

Migration 0567 strips ``experts.prompts``; a manifest-backed Expert and each
Project's inline copy of it keep theirs in ``srw_resources``. The startup pass
re-saves them as new revisions through the store, and the result is what the
Project's own rebuild would write.
"""

from __future__ import annotations

import json
import logging
from uuid import UUID

import pytest

from orchestrator.services.manifest_experts import migrate_stored_experts
from orchestrator.services.manifest_phase_prompt_retirement import (
    retire_manifest_phase_prompts,
    strip_retired_prompts,
)
from orchestrator.services.manifest_projects import persist_project_resource
from orchestrator.services.manifest_store import ManifestStore
from shared.manifests.resolution import content_revision
from tests import test_manifest_native_full_schema as full_schema

database = full_schema.database
postgres_url = full_schema.postgres_url

_OLD_PROMPTS = {
    "persona": "Careful reviewer.",
    "strategic": "OLD STRATEGIC TEXT",
    "tactical": "OLD TACTICAL TEXT",
}


def _has_retired(value) -> bool:
    text = json.dumps(value)
    return "OLD STRATEGIC TEXT" in text or "OLD TACTICAL TEXT" in text


async def _live(db, sql: str, *args):
    return [dict(row) for row in await db.fetch(sql, *args)]


async def _seed(db):
    """A pre-0567 install: a manifest-backed Expert carrying phase text, and a
    Project that uses it (inline copies + managed children)."""
    owner = await db.fetchval(
        "INSERT INTO users(display_name,is_approved) VALUES('Owner',TRUE) RETURNING id"
    )
    expert = await db.fetchval(
        """INSERT INTO experts(name,display_name,expert_type,owner_id,config,prompts)
        VALUES('reviewer','Reviewer','worker',$1,'{}'::jsonb,$2::jsonb) RETURNING id""",
        owner,
        json.dumps(_OLD_PROMPTS),
    )
    await migrate_stored_experts(db)
    project = await db.fetchval(
        "INSERT INTO projects(name) VALUES('Phase prompts') RETURNING id"
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
    return expert, project


async def _project_row(db, project: UUID) -> dict:
    return await ManifestStore(db).by_link("Project", project)


def test_strip_reaches_every_expert_spec_and_nothing_else():
    spec = {
        "runtime": {"config": {"prompts": dict(_OLD_PROMPTS), "config": {}}},
        "resources": {
            "experts": {
                "a": {
                    "inline": {"runtime": {"config": {"prompts": dict(_OLD_PROMPTS)}}}
                },
                "b": {"ref": {"name": "x"}},
            }
        },
        "note": {"prompts": {"strategic": "not an expert spec"}},
    }
    assert strip_retired_prompts(spec) is True
    assert spec["runtime"]["config"]["prompts"] == {"persona": "Careful reviewer."}
    inline = spec["resources"]["experts"]["a"]["inline"]
    assert inline["runtime"]["config"]["prompts"] == {"persona": "Careful reviewer."}
    assert spec["note"] == {"prompts": {"strategic": "not an expert spec"}}
    assert strip_retired_prompts(spec) is False


@pytest.mark.asyncio
async def test_startup_pass_rewrites_experts_and_projects_as_new_revisions(database):
    db = database
    expert, project = await _seed(db)
    store = ManifestStore(db)
    expert_before = await store.by_link("Expert", expert)
    project_before = await _project_row(db, project)
    children_before = await _live(
        db,
        "SELECT * FROM srw_resources WHERE managed_by=$1 AND deleted_at IS NULL",
        project_before["id"],
    )
    # The seed reproduces what pre-0567 code wrote.
    assert _has_retired(expert_before["document"])
    assert _has_retired(project_before["document"])
    assert children_before and all(_has_retired(c["document"]) for c in children_before)

    counts = await retire_manifest_phase_prompts(db)

    assert counts == {"projects": 1, "experts": 1, "skipped": 0, "managed_left": 0}
    live = await _live(
        db,
        "SELECT * FROM srw_resources WHERE deleted_at IS NULL AND kind IN ('Expert','Project')",
    )
    assert not any(_has_retired((r["document"], r["resolved"])) for r in live)

    expert_after = await store.by_link("Expert", expert)
    assert expert_after["resource_version"] == expert_before["resource_version"] + 1
    assert expert_after["revision"] == content_revision(
        expert_after["resolved"]["spec"]
    )
    assert expert_after["revision"] != expert_before["revision"]
    prompts = expert_after["document"]["spec"]["runtime"]["config"]["prompts"]
    assert prompts == {"persona": "Careful reviewer."}
    # The new revision is in the history, after the old one.
    history = await _live(
        db,
        "SELECT resource_version,revision FROM srw_resource_revisions "
        "WHERE resource_id=$1 ORDER BY resource_version",
        expert_after["id"],
    )
    assert [h["revision"] for h in history][-2:] == [
        expert_before["revision"],
        expert_after["revision"],
    ]

    project_after = await _project_row(db, project)
    assert project_after["revision"] == content_revision(
        project_after["resolved"]["spec"]
    )
    assert project_after["active_revision"] == project_after["revision"]
    children = {
        str(c["id"]): c
        for c in await _live(
            db,
            "SELECT * FROM srw_resources WHERE managed_by=$1 AND deleted_at IS NULL",
            project_after["id"],
        )
    }
    pinned = {
        d["uid"]: d["revision"]
        for d in project_after["dependencies"]
        if isinstance(d, dict) and d.get("uid") in children
    }
    assert pinned and pinned == {uid: c["revision"] for uid, c in children.items()}

    # Rerun: nothing left to do.
    assert await retire_manifest_phase_prompts(db) == {
        "projects": 0,
        "experts": 0,
        "skipped": 0,
        "managed_left": 0,
    }

    # The Project's own rebuild composes the same copies from the stripped
    # source: its document and its managed children do not change.
    async with db.transaction_scope():
        await persist_project_resource(db, str(project))
    rebuilt = await _project_row(db, project)
    assert rebuilt["document"] == project_after["document"]
    for uid, child in children.items():
        again = await store.by_id(uid)
        assert again["resource_version"] == child["resource_version"]


@pytest.mark.asyncio
async def test_a_revision_that_is_not_its_digest_is_left_alone(database, caplog):
    db = database
    expert, _project = await _seed(db)
    row = await ManifestStore(db).by_link("Expert", expert)
    await db.execute(
        "UPDATE srw_resources SET revision='not-a-digest' WHERE id=$1", row["id"]
    )

    with caplog.at_level(logging.WARNING):
        counts = await retire_manifest_phase_prompts(db)

    assert counts["experts"] == 0 and counts["skipped"] == 1
    assert "not its content digest" in caplog.text
    untouched = await ManifestStore(db).by_link("Expert", expert)
    assert untouched["resource_version"] == row["resource_version"]
    assert _has_retired(untouched["document"])
