"""Historical attach adoption is exact, secret-free and serialized with PATCH."""

import asyncio
from copy import deepcopy
import json

from fastapi import HTTPException
import pytest

from orchestrator.services.manifest_session_delivery import (
    capture_session_delivery,
    prepare_delivery_snapshot,
)
from orchestrator.services.manifest_store import ManifestStore
from tests import test_manifest_native_full_schema as full_schema

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url


async def historical_session(db, owner):
    return dict(
        await db.fetchrow(
            "INSERT INTO threads(user_id,kind,title,status) VALUES($1,'session','Historical attach','created') RETURNING *",
            owner["id"],
        )
    )


def prepared(thread, model="saved-model"):
    blob = {
        "agent": {
            "agent_id": "session",
            "llm": {"model": model, "temperature": 0.2},
            "workspace": {"backend": "sandbox"},
            "interactive": {"permission_mode": "read_only"},
        },
        "prompts": {"persona": "The exact authorized persona."},
    }
    candidate = prepare_delivery_snapshot(
        thread,
        {},
        blob,
        blob["agent"],
        image="srw:installed",
        config_name="session_base",
    )
    return blob, candidate


@pytest.mark.asyncio
async def test_prepare_is_read_only_and_attach_freezes_before_delivering(
    database, actor
):
    thread = await historical_session(database, actor)
    blob, candidate = prepared(thread)
    assert await ManifestStore(database).execution("Session", str(thread["id"])) is None
    # Credential delivery enriches a copy after the resolver captured policy.
    delivered = deepcopy(blob)
    delivered["agent"]["llm"]["api_key"] = "ephemeral-delivery-sentinel"
    delivered["agent"]["workspace"]["remote"] = {"ssh_key": "ephemeral-ssh-sentinel"}
    async with database.thread_datasource_lock(str(thread["id"])):
        result = await capture_session_delivery(
            database,
            thread,
            delivered,
            {"_manifest_snapshot": candidate},
            project_ids=[],
        )
    snapshot = await ManifestStore(database).execution("Session", str(thread["id"]))
    private = snapshot["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"][
        "config"
    ]
    assert private["resolved"] == blob
    assert result["execution_snapshot"]["generation"] == 1
    assert result["execution_snapshot"]["revision"] == snapshot["revision"]
    assert result["agent"]["llm"]["api_key"] == "ephemeral-delivery-sentinel"
    assert "ephemeral-" not in json.dumps(snapshot["resolved"])
    assert (
        await database.fetchval("SELECT status FROM threads WHERE id=$1", thread["id"])
        == "created"
    )


@pytest.mark.asyncio
async def test_competing_first_attach_cannot_relabel_another_generations_blob(
    database, actor
):
    thread = await historical_session(database, actor)

    async def deliver(model):
        blob, candidate = prepared(thread, model)
        return await capture_session_delivery(
            database, thread, blob, {"_manifest_snapshot": candidate}, project_ids=[]
        )

    results = await asyncio.gather(
        deliver("one"), deliver("two"), return_exceptions=True
    )
    failures = [result for result in results if isinstance(result, Exception)]
    assert len(failures) == 1
    assert isinstance(failures[0], HTTPException) and failures[0].status_code == 409
    delivered = next(result for result in results if isinstance(result, dict))
    snapshot = await ManifestStore(database).execution("Session", str(thread["id"]))
    stored = snapshot["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"][
        "config"
    ]["resolved"]
    assert delivered["agent"]["llm"]["model"] == stored["agent"]["llm"]["model"]
    assert (
        await database.fetchval("SELECT count(*) FROM srw_execution_spec_revisions")
        == 1
    )


@pytest.mark.asyncio
async def test_settings_changed_since_resolution_refuse_first_attach(database, actor):
    thread = await historical_session(database, actor)
    blob, candidate = prepared(thread)
    await database.execute(
        "UPDATE threads SET metadata=jsonb_build_object('config_override',jsonb_build_object('llm',jsonb_build_object('model','new-selection'))) WHERE id=$1",
        thread["id"],
    )
    with pytest.raises(HTTPException) as caught:
        await capture_session_delivery(
            database, thread, blob, {"_manifest_snapshot": candidate}, project_ids=[]
        )
    assert caught.value.status_code == 409
    assert await ManifestStore(database).execution("Session", str(thread["id"])) is None
