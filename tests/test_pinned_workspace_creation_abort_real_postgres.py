"""A failed attach must not abandon a durable workspace create obligation."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from orchestrator.services.session_attach_binding import release_session_attach_binding
from tests import test_persistent_recycler_real_postgres as authority
from tests import test_pinned_failed_start_end_real_postgres as start

db = authority.db
pg_dsn = authority.pg_dsn
_schema_applied = start._schema_applied


async def _creating(db, monkeypatch):
    ids = await authority._seed(db, protected_agent_pod=True, workspace_claim=False)
    await db.execute(
        "DELETE FROM project_officers WHERE thread_id=$1::uuid", ids["thread"]
    )
    await db.execute(
        "UPDATE threads SET status='created',metadata=jsonb_set(metadata,"
        "'{config_override,workspace,backend}',to_jsonb('sandbox'::text)) WHERE id=$1::uuid",
        ids["thread"],
    )
    thread = await db.get_thread(ids["thread"])
    generation = str(thread["runtime_generation"])
    cluster = start.PinnedPullCluster()
    provider = start.pull._provisioner(monkeypatch, db, cluster)
    returned, resume = asyncio.Event(), asyncio.Event()
    original = provider._create_pvc

    async def create(*args, **kwargs):
        result = await original(*args, **kwargs)
        returned.set()
        await resume.wait()
        return result

    monkeypatch.setattr(provider, "_create_pvc", create)
    task = asyncio.create_task(provider.create_pinned_thread_workspace(ids["thread"]))
    await asyncio.wait_for(returned.wait(), 10)
    return ids, generation, cluster, provider, task, resume


async def _release(db, ids, generation):
    return await release_session_attach_binding(
        ids["agent"],
        ids["thread"],
        expected_runtime_generation=generation,
        expected_attach_token=ids["attach_token"],
        expected_agent_pod_uid="old-pod",
        local_runtime_quiesced=True,
        local_quiescence_protocol="agent_attach_not_started_v1",
        dependencies=SimpleNamespace(store=db),
    )


@pytest.mark.asyncio
async def test_creation_return_before_abort_retains_publication_authority(
    db, monkeypatch
):
    ids, generation, cluster, _, task, resume = await _creating(db, monkeypatch)
    try:
        assert "pvc" in cluster.objects and "pod" not in cluster.objects
        intent = await db.fetchrow(
            "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid",
            ids["thread"],
        )
        assert intent["status"] == "planned" and intent["pvc_uid"] is None
        # Ordinary creates do not use the retained-successor effect latch.
        assert intent["creation_effects_admitted_at"] is None
        assert await _release(db, ids, generation) == "unsafe"
        current = await db.get_thread(ids["thread"])
        assert str(current["runtime_generation"]) == generation
        assert str(current["agent_id"]) == ids["agent"]
    finally:
        resume.set()
        await task
    intent = await db.fetchrow(
        "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1::uuid",
        ids["thread"],
    )
    assert intent["pvc_uid"] == cluster.objects["pvc"].metadata.uid
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert retirement["state"] == "pending", json.dumps(retirement, default=str)
