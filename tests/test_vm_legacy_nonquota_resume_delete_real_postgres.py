"""Permanent Delete must settle already accepted legacy non-quota Resume."""

import json
from uuid import UUID, uuid4

import pytest

from tests.test_pinned_vm_failed_initial_end_real_postgres import _release_binding
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent
from tests.test_vm_nonquota_retained_resume_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    _thread_schema,  # noqa: F401
    adopted_source,
    cleaned_retirement,
    db,  # noqa: F401
    pg_dsn,  # noqa: F401
    setup,  # noqa: F401
    thread_schema,  # noqa: F401
)


async def legacy_resumed(store, controller_setup, monkeypatch, *, released_actor):
    from orchestrator.services import vm_resource_thread_cleanup as cleanup_owner
    from orchestrator.services import vm_thread_retained_resume as resume_owner

    current, source = await adopted_source(store, controller_setup, monkeypatch)
    scope, predecessor = (
        cleanup_owner.thread_cleanup_scope,
        resume_owner.predecessor_on_conn,
    )

    async def old_scope(conn, recovery, permit, proof):
        row = await conn.fetchrow(
            "SELECT controller_configuration FROM vm_creation_retries WHERE thread_id=$1",
            current["id"],
        )
        if json.loads(row["controller_configuration"])["version"] == 1:
            return None
        return await scope(conn, recovery, permit, proof)

    async def old_predecessor(conn, thread):
        row = await conn.fetchrow(
            "SELECT controller_configuration FROM vm_creation_retries WHERE thread_id=$1",
            current["id"],
        )
        if json.loads(row["controller_configuration"])["version"] == 1:
            return None
        return await predecessor(conn, thread)

    # Use exactly the old production v1 exclusions, while all capture, creation,
    # cleanup admission, settlement, Resume and release SQL remains real.
    with monkeypatch.context() as old:
        old.setattr(cleanup_owner, "thread_cleanup_scope", old_scope)
        old.setattr(resume_owner, "predecessor_on_conn", old_predecessor)
        soft = await cleaned_retirement(store, current, permanent=False)
        assert await store.settle_pinned_thread_retirement(
            str(current["id"]),
            token=soft["token"],
            generation=soft["generation"],
            final_status="ended",
        )
        ended = await store.get_thread(str(current["id"]))
        assert json.loads(ended["metadata"])["vm"]["status"] == "deleted"
        assert await store.resume_thread(str(current["id"]))
    resumed = await store.get_thread(str(current["id"]))
    assert resumed["runtime_generation"] != current["runtime_generation"]
    assert resumed["status"] == "created" and resumed["agent_id"] is None
    assert await store.fetchval("SELECT count(*) FROM vm_thread_retained_resumes") == 0
    assert (
        await store.fetchval("SELECT count(*) FROM vm_resource_thread_cleanup_stops")
        == 0
    )
    if released_actor:
        actor = await _bind_cold_agent(
            store, current["id"], pod_name="srw-agent-s-" + uuid4().hex[:8]
        )
        assert await _release_binding(store, actor) == "released"
        assert await store.delete_agent(str(actor["agent_id"]))
        resumed = await store.get_thread(str(current["id"]))
        assert resumed["runtime_generation"] != actor["runtime_generation"]
    return resumed, source, soft


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "released_actor",
    [False, True],
    ids=["uncreated-successor", "confirmed-pre-setup-release"],
)
async def test_legacy_nonquota_resume_permanent_delete_keeps_exact_old_source(
    db,  # noqa: F811 - imported PostgreSQL fixture
    setup,  # noqa: F811 - imported controller fixture
    monkeypatch,
    released_actor,
):
    current, source, soft = await legacy_resumed(
        db, setup, monkeypatch, released_actor=released_actor
    )
    permanent = await cleaned_retirement(db, current, permanent=True)
    captured = permanent["context"]
    assert captured["entry_status"] == "created"
    assert captured["runtime_authority_exposed"] is False
    assert captured["agent_id"] is None and captured["runtime_attach_token"] is None
    row = await db.get_thread(str(current["id"]))
    assert row["runtime_retirement_external_cleanup"] is not None
    assert "vm" not in json.loads(row["metadata"])
    assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == 0
    assert await db.fetchval("SELECT count(*) FROM vm_creation_retries") == 1
    assert await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM thread_runtime_retirement_outcomes WHERE thread_id=$1 AND runtime_generation=$2 AND retirement_token=$3 AND NOT permanent AND outcome='settled' AND disposition='ended')",
        current["id"],
        source["thread_runtime_generation"],
        UUID(soft["token"]),
    )
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=permanent["generation"],
        expected_runtime_retirement_token=permanent["token"],
    )
    assert await db.get_thread(str(current["id"])) is None
    audit = await db.fetchrow(
        "SELECT * FROM vm_thread_creation_owners WHERE thread_id=$1", current["id"]
    )
    assert audit["live_thread_id"] is None and audit["deleted_at"] is not None
    assert str(audit["deleted_runtime_generation"]) == permanent["generation"]
    assert str(audit["deleted_retirement_token"]) == permanent["token"]
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
        )
        == source
    )
