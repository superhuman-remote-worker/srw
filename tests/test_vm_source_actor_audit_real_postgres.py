"""VM source actor provenance survives ordinary operational-agent cleanup."""

import asyncio
import json
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from fastapi import HTTPException

from tests.test_vm_thread_audit_owner_real_postgres import (
    _ready_to_delete,
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    audit_schema,  # noqa: F401
    cleanup_lineage_schema,  # noqa: F401
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)
from tests.test_pinned_vm_initial_binding_real_postgres import (
    _bind_protected_agent,
    _thread,
    _initial_vm,
    _poll,
)
from tests.test_pinned_vm_failed_initial_end_real_postgres import (
    _abort_and_rebind_same_pod,
    _failed_source,
    _begin,
    _authorize,
)
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)


@pytest_asyncio.fixture(scope="module")
async def actor_schema(audit_schema, pg_dsn):  # noqa: F811
    migration = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0291_vm_thread_source_actor_provenance.sql"
    )
    if migration.exists():
        conn = await asyncpg.connect(pg_dsn)
        try:
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='vm_creation_retries_thread_agent_id_fkey')"
            ):
                await conn.execute(migration.read_text())
        finally:
            await conn.close()


@pytest_asyncio.fixture
async def db(actor_schema, _base_db):  # noqa: F811
    yield _base_db


SOURCE_INSERT = (
    "INSERT INTO vm_creation_retries "
    "(request_id,owner_kind,thread_id,thread_runtime_generation,thread_agent_id,thread_attach_token,"
    "provision_generation,origin,request_digest,canonical_request,controller_configuration_digest,controller_configuration) "
    "VALUES($1,'thread',$2,$3,$4,$5,$6,'initial',$7,$8::jsonb,$9,$10::jsonb) RETURNING *"
)


async def _sql_source(db):
    # Prepare the real protected actor and the exact projection immediately
    # before the SQL source INSERT. Like the existing native-source tests,
    # minimal request/config bodies isolate database admission authority.
    _, thread_id = await _thread(db, lane="pinned", status="created")
    current = await _bind_protected_agent(db, thread_id)
    request_id, generation = uuid4(), uuid4()
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) WHERE id=$1",
        thread_id,
        json.dumps(
            {
                "status": "provisioning",
                "provision_generation": str(generation),
                "creation_request_id": str(request_id),
            }
        ),
    )
    return current, (
        request_id,
        thread_id,
        current["runtime_generation"],
        current["agent_id"],
        current["runtime_attach_token"],
        generation,
        "sha256:" + "a" * 64,
        json.dumps(
            {
                "job_id": str(thread_id),
                "entity_type": "thread",
                "provision_generation": str(generation),
            }
        ),
        "sha256:" + "b" * 64,
        json.dumps({"version": 3}),
    )


@pytest.mark.asyncio
async def test_direct_source_insert_locks_reciprocal_actor_against_nonkey_update(db):
    current, arguments = await _sql_source(db)
    async with db.acquire() as conn, conn.transaction():
        assert await conn.fetchrow(SOURCE_INSERT, *arguments)
        async with db.acquire() as competitor, competitor.transaction():
            # An ordinary FK's KEY SHARE permits NO KEY UPDATE. The explicit
            # live-binding SHARE must protect the actor tuple as well.
            with pytest.raises(asyncpg.LockNotAvailableError):
                await competitor.fetchval(
                    "SELECT id FROM agents WHERE id=$1 FOR NO KEY UPDATE NOWAIT",
                    current["agent_id"],
                )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "malformed", ["missing", "unbound", "actor", "generation", "attach"]
)
async def test_direct_source_rejects_missing_or_wrong_current_actor(db, malformed):
    current, arguments = await _sql_source(db)
    arguments = list(arguments)
    if malformed in {"missing", "unbound"}:
        # Explicit historical-corruption fixture; the normal protected binding
        # writers cannot create these nonreciprocal states.
        async with db.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "DELETE FROM agents WHERE id=$1"
                if malformed == "missing"
                else "UPDATE agents SET thread_id=NULL WHERE id=$1",
                current["agent_id"],
            )
    else:
        arguments[{"actor": 3, "generation": 2, "attach": 4}[malformed]] = uuid4()
    with pytest.raises(asyncpg.CheckViolationError):
        await db.fetchrow(SOURCE_INSERT, *arguments)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1", current["id"]
        )
        == 0
    )


async def _wait_for_locks(db, count):
    for _ in range(300):
        if (
            await db.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'"
            )
            >= count
        ):
            return
        await asyncio.sleep(0.01)
    raise AssertionError("actor audit contenders did not queue on the owned database")


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["insert", "cleanup"])
async def test_source_insert_and_protected_abort_then_agent_delete_serialize(db, first):
    current, arguments = await _sql_source(db)

    async def insert():
        try:
            return await db.fetchrow(SOURCE_INSERT, *arguments)
        except asyncpg.CheckViolationError:
            return None

    async def cleanup():
        detached = await _abort_and_rebind_same_pod(db, current, rebind=False)
        assert detached["runtime_generation"] != current["runtime_generation"]
        assert await db.delete_agent(str(current["agent_id"]))
        return detached

    async with db.acquire() as locked, locked.transaction():
        await locked.fetchval(
            "SELECT id FROM threads WHERE id=$1 FOR UPDATE", current["id"]
        )
        first_task = asyncio.create_task(insert() if first == "insert" else cleanup())
        await _wait_for_locks(db, 1)
        second_task = asyncio.create_task(cleanup() if first == "insert" else insert())
        await _wait_for_locks(db, 2)
    results = await asyncio.gather(first_task, second_task)
    source = results[0] if first == "insert" else results[1]
    assert (source is not None) == (first == "insert")
    assert await db.get_agent(str(current["agent_id"])) is None
    if source is not None:
        assert source["thread_agent_id"] == current["agent_id"]
        assert (
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
            == source
        )
        async with db.acquire() as conn, conn.transaction():
            with pytest.raises(VMCreationRetryConflict):
                await VMCreationRetryStore(db)._thread_scope(conn, source)
        with pytest.raises(
            asyncpg.CheckViolationError, match="source identity is immutable"
        ):
            await db.execute(
                "UPDATE vm_creation_retries SET thread_agent_id=NULL,thread_attach_token=NULL WHERE request_id=$1",
                source["request_id"],
            )


@pytest.mark.asyncio
async def test_current_bound_source_actor_still_cannot_be_deleted(db, monkeypatch):
    current, source = await _failed_source(db, monkeypatch, abort_count=0)
    agent = await db.get_agent(str(current["agent_id"]))
    assert not await db.delete_agent(str(current["agent_id"]))
    assert not await db.delete_exact_offline_unbound_agent(
        str(current["agent_id"]),
        expected_hostname=agent["hostname"],
        expected_pod_uid=agent["pod_uid"],
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute("DELETE FROM agents WHERE id=$1", current["agent_id"])
    assert (
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == source
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["poll", "cleanup"])
async def test_application_source_poll_and_protected_agent_cleanup_serialize(
    db, monkeypatch, first
):
    thread_id, policy, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_protected_agent(db, thread_id)

    async def poll():
        try:
            return await _poll(db, dependencies.vm_provisioner, current)
        except HTTPException as error:
            assert error.status_code == 409
            return None

    async def cleanup():
        await _abort_and_rebind_same_pod(db, current, rebind=False)
        assert await db.delete_agent(str(current["agent_id"]))

    async with db.acquire() as locked, locked.transaction():
        await locked.fetchval(
            "SELECT id FROM threads WHERE id=$1 FOR UPDATE", thread_id
        )
        first_task = asyncio.create_task(poll() if first == "poll" else cleanup())
        await _wait_for_locks(db, 1)
        second_task = asyncio.create_task(cleanup() if first == "poll" else poll())
        await _wait_for_locks(db, 2)
    await asyncio.gather(first_task, second_task)
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    assert (source is not None) == (first == "poll")
    assert await db.get_agent(str(current["agent_id"])) is None
    if source is not None:
        assert source["thread_agent_id"] == current["agent_id"]
        assert source["thread_runtime_generation"] == current["runtime_generation"]
        assert (await policy.admit(request_id=str(source["request_id"])))[
            "action"
        ] == "unavailable"
        # An exact protected abort can outlive its operational actor while
        # creation is pending. Cleanup still requires independent current End.
        detached = await db.get_thread(str(thread_id))
        retirement = await _begin(db, detached)
        assert retirement["state"] == "pending", retirement
        await _authorize(db, detached, retirement)
        assert (
            await VMCreationRetryStore(db).settle_never_issued(
                request_id=str(source["request_id"])
            )
        )["settled"]
        assert await db.clear_pinned_retirement_physical_runtime_endpoint(
            str(thread_id),
            runtime_generation=retirement["generation"],
            retirement_token=retirement["token"],
        )
        # Source settlement now terminalizes its exact unadmitted waiter in
        # the same transaction; no independent maintenance pass is required.
        assert (
            await db.fetchval(
                "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
                source["request_id"],
            )
            == "cancelled"
        )
        await db.delete_thread(
            str(thread_id),
            expected_runtime_generation=retirement["generation"],
            expected_runtime_retirement_token=retirement["token"],
        )
        assert await db.get_thread(str(thread_id)) is None
        assert (
            await db.fetchval(
                "SELECT thread_agent_id FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
            == source["thread_agent_id"]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup", ["delete", "exact", "gc"])
async def test_settled_bound_source_does_not_block_agent_cleanup(
    db,
    monkeypatch,
    cleanup,  # noqa: F811
):
    current, original, retirement = await _ready_to_delete(
        db, monkeypatch, soft_first=True
    )
    await db.delete_thread(
        str(current["id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", original["request_id"]
    )
    agent = await db.fetchrow(
        "SELECT * FROM agents WHERE id=$1", source["thread_agent_id"]
    )
    assert agent["status"] == "offline"
    assert agent["thread_id"] is None and agent["current_job_id"] is None
    assert (
        await db.fetchval(
            "SELECT count(*) FROM threads WHERE agent_id=$1 OR control_admission_agent_id=$1",
            agent["id"],
        )
        == 0
    )
    if cleanup == "delete":
        assert await db.delete_agent(str(agent["id"]))
    elif cleanup == "exact":
        assert not await db.delete_exact_offline_unbound_agent(
            str(agent["id"]),
            expected_hostname=agent["hostname"],
            expected_pod_uid=str(uuid4()),
        )
        assert await db.delete_exact_offline_unbound_agent(
            str(agent["id"]),
            expected_hostname=agent["hostname"],
            expected_pod_uid=agent["pod_uid"],
        )
    else:
        unrelated = uuid4()
        await db.execute(
            "INSERT INTO agents(id,config_name,hostname,status,last_heartbeat) "
            "VALUES($1,'worker_base','owned-gc-control','offline',now()-interval '25 hours')",
            unrelated,
        )
        await db.execute(
            "UPDATE agents SET last_heartbeat=now()-interval '25 hours' WHERE id=$1",
            agent["id"],
        )
        assert await db.gc_offline_agents(retention_hours=24) == 2
        assert await db.get_agent(str(unrelated)) is None
    assert await db.get_agent(str(agent["id"])) is None
    assert (
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        == source
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1 AND state<>'released'",
            source["request_id"],
        )
        == 0
    )
