"""Permanent pinned retirement must hand exact warm Pod protection to cleanup."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from tests import test_persistent_recycler_real_postgres as fixtures
from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB
from orchestrator.security import crypto
from orchestrator.services.pinned_agent_authority import (
    reserve_pinned_warm_agent_binding,
)
from orchestrator.services.pinned_agent_authority import (
    reconcile_pinned_warm_binding_protections,
)


db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


async def _bound_warm_thread(db, monkeypatch):
    ids = await fixtures._seed_warm_pool_binding(db, bound=False)
    api = fixtures.StatefulPinnedK8sApi()
    fixtures._install_warm_pool_pod(api, ids)
    monkeypatch.setenv("PINNED_LEGACY_AGENT_NAMESPACES", "agents-a")
    provider = fixtures._production_warm_provisioner(db, api)
    binding = await reserve_pinned_warm_agent_binding(
        db,
        agent_provisioner=provider,
        persistent_provisioner=None,
        thread_id=ids["thread"],
        agent_id=ids["agent"],
        expected_runtime_generation=ids["runtime_generation"],
    )
    assert binding.bound
    ids["attach_token"] = str(binding.attach_token)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET status='active' WHERE id=$1::uuid",
            UUID(ids["thread"]),
        )
    return ids, api, provider


async def _legacy_deleted_owner(db, monkeypatch):
    """Model the old committed delete, with its exact append-only outcome."""

    ids, api, provider = await _bound_warm_thread(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    await fixtures._authorize_and_ack(db, ids, retirement)
    assert await db.clear_pinned_retirement_physical_runtime_endpoint(
        ids["thread"],
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        completed_quiescence_protocol="agent_runtime_zero_v1",
    )
    async with db.acquire() as conn:
        async with conn.transaction():
            # This is fixture construction for the pre-fix state, not the
            # production transition being exercised below.
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "UPDATE agents SET thread_id=NULL WHERE id=$1::uuid",
                UUID(ids["agent"]),
            )
            await conn.execute(
                "INSERT INTO thread_runtime_retirement_outcomes "
                "(thread_id,runtime_generation,retirement_token,agent_id,"
                "runtime_attach_token,disposition,permanent,outcome) VALUES "
                "($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5::uuid,'ended',true,'deleted')",
                UUID(ids["thread"]),
                UUID(ids["runtime_generation"]),
                UUID(retirement["token"]),
                UUID(ids["agent"]),
                UUID(ids["attach_token"]),
            )
            await conn.execute(
                "DELETE FROM threads WHERE id=$1::uuid", UUID(ids["thread"])
            )
            await conn.execute(
                "DELETE FROM agents WHERE id=$1::uuid", UUID(ids["agent"])
            )
            await conn.execute(
                "UPDATE thread_agent_warm_binding_protections "
                "SET lease_expires_at=created_at+interval '1 millisecond' "
                "WHERE thread_id=$1::uuid",
                UUID(ids["thread"]),
            )
    return ids, api, provider


@pytest.mark.asyncio
async def test_active_permanent_delete_persists_exact_warm_release_through_retry(
    db, monkeypatch
):
    ids, api, provider = await _bound_warm_thread(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert retirement["context"]["agent_pod"]["warm_binding_protection"]
    await fixtures._authorize_and_ack(db, ids, retirement)
    assert await db.clear_pinned_retirement_physical_runtime_endpoint(
        ids["thread"],
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        completed_quiescence_protocol="agent_runtime_zero_v1",
    )
    await db.delete_thread(
        ids["thread"],
        expected_runtime_retirement_token=retirement["token"],
        expected_runtime_generation=retirement["generation"],
    )
    assert await db.get_thread(ids["thread"]) is None
    async with db.acquire() as conn:
        warm = await conn.fetchrow(
            "SELECT status,release_started_at,pod_uid FROM "
            "thread_agent_warm_binding_protections WHERE thread_id=$1::uuid",
            UUID(ids["thread"]),
        )
        actor = await conn.fetchrow(
            "SELECT status::text AS status,thread_id FROM agents WHERE id=$1::uuid",
            UUID(ids["agent"]),
        )
    assert warm["status"] == "terminal_release"
    assert warm["release_started_at"] is not None
    assert warm["pod_uid"] == ids["pod_uid"]
    assert dict(actor) == {"status": "draining", "thread_id": None}

    # A crash after the DB commit leaves this receipt for a later leader.
    # Even after its lease expires, the still-running caller cannot be freed.
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "UPDATE thread_agent_warm_binding_protections "
                "SET lease_expires_at=created_at+interval '1 millisecond' "
                "WHERE thread_id=$1::uuid",
                UUID(ids["thread"]),
            )
    pod = api.pods[("agents-a", ids["pod_name"])]
    async with db.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM thread_agent_warm_binding_protections "
            "WHERE thread_id=$1::uuid AND status IN "
            "('planned','protecting','protected','releasing') "
            "AND lease_expires_at<=now())",
            UUID(ids["thread"]),
        )
    await reconcile_pinned_warm_binding_protections(
        db, agent_provisioner=provider, persistent_provisioner=None
    )
    assert pod.metadata.finalizers == [fixtures.PINNED_AUTHORITY_FINALIZER]
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as denied:
            await conn.execute(
                "UPDATE agents SET status='ready' WHERE id=$1::uuid",
                UUID(ids["agent"]),
            )
        assert denied.value.constraint_name == "agents_pinned_warm_binding_authority"
        await conn.execute(
            "UPDATE agents SET last_heartbeat=now()-interval '1 day' WHERE id=$1::uuid",
            UUID(ids["agent"]),
        )
    # Old replicas can run this global stale sweep without poisoning it;
    # offline is still an exact, unbound terminal actor, never a ready pool slot.
    assert await db.mark_stale_agents_offline(timeout_minutes=3)
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT status::text FROM agents WHERE id=$1::uuid",
                UUID(ids["agent"]),
            )
            == "offline"
        )
    api.mark_terminal("agents-a", ids["pod_name"])
    assert pod.metadata.deletion_timestamp is None
    await reconcile_pinned_warm_binding_protections(
        db, agent_provisioner=provider, persistent_provisioner=None
    )
    async with db.acquire() as conn:
        settled = await conn.fetchrow(
            "SELECT status,release_outcome FROM "
            "thread_agent_warm_binding_protections WHERE thread_id=$1::uuid",
            UUID(ids["thread"]),
        )
    assert dict(settled) == {
        "status": "released",
        "release_outcome": "exact_absent_v1",
    }


@pytest.mark.asyncio
async def test_legacy_deleted_owner_releases_only_terminal_exact_pod(db, monkeypatch):
    ids, api, provider = await _legacy_deleted_owner(db, monkeypatch)
    pod = api.pods[("agents-a", ids["pod_name"])]

    # The immutable receipt makes this recoverable, but lease expiry alone
    # does not prove process zero while the exact Pod is still running.
    await reconcile_pinned_warm_binding_protections(
        db, agent_provisioner=provider, persistent_provisioner=None
    )
    assert pod.metadata.finalizers == [fixtures.PINNED_AUTHORITY_FINALIZER]

    api.mark_terminal("agents-a", ids["pod_name"])
    pod.metadata.deletion_timestamp = "now"
    await reconcile_pinned_warm_binding_protections(
        db, agent_provisioner=provider, persistent_provisioner=None
    )
    async with db.acquire() as conn:
        warm = await conn.fetchrow(
            "SELECT status,release_outcome FROM "
            "thread_agent_warm_binding_protections WHERE thread_id=$1::uuid",
            UUID(ids["thread"]),
        )
    assert dict(warm) == {"status": "released", "release_outcome": "exact_absent_v1"}
    assert ("agents-a", ids["pod_name"]) not in api.pods


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt_change", ["missing", "wrong_attach", "wrong_generation"]
)
async def test_orphan_without_exact_deleted_receipt_keeps_finalizer(
    db, monkeypatch, receipt_change
):
    ids, api, provider = await _legacy_deleted_owner(db, monkeypatch)
    pod = api.pods[("agents-a", ids["pod_name"])]
    api.mark_terminal("agents-a", ids["pod_name"])
    pod.metadata.deletion_timestamp = "now"
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            if receipt_change == "missing":
                await conn.execute(
                    "DELETE FROM thread_runtime_retirement_outcomes "
                    "WHERE thread_id=$1::uuid",
                    UUID(ids["thread"]),
                )
            elif receipt_change == "wrong_attach":
                await conn.execute(
                    "UPDATE thread_runtime_retirement_outcomes "
                    "SET runtime_attach_token=$2::uuid WHERE thread_id=$1::uuid",
                    UUID(ids["thread"]),
                    uuid4(),
                )
            else:
                await conn.execute(
                    "UPDATE thread_runtime_retirement_outcomes "
                    "SET runtime_generation=$2::uuid WHERE thread_id=$1::uuid",
                    UUID(ids["thread"]),
                    uuid4(),
                )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as denied:
            await conn.execute(
                "UPDATE thread_agent_warm_binding_protections "
                "SET status='terminal_release',"
                "release_started_at=transaction_timestamp() "
                "WHERE thread_id=$1::uuid AND status='bound'",
                UUID(ids["thread"]),
            )
    assert denied.value.constraint_name == "thread_agent_warm_binding_reciprocity"
    await reconcile_pinned_warm_binding_protections(
        db, agent_provisioner=provider, persistent_provisioner=None
    )
    assert pod.metadata.finalizers == [fixtures.PINNED_AUTHORITY_FINALIZER]
    async with db.acquire() as conn:
        status = await conn.fetchval(
            "SELECT status FROM thread_agent_warm_binding_protections "
            "WHERE thread_id=$1::uuid",
            UUID(ids["thread"]),
        )
    assert status == "bound"


@pytest.mark.asyncio
async def test_deleted_owner_release_holds_repurposed_actor(db, monkeypatch):
    ids, api, provider = await _legacy_deleted_owner(db, monkeypatch)
    async with db.acquire() as conn:
        protection_id = await conn.fetchval(
            "SELECT protection_id FROM thread_agent_warm_binding_protections "
            "WHERE thread_id=$1::uuid",
            UUID(ids["thread"]),
        )
    assert await db.begin_deleted_pinned_warm_binding_release(str(protection_id))
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as denied:
            await conn.execute(
                "INSERT INTO agents (id,config_name,hostname,pod_ip,pod_uid,status,"
                "agent_mode,last_heartbeat) VALUES "
                "($1::uuid,'worker_base','new-owner','127.0.0.1',"
                "'new-pod-uid','draining','dual',now())",
                UUID(ids["agent"]),
            )
        assert denied.value.constraint_name == "agents_pinned_warm_binding_authority"
        # Model a predecessor bypassing the new trigger before rollout.
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "INSERT INTO agents (id,config_name,hostname,pod_ip,pod_uid,status,"
                "agent_mode,last_heartbeat) VALUES "
                "($1::uuid,'worker_base','new-owner','127.0.0.1',"
                "'new-pod-uid','draining','dual',now())",
                UUID(ids["agent"]),
            )
    pod = api.pods[("agents-a", ids["pod_name"])]
    api.mark_terminal("agents-a", ids["pod_name"])
    pod.metadata.deletion_timestamp = "now"
    await reconcile_pinned_warm_binding_protections(
        db, agent_provisioner=provider, persistent_provisioner=None
    )
    assert pod.metadata.finalizers == [fixtures.PINNED_AUTHORITY_FINALIZER]
    async with db.acquire() as conn:
        warm = await conn.fetchrow(
            "SELECT status,release_outcome FROM "
            "thread_agent_warm_binding_protections WHERE protection_id=$1::uuid",
            protection_id,
        )
        actor = await conn.fetchrow(
            "SELECT status::text AS status,pod_uid FROM agents WHERE id=$1::uuid",
            UUID(ids["agent"]),
        )
    assert dict(warm) == {"status": "terminal_release", "release_outcome": None}
    assert dict(actor) == {"status": "draining", "pod_uid": "new-pod-uid"}


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["pod_uid", "namespace", "runtime_attach_token"])
async def test_active_delete_refuses_mismatched_warm_identity(db, monkeypatch, field):
    ids, _api, _provider = await _bound_warm_thread(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    await fixtures._authorize_and_ack(db, ids, retirement)
    assert await db.clear_pinned_retirement_physical_runtime_endpoint(
        ids["thread"],
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
        completed_quiescence_protocol="agent_runtime_zero_v1",
    )
    wrong_value = uuid4() if field == "runtime_attach_token" else "wrong-identity"
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                f"UPDATE thread_agent_warm_binding_protections SET {field}=$2 "
                "WHERE thread_id=$1::uuid",
                UUID(ids["thread"]),
                wrong_value,
            )
    with pytest.raises(RuntimeError, match="warm Pod release lacks exact authority"):
        await db.delete_thread(
            ids["thread"],
            expected_runtime_retirement_token=retirement["token"],
            expected_runtime_generation=retirement["generation"],
        )
    assert await db.get_thread(ids["thread"]) is not None
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT status FROM thread_agent_warm_binding_protections "
                "WHERE thread_id=$1::uuid",
                UUID(ids["thread"]),
            )
            == "bound"
        )
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM thread_runtime_retirement_outcomes "
            "WHERE thread_id=$1::uuid)",
            UUID(ids["thread"]),
        )


@pytest.mark.asyncio
async def test_orphan_release_never_patches_same_name_successor(db, monkeypatch):
    ids, api, provider = await _legacy_deleted_owner(db, monkeypatch)
    api.install_old_pod(
        namespace="agents-a",
        name=ids["pod_name"],
        uid="new-pod-uid",
        labels={"srw/managed-by": "agent-provisioner", "srw/purpose": "job"},
    )
    successor = api.pods[("agents-a", ids["pod_name"])]
    await reconcile_pinned_warm_binding_protections(
        db, agent_provisioner=provider, persistent_provisioner=None
    )
    assert successor.metadata.finalizers == [fixtures.PINNED_AUTHORITY_FINALIZER]
    assert successor.metadata.uid == "new-pod-uid"
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT status FROM thread_agent_warm_binding_protections "
                "WHERE thread_id=$1::uuid",
                UUID(ids["thread"]),
            )
            == "terminal_release"
        )


@pytest.mark.asyncio
async def test_terminal_release_requires_absent_receipt_and_rechecks_deleted_owner(
    db, monkeypatch
):
    ids, api, provider = await _legacy_deleted_owner(db, monkeypatch)
    async with db.acquire() as conn:
        protection_id = await conn.fetchval(
            "SELECT protection_id FROM thread_agent_warm_binding_protections "
            "WHERE thread_id=$1::uuid",
            UUID(ids["thread"]),
        )
    assert await db.begin_deleted_pinned_warm_binding_release(str(protection_id))
    assert not await db.complete_pinned_warm_binding_release(
        str(protection_id),
        release_outcome="exact_live_unprotected_v1",
        agent_present=True,
    )
    assert not await db.complete_pinned_warm_binding_release(
        str(protection_id),
        release_outcome="exact_replacement_v1",
        agent_present=False,
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as denied:
            await conn.execute(
                "UPDATE thread_agent_warm_binding_protections SET "
                "status='released',release_outcome='exact_live_unprotected_v1',"
                "released_at=transaction_timestamp() "
                "WHERE protection_id=$1::uuid",
                protection_id,
            )
        assert denied.value.constraint_name == "thread_agent_warm_binding_authority"
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "DELETE FROM thread_runtime_retirement_outcomes "
                "WHERE thread_id=$1::uuid",
                UUID(ids["thread"]),
            )
    pod = api.pods[("agents-a", ids["pod_name"])]
    api.mark_terminal("agents-a", ids["pod_name"])
    pod.metadata.deletion_timestamp = "now"
    await reconcile_pinned_warm_binding_protections(
        db, agent_provisioner=provider, persistent_provisioner=None
    )
    assert pod.metadata.finalizers == [fixtures.PINNED_AUTHORITY_FINALIZER]


@pytest.mark.asyncio
async def test_populated_0300_warm_binding_upgrades_and_validates(
    tmp_path, monkeypatch
):
    migrations = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    stage = tmp_path / "migrations"
    stage.mkdir()
    for path in migrations.glob("*.sql"):
        if path.name.split("_", 1)[0] <= "0300":
            (stage / path.name).write_bytes(path.read_bytes())

    with PostgresContainer("postgres:15") as container:
        dsn = container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        store = PostgresDB(connection_string=dsn, min_connections=1, max_connections=4)
        try:
            await run_migrations(pool, stage)
            await store.connect()
            monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
            monkeypatch.setenv("APP_ENCRYPTION_KEY", "P" * 32)
            crypto.reset_cipher_cache()
            ids, _api, _provider = await _bound_warm_thread(store, monkeypatch)
            async with store.acquire() as conn:
                before = await conn.fetchrow(
                    "SELECT protection_id,thread_id,agent_id,status,pod_uid "
                    "FROM thread_agent_warm_binding_protections "
                    "WHERE thread_id=$1::uuid",
                    UUID(ids["thread"]),
                )
            assert before["status"] == "bound"
            for name in (
                "0301_pinned_permanent_warm_release.sql",
                "0302_pinned_warm_reconcile_terminal_idx.notx.sql",
                "0303_pinned_warm_thread_terminal_idx.notx.sql",
                "0304_pinned_warm_agent_terminal_idx.notx.sql",
                "0305_validate_pinned_permanent_warm_release.sql",
            ):
                (stage / name).write_bytes((migrations / name).read_bytes())
            await run_migrations(pool, stage)
            await run_migrations(pool, stage)
            async with store.acquire() as conn:
                after = await conn.fetchrow(
                    "SELECT protection_id,thread_id,agent_id,status,pod_uid "
                    "FROM thread_agent_warm_binding_protections "
                    "WHERE thread_id=$1::uuid",
                    UUID(ids["thread"]),
                )
                validated = await conn.fetchval(
                    "SELECT bool_and(convalidated) FROM pg_constraint "
                    "WHERE conrelid='thread_agent_warm_binding_protections'::regclass "
                    "AND conname IN ("
                    "'thread_agent_warm_binding_protections_status_check',"
                    "'thread_agent_warm_binding_protections_check2')"
                )
                online_indexes = await conn.fetchval(
                    "SELECT count(*) FROM pg_index i "
                    "JOIN pg_class c ON c.oid=i.indexrelid "
                    "WHERE c.relname=ANY($1::text[]) "
                    "AND i.indisvalid AND i.indisready "
                    "AND pg_get_expr(i.indpred,i.indrelid) LIKE '%terminal_release%'",
                    [
                        "idx_thread_agent_warm_binding_reconcile_v2",
                        "idx_thread_agent_warm_binding_thread_active_v2",
                        "idx_thread_agent_warm_binding_agent_active_v2",
                    ],
                )
            assert after == before
            assert validated is True
            assert online_indexes == 3
        finally:
            await store.close()
            await pool.close()
            crypto.reset_cipher_cache()
