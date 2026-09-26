"""Upgrade an upstream (0300) database to the claim-less retired-Pod capture.

0301 replaces ``capture_retired_pinned_agent_pod()`` only. Each test runs the
real migration runner twice on its own empty PostgreSQL database: first the
chain without 0301 (the upstream head), then the complete chain. Retirement records
are written through the production End funnel before the upgrade, so they are
exactly what an upgraded installation already holds:

* a claim-bearing dedicated life, settled with the pre-0301 capture (it records
  the Pod and its agent workspace claim);
* a claim-less dedicated life, settled with the pre-0301 capture (it records no
  Pod relation at all).

The upgrade must apply 0301 alone, re-verify every applied file's checksum,
leave the historical records byte-for-byte unchanged and never infer a Pod
relation for a pre-0301 claim-less outcome. After the upgrade a new claim-less
life records its exact Pod, an incomplete shape still records nothing, and a
permanent Delete retires exactly the Pods the records prove.

See knowledge-base/knowledge/features/codebase_restructure_r32_acceptance_followup_2026_09_26.md.
"""

from __future__ import annotations

import json
import shutil
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator import main
from orchestrator.database import migrate
from orchestrator.database.postgres import PostgresDB
from orchestrator.services import agent_provisioner as agent_provisioner_module
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.pinned_k8s_effect import PINNED_AUTHORITY_FINALIZER
from orchestrator.security import crypto
from tests import test_self_ended_pinned_retirement_real_postgres as self_end

MIGRATIONS = self_end.authority_fixtures.SCHEMA_FILE.parent / "migrations" / "app"
CAPTURE_MIGRATION = "0301_capture_claimless_retired_agent_pod.sql"
UPSTREAM_HEAD = "0300_ide_restore_zero_effect_cancellation.sql"
NAMESPACE = self_end.NAMESPACE


@pytest.fixture(scope="module")
def server_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local Postgres container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture
async def empty_dsn(server_dsn):
    """A new, empty database on the module's server for each test."""

    name = f"upgrade_{uuid4().hex[:12]}"
    admin = await asyncpg.connect(server_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    return f"{server_dsn.rsplit('/', 1)[0]}/{name}"


async def _migrate(dsn, directory):
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        await migrate.run_migrations(pool, directory)
        return {
            row["filename"]: row["checksum"]
            for row in await pool.fetch(
                "SELECT filename, checksum FROM public.schema_migrations "
                "WHERE success ORDER BY filename"
            )
        }
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def upstream_ledger(empty_dsn, tmp_path_factory):
    """The database as upstream leaves it: every app migration up to 0300."""

    upstream = tmp_path_factory.mktemp("upstream-app-migrations")
    for path in migrate.discover(MIGRATIONS):
        if path.name != CAPTURE_MIGRATION:
            shutil.copy2(path, upstream / path.name)
    ledger = await _migrate(empty_dsn, upstream)
    assert max(ledger) == UPSTREAM_HEAD
    assert CAPTURE_MIGRATION not in ledger
    return ledger


@pytest_asyncio.fixture
async def store(empty_dsn, upstream_ledger, monkeypatch):
    monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
    monkeypatch.setenv("APP_ENCRYPTION_KEY", "P" * 32)
    crypto.reset_cipher_cache()
    db = PostgresDB(connection_string=empty_dsn, min_connections=1, max_connections=10)
    await db.connect()
    try:
        yield db
    finally:
        await db.close()
        crypto.reset_cipher_cache()


@pytest.fixture
def stack(store, monkeypatch):
    k8s = self_end.SelfEndK8sApi()
    provider = AgentProvisioner()
    provider._k8s_available = True
    provider._core_api = k8s
    provider._namespace = NAMESPACE
    monkeypatch.setattr(main.app.state.resources, "postgres_db", store)
    monkeypatch.setattr(agent_provisioner_module, "agent_provisioner", provider)
    return self_end.Stack(store, k8s)


async def _outcomes(db, thread_ids):
    rows = await db.fetch(
        "SELECT to_jsonb(o) AS row FROM thread_runtime_retirement_outcomes o "
        "WHERE thread_id = ANY($1::uuid[]) ORDER BY thread_id, settled_at",
        thread_ids,
    )
    return [json.loads(row["row"]) for row in rows]


async def _capture_source(db):
    return await db.fetchval(
        "SELECT prosrc FROM pg_proc WHERE proname='capture_retired_pinned_agent_pod'"
    )


@pytest.mark.asyncio
async def test_upgrade_from_upstream_head_applies_only_the_capture(
    stack, empty_dsn, upstream_ledger
):
    db = stack.db
    before_capture = await _capture_source(db)

    # Retirement records written by the pre-0301 capture.
    claimed = await self_end._bind_life(
        stack, await self_end._thread(db), with_claim=True
    )
    await self_end._agent_settles_soft_end(stack, claimed)
    legacy = await self_end._bind_life(
        stack, await self_end._thread(db), with_claim=False
    )
    await self_end._agent_settles_soft_end(stack, legacy)
    assert (await self_end._outcome_proof(db, claimed))["pod_uid"] == claimed["pod_uid"]
    assert await self_end._outcome_proof(db, legacy) is None
    history = await _outcomes(db, [claimed["thread"], legacy["thread"]])

    ledger = await _migrate(empty_dsn, MIGRATIONS)

    # Only 0301 was applied; every applied file kept its recorded checksum.
    assert set(ledger) - set(upstream_ledger) == {CAPTURE_MIGRATION}
    assert {name: ledger[name] for name in upstream_ledger} == upstream_ledger
    assert max(ledger) == CAPTURE_MIGRATION
    assert await _capture_source(db) != before_capture
    # Historical records are untouched: nothing is backfilled or inferred.
    assert await _outcomes(db, [claimed["thread"], legacy["thread"]]) == history

    # A new claim-less life now records its exact Pod.
    fresh = await self_end._bind_life(
        stack, await self_end._thread(db), with_claim=False
    )
    await self_end._agent_settles_soft_end(stack, fresh)
    assert await self_end._outcome_proof(db, fresh) == {
        "version": 1,
        "pod_name": fresh["pod_name"],
        "pod_uid": fresh["pod_uid"],
        "namespace": NAMESPACE,
        "provisioner": "agent",
        "provision_attempt": fresh["attempt"],
        "protection_protocol": "finalizer_v1",
    }

    # Permanent Delete retires exactly the proven Pods. The pre-0301
    # claim-less Pod has no proof, so it is neither deleted nor unprotected.
    for life in (claimed, legacy, fresh):
        stack.k8s.exit_and_reap(life)
        outcomes = await self_end._delete_until_settled(stack, life["thread"])
        assert outcomes[-1] == "deleted", outcomes
        assert await db.get_thread(life["thread"]) is None
    assert sorted(stack.k8s.removed_pods) == sorted(
        [claimed["pod_uid"], fresh["pod_uid"]]
    )
    assert stack.k8s.deleted_pvcs == [f"pvc-{claimed['thread']}"]
    kept = self_end._pod(stack, legacy)
    assert kept is not None
    assert kept.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]


@pytest.mark.asyncio
async def test_upgraded_capture_still_refuses_an_inexact_actor(
    stack, empty_dsn, upstream_ledger
):
    ledger = await _migrate(empty_dsn, MIGRATIONS)
    assert set(ledger) - set(upstream_ledger) == {CAPTURE_MIGRATION}
    db = stack.db
    life = await self_end._bind_life(
        stack, await self_end._thread(db), with_claim=False
    )
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "UPDATE thread_agent_pod_provision_intents SET pod_uid=$2 "
                "WHERE attempt_id=$1::uuid",
                life["attempt"],
                str(uuid4()),
            )

    retirement = await db.begin_pinned_thread_retirement(
        life["thread"],
        permanent=False,
        settle_status="ended",
        initiator="agent",
        expected_runtime_generation=life["generation"],
        expected_agent_id=life["agent"],
        expected_attach_token=life["attach_token"],
        authorize_immediately=True,
    )
    assert retirement["state"] == "pending"
    await self_end._ack_local_quiescence(db, life, retirement)
    assert await db.settle_pinned_thread_retirement(
        life["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        final_status="ended",
    )
    assert await self_end._outcome_proof(db, life) is None
