"""Upgrade an upstream (0300) database to the R3.2 retirement migrations.

0301 replaces ``capture_retired_pinned_agent_pod()`` and 0302 replaces
``validate_thread_agent_warm_binding_protection()``; neither touches data. Each
test runs the real migration runner twice on its own empty PostgreSQL database:
first the chain up to the upstream head, then the complete chain. Retirement records
are written through the production End funnel before the upgrade, so they are
exactly what an upgraded installation already holds:

* a claim-bearing dedicated life, settled with the pre-0301 capture (it records
  the Pod and its agent workspace claim);
* a claim-less dedicated life, settled with the pre-0301 capture (it records no
  Pod relation at all).

The upgrade must apply 0301 and 0302 alone, re-verify every applied file's checksum,
leave the historical records byte-for-byte unchanged and never infer a Pod
relation for a pre-0301 claim-less outcome. After the upgrade a new claim-less
life records its exact Pod, an incomplete shape still records nothing, a
permanent Delete retires exactly the Pods the records prove, and a live warm
life's permanent Delete settles its protection record.

The local k3d development database took a third path. It applied these two
files, successfully, as ``0286_capture_claimless_retired_agent_pod.sql`` and
``0287_permanent_retirement_releases_warm_protection.sql`` (source 131dd22ee)
before upstream published its own 0286-0300. That exact 281-row history is
rebuilt here from the published bytes under their historical names, as an
ordinary (non-superuser) owner on PostgreSQL 15 like the deployed server, with
retirement records written both before and while the historical pair ran.
``migration_recovery.RENAMED_APPLIED_MIGRATIONS`` keeps those two exact rows
from counting as missing files; the canonical files then apply after upstream's
0286-0300 like everywhere else, re-running the same reviewed bytes once. The
result must equal a fresh installation, keep every record, and replace each
function in place; anything but the exact reviewed rows is still refused.

See knowledge-base/knowledge/features/codebase_restructure_r32_acceptance_followup_2026_09_26.md.
"""

from __future__ import annotations

import json
import shutil
from types import SimpleNamespace as NS
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator import main
from orchestrator.database import migrate
from orchestrator.database.migration_recovery import (
    APPLIED_CHECKSUM_COMPATIBILITIES,
    RENAMED_APPLIED_MIGRATIONS,
    AppliedChecksumCompatibility,
    RenamedAppliedMigration,
)
from orchestrator.database.postgres import PostgresDB
from orchestrator.services import agent_provisioner as agent_provisioner_module
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.pinned_k8s_effect import PINNED_AUTHORITY_FINALIZER
from orchestrator.security import crypto
from tests import test_self_ended_pinned_retirement_real_postgres as self_end

MIGRATIONS = self_end.authority_fixtures.SCHEMA_FILE.parent / "migrations" / "app"
CAPTURE_MIGRATION = "0301_capture_claimless_retired_agent_pod.sql"
WARM_RELEASE_MIGRATION = "0302_permanent_retirement_releases_warm_protection.sql"
LOCAL_MIGRATIONS = {CAPTURE_MIGRATION, WARM_RELEASE_MIGRATION}
UPSTREAM_HEAD = "0300_ide_restore_zero_effect_cancellation.sql"
NAMESPACE = self_end.NAMESPACE

# The deployed local history: 131dd22ee's app chain, exactly as the k3d
# database recorded it (read-only inspection, 2026-09-28): 281 successful rows
# ending with the pair under its historical names and these checksums.
DEPLOYED_HISTORY_THROUGH = "0285"
DEPLOYED_PAIR = {
    "0286_capture_claimless_retired_agent_pod.sql": (
        CAPTURE_MIGRATION,
        "a2d08b52d9197d52e43da0859bf328be91c16fbb94feea31c1cdf2e1694bac2e",
    ),
    "0287_permanent_retirement_releases_warm_protection.sql": (
        WARM_RELEASE_MIGRATION,
        "c8df370587940ecebc93c0c53a4ff48e29e1d761018fb5b78e318321f2be6d8f",
    ),
}
DEPLOYED_LEDGER_ROWS = 281
PAIR_FUNCTIONS = (
    "capture_retired_pinned_agent_pod",
    "validate_thread_agent_warm_binding_protection",
)
LEDGER_QUERY = "SELECT * FROM public.schema_migrations ORDER BY filename"
FUNCTION_IDENTITY_QUERY = """
SELECT p.proname,
       p.oid::bigint AS oid,
       pg_catalog.pg_get_userbyid(p.proowner) AS owner,
       pg_catalog.md5(p.prosrc) AS source,
       p.proacl::text AS acl,
       p.prosecdef AS security_definer,
       p.proconfig AS config,
       ARRAY(
           SELECT t.tgrelid::regclass::text || ':' || t.tgname
             FROM pg_catalog.pg_trigger t
            WHERE t.tgfoid = p.oid
            ORDER BY 1
       ) AS triggers
  FROM pg_catalog.pg_proc p
 WHERE p.pronamespace = 'public'::regnamespace
   AND p.proname = ANY($1::text[])
 ORDER BY p.proname
"""
# Every application catalog object a migration can create or change, without
# owners or OIDs (each test database has its own owner role).
CATALOG_QUERY = """
WITH ns AS (
    SELECT oid, nspname FROM pg_catalog.pg_namespace
     WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
       AND nspname NOT LIKE 'pg_temp_%' AND nspname NOT LIKE 'pg_toast_temp_%'
)
SELECT line FROM (
    SELECT format('relation %s.%s %s', ns.nspname, c.relname, c.relkind) AS line
      FROM pg_catalog.pg_class c JOIN ns ON ns.oid = c.relnamespace
     WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', 'i', 'I', 'c')
    UNION ALL
    SELECT format('column %s.%s %s %s notnull=%s default=%s identity=%s generated=%s',
                  c.oid::regclass, a.attnum, a.attname,
                  pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull,
                  pg_catalog.pg_get_expr(d.adbin, d.adrelid), a.attidentity,
                  a.attgenerated)
      FROM pg_catalog.pg_attribute a
      JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
      JOIN ns ON ns.oid = c.relnamespace
      LEFT JOIN pg_catalog.pg_attrdef d
        ON d.adrelid = a.attrelid AND d.adnum = a.attnum
     WHERE a.attnum > 0 AND NOT a.attisdropped
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'c')
    UNION ALL
    SELECT format('constraint %s %s %s validated=%s deferrable=%s deferred=%s',
                  co.conrelid::regclass, co.conname,
                  pg_catalog.pg_get_constraintdef(co.oid), co.convalidated,
                  co.condeferrable, co.condeferred)
      FROM pg_catalog.pg_constraint co JOIN ns ON ns.oid = co.connamespace
    UNION ALL
    SELECT format('index %s valid=%s ready=%s', pg_catalog.pg_get_indexdef(i.indexrelid),
                  i.indisvalid, i.indisready)
      FROM pg_catalog.pg_index i
      JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
      JOIN ns ON ns.oid = c.relnamespace
    UNION ALL
    SELECT format('trigger %s enabled=%s', pg_catalog.pg_get_triggerdef(t.oid),
                  t.tgenabled)
      FROM pg_catalog.pg_trigger t
      JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
      JOIN ns ON ns.oid = c.relnamespace
     WHERE NOT t.tgisinternal
    UNION ALL
    SELECT format('function %s %s', p.oid::regprocedure,
                  pg_catalog.md5(pg_catalog.pg_get_functiondef(p.oid)))
      FROM pg_catalog.pg_proc p JOIN ns ON ns.oid = p.pronamespace
     WHERE p.prokind IN ('f', 'p')
    UNION ALL
    SELECT format('view %s %s', c.oid::regclass,
                  pg_catalog.md5(pg_catalog.pg_get_viewdef(c.oid)))
      FROM pg_catalog.pg_class c JOIN ns ON ns.oid = c.relnamespace
     WHERE c.relkind IN ('v', 'm')
    UNION ALL
    SELECT format('enum %s %s', t.typname,
                  string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder))
      FROM pg_catalog.pg_type t
      JOIN ns ON ns.oid = t.typnamespace
      JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
     GROUP BY t.typname
    UNION ALL
    SELECT format('policy %s %s %s %s', pol.polrelid::regclass, pol.polname,
                  pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),
                  pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid))
      FROM pg_catalog.pg_policy pol
    UNION ALL
    SELECT format('extension %s %s', extname, extversion)
      FROM pg_catalog.pg_extension
) catalog
ORDER BY line
"""


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
        if path.name not in LOCAL_MIGRATIONS:
            shutil.copy2(path, upstream / path.name)
    ledger = await _migrate(empty_dsn, upstream)
    assert max(ledger) == UPSTREAM_HEAD
    assert not LOCAL_MIGRATIONS & set(ledger)
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


def _dedicated_stack(store, monkeypatch):
    k8s = self_end.SelfEndK8sApi()
    provider = AgentProvisioner()
    provider._k8s_available = True
    provider._core_api = k8s
    provider._namespace = NAMESPACE
    # The retirement operations bind the application's store when built.
    monkeypatch.setattr(main.app.state.resources, "postgres_db", store)
    monkeypatch.setattr(agent_provisioner_module, "agent_provisioner", provider)
    stack = self_end.Stack(store, k8s)
    stack.provider = provider
    return stack


def _use_dedicated(stack, monkeypatch):
    """Reinstall the dedicated provisioner after a warm helper replaced it."""

    monkeypatch.setattr(main.app.state.resources, "postgres_db", stack.db)
    monkeypatch.setattr(agent_provisioner_module, "agent_provisioner", stack.provider)


@pytest.fixture
def stack(store, monkeypatch):
    return _dedicated_stack(store, monkeypatch)


async def _outcomes(db, thread_ids):
    rows = await db.fetch(
        "SELECT to_jsonb(o) AS row FROM thread_runtime_retirement_outcomes o "
        "WHERE thread_id = ANY($1::uuid[]) ORDER BY thread_id, settled_at",
        thread_ids,
    )
    return [json.loads(row["row"]) for row in rows]


async def _function_sources(db):
    rows = await db.fetch(
        "SELECT proname, prosrc FROM pg_proc WHERE proname IN "
        "('capture_retired_pinned_agent_pod',"
        "'validate_thread_agent_warm_binding_protection')"
    )
    return {row["proname"]: row["prosrc"] for row in rows}


async def _warm_rows(db):
    rows = await db.fetch(
        "SELECT to_jsonb(w) AS row FROM thread_agent_warm_binding_protections w "
        "ORDER BY protection_id"
    )
    return [json.loads(row["row"]) for row in rows]


@pytest.mark.asyncio
async def test_upgrade_from_upstream_head_applies_only_the_r32_migrations(
    stack, empty_dsn, upstream_ledger
):
    db = stack.db
    before = await _function_sources(db)

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

    # Only 0301 and 0302 were applied; every applied file kept its checksum.
    assert set(ledger) - set(upstream_ledger) == LOCAL_MIGRATIONS
    assert {name: ledger[name] for name in upstream_ledger} == upstream_ledger
    assert max(ledger) == WARM_RELEASE_MIGRATION
    after = await _function_sources(db)
    assert (
        set(after)
        == set(before)
        == {
            "capture_retired_pinned_agent_pod",
            "validate_thread_agent_warm_binding_protection",
        }
    )
    assert all(after[name] != before[name] for name in after)
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
    assert set(ledger) - set(upstream_ledger) == LOCAL_MIGRATIONS
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


@pytest.mark.asyncio
async def test_upgrade_keeps_warm_history_and_settles_a_live_warm_life(
    store, empty_dsn, upstream_ledger, monkeypatch
):
    db = store
    # Warm history written before the upgrade: a pool Pod's own End released
    # its protection (the soft path is unchanged by 0302).
    ended, _, _, ended_stack = await self_end._bind_warm_life(db, monkeypatch)
    await self_end._warm_agent_self_end(ended_stack, ended)
    history = await _warm_rows(db)
    assert [(row["status"], row["release_outcome"]) for row in history] == [
        ("released", "exact_live_unprotected_v1")
    ]

    ledger = await _migrate(empty_dsn, MIGRATIONS)

    assert set(ledger) - set(upstream_ledger) == LOCAL_MIGRATIONS
    assert await _warm_rows(db) == history

    # A live warm life bound after the upgrade: its permanent Delete settles
    # the protection it captured instead of leaving it ``bound``.
    live, api, _, stack = await self_end._bind_warm_life(db, monkeypatch)
    handoff = await self_end._owner_permanent_then_agent_ack(stack, live)
    assert handoff.get("retiring_agent_exit_authorized") is True
    api.mark_terminal("agents-a", live["pod_name"])
    result = await self_end._durable_retry(stack, live)

    assert result.get("status") == "deleted", result
    await self_end._assert_warm_ledger_settled(db, live, outcome="exact_absent_v1")
    assert [
        row for row in await _warm_rows(db) if row["thread_id"] == ended["thread"]
    ] == history


# ---------------------------------------------------------------------------
# The deployed local history: the pair applied as 0286/0287
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def owned_databases(server_dsn):
    """Create databases owned by ordinary logins, as the deployed servers are.

    The migration runner connects as the owner, so every function the chain
    creates or replaces belongs to it. Test records are written through the
    server administrator, which some shared fixtures need.
    """

    created = []
    base, host = server_dsn.rsplit("/", 1)[0], server_dsn.split("@", 1)[1]
    host = host.rsplit("/", 1)[0]

    async def new():
        name = f"hist_{uuid4().hex[:12]}"
        admin = await asyncpg.connect(server_dsn)
        try:
            await admin.execute(f"CREATE ROLE {name} LOGIN PASSWORD '{name}'")
            await admin.execute(f"CREATE DATABASE {name} OWNER {name}")
        finally:
            await admin.close()
        created.append(name)
        return NS(
            name=name,
            owner_dsn=f"postgresql://{name}:{name}@{host}/{name}",
            admin_dsn=f"{base}/{name}",
        )

    yield new
    admin = await asyncpg.connect(server_dsn)
    try:
        for name in created:
            await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            await admin.execute(f'DROP ROLE IF EXISTS "{name}"')
    finally:
        await admin.close()


def _stage(tmp_path, name, *, through):
    staged = tmp_path / name
    staged.mkdir()
    for path in migrate.discover(MIGRATIONS):
        if path.name.split("_", 1)[0] <= through:
            shutil.copy2(path, staged / path.name)
    return staged


def _stage_deployed_history(tmp_path, name="deployed-history"):
    """131dd22ee's app chain: everything through 0285 plus the historical pair."""

    staged = _stage(tmp_path, name, through=DEPLOYED_HISTORY_THROUGH)
    for historical, (canonical, checksum) in DEPLOYED_PAIR.items():
        shutil.copy2(MIGRATIONS / canonical, staged / historical)
        assert migrate._checksum((staged / historical).read_text()) == checksum
    return staged


async def _run_as_owner(database, directory, *, dry_run=False):
    pool = await asyncpg.create_pool(database.owner_dsn, min_size=1, max_size=2)
    try:
        assert await pool.fetchval("SHOW is_superuser") == "off"
        await migrate.run_migrations(pool, directory, dry_run=dry_run)
    finally:
        await pool.close()


async def _fetch(dsn, query, *args):
    conn = await asyncpg.connect(dsn)
    try:
        return await conn.fetch(query, *args)
    finally:
        await conn.close()


async def _ledger(database):
    return [dict(row) for row in await _fetch(database.admin_dsn, LEDGER_QUERY)]


async def _catalog(database):
    return [row["line"] for row in await _fetch(database.admin_dsn, CATALOG_QUERY)]


async def _pair_functions(database):
    return [
        dict(row)
        for row in await _fetch(
            database.admin_dsn, FUNCTION_IDENTITY_QUERY, list(PAIR_FUNCTIONS)
        )
    ]


async def _application_data(database):
    """Every row of every application table, with its column set."""

    tables = await _fetch(
        database.admin_dsn,
        "SELECT c.oid::regclass::text AS name, "
        "ARRAY(SELECT a.attname::text FROM pg_catalog.pg_attribute a "
        "       WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped "
        "       ORDER BY a.attnum) AS columns "
        "FROM pg_catalog.pg_class c "
        "WHERE c.relnamespace = 'public'::regnamespace "
        "AND c.relkind IN ('r', 'p') AND NOT c.relispartition "
        "AND c.relname <> 'schema_migrations' ORDER BY 1",
    )
    data = {}
    for table in tables:
        rows = await _fetch(
            database.admin_dsn,
            f"SELECT to_jsonb(t)::text AS row FROM {table['name']} t",
        )
        data[table["name"]] = (
            list(table["columns"]),
            [json.loads(row["row"]) for row in rows],
        )
    return data


def _project(before, after):
    """``after`` restricted to the tables and columns ``before`` had."""

    projected = {}
    for table, (columns, _) in before.items():
        _, rows = after[table]
        projected[table] = (
            columns,
            sorted(
                ({key: row[key] for key in columns} for row in rows),
                key=lambda row: json.dumps(row, sort_keys=True),
            ),
        )
    return projected


def _sorted(data):
    return {
        table: (
            columns,
            sorted(rows, key=lambda row: json.dumps(row, sort_keys=True)),
        )
        for table, (columns, rows) in data.items()
    }


async def _open_store(database, monkeypatch):
    monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
    monkeypatch.setenv("APP_ENCRYPTION_KEY", "P" * 32)
    crypto.reset_cipher_cache()
    db = PostgresDB(
        connection_string=database.admin_dsn, min_connections=1, max_connections=10
    )
    await db.connect()
    return db


async def _warm_permanent_delete(store, monkeypatch):
    live, api, _, warm_stack = await self_end._bind_warm_life(store, monkeypatch)
    handoff = await self_end._owner_permanent_then_agent_ack(warm_stack, live)
    assert handoff.get("retiring_agent_exit_authorized") is True
    api.mark_terminal("agents-a", live["pod_name"])
    result = await self_end._durable_retry(warm_stack, live)
    assert result.get("status") == "deleted", result
    await self_end._assert_warm_ledger_settled(store, live, outcome="exact_absent_v1")
    return live


@pytest.mark.asyncio
async def test_upgrade_from_the_deployed_local_0286_0287_history(
    owned_databases, tmp_path, monkeypatch
):
    through_0285 = _stage(tmp_path, "through-0285", through=DEPLOYED_HISTORY_THROUGH)
    deployed = _stage_deployed_history(tmp_path)
    canonical = {path.name for path in migrate.discover(MIGRATIONS)}
    history = await owned_databases()
    await _run_as_owner(history, through_0285)

    store = await _open_store(history, monkeypatch)
    try:
        stack = _dedicated_stack(store, monkeypatch)
        # Records written before the pair: 0224's capture, 0200's warm check.
        pre_claimed = await self_end._bind_life(
            stack, await self_end._thread(store), with_claim=True
        )
        await self_end._agent_settles_soft_end(stack, pre_claimed)
        pre_claimless = await self_end._bind_life(
            stack, await self_end._thread(store), with_claim=False
        )
        await self_end._agent_settles_soft_end(stack, pre_claimless)
        assert await self_end._outcome_proof(store, pre_claimless) is None
        warm_ended, _, _, warm_stack = await self_end._bind_warm_life(
            store, monkeypatch
        )
        await self_end._warm_agent_self_end(warm_stack, warm_ended)

        # The pair under its historical names, as the k3d database ran it.
        await _run_as_owner(history, deployed)
        deployed_ledger = await _ledger(history)
        assert len(deployed_ledger) == DEPLOYED_LEDGER_ROWS
        assert all(row["success"] for row in deployed_ledger)
        assert max(row["filename"] for row in deployed_ledger) == max(DEPLOYED_PAIR)
        assert {
            row["filename"]: row["checksum"]
            for row in deployed_ledger
            if row["filename"] in DEPLOYED_PAIR
        } == {name: checksum for name, (_, checksum) in DEPLOYED_PAIR.items()}

        # Records written while the historical pair ran: a claim-less life
        # recorded its Pod, and a live warm life's permanent Delete settled its
        # protection (both impossible before the pair).
        _use_dedicated(stack, monkeypatch)
        pair_claimless = await self_end._bind_life(
            stack, await self_end._thread(store), with_claim=False
        )
        await self_end._agent_settles_soft_end(stack, pair_claimless)
        assert (await self_end._outcome_proof(store, pair_claimless))[
            "pod_uid"
        ] == pair_claimless["pod_uid"]
        await _warm_permanent_delete(store, monkeypatch)

        functions_before = await _pair_functions(history)
        assert [row["owner"] for row in functions_before] == [history.name] * 2
        catalog_before = await _catalog(history)
        data_before = await _application_data(history)

        # A dry run of the published chain is observational.
        await _run_as_owner(history, MIGRATIONS, dry_run=True)
        assert await _ledger(history) == deployed_ledger
        assert await _catalog(history) == catalog_before

        await _run_as_owner(history, MIGRATIONS)
        upgraded = await _ledger(history)
        deployed_names = {row["filename"] for row in deployed_ledger}
        # Every deployed row, both historical names included, is untouched.
        assert [
            row for row in upgraded if row["filename"] in deployed_names
        ] == deployed_ledger
        # Everything published that the history lacks applied at its own
        # position: upstream 0286-0300 and the canonical pair.
        assert {row["filename"] for row in upgraded} == canonical | deployed_names
        assert {
            row["filename"]: row["checksum"]
            for row in upgraded
            if row["filename"] in LOCAL_MIGRATIONS
        } == {name: checksum for name, checksum in DEPLOYED_PAIR.values()}
        assert all(row["success"] for row in upgraded)
        # The canonical files replaced both functions in place: same object,
        # owner, source and trigger bindings as the historical pair left them.
        assert await _pair_functions(history) == functions_before
        # Application data is preserved; upstream only added columns.
        data_after = await _application_data(history)
        assert _project(data_before, data_after) == _sorted(data_before)

        # The same schema as a fresh installation of the published chain.
        fresh = await owned_databases()
        await _run_as_owner(fresh, MIGRATIONS)
        fresh_ledger = await _ledger(fresh)
        assert {row["filename"] for row in fresh_ledger} == canonical
        assert not (deployed_names - canonical) & {
            row["filename"] for row in fresh_ledger
        }
        catalog_after = await _catalog(history)
        assert catalog_after == await _catalog(fresh)
        assert [
            (row["proname"], row["source"], row["triggers"])
            for row in await _pair_functions(fresh)
        ] == [
            (row["proname"], row["source"], row["triggers"]) for row in functions_before
        ]

        # A second startup changes nothing.
        await _run_as_owner(history, MIGRATIONS)
        assert await _ledger(history) == upgraded
        assert await _catalog(history) == catalog_after

        # Retirement after the upgrade: a new claim-less life records its Pod;
        # permanent Delete retires exactly the Pods the records prove (the
        # pre-pair claim-less life has none, so its Pod keeps its protection);
        # a live warm life's permanent Delete settles its protection.
        _use_dedicated(stack, monkeypatch)
        fresh_claimless = await self_end._bind_life(
            stack, await self_end._thread(store), with_claim=False
        )
        await self_end._agent_settles_soft_end(stack, fresh_claimless)
        assert (await self_end._outcome_proof(store, fresh_claimless))[
            "pod_uid"
        ] == fresh_claimless["pod_uid"]
        lives = (pre_claimed, pre_claimless, pair_claimless, fresh_claimless)
        for life in lives:
            stack.k8s.exit_and_reap(life)
            outcomes = await self_end._delete_until_settled(stack, life["thread"])
            assert outcomes[-1] == "deleted", outcomes
            assert await store.get_thread(life["thread"]) is None
        assert sorted(stack.k8s.removed_pods) == sorted(
            life["pod_uid"] for life in (pre_claimed, pair_claimless, fresh_claimless)
        )
        kept = self_end._pod(stack, pre_claimless)
        assert kept is not None
        assert kept.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
        await _warm_permanent_delete(store, monkeypatch)
    finally:
        await store.close()
        crypto.reset_cipher_cache()


def test_renamed_history_contract_is_exact_and_replay_safe():
    """Pin the reviewed rename, and the property its replay relies on."""

    on_disk = {path.name for path in migrate.discover(MIGRATIONS)}
    assert RENAMED_APPLIED_MIGRATIONS == {
        historical: RenamedAppliedMigration(
            canonical_filename=canonical, checksum=checksum
        )
        for historical, (canonical, checksum) in DEPLOYED_PAIR.items()
    }
    for historical, renamed in RENAMED_APPLIED_MIGRATIONS.items():
        assert historical not in on_disk
        assert renamed.canonical_filename in on_disk
        sql = (MIGRATIONS / renamed.canonical_filename).read_text()
        assert migrate._checksum(sql) == renamed.checksum
        # One statement replacing an existing trigger function; no data.
        statements = migrate._top_level_sql_statements(sql)
        assert len(statements) == 1
        assert migrate._leading_sql_keywords(sql, limit=4) == (
            "CREATE",
            "OR",
            "REPLACE",
            "FUNCTION",
        )
        assert "RETURNS trigger" in sql


async def _set_function_owner(database, name, owner=None):
    admin = await asyncpg.connect(database.admin_dsn)
    try:
        quoted = await admin.fetchval(
            "SELECT pg_catalog.quote_ident(COALESCE($1::text, current_user))", owner
        )
        await admin.execute(f"ALTER FUNCTION public.{name}() OWNER TO {quoted}")
    finally:
        await admin.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "unregistered_name",
        "other_historical_checksum",
        "failed_historical_row",
        "changed_canonical_file",
        "missing_canonical_file",
    ],
)
async def test_renamed_history_refuses_anything_but_the_reviewed_rows(
    owned_databases, tmp_path, mutation
):
    capture_row, warm_row = DEPLOYED_PAIR
    history = _stage_deployed_history(tmp_path)
    published = tmp_path / "published"
    shutil.copytree(MIGRATIONS, published)
    database = await owned_databases()
    if mutation == "unregistered_name":
        renamed = "0286_capture_claimless_retired_agent_pod_v2.sql"
        (history / capture_row).rename(history / renamed)
        expected = rf"applied but missing on disk: \['{renamed}'\]"
    elif mutation == "other_historical_checksum":
        path = history / capture_row
        path.write_text(path.read_text() + "\n-- unreviewed historical edit\n")
        expected = rf"checksum changed: {capture_row}"
    elif mutation == "failed_historical_row":
        expected = rf"dirty migration '{capture_row}'"
    elif mutation == "changed_canonical_file":
        path = published / CAPTURE_MIGRATION
        path.write_text(path.read_text() + "\n-- unreviewed published edit\n")
        expected = rf"renamed migration {capture_row} requires {CAPTURE_MIGRATION}"
    elif mutation == "missing_canonical_file":
        (published / WARM_RELEASE_MIGRATION).unlink()
        expected = rf"renamed migration {warm_row} requires {WARM_RELEASE_MIGRATION}"
    else:
        raise AssertionError(mutation)

    if mutation == "failed_historical_row":
        # PostgreSQL refuses the owner's CREATE OR REPLACE of a function it
        # does not own; the runner records the failure with the exact
        # historical checksum. Restoring ownership afterwards must not turn
        # that failed row into replay authority.
        await _run_as_owner(
            database,
            _stage(tmp_path, "through-0285", through=DEPLOYED_HISTORY_THROUGH),
        )
        await _set_function_owner(database, PAIR_FUNCTIONS[0])
        with pytest.raises(asyncpg.InsufficientPrivilegeError, match="must be owner"):
            await _run_as_owner(database, history)
        await _set_function_owner(database, PAIR_FUNCTIONS[0], database.name)
    else:
        await _run_as_owner(database, history)
    ledger = await _ledger(database)
    catalog = await _catalog(database)
    if mutation == "failed_historical_row":
        failed = next(row for row in ledger if row["filename"] == capture_row)
        assert failed["success"] is False
        assert failed["checksum"] == DEPLOYED_PAIR[capture_row][1]

    for dry_run in (False, True):
        with pytest.raises(RuntimeError, match=expected):
            await _run_as_owner(database, published, dry_run=dry_run)
        assert await _ledger(database) == ledger
        assert await _catalog(database) == catalog


# ---------------------------------------------------------------------------
# The k3d database after the 2026-09-28 manual ledger repair
# ---------------------------------------------------------------------------

# On 2026-09-28 a Tilt deploy of local develop 50d0af34a met the deployed
# history above and refused it. A concurrent session then deleted the two
# historical rows by hand (after saving the ledger) and restarted: upstream's
# 0286-0300 applied, and the pair re-ran as 0301/0302 from 50d0af34a, whose
# 0301 carried a renumbered header comment. A read-only inspection afterwards
# found 296 rows equal to the published chain except that one checksum.
REPAIRED_0301_CHECKSUM = (
    "587ed9b5edc56bd4946cf0637c679eaba1484ce5237da7f45b1873542fe838e5"
)
ORIGINAL_0301_HEADER = (
    "-- (Numbered 0286: origin/develop already carries 0284 and 0285.)\n"
)
RENUMBERED_0301_HEADER = (
    "-- (Numbered 0301: written as 0286, renumbered at integration because\n"
    "-- origin/develop carries 0286-0300.)\n"
)
# The deleted rows exactly as the saved ledger holds them.
DELETED_HISTORICAL_ROWS = (
    ("0286_capture_claimless_retired_agent_pod.sql", 0),
    ("0287_permanent_retirement_releases_warm_protection.sql", 2),
)
DELETED_ROWS_APPLIED_AT = "2026-09-27 07:47:57.62744+00"


def _renumbered_0301_sql():
    original = (MIGRATIONS / CAPTURE_MIGRATION).read_text()
    assert original.count(ORIGINAL_0301_HEADER) == 1
    variant = original.replace(ORIGINAL_0301_HEADER, RENUMBERED_0301_HEADER, 1)
    assert migrate._checksum(variant) == REPAIRED_0301_CHECKSUM
    return variant


def _stage_develop_50d0af34a(tmp_path):
    """The published chain as local develop 50d0af34a carried it."""

    staged = tmp_path / "develop-50d0af34a"
    shutil.copytree(MIGRATIONS, staged)
    (staged / CAPTURE_MIGRATION).write_text(_renumbered_0301_sql())
    return staged


def test_repaired_0301_variant_is_pinned_and_differs_only_in_its_header():
    assert APPLIED_CHECKSUM_COMPATIBILITIES[
        CAPTURE_MIGRATION
    ] == AppliedChecksumCompatibility(
        canonical_checksum=DEPLOYED_PAIR[
            "0286_capture_claimless_retired_agent_pod.sql"
        ][1],
        historical_checksum=REPAIRED_0301_CHECKSUM,
    )
    original = (MIGRATIONS / CAPTURE_MIGRATION).read_text()
    variant = _renumbered_0301_sql()
    # Same executable statement: only the leading comment differs.
    for sql in (original, variant):
        assert len(migrate._top_level_sql_statements(sql)) == 1
    start = original.index("CREATE OR REPLACE FUNCTION")
    assert variant[variant.index("CREATE OR REPLACE FUNCTION") :] == original[start:]


@pytest.mark.asyncio
@pytest.mark.parametrize("restored_rows", [False, True])
async def test_upgrade_from_the_repaired_k3d_history(
    owned_databases, tmp_path, restored_rows
):
    database = await owned_databases()
    await _run_as_owner(database, _stage_deployed_history(tmp_path))
    admin = await asyncpg.connect(database.admin_dsn)
    try:
        # The manual repair: delete exactly the two historical rows.
        deleted = await admin.fetch(
            "DELETE FROM public.schema_migrations "
            "WHERE filename = ANY($1::text[]) RETURNING filename, checksum",
            list(DEPLOYED_PAIR),
        )
        assert {row["filename"]: row["checksum"] for row in deleted} == {
            name: checksum for name, (_, checksum) in DEPLOYED_PAIR.items()
        }
    finally:
        await admin.close()
    await _run_as_owner(database, _stage_develop_50d0af34a(tmp_path))
    ledger = await _ledger(database)
    canonical = {path.name for path in migrate.discover(MIGRATIONS)}
    assert {row["filename"] for row in ledger} == canonical
    assert {
        row["filename"]: row["checksum"]
        for row in ledger
        if row["filename"] in LOCAL_MIGRATIONS
    } == {
        CAPTURE_MIGRATION: REPAIRED_0301_CHECKSUM,
        WARM_RELEASE_MIGRATION: DEPLOYED_PAIR[
            "0287_permanent_retirement_releases_warm_protection.sql"
        ][1],
    }
    if restored_rows:
        # The optional restoration of the deleted rows from the saved ledger.
        admin = await asyncpg.connect(database.admin_dsn)
        try:
            for filename, execution_ms in DELETED_HISTORICAL_ROWS:
                await admin.execute(
                    "INSERT INTO public.schema_migrations"
                    "(filename, checksum, applied_at, applied_by, execution_ms) "
                    "VALUES ($1, $2, $3::text::timestamptz, 'srw', $4)",
                    filename,
                    DEPLOYED_PAIR[filename][1],
                    DELETED_ROWS_APPLIED_AT,
                    execution_ms,
                )
        finally:
            await admin.close()
        ledger = await _ledger(database)
    # A replay of either function would now fail at PostgreSQL's ownership
    # boundary, so a passing upgrade proves nothing re-ran.
    for name in PAIR_FUNCTIONS:
        await _set_function_owner(database, name)
    catalog = await _catalog(database)
    data = await _application_data(database)

    for dry_run in (True, False, False):
        await _run_as_owner(database, MIGRATIONS, dry_run=dry_run)
        assert await _ledger(database) == ledger
        assert await _catalog(database) == catalog
        assert await _application_data(database) == data

    fresh = await owned_databases()
    await _run_as_owner(fresh, MIGRATIONS)
    for name in PAIR_FUNCTIONS:
        await _set_function_owner(fresh, name)
    assert await _catalog(fresh) == catalog


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["other_variant", "reverse_variant"])
async def test_repaired_k3d_history_refuses_other_0301_bytes(
    owned_databases, tmp_path, mutation
):
    history = _stage_develop_50d0af34a(tmp_path)
    published = tmp_path / "published"
    shutil.copytree(MIGRATIONS, published)
    if mutation == "other_variant":
        path = history / CAPTURE_MIGRATION
        path.write_text(path.read_text() + "\n-- unreviewed historical edit\n")
    elif mutation == "reverse_variant":
        # Only the original bytes are canonical: a database that applied them
        # must not accept the renumbered variant on disk.
        (history / CAPTURE_MIGRATION).write_text(
            (MIGRATIONS / CAPTURE_MIGRATION).read_text()
        )
        (published / CAPTURE_MIGRATION).write_text(_renumbered_0301_sql())
    else:
        raise AssertionError(mutation)
    database = await owned_databases()
    await _run_as_owner(database, history)
    ledger = await _ledger(database)
    for dry_run in (False, True):
        with pytest.raises(
            RuntimeError, match=f"checksum changed: {CAPTURE_MIGRATION}"
        ):
            await _run_as_owner(database, published, dry_run=dry_run)
        assert await _ledger(database) == ledger
