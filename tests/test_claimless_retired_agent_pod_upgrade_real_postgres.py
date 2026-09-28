"""Upgrade every supported history to the reconciled R3.2 retirement chain.

R3.2 wrote two app migrations: the claim-less retired-Pod capture (one
``CREATE OR REPLACE`` of ``capture_retired_pinned_agent_pod()``) and a warm
binding validator whose ``releasing`` branch accepted a deleted owner.
Upstream then published 0301-0305, its own fenced fix of the same warm defect
(``terminal_release``). The published chain keeps upstream's fix, restores
0200's validator where the R3.2 variant is installed (the 0300z interstitial;
upstream's 0301 refuses any other body), and publishes the capture as 0306
with the bytes first applied.

Histories rebuilt here, each on its own PostgreSQL 15 database owned by an
ordinary login like the deployed servers:

* H1 fresh installation;
* H2 upstream 0305 (main dev);
* H3 the local k3d database as first deployed (131dd22ee): the pair applied as
  0286/0287 before upstream's 0286-0300;
* H4 that database today: the two historical rows were deleted by hand, and
  the pair re-ran as 0301/0302 from develop 50d0af34a (a header-only 0301
  variant);
* H5 H4 with the two deleted rows restored from the saved ledger;
* H6 a fresh installation of local develop 9270a8aa1 (0301/0302, original
  bytes).

Retirement records are written through the production End funnel before the
upgrade: claim and claim-less soft End, a warm soft End, a warm permanent
Delete by the historical writer (R3.2's settled ``releasing`` release, or
upstream's in-flight ``terminal_release``) and, where it
existed, a ``bound`` row left at a deleted owner by the pre-fix writer. The
upgrade must keep every historical ledger row and record, reach the same
catalog as H1, replace functions in place and change nothing on a second
start; the refusal matrix covers every contract.

See knowledge-base/knowledge/features/codebase_restructure_r3_execution_2026_09_25.md §13.
"""

from __future__ import annotations

import json
import shutil
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace as NS
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator import main
from orchestrator.database import migrate
from orchestrator.database.migration_recovery import (
    APPLIED_CHECKSUM_COMPATIBILITIES,
    RENAMED_APPLIED_MIGRATIONS,
    SUPERSEDED_APPLIED_MIGRATIONS,
    RenamedAppliedMigration,
    SupersededAppliedMigration,
)
from orchestrator.database.postgres import PostgresDB
from orchestrator.services import agent_provisioner as agent_provisioner_module
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.pinned_agent_authority import (
    reconcile_pinned_warm_binding_protections,
    release_pinned_warm_binding_protection,
)
from orchestrator.services.pinned_k8s_effect import PINNED_AUTHORITY_FINALIZER
from orchestrator.security import crypto
from tests import test_pinned_permanent_warm_release_real_postgres as upstream_warm
from tests import test_self_ended_pinned_retirement_real_postgres as self_end

MIGRATIONS = self_end.authority_fixtures.SCHEMA_FILE.parent / "migrations" / "app"
FIXTURES = Path(__file__).parent / "fixtures" / "migrations"
NAMESPACE = self_end.NAMESPACE

CAPTURE_MIGRATION = "0306_capture_claimless_retired_agent_pod.sql"
BRIDGE_MIGRATION = "0300z_restore_upstream_warm_binding_validator.sql"
UPSTREAM_WARM_MIGRATION = "0301_pinned_permanent_warm_release.sql"
RECONCILED_MIGRATIONS = {BRIDGE_MIGRATION, CAPTURE_MIGRATION}
UPSTREAM_HEAD = "0305_validate_pinned_permanent_warm_release.sql"

CAPTURE_CHECKSUM = "a2d08b52d9197d52e43da0859bf328be91c16fbb94feea31c1cdf2e1694bac2e"
VARIANT_CAPTURE_CHECKSUM = (
    "587ed9b5edc56bd4946cf0637c679eaba1484ce5237da7f45b1873542fe838e5"
)
R32_VALIDATOR_CHECKSUM = (
    "c8df370587940ecebc93c0c53a4ff48e29e1d761018fb5b78e318321f2be6d8f"
)
R32_VALIDATOR_SQL = (
    FIXTURES / "app_r32_permanent_retirement_releases_warm_protection.sql"
).read_text()

CAPTURE_0286 = "0286_capture_claimless_retired_agent_pod.sql"
VALIDATOR_0287 = "0287_permanent_retirement_releases_warm_protection.sql"
CAPTURE_0301 = "0301_capture_claimless_retired_agent_pod.sql"
VALIDATOR_0302 = "0302_permanent_retirement_releases_warm_protection.sql"

# 131dd22ee's app chain as the k3d database recorded it: 281 rows ending with
# the pair under its first historical names.
DEPLOYED_HISTORY_THROUGH = "0285"
DEPLOYED_LEDGER_ROWS = 281
# That database after the 2026-09-28 manual repair: upstream's 0286-0300, and
# the pair again as 0301/0302 from 50d0af34a.
REPAIRED_LEDGER_ROWS = 296
ORIGINAL_CAPTURE_HEADER = (
    "-- (Numbered 0286: origin/develop already carries 0284 and 0285.)\n"
)
RENUMBERED_CAPTURE_HEADER = (
    "-- (Numbered 0301: written as 0286, renumbered at integration because\n"
    "-- origin/develop carries 0286-0300.)\n"
)
# The deleted rows exactly as the saved ledger holds them.
DELETED_HISTORICAL_ROWS = (
    (CAPTURE_0286, CAPTURE_CHECKSUM, 0),
    (VALIDATOR_0287, R32_VALIDATOR_CHECKSUM, 2),
)
DELETED_ROWS_APPLIED_AT = "2026-09-27 07:47:57.62744+00"

RECONCILED_FUNCTIONS = (
    "capture_retired_pinned_agent_pod",
    "validate_thread_agent_warm_binding_protection",
)
# The terminal warm release as the published delete writes it, and R3.2's
# ordinary release in its place (the only statement the historical writer
# changed; its agent detach had the same effect).
TERMINAL_RELEASE_SQL = (
    "UPDATE thread_agent_warm_binding_protections SET "
    "status='terminal_release',"
    "release_started_at=transaction_timestamp() "
    "WHERE protection_id=$1::uuid AND status='bound'"
)
R32_RELEASE_SQL = TERMINAL_RELEASE_SQL.replace("'terminal_release'", "'releasing'")

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


# ---------------------------------------------------------------------------
# Servers, databases and staged migration directories
# ---------------------------------------------------------------------------


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


def _stage(tmp_path, name, *, through, exclude=()):
    staged = tmp_path / name
    staged.mkdir()
    for path in migrate.discover(MIGRATIONS):
        if path.name.split("_", 1)[0] <= through and path.name not in exclude:
            shutil.copy2(path, staged / path.name)
    return staged


def _variant_capture_sql():
    original = (MIGRATIONS / CAPTURE_MIGRATION).read_text()
    assert original.count(ORIGINAL_CAPTURE_HEADER) == 1
    variant = original.replace(ORIGINAL_CAPTURE_HEADER, RENUMBERED_CAPTURE_HEADER, 1)
    assert migrate._checksum(variant) == VARIANT_CAPTURE_CHECKSUM
    return variant


def _stage_upstream(tmp_path):
    """origin/develop 13c904ea6: the published chain without this reconciliation."""

    staged = _stage(tmp_path, "upstream", through="9999", exclude=RECONCILED_MIGRATIONS)
    assert max(path.name for path in staged.iterdir()) == UPSTREAM_HEAD
    return staged


def _stage_original_local(tmp_path, name="original-local"):
    """131dd22ee's chain: everything through 0285 plus the pair as 0286/0287."""

    staged = _stage(tmp_path, name, through=DEPLOYED_HISTORY_THROUGH)
    (staged / CAPTURE_0286).write_text((MIGRATIONS / CAPTURE_MIGRATION).read_text())
    (staged / VALIDATOR_0287).write_text(R32_VALIDATOR_SQL)
    assert migrate._checksum((staged / CAPTURE_0286).read_text()) == CAPTURE_CHECKSUM
    assert (
        migrate._checksum((staged / VALIDATOR_0287).read_text())
        == R32_VALIDATOR_CHECKSUM
    )
    return staged


def _stage_local_candidate(tmp_path, name, *, capture_sql):
    """Upstream's 0286-0300 plus the pair as 0301/0302 (50d0af34a or 9270a8aa1)."""

    staged = _stage(tmp_path, name, through="0300")
    (staged / CAPTURE_0301).write_text(capture_sql)
    (staged / VALIDATOR_0302).write_text(R32_VALIDATOR_SQL)
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


async def _execute(dsn, query, *args):
    conn = await asyncpg.connect(dsn)
    try:
        return await conn.execute(query, *args)
    finally:
        await conn.close()


async def _ledger(database):
    return [dict(row) for row in await _fetch(database.admin_dsn, LEDGER_QUERY)]


async def _catalog(database):
    return [row["line"] for row in await _fetch(database.admin_dsn, CATALOG_QUERY)]


async def _reconciled_functions(database):
    return [
        dict(row)
        for row in await _fetch(
            database.admin_dsn, FUNCTION_IDENTITY_QUERY, list(RECONCILED_FUNCTIONS)
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


async def _fresh_reference(owned_databases):
    fresh = await owned_databases()
    await _run_as_owner(fresh, MIGRATIONS)
    return fresh


# ---------------------------------------------------------------------------
# Retirement records, written through the production funnel
# ---------------------------------------------------------------------------


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


class _R32Connection:
    """A pool connection whose terminal warm release is R3.2's ``releasing``."""

    def __init__(self, conn, calls):
        self._conn = conn
        self._calls = calls

    def __getattr__(self, name):
        return getattr(self._conn, name)

    async def execute(self, query, *args, **kwargs):
        if query == TERMINAL_RELEASE_SQL:
            self._calls.append(args)
            query = R32_RELEASE_SQL
        return await self._conn.execute(query, *args, **kwargs)


@contextmanager
def _r32_writer(store, monkeypatch):
    """Run the published delete as R3.2's writer did (``releasing``)."""

    from contextlib import asynccontextmanager

    calls = []
    original = store.acquire

    @asynccontextmanager
    async def acquire(*args, **kwargs):
        async with original(*args, **kwargs) as conn:
            yield _R32Connection(conn, calls)

    monkeypatch.setattr(store, "acquire", acquire)
    try:
        yield calls
    finally:
        monkeypatch.setattr(store, "acquire", original)


async def _warm_rows(store, thread_id=None):
    rows = await store.fetch(
        "SELECT to_jsonb(w) AS row FROM thread_agent_warm_binding_protections w "
        "WHERE $1::uuid IS NULL OR thread_id=$1::uuid ORDER BY protection_id",
        thread_id,
    )
    return [json.loads(row["row"]) for row in rows]


async def _warm_permanent_delete(store, monkeypatch, *, writer, complete=True):
    """A live warm life's permanent Delete by one writer generation.

    ``published``: upstream's ``terminal_release``; ``r32``: R3.2's
    ``releasing`` plus its immediate release after the delete. Without
    ``complete`` the release is left where a failed follow-up would leave it.
    """

    live, api, provisioner, warm_stack = await self_end._bind_warm_life(
        store, monkeypatch
    )
    handoff = await self_end._owner_permanent_then_agent_ack(warm_stack, live)
    assert handoff.get("retiring_agent_exit_authorized") is True
    api.mark_terminal("agents-a", live["pod_name"])
    if writer == "r32":
        with _r32_writer(store, monkeypatch) as calls:
            result = await self_end._durable_retry(warm_stack, live)
        assert len(calls) == 1
    else:
        result = await self_end._durable_retry(warm_stack, live)
    assert result.get("status") == "deleted", result
    assert await store.get_thread(live["thread"]) is None
    (row,) = await _warm_rows(store, live["thread"])
    assert row["status"] == ("releasing" if writer == "r32" else "terminal_release")
    if writer == "r32" and complete:
        assert await release_pinned_warm_binding_protection(
            store,
            protection_id=row["protection_id"],
            agent_provisioner=provisioner,
            persistent_provisioner=None,
        )
        await self_end._assert_warm_ledger_settled(
            store, live, outcome="exact_absent_v1"
        )
    return NS(life=live, api=api, provisioner=provisioner)


async def _pre_fix_deleted_owner(store, monkeypatch):
    """A ``bound`` row the pre-fix permanent Delete left at a deleted owner."""

    ids, api, provider = await upstream_warm._legacy_deleted_owner(store, monkeypatch)
    ids["thread"] = str(ids["thread"])
    return NS(life=ids, api=api, provisioner=provider)


async def _expire_warm_lease(store, thread_id):
    async with store.acquire() as conn:
        async with conn.transaction():
            # Fixture time travel only: the reconciler takes expired leases.
            await conn.execute("SET LOCAL session_replication_role='replica'")
            await conn.execute(
                "UPDATE thread_agent_warm_binding_protections "
                "SET lease_expires_at=created_at+interval '1 millisecond' "
                "WHERE thread_id=$1::uuid",
                UUID(str(thread_id)),
            )


async def _reconcile_to_released(store, warm):
    """The leader's reconciler settles a deleted owner's protection exactly."""

    thread_id = str(warm.life["thread"])
    await _expire_warm_lease(store, thread_id)
    pod = warm.api.pods.get(("agents-a", warm.life["pod_name"]))
    if pod is not None:
        warm.api.mark_terminal("agents-a", warm.life["pod_name"])
        pod.metadata.deletion_timestamp = "now"
    # One fake cluster per life: scope the pass to this life's row.
    await reconcile_pinned_warm_binding_protections(
        store,
        agent_provisioner=warm.provisioner,
        persistent_provisioner=None,
        thread_id=thread_id,
    )
    (row,) = await _warm_rows(store, thread_id)
    assert (row["status"], row["release_outcome"]) == ("released", "exact_absent_v1"), (
        row
    )
    assert ("agents-a", warm.life["pod_name"]) not in warm.api.pods
    agent = await store.fetchrow(
        "SELECT status::text AS status,thread_id FROM agents WHERE id=$1::uuid",
        UUID(str(warm.life["agent"])),
    )
    # Never returned to the pool.
    assert agent is None or dict(agent) == {"status": "offline", "thread_id": None}


async def _soft_lives(stack):
    """A claim-bearing and a claim-less dedicated life, each ended by itself."""

    store = stack.db
    claimed = await self_end._bind_life(
        stack, await self_end._thread(store), with_claim=True
    )
    await self_end._agent_settles_soft_end(stack, claimed)
    claimless = await self_end._bind_life(
        stack, await self_end._thread(store), with_claim=False
    )
    await self_end._agent_settles_soft_end(stack, claimless)
    return claimed, claimless


async def _warm_soft_end(store, monkeypatch):
    ended, _, _, ended_stack = await self_end._bind_warm_life(store, monkeypatch)
    await self_end._warm_agent_self_end(ended_stack, ended)
    return ended


async def _assert_warm_fences(database, store, monkeypatch):
    """Upstream's cross-replica fences on an upgraded database."""

    rows = await _fetch(
        database.admin_dsn,
        "SELECT conname, convalidated, pg_get_constraintdef(oid) AS def "
        "FROM pg_constraint WHERE conrelid="
        "'thread_agent_warm_binding_protections'::regclass AND conname IN ("
        "'thread_agent_warm_binding_protections_status_check',"
        "'thread_agent_warm_binding_protections_check2') ORDER BY 1",
    )
    assert [row["convalidated"] for row in rows] == [True, True]
    assert all("terminal_release" in row["def"] for row in rows)
    indexes = await _fetch(
        database.admin_dsn,
        "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
        "WHERE i.indisvalid AND i.indisready "
        "AND pg_get_expr(i.indpred,i.indrelid) LIKE '%terminal_release%' "
        "ORDER BY 1",
    )
    assert [row["relname"] for row in indexes] == [
        "idx_thread_agent_warm_binding_agent_active_v2",
        "idx_thread_agent_warm_binding_reconcile_v2",
        "idx_thread_agent_warm_binding_thread_active_v2",
    ]

    # A live warm life: the published delete leaves ``terminal_release``, which
    # holds the actor out of the pool until the exact Pod is proven absent.
    warm = await _warm_permanent_delete(store, monkeypatch, writer="published")
    async with store.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError) as denied:
            await conn.execute(
                "UPDATE agents SET status='ready' WHERE id=$1::uuid",
                UUID(str(warm.life["agent"])),
            )
    assert denied.value.constraint_name == "agents_pinned_warm_binding_authority"
    await _reconcile_to_released(store, warm)

    # An R3.2 replica still running after the upgrade is refused: its ordinary
    # release at a deleted owner no longer validates, so the delete rolls back
    # and stays retryable instead of freeing the Pod through the soft path.
    old = await _bind_and_ack_warm(store, monkeypatch)
    with _r32_writer(store, monkeypatch) as calls:
        try:
            refused = await self_end._durable_retry(old.stack, old.life)
        except Exception as exc:  # the rolled-back delete may surface either way
            refused = {"error": type(exc).__name__}
    assert len(calls) >= 1
    assert refused.get("status") != "deleted", refused
    assert await store.get_thread(old.life["thread"]) is not None
    (row,) = await _warm_rows(store, old.life["thread"])
    assert row["status"] == "bound"
    # The published writer then completes the same retirement.
    result = await self_end._durable_retry(old.stack, old.life)
    assert result.get("status") == "deleted", result
    await _reconcile_to_released(store, old)


async def _bind_and_ack_warm(store, monkeypatch):
    live, api, provisioner, warm_stack = await self_end._bind_warm_life(
        store, monkeypatch
    )
    handoff = await self_end._owner_permanent_then_agent_ack(warm_stack, live)
    assert handoff.get("retiring_agent_exit_authorized") is True
    api.mark_terminal("agents-a", live["pod_name"])
    return NS(life=live, api=api, provisioner=provisioner, stack=warm_stack)


# ---------------------------------------------------------------------------
# Contracts
# ---------------------------------------------------------------------------


def test_reconciled_contracts_are_exact():
    """Pin every reviewed name, checksum and the properties they rely on."""

    on_disk = {path.name: path for path in migrate.discover(MIGRATIONS)}
    assert CAPTURE_MIGRATION in on_disk and BRIDGE_MIGRATION in on_disk
    assert not {CAPTURE_0286, CAPTURE_0301, VALIDATOR_0287, VALIDATOR_0302} & set(
        on_disk
    )
    assert migrate._checksum(on_disk[CAPTURE_MIGRATION].read_text()) == CAPTURE_CHECKSUM
    assert RENAMED_APPLIED_MIGRATIONS == {
        CAPTURE_0286: RenamedAppliedMigration(
            canonical_filename=CAPTURE_MIGRATION,
            checksum=CAPTURE_CHECKSUM,
            historical_checksums=(CAPTURE_CHECKSUM,),
        ),
        CAPTURE_0301: RenamedAppliedMigration(
            canonical_filename=CAPTURE_MIGRATION,
            checksum=CAPTURE_CHECKSUM,
            historical_checksums=(CAPTURE_CHECKSUM, VARIANT_CAPTURE_CHECKSUM),
        ),
    }
    superseded_by = (
        (BRIDGE_MIGRATION, migrate._checksum(on_disk[BRIDGE_MIGRATION].read_text())),
        (
            UPSTREAM_WARM_MIGRATION,
            migrate._checksum(on_disk[UPSTREAM_WARM_MIGRATION].read_text()),
        ),
    )
    assert SUPERSEDED_APPLIED_MIGRATIONS == {
        name: SupersededAppliedMigration(
            checksum=R32_VALIDATOR_CHECKSUM, superseded_by=superseded_by
        )
        for name in (VALIDATOR_0287, VALIDATOR_0302)
    }
    # No on-disk file keeps an alternate app checksum any more.
    assert not set(APPLIED_CHECKSUM_COMPATIBILITIES) & set(on_disk)
    assert migrate._checksum(R32_VALIDATOR_SQL) == R32_VALIDATOR_CHECKSUM

    # The renamed statement: one CREATE OR REPLACE of a trigger function, and
    # the k3d variant differs only in its leading comment.
    sql = on_disk[CAPTURE_MIGRATION].read_text()
    assert len(migrate._top_level_sql_statements(sql)) == 1
    assert migrate._leading_sql_keywords(sql, limit=4) == (
        "CREATE",
        "OR",
        "REPLACE",
        "FUNCTION",
    )
    assert "RETURNS trigger" in sql
    variant = _variant_capture_sql()
    start = "CREATE OR REPLACE FUNCTION"
    assert variant[variant.index(start) :] == sql[sql.index(start) :]

    # The interstitial names exactly 0200's body, 0301's extension of it and
    # the superseded R3.2 body, and restores 0200's body byte for byte.
    import hashlib
    import re

    def body(text, marker):
        start = text.index(marker)
        open_ = text.index("$$", start)
        return text[open_ + 2 : text.index("$$", open_ + 2)]

    marker = "FUNCTION public.validate_thread_agent_warm_binding_protection()"
    original = body(
        on_disk["0200_pinned_agent_recycle_authority.sql"].read_text(), marker
    )
    upstream = on_disk[UPSTREAM_WARM_MIGRATION].read_text()
    block = upstream[upstream.rindex("DO $migration$") :]
    old = re.search(r"old_fragment text := \$old\$(.*?)\$old\$;", block, re.S)[1]
    new = re.search(r"new_fragment text := \$new\$(.*?)\$new\$;", block, re.S)[1]
    extended = original.replace(old, new)
    assert original.count(old) == 1 and extended != original
    bridge = on_disk[BRIDGE_MIGRATION].read_text()
    md5 = [
        hashlib.md5(text.encode()).hexdigest()
        for text in (original, extended, body(R32_VALIDATOR_SQL, marker))
    ]
    assert f"IN ('{md5[0]}', '{md5[1]}')" in bridge
    assert f"IS DISTINCT FROM '{md5[2]}'" in bridge
    assert body(bridge, marker) == original


# ---------------------------------------------------------------------------
# H1 / H2: fresh and upstream
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upgrade_from_upstream_0305_adds_only_the_reconciled_files(
    owned_databases, tmp_path, monkeypatch
):
    history = await owned_databases()
    await _run_as_owner(history, _stage_upstream(tmp_path))
    store = await _open_store(history, monkeypatch)
    try:
        stack = _dedicated_stack(store, monkeypatch)
        claimed, claimless = await _soft_lives(stack)
        assert await self_end._outcome_proof(store, claimless) is None
        warm_ended = await _warm_soft_end(store, monkeypatch)
        in_flight = await _warm_permanent_delete(store, monkeypatch, writer="published")
        pre_fix = await _pre_fix_deleted_owner(store, monkeypatch)

        before = await _ledger(history)
        functions_before = await _reconciled_functions(history)
        data_before = await _application_data(history)
        catalog_before = await _catalog(history)
        await _run_as_owner(history, MIGRATIONS, dry_run=True)
        assert await _ledger(history) == before
        assert await _catalog(history) == catalog_before

        await _run_as_owner(history, MIGRATIONS)
        upgraded = await _ledger(history)
        names_before = {row["filename"] for row in before}
        assert [row for row in upgraded if row["filename"] in names_before] == before
        assert {row["filename"] for row in upgraded} - {
            row["filename"] for row in before
        } == RECONCILED_MIGRATIONS
        assert all(row["success"] for row in upgraded)
        # 0300z found 0301's extension and did nothing; 0306 installed the
        # claim-less capture in place.
        after = {row["proname"]: row for row in await _reconciled_functions(history)}
        before_by_name = {row["proname"]: row for row in functions_before}
        assert (
            after["validate_thread_agent_warm_binding_protection"]
            == (before_by_name["validate_thread_agent_warm_binding_protection"])
        )
        capture = after["capture_retired_pinned_agent_pod"]
        old_capture = before_by_name["capture_retired_pinned_agent_pod"]
        assert capture["source"] != old_capture["source"]
        assert {key: capture[key] for key in ("oid", "owner", "acl", "triggers")} == {
            key: old_capture[key] for key in ("oid", "owner", "acl", "triggers")
        }
        assert _project(data_before, await _application_data(history)) == _sorted(
            data_before
        )
        fresh = await _fresh_reference(owned_databases)
        catalog_after = await _catalog(history)
        assert catalog_after == await _catalog(fresh)
        await _run_as_owner(history, MIGRATIONS)
        assert await _ledger(history) == upgraded
        assert await _catalog(history) == catalog_after

        # Records settle under the published reconciler; history is untouched.
        (warm_ended_row,) = await _warm_rows(store, warm_ended["thread"])
        assert warm_ended_row["status"] == "released"
        await _reconcile_to_released(store, in_flight)
        await _reconcile_to_released(store, pre_fix)
        _use_dedicated(stack, monkeypatch)
        new_claimless = await self_end._bind_life(
            stack, await self_end._thread(store), with_claim=False
        )
        await self_end._agent_settles_soft_end(stack, new_claimless)
        assert (await self_end._outcome_proof(store, new_claimless))[
            "pod_uid"
        ] == new_claimless["pod_uid"]
        await _assert_warm_fences(history, store, monkeypatch)
    finally:
        await store.close()
        crypto.reset_cipher_cache()


# ---------------------------------------------------------------------------
# H3-H6: the local histories
# ---------------------------------------------------------------------------


async def _local_writer_records(store, stack, monkeypatch):
    """Records only the R3.2 migrations and code could write."""

    _use_dedicated(stack, monkeypatch)
    claimless = await self_end._bind_life(
        stack, await self_end._thread(store), with_claim=False
    )
    await self_end._agent_settles_soft_end(stack, claimless)
    assert (await self_end._outcome_proof(store, claimless))["pod_uid"] == claimless[
        "pod_uid"
    ]
    settled = await _warm_permanent_delete(store, monkeypatch, writer="r32")
    return NS(claimless=claimless, settled=settled)


async def _assert_local_upgrade(
    history, store, stack, monkeypatch, owned_databases, *, pre, local, pre_fix=None
):
    """The shared post-conditions of every local history's upgrade."""

    before = await _ledger(history)
    assert all(row["success"] for row in before)
    functions_before = await _reconciled_functions(history)
    assert [row["owner"] for row in functions_before] == [history.name] * 2
    catalog_before = await _catalog(history)
    data_before = await _application_data(history)

    # A dry run of the published chain is observational.
    await _run_as_owner(history, MIGRATIONS, dry_run=True)
    assert await _ledger(history) == before
    assert await _catalog(history) == catalog_before

    await _run_as_owner(history, MIGRATIONS)
    upgraded = await _ledger(history)
    names_before = {row["filename"] for row in before}
    canonical = {path.name for path in migrate.discover(MIGRATIONS)}
    # Every historical row is untouched; everything published that the
    # history lacks applied at its own position.
    assert [row for row in upgraded if row["filename"] in names_before] == before
    assert {row["filename"] for row in upgraded} == canonical | names_before
    assert all(row["success"] for row in upgraded)
    # Both reconciled functions were replaced in place.
    functions_after = await _reconciled_functions(history)
    identity = (
        "proname",
        "oid",
        "owner",
        "acl",
        "security_definer",
        "config",
        "triggers",
    )
    assert [{key: row[key] for key in identity} for row in functions_after] == [
        {key: row[key] for key in identity} for row in functions_before
    ]
    assert _project(data_before, await _application_data(history)) == _sorted(
        data_before
    )
    fresh = await _fresh_reference(owned_databases)
    catalog_after = await _catalog(history)
    assert catalog_after == await _catalog(fresh)
    assert [row["source"] for row in functions_after] == [
        row["source"] for row in await _reconciled_functions(fresh)
    ]
    # A second startup changes nothing.
    await _run_as_owner(history, MIGRATIONS)
    assert await _ledger(history) == upgraded
    assert await _catalog(history) == catalog_after

    # Existing warm records settle exactly under the published reconciler.
    (settled,) = await _warm_rows(store, local.settled.life["thread"])
    assert (settled["status"], settled["release_outcome"]) == (
        "released",
        "exact_absent_v1",
    )
    if pre_fix is not None:
        await _reconcile_to_released(store, pre_fix)

    # Retirement after the upgrade: a new claim-less life records its Pod;
    # permanent Delete retires exactly the Pods the records prove (the
    # pre-capture claim-less life has none, so its Pod keeps its protection).
    _use_dedicated(stack, monkeypatch)
    fresh_claimless = await self_end._bind_life(
        stack, await self_end._thread(store), with_claim=False
    )
    await self_end._agent_settles_soft_end(stack, fresh_claimless)
    assert (await self_end._outcome_proof(store, fresh_claimless))[
        "pod_uid"
    ] == fresh_claimless["pod_uid"]
    pre_claimed, pre_claimless = pre
    lives = (pre_claimed, pre_claimless, local.claimless, fresh_claimless)
    # A claim-less life settled before the capture existed has no proof (H3,
    # H4); on a history that installed the capture first it has one (H6).
    proven = [
        life for life in lives if await self_end._outcome_proof(store, life) is not None
    ]
    assert [life for life in lives if life not in proven] in ([], [pre_claimless])
    for life in lives:
        stack.k8s.exit_and_reap(life)
        outcomes = await self_end._delete_until_settled(stack, life["thread"])
        assert outcomes[-1] == "deleted", outcomes
        assert await store.get_thread(life["thread"]) is None
    assert sorted(stack.k8s.removed_pods) == sorted(life["pod_uid"] for life in proven)
    if pre_claimless not in proven:
        kept = self_end._pod(stack, pre_claimless)
        assert kept is not None
        assert kept.metadata.finalizers == [PINNED_AUTHORITY_FINALIZER]
    await _assert_warm_fences(history, store, monkeypatch)
    return upgraded


@pytest.mark.asyncio
async def test_upgrade_from_the_original_local_0286_0287_history(
    owned_databases, tmp_path, monkeypatch
):
    """H3: the pair applied as 0286/0287 before upstream's 0286-0300."""

    history = await owned_databases()
    await _run_as_owner(
        history, _stage(tmp_path, "through-0285", through=DEPLOYED_HISTORY_THROUGH)
    )
    store = await _open_store(history, monkeypatch)
    try:
        stack = _dedicated_stack(store, monkeypatch)
        pre = await _soft_lives(stack)
        assert await self_end._outcome_proof(store, pre[1]) is None
        await _warm_soft_end(store, monkeypatch)

        await _run_as_owner(history, _stage_original_local(tmp_path))
        deployed = await _ledger(history)
        assert len(deployed) == DEPLOYED_LEDGER_ROWS
        assert {
            row["filename"]: row["checksum"]
            for row in deployed
            if row["filename"] in {CAPTURE_0286, VALIDATOR_0287}
        } == {CAPTURE_0286: CAPTURE_CHECKSUM, VALIDATOR_0287: R32_VALIDATOR_CHECKSUM}
        local = await _local_writer_records(store, stack, monkeypatch)

        upgraded = await _assert_local_upgrade(
            history, store, stack, monkeypatch, owned_databases, pre=pre, local=local
        )
        assert {CAPTURE_0286, VALIDATOR_0287} <= {row["filename"] for row in upgraded}
    finally:
        await store.close()
        crypto.reset_cipher_cache()


async def _build_repaired_k3d_history(history, tmp_path, store, stack, monkeypatch):
    """H4 as it happened: H3, the manual repair, then 50d0af34a's chain."""

    await _run_as_owner(
        history, _stage(tmp_path, "through-0285", through=DEPLOYED_HISTORY_THROUGH)
    )
    pre = await _soft_lives(stack)
    await _warm_soft_end(store, monkeypatch)
    pre_fix = await _pre_fix_deleted_owner(store, monkeypatch)
    await _run_as_owner(history, _stage_original_local(tmp_path))
    early = await _local_writer_records(store, stack, monkeypatch)
    deleted = await _fetch(
        history.admin_dsn,
        "DELETE FROM public.schema_migrations "
        "WHERE filename = ANY($1::text[]) RETURNING filename, checksum",
        [CAPTURE_0286, VALIDATOR_0287],
    )
    assert {row["filename"]: row["checksum"] for row in deleted} == {
        CAPTURE_0286: CAPTURE_CHECKSUM,
        VALIDATOR_0287: R32_VALIDATOR_CHECKSUM,
    }
    await _run_as_owner(
        history,
        _stage_local_candidate(
            tmp_path, "develop-50d0af34a", capture_sql=_variant_capture_sql()
        ),
    )
    repaired = await _ledger(history)
    assert len(repaired) == REPAIRED_LEDGER_ROWS
    assert max(row["filename"] for row in repaired) == VALIDATOR_0302
    assert {
        row["filename"]: row["checksum"]
        for row in repaired
        if row["filename"] in {CAPTURE_0301, VALIDATOR_0302}
    } == {
        CAPTURE_0301: VARIANT_CAPTURE_CHECKSUM,
        VALIDATOR_0302: R32_VALIDATOR_CHECKSUM,
    }
    late = await _local_writer_records(store, stack, monkeypatch)
    return pre, pre_fix, early, late


@pytest.mark.asyncio
@pytest.mark.parametrize("restored_rows", [False, True], ids=["H4", "H5"])
async def test_upgrade_from_the_repaired_k3d_history(
    owned_databases, tmp_path, monkeypatch, restored_rows
):
    """H4: k3d-srw today (296 rows); H5: with the two deleted rows restored."""

    history = await owned_databases()
    store = await _open_store(history, monkeypatch)
    try:
        stack = _dedicated_stack(store, monkeypatch)
        pre, pre_fix, early, late = await _build_repaired_k3d_history(
            history, tmp_path, store, stack, monkeypatch
        )
        if restored_rows:
            for filename, checksum, execution_ms in DELETED_HISTORICAL_ROWS:
                await _execute(
                    history.admin_dsn,
                    "INSERT INTO public.schema_migrations"
                    "(filename, checksum, applied_at, applied_by, execution_ms) "
                    "VALUES ($1, $2, $3::text::timestamptz, $4, $5)",
                    filename,
                    checksum,
                    DELETED_ROWS_APPLIED_AT,
                    history.name,
                    execution_ms,
                )
        upgraded = await _assert_local_upgrade(
            history,
            store,
            stack,
            monkeypatch,
            owned_databases,
            pre=pre,
            local=late,
            pre_fix=pre_fix,
        )
        historical = {CAPTURE_0301, VALIDATOR_0302}
        if restored_rows:
            historical |= {CAPTURE_0286, VALIDATOR_0287}
        assert historical <= {row["filename"] for row in upgraded}
        (early_row,) = await _warm_rows(store, early.settled.life["thread"])
        assert early_row["status"] == "released"
    finally:
        await store.close()
        crypto.reset_cipher_cache()


@pytest.mark.asyncio
async def test_upgrade_from_a_fresh_install_of_the_previous_candidate(
    owned_databases, tmp_path, monkeypatch
):
    """H6: local develop 9270a8aa1 (0301/0302, original bytes) installed fresh."""

    history = await owned_databases()
    await _run_as_owner(
        history,
        _stage_local_candidate(
            tmp_path,
            "develop-9270a8aa1",
            capture_sql=(MIGRATIONS / CAPTURE_MIGRATION).read_text(),
        ),
    )
    assert {
        row["filename"]: row["checksum"]
        for row in await _ledger(history)
        if row["filename"] in {CAPTURE_0301, VALIDATOR_0302}
    } == {CAPTURE_0301: CAPTURE_CHECKSUM, VALIDATOR_0302: R32_VALIDATOR_CHECKSUM}
    store = await _open_store(history, monkeypatch)
    try:
        stack = _dedicated_stack(store, monkeypatch)
        pre = await _soft_lives(stack)
        # This chain installed the claim-less capture from the start.
        assert (await self_end._outcome_proof(store, pre[1]))["pod_uid"] == pre[1][
            "pod_uid"
        ]
        await _warm_soft_end(store, monkeypatch)
        local = await _local_writer_records(store, stack, monkeypatch)
        await _assert_local_upgrade(
            history, store, stack, monkeypatch, owned_databases, pre=pre, local=local
        )
    finally:
        await store.close()
        crypto.reset_cipher_cache()


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


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
        "superseded_other_checksum",
        "missing_bridge",
        "changed_bridge",
        "changed_upstream_warm_release",
    ],
)
async def test_original_local_history_refuses_anything_but_the_reviewed_rows(
    owned_databases, tmp_path, mutation
):
    history = _stage_original_local(tmp_path)
    published = tmp_path / "published"
    shutil.copytree(MIGRATIONS, published)
    database = await owned_databases()
    if mutation == "unregistered_name":
        renamed = "0286_capture_claimless_retired_agent_pod_v2.sql"
        (history / CAPTURE_0286).rename(history / renamed)
        expected = rf"applied but missing on disk: \['{renamed}'\]"
    elif mutation == "other_historical_checksum":
        path = history / CAPTURE_0286
        path.write_text(path.read_text() + "\n-- unreviewed historical edit\n")
        expected = rf"checksum changed: {CAPTURE_0286}"
    elif mutation == "failed_historical_row":
        expected = rf"dirty migration '{CAPTURE_0286}'"
    elif mutation == "changed_canonical_file":
        path = published / CAPTURE_MIGRATION
        path.write_text(path.read_text() + "\n-- unreviewed published edit\n")
        expected = rf"renamed migration {CAPTURE_0286} requires {CAPTURE_MIGRATION}"
    elif mutation == "missing_canonical_file":
        (published / CAPTURE_MIGRATION).unlink()
        expected = rf"renamed migration {CAPTURE_0286} requires {CAPTURE_MIGRATION}"
    elif mutation == "superseded_other_checksum":
        path = history / VALIDATOR_0287
        path.write_text(path.read_text() + "\n-- unreviewed historical edit\n")
        expected = rf"checksum changed: {VALIDATOR_0287}"
    elif mutation == "missing_bridge":
        (published / BRIDGE_MIGRATION).unlink()
        expected = rf"superseded migration {VALIDATOR_0287} requires {BRIDGE_MIGRATION}"
    elif mutation == "changed_bridge":
        path = published / BRIDGE_MIGRATION
        path.write_text(path.read_text() + "\n-- unreviewed published edit\n")
        expected = rf"superseded migration {VALIDATOR_0287} requires {BRIDGE_MIGRATION}"
    elif mutation == "changed_upstream_warm_release":
        path = published / UPSTREAM_WARM_MIGRATION
        path.write_text(path.read_text() + "\n-- unreviewed published edit\n")
        expected = (
            rf"superseded migration {VALIDATOR_0287} requires {UPSTREAM_WARM_MIGRATION}"
        )
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
        await _set_function_owner(database, RECONCILED_FUNCTIONS[0])
        with pytest.raises(asyncpg.InsufficientPrivilegeError, match="must be owner"):
            await _run_as_owner(database, history)
        await _set_function_owner(database, RECONCILED_FUNCTIONS[0], database.name)
    else:
        await _run_as_owner(database, history)
    ledger = await _ledger(database)
    catalog = await _catalog(database)
    if mutation == "failed_historical_row":
        failed = next(row for row in ledger if row["filename"] == CAPTURE_0286)
        assert failed["success"] is False
        assert failed["checksum"] == CAPTURE_CHECKSUM

    for dry_run in (False, True):
        with pytest.raises(RuntimeError, match=expected):
            await _run_as_owner(database, published, dry_run=dry_run)
        assert await _ledger(database) == ledger
        assert await _catalog(database) == catalog


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["other_variant", "superseded_other_checksum"])
async def test_repaired_history_refuses_other_bytes(
    owned_databases, tmp_path, mutation
):
    history = _stage_local_candidate(
        tmp_path, "develop-50d0af34a", capture_sql=_variant_capture_sql()
    )
    if mutation == "other_variant":
        path = history / CAPTURE_0301
        path.write_text(path.read_text() + "\n-- unreviewed historical edit\n")
        expected = f"checksum changed: {CAPTURE_0301}"
    else:
        path = history / VALIDATOR_0302
        path.write_text(path.read_text() + "\n-- unreviewed historical edit\n")
        expected = f"checksum changed: {VALIDATOR_0302}"
    database = await owned_databases()
    await _run_as_owner(database, history)
    ledger = await _ledger(database)
    catalog = await _catalog(database)
    for dry_run in (False, True):
        with pytest.raises(RuntimeError, match=expected):
            await _run_as_owner(database, MIGRATIONS, dry_run=dry_run)
        assert await _ledger(database) == ledger
        assert await _catalog(database) == catalog


@pytest.mark.asyncio
async def test_upstream_0301_alone_refuses_the_r32_validator(owned_databases, tmp_path):
    """Why 0300z exists: 0301's text replacement needs 0200's body."""

    database = await owned_databases()
    await _run_as_owner(
        database,
        _stage_local_candidate(
            tmp_path, "develop-50d0af34a", capture_sql=_variant_capture_sql()
        ),
    )
    catalog = await _catalog(database)
    sql = migrate._runner_owned_transaction_sql(
        (MIGRATIONS / UPSTREAM_WARM_MIGRATION).read_text()
    )
    conn = await asyncpg.connect(database.owner_dsn)
    try:
        with pytest.raises(
            asyncpg.RaiseError, match="warm release reciprocity branch drifted"
        ):
            async with conn.transaction():
                await conn.execute(sql)
    finally:
        await conn.close()
    assert await _catalog(database) == catalog


@pytest.mark.asyncio
async def test_bridge_refuses_an_unknown_validator_body(owned_databases, tmp_path):
    """0300z restores only the reviewed R3.2 body; anything else stays dirty."""

    database = await owned_databases()
    await _run_as_owner(
        database,
        _stage_local_candidate(
            tmp_path, "develop-50d0af34a", capture_sql=_variant_capture_sql()
        ),
    )
    # An out-of-band edit of the installed validator.
    source = (
        await _fetch(
            database.admin_dsn,
            "SELECT prosrc FROM pg_proc WHERE proname="
            "'validate_thread_agent_warm_binding_protection'",
        )
    )[0]["prosrc"]
    await _execute(
        database.owner_dsn,
        "CREATE OR REPLACE FUNCTION "
        "public.validate_thread_agent_warm_binding_protection() RETURNS trigger "
        "LANGUAGE plpgsql AS $body$" + source + "-- out-of-band\n$body$",
    )
    ledger = await _ledger(database)
    catalog = await _catalog(database)
    with pytest.raises(asyncpg.RaiseError, match="warm binding validator drifted"):
        await _run_as_owner(database, MIGRATIONS)
    dirty = [row for row in await _ledger(database) if not row["success"]]
    assert [row["filename"] for row in dirty] == [BRIDGE_MIGRATION]
    assert [row for row in await _ledger(database) if row["success"]] == ledger
    assert await _catalog(database) == catalog
    with pytest.raises(RuntimeError, match=f"dirty migration '{BRIDGE_MIGRATION}'"):
        await _run_as_owner(database, MIGRATIONS)
