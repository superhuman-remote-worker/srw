"""Real-PostgreSQL proofs for the git swap driver's per-delivery decision
(C3 review B1): the launch outcome and capacity queries, and where a lease
owner's workspace runs, against ``schema_current.sql``.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services import connector_credential_leases as leases
from orchestrator.services import connector_git_swap_delivery as swaps
from orchestrator.services.connector_driver_identities import (
    mint_driver_identity,
    revoke_driver_identity,
)
from shared.connectors.builtin import GIT_SWAP_SPEC

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
DIGEST = "sha256:" + "ab" * 32


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=4)
    await store.connect()
    swaps.configure_git_swap_delivery(
        swaps.GitSwapDeliverySettings(installed=True, max_installation=50)
    )
    try:
        yield store
    finally:
        swaps.configure_git_swap_delivery(swaps.GitSwapDeliverySettings())
        async with store.acquire() as conn:
            await conn.execute("DELETE FROM connector_driver_identities")
        await store.close()


async def _connector(db) -> str:
    connector_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO datasources (id, name, type, scope_mode, policy_revision, "
            "connection_url, config, updated_at) VALUES ($1, $2, 'repository', "
            "'all', 1, 'https://github.com/o/r.git', '{\"forge\": \"github\"}'::jsonb, "
            "now() - interval '1 hour')",
            connector_id,
            f"repo-{str(connector_id)[:8]}",
        )
    return str(connector_id)


async def _pod(
    db, connector: str, *, stop: str | None = None, error: str = "", generation="g1"
) -> str:
    async with db.acquire() as conn:
        minted = await mint_driver_identity(
            conn,
            connector_id=connector,
            driver=GIT_SWAP_SPEC.name,
            image_digest=DIGEST,
            pod_namespace="srw-connectors",
        )
        await conn.execute(
            "UPDATE connector_driver_identities SET pod_name = 'srw-drv-' || "
            "replace(id::text, '-', ''), credential_generation = $3, "
            "image_reference = 'ghcr.io/x/y:1', launch_error = NULLIF($2, '') "
            "WHERE id = $1",
            UUID(minted.id),
            error,
            generation,
        )
        if stop is not None:
            await revoke_driver_identity(conn, identity_id=minted.id, reason=stop)
    return minted.id


@pytest.mark.asyncio
async def test_the_last_pod_decides_within_the_back_off(db):
    connector = await _connector(db)
    async with db.acquire() as conn:
        assert await swaps.launch_problem(conn, connector) is None
    await _pod(
        db,
        connector,
        stop="upstream_unreachable",
        error="untrusted certificate: unknown authority",
    )
    async with db.acquire() as conn:
        problem = await swaps.launch_problem(conn, connector)
    # A fixed reason; the driver's words stay in the detail (the log).
    assert problem.reason == "untrusted_certificate"
    assert problem.text == swaps.REASONS["untrusted_certificate"]
    assert "unknown authority" in problem.detail
    # An idle stop is no failure.
    other = await _connector(db)
    await _pod(db, other, stop="idle")
    async with db.acquire() as conn:
        assert await swaps.launch_problem(conn, other) is None
        # Past the back-off, it is tried again.
        await conn.execute(
            "UPDATE connector_driver_identities SET revoked_at = now() - interval '1 hour' "
            "WHERE connector_id = $1",
            UUID(connector),
        )
        assert await swaps.launch_problem(conn, connector) is None
        # So is one whose connector changed since (a new upstream CA).
        await conn.execute(
            "UPDATE connector_driver_identities SET revoked_at = now() - interval '1 minute' "
            "WHERE connector_id = $1",
            UUID(connector),
        )
        assert await swaps.launch_problem(conn, connector) is not None
        await conn.execute(
            "UPDATE datasources SET updated_at = now() WHERE id = $1", UUID(connector)
        )
        assert await swaps.launch_problem(conn, connector) is None


@pytest.mark.asyncio
async def test_a_live_pod_serves_and_a_full_installation_refuses(db):
    swaps.configure_git_swap_delivery(
        swaps.GitSwapDeliverySettings(installed=True, max_installation=1)
    )
    serving = await _connector(db)
    async with db.acquire() as conn:
        generation = await swaps.current_generation(conn, serving)
    live = await _pod(db, serving, generation=generation)
    await _set(db, live, "ready_at = now()")
    # A newer failed pod does not hide the live one; its objects are gone.
    failed = await _pod(db, serving, stop="start_timeout", generation="g2")
    await _set(db, failed, "removed_at = now()")
    async with db.acquire() as conn:
        assert await swaps.launch_problem(conn, serving) is None
        waiting = await _connector(db)
        problem = await swaps.launch_problem(conn, waiting)
        # The live pod is busy; the removed one holds no slot.
        assert problem.reason == "no_room"
        # Idle pods make room: the reconciler evicts the longest-idle one.
        await conn.execute(
            "UPDATE connector_driver_identities SET idle_since = now() "
            "WHERE connector_id = $1",
            UUID(serving),
        )
        assert await swaps.launch_problem(conn, waiting) is None


@pytest.mark.asyncio
async def test_the_owners_workspace_is_read_from_its_row(db):
    job_id, thread_id = uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, context, config_override) "
            "VALUES ($1, 'swap', 'processing', $2::jsonb, $3::jsonb)",
            job_id,
            json.dumps({"workspace_container": {"provisioner": "docker"}}),
            json.dumps({"workspace": {"backend": "sandbox"}}),
        )
        await conn.execute(
            "INSERT INTO threads (id, status, metadata) VALUES ($1, 'active', $2::jsonb)",
            thread_id,
            json.dumps(
                {
                    "config_override": {"workspace": {"backend": "sandbox"}},
                    "workspace_container": {"provisioner": "k8s"},
                }
            ),
        )
        problem = await swaps.owner_workspace_problem(
            conn, leases.LeaseOwner.job(str(job_id))
        )
        assert problem.reason == "workspace_static_pool"
        assert (
            await swaps.owner_workspace_problem(
                conn, leases.LeaseOwner.thread(str(thread_id))
            )
            is None
        )


# =============================================================================
# C3 re-review 2
# =============================================================================


async def _thread(db) -> str:
    thread_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, metadata) VALUES ($1, 'active', '{}'::jsonb)",
            thread_id,
        )
    return str(thread_id)


async def _bind(db, connector: str) -> None:
    async with db.acquire() as conn:
        await leases.issue_or_redeliver(
            conn,
            owner=leases.LeaseOwner.thread(await _thread(db)),
            connector_id=connector,
            driver=GIT_SWAP_SPEC.name,
            access="ReadWrite",
            image_digest=DIGEST,
        )


async def _set(db, identity: str, assignments: str) -> None:
    async with db.acquire() as conn:
        await conn.execute(
            f"UPDATE connector_driver_identities SET {assignments} WHERE id = $1",
            UUID(identity),
        )


@pytest.mark.asyncio
async def test_only_a_ready_pod_of_the_current_generation_serves(db):
    """The probe of C3 re-review 2: an old-CA pod that lives its hour must
    not hide a new generation that exits 78 (clones would time out "NOT
    cloned" instead of falling back)."""
    connector = await _connector(db)
    async with db.acquire() as conn:
        generation = await swaps.current_generation(conn, connector)
    assert generation and generation.startswith("hmac-sha256:")
    old = await _pod(db, connector, generation="g1")  # serves the old CA
    await _set(db, old, "ready_at = now() - interval '2 hours'")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE datasources SET updated_at = now() - interval '1 minute' "
            "WHERE id = $1",
            UUID(connector),
        )
    await _pod(
        db,
        connector,
        stop="upstream_unreachable",
        error="untrusted certificate: unknown authority",
        generation=generation,
    )
    async with db.acquire() as conn:
        assert await swaps._serving(conn, connector) is False
        problem = await swaps.launch_problem(conn, connector)
    assert problem.reason == "untrusted_certificate"
    # The current generation's pod, starting: still no proof.
    current = await _pod(db, connector, generation=generation)
    async with db.acquire() as conn:
        assert await swaps._serving(conn, connector) is False
    await _set(db, current, "ready_at = now()")
    async with db.acquire() as conn:
        assert await swaps._serving(conn, connector) is True
        assert await swaps.launch_problem(conn, connector) is None


@pytest.mark.asyncio
async def test_the_busy_count_is_what_the_reconciler_would_not_stop(db):
    """A connector's own superseded pod gives way to its successor; an idle
    pod no binding uses makes room; another connector's superseded pod,
    draining its bindings, and a bound pod do not."""
    own, idle, draining, busy = [await _connector(db) for _ in range(4)]
    own_old = await _pod(db, own, generation="old")
    await _set(db, own_old, "idle_since = now(), ready_at = now()")
    await _bind(db, own)
    idle_pod = await _pod(db, idle)
    await _set(db, idle_pod, "idle_since = now()")
    draining_pod = await _pod(db, draining, generation="old")
    await _set(db, draining_pod, "idle_since = now()")
    await _bind(db, draining)
    await _pod(db, busy)
    stranger = await _connector(db)
    async with db.acquire() as conn:
        generation = await swaps.current_generation(conn, own)
        for_own = await conn.fetchval(swaps._BUSY_PODS, UUID(own), generation)
        for_another = await conn.fetchval(
            swaps._BUSY_PODS,
            UUID(stranger),
            await swaps.current_generation(conn, stranger),
        )
    assert for_own == 2  # draining and busy
    assert for_another == 3  # and own's old pod, which drains its binding
    # A stopped pod still terminating holds its slot: another key's start
    # may be waiting for it (the reconciler review's fix 2), even one that
    # was idle without a binding; it is free once its objects are gone.
    await _set(db, idle_pod, "revoked_at = now(), revoke_reason = 'idle_evicted'")
    async with db.acquire() as conn:
        assert await conn.fetchval(swaps._BUSY_PODS, UUID(stranger), "x") == 4
    await _set(db, idle_pod, "removed_at = now()")
    async with db.acquire() as conn:
        assert await conn.fetchval(swaps._BUSY_PODS, UUID(stranger), "x") == 3


@pytest.mark.asyncio
async def test_a_threads_selection_is_read_by_uuid(db, monkeypatch):
    connector = await _connector(db)
    other = await _connector(db)
    thread_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, metadata) VALUES ($1, 'active', $2::jsonb)",
            thread_id,
            json.dumps({"datasource_ids": [connector.upper(), "not-a-uuid", "", 42]}),
        )
    prepared: list = []

    async def prepare(db_, entries, *, owner):
        prepared.append([entry["datasource_id"] for entry in entries])

    monkeypatch.setattr(leases, "prepare_lease_delivery", prepare)
    await leases.prepare_thread_lease_delivery(db, str(thread_id))
    # A malformed id selects nothing (never a failed cast); the uuid
    # comparison finds an id written in capitals; other is not selected.
    assert prepared == [[connector]] and other not in prepared[0]


@pytest.mark.asyncio
async def test_the_reconcilers_listen_comes_back_after_its_backend_ends(
    pg_dsn, _schema_applied, monkeypatch
):
    """The probe of C3 re-review 2: after pg_terminate_backend (a restart, a
    failover) the LISTEN is opened again and a NOTIFY wakes the loop; the
    lost connection went back to the pool."""
    import asyncio
    from contextlib import asynccontextmanager

    from orchestrator.services import connector_service_hosting as hosting

    monkeypatch.setattr(hosting, "LISTEN_RETRY_SECONDS", 0.2)
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)

    class Store:
        @asynccontextmanager
        async def acquire(self):
            async with pool.acquire() as conn:
                yield conn

    async def notify_until_heard(wake) -> bool:
        for _ in range(60):
            async with pool.acquire() as conn:
                await conn.execute(
                    "SELECT pg_notify($1, '')", hosting.RECONCILE_CHANNEL
                )
            try:
                await asyncio.wait_for(wake.wait(), 0.25)
                return True
            except asyncio.TimeoutError:
                continue
        return False

    wake, shutdown = asyncio.Event(), asyncio.Event()
    task = asyncio.create_task(hosting._listen(Store(), wake, shutdown))
    try:
        assert await notify_until_heard(wake)
        wake.clear()
        async with pool.acquire() as conn:
            killed = await conn.fetchval(
                "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                "WHERE query ILIKE 'LISTEN%' AND pid <> pg_backend_pid()"
            )
        assert killed == 1
        # Heard again within seconds: a new connection LISTENs (with a pool
        # of two, the dead one must have gone back for this to work).
        assert await notify_until_heard(wake)
        assert not task.done()
    finally:
        shutdown.set()
        await asyncio.wait_for(task, 10)
        await pool.close()
