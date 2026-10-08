"""Real-PostgreSQL lock-order proofs for credential leases (slice C2).

Every transaction that touches a lease takes its rows in one order: the
catalog, the threads (or Job) that name a connector, the connector's row,
then lease rows in connector-id order. A connector delete and a lease issue
therefore serialize instead of deadlocking, and the sweeper, which spans many
executions, skips the lease rows a delivering transaction holds.

The server runs with a short ``deadlock_timeout`` so a regression shows up as
``DeadlockDetectedError`` in well under a second rather than as a slow test.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.security.crypto import encrypt
from orchestrator.services import connector_credential_leases as leases

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
DRIVER = "srw.lease-probe/v1"
#: Every wait below resolves in well under this; a hang is a regression.
WAIT_SECONDS = 15


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.with_command(
            "postgres -c fsync=off -c synchronous_commit=off "
            "-c full_page_writes=off -c deadlock_timeout=200ms"
        )
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
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=8)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE connector_credential_leases, connector_driver_identities, "
            "security_events, project_datasources, datasources, jobs, threads, "
            "projects CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()


async def _connector(db, *, project: str | None = None) -> str:
    connector_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO datasources (id, name, type, scope_mode, policy_revision, "
            "credentials, config) VALUES ($1, $2, 'lease_probe', $3, 1, "
            "$4::jsonb, '{}'::jsonb)",
            connector_id,
            f"probe-{str(connector_id)[:8]}",
            "projects" if project else "all",
            json.dumps(encrypt(json.dumps({"secret": "s"}))),
        )
        if project:
            await conn.execute(
                "INSERT INTO project_datasources (project_id, datasource_id) "
                "VALUES ($1, $2)",
                UUID(project),
                connector_id,
            )
    return str(connector_id)


async def _project(db) -> str:
    project_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, 'lock order')", project_id
        )
    return str(project_id)


async def _thread(db, connector_ids) -> str:
    thread_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, metadata) VALUES ($1, 'active', "
            "$2::jsonb)",
            thread_id,
            json.dumps({"datasource_ids": list(connector_ids)}),
        )
    return str(thread_id)


async def _job(db) -> str:
    job_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) VALUES ($1, 'lock order', "
            "'processing')",
            job_id,
        )
    return str(job_id)


async def _issue_on(conn, owner, connector):
    return await leases.issue_or_redeliver(
        conn, owner=owner, connector_id=connector, driver=DRIVER, access="ReadWrite"
    )


async def _issue(db, owner, connector):
    async with db.acquire() as conn:
        return await _issue_on(conn, owner, connector)


async def _live_leases(db) -> int:
    async with db.acquire() as conn:
        return await conn.fetchval(
            "SELECT count(*) FROM connector_credential_leases WHERE revoked_at IS NULL"
        )


class _RowHolder:
    """A transaction holding one lease row, released on demand."""

    def __init__(self, db) -> None:
        self.db = db
        self.conn = None
        self.transaction = None

    async def hold(self, lease_id: str) -> None:
        self.conn = await self.db._pool.acquire()
        self.transaction = self.conn.transaction()
        await self.transaction.start()
        await self.conn.execute(
            "SELECT 1 FROM connector_credential_leases WHERE id = $1 FOR UPDATE",
            UUID(lease_id),
        )

    async def release(self) -> None:
        if self.transaction is not None:
            await self.transaction.rollback()
            await self.db._pool.release(self.conn)
            self.transaction = None


def _deadlocks(results) -> list:
    return [r for r in results if isinstance(r, asyncpg.DeadlockDetectedError)]


# =============================================================================
# Connector delete against an issue
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize("scoped", [False, True], ids=["unscoped", "scoped"])
async def test_a_connector_delete_and_a_session_issue_serialize(db, scoped):
    """Warm/pinned attach shape: the issue in its own transaction.

    The delete is stopped (by another transaction holding one of the
    connector's lease rows) after it took its locks; an issue for a thread
    naming the connector then waits for the delete instead of taking the
    thread and waiting on the connector behind it."""
    project = await _project(db) if scoped else None
    connector = await _connector(db, project=project)
    thread = await _thread(db, [connector])
    other = await _issue(db, leases.LeaseOwner.job(await _job(db)), connector)
    holder = _RowHolder(db)
    await holder.hold(other.id)
    try:
        deleting = asyncio.create_task(
            db.delete_datasource(connector, authority_project_scope_id=project)
        )
        await asyncio.sleep(0.3)
        issuing = asyncio.create_task(
            _issue(db, leases.LeaseOwner.thread(thread), connector)
        )
        await asyncio.sleep(0.5)
        assert not deleting.done() and not issuing.done()
    finally:
        await holder.release()
    results = await asyncio.wait_for(
        asyncio.gather(deleting, issuing, return_exceptions=True), WAIT_SECONDS
    )

    assert _deadlocks(results) == [], results
    assert results[0] is True
    # The issue ran after the delete: the connector is gone.
    assert isinstance(results[1], leases.LeaseDeliveryError), results[1]
    assert await _live_leases(db) == 0


@pytest.mark.asyncio
async def test_a_connector_delete_and_a_stateless_claim_serialize(db):
    """Stateless claim shape: the claim transaction already holds the thread
    FOR UPDATE and writes it before it issues."""
    connector = await _connector(db)
    thread = await _thread(db, [connector])
    other = await _issue(db, leases.LeaseOwner.job(await _job(db)), connector)
    holder = _RowHolder(db)
    await holder.hold(other.id)

    async def claim():
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT 1 FROM threads WHERE id = $1 FOR UPDATE", UUID(thread)
                )
                await conn.execute(
                    "UPDATE threads SET metadata = metadata || '{\"stamp\": 1}' "
                    "WHERE id = $1",
                    UUID(thread),
                )
                await _issue_on(conn, leases.LeaseOwner.thread(thread), connector)

    try:
        deleting = asyncio.create_task(db.delete_datasource(connector))
        await asyncio.sleep(0.3)
        claiming = asyncio.create_task(claim())
        await asyncio.sleep(0.5)
    finally:
        await holder.release()
    results = await asyncio.wait_for(
        asyncio.gather(deleting, claiming, return_exceptions=True), WAIT_SECONDS
    )

    assert _deadlocks(results) == [], results
    assert results[0] is True
    assert isinstance(results[1], leases.LeaseDeliveryError), results[1]
    assert await _live_leases(db) == 0


@pytest.mark.asyncio
async def test_a_claim_holding_the_thread_first_wins_and_the_delete_revokes(db):
    """The other order: the claim holds the thread, then the delete arrives.
    The delete waits on the thread, the claim issues and commits, and the
    delete then revokes the new lease with the connector."""
    connector = await _connector(db)
    thread = await _thread(db, [connector])
    claim_holds_thread = asyncio.Event()
    go_issue = asyncio.Event()

    async def claim():
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT 1 FROM threads WHERE id = $1 FOR UPDATE", UUID(thread)
                )
                claim_holds_thread.set()
                await go_issue.wait()
                return await _issue_on(
                    conn, leases.LeaseOwner.thread(thread), connector
                )

    claiming = asyncio.create_task(claim())
    await claim_holds_thread.wait()
    deleting = asyncio.create_task(db.delete_datasource(connector))
    await asyncio.sleep(0.5)
    assert not deleting.done()
    go_issue.set()
    results = await asyncio.wait_for(
        asyncio.gather(claiming, deleting, return_exceptions=True), WAIT_SECONDS
    )

    assert _deadlocks(results) == [], results
    assert isinstance(results[0], leases.DeliveredLease) and results[0].issued
    assert results[1] is True
    assert await _live_leases(db) == 0
    async with db.acquire() as conn:
        reasons = await conn.fetch(
            "SELECT detail FROM security_events "
            "WHERE event_type = 'connector_lease_revoked'"
        )
    assert ["reason=connector_deleted" in row["detail"] for row in reasons] == [True]


# =============================================================================
# The sweeper against a multi-connector claim
# =============================================================================


async def _two_connector_session(db):
    first = await _connector(db)
    second = await _connector(db)
    a, b = sorted([first, second])
    thread = await _thread(db, [a, b])
    owner = leases.LeaseOwner.thread(thread)
    # B is written first so a scan meets it before A.
    await _issue(db, owner, b)
    await _issue(db, owner, a)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE connector_credential_leases "
            "SET expires_at = now() + interval '100 seconds'"
        )
    return owner, a, b


@pytest.mark.asyncio
async def test_the_renewal_skips_a_lease_a_claim_holds(db):
    """A claim delivering A then B holds A's row while the sweep runs. The
    sweep renews B and skips A instead of locking B and waiting on A (which
    the claim, wanting B next, would turn into a deadlock)."""
    owner, a, b = await _two_connector_session(db)
    claim_holds_a = asyncio.Event()
    go_b = asyncio.Event()

    async def claim():
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT 1 FROM threads WHERE id = $1 FOR UPDATE", UUID(owner.id)
                )
                await _issue_on(conn, owner, a)
                claim_holds_a.set()
                await go_b.wait()
                await _issue_on(conn, owner, b)

    claiming = asyncio.create_task(claim())
    await claim_holds_a.wait()
    try:
        async with db.acquire() as conn:
            renewed = await asyncio.wait_for(
                leases.renew_live_leases(conn, ttl_seconds=900), WAIT_SECONDS
            )
    finally:
        go_b.set()
    await asyncio.wait_for(claiming, WAIT_SECONDS)

    assert renewed == 1
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT connector_id::text AS connector, "
            "expires_at > now() + interval '200 seconds' AS renewed "
            "FROM connector_credential_leases WHERE revoked_at IS NULL"
        )
    assert {row["connector"]: row["renewed"] for row in rows} == {a: False, b: True}
    # The next pass renews what this one skipped.
    async with db.acquire() as conn:
        assert await leases.renew_live_leases(conn, ttl_seconds=900) == 1


async def _lease_of(db, connector: str) -> str:
    async with db.acquire() as conn:
        return await conn.fetchval(
            "SELECT id::text FROM connector_credential_leases WHERE connector_id = $1",
            UUID(connector),
        )


@pytest.mark.asyncio
async def test_the_terminal_backstop_skips_a_held_row(db):
    owner, a, b = await _two_connector_session(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET status = 'ended' WHERE id = $1", UUID(owner.id)
        )
    held, other = await _lease_of(db, a), await _lease_of(db, b)
    holder = _RowHolder(db)
    await holder.hold(held)
    try:
        async with db.acquire() as conn:
            revoked = await asyncio.wait_for(
                leases.revoke_leases_of_terminal_executions(conn), WAIT_SECONDS
            )
    finally:
        await holder.release()
    assert revoked == [other]
    async with db.acquire() as conn:
        assert await leases.revoke_leases_of_terminal_executions(conn) == [held]


@pytest.mark.asyncio
async def test_the_expiry_housekeeping_skips_a_held_row(db):
    _owner, a, _b = await _two_connector_session(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE connector_credential_leases SET issued_at = now() - "
            "interval '1 day', expires_at = now() - interval '1 second'"
        )
    held = await _lease_of(db, a)
    holder = _RowHolder(db)
    await holder.hold(held)
    try:
        async with db.acquire() as conn:
            retired = await asyncio.wait_for(
                leases.retire_expired_leases(conn), WAIT_SECONDS
            )
    finally:
        await holder.release()
    assert retired == 1
    async with db.acquire() as conn:
        assert await leases.retire_expired_leases(conn) == 1
        assert (
            await conn.fetchval(
                "SELECT revoke_reason FROM connector_credential_leases WHERE id = $1",
                UUID(held),
            )
            == leases.EXPIRED
        )
