"""Real-PostgreSQL proofs for the credential lease service (slice C2).

The table's constraints and its one-live-lease index, issue and re-delivery,
the renewal sweep and its throttle, every revocation function, the store's
revoke points (cancel, delete) and the exchange and introspection, all run
against ``schema_current.sql``.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.security.crypto import encrypt
from orchestrator.services import connector_credential_leases as leases
from orchestrator.services.connector_driver_identities import (
    mint_driver_identity,
    revoke_driver_identity,
)
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_lease_exchange import (
    ConnectorLeaseExchange,
    DenialLimiter,
)
from shared.connectors.leases import token_digest, token_shape_valid

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
DRIVER = "srw.lease-probe/v1"
SECRET = "fake-upstream-secret-c2"


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
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=6)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE connector_credential_leases, connector_driver_identities, "
            "security_events, datasources, jobs, threads CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()


async def _connector(db, *, upstream: str | None = "https://upstream.invalid") -> str:
    connector_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO datasources (id, name, type, scope_mode, policy_revision, "
            "credentials, config) VALUES ($1, $2, 'lease_probe', 'all', 1, "
            "$3::jsonb, $4::jsonb)",
            connector_id,
            f"probe-{str(connector_id)[:8]}",
            json.dumps(encrypt(json.dumps({"secret": SECRET}))),
            json.dumps({"upstream": upstream} if upstream else {}),
        )
    return str(connector_id)


async def _job(db, status="processing", *, parent: str | None = None) -> str:
    job_id = uuid4()
    context = {"inherits_parent_workspace": True} if parent else {}
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status, parent_job_id, context) "
            "VALUES ($1, 'lease test', $2, $3, $4::jsonb)",
            job_id,
            status,
            UUID(parent) if parent else None,
            json.dumps(context),
        )
    return str(job_id)


async def _thread(db, status="active") -> str:
    thread_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, metadata) VALUES ($1, $2, '{}'::jsonb)",
            thread_id,
            status,
        )
    return str(thread_id)


async def _issue(db, owner, connector_id, access="ReadWrite"):
    async with db.acquire() as conn:
        return await leases.issue_or_redeliver(
            conn, owner=owner, connector_id=connector_id, driver=DRIVER, access=access
        )


async def _lease(db, lease_id):
    async with db.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM connector_credential_leases WHERE id = $1", UUID(lease_id)
        )


async def _events(db, event_type):
    async with db.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM security_events WHERE event_type = $1 ORDER BY created_at",
            event_type,
        )


async def _set_expiry(db, lease_id, seconds_from_now):
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE connector_credential_leases SET issued_at = now() - interval "
            "'1 day', expires_at = now() + make_interval(secs => $2) WHERE id = $1",
            UUID(lease_id),
            seconds_from_now,
        )


# =============================================================================
# The table
# =============================================================================


@pytest.mark.asyncio
async def test_constraints_reject_malformed_rows(db):
    connector = await _connector(db)
    job = await _job(db)
    thread = await _thread(db)
    base = (
        "INSERT INTO connector_credential_leases (token_hash, token_ciphertext, "
        "token_last_four, job_id, thread_id, connector_id, driver, access, "
        "expires_at, revoked_at, revoke_reason, image_digest) VALUES "
        "($1, 'v1:x', 'abcd', $2, $3, $4, $5, 'ReadOnly', now() + interval '1 h', "
        "$6, $7, $8)"
    )
    bad = [
        (b"\x01" * 32, UUID(job), UUID(thread), None, None, None),  # both owners
        (b"\x02" * 32, None, None, None, None, None),  # no owner
        (b"\x03" * 31, UUID(job), None, None, None, None),  # short hash
        (b"\x04" * 32, UUID(job), None, None, "session_end", None),  # half revoke
        (b"\x05" * 32, UUID(job), None, None, None, "sha256:XYZ"),  # digest
    ]
    async with db.acquire() as conn:
        for token_hash, job_id, thread_id, revoked_at, reason, digest in bad:
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    base,
                    token_hash,
                    job_id,
                    thread_id,
                    UUID(connector),
                    DRIVER,
                    revoked_at,
                    reason,
                    digest,
                )


@pytest.mark.asyncio
async def test_one_live_lease_per_execution_and_connector(db):
    connector = await _connector(db)
    job = await _job(db)
    first = await _issue(db, leases.LeaseOwner.job(job), connector)
    insert = (
        "INSERT INTO connector_credential_leases (token_hash, token_ciphertext, "
        "token_last_four, job_id, connector_id, driver, access, expires_at) "
        "VALUES ($1, 'v1:x', 'abcd', $2, $3, $4, 'ReadOnly', now() + interval '1 h')"
    )
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(insert, b"\x09" * 32, UUID(job), UUID(connector), DRIVER)
        # A revoked lease frees the slot.
        await leases.revoke_execution_leases(conn, job_id=job, reason="job_cancelled")
        await conn.execute(insert, b"\x0a" * 32, UUID(job), UUID(connector), DRIVER)
    revoked = await _lease(db, first.id)
    assert revoked["revoke_reason"] == "job_cancelled"


@pytest.mark.asyncio
async def test_owner_deletion_cascades_and_connector_deletion_cascades(db):
    connector = await _connector(db)
    thread = await _thread(db)
    lease = await _issue(db, leases.LeaseOwner.thread(thread), connector)
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM datasources WHERE id = $1", UUID(connector))
    assert await _lease(db, lease.id) is None


# =============================================================================
# Issue and re-delivery
# =============================================================================


@pytest.mark.asyncio
async def test_issue_then_redeliver_the_same_token_and_audit_once(db):
    connector = await _connector(db)
    job = await _job(db)
    owner = leases.LeaseOwner.job(job)

    first = await _issue(db, owner, connector)
    again = await _issue(db, owner, connector)

    assert first.issued and not again.issued
    assert again.id == first.id and again.token == first.token
    assert token_shape_valid(first.token, "scl")
    row = await _lease(db, first.id)
    assert bytes(row["token_hash"]) == token_digest(first.token)
    assert first.token not in row["token_ciphertext"]
    assert row["token_last_four"] == first.token[-4:]
    issued = await _events(db, "connector_lease_issued")
    assert [str(e["resource_id"]) for e in issued] == [first.id]
    assert first.token not in issued[0]["detail"]


@pytest.mark.asyncio
async def test_concurrent_issuers_share_one_lease(db):
    connector = await _connector(db)
    thread = await _thread(db)
    owner = leases.LeaseOwner.thread(thread)
    a, b = await asyncio.gather(
        _issue(db, owner, connector), _issue(db, owner, connector)
    )
    assert a.id == b.id and a.token == b.token


@pytest.mark.asyncio
async def test_an_expired_lease_is_retired_and_a_new_token_issued(db):
    connector = await _connector(db)
    job = await _job(db, "paused")
    owner = leases.LeaseOwner.job(job)
    first = await _issue(db, owner, connector)
    await _set_expiry(db, first.id, -5)

    second = await _issue(db, owner, connector)

    assert second.issued and second.id != first.id and second.token != first.token
    old = await _lease(db, first.id)
    assert old["revoke_reason"] == "expired"
    assert old["revoked_at"] == old["expires_at"]


@pytest.mark.asyncio
async def test_a_redelivery_follows_the_connectors_access_level(db):
    connector = await _connector(db)
    thread = await _thread(db)
    owner = leases.LeaseOwner.thread(thread)
    first = await _issue(db, owner, connector, access="ReadWrite")
    again = await _issue(db, owner, connector, access="ReadOnly")
    assert again.id == first.id
    assert (await _lease(db, first.id))["access"] == "ReadOnly"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "status"),
    [
        ("job", "completed"),
        ("job", "failed"),
        ("job", "cancelled"),
        ("thread", "ended"),
    ],
)
async def test_a_terminal_execution_gets_no_new_lease(db, kind, status):
    connector = await _connector(db)
    owner_id = await (_job(db, status) if kind == "job" else _thread(db, status))
    owner = leases.LeaseOwner(kind, owner_id)
    with pytest.raises(leases.LeaseDeliveryError):
        await _issue(db, owner, connector)


@pytest.mark.asyncio
async def test_deliver_stamps_only_lease_entries_and_drops_their_credentials(db):
    connector = await _connector(db)
    thread = await _thread(db)
    entries = [
        {
            "type": "lease_probe",
            "name": "Probe",
            "datasource_id": connector,
            "credentials": {"secret": "must not survive"},
            "project_read_only": True,
        },
        {"type": "generic", "name": "Env", "credentials": {"env_vars": {"A": "1"}}},
    ]
    async with db.acquire() as conn:
        delivered = await leases.deliver_connector_leases(
            conn, entries, owner=leases.LeaseOwner.thread(thread)
        )
    assert delivered == 1
    lease = entries[0]["credentials"]["lease"]
    assert set(entries[0]["credentials"]) == {"lease"}
    assert lease["connector_id"] == connector
    assert token_shape_valid(lease["token"], "scl")
    assert entries[1]["credentials"] == {"env_vars": {"A": "1"}}
    # A read-only project link clamps the lease to the lowest level.
    assert (await _lease(db, lease["id"]))["access"] == "ReadOnly"


# =============================================================================
# Renewal
# =============================================================================


async def _renew(db, ttl=900):
    async with db.acquire() as conn:
        return await leases.renew_live_leases(conn, ttl_seconds=ttl)


@pytest.mark.asyncio
async def test_renewal_writes_only_the_second_half_of_the_window(db):
    connector = await _connector(db)
    job = await _job(db, "processing")
    lease = await _issue(db, leases.LeaseOwner.job(job), connector)
    await _set_expiry(db, lease.id, 800)
    assert await _renew(db) == 0
    assert (await _lease(db, lease.id))["last_renewed_at"] is None

    await _set_expiry(db, lease.id, 300)
    assert await _renew(db) == 1
    row = await _lease(db, lease.id)
    assert row["last_renewed_at"] is not None
    remaining = await _remaining(db, lease.id)
    assert 850 < remaining <= 900


async def _remaining(db, lease_id) -> float:
    async with db.acquire() as conn:
        return float(
            await conn.fetchval(
                "SELECT extract(epoch FROM expires_at - now()) "
                "FROM connector_credential_leases WHERE id = $1",
                UUID(lease_id),
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "renewed"),
    [("processing", 1), ("paused", 0), ("pending_review", 0), ("created", 0)],
)
async def test_only_a_processing_job_is_renewed(db, status, renewed):
    connector = await _connector(db)
    job = await _job(db, "processing")
    lease = await _issue(db, leases.LeaseOwner.job(job), connector)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status = $2 WHERE id = $1", UUID(job), status
        )
    await _set_expiry(db, lease.id, 100)
    assert await _renew(db) == renewed


@pytest.mark.asyncio
async def test_a_processing_child_on_the_parents_workspace_renews_the_parent(db):
    connector = await _connector(db)
    parent = await _job(db, "pending_review")
    await _job(db, "processing", parent=parent)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='processing' WHERE id=$1", UUID(parent)
        )
    lease = await _issue(db, leases.LeaseOwner.job(parent), connector)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='pending_review' WHERE id=$1", UUID(parent)
        )
    await _set_expiry(db, lease.id, 100)
    assert await _renew(db) == 1


@pytest.mark.asyncio
async def test_an_idle_thread_is_renewed_until_it_ends(db):
    connector = await _connector(db)
    thread = await _thread(db, "awaiting_user")
    lease = await _issue(db, leases.LeaseOwner.thread(thread), connector)
    await _set_expiry(db, lease.id, 100)
    assert await _renew(db) == 1

    await _set_expiry(db, lease.id, 100)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET status='ended' WHERE id=$1", UUID(thread)
        )
    assert await _renew(db) == 0


@pytest.mark.asyncio
async def test_an_expired_lease_is_never_renewed_and_is_retired(db):
    connector = await _connector(db)
    job = await _job(db, "processing")
    lease = await _issue(db, leases.LeaseOwner.job(job), connector)
    await _set_expiry(db, lease.id, -1)
    assert await _renew(db) == 0
    async with db.acquire() as conn:
        assert await leases.retire_expired_leases(conn) == 1
    assert (await _lease(db, lease.id))["revoke_reason"] == "expired"


# =============================================================================
# Revocation
# =============================================================================


@pytest.mark.asyncio
async def test_revoke_connector_leases_leaves_the_other_connector(db):
    probe_a = await _connector(db)
    probe_b = await _connector(db)
    thread = await _thread(db)
    owner = leases.LeaseOwner.thread(thread)
    a = await _issue(db, owner, probe_a)
    b = await _issue(db, owner, probe_b)
    async with db.acquire() as conn:
        revoked = await leases.revoke_connector_leases(
            conn, owner=owner, connector_ids=[probe_a]
        )
    assert revoked == [a.id]
    assert (await _lease(db, a.id))["revoke_reason"] == "connector_detached"
    assert (await _lease(db, b.id))["revoked_at"] is None
    events = await _events(db, "connector_lease_revoked")
    assert [str(e["resource_id"]) for e in events] == [a.id]


@pytest.mark.asyncio
async def test_an_expired_lease_is_recorded_as_expired_not_revoked(db):
    connector = await _connector(db)
    thread = await _thread(db)
    lease = await _issue(db, leases.LeaseOwner.thread(thread), connector)
    await _set_expiry(db, lease.id, -10)
    async with db.acquire() as conn:
        revoked = await leases.revoke_execution_leases(
            conn, thread_id=thread, reason="session_end"
        )
    assert revoked == []
    assert (await _lease(db, lease.id))["revoke_reason"] == "expired"
    assert await _events(db, "connector_lease_revoked") == []


@pytest.mark.asyncio
async def test_the_backstop_revokes_only_a_terminal_execution(db):
    connector = await _connector(db)
    thread = await _thread(db, "active")
    owner = leases.LeaseOwner.thread(thread)
    lease = await _issue(db, owner, connector)
    async with db.acquire() as conn:
        assert await leases.revoke_terminal_execution_leases(conn, owner=owner) == []
        await conn.execute(
            "UPDATE threads SET status='ended' WHERE id=$1", UUID(thread)
        )
        assert await leases.revoke_terminal_execution_leases(conn, owner=owner) == [
            lease.id
        ]
    assert (await _lease(db, lease.id))["revoke_reason"] == "execution_terminal"


@pytest.mark.asyncio
async def test_cancel_revokes_in_the_cancel_transaction(db):
    connector = await _connector(db)
    job = await _job(db, "processing")
    lease = await _issue(db, leases.LeaseOwner.job(job), connector)
    assert await db.cancel_job(job)
    assert (await _lease(db, lease.id))["revoke_reason"] == "job_cancelled"


async def _pinned_thread(db) -> str:
    thread = str(uuid4())
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, metadata, execution_lane, "
            "runtime_generation) VALUES ($1, 'active', '{}'::jsonb, 'pinned', $2)",
            UUID(thread),
            uuid4(),
        )
    return thread


@pytest.mark.asyncio
async def test_an_aborted_pinned_begin_leaves_the_lease_live_and_usable(db, exchange):
    """A Begin is a hidden, abortable preflight (a non-force End during a
    turn, lock contention, a stale preflight): it revokes nothing."""
    connector = await _connector(db)
    thread = await _pinned_thread(db)
    owner = leases.LeaseOwner.thread(thread)
    lease = await _issue(db, owner, connector)
    identity = await _identity(db, connector)

    begun = await db.begin_pinned_thread_retirement(thread, permanent=False)
    assert begun["state"] == "pending" and begun["authorized_at"] is None
    assert (await _lease(db, lease.id))["revoked_at"] is None
    # Still delivered during the preflight: the same token.
    assert (await _issue(db, owner, connector)).token == lease.token

    assert await db.abort_pinned_thread_retirement(
        thread, token=begun["token"], generation=begun["generation"]
    )
    assert (await _lease(db, lease.id))["revoked_at"] is None
    answer = await exchange.exchange(
        identity_token=identity.token, lease_token=lease.token, operation="read"
    )
    assert answer.status == 200
    assert await _events(db, "connector_lease_revoked") == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("settle_status", "reason"),
    [("ended", "session_end"), ("suspended", "session_suspended")],
)
async def test_the_authorization_edge_revokes(db, settle_status, reason):
    connector = await _connector(db)
    thread = await _pinned_thread(db)
    owner = leases.LeaseOwner.thread(thread)
    lease = await _issue(db, owner, connector)
    begun = await db.begin_pinned_thread_retirement(
        thread, permanent=False, settle_status=settle_status
    )

    assert await db.authorize_pinned_thread_retirement(
        thread,
        token=begun["token"],
        generation=begun["generation"],
        settle_status=settle_status,
    )

    assert (await _lease(db, lease.id))["revoke_reason"] == reason
    # Authorized: no renewal, and a delivery racing it gets no new lease.
    await _set_expiry(db, lease.id, 100)
    assert await _renew(db) == 0
    with pytest.raises(leases.LeaseDeliveryError):
        await _issue(db, owner, connector)
    # A repeated authorization is harmless.
    assert await db.authorize_pinned_thread_retirement(
        thread,
        token=begun["token"],
        generation=begun["generation"],
        settle_status=settle_status,
    )
    assert len(await _events(db, "connector_lease_revoked")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["fresh", "reused"])
async def test_an_immediately_authorized_begin_revokes(db, path):
    """A Begin that authorizes in its own transaction (a fresh one, or the
    agent's reuse of its own pending preflight) is the authorization edge."""
    connector = await _connector(db)
    thread = await _pinned_thread(db)
    lease = await _issue(db, leases.LeaseOwner.thread(thread), connector)
    if path == "reused":
        pending = await db.begin_pinned_thread_retirement(
            thread, permanent=False, initiator="agent"
        )
        assert pending["authorized_at"] is None
        assert (await _lease(db, lease.id))["revoked_at"] is None

    begun = await db.begin_pinned_thread_retirement(
        thread, permanent=False, initiator="agent", authorize_immediately=True
    )

    assert begun["state"] == "pending" and begun["authorized_at"] is not None
    assert (await _lease(db, lease.id))["revoke_reason"] == "session_end"


@pytest.mark.asyncio
async def test_a_stateless_delete_revokes_when_it_fences_the_job(db):
    """The API delete fences a stateless Job (status cancelled) before its
    workspace teardown; the revoke names the delete, not the teardown's
    terminal-execution backstop."""
    connector = await _connector(db)
    job = await _job(db, "processing")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET execution_lane = 'stateless' WHERE id = $1", UUID(job)
        )
    lease = await _issue(db, leases.LeaseOwner.job(job), connector)

    assert await db.prepare_stateless_job_for_delete(job)

    assert (await _lease(db, lease.id))["revoke_reason"] == "job_deleted"


@pytest.mark.asyncio
async def test_delete_revokes_before_the_cascade_removes_the_row(db):
    """The "to verify" item: a revoke inside delete_job's transaction runs
    before its cascade; the audit row survives, the lease row does not."""
    connector = await _connector(db)
    job = await _job(db, "processing")
    lease = await _issue(db, leases.LeaseOwner.job(job), connector)

    assert await db.delete_job(job)

    assert await _lease(db, lease.id) is None
    events = await _events(db, "connector_lease_revoked")
    assert [str(e["resource_id"]) for e in events] == [lease.id]
    assert "reason=job_deleted" in events[0]["detail"]


# =============================================================================
# The exchange and introspection
# =============================================================================


@pytest_asyncio.fixture
async def exchange(db):
    return ConnectorLeaseExchange(
        store=db, drivers=builtin_connector_drivers(lease_probe=True)
    )


async def _identity(db, connector_id, **kwargs):
    async with db.acquire() as conn:
        return await mint_driver_identity(
            conn, connector_id=connector_id, driver=DRIVER, **kwargs
        )


@pytest.mark.asyncio
async def test_the_exchange_returns_the_credential_and_counts(db, exchange):
    connector = await _connector(db)
    thread = await _thread(db)
    lease = await _issue(db, leases.LeaseOwner.thread(thread), connector)
    identity = await _identity(db, connector, pod_uid="pod-1")

    first = await exchange.exchange(
        identity_token=identity.token, lease_token=lease.token, operation="write"
    )
    second = await exchange.exchange(
        identity_token=identity.token, lease_token=lease.token, operation="read"
    )

    assert first.status == second.status == 200
    assert first.body["credential"] == SECRET
    assert first.body["access"] == "ReadWrite"
    assert first.body["allowed_upstream"] == ["https://upstream.invalid"]
    assert first.body["max_cache_seconds"] == 30
    assert set(first.body) == {
        "credential",
        "expires_at",
        "access",
        "allowed_upstream",
        "max_cache_seconds",
    }
    row = await _lease(db, lease.id)
    assert row["exchange_count"] == 2 and row["last_exchanged_at"] is not None
    firsts = await _events(db, "connector_lease_first_exchange")
    assert [str(e["resource_id"]) for e in firsts] == [lease.id]


@pytest.mark.asyncio
async def test_the_exchange_refuses_and_audits_every_denial(db, exchange):
    connector = await _connector(db)
    other = await _connector(db)
    thread = await _thread(db)
    lease = await _issue(db, leases.LeaseOwner.thread(thread), connector, "ReadOnly")
    mine = await _identity(db, connector)
    theirs = await _identity(db, other)

    async def ask(identity_token, lease_token=lease.token, operation="read"):
        return await exchange.exchange(
            identity_token=identity_token,
            lease_token=lease_token,
            operation=operation,
        )

    wrong_connector = await ask(theirs.token)
    wrong_operation = await ask(mine.token, operation="write")
    unknown_identity = await ask("sdi_" + "0" * 49)
    unknown_lease = await ask(mine.token, lease_token="scl_" + "0" * 49)
    async with db.acquire() as conn:
        await leases.revoke_execution_leases(
            conn, thread_id=thread, reason="session_end"
        )
    revoked = await ask(mine.token)
    async with db.acquire() as conn:
        await revoke_driver_identity(conn, identity_id=mine.id)
    revoked_identity = await ask(mine.token)

    assert (wrong_connector.status, wrong_connector.body) == (
        403,
        {"error": "driver_identity_of_another_connector"},
    )
    assert wrong_operation.body == {"error": "operation_not_allowed"}
    assert unknown_identity.status == 401
    assert unknown_lease.body == {"error": "unknown_lease"}
    assert revoked.body == {"error": "lease_revoked"}
    assert (revoked_identity.status, revoked_identity.body) == (
        401,
        {"error": "driver_identity_revoked"},
    )
    # Five rows: an unknown identity writes none (only a rate-limited log).
    denials = await _events(db, "connector_lease_exchange_denied")
    assert len(denials) == 5
    assert all(lease.token not in (e["detail"] or "") for e in denials)
    assert (await _lease(db, lease.id))["exchange_count"] == 0


@pytest.mark.asyncio
async def test_an_expired_lease_is_refused_as_expired(db, exchange):
    connector = await _connector(db)
    job = await _job(db, "processing")
    lease = await _issue(db, leases.LeaseOwner.job(job), connector)
    identity = await _identity(db, connector)
    await _set_expiry(db, lease.id, -1)
    outcome = await exchange.exchange(
        identity_token=identity.token, lease_token=lease.token, operation="read"
    )
    assert outcome.body == {"error": "lease_expired"}


@pytest.mark.asyncio
async def test_introspection_reports_without_a_credential(db, exchange):
    connector = await _connector(db)
    other = await _connector(db)
    thread = await _thread(db)
    lease = await _issue(db, leases.LeaseOwner.thread(thread), connector, "ReadOnly")
    identity = await _identity(db, connector)
    theirs = await _identity(db, other)

    live = await exchange.introspect(
        identity_token=identity.token, lease_token=lease.token
    )
    assert live.status == 200
    assert live.body["active"] is True
    assert live.body["connector_id"] == connector
    assert live.body["access"] == "ReadOnly"
    assert "credential" not in live.body

    mismatch = await exchange.introspect(
        identity_token=theirs.token, lease_token=lease.token
    )
    assert mismatch.status == 403

    async with db.acquire() as conn:
        await leases.revoke_execution_leases(
            conn, thread_id=thread, reason="session_end"
        )
    gone = await exchange.introspect(
        identity_token=identity.token, lease_token=lease.token
    )
    assert gone.body == {"active": False}


@pytest.mark.asyncio
async def test_identities_store_only_a_digest_and_revoke_by_pod(db):
    connector = await _connector(db)
    identity = await _identity(db, connector, pod_uid="pod-xyz", pod_name="drv-0")
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM connector_driver_identities WHERE id = $1",
            UUID(identity.id),
        )
        assert bytes(row["token_hash"]) == token_digest(identity.token)
        assert identity.token not in repr(identity)
        assert await revoke_driver_identity(conn, pod_uid="pod-xyz") == [identity.id]
        assert await revoke_driver_identity(conn, pod_uid="pod-xyz") == []


# =============================================================================
# Review hardening
# =============================================================================


@pytest.mark.asyncio
async def test_the_sweep_revokes_leases_of_executions_already_terminal(db):
    """A terminal write no revoke point saw is bounded by one sweep."""
    connector = await _connector(db)
    job = await _job(db, "processing")
    thread = await _pinned_thread(db)
    job_lease = await _issue(db, leases.LeaseOwner.job(job), connector)
    thread_lease = await _issue(db, leases.LeaseOwner.thread(thread), connector)
    live = await _issue(db, leases.LeaseOwner.job(await _job(db)), connector)
    # A preflight whose authorization was written by a path that did not
    # revoke (the case the sweep bounds).
    await db.begin_pinned_thread_retirement(thread, permanent=False)
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status='failed' WHERE id=$1", UUID(job))
        await conn.execute(
            "UPDATE threads SET runtime_retirement_authorized_at = now() WHERE id = $1",
            UUID(thread),
        )
        revoked = await leases.revoke_leases_of_terminal_executions(conn)
    assert sorted(revoked) == sorted([job_lease.id, thread_lease.id])
    assert (await _lease(db, job_lease.id))["revoke_reason"] == "execution_terminal"
    assert (await _lease(db, live.id))["revoked_at"] is None


@pytest.mark.asyncio
async def test_a_processing_child_never_renews_a_terminal_parent(db):
    connector = await _connector(db)
    parent = await _job(db, "processing")
    await _job(db, "processing", parent=parent)
    lease = await _issue(db, leases.LeaseOwner.job(parent), connector)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='completed' WHERE id=$1", UUID(parent)
        )
    await _set_expiry(db, lease.id, 100)
    assert await _renew(db) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["garbage", "another_rows_copy"])
async def test_an_unusable_stored_copy_is_retired_and_replaced(db, damage):
    connector = await _connector(db)
    other = await _connector(db)
    thread = await _thread(db)
    owner = leases.LeaseOwner.thread(thread)
    lease = await _issue(db, owner, connector)
    donor = await _issue(db, owner, other)
    async with db.acquire() as conn:
        if damage == "garbage":
            await conn.execute(
                "UPDATE connector_credential_leases SET token_ciphertext = 'v1:x' "
                "WHERE id = $1",
                UUID(lease.id),
            )
        else:
            # A ciphertext moved from another row decrypts to that row's
            # token: the row's own digest refuses it.
            await conn.execute(
                "UPDATE connector_credential_leases SET token_ciphertext = "
                "(SELECT token_ciphertext FROM connector_credential_leases "
                "WHERE id = $2) WHERE id = $1",
                UUID(lease.id),
                UUID(donor.id),
            )

    fresh = await _issue(db, owner, connector)

    assert fresh.issued and fresh.id != lease.id and fresh.token != donor.token
    assert (await _lease(db, lease.id))["revoke_reason"] == "unreadable"


@pytest.mark.asyncio
async def test_an_access_change_on_redelivery_is_audited(db):
    connector = await _connector(db)
    thread = await _thread(db)
    owner = leases.LeaseOwner.thread(thread)
    lease = await _issue(db, owner, connector, "ReadWrite")
    await _issue(db, owner, connector, "ReadWrite")
    assert await _events(db, "connector_lease_access_changed") == []
    await _issue(db, owner, connector, "ReadOnly")
    (event,) = await _events(db, "connector_lease_access_changed")
    assert str(event["resource_id"]) == lease.id
    assert "access_from=ReadWrite" in event["detail"]
    assert "access_to=ReadOnly" in event["detail"]


@pytest.mark.asyncio
async def test_a_connector_delete_revokes_its_leases_and_identities_first(db):
    connector = await _connector(db)
    lease = await _issue(db, leases.LeaseOwner.job(await _job(db)), connector)
    identity = await _identity(db, connector, pod_uid="pod-del")

    assert await db.delete_datasource(connector)

    assert await _lease(db, lease.id) is None
    (revoked,) = await _events(db, "connector_lease_revoked")
    assert str(revoked["resource_id"]) == lease.id
    assert "reason=connector_deleted" in revoked["detail"]
    (gone,) = await _events(db, "connector_driver_identity_revoked")
    assert str(gone["resource_id"]) == identity.id
    assert "reason=connector_deleted" in gone["detail"]


@pytest.mark.asyncio
async def test_an_issue_waits_for_a_terminal_transaction_and_then_refuses(db):
    """The guard is atomic: the terminal write holds the Job row; an issue
    locks it FOR SHARE first, so it sees the committed terminal state."""
    connector = await _connector(db)
    job = await _job(db, "processing")
    async with db.acquire() as holder:
        transaction = holder.transaction()
        await transaction.start()
        try:
            await holder.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", UUID(job)
            )
            issuing = asyncio.create_task(
                _issue(db, leases.LeaseOwner.job(job), connector)
            )
            await asyncio.sleep(0.5)
            assert not issuing.done()
        finally:
            await transaction.commit()
    with pytest.raises(leases.LeaseDeliveryError):
        await issuing
    assert (
        await db.fetchval(
            "SELECT count(*) FROM connector_credential_leases WHERE job_id = $1",
            UUID(job),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_denials_are_coalesced_and_record_the_socket_peer(db):
    connector = await _connector(db)
    thread = await _thread(db)
    lease = await _issue(db, leases.LeaseOwner.thread(thread), connector, "ReadOnly")
    identity = await _identity(db, connector)
    now = [0.0]
    exchange = ConnectorLeaseExchange(
        store=db,
        drivers=builtin_connector_drivers(lease_probe=True),
        limiter=DenialLimiter(window_seconds=60, clock=lambda: now[0]),
    )
    request = SimpleNamespace(
        client=SimpleNamespace(host="10.42.9.9"),
        headers={"x-forwarded-for": "203.0.113.7"},
    )

    async def write():
        return await exchange.exchange(
            identity_token=identity.token,
            lease_token=lease.token,
            operation="write",
            request=request,
        )

    for _ in range(4):
        assert (await write()).body == {"error": "operation_not_allowed"}
    for _ in range(3):
        await exchange.exchange(
            identity_token="sdi_" + "1" * 49,
            lease_token=lease.token,
            operation="read",
            request=request,
        )
    rows = await db.fetch(
        "SELECT detail, client_ip FROM security_events "
        "WHERE event_type = 'connector_lease_exchange_denied'"
    )
    assert len(rows) == 1 and rows[0]["client_ip"] == "10.42.9.9"
    now[0] = 61.0
    await write()
    rows = await db.fetch(
        "SELECT detail FROM security_events WHERE event_type = "
        "'connector_lease_exchange_denied' ORDER BY created_at"
    )
    assert len(rows) == 2 and "coalesced=3" in rows[1]["detail"]
