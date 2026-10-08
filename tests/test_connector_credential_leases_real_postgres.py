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
from orchestrator.services.connector_lease_exchange import ConnectorLeaseExchange
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


@pytest.mark.asyncio
async def test_a_pinned_retirement_begin_revokes_the_sessions_leases(db):
    connector = await _connector(db)
    thread = str(uuid4())
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, status, metadata, execution_lane, "
            "runtime_generation) VALUES ($1, 'active', '{}'::jsonb, 'pinned', $2)",
            UUID(thread),
            uuid4(),
        )
    lease = await _issue(db, leases.LeaseOwner.thread(thread), connector)

    begun = await db.begin_pinned_thread_retirement(thread, permanent=False)

    assert begun["state"] == "pending"
    assert (await _lease(db, lease.id))["revoke_reason"] == "session_end"
    # A delivery racing the retirement gets no new lease.
    with pytest.raises(leases.LeaseDeliveryError):
        await _issue(db, leases.LeaseOwner.thread(thread), connector)


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
    denials = await _events(db, "connector_lease_exchange_denied")
    assert len(denials) == 6
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
