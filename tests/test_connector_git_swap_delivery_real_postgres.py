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
        db, connector, stop="upstream_unreachable", error="x509: unknown authority"
    )
    async with db.acquire() as conn:
        problem = await swaps.launch_problem(conn, connector)
    assert problem == (
        "its driver pod did not start (upstream_unreachable: x509: unknown authority)"
    )
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
        swaps.GitSwapDeliverySettings(installed=True, max_installation=2)
    )
    serving = await _connector(db)
    await _pod(db, serving)
    # A newer failed pod does not hide the live one.
    await _pod(db, serving, stop="start_timeout", generation="g2")
    async with db.acquire() as conn:
        assert await swaps.launch_problem(conn, serving) is None
        waiting = await _connector(db)
        assert "cap of 2 driver pods" in await swaps.launch_problem(conn, waiting)


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
        assert "static-pool" in await swaps.owner_workspace_problem(
            conn, leases.LeaseOwner.job(str(job_id))
        )
        assert (
            await swaps.owner_workspace_problem(
                conn, leases.LeaseOwner.thread(str(thread_id))
            )
            is None
        )
