"""MCP OAuth grants on a real PostgreSQL (G6).

A connection is one ``auth_tokens`` access row plus one ``mcp_oauth_grants``
row holding the hashed refresh token. Rotation swaps both in place; a
replayed refresh token, a revoked access row or an expired grant ends it.
Design: knowledge-base/knowledge/features/knowledge_base_mcp_for_chat_uis.md G6.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)


def _h(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


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
    async with store.acquire() as conn:
        await conn.execute("TRUNCATE mcp_oauth_grants, auth_tokens, users CASCADE")
    try:
        yield store
    finally:
        await store.close()


async def _user(db: PostgresDB) -> UUID:
    user_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name, email) VALUES ($1, 'g6', $2)",
            user_id,
            f"{user_id}@example.test",
        )
    return user_id


async def _connect(
    db, user_id, *, grant_days: int | None = 30, access="srw_a1", refresh="srwr_r1"
):
    now = datetime.now(timezone.utc)
    return await db.create_mcp_oauth_grant(
        user_id=str(user_id),
        name="OAuth: Claude",
        oauth_client_id="https://claude.ai/oauth/client.json",
        scope="user",
        origin="oauth:Claude",
        access_token_hash=_h(access),
        access_token_prefix=access[:12],
        access_expires_at=now + timedelta(hours=1),
        refresh_token_hash=_h(refresh),
        grant_expires_at=now + timedelta(days=grant_days) if grant_days else None,
    )


async def _token_rows(db, user_id) -> int:
    async with db.acquire() as conn:
        return await conn.fetchval(
            "SELECT count(*) FROM auth_tokens WHERE user_id = $1", user_id
        )


@pytest.mark.asyncio
async def test_a_connection_is_one_access_row_and_one_grant(db):
    user_id = await _user(db)
    await _connect(db, user_id)

    assert await db.get_mcp_token_by_hash(_h("srw_a1")) is not None
    grant = await db.get_mcp_oauth_grant_by_refresh_hash(_h("srwr_r1"))
    assert grant["oauth_client_id"] == "https://claude.ai/oauth/client.json"
    assert grant["scope"] == "user"
    # A refresh token is never an access token.
    assert await db.get_mcp_token_by_hash(_h("srwr_r1")) is None


@pytest.mark.asyncio
async def test_rotation_swaps_both_tokens_in_place(db):
    user_id = await _user(db)
    await _connect(db, user_id)

    rotated = await db.rotate_mcp_oauth_grant(
        refresh_token_hash=_h("srwr_r1"),
        new_refresh_token_hash=_h("srwr_r2"),
        new_access_token_hash=_h("srw_a2"),
        new_access_token_prefix="srw_a2",
        access_ttl_seconds=3600,
    )

    assert rotated["scope"] == "user"
    assert await db.get_mcp_token_by_hash(_h("srw_a1")) is None
    assert await db.get_mcp_token_by_hash(_h("srw_a2")) is not None
    assert await db.get_mcp_oauth_grant_by_refresh_hash(_h("srwr_r1")) is None
    assert await db.get_mcp_oauth_grant_by_refresh_hash(_h("srwr_r2")) is not None
    # Still one row in the user's token list.
    assert await _token_rows(db, user_id) == 1


@pytest.mark.asyncio
async def test_the_grant_lifetime_caps_the_access_token(db):
    user_id = await _user(db)
    now = datetime.now(timezone.utc)
    await db.create_mcp_oauth_grant(
        user_id=str(user_id),
        name="OAuth: Claude",
        oauth_client_id="c",
        scope="user",
        origin="oauth:Claude",
        access_token_hash=_h("srw_a1"),
        access_token_prefix="srw_a1",
        access_expires_at=now + timedelta(minutes=5),
        refresh_token_hash=_h("srwr_r1"),
        grant_expires_at=now + timedelta(minutes=10),
    )

    rotated = await db.rotate_mcp_oauth_grant(
        refresh_token_hash=_h("srwr_r1"),
        new_refresh_token_hash=_h("srwr_r2"),
        new_access_token_hash=_h("srw_a2"),
        new_access_token_prefix="srw_a2",
        access_ttl_seconds=3600,
    )

    assert rotated["access_expires_at"] <= rotated["grant_expires_at"]


@pytest.mark.asyncio
async def test_a_connection_without_expiry_gets_the_full_access_ttl(db):
    user_id = await _user(db)
    await _connect(db, user_id, grant_days=None)

    rotated = await db.rotate_mcp_oauth_grant(
        refresh_token_hash=_h("srwr_r1"),
        new_refresh_token_hash=_h("srwr_r2"),
        new_access_token_hash=_h("srw_a2"),
        new_access_token_prefix="srw_a2",
        access_ttl_seconds=3600,
    )

    remaining = rotated["access_expires_at"] - datetime.now(timezone.utc)
    assert timedelta(minutes=55) < remaining <= timedelta(hours=1)


@pytest.mark.asyncio
async def test_a_replayed_refresh_token_ends_the_connection(db):
    user_id = await _user(db)
    await _connect(db, user_id)
    await db.rotate_mcp_oauth_grant(
        refresh_token_hash=_h("srwr_r1"),
        new_refresh_token_hash=_h("srwr_r2"),
        new_access_token_hash=_h("srw_a2"),
        new_access_token_prefix="srw_a2",
        access_ttl_seconds=3600,
    )

    assert await db.get_mcp_oauth_grant_by_refresh_hash(_h("srwr_r1")) is None
    assert await db.revoke_mcp_oauth_grant_on_replay(_h("srwr_r1")) is True

    assert await db.get_mcp_oauth_grant_by_refresh_hash(_h("srwr_r2")) is None
    assert await db.get_mcp_token_by_hash(_h("srw_a2")) is None


@pytest.mark.asyncio
async def test_revoking_the_access_row_in_the_token_list_ends_the_connection(db):
    user_id = await _user(db)
    created = await _connect(db, user_id)
    assert await db.revoke_mcp_token(created["access_token_id"], str(user_id))

    assert await db.get_mcp_oauth_grant_by_refresh_hash(_h("srwr_r1")) is None
    assert (
        await db.rotate_mcp_oauth_grant(
            refresh_token_hash=_h("srwr_r1"),
            new_refresh_token_hash=_h("srwr_r2"),
            new_access_token_hash=_h("srw_a2"),
            new_access_token_prefix="srw_a2",
            access_ttl_seconds=3600,
        )
        is None
    )
    # The rotation did not un-revoke the row.
    assert await db.get_mcp_token_by_hash(_h("srw_a2")) is None


@pytest.mark.asyncio
async def test_an_expired_grant_cannot_refresh(db):
    user_id = await _user(db)
    await _connect(db, user_id)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE mcp_oauth_grants SET expires_at = now() - interval '1 minute'"
        )

    assert await db.get_mcp_oauth_grant_by_refresh_hash(_h("srwr_r1")) is None


@pytest.mark.asyncio
async def test_revoke_by_refresh_token_ends_both(db):
    user_id = await _user(db)
    await _connect(db, user_id)

    assert await db.revoke_mcp_oauth_grant(_h("srwr_r1")) is True
    assert await db.revoke_mcp_oauth_grant(_h("srwr_r1")) is False
    assert await db.get_mcp_token_by_hash(_h("srw_a1")) is None


@pytest.mark.asyncio
async def test_the_migration_revokes_the_old_365_day_refresh_rows(
    pg_dsn, _schema_applied
):
    """0563's UPDATE, replayed on a row shaped like the old bridge minted."""
    migration = (
        SCHEMA_FILE.parent / "migrations" / "app" / "0563_mcp_oauth_grants.sql"
    ).read_text()
    update = migration[migration.index("UPDATE public.auth_tokens") :].split(";")[0]
    conn = await asyncpg.connect(pg_dsn)
    try:
        user_id = uuid4()
        await conn.execute(
            "INSERT INTO users (id, display_name, email) VALUES ($1, 'old', $2)",
            user_id,
            f"{user_id}@example.test",
        )
        for origin in ("oauth:Claude:refresh", "oauth:Claude", None):
            await conn.execute(
                "INSERT INTO auth_tokens (user_id, name, token_hash, token_prefix, kind, "
                "scope, origin) VALUES ($1, 'x', $2, 'srw_x', 'mcp', 'user', $3)",
                user_id,
                _h(f"{origin}"),
                origin,
            )
        await conn.execute(update)
        revoked = await conn.fetch(
            "SELECT origin FROM auth_tokens WHERE user_id = $1 AND revoked_at IS NOT NULL",
            user_id,
        )
    finally:
        await conn.close()

    assert [r["origin"] for r in revoked] == ["oauth:Claude:refresh"]
