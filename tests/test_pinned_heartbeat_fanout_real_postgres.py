"""The pinned fan-out switch rides the heartbeat (parallel_subagents.md §14.2 P5).

A pinned runtime claims its inputs straight from Postgres, so before P5 it
learned the operator's switch (``SESSION_SUBAGENT_FANOUT_LANES``) only at
attach, and turning the lane off never reached a running session. The
heartbeat already resolves the agent's bound thread; it now carries the same
two keys as the pinned attach body for a pinned thread and neither for
anything else. Real Postgres with the full migration chain, a real pinned
binding (``bind_registered_persistent_agent``) and the real heartbeat path
(``PostgresDB.heartbeat`` under ``agent_heartbeat``).
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import asyncpg
import orchestrator
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.application.settings import parse_session_subagent_fanout_lanes
from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB
from orchestrator.schemas.agent_runtime import AgentHeartbeat
from orchestrator.services.agent_registration import (
    AgentRegistrationDependencies,
    agent_heartbeat,
)
from orchestrator.services.session_attach_binding import (
    bind_registered_persistent_agent,
)
from orchestrator.services.session_runtime_identity import (
    thread_accepts_runtime,
    thread_uses_pinned_execution,
)
from shared.session_subagent_batch import (
    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY,
    SESSION_SUBAGENT_FANOUT_KEY,
)

ROOT = Path(orchestrator.__file__).resolve().parents[2]
MIGRATIONS = ROOT / "src/orchestrator/database/migrations/app"

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def pg_dsn():
    with PostgresContainer("postgres:16") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture(scope="module")
async def migrated_dsn(pg_dsn):
    async with asyncpg.create_pool(pg_dsn, min_size=1, max_size=2) as pool:
        await run_migrations(pool, MIGRATIONS)
    return pg_dsn


@pytest_asyncio.fixture
async def db(migrated_dsn):
    store = PostgresDB(migrated_dsn, min_connections=1, max_connections=4)
    await store.connect()
    try:
        yield store
    finally:
        await store.close()


async def _owner(db: PostgresDB) -> UUID:
    owner = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name) VALUES ($1, 'owner')", owner
        )
    return owner


async def _pinned_life(db: PostgresDB) -> SimpleNamespace:
    """One pinned session bound to a registered agent (generation, token)."""

    owner, thread_id, agent_id = await _owner(db), uuid4(), uuid4()
    pod_uid, pod_name, attempt = f"pod-{uuid4()}", f"pinned-{uuid4()}", str(uuid4())
    async with db.acquire() as conn:
        generation = str(
            await conn.fetchval(
                "INSERT INTO threads (id, user_id, title, status) "
                "VALUES ($1, $2, 'P5 switch', 'active') RETURNING runtime_generation",
                thread_id,
                owner,
            )
        )
    assert await db.reserve_pinned_agent_pod_provision_intent(
        str(thread_id),
        expected_runtime_generation=generation,
        attempt_id=attempt,
        pod_name=pod_name,
        provisioner="agent",
        namespace="test",
    )
    assert await db.publish_pinned_agent_pod_provision_intent(
        str(thread_id),
        expected_runtime_generation=generation,
        attempt_id=attempt,
        pod_name=pod_name,
        pod_uid=pod_uid,
        namespace="test",
    )
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO agents (id, config_name, status, metadata, hostname, pod_uid) "
            "VALUES ($1, 'session_base', 'session', '{}'::jsonb, $2, $3)",
            agent_id,
            pod_name,
            pod_uid,
        )
    attach_token = await bind_registered_persistent_agent(
        str(thread_id),
        str(agent_id),
        None,
        generation,
        dependencies=SimpleNamespace(store=db),
    )
    assert attach_token
    return SimpleNamespace(
        thread_id=str(thread_id),
        agent_id=str(agent_id),
        generation=generation,
        attach_token=str(attach_token),
    )


def _deps(db: PostgresDB, lanes: str) -> AgentRegistrationDependencies:
    """The heartbeat's real collaborators where they decide the payload: the
    lane predicate and the per-lane switch the application binds."""

    enabled = parse_session_subagent_fanout_lanes(lanes)
    return AgentRegistrationDependencies(
        store=db,
        gitea_client=MagicMock(),
        logger=MagicMock(),
        require_internal=AsyncMock(),
        require_admin=AsyncMock(),
        is_internal_call=MagicMock(return_value=True),
        log_security_event=AsyncMock(),
        completion_commands_enabled=lambda: False,
        require_pinned_status_identity=lambda: True,
        thread_uses_pinned_execution=thread_uses_pinned_execution,
        thread_accepts_runtime=thread_accepts_runtime,
        protected_cloud_delivery_state=AsyncMock(return_value=("ready", None)),
        bind_registered_persistent_agent=AsyncMock(),
        slide_thread_grant_on_liveness=AsyncMock(),
        trigger_dispatch=MagicMock(),
        session_subagent_fanout=lambda lane: lane in enabled,
    )


async def _beat(db, agent_id, lanes, **identity) -> dict:
    return await agent_heartbeat(
        MagicMock(),
        agent_id,
        AgentHeartbeat(status="session", **identity),
        dependencies=_deps(db, lanes),
    )


@pytest.mark.parametrize(
    ("lanes", "expected"),
    [("stateless,pinned", True), ("stateless", False), ("", False)],
)
async def test_a_live_pinned_session_heartbeat_carries_the_switch(db, lanes, expected):
    life = await _pinned_life(db)

    response = await _beat(
        db,
        life.agent_id,
        lanes,
        session_runtime_generation=life.generation,
        session_runtime_attach_token=life.attach_token,
    )

    assert response[SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY] == 1
    assert response[SESSION_SUBAGENT_FANOUT_KEY] is expected
    # The same JSON the agent receives.
    assert json.loads(json.dumps(response))[SESSION_SUBAGENT_FANOUT_KEY] is expected


async def test_turning_the_lane_off_reaches_the_next_heartbeat(db):
    """No attach in between: the same live binding sees the flip."""

    life = await _pinned_life(db)
    identity = {
        "session_runtime_generation": life.generation,
        "session_runtime_attach_token": life.attach_token,
    }

    on = await _beat(db, life.agent_id, "stateless,pinned", **identity)
    off = await _beat(db, life.agent_id, "stateless", **identity)

    assert on[SESSION_SUBAGENT_FANOUT_KEY] is True
    assert off[SESSION_SUBAGENT_FANOUT_KEY] is False


async def test_an_unbound_agent_heartbeat_carries_no_advertisement(db):
    agent_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO agents (id, config_name, status, metadata, hostname) "
            "VALUES ($1, 'session_base', 'ready', '{}'::jsonb, $2)",
            agent_id,
            f"pool-{agent_id}",
        )

    response = await _beat(db, str(agent_id), "stateless,pinned")

    assert response["status"] == "ok"
    assert SESSION_SUBAGENT_FANOUT_KEY not in response
    assert SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY not in response


async def test_a_stateless_thread_heartbeat_carries_no_advertisement(db):
    """A stateless session learns its switch from every claim bundle. Should
    an agent row ever name a stateless thread, its heartbeat stays silent."""

    owner, thread_id, agent_id = await _owner(db), uuid4(), uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, user_id, title, status, execution_lane) "
            "VALUES ($1, $2, 'stateless', 'active', 'stateless')",
            thread_id,
            owner,
        )
        await conn.execute(
            "INSERT INTO agents (id, config_name, status, metadata, hostname, "
            "thread_id) VALUES ($1, 'session_base', 'ready', '{}'::jsonb, $2, $3)",
            agent_id,
            f"stateless-{agent_id}",
            thread_id,
        )

    response = await _beat(db, str(agent_id), "stateless,pinned")

    assert response["status"] == "ok"
    assert SESSION_SUBAGENT_FANOUT_KEY not in response
    assert SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY not in response
