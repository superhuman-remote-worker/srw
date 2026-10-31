"""A trigger on a replica that is not the leader reaches the leader's loop.

Two ``PostgresDB`` pools stand in for two orchestrator replicas on one
database: the follower's :func:`trigger_dispatch` NOTIFYs, and the leader's
:func:`auto_assign_dispatcher` hears it on its LISTEN and schedules a pass
long before its 30 s catch-all pass would have.
"""

from __future__ import annotations

import asyncio
import dataclasses
import time

import pytest
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services import job_dispatcher, leader_election
from tests import test_b11_job_dispatcher as dispatch_tests


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


async def _listening(store: PostgresDB) -> bool:
    for _ in range(100):
        async with store.acquire() as conn:
            if await conn.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE query ILIKE $1",
                f"LISTEN %{job_dispatcher.DISPATCH_CHANNEL}%",
            ):
                return True
        await asyncio.sleep(0.05)
    return False


@pytest.mark.asyncio
async def test_a_follower_trigger_wakes_the_leader_loop(pg_dsn, monkeypatch):
    leader_db = PostgresDB(
        connection_string=pg_dsn, min_connections=1, max_connections=2
    )
    follower_db = PostgresDB(
        connection_string=pg_dsn, min_connections=1, max_connections=2
    )
    await leader_db.connect()
    await follower_db.connect()
    # This process plays the follower for trigger_dispatch; the leader loop
    # itself never reads the flag (its pass request is recorded below).
    monkeypatch.setattr(leader_election, "is_leader", asyncio.Event())
    shutdown = asyncio.Event()
    requested: list[float] = []

    async def catch_all(*, dependencies):
        return None

    def request(dependencies, *, discover):
        assert discover is True
        requested.append(time.monotonic())
        shutdown.set()

    monkeypatch.setattr(job_dispatcher, "dispatch_pending_jobs", catch_all)
    monkeypatch.setattr(job_dispatcher, "_request_dispatch", request)

    def replica(store: PostgresDB) -> job_dispatcher.JobDispatchDependencies:
        return dataclasses.replace(
            dispatch_tests._deps(dispatch_tests.FakeStore()),
            request_leader_dispatch=lambda: job_dispatcher.request_leader_dispatch(
                store
            ),
            listen_for_dispatch_requests=lambda wake, stop: (
                job_dispatcher.listen_for_dispatch_requests(store, wake, stop)
            ),
        )

    leader = asyncio.create_task(
        job_dispatcher.auto_assign_dispatcher(shutdown, dependencies=replica(leader_db))
    )
    try:
        assert await _listening(leader_db)
        follower = replica(follower_db)
        triggered = time.monotonic()

        job_dispatcher.trigger_dispatch(dependencies=follower)

        await asyncio.wait_for(leader, 5.0)
        assert len(requested) == 1
        assert requested[0] - triggered < 2.0
        await asyncio.gather(*follower.state.tasks)
    finally:
        shutdown.set()
        if not leader.done():
            leader.cancel()
            await asyncio.gather(leader, return_exceptions=True)
        await follower_db.close()
        await leader_db.close()
