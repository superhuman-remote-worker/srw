"""Real-Postgres proofs for start refusals under completion commands.

Connector drivers decision 34: while completion commands own job status, a
refused job start is admitted as the claim's terminal report under the
claim's own fence. These tests run the real admission, finalizer claim,
status CAS, lease-recovery sweep and worker claim against PostgreSQL's lock
manager: the job ends ``failed`` with the refusal's message exactly once, is
never claimed again, and a cancel or control that won the row keeps it.

The finalizer workflow here performs only the completion body's status write
(``determine_job_status`` and the command-fenced ``update_job_status``); the
whole body over a refusal is covered in
``tests/test_job_completion_endpoint_wrapper.py``.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from orchestrator.database.postgres import PostgresDB
from orchestrator.services import job_completion
from orchestrator.services.completion import determine_job_status
from orchestrator.services.completion_control import CompletionControl
from orchestrator.services.completion_finalizer import (
    CompletionDispositionSuperseded,
    CompletionFinalizer,
)
from orchestrator.services.job_completion_commands import accept_completion_command
from shared.worker_queue import claim_worker_batch
from tests.test_job_completion_accept_real_postgres import (  # noqa: F401
    _schema_applied,
    pg,
    pg_dsn,
)

MESSAGE = (
    "Pinned model(s) have no resolvable endpoint or provider after dispatch "
    "resolution: llm.model."
)


def _db(pool) -> PostgresDB:
    database = PostgresDB.__new__(PostgresDB)
    database._pool = pool
    return database


def _dependencies(pool) -> job_completion.JobCompletionDependencies:
    return job_completion.JobCompletionDependencies(
        store=pool,
        require_internal=AsyncMock(side_effect=AssertionError("server-side call")),
        commands_enabled=lambda: True,
        status_reorder_enabled=lambda: False,
        inline_delay_seconds=lambda: 0.0,
        accept_command=accept_completion_command,
        finalizer=MagicMock(side_effect=AssertionError("admission only")),
        legacy_complete=AsyncMock(side_effect=AssertionError("admission only")),
        logger=logging.getLogger(__name__),
    )


async def _refuse(pool, job_id, **fence) -> bool:
    return await job_completion.refuse_job_start(
        str(job_id),
        reason="unrouted_model",
        message=MESSAGE,
        **fence,
        dependencies=_dependencies(pool),
    )


async def _claimed_pinned_job(pool) -> tuple[UUID, UUID]:
    """A pinned job as the dispatcher's claim leaves it."""

    async with pool.acquire() as conn:
        agent_id = await conn.fetchval(
            "INSERT INTO agents (config_name, hostname, status) "
            "VALUES ('worker_base', $1, 'ready') RETURNING id",
            f"refusal-{uuid4().hex[:10]}",
        )
        job_id = await conn.fetchval(
            "INSERT INTO jobs (description, status, execution_lane, "
            "assigned_agent_id, lease_expires_at) "
            "VALUES ('refused start', 'processing', 'pinned', $1, "
            "now() + interval '4 minutes') RETURNING id",
            agent_id,
        )
    return job_id, agent_id


async def _leased_stateless_job(pool, *, lease_token: int = 7) -> UUID:
    async with pool.acquire() as conn:
        job_id = await conn.fetchval(
            "INSERT INTO jobs (description, status, execution_lane) "
            "VALUES ('refused worker start', 'processing', 'stateless') "
            "RETURNING id"
        )
        await conn.execute(
            """
            INSERT INTO run_queue (
                unit_id, unit_kind, state, attempts_since_completion,
                lease_token, leased_by, last_leased_by, leased_until,
                input_seq, consumed_seq
            ) VALUES (
                $1, 'worker_batch', 'leased', 1,
                $2, 'refusal-pod', 'refusal-pod',
                now() + interval '5 minutes', 4, 3
            )
            """,
            job_id,
            lease_token,
        )
    return job_id


async def _commands(pool, job_id) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, state, payload, accepted_agent_id, accepted_lease_token, "
            "accepted_job_status, requested_by FROM job_completion_commands "
            "WHERE job_id=$1 ORDER BY report_seq",
            job_id,
        )
    return [dict(row) for row in rows]


async def _job(pool, job_id) -> dict:
    async with pool.acquire() as conn:
        return dict(
            await conn.fetchrow(
                "SELECT status::text AS status, error_message, assigned_agent_id, "
                "failed_at FROM jobs WHERE id=$1",
                job_id,
            )
        )


async def _finalize(pool, command_id):
    """Drive the command through the real finalizer, writing what the
    completion body's main status write would."""

    database = _db(pool)
    calls = []

    async def workflow(runner):
        calls.append(runner.command_id)
        job = await database.get_job(str(runner.command["job_id"]))
        status, error_message = determine_job_status(job, runner.command["payload"])
        written = await database.update_job_status(
            str(runner.command["job_id"]),
            status=status,
            error_message=error_message,
            expected_status=runner.command["resolved_entry_status"],
            completion_command_id=runner.command_id,
            completion_finalizing_by=runner.owner,
        )
        if not written:
            current = await database.get_job(str(runner.command["job_id"]))
            raise CompletionDispositionSuperseded(
                observed_status=str(current["status"]),
                expected_statuses=(runner.command["resolved_entry_status"],),
            )
        return {"status": "handled", "new_status": status}

    result = await CompletionFinalizer(pool, workflow=workflow).finalize_command(
        str(command_id)
    )
    return result, calls


@pytest.mark.asyncio
async def test_a_refused_pinned_start_fails_once_and_is_never_claimed_again(pg):  # noqa: F811
    job_id, agent_id = await _claimed_pinned_job(pg)
    database = _db(pg)

    assert await _refuse(pg, job_id, agent_id=str(agent_id)) is True

    [command] = await _commands(pg, job_id)
    assert command["state"] == "pending"
    assert command["accepted_agent_id"] == agent_id
    assert command["accepted_lease_token"] is None
    assert command["accepted_job_status"] == "processing"
    assert command["requested_by"] == f"start-refusal:agent:{agent_id}"
    assert json.loads(command["payload"])["error"]["message"] == MESSAGE

    # Not dispatchable while the command is pending: no other agent claims it.
    other_agent = uuid4()
    assert not await database.claim_job_for_agent(
        str(job_id), str(other_agent), completion_commands_enabled=True
    )
    # Its claim lease expiring does not re-queue it either: the pending
    # command excludes it from lease recovery.
    async with pg.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET lease_expires_at = now() - interval '1 second' "
            "WHERE id=$1",
            job_id,
        )
    batch = await database.recover_expired_lease_jobs(completion_commands_enabled=True)
    assert str(job_id) not in batch.recovered_job_ids
    assert (await _job(pg, job_id))["status"] == "processing"

    result, calls = await _finalize(pg, command["id"])

    assert result.disposition == "done"
    assert calls == [str(command["id"])]
    job = await _job(pg, job_id)
    assert job["status"] == "failed"
    assert job["error_message"] == MESSAGE
    assert job["failed_at"] is not None
    # Failed for good: nothing recovers or claims it afterwards.
    batch = await database.recover_expired_lease_jobs(completion_commands_enabled=True)
    assert str(job_id) not in batch.recovered_job_ids
    assert not await database.claim_job_for_agent(
        str(job_id), str(other_agent), completion_commands_enabled=True
    )
    assert (await _job(pg, job_id))["status"] == "failed"


@pytest.mark.asyncio
async def test_a_cancel_before_the_refusal_keeps_the_pinned_job(pg):  # noqa: F811
    job_id, agent_id = await _claimed_pinned_job(pg)
    assert await _db(pg).linearize_pinned_cancel(
        str(job_id), expected_status="processing", completion_commands_enabled=True
    )

    assert await _refuse(pg, job_id, agent_id=str(agent_id)) is False

    assert await _commands(pg, job_id) == []
    assert (await _job(pg, job_id))["status"] == "cancelled"


@pytest.mark.asyncio
async def test_a_control_claim_in_flight_keeps_the_pinned_job(pg):  # noqa: F811
    job_id, agent_id = await _claimed_pinned_job(pg)
    control = CompletionControl(
        _db(pg), SimpleNamespace(enqueue_job=AsyncMock(return_value=None))
    )
    await control.claim_job(
        job_id,
        source="public_cancel",
        expected_status="processing",
        expected_lane="pinned",
    )

    assert await _refuse(pg, job_id, agent_id=str(agent_id)) is False

    assert await _commands(pg, job_id) == []
    assert (await _job(pg, job_id))["status"] == "processing"


@pytest.mark.asyncio
async def test_a_cancel_after_the_refusal_still_wins(pg):  # noqa: F811
    job_id, agent_id = await _claimed_pinned_job(pg)
    assert await _refuse(pg, job_id, agent_id=str(agent_id)) is True
    [command] = await _commands(pg, job_id)

    # The pending command does not hold the cancel off.
    assert await _db(pg).linearize_pinned_cancel(
        str(job_id), expected_status="processing", completion_commands_enabled=True
    )
    result, calls = await _finalize(pg, command["id"])

    assert result.disposition == "superseded"
    assert calls == []
    job = await _job(pg, job_id)
    assert job["status"] == "cancelled"
    assert job["error_message"] is None


@pytest.mark.asyncio
async def test_a_refused_worker_start_fails_and_closes_its_unit(pg):  # noqa: F811
    job_id = await _leased_stateless_job(pg, lease_token=7)

    assert await _refuse(pg, job_id, lease_token=7) is True

    [command] = await _commands(pg, job_id)
    assert command["accepted_lease_token"] == 7
    assert command["accepted_agent_id"] is None
    assert command["requested_by"] == "start-refusal:worker-lease:7"
    async with pg.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state, leased_by, consumed_seq FROM run_queue WHERE unit_id=$1",
            job_id,
        )
    assert dict(queue) == {"state": "done", "leased_by": None, "consumed_seq": 4}
    # No worker claims it again.
    assert (
        await claim_worker_batch(
            pg, pod_name="successor-pod", completion_commands_enabled=True
        )
        is None
    )

    result, _calls = await _finalize(pg, command["id"])

    assert result.disposition == "done"
    job = await _job(pg, job_id)
    assert job["status"] == "failed"
    assert job["error_message"] == MESSAGE


@pytest.mark.asyncio
async def test_a_stale_worker_claim_cannot_fail_the_job(pg):  # noqa: F811
    job_id = await _leased_stateless_job(pg, lease_token=8)

    # The claim-bundle request outlived lease 7; lease 8 owns the job now.
    assert await _refuse(pg, job_id, lease_token=7) is False

    assert await _commands(pg, job_id) == []
    async with pg.acquire() as conn:
        queue = await conn.fetchrow(
            "SELECT state, lease_token FROM run_queue WHERE unit_id=$1", job_id
        )
    assert dict(queue) == {"state": "leased", "lease_token": 8}
    assert (await _job(pg, job_id))["status"] == "processing"


@pytest.mark.asyncio
async def test_a_worker_cancel_before_the_refusal_keeps_the_job(pg):  # noqa: F811
    job_id = await _leased_stateless_job(pg, lease_token=7)
    cancelled, queue_closed = await _db(pg).cancel_stateless_job(
        str(job_id), completion_commands_enabled=True
    )
    assert cancelled is True
    # A leased unit stays with its holder until it observes the cancel.
    assert queue_closed is False

    assert await _refuse(pg, job_id, lease_token=7) is False

    assert await _commands(pg, job_id) == []
    assert (await _job(pg, job_id))["status"] == "cancelled"
