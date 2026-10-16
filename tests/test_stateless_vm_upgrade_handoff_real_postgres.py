"""Stateless → pinned VM upgrade (C2) against the production schema snapshot.

All C2 real-PG cases live here (C1's rule): the reliability stream's suites
stay unedited and run as regression neighbours. Fixtures are C1's.
"""

from __future__ import annotations

import json  # noqa: F401 -- later C2 tasks append cases that use it
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from orchestrator.services.vm_upgrade_seed import (
    PINNED_IDENTITY_MISMATCH,
    agent_vm_upgrade_seed_source,
    mark_vm_upgrade_seeded,
)
from tests import test_stateless_workspace_upgrade_real_postgres as c1
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent

actor = c1.actor
database = c1.database
postgres_url = c1.postgres_url
metadata = c1.metadata
lite_session = c1.lite_session


async def pinned_virtual_session(database, actor):
    workspace, selection = await select_execution_workspace(
        database,
        actor,
        role="session",
        project_id=None,
        supplied=True,
        workspace={"template": {"inline": {"backend": "virtual"}}},
    )
    return await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        execution_lane="pinned",
        initial_metadata={"config_override": {"workspace": workspace}},
        workspace_selection=selection,
    )


async def bound_with_pending_marker(database, actor):
    thread_id = await pinned_virtual_session(database, actor)
    bound = await _bind_cold_agent(database, thread_id)
    generation, request_id = str(uuid4()), str(uuid4())
    assert await database.merge_thread_vm_context(
        str(thread_id),
        {
            "status": "ready",
            "provision_generation": generation,
            "upgrade_seed": {
                "control_request_id": request_id,
                "state": "pending",
                "source": None,
            },
        },
    )
    return thread_id, str(bound["agent_id"]), generation, request_id


@pytest.mark.asyncio
async def test_seeded_marks_the_exact_provision_once_and_idempotently(database, actor):
    thread_id, agent_id, generation, request_id = await bound_with_pending_marker(
        database, actor
    )
    for _ in range(2):
        assert await mark_vm_upgrade_seeded(
            database,
            str(thread_id),
            agent_id=agent_id,
            provision_generation=generation,
            control_request_id=request_id,
        )
    vm = metadata(await database.get_thread(thread_id))["vm"]
    assert vm["upgrade_seed"]["state"] == "seeded"
    assert vm["provision_generation"] == generation


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong", ["generation", "request", "agent"])
async def test_seeded_refuses_a_stale_provision_generation(database, actor, wrong):
    """Review Focus 3: a stale agent can't mark a newer VM as seeded."""
    thread_id, agent_id, generation, request_id = await bound_with_pending_marker(
        database, actor
    )
    kwargs = {
        "agent_id": agent_id,
        "provision_generation": generation,
        "control_request_id": request_id,
    }
    kwargs[
        {
            "generation": "provision_generation",
            "request": "control_request_id",
            "agent": "agent_id",
        }[wrong]
    ] = str(uuid4())
    assert not await mark_vm_upgrade_seeded(database, str(thread_id), **kwargs)
    vm = metadata(await database.get_thread(thread_id))["vm"]
    assert vm["upgrade_seed"]["state"] == "pending"


async def seed_source(database, thread):
    """``seed-source`` as the bound runtime presents itself."""
    request = SimpleNamespace(
        headers={
            "X-Agent-ID": str(thread["agent_id"]),
            "X-Session-Runtime-Generation": str(thread["runtime_generation"]),
            "X-Session-Runtime-Attach-Token": str(thread["runtime_attach_token"]),
        }
    )
    return await agent_vm_upgrade_seed_source(
        request,
        str(thread["id"]),
        dependencies=SimpleNamespace(require_internal=AsyncMock(), store=database),
    )


class _RollBack(Exception):
    """Leaves the half-unbound agent uncommitted: its deferred fence refuses it."""


@pytest.mark.asyncio
async def test_seed_source_refuses_an_agent_that_no_longer_points_at_the_session(
    database, actor
):
    thread_id, agent_id, generation, request_id = await bound_with_pending_marker(
        database, actor
    )
    await database.execute(
        "INSERT INTO thread_control_requests "
        "(id,thread_id,request_seq,client_request_id,verb,requested_by) "
        "VALUES ($1::uuid,$2::uuid,1,$3::uuid,'upgrade-to-workspace','owner')",
        request_id,
        str(thread_id),
        str(uuid4()),
    )
    thread = await database.get_thread(thread_id)
    served = await seed_source(database, thread)
    assert served["seed_from"] is None
    assert served["control_request_id"] == request_id
    assert served["provision_generation"] == generation
    with pytest.raises(_RollBack):
        async with database.transaction_scope():
            await database.execute(
                "UPDATE agents SET thread_id = NULL WHERE id = $1::uuid", agent_id
            )
            with pytest.raises(HTTPException) as refused:
                await seed_source(database, thread)
            assert refused.value.status_code == 409
            assert refused.value.detail == PINNED_IDENTITY_MISMATCH
            raise _RollBack
