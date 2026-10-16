"""Stateless → pinned VM upgrade (C2) against the production schema snapshot.

All C2 real-PG cases live here (C1's rule): the reliability stream's suites
stay unedited and run as regression neighbours. Fixtures are C1's.
"""

from __future__ import annotations

import json  # noqa: F401 -- later C2 tasks append cases that use it
from uuid import uuid4

import pytest

from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from orchestrator.services.vm_upgrade_seed import mark_vm_upgrade_seeded
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
