"""Stateless workspace upgrade (C1) against the production schema snapshot.

The transition is one UPDATE on a lite stateless row; every production trigger
stays enabled. The create path is the real one, with only Kubernetes faked.
"""

from __future__ import annotations

import json
import os
from uuid import uuid4

import pytest

from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from orchestrator.services.stateless_workspace_gate import stateless_workspace_check
from shared.workspace_defaults import ENV_NAME as WORKSPACE_DEFAULTS
from tests import test_stateless_workspace_continuation_real_postgres as continuation

actor = continuation.actor
database = continuation.database
postgres_url = continuation.postgres_url
IMAGE = continuation.IMAGE
VIRTUAL_BACKING = "rclone:" + "a" * 64


def metadata(thread):
    value = thread["metadata"]
    return json.loads(value) if isinstance(value, str) else dict(value)


async def _none_session_workspace(database, actor):
    # The manifest WorkspaceSpec has no "none" backend, so a None Session
    # comes from the defaults chain, as on an installation that defaults
    # Sessions to none.
    previous = os.environ.get(WORKSPACE_DEFAULTS)
    os.environ[WORKSPACE_DEFAULTS] = json.dumps({"sessions": "none"})
    try:
        workspace, selection = await select_execution_workspace(
            database, actor, role="session", project_id=None
        )
    finally:
        if previous is None:
            os.environ.pop(WORKSPACE_DEFAULTS, None)
        else:
            os.environ[WORKSPACE_DEFAULTS] = previous
    assert workspace == {"backend": "none"}, workspace
    return workspace, selection


async def lite_session(database, actor, *, backend="virtual", bind=True):
    if backend == "none":
        workspace, selection = await _none_session_workspace(database, actor)
    else:
        workspace, selection = await select_execution_workspace(
            database,
            actor,
            role="session",
            project_id=None,
            supplied=True,
            workspace={"template": {"inline": {"backend": backend}}},
        )
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        execution_lane="stateless",
        initial_metadata={"config_override": {"workspace": workspace}},
        workspace_selection=selection,
    )
    if bind and backend == "virtual":
        await database.bind_thread_workspace_backing(
            thread_id, backing_kind="virtual", backing_id=VIRTUAL_BACKING
        )
    return thread_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shape", ["virtual_bound", "virtual_unbound", "none", "empty_container"]
)
async def test_the_guards_admit_the_upgrade_envelope(database, actor, shape):
    """Probe B (spec "One transition"): with the runtime generation kept, the
    0198/0223 guards (as patched by 0332) admit the container envelope."""
    backend = "none" if shape == "none" else "virtual"
    thread_id = await lite_session(
        database, actor, backend=backend, bind=shape == "virtual_bound"
    )
    if shape == "empty_container":
        await database.execute(
            "UPDATE threads SET metadata = jsonb_set(metadata, "
            "'{workspace_container}', '{}'::jsonb) WHERE id = $1::uuid",
            thread_id,
        )
    row = await database.get_thread(thread_id)
    generation = str(row["runtime_generation"])
    next_metadata = metadata(row)
    binding = next_metadata.pop("_workspace_binding", None)
    next_metadata["config_override"]["workspace"]["backend"] = "sandbox"
    next_metadata["workspace_container"] = {
        "status": "pending",
        "provisioner": "k8s",
        "_runtime_creation": {
            "generation": generation,
            "mode": "create",
            "attempted": False,
            "replaces_uid": None,
        },
        "upgrade_config": {"image": IMAGE},
        "upgrade_sources": None,
        "seed_from": {
            "backend": backend,
            "prefix": f"threads/{thread_id}/",
            "binding": binding,
        },
    }

    await database.execute(
        "UPDATE threads SET metadata = $2::jsonb WHERE id = $1::uuid",
        thread_id,
        json.dumps(next_metadata),
    )

    after = await database.get_thread(thread_id)
    assert str(after["runtime_generation"]) == generation
    assert stateless_workspace_check(after) == ("sandbox", None)


from orchestrator.database.stateless_workspace_upgrade import (  # noqa: E402
    UPGRADE_NOT_POSSIBLE,
    StatelessUpgradeRefused,
    begin_stateless_session_workspace_upgrade,
)

UPGRADE_CONFIG = {"image": IMAGE, "cpu": 1, "memory": "3Gi", "storage": "15Gi"}
UPGRADE_SOURCES = {
    "tier": "explicit",
    "template": "explicit",
    "template_name": "container-minimal",
}


async def transition(database, thread_id, **row_overrides):
    async with database.acquire() as conn:
        async with conn.transaction():
            row = dict(
                await conn.fetchrow(
                    "SELECT id, status, execution_lane, metadata, runtime_generation "
                    "FROM threads WHERE id = $1::uuid FOR UPDATE",
                    thread_id,
                )
            )
            row.update(row_overrides)
            return await begin_stateless_session_workspace_upgrade(
                conn,
                row,
                upgrade_config=UPGRADE_CONFIG,
                upgrade_sources=UPGRADE_SOURCES,
            )


@pytest.mark.asyncio
async def test_transition_turns_a_lite_session_into_a_pending_container_session(
    database, actor
):
    thread_id = await lite_session(database, actor)
    before = await database.get_thread(thread_id)
    binding = metadata(before)["_workspace_binding"]

    await transition(database, thread_id)

    after = await database.get_thread(thread_id)
    meta = metadata(after)
    generation = str(before["runtime_generation"])
    assert str(after["runtime_generation"]) == generation
    assert meta["config_override"]["workspace"]["backend"] == "sandbox"
    assert "_workspace_binding" not in meta
    assert meta["workspace_container"] == {
        "status": "pending",
        "provisioner": "k8s",
        "_runtime_creation": {
            "generation": generation,
            "mode": "create",
            "attempted": False,
            "replaces_uid": None,
        },
        "upgrade_config": UPGRADE_CONFIG,
        "upgrade_sources": UPGRADE_SOURCES,
        "seed_from": {
            "backend": "virtual",
            "prefix": f"threads/{thread_id}/",
            "binding": binding,
        },
    }
    assert stateless_workspace_check(after) == ("sandbox", None)


@pytest.mark.asyncio
async def test_a_none_session_records_a_none_source(database, actor):
    thread_id = await lite_session(database, actor, backend="none")
    await transition(database, thread_id)
    seed = metadata(await database.get_thread(thread_id))["workspace_container"][
        "seed_from"
    ]
    assert seed == {
        "backend": "none",
        "prefix": f"threads/{thread_id}/",
        "binding": None,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "sandbox_backend",
        "vm_context",
        "stop_marker",
        "foreign_container_keys",
        "stale_generation",
    ],
)
async def test_transition_refuses_rows_it_cannot_upgrade(database, actor, change):
    thread_id = await lite_session(database, actor)
    overrides = {}
    if change == "stale_generation":
        overrides["runtime_generation"] = str(uuid4())
    else:
        patch = {
            "sandbox_backend": (
                "jsonb_set(metadata, '{config_override,workspace,backend}', "
                "'\"sandbox\"'::jsonb)"
            ),
            "vm_context": 'metadata || \'{"vm": {"status": "failed"}}\'::jsonb',
            "stop_marker": (
                "metadata || '{\"_stateless_claim_retirement\": {}}'::jsonb"
            ),
            "foreign_container_keys": (
                "jsonb_set(metadata, '{workspace_container}', "
                '\'{"repo_name": "thread-x"}\'::jsonb)'
            ),
        }[change]
        overrides["metadata"] = await database.fetchval(
            f"SELECT {patch} FROM threads WHERE id = $1::uuid", thread_id
        )
    before = await database.get_thread(thread_id)

    with pytest.raises(StatelessUpgradeRefused) as refused:
        await transition(database, thread_id, **overrides)

    assert str(refused.value) == UPGRADE_NOT_POSSIBLE
    assert await database.get_thread(thread_id) == before
