"""A Session's captured container settings survive unrelated patches."""

import json

from fastapi import HTTPException
import pytest

from orchestrator.services.manifest_execution_snapshot import (
    prepare_srw_session_patch,
    read_execution,
    srw_snapshot_config,
)
from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from tests import test_manifest_native_full_schema as full_schema

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url

IMAGE = "registry.example/team/workspace@sha256:" + "c" * 64
SPEC = {
    "backend": "sandbox",
    "resources": {"cpu": 1, "memory": "3Gi", "storage": "15Gi"},
    "environment": {"image": IMAGE, "pullPolicy": "IfNotPresent"},
}
SANDBOX = {
    "image": IMAGE,
    "pull_policy": "IfNotPresent",
    "cpu": 1,
    "memory": "3Gi",
    "storage": "15Gi",
}


async def sandbox_session(database, actor):
    workspace, receipt = await select_execution_workspace(
        database,
        actor,
        role="session",
        project_id=None,
        supplied=True,
        workspace={"template": {"inline": SPEC}},
    )
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        initial_metadata={"config_override": {"workspace": workspace}},
        workspace_selection=receipt,
    )
    return thread_id


@pytest.mark.asyncio
async def test_session_snapshot_keeps_container_settings_and_refuses_changes(
    database, actor
):
    thread_id = await sandbox_session(database, actor)
    current = await read_execution(database, "Session", thread_id)
    _, policy = srw_snapshot_config(current)
    assert policy["workspace"]["sandbox"] == SANDBOX
    thread = await database.get_thread(thread_id)
    metadata = json.loads(thread["metadata"])
    prepared, _ = await prepare_srw_session_patch(
        database, current, thread, metadata, [], {"llm": {"temperature": 0.2}}
    )
    _, patched = srw_snapshot_config(prepared)
    assert patched["workspace"]["sandbox"] == SANDBOX
    with pytest.raises(HTTPException) as denied:
        await prepare_srw_session_patch(
            database,
            current,
            thread,
            metadata,
            [],
            {"workspace": {"sandbox": {"image": "registry.example/other:v2"}}},
        )
    assert denied.value.status_code == 422
    assert "container image or resources" in denied.value.detail
    assert await read_execution(database, "Session", thread_id) == current


@pytest.mark.asyncio
async def test_resolver_reads_the_captured_session_settings(database, actor):
    from orchestrator.services.sandbox_workspace_settings import (
        SandboxSettings,
        resolve_sandbox_settings,
    )

    thread_id = await sandbox_session(database, actor)
    assert await resolve_sandbox_settings(database, "session", thread_id) == (
        SandboxSettings(
            image=IMAGE,
            pull_policy="IfNotPresent",
            cpu=1,
            memory="3Gi",
            storage="15Gi",
        )
    )
