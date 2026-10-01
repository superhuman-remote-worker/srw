"""Real-schema coverage for virtual Session soft and permanent End."""

from dataclasses import fields
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest
from fastapi import HTTPException

from orchestrator.services import thread_uploads, workspace_binding
from orchestrator.services.session_class_policy import require_stateless_end_workspace
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.services.thread_retirement import (
    ThreadRetirementDependencies,
    ThreadRetirementOperations,
)
from tests import test_manifest_native_full_schema as full_schema

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url


async def _soft_ended_virtual(database, actor, monkeypatch, *, failure=None):
    spec = {
        "type": "s3",
        "root": "native-virtual-end-fixture",
        "config": {"endpoint": "http://127.0.0.1:1"},
    }
    current_spec = {"value": spec}
    monkeypatch.setattr(
        workspace_binding,
        "virtual_workspace_rclone_spec",
        lambda: current_spec["value"],
    )
    monkeypatch.setattr(workspace_binding.shutil, "which", lambda _: "/usr/bin/rclone")
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        execution_lane="stateless",
        initial_metadata={"config_override": {"workspace": {"backend": "virtual"}}},
    )
    assert (
        await workspace_binding.ensure_virtual_thread_workspace_binding(
            database, thread_id
        )
        is not None
    )
    await database.execute(
        "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token) "
        "VALUES($1::uuid,'session_turn','queued',0)",
        thread_id,
    )
    prefix = f"threads/{thread_id}/"
    keys = {prefix + "first", prefix + "second"}
    purge_targets = []

    def fake_external_purge(target):
        purge_targets.append(target.prefix)
        if target.prefix != prefix:
            return False
        if len(purge_targets) == 1 and failure == "partial":
            keys.remove(prefix + "first")
            return False
        keys.clear()
        return not (len(purge_targets) == 1 and failure == "lost_response")

    monkeypatch.setattr(thread_uploads, "_virtual_purge_prefix", fake_external_purge)
    values = {field.name: None for field in fields(ThreadRetirementDependencies)}
    values.update(
        store=database,
        container_provisioner=Mock(),
        pinned_retirement=Mock(),
        require_stateless_end_workspace=require_stateless_end_workspace,
        conclude_conference_if_any=AsyncMock(),
        snapshot_service=SimpleNamespace(is_available=False),
        logger=logging.getLogger(__name__),
    )
    operations = ThreadRetirementOperations(ThreadRetirementDependencies(**values))
    before = await database.get_thread(thread_id)
    assert await operations.end_thread_flow(
        thread_id, before, permanent=False, force=False
    ) == {"status": "ended"}
    ended = await database.get_thread(thread_id)
    metadata = thread_metadata_object(ended)
    assert ended["status"] == "ended"
    assert metadata["workspace_container"] == {"volume_reclaimed": False}
    settled = metadata["_stateless_workspace_retirement_settled"]
    assert settled["permanent"] is False
    assert settled["backing_id"] == workspace_binding.virtual_thread_backing_id(
        thread_id, spec
    )
    assert metadata["_workspace_binding"]["backing_id"] == settled["backing_id"]
    queue = await database.fetchrow(
        "SELECT state, lease_token FROM run_queue WHERE unit_id=$1::uuid", thread_id
    )
    assert queue is not None and dict(queue) == {"state": "done", "lease_token": 1}
    assert keys == {prefix + "first", prefix + "second"}
    return thread_id, ended, operations, current_spec, prefix, keys, purge_targets


@pytest.mark.asyncio
async def test_soft_ended_virtual_session_permanently_deletes(
    database, actor, monkeypatch
):
    thread_id, ended, operations, _, prefix, keys, targets = await _soft_ended_virtual(
        database, actor, monkeypatch
    )
    assert await operations.end_thread_flow(
        thread_id, ended, permanent=True, force=False
    ) == {"status": "deleted"}
    assert targets == [prefix] and keys == set()
    assert await database.get_thread(thread_id) is None
    assert (
        await database.fetchrow(
            "SELECT state FROM run_queue WHERE unit_id=$1::uuid", thread_id
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["partial", "lost_response"])
async def test_failed_virtual_purge_keeps_permanent_retry_intent(
    database, actor, monkeypatch, failure
):
    thread_id, ended, operations, _, prefix, keys, targets = await _soft_ended_virtual(
        database, actor, monkeypatch, failure=failure
    )
    with pytest.raises(HTTPException) as exc:
        await operations.end_thread_flow(thread_id, ended, permanent=True, force=False)
    assert exc.value.status_code == 503
    retained = await database.get_thread(thread_id)
    metadata = thread_metadata_object(retained)
    assert retained["status"] == "ended"
    assert metadata["_stateless_workspace_retirement_settled"]["permanent"] is True
    assert metadata["workspace_container"]["volume_reclaimed"] is False
    assert len(keys) == (1 if failure == "partial" else 0)
    assert (
        await database.fetchval(
            "SELECT lease_token FROM run_queue WHERE unit_id=$1::uuid", thread_id
        )
        == 1
    )
    assert await operations.end_thread_flow(
        thread_id, retained, permanent=True, force=False
    ) == {"status": "deleted"}
    assert targets == [prefix, prefix] and keys == set()
    assert await database.get_thread(thread_id) is None


@pytest.mark.asyncio
async def test_virtual_binding_spec_mismatch_keeps_bound_prefix(
    database, actor, monkeypatch
):
    (
        thread_id,
        ended,
        operations,
        current_spec,
        _,
        keys,
        targets,
    ) = await _soft_ended_virtual(database, actor, monkeypatch)
    current_spec["value"] = {
        "type": "s3",
        "root": "different-store",
        "config": {"endpoint": "http://127.0.0.1:1"},
    }
    with pytest.raises(HTTPException) as exc:
        await operations.end_thread_flow(thread_id, ended, permanent=True, force=False)
    assert exc.value.status_code == 503
    assert targets == [] and len(keys) == 2
    metadata = thread_metadata_object(await database.get_thread(thread_id))
    assert metadata["_stateless_workspace_retirement_settled"]["permanent"] is True
    assert metadata["workspace_container"]["volume_reclaimed"] is False


@pytest.mark.asyncio
async def test_stale_queue_refuses_permanent_virtual_end_before_purge(
    database, actor, monkeypatch
):
    thread_id, ended, operations, _, _, keys, targets = await _soft_ended_virtual(
        database, actor, monkeypatch
    )
    await database.execute(
        "UPDATE run_queue SET lease_token=lease_token+1 WHERE unit_id=$1::uuid",
        thread_id,
    )
    with pytest.raises(HTTPException) as exc:
        await operations.end_thread_flow(thread_id, ended, permanent=True, force=False)
    assert exc.value.status_code == 503
    assert targets == [] and len(keys) == 2
    metadata = thread_metadata_object(await database.get_thread(thread_id))
    assert metadata["_stateless_workspace_retirement_settled"]["permanent"] is False


@pytest.mark.asyncio
async def test_sql_delete_cannot_consume_virtual_intent_with_live_queue(
    database, actor, monkeypatch
):
    thread_id, _, _, _, _, _, _ = await _soft_ended_virtual(
        database, actor, monkeypatch
    )
    upgraded = await database.begin_stateless_thread_workspace_retirement(
        thread_id, permanent=True
    )
    assert upgraded["state"] == "settled" and upgraded["permanent"] is True
    with pytest.raises(asyncpg.CheckViolationError) as exc:
        await database.execute("DELETE FROM threads WHERE id=$1::uuid", thread_id)
    assert exc.value.sqlstate == "23514"
    assert await database.get_thread(thread_id) is not None


@pytest.mark.asyncio
async def test_schema_rejects_a_physical_stamp_on_settled_virtual_workspace(
    database, actor, monkeypatch
):
    thread_id, _, _, _, _, _, _ = await _soft_ended_virtual(
        database, actor, monkeypatch
    )
    with pytest.raises(asyncpg.CheckViolationError) as exc:
        await database.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata, "
            "'{workspace_container,provisioner}', '\"k8s\"'::jsonb) "
            "WHERE id=$1::uuid",
            thread_id,
        )
    assert exc.value.sqlstate == "23514"
    assert thread_metadata_object(await database.get_thread(thread_id))[
        "workspace_container"
    ] == {"volume_reclaimed": False}
