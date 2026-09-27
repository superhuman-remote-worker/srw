"""A real ordinary Resume remains creation progress after prior idle history."""
# ruff: noqa: F811 -- imported pytest fixture and its parameter name

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from orchestrator.routers.thread_session import get_thread
from orchestrator.routers.thread_files import get_thread_ide_status
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.vm_creation_owner_view import thread_creation_views
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from shared.workspace_idle_policy import RuntimeIdentity
from shared.workspace_idle_store import apply_idle_transition_on_conn
from tests.test_vm_session_retained_resume_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    db,  # noqa: F401
    pg_dsn,  # noqa: F401
    retained_resume,
    thread_schema,  # noqa: F401
)


@pytest.mark.asyncio
async def test_native_retained_resume_with_prior_idle_history_projects_creation(
    db,
    monkeypatch,
):
    publish = VMProvisioningPhaseStore.publish_thread_ready

    async def publish_with_idle_history(self, thread_id, generation, *args, **kwargs):
        accepted = await publish(self, thread_id, generation, *args, **kwargs)
        assert accepted
        async with db.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM threads WHERE id=$1::uuid FOR UPDATE",
                thread_id,
            )
            vm = json.loads(row["metadata"])["vm"]
            identity = RuntimeIdentity(
                "thread",
                str(row["id"]),
                "vm",
                generation,
                vm["vm_uid"],
            )
            entered = await apply_idle_transition_on_conn(
                conn,
                runtime=identity,
                event="enter",
                expected_revision=0,
                expected_episode_id=None,
                wait_kind="natural_pause",
                wait_key=str(uuid4()),
            )
            await apply_idle_transition_on_conn(
                conn,
                runtime=identity,
                event="exit",
                expected_revision=entered.revision,
                expected_episode_id=entered.episode.episode_id,
            )
        return accepted

    monkeypatch.setattr(
        VMProvisioningPhaseStore,
        "publish_thread_ready",
        publish_with_idle_history,
    )
    case, _, current, suspension = await retained_resume(
        db,
        monkeypatch,
        ready=True,
        marker=False,
        bind=True,
    )
    thread_id = str(case["thread_id"])
    await ensure_session_workspace(
        thread_id,
        db=db,
        provisioner=None,
        suspension=suspension,
        expected_runtime_generation=str(current["runtime_generation"]),
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1 AND request_id<>$2",
        case["thread_id"],
        case["request_id"],
    )
    assert source["thread_retained_resume_id"] is not None
    assert source["ready_at"] is None and source["observed_vm_uid"] is None
    assert source["expected_pvc_uid"] == UUID(case["pvc_uid"])
    assert await db.merge_thread_vm_context_if_provision_generation(
        thread_id,
        str(source["provision_generation"]),
        {"status": "ssh_pending"},
        require_status_not_ready=True,
    )
    current = await db.get_thread(thread_id)
    assert current["workspace_idle_revision"] == 2
    assert current["workspace_idle_episode"] is None
    assert await db.fetchval("SELECT count(*) FROM vm_idle_operations") == 0
    progress = await thread_creation_views(
        db,
        [thread_id],
        viewer_user_id=str(current["user_id"]),
    )
    assert progress[thread_id]["request_id"] == str(source["request_id"])
    assert progress[thread_id]["state"] in {"queued", "reconciling"}
    dependencies = SimpleNamespace(
        store=db,
        require_thread_owner=AsyncMock(
            return_value=(
                {"id": str(current["user_id"]), "is_admin": False},
                current,
            )
        ),
        resolve_cloud_session_url=lambda *_: None,
    )
    response = await get_thread(thread_id, object(), dependencies=dependencies)
    assert response["vm_creation"] == progress[thread_id]
    assert response["workspace_lifecycle"] is None
    ide = await get_thread_ide_status(thread_id, object(), dependencies=dependencies)
    assert ide == {
        "status": "restoring",
        "workspace_lifecycle": None,
        "code_server_url": None,
        "gitea_url": None,
    }
    assert (
        await thread_creation_views(
            db,
            [thread_id],
            viewer_user_id=str(uuid4()),
        )
        == {}
    )
    assert source == await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1",
        source["request_id"],
    )
    # A real new End invalidates the current-source read immediately, even
    # before authorization. Old progress must not become a lifecycle bypass.
    retirement = await db.begin_pinned_thread_retirement(thread_id, permanent=False)
    assert retirement["state"] == "pending"
    assert (
        await thread_creation_views(
            db,
            [thread_id],
            viewer_user_id=str(current["user_id"]),
        )
        == {}
    )
