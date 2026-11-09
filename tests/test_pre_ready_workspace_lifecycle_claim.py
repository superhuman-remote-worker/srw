"""A failed pre-Ready stateless Job Pod remains under creation authority.

Regression for the captured exit-17 startup: the lifecycle reconciler must
not take a two-hour completion-control claim for an active creation that has
never produced first Ready. The source creation/cancel protocol owns this
runtime; a terminal Job or a pinned/legacy workspace is a different case.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from orchestrator.services.lifecycle import InstanceLifecycleReconciler
from tests.test_lifecycle_workspace_manager import (
    _fake_to_thread,
    _make_manager,
    _make_pod,
)


@pytest.mark.asyncio
async def test_failed_never_ready_stateless_job_does_not_claim_unhealthy_delete():
    job_id, pod_uid, creation_id = (str(uuid4()) for _ in range(3))
    pod_name = f"workspace-{job_id[:12]}"
    pod = _make_pod(
        pod_name,
        labels={"srw/job-id": job_id, "srw.io/component": "agent-workspace"},
        phase="Failed",
    )
    pod.metadata.uid = pod_uid
    context = {
        "workspace_container": {
            "status": "creating",
            "provisioner": "k8s",
            "pod_name": pod.metadata.name,
            "_runtime_incarnation": pod_uid,
            "_creation_reservation_id": creation_id,
            "_creation_claim_token": 270,
        }
    }
    job = {
        "id": job_id,
        "status": "created",
        "execution_lane": "stateless",
        "context": context,
    }
    mgr, container, _, _, db = _make_manager(
        pods=[pod], job_rows={job_id: job}, completion_commands_enabled=True
    )
    # Model the current admission refusal: no intent or physical cleanup is
    # recorded. The fake connection persists a completion claim if acquired.
    container.prepare_workspace_cleanup_intent = AsyncMock(return_value=None)
    empty_pvcs = MagicMock()
    empty_pvcs.items = []
    container._core_api.list_namespaced_persistent_volume_claim.return_value = (
        empty_pvcs
    )
    conn = db.acquire.return_value.__aenter__.return_value
    original_fetchrow = conn.fetchrow.side_effect

    async def fetchrow(sql, *args):
        if "UPDATE jobs" in sql and "jsonb_build_object" in sql:
            context["_completion_control_claim"] = {
                "source": args[3],
                "expected_status": args[4],
                "expected_lane": args[5],
                "fence_kind": args[6],
                "fence_value": args[7],
            }
            return {"context": context}
        if "UPDATE jobs" in sql and "- '_completion_control_claim'" in sql:
            context.pop("_completion_control_claim", None)
            return {"id": job_id}
        return await original_fetchrow(sql, *args)

    conn.fetchrow.side_effect = fetchrow
    with patch(
        "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
        side_effect=_fake_to_thread,
    ):
        report = await InstanceLifecycleReconciler([mgr]).tick()

    assert report["workspace"]["listed"] == 1
    assert "_completion_control_claim" not in context
    assert report["workspace"]["unhealthy"] == 0
    container.prepare_workspace_cleanup_intent.assert_not_awaited()
    container.reconcile_workspace_cleanup_intent.assert_not_awaited()
    assert job["status"] == "created"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("job_status", "execution_lane"),
    [("completed", "stateless"), ("created", "pinned")],
    ids=["terminal-stateless", "pre-ready-pinned"],
)
async def test_other_failed_job_workspaces_keep_generic_unhealthy_classification(
    job_status, execution_lane
):
    job_id = str(uuid4())
    pod = _make_pod(
        f"workspace-{job_id[:12]}",
        labels={"srw/job-id": job_id, "srw.io/component": "agent-workspace"},
        phase="Failed",
    )
    row = {
        "id": job_id,
        "status": job_status,
        "execution_lane": execution_lane,
        "context": {"workspace_container": {"status": "creating"}},
    }
    mgr, *_ = _make_manager(
        pods=[pod], job_rows={job_id: row}, completion_commands_enabled=True
    )
    with patch(
        "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
        side_effect=_fake_to_thread,
    ):
        [inst] = await mgr.list_instances()

    assert inst.metadata["job_status"] == job_status
    assert inst.metadata["execution_lane"] == execution_lane
    assert inst.metadata["pod_phase"] == "Failed"
    assert await mgr.is_healthy(inst) is False


def _released_then_resumed_job(job_id, pod_uid, creation_id, *, updated_at):
    """Kai's cca716a8 on dev: paused, a Running Pod never published Ready."""

    context = {
        "workspace_container": {
            "status": "creating",
            "provisioner": "k8s",
            "pod_name": f"workspace-{job_id[:12]}",
            "_runtime_incarnation": pod_uid,
            "_creation_reservation_id": creation_id,
            "_creation_claim_token": 3,
            "snapshot_attempts": 5,
        },
        "queued_feedback": "Remove the two tracked .pyc files.",
    }
    return {
        "id": job_id,
        "status": "paused",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
        "updated_at": updated_at,
        "context": context,
    }


def _pvc_backed_pod(job_id, pod_uid):
    pod = _make_pod(
        f"workspace-{job_id[:12]}",
        labels={"srw/job-id": job_id, "srw.io/component": "agent-workspace"},
        phase="Running",
    )
    pod.metadata.uid = pod_uid
    volume = MagicMock()
    volume.name = "workspace-data"
    volume.persistent_volume_claim = MagicMock()
    pod.spec.volumes = [volume]
    return pod


@pytest.mark.asyncio
@pytest.mark.parametrize("idle_window_elapsed", [False, True])
async def test_reaper_leaves_a_paused_jobs_unsettled_creation_alone(
    monkeypatch, idle_window_elapsed
):
    """No claim, no attempt, no give_up for a creation that has not settled.

    On dev the reaper counted snapshot attempts against the starting Pod (it
    has no pod_ip, so it is never reachable), gave up, had its delete refused
    by automatic admission and held a 2 h control claim that blocked resume
    and Cancel, re-armed on every expiry.
    """

    from datetime import datetime, timedelta, timezone

    monkeypatch.setenv(
        "WORKSPACE_IDLE_POD_RELEASE_SECONDS", "60" if idle_window_elapsed else "86400"
    )
    job_id, pod_uid, creation_id = (str(uuid4()) for _ in range(3))
    pod = _pvc_backed_pod(job_id, pod_uid)
    job = _released_then_resumed_job(
        job_id,
        pod_uid,
        creation_id,
        updated_at=datetime.now(timezone.utc) - timedelta(days=2),
    )
    context = job["context"]
    mgr, container, _, _, db = _make_manager(
        pods=[pod], job_rows={job_id: job}, completion_commands_enabled=True
    )
    db.managed_repository_workspace_runtime_is_unannotated = AsyncMock(
        return_value=True
    )
    conn = db.acquire.return_value.__aenter__.return_value
    original_fetchrow = conn.fetchrow.side_effect

    async def fetchrow(sql, *args):
        if "UPDATE jobs" in sql and "jsonb_build_object" in sql:
            context["_completion_control_claim"] = {"source": args[3]}
            return {"context": context}
        return await original_fetchrow(sql, *args)

    conn.fetchrow.side_effect = fetchrow
    with patch(
        "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
        side_effect=_fake_to_thread,
    ):
        [inst] = await mgr.list_instances()
        assert inst.metadata["creation_unsettled"] is True
        assert await mgr.is_reapable(inst) is False
        assert await mgr.is_healthy(inst) is True
        report = await InstanceLifecycleReconciler([mgr]).tick()

    db.managed_repository_workspace_runtime_is_unannotated.assert_awaited_with(
        job_id, owner_kind="job", scope="workspace_container"
    )
    assert "_completion_control_claim" not in context
    assert report["workspace"].get("reap_attempts", 0) == 0
    assert report["workspace"].get("reap_forced", 0) == 0
    container.prepare_workspace_cleanup_intent.assert_not_awaited()
    db.merge_workspace_container_context_if_runtime.assert_not_awaited()
    assert context["workspace_container"]["snapshot_attempts"] == 5


@pytest.mark.asyncio
async def test_settled_runtime_of_the_same_paused_job_stays_releasable(monkeypatch):
    """The exemption is keyed on the receipt, not on the status: control case."""

    from datetime import datetime, timedelta, timezone

    monkeypatch.setenv("WORKSPACE_IDLE_POD_RELEASE_SECONDS", "60")
    job_id, pod_uid, creation_id = (str(uuid4()) for _ in range(3))
    pod = _pvc_backed_pod(job_id, pod_uid)
    job = _released_then_resumed_job(
        job_id,
        pod_uid,
        creation_id,
        updated_at=datetime.now(timezone.utc) - timedelta(days=2),
    )
    mgr, _, _, _, db = _make_manager(
        pods=[pod], job_rows={job_id: job}, completion_commands_enabled=True
    )
    db.managed_repository_workspace_runtime_is_unannotated = AsyncMock(
        return_value=False
    )
    with patch(
        "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
        side_effect=_fake_to_thread,
    ):
        [inst] = await mgr.list_instances()

    assert inst.metadata["creation_unsettled"] is False
    assert await mgr.is_reapable(inst) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["cancelled", "failed", "completed"])
async def test_terminal_job_with_unsettled_creation_stays_reapable(status):
    """A creation that never settles cannot pin a terminal Job's Pod forever."""

    job_id, pod_uid, creation_id = (str(uuid4()) for _ in range(3))
    pod = _pvc_backed_pod(job_id, pod_uid)
    job = _released_then_resumed_job(job_id, pod_uid, creation_id, updated_at=None)
    job["status"] = status
    mgr, _, _, _, db = _make_manager(
        pods=[pod], job_rows={job_id: job}, completion_commands_enabled=True
    )
    db.managed_repository_workspace_runtime_is_unannotated = AsyncMock(
        return_value=True
    )
    with patch(
        "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
        side_effect=_fake_to_thread,
    ):
        [inst] = await mgr.list_instances()

    assert inst.metadata["creation_unsettled"] is False
    db.managed_repository_workspace_runtime_is_unannotated.assert_not_awaited()
    assert await mgr.is_reapable(inst) is True


@pytest.mark.asyncio
async def test_refused_cleanup_admission_is_logged(caplog):
    """Dev refused this delete for a day and said nothing at any level."""

    job_id, pod_uid, creation_id = (str(uuid4()) for _ in range(3))
    pod = _pvc_backed_pod(job_id, pod_uid)
    job = _released_then_resumed_job(job_id, pod_uid, creation_id, updated_at=None)
    job["status"] = "cancelled"
    mgr, container, *_ = _make_manager(pods=[pod], job_rows={job_id: job})
    container.prepare_workspace_cleanup_intent = AsyncMock(return_value=None)
    with patch(
        "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
        side_effect=_fake_to_thread,
    ):
        [inst] = await mgr.list_instances()
    with caplog.at_level("INFO"):
        assert await mgr._delete_owned(inst, 0) is False
    assert "was not admitted" in caplog.text
    assert job_id in caplog.text
