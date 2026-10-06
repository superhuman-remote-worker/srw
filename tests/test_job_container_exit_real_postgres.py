"""A Job whose container exits before Ready fails, then is cleaned up (D3, b2).

Real PostgreSQL with the full migration chain; only Kubernetes is faked. With
startup-stage tracking off (the chart default), the legacy readiness wait of a
fresh Job create sees its exact ``restartPolicy: Never`` Pod already stopped
and fails at once with the exit code. The creation adds no settle authority:
its reservation stays open (the abort is refused once the Pod was issued), the
dispatcher fails the Job, the real terminal-owner trigger cancels the
reservation and opens the terminal cleanup intent, the reservation reconciler
hands off through the stopped-Job branch, and teardown proves process zero
from the exactly terminated Pod without SSH.

A failed Job reuses its terminal intent only with that exact process-zero
evidence; its running or never-started Pod stays held, as before.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import container_provisioner as provisioner_module
from orchestrator.services.container_provisioner import WorkspaceContainerExitedError
from orchestrator.services.workspace_lifecycle import (
    EnsureOutcome,
    WorkspaceOwner,
    ensure_workspace,
)
from tests import _b09_control_seams as control_seams
from tests import test_workspace_pull_failure_real_postgres as fixtures

db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied

EXITED = (
    "Workspace container exited with code 0 (Completed) before it became ready. "
    "A workspace image must keep running: build it FROM an SRW base image and "
    "don't override its ENTRYPOINT or USER."
)


class ExitingCluster(fixtures.NeverPullingCluster):
    """The image pulls, its main process runs to completion and exits 0."""

    def create_namespaced_pod(self, *, body, **kwargs):
        pod = super().create_namespaced_pod(body=body, **kwargs)
        pod.spec.restart_policy = body["spec"]["restartPolicy"]
        pod.spec.node_name = "test-worker"
        # The production all-container proof reads declared container names.
        pod.spec.containers = [
            SimpleNamespace(**item) for item in body["spec"]["containers"]
        ]
        pod.status.phase = "Succeeded"
        (container,) = pod.status.container_statuses
        container.container_id = "containerd://exited-workspace"
        container.ready = False
        container.started = False
        container.state = SimpleNamespace(
            waiting=None,
            running=None,
            terminated=SimpleNamespace(
                exit_code=0,
                reason="Completed",
                started_at=datetime.now(timezone.utc),
                finished_at=datetime.now(timezone.utc),
            ),
        )
        return pod

    def delete_namespaced_pod(self, *, name, body=None, **_):
        # Its containers already stopped: the kubelet has nothing to
        # terminate, and the object stays until the finalizer is released.
        pod = self._read("pod", name)
        if fixtures._precondition(body) not in {None, pod.metadata.uid}:
            raise fixtures.ApiException(status=409, reason="Conflict")
        self.pod_deletes += 1
        pod.metadata.deletion_timestamp = datetime.now(timezone.utc)
        if not pod.metadata.finalizers:
            del self.objects["pod"]


async def _reservation(db, job):
    return await db.fetchrow(
        "SELECT phase, settled_at, startup_protocol_version, cancel_requested_at, "
        "cancel_target_disposition, cancel_resource_policy, runtime_incarnation "
        "FROM managed_repository_workspace_creation_reservations "
        "WHERE owner_kind='job' AND owner_id=$1 AND scope='workspace_container'",
        job,
    )


async def _open_intent(db, job):
    return await db.fetchrow(
        "SELECT id, runtime_incarnation, target_disposition, resource_policy, "
        "capture_complete, resources_captured_at, attempts, claimed_by "
        "FROM managed_repository_workspace_cleanup_intents "
        "WHERE owner_kind='job' AND owner_id=$1 AND scope='workspace_container' "
        "AND settled_at IS NULL",
        job,
    )


@pytest.mark.asyncio
async def test_a_failed_job_whose_container_exited_is_cleaned_up_and_deletable(
    db, monkeypatch
):
    monkeypatch.delenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", raising=False)
    job = await fixtures._job(db)
    owner = WorkspaceOwner.job(str(job))
    cluster = ExitingCluster()
    p = fixtures._provisioner(monkeypatch, db, cluster)
    settle_failed = p._settle_failed_job_creation
    p._settle_failed_job_creation = AsyncMock(side_effect=settle_failed)

    # The dispatcher's first ensure: a real create and the real legacy wait.
    result = await ensure_workspace(
        owner, provisioner=p, suspension=SimpleNamespace(), current_status=None
    )
    assert result.outcome is EnsureOutcome.FAILED

    workspace = await fixtures._workspace(db, job)
    runtime_uid = cluster.objects["pod"].metadata.uid
    assert workspace["error"] == EXITED
    assert workspace["status"] == "created"
    assert workspace["_runtime_incarnation"] == runtime_uid
    assert set(cluster.objects) == {"pvc", "service", "pod"}
    assert cluster.pod_deletes == 0
    # b2: no settle authority. Task 11a's never-started settle refuses a Pod
    # that ran, and the abort is refused once the Pod was issued, so the
    # unmarked creation stays open and bound to the exact Pod.
    ((_, _, error), _kwargs) = p._settle_failed_job_creation.await_args
    assert isinstance(error, WorkspaceContainerExitedError)
    open_receipt = await _reservation(db, job)
    assert open_receipt["phase"] == "runtime_bound"
    assert open_receipt["settled_at"] is None
    assert open_receipt["startup_protocol_version"] is None
    assert open_receipt["cancel_requested_at"] is None
    assert str(open_receipt["runtime_incarnation"]) == runtime_uid
    assert await fixtures._open_authority(db, job) == {"reservations": 1, "intents": 0}

    # The dispatcher fails the Job with the recorded error as is (its message
    # composition is unit-tested in test_stateless_worker_control).
    assert workspace["error"].startswith(WorkspaceContainerExitedError.MESSAGE_PREFIX)
    await db.update_job_status(str(job), status="failed", error_message=EXITED)
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status::text, error_message FROM jobs WHERE id=$1", job
        )
    assert tuple(row) == ("failed", EXITED)

    # The real terminal-owner trigger cancels the open creation for terminal
    # reclaim and admits exactly one cleanup intent for the same Pod UID.
    cancelled = await _reservation(db, job)
    assert cancelled["phase"] == "runtime_bound"
    assert cancelled["settled_at"] is None
    assert cancelled["cancel_requested_at"] is not None
    assert (
        cancelled["cancel_target_disposition"],
        cancelled["cancel_resource_policy"],
    ) == ("deleted", "terminal_reclaim")
    intent_before = await _open_intent(db, job)
    assert str(intent_before["runtime_incarnation"]) == runtime_uid
    assert (intent_before["target_disposition"], intent_before["resource_policy"]) == (
        "deleted",
        "terminal_reclaim",
    )
    assert intent_before["capture_complete"] is False
    assert intent_before["resources_captured_at"] is None
    assert await fixtures._open_authority(db, job) == {"reservations": 1, "intents": 1}

    # Lifecycle reaper, step 1: the reviewed stopped-Job branch (436a40e13)
    # accepts the failed Job's exactly terminated Pod and hands the cancelled
    # creation to that same unclaimed intent.
    reuse = p._cancelled_creation_may_reuse_terminal_intent
    reuse_results = []

    async def observed_reuse(*args, **kwargs):
        reuse_results.append(await reuse(*args, **kwargs))
        return reuse_results[-1]

    monkeypatch.setattr(
        p, "_cancelled_creation_may_reuse_terminal_intent", observed_reuse
    )
    assert await p.reconcile_pending_workspace_creation_reservations(limit=25) == {
        "handed_off": 1,
        "aborted": 0,
        "retryable": 0,
    }
    assert reuse_results == [True]
    assert await fixtures._open_authority(db, job) == {"reservations": 0, "intents": 1}
    assert await _open_intent(db, job) == intent_before
    assert cluster.pod_deletes == 0

    # Step 2: teardown proves process zero from the exactly terminated Pod
    # (exact_terminal); there is no SSH endpoint and none is attempted.
    p.attest_workspace_runtime = AsyncMock(
        side_effect=AssertionError("an exited Pod has no SSH endpoint")
    )
    p._retire_managed_repository_agents = AsyncMock(
        side_effect=AssertionError("an exited Pod needs no SSH retirement")
    )
    authority = p.workspace_pod_authority
    authorities = []

    async def observed_authority(*args, **kwargs):
        authorities.append(await authority(*args, **kwargs))
        return authorities[-1]

    monkeypatch.setattr(p, "workspace_pod_authority", observed_authority)
    assert await p.reconcile_pending_workspace_cleanup_intents(limit=25) == {
        "settled": 1,
        "superseded": 0,
        "retryable": 0,
    }
    assert "exact_terminal" in authorities
    p.attest_workspace_runtime.assert_not_awaited()
    p._retire_managed_repository_agents.assert_not_awaited()
    assert cluster.objects == {}
    assert cluster.pod_deletes == 1
    assert (await fixtures._workspace(db, job))["status"] == "deleted"
    assert await fixtures._open_authority(db, job) == {"reservations": 0, "intents": 0}
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='job' AND owner_id=$1 AND scope='workspace_container' "
            "AND provisioner='k8s' AND runtime_incarnation=$2",
            job,
            runtime_uid,
        )
        == 1
    )

    # DELETE /api/jobs/{id}: nothing is left to retire, and the row may go.
    monkeypatch.setattr(provisioner_module, "container_provisioner", p)
    import orchestrator.main as main

    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    assert await control_seams.archive_and_cleanup_workspace(str(job)) == []
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM jobs WHERE id=$1", job)
        assert await conn.fetchval("SELECT count(*) FROM jobs WHERE id=$1", job) == 0


def _running_never_ready(pod):
    pod.spec.node_name = "test-worker"
    pod.spec.containers = [
        SimpleNamespace(**item) if isinstance(item, dict) else item
        for item in pod.spec.containers
    ]
    pod.status.phase = "Running"
    (container,) = pod.status.container_statuses
    container.ready = False
    container.started = True
    container.container_id = "containerd://running-workspace"
    container.state = SimpleNamespace(
        waiting=None,
        running=SimpleNamespace(started_at=datetime.now(timezone.utc)),
        terminated=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("pod_state", ["running", "never-started"])
async def test_a_failed_job_whose_pod_has_not_stopped_keeps_its_runtime_held(
    db, monkeypatch, pod_state
):
    """Only exact process-zero evidence lets a failed Job's creation hand off."""

    job, _owner, cluster, provider, runtime_uid = await fixtures._interrupted_pull(
        db, monkeypatch
    )
    if pod_state == "running":
        _running_never_ready(cluster.objects["pod"])
    await db.update_job_status(
        str(job), status="failed", error_message="failed for another reason"
    )
    assert await fixtures._open_authority(db, job) == {"reservations": 1, "intents": 1}
    intent_before = await _open_intent(db, job)

    # Counts are module-wide (earlier held rows stay open); judge this Job.
    counts = await provider.reconcile_pending_workspace_creation_reservations(limit=25)
    assert (counts["handed_off"], counts["aborted"]) == (0, 0)
    assert counts["retryable"] >= 1
    counts = await provider.reconcile_pending_workspace_cleanup_intents(limit=25)
    assert counts["settled"] == 0
    assert await fixtures._open_authority(db, job) == {"reservations": 1, "intents": 1}
    assert await _open_intent(db, job) == intent_before
    assert set(cluster.objects) == {"pvc", "service", "pod"}
    assert cluster.pod_deletes == 0
    assert (await _reservation(db, job))["settled_at"] is None


@pytest.mark.asyncio
async def test_a_failed_job_whose_container_exited_can_be_deleted_before_any_sweep(
    db, monkeypatch
):
    """DELETE /api/jobs/{id} right after the failure needs no SSH attestation."""

    monkeypatch.delenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", raising=False)
    job = await fixtures._job(db)
    owner = WorkspaceOwner.job(str(job))
    cluster = ExitingCluster()
    p = fixtures._provisioner(monkeypatch, db, cluster)
    p.attest_workspace_runtime = AsyncMock(
        side_effect=AssertionError("an exited Pod has no SSH endpoint")
    )
    monkeypatch.setattr(provisioner_module, "container_provisioner", p)
    import orchestrator.main as main

    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)

    result = await ensure_workspace(
        owner, provisioner=p, suspension=SimpleNamespace(), current_status=None
    )
    assert result.outcome is EnsureOutcome.FAILED
    await db.update_job_status(str(job), status="failed", error_message=EXITED)

    assert await control_seams.archive_and_cleanup_workspace(str(job)) == [
        "k8s workspace released"
    ]
    assert cluster.objects == {}
    assert (await fixtures._workspace(db, job))["status"] == "deleted"
    assert await fixtures._open_authority(db, job) == {"reservations": 0, "intents": 0}
    p.attest_workspace_runtime.assert_not_awaited()
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM jobs WHERE id=$1", job)
