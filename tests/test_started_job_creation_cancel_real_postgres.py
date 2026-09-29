"""Terminal Job cancellation hands an already started creation to cleanup."""

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests import test_workspace_pull_failure_real_postgres as fixtures
from orchestrator.services.container_provisioner import (
    WORKSPACE_CREATION_RESERVATION_ANNOTATION,
)


db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


@pytest.mark.asyncio
async def test_cancelled_started_job_hands_off_then_cleans_exact_runtime(
    db, monkeypatch
):
    job, owner, cluster, provider, runtime_uid = await fixtures._interrupted_pull(
        db, monkeypatch
    )
    pod = cluster.objects["pod"]
    pod.status.phase = "Running"
    (container,) = pod.status.container_statuses
    container.ready = True
    container.started = True
    container.container_id = "containerd://started-workspace"
    container.state = SimpleNamespace(
        waiting=None,
        running=SimpleNamespace(started_at=datetime.now(timezone.utc)),
        terminated=None,
    )

    # The real terminal-owner trigger revokes the creator and admits exactly
    # one prepared reclaim intent for the same immutable Pod UID.
    assert await db.cancel_job(str(job))
    assert await fixtures._open_authority(db, job) == {
        "reservations": 1,
        "intents": 1,
    }
    before = await db.fetchrow(
        "SELECT id, runtime_incarnation, resources_captured_at, attempts "
        "FROM managed_repository_workspace_cleanup_intents WHERE owner_id=$1",
        job,
    )
    assert str(before["runtime_incarnation"]) == runtime_uid
    assert before["resources_captured_at"] is None
    assert before["attempts"] == 0

    assert await provider.reconcile_pending_workspace_creation_reservations(limit=25) == {
        "handed_off": 1,
        "aborted": 0,
        "retryable": 0,
    }
    assert await fixtures._open_authority(db, job) == {
        "reservations": 0,
        "intents": 1,
    }
    assert cluster.pod_deletes == 0
    after = await db.fetchrow(
        "SELECT id, runtime_incarnation, resources_captured_at, attempts "
        "FROM managed_repository_workspace_cleanup_intents WHERE owner_id=$1",
        job,
    )
    assert after == before

    # Model only kubelet termination. The production cleanup records process
    # zero and releases the finalizer after seeing all containers stopped.
    delete_pod = cluster.delete_namespaced_pod

    def terminate_pod(**kwargs):
        delete_pod(**kwargs)
        if "pod" in cluster.objects:
            status = cluster.objects["pod"].status.container_statuses[0]
            status.ready = False
            status.started = False

    monkeypatch.setattr(cluster, "delete_namespaced_pod", terminate_pod)
    assert await provider.reconcile_pending_workspace_cleanup_intents(limit=25) == {
        "settled": 1,
        "superseded": 0,
        "retryable": 0,
    }
    assert await fixtures._open_authority(db, job) == {
        "reservations": 0,
        "intents": 0,
    }
    assert cluster.objects == {}
    assert cluster.pod_deletes == 1
    assert (await fixtures._workspace(db, job))["status"] == "deleted"
    assert await db.fetchval(
        "SELECT count(*) FROM managed_repository_process_zero_receipts "
        "WHERE owner_id=$1 AND runtime_incarnation=$2",
        job,
        runtime_uid,
    ) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsafe",
    [
        "replaced-pod",
        "wrong-annotation",
        "wrong-owner",
        "missing-finalizer",
        "unreadable-statuses",
        "kept-volume",
        "claimed-intent",
    ],
)
async def test_started_job_reuse_refuses_unsafe_authority(db, monkeypatch, unsafe):
    job, owner, cluster, provider, _runtime_uid = await fixtures._interrupted_pull(
        db, monkeypatch, kept_volume=unsafe == "kept-volume"
    )
    pod = cluster.objects["pod"]
    pod.status.phase = "Running"
    (container,) = pod.status.container_statuses
    container.ready = True
    container.started = True
    container.state = SimpleNamespace(
        waiting=None,
        running=SimpleNamespace(started_at=datetime.now(timezone.utc)),
        terminated=None,
    )
    assert await db.cancel_job(str(job))

    if unsafe == "replaced-pod":
        pod.metadata.uid = str(uuid4())
    elif unsafe == "wrong-annotation":
        pod.metadata.annotations[WORKSPACE_CREATION_RESERVATION_ANNOTATION] = str(
            uuid4()
        )
    elif unsafe == "wrong-owner":
        pod.metadata.labels[owner.label_key] = str(uuid4())
    elif unsafe == "missing-finalizer":
        pod.metadata.finalizers = []
    elif unsafe == "unreadable-statuses":
        pod.status.container_statuses = object()
    elif unsafe == "claimed-intent":
        intent_id = await db.fetchval(
            "SELECT id FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_id=$1",
            job,
        )
        assert await db.claim_managed_repository_workspace_cleanup_intent(
            str(intent_id), claimant="other-cleaner", lease_seconds=300
        )

    cancellation = await provider.request_workspace_creation_cancellation(
        owner, target_disposition="deleted", reclaim_shared_resources=True
    )
    assert cancellation is not None
    assert cancellation["reconciliation_outcome"] == "retryable"
    assert await fixtures._open_authority(db, job) == {
        "reservations": 1,
        "intents": 1,
    }
    assert cluster.pod_deletes == 0
    assert "pod" in cluster.objects
    assert "pvc" in cluster.objects
