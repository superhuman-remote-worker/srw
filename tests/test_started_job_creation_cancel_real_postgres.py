"""Terminal Job cancellation hands an already started creation to cleanup."""

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests import test_workspace_pull_failure_real_postgres as fixtures
from orchestrator.database.container_startup_stage import (
    ReadyObservedAt,
    ScheduledAt,
    StageBudgets,
    StartupAttention,
)
from orchestrator.services.container_provisioner import (
    WORKSPACE_CREATION_RESERVATION_ANNOTATION,
)


db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


def _failed_exit17_before_ready(pod):
    # Keep the fake's Kubernetes container declaration complete enough for
    # the production all-container proof and storage-binding checks.
    pod.spec.node_name = "test-worker"
    pod.spec.containers = [SimpleNamespace(**item) for item in pod.spec.containers]
    pod.status.phase = "Failed"
    (container,) = pod.status.container_statuses
    container.container_id = "containerd://exit17-workspace"
    container.ready = False
    container.started = False
    container.state = SimpleNamespace(
        waiting=None,
        running=None,
        terminated=SimpleNamespace(
            exit_code=17,
            reason="Error",
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
        ),
    )


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

    assert await provider.reconcile_pending_workspace_creation_reservations(
        limit=25
    ) == {
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
    assert (
        await db.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts "
            "WHERE owner_id=$1 AND runtime_incarnation=$2",
            job,
            runtime_uid,
        )
        == 1
    )


@pytest.mark.asyncio
async def test_cancelled_exit17_before_ready_reuses_exact_terminal_intent_and_reclaims(
    db, monkeypatch
):
    """A failed but fully terminated original Pod must not strand its prepared intent."""

    job, _owner, cluster, provider, runtime_uid = await fixtures._interrupted_pull(
        db, monkeypatch
    )
    pod = cluster.objects["pod"]
    _failed_exit17_before_ready(pod)
    reservation = await db.fetchrow(
        "SELECT id, claim_token, pod_uid, runtime_incarnation, phase FROM "
        "managed_repository_workspace_creation_reservations "
        "WHERE owner_kind='job' AND owner_id=$1 AND scope='workspace_container' "
        "AND settled_at IS NULL",
        job,
    )
    assert reservation["phase"] == "runtime_bound"
    assert (
        str(reservation["pod_uid"])
        == str(reservation["runtime_incarnation"])
        == runtime_uid
    )
    assert await db.observe_container_startup(
        owner_kind="job",
        owner_id=str(job),
        reservation_id=str(reservation["id"]),
        claim_token=int(reservation["claim_token"]),
        pod_uid=runtime_uid,
        observation=ScheduledAt(datetime.now(timezone.utc)),
        budgets=StageBudgets(ready_seconds=1, pull_seconds=1, ssh_seconds=1),
        adopt_if_unmarked=True,
    )
    await asyncio.sleep(1.1)
    assert await db.observe_container_startup(
        owner_kind="job",
        owner_id=str(job),
        reservation_id=str(reservation["id"]),
        claim_token=int(reservation["claim_token"]),
        pod_uid=runtime_uid,
        observation=StartupAttention("readiness_deadline"),
    )
    source = await db.fetchrow(
        "SELECT startup_protocol_version,startup_stage,startup_state,"
        "startup_reason_code,startup_first_ready_at FROM "
        "managed_repository_workspace_creation_reservations WHERE id=$1",
        reservation["id"],
    )
    assert tuple(source) == (1, "readiness", "attention", "readiness_deadline", None)

    assert await db.cancel_job(str(job))
    intent_before = await db.fetchrow(
        "SELECT id,runtime_incarnation,target_disposition,resource_policy,"
        "resources_captured_at,attempts,claimed_by FROM "
        "managed_repository_workspace_cleanup_intents "
        "WHERE owner_kind='job' AND owner_id=$1 AND scope='workspace_container' "
        "AND settled_at IS NULL",
        job,
    )
    assert str(intent_before["runtime_incarnation"]) == runtime_uid
    assert (intent_before["target_disposition"], intent_before["resource_policy"]) == (
        "deleted",
        "terminal_reclaim",
    )
    assert intent_before["resources_captured_at"] is None
    assert intent_before["attempts"] == 0
    assert intent_before["claimed_by"] is None
    assert await fixtures._open_authority(db, job) == {"reservations": 1, "intents": 1}
    assert await provider.reconcile_pending_workspace_creation_reservations(
        limit=25
    ) == {"handed_off": 1, "aborted": 0, "retryable": 0}
    assert await fixtures._open_authority(db, job) == {"reservations": 0, "intents": 1}
    intent_after = await db.fetchrow(
        "SELECT id,runtime_incarnation,target_disposition,resource_policy,"
        "resources_captured_at,attempts,claimed_by FROM "
        "managed_repository_workspace_cleanup_intents WHERE id=$1",
        intent_before["id"],
    )
    assert intent_after == intent_before
    assert cluster.pod_deletes == 0

    assert await provider.reconcile_pending_workspace_cleanup_intents(limit=25) == {
        "settled": 1,
        "superseded": 0,
        "retryable": 0,
    }
    assert await fixtures._open_authority(db, job) == {"reservations": 0, "intents": 0}
    assert cluster.objects == {}
    assert cluster.pod_deletes == 1
    assert (await fixtures._workspace(db, job))["status"] == "deleted"
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


@pytest.mark.asyncio
async def test_cancelled_succeeded_before_ready_reuses_exact_terminal_intent(
    db, monkeypatch
):
    job, _owner, cluster, provider, runtime_uid = await fixtures._interrupted_pull(
        db, monkeypatch
    )
    pod = cluster.objects["pod"]
    _failed_exit17_before_ready(pod)
    pod.status.phase = "Succeeded"
    pod.status.container_statuses[0].state.terminated.exit_code = 0

    assert await db.cancel_job(str(job))
    intent_id = await db.fetchval(
        "SELECT id FROM managed_repository_workspace_cleanup_intents "
        "WHERE owner_kind='job' AND owner_id=$1 AND scope='workspace_container' "
        "AND settled_at IS NULL",
        job,
    )
    assert await provider.reconcile_pending_workspace_creation_reservations(
        limit=25
    ) == {"handed_off": 1, "aborted": 0, "retryable": 0}
    assert await fixtures._open_authority(db, job) == {"reservations": 0, "intents": 1}
    assert (
        str(
            await db.fetchval(
                "SELECT runtime_incarnation FROM managed_repository_workspace_cleanup_intents "
                "WHERE id=$1",
                intent_id,
            )
        )
        == runtime_uid
    )
    assert cluster.pod_deletes == 0

    assert await provider.reconcile_pending_workspace_cleanup_intents(limit=25) == {
        "settled": 1,
        "superseded": 0,
        "retryable": 0,
    }
    assert cluster.objects == {}
    assert await fixtures._open_authority(db, job) == {"reservations": 0, "intents": 0}


@pytest.mark.asyncio
async def test_cancelled_started_v1_ssh_attention_hands_off_and_cleans_exact_runtime(
    db, monkeypatch
):
    job, _owner, cluster, provider, runtime_uid = await fixtures._interrupted_pull(
        db, monkeypatch
    )
    pod = cluster.objects["pod"]
    pod.status.phase = "Running"
    (container,) = pod.status.container_statuses
    container.ready = True
    container.started = True
    container.container_id = "containerd://started-v1-workspace"
    container.state = SimpleNamespace(
        waiting=None,
        running=SimpleNamespace(started_at=datetime.now(timezone.utc)),
        terminated=None,
    )
    receipt = await db.fetchrow(
        "SELECT id, claim_token, phase, pod_uid, runtime_incarnation "
        "FROM managed_repository_workspace_creation_reservations "
        "WHERE owner_kind='job' AND owner_id=$1 AND scope='workspace_container' "
        "AND operation_kind='create' AND settled_at IS NULL",
        job,
    )
    assert receipt["phase"] == "runtime_bound"
    assert str(receipt["pod_uid"]) == str(receipt["runtime_incarnation"]) == runtime_uid
    observe = dict(
        owner_kind="job",
        owner_id=str(job),
        reservation_id=str(receipt["id"]),
        claim_token=int(receipt["claim_token"]),
        pod_uid=runtime_uid,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO srw_execution_specs "
            "(work_kind,work_id,document,resolved,revision,harness_adapter) "
            "VALUES('Job',$1,$2::jsonb,$3::jsonb,$4,'srw/v1')",
            job,
            json.dumps({}),
            json.dumps({"spec": {}}),
            str(uuid4()),
        )
    execution = await db.fetchrow(
        "SELECT harness_adapter, resolved->'spec'->>'timeoutSeconds' "
        "AS timeout_seconds FROM srw_execution_specs "
        "WHERE work_kind='Job' AND work_id=$1",
        job,
    )
    assert execution["harness_adapter"] == "srw/v1"
    assert execution["timeout_seconds"] is None

    scheduled_at = datetime.now(timezone.utc)
    assert await db.observe_container_startup(
        **observe,
        observation=ScheduledAt(scheduled_at),
        budgets=StageBudgets(ready_seconds=180, pull_seconds=None, ssh_seconds=1),
        adopt_if_unmarked=True,
    )
    ready_at = datetime.now(timezone.utc)
    assert await db.observe_container_startup(
        **observe, observation=ReadyObservedAt(ready_at)
    )
    await asyncio.sleep(1.1)
    assert await db.observe_container_startup(
        **observe, observation=StartupAttention("ssh_deadline")
    )
    before = await db.fetchrow(
        "SELECT startup_protocol_version, startup_stage, startup_state, "
        "startup_reason_code, scheduled_at, startup_first_ready_at, "
        "startup_attention_at, settled_at FROM "
        "managed_repository_workspace_creation_reservations WHERE id=$1",
        receipt["id"],
    )
    assert (
        before["startup_protocol_version"],
        before["startup_stage"],
        before["startup_state"],
        before["startup_reason_code"],
    ) == (1, "readiness", "attention", "ssh_deadline")
    assert before["scheduled_at"] == scheduled_at
    assert before["startup_first_ready_at"] == ready_at
    assert before["startup_attention_at"] is not None
    assert before["settled_at"] is None
    assert not await db.observe_container_startup(
        **observe, observation=ReadyObservedAt(datetime.now(timezone.utc))
    )
    assert not await db.observe_container_startup(
        **observe, observation=ScheduledAt(scheduled_at)
    )

    # The real terminal trigger prepares exactly one reclaim intent; neither
    # a sticky attention state nor an optional execution timeout may replace it.
    assert await db.cancel_job(str(job))
    assert await fixtures._open_authority(db, job) == {
        "reservations": 1,
        "intents": 1,
    }
    intent_before = await db.fetchrow(
        "SELECT id, runtime_incarnation, target_disposition, resource_policy, "
        "resources_captured_at, attempts FROM "
        "managed_repository_workspace_cleanup_intents "
        "WHERE owner_kind='job' AND owner_id=$1 AND scope='workspace_container' "
        "AND settled_at IS NULL",
        job,
    )
    assert str(intent_before["runtime_incarnation"]) == runtime_uid
    assert (intent_before["target_disposition"], intent_before["resource_policy"]) == (
        "deleted",
        "terminal_reclaim",
    )
    assert intent_before["resources_captured_at"] is None
    assert intent_before["attempts"] == 0
    assert await provider.reconcile_pending_workspace_creation_reservations(
        limit=25
    ) == {
        "handed_off": 1,
        "aborted": 0,
        "retryable": 0,
    }
    assert await fixtures._open_authority(db, job) == {
        "reservations": 0,
        "intents": 1,
    }
    intent_after = await db.fetchrow(
        "SELECT id, runtime_incarnation, target_disposition, resource_policy, "
        "resources_captured_at, attempts FROM "
        "managed_repository_workspace_cleanup_intents WHERE id=$1",
        intent_before["id"],
    )
    assert intent_after == intent_before
    assert cluster.pod_deletes == 0

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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsafe",
    [
        "missing-status",
        "unknown-status",
        "still-running",
        "kept-volume",
        "replaced-pod",
        "wrong-annotation",
        "wrong-owner",
        "missing-finalizer",
        "claimed-intent",
    ],
)
async def test_cancelled_exit17_reuse_refuses_unsafe_authority(db, monkeypatch, unsafe):
    job, owner, cluster, provider, _runtime_uid = await fixtures._interrupted_pull(
        db, monkeypatch, kept_volume=unsafe == "kept-volume"
    )
    pod = cluster.objects["pod"]
    _failed_exit17_before_ready(pod)
    assert await db.cancel_job(str(job))

    if unsafe == "missing-status":
        pod.status.container_statuses = []
    elif unsafe == "unknown-status":
        pod.status.container_statuses = object()
    elif unsafe == "still-running":
        pod.status.container_statuses[0].state = SimpleNamespace(
            waiting=None,
            running=SimpleNamespace(started_at=datetime.now(timezone.utc)),
            terminated=None,
        )
    elif unsafe == "replaced-pod":
        pod.metadata.uid = str(uuid4())
    elif unsafe == "wrong-annotation":
        pod.metadata.annotations[WORKSPACE_CREATION_RESERVATION_ANNOTATION] = str(
            uuid4()
        )
    elif unsafe == "wrong-owner":
        pod.metadata.labels[owner.label_key] = str(uuid4())
    elif unsafe == "missing-finalizer":
        pod.metadata.finalizers = []
    elif unsafe == "claimed-intent":
        intent_id = await db.fetchval(
            "SELECT id FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind='job' AND owner_id=$1 AND scope='workspace_container'",
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
    assert await fixtures._open_authority(db, job) == {"reservations": 1, "intents": 1}
    assert cluster.pod_deletes == 0
    assert "pod" in cluster.objects
    assert "pvc" in cluster.objects
