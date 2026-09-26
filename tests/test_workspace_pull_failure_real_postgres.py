"""A pull-failed Job leaves no Pod, Service or finalizer behind (Task 11a).

Real PostgreSQL with the full migration chain, so the reservation and cleanup
intent tables, the envelope checks and the terminal-owner trigger are the
production ones. Only Kubernetes is faked, and the fake follows the kubelet
for a Pod whose image never pulls: the container stays waiting, deletion marks
it terminated (``ContainerStatusUnknown``), and the object stays until the
process-zero finalizer is released.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from kubernetes.client.exceptions import ApiException

from orchestrator.database.migrate import run_migrations
from orchestrator.services import container_provisioner as provisioner_module
from orchestrator.services.container_provisioner import (
    STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER,
    ContainerProvisioner,
)
from orchestrator.services.sandbox_workspace_settings import SandboxSettings
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import _b09_control_seams as control_seams
from tests import test_non_pinned_workspace_lifecycle_real_postgres as lifecycle_tests
from tests.test_container_provisioner import (
    _pod_from_manifest,
    _pvc_from_manifest,
    _service_from_manifest,
)
from tests.test_sandbox_workspace_provisioner import stub_plan_inputs

pg_dsn = lifecycle_tests.pg_dsn
db = lifecycle_tests.db

IMAGE = "registry.example/team/does-not-exist:1"
PULL_FAILURE = (
    f"Workspace image {IMAGE} could not be pulled: InvalidImageName (bad ref)"
)


# Deliberately overrides the imported module's fixture of the same name: its
# `db` fixture requests `_schema_applied` by name, and this module needs the full
# migration chain (terminal-owner trigger, reservation and intent tables).
@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    async with asyncpg.create_pool(pg_dsn, min_size=1, max_size=2) as pool:
        await run_migrations(
            pool,
            Path(__file__).resolve().parents[1]
            / "src/orchestrator/database/migrations/app",
        )


def _missing():
    return ApiException(status=404, reason="Not Found")


def _precondition(body):
    return ((body or {}).get("preconditions") or {}).get("uid")


class NeverPullingCluster:
    """One owner's objects; the workspace image never pulls."""

    def __init__(self):
        self.objects: dict[str, SimpleNamespace] = {}
        self.pod_deletes = 0

    def _read(self, kind, name):
        current = self.objects.get(kind)
        if current is None or current.metadata.name != name:
            raise _missing()
        return current

    def _delete(self, kind, name, body):
        current = self._read(kind, name)
        if _precondition(body) not in {None, current.metadata.uid}:
            raise ApiException(status=409, reason="Conflict")
        del self.objects[kind]

    # PersistentVolumeClaim
    def create_namespaced_persistent_volume_claim(self, *, body, **_):
        if "pvc" in self.objects:
            raise ApiException(status=409, reason="AlreadyExists")
        self.objects["pvc"] = _pvc_from_manifest(body, uid=str(uuid4()))
        return self.objects["pvc"]

    def read_namespaced_persistent_volume_claim(self, *, name, **_):
        return self._read("pvc", name)

    def delete_namespaced_persistent_volume_claim(self, *, name, body=None, **_):
        self._delete("pvc", name, body)

    # Service
    def create_namespaced_service(self, *, body, **_):
        if "service" in self.objects:
            raise ApiException(status=409, reason="AlreadyExists")
        self.objects["service"] = _service_from_manifest(body, uid=str(uuid4()))
        return self.objects["service"]

    def read_namespaced_service(self, *, name, **_):
        return self._read("service", name)

    def delete_namespaced_service(self, *, name, body=None, **_):
        self._delete("service", name, body)

    # ConfigMap (these Jobs carry no seed)
    def read_namespaced_config_map(self, **_):
        raise _missing()

    # Pod
    def create_namespaced_pod(self, *, body, **_):
        if "pod" in self.objects:
            raise ApiException(status=409, reason="AlreadyExists")
        pod = _pod_from_manifest(body, uid=str(uuid4()), phase="Pending")
        pod.metadata.finalizers = list(body["metadata"].get("finalizers") or [])
        pod.metadata.creation_timestamp = datetime.now(timezone.utc)
        (status,) = pod.status.container_statuses
        status.ready = False
        status.restart_count = 0
        status.state = SimpleNamespace(
            waiting=SimpleNamespace(reason="InvalidImageName", message="bad ref"),
            running=None,
            terminated=None,
        )
        self.objects["pod"] = pod
        return pod

    def read_namespaced_pod(self, *, name, **_):
        return self._read("pod", name)

    def delete_namespaced_pod(self, *, name, body=None, **_):
        pod = self._read("pod", name)
        if _precondition(body) not in {None, pod.metadata.uid}:
            raise ApiException(status=409, reason="Conflict")
        self.pod_deletes += 1
        pod.metadata.deletion_timestamp = datetime.now(timezone.utc)
        # The kubelet's TerminatePod: a container it never found is reported
        # terminated so the Pod can reach a terminal phase.
        pod.status.phase = "Failed"
        for status in pod.status.container_statuses:
            status.state = SimpleNamespace(
                waiting=None,
                running=None,
                terminated=SimpleNamespace(
                    exit_code=137, reason="ContainerStatusUnknown", started_at=None
                ),
            )
        if not pod.metadata.finalizers:
            del self.objects["pod"]

    def patch_namespaced_pod(self, *, name, body, **_):
        pod = self._read("pod", name)
        for op in body:
            path = op["path"]
            if op["op"] == "test":
                if path == "/metadata/uid":
                    observed = pod.metadata.uid
                else:
                    observed = pod.metadata.finalizers[int(path.rsplit("/", 1)[-1])]
                if observed != op["value"]:
                    raise ApiException(status=422, reason="test failed")
            elif op["op"] == "remove" and path.startswith("/metadata/finalizers/"):
                del pod.metadata.finalizers[int(path.rsplit("/", 1)[-1])]
            else:
                raise AssertionError(f"unexpected patch {op}")
        if pod.metadata.deletion_timestamp is not None and not pod.metadata.finalizers:
            del self.objects["pod"]
        return pod


def _provisioner(monkeypatch, db, cluster):
    p = ContainerProvisioner()
    p._db = db
    p._k8s_available = True
    p._namespace = "agent-workspaces"
    p._storage_class = "test-storage"
    p._pvc_enabled = True
    p._core_api = cluster
    stub_plan_inputs(monkeypatch, p)
    monkeypatch.setattr(
        provisioner_module,
        "resolve_sandbox_settings",
        AsyncMock(return_value=SandboxSettings(image=IMAGE)),
    )
    for name in ("open_interval", "close_interval"):
        monkeypatch.setattr(
            provisioner_module.workspace_metering, name, AsyncMock(return_value=None)
        )
    return p


async def _job(db):
    job = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs (id, description, status) "
            "VALUES ($1, 'pull failure', 'created')",
            job,
        )
    return job


async def _workspace(db, job):
    async with db.acquire() as conn:
        raw = await conn.fetchval(
            "SELECT context->'workspace_container' FROM jobs WHERE id=$1", job
        )
    return json.loads(raw) if isinstance(raw, str) else raw


async def _open_authority(db, job):
    async with db.acquire() as conn:
        return {
            "reservations": await conn.fetchval(
                "SELECT count(*) FROM managed_repository_workspace_creation_reservations "
                "WHERE owner_id=$1 AND settled_at IS NULL",
                job,
            ),
            "intents": await conn.fetchval(
                "SELECT count(*) FROM managed_repository_workspace_cleanup_intents "
                "WHERE owner_id=$1 AND settled_at IS NULL",
                job,
            ),
        }


async def _fail_like_the_dispatcher(db, job):
    workspace = await _workspace(db, job)
    await db.update_job_status(
        str(job),
        status="failed",
        error_message=f"Workspace container failed: {workspace['error']}",
    )


@pytest.mark.asyncio
async def test_a_pull_failed_job_is_cleaned_up_and_deletable(db, monkeypatch):
    job = await _job(db)
    owner = WorkspaceOwner.job(str(job))
    cluster = NeverPullingCluster()
    p = _provisioner(monkeypatch, db, cluster)

    assert await p.create_workspace(owner) is False

    # The creation closed its generation on the exact never-started Pod and
    # kept the reason for the Job. Nothing is deleted in the request itself.
    workspace = await _workspace(db, job)
    assert workspace["error"] == PULL_FAILURE
    assert workspace["_runtime_incarnation"] == cluster.objects["pod"].metadata.uid
    assert set(cluster.objects) == {"pvc", "service", "pod"}
    assert await _open_authority(db, job) == {"reservations": 0, "intents": 0}

    # The dispatcher fails the Job; its terminal transition admits the Job's
    # ordinary terminal cleanup instead of wedging a cancelled reservation.
    await _fail_like_the_dispatcher(db, job)
    assert (await _workspace(db, job))["status"] == "retiring_process_zero"
    assert await _open_authority(db, job) == {"reservations": 0, "intents": 1}

    # One lifecycle sweep retires the Pod through its finalizer and reclaims
    # the Service and the volume. No SSH attestation is involved.
    counts = await p.reconcile_pending_workspace_cleanup_intents(limit=25)
    assert counts["settled"] == 1
    assert cluster.objects == {}
    assert cluster.pod_deletes == 1
    assert (await _workspace(db, job))["status"] == "deleted"
    assert await _open_authority(db, job) == {"reservations": 0, "intents": 0}

    # DELETE /api/jobs/{id}: nothing is left to retire, and the row may go.
    monkeypatch.setattr(provisioner_module, "container_provisioner", p)
    import orchestrator.main as main

    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    assert await control_seams.archive_and_cleanup_workspace(str(job)) == []
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM jobs WHERE id=$1", job)
        assert await conn.fetchval("SELECT count(*) FROM jobs WHERE id=$1", job) == 0


@pytest.mark.asyncio
async def test_finalizer_is_held_until_every_container_is_terminal(db, monkeypatch):
    """Settling on a never-started Pod never skips the kubelet's terminal proof."""

    job = await _job(db)
    owner = WorkspaceOwner.job(str(job))
    cluster = NeverPullingCluster()
    p = _provisioner(monkeypatch, db, cluster)
    patches = []
    real_patch = cluster.patch_namespaced_pod

    def observed_patch(**kwargs):
        pod = cluster.objects["pod"]
        patches.append(
            (
                pod.metadata.deletion_timestamp is not None,
                [status.state.terminated for status in pod.status.container_statuses],
                list(pod.metadata.finalizers),
            )
        )
        return real_patch(**kwargs)

    cluster.patch_namespaced_pod = observed_patch

    assert await p.create_workspace(owner) is False
    await _fail_like_the_dispatcher(db, job)
    assert (await p.reconcile_pending_workspace_cleanup_intents(limit=25))[
        "settled"
    ] == 1

    ((deleting, terminated, finalizers),) = patches
    assert deleting is True
    assert all(state is not None for state in terminated)
    assert finalizers == [STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER]
    async with db.acquire() as conn:
        receipts = await conn.fetchval(
            "SELECT count(*) FROM managed_repository_process_zero_receipts "
            "WHERE owner_id=$1",
            job,
        )
    assert receipts == 1


@pytest.mark.asyncio
async def test_a_pull_failed_job_can_be_deleted_before_any_sweep(db, monkeypatch):
    """DELETE /api/jobs/{id} right after the failure needs no SSH attestation."""

    job = await _job(db)
    owner = WorkspaceOwner.job(str(job))
    cluster = NeverPullingCluster()
    p = _provisioner(monkeypatch, db, cluster)
    p.attest_workspace_runtime = AsyncMock(
        side_effect=AssertionError("a never-started Pod has no SSH endpoint")
    )
    monkeypatch.setattr(provisioner_module, "container_provisioner", p)
    import orchestrator.main as main

    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)

    assert await p.create_workspace(owner) is False
    await _fail_like_the_dispatcher(db, job)

    assert await control_seams.archive_and_cleanup_workspace(str(job)) == [
        "k8s workspace released"
    ]
    assert cluster.objects == {}
    assert (await _workspace(db, job))["status"] == "deleted"
    assert await _open_authority(db, job) == {"reservations": 0, "intents": 0}
    p.attest_workspace_runtime.assert_not_awaited()
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM jobs WHERE id=$1", job)


async def _interrupted_pull(db, monkeypatch, *, kept_volume=False):
    job = await _job(db)
    owner = WorkspaceOwner.job(str(job))
    cluster = NeverPullingCluster()
    p = _provisioner(monkeypatch, db, cluster)
    if kept_volume:
        cluster.create_namespaced_persistent_volume_claim(
            body={
                "metadata": {
                    "name": f"pvc-{owner.pod_name}",
                    "namespace": p._namespace,
                    "labels": {
                        "app": "srw-workspace",
                        "srw/component": "workspace-pvc",
                        "srw.io/component": "agent-workspace",
                        owner.label_key: owner.id,
                    },
                },
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "storageClassName": p._storage_class,
                },
            }
        )
    pulling = asyncio.Event()

    async def wait_during_pull(*_args, **_kwargs):
        # Pause only the readiness wait: creation, UID capture, owner
        # projection and the advisory mutation guard are production code.
        (status,) = cluster.objects["pod"].status.container_statuses
        status.state.waiting.reason = "ContainerCreating"
        status.state.waiting.message = "Pulling image"
        pulling.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(p, "_wait_for_ready", wait_during_pull)
    creator = asyncio.create_task(p.create_workspace(owner))
    try:
        await asyncio.wait_for(pulling.wait(), timeout=10)
        runtime_uid = cluster.objects["pod"].metadata.uid
        assert (await _workspace(db, job))["_runtime_incarnation"] == runtime_uid
        assert await _open_authority(db, job) == {"reservations": 1, "intents": 0}

        # This refusal is distinct from the durable two-owner deadlock below:
        # cancellation cannot commit while the original creator holds its
        # cross-replica physical-mutation guard.
        with pytest.raises(
            asyncpg.SerializationError, match="Workspace mutation is still in progress"
        ):
            await db.cancel_job(str(job))
        async with db.acquire() as conn:
            assert (
                await conn.fetchval("SELECT status::text FROM jobs WHERE id=$1", job)
                == "created"
            )
        assert await _open_authority(db, job) == {"reservations": 1, "intents": 0}
    finally:
        creator.cancel()
        with pytest.raises(asyncio.CancelledError):
            await creator
    return job, owner, cluster, p, runtime_uid


@pytest.mark.asyncio
async def test_cancel_after_interrupted_image_pull_settles_both_cleanup_owners(
    db, monkeypatch
):
    """A creator restart during a pull must not strand terminal cancellation."""

    job, owner, cluster, p, runtime_uid = await _interrupted_pull(db, monkeypatch)

    # Task interruption models the creator stopping after all accepted UIDs
    # are durable but before readiness/settlement. It releases the guard; the
    # Pod is still pulling and cancellation now commits through the real
    # terminal-owner trigger.
    assert await db.cancel_job(str(job))
    assert await _open_authority(db, job) == {"reservations": 1, "intents": 1}
    async with db.acquire() as conn:
        reservation = await conn.fetchrow(
            "SELECT phase, runtime_incarnation, cancel_requested_at FROM "
            "managed_repository_workspace_creation_reservations "
            "WHERE owner_id=$1 AND settled_at IS NULL",
            job,
        )
        intent = await conn.fetchrow(
            "SELECT * FROM "
            "managed_repository_workspace_cleanup_intents "
            "WHERE owner_id=$1 AND settled_at IS NULL",
            job,
        )
    assert reservation["phase"] == "runtime_bound"
    assert reservation["cancel_requested_at"] is not None
    assert str(reservation["runtime_incarnation"]) == runtime_uid
    assert str(intent["runtime_incarnation"]) == runtime_uid
    assert intent["capture_complete"] is False

    assert await p.reconcile_pending_workspace_creation_reservations(limit=25) == {
        "handed_off": 1,
        "aborted": 0,
        "retryable": 0,
    }
    assert await _open_authority(db, job) == {"reservations": 0, "intents": 1}
    assert cluster.pod_deletes == 0
    async with db.acquire() as conn:
        assert (
            await conn.fetchrow(
                "SELECT * FROM managed_repository_workspace_cleanup_intents WHERE id=$1",
                intent["id"],
            )
            == intent
        )

    # A fresh provisioner models restart: no caller-local state may be needed.
    p = _provisioner(monkeypatch, db, cluster)
    sweeps = [await p.reconcile_pending_workspace_cleanup_intents(limit=25)]
    p = _provisioner(monkeypatch, db, cluster)
    sweeps.append(await p.reconcile_pending_workspace_cleanup_intents(limit=25))
    remaining = await _open_authority(db, job)
    assert remaining == {"reservations": 0, "intents": 0}, (
        f"same-runtime cancellation did not converge: sweeps={sweeps}, "
        f"open_authority={remaining}, objects={sorted(cluster.objects)}, "
        f"pod_deletes={cluster.pod_deletes}"
    )
    assert cluster.objects == {}
    assert cluster.pod_deletes == 1
    assert (await _workspace(db, job))["status"] == "deleted"
    assert sweeps == [
        {"settled": 1, "superseded": 0, "retryable": 0},
        {"settled": 0, "superseded": 0, "retryable": 0},
    ]
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_workspace_cleanup_intents "
                "WHERE owner_id=$1",
                job,
            )
            == 1
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM managed_repository_process_zero_receipts "
                "WHERE owner_id=$1 AND runtime_incarnation=$2",
                job,
                runtime_uid,
            )
            == 1
        )
        await conn.execute("DELETE FROM jobs WHERE id=$1", job)


@pytest.mark.asyncio
async def test_interrupted_creation_without_terminal_intent_keeps_legacy_handoff(
    db, monkeypatch
):
    job, owner, cluster, p, runtime_uid = await _interrupted_pull(db, monkeypatch)

    cancellation = await p.request_workspace_creation_cancellation(
        owner, target_disposition="suspended", reclaim_shared_resources=False
    )
    assert cancellation["reconciliation_outcome"] == "handed_off"
    assert await _open_authority(db, job) == {"reservations": 0, "intents": 1}
    async with db.acquire() as conn:
        intent = await conn.fetchrow(
            "SELECT * FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_id=$1",
            job,
        )
    assert str(intent["runtime_incarnation"]) == runtime_uid
    assert intent["resource_policy"] == "preserve"
    assert intent["target_disposition"] == "suspended"
    assert intent["capture_complete"] is True

    assert await p.reconcile_pending_workspace_cleanup_intents(limit=25) == {
        "settled": 1,
        "superseded": 0,
        "retryable": 0,
    }
    assert set(cluster.objects) == {"pvc"}
    assert (await _workspace(db, job))["status"] == "suspended"


@pytest.mark.asyncio
@pytest.mark.parametrize("unsafe", ["kept-volume", "ran-container", "no-finalizer"])
async def test_interrupted_pull_reuse_keeps_unproven_data_and_runtime_held(
    db, monkeypatch, unsafe
):
    job, owner, cluster, p, runtime_uid = await _interrupted_pull(
        db, monkeypatch, kept_volume=unsafe == "kept-volume"
    )
    if unsafe == "ran-container":
        cluster.objects["pod"].status.container_statuses[0].restart_count = 1
    elif unsafe == "no-finalizer":
        cluster.objects["pod"].metadata.finalizers = []
    assert await db.cancel_job(str(job))

    counts = await p.reconcile_pending_workspace_cleanup_intents(limit=25)
    assert counts["settled"] == 0
    assert await _open_authority(db, job) == {"reservations": 1, "intents": 1}
    assert set(cluster.objects) == {"pvc", "service", "pod"}
    assert cluster.pod_deletes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "conflict",
    ["successor", "capture", "claimed", "terminal-token", "stale-claim"],
)
async def test_cancelled_creation_reuse_refuses_conflicting_or_stale_authority(
    db, monkeypatch, conflict
):
    job, owner, cluster, p, runtime_uid = await _interrupted_pull(db, monkeypatch)
    assert await db.cancel_job(str(job))
    reservation = await db.request_managed_repository_workspace_creation_cancellation(
        str(job),
        owner_kind="job",
        scope="workspace_container",
        target_disposition="deleted",
        reclaim_shared_resources=True,
        claimant="cancel-test",
    )
    assert reservation is not None
    async with db.acquire() as conn:
        if conflict == "successor":
            await conn.execute(
                "UPDATE managed_repository_workspace_cleanup_intents "
                "SET runtime_incarnation=$2, pod_uid=$2 WHERE owner_id=$1",
                job,
                uuid4(),
            )
        elif conflict == "capture":
            await conn.execute(
                "UPDATE managed_repository_workspace_cleanup_intents "
                "SET pvc_uid=$2, capture_complete=TRUE, resources_captured_at=now(), "
                "phase='captured' WHERE owner_id=$1",
                job,
                uuid4(),
            )
        elif conflict == "claimed":
            intent_id = await conn.fetchval(
                "SELECT id FROM managed_repository_workspace_cleanup_intents "
                "WHERE owner_id=$1",
                job,
            )
        elif conflict == "terminal-token":
            await conn.execute(
                "UPDATE managed_repository_workspace_cleanup_intents "
                "SET terminal_queue_token=1 WHERE owner_id=$1",
                job,
            )
    if conflict == "claimed":
        assert (
            await db.claim_managed_repository_workspace_cleanup_intent(
                str(intent_id), claimant="other-cleaner", lease_seconds=300
            )
            is not None
        )

    async with db.acquire() as conn:
        before = await conn.fetchrow(
            "SELECT row_to_json(i)::text AS intent, row_to_json(r)::text AS reservation "
            "FROM managed_repository_workspace_cleanup_intents i JOIN "
            "managed_repository_workspace_creation_reservations r "
            "ON r.owner_id=i.owner_id WHERE r.owner_id=$1",
            job,
        )
    result = await db.convert_cancelled_workspace_creation_to_cleanup_intent(
        str(job),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=int(reservation["reservation_generation"]),
        claimant="cancel-test",
        claim_token=int(reservation["claim_token"]) - (conflict == "stale-claim"),
        runtime_incarnation=runtime_uid,
        allow_existing_terminal_intent=True,
    )
    assert result is None
    async with db.acquire() as conn:
        after = await conn.fetchrow(
            "SELECT row_to_json(i)::text AS intent, row_to_json(r)::text AS reservation "
            "FROM managed_repository_workspace_cleanup_intents i JOIN "
            "managed_repository_workspace_creation_reservations r "
            "ON r.owner_id=i.owner_id WHERE r.owner_id=$1",
            job,
        )
    assert before == after
    assert cluster.pod_deletes == 0


@pytest.mark.asyncio
async def test_a_pull_failure_over_a_kept_volume_settles_nothing(db, monkeypatch):
    """A kept claim may hold user data: the failed creation leaves it all alone."""

    job = await _job(db)
    owner = WorkspaceOwner.job(str(job))
    cluster = NeverPullingCluster()
    p = _provisioner(monkeypatch, db, cluster)
    # A volume kept from an earlier runtime (suspension keeps the PVC).
    cluster.create_namespaced_persistent_volume_claim(
        body={
            "metadata": {
                "name": f"pvc-{owner.pod_name}",
                "namespace": p._namespace,
                "labels": {
                    "app": "srw-workspace",
                    "srw/component": "workspace-pvc",
                    "srw.io/component": "agent-workspace",
                    owner.label_key: owner.id,
                },
            },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": p._storage_class,
            },
        }
    )
    kept = cluster.objects["pvc"].metadata.uid

    assert await p.create_workspace(owner) is False

    assert (await _workspace(db, job))["error"] == PULL_FAILURE
    assert set(cluster.objects) == {"pvc", "service", "pod"}
    assert cluster.objects["pvc"].metadata.uid == kept
    assert cluster.pod_deletes == 0
    # The generation stays open, exactly as before this fix.
    assert await _open_authority(db, job) == {"reservations": 1, "intents": 0}
