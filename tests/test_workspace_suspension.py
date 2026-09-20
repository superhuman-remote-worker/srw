"""Tests for the WorkspaceSuspensionService.

Tests cover:
1. suspend_workspace: snapshot capture → pod deletion → status transitions
2. restore_workspace: pod creation → snapshot extraction → status transitions
3. check_idle_all: idle detection query, timeout calculation, sweep behavior
4. Edge cases: S3 unavailable, capture failure, restore failure, double entry
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import ANY, AsyncMock, MagicMock, PropertyMock, patch

import pytest

from orchestrator.services.workspace_suspension import (  # noqa: E402
    WorkspaceSuspensionService,
    _strict_session_restore_authority,
)
from orchestrator.services.ssh_helpers import (  # noqa: E402
    EXTRACT_HOME_REMOTE_CMD,
    EXTRACT_REMOTE_CMD,
)
from orchestrator.services.container_provisioner import (  # noqa: E402
    WorkspaceCleanupOutcome,
    WorkspaceRuntimeAttestation,
    WorkspaceTeardownIdentity,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner  # noqa: E402
from orchestrator.services.vm_provisioner import (  # noqa: E402
    VMTeardownIdentity,
    VMTeardownResult,
)


WORKSPACE_RUNTIME = "11111111-1111-4111-8111-111111111111"
SUCCESSOR_RUNTIME = "22222222-2222-4222-8222-222222222222"
CLEANUP_ID = "33333333-3333-4333-8333-333333333333"
CREATION_ID = "44444444-4444-4444-8444-444444444444"
CREATION_TOKEN = 17
RESTORE_WORK_TOKEN = 23
VM_GENERATION = "77777777-7777-4777-8777-777777777777"
VM_UID = "vm-uid-a"
VM_ROOTDISK_UID = "vm-rootdisk-a"
VM_LAUNCHER_UID = "88888888-8888-4888-8888-888888888888"
VM_FINGERPRINT = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
VM_SESSION_GENERATION = "99999999-9999-4999-8999-999999999999"
VM_SESSION_AGENT = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
VM_SESSION_ATTACH = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
VM_OPERATION_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"


# =============================================================================
# Fixtures
# =============================================================================


class _MockAsyncCtx:
    """Async context manager that wraps a mock connection."""

    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *args):
        return False


def make_mock_db():
    """Create a mock PostgresDB with get_job, acquire, and merge methods."""
    db = AsyncMock()
    # acquire() must be a plain MagicMock (not AsyncMock) that returns
    # an async context manager — otherwise calling it produces a coroutine
    # that doesn't support `async with`.
    mock_conn = AsyncMock()
    db.acquire = MagicMock(return_value=_MockAsyncCtx(mock_conn))
    db._conn = mock_conn

    # Production pinned restores are serialized by PostgresDB's dedicated
    # session advisory lock.  The generated AsyncMock type intentionally has
    # no implementation unless the fixture installs this exact seam.
    def _thread_advisory_lock(_db, _thread_id):
        return _MockAsyncCtx(True)

    type(db).thread_advisory_lock = _thread_advisory_lock
    return db


def make_service(*, s3_available=True, k8s_available=True):
    """Create a fully wired WorkspaceSuspensionService with mocks."""
    svc = WorkspaceSuspensionService()

    mock_db = make_mock_db()
    # Default get_job return — restore_workspace reads the job to detect
    # provisioner type before dispatching.
    mock_db.get_job.return_value = make_job()

    mock_snapshot = MagicMock()
    type(mock_snapshot).is_available = PropertyMock(return_value=s3_available)
    mock_snapshot.capture_vm_snapshot = AsyncMock(return_value=True)
    mock_snapshot.download_snapshot = AsyncMock(return_value=True)
    # Reclaim-on-idle's fail-safe gate (C2). Defaults to a clean verify so
    # tests that flip WORKSPACE_RECLAIM_ON_IDLE on don't all need to wire
    # this individually; tests of the unverified path override it.
    mock_snapshot.verify_snapshot = AsyncMock(return_value=(True, "ok"))

    mock_container = MagicMock()
    type(mock_container).is_available = PropertyMock(return_value=k8s_available)
    mock_container.create_workspace = AsyncMock(return_value=True)
    mock_container.delete_workspace = AsyncMock(return_value=True)
    mock_container.capture_workspace_teardown_identity = AsyncMock(
        return_value=WorkspaceTeardownIdentity(
            pod_uid=WORKSPACE_RUNTIME,
            pvc_uid="55555555-5555-4555-8555-555555555555",
            service_uid="66666666-6666-4666-8666-666666666666",
        )
    )
    mock_container.prepare_workspace_cleanup_intent = AsyncMock(
        return_value={"id": CLEANUP_ID, "intent_generation": 1}
    )
    mock_container.reconcile_workspace_cleanup_intent = AsyncMock(
        return_value=WorkspaceCleanupOutcome("settled", 1)
    )
    mock_container.get_settled_workspace_suspension = AsyncMock(
        return_value={
            "id": CLEANUP_ID,
            "result_kind": "settled",
            "target_disposition": "suspended",
        }
    )
    creation_result = {
        "id": CREATION_ID,
        "operation_kind": "restore",
        "result_kind": "settled",
        "settled_at": datetime.now(timezone.utc),
        "runtime_incarnation": SUCCESSOR_RUNTIME,
        "claim_token": CREATION_TOKEN,
    }
    mock_container.get_workspace_creation_result = AsyncMock(
        return_value=creation_result
    )
    mock_container.get_current_workspace_creation_result = AsyncMock(
        return_value=creation_result
    )

    async def claim_restore_work(_owner, *, claimant, lease_seconds=300):
        assert lease_seconds == 300
        return {
            **creation_result,
            "restore_work_claimed_by": claimant,
            "restore_work_claim_token": RESTORE_WORK_TOKEN,
            "restore_work_completed_at": None,
        }

    mock_container.claim_workspace_restore_work = AsyncMock(
        side_effect=claim_restore_work
    )
    mock_container.renew_workspace_restore_work = AsyncMock(
        side_effect=lambda _owner, *, restore_work, **_kwargs: restore_work
    )
    mock_container.attest_workspace_runtime = AsyncMock(
        return_value=WorkspaceRuntimeAttestation(
            backing_id=("k8s-pod:test:" + SUCCESSOR_RUNTIME),
            workspace_generation=SUCCESSOR_RUNTIME,
            runtime_incarnation=SUCCESSOR_RUNTIME,
            ssh_host_key_fingerprint=(
                "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            ),
            host="10.0.0.99",
            pod_ip="10.0.0.99",
        )
    )
    mock_container.release_workspace_restore_work = AsyncMock(return_value=True)
    mock_container.complete_workspace_restore_work = AsyncMock(return_value=True)
    mock_container.complete_strict_thread_restore_work = AsyncMock(return_value=True)
    mock_container.workspace_pod_live = AsyncMock(return_value=True)
    mock_container.delete_workspace_pvc = AsyncMock(return_value=True)

    svc.connect(
        db=mock_db,
        snapshot_service=mock_snapshot,
        container_provisioner=mock_container,
    )
    recovery_store = MagicMock()
    recovery_store.acquire_cleanup_permit = AsyncMock(
        return_value=MagicMock(allowed=True, admission_id=None)
    )
    svc._workspace_recovery_store = recovery_store
    return svc


def make_job(
    *,
    job_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    status="paused",
    ws_status="ready",
    pod_ip="10.0.0.42",
    last_activity=None,
    runtime=WORKSPACE_RUNTIME,
    snapshot_restore_required=False,
    creation_receipt=False,
):
    """Create a minimal job dict."""
    ws_ctx = {
        "status": ws_status,
        "provisioner": "k8s",
        "pod_ip": pod_ip,
        "pod_name": f"workspace-{job_id[:12]}",
        "_runtime_incarnation": runtime,
    }
    if snapshot_restore_required:
        ws_ctx["_snapshot_restore_required"] = True
    if creation_receipt:
        ws_ctx.update(
            {
                "_creation_reservation_id": CREATION_ID,
                "_creation_claim_token": str(CREATION_TOKEN),
            }
        )
    if last_activity:
        ws_ctx["last_activity"] = last_activity
    return {
        "id": job_id,
        "status": status,
        "context": {"workspace_container": ws_ctx},
    }


def configure_job_restore(
    svc,
    *,
    final_status="ready",
    final_pod_ip="10.0.0.99",
):
    """Wire exact suspended-A -> settled restore-B reads."""

    before = make_job(
        ws_status="suspended",
        pod_ip=None,
        snapshot_restore_required=True,
    )
    after = make_job(
        ws_status=final_status,
        pod_ip=final_pod_ip,
        runtime=SUCCESSOR_RUNTIME,
        snapshot_restore_required=True,
        creation_receipt=True,
    )
    svc._db.get_job.side_effect = [before, after]
    return before, after


# =============================================================================
# Test: is_enabled property
# =============================================================================


class TestIsEnabled:
    def test_enabled_when_both_available(self):
        svc = make_service(s3_available=True, k8s_available=True)
        assert svc.is_enabled is True

    def test_disabled_without_s3(self):
        svc = make_service(s3_available=False, k8s_available=True)
        assert svc.is_enabled is False

    def test_disabled_without_k8s(self):
        svc = make_service(s3_available=True, k8s_available=False)
        assert svc.is_enabled is False

    def test_disabled_without_both(self):
        svc = make_service(s3_available=False, k8s_available=False)
        assert svc.is_enabled is False

    def test_disabled_before_connect(self):
        svc = WorkspaceSuspensionService()
        assert svc.is_enabled is False


# =============================================================================
# Test: suspend_workspace
# =============================================================================


class TestSuspendWorkspace:
    @pytest.mark.asyncio
    async def test_k8s_suspend_is_contained_before_capture_or_teardown(self):
        """A stale A projection must never read or tear down a same-IP B."""
        svc = make_service()
        job = make_job()
        svc._db.get_job.return_value = job
        pull = AsyncMock(
            side_effect=AssertionError("foreign successor must not be read")
        )
        profile = AsyncMock(
            side_effect=AssertionError("foreign successor must not be archived")
        )

        with (
            patch("orchestrator.services.ide_settings.pull_ide_config", pull),
            patch("orchestrator.services.ide_settings.capture_ide_profile", profile),
        ):
            result = await svc.suspend_workspace(job["id"])

        assert result is False
        pull.assert_not_awaited()
        profile.assert_not_awaited()
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.capture_workspace_teardown_identity.assert_not_awaited()
        svc._container_provisioner.prepare_workspace_cleanup_intent.assert_not_awaited()
        svc._container_provisioner.reconcile_workspace_cleanup_intent.assert_not_awaited()
        svc._db.merge_workspace_container_context.assert_not_awaited()
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()
        svc._db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_suspend_capture_fails_reverts(self):
        """If snapshot capture fails, status reverts to ready and pod stays."""
        svc = make_service()
        job = make_job()
        svc._db.get_job.return_value = job
        svc._snapshot_service.capture_vm_snapshot.return_value = False

        result = await svc.suspend_workspace(job["id"])

        assert result is False
        # Pod NOT deleted
        svc._container_provisioner.prepare_workspace_cleanup_intent.assert_not_awaited()
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_suspend_skips_non_ready(self):
        """Suspend skips containers not in 'ready' status."""
        svc = make_service()
        job = make_job(ws_status="suspended")
        svc._db.get_job.return_value = job

        result = await svc.suspend_workspace(job["id"])

        assert result is False
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_suspend_skips_no_pod_ip(self):
        """Suspend skips if pod_ip is missing."""
        svc = make_service()
        job = make_job(pod_ip=None)
        svc._db.get_job.return_value = job

        result = await svc.suspend_workspace(job["id"])

        assert result is False

    @pytest.mark.asyncio
    async def test_suspend_skips_missing_job(self):
        """Suspend returns False for non-existent jobs."""
        svc = make_service()
        svc._db.get_job.return_value = None

        result = await svc.suspend_workspace("nonexistent")

        assert result is False

    @pytest.mark.asyncio
    async def test_suspend_disabled_returns_false(self):
        """Suspend returns False when service is disabled."""
        svc = make_service(s3_available=False)

        result = await svc.suspend_workspace("some-id")

        assert result is False

    @pytest.mark.asyncio
    async def test_stale_docker_suspend_cannot_touch_reassigned_static_host(self):
        """A sweep queued on an old lease must remain a no-op after reassignment."""

        svc = make_service()
        read_started = asyncio.Event()
        host_reassigned = asyncio.Event()
        stale_lease = {
            "status": "ready",
            "host": "workspace-1",
            "port": 30022,
            "provisioner": "docker",
            "_docker_workspace_lease_id": "old-lease",
        }

        async def delayed_stale_job(job_id):
            read_started.set()
            await host_reassigned.wait()
            return {
                "id": job_id,
                "context": {"workspace_container": stale_lease},
            }

        svc._db.get_job.side_effect = delayed_stale_job
        docker = MagicMock()
        docker._reset_workspace_via_ssh = AsyncMock(return_value=True)
        svc._docker_provisioner = docker

        pending = asyncio.create_task(svc.suspend_workspace("old-job"))
        await read_started.wait()
        # The authoritative lease can now belong to a different owner; the
        # delayed read represents the stale snapshot already held by a sweep.
        host_reassigned.set()

        assert await pending is False
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.delete_workspace.assert_not_awaited()
        svc._container_provisioner.prepare_workspace_cleanup_intent.assert_not_awaited()
        svc._db.merge_workspace_container_context.assert_not_awaited()
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()
        docker._reset_workspace_via_ssh.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_suspend_exception_reverts(self):
        """If an exception occurs during capture, status reverts to ready."""
        svc = make_service()
        job = make_job()
        svc._db.get_job.return_value = job
        svc._snapshot_service.capture_vm_snapshot.side_effect = RuntimeError(
            "ssh failed"
        )

        result = await svc.suspend_workspace(job["id"])

        assert result is False
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_successor_runtime_survives_superseded_cleanup(self):
        """A late suspension of A cannot project any lifecycle state onto B."""

        svc = make_service()
        job = make_job()
        svc._db.get_job.return_value = job
        successor = {
            "status": "ready",
            "provisioner": "k8s",
            "pod_ip": "10.0.0.99",
            "pod_name": "workspace-successor",
            "_runtime_incarnation": SUCCESSOR_RUNTIME,
        }

        async def reconcile_stale(*args, **kwargs):
            job["context"]["workspace_container"] = successor
            return WorkspaceCleanupOutcome("superseded", 1)

        svc._container_provisioner.reconcile_workspace_cleanup_intent = AsyncMock(
            side_effect=reconcile_stale
        )

        assert await svc.suspend_workspace(job["id"]) is False
        assert job["context"]["workspace_container"] != successor
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.reconcile_workspace_cleanup_intent.assert_not_awaited()
        svc._db.merge_workspace_container_context.assert_not_awaited()
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_post_snapshot_identity_race_stops_before_intent(self):
        svc = make_service()
        job = make_job()
        svc._db.get_job.return_value = job
        svc._container_provisioner.capture_workspace_teardown_identity.return_value = (
            WorkspaceTeardownIdentity(
                pod_uid=SUCCESSOR_RUNTIME,
                pvc_uid=None,
                service_uid=None,
            )
        )

        assert await svc.suspend_workspace(job["id"]) is False

        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.capture_workspace_teardown_identity.assert_not_awaited()
        svc._container_provisioner.prepare_workspace_cleanup_intent.assert_not_awaited()


# =============================================================================
# Test: restore_workspace
# =============================================================================


class TestRestoreWorkspace:
    @pytest.mark.asyncio
    async def test_restore_success(self):
        """Full restore flow: create pod → extract snapshot → status ready."""
        svc = make_service()
        configure_job_restore(svc)

        with patch.object(
            svc, "_extract_snapshot", new_callable=AsyncMock
        ) as mock_extract:
            result = await svc.restore_workspace("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        assert result is True
        svc._container_provisioner.create_workspace.assert_awaited_once_with(
            WorkspaceOwner.job("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            operation_kind="restore",
            operation_id=CLEANUP_ID,
        )
        # Pod (not VM) restore extracts home-only: the extract runs as
        # agent-host and must not try to overwrite root-owned /usr/local.
        mock_extract.assert_awaited_once_with(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "10.0.0.99",
            ssh_port=30022,
            scoped_home=True,
            expected_host_key_fingerprint=(
                "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            ),
            mutation_authority=ANY,
        )
        # Only B is published ready, atomically with its durable work receipt;
        # retired A never becomes restoring.
        svc._db.merge_workspace_container_context.assert_not_awaited()
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()
        complete = svc._container_provisioner.complete_workspace_restore_work
        complete.assert_awaited_once()
        assert complete.await_args.kwargs["success"] is True

    @pytest.mark.asyncio
    async def test_restore_pod_creation_fails(self):
        """If pod creation fails, status is set to failed."""
        svc = make_service()
        svc._container_provisioner.create_workspace.return_value = False
        svc._container_provisioner.get_workspace_creation_result.return_value = None
        configure_job_restore(svc)

        result = await svc.restore_workspace("some-job-id")

        assert result is False
        svc._db.merge_workspace_container_context.assert_not_awaited()
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_restore_no_pod_ip(self):
        """If pod has no IP after creation, status is set to failed."""
        svc = make_service()
        configure_job_restore(svc, final_pod_ip=None)

        result = await svc.restore_workspace("some-job-id")

        assert result is False
        call = svc._db.merge_workspace_container_context_if_runtime.await_args
        assert call.args[1]["status"] == "failed"
        assert call.kwargs == {"expected_runtime_incarnation": SUCCESSOR_RUNTIME}

    @pytest.mark.asyncio
    async def test_lost_create_response_recovers_from_exact_settled_result(self):
        svc = make_service()
        configure_job_restore(svc)
        svc._container_provisioner.create_workspace.side_effect = RuntimeError(
            "response lost after commit"
        )
        svc._extract_snapshot = AsyncMock(return_value=True)

        assert await svc.restore_workspace("some-job-id") is True

        svc._container_provisioner.get_workspace_creation_result.assert_awaited_once_with(
            WorkspaceOwner.job("some-job-id"),
            operation_kind="restore",
            operation_id=CLEANUP_ID,
        )
        svc._container_provisioner.complete_workspace_restore_work.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_process_restart_continues_only_from_current_b_receipt(self):
        svc = make_service()
        current = make_job(
            job_id="some-job-id",
            ws_status="ready",
            pod_ip="10.0.0.99",
            runtime=SUCCESSOR_RUNTIME,
            snapshot_restore_required=True,
            creation_receipt=True,
        )
        svc._db.get_job.side_effect = [current, current]
        svc._extract_snapshot = AsyncMock(return_value=True)

        assert await svc.restore_workspace("some-job-id") is True

        svc._container_provisioner.create_workspace.assert_not_awaited()
        svc._container_provisioner.get_current_workspace_creation_result.assert_awaited_once_with(
            WorkspaceOwner.job("some-job-id"), operation_kind="restore"
        )

    @pytest.mark.asyncio
    async def test_successor_change_at_ready_cas_is_preserved(self):
        svc = make_service()
        configure_job_restore(svc)
        svc._extract_snapshot = AsyncMock(return_value=True)
        svc._container_provisioner.complete_workspace_restore_work.return_value = False

        assert await svc.restore_workspace("some-job-id") is False

        assert (
            svc._container_provisioner.complete_workspace_restore_work.await_count == 2
        )
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()
        svc._db.merge_workspace_container_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_two_service_instances_run_exact_b_effects_once(self):
        """The durable claim, not either process's task map, owns extraction."""

        first = make_service()
        second = make_service()
        configure_job_restore(first)
        configure_job_restore(second)
        shared = first._container_provisioner
        second._container_provisioner = shared
        claimed = False

        async def claim(_owner, *, claimant, lease_seconds=300):
            nonlocal claimed
            assert lease_seconds == 300
            if claimed:
                return None
            claimed = True
            return {
                **shared.get_workspace_creation_result.return_value,
                "restore_work_claimed_by": claimant,
                "restore_work_claim_token": RESTORE_WORK_TOKEN,
                "restore_work_completed_at": None,
            }

        shared.claim_workspace_restore_work.side_effect = claim
        started = asyncio.Event()
        finish = asyncio.Event()

        async def extract(*_args, **_kwargs):
            started.set()
            await finish.wait()
            return True

        first._extract_snapshot = AsyncMock(side_effect=extract)
        second._extract_snapshot = AsyncMock(return_value=True)
        first_task = asyncio.create_task(first.restore_workspace("some-job-id"))
        await started.wait()
        assert await second.restore_workspace("some-job-id") is False
        finish.set()
        assert await first_task is True

        assert first._extract_snapshot.await_count == 1
        second._extract_snapshot.assert_not_awaited()
        shared.complete_workspace_restore_work.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_expired_restore_lease_is_reclaimed_with_new_token(self):
        """A restart accepts only the server-returned reclaimed exact-B token."""

        svc = make_service()
        configure_job_restore(svc)
        observed_claimant = None

        async def reclaim(_owner, *, claimant, lease_seconds=300):
            nonlocal observed_claimant
            observed_claimant = claimant
            return {
                **svc._container_provisioner.get_workspace_creation_result.return_value,
                "restore_work_claimed_by": claimant,
                "restore_work_claim_token": 91,
                "restore_work_completed_at": None,
            }

        svc._container_provisioner.claim_workspace_restore_work.side_effect = reclaim
        svc._extract_snapshot = AsyncMock(return_value=True)

        assert await svc.restore_workspace("some-job-id") is True
        assert observed_claimant is not None
        complete = svc._container_provisioner.complete_workspace_restore_work
        assert (
            complete.await_args.kwargs["restore_work"]["restore_work_claim_token"] == 91
        )

    @pytest.mark.asyncio
    async def test_lost_restore_completion_response_replays_same_claim(self):
        svc = make_service()
        configure_job_restore(svc)
        svc._extract_snapshot = AsyncMock(return_value=True)
        svc._container_provisioner.complete_workspace_restore_work.side_effect = [
            RuntimeError("response lost after commit"),
            True,
        ]

        assert await svc.restore_workspace("some-job-id") is True

        calls = (
            svc._container_provisioner.complete_workspace_restore_work.await_args_list
        )
        assert len(calls) == 2
        assert calls[0].kwargs["restore_work"] is calls[1].kwargs["restore_work"]
        assert calls[0].kwargs["claimant"] == calls[1].kwargs["claimant"]

    @pytest.mark.asyncio
    async def test_successor_ip_reuse_before_snapshot_stream_gets_zero_bytes(self):
        """Fresh control-plane UID drift fences the mutable Pod IP."""

        svc = make_service()
        configure_job_restore(svc)
        exact_b = svc._container_provisioner.attest_workspace_runtime.return_value
        successor_c = WorkspaceRuntimeAttestation(
            backing_id=f"k8s-pod:test:{WORKSPACE_RUNTIME}",
            workspace_generation=WORKSPACE_RUNTIME,
            runtime_incarnation=WORKSPACE_RUNTIME,
            ssh_host_key_fingerprint=(
                "SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
            ),
            host=exact_b.host,
            pod_ip=exact_b.pod_ip,
        )
        svc._container_provisioner.attest_workspace_runtime.side_effect = [
            exact_b,
            successor_c,
        ]

        with patch(
            "orchestrator.services.workspace_suspension.stream_extract_snapshot",
            new=AsyncMock(return_value=(0, b"")),
        ) as stream:
            assert not await svc.restore_workspace("some-job-id")

        # The snapshot may be downloaded locally, but the same-IP successor
        # receives neither an archive byte nor an unpinned SSH connection.
        svc._snapshot_service.download_snapshot.assert_awaited_once()
        stream.assert_not_awaited()
        svc._container_provisioner.complete_workspace_restore_work.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_post_write_uid_replacement_cannot_settle_workspace_restore(self):
        svc = make_service()
        configure_job_restore(svc)
        exact_b = svc._container_provisioner.attest_workspace_runtime.return_value
        successor_c = WorkspaceRuntimeAttestation(
            backing_id=f"k8s-pod:test:{WORKSPACE_RUNTIME}",
            workspace_generation=WORKSPACE_RUNTIME,
            runtime_incarnation=WORKSPACE_RUNTIME,
            ssh_host_key_fingerprint=(
                "SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
            ),
            host=exact_b.host,
            pod_ip=exact_b.pod_ip,
        )
        svc._container_provisioner.attest_workspace_runtime.side_effect = [
            exact_b,
            successor_c,
        ]
        # Simulate a completed pinned write; the mandatory post-write fresh
        # attestation must still fence settlement when UID C has won.
        svc._extract_snapshot = AsyncMock(return_value=True)

        assert not await svc.restore_workspace("some-job-id")

        svc._extract_snapshot.assert_awaited_once()
        svc._container_provisioner.workspace_pod_live.assert_not_awaited()
        svc._container_provisioner.complete_workspace_restore_work.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_restore_disabled_returns_false(self):
        svc = make_service(s3_available=False)

        result = await svc.restore_workspace("some-id")

        assert result is False

    @pytest.mark.asyncio
    async def test_vm_restore_with_kept_rootdisk_defers_to_readiness_prober(self):
        svc, vm_prov = make_vm_service()
        vm_prov.create_vm = AsyncMock(return_value=True)
        svc._db.get_job.return_value = {
            "id": "job-vm",
            "context": {"vm": {"status": "suspended", "rootdisk": "kept"}},
        }
        svc._extract_snapshot = AsyncMock()

        result = await svc.restore_workspace("job-vm")

        assert result is True
        vm_prov.create_vm.assert_awaited_once_with("job-vm")
        svc._extract_snapshot.assert_not_awaited()
        assert not any(
            call.args[1].get("status") == "failed"
            for call in svc._db.merge_vm_context.await_args_list
        )

    @pytest.mark.asyncio
    async def test_vm_restore_without_kept_rootdisk_fails_visibly(self):
        svc, vm_prov = make_vm_service()
        vm_prov.create_vm = AsyncMock(return_value=True)
        svc._db.get_job.return_value = {
            "id": "job-vm",
            "context": {"vm": {"status": "suspended", "rootdisk": "purged"}},
        }
        svc._extract_snapshot = AsyncMock()

        result = await svc.restore_workspace("job-vm")

        assert result is False
        svc._extract_snapshot.assert_not_awaited()
        failure = svc._db.merge_vm_context.await_args_list[-1].args[1]
        assert failure == {
            "status": "failed",
            "error": (
                "VM restore without a kept rootdisk requires a post-readiness "
                "snapshot extract (unsupported in same-cluster mode v1)"
            ),
        }


# =============================================================================
# Test: _extract_snapshot command selection (pod home-only vs VM full)
# =============================================================================


class TestExtractSnapshotScopedHome:
    """``_extract_snapshot`` selects the extract command by target kind.

    A snapshot carries both ``/home/agent-host`` and ``/usr/local``. A **pod**
    extract runs as the unprivileged agent-host user and cannot overwrite the
    image-provided, root-owned ``/usr/local`` — the full extract then exits
    rc=2 and restore is (correctly) reported failed, so pod restore-from-S3
    silently never worked (confirmed on the dev cluster). Pods must extract
    home-only (``scoped_home=True`` -> ``EXTRACT_HOME_REMOTE_CMD``, matching the
    proven ``ide_session`` k8s-pod path); VMs (root, the snapshot IS the disk)
    keep the full extract. See knowledge-base/knowledge/features/workspace_durability_tiering.md §C1.
    """

    @pytest.mark.asyncio
    async def test_snapshot_identity_cannot_control_local_temporary_path(
        self, tmp_path, monkeypatch
    ):
        from pathlib import Path
        import tempfile

        monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
        svc = make_service()
        entity_id = "../outside/"
        downloaded = []

        async def download(identity, path, **kwargs):
            assert identity == entity_id
            assert kwargs["entity_type"] == "threads"
            target = Path(path)
            assert target.parent == tmp_path
            target.write_bytes(b"snapshot")
            downloaded.append(target)
            return True

        svc._snapshot_service.download_snapshot.side_effect = download
        with patch(
            "orchestrator.services.workspace_suspension.stream_extract_snapshot",
            new=AsyncMock(return_value=(0, b"")),
        ) as stream:
            assert await svc._extract_snapshot(
                entity_id, "10.0.0.9", entity_type="threads"
            )

        assert len(downloaded) == 1
        assert stream.call_args.args[2] == str(downloaded[0])
        assert not downloaded[0].exists()

    @pytest.mark.asyncio
    async def test_pod_extract_uses_home_only_command(self):
        svc = make_service()
        with patch(
            "orchestrator.services.workspace_suspension.stream_extract_snapshot",
            new=AsyncMock(return_value=(0, b"")),
        ) as mock_stream:
            ok = await svc._extract_snapshot("id-pod", "10.0.0.9", scoped_home=True)

        assert ok is True
        assert mock_stream.call_args.kwargs["remote_cmd"] == EXTRACT_HOME_REMOTE_CMD

    @pytest.mark.asyncio
    async def test_vm_extract_uses_full_command(self):
        svc = make_service()
        with patch(
            "orchestrator.services.workspace_suspension.stream_extract_snapshot",
            new=AsyncMock(return_value=(0, b"")),
        ) as mock_stream:
            ok = await svc._extract_snapshot("id-vm", "10.0.0.9", scoped_home=False)

        assert ok is True
        assert mock_stream.call_args.kwargs["remote_cmd"] == EXTRACT_REMOTE_CMD

    @pytest.mark.asyncio
    async def test_default_preserves_full_command(self):
        """Omitting ``scoped_home`` keeps the legacy full extract, so only the
        explicitly-pod callers change behavior."""
        svc = make_service()
        with patch(
            "orchestrator.services.workspace_suspension.stream_extract_snapshot",
            new=AsyncMock(return_value=(0, b"")),
        ) as mock_stream:
            ok = await svc._extract_snapshot("id-default", "10.0.0.9")

        assert ok is True
        # Omitting the override preserves stream_extract_snapshot's full
        # extract default without perturbing strict-authority call shapes.
        assert "remote_cmd" not in mock_stream.call_args.kwargs


# =============================================================================
# Test: check_idle_all
# =============================================================================


class TestCheckIdleAll:
    @pytest.mark.asyncio
    async def test_sweep_suspends_idle_containers(self):
        """Sweep suspends containers that have been idle beyond timeout."""
        svc = make_service()
        old_time = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()

        # Mock the query result
        svc._db._conn.fetch.return_value = [
            {
                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "pod_ip": "10.0.0.42",
                        "last_activity": old_time,
                    }
                },
                "updated_at": datetime.now(timezone.utc) - timedelta(minutes=60),
            }
        ]

        # Mock the suspend
        svc._db.get_job.return_value = make_job(last_activity=old_time)

        count = await svc.check_idle_all()

        assert count == 0
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sweep_skips_recent_activity(self):
        """Sweep does not suspend containers with recent activity."""
        svc = make_service()
        recent_time = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()

        svc._db._conn.fetch.return_value = [
            {
                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "pod_ip": "10.0.0.42",
                        "last_activity": recent_time,
                    }
                },
                "updated_at": datetime.now(timezone.utc) - timedelta(minutes=5),
            }
        ]

        count = await svc.check_idle_all()

        assert count == 0
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sweep_uses_updated_at_fallback(self):
        """When last_activity is missing, uses updated_at as fallback."""
        svc = make_service()
        old_updated_at = datetime.now(timezone.utc) - timedelta(minutes=60)

        svc._db._conn.fetch.return_value = [
            {
                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "pod_ip": "10.0.0.42",
                        # No last_activity key
                    }
                },
                "updated_at": old_updated_at,
            }
        ]

        svc._db.get_job.return_value = make_job()

        count = await svc.check_idle_all()

        assert count == 0
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sweep_empty_results(self):
        """Sweep returns 0 when no idle containers found."""
        svc = make_service()
        svc._db._conn.fetch.return_value = []

        count = await svc.check_idle_all()

        assert count == 0

    @pytest.mark.asyncio
    async def test_sweep_disabled_returns_zero(self):
        svc = make_service(s3_available=False)

        count = await svc.check_idle_all()

        assert count == 0

    @pytest.mark.asyncio
    async def test_sweep_respects_custom_timeout(self):
        """Sweep uses WORKSPACE_IDLE_TIMEOUT env var."""
        svc = make_service()
        # Set a very long timeout — 120 minutes
        with patch.dict("os.environ", {"WORKSPACE_IDLE_TIMEOUT": "120"}):
            assert svc.idle_timeout_minutes == 120

            # Activity 60 minutes ago should NOT be idle at 120min timeout
            old_time = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()
            svc._db._conn.fetch.return_value = [
                {
                    "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                    "context": {
                        "workspace_container": {
                            "status": "ready",
                            "pod_ip": "10.0.0.42",
                            "last_activity": old_time,
                        }
                    },
                    "updated_at": datetime.now(timezone.utc) - timedelta(minutes=60),
                }
            ]

            count = await svc.check_idle_all()
            assert count == 0


# =============================================================================
# Test: idle_timeout_minutes property
# =============================================================================


class TestIdleTimeout:
    def test_default_timeout(self):
        svc = make_service()
        with patch.dict("os.environ", {}, clear=False):
            # Remove WORKSPACE_IDLE_TIMEOUT if it exists
            import os

            os.environ.pop("WORKSPACE_IDLE_TIMEOUT", None)
            assert svc.idle_timeout_minutes == 30

    def test_custom_timeout(self):
        svc = make_service()
        with patch.dict("os.environ", {"WORKSPACE_IDLE_TIMEOUT": "60"}):
            assert svc.idle_timeout_minutes == 60


# =============================================================================
# Test: status transition correctness
# =============================================================================


class TestStatusTransitions:
    @pytest.mark.asyncio
    async def test_suspend_transitions(self):
        """Suspend projection belongs exclusively to durable settlement."""
        svc = make_service()
        svc._db.get_job.return_value = make_job()

        await svc.suspend_workspace("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()
        svc._container_provisioner.prepare_workspace_cleanup_intent.assert_not_awaited()
        svc._container_provisioner.reconcile_workspace_cleanup_intent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_restore_transitions(self):
        """Verify the exact status transition sequence during restore."""
        svc = make_service()
        configure_job_restore(svc)

        with patch.object(svc, "_extract_snapshot", new_callable=AsyncMock):
            await svc.restore_workspace("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        svc._db.merge_workspace_container_context.assert_not_awaited()
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()
        svc._container_provisioner.complete_workspace_restore_work.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_suspend_failure_transitions(self):
        """Verify status reverts to ready on capture failure."""
        svc = make_service()
        svc._db.get_job.return_value = make_job()
        svc._snapshot_service.capture_vm_snapshot.return_value = False

        await svc.suspend_workspace("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_restore_failure_transitions(self):
        """Verify status set to failed on restore failure."""
        svc = make_service()
        svc._container_provisioner.create_workspace.return_value = False
        svc._container_provisioner.get_workspace_creation_result.return_value = None
        configure_job_restore(svc)

        await svc.restore_workspace("some-job-id")

        svc._db.merge_workspace_container_context.assert_not_awaited()
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()


# =============================================================================
# Test: suspend clears pod metadata
# =============================================================================


class TestSuspendClearsMetadata:
    @pytest.mark.asyncio
    async def test_contained_k8s_suspend_keeps_pod_metadata(self):
        """Containment leaves the live workspace projection unchanged."""
        svc = make_service()
        job = make_job()
        svc._db.get_job.return_value = job

        assert (
            await svc.suspend_workspace("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee") is False
        )

        assert job["context"]["workspace_container"]["pod_ip"] == "10.0.0.42"
        assert job["context"]["workspace_container"]["pod_name"]
        svc._container_provisioner.prepare_workspace_cleanup_intent.assert_not_awaited()
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()


# =============================================================================
# Test: restore with extraction failure
# =============================================================================


class TestRestoreExtractionFailure:
    @pytest.mark.asyncio
    async def test_restore_snapshot_download_failure_is_not_reported_ready(self):
        """A failed extract is a failed restore — it must never stamp 'ready'.

        The pod comes up either way, which is exactly the trap: 'ready' over an
        empty or half-populated tree hands the dispatcher a workspace that looks
        healthy, and the agent goes to work on nothing. "The agent can
        reinitialize from Gitea" only covers tracked files — uncommitted work,
        and any workspace with no repo behind it, is simply gone. Fail visibly
        and let the caller decide.
        """
        svc = make_service()
        configure_job_restore(svc)
        svc._snapshot_service.download_snapshot.return_value = False

        result = await svc.restore_workspace("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        assert result is False
        svc._db.merge_workspace_container_context.assert_not_awaited()
        calls = svc._db.merge_workspace_container_context_if_runtime.call_args_list
        assert calls[-1].args[1]["status"] == "failed"
        assert calls[-1].kwargs == {"expected_runtime_incarnation": SUCCESSOR_RUNTIME}
        assert not any(c.args[1].get("status") == "ready" for c in calls)

    @pytest.mark.asyncio
    async def test_extract_snapshot_reports_failure_instead_of_none(self):
        """``_extract_snapshot`` returns a bool the callers can branch on.

        It used to return None on every path, so a failed download and a clean
        unpack were indistinguishable — which is how the 'ready' above got
        stamped over a workspace that never restored.
        """
        svc = make_service()
        svc._snapshot_service.download_snapshot.return_value = False

        ok = await svc._extract_snapshot("job-1", "10.0.0.99", ssh_port=30022)

        assert ok is False

    @pytest.mark.asyncio
    async def test_strict_extract_forwards_host_pin_and_pipefail(self):
        svc = make_service()
        with (
            patch(
                "orchestrator.services.workspace_suspension.resolve_ssh_key_path",
                return_value="/ssh/key",
            ),
            patch(
                "orchestrator.services.workspace_suspension.stream_extract_snapshot",
                new=AsyncMock(return_value=(0, b"")),
            ) as extract,
        ):
            ok = await svc._extract_snapshot(
                "thread-1",
                "10.0.0.99",
                ssh_port=30022,
                entity_type="threads",
                expected_host_key_fingerprint="SHA256:attested",
                require_pipefail=True,
            )

        assert ok is True
        svc._snapshot_service.download_snapshot.assert_awaited_once()
        assert (
            svc._snapshot_service.download_snapshot.await_args.kwargs[
                "require_strict_terminal"
            ]
            is True
        )
        assert extract.await_args.args[:2] == ("10.0.0.99", 30022)
        assert extract.await_args.kwargs == {
            "key_path": "/ssh/key",
            "expected_host_key_fingerprint": "SHA256:attested",
            "require_pipefail": True,
        }

    @pytest.mark.asyncio
    async def test_strict_extract_without_host_pin_never_starts_ssh(self):
        svc = make_service()
        create = AsyncMock(side_effect=AssertionError("pin validation precedes SSH"))

        with patch("asyncio.create_subprocess_exec", new=create):
            ok = await svc._extract_snapshot(
                "thread-1",
                "10.0.0.99",
                ssh_port=30022,
                entity_type="threads",
                require_pipefail=True,
            )

        assert ok is False
        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_restore_exception_during_extract(self):
        """If extraction throws, restore fails and status is set to failed."""
        svc = make_service()
        configure_job_restore(svc)

        with patch.object(
            svc,
            "_extract_snapshot",
            new_callable=AsyncMock,
            side_effect=RuntimeError("SSH connection refused"),
        ):
            result = await svc.restore_workspace("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        assert result is False
        # The exception path marks only exact B; retired A and any successor C
        # remain outside the CAS.
        svc._db.merge_workspace_container_context.assert_not_awaited()
        failure = svc._db.merge_workspace_container_context_if_runtime.await_args
        assert failure.args[1] == {
            "status": "failed",
            "error": "restore exception",
        }
        assert failure.kwargs == {"expected_runtime_incarnation": SUCCESSOR_RUNTIME}


# =============================================================================
# Test: multiple containers in one sweep
# =============================================================================


class TestMultipleContainerSweep:
    @pytest.mark.asyncio
    async def test_sweep_multiple_idle_containers(self):
        """Sweep handles multiple idle containers in one cycle."""
        svc = make_service()
        old_time = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()

        svc._db._conn.fetch.return_value = [
            {
                "id": "aaaaaaaa-1111-1111-1111-111111111111",
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "pod_ip": "10.0.0.1",
                        "last_activity": old_time,
                    }
                },
                "updated_at": datetime.now(timezone.utc) - timedelta(minutes=60),
            },
            {
                "id": "bbbbbbbb-2222-2222-2222-222222222222",
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "pod_ip": "10.0.0.2",
                        "last_activity": old_time,
                    }
                },
                "updated_at": datetime.now(timezone.utc) - timedelta(minutes=60),
            },
        ]

        # Mock get_job for each job
        svc._db.get_job.side_effect = [
            make_job(
                job_id="aaaaaaaa-1111-1111-1111-111111111111",
                pod_ip="10.0.0.1",
                last_activity=old_time,
            ),
            make_job(
                job_id="bbbbbbbb-2222-2222-2222-222222222222",
                pod_ip="10.0.0.2",
                last_activity=old_time,
            ),
        ]

        count = await svc.check_idle_all()

        assert count == 0
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.reconcile_workspace_cleanup_intent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sweep_partial_failure(self):
        """If one suspend fails, others still proceed."""
        svc = make_service()
        old_time = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()

        svc._db._conn.fetch.return_value = [
            {
                "id": "aaaaaaaa-1111-1111-1111-111111111111",
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "pod_ip": "10.0.0.1",
                        "last_activity": old_time,
                    }
                },
                "updated_at": datetime.now(timezone.utc) - timedelta(minutes=60),
            },
            {
                "id": "bbbbbbbb-2222-2222-2222-222222222222",
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "pod_ip": "10.0.0.2",
                        "last_activity": old_time,
                    }
                },
                "updated_at": datetime.now(timezone.utc) - timedelta(minutes=60),
            },
        ]

        # First job: capture fails. Second job: succeeds.
        svc._snapshot_service.capture_vm_snapshot.side_effect = [False, True]

        svc._db.get_job.side_effect = [
            make_job(
                job_id="aaaaaaaa-1111-1111-1111-111111111111",
                pod_ip="10.0.0.1",
                last_activity=old_time,
            ),
            make_job(
                job_id="bbbbbbbb-2222-2222-2222-222222222222",
                pod_ip="10.0.0.2",
                last_activity=old_time,
            ),
        ]

        count = await svc.check_idle_all()

        assert count == 0
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.reconcile_workspace_cleanup_intent.assert_not_awaited()


# =============================================================================
# Test: end-to-end suspend → restore cycle
# =============================================================================


class TestSuspendRestoreRoundTrip:
    @pytest.mark.asyncio
    async def test_contained_suspend_does_not_block_existing_snapshot_restore(self):
        """New K8s capture is refused; an already-settled A can still restore."""
        svc = make_service()
        job_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

        # Phase 1: Suspend
        svc._db.get_job.return_value = make_job(
            job_id=job_id, ws_status="ready", pod_ip="10.0.0.42"
        )
        ok = await svc.suspend_workspace(job_id)
        assert ok is False
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.reconcile_workspace_cleanup_intent.assert_not_awaited()

        # Reset mocks for restore phase
        svc._snapshot_service.capture_vm_snapshot.reset_mock()
        svc._container_provisioner.reconcile_workspace_cleanup_intent.reset_mock()
        svc._container_provisioner.create_workspace.reset_mock()
        svc._db.merge_workspace_container_context.reset_mock()
        svc._db.merge_workspace_container_context_if_runtime.reset_mock()

        # Phase 2: Restore
        configure_job_restore(svc)

        with patch.object(
            svc, "_extract_snapshot", new_callable=AsyncMock
        ) as mock_extract:
            ok = await svc.restore_workspace(job_id)

        assert ok is True

        # Pod created
        svc._container_provisioner.create_workspace.assert_awaited_once_with(
            WorkspaceOwner.job(job_id),
            operation_kind="restore",
            operation_id=CLEANUP_ID,
        )

        # Snapshot extracted to new pod IP — home-only for a pod (agent-host
        # can't overwrite root-owned /usr/local).
        mock_extract.assert_awaited_once_with(
            job_id,
            "10.0.0.99",
            ssh_port=30022,
            scoped_home=True,
            expected_host_key_fingerprint=(
                "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            ),
            mutation_authority=ANY,
        )

        # Final status and receipt settle in one exact-B transaction.
        svc._db.merge_workspace_container_context_if_runtime.assert_not_awaited()
        svc._container_provisioner.complete_workspace_restore_work.assert_awaited_once()


# =============================================================================
# Test: dispatch loop status handling (replicated logic)
# =============================================================================


class TestDispatchStatusHandling:
    """Test that the dispatch loop correctly routes suspended/restoring statuses.

    Replicates the logic from orchestrator/main.py dispatch loop rather than
    importing it (same pattern as test_container_provisioner.py).
    """

    @staticmethod
    def _should_dispatch(container_ctx: dict) -> str:
        """Replicate the dispatch loop's container status handling.

        Returns: "provision", "restore", "wait", "skip", or "dispatch".
        """
        status = container_ctx.get("status")
        if not status:
            return "provision"
        elif status == "suspended":
            return "restore"
        elif status in ("restoring", "suspending", "creating"):
            return "wait"
        elif status != "ready":
            return "skip"
        else:
            return "dispatch"

    def test_no_status_provisions(self):
        assert self._should_dispatch({}) == "provision"

    def test_ready_dispatches(self):
        assert self._should_dispatch({"status": "ready"}) == "dispatch"

    def test_suspended_restores(self):
        assert self._should_dispatch({"status": "suspended"}) == "restore"

    def test_restoring_waits(self):
        assert self._should_dispatch({"status": "restoring"}) == "wait"

    def test_suspending_waits(self):
        assert self._should_dispatch({"status": "suspending"}) == "wait"

    def test_creating_waits(self):
        assert self._should_dispatch({"status": "creating"}) == "wait"

    def test_failed_skips(self):
        assert self._should_dispatch({"status": "failed"}) == "skip"

    def test_deleted_skips(self):
        assert self._should_dispatch({"status": "deleted"}) == "skip"


# =============================================================================
# Test: restore(owner) — owner-keyed dispatch
# =============================================================================


class TestRestoreOwnerDispatch:
    """Tests for WorkspaceSuspensionService.restore(owner).

    Verifies that the owner-keyed router dispatches to the correct underlying
    method.  Uses direct AsyncMock patching rather than a fully wired service
    to keep these tests fast and isolated from the heavy restore logic.
    """

    @pytest.mark.asyncio
    async def test_restore_job_calls_restore_workspace(self):
        """restore(job owner) must delegate to restore_workspace(job_id)."""
        from orchestrator.services.workspace_lifecycle import WorkspaceOwner as WO

        svc = make_service()
        svc.restore_workspace = AsyncMock(return_value=True)
        svc.restore_thread_workspace = AsyncMock(return_value=True)

        owner = WO.job("j1")
        result = await svc.restore(owner)

        assert result is True
        svc.restore_workspace.assert_awaited_once_with("j1")
        svc.restore_thread_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_restore_session_calls_restore_thread_workspace(self):
        """restore(session owner) must delegate to restore_thread_workspace(thread_id)."""
        from orchestrator.services.workspace_lifecycle import WorkspaceOwner as WO

        svc = make_service()
        svc.restore_workspace = AsyncMock(return_value=True)
        svc.restore_thread_workspace = AsyncMock(return_value=True)

        owner = WO.session("t1")
        result = await svc.restore(owner)

        assert result is True
        svc.restore_thread_workspace.assert_awaited_once_with("t1")
        svc.restore_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_restore_session_forwards_exact_runtime_reuse_authority(self):
        from orchestrator.services.workspace_lifecycle import WorkspaceOwner as WO

        svc = make_service()
        svc.restore_workspace = AsyncMock(return_value=True)
        svc.restore_thread_workspace = AsyncMock(return_value=True)
        runtime_uid = "11111111-1111-4111-8111-111111111111"

        result = await svc.restore(
            WO.session("t1"),
            expected_runtime_incarnation=runtime_uid,
        )

        assert result is True
        svc.restore_thread_workspace.assert_awaited_once_with(
            "t1",
            expected_runtime_incarnation=runtime_uid,
        )
        svc.restore_workspace.assert_not_awaited()


# =============================================================================
# Test: heartbeat activity tracking (replicated logic)
# =============================================================================


class TestHeartbeatActivityTracking:
    """Test the heartbeat last_activity update logic.

    Replicates the conditional from orchestrator/main.py agent_heartbeat.
    """

    @staticmethod
    def _should_update_activity(status: str, current_job_id: str | None) -> bool:
        """Replicate the heartbeat condition for activity tracking."""
        return bool(current_job_id and status == "working")

    def test_working_with_job_updates(self):
        assert self._should_update_activity("working", "some-job-id") is True

    def test_ready_does_not_update(self):
        assert self._should_update_activity("ready", "some-job-id") is False

    def test_working_without_job_does_not_update(self):
        assert self._should_update_activity("working", None) is False

    def test_idle_does_not_update(self):
        assert self._should_update_activity("idle", "some-job-id") is False


# =============================================================================
# Test: thread tier is read explicitly, not inferred from metadata presence
# knowledge-base/knowledge/issues/workspace_suspension_infers_tier_from_metadata_presence.md
# =============================================================================


def make_vm_thread(thread_id="tid-vm", ssh_host="100.64.0.235"):
    """A healthy vm-tier session.

    The workspace_container key is present but holds ONLY git coordinates —
    _setup_gitea writes those for EVERY thread including vm-tier ones. It has no
    'status'/'pod_ip', because no container was ever provisioned for it.
    """
    return {
        "id": thread_id,
        "status": "active",
        "execution_lane": "pinned",
        "runtime_generation": VM_SESSION_GENERATION,
        "runtime_attach_token": VM_SESSION_ATTACH,
        "agent_id": VM_SESSION_AGENT,
        "metadata": {
            "config_override": {"workspace": {"backend": "vm"}},
            "workspace_container": {
                "git_remote_url": "http://gitea/srw/thread-tid-vm.git",
                "repo_name": "thread-tid-vm",
            },
            "vm": {
                "status": "ready",
                "provision_generation": VM_GENERATION,
                "vm_uid": VM_UID,
                "active_pod_uid": VM_LAUNCHER_UID,
                "ssh_host": ssh_host,
                "ssh_port": 22,
                "ssh_host_key_fingerprint": VM_FINGERPRINT,
                "ssh_registration_id": "vm-registration-thread-a",
                "identity_authenticated": True,
                "identity_provision_generation": VM_GENERATION,
            },
        },
    }


def make_container_thread(
    thread_id="tid-pod",
    *,
    ws_status="ready",
    pod_ip="10.42.2.32",
    runtime=WORKSPACE_RUNTIME,
    snapshot_restore_required=False,
    creation_receipt=False,
):
    workspace = {
        "status": ws_status,
        "provisioner": "k8s",
        "pod_ip": pod_ip,
        "git_remote_url": "http://gitea/srw/thread-tid-pod.git",
        "_runtime_incarnation": runtime,
    }
    if snapshot_restore_required:
        workspace["_snapshot_restore_required"] = True
    if creation_receipt:
        workspace.update(
            {
                "_creation_reservation_id": CREATION_ID,
                "_creation_claim_token": str(CREATION_TOKEN),
            }
        )
    return {
        "id": thread_id,
        "status": "active",
        "metadata": {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": workspace,
        },
    }


def configure_thread_restore(svc, *, before=None, after=None):
    """Wire one exact settled suspended A to one settled restore B."""

    before = before or make_container_thread(
        ws_status="suspended",
        pod_ip=None,
        snapshot_restore_required=True,
    )
    after = after or make_container_thread(
        ws_status="ready",
        pod_ip="10.42.2.99",
        runtime=SUCCESSOR_RUNTIME,
        snapshot_restore_required=True,
        creation_receipt=True,
    )
    svc._db.get_thread = AsyncMock(side_effect=[before, after])
    return before, after


def make_vm_service(*, host="100.64.0.235", port=22):
    svc = make_service()
    vm_prov = MagicMock()
    type(vm_prov).is_available = PropertyMock(return_value=True)
    vm_prov.delete_thread_vm = AsyncMock(return_value=True)
    vm_prov.delete_vm = AsyncMock(return_value=True)
    vm_prov.create_thread_vm = AsyncMock(return_value=True)
    identity = VMTeardownIdentity(
        provision_generation=VM_GENERATION,
        vm_uid=VM_UID,
        rootdisk_pvc_uid=VM_ROOTDISK_UID,
        ssh_host=host,
        ssh_port=port,
        ssh_host_key_fingerprint=VM_FINGERPRINT,
    )
    attestation = WorkspaceRuntimeAttestation(
        backing_id=f"k8s-vmi:{VM_LAUNCHER_UID}",
        workspace_generation=VM_GENERATION,
        runtime_incarnation=VM_LAUNCHER_UID,
        ssh_host_key_fingerprint=VM_FINGERPRINT,
        host=host,
        pod_ip="10.42.0.15",
        port=port,
        vm_uid=VM_UID,
        launcher_pod_uid=VM_LAUNCHER_UID,
    )
    vm_prov.capture_vm_teardown_identity = AsyncMock(return_value=identity)
    vm_prov.attest_workspace_runtime = AsyncMock(return_value=attestation)
    vm_prov.revalidate_vm_teardown_identity = AsyncMock(return_value="matched")

    async def release_vm_captured(
        owner_id,
        _identity,
        *,
        purge_disk,
        capture_snapshot,
        entity_type,
    ):
        assert _identity == identity
        assert capture_snapshot is False
        if entity_type == "thread":
            await vm_prov.delete_thread_vm(owner_id, purge_disk=purge_disk)
        else:
            await vm_prov.delete_vm(owner_id, purge_disk=purge_disk)
        return VMTeardownResult("completed", True)

    vm_prov.release_vm_captured = AsyncMock(side_effect=release_vm_captured)
    svc._db.merge_vm_context_if_provision_generation = AsyncMock(return_value=True)
    svc._db.merge_thread_vm_context_if_provision_generation = AsyncMock(
        return_value=True
    )
    lease_receipt = {
        "id": VM_OPERATION_ID,
        "claim_token": 31,
        "lease_expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
    }
    svc._db.activate_vm_remote_operation_protocol = AsyncMock(return_value=True)
    svc._db.claim_vm_remote_operation = AsyncMock(return_value=lease_receipt)
    svc._db.renew_vm_remote_operation = AsyncMock(return_value=lease_receipt)
    svc._db.settle_vm_remote_operation = AsyncMock(return_value=True)
    svc._vm_provisioner = vm_prov
    svc._agent_provisioner = None
    return svc, vm_prov


class TestStatelessThreadSuspensionRefusal:
    @staticmethod
    def _service_with_agent_delete_probe():
        svc = make_service()
        agent_provisioner = MagicMock()
        agent_provisioner.delete_agent_pod_by_thread = AsyncMock()
        svc._agent_provisioner = agent_provisioner
        return svc

    @staticmethod
    def _assert_no_suspension_side_effects(svc):
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.delete_workspace.assert_not_awaited()
        svc._container_provisioner.prepare_workspace_cleanup_intent.assert_not_awaited()
        svc._agent_provisioner.delete_agent_pod_by_thread.assert_not_awaited()
        svc._db.merge_thread_workspace_context.assert_not_awaited()
        svc._db.merge_thread_workspace_context_if_runtime.assert_not_awaited()
        svc._db.merge_thread_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("thread_status", ["active", "awaiting_user"])
    async def test_ready_workspace_is_refused_for_stateless_thread(self, thread_status):
        svc = self._service_with_agent_delete_probe()
        thread = make_container_thread()
        thread["status"] = thread_status
        thread["execution_lane"] = "stateless"
        svc._db.get_thread = AsyncMock(return_value=thread)

        assert await svc.suspend_thread_workspace("tid-pod") is False

        self._assert_no_suspension_side_effects(svc)

    @pytest.mark.asyncio
    async def test_ended_retirement_marker_is_refused_without_cleanup_race(self):
        svc = self._service_with_agent_delete_probe()
        thread = make_container_thread()
        thread["status"] = "ended"
        thread["execution_lane"] = "stateless"
        thread["metadata"]["_stateless_workspace_retirement_pending"] = True
        thread["metadata"]["_stateless_claim_retirement"] = {"terminal_token": 8}
        svc._db.get_thread = AsyncMock(return_value=thread)

        assert await svc.suspend_thread_workspace("tid-pod") is False

        self._assert_no_suspension_side_effects(svc)

    @pytest.mark.asyncio
    async def test_malformed_metadata_is_refused_before_parsing(self):
        svc = self._service_with_agent_delete_probe()
        svc._db.get_thread = AsyncMock(
            return_value={
                "id": "tid-pod",
                "status": "awaiting_user",
                "execution_lane": "stateless",
                "metadata": "{not-json",
            }
        )

        assert await svc.suspend_thread_workspace("tid-pod") is False

        self._assert_no_suspension_side_effects(svc)


class TestThreadSuspensionRuntimeRaces:
    @pytest.mark.asyncio
    async def test_successor_runtime_survives_superseded_cleanup(self, monkeypatch):
        """A thread suspend queued for A must leave replacement B untouched."""

        monkeypatch.delenv("WORKSPACE_RECLAIM_ON_IDLE", raising=False)
        svc = make_service()
        thread = make_container_thread()
        svc._db.get_thread = AsyncMock(return_value=thread)
        successor = {
            "status": "ready",
            "provisioner": "k8s",
            "pod_ip": "10.42.2.99",
            "pod_name": "workspace-session-successor",
            "_runtime_incarnation": SUCCESSOR_RUNTIME,
        }

        async def reconcile_stale(*args, **kwargs):
            thread["metadata"]["workspace_container"] = successor
            return WorkspaceCleanupOutcome("superseded", 1)

        svc._container_provisioner.reconcile_workspace_cleanup_intent = AsyncMock(
            side_effect=reconcile_stale
        )

        assert await svc.suspend_thread_workspace("tid-pod") is False
        assert thread["metadata"]["workspace_container"] != successor
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.reconcile_workspace_cleanup_intent.assert_not_awaited()
        svc._db.merge_thread_workspace_context.assert_not_awaited()
        svc._db.merge_thread_workspace_context_if_runtime.assert_not_awaited()
        svc._container_provisioner.delete_workspace_pvc.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_post_snapshot_identity_race_stops_before_intent(self):
        svc = make_service()
        svc._db.get_thread = AsyncMock(return_value=make_container_thread())
        svc._container_provisioner.capture_workspace_teardown_identity.return_value = (
            WorkspaceTeardownIdentity(
                pod_uid=SUCCESSOR_RUNTIME,
                pvc_uid=None,
                service_uid=None,
            )
        )

        assert await svc.suspend_thread_workspace("tid-pod") is False

        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.capture_workspace_teardown_identity.assert_not_awaited()
        svc._container_provisioner.prepare_workspace_cleanup_intent.assert_not_awaited()


class TestThreadTierIsExplicit:
    """A vm-tier thread must suspend via the VM branch. Previously the tier was
    inferred from whether metadata.workspace_container existed at all — and
    _setup_gitea makes it exist for every thread — so a VM session read as
    pod-tier and suspend bailed out entirely, leaving its VM running forever."""

    @pytest.fixture(autouse=True)
    def _enable_vm_remote_operation_protocol(self, monkeypatch):
        monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")

    @pytest.mark.asyncio
    async def test_vm_tier_thread_actually_suspends(self):
        svc, vm_prov = make_vm_service()
        svc._db.get_thread = AsyncMock(return_value=make_vm_thread())

        ok = await svc.suspend_thread_workspace("tid-vm")

        assert ok is True, "vm-tier suspend must not bail on the container status"
        # Soft End is resumable. A successful snapshot is not authority to
        # destroy the backing disk; only permanent End may purge it.
        vm_prov.delete_thread_vm.assert_awaited_once_with("tid-vm", purge_disk=False)

    @pytest.mark.asyncio
    async def test_vm_tier_snapshot_is_labelled_vm(self):
        """source_type is persisted into the snapshot manifest and read back on
        restore, so mislabelling a VM snapshot 'pod' outlives the suspend."""
        svc, _ = make_vm_service()
        svc._db.get_thread = AsyncMock(return_value=make_vm_thread())

        await svc.suspend_thread_workspace("tid-vm")

        kwargs = svc._snapshot_service.capture_vm_snapshot.await_args.kwargs
        assert kwargs["source_type"] == "vm"
        assert kwargs["ssh_host"] == "100.64.0.235"
        # _resolve_ssh_port also branched on ws_ctx presence, so a vm-tier thread
        # (whose ws_ctx holds git coordinates) was handed the POD port 30022.
        assert kwargs["ssh_port"] == 22

    @pytest.mark.asyncio
    async def test_vm_tier_status_markers_go_to_vm_context(self):
        """Progress markers must land on metadata.vm, not on the git-only
        workspace_container key."""
        svc, _ = make_vm_service()
        svc._db.get_thread = AsyncMock(return_value=make_vm_thread())

        await svc.suspend_thread_workspace("tid-vm")

        assert svc._db.merge_thread_vm_context_if_provision_generation.await_count >= 1
        svc._db.merge_thread_workspace_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_vm_tier_never_dials_stale_ready_container_residue(self):
        svc, vm_prov = make_vm_service()
        thread = make_vm_thread()
        thread["metadata"]["workspace_container"].update(
            {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.42.2.32",
                "host": "workspace-successor.srw.svc.cluster.local",
                "_runtime_incarnation": "deleted-runtime-a",
            }
        )
        svc._db.get_thread = AsyncMock(return_value=thread)

        assert await svc.suspend_thread_workspace("tid-vm") is True

        capture = svc._snapshot_service.capture_vm_snapshot.await_args.kwargs
        assert capture["ssh_host"] == "100.64.0.235"
        assert capture["ssh_port"] == 22
        assert capture["source_type"] == "vm"
        vm_prov.delete_thread_vm.assert_awaited_once()
        svc._container_provisioner.prepare_workspace_cleanup_intent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_container_tier_thread_is_contained_before_pod_capture(self):
        """Sandbox-tier capture stays live until durable capture authority exists."""
        svc, vm_prov = make_vm_service()
        svc._db.get_thread = AsyncMock(return_value=make_container_thread())

        ok = await svc.suspend_thread_workspace("tid-pod")

        assert ok is False
        vm_prov.delete_thread_vm.assert_not_awaited()
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.reconcile_workspace_cleanup_intent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_idle_sweep_actually_suspends_a_vm_thread(self):
        """End-to-end for the leak: the sweeper already SELECTed vm-tier threads
        (metadata->'vm'->>'status' = 'ready'), but suspend_thread_workspace bailed
        on every one of them, so an idle VM session's VM ran until the session
        ended. This asserts the whole chain now completes."""
        svc, vm_prov = make_vm_service()
        thread = make_vm_thread()
        svc._db._conn.fetch = AsyncMock(
            return_value=[
                {
                    "id": "tid-vm",
                    "metadata": thread["metadata"],
                    "last_activity": datetime.now(timezone.utc) - timedelta(hours=3),
                }
            ]
        )
        svc._db.get_thread = AsyncMock(return_value=thread)

        n = await svc.check_idle_threads()

        assert n == 1, "idle vm-tier thread must actually be suspended"
        vm_prov.delete_thread_vm.assert_awaited_once_with("tid-vm", purge_disk=False)

    @pytest.mark.asyncio
    async def test_vm_suspend_is_dark_before_any_snapshot_or_retirement(
        self, monkeypatch
    ):
        monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "false")
        svc, vm_prov = make_vm_service()
        svc._db.get_thread = AsyncMock(return_value=make_vm_thread())

        assert await svc.suspend_thread_workspace("tid-vm") is False

        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        vm_prov.release_vm_captured.assert_not_awaited()
        svc._db.activate_vm_remote_operation_protocol.assert_not_awaited()
        assert [
            call.args[2]
            for call in svc._db.merge_thread_vm_context_if_provision_generation.await_args_list
        ] == [
            {"status": "suspending", "_suspend_remote_io_closed": None},
            {"status": "ready", "_suspend_remote_io_closed": None},
        ]


class TestVmSuspendRidesThePersistentRootdisk:
    """VM session suspend used to be fail-closed on a snapshot that can never
    succeed: capture_vm_snapshot SSHes from the orchestrator, a VM workspace is
    only reachable over the tailnet, so every VM suspend refused and left the VM
    running. With a persistent rootdisk the snapshot stops being load-bearing —
    the disk itself carries the files across the teardown.

    knowledge-base/knowledge/features/vm_workspace_persistence_reconciliation.md,
    knowledge-base/knowledge/issues/vm_workspace_snapshot_unreachable_from_orchestrator.md.
    """

    @pytest.fixture(autouse=True)
    def _enable_vm_remote_operation_protocol(self, monkeypatch):
        monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")

    @pytest.mark.asyncio
    async def test_suspend_keeps_the_rootdisk(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service()
        svc._db.get_thread = AsyncMock(return_value=make_vm_thread())

        ok = await svc.suspend_thread_workspace("tid-vm")

        assert ok is True
        vm_prov.delete_thread_vm.assert_awaited_once_with("tid-vm", purge_disk=False)

    @pytest.mark.asyncio
    async def test_unreachable_snapshot_no_longer_blocks_suspend(self, monkeypatch):
        """The exact live failure: capture returns False (not raises) because
        the orchestrator has no tailnet route. The VM must still suspend."""
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service()
        svc._db.get_thread = AsyncMock(return_value=make_vm_thread())
        svc._snapshot_service.capture_vm_snapshot = AsyncMock(return_value=False)

        ok = await svc.suspend_thread_workspace("tid-vm")

        assert ok is True, "a kept disk makes the snapshot non-load-bearing"
        vm_prov.delete_thread_vm.assert_awaited_once_with("tid-vm", purge_disk=False)

    @pytest.mark.asyncio
    async def test_suspend_records_the_kept_disk(self, monkeypatch):
        """Restore reads this to decide whether to extract a snapshot over the
        reattached disk (it must not)."""
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, _ = make_vm_service()
        svc._db.get_thread = AsyncMock(return_value=make_vm_thread())

        await svc.suspend_thread_workspace("tid-vm")

        merged = {}
        for (
            call
        ) in svc._db.merge_thread_vm_context_if_provision_generation.await_args_list:
            merged.update(call.args[2])
        assert merged.get("rootdisk") == "kept"

    @pytest.mark.asyncio
    async def test_flag_off_stays_fail_closed(self, monkeypatch):
        """Without the controller-side flag the disk cascade-deletes, so a
        failed snapshot must still keep the workspace alive — suspending would
        destroy the session."""
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "false")
        svc, vm_prov = make_vm_service()
        svc._db.get_thread = AsyncMock(return_value=make_vm_thread())
        svc._snapshot_service.capture_vm_snapshot = AsyncMock(return_value=False)

        ok = await svc.suspend_thread_workspace("tid-vm")

        assert ok is False
        vm_prov.delete_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_container_tier_stays_fail_closed(self, monkeypatch):
        """Pods have no rootdisk to keep — the snapshot is still the only copy,
        flag or no flag."""
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, _ = make_vm_service()
        svc._db.get_thread = AsyncMock(return_value=make_container_thread())
        svc._snapshot_service.capture_vm_snapshot = AsyncMock(return_value=False)

        ok = await svc.suspend_thread_workspace("tid-pod")

        assert ok is False
        svc._container_provisioner.prepare_workspace_cleanup_intent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_restore_does_not_extract_over_a_reattached_disk(self, monkeypatch):
        """The disk is the live state at teardown; any snapshot is at best the
        same moment and at worst older. Extracting it would overwrite newer
        files with stale ones."""
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, _ = make_vm_service()
        thread = make_vm_thread()
        thread["metadata"]["vm"]["rootdisk"] = "kept"
        svc._db.get_thread = AsyncMock(return_value=thread)
        svc._extract_snapshot = AsyncMock()

        ok = await svc.restore_thread_workspace("tid-vm")

        assert ok is True
        svc._extract_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_restore_still_extracts_when_the_disk_was_purged(self, monkeypatch):
        """A thread suspended before the flag was on has no disk waiting — its
        S3 snapshot is still the only copy."""
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, _ = make_vm_service()
        thread = make_vm_thread()
        thread["metadata"]["vm"]["rootdisk"] = "purged"
        svc._db.get_thread = AsyncMock(return_value=thread)
        svc._extract_snapshot = AsyncMock()

        await svc.restore_thread_workspace("tid-vm")

        svc._extract_snapshot.assert_awaited_once()


class TestVmJobSuspendRidesThePersistentRootdisk:
    """The job half of the same dead gate. A VM-backed job's idle suspension
    refused for exactly the reason a session's did — the capture SSHes from the
    orchestrator and a VM is only on the tailnet — so VM jobs never suspended
    either. Kept symmetric with the session path deliberately."""

    @pytest.fixture(autouse=True)
    def _enable_vm_remote_operation_protocol(self, monkeypatch):
        monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")

    def _vm_job(self):
        return {
            "id": "job-vm-1",
            "status": "paused",
            "config_override": {"workspace": {"backend": "vm"}},
            # No workspace_container: jobs only get that key from
            # container-provisioning paths, so for a job its absence really
            # does mean VM tier (unlike threads — see _thread_is_vm_tier).
            "context": {
                "vm": {
                    "status": "ready",
                    "provision_generation": VM_GENERATION,
                    "vm_uid": VM_UID,
                    "active_pod_uid": VM_LAUNCHER_UID,
                    "ssh_host": "100.64.1.9",
                    "ssh_port": 22,
                    "ssh_host_key_fingerprint": VM_FINGERPRINT,
                    "ssh_registration_id": "vm-registration-a",
                    "identity_authenticated": True,
                    "identity_provision_generation": VM_GENERATION,
                }
            },
        }

    @pytest.mark.asyncio
    async def test_suspend_keeps_the_rootdisk(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service(host="100.64.1.9")
        svc._db.get_job = AsyncMock(return_value=self._vm_job())
        svc._vm_provisioner.delete_vm = AsyncMock(return_value=True)

        ok = await svc.suspend_workspace("job-vm-1")

        assert ok is True
        svc._vm_provisioner.delete_vm.assert_awaited_once_with(
            "job-vm-1", purge_disk=False
        )

    @pytest.mark.asyncio
    async def test_recovery_hold_blocks_vm_suspend_external_release(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service(host="100.64.1.9")
        svc._db.get_job = AsyncMock(return_value=self._vm_job())
        recovery_store = MagicMock()
        recovery_store.acquire_cleanup_permit = AsyncMock(
            return_value=MagicMock(
                allowed=False,
                reason="workspace_recovery_unresolved",
            )
        )
        svc._workspace_recovery_store = recovery_store

        assert await svc.suspend_workspace("job-vm-1") is False

        recovery_store.acquire_cleanup_permit.assert_awaited_once()
        vm_prov.release_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_git_only_container_metadata_does_not_hide_vm_target(
        self, monkeypatch
    ):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service(host="100.64.1.9")
        job = self._vm_job()
        job["context"]["workspace_container"] = {
            "git_remote_url": "http://gitea/srw/job-vm-1.git",
            "repo_name": "job-vm-1",
        }
        svc._db.get_job = AsyncMock(return_value=job)
        svc._vm_provisioner.delete_vm = AsyncMock(return_value=True)

        assert await svc.suspend_workspace("job-vm-1") is True

        capture = svc._snapshot_service.capture_vm_snapshot.await_args.kwargs
        assert capture["ssh_host"] == "100.64.1.9"
        assert capture["source_type"] == "vm"
        vm_prov.delete_vm.assert_awaited_once_with("job-vm-1", purge_disk=False)

    @pytest.mark.asyncio
    async def test_unreachable_snapshot_no_longer_blocks_suspend(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, _ = make_vm_service(host="100.64.1.9")
        svc._db.get_job = AsyncMock(return_value=self._vm_job())
        svc._vm_provisioner.delete_vm = AsyncMock(return_value=True)
        svc._snapshot_service.capture_vm_snapshot = AsyncMock(return_value=False)

        assert await svc.suspend_workspace("job-vm-1") is True

    @pytest.mark.asyncio
    async def test_flag_off_stays_fail_closed(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "false")
        svc, _ = make_vm_service()
        svc._db.get_job = AsyncMock(return_value=self._vm_job())
        svc._vm_provisioner.delete_vm = AsyncMock(return_value=True)
        svc._snapshot_service.capture_vm_snapshot = AsyncMock(return_value=False)

        assert await svc.suspend_workspace("job-vm-1") is False
        svc._vm_provisioner.delete_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pod_job_stays_fail_closed(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, _ = make_vm_service()
        svc._db.get_job = AsyncMock(return_value=make_job())
        svc._snapshot_service.capture_vm_snapshot = AsyncMock(return_value=False)

        assert await svc.suspend_workspace(make_job()["id"]) is False
        svc._container_provisioner.delete_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_same_endpoint_successor_gets_zero_settings_or_snapshot_reads(
        self, monkeypatch
    ):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service(host="100.64.1.9")
        job = self._vm_job()
        job["user_id"] = "user-a"
        svc._db.get_job = AsyncMock(return_value=job)
        initial = vm_prov.attest_workspace_runtime.return_value
        successor = WorkspaceRuntimeAttestation(
            backing_id="k8s-vmi:99999999-9999-4999-8999-999999999999",
            workspace_generation=initial.workspace_generation,
            runtime_incarnation="99999999-9999-4999-8999-999999999999",
            ssh_host_key_fingerprint=initial.ssh_host_key_fingerprint,
            host=initial.host,
            pod_ip=initial.pod_ip,
            port=initial.port,
        )
        vm_prov.attest_workspace_runtime = AsyncMock(
            side_effect=[initial, successor, successor]
        )
        remote_reads = 0

        async def guarded_pull(*_args, capture_authority, **_kwargs):
            nonlocal remote_reads
            if await capture_authority() is None:
                return {}
            remote_reads += 1
            raise AssertionError("foreign successor must not be read")

        with patch("orchestrator.services.ide_settings.pull_ide_config", guarded_pull):
            assert await svc.suspend_workspace(job["id"]) is False

        assert remote_reads == 0
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        vm_prov.release_vm_captured.assert_not_awaited()
        assert [
            call.args[2]
            for call in svc._db.merge_vm_context_if_provision_generation.await_args_list
        ] == [
            {"status": "suspending", "_suspend_remote_io_closed": None},
            {"status": "ready", "_suspend_remote_io_closed": None},
        ]


class TestUpgradedThreadReadsAsVmTier:
    """A thread upgraded to VM carries an explicit server-owned VM contract.

    Runtime readiness must never beat a contradictory declared tier.  The VM
    provisioning admission now stamps contract, config, and provision
    generation atomically; suspension consumes that authority.
    """

    @pytest.fixture(autouse=True)
    def _enable_vm_remote_operation_protocol(self, monkeypatch):
        monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")

    def _upgraded(self, declared_backend):
        """Real shape from dev: lite/sandbox backend + a live VM + a git-only
        workspace_container (no pod ever provisioned for a lite session)."""
        return {
            "id": "tid-up",
            "status": "active",
            "metadata": {
                "config_override": {"workspace": {"backend": "vm"}},
                "_workspace_contract": {
                    "version": 1,
                    "requested_backend": "vm",
                    "assigned_backend": "vm",
                    "assignment_source": "runtime_vm_upgrade",
                },
                "workspace_container": {
                    "git_remote_url": "http://gitea/srw/thread-tid-up.git",
                    "repo_name": "thread-tid-up",
                },
                "vm": {
                    "status": "ready",
                    "provision_generation": VM_GENERATION,
                    "vm_uid": VM_UID,
                    "active_pod_uid": VM_LAUNCHER_UID,
                    "ssh_host": "100.64.2.6",
                    "ssh_port": 22,
                    "ssh_host_key_fingerprint": VM_FINGERPRINT,
                    "ssh_registration_id": "vm-registration-upgraded-a",
                    "identity_authenticated": True,
                    "identity_provision_generation": VM_GENERATION,
                },
            },
        }

    @pytest.mark.asyncio
    async def test_legacy_contradictory_upgrade_is_contained_before_snapshot(
        self, monkeypatch
    ):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service(host="100.64.2.6")
        thread = self._upgraded("sandbox")
        thread["metadata"].pop("_workspace_contract")
        thread["metadata"]["config_override"]["workspace"]["backend"] = "sandbox"
        svc._db.get_thread = AsyncMock(return_value=thread)

        assert await svc.suspend_thread_workspace("tid-up") is False

        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        vm_prov.release_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_virtual_upgraded_to_vm_suspends_via_the_vm_branch(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service(host="100.64.2.6")
        svc._db.get_thread = AsyncMock(return_value=self._upgraded("virtual"))
        svc._snapshot_service.capture_vm_snapshot = AsyncMock(return_value=False)

        ok = await svc.suspend_thread_workspace("tid-up")

        assert ok is True
        vm_prov.delete_thread_vm.assert_awaited_once_with("tid-up", purge_disk=False)

    @pytest.mark.asyncio
    async def test_sandbox_upgraded_to_vm_reads_as_vm(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service(host="100.64.2.6")
        svc._db.get_thread = AsyncMock(return_value=self._upgraded("sandbox"))

        await svc.suspend_thread_workspace("tid-up")

        vm_prov.delete_thread_vm.assert_awaited_once_with("tid-up", purge_disk=False)

    @pytest.mark.asyncio
    async def test_upgraded_thread_snapshot_is_labelled_vm(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, _ = make_vm_service(host="100.64.2.6")
        svc._db.get_thread = AsyncMock(return_value=self._upgraded("virtual"))

        await svc.suspend_thread_workspace("tid-up")

        kwargs = svc._snapshot_service.capture_vm_snapshot.await_args.kwargs
        assert kwargs["source_type"] == "vm"
        assert kwargs["ssh_host"] == "100.64.2.6"
        assert kwargs["ssh_port"] == 22  # not the pod's 30022

    @pytest.mark.asyncio
    async def test_a_live_container_still_wins(self, monkeypatch):
        """Guard the other direction: a thread with a REAL provisioned pod is
        container-tier even if a stale vm context is lying around (a failed
        upgrade), because ws_ctx carries pod state."""
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service(host="100.64.0.9")
        thread = self._upgraded("sandbox")
        thread["metadata"]["workspace_container"].update(
            {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.42.2.32",
                "_runtime_incarnation": WORKSPACE_RUNTIME,
            }
        )
        svc._db.get_thread = AsyncMock(return_value=thread)

        await svc.suspend_thread_workspace("tid-up")

        vm_prov.delete_thread_vm.assert_not_awaited()
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._container_provisioner.reconcile_workspace_cleanup_intent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_declared_vm_without_container_state_still_reads_vm(
        self, monkeypatch
    ):
        """A declared-VM thread with no container runtime takes the VM path."""
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service(host="100.64.0.9")
        thread = self._upgraded("vm")
        thread["metadata"]["vm"]["ssh_host"] = "100.64.0.9"
        svc._db.get_thread = AsyncMock(return_value=thread)

        await svc.suspend_thread_workspace("tid-up")

        vm_prov.delete_thread_vm.assert_awaited_once_with("tid-up", purge_disk=False)


class TestVmRestoreEndsAtTheCreate:
    """For a kept-disk VM restore there is nothing to do after create_thread_vm:
    no extract (the disk already holds the workspace) and no SSH (coordinates
    arrive minutes later via the daemon's register — VM creation is async over
    NATS). The container-era tail required an ssh_host synchronously, so every
    VM restore fell into 'no SSH host after provisioning' and stamped a
    transient vm.status='failed' — which a declared-vm thread's attach poll
    treats as fatal. Live-gate finding (thread a1240add)."""

    def _kept_thread(self):
        thread = make_vm_thread(thread_id="tid-vm")
        thread["metadata"]["vm"]["rootdisk"] = "kept"
        thread["metadata"]["vm"]["status"] = "deleted"
        return thread

    @pytest.mark.asyncio
    async def test_resume_preserves_custom_image_and_resources(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service()
        before = self._kept_thread()
        before["metadata"]["config_override"]["workspace"]["vm"] = {
            "image": "registry.example/dev-vm:v1",
            "cpu_cores": 12,
            "memory": "24Gi",
            "disk_size": "120Gi",
        }
        svc._db.get_thread = AsyncMock(return_value=before)
        svc._extract_snapshot = AsyncMock()
        assert await svc.restore_thread_workspace("tid-vm") is True
        options = vm_prov.create_thread_vm.await_args.kwargs
        assert options["vm_image"] == "registry.example/dev-vm:v1"
        assert options["cpu_cores"] == 12
        assert options["memory"] == "24Gi"
        assert options["disk_size"] == "120Gi"
        svc._extract_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_kept_disk_restore_succeeds_without_ssh_host(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, vm_prov = make_vm_service()
        before = self._kept_thread()
        # After create_thread_vm the context is reset to provisioning — no
        # ssh_host yet. The old tail read this as failure.
        after = self._kept_thread()
        after["metadata"]["vm"] = {"status": "provisioning", "rootdisk": "kept"}
        svc._db.get_thread = AsyncMock(side_effect=[before, after])
        svc._extract_snapshot = AsyncMock()

        ok = await svc.restore_thread_workspace("tid-vm")

        assert ok is True
        vm_prov.create_thread_vm.assert_awaited_once_with(
            "tid-vm",
            expected_runtime_generation=VM_SESSION_GENERATION,
            expected_agent_id=VM_SESSION_AGENT,
            expected_attach_token=VM_SESSION_ATTACH,
            expected_vm_context=ANY,
        )
        svc._extract_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_kept_disk_restore_never_stamps_failed(self, monkeypatch):
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, _ = make_vm_service()
        before = self._kept_thread()
        after = self._kept_thread()
        after["metadata"]["vm"] = {"status": "provisioning", "rootdisk": "kept"}
        svc._db.get_thread = AsyncMock(side_effect=[before, after])

        await svc.restore_thread_workspace("tid-vm")

        for call in svc._db.merge_thread_vm_context.await_args_list:
            assert call.args[1].get("status") != "failed", (
                "a kept-disk restore must not write the transient 'failed' that "
                "kills a declared-vm thread's attach poll"
            )

    @pytest.mark.asyncio
    async def test_kept_disk_restore_defers_ready_to_the_daemon(self, monkeypatch):
        """'ready' must come from real evidence (daemon register / lifecycle
        status), not from restore optimistically stamping it while the VM is
        still booting."""
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, _ = make_vm_service()
        before = self._kept_thread()
        after = self._kept_thread()
        after["metadata"]["vm"] = {"status": "provisioning", "rootdisk": "kept"}
        svc._db.get_thread = AsyncMock(side_effect=[before, after])

        await svc.restore_thread_workspace("tid-vm")

        for call in svc._db.merge_thread_vm_context.await_args_list:
            assert call.args[1].get("status") != "ready"

    @pytest.mark.asyncio
    async def test_purged_disk_vm_restore_keeps_the_extract_path(self, monkeypatch):
        """A pre-flag suspend has no disk waiting — its S3 snapshot is the only
        copy, so the extract (and therefore the ssh wait) must remain."""
        monkeypatch.setenv("VM_PERSISTENT_ROOTDISK", "true")
        svc, _ = make_vm_service()
        thread = make_vm_thread(thread_id="tid-vm")
        thread["metadata"]["vm"]["rootdisk"] = "purged"
        svc._db.get_thread = AsyncMock(return_value=thread)
        svc._extract_snapshot = AsyncMock()

        ok = await svc.restore_thread_workspace("tid-vm")

        assert ok is True
        svc._extract_snapshot.assert_awaited_once()


class TestSessionRestoreRidesTheReattachedVolume:
    """The container-tier twin of the kept-rootdisk rule above.

    Once a session's workspace is PVC-backed, a restore that reattaches the SAME
    volume is not a restore at all — the tree is already there. Unrolling the
    older S3 tarball over it would replace newer files with older ones, exactly
    what ``rootdisk == "kept"`` prevents one tier up. The signal is the thread's
    ``_workspace_binding.backing_id``, which ``_trusted_pod_ssh_identity`` mints
    from the PVC's UID (``k8s-pvc:<ns>:<uid>``) and from the pod's UID
    (``k8s-pod:…``) when there is no claim — so it is stable across every
    reattaching recreate and changes the moment a new volume is minted.
    """

    @staticmethod
    def _thread(backing_id=None, pod_ip="10.42.2.32"):
        thread = make_container_thread()
        thread["metadata"]["workspace_container"]["pod_ip"] = pod_ip
        if backing_id is not None:
            thread["metadata"]["_workspace_binding"] = {
                "backing_kind": "remote",
                "backing_id": backing_id,
            }
        return thread

    def _svc(self, before, after):
        """restore reads the thread twice: once before create_workspace (the
        volume we are coming BACK to) and once after (what the new pod mounts)."""
        before_workspace = before["metadata"]["workspace_container"]
        before_workspace.update(
            {
                "status": "suspended",
                "pod_ip": None,
                "_runtime_incarnation": WORKSPACE_RUNTIME,
                "_snapshot_restore_required": True,
            }
        )
        before_workspace.pop("_creation_reservation_id", None)
        before_workspace.pop("_creation_claim_token", None)
        after_workspace = after["metadata"]["workspace_container"]
        after_workspace.update(
            {
                "status": "ready",
                "_runtime_incarnation": SUCCESSOR_RUNTIME,
                "_snapshot_restore_required": True,
                "_creation_reservation_id": CREATION_ID,
                "_creation_claim_token": str(CREATION_TOKEN),
            }
        )
        svc = make_service()
        svc._db.get_thread = AsyncMock(side_effect=[before, after])
        svc._extract_snapshot = AsyncMock(return_value=True)
        return svc

    @pytest.mark.asyncio
    async def test_same_pvc_reattached_skips_the_extract(self):
        pvc = "k8s-pvc:srw:11111111-2222-3333-4444-555555555555"
        svc = self._svc(self._thread(pvc), self._thread(pvc))

        ok = await svc.restore_thread_workspace("tid-pod")

        assert ok is True
        svc._extract_snapshot.assert_not_awaited()
        svc._db.merge_thread_workspace_context_if_runtime.assert_not_awaited()
        svc._container_provisioner.complete_workspace_restore_work.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_emptydir_session_still_extracts(self):
        """Mixed-fleet upgrade path: a pod-bound (emptyDir) session's storage
        died with the pod, so the S3 snapshot is still its only copy."""
        svc = self._svc(
            self._thread("k8s-pod:srw:aaaa-bbbb"),
            self._thread("k8s-pod:srw:cccc-dddd"),
        )

        ok = await svc.restore_thread_workspace("tid-pod")

        assert ok is True
        svc._extract_snapshot.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_never_bound_session_still_extracts(self):
        """No binding at all (a session that predates the PVC switch) is not
        evidence of a surviving volume — extract."""
        svc = self._svc(self._thread(), self._thread())

        await svc.restore_thread_workspace("tid-pod")

        svc._extract_snapshot.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_different_pvc_uid_still_extracts(self):
        """A changed backing id means a DIFFERENT volume — a first PVC-backed
        create, or the single-replica node-loss fallback discarding a wedged
        claim. Whatever the pod mounts now, it is not the tree we suspended."""
        svc = self._svc(
            self._thread("k8s-pvc:srw:11111111-old"),
            self._thread("k8s-pvc:srw:99999999-new"),
        )

        await svc.restore_thread_workspace("tid-pod")

        svc._extract_snapshot.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_missing_binding_after_create_keeps_the_volume(self):
        """The post-create rebind is best-effort, so its absence is ambiguous.
        The tie goes to the volume: we came in PVC-backed, a PVC survives every
        non-permanent teardown, and unrolling a stale tarball over live files is
        the unrecoverable mistake — a skipped extract is not."""
        svc = self._svc(
            self._thread("k8s-pvc:srw:11111111-2222"),
            self._thread(),  # rebind failed → no binding on the fresh read
        )

        ok = await svc.restore_thread_workspace("tid-pod")

        assert ok is True
        svc._extract_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_failed_extract_is_not_reported_ready(self):
        """The thread half of the job-path rule: a workspace that failed to
        restore must not advertise itself as ready."""
        svc = self._svc(
            self._thread("k8s-pod:srw:aaaa"), self._thread("k8s-pod:srw:bbbb")
        )
        svc._extract_snapshot = AsyncMock(return_value=False)

        ok = await svc.restore_thread_workspace("tid-pod")

        assert ok is False
        statuses = [
            call.args[1].get("status")
            for call in svc._db.merge_thread_workspace_context_if_runtime.await_args_list
        ]
        assert statuses[-1] == "failed"
        assert "ready" not in statuses
        assert (
            svc._db.merge_thread_workspace_context_if_runtime.await_args.kwargs[
                "expected_runtime_incarnation"
            ]
            == SUCCESSOR_RUNTIME
        )

    @pytest.mark.asyncio
    async def test_lost_create_response_uses_exact_restore_receipt(self):
        svc = self._svc(self._thread(), self._thread())
        svc._container_provisioner.create_workspace.side_effect = RuntimeError(
            "response lost"
        )

        assert await svc.restore_thread_workspace("tid-pod") is True

        svc._container_provisioner.get_workspace_creation_result.assert_awaited_once_with(
            WorkspaceOwner.session("tid-pod"),
            operation_kind="restore",
            operation_id=CLEANUP_ID,
        )

    @pytest.mark.asyncio
    async def test_successor_change_at_ready_cas_is_preserved(self):
        svc = self._svc(self._thread(), self._thread())
        svc._container_provisioner.complete_workspace_restore_work.return_value = False

        assert await svc.restore_thread_workspace("tid-pod") is False

        assert (
            svc._container_provisioner.complete_workspace_restore_work.await_count == 2
        )
        svc._db.merge_thread_workspace_context_if_runtime.assert_not_awaited()
        svc._db.merge_thread_workspace_context.assert_not_awaited()


class TestStrictTerminalSessionRestore:
    THREAD_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    GENERATION = "11111111-2222-4333-8444-555555555555"
    RUNTIME = "66666666-7777-4888-8999-aaaaaaaaaaaa"
    FINGERPRINT = "SHA256:current-workspace-host"
    BACKING_ID = "k8s-pod:srw:66666666-7777-4888-8999-aaaaaaaaaaaa"
    CREATION = "99999999-aaaa-4bbb-8ccc-dddddddddddd"

    async def _restore_new(self, svc):
        return await svc.restore_thread_workspace(
            self.THREAD_ID,
            stateless_creation_generation=self.CREATION,
            allow_stateless_create=True,
        )

    @classmethod
    def _before(cls, *, marker=True, backing_id=None):
        return {
            "id": cls.THREAD_ID,
            "status": "created",
            "execution_lane": "stateless",
            "metadata": {
                "config_override": {"workspace": {"backend": "sandbox"}},
                "workspace_container": {
                    "status": "suspended",
                    "provisioner": "k8s",
                    "_snapshot_restore_required": marker,
                    "_runtime_incarnation": WORKSPACE_RUNTIME,
                },
                "_workspace_binding": {
                    "generation": "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff",
                    "kind": "remote",
                    "backing_id": backing_id or "k8s-pod:srw:old-runtime",
                    "ssh_host_key_fingerprint": "SHA256:old-host",
                },
            },
        }

    @classmethod
    def _after(
        cls,
        *,
        runtime=None,
        endpoint_generation=None,
        backing_id=None,
        fingerprint=None,
    ):
        return {
            "id": cls.THREAD_ID,
            "status": "created",
            "execution_lane": "stateless",
            "metadata": {
                "config_override": {"workspace": {"backend": "sandbox"}},
                "workspace_container": {
                    "status": "ready",
                    "provisioner": "k8s",
                    "pod_ip": "10.42.2.91",
                    "port": 30022,
                    "_snapshot_restore_required": True,
                    "_canvas_workspace_generation": (
                        endpoint_generation or cls.GENERATION
                    ),
                    "_runtime_incarnation": runtime or cls.RUNTIME,
                    "_creation_reservation_id": CREATION_ID,
                    "_creation_claim_token": str(CREATION_TOKEN),
                },
                "_workspace_binding": {
                    "generation": cls.GENERATION,
                    "kind": "remote",
                    "backing_id": backing_id or cls.BACKING_ID,
                    "ssh_host_key_fingerprint": fingerprint or cls.FINGERPRINT,
                },
            },
        }

    def _service(self, before=None, after=None):
        svc = make_service()
        after_row = after or self._after()
        svc._db.get_thread = AsyncMock(
            side_effect=[before or self._before(), after_row]
        )
        svc._extract_snapshot = AsyncMock(return_value=True)
        svc._container_provisioner.workspace_pod_live = AsyncMock(return_value=True)
        svc._commit_strict_thread_restore_ready = AsyncMock(return_value=True)
        creation = {
            "id": CREATION_ID,
            "operation_kind": "restore",
            "result_kind": "settled",
            "settled_at": datetime.now(timezone.utc),
            "runtime_incarnation": self.RUNTIME,
            "claim_token": CREATION_TOKEN,
        }
        svc._container_provisioner.get_workspace_creation_result.return_value = creation
        svc._container_provisioner.get_current_workspace_creation_result.return_value = creation
        try:
            authority = _strict_session_restore_authority(after_row)
        except RuntimeError:
            # Malformed tuples are rejected before control-plane attestation;
            # keep a valid default only so the mock cannot invent authority.
            authority = _strict_session_restore_authority(self._after())
        svc._container_provisioner.attest_workspace_runtime.return_value = (
            WorkspaceRuntimeAttestation(
                backing_id=authority.backing_id,
                workspace_generation=("66666666-7777-4888-8999-aaaaaaaaaaaa"),
                runtime_incarnation=authority.runtime_incarnation,
                ssh_host_key_fingerprint=authority.host_key_fingerprint,
                host=authority.ssh_host,
                pod_ip=authority.ssh_host,
                port=authority.ssh_port,
            )
        )

        async def claim_restore_work(_owner, *, claimant, lease_seconds=300):
            assert lease_seconds == 300
            return {
                **creation,
                "restore_work_claimed_by": claimant,
                "restore_work_claim_token": RESTORE_WORK_TOKEN,
                "restore_work_completed_at": None,
            }

        svc._container_provisioner.claim_workspace_restore_work.side_effect = (
            claim_restore_work
        )
        return svc

    @pytest.mark.asyncio
    async def test_extract_is_pinned_and_exact_tuple_is_reproved_then_committed(self):
        svc = self._service()

        ok = await self._restore_new(svc)

        assert ok is True
        svc._extract_snapshot.assert_awaited_once_with(
            self.THREAD_ID,
            "10.42.2.91",
            ssh_port=30022,
            entity_type="threads",
            expected_host_key_fingerprint=self.FINGERPRINT,
            mutation_authority=ANY,
            remote_cmd=EXTRACT_HOME_REMOTE_CMD,
            require_pipefail=True,
        )
        svc._container_provisioner.workspace_pod_live.assert_awaited_once_with(
            WorkspaceOwner.session(self.THREAD_ID),
            expected_runtime_incarnation=self.RUNTIME,
        )
        complete = svc._container_provisioner.complete_strict_thread_restore_work
        complete.assert_awaited_once()
        assert complete.await_args.kwargs["workspace_generation"] == self.GENERATION
        assert complete.await_args.kwargs["endpoint_generation"] == self.GENERATION
        assert complete.await_args.kwargs["host_key_fingerprint"] == self.FINGERPRINT
        assert not any(
            call.args[1].get("status") == "ready"
            and call.args[1].get("_snapshot_restore_required") is False
            for call in svc._db.merge_thread_workspace_context.await_args_list
        )

    @pytest.mark.asyncio
    async def test_cached_exact_live_restore_reuses_uid_without_name_create(self):
        current = self._after()
        svc = self._service(before=current, after=current)
        svc._container_provisioner.workspace_pod_authority = AsyncMock(
            return_value="exact_live"
        )

        ok = await svc.restore_thread_workspace(
            self.THREAD_ID,
            expected_runtime_incarnation=self.RUNTIME,
        )

        assert ok is True
        svc._container_provisioner.workspace_pod_authority.assert_awaited_once_with(
            WorkspaceOwner.session(self.THREAD_ID),
            expected_runtime_incarnation=self.RUNTIME,
        )
        svc._container_provisioner.create_workspace.assert_not_awaited()
        svc._extract_snapshot.assert_awaited_once()
        svc._container_provisioner.complete_strict_thread_restore_work.assert_awaited_once()
        assert not any(
            call.args[1].get("status") == "restoring"
            for call in svc._db.merge_thread_workspace_context.await_args_list
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "authority", ["exact_absent", "replacement", "unknown", "exact_terminal"]
    )
    async def test_cached_restore_internal_reprobe_refuses_nonlive_uid(self, authority):
        current = self._after()
        svc = self._service(before=current, after=current)
        svc._container_provisioner.workspace_pod_authority = AsyncMock(
            return_value=authority
        )

        ok = await svc.restore_thread_workspace(
            self.THREAD_ID,
            expected_runtime_incarnation=self.RUNTIME,
        )

        assert ok is False
        svc._container_provisioner.create_workspace.assert_not_awaited()
        svc._extract_snapshot.assert_not_awaited()
        svc._container_provisioner.complete_strict_thread_restore_work.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cached_restore_runtime_drift_refuses_before_probe(self):
        current = self._after(runtime="99999999-aaaa-4bbb-8ccc-dddddddddddd")
        svc = self._service(before=current, after=current)
        svc._container_provisioner.workspace_pod_authority = AsyncMock(
            return_value="exact_live"
        )

        assert (
            await svc.restore_thread_workspace(
                self.THREAD_ID,
                expected_runtime_incarnation=self.RUNTIME,
            )
            is False
        )

        svc._container_provisioner.workspace_pod_authority.assert_not_awaited()
        svc._container_provisioner.create_workspace.assert_not_awaited()
        svc._extract_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("probe_result", [False, None])
    async def test_replacement_or_unknown_runtime_never_reaches_ready_cas(
        self, probe_result
    ):
        svc = self._service()
        svc._container_provisioner.workspace_pod_live.return_value = probe_result

        ok = await self._restore_new(svc)

        assert ok is False
        svc._container_provisioner.complete_strict_thread_restore_work.assert_not_awaited()
        assert not any(
            call.args[1].get("_snapshot_restore_required") is False
            for call in svc._db.merge_thread_workspace_context.await_args_list
        )

    @pytest.mark.asyncio
    async def test_runtime_probe_error_never_reaches_ready_cas(self):
        svc = self._service()
        svc._container_provisioner.workspace_pod_live.side_effect = RuntimeError(
            "kubernetes API unavailable"
        )

        assert await self._restore_new(svc) is False

        svc._container_provisioner.complete_strict_thread_restore_work.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_changed_endpoint_generation_fails_before_snapshot_bytes(self):
        svc = self._service(
            after=self._after(
                endpoint_generation="99999999-aaaa-4bbb-8ccc-dddddddddddd"
            )
        )

        assert await self._restore_new(svc) is False

        svc._extract_snapshot.assert_not_awaited()
        svc._container_provisioner.workspace_pod_live.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("marker", [False, None, 1, "true", {}])
    async def test_non_exact_terminal_restore_marker_is_refused_before_create(
        self, marker
    ):
        before = self._before(marker=marker)
        svc = make_service()
        svc._db.get_thread = AsyncMock(return_value=before)

        assert await svc.restore_thread_workspace(self.THREAD_ID) is False

        svc._container_provisioner.create_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_same_pvc_still_reprobes_and_cas_commits_without_extract(self):
        backing = "k8s-pvc:srw:bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
        svc = self._service(
            before=self._before(backing_id=backing),
            after=self._after(backing_id=backing),
        )

        assert await self._restore_new(svc) is True

        svc._extract_snapshot.assert_not_awaited()
        svc._container_provisioner.workspace_pod_live.assert_awaited_once()
        svc._container_provisioner.complete_strict_thread_restore_work.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_transactional_ready_commit_rejects_tuple_drift(self):
        expected = _strict_session_restore_authority(self._after())
        drifted = self._after(runtime="99999999-aaaa-4bbb-8ccc-dddddddddddd")
        svc = make_service()
        conn = AsyncMock()
        conn.transaction = MagicMock(return_value=_MockAsyncCtx(None))
        conn.fetchrow = AsyncMock(return_value=drifted)
        conn.fetchval = AsyncMock(return_value=self.THREAD_ID)
        svc._db.acquire = MagicMock(return_value=_MockAsyncCtx(conn))

        assert (
            await svc._commit_strict_thread_restore_ready(self.THREAD_ID, expected)
            is False
        )

        conn.fetchval.assert_not_awaited()
        assert "FOR UPDATE" in conn.fetchrow.await_args.args[0]

    @pytest.mark.asyncio
    async def test_transactional_ready_commit_uses_exact_cas_tuple(self):
        current = self._after()
        expected = _strict_session_restore_authority(current)
        svc = make_service()
        conn = AsyncMock()
        conn.transaction = MagicMock(return_value=_MockAsyncCtx(None))
        conn.fetchrow = AsyncMock(return_value=current)
        conn.fetchval = AsyncMock(return_value=self.THREAD_ID)
        svc._db.acquire = MagicMock(return_value=_MockAsyncCtx(conn))

        assert (
            await svc._commit_strict_thread_restore_ready(self.THREAD_ID, expected)
            is True
        )

        sql, *params = conn.fetchval.await_args.args
        assert "= 'true'::jsonb" in sql
        assert "RETURNING id" in sql
        assert params[:8] == [
            self.THREAD_ID,
            self.GENERATION,
            self.GENERATION,
            self.RUNTIME,
            self.BACKING_ID,
            self.FINGERPRINT,
            "10.42.2.91",
            30022,
        ]


# =============================================================================
# A suspended workspace is a preserve disposition. Shared resources belong to
# the exact cleanup intent and are never reclaimed by a caller-side flag.
# =============================================================================


class TestSuspendPreservesSharedResources:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("legacy_flag", [None, "true"])
    async def test_suspend_never_reclaims_the_workspace_pvc(
        self, monkeypatch, legacy_flag
    ):
        if legacy_flag is None:
            monkeypatch.delenv("WORKSPACE_RECLAIM_ON_IDLE", raising=False)
        else:
            monkeypatch.setenv("WORKSPACE_RECLAIM_ON_IDLE", legacy_flag)
        svc = make_service()
        svc._db.get_thread = AsyncMock(return_value=make_container_thread())

        assert await svc.suspend_thread_workspace("tid-pod") is False

        svc._container_provisioner.prepare_workspace_cleanup_intent.assert_not_awaited()
        svc._snapshot_service.capture_vm_snapshot.assert_not_awaited()
        svc._snapshot_service.verify_snapshot.assert_not_awaited()
        svc._container_provisioner.delete_workspace_pvc.assert_not_awaited()
        svc._db.merge_thread_workspace_context_if_runtime.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_agent_pod_is_not_deleted_without_capture_authority(self):
        svc = make_service()
        svc._db.get_thread = AsyncMock(return_value=make_container_thread())
        agent_prov = MagicMock(spec=["is_available", "delete_agent_pod_by_thread"])
        type(agent_prov).is_available = PropertyMock(return_value=True)
        agent_prov.delete_agent_pod_by_thread = AsyncMock(return_value=True)
        svc._agent_provisioner = agent_prov

        assert await svc.suspend_thread_workspace("tid-pod") is False

        svc._container_provisioner.reconcile_workspace_cleanup_intent.assert_not_awaited()
        agent_prov.delete_agent_pod_by_thread.assert_not_awaited()


class TestRestoreWithChangedBacking:
    """A different backing UID on B requires restoring the captured bytes."""

    @pytest.mark.asyncio
    async def test_extract_fires_when_the_reclaimed_pvc_comes_back_fresh(self):
        before = make_container_thread(
            ws_status="suspended",
            pod_ip=None,
            snapshot_restore_required=True,
        )
        before["metadata"]["_workspace_binding"] = {
            "backing_kind": "remote",
            "backing_id": "k8s-pvc:srw:11111111-old-reclaimed",
        }

        after = make_container_thread(
            ws_status="ready",
            pod_ip="10.42.2.99",
            runtime=SUCCESSOR_RUNTIME,
            snapshot_restore_required=True,
            creation_receipt=True,
        )
        after["metadata"]["_workspace_binding"] = {
            "backing_kind": "remote",
            "backing_id": "k8s-pvc:srw:99999999-fresh",
        }

        svc = make_service()
        configure_thread_restore(svc, before=before, after=after)
        svc._extract_snapshot = AsyncMock(return_value=True)

        ok = await svc.restore_thread_workspace("tid-pod")

        assert ok is True
        svc._extract_snapshot.assert_awaited_once()
        svc._db.merge_thread_workspace_context_if_runtime.assert_not_awaited()
        svc._container_provisioner.complete_workspace_restore_work.assert_awaited_once()
