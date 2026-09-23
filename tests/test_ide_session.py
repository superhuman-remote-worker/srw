"""Unit tests for IDE session restore routing and container clone behavior."""

import asyncio
import logging
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest


IDE_RUNTIME = "11111111-1111-4111-8111-111111111111"
IDE_RESERVATION = "22222222-2222-4222-8222-222222222222"
IDE_CLAIM_TOKEN = 7
IDE_WORK_TOKEN = 11


@pytest.fixture(autouse=True)
def legacy_resource_cleanup(monkeypatch):
    """Synthetic IDE cleanup owners have no v3 resource charge."""
    import orchestrator.services.vm_workspace_recovery_store as recovery

    monkeypatch.setattr(
        recovery, "prepare_vm_cleanup_resource", AsyncMock(return_value=None)
    )


class _ExactIdeRuntimeDB:
    """Small concrete DB seam so permissive mocks cannot invent CAS authority."""

    def __init__(self, job: dict, *, cas_result: bool = True):
        self.job = job
        self.cas_result = cas_result
        self.cas_calls: list[tuple[str, dict, str]] = []
        self.unconditional_calls: list[tuple[str, dict]] = []

    async def get_job(self, job_id: str) -> dict:
        assert job_id == self.job["id"]
        return self.job

    async def merge_ide_session_context_if_runtime(
        self,
        job_id: str,
        session_updates: dict,
        *,
        expected_runtime_incarnation: str,
    ) -> bool:
        self.cas_calls.append((job_id, session_updates, expected_runtime_incarnation))
        return self.cas_result

    async def merge_ide_session_context(
        self, job_id: str, session_updates: dict
    ) -> bool:
        self.unconditional_calls.append((job_id, session_updates))
        return True


@pytest.fixture
def browser_ide_transport_available(monkeypatch):
    """Present a deployment whose workspace is credential-bound.

    Both halves are needed: a root key, so the cheap environment gate passes,
    and a resolved target carrying a credential, so the per-workspace check
    does too. Tests that care about the *underlying* status resolution take
    this fixture so the advertisement layer stays out of their way.
    """

    from types import SimpleNamespace
    from unittest.mock import AsyncMock as _AsyncMock

    import orchestrator.services.ide_proxy as ide_proxy

    monkeypatch.setenv("IDE_CREDENTIAL_KEY", "test-root-key")
    monkeypatch.setattr(
        ide_proxy.ide_proxy_service,
        "resolve_target",
        _AsyncMock(return_value=SimpleNamespace(backend="k8s", credential="bound")),
    )


@pytest.fixture
def service_factory():
    from orchestrator.services.ide_session import IdeSessionService
    from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation

    db = AsyncMock()
    db.merge_ide_session_context = AsyncMock()
    db.get_job = AsyncMock(
        side_effect=lambda job_id: {
            "id": job_id,
            "context": {
                "ide_session": {
                    "status": "restoring",
                    "restore_type": "k8s_container",
                    "pod_ip": "10.0.0.10",
                    "_runtime_incarnation": IDE_RUNTIME,
                    "_creation_reservation_id": IDE_RESERVATION,
                    "_creation_claim_token": str(IDE_CLAIM_TOKEN),
                }
            },
        }
    )

    container_provisioner = AsyncMock()
    container_provisioner.is_available = True
    creation = {
        "id": IDE_RESERVATION,
        "operation_kind": "restore",
        "result_kind": "settled",
        "settled_at": "2026-08-27T00:00:00+00:00",
        "runtime_incarnation": IDE_RUNTIME,
        "claim_token": IDE_CLAIM_TOKEN,
    }
    container_provisioner.get_current_ide_creation_result = AsyncMock(
        return_value=creation
    )
    container_provisioner.get_ide_creation_result = AsyncMock(return_value=creation)

    async def claim_restore_work(_job_id, *, claimant, lease_seconds=300):
        assert lease_seconds == 300
        return {
            **creation,
            "restore_work_claimed_by": claimant,
            "restore_work_claim_token": IDE_WORK_TOKEN,
            "restore_work_completed_at": None,
        }

    container_provisioner.claim_ide_restore_work = AsyncMock(
        side_effect=claim_restore_work
    )
    container_provisioner.renew_ide_restore_work = AsyncMock(
        side_effect=lambda _job_id, *, restore_work, **_kwargs: restore_work
    )
    container_provisioner.attest_ide_runtime = AsyncMock(
        return_value=WorkspaceRuntimeAttestation(
            backing_id=f"k8s-pod:test:{IDE_RUNTIME}",
            workspace_generation=IDE_RUNTIME,
            runtime_incarnation=IDE_RUNTIME,
            ssh_host_key_fingerprint=(
                "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            ),
            host="10.0.0.10",
            pod_ip="10.0.0.10",
        )
    )
    container_provisioner.release_ide_restore_work = AsyncMock(return_value=True)
    container_provisioner.complete_ide_restore_work = AsyncMock(return_value=True)
    container_provisioner.ide_pod_live = AsyncMock(return_value=True)

    vm_provisioner = AsyncMock()
    vm_provisioner.is_available = True

    svc = IdeSessionService()
    svc.connect(
        db=db,
        snapshot_service=AsyncMock(),
        vm_provisioner=vm_provisioner,
        gitea_client=None,
        container_provisioner=container_provisioner,
    )
    recovery_store = MagicMock()
    recovery_store.acquire_cleanup_permit = AsyncMock(
        return_value=MagicMock(allowed=True, admission_id=None)
    )
    svc._workspace_recovery_store = recovery_store
    return svc


def _k8s_ide_job(*, job_id: str, runtime_incarnation: str) -> dict:
    return {
        "id": job_id,
        "context": {
            "ide_session": {
                "status": "active",
                "restore_type": "k8s_container",
                "container_name": f"ide-{job_id[:12]}",
                "_runtime_incarnation": runtime_incarnation,
                "code_server_url": "https://ide.invalid",
            }
        },
    }


@pytest.mark.asyncio
async def test_vm_ide_delete_stands_down_for_workspace_recovery(
    service_factory,
) -> None:
    from types import SimpleNamespace

    job_id = "11111111-1111-4111-8111-111111111111"
    svc = service_factory
    svc._vm_provisioner.lifecycle_available = True
    svc._vm_provisioner.capture_vm_teardown_identity = AsyncMock(
        return_value=SimpleNamespace(
            provision_generation="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            vm_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            rootdisk_pvc_uid="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        )
    )
    svc._vm_provisioner.release_vm_captured = AsyncMock()
    svc._vm_provisioner.delete_vm = AsyncMock(return_value=True)
    recovery_store = MagicMock()
    recovery_store.acquire_cleanup_permit = AsyncMock(
        return_value=SimpleNamespace(
            allowed=False,
            reason="workspace_recovery_unresolved",
        )
    )
    svc._workspace_recovery_store = recovery_store

    assert not await svc._delete_ide_vm(job_id, "ide-vm")

    recovery_store.acquire_cleanup_permit.assert_awaited_once()
    svc._vm_provisioner.release_vm_captured.assert_not_awaited()
    svc._vm_provisioner.delete_vm.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_k8s_ide_stale_deletion_is_superseded_without_context_mutation(
    service_factory,
):
    from orchestrator.services.container_provisioner import RuntimeDeletionOutcome

    job_id = "11111111-1111-4111-8111-111111111111"
    runtime = "22222222-2222-4222-8222-222222222222"
    svc = service_factory
    db = _ExactIdeRuntimeDB(_k8s_ide_job(job_id=job_id, runtime_incarnation=runtime))
    svc._db = db
    svc._delete_k8s_ide_container_with_outcome = AsyncMock(
        return_value=RuntimeDeletionOutcome("stale_target_settled")
    )

    result = await svc.stop_session(job_id)

    assert result == {
        "status": "superseded",
        "job_id": job_id,
        "retryable": False,
    }
    assert db.cas_calls == []
    assert db.unconditional_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome_state", "expected_status", "expected_result"),
    (
        ("current_deleted", "expired", "stopped"),
        ("refused", "cleanup_pending", "cleanup_pending"),
    ),
)
async def test_stop_k8s_ide_projects_outcome_through_exact_runtime_cas(
    service_factory,
    outcome_state,
    expected_status,
    expected_result,
):
    from orchestrator.services.container_provisioner import RuntimeDeletionOutcome

    job_id = "33333333-3333-4333-8333-333333333333"
    runtime = "44444444-4444-4444-8444-444444444444"
    svc = service_factory
    db = _ExactIdeRuntimeDB(_k8s_ide_job(job_id=job_id, runtime_incarnation=runtime))
    svc._db = db
    svc._delete_k8s_ide_container_with_outcome = AsyncMock(
        return_value=RuntimeDeletionOutcome(outcome_state)
    )

    result = await svc.stop_session(job_id)

    assert result["status"] == expected_result
    assert len(db.cas_calls) == 1
    observed_job, updates, observed_runtime = db.cas_calls[0]
    assert observed_job == job_id
    assert observed_runtime == runtime
    assert updates["status"] == expected_status
    assert db.unconditional_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome_state", ("current_deleted", "refused"))
async def test_stop_k8s_ide_cas_loss_preserves_successor(
    service_factory,
    outcome_state,
):
    from orchestrator.services.container_provisioner import RuntimeDeletionOutcome

    job_id = "55555555-5555-4555-8555-555555555555"
    runtime = "66666666-6666-4666-8666-666666666666"
    svc = service_factory
    db = _ExactIdeRuntimeDB(
        _k8s_ide_job(job_id=job_id, runtime_incarnation=runtime),
        cas_result=False,
    )
    svc._db = db
    svc._delete_k8s_ide_container_with_outcome = AsyncMock(
        return_value=RuntimeDeletionOutcome(outcome_state)
    )

    result = await svc.stop_session(job_id)

    assert result == {
        "status": "superseded",
        "job_id": job_id,
        "retryable": False,
    }
    assert len(db.cas_calls) == 1
    assert db.cas_calls[0][2] == runtime
    assert db.unconditional_calls == []


@pytest.mark.asyncio
async def test_restore_session_uses_snapshot_container_for_pod_snapshot(
    service_factory,
):
    """Pod snapshots should go through the IDE pod restore path."""
    svc = service_factory
    svc._restore_snapshot_container = AsyncMock(return_value=True)
    svc._restore_gitea_container = AsyncMock(return_value=True)
    svc._restore_vm_session = AsyncMock()

    svc._container_provisioner.is_available = True
    job = {
        "id": "job-0001",
        "context": {"snapshot": {"status": "available", "source_type": "pod"}},
    }

    await svc._restore_session("job-0001", job, "snapshot", 8, "16Gi")

    svc._restore_snapshot_container.assert_awaited_once()
    assert svc._restore_snapshot_container.await_args.args == ("job-0001", job)
    assert (
        svc._restore_snapshot_container.await_args.kwargs["settle_restore_work"]
        is False
    )
    svc._restore_vm_session.assert_not_awaited()
    svc._restore_gitea_container.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_session_falls_back_to_gitea_when_pod_snapshot_restore_fails(
    service_factory,
):
    """If pod snapshot restore fails, fallback to Gitea when possible."""
    svc = service_factory
    svc._restore_snapshot_container = AsyncMock(return_value=False)
    svc._restore_gitea_container = AsyncMock(return_value=True)
    svc._restore_vm_session = AsyncMock()

    svc._container_provisioner.is_available = True
    job = {
        "id": "job-0002",
        "repo_name": "demo/repo",
        "context": {"snapshot": {"status": "available", "source_type": "pod"}},
    }

    await svc._restore_session("job-0002", job, "snapshot", 8, "16Gi")

    svc._restore_snapshot_container.assert_awaited_once()
    svc._restore_gitea_container.assert_awaited_once()
    assert svc._restore_snapshot_container.await_args.args == ("job-0002", job)
    assert svc._restore_gitea_container.await_args.args == ("job-0002", job)
    svc._restore_vm_session.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_session_routes_vm_snapshot_to_vm(service_factory):
    """VM snapshots stay on VM restore path."""
    svc = service_factory
    svc._restore_snapshot_container = AsyncMock(return_value=True)
    svc._restore_gitea_container = AsyncMock(return_value=True)
    svc._restore_vm_session = AsyncMock()

    job = {
        "id": "job-0003",
        "context": {"snapshot": {"status": "available", "source_type": "vm"}},
    }

    await svc._restore_session("job-0003", job, "snapshot", 8, "16Gi")

    svc._restore_vm_session.assert_awaited_once_with(
        "job-0003",
        job,
        "snapshot",
        8,
        "16Gi",
    )
    svc._restore_snapshot_container.assert_not_awaited()
    svc._restore_gitea_container.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_k8s_ide_container_clone_failure_releases_exact_b_for_retry(
    service_factory,
):
    """A transient Gitea failure must not absorb the durable restore receipt."""
    svc = service_factory
    svc._container_provisioner.create_ide_pod = AsyncMock(return_value="10.0.0.10")
    svc._install_and_sync_managed_repository_over_ssh = AsyncMock(return_value=False)
    svc._set_session_context_if_runtime = AsyncMock(return_value=True)

    ok = await svc._restore_k8s_ide_container(
        "job-0004",
        {"branch_name": "main", "repo_name": "demo/repo"},
        "demo/repo",
        "main",
    )

    assert ok is False
    svc._install_and_sync_managed_repository_over_ssh.assert_awaited_once_with(
        "job-0004",
        backend="sandbox",
        ssh_host="10.0.0.10",
        ssh_port=30022,
        branch="main",
        workspace_path="/home/agent-host/workspace",
        expected_host_key_fingerprint=(
            "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ),
        mutation_authority=ANY,
    )
    svc._container_provisioner.complete_ide_restore_work.assert_not_awaited()
    svc._container_provisioner.delete_ide_pod.assert_not_awaited()
    svc._container_provisioner.release_ide_restore_work.assert_awaited_once_with(
        "job-0004",
        restore_work=ANY,
        claimant=ANY,
        retry_seconds=30,
    )


@pytest.mark.asyncio
async def test_ide_repository_key_stays_local_when_post_attestation_lease_is_lost(
    service_factory,
):
    """A reclaimed exact-B token receives no repository-key byte."""

    svc = service_factory
    secret = bytearray(b"private-key")
    svc._managed_repository_payload = AsyncMock(return_value={})
    svc._managed_git_command = MagicMock(return_value=("git-command", secret))
    svc._run_secret_stdin_process = AsyncMock(return_value=True)
    svc._set_session_context_if_runtime = AsyncMock(return_value=True)
    renew_calls = 0

    async def renew(_job_id, *, restore_work, **_kwargs):
        nonlocal renew_calls
        renew_calls += 1
        # Heartbeat entry and the tail's first attestation are current. The
        # key-specific fresh attestation/re-lock then observes reclamation.
        return restore_work if renew_calls <= 2 else None

    svc._container_provisioner.renew_ide_restore_work.side_effect = renew

    assert not await svc._restore_k8s_ide_container(
        "job-key-fenced",
        {"repo_name": "demo/repo", "branch_name": "main"},
        "demo/repo",
        "main",
    )

    assert renew_calls == 3
    svc._run_secret_stdin_process.assert_not_awaited()
    assert bytes(secret) == b"\x00" * len(secret)


@pytest.mark.asyncio
async def test_ide_repository_key_refuses_raw_coordinate_before_loading_secret(
    service_factory,
):
    svc = service_factory
    svc._managed_repository_payload = AsyncMock(
        side_effect=AssertionError("raw target must not load repository authority")
    )

    assert not await svc._install_and_sync_managed_repository_over_ssh(
        "job-raw-coordinate",
        backend="vm",
        ssh_host="10.0.0.10",
        ssh_port=22,
        branch="main",
        workspace_path="/home/agent-host/workspace",
    )

    svc._managed_repository_payload.assert_not_awaited()


@pytest.mark.asyncio
async def test_ide_repository_key_rejects_reused_ip_host_key_before_stdin(
    service_factory,
):
    """A same-IP successor with a different SSH key receives zero bytes."""

    svc = service_factory
    secret = bytearray(b"private-key")
    svc._managed_repository_payload = AsyncMock(return_value={})
    svc._managed_git_command = MagicMock(return_value=("git-command", secret))
    svc._run_secret_stdin_process = AsyncMock(return_value=True)
    attestation = svc._container_provisioner.attest_ide_runtime.return_value

    with patch(
        "orchestrator.services.ssh_helpers._scan_pinned_host_key",
        new=AsyncMock(return_value=(None, b"SSH host key mismatch")),
    ):
        assert not await svc._install_and_sync_managed_repository_over_ssh(
            "job-reused-ip",
            backend="sandbox",
            ssh_host="10.0.0.10",
            ssh_port=30022,
            branch="main",
            workspace_path="/home/agent-host/workspace",
            expected_host_key_fingerprint=(attestation.ssh_host_key_fingerprint),
            mutation_authority=AsyncMock(return_value=attestation),
        )

    svc._run_secret_stdin_process.assert_not_awaited()
    assert bytes(secret) == b"\x00" * len(secret)


@pytest.mark.asyncio
async def test_ide_post_write_uid_replacement_cannot_settle_restore(service_factory):
    from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation

    svc = service_factory
    exact_b = svc._container_provisioner.attest_ide_runtime.return_value
    successor_c = WorkspaceRuntimeAttestation(
        backing_id="k8s-pod:test:33333333-3333-4333-8333-333333333333",
        workspace_generation="33333333-3333-4333-8333-333333333333",
        runtime_incarnation="33333333-3333-4333-8333-333333333333",
        ssh_host_key_fingerprint=("SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
        host=exact_b.host,
        pod_ip=exact_b.pod_ip,
    )
    svc._container_provisioner.attest_ide_runtime.side_effect = [
        exact_b,
        successor_c,
    ]
    svc._install_and_sync_managed_repository_over_ssh = AsyncMock(return_value=True)
    svc._wait_for_code_server = AsyncMock(return_value=True)
    svc._set_session_context_if_runtime = AsyncMock(return_value=True)

    assert not await svc._restore_k8s_ide_container(
        "job-post-write-c",
        {"repo_name": "demo/repo", "branch_name": "main"},
        "demo/repo",
        "main",
    )

    svc._install_and_sync_managed_repository_over_ssh.assert_awaited_once()
    svc._container_provisioner.complete_ide_restore_work.assert_not_awaited()
    svc._container_provisioner.ide_pod_live.assert_not_awaited()


@pytest.mark.asyncio
async def test_repository_key_ssh_is_killed_and_reaped_on_lease_cancellation(
    service_factory,
):
    """Heartbeat cancellation cannot leave a detached Git/key subprocess."""

    svc = service_factory
    process = MagicMock(returncode=None)
    process.stdin = MagicMock()
    process.stdin.drain = AsyncMock(return_value=None)
    started = asyncio.Event()
    blocker = asyncio.Event()

    async def wait():
        started.set()
        if process.returncode is None:
            await blocker.wait()
        return process.returncode

    def kill():
        process.returncode = -9
        blocker.set()

    process.wait = AsyncMock(side_effect=wait)
    process.kill = MagicMock(side_effect=kill)
    secret = bytearray(b"private-key")

    with patch(
        "orchestrator.services.ide_session.asyncio.create_subprocess_exec",
        new=AsyncMock(return_value=process),
    ):
        task = asyncio.create_task(svc._run_secret_stdin_process(["ssh"], secret))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    process.kill.assert_called_once()
    assert process.wait.await_count >= 2
    assert bytes(secret) == b"\x00" * len(secret)


@pytest.mark.asyncio
async def test_restore_session_uses_long_vm_wait_estimate(
    service_factory, browser_ide_transport_available
):
    """VM source snapshots report the longer, realistic restore estimate."""
    svc = service_factory
    svc._db.get_job = AsyncMock(
        return_value={
            "id": "job-0005",
            "context": {"snapshot": {"status": "available", "source_type": "vm"}},
        }
    )
    svc._restore_session = AsyncMock(return_value=True)

    result = await svc.start_session("job-0005")

    assert result["status"] == "restoring"
    assert result["estimated_seconds"] == 420


@pytest.mark.asyncio
async def test_start_expired_k8s_ide_never_reactivates_runtime_a(
    service_factory, browser_ide_transport_available
):
    """The exact expired UID is the stable restore operation, not a row update."""

    retired = "33333333-3333-4333-8333-333333333333"
    svc = service_factory
    svc._db.get_job = AsyncMock(
        return_value={
            "id": "job-expired-a",
            "repo_name": "demo/repo",
            "context": {
                "ide_session": {
                    "status": "expired",
                    "restore_type": "k8s_container",
                    "_runtime_incarnation": retired,
                }
            },
        }
    )
    svc._restore_session = AsyncMock()

    result = await svc.start_session("job-expired-a")
    await asyncio.sleep(0)

    assert result["status"] == "restoring"
    svc._db.merge_ide_session_context.assert_not_awaited()
    assert svc._restore_session.await_args.kwargs["restore_operation_id"] == retired


@pytest.mark.asyncio
async def test_start_replays_receipt_backed_restoring_b_after_process_restart(
    service_factory, browser_ide_transport_available
):
    """A new service process resumes B instead of treating restoring as live work."""

    svc = service_factory
    svc._db.get_job = AsyncMock(
        return_value={
            "id": "job-restoring-b",
            "repo_name": "demo/repo",
            "context": {
                "ide_session": {
                    "status": "restoring",
                    "source": "gitea",
                    "snapshot_type": "gitea",
                    "restore_type": "k8s_container",
                    "pod_ip": "10.0.0.10",
                    "_runtime_incarnation": IDE_RUNTIME,
                    "_creation_reservation_id": IDE_RESERVATION,
                    "_creation_claim_token": str(IDE_CLAIM_TOKEN),
                    "estimated_seconds": 45,
                    "cpu_cores": 4,
                    "memory": "8Gi",
                }
            },
        }
    )
    svc._restore_session = AsyncMock()

    result = await svc.start_session("job-restoring-b")
    await asyncio.sleep(0)

    assert result["status"] == "restoring"
    svc._restore_session.assert_awaited_once_with(
        "job-restoring-b",
        ANY,
        "gitea",
        4,
        "8Gi",
        restore_operation_id=None,
        restore_context={"snapshot_type": "gitea"},
        expected_restore_runtime=IDE_RUNTIME,
    )
    svc._container_provisioner.create_ide_pod.assert_not_awaited()
    svc._db.merge_ide_session_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_start_reuses_one_local_receipt_backed_restore_task(
    service_factory, browser_ide_transport_available
):
    svc = service_factory
    svc._db.get_job = AsyncMock(
        return_value={
            "id": "job-restoring-b",
            "repo_name": "demo/repo",
            "context": {
                "ide_session": {
                    "status": "restoring",
                    "source": "gitea",
                    "snapshot_type": "gitea",
                    "restore_type": "k8s_container",
                    "pod_ip": "10.0.0.10",
                    "_runtime_incarnation": IDE_RUNTIME,
                    "_creation_reservation_id": IDE_RESERVATION,
                    "_creation_claim_token": str(IDE_CLAIM_TOKEN),
                }
            },
        }
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def restore(*_args, **_kwargs):
        started.set()
        await release.wait()

    svc._restore_session = AsyncMock(side_effect=restore)
    try:
        assert (await svc.start_session("job-restoring-b"))["status"] == "restoring"
        await started.wait()
        assert (await svc.start_session("job-restoring-b"))["status"] == "restoring"
        assert svc._restore_session.await_count == 1
    finally:
        release.set()
        await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("successor_won", (False, True))
async def test_snapshot_failure_releases_only_exact_receipt_b(
    service_factory,
    successor_won,
):
    """The no-repo failure cannot rewrite retired A or later successor C."""

    retired = "33333333-3333-4333-8333-333333333333"
    svc = service_factory
    svc._restore_snapshot_container = AsyncMock(return_value=False)
    svc._set_session_context_if_runtime = AsyncMock(return_value=True)
    if successor_won:
        svc._db.get_job = AsyncMock(
            return_value={
                "id": "job-snapshot-b",
                "context": {
                    "snapshot": {"status": "available", "source_type": "pod"},
                    "ide_session": {
                        "status": "restoring",
                        "restore_type": "k8s_container",
                        "pod_ip": "10.0.0.11",
                        "_runtime_incarnation": "44444444-4444-4444-8444-444444444444",
                        "_creation_reservation_id": "55555555-5555-4555-8555-555555555555",
                        "_creation_claim_token": "9",
                    },
                },
            }
        )

    job = {
        "id": "job-snapshot-b",
        "context": {"snapshot": {"status": "available", "source_type": "pod"}},
    }
    await svc._restore_session(
        "job-snapshot-b",
        job,
        "snapshot",
        8,
        "16Gi",
        restore_operation_id=retired,
        restore_context={"snapshot_type": "pod"},
    )

    svc._db.merge_ide_session_context.assert_not_awaited()
    svc._set_session_context_if_runtime.assert_not_awaited()
    svc._container_provisioner.complete_ide_restore_work.assert_not_awaited()
    if successor_won:
        svc._container_provisioner.release_ide_restore_work.assert_not_awaited()
    else:
        svc._container_provisioner.release_ide_restore_work.assert_awaited_once()


@pytest.mark.asyncio
async def test_expired_ide_a_creates_and_publishes_only_exact_b(service_factory):
    retired = "33333333-3333-4333-8333-333333333333"
    svc = service_factory
    svc._set_session_context_if_runtime = AsyncMock(return_value=True)
    svc._install_and_sync_managed_repository_over_ssh = AsyncMock(return_value=True)
    svc._wait_for_code_server = AsyncMock(return_value=True)

    assert await svc._restore_k8s_ide_container(
        "job-expired-a",
        {"repo_name": "demo/repo"},
        "demo/repo",
        "main",
        restore_operation_id=retired,
        restore_context={"started_at": "now"},
    )

    svc._container_provisioner.create_ide_pod.assert_awaited_once_with(
        "job-expired-a", operation_id=retired
    )
    svc._container_provisioner.get_ide_creation_result.assert_awaited_once_with(
        "job-expired-a", operation_id=retired
    )
    assert all(
        call.kwargs == {"expected_runtime_incarnation": IDE_RUNTIME}
        for call in svc._set_session_context_if_runtime.await_args_list
    )
    svc._db.merge_ide_session_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_ide_lost_create_response_reuses_same_b(service_factory):
    retired = "33333333-3333-4333-8333-333333333333"
    svc = service_factory
    svc._container_provisioner.create_ide_pod.side_effect = RuntimeError(
        "response lost after settlement"
    )
    svc._set_session_context_if_runtime = AsyncMock(return_value=True)
    svc._install_and_sync_managed_repository_over_ssh = AsyncMock(return_value=True)
    svc._wait_for_code_server = AsyncMock(return_value=True)

    assert await svc._restore_k8s_ide_container(
        "job-expired-a",
        {"repo_name": "demo/repo"},
        "demo/repo",
        "main",
        restore_operation_id=retired,
    )

    svc._container_provisioner.get_ide_creation_result.assert_awaited_once_with(
        "job-expired-a", operation_id=retired
    )


@pytest.mark.asyncio
async def test_ide_ready_cas_loss_preserves_successor_c(service_factory):
    svc = service_factory
    svc._set_session_context_if_runtime = AsyncMock(return_value=True)
    svc._container_provisioner.complete_ide_restore_work.return_value = False
    svc._install_and_sync_managed_repository_over_ssh = AsyncMock(return_value=True)
    svc._wait_for_code_server = AsyncMock(return_value=True)

    assert not await svc._restore_k8s_ide_container(
        "job-current-b",
        {"repo_name": "demo/repo"},
        "demo/repo",
        "main",
    )

    svc._container_provisioner.create_ide_pod.assert_not_awaited()
    assert svc._container_provisioner.complete_ide_restore_work.await_count == 2
    assert not any(
        call.args[1].get("status") == "active"
        for call in svc._set_session_context_if_runtime.await_args_list
    )
    svc._db.merge_ide_session_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_two_ide_service_instances_run_exact_b_effects_once(service_factory):
    """Two orchestrator replicas cannot concurrently mutate one restored Pod."""

    from orchestrator.services.ide_session import IdeSessionService

    first = service_factory
    second = IdeSessionService()
    second.connect(
        db=first._db,
        snapshot_service=first._snapshot_service,
        vm_provisioner=first._vm_provisioner,
        gitea_client=first._gitea_client,
        container_provisioner=first._container_provisioner,
    )
    shared = first._container_provisioner
    claimed = False

    async def claim(_job_id, *, claimant, lease_seconds=300):
        nonlocal claimed
        assert lease_seconds == 300
        if claimed:
            return None
        claimed = True
        return {
            **shared.get_current_ide_creation_result.return_value,
            "restore_work_claimed_by": claimant,
            "restore_work_claim_token": IDE_WORK_TOKEN,
            "restore_work_completed_at": None,
        }

    shared.claim_ide_restore_work.side_effect = claim
    started = asyncio.Event()
    finish = asyncio.Event()

    async def install(*_args, **_kwargs):
        started.set()
        await finish.wait()
        return True

    first._install_and_sync_managed_repository_over_ssh = AsyncMock(side_effect=install)
    first._wait_for_code_server = AsyncMock(return_value=True)
    first._set_session_context_if_runtime = AsyncMock(return_value=True)
    second._install_and_sync_managed_repository_over_ssh = AsyncMock(return_value=True)
    second._wait_for_code_server = AsyncMock(return_value=True)
    second._set_session_context_if_runtime = AsyncMock(return_value=True)
    job = {"repo_name": "demo/repo", "branch_name": "main"}

    first_task = asyncio.create_task(
        first._restore_k8s_ide_container("job-cross-replica", job, "demo/repo", "main")
    )
    await started.wait()
    assert not await second._restore_k8s_ide_container(
        "job-cross-replica", job, "demo/repo", "main"
    )
    finish.set()
    assert await first_task

    first._install_and_sync_managed_repository_over_ssh.assert_awaited_once()
    second._install_and_sync_managed_repository_over_ssh.assert_not_awaited()
    shared.complete_ide_restore_work.assert_awaited_once()


@pytest.mark.asyncio
async def test_ide_restart_reclaims_expired_work_token(service_factory):
    svc = service_factory
    creation = svc._container_provisioner.get_current_ide_creation_result.return_value

    async def reclaim(_job_id, *, claimant, lease_seconds=300):
        return {
            **creation,
            "restore_work_claimed_by": claimant,
            "restore_work_claim_token": 97,
            "restore_work_completed_at": None,
        }

    svc._container_provisioner.claim_ide_restore_work.side_effect = reclaim
    svc._install_and_sync_managed_repository_over_ssh = AsyncMock(return_value=True)
    svc._wait_for_code_server = AsyncMock(return_value=True)
    svc._set_session_context_if_runtime = AsyncMock(return_value=True)

    assert await svc._restore_k8s_ide_container(
        "job-reclaimed", {"repo_name": "demo/repo"}, "demo/repo", "main"
    )

    complete = svc._container_provisioner.complete_ide_restore_work
    assert complete.await_args.kwargs["restore_work"]["restore_work_claim_token"] == 97


@pytest.mark.asyncio
async def test_ide_lost_completion_response_replays_same_claim(service_factory):
    svc = service_factory
    svc._install_and_sync_managed_repository_over_ssh = AsyncMock(return_value=True)
    svc._wait_for_code_server = AsyncMock(return_value=True)
    svc._set_session_context_if_runtime = AsyncMock(return_value=True)
    svc._container_provisioner.complete_ide_restore_work.side_effect = [
        RuntimeError("response lost after commit"),
        True,
    ]

    assert await svc._restore_k8s_ide_container(
        "job-lost-complete", {"repo_name": "demo/repo"}, "demo/repo", "main"
    )

    calls = svc._container_provisioner.complete_ide_restore_work.await_args_list
    assert len(calls) == 2
    assert calls[0].kwargs["restore_work"] is calls[1].kwargs["restore_work"]
    assert calls[0].kwargs["claimant"] == calls[1].kwargs["claimant"]


@pytest.mark.asyncio
async def test_lost_ide_lease_blocks_snapshot_and_gitea_fallback_effects(
    service_factory,
):
    """A known-stale token cannot gain a fresh 60s mutation window."""

    svc = service_factory
    svc._container_provisioner.renew_ide_restore_work.side_effect = (
        lambda *_args, **_kwargs: None
    )
    svc._container_provisioner.complete_ide_restore_work.return_value = False
    svc._extract_snapshot_to_k8s_pod = AsyncMock(return_value=True)
    svc._repair_git_after_snapshot = AsyncMock(return_value=True)
    svc._seed_ide_profile_for_user = AsyncMock()
    svc._install_and_sync_managed_repository_over_ssh = AsyncMock(return_value=True)
    svc._wait_for_code_server = AsyncMock(return_value=True)
    job = {
        "id": "job-stale-lease",
        "repo_name": "demo/repo",
        "branch_name": "main",
        "context": {"snapshot": {"status": "available", "source_type": "pod"}},
    }

    await svc._restore_session(
        "job-stale-lease",
        job,
        "snapshot",
        8,
        "16Gi",
        restore_context={"snapshot_type": "pod"},
    )

    # Both snapshot and fallback enter through synchronous renewal. The second
    # attempt still owns no authority and reaches none of its external work.
    assert svc._container_provisioner.renew_ide_restore_work.await_count == 2
    svc._extract_snapshot_to_k8s_pod.assert_not_awaited()
    svc._repair_git_after_snapshot.assert_not_awaited()
    svc._seed_ide_profile_for_user.assert_not_awaited()
    svc._install_and_sync_managed_repository_over_ssh.assert_not_awaited()
    svc._wait_for_code_server.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_vm_session_marks_unavailable_when_orchestrator_cannot_reach(
    service_factory,
):
    """VM restore refuses before provisioning or endpoint discovery."""
    svc = service_factory
    svc._vm_provisioner.create_vm = AsyncMock(return_value=True)
    svc._wait_for_vm_ready = AsyncMock(return_value=(None, None))
    svc._get_job = AsyncMock(
        return_value={
            "id": "job-0006",
            "context": {"vm": {"ssh_host": "100.64.23.44"}},
        }
    )

    await svc._restore_vm_session("job-0006", {"id": "job-0006"}, "snapshot", 8, "16Gi")

    last_ctx = svc._db.merge_ide_session_context.await_args_list[-1]
    assert last_ctx.kwargs == {}
    assert last_ctx.args[1]["status"] == "unavailable"
    assert last_ctx.args[1]["error"] == (
        "VM IDE restore awaits exact generation authority"
    )
    svc._vm_provisioner.create_vm.assert_not_awaited()
    svc._wait_for_vm_ready.assert_not_awaited()
    svc._snapshot_service.download_snapshot.assert_not_awaited()


@pytest.mark.asyncio
async def test_wait_for_vm_ready_returns_none_on_unroutable_host(service_factory):
    """The wait loop should abandon VM restore as soon as host is unroutable."""
    svc = service_factory
    svc._get_job = AsyncMock(
        return_value={
            "id": "job-0007",
            "context": {"vm": {"ssh_host": "100.64.23.44", "status": "creating"}},
        }
    )

    with patch(
        "orchestrator.services.ide_session.orchestrator_can_reach",
        return_value=False,
    ):
        ssh_host, ssh_port = await svc._wait_for_vm_ready("job-0007", timeout=5)

    assert ssh_host is None
    assert ssh_port is None


@pytest.mark.asyncio
async def test_extract_snapshot_uses_home_scoped_command(service_factory):
    """IDE pod extraction must scope to home/agent-host, not the full tarball."""
    from orchestrator.services.ide_session import EXTRACT_HOME_REMOTE_CMD

    svc = service_factory
    svc._snapshot_service.is_available = True
    svc._snapshot_service.download_snapshot = AsyncMock(return_value=True)

    with (
        patch(
            "orchestrator.services.ide_session.stream_extract_snapshot",
            AsyncMock(return_value=(0, b"")),
        ) as mock_extract,
        patch(
            "orchestrator.services.ide_session.resolve_ssh_key_path", return_value="k"
        ),
    ):
        ok = await svc._extract_snapshot_to_k8s_pod("job-0008", "10.0.0.10", 30022)

    assert ok is True
    assert mock_extract.await_args.kwargs["remote_cmd"] == EXTRACT_HOME_REMOTE_CMD


@pytest.mark.asyncio
async def test_extract_rc_nonzero_with_populated_workspace_continues(service_factory):
    """tar noise (rc=2) must not fail the restore when the workspace landed."""
    svc = service_factory
    svc._snapshot_service.is_available = True
    svc._snapshot_service.download_snapshot = AsyncMock(return_value=True)
    svc._workspace_populated = AsyncMock(return_value=True)

    with (
        patch(
            "orchestrator.services.ide_session.stream_extract_snapshot",
            AsyncMock(return_value=(2, b"tar: usr/local/bin/x: Cannot open")),
        ),
        patch(
            "orchestrator.services.ide_session.resolve_ssh_key_path", return_value="k"
        ),
    ):
        ok = await svc._extract_snapshot_to_k8s_pod("job-0009", "10.0.0.10", 30022)

    assert ok is True
    for call in svc._db.merge_ide_session_context.await_args_list:
        assert call.args[1].get("status") != "failed"


@pytest.mark.asyncio
async def test_extract_rc_nonzero_with_empty_workspace_fails(service_factory):
    """A genuinely failed extraction (empty workspace) still fails the session."""
    svc = service_factory
    svc._snapshot_service.is_available = True
    svc._snapshot_service.download_snapshot = AsyncMock(return_value=True)
    svc._workspace_populated = AsyncMock(return_value=False)

    with (
        patch(
            "orchestrator.services.ide_session.stream_extract_snapshot",
            AsyncMock(return_value=(2, b"zstd: corrupt input")),
        ),
        patch(
            "orchestrator.services.ide_session.resolve_ssh_key_path", return_value="k"
        ),
    ):
        ok = await svc._extract_snapshot_to_k8s_pod("job-0010", "10.0.0.10", 30022)

    assert ok is False
    last_ctx = svc._db.merge_ide_session_context.await_args_list[-1]
    assert last_ctx.args[1]["status"] == "failed"


@pytest.mark.asyncio
async def test_gitea_clone_chain_is_idempotent(service_factory):
    """Retries against a pre-populated workspace must not die on remote add."""
    svc = service_factory
    svc._container_provisioner.create_ide_pod = AsyncMock(return_value="10.0.0.10")
    svc._install_and_sync_managed_repository_over_ssh = AsyncMock(return_value=True)
    svc._set_session_context_if_runtime = AsyncMock(return_value=True)

    with patch(
        "orchestrator.services.ide_session.IdeSessionService._wait_for_code_server",
        AsyncMock(return_value=True),
    ):
        ok = await svc._restore_k8s_ide_container(
            "job-0011",
            {"branch_name": "main", "repo_name": "demo/repo"},
            "demo/repo",
            "main",
        )

    assert ok is True
    svc._install_and_sync_managed_repository_over_ssh.assert_awaited_once()

    payload = {
        "version": 1,
        "authority_id": "2fd83ae5-f72c-41db-ae18-02e69d598aef",
        "generation": 1,
        "access_mode": "write",
        "repo_name": "demo",
        "repository_owner": "srw",
        "alias": "srw-repo-2fd83ae5f72c41dbae1802e69d598aef",
        "ssh_host": "srw-gitea",
        "ssh_port": 2222,
        "clone_url": ("ssh://srw-repo-2fd83ae5f72c41dbae1802e69d598aef/srw/demo.git"),
        "public_key_fingerprint": f"SHA256:{'A' * 43}",
        "private_key": "private-material",
    }
    clone_cmd, secret = svc._managed_git_command(
        payload,
        branch="main",
        workspace_path="/home/agent-host/workspace",
        require_existing=False,
    )
    assert "remote set-url origin" in clone_cmd
    assert "private-material" not in clone_cmd
    assert "IdentitiesOnly" not in clone_cmd
    assert bytes(secret) == b"private-material"
    assert "private_key" not in payload
    assert clone_cmd.index("flock -x 9") < clone_cmd.index(
        "Host srw-repo-2fd83ae5f72c41dbae1802e69d598aef"
    )
    assert clone_cmd.index("present") < clone_cmd.index(
        "Host srw-repo-2fd83ae5f72c41dbae1802e69d598aef"
    )


@pytest.mark.asyncio
async def test_local_ide_delivers_repository_key_to_immutable_container_id(
    service_factory,
):
    """A mutable container name is only an assertion, never a key target."""

    svc = service_factory
    container_id = "a" * 64
    process = MagicMock(returncode=0)
    process.stdout.read = AsyncMock(side_effect=[container_id.encode(), b""])
    process.stderr.read = AsyncMock(side_effect=[b""])
    process.wait = AsyncMock(return_value=0)
    svc._find_free_port = AsyncMock(return_value=38080)
    svc._detect_container_runtime = AsyncMock(return_value="podman")
    svc._inspect_container_id = AsyncMock(return_value=container_id)
    svc._managed_repository_payload = AsyncMock(return_value={"private_key": "x"})
    private_key = bytearray(b"private-key")
    svc._managed_git_command = MagicMock(return_value=("git-command", private_key))
    svc._run_secret_stdin_process = AsyncMock(return_value=True)
    svc._wait_for_code_server = AsyncMock(return_value=True)

    with patch(
        "orchestrator.services.ide_session.asyncio.create_subprocess_exec",
        AsyncMock(return_value=process),
    ):
        assert await svc._restore_local_ide_container(
            "job-immutable-id", "demo/repo", "main"
        )

    container_name = "srw-ide-job-immutabl"
    svc._inspect_container_id.assert_awaited_once_with("podman", container_name)
    command, delivered_key = svc._run_secret_stdin_process.await_args.args
    assert command[:4] == ["podman", "exec", "-i", container_id]
    assert container_name not in command
    assert delivered_key is private_key
    assert any(
        call.args[1] == {"container_id": container_id}
        for call in svc._db.merge_ide_session_context.await_args_list
    )


@pytest.mark.asyncio
async def test_local_ide_refuses_name_replacement_before_loading_repository_key(
    service_factory,
):
    """A same-name replacement cannot receive the predecessor's private key."""

    svc = service_factory
    container_id = "a" * 64
    replacement_id = "b" * 64
    process = MagicMock(returncode=0)
    process.stdout.read = AsyncMock(side_effect=[container_id.encode(), b""])
    process.stderr.read = AsyncMock(side_effect=[b""])
    process.wait = AsyncMock(return_value=0)
    svc._find_free_port = AsyncMock(return_value=38080)
    svc._detect_container_runtime = AsyncMock(return_value="podman")
    svc._inspect_container_id = AsyncMock(return_value=replacement_id)
    svc._managed_repository_payload = AsyncMock()
    svc._run_secret_stdin_process = AsyncMock()
    svc._delete_ide_container = AsyncMock(return_value=False)

    with patch(
        "orchestrator.services.ide_session.asyncio.create_subprocess_exec",
        AsyncMock(return_value=process),
    ):
        assert not await svc._restore_local_ide_container(
            "job-replaced-id", "demo/repo", "main"
        )

    svc._managed_repository_payload.assert_not_awaited()
    svc._run_secret_stdin_process.assert_not_awaited()
    svc._delete_ide_container.assert_awaited_once_with(
        "job-replaced-id",
        "srw-ide-job-replaced",
        "container",
        expected_container_id=container_id,
    )


@pytest.mark.asyncio
async def test_snapshot_restore_runs_git_repair_fetch(service_factory):
    """Successful extraction is followed by the best-effort git object refetch."""
    svc = service_factory
    svc._container_provisioner.create_ide_pod = AsyncMock(return_value="10.0.0.10")
    svc._extract_snapshot_to_k8s_pod = AsyncMock(return_value=True)
    svc._repair_git_after_snapshot = AsyncMock()
    svc._seed_ide_profile_for_user = AsyncMock()
    svc._wait_for_code_server = AsyncMock(return_value=True)
    svc._set_session_context_if_runtime = AsyncMock(return_value=True)

    ok = await svc._restore_snapshot_container(
        "job-0012", {"repo_name": "demo/repo", "branch_name": "job/abc"}
    )

    assert ok is True
    svc._repair_git_after_snapshot.assert_awaited_once_with(
        "job-0012",
        ANY,
        "10.0.0.10",
        30022,
        backend="sandbox",
        expected_host_key_fingerprint=(
            "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ),
        mutation_authority=ANY,
    )


# =============================================================================
# Test: _extract_snapshot_to_vm honest return (C1a)
# =============================================================================
#
# _extract_snapshot_to_vm used to return None on every path (no-service,
# download failure, rc != 0, AND a clean extract), which made a failed
# restore indistinguishable from a successful one. Mirrors the fix already
# applied to workspace_suspension.py's _extract_snapshot.


@pytest.mark.asyncio
async def test_extract_snapshot_to_vm_returns_false_on_download_failure(
    service_factory,
):
    """Missing exact authority refuses before even downloading snapshot bytes."""
    svc = service_factory
    svc._snapshot_service.download_snapshot = AsyncMock(return_value=False)

    ok = await svc._extract_snapshot_to_vm("job-0013", "10.0.0.10", 30022)

    assert ok is False
    svc._snapshot_service.download_snapshot.assert_not_awaited()


@pytest.mark.asyncio
async def test_extract_snapshot_to_vm_returns_false_on_extract_rc_nonzero(
    service_factory,
):
    """A tar/ssh failure (rc != 0) must not be reported as a successful extract."""
    svc = service_factory
    svc._snapshot_service.download_snapshot = AsyncMock(return_value=True)
    authority = svc._container_provisioner.attest_ide_runtime.return_value
    mutation_authority = AsyncMock(return_value=authority)

    with (
        patch(
            "orchestrator.services.ide_session.stream_extract_snapshot",
            AsyncMock(return_value=(2, b"boom")),
        ),
        patch(
            "orchestrator.services.ide_session.resolve_ssh_key_path", return_value="k"
        ),
    ):
        ok = await svc._extract_snapshot_to_vm(
            "job-0014",
            "10.0.0.10",
            30022,
            expected_host_key_fingerprint=authority.ssh_host_key_fingerprint,
            mutation_authority=mutation_authority,
        )

    assert ok is False


@pytest.mark.asyncio
async def test_extract_snapshot_to_vm_returns_true_on_clean_extract(
    service_factory,
):
    """A clean download + unpack is the only path that reports success."""
    svc = service_factory
    svc._snapshot_service.download_snapshot = AsyncMock(return_value=True)
    authority = svc._container_provisioner.attest_ide_runtime.return_value
    mutation_authority = AsyncMock(return_value=authority)
    extract = AsyncMock(return_value=(0, b""))

    with (
        patch(
            "orchestrator.services.ide_session.stream_extract_snapshot",
            extract,
        ),
        patch(
            "orchestrator.services.ide_session.resolve_ssh_key_path", return_value="k"
        ),
    ):
        ok = await svc._extract_snapshot_to_vm(
            "job-0015",
            "10.0.0.10",
            30022,
            expected_host_key_fingerprint=authority.ssh_host_key_fingerprint,
            mutation_authority=mutation_authority,
        )

    assert ok is True
    assert mutation_authority.await_count == 3
    assert extract.await_args.kwargs["expected_host_key_fingerprint"] == (
        authority.ssh_host_key_fingerprint
    )


@pytest.mark.asyncio
async def test_extract_snapshot_to_vm_reused_endpoint_successor_gets_no_bytes(
    service_factory,
):
    """A VM replacement at A's endpoint is rejected before SSH extraction."""
    from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation

    svc = service_factory
    svc._snapshot_service.download_snapshot = AsyncMock(return_value=True)
    authority_a = svc._container_provisioner.attest_ide_runtime.return_value
    authority_b = WorkspaceRuntimeAttestation(
        backing_id="vm:successor-b",
        workspace_generation="33333333-3333-4333-8333-333333333333",
        runtime_incarnation="44444444-4444-4444-8444-444444444444",
        ssh_host_key_fingerprint=("SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
        host=authority_a.host,
        pod_ip=authority_a.pod_ip,
        port=authority_a.port,
    )
    mutation_authority = AsyncMock(side_effect=[authority_a, authority_b])
    extract = AsyncMock(return_value=(0, b""))

    with patch(
        "orchestrator.services.ide_session.stream_extract_snapshot",
        extract,
    ):
        ok = await svc._extract_snapshot_to_vm(
            "job-0015",
            authority_a.host,
            authority_a.port,
            expected_host_key_fingerprint=authority_a.ssh_host_key_fingerprint,
            mutation_authority=mutation_authority,
        )

    assert ok is False
    extract.assert_not_awaited()


# =============================================================================
# Test: restore_snapshot_for_resume gates on the extract result (C1a)
# =============================================================================
#
# restore_snapshot_for_resume used to ignore _extract_snapshot_to_vm's return
# value entirely, unconditionally logging "Snapshot restored" and returning
# True — so a resume whose extract failed still reported success and the
# agent resumed on an empty/half-populated tree.


@pytest.mark.asyncio
async def test_restore_snapshot_for_resume_returns_false_when_extract_fails(
    service_factory, caplog
):
    """A failed extract must fail the resume, not silently report success."""
    svc = service_factory
    svc._snapshot_service.is_available = True
    svc._db.get_job = AsyncMock(
        return_value={
            "id": "job-0016",
            "context": {"snapshot": {"status": "available"}},
        }
    )
    svc._extract_snapshot_to_vm = AsyncMock(return_value=False)

    with caplog.at_level(logging.INFO, logger="orchestrator.services.ide_session"):
        result = await svc.restore_snapshot_for_resume("job-0016", "10.0.0.10", 30022)

    assert result is False
    assert "Snapshot restored for job resume" not in caplog.text
    svc._db.get_job.assert_not_awaited()
    svc._extract_snapshot_to_vm.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_snapshot_for_resume_refuses_even_when_legacy_extract_would_succeed(
    service_factory,
):
    """Public resume cannot mutate a VM before its durable execution claim."""
    svc = service_factory
    svc._snapshot_service.is_available = True
    svc._db.get_job = AsyncMock(
        return_value={
            "id": "job-0017",
            "context": {"snapshot": {"status": "available"}},
        }
    )
    svc._extract_snapshot_to_vm = AsyncMock(return_value=True)

    result = await svc.restore_snapshot_for_resume("job-0017", "10.0.0.10", 30022)

    assert result is False
    svc._db.get_job.assert_not_awaited()
    svc._extract_snapshot_to_vm.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_vm_status_is_unavailable_until_code_server_answers(
    service_factory,
):
    """A routable (same-cluster) VM must not be advertised as an active IDE
    when nothing answers on the code-server port — the proxy would 502."""
    svc = service_factory
    svc._get_job = AsyncMock(
        return_value={
            "id": "job-0018",
            "context": {"vm": {"status": "ready", "ssh_host": "10.42.0.46"}},
        }
    )

    with (
        patch(
            "orchestrator.services.ide_session.orchestrator_can_reach",
            return_value=True,
        ),
        patch.object(
            type(svc), "_wait_for_code_server", AsyncMock(return_value=False)
        ) as probe,
    ):
        result = await svc.get_session_status("job-0018")

    assert result["status"] == "unavailable"
    assert result["code_server_url"] is None
    assert "code-server" in result["error"]
    probe.assert_awaited_once_with("http://10.42.0.46:38080", timeout=ANY)


@pytest.mark.asyncio
async def test_live_vm_status_is_active_when_code_server_answers(
    service_factory, browser_ide_transport_available
):
    svc = service_factory
    svc._get_job = AsyncMock(
        return_value={
            "id": "job-0019",
            "context": {"vm": {"status": "ready", "ssh_host": "10.42.0.46"}},
        }
    )

    with (
        patch(
            "orchestrator.services.ide_session.orchestrator_can_reach",
            return_value=True,
        ),
        patch.object(type(svc), "_wait_for_code_server", AsyncMock(return_value=True)),
    ):
        result = await svc.get_session_status("job-0019")

    assert result["status"] == "active"
    assert result["source"] == "live_vm"
    assert result["code_server_url"]


# =============================================================================
# Browser-transport containment — the advertisement must match the proxy
# =============================================================================
#
# The IDE proxy refuses every browser transport (see services/ide_proxy.py
# ``browser_ide_refusal``). While it does, a job must not be told it has an
# openable IDE, and must not pay to restore one.


@pytest.mark.asyncio
async def test_live_workspace_status_withholds_url_while_contained(
    service_factory, monkeypatch
):
    """A ready workspace resolves normally, then loses only its URL."""
    monkeypatch.delenv("IDE_CREDENTIAL_KEY", raising=False)
    svc = service_factory
    svc._get_job = AsyncMock(
        return_value={
            "id": "job-0020",
            "context": {
                "workspace_container": {"status": "ready", "pod_ip": "10.42.0.9"}
            },
        }
    )

    result = await svc.get_session_status("job-0020")

    assert result["status"] == "unavailable"
    assert result["code_server_url"] is None
    assert result["code"] == "ide_credential_key_unconfigured"
    assert result["error"]
    # The underlying resolution is preserved, not discarded: only the
    # advertisement is withheld.
    assert result["source"] == "live_workspace"


@pytest.mark.asyncio
async def test_start_session_refuses_before_it_can_spend(service_factory, monkeypatch):
    """No job read, no snapshot pull, no VM — the refusal precedes all of it."""
    monkeypatch.delenv("IDE_CREDENTIAL_KEY", raising=False)
    svc = service_factory
    svc._get_job = AsyncMock()

    result = await svc.start_session("job-0021")

    assert result["status"] == "unavailable"
    assert result["code_server_url"] is None
    assert result["code"] == "ide_credential_key_unconfigured"
    svc._get_job.assert_not_awaited()
    svc._db.merge_ide_session_context.assert_not_awaited()
