"""Turn-end stage triggers — internal endpoint + teardown hook (Slice C, Task 5).

Covers:
* ``POST /api/agents/threads/{thread_id}/cloud-stage`` — internal-key gate,
  flag-off no-op, fire-and-forget task scheduling + de-dupe registry
  (``main.cloud_task_registry``'s stage half, which mirrors its
  protected-engage half).
* ``WorkspaceSuspensionService.suspend_thread_workspace`` — Kubernetes
  capture is contained before any remote read, while a VM snapshot runs only
  under its own lease and never borrows that lease for multi-write staging.

Follows the house patterns: ``tests/test_export_to_cloud_endpoint.py``
(ExitStack-patch + ``import main`` directly) and
``tests/test_internal_auth.py`` (bare ``fake_request`` fixture + patched
``access_module._INTERNAL_KEY`` for the 401 case).
"""

import dataclasses
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from tests._workspace_recovery_fakes import idle_recovery_store

import pytest
from fastapi import HTTPException

import orchestrator.main
import orchestrator.security.access as access_module
from orchestrator.routers import agent_cloud_stage as agent_cloud_stage_routes
from orchestrator.services import cloud_stage_authority
from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation
from orchestrator.services.vm_provisioner import VMTeardownIdentity, VMTeardownResult
from orchestrator.services.workspace_suspension import WorkspaceSuspensionService
from orchestrator.application import workspace as workspace_composition
from orchestrator.services import snapshot_service as snapshot_service_module
from orchestrator.services import (
    thread_workspace_delivery as thread_workspace_delivery_module,
)
from orchestrator.services import vm_provisioner as vm_provisioner_module


# =============================================================================
# POST /api/agents/threads/{thread_id}/cloud-stage
# =============================================================================

_STAGE_AUTHORITY = {
    "runtime_generation": "11111111-1111-4111-8111-111111111111",
    "workspace_generation": "22222222-2222-4222-8222-222222222222",
    "expected_staged_epoch": 3,
    "runtime_retirement_token": None,
}


@asynccontextmanager
async def _owned_lock(*_args, **_kwargs):
    yield True


def _stage_tasks() -> dict:
    """The application's live stage-task registry slot map."""
    return orchestrator.main.app.state.resources.cloud_task_registry.cloud_stage_tasks


def _stage_deps(**overrides):
    """Route dependencies built from the patched globals.

    ``capture_cloud_stage_authority`` is a field default on
    ``AgentCloudStageDependencies``, so it is replaced here rather than
    patched on a module the router never reads.
    """
    return dataclasses.replace(
        workspace_composition.agent_cloud_stage_dependencies(
            orchestrator.main.app.state.resources
        ),
        **overrides,
    )


class TestCloudStageEndpoint:
    @pytest.mark.asyncio
    async def test_cloud_stage_requires_internal_key(self, fake_request):
        """No/garbage X-Internal-Key -> 401, before the flag or task logic runs."""
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            with pytest.raises(HTTPException) as exc:
                await agent_cloud_stage_routes.agent_trigger_cloud_stage(
                    fake_request, "thread-1", dependencies=_stage_deps()
                )
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_cloud_stage_flag_off_skips(self, fake_request):
        """Flag off -> {"skipped": "flag_off"}; no task is ever scheduled."""
        fake_request.headers = {"X-Internal-Key": "secret"}
        _stage_tasks().clear()
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch(
                "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
                return_value=False,
            ),
        ):
            result = await agent_cloud_stage_routes.agent_trigger_cloud_stage(
                fake_request, "thread-1", dependencies=_stage_deps()
            )
        assert result == {"skipped": "flag_off"}
        assert _stage_tasks() == {}

    @pytest.mark.asyncio
    async def test_cloud_stage_schedules_task(self, fake_request):
        """Flag on -> a background task is registered + {"scheduled": True};
        the task calls stage_thread_cloud_diff and self-evicts from the
        registry when done."""
        fake_request.headers = {"X-Internal-Key": "secret"}
        _stage_tasks().clear()
        stage_mock = AsyncMock(return_value={"epoch": 1, "counts": {}})
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch(
                "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
                return_value=True,
            ),
            patch(
                "orchestrator.services.cloud_staging.stage.stage_thread_cloud_diff",
                stage_mock,
            ),
            patch.object(
                orchestrator.main.app.state.resources.postgres_db,
                "get_thread",
                AsyncMock(return_value={"id": "thread-1"}),
            ),
            patch.object(
                orchestrator.main.app.state.resources.postgres_db,
                "get_ro_mount_by_thread",
                AsyncMock(return_value={"id": "mount-1"}),
            ),
            patch.object(
                orchestrator.main.app.state.resources.postgres_db,
                "thread_advisory_lock",
                side_effect=_owned_lock,
            ),
            patch.object(
                thread_workspace_delivery_module,
                "require_pinned_workspace_credential_owner",
                AsyncMock(),
            ),
        ):
            result = await agent_cloud_stage_routes.agent_trigger_cloud_stage(
                fake_request,
                "thread-1",
                dependencies=_stage_deps(
                    capture_cloud_stage_authority=MagicMock(
                        return_value=dict(_STAGE_AUTHORITY)
                    )
                ),
            )
            assert result == {"scheduled": True}
            # Task is registered synchronously (create_task schedules but does
            # not run until the event loop gets control back).
            task_key = cloud_stage_authority._cloud_stage_task_key(
                "thread-1", _STAGE_AUTHORITY
            )
            assert task_key in _stage_tasks()
            task = _stage_tasks()[task_key]
            await task

        stage_mock.assert_awaited_once_with(
            thread_id="thread-1",
            postgres_db=orchestrator.main.app.state.resources.postgres_db,
            snapshot_service=snapshot_service_module.snapshot_service,
            authority=_STAGE_AUTHORITY,
            vm_provisioner=vm_provisioner_module.vm_provisioner,
        )
        # Self-evicts once the task completes.
        assert task_key not in _stage_tasks()

    @pytest.mark.asyncio
    async def test_cloud_stage_dedupes_inflight_thread(self, fake_request):
        """A second ping for the same thread while one is still in flight
        must not spawn a duplicate task."""
        fake_request.headers = {"X-Internal-Key": "secret"}
        _stage_tasks().clear()
        sentinel_task = MagicMock()
        task_key = cloud_stage_authority._cloud_stage_task_key(
            "thread-1", _STAGE_AUTHORITY
        )
        _stage_tasks()[task_key] = sentinel_task
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch(
                "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
                return_value=True,
            ),
            patch.object(
                orchestrator.main.app.state.resources.postgres_db,
                "get_thread",
                AsyncMock(return_value={"id": "thread-1"}),
            ),
            patch.object(
                orchestrator.main.app.state.resources.postgres_db,
                "get_ro_mount_by_thread",
                AsyncMock(return_value={"id": "mount-1"}),
            ),
            patch.object(
                thread_workspace_delivery_module,
                "require_pinned_workspace_credential_owner",
                AsyncMock(),
            ),
        ):
            result = await agent_cloud_stage_routes.agent_trigger_cloud_stage(
                fake_request,
                "thread-1",
                dependencies=_stage_deps(
                    capture_cloud_stage_authority=MagicMock(
                        return_value=dict(_STAGE_AUTHORITY)
                    )
                ),
            )
        assert result == {"scheduled": True}
        # Registry slot untouched — still the sentinel, no new task created.
        assert _stage_tasks()[task_key] is sentinel_task
        _stage_tasks().clear()


# =============================================================================
# WorkspaceSuspensionService.suspend_thread_workspace — teardown hook
# =============================================================================


def _make_protected_thread(**overrides):
    generation = "11111111-1111-4111-8111-111111111111"
    launcher_uid = "22222222-2222-4222-8222-222222222222"
    fingerprint = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    thread = {
        "id": "thread-1",
        "metadata": {
            "protected_cloud": True,
            "config_override": {"workspace": {"backend": "vm"}},
            "workspace_container": {
                "git_remote_url": "http://gitea/srw/thread-1.git",
            },
            "vm": {
                "status": "ready",
                "provision_generation": generation,
                "identity_provision_generation": generation,
                "identity_authenticated": True,
                "vm_uid": "vm-a",
                "active_pod_uid": launcher_uid,
                "ssh_host": "100.64.0.5",
                "ssh_port": 22,
                "ssh_host_key_fingerprint": fingerprint,
                "ssh_registration_id": "registration-a",
            },
        },
    }
    thread.update(overrides)
    return thread


@pytest.fixture(autouse=True)
def legacy_resource_cleanup(monkeypatch):
    """Thread-owned suspension cleanup carries no v3 Job resource charge.

    ``complete_vm_cleanup_permit`` fences a charged Job cleanup through
    ``prepare_vm_cleanup_resource``; for a thread owner the real scope is
    always ``None``, so the synthetic recovery store needs no DB pool.
    """
    import orchestrator.services.vm_workspace_recovery_store as recovery

    monkeypatch.setattr(
        recovery, "prepare_vm_cleanup_resource", AsyncMock(return_value=None)
    )


def _make_suspension_service(db):
    svc = WorkspaceSuspensionService()
    snapshot_service = MagicMock()
    snapshot_service.is_available = True
    snapshot_service.capture_vm_snapshot = AsyncMock(return_value=True)
    container_provisioner = MagicMock()
    container_provisioner.is_available = True
    container_provisioner.delete_workspace = AsyncMock(return_value=True)
    vm_provisioner = MagicMock()
    vm_provisioner.is_available = True
    generation = "11111111-1111-4111-8111-111111111111"
    launcher_uid = "22222222-2222-4222-8222-222222222222"
    fingerprint = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    identity = VMTeardownIdentity(
        provision_generation=generation,
        vm_uid="vm-a",
        rootdisk_pvc_uid="rootdisk-a",
        ssh_host="100.64.0.5",
        ssh_port=22,
        ssh_host_key_fingerprint=fingerprint,
    )
    vm_provisioner.capture_vm_teardown_identity = AsyncMock(return_value=identity)
    vm_provisioner.attest_workspace_runtime = AsyncMock(
        return_value=WorkspaceRuntimeAttestation(
            backing_id=f"k8s-vmi:{launcher_uid}",
            workspace_generation=generation,
            runtime_incarnation=launcher_uid,
            ssh_host_key_fingerprint=fingerprint,
            host="100.64.0.5",
            pod_ip="10.42.0.5",
            port=22,
            vm_uid="vm-a",
            launcher_pod_uid=launcher_uid,
        )
    )
    vm_provisioner.revalidate_vm_teardown_identity = AsyncMock(return_value="matched")
    vm_provisioner.release_vm_captured = AsyncMock(
        return_value=VMTeardownResult("completed", True)
    )
    svc.connect(
        db=db,
        snapshot_service=snapshot_service,
        container_provisioner=container_provisioner,
        vm_provisioner=vm_provisioner,
    )
    svc._workspace_recovery_store = idle_recovery_store()
    return svc, snapshot_service, container_provisioner


def _make_db(thread):
    db = MagicMock()
    db.get_thread = AsyncMock(return_value=thread)
    db.merge_thread_workspace_context = AsyncMock(return_value=True)
    db.merge_thread_vm_context = AsyncMock(return_value=True)
    db.merge_thread_vm_context_if_provision_generation = AsyncMock(return_value=True)
    receipt_id = str(uuid4())
    receipt = {
        "id": receipt_id,
        "claim_token": 7,
        "lease_expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
    }
    db.activate_vm_remote_operation_protocol = AsyncMock(return_value=True)
    db.claim_vm_remote_operation = AsyncMock(return_value=receipt)
    db.renew_vm_remote_operation = AsyncMock(return_value=receipt)
    db.settle_vm_remote_operation = AsyncMock(return_value=True)
    return db


class TestTeardownStageHook:
    @pytest.mark.asyncio
    async def test_k8s_thread_is_refused_before_cloud_stage_or_snapshot(self):
        thread = {
            "id": "thread-1",
            "execution_lane": "pinned",
            "metadata": {
                "protected_cloud": True,
                "config_override": {"workspace": {"backend": "sandbox"}},
                "workspace_container": {
                    "status": "ready",
                    "provisioner": "k8s",
                    "pod_ip": "10.0.0.5",
                    "_runtime_incarnation": ("33333333-3333-4333-8333-333333333333"),
                },
            },
        }
        db = _make_db(thread)
        svc, snapshot_service, _ = _make_suspension_service(db)
        stage_mock = AsyncMock(
            side_effect=AssertionError("foreign successor must not be read")
        )

        with patch(
            "orchestrator.services.cloud_staging.stage.stage_thread_cloud_diff",
            stage_mock,
        ):
            assert await svc.suspend_thread_workspace("thread-1") is False

        stage_mock.assert_not_awaited()
        snapshot_service.capture_vm_snapshot.assert_not_awaited()
        db.merge_thread_workspace_context.assert_not_awaited()
        db.merge_thread_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_teardown_hook_contains_unleased_cloud_stage_before_snapshot(
        self, monkeypatch
    ):
        """VM suspension cannot lend its filesystem lease to cloud staging."""
        monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
        thread = _make_protected_thread()
        db = _make_db(thread)
        svc, snapshot_service, _ = _make_suspension_service(db)

        call_order: list[str] = []

        async def fake_stage(**_kwargs):
            call_order.append("stage")
            return {"epoch": 1, "counts": {}}

        async def fake_capture(*args, **kwargs):
            call_order.append("snapshot")
            return True

        snapshot_service.capture_vm_snapshot = AsyncMock(side_effect=fake_capture)

        with patch(
            "orchestrator.services.cloud_staging.stage.stage_thread_cloud_diff",
            AsyncMock(side_effect=fake_stage),
        ):
            result = await svc.suspend_thread_workspace("thread-1")

        assert result is True
        assert call_order == ["snapshot"]

    @pytest.mark.asyncio
    async def test_teardown_hook_does_not_enter_unleased_stage(self, monkeypatch):
        monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
        thread = _make_protected_thread()
        db = _make_db(thread)
        svc, snapshot_service, _ = _make_suspension_service(db)

        stage_mock = AsyncMock(side_effect=RuntimeError("ssh capture failed"))
        with patch(
            "orchestrator.services.cloud_staging.stage.stage_thread_cloud_diff",
            stage_mock,
        ):
            result = await svc.suspend_thread_workspace("thread-1")

        assert result is True
        stage_mock.assert_not_awaited()
        snapshot_service.capture_vm_snapshot.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_teardown_hook_skipped_for_unprotected_thread(self, monkeypatch):
        """Non-protected threads never call the stage path at all."""
        monkeypatch.setenv("VM_REMOTE_OPERATION_PROTOCOL_ENABLED", "true")
        thread = _make_protected_thread()
        thread["metadata"]["protected_cloud"] = False
        db = _make_db(thread)
        svc, snapshot_service, _ = _make_suspension_service(db)

        stage_mock = AsyncMock(return_value={"epoch": 1})
        with patch(
            "orchestrator.services.cloud_staging.stage.stage_thread_cloud_diff",
            stage_mock,
        ):
            result = await svc.suspend_thread_workspace("thread-1")

        assert result is True
        stage_mock.assert_not_awaited()
        snapshot_service.capture_vm_snapshot.assert_awaited_once()
