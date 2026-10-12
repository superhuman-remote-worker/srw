"""Pins for the thread VM-upgrade abort endpoint (workspace_tier_upgrade.md Q7).

When the live upgrade handler's ``_poll_vm_ready`` gives up (a cold ~2.8GB CDI
import outruns the poll budget), the agent calls
``POST /api/agents/threads/{id}/abort-vm-upgrade`` so the half-provisioned VM is
torn down instead of leaking — the orphan that previously needed a manual
``kubectl delete``. The endpoint must:

- 404 on an unknown thread,
- delete the VM when the provisioner is available,
- Mark ``metadata.vm.status='aborted'`` only after exact process-zero-backed
  deletion. A transport outage or unavailable provisioner must preserve the
  owner/runtime binding and return a retryable refusal.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from tests._workspace_recovery_fakes import idle_recovery_store

import pytest

# R1.B06: these handlers moved to services/thread_config_update with their
# routes in routers/thread_config. The application's dependency factory
# (``orchestrator.application.sessions``) reads the provisioner singleton from
# its owning module and the store from ``app.state.resources`` at call time.
from orchestrator.services import thread_config_update  # noqa: E402

import orchestrator.main as orch_main
from orchestrator.application import sessions as sessions_composition
from orchestrator.security import access as access_module
from orchestrator.services import vm_provisioner as vm_provisioner_module
import dataclasses

import fastapi as fastapi_module


def _db(thread):
    return SimpleNamespace(
        get_thread=AsyncMock(return_value=thread),
        merge_thread_vm_context=AsyncMock(return_value=True),
    )


def _provisioner(*, available=True, delete_result=True, delete_exc=None):
    return SimpleNamespace(
        is_available=available,
        lifecycle_available=available,
        capture_vm_teardown_identity=AsyncMock(
            return_value=SimpleNamespace(
                provision_generation="generation", vm_uid="vm", rootdisk_pvc_uid="disk"
            )
        ),
        release_vm_captured=AsyncMock(
            return_value=SimpleNamespace(
                disposition="completed" if delete_result else "unproven"
            ),
            side_effect=delete_exc,
        ),
    )


@pytest.fixture(autouse=True)
def legacy_resource_cleanup(monkeypatch):
    """Thread abort fixtures have no v3 Job creation/retry resource charge."""
    import orchestrator.services.vm_workspace_recovery_store as recovery

    monkeypatch.setattr(
        recovery, "prepare_vm_cleanup_resource", AsyncMock(return_value=None)
    )


@pytest.fixture(autouse=True)
def recovery_store(monkeypatch):
    """Inject an idle recovery store through the composition factory.

    ``thread_config_update_dependencies`` builds its recovery store per call.
    """
    store = idle_recovery_store()
    dependencies = sessions_composition.thread_config_update_dependencies
    monkeypatch.setattr(
        sessions_composition,
        "thread_config_update_dependencies",
        lambda resources: dataclasses.replace(
            dependencies(resources), recovery_store=store
        ),
    )
    return store


class TestAbortThreadVmUpgrade:
    @pytest.mark.asyncio
    async def test_404_when_thread_missing(self):
        db = _db(None)
        with (
            patch.object(access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(vm_provisioner_module, "vm_provisioner", _provisioner()),
        ):
            with pytest.raises(fastapi_module.HTTPException) as exc:
                await thread_config_update.agent_abort_thread_vm_upgrade(
                    MagicMock(),
                    "tid",
                    dependencies=sessions_composition.thread_config_update_dependencies(
                        orch_main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_deletes_vm_and_marks_aborted(self):
        db = _db({"id": "tid"})
        prov = _provisioner(available=True, delete_result=True)
        with (
            patch.object(access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(vm_provisioner_module, "vm_provisioner", prov),
        ):
            out = await thread_config_update.agent_abort_thread_vm_upgrade(
                MagicMock(),
                "tid",
                dependencies=sessions_composition.thread_config_update_dependencies(
                    orch_main.app.state.resources
                ),
            )

        prov.release_vm_captured.assert_awaited_once()
        call = prov.release_vm_captured.await_args
        assert call.args == ("tid", prov.capture_vm_teardown_identity.return_value)
        assert call.kwargs["entity_type"] == "thread"
        assert call.kwargs["purge_disk"] is True
        assert call.kwargs["parent_cleanup"]["intent"]["vm_uid"] == "vm"
        db.merge_thread_vm_context.assert_awaited_once_with(
            "tid", {"status": "aborted"}
        )
        assert out == {"status": "aborted", "thread_id": "tid", "vm_deleted": True}

    @pytest.mark.asyncio
    async def test_provisioner_unavailable_preserves_vm_authority(self):
        db = _db({"id": "tid"})
        prov = _provisioner(available=False)
        with (
            patch.object(access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(vm_provisioner_module, "vm_provisioner", prov),
        ):
            with pytest.raises(fastapi_module.HTTPException) as exc:
                await thread_config_update.agent_abort_thread_vm_upgrade(
                    MagicMock(),
                    "tid",
                    dependencies=sessions_composition.thread_config_update_dependencies(
                        orch_main.app.state.resources
                    ),
                )

        prov.release_vm_captured.assert_not_called()
        db.merge_thread_vm_context.assert_not_awaited()
        assert exc.value.status_code == 503
        assert exc.value.detail == {
            "code": "vm_process_zero_unproven",
            "retryable": True,
        }

    @pytest.mark.asyncio
    async def test_delete_exception_preserves_vm_authority(self):
        db = _db({"id": "tid"})
        prov = _provisioner(available=True, delete_exc=RuntimeError("nats down"))
        with (
            patch.object(access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(vm_provisioner_module, "vm_provisioner", prov),
        ):
            with pytest.raises(fastapi_module.HTTPException) as exc:
                await thread_config_update.agent_abort_thread_vm_upgrade(
                    MagicMock(),
                    "tid",
                    dependencies=sessions_composition.thread_config_update_dependencies(
                        orch_main.app.state.resources
                    ),
                )

        db.merge_thread_vm_context.assert_not_awaited()
        assert exc.value.status_code == 503
        assert exc.value.detail == {
            "code": "vm_process_zero_unproven",
            "retryable": True,
        }
