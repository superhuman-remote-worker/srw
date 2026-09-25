"""VM park fails the job + orchestrator-vantage tailnet guards.

Covers the fix for knowledge-base/knowledge/issues/
vm_ssh_readiness_probe_unroutable_from_orchestrator.md:

- ``_fail_vm_parked_job`` (main.py): a terminally-parked VM job must go
  ``failed`` with a visible error — parked-but-'created' is an invisible
  wedge that stalls any loop whose current_job never turns terminal.
- ``is_tailnet_addr`` / ``orchestrator_can_reach`` (ssh_helpers): the
  orchestrator has no route to 100.64.0.0/10; SSH consumers must skip
  tailnet targets visibly instead of timing out quietly.
- ``stream_extract_snapshot`` / ``capture_vm_snapshot`` /
  ``_seed_vm_ide_config``: the actual skip behavior at each choke point.
"""

import os
from unittest.mock import AsyncMock, patch

import pytest
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import job_workspace_runtime as job_workspace_runtime_module

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.services.ssh_helpers import (  # noqa: E402
    is_tailnet_addr,
    orchestrator_can_reach,
    stream_extract_snapshot,
)


# =============================================================================
# is_tailnet_addr / orchestrator_can_reach
# =============================================================================


class TestTailnetPredicates:
    def test_tailnet_ip_detected(self):
        assert is_tailnet_addr("100.64.23.180") is True
        assert is_tailnet_addr("100.127.255.254") is True  # top of /10

    def test_non_tailnet_ip(self):
        assert is_tailnet_addr("10.0.2.15") is False
        assert is_tailnet_addr("100.128.0.1") is False  # just past /10
        assert is_tailnet_addr("192.168.1.1") is False

    def test_hostname_and_empty(self):
        assert is_tailnet_addr("workspace-abc") is False
        assert is_tailnet_addr("") is False
        assert is_tailnet_addr(None) is False

    def test_orchestrator_cannot_reach_tailnet(self, monkeypatch):
        monkeypatch.delenv("ORCHESTRATOR_HAS_TAILNET_ROUTE", raising=False)
        assert orchestrator_can_reach("100.64.23.180") is False

    def test_orchestrator_reaches_everything_else(self, monkeypatch):
        monkeypatch.delenv("ORCHESTRATOR_HAS_TAILNET_ROUTE", raising=False)
        assert orchestrator_can_reach("10.42.0.7") is True
        assert orchestrator_can_reach("workspace-abc") is True

    def test_env_escape_hatch(self, monkeypatch):
        """Deployments that give the orchestrator tailnet membership (e.g. a
        future tailscale sidecar) can assert the route via env."""
        monkeypatch.setenv("ORCHESTRATOR_HAS_TAILNET_ROUTE", "true")
        assert orchestrator_can_reach("100.64.23.180") is True


# =============================================================================
# stream_extract_snapshot skips tailnet targets
# =============================================================================


class TestStreamExtractTailnetGuard:
    @pytest.mark.asyncio
    async def test_tailnet_target_skipped_without_subprocess(self, tmp_path):
        tar = tmp_path / "snap.tar.zst"
        tar.write_bytes(b"data")

        with patch("asyncio.create_subprocess_exec") as mock_exec:
            rc, stderr = await stream_extract_snapshot(
                "100.64.23.180", 22, str(tar), key_path=""
            )

        assert rc == 255
        assert b"unroutable tailnet target" in stderr
        mock_exec.assert_not_called()


# =============================================================================
# capture_vm_snapshot skips tailnet targets
# =============================================================================


class TestSnapshotCaptureTailnetGuard:
    @pytest.mark.asyncio
    async def test_tailnet_target_skipped(self):
        from orchestrator.services.snapshot_service import SnapshotService

        svc = SnapshotService()
        svc._available = True
        svc._set_snapshot_context = AsyncMock()

        ok = await svc.capture_vm_snapshot(
            job_id="job-tailnet", ssh_host="100.64.23.180", ssh_port=22
        )

        assert ok is False
        svc._set_snapshot_context.assert_awaited_once()
        ctx_updates = svc._set_snapshot_context.call_args.args[1]
        assert ctx_updates["status"] == "capture_skipped"
        assert "unroutable" in ctx_updates["error"]

    @pytest.mark.asyncio
    async def test_routable_target_proceeds_to_capture(self):
        """A pod-network host passes the guard (and proceeds into the normal
        capture path — here it goes on to 'capturing')."""
        from orchestrator.services.snapshot_service import SnapshotService

        svc = SnapshotService()
        svc._available = True
        svc._set_snapshot_context = AsyncMock()

        with patch(
            "orchestrator.services.snapshot_service.resolve_ssh_key_path",
            side_effect=RuntimeError("stop here"),
        ):
            await svc.capture_vm_snapshot(
                job_id="job-pod", ssh_host="10.42.0.7", ssh_port=2222
            )

        first_status = svc._set_snapshot_context.call_args_list[0].args[1]["status"]
        assert first_status == "capturing"


# =============================================================================
# _seed_vm_ide_config skips tailnet targets
# =============================================================================


class TestIdeSeedTailnetGuard:
    @pytest.mark.asyncio
    async def test_tailnet_target_skipped_before_db_access(self):
        from orchestrator.services.nats_bridge import NatsBridge
        from orchestrator.security.vm_guest import VmGuestIdentity

        bridge = NatsBridge()
        bridge._db = AsyncMock()

        assert await bridge._seed_vm_ide_config(
            VmGuestIdentity("job", "job-1", "00000000-0000-4000-8000-000000000001"),
            "100.64.23.180",
            22,
        )

        bridge._db.get_job.assert_not_awaited()
        bridge._db.get_thread.assert_not_awaited()


# =============================================================================
# _fail_vm_parked_job (dispatcher park → job failure)
# =============================================================================


class TestFailVmParkedJob:
    @pytest.mark.asyncio
    async def test_fails_job_with_visible_error(self, monkeypatch):
        import orchestrator.main

        update = AsyncMock()
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "update_job_status",
            update,
        )

        await job_workspace_runtime_module.fail_vm_parked_job(
            "job-parked",
            "provisioning exhausted after 3 attempts (never reached 'ready')",
            dependencies=preparation_composition.job_workspace_runtime_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        update.assert_awaited_once()
        kwargs = update.call_args.kwargs
        assert update.call_args.args[0] == "job-parked"
        assert kwargs["status"] == "failed"
        assert "provisioning exhausted after 3 attempts" in kwargs["error_message"]
        assert "resume" in kwargs["error_message"].lower()
        assert "clear context.vm" not in kwargs["error_message"].lower()
