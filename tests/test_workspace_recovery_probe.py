"""Tests for the pod workspace-recovery arm (probe / counter reset / no-leak).

knowledge-base/knowledge/issues/maxsessions_parallel_tools_false_workspace_death.md (slice D):

1. Probe before punch — on a ``workspace_unavailable`` report the orchestrator
   TCP-probes the workspace sshd; a listening pod is NOT deleted (the report
   was a misclassification or a transient), only paused + re-dispatched. The
   attempts counter still increments either way so a report-loop stays bounded.
2. Counter reset — a handled completion that is not ``workspace_unavailable``
   resets ``recovery_attempts`` (one recovered blip must not poison the job).
3. Exhaustion preserves work under an explicit Resume hold. Retry count alone
   does not authorize deleting the Pod, PVC, or checkpoints.
"""

import asyncio
import socket
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.completion import (  # noqa: E402
    handle_pod_workspace_recovery,
    probe_workspace_ssh,
    should_persist_completion_freeze,
    should_reset_recovery_counter,
)


# =============================================================================
# probe_workspace_ssh
# =============================================================================


class TestProbeWorkspaceSsh:
    @pytest.mark.asyncio
    async def test_listening_port_probes_true(self):
        server = await asyncio.start_server(
            lambda r, w: w.close(), host="127.0.0.1", port=0
        )
        port = server.sockets[0].getsockname()[1]
        try:
            assert await probe_workspace_ssh("127.0.0.1", port) is True
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_closed_port_probes_false(self):
        # Bind-then-close to find a port that is definitely not listening.
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        assert await probe_workspace_ssh("127.0.0.1", port) is False

    @pytest.mark.asyncio
    async def test_unroutable_host_times_out_false(self):
        assert await probe_workspace_ssh("10.255.255.1", 30022, timeout=0.2) is False


# =============================================================================
# should_reset_recovery_counter
# =============================================================================


class TestShouldResetRecoveryCounter:
    def test_resets_after_clean_completion(self):
        assert should_reset_recovery_counter({"recovery_attempts": 2}, None) is True

    def test_resets_after_ordinary_job_error(self):
        assert (
            should_reset_recovery_counter(
                {"recovery_attempts": 1}, {"type": "job_error", "message": "boom"}
            )
            is True
        )

    def test_does_not_reset_on_workspace_unavailable(self):
        assert (
            should_reset_recovery_counter(
                {"recovery_attempts": 1},
                {"type": "workspace_unavailable", "message": "ssh"},
            )
            is False
        )

    def test_no_reset_needed_at_zero(self):
        assert should_reset_recovery_counter({}, None) is False
        assert should_reset_recovery_counter({"recovery_attempts": 0}, None) is False


# =============================================================================
# handle_pod_workspace_recovery
# Dead-probe contracts are exercised through real SQL and cleanup authority in
# test_container_dead_probe_recovery_real_postgres and
# test_container_recovery_cleanup_real_postgres.
# =============================================================================


def _make_deps(*, pod_alive: bool, pause_ok: bool = True):
    db = AsyncMock()
    db.pause_job_shed_freeze.return_value = pause_ok
    delete_workspace = AsyncMock()
    delete_workspace.return_value = True
    trigger_dispatch = MagicMock()

    async def probe(_host, _port, timeout=3.0):
        return pod_alive

    return db, delete_workspace, trigger_dispatch, probe


def _job(attempts: int = 0) -> dict:
    ctx = {
        "host": "workspace-aaaa.svc.cluster.local",
        "port": 30022,
        "status": "ready",
        "pod_ip": "10.42.0.1",
        "_runtime_incarnation": "55555555-5555-4555-8555-555555555555",
    }
    if attempts:
        ctx["recovery_attempts"] = attempts
    return {
        "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "context": {"workspace_container": ctx},
    }


_ERROR = {"type": "workspace_unavailable", "message": "SSH command failed"}


class TestHandlePodWorkspaceRecovery:
    @pytest.mark.asyncio
    async def test_live_pod_is_not_deleted(self):
        db, delete_workspace, trigger_dispatch, probe = _make_deps(pod_alive=True)
        job = _job()

        result = await handle_pod_workspace_recovery(
            job,
            job["id"],
            _ERROR,
            db=db,
            delete_workspace=delete_workspace,
            trigger_dispatch=trigger_dispatch,
            probe=probe,
        )

        delete_workspace.assert_not_awaited()
        merged = db.merge_workspace_container_context.await_args.args[1]
        # The warm pod stays adoptable: no teardown markers.
        assert merged.get("status") != "deleted"
        assert "pod_ip" not in merged
        assert merged["recovery_attempts"] == 1
        assert result["new_status"] == "paused"
        trigger_dispatch.assert_called_once()

    @pytest.mark.asyncio
    async def test_durable_reconciled_cleanup_failure_remains_retryable(self):
        db, delete_workspace, trigger_dispatch, probe = _make_deps(pod_alive=False)
        delete_workspace.side_effect = RuntimeError("kubernetes unavailable")
        command_id = "44444444-4444-4444-8444-444444444444"
        job = _job(attempts=4)
        job["context"]["workspace_container"].update(
            {
                "recovery_completion_command_id": command_id,
                "recovery_completion_outcome": {
                    "status": "handled",
                    "job_id": job["id"],
                    "new_status": "failed",
                    "actions": ["failed loud"],
                },
            }
        )

        with pytest.raises(RuntimeError, match="kubernetes unavailable"):
            await handle_pod_workspace_recovery(
                job,
                job["id"],
                _ERROR,
                db=db,
                delete_workspace=delete_workspace,
                trigger_dispatch=trigger_dispatch,
                probe=probe,
                completion_command_id=command_id,
                completion_finalizing_by="owner-b",
            )

        db.merge_workspace_container_context.assert_not_awaited()
        db.pause_job_shed_freeze.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_counter_increments_even_when_pod_alive(self):
        db, delete_workspace, trigger_dispatch, probe = _make_deps(pod_alive=True)
        job = _job(attempts=1)

        await handle_pod_workspace_recovery(
            job,
            job["id"],
            _ERROR,
            db=db,
            delete_workspace=delete_workspace,
            trigger_dispatch=trigger_dispatch,
            probe=probe,
        )

        merged = db.merge_workspace_container_context.await_args.args[1]
        assert merged["recovery_attempts"] == 2

    @pytest.mark.asyncio
    async def test_exhausted_cap_retains_workspace_under_explicit_resume_hold(self):
        db, delete_workspace, trigger_dispatch, probe = _make_deps(pod_alive=True)
        job = _job(attempts=3)
        held = {"new_status": "paused", "held_for_resume": True, "paused": True}
        db.hold_exhausted_workspace_recovery.return_value = held
        result = await handle_pod_workspace_recovery(
            job,
            job["id"],
            _ERROR,
            db=db,
            delete_workspace=delete_workspace,
            trigger_dispatch=trigger_dispatch,
            probe=probe,
        )
        assert result == held
        delete_workspace.assert_not_awaited()
        trigger_dispatch.assert_not_called()
        db.update_job_status.assert_not_awaited()
        db.pause_job_shed_freeze.assert_not_awaited()
        db.hold_exhausted_workspace_recovery.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_durable_recovery_reconciles_atomic_disposition_without_reprobe(self):
        db, delete_workspace, trigger_dispatch, probe = _make_deps(pod_alive=False)
        command_id = "44444444-4444-4444-8444-444444444444"
        outcome = {
            "status": "handled",
            "job_id": _job()["id"],
            "new_status": "paused",
            "paused": True,
            "actions": ["workspace recovery: reconciled"],
        }
        job = _job(attempts=2)
        job["context"]["workspace_container"].update(
            {
                "recovery_completion_command_id": command_id,
                "recovery_completion_outcome": outcome,
            }
        )

        assert (
            await handle_pod_workspace_recovery(
                job,
                job["id"],
                _ERROR,
                db=db,
                delete_workspace=delete_workspace,
                trigger_dispatch=trigger_dispatch,
                probe=probe,
                completion_command_id=command_id,
                completion_finalizing_by="owner-b",
            )
            == outcome
        )
        db.merge_workspace_container_context.assert_not_awaited()
        db.pause_job_shed_freeze.assert_not_awaited()
        delete_workspace.assert_not_awaited()
        trigger_dispatch.assert_called_once()

    @pytest.mark.asyncio
    async def test_durable_recovery_reconciles_pending_exact_uid_delete(self):
        db, delete_workspace, trigger_dispatch, probe = _make_deps(pod_alive=False)
        command_id = "44444444-4444-4444-8444-444444444444"
        outcome = {
            "status": "handled",
            "job_id": _job()["id"],
            "new_status": "paused",
            "paused": True,
            "actions": ["workspace recovery: reconciled"],
        }
        job = _job(attempts=2)
        job["context"]["workspace_container"].update(
            {
                "recovery_completion_command_id": command_id,
                "recovery_completion_outcome": outcome,
                "recovery_delete_pending": True,
            }
        )

        assert (
            await handle_pod_workspace_recovery(
                job,
                job["id"],
                _ERROR,
                db=db,
                delete_workspace=delete_workspace,
                trigger_dispatch=trigger_dispatch,
                probe=probe,
                completion_command_id=command_id,
                completion_finalizing_by="owner-b",
            )
            == outcome
        )
        delete_workspace.assert_awaited_once_with(job["id"])
        db.merge_workspace_container_context.assert_awaited_once_with(
            job["id"],
            {"recovery_delete_pending": False},
            completion_command_id=command_id,
            completion_finalizing_by="owner-b",
        )
        db.pause_job_shed_freeze.assert_not_awaited()
        trigger_dispatch.assert_called_once()


# =============================================================================
# should_persist_completion_freeze
# =============================================================================


class TestShouldPersistCompletionFreeze:
    """A workspace_unavailable completion can only ECHO a stale freeze."""

    def test_workspace_unavailable_blocks_persist(self):
        result = {
            "error": {"type": "workspace_unavailable", "message": "gone"},
            "freeze_data": {"freeze_type": "job_complete"},
        }
        assert should_persist_completion_freeze(result) is False

    def test_no_error_persists(self):
        assert (
            should_persist_completion_freeze(
                {"freeze_data": {"freeze_type": "phase_boundary"}}
            )
            is True
        )

    def test_other_error_type_still_persists(self):
        """An llm_outage-style freeze on an errored completion is genuine."""
        result = {
            "error": {"type": "llm_outage", "message": "provider down"},
            "freeze_data": {"freeze_type": "llm_outage"},
        }
        assert should_persist_completion_freeze(result) is True

    def test_non_dict_error_persists(self):
        assert should_persist_completion_freeze({"error": "boom"}) is True
