"""Stateless End -> Resume -> permanent Delete ordering at the owning services.

Covers the owner-side rules the R1.B12 wedge sequence crosses, apart from the
remote fence and the backstop (tested with real bash and real PostgreSQL in
their own files):

* Resume admits a successor only after the predecessor's retirement settled;
  a genuinely incomplete retirement refuses before any lifecycle write.
* A permanent retirement in progress is never revived by Resume.
* End/Delete captured before the lifecycle lock refuses when Resume's
  successor publication changed the generation while it waited (the B12
  ``409`` four seconds after Resume), without touching the successor.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.services import thread_resume
from orchestrator.services import thread_retirement


THREAD_ID = "44444444-4444-4444-8444-444444444444"
RUNTIME = "55555555-5555-4555-8555-555555555555"


@asynccontextmanager
async def _owned_lock(*_args, **_kwargs):
    yield True


GENERATION = "66666666-6666-4666-8666-666666666666"
FINGERPRINT = "SHA256:" + ("B" * 43)


def _ended_stateless(*, pending: bool, permanent: bool = False) -> dict:
    """An ended stateless thread whose End/Delete stopped at the resident stage."""

    metadata: dict = {
        "config_override": {"workspace": {"backend": "sandbox"}},
        "workspace_container": {
            "provisioner": "k8s",
            "status": "retiring_process_zero",
            "pod_ip": "10.42.0.8",
            "port": 30022,
            "_canvas_workspace_generation": GENERATION,
            "_runtime_incarnation": RUNTIME,
        },
        "_workspace_binding": {
            "generation": GENERATION,
            "kind": "remote",
            "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
            "ssh_host_key_fingerprint": FINGERPRINT,
        },
    }
    if pending:
        metadata["_stateless_workspace_retirement_pending"] = True
        metadata["_stateless_claim_retirement"] = {
            "terminal_token": 9,
            "claimant_quiesced": True,
            "shell_retirement_required": True,
            "resident_cleanup_required": True,
            "residents_retired": False,
            "remote_retired": False,
            "permanent": permanent,
            "workspace_absence_proven": False,
            "workspace_generation": GENERATION,
            "endpoint_generation": GENERATION,
            "runtime_incarnation": RUNTIME,
            "host_key_fingerprint": FINGERPRINT,
        }
    return {
        "id": THREAD_ID,
        "user_id": "owner-1",
        "status": "ended",
        "execution_lane": "stateless",
        "metadata": metadata,
    }


def _resume_dependencies(store, reconcile) -> MagicMock:
    # Resume unpacks every collaborator up front; only these are reached
    # before the stateless lifecycle lock decides.
    dependencies = MagicMock(name="ThreadResumeDependencies")
    dependencies.store = store
    dependencies.retirement = SimpleNamespace(
        stateless_retirement_marker=thread_retirement.stateless_retirement_marker,
        reconcile_stateless_thread_retirement=reconcile,
    )
    dependencies.require_supported_protected_session_class = AsyncMock()
    dependencies.thread_workspace_backend = MagicMock(return_value="sandbox")
    dependencies.schedule_stateless_workspace_ensure = MagicMock()
    dependencies.logger = logging.getLogger("test")
    return dependencies


async def _resume(thread: dict, store, reconcile):
    with (
        patch.object(thread_resume, "thread_config_drift", AsyncMock(return_value=[])),
        patch.object(thread_resume, "require_srw_runtime"),
    ):
        return await thread_resume.resume_thread(
            THREAD_ID,
            {"id": "owner-1"},
            thread,
            None,
            dependencies=_resume_dependencies(store, reconcile),
        )


class TestResumeAdmitsOnlyASettledPredecessor:
    @pytest.mark.asyncio
    async def test_a_retryable_retirement_refusal_is_returned_without_resuming(self):
        thread = _ended_stateless(pending=True)
        store = SimpleNamespace(
            get_user=AsyncMock(return_value={"id": "owner-1"}),
            stateless_session_workspace_ensure_lock=_owned_lock,
            get_thread=AsyncMock(return_value=thread),
            resume_thread=AsyncMock(return_value=True),
        )
        reconcile = AsyncMock(
            side_effect=HTTPException(
                status_code=503,
                detail="Workspace resident retirement is not yet acknowledged",
            )
        )

        with pytest.raises(HTTPException) as exc_info:
            await _resume(thread, store, reconcile)

        assert exc_info.value.status_code == 503
        reconcile.assert_awaited_once_with(THREAD_ID, force=True, permanent=False)
        store.resume_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_marker_that_survives_reconciliation_refuses_resume(self):
        thread = _ended_stateless(pending=True)
        store = SimpleNamespace(
            get_user=AsyncMock(return_value={"id": "owner-1"}),
            stateless_session_workspace_ensure_lock=_owned_lock,
            get_thread=AsyncMock(return_value=thread),
            resume_thread=AsyncMock(return_value=True),
        )
        reconcile = AsyncMock(return_value={"state": "closed"})

        with pytest.raises(HTTPException) as exc_info:
            await _resume(thread, store, reconcile)

        assert exc_info.value.status_code == 503
        assert exc_info.value.detail == (
            "Stateless workspace retirement remains incomplete"
        )
        store.resume_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_permanent_retirement_in_progress_is_never_revived(self):
        thread = _ended_stateless(pending=True, permanent=True)
        store = SimpleNamespace(
            get_user=AsyncMock(return_value={"id": "owner-1"}),
            stateless_session_workspace_ensure_lock=_owned_lock,
            get_thread=AsyncMock(return_value=thread),
            resume_thread=AsyncMock(return_value=True),
        )
        reconcile = AsyncMock()

        with pytest.raises(HTTPException) as exc_info:
            await _resume(thread, store, reconcile)

        assert exc_info.value.status_code == 409
        reconcile.assert_not_awaited()
        store.resume_thread.assert_not_awaited()


class TestDeleteCapturedBeforeTheSuccessorWasPublished:
    @pytest.mark.asyncio
    async def test_generation_change_while_waiting_refuses_without_effects(self):
        """B12 Delete #1: Resume's ensure published B while Delete waited."""

        before = {
            "status": "created",
            "last_activity": "t0",
            "ended_at": None,
            "retirement_pending": False,
            "retirement_token": None,
            "unit_kind": "session_turn",
            "queue_state": "done",
            "lease_token": 8,
        }
        after = {**before, "last_activity": "t1"}
        thread = {
            "id": THREAD_ID,
            "status": "created",
            "execution_lane": "stateless",
            "metadata": {"config_override": {"workspace": {"backend": "sandbox"}}},
        }
        store = SimpleNamespace(
            get_stateless_thread_lifecycle_authority=AsyncMock(
                side_effect=[before, after]
            ),
            stateless_session_workspace_ensure_lock=_owned_lock,
            get_thread=AsyncMock(return_value=thread),
            begin_stateless_thread_workspace_retirement=AsyncMock(),
            delete_thread=AsyncMock(),
        )
        dependencies = SimpleNamespace(
            store=store,
            container_provisioner=MagicMock(),
            vm_provisioner=MagicMock(),
            snapshot_service=MagicMock(),
            gitea_client=MagicMock(),
            main_cloud_router=MagicMock(),
            logger=logging.getLogger("test"),
            pinned_retirement=MagicMock(),
            require_stateless_end_workspace=MagicMock(),
            decommission_officer_post=AsyncMock(),
            conclude_conference_if_any=AsyncMock(),
        )

        with pytest.raises(HTTPException) as exc_info:
            await thread_retirement.end_thread_flow(
                THREAD_ID,
                thread,
                permanent=True,
                force=False,
                dependencies=dependencies,
            )

        assert exc_info.value.status_code == 409
        assert exc_info.value.detail == (
            "Thread lifecycle generation changed while waiting for cleanup ownership"
        )
        store.begin_stateless_thread_workspace_retirement.assert_not_awaited()
        store.delete_thread.assert_not_awaited()
