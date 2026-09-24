from __future__ import annotations

from tests import _b09_control_seams as control_seams

from copy import deepcopy
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, patch

import pytest
from fastapi import HTTPException

import orchestrator.main as main
from orchestrator.services.container_provisioner import WorkspaceCleanupOutcome
from orchestrator.security import access as access_module
from orchestrator.services import agent_provisioner as agent_provisioner_module
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import officer_conference as officer_conference_module
from orchestrator.services import (
    persistent_provisioner as persistent_provisioner_module,
)
from orchestrator.services import session_class_policy as session_class_policy_module
from orchestrator.services import snapshot_service as snapshot_service_module
from orchestrator.services import thread_retirement as thread_retirement_module
from orchestrator.services import workspace_suspension as workspace_suspension_module


THREAD_ID = "11111111-1111-4111-8111-111111111111"
GENERATION = "22222222-2222-4222-8222-222222222222"
RUNTIME = "33333333-3333-4333-8333-333333333333"
FINGERPRINT = "SHA256:" + ("A" * 43)


@pytest.fixture(autouse=True)
def history_cleanup():
    # These route-only database fixtures contain no previous runtime ledger.
    # The actual history/namespace proof is covered with migrated PostgreSQL in
    # test_stateless_workspace_history_cleanup.py.
    with patch(
        "orchestrator.services.stateless_workspace_history_cleanup.reclaim_stateless_workspace_history",
        new=AsyncMock(return_value=True),
    ) as cleanup:
        yield cleanup


def _settled_thread() -> dict:
    return {
        "id": THREAD_ID,
        "user_id": "user-1",
        "status": "ended",
        "execution_lane": "stateless",
        "metadata": {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {
                "status": "deleted",
                "provisioner": "k8s",
                "_canvas_workspace_generation": GENERATION,
                "_runtime_incarnation": None,
                "_snapshot_restore_required": True,
            },
            "_workspace_binding": {
                "generation": GENERATION,
                "kind": "remote",
                "backing_id": "k8s-pod:agent-workspaces:old-pod",
                "ssh_host_key_fingerprint": FINGERPRINT,
            },
            "_stateless_workspace_retirement_settled": {
                "terminal_token": 8,
                "cleanup_complete": True,
                "permanent": False,
                "backing_id": "k8s-pod:agent-workspaces:old-pod",
                "runtime_incarnation": RUNTIME,
                "snapshot_restore_required": True,
            },
        },
    }


def _settled_virtual_thread() -> dict:
    return {
        "id": THREAD_ID,
        "user_id": "user-1",
        "status": "ended",
        "execution_lane": "stateless",
        "metadata": {
            "config_override": {"workspace": {"backend": "virtual"}},
            "_workspace_binding": {
                "generation": GENERATION,
                "kind": "virtual",
                "backing_id": f"rclone:threads/{THREAD_ID}",
                "ssh_host_key_fingerprint": None,
            },
            "_stateless_workspace_retirement_settled": {
                "terminal_token": 8,
                "cleanup_complete": True,
                "permanent": False,
                "backing_id": f"rclone:threads/{THREAD_ID}",
                "runtime_incarnation": None,
                "snapshot_restore_required": False,
            },
        },
    }


def _in_progress_creation_thread(*, ready: bool = False) -> dict:
    workspace = {
        "status": "ready" if ready else "created",
        "provisioner": "k8s",
        "pod_name": f"ws-thread-{THREAD_ID[:12]}",
        "namespace": "agent-workspaces",
        "pod_ip": "10.42.0.8",
        "port": 30022,
        "_runtime_incarnation": RUNTIME,
    }
    metadata = {
        "config_override": {"workspace": {"backend": "sandbox"}},
        "workspace_container": workspace,
    }
    if ready:
        workspace["_canvas_workspace_generation"] = GENERATION
        metadata["_workspace_binding"] = {
            "generation": GENERATION,
            "kind": "remote",
            "backing_id": f"k8s-pod:agent-workspaces:{RUNTIME}",
            "ssh_host_key_fingerprint": FINGERPRINT,
        }
    else:
        workspace["_runtime_creation"] = {
            "generation": "44444444-4444-4444-8444-444444444444",
            "mode": "create",
            "attempted": True,
            "replaces_uid": None,
        }
    return {
        "id": THREAD_ID,
        "user_id": "user-1",
        "status": "created",
        "execution_lane": "stateless",
        "metadata": metadata,
    }


def _release_authorized_live_thread() -> dict:
    ack = {
        "kind": "protocol",
        "terminal_token": 8,
        "workspace_generation": GENERATION,
        "endpoint_generation": GENERATION,
        "runtime_incarnation": RUNTIME,
        "host_key_fingerprint": FINGERPRINT,
    }
    return {
        "id": THREAD_ID,
        "user_id": "user-1",
        "status": "ended",
        "execution_lane": "stateless",
        "metadata": {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "_stateless_workspace_retirement_pending": True,
            "_stateless_claim_retirement": {
                "terminal_token": 8,
                "claimant_quiesced": True,
                "shell_retirement_required": True,
                "resident_cleanup_required": True,
                "residents_retired": True,
                "residents_retired_by": "protocol",
                "remote_retired": True,
                "remote_retired_by": "protocol",
                "permanent": False,
                "workspace_absence_proven": False,
                "workspace_generation": GENERATION,
                "endpoint_generation": GENERATION,
                "runtime_incarnation": RUNTIME,
                "host_key_fingerprint": FINGERPRINT,
            },
            "_stateless_resident_retirement_ack": dict(ack),
            "_stateless_shell_retirement_ack": dict(ack),
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_name": f"ws-thread-{THREAD_ID[:12]}",
                "namespace": "agent-workspaces",
                "pod_ip": "10.42.0.8",
                "port": 30022,
                "_canvas_workspace_generation": GENERATION,
                "_runtime_incarnation": RUNTIME,
                "_snapshot_restore_required": False,
            },
            "_workspace_binding": {
                "generation": GENERATION,
                "kind": "remote",
                "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
                "ssh_host_key_fingerprint": FINGERPRINT,
            },
        },
    }


def _retiring_process_zero_thread() -> dict:
    thread = _release_authorized_live_thread()
    thread["metadata"]["workspace_container"]["status"] = "retiring_process_zero"
    return thread


@asynccontextmanager
async def _owned_lock(*_args, **_kwargs):
    yield True


def _lifecycle_authority() -> dict:
    return {
        "status": "ended",
        "last_activity": "stable",
        "ended_at": "stable",
        "retirement_pending": False,
        "retirement_token": None,
        "unit_kind": "session_turn",
        "queue_state": "done",
        "lease_token": 8,
    }


def _absence_thread(
    *,
    backing_id: str | None,
    snapshot_captured: bool,
    pending: bool,
    permanent: bool,
) -> dict:
    metadata = {
        "config_override": {"workspace": {"backend": "sandbox"}},
        "workspace_container": {
            "status": "deleted" if pending else "created",
            "provisioner": "k8s",
            "_runtime_incarnation": None,
            "_snapshot_restore_required": snapshot_captured,
        },
        "_workspace_binding": {
            "kind": "remote",
            "backing_id": backing_id,
        },
    }
    if pending:
        metadata.update(
            {
                "_stateless_workspace_retirement_pending": True,
                "_stateless_claim_retirement": {
                    "terminal_token": 0,
                    "claimant_quiesced": True,
                    "shell_retirement_required": False,
                    "resident_cleanup_required": False,
                    "residents_retired": True,
                    "remote_retired": True,
                    "permanent": permanent,
                    "workspace_absence_proven": True,
                    "workspace_generation": None,
                    "endpoint_generation": None,
                    "runtime_incarnation": None,
                    "host_key_fingerprint": None,
                },
            }
        )
    return {
        "id": THREAD_ID,
        "user_id": "user-1",
        "status": "ended" if pending else "created",
        "execution_lane": "stateless",
        "metadata": metadata,
    }


def _settled_cleanup_provisioner(**extra):
    """A permanent settled End now settles a durable 0198 cleanup intent first."""

    # Use the exact class main.py compares against; a second import of the
    # same file under a different module name would fail isinstance.
    outcome_type = WorkspaceCleanupOutcome

    async def _prepare(_owner, *, reclaim_shared_resources: bool, **_kwargs):
        return {
            "intent_generation": 1,
            "resources_captured_at": "2026-08-27T00:00:00+00:00",
            "reclaim_shared_resources": reclaim_shared_resources,
        }

    return SimpleNamespace(
        prepare_workspace_cleanup_intent=AsyncMock(side_effect=_prepare),
        reconcile_workspace_cleanup_intent=AsyncMock(
            return_value=outcome_type("settled", 1)
        ),
        **extra,
    )


def _db_for_settled(thread: dict, *, permanent: bool) -> SimpleNamespace:
    closure = {
        "state": "settled",
        "terminal_token": 8,
        "permanent": permanent,
        "backing_id": "k8s-pod:agent-workspaces:old-pod",
        "runtime_incarnation": RUNTIME,
        "snapshot_restore_required": True,
        "retry": True,
    }
    return SimpleNamespace(
        get_thread=AsyncMock(return_value=thread),
        get_stateless_thread_lifecycle_authority=AsyncMock(
            return_value=_lifecycle_authority()
        ),
        stateless_session_workspace_ensure_lock=_owned_lock,
        begin_stateless_thread_workspace_retirement=AsyncMock(return_value=closure),
        merge_thread_config_override=AsyncMock(),
        has_unfinished_session_memory_effects=AsyncMock(return_value=False),
        delete_thread=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_duplicate_soft_end_reuses_settled_proof_without_effects() -> None:
    thread = _settled_thread()
    db = _db_for_settled(thread, permanent=False)
    provisioner = _settled_cleanup_provisioner(release_absent_workspace=AsyncMock())
    snapshots = SimpleNamespace(is_available=True, delete_snapshot=AsyncMock())

    with (
        patch.object(
            access_module,
            "require_thread_owner",
            AsyncMock(return_value=({"sub": "user-1"}, thread)),
        ),
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            container_provisioner_module, "container_provisioner", provisioner
        ),
        patch.object(snapshot_service_module, "snapshot_service", snapshots),
        patch.object(
            officer_conference_module, "conclude_conference_if_any", AsyncMock()
        ),
    ):
        result = await control_seams.end_thread(
            THREAD_ID, SimpleNamespace(), permanent=False, force=True
        )

    assert result == {"status": "ended"}
    db.begin_stateless_thread_workspace_retirement.assert_awaited_once()
    provisioner.release_absent_workspace.assert_not_awaited()
    snapshots.delete_snapshot.assert_not_awaited()
    db.delete_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_permanent_end_retry_accepts_exact_process_zero_authority() -> None:
    thread = _retiring_process_zero_thread()
    authority = {
        **_lifecycle_authority(),
        "retirement_pending": True,
        "retirement_token": "8",
    }
    db = SimpleNamespace(
        get_thread=AsyncMock(return_value=thread),
        get_stateless_thread_lifecycle_authority=AsyncMock(return_value=authority),
        stateless_session_workspace_ensure_lock=_owned_lock,
    )
    reconcile = AsyncMock(return_value={"state": "missing"})

    with (
        patch.object(
            access_module,
            "require_thread_owner",
            AsyncMock(return_value=({"sub": "user-1"}, thread)),
        ),
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            thread_retirement_module,
            "reconcile_stateless_thread_retirement",
            reconcile,
        ),
    ):
        result = await control_seams.end_thread(
            THREAD_ID, SimpleNamespace(), permanent=True, force=True
        )

    assert result == {"status": "deleted"}
    reconcile.assert_awaited_once_with(
        THREAD_ID,
        dependencies=ANY,
        force=True,
        permanent=True,
    )


def test_process_zero_without_pending_retirement_stays_fail_closed() -> None:
    thread = _retiring_process_zero_thread()
    thread["metadata"].pop("_stateless_workspace_retirement_pending")
    thread["metadata"].pop("_stateless_claim_retirement")

    with pytest.raises(HTTPException) as exc:
        session_class_policy_module.require_stateless_end_workspace(thread)

    assert exc.value.status_code == 409
    assert "workspace_status_unavailable" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_process_zero_retry_retires_exact_live_residents_and_shell(
    monkeypatch,
) -> None:
    from orchestrator.services import stateless_session_retirement as retirement_service
    from orchestrator.services.cloud import RcloneMountSpec

    settled = _retiring_process_zero_thread()
    settled.update(
        {
            "main_cloud_backend": "nextcloud",
            "main_cloud_backend_instance_id": ("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            "main_cloud_session_handle": f"sessions/{THREAD_ID}",
        }
    )
    settled_marker = settled["metadata"]["_stateless_claim_retirement"]
    settled_marker["permanent"] = True

    resident_settled = deepcopy(settled)
    resident_marker = resident_settled["metadata"]["_stateless_claim_retirement"]
    resident_marker["remote_retired"] = False
    resident_marker.pop("remote_retired_by")
    resident_settled["metadata"].pop("_stateless_shell_retirement_ack")

    unacknowledged = deepcopy(resident_settled)
    unacknowledged_marker = unacknowledged["metadata"]["_stateless_claim_retirement"]
    unacknowledged_marker["residents_retired"] = False
    unacknowledged_marker.pop("residents_retired_by")
    unacknowledged["metadata"].pop("_stateless_resident_retirement_ack")

    def closure(*, residents: bool, shell: bool) -> dict:
        return {
            "state": "closed",
            "terminal_token": 8,
            "claimant_quiesced": True,
            "claim_losses": [],
            "resident_cleanup_required": True,
            "resident_acknowledged": residents,
            "shell_retirement_required": True,
            "remote_acknowledged": shell,
            "permanent": True,
            "workspace_absence_proven": False,
            "retry": True,
        }

    db = SimpleNamespace(
        get_thread=AsyncMock(
            side_effect=[
                unacknowledged,
                unacknowledged,
                resident_settled,
                settled,
            ]
        ),
        begin_stateless_thread_workspace_retirement=AsyncMock(
            side_effect=[
                closure(residents=False, shell=False),
                closure(residents=True, shell=False),
                closure(residents=True, shell=True),
            ]
        ),
        list_thread_mounts=AsyncMock(return_value=[]),
        record_managed_repository_workspace_process_zero=AsyncMock(return_value=True),
        acknowledge_stateless_thread_resident_retirement=AsyncMock(return_value=True),
        acknowledge_stateless_thread_shell_retirement=AsyncMock(return_value=True),
    )
    teardown_identity = container_provisioner_module.WorkspaceTeardownIdentity(
        pod_uid=RUNTIME,
        pvc_uid="44444444-4444-4444-8444-444444444444",
        service_uid="55555555-5555-4555-8555-555555555555",
        pod_ip="10.0.0.8",
        ssh_host_key_fingerprint=FINGERPRINT,
    )
    provisioner = SimpleNamespace(
        workspace_pod_authority=AsyncMock(return_value="exact_live"),
        capture_terminal_workspace_identity=AsyncMock(return_value=teardown_identity),
        prepare_workspace_cleanup_intent=AsyncMock(
            return_value={
                "resources_captured_at": "2026-08-30T00:00:00+00:00",
                "reclaim_shared_resources": True,
            }
        ),
        release_workspace=AsyncMock(return_value=True),
    )
    authority = SimpleNamespace(
        workspace_generation=GENERATION,
        runtime_incarnation=RUNTIME,
        host_key_fingerprint=FINGERPRINT,
    )
    proof = SimpleNamespace(authority=authority, as_dict=lambda: {})
    retire_residents = AsyncMock(return_value=proof)
    retire_shell = AsyncMock(return_value=authority)
    verify_residents = AsyncMock(return_value=authority)

    class Backend:
        backend_id = "nextcloud"
        is_initialized = True

        async def build_rclone_mount_spec(self, *, mount_kind, target_path, **kwargs):
            return RcloneMountSpec(
                source_type="webdav",
                source_config={
                    "url": "https://nc.test/remote.php/dav/files/agent/session/",
                    "vendor": "nextcloud",
                    "user": "agent-service",
                },
                auth={"type": "basic", "password": "agent-pass"},
            )

    cloud_router = SimpleNamespace(
        for_thread=lambda _thread: Backend(),
        for_thread_optional=lambda _thread: Backend(),
    )
    monkeypatch.setenv("CLOUD_WORKSPACE_DRIVER", "rclone_mount")
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            container_provisioner_module, "container_provisioner", provisioner
        ),
        patch.object(main.app.state.resources, "main_cloud_router", cloud_router),
        patch.object(
            retirement_service,
            "retire_stateless_workspace_residents",
            retire_residents,
        ),
        patch.object(
            retirement_service,
            "retire_stateless_session_shell",
            retire_shell,
        ),
        patch.object(
            retirement_service,
            "verify_stateless_workspace_residents_retired",
            verify_residents,
        ),
    ):
        result = await control_seams.reconcile_stateless_thread_retirement(
            THREAD_ID,
            force=True,
            permanent=True,
        )

    assert result["state"] == "settled"
    retire_residents.assert_awaited_once()
    terminal_cloud_mount = retire_residents.await_args.kwargs["cloud_mount_cfg"]
    assert terminal_cloud_mount is not None
    assert terminal_cloud_mount["driver"] == "rclone"
    assert terminal_cloud_mount["mounts"][0]["mount_kind"] == "session_folder"
    db.record_managed_repository_workspace_process_zero.assert_awaited_once_with(
        THREAD_ID,
        owner_kind="thread",
        scope="workspace_container",
        provisioner="k8s",
        runtime_incarnation=RUNTIME,
    )
    retire_shell.assert_awaited_once()
    verify_residents.assert_awaited_once()
    assert verify_residents.await_args.kwargs["cloud_mount_cfg"] is terminal_cloud_mount
    provisioner.prepare_workspace_cleanup_intent.assert_awaited_once()
    assert provisioner.prepare_workspace_cleanup_intent.await_args.kwargs == {
        "expected_runtime_incarnation": RUNTIME,
        "target_disposition": "deleted",
        "reclaim_shared_resources": True,
        "identity": teardown_identity,
    }
    assert (
        provisioner.release_workspace.await_args.kwargs["teardown_identity"]
        == teardown_identity
    )


@pytest.mark.asyncio
async def test_end_holds_before_begin_when_published_runtime_cannot_reach_ready() -> (
    None
):
    in_progress = _in_progress_creation_thread()
    db = SimpleNamespace(
        get_thread=AsyncMock(side_effect=[in_progress, in_progress]),
        begin_stateless_thread_workspace_retirement=AsyncMock(),
    )
    ensure = AsyncMock()
    provisioner = SimpleNamespace()
    suspension = SimpleNamespace()

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(thread_retirement_module, "ensure_session_workspace", ensure),
        patch.object(
            container_provisioner_module, "container_provisioner", provisioner
        ),
        patch.object(
            workspace_suspension_module, "workspace_suspension_service", suspension
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await control_seams.reconcile_stateless_thread_retirement(
                THREAD_ID, force=True, permanent=False
            )

    assert exc.value.status_code == 503
    ensure.assert_awaited_once_with(
        THREAD_ID,
        db=db,
        provisioner=provisioner,
        suspension=suspension,
        _workspace_lifecycle_lock_held=True,
    )
    db.begin_stateless_thread_workspace_retirement.assert_not_awaited()


@pytest.mark.asyncio
async def test_end_continues_exact_runtime_to_ready_before_begin() -> None:
    in_progress = _in_progress_creation_thread()
    ready = _in_progress_creation_thread(ready=True)
    db = SimpleNamespace(
        get_thread=AsyncMock(side_effect=[in_progress, ready]),
        begin_stateless_thread_workspace_retirement=AsyncMock(
            return_value={"state": "busy", "reason": "turn_in_flight"}
        ),
    )

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            thread_retirement_module,
            "ensure_session_workspace",
            AsyncMock(),
        ),
        patch.object(
            container_provisioner_module, "container_provisioner", SimpleNamespace()
        ),
        patch.object(
            workspace_suspension_module,
            "workspace_suspension_service",
            SimpleNamespace(),
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await control_seams.reconcile_stateless_thread_retirement(
                THREAD_ID, force=False, permanent=False
            )

    assert exc.value.status_code == 409
    db.begin_stateless_thread_workspace_retirement.assert_awaited_once_with(
        THREAD_ID,
        force=False,
        permanent=False,
        workspace_absence_proven=False,
    )


@pytest.mark.asyncio
async def test_end_holds_when_creation_continuation_leaves_restore_debt() -> None:
    in_progress = _in_progress_creation_thread()
    workspace = in_progress["metadata"]["workspace_container"]
    workspace["_snapshot_restore_required"] = True
    workspace["_runtime_creation"]["mode"] = "restore"
    ready_with_restore_debt = _in_progress_creation_thread(ready=True)
    ready_with_restore_debt["metadata"]["workspace_container"][
        "_snapshot_restore_required"
    ] = True
    db = SimpleNamespace(
        get_thread=AsyncMock(side_effect=[in_progress, ready_with_restore_debt]),
        begin_stateless_thread_workspace_retirement=AsyncMock(),
    )
    ensure = AsyncMock()

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(thread_retirement_module, "ensure_session_workspace", ensure),
        patch.object(
            container_provisioner_module, "container_provisioner", SimpleNamespace()
        ),
        patch.object(
            workspace_suspension_module,
            "workspace_suspension_service",
            SimpleNamespace(),
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await control_seams.reconcile_stateless_thread_retirement(
                THREAD_ID, force=True, permanent=False
            )

    assert exc.value.status_code == 503
    ensure.assert_awaited_once()
    db.begin_stateless_thread_workspace_retirement.assert_not_awaited()


@pytest.mark.asyncio
async def test_end_holds_markerless_ready_workspace_until_restore_clears() -> None:
    restore_debt = _in_progress_creation_thread(ready=True)
    restore_debt["metadata"]["workspace_container"]["_snapshot_restore_required"] = True
    db = SimpleNamespace(
        get_thread=AsyncMock(side_effect=[restore_debt, restore_debt]),
        begin_stateless_thread_workspace_retirement=AsyncMock(),
    )
    ensure = AsyncMock()

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(thread_retirement_module, "ensure_session_workspace", ensure),
        patch.object(
            container_provisioner_module, "container_provisioner", SimpleNamespace()
        ),
        patch.object(
            workspace_suspension_module,
            "workspace_suspension_service",
            SimpleNamespace(),
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await control_seams.reconcile_stateless_thread_retirement(
                THREAD_ID, force=True, permanent=False
            )

    assert exc.value.status_code == 503
    ensure.assert_awaited_once()
    db.begin_stateless_thread_workspace_retirement.assert_not_awaited()


@pytest.mark.asyncio
async def test_end_begins_only_after_exact_restore_debt_is_cleared() -> None:
    restore_debt = _in_progress_creation_thread(ready=True)
    restore_debt["metadata"]["workspace_container"]["_snapshot_restore_required"] = True
    restored = _in_progress_creation_thread(ready=True)
    restored["metadata"]["workspace_container"]["_snapshot_restore_required"] = False
    db = SimpleNamespace(
        get_thread=AsyncMock(side_effect=[restore_debt, restored]),
        begin_stateless_thread_workspace_retirement=AsyncMock(
            return_value={"state": "busy", "reason": "turn_in_flight"}
        ),
    )
    ensure = AsyncMock()

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(thread_retirement_module, "ensure_session_workspace", ensure),
        patch.object(
            container_provisioner_module, "container_provisioner", SimpleNamespace()
        ),
        patch.object(
            workspace_suspension_module,
            "workspace_suspension_service",
            SimpleNamespace(),
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await control_seams.reconcile_stateless_thread_retirement(
                THREAD_ID, force=False, permanent=False
            )

    assert exc.value.status_code == 409
    ensure.assert_awaited_once()
    db.begin_stateless_thread_workspace_retirement.assert_awaited_once_with(
        THREAD_ID,
        force=False,
        permanent=False,
        workspace_absence_proven=False,
    )


@pytest.mark.asyncio
async def test_delete_acceptance_cannot_finish_until_exact_old_uid_is_404() -> None:
    thread = _release_authorized_live_thread()
    closure = {
        "state": "closed",
        "terminal_token": 8,
        "claimant_quiesced": True,
        "claim_losses": [],
        "resident_cleanup_required": True,
        "resident_acknowledged": True,
        "shell_retirement_required": True,
        "remote_acknowledged": True,
        "permanent": False,
        "workspace_absence_proven": False,
        "retry": True,
    }
    db = SimpleNamespace(
        get_thread=AsyncMock(return_value=thread),
        begin_stateless_thread_workspace_retirement=AsyncMock(return_value=closure),
        finish_stateless_thread_workspace_retirement=AsyncMock(return_value=True),
    )
    teardown_identity = container_provisioner_module.WorkspaceTeardownIdentity(
        pod_uid=RUNTIME,
        pvc_uid="44444444-4444-4444-8444-444444444444",
        service_uid="55555555-5555-4555-8555-555555555555",
        pod_ip="10.0.0.8",
        ssh_host_key_fingerprint=FINGERPRINT,
    )
    provisioner = SimpleNamespace(
        workspace_pod_authority=AsyncMock(side_effect=["exact_live", "exact_absent"]),
        capture_terminal_workspace_identity=AsyncMock(return_value=teardown_identity),
        prepare_workspace_cleanup_intent=AsyncMock(
            return_value={
                "resources_captured_at": "2026-08-30T00:00:00+00:00",
                "reclaim_shared_resources": False,
            }
        ),
        # First pass models UID-preconditioned DELETE acceptance while the old
        # object remains Terminating past the bounded exact-absence wait.
        release_workspace=AsyncMock(return_value=False),
        release_absent_workspace=AsyncMock(return_value=True),
    )

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            container_provisioner_module, "container_provisioner", provisioner
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await control_seams.reconcile_stateless_thread_retirement(
                THREAD_ID, force=True, permanent=False
            )
        assert exc.value.status_code == 503
        db.finish_stateless_thread_workspace_retirement.assert_not_awaited()

        result = await control_seams.reconcile_stateless_thread_retirement(
            THREAD_ID, force=True, permanent=False
        )

    assert result["state"] == "settled"
    provisioner.release_workspace.assert_awaited_once()
    provisioner.release_absent_workspace.assert_awaited_once()
    absent_owner = provisioner.release_absent_workspace.await_args.args[0]
    assert (absent_owner.kind, absent_owner.id) == ("session", THREAD_ID)
    assert provisioner.release_absent_workspace.await_args.kwargs == {
        "reclaim_volume": False,
        "expected_runtime_incarnation": RUNTIME,
        "strict": True,
    }
    db.finish_stateless_thread_workspace_retirement.assert_awaited_once_with(THREAD_ID)


@pytest.mark.asyncio
async def test_exact_terminal_uid_acknowledges_then_deletes_through_finalizer_path() -> (
    None
):
    acknowledged_thread = _release_authorized_live_thread()
    unacknowledged_thread = _release_authorized_live_thread()
    marker = unacknowledged_thread["metadata"]["_stateless_claim_retirement"]
    marker.update(
        {
            "residents_retired": False,
            "residents_retired_by": None,
            "remote_retired": False,
            "remote_retired_by": None,
        }
    )
    unacknowledged_thread["metadata"].pop("_stateless_resident_retirement_ack", None)
    unacknowledged_thread["metadata"].pop("_stateless_shell_retirement_ack", None)
    awaiting_ack = {
        "state": "closed",
        "terminal_token": 8,
        "claimant_quiesced": True,
        "claim_losses": [],
        "resident_cleanup_required": True,
        "resident_acknowledged": False,
        "shell_retirement_required": True,
        "remote_acknowledged": False,
        "permanent": False,
        "workspace_absence_proven": False,
        "retry": True,
    }
    acknowledged = {
        **awaiting_ack,
        "resident_acknowledged": True,
        "remote_acknowledged": True,
    }
    db = SimpleNamespace(
        get_thread=AsyncMock(
            side_effect=[
                unacknowledged_thread,
                acknowledged_thread,
                acknowledged_thread,
            ]
        ),
        begin_stateless_thread_workspace_retirement=AsyncMock(
            side_effect=[awaiting_ack, acknowledged, acknowledged]
        ),
        acknowledge_stateless_thread_shell_absent=AsyncMock(return_value=True),
        finish_stateless_thread_workspace_retirement=AsyncMock(return_value=True),
    )
    provisioner = _settled_cleanup_provisioner(
        workspace_pod_authority=AsyncMock(return_value="exact_terminal"),
        delete_workspace=AsyncMock(return_value=True),
        delete_workspace_with_outcome=AsyncMock(
            return_value=SimpleNamespace(
                stale_target_settled=False, current_deleted=True
            )
        ),
        release_absent_workspace=AsyncMock(return_value=True),
    )

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            container_provisioner_module, "container_provisioner", provisioner
        ),
    ):
        result = await control_seams.reconcile_stateless_thread_retirement(
            THREAD_ID, force=True, permanent=False
        )

    assert result["state"] == "settled"
    db.acknowledge_stateless_thread_shell_absent.assert_awaited_once_with(
        THREAD_ID,
        terminal_token=8,
        runtime_incarnation=RUNTIME,
    )
    # Terminal deletion now runs through the durable cleanup intent and the
    # outcome-returning delete, so the exact captured resource identities are
    # committed before the Kubernetes effect.
    provisioner.prepare_workspace_cleanup_intent.assert_awaited_once()
    assert provisioner.prepare_workspace_cleanup_intent.await_args.kwargs == {
        "expected_runtime_incarnation": RUNTIME,
        "target_disposition": "deleted",
        "reclaim_shared_resources": False,
    }
    provisioner.delete_workspace_with_outcome.assert_awaited_once()
    deletion_kwargs = dict(provisioner.delete_workspace_with_outcome.await_args.kwargs)
    carried_intent = deletion_kwargs.pop("cleanup_intent")
    assert deletion_kwargs == {
        "expected_runtime_incarnation": RUNTIME,
        "wait_for_exact_absence": True,
        "target_disposition": "deleted",
        "reclaim_shared_resources": False,
    }
    # The effect carries the exact intent that was committed first.
    assert carried_intent["resources_captured_at"] is not None
    assert carried_intent["reclaim_shared_resources"] is False
    provisioner.delete_workspace.assert_not_awaited()
    db.finish_stateless_thread_workspace_retirement.assert_awaited_once_with(THREAD_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize("historical_clean", (True, False))
async def test_soft_end_to_permanent_reclaims_snapshot_before_row_delete(
    history_cleanup, historical_clean
) -> None:
    history_cleanup.return_value = historical_clean
    thread = _settled_thread()
    db = _db_for_settled(thread, permanent=True)
    provisioner = _settled_cleanup_provisioner(
        release_absent_workspace=AsyncMock(return_value=True)
    )
    snapshots = SimpleNamespace(
        is_available=True,
        delete_snapshot=AsyncMock(return_value=True),
    )

    with (
        patch.object(
            access_module,
            "require_thread_owner",
            AsyncMock(return_value=({"sub": "user-1"}, thread)),
        ),
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            container_provisioner_module, "container_provisioner", provisioner
        ),
        patch.object(snapshot_service_module, "snapshot_service", snapshots),
        patch.object(
            main.app.state.resources,
            "gitea_client",
            SimpleNamespace(is_initialized=False),
        ),
        patch.object(
            officer_conference_module, "conclude_conference_if_any", AsyncMock()
        ),
    ):
        if historical_clean:
            result = await control_seams.end_thread(
                THREAD_ID, SimpleNamespace(), permanent=True, force=True
            )
        else:
            with pytest.raises(HTTPException) as caught:
                await control_seams.end_thread(
                    THREAD_ID, SimpleNamespace(), permanent=True, force=True
                )
            assert caught.value.status_code == 503
            assert (
                caught.value.detail
                == "Historical workspace permanent cleanup is incomplete"
            )
            db.delete_thread.assert_not_awaited()
            return

    assert result == {"status": "deleted"}
    # Permanent reclaim now runs through the durable 0198 cleanup intent, not a
    # bare release call: the disposition and captured resource identities are
    # committed before any Kubernetes effect.
    provisioner.release_absent_workspace.assert_not_awaited()
    provisioner.prepare_workspace_cleanup_intent.assert_awaited_once()
    owner = provisioner.prepare_workspace_cleanup_intent.await_args.args[0]
    assert (owner.kind, owner.id) == ("session", THREAD_ID)
    assert provisioner.prepare_workspace_cleanup_intent.await_args.kwargs == {
        "expected_runtime_incarnation": RUNTIME,
        "target_disposition": "deleted",
        "reclaim_shared_resources": True,
    }
    provisioner.reconcile_workspace_cleanup_intent.assert_awaited_once()
    snapshots.delete_snapshot.assert_awaited_once_with(THREAD_ID, entity_type="threads")
    db.delete_thread.assert_awaited_once_with(THREAD_ID)


@pytest.mark.asyncio
async def test_snapshot_delete_failure_keeps_settled_thread_retryable() -> None:
    thread = _settled_thread()
    db = _db_for_settled(thread, permanent=True)
    provisioner = _settled_cleanup_provisioner(
        release_absent_workspace=AsyncMock(return_value=True)
    )
    snapshots = SimpleNamespace(
        is_available=True,
        delete_snapshot=AsyncMock(return_value=False),
    )

    with (
        patch.object(
            access_module,
            "require_thread_owner",
            AsyncMock(return_value=({"sub": "user-1"}, thread)),
        ),
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            container_provisioner_module, "container_provisioner", provisioner
        ),
        patch.object(snapshot_service_module, "snapshot_service", snapshots),
        patch.object(
            main.app.state.resources,
            "gitea_client",
            SimpleNamespace(is_initialized=False),
        ),
        patch.object(
            officer_conference_module, "conclude_conference_if_any", AsyncMock()
        ),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await control_seams.end_thread(
                THREAD_ID, SimpleNamespace(), permanent=True, force=True
            )

    assert exc_info.value.status_code == 503
    db.delete_thread.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backing_id", "snapshot_captured", "permanent", "succeeds"),
    [
        (None, False, False, False),
        (None, False, True, False),
        ("k8s-pvc:agent-workspaces:pvc-uid", False, False, False),
        ("k8s-pvc:agent-workspaces:pvc-uid", False, True, False),
        ("k8s-pod:agent-workspaces:pod-uid", False, False, False),
        ("k8s-pod:agent-workspaces:pod-uid", True, False, False),
        ("k8s-pod:agent-workspaces:pod-uid", False, True, False),
    ],
)
async def test_missing_runtime_absence_matrix(
    backing_id,
    snapshot_captured,
    permanent,
    succeeds,
) -> None:
    preflight = _absence_thread(
        backing_id=backing_id,
        snapshot_captured=snapshot_captured,
        pending=False,
        permanent=permanent,
    )
    current = _absence_thread(
        backing_id=backing_id,
        snapshot_captured=snapshot_captured,
        pending=True,
        permanent=permanent,
    )
    closure = {
        "state": "closed",
        "terminal_token": 0,
        "claimant_quiesced": True,
        "claim_losses": [],
        "resident_cleanup_required": False,
        "resident_acknowledged": True,
        "shell_retirement_required": False,
        "remote_acknowledged": True,
        "permanent": permanent,
        "workspace_absence_proven": True,
        "retry": False,
    }
    db = SimpleNamespace(
        get_thread=AsyncMock(side_effect=[preflight, current]),
        begin_stateless_thread_workspace_retirement=AsyncMock(
            side_effect=[closure, closure]
        ),
        finish_stateless_thread_workspace_retirement=AsyncMock(return_value=True),
    )
    provisioner = SimpleNamespace(
        workspace_pod_authority=AsyncMock(return_value="exact_absent"),
        release_absent_workspace=AsyncMock(return_value=True),
    )

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            container_provisioner_module, "container_provisioner", provisioner
        ),
    ):
        if succeeds:
            result = await control_seams.reconcile_stateless_thread_retirement(
                THREAD_ID,
                force=True,
                permanent=permanent,
            )
            assert result["state"] == "settled"
        else:
            with pytest.raises(HTTPException) as exc:
                await control_seams.reconcile_stateless_thread_retirement(
                    THREAD_ID,
                    force=True,
                    permanent=permanent,
                )
            assert exc.value.status_code == 503

    assert succeeds is False
    provisioner.workspace_pod_authority.assert_not_awaited()
    provisioner.release_absent_workspace.assert_not_awaited()
    db.begin_stateless_thread_workspace_retirement.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("authority", ["exact_live", "replacement", "unknown"])
async def test_missing_runtime_nonabsence_refuses_before_queue_close(authority) -> None:
    thread = _absence_thread(
        backing_id=None,
        snapshot_captured=False,
        pending=False,
        permanent=False,
    )
    db = SimpleNamespace(
        get_thread=AsyncMock(return_value=thread),
        begin_stateless_thread_workspace_retirement=AsyncMock(),
    )
    provisioner = SimpleNamespace(
        workspace_pod_authority=AsyncMock(return_value=authority),
        release_absent_workspace=AsyncMock(),
    )

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            container_provisioner_module, "container_provisioner", provisioner
        ),
        pytest.raises(HTTPException) as exc,
    ):
        await control_seams.reconcile_stateless_thread_retirement(
            THREAD_ID,
            force=True,
            permanent=False,
        )

    assert exc.value.status_code == 503
    db.begin_stateless_thread_workspace_retirement.assert_not_awaited()
    provisioner.release_absent_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_unsupported_absence_settled_proof_cannot_upgrade_to_permanent() -> None:
    backing_id = None
    snapshot_captured = False
    thread = _absence_thread(
        backing_id=backing_id,
        snapshot_captured=snapshot_captured,
        pending=False,
        permanent=False,
    )
    thread["status"] = "ended"
    thread["metadata"]["workspace_container"]["status"] = "deleted"
    thread["metadata"]["_stateless_workspace_retirement_settled"] = {
        "terminal_token": 0,
        "cleanup_complete": True,
        "permanent": True,
        "backing_id": backing_id,
        "runtime_incarnation": None,
        "snapshot_restore_required": snapshot_captured,
        "workspace_absence_proven": True,
    }
    db = SimpleNamespace(
        get_thread=AsyncMock(return_value=thread),
        begin_stateless_thread_workspace_retirement=AsyncMock(
            side_effect=RuntimeError("settled workspace absence proof is unsupported")
        ),
    )
    provisioner = SimpleNamespace(
        release_absent_workspace=AsyncMock(return_value=True),
    )

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            container_provisioner_module, "container_provisioner", provisioner
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await control_seams.reconcile_stateless_thread_retirement(
                THREAD_ID,
                force=True,
                permanent=True,
            )

    assert exc.value.status_code == 503
    provisioner.release_absent_workspace.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("snapshot_service_available", [False, True])
async def test_direct_permanent_end_does_not_require_an_uncaptured_snapshot(
    snapshot_service_available: bool,
) -> None:
    thread = _settled_thread()
    thread["status"] = "active"
    thread["metadata"].pop("_stateless_workspace_retirement_settled")
    thread["metadata"]["workspace_container"]["_snapshot_restore_required"] = False
    authority = _lifecycle_authority()
    authority["status"] = "active"
    db = SimpleNamespace(
        get_thread=AsyncMock(return_value=thread),
        get_stateless_thread_lifecycle_authority=AsyncMock(return_value=authority),
        stateless_session_workspace_ensure_lock=_owned_lock,
        merge_thread_config_override=AsyncMock(),
        has_unfinished_session_memory_effects=AsyncMock(return_value=False),
        delete_thread=AsyncMock(),
    )
    snapshots = SimpleNamespace(
        is_available=snapshot_service_available,
        delete_snapshot=AsyncMock(return_value=True),
    )
    reconcile = AsyncMock(
        return_value={
            "state": "settled",
            "thread": thread,
            "closure": {
                "permanent": True,
                "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
                "snapshot_restore_required": False,
            },
        }
    )

    with (
        patch.object(
            access_module,
            "require_thread_owner",
            AsyncMock(return_value=({"sub": "user-1"}, thread)),
        ),
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(snapshot_service_module, "snapshot_service", snapshots),
        patch.object(
            main.app.state.resources,
            "gitea_client",
            SimpleNamespace(is_initialized=False),
        ),
        patch.object(
            thread_retirement_module,
            "reconcile_stateless_thread_retirement",
            reconcile,
        ),
        patch.object(
            officer_conference_module, "conclude_conference_if_any", AsyncMock()
        ),
    ):
        result = await control_seams.end_thread(
            THREAD_ID, SimpleNamespace(), permanent=True, force=True
        )

    assert result == {"status": "deleted"}
    if snapshot_service_available:
        snapshots.delete_snapshot.assert_awaited_once_with(
            THREAD_ID, entity_type="threads"
        )
    else:
        snapshots.delete_snapshot.assert_not_awaited()
    db.delete_thread.assert_awaited_once_with(THREAD_ID)


@pytest.mark.asyncio
async def test_emptydir_permanent_requires_snapshot_prefix_cleanup_after_restore() -> (
    None
):
    thread = _settled_thread()
    thread["metadata"]["workspace_container"]["_snapshot_restore_required"] = False
    authority = _lifecycle_authority()
    db = SimpleNamespace(
        get_thread=AsyncMock(return_value=thread),
        get_stateless_thread_lifecycle_authority=AsyncMock(return_value=authority),
        stateless_session_workspace_ensure_lock=_owned_lock,
        merge_thread_config_override=AsyncMock(),
        has_unfinished_session_memory_effects=AsyncMock(return_value=False),
        delete_thread=AsyncMock(),
    )
    reconcile = AsyncMock(
        return_value={
            "state": "settled",
            "thread": thread,
            "closure": {
                "permanent": True,
                # A successful Resume clears this marker but deliberately
                # retains the immutable S3 prefix for later retries.
                "snapshot_restore_required": False,
            },
        }
    )

    with (
        patch.object(
            access_module,
            "require_thread_owner",
            AsyncMock(return_value=({"sub": "user-1"}, thread)),
        ),
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            snapshot_service_module,
            "snapshot_service",
            SimpleNamespace(is_available=False),
        ),
        patch.object(
            thread_retirement_module,
            "reconcile_stateless_thread_retirement",
            reconcile,
        ),
        patch.object(
            officer_conference_module, "conclude_conference_if_any", AsyncMock()
        ),
        pytest.raises(HTTPException) as exc,
    ):
        await control_seams.end_thread(
            THREAD_ID, SimpleNamespace(), permanent=True, force=True
        )

    assert exc.value.status_code == 503
    db.delete_thread.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("purged", [True, False])
async def test_permanent_virtual_end_purges_exact_workspace_before_row_delete(
    purged,
) -> None:
    from orchestrator.services import thread_uploads

    thread = _settled_virtual_thread()
    authority = _lifecycle_authority()
    db = SimpleNamespace(
        get_thread=AsyncMock(return_value=thread),
        get_stateless_thread_lifecycle_authority=AsyncMock(return_value=authority),
        stateless_session_workspace_ensure_lock=_owned_lock,
        begin_stateless_thread_workspace_retirement=AsyncMock(
            return_value={
                "state": "settled",
                "terminal_token": 8,
                "permanent": True,
                "backing_id": f"rclone:threads/{THREAD_ID}",
                "runtime_incarnation": None,
                "snapshot_restore_required": False,
                "retry": True,
            }
        ),
        merge_thread_config_override=AsyncMock(),
        has_unfinished_session_memory_effects=AsyncMock(return_value=False),
        delete_thread=AsyncMock(),
    )
    purge = AsyncMock(return_value=purged)

    with (
        patch.object(
            access_module,
            "require_thread_owner",
            AsyncMock(return_value=({"sub": "user-1"}, thread)),
        ),
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            snapshot_service_module,
            "snapshot_service",
            SimpleNamespace(is_available=False),
        ),
        patch.object(
            thread_uploads, "purge_attested_stateless_virtual_workspace", purge
        ),
        patch.object(
            main.app.state.resources,
            "gitea_client",
            SimpleNamespace(is_initialized=False),
        ),
        patch.object(
            officer_conference_module, "conclude_conference_if_any", AsyncMock()
        ),
    ):
        if purged:
            result = await control_seams.end_thread(
                THREAD_ID, SimpleNamespace(), permanent=True, force=True
            )
            assert result == {"status": "deleted"}
        else:
            with pytest.raises(HTTPException) as exc:
                await control_seams.end_thread(
                    THREAD_ID, SimpleNamespace(), permanent=True, force=True
                )
            assert exc.value.status_code == 503

    purge.assert_awaited_once()
    if purged:
        db.delete_thread.assert_awaited_once_with(THREAD_ID)
    else:
        db.delete_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_stateless_legacy_suspend_caller_never_falls_through_to_pod_delete() -> (
    None
):
    db = SimpleNamespace(
        get_thread=AsyncMock(
            return_value={"id": THREAD_ID, "execution_lane": "stateless"}
        )
    )
    suspension = SimpleNamespace(
        is_enabled=True,
        suspend_thread_workspace=AsyncMock(return_value=False),
    )
    agent = SimpleNamespace(
        is_available=True,
        delete_agent_pod_by_thread=AsyncMock(),
    )
    persistent = SimpleNamespace(
        is_available=True,
        delete_agent_pod=AsyncMock(),
    )

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            workspace_suspension_module, "workspace_suspension_service", suspension
        ),
        patch.object(agent_provisioner_module, "agent_provisioner", agent),
        patch.object(
            persistent_provisioner_module, "persistent_provisioner", persistent
        ),
    ):
        await control_seams.suspend_thread_resources_inner(THREAD_ID)

    suspension.suspend_thread_workspace.assert_not_awaited()
    agent.delete_agent_pod_by_thread.assert_not_awaited()
    persistent.delete_agent_pod.assert_not_awaited()


@pytest.mark.asyncio
async def test_release_never_starts_while_resident_proof_is_incomplete() -> None:
    metadata = {
        "_stateless_workspace_retirement_pending": True,
        "_stateless_claim_retirement": {
            "terminal_token": 8,
            "claimant_quiesced": True,
            "shell_retirement_required": False,
            "resident_cleanup_required": True,
            "residents_retired": False,
            "remote_retired": True,
            "permanent": False,
            "workspace_generation": None,
            "endpoint_generation": None,
            "runtime_incarnation": None,
            "host_key_fingerprint": None,
        },
        "workspace_container": {},
        "_workspace_binding": {},
    }
    current = {
        "id": THREAD_ID,
        "status": "ended",
        "execution_lane": "stateless",
        "metadata": metadata,
    }
    closure = {
        "state": "closed",
        "terminal_token": 8,
        "claimant_quiesced": True,
        "resident_cleanup_required": False,
        "resident_acknowledged": True,
        "shell_retirement_required": False,
        "remote_acknowledged": True,
        "permanent": False,
    }
    db = SimpleNamespace(
        begin_stateless_thread_workspace_retirement=AsyncMock(return_value=closure),
        get_thread=AsyncMock(return_value=current),
    )
    provisioner = SimpleNamespace(
        release_workspace=AsyncMock(),
        release_absent_workspace=AsyncMock(),
    )

    with (
        patch.object(main.app.state.resources, "postgres_db", db),
        patch.object(
            container_provisioner_module, "container_provisioner", provisioner
        ),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await control_seams.reconcile_stateless_thread_retirement(
                THREAD_ID,
                force=True,
                permanent=False,
            )

    assert exc_info.value.status_code == 503
    provisioner.release_workspace.assert_not_awaited()
    provisioner.release_absent_workspace.assert_not_awaited()
