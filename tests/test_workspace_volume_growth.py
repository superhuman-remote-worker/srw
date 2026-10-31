"""A full job volume grows once, on the exact runtime's own PVC.

knowledge-base/knowledge/issues/workspace_disk_full_pauses_job_as_transport_lost.md
(option D): the orchestrator grows a full job PVC once, up to
WORKSPACE_PVC_GROW_SIZE (20Gi), waits for the online resize and resumes.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from orchestrator.services.completion import handle_pod_workspace_recovery
from orchestrator.services.container_provisioner import (
    ContainerProvisioner,
    _pvc_name_for,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from shared.orch_surface.formatters import _operator_pause_hold_lines
from shared.workspace_disk import WorkspaceDiskFullReport, WorkspaceVolumeGrowth

OWNER = WorkspaceOwner.job("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
RUNTIME = "11111111-1111-4111-8111-111111111111"
PVC_UID = "22222222-2222-4222-8222-222222222222"
GIB = 1024**3


def _provisioner() -> ContainerProvisioner:
    provisioner = ContainerProvisioner()
    provisioner._k8s_available = True
    provisioner._core_api = MagicMock()
    provisioner._pvc_grow_size = "20Gi"
    provisioner._pvc_grow_wait_seconds = 0.0
    provisioner._core_api.read_namespaced_pod.return_value = SimpleNamespace(
        metadata=SimpleNamespace(uid=RUNTIME),
        spec=SimpleNamespace(
            volumes=[
                {
                    "name": "workspace-data",
                    "persistentVolumeClaim": {"claimName": _pvc_name_for(OWNER)},
                }
            ],
            containers=[
                {
                    "name": "workspace",
                    "volumeMounts": [
                        {"name": "workspace-data", "mountPath": "/home/agent-host"}
                    ],
                }
            ],
            init_containers=[],
            ephemeral_containers=[],
        ),
    )
    return provisioner


def _claim(provisioner, *, requested="10Gi", capacity="10Gi", annotations=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=_pvc_name_for(OWNER),
            namespace=provisioner._namespace,
            uid=PVC_UID,
            resource_version="7",
            deletion_timestamp=None,
            annotations=annotations,
            labels={
                "app": "srw-workspace",
                "srw/component": "workspace-pvc",
                "srw.io/component": "agent-workspace",
                OWNER.label_key: OWNER.id,
            },
        ),
        spec=SimpleNamespace(
            access_modes=["ReadWriteOnce"],
            storage_class_name="longhorn-ephemeral",
            volume_mode="Filesystem",
            selector=None,
            data_source=None,
            data_source_ref=None,
            resources=SimpleNamespace(requests={"storage": requested}),
        ),
        status=SimpleNamespace(capacity={"storage": capacity}),
    )


async def _grow(provisioner):
    return await provisioner.grow_job_workspace_volume(
        OWNER, expected_runtime_incarnation=RUNTIME
    )


class TestGrowJobWorkspaceVolume:
    @pytest.mark.asyncio
    async def test_grows_the_exact_claim_with_a_fenced_patch_and_waits(self):
        provisioner = _provisioner()
        api = provisioner._core_api
        api.read_namespaced_persistent_volume_claim.side_effect = [
            _claim(provisioner),
            _claim(provisioner, requested="20Gi", capacity="10Gi"),
            _claim(provisioner, requested="20Gi", capacity="20Gi"),
        ]
        provisioner._pvc_grow_wait_seconds = 30.0

        with patch(
            "orchestrator.services.container_provisioner.asyncio.sleep",
            new=AsyncMock(),
        ):
            growth = await _grow(provisioner)

        assert growth == WorkspaceVolumeGrowth("resized", 20 * GIB)
        patch_call = api.patch_namespaced_persistent_volume_claim.call_args
        assert patch_call.kwargs["name"] == _pvc_name_for(OWNER)
        assert patch_call.kwargs["body"] == [
            {"op": "test", "path": "/metadata/uid", "value": PVC_UID},
            {"op": "test", "path": "/metadata/resourceVersion", "value": "7"},
            {"op": "test", "path": "/spec/resources/requests/storage", "value": "10Gi"},
            {
                "op": "add",
                "path": "/metadata/annotations",
                "value": {"srw.io/workspace-disk-grown-from": "10Gi"},
            },
            {
                "op": "replace",
                "path": "/spec/resources/requests/storage",
                "value": "20Gi",
            },
        ]

    @pytest.mark.asyncio
    async def test_existing_annotations_are_kept(self):
        provisioner = _provisioner()
        api = provisioner._core_api
        api.read_namespaced_persistent_volume_claim.side_effect = [
            _claim(provisioner, annotations={"other": "kept"}),
            _claim(provisioner, requested="20Gi", capacity="20Gi"),
        ]
        await _grow(provisioner)
        added = api.patch_namespaced_persistent_volume_claim.call_args.kwargs["body"][3]
        assert added == {
            "op": "add",
            "path": "/metadata/annotations/srw.io~1workspace-disk-grown-from",
            "value": "10Gi",
        }

    @pytest.mark.asyncio
    async def test_unfinished_resize_is_pending(self):
        provisioner = _provisioner()
        provisioner._core_api.read_namespaced_persistent_volume_claim.side_effect = [
            _claim(provisioner),
            _claim(provisioner, requested="20Gi", capacity="10Gi"),
        ]
        assert await _grow(provisioner) == WorkspaceVolumeGrowth("pending", 20 * GIB)

    @pytest.mark.asyncio
    async def test_refused_expansion_is_reported_not_raised(self):
        provisioner = _provisioner()
        api = provisioner._core_api
        api.read_namespaced_persistent_volume_claim.return_value = _claim(provisioner)
        refusal = Exception("forbidden")
        refusal.status = 403
        refusal.reason = "Forbidden"
        api.patch_namespaced_persistent_volume_claim.side_effect = refusal

        growth = await _grow(provisioner)

        assert growth.outcome == "refused"
        assert growth.detail == "the cluster refused it (HTTP 403 Forbidden)"

    @pytest.mark.asyncio
    async def test_replaced_pod_volume_is_never_touched(self):
        provisioner = _provisioner()
        provisioner._core_api.read_namespaced_pod.return_value.metadata.uid = (
            "33333333-3333-4333-8333-333333333333"
        )
        growth = await _grow(provisioner)
        assert growth.outcome == "unsupported"
        provisioner._core_api.patch_namespaced_persistent_volume_claim.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_volume_already_at_the_size_is_left_alone(self):
        provisioner = _provisioner()
        provisioner._core_api.read_namespaced_persistent_volume_claim.return_value = (
            _claim(provisioner, requested="30Gi", capacity="30Gi")
        )
        growth = await _grow(provisioner)
        assert growth.outcome == "unsupported"
        assert growth.detail == "the volume already requests 30 GiB"
        provisioner._core_api.patch_namespaced_persistent_volume_claim.assert_not_called()

    @pytest.mark.asyncio
    async def test_retried_effect_recognizes_its_own_growth(self):
        provisioner = _provisioner()
        provisioner._core_api.read_namespaced_persistent_volume_claim.return_value = (
            _claim(
                provisioner,
                requested="20Gi",
                capacity="20Gi",
                annotations={"srw.io/workspace-disk-grown-from": "10Gi"},
            )
        )
        assert await _grow(provisioner) == WorkspaceVolumeGrowth("resized", 20 * GIB)
        provisioner._core_api.patch_namespaced_persistent_volume_claim.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("size", ["", "0"])
    async def test_growth_can_be_turned_off(self, size):
        provisioner = _provisioner()
        provisioner._pvc_grow_size = size
        growth = await _grow(provisioner)
        assert growth.outcome == "unsupported"
        provisioner._core_api.read_namespaced_pod.assert_not_called()


def _disk_full_error(*, command_in_flight=False):
    return {
        "type": "workspace_unavailable",
        "recoverable": True,
        "message": "Remote tmux create or observe failed with exit code 82",
        "reason": "workspace_disk_full",
        "disk": {
            "size_bytes": 10 * GIB,
            "used_bytes": 10 * GIB,
            "available_bytes": 0,
        },
        "command_in_flight": command_in_flight,
    }


class TestDiskFullCompletionArm:
    """Unit view of the arm; the real-Postgres file covers the jobs-row write."""

    @staticmethod
    async def _recover(job, error, *, growth, db=None):
        db = db or MagicMock()
        db.hold_full_workspace_disk = AsyncMock(
            return_value={"paused": True, "actions": ["x"], "held_for_resume": True}
        )
        dispatch = Mock()
        probe = AsyncMock(side_effect=AssertionError("no probe for a full disk"))
        delete = AsyncMock(side_effect=AssertionError("no cleanup for a full disk"))
        grow = AsyncMock(return_value=growth)
        await handle_pod_workspace_recovery(
            job,
            "job-1",
            error,
            db=db,
            delete_workspace=delete,
            trigger_dispatch=dispatch,
            probe=probe,
            grow_workspace_volume=grow,
        )
        return db.hold_full_workspace_disk.call_args.kwargs, dispatch, grow

    @pytest.mark.asyncio
    async def test_resized_volume_resumes_without_a_command_in_flight(self):
        job = {"context": {"workspace_container": {"recovery_attempts": 3}}}
        resized = WorkspaceVolumeGrowth("resized", 20 * GIB)
        kwargs, dispatch, _ = await self._recover(
            job, _disk_full_error(), growth=resized
        )
        assert kwargs["resume"] is True
        assert kwargs["growth"] == resized
        assert kwargs["disk_full"] == WorkspaceDiskFullReport.from_error(
            _disk_full_error()
        )
        dispatch.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_command_in_flight_keeps_the_explicit_resume_hold(self):
        job = {"context": {"workspace_container": {}}}
        kwargs, dispatch, _ = await self._recover(
            job,
            _disk_full_error(command_in_flight=True),
            growth=WorkspaceVolumeGrowth("resized", 20 * GIB),
        )
        assert kwargs["resume"] is False
        dispatch.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_volume_grows_only_once_per_job(self):
        job = {
            "context": {
                "workspace_container": {
                    "disk_growth": {"outcome": "resized", "size_bytes": 20 * GIB}
                }
            }
        }
        kwargs, dispatch, grow = await self._recover(
            job, _disk_full_error(), growth=WorkspaceVolumeGrowth("resized", 20 * GIB)
        )
        grow.assert_not_awaited()
        assert kwargs["growth"] == WorkspaceVolumeGrowth("used", 20 * GIB)
        assert kwargs["resume"] is False
        dispatch.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_failed_growth_still_holds(self):
        job = {"context": {"workspace_container": {}}}
        db = MagicMock()
        kwargs, _, _ = await self._recover(
            job,
            _disk_full_error(),
            growth=WorkspaceVolumeGrowth("refused", 20 * GIB, "HTTP 403"),
            db=db,
        )
        assert kwargs["resume"] is False


def test_disk_full_hold_line_shows_the_usage():
    job = {
        "context": {
            "_operator_pause_hold": {
                "source": "workspace_disk_full",
                "attention": {
                    "disk": {
                        "size_bytes": 10 * GIB,
                        "used_bytes": 10 * GIB,
                        "available_bytes": 0,
                    }
                },
            }
        }
    }
    assert _operator_pause_hold_lines(job) == [
        "Workspace disk full hold (10 GiB of 10 GiB used, 0 B free): workspace "
        "and checkpoints retained until an explicit Resume or Cancel"
    ]
