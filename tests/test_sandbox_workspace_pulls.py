"""A custom image that cannot be pulled fails with its reason, never hangs."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from kubernetes.client.exceptions import ApiException

from orchestrator.services import container_provisioner as provisioner_module
from orchestrator.services.container_provisioner import (
    ContainerProvisioner,
    WorkspaceImagePullError,
    WorkspaceRuntimeAuthorityError,
)
from orchestrator.services.sandbox_workspace_settings import (
    SandboxSettings,
    classify_image_pull,
    pod_admission_rejection,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests.test_container_provisioner import (
    _TEST_POD_UID,
    _PinnedSessionContainerDB,
    _owned_pod,
    _pod_from_manifest,
)
from tests.test_sandbox_workspace_provisioner import (
    _TemplatedPinnedDB,
    fake_cluster,
    stub_plan_inputs,
)

IMAGE = "registry.example/team/workspace:1"
NOW = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)
JOB_ID = "11111111-2222-4333-8444-555555555555"
PULL_FAILURE = (
    f"Workspace image {IMAGE} could not be pulled: InvalidImageName (bad ref)"
)


def pod(reason, *, age_seconds=0, message=None, phase="Pending"):
    waiting = SimpleNamespace(reason=reason, message=message) if reason else None
    status = SimpleNamespace(
        name="workspace",
        ready=False,
        state=SimpleNamespace(waiting=waiting),
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(
            creation_timestamp=NOW - timedelta(seconds=age_seconds)
        ),
        status=SimpleNamespace(phase=phase, pod_ip=None, container_statuses=[status]),
    )


def verdict(p, timeout=600):
    return classify_image_pull(p, image=IMAGE, now=NOW, pull_timeout_seconds=timeout)


def test_invalid_image_name_fails_at_once():
    result = verdict(pod("InvalidImageName", message="bad ref"))
    assert result.state == "failed"
    assert result.message == PULL_FAILURE


def test_a_never_pull_image_missing_from_the_node_fails_at_once():
    # pullPolicy: Never with the image absent can't resolve without someone
    # loading the image onto the node, so waiting out the budget is pointless.
    result = verdict(pod("ErrImageNeverPull", message="not present"))
    assert result.state == "failed"
    assert result.message == (
        f"Workspace image {IMAGE} could not be pulled: ErrImageNeverPull (not present)"
    )


def test_backoff_is_pulling_until_the_budget_then_fails():
    assert verdict(pod("ImagePullBackOff", age_seconds=30)).state == "pulling"
    assert verdict(pod("ImagePullBackOff", age_seconds=601)).state == "failed"
    assert verdict(pod("ErrImagePull", age_seconds=601)).state == "failed"


def test_container_creating_never_fails_on_its_own():
    assert verdict(pod("ContainerCreating", age_seconds=9999)).state == "pulling"


def test_running_container_is_ok():
    assert verdict(pod(None, phase="Running")).state == "ok"


def test_quota_rejection_is_described():
    exc = SimpleNamespace(
        status=403,
        body='{"message": "exceeded quota: workspace-quota, requested: memory=64Gi"}',
    )
    assert pod_admission_rejection(exc) == (
        "Workspace pod was rejected by the cluster: exceeded quota: "
        "workspace-quota, requested: memory=64Gi"
    )
    assert pod_admission_rejection(SimpleNamespace(status=500, body="")) is None


def test_quota_rejection_reads_the_kubernetes_client_bytes_body():
    # kubernetes 36 hands the raw urllib3 bytes to ApiException.body.
    exc = ApiException(status=403, reason="Forbidden")
    exc.body = b'{"kind": "Status", "message": "exceeded quota: workspace-quota"}'
    assert pod_admission_rejection(exc) == (
        "Workspace pod was rejected by the cluster: exceeded quota: workspace-quota"
    )


def provisioner_reading(p):
    provisioner = ContainerProvisioner()
    provisioner._core_api = SimpleNamespace(read_namespaced_pod=None)
    provisioner._bounded_kubernetes_call = AsyncMock(return_value=p)
    return provisioner


def fast_polls(monkeypatch):
    real_sleep = asyncio.sleep
    monkeypatch.setattr(
        "orchestrator.services.container_provisioner.asyncio.sleep",
        lambda _seconds: real_sleep(0.05),
    )


@pytest.mark.asyncio
async def test_wait_raises_on_a_failed_pull():
    provisioner = provisioner_reading(pod("InvalidImageName", message="bad ref"))
    with pytest.raises(WorkspaceImagePullError, match="InvalidImageName"):
        await provisioner._wait_for_ready("workspace-x", timeout=5, pull_image=IMAGE)


@pytest.mark.asyncio
async def test_wait_extends_while_the_image_is_still_pulling(monkeypatch):
    young = pod("ContainerCreating")
    young.metadata.creation_timestamp = datetime.now(timezone.utc)
    provisioner = provisioner_reading(young)
    provisioner._image_pull_timeout = 0.5
    fast_polls(monkeypatch)
    loop = asyncio.get_running_loop()
    started = loop.time()
    assert (
        await provisioner._wait_for_ready("workspace-x", timeout=0.1, pull_image=IMAGE)
        is None
    )
    assert loop.time() - started >= 0.4


@pytest.mark.asyncio
async def test_wait_fails_a_backoff_whose_budget_runs_out_between_polls(monkeypatch):
    # The pod's age comes from the API server's clock (here 50ms ahead of ours)
    # and can read just under the budget at the wait's last poll. The wait must
    # still fail, not report "not ready yet" and leave the job waiting on a pod
    # that never starts.
    backoff = pod("ImagePullBackOff")
    backoff.metadata.creation_timestamp = datetime.now(timezone.utc) + timedelta(
        seconds=0.05
    )
    provisioner = provisioner_reading(backoff)
    provisioner._image_pull_timeout = 0.5
    fast_polls(monkeypatch)
    with pytest.raises(WorkspaceImagePullError, match="ImagePullBackOff"):
        await provisioner._wait_for_ready("workspace-x", timeout=0.1, pull_image=IMAGE)


@pytest.mark.asyncio
async def test_a_pod_that_is_not_ours_is_an_authority_error_not_a_pull_error():
    owner = WorkspaceOwner.job(JOB_ID)
    provisioner = ContainerProvisioner()
    foreign = _owned_pod(
        owner,
        uid="99999999-9999-4999-8999-999999999999",
        namespace=provisioner._namespace,
    )
    foreign.status = pod("InvalidImageName", message="bad ref").status
    provisioner._core_api = SimpleNamespace(read_namespaced_pod=None)
    provisioner._bounded_kubernetes_call = AsyncMock(return_value=foreign)
    with pytest.raises(WorkspaceRuntimeAuthorityError):
        await provisioner._wait_for_ready(
            owner.pod_name,
            timeout=5,
            expected_owner=owner,
            expected_runtime_incarnation=_TEST_POD_UID,
            pull_image=IMAGE,
        )


@pytest.mark.asyncio
async def test_job_creation_records_the_pull_error_for_the_job():
    provisioner = ContainerProvisioner()
    provisioner._workspace_creation_reservation_is_current = AsyncMock(
        return_value=True
    )
    provisioner._set_context = AsyncMock(return_value=True)
    owner = WorkspaceOwner.job("11111111-2222-4333-8444-555555555555")
    await provisioner._record_creation_diagnostic(
        owner,
        {"id": "r"},
        WorkspaceImagePullError("Workspace image x could not be pulled: ErrImagePull"),
        strict_stateless=False,
    )
    provisioner._set_context.assert_awaited_once_with(
        owner, {"error": "Workspace image x could not be pulled: ErrImagePull"}
    )


@pytest.mark.asyncio
async def test_strict_stateless_creation_publishes_no_projection():
    provisioner = ContainerProvisioner()
    provisioner._set_context = AsyncMock(return_value=True)
    await provisioner._record_creation_diagnostic(
        WorkspaceOwner.session("66666666-7777-4888-8999-aaaaaaaaaaaa"),
        {"id": "r"},
        WorkspaceImagePullError("x"),
        strict_stateless=True,
    )
    provisioner._set_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_session_creation_never_gets_an_error_projection():
    # Sessions only log in A1 (spec refinement 1), even on a non-strict path.
    provisioner = ContainerProvisioner()
    provisioner._workspace_creation_reservation_is_current = AsyncMock(
        return_value=True
    )
    provisioner._set_context = AsyncMock(return_value=True)
    await provisioner._record_creation_diagnostic(
        WorkspaceOwner.session("66666666-7777-4888-8999-aaaaaaaaaaaa"),
        {"id": "r"},
        WorkspaceImagePullError("x"),
        strict_stateless=False,
    )
    provisioner._set_context.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reservation_check",
    [
        {"return_value": False},
        {"side_effect": RuntimeError("database unavailable")},
    ],
    ids=["superseded", "unreadable"],
)
async def test_a_diagnostic_needs_a_current_reservation_and_never_raises(
    reservation_check,
):
    provisioner = ContainerProvisioner()
    provisioner._workspace_creation_reservation_is_current = AsyncMock(
        **reservation_check
    )
    provisioner._set_context = AsyncMock(return_value=True)
    await provisioner._record_creation_diagnostic(
        WorkspaceOwner.job(JOB_ID),
        {"id": "r"},
        WorkspaceImagePullError("x"),
        strict_stateless=False,
    )
    provisioner._set_context.assert_not_awaited()


def failing_pull(body):
    created = _pod_from_manifest(body, phase="Pending")
    (status,) = created.status.container_statuses
    status.ready = False
    status.state.waiting = SimpleNamespace(reason="InvalidImageName", message="bad ref")
    return created


class _TemplatedJobDB(_PinnedSessionContainerDB):
    async def fetchrow(self, *args):
        return None


def job_provisioner(monkeypatch, settings):
    provisioner = ContainerProvisioner()
    provisioner._k8s_available = True
    provisioner._pvc_enabled = False
    provisioner._db = _TemplatedJobDB()
    provisioner._core_api = MagicMock()
    stub_plan_inputs(monkeypatch, provisioner)
    monkeypatch.setattr(
        provisioner_module,
        "resolve_sandbox_settings",
        AsyncMock(return_value=settings),
    )
    monkeypatch.setattr(
        provisioner_module.workspace_metering,
        "open_interval",
        AsyncMock(return_value=None),
    )
    return provisioner


def job_context_updates(provisioner):
    return [
        call.args[1]
        for call in provisioner._db.merge_workspace_container_context.await_args_list
    ]


@pytest.mark.asyncio
async def test_a_job_whose_image_cannot_be_pulled_fails_with_the_reason(monkeypatch):
    provisioner = job_provisioner(monkeypatch, SandboxSettings(image=IMAGE))
    cluster = {}

    def create_pod(*, body, **_kwargs):
        cluster["pod"] = failing_pull(body)
        return cluster["pod"]

    provisioner._core_api.create_namespaced_pod.side_effect = create_pod
    provisioner._core_api.read_namespaced_pod.side_effect = lambda **_: cluster["pod"]

    assert await provisioner.create_workspace(WorkspaceOwner.job(JOB_ID)) is False

    updates = job_context_updates(provisioner)
    assert updates[-1] == {"error": PULL_FAILURE}
    assert not any(update.get("status") in {"failed", "ready"} for update in updates)


@pytest.mark.asyncio
async def test_a_job_pod_rejected_by_quota_fails_with_the_reason(monkeypatch):
    provisioner = job_provisioner(monkeypatch, SandboxSettings(image=IMAGE))
    rejection = ApiException(status=403, reason="Forbidden")
    rejection.body = b'{"message": "exceeded quota: workspace-quota"}'
    provisioner._core_api.create_namespaced_pod.side_effect = rejection

    assert await provisioner.create_workspace(WorkspaceOwner.job(JOB_ID)) is False

    assert job_context_updates(provisioner) == [
        {
            "error": (
                "Workspace pod was rejected by the cluster: exceeded quota: "
                "workspace-quota"
            )
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("settings", "watched"),
    [(SandboxSettings(), None), (SandboxSettings(image=IMAGE), IMAGE)],
    ids=["installation-image", "custom-image"],
)
async def test_only_a_custom_image_is_watched_for_pull_failures(
    monkeypatch, settings, watched
):
    provisioner = job_provisioner(monkeypatch, settings)
    provisioner._core_api.create_namespaced_pod.side_effect = (
        lambda *, body, **_: _pod_from_manifest(body)
    )
    provisioner._wait_for_ready = AsyncMock(return_value="10.42.0.100")

    assert await provisioner.create_workspace(WorkspaceOwner.job(JOB_ID)) is True

    wait = provisioner._wait_for_ready.await_args.kwargs
    assert (wait["timeout"], wait["pull_image"]) == (120, watched)


async def create_pinned_session(monkeypatch, *, wait_for_ready=None):
    events = []
    db = _TemplatedPinnedDB(events)
    provisioner = ContainerProvisioner()
    provisioner._db = db
    provisioner._k8s_available = True
    provisioner._core_api = MagicMock()
    provisioner._pvc_enabled = True
    stub_plan_inputs(monkeypatch, provisioner)
    fake_cluster(provisioner, events)
    monkeypatch.setattr(
        provisioner_module.workspace_metering,
        "open_interval",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        provisioner_module,
        "resolve_sandbox_settings",
        AsyncMock(return_value=SandboxSettings(image=IMAGE)),
    )
    if wait_for_ready is not None:
        provisioner._wait_for_ready = wait_for_ready
    else:
        read_pod = provisioner._core_api.read_namespaced_pod.side_effect

        def read_pulling(**kwargs):
            observed = read_pod(**kwargs)
            (status,) = observed.status.container_statuses
            status.ready = False
            status.state.waiting = SimpleNamespace(
                reason="InvalidImageName", message="bad ref"
            )
            observed.status.phase = "Pending"
            return observed

        provisioner._core_api.read_namespaced_pod.side_effect = read_pulling
    created = await provisioner.create_pinned_thread_workspace(db.THREAD_ID)
    return created, db, [name for name, _ in events]


@pytest.mark.asyncio
async def test_a_pinned_session_pull_failure_is_logged_and_ends_like_a_timeout(
    monkeypatch, caplog
):
    caplog.set_level(logging.ERROR, logger=provisioner_module.__name__)

    failed, db, events = await create_pinned_session(monkeypatch)
    timed_out, timeout_db, timeout_events = await create_pinned_session(
        monkeypatch, wait_for_ready=AsyncMock(return_value=None)
    )

    assert (failed, timed_out) == (False, False)
    assert PULL_FAILURE in caplog.text
    assert events == timeout_events
    assert "complete" not in events
    assert db.intent["status"] == timeout_db.intent["status"] == "planned"
    assert db.published.keys() == timeout_db.published.keys()
    assert db.workspace["status"] == timeout_db.workspace["status"] == "pending"
