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


# -----------------------------------------------------------------------------
# A pull-failed Job leaves nothing behind (Task 11a)
#
# The dispatcher fails the Job after create_workspace returns False. That
# terminal transition cancels the creation reservation *and* admits a cleanup
# intent for the same runtime, and neither can settle while the other is open
# (convert_cancelled_workspace_creation_to_cleanup_intent refuses while an
# unsettled intent exists). So the creation itself must hand its never-started
# runtime to the ordinary cancellation -> cleanup protocol before it returns,
# while it still holds the mutation guard and the reservation.
# -----------------------------------------------------------------------------


def container_status(**fields):
    fields.setdefault("name", "workspace")
    fields.setdefault("ready", False)
    fields.setdefault("restart_count", 0)
    fields.setdefault(
        "state",
        SimpleNamespace(
            waiting=SimpleNamespace(reason="ImagePullBackOff", message="not found"),
            running=None,
            terminated=None,
        ),
    )
    fields.setdefault(
        "last_state", SimpleNamespace(waiting=None, running=None, terminated=None)
    )
    return SimpleNamespace(**fields)


def pod_status(*statuses, phase="Pending", ephemeral=(), init=()):
    return SimpleNamespace(
        phase=phase,
        pod_ip="10.42.0.185",
        container_statuses=list(statuses),
        init_container_statuses=list(init),
        ephemeral_container_statuses=list(ephemeral),
    )


EXITED = SimpleNamespace(exit_code=1, reason="Error", started_at=NOW)
EVER_STARTED = {
    "running": pod_status(
        container_status(
            ready=True,
            started=True,
            state=SimpleNamespace(
                waiting=None, running=SimpleNamespace(), terminated=None
            ),
        ),
        phase="Running",
    ),
    "terminated": pod_status(
        container_status(
            state=SimpleNamespace(waiting=None, running=None, terminated=EXITED)
        ),
        phase="Failed",
    ),
    # What the kubelet reports once a never-started Pod is deleted. It is
    # terminal evidence, not never-started evidence: the finalizer protocol
    # owns that state.
    "terminated-unknown": pod_status(
        container_status(
            state=SimpleNamespace(
                waiting=None,
                running=None,
                terminated=SimpleNamespace(
                    exit_code=137, reason="ContainerStatusUnknown", started_at=None
                ),
            )
        ),
        phase="Failed",
    ),
    "restarted": pod_status(container_status(restart_count=1)),
    "last-state-terminated": pod_status(
        container_status(
            last_state=SimpleNamespace(waiting=None, running=None, terminated=EXITED)
        )
    ),
    "started": pod_status(container_status(started=True)),
    "ready": pod_status(container_status(ready=True)),
    "ephemeral-debug-running": pod_status(
        container_status(),
        ephemeral=[
            container_status(
                name="debugger",
                state=SimpleNamespace(
                    waiting=None, running=SimpleNamespace(), terminated=None
                ),
            )
        ],
    ),
    "running-phase": pod_status(container_status(), phase="Running"),
    # Pending pods whose only evidence is the container state itself: the
    # phase gate cannot hide the terminated check or the init-container scan.
    "pending-terminated-main": pod_status(
        container_status(
            state=SimpleNamespace(waiting=None, running=None, terminated=EXITED)
        )
    ),
    "pending-completed-init-container": pod_status(
        container_status(),
        init=[
            container_status(
                name="setup",
                state=SimpleNamespace(
                    waiting=None,
                    running=None,
                    terminated=SimpleNamespace(
                        exit_code=0, reason="Completed", started_at=NOW
                    ),
                ),
            )
        ],
    ),
    # The runtime created the container even though it reports waiting.
    "container-id": pod_status(
        container_status(container_id="containerd://0123456789abcdef")
    ),
    "unreadable-statuses": SimpleNamespace(
        phase="Pending", container_statuses="garbage"
    ),
}


@pytest.mark.parametrize(
    "status",
    [
        pod_status(container_status()),
        pod_status(
            container_status(
                state=SimpleNamespace(
                    waiting=SimpleNamespace(reason="InvalidImageName", message="x"),
                    running=None,
                    terminated=None,
                )
            )
        ),
        pod_status(),
        SimpleNamespace(phase="Pending", container_statuses=None),
    ],
    ids=["image-pull-backoff", "invalid-image-name", "no-statuses", "statuses-none"],
)
def test_a_pod_whose_container_never_ran_is_never_started(status):
    assert provisioner_module._pod_never_started_a_container(
        SimpleNamespace(status=status)
    )


@pytest.mark.parametrize("status", EVER_STARTED.values(), ids=EVER_STARTED.keys())
def test_a_pod_whose_container_ever_ran_is_not_never_started(status):
    assert not provisioner_module._pod_never_started_a_container(
        SimpleNamespace(status=status)
    )


def settling_job_provisioner(monkeypatch, *, pod_status_override=None):
    provisioner = job_provisioner(monkeypatch, SandboxSettings(image=IMAGE))
    cluster = {}

    def create_pod(*, body, **_kwargs):
        cluster["pod"] = failing_pull(body)
        if pod_status_override is not None:
            cluster["pod"].status = pod_status_override
        return cluster["pod"]

    provisioner._core_api.create_namespaced_pod.side_effect = create_pod
    provisioner._core_api.read_namespaced_pod.side_effect = lambda **_: cluster["pod"]
    order = []
    diagnostic = provisioner._record_creation_diagnostic

    async def record_diagnostic(*args, **kwargs):
        order.append("diagnostic")
        return await diagnostic(*args, **kwargs)

    provisioner._record_creation_diagnostic = record_diagnostic
    real_settle = (
        _TemplatedJobDB.settle_managed_repository_workspace_creation_reservation
    )
    settles = []

    async def settle(db, owner_id, **kwargs):
        order.append("settle")
        settles.append({"owner_id": owner_id, **kwargs})
        return await real_settle(db, owner_id, **kwargs)

    monkeypatch.setattr(
        _TemplatedJobDB,
        "settle_managed_repository_workspace_creation_reservation",
        settle,
    )
    return provisioner, order, settles


@pytest.mark.asyncio
async def test_a_pull_failed_job_settles_its_creation_on_the_never_started_pod(
    monkeypatch,
):
    provisioner, order, settles = settling_job_provisioner(monkeypatch)

    assert await provisioner.create_workspace(WorkspaceOwner.job(JOB_ID)) is False

    # The Job keeps its reason, written while the reservation was still open,
    # and the generation then closes on exactly the Pod it created. The Job's
    # terminal cleanup, not this request, deletes that Pod.
    assert job_context_updates(provisioner)[-1] == {"error": PULL_FAILURE}
    assert order == ["diagnostic", "settle"]
    ((settled,),) = [settles]
    assert settled["owner_kind"] == "job"
    assert settled["scope"] == "workspace_container"
    assert settled["runtime_incarnation"] == _TEST_POD_UID
    assert provisioner._db._creation_reservation["phase"] == "settled"
    provisioner._core_api.delete_namespaced_pod.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", EVER_STARTED.values(), ids=EVER_STARTED.keys())
async def test_a_pull_failure_never_settles_on_a_pod_that_ever_ran(monkeypatch, status):
    provisioner, _order, settles = settling_job_provisioner(
        monkeypatch, pod_status_override=status
    )
    provisioner._wait_for_ready = AsyncMock(
        side_effect=WorkspaceImagePullError(PULL_FAILURE)
    )

    assert await provisioner.create_workspace(WorkspaceOwner.job(JOB_ID)) is False

    assert settles == []
    assert provisioner._db._creation_reservation["settled_at"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owner", "error", "strict_stateless"),
    [
        (
            WorkspaceOwner.session("66666666-7777-4888-8999-aaaaaaaaaaaa"),
            WorkspaceImagePullError(PULL_FAILURE),
            True,
        ),
        (
            WorkspaceOwner.session("66666666-7777-4888-8999-aaaaaaaaaaaa"),
            WorkspaceImagePullError(PULL_FAILURE),
            False,
        ),
        (WorkspaceOwner.job(JOB_ID), ApiException(status=403), False),
        (WorkspaceOwner.job(JOB_ID), RuntimeError("seed failed"), False),
    ],
    ids=["stateless-session", "pinned-session", "quota-rejection", "other-failure"],
)
async def test_only_a_job_pull_failure_is_settled(owner, error, strict_stateless):
    provisioner = ContainerProvisioner()
    provisioner._db = _TemplatedJobDB()
    provisioner._core_api = MagicMock()

    settled = await provisioner._settle_failed_job_creation(
        owner,
        {"id": "r", "runtime_incarnation": _TEST_POD_UID, "operation_kind": "create"},
        error,
        strict_stateless=strict_stateless,
        fresh_storage=True,
    )

    assert settled is False
    provisioner._core_api.read_namespaced_pod.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation_kind", "fresh_storage"),
    [
        ("restore", True),
        ("reattach", True),
        ("adopt", True),
        (None, True),
        ("create", False),
    ],
    ids=["restore", "reattach", "adopt", "unknown", "reused-claim"],
)
async def test_only_a_fresh_create_is_settled(operation_kind, fresh_storage):
    # Settling hands the volume to the Job's terminal reclaim, which deletes
    # it. A kept claim may hold user data that still needs its archive.
    provisioner = ContainerProvisioner()
    provisioner._db = _TemplatedJobDB()
    provisioner._core_api = MagicMock()

    settled = await provisioner._settle_failed_job_creation(
        WorkspaceOwner.job(JOB_ID),
        {
            "id": "r",
            "runtime_incarnation": _TEST_POD_UID,
            "operation_kind": operation_kind,
        },
        WorkspaceImagePullError(PULL_FAILURE),
        strict_stateless=False,
        fresh_storage=fresh_storage,
    )

    assert settled is False
    provisioner._core_api.read_namespaced_pod.assert_not_called()


def with_workspace_volume(provisioner, *, kept):
    """Serve a PVC and Service; ``kept`` makes the claim already exist (409)."""

    api = provisioner._core_api
    pod_create = api.create_namespaced_pod.side_effect
    pod_read = api.read_namespaced_pod.side_effect
    fake_cluster(provisioner, [])
    api.create_namespaced_pod.side_effect = pod_create
    api.read_namespaced_pod.side_effect = pod_read
    provisioner._pvc_enabled = True
    if kept:
        create_claim = api.create_namespaced_persistent_volume_claim.side_effect

        def already_there(**kwargs):
            create_claim(**kwargs)
            raise ApiException(status=409, reason="AlreadyExists")

        api.create_namespaced_persistent_volume_claim.side_effect = already_there


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation_kind", "kept", "settles"),
    [
        ("create", False, True),
        ("create", True, False),
        ("restore", False, False),
        ("restore", True, False),
    ],
    ids=["fresh-create", "create-over-kept-claim", "restore", "restore-kept-claim"],
)
async def test_a_pull_failure_settles_only_a_create_with_a_fresh_volume(
    monkeypatch, operation_kind, kept, settles
):
    provisioner, _order, settle_calls = settling_job_provisioner(monkeypatch)
    with_workspace_volume(provisioner, kept=kept)

    assert (
        await provisioner.create_workspace(
            WorkspaceOwner.job(JOB_ID), operation_kind=operation_kind
        )
        is False
    )

    # The Job still gets its reason either way.
    assert job_context_updates(provisioner)[-1] == {"error": PULL_FAILURE}
    assert bool(settle_calls) is settles
    assert (provisioner._db._creation_reservation["settled_at"] is not None) is settles
    # Nothing is deleted by the failed creation in any case.
    provisioner._core_api.delete_namespaced_pod.assert_not_called()
    provisioner._core_api.delete_namespaced_persistent_volume_claim.assert_not_called()
    provisioner._core_api.delete_namespaced_service.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["superseded-reservation", "replacement-pod", "pod-unreadable"]
)
async def test_settling_needs_the_exact_current_creation_and_never_raises(
    monkeypatch, change
):
    provisioner, _order, settles = settling_job_provisioner(monkeypatch)
    real_settle = provisioner._settle_failed_job_creation
    observed = {}

    async def settle_failed(owner_, reservation, error, **kwargs):
        if change == "superseded-reservation":
            provisioner._workspace_creation_reservation_is_current = AsyncMock(
                return_value=False
            )
        elif change == "replacement-pod":
            reservation = {
                **reservation,
                "runtime_incarnation": "99999999-9999-4999-8999-999999999999",
            }
        else:
            provisioner._core_api.read_namespaced_pod.side_effect = RuntimeError(
                "apiserver unavailable"
            )
        observed["settled"] = await real_settle(owner_, reservation, error, **kwargs)
        return observed["settled"]

    provisioner._settle_failed_job_creation = settle_failed

    assert await provisioner.create_workspace(WorkspaceOwner.job(JOB_ID)) is False

    assert observed["settled"] is False
    assert settles == []
    assert provisioner._db._creation_reservation["settled_at"] is None


class _TerminalReclaimDB:
    def __init__(self, *, captured):
        self.intent = {
            "intent_generation": 7,
            "target_disposition": "deleted",
            "resource_policy": "terminal_reclaim",
            "reclaim_shared_resources": True,
            "capture_complete": captured,
            "resources_captured_at": NOW if captured else None,
        }

    async def get_managed_repository_workspace_cleanup_intent(self, *_a, **_k):
        return dict(self.intent)


def replaying_provisioner(status, *, finalizers=("default",), captured=False):
    owner = WorkspaceOwner.job(JOB_ID)
    provisioner = ContainerProvisioner()
    provisioner._k8s_available = True
    provisioner._db = _TerminalReclaimDB(captured=captured)
    retained = _owned_pod(owner, namespace=provisioner._namespace)
    retained.metadata.finalizers = (
        [provisioner_module.STATELESS_WORKSPACE_PROCESS_ZERO_FINALIZER]
        if finalizers == ("default",)
        else list(finalizers)
    )
    retained.status = status
    provisioner._core_api = SimpleNamespace(read_namespaced_pod=None)
    provisioner._bounded_kubernetes_call = AsyncMock(return_value=retained)
    provisioner.workspace_pod_authority = AsyncMock(return_value="exact_live")
    provisioner.reconcile_workspace_cleanup_intent = AsyncMock(
        return_value=provisioner_module.WorkspaceCleanupOutcome("settled", 7)
    )
    return owner, provisioner


@pytest.mark.asyncio
async def test_deleting_a_failed_job_reconciles_its_never_started_pod_without_ssh():
    owner, provisioner = replaying_provisioner(pod_status(container_status()))

    outcome = await provisioner.replay_terminal_workspace_cleanup(
        owner, expected_runtime_incarnation=_TEST_POD_UID
    )

    assert outcome.settled
    provisioner.reconcile_workspace_cleanup_intent.assert_awaited_once_with(
        owner, expected_runtime_incarnation=_TEST_POD_UID, intent_generation=7
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("captured", [False, True], ids=["uncaptured", "captured"])
@pytest.mark.parametrize(
    ("status", "finalizers"),
    [(status, ("default",)) for status in EVER_STARTED.values()]
    + [(pod_status(container_status()), ())],
    ids=[*EVER_STARTED.keys(), "never-started-without-finalizer"],
)
async def test_deleting_a_job_keeps_ssh_retirement_for_any_other_live_pod(
    status, finalizers, captured
):
    owner, provisioner = replaying_provisioner(
        status, finalizers=finalizers, captured=captured
    )

    # None sends the caller to today's SSH attestation and captured release.
    assert (
        await provisioner.replay_terminal_workspace_cleanup(
            owner, expected_runtime_incarnation=_TEST_POD_UID
        )
        is None
    )
    provisioner.reconcile_workspace_cleanup_intent.assert_not_awaited()
