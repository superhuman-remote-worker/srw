"""Provider observations of one exact, bound container Pod."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.database.container_startup_stage import (
    BoundPodObserved,
    ReadyObservedAt,
    ScheduledAt,
    StageBudgets,
    StartupAttention,
    Unscheduled,
)
from orchestrator.services.container_provisioner import (
    WorkspaceRuntimeAuthorityError,
    _pod_is_terminal_before_first_ready,
)
from orchestrator.services.container_startup_config import (
    startup_stage_activation_enabled,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests.test_container_provisioner import (
    TestStrictStatelessWorkspaceCreation as Fixture,
    _StrictCreationDB,
)


CREATED = datetime.now(timezone.utc) - timedelta(minutes=8)


def recent_schedule():
    return datetime.now(timezone.utc) - timedelta(seconds=10)


class StageDB(_StrictCreationDB):
    def __init__(self):
        super().__init__()
        self._creation_reservation = {
            "id": "33333333-3333-4333-8333-333333333333",
            "owner_id": Fixture.THREAD_ID,
            "owner_kind": "thread",
            "scope": "workspace_container",
            "operation_kind": "create",
            "phase": "runtime_bound",
            "runtime_incarnation": Fixture.RUNTIME,
            "pod_uid": Fixture.RUNTIME,
            "claim_token": 1,
            "reservation_generation": 1,
            "claimed_by": "container-create:test",
            "settled_at": None,
            "cancel_requested_at": None,
            "startup_protocol_version": None,
            "startup_state": None,
            "scheduled_at": None,
        }
        self.observations = []
        self.allow_observation = True

    async def observe_container_startup(
        self,
        owner_kind,
        owner_id,
        reservation_id,
        claim_token,
        pod_uid,
        observation,
        *,
        budgets=None,
        adopt_if_unmarked=False,
    ):
        if not self.allow_observation:
            return False
        self.observations.append((observation, budgets, adopt_if_unmarked))
        row = self._creation_reservation
        if isinstance(observation, BoundPodObserved):
            row.update(startup_protocol_version=1, startup_state="observing")
        elif isinstance(observation, Unscheduled):
            row.update(startup_state="waiting_capacity")
        elif isinstance(observation, ScheduledAt):
            row.update(startup_state="starting", scheduled_at=observation.scheduled_at)
            if budgets is not None:
                row.update(
                    ready_budget_seconds=budgets.ready_seconds,
                    pull_budget_seconds=budgets.pull_seconds,
                    ssh_budget_seconds=budgets.ssh_seconds,
                )
        elif isinstance(observation, ReadyObservedAt):
            hard = row["scheduled_at"] + timedelta(
                seconds=max(
                    row["ready_budget_seconds"],
                    row.get("pull_budget_seconds") or 0,
                )
            )
            if observation.ready_at > hard:
                row.update(
                    startup_state="attention", startup_reason_code="readiness_deadline"
                )
            else:
                row["startup_first_ready_at"] = observation.ready_at
        elif isinstance(observation, StartupAttention):
            row.update(
                startup_state="attention",
                startup_reason_code=observation.reason_code,
                startup_exit_code=observation.exit_code,
            )
        return True


def pod(*, scheduled=None, ready=None, wrong_annotation=False, owner_fixture=Fixture):
    result = owner_fixture._pod()
    result.metadata.creation_timestamp = CREATED
    result.spec.node_name = "node8" if scheduled else None
    result.status.phase = "Running" if scheduled else "Pending"
    result.status.conditions = (
        [
            SimpleNamespace(
                type="PodScheduled",
                status="True",
                last_transition_time=scheduled,
                reason=None,
            ),
            *(
                [
                    SimpleNamespace(
                        type="Ready", status="True", last_transition_time=ready
                    )
                ]
                if ready
                else []
            ),
        ]
        if scheduled
        else [
            SimpleNamespace(
                type="PodScheduled",
                status="False",
                last_transition_time=CREATED,
                reason="Unschedulable",
            )
        ]
    )
    if wrong_annotation:
        result.metadata.annotations["srw.io/workspace-creation-reservation"] = (
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        )
    return result


def exited_pod(
    owner_fixture=Fixture,
    *,
    exit_code=3,
    reason="Error",
    phase="Failed",
    scheduled=None,
    pod_reason=None,
):
    """A scheduled restartPolicy Never Pod whose workspace container ran and stopped."""

    scheduled = scheduled or recent_schedule()
    result = pod(scheduled=scheduled, owner_fixture=owner_fixture)
    result.spec.restart_policy = "Never"
    result.status.phase = phase
    result.status.reason = pod_reason
    result.status.conditions.append(
        SimpleNamespace(
            type="Ready",
            status="False",
            last_transition_time=scheduled,
            reason="PodCompleted",
        )
    )
    (workspace,) = result.status.container_statuses
    workspace.ready = False
    workspace.restart_count = 0
    workspace.container_id = "containerd://exited-workspace"
    workspace.state = SimpleNamespace(
        waiting=None,
        running=None,
        terminated=SimpleNamespace(exit_code=exit_code, reason=reason),
    )
    return result


def provider(db, observed_pod):
    provisioner = Fixture._provisioner(db)
    provisioner._core_api.read_namespaced_pod.return_value = observed_pod
    provisioner._ssh_auth_ready_timeout = 30
    provisioner._trusted_pod_ssh_identity = AsyncMock(
        return_value=(
            f"k8s-pod:superhuman-remote-worker:{Fixture.RUNTIME}",
            "SHA256:" + "A" * 43,
            Fixture.RUNTIME,
        )
    )
    return provisioner


async def observe(provisioner, db, owner_fixture=Fixture, *, timeout=120):
    owner = owner_fixture._owner()
    return await provisioner._wait_for_ready(
        owner.pod_name,
        timeout=timeout,
        expected_owner=owner,
        expected_runtime_incarnation=Fixture.RUNTIME,
        expected_creation_generation=(
            Fixture.GENERATION if owner.kind == "session" else None
        ),
        expected_network_tier="internet-only",
        expected_pvc_name=None,
        expected_seed_configmap=None,
        startup_reservation=db._creation_reservation.copy(),
    )


def test_gate_is_default_off_and_explicit_opt_in(monkeypatch):
    monkeypatch.delenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", raising=False)
    assert not startup_stage_activation_enabled()
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "garbage")
    assert not startup_stage_activation_enabled()
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    assert startup_stage_activation_enabled()


@pytest.mark.asyncio
async def test_unscheduled_old_pod_adopts_without_spending_budget_then_same_uid_schedules(
    monkeypatch,
):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    db = StageDB()
    p = provider(db, pod())
    assert await observe(p, db) is None
    assert [type(event) for event, _, _ in db.observations] == [
        BoundPodObserved,
        Unscheduled,
    ]
    assert db._creation_reservation["scheduled_at"] is None
    assert db._creation_reservation["startup_state"] == "waiting_capacity"

    scheduled = recent_schedule()
    p._core_api.read_namespaced_pod.return_value = pod(scheduled=scheduled)
    assert await observe(p, db) is None
    assert db._creation_reservation["scheduled_at"] == scheduled
    first_budget = db.observations[-1][1]
    assert first_budget == StageBudgets(120, None, 30)

    p._ssh_auth_ready_timeout = 99
    assert await observe(p, db) is None
    assert db.observations[-1] == (ScheduledAt(scheduled), None, True)
    assert db._creation_reservation["ssh_budget_seconds"] == 30


@pytest.mark.asyncio
async def test_wrong_reservation_annotation_never_adopts(monkeypatch):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    db = StageDB()
    p = provider(db, pod(wrong_annotation=True))
    with pytest.raises(WorkspaceRuntimeAuthorityError):
        await observe(p, db)
    assert db.observations == []


@pytest.mark.asyncio
async def test_gate_off_legacy_does_not_adopt_and_existing_v1_continues(monkeypatch):
    monkeypatch.delenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", raising=False)
    db = StageDB()
    p = provider(db, pod())
    p._core_api.read_namespaced_pod.side_effect = RuntimeError("legacy read")
    assert await observe(p, db, timeout=1) is None
    assert db.observations == []

    db._creation_reservation["startup_protocol_version"] = 1
    p._core_api.read_namespaced_pod.side_effect = None
    assert await observe(p, db) is None
    assert isinstance(db.observations[0][0], Unscheduled)


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", ["true", None], ids=["gate-on", "existing-v1"])
async def test_v1_observer_never_fails_an_exited_container_at_once(monkeypatch, gate):
    """A v1 receipt stays accepted-pending; only the legacy wait may raise."""

    if gate is None:
        monkeypatch.delenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", raising=False)
    else:
        monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", gate)
    db = StageDB()
    if gate is None:
        db._creation_reservation["startup_protocol_version"] = 1
    exited = exited_pod(exit_code=0, reason="Completed", phase="Succeeded")
    # The exact evidence the legacy wait would fail on.
    assert _pod_is_terminal_before_first_ready(exited)
    p = provider(db, exited)

    assert (
        await p._wait_for_ready(
            Fixture._owner().pod_name,
            timeout=120,
            expected_owner=Fixture._owner(),
            expected_runtime_incarnation=Fixture.RUNTIME,
            expected_creation_generation=Fixture.GENERATION,
            expected_network_tier="internet-only",
            expected_pvc_name=None,
            expected_seed_configmap=None,
            startup_reservation=db._creation_reservation.copy(),
            fail_on_exited_container=True,
        )
        is None
    )
    assert db._creation_reservation["startup_state"] == "starting"
    assert not any(
        isinstance(event, StartupAttention) for event, _, _ in db.observations
    )


@pytest.mark.asyncio
async def test_unknown_schedule_evidence_only_adopts_initial_bound_marker(monkeypatch):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    db = StageDB()
    observed = pod(scheduled=recent_schedule())
    observed.status.conditions = []
    p = provider(db, observed)
    assert await observe(p, db) is None
    assert [type(event) for event, _, _ in db.observations] == [BoundPodObserved]
    assert db._creation_reservation["scheduled_at"] is None


@pytest.mark.asyncio
async def test_first_ready_clock_freezes_and_bounds_ssh_on_same_uid(monkeypatch):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    db = StageDB()
    scheduled = recent_schedule()
    ready_at = scheduled + timedelta(seconds=5)
    p = provider(db, pod(scheduled=scheduled, ready=ready_at))
    ssh = AsyncMock(return_value=(True, 1, None))
    monkeypatch.setattr(
        "orchestrator.services.container_provisioner.wait_for_agent_ssh", ssh
    )
    monkeypatch.setattr(
        "orchestrator.services.container_provisioner.resolve_ssh_key_path",
        lambda: "/fake/key",
    )
    monkeypatch.setattr(
        "orchestrator.services.container_provisioner.workspace_private_key_fingerprint",
        lambda _key: "SHA256:trusted",
    )
    assert await observe(p, db) == "10.42.0.8"
    assert db._creation_reservation["startup_first_ready_at"] == ready_at
    assert ssh.await_args.kwargs["deadline_s"] < 30
    assert ssh.await_args.kwargs["expected_host_key_fingerprint"] == (
        "SHA256:" + "A" * 43
    )

    p._core_api.read_namespaced_pod.return_value = pod(
        scheduled=scheduled, ready=ready_at + timedelta(seconds=3)
    )
    assert await observe(p, db) == "10.42.0.8"
    assert db._creation_reservation["startup_first_ready_at"] == ready_at
    assert (
        sum(isinstance(event, ReadyObservedAt) for event, _, _ in db.observations) == 1
    )


@pytest.mark.asyncio
async def test_late_first_ready_and_invalid_image_never_start_ssh(monkeypatch):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    db = StageDB()
    scheduled = datetime.now(timezone.utc) - timedelta(seconds=140)
    ready = scheduled + timedelta(seconds=130)
    p = provider(db, pod(scheduled=scheduled, ready=ready))
    ssh = AsyncMock(return_value=(True, 1, None))
    monkeypatch.setattr(
        "orchestrator.services.container_provisioner.wait_for_agent_ssh", ssh
    )
    assert await observe(p, db) is None
    assert db._creation_reservation["startup_reason_code"] == "readiness_deadline"
    ssh.assert_not_awaited()

    db = StageDB()
    invalid = pod(scheduled=recent_schedule())
    invalid.status.container_statuses[0].ready = False
    invalid.status.container_statuses[0].state.waiting = SimpleNamespace(
        reason="InvalidImageName", message="bad reference"
    )
    p = provider(db, invalid)
    assert (
        await p._wait_for_ready(
            Fixture._owner().pod_name,
            timeout=120,
            expected_owner=Fixture._owner(),
            expected_runtime_incarnation=Fixture.RUNTIME,
            expected_creation_generation=Fixture.GENERATION,
            expected_network_tier="internet-only",
            expected_pvc_name=None,
            expected_seed_configmap=None,
            pull_image="bad/image",
            startup_reservation=db._creation_reservation.copy(),
        )
        is None
    )
    assert db._creation_reservation["startup_reason_code"] == "invalid_image"
    ssh.assert_not_awaited()


@pytest.mark.asyncio
async def test_observation_refusal_holds_before_ssh(monkeypatch):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    db = StageDB()
    db.allow_observation = False
    scheduled = recent_schedule()
    p = provider(db, pod(scheduled=scheduled, ready=scheduled))
    ssh = AsyncMock()
    monkeypatch.setattr(
        "orchestrator.services.container_provisioner.wait_for_agent_ssh", ssh
    )
    assert await observe(p, db) is None
    ssh.assert_not_awaited()


@pytest.mark.asyncio
async def test_authenticated_endpoint_must_still_match_exact_pod(monkeypatch):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    db = StageDB()
    scheduled = recent_schedule()
    first = pod(scheduled=scheduled, ready=scheduled)
    changed = pod(scheduled=scheduled, ready=scheduled)
    changed.status.pod_ip = "10.42.0.99"
    p = provider(db, first)
    p._core_api.read_namespaced_pod.side_effect = [first, changed]
    monkeypatch.setattr(
        "orchestrator.services.container_provisioner.wait_for_agent_ssh",
        AsyncMock(return_value=(True, 1, None)),
    )
    monkeypatch.setattr(
        "orchestrator.services.container_provisioner.resolve_ssh_key_path",
        lambda: "/fake/key",
    )
    monkeypatch.setattr(
        "orchestrator.services.container_provisioner.workspace_private_key_fingerprint",
        lambda _key: "SHA256:trusted",
    )
    assert await observe(p, db) is None


@pytest.mark.asyncio
async def test_conflicting_ready_and_pull_failure_cannot_write_attention(monkeypatch):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    db = StageDB()
    scheduled = recent_schedule()
    conflicting = pod(scheduled=scheduled, ready=scheduled)
    conflicting.status.container_statuses[0].state.waiting = SimpleNamespace(
        reason="InvalidImageName", message="bad reference"
    )
    p = provider(db, conflicting)
    assert (
        await p._wait_for_ready(
            Fixture._owner().pod_name,
            timeout=120,
            expected_owner=Fixture._owner(),
            expected_runtime_incarnation=Fixture.RUNTIME,
            expected_creation_generation=Fixture.GENERATION,
            expected_network_tier="internet-only",
            expected_pvc_name=None,
            expected_seed_configmap=None,
            pull_image="bad/image",
            startup_reservation=db._creation_reservation.copy(),
        )
        is None
    )
    assert db._creation_reservation["startup_state"] == "starting"
    assert not any(
        isinstance(event, StartupAttention) for event, _, _ in db.observations
    )


JOB_ID = "77777777-8888-4999-8aaa-bbbbbbbbbbbb"


class JobFixture(Fixture):
    """The same exact-Pod fixture, owned by a Job."""

    @classmethod
    def _owner(cls):
        return WorkspaceOwner.job(JOB_ID)


class JobStageDB(StageDB):
    def __init__(self):
        super().__init__()
        self._creation_reservation.update(owner_id=JOB_ID, owner_kind="job")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exit_code", "reason", "phase"),
    [(3, "Error", "Failed"), (0, "Completed", "Succeeded")],
    ids=["exit-3", "exit-0"],
)
async def test_job_container_exit_records_container_exited_on_first_tick(
    monkeypatch, exit_code, reason, phase
):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    ssh = AsyncMock(return_value=(True, 1, None))
    monkeypatch.setattr(
        "orchestrator.services.container_provisioner.wait_for_agent_ssh", ssh
    )
    db = JobStageDB()
    p = provider(
        db, exited_pod(JobFixture, exit_code=exit_code, reason=reason, phase=phase)
    )

    assert await observe(p, db, JobFixture) is None

    assert [type(event) for event, _, _ in db.observations] == [
        BoundPodObserved,
        ScheduledAt,
        StartupAttention,
    ]
    assert db.observations[-1][0] == StartupAttention(
        "container_exited", exit_code=exit_code
    )
    assert db._creation_reservation["startup_exit_code"] == exit_code
    ssh.assert_not_awaited()


@pytest.mark.asyncio
async def test_exit_seen_after_the_deadline_still_reports_container_exited(
    monkeypatch,
):
    # An orchestrator that was down past the readiness budget still names the
    # exit, the more specific positive evidence.
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    db = JobStageDB()
    late = datetime.now(timezone.utc) - timedelta(seconds=200)
    p = provider(db, exited_pod(JobFixture, scheduled=late))

    assert await observe(p, db, JobFixture) is None

    assert db.observations[-1][0] == StartupAttention("container_exited", exit_code=3)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reason", "pod_reason"),
    [("OOMKilled", None), ("ContainerStatusUnknown", None), ("Error", "Evicted")],
    ids=["oom-killed", "status-unknown", "evicted"],
)
async def test_cluster_stopped_job_container_keeps_the_deadline_path(
    monkeypatch, reason, pod_reason
):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    stopped = dict(exit_code=137, reason=reason, pod_reason=pod_reason)

    db = JobStageDB()
    p = provider(db, exited_pod(JobFixture, **stopped))
    assert await observe(p, db, JobFixture) is None
    assert db._creation_reservation["startup_state"] == "starting"
    assert not any(
        isinstance(event, StartupAttention) for event, _, _ in db.observations
    )

    db = JobStageDB()
    late = datetime.now(timezone.utc) - timedelta(seconds=200)
    p = provider(db, exited_pod(JobFixture, scheduled=late, **stopped))
    assert await observe(p, db, JobFixture) is None
    assert db.observations[-1][0] == StartupAttention("readiness_deadline")


@pytest.mark.asyncio
async def test_job_receipt_with_first_ready_never_records_container_exited(
    monkeypatch,
):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    db = JobStageDB()
    exited = exited_pod(JobFixture)
    scheduled = exited.status.conditions[0].last_transition_time
    db._creation_reservation.update(
        startup_protocol_version=1,
        startup_state="starting",
        scheduled_at=scheduled,
        startup_first_ready_at=scheduled + timedelta(seconds=1),
        ready_budget_seconds=120,
        pull_budget_seconds=None,
        ssh_budget_seconds=30,
    )
    p = provider(db, exited)

    assert await observe(p, db, JobFixture) is None

    assert not any(
        isinstance(event, StartupAttention) for event, _, _ in db.observations
    )
    assert db._creation_reservation["startup_state"] == "starting"
