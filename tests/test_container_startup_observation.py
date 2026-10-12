"""Pure scheduling evidence and deadlines for an exact container Pod."""

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from kubernetes.client import (
    V1Pod,
    V1PodCondition,
    V1PodSpec,
    V1PodStatus,
    V1ObjectMeta,
    V1Container,
)

from orchestrator.database.container_startup_stage import (
    ScheduledAt,
    StageBudgets,
    Unscheduled,
    Unknown,
    classify_exact_pod_schedule,
    scheduled_stage_deadlines,
)


UID = "3c328df2-4e17-4b95-8a72-874c76822f3f"
OTHER_UID = "f7945d11-1214-436c-9a60-968602795d43"
CREATED = datetime(2026, 9, 28, 10, 0, tzinfo=timezone.utc)


def pod(*, uid=UID, node=None, condition=None, created=CREATED, phase="Pending"):
    return SimpleNamespace(
        metadata=SimpleNamespace(uid=uid, creation_timestamp=created),
        spec=SimpleNamespace(node_name=node),
        status=SimpleNamespace(
            phase=phase,
            conditions=[] if condition is None else [condition],
        ),
    )


def scheduled_condition(status, *, at=None, reason=None):
    return SimpleNamespace(
        type="PodScheduled", status=status, last_transition_time=at, reason=reason
    )


def test_unscheduled_pod_after_180_seconds_can_schedule_same_uid():
    unscheduled = pod(condition=scheduled_condition("False", reason="Unschedulable"))
    assert classify_exact_pod_schedule(
        unscheduled, UID, CREATED + timedelta(minutes=5)
    ) == Unscheduled("scheduler_unschedulable")

    scheduled_at = CREATED + timedelta(minutes=6)
    same_uid = pod(node="node8", condition=scheduled_condition("True", at=scheduled_at))
    assert classify_exact_pod_schedule(
        same_uid, UID, scheduled_at + timedelta(seconds=1)
    ) == ScheduledAt(scheduled_at)


@pytest.mark.parametrize(
    ("node", "status", "reason", "expected"),
    [
        (None, "False", "Unschedulable", Unscheduled("scheduler_unschedulable")),
        (None, "False", "SchedulingGated", Unscheduled("scheduling_other")),
        ("node8", "False", "Unschedulable", Unknown()),
        (None, "True", None, Unknown()),
        (None, "Unknown", None, Unknown()),
    ],
)
def test_node_assignment_and_podscheduled_condition_must_agree(
    node, status, reason, expected
):
    result = classify_exact_pod_schedule(
        pod(
            node=node,
            condition=scheduled_condition(status, at=CREATED, reason=reason),
        ),
        UID,
        CREATED + timedelta(minutes=1),
    )
    assert result == expected


@pytest.mark.parametrize(
    "transition",
    [
        None,
        datetime(2026, 9, 28, 10, 1),
        CREATED - timedelta(microseconds=1),
        CREATED + timedelta(minutes=2),
    ],
)
def test_missing_naive_precreation_or_future_schedule_clock_is_unknown(transition):
    result = classify_exact_pod_schedule(
        pod(node="node8", condition=scheduled_condition("True", at=transition)),
        UID,
        CREATED + timedelta(minutes=1),
    )
    assert result == Unknown()


def test_changed_uid_cannot_supply_schedule_clock():
    result = classify_exact_pod_schedule(
        pod(
            uid=OTHER_UID,
            node="node8",
            condition=scheduled_condition("True", at=CREATED),
        ),
        UID,
        CREATED + timedelta(minutes=1),
    )
    assert result == Unknown()


def test_classifier_accepts_real_kubernetes_client_field_shapes():
    transition = CREATED + timedelta(seconds=30)
    observed = V1Pod(
        metadata=V1ObjectMeta(uid=UID, creation_timestamp=CREATED),
        spec=V1PodSpec(containers=[V1Container(name="workspace")], node_name="node8"),
        status=V1PodStatus(
            phase="Running",
            conditions=[
                V1PodCondition(
                    type="PodScheduled", status="True", last_transition_time=transition
                )
            ],
        ),
    )
    assert classify_exact_pod_schedule(
        observed, UID, transition + timedelta(seconds=1)
    ) == ScheduledAt(transition)


def test_immutable_budgets_begin_at_schedule_not_pod_creation():
    scheduled_at = CREATED + timedelta(minutes=6)
    budgets = StageBudgets(ready_seconds=120, pull_seconds=600, ssh_seconds=30)
    deadlines = scheduled_stage_deadlines(scheduled_at, budgets)

    assert deadlines.readiness_deadline_at == datetime(
        2026, 9, 28, 10, 16, tzinfo=timezone.utc
    )
    assert deadlines.latest_ssh_deadline_at == datetime(
        2026, 9, 28, 10, 16, 30, tzinfo=timezone.utc
    )
    with pytest.raises(FrozenInstanceError):
        budgets.ready_seconds = 180
    assert scheduled_stage_deadlines(scheduled_at, budgets) == deadlines


def test_ordinary_image_uses_readiness_budget_without_pull_extension():
    deadlines = scheduled_stage_deadlines(
        CREATED, StageBudgets(ready_seconds=120, pull_seconds=None, ssh_seconds=30)
    )
    assert deadlines.readiness_deadline_at == CREATED + timedelta(seconds=120)
    assert deadlines.latest_ssh_deadline_at == CREATED + timedelta(seconds=150)


def test_timely_ready_gets_only_bounded_ssh_window():
    deadlines = scheduled_stage_deadlines(
        CREATED, StageBudgets(ready_seconds=120, pull_seconds=600, ssh_seconds=30)
    )
    ready_at = CREATED + timedelta(seconds=590)
    assert deadlines.ssh_deadline_for_ready(
        ready_at, now=CREATED + timedelta(seconds=595)
    ) == CREATED + timedelta(seconds=620)
    assert (
        deadlines.ssh_deadline_for_ready(ready_at, now=CREATED + timedelta(seconds=621))
        is None
    )


@pytest.mark.parametrize(
    "ready_at",
    [
        CREATED - timedelta(microseconds=1),
        CREATED + timedelta(seconds=121),
        datetime(2026, 9, 28, 10, 1),
    ],
)
def test_late_or_invalid_ready_cannot_open_ssh_window(ready_at):
    deadlines = scheduled_stage_deadlines(
        CREATED, StageBudgets(ready_seconds=120, pull_seconds=None, ssh_seconds=30)
    )
    assert (
        deadlines.ssh_deadline_for_ready(ready_at, now=CREATED + timedelta(seconds=122))
        is None
    )


@pytest.mark.parametrize(
    ("ready", "pull", "ssh"),
    [
        (0, None, 30),
        (-1, None, 30),
        (True, None, 30),
        (120, 0, 30),
        (120, float("nan"), 30),
        (120, float("inf"), 30),
        (120, None, float("-inf")),
        (120, None, "30"),
        (10**400, None, 30),
    ],
)
def test_nonpositive_nonfinite_or_non_numeric_budgets_are_rejected(ready, pull, ssh):
    with pytest.raises(ValueError):
        StageBudgets(ready_seconds=ready, pull_seconds=pull, ssh_seconds=ssh)


def test_fractional_ssh_budget_is_supported_but_unaware_schedule_is_rejected():
    budgets = StageBudgets(ready_seconds=120, pull_seconds=None, ssh_seconds=0.5)
    deadlines = scheduled_stage_deadlines(CREATED, budgets)
    assert deadlines.latest_ssh_deadline_at == CREATED + timedelta(seconds=120.5)
    with pytest.raises(ValueError):
        scheduled_stage_deadlines(datetime(2026, 9, 28, 10, 0), budgets)


def test_fall_back_fold_order_is_compared_by_utc_instant():
    berlin = ZoneInfo("Europe/Berlin")
    created = datetime(2026, 10, 25, 2, 50, tzinfo=berlin, fold=0)
    transition = datetime(2026, 10, 25, 2, 55, tzinfo=berlin, fold=0)
    now = datetime(2026, 10, 25, 2, 10, tzinfo=berlin, fold=1)

    result = classify_exact_pod_schedule(
        pod(
            created=created,
            node="node8",
            condition=scheduled_condition("True", at=transition),
        ),
        UID,
        now,
    )
    assert result == ScheduledAt(datetime(2026, 10, 25, 0, 55, tzinfo=timezone.utc))
    assert result.scheduled_at.tzinfo is timezone.utc


def test_fall_back_deadline_and_ssh_grace_follow_elapsed_time():
    berlin = ZoneInfo("Europe/Berlin")
    scheduled = datetime(2026, 10, 25, 2, 55, tzinfo=berlin, fold=0)
    deadlines = scheduled_stage_deadlines(
        scheduled, StageBudgets(ready_seconds=600, pull_seconds=None, ssh_seconds=30)
    )
    assert deadlines.readiness_deadline_at == datetime(
        2026, 10, 25, 1, 5, tzinfo=timezone.utc
    )
    assert deadlines.latest_ssh_deadline_at == datetime(
        2026, 10, 25, 1, 5, 30, tzinfo=timezone.utc
    )
    ready = datetime(2026, 10, 25, 2, 59, 50, tzinfo=berlin, fold=0)
    now = datetime(2026, 10, 25, 2, 0, 5, tzinfo=berlin, fold=1)
    assert deadlines.ssh_deadline_for_ready(ready, now=now) == datetime(
        2026, 10, 25, 1, 0, 20, tzinfo=timezone.utc
    )


def test_spring_forward_deadline_uses_elapsed_time_across_offset_change():
    berlin = ZoneInfo("Europe/Berlin")
    scheduled = datetime(2026, 3, 29, 1, 55, tzinfo=berlin)
    deadlines = scheduled_stage_deadlines(
        scheduled, StageBudgets(ready_seconds=4200, pull_seconds=None, ssh_seconds=30)
    )
    assert deadlines.readiness_deadline_at == datetime(
        2026, 3, 29, 2, 5, tzinfo=timezone.utc
    )
    assert deadlines.latest_ssh_deadline_at == datetime(
        2026, 3, 29, 2, 5, 30, tzinfo=timezone.utc
    )


@pytest.mark.parametrize(
    ("ready", "pull", "ssh"),
    [
        (1e-12, None, 30),
        (120, 1e-12, 30),
        (120, None, 1e-12),
    ],
)
def test_positive_budget_below_one_microsecond_is_rejected(ready, pull, ssh):
    with pytest.raises(ValueError):
        StageBudgets(ready_seconds=ready, pull_seconds=pull, ssh_seconds=ssh)


def test_one_microsecond_budget_remains_positive_in_effect():
    deadlines = scheduled_stage_deadlines(
        CREATED, StageBudgets(ready_seconds=1e-6, pull_seconds=None, ssh_seconds=1e-6)
    )
    assert deadlines.readiness_deadline_at == CREATED + timedelta(microseconds=1)
    assert deadlines.latest_ssh_deadline_at == CREATED + timedelta(microseconds=2)
