"""Pure, fail-closed scheduling evidence for one exact container Pod."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Any, Literal, Mapping, TypeAlias, TypedDict


@dataclass(frozen=True, slots=True)
class Unknown:
    """The observation grants no scheduling clock or terminal conclusion."""


@dataclass(frozen=True, slots=True)
class Unscheduled:
    """This exact Pod has no node and has not entered startup readiness."""

    reason_code: Literal["scheduler_unschedulable", "scheduling_other"]


@dataclass(frozen=True, slots=True)
class ScheduledAt:
    """An exact Pod has an attested Kubernetes scheduling transition."""

    scheduled_at: datetime


@dataclass(frozen=True, slots=True)
class BoundPodObserved:
    """The caller attested the bound Pod identity without a usable stage clock."""


@dataclass(frozen=True, slots=True)
class ReadyObservedAt:
    """The first exact Kubernetes Ready transition for this Pod."""

    ready_at: datetime


@dataclass(frozen=True, slots=True)
class StartupAttention:
    """An attested invalidity or elapsed frozen startup bound."""

    reason_code: str


SchedulingObservation: TypeAlias = Unknown | Unscheduled | ScheduledAt


class WorkspaceCreationView(TypedDict):
    """The complete allowlisted public container creation projection."""

    stage: str
    state: str
    reason_code: str
    readiness_deadline_at: datetime | None


def public_workspace_creation_view(
    row: Mapping[str, Any],
) -> WorkspaceCreationView | None:
    """Project a prefiltered exact receipt without exporting its authority fields."""

    if row["startup_protocol_version"] is None:
        if (
            row["phase"] == "settled"
            and row["result_kind"] == "settled"
            and row["settled_at"] is not None
            and row["workspace_status"] == "creating"
        ):
            reason = "legacy_receipt_held"
        elif (
            row["phase"] == "runtime_bound"
            and row["result_kind"] is None
            and row["settled_at"] is None
            and row["cancel_requested_at"] is None
        ):
            reason = "observation_pending"
        else:
            return None
        return {
            "stage": "scheduling",
            "state": "observing",
            "reason_code": reason,
            "readiness_deadline_at": None,
        }
    if (
        row["startup_protocol_version"] != 1
        or row["phase"] != "runtime_bound"
        or row["result_kind"] is not None
        or row["settled_at"] is not None
        or row["cancel_requested_at"] is not None
    ):
        return None
    stage = row["startup_stage"]
    state = row["startup_state"]
    reason = row["startup_reason_code"]
    scheduled_at = row["scheduled_at"]
    if stage == "scheduling" and scheduled_at is None:
        if (state, reason) not in {
            ("observing", "observation_pending"),
            ("observing", "scheduling_other"),
            ("waiting_capacity", "scheduler_unschedulable"),
            ("waiting_capacity", "insufficient_capacity"),
        }:
            return None
        deadline = None
    elif stage == "readiness" and _aware(scheduled_at):
        if (state, reason) not in {
            ("starting", "scheduled"),
            ("attention", "invalid_image"),
            ("attention", "invalid_configuration"),
            ("attention", "pull_deadline"),
            ("attention", "readiness_deadline"),
            ("attention", "ssh_deadline"),
        }:
            return None
        try:
            budgets = StageBudgets(
                row["ready_budget_seconds"],
                row["pull_budget_seconds"],
                row["ssh_budget_seconds"],
            )
            deadline = scheduled_stage_deadlines(
                scheduled_at, budgets
            ).readiness_deadline_at
        except (TypeError, ValueError, OverflowError):
            return None
    else:
        return None
    return {
        "stage": stage,
        "state": state,
        "reason_code": reason,
        "readiness_deadline_at": deadline,
    }


def _finite_positive_seconds(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("startup budget must be a positive finite number")
    try:
        seconds = float(value)
    except OverflowError as exc:
        raise ValueError("startup budget is too large") from exc
    if (
        not isfinite(seconds)
        or seconds < 1e-6
        or seconds > timedelta.max.total_seconds()
    ):
        raise ValueError("startup budget must be a positive finite number")
    return seconds


@dataclass(frozen=True, slots=True)
class StageBudgets:
    """Effective, immutable startup budgets in seconds."""

    ready_seconds: float
    pull_seconds: float | None
    ssh_seconds: float

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "ready_seconds", _finite_positive_seconds(self.ready_seconds)
        )
        if self.pull_seconds is not None:
            object.__setattr__(
                self, "pull_seconds", _finite_positive_seconds(self.pull_seconds)
            )
        object.__setattr__(
            self, "ssh_seconds", _finite_positive_seconds(self.ssh_seconds)
        )


@dataclass(frozen=True, slots=True)
class StageDeadlines:
    """Frozen boundaries from the attested scheduled transition."""

    scheduled_at: datetime
    readiness_deadline_at: datetime
    latest_ssh_deadline_at: datetime
    ssh_budget_seconds: float

    def ssh_deadline_for_ready(
        self, ready_at: datetime, *, now: datetime
    ) -> datetime | None:
        """No fresh SSH grace for a late/invalid Ready or an expired window."""

        if not all(
            _aware(value)
            for value in (
                ready_at,
                now,
                self.scheduled_at,
                self.readiness_deadline_at,
                self.latest_ssh_deadline_at,
            )
        ):
            return None
        ready_at = ready_at.astimezone(timezone.utc)
        now = now.astimezone(timezone.utc)
        scheduled_at = self.scheduled_at.astimezone(timezone.utc)
        readiness_deadline = self.readiness_deadline_at.astimezone(timezone.utc)
        latest_ssh_deadline = self.latest_ssh_deadline_at.astimezone(timezone.utc)
        if ready_at < scheduled_at or ready_at > readiness_deadline or ready_at > now:
            return None
        deadline = min(
            ready_at + timedelta(seconds=self.ssh_budget_seconds),
            latest_ssh_deadline,
        )
        return deadline if now < deadline else None


def scheduled_stage_deadlines(
    scheduled_at: datetime, budgets: StageBudgets
) -> StageDeadlines:
    """Freeze boot/pull and outer SSH bounds at one trusted schedule clock."""

    if not _aware(scheduled_at) or not isinstance(budgets, StageBudgets):
        raise ValueError("trusted scheduled clock and budgets are required")
    scheduled_at = scheduled_at.astimezone(timezone.utc)
    try:
        readiness_deadline = scheduled_at + timedelta(
            seconds=max(budgets.ready_seconds, budgets.pull_seconds or 0)
        )
        latest_ssh_deadline = readiness_deadline + timedelta(
            seconds=budgets.ssh_seconds
        )
    except OverflowError as exc:
        raise ValueError("startup deadline exceeds datetime range") from exc
    return StageDeadlines(
        scheduled_at=scheduled_at,
        readiness_deadline_at=readiness_deadline,
        latest_ssh_deadline_at=latest_ssh_deadline,
        ssh_budget_seconds=budgets.ssh_seconds,
    )


def _aware(value: Any) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() is not None
    )


def classify_exact_pod_schedule(
    pod: Any, expected_uid: str, now: datetime
) -> SchedulingObservation:
    """Classify only current exact Pod scheduling evidence; never infer a clock."""

    if not _aware(now) or not isinstance(expected_uid, str) or not expected_uid:
        return Unknown()
    metadata = getattr(pod, "metadata", None)
    if (
        getattr(metadata, "uid", None) != expected_uid
        or getattr(metadata, "deletion_timestamp", None) is not None
    ):
        return Unknown()
    created = getattr(metadata, "creation_timestamp", None)
    if not _aware(created):
        return Unknown()
    now_utc = now.astimezone(timezone.utc)
    created_utc = created.astimezone(timezone.utc)
    if created_utc > now_utc:
        return Unknown()

    spec = getattr(pod, "spec", None)
    status = getattr(pod, "status", None)
    node = getattr(spec, "node_name", None)
    if node not in (None, "") and (not isinstance(node, str) or not node.strip()):
        return Unknown()
    conditions = getattr(status, "conditions", None)
    if not isinstance(conditions, (list, tuple)):
        return Unknown()
    scheduled = [
        condition
        for condition in conditions
        if getattr(condition, "type", None) == "PodScheduled"
    ]
    if len(scheduled) != 1:
        return Unknown()
    condition = scheduled[0]
    condition_status = getattr(condition, "status", None)
    if condition_status == "False" and node in (None, ""):
        if getattr(status, "phase", None) != "Pending":
            return Unknown()
        reason = getattr(condition, "reason", None)
        return Unscheduled(
            "scheduler_unschedulable"
            if reason == "Unschedulable"
            else "scheduling_other"
        )
    if condition_status == "True" and node not in (None, ""):
        transition = getattr(condition, "last_transition_time", None)
        if _aware(transition):
            transition_utc = transition.astimezone(timezone.utc)
            if created_utc <= transition_utc <= now_utc:
                return ScheduledAt(transition_utc)
    return Unknown()
