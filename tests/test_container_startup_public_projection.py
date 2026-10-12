"""Only the documented container startup combinations cross the public wire."""

from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.database.container_startup_stage import public_workspace_creation_view


SCHEDULED = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)


def receipt(**changes):
    return {
        "startup_protocol_version": 1,
        "phase": "runtime_bound",
        "result_kind": None,
        "settled_at": None,
        "cancel_requested_at": None,
        "workspace_status": "creating",
        "startup_stage": "scheduling",
        "startup_state": "observing",
        "startup_reason_code": "observation_pending",
        "scheduled_at": None,
        "ready_budget_seconds": None,
        "pull_budget_seconds": None,
        "ssh_budget_seconds": None,
        "secret_claim_token": "must-never-leave",
        **changes,
    }


@pytest.mark.parametrize(
    "state,reason",
    [
        ("observing", "observation_pending"),
        ("observing", "scheduling_other"),
        ("waiting_capacity", "scheduler_unschedulable"),
        ("waiting_capacity", "insufficient_capacity"),
    ],
)
def test_unscheduled_public_shape_has_no_deadline_or_authority(state, reason):
    view = public_workspace_creation_view(
        receipt(startup_state=state, startup_reason_code=reason)
    )
    assert view == {
        "stage": "scheduling",
        "state": state,
        "reason_code": reason,
        "readiness_deadline_at": None,
    }


@pytest.mark.parametrize(
    "state,reason",
    [
        ("starting", "scheduled"),
        ("attention", "invalid_image"),
        ("attention", "invalid_configuration"),
        ("attention", "pull_deadline"),
        ("attention", "readiness_deadline"),
        ("attention", "ssh_deadline"),
    ],
)
def test_scheduled_public_hard_deadline_does_not_include_ssh(state, reason):
    view = public_workspace_creation_view(
        receipt(
            startup_stage="readiness",
            startup_state=state,
            startup_reason_code=reason,
            scheduled_at=SCHEDULED,
            ready_budget_seconds=180,
            pull_budget_seconds=300,
            ssh_budget_seconds=40,
        )
    )
    assert view == {
        "stage": "readiness",
        "state": state,
        "reason_code": reason,
        "readiness_deadline_at": SCHEDULED + timedelta(seconds=300),
    }


@pytest.mark.parametrize(
    "change",
    [
        {"startup_reason_code": "raw scheduler text"},
        {
            "startup_state": "attention",
            "startup_reason_code": "scheduler_unschedulable",
        },
        {"startup_stage": "readiness", "scheduled_at": None},
        {"phase": "settled"},
        {"cancel_requested_at": SCHEDULED},
    ],
)
def test_malformed_or_inactive_v1_receipt_is_not_projected(change):
    assert public_workspace_creation_view(receipt(**change)) is None
