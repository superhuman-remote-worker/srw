"""A stuck-completion alarm reaches only the officer who can act on it.

The completion monitor samples the globally oldest unfinalized command on every
tick under one fixed dedup key. Broadcast to every officer with no cool-down,
it woke each of them every ~30 s about another project's job: 99.7% of one
officer's sitreps, ~723k tokens in 15 minutes for a test officer
(officer_visibility_streamline.md). It was still live on main dev on
2026-11-06 for a 69-day-old Better Resavio command, waking a freshly
commissioned officer of an unrelated project 14 times in ten minutes.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.completion_runtime import (
    COMPLETION_ALERT_COOLDOWN_SECONDS,
    CompletionAlertDependencies,
    CompletionAlerts,
)

JOB_ID = "02710eb6-6291-42ab-9561-72797559ee4d"
PROJECT_ID = "a572e4a0-d97a-4103-91fd-92a980d6717d"
OFFICER_ID = "9ada8b43-5abd-404d-9dd8-9e4336d34b75"


def _alert(job_id=JOB_ID):
    return SimpleNamespace(
        dedup_key="completion.command.oldest_unfinalized",
        kind="oldest_unfinalized_command",
        message="oldest unfinished completion command is 5975344.9s old",
        command_id="c-1",
        job_id=job_id,
        command_state="parked",
        age_seconds=5975344.9,
        unit_id=None,
        queue_state=None,
        runnable_at=None,
    )


def _alerts(*, officer=True, project=PROJECT_ID):
    store = MagicMock()
    store.get_job = AsyncMock(return_value={"id": JOB_ID, "project_id": project})
    store.get_officer_thread_for_project = AsyncMock(
        return_value={"id": OFFICER_ID} if officer else None
    )
    store.enqueue_session_wake_event = AsyncMock(return_value=True)
    broadcast = AsyncMock()
    kick = MagicMock()
    deps = CompletionAlertDependencies(
        store=store, notify_all_officers=broadcast, kick_officer_event_drain=kick
    )
    return CompletionAlerts(deps), store, broadcast, kick


@pytest.mark.asyncio
async def test_alarm_goes_only_to_the_jobs_own_officer_with_a_cooldown():
    alerts, store, broadcast, kick = _alerts()

    await alerts.monitor(_alert())

    broadcast.assert_not_awaited()
    store.get_officer_thread_for_project.assert_awaited_once_with(PROJECT_ID)
    args, kwargs = store.enqueue_session_wake_event.await_args
    assert args == (OFFICER_ID,)
    assert kwargs["source"] == "completion_monitor"
    assert kwargs["dedup_key"].endswith(JOB_ID)
    assert kwargs["project_id"] == PROJECT_ID
    assert (
        kwargs["suppress_if_delivered_within_seconds"]
        == COMPLETION_ALERT_COOLDOWN_SECONDS
    )
    kick.assert_called_once()


@pytest.mark.asyncio
async def test_cooldown_fits_inside_the_sent_row_retention():
    from orchestrator.services import session_wake

    assert (
        COMPLETION_ALERT_COOLDOWN_SECONDS < session_wake._OFFICER_SENT_RETENTION_SECONDS
    )


@pytest.mark.asyncio
async def test_project_without_an_officer_wakes_nobody():
    alerts, store, broadcast, kick = _alerts(officer=False)

    await alerts.monitor(_alert())

    broadcast.assert_not_awaited()
    store.enqueue_session_wake_event.assert_not_awaited()
    kick.assert_not_called()


@pytest.mark.asyncio
async def test_alarm_without_a_job_wakes_nobody():
    alerts, store, broadcast, kick = _alerts()

    await alerts.monitor(_alert(job_id=None))

    broadcast.assert_not_awaited()
    store.get_job.assert_not_awaited()
    store.enqueue_session_wake_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_unrouted_alarm_is_logged_once_per_cooldown(caplog):
    alerts, _store, _broadcast, _kick = _alerts(officer=False)

    with caplog.at_level("WARNING"):
        for _ in range(3):
            await alerts.monitor(_alert())

    assert (
        sum("not routed to an officer" in r.getMessage() for r in caplog.records) == 1
    )


# --- a parked completion command (D4: the owner and the project's officer) ---
# knowledge-base/knowledge/issues/workspace_report_after_automatic_pod_cleanup_parks_job_in_processing.md

COMMAND_ID = "03dc2497-cad4-4481-8af0-d94fccf04ec2"
OWNER_ID = "5d0f6a3e-1111-4000-8000-000000000001"


def _park_alert(route="alert_only", attempt=5):
    from orchestrator.services.completion_sweep_router import CompletionParkAlert

    return CompletionParkAlert(
        job_id=JOB_ID,
        command_id=COMMAND_ID,
        command_attempt=attempt,
        route=route,
        message=f"completion sweep routed job={JOB_ID} command={COMMAND_ID} "
        f"attempt={attempt} route={route}",
    )


def _parked_alerts(*, status="processing", parked=True, officer=True, owner=True):
    alerts, store, broadcast, kick = _alerts(officer=officer)
    store.get_job = AsyncMock(
        return_value={
            "id": JOB_ID,
            "project_id": PROJECT_ID,
            "status": status,
            "user_id": OWNER_ID if owner else None,
            "description": "transcribe the lecture",
        }
    )
    store.parked_completion_commands_for_jobs = AsyncMock(
        return_value=(
            {
                JOB_ID: {
                    "command_id": COMMAND_ID,
                    "attempts": 5,
                    "error_code": "HTTPException",
                }
            }
            if parked
            else {}
        )
    )
    notifier = SimpleNamespace(record=AsyncMock(), resolve_source=AsyncMock())
    alerts.dependencies = CompletionAlertDependencies(
        store=store,
        notify_all_officers=broadcast,
        kick_officer_event_drain=kick,
        notifier=notifier,
    )
    return alerts, store, broadcast, kick, notifier


@pytest.mark.asyncio
async def test_parked_command_tells_the_owner_and_the_projects_officer():
    alerts, store, broadcast, kick, notifier = _parked_alerts()

    await alerts.parked(_park_alert())

    broadcast.assert_not_awaited()
    record = notifier.record.await_args.kwargs
    assert record["recipient_id"] == OWNER_ID
    assert record["category"] == "incident"
    assert record["dedup_key"] == f"completion_parked:{COMMAND_ID}"
    assert record["source_kind"] == "completion_command"
    assert record["source_id"] == COMMAND_ID
    assert record["action_params"] == {"job_id": JOB_ID}
    assert JOB_ID[:8] in record["subject"]
    assert "after 5 attempts (`HTTPException`)" in record["body"]
    assert "`processing`" in record["body"]
    args, kwargs = store.enqueue_session_wake_event.await_args
    assert args == (OFFICER_ID,)
    assert kwargs["source"] == "completion_sweep"
    assert kwargs["dedup_key"] == f"completion_sweep:{COMMAND_ID}:5"
    assert kwargs["payload"]["job_id"] == JOB_ID
    assert kwargs["payload"]["kind"] == "completion_parked"
    kick.assert_called_once()


@pytest.mark.asyncio
async def test_a_park_route_whose_command_finished_tells_nobody():
    alerts, store, broadcast, kick, notifier = _parked_alerts(parked=False)

    await alerts.parked(_park_alert(route="park_alert"))

    notifier.record.assert_not_awaited()
    store.enqueue_session_wake_event.assert_not_awaited()
    broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_finished_job_tells_its_officer_but_not_its_owner():
    alerts, store, _broadcast, _kick, notifier = _parked_alerts(status="completed")

    await alerts.parked(_park_alert())

    notifier.record.assert_not_awaited()
    store.enqueue_session_wake_event.assert_awaited_once()


@pytest.mark.asyncio
async def test_without_an_officer_the_owner_is_still_told(caplog):
    alerts, store, broadcast, _kick, notifier = _parked_alerts(officer=False)

    with caplog.at_level("WARNING"):
        await alerts.parked(_park_alert())

    notifier.record.assert_awaited_once()
    store.enqueue_session_wake_event.assert_not_awaited()
    broadcast.assert_not_awaited()
    assert "not routed to an officer" in caplog.text


@pytest.mark.asyncio
async def test_a_failing_feed_never_costs_the_officer_wake():
    alerts, store, _broadcast, _kick, notifier = _parked_alerts()
    notifier.record.side_effect = RuntimeError("feed down")

    await alerts.parked(_park_alert())

    store.enqueue_session_wake_event.assert_awaited_once()


@pytest.mark.asyncio
async def test_settling_a_parked_command_resolves_the_owner_incident():
    alerts, _store, _broadcast, _kick, notifier = _parked_alerts()

    await alerts.parked_settled(COMMAND_ID, "admin-1")

    notifier.resolve_source.assert_awaited_once_with(
        "completion_command", COMMAND_ID, resolved_by="user:admin-1"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state,resolved",
    [("parked", False), ("pending", True), ("done", True), (None, True)],
)
async def test_the_feed_probe_reads_a_parked_command_as_still_waiting(state, resolved):
    """No mail goes out about a command an operator has already moved on."""
    from orchestrator.services.notification_actions import (
        register_notification_actions,
    )
    from orchestrator.services.notification_catalog import source_probe

    register_notification_actions(dependencies=MagicMock())
    probe = source_probe("completion_command")
    db = SimpleNamespace(completion_command_state=AsyncMock(return_value=state))

    assert await probe(db, COMMAND_ID) is resolved
    db.completion_command_state.assert_awaited_once_with(COMMAND_ID)
