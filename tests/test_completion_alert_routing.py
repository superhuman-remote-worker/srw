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
