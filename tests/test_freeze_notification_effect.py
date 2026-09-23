"""Freeze events → feed rows (unified notification system, slice 1).

``_notify_operator_freeze`` used to fan out an email; now it records one row
per freeze on the job owner's feed. What these pin:

* the freeze type → category / source mapping the cockpit relies on;
* the idempotency key: inside a completion effect it is the command id, so a
  journal replay lands on the same row (D10); on the runner-less legacy route
  every call gets a fresh key because nothing replays there and a job can
  legitimately freeze the same way twice.
"""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests import b08_completion_helpers as b08_helpers

import orchestrator.main
from orchestrator.services.notification_service import RecordResult

JOB_ID = str(uuid.uuid4())
USER_ID = str(uuid.uuid4())
SUDO_ID = str(uuid.uuid4())


@pytest.fixture
def record(monkeypatch):
    mock = AsyncMock(return_value=RecordResult("n-1", True, {"in_app": True}))
    monkeypatch.setattr(orchestrator.main.notification_service, "record", mock)
    return mock


def _job(**overrides):
    job = {
        "id": JOB_ID,
        "user_id": USER_ID,
        "config_name": "worker_base",
        "description": "Publish the reachable Reception-Cockpit demo",
    }
    job.update(overrides)
    return job


class TestNotifyOperatorFreeze:
    @pytest.mark.asyncio
    async def test_job_complete_is_a_review_queue_item(self, record):
        result = await b08_helpers.notify_operator_freeze(
            _job(),
            JOB_ID,
            "job_complete",
            {"summary": "done", "confidence": 0.9, "phase_number": 3},
            dedup_key="freeze_notification:cmd-1",
        )
        assert result.inserted is True
        kwargs = record.call_args.kwargs
        assert kwargs["recipient_id"] == USER_ID
        assert kwargs["category"] == "review_queue"
        assert kwargs["dedup_key"] == "freeze_notification:cmd-1"
        assert (kwargs["source_kind"], kwargs["source_id"]) == ("job", JOB_ID)
        assert kwargs["action_params"] == {"job_id": JOB_ID}
        assert kwargs["payload"]["freeze_type"] == "job_complete"
        assert kwargs["payload"]["phase_number"] == 3
        assert "awaiting review" in kwargs["body"]

    @pytest.mark.asyncio
    async def test_a_held_seal_says_why_before_anyone_reads_the_branch(self, record):
        """worker_git_versioning_stops_midjob_seal_pins_stale_revision.md: a
        seal held because delivery could not be proven is still a review item,
        but the reviewer must learn the branch may be stale first."""
        await b08_helpers.notify_operator_freeze(
            _job(),
            JOB_ID,
            "job_complete",
            {
                "summary": "three reports",
                "confidence": 0.9,
                "delivery_hold": (
                    "At job completion, the final commit did not land, so "
                    "deliverable path(s) output/obstacles.md are not in the "
                    "pushed revision."
                ),
            },
            dedup_key="freeze_notification:cmd-hold",
        )
        kwargs = record.call_args.kwargs
        assert kwargs["category"] == "review_queue"
        assert "delivery unproven" in kwargs["subject"]
        assert "output/obstacles.md are not in the pushed revision" in kwargs["body"]
        assert kwargs["payload"]["delivery_hold"].startswith("At job completion")

    @pytest.mark.asyncio
    async def test_the_agents_own_delivery_error_is_named_too(self, record):
        await b08_helpers.notify_operator_freeze(
            _job(),
            JOB_ID,
            "job_complete",
            {
                "summary": "done",
                "delivery_failed": True,
                "delivery_error": "The job-ending git push failed at job freeze "
                "(retried once).",
            },
            dedup_key="freeze_notification:cmd-df",
        )
        kwargs = record.call_args.kwargs
        assert "delivery unproven" in kwargs["subject"]
        assert "(retried once)" in kwargs["body"]

    @pytest.mark.asyncio
    async def test_vm_upgrade_points_at_the_sudo_request(self, record):
        await b08_helpers.notify_operator_freeze(
            _job(),
            JOB_ID,
            "vm_upgrade_required",
            {"command": "sudo apt install x"},
            sudo_request_id=SUDO_ID,
            dedup_key="freeze_notification:cmd-2",
        )
        kwargs = record.call_args.kwargs
        assert kwargs["category"] == "vm_upgrade"
        assert (kwargs["source_kind"], kwargs["source_id"]) == ("sudo_request", SUDO_ID)
        assert kwargs["action_params"] == {"job_id": JOB_ID, "request_id": SUDO_ID}

    @pytest.mark.asyncio
    async def test_vm_upgrade_without_request_falls_back_to_the_job(self, record):
        await b08_helpers.notify_operator_freeze(
            _job(), JOB_ID, "vm_upgrade_required", {}, dedup_key="k"
        )
        kwargs = record.call_args.kwargs
        assert (kwargs["source_kind"], kwargs["source_id"]) == ("job", JOB_ID)

    @pytest.mark.asyncio
    async def test_llm_unavailable_and_unknown_types_are_incidents(self, record):
        await b08_helpers.notify_operator_freeze(
            _job(), JOB_ID, "llm_unavailable", {"model": "m"}, dedup_key="a"
        )
        assert record.call_args.kwargs["category"] == "incident"
        await b08_helpers.notify_operator_freeze(
            _job(), JOB_ID, "something_new", {}, dedup_key="b"
        )
        assert record.call_args.kwargs["category"] == "incident"

    @pytest.mark.asyncio
    async def test_budget_exceeded(self, record):
        await b08_helpers.notify_operator_freeze(
            _job(), JOB_ID, "budget_exceeded", {"phase_number": 2}, dedup_key="c"
        )
        assert record.call_args.kwargs["category"] == "budget_exceeded"

    @pytest.mark.asyncio
    async def test_no_owner_records_nothing(self, record):
        result = await b08_helpers.notify_operator_freeze(
            _job(user_id=None), JOB_ID, "job_complete", {}, dedup_key="d"
        )
        assert result is None
        record.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dedup_key_is_required(self, record):
        with pytest.raises(TypeError):
            await b08_helpers.notify_operator_freeze(_job(), JOB_ID, "job_complete", {})


class TestCompletionEffectDedupKey:
    def test_journalled_effect_uses_the_command_id(self):
        runner = SimpleNamespace(command_id="cmd-42")
        key = (
            orchestrator.main.completion_effect_operations.completion_effect_dedup_key(
                runner, "freeze_notification", JOB_ID
            )
        )
        assert key == "freeze_notification:cmd-42"
        # Stable across retries and restarts of the same command.
        assert (
            key
            == orchestrator.main.completion_effect_operations.completion_effect_dedup_key(
                runner, "freeze_notification", JOB_ID
            )
        )

    def test_runner_less_route_gets_a_fresh_key_each_time(self):
        a = orchestrator.main.completion_effect_operations.completion_effect_dedup_key(
            None, "freeze_notification", JOB_ID
        )
        b = orchestrator.main.completion_effect_operations.completion_effect_dedup_key(
            None, "freeze_notification", JOB_ID
        )
        assert a != b
        assert a.startswith(f"freeze_notification:{JOB_ID}:")

    def test_effect_names_are_still_the_versioned_vocabulary(self):
        # The journal vocabulary is a resumability contract; slice 1 must not
        # have renamed the notification effects (completion_effect_policy.py).
        from orchestrator.services.completion_effect_policy import (
            COMPLETION_EFFECT_INDEX,
        )

        for pair in (
            ("freeze_notification", "freeze_notification"),
            ("llm_give_up_operator_alert", "llm_give_up_alert"),
            ("drain_stall_operator_alert", "drain_stall_notification"),
        ):
            assert pair in COMPLETION_EFFECT_INDEX
