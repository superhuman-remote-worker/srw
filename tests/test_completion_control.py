"""Focused M2 command-aware control admission proofs."""

from tests import _b09_control_seams as control_seams

import dataclasses
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services.completion_control import (
    CompletionControl,
    completion_control_claim_active,
    completion_control_claim_detail,
)
from fastapi import HTTPException
from unittest.mock import MagicMock, patch

import orchestrator.main as main
from orchestrator.routers import job_diff as job_diff_routes
from orchestrator.routers import job_controls as job_control_routes
from orchestrator.routers import job_lifecycle as job_lifecycle_routes
from orchestrator.services import agent_messaging


async def _resume_endpoint(request, job_id, body):
    return await job_control_routes.resume_job(
        request,
        job_id,
        body,
        dependencies=main._job_control_route_dependencies(),
    )


async def _approve_endpoint(request, job_id, body):
    return await job_control_routes.approve_job(
        request,
        job_id,
        body,
        dependencies=main._job_control_route_dependencies(),
    )


async def _agent_release_endpoint(request, job_id, **kwargs):
    return await job_lifecycle_routes.agent_release_job(
        request,
        job_id,
        dependencies=main._job_mutation_route_dependencies(),
        **kwargs,
    )


def _send_agent_message(job_id, body):
    """Drive the extracted send funnel with the application's own collaborators.

    ``main._agent_messaging_dependencies()`` reads ``main.postgres_db``,
    ``main.notification_service`` and ``main.COMPLETION_COMMANDS_ENABLED`` at
    call time, so building it inside the patch scope is what keeps the patches
    below steering the code under test.
    """
    return agent_messaging.send_agent_message(
        MagicMock(),
        job_id,
        body,
        dependencies=main._agent_messaging_dependencies(),
    )


def test_control_marker_expiry_and_malformed_fail_closed():
    live = {"_completion_control_claim": {"version": 1, "expires_epoch": 101.0}}
    expired = {"_completion_control_claim": {"version": 1, "expires_epoch": 99.0}}
    malformed = {"_completion_control_claim": {"version": 1, "expires_epoch": "x"}}

    assert completion_control_claim_active(live, now_epoch=100.0)
    assert not completion_control_claim_active(expired, now_epoch=100.0)
    assert completion_control_claim_active(malformed, now_epoch=100.0)
    assert completion_control_claim_active(
        {"_completion_control_claim": None}, now_epoch=100.0
    )
    assert completion_control_claim_active(
        {"_completion_control_claim": []}, now_epoch=100.0
    )
    assert completion_control_claim_active(
        {"_completion_control_claim": {"version": 99}}, now_epoch=100.0
    )
    assert completion_control_claim_active(
        {
            "_completion_control_claim": {
                "version": 1,
                "expires_epoch": float("nan"),
            }
        },
        now_epoch=100.0,
    )
    assert completion_control_claim_active(
        {
            "_completion_control_claim": {
                "version": 1,
                "expires_epoch": 10**10000,
            }
        },
        now_epoch=100.0,
    )


def test_control_claim_detail_names_source_and_expiry_with_bare_fallback():
    marker = {
        "_completion_control_claim": {
            "version": 1,
            "source": "public_pause",
            "expires_epoch": 4_102_444_800,
        }
    }
    detail = completion_control_claim_detail(marker)
    assert detail == (
        "job control is in progress"
        " (source=public_pause, expires_at=2100-01-01T00:00:00Z)"
    )
    assert completion_control_claim_detail(json.dumps(marker)) == detail

    bare = "job control is in progress"
    assert completion_control_claim_detail(None) == bare
    assert completion_control_claim_detail({"_completion_control_claim": []}) == bare
    assert (
        completion_control_claim_detail({"_completion_control_claim": {"version": 99}})
        == bare
    )
    assert (
        completion_control_claim_detail(
            {
                "_completion_control_claim": {
                    "version": 1,
                    "source": "public_pause",
                    "expires_epoch": 10**10000,
                }
            }
        )
        == bare
    )


class _Conn:
    def __init__(self, route: str | None) -> None:
        self.fetchval = AsyncMock(return_value=uuid4())
        self.fetchrow = AsyncMock(
            return_value=(
                {"command_id": uuid4(), "route": route} if route is not None else None
            )
        )

    @asynccontextmanager
    async def transaction(self):
        yield


class _DB:
    def __init__(self, conn: _Conn) -> None:
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["resume_finalizer", "park_alert", "alert_only"])
async def test_actionable_control_is_blocked_and_durably_nudged(route):
    conn = _Conn(route)
    router = AsyncMock()
    job_id = uuid4()

    decision = await CompletionControl(_DB(conn), router).guard_job(
        job_id, source="public_resume"
    )

    assert decision.blocked
    assert decision.route == route
    router.enqueue_job.assert_awaited_once_with(job_id, source="public_resume")
    assert "FOR UPDATE" in conn.fetchval.await_args.args[0]


@pytest.mark.asyncio
async def test_live_finalizer_blocks_without_allocating_route_action():
    conn = _Conn("stand_down")
    router = AsyncMock()

    decision = await CompletionControl(_DB(conn), router).guard_job(
        uuid4(), source="public_approve"
    )

    assert decision.blocked
    assert decision.route == "stand_down"
    router.enqueue_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_command_allows_control():
    conn = _Conn(None)
    router = AsyncMock()

    decision = await CompletionControl(_DB(conn), router).guard_job(
        uuid4(), source="public_resume"
    )

    assert not decision.blocked
    router.enqueue_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_flag_off_guard_never_builds_completion_service():
    getter = MagicMock()
    with (
        patch.object(main, "COMPLETION_COMMANDS_ENABLED", False),
        patch.object(main._completion_runtime, "control", getter),
    ):
        await main._completion_control_boundary.guard(str(uuid4()), source="test")
    getter.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "auth_name"),
    [
        (_resume_endpoint, "require_internal_or_job_access"),
        (_approve_endpoint, "require_internal_or_job_access"),
        (job_diff_routes.accept_job_diff, "require_job_access"),
        (job_diff_routes.reject_job_diff, "require_job_access"),
    ],
)
async def test_public_control_endpoints_return_exact_409_before_mutation(
    endpoint, auth_name, monkeypatch
):
    monkeypatch.setattr(
        main.VMWorkspaceRecoveryStore,
        "unresolved_participation",
        AsyncMock(return_value=None),
    )
    job_id = str(uuid4())
    job = {"id": job_id, "status": "pending_review", "context": {}}
    guard = AsyncMock(side_effect=HTTPException(409, "completion finalizing"))
    authorized = AsyncMock(return_value=({}, job))
    db = MagicMock()
    db.queue_job_for_resume = AsyncMock()
    db.queue_stateless_job_for_resume = AsyncMock()
    with (
        patch.object(main, auth_name, authorized),
        patch.object(main._completion_control_boundary, "guard", guard),
        patch.object(main, "postgres_db", db),
    ):
        with pytest.raises(HTTPException) as exc:
            if endpoint is _resume_endpoint:
                await endpoint(MagicMock(), job_id, None)
            elif endpoint is _approve_endpoint:
                await endpoint(MagicMock(), job_id, None)
            else:
                # The diff routes carry ``auth_name`` as a field default on
                # their route dependencies, so the same gate is short-circuited
                # there rather than on ``main``.
                await endpoint(
                    MagicMock(),
                    job_id,
                    dependencies=dataclasses.replace(
                        main._job_diff_dependencies(), **{auth_name: authorized}
                    ),
                )

    assert exc.value.status_code == 409
    assert exc.value.detail == "completion finalizing"
    db.queue_job_for_resume.assert_not_awaited()
    db.queue_stateless_job_for_resume.assert_not_awaited()


@pytest.mark.asyncio
async def test_blocking_reply_internal_resume_guard_precedes_queue_mutation():
    job_id = str(uuid4())
    db = MagicMock()
    db.get_job = AsyncMock(
        return_value={
            "id": job_id,
            "status": "waiting_for_reply",
            "execution_lane": "pinned",
        }
    )
    db.queue_job_for_resume = AsyncMock()
    guard = AsyncMock(side_effect=HTTPException(409, "completion finalizing"))
    with (
        patch.object(main, "postgres_db", db),
        patch.object(main._completion_control_boundary, "guard", guard),
    ):
        with pytest.raises(HTTPException) as exc:
            await main._job_control_operations().internal_resume_job(job_id, "reply")
    assert exc.value.detail == "completion finalizing"
    db.queue_job_for_resume.assert_not_awaited()


@pytest.mark.asyncio
async def test_flag_on_pinned_resume_queues_without_agent_selection_or_post(
    monkeypatch,
):
    monkeypatch.setattr(
        main.VMWorkspaceRecoveryStore,
        "unresolved_participation",
        AsyncMock(return_value=None),
    )
    job_id = str(uuid4())
    job = {
        "id": job_id,
        "status": "paused",
        "execution_lane": "pinned",
        "assigned_agent_id": str(uuid4()),
        "context": {},
        "priority": 0,
    }
    db = MagicMock()
    db.queue_job_for_resume = AsyncMock(return_value=True)
    db.get_agent = AsyncMock()
    db.list_agents = AsyncMock()
    with (
        patch.object(main, "COMPLETION_COMMANDS_ENABLED", True),
        patch.object(
            main,
            "require_internal_or_job_access",
            AsyncMock(return_value=({}, job)),
        ),
        patch.object(main._completion_control_boundary, "guard", AsyncMock()),
        patch.object(main, "_user_experts_enabled", AsyncMock(return_value=False)),
        patch.object(
            main.job_workspace_runtime,
            "resume_missing_workspace",
            return_value=None,
        ),
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch", MagicMock()),
    ):
        result = await _resume_endpoint(MagicMock(), job_id, None)

    assert result["status"] == "queued"
    db.queue_job_for_resume.assert_awaited_once()
    db.get_agent.assert_not_awaited()
    db.list_agents.assert_not_awaited()


@pytest.mark.asyncio
async def test_delayed_agent_release_reports_owner_conflict_without_dispatch():
    job_id = str(uuid4())
    old_agent = str(uuid4())
    successor = str(uuid4())
    db = MagicMock()
    db.get_job = AsyncMock(
        return_value={
            "id": job_id,
            "status": "processing",
            "execution_lane": "pinned",
            "assigned_agent_id": successor,
            "context": {},
        }
    )
    db.pause_job = AsyncMock(return_value=False)
    trigger = MagicMock()
    with (
        patch.object(main, "COMPLETION_COMMANDS_ENABLED", True),
        patch.object(main, "require_internal", AsyncMock()),
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch", trigger),
    ):
        with pytest.raises(HTTPException) as exc:
            await _agent_release_endpoint(MagicMock(), job_id, agent_id=old_agent)

    assert exc.value.status_code == 409
    assert exc.value.detail == "Job ownership changed before agent release"
    db.pause_job.assert_awaited_once_with(
        job_id,
        completion_commands_enabled=True,
        expected_agent_id=old_agent,
    )
    trigger.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_leased_agent_release_routes_to_recovery_without_dispatch(enabled):
    job_id = str(uuid4())
    agent_id = str(uuid4())
    db = MagicMock()
    db.get_job = AsyncMock(
        return_value={
            "id": job_id,
            "status": "processing",
            "execution_lane": "pinned",
            "assigned_agent_id": agent_id,
            "lease_expires_at": "2026-08-18T12:00:00+00:00",
            "context": {},
        }
    )
    db.route_pinned_agent_release_to_lease_recovery = AsyncMock(return_value=True)
    db.pause_job = AsyncMock()
    trigger = MagicMock()
    with (
        patch.object(main, "COMPLETION_COMMANDS_ENABLED", enabled),
        patch.object(main, "require_internal", AsyncMock()),
        patch.object(main, "postgres_db", db),
        patch.object(main, "_trigger_dispatch", trigger),
    ):
        result = await _agent_release_endpoint(MagicMock(), job_id, agent_id=agent_id)

    assert result == {"status": "lease_recovery_pending", "job_id": job_id}
    db.route_pinned_agent_release_to_lease_recovery.assert_awaited_once_with(
        job_id,
        completion_commands_enabled=enabled,
        expected_agent_id=agent_id,
    )
    db.pause_job.assert_not_awaited()
    trigger.assert_not_called()


@pytest.mark.asyncio
async def test_blocking_message_loser_has_zero_notification_side_effects():
    job_id = str(uuid4())
    agent_id = str(uuid4())
    user_id = str(uuid4())
    db = MagicMock()
    db.get_job = AsyncMock(
        return_value={
            "id": job_id,
            "status": "processing",
            "execution_lane": "pinned",
            "assigned_agent_id": agent_id,
            "user_id": user_id,
            "description": "test",
            "config_name": "worker_base",
            "context": {},
        }
    )
    db.get_user = AsyncMock(
        return_value={"email": "owner@example.com", "display_name": "Owner"}
    )
    db.check_message_rate_limit = AsyncMock(
        return_value={"job_hourly": 0, "job_daily": 0, "user_daily": 0}
    )
    db.reserve_message_delivery_intent = AsyncMock(
        return_value={
            "allowed": True,
            "intent_id": str(uuid4()),
            "accepted_at": None,
        }
    )
    db.begin_message_delivery_attempt = AsyncMock(
        return_value={"delivery_claimed": True, "attempt_number": 1}
    )
    db.settle_message_delivery_attempt = AsyncMock(return_value=True)
    db.get_message_sequence = AsyncMock(return_value=1)
    db.publish_blocking_message = AsyncMock(return_value=False)
    # OC-01: the blocking send is now one atomic message+route+freeze unit.
    # None is the same "guard lost" signal publish_blocking_message's False
    # was — the point of this test is that the loser touches nothing.
    db.create_routed_blocking_freeze = AsyncMock(return_value=None)
    db.log_message = AsyncMock()
    notifier = MagicMock()
    notifier.record_agent_message = AsyncMock()
    body = main.MessageSendRequest(
        to="user",
        subject="Need input",
        message="Please answer",
        mode="blocking",
        agent_id=agent_id,
    )
    with (
        patch.object(main, "COMPLETION_COMMANDS_ENABLED", True),
        patch.object(main, "postgres_db", db),
        patch.object(main, "notification_service", notifier),
    ):
        with pytest.raises(HTTPException) as exc:
            await _send_agent_message(job_id, body)

    assert exc.value.status_code == 409
    notifier.record_agent_message.assert_not_awaited()
    db.log_message.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("assigned", [None, "agent-offline"])
async def test_flag_off_cascade_pause_preserves_unusable_agent_early_return(assigned):
    child_id = str(uuid4())
    child = {
        "id": child_id,
        "status": "processing",
        "execution_lane": "pinned",
        "assigned_agent_id": assigned,
        "context": {"vm": {"requested": True, "status": "ready"}},
    }
    db = MagicMock()
    db.get_descendant_jobs = AsyncMock(return_value=[child])
    db.get_agent = AsyncMock(
        return_value={"id": assigned, "status": "offline", "pod_ip": None}
    )
    db.pause_job = AsyncMock()
    with (
        patch.object(main, "COMPLETION_COMMANDS_ENABLED", False),
        patch.object(main, "postgres_db", db),
    ):
        await control_seams.cascade_pause_to_children(str(uuid4()))

    db.pause_job.assert_not_awaited()
