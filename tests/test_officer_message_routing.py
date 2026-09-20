"""Officer-aware worker message routing (officer_message_routing.md M1–M3).

Covers the M1 effective-policy resolver matrix, the M2 transactional
blocking-send branching (officer transaction, guard-loss 409, §5.1 immediate
fallbacks, user_direct byte-compat), the hold/decommission drains, and the M3
officer action guard matrix + delivery flows. The M4 reconciler has its own
file (tests/test_message_route_reconciler.py); CAS races against a real
Postgres live in tests/test_officer_message_routing_real_postgres.py.
"""

from __future__ import annotations

import dataclasses
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import orchestrator.main as main
from orchestrator.routers import messaging as messaging_routes
from orchestrator.services import agent_messaging
from orchestrator.services import inbound_reply as inbound_reply_svc
from orchestrator.services import message_routing as routing
from orchestrator.services import officer_message_actions as officer_actions
from orchestrator.services import officer_post_lifecycle as officer_lifecycle
from orchestrator.services.notification_service import RecordResult
from shared.runtime_actor import RuntimeActorContext


OFFICER_TID = str(uuid4())
PROJECT_ID = str(uuid4())


def _post(policy=None, thread_id=OFFICER_TID, incarnations=None):
    return {
        "project_id": PROJECT_ID,
        "thread_id": thread_id,
        "communication_policy": policy
        or {"worker_messages": "user_direct", "officer_response_minutes": 15},
        "incarnations": incarnations if incarnations is not None else [],
        "state": {},
        "config_override": {},
    }


def _officer_thread(held=False):
    metadata = {"config_override": {"officer": {"enabled": True}}}
    if held:
        metadata["config_override"]["officer"]["hold"] = {
            "kind": "maintenance",
            "since": "2026-08-14T00:00:00+00:00",
        }
    return {"id": OFFICER_TID, "status": "active", "metadata": metadata}


def _job(**overrides):
    job = {
        "id": str(uuid4()),
        "status": "processing",
        "execution_lane": "pinned",
        "assigned_agent_id": str(uuid4()),
        "user_id": str(uuid4()),
        "description": "test job",
        "config_name": "worker_base",
        "context": {},
        "project_id": PROJECT_ID,
    }
    job.update(overrides)
    return job


# =============================================================================
# M1 — effective-policy resolver matrix
# =============================================================================


class TestResolveEffectivePolicy:
    @pytest.mark.asyncio
    async def test_explicit_recipient_stays_direct_without_db_lookups(self):
        db = MagicMock()
        result = await routing.resolve_effective_policy(db, _job(), to="Alice Example")
        assert result["applied"] == "user_direct"
        assert result["reason"] == "explicit_recipient"
        db.get_project_officer.assert_not_called()

    @pytest.mark.asyncio
    async def test_project_less_job_stays_direct(self):
        db = MagicMock()
        result = await routing.resolve_effective_policy(db, _job(project_id=None))
        assert result["applied"] == "user_direct"
        assert result["reason"] == "no_project"
        db.get_project_officer.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_post_row_stays_direct(self):
        db = MagicMock()
        db.get_project_officer = AsyncMock(return_value=None)
        result = await routing.resolve_effective_policy(db, _job())
        assert result["applied"] == "user_direct"
        assert result["reason"] == "no_post"

    @pytest.mark.asyncio
    async def test_row_policy_user_direct_short_circuits(self):
        db = MagicMock()
        db.get_project_officer = AsyncMock(
            return_value=_post(
                {"worker_messages": "user_direct", "officer_response_minutes": 30}
            )
        )
        db.get_officer_thread_for_project = AsyncMock()
        result = await routing.resolve_effective_policy(db, _job())
        assert result["applied"] == "user_direct"
        assert result["reason"] == "policy_user_direct"
        assert result["officer_response_minutes"] == 30
        # No commissioned-officer lookup is needed for a direct policy.
        db.get_officer_thread_for_project.assert_not_called()

    @pytest.mark.asyncio
    async def test_vacant_post_collapses_officer_first_to_direct(self):
        db = MagicMock()
        db.get_project_officer = AsyncMock(
            return_value=_post(
                {"worker_messages": "officer_first", "officer_response_minutes": 15},
                thread_id=None,
            )
        )
        db.get_officer_thread_for_project = AsyncMock(return_value=None)
        result = await routing.resolve_effective_policy(db, _job())
        assert result["applied"] == "user_direct"
        assert result["requested"] == "officer_first"
        assert result["reason"] == "vacant"

    @pytest.mark.asyncio
    async def test_commissioned_officer_first_resolves_with_incarnation(self):
        db = MagicMock()
        db.get_project_officer = AsyncMock(
            return_value=_post(
                {"worker_messages": "officer_first", "officer_response_minutes": 7},
                incarnations=[{"thread_id": "old"}, {"thread_id": "older"}],
            )
        )
        db.get_officer_thread_for_project = AsyncMock(return_value=_officer_thread())
        result = await routing.resolve_effective_policy(db, _job())
        assert result["applied"] == "officer_first"
        assert result["reason"] == "commissioned"
        assert result["officer_thread_id"] == OFFICER_TID
        assert result["officer_incarnation"] == 2
        assert result["officer_response_minutes"] == 7
        assert result["officer_held"] is False

    @pytest.mark.asyncio
    async def test_held_officer_is_reported_not_collapsed(self):
        """The hold rule is mode-dependent, so the resolver only REPORTS it."""
        db = MagicMock()
        db.get_project_officer = AsyncMock(
            return_value=_post({"worker_messages": "officer_first"})
        )
        db.get_officer_thread_for_project = AsyncMock(
            return_value=_officer_thread(held=True)
        )
        result = await routing.resolve_effective_policy(db, _job())
        assert result["applied"] == "officer_first"
        assert result["officer_held"] is True

    @pytest.mark.asyncio
    async def test_resolver_failure_degrades_to_direct(self):
        db = MagicMock()
        db.get_project_officer = AsyncMock(side_effect=RuntimeError("db down"))
        result = await routing.resolve_effective_policy(db, _job())
        assert result["applied"] == "user_direct"
        assert result["reason"] == "no_post"

    @pytest.mark.asyncio
    async def test_minutes_are_clamped_to_patch_bounds(self):
        db = MagicMock()
        db.get_project_officer = AsyncMock(
            return_value=_post(
                {"worker_messages": "officer_first", "officer_response_minutes": 9999}
            )
        )
        db.get_officer_thread_for_project = AsyncMock(return_value=_officer_thread())
        result = await routing.resolve_effective_policy(db, _job())
        assert result["officer_response_minutes"] == 120
        db.get_project_officer = AsyncMock(
            return_value=_post(
                {"worker_messages": "officer_first", "officer_response_minutes": "x"}
            )
        )
        result = await routing.resolve_effective_policy(db, _job())
        assert result["officer_response_minutes"] == 15

    def test_snapshot_freezes_the_resolution(self):
        """M1: the snapshot is what the route keeps — a later project-setting
        change never retargets a waiting question because nothing re-resolves."""
        resolution = {
            "requested": "officer_first",
            "reason": "commissioned",
            "officer_response_minutes": 20,
        }
        snapshot = routing.snapshot_for_route(
            resolution, applied="officer_first", purpose="blocker"
        )
        assert snapshot["worker_messages"] == "officer_first"
        assert snapshot["applied"] == "officer_first"
        assert snapshot["officer_response_minutes"] == 20
        assert snapshot["purpose"] == "blocker"
        # Mutating the source afterwards does not touch the snapshot.
        resolution["officer_response_minutes"] = 5
        assert snapshot["officer_response_minutes"] == 20


class TestDeadlinesAndTimeout:
    def test_blocking_timeout_prefers_resolved_config(self):
        job = _job(
            resolved_config={"agent": {"communication": {"blocking_timeout_hours": 2}}},
            config_override={"communication": {"blocking_timeout_hours": 9}},
        )
        assert routing.blocking_timeout_hours(job) == 2.0

    def test_blocking_timeout_falls_back_to_override_then_default(self):
        assert (
            routing.blocking_timeout_hours(
                _job(config_override={"communication": {"blocking_timeout_hours": 9}})
            )
            == 9.0
        )
        assert routing.blocking_timeout_hours(_job()) == 24.0
        assert (
            routing.blocking_timeout_hours(
                _job(
                    resolved_config={
                        "agent": {"communication": {"blocking_timeout_hours": 0}}
                    }
                )
            )
            == 24.0
        )

    def test_route_deadlines_only_blocking_pending_officer_gets_sla(self):
        now = datetime(2026, 8, 14, 12, 0, tzinfo=timezone.utc)
        officer = routing.route_deadlines(
            blocking=True,
            state="pending_officer",
            officer_response_minutes=15,
            timeout_hours=24,
            now=now,
        )
        assert officer["officer_deadline"] == now + timedelta(minutes=15)
        assert officer["total_deadline"] == now + timedelta(hours=24)
        both = routing.route_deadlines(
            blocking=True,
            state="pending_both",
            officer_response_minutes=15,
            timeout_hours=24,
            now=now,
        )
        assert both["officer_deadline"] is None
        assert both["total_deadline"] == now + timedelta(hours=24)
        async_route = routing.route_deadlines(
            blocking=False,
            state="pending_officer",
            officer_response_minutes=15,
            timeout_hours=24,
            now=now,
        )
        assert async_route["officer_deadline"] is None
        assert async_route["total_deadline"] is None


# =============================================================================
# M2 — the send path
# =============================================================================


def _send_db(job, policy, *, officer=True, held=False):
    """A MagicMock DB wired for one send_agent_message call."""
    db = MagicMock()
    db.get_job = AsyncMock(return_value=job)
    db.get_user = AsyncMock(
        return_value={"email": "owner@example.com", "display_name": "Owner"}
    )
    db.check_message_rate_limit = AsyncMock(
        return_value={"job_hourly": 0, "job_daily": 0, "user_daily": 0}
    )
    db.reserve_message_delivery_intent = AsyncMock(
        side_effect=lambda **_kwargs: {
            "allowed": True,
            "idempotent": False,
            "intent_id": str(uuid4()),
            "accepted_at": None,
        }
    )
    db.begin_message_delivery_attempt = AsyncMock(
        return_value={
            "delivery_claimed": True,
            "accepted": False,
            "attempt_number": 1,
        }
    )
    db.settle_message_delivery_attempt = AsyncMock(return_value=True)
    db.get_message_route = AsyncMock(return_value=None)
    db.get_message_sequence = AsyncMock(return_value=1)
    db.get_project_officer = AsyncMock(return_value=_post(policy))
    db.get_officer_thread_for_project = AsyncMock(
        return_value=_officer_thread(held=held) if officer else None
    )
    db.create_routed_blocking_freeze = AsyncMock(
        return_value={
            "route_id": str(uuid4()),
            "originating_message_id": str(uuid4()),
        }
    )
    db.create_message_route = AsyncMock(return_value=str(uuid4()))
    db.mark_route_user_delivery = AsyncMock(return_value=True)
    db.set_message_email_id = AsyncMock(return_value=True)
    db.settle_outbound_message_log = AsyncMock(return_value=True)
    db.publish_blocking_message = AsyncMock(return_value=True)
    db.log_message = AsyncMock(
        return_value={"id": str(uuid4()), "thread_id": "t", "status": "sent"}
    )
    return db


def _notifier(email=True):
    """The notification system's agent-message seam. The durable feed row is
    itself an accepted delivery (``in_app``); an immediate email rides along
    with its Message-ID when the row's severity mails now."""
    notifier = MagicMock()
    deliveries = {"in_app": True, "email": email}
    if email:
        deliveries["email_message_id"] = "<m1@x>"
    notifier.record_agent_message = AsyncMock(
        return_value=RecordResult("n-1", True, deliveries)
    )
    return notifier


def _body(**overrides):
    values = {
        "to": "user",
        "subject": "Need input",
        "message": "Please answer",
        "mode": "blocking",
    }
    values.update(overrides)
    return main.MessageSendRequest(**values)


@contextmanager
def _send_patches(db, notifier, *, flag=True):
    """The application globals ``main._agent_messaging_dependencies()`` reads.

    The internal-key gate is no longer part of the send operation — the route
    declaration in ``orchestrator.routers.messaging`` calls it — so it is
    proved once, at the route, by ``TestRouteWiring`` below instead of being
    stubbed here where nothing would call it.
    """
    with (
        patch.object(main, "COMPLETION_COMMANDS_ENABLED", flag),
        patch.object(main, "postgres_db", db),
        patch.object(main, "notification_service", notifier),
        patch.object(main, "_kick_officer_event_drain", MagicMock()),
    ):
        yield


async def _send(job_id, body):
    """Drive the extracted send funnel with the application's collaborators.

    Call inside ``_send_patches``: the factory resolves ``postgres_db``,
    ``notification_service`` and ``COMPLETION_COMMANDS_ENABLED`` at call time,
    which is what keeps those patches steering the code under test.
    """
    return await agent_messaging.send_agent_message(
        MagicMock(), job_id, body, dependencies=main._agent_messaging_dependencies()
    )


def test_message_quota_policy_defaults_and_overrides(monkeypatch):
    for name in (
        "MESSAGE_HUMAN_JOB_HOURLY_LIMIT",
        "MESSAGE_HUMAN_JOB_DAILY_LIMIT",
        "MESSAGE_HUMAN_USER_DAILY_LIMIT",
        "OFFICER_MESSAGE_JOB_HOURLY_LIMIT",
        "OFFICER_MESSAGE_PROJECT_DAILY_LIMIT",
    ):
        monkeypatch.delenv(name, raising=False)
    policy = routing.message_quota_policy()
    assert (
        policy.human_job_hourly,
        policy.human_job_daily,
        policy.human_user_daily,
    ) == (5, 15, 30)
    assert (policy.internal_job_hourly, policy.internal_project_daily) == (30, 300)

    monkeypatch.setenv("OFFICER_MESSAGE_JOB_HOURLY_LIMIT", "17")
    monkeypatch.setenv("OFFICER_MESSAGE_PROJECT_DAILY_LIMIT", "91")
    overridden = routing.message_quota_policy()
    assert (overridden.internal_job_hourly, overridden.internal_project_daily) == (
        17,
        91,
    )


class TestOfficerFirstBlockingSend:
    @pytest.mark.asyncio
    async def test_one_transaction_and_no_user_notification(self):
        job = _job(execution_lane="pinned")
        db = _send_db(
            job,
            {"worker_messages": "officer_first", "officer_response_minutes": 10},
        )
        notifier = _notifier()
        with _send_patches(db, notifier):
            result = await _send(job["id"], _body(agent_id=job["assigned_agent_id"]))

        assert result["status"] == "sent"
        assert result["recipient"] == "project officer"
        assert result["routing"]["applied"] == "officer_first"
        assert result["routing"]["state"] == "pending_officer"

        # Acceptance 1: no user notification for blocking officer_first.
        notifier.record_agent_message.assert_not_awaited()
        # The unit is the transaction — no separate log_message call.
        db.log_message.assert_not_awaited()
        db.publish_blocking_message.assert_not_awaited()

        kwargs = db.create_routed_blocking_freeze.await_args.kwargs
        args = db.create_routed_blocking_freeze.await_args.args
        assert args[0] == job["id"]
        freeze = args[1]
        route = kwargs["route"]
        wake = kwargs["wake"]
        assert route["state"] == "pending_officer"
        assert route["officer_thread_id"] == OFFICER_TID
        assert route["policy_snapshot"]["worker_messages"] == "officer_first"
        assert route["officer_deadline"] is not None
        assert route["total_deadline"] is not None
        # Freeze carries the route generation and the matching thread.
        assert freeze["route_id"] == route["route_id"]
        assert freeze["thread_id"] == route["thread_id"]
        assert freeze["freeze_type"] == "blocking_message"
        # Durable wake intent, high-urgency source, route-scoped dedup.
        assert wake["source"] == "worker_message"
        assert wake["dedup_key"] == f"route:{route['route_id']}"
        assert wake["thread_id"] == OFFICER_TID
        assert kwargs["message_entry"]["recipient_email"] is None
        quota = db.reserve_message_delivery_intent.await_args.kwargs
        assert quota["bucket"] == "officer_internal"
        assert quota["effective_audience"] == "officer"

    @pytest.mark.asyncio
    async def test_guard_loss_is_409_with_zero_side_effects(self):
        """Failure injection: the unit did not commit → the job stays
        runnable, nothing was notified, and NO fallback may freeze it."""
        job = _job()
        db = _send_db(job, {"worker_messages": "officer_first"})
        db.create_routed_blocking_freeze = AsyncMock(return_value=None)
        notifier = _notifier()
        with _send_patches(db, notifier):
            with pytest.raises(HTTPException) as exc:
                await _send(job["id"], _body(agent_id=job["assigned_agent_id"]))
        assert exc.value.status_code == 409
        notifier.record_agent_message.assert_not_awaited()
        db.log_message.assert_not_awaited()
        db.publish_blocking_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_infra_failure_falls_back_to_user_immediately(self):
        """§5.1: an officer-leg failure (e.g. the wake insert) still reaches
        the user — now through the SAME atomic unit, minus the wake.

        The old fallback wrote a freeze with best-effort route bookkeeping,
        which is precisely the OC-01 defect. The retry is safe because the
        thing that failed is the officer half: with wake=None it commits."""
        job = _job()
        db = _send_db(job, {"worker_messages": "officer_first"})

        async def _fail_only_the_officer_leg(*args, **kwargs):
            if kwargs.get("wake") is not None:
                raise RuntimeError("wake insert failed")
            return {"route_id": str(uuid4()), "originating_message_id": str(uuid4())}

        db.create_routed_blocking_freeze = AsyncMock(
            side_effect=_fail_only_the_officer_leg
        )
        notifier = _notifier()
        with _send_patches(db, notifier):
            result = await _send(job["id"], _body(agent_id=job["assigned_agent_id"]))
        assert result["status"] == "sent"
        assert result["routing"]["applied"] == "user_direct"
        assert result["routing"]["reason"] == "officer_route_failed"
        notifier.record_agent_message.assert_awaited_once()
        # The direct retry is the same transactional unit without the wake,
        # which also logs the message — hence no separate log_message call.
        assert db.create_routed_blocking_freeze.await_args.kwargs["wake"] is None
        db.log_message.assert_not_awaited()
        assert [
            call.kwargs["bucket"]
            for call in db.reserve_message_delivery_intent.await_args_list
        ] == ["officer_internal", "human"]

    @pytest.mark.asyncio
    async def test_held_officer_routes_blocking_to_user_immediately(self):
        job = _job()
        db = _send_db(job, {"worker_messages": "officer_first"}, held=True)
        notifier = _notifier()
        with _send_patches(db, notifier):
            result = await _send(job["id"], _body(agent_id=job["assigned_agent_id"]))
        assert result["routing"]["applied"] == "user_direct"
        assert result["routing"]["reason"] == "officer_held"
        # OC-01: the direct path is atomic too now, so the helper IS called —
        # what marks it as direct is the absence of an officer wake.
        kwargs = db.create_routed_blocking_freeze.await_args.kwargs
        assert kwargs["wake"] is None
        assert kwargs["route"]["state"] == "user_direct"
        notifier.record_agent_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_vacant_post_routes_blocking_to_user_immediately(self):
        job = _job()
        db = _send_db(job, {"worker_messages": "officer_first"}, officer=False)
        notifier = _notifier()
        with _send_patches(db, notifier):
            result = await _send(job["id"], _body(agent_id=job["assigned_agent_id"]))
        assert result["routing"]["applied"] == "user_direct"
        assert result["routing"]["reason"] == "vacant"
        kwargs = db.create_routed_blocking_freeze.await_args.kwargs
        assert kwargs["wake"] is None
        assert kwargs["route"]["state"] == "user_direct"
        notifier.record_agent_message.assert_awaited_once()


class TestOfficerAndUserSend:
    @pytest.mark.asyncio
    async def test_blocking_delivers_to_both_and_stamps_user_delivery(self):
        job = _job()
        db = _send_db(job, {"worker_messages": "officer_and_user"})
        notifier = _notifier()
        with _send_patches(db, notifier):
            result = await _send(job["id"], _body(agent_id=job["assigned_agent_id"]))
        assert result["routing"]["applied"] == "officer_and_user"
        assert result["routing"]["state"] == "pending_both"
        kwargs = db.create_routed_blocking_freeze.await_args.kwargs
        assert kwargs["route"]["state"] == "pending_both"
        # pending_both carries no officer SLA — the user already has it.
        assert kwargs["route"]["officer_deadline"] is None
        assert kwargs["wake"] is not None
        notifier.record_agent_message.assert_awaited_once()
        db.mark_route_user_delivery.assert_awaited_once()
        db.settle_outbound_message_log.assert_awaited_once()
        quota = db.reserve_message_delivery_intent.await_args.kwargs
        assert quota["bucket"] == "human"
        assert quota["effective_audience"] == "officer_and_user"

    @pytest.mark.asyncio
    async def test_async_officer_first_creates_route_without_wake_or_email(self):
        job = _job()
        db = _send_db(job, {"worker_messages": "officer_first"})
        notifier = _notifier()
        with _send_patches(db, notifier):
            result = await _send(job["id"], _body(mode="async", purpose="update"))
        assert result["routing"]["applied"] == "officer_first"
        # Acceptance 9: async does not freeze and coalesces into the sitrep.
        db.create_routed_blocking_freeze.assert_not_awaited()
        db.publish_blocking_message.assert_not_awaited()
        notifier.record_agent_message.assert_not_awaited()
        route = db.create_message_route.await_args.args[0]
        assert route["state"] == "pending_officer"
        assert route["blocking"] is False
        assert route["policy_snapshot"]["purpose"] == "update"
        db.log_message.assert_awaited_once()
        assert db.log_message.await_args.kwargs["recipient_email"] is None


class TestUserDirectByteCompat:
    @pytest.mark.asyncio
    async def test_user_direct_keeps_the_legacy_flow_plus_bookkeeping(self):
        job = _job()
        db = _send_db(job, {"worker_messages": "user_direct"})
        notifier = _notifier()
        with _send_patches(db, notifier):
            result = await _send(job["id"], _body(agent_id=job["assigned_agent_id"]))
        assert result["status"] == "sent"
        assert result["recipient"] == "o***@example.com"
        # OC-01 changed this contract deliberately. The old order — freeze,
        # dispatch, log, then best-effort route — is exactly the window that
        # could strand a job forever, and user_direct is the DEFAULT policy so
        # it was the common path. Message, route and freeze now commit as ONE
        # unit BEFORE any external delivery.
        kwargs = db.create_routed_blocking_freeze.await_args.kwargs
        assert kwargs["wake"] is None
        route = kwargs["route"]
        assert route["state"] == "user_direct"
        assert route["blocking"] is True
        assert route["total_deadline"] is not None
        freeze = db.create_routed_blocking_freeze.await_args.args[1]
        assert freeze["route_id"] == route["route_id"]
        assert kwargs["message_entry"]["subject"] == "Need input"
        # The separate freeze and best-effort route are gone; the message is
        # logged inside the transaction, so no second log entry.
        db.publish_blocking_message.assert_not_awaited()
        db.create_message_route.assert_not_awaited()
        db.log_message.assert_not_awaited()
        notifier.record_agent_message.assert_awaited_once()
        # The user leg is a feed row (unified notification system): owner,
        # job, thread, the worker's text verbatim, and the ledger row id from
        # the transaction so a deferred email's Message-ID can be stamped
        # onto message_log for reply routing.
        recorded = notifier.record_agent_message.await_args.kwargs
        assert recorded["user_id"] == job["user_id"]
        assert recorded["job_id"] == job["id"]
        assert recorded["thread_id"] == route["thread_id"]
        assert recorded["blocking"] is True
        assert recorded["subject"] == "Need input"
        assert recorded["message_md"] == "Please answer"
        assert (
            recorded["message_log_id"]
            == (db.create_routed_blocking_freeze.return_value["originating_message_id"])
        )
        assert recorded["deliver_to"] == ("owner@example.com", "Owner")
        quota = db.reserve_message_delivery_intent.await_args.kwargs
        assert quota["bucket"] == "human"
        assert quota["effective_audience"] == "human"

    @pytest.mark.asyncio
    async def test_route_bookkeeping_failure_never_fails_the_send(self):
        job = _job()
        db = _send_db(job, {"worker_messages": "user_direct"})
        db.create_message_route = AsyncMock(side_effect=RuntimeError("no table"))
        notifier = _notifier()
        with _send_patches(db, notifier):
            result = await _send(job["id"], _body(agent_id=job["assigned_agent_id"]))
        assert result["status"] == "sent"

    @pytest.mark.asyncio
    async def test_async_user_direct_creates_no_route(self):
        job = _job()
        db = _send_db(job, {"worker_messages": "user_direct"})
        notifier = _notifier()
        with _send_patches(db, notifier):
            result = await _send(job["id"], _body(mode="async"))
        assert result["status"] == "sent"
        db.create_message_route.assert_not_awaited()
        db.log_message.assert_awaited_once()
        assert db.log_message.await_args.kwargs["status"] == "pending"
        db.settle_outbound_message_log.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_async_user_direct_refuses_delivery_when_prelog_fails(self):
        job = _job()
        db = _send_db(job, {"worker_messages": "user_direct"})
        db.log_message = AsyncMock(return_value=None)
        notifier = _notifier()
        with _send_patches(db, notifier):
            with pytest.raises(HTTPException) as exc:
                await _send(job["id"], _body(mode="async"))
        assert exc.value.status_code == 503
        notifier.record_agent_message.assert_not_awaited()
        assert (
            db.settle_message_delivery_attempt.await_args.kwargs["failure_class"]
            == "message_log_failed"
        )

    @pytest.mark.asyncio
    async def test_explicit_recipient_stays_direct_and_charges_human_once(self):
        job = _job()
        db = _send_db(job, {"worker_messages": "officer_first"})
        db.get_project_members = AsyncMock(
            return_value=[
                {
                    "user_id": str(uuid4()),
                    "display_name": "Alice Example",
                    "email": "alice@example.test",
                }
            ]
        )
        notifier = _notifier()
        with _send_patches(db, notifier):
            result = await _send(
                job["id"],
                _body(to="Alice Example", mode="async", project_id=PROJECT_ID),
            )
        assert result["status"] == "sent"
        db.get_project_officer.assert_not_awaited()
        quota = db.reserve_message_delivery_intent.await_args.kwargs
        assert quota["bucket"] == "human"
        assert quota["effective_audience"] == "explicit_recipient"
        assert db.reserve_message_delivery_intent.await_count == 1

    @pytest.mark.asyncio
    async def test_internal_flood_refusal_never_touches_human_notifier(self):
        job = _job()
        db = _send_db(job, {"worker_messages": "officer_first"})
        db.reserve_message_delivery_intent = AsyncMock(
            return_value={
                "allowed": False,
                "bucket": "officer_internal",
                "limit": "internal_job_hourly",
                "retry_after_seconds": 3600,
            }
        )
        notifier = _notifier()
        with _send_patches(db, notifier):
            result = await _send(job["id"], _body(mode="async"))
        assert result.status_code == 429
        notifier.record_agent_message.assert_not_awaited()
        assert db.log_message.await_args.kwargs["effective_audience"] == "officer"


# =============================================================================
# §5.1 — hold/decommission drains
# =============================================================================


class TestDrains:
    @pytest.mark.asyncio
    async def test_drain_escalates_every_pending_blocking_route(self):
        route_a = {"route_id": str(uuid4()), "job_id": str(uuid4()), "thread_id": "a1"}
        route_b = {"route_id": str(uuid4()), "job_id": str(uuid4()), "thread_id": "b2"}
        db = MagicMock()
        db.list_pending_officer_blocking_routes = AsyncMock(
            return_value=[route_a, route_b]
        )
        with patch.object(
            routing,
            "escalate_route",
            AsyncMock(return_value={"escalated": True, "delivered": True}),
        ) as escalate:
            drained = await routing.drain_officer_blocking_routes(
                db, PROJECT_ID, reason="officer_hold"
            )
        assert drained == 2
        assert escalate.await_count == 2
        assert escalate.await_args.kwargs["reason"] == "officer_hold"

    @pytest.mark.asyncio
    async def test_drain_survives_listing_failure(self):
        db = MagicMock()
        db.list_pending_officer_blocking_routes = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        assert (
            await routing.drain_officer_blocking_routes(
                db, PROJECT_ID, reason="officer_hold"
            )
            == 0
        )

    @pytest.mark.asyncio
    async def test_escalate_route_cas_loss_reports_not_escalated(self):
        db = MagicMock()
        db.transition_message_route = AsyncMock(return_value=None)
        outcome = await routing.escalate_route(
            db,
            {"route_id": str(uuid4()), "job_id": str(uuid4()), "thread_id": "t"},
            reason="officer_hold",
            actor_kind="system",
        )
        assert outcome == {"escalated": False, "delivered": False}

    @pytest.mark.asyncio
    async def test_hold_endpoint_stages_routes_in_the_hold_transaction(self):
        db = MagicMock()
        db.get_officer_thread_for_project = AsyncMock(return_value=_officer_thread())
        routes = [{"route_id": str(uuid4()), "job_id": str(uuid4())} for _ in range(3)]
        db.set_project_officer_hold = AsyncMock(
            return_value={"thread": _officer_thread(), "routes": routes}
        )
        # The project-admin gate is declared on the route
        # (``orchestrator.routers.officers``), not inside the operation, so it
        # is no longer part of what this test drives.
        with (
            patch.object(main, "postgres_db", db),
            patch(
                "orchestrator.services.officer_notices.inject_officer_notice",
                AsyncMock(return_value=True),
            ),
            patch(
                "orchestrator.services.message_routing.deliver_route_to_user",
                AsyncMock(return_value=True),
            ) as deliver,
        ):
            result = await officer_lifecycle.hold_project_officer(
                MagicMock(),
                PROJECT_ID,
                None,
                dependencies=main._officer_post_lifecycle_dependencies(),
            )
        assert result["status"] == "held"
        assert result["drained_blocking_routes"] == 3
        assert result["delivered_blocking_routes"] == 3
        db.set_project_officer_hold.assert_awaited_once()
        assert db.set_project_officer_hold.await_args.kwargs["route_reason"] == (
            "officer_hold"
        )
        assert deliver.await_count == 3
        assert deliver.await_args.kwargs["reason"] == "officer_hold"


# =============================================================================
# M3 — officer action guards and flows
# =============================================================================


def _guard_request(scope: str | None = None):
    request = MagicMock()
    request.headers = {"X-MCP-Scope": scope} if scope else {}
    return request


def _runtime_officer(
    *, project_id: str = PROJECT_ID, thread_id: str = OFFICER_TID, incarnation: int = 0
) -> RuntimeActorContext:
    return RuntimeActorContext(
        caller_kind="officer",
        project_id=project_id,
        project_role="owner",
        thread_id=thread_id,
        officer_incarnation=incarnation,
        user_id=str(uuid4()),
    )


def _authorized_officer():
    return patch.object(
        main,
        "authorize_runtime_actor_request",
        AsyncMock(return_value=_runtime_officer()),
    )


def _officer_action_deps(**overrides):
    """The officer-action collaborators, from the application's own factory.

    Built inside the patch scope so ``main.postgres_db``,
    ``main.notification_service`` and ``main.authorize_runtime_actor_request``
    patches steer it. ``overrides`` replaces a constructed port — the reply
    lane and the route-resolution recorder are ports on this object now, not
    names on ``main``.
    """
    return dataclasses.replace(main._officer_message_action_dependencies(), **overrides)


def _action_db(job, *, officer=True, route=None):
    db = MagicMock()
    db.get_job = AsyncMock(return_value=job)
    db.get_officer_thread_for_project = AsyncMock(
        return_value=_officer_thread() if officer else None
    )
    db.get_project_officer = AsyncMock(return_value=_post())
    db.find_message_route_for_thread = AsyncMock(return_value=route)
    db.get_message_route = AsyncMock(return_value=route)
    db.transition_message_route = AsyncMock(return_value=route)
    db.record_security_event = AsyncMock()
    return db


def _route(**overrides):
    route = {
        "route_id": str(uuid4()),
        "job_id": str(uuid4()),
        "project_id": PROJECT_ID,
        "thread_id": "abc123",
        "state": "pending_officer",
        "blocking": True,
        "officer_thread_id": OFFICER_TID,
        "policy_snapshot": {"worker_messages": "officer_first"},
    }
    route.update(overrides)
    return route


class TestOfficerActionGuards:
    @pytest.mark.asyncio
    async def test_project_less_job_is_403(self):
        job = _job(project_id=None)
        db = _action_db(job)
        with (
            patch.object(main, "postgres_db", db),
        ):
            with pytest.raises(HTTPException) as exc:
                await officer_actions.officer_reply_to_worker_message(
                    _guard_request(),
                    job["id"],
                    "abc123",
                    main.OfficerMessageReplyRequest(message="hi"),
                    dependencies=_officer_action_deps(),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("action", ["reply", "escalate", "ack"])
    async def test_shared_key_worker_is_denied_every_officer_action(self, action):
        """OC-02: shared transport + correct body thread/scope is never identity."""
        job = _job()
        db = _action_db(job)
        with (
            patch.object(main, "postgres_db", db),
        ):
            with pytest.raises(HTTPException) as exc:
                request = _guard_request(scope=f"project:{PROJECT_ID}")
                if action == "reply":
                    await officer_actions.officer_reply_to_worker_message(
                        request,
                        job["id"],
                        "abc123",
                        main.OfficerMessageReplyRequest(
                            message="hi", officer_thread_id=OFFICER_TID
                        ),
                        dependencies=_officer_action_deps(),
                    )
                elif action == "escalate":
                    await officer_actions.officer_escalate_worker_message(
                        request,
                        job["id"],
                        "abc123",
                        main.OfficerMessageEscalateRequest(
                            context="help", officer_thread_id=OFFICER_TID
                        ),
                        dependencies=_officer_action_deps(),
                    )
                else:
                    await officer_actions.officer_acknowledge_worker_message(
                        request,
                        job["id"],
                        "abc123",
                        main.OfficerMessageAckRequest(
                            note="seen", officer_thread_id=OFFICER_TID
                        ),
                        dependencies=_officer_action_deps(),
                    )
        assert exc.value.status_code == 403
        assert exc.value.detail["code"] == "missing_credential"
        db.record_security_event.assert_awaited_once()

    def test_public_action_schema_contains_no_actor_identity(self):
        assert "officer_thread_id" not in main.OfficerMessageReplyRequest.model_fields
        assert (
            "officer_thread_id" not in main.OfficerMessageEscalateRequest.model_fields
        )
        assert "officer_thread_id" not in main.OfficerMessageAckRequest.model_fields

    @pytest.mark.asyncio
    async def test_no_open_route_is_409(self):
        job = _job()
        db = _action_db(job, route=None)
        with (
            patch.object(main, "postgres_db", db),
            _authorized_officer(),
        ):
            with pytest.raises(HTTPException) as exc:
                await officer_actions.officer_reply_to_worker_message(
                    _guard_request(),
                    job["id"],
                    "abc123",
                    main.OfficerMessageReplyRequest(message="hi"),
                    dependencies=_officer_action_deps(),
                )
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_previous_incarnation_route_is_not_adoptable(self):
        """§5.1: recommission never adopts an old waiting route."""
        job = _job()
        old_route = _route(officer_thread_id=str(uuid4()))
        db = _action_db(job, route=old_route)
        with (
            patch.object(main, "postgres_db", db),
            _authorized_officer(),
        ):
            with pytest.raises(HTTPException) as exc:
                await officer_actions.officer_reply_to_worker_message(
                    _guard_request(),
                    job["id"],
                    "abc123",
                    main.OfficerMessageReplyRequest(message="hi"),
                    dependencies=_officer_action_deps(),
                )
        assert exc.value.status_code == 409
        assert "incarnation" in exc.value.detail

    @pytest.mark.asyncio
    async def test_ack_on_blocking_route_is_400(self):
        job = _job()
        db = _action_db(job, route=_route(blocking=True))
        with (
            patch.object(main, "postgres_db", db),
            _authorized_officer(),
        ):
            with pytest.raises(HTTPException) as exc:
                await officer_actions.officer_acknowledge_worker_message(
                    _guard_request(),
                    job["id"],
                    "abc123",
                    main.OfficerMessageAckRequest(),
                    dependencies=_officer_action_deps(),
                )
        assert exc.value.status_code == 400
        assert "frozen" in exc.value.detail


class TestOfficerActionFlows:
    @pytest.mark.asyncio
    async def test_reply_delivers_through_existing_lane_as_officer(self):
        job = _job()
        route = _route()
        db = _action_db(job, route=route)
        deliver = AsyncMock(return_value=("immediate_resume", 2))
        record = AsyncMock()
        with (
            patch.object(main, "postgres_db", db),
            _authorized_officer(),
        ):
            result = await officer_actions.officer_reply_to_worker_message(
                _guard_request(scope=f"project:{PROJECT_ID}"),
                job["id"],
                "abc123",
                main.OfficerMessageReplyRequest(message="Use option B."),
                dependencies=_officer_action_deps(
                    route_inbound_reply=deliver,
                    record_route_reply_resolution=record,
                ),
            )
        assert result["status"] == "replied"
        assert result["delivery_strategy"] == "immediate_resume"
        kwargs = deliver.await_args.kwargs
        assert kwargs["resolver_kind"] == "officer"
        assert kwargs["resolver_id"] == OFFICER_TID
        assert "Use option B." in kwargs["message"]
        record.assert_awaited_once()
        assert record.await_args.kwargs["actor_kind"] == "officer"

    @pytest.mark.asyncio
    async def test_escalate_carries_officer_context(self):
        job = _job()
        route = _route()
        db = _action_db(job, route=route)
        escalate = AsyncMock(return_value={"escalated": True, "delivered": True})
        with (
            patch.object(main, "postgres_db", db),
            patch("orchestrator.services.message_routing.escalate_route", escalate),
            _authorized_officer(),
        ):
            result = await officer_actions.officer_escalate_worker_message(
                _guard_request(),
                job["id"],
                "abc123",
                main.OfficerMessageEscalateRequest(
                    context="I recommend option B, but it costs money.",
                ),
                dependencies=_officer_action_deps(),
            )
        assert result == {
            "status": "escalated",
            "delivered": True,
            "route_id": route["route_id"],
        }
        kwargs = escalate.await_args.kwargs
        assert kwargs["reason"] == "officer_escalated"
        assert kwargs["actor_kind"] == "officer"
        assert kwargs["officer_thread_id"] == OFFICER_TID
        assert kwargs["officer_incarnation"] == 0
        assert kwargs["officer_context"].startswith("I recommend")

    @pytest.mark.asyncio
    async def test_escalate_race_with_sla_reports_already_escalated(self):
        job = _job()
        route = _route()
        db = _action_db(job, route=route)
        db.get_message_route = AsyncMock(
            return_value={**route, "state": "escalated_to_user", "user_delivery_at": 1}
        )
        with (
            patch.object(main, "postgres_db", db),
            patch(
                "orchestrator.services.message_routing.escalate_route",
                AsyncMock(return_value={"escalated": False, "delivered": False}),
            ),
            _authorized_officer(),
        ):
            result = await officer_actions.officer_escalate_worker_message(
                _guard_request(),
                job["id"],
                "abc123",
                main.OfficerMessageEscalateRequest(),
                dependencies=_officer_action_deps(),
            )
        assert result["status"] == "escalated"
        assert "already escalated" in result["note"]

    @pytest.mark.asyncio
    async def test_ack_closes_async_route(self):
        job = _job()
        route = _route(blocking=False)
        resolved = {**route, "state": "resolved_by_officer"}
        db = _action_db(job, route=route)
        db.transition_message_route = AsyncMock(return_value=resolved)
        with (
            patch.object(main, "postgres_db", db),
            _authorized_officer(),
        ):
            result = await officer_actions.officer_acknowledge_worker_message(
                _guard_request(),
                job["id"],
                "abc123",
                main.OfficerMessageAckRequest(note="seen"),
                dependencies=_officer_action_deps(),
            )
        assert result["status"] == "acknowledged"
        kwargs = db.transition_message_route.await_args.kwargs
        assert kwargs["to_state"] == "resolved_by_officer"
        assert kwargs["actor_kind"] == "officer"
        assert kwargs["officer_thread_id"] == OFFICER_TID
        assert "acknowledged: seen" in kwargs["note"]


# =============================================================================
# [A-reply] — inbound reply integration (§5.3)
# =============================================================================


def _reply_db(job, *, route=None):
    db = MagicMock()
    db.get_job = AsyncMock(return_value=job)
    db.get_message_sequence = AsyncMock(return_value=2)
    db.log_message = AsyncMock()
    db.find_message_route_for_thread = AsyncMock(return_value=route)
    db.get_user_settings = AsyncMock(return_value={})
    db.append_queued_reply = AsyncMock(return_value=True)
    return db


async def _reply(*args, **kwargs):
    """Drive the extracted reply funnel with the application's collaborators.

    Call inside the patch scope: ``main._inbound_reply_dependencies()`` binds
    ``postgres_db``, ``notification_service``, ``_internal_resume_job``,
    the completion-control boundary at call time, so those patches keep
    steering the code under test.
    """
    return await inbound_reply_svc.route_inbound_reply(
        *args, **kwargs, dependencies=main._inbound_reply_dependencies()
    )


class TestInboundReplyRouteIntegration:
    @pytest.mark.asyncio
    async def test_blocking_resume_records_user_resolution(self):
        job = _job(
            status="waiting_for_reply",
            freeze_data={"thread_id": "abc123", "route_id": "r1"},
        )
        db = _reply_db(job)
        record = AsyncMock()
        with (
            patch.object(main, "COMPLETION_COMMANDS_ENABLED", False),
            patch.object(main, "postgres_db", db),
            patch.object(
                main.job_control_operations.JobControlOperations,
                "internal_resume_job",
                AsyncMock(return_value=True),
            ),
            patch.object(inbound_reply_svc, "record_route_reply_resolution", record),
        ):
            strategy, _seq = await _reply(job["id"], "abc123", "the answer")
        assert strategy == "immediate_resume"
        record.assert_awaited_once()
        assert record.await_args.kwargs["actor_kind"] == "user"

    @pytest.mark.asyncio
    async def test_officer_resolver_kind_is_recorded(self):
        job = _job(
            status="waiting_for_reply",
            freeze_data={"thread_id": "abc123", "route_id": "r1"},
        )
        db = _reply_db(job)
        record = AsyncMock()
        with (
            patch.object(main, "COMPLETION_COMMANDS_ENABLED", False),
            patch.object(main, "postgres_db", db),
            patch.object(
                main.job_control_operations.JobControlOperations,
                "internal_resume_job",
                AsyncMock(return_value=True),
            ),
            patch.object(inbound_reply_svc, "record_route_reply_resolution", record),
        ):
            await _reply(
                job["id"],
                "abc123",
                "the answer",
                resolver_kind="officer",
                resolver_id=OFFICER_TID,
            )
        assert record.await_args.kwargs["actor_kind"] == "officer"
        assert record.await_args.kwargs["actor_id"] == OFFICER_TID

    @pytest.mark.asyncio
    async def test_late_user_reply_on_officer_resolved_route_rides_guidance(self):
        """§5.3: the officer answered; a later user reply supersedes it via
        the P1-A guidance lane instead of waiting for a phase boundary."""
        job = _job(status="processing")
        route = _route(state="resolved_by_officer")
        db = _reply_db(job, route=route)
        guidance = AsyncMock(return_value="guidance_next_turn")
        with (
            patch.object(main, "COMPLETION_COMMANDS_ENABLED", False),
            patch.object(main, "postgres_db", db),
            patch.object(inbound_reply_svc, "queue_supervisor_guidance", guidance),
        ):
            strategy, _seq = await _reply(job["id"], "abc123", "Actually do C.")
        assert strategy == "guidance_next_turn"
        text = guidance.await_args.args[2]
        assert "supersedes" in text
        assert "Actually do C." in text

    @pytest.mark.asyncio
    async def test_reply_after_disposition_is_recorded_and_wakes_officer(self):
        """§5.3: after disposition it is recorded and wakes the officer
        rather than pretending a finished job can resume."""
        job = _job(status="completed")
        route = _route()
        db = _reply_db(job, route=route)
        wake = AsyncMock(return_value=True)
        with (
            patch.object(main, "COMPLETION_COMMANDS_ENABLED", False),
            patch.object(main, "postgres_db", db),
            patch("orchestrator.services.session_wake.notify_officer", wake),
            patch.object(main, "_kick_officer_event_drain", MagicMock()),
        ):
            strategy, _seq = await _reply(job["id"], "abc123", "thanks anyway")
        assert strategy == "recorded_after_disposition"
        wake.assert_awaited_once()
        assert wake.await_args.kwargs["source"] == "worker_message"
        db.append_queued_reply.assert_not_awaited()
        db.log_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_threads_without_routes_keep_legacy_queueing(self):
        job = _job(status="processing")
        db = _reply_db(job, route=None)
        with (
            patch.object(main, "COMPLETION_COMMANDS_ENABLED", False),
            patch.object(main, "postgres_db", db),
        ):
            strategy, _seq = await _reply(job["id"], "abc123", "noted")
        assert strategy == "next_strategic_phase"
        db.append_queued_reply.assert_awaited_once()


# =============================================================================
# Route wiring — the gate and the factory the handlers resolve through
# =============================================================================


def _messaging_client(**factories) -> TestClient:
    """Mount the extracted router on a bare app, the way the application does."""
    app = FastAPI()
    for name, value in factories.items():
        setattr(app.state, name, value)
    app.include_router(messaging_routes.router)
    return TestClient(app, raise_server_exceptions=False)


class TestRouteWiring:
    """The tests above drive the operations directly. These two prove the one
    thing that can only be proved at the route: the internal-key gate — which
    left ``send_agent_message`` for the route declaration — still runs, and
    still runs BEFORE the funnel."""

    def test_send_route_runs_the_internal_gate_before_the_funnel(self):
        from orchestrator.security.access import require_internal as real_gate

        store = AsyncMock()
        dependencies = agent_messaging.AgentMessagingDependencies(
            store=store,
            notifier=MagicMock(),
            require_internal=real_gate,
            completion_commands_enabled=lambda: True,
            kick_officer_event_drain=MagicMock(),
        )
        response = _messaging_client(
            agent_messaging_dependencies_factory=lambda: dependencies
        ).post(
            f"/api/jobs/{uuid4()}/messages/send",
            json={"to": "user", "subject": "s", "message": "m", "mode": "async"},
        )
        assert response.status_code == 401
        store.get_job.assert_not_awaited()

    def test_send_route_hands_the_funnel_the_applications_dependencies(self):
        job = _job()
        db = _send_db(job, {"worker_messages": "user_direct"})
        notifier = _notifier()
        with _send_patches(db, notifier):
            dependencies = dataclasses.replace(
                main._agent_messaging_dependencies(), require_internal=AsyncMock()
            )
            response = _messaging_client(
                agent_messaging_dependencies_factory=lambda: dependencies
            ).post(
                f"/api/jobs/{job['id']}/messages/send",
                json={
                    "to": "user",
                    "subject": "Need input",
                    "message": "Please answer",
                    "mode": "blocking",
                },
            )
        assert response.status_code == 200
        assert response.json()["status"] == "sent"
        assert dependencies.store is db
        db.create_routed_blocking_freeze.assert_awaited_once()
        notifier.record_agent_message.assert_awaited_once()


# =============================================================================
# Wake plumbing + worker tool compatibility
# =============================================================================


class TestWakeAndToolPlumbing:
    def test_worker_message_wake_bypasses_debounce(self):
        from orchestrator.services import session_wake

        assert session_wake.OFFICER_DEBOUNCE_BY_SOURCE["worker_message"] == 0

    def test_send_message_tool_signature_stays_compatible(self):
        """Worker interface compatibility: purpose is optional; the old
        five-argument call shape still binds."""
        import inspect

        from agent.tools.communication.messaging import create_communication_tools

        context = MagicMock()
        context.job_id = "j"
        context.user_id = None
        context.has_workspace.return_value = False
        tools = create_communication_tools(context)
        send_message = tools[0]
        signature = inspect.signature(send_message.coroutine)
        parameters = list(signature.parameters)
        assert parameters[:5] == ["to", "subject", "message", "mode", "thread_id"]
        assert signature.parameters["purpose"].default is None

    @pytest.mark.asyncio
    async def test_sitrep_worker_messages_section_lists_open_routes(self):
        from orchestrator.services.sitrep import _worker_messages_section

        now = datetime.now(timezone.utc)
        db = MagicMock()
        db.list_open_worker_message_routes = AsyncMock(
            return_value=[
                {
                    "job_id": "12345678-0000-0000-0000-000000000000",
                    "thread_id": "abc123",
                    "state": "pending_officer",
                    "blocking": True,
                    "created_at": now - timedelta(minutes=12),
                    "subject": "Which DB?",
                    "policy_snapshot": {"purpose": "question"},
                }
            ]
        )
        lines = await _worker_messages_section(db, PROJECT_ID, now)
        joined = "\n".join(lines)
        assert "Worker messages (1 open):" in joined
        assert "BLOCKING pending_officer" in joined
        assert "job 12345678" in joined
        assert "Which DB?" in joined
        assert "reply_to_job_message" in joined

    @pytest.mark.asyncio
    async def test_sitrep_section_is_silent_when_empty(self):
        from orchestrator.services.sitrep import _worker_messages_section

        db = MagicMock()
        db.list_open_worker_message_routes = AsyncMock(return_value=[])
        assert (
            await _worker_messages_section(db, PROJECT_ID, datetime.now(timezone.utc))
            == []
        )


# =============================================================================
# Terminal auto-close (a dead job's open routes stop haunting the officer)
# =============================================================================


class TestTerminalRouteAutoClose:
    """Ruling: when a job reaches a terminal status its still-open routes are
    closed automatically — stamped, in history, out of every open count —
    because no manual closure verb is safe (ack refuses blocking routes; a
    human reply risks resuming the dead job)."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
    async def test_terminal_status_closes_via_the_db_claim(self, status):
        db = MagicMock()
        db.close_message_routes_for_terminal_jobs = AsyncMock(
            return_value=[{"route_id": str(uuid4()), "thread_id": "abc123"}]
        )
        job_id = str(uuid4())
        closed = await routing.close_routes_for_terminal_job(db, job_id, status)
        assert closed == 1
        db.close_message_routes_for_terminal_jobs.assert_awaited_once_with(job_id)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["pending_review", "paused", "processing"])
    async def test_non_terminal_status_is_a_no_op(self, status):
        db = MagicMock()
        db.close_message_routes_for_terminal_jobs = AsyncMock()
        assert (
            await routing.close_routes_for_terminal_job(db, str(uuid4()), status) == 0
        )
        db.close_message_routes_for_terminal_jobs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_close_failure_never_raises(self):
        """Fail-open discipline: a broken close must not break the terminal
        transition that triggered it."""
        db = MagicMock()
        db.close_message_routes_for_terminal_jobs = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        assert (
            await routing.close_routes_for_terminal_job(db, str(uuid4()), "cancelled")
            == 0
        )

    def test_closed_is_terminal_and_never_an_open_state(self):
        """The whole point: 'closed' must drop out of every open-state filter
        (sitrep listing, reply-lane open_only lookup, reconciler scans) and
        must never be officer-actionable."""
        assert "closed" in routing.ROUTE_TERMINAL_STATES
        assert "closed" not in routing.ROUTE_OPEN_STATES
        assert "closed" not in routing.OFFICER_ACTIONABLE_STATES


# =============================================================================
# Audit repairs OC-05 (content redaction) and OC-06 (honest delivery stamps)
# =============================================================================


class TestDeliveryOutcomeClassification:
    """OC-06. NotificationService reports failure by RETURNING an error dict,
    not by raising, so a try/except around dispatch sees success. Every route
    and log decision now derives from one normalized outcome instead."""

    def test_provider_acceptance_is_accepted(self):
        from orchestrator.services.message_routing import classify_dispatch

        assert classify_dispatch(
            {"email": True, "email_message_id": "<m@x>", "queued": False}
        ).accepted

    def test_queued_for_digest_is_accepted(self):
        # Quiet-hours queueing is durable and genuinely accepted; retrying it
        # would double-send once the window closes.
        from orchestrator.services.message_routing import classify_dispatch

        assert classify_dispatch({"queued": True}).accepted

    def test_an_uninitialized_service_is_retryable_not_delivered(self):
        from orchestrator.services.message_routing import classify_dispatch

        out = classify_dispatch({"error": "NotificationService not initialized"})
        assert not out.accepted and "not initialized" in out.detail

    def test_empty_and_all_false_results_are_retryable(self):
        from orchestrator.services.message_routing import classify_dispatch

        assert not classify_dispatch({}).accepted
        assert not classify_dispatch({"email": False, "ntfy": False}).accepted

    def test_a_non_dict_result_is_retryable_rather_than_crashing(self):
        from orchestrator.services.message_routing import classify_dispatch

        assert not classify_dispatch(None).accepted

    def test_log_status_derives_from_the_same_outcome(self):
        from orchestrator.services.message_routing import classify_dispatch

        assert classify_dispatch({"email": True}).log_status == "sent"
        assert classify_dispatch({"error": "x"}).log_status == "failed"


class TestFailedDeliveryStaysRetryable:
    """OC-06 end to end: the reconciler retries exactly while
    ``user_delivery_at`` is null, so stamping a failure is what strands the
    user's thread forever."""

    @pytest.mark.asyncio
    async def test_a_failed_dispatch_does_not_stamp_delivery(self):
        from orchestrator.services import message_routing

        db = _send_db(_job(), "user_direct")
        notifier = MagicMock()
        notifier.record_agent_message = AsyncMock(
            side_effect=RuntimeError("NotificationService not initialized")
        )
        route = {
            "route_id": str(uuid4()),
            "job_id": str(uuid4()),
            "thread_id": str(uuid4()),
        }
        ok = await message_routing.deliver_route_to_user(
            db, route, reason="officer_escalated", notifier=notifier
        )
        assert ok is False
        db.mark_route_user_delivery.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_accepted_dispatch_stamps_exactly_once(self):
        from orchestrator.services import message_routing

        job = _job()
        db = _send_db(job, "user_direct")
        notifier = _notifier()
        route = {
            "route_id": str(uuid4()),
            "job_id": str(uuid4()),
            "thread_id": str(uuid4()),
        }
        ok = await message_routing.deliver_route_to_user(
            db, route, reason="officer_escalated", notifier=notifier
        )
        assert ok is True
        db.mark_route_user_delivery.assert_awaited_once()
        # Ledger first, feed row second: the escalation is logged ``pending``
        # BEFORE the notification is recorded (so a deferred mail can stamp
        # its Message-ID onto that row), then settled from the outcome.
        assert db.log_message.await_args.kwargs["status"] == "pending"
        log_id = db.log_message.return_value["id"]
        recorded = notifier.record_agent_message.await_args.kwargs
        assert recorded["user_id"] == job["user_id"]
        assert recorded["job_id"] == route["job_id"]
        assert recorded["thread_id"] == route["thread_id"]
        assert recorded["sequence"] is None
        # The automated tier already failed the user: high, so the mail goes now.
        assert recorded["severity"] == "high"
        assert recorded["dedup_key"] == (
            f"route_escalation:{route['route_id']}:officer_escalated"
        )
        assert recorded["message_log_id"] == log_id
        db.settle_outbound_message_log.assert_awaited_once_with(
            log_id,
            accepted=True,
            error_message="provider accepted",
            email_message_id="<m1@x>",
        )


class TestRoutedWorkerTextIsSanitized:
    """OC-05. 'Verbatim' means unedited by us, not unsanitized — this body is
    emailed to a human."""

    @pytest.mark.asyncio
    async def test_a_credential_in_worker_text_never_reaches_the_user(self):
        from orchestrator.services import message_routing

        db = _send_db(_job(), "user_direct")
        db.get_thread_messages = AsyncMock(
            return_value={
                "subject": "deploy failed",
                "messages": [
                    {
                        "direction": "outbound",
                        "subject": "deploy failed",
                        "message": "used sk-abcdefghijklmnopqrstuvwxyz012345 to push",
                    }
                ],
            }
        )
        notifier = _notifier()
        route = {
            "route_id": str(uuid4()),
            "job_id": str(uuid4()),
            "thread_id": str(uuid4()),
        }
        await message_routing.deliver_route_to_user(
            db, route, reason="officer_escalated", notifier=notifier
        )
        sent = notifier.record_agent_message.await_args.kwargs["message_md"]
        assert "sk-abcdefghijklmnopqrstuvwxyz012345" not in sent
        # The reader is told text was withheld rather than shown a quiet edit.
        assert "redacted" in sent.lower()
