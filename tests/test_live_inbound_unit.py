"""Inbound live sessions without a database (live_session_jobs.md §3.1, §4.4).

The connector's public webhook (verification, parsing, the ``ringing`` row,
one admission task per session), the job a live automation fires, and the
admission task's outcomes: no automation and a spent budget reject with SIP
480, a refused admission fails the session with its reason kept, a created
job records the fire.
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from fastapi import HTTPException

from orchestrator.services.live import inbound
from orchestrator.services.live.inbound import (
    LiveInboundDependencies,
    LiveTaskRegistry,
    job_from_automation,
    receive_webhook,
    start_inbound,
)
from orchestrator.services.live.pods import LivePodError
from shared.runtime.live.contract import SessionStatus
from shared.runtime.live.webhook import IncomingCall, sign_webhook

CONNECTOR = "00000000-0000-4000-8000-0000000000a1"
SESSION = "00000000-0000-4000-8000-0000000000b1"
JOB = "00000000-0000-4000-8000-0000000000c1"
AUTOMATION = "00000000-0000-4000-8000-0000000000d1"
OWNER = "00000000-0000-4000-8000-0000000000e1"
PROJECT = "00000000-0000-4000-8000-0000000000f1"
EXPERT_ID = "00000000-0000-4000-8000-00000000ee01"
SECRET = "whsec_" + base64.b64encode(b"live-inbound-unit-secret").decode()
OTHER_SECRET = "whsec_" + base64.b64encode(b"some-other-secret-value").decode()


def _connector(ds_type: str = "gpt_live_sip", **credentials: Any) -> dict[str, Any]:
    return {
        "id": UUID(CONNECTOR),
        "type": ds_type,
        "config": {"model": "gpt-live-1"},
        "credentials": credentials if credentials else {"webhook_secret": SECRET},
    }


def _call(session_id: str | None = "rtc_1", **data: Any) -> dict[str, Any]:
    body = {
        "type": "sip",
        "sip_headers": [
            {"name": "From", "value": '"Caller" <sip:+4960100@pbx;tag=x>'},
            {"name": "To", "value": "<sip:+4960200@openai>"},
        ],
    }
    if session_id is not None:
        body["session_id"] = session_id
    body.update(data)
    return {
        "object": "event",
        "id": "evt_1",
        "type": "live.transport.incoming",
        "created_at": 1_760_000_000,
        "data": body,
    }


def _signed(
    payload: dict[str, Any], *, secret: str = SECRET, webhook_id: str = "wh_1"
) -> tuple[dict[str, str], bytes]:
    body = json.dumps(payload).encode()
    timestamp = int(time.time())
    headers = {
        "Webhook-Id": webhook_id,
        "webhook-timestamp": str(timestamp),
        "webhook-signature": sign_webhook(secret, webhook_id, timestamp, body),
    }
    return headers, body


class _Store:
    """The session store the inbound path reaches (``dependencies.sessions``)."""

    def __init__(self, inserted: dict | None = None):
        self.insert_incoming = AsyncMock(return_value=inserted)
        self.transition = AsyncMock(return_value=(True, {}))
        self.note_admission = AsyncMock()


class _Tasks:
    def __init__(self):
        self.started: list[tuple[str, Any]] = []

    def start(self, key: str, coroutine: Any) -> None:
        self.started.append((key, coroutine))
        coroutine.close()


@pytest.fixture
def store(monkeypatch):
    fake = _Store({"id": UUID(SESSION), "provider_session_id": "rtc_1"})
    monkeypatch.setattr(inbound, "LiveSessionStore", lambda _db: fake)
    return fake


def _dependencies(
    *,
    connector: dict | None = None,
    tasks: Any = None,
    automation: dict | None = None,
    claimed: bool = True,
    admit_as_owner: Any = None,
    reject: Any = None,
) -> LiveInboundDependencies:
    db = SimpleNamespace(
        get_datasource=AsyncMock(return_value=connector),
        live_automation_for_connector=AsyncMock(return_value=automation),
        claim_live_automation_fire=AsyncMock(return_value=claimed),
        record_live_automation_fire=AsyncMock(),
    )
    return LiveInboundDependencies(
        db=db,
        pods=SimpleNamespace(reject=reject or AsyncMock()),
        tasks=tasks if tasks is not None else _Tasks(),
        admit_as_owner=admit_as_owner or AsyncMock(return_value={"id": UUID(JOB)}),
    )


# =============================================================================
# The public webhook
# =============================================================================


class TestReceiveWebhook:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("connector_id", ["not-a-uuid", "", "../etc"])
    async def test_a_malformed_connector_id_is_404_without_a_read(
        self, store, connector_id
    ):
        dependencies = _dependencies(connector=_connector())
        with pytest.raises(HTTPException) as error:
            await receive_webhook(connector_id, {}, b"{}", dependencies)
        assert error.value.status_code == 404
        dependencies.db.get_datasource.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unknown_connector_is_404(self, store):
        dependencies = _dependencies(connector=None)
        with pytest.raises(HTTPException) as error:
            await receive_webhook(CONNECTOR, *_signed(_call()), dependencies)
        assert error.value.status_code == 404

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ds_type", ["postgresql", "generic", "unknown_type"])
    async def test_a_connector_of_no_live_driver_is_404(self, store, ds_type):
        dependencies = _dependencies(connector=_connector(ds_type))
        with pytest.raises(HTTPException) as error:
            await receive_webhook(CONNECTOR, *_signed(_call()), dependencies)
        assert error.value.status_code == 404
        store.insert_incoming.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("credentials", [None, {}, {"webhook_secret": ""}, "x"])
    async def test_a_connector_without_a_secret_is_401(self, store, credentials):
        connector = _connector()
        connector["credentials"] = credentials
        dependencies = _dependencies(connector=connector)
        with pytest.raises(HTTPException) as error:
            await receive_webhook(CONNECTOR, *_signed(_call()), dependencies)
        assert error.value.status_code == 401
        store.insert_incoming.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_bad_signature_is_401(self, store):
        dependencies = _dependencies(connector=_connector())
        headers, body = _signed(_call(), secret=OTHER_SECRET)
        with pytest.raises(HTTPException) as error:
            await receive_webhook(CONNECTOR, headers, body, dependencies)
        assert error.value.status_code == 401
        assert error.value.detail == "Webhook verification failed"
        store.insert_incoming.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_tampered_body_is_401(self, store):
        dependencies = _dependencies(connector=_connector())
        headers, _ = _signed(_call())
        with pytest.raises(HTTPException) as error:
            await receive_webhook(
                CONNECTOR, headers, json.dumps(_call("rtc_2")).encode(), dependencies
            )
        assert error.value.status_code == 401

    @pytest.mark.asyncio
    async def test_a_stale_delivery_is_401(self, store):
        dependencies = _dependencies(connector=_connector())
        body = json.dumps(_call()).encode()
        stale = int(time.time()) - 3600
        headers = {
            "webhook-id": "wh_1",
            "webhook-timestamp": str(stale),
            "webhook-signature": sign_webhook(SECRET, "wh_1", stale, body),
        }
        with pytest.raises(HTTPException) as error:
            await receive_webhook(CONNECTOR, headers, body, dependencies)
        assert error.value.status_code == 401

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        [
            {"type": "live.session.closed", "data": {"session_id": "rtc_1"}},
            {"type": "response.done"},
            _call(type="webrtc"),
        ],
    )
    async def test_another_event_is_a_no_op(self, store, payload):
        tasks = _Tasks()
        dependencies = _dependencies(connector=_connector(), tasks=tasks)
        answer = await receive_webhook(CONNECTOR, *_signed(payload), dependencies)
        assert answer == {"received": True}
        store.insert_incoming.assert_not_awaited()
        assert tasks.started == []

    @pytest.mark.asyncio
    async def test_a_call_without_a_session_id_is_400(self, store):
        dependencies = _dependencies(connector=_connector())
        with pytest.raises(HTTPException) as error:
            await receive_webhook(CONNECTOR, *_signed(_call(None)), dependencies)
        assert error.value.status_code == 400
        store.insert_incoming.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_duplicate_delivery_is_a_no_op(self, store):
        store.insert_incoming.return_value = None
        tasks = _Tasks()
        dependencies = _dependencies(connector=_connector(), tasks=tasks)
        answer = await receive_webhook(CONNECTOR, *_signed(_call()), dependencies)
        assert answer == {"received": True}
        store.insert_incoming.assert_awaited_once()
        assert tasks.started == []

    @pytest.mark.asyncio
    async def test_a_new_call_is_recorded_and_admitted_once(self, store):
        tasks = _Tasks()
        connector = _connector()
        dependencies = _dependencies(connector=connector, tasks=tasks)
        answer = await receive_webhook(
            CONNECTOR, *_signed(_call(), webhook_id="wh_42"), dependencies
        )
        assert answer == {"received": True}
        connector_id, incoming = store.insert_incoming.await_args.args
        assert connector_id == CONNECTOR
        assert isinstance(incoming, IncomingCall)
        assert (incoming.session_id, incoming.webhook_id) == ("rtc_1", "wh_42")
        assert incoming.sip_header("from") == '"Caller" <sip:+4960100@pbx;tag=x>'
        assert [key for key, _ in tasks.started] == [f"live-admit-{SESSION}"]

    @pytest.mark.asyncio
    async def test_the_task_admits_the_recorded_session(self, store, monkeypatch):
        admitted = AsyncMock()
        monkeypatch.setattr(inbound, "start_inbound", admitted)
        registry = LiveTaskRegistry()
        connector = _connector()
        dependencies = _dependencies(connector=connector, tasks=registry)
        await receive_webhook(CONNECTOR, *_signed(_call()), dependencies)
        await asyncio.gather(*registry.tasks.values())
        admitted.assert_awaited_once_with(
            store.insert_incoming.return_value, connector, dependencies
        )


# =============================================================================
# The task registry
# =============================================================================


class TestLiveTaskRegistry:
    @pytest.mark.asyncio
    async def test_one_task_per_key(self):
        registry = LiveTaskRegistry()
        release = asyncio.Event()
        runs: list[str] = []

        async def admit(name: str) -> None:
            runs.append(name)
            await release.wait()

        first = registry.start("live-admit-1", admit("first"))
        second_coroutine = admit("second")
        second = registry.start("live-admit-1", second_coroutine)
        assert second is first
        assert inspect.getcoroutinestate(second_coroutine) == inspect.CORO_CLOSED
        release.set()
        await first
        await asyncio.sleep(0)
        assert runs == ["first"]
        assert registry.tasks == {}

    @pytest.mark.asyncio
    async def test_a_finished_key_may_start_again(self):
        registry = LiveTaskRegistry()

        async def noop() -> None:
            return None

        first = registry.start("k", noop())
        await first
        await asyncio.sleep(0)
        second = registry.start("k", noop())
        assert second is not first
        await second

    @pytest.mark.asyncio
    async def test_a_failed_task_is_logged_and_forgotten(self, caplog):
        registry = LiveTaskRegistry()

        async def boom() -> None:
            raise RuntimeError("secret detail")

        task = registry.start("k", boom())
        with pytest.raises(RuntimeError):
            await task
        await asyncio.sleep(0)
        assert registry.tasks == {}
        messages = [r.getMessage() for r in caplog.records]
        assert any("Live task k failed (RuntimeError)" in m for m in messages)
        assert not any("secret detail" in m for m in messages)

    @pytest.mark.asyncio
    async def test_drain_cancels_and_reopens(self):
        registry = LiveTaskRegistry()
        started = asyncio.Event()

        async def forever() -> None:
            started.set()
            await asyncio.Event().wait()

        task = registry.start("k", forever())
        await started.wait()
        await registry.drain()
        assert task.cancelled()
        assert registry.tasks == {}
        assert registry.stopping is False

        async def noop() -> None:
            return None

        again = registry.start("k", noop())
        assert again is not None
        await again

    @pytest.mark.asyncio
    async def test_no_task_starts_while_draining(self):
        registry = LiveTaskRegistry()
        registry.stopping = True

        async def noop() -> None:
            return None

        coroutine = noop()
        assert registry.start("k", coroutine) is None
        assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
        assert registry.tasks == {}


# =============================================================================
# The job a live automation fires
# =============================================================================


def _automation(**overrides: Any) -> dict[str, Any]:
    row = {
        "id": UUID(AUTOMATION),
        "owner_id": UUID(OWNER),
        "name": "Front desk",
        "expert": "receptionist",
        "expert_id": None,
        "prompt": "Answer the caller",
        "config_override": {},
        "autonomy": None,
        "priority": 7,
        "project_id": UUID(PROJECT),
        "job_shape": {
            "execution": {"connectors": {"tel": {"ref": {"name": "line"}}}},
            "primary_connector": "tel",
        },
    }
    row.update(overrides)
    return row


class TestJobFromAutomation:
    def test_the_live_job(self):
        job = job_from_automation(_automation(), "rtc_9")
        assert job.live_session is True
        assert job.description == "Answer the caller"
        assert job.expert == "receptionist"
        assert job.priority == 7
        assert job.project_id == PROJECT
        assert job.context == {
            "automation_id": AUTOMATION,
            "automation_name": "Front desk",
            "automation_trigger": "live",
        }
        assert list(job.execution.connectors) == ["tel"]
        assert job.execution.connectors["tel"].ref.name == "line"
        assert job.execution.session.connector == "tel"
        assert job.execution.session.id == "rtc_9"
        assert job.config_override is None

    def test_the_db_expert_wins_over_the_name(self):
        job = job_from_automation(
            _automation(expert="developer", expert_id=UUID(EXPERT_ID)), "rtc"
        )
        assert job.expert == EXPERT_ID

    @pytest.mark.parametrize("expert", ["worker_base", None, ""])
    def test_the_default_worker_is_the_deployment_default(self, expert):
        assert job_from_automation(_automation(expert=expert), "rtc").expert is None

    def test_the_autonomy_is_injected_when_the_override_has_none(self):
        job = job_from_automation(
            _automation(autonomy="review", config_override={"x": 1}), "rtc"
        )
        assert job.config_override == {"x": 1, "autonomy": "review"}

    def test_an_explicit_override_autonomy_is_kept(self):
        row = _automation(autonomy="review", config_override={"autonomy": "full"})
        assert job_from_automation(row, "rtc").config_override == {"autonomy": "full"}
        assert row["config_override"] == {"autonomy": "full"}

    def test_the_stored_override_is_not_mutated(self):
        override = {"x": 1}
        job_from_automation(_automation(autonomy="full", config_override=override), "r")
        assert override == {"x": 1}

    def test_a_missing_workspace_is_the_default(self):
        job = job_from_automation(_automation(), "rtc")
        assert "workspace" not in job.model_fields_set

    def test_a_null_workspace_is_none(self):
        shape = {**_automation()["job_shape"], "workspace": None}
        job = job_from_automation(_automation(job_shape=shape), "rtc")
        assert "workspace" in job.model_fields_set
        assert job.workspace is None

    def test_a_workspace_binding_is_kept(self):
        binding = {"template": {"ref": {"name": "development"}}}
        shape = {**_automation()["job_shape"], "workspace": binding}
        job = job_from_automation(_automation(job_shape=shape), "rtc")
        assert job.workspace == binding

    def test_no_project(self):
        assert job_from_automation(_automation(project_id=None), "r").project_id is None

    def test_default_priority(self):
        assert job_from_automation(_automation(priority=None), "r").priority == 5


# =============================================================================
# The admission task
# =============================================================================


def _session() -> dict[str, Any]:
    return {"id": UUID(SESSION), "provider_session_id": "rtc_1", "status": "ringing"}


class TestStartInbound:
    @pytest.mark.asyncio
    async def test_no_automation_rejects_with_480(self, store):
        dependencies = _dependencies(automation=None)
        await start_inbound(_session(), _connector(), dependencies)
        dependencies.db.live_automation_for_connector.assert_awaited_once_with(
            CONNECTOR
        )
        dependencies.pods.reject.assert_awaited_once_with(
            CONNECTOR, SESSION, "rtc_1", 480
        )
        store.transition.assert_awaited_once_with(
            UUID(SESSION), SessionStatus.REJECTED, close_reason="no_automation"
        )
        dependencies.db.claim_live_automation_fire.assert_not_awaited()
        dependencies.admit_as_owner.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_spent_daily_budget_rejects(self, store):
        dependencies = _dependencies(automation=_automation(), claimed=False)
        await start_inbound(_session(), _connector(), dependencies)
        dependencies.db.claim_live_automation_fire.assert_awaited_once_with(AUTOMATION)
        record = dependencies.db.record_live_automation_fire.await_args
        assert record.args == (AUTOMATION,)
        assert record.kwargs["job_id"] is None
        assert record.kwargs["status"].startswith("refused: daily fire limit")
        dependencies.pods.reject.assert_awaited_once_with(
            CONNECTOR, SESSION, "rtc_1", 480
        )
        store.transition.assert_awaited_once_with(
            UUID(SESSION), SessionStatus.REJECTED, close_reason="fire_limit"
        )
        dependencies.admit_as_owner.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_created_job_records_the_fire(self, store):
        dependencies = _dependencies(automation=_automation())
        await start_inbound(_session(), _connector(), dependencies)
        owner, command = dependencies.admit_as_owner.await_args.args
        assert owner == OWNER
        assert command.live_session is True
        assert command.execution.session.id == "rtc_1"
        assert command.context["automation_id"] == AUTOMATION
        dependencies.db.record_live_automation_fire.assert_awaited_once_with(
            AUTOMATION, job_id=JOB, status=None
        )
        # The created job row has no context: the fire names the automation.
        store.note_admission.assert_awaited_once_with(
            UUID(SESSION), automation_id=AUTOMATION
        )
        store.transition.assert_not_awaited()
        dependencies.pods.reject.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("detail", "reason"),
        [
            ({"code": "live_session_bound", "message": "boom"}, "boom"),
            ({"code": "only_code"}, "only_code"),
            ("plain boom", "plain boom"),
        ],
    )
    async def test_a_refused_admission_fails_the_session_with_its_reason(
        self, store, detail, reason
    ):
        dependencies = _dependencies(
            automation=_automation(),
            admit_as_owner=AsyncMock(side_effect=HTTPException(422, detail)),
        )
        await start_inbound(_session(), _connector(), dependencies)
        dependencies.db.record_live_automation_fire.assert_awaited_once_with(
            AUTOMATION, job_id=None, status=f"refused: {reason}"
        )
        dependencies.pods.reject.assert_awaited_once_with(
            CONNECTOR, SESSION, "rtc_1", 480
        )
        store.transition.assert_awaited_once_with(
            UUID(SESSION), SessionStatus.FAILED, error=f"admission refused: {reason}"
        )

    @pytest.mark.asyncio
    async def test_an_admission_crash_fails_the_session(self, store):
        dependencies = _dependencies(
            automation=_automation(),
            admit_as_owner=AsyncMock(side_effect=RuntimeError("kaput")),
        )
        await start_inbound(_session(), _connector(), dependencies)
        dependencies.db.record_live_automation_fire.assert_awaited_once_with(
            AUTOMATION, job_id=None, status="failed: RuntimeError"
        )
        store.transition.assert_awaited_once_with(
            UUID(SESSION),
            SessionStatus.FAILED,
            error="admission failed: RuntimeError: kaput",
        )

    @pytest.mark.asyncio
    async def test_an_unbuildable_job_fails_the_session(self, store):
        """A stored shape that no longer makes a job (no primary alias)."""
        automation = _automation(
            job_shape={"execution": {"connectors": {"tel": {"ref": {"name": "l"}}}}}
        )
        dependencies = _dependencies(automation=automation)
        await start_inbound(_session(), _connector(), dependencies)
        dependencies.admit_as_owner.assert_not_awaited()
        status, kwargs = (
            store.transition.await_args.args[1],
            store.transition.await_args.kwargs,
        )
        assert status == SessionStatus.FAILED
        assert kwargs["error"].startswith("admission failed: ValidationError")
        record = dependencies.db.record_live_automation_fire.await_args
        assert record.kwargs == {"job_id": None, "status": "failed: ValidationError"}

    @pytest.mark.asyncio
    async def test_a_pod_that_cannot_reject_still_settles_the_session(self, store):
        dependencies = _dependencies(
            automation=None,
            reject=AsyncMock(side_effect=LivePodError("not ready")),
        )
        await start_inbound(_session(), _connector(), dependencies)
        store.transition.assert_awaited_once_with(
            UUID(SESSION), SessionStatus.REJECTED, close_reason="no_automation"
        )

    @pytest.mark.asyncio
    async def test_the_fire_record_follows_the_job_id(self, store):
        dependencies = _dependencies(
            automation=_automation(),
            admit_as_owner=AsyncMock(return_value={"id": JOB, "status": "created"}),
        )
        await start_inbound(_session(), _connector(), dependencies)
        assert dependencies.db.record_live_automation_fire.await_args.kwargs == {
            "job_id": JOB,
            "status": None,
        }
