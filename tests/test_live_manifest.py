"""A live-session ``kind: Job`` manifest (live_session_jobs §3.3, §4.1).

The schema takes ``spec.liveSession`` and ``spec.execution.session``;
resolution refuses the contradictions (a live Job without a session, a
session on a Job that is not live, a session connector that is no key of
``execution.connectors``). Execution's ``_live_session`` applies the REST
admission's rules to the manifest's own connectors.
"""

from __future__ import annotations

import contextlib
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from orchestrator.services.live import admission as live_admission
from orchestrator.services.live.admission import LiveAdmission
from orchestrator.services.live.instructions import LiveVoice, LiveVoiceError
from orchestrator.services.manifest_execution import ManifestExecutionService
from shared.manifests import (
    API_VERSION,
    ManifestError,
    preview_documents,
    validate_documents,
)

ACCOUNT = {"kind": "Account", "name": "personal"}
LINE = "00000000-0000-4000-8000-00000000a1e1"


def resource(kind="Expert", name="worker", spec=None, scope=ACCOUNT):
    return {
        "apiVersion": API_VERSION,
        "kind": kind,
        "metadata": {"name": name, "scope": deepcopy(scope)},
        "spec": deepcopy(
            spec
            if spec is not None
            else {"runtime": {"image": "example/worker:1", "adapter": "srw/v1"}}
        ),
    }


def _line_connector() -> dict:
    return {"inline": {"driver": "srw.datasource/v1", "config": {"datasourceId": LINE}}}


def live_job(session: Any = ..., **spec: Any) -> dict:
    execution = {
        "expert": {"inline": resource()["spec"]},
        "connectors": {"telefon": _line_connector()},
    }
    if session is ...:
        session = {"connector": "telefon", "id": "rtc_123"}
    if session is not None:
        execution["session"] = session
    body = {"task": {"text": "Answer the call"}, "execution": execution}
    body.update(spec)
    return resource("Job", "call", body)


def error_code(code, operation, *args, **kwargs):
    with pytest.raises(ManifestError) as error:
        operation(*args, **kwargs)
    assert error.value.issue.code == code
    return error.value.issue


class TestSchema:
    def test_a_live_job_validates(self):
        job = live_job(liveSession=True)
        assert validate_documents([job]) == [job]

    @pytest.mark.parametrize(
        "session",
        [
            {"connector": "telefon"},
            {"id": "rtc_1"},
            {"connector": "telefon", "id": ""},
            {"connector": "telefon", "id": "rtc_1", "extra": True},
            {"connector": "Not A Name!", "id": "rtc_1"},
            "rtc_1",
        ],
    )
    def test_a_malformed_session_is_refused(self, session):
        with pytest.raises(ManifestError):
            validate_documents([live_job(session, liveSession=True)])

    @pytest.mark.parametrize("value", ["true", 1, None, {"on": True}])
    def test_live_session_is_a_boolean(self, value):
        with pytest.raises(ManifestError):
            validate_documents([live_job(liveSession=value)])


class TestResolution:
    def test_a_live_job_resolves_its_session(self):
        result = preview_documents([live_job(liveSession=True)])
        spec = result["resolved"][0]["spec"]
        assert spec["liveSession"] is True
        assert spec["execution"]["session"] == {
            "connector": "telefon",
            "id": "rtc_123",
        }
        assert "telefon" in spec["execution"]["connectors"]

    def test_a_live_job_without_a_session_is_refused(self):
        issue = error_code(
            "LiveSessionMismatch",
            preview_documents,
            [live_job(None, liveSession=True)],
        )
        assert issue.path.endswith("/execution/session")

    @pytest.mark.parametrize("spec", [{}, {"liveSession": False}])
    def test_a_session_on_a_job_that_is_not_live_is_refused(self, spec):
        error_code("LiveSessionMismatch", preview_documents, [live_job(**spec)])

    def test_a_job_that_is_neither_resolves(self):
        result = preview_documents([live_job(None, liveSession=False)])
        assert "session" not in result["resolved"][0]["spec"]["execution"]
        preview_documents([live_job(None)])

    def test_the_session_connector_is_a_connector_alias(self):
        issue = error_code(
            "LiveSessionConnectorUnknown",
            preview_documents,
            [live_job({"connector": "phone", "id": "rtc_1"}, liveSession=True)],
        )
        assert issue.path.endswith("/execution/session/connector")

    def test_a_job_without_connectors_has_no_session_connector(self):
        job = live_job(liveSession=True)
        del job["spec"]["execution"]["connectors"]
        error_code("LiveSessionConnectorUnknown", preview_documents, [job])


# =============================================================================
# ManifestExecutionService._live_session
# =============================================================================


class _Conn:
    def __init__(self, rows: list[Any]) -> None:
        self.fetchrow = AsyncMock(side_effect=rows)


class _DB:
    def __init__(self, connector: Any, rows: list[Any] | None = None) -> None:
        self.get_datasource = AsyncMock(return_value=connector)
        self.conn = _Conn(rows or [])

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self.conn


VOICE = LiveVoice(
    entry="voice",
    instructions="persona",
    greeting_instruction="Greet",
    voice="marin",
    fallback="Tell the caller",
)


def _session(**over: Any) -> dict[str, Any]:
    row = {
        "id": uuid4(),
        "connector_id": UUID(LINE),
        "provider_session_id": "rtc_123",
        "status": "ringing",
        "job_id": None,
    }
    row.update(over)
    return row


def _line(**over: Any) -> dict[str, Any]:
    row = {"id": LINE, "type": "gpt_live_sip", "config": {"model": "gpt-live-1"}}
    row.update(over)
    return row


EXECUTION = {
    "connectors": {"telefon": _line_connector()},
    "session": {"connector": "telefon", "id": "rtc_123"},
}


@pytest.fixture
def voice(monkeypatch):
    resolve = AsyncMock(return_value=VOICE)
    monkeypatch.setattr(live_admission, "resolve_live_voice", resolve)
    return resolve


async def _live_session(db, connector_id=LINE, datasource_ids=(LINE,)):
    return await ManifestExecutionService._live_session(
        SimpleNamespace(db=db),
        EXECUTION,
        connector_id,
        list(datasource_ids),
        config_name="receptionist",
        expert_row={"config": {"agent": {}}},
        config_override={"autonomy": "full"},
    )


class TestLiveSessionOfAManifestJob:
    @pytest.mark.asyncio
    async def test_a_new_session_is_created_for_the_manifest(self, voice):
        session = _session()
        db = _DB(_line(), [session])
        live = await _live_session(db)
        assert isinstance(live, LiveAdmission)
        assert live.session == session and live.voice is VOICE
        assert live.connector == _line()
        assert live.context_value == {
            "session_id": str(session["id"]),
            "connector_id": LINE,
            "provider_session_id": "rtc_123",
        }
        _sql, connector, provider_id, origin = db.conn.fetchrow.await_args.args
        assert (connector, provider_id, origin) == (UUID(LINE), "rtc_123", "manifest")
        assert voice.await_args.kwargs == {
            "config_name": "receptionist",
            "expert_id": None,
            "expert_row": {"config": {"agent": {}}},
            "config_override": {"autonomy": "full"},
        }

    @pytest.mark.asyncio
    async def test_an_active_session_named_first_by_the_webhook_is_taken(self, voice):
        session = _session(status="active")
        # The insert conflicts; the existing row is read.
        db = _DB(_line(), [None, session])
        live = await _live_session(db)
        assert live.session == session
        assert db.conn.fetchrow.await_count == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("connector_id", "datasource_ids"),
        [
            (None, (LINE,)),
            (LINE, ()),
            (LINE, ("00000000-0000-4000-8000-00000000a1e2",)),
        ],
    )
    async def test_the_session_connector_is_one_of_the_jobs(
        self, voice, connector_id, datasource_ids
    ):
        db = _DB(_line())
        with pytest.raises(HTTPException) as error:
            await _live_session(db, connector_id, datasource_ids)
        assert error.value.status_code == 422
        db.get_datasource.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "connector", [None, _line(type="generic"), _line(type="x")]
    )
    async def test_the_session_connector_is_a_live_connector(self, voice, connector):
        db = _DB(connector)
        with pytest.raises(HTTPException) as error:
            await _live_session(db)
        assert error.value.status_code == 422
        assert "not a live connector" in error.value.detail
        voice.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_expert_without_a_live_voice_is_refused(self, voice):
        voice.side_effect = LiveVoiceError("This expert has no live voice")
        db = _DB(_line())
        with pytest.raises(HTTPException) as error:
            await _live_session(db)
        assert error.value.status_code == 422
        assert error.value.detail == "This expert has no live voice"
        db.conn.fetchrow.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_bound_session_is_409(self, voice):
        db = _DB(_line(), [None, _session(job_id=uuid4())])
        with pytest.raises(HTTPException) as error:
            await _live_session(db)
        assert error.value.status_code == 409
        assert "already bound" in error.value.detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["ended", "rejected", "failed"])
    async def test_a_session_that_is_over_is_409(self, voice, status):
        db = _DB(_line(), [None, _session(status=status)])
        with pytest.raises(HTTPException) as error:
            await _live_session(db)
        assert error.value.status_code == 409
        assert status in error.value.detail


GENERIC_IMAGE = "example/x@sha256:" + "d" * 64


class TestAdmitRefusesALiveGenericImage:
    @pytest.mark.asyncio
    async def test_a_generic_image_cannot_take_a_live_session(self):
        user_id = str(uuid4())
        db = MagicMock()
        db.user_can_run_custom_harness_images = AsyncMock(return_value=True)
        service = ManifestExecutionService(
            db,
            runtime=MagicMock(),
            namespace="srw",
            native_hosting_enabled=True,
            approved_images=[{"name": "x", "image": GENERIC_IMAGE}],
            connector_drivers=MagicMock(),
        )
        prepared = {
            "resolved": {
                "metadata": {"scope": {"kind": "Account", "name": user_id}},
                "spec": {
                    "liveSession": True,
                    "execution": {
                        "expert": {"inline": {"runtime": {"image": GENERIC_IMAGE}}},
                        "connectors": {"telefon": _line_connector()},
                        "session": {"connector": "telefon", "id": "rtc_1"},
                    },
                },
            }
        }
        with pytest.raises(HTTPException) as error:
            await service.admit(prepared, {}, {"id": user_id})
        assert error.value.status_code == 422
        assert "SRW harness" in error.value.detail


INSTALLED_SRW_IMAGE = "example/srw@sha256:" + "e" * 64
CUSTOM_SRW_IMAGE = "example/custom@sha256:" + "c" * 64


def _srw_job_service(image, *, grant):
    user_id = str(uuid4())
    db = MagicMock()
    db.user_can_run_custom_harness_images = AsyncMock(return_value=grant)
    db.create_job = AsyncMock(return_value={"id": uuid4()})
    service = ManifestExecutionService(
        db,
        runtime=MagicMock(),
        namespace="srw",
        srw_image=INSTALLED_SRW_IMAGE,
        native_hosting_enabled=True,
        approved_images=[{"name": "cc", "image": CUSTOM_SRW_IMAGE}],
        connector_drivers=MagicMock(),
    )
    prepared = {
        "document": {},
        "revision": "r",
        "dependencies": [],
        "resolved": {
            "metadata": {"scope": {"kind": "Account", "name": user_id}},
            "spec": {
                "task": {"text": "go"},
                "completion": {"mode": "Reported"},
                "retry": {"maxAttempts": 1},
                "execution": {
                    "expert": {
                        "inline": {
                            "runtime": {
                                "adapter": "srw/v1",
                                "image": image,
                                "config": {"config_name": "worker_base"},
                            }
                        }
                    },
                    "connectors": {},
                    "workspace": None,
                },
            },
        },
    }
    return service, db, prepared, {"id": user_id}


def _srw_snapshot(monkeypatch):
    srw = {
        "resolved": {
            "spec": {
                "execution": {"expert": {"inline": {"runtime": {"config": {"x": 1}}}}}
            }
        }
    }
    monkeypatch.setattr(
        "orchestrator.services.manifest_execution_snapshot.prepare_srw_snapshot",
        AsyncMock(return_value=srw),
    )


class TestAdmitSrwJobOnAnApprovedImage:
    @pytest.mark.asyncio
    async def test_srw_job_on_an_approved_image_needs_the_grant(self, monkeypatch):
        _srw_snapshot(monkeypatch)
        service, db, prepared, user = _srw_job_service(CUSTOM_SRW_IMAGE, grant=False)
        with pytest.raises(HTTPException) as denied:
            await service.admit(prepared, {}, user)
        assert denied.value.status_code == 403
        assert "custom_harness_images" in denied.value.detail
        db.create_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_srw_job_on_an_approved_image_records_it_and_pins_the_lane(
        self, monkeypatch
    ):
        _srw_snapshot(monkeypatch)
        service, db, prepared, user = _srw_job_service(CUSTOM_SRW_IMAGE, grant=True)
        await service.admit(prepared, {}, user)
        kwargs = db.create_job.call_args.kwargs
        assert kwargs["harness_image"] == CUSTOM_SRW_IMAGE
        assert kwargs["execution_lane"] == "pinned"
        runtime = kwargs["execution_manifest"]["resolved"]["spec"]["execution"][
            "expert"
        ]["inline"]["runtime"]
        assert runtime["image"] == CUSTOM_SRW_IMAGE

    @pytest.mark.asyncio
    async def test_srw_job_on_the_installed_image_records_no_harness_image(
        self, monkeypatch
    ):
        _srw_snapshot(monkeypatch)
        service, db, prepared, user = _srw_job_service(INSTALLED_SRW_IMAGE, grant=False)
        await service.admit(prepared, {}, user)
        kwargs = db.create_job.call_args.kwargs
        assert kwargs["harness_image"] is None
        db.user_can_run_custom_harness_images.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_live_job_on_a_custom_srw_image_is_refused(self, monkeypatch):
        _srw_snapshot(monkeypatch)
        service, db, prepared, user = _srw_job_service(CUSTOM_SRW_IMAGE, grant=True)
        prepared["resolved"]["spec"]["liveSession"] = True
        with pytest.raises(HTTPException) as error:
            await service.admit(prepared, {}, user)
        assert error.value.status_code == 422
        assert "SRW harness" in error.value.detail
        db.create_job.assert_not_called()
