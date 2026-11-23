"""A ``kind: Job`` manifest that says whether it plans in phases (``spec.phases``).

The schema takes a boolean; resolution keeps the value as authored, adds none
when it is omitted, and refuses ``phases: false`` on a Job that is also a
live-session Job. Admission writes the value into the job's
``config_override`` (so it is frozen in the execution snapshot and stored on
the job) and refuses the key on a Job that does not run SRW's own worker.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.services.manifest_execution import ManifestExecutionService
from shared.job_phases import PHASES_KEY
from shared.manifests import (
    API_VERSION,
    ManifestError,
    load_schema,
    parse_documents,
    preview_documents,
    validate_documents,
)

ACCOUNT = {"kind": "Account", "name": "personal"}
LINE = "00000000-0000-4000-8000-00000000a1e1"
EXAMPLE = Path(__file__).resolve().parents[1] / "examples/manifests/phased-job.yaml"
VALUES = (True, False)


def resource(kind: str, name: str, spec: dict) -> dict:
    return {
        "apiVersion": API_VERSION,
        "kind": kind,
        "metadata": {"name": name, "scope": deepcopy(ACCOUNT)},
        "spec": deepcopy(spec),
    }


def job(**spec: Any) -> dict:
    body: dict[str, Any] = {
        "task": {"text": "Rename one heading"},
        "execution": {
            "expert": {
                "inline": {
                    "runtime": {
                        "adapter": "srw/v1",
                        "config": {"config_name": "general-worker"},
                    }
                }
            },
            "connectors": {},
        },
    }
    body.update(spec)
    return resource("Job", "small-task", body)


def live_job(**spec: Any) -> dict:
    document = job(liveSession=True, **spec)
    execution = document["spec"]["execution"]
    execution["connectors"] = {
        "telefon": {
            "inline": {
                "driver": "srw.datasource/v1",
                "config": {"datasourceId": LINE},
            }
        }
    }
    execution["session"] = {"connector": "telefon", "id": "rtc_123"}
    return document


def error_code(code, operation, *args, **kwargs):
    with pytest.raises(ManifestError) as error:
        operation(*args, **kwargs)
    assert error.value.issue.code == code
    return error.value.issue


class TestSchema:
    @pytest.mark.parametrize("value", VALUES)
    def test_both_values_validate(self, value):
        document = job(phases=value)
        assert validate_documents([document]) == [document]

    def test_the_schema_takes_a_boolean_without_a_default(self):
        prop = load_schema()["$defs"]["JobSpec"]["properties"]["phases"]
        assert prop["type"] == "boolean"
        # Omitted means the Expert's own setting, decided at admission.
        assert "default" not in prop

    @pytest.mark.parametrize("value", ["on", "true", "agent", "", 1, 0, None, ["true"]])
    def test_anything_else_is_refused(self, value):
        with pytest.raises(ManifestError):
            validate_documents([job(phases=value)])

    def test_the_setting_is_not_an_execution_brick(self):
        document = job()
        document["spec"]["execution"]["phases"] = False
        with pytest.raises(ManifestError):
            validate_documents([document])

    def test_phases_is_the_only_key_that_chooses_how_the_worker_runs(self):
        job_spec = load_schema()["$defs"]["JobSpec"]
        assert job_spec["additionalProperties"] is False
        assert set(job_spec["properties"]) == {
            "task",
            "execution",
            "completion",
            "retry",
            "timeoutSeconds",
            "liveSession",
            "phases",
        }


class TestResolution:
    @pytest.mark.parametrize("value", VALUES)
    def test_a_job_keeps_its_value(self, value):
        result = preview_documents([job(phases=value)])
        assert result["resolved"][0]["spec"]["phases"] is value

    def test_an_omitted_value_stays_omitted(self):
        # Resolution writes no default: the Expert's own setting, and then
        # no phases, apply at admission.
        result = preview_documents([job()])
        assert "phases" not in result["resolved"][0]["spec"]

    def test_no_phases_with_a_live_session_is_refused(self):
        issue = error_code(
            "NoPhasesLiveSession", preview_documents, [live_job(phases=False)]
        )
        assert issue.path.endswith("/phases")

    def test_phases_with_a_live_session_resolves(self):
        result = preview_documents([live_job(phases=True)])
        spec = result["resolved"][0]["spec"]
        assert spec["phases"] is True and spec["liveSession"] is True

    def test_an_omitted_value_with_a_live_session_resolves(self):
        result = preview_documents([live_job()])
        spec = result["resolved"][0]["spec"]
        assert "phases" not in spec and spec["liveSession"] is True

    def test_the_published_example_is_a_phased_job(self):
        documents = parse_documents(EXAMPLE.read_text(encoding="utf-8"))
        result = preview_documents(documents)
        (resolved,) = result["resolved"]
        assert resolved["kind"] == "Job"
        assert resolved["spec"]["phases"] is True
        assert "liveSession" not in resolved["spec"]
        runtime = resolved["spec"]["execution"]["expert"]["inline"]["runtime"]
        assert runtime["adapter"] == "srw/v1"
        # An Expert that does not plan in phases by itself: the Job turns it on.
        assert runtime["config"]["config_name"] == "engineer"


# =============================================================================
# ManifestExecutionService.admit
# =============================================================================

INSTALLED_SRW_IMAGE = "example/srw@sha256:" + "e" * 64
CUSTOM_SRW_IMAGE = "example/custom@sha256:" + "c" * 64
GENERIC_IMAGE = "example/x@sha256:" + "d" * 64


def _srw_job_service(
    image=None, *, phases=None, grant=True, live=False, config_name="general-worker"
):
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
        authorize_datasources=AsyncMock(return_value=([LINE], {})),
    )
    runtime: dict[str, Any] = {
        "adapter": "srw/v1",
        "config": {"config_name": config_name},
    }
    if image is not None:
        runtime["image"] = image
    spec: dict[str, Any] = {
        "task": {"text": "go"},
        "completion": {"mode": "Reported"},
        "retry": {"maxAttempts": 1},
        "execution": {
            "expert": {"inline": {"runtime": runtime}},
            "connectors": {},
            "workspace": None,
        },
    }
    if phases is not None:
        spec["phases"] = phases
    if live:
        spec["liveSession"] = True
        spec["execution"]["connectors"] = live_job()["spec"]["execution"]["connectors"]
        spec["execution"]["session"] = {"connector": "telefon", "id": "rtc_123"}
    prepared = {
        "document": {},
        "revision": "r",
        "dependencies": [],
        "resolved": {
            "metadata": {"scope": {"kind": "Account", "name": user_id}},
            "spec": spec,
        },
    }
    return service, db, prepared, {"id": user_id}


@pytest.fixture
def snapshot(monkeypatch):
    prepare = AsyncMock(
        return_value={
            "resolved": {
                "spec": {
                    "execution": {
                        "expert": {"inline": {"runtime": {"config": {"x": 1}}}}
                    }
                }
            }
        }
    )
    monkeypatch.setattr(
        "orchestrator.services.manifest_execution_snapshot.prepare_srw_snapshot",
        prepare,
    )
    return prepare


class _Stop(Exception):
    pass


class TestAdmit:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", VALUES)
    async def test_the_value_is_frozen_and_stored_with_the_job(self, snapshot, value):
        service, db, prepared, user = _srw_job_service(phases=value)
        await service.admit(prepared, {}, user)
        frozen = snapshot.await_args.kwargs["config_override"]
        stored = db.create_job.call_args.kwargs["config_override"]
        assert frozen[PHASES_KEY] is value
        assert stored[PHASES_KEY] is value
        # The workspace binding admission writes is still there.
        assert "workspace" in stored

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "config_name,expected", [("general-worker", False), ("developer", True)]
    )
    async def test_an_omitted_value_writes_the_experts_effective_value(
        self, snapshot, config_name, expected
    ):
        # As in REST admission (job_admission.resolved_phases_override): every
        # Job on SRW's own worker states its value, frozen and stored.
        service, db, prepared, user = _srw_job_service(config_name=config_name)
        await service.admit(prepared, {}, user)
        assert snapshot.await_args.kwargs["config_override"][PHASES_KEY] is expected
        stored = db.create_job.call_args.kwargs["config_override"]
        assert stored[PHASES_KEY] is expected
        assert "workspace" in stored

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", VALUES)
    async def test_the_jobs_own_value_wins_over_the_experts(self, snapshot, value):
        for config_name in ("general-worker", "developer"):
            service, db, prepared, user = _srw_job_service(
                phases=value, config_name=config_name
            )
            await service.admit(prepared, {}, user)
            stored = db.create_job.call_args.kwargs["config_override"]
            assert stored[PHASES_KEY] is value, config_name

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", VALUES)
    async def test_a_custom_srw_image_cannot_take_the_setting(self, snapshot, value):
        service, db, prepared, user = _srw_job_service(CUSTOM_SRW_IMAGE, phases=value)
        with pytest.raises(HTTPException) as error:
            await service.admit(prepared, {}, user)
        assert error.value.status_code == 422
        assert "Remove phases" in error.value.detail
        db.create_job.assert_not_called()
        snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_custom_srw_image_without_the_setting_is_admitted(self, snapshot):
        service, db, prepared, user = _srw_job_service(CUSTOM_SRW_IMAGE)
        await service.admit(prepared, {}, user)
        db.create_job.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_generic_image_cannot_take_the_setting(self):
        user_id = str(uuid4())
        db = MagicMock()
        db.user_can_run_custom_harness_images = AsyncMock(return_value=True)
        db.create_job = AsyncMock()
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
                    "phases": False,
                    "execution": {
                        "expert": {"inline": {"runtime": {"image": GENERIC_IMAGE}}},
                        "connectors": {},
                    },
                },
            }
        }
        with pytest.raises(HTTPException) as error:
            await service.admit(prepared, {}, {"id": user_id})
        assert error.value.status_code == 422
        assert "generic or custom image" in error.value.detail
        db.create_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_live_session_job_with_phases_carries_them(
        self, snapshot, monkeypatch
    ):
        seen: dict = {}

        async def live_session(self, *args, config_override, **kwargs):
            seen["override"] = deepcopy(config_override)
            raise _Stop

        monkeypatch.setattr(ManifestExecutionService, "_live_session", live_session)
        service, db, prepared, user = _srw_job_service(phases=True, live=True)
        with pytest.raises(_Stop):
            await service.admit(prepared, {}, user)
        assert seen["override"][PHASES_KEY] is True
        assert snapshot.await_args.kwargs["config_override"][PHASES_KEY] is True

    @pytest.mark.asyncio
    async def test_a_live_session_job_that_omits_the_setting_plans_in_phases(
        self, snapshot, monkeypatch
    ):
        seen: dict = {}

        async def live_session(self, *args, config_override, **kwargs):
            seen["override"] = deepcopy(config_override)
            raise _Stop

        monkeypatch.setattr(ManifestExecutionService, "_live_session", live_session)
        service, db, prepared, user = _srw_job_service(live=True)
        with pytest.raises(_Stop):
            await service.admit(prepared, {}, user)
        assert snapshot.await_args.kwargs["config_override"].get(PHASES_KEY) is True
        assert seen["override"].get(PHASES_KEY) is True
