"""A ``kind: Job`` manifest that chooses its job mode (``spec.jobMode``).

The schema takes ``worker`` or ``agent``; resolution keeps the value as
authored, adds none when it is omitted, and refuses an agent-mode Job that is
also a live-session Job. Admission writes the mode into the job's
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
from shared.job_mode import JOB_MODE_KEY, JOB_MODES
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
EXAMPLE = Path(__file__).resolve().parents[1] / "examples/manifests/agent-mode-job.yaml"


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
    @pytest.mark.parametrize("mode", JOB_MODES)
    def test_both_modes_validate(self, mode):
        document = job(jobMode=mode)
        assert validate_documents([document]) == [document]

    def test_the_schema_names_the_shared_vocabulary(self):
        prop = load_schema()["$defs"]["JobSpec"]["properties"]["jobMode"]
        assert tuple(prop["enum"]) == JOB_MODES
        assert prop["default"] == "worker"

    @pytest.mark.parametrize("value", ["Agent", "light", "", True, None, ["agent"]])
    def test_anything_else_is_refused(self, value):
        with pytest.raises(ManifestError):
            validate_documents([job(jobMode=value)])

    def test_the_mode_is_not_an_execution_brick(self):
        document = job()
        document["spec"]["execution"]["jobMode"] = "agent"
        with pytest.raises(ManifestError):
            validate_documents([document])


class TestResolution:
    def test_an_agent_mode_job_keeps_its_mode(self):
        result = preview_documents([job(jobMode="agent")])
        assert result["resolved"][0]["spec"]["jobMode"] == "agent"

    def test_an_omitted_mode_stays_omitted(self):
        # Resolution writes no default: the Expert's own setting, and then
        # worker mode, apply at admission.
        result = preview_documents([job()])
        assert "jobMode" not in result["resolved"][0]["spec"]

    def test_agent_mode_with_a_live_session_is_refused(self):
        issue = error_code(
            "AgentModeLiveSession", preview_documents, [live_job(jobMode="agent")]
        )
        assert issue.path.endswith("/jobMode")

    def test_worker_mode_with_a_live_session_resolves(self):
        result = preview_documents([live_job(jobMode="worker")])
        spec = result["resolved"][0]["spec"]
        assert spec["jobMode"] == "worker" and spec["liveSession"] is True

    def test_the_published_example_is_an_agent_mode_job(self):
        documents = parse_documents(EXAMPLE.read_text(encoding="utf-8"))
        result = preview_documents(documents)
        (resolved,) = result["resolved"]
        assert resolved["kind"] == "Job"
        assert resolved["spec"]["jobMode"] == "agent"
        assert "liveSession" not in resolved["spec"]
        runtime = resolved["spec"]["execution"]["expert"]["inline"]["runtime"]
        assert runtime["adapter"] == "srw/v1"


# =============================================================================
# ManifestExecutionService.admit
# =============================================================================

INSTALLED_SRW_IMAGE = "example/srw@sha256:" + "e" * 64
CUSTOM_SRW_IMAGE = "example/custom@sha256:" + "c" * 64
GENERIC_IMAGE = "example/x@sha256:" + "d" * 64


def _srw_job_service(image=None, *, job_mode=None, grant=True):
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
    runtime: dict[str, Any] = {
        "adapter": "srw/v1",
        "config": {"config_name": "general-worker"},
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
    if job_mode is not None:
        spec["jobMode"] = job_mode
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


class TestAdmit:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", JOB_MODES)
    async def test_the_mode_is_frozen_and_stored_with_the_job(self, snapshot, mode):
        service, db, prepared, user = _srw_job_service(job_mode=mode)
        await service.admit(prepared, {}, user)
        frozen = snapshot.await_args.kwargs["config_override"]
        stored = db.create_job.call_args.kwargs["config_override"]
        assert frozen[JOB_MODE_KEY] == mode
        assert stored[JOB_MODE_KEY] == mode
        # The workspace binding admission writes is still there.
        assert "workspace" in stored

    @pytest.mark.asyncio
    async def test_an_omitted_mode_writes_no_key(self, snapshot):
        service, db, prepared, user = _srw_job_service()
        await service.admit(prepared, {}, user)
        assert JOB_MODE_KEY not in snapshot.await_args.kwargs["config_override"]
        assert JOB_MODE_KEY not in db.create_job.call_args.kwargs["config_override"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", JOB_MODES)
    async def test_a_custom_srw_image_cannot_take_a_mode(self, snapshot, mode):
        service, db, prepared, user = _srw_job_service(CUSTOM_SRW_IMAGE, job_mode=mode)
        with pytest.raises(HTTPException) as error:
            await service.admit(prepared, {}, user)
        assert error.value.status_code == 422
        assert "jobMode" in error.value.detail
        db.create_job.assert_not_called()
        snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_custom_srw_image_without_a_mode_is_admitted(self, snapshot):
        service, db, prepared, user = _srw_job_service(CUSTOM_SRW_IMAGE)
        await service.admit(prepared, {}, user)
        db.create_job.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_generic_image_cannot_take_a_mode(self):
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
                    "jobMode": "agent",
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
