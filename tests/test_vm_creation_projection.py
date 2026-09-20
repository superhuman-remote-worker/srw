"""Creation progress is useful publicly without exposing runtime authority."""

from copy import deepcopy
import json
from uuid import uuid4

import pytest

from tests.test_job_projection import redact


def progress(**changes):
    return {
        "request_id": str(uuid4()),
        "stage": "creation",
        "state": "attention",
        "reason": "vm_creation_retry_blocked",
        "admission_deadline": None,
        "ready_at": None,
        "pending": True,
        "resume_blocked": False,
        "raw_error": "SECRET raw controller body",
        "host": "10.42.0.19",
        **changes,
    }


@pytest.mark.parametrize(
    "reason,words",
    [
        ("capacity_wait", "capacity"),
        ("golden_wait", "disk"),
        ("preparation_wait", "preparation"),
        ("headscale_wait", "network"),
        ("disk_wait", "disk"),
        ("creation_observation_pending", "verifying"),
        ("controller_unavailable", "controller"),
        ("job_admission_expired", "deadline"),
        ("execution_manifest_changed", "execution"),
    ],
)
def test_bounded_progress_and_safe_resume_advice(monkeypatch, reason, words):
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    source = {
        "status": "paused",
        "_vm_creation": progress(reason=reason),
        "context": {
            "_vm_creation_pending": "private",
            "vm": {"ssh_host": "10.42.0.19"},
            "last_vm": {"cloud_init_secret": "SECRET"},
        },
    }
    before = deepcopy(source)
    result = redact(source)
    assert words in result["vm_creation"]["message"].lower()
    assert result["vm_creation"]["reason_code"] == reason
    assert "_vm_creation" not in result
    assert "_vm_creation_pending" not in result["context"]
    assert "SECRET" not in json.dumps(result)
    assert "10.42.0.19" not in json.dumps(result)
    assert source == before
    if reason in {"job_admission_expired", "execution_manifest_changed"}:
        assert result["vm_creation"]["resumable"] is False


@pytest.mark.parametrize(
    "change",
    [
        {"admission_deadline": "2001-01-01T00:00:00Z"},
        {"admission_deadline": "garbage"},
        {"admission_deadline": True},
        {"pending": False},
        {"resume_blocked": True},
        {"reason": "SECRET raw body"},
        {"state": "reconciling"},
    ],
)
def test_advice_never_offers_resume_with_missing_or_expired_authority(
    monkeypatch, change
):
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    result = redact({"status": "paused", "_vm_creation": progress(**change)})
    assert result["vm_creation"]["resumable"] is False
    assert "SECRET" not in json.dumps(result)


def test_creation_attention_is_visible_only_with_enabled_admission(monkeypatch):
    source = {"status": "paused", "_vm_creation": progress()}
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    assert redact(source)["vm_creation"]["resumable"] is True
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "false")
    assert redact(source)["vm_creation"]["resumable"] is False


def test_preflight_progress_is_projected_without_its_request_or_endpoint(monkeypatch):
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    request_id = str(uuid4())
    raw = {
        "vm": {
            "status": "waiting_creation_configuration",
            "creation_preflight": {
                "request_id": request_id,
                "state": "attention",
                "reason": "creation_configuration_unproven",
                "admission_deadline": None,
                "request": {"ssh_host": "10.0.0.1"},
            },
        },
        "_vm_creation_pending": request_id,
    }
    result = redact({"status": "paused", "context": json.dumps(raw)})
    assert result["vm_creation"]["stage"] == "configuration"
    assert result["vm_creation"]["resumable"] is True
    assert result["vm_creation"]["request_id"] == request_id
    assert json.loads(result["context"]) == {}


def test_finished_creation_disappears_but_adopted_not_ready_remains_visible():
    source = {
        "status": "paused",
        "_vm_creation": progress(state="succeeded", reason="creation_adopted"),
    }
    result = redact(source)
    assert result["vm_creation"]["stage"] == "readiness"
    assert result["vm_creation"]["resumable"] is False
    source["_vm_creation"]["ready_at"] = "2026-09-20T00:00:00Z"
    assert redact(source).get("vm_creation") is None


def test_progress_does_not_replace_existing_failure_or_cleanup():
    source = {
        "status": "paused",
        "error_message": "Original failure",
        "_vm_creation": progress(),
        "context": {"vm": {"status": "retiring_process_zero"}},
    }
    assert redact(source)["error_message"] == "Original failure"
    source.pop("error_message")
    assert "cleanup" in redact(source)["error_message"].lower()


def test_explicit_cleanup_marker_does_not_require_status():
    result = redact(
        {
            "status": "paused",
            "_vm_creation": progress(),
            "context": {"vm": {"retirement_cleanup_pending": True}},
        }
    )
    assert "cleanup" in result["error_message"].lower()


def test_plain_job_keeps_existing_public_shape():
    assert redact({"status": "created"})["vm_creation"] is None


@pytest.mark.parametrize("field", ["state", "stage"])
def test_malformed_progress_cannot_crash_public_projection(field):
    result = redact({"status": "paused", "_vm_creation": progress(**{field: []})})
    assert "_vm_creation" not in result
