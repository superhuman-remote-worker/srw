from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from orchestrator.routers import vm_workspace_cleanup_authority as authority
from orchestrator.services.vm_lifecycle_auth import (
    AUTH_FIELD,
    sign_payload,
    unsigned_payload,
    verify_payload,
)


SECRET = b"workspace-cleanup-authority-test-secret"


def _client(monkeypatch, store) -> TestClient:
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())
    monkeypatch.setattr(authority, "_store_factory", lambda: store)
    app = FastAPI()
    app.include_router(authority.router)
    return TestClient(app)


def _signed(payload: dict, *, operation: str) -> tuple[dict, str]:
    value = sign_payload(
        payload, direction="request", operation=operation, secret=SECRET
    )
    return value, value[AUTH_FIELD]["request_id"]


@pytest.fixture(params=("acquire", "complete", "resume"))
def cleanup_request(request):
    endpoint = request.param
    payload = {"request_id": "00000000-0000-4000-8000-000000000925"}
    if endpoint in {"acquire", "resume"}:
        payload.update(
            source="controller_rootdisk_delete",
            owner_kind="job",
            owner_id="00000000-0000-4000-8000-000000000922",
        )
    if endpoint == "acquire":
        payload.update(
            pvc_uid="00000000-0000-4000-8000-000000000923",
            dv_uid="dv-uid",
            provision_generation="00000000-0000-4000-8000-000000000924",
        )
    else:
        payload.update(
            admission_id="00000000-0000-4000-8000-000000000921",
            intent_digest="sha256:exact-cleanup-intent",
        )
    if endpoint == "complete":
        payload["outcome"] = "deleted"
    return endpoint, payload


@pytest.mark.parametrize("error_type", (KeyError, TypeError, ValueError, RuntimeError))
def test_store_errors_do_not_disclose_exception_text(
    monkeypatch, caplog, cleanup_request, error_type
) -> None:
    endpoint, payload = cleanup_request
    operation = f"recovery-cleanup-{endpoint}"
    internal_detail = "synthetic internal failure at /srv/private/cleanup.sql"
    error = error_type(internal_detail)
    store_call = AsyncMock(side_effect=error)
    store = SimpleNamespace(**{f"{endpoint}_cleanup_permit": store_call})
    client = _client(monkeypatch, store)
    request, correlation_id = _signed(payload, operation=operation)

    response = client.post(
        f"/api/internal/vm-workspace-cleanup-authority/{endpoint}", json=request
    )

    store_call.assert_awaited_once()
    assert response.status_code == (503 if error_type is RuntimeError else 400)
    assert internal_detail not in response.text
    assert "Traceback" not in response.text
    value = response.json()
    assert verify_payload(
        value,
        direction="response",
        operation=operation,
        secret=SECRET,
        expected_correlation_id=correlation_id,
    )
    result_key = "completed" if endpoint == "complete" else "allowed"
    assert unsigned_payload(value) == {
        result_key: False,
        "reason": (
            "cleanup authority unavailable"
            if error_type is RuntimeError
            else "invalid cleanup authority request"
        ),
    }
    if error_type is not RuntimeError:
        assert any(
            record.exc_info is not None
            and record.exc_info[1] is error
            and operation in record.getMessage()
            and correlation_id in record.getMessage()
            for record in caplog.records
        )


def test_malformed_request_returns_signed_safe_reason(
    monkeypatch, cleanup_request
) -> None:
    endpoint, payload = cleanup_request
    operation = f"recovery-cleanup-{endpoint}"
    payload["request_id"] = "invalid-request-id"
    store_call = AsyncMock()
    store = SimpleNamespace(**{f"{endpoint}_cleanup_permit": store_call})
    client = _client(monkeypatch, store)
    request, correlation_id = _signed(payload, operation=operation)

    response = client.post(
        f"/api/internal/vm-workspace-cleanup-authority/{endpoint}", json=request
    )

    store_call.assert_not_awaited()
    assert response.status_code == 400
    value = response.json()
    assert verify_payload(
        value,
        direction="response",
        operation=operation,
        secret=SECRET,
        expected_correlation_id=correlation_id,
    )
    result_key = "completed" if endpoint == "complete" else "allowed"
    assert unsigned_payload(value) == {
        result_key: False,
        "reason": "invalid cleanup authority request",
    }


def test_unsigned_request_is_rejected_before_store_access(
    monkeypatch, cleanup_request
) -> None:
    endpoint, payload = cleanup_request
    store_call = AsyncMock()
    store = SimpleNamespace(**{f"{endpoint}_cleanup_permit": store_call})
    client = _client(monkeypatch, store)

    response = client.post(
        f"/api/internal/vm-workspace-cleanup-authority/{endpoint}", json=payload
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "unauthenticated"}
    store_call.assert_not_awaited()


def test_acquire_is_authenticated_and_revalidates_completed_replay(monkeypatch) -> None:
    admission_id = UUID("00000000-0000-4000-8000-000000000921")
    store = SimpleNamespace(
        acquire_cleanup_permit=AsyncMock(
            return_value=SimpleNamespace(
                allowed=True,
                admission_id=admission_id,
                recovery_id=None,
                reason=None,
                completed_outcome=None,
            )
        )
    )
    client = _client(monkeypatch, store)
    request, correlation_id = _signed(
        {
            "source": "controller_rootdisk_delete",
            "owner_kind": "job",
            "owner_id": "00000000-0000-4000-8000-000000000922",
            "pvc_uid": "00000000-0000-4000-8000-000000000923",
            "dv_uid": "dv-uid",
            "provision_generation": "00000000-0000-4000-8000-000000000924",
            "request_id": "00000000-0000-4000-8000-000000000925",
        },
        operation="recovery-cleanup-acquire",
    )

    response = client.post(
        "/api/internal/vm-workspace-cleanup-authority/acquire", json=request
    )

    assert response.status_code == 200
    value = response.json()
    assert verify_payload(
        value,
        direction="response",
        operation="recovery-cleanup-acquire",
        secret=SECRET,
        expected_correlation_id=correlation_id,
    )
    response_payload = unsigned_payload(value)
    assert response_payload["admission_id"] == str(admission_id)
    assert response_payload["request_id"] == ("00000000-0000-4000-8000-000000000925")
    assert (
        response_payload["intent_digest"]
        == (store.acquire_cleanup_permit.await_args.kwargs["intent_digest"])
    )
    assert store.acquire_cleanup_permit.await_args.kwargs["revalidate_completed"]


def test_acquire_forwards_authenticated_parent_and_captured_vm(monkeypatch) -> None:
    store = SimpleNamespace(
        acquire_cleanup_permit=AsyncMock(
            return_value=SimpleNamespace(
                allowed=False,
                admission_id=None,
                recovery_id=None,
                reason="parent_cleanup_identity_changed",
                completed_outcome=None,
            )
        )
    )
    proof = {"admission_id": "original-parent", "intent_digest": "sha256:original"}
    generation = "00000000-0000-4000-8000-000000000924"
    request, _ = _signed(
        {
            "source": "controller_rootdisk_delete",
            "owner_kind": "job",
            "owner_id": "00000000-0000-4000-8000-000000000922",
            "pvc_uid": "00000000-0000-4000-8000-000000000923",
            "dv_uid": "dv-uid",
            "provision_generation": generation,
            "request_id": "00000000-0000-4000-8000-000000000925",
            "parent_cleanup": proof,
            "parent_provision_generation": generation,
            "expected_vm_uid": "captured-vm",
        },
        operation="recovery-cleanup-acquire",
    )
    response = _client(monkeypatch, store).post(
        "/api/internal/vm-workspace-cleanup-authority/acquire", json=request
    )
    assert response.status_code == 200
    assert unsigned_payload(response.json())["allowed"] is False
    forwarded = store.acquire_cleanup_permit.await_args.kwargs
    assert forwarded["parent_cleanup"] == proof
    assert forwarded["parent_provision_generation"] == generation
    assert forwarded["expected_vm_uid"] == "captured-vm"


def test_acquire_fails_closed_with_signed_unavailable_response(monkeypatch) -> None:
    store = SimpleNamespace(
        acquire_cleanup_permit=AsyncMock(side_effect=RuntimeError("database down"))
    )
    client = _client(monkeypatch, store)
    request, correlation_id = _signed(
        {
            "source": "controller_failed_dv_recreate",
            "owner_kind": "job",
            "owner_id": "00000000-0000-4000-8000-000000000926",
            "pvc_uid": "00000000-0000-4000-8000-000000000927",
            "dv_uid": "dv-uid",
            "provision_generation": "00000000-0000-4000-8000-000000000928",
            "request_id": "00000000-0000-4000-8000-000000000929",
        },
        operation="recovery-cleanup-acquire",
    )

    response = client.post(
        "/api/internal/vm-workspace-cleanup-authority/acquire", json=request
    )

    assert response.status_code == 503
    value = response.json()
    assert verify_payload(
        value,
        direction="response",
        operation="recovery-cleanup-acquire",
        secret=SECRET,
        expected_correlation_id=correlation_id,
    )
    assert unsigned_payload(value) == {
        "allowed": False,
        "reason": "cleanup authority unavailable",
    }


def test_resume_and_complete_bind_exact_permit_identity(monkeypatch) -> None:
    admission_id = UUID("00000000-0000-4000-8000-000000000930")
    request_id = UUID("00000000-0000-4000-8000-000000000931")
    digest = "sha256:exact-cleanup-intent"
    store = SimpleNamespace(
        resume_cleanup_permit=AsyncMock(
            return_value=SimpleNamespace(
                allowed=True,
                admission_id=admission_id,
                reason=None,
                completed_outcome=None,
            )
        ),
        complete_cleanup_permit=AsyncMock(return_value=True),
    )
    client = _client(monkeypatch, store)

    resume_request, _ = _signed(
        {
            "admission_id": str(admission_id),
            "request_id": str(request_id),
            "intent_digest": digest,
            "source": "controller_failed_dv_recreate",
            "owner_kind": "job",
            "owner_id": "00000000-0000-4000-8000-000000000932",
        },
        operation="recovery-cleanup-resume",
    )
    resume_response = client.post(
        "/api/internal/vm-workspace-cleanup-authority/resume", json=resume_request
    )

    assert resume_response.status_code == 200
    assert store.resume_cleanup_permit.await_args.kwargs["request_id"] == request_id
    assert store.resume_cleanup_permit.await_args.kwargs["intent_digest"] == digest

    complete_request, _ = _signed(
        {
            "admission_id": str(admission_id),
            "request_id": str(request_id),
            "intent_digest": digest,
            "outcome": "recreated",
        },
        operation="recovery-cleanup-complete",
    )
    complete_response = client.post(
        "/api/internal/vm-workspace-cleanup-authority/complete",
        json=complete_request,
    )

    assert complete_response.status_code == 200
    store.complete_cleanup_permit.assert_awaited_once_with(
        admission_id,
        outcome="recreated",
        request_id=request_id,
        intent_digest=digest,
    )


def test_legacy_authority_rejects_dedicated_disposition_source_before_store(
    monkeypatch,
):
    from uuid import uuid4

    store = SimpleNamespace(resume_cleanup_permit=AsyncMock())
    client = _client(monkeypatch, store)
    monkeypatch.setattr(
        authority,
        "_SOURCES",
        authority._SOURCES - {"controller_creation_rootdisk_delete"},
    )
    payload, correlation = _signed(
        {
            "admission_id": str(uuid4()),
            "request_id": str(uuid4()),
            "owner_kind": "job",
            "owner_id": str(uuid4()),
            "source": "controller_creation_rootdisk_delete",
            "intent_digest": "sha256:" + "0" * 64,
        },
        operation="recovery-cleanup-resume",
    )
    result = client.post(
        "/api/internal/vm-workspace-cleanup-authority/resume", json=payload
    )
    assert result.status_code == 400
    assert verify_payload(
        result.json(),
        direction="response",
        operation="recovery-cleanup-resume",
        secret=SECRET,
        expected_correlation_id=correlation,
    )
    assert unsigned_payload(result.json())["allowed"] is False
    store.resume_cleanup_permit.assert_not_awaited()
