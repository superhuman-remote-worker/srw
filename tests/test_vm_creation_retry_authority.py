from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from orchestrator.routers import vm_creation_retry_authority as authority
from shared.vm_lifecycle_auth import (
    AUTH_FIELD,
    sign_payload,
    verify_payload,
    unsigned_payload,
)

SECRET = b"creation-issuance-test-secret-at-least-32-bytes"


def client(monkeypatch, store):
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())
    authority.configure(store_factory=lambda: store)
    app = FastAPI()
    app.include_router(authority.router)
    return TestClient(app)


def test_authority_refuses_unsigned_or_wrong_operation_without_store_call(monkeypatch):
    store = SimpleNamespace(authorize_controller=AsyncMock())
    test_client = client(monkeypatch, store)
    for value in [
        {},
        sign_payload({}, direction="request", operation="create", secret=SECRET),
    ]:
        response = test_client.post(
            "/api/internal/vm-creation-retries/authorize", json=value
        )
        assert response.status_code == 401
    store.authorize_controller.assert_not_awaited()


def test_authorize_is_reservation_only_and_correlated(monkeypatch):
    store = SimpleNamespace(
        authorize_controller=AsyncMock(
            return_value={"allowed": True, "admission_id": uuid4()}
        )
    )
    value = sign_payload(
        {"request_id": str(uuid4()), "claim_token": str(uuid4()), "observed": {}},
        direction="request",
        operation="creation_retry_authorize",
        secret=SECRET,
    )
    response = client(monkeypatch, store).post(
        "/api/internal/vm-creation-retries/authorize", json=value
    )
    assert response.status_code == 200
    assert verify_payload(
        response.json(),
        direction="response",
        operation="creation_retry_authorize",
        secret=SECRET,
        expected_correlation_id=value[AUTH_FIELD]["request_id"],
    )
    assert unsigned_payload(response.json())["actuation_allowed"] is False


@pytest.mark.parametrize(
    "path,operation,method",
    [
        ("begin-effect", "creation_retry_begin_effect", "begin_effect"),
        ("observe-effect", "creation_retry_observe_effect", "observe_effect"),
        ("inspect", "creation_retry_inspect", "inspect"),
        ("settle-adopted", "creation_retry_settle_adopted", "settle_adopted"),
        (
            "authorize-disposition",
            "creation_retry_authorize_disposition",
            "authorize_disposition",
        ),
        (
            "record-disposition",
            "creation_retry_record_disposition",
            "record_disposition",
        ),
        (
            "settle-disposition",
            "creation_retry_settle_disposition",
            "settle_disposition",
        ),
        (
            "prepare-disposition",
            "creation_retry_prepare_disposition",
            "prepare_disposition",
        ),
        (
            "freeze-disposition",
            "creation_retry_freeze_disposition",
            "freeze_disposition",
        ),
        (
            "settle-never-issued",
            "creation_retry_settle_never_issued",
            "settle_never_issued",
        ),
    ],
)
def test_each_effect_boundary_authenticates_its_own_operation(
    monkeypatch, path, operation, method
):
    action = AsyncMock(return_value={"actuation_allowed": False})
    store = SimpleNamespace(**{method: action})
    test_client = client(monkeypatch, store)
    assert (
        test_client.post(
            "/api/internal/vm-creation-retries/" + path, json={}
        ).status_code
        == 401
    )
    action.assert_not_awaited()
    payload = {"request_id": str(uuid4())}
    if method == "begin_effect":
        payload["claim_token"] = str(uuid4())
    value = sign_payload(
        payload, direction="request", operation=operation, secret=SECRET
    )
    response = test_client.post("/api/internal/vm-creation-retries/" + path, json=value)
    assert response.status_code == 200
    assert verify_payload(
        response.json(),
        direction="response",
        operation=operation,
        secret=SECRET,
        expected_correlation_id=value[AUTH_FIELD]["request_id"],
    )
    action.assert_awaited_once_with(**payload)


def test_store_unavailability_returns_only_a_signed_safe_reason(monkeypatch):
    store = SimpleNamespace(
        begin_effect=AsyncMock(side_effect=RuntimeError("credential-do-not-leak"))
    )
    value = sign_payload(
        {"request_id": str(uuid4()), "claim_token": str(uuid4()), "carrier": {}},
        direction="request",
        operation="creation_retry_begin_effect",
        secret=SECRET,
    )
    response = client(monkeypatch, store).post(
        "/api/internal/vm-creation-retries/begin-effect", json=value
    )
    assert response.status_code == 503
    assert "credential-do-not-leak" not in response.text
    assert verify_payload(
        response.json(),
        direction="response",
        operation="creation_retry_begin_effect",
        secret=SECRET,
        expected_correlation_id=value[AUTH_FIELD]["request_id"],
    )
