"""The dedicated cancellation route authenticates before any controller work."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.test_vm_creation_replay_transport import replay_fixture, SECRET
from shared.vm_creation_disposition import disposition_identity
from shared.vm_lifecycle_auth import sign_payload
from vm_controller import controller as settings


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", [None, "create", "creation_retry_create"])
async def test_dispose_endpoint_refuses_other_purposes_without_authority_or_kubernetes(
    monkeypatch, purpose
):
    ctrl = settings.VMController.__new__(settings.VMController)
    monkeypatch.setattr(settings, "LIFECYCLE_HMAC_SECRET", SECRET)
    ctrl._workspace_cleanup_authority_request = AsyncMock(
        side_effect=AssertionError("no authority")
    )
    ctrl._claim_lifecycle_nonce = AsyncMock(side_effect=AssertionError("no nonce"))
    row, _ = replay_fixture()
    payload = disposition_identity(row)
    if purpose:
        payload = sign_payload(
            payload, direction="request", operation=purpose, secret=SECRET
        )
    response = await ctrl.http_creation_dispose(
        SimpleNamespace(json=AsyncMock(return_value=payload))
    )
    assert response.status == 401
    ctrl._workspace_cleanup_authority_request.assert_not_awaited()
    ctrl._claim_lifecycle_nonce.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispose_reply_is_signed_and_correlated_without_leaking_failure(
    monkeypatch,
):
    import json
    from shared.vm_lifecycle_auth import AUTH_FIELD, verify_payload, unsigned_payload

    ctrl = settings.VMController.__new__(settings.VMController)
    monkeypatch.setattr(settings, "LIFECYCLE_HMAC_SECRET", SECRET)
    ctrl._claim_lifecycle_nonce = AsyncMock(return_value=True)
    ctrl._workspace_cleanup_authority_request = AsyncMock(
        side_effect=RuntimeError("private authority token")
    )
    row, _ = replay_fixture()
    identity = disposition_identity(row)
    payload = sign_payload(
        identity, direction="request", operation="creation_retry_dispose", secret=SECRET
    )
    response = await ctrl.http_creation_dispose(
        SimpleNamespace(json=AsyncMock(return_value=payload))
    )
    assert response.status == 200
    reply = json.loads(response.text)
    assert verify_payload(
        reply,
        direction="response",
        operation="creation_retry_dispose",
        secret=SECRET,
        expected_correlation_id=payload[AUTH_FIELD]["request_id"],
    )
    result = unsigned_payload(reply)
    assert result["status"] == "creation_disposition_pending"
    assert all(result[key] == value for key, value in identity.items())
    assert "private" not in response.text


@pytest.mark.asyncio
async def test_signed_dispose_request_cannot_supply_replacement_options(monkeypatch):
    ctrl = settings.VMController.__new__(settings.VMController)
    monkeypatch.setattr(settings, "LIFECYCLE_HMAC_SECRET", SECRET)
    ctrl._claim_lifecycle_nonce = AsyncMock(return_value=True)
    ctrl._workspace_cleanup_authority_request = AsyncMock()
    row, _ = replay_fixture()
    payload = sign_payload(
        {**disposition_identity(row), "purge_disk": True},
        direction="request",
        operation="creation_retry_dispose",
        secret=SECRET,
    )
    response = await ctrl.http_creation_dispose(
        SimpleNamespace(json=AsyncMock(return_value=payload))
    )
    assert response.status == 400
    ctrl._workspace_cleanup_authority_request.assert_not_awaited()
