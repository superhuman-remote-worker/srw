"""Cancellation has its own authenticated route and cannot replay a create."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from shared.vm_lifecycle_auth import (
    AUTH_FIELD,
    sign_payload,
    unsigned_payload,
    verify_payload,
)
from tests.test_vm_creation_replay_transport import replay_fixture, SECRET


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        None,
        "unsigned",
        "purpose",
        "correlation",
        "identity",
        "malformed",
        "404",
        "timeout",
    ],
)
async def test_disposition_transport_is_correlated_and_has_no_create_fallback(failure):
    from orchestrator.services.vm_creation_transport import dispose_vm_creation

    row, _ = replay_fixture()
    row["state"] = "cancel_requested"
    calls = []

    async def post(path, *, json, timeout):
        calls.append(path)
        assert path == "/vm-creation/dispose" and timeout == 30.0
        assert verify_payload(
            json, direction="request", operation="creation_retry_dispose", secret=SECRET
        )
        payload = unsigned_payload(json)
        assert set(payload) == {
            "version",
            "request_id",
            "job_id",
            "provision_generation",
            "request_digest",
            "controller_configuration_digest",
        }
        assert payload["request_id"] == str(row["request_id"])
        if failure == "timeout":
            raise httpx.ReadTimeout("private-key-must-not-leak")
        reply = {
            **payload,
            "status": "creation_disposition_pending",
            "reason": "creation_disposition_pending",
        }
        if failure == "identity":
            reply["request_id"] = str(uuid4())
        if failure == "malformed":
            reply["status"] = "made-up-private-status"
        if failure != "unsigned":
            reply = sign_payload(
                reply,
                direction="response",
                operation="create"
                if failure == "purpose"
                else "creation_retry_dispose",
                secret=SECRET,
                correlation_id="foreign"
                if failure == "correlation"
                else json[AUTH_FIELD]["request_id"],
            )
        return httpx.Response(
            404 if failure == "404" else 200,
            json=reply,
            request=httpx.Request("POST", "http://controller" + path),
        )

    result = await dispose_vm_creation(SimpleNamespace(post=post), row, secret=SECRET)
    assert calls == ["/vm-creation/dispose"]
    assert result["outcome"] == (
        "observation_wait"
        if failure is None
        else "transport_unknown"
        if failure == "timeout"
        else "blocked"
    )
    assert "private" not in str(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["state", "secret", "digest"])
async def test_disposition_transport_refuses_unproven_local_request_before_io(failure):
    from orchestrator.services.vm_creation_transport import dispose_vm_creation

    row, _ = replay_fixture()
    row["state"] = "cancel_requested"
    if failure == "state":
        row["state"] = "reconciling"
    if failure == "digest":
        row["canonical_request"]["memory"] = "changed"
    client = SimpleNamespace(post=AsyncMock())
    with pytest.raises(ValueError):
        await dispose_vm_creation(
            client, row, secret=None if failure == "secret" else SECRET
        )
    client.post.assert_not_awaited()
