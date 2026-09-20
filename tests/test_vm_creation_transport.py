"""Authenticated read-only configuration precedes any durable create intent."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from shared.vm_lifecycle_auth import (
    AUTH_FIELD,
    sign_payload,
    unsigned_payload,
    verify_payload,
)
from tests.test_vm_creation_configuration import controller
from tests.test_vm_creation_request import SECRET, options
from orchestrator.services.vm_creation_request import build_vm_creation_request
from vm_controller.creation_configuration import resolve_creation_configuration


def fixture(*, mutate=None, signed=True, correlation=True, status=200):
    request = build_vm_creation_request(
        **options(vm_image=None), network_tier="restricted"
    )
    resolved = resolve_creation_configuration(controller(), request)
    resolved["creation_retry_protocol"] = 1
    if mutate:
        mutate(resolved)

    async def post(path, *, json, timeout):
        assert path == "/vm-creation/configuration"
        assert timeout == 10.0
        assert verify_payload(
            json,
            direction="request",
            operation="creation_config_resolve",
            secret=SECRET,
        )
        assert unsigned_payload(json) == {"request": request}
        value = deepcopy(resolved)
        if signed:
            value = sign_payload(
                value,
                direction="response",
                operation="creation_config_resolve",
                secret=SECRET,
                correlation_id=json[AUTH_FIELD]["request_id"]
                if correlation
                else "different-request",
            )
        return httpx.Response(
            status,
            json=value,
            request=httpx.Request("POST", "http://controller" + path),
        )

    return request, resolved, SimpleNamespace(post=AsyncMock(side_effect=post))


@pytest.mark.asyncio
async def test_configuration_resolution_authenticates_materialized_defaults_without_creating():
    from orchestrator.services.vm_creation_transport import (
        resolve_vm_creation_configuration,
    )

    request, expected, client = fixture()
    original = deepcopy(request)
    actual = await resolve_vm_creation_configuration(client, request, secret=SECRET)
    assert actual == expected
    assert request == original
    client.post.assert_awaited_once()
    assert "_auth" not in actual


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "unsigned",
        "correlation",
        "owner",
        "options",
        "digest",
        "configuration",
        "unavailable",
        "legacy",
        "boolean_protocol",
    ],
)
async def test_configuration_resolution_refuses_unproven_or_changed_intent(failure):
    from orchestrator.services.vm_creation_transport import (
        CreationConfigurationUnavailable,
        resolve_vm_creation_configuration,
    )
    from shared.vm_creation_retry import canonical_request_digest

    def mutate(result):
        if failure == "owner":
            result["request"]["job_id"] = "00000000-0000-4000-8000-000000000099"
            result["request_digest"] = canonical_request_digest(result["request"])
        elif failure == "options":
            result["request"]["memory"] = "64Gi"
            result["request_digest"] = canonical_request_digest(result["request"])
        elif failure == "digest":
            result["request_digest"] = "sha256:" + "0" * 64
        elif failure == "configuration":
            result["controller_configuration"]["namespace"] = "foreign"
        elif failure == "legacy":
            result.pop("creation_retry_protocol")
        elif failure == "boolean_protocol":
            result["creation_retry_protocol"] = True

    request, _, client = fixture(
        mutate=mutate,
        signed=failure != "unsigned",
        correlation=failure != "correlation",
        status=503 if failure == "unavailable" else 200,
    )
    with pytest.raises(CreationConfigurationUnavailable) as exc:
        await resolve_vm_creation_configuration(client, request, secret=SECRET)
    assert str(exc.value) == (
        "controller_unavailable"
        if failure == "unavailable"
        else "creation_configuration_unproven"
    )
    assert client.post.await_count == 1


@pytest.mark.asyncio
async def test_configuration_transport_failure_omits_private_response_details():
    from orchestrator.services.vm_creation_transport import (
        CreationConfigurationUnavailable,
        resolve_vm_creation_configuration,
    )

    request, _, client = fixture()
    client.post.side_effect = httpx.ConnectError("synthetic-secret-controller-url")
    with pytest.raises(CreationConfigurationUnavailable) as exc:
        await resolve_vm_creation_configuration(client, request, secret=SECRET)
    assert "synthetic-secret" not in str(exc.value)


@pytest.mark.asyncio
async def test_configuration_resolution_requires_authentication_before_any_io():
    from orchestrator.services.vm_creation_transport import (
        CreationConfigurationUnavailable,
        resolve_vm_creation_configuration,
    )

    request, _, client = fixture()
    with pytest.raises(CreationConfigurationUnavailable):
        await resolve_vm_creation_configuration(client, request, secret=None)
    client.post.assert_not_awaited()
