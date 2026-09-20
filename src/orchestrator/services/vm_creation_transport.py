"""Authenticated VM creation transport; never treats resolution as an effect grant."""

from collections.abc import Mapping
from copy import deepcopy

import httpx

from shared.vm_creation_issuance import canonical_configuration_digest
from shared.vm_creation_retry import canonical_request_digest
from shared.vm_lifecycle_auth import (
    AUTH_FIELD,
    sign_payload,
    unsigned_payload,
    verify_payload,
)


class CreationConfigurationUnavailable(ValueError):
    def __init__(self, reason="creation_configuration_unproven"):
        self.reason = reason
        super().__init__(reason)


def validate_creation_resolution(request: Mapping, result: Mapping) -> dict:
    """Validate a previously authenticated reply against the frozen preflight."""
    if (
        type(result.get("creation_retry_protocol")) is not int
        or result["creation_retry_protocol"] != 1
    ):
        raise ValueError("creation protocol unsupported")
    resolved = result["request"]
    if canonical_request_digest(resolved) != result["request_digest"]:
        raise ValueError("request digest mismatch")
    if (
        canonical_configuration_digest(result["controller_configuration"])
        != result["controller_configuration_digest"]
    ):
        raise ValueError("configuration digest mismatch")
    for key, value in request.items():
        # The existing controller raises disk_size to its source-disk floor.
        # It materializes omitted/empty defaults and normalizes tier whitespace.
        if key == "disk_size" or value in (None, ""):
            continue
        expected = (
            value.strip() if key == "network_tier" and isinstance(value, str) else value
        )
        if resolved.get(key) != expected:
            raise ValueError("caller intent changed")
    if any(
        resolved.get(key) != request.get(key)
        for key in ("job_id", "provision_generation")
    ):
        raise ValueError("creation owner changed")
    return deepcopy(result)


async def resolve_vm_creation_configuration(
    client, request: Mapping, *, secret: bytes
) -> dict:
    """Resolve before capture, requiring capability from the same signed response.

    Missing capability must not send a protocol create: a legacy controller may
    otherwise ignore an unknown envelope and use its unfenced creation path.
    Caller options are preserved except documented default/floor normalization.
    """
    operation = "creation_config_resolve"
    try:
        if not secret or client is None:
            raise ValueError("authentication unavailable")
        canonical_request_digest(request)
        signed = sign_payload(
            {"request": deepcopy(dict(request))},
            direction="request",
            operation=operation,
            secret=secret,
        )
        response = await client.post(
            "/vm-creation/configuration", json=signed, timeout=10.0
        )
        if response.status_code >= 500 or response.status_code == 429:
            # Proxy errors need not be signed; they prove no configuration or
            # issuance fact and remain a read-only infrastructure outage.
            raise CreationConfigurationUnavailable("controller_unavailable")
        data = response.json()
        if not isinstance(data, Mapping) or not verify_payload(
            data,
            direction="response",
            operation=operation,
            secret=secret,
            expected_correlation_id=signed[AUTH_FIELD]["request_id"],
        ):
            raise ValueError("configuration response authentication failed")
        response.raise_for_status()
        result = unsigned_payload(data)
        return validate_creation_resolution(request, result)
    except CreationConfigurationUnavailable:
        raise
    except httpx.RequestError:
        raise CreationConfigurationUnavailable("controller_unavailable") from None
    except (httpx.HTTPError, ValueError, TypeError, KeyError, AttributeError):
        # Never surface raw response bodies, URLs, credentials or exception text.
        raise CreationConfigurationUnavailable() from None
