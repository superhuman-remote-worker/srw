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


async def replay_vm_creation(client, row: Mapping, *, secret: bytes) -> dict:
    """Replay a claimed immutable intent only through the fenced protocol route.

    Controller effect observations settle adoption in the authoritative store.
    The reply here only schedules further observation; it never grants readiness
    or supplies context fields to merge. Cancellation uses observation, not create.
    """
    from uuid import UUID
    from shared.vm_creation_retry import VMCreationRetryIdentity

    request = deepcopy(row["canonical_request"])
    identity = VMCreationRetryIdentity(
        request_id=str(row["request_id"]),
        job_id=str(row["job_id"]),
        provision_generation=str(row["provision_generation"]),
        request_digest=row["request_digest"],
        expected_pvc_uid=str(row["expected_pvc_uid"])
        if row["expected_pvc_uid"]
        else None,
        claim_token=str(row["claim_token"]),
    )
    if (
        not secret
        or client is None
        or row["state"] != "reconciling"
        or canonical_request_digest(request) != identity.request_digest
        or request["job_id"] != identity.job_id
        or request["provision_generation"] != identity.provision_generation
    ):
        raise ValueError("creation replay source unproven")
    envelope = {
        "version": 1,
        "request_id": identity.request_id,
        "claim_token": identity.claim_token,
        "request_digest": identity.request_digest,
        "controller_configuration_digest": row["controller_configuration_digest"],
    }
    operation = "creation_retry_create"
    signed = sign_payload(
        {**request, "creation_retry": envelope},
        direction="request",
        operation=operation,
        secret=secret,
    )
    try:
        response = await client.post("/vm-creation/create", json=signed, timeout=30.0)
        if response.status_code >= 500 or response.status_code == 429:
            return {"outcome": "transport_unknown", "reason": "controller_unavailable"}
        data = response.json()
        if not isinstance(data, Mapping) or not verify_payload(
            data,
            direction="response",
            operation=operation,
            secret=secret,
            expected_correlation_id=signed[AUTH_FIELD]["request_id"],
        ):
            raise ValueError("creation response unproven")
        response.raise_for_status()
        result = unsigned_payload(data)
        if (
            result.get("job_id") != identity.job_id
            or result.get("provision_generation") != identity.provision_generation
        ):
            raise ValueError("creation owner changed")
        status = result.get("status")
        reason = result.get("reason")
        if status == "created":
            # Even an authenticated malformed result is not adoption evidence.
            for key in ("vm_uid", "rootdisk_pvc_uid"):
                if str(UUID(result[key])) != result[key]:
                    raise ValueError("creation identity unproven")
            return {"outcome": "adopted"}
        if status == "creation_attention":
            return {"outcome": "blocked", "reason": "creation_evidence_unproven"}
        if status == "creation_pending":
            if reason == "capacity_wait":
                return {"outcome": "capacity_wait", "reason": reason}
            if reason in {
                "golden_wait",
                "preparation_wait",
                "headscale_wait",
                "disk_wait",
            }:
                return {"outcome": "dependency_wait", "reason": reason}
            if reason == "creation_observation_pending":
                return {"outcome": "observation_wait", "reason": reason}
        raise ValueError("creation result unsupported")
    except httpx.RequestError:
        return {"outcome": "transport_unknown", "reason": "controller_unavailable"}
    except (httpx.HTTPError, ValueError, TypeError, KeyError, AttributeError):
        return {"outcome": "blocked", "reason": "creation_evidence_unproven"}


async def dispose_vm_creation(client, row: Mapping, *, secret: bytes) -> dict:
    """Poll cancelled intent on a dedicated route; never replay a create request."""
    from shared.vm_creation_disposition import disposition_identity

    identity = disposition_identity(row)
    request = row["canonical_request"]
    if (
        not secret
        or client is None
        or row["state"] != "cancel_requested"
        or canonical_request_digest(request) != identity["request_digest"]
        or any(
            request[key] != identity[key] for key in ("job_id", "provision_generation")
        )
    ):
        raise ValueError("Cancellation source is unproven")
    operation = "creation_retry_dispose"
    signed = sign_payload(
        identity, direction="request", operation=operation, secret=secret
    )
    try:
        response = await client.post("/vm-creation/dispose", json=signed, timeout=30.0)
        if response.status_code >= 500 or response.status_code == 429:
            return {"outcome": "transport_unknown", "reason": "controller_unavailable"}
        data = response.json()
        if not isinstance(data, Mapping) or not verify_payload(
            data,
            direction="response",
            operation=operation,
            secret=secret,
            expected_correlation_id=signed[AUTH_FIELD]["request_id"],
        ):
            raise ValueError("Cancellation reply is unproven")
        response.raise_for_status()
        result = unsigned_payload(data)
        if any(result.get(key) != value for key, value in identity.items()):
            raise ValueError("Cancellation identity changed")
        if result.get("status") in {
            "creation_disposition_pending",
            "creation_disposed",
            "creation_adopted",
        }:
            # Completion/adoption must already be committed by the controller's
            # authority call. This transport never merges context or releases holds.
            return {
                "outcome": "observation_wait",
                "reason": "creation_observation_pending",
            }
        raise ValueError("Cancellation evidence is unproven")
    except httpx.RequestError:
        return {"outcome": "transport_unknown", "reason": "controller_unavailable"}
    except (httpx.HTTPError, ValueError, TypeError, KeyError, AttributeError):
        return {"outcome": "blocked", "reason": "creation_evidence_unproven"}
