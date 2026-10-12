"""Protocol replay stays on its fenced route and returns bounded observations."""

from copy import deepcopy
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
from tests.test_vm_creation_transport import fixture
from tests.test_vm_creation_request import SECRET


def replay_fixture(*, status="creation_pending", reason="capacity_wait", failure=None):
    _, resolved, _ = fixture()
    row = {
        **{
            key: resolved[key]
            for key in ("request_digest", "controller_configuration_digest")
        },
        "request_id": uuid4(),
        "claim_token": uuid4(),
        "state": "reconciling",
        "canonical_request": resolved["request"],
        # Store claims are always normalized by the retry store's ``_record``
        # to carry the typed 0278 source columns; a Job claim has no thread.
        "owner_kind": "job",
        "thread_id": None,
        "job_id": resolved["request"]["job_id"],
        "provision_generation": resolved["request"]["provision_generation"],
        "expected_pvc_uid": None,
    }
    reply = {
        "job_id": row["job_id"],
        "provision_generation": row["provision_generation"],
        "status": status,
        "reason": reason,
    }
    if status == "created":
        reply.update(
            vm_uid=str(uuid4()),
            rootdisk_pvc_uid=str(uuid4()),
            vm_name="agent-vm-" + row["job_id"],
            namespace="vm-ns",
            ssh_host_key_fingerprint="SHA256:test",
        )
    if failure == "owner":
        reply["job_id"] = str(uuid4())
    if failure == "generation":
        reply["provision_generation"] = str(uuid4())
    if failure == "unknown_reason":
        reply["reason"] = "secret-internal-url"
    if failure == "invalid_created":
        reply["vm_uid"] = "invalid"

    async def post(path, *, json, timeout):
        assert path == "/vm-creation/create"
        assert timeout == 30.0
        assert verify_payload(
            json, direction="request", operation="creation_retry_create", secret=SECRET
        )
        sent = unsigned_payload(json)
        envelope = sent.pop("creation_retry")
        assert sent == resolved["request"]
        assert envelope == {
            "version": 1,
            "request_id": str(row["request_id"]),
            "claim_token": str(row["claim_token"]),
            "request_digest": row["request_digest"],
            "controller_configuration_digest": row["controller_configuration_digest"],
        }
        if failure == "lost_reply":
            raise httpx.ReadTimeout("secret-internal-url")
        if failure in {"proxy", "old_replica"}:
            return httpx.Response(
                503 if failure == "proxy" else 404,
                text="secret-internal-url",
                request=httpx.Request("POST", "http://controller" + path),
            )
        value = sign_payload(
            reply,
            direction="response",
            operation="create" if failure == "operation" else "creation_retry_create",
            secret=SECRET,
            correlation_id="other"
            if failure == "correlation"
            else json[AUTH_FIELD]["request_id"],
        )
        if failure == "unsigned":
            value = reply
        return httpx.Response(
            200, json=value, request=httpx.Request("POST", "http://controller" + path)
        )

    return row, SimpleNamespace(post=AsyncMock(side_effect=post))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,reason,outcome",
    [
        ("creation_pending", "capacity_wait", "capacity_wait"),
        ("creation_pending", "golden_wait", "dependency_wait"),
        ("creation_pending", "preparation_wait", "dependency_wait"),
        ("creation_pending", "headscale_wait", "dependency_wait"),
        ("creation_pending", "disk_wait", "dependency_wait"),
        ("creation_pending", "creation_observation_pending", "observation_wait"),
        ("creation_attention", "creation_evidence_unproven", "blocked"),
        ("created", None, "adopted"),
    ],
)
async def test_frozen_replay_uses_dedicated_route_and_bounded_outcomes(
    status, reason, outcome
):
    from orchestrator.services.vm_creation_transport import replay_vm_creation

    row, client = replay_fixture(status=status, reason=reason)
    before = deepcopy(row)
    result = await replay_vm_creation(client, row, secret=SECRET)
    assert result["outcome"] == outcome
    # Response identity is never an instruction to merge context or release queue.
    assert set(result) <= {"outcome", "reason"}
    assert row == before
    client.post.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,outcome",
    [
        ("owner", "blocked"),
        ("generation", "blocked"),
        ("unknown_reason", "blocked"),
        ("operation", "blocked"),
        ("correlation", "blocked"),
        ("unsigned", "blocked"),
        ("lost_reply", "transport_unknown"),
        ("proxy", "transport_unknown"),
        ("old_replica", "blocked"),
    ],
)
async def test_replay_ambiguous_or_unproven_reply_never_becomes_absence(
    failure, outcome
):
    from orchestrator.services.vm_creation_transport import replay_vm_creation

    row, client = replay_fixture(failure=failure)
    result = await replay_vm_creation(client, row, secret=SECRET)
    assert result["outcome"] == outcome
    assert "secret-internal-url" not in str(result)
    client.post.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["digest", "cancelled", "claim", "secret"])
async def test_invalid_or_cancelled_source_never_sends_create(failure):
    from orchestrator.services.vm_creation_transport import replay_vm_creation

    row, client = replay_fixture()
    if failure == "digest":
        row["canonical_request"]["memory"] = "64Gi"
    elif failure == "cancelled":
        row["state"] = "cancel_requested"
    elif failure == "claim":
        row["claim_token"] = None
    with pytest.raises(ValueError):
        await replay_vm_creation(
            client, row, secret=None if failure == "secret" else SECRET
        )
    client.post.assert_not_awaited()
