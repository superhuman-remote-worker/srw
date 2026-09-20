"""Legacy attachment writers must respect outstanding protocol issuance."""

from copy import deepcopy
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from tests.test_vm_retained_storage import binding, runtime


async def marked(state="reconciling"):
    value = binding()
    service, pvc = runtime(value)
    await service.claim(value, value["owner_id"])
    lease = service.controller.coordination_api.lease
    request_id, generation, nonce = (str(uuid4()) for _ in range(3))
    lease.metadata.uid, lease.metadata.namespace = str(uuid4()), "test"
    lease.metadata.annotations = {
        "srw.io/vm-create-request-id": request_id,
        "srw.io/vm-create-effect-nonce": nonce,
        "srw.io/provision-generation": generation,
    }
    row = {
        "request_id": request_id,
        "job_id": value["owner_id"],
        "provision_generation": generation,
        "state": state,
        "reason": "creation_adopted" if state == "succeeded" else None,
        "effects": [
            {
                "state": "observed",
                "carrier_intent": {
                    "effect_kind": "workspace_attach",
                    "effect_nonce": nonce,
                    "workspace_attachment": {"binding": value},
                },
                "evidence": {"uid": lease.metadata.uid, "namespace": "test"},
            },
            {
                "state": "observed",
                "carrier_intent": {"effect_kind": "vm"},
                "evidence": {"uid": str(uuid4())},
            },
        ],
    }
    service.controller._workspace_cleanup_authority_request = AsyncMock(
        return_value=row
    )
    return service, {**value, "pvc_uid": pvc.metadata.uid}, row


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["claim", "detach", "delete"])
@pytest.mark.parametrize("state", ["reconciling", "cancel_requested", "attention"])
async def test_legacy_writer_cannot_cross_active_creation_marker(action, state):
    service, value, _ = await marked(state)
    before = deepcopy(service.controller.coordination_api.lease)
    with pytest.raises(RuntimeError, match="held"):
        if action == "claim":
            await service.claim(value, value["owner_id"])
        else:
            await getattr(service, action)(value)
    assert service.controller.coordination_api.lease.to_dict() == before.to_dict()
    service.controller._delete_captured_rootdisk.assert_not_awaited()


@pytest.mark.asyncio
async def test_exact_adoption_allows_idempotent_legacy_detach():
    service, value, _ = await marked("succeeded")
    assert await service.detach(value)
    assert await service.detach(value)
    assert (
        service.controller.coordination_api.lease.metadata.annotations[
            "srw.io/detached"
        ]
        == "true"
    )


@pytest.mark.asyncio
async def test_unavailable_marker_authority_never_mutates_attachment():
    service, value, _ = await marked()
    service.controller._workspace_cleanup_authority_request.side_effect = TimeoutError(
        "authority unavailable"
    )
    before = deepcopy(service.controller.coordination_api.lease)
    with pytest.raises(TimeoutError):
        await service.detach(value)
    assert service.controller.coordination_api.lease.to_dict() == before.to_dict()


@pytest.mark.asyncio
async def test_unrelated_completed_adoption_is_not_permission_to_detach():
    service, value, row = await marked("succeeded")
    row["effects"][0]["evidence"]["uid"] = str(uuid4())
    with pytest.raises(RuntimeError, match="identity changed"):
        await service.detach(value)


@pytest.mark.asyncio
async def test_completed_protocol_lease_cannot_start_a_legacy_create():
    service, value, _ = await marked("succeeded")
    with pytest.raises(RuntimeError, match="held"):
        await service.claim(value, value["owner_id"])
