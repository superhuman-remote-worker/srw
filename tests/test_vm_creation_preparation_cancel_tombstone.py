"""A delayed initial ensure cannot recreate a cancelled preparation allocation."""

from uuid import uuid4

import pytest

from tests.test_vm_preparation_lifecycle import engine, request
from vm_controller.workspace_preparation import allocation_name


@pytest.mark.asyncio
async def test_cancelled_creation_allocation_survives_gc_and_delayed_initial_ensure(
    monkeypatch,
):
    service, value = engine(), request()
    original = await service.store.ensure(
        allocation_name(value),
        "allocation",
        value,
        {
            "phase": "Cancelled",
            "workspace_source_issued": False,
            "expires_at": 1,
            "creation_disposition": {
                "version": 1,
                "kind": "prepared_source_never_delivered",
                "plan": {"disposition_id": str(uuid4())},
            },
        },
    )
    # Even malformed legacy disposition evidence must preserve the fence; it
    # cannot authorize cleanup or justify erasing cancellation provenance.
    monkeypatch.setattr("vm_controller.workspace_preparation.now", lambda: 10**9)
    await service.reconcile()
    delayed = await service.store.ensure(
        allocation_name(value),
        "allocation",
        value,
        {"phase": "Pending", "workspace_source_issued": False},
    )
    assert delayed.uid == original.uid
    assert delayed.state["phase"] == "Cancelled"
