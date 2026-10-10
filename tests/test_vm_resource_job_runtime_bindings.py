"""Pre-Ready accounting must not depend on a successful SSH probe."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services.vm_resource_reservation_store import (
    VMResourceReservationStore,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("inventory_accepts", [False, True])
async def test_unreachable_guest_still_requires_inventory_for_occupancy(
    inventory_accepts,
):
    owner, generation = uuid4(), uuid4()
    retry = {
        "owner_kind": "job",
        "job_id": owner,
        "provision_generation": generation,
        "reason": "creation_adopted",
        "ready_at": None,
    }
    vm = {"status": "ssh_unreachable"}
    store = VMResourceReservationStore.__new__(VMResourceReservationStore)
    store.bind_ready_on_conn = AsyncMock(return_value=inventory_accepts)
    conn = object()

    accepted = await store.bind_observed_runtime_on_conn(
        conn, retry=retry, vm=vm, job_id=str(owner), generation=str(generation)
    )

    assert accepted is inventory_accepts
    store.bind_ready_on_conn.assert_awaited_once_with(
        conn,
        retry=retry,
        vm=vm,
        job_id=str(owner),
        generation=str(generation),
        observed_before_ready=True,
    )
    assert vm == {"status": "ssh_unreachable"}
    assert retry["ready_at"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prior_evidence", ["ready_status", "ready_at", "ssh", "launcher"]
)
async def test_unreachable_guest_cannot_bypass_prior_readiness(prior_evidence):
    owner, generation = uuid4(), uuid4()
    retry = {
        "owner_kind": "job",
        "job_id": owner,
        "provision_generation": generation,
        "reason": "creation_adopted",
        "ready_at": None,
    }
    vm = {"status": "ssh_unreachable"}
    if prior_evidence == "ready_status":
        vm["status"] = "ready"
    elif prior_evidence == "ready_at":
        retry["ready_at"] = datetime.now(timezone.utc)
    elif prior_evidence == "ssh":
        vm["ssh_verified_at"] = datetime.now(timezone.utc).isoformat()
    else:
        vm["active_pod_uid"] = str(uuid4())
    store = VMResourceReservationStore.__new__(VMResourceReservationStore)
    store.bind_ready_on_conn = AsyncMock(return_value=True)

    assert not await store.bind_observed_runtime_on_conn(
        object(), retry=retry, vm=vm, job_id=str(owner), generation=str(generation)
    )
    store.bind_ready_on_conn.assert_not_awaited()
