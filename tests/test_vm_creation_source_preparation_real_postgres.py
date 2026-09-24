"""Source-only cancellation uses genuine prepared allocation and SQL authority."""

from copy import deepcopy
from uuid import UUID, uuid4

import pytest

from tests.test_vm_creation_prepared_real_postgres import (
    setup as _setup_fixture,
    prepared as _prepared_fixture,
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    test_prepared_real_authority_lost_vm_reply_and_completed_clone as _prepare_without_root,
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from shared.vm_creation_disposition import disposition_identity
from shared.vm_creation_source_completion import validate_source_completion
from vm_controller.creation_disposition import CreationDisposer
from vm_controller.creation_sources import pins
from vm_controller.workspace_preparation import allocation_name, creation_held

setup, prepared, db = _setup_fixture, _prepared_fixture, _db_fixture


async def assert_settled_source_disposition(
    db, current, allocation, *, source_kind, outcome
):
    assert (current["state"], current["reason"]) == ("settled", "creation_disposed")
    disposition = current["cancellation_disposition"]
    plan = current["cancellation_progress"]["source"]
    completion = current["cancellation_completion"]["source"]
    assert plan["kind"] == "source_disposition_planned"
    assert plan["disposition_id"] == disposition["disposition_id"]
    assert plan["source"]["kind"] == source_kind
    assert completion["outcome"] == outcome
    assert completion["allocation"]["uid"] == allocation.uid
    assert validate_source_completion(plan, completion) == completion
    async with db.acquire() as conn:
        parent = await conn.fetchrow(
            "SELECT completed_at,outcome FROM vm_workspace_cleanup_admissions WHERE id=$1",
            UUID(current["creation_admission_id"]),
        )
    assert parent["completed_at"] is not None
    assert parent["outcome"] == "creation_disposed"


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_allocation_reply", [False, True])
async def test_precarrier_prepared_source_cancel_releases_only_exact_pin_and_allocation(
    db, prepared, lost_allocation_reply
):
    # Finish the genuine engine build; a rejected malformed root proposal leaves
    # its real delivered source/pin, with no issued SQL root effect or root object.
    await _prepare_without_root(db, prepared, False, "scope")
    ctrl, api, _, payload, service = prepared
    store = VMCreationRetryStore(db)
    row = await store.inspect(request_id=payload["creation_retry"]["request_id"])
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    assert creation_held(allocation)
    source = allocation.state["creation_source"]
    assert (
        pins(api.read("DataVolume", source["name"]))[row["request_id"]]["state"]
        == "active"
    )
    assert await db.cancel_job(row["job_id"])
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    original_save = service.store.save
    lost = False

    async def save(record, state):
        nonlocal lost
        result = await original_save(record, state)
        if (
            lost_allocation_reply
            and not lost
            and record.kind == "allocation"
            and "creation_disposition" in state
        ):
            lost = True
            raise TimeoutError("accepted allocation disposition save reply lost")
        return result

    service.store.save = save
    await CreationDisposer(ctrl).run(disposition_identity(row))
    await CreationDisposer(ctrl)._run(disposition_identity(row), {})

    allocation = await service.store.get(allocation.name)
    assert allocation.state["phase"] == "Cancelled"
    assert "creation_root" not in allocation.state
    assert not creation_held(allocation)
    for field in ("creation_binding", "creation_source", "creation_disposition"):
        changed = deepcopy(allocation)
        changed.state.pop(field)
        assert creation_held(changed)
    changed = deepcopy(allocation)
    changed.state["creation_disposition"]["plan"]["source"]["allocation"]["uid"] = str(
        uuid4()
    )
    assert creation_held(changed)

    assert (
        pins(api.read("DataVolume", source["name"]))[row["request_id"]]["state"]
        == "disposed"
    )
    current = await store.inspect(request_id=row["request_id"])
    await assert_settled_source_disposition(
        db, current, allocation, source_kind="prepared", outcome="pin_disposed"
    )


@pytest.mark.asyncio
async def test_existing_undelivered_allocation_is_cancelled_without_creating_source(
    db, prepared
):
    ctrl, api, _, payload, service = prepared
    service.store.finish = lambda *args, **kwargs: None
    await _prepare_without_root(db, prepared, False, "scope")
    store = VMCreationRetryStore(db)
    row = await store.inspect(request_id=payload["creation_retry"]["request_id"])
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    assert allocation.state["workspace_source_issued"] is False
    writes = list(api.writes)
    assert await db.cancel_job(row["job_id"])
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    await CreationDisposer(ctrl)._run(disposition_identity(row), {})
    allocation = await service.store.get(allocation.name)
    assert allocation.state["phase"] == "Cancelled"
    assert allocation.state["workspace_source_issued"] is False
    assert "creation_root" not in allocation.state
    assert [kind for kind in api.writes if kind != "Lease"] == [
        kind for kind in writes if kind != "Lease"
    ]
    current = await store.inspect(request_id=row["request_id"])
    await assert_settled_source_disposition(
        db,
        current,
        allocation,
        source_kind="preparation_never_delivered",
        outcome="allocation_never_delivered",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    ["allocation_missing", "allocation_uid", "source_missing", "artifact_missing"],
)
async def test_unknown_preparation_evidence_cannot_release_or_recreate_source(
    db, prepared, fault
):
    ctrl, api, _, payload, service = prepared
    await _prepare_without_root(db, prepared, False, "scope")
    store = VMCreationRetryStore(db)
    row = await store.inspect(request_id=payload["creation_retry"]["request_id"])
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    source = deepcopy(allocation.state["creation_source"])
    if fault == "allocation_missing":
        del service.store.data[allocation.name]
    elif fault == "allocation_uid":
        service.store.data[allocation.name].uid = str(uuid4())
    elif fault == "source_missing":
        del service.store.data[allocation.name].state["creation_source"]
    else:
        del service.store.data[allocation.state["artifact"]]
    identities = set(service.store.data)
    assert await db.cancel_job(row["job_id"])
    await CreationDisposer(ctrl).run(disposition_identity(row))
    assert set(service.store.data) == identities
    assert (
        pins(api.read("DataVolume", source["name"]))[row["request_id"]]["state"]
        == "active"
    )
    assert (await store.inspect(request_id=row["request_id"]))[
        "cancellation_progress"
    ] == {}
