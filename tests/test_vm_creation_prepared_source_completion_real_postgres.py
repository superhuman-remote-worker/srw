"""Prepared completion needs the exact durable allocation, even after source GC."""

from copy import deepcopy

import pytest

from tests.test_vm_creation_source_preparation_real_postgres import (
    db as _db_fixture,
    setup as _setup_fixture,
    prepared as _prepared_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    _prepare_without_root,
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from shared.vm_creation_disposition import disposition_identity
from vm_controller.creation_actuation import CreationActuator
from vm_controller.creation_disposition import CreationDisposer
from vm_controller.creation_disposition_sources import DispositionSources
from vm_controller.workspace_preparation import allocation_name, creation_held

db, setup, prepared = _db_fixture, _setup_fixture, _prepared_fixture


@pytest.mark.asyncio
@pytest.mark.parametrize("source_gc", [False, True])
@pytest.mark.parametrize(
    "lost_boundary", ["before_source_cas", "after_allocation_save"]
)
async def test_prepared_completion_binds_saved_allocation_and_exact_old_source(
    db, prepared, source_gc, lost_boundary
):
    ctrl, api, _, payload, service = prepared
    await _prepare_without_root(db, prepared, False, "scope")
    retries = VMCreationRetryStore(db)
    row = await retries.inspect(request_id=payload["creation_retry"]["request_id"])
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    source = deepcopy(allocation.state["creation_source"])
    assert await db.cancel_job(row["job_id"])
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    # Freeze the real source before external CAS. Fail exactly at its CAS so the
    # allocation still carries an active hold when GC/replay is exercised.
    original = ctrl.k8s_client.replace_namespaced_custom_object
    original_save = service.store.save

    def blocked(**kwargs):
        raise TimeoutError("source CAS not accepted")

    async def lost_save(record, state):
        result = await original_save(record, state)
        if record.kind == "allocation" and "creation_disposition" in state:
            raise TimeoutError("accepted allocation receipt save reply lost")
        return result

    if lost_boundary == "before_source_cas":
        ctrl.k8s_client.replace_namespaced_custom_object = blocked
    else:
        service.store.save = lost_save
    await CreationDisposer(ctrl).run(disposition_identity(row))
    ctrl.k8s_client.replace_namespaced_custom_object = original
    service.store.save = original_save
    row = await retries.inspect(request_id=row["request_id"])
    assert row["cancellation_progress"]["source"]["source"] == source
    if source_gc:
        del api.objects["DataVolume", source["name"]]
        del api.objects["PersistentVolumeClaim", source["name"]]
    carrier = api.read(
        "Lease", "srw-cleanup-" + row["creation_admission_id"].replace("-", "")
    )
    result = await DispositionSources(
        CreationActuator(ctrl), row, carrier, row["cancellation_disposition"]
    ).run()
    assert result["allocation"]["uid"] == allocation.uid
    assert result["allocation"]["request"] == allocation.request
    assert result["allocation"]["phase"] == "Cancelled"
    assert (
        result["allocation"]["receipt"]["plan"]
        == row["cancellation_progress"]["source"]
    )
    assert result["outcome"] == (
        "source_identity_gone" if source_gc else "pin_disposed"
    )
    observed = await service.store.get(allocation.name)
    assert not creation_held(observed)
    assert "creation_root" not in observed.state
    changed = deepcopy(observed)
    changed.state["creation_disposition"]["plan"]["source"]["allocation"]["uid"] = row[
        "job_id"
    ]
    assert creation_held(changed)
    assert (await retries.inspect(request_id=row["request_id"]))[
        "state"
    ] == "cancel_requested"
