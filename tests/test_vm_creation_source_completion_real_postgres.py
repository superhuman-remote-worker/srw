"""Actual source readback is distinct from the immutable cancellation plan."""

from copy import deepcopy
from uuid import UUID, uuid4

import pytest

from tests.test_vm_creation_source_disposition_real_postgres import (
    db as _db_fixture,
    setup as _setup_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    frozen_golden,
    source_runtime,
)
from vm_controller.creation_actuation import CreationActuator
from vm_controller.creation_disposition_sources import DispositionSources

db, setup = _db_fixture, _setup_fixture


@pytest.mark.asyncio
async def test_actual_source_cas_readback_yields_typed_completion_without_release(
    db, monkeypatch, setup
):
    service, row, carrier, disposition, source = await frozen_golden(db, monkeypatch)
    ctrl, api = source_runtime(setup, service, row, source, monkeypatch)
    api.objects["PersistentVolumeClaim", source["name"]]["metadata"][
        "resourceVersion"
    ] = "1"
    result = await DispositionSources(
        CreationActuator(ctrl), row, carrier, disposition
    ).run()
    assert isinstance(result, dict), "source CAS must produce actual typed evidence"
    assert result["kind"] == "source_disposition_completed"
    assert result["outcome"] == "pin_disposed"
    assert result["source_observation"]["dv"]["uid"] == source["dv_uid"]
    assert result["source_observation"]["pvc"]["uid"] == source["pvc_uid"]
    assert result["source_observation"]["pin"] == result["plan"]["tombstone"]
    assert len(api.replacements) == 1
    # Merely returning controller evidence cannot release SQL authority.
    current = await service.retries.inspect(request_id=row["request_id"])
    assert current["state"] == "cancel_requested"
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
            UUID(disposition["admission_id"]),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dv_state,pvc_state",
    [
        ("absent", "absent"),
        ("absent", "same"),
        ("replaced", "same"),
        ("replaced", "replaced"),
        ("same", "absent"),
        ("same", "replaced"),
    ],
)
async def test_exact_frozen_source_gc_records_each_identity_without_reselecting(
    db, monkeypatch, setup, dv_state, pvc_state
):
    service, row, carrier, disposition, source = await frozen_golden(db, monkeypatch)
    ctrl, api = source_runtime(setup, service, row, source, monkeypatch)
    plan = (
        await service.authorize(
            request_id=row["request_id"], carrier=carrier, stage="source", source=source
        )
    )["plan"]
    original = deepcopy(api.objects)
    for kind, state in (("DataVolume", dv_state), ("PersistentVolumeClaim", pvc_state)):
        if state == "absent":
            del api.objects[kind, source["name"]]
        elif state == "replaced":
            api.objects[kind, source["name"]]["metadata"]["uid"] = str(uuid4())
    before = deepcopy(api.objects)
    current = await service.retries.inspect(request_id=row["request_id"])
    result = await DispositionSources(
        CreationActuator(ctrl), current, carrier, disposition
    ).run()
    assert result["plan"] == plan
    assert result["outcome"] == (
        "pin_disposed" if dv_state == "same" else "source_identity_gone"
    )
    for key, kind, state in (
        ("dv", "DataVolume", dv_state),
        ("pvc", "PersistentVolumeClaim", pvc_state),
    ):
        fact = result["source_observation"][key]
        if state == "absent":
            assert fact is None
        else:
            assert fact["uid"] == before[kind, source["name"]]["metadata"]["uid"]
            if state == "replaced":
                assert fact["uid"] != original[kind, source["name"]]["metadata"]["uid"]
    # No write to a replacement source or to any PVC, including the old surviving
    # PVC. Only a still-existing exact old DV needs its stale-writer CAS fence.
    assert len(api.replacements) == (1 if dv_state == "same" else 0)
    for key, value in before.items():
        if key != ("DataVolume", source["name"]) or dv_state != "same":
            assert api.objects[key] == value
    assert (await service.retries.inspect(request_id=row["request_id"]))[
        "state"
    ] == "cancel_requested"


@pytest.mark.asyncio
async def test_source_gc_cannot_hide_new_target_or_missing_persisted_source(
    db, monkeypatch, setup
):
    from vm_controller.creation_sources import GoldenWaiting

    service, row, carrier, disposition, source = await frozen_golden(db, monkeypatch)
    ctrl, api = source_runtime(setup, service, row, source, monkeypatch)
    # A missing source without any frozen identity is not evidence of source or
    # target non-issuance and cannot create a new cache entry.
    del api.objects["DataVolume", source["name"]]
    del api.objects["PersistentVolumeClaim", source["name"]]
    with pytest.raises(GoldenWaiting):
        await DispositionSources(
            CreationActuator(ctrl), row, carrier, disposition
        ).run()
    assert api.replacements == []
    plan = (
        await service.authorize(
            request_id=row["request_id"], carrier=carrier, stage="source", source=source
        )
    )["plan"]
    api.objects["DataVolume", plan["target"]["name"]] = {
        "metadata": {"name": plan["target"]["name"], "uid": str(uuid4())}
    }
    current = await service.retries.inspect(request_id=row["request_id"])
    with pytest.raises(ValueError, match="target still exists"):
        await DispositionSources(
            CreationActuator(ctrl), current, carrier, disposition
        ).run()
    assert api.replacements == []
