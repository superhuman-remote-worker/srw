"""No-VM fixed-resource and non-issuance facts become typed actual completion."""

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
from tests.test_vm_creation_disposition_actuation_real_postgres import runtime
from shared.vm_creation_disposition import disposition_identity
from vm_controller.creation_disposition import CreationDisposer

db, setup = _db_fixture, _setup_fixture


@pytest.mark.asyncio
async def test_fixed_root_and_secret_actual_completion_is_separate_from_progress(
    db, monkeypatch, setup
):
    ctrl, api, store, row, carrier = await runtime(db, setup, monkeypatch)
    await CreationDisposer(ctrl)._run(disposition_identity(row), {})
    current = await store.inspect(request_id=str(row["request_id"]))
    assert set(current["cancellation_completion"]) == {
        "cloud_init",
        "rootdisk",
        "source",
        "workspace_attachment",
    }
    for stage in ("cloud_init", "rootdisk"):
        assert (
            current["cancellation_completion"][stage]
            == current["cancellation_progress"][stage]
        )
    deletes = list(api.deletes)
    await CreationDisposer(ctrl)._run(disposition_identity(row), {})
    assert api.deletes == deletes
    assert (await store.inspect(request_id=str(row["request_id"])))[
        "state"
    ] == "settled"


@pytest.mark.asyncio
async def test_zero_root_and_secret_effects_require_distinct_never_issued_receipts(
    db, monkeypatch, setup
):
    service, row, carrier, disposition, source = await frozen_golden(db, monkeypatch)
    ctrl, api = source_runtime(setup, service, row, source, monkeypatch)
    api.objects["Lease", carrier["metadata"]["name"]] = carrier
    await CreationDisposer(ctrl)._run(disposition_identity(row), {})
    current = await service.retries.inspect(request_id=str(row["request_id"]))
    assert (
        current["cancellation_completion"]["rootdisk"]["kind"]
        == "rootdisk_never_issued"
    )
    assert (
        current["cancellation_completion"]["cloud_init"]["kind"]
        == "secret_never_issued"
    )
    assert current["state"] == "settled"
