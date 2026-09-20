"""Cancelled source preparation can be found before any creation carrier exists."""

from uuid import UUID

import pytest

from tests.test_vm_creation_actuation import setup as _setup_fixture
from tests.test_vm_creation_effects_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    reserved,
    SECRET,
)
from shared.vm_creation_disposition import disposition_identity
from vm_controller import controller as settings

db = _db_fixture
setup = _setup_fixture


@pytest.mark.asyncio
@pytest.mark.parametrize("lost", [False, True])
async def test_precarrier_cancellation_freezes_same_admission_without_create_effects(
    db, setup, monkeypatch, lost
):
    from vm_controller.creation_disposition import CreationDisposer

    ctrl, api, _, _ = setup
    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", True)
    monkeypatch.setattr(settings, "LIFECYCLE_HMAC_SECRET", SECRET)
    store, row, _, _ = await reserved(db, monkeypatch)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    calls = []

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        calls.append(method)
        assert operation == "creation_retry_" + method
        assert method not in {"authorize", "begin_effect"}
        return await getattr(store, method)(**body)

    ctrl._workspace_cleanup_authority_request = authority
    if lost:
        api.lost.add("Lease")
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_disposition_pending"
    assert api.writes == ["Lease"]
    repeat = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert repeat == result
    assert api.writes == ["Lease"]
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
                row["request_id"],
            )
            == 0
        )
        frozen = await store.inspect(request_id=str(row["request_id"]))
        assert frozen["cancellation_disposition"]["source_resolution"] == "unknown"
        assert await conn.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
            UUID(frozen["creation_admission_id"]),
        )


@pytest.mark.asyncio
async def test_changed_cancel_identity_is_refused_before_carrier_publication(
    db, setup, monkeypatch
):
    from vm_controller.creation_disposition import CreationDisposer

    ctrl, api, _, _ = setup
    monkeypatch.setattr(settings, "LIFECYCLE_HMAC_SECRET", SECRET)
    store, row, _, _ = await reserved(db, monkeypatch)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")

    async def authority(path, body, *, operation):
        assert path.endswith("/inspect")
        return await store.inspect(**body)

    ctrl._workspace_cleanup_authority_request = authority
    identity = disposition_identity(row)
    identity["request_digest"] = "sha256:" + "0" * 64
    result = await CreationDisposer(ctrl).run(identity)
    assert result["status"] == "creation_attention"
    assert api.writes == []
