"""The initial caller freezes provenance without sending a legacy create."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.test_vm_creation_preflight_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    initial_job,
)  # noqa: F401
from tests.test_vm_creation_request import SECRET
from orchestrator.services.vm_provisioner import VMProvisioner


db = _db_fixture


def provisioner(db, monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    result = VMProvisioner()
    result._db = db
    result._lifecycle_hmac_secret = SECRET
    result._http_client = SimpleNamespace(post=AsyncMock())
    result._create_http = AsyncMock(
        side_effect=AssertionError("legacy create forbidden")
    )
    result._set_vm_context = AsyncMock(
        side_effect=AssertionError("generic generation write forbidden")
    )
    monkeypatch.setattr(
        db, "get_workspace_network_tier", AsyncMock(return_value="restricted")
    )
    return result


@pytest.mark.asyncio
async def test_protocol_initial_create_freezes_before_transport_and_repeated_call_keeps_generation(
    db, monkeypatch
):
    job = await initial_job(db)
    creator = provisioner(db, monkeypatch)
    first = await creator.create_vm(
        str(job), vm_image="pinned-image", memory="2Gi", cpu_cores=2
    )
    again = await creator.create_vm(
        str(job), vm_image="changed-current-default", memory="64Gi", fresh=False
    )
    assert first["creation_retry_protocol"] == 1
    assert again == first
    creator._http_client.post.assert_not_awaited()
    creator._create_http.assert_not_awaited()
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        frozen = context["vm"]["creation_preflight"]["request"]
        assert frozen["vm_image"] == "pinned-image"
        assert frozen["memory"] == "2Gi"
        assert context["vm"]["provision_attempts"] == 0
        assert context["_vm_creation_pending"] == first["request_id"]


@pytest.mark.asyncio
async def test_feature_off_keeps_existing_protocol_on_its_durable_path(db, monkeypatch):
    job = await initial_job(db)
    creator = provisioner(db, monkeypatch)
    first = await creator.create_vm(str(job))
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "false")
    assert await creator.create_vm(str(job)) == first
    creator._create_http.assert_not_awaited()
    creator._http_client.post.assert_not_awaited()


@pytest.mark.asyncio
async def test_protocol_persistence_failure_cannot_fall_back_to_legacy_creation(
    db, monkeypatch
):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore

    job = await initial_job(db)
    creator = provisioner(db, monkeypatch)
    monkeypatch.setattr(
        VMCreationPreflightStore,
        "begin",
        AsyncMock(side_effect=RuntimeError("database unavailable")),
    )
    with pytest.raises(RuntimeError):
        await creator.create_vm(str(job))
    creator._http_client.post.assert_not_awaited()
    creator._create_http.assert_not_awaited()


@pytest.mark.asyncio
async def test_existing_legacy_generation_is_not_upgraded_by_enabling_protocol(
    db, monkeypatch
):
    from uuid import uuid4
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

    old = {"vm": {"status": "failed", "provision_generation": str(uuid4())}}
    job = await initial_job(db, context=old)
    creator = provisioner(db, monkeypatch)
    with pytest.raises(VMCreationRetryConflict):
        await creator.create_vm(str(job))
    creator._http_client.post.assert_not_awaited()
    async with db.acquire() as conn:
        assert (
            json.loads(await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job))
            == old
        )
