"""An exact pre-setup release changes actor life without rewriting VM source."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services.vm_provisioner import VMProvisioner
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from orchestrator.services.vm_readiness import VMReadinessService
from orchestrator.services.vm_thread_network import document
from tests.test_pinned_vm_failed_initial_end_real_postgres import _release_binding
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent
from tests.test_vm_resource_thread_source_real_postgres import (
    _base_db,  # noqa: F401
    _ready_charged_thread,
    _schema_applied,  # noqa: F401
    db as pg_store,  # noqa: F401
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)


async def successor(db, monkeypatch):
    ready = await _ready_charged_thread(db, monkeypatch, bind_actor=_bind_cold_agent)
    before = await db.get_thread(str(ready["thread_id"]))
    source = dict(
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", ready["request_id"]
        )
    )
    assert json.loads(source["controller_configuration"])["version"] == 3
    assert await _release_binding(db, before) == "released"
    assert await db.delete_agent(str(before["agent_id"]))
    current = await _bind_cold_agent(db, ready["thread_id"])
    assert current["runtime_generation"] != before["runtime_generation"]
    assert current["agent_id"] != before["agent_id"]
    assert await db.fetchval(
        "SELECT public.vm_thread_creation_pre_setup_abort_evidence(t,c) IS NOT NULL "
        "FROM threads t JOIN vm_creation_retries c ON c.thread_id=t.id "
        "WHERE t.id=$1 AND c.request_id=$2",
        ready["thread_id"],
        ready["request_id"],
    )
    return ready, current, source


async def unchanged(db, ready, source):
    after = dict(
        await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1", ready["request_id"]
        )
    )
    # Ready publication owns only its timestamp/revision; creation authority is frozen.
    assert {
        k: v
        for k, v in after.items()
        if k not in {"ready_at", "revision", "updated_at"}
    } == {
        k: v
        for k, v in source.items()
        if k not in {"ready_at", "revision", "updated_at"}
    }


@pytest.mark.asyncio
async def test_confirmed_pre_setup_successor_reaches_actual_ssh_readiness_gate(
    pg_store,  # noqa: F811 - imported PostgreSQL fixture
    monkeypatch,
):
    ready, current, source = await successor(pg_store, monkeypatch)
    rows = await pg_store.list_thread_vm_readiness_candidates()
    row = next(row for row in rows if row["entity_id"] == str(ready["thread_id"]))
    vm = document(row["vm"])
    ssh = AsyncMock(return_value=(False, 1, "controlled SSH observation"))
    monkeypatch.setattr("orchestrator.services.vm_readiness.wait_for_agent_ssh", ssh)
    provisioner = SimpleNamespace(
        query_status=AsyncMock(
            return_value=dict(
                phase="Running",
                ready=True,
                provision_generation=str(ready["generation"]),
                pod_ip="10.42.0.42",
                active_pod_uid=ready["launcher_uid"],
            )
        ),
        _set_context_if_generation=AsyncMock(return_value=True),
    )
    await VMReadinessService(
        pg_store, provisioner, trigger_dispatch=lambda: None
    )._probe(
        "thread",
        str(ready["thread_id"]),
        str(ready["generation"]),
        vm,
        row,
        False,
    )
    assert ssh.await_count == 1
    await unchanged(pg_store, ready, source)


@pytest.mark.asyncio
async def test_confirmed_pre_setup_successor_polls_original_request_without_creation(
    pg_store,  # noqa: F811 - imported PostgreSQL fixture
    monkeypatch,
):
    ready, current, source = await successor(pg_store, monkeypatch)
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    provisioner = VMProvisioner()
    provisioner._db = pg_store
    provisioner._controller_url = "http://controller.test"
    provisioner._http_client = object()
    provisioner._lifecycle_hmac_secret = b"test-secret"
    assert (
        await provisioner.create_thread_vm(
            str(ready["thread_id"]),
            expected_runtime_generation=str(current["runtime_generation"]),
            expected_agent_id=str(current["agent_id"]),
            expected_attach_token=str(current["runtime_attach_token"]),
            expected_vm_context=document(current["metadata"])["vm"],
            poll=True,
        )
        is True
    )
    await unchanged(pg_store, ready, source)


@pytest.mark.asyncio
async def test_confirmed_pre_setup_successor_publishes_ready_under_locked_current_life(
    pg_store,  # noqa: F811 - imported PostgreSQL fixture
    monkeypatch,
):
    ready, current, source = await successor(pg_store, monkeypatch)
    assert (
        await VMProvisioningPhaseStore(pg_store).publish_thread_ready(
            str(ready["thread_id"]),
            str(ready["generation"]),
            ready["registration"],
            ready["vm_uid"],
            ready["updates"],
        )
        is True
    )
    after = await pg_store.get_thread(str(ready["thread_id"]))
    assert after["runtime_generation"] == current["runtime_generation"]
    assert document(after["metadata"])["vm"]["status"] == "ready"
    await unchanged(pg_store, ready, source)


@pytest.mark.asyncio
async def test_readiness_candidate_carries_exact_current_binding(
    pg_store,  # noqa: F811 - imported PostgreSQL fixture
    monkeypatch,
):
    ready, current, source = await successor(pg_store, monkeypatch)
    rows = await pg_store.list_thread_vm_readiness_candidates()
    row = next(row for row in rows if row["entity_id"] == str(ready["thread_id"]))
    assert row.get("agent_id") == str(current["agent_id"])
    assert row.get("runtime_attach_token") == str(current["runtime_attach_token"])
    assert row["runtime_generation"] == str(current["runtime_generation"])
    await unchanged(pg_store, ready, source)
