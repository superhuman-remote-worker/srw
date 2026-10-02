"""An exact pre-setup release changes actor life without rewriting VM source."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services.vm_provisioner import VMProvisioner
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from orchestrator.services.vm_readiness import VMReadinessService
from orchestrator.services.vm_thread_network import document
from tests.test_pinned_vm_failed_initial_end_real_postgres import (
    _damage_first_edge,
    _release_binding,
)
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_cold_agent
from tests.test_vm_resource_thread_source_real_postgres import (
    _base_db,  # noqa: F401
    _ready_charged_thread,
    _schema_applied,  # noqa: F401
    _simulate_stale_thread_identity,
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


async def poll_original(db, monkeypatch, ready, current, **overrides):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.test"
    provisioner._http_client = object()
    provisioner._lifecycle_hmac_secret = b"test-secret"
    args = {
        "expected_runtime_generation": str(current["runtime_generation"]),
        "expected_agent_id": str(current["agent_id"]),
        "expected_attach_token": str(current["runtime_attach_token"]),
        "expected_vm_context": document(current["metadata"])["vm"],
        "poll": True,
    }
    return await provisioner.create_thread_vm(
        str(ready["thread_id"]), **(args | overrides)
    )


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption",
    ["missing", "actor", "attach", "pod", "cycle", "foreign", "protocol", "workspace"],
)
async def test_unproven_pre_setup_edge_refuses_poll_and_ready(
    pg_store,  # noqa: F811 - imported PostgreSQL fixture
    monkeypatch,
    corruption,
):
    ready, current, source = await successor(pg_store, monkeypatch)
    await _damage_first_edge(pg_store, source, corruption)
    assert await poll_original(pg_store, monkeypatch, ready, current) is False
    assert (
        await VMProvisioningPhaseStore(pg_store).publish_thread_ready(
            str(ready["thread_id"]),
            str(ready["generation"]),
            ready["registration"],
            ready["vm_uid"],
            ready["updates"],
        )
        is False
    )
    await unchanged(pg_store, ready, source)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "argument",
    ["expected_runtime_generation", "expected_agent_id", "expected_attach_token"],
)
async def test_pre_setup_poll_refuses_a_different_captured_actor(
    pg_store,  # noqa: F811 - imported PostgreSQL fixture
    monkeypatch,
    argument,
):
    ready, current, source = await successor(pg_store, monkeypatch)
    assert (
        await poll_original(
            pg_store, monkeypatch, ready, current, **{argument: str(uuid4())}
        )
        is False
    )
    await unchanged(pg_store, ready, source)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption",
    [
        "vm_uid",
        "rootdisk_pvc_uid",
        "terminal_intent",
        "terminal_status",
        "original_pod",
    ],
)
async def test_pre_setup_source_refuses_changed_physical_or_terminal_authority(
    pg_store,  # noqa: F811 - imported PostgreSQL fixture
    monkeypatch,
    corruption,
):
    ready, current, source = await successor(pg_store, monkeypatch)
    async with pg_store.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role='replica'")
        if corruption == "terminal_intent":
            await conn.execute(
                "UPDATE threads SET pinned_idle_terminal_intent_at=clock_timestamp() WHERE id=$1",
                ready["thread_id"],
            )
        elif corruption == "terminal_status":
            await conn.execute(
                "UPDATE threads SET status='ended',ended_at=clock_timestamp() WHERE id=$1",
                ready["thread_id"],
            )
        elif corruption == "original_pod":
            await conn.execute(
                "UPDATE thread_agent_pod_provision_intents SET pod_uid=$2 WHERE thread_id=$1",
                ready["thread_id"],
                str(uuid4()),
            )
        else:
            await conn.execute(
                "UPDATE threads SET metadata=jsonb_set(metadata,ARRAY['vm',$2::text],to_jsonb($3::text)) WHERE id=$1",
                ready["thread_id"],
                corruption,
                str(uuid4()),
            )
    assert await poll_original(pg_store, monkeypatch, ready, current) is False
    assert (
        await VMProvisioningPhaseStore(pg_store).publish_thread_ready(
            str(ready["thread_id"]),
            str(ready["generation"]),
            ready["registration"],
            ready["vm_uid"],
            ready["updates"],
        )
        is False
    )
    await unchanged(pg_store, ready, source)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["runtime_generation", "agent_id", "runtime_attach_token"]
)
async def test_successor_changed_after_ssh_read_cannot_admit_remote_writes(
    pg_store,  # noqa: F811 - imported PostgreSQL fixture
    monkeypatch,
    field,
):
    ready, current, source = await successor(pg_store, monkeypatch)
    row = next(
        row
        for row in await pg_store.list_thread_vm_readiness_candidates()
        if row["entity_id"] == str(ready["thread_id"])
    )
    vm = document(row["vm"])
    attestation = SimpleNamespace(
        workspace_generation=str(ready["generation"]),
        runtime_incarnation=ready["launcher_uid"],
        backing_id=ready["vm_uid"],
        host="10.42.0.42",
        port=22,
        ssh_host_key_fingerprint=vm["ssh_host_key_fingerprint"],
    )
    admitted = []

    async def seed(*args, mutation_authority, **kwargs):
        await _simulate_stale_thread_identity(
            pg_store, ready["thread_id"], field, uuid4()
        )
        admitted.append(await mutation_authority())
        return False

    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.wait_for_agent_ssh",
        AsyncMock(return_value=(True, 1, None)),
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_readiness.seed_ide_config_for_user", seed
    )
    provisioner = SimpleNamespace(
        query_status=AsyncMock(
            return_value={
                "phase": "Running",
                "ready": True,
                "provision_generation": str(ready["generation"]),
                "pod_ip": "10.42.0.42",
                "active_pod_uid": ready["launcher_uid"],
            }
        ),
        _set_context_if_generation=AsyncMock(return_value=True),
        attest_workspace_runtime=AsyncMock(return_value=attestation),
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
    assert admitted == [None]
    await unchanged(pg_store, ready, source)
