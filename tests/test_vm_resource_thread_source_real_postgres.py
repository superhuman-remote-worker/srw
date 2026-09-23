"""Typed pinned-thread creation source uses real owner rows, never a Job."""

import json
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.services.vm_creation_request import build_vm_creation_request
from orchestrator.services.vm_provisioner import VMProvisioner
from shared.vm_creation_issuance import canonical_configuration_digest
from shared.vm_creation_retry import canonical_request_digest
from shared.vm_launcher_profile import predict_launcher
from tests.test_b10_session_queries_real_postgres import (
    _schema_applied,  # noqa: F401
    _thread,
    db as _base_db,  # noqa: F401
    pg_dsn,  # noqa: F401
)
from tests.test_vm_resource_configuration import whole_launcher_configuration
from tests.test_vm_resource_whole_store_real_postgres import environment


MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "src/orchestrator/database/migrations/app/0278_vm_resource_thread_runtime.sql"
)


@pytest_asyncio.fixture(scope="module")
async def thread_schema(pg_dsn, _schema_applied):  # noqa: F811
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(MIGRATION.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(thread_schema, _base_db):  # noqa: F811
    yield _base_db


@pytest.mark.asyncio
async def test_thread_creation_source_has_exclusive_owner_and_frozen_identity(db):
    _, thread_id = await _thread(db, lane="pinned", status="created")
    runtime_generation = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id,
    )
    request_id, generation = uuid4(), uuid4()
    request = {
        "job_id": str(thread_id), "entity_type": "thread",
        "provision_generation": str(generation),
    }
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) "
        "WHERE id=$1",
        thread_id, json.dumps({"status": "provisioning", "provision_generation": str(generation)}),
    )
    row = await db.fetchrow(
        "INSERT INTO vm_creation_retries "
        "(request_id,owner_kind,thread_id,thread_runtime_generation,"
        "provision_generation,origin,request_digest,canonical_request,"
        "controller_configuration_digest,controller_configuration) "
        "VALUES($1,'thread',$2,$3,$4,'initial',$5,$6::jsonb,$7,$8::jsonb) "
        "RETURNING job_id,thread_id,owner_kind,thread_runtime_generation",
        request_id, thread_id, runtime_generation, generation,
        "sha256:" + "a" * 64, json.dumps(request),
        "sha256:" + "b" * 64, json.dumps({"version": 3}),
    )
    assert row["job_id"] is None
    assert row["thread_id"] == thread_id
    assert row["owner_kind"] == "thread"
    assert row["thread_runtime_generation"] == runtime_generation


@pytest.mark.asyncio
async def test_thread_waiter_uses_same_resource_ledger_with_real_owner(db):
    owner, thread_id = await _thread(db, lane="pinned", status="created")
    generation, request_id = uuid4(), uuid4()
    runtime_generation = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id,
    )
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) WHERE id=$1",
        thread_id, json.dumps({"status": "provisioning", "provision_generation": str(generation)}),
    )
    request_digest = "sha256:" + "a" * 64
    policy_digest = "sha256:" + "b" * 64
    await db.execute(
        "INSERT INTO vm_creation_retries "
        "(request_id,owner_kind,thread_id,thread_runtime_generation,"
        "provision_generation,origin,request_digest,canonical_request,"
        "controller_configuration_digest,controller_configuration) "
        "VALUES($1,'thread',$2,$3,$4,'initial',$5,$6::jsonb,$7,$8::jsonb)",
        request_id, thread_id, runtime_generation, generation,
        request_digest, json.dumps({
            "job_id": str(thread_id), "entity_type": "thread",
            "provision_generation": str(generation),
        }), policy_digest, json.dumps({"version": 3}),
    )
    await db.execute(
        "INSERT INTO vm_resource_admission_policy "
        "(cluster_id,namespace,policy_digest,document,mode) "
        "VALUES('thread-test','thread-test',$1,'{}'::jsonb,'off')",
        policy_digest,
    )
    await db.execute(
        "INSERT INTO vm_resource_waiters "
        "(request_id,owner_kind,thread_id,provision_generation,cluster_id,"
        "policy_digest,owner_key,priority,request_digest,guest_vcpus,"
        "guest_memory_bytes,cpu_millicores,memory_bytes,kvm_devices,"
        "ephemeral_storage_bytes,tun_devices,vhost_net_devices,resource_version,placement) "
        "VALUES($1,'thread',$2,$3,'thread-test',$4,$5,0,$6,1,1073741824,"
        "1000,1073741824,1,1073741824,1,1,2,'{}'::jsonb)",
        request_id, thread_id, generation, policy_digest,
        "user:" + str(owner), request_digest,
    )
    assert await db.fetchval(
        "SELECT job_id IS NULL AND thread_id=$2 AND owner_kind='thread' "
        "FROM vm_resource_waiters WHERE request_id=$1", request_id, thread_id,
    )


@pytest.mark.asyncio
async def test_actual_pinned_provision_cas_captures_thread_source_and_waiter(db):
    store, inventory, _, _ = await environment(db)
    owner, thread_id = await _thread(db, lane="pinned", status="created")
    runtime_generation = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id,
    )
    generation, request_id = uuid4(), uuid4()
    config = whole_launcher_configuration()
    config.update(namespace="workers", storage_class="local")
    resource = config["resource_admission"]
    resource["cluster_id"] = inventory.cluster_id
    resource["policy_digest"] = inventory.policy_digest
    resource["template_profile"].update(
        storage_class="local", guest_vcpus=8, guest_memory_bytes=16 * 1024**3,
    )
    resource["launcher_prediction"]["vector"] = predict_launcher(
        store.launcher_profile, guest_vcpus=8, guest_memory_bytes=16 * 1024**3,
    ).to_six_dict()
    resource["host_mapping"]["vector"] = store.cost.cost(8, "16Gi").to_six_dict()
    request = build_vm_creation_request(
        job_id=str(thread_id), entity_type="thread", agent_config="worker_base",
        vm_image="pinned:image", cpu_cores=8, memory="16Gi",
        description="thread", network_tier="restricted",
        provision_generation=str(generation),
    )
    proposed = VMProvisioner._fresh_provision_ctx()
    proposed.update(status="provisioning", provision_generation=str(generation))
    assert await db.begin_pinned_thread_vm_provisioning(
        str(thread_id), expected_runtime_generation=str(runtime_generation),
        expected_agent_id=None, expected_attach_token=None,
        expected_vm_context=None, provision_context=proposed,
        creation_source={
            "request_id": str(request_id), "request": request,
            "request_digest": canonical_request_digest(request),
            "controller_configuration": config,
            "controller_configuration_digest": canonical_configuration_digest(config),
        },
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", request_id,
    )
    waiter = await db.fetchrow(
        "SELECT * FROM vm_resource_waiters WHERE request_id=$1", request_id,
    )
    assert source["thread_id"] == thread_id and source["job_id"] is None
    assert source["thread_owner_user_id"] == owner
    assert waiter["thread_id"] == thread_id and waiter["job_id"] is None
    assert waiter["owner_key"] == "user:" + str(owner)
