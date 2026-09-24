"""Typed pinned-thread creation source uses real owner rows, never a Job."""

import json
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.services.vm_creation_request import build_vm_creation_request
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)
from orchestrator.services.vm_provisioner import VMProvisioner
from shared.vm_creation_issuance import canonical_configuration_digest
from shared.vm_creation_issuance import seal_creation_carrier
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
        installed = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='vm_creation_retries' "
            "AND column_name='owner_kind')"
        )
        if not installed:
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
async def test_actual_pinned_provision_cas_captures_thread_source_and_waiter(
    db, monkeypatch,
):
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
    admitted = await store.admit(request_id=str(request_id))
    assert admitted["action"] == "admitted", admitted
    claims = await VMCreationRetryStore(db).claim_due(limit=1)
    assert len(claims) == 1
    assert claims[0]["request_id"] == request_id
    assert claims[0]["thread_id"] == thread_id
    assert claims[0]["claim_token"] is not None
    authorized = await VMCreationRetryStore(db).authorize_controller(
        request_id=str(request_id), claim_token=str(claims[0]["claim_token"]),
        observed={
            "job_id": str(thread_id),
            "provision_generation": str(generation),
            "request_digest": source["request_digest"],
            "controller_configuration_digest": source["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    assert authorized["allowed"] is True, authorized
    assert authorized["resource_grant"]["id"] == admitted["reservation_id"]
    inspected = await VMCreationRetryStore(db).inspect(request_id=str(request_id))
    assert inspected["job_id"] == str(thread_id)
    assert inspected["owner_kind"] == "thread"
    assert inspected["thread_runtime_generation"] == str(runtime_generation)
    from vm_controller.creation_actuation import CreationActuator

    generated = CreationActuator.values(
        None, inspected, authorized, "rootdisk", None, None, None,
        {"kind": "registry", "image": request["vm_image"]},
    )
    assert generated["owner_kind"] == "thread"
    assert generated["thread_runtime_generation"] == str(runtime_generation)
    assert generated["job_id"] == str(thread_id)
    from tests.test_vm_creation_actuation import SECRET

    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())
    intent = {
        "version": 4, "resource_grant": authorized["resource_grant"],
        "rootdisk_source": {"kind": "registry", "image": request["vm_image"]},
        "source": "controller_vm_create",
        "admission_id": str(authorized["admission_id"]),
        "reservation_request_id": authorized["request_id"],
        "intent_digest": authorized["intent_digest"],
        "retry_request_id": str(request_id),
        "job_id": str(thread_id),
        "owner_kind": "thread",
        "thread_runtime_generation": str(runtime_generation),
        "thread_agent_id": None,
        "thread_attach_token": None,
        "thread_wake_operation_id": None,
        "provision_generation": str(generation),
        "request_digest": source["request_digest"],
        "controller_configuration_digest": source["controller_configuration_digest"],
        "expected_pvc_uid": None, "retained_dv_uid": None,
        "current_dv_uid": None, "current_pvc_uid": None,
        "current_secret_uid": None, "effect_kind": "rootdisk",
        "effect_nonce": str(uuid4()),
        "object_name": "agent-vm-" + str(thread_id) + "-rootdisk",
    }
    carrier = seal_creation_carrier(
        intent, namespace="workers", uid=str(uuid4()),
        resource_version="1", secret=SECRET,
    )
    granted = await VMCreationRetryStore(db).begin_effect(
        request_id=str(request_id), claim_token=str(claims[0]["claim_token"]),
        carrier=carrier,
    )
    assert granted["actuation_allowed"] is True, granted

    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(runtime_generation),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id), token=retirement["token"],
        generation=retirement["generation"], settle_status="ended",
    )
    retries = VMCreationRetryStore(db)
    assert (await retries.authorize_controller(
        request_id=str(request_id), claim_token=str(claims[0]["claim_token"]),
        observed={
            "job_id": str(thread_id),
            "provision_generation": str(generation),
            "request_digest": source["request_digest"],
            "controller_configuration_digest": source["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    ))["allowed"] is False
    with pytest.raises(VMCreationRetryConflict):
        await retries.begin_effect(
            request_id=str(request_id),
            claim_token=str(claims[0]["claim_token"]), carrier=carrier,
        )
    assert await retries.settle_never_issued(request_id=str(request_id)) == {
        "settled": False, "reason": "creation_effect_unresolved",
    }
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE request_id=$1", request_id,
    ) == "reserved"
    assert await retries.observe_effect(
        request_id=str(request_id), carrier=carrier,
        observation={
            "outcome": "rejected",
            "api_status": {
                "apiVersion": "v1", "kind": "Status", "status": "Failure",
                "code": 403, "reason": "Forbidden",
            },
        },
    ) == {"recorded": True, "effect_state": "rejected"}
    assert await retries.settle_never_issued(request_id=str(request_id)) == {
        "settled": True, "disposition": "never_issued",
    }
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE request_id=$1", request_id,
    ) == "released"


@pytest.mark.asyncio
async def test_real_thread_provisioner_uses_immutable_source_not_legacy_create(
    db, monkeypatch,
):
    store, inventory, _, _ = await environment(db)
    _, thread_id = await _thread(db, lane="pinned", status="created")
    runtime_generation = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id,
    )
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
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_RESOURCE_ADMISSION_CONFIG", json.dumps(store.policy_document))

    async def resolve(_client, request, *, secret):
        assert secret == b"thread-resource-test-secret"
        return {"request": request, "controller_configuration": config}

    async def legacy(*_args, **_kwargs):
        raise AssertionError("Legacy create transport must not run")

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    monkeypatch.setattr(VMProvisioner, "_create_http", legacy)
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.test"
    provisioner._http_client = object()
    provisioner._lifecycle_hmac_secret = b"thread-resource-test-secret"
    assert await provisioner.create_thread_vm(
        str(thread_id), vm_image="pinned:image", cpu_cores=8, memory="16Gi",
        expected_runtime_generation=str(runtime_generation),
        expected_agent_id=None, expected_attach_token=None,
        expected_vm_context=None,
    ) is True
    row = await db.fetchrow(
        "SELECT request_id,thread_id,job_id,canonical_request FROM vm_creation_retries "
        "WHERE thread_id=$1", thread_id,
    )
    assert row["thread_id"] == thread_id and row["job_id"] is None
    assert json.loads(row["canonical_request"])["entity_type"] == "thread"
    assert await db.fetchval(
        "SELECT count(*) FROM vm_resource_waiters WHERE request_id=$1", row["request_id"],
    ) == 1
    assert (await store.admit(request_id=str(row["request_id"])))["action"] == "admitted"
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(runtime_generation),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert retirement["state"] == "pending", retirement
    assert retirement["context"]["vm_creation_source"]["request_id"] == str(
        row["request_id"]
    )
    assert await db.fetchval(
        "SELECT state FROM vm_creation_retries WHERE request_id=$1",
        row["request_id"],
    ) == "queued"
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id), token=retirement["token"],
        generation=retirement["generation"], settle_status="ended",
    )
    assert await db.fetchval(
        "SELECT state FROM vm_creation_retries WHERE request_id=$1",
        row["request_id"],
    ) == "cancel_requested"
    # Begin freezes the source and closes fresh grants; it cannot itself
    # release a charge or infer that an already issued effect is absent.
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
        row["request_id"],
    ) == "reserved"
    assert await VMCreationRetryStore(db).settle_never_issued(
        request_id=str(row["request_id"])
    ) == {"settled": True, "disposition": "never_issued"}
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
        row["request_id"],
    ) == "released"
    assert await db.fetchval(
        "SELECT metadata ? 'vm' FROM threads WHERE id=$1", thread_id,
    ) is False
