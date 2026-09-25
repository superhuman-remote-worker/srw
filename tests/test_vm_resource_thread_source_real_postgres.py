"""Typed pinned-thread creation source uses real owner rows, never a Job."""

import asyncio
from datetime import datetime, timezone
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
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from shared.vm_creation_issuance import (
    EFFECT_NONCE_ANNOTATION,
    REQUEST_ANNOTATION,
    canonical_configuration_digest,
    seal_creation_carrier,
)
from shared.vm_creation_retry import canonical_request_digest
from shared.vm_launcher_profile import predict_launcher
from tests.test_b10_session_queries_real_postgres import (
    _schema_applied,  # noqa: F401
    _thread,
    db as _base_db,  # noqa: F401
    pg_dsn,  # noqa: F401
)
from tests.test_vm_resource_configuration import whole_launcher_configuration
from tests.test_vm_resource_inventory_real_postgres import publish, successor
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
        thread_id, json.dumps({"status": "provisioning", "provision_generation": str(generation),
                               "creation_request_id": str(request_id)}),
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
@pytest.mark.parametrize(
    "missing",
    ["vm", "vm_scalar", "vm_generation", "vm_status", "vm_request", "entity_type",
     "request_owner", "config_version", "null_config_version"],
)
async def test_native_thread_source_requires_complete_exact_identity(db, missing):
    _, thread_id = await _thread(db, lane="pinned", status="created")
    runtime = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id,
    )
    request_id, generation = uuid4(), uuid4()
    request = {
        "job_id": str(thread_id), "entity_type": "thread",
        "provision_generation": str(generation),
    }
    vm = {
        "status": "provisioning", "provision_generation": str(generation),
        "creation_request_id": str(request_id),
    }
    config = {"version": 3}
    if missing == "vm_generation":
        del vm["provision_generation"]
    elif missing == "vm_scalar":
        vm = "invalid"
    elif missing == "vm_status":
        del vm["status"]
    elif missing == "vm_request":
        del vm["creation_request_id"]
    elif missing == "entity_type":
        del request["entity_type"]
    elif missing == "request_owner":
        del request["job_id"]
    elif missing == "config_version":
        del config["version"]
    elif missing == "null_config_version":
        config["version"] = None
    if missing != "vm":
        await db.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) WHERE id=$1",
            thread_id, json.dumps(vm),
        )
    expected_message = (
        "VM thread creation owner changed"
        if missing.startswith("vm") else "VM thread creation request identity mismatch"
    )
    with pytest.raises(asyncpg.CheckViolationError, match=expected_message):
        await db.execute(
            "INSERT INTO vm_creation_retries "
            "(request_id,owner_kind,thread_id,thread_runtime_generation,"
            "provision_generation,origin,request_digest,canonical_request,"
            "controller_configuration_digest,controller_configuration) "
            "VALUES($1,'thread',$2,$3,$4,'initial',$5,$6::jsonb,$7,$8::jsonb)",
            request_id, thread_id, runtime, generation,
            "sha256:" + "a" * 64, json.dumps(request),
            "sha256:" + "b" * 64, json.dumps(config),
        )


@pytest.mark.asyncio
async def test_native_wake_source_rejects_missing_wake_request_identity(db):
    _, thread_id = await _thread(db, lane="pinned", status="created")
    runtime = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id,
    )
    request_id, generation, pvc_uid, operation_id = (uuid4() for _ in range(4))
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) WHERE id=$1",
        thread_id, json.dumps({
            "status": "provisioning", "provision_generation": str(generation),
            "creation_request_id": str(request_id),
        }),
    )
    # Isolate 0278's source guard with a historical malformed operation; the
    # ordinary idle writer never publishes this incomplete wake tuple.
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role = 'replica'")
        await conn.execute(
            "INSERT INTO vm_idle_operations "
            "(id,owner_kind,owner_id,phase,episode_id,episode_revision,"
            "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,"
            "retained_kind,release_kind,thread_runtime_generation,"
            "thread_retirement_token,thread_agent_pod_identity,"
            "stop_evidence,stop_verified_at) "
            "VALUES($1,'thread',$2,'waking',$3,1,$4,$5,$6,$7,$8,"
            "'rootdisk','pinned_thread',$9,$10,'{}'::jsonb,'{}'::jsonb,"
            "clock_timestamp())",
            operation_id, thread_id, uuid4(), uuid4(), uuid4(), uuid4(), uuid4(),
            pvc_uid, runtime, uuid4(),
        )
    with pytest.raises(asyncpg.CheckViolationError, match="VM thread wake source changed"):
        await db.execute(
            "INSERT INTO vm_creation_retries "
            "(request_id,owner_kind,thread_id,thread_runtime_generation,"
            "thread_wake_operation_id,expected_pvc_uid,provision_generation,"
            "origin,request_digest,canonical_request,"
            "controller_configuration_digest,controller_configuration) "
            "VALUES($1,'thread',$2,$3,$4,$5,$6,'initial',$7,$8::jsonb,$9,$10::jsonb)",
            request_id, thread_id, runtime, operation_id, pvc_uid, generation,
            "sha256:" + "a" * 64,
            json.dumps({"job_id": str(thread_id), "entity_type": "thread",
                        "provision_generation": str(generation)}),
            "sha256:" + "b" * 64, json.dumps({"version": 3}),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_first", [False, True])
async def test_thread_source_and_cleanup_permit_do_not_deadlock_on_observed_pvc(
    db, cleanup_first,
):
    _, thread_id = await _thread(db, lane="pinned", status="created")
    runtime = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id,
    )
    request_id, generation, pvc = uuid4(), uuid4(), uuid4()
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) WHERE id=$1",
        thread_id, json.dumps({
            "status": "provisioning", "provision_generation": str(generation),
            "creation_request_id": str(request_id),
        }),
    )
    source = await db.fetchrow(
        "INSERT INTO vm_creation_retries "
        "(request_id,owner_kind,thread_id,thread_runtime_generation,"
        "provision_generation,origin,request_digest,canonical_request,"
        "controller_configuration_digest,controller_configuration,observed_pvc_uid) "
        "VALUES($1,'thread',$2,$3,$4,'initial',$5,$6::jsonb,$7,$8::jsonb,$9) "
        "RETURNING *",
        request_id, thread_id, runtime, generation,
        "sha256:" + "a" * 64,
        json.dumps({"job_id": str(thread_id), "entity_type": "thread",
                    "provision_generation": str(generation)}),
        "sha256:" + "b" * 64, json.dumps({"version": 3}), pvc,
    )
    scoped, owner_locked, authorization_started = (
        asyncio.Event(), asyncio.Event(), asyncio.Event()
    )
    cleanup = VMWorkspaceRecoveryStore(db)
    arguments = dict(
        owner_kind="thread", owner_id=thread_id, pvc_uid=pvc,
        source="controller_vm_create", intent_digest="test-intent",
    )

    async def authorization():
        if cleanup_first:
            await owner_locked.wait()
        async with db.acquire() as conn, conn.transaction():
            authorization_started.set()
            await VMCreationRetryStore(db)._thread_scope(conn, source)
            scoped.set()
            if not cleanup_first:
                try:
                    await asyncio.wait_for(owner_locked.wait(), 0.2)
                except TimeoutError:
                    # Owner-first ordering prevents the competing cleanup from
                    # reaching its PVC lock until this transaction commits.
                    pass
            return await cleanup.acquire_cleanup_permit_on_conn(
                conn, request_id=uuid4(), **arguments,
            )

    async def ordinary_cleanup():
        if not cleanup_first:
            await scoped.wait()
        async with db.acquire() as conn, conn.transaction():
            class Connection:
                def __getattr__(self, key):
                    return getattr(conn, key)

                async def execute(self, query, *args):
                    result = await conn.execute(query, *args)
                    if args == (f"workspace-recovery:thread:{thread_id}",):
                        owner_locked.set()
                        if cleanup_first:
                            await authorization_started.wait()
                            await asyncio.sleep(0.1)
                    return result

            return await cleanup.acquire_cleanup_permit_on_conn(
                Connection(), request_id=uuid4(), **arguments,
            )

    results = await asyncio.wait_for(
        asyncio.gather(authorization(), ordinary_cleanup(), return_exceptions=True), 8,
    )
    unexpected = [
        result for result in results
        if isinstance(result, BaseException)
        and not (
            cleanup_first
            and isinstance(result, VMCreationRetryConflict)
            and str(result) == "workspace_cleanup_already_admitted"
        )
    ]
    assert not unexpected, results
    if cleanup_first:
        assert isinstance(results[0], VMCreationRetryConflict), results


async def _adopted_charged_thread(
    db, monkeypatch, *, stop_after="vm", adopt=True, observe_last=True,
    stop_before_effect=False, golden_enabled=False,
):
    """Real typed CAS, shared admit, signed effects and optional adoption."""
    from tests.test_vm_creation_actuation import SECRET

    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())
    policy, inventory, sample, demand = await environment(db)
    _, thread_id = await _thread(db, lane="pinned", status="created")
    runtime = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id,
    )
    generation, request_id = uuid4(), uuid4()
    config = whole_launcher_configuration()
    config.update(namespace="workers", storage_class="local")
    if golden_enabled:
        config["golden_enabled"] = True
    resource = config["resource_admission"]
    resource["cluster_id"] = inventory.cluster_id
    resource["policy_digest"] = inventory.policy_digest
    resource["template_profile"].update(
        storage_class="local", guest_vcpus=8, guest_memory_bytes=16 * 1024**3,
    )
    resource["launcher_prediction"]["vector"] = predict_launcher(
        policy.launcher_profile, guest_vcpus=8, guest_memory_bytes=16 * 1024**3,
    ).to_six_dict()
    resource["host_mapping"]["vector"] = policy.cost.cost(8, "16Gi").to_six_dict()
    request = build_vm_creation_request(
        job_id=str(thread_id), entity_type="thread", agent_config="worker_base",
        vm_image="pinned:image", cpu_cores=8, memory="16Gi",
        description="thread", network_tier="restricted",
        provision_generation=str(generation),
    )
    proposed = VMProvisioner._fresh_provision_ctx()
    proposed.update(status="provisioning", provision_generation=str(generation))
    assert await db.begin_pinned_thread_vm_provisioning(
        str(thread_id), expected_runtime_generation=str(runtime),
        expected_agent_id=None, expected_attach_token=None,
        expected_vm_context=None, provision_context=proposed,
        creation_source={
            "request_id": str(request_id), "request": request,
            "request_digest": canonical_request_digest(request),
            "controller_configuration": config,
            "controller_configuration_digest": canonical_configuration_digest(config),
        },
    )
    admitted = await policy.admit(request_id=str(request_id))
    assert admitted["action"] == "admitted"
    retry = VMCreationRetryStore(db)
    claim = (await retry.claim_due(limit=1))[0]
    grant = await retry.authorize_controller(
        request_id=str(request_id), claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(thread_id), "provision_generation": str(generation),
            "request_digest": canonical_request_digest(request),
            "controller_configuration_digest": canonical_configuration_digest(config),
            "expected_pvc_uid": None,
        },
    )
    assert grant["allowed"] is True
    if stop_before_effect:
        return (
            policy, inventory, sample, demand, thread_id, runtime, generation,
            request_id, admitted, {}, None,
        )
    values = {
        "version": 4, "resource_grant": grant["resource_grant"],
        "rootdisk_source": {"kind": "registry", "image": request["vm_image"]},
        "source": "controller_vm_create",
        "admission_id": str(grant["admission_id"]),
        "reservation_request_id": grant["request_id"],
        "intent_digest": grant["intent_digest"],
        "retry_request_id": str(request_id), "job_id": str(thread_id),
        "owner_kind": "thread", "thread_runtime_generation": str(runtime),
        "thread_agent_id": None, "thread_attach_token": None,
        "thread_wake_operation_id": None,
        "provision_generation": str(generation),
        "request_digest": canonical_request_digest(request),
        "controller_configuration_digest": canonical_configuration_digest(config),
        "expected_pvc_uid": None, "retained_dv_uid": None,
        "current_dv_uid": None, "current_pvc_uid": None,
        "current_secret_uid": None,
        "effect_kind": "rootdisk", "effect_nonce": str(uuid4()),
        "object_name": "agent-vm-" + str(thread_id) + "-rootdisk",
    }
    carrier_uid = str(uuid4())
    observations = {}
    for kind in ("rootdisk", "cloud_init", "vm"):
        if kind != "rootdisk":
            values = {
                **values, "effect_kind": kind, "effect_nonce": str(uuid4()),
                "object_name": "agent-vm-" + str(thread_id)
                + ("-cloudinit" if kind == "cloud_init" else ""),
                "current_dv_uid": observations["rootdisk"]["object"]["metadata"]["uid"],
                "current_pvc_uid": observations["rootdisk"]["pvc"]["metadata"]["uid"],
                "current_secret_uid": observations["cloud_init"]["object"]["metadata"]["uid"]
                if kind == "vm" else None,
            }
        carrier = seal_creation_carrier(
            values, namespace="workers", uid=carrier_uid,
            resource_version="3", secret=SECRET,
        )
        assert (await retry.begin_effect(
            request_id=str(request_id), claim_token=str(claim["claim_token"]),
            carrier=carrier,
        ))["actuation_allowed"] is True
        if kind == stop_after and not observe_last:
            break
        object_metadata = {
            "uid": str(uuid4()), "name": values["object_name"],
            "namespace": "workers",
            "labels": {"srw.io/owner-kind": "thread", "srw.io/owner-id": str(thread_id)},
            "annotations": {
                EFFECT_NONCE_ANNOTATION: values["effect_nonce"],
                REQUEST_ANNOTATION: str(request_id),
                "srw.io/provision-generation": str(generation),
                "srw.io/ssh-host-key-fingerprint": "SHA256:" + "A" * 43,
            },
        }
        if kind == "vm":
            object_metadata["annotations"].update({
                "srw.io/vm-resource-reservation": admitted["reservation_id"],
                "srw.io/vm-resource-node-uid": admitted["node_uid"],
            })
        if kind == "rootdisk":
            observation = {
                "outcome": "observed",
                "object": {"apiVersion": "cdi.kubevirt.io/v1beta1", "kind": "DataVolume",
                           "metadata": object_metadata,
                           "spec": {"source": {"registry": {
                               "url": "docker://" + request["vm_image"]}}}},
                "pvc": {"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                        "metadata": {
                            "uid": str(uuid4()), "name": values["object_name"],
                            "namespace": "workers",
                            "ownerReferences": [{"kind": "DataVolume",
                                                 "uid": object_metadata["uid"]}],
                        }},
            }
        else:
            observation = {
                "outcome": "observed",
                "object": {
                    "apiVersion": "v1" if kind == "cloud_init" else "kubevirt.io/v1",
                    "kind": "Secret" if kind == "cloud_init" else "VirtualMachine",
                    "metadata": object_metadata,
                    "spec": {"template": {"metadata": {"annotations": {
                        "srw.io/vm-resource-reservation": admitted["reservation_id"],
                        "srw.io/vm-resource-node-uid": admitted["node_uid"],
                        "srw.io/provision-generation": str(generation),
                    }}, "spec": {"volumes": [
                        {"name": "rootdisk", "dataVolume": {
                            "name": "agent-vm-" + str(thread_id) + "-rootdisk"}},
                        {"name": "cloud-init", "cloudInitNoCloud": {"secretRef": {
                            "name": "agent-vm-" + str(thread_id) + "-cloudinit"}}},
                    ]}}},
                },
            }
        assert (await retry.observe_effect(
            request_id=str(request_id), carrier=carrier,
            observation=observation,
        ))["recorded"] is True
        observations[kind] = observation
        if kind == stop_after:
            break
    if adopt:
        assert stop_after == "vm"
        assert await retry.settle_adopted(
            request_id=str(request_id), carrier=carrier, observations=observations,
        ) == {"settled": True, "disposition": "adopted"}
    return (
        policy, inventory, sample, demand, thread_id, runtime, generation,
        request_id, admitted, observations, carrier,
    )


async def _ready_charged_thread(db, monkeypatch):
    (
        _, inventory, sample, demand, thread_id, runtime, generation,
        request_id, admitted, observations, _,
    ) = await _adopted_charged_thread(db, monkeypatch)
    vm_uid = observations["vm"]["object"]["metadata"]["uid"]
    vmi_uid, launcher_uid, registration = (str(uuid4()) for _ in range(3))
    node_uid, node_name = admitted["node_uid"], admitted["node_name"]
    observed = successor(sample)
    observed["vms"] = [{
        "uid": vm_uid, "name": "agent-vm-" + str(thread_id),
        "owner_kind": "thread", "owner_id": str(thread_id),
        "provision_generation": str(generation), "deleting": False,
    }]
    observed["vmis"] = [{
        "uid": vmi_uid, "name": "agent-vm-" + str(thread_id),
        "vm_uid": vm_uid, "node_uid": node_uid, "node_name": node_name,
        "phase": "Running", "deleting": False,
    }]
    observed["pods"] = [{
        "uid": launcher_uid, "namespace": "workers", "name": "virt-launcher-thread",
        "node_uid": node_uid, "node_name": node_name,
        "terminal": False, "deleting": False,
        "requests": demand.to_six_dict(), "vmi_uid": vmi_uid,
        "reservation_id": admitted["reservation_id"],
        "provision_generation": str(generation),
    }]
    await publish(inventory, observed)
    pending = {
        "status": "ssh_pending", "vmi_uid": vmi_uid,
        "active_pod_uid": launcher_uid, "ssh_registration_id": registration,
        "ssh_host": "10.42.0.42", "pod_ip": "10.42.0.42", "ssh_port": 22,
        "ssh_ready_source": "provisioner_probe",
        "ssh_verified_at": datetime.now(timezone.utc).isoformat(),
    }
    assert await db.merge_thread_vm_context_if_provision_generation(
        str(thread_id), str(generation), pending, require_status_not_ready=True,
    )
    return {
        "inventory": inventory, "observed": observed,
        "demand": demand, "thread_id": thread_id, "runtime": runtime,
        "generation": generation, "request_id": request_id,
        "admitted": admitted, "vm_uid": vm_uid,
        "vmi_uid": vmi_uid, "launcher_uid": launcher_uid,
        "registration": registration, "updates": {**pending, "status": "ready"},
    }


async def _simulate_stale_thread_identity(db, thread_id, field, value):
    """Model a replaced physical identity despite the native in-life guard."""
    assert field in {"runtime_generation", "agent_id", "runtime_attach_token"}
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL session_replication_role = 'replica'")
        await conn.execute(
            f"UPDATE threads SET {field}=$2 WHERE id=$1", thread_id, value,
        )


@pytest.mark.asyncio
async def test_typed_thread_adoption_and_probed_ready_bind_one_shared_charge(
    db, monkeypatch,
):
    prepared = await _ready_charged_thread(db, monkeypatch)
    thread_id = prepared["thread_id"]
    generation = prepared["generation"]
    request_id = prepared["request_id"]
    admitted = prepared["admitted"]
    vm_uid = prepared["vm_uid"]
    vmi_uid = prepared["vmi_uid"]
    launcher_uid = prepared["launcher_uid"]
    registration = prepared["registration"]
    updates = prepared["updates"]
    phases = VMProvisioningPhaseStore(db)
    # The thread/agent/attach tuple and exact installed launcher must still
    # match when the final probe attempts its Ready transaction.
    for field, current in (
        ("runtime_generation", prepared["runtime"]),
        ("agent_id", None),
        ("runtime_attach_token", None),
    ):
        await _simulate_stale_thread_identity(db, thread_id, field, uuid4())
        assert await phases.publish_thread_ready(
            str(thread_id), str(generation), registration, vm_uid, updates,
        ) is False
        assert await db.fetchval(
            "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1", request_id,
        ) is None
        await _simulate_stale_thread_identity(db, thread_id, field, current)
    wrong = successor(prepared["observed"])
    wrong["pods"][0]["reservation_id"] = str(uuid4())
    await publish(prepared["inventory"], wrong)
    assert await phases.publish_thread_ready(
        str(thread_id), str(generation), registration, vm_uid, updates,
    ) is False
    assert await db.fetchval(
        "SELECT vm_uid FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) is None
    corrected = successor(wrong)
    corrected["pods"][0]["reservation_id"] = admitted["reservation_id"]
    await publish(prepared["inventory"], corrected)
    assert await phases.publish_thread_ready(
        str(thread_id), str(generation), registration, vm_uid, updates,
    ) is True
    reservation = await db.fetchrow(
        "SELECT state,vm_uid,vmi_uid,launcher_uid,observed_cpu_millicores "
        "FROM vm_resource_reservations WHERE id=$1", admitted["reservation_id"],
    )
    assert (
        reservation["state"], str(reservation["vm_uid"]),
        str(reservation["vmi_uid"]), str(reservation["launcher_uid"]),
        reservation["observed_cpu_millicores"],
    ) == ("active", vm_uid, vmi_uid, launcher_uid,
          prepared["demand"].cpu_millicores)
    thread = await db.fetchrow("SELECT metadata FROM threads WHERE id=$1", thread_id)
    assert json.loads(thread["metadata"])["vm"]["status"] == "ready"
    assert await db.fetchval(
        "SELECT ready_at IS NOT NULL FROM vm_creation_retries WHERE request_id=$1",
        request_id,
    ) is True


@pytest.mark.asyncio
async def test_end_winning_before_thread_ready_leaves_exact_charge_held(
    db, monkeypatch,
):
    prepared = await _ready_charged_thread(db, monkeypatch)
    thread_id = prepared["thread_id"]
    request_id = prepared["request_id"]
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(prepared["runtime"]),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id), token=retirement["token"],
        generation=retirement["generation"], settle_status="ended",
    )
    assert await VMProvisioningPhaseStore(db).publish_thread_ready(
        str(thread_id), str(prepared["generation"]),
        prepared["registration"], prepared["vm_uid"], prepared["updates"],
    ) is False
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
        request_id,
    ) == "reserved"
    assert await db.fetchval(
        "SELECT ready_at FROM vm_creation_retries WHERE request_id=$1", request_id,
    ) is None


@pytest.mark.asyncio
async def test_thread_waiter_uses_same_resource_ledger_with_real_owner(db):
    owner, thread_id = await _thread(db, lane="pinned", status="created")
    generation, request_id = uuid4(), uuid4()
    runtime_generation = await db.fetchval(
        "SELECT runtime_generation FROM threads WHERE id=$1", thread_id,
    )
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) WHERE id=$1",
        thread_id, json.dumps({"status": "provisioning", "provision_generation": str(generation),
                               "creation_request_id": str(request_id)}),
    )
    request_digest = "sha256:" + "a" * 64
    policy_digest = "sha256:" + "b" * 64
    await db.execute(
        "INSERT INTO vm_creation_retries "
        "(request_id,owner_kind,thread_id,thread_runtime_generation,"
        "provision_generation,origin,request_digest,canonical_request,"
        "controller_configuration_digest,controller_configuration,thread_owner_user_id,reason) "
        "VALUES($1,'thread',$2,$3,$4,'initial',$5,$6::jsonb,$7,$8::jsonb,$9,'resource_wait')",
        request_id, thread_id, runtime_generation, generation,
        request_digest, json.dumps({
            "job_id": str(thread_id), "entity_type": "thread",
            "provision_generation": str(generation),
        }), policy_digest, json.dumps({"version": 3}), owner,
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
    from orchestrator.services.vm_creation_owner_view import thread_creation_views

    current = await thread_creation_views(
        db, [str(thread_id)], viewer_user_id=str(owner),
    )
    assert current[str(thread_id)]["wait"]["kind"] == "resource"
    assert current[str(thread_id)]["wait"]["guest_vcpus"] == 1
    assert await thread_creation_views(
        db, [str(thread_id)], viewer_user_id=str(uuid4()),
    ) == {}
    # The owner-gated detail route must carry this projection, not just the
    # helper used to compute it. The gate is supplied explicitly here; the
    # helper independently rechecks the current thread owner in PostgreSQL.
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.routers.thread_session import get_thread

    response = await get_thread(
        str(thread_id), object(),
        dependencies=SimpleNamespace(
            store=db,
            require_thread_owner=AsyncMock(return_value=(
                {"id": str(owner), "is_admin": False},
                await db.get_thread(str(thread_id)),
            )),
            resolve_cloud_session_url=lambda *_: None,
        ),
    )
    assert response["vm_creation"]["wait"]["kind"] == "resource"
    assert "resource_wait" not in response


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
    await db.execute(
        "UPDATE vm_creation_retries SET claim_expires_at=clock_timestamp()+interval '30 seconds' WHERE request_id=$1",
        request_id,
    )
    retries = VMCreationRetryStore(db)
    assert await retries.apply_observation(
        request_id=str(request_id), claim_token=str(claims[0]["claim_token"]),
        expected_revision=claims[0]["revision"],
        observation={"outcome": "transport_unknown"},
    )
    assert await retries.claim_due(limit=1) == []
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
