"""Typed partial creation End keeps a genuine thread charge until exact cleanup."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from copy import deepcopy
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from kubernetes.client.exceptions import ApiException

from orchestrator.services.vm_creation_disposition_store import VMCreationDispositionStore
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)
from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore
from orchestrator.services.vm_creation_request import build_vm_creation_request
from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
from orchestrator.services.vm_provisioner import VMProvisioner
from orchestrator.services.agent_thread_status import update_thread_status
from shared.vm_creation_issuance import canonical_configuration_digest, seal_creation_carrier
from shared.vm_creation_retry import canonical_request_digest
from shared.vm_launcher_profile import predict_launcher
from tests.test_vm_idle_pinned_session_real_postgres import ready_pinned_thread
from tests.test_vm_resource_configuration import whole_launcher_configuration
from tests.test_vm_resource_whole_store_real_postgres import environment
from tests.test_vm_resource_inventory_real_postgres import publish, successor
from shared.vm_creation_disposition import disposition_identity
from tests.test_vm_creation_actuation import setup as _setup_fixture
from tests.test_vm_resource_thread_source_real_postgres import (
    _adopted_charged_thread,
    _ready_charged_thread,
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)
from vm_controller.creation_disposition import CreationDisposer

setup = _setup_fixture


async def retained_wake_partial(
    db, monkeypatch, *, secret=False, effect=True, authorize_end=True,
):
    """Build a real suspended pinned wake, typed grant, and observed retained DV."""
    from tests.test_vm_creation_actuation import SECRET

    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())
    thread_id, body, old = await ready_pinned_thread(db, monkeypatch)
    await update_thread_status(
        str(thread_id), body,
        dependencies=SimpleNamespace(db=db, thread_accepts_runtime=lambda _: True),
    )
    episode = json.loads(await db.fetchval(
        "SELECT workspace_idle_episode FROM threads WHERE id=$1", thread_id,
    ))
    episode["episode_id"] = str(uuid4())
    episode["entered_at"] = (datetime.now(timezone.utc) - timedelta(minutes=16)).isoformat()
    await db.execute(
        "UPDATE threads SET workspace_idle_revision=2,workspace_idle_episode=$2::jsonb WHERE id=$1",
        thread_id, json.dumps(episode),
    )
    idle = VMIdleLifecycleStore(db)
    operation = await idle.admit_thread_release(
        str(thread_id), episode_id=episode["episode_id"], revision=2,
        identity=old, turn_quiescent=True,
    )
    assert operation is not None
    context = json.loads(await db.fetchval(
        "SELECT runtime_retirement_context FROM threads WHERE id=$1", thread_id,
    ))
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(thread_id),
        expected_runtime_generation=str(operation["thread_runtime_generation"]),
        expected_retirement_token=str(operation["thread_retirement_token"]),
        expected_agent_id=context["agent_id"],
        expected_attach_token=context["runtime_attach_token"],
        expected_settle_status="suspended",
        expected_quiescence_protocol="workspace_actuator_zero_v1",
        expected_workspace_generation=old["generation"],
        expected_workspace_runtime_incarnation=old["vm_uid"],
    ) is not None
    assert await db.settle_pinned_thread_retirement(
        str(thread_id), token=str(operation["thread_retirement_token"]),
        generation=str(operation["thread_runtime_generation"]),
        final_status="suspended",
    )
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',metadata->'vm' || $2::jsonb) WHERE id=$1",
        thread_id, json.dumps({
            "status": "suspending", "_suspend_remote_io_closed": str(operation["id"]),
        }),
    )
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('thread',$1,'vm','vm',$2)", thread_id, old["generation"],
    )
    pod = json.loads(operation["thread_agent_pod_identity"])
    assert await idle.record_thread_agent_stop(
        str(operation["id"]), evidence={
            "version": 1, "pod": pod, "disposition": "exact_absent",
            "retirement_token": str(operation["thread_retirement_token"]),
            "controller_authenticated": True,
        },
    )
    assert await idle.complete_release(str(operation["id"]), evidence={
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]), "generation": old["generation"],
        "vm_uid": old["vm_uid"], "vmi_uid": old["vmi_uid"],
        "launcher_uid": old["launcher_uid"], "pvc_uid": old["pvc_uid"],
        "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "retained_pvc": True, "controller_authenticated": True,
        "same_generation_replacement": False,
    })
    waking = await idle.request_thread_wake(str(thread_id), execution_requested=True)
    assert waking is not None
    owner = await db.get_thread(str(thread_id))
    predecessor = json.loads(owner["metadata"])["vm"]
    runtime = owner["runtime_generation"]
    generation, request_id = waking["wake_generation"], waking["wake_request_id"]
    proposed = VMProvisioner._fresh_provision_ctx()
    proposed.update(
        status="provisioning", provision_generation=str(generation),
        idle_wake_operation_id=str(waking["id"]),
        idle_wake_request_id=str(request_id),
        idle_predecessor_pvc_uid=old["pvc_uid"],
    )
    policy, inventory, sample, _ = await environment(db)
    sample = successor(sample)
    pv_uid = str(uuid4())
    sample["pvcs"] = [{
        "uid": old["pvc_uid"], "name": f"agent-vm-{thread_id}-rootdisk",
        "pv_uid": pv_uid, "pv_name": "thread-retained",
        "storage_class_uid": sample["storage_classes"][0]["uid"],
        "phase": "Bound",
    }]
    sample["pvs"] = [{
        "uid": pv_uid, "name": "thread-retained",
        "claim_uid": old["pvc_uid"],
        "required_affinity": {"nodeSelectorTerms": [{"matchExpressions": [{
            "key": "kubernetes.io/hostname", "operator": "In",
            "values": ["node-a"],
        }]}]},
    }]
    await publish(inventory, sample)
    config = whole_launcher_configuration()
    config.update(namespace="workers", storage_class="local")
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
        vm_image="pinned:image", cpu_cores=8, memory="16Gi", description="thread wake",
        network_tier="restricted", provision_generation=str(generation),
    )
    assert await db.begin_pinned_thread_vm_provisioning(
        str(thread_id), expected_runtime_generation=str(runtime),
        expected_agent_id=None, expected_attach_token=None,
        expected_vm_context=predecessor, provision_context=proposed,
        wake_operation_id=str(waking["id"]), creation_source={
            "request_id": str(request_id), "request": request,
            "request_digest": canonical_request_digest(request),
            "controller_configuration": config,
            "controller_configuration_digest": canonical_configuration_digest(config),
        },
    )
    admitted = await policy.admit(request_id=str(request_id))
    assert admitted["action"] == "admitted", admitted
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    grant = await store.authorize_controller(
        request_id=str(request_id), claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(thread_id), "provision_generation": str(generation),
            "request_digest": canonical_request_digest(request),
            "controller_configuration_digest": canonical_configuration_digest(config),
            "expected_pvc_uid": old["pvc_uid"],
        },
    )
    assert grant["allowed"] is True
    dv_uid = str(uuid4())
    values = {
        "version": 4, "resource_grant": grant["resource_grant"],
        "rootdisk_source": {"kind": "retained", "pvc_uid": old["pvc_uid"]},
        "source": "controller_vm_create", "admission_id": str(grant["admission_id"]),
        "reservation_request_id": grant["request_id"],
        "intent_digest": grant["intent_digest"], "retry_request_id": str(request_id),
        "job_id": str(thread_id), "owner_kind": "thread",
        "thread_runtime_generation": str(runtime), "thread_agent_id": None,
        "thread_attach_token": None, "thread_wake_operation_id": str(waking["id"]),
        "provision_generation": str(generation),
        "request_digest": canonical_request_digest(request),
        "controller_configuration_digest": canonical_configuration_digest(config),
        "expected_pvc_uid": old["pvc_uid"], "retained_dv_uid": dv_uid,
        "current_dv_uid": dv_uid, "current_pvc_uid": old["pvc_uid"],
        "current_secret_uid": None, "effect_kind": "rootdisk",
        "effect_nonce": str(uuid4()),
        "object_name": f"agent-vm-{thread_id}-rootdisk",
    }
    carrier = seal_creation_carrier(
        values, namespace="workers", uid=str(uuid4()), resource_version="3",
        secret=SECRET,
    )
    if effect:
        assert (await store.begin_effect(
            request_id=str(request_id), claim_token=str(claim["claim_token"]),
            carrier=carrier,
        ))["actuation_allowed"] is True
    else:
        carrier = None
    labels = {"srw.io/owner-kind": "thread", "srw.io/owner-id": str(thread_id)}
    dv = {
        "apiVersion": "cdi.kubevirt.io/v1beta1", "kind": "DataVolume",
        "metadata": {"uid": dv_uid, "name": values["object_name"],
                     "namespace": "workers", "labels": labels},
        "status": {"phase": "Succeeded"},
    }
    pvc = {
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"uid": old["pvc_uid"], "name": values["object_name"],
                     "namespace": "workers", "labels": labels,
                     "ownerReferences": [{"kind": "DataVolume", "uid": dv_uid,
                                           "controller": True}]},
        "spec": {"volumeMode": "Filesystem"}, "status": {"phase": "Bound"},
    }
    if effect:
        assert (await store.observe_effect(
            request_id=str(request_id), carrier=carrier,
            observation={"outcome": "observed", "object": dv, "pvc": pvc},
        ))["recorded"] is True
    secret_obj = None
    if secret and effect:
        from shared.vm_creation_issuance import EFFECT_NONCE_ANNOTATION, REQUEST_ANNOTATION

        values = {
            **values, "effect_kind": "cloud_init", "effect_nonce": str(uuid4()),
            "object_name": f"agent-vm-{thread_id}-cloudinit",
        }
        carrier = seal_creation_carrier(
            values, namespace="workers", uid=carrier["metadata"]["uid"],
            resource_version="4", secret=SECRET,
        )
        assert (await store.begin_effect(
            request_id=str(request_id), claim_token=str(claim["claim_token"]),
            carrier=carrier,
        ))["actuation_allowed"] is True
        secret_obj = {
            "apiVersion": "v1", "kind": "Secret", "metadata": {
                "uid": str(uuid4()), "name": values["object_name"],
                "namespace": "workers", "labels": labels, "annotations": {
                    EFFECT_NONCE_ANNOTATION: values["effect_nonce"],
                    REQUEST_ANNOTATION: str(request_id),
                    "srw.io/provision-generation": str(generation),
                    "srw.io/ssh-host-key-fingerprint": "SHA256:" + "A" * 43,
                },
            },
        }
        assert (await store.observe_effect(
            request_id=str(request_id), carrier=carrier,
            observation={"outcome": "observed", "object": secret_obj},
        ))["recorded"] is True
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(runtime),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert retirement["state"] == "pending", retirement
    if authorize_end:
        assert await db.authorize_pinned_thread_retirement(
            str(thread_id), token=retirement["token"],
            generation=retirement["generation"], settle_status="ended",
        )
    return store, thread_id, request_id, admitted, dv, pvc, carrier, waking, secret_obj


@pytest.mark.asyncio
async def test_retained_thread_wake_rootdisk_end_holds_then_freezes(db, monkeypatch):
    store, _, request_id, admitted, _, _, carrier, _, _ = await retained_wake_partial(
        db, monkeypatch,
    )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"
    frozen = await store.freeze_disposition(request_id=str(request_id), carrier=carrier)
    assert frozen["frozen"] is True
    assert frozen["disposition"]["disk_policy"] == "retain"


@pytest.mark.asyncio
async def test_retained_wake_without_original_carrier_settles_never_issued(
    db, monkeypatch,
):
    store, thread_id, request_id, admitted, _, _, carrier, waking, _ = await retained_wake_partial(
        db, monkeypatch, effect=False,
    )
    assert carrier is None
    assert await db.fetchval(
        "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1", request_id,
    ) == 0
    with pytest.raises(VMCreationRetryConflict, match="creation_carrier_required"):
        await store.prepare_disposition(request_id=str(request_id))
    never_issued = await store.settle_never_issued(request_id=str(request_id))
    assert never_issued == {"settled": True, "disposition": "never_issued"}
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "released"
    current = await store.inspect(request_id=str(request_id))
    assert current["creation_carrier_uid"] is None
    assert await db.fetchval(
        "SELECT pvc_uid FROM vm_idle_operations WHERE id=$1", waking["id"],
    ) == waking["pvc_uid"]
    assert await db.fetchval(
        "SELECT public.thread_vm_creation_never_issued_source($1,$2)",
        thread_id, current["provision_generation"],
    )


async def retained_controller_runtime(db, setup, monkeypatch, *, secret=False):
    """Wire the real typed wake to the fault-injected K8s controller API."""
    from vm_controller import controller as settings

    monkeypatch.setattr(settings, "VM_NAMESPACE", "workers")
    ctrl, api, _, _ = setup
    store, thread_id, request_id, admitted, dv, pvc, carrier, waking, secret_obj = (
        await retained_wake_partial(db, monkeypatch, secret=secret)
    )
    observations = {"rootdisk": {"object": dv, "pvc": pvc}}
    if secret:
        observations["cloud_init"] = {"object": secret_obj}
    for value in (
        carrier, dv, pvc,
        *([observations["cloud_init"]["object"]] if secret else []),
    ):
        api.objects[value["kind"], value["metadata"]["name"]] = deepcopy(value)
    api.deletes = []
    api.lost_deletes = set()

    def listed(kind):
        return {
            "metadata": {"resourceVersion": "1"},
            "items": [deepcopy(value) for (resource, _), value in api.objects.items()
                      if resource == kind],
        }

    def delete(kind, name, body):
        current = api.read(kind, name)
        uid = body["preconditions"]["uid"]
        if uid != current["metadata"]["uid"]:
            raise ApiException(status=409)
        api.deletes.append((kind, name, uid))
        del api.objects[kind, name]
        if kind in api.lost_deletes:
            api.lost_deletes.remove(kind)
            raise TimeoutError("lost delete reply")

    ctrl.k8s_client.list_namespaced_custom_object = lambda **kw: listed({
        "virtualmachines": "VirtualMachine",
        "virtualmachineinstances": "VirtualMachineInstance",
    }[kw["plural"]])
    ctrl.core_api.list_namespaced_pod = lambda **kw: listed("Pod")
    ctrl.k8s_client.delete_namespaced_custom_object = lambda **kw: delete(
        "DataVolume", kw["name"], kw["body"],
    )
    ctrl.core_api.delete_namespaced_persistent_volume_claim = lambda **kw: delete(
        "PersistentVolumeClaim", kw["name"], kw["body"],
    )
    ctrl.core_api.delete_namespaced_secret = lambda **kw: delete(
        "Secret", kw["name"], kw["body"],
    )
    ctrl.coordination_api.delete_namespaced_lease = lambda **kw: delete(
        "Lease", kw["name"], kw["body"],
    )

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        assert operation == "creation_retry_" + method
        return await getattr(store, method)(**body)

    ctrl._workspace_cleanup_authority_request = authority
    row = await store.inspect(request_id=str(request_id))
    return ctrl, api, store, row, admitted, observations, carrier, waking


@pytest.mark.asyncio
@pytest.mark.parametrize("secret", [False, True])
async def test_retained_thread_controller_keeps_pvc_and_releases_only_after_end(
    db, setup, monkeypatch, secret,
):
    ctrl, api, store, row, admitted, observations, carrier, waking = (
        await retained_controller_runtime(db, setup, monkeypatch, secret=secret)
    )
    assert (await store.freeze_disposition(
        request_id=row["request_id"], carrier=carrier,
    ))["frozen"] is True
    with pytest.raises(VMCreationRetryConflict, match="creation_disposition_incomplete"):
        await store.settle_disposition(request_id=row["request_id"], carrier=carrier)
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"
    if secret:
        api.lost_deletes.add("Secret")
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_disposed", result
    assert api.read("PersistentVolumeClaim", observations["rootdisk"]["pvc"]["metadata"]["name"])
    assert api.read("DataVolume", observations["rootdisk"]["object"]["metadata"]["name"])
    assert not any(kind in {"PersistentVolumeClaim", "DataVolume"} for kind, _, _ in api.deletes)
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "released"
    completed = await store.inspect(request_id=row["request_id"])
    assert completed["state"] == "settled"
    assert completed["cancellation_completion"]["rootdisk"]["kind"] == "rootdisk_retained"
    assert completed["cancellation_completion"]["source"]["outcome"] == "not_required"
    assert completed["cancellation_completion"]["workspace_attachment"]["outcome"] == "not_applicable"
    assert (await store.settle_disposition(
        request_id=row["request_id"], carrier=carrier,
    ))["settled"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["pvc_uid", "dv_uid", "pvc_owner", "carrier_uid"])
async def test_retained_thread_replaced_identity_preserves_old_charge(
    db, setup, monkeypatch, changed,
):
    ctrl, api, store, row, admitted, observations, carrier, _ = (
        await retained_controller_runtime(db, setup, monkeypatch)
    )
    assert (await store.freeze_disposition(
        request_id=row["request_id"], carrier=carrier,
    ))["frozen"] is True
    root_name = observations["rootdisk"]["object"]["metadata"]["name"]
    if changed == "carrier_uid":
        name = carrier["metadata"]["name"]
        api.objects["Lease", name]["metadata"]["uid"] = str(uuid4())
    elif changed == "dv_uid":
        api.objects["DataVolume", root_name]["metadata"]["uid"] = str(uuid4())
    elif changed == "pvc_uid":
        api.objects["PersistentVolumeClaim", root_name]["metadata"]["uid"] = str(uuid4())
    else:
        api.objects["PersistentVolumeClaim", root_name]["metadata"]["labels"][
            "srw.io/owner-id"
        ] = str(uuid4())
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_attention", result
    assert not api.deletes
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"


@pytest.mark.asyncio
async def test_retained_thread_stale_end_token_refuses_disposition(db, monkeypatch):
    store, thread_id, request_id, admitted, _, _, carrier, _, _ = (
        await retained_wake_partial(db, monkeypatch, authorize_end=False)
    )
    owner = await db.fetchrow(
        "SELECT runtime_generation,runtime_retirement_token FROM threads WHERE id=$1",
        thread_id,
    )
    assert not await db.authorize_pinned_thread_retirement(
        str(thread_id), token=str(uuid4()),
        generation=str(owner["runtime_generation"]), settle_status="ended",
    )
    with pytest.raises(VMCreationRetryConflict):
        await store.freeze_disposition(request_id=str(request_id), carrier=carrier)
    assert await db.fetchval(
        "SELECT state FROM vm_creation_retries WHERE request_id=$1", request_id,
    ) == "reconciling"
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"


@pytest_asyncio.fixture(scope="module")
async def disposition_schema(pg_dsn, thread_schema):  # noqa: F811
    conn = await asyncpg.connect(pg_dsn)
    try:
        if not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname="
            "'valid_vm_creation_thread_disposition_identity')"
        ):
            migration = (
                Path(__file__).resolve().parents[1]
                / "src/orchestrator/database/migrations/app/0279_vm_thread_creation_disposition.sql"
            )
            await conn.execute(migration.read_text())
        migration = (
            Path(__file__).resolve().parents[1]
            / "src/orchestrator/database/migrations/app/0280_vm_thread_cancel_carrier.sql"
        )
        if not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns "
            "WHERE table_name='vm_creation_retries' "
            "AND column_name='disposition_carrier_uid')"
        ):
            await conn.execute(migration.read_text())
        migration = (
            Path(__file__).resolve().parents[1]
            / "src/orchestrator/database/migrations/app/0281_vm_thread_retained_creation_disposition.sql"
        )
        await conn.execute(migration.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(disposition_schema, _base_db):  # noqa: F811
    yield _base_db


async def partial_thread(db, monkeypatch, last_effect):
    (
        _, _, _, _, thread_id, runtime, generation, request_id,
        admitted, observations, carrier,
    ) = await _adopted_charged_thread(
        db, monkeypatch, stop_after=last_effect, adopt=False,
    )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(runtime),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert retirement["state"] == "pending", retirement
    assert retirement["context"]["vm_creation_source"]["request_id"] == str(request_id)
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id), token=retirement["token"],
        generation=retirement["generation"], settle_status="ended",
    )
    assert await db.fetchval(
        "SELECT state FROM vm_creation_retries WHERE request_id=$1", request_id,
    ) == "cancel_requested"
    service = VMCreationDispositionStore(VMCreationRetryStore(db))
    return service, thread_id, runtime, generation, request_id, admitted, observations, carrier


async def controller_runtime(db, setup, monkeypatch, last_effect):
    ctrl, api, _, _ = setup
    service, thread_id, runtime, generation, request_id, admitted, observations, carrier = (
        await partial_thread(db, monkeypatch, last_effect)
    )
    store = service.retries
    observations["rootdisk"]["pvc"]["metadata"]["labels"] = deepcopy(
        observations["rootdisk"]["object"]["metadata"]["labels"]
    )
    observations["rootdisk"]["pvc"]["metadata"]["ownerReferences"][0]["controller"] = True
    for value in (carrier, observations["rootdisk"]["object"],
                  observations["rootdisk"]["pvc"],
                  *([observations["cloud_init"]["object"]] if last_effect == "cloud_init" else [])):
        api.objects[value["kind"], value["metadata"]["name"]] = deepcopy(value)
    api.deletes = []
    api.lost_deletes = set()

    def listed(kind):
        return {
            "metadata": {"resourceVersion": "1"},
            "items": [deepcopy(value) for (resource, _), value in api.objects.items()
                      if resource == kind],
        }

    def delete(kind, name, body):
        current = api.read(kind, name)
        uid = body["preconditions"]["uid"]
        if uid != current["metadata"]["uid"]:
            raise ApiException(status=409)
        api.deletes.append((kind, name, uid))
        del api.objects[kind, name]
        if kind in api.lost_deletes:
            api.lost_deletes.remove(kind)
            raise TimeoutError("lost delete reply")

    ctrl.k8s_client.list_namespaced_custom_object = lambda **kw: listed({
        "virtualmachines": "VirtualMachine",
        "virtualmachineinstances": "VirtualMachineInstance",
    }[kw["plural"]])
    ctrl.core_api.list_namespaced_pod = lambda **kw: listed("Pod")
    ctrl.k8s_client.delete_namespaced_custom_object = lambda **kw: delete(
        "DataVolume", kw["name"], kw["body"],
    )
    ctrl.core_api.delete_namespaced_persistent_volume_claim = lambda **kw: delete(
        "PersistentVolumeClaim", kw["name"], kw["body"],
    )
    ctrl.core_api.delete_namespaced_secret = lambda **kw: delete(
        "Secret", kw["name"], kw["body"],
    )
    ctrl.coordination_api.delete_namespaced_lease = lambda **kw: delete(
        "Lease", kw["name"], kw["body"],
    )

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        if "vm-creation-retries" in path:
            assert operation == "creation_retry_" + method
            assert method not in {"authorize", "begin_effect", "settle_adopted"}
            return await getattr(store, method)(**body)
        assert operation == "recovery-cleanup-" + method
        if method == "complete":
            completed = await store.cleanup.complete_cleanup_permit(
                UUID(body["admission_id"]), request_id=UUID(body["request_id"]),
                intent_digest=body["intent_digest"], outcome=body["outcome"],
            )
            return {"completed": completed}
        if method == "resume":
            permit = await store.cleanup.resume_cleanup_permit(
                UUID(body["admission_id"]), owner_kind=body["owner_kind"],
                owner_id=UUID(body["owner_id"]), source=body["source"],
                request_id=UUID(body["request_id"]),
                intent_digest=body["intent_digest"],
            )
            return {
                "allowed": permit.allowed, "reason": permit.reason,
                "completed_outcome": permit.completed_outcome,
                "creation_disposition": getattr(permit, "creation_disposition", None),
            }
        raise AssertionError(method)

    ctrl._workspace_cleanup_authority_request = authority
    row = await store.inspect(request_id=str(request_id))
    return ctrl, api, store, row, thread_id, admitted, observations, carrier


async def missing_carrier_runtime(db, setup, monkeypatch, *, pin=True):
    """Real typed grant and source DV/PVC, without any creation Lease/effect."""
    from vm_controller import controller as settings
    from vm_controller.creation_sources import GoldenSources

    (
        _, _, _, _, thread_id, runtime, _, request_id,
        admitted, _, carrier,
    ) = await _adopted_charged_thread(
        db, monkeypatch, stop_before_effect=True, adopt=False,
        golden_enabled=True,
    )
    assert carrier is None
    ctrl, api, _, _ = setup
    monkeypatch.setattr(settings, "VM_NAMESPACE", "workers")
    monkeypatch.setattr(settings, "VM_STORAGE_CLASS", "local")
    monkeypatch.setattr(settings, "VM_GOLDEN_DISK_SIZE", "10Gi")
    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", True)
    store = VMCreationRetryStore(db)

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        assert operation == "creation_retry_" + method
        return await getattr(store, method)(**body)

    ctrl._workspace_cleanup_authority_request = authority
    row = await store.inspect(request_id=str(request_id))
    name = settings._golden_name(row["request"]["vm_image"])
    dv = ctrl._golden_dv_manifest(name, row["request"]["vm_image"])
    dv["metadata"].update(uid=str(UUID(int=2)), resourceVersion="1")
    dv["status"] = {"phase": "Succeeded"}
    api.objects["DataVolume", name] = dv
    api.objects["PersistentVolumeClaim", name] = {
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": name, "namespace": "workers", "uid": str(UUID(int=3)),
            "resourceVersion": "1", "ownerReferences": [{
                "kind": "DataVolume", "uid": dv["metadata"]["uid"],
                "controller": True,
            }],
        },
        "spec": {"volumeMode": "Filesystem"}, "status": {"phase": "Bound"},
    }
    api.replacements = []

    def replace_source(**kwargs):
        old = api.read("DataVolume", kwargs["name"])
        body = deepcopy(kwargs["body"])
        if any(
            body["metadata"][key] != old["metadata"][key]
            for key in ("uid", "resourceVersion")
        ):
            raise ApiException(status=409)
        body["metadata"]["resourceVersion"] = str(
            int(old["metadata"]["resourceVersion"]) + 1
        )
        api.objects["DataVolume", kwargs["name"]] = body
        api.replacements.append(deepcopy(body))
        if "DataVolume" in api.lost:
            api.lost.remove("DataVolume")
            raise TimeoutError("accepted source CAS reply lost")
        return body

    ctrl.k8s_client.replace_namespaced_custom_object = replace_source
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"}, "items": [],
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"}, "items": [],
    }
    if pin:
        source, observed = await GoldenSources(ctrl).facts(row, name)
        assert await GoldenSources(ctrl).hold(row, source, observed) == source
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(runtime),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id), token=retirement["token"],
        generation=retirement["generation"], settle_status="ended",
    )
    return ctrl, api, store, await store.inspect(request_id=str(request_id)), admitted, name


@pytest.mark.asyncio
@pytest.mark.parametrize("last_effect", ["rootdisk", "cloud_init"])
async def test_partial_thread_end_freezes_exact_observed_disposition_and_holds_charge(
    db, monkeypatch, last_effect,
):
    service, thread_id, runtime, generation, request_id, admitted, observations, carrier = (
        await partial_thread(db, monkeypatch, last_effect)
    )
    assert set(observations) == (
        {"rootdisk"} if last_effect == "rootdisk" else {"rootdisk", "cloud_init"}
    )
    frozen = await service.freeze(request_id=str(request_id), carrier=carrier)
    assert frozen["frozen"] is True
    disposition = frozen["disposition"]
    assert disposition["owner_kind"] == "thread"
    assert disposition["thread_id"] == str(thread_id)
    assert disposition["thread_runtime_generation"] == str(runtime)
    assert disposition["provision_generation"] == str(generation)
    assert disposition["objects"]["rootdisk"]["pvc_uid"] == observations[
        "rootdisk"
    ]["pvc"]["metadata"]["uid"]
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"


@pytest.mark.asyncio
async def test_observed_thread_disk_grants_only_exact_child_and_holds_parent(db, monkeypatch):
    service, thread_id, _, generation, request_id, admitted, observations, carrier = (
        await partial_thread(db, monkeypatch, "rootdisk")
    )
    frozen = await service.freeze(request_id=str(request_id), carrier=carrier)
    assert frozen["frozen"] is True
    with pytest.raises(VMCreationRetryConflict, match="incomplete"):
        await service.settle(request_id=str(request_id), carrier=carrier)
    grant = await service.authorize(
        request_id=str(request_id), carrier=carrier, stage="rootdisk",
    )
    assert grant["operation"] == "purge_rootdisk"
    assert grant["cleanup"]["owner_kind"] == "thread"
    assert grant["cleanup"]["owner_id"] == str(thread_id)
    assert grant["cleanup"]["provision_generation"] == str(generation)
    assert grant["cleanup"]["pvc_uid"] == observations["rootdisk"]["pvc"]["metadata"]["uid"]
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_resource_reservations SET state='released',"
                "released_at=clock_timestamp(),release_evidence=$2::jsonb "
                "WHERE id=$1::uuid",
                UUID(admitted["reservation_id"]),
                '{"kind":"never_vm_issued"}',
            )


@pytest.mark.asyncio
async def test_hidden_thread_end_preflight_cannot_freeze_or_release(db, monkeypatch):
    (
        _, _, _, _, thread_id, runtime, _, request_id,
        admitted, _, carrier,
    ) = await _adopted_charged_thread(
        db, monkeypatch, stop_after="rootdisk", adopt=False,
    )
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(runtime),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert retirement["state"] == "pending"
    assert retirement["authorized_at"] is None
    assert await db.fetchval(
        "SELECT state FROM vm_creation_retries WHERE request_id=$1", request_id,
    ) == "reconciling"
    with pytest.raises(VMCreationRetryConflict, match="thread_retirement_source_changed"):
        await VMCreationDispositionStore(VMCreationRetryStore(db)).freeze(
            request_id=str(request_id), carrier=carrier,
        )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"


@pytest.mark.asyncio
async def test_captured_thread_disk_uid_must_match_observed_creation_source(
    db, monkeypatch,
):
    (
        _, _, _, _, thread_id, runtime, _, request_id,
        admitted, observations, carrier,
    ) = await _adopted_charged_thread(
        db, monkeypatch, stop_after="rootdisk", adopt=False,
    )
    wrong_uid = str(UUID(int=2))
    assert wrong_uid != observations["rootdisk"]["pvc"]["metadata"]["uid"]
    await db.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata,'{vm,rootdisk_pvc_uid}',"
        "to_jsonb($2::text)) WHERE id=$1", thread_id, wrong_uid,
    )
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(runtime),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id), token=retirement["token"],
        generation=retirement["generation"], settle_status="ended",
    )
    with pytest.raises(VMCreationRetryConflict, match="thread_retirement_source_changed"):
        await VMCreationDispositionStore(VMCreationRetryStore(db)).freeze(
            request_id=str(request_id), carrier=carrier,
        )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"


@pytest.mark.asyncio
async def test_missing_typed_carrier_with_unresolved_golden_source_stays_charged(
    db, monkeypatch,
):
    (
        _, _, _, _, thread_id, runtime, _, request_id,
        admitted, observations, carrier,
    ) = await _adopted_charged_thread(
        db, monkeypatch, stop_before_effect=True, adopt=False,
        golden_enabled=True,
    )
    assert observations == {} and carrier is None
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(runtime),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id), token=retirement["token"],
        generation=retirement["generation"], settle_status="ended",
    )
    assert await VMCreationRetryStore(db).settle_never_issued(
        request_id=str(request_id),
    ) == {"settled": False, "reason": "creation_source_unresolved"}
    prepared = await VMCreationDispositionStore(VMCreationRetryStore(db)).prepare(
        request_id=str(request_id),
    )
    assert prepared["actuation_allowed"] is False
    assert prepared["carrier_intent"]["kind"] == "thread_creation_cancel"
    assert prepared["carrier_intent"]["thread_runtime_generation"] == str(runtime)
    assert prepared["carrier_intent"]["reservation_id"] == str(admitted["reservation_id"])
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_stage", ["create", "seal"])
async def test_controller_publishes_separate_thread_cancel_lease_after_lost_reply(
    db, setup, monkeypatch, lost_stage,
):
    from shared.vm_creation_cancel_carrier import (
        carrier_name, verify_cancel_carrier,
    )
    from tests.test_vm_creation_actuation import SECRET

    (
        _, _, _, _, thread_id, runtime, _, request_id,
        admitted, _, carrier,
    ) = await _adopted_charged_thread(
        db, monkeypatch, stop_before_effect=True, adopt=False,
        golden_enabled=True,
    )
    assert carrier is None
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(runtime),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id), token=retirement["token"],
        generation=retirement["generation"], settle_status="ended",
    )
    ctrl, api, _, _ = setup
    from vm_controller import controller as settings

    monkeypatch.setattr(settings, "VM_NAMESPACE", "workers")
    store = VMCreationRetryStore(db)

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        assert operation == "creation_retry_" + method
        return await getattr(store, method)(**body)

    ctrl._workspace_cleanup_authority_request = authority
    row = await store.inspect(request_id=str(request_id))
    if lost_stage == "create":
        api.lost.add("Lease")
    else:
        original_replace = api.replace
        lost = False

        def replace_with_lost_reply(body):
            nonlocal lost
            result = original_replace(body)
            if not lost:
                lost = True
                raise TimeoutError("accepted Lease seal reply lost")
            return result

        api.replace = replace_with_lost_reply
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    frozen = await store.inspect(request_id=str(request_id))
    assert frozen["cancellation_disposition"] is not None, result
    assert frozen["cancellation_disposition"]["carrier_kind"] == "thread_creation_cancel"
    lease = api.read("Lease", carrier_name(frozen["creation_admission_id"]))
    values = verify_cancel_carrier(lease, secret=SECRET)
    assert values["reservation_id"] == str(admitted["reservation_id"])
    assert frozen["disposition_carrier_uid"] == lease["metadata"]["uid"]
    assert frozen["creation_carrier_uid"] is None
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_missing_carrier_source_pin_cas_acknowledges_before_release(
    db, setup, monkeypatch, lost_reply,
):
    from shared.vm_creation_cancel_carrier import carrier_name
    from vm_controller.creation_sources import GoldenSources, pins

    ctrl, api, store, row, admitted, source_name = await missing_carrier_runtime(
        db, setup, monkeypatch,
    )
    request_id = row["request_id"]
    assert pins(api.read("DataVolume", source_name))[request_id]["state"] == "active"
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"
    prepared = await store.prepare_disposition(request_id=request_id)
    lease = await CreationDisposer(ctrl)._publish_thread_cancel(prepared["carrier_intent"])
    assert (await store.freeze_disposition(
        request_id=request_id, carrier=lease,
    ))["frozen"] is True
    source, _ = await GoldenSources(ctrl).facts(row, source_name)
    plan = await store.authorize_disposition(
        request_id=request_id, carrier=lease, stage="source", source=source,
    )
    assert plan["plan"]["tombstone"]["state"] == "disposed"
    with pytest.raises(VMCreationRetryConflict, match="creation_disposition_incomplete"):
        await store.settle_disposition(request_id=request_id, carrier=lease)
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"
    if lost_reply:
        await db.execute(
            "UPDATE vm_resource_admission_policy SET mode='off' "
            "WHERE cluster_id=(SELECT cluster_id FROM vm_resource_reservations "
            "WHERE id=$1)", admitted["reservation_id"],
        )
        api.lost.add("DataVolume")
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_disposed", result
    current = await store.inspect(request_id=request_id)
    assert current["state"] == "settled"
    assert current["reason"] == "creation_disposed"
    assert current["cancellation_completion"]["source"]["outcome"] == "pin_disposed"
    assert pins(api.read("DataVolume", source_name))[request_id]["state"] == "disposed"
    assert len(api.replacements) == 2
    assert current["creation_carrier_uid"] is None
    assert current["disposition_carrier_uid"] == api.read(
        "Lease", carrier_name(current["creation_admission_id"])
    )["metadata"]["uid"]
    assert await store.settle_disposition(
        request_id=request_id, carrier=lease,
    ) == {"settled": True, "disposition": "creation_disposed"}
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "released"


@pytest.mark.asyncio
async def test_foreign_thread_cancel_lease_does_not_freeze_or_release(
    db, setup, monkeypatch,
):
    from shared.vm_creation_cancel_carrier import carrier_name
    from vm_controller.creation_sources import pins

    ctrl, api, store, row, admitted, source_name = await missing_carrier_runtime(
        db, setup, monkeypatch,
    )
    name = carrier_name(row["creation_admission_id"])
    api.objects["Lease", name] = {
        "apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
        "metadata": {
            "name": name, "namespace": "workers", "uid": str(UUID(int=4)),
            "resourceVersion": "1", "labels": {}, "annotations": {},
        },
        "spec": {"holderIdentity": row["creation_admission_id"]},
    }
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_attention"
    assert (await store.inspect(request_id=row["request_id"]))["cancellation_disposition"] is None
    assert pins(api.read("DataVolume", source_name))[row["request_id"]]["state"] == "active"
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"


@pytest.mark.asyncio
async def test_replaced_signed_thread_cancel_lease_uid_remains_held(
    db, setup, monkeypatch,
):
    from shared.vm_creation_cancel_carrier import carrier_name
    from vm_controller.creation_sources import pins

    ctrl, api, store, row, admitted, source_name = await missing_carrier_runtime(
        db, setup, monkeypatch,
    )
    prepared = await store.prepare_disposition(request_id=row["request_id"])
    disposer = CreationDisposer(ctrl)
    lease = await disposer._publish_thread_cancel(prepared["carrier_intent"])
    assert (await store.freeze_disposition(
        request_id=row["request_id"], carrier=lease,
    ))["frozen"] is True
    name = carrier_name(row["creation_admission_id"])
    replaced = deepcopy(api.read("Lease", name))
    replaced["metadata"]["uid"] = str(UUID(int=5))
    api.objects["Lease", name] = replaced
    result = await disposer.run(disposition_identity(row))
    assert result["status"] == "creation_attention"
    assert (await store.inspect(request_id=row["request_id"]))["state"] == "cancel_requested"
    assert pins(api.read("DataVolume", source_name))[row["request_id"]]["state"] == "active"
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"


@pytest.mark.asyncio
async def test_thread_cancel_carrier_has_no_create_authority_and_needs_authorized_end(
    db, monkeypatch,
):
    from shared.vm_creation_issuance import verify_creation_carrier
    from shared.vm_creation_cancel_carrier import seal_cancel_carrier
    from tests.test_vm_creation_actuation import SECRET

    (
        _, _, _, _, thread_id, runtime, _, request_id,
        admitted, _, _,
    ) = await _adopted_charged_thread(
        db, monkeypatch, stop_before_effect=True, adopt=False,
        golden_enabled=True,
    )
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(runtime),
        expected_agent_id=None, expected_attach_token=None,
    )
    store = VMCreationRetryStore(db)
    with pytest.raises(VMCreationRetryConflict):
        await store.prepare_disposition(request_id=str(request_id))
    assert not await db.authorize_pinned_thread_retirement(
        str(thread_id), token=str(UUID(int=6)),
        generation=retirement["generation"], settle_status="ended",
    )
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id), token=retirement["token"],
        generation=retirement["generation"], settle_status="ended",
    )
    prepared = await store.prepare_disposition(request_id=str(request_id))
    carrier = seal_cancel_carrier(
        prepared["carrier_intent"], namespace="workers",
        uid=str(UUID(int=7)), resource_version="1", secret=SECRET,
    )
    with pytest.raises(ValueError):
        verify_creation_carrier(carrier, secret=SECRET)
    with pytest.raises(ValueError):
        await store.begin_effect(
            request_id=str(request_id), claim_token=str(UUID(int=8)),
            carrier=carrier,
        )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"


@pytest.mark.asyncio
async def test_ready_thread_end_keeps_active_physical_charge_for_vm_retirement(
    db, monkeypatch,
):
    prepared = await _ready_charged_thread(db, monkeypatch)
    assert await VMProvisioningPhaseStore(db).publish_thread_ready(
        str(prepared["thread_id"]), str(prepared["generation"]),
        prepared["registration"], prepared["vm_uid"], prepared["updates"],
    ) is True
    retirement = await db.begin_pinned_thread_retirement(
        str(prepared["thread_id"]), permanent=True,
        expected_runtime_generation=str(prepared["runtime"]),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert retirement["state"] == "pending", retirement
    assert retirement["context"]["vm_creation_source"] is None
    assert retirement["context"]["vm"]["vm_uid"] == prepared["vm_uid"]
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        prepared["admitted"]["reservation_id"],
    ) == "active"


@pytest.mark.asyncio
async def test_issued_unknown_thread_end_keeps_charge_until_authenticated_observation(
    db, monkeypatch,
):
    (
        _, _, _, _, thread_id, runtime, generation, request_id,
        admitted, observations, carrier,
    ) = await _adopted_charged_thread(
        db, monkeypatch, stop_after="rootdisk", adopt=False, observe_last=False,
    )
    assert observations == {}
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id), permanent=True,
        expected_runtime_generation=str(runtime),
        expected_agent_id=None, expected_attach_token=None,
    )
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id), token=retirement["token"],
        generation=retirement["generation"], settle_status="ended",
    )
    service = VMCreationDispositionStore(VMCreationRetryStore(db))
    assert await service.freeze(request_id=str(request_id), carrier=carrier) == {
        "frozen": False, "reason": "creation_effect_unresolved",
    }
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"
    assert not await db.fetchval(
        "SELECT public.thread_vm_creation_never_issued_source($1,$2)",
        thread_id, str(generation),
    )
    assert await VMCreationRetryStore(db).observe_effect(
        request_id=str(request_id), carrier=carrier,
        observation={"outcome": "rejected", "api_status": {
            "apiVersion": "v1", "kind": "Status", "status": "Failure",
            "code": 403, "reason": "Forbidden",
        }},
    ) == {"recorded": True, "effect_state": "rejected"}
    assert await VMCreationRetryStore(db).settle_never_issued(
        request_id=str(request_id),
    ) == {"settled": True, "disposition": "never_issued"}
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "released"
    assert await db.fetchval(
        "SELECT public.thread_vm_creation_never_issued_source($1,$2)",
        thread_id, str(generation),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("last_effect", ["rootdisk", "cloud_init"])
async def test_controller_disposes_exact_thread_partial_and_releases_old_charge(
    db, setup, monkeypatch, last_effect,
):
    ctrl, api, store, row, thread_id, admitted, observations, carrier = await controller_runtime(
        db, setup, monkeypatch, last_effect,
    )
    for _ in range(6):
        result = await CreationDisposer(ctrl).run(disposition_identity(row))
        if result["status"] == "creation_disposed":
            break
    assert result["status"] == "creation_disposed", result
    current = await store.inspect(request_id=row["request_id"])
    assert current["state"] == "settled"
    assert current["reason"] == "creation_disposed"
    assert await VMCreationDispositionStore(store).settle(
        request_id=row["request_id"], carrier=carrier,
    ) == {"settled": True, "disposition": "creation_disposed"}
    assert set(current["cancellation_completion"]) == {
        "source", "cloud_init", "rootdisk", "workspace_attachment",
    }
    expected = ["DataVolume", "PersistentVolumeClaim"]
    if last_effect == "cloud_init":
        expected.insert(0, "Secret")
    assert [item[0] for item in api.deletes if item[0] != "Lease"] == expected
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "released"
    assert not await db.fetchval(
        "SELECT metadata ? 'vm' FROM threads WHERE id=$1", thread_id,
    )
    assert await db.fetchval(
        "SELECT public.thread_vm_creation_never_issued_source($1,$2)",
        thread_id, row["provision_generation"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["DataVolume", "PersistentVolumeClaim", "Secret"])
async def test_replaced_thread_partial_uid_holds_charge_without_delete(
    db, setup, monkeypatch, kind,
):
    ctrl, api, store, row, _, admitted, _, _ = await controller_runtime(
        db, setup, monkeypatch, "cloud_init",
    )
    object_name = next(
        name for resource, name in api.objects if resource == kind
    )
    api.objects[kind, object_name]["metadata"]["uid"] = str(UUID(int=1))
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_attention", result
    assert api.deletes == []
    assert (await store.inspect(request_id=row["request_id"]))["state"] == "cancel_requested"
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "reserved"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["DataVolume", "PersistentVolumeClaim", "Secret"])
async def test_lost_thread_partial_delete_reply_requires_exact_readback(
    db, setup, monkeypatch, kind,
):
    ctrl, api, store, row, _, admitted, _, _ = await controller_runtime(
        db, setup, monkeypatch, "cloud_init",
    )
    api.lost_deletes.add(kind)
    for _ in range(8):
        result = await CreationDisposer(ctrl).run(disposition_identity(row))
        if result["status"] == "creation_disposed":
            break
    assert result["status"] == "creation_disposed", result
    assert [item[0] for item in api.deletes if item[0] != "Lease"] == [
        "Secret", "DataVolume", "PersistentVolumeClaim",
    ]
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == "released"
