"""Typed partial creation End keeps a genuine thread charge until exact cleanup."""

from pathlib import Path
from copy import deepcopy
from uuid import UUID

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
