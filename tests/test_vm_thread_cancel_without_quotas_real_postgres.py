"""Durable thread cancellation retains authority when optional quotas are off."""

import json
from copy import deepcopy
from uuid import UUID, uuid4

import pytest

from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)
from tests.test_pinned_vm_initial_binding_real_postgres import (
    _bind_cold_agent,
    _initial_vm,
)
from tests.test_vm_creation_actuation import SECRET, setup as _setup
from tests.test_vm_resource_configuration import whole_launcher_configuration
from tests.test_vm_resource_thread_source_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    db as _db,
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)

db = _db
setup = _setup


async def cancelled_source(
    db, monkeypatch, *, retire=True, golden_enabled=True, creation_resolver=None
):
    thread_id, _, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_cold_agent(db, thread_id)
    configuration = whole_launcher_configuration()
    configuration.update(
        version=1,
        namespace="workers",
        storage_class="local",
        golden_enabled=golden_enabled,
    )
    configuration.pop("resource_admission")
    monkeypatch.delenv("VM_RESOURCE_ADMISSION_CONFIG")
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())

    async def resolve(_client, request, *, secret):
        return (
            creation_resolver(request)
            if creation_resolver
            else {"request": request, "controller_configuration": configuration}
        )

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    assert await dependencies.vm_provisioner.create_thread_vm(
        str(thread_id),
        vm_image=override["workspace"]["vm"]["image"],
        expected_runtime_generation=str(current["runtime_generation"]),
        expected_agent_id=str(current["agent_id"]),
        expected_attach_token=str(current["runtime_attach_token"]),
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    retry = VMCreationRetryStore(db)
    claim = (await retry.claim_due(limit=1))[0]
    grant = await retry.authorize_controller(
        request_id=str(source["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(thread_id),
            "provision_generation": str(source["provision_generation"]),
            "request_digest": source["request_digest"],
            "controller_configuration_digest": source[
                "controller_configuration_digest"
            ],
            "expected_pvc_uid": None,
        },
    )
    assert grant["allowed"] is True
    if not retire:
        return (
            retry,
            await retry.inspect(request_id=str(source["request_id"])),
            current,
            None,
        )
    retirement = await retire_source(db, current)
    row = await retry.inspect(request_id=str(source["request_id"]))
    assert row["state"] == "cancel_requested"
    assert row["creation_admission_id"] == str(grant["admission_id"])
    assert row["creation_carrier_uid"] is None and row["effects"] == []
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    return retry, row, current, retirement


async def retire_source(db, current):
    retirement = await db.begin_pinned_thread_retirement(
        str(current["id"]), permanent=True
    )
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        str(current["id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    return retirement


@pytest.mark.asyncio
async def test_cancel_with_original_admission_without_quotas_keeps_source_obligation(
    db, monkeypatch
):
    retry, row, current, retirement = await cancelled_source(db, monkeypatch)
    # An admission may already have published a golden pin before its create
    # Lease/effect. Empty effects alone must not bypass that obligation.
    assert await retry.settle_never_issued(request_id=row["request_id"]) == {
        "settled": False,
        "reason": "creation_source_unresolved",
    }
    prepared = await retry.prepare_disposition(request_id=row["request_id"])
    assert prepared["actuation_allowed"] is False
    intent = prepared["carrier_intent"]
    assert intent["version"] == 2
    assert intent["kind"] == "thread_creation_cancel"
    assert intent["admission_id"] == row["creation_admission_id"]
    assert intent["thread_runtime_generation"] == str(current["runtime_generation"])
    assert intent["thread_agent_id"] == str(current["agent_id"])
    assert intent["thread_attach_token"] == str(current["runtime_attach_token"])
    assert intent["retirement_token"] == retirement["token"]
    assert intent["source_pin_key"] == row["request_id"]
    assert not {
        "reservation_id",
        "reservation_revision",
        "reservation_cluster_id",
        "reservation_policy_digest",
        "effect_nonce",
        "effect_kind",
        "resource_grant",
    }.intersection(intent)
    owner = await db.get_thread(str(current["id"]))
    metadata = json.loads(owner["metadata"])
    assert metadata["vm"]["creation_request_id"] == row["request_id"]
    assert metadata["vm"].get("vm_uid") is None
    assert owner["runtime_retirement_token"] is not None


async def golden_runtime(db, setup, monkeypatch):
    from kubernetes.client.exceptions import ApiException
    from vm_controller import controller as settings
    from vm_controller.creation_sources import GoldenSources

    retry, row, current, _ = await cancelled_source(db, monkeypatch, retire=False)
    ctrl, api, _, _ = setup
    monkeypatch.setattr(settings, "VM_NAMESPACE", "workers")
    monkeypatch.setattr(settings, "VM_STORAGE_CLASS", "local")
    monkeypatch.setattr(settings, "VM_GOLDEN_DISK_SIZE", "10Gi")
    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", True)

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        assert operation == "creation_retry_" + method
        assert method not in {"authorize", "begin_effect", "settle_adopted"}
        return await getattr(retry, method)(**body)

    ctrl._workspace_cleanup_authority_request = authority
    name = settings._golden_name(row["request"]["vm_image"])
    dv = ctrl._golden_dv_manifest(name, row["request"]["vm_image"])
    dv["metadata"].update(uid=str(uuid4()), resourceVersion="1")
    dv["status"] = {"phase": "Succeeded"}
    api.objects["DataVolume", name] = dv
    api.objects["PersistentVolumeClaim", name] = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": name,
            "namespace": "workers",
            "uid": str(uuid4()),
            "resourceVersion": "1",
            "ownerReferences": [
                {
                    "kind": "DataVolume",
                    "uid": dv["metadata"]["uid"],
                    "controller": True,
                }
            ],
        },
        "spec": {"volumeMode": "Filesystem"},
        "status": {"phase": "Bound"},
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
        api.objects["DataVolume", name] = body
        api.replacements.append(deepcopy(body))
        if "DataVolume" in api.lost:
            api.lost.remove("DataVolume")
            raise TimeoutError("accepted source CAS reply lost")
        return body

    ctrl.k8s_client.replace_namespaced_custom_object = replace_source
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    source, observed = await GoldenSources(ctrl).facts(row, name)
    assert await GoldenSources(ctrl).hold(row, source, observed) == source
    retirement = await retire_source(db, current)
    row = await retry.inspect(request_id=row["request_id"])
    return ctrl, api, retry, row, current, retirement, name


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_quota_free_controller_cancellation_closes_exact_source_before_settlement(
    db, setup, monkeypatch, lost_reply
):
    from shared.vm_creation_disposition import disposition_identity
    from shared.vm_creation_cancel_carrier import carrier_name, verify_cancel_carrier
    from vm_controller.creation_disposition import CreationDisposer
    from vm_controller.creation_sources import pins

    ctrl, api, retry, row, current, retirement, name = await golden_runtime(
        db, setup, monkeypatch
    )
    assert pins(api.read("DataVolume", name))[row["request_id"]]["state"] == "active"
    prepared = await retry.prepare_disposition(request_id=row["request_id"])
    lease = await CreationDisposer(ctrl)._publish_thread_cancel(
        prepared["carrier_intent"]
    )
    assert verify_cancel_carrier(lease, secret=SECRET)["version"] == 2
    assert (
        await retry.freeze_disposition(request_id=row["request_id"], carrier=lease)
    )["frozen"]
    with pytest.raises(
        VMCreationRetryConflict, match="creation_disposition_incomplete"
    ):
        await retry.settle_disposition(request_id=row["request_id"], carrier=lease)
    if lost_reply:
        api.lost.add("DataVolume")
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_disposed", result
    settled = await retry.inspect(request_id=row["request_id"])
    assert settled["state"] == "settled" and settled["reason"] == "creation_disposed"
    assert settled["cancellation_completion"]["source"]["outcome"] == "pin_disposed"
    assert pins(api.read("DataVolume", name))[row["request_id"]]["state"] == "disposed"
    assert len(api.replacements) == 2
    assert settled["creation_carrier_uid"] is None and settled["effects"] == []
    assert (
        settled["disposition_carrier_uid"]
        == api.read("Lease", carrier_name(row["creation_admission_id"]))["metadata"][
            "uid"
        ]
    )
    assert await retry.settle_disposition(
        request_id=row["request_id"], carrier=lease
    ) == {
        "settled": True,
        "disposition": "creation_disposed",
    }
    owner = await db.get_thread(str(current["id"]))
    assert "vm" not in json.loads(owner["metadata"])
    assert str(owner["runtime_generation"]) == retirement["generation"]
    assert str(owner["runtime_retirement_token"]) == retirement["token"]
    assert await db.fetchval(
        "SELECT completed_at IS NOT NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
        UUID(row["creation_admission_id"]),
    )
    assert await retry.claim_due(limit=10) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "admission_id",
        "reservation_request_id",
        "intent_digest",
        "retry_request_id",
        "thread_id",
        "thread_runtime_generation",
        "thread_agent_id",
        "thread_attach_token",
        "thread_wake_operation_id",
        "retirement_token",
        "provision_generation",
        "request_digest",
        "controller_configuration_digest",
    ],
)
async def test_quota_free_cancel_rejects_changed_signed_authority(
    db, monkeypatch, field
):
    from shared.vm_creation_cancel_carrier import seal_cancel_carrier

    retry, row, current, _ = await cancelled_source(db, monkeypatch)
    prepared = await retry.prepare_disposition(request_id=row["request_id"])
    changed = dict(prepared["carrier_intent"])
    changed[field] = "sha256:" + "f" * 64 if field.endswith("digest") else str(uuid4())
    if field == "retry_request_id":
        changed["source_pin_key"] = changed[field]
    carrier = seal_cancel_carrier(
        changed,
        namespace="workers",
        uid=str(uuid4()),
        resource_version="1",
        secret=SECRET,
    )
    with pytest.raises(
        VMCreationRetryConflict, match="creation_disposition_carrier_changed"
    ):
        await retry.freeze_disposition(request_id=row["request_id"], carrier=carrier)
    untouched = await retry.inspect(request_id=row["request_id"])
    assert untouched["cancellation_disposition"] is None
    assert untouched["disposition_carrier_uid"] is None and untouched["effects"] == []
    assert "vm" in json.loads((await db.get_thread(str(current["id"])))["metadata"])


@pytest.mark.asyncio
async def test_quota_free_cancel_does_not_authorize_creation(db, monkeypatch):
    from shared.vm_creation_cancel_carrier import seal_cancel_carrier, validate_intent
    from shared.vm_creation_issuance import verify_creation_carrier

    retry, row, _, _ = await cancelled_source(db, monkeypatch)
    prepared = await retry.prepare_disposition(request_id=row["request_id"])
    intent = prepared["carrier_intent"]
    carrier = seal_cancel_carrier(
        intent,
        namespace="workers",
        uid=str(uuid4()),
        resource_version="1",
        secret=SECRET,
    )
    with pytest.raises(ValueError):
        verify_creation_carrier(carrier, secret=SECRET)
    for extra in ("reservation_id", "effect_kind", "effect_nonce", "resource_grant"):
        with pytest.raises(ValueError, match="intent is incomplete"):
            validate_intent({**intent, extra: str(uuid4())})
    assert (await retry.inspect(request_id=row["request_id"]))["effects"] == []


@pytest.mark.asyncio
async def test_resource_thread_cannot_downgrade_to_quota_free_cancel(db, monkeypatch):
    from shared.vm_creation_cancel_carrier import seal_cancel_carrier
    from tests.test_vm_resource_thread_source_real_postgres import (
        _adopted_charged_thread,
    )

    (
        _,
        _,
        _,
        _,
        thread_id,
        runtime,
        _,
        request_id,
        admitted,
        _,
        _,
    ) = await _adopted_charged_thread(
        db, monkeypatch, stop_before_effect=True, adopt=False, golden_enabled=True
    )
    retirement = await db.begin_pinned_thread_retirement(
        str(thread_id),
        permanent=True,
        expected_runtime_generation=str(runtime),
        expected_agent_id=None,
        expected_attach_token=None,
    )
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    retry = VMCreationRetryStore(db)
    prepared = await retry.prepare_disposition(request_id=str(request_id))
    assert prepared["carrier_intent"]["version"] == 1
    downgraded = {
        key: value
        for key, value in prepared["carrier_intent"].items()
        if key
        not in {
            "reservation_id",
            "reservation_revision",
            "reservation_cluster_id",
            "reservation_policy_digest",
        }
    }
    downgraded["version"] = 2
    carrier = seal_cancel_carrier(
        downgraded,
        namespace="workers",
        uid=str(uuid4()),
        resource_version="1",
        secret=SECRET,
    )
    with pytest.raises(
        VMCreationRetryConflict, match="creation_disposition_carrier_changed"
    ):
        await retry.freeze_disposition(request_id=str(request_id), carrier=carrier)
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            admitted["reservation_id"],
        )
        == "reserved"
    )


@pytest.mark.asyncio
async def test_quota_free_cancel_refuses_unready_source_and_replacement_lease(
    db, setup, monkeypatch
):
    from shared.vm_creation_disposition import disposition_identity
    from shared.vm_creation_cancel_carrier import carrier_name
    from vm_controller.creation_disposition import CreationDisposer
    from vm_controller.creation_sources import pins

    ctrl, api, retry, row, current, _, name = await golden_runtime(
        db, setup, monkeypatch
    )
    api.objects["DataVolume", name]["status"]["phase"] = "ImportInProgress"
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_disposition_pending"
    assert pins(api.read("DataVolume", name))[row["request_id"]]["state"] == "active"
    assert (await retry.inspect(request_id=row["request_id"]))[
        "state"
    ] == "cancel_requested"
    assert await db.fetchval(
        "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
        UUID(row["creation_admission_id"]),
    )
    assert "vm" in json.loads((await db.get_thread(str(current["id"])))["metadata"])
    api.objects["DataVolume", name]["status"]["phase"] = "Succeeded"
    lease = api.objects["Lease", carrier_name(row["creation_admission_id"])]
    original_uid = lease["metadata"]["uid"]
    lease["metadata"]["uid"] = str(uuid4())
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_attention"
    assert pins(api.read("DataVolume", name))[row["request_id"]]["state"] == "active"
    lease["metadata"]["uid"] = original_uid
    assert (await CreationDisposer(ctrl).run(disposition_identity(row)))[
        "status"
    ] == "creation_disposed"
