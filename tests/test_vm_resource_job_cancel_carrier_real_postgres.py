"""A resource-enforced Job End can settle only its exact no-effect source."""

from copy import deepcopy
import json
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
from kubernetes.client.exceptions import ApiException

from orchestrator.services.vm_creation_disposition_store import VMCreationDispositionStore
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict, VMCreationRetryStore,
)
from shared.vm_creation_disposition import disposition_identity
from shared.vm_creation_issuance import seal_creation_carrier, verify_creation_carrier
from shared.vm_creation_job_cancel_carrier import seal_cancel_carrier
from tests.test_vm_creation_actuation import (
    SECRET, setup as _controller_setup,  # noqa: F401
)
from tests.test_vm_resource_job_runtime_real_postgres import (
    db as _job_db,
    _db_fixture,  # noqa: F401
    whole_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    runtime_schema,  # noqa: F401
    environment, waiter,
)
from vm_controller import controller as controller_settings
from vm_controller.creation_disposition import CreationDisposer

db = _job_db
controller_setup = _controller_setup


async def cancelled_job(db, controller_setup, monkeypatch, *, golden=False):
    if golden:
        from tests import test_vm_resource_whole_store_real_postgres as whole

        config = whole.whole_launcher_configuration()
        config["golden_enabled"] = True
        monkeypatch.setattr(whole, "whole_launcher_configuration", lambda: deepcopy(config))
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory, lane="stateless")
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    authorized = await store.authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(claim["job_id"]),
            "provision_generation": str(claim["provision_generation"]),
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    assert authorized["allowed"]
    ctrl, api, _, _ = controller_setup
    monkeypatch.setattr(controller_settings, "VM_NAMESPACE", "workers")

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        return await getattr(store, method)(**body)

    ctrl._workspace_cleanup_authority_request = authority
    if golden:
        from vm_controller.creation_sources import GoldenSources

        monkeypatch.setattr(controller_settings, "VM_STORAGE_CLASS", "local")
        monkeypatch.setattr(controller_settings, "VM_GOLDEN_DISK_SIZE", "10Gi")
        monkeypatch.setattr(controller_settings, "VM_GOLDEN_IMAGE_ENABLED", True)
        row = await store.inspect(request_id=str(retry["request_id"]))
        name = controller_settings._golden_name(row["request"]["vm_image"])
        dv = ctrl._golden_dv_manifest(name, row["request"]["vm_image"])
        dv["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        dv["status"] = {"phase": "Succeeded"}
        api.objects["DataVolume", name] = dv
        api.objects["PersistentVolumeClaim", name] = {
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": name, "namespace": "workers", "uid": str(uuid4()),
                "resourceVersion": "1", "ownerReferences": [{
                    "kind": "DataVolume", "uid": dv["metadata"]["uid"],
                    "controller": True,
                }],
            },
            "spec": {"volumeMode": "Filesystem"}, "status": {"phase": "Bound"},
        }

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
            return deepcopy(body)

        ctrl.k8s_client.replace_namespaced_custom_object = replace_source
        source, observed = await GoldenSources(ctrl).facts(row, name)
        assert await GoldenSources(ctrl).hold(row, source, observed) == source
    assert (await db.cancel_stateless_job(str(retry["job_id"])))[0]
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"}, "items": [],
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"}, "items": [],
    }

    return retry, admitted, store, ctrl, api, authorized


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_job_cancel_settles_exact_no_effect_carrier(
    db, controller_setup, monkeypatch, legacy,
):
    retry, admitted, store, ctrl, api, _ = await cancelled_job(
        db, controller_setup, monkeypatch,
    )
    request_id = str(retry["request_id"])
    prepared = await VMCreationDispositionStore(store).prepare(request_id=request_id)
    assert prepared["carrier_intent"]["kind"] == "job_creation_cancel"
    original_uid = str(uuid4())
    if legacy:
        old = seal_creation_carrier(
            prepared["legacy_intent"], namespace="workers", uid=original_uid,
            resource_version="1", secret=SECRET,
        )
        api.objects["Lease", old["metadata"]["name"]] = old
    result = await CreationDisposer(ctrl).run(disposition_identity(retry))
    assert result["status"] == "creation_disposed", result
    row = await store.inspect(request_id=request_id)
    assert row["disposition_carrier_uid"]
    assert row["creation_carrier_uid"] is None
    assert row["cancellation_disposition"]["carrier_kind"] == "job_creation_cancel"
    if legacy:
        assert row["disposition_carrier_uid"] == original_uid
    lease = api.objects[
        "Lease", "srw-cleanup-" + UUID(row["creation_admission_id"]).hex
    ]
    assert lease["metadata"]["uid"] == row["disposition_carrier_uid"]
    from shared.vm_creation_job_cancel_carrier import verify_cancel_carrier

    assert verify_cancel_carrier(lease, secret=SECRET) == prepared["carrier_intent"]
    with pytest.raises(ValueError):
        verify_creation_carrier(lease, secret=SECRET)
    assert await VMCreationDispositionStore(store).settle(
        request_id=request_id, carrier=lease,
    ) == {"settled": True, "disposition": "creation_disposed"}
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "released"
    assert await db.fetchval(
        "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
        retry["request_id"],
    ) == "released"


@pytest.mark.asyncio
async def test_job_cancel_legacy_carrier_disposes_golden_pin_before_release(
    db, controller_setup, monkeypatch,
):
    from vm_controller.creation_sources import pins

    retry, admitted, store, ctrl, api, _ = await cancelled_job(
        db, controller_setup, monkeypatch, golden=True,
    )
    request_id = str(retry["request_id"])
    row = await store.inspect(request_id=request_id)
    name = controller_settings._golden_name(row["request"]["vm_image"])
    assert pins(api.read("DataVolume", name))[request_id]["state"] == "active"
    prepared = await VMCreationDispositionStore(store).prepare(request_id=request_id)
    old = seal_creation_carrier(
        prepared["legacy_intent"], namespace="workers", uid=str(uuid4()),
        resource_version="1", secret=SECRET,
    )
    api.objects["Lease", old["metadata"]["name"]] = old
    result = await CreationDisposer(ctrl).run(disposition_identity(retry))
    assert result["status"] == "creation_disposed", result
    settled = await store.inspect(request_id=request_id)
    assert settled["cancellation_completion"]["source"]["outcome"] == "pin_disposed"
    assert pins(api.read("DataVolume", name))[request_id]["state"] == "disposed"
    assert settled["disposition_carrier_uid"] == old["metadata"]["uid"]
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "released"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("forged_grant", "policy_off"),
    [(False, False), (False, True), (True, False)],
)
async def test_job_cancel_existing_v4_golden_carrier_keeps_full_grant_authority(
    db, controller_setup, monkeypatch, forged_grant, policy_off,
):
    from vm_controller.creation_sources import GoldenSources, pins

    retry, admitted, store, ctrl, api, authorized = await cancelled_job(
        db, controller_setup, monkeypatch, golden=True,
    )
    request_id = str(retry["request_id"])
    row = await store.inspect(request_id=request_id)
    name = controller_settings._golden_name(row["request"]["vm_image"])
    source, _ = await GoldenSources(ctrl).facts(row, name)
    prepared = await VMCreationDispositionStore(store).prepare(request_id=request_id)
    grant = deepcopy(authorized["resource_grant"])
    if forged_grant:
        grant["id"] = str(uuid4())
    intent = {
        **prepared["legacy_intent"],
        "version": 4,
        "resource_grant": grant,
        "rootdisk_source": source,
        "effect_nonce": str(uuid4()),
    }
    carrier = seal_creation_carrier(
        intent, namespace="workers", uid=str(uuid4()),
        resource_version="1", secret=SECRET,
    )
    api.objects["Lease", carrier["metadata"]["name"]] = carrier
    if policy_off:
        await db.execute(
            "UPDATE vm_resource_admission_policy SET mode='off' WHERE cluster_id=$1",
            authorized["resource_grant"]["cluster_id"],
        )
    outcome = await CreationDisposer(ctrl).run(disposition_identity(retry))
    after = await store.inspect(request_id=request_id)
    assert api.objects["Lease", carrier["metadata"]["name"]] == carrier
    if forged_grant:
        assert outcome["status"] != "creation_disposed"
        assert after["cancellation_disposition"] is None
        assert after["creation_carrier_uid"] is None
        assert pins(api.read("DataVolume", name))[request_id]["state"] == "active"
        assert await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(admitted["reservation_id"]),
        ) == "reserved"
    else:
        assert outcome["status"] == "creation_disposed", outcome
        assert after["creation_carrier_uid"] == carrier["metadata"]["uid"]
        assert after["disposition_carrier_uid"] is None
        assert after["cancellation_disposition"].get("carrier_kind") is None
        assert after["cancellation_completion"]["source"]["outcome"] == "pin_disposed"
        assert await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(admitted["reservation_id"]),
        ) == "released"


@pytest.mark.asyncio
async def test_job_cancel_refuses_changed_legacy_intent_and_uid(
    db, controller_setup, monkeypatch,
):
    retry, admitted, store, ctrl, api, _ = await cancelled_job(
        db, controller_setup, monkeypatch,
    )
    request_id = str(retry["request_id"])
    prepared = await VMCreationDispositionStore(store).prepare(request_id=request_id)
    old_intent = deepcopy(prepared["legacy_intent"])
    old_intent["effect_nonce"] = str(uuid4())
    old = seal_creation_carrier(
        old_intent, namespace="workers", uid=str(uuid4()),
        resource_version="1", secret=SECRET,
    )
    api.objects["Lease", old["metadata"]["name"]] = old
    assert (await CreationDisposer(ctrl).run(disposition_identity(retry)))[
        "status"
    ] == "creation_attention"
    assert api.objects["Lease", old["metadata"]["name"]] == old
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "reserved"

    good = seal_creation_carrier(
        prepared["legacy_intent"], namespace="workers", uid=str(uuid4()),
        resource_version="1", secret=SECRET,
    )
    api.objects["Lease", good["metadata"]["name"]] = good
    await db.execute(
        "UPDATE vm_creation_retries SET creation_carrier_uid=$2,"
        "creation_carrier_namespace='workers' WHERE request_id=$1",
        retry["request_id"], uuid4(),
    )
    assert (await CreationDisposer(ctrl).run(disposition_identity(retry)))[
        "status"
    ] == "creation_attention"
    assert api.objects["Lease", good["metadata"]["name"]] == good


@pytest.mark.asyncio
async def test_job_cancel_lost_replace_reply_replays_exact_legacy_uid(
    db, controller_setup, monkeypatch,
):
    retry, _, store, ctrl, api, _ = await cancelled_job(
        db, controller_setup, monkeypatch,
    )
    prepared = await VMCreationDispositionStore(store).prepare(
        request_id=str(retry["request_id"]),
    )
    old = seal_creation_carrier(
        prepared["legacy_intent"], namespace="workers", uid=str(uuid4()),
        resource_version="1", secret=SECRET,
    )
    api.objects["Lease", old["metadata"]["name"]] = old
    replace = api.replace
    lost = False

    def replace_then_lose(body):
        nonlocal lost
        result = replace(body)
        if not lost:
            lost = True
            raise TimeoutError("CAS reply lost after acceptance")
        return result

    api.replace = replace_then_lose
    result = await CreationDisposer(ctrl).run(disposition_identity(retry))
    assert result["status"] == "creation_disposed", result
    assert lost
    row = await store.inspect(request_id=str(retry["request_id"]))
    assert row["disposition_carrier_uid"] == old["metadata"]["uid"]
    assert api.objects["Lease", old["metadata"]["name"]]["metadata"][
        "resourceVersion"
    ] == "2"


@pytest.mark.asyncio
@pytest.mark.parametrize("race", ["effect", "stale_intent"])
async def test_job_cancel_refuses_raced_effect_or_stale_reservation(
    db, controller_setup, monkeypatch, race,
):
    retry, admitted, store, _, _, _ = await cancelled_job(
        db, controller_setup, monkeypatch,
    )
    request_id = str(retry["request_id"])
    service = VMCreationDispositionStore(store)
    prepared = await service.prepare(request_id=request_id)
    carrier = seal_cancel_carrier(
        prepared["carrier_intent"], namespace="workers", uid=str(uuid4()),
        resource_version="1", secret=SECRET,
    )
    if race == "effect":
        await db.execute(
            "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,"
            "effect_kind,carrier_uid,carrier_namespace,carrier_intent) "
            "VALUES($1,$2,1,'rootdisk',$3,'workers',$4::jsonb)",
            uuid4(), retry["request_id"], uuid4(),
            json.dumps(prepared["legacy_intent"]),
        )
        with pytest.raises(VMCreationRetryConflict, match="creation_carrier_required"):
            await service.prepare(request_id=request_id)
    else:
        stale = deepcopy(prepared["carrier_intent"])
        stale["reservation_revision"] += 1
        carrier = seal_cancel_carrier(
            stale, namespace="workers", uid=carrier["metadata"]["uid"],
            resource_version="1", secret=SECRET,
        )
    with pytest.raises(VMCreationRetryConflict):
        await service.freeze(request_id=request_id, carrier=carrier)
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "reserved"


@pytest.mark.asyncio
async def test_job_cancel_freeze_fences_uid_and_direct_sql_creation_carrier(
    db, controller_setup, monkeypatch,
):
    retry, admitted, store, _, _, _ = await cancelled_job(
        db, controller_setup, monkeypatch,
    )
    request_id = str(retry["request_id"])
    service = VMCreationDispositionStore(store)
    prepared = await service.prepare(request_id=request_id)
    uid = str(uuid4())
    carrier = seal_cancel_carrier(
        prepared["carrier_intent"], namespace="workers", uid=uid,
        resource_version="1", secret=SECRET,
    )
    for carrier_kind in (None, "thread_creation_cancel"):
        forged = {
            "request_id": request_id,
            "admission_id": prepared["carrier_intent"]["admission_id"],
            "job_id": str(retry["job_id"]),
            "provision_generation": str(retry["provision_generation"]),
            "carrier_uid": uid, "namespace": "workers",
        }
        if carrier_kind is not None:
            forged["carrier_kind"] = carrier_kind
        with pytest.raises(asyncpg.CheckViolationError):
            await db.execute(
                "UPDATE vm_creation_retries SET disposition_carrier_uid=$2,"
                "disposition_carrier_namespace='workers',"
                "cancellation_disposition=$3::jsonb WHERE request_id=$1",
                retry["request_id"], UUID(uid), json.dumps(forged),
            )
    assert (await service.freeze(request_id=request_id, carrier=carrier))["frozen"]
    changed_uid = seal_cancel_carrier(
        prepared["carrier_intent"], namespace="workers", uid=str(uuid4()),
        resource_version="1", secret=SECRET,
    )
    with pytest.raises(VMCreationRetryConflict, match="creation_disposition_carrier_changed"):
        await service.freeze(request_id=request_id, carrier=changed_uid)
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "UPDATE vm_creation_retries SET creation_carrier_uid=$2,"
            "creation_carrier_namespace='workers' WHERE request_id=$1",
            retry["request_id"], uuid4(),
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await db.execute(
            "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,"
            "effect_kind,carrier_uid,carrier_namespace,carrier_intent) "
            "VALUES($1,$2,1,'rootdisk',$3,'workers','{}'::jsonb)",
            uuid4(), retry["request_id"], uuid4(),
        )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "reserved"


@pytest.mark.asyncio
async def test_job_cancel_migration_upgrades_0281_guard_in_place(pg_dsn, _schema_applied):  # noqa: F811
    """The additive migration applies to the predecessor constraint/function."""
    migrations = Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    old = (migrations / "0280_vm_thread_cancel_carrier.sql").read_text()
    start = old.index("CREATE OR REPLACE FUNCTION public.guard_vm_creation_disposition()")
    end = old.index("\n$$;", start) + len("\n$$;")
    conn = await asyncpg.connect(pg_dsn)
    tx = conn.transaction()
    await tx.start()
    try:
        await conn.execute(
            "ALTER TABLE public.vm_creation_retries "
            "DROP CONSTRAINT vm_cancel_carrier_exclusive, "
            "ADD CONSTRAINT vm_thread_cancel_carrier_exclusive CHECK ("
            "disposition_carrier_uid IS NULL OR "
            "(owner_kind='thread' AND creation_carrier_uid IS NULL))"
        )
        await conn.execute(old[start:end])
        await conn.execute(
            "DROP FUNCTION public.valid_vm_creation_job_cancel_identity("
            "public.vm_creation_retries)"
        )
        for filename in (
            "0282_vm_job_cancel_carrier.sql",
            "0283_validate_vm_job_cancel_carrier.sql",
        ):
            sql = "\n".join(
                line for line in (migrations / filename).read_text().splitlines()
                if line not in {"BEGIN;", "COMMIT;"}
            )
            await conn.execute(sql)
        assert await conn.fetchval(
            "SELECT convalidated FROM pg_constraint "
            "WHERE conname='vm_cancel_carrier_exclusive'"
        ) is True
    finally:
        await tx.rollback()
        await conn.close()
