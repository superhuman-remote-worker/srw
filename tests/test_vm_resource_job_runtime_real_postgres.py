"""Job resource reservation before the actual creation-retry transport."""

from copy import deepcopy
from datetime import datetime, timezone
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services.vm_creation_retry import VMCreationRetryService
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from orchestrator.services.vm_resource_job_runtime import installed_job_resource_store
from shared.vm_creation_issuance import seal_creation_carrier
from shared.vm_lifecycle_auth import AUTH_FIELD, sign_payload
from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_effect_node import fresh_resource_effect_node
from tests.test_vm_resource_whole_store_real_postgres import (
    db as _db_fixture,
    whole_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    environment,
    waiter,
)
from tests.test_vm_resource_inventory_real_postgres import publish
from tests.test_vm_resource_policy import whole_launcher_policy

db = _db_fixture


@pytest.mark.asyncio
@pytest.mark.parametrize("entity_type", ["job", "thread"])
async def test_installed_enforcement_refuses_legacy_controller_create_for_any_owner(
    monkeypatch, entity_type,
):
    from vm_controller.controller import VMController

    document = whole_launcher_policy()
    document["policy"].update(shadowEnabled=True, enforcementEnabled=True)
    monkeypatch.setenv("VM_RESOURCE_ADMISSION_CONFIG", json.dumps(document))
    controller = VMController.__new__(VMController)
    with pytest.raises(ValueError, match="durable creation authority"):
        await controller._do_create_serialized({
            "job_id": str(uuid4()), "entity_type": entity_type,
        })


class UnexpectedCreate:
    def __init__(self):
        self.calls = 0

    async def post(self, *_args, **_kwargs):
        self.calls += 1
        raise AssertionError("unreserved controller create")


class SignedPendingCreate:
    def __init__(self, db, request_id):
        self.db = db
        self.request_id = request_id
        self.calls = 0

    async def post(self, path, *, json, timeout):
        assert path == "/vm-creation/create"
        assert timeout == 30.0
        assert await self.db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations "
            "WHERE request_id=$1 AND state='reserved'", self.request_id,
        ) == 1
        self.calls += 1
        result = sign_payload(
            {
                "job_id": json["job_id"],
                "provision_generation": json["provision_generation"],
                "status": "creation_pending",
                "reason": "creation_observation_pending",
            },
            direction="response", operation="creation_retry_create",
            secret=b"test-key",
            correlation_id=json[AUTH_FIELD]["request_id"],
        )
        return SimpleNamespace(
            status_code=200, json=lambda: result, raise_for_status=lambda: None,
        )


@pytest.mark.asyncio
async def test_retry_scan_preserves_waiter_and_never_posts_with_conflicted_inventory(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    await db.execute(
        "UPDATE vm_resource_inventory_heads SET observation_conflict=TRUE "
        "WHERE cluster_id=$1 AND policy_digest=$2",
        inventory.cluster_id, inventory.policy_digest,
    )
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    client = UnexpectedCreate()
    service = VMCreationRetryService(
        db,
        SimpleNamespace(_http_client=client, _lifecycle_hmac_secret=b"test-key"),
    )

    await service._replay(claim)

    row = await db.fetchrow(
        "SELECT state,reason,request_digest,provision_generation "
        "FROM vm_creation_retries WHERE request_id=$1", retry["request_id"],
    )
    assert (row["state"], row["reason"]) == ("queued", "capacity_wait")
    assert row["request_digest"] == retry["request_digest"]
    assert row["provision_generation"] == retry["provision_generation"]
    assert await db.fetchval(
        "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
        retry["request_id"],
    ) == 0
    assert client.calls == 0


@pytest.mark.asyncio
async def test_retry_scan_commits_held_reservation_before_signed_controller_create(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    client = SignedPendingCreate(db, retry["request_id"])
    service = VMCreationRetryService(
        db,
        SimpleNamespace(_http_client=client, _lifecycle_hmac_secret=b"test-key"),
    )

    await service._replay(claim)

    assert client.calls == 1
    row = await db.fetchrow(
        "SELECT id,revision,node_uid,node_name FROM vm_resource_reservations "
        "WHERE request_id=$1 AND state='reserved'", retry["request_id"],
    )
    assert row is not None and row["revision"] == 1


@pytest.mark.asyncio
async def test_issued_effect_replays_for_observation_after_installed_policy_is_off(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    await db.execute(
        "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,"
        "effect_kind,carrier_uid,carrier_namespace,carrier_intent) "
        "VALUES($1,$2,1,'rootdisk',$3,'workers','{}'::jsonb)",
        uuid4(), retry["request_id"], uuid4(),
    )
    await db.execute(
        "UPDATE vm_resource_admission_policy SET mode='off',revision=revision+1 "
        "WHERE cluster_id=$1", inventory.cluster_id,
    )
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    client = SignedPendingCreate(db, retry["request_id"])
    service = VMCreationRetryService(
        db,
        SimpleNamespace(_http_client=client, _lifecycle_hmac_secret=b"test-key"),
    )

    await service._replay(claim)

    assert client.calls == 1
    row = await db.fetchrow(
        "SELECT state,reason FROM vm_creation_retries WHERE request_id=$1",
        retry["request_id"],
    )
    assert (row["state"], row["reason"]) == (
        "queued", "creation_observation_pending",
    )


@pytest.mark.asyncio
async def test_controller_authorize_refuses_v3_request_without_held_reservation(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    observed = {
        "job_id": str(claim["job_id"]),
        "provision_generation": str(claim["provision_generation"]),
        "request_digest": claim["request_digest"],
        "controller_configuration_digest": claim["controller_configuration_digest"],
        "expected_pvc_uid": None,
    }

    result = await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed=observed,
    )

    assert result == {"allowed": False, "reason": "resource_reservation_missing"}
    assert await db.fetchval(
        "SELECT creation_admission_id FROM vm_creation_retries WHERE request_id=$1",
        retry["request_id"],
    ) is None


@pytest.mark.asyncio
async def test_v3_authorize_and_begin_bind_exact_held_resource_grant(db, monkeypatch):
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", "resource-test-secret-at-least-32-bytes")
    policy, inventory, _, demand = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    store = VMCreationRetryStore(db)
    observed = {
        "job_id": str(claim["job_id"]),
        "provision_generation": str(claim["provision_generation"]),
        "request_digest": claim["request_digest"],
        "controller_configuration_digest": claim["controller_configuration_digest"],
        "expected_pvc_uid": None,
    }
    authorization = await store.authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]), observed=observed,
    )
    assert authorization["allowed"] is True
    grant = authorization["resource_grant"]
    assert grant["id"] == admitted["reservation_id"]
    assert grant["vector"] == demand.to_six_dict()
    values = {
        "version": 4, "resource_grant": grant,
        "rootdisk_source": {
            "kind": "registry", "image": claim["canonical_request"]["vm_image"],
        },
        "source": "controller_vm_create",
        "admission_id": str(authorization["admission_id"]),
        "reservation_request_id": authorization["request_id"],
        "intent_digest": authorization["intent_digest"],
        "retry_request_id": str(retry["request_id"]),
        "job_id": str(claim["job_id"]),
        "provision_generation": str(claim["provision_generation"]),
        "request_digest": claim["request_digest"],
        "controller_configuration_digest": claim["controller_configuration_digest"],
        "expected_pvc_uid": None, "retained_dv_uid": None,
        "current_dv_uid": None, "current_pvc_uid": None,
        "current_secret_uid": None, "effect_kind": "rootdisk",
        "effect_nonce": str(uuid4()),
        "object_name": "agent-vm-" + str(claim["job_id"]) + "-rootdisk",
    }

    def carrier(intent):
        return seal_creation_carrier(
            intent, namespace="workers", uid=str(uuid4()),
            resource_version="1", secret=b"resource-test-secret-at-least-32-bytes",
        )

    forged = {**values, "resource_grant": {**grant, "id": str(uuid4())}}
    with pytest.raises(VMCreationRetryConflict, match="resource_reservation_changed"):
        await store.begin_effect(
            request_id=str(retry["request_id"]),
            claim_token=str(claim["claim_token"]), carrier=carrier(forged),
        )
    assert await db.fetchval(
        "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
        retry["request_id"],
    ) == 0
    await db.execute(
        "UPDATE vm_resource_inventory_heads SET observation_conflict=TRUE "
        "WHERE cluster_id=$1 AND policy_digest=$2",
        inventory.cluster_id, inventory.policy_digest,
    )
    with pytest.raises(VMCreationRetryConflict, match="inventory_missing"):
        await store.begin_effect(
            request_id=str(retry["request_id"]),
            claim_token=str(claim["claim_token"]), carrier=carrier(values),
        )
    assert await db.fetchval(
        "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
        retry["request_id"],
    ) == 0
    await db.execute(
        "UPDATE vm_resource_inventory_heads SET observation_conflict=FALSE "
        "WHERE cluster_id=$1 AND policy_digest=$2",
        inventory.cluster_id, inventory.policy_digest,
    )
    result = await store.begin_effect(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=carrier(values),
    )
    assert result["actuation_allowed"] is True


@pytest.mark.asyncio
async def test_fresh_controller_inventory_rechecks_selected_node_before_effect(db):
    policy, inventory, snapshot, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    authorization = await VMCreationRetryStore(db).authorize_controller(
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
    assert authorization["resource_grant"]["id"] == admitted["reservation_id"]
    collector = SimpleNamespace(collect=AsyncMock(return_value=snapshot))
    controller = SimpleNamespace(resource_inventory_collector=collector)
    row = {
        "controller_configuration": claim["controller_configuration"],
        "expected_pvc_uid": None,
    }
    grant = authorization["resource_grant"]
    await fresh_resource_effect_node(controller, row, grant)
    for change in ("uid", "ready", "hostname", "architecture", "storage_topology"):
        stale = deepcopy(snapshot)
        if change == "uid":
            stale["nodes"][0]["uid"] = str(uuid4())
        elif change == "ready":
            stale["nodes"][0]["ready"] = False
        elif change == "hostname":
            stale["nodes"][0]["labels"]["kubernetes.io/hostname"] = "other"
        elif change == "architecture":
            stale["nodes"][0]["labels"]["kubernetes.io/arch"] = "arm64"
        else:
            stale["storage_classes"][0]["allowed_topology"] = {
                "nodeSelectorTerms": [{"matchExpressions": [{
                    "key": "kubernetes.io/hostname", "operator": "In",
                    "values": ["other"],
                }]}],
            }
        collector.collect.return_value = stale
        with pytest.raises(ResourceAdmissionError, match="resource_node_changed"):
            await fresh_resource_effect_node(controller, row, grant)


@pytest.mark.asyncio
async def test_ready_binding_uses_exact_pod_and_keeps_overreserve_high_water(db):
    policy, inventory, original, demand = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (str(uuid4()) for _ in range(4))
    await VMCreationRetryStore(db).authorize_controller(
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
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',revision=revision+1,"
        "observed_vm_uid=$2,observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1",
        retry["request_id"], vm_uid, pvc_uid,
    )
    job_id, generation = str(claim["job_id"]), str(claim["provision_generation"])
    vm = {
        "vm_uid": vm_uid, "vmi_uid": vmi_uid,
        "active_pod_uid": launcher_uid, "rootdisk_pvc_uid": pvc_uid,
    }
    node_uid, node_name = admitted["node_uid"], admitted["node_name"]
    sample = deepcopy(original)
    sample["vms"] = [{
        "uid": vm_uid, "name": "agent-vm-" + job_id,
        "owner_kind": "job", "owner_id": job_id,
        "provision_generation": generation, "deleting": False,
    }]
    sample["vmis"] = [{
        "uid": vmi_uid, "name": "agent-vm-" + job_id,
        "vm_uid": vm_uid, "node_uid": node_uid,
        "node_name": node_name, "phase": "Running", "deleting": False,
    }]
    sample["pods"] = [{
        "uid": launcher_uid, "namespace": "workers", "name": "virt-launcher-test",
        "node_uid": node_uid, "node_name": node_name,
        "terminal": False, "deleting": False,
        "requests": demand.to_six_dict(), "vmi_uid": vmi_uid,
        "reservation_id": str(uuid4()),
        "provision_generation": generation,
    }]

    async def publish_sample(document):
        value = deepcopy(document)
        value["snapshot_id"] = str(uuid4())
        value["sequence"] += 1
        value["started_at"] = value["finished_at"] = datetime.now(
            timezone.utc
        ).isoformat()
        await publish(inventory, value)

    async def bind():
        async with db.acquire() as conn, conn.transaction():
            await conn.fetchrow(
                "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", claim["job_id"],
            )
            source = await conn.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
                retry["request_id"],
            )
            installed = await installed_job_resource_store(
                conn, db, source["controller_configuration"], fresh=False,
            )
            return await installed.bind_ready_on_conn(
                conn, retry=source, vm=vm, job_id=job_id, generation=generation,
            )

    await publish_sample(sample)
    assert not await bind()
    assert await db.fetchval(
        "SELECT vm_uid FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) is None
    sample["pods"][0]["reservation_id"] = admitted["reservation_id"]
    await publish_sample(sample)
    assert await bind()
    row = await db.fetchrow(
        "SELECT state,vm_uid,vmi_uid,launcher_uid,observed_cpu_millicores "
        "FROM vm_resource_reservations WHERE id=$1", admitted["reservation_id"],
    )
    assert (row["state"], str(row["vm_uid"]), str(row["vmi_uid"]),
            str(row["launcher_uid"]), row["observed_cpu_millicores"]) == (
        "active", vm_uid, vmi_uid, launcher_uid, demand.cpu_millicores,
    )
    sample["pods"][0]["requests"]["cpu_millicores"] = demand.cpu_millicores + 1
    await publish_sample(sample)
    assert not await bind()
    assert await db.fetchval(
        "SELECT observed_cpu_millicores FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    ) == demand.cpu_millicores + 1
    sample["pods"][0]["requests"]["cpu_millicores"] = demand.cpu_millicores
    await publish_sample(sample)
    assert not await bind()
