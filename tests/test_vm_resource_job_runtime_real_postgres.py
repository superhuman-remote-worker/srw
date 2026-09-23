"""Job resource reservation before the actual creation-retry transport."""

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from orchestrator.services.vm_creation_retry import VMCreationRetryService
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from orchestrator.services.vm_creation_disposition_store import VMCreationDispositionStore
from orchestrator.services.vm_resource_job_runtime import installed_job_resource_store
from orchestrator.services.vm_workspace_recovery_store import VMWorkspaceRecoveryStore
from shared.vm_creation_issuance import seal_creation_carrier
from shared.vm_lifecycle_auth import AUTH_FIELD, sign_payload
from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_effect_node import fresh_resource_effect_node
from shared.worker_queue import claim_worker_batch
from tests.test_vm_resource_whole_store_real_postgres import (
    db as _db_fixture,  # noqa: F401
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
from tests.test_vm_creation_actuation import (
    SECRET as ACTUATION_SECRET,
    setup as _controller_setup,  # noqa: F401
)
from tests.test_vm_workspace_recovery_real_postgres import (
    admission_kwargs,
    recovery_guest_network,
)
from vm_controller import controller as controller_settings
from vm_controller.creation_disposition import CreationDisposer
from shared.vm_creation_disposition import disposition_identity

controller_setup = _controller_setup

@pytest_asyncio.fixture(scope="module")
async def runtime_schema(whole_schema, pg_dsn):  # noqa: F811
    conn = await asyncpg.connect(pg_dsn)
    try:
        if not await conn.fetchval(
            "SELECT to_regclass('public.vm_resource_recovery_successors') IS NOT NULL"
        ):
            migration = (
                Path(__file__).resolve().parents[1]
                / "src/orchestrator/database/migrations/app"
                / "0276_vm_resource_job_runtime.sql"
            )
            await conn.execute(migration.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(runtime_schema, _db_fixture):  # noqa: F811
    # The base fixture truncates jobs; run_queue is an independent substrate
    # and can retain a previous test's unclaimed unit after that truncation.
    await _db_fixture.execute("TRUNCATE run_queue CASCADE")
    yield _db_fixture


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


@pytest.mark.asyncio
async def test_never_issued_cancel_releases_held_reservation_in_same_settlement(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    assert admitted["action"] == "admitted"
    store = VMCreationRetryStore(db)
    assert await db.linearize_pinned_cancel(
        str(retry["job_id"]), expected_status="paused"
    )

    result = await store.settle_never_issued(request_id=str(retry["request_id"]))

    assert result == {"settled": True, "disposition": "never_issued"}
    row = await db.fetchrow(
        "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
        admitted["reservation_id"],
    )
    assert row["state"] == "released"
    assert json.loads(row["release_evidence"])["kind"] == "never_vm_issued"
    assert await db.fetchval(
        "SELECT state FROM vm_resource_waiters WHERE request_id=$1",
        retry["request_id"],
    ) == "released"


@pytest.mark.asyncio
async def test_rejected_partial_effect_disposition_releases_only_after_real_completion(
    db, monkeypatch, controller_setup,
):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
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
    assert authorized["allowed"] is True
    intent = {
        "version": 4, "resource_grant": authorized["resource_grant"],
        "rootdisk_source": {
            "kind": "registry", "image": claim["canonical_request"]["vm_image"],
        },
        "source": "controller_vm_create",
        "admission_id": str(authorized["admission_id"]),
        "reservation_request_id": authorized["request_id"],
        "intent_digest": authorized["intent_digest"],
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
    carrier = seal_creation_carrier(
        intent, namespace="workers", uid=str(uuid4()),
        resource_version="1", secret=ACTUATION_SECRET,
    )
    assert (await store.begin_effect(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]), carrier=carrier,
    ))["actuation_allowed"]
    assert await store.observe_effect(
        request_id=str(retry["request_id"]), carrier=carrier,
        observation={
            "outcome": "rejected",
            "api_status": {
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "reason": "Invalid", "code": 422,
            },
        },
    ) == {"recorded": True, "effect_state": "rejected"}
    assert await db.linearize_pinned_cancel(
        str(retry["job_id"]), expected_status="paused"
    )
    service = VMCreationDispositionStore(store)
    frozen = await service.freeze(request_id=str(retry["request_id"]), carrier=carrier)
    assert frozen["frozen"] is True
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "reserved"
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError, match="never-issued release unproven"):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE vm_resource_reservations SET state='released',"
                    "released_at=clock_timestamp(),release_evidence=$2::jsonb "
                    "WHERE id=$1", UUID(admitted["reservation_id"]),
                    json.dumps({
                        "kind": "never_vm_issued",
                        "request_id": str(retry["request_id"]),
                        "job_id": str(retry["job_id"]),
                        "provision_generation": str(retry["provision_generation"]),
                    }),
                )
    ctrl, api, _, _ = controller_setup
    monkeypatch.setattr(controller_settings, "VM_NAMESPACE", "workers")
    api.objects["Lease", carrier["metadata"]["name"]] = carrier
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"}, "items": [],
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"}, "items": [],
    }

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        return await getattr(store, method)(**body)

    ctrl._workspace_cleanup_authority_request = authority
    outcome = await CreationDisposer(ctrl).run(disposition_identity(retry))
    assert outcome["status"] == "creation_disposed"
    row = await db.fetchrow(
        "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    )
    assert row["state"] == "released"
    assert json.loads(row["release_evidence"])["disposition_id"] == (
        frozen["disposition"]["disposition_id"]
    )


@pytest.mark.asyncio
async def test_idle_charge_requires_persisted_exact_operation_before_teardown(db):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (uuid4() for _ in range(4))
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    authorized = await VMCreationRetryStore(db).authorize_controller(
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
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',revision=revision+1,"
        "observed_vm_uid=$2,observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL WHERE request_id=$1",
        retry["request_id"], vm_uid, pvc_uid,
    )
    await db.execute(
        "UPDATE vm_resource_reservations SET state='active',vm_uid=$2,"
        "vmi_uid=$3,launcher_uid=$4 WHERE id=$1",
        UUID(admitted["reservation_id"]), vm_uid, vmi_uid, launcher_uid,
    )
    fake = {
        "id": uuid4(), "owner_kind": "job", "owner_id": retry["job_id"],
        "provision_generation": retry["provision_generation"],
        "vm_uid": vm_uid, "vmi_uid": vmi_uid,
        "launcher_uid": launcher_uid, "pvc_uid": pvc_uid,
        "phase": "releasing", "stop_evidence": None, "stop_verified_at": None,
    }
    async with db.acquire() as conn:
        with pytest.raises(ResourceAdmissionError, match="resource_idle_operation_changed"):
            async with conn.transaction():
                source = await conn.fetchrow(
                    "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
                    retry["request_id"],
                )
                await policy.mark_idle_teardown_on_conn(
                    conn, retry=source, operation=fake,
                )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "active"
    operation = await db.fetchrow(
        "INSERT INTO vm_idle_operations "
        "(owner_kind,owner_id,phase,episode_id,episode_revision,"
        "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,retained_kind) "
        "VALUES('job',$1,'releasing',$2,1,$3,$4,$5,$6,$7,'rootdisk') RETURNING *",
        retry["job_id"], uuid4(), retry["provision_generation"],
        vm_uid, vmi_uid, launcher_uid, pvc_uid,
    )
    async with db.acquire() as conn, conn.transaction():
        source = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        assert await policy.mark_idle_teardown_on_conn(
            conn, retry=source, operation=operation,
        )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "teardown"
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError, match="physical release proof changed"):
            await conn.execute(
                "UPDATE vm_resource_reservations SET state='released',"
                "released_at=clock_timestamp(),release_evidence=$2::jsonb "
                "WHERE id=$1", UUID(admitted["reservation_id"]),
                json.dumps({
                    "kind": "exact_compute_absent",
                    "operation_id": str(operation["id"]),
                    "job_id": str(retry["job_id"]),
                    "provision_generation": str(retry["provision_generation"]),
                    "vm_uid": str(vm_uid), "vmi_uid": str(vmi_uid),
                    "launcher_uid": str(launcher_uid), "pvc_uid": str(pvc_uid),
                    "stop_evidence_digest": "sha256:" + "0" * 64,
                }),
            )
    async with db.acquire() as conn:
        with pytest.raises(ResourceAdmissionError, match="resource_physical_release_unproven"):
            async with conn.transaction():
                source = await conn.fetchrow(
                    "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
                    retry["request_id"],
                )
                await policy.release_idle_compute_on_conn(
                    conn, retry=source, operation=operation,
                )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    ) == "teardown"
    evidence = {
        "version": 1, "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]),
        "generation": str(retry["provision_generation"]),
        "vm_uid": str(vm_uid), "vmi_uid": str(vmi_uid),
        "launcher_uid": str(launcher_uid), "pvc_uid": str(pvc_uid),
        "vm_absent": True, "vmi_absent": True, "launcher_absent": True,
        "retained_pvc": True, "controller_authenticated": True,
        "same_generation_replacement": False,
    }
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        retry["job_id"], str(retry["provision_generation"]),
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_build_object('vm',$2::jsonb) WHERE id=$1",
        retry["job_id"], json.dumps({
            "status": "suspended", "_suspend_remote_io_closed": str(operation["id"]),
            "provision_generation": str(retry["provision_generation"]),
            "vm_uid": str(vm_uid), "rootdisk_pvc_uid": str(pvc_uid),
        }),
    )
    updated = await db.fetchrow(
        "UPDATE vm_idle_operations SET phase='suspended',"
        "stop_evidence=$2::jsonb,stop_verified_at=clock_timestamp() "
        "WHERE id=$1 RETURNING *", operation["id"], json.dumps(evidence),
    )
    async with db.acquire() as conn, conn.transaction():
        source = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        await conn.fetchrow(
            "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE",
            operation["id"],
        )
        await policy.release_idle_compute_on_conn(
            conn, retry=source, operation=updated,
        )
    final = await db.fetchrow(
        "SELECT state,release_evidence FROM vm_resource_reservations WHERE id=$1",
        UUID(admitted["reservation_id"]),
    )
    assert final["state"] == "released"
    assert json.loads(final["release_evidence"])["operation_id"] == str(operation["id"])


@pytest.mark.asyncio
async def test_retry_service_runs_bounded_real_waiter_maintenance(db, monkeypatch):
    policy, inventory, _, _ = await environment(db)
    retry = await waiter(db, policy, inventory)
    monkeypatch.setenv(
        "VM_RESOURCE_ADMISSION_CONFIG", json.dumps(policy.policy_document),
    )
    await db.execute(
        "UPDATE vm_resource_waiters SET state='parked',reason='worker_lease_active' "
        "WHERE request_id=$1", retry["request_id"],
    )
    before = await db.fetchval(
        "SELECT enqueued_at FROM vm_resource_waiters WHERE request_id=$1",
        retry["request_id"],
    )
    service = VMCreationRetryService(db, SimpleNamespace())
    service.preflight.settle_cancelled = AsyncMock()
    service.preflight.claim_due = AsyncMock(return_value=[])
    service.store.claim_due = AsyncMock(return_value=[])

    await service.reconcile_once()

    row = await db.fetchrow(
        "SELECT state,reason,enqueued_at FROM vm_resource_waiters WHERE request_id=$1",
        retry["request_id"],
    )
    assert (row["state"], row["reason"], row["enqueued_at"]) == (
        "waiting", None, before,
    )


@pytest.mark.asyncio
async def test_enforcement_transition_refuses_unclassified_live_vm(db):
    from orchestrator.services.vm_resource_policy_lifecycle_store import (
        VMResourcePolicyLifecycleStore,
    )
    from shared.vm_resource_policy import validate_enforcement_resource_policy

    store, inventory, original, _ = await environment(db)
    await db.execute(
        "UPDATE vm_resource_admission_policy SET mode='shadow' WHERE cluster_id=$1",
        inventory.cluster_id,
    )
    lifecycle = VMResourcePolicyLifecycleStore(
        db, snapshot=validate_enforcement_resource_policy(store.policy_document),
    )
    current = await db.fetchrow(
        "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1",
        inventory.cluster_id,
    )
    shadow = lifecycle._receipt(current)
    marked = deepcopy(original)
    marked["vms"] = [{
        "uid": str(uuid4()), "name": "agent-vm-legacy",
        "owner_kind": "job", "owner_id": str(uuid4()),
        "provision_generation": str(uuid4()), "deleting": False,
    }]
    marked["snapshot_id"] = str(uuid4())
    marked["sequence"] += 1
    marked["started_at"] = marked["finished_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    await publish(inventory, marked)

    with pytest.raises(ResourceAdmissionError, match="legacy_occupancy_unclassified"):
        await lifecycle.activate_enforce(expected=shadow)
    assert await db.fetchval(
        "SELECT mode FROM vm_resource_admission_policy WHERE cluster_id=$1",
        inventory.cluster_id,
    ) == "shadow"


@pytest.mark.asyncio
async def test_enforcement_transition_accepts_fresh_empty_srw_inventory(db):
    from orchestrator.services.vm_resource_policy_lifecycle_store import (
        VMResourcePolicyLifecycleStore,
    )
    from shared.vm_resource_policy import validate_enforcement_resource_policy

    store, inventory, _, _ = await environment(db)
    await db.execute(
        "UPDATE vm_resource_admission_policy SET mode='shadow' WHERE cluster_id=$1",
        inventory.cluster_id,
    )
    lifecycle = VMResourcePolicyLifecycleStore(
        db, snapshot=validate_enforcement_resource_policy(store.policy_document),
    )
    shadow = lifecycle._receipt(await db.fetchrow(
        "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1",
        inventory.cluster_id,
    ))
    enforced = await lifecycle.activate_enforce(expected=shadow)
    assert (enforced.mode, enforced.revision) == ("enforce", shadow.revision + 1)


@pytest.mark.asyncio
async def test_drain_cannot_finish_off_until_held_charge_is_released(db):
    from orchestrator.services.vm_resource_policy_lifecycle_store import (
        VMResourcePolicyLifecycleStore,
    )
    from shared.vm_resource_policy import validate_enforcement_resource_policy

    store, inventory, _, _ = await environment(db)
    lifecycle = VMResourcePolicyLifecycleStore(
        db, snapshot=validate_enforcement_resource_policy(store.policy_document),
    )
    enforce = lifecycle._receipt(await db.fetchrow(
        "SELECT * FROM vm_resource_admission_policy WHERE cluster_id=$1",
        inventory.cluster_id,
    ))
    retry = await waiter(db, store, inventory)
    await store.admit(request_id=str(retry["request_id"]))
    drained = await lifecycle.begin_drain(expected=enforce)
    with pytest.raises(ResourceAdmissionError, match="resource_charge_unresolved"):
        await lifecycle.finalize_off(expected=drained)
    assert await db.linearize_pinned_cancel(
        str(retry["job_id"]), expected_status="paused"
    )
    await VMCreationRetryStore(db).settle_never_issued(
        request_id=str(retry["request_id"]),
    )
    off = await lifecycle.finalize_off(expected=drained)
    assert (off.mode, off.revision) == ("off", drained.revision + 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("teardown_before_release", [False, True])
async def test_genuine_recovery_release_appends_exact_charged_successor(
    db, teardown_before_release,
):
    from shared.workspace_recovery import WorkspaceRecoveryCode

    policy, inventory, original, demand = await environment(
        db, installation_count=2, owner_count=2,
    )
    retry = await waiter(db, policy, inventory, lane="stateless")
    admitted = await policy.admit(request_id=str(retry["request_id"]))
    job_id, generation = retry["job_id"], retry["provision_generation"]
    vm_uid, vmi_uid, launcher_uid, pvc_uid = (uuid4() for _ in range(4))
    successor_vmi, successor_launcher = uuid4(), uuid4()
    claim = (await VMCreationRetryStore(db).claim_due(limit=1))[0]
    authorized = await VMCreationRetryStore(db).authorize_controller(
        request_id=str(retry["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(job_id),
            "provision_generation": str(generation),
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    assert authorized["allowed"]
    await db.execute(
        "UPDATE vm_creation_retries SET state='succeeded',revision=revision+1,"
        "observed_vm_uid=$2,observed_pvc_uid=$3,resolved_at=clock_timestamp(),"
        "claim_token=NULL,claim_expires_at=NULL "
        "WHERE request_id=$1",
        retry["request_id"], vm_uid, pvc_uid,
    )
    # The predecessor creation's adoption has settled before a worker may
    # report recovery. Its full effect receipts are tested by the creation
    # suite; this fixture starts at the already-created runtime boundary.
    await db.execute(
        "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
        "outcome='adopted' WHERE id=$1",
        authorized["admission_id"],
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_build_object('vm',$2::jsonb) "
        "WHERE id=$1",
        job_id,
        json.dumps({
            "provision_generation": str(generation), "vm_uid": str(vm_uid),
            "vmi_uid": str(vmi_uid), "active_pod_uid": str(launcher_uid),
            "rootdisk_pvc_uid": str(pvc_uid), "status": "ready",
        }),
    )
    sample = deepcopy(original)
    sample["vms"] = [{
        "uid": str(vm_uid), "name": "agent-vm-" + str(job_id),
        "owner_kind": "job", "owner_id": str(job_id),
        "provision_generation": str(generation), "deleting": False,
    }]
    sample["vmis"] = [{
        "uid": str(vmi_uid), "name": "agent-vm-" + str(job_id),
        "vm_uid": str(vm_uid), "node_uid": admitted["node_uid"],
        "node_name": admitted["node_name"], "phase": "Running",
        "deleting": False,
    }]
    sample["pods"] = [{
        "uid": str(launcher_uid), "namespace": "workers", "name": "virt-launcher-old",
        "node_uid": admitted["node_uid"], "node_name": admitted["node_name"],
        "terminal": False, "deleting": False,
        "requests": demand.to_six_dict(), "vmi_uid": str(vmi_uid),
        "reservation_id": admitted["reservation_id"],
        "provision_generation": str(generation),
    }]
    sample["snapshot_id"] = str(uuid4())
    sample["sequence"] += 1
    sample["started_at"] = sample["finished_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    await publish(inventory, sample)
    async with db.acquire() as conn, conn.transaction():
        await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job_id)
        source = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        assert await policy.bind_ready_on_conn(
            conn, retry=source,
            vm={
                "vm_uid": str(vm_uid), "vmi_uid": str(vmi_uid),
                "active_pod_uid": str(launcher_uid),
                "rootdisk_pvc_uid": str(pvc_uid),
            },
            job_id=str(job_id), generation=str(generation),
        )
    assert await db.queue_stateless_job_for_resume(
        str(job_id), expected_status="paused", void_completion_decision=False,
    )
    worker = await claim_worker_batch(db, pod_name="worker-a")
    assert worker is not None and worker.unit_id == job_id
    values = admission_kwargs(job_id, worker.lease_token) | {
        "provision_generation": generation, "vm_uid": vm_uid,
        "prior_vmi_uid": vmi_uid, "prior_launcher_uid": launcher_uid,
        "root_pvc_uid": pvc_uid,
        "code": WorkspaceRecoveryCode.RUNTIME_NOT_READY,
    }
    recovery = VMWorkspaceRecoveryStore(db, worker_id="resource-test")
    admitted_recovery = await recovery.admit_hold(**values)
    claim = await recovery.claim_due(admitted_recovery.operation_id)
    assert claim is not None
    observed_at = datetime.now(timezone.utc).isoformat()
    stop = {
        "protocol_version": 1, "vm_uid": str(vm_uid),
        "vmi_uid": str(vmi_uid), "launcher_uid": str(launcher_uid),
        "container_id": "containerd://old-compute", "root_pvc_uid": str(pvc_uid),
        "controller_identity": "controller/pod-1", "observed_at": observed_at,
        "containers": [{
            "name": "compute", "kind": "regular",
            "container_id": "containerd://old-compute",
            "terminated_container_id": "containerd://old-compute",
            "restart_count": 0, "state": "terminated", "last_state": None,
            "finished_at": observed_at, "reason": "Completed",
        }],
        "declared_containers": {"regular": ["compute"], "init": []},
        "pod_terminal": {"phase": "Succeeded", "restart_policy": "Never"},
    }
    stop_digest = await recovery.accept_stop_evidence(claim, stop)
    assert stop_digest is not None
    observation = {
        "ready": True, "authenticated": True, "ambiguous": False,
        "owner_kind": "job", "owner_id": str(job_id),
        "provision_generation": str(generation), "vm_uid": str(vm_uid),
        "root_pvc_uid": str(pvc_uid), "prior_runtime": "stopped",
        "stop_receipt_digest": stop_digest,
        "remote_operations": "settled", "continuation": "safe",
        "successor": {
            "vmi_uid": str(successor_vmi), "launcher_uid": str(successor_launcher),
            "node_uid": "node-8", "pod_ip": "10.42.0.90",
            "ssh_registration_id": "54" * 16,
            "guest_boot_id": "00000000-0000-4000-8000-000000000041",
            "guest_machine_id": "41" * 16,
            "interface_mac": "02:00:00:00:00:41",
            "guest_network": recovery_guest_network(),
        },
    }
    staged = await recovery.stage_observation(
        operation_id=claim.operation_id, version=claim.version,
        claim_token=claim.claim_token, phase="attesting", observation=observation,
    )
    assert staged is not None
    if teardown_before_release:
        await db.execute(
            "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
            UUID(admitted["reservation_id"]),
        )
        assert not await recovery.release_recovered(
            operation_id=staged.operation_id, version=staged.version,
            claim_token=staged.claim_token,
            initial_observation=observation,
            final_observation=observation.copy(),
            resume_receipt={"kind": "workspace_recovery"},
        )
        assert await db.fetchval(
            "SELECT count(*) FROM vm_resource_recovery_successors "
            "WHERE recovery_id=$1", staged.operation_id,
        ) == 0
        assert await db.fetchval(
            "SELECT resolved_at FROM vm_workspace_recoveries WHERE id=$1",
            staged.operation_id,
        ) is None
        queue = await db.fetchrow(
            "SELECT state,park_reason FROM run_queue WHERE unit_id=$1", job_id,
        )
        assert tuple(queue) == ("parked", "workspace_recovery")
        return
    assert await recovery.release_recovered(
        operation_id=staged.operation_id, version=staged.version,
        claim_token=staged.claim_token,
        initial_observation=observation, final_observation=observation.copy(),
        resume_receipt={"kind": "workspace_recovery"},
    )
    receipt = await db.fetchrow(
        "SELECT * FROM vm_resource_recovery_successors WHERE recovery_id=$1",
        staged.operation_id,
    )
    assert receipt is not None
    assert (receipt["reservation_id"], receipt["ordinal"],
            receipt["prior_vmi_uid"], receipt["successor_vmi_uid"],
            receipt["prior_launcher_uid"], receipt["successor_launcher_uid"]) == (
        UUID(admitted["reservation_id"]),
        1, vmi_uid, successor_vmi, launcher_uid, successor_launcher,
    )
    assert await db.fetchval(
        "SELECT state FROM vm_resource_reservations WHERE id=$1",
        receipt["reservation_id"],
    ) == "active"
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError, match="append-only"):
            await conn.execute(
                "UPDATE vm_resource_recovery_successors SET successor_vmi_uid=$2 "
                "WHERE recovery_id=$1", staged.operation_id, uuid4(),
            )
        with pytest.raises(asyncpg.CheckViolationError, match="idle release missing"):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
                    receipt["reservation_id"],
                )
                await conn.execute(
                    "UPDATE vm_resource_reservations SET state='released',"
                    "released_at=clock_timestamp(),release_evidence=$2::jsonb "
                    "WHERE id=$1", receipt["reservation_id"],
                    json.dumps({"kind": "exact_compute_absent"}),
                )
    successor_sample = deepcopy(sample)
    successor_sample["vmis"][0]["uid"] = str(successor_vmi)
    successor_sample["pods"][0]["uid"] = str(successor_launcher)
    successor_sample["pods"][0]["vmi_uid"] = str(successor_vmi)
    successor_sample["snapshot_id"] = str(uuid4())
    successor_sample["sequence"] += 1
    successor_sample["started_at"] = successor_sample["finished_at"] = (
        datetime.now(timezone.utc).isoformat()
    )
    await publish(inventory, successor_sample)
    async with db.acquire() as conn, conn.transaction():
        await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job_id)
        source = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        assert await policy.bind_ready_on_conn(
            conn, retry=source,
            vm={
                "vm_uid": str(vm_uid), "vmi_uid": str(successor_vmi),
                "active_pod_uid": str(successor_launcher),
                "rootdisk_pvc_uid": str(pvc_uid),
            },
            job_id=str(job_id), generation=str(generation),
        )
    next_retry = await waiter(db, policy, inventory)
    next_admission = await policy.admit(request_id=str(next_retry["request_id"]))
    assert next_admission["action"] == "admitted", next_admission
    second_worker = await claim_worker_batch(db, pod_name="worker-a")
    assert second_worker is not None and second_worker.unit_id == job_id
    second_values = admission_kwargs(job_id, second_worker.lease_token) | {
        "provision_generation": generation, "vm_uid": vm_uid,
        "prior_vmi_uid": successor_vmi,
        "prior_launcher_uid": successor_launcher,
        "root_pvc_uid": pvc_uid,
        "code": WorkspaceRecoveryCode.RUNTIME_NOT_READY,
    }
    second = await recovery.admit_hold(**second_values)
    second_claim = await recovery.claim_due(second.operation_id)
    assert second_claim is not None
    second_stop = {
        **stop,
        "vmi_uid": str(successor_vmi),
        "launcher_uid": str(successor_launcher),
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    second_digest = await recovery.accept_stop_evidence(
        second_claim, second_stop,
    )
    assert second_digest is not None
    second_vmi, second_launcher = uuid4(), uuid4()
    second_observation = {
        **observation,
        "stop_receipt_digest": second_digest,
        "successor": {
            **observation["successor"],
            "vmi_uid": str(second_vmi),
            "launcher_uid": str(second_launcher),
        },
    }
    second_stage = await recovery.stage_observation(
        operation_id=second_claim.operation_id, version=second_claim.version,
        claim_token=second_claim.claim_token, phase="attesting",
        observation=second_observation,
    )
    assert second_stage is not None
    assert await recovery.release_recovered(
        operation_id=second_stage.operation_id, version=second_stage.version,
        claim_token=second_stage.claim_token,
        initial_observation=second_observation,
        final_observation=second_observation.copy(),
        resume_receipt={"kind": "workspace_recovery"},
    )
    lineage = await db.fetch(
        "SELECT ordinal,prior_vmi_uid,successor_vmi_uid,prior_launcher_uid,"
        "successor_launcher_uid FROM vm_resource_recovery_successors "
        "WHERE reservation_id=$1 ORDER BY ordinal", receipt["reservation_id"],
    )
    assert [tuple(row) for row in lineage] == [
        (1, vmi_uid, successor_vmi, launcher_uid, successor_launcher),
        (2, successor_vmi, second_vmi, successor_launcher, second_launcher),
    ]
