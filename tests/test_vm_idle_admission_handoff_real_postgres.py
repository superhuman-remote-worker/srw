"""Default idle composition must reach durable creation, then real admission."""

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
from orchestrator.services.vm_provisioner import VMProvisioner
from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest
from tests.test_vm_resource_job_runtime_real_postgres import (
    charged_idle_wait,
    db as _runtime_db,
    runtime_schema,  # noqa: F401
    _db_fixture,  # noqa: F401
    whole_schema,  # noqa: F401
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
)


db = _runtime_db


async def suspended_charged_job(db, monkeypatch):
    """Seed prior physical evidence; exercise native idle/resource settlement."""
    policy, retry, admitted, episode, identity = await charged_idle_wait(
        db, monkeypatch
    )
    monkeypatch.setenv(
        "VM_RESOURCE_ADMISSION_CONFIG", json.dumps(policy.policy_document)
    )
    store = VMIdleLifecycleStore(db)
    operation = await store.admit_release(
        str(retry["job_id"]),
        episode_id=episode.episode_id,
        revision=episode.revision,
        identity=identity,
    )
    assert operation is not None
    await db.execute(
        "INSERT INTO managed_repository_process_zero_receipts "
        "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
        "VALUES('job',$1,'vm','vm',$2)",
        retry["job_id"],
        identity["generation"],
    )
    evidence = {
        "version": 1,
        "kind": "vm_idle_physical_stop",
        "operation_id": str(operation["id"]),
        **identity,
        "vm_absent": True,
        "vmi_absent": True,
        "launcher_absent": True,
        "retained_pvc": True,
        "controller_authenticated": True,
        "same_generation_replacement": False,
    }
    assert await store.complete_release(str(operation["id"]), evidence=evidence)
    intent = {
        "owner_kind": "job",
        "owner_id": str(retry["job_id"]),
        "provision_generation": identity["generation"],
        "vm_uid": identity["vm_uid"],
        "pvc_uid": identity["pvc_uid"],
        "purge_disk": False,
        "resource": "vm_workspace",
        "source": "vm_idle_release",
    }
    await db.execute(
        "INSERT INTO vm_workspace_cleanup_admissions "
        "(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) "
        "VALUES($1,'job',$2,$3,'vm_idle_release',$4,$5,clock_timestamp(),'completed')",
        uuid4(),
        retry["job_id"],
        UUID(identity["pvc_uid"]),
        uuid4(),
        cleanup_intent_digest(intent),
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(admitted["reservation_id"]),
        )
        == "released"
    )
    wake = await store.request_wake(str(retry["job_id"]), execution_requested=False)
    assert wake is not None
    return policy, retry, operation, wake, identity


@pytest.mark.asyncio
async def test_default_application_idle_wake_reaches_native_preflight_under_enforcement(
    db,
    monkeypatch,
):
    _, retry, operation, wake, identity = await suspended_charged_job(db, monkeypatch)
    service = application_service(db, monkeypatch)

    advanced = await service.reconcile_once()
    observed = await service.store.get_operation(str(operation["id"]))
    assert advanced == 1, (observed["phase"], observed["reason"])

    row = await db.fetchrow(
        "SELECT status,context FROM jobs WHERE id=$1", retry["job_id"]
    )
    context = json.loads(row["context"])
    assert row["status"] == "waiting_for_reply"
    assert context["vm"]["creation_preflight"]["request_id"] == str(
        wake["wake_request_id"]
    )
    assert context["vm"]["provision_generation"] == str(wake["wake_generation"])
    assert (
        context["vm"]["creation_preflight"]["expected_pvc_uid"] == identity["pvc_uid"]
    )
    assert context["last_vm"]["provision_generation"] == identity["generation"]
    assert (await service.store.get_operation(str(operation["id"])))[
        "wake_execution_requested"
    ] is False
    assert await db.fetchval("SELECT count(*) FROM vm_creation_effects") == 0
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE state<>'released'"
        )
        == 0
    )


def application_service(db, monkeypatch):
    from orchestrator.application import workspace as composition

    provisioner = VMProvisioner()
    provisioner._db = db
    monkeypatch.setattr(
        composition.vm_provisioner_module, "vm_provisioner", provisioner
    )
    # This Job case never calls the unrelated thread retirement collaborator.
    monkeypatch.setattr(
        composition.controls_composition, "thread_retirement_operations", lambda _: None
    )
    return composition.vm_idle_service(SimpleNamespace(postgres_db=db))


async def resolve_wake(db, monkeypatch, policy, wake, identity):
    """Real read-only configuration resolver, SQL source handoff, and inventory."""
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from tests.test_vm_resource_inventory_real_postgres import publish, successor
    from tests.test_vm_resource_template import shipped_template
    from vm_controller import controller as settings
    from vm_controller.creation_configuration import resolve_creation_configuration

    for key, value in {
        "VM_NAMESPACE": "workers",
        "VM_STORAGE_CLASS": "local",
        "VM_NODE_SELECTOR": {},
        "VM_TOLERATIONS": [],
        "VM_PERSISTENT_ROOTDISK": True,
    }.items():
        monkeypatch.setattr(settings, key, value)
    controller = SimpleNamespace(
        template_text=shipped_template(),
        cloud_init_text="#cloud-config",
        headscale=SimpleNamespace(is_available=False),
    )
    preflight = VMCreationPreflightStore(db)
    claim = (await preflight.claim_due(limit=1))[0]
    assert claim["request_id"] == str(wake["wake_request_id"])
    resolved = resolve_creation_configuration(controller, claim["request"])
    assert resolved["controller_configuration"]["version"] == 3
    row = await preflight.complete_resolution(claim, resolved)
    assert row["request_id"] == wake["wake_request_id"]
    sample = successor((await policy.inventory.current())["snapshot"])
    sample["started_at"] = sample["finished_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    pv_uid = str(uuid4())
    sample["pvcs"] = [
        {
            "uid": identity["pvc_uid"],
            "name": "retained-root",
            "pv_uid": pv_uid,
            "pv_name": "retained-pv",
            "storage_class_uid": sample["storage_classes"][0]["uid"],
            "phase": "Bound",
        }
    ]
    sample["pvs"] = [
        {
            "uid": pv_uid,
            "name": "retained-pv",
            "claim_uid": identity["pvc_uid"],
            "required_affinity": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {
                                "key": "kubernetes.io/hostname",
                                "operator": "In",
                                "values": ["node-a"],
                            }
                        ]
                    }
                ]
            },
        }
    ]
    await publish(policy.inventory, sample)
    return row


@pytest.mark.asyncio
@pytest.mark.parametrize("occupied", [False, True])
async def test_native_wake_retry_uses_capacity_before_controller_request(
    db,
    monkeypatch,
    occupied,
):
    from orchestrator.services.vm_creation_retry import VMCreationRetryService
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
    from tests.test_vm_resource_job_runtime_real_postgres import (
        SignedPendingCreate,
        UnexpectedCreate,
    )
    from tests.test_vm_resource_whole_store_real_postgres import waiter

    policy, retry, _, wake, identity = await suspended_charged_job(db, monkeypatch)
    if occupied:
        other = await waiter(db, policy, policy.inventory, user_id=uuid4())
        assert (await policy.admit(request_id=str(other["request_id"])))[
            "action"
        ] == "admitted"
    assert await application_service(db, monkeypatch).reconcile_once() == 1
    source = await resolve_wake(db, monkeypatch, policy, wake, identity)
    store = VMCreationRetryStore(db)
    claims = await store.claim_due(limit=10)
    claim = next(row for row in claims if row["request_id"] == source["request_id"])
    observed = {
        "job_id": str(retry["job_id"]),
        "provision_generation": str(wake["wake_generation"]),
        "request_digest": claim["request_digest"],
        "controller_configuration_digest": claim["controller_configuration_digest"],
        "expected_pvc_uid": identity["pvc_uid"],
    }
    # Even a caller skipping retry cannot authorize physical creation yet.
    assert await store.authorize_controller(
        request_id=str(source["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed=observed,
    ) == {"allowed": False, "reason": "resource_reservation_missing"}
    client = (
        UnexpectedCreate()
        if occupied
        else SignedPendingCreate(db, source["request_id"])
    )
    service = VMCreationRetryService(
        db,
        SimpleNamespace(_http_client=client, _lifecycle_hmac_secret=b"test-key"),
    )
    await service._replay(claim)
    assert client.calls == (0 if occupied else 1)
    current = await db.fetchrow(
        "SELECT state,reason FROM vm_creation_retries WHERE request_id=$1",
        source["request_id"],
    )
    assert (current["state"], current["reason"]) == (
        "queued",
        "resource_wait" if occupied else "creation_observation_pending",
    )
    reservations = await db.fetch(
        "SELECT * FROM vm_resource_reservations WHERE request_id=$1",
        source["request_id"],
    )
    assert len(reservations) == (0 if occupied else 1)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    if not occupied:
        results = await asyncio.gather(
            *(policy.admit(request_id=str(source["request_id"])) for _ in range(2))
        )
        assert results[0] == results[1]
        assert results[0]["reservation_id"] == str(reservations[0]["id"])
    assert (
        await db.fetchval("SELECT status FROM jobs WHERE id=$1", retry["job_id"])
        == "waiting_for_reply"
    )
    assert (
        await db.fetchval(
            "SELECT state FROM run_queue WHERE unit_id=$1", retry["job_id"]
        )
        == "done"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_default_wake_restart_and_concurrency_preserve_one_source_deadline_and_pvc(
    db,
    monkeypatch,
    lost_reply,
):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore

    policy, retry, operation, wake, identity = await suspended_charged_job(
        db, monkeypatch
    )
    predecessor = json.loads(
        await db.fetchval(
            "SELECT context->'vm'->'creation_preflight' FROM jobs WHERE id=$1",
            retry["job_id"],
        )
    )
    service = application_service(db, monkeypatch)
    begin = VMCreationPreflightStore.begin
    if lost_reply:

        async def lost(self, *args, **kwargs):
            await begin(self, *args, **kwargs)
            raise RuntimeError("response lost after preflight commit")

        monkeypatch.setattr(VMCreationPreflightStore, "begin", lost)
        assert await service.reconcile_once() == 0
        monkeypatch.setattr(VMCreationPreflightStore, "begin", begin)
    else:
        results = await asyncio.gather(
            service.reconcile_once(),
            application_service(db, monkeypatch).reconcile_once(),
        )
        assert sum(results) == 1
    reconstructed = application_service(db, monkeypatch)
    assert await reconstructed.reconcile_once() == 0
    result = await reconstructed.provisioner.create_vm(
        str(retry["job_id"]),
        idle_wake_id=str(operation["id"]),
    )
    assert result["request_id"] == str(wake["wake_request_id"])
    source = await resolve_wake(db, monkeypatch, policy, wake, identity)
    assert source["admission_deadline"].isoformat() == predecessor["admission_deadline"]
    assert str(source["expected_pvc_uid"]) == identity["pvc_uid"]
    for key, value in predecessor["request"].items():
        assert source["canonical_request"][key] == (
            str(wake["wake_generation"]) if key == "provision_generation" else value
        )
    # The old synthetic fixture did not resolve an explicit disk size; the
    # native resolver may freeze that default without changing supplied options.
    assert source["canonical_request"]["disk_size"]
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE job_id=$1",
            retry["job_id"],
        )
        == 2
    )  # One immutable predecessor and exactly one successor.
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_waiters WHERE request_id=$1",
            source["request_id"],
        )
        == 1
    )
    assert await db.fetchval("SELECT count(*) FROM vm_creation_effects") == 0
    assert (
        await db.fetchval("SELECT status FROM jobs WHERE id=$1", retry["job_id"])
        == "waiting_for_reply"
    )
    assert (
        await db.fetchval(
            "SELECT state FROM run_queue WHERE unit_id=$1", retry["job_id"]
        )
        == "done"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["stop", "pvc", "cancelled", "expired", "protocol"])
async def test_default_wake_preserves_native_authority_refusals(
    db, monkeypatch, changed
):
    _, retry, operation, wake, _ = await suspended_charged_job(db, monkeypatch)
    if changed == "stop":
        await db.execute(
            "UPDATE vm_idle_operations SET stop_verified_at=NULL,stop_evidence=NULL WHERE id=$1",
            operation["id"],
        )
    elif changed == "pvc":
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,rootdisk_pvc_uid}',to_jsonb($2::text)) WHERE id=$1",
            retry["job_id"],
            str(uuid4()),
        )
    elif changed == "cancelled":
        await db.execute(
            "UPDATE jobs SET status='cancelled' WHERE id=$1", retry["job_id"]
        )
    elif changed == "expired":
        # Rewrite the manifest and projection, but preserve the immutable source
        # deadline. The earlier source-binding guard must reject this forgery.
        deadline = await db.fetchval(
            "UPDATE srw_execution_specs SET created_at=clock_timestamp()-interval '2 hours' "
            "WHERE work_kind='Job' AND work_id=$1 "
            "RETURNING created_at+interval '1 hour'",
            retry["job_id"],
        )
        await db.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,creation_preflight,admission_deadline}',to_jsonb($2::text)) WHERE id=$1",
            retry["job_id"],
            deadline.isoformat(),
        )
    else:
        monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "false")
    service = application_service(db, monkeypatch)
    if changed in {"cancelled", "expired", "protocol"}:
        from orchestrator.services.vm_creation_retry_store import (
            VMCreationRetryConflict,
        )

        with pytest.raises(
            VMCreationRetryConflict,
            match="idle_wake_predecessor_unproven"
            if changed == "expired"
            else "idle_wake_unproven",
        ):
            await service._wake(
                await service.store.get_operation(str(operation["id"])),
                current=lambda: True,
            )
    else:
        assert (
            await service._wake(
                await service.store.get_operation(str(operation["id"])),
                current=lambda: True,
            )
            is False
        )
    context = json.loads(
        await db.fetchval("SELECT context FROM jobs WHERE id=$1", retry["job_id"])
    )
    assert context["vm"]["provision_generation"] != str(wake["wake_generation"])
    assert await db.fetchval("SELECT count(*) FROM vm_creation_effects") == 0
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE state<>'released'"
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("requesting_access", [False, True])
async def test_native_wake_exact_grant_issues_one_effect_and_replay_only_observes(
    db, monkeypatch, requesting_access
):
    from orchestrator.services.vm_creation_retry_store import (
        VMCreationRetryConflict,
        VMCreationRetryStore,
    )
    from shared.vm_creation_issuance import seal_creation_carrier

    secret = b"idle-wake-resource-secret-at-least-32-bytes"
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", secret.decode())
    policy, retry, _, wake, identity = await suspended_charged_job(db, monkeypatch)
    if requesting_access:
        from orchestrator.services.vm_idle_access import VMIdleAccessStore

        leases = await asyncio.gather(
            *(
                VMIdleAccessStore(db).request(
                    owner_kind="job",
                    owner_id=str(retry["job_id"]),
                    kind="ide",
                    user_id=str(uuid4()),
                    connection_id=str(uuid4()),
                )
                for _ in range(2)
            )
        )
        assert all(leases)
    assert await application_service(db, monkeypatch).reconcile_once() == 1
    source = await resolve_wake(db, monkeypatch, policy, wake, identity)
    admitted = await policy.admit(request_id=str(source["request_id"]))
    assert admitted["action"] == "admitted", admitted
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    grant = await store.authorize_controller(
        request_id=str(source["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(retry["job_id"]),
            "provision_generation": str(wake["wake_generation"]),
            "request_digest": claim["request_digest"],
            "controller_configuration_digest": claim["controller_configuration_digest"],
            "expected_pvc_uid": identity["pvc_uid"],
        },
    )
    assert grant["allowed"] is True
    assert grant["resource_grant"]["id"] == admitted["reservation_id"]
    dv_uid = str(uuid4())
    values = {
        "version": 4,
        "resource_grant": grant["resource_grant"],
        "rootdisk_source": {"kind": "retained", "pvc_uid": identity["pvc_uid"]},
        "source": "controller_vm_create",
        "admission_id": str(grant["admission_id"]),
        "reservation_request_id": grant["request_id"],
        "intent_digest": grant["intent_digest"],
        "retry_request_id": str(source["request_id"]),
        "job_id": str(retry["job_id"]),
        "provision_generation": str(wake["wake_generation"]),
        "request_digest": claim["request_digest"],
        "controller_configuration_digest": claim["controller_configuration_digest"],
        "expected_pvc_uid": identity["pvc_uid"],
        "retained_dv_uid": dv_uid,
        "current_dv_uid": dv_uid,
        "current_pvc_uid": identity["pvc_uid"],
        "current_secret_uid": None,
        "effect_kind": "rootdisk",
        "effect_nonce": str(uuid4()),
        "object_name": f"agent-vm-{retry['job_id']}-rootdisk",
    }
    carrier_uid = str(uuid4())

    def seal(intent):
        return seal_creation_carrier(
            intent,
            namespace="workers",
            uid=carrier_uid,
            resource_version="1",
            secret=secret,
        )

    with pytest.raises(VMCreationRetryConflict, match="resource_reservation_changed"):
        await store.begin_effect(
            request_id=str(source["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=seal(
                {
                    **values,
                    "resource_grant": {**grant["resource_grant"], "id": str(uuid4())},
                }
            ),
        )
    results = await asyncio.gather(
        *(
            store.begin_effect(
                request_id=str(source["request_id"]),
                claim_token=str(claim["claim_token"]),
                carrier=seal(values),
            )
            for _ in range(2)
        )
    )
    assert sorted(row["actuation_allowed"] for row in results) == [False, True]
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
            source["request_id"],
        )
        == 1
    )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == 1
    )
