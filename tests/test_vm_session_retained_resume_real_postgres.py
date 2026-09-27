"""Ordinary Session Resume must carry retained-disk and current-actor authority.

The predecessor uses the native charged source/adoption/Ready/End fixtures. Only
the physical stop observation and controller configuration transport are modeled;
Resume, protected actor binding, workspace dispatch and source insertion are real.
"""

import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.vm_provisioner import VMProvisioner
from orchestrator.services.workspace_suspension import WorkspaceSuspensionService
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_protected_agent
from tests.test_vm_creation_actuation import setup as _controller_setup
from tests.test_vm_thread_retained_disk_purge_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    db as _db,
    pg_dsn as _pg_dsn,
    operations,
    permanent_begin,
    predecessor_snapshot,
    settled,
    thread_schema,  # noqa: F401
)

db = _db
pg_dsn = _pg_dsn
controller_setup = _controller_setup


async def assert_predecessor_unchanged(db, case):
    after = await predecessor_snapshot(db, case)
    for table, rows in case["history"].items():
        assert all(row in after[table] for row in rows), table


async def retained_resume(
    db, monkeypatch, *, ready, marker, bind, observed_before_ready=False
):
    if observed_before_ready:
        from tests import test_vm_resource_thread_cleanup_real_postgres as first_end

        adopt = first_end._adopted_charged_thread

        async def adopt_with_observation(store, patch):
            result = await adopt(store, patch)
            assert await store.merge_thread_vm_context_if_provision_generation(
                str(result[4]),
                str(result[6]),
                {"vmi_uid": str(uuid4()), "active_pod_uid": str(uuid4())},
                require_status_not_ready=True,
            )
            return result

        monkeypatch.setattr(
            first_end, "_adopted_charged_thread", adopt_with_observation
        )
    case, physical = await settled(db, monkeypatch, ready=ready)
    case["history"] = await predecessor_snapshot(db, case)
    thread_id = str(case["thread_id"])
    old = await db.get_thread(thread_id)
    old_vm = json.loads(old["metadata"])["vm"]
    assert old_vm.get("rootdisk") is None
    assert old_vm["rootdisk_pvc_uid"] == case["pvc_uid"]
    if observed_before_ready:
        assert old_vm["vmi_uid"] is not None and old_vm["active_pod_uid"] is not None
        charge = await db.fetchrow(
            "SELECT vm_uid,vmi_uid,launcher_uid FROM vm_resource_reservations WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
        )
        assert dict(charge) == {"vm_uid": None, "vmi_uid": None, "launcher_uid": None}
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(case["admitted"]["reservation_id"]),
        )
        == "released"
    )
    assert await db.fetchval("SELECT count(*) FROM vm_idle_operations") == 0
    # Use the real owner reopen transaction: its trigger rotates G and clears
    # attach. No old source actor or receipt is rewritten for the new actor.
    assert await db.resume_thread(thread_id)
    current = await db.get_thread(thread_id)
    assert current["status"] == "created"
    assert current["runtime_generation"] != old["runtime_generation"]
    assert current["agent_id"] is None
    assert current["runtime_attach_token"] is None
    if bind:
        current = await _bind_protected_agent(db, case["thread_id"])
        assert current["agent_id"] is not None
        assert current["runtime_attach_token"] is not None
    if marker:
        # Deliberately add only the presentation marker to expose the next
        # boundary; this is diagnostic input, never claimed as disk authority.
        await db.merge_thread_vm_context(thread_id, {"rootdisk": "kept"})
    source = await db.fetchrow(
        "SELECT controller_configuration FROM vm_creation_retries WHERE request_id=$1",
        case["request_id"],
    )
    configuration = json.loads(source["controller_configuration"])
    policy = await db.fetchval(
        "SELECT document FROM vm_resource_admission_policy WHERE cluster_id=$1",
        configuration["resource_admission"]["cluster_id"],
    )
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("VM_CREATION_RETRY_ENABLED", "true")
    monkeypatch.setenv("VM_RESOURCE_ADMISSION_CONFIG", policy)
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "false")

    async def resolve(_client, request, *, secret):
        assert secret == b"retained-session-resume-test"
        return {"request": request, "controller_configuration": configuration}

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    provisioner = VMProvisioner()
    provisioner._db = db
    provisioner._controller_url = "http://controller.test"
    provisioner._http_client = object()
    provisioner._lifecycle_hmac_secret = b"retained-session-resume-test"
    suspension = WorkspaceSuspensionService()
    suspension.connect(
        db,
        SimpleNamespace(is_available=True),
        None,
        vm_provisioner=provisioner,
    )
    return case, physical, current, suspension


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ready", "observed_before_ready"),
    [(True, False), (False, False), (False, True)],
    ids=["ready", "adopted-never-ready", "observed-vmi-before-ready"],
)
@pytest.mark.parametrize("marker", [False, True], ids=["no-marker", "kept-marker"])
async def test_native_retained_resume_admits_exact_disk_after_new_actor_binding(
    db, monkeypatch, ready, marker, observed_before_ready
):
    case, physical, current, suspension = await retained_resume(
        db,
        monkeypatch,
        ready=ready,
        marker=marker,
        bind=True,
        observed_before_ready=observed_before_ready,
    )
    await ensure_session_workspace(
        str(case["thread_id"]),
        db=db,
        provisioner=None,
        suspension=suspension,
        expected_runtime_generation=str(current["runtime_generation"]),
    )
    resumed = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1 AND request_id<>$2",
        case["thread_id"],
        case["request_id"],
    )
    assert physical.stopped and not physical.purged
    await assert_predecessor_unchanged(db, case)
    assert await db.fetchval("SELECT count(*) FROM vm_creation_effects") == 3
    assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == 1
    assert resumed is not None, "Settled retained End did not admit a Resume source"
    assert resumed["thread_runtime_generation"] == current["runtime_generation"]
    assert resumed["thread_agent_id"] == current["agent_id"]
    assert resumed["thread_attach_token"] == current["runtime_attach_token"]
    assert resumed["provision_generation"] != case["generation"]
    assert resumed["expected_pvc_uid"] == UUID(case["pvc_uid"]), (
        "Ordinary Resume source lacks exact retained-PVC authority"
    )


@pytest.mark.asyncio
async def test_native_retained_resume_waits_for_reciprocal_actor_binding(
    db, monkeypatch
):
    case, _, current, suspension = await retained_resume(
        db, monkeypatch, ready=True, marker=True, bind=False
    )
    await ensure_session_workspace(
        str(case["thread_id"]),
        db=db,
        provisioner=None,
        suspension=suspension,
        expected_runtime_generation=str(current["runtime_generation"]),
    )
    await assert_predecessor_unchanged(db, case)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_retries WHERE thread_id=$1 AND request_id<>$2",
            case["thread_id"],
            case["request_id"],
        )
        == 0
    ), "Resume admitted an immutable source before its new actor was bound"


@pytest.mark.asyncio
async def test_native_resumed_source_reaches_retained_disk_controller_boundary(
    db, monkeypatch, controller_setup
):
    """Real configuration/inspect/disk checks; no resource grant or VM effect yet."""
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
    from tests.test_vm_resource_template import shipped_template
    from vm_controller import controller as settings
    from vm_controller.creation_actuation import CreationActuator
    from vm_controller.creation_configuration import resolve_creation_configuration

    ctrl, api, _, _ = controller_setup
    ctrl.template_text = shipped_template()
    monkeypatch.setattr(settings, "VM_NAMESPACE", "workers")
    monkeypatch.setattr(settings, "VM_STORAGE_CLASS", "local")
    monkeypatch.setattr(settings, "VM_NODE_SELECTOR", {})
    monkeypatch.setattr(settings, "VM_TOLERATIONS", [])
    case, _, current, suspension = await retained_resume(
        db, monkeypatch, ready=True, marker=True, bind=True
    )

    async def resolve(_client, request, *, secret):
        return resolve_creation_configuration(ctrl, request)

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    await ensure_session_workspace(
        str(case["thread_id"]),
        db=db,
        provisioner=None,
        suspension=suspension,
        expected_runtime_generation=str(current["runtime_generation"]),
    )
    retry = VMCreationRetryStore(db)
    claims = await retry.claim_due(limit=1)
    assert len(claims) == 1
    source = await retry.inspect(request_id=str(claims[0]["request_id"]))
    disk = json.loads(
        await db.fetchval(
            "SELECT evidence FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='rootdisk'",
            case["request_id"],
        )
    )
    name = "agent-vm-" + str(case["thread_id"]) + "-rootdisk"
    metadata = {
        "name": name,
        "namespace": "workers",
        "labels": {
            "srw.io/owner-kind": "thread",
            "srw.io/owner-id": str(case["thread_id"]),
        },
    }
    api.objects["DataVolume", name] = {
        "apiVersion": "cdi.kubevirt.io/v1beta1",
        "kind": "DataVolume",
        "metadata": {**metadata, "uid": disk["uid"]},
    }
    api.objects["PersistentVolumeClaim", name] = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            **metadata,
            "uid": case["pvc_uid"],
            "ownerReferences": [{"kind": "DataVolume", "uid": disk["uid"]}],
        },
    }

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[1].replace("-", "_")
        return await getattr(
            retry, "authorize_controller" if method == "authorize" else method
        )(**body)

    ctrl._workspace_cleanup_authority_request = authority
    payload = {
        **source["request"],
        "creation_retry": {
            "version": 1,
            "request_id": source["request_id"],
            "claim_token": str(claims[0]["claim_token"]),
            "request_digest": source["request_digest"],
            "controller_configuration_digest": source[
                "controller_configuration_digest"
            ],
        },
    }
    try:
        result = await CreationActuator(ctrl)._run(payload)
        assert result["status"] == "creation_pending"
    finally:
        await assert_predecessor_unchanged(db, case)
        assert api.writes == []
        assert await db.fetchval("SELECT count(*) FROM vm_creation_effects") == 3
        assert await db.fetchval("SELECT count(*) FROM vm_resource_reservations") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True], ids=["soft-end", "later-purge"])
async def test_native_resume_ended_before_actor_or_source_retains_disk_and_settles(
    db, monkeypatch, permanent
):
    case, physical, current, _ = await retained_resume(
        db, monkeypatch, ready=True, marker=False, bind=False
    )
    retirement = await db.begin_pinned_thread_retirement(
        str(case["thread_id"]), permanent=False
    )
    assert retirement["state"] == "pending", retirement
    assert retirement["generation"] == str(current["runtime_generation"])
    assert await db.authorize_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    await operations(db, physical).cleanup_pinned_thread_retirement(
        retirement, cleanup_agent_pod=False
    )
    assert not physical.purged
    assert len(physical.effects) == 1
    assert await db.settle_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
    ), "Unstarted Resume cannot settle End from its retained predecessor"
    await assert_predecessor_unchanged(db, case)
    if permanent:
        final = await permanent_begin(db, case)
        await operations(db, physical).cleanup_pinned_thread_retirement(
            final, cleanup_agent_pod=False
        )
        assert physical.purged


@pytest.mark.asyncio
async def test_native_queued_resume_end_proves_current_actor_and_can_resume_again(
    db, monkeypatch
):
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
    from tests.test_vm_unadmitted_waiter_end_real_postgres import (
        _current_zero_arguments,
    )

    case, _, current, suspension = await retained_resume(
        db, monkeypatch, ready=True, marker=False, bind=True
    )
    await ensure_session_workspace(
        str(case["thread_id"]),
        db=db,
        provisioner=None,
        suspension=suspension,
        expected_runtime_generation=str(current["runtime_generation"]),
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1 AND request_id<>$2",
        case["thread_id"],
        case["request_id"],
    )
    retirement = await db.begin_pinned_thread_retirement(
        str(case["thread_id"]), permanent=False
    )
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    assert (
        await VMCreationRetryStore(db).settle_never_issued(
            request_id=str(source["request_id"])
        )
    )["settled"]
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(case["thread_id"]), **await _current_zero_arguments(db, current, retirement)
    ), "Settled retained creation cannot prove the exact new actor zero"
    assert await db.settle_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
    )
    assert await db.resume_thread(str(case["thread_id"]))
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_thread_retained_resumes WHERE thread_id=$1",
            case["thread_id"],
        )
        == 2
    )
    await assert_predecessor_unchanged(db, case)


async def resumed_effects(
    db, monkeypatch, *, stop_after="vm", adopt=False, observe_last=True
):
    """Native source/grant/effect ledger; only signed Kubernetes observations modeled."""
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
    from orchestrator.services.vm_resource_job_runtime import (
        installed_job_resource_store,
    )
    from shared.vm_creation_issuance import (
        EFFECT_NONCE_ANNOTATION,
        REQUEST_ANNOTATION,
        canonical_configuration_digest,
        seal_creation_carrier,
    )
    from shared.vm_creation_retry import canonical_request_digest
    from tests.test_vm_creation_actuation import SECRET
    from tests.test_vm_resource_inventory_real_postgres import publish, successor

    case, physical, current, suspension = await retained_resume(
        db, monkeypatch, ready=True, marker=False, bind=True
    )
    thread_id, runtime = case["thread_id"], current["runtime_generation"]
    await ensure_session_workspace(
        str(thread_id),
        db=db,
        provisioner=None,
        suspension=suspension,
        expected_runtime_generation=str(runtime),
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1 AND request_id<>$2",
        thread_id,
        case["request_id"],
    )
    request_id, generation = source["request_id"], source["provision_generation"]
    request, config = (
        json.loads(source["canonical_request"]),
        json.loads(source["controller_configuration"]),
    )
    async with db.acquire() as conn:
        policy = await installed_job_resource_store(conn, db, config)
    sample = successor(case["observed"])
    sample.update(vms=[], vmis=[], pods=[])
    pv_uid = str(uuid4())
    sample["pvcs"] = [
        {
            "uid": case["pvc_uid"],
            "name": f"agent-vm-{thread_id}-rootdisk",
            "pv_uid": pv_uid,
            "pv_name": "thread-retained",
            "storage_class_uid": sample["storage_classes"][0]["uid"],
            "phase": "Bound",
        }
    ]
    sample["pvs"] = [
        {
            "uid": pv_uid,
            "name": "thread-retained",
            "claim_uid": case["pvc_uid"],
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
    await publish(case["inventory"], sample)
    case["resume_sample"] = sample
    admitted = await policy.admit(request_id=str(request_id))
    assert admitted["action"] == "admitted", admitted
    retry = VMCreationRetryStore(db)
    claim = (await retry.claim_due(limit=1))[0]
    old_root = json.loads(
        await db.fetchval(
            "SELECT evidence FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='rootdisk'",
            case["request_id"],
        )
    )
    grant = await retry.authorize_controller(
        request_id=str(request_id),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(thread_id),
            "provision_generation": str(generation),
            "request_digest": canonical_request_digest(request),
            "controller_configuration_digest": canonical_configuration_digest(config),
            "expected_pvc_uid": case["pvc_uid"],
        },
    )
    assert grant["allowed"] is True
    values = {
        "version": 4,
        "resource_grant": grant["resource_grant"],
        "rootdisk_source": {"kind": "retained", "pvc_uid": case["pvc_uid"]},
        "source": "controller_vm_create",
        "admission_id": str(grant["admission_id"]),
        "reservation_request_id": grant["request_id"],
        "intent_digest": grant["intent_digest"],
        "retry_request_id": str(request_id),
        "job_id": str(thread_id),
        "owner_kind": "thread",
        "thread_runtime_generation": str(runtime),
        "thread_agent_id": str(current["agent_id"]),
        "thread_attach_token": str(current["runtime_attach_token"]),
        "thread_wake_operation_id": None,
        "provision_generation": str(generation),
        "request_digest": canonical_request_digest(request),
        "controller_configuration_digest": canonical_configuration_digest(config),
        "expected_pvc_uid": case["pvc_uid"],
        "retained_dv_uid": old_root["uid"],
        "current_dv_uid": old_root["uid"],
        "current_pvc_uid": case["pvc_uid"],
        "current_secret_uid": None,
        "effect_kind": "rootdisk",
        "effect_nonce": str(uuid4()),
        "object_name": "agent-vm-" + str(thread_id) + "-rootdisk",
    }
    carrier_uid = str(uuid4())
    observations = {}
    for kind in ("rootdisk", "cloud_init", "vm"):
        if kind != "rootdisk":
            values = {
                **values,
                "effect_kind": kind,
                "effect_nonce": str(uuid4()),
                "object_name": "agent-vm-"
                + str(thread_id)
                + ("-cloudinit" if kind == "cloud_init" else ""),
                "current_dv_uid": observations["rootdisk"]["object"]["metadata"]["uid"],
                "current_pvc_uid": observations["rootdisk"]["pvc"]["metadata"]["uid"],
                "current_secret_uid": observations["cloud_init"]["object"]["metadata"][
                    "uid"
                ]
                if kind == "vm"
                else None,
            }
        carrier = seal_creation_carrier(
            values,
            namespace="workers",
            uid=carrier_uid,
            resource_version="3",
            secret=SECRET,
        )
        assert (
            await retry.begin_effect(
                request_id=str(request_id),
                claim_token=str(claim["claim_token"]),
                carrier=carrier,
            )
        )["actuation_allowed"] is True
        object_metadata = {
            "uid": str(uuid4()),
            "name": values["object_name"],
            "namespace": "workers",
            "labels": {
                "srw.io/owner-kind": "thread",
                "srw.io/owner-id": str(thread_id),
            },
            "annotations": {
                EFFECT_NONCE_ANNOTATION: values["effect_nonce"],
                REQUEST_ANNOTATION: str(request_id),
                "srw.io/provision-generation": str(generation),
                "srw.io/ssh-host-key-fingerprint": "SHA256:" + "A" * 43,
            },
        }
        if kind == "vm":
            object_metadata["annotations"].update(
                {
                    "srw.io/vm-resource-reservation": admitted["reservation_id"],
                    "srw.io/vm-resource-node-uid": admitted["node_uid"],
                }
            )
        if kind == "rootdisk":
            object_metadata["uid"] = old_root["uid"]
            object_metadata.pop("annotations")
            observation = {
                "outcome": "observed",
                "object": {
                    "apiVersion": "cdi.kubevirt.io/v1beta1",
                    "kind": "DataVolume",
                    "metadata": object_metadata,
                    "status": {"phase": "Succeeded"},
                    "spec": {
                        "source": {
                            "registry": {"url": "docker://" + request["vm_image"]}
                        }
                    },
                },
                "pvc": {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "status": {"phase": "Bound"},
                    "metadata": {
                        "uid": case["pvc_uid"],
                        "name": values["object_name"],
                        "labels": object_metadata["labels"],
                        "namespace": "workers",
                        "ownerReferences": [
                            {"kind": "DataVolume", "uid": object_metadata["uid"]}
                        ],
                    },
                },
            }
        else:
            observation = {
                "outcome": "observed",
                "object": {
                    "apiVersion": "v1" if kind == "cloud_init" else "kubevirt.io/v1",
                    "kind": "Secret" if kind == "cloud_init" else "VirtualMachine",
                    "metadata": object_metadata,
                    "spec": {
                        "template": {
                            "metadata": {
                                "annotations": {
                                    "srw.io/vm-resource-reservation": admitted[
                                        "reservation_id"
                                    ],
                                    "srw.io/vm-resource-node-uid": admitted["node_uid"],
                                    "srw.io/provision-generation": str(generation),
                                }
                            },
                            "spec": {
                                "volumes": [
                                    {
                                        "name": "rootdisk",
                                        "dataVolume": {
                                            "name": "agent-vm-"
                                            + str(thread_id)
                                            + "-rootdisk"
                                        },
                                    },
                                    {
                                        "name": "cloud-init",
                                        "cloudInitNoCloud": {
                                            "secretRef": {
                                                "name": "agent-vm-"
                                                + str(thread_id)
                                                + "-cloudinit"
                                            }
                                        },
                                    },
                                ]
                            },
                        }
                    },
                },
            }
        observations[kind] = observation
        if kind == stop_after and not observe_last:
            break
        assert (
            await retry.observe_effect(
                request_id=str(request_id),
                carrier=carrier,
                observation=observation,
            )
        )["recorded"] is True
        observations[kind] = observation
        if kind == stop_after:
            break
    if adopt:
        assert stop_after == "vm"
        assert await retry.settle_adopted(
            request_id=str(request_id),
            carrier=carrier,
            observations=observations,
        ) == {"settled": True, "disposition": "adopted"}
    return case, physical, current, source, retry, admitted, observations, carrier


@pytest.mark.asyncio
async def test_observed_resume_vm_ended_before_adoption_hands_off_exact_new_charge(
    db, monkeypatch
):
    (
        case,
        _,
        current,
        source,
        retry,
        admitted,
        observations,
        carrier,
    ) = await resumed_effects(db, monkeypatch)
    retirement = await db.begin_pinned_thread_retirement(
        str(case["thread_id"]), permanent=False
    )
    assert retirement["state"] == "pending", retirement
    assert retirement["context"]["vm"] is None
    assert await db.authorize_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    original_context = await db.fetchval(
        "SELECT runtime_retirement_context FROM threads WHERE id=$1", case["thread_id"]
    )
    result = await retry.settle_adopted(
        request_id=str(source["request_id"]), carrier=carrier, observations=observations
    )
    assert result == {"settled": True, "disposition": "retirement_handoff"}
    new = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1", source["request_id"]
    )
    assert new["state"] == "settled" and new["ready_at"] is None
    assert (
        await db.fetchval(
            "SELECT runtime_retirement_context FROM threads WHERE id=$1",
            case["thread_id"],
        )
        == original_context
    )
    authority = await db.fetchrow(
        "SELECT * FROM vm_resource_thread_cleanup_authorities WHERE request_id=$1",
        source["request_id"],
    )
    assert authority is not None
    assert authority["reservation_id"] == UUID(admitted["reservation_id"])
    assert str(authority["vm_uid"]) == observations["vm"]["object"]["metadata"]["uid"]
    assert authority["vm_uid"] != UUID(case["vm_uid"])
    assert authority["pvc_uid"] == UUID(case["pvc_uid"])
    charge = await db.fetchrow(
        "SELECT * FROM vm_resource_reservations WHERE id=$1",
        authority["reservation_id"],
    )
    assert charge["state"] == "teardown"
    assert (charge["vm_uid"], charge["vmi_uid"], charge["launcher_uid"]) == (
        None,
        None,
        None,
    )
    await assert_predecessor_unchanged(db, case)
    from tests.test_vm_resource_thread_cleanup_real_postgres import PhysicalStop
    from tests.test_vm_unadmitted_waiter_end_real_postgres import (
        _current_zero_arguments,
    )

    new_case = {
        **case,
        "request_id": source["request_id"],
        "generation": source["provision_generation"],
        "vm_uid": str(authority["vm_uid"]),
        "vmi_uid": None,
        "launcher_uid": None,
        "admitted": admitted,
    }
    physical = PhysicalStop(db, new_case)
    cleanup = operations(db, physical)

    async def route_zero(thread_id, **identity):
        assert thread_id == str(case["thread_id"])
        assert identity["expected_runtime_generation"] == retirement["generation"]
        assert (
            identity["expected_owner_uid"]
            == retirement["context"]["route"]["owner_pod_uid"]
        )
        return True

    cleanup.dependencies.session_router = SimpleNamespace(teardown_route=route_zero)
    await cleanup.cleanup_pinned_thread_retirement(retirement, cleanup_agent_pod=False)
    assert physical.stopped and physical.purged is False
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            authority["reservation_id"],
        )
        == "released"
    )
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(case["thread_id"]), **await _current_zero_arguments(db, current, retirement)
    )
    assert await db.settle_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
    )
    assert await db.resume_thread(str(case["thread_id"]))
    await assert_predecessor_unchanged(db, case)


@pytest.mark.asyncio
async def test_native_resume_permanently_ended_before_binding_purges_exact_disk(
    db, monkeypatch
):
    case, physical, current, _ = await retained_resume(
        db, monkeypatch, ready=True, marker=False, bind=False
    )
    retirement = await db.begin_pinned_thread_retirement(
        str(case["thread_id"]), permanent=True
    )
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    await operations(db, physical).cleanup_pinned_thread_retirement(
        retirement, cleanup_agent_pod=False
    )
    assert physical.purged
    await db.delete_thread(
        str(case["thread_id"]),
        expected_runtime_generation=retirement["generation"],
        expected_runtime_retirement_token=retirement["token"],
    )
    assert await db.get_thread(str(case["thread_id"])) is None
    await assert_predecessor_unchanged(db, case)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["rootdisk", "cloud_init"])
@pytest.mark.parametrize("permanent", [False, True])
async def test_partial_resumed_creation_retains_root_and_settles_current_actor(
    db, monkeypatch, controller_setup, stage, permanent
):
    from copy import deepcopy
    from kubernetes.client.exceptions import ApiException
    from tests.test_vm_unadmitted_waiter_end_real_postgres import (
        _current_zero_arguments,
    )
    from vm_controller import controller as settings
    from vm_controller.creation_disposition import CreationDisposer

    (
        case,
        physical,
        current,
        source,
        retry,
        admitted,
        observations,
        carrier,
    ) = await resumed_effects(db, monkeypatch, stop_after=stage)
    retirement = await db.begin_pinned_thread_retirement(
        str(case["thread_id"]), permanent=permanent
    )
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    ctrl, api, _, _ = controller_setup
    monkeypatch.setattr(settings, "VM_NAMESPACE", "workers")
    for value in [
        carrier,
        observations["rootdisk"]["pvc"],
        *[item["object"] for item in observations.values()],
    ]:
        api.objects[value["kind"], value["metadata"]["name"]] = deepcopy(value)
    deletes = []

    def listed(kind):
        return {
            "metadata": {"resourceVersion": "1"},
            "items": [
                deepcopy(value)
                for (resource, _), value in api.objects.items()
                if resource == kind
            ],
        }

    def delete(kind, name, body):
        value = api.read(kind, name)
        if body["preconditions"]["uid"] != value["metadata"]["uid"]:
            raise ApiException(status=409)
        deletes.append(kind)
        del api.objects[kind, name]

    ctrl.k8s_client.list_namespaced_custom_object = lambda **kw: listed(
        {
            "virtualmachines": "VirtualMachine",
            "virtualmachineinstances": "VirtualMachineInstance",
        }[kw["plural"]]
    )
    ctrl.core_api.list_namespaced_pod = lambda **kw: listed("Pod")
    ctrl.core_api.delete_namespaced_secret = lambda **kw: delete(
        "Secret", kw["name"], kw["body"]
    )
    ctrl.coordination_api.delete_namespaced_lease = lambda **kw: delete(
        "Lease", kw["name"], kw["body"]
    )

    async def authority(path, body, *, operation):
        return await getattr(retry, path.rsplit("/", 1)[-1].replace("-", "_"))(**body)

    ctrl._workspace_cleanup_authority_request = authority
    from shared.vm_creation_disposition import disposition_identity

    identity = disposition_identity(
        await retry.inspect(request_id=str(source["request_id"]))
    )
    result = await CreationDisposer(ctrl)._run(
        identity, {**identity, "status": "creation_disposition_pending"}
    )
    assert result["status"] == "creation_disposed", result
    assert "DataVolume" not in deletes and "PersistentVolumeClaim" not in deletes
    assert ("Secret" in deletes) == (stage == "cloud_init")
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(admitted["reservation_id"]),
        )
        == "released"
    )
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(case["thread_id"]), **await _current_zero_arguments(db, current, retirement)
    )
    if permanent:
        cleanup = operations(db, physical)

        async def route_zero(*args, **kwargs):
            return True

        cleanup.dependencies.session_router = SimpleNamespace(teardown_route=route_zero)
        await cleanup.cleanup_pinned_thread_retirement(
            retirement, cleanup_agent_pod=False
        )
        assert physical.purged
        await db.delete_thread(
            str(case["thread_id"]),
            expected_runtime_generation=retirement["generation"],
            expected_runtime_retirement_token=retirement["token"],
        )
        assert await db.get_thread(str(case["thread_id"])) is None
    else:
        assert await db.settle_pinned_thread_retirement(
            str(case["thread_id"]),
            token=retirement["token"],
            generation=retirement["generation"],
        )
        assert await db.resume_thread(str(case["thread_id"]))
    await assert_predecessor_unchanged(db, case)


@pytest.mark.asyncio
@pytest.mark.parametrize("admitted_source", [False, True])
async def test_attach_abort_keeps_resume_disk_but_never_rebinds_source(
    db, monkeypatch, admitted_source
):
    from tests.test_pinned_vm_failed_initial_end_real_postgres import (
        _abort_and_rebind_same_pod,
    )
    from tests.test_vm_unadmitted_waiter_end_real_postgres import (
        _current_zero_arguments,
    )
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

    case, _, current, dependencies = await retained_resume(
        db, monkeypatch, ready=True, marker=False, bind=True
    )
    if admitted_source:
        await ensure_session_workspace(
            str(case["thread_id"]),
            db=db,
            provisioner=None,
            suspension=dependencies,
            expected_runtime_generation=str(current["runtime_generation"]),
        )
    current = await _abort_and_rebind_same_pod(db, current)
    await ensure_session_workspace(
        str(case["thread_id"]),
        db=db,
        provisioner=None,
        suspension=dependencies,
        expected_runtime_generation=str(current["runtime_generation"]),
    )
    sources = await db.fetch(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1 AND request_id<>$2",
        case["thread_id"],
        case["request_id"],
    )
    assert len(sources) == int(admitted_source)
    retirement = await db.begin_pinned_thread_retirement(
        str(case["thread_id"]), permanent=False
    )
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    if admitted_source:
        assert (
            await VMCreationRetryStore(db).settle_never_issued(
                request_id=str(sources[0]["request_id"])
            )
        )["settled"]
    zero = await _current_zero_arguments(db, current, retirement)
    if not admitted_source:
        zero.update(
            expected_quiescence_protocol="workspace_actuator_zero_v1",
            expected_workspace_generation=str(case["generation"]),
            expected_workspace_runtime_incarnation=case["vm_uid"],
        )
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(case["thread_id"]), **zero
    )
    assert await db.settle_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
    )
    assert await db.resume_thread(str(case["thread_id"]))
    await assert_predecessor_unchanged(db, case)


@pytest.mark.asyncio
async def test_unknown_resume_vm_issuance_holds_until_exact_late_observation(
    db, monkeypatch
):
    case, _, _, source, retry, admitted, observations, carrier = await resumed_effects(
        db, monkeypatch, observe_last=False
    )
    retirement = await db.begin_pinned_thread_retirement(
        str(case["thread_id"]), permanent=False
    )
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    assert not (await retry.settle_never_issued(request_id=str(source["request_id"])))[
        "settled"
    ]
    assert not await db.pinned_vm_creation_source_settled(
        str(case["thread_id"]),
        runtime_generation=retirement["generation"],
        retirement_token=retirement["token"],
    )
    assert (
        await db.fetchval(
            "SELECT state FROM vm_resource_reservations WHERE id=$1",
            UUID(admitted["reservation_id"]),
        )
        != "released"
    )
    assert (
        await retry.observe_effect(
            request_id=str(source["request_id"]),
            carrier=carrier,
            observation=observations["vm"],
        )
    )["recorded"]
    assert await retry.settle_adopted(
        request_id=str(source["request_id"]), carrier=carrier, observations=observations
    ) == {"settled": True, "disposition": "retirement_handoff"}
    await assert_predecessor_unchanged(db, case)


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [False, True])
async def test_adopted_ordinary_resume_can_end_and_resume_again(db, monkeypatch, ready):
    from tests import test_vm_resource_thread_source_real_postgres as sources
    from tests.test_vm_resource_thread_cleanup_real_postgres import PhysicalStop
    from tests.test_vm_unadmitted_waiter_end_real_postgres import (
        _current_zero_arguments,
    )
    from orchestrator.services.vm_provisioning_phases import VMProvisioningPhaseStore

    (
        case,
        _,
        current,
        source,
        _,
        admitted,
        observations,
        carrier,
    ) = await resumed_effects(db, monkeypatch, adopt=True)
    new_case = {
        **case,
        "request_id": source["request_id"],
        "generation": source["provision_generation"],
        "runtime": current["runtime_generation"],
        "admitted": admitted,
        "vm_uid": observations["vm"]["object"]["metadata"]["uid"],
        "vmi_uid": None,
        "launcher_uid": None,
    }
    if ready:

        async def adopted(*args, **kwargs):
            return (
                None,
                case["inventory"],
                case["resume_sample"],
                case["demand"],
                case["thread_id"],
                current["runtime_generation"],
                source["provision_generation"],
                source["request_id"],
                admitted,
                observations,
                carrier,
            )

        monkeypatch.setattr(sources, "_adopted_charged_thread", adopted)
        prepared = await sources._ready_charged_thread(db, monkeypatch)
        assert await VMProvisioningPhaseStore(db).publish_thread_ready(
            str(case["thread_id"]),
            str(source["provision_generation"]),
            prepared["registration"],
            prepared["vm_uid"],
            prepared["updates"],
        )
        new_case.update(prepared)
        before = await db.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE request_id=$1",
            source["request_id"],
        )
        assert (
            await ensure_session_workspace(
                str(case["thread_id"]),
                db=db,
                provisioner=None,
                suspension=SimpleNamespace(_vm_provisioner=object()),
                expected_runtime_generation=str(current["runtime_generation"]),
            )
            is None
        ), "Ready resumed VM remains incorrectly pending in native ensure"
        assert (
            await db.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                source["request_id"],
            )
            == before
        )
    retirement = await db.begin_pinned_thread_retirement(
        str(case["thread_id"]), permanent=False
    )
    assert retirement["state"] == "pending", retirement
    assert await db.authorize_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    physical = PhysicalStop(db, new_case)
    cleanup = operations(db, physical)

    async def route_zero(*args, **kwargs):
        return True

    cleanup.dependencies.session_router = SimpleNamespace(teardown_route=route_zero)
    await cleanup.cleanup_pinned_thread_retirement(retirement, cleanup_agent_pod=False)
    zero = await _current_zero_arguments(db, current, retirement)
    zero.update(
        expected_quiescence_protocol="workspace_actuator_zero_v1",
        expected_workspace_generation=str(source["provision_generation"]),
        expected_workspace_runtime_incarnation=new_case["vm_uid"],
    )
    assert await db.acknowledge_pinned_thread_local_quiescence(
        str(case["thread_id"]), **zero
    )
    assert await db.settle_pinned_thread_retirement(
        str(case["thread_id"]),
        token=retirement["token"],
        generation=retirement["generation"],
    )
    assert await db.resume_thread(str(case["thread_id"]))
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_thread_retained_resumes WHERE thread_id=$1",
            case["thread_id"],
        )
        == 2
    )
    await assert_predecessor_unchanged(db, case)


@pytest.mark.asyncio
async def test_resume_saved_image_and_absent_profile_survive_new_defaults(
    db, monkeypatch
):
    case, _, current, suspension = await retained_resume(
        db, monkeypatch, ready=True, marker=False, bind=True
    )
    prior = json.loads(
        await db.fetchval(
            "SELECT canonical_request FROM vm_creation_retries WHERE request_id=$1",
            case["request_id"],
        )
    )
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "true")
    monkeypatch.setenv(
        "VM_NETWORK_PROFILE_IMAGE_ALLOWLIST",
        "registry.example/fresh@sha256:" + "b" * 64,
    )
    monkeypatch.setenv("VM_IMAGE", "registry.example/fresh@sha256:" + "b" * 64)
    for _ in range(2):
        await ensure_session_workspace(
            str(case["thread_id"]),
            db=db,
            provisioner=None,
            suspension=suspension,
            expected_runtime_generation=str(current["runtime_generation"]),
        )
    rows = await db.fetch(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1 AND request_id<>$2",
        case["thread_id"],
        case["request_id"],
    )
    assert len(rows) == 1
    request = json.loads(rows[0]["canonical_request"])
    assert request["vm_image"] == prior["vm_image"]
    assert request.get("network_profile") is None
    assert request.get("initialization") is None and request.get("preparation") is None
    assert rows[0]["expected_pvc_uid"] == UUID(case["pvc_uid"])
    await assert_predecessor_unchanged(db, case)


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["warm", "cold"])
async def test_authenticated_resume_poll_commits_current_actor_and_no_credentials(
    db, monkeypatch, binding
):
    from tests.test_pinned_vm_initial_binding_real_postgres import (
        _bind_cold_agent,
        _poll,
    )

    case, _, current, suspension = await retained_resume(
        db, monkeypatch, ready=True, marker=False, bind=False
    )
    current = await (_bind_protected_agent if binding == "warm" else _bind_cold_agent)(
        db, case["thread_id"]
    )
    payload = await _poll(db, suspension._vm_provisioner, current)
    assert payload["status"] == "creating" and payload["vm_status"] == "provisioning"
    assert payload["session_runtime_generation"] == str(current["runtime_generation"])
    assert not any(
        payload.get(key)
        for key in (
            "vm_ssh_host",
            "ssh_key_path",
            "resolved_config",
            "datasources",
            "cloud_mount",
        )
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1 AND request_id<>$2",
        case["thread_id"],
        case["request_id"],
    )
    assert source["thread_agent_id"] == current["agent_id"]
    assert source["thread_attach_token"] == current["runtime_attach_token"]
    assert source["expected_pvc_uid"] == UUID(case["pvc_uid"])
    assert await _poll(db, suspension._vm_provisioner, current) == payload
    await assert_predecessor_unchanged(db, case)
