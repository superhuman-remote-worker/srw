"""Partial cancellation uses real SQL authority and faulted exact Kubernetes effects."""

from copy import deepcopy
from uuid import UUID, uuid4

import pytest
from kubernetes.client.exceptions import ApiException

from tests.test_vm_creation_actuation import setup as _setup_fixture
from tests.test_vm_creation_effects_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    observed_creation,
    SECRET,
)
from shared.vm_creation_disposition import disposition_identity
from vm_controller import controller as settings
from vm_controller.creation_disposition import CreationDisposer

setup = _setup_fixture
db = _db_fixture


def consumer_metadata(name):
    return {"name": name, "namespace": settings.VM_NAMESPACE, "uid": str(uuid4())}


async def runtime(db, setup, monkeypatch):
    ctrl, api, _, _ = setup
    monkeypatch.setattr(settings, "LIFECYCLE_HMAC_SECRET", SECRET)
    store, row, lease, observations = await observed_creation(
        db, monkeypatch, stop_after="cloud_init"
    )
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    observations["rootdisk"]["pvc"]["metadata"]["labels"] = deepcopy(
        observations["rootdisk"]["object"]["metadata"]["labels"]
    )
    observations["rootdisk"]["pvc"]["metadata"]["ownerReferences"][0]["controller"] = (
        True
    )
    for value in (
        observations["rootdisk"]["object"],
        observations["rootdisk"]["pvc"],
        observations["cloud_init"]["object"],
        lease,
    ):
        api.objects[value["kind"], value["metadata"]["name"]] = deepcopy(value)
    api.deletes = []
    api.lost_deletes = set()
    api.calls = []

    def delete(kind, name, body):
        value = api.read(kind, name)
        uid = body["preconditions"]["uid"]
        if uid != value["metadata"]["uid"]:
            raise ApiException(status=409)
        api.deletes.append((kind, name, uid))
        del api.objects[kind, name]
        if kind in api.lost_deletes:
            api.lost_deletes.remove(kind)
            raise TimeoutError("lost delete reply")

    def listed(kind):
        return {
            "metadata": {"resourceVersion": "1"},
            "items": [
                deepcopy(value)
                for (resource, _), value in api.objects.items()
                if resource == kind
            ],
        }

    ctrl.k8s_client.list_namespaced_custom_object = lambda **kw: listed(
        {
            "virtualmachines": "VirtualMachine",
            "virtualmachineinstances": "VirtualMachineInstance",
        }[kw["plural"]]
    )
    ctrl.core_api.list_namespaced_pod = lambda **kw: listed("Pod")
    ctrl.k8s_client.delete_namespaced_custom_object = lambda **kw: delete(
        "DataVolume", kw["name"], kw["body"]
    )
    ctrl.core_api.delete_namespaced_persistent_volume_claim = lambda **kw: delete(
        "PersistentVolumeClaim", kw["name"], kw["body"]
    )
    ctrl.core_api.delete_namespaced_secret = lambda **kw: delete(
        "Secret", kw["name"], kw["body"]
    )
    ctrl.coordination_api.delete_namespaced_lease = lambda **kw: delete(
        "Lease", kw["name"], kw["body"]
    )

    async def authority(path, body, *, operation):
        method = path.rsplit("/", 1)[-1].replace("-", "_")
        api.calls.append(method)
        if "vm-creation-retries" in path:
            assert operation == "creation_retry_" + method
            assert method not in {"authorize", "begin_effect", "settle_adopted"}
            return await getattr(store, method)(**body)
        assert operation == "recovery-cleanup-" + method
        if method == "complete":
            complete = await store.cleanup.complete_cleanup_permit(
                UUID(body["admission_id"]),
                request_id=UUID(body["request_id"]),
                intent_digest=body["intent_digest"],
                outcome=body["outcome"],
            )
            return {"completed": complete}
        if method == "resume":
            permit = await store.cleanup.resume_cleanup_permit(
                UUID(body["admission_id"]),
                owner_kind=body["owner_kind"],
                owner_id=UUID(body["owner_id"]),
                source=body["source"],
                request_id=UUID(body["request_id"]),
                intent_digest=body["intent_digest"],
            )
            return {
                "allowed": permit.allowed,
                "reason": permit.reason,
                "completed_outcome": permit.completed_outcome,
                "creation_disposition": getattr(permit, "creation_disposition", None),
            }
        raise AssertionError(method)

    ctrl._workspace_cleanup_authority_request = authority
    return ctrl, api, store, row, lease


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lost", [None, "Secret", "DataVolume", "PersistentVolumeClaim"]
)
async def test_fixed_partial_resources_are_disposed_once_and_parent_settles(
    db, setup, monkeypatch, lost
):
    ctrl, api, store, row, _ = await runtime(db, setup, monkeypatch)
    if lost:
        api.lost_deletes.add(lost)
    for _ in range(8):
        result = await CreationDisposer(ctrl).run(disposition_identity(row))
        if result["status"] == "creation_disposed":
            break
    assert result["status"] == "creation_disposed"
    state = await store.inspect(request_id=str(row["request_id"]))
    assert set(state["cancellation_completion"]) == {"cloud_init", "rootdisk", "source", "workspace_attachment"}
    assert state["state"] == "settled"
    assert [item[0] for item in api.deletes if item[0] != "Lease"] == [
        "Secret",
        "DataVolume",
        "PersistentVolumeClaim",
    ]
    assert set(api.writes) <= {"Lease"}
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT completed_at IS NOT NULL AND outcome='creation_disposed' FROM vm_workspace_cleanup_admissions WHERE id=$1",
            UUID(state["creation_admission_id"]),
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE parent_admission_id=$1",
                UUID(state["creation_admission_id"]),
            )
            == 1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["Secret", "DataVolume", "PersistentVolumeClaim"])
async def test_replaced_uid_refuses_before_any_resource_delete(
    db, setup, monkeypatch, kind
):
    ctrl, api, store, row, _ = await runtime(db, setup, monkeypatch)
    value = next(
        value for (resource, _), value in api.objects.items() if resource == kind
    )
    value["metadata"]["uid"] = str(uuid4())
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] == "creation_attention"
    assert api.deletes == []
    assert (await store.inspect(request_id=str(row["request_id"])))[
        "cancellation_progress"
    ] == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["VirtualMachine", "VirtualMachineInstance", "Pod"])
async def test_live_consumer_including_terminating_pod_holds_disposition(
    db, setup, monkeypatch, kind
):
    ctrl, api, _, row, _ = await runtime(db, setup, monkeypatch)
    name = f"agent-vm-{row['job_id']}-rootdisk"
    spec = {"volumes": [{"name": "root", "persistentVolumeClaim": {"claimName": name}}]}
    if kind == "VirtualMachine":
        spec = {"template": {"spec": spec}}
    api.objects[kind, "foreign-consumer"] = {
        "metadata": {
            **consumer_metadata("foreign-consumer"),
            "deletionTimestamp": "2026-09-20T00:00:00Z",
        },
        "spec": spec,
    }
    await CreationDisposer(ctrl).run(disposition_identity(row))
    assert api.deletes == []
    assert "authorize_disposition" not in api.calls


@pytest.mark.asyncio
async def test_restarted_child_carrier_cannot_bypass_consumer_fence(
    db, setup, monkeypatch
):
    ctrl, api, store, row, lease = await runtime(db, setup, monkeypatch)
    await store.freeze_disposition(request_id=str(row["request_id"]), carrier=lease)
    grant = await store.authorize_disposition(
        request_id=str(row["request_id"]), carrier=lease, stage="rootdisk"
    )
    cleanup = grant["cleanup"]
    child = await ctrl._ensure_workspace_cleanup_carrier(
        cleanup,
        **{
            key: cleanup[key]
            for key in (
                "source",
                "owner_kind",
                "owner_id",
                "pvc_uid",
                "dv_uid",
                "provision_generation",
            )
        },
    )
    api.objects["Pod", "late-consumer"] = {
        "metadata": consumer_metadata("late-consumer"),
        "spec": {
            "volumes": [
                {
                    "name": "root",
                    "persistentVolumeClaim": {"claimName": grant["resource"]["name"]},
                }
            ]
        },
    }
    await ctrl._reconcile_workspace_cleanup_carrier(child)
    assert api.deletes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["authorize_disposition", "record_disposition", "carrier_create"]
)
async def test_lost_root_grant_progress_or_carrier_reply_replays_one_child(
    db, setup, monkeypatch, fault
):
    ctrl, api, store, row, _ = await runtime(db, setup, monkeypatch)
    original = ctrl._workspace_cleanup_authority_request
    lost = False

    async def authority(path, body, *, operation):
        nonlocal lost
        result = await original(path, body, operation=operation)
        if (
            not lost
            and path.endswith(fault.replace("_", "-"))
            and body.get("stage") == "rootdisk"
        ):
            lost = True
            raise TimeoutError("lost exact authority reply")
        return result

    ctrl._workspace_cleanup_authority_request = authority
    if fault == "carrier_create":
        api.lost.add("Lease")
    for _ in range(3):
        await CreationDisposer(ctrl).run(disposition_identity(row))
    current = await store.inspect(request_id=str(row["request_id"]))
    assert set(current["cancellation_completion"]) == {"cloud_init", "rootdisk", "source", "workspace_attachment"}
    assert current["state"] == "settled"
    assert len([value for value in api.deletes if value[0] != "Lease"]) == 3
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE parent_admission_id=$1",
                UUID(current["creation_admission_id"]),
            )
            == 1
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
                row["request_id"],
            )
            == 2
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["cloud_init", "rootdisk"])
async def test_consumer_appearing_after_grant_is_checked_before_delete(
    db, setup, monkeypatch, stage
):
    ctrl, api, _, row, _ = await runtime(db, setup, monkeypatch)
    original = ctrl._workspace_cleanup_authority_request

    async def authority(path, body, *, operation):
        result = await original(path, body, operation=operation)
        if path.endswith("/authorize-disposition") and body["stage"] == stage:
            api.objects["VirtualMachine", f"agent-vm-{row['job_id']}"] = {
                "metadata": {"name": f"agent-vm-{row['job_id']}"},
                "spec": {},
            }
        return result

    ctrl._workspace_cleanup_authority_request = authority
    await CreationDisposer(ctrl).run(disposition_identity(row))
    assert [value[0] for value in api.deletes] == (
        [] if stage == "cloud_init" else ["Secret"]
    )


@pytest.mark.asyncio
async def test_pvc_consumer_after_dv_delete_prevents_pvc_delete(db, setup, monkeypatch):
    ctrl, api, _, row, _ = await runtime(db, setup, monkeypatch)
    delete = ctrl.k8s_client.delete_namespaced_custom_object

    def delete_dv(**kwargs):
        result = delete(**kwargs)
        api.objects["Pod", "late-pvc-consumer"] = {
            "metadata": consumer_metadata("late-pvc-consumer"),
            "spec": {
                "volumes": [
                    {
                        "name": "root",
                        "persistentVolumeClaim": {"claimName": kwargs["name"]},
                    }
                ]
            },
        }
        return result

    ctrl.k8s_client.delete_namespaced_custom_object = delete_dv
    await CreationDisposer(ctrl).run(disposition_identity(row))
    assert [value[0] for value in api.deletes] == ["Secret", "DataVolume"]


@pytest.mark.asyncio
async def test_secret_only_consumer_also_prevents_disk_disposition(
    db, setup, monkeypatch
):
    ctrl, api, _, row, _ = await runtime(db, setup, monkeypatch)
    api.objects["Pod", "secret-consumer"] = {
        "metadata": consumer_metadata("secret-consumer"),
        "spec": {
            "containers": [
                {
                    "envFrom": [
                        {"secretRef": {"name": f"agent-vm-{row['job_id']}-cloudinit"}}
                    ]
                }
            ]
        },
    }
    await CreationDisposer(ctrl).run(disposition_identity(row))
    assert api.deletes == []
    assert "authorize_disposition" not in api.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [
        {},
        {"items": [], "metadata": {"continue": "next"}},
        {"items": [], "metadata": {"resourceVersion": "1", "remainingItemCount": 1}},
        {"items": [{}], "metadata": {"resourceVersion": "1"}},
    ],
)
async def test_incomplete_consumer_list_cannot_prove_absence(
    db, setup, monkeypatch, result
):
    ctrl, api, _, row, _ = await runtime(db, setup, monkeypatch)
    ctrl.core_api.list_namespaced_pod = lambda **_: result
    await CreationDisposer(ctrl).run(disposition_identity(row))
    assert api.deletes == []
    assert "authorize_disposition" not in api.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,api_name,method",
    [
        ("Secret", "core_api", "delete_namespaced_secret"),
        ("DataVolume", "k8s_client", "delete_namespaced_custom_object"),
        (
            "PersistentVolumeClaim",
            "core_api",
            "delete_namespaced_persistent_volume_claim",
        ),
    ],
)
async def test_uid_precondition_fences_replacement_at_delete_boundary(
    db, setup, monkeypatch, kind, api_name, method
):
    ctrl, api, store, row, _ = await runtime(db, setup, monkeypatch)
    client = getattr(ctrl, api_name)
    original = getattr(client, method)
    replacement_uid = str(uuid4())

    def replace_then_delete(**kwargs):
        api.objects[kind, kwargs["name"]]["metadata"]["uid"] = replacement_uid
        return original(**kwargs)

    setattr(client, method, replace_then_delete)
    await CreationDisposer(ctrl).run(disposition_identity(row))
    assert not any(item[0] == kind for item in api.deletes)
    assert any(
        value["metadata"]["uid"] == replacement_uid
        for (resource, _), value in api.objects.items()
        if resource == kind
    )
    assert (await store.inspect(request_id=str(row["request_id"])))[
        "state"
    ] == "cancel_requested"


@pytest.mark.asyncio
async def test_late_cancelled_vm_still_adopts_without_partial_cleanup(
    db, setup, monkeypatch
):
    from tests.test_vm_creation_actuation_real_postgres import (
        test_real_authority_lost_vm_reply_adoption_retains_attempts_and_hold as original_bridge,
    )

    ctrl, api, _, _ = setup
    create = ctrl._do_create_serialized
    count = 0

    async def create_then_dispose(body):
        nonlocal count
        count += 1
        if count == 1:
            return await create(body)
        authority = ctrl._workspace_cleanup_authority_request
        current = await authority(
            "/api/internal/vm-creation-retries/inspect",
            {"request_id": body["creation_retry"]["request_id"]},
            operation="creation_retry_inspect",
        )
        prior_writes = list(api.writes)

        async def observation_only(path, values, *, operation):
            assert not path.endswith(
                (
                    "/authorize",
                    "/begin-effect",
                    "/prepare-disposition",
                    "/freeze-disposition",
                    "/authorize-disposition",
                    "/record-disposition",
                )
            )
            return await authority(path, values, operation=operation)

        monkeypatch.setattr(
            ctrl, "_workspace_cleanup_authority_request", observation_only
        )
        assert (await CreationDisposer(ctrl).run(disposition_identity(current)))[
            "status"
        ] == "creation_adopted"
        assert api.writes == prior_writes
        vm = api.read("VirtualMachine", "agent-vm-" + body["job_id"])
        return {"status": "created", "vm_uid": vm["metadata"]["uid"]}

    monkeypatch.setattr(ctrl, "_do_create_serialized", create_then_dispose)
    await original_bridge(db, setup, True)


@pytest.mark.asyncio
async def test_generic_resume_never_grants_old_controller_partial_disk_deletion(
    db, setup, monkeypatch
):
    ctrl, _, store, row, lease = await runtime(db, setup, monkeypatch)
    await store.freeze_disposition(request_id=str(row["request_id"]), carrier=lease)
    grant = await store.authorize_disposition(
        request_id=str(row["request_id"]), carrier=lease, stage="rootdisk"
    )
    cleanup = grant["cleanup"]
    permit = await store.cleanup.resume_cleanup_permit(
        UUID(cleanup["admission_id"]),
        owner_kind="job",
        owner_id=row["job_id"],
        source=cleanup["source"],
        request_id=UUID(cleanup["request_id"]),
        intent_digest=cleanup["intent_digest"],
    )
    assert permit.allowed is False
    assert permit.reason == "creation_disposition_required"
    assert permit.creation_disposition == disposition_identity(row)


@pytest.mark.asyncio
async def test_disposition_carrier_is_unreadable_to_legacy_cleanup_parser(
    db, setup, monkeypatch
):
    ctrl, api, store, row, lease = await runtime(db, setup, monkeypatch)
    await store.freeze_disposition(request_id=str(row["request_id"]), carrier=lease)
    grant = await store.authorize_disposition(
        request_id=str(row["request_id"]), carrier=lease, stage="rootdisk"
    )
    cleanup = grant["cleanup"]
    assert cleanup["source"] == "controller_creation_rootdisk_delete"
    child = await ctrl._ensure_workspace_cleanup_carrier(
        cleanup,
        **{
            key: cleanup[key]
            for key in (
                "source",
                "owner_kind",
                "owner_id",
                "pvc_uid",
                "dv_uid",
                "provision_generation",
            )
        },
    )
    monkeypatch.setattr(
        settings,
        "_CLEANUP_OUTCOMES",
        {
            key: value
            for key, value in settings._CLEANUP_OUTCOMES.items()
            if key != cleanup["source"]
        },
    )
    with pytest.raises(RuntimeError, match="malformed"):
        ctrl._parse_workspace_cleanup_carrier(
            api.objects["Lease", child["carrier_name"]]
        )
    assert api.deletes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["VirtualMachine", "VirtualMachineInstance"])
@pytest.mark.parametrize("source", ["ephemeral", "memoryDump"])
async def test_nested_pvc_consumer_prevents_every_disposition_effect(
    db, setup, monkeypatch, kind, source
):
    ctrl, api, _, row, _ = await runtime(db, setup, monkeypatch)
    root = f"agent-vm-{row['job_id']}-rootdisk"
    reference = {"claimName": root}
    if source == "ephemeral":
        reference = {"persistentVolumeClaim": reference}
    spec = {
        "domain": {
            "resources": {"requests": {"memory": "64M"}},
            "devices": {"disks": [{"name": "dependent", "disk": {"bus": "virtio"}}]},
        },
        "volumes": [{"name": "dependent", source: reference}],
    }
    if kind == "VirtualMachine":
        spec = {"runStrategy": "Halted", "template": {"spec": spec}}
    api.objects[kind, "foreign-consumer"] = {
        "apiVersion": "kubevirt.io/v1",
        "kind": kind,
        "metadata": {
            "name": "foreign-consumer",
            "namespace": settings.VM_NAMESPACE,
            "uid": str(uuid4()),
        },
        "spec": spec,
    }
    await CreationDisposer(ctrl).run(disposition_identity(row))
    assert api.deletes == []
    assert "authorize_disposition" not in api.calls


@pytest.mark.asyncio
async def test_nested_consumer_after_dv_delete_prevents_pvc_delete(
    db, setup, monkeypatch
):
    ctrl, api, _, row, _ = await runtime(db, setup, monkeypatch)
    delete = ctrl.k8s_client.delete_namespaced_custom_object

    def delete_dv(**kwargs):
        result = delete(**kwargs)
        api.objects["VirtualMachineInstance", "late-nested-consumer"] = {
            "metadata": consumer_metadata("late-nested-consumer"),
            "spec": {
                "volumes": [
                    {
                        "name": "root",
                        "ephemeral": {
                            "persistentVolumeClaim": {"claimName": kwargs["name"]}
                        },
                    }
                ]
            },
        }
        return result

    ctrl.k8s_client.delete_namespaced_custom_object = delete_dv
    await CreationDisposer(ctrl).run(disposition_identity(row))
    assert [value[0] for value in api.deletes] == ["Secret", "DataVolume"]
