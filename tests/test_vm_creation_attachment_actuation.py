"""A retained Lease must be observed before its disk or VM dependencies."""

from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests.test_vm_creation_actuation import (
    poll_until_terminal,
    setup as _setup_fixture,
)
from tests.test_vm_creation_golden import golden as _golden_fixture
from vm_controller.creation_configuration import resolve_creation_configuration
from shared.vm_workspace_storage import storage_name

setup = _setup_fixture
golden = _golden_fixture


@pytest.fixture
def attached(setup):
    return bind_attachment(setup)


def bind_attachment(setup):
    ctrl, api, authority, payload = setup
    binding = {
        "uid": str(uuid4()),
        "generation": 1,
        "pvc_uid": None,
        "owner_id": payload["job_id"],
        "owner_kind": "job",
    }
    resolved = resolve_creation_configuration(
        ctrl, {**authority.row["request"], "workspace_storage": binding}
    )
    authority.row.update(resolved)
    payload.update(resolved["request"])
    payload["creation_retry"].update(
        {
            key: resolved[key]
            for key in ("request_digest", "controller_configuration_digest")
        }
    )
    ctrl.k8s_client.list_namespaced_custom_object = lambda **kw: {
        "items": [
            deepcopy(value)
            for (kind, _), value in api.objects.items()
            if kind
            == {
                "virtualmachines": "VirtualMachine",
                "virtualmachineinstances": "VirtualMachineInstance",
                "datavolumes": "DataVolume",
            }[kw["plural"]]
        ]
    }
    ctrl.core_api.list_namespaced_pod = lambda **kw: SimpleNamespace(items=[])
    return ctrl, api, authority, payload


@pytest.mark.asyncio
async def test_new_retained_workspace_observes_attachment_before_root(attached):
    ctrl, api, authority, payload = attached
    result = await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5)
    assert result["status"] == "created"
    assert [
        effect["carrier_intent"]["effect_kind"] for effect in authority.row["effects"]
    ] == ["workspace_attach", "rootdisk", "cloud_init", "vm"]
    assert all(effect["state"] == "observed" for effect in authority.row["effects"])
    name = storage_name(payload["workspace_storage"])
    assert (
        api.read("Lease", name)["metadata"]["annotations"][
            "srw.io/vm-create-request-id"
        ]
        == authority.row["request_id"]
    )
    assert (
        api.read("DataVolume", name)["metadata"]["labels"]["srw.io/workspace-instance"]
        == payload["workspace_storage"]["uid"]
    )
    assert api.writes == ["Lease", "Lease", "DataVolume", "Secret", "VirtualMachine"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_lost_attachment_post_reply_is_observation_only(attached, cancelled):
    ctrl, api, authority, payload = attached
    original = api.create
    lost = False

    def create(body):
        nonlocal lost
        result = original(body)
        if (
            body["kind"] == "Lease"
            and body["metadata"]["name"] == storage_name(payload["workspace_storage"])
            and not lost
        ):
            lost = True
            raise TimeoutError("accepted attachment reply lost")
        return result

    api.create = create
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert lost and api.writes == ["Lease", "Lease"]
    if cancelled:
        authority.row["state"] = "cancel_requested"
        authority.deny = True
    result = await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5)
    assert result["status"] == ("creation_attention" if cancelled else "created")
    assert api.writes.count("Lease") == 2
    assert api.writes.count("DataVolume") == (0 if cancelled else 1)


@pytest.mark.asyncio
async def test_golden_attachment_clone_pin_uses_exact_workspace_target(golden):
    from vm_controller.creation_sources import GoldenSources, PINS, pins

    ctrl, api, authority, payload, source_name = golden
    bind_attachment((ctrl, api, authority, payload))
    result = await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5)
    assert result["status"] == "created"
    name = storage_name(payload["workspace_storage"])
    pin = pins(api.read("DataVolume", source_name))[authority.row["request_id"]]
    assert pin["rootdisk_name"] == name
    assert pin["state"] == "active"
    stale = api.read("DataVolume", source_name)
    api.objects["DataVolume", name]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", name]["status"] = {"phase": "Bound"}
    sources = GoldenSources(ctrl)
    await sources.release_completed(authority.row)
    assert (
        pins(api.read("DataVolume", source_name))[authority.row["request_id"]]["state"]
        == "released"
    )
    # A delayed source-pin mutation cannot revive the released consumer.
    from kubernetes.client.exceptions import ApiException

    with pytest.raises(ApiException):
        await sources.replace(stale)
    del stale["metadata"]["annotations"][PINS]
    stale["metadata"]["resourceVersion"] = api.read("DataVolume", source_name)[
        "metadata"
    ]["resourceVersion"]
    source = authority.row["effects"][1]["carrier_intent"]["rootdisk_source"]
    with pytest.raises(ValueError):
        await sources.hold(authority.row, source, stale)


@pytest.mark.asyncio
@pytest.mark.parametrize("location", ["metadata", "template"])
async def test_vm_attachment_label_drift_cannot_be_adopted(attached, location):
    ctrl, api, authority, payload = attached
    api.lost.add("VirtualMachine")
    for _ in range(4):
        assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    vm = api.objects["VirtualMachine", "agent-vm-" + payload["job_id"]]
    metadata = (
        vm["metadata"] if location == "metadata" else vm["spec"]["template"]["metadata"]
    )
    metadata["labels"]["srw.io/workspace-instance"] = str(uuid4())
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] != "created"
    assert not authority.settled


def retained_attachment(attached, *, advance=False):
    from shared.vm_workspace_storage import storage_labels
    from vm_controller import controller as settings

    ctrl, api, authority, payload = attached
    binding = payload["workspace_storage"]
    name = storage_name(binding)
    api.create(
        {
            "apiVersion": "cdi.kubevirt.io/v1beta1",
            "kind": "DataVolume",
            "metadata": {
                "name": name,
                "namespace": settings.VM_NAMESPACE,
                "labels": {
                    **storage_labels(binding, payload["job_id"]),
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": payload["job_id"],
                },
            },
            "spec": {"source": {"registry": {"url": "docker://old:image"}}},
        }
    )
    api.create(
        {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": name,
                "namespace": settings.VM_NAMESPACE,
                "labels": storage_labels(binding, payload["job_id"]),
                "annotations": {"srw.io/detached": "true"} if advance else {},
            },
            "spec": {},
        }
    )
    binding = {
        **binding,
        "generation": 2 if advance else 1,
        "pvc_uid": api.read("PersistentVolumeClaim", name)["metadata"]["uid"],
    }
    resolved = resolve_creation_configuration(
        ctrl, {**authority.row["request"], "workspace_storage": binding}
    )
    authority.row.update(resolved, expected_pvc_uid=binding["pvc_uid"])
    payload.update(resolved["request"])
    payload["creation_retry"].update(
        {
            key: resolved[key]
            for key in ("request_digest", "controller_configuration_digest")
        }
    )
    api.writes.clear()
    return name


@pytest.mark.asyncio
@pytest.mark.parametrize("advance", [False, True])
async def test_retained_attachment_cas_keeps_disk_and_recovers_lost_put(
    attached, advance
):
    ctrl, api, authority, payload = attached
    name = retained_attachment(attached, advance=advance)
    before = api.read("Lease", name)
    disk = api.read("DataVolume", name)
    original, puts = api.replace, []

    def replace(body):
        result = original(body)
        if body["metadata"]["name"] == name:
            puts.append(deepcopy(body))
            raise TimeoutError("accepted attachment PUT reply lost")
        return result

    api.replace = replace
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert api.writes == ["Lease"]
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"
    assert len(puts) == 1
    assert puts[0]["metadata"]["uid"] == before["metadata"]["uid"]
    assert (
        puts[0]["metadata"]["resourceVersion"] == before["metadata"]["resourceVersion"]
    )
    assert api.read("DataVolume", name) == disk
    assert "DataVolume" not in api.writes
    assert authority.row["effects"][0]["carrier_intent"]["workspace_attachment"][
        "action"
    ] == ("replace" if advance else "claim")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["rv", "uid", "detached"])
async def test_attachment_change_after_grant_cannot_reach_put(attached, change):
    ctrl, api, authority, payload = attached
    name = retained_attachment(attached)
    original = authority.call
    original_replace = api.replace
    attachment_replaces = []

    def replace(body):
        if body["metadata"]["name"] == name:
            attachment_replaces.append(deepcopy(body))
        return original_replace(body)

    api.replace = replace

    async def call(path, body, *, operation):
        result = await original(path, body, operation=operation)
        if path.endswith("begin-effect"):
            meta = api.objects["Lease", name]["metadata"]
            if change == "rv":
                meta["resourceVersion"] = "999"
            elif change == "uid":
                meta["uid"] = str(uuid4())
            else:
                meta.setdefault("annotations", {})["srw.io/detached"] = "true"
        return result

    ctrl._workspace_cleanup_authority_request = call
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert api.writes == ["Lease"]
    assert authority.row["effects"][0]["state"] == "rejected"
    assert authority.row["effects"][0]["evidence"] == {
        "outcome": "not_attempted",
        "reason": "workspace_attachment_unproven",
    }
    assert len(authority.surrenders) == 1
    assert authority.surrenders[0]["effect_nonce"] == authority.row["effects"][0][
        "carrier_intent"
    ]["effect_nonce"]
    assert authority.surrenders[0]["reason"] == "workspace_attachment_unproven"
    assert attachment_replaces == []
    assert (await ctrl._do_create_serialized(payload))["status"] != "created"
    assert api.writes == ["Lease"]
    assert attachment_replaces == []


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["create", "replace"])
async def test_attachment_create_method_entry_keeps_unknown_effect(
    attached, monkeypatch, action
):
    from vm_controller.creation_attachment import CreationAttachment

    ctrl, api, authority, payload = attached
    if action == "replace":
        retained_attachment(attached, advance=True)
    entered = []

    async def fail_after_entry(self, body, intent):
        entered.append(intent["action"])
        raise TimeoutError("attachment reply lost after method entry")

    monkeypatch.setattr(CreationAttachment, "create", fail_after_entry)
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert entered == [action]
    assert api.writes == ["Lease"]
    assert authority.surrenders == []
    assert authority.row["effects"][0]["state"] == "issued"


@pytest.mark.asyncio
async def test_unsupported_attachment_validation_code_keeps_issued_hold(
    attached, monkeypatch
):
    from vm_controller.creation_actuation import CreationUnproven
    from vm_controller.creation_attachment import CreationAttachment

    ctrl, api, authority, payload = attached
    original = CreationAttachment.validate

    async def validate(self, row, intent):
        if authority.row["effects"]:
            raise CreationUnproven("unsupported_creation_code")
        return await original(self, row, intent)

    monkeypatch.setattr(CreationAttachment, "validate", validate)
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_attention"
    assert api.writes == ["Lease"]
    assert authority.surrenders == []
    assert authority.row["effects"][0]["state"] == "issued"


@pytest.mark.asyncio
async def test_foreign_workspace_disk_cannot_receive_attachment_marker(attached):
    ctrl, api, authority, payload = attached
    name = retained_attachment(attached)
    api.objects["DataVolume", name]["metadata"]["labels"][
        "srw.io/workspace-instance"
    ] = str(uuid4())
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_attention"
    assert api.writes == []
    assert authority.row["effects"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["VirtualMachine", "VirtualMachineInstance", "Pod"])
async def test_active_workspace_consumer_refuses_before_attachment(attached, kind):
    ctrl, api, authority, payload = attached
    name = retained_attachment(attached)
    if kind == "Pod":
        ctrl.core_api.list_namespaced_pod = lambda **kw: SimpleNamespace(
            items=[
                SimpleNamespace(
                    spec=SimpleNamespace(
                        volumes=[
                            SimpleNamespace(
                                persistent_volume_claim=SimpleNamespace(claim_name=name)
                            )
                        ]
                    )
                )
            ]
        )
    else:
        spec = {"volumes": [{"persistentVolumeClaim": {"claimName": name}}]}
        api.objects[kind, "other-consumer"] = {
            "kind": kind,
            "metadata": {"name": "other-consumer"},
            "spec": {"template": {"spec": spec}} if kind == "VirtualMachine" else spec,
        }
    assert (await ctrl._do_create_serialized(payload))["status"] != "created"
    assert api.writes == []
    assert authority.row["effects"] == []


@pytest.mark.asyncio
async def test_concurrent_attachment_tombstone_wins_final_put_cas(attached):
    ctrl, api, authority, payload = attached
    name = retained_attachment(attached)
    original, puts = api.replace, []

    def replace(body):
        if body["metadata"]["name"] == name:
            puts.append(deepcopy(body))
            api.objects["Lease", name]["metadata"]["resourceVersion"] = "999"
            api.objects["Lease", name]["metadata"].setdefault("annotations", {})[
                "srw.io/released"
            ] = "true"
        return original(body)

    api.replace = replace
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert len(puts) == 1
    assert api.writes == ["Lease"]
    assert authority.row["effects"][0]["state"] == "issued"
    assert (await ctrl._do_create_serialized(payload))["status"] != "created"
    assert len(puts) == 1 and api.writes == ["Lease"]
