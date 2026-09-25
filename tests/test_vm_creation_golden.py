"""Golden sources remain exact while a job clone can still consume them."""

from copy import deepcopy
from uuid import uuid4

import pytest
from kubernetes.client.exceptions import ApiException

from tests.test_vm_creation_actuation import (
    poll_until_terminal,
    setup as _actuation_fixture,
)
from vm_controller import controller as settings
from vm_controller.creation_configuration import resolve_creation_configuration

setup = _actuation_fixture


@pytest.fixture
def golden(setup, monkeypatch):
    ctrl, api, authority, payload = setup
    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", True)
    resolved = resolve_creation_configuration(ctrl, authority.row["request"])
    authority.row.update(resolved)
    payload["creation_retry"]["controller_configuration_digest"] = resolved[
        "controller_configuration_digest"
    ]
    name = settings._golden_name(payload["vm_image"])
    source = ctrl._golden_dv_manifest(name, payload["vm_image"])
    source["metadata"].update(uid=str(uuid4()), resourceVersion="1")
    source["status"] = {"phase": "Succeeded"}
    pvc = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": name,
            "namespace": settings.VM_NAMESPACE,
            "uid": str(uuid4()),
            "resourceVersion": "1",
            "ownerReferences": [
                {
                    "kind": "DataVolume",
                    "uid": source["metadata"]["uid"],
                    "controller": True,
                }
            ],
        },
        "spec": {"volumeMode": "Filesystem"},
        "status": {"phase": "Bound"},
    }
    api.objects["DataVolume", name] = source
    api.objects["PersistentVolumeClaim", name] = pvc
    api.replacements = []
    api.deletions = []

    def replace(**kw):
        body = deepcopy(kw["body"])
        old = api.read("DataVolume", kw["name"])
        if (body["metadata"]["uid"], body["metadata"]["resourceVersion"]) != (
            old["metadata"]["uid"],
            old["metadata"]["resourceVersion"],
        ):
            raise ApiException(status=409)
        body["metadata"]["resourceVersion"] = str(
            int(old["metadata"]["resourceVersion"]) + 1
        )
        api.objects["DataVolume", kw["name"]] = body
        api.replacements.append(deepcopy(body))
        return deepcopy(body)

    def delete(**kw):
        old = api.read("DataVolume", kw["name"])
        preconditions = kw.get("body", {}).get("preconditions", {})
        if preconditions != {
            "uid": old["metadata"]["uid"],
            "resourceVersion": old["metadata"]["resourceVersion"],
        }:
            raise ApiException(status=409)
        api.deletions.append((kw["name"], deepcopy(preconditions)))
        del api.objects["DataVolume", kw["name"]]
        return {}

    ctrl.k8s_client.replace_namespaced_custom_object = replace
    ctrl.k8s_client.delete_namespaced_custom_object = delete
    ctrl.k8s_client.list_namespaced_custom_object = lambda **kw: {
        "items": [
            deepcopy(obj)
            for (kind, _), obj in api.objects.items()
            if kind == "DataVolume"
            and obj["metadata"].get("labels", {}).get("srw.io/rootdisk") == "true"
        ]
    }
    ctrl._get_dv = lambda name: _read_dv(api, name)
    return ctrl, api, authority, payload, name


async def _read_dv(api, name):
    try:
        return api.read("DataVolume", name)
    except ApiException as exc:
        if exc.status == 404:
            return None
        raise


@pytest.mark.asyncio
async def test_ready_golden_source_is_frozen_before_rootdisk_grant(golden):
    ctrl, api, authority, payload, name = golden
    result = await poll_until_terminal(ctrl._do_create_serialized, payload)
    assert result["status"] == "created"
    effect = authority.row["effects"][0]
    source = effect["carrier_intent"]["rootdisk_source"]
    assert source["kind"] == "golden"
    assert source["dv_uid"] == api.objects["DataVolume", name]["metadata"]["uid"]
    root = api.read("DataVolume", "agent-vm-" + payload["job_id"] + "-rootdisk")
    assert root["spec"]["source"] == {
        "pvc": {"name": name, "namespace": settings.VM_NAMESPACE}
    }
    assert api.replacements  # Source pin was durably admitted before job disk POST.


@pytest.mark.asyncio
async def test_importing_golden_waits_without_job_effect(golden):
    ctrl, api, authority, payload, name = golden
    api.objects["DataVolume", name]["status"]["phase"] = "ImportInProgress"
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "creation_pending"
    assert result["reason"] == "golden_wait"
    assert authority.row["effects"] == []
    assert api.writes == []


@pytest.mark.asyncio
async def test_pinned_golden_cannot_be_deleted_by_any_delete_caller(golden):
    ctrl, api, authority, payload, name = golden
    api.lost.add("DataVolume")
    await ctrl._do_create_serialized(payload)
    with pytest.raises((RuntimeError, ValueError)):
        await ctrl._delete_dv(name)
    assert not api.deletions


@pytest.mark.asyncio
async def test_golden_with_standalone_clone_reference_is_not_deleted(golden):
    ctrl, api, authority, payload, name = golden
    api.objects["DataVolume", "existing-rootdisk"] = {
        "kind": "DataVolume",
        "metadata": {
            "name": "existing-rootdisk",
            "labels": {"srw.io/rootdisk": "true"},
        },
        "spec": {"source": {"pvc": {"name": name}}},
    }
    with pytest.raises((RuntimeError, ValueError)):
        await ctrl._delete_dv(name)
    assert not api.deletions


@pytest.mark.asyncio
async def test_completed_clone_releases_pin_with_tombstone_and_fences_stale_writer(
    golden,
):
    from vm_controller.creation_sources import GoldenSources, PINS, pins
    import json

    ctrl, api, authority, payload, name = golden
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload))["status"] == "created"
    sources = GoldenSources(ctrl)
    stale = api.read("DataVolume", name)
    await sources.release_completed(authority.row)
    assert (
        pins(api.read("DataVolume", name))[authority.row["request_id"]]["state"]
        == "active"
    )
    rootname = "agent-vm-" + payload["job_id"] + "-rootdisk"
    api.objects["DataVolume", rootname]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", rootname]["status"] = {"phase": "Bound"}
    await sources.release_completed(authority.row)
    released = pins(api.read("DataVolume", name))[authority.row["request_id"]]
    assert released["state"] == "released"
    assert (
        released["rootdisk_uid"]
        == api.objects["DataVolume", rootname]["metadata"]["uid"]
    )
    with pytest.raises(ApiException) as exc:
        await sources.replace(stale)
    assert exc.value.status == 409
    with pytest.raises(ValueError):
        await sources.prepare(
            authority.row,
            authority.row["effects"][0]["carrier_intent"]["rootdisk_source"],
        )
    assert (
        json.loads(api.read("DataVolume", name)["metadata"]["annotations"][PINS])[
            authority.row["request_id"]
        ]
        == released
    )


@pytest.mark.asyncio
async def test_completed_replacement_clone_does_not_release_original_pin(golden):
    from vm_controller.creation_sources import GoldenSources, pins

    ctrl, api, authority, payload, name = golden
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload))["status"] == "created"
    rootname = "agent-vm-" + payload["job_id"] + "-rootdisk"
    api.objects["DataVolume", rootname]["metadata"]["uid"] = str(uuid4())
    api.objects["DataVolume", rootname]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", rootname]["status"] = {"phase": "Bound"}
    with pytest.raises(ValueError):
        await GoldenSources(ctrl).release_completed(authority.row)
    assert (
        pins(api.read("DataVolume", name))[authority.row["request_id"]]["state"]
        == "active"
    )


@pytest.mark.asyncio
async def test_delete_cas_loses_to_concurrent_pin(golden):
    from vm_controller.creation_sources import PINS
    import json

    ctrl, api, authority, payload, name = golden
    original = ctrl.k8s_client.delete_namespaced_custom_object

    def racing_delete(**kw):
        dv = api.objects["DataVolume", name]
        dv["metadata"]["resourceVersion"] = "2"
        dv["metadata"]["annotations"][PINS] = json.dumps(
            {authority.row["request_id"]: {"state": "active"}}
        )
        return original(**kw)

    ctrl.k8s_client.delete_namespaced_custom_object = racing_delete
    with pytest.raises(ApiException) as exc:
        await ctrl._delete_dv(name)
    assert exc.value.status == 409
    assert not api.deletions


@pytest.mark.asyncio
async def test_lost_pin_reply_is_exactly_observed_and_never_rechooses_source(golden):
    ctrl, api, authority, payload, name = golden
    original = ctrl.k8s_client.replace_namespaced_custom_object

    def lost_replace(**kw):
        original(**kw)
        raise TimeoutError("lost pin acknowledgement")

    ctrl.k8s_client.replace_namespaced_custom_object = lost_replace
    api.lost.add("DataVolume")
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    source = deepcopy(authority.row["effects"][0]["carrier_intent"]["rootdisk_source"])
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload))["status"] == "created"
    assert all(
        effect["carrier_intent"]["rootdisk_source"] == source
        for effect in authority.row["effects"]
    )
    assert api.writes.count("DataVolume") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("after_grant", [False, True])
async def test_golden_source_replacement_at_grant_boundary_has_no_job_disk_write(
    golden, after_grant
):
    ctrl, api, authority, payload, name = golden
    original = ctrl._workspace_cleanup_authority_request

    async def changing(path, body, *, operation):
        if operation == "creation_retry_begin_effect":
            if after_grant:
                result = await original(path, body, operation=operation)
            api.objects["DataVolume", name]["metadata"]["uid"] = str(uuid4())
            if after_grant:
                return result
        return await original(path, body, operation=operation)

    ctrl._workspace_cleanup_authority_request = changing
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_attention"
    assert "DataVolume" not in api.writes
    assert "Secret" not in api.writes
    assert "VirtualMachine" not in api.writes
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert "DataVolume" not in api.writes


@pytest.mark.asyncio
async def test_released_tombstone_allows_source_delete_once_clone_is_gone(golden):
    from vm_controller.creation_sources import GoldenSources

    ctrl, api, authority, payload, name = golden
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload))["status"] == "created"
    rootname = "agent-vm-" + payload["job_id"] + "-rootdisk"
    api.objects["DataVolume", rootname]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", rootname]["status"] = {"phase": "Bound"}
    await GoldenSources(ctrl).release_completed(authority.row)
    del api.objects["DataVolume", rootname]
    await ctrl._delete_dv(name)
    assert len(api.deletions) == 1


@pytest.mark.asyncio
async def test_long_lived_golden_compacts_tombstones_without_reviving_old_request(
    golden,
):
    from vm_controller.creation_sources import GoldenSources, pins, PINS
    import json

    ctrl, api, authority, payload, name = golden
    row_before_creation = deepcopy(authority.row)
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload))["status"] == "created"
    rootname = "agent-vm-" + payload["job_id"] + "-rootdisk"
    api.objects["DataVolume", rootname]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", rootname]["status"] = {"phase": "Bound"}
    sources = GoldenSources(ctrl)
    source = authority.row["effects"][0]["carrier_intent"]["rootdisk_source"]
    await sources.release_completed(authority.row)
    # Churn many distinct, completed workspaces through one long-lived golden.
    # Each publication and release executes its actual UID/RV replacement.
    from shared.vm_creation_issuance import (
        public_effect_observation,
        REQUEST_ANNOTATION,
        EFFECT_NONCE_ANNOTATION,
    )
    from shared.vm_creation_retry import canonical_request_digest

    for _ in range(160):
        row = deepcopy(authority.row)
        row.update(
            request_id=str(uuid4()),
            job_id=str(uuid4()),
            provision_generation=str(uuid4()),
        )
        row["request"].update(
            job_id=row["job_id"], provision_generation=row["provision_generation"]
        )
        row["request_digest"] = canonical_request_digest(row["request"])
        effect = row["effects"][0]
        intent = effect["carrier_intent"]
        clone_name = "agent-vm-" + row["job_id"] + "-rootdisk"
        intent.update(
            retry_request_id=row["request_id"],
            job_id=row["job_id"],
            provision_generation=row["provision_generation"],
            request_digest=row["request_digest"],
            effect_nonce=str(uuid4()),
            object_name=clone_name,
        )
        root = deepcopy(api.objects["DataVolume", rootname])
        root["metadata"].update(name=clone_name, uid=str(uuid4()))
        root["metadata"]["labels"]["srw.io/owner-id"] = row["job_id"]
        root["metadata"]["annotations"].update(
            {
                REQUEST_ANNOTATION: row["request_id"],
                EFFECT_NONCE_ANNOTATION: intent["effect_nonce"],
                "srw.io/provision-generation": row["provision_generation"],
            }
        )
        pvc = deepcopy(api.objects["PersistentVolumeClaim", rootname])
        pvc["metadata"].update(
            name=clone_name,
            uid=str(uuid4()),
            ownerReferences=[
                {
                    "kind": "DataVolume",
                    "uid": root["metadata"]["uid"],
                    "controller": True,
                }
            ],
        )
        api.objects["DataVolume", clone_name] = root
        api.objects["PersistentVolumeClaim", clone_name] = pvc
        effect["evidence"] = public_effect_observation(
            intent,
            {"metadata": {"namespace": source["namespace"]}},
            {"outcome": "observed", "object": root, "pvc": pvc},
        )
        dv = api.read("DataVolume", name)
        current = pins(dv)
        current[row["request_id"]] = sources.pin(row, source)
        dv["metadata"]["annotations"][PINS] = json.dumps(current)
        await sources.replace(dv)
        await sources.release_completed(row)
        assert len(pins(api.read("DataVolume", name))) <= 32
        assert (
            len(api.read("DataVolume", name)["metadata"]["annotations"][PINS].encode())
            < 20000
        )
        del api.objects["DataVolume", clone_name]
        del api.objects["PersistentVolumeClaim", clone_name]
    # A compacted tombstone cannot make the old caller's pre-effect row current.
    compacted = pins(api.read("DataVolume", name))
    compacted.pop(authority.row["request_id"], None)
    api.objects["DataVolume", name]["metadata"]["annotations"][PINS] = json.dumps(
        compacted
    )
    authority.row["state"] = "reconciling"  # The immutable effect itself fences it.
    with pytest.raises(ValueError):
        await sources.prepare(row_before_creation, source)
    assert authority.row["request_id"] not in pins(api.read("DataVolume", name))


@pytest.mark.asyncio
async def test_pin_probe_racing_completed_release_and_compaction_loses_cas(golden):
    from vm_controller.creation_sources import GoldenSources, PINS, pins

    ctrl, api, authority, payload, name = golden
    original = ctrl._workspace_cleanup_authority_request

    async def completed_after_probe(path, body, *, operation):
        result = await original(path, body, operation=operation)
        if operation == "creation_retry_inspect":
            # Another handler pins, issues, observes completion, releases, and
            # compacts after this fresh DB result but before our replacement.
            dv = api.objects["DataVolume", name]
            dv["metadata"]["resourceVersion"] = str(
                int(dv["metadata"]["resourceVersion"]) + 3
            )
            dv["metadata"]["annotations"][PINS] = "{}"
            authority.row["state"] = "succeeded"
        return result

    ctrl._workspace_cleanup_authority_request = completed_after_probe
    with pytest.raises(ApiException) as exc:
        await GoldenSources(ctrl).prepare(deepcopy(authority.row))
    assert exc.value.status == 409
    assert pins(api.read("DataVolume", name)) == {}
    assert not api.writes


@pytest.mark.asyncio
async def test_missing_pin_after_unknown_issuance_cannot_be_recreated(golden):
    from vm_controller.creation_sources import GoldenSources, PINS

    ctrl, api, authority, payload, name = golden
    api.lost.add("DataVolume")
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert authority.row["effects"][0]["state"] == "issued"
    api.objects["DataVolume", name]["metadata"]["annotations"][PINS] = "{}"
    with pytest.raises(ValueError, match="durable issuance"):
        await GoldenSources(ctrl).prepare(
            authority.row,
            authority.row["effects"][0]["carrier_intent"]["rootdisk_source"],
        )
    assert api.writes.count("DataVolume") == 1
