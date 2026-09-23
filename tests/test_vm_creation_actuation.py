"""Controller effects use real provenance validators and fault-injected API stores."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from kubernetes.client.exceptions import ApiException

from vm_controller import controller as settings
from vm_controller.creation_configuration import resolve_creation_configuration
from shared.vm_creation_issuance import (
    public_effect_observation,
    verify_creation_carrier,
)

SECRET = b"creation-actuation-test-secret-at-least-32-bytes"


class API:
    def __init__(self):
        self.objects = {}
        self.writes = []
        self.lost = set()

    def read(self, kind, name):
        if (kind, name) not in self.objects:
            raise ApiException(status=404)
        return deepcopy(self.objects[kind, name])

    def create(self, body):
        body = deepcopy(body)
        kind, name = body["kind"], body["metadata"]["name"]
        self.writes.append(kind)
        if (kind, name) in self.objects:
            raise ApiException(status=409)
        body["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        self.objects[kind, name] = body
        if kind == "DataVolume":
            self.objects["PersistentVolumeClaim", name] = {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {
                    **deepcopy(body["metadata"]),
                    "uid": str(uuid4()),
                    "ownerReferences": [
                        {
                            "kind": "DataVolume",
                            "uid": body["metadata"]["uid"],
                            "controller": True,
                        }
                    ],
                },
                "status": {"phase": "Pending"},
            }
        if kind in self.lost:
            self.lost.remove(kind)
            raise TimeoutError("reply lost after acceptance")
        return deepcopy(body)

    def replace(self, body):
        old = self.read("Lease", body["metadata"]["name"])
        if old["metadata"]["resourceVersion"] != body["metadata"]["resourceVersion"]:
            raise ApiException(status=409)
        body = deepcopy(body)
        body["metadata"]["resourceVersion"] = str(
            int(old["metadata"]["resourceVersion"]) + 1
        )
        self.objects["Lease", body["metadata"]["name"]] = body
        return deepcopy(body)


class Authority:
    def __init__(self, resolved):
        self.row = {
            **resolved,
            "request_id": str(uuid4()),
            "job_id": resolved["request"]["job_id"],
            "provision_generation": resolved["request"]["provision_generation"],
            "expected_pvc_uid": None,
            "state": "reconciling",
            "effects": [],
            "creation_admission_id": str(uuid4()),
            "creation_carrier_uid": None,
        }
        self.reservation = {
            "allowed": True,
            "admission_id": self.row["creation_admission_id"],
            "request_id": str(uuid4()),
            "intent_digest": "sha256:" + "a" * 64,
        }
        self.deny = False
        self.lost_grant = False
        self.settled = False

    async def call(self, path, payload, *, operation):
        assert operation == "creation_retry_" + path.rsplit("/", 1)[1].replace("-", "_")
        method = path.rsplit("/", 1)[1]
        if method == "inspect":
            return deepcopy(self.row)
        if method == "authorize":
            return {"allowed": False} if self.deny else self.reservation
        values = verify_creation_carrier(payload["carrier"], secret=SECRET)
        if method == "begin-effect":
            if self.deny:
                return {"actuation_allowed": False, "reason": "cancelled"}
            for effect in self.row["effects"]:
                if effect["carrier_intent"]["effect_nonce"] == values["effect_nonce"]:
                    return {"actuation_allowed": False, "effect_state": effect["state"]}
            assert not any(x["state"] == "issued" for x in self.row["effects"])
            self.row["effects"].append(
                {"carrier_intent": values, "state": "issued", "evidence": {}}
            )
            self.row["creation_carrier_uid"] = payload["carrier"]["metadata"]["uid"]
            if self.lost_grant:
                self.lost_grant = False
                raise TimeoutError("grant reply lost")
            return {"actuation_allowed": True}
        if method == "observe-effect":
            effect = next(
                x for x in self.row["effects"] if x["carrier_intent"] == values
            )
            prior = {
                x["carrier_intent"]["effect_kind"]: x["evidence"]
                for x in self.row["effects"]
                if x["state"] == "observed"
            }
            effect["evidence"] = public_effect_observation(
                values,
                payload["carrier"],
                payload["observation"],
                rootdisk=prior.get("rootdisk"),
                cloud_init=prior.get("cloud_init"),
            )
            effect["state"] = effect["evidence"]["outcome"]
            return {"recorded": True, "effect_state": effect["state"]}
        if method == "settle-adopted":
            self.settled = True
            self.row["state"] = "succeeded"
            return {"settled": True}
        raise AssertionError(method)


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())
    monkeypatch.setattr(settings, "LIFECYCLE_HMAC_SECRET", SECRET)
    monkeypatch.setattr(settings, "VM_PERSISTENT_ROOTDISK", True)
    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", False)
    ctrl = settings.VMController.__new__(settings.VMController)
    ctrl.template_text = "template"
    ctrl.cloud_init_text = "cloud-init"
    ctrl.headscale = SimpleNamespace(is_available=False)
    ctrl._capacity_wait = AsyncMock(return_value=None)
    ctrl._active_recovery_pins = AsyncMock(return_value=())
    api = API()
    ctrl.k8s_client = SimpleNamespace(
        get_namespaced_custom_object=lambda **kw: api.read(
            "DataVolume" if kw["plural"] == "datavolumes" else "VirtualMachine",
            kw["name"],
        ),
        create_namespaced_custom_object=lambda **kw: api.create(kw["body"]),
    )
    ctrl.core_api = SimpleNamespace(
        read_namespaced_persistent_volume_claim=lambda **kw: api.read(
            "PersistentVolumeClaim", kw["name"]
        ),
        read_namespaced_secret=lambda **kw: api.read("Secret", kw["name"]),
        create_namespaced_secret=lambda **kw: api.create(kw["body"]),
    )
    ctrl.coordination_api = SimpleNamespace(
        read_namespaced_lease=lambda **kw: api.read("Lease", kw["name"]),
        create_namespaced_lease=lambda **kw: api.create(kw["body"]),
        replace_namespaced_lease=lambda **kw: api.replace(kw["body"]),
    )
    request = {
        "job_id": str(uuid4()),
        "provision_generation": str(uuid4()),
        "entity_type": "job",
    }
    resolved = resolve_creation_configuration(ctrl, request)
    authority = Authority(resolved)
    ctrl._workspace_cleanup_authority_request = authority.call
    name = "agent-vm-" + request["job_id"]

    def render(*args):
        return {
            "apiVersion": "kubevirt.io/v1",
            "kind": "VirtualMachine",
            "metadata": {"name": name},
            "spec": {
                "dataVolumeTemplates": [
                    {
                        "metadata": {"name": name + "-rootdisk"},
                        "spec": {
                            "source": {
                                "registry": {"url": "docker://" + args[0]["vm_image"]}
                            }
                        },
                    }
                ],
                "template": {
                    "spec": {
                        "volumes": [
                            {
                                "name": "rootdisk",
                                "dataVolume": {"name": name + "-rootdisk"},
                            },
                            {
                                "name": "cloud-init",
                                "cloudInitNoCloud": {
                                    "secretRef": {"name": name + "-cloudinit"}
                                },
                            },
                        ]
                    }
                },
            },
            "_srwCloudInitUserData": "private-host-key",
            "_srwSSHHostKeyFingerprint": "SHA256:" + "A" * 43,
        }

    ctrl.render_template = render
    payload = {
        **resolved["request"],
        "creation_retry": {
            "version": 1,
            "request_id": authority.row["request_id"],
            "claim_token": str(uuid4()),
            "request_digest": resolved["request_digest"],
            "controller_configuration_digest": resolved[
                "controller_configuration_digest"
            ],
        },
    }
    return ctrl, api, authority, payload


@pytest.fixture
def profiled_setup(setup, monkeypatch):
    from shared.vm_network_profile import NETWORK_PROFILE
    from tests.test_vm_resource_template import shipped_template

    image = "registry.example/srw-vm@sha256:" + "a" * 64
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_IMAGE_ALLOWLIST", image)
    monkeypatch.setattr(settings, "VM_NODE_SELECTOR", {})
    monkeypatch.setattr(settings, "VM_TOLERATIONS", [])
    monkeypatch.setattr(
        settings, "_inject_ssh_host_key", lambda value: (value, "SHA256:" + "A" * 43)
    )
    ctrl, api, authority, payload = setup
    ctrl.template_text = shipped_template()
    ctrl.cloud_init_text = "#cloud-config"
    ctrl.render_template = settings.VMController.render_template.__get__(ctrl)
    request = {
        **authority.row["request"],
        "vm_image": image,
        "network_profile": NETWORK_PROFILE,
        "cpu_cores": 2,
        "memory": "512Mi",
        "disk_size": "32Gi",
    }
    resolved = resolve_creation_configuration(ctrl, request)
    authority.row.update(resolved)
    payload.update(resolved["request"])
    payload["creation_retry"].update(
        request_digest=resolved["request_digest"],
        controller_configuration_digest=resolved["controller_configuration_digest"],
    )
    return ctrl, api, authority, payload


def test_authenticated_configuration_resolves_genuine_thread_owner(setup):
    ctrl, _, authority, _ = setup
    request = {
        **authority.row["request"],
        "job_id": str(uuid4()),
        "entity_type": "thread",
    }
    resolved = resolve_creation_configuration(ctrl, request)
    assert resolved["request"]["entity_type"] == "thread"
    assert resolved["request"]["job_id"] == request["job_id"]


@pytest.mark.asyncio
async def test_protocol_authority_denial_performs_no_job_effect(setup):
    ctrl, api, authority, payload = setup
    authority.deny = True
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "creation_pending"
    assert api.writes == []


@pytest.mark.asyncio
async def test_new_disk_secret_vm_each_have_one_durable_effect(setup):
    ctrl, api, authority, payload = setup
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "created"
    assert api.writes == ["Lease", "DataVolume", "Secret", "VirtualMachine"]
    assert authority.settled
    assert [x["state"] for x in authority.row["effects"]] == ["observed"] * 3


@pytest.mark.asyncio
async def test_profile_only_v1_reaches_first_vm_effect_with_ordinary_resolver(
    profiled_setup,
):
    from shared.vm_network_profile import NETWORK_DATA

    ctrl, api, authority, payload = profiled_setup
    resolved = resolve_creation_configuration(ctrl, authority.row["request"])
    assert resolved["controller_configuration"]["version"] == 1
    assert "resource_admission" not in resolved["controller_configuration"]

    result = await ctrl._do_create_serialized(payload)

    assert result["status"] == "created", (
        api.writes,
        [effect["state"] for effect in authority.row["effects"]],
        result,
    )
    assert api.writes == ["Lease", "DataVolume", "Secret", "VirtualMachine"]
    cloud = next(
        volume["cloudInitNoCloud"]
        for volume in api.read("VirtualMachine", "agent-vm-" + payload["job_id"])[
            "spec"
        ]["template"]["spec"]["volumes"]
        if "cloudInitNoCloud" in volume
    )
    assert cloud["networkData"] == NETWORK_DATA


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["missing", "changed"])
async def test_profile_only_refuses_bad_network_data_before_vm_post(
    profiled_setup, mutation
):
    ctrl, api, authority, payload = profiled_setup
    original = ctrl.render_template

    def changed_render(*args):
        manifest = original(*args)
        cloud = next(
            volume["cloudInitNoCloud"]
            for volume in manifest["spec"]["template"]["spec"]["volumes"]
            if "cloudInitNoCloud" in volume
        )
        if mutation == "missing":
            cloud.pop("networkData")
        else:
            cloud["networkData"] = cloud["networkData"].replace("enp1s0", "eth0")
        return manifest

    ctrl.render_template = changed_render
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "creation_attention"
    assert api.writes == ["Lease", "DataVolume", "Secret"]
    assert [
        effect["carrier_intent"]["effect_kind"] for effect in authority.row["effects"]
    ] == ["rootdisk", "cloud_init"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ["policy", "missing_configuration", "image", "request", "template", "effect"],
)
async def test_profile_only_refuses_identity_drift_before_vm_post(
    profiled_setup, field, monkeypatch
):
    from vm_controller.creation_actuation import CreationActuator

    ctrl, api, authority, payload = profiled_setup
    if field == "policy":
        authority.row["controller_configuration"]["network_profile_policy"]["image"] = (
            "registry.example/other@sha256:" + "b" * 64
        )
    elif field == "missing_configuration":
        authority.row.pop("controller_configuration")
    elif field == "image":
        payload["vm_image"] = "registry.example/other@sha256:" + "b" * 64
    elif field == "request":
        payload["disk_size"] = "64Gi"
    elif field == "template":
        ctrl.template_text += "\n# changed"
    else:
        original = CreationActuator.body

        async def changed_body(self, row, values):
            body = await original(self, row, values)
            if values["effect_kind"] == "vm":
                body["metadata"]["annotations"]["srw.io/vm-create-effect-nonce"] = str(
                    uuid4()
                )
            return body

        monkeypatch.setattr(CreationActuator, "body", changed_body)
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "creation_attention"
    assert "VirtualMachine" not in api.writes
    assert not any(
        effect["carrier_intent"]["effect_kind"] == "vm"
        for effect in authority.row["effects"]
    )


@pytest.mark.asyncio
async def test_creation_attention_logs_only_allowlisted_reason_code(
    setup, monkeypatch, caplog
):
    from vm_controller.creation_actuation import CreationActuator, CreationUnproven

    ctrl, _api, _authority, payload = setup
    actuator = CreationActuator(ctrl)

    async def recognized(_payload):
        raise CreationUnproven("creation_network_profile_unproven")

    monkeypatch.setattr(actuator, "_run", recognized)
    result = await actuator.run(payload)
    assert result["status"] == "creation_attention"
    assert result["reason"] == "creation_evidence_unproven"
    assert "creation_network_profile_unproven" in caplog.text

    caplog.clear()

    async def unknown(_payload):
        raise CreationUnproven("token=private-auth-value")

    monkeypatch.setattr(actuator, "_run", unknown)
    result = await actuator.run(payload)
    assert result["reason"] == "creation_evidence_unproven"
    assert "token=private-auth-value" not in caplog.text
    assert "creation_network_profile_unproven" not in caplog.text

    caplog.clear()

    async def arbitrary(_payload):
        raise ValueError("https://private.example/?token=private-auth-value")

    monkeypatch.setattr(actuator, "_run", arbitrary)
    result = await actuator.run(payload)
    assert result["reason"] == "creation_evidence_unproven"
    assert "private-auth-value" not in caplog.text

    async def malformed(_payload):
        raise CreationUnproven({"token": "private-auth-value"})

    monkeypatch.setattr(actuator, "_run", malformed)
    result = await actuator.run(payload)
    assert result["reason"] == "creation_evidence_unproven"
    assert "private-auth-value" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_at", "error", "stage", "family"),
    [
        (
            "publish",
            ValueError("token=private-auth-value"),
            "vm_carrier_publish",
            "value_error",
        ),
        (
            "previous",
            KeyError("token=private-auth-value"),
            "vm_postpublish_previous",
            "key_error",
        ),
        (
            "disk",
            TypeError("token=private-auth-value"),
            "vm_postpublish_disk",
            "type_error",
        ),
        (
            "absent",
            ValueError("token=private-auth-value"),
            "vm_postpublish_absence",
            "value_error",
        ),
        ("body", TypeError("token=private-auth-value"), "vm_render_body", "type_error"),
        (
            "manifest",
            ValueError("token=private-auth-value"),
            "vm_final_manifest",
            "value_error",
        ),
        ("grant", KeyError("token=private-auth-value"), "vm_begin_effect", "key_error"),
    ],
)
async def test_vm_creation_run_logs_closed_stage_and_family_without_secret_or_grant(
    profiled_setup,
    monkeypatch,
    caplog,
    failure_at,
    error,
    stage,
    family,
):
    from vm_controller.creation_actuation import CreationActuator

    ctrl, api, authority, payload = profiled_setup
    actuator = CreationActuator(ctrl)
    original_publish = CreationActuator.publish
    original_previous = CreationActuator.exact_previous
    original_disk = CreationActuator.disk
    original_absent = CreationActuator.require_vm_absent
    original_body = CreationActuator.body
    original_authority = actuator.authority
    postpublish_previous_complete = False

    def vm_carrier_exists():
        name = "srw-cleanup-" + authority.row["creation_admission_id"].replace("-", "")
        lease = api.objects.get(("Lease", name))
        return (
            lease is not None
            and verify_creation_carrier(
                lease,
                secret=SECRET,
            )["effect_kind"]
            == "vm"
        )

    async def publish(self, values, prior=None):
        nonlocal postpublish_previous_complete
        if failure_at == "publish" and values["effect_kind"] == "vm":
            raise error
        if values["effect_kind"] == "vm":
            postpublish_previous_complete = False
        return await original_publish(self, values, prior=prior)

    async def previous(self, row, lease):
        nonlocal postpublish_previous_complete
        if failure_at == "previous" and vm_carrier_exists():
            raise error
        result = await original_previous(self, row, lease)
        if vm_carrier_exists():
            postpublish_previous_complete = True
        return result

    async def disk(self, row, *, expected=None, require_attachment=True):
        if failure_at == "disk" and postpublish_previous_complete:
            raise error
        return await original_disk(
            self,
            row,
            expected=expected,
            require_attachment=require_attachment,
        )

    async def absent(self, row):
        if failure_at == "absent" and vm_carrier_exists():
            raise error
        return await original_absent(self, row)

    async def body(self, row, values):
        if failure_at == "body" and values["effect_kind"] == "vm":
            raise error
        return await original_body(self, row, values)

    async def authority_call(method, **values):
        if failure_at == "grant" and method == "begin-effect" and vm_carrier_exists():
            raise error
        return await original_authority(method, **values)

    monkeypatch.setattr(CreationActuator, "publish", publish)
    monkeypatch.setattr(CreationActuator, "exact_previous", previous)
    monkeypatch.setattr(CreationActuator, "disk", disk)
    monkeypatch.setattr(CreationActuator, "require_vm_absent", absent)
    monkeypatch.setattr(CreationActuator, "body", body)
    monkeypatch.setattr(actuator, "authority", authority_call)
    if failure_at == "manifest":

        def reject_manifest(*_args, **_kwargs):
            raise error

        monkeypatch.setattr(
            "shared.vm_resource_manifest.validate_final_vm_manifest",
            reject_manifest,
        )

    result = await actuator.run(payload)

    assert result["status"] == "creation_attention"
    assert result["reason"] == "creation_evidence_unproven"
    assert api.writes == ["Lease", "DataVolume", "Secret"]
    assert [
        effect["carrier_intent"]["effect_kind"] for effect in authority.row["effects"]
    ] == [
        "rootdisk",
        "cloud_init",
    ]
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "vm_controller.creation_actuation"
    ]
    assert len(messages) == 1
    assert f"stage={stage}" in messages[0]
    assert f"family={family}" in messages[0]
    assert "location=creation_actuation._run:" in messages[0]
    assert "private-auth-value" not in caplog.text


@pytest.mark.asyncio
async def test_creation_source_validation_refusal_has_closed_stage_without_grant(
    profiled_setup,
    monkeypatch,
    caplog,
):
    from vm_controller.creation_actuation import CreationActuator
    from vm_controller.creation_sources import GoldenSources

    ctrl, api, authority, payload = profiled_setup

    async def reject_source(self, row, source):
        raise KeyError("token=private-auth-value")

    monkeypatch.setattr(GoldenSources, "validate", reject_source)
    result = await CreationActuator(ctrl).run(payload)

    assert result["status"] == "creation_attention"
    assert result["reason"] == "creation_evidence_unproven"
    assert api.writes == ["Lease"]
    assert authority.row["effects"] == []
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "vm_controller.creation_actuation"
    ]
    assert len(messages) == 1
    assert "stage=rootdisk_source_validate" in messages[0]
    assert "family=key_error" in messages[0]
    assert "location=creation_actuation._run:" in messages[0]
    assert "private-auth-value" not in caplog.text


@pytest.mark.asyncio
async def test_creation_run_stages_are_isolated_across_concurrent_invocations(
    profiled_setup,
    monkeypatch,
    caplog,
):
    from vm_controller.creation_actuation import CreationActuator

    ctrl, api, authority, payload = profiled_setup
    actuator = CreationActuator(ctrl)
    original_body = CreationActuator.body

    async def seed_previous_effects(self, row, values):
        if values["effect_kind"] == "vm":
            raise ValueError("seed-only refusal")
        return await original_body(self, row, values)

    monkeypatch.setattr(CreationActuator, "body", seed_previous_effects)
    assert (await actuator.run(payload))["status"] == "creation_attention"
    assert len(authority.row["effects"]) == 2
    caplog.clear()
    entered_body = asyncio.Event()
    release_body = asyncio.Event()
    original_authority = actuator.authority

    async def concurrent_body(self, row, values):
        if (
            values["effect_kind"] == "vm"
            and asyncio.current_task().get_name() == "body-task"
        ):
            entered_body.set()
            await release_body.wait()
            raise ValueError("token=private-auth-value")
        return await original_body(self, row, values)

    async def concurrent_authority(method, **values):
        if (
            method == "begin-effect"
            and asyncio.current_task().get_name() == "grant-task"
        ):
            release_body.set()
            raise TypeError("token=private-auth-value")
        return await original_authority(method, **values)

    monkeypatch.setattr(CreationActuator, "body", concurrent_body)
    monkeypatch.setattr(actuator, "authority", concurrent_authority)
    body_task = asyncio.create_task(actuator.run(payload), name="body-task")
    await asyncio.wait_for(entered_body.wait(), timeout=5)
    grant_task = asyncio.create_task(actuator.run(payload), name="grant-task")
    results = await asyncio.wait_for(asyncio.gather(body_task, grant_task), timeout=5)

    assert [result["status"] for result in results] == [
        "creation_attention",
        "creation_attention",
    ]
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "vm_controller.creation_actuation"
    ]
    assert len(messages) == 2
    assert any(
        "stage=vm_render_body" in message and "family=value_error" in message
        for message in messages
    )
    assert any(
        "stage=vm_begin_effect" in message and "family=type_error" in message
        for message in messages
    )
    assert "private-auth-value" not in caplog.text
    assert api.writes == ["Lease", "DataVolume", "Secret"]
    assert len(authority.row["effects"]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["DataVolume", "Secret", "VirtualMachine"])
async def test_lost_api_reply_replays_only_observation(setup, kind):
    ctrl, api, authority, payload = setup
    api.lost.add(kind)
    await ctrl._do_create_serialized(payload)
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "created"
    assert api.writes.count(kind) == 1
    assert authority.settled
    assert (
        api.read("Secret", "agent-vm-" + payload["job_id"] + "-cloudinit")[
            "stringData"
        ]["userdata"]
        == "private-host-key"
    )


@pytest.mark.asyncio
async def test_lost_grant_never_creates_from_absence(setup):
    ctrl, api, authority, payload = setup
    authority.lost_grant = True
    await ctrl._do_create_serialized(payload)
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "creation_pending"
    assert api.writes == ["Lease"]


@pytest.mark.asyncio
async def test_completed_source_missing_vm_never_recreates(setup):
    ctrl, api, authority, payload = setup
    await ctrl._do_create_serialized(payload)
    del api.objects["VirtualMachine", "agent-vm-" + payload["job_id"]]
    writes = list(api.writes)
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "creation_attention"
    assert api.writes == writes


@pytest.mark.asyncio
async def test_current_configuration_drift_cannot_start_effect(setup, monkeypatch):
    ctrl, api, authority, payload = setup
    monkeypatch.setattr(settings, "VM_STORAGE_CLASS", "changed")
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "creation_attention"
    assert api.writes == []


@pytest.mark.asyncio
async def test_two_controller_handlers_share_one_stage_nonce(setup):
    import asyncio

    ctrl, api, authority, payload = setup
    await asyncio.gather(
        ctrl._do_create_serialized(payload), ctrl._do_create_serialized(payload)
    )
    # An observer may stop while its winning peer is still in flight.
    await ctrl._do_create_serialized(payload)
    assert api.writes.count("DataVolume") == 1
    assert api.writes.count("Secret") == 1
    assert api.writes.count("VirtualMachine") == 1


@pytest.mark.asyncio
async def test_exact_late_vm_adoption_survives_cancel_and_configuration_drift(
    setup, monkeypatch
):
    ctrl, api, authority, payload = setup
    api.lost.add("VirtualMachine")
    await ctrl._do_create_serialized(payload)
    authority.row["state"] = "cancel_requested"
    authority.deny = True
    monkeypatch.setattr(settings, "VM_STORAGE_CLASS", "changed")
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "created"
    assert api.writes.count("VirtualMachine") == 1
    assert authority.settled


@pytest.mark.asyncio
async def test_disk_replacement_after_grant_refuses_vm_api_write(setup):
    ctrl, api, authority, payload = setup
    original = authority.call

    async def replace_after_grant(path, body, *, operation):
        result = await original(path, body, operation=operation)
        if (
            path.endswith("begin-effect")
            and verify_creation_carrier(body["carrier"], secret=SECRET)["effect_kind"]
            == "vm"
        ):
            api.objects[
                "PersistentVolumeClaim", "agent-vm-" + payload["job_id"] + "-rootdisk"
            ]["metadata"]["uid"] = str(uuid4())
        return result

    ctrl._workspace_cleanup_authority_request = replace_after_grant
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "creation_attention"
    assert "VirtualMachine" not in api.writes
    assert authority.row["effects"][-1]["state"] == "issued"


@pytest.mark.asyncio
async def test_recovery_pin_refuses_next_stage_before_secret_write(setup):
    ctrl, api, authority, payload = setup
    api.lost.add("DataVolume")
    await ctrl._do_create_serialized(payload)
    pvc = api.read(
        "PersistentVolumeClaim", "agent-vm-" + payload["job_id"] + "-rootdisk"
    )
    ctrl._active_recovery_pins.return_value = ({"pvc_uid": pvc["metadata"]["uid"]},)
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "creation_attention"
    assert "Secret" not in api.writes


@pytest.mark.asyncio
async def test_protocol_objects_retain_a2_disk_observation_associations(setup):
    from vm_controller.provisioning_observation import build_provisioning_observation

    ctrl, api, authority, payload = setup
    await ctrl._do_create_serialized(payload)
    name = "agent-vm-" + payload["job_id"]
    observed = build_provisioning_observation(
        vm=api.read("VirtualMachine", name),
        vmi=None,
        datavolume=api.read("DataVolume", name + "-rootdisk"),
        pvc=api.read("PersistentVolumeClaim", name + "-rootdisk"),
        namespace=settings.VM_NAMESPACE,
        owner_kind="job",
        owner_id=payload["job_id"],
        generation=payload["provision_generation"],
        rootdisk_name=name + "-rootdisk",
        rootdisk_owner_kind="job",
        rootdisk_owner_id=payload["job_id"],
    )
    assert observed["disk_mode"] == "clone"
    assert observed["rootdisk_dv_uid"] is not None
    assert observed["rootdisk_pvc_uid"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("bound", [False, True])
@pytest.mark.parametrize("golden_enabled", [False, True])
async def test_retained_exact_disk_reuses_same_permit_without_dv_write(
    setup, bound, golden_enabled, monkeypatch
):
    from shared.vm_workspace_storage import storage_labels, storage_name

    ctrl, api, authority, payload = setup
    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", golden_enabled)
    resolved = resolve_creation_configuration(ctrl, authority.row["request"])
    authority.row.update(resolved)
    payload["creation_retry"]["controller_configuration_digest"] = resolved[
        "controller_configuration_digest"
    ]
    job = payload["job_id"]
    name = "agent-vm-" + job + "-rootdisk"
    pvc_uid = str(uuid4())
    binding = None
    labels = {"srw.io/owner-kind": "job", "srw.io/owner-id": job}
    if bound:
        ctrl.k8s_client.list_namespaced_custom_object = lambda **kw: {
            "items": [
                deepcopy(obj)
                for (kind, _), obj in api.objects.items()
                if kind
                == {
                    "virtualmachines": "VirtualMachine",
                    "virtualmachineinstances": "VirtualMachineInstance",
                }[kw["plural"]]
            ]
        }
        ctrl.core_api.list_namespaced_pod = lambda **kw: SimpleNamespace(items=[])
        binding = {
            "uid": str(uuid4()),
            "owner_id": job,
            "owner_kind": "job",
            "generation": 2,
            "pvc_uid": pvc_uid,
        }
        name = storage_name(binding)
        labels.update(storage_labels(binding, job))
        api.objects["Lease", name] = {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": name,
                "uid": str(uuid4()),
                "resourceVersion": "1",
                "namespace": settings.VM_NAMESPACE,
                "labels": storage_labels(binding, job),
            },
        }
        request = {k: v for k, v in payload.items() if k != "creation_retry"}
        request["workspace_storage"] = binding
        resolved = resolve_creation_configuration(ctrl, request)
        authority.row.update(resolved)
        payload.update(resolved["request"])
        payload["creation_retry"].update(
            {
                key: resolved[key]
                for key in ("request_digest", "controller_configuration_digest")
            }
        )
    dv_uid = str(uuid4())
    metadata = {
        "name": name,
        "namespace": settings.VM_NAMESPACE,
        "uid": dv_uid,
        "labels": labels,
    }
    api.objects["DataVolume", name] = {
        "apiVersion": "cdi.kubevirt.io/v1beta1",
        "kind": "DataVolume",
        "metadata": metadata,
        "status": {"phase": "Succeeded"},
    }
    api.objects["PersistentVolumeClaim", name] = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            **deepcopy(metadata),
            "uid": pvc_uid,
            "ownerReferences": [
                {"kind": "DataVolume", "uid": dv_uid, "controller": True}
            ],
        },
    }
    authority.row["expected_pvc_uid"] = pvc_uid
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "created"
    assert "DataVolume" not in api.writes
    assert result["rootdisk_pvc_uid"] == pvc_uid
    assert authority.row["effects"][1 if bound else 0]["evidence"]["uid"] == dv_uid


@pytest.mark.asyncio
async def test_periodic_carrier_observer_adopts_after_lost_vm_reply_without_post(setup):
    from vm_controller.creation_actuation import (
        carrier_record,
        reconcile_creation_carrier,
    )

    ctrl, api, authority, payload = setup
    api.lost.add("VirtualMachine")
    await ctrl._do_create_serialized(payload)
    lease = api.read(
        "Lease",
        "srw-cleanup-" + authority.row["creation_admission_id"].replace("-", ""),
    )
    carrier = carrier_record(lease, secret=SECRET)
    writes = list(api.writes)
    await reconcile_creation_carrier(ctrl, carrier)
    await reconcile_creation_carrier(ctrl, carrier)
    assert authority.settled
    assert api.writes == writes


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [422, 500])
async def test_only_matching_definitive_api_status_resolves_effect(setup, status):
    import json

    ctrl, api, authority, payload = setup
    original = api.create

    def reject(body):
        if body["kind"] == "DataVolume":
            error = ApiException(status=status)
            error.body = json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Failure",
                    "code": 422,
                    "reason": "Invalid",
                }
            )
            raise error
        return original(body)

    api.create = reject
    await ctrl._do_create_serialized(payload)
    assert authority.row["effects"][-1]["state"] == (
        "rejected" if status == 422 else "issued"
    )


@pytest.mark.asyncio
async def test_protocol_specific_endpoint_cannot_fall_back_to_legacy(setup):
    import json
    from shared.vm_lifecycle_auth import sign_payload, verify_payload, AUTH_FIELD

    ctrl, api, authority, payload = setup
    ctrl._verify_lifecycle_request = AsyncMock(return_value=True)
    missing = {k: v for k, v in payload.items() if k != "creation_retry"}
    request = sign_payload(
        missing, direction="request", operation="creation_retry_create", secret=SECRET
    )
    response = await ctrl.http_creation_retry(
        SimpleNamespace(json=AsyncMock(return_value=request))
    )
    assert response.status == 400
    assert api.writes == []
    ctrl._verify_lifecycle_request.assert_awaited_once_with(
        request, "creation_retry_create", mutating=True
    )
    assert verify_payload(
        json.loads(response.text),
        direction="response",
        operation="creation_retry_create",
        secret=SECRET,
        expected_correlation_id=request[AUTH_FIELD]["request_id"],
    )


@pytest.mark.asyncio
async def test_old_controller_route_table_returns_404_without_legacy_create(setup):
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from shared.vm_lifecycle_auth import sign_payload

    ctrl, api, authority, payload = setup
    legacy = AsyncMock(
        side_effect=AssertionError("protocol must not invoke legacy create")
    )
    app = web.Application()
    app.router.add_post("/vms", legacy)
    async with TestClient(TestServer(app)) as client:
        response = await client.post(
            "/vm-creation/create",
            json=sign_payload(
                payload,
                direction="request",
                operation="creation_retry_create",
                secret=SECRET,
            ),
        )
        assert response.status == 404
    legacy.assert_not_awaited()
    assert api.writes == []


@pytest.mark.asyncio
async def test_unsealed_creation_carrier_does_not_break_other_cleanup_inventory(setup):
    from shared.vm_creation_issuance import CREATION_INTENT_ANNOTATION

    ctrl, api, authority, payload = setup
    ctrl.coordination_api.list_namespaced_lease = lambda **kw: {
        "items": [
            {
                "metadata": {
                    "labels": {"srw.io/vm-workspace-cleanup-carrier": "true"},
                    "annotations": {CREATION_INTENT_ANNOTATION: "{}"},
                }
            }
        ]
    }
    # An unsealed publication is never source evidence or actuation authority;
    # its database reservation still excludes conflicting owner/PVC cleanup.
    assert await ctrl._list_workspace_cleanup_carriers() == ()
    assert api.writes == []


def foreign_vm(payload):
    return {
        "apiVersion": "kubevirt.io/v1",
        "kind": "VirtualMachine",
        "metadata": {
            "name": "agent-vm-" + payload["job_id"],
            "namespace": settings.VM_NAMESPACE,
            "uid": str(uuid4()),
            "labels": {
                "srw.io/owner-kind": "job",
                "srw.io/owner-id": payload["job_id"],
            },
            "annotations": {"srw.io/provision-generation": str(uuid4())},
        },
        "spec": {
            "template": {
                "spec": {
                    "volumes": [
                        {
                            "name": "rootdisk",
                            "dataVolume": {
                                "name": "agent-vm-" + payload["job_id"] + "-rootdisk"
                            },
                        },
                        {
                            "name": "cloud-init",
                            "cloudInitNoCloud": {
                                "secretRef": {
                                    "name": "agent-vm-"
                                    + payload["job_id"]
                                    + "-cloudinit"
                                }
                            },
                        },
                    ]
                }
            }
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("same_generation", [False, True])
async def test_unproven_existing_vm_receives_no_new_dependencies(
    setup, same_generation
):
    ctrl, api, authority, payload = setup
    vm = foreign_vm(payload)
    if same_generation:
        vm["metadata"]["annotations"]["srw.io/provision-generation"] = payload[
            "provision_generation"
        ]
    api.objects["VirtualMachine", vm["metadata"]["name"]] = vm
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "creation_attention"
    assert api.writes == []
    assert authority.row["effects"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage,forbidden",
    [("rootdisk", "DataVolume"), ("cloud_init", "Secret"), ("vm", "VirtualMachine")],
)
async def test_unexpected_vm_after_grant_prevents_fresh_effect(setup, stage, forbidden):
    ctrl, api, authority, payload = setup
    original = authority.call

    async def appear_after_grant(path, body, *, operation):
        result = await original(path, body, operation=operation)
        if (
            path.endswith("begin-effect")
            and verify_creation_carrier(body["carrier"], secret=SECRET)["effect_kind"]
            == stage
        ):
            vm = foreign_vm(payload)
            api.objects["VirtualMachine", vm["metadata"]["name"]] = vm
        return result

    ctrl._workspace_cleanup_authority_request = appear_after_grant
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "creation_attention"
    assert forbidden not in api.writes
    assert authority.row["effects"][-1]["state"] == "issued"
