"""Initial Ready positive-stop wire must stay distinct from neverReady v1."""

import asyncio
from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from tests.test_vm_initial_ready_retention_binding import ready_pair
from tests.test_vm_job_retained_resume_protocol import (
    ready_candidate as continuation_candidate,
)
from tests.test_vm_controller import (
    PROVISION_GENERATION,
    SAMPLE_JOB_CONFIG,
    TestWorkspaceRecoveryControllerEvidence as _ControllerWire,
    _make_controller,
)

from shared.vm_pre_ssh_stop import (
    PRE_SSH_STOP_ANNOTATION,
    PRE_SSH_STOP_FINALIZER,
    valid_frozen_stop_candidate,
    valid_positive_stop_proof,
)
from tests.test_vm_pre_ssh_stop_protocol import candidate, proof, uid


def initial_ready_transport_payload():
    frozen, preflight = ready_pair()
    return {
        "action": "inspect_initial_ready",
        "job_id": frozen["job_id"],
        "provision_generation": frozen["provision_generation"],
        "expected_vm_uid": frozen["vm_uid"],
        "expected_pvc_uid": frozen["pvc_uid"],
        "parent_cleanup": {
            "admission_id": frozen["cleanup_admission_id"],
            "request_id": frozen["cleanup_request_id"],
            "intent_digest": frozen["cleanup_intent_digest"],
            "intent": {
                "owner_id": frozen["job_id"],
                "owner_kind": "job",
                "provision_generation": frozen["provision_generation"],
                "purge_disk": False,
                "pvc_uid": frozen["pvc_uid"],
                "resource": "vm_workspace",
                "source": "job_terminal_vm_release",
                "vm_uid": frozen["vm_uid"],
            },
            "retention_preflight": preflight,
        },
    }


@pytest.mark.asyncio
async def test_initial_ready_inspect_reaches_signed_http_stop_transport(monkeypatch):
    from orchestrator.services.vm_provisioner import VMProvisioner
    from orchestrator.services.vm_lifecycle_auth import (
        AUTH_FIELD,
        sign_payload,
        unsigned_payload,
        verify_payload,
    )

    payload = initial_ready_transport_payload()
    secret = b"test-ready-stop-secret-32-bytes-long!"
    requests = []

    class Client:
        async def post(self, path, *, json):
            requests.append(path)
            assert path == "/vm-pre-ssh-stop"
            assert verify_payload(
                json,
                direction="request",
                operation="pre-ssh-stop",
                secret=secret,
            )
            assert unsigned_payload(json) == payload
            reply = sign_payload(
                {
                    "job_id": payload["job_id"],
                    "provision_generation": payload["provision_generation"],
                    "status": "candidate",
                },
                direction="response",
                operation="pre-ssh-stop",
                secret=secret,
                correlation_id=json[AUTH_FIELD]["request_id"],
            )
            return httpx.Response(
                200,
                json=reply,
                request=httpx.Request("POST", "http://controller" + path),
            )

    monkeypatch.setattr(VMProvisioner, "_nats_available", property(lambda self: False))
    monkeypatch.setattr(VMProvisioner, "_http_available", property(lambda self: True))
    provisioner = VMProvisioner.__new__(VMProvisioner)
    provisioner._lifecycle_hmac_secret = secret
    provisioner._http_client = Client()
    result = await provisioner._request_pre_ssh_stop(payload)
    assert result == {
        "job_id": payload["job_id"],
        "provision_generation": payload["provision_generation"],
        "status": "candidate",
        "_identity_authenticated": True,
    }
    assert requests == ["/vm-pre-ssh-stop"]


@pytest.mark.asyncio
async def test_initial_ready_inspect_reaches_signed_nats_stop_transport(monkeypatch):
    from orchestrator.services import vm_provisioner as module
    from orchestrator.services.nats_bridge import NatsBridge
    from orchestrator.services.vm_provisioner import VMProvisioner
    from orchestrator.services.vm_lifecycle_auth import (
        AUTH_FIELD,
        sign_payload,
        unsigned_payload,
        verify_payload,
    )

    payload = initial_ready_transport_payload()
    secret = b"test-ready-stop-secret-32-bytes-long!"
    bridge = NatsBridge(url="nats://test.invalid")
    bridge._available = True
    bridge._orchestrator_id = "test"
    bridge._lifecycle_hmac_secret = secret
    subjects = []

    class NC:
        async def request(self, subject, raw, *, timeout):
            subjects.append(subject)
            assert timeout == 8.0
            sent = json.loads(raw)
            assert verify_payload(
                sent,
                direction="request",
                operation="pre-ssh-stop",
                secret=secret,
            )
            assert unsigned_payload(sent) == {
                **payload,
                "orchestrator_id": "test",
            }
            reply = sign_payload(
                {
                    "job_id": payload["job_id"],
                    "provision_generation": payload["provision_generation"],
                    "status": "candidate",
                },
                direction="response",
                operation="pre-ssh-stop",
                secret=secret,
                correlation_id=sent[AUTH_FIELD]["request_id"],
            )
            return SimpleNamespace(data=json.dumps(reply).encode())

    bridge._nc = NC()
    monkeypatch.setattr(module, "nats_bridge", bridge)
    monkeypatch.setattr(VMProvisioner, "_nats_available", property(lambda self: True))
    monkeypatch.setattr(VMProvisioner, "_http_available", property(lambda self: False))
    provisioner = VMProvisioner.__new__(VMProvisioner)
    provisioner._lifecycle_hmac_secret = secret
    provisioner._http_client = None
    result = await provisioner._request_pre_ssh_stop(payload)
    assert result == {
        "job_id": payload["job_id"],
        "provision_generation": payload["provision_generation"],
        "status": "candidate",
        "_identity_authenticated": True,
    }
    assert subjects == ["vm.lifecycle.pre_ssh_stop.test"]


@pytest.mark.asyncio
async def test_unknown_pre_ssh_stop_action_refuses_before_http_send(monkeypatch):
    from orchestrator.services.vm_provisioner import VMProvisioner

    payload = initial_ready_transport_payload()
    payload["action"] = "inspect_initial_ready_unowned"
    monkeypatch.setattr(VMProvisioner, "_nats_available", property(lambda self: False))
    monkeypatch.setattr(VMProvisioner, "_http_available", property(lambda self: True))

    class Client:
        async def post(self, path, *, json):
            pytest.fail("invalid action reached controller transport")

    provisioner = VMProvisioner.__new__(VMProvisioner)
    provisioner._lifecycle_hmac_secret = b"test-ready-stop-secret-32-bytes-long!"
    provisioner._http_client = Client()
    assert await provisioner._request_pre_ssh_stop(payload) is None


@pytest.mark.asyncio
async def test_initial_ready_inspect_requires_typed_parent_before_read(monkeypatch):
    from vm_controller import controller as module

    calls = []

    class Ctrl:
        async def _do_inspect_pre_ssh_stop(self, *args, **kwargs):
            calls.append(kwargs)
            return frozen

    frozen = ready_candidate()
    ctrl = Ctrl()
    request = {
        "action": "inspect_initial_ready",
        "job_id": frozen["job_id"],
        "provision_generation": frozen["provision_generation"],
        "expected_vm_uid": frozen["vm_uid"],
        "expected_pvc_uid": frozen["pvc_uid"],
    }
    result = await module.VMController._dispatch_pre_ssh_stop(ctrl, request)
    assert result["status"] == "identity_refused"
    assert calls == []

    frozen, preflight = ready_pair()
    monkeypatch.setattr(module, "VM_NAMESPACE", frozen["namespace"])
    parent = {
        "admission_id": frozen["cleanup_admission_id"],
        "request_id": frozen["cleanup_request_id"],
        "intent_digest": frozen["cleanup_intent_digest"],
        "intent": {
            "owner_id": frozen["job_id"],
            "owner_kind": "job",
            "provision_generation": frozen["provision_generation"],
            "purge_disk": False,
            "pvc_uid": frozen["pvc_uid"],
            "resource": "vm_workspace",
            "source": "job_terminal_vm_release",
            "vm_uid": frozen["vm_uid"],
        },
        "retention_preflight": preflight,
    }
    request.update(
        job_id=frozen["job_id"],
        provision_generation=frozen["provision_generation"],
        expected_vm_uid=frozen["vm_uid"],
        expected_pvc_uid=frozen["pvc_uid"],
        parent_cleanup=parent,
    )
    result = await module.VMController._dispatch_pre_ssh_stop(ctrl, request)
    assert result == {
        "job_id": frozen["job_id"],
        "provision_generation": frozen["provision_generation"],
        "status": "candidate",
        "frozen": frozen,
        "retention_preflight": preflight,
    }
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_initial_ready_stop_and_release_refuse_missing_preflight_before_effect(
    monkeypatch,
):
    from vm_controller import controller as module

    frozen = ready_candidate()
    monkeypatch.setattr(module, "VM_NAMESPACE", frozen["namespace"])
    called = []

    class Ctrl:
        async def _workspace_lifecycle(self, *args):
            called.append("lifecycle")

    ctrl = Ctrl()
    stopped = await module.VMController._do_pre_ssh_stop(
        ctrl, frozen, "sha256:" + "a" * 64
    )
    released = await module.VMController._do_release_pre_ssh_stop_finalizer(
        ctrl,
        frozen,
        "sha256:" + "a" * 64,
        ready_proof(frozen),
        process_zero_receipt_id=uid(15),
    )
    assert stopped == {"status": "identity_refused"}
    assert released == {"status": "identity_refused"}
    assert called == []


def ready_candidate() -> dict:
    frozen = candidate()
    frozen.update(
        kind="vm_initial_ready_positive_stop_candidate_v1",
        cleanup_admission_id=uid(8),
        cleanup_request_id=uid(9),
        cleanup_intent_digest="sha256:" + "b" * 64,
    )
    return frozen


def ready_proof(frozen: dict) -> dict:
    observed = proof(frozen)
    observed["kind"] = "vm_initial_ready_positive_stop_v1"
    return observed


@pytest.mark.asyncio
async def test_initial_ready_status_dispatch_keeps_continuation_ready_gate(monkeypatch):
    from vm_controller import controller as module

    initial = continuation_candidate()
    initial.pop("continuation_id")
    initial["kind"] = "vm_job_initial_ready_stop_candidate_v1"
    monkeypatch.setattr(module, "VM_NAMESPACE", initial["namespace"])

    async def inline(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", inline)
    ctrl = _make_controller(headscale_available=False)
    vm = {
        "metadata": {
            "name": "agent-vm-" + initial["job_id"],
            "uid": initial["vm_uid"],
            "labels": {
                "srw.io/owner-kind": "job",
                "srw.io/owner-id": initial["job_id"],
            },
            "annotations": {
                "srw.io/provision-generation": initial["provision_generation"]
            },
        },
        "status": {
            "created": True,
            "conditions": [{"type": "Ready", "status": "False"}],
        },
    }
    vmi = {
        "metadata": {"uid": initial["vmi_uid"]},
        "status": {
            "phase": "Running",
            "activePods": {initial["launcher_uid"]: "node8"},
        },
    }
    ctrl.k8s_client.get_namespaced_custom_object.side_effect = (
        lambda **kwargs: vm if kwargs["plural"] == module.KUBEVIRT_PLURAL else vmi
    )
    ctrl._rootdisk_pvc_uid = AsyncMock(return_value=initial["pvc_uid"])
    ctrl._provisioning_status_evidence = AsyncMock(return_value={})
    ctrl._qualify_ready_retention_preflight = AsyncMock(
        return_value={"qualified": True}
    )
    result = await ctrl._do_status(
        initial["job_id"],
        initial["provision_generation"],
        retained_ready_stop_candidate=initial,
    )
    assert result["ready"] is False
    assert result["retention_preflight"] == {"qualified": True}
    assert (
        ctrl._qualify_ready_retention_preflight.await_args.kwargs["require_ready"]
        is False
    )

    continuation = continuation_candidate()
    ctrl._qualify_ready_retention_preflight.reset_mock()
    await ctrl._do_status(
        initial["job_id"],
        initial["provision_generation"],
        retained_ready_stop_candidate=continuation,
    )
    assert (
        ctrl._qualify_ready_retention_preflight.await_args.kwargs["require_ready"]
        is True
    )


@pytest.mark.asyncio
async def test_initial_ready_delete_accepts_ssh_zero_or_halted_with_exact_custody(
    monkeypatch,
):
    from vm_controller import controller as module

    frozen, preflight = ready_pair()
    monkeypatch.setattr(module, "LIFECYCLE_HMAC_SECRET", b"test-secret")
    monkeypatch.setattr(module, "VM_NAMESPACE", frozen["namespace"])
    vm = {
        "metadata": {
            "name": frozen["vm_name"],
            "uid": frozen["vm_uid"],
            "labels": {"srw.io/owner-kind": "job", "srw.io/owner-id": frozen["job_id"]},
            "annotations": {
                "srw.io/provision-generation": frozen["provision_generation"]
            },
        },
        "spec": {"runStrategy": "RerunOnFailure"},
    }
    deleted = []
    fresh = [preflight]

    class K8s:
        def get_namespaced_custom_object(self, **kwargs):
            return vm

        def delete_namespaced_custom_object(self, **kwargs):
            deleted.append(kwargs)
            raise RuntimeError("delete_reached")

    class Ctrl:
        k8s_client = K8s()

        async def _rootdisk_pvc_probe(self, *args, **kwargs):
            return True, frozen["pvc_uid"]

        async def _qualify_cancel_retained_rootdisk(self, *args, **kwargs):
            return {"dv_uid": preflight["dv_uid"]}

        async def _qualify_ready_retention_preflight(self, *args, **kwargs):
            assert kwargs["require_ready"] is False
            return fresh[0]

    parent = {
        "admission_id": frozen["cleanup_admission_id"],
        "request_id": frozen["cleanup_request_id"],
        "intent_digest": frozen["cleanup_intent_digest"],
        "intent": {
            "owner_id": frozen["job_id"],
            "owner_kind": "job",
            "provision_generation": frozen["provision_generation"],
            "purge_disk": False,
            "pvc_uid": frozen["pvc_uid"],
            "resource": "vm_workspace",
            "source": "job_terminal_vm_release",
            "vm_uid": frozen["vm_uid"],
        },
        "retention_preflight": preflight,
    }
    kwargs = {
        "purge_disk": False,
        "provision_generation": frozen["provision_generation"],
        "expected_vm_uid": frozen["vm_uid"],
        "expected_rootdisk_pvc_uid": frozen["pvc_uid"],
        "parent_cleanup": parent,
    }
    with pytest.raises(RuntimeError, match="delete_reached"):
        await module.VMController._do_delete_serialized(
            Ctrl(), frozen["job_id"], **kwargs
        )
    assert len(deleted) == 1
    fresh[0] = None
    with pytest.raises(RuntimeError, match="initial Ready runtime or disk changed"):
        await module.VMController._do_delete_serialized(
            Ctrl(), frozen["job_id"], **kwargs
        )
    assert len(deleted) == 1
    vm["spec"]["runStrategy"] = "Halted"
    parent["retention_preflight"] = dict(preflight, dv_uid=uid(51))
    with pytest.raises(
        RuntimeError, match="initial Ready retained disk identity changed"
    ):
        await module.VMController._do_delete_serialized(
            Ctrl(), frozen["job_id"], **kwargs
        )
    assert len(deleted) == 1
    parent["retention_preflight"] = preflight
    with pytest.raises(RuntimeError, match="delete_reached"):
        await module.VMController._do_delete_serialized(
            Ctrl(), frozen["job_id"], **kwargs
        )
    assert len(deleted) == 2


@pytest.mark.asyncio
async def test_initial_ready_status_witness_is_distinct_and_exact(monkeypatch):
    from vm_controller import controller as module

    candidate = continuation_candidate()
    candidate.pop("continuation_id")
    candidate["kind"] = "vm_job_initial_ready_stop_candidate_v1"
    monkeypatch.setattr(module, "LIFECYCLE_HMAC_SECRET", b"test-secret")
    monkeypatch.setattr(module, "VM_NAMESPACE", candidate["namespace"])
    name = "agent-vm-" + candidate["job_id"]
    vm = {
        "metadata": {
            "name": name,
            "uid": candidate["vm_uid"],
            "labels": {
                "srw.io/owner-kind": "job",
                "srw.io/owner-id": candidate["job_id"],
            },
            "annotations": {
                "srw.io/provision-generation": candidate["provision_generation"],
                "srw.io/vm-create-request-id": candidate["request_id"],
            },
        },
        "status": {
            "created": True,
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }
    vmi = {
        "metadata": {"name": name, "uid": candidate["vmi_uid"]},
        "status": {"phase": "Running"},
    }
    pod = {
        "metadata": {"name": "virt-launcher-ready", "uid": candidate["launcher_uid"]},
        "spec": {"nodeName": "node8"},
        "status": {"phase": "Running"},
    }

    class K8s:
        def get_namespaced_custom_object(self, *, plural, **kwargs):
            return vm if plural == module.KUBEVIRT_PLURAL else vmi

    class Core:
        def list_namespaced_pod(self, **kwargs):
            return SimpleNamespace(items=[pod])

        def read_node(self, **kwargs):
            return {"metadata": {"uid": candidate["node_uid"]}}

    class Ctrl:
        k8s_client = K8s()
        core_api = Core()
        resource_inventory_collector = SimpleNamespace(
            namespace=candidate["namespace"], cluster_id=candidate["cluster_id"]
        )

        async def _qualify_cancel_retained_rootdisk(
            self, job_id, pvc_uid, *, allowed_runtime
        ):
            assert allowed_runtime["vm_uid"] == candidate["vm_uid"]
            return {
                "namespace": candidate["namespace"],
                "owner_id": job_id,
                "pvc_name": name + "-rootdisk",
                "pvc_uid": pvc_uid,
                "dv_uid": uid(50),
                "ownership": "standalone_dv",
                "deleting": False,
            }

    proof = await module.VMController._qualify_ready_retention_preflight(
        Ctrl(), candidate
    )
    assert proof["kind"] == "vm_job_initial_ready_preflight_v1"
    assert proof["stop_policy"] == "initial_ready_cancel_v1"
    assert proof["frozen"] == candidate
    assert "continuation_id" not in proof["frozen"]
    vm["status"]["conditions"] = [{"type": "Ready", "status": "False"}]
    assert (
        await module.VMController._qualify_ready_retention_preflight(Ctrl(), candidate)
        is None
    )
    assert (
        await module.VMController._qualify_ready_retention_preflight(
            Ctrl(), candidate, require_ready=False
        )
        == proof
    )
    foreign_kind = dict(candidate, kind="vm_job_retained_ready_stop_candidate_v1")
    assert (
        await module.VMController._qualify_ready_retention_preflight(
            Ctrl(), foreign_kind, require_ready=False
        )
        is None
    )


@pytest.mark.asyncio
async def test_initial_ready_halted_replay_proves_exact_terminal_vector(monkeypatch):
    from kubernetes.client.exceptions import ApiException

    async def inline(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", inline)
    controller = _make_controller(headscale_available=True)
    helper = _ControllerWire()
    vm, pod = helper.wire_pre_ssh_stop(controller)
    old = await controller._do_inspect_pre_ssh_stop(
        SAMPLE_JOB_CONFIG["job_id"],
        provision_generation=PROVISION_GENERATION,
        expected_vm_uid=helper.VM_UID,
        expected_pvc_uid=helper.PVC_UID,
    )
    frozen = dict(
        old,
        kind="vm_initial_ready_positive_stop_candidate_v1",
        cleanup_admission_id=uid(40),
        cleanup_request_id=uid(41),
        cleanup_intent_digest="sha256:" + "b" * 64,
    )
    _, preflight = ready_pair()
    preflight["frozen"].update(
        {
            key: frozen[key]
            for key in (
                "job_id",
                "provision_generation",
                "namespace",
                "vm_uid",
                "vmi_uid",
                "launcher_uid",
                "pvc_uid",
                "node_uid",
                "cleanup_request_id",
                "cleanup_intent_digest",
            )
        }
    )
    preflight.update(
        namespace=frozen["namespace"],
        owner_id=frozen["job_id"],
        pvc_name=f"agent-vm-{frozen['job_id']}-rootdisk",
        pvc_uid=frozen["pvc_uid"],
    )
    controller._qualify_cancel_retained_rootdisk = AsyncMock(
        return_value={
            key: preflight[key]
            for key in (
                "namespace",
                "owner_id",
                "pvc_name",
                "pvc_uid",
                "dv_uid",
                "ownership",
                "deleting",
            )
        }
    )
    digest = "sha256:" + "a" * 64
    vm["spec"]["runStrategy"] = "Halted"
    vm["metadata"]["generation"] = frozen["vm_generation"] + 1
    vm["metadata"]["annotations"]["srw.io/vm-create-request-id"] = preflight["frozen"][
        "request_id"
    ]
    vm["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] = 30
    pod["metadata"].update(
        annotations={PRE_SSH_STOP_ANNOTATION: digest},
        finalizers=[PRE_SSH_STOP_FINALIZER],
        deletionGracePeriodSeconds=0,
        deletionTimestamp="2026-10-07T12:01:00Z",
    )
    pod["spec"]["terminationGracePeriodSeconds"] = 60
    pod["status"]["phase"] = "Succeeded"
    for status in pod["status"]["containerStatuses"]:
        status["state"] = {
            "terminated": {
                "containerID": status["containerID"],
                "startedAt": "2026-10-07T11:00:00Z",
                "finishedAt": "2026-10-07T12:00:00Z",
                "reason": "Completed",
            }
        }
    controller.core_api.read_namespaced_pod.return_value = pod

    def vm_or_missing_vmi(**kwargs):
        if kwargs["plural"] == "virtualmachineinstances":
            raise ApiException(status=404)
        return vm

    controller.k8s_client.get_namespaced_custom_object.side_effect = vm_or_missing_vmi
    result = await controller._do_pre_ssh_stop(
        frozen, digest, retention_preflight=preflight
    )
    assert result["status"] == "positive_terminal_proof"
    assert result["terminal_evidence"]["kind"] == "vm_initial_ready_positive_stop_v1"
    assert (
        result["terminal_evidence"]["containers"] == ready_proof(frozen)["containers"]
    )
    controller.core_api.patch_namespaced_pod.assert_not_called()
    controller.k8s_client.patch_namespaced_custom_object.assert_not_called()
    changed = dict(preflight, dv_uid=uid(42))
    controller.core_api.patch_namespaced_pod.reset_mock()
    release = await controller._do_release_pre_ssh_stop_finalizer(
        frozen,
        digest,
        result["terminal_evidence"],
        process_zero_receipt_id=uid(43),
        retention_preflight=changed,
    )
    assert release == {"status": "identity_refused"}
    controller.core_api.patch_namespaced_pod.assert_not_called()
    release = await controller._do_release_pre_ssh_stop_finalizer(
        frozen,
        digest,
        result["terminal_evidence"],
        process_zero_receipt_id=uid(43),
        retention_preflight=preflight,
    )
    assert release == {"status": "finalizer_pending"}
    controller.core_api.patch_namespaced_pod.assert_called_once()


@pytest.mark.asyncio
async def test_initial_ready_inspection_freezes_actual_ready_runtime_and_parent(
    monkeypatch,
):
    async def inline(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", inline)
    controller = _make_controller(headscale_available=True)
    helper = _ControllerWire()
    vm, pod = helper.wire_pre_ssh_stop(controller)
    old = await controller._do_inspect_pre_ssh_stop(
        SAMPLE_JOB_CONFIG["job_id"],
        provision_generation=PROVISION_GENERATION,
        expected_vm_uid=helper.VM_UID,
        expected_pvc_uid=helper.PVC_UID,
    )
    ready, preflight = ready_pair()
    ready.update(
        {
            key: old[key]
            for key in (
                "job_id",
                "provision_generation",
                "namespace",
                "vm_name",
                "vm_uid",
                "vmi_uid",
                "launcher_name",
                "launcher_uid",
                "pvc_uid",
                "node_name",
                "node_uid",
                "vm_resource_version",
                "vm_generation",
                "launcher_resource_version",
                "containers",
            )
        }
    )
    authority = preflight["frozen"]
    authority.update(
        {
            key: ready[key]
            for key in (
                "job_id",
                "provision_generation",
                "namespace",
                "vm_uid",
                "vmi_uid",
                "launcher_uid",
                "pvc_uid",
                "node_uid",
                "cleanup_request_id",
                "cleanup_intent_digest",
            )
        }
    )
    preflight.update(
        namespace=ready["namespace"],
        owner_id=ready["job_id"],
        pvc_name=f"agent-vm-{ready['job_id']}-rootdisk",
        pvc_uid=ready["pvc_uid"],
    )
    parent = {
        "admission_id": ready["cleanup_admission_id"],
        "request_id": ready["cleanup_request_id"],
        "intent_digest": ready["cleanup_intent_digest"],
        "intent": {
            "owner_id": ready["job_id"],
            "owner_kind": "job",
            "provision_generation": ready["provision_generation"],
            "purge_disk": False,
            "pvc_uid": ready["pvc_uid"],
            "resource": "vm_workspace",
            "source": "job_terminal_vm_release",
            "vm_uid": ready["vm_uid"],
        },
        "retention_preflight": preflight,
    }
    vm["status"].update(created=True, conditions=[{"type": "Ready", "status": "True"}])
    vm["metadata"]["annotations"]["srw.io/vm-create-request-id"] = authority[
        "request_id"
    ]
    controller._qualify_ready_retention_preflight = AsyncMock(return_value=preflight)
    actual = await controller._do_inspect_pre_ssh_stop(
        ready["job_id"],
        provision_generation=ready["provision_generation"],
        expected_vm_uid=ready["vm_uid"],
        expected_pvc_uid=ready["pvc_uid"],
        parent_cleanup=parent,
    )
    assert actual == ready
    controller._qualify_ready_retention_preflight.assert_awaited_once()
    assert (
        await controller._do_inspect_pre_ssh_stop(
            ready["job_id"],
            provision_generation=ready["provision_generation"],
            expected_vm_uid=ready["vm_uid"],
            expected_pvc_uid=ready["pvc_uid"],
        )
        is None
    )
    vm["status"]["conditions"] = [{"type": "Ready", "status": "False"}]
    assert (
        await controller._do_inspect_pre_ssh_stop(
            ready["job_id"],
            provision_generation=ready["provision_generation"],
            expected_vm_uid=ready["vm_uid"],
            expected_pvc_uid=ready["pvc_uid"],
            parent_cleanup=parent,
        )
        == ready
    )
    assert (
        await controller._do_inspect_pre_ssh_stop(
            ready["job_id"],
            provision_generation=ready["provision_generation"],
            expected_vm_uid=ready["vm_uid"],
            expected_pvc_uid=ready["pvc_uid"],
            parent_cleanup=dict(parent, request_id=uid(31)),
        )
        is None
    )
    controller.core_api.patch_namespaced_pod.assert_not_called()
    controller.k8s_client.patch_namespaced_custom_object.assert_not_called()
    controller.core_api.read_namespaced_pod.return_value = pod
    controller._qualify_cancel_retained_rootdisk = AsyncMock(
        return_value={
            key: preflight[key]
            for key in (
                "namespace",
                "owner_id",
                "pvc_name",
                "pvc_uid",
                "dv_uid",
                "ownership",
                "deleting",
            )
        }
    )
    stopped = await controller._do_pre_ssh_stop(
        ready, "sha256:" + "a" * 64, retention_preflight=preflight
    )
    assert stopped == {"status": "finalizer_pending"}
    controller.core_api.patch_namespaced_pod.assert_called_once()
    controller.k8s_client.patch_namespaced_custom_object.assert_not_called()


def test_initial_ready_positive_stop_requires_exact_distinct_kinds():
    frozen = ready_candidate()
    observed = ready_proof(frozen)
    assert valid_frozen_stop_candidate(frozen)
    assert valid_positive_stop_proof(
        frozen, observed, frozen_digest="sha256:" + "a" * 64
    )
    assert not valid_positive_stop_proof(
        candidate(), observed, frozen_digest="sha256:" + "a" * 64
    )
    assert not valid_positive_stop_proof(
        frozen, proof(frozen), frozen_digest="sha256:" + "a" * 64
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("cleanup_admission_id", None),
        ("cleanup_admission_id", "not-a-uuid"),
        ("cleanup_request_id", None),
        ("cleanup_request_id", "not-a-uuid"),
        ("cleanup_intent_digest", ""),
        ("cleanup_intent_digest", "sha256:" + "g" * 64),
    ],
)
def test_initial_ready_candidate_rejects_bad_cleanup_identity(field, value):
    frozen = ready_candidate()
    frozen[field] = value
    assert not valid_frozen_stop_candidate(frozen)


def test_initial_ready_candidate_refuses_old_kind_or_extra_field():
    frozen = ready_candidate()
    frozen["kind"] = "vm_pre_ssh_stop_candidate_v1"
    assert not valid_frozen_stop_candidate(frozen)
    frozen = deepcopy(ready_candidate())
    frozen["unexpected"] = True
    assert not valid_frozen_stop_candidate(frozen)
