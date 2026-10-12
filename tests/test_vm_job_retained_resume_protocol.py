"""Ready continuation storage witness and authenticated lifecycle transport."""

import json
from uuid import uuid4
from types import SimpleNamespace

import pytest

from shared.vm_cancel_retention import (
    retained_rootdisk_from_preflight,
    valid_ready_retention_candidate,
    valid_ready_retention_preflight,
    valid_retained_rootdisk,
)


def ready_candidate():
    return {
        "version": 1,
        "kind": "vm_job_retained_ready_stop_candidate_v1",
        "owner_kind": "job",
        "job_id": str(uuid4()),
        "namespace": "agent-vms",
        "cluster_id": "cluster-main",
        "continuation_id": str(uuid4()),
        "request_id": str(uuid4()),
        "provision_generation": str(uuid4()),
        "reservation_id": str(uuid4()),
        "reservation_revision": 2,
        "vm_uid": str(uuid4()),
        "vmi_uid": str(uuid4()),
        "launcher_uid": str(uuid4()),
        "node_uid": str(uuid4()),
        "pvc_uid": str(uuid4()),
        "cleanup_request_id": str(uuid4()),
        "cleanup_intent_digest": "sha256:" + "a" * 64,
    }


def ready_preflight(frozen):
    return {
        "version": 1,
        "kind": "vm_job_retained_ready_preflight_v1",
        "stop_policy": "retained_ready_continuation_v1",
        "frozen": frozen,
        "namespace": frozen["namespace"],
        "owner_id": frozen["job_id"],
        "pvc_name": "agent-vm-" + frozen["job_id"] + "-rootdisk",
        "pvc_uid": frozen["pvc_uid"],
        "dv_uid": str(uuid4()),
        "ownership": "standalone_dv",
        "deleting": False,
        "consumer_scope": "exact_frozen_runtime_only",
    }


def test_ready_schema_is_exact_and_not_pre_ssh():
    frozen = ready_candidate()
    proof = ready_preflight(frozen)
    assert valid_ready_retention_candidate(frozen)
    assert valid_ready_retention_preflight(proof, frozen)
    final = retained_rootdisk_from_preflight(proof)
    assert valid_retained_rootdisk(final, proof)
    assert final["no_consumers"] is True
    assert "no_consumers" not in proof
    assert not valid_ready_retention_candidate(dict(frozen, unrelated=True))
    assert not valid_ready_retention_candidate(
        {key: value for key, value in frozen.items() if key != "node_uid"}
    )
    for field, wrong in (
        ("reservation_revision", True),
        ("reservation_revision", 0),
        ("pvc_uid", "wrong"),
        ("cleanup_intent_digest", "sha256:wrong"),
        ("owner_kind", "thread"),
        ("kind", "vm_pre_ssh_stop_candidate_v1"),
    ):
        changed = dict(frozen, **{field: wrong})
        assert not valid_ready_retention_candidate(changed), field
    for field, wrong in (
        ("pvc_uid", str(uuid4())),
        ("dv_uid", "wrong"),
        ("pvc_name", "other-rootdisk"),
        ("consumer_scope", "all"),
        ("deleting", True),
    ):
        assert not valid_ready_retention_preflight(
            dict(proof, **{field: wrong}), frozen
        )
    assert not valid_ready_retention_preflight(dict(proof, no_consumers=True), frozen)
    assert not valid_ready_retention_preflight(
        dict(proof, frozen=dict(frozen, node_uid=str(uuid4()))), frozen
    )
    assert not valid_retained_rootdisk(dict(final, pvc_uid=str(uuid4())), proof)


@pytest.mark.asyncio
async def test_ready_status_missing_capability_holds_before_any_stop(monkeypatch):
    from orchestrator.services.vm_provisioner import VMProvisioner

    frozen = ready_candidate()
    provider = object.__new__(VMProvisioner)
    provider._lifecycle_hmac_secret = b"test-secret"
    provider._db = None
    monkeypatch.setattr(VMProvisioner, "_nats_available", property(lambda self: True))
    monkeypatch.setattr(VMProvisioner, "_http_available", property(lambda self: False))
    monkeypatch.setattr(
        VMProvisioner, "lifecycle_available", property(lambda self: True)
    )
    provider._http_client = None
    from orchestrator.services import vm_provisioner as module

    class Bridge:
        async def query_vm_status(self, job_id, **kwargs):
            assert kwargs["retained_ready_stop_candidate"] == frozen
            return {
                "job_id": job_id,
                "provision_generation": frozen["provision_generation"],
                "_identity_authenticated": True,
                "ready": True,
            }

    original = module.nats_bridge
    module.nats_bridge = Bridge()
    try:
        assert await provider.qualify_retained_ready_stop(frozen) is None
    finally:
        module.nats_bridge = original


@pytest.mark.asyncio
async def test_controller_ready_witness_requires_exact_runtime_and_node(monkeypatch):
    from vm_controller import controller as module

    frozen = ready_candidate()
    actual_node_uid = frozen["node_uid"]
    monkeypatch.setattr(module, "LIFECYCLE_HMAC_SECRET", b"test-secret")
    monkeypatch.setattr(module, "VM_NAMESPACE", frozen["namespace"])
    vm_name = "agent-vm-" + frozen["job_id"]
    vm = {
        "metadata": {
            "name": vm_name,
            "uid": frozen["vm_uid"],
            "labels": {"srw.io/owner-kind": "job", "srw.io/owner-id": frozen["job_id"]},
            "annotations": {
                module._PROVISION_GENERATION_ANNOTATION: frozen["provision_generation"],
                "srw.io/vm-create-request-id": frozen["request_id"],
            },
        },
        "status": {
            "created": True,
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }
    vmi = {
        "metadata": {"name": vm_name, "uid": frozen["vmi_uid"]},
        "status": {"phase": "Running"},
    }
    pod = {
        "metadata": {"name": "virt-launcher-test", "uid": frozen["launcher_uid"]},
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
            return {"metadata": {"uid": actual_node_uid}}

    class Ctrl:
        k8s_client = K8s()
        core_api = Core()
        resource_inventory_collector = SimpleNamespace(
            namespace=frozen["namespace"], cluster_id=frozen["cluster_id"]
        )

        async def _qualify_cancel_retained_rootdisk(
            self, job_id, pvc_uid, *, allowed_runtime
        ):
            assert job_id == frozen["job_id"] and pvc_uid == frozen["pvc_uid"]
            assert allowed_runtime == {
                "vm_name": vm_name,
                "vm_uid": frozen["vm_uid"],
                "vmi_uid": frozen["vmi_uid"],
                "launcher_name": "virt-launcher-test",
                "launcher_uid": frozen["launcher_uid"],
            }
            return {
                "namespace": frozen["namespace"],
                "owner_id": job_id,
                "pvc_name": "agent-vm-" + job_id + "-rootdisk",
                "pvc_uid": pvc_uid,
                "dv_uid": str(dv_uid),
                "ownership": "standalone_dv",
                "deleting": False,
            }

    dv_uid = uuid4()
    ctrl = Ctrl()
    proof = await module.VMController._qualify_ready_retention_preflight(ctrl, frozen)
    assert valid_ready_retention_preflight(proof, frozen)
    wrong_cluster = dict(frozen, cluster_id="foreign-cluster")
    assert (
        await module.VMController._qualify_ready_retention_preflight(
            ctrl, wrong_cluster
        )
        is None
    )
    wrong_request = dict(frozen, request_id=str(uuid4()))
    assert (
        await module.VMController._qualify_ready_retention_preflight(
            ctrl, wrong_request
        )
        is None
    )
    frozen["node_uid"] = str(uuid4())
    assert (
        await module.VMController._qualify_ready_retention_preflight(ctrl, frozen)
        is None
    )


@pytest.mark.asyncio
async def test_provisioner_accepts_only_echoed_authenticated_ready_witness(monkeypatch):
    from orchestrator.services.vm_provisioner import VMProvisioner
    from orchestrator.services import vm_provisioner as module

    frozen = ready_candidate()
    proof = ready_preflight(frozen)
    provider = object.__new__(VMProvisioner)
    provider._lifecycle_hmac_secret = b"test-secret"
    provider._db = None
    monkeypatch.setattr(VMProvisioner, "_nats_available", property(lambda self: True))
    monkeypatch.setattr(VMProvisioner, "_http_available", property(lambda self: False))
    monkeypatch.setattr(
        VMProvisioner, "lifecycle_available", property(lambda self: True)
    )

    class Bridge:
        calls = 0

        async def query_vm_status(self, job_id, **kwargs):
            self.calls += 1
            return {
                "job_id": job_id,
                "provision_generation": frozen["provision_generation"],
                "_identity_authenticated": True,
                "ready": True,
                "vm_uid": frozen["vm_uid"],
                "vmi_uid": frozen["vmi_uid"],
                "active_pod_uid": frozen["launcher_uid"],
                "retention_preflight": proof,
            }

    bridge = Bridge()
    monkeypatch.setattr(module, "nats_bridge", bridge)
    assert await provider.qualify_retained_ready_stop(frozen) == proof
    proof["owner_id"] = str(uuid4())
    assert await provider.qualify_retained_ready_stop(frozen) is None
    proof["owner_id"] = frozen["job_id"]
    proof["frozen"] = dict(frozen, pvc_uid=str(uuid4()))
    assert await provider.qualify_retained_ready_stop(frozen) is None

    async def bound(*args):
        return {"unexpected": "workspace_binding"}

    provider._storage_context = bound
    calls = bridge.calls
    assert await provider.qualify_retained_ready_stop(frozen) is None
    assert bridge.calls == calls


@pytest.mark.asyncio
async def test_ready_parent_revalidated_before_release_probe(monkeypatch):
    from orchestrator.services.vm_provisioner import VMProvisioner, VMTeardownIdentity

    frozen = ready_candidate()
    proof = ready_preflight(frozen)
    provider = object.__new__(VMProvisioner)
    provider._db = object()

    async def current(*args):
        return frozen["provision_generation"]

    async def storage(*args):
        return None

    async def forbidden_probe(*args, **kwargs):
        raise AssertionError("VM probe or SSH reached before parent validation")

    provider._current_provision_generation = current
    provider._storage_context = storage
    provider._probe_vm_teardown_identity = forbidden_probe
    identity = VMTeardownIdentity(
        frozen["provision_generation"], frozen["vm_uid"], frozen["pvc_uid"]
    )
    result = await provider.release_vm_captured(
        frozen["job_id"],
        identity,
        purge_disk=False,
        capture_snapshot=False,
        parent_cleanup={"retention_preflight": proof},
    )
    assert result.disposition == "retention_preflight_unproven"


@pytest.mark.asyncio
async def test_ready_parent_helper_refuses_omitted_or_changed_witness(monkeypatch):
    from orchestrator.services.vm_provisioner import VMProvisioner
    from orchestrator.services import vm_job_retained_resume as lineage

    frozen = ready_candidate()
    proof = ready_preflight(frozen)
    provider = object.__new__(VMProvisioner)
    provider._db = object()

    async def read_ready(*args, **kwargs):
        return proof

    monkeypatch.setattr(
        lineage, "read_current_ready_preflight", read_ready, raising=False
    )
    kwargs = dict(
        job_id=frozen["job_id"],
        generation=frozen["provision_generation"],
        expected_vm_uid=frozen["vm_uid"],
        expected_pvc_uid=frozen["pvc_uid"],
        entity_type="job",
    )
    assert await provider._ready_parent_is_current(
        {"retention_preflight": proof}, **kwargs
    )
    assert not await provider._ready_parent_is_current({}, **kwargs)
    changed = dict(proof, dv_uid=str(uuid4()))
    assert not await provider._ready_parent_is_current(
        {"retention_preflight": changed}, **kwargs
    )

    async def legacy(*args, **kwargs):
        return None

    monkeypatch.setattr(lineage, "read_current_ready_preflight", legacy)
    assert await provider._ready_parent_is_current({}, **kwargs)
    noncanonical = dict(kwargs, job_id="job-legacy")
    assert not await provider._ready_parent_is_current(
        {"retention_preflight": proof}, **noncanonical
    )
    assert not await provider._ready_parent_is_current(
        {"intent": {"source": "job_terminal_vm_release"}}, **noncanonical
    )
    provider._db = None
    assert not await provider._ready_parent_is_current(
        {"intent": {"source": "job_terminal_vm_release"}}, **kwargs
    )


@pytest.mark.asyncio
async def test_controller_changed_ready_disk_holds_before_vm_delete(monkeypatch):
    from vm_controller import controller as module

    frozen = ready_candidate()
    proof = ready_preflight(frozen)
    monkeypatch.setattr(module, "LIFECYCLE_HMAC_SECRET", b"test-secret")
    monkeypatch.setattr(module, "VM_NAMESPACE", frozen["namespace"])
    vm_name = "agent-vm-" + frozen["job_id"]
    vm = {
        "metadata": {
            "name": vm_name,
            "uid": frozen["vm_uid"],
            "annotations": {
                module._PROVISION_GENERATION_ANNOTATION: frozen["provision_generation"],
                "srw.io/vm-create-request-id": frozen["request_id"],
            },
            "labels": {"srw.io/owner-kind": "job", "srw.io/owner-id": frozen["job_id"]},
        }
    }
    deleted = []

    class K8s:
        def get_namespaced_custom_object(self, **kwargs):
            return vm

        def delete_namespaced_custom_object(self, **kwargs):
            deleted.append(kwargs)

    class Ctrl:
        k8s_client = K8s()

        async def _rootdisk_pvc_probe(self, *args, **kwargs):
            return True, frozen["pvc_uid"]

        async def _qualify_ready_retention_preflight(self, *args, **kwargs):
            return None  # changed DV/PVC or foreign runtime consumer

    parent = {
        "admission_id": str(uuid4()),
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
        "retention_preflight": proof,
    }
    with pytest.raises(RuntimeError, match="Ready retained runtime or disk changed"):
        await module.VMController._do_delete_serialized(
            Ctrl(),
            frozen["job_id"],
            purge_disk=False,
            provision_generation=frozen["provision_generation"],
            expected_vm_uid=frozen["vm_uid"],
            expected_rootdisk_pvc_uid=frozen["pvc_uid"],
            parent_cleanup=parent,
        )
    assert deleted == []


@pytest.mark.asyncio
async def test_nats_ready_status_candidate_is_signed_and_correlated():
    from orchestrator.services.nats_bridge import NatsBridge
    from orchestrator.services.vm_lifecycle_auth import (
        AUTH_FIELD,
        sign_payload,
        verify_payload,
    )

    frozen = ready_candidate()
    secret = b"test-ready-status-secret"
    bridge = NatsBridge(url="nats://test")
    bridge._available = True
    bridge._orchestrator_id = "test"
    bridge._lifecycle_hmac_secret = secret

    class NC:
        async def request(self, subject, raw, *, timeout):
            sent = json.loads(raw)
            assert subject == "vm.lifecycle.get.test"
            assert sent["retained_ready_stop_candidate"] == frozen
            assert verify_payload(
                sent, direction="request", operation="status", secret=secret
            )
            response = sign_payload(
                {
                    "job_id": frozen["job_id"],
                    "provision_generation": frozen["provision_generation"],
                    "retention_preflight": ready_preflight(frozen),
                },
                direction="response",
                operation="status",
                secret=secret,
                correlation_id=sent[AUTH_FIELD]["request_id"],
            )
            return SimpleNamespace(data=json.dumps(response).encode())

    bridge._nc = NC()
    result = await bridge.query_vm_status(
        frozen["job_id"],
        provision_generation=frozen["provision_generation"],
        retained_ready_stop_candidate=frozen,
    )
    assert result["_identity_authenticated"] is True
    assert valid_ready_retention_preflight(result["retention_preflight"], frozen)


@pytest.mark.asyncio
async def test_http_ready_status_candidate_is_signed_and_correlated():
    from orchestrator.services.vm_provisioner import VMProvisioner
    from orchestrator.services.vm_lifecycle_auth import (
        AUTH_FIELD,
        sign_payload,
        verify_payload,
    )
    from vm_controller.controller import _authenticated_http_payload

    frozen = ready_candidate()
    secret = b"test-ready-status-secret"
    provider = VMProvisioner()
    provider._lifecycle_hmac_secret = secret

    async def storage(*args):
        return None

    provider._storage_context = storage

    class Response:
        status_code = 200

        def __init__(self, body):
            self.body = body

        def json(self):
            return self.body

        def raise_for_status(self):
            pass

    class Client:
        async def get(self, path, *, params, timeout):
            assert path == "/vms/" + frozen["job_id"]
            candidate_raw = params["retained_ready_stop_candidate"]
            assert json.loads(candidate_raw) == frozen
            reconstructed = _authenticated_http_payload(
                SimpleNamespace(query=params),
                {
                    "job_id": frozen["job_id"],
                    "provision_generation": frozen["provision_generation"],
                    "retained_ready_stop_candidate": candidate_raw,
                },
                operation="status",
            )
            assert verify_payload(
                reconstructed, direction="request", operation="status", secret=secret
            )
            signed = sign_payload(
                {
                    "job_id": frozen["job_id"],
                    "provision_generation": frozen["provision_generation"],
                    "retention_preflight": ready_preflight(frozen),
                },
                direction="response",
                operation="status",
                secret=secret,
                correlation_id=reconstructed[AUTH_FIELD]["request_id"],
            )
            return Response(signed)

    provider._http_client = Client()
    result = await provider._query_http(
        frozen["job_id"],
        provision_generation=frozen["provision_generation"],
        retained_ready_stop_candidate=frozen,
    )
    assert result["_identity_authenticated"] is True
    assert valid_ready_retention_preflight(result["retention_preflight"], frozen)


@pytest.mark.asyncio
async def test_final_stop_attestation_requires_same_disk_and_no_consumers():
    from orchestrator.services.vm_provisioner import (
        VMProvisioner,
        VMTeardownIdentity,
        _VMTeardownProbe,
    )

    frozen = ready_candidate()
    proof = ready_preflight(frozen)
    final = retained_rootdisk_from_preflight(proof)
    provider = object.__new__(VMProvisioner)

    async def current(*args):
        return frozen["provision_generation"]

    async def storage(*args):
        return None

    async def probe(*args, **kwargs):
        return _VMTeardownProbe(
            disposition="absent",
            identity=VMTeardownIdentity(
                frozen["provision_generation"], frozen["vm_uid"], frozen["pvc_uid"]
            ),
            rootdisk_identity_known=True,
            runtime_absence_known=True,
            vmi_absent=True,
            launcher_absent=True,
            retained_rootdisk=final,
        )

    provider._current_provision_generation = current
    provider._storage_context = storage
    provider._probe_vm_teardown_identity = probe
    candidate = {
        key: frozen[key]
        for key in (
            "job_id",
            "provision_generation",
            "vm_uid",
            "vmi_uid",
            "launcher_uid",
            "pvc_uid",
        )
    }
    candidate.update(purge_disk=False, retention_preflight=proof)
    result = await provider.attest_vm_cleanup_stop(candidate)
    assert result["retained_rootdisk"] == final
    assert result["pvc_disposition"] == "retained"
    final = dict(final, no_consumers=False)
    assert await provider.attest_vm_cleanup_stop(candidate) is None
    final = dict(final, no_consumers=True, dv_uid=str(uuid4()))
    assert await provider.attest_vm_cleanup_stop(candidate) is None
