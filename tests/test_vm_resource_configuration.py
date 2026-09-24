"""Opt-in resource estimates are frozen configuration, not admission authority."""

from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from shared.vm_creation_issuance import canonical_configuration_digest
from tests.test_vm_resource_policy import complete_policy, snapshot
from tests.test_vm_resource_template import shipped_template

# Literal bytes and digest captured with the unchanged v1 validator.
V1_JSON = '{"authorized_public_key_digest":"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","cloud_init_template_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","golden_disk_size":"10Gi","golden_enabled":false,"headscale_api_url":"","headscale_enabled":false,"headscale_key_expiry_minutes":30,"headscale_url":"","headscale_user":"workers","implementation_digest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","namespace":"agent-vms","nats_url":"nats://nats:4222","node_selector":{},"orchestrator_id":"orchestrator","orchestrator_url":"http://orchestrator:8000","persistent_rootdisk":true,"preparation":{},"storage_class":"local-path","tolerations":[],"version":1,"vm_template_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'
V1_DIGEST = "sha256:6ddfa9d819b9d24c0a3d8184097e39864a833f31992c76169255702a6ef436af"


def envelope():
    return {
        "version": 1,
        "cluster_id": "cluster-one",
        "policy_digest": snapshot().policy_digest,
        "profile_algorithm": "srw-vm-template-profile-v1",
        "template_profile": {
            "version": 1,
            "selector": {},
            "tolerations": [],
            "required_affinity": None,
            "storage_class": "local-path",
            "guest_vcpus": 2,
            "guest_memory_bytes": 4 * 1024**3,
        },
        "host_mapping": {
            "algorithm": "srw-vm-host-cost-v1",
            "policy": complete_policy()["policy"]["hostCost"],
            "vector": {
                "cpu_millicores": 200,
                "memory_bytes": 4 * 1024**3 + 276 * 1024**2,
                "kvm_devices": 1,
            },
        },
    }


def configuration():
    return {**json.loads(V1_JSON), "version": 2, "resource_admission": envelope()}


def whole_launcher_configuration():
    from shared.vm_launcher_profile import LAUNCHER_ALGORITHM, predict_launcher
    from shared.vm_resource_policy import validate_complete_resource_policy
    from shared.vm_resource_admission import WHOLE_LAUNCHER_HOST_COST_ALGORITHM
    from tests.test_vm_resource_policy import whole_launcher_policy

    policy = whole_launcher_policy()
    frozen = validate_complete_resource_policy(policy)
    doc = configuration()
    doc["version"] = 3
    resource = doc["resource_admission"]
    resource["version"] = 2
    resource["policy_digest"] = frozen.policy_digest
    resource["launcher_profile"] = frozen.launcher_profile
    resource["launcher_prediction"] = {
        "algorithm": LAUNCHER_ALGORITHM,
        "vector": predict_launcher(
            frozen.launcher_profile, guest_vcpus=2, guest_memory_bytes=4 * 1024**3
        ).to_six_dict(),
    }
    resource["host_mapping"] = {
        "algorithm": WHOLE_LAUNCHER_HOST_COST_ALGORITHM,
        "policy": policy["policy"]["hostCost"],
        "vector": frozen.host_cost.cost(2, "4Gi").to_six_dict(),
    }
    return doc


def test_v3_inner_v2_recomputes_prediction_reserve_and_preserves_old_digest():
    doc = whole_launcher_configuration()
    digest = canonical_configuration_digest(doc)
    assert digest != V1_DIGEST
    assert canonical_configuration_digest(json.loads(V1_JSON)) == V1_DIGEST
    for path, value in [
        (("launcher_profile", "cpuAllocationRatio"), 5),
        (("launcher_prediction", "vector", "ephemeral_storage_bytes"), 0),
        (("host_mapping", "vector", "tun_devices"), 0),
        (("host_mapping", "policy", "cpuMillicoresPerVcpuDenominator"), 1),
    ]:
        changed = deepcopy(doc)
        target = changed["resource_admission"]
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
        with pytest.raises(ValueError):
            canonical_configuration_digest(changed)
    under = deepcopy(doc)
    under["resource_admission"]["host_mapping"]["policy"]["ephemeralStorageReserveBytes"] = 1
    under["resource_admission"]["host_mapping"]["vector"]["ephemeral_storage_bytes"] = 1
    with pytest.raises(ValueError):
        canonical_configuration_digest(under)


def test_literal_v1_canonical_bytes_and_digest_are_unchanged():
    doc = json.loads(V1_JSON)
    assert (
        json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        == V1_JSON
    )
    assert canonical_configuration_digest(doc) == V1_DIGEST
    assert (
        canonical_configuration_digest(dict(reversed(list(doc.items())))) == V1_DIGEST
    )


def test_v2_closed_configuration_is_deterministic_and_binds_resource_identity():
    doc = configuration()
    digest = canonical_configuration_digest(doc)
    assert digest != V1_DIGEST
    assert canonical_configuration_digest(dict(reversed(list(doc.items())))) == digest
    for path, value in [
        (("cluster_id",), "other"),
        (("policy_digest",), "sha256:" + "e" * 64),
        (("template_profile", "selector"), {"zone": "a"}),
        (("template_profile", "required_affinity"), {"nodeSelectorTerms": []}),
    ]:
        changed = deepcopy(doc)
        target = changed["resource_admission"]
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
        assert canonical_configuration_digest(changed) != digest


@pytest.mark.parametrize(
    "version,present", [(1, True), (2, False), (True, False), (3, True)]
)
def test_versions_have_separate_exact_field_sets(version, present):
    doc = configuration()
    doc["version"] = version
    if not present:
        doc.pop("resource_admission")
    with pytest.raises(ValueError):
        canonical_configuration_digest(doc)


@pytest.mark.parametrize(
    "path,value",
    [
        (("version",), True),
        (("version",), 2),
        (("cluster_id",), ""),
        (("cluster_id",), "bad/cluster"),
        (("policy_digest",), "sha256:" + "z" * 64),
        (("profile_algorithm",), "future"),
        (("template_profile", "version"), True),
        (("template_profile", "guest_vcpus"), True),
        (("template_profile", "guest_vcpus"), 0),
        (("template_profile", "guest_memory_bytes"), 4.0),
        (("template_profile", "storage_class"), "other"),
        (("template_profile", "selector"), {"zone": True}),
        (("template_profile", "selector"), {"zone": "${ZONE}"}),
        (("template_profile", "tolerations"), [{"operator": "Unknown"}]),
        (("template_profile", "required_affinity"), {"podAffinity": {}}),
        (("host_mapping", "algorithm"), "future"),
        (("host_mapping", "policy", "cpuMillicoresPerVcpuDenominator"), 0),
        (("host_mapping", "policy", "fixedMemoryOverheadBytes"), 2**63 - 1),
        (("host_mapping", "policy", "memoryOverheadBasisPoints"), False),
        (("host_mapping", "vector", "cpu_millicores"), 201),
        (("host_mapping", "vector", "cpu_millicores"), 200.0),
        (("host_mapping", "vector", "kvm_devices"), True),
        (("host_mapping", "vector", "memory_bytes"), 0),
    ],
)
def test_v2_refuses_invalid_or_inconsistent_resource_facts(path, value):
    doc = configuration()
    target = doc["resource_admission"]
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ValueError):
        canonical_configuration_digest(doc)


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("template_profile",),
        ("host_mapping",),
        ("host_mapping", "policy"),
        ("host_mapping", "vector"),
    ],
)
@pytest.mark.parametrize("change", ["extra", "missing", "null"])
def test_v2_nested_vocabularies_are_closed(path, change):
    doc = configuration()
    target = doc["resource_admission"]
    for key in path:
        target = target[key]
    if change == "extra":
        target["extra"] = 0
    elif change == "missing":
        target.pop(next(iter(target)))
    else:
        target[next(iter(target))] = None
    with pytest.raises(ValueError):
        canonical_configuration_digest(doc)


@pytest.fixture
def resolver_inputs(monkeypatch):
    from vm_controller import controller as module
    from tests.test_vm_creation_configuration import request

    monkeypatch.setattr(module, "VM_NAMESPACE", snapshot().inventory.namespace)
    monkeypatch.setattr(module, "VM_STORAGE_CLASS", "local-path")
    monkeypatch.setattr(module, "VM_NODE_SELECTOR", {})
    monkeypatch.setattr(module, "VM_TOLERATIONS", [])
    vm = SimpleNamespace(
        template_text=shipped_template(),
        cloud_init_text="#cloud-config",
        headscale=SimpleNamespace(is_available=False, create_auth_key=AsyncMock()),
        render_template=Mock(side_effect=AssertionError("no render")),
        custom_api=Mock(side_effect=AssertionError("no Kubernetes")),
    )
    return vm, {**request(), "cpu_cores": 2, "memory": "4Gi"}


def test_resolver_opt_in_is_private_typed_and_no_policy_keeps_v1(resolver_inputs):
    from vm_controller.creation_configuration import resolve_creation_configuration

    vm, request = resolver_inputs
    before = resolve_creation_configuration(vm, request)
    explicit_none = resolve_creation_configuration(
        vm, request, _resource_policy_snapshot=None
    )
    assert before == explicit_none
    assert before["controller_configuration"]["version"] == 1
    resolved = resolve_creation_configuration(
        vm, request, _resource_policy_snapshot=snapshot()
    )
    doc = resolved["controller_configuration"]
    assert doc["version"] == 2
    expected = envelope()
    expected["cluster_id"] = snapshot().inventory.cluster_id
    assert doc["resource_admission"] == expected
    assert {**doc, "version": 1}.keys() == (
        before["controller_configuration"].keys() | {"resource_admission"}
    )
    legacy = {k: v for k, v in doc.items() if k != "resource_admission"}
    legacy["version"] = 1
    assert legacy == before["controller_configuration"]
    assert resolved["request"] == before["request"]
    assert resolved[
        "controller_configuration_digest"
    ] == canonical_configuration_digest(doc)
    vm.headscale.create_auth_key.assert_not_awaited()
    vm.render_template.assert_not_called()
    vm.custom_api.assert_not_called()


@pytest.mark.parametrize("kubevirt_version", ["v1.6.6", "v1.8.4"])
def test_operator_enforcement_selects_v3_on_actual_configuration_resolver(
    resolver_inputs, monkeypatch, kubevirt_version,
):
    from tests.test_vm_resource_policy import whole_launcher_policy
    from vm_controller.creation_configuration import resolve_creation_configuration

    policy = whole_launcher_policy()
    policy["policy"].update(shadowEnabled=True, enforcementEnabled=True)
    policy["policy"]["launcherProfile"].update(
        kubevirtVersion=kubevirt_version,
        costAlgorithm=f"kubevirt-{kubevirt_version}-amd64-ordinary-pvc-v1",
    )
    monkeypatch.setenv("VM_RESOURCE_ADMISSION_CONFIG", json.dumps(policy))
    vm, request = resolver_inputs

    result = resolve_creation_configuration(vm, request)

    assert result["controller_configuration"]["version"] == 3
    assert result["controller_configuration"]["resource_admission"]["version"] == 2
    assert result["controller_configuration"]["resource_admission"]["cluster_id"] == "test-cluster"
    resource = result["controller_configuration"]["resource_admission"]
    assert resource["launcher_prediction"]["algorithm"] == policy["policy"]["launcherProfile"]["costAlgorithm"]
    changed = deepcopy(result["controller_configuration"])
    changed["resource_admission"]["launcher_prediction"]["algorithm"] = "unknown"
    with pytest.raises(ValueError):
        canonical_configuration_digest(changed)



@pytest.mark.parametrize("invalid", [{}, "sha256:" + "a" * 64, False])
def test_explicit_invalid_policy_does_not_downgrade(resolver_inputs, invalid):
    from vm_controller.creation_configuration import resolve_creation_configuration

    with pytest.raises(ValueError):
        resolve_creation_configuration(
            *resolver_inputs, _resource_policy_snapshot=invalid
        )


def test_resolver_refuses_policy_from_another_namespace(resolver_inputs):
    from vm_controller.creation_configuration import resolve_creation_configuration

    policy = complete_policy()
    policy["namespace"] = "other"
    with pytest.raises(ValueError):
        resolve_creation_configuration(
            *resolver_inputs, _resource_policy_snapshot=snapshot(policy)
        )


def test_resolver_policy_drift_changes_digest_and_frozen_document_is_owned(
    resolver_inputs,
):
    from vm_controller.creation_configuration import resolve_creation_configuration

    vm, request = resolver_inputs
    first = resolve_creation_configuration(
        vm, request, _resource_policy_snapshot=snapshot()
    )
    policy = complete_policy()
    policy["policy"]["fairness"]["maxBypasses"] += 1
    second = resolve_creation_configuration(
        vm, request, _resource_policy_snapshot=snapshot(policy)
    )
    assert (
        second["controller_configuration_digest"]
        != first["controller_configuration_digest"]
    )
    assert (
        first["controller_configuration"]["resource_admission"]["host_mapping"][
            "vector"
        ]
        == second["controller_configuration"]["resource_admission"]["host_mapping"][
            "vector"
        ]
    )
    policy["policy"]["hostCost"]["fixedMemoryOverheadBytes"] = 0
    assert (
        first["controller_configuration"]["resource_admission"]["host_mapping"][
            "policy"
        ]["fixedMemoryOverheadBytes"]
        == 260 * 1024**2
    )


@pytest.mark.parametrize("change", ["storage", "selector", "guest", "cost"])
def test_consistent_resource_changes_produce_distinct_frozen_digests(change):
    doc = configuration()
    before = canonical_configuration_digest(doc)
    resource = doc["resource_admission"]
    if change == "storage":
        resource["template_profile"]["storage_class"] = doc["storage_class"] = "other"
    elif change == "selector":
        resource["template_profile"]["selector"] = doc["node_selector"] = {"zone": "b"}
    else:
        from shared.vm_resource_policy import parse_host_cost_policy

        if change == "guest":
            resource["template_profile"]["guest_vcpus"] = 3
        else:
            resource["host_mapping"]["policy"]["launcherCpuOverheadMillicores"] = 10
        profile = resource["template_profile"]
        cost = parse_host_cost_policy(resource["host_mapping"]["policy"])
        resource["host_mapping"]["vector"] = cost.cost(
            profile["guest_vcpus"], str(profile["guest_memory_bytes"])
        ).to_dict()
    assert canonical_configuration_digest(doc) != before


@pytest.mark.asyncio
@pytest.mark.parametrize("location", ["none", "outer", "request"])
async def test_signed_http_request_cannot_opt_into_private_policy(
    resolver_inputs, monkeypatch, location
):
    from shared.vm_lifecycle_auth import sign_payload
    from vm_controller import controller as module

    secret = b"configuration-v2-fixture-secret-at-least-32"
    monkeypatch.setattr(module, "LIFECYCLE_HMAC_SECRET", secret)
    original, payload = resolver_inputs
    vm = module.VMController.__new__(module.VMController)
    vm.template_text = original.template_text
    vm.cloud_init_text = original.cloud_init_text
    vm.headscale = original.headscale
    body = {"request": payload}
    if location != "none":
        target = body if location == "outer" else body["request"]
        target["_resource_policy_snapshot"] = complete_policy()
    body = sign_payload(
        body, direction="request", operation="creation_config_resolve", secret=secret
    )
    response = await vm.http_resolve_creation_config(
        SimpleNamespace(json=AsyncMock(return_value=body))
    )
    result = json.loads(response.text)
    assert response.status == (200 if location == "none" else 409)
    if location == "none":
        assert result["controller_configuration"]["version"] == 1
        assert "resource_admission" not in result["controller_configuration"]
    vm.headscale.create_auth_key.assert_not_awaited()


@pytest.mark.asyncio
async def test_v2_original_capture_cannot_recapture_policy_drift(resolver_inputs):
    from tests.test_vm_creation_request import CreationDB, options, JOB, GENERATION
    from orchestrator.services.vm_creation_request import (
        build_vm_creation_request,
        capture_vm_creation_request,
    )
    from vm_controller.creation_configuration import resolve_creation_configuration

    vm, _ = resolver_inputs
    payload = build_vm_creation_request(**options(), network_tier="restricted")
    resolved = resolve_creation_configuration(
        vm, payload, _resource_policy_snapshot=snapshot()
    )
    db = CreationDB()

    async def capture(result):
        return await capture_vm_creation_request(
            db,
            job_id=JOB,
            generation=GENERATION,
            request=result["request"],
            controller_configuration=result["controller_configuration"],
            controller_configuration_digest=result["controller_configuration_digest"],
        )

    original = await capture(resolved)
    changed = complete_policy()
    changed["policy"]["hostCost"]["launcherCpuOverheadMillicores"] = 10
    drift = resolve_creation_configuration(
        vm, payload, _resource_policy_snapshot=snapshot(changed)
    )
    with pytest.raises(ValueError):
        await capture(drift)
    assert db.snapshot == original


def test_template_placement_and_environment_drift_are_bound(
    resolver_inputs, monkeypatch
):
    import yaml
    from vm_controller import controller as module
    from vm_controller.creation_configuration import resolve_creation_configuration

    vm, payload = resolver_inputs
    original = resolve_creation_configuration(
        vm, payload, _resource_policy_snapshot=snapshot()
    )
    raw = yaml.safe_load(vm.template_text)
    raw["spec"]["template"]["spec"]["nodeSelector"] = {"zone": "a"}
    vm.template_text = yaml.safe_dump(raw)
    inherited = resolve_creation_configuration(
        vm, payload, _resource_policy_snapshot=snapshot()
    )
    assert inherited["controller_configuration"]["resource_admission"][
        "template_profile"
    ]["selector"] == {"zone": "a"}
    monkeypatch.setattr(module, "VM_NODE_SELECTOR", {"zone": "b"})
    overridden = resolve_creation_configuration(
        vm, payload, _resource_policy_snapshot=snapshot()
    )
    assert overridden["controller_configuration"]["resource_admission"][
        "template_profile"
    ]["selector"] == {"zone": "b"}
    assert (
        len(
            {
                r["controller_configuration_digest"]
                for r in (original, inherited, overridden)
            }
        )
        == 3
    )
