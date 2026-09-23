"""Final resource manifests preserve frozen constraints before future wiring."""

from copy import deepcopy

import pytest

from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_placement import _affinity_matches
from tests.test_vm_creation_preparation import prepared_case as _prepared_fixture

prepared_case = _prepared_fixture


@pytest.mark.parametrize(
    "required",
    [
        None,
        {"nodeSelectorTerms": []},
        {"nodeSelectorTerms": [{}]},
        {"nodeSelectorTerms": [{"matchExpressions": [], "matchFields": []}]},
        {
            "nodeSelectorTerms": [
                {},
                {
                    "matchExpressions": [
                        {"key": "zone", "operator": "In", "values": ["a"]}
                    ]
                },
            ]
        },
        {
            "nodeSelectorTerms": [
                {
                    "matchExpressions": [
                        {"key": "zone", "operator": "In", "values": ["a"]}
                    ]
                },
                {
                    "matchExpressions": [
                        {"key": "zone", "operator": "NotIn", "values": ["b"]}
                    ]
                },
            ]
        },
        {
            "nodeSelectorTerms": [
                {
                    "matchFields": [
                        {"key": "metadata.name", "operator": "In", "values": ["node-a"]}
                    ]
                }
            ]
        },
        {
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
        {
            "nodeSelectorTerms": [
                {
                    "matchExpressions": [
                        {
                            "key": "kubernetes.io/hostname",
                            "operator": "In",
                            "values": ["other"],
                        }
                    ]
                }
            ]
        },
    ],
)
def test_hostname_binding_never_broadens_original_affinity(required):
    from shared.vm_resource_manifest import conjoin_required_hostname

    original = deepcopy(required)
    result = conjoin_required_hostname(required, "node-a")
    assert required == original
    assert result == conjoin_required_hostname(result, "node-a")
    for name in ("node-a", "node-b"):
        for zone in ("a", "b", "c"):
            for host in ("node-a", "node-b", None):
                labels = {"zone": zone}
                if host is not None:
                    labels["kubernetes.io/hostname"] = host
                assert _affinity_matches(result, labels, name) == (
                    _affinity_matches(required, labels, name) and host == "node-a"
                )
    result["nodeSelectorTerms"].append({"matchExpressions": []})
    assert required == original


@pytest.mark.parametrize(
    "required,hostname",
    [
        ({"nodeSelectorTerms": [False]}, "node-a"),
        ({"nodeSelectorTerms": [{"futureConstraint": True}]}, "node-a"),
        (
            {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {"key": "zone", "operator": "In", "values": ["${ZONE}"]}
                        ]
                    }
                ]
            },
            "node-a",
        ),
        (None, "${NODE}"),
        (None, ""),
        (None, True),
        (None, "bad/name"),
        (None, "a" * 64),
        (None, {}),
    ],
)
def test_hostname_binding_refuses_unsupported_or_dynamic_placement(required, hostname):
    from shared.vm_resource_manifest import conjoin_required_hostname

    with pytest.raises(ResourceAdmissionError):
        conjoin_required_hostname(required, hostname)


@pytest.fixture
def final_case(monkeypatch):
    from types import SimpleNamespace
    from uuid import uuid4
    from vm_controller import controller as settings
    from vm_controller.creation_actuation import CreationActuator
    from vm_controller.creation_configuration import resolve_creation_configuration
    from tests.test_vm_creation_issuance import carrier_values
    from tests.test_vm_resource_policy import snapshot
    from tests.test_vm_resource_template import shipped_template

    monkeypatch.setattr(settings, "VM_NAMESPACE", snapshot().inventory.namespace)
    monkeypatch.setattr(settings, "VM_STORAGE_CLASS", "local-path")
    monkeypatch.setattr(settings, "VM_NODE_SELECTOR", {})
    monkeypatch.setattr(settings, "VM_TOLERATIONS", [])
    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", False)
    monkeypatch.setattr(
        settings,
        "_inject_ssh_host_key",
        lambda value: (value, "synthetic-public-fingerprint"),
    )
    ctrl = settings.VMController.__new__(settings.VMController)
    ctrl.template_text = shipped_template()
    ctrl.cloud_init_text = "#cloud-config"
    ctrl.headscale = SimpleNamespace(is_available=False)
    resolved = resolve_creation_configuration(
        ctrl,
        {
            "job_id": str(uuid4()),
            "entity_type": "job",
            "provision_generation": str(uuid4()),
            "cpu_cores": 2,
            "memory": "512Mi",
            "disk_size": "32Gi",
        },
        _resource_policy_snapshot=snapshot(),
    )
    row = {
        **resolved,
        "request_id": str(uuid4()),
        "job_id": resolved["request"]["job_id"],
        "provision_generation": resolved["request"]["provision_generation"],
    }

    def intent(kind):
        job = row["job_id"]
        return carrier_values(
            version=2,
            rootdisk_source={"kind": "registry", "image": row["request"]["vm_image"]},
            effect_kind=kind,
            job_id=job,
            provision_generation=row["provision_generation"],
            retry_request_id=row["request_id"],
            request_digest=row["request_digest"],
            controller_configuration_digest=row["controller_configuration_digest"],
            object_name="agent-vm-" + job + ("-rootdisk" if kind == "rootdisk" else ""),
            current_dv_uid=str(uuid4()) if kind == "vm" else None,
            current_pvc_uid=str(uuid4()) if kind == "vm" else None,
            current_secret_uid=str(uuid4()) if kind == "vm" else None,
        )

    return ctrl, CreationActuator(ctrl), row, intent


def validate_final(kind, body, ctrl, row, values, **kw):
    from shared import vm_resource_manifest

    validator = getattr(
        vm_resource_manifest,
        "validate_final_" + ("vm" if kind == "vm" else "rootdisk") + "_manifest",
    )
    return validator(
        body,
        template_text=ctrl.template_text,
        request=row["request"],
        configuration=row["controller_configuration"],
        effect_intent=values,
        **kw,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["vm", "rootdisk"])
async def test_actual_actuator_body_satisfies_pure_final_contract(final_case, kind):
    ctrl, actuator, row, intent = final_case
    values = intent(kind)
    body = await actuator.body(row, values)
    before = deepcopy(body)
    validate_final(kind, body, ctrl, row, values)
    assert body == before


@pytest.mark.asyncio
async def test_v3_whole_launcher_requires_exact_final_vm_body(final_case):
    from uuid import uuid4
    from vm_controller.creation_configuration import resolve_creation_configuration
    from shared.vm_resource_policy import validate_complete_resource_policy
    from tests.test_vm_resource_policy import whole_launcher_policy

    ctrl, actuator, row, intent = final_case
    resolved = resolve_creation_configuration(
        ctrl, row["request"],
        _resource_policy_snapshot=validate_complete_resource_policy(
            whole_launcher_policy()
        ),
    )
    row.update(resolved)
    assert row["controller_configuration"]["version"] == 3
    values = intent("vm")
    resource = row["controller_configuration"]["resource_admission"]
    values["version"] = 4
    values["resource_grant"] = {
        "version": 1, "id": str(uuid4()), "revision": 1,
        "cluster_id": resource["cluster_id"],
        "policy_digest": resource["policy_digest"],
        "node_uid": str(uuid4()), "node_name": "node-a",
        "vector": resource["host_mapping"]["vector"],
        "headroom": {
            "cpu_millicores": 0, "memory_bytes": 0,
            "ephemeral_storage_bytes": 0, "kvm_devices": 0,
            "tun_devices": 0, "vhost_net_devices": 0,
        },
        "snapshot_id": str(uuid4()), "snapshot_digest": "sha256:" + "a" * 64,
    }
    body = await actuator.body(row, values)
    validate_final("vm", body, ctrl, row, values, reservation_hostname="node-a")
    body["spec"]["template"]["spec"]["domain"]["devices"]["interfaces"] = []
    with pytest.raises(ResourceAdmissionError):
        validate_final("vm", body, ctrl, row, values, reservation_hostname="node-a")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path,value",
    [
        (("status",), {}),
        (("_srwCloudInitUserData",), "private-fixture"),
        (("metadata", "uid"), "a-live-uid"),
        (("metadata", "annotations", "hookSidecars"), "hidden"),
        (("metadata", "annotations", "srw.io/provision-generation"), "wrong"),
        (("metadata", "labels", "srw.io/owner-id"), "wrong"),
        (("spec", "dataVolumeTemplates"), []),
        (("spec", "template", "spec", "nodeName"), "other"),
        (
            ("spec", "template", "spec", "domain", "resources"),
            {"requests": {"memory": "512Mi"}},
        ),
        (("spec", "template", "spec", "domain", "cpu", "cores"), 2.0),
        (("spec", "template", "spec", "domain", "memory", "guest"), "513Mi"),
        (
            ("spec", "template", "spec", "readinessProbe"),
            {"exec": {"command": ["true"]}},
        ),
        (("spec", "template", "spec", "tolerations"), [{"operator": "Exists"}]),
        (("spec", "template", "spec", "nodeSelector"), {"zone": "${ZONE}"}),
        (("spec", "template", "spec", "volumes"), []),
    ],
)
async def test_final_vm_rejects_hidden_or_changed_semantics(final_case, path, value):
    ctrl, actuator, row, intent = final_case
    values = intent("vm")
    body = await actuator.body(row, values)
    target = body
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ResourceAdmissionError):
        validate_final("vm", body, ctrl, row, values)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path,value",
    [
        (("metadata", "name"), "foreign"),
        (("metadata", "annotations", "srw.io/vm-create-effect-nonce"), "wrong"),
        (("spec", "storage", "selector"), {"matchLabels": {"disk": "foreign"}}),
        (("spec", "storage", "volumeMode"), "Block"),
        (("spec", "storage", "storageClassName"), "other"),
        (("spec", "storage", "resources", "requests", "storage"), "1Gi"),
        (("spec", "storage", "resources", "limits"), {"storage": "32Gi"}),
        (("spec", "source"), {"pvc": {"name": "foreign", "namespace": "other"}}),
    ],
)
async def test_final_dv_rejects_changed_storage_and_source(final_case, path, value):
    ctrl, actuator, row, intent = final_case
    values = intent("rootdisk")
    body = await actuator.body(row, values)
    target = body
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ResourceAdmissionError):
        validate_final("rootdisk", body, ctrl, row, values)


@pytest.mark.asyncio
async def test_final_bound_vm_requires_exact_conjoined_affinity(final_case):
    from shared.vm_resource_manifest import conjoin_required_hostname

    ctrl, actuator, row, intent = final_case
    values = intent("vm")
    body = await actuator.body(row, values)
    with pytest.raises(ResourceAdmissionError):
        validate_final("vm", body, ctrl, row, values, reservation_hostname="node-a")
    body["spec"]["template"]["spec"]["affinity"] = {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": conjoin_required_hostname(
                None, "node-a"
            )
        }
    }
    validate_final("vm", body, ctrl, row, values, reservation_hostname="node-a")
    with pytest.raises(ResourceAdmissionError):
        validate_final("vm", body, ctrl, row, values, reservation_hostname="node-b")


@pytest.mark.asyncio
async def test_final_validation_refuses_template_drift_and_intent_digest_changes(
    final_case,
):
    ctrl, actuator, row, intent = final_case
    values = intent("vm")
    body = await actuator.body(row, values)
    old = ctrl.template_text
    ctrl.template_text += "\n"
    with pytest.raises(ResourceAdmissionError):
        validate_final("vm", body, ctrl, row, values)
    ctrl.template_text = old
    values["request_digest"] = "sha256:" + "f" * 64
    with pytest.raises(ResourceAdmissionError):
        validate_final("vm", body, ctrl, row, values)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["vm", "rootdisk"])
@pytest.mark.parametrize("workspace", [False, True])
async def test_final_golden_body_and_canonical_workspace_names(
    final_case, kind, workspace
):
    import hashlib
    from uuid import uuid4
    from shared.vm_creation_issuance import canonical_configuration_digest
    from shared.vm_creation_retry import canonical_request_digest
    from shared.vm_workspace_storage import storage_name

    ctrl, actuator, row, intent = final_case
    config = row["controller_configuration"]
    config["golden_enabled"] = True
    row["controller_configuration_digest"] = canonical_configuration_digest(config)
    if workspace:
        row["request"]["workspace_storage"] = {
            "uid": str(uuid4()),
            "generation": 1,
            "pvc_uid": None,
            "owner_id": str(uuid4()),
            "owner_kind": "job",
        }
        row["request_digest"] = canonical_request_digest(row["request"])
    values = intent(kind)
    image = row["request"]["vm_image"]
    dv_uid = str(uuid4())
    values["rootdisk_source"] = {
        "kind": "golden",
        "image": image,
        "namespace": config["namespace"],
        "name": "agent-vm-golden-" + hashlib.sha256(image.encode()).hexdigest()[:12],
        "dv_uid": dv_uid,
        "pvc_uid": str(uuid4()),
        "pvc_owner_dv_uid": dv_uid,
        "image_ref": image,
        "registry_source": {"registry": {"url": "docker://" + image}},
        "storage": {
            "accessModes": ["ReadWriteOnce"],
            "volumeMode": "Filesystem",
            "storageClassName": config["storage_class"],
            "resources": {"requests": {"storage": config["golden_disk_size"]}},
        },
        "pvc_volume_mode": "Filesystem",
    }
    if workspace and kind == "rootdisk":
        values["object_name"] = storage_name(row["request"]["workspace_storage"])
    body = await actuator.body(row, values)
    validate_final(kind, body, ctrl, row, values)
    assert body["metadata"]["labels"]["srw.io/owner-id"] == row["job_id"]
    if kind == "rootdisk":
        body["spec"]["source"]["pvc"]["name"] = "different-source"
    else:
        body["spec"]["template"]["spec"]["volumes"][0]["dataVolume"]["name"] = (
            "different-root"
        )
    with pytest.raises(ResourceAdmissionError):
        validate_final(kind, body, ctrl, row, values)


@pytest.mark.asyncio
async def test_retained_vm_reference_is_valid_but_no_new_root_effect_is_allowed(
    final_case,
):
    from uuid import uuid4

    ctrl, actuator, row, intent = final_case
    values = intent("vm")
    values["expected_pvc_uid"] = values["current_pvc_uid"]
    values["retained_dv_uid"] = values["current_dv_uid"]
    values["rootdisk_source"] = {
        "kind": "retained",
        "pvc_uid": values["expected_pvc_uid"],
    }
    body = await actuator.body(row, values)
    validate_final("vm", body, ctrl, row, values)
    values.update(
        effect_kind="rootdisk",
        current_secret_uid=None,
        effect_nonce=str(uuid4()),
        object_name="agent-vm-" + row["job_id"] + "-rootdisk",
    )
    body = await actuator.body(row, values)
    with pytest.raises(ResourceAdmissionError):
        validate_final("rootdisk", body, ctrl, row, values)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["vm", "rootdisk"])
async def test_actual_prepared_source_body_preserves_receipt_and_clone(
    final_case, prepared_case, kind, monkeypatch
):
    from vm_controller.creation_configuration import resolve_creation_configuration
    from tests.test_vm_resource_policy import snapshot

    ctrl, actuator, row, intent = final_case
    service, source, prepared_request, _ = prepared_case
    from vm_controller import controller as settings
    from tests.test_vm_resource_policy import complete_policy

    policy = complete_policy()
    policy["namespace"] = source["namespace"]
    monkeypatch.setattr(settings, "VM_NAMESPACE", source["namespace"])
    actuator.namespace = source["namespace"]
    ctrl._workspace_preparation_service = service
    resolved = resolve_creation_configuration(
        ctrl,
        {**row["request"], **prepared_request},
        _resource_policy_snapshot=snapshot(policy),
    )
    row.update(resolved, job_id=prepared_request["job_id"])
    values = intent(kind)
    values["rootdisk_source"] = source
    body = await actuator.body(row, values)
    validate_final(kind, body, ctrl, row, values)
    if kind == "vm":
        body["metadata"]["annotations"]["srw.io/prepared-artifact"] = "{}"
    else:
        body["spec"]["source"]["pvc"]["namespace"] = "foreign"
    with pytest.raises(ResourceAdmissionError):
        validate_final(kind, body, ctrl, row, values)


@pytest.mark.asyncio
async def test_probe_presence_is_bound_by_template_bytes_even_with_same_profile(
    final_case,
):
    import yaml
    from vm_controller.creation_configuration import resolve_creation_configuration
    from tests.test_vm_resource_policy import snapshot

    ctrl, actuator, row, intent = final_case
    raw = yaml.safe_load(ctrl.template_text)
    probe = raw["spec"]["template"]["spec"].pop("readinessProbe")
    ctrl.template_text = yaml.safe_dump(raw)
    row.update(
        resolve_creation_configuration(
            ctrl, row["request"], _resource_policy_snapshot=snapshot()
        )
    )
    values = intent("vm")
    body = await actuator.body(row, values)
    validate_final("vm", body, ctrl, row, values)
    body["spec"]["template"]["spec"]["readinessProbe"] = probe
    with pytest.raises(ResourceAdmissionError):
        validate_final("vm", body, ctrl, row, values)
