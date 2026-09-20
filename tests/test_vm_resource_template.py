"""Static template semantics, without credential rendering or Kubernetes I/O."""

from copy import deepcopy
from pathlib import Path
from textwrap import dedent

import pytest
import yaml

from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_template import inspect_resource_template


def shipped_template():
    text = (
        Path(__file__).parents[1] / "helm/templates/vm-controller/configmap.yaml"
    ).read_text()
    return dedent(
        text.split("  vm-template.yaml: |\n", 1)[1].split("  cloud-init.yaml: |", 1)[0]
    )


def inspect(document=None, **overrides):
    args = dict(
        request={"cpu_cores": 4, "memory": "8Gi"},
        node_selector={},
        tolerations=[],
        storage_class="local",
    )
    args.update(overrides)
    source = shipped_template() if document is None else yaml.safe_dump(document)
    return inspect_resource_template(source, **args)


def template():
    return yaml.safe_load(shipped_template())


def test_shipped_template_has_exact_guest_and_unconstrained_placement():
    result = inspect()
    assert result == {
        "version": 1,
        "selector": {},
        "tolerations": [],
        "required_affinity": None,
        "storage_class": "local",
        "guest_vcpus": 4,
        "guest_memory_bytes": 8 * 1024**3,
    }


def test_environment_empty_preserves_template_constraints_and_nonempty_overrides():
    doc = template()
    spec = doc["spec"]["template"]["spec"]
    spec["nodeSelector"] = {"zone": "a"}
    spec["tolerations"] = [{"key": "vm", "operator": "Exists", "effect": "NoSchedule"}]
    required = {
        "nodeSelectorTerms": [
            {
                "matchExpressions": [
                    {"key": "zone", "operator": "In", "values": ["a", "b"]}
                ]
            }
        ]
    }
    spec["affinity"] = {
        "nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": required}
    }
    before = deepcopy(doc)
    result = inspect(doc)
    assert result["selector"] == {"zone": "a"}
    assert result["tolerations"] == spec["tolerations"]
    assert result["required_affinity"] == required
    other = inspect(
        doc, node_selector={"zone": "b"}, tolerations=[{"operator": "Exists"}]
    )
    assert other["selector"] == {"zone": "b"}
    assert other["tolerations"] == [{"operator": "Exists"}]
    assert other["required_affinity"] == required
    assert doc == before


def test_empty_affinity_remains_impossible_not_absent():
    doc = template()
    doc["spec"]["template"]["spec"]["affinity"] = {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": []}
        }
    }
    assert inspect(doc)["required_affinity"] == {"nodeSelectorTerms": []}


@pytest.mark.parametrize(
    "field,value",
    [
        ("nodeName", "node-a"),
        ("schedulerName", "custom"),
        ("topologySpreadConstraints", []),
        ("schedulingGates", []),
        ("affinity", {"podAffinity": {}}),
        (
            "affinity",
            {"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": []}},
        ),
        ("nodeSelector", {"zone": "${ZONE}"}),
        ("nodeSelector", {"${KEY}": "a"}),
        ("nodeSelector", {"zone": True}),
        ("tolerations", [{"operator": "Unknown"}]),
        ("futureSchedulingConstraint", {}),
    ],
)
def test_unsupported_scheduling_is_refused(field, value):
    doc = template()
    doc["spec"]["template"]["spec"][field] = value
    with pytest.raises(ResourceAdmissionError):
        inspect(doc)


@pytest.mark.parametrize(
    "field,value",
    [
        ("cpu", {"cores": "${CPU_CORES}", "dedicatedCpuPlacement": True}),
        ("cpu", {"cores": 8}),
        ("cpu", {"cores": True}),
        ("memory", {"guest": "16Gi"}),
        ("memory", {"guest": "${OTHER}"}),
        ("memory", {"guest": "${MEMORY}", "hugepages": {"pageSize": "2Mi"}}),
        ("resources", {"requests": {"cpu": "8"}}),
    ],
)
def test_custom_or_mismatched_guest_demand_is_refused(field, value):
    doc = template()
    doc["spec"]["template"]["spec"]["domain"][field] = value
    with pytest.raises(ResourceAdmissionError):
        inspect(doc)


@pytest.mark.parametrize(
    "mutation", ["gpu", "host_device", "instancetype", "storage_class"]
)
def test_hidden_demand_and_storage_mismatch_are_refused(mutation):
    doc = template()
    if mutation in {"gpu", "host_device"}:
        devices = doc["spec"]["template"]["spec"]["domain"]["devices"]
        devices["gpus" if mutation == "gpu" else "hostDevices"] = []
    elif mutation == "instancetype":
        doc["spec"]["instancetype"] = {"name": "large"}
    else:
        doc["spec"]["dataVolumeTemplates"][0]["spec"]["storage"]["storageClassName"] = (
            "different"
        )
    with pytest.raises(ResourceAdmissionError):
        inspect(doc)


@pytest.mark.parametrize(
    "source",
    [
        "kind: VirtualMachine\nkind: VirtualMachine\n",
        "root: &loop [*loop]",
        "root: " + "[" * 100 + "0" + "]" * 100,
        "kind: VirtualMachine\n---\nkind: VirtualMachine\n",
        "root: " + "x" * (1024 * 1024),
    ],
)
def test_ambiguous_or_unbounded_yaml_is_refused_without_echoing_input(source):
    with pytest.raises(ResourceAdmissionError) as error:
        inspect_resource_template(
            source,
            request={"cpu_cores": 4, "memory": "8Gi"},
            node_selector={},
            tolerations=[],
            storage_class="local",
        )
    assert str(error.value) == "unsupported_resource_template"
