"""Inspect the closed ordinary VM template profile without rendering secrets.

This is an unconnected prerequisite for authenticated resource configuration.
It proves static scheduling/guest inputs, not launcher host cost, final rendered
manifest equivalence, node binding, or permission to enable admission.
"""

from copy import deepcopy
import re

import yaml

from shared.kubernetes_quantities import normalize_byte_quantity
from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_placement import _labels, _unmatched_taints, affinity_label_keys


def _refuse():
    raise ResourceAdmissionError("unsupported_resource_template")


class _TemplateLoader(yaml.SafeLoader):
    def __init__(self, source):
        self.depth = self.nodes = 0
        super().__init__(source)

    def compose_node(self, parent, index):
        self.nodes += 1
        if self.check_event(yaml.AliasEvent) or self.nodes > 10000 or self.depth >= 32:
            _refuse()
        self.depth += 1
        try:
            return super().compose_node(parent, index)
        finally:
            self.depth -= 1

    def construct_mapping(self, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                _refuse()
            key = self.construct_object(key_node, deep=deep)
            if type(key) is not str or key in result:
                _refuse()
            result[key] = self.construct_object(value_node, deep=deep)
        return result


def _json_shape(value, *, static=False):
    pending = [(value, 0)]
    remaining = 10000
    while pending:
        item, depth = pending.pop()
        remaining -= 1
        if remaining < 0 or depth > 32:
            _refuse()
        if isinstance(item, dict):
            for key, child in item.items():
                if type(key) is not str:
                    _refuse()
                pending.extend(((key, depth + 1), (child, depth + 1)))
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        elif type(item) is str:
            if len(item.encode("utf-8")) > 1024 * 1024 or (static and "${" in item):
                _refuse()
        elif item is not None and type(item) not in (int, bool):
            _refuse()


def _object(value, allowed, required=()):
    if (
        not isinstance(value, dict)
        or set(value) - set(allowed)
        or set(required) - set(value)
    ):
        _refuse()
    return value


def inspect_resource_template(
    template_text, *, request, node_selector, tolerations, storage_class
):
    """Return bounded static facts; reject unsupported or ambiguous profiles.

    Environment overrides follow the existing renderer: only nonempty selectors
    or tolerations replace template values. No credentials are substituted.
    """
    try:
        if (
            not isinstance(template_text, str)
            or len(template_text.encode("utf-8")) > 1024 * 1024
        ):
            _refuse()
        raw = yaml.load(template_text, Loader=_TemplateLoader)
        _json_shape(raw)
        vm = _object(
            raw,
            {"apiVersion", "kind", "metadata", "spec"},
            {"apiVersion", "kind", "spec"},
        )
        if vm["apiVersion"] != "kubevirt.io/v1" or vm["kind"] != "VirtualMachine":
            _refuse()
        _object(vm.get("metadata", {}), {"name", "labels"})
        spec = _object(
            vm["spec"],
            {"runStrategy", "dataVolumeTemplates", "template"},
            {"runStrategy", "dataVolumeTemplates", "template"},
        )
        if spec["runStrategy"] != "RerunOnFailure":
            _refuse()
        template = _object(spec["template"], {"metadata", "spec"}, {"spec"})
        _object(template.get("metadata", {}), {"labels"})
        vmi = _object(
            template["spec"],
            {
                "domain",
                "networks",
                "volumes",
                "readinessProbe",
                "nodeSelector",
                "tolerations",
                "affinity",
                "schedulerName",
            },
            {"domain", "networks", "volumes"},
        )
        if vmi.get("schedulerName", "default-scheduler") != "default-scheduler":
            _refuse()
        # Validate even empty environment values; malformed false-like values
        # must not silently inherit a different template configuration.
        _labels(node_selector)
        _unmatched_taints({"spec": {}}, tolerations)
        selector = node_selector or vmi.get("nodeSelector", {})
        effective_tolerations = tolerations or vmi.get("tolerations", [])
        affinity = _object(vmi.get("affinity", {}), {"nodeAffinity"})
        node_affinity = _object(
            affinity.get("nodeAffinity", {}),
            {"requiredDuringSchedulingIgnoredDuringExecution"},
        )
        required = node_affinity.get("requiredDuringSchedulingIgnoredDuringExecution")
        for value in (selector, effective_tolerations, required):
            _json_shape(value, static=True)
        _labels(selector)
        _unmatched_taints({"spec": {}}, effective_tolerations)
        affinity_label_keys(required)

        domain = _object(
            vmi["domain"],
            {"cpu", "memory", "devices", "machine"},
            {"cpu", "memory", "devices", "machine"},
        )
        cpu = _object(domain["cpu"], {"cores"}, {"cores"})["cores"]
        memory = _object(domain["memory"], {"guest"}, {"guest"})["guest"]
        cores = request["cpu_cores"]
        if type(cores) is not int or not 1 <= cores < 2**63:
            _refuse()
        if cpu != "${CPU_CORES}" and (type(cpu) is not int or cpu != cores):
            _refuse()
        guest_bytes = normalize_byte_quantity(request["memory"]).normalized_value
        if not 0 < guest_bytes < 2**63:
            _refuse()
        if (
            memory != "${MEMORY}"
            and normalize_byte_quantity(memory).normalized_value != guest_bytes
        ):
            _refuse()
        if domain["machine"] != {"type": "q35"} or domain["devices"] != {
            "disks": [
                {"name": "rootdisk", "disk": {"bus": "virtio"}},
                {"name": "cloud-init", "disk": {"bus": "virtio"}},
            ],
            "interfaces": [{"name": "default", "masquerade": {}}],
        }:
            _refuse()
        if vmi["networks"] != [{"name": "default", "pod": {}}] or vmi["volumes"] != [
            {"name": "rootdisk", "dataVolume": {"name": "agent-vm-${JOB_ID}-rootdisk"}},
            {
                "name": "cloud-init",
                "cloudInitNoCloud": {
                    "secretRef": {"name": "agent-vm-${JOB_ID}-cloudinit"}
                },
            },
        ]:
            _refuse()
        if (
            not isinstance(storage_class, str)
            or len(storage_class) > 253
            or not re.fullmatch(r"[a-z0-9]([a-z0-9.-]*[a-z0-9])?", storage_class)
        ):
            _refuse()
        disks = spec["dataVolumeTemplates"]
        if not isinstance(disks, list) or len(disks) != 1:
            _refuse()
        root = _object(disks[0], {"metadata", "spec"}, {"metadata", "spec"})
        metadata = _object(root["metadata"], {"name", "labels"}, {"name"})
        if metadata["name"] != "agent-vm-${JOB_ID}-rootdisk":
            _refuse()
        root_spec = _object(root["spec"], {"storage", "source"}, {"storage", "source"})
        if root_spec["source"] != {"registry": {"url": "docker://${VM_IMAGE}"}}:
            _refuse()
        storage_fields = {"storageClassName", "volumeMode", "accessModes", "resources"}
        storage = _object(root_spec["storage"], storage_fields, storage_fields)
        resources = _object(storage["resources"], {"requests"}, {"requests"})
        disk_size = _object(resources["requests"], {"storage"}, {"storage"})["storage"]
        if disk_size != "${VM_DISK_SIZE}":
            expected_bytes = normalize_byte_quantity(
                request["disk_size"]
            ).normalized_value
            if (
                expected_bytes <= 0
                or normalize_byte_quantity(disk_size).normalized_value != expected_bytes
            ):
                _refuse()
        if storage["storageClassName"] not in {"${VM_STORAGE_CLASS}", storage_class}:
            _refuse()
        if storage.get("volumeMode") != "Filesystem" or storage.get("accessModes") != [
            "ReadWriteOnce"
        ]:
            _refuse()
        return deepcopy(
            {
                "version": 1,
                "selector": selector,
                "tolerations": effective_tolerations,
                "required_affinity": required,
                "storage_class": storage_class,
                "guest_vcpus": cores,
                "guest_memory_bytes": guest_bytes,
            }
        )
    except (
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        yaml.YAMLError,
        RecursionError,
        OverflowError,
    ):
        # Never echo YAML parser exceptions: they may include secret source.
        _refuse()
