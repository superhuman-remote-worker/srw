"""Pure final resource-contract checks; never create or reserve infrastructure."""

from copy import deepcopy
import re

from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_resource_placement import affinity_label_keys
from shared.vm_resource_template import _json_shape


def conjoin_required_hostname(required_affinity, hostname: str) -> dict:
    """Add one AND requirement to every satisfiable-shaped original OR branch.

    Empty terms are impossible in Kubernetes. Adding an expression to such a
    term would broaden eligibility, so they must remain empty/impossible.
    """
    try:
        _json_shape(required_affinity, static=True)
        affinity_label_keys(required_affinity)
        if (
            not isinstance(hostname, str)
            or not 1 <= len(hostname) <= 63
            or re.fullmatch(r"[A-Za-z0-9](?:[-_.A-Za-z0-9]*[A-Za-z0-9])?", hostname)
            is None
        ):
            raise ValueError
        condition = {
            "key": "kubernetes.io/hostname",
            "operator": "In",
            "values": [hostname],
        }
        if required_affinity is None:
            return {"nodeSelectorTerms": [{"matchExpressions": [condition]}]}
        result = deepcopy(required_affinity)
        for term in result["nodeSelectorTerms"]:
            if not term.get("matchExpressions") and not term.get("matchFields"):
                continue
            expressions = term.setdefault("matchExpressions", [])
            if condition not in expressions:
                expressions.append(deepcopy(condition))
        affinity_label_keys(result)
        return result
    except (ValueError, TypeError, KeyError, RecursionError):
        raise ResourceAdmissionError("invalid_resource_affinity") from None


def _encoded(value):
    import json

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _metadata(raw, replacements):
    """Resolve only nonsecret metadata slots; never render the VM/cloud-init."""
    from shared.vm_resource_placement import _labels

    def text(value):
        if not isinstance(value, str):
            raise ValueError
        for key, replacement in replacements.items():
            value = value.replace("${" + key + "}", replacement)
        if "${" in value:
            raise ValueError
        return value

    result = {}
    if "name" in raw:
        result["name"] = text(raw["name"])
    if "labels" in raw:
        result["labels"] = {
            text(key): text(value) for key, value in raw["labels"].items()
        }
        # Refuse substitution collisions rather than silently discarding a label.
        if len(result["labels"]) != len(raw["labels"]):
            raise ValueError
        _labels(result["labels"])
    return result


def _contract(template_text, request, configuration, effect_intent, kind):
    import hashlib
    import yaml
    from shared.vm_creation_retry import canonical_request_digest
    from shared.vm_creation_issuance import (
        _values,
        canonical_configuration_digest,
        validate_rootdisk_source,
    )
    from shared.vm_resource_template import inspect_resource_template, _TemplateLoader
    from shared.vm_workspace_storage import storage_binding, storage_name

    values = _values(effect_intent)
    profile_only_vm = (
        kind == "vm" and configuration["version"] == 1 and "network_profile" in request
    )
    if (
        values["effect_kind"] != kind
        or (configuration["version"] not in (2, 3) and not profile_only_vm)
        or (configuration["version"] == 3) != (values["version"] in (4, 5))
        or canonical_configuration_digest(configuration)
        != values["controller_configuration_digest"]
        or canonical_request_digest(request) != values["request_digest"]
        or values["job_id"] != request["job_id"]
        or values["provision_generation"] != request["provision_generation"]
        or "sha256:" + hashlib.sha256(template_text.encode()).hexdigest()
        != configuration["vm_template_digest"]
    ):
        raise ValueError
    profile = inspect_resource_template(
        template_text,
        request=request,
        node_selector=configuration["node_selector"],
        tolerations=configuration["tolerations"],
        storage_class=configuration["storage_class"],
    )
    if configuration["version"] in (2, 3) and _encoded(profile) != _encoded(
        configuration["resource_admission"]["template_profile"]
    ):
        raise ValueError
    source = values["rootdisk_source"]
    validate_rootdisk_source(
        source,
        request=request,
        configuration=configuration,
        expected_pvc_uid=values["expected_pvc_uid"],
    )
    from shared.vm_network_profile import NETWORK_PROFILE

    if "network_profile" in request:
        if request["network_profile"] != NETWORK_PROFILE or configuration.get(
            "network_profile_policy"
        ) != {
            "version": 1,
            "image": request["vm_image"],
            "profile": NETWORK_PROFILE,
        }:
            raise ValueError
    elif "network_profile_policy" in configuration:
        raise ValueError
    binding = request.get("workspace_storage")
    if binding is not None:
        binding = storage_binding(binding)
        if (
            values["version"] in (3, 5)
            and values["workspace_attachment"]["binding"] != binding
        ):
            raise ValueError
    root_name = (
        storage_name(binding)
        if binding
        else "agent-vm-" + request["job_id"] + "-rootdisk"
    )
    raw = yaml.load(template_text, Loader=_TemplateLoader)
    return raw, profile, source, binding, root_name


def _stamp(metadata, request, values=None):
    labels = metadata.setdefault("labels", {})
    labels.update({
        "srw.io/owner-kind": request.get("entity_type", "job"),
        "srw.io/owner-id": request["job_id"],
    })
    annotations = metadata.setdefault("annotations", {})
    annotations["srw.io/provision-generation"] = request["provision_generation"]
    if values is not None:
        from shared.vm_creation_issuance import (
            EFFECT_NONCE_ANNOTATION,
            REQUEST_ANNOTATION,
        )

        annotations.update(
            {
                EFFECT_NONCE_ANNOTATION: values["effect_nonce"],
                REQUEST_ANNOTATION: values["retry_request_id"],
            }
        )


def validate_final_vm_manifest(
    manifest: dict,
    *,
    template_text: str,
    request: dict,
    configuration: dict,
    effect_intent: dict,
    reservation_hostname: str | None = None,
) -> None:
    """Compare the complete final VM body to digest-bound supported semantics.

    Inputs must already be authenticated by the effect protocol. This pure
    comparison does not prove live Node/PVC identity or grant an effect.
    """
    try:
        from shared.kubernetes_quantities import normalize_byte_quantity
        from shared.vm_workspace_storage import storage_labels
        from shared.vm_creation_issuance import prepared_source_metadata
        from shared.workspace_preparation import PREPARATION_LABEL, canonical

        _json_shape(manifest, static=True)
        raw, profile, source, binding, root_name = _contract(
            template_text, request, configuration, effect_intent, "vm"
        )
        replacements = {
            "JOB_ID": request["job_id"],
            "OWNER_ID": request["job_id"],
            "OWNER_KIND": "job",
            "NETWORK_TIER": request["network_tier"],
        }
        metadata = _metadata(raw.get("metadata", {}), replacements)
        if metadata.get("name") != "agent-vm-" + request["job_id"]:
            raise ValueError
        metadata["namespace"] = configuration["namespace"]
        template_metadata = _metadata(
            raw["spec"]["template"].get("metadata", {}), replacements
        )
        for meta in (metadata, template_metadata):
            if binding:
                meta.setdefault("labels", {}).update(
                    storage_labels(binding, request["job_id"])
                )
            if "resource_grant" in effect_intent:
                grant = effect_intent["resource_grant"]
                meta.setdefault("annotations", {}).update(
                    {
                        "srw.io/vm-resource-reservation": grant["id"],
                        "srw.io/vm-resource-node-uid": grant["node_uid"],
                        "srw.io/provision-generation": request["provision_generation"],
                    }
                )
        _stamp(metadata, request, effect_intent)
        _stamp(template_metadata, request)
        if source["kind"] == "prepared":
            metadata["labels"][PREPARATION_LABEL] = source["artifact"]["uid"]
            metadata["annotations"]["srw.io/prepared-artifact"] = canonical(
                prepared_source_metadata(source)
            )
        vmi = deepcopy(raw["spec"]["template"]["spec"])
        vmi["domain"]["cpu"]["cores"] = profile["guest_vcpus"]
        vmi["domain"]["memory"]["guest"] = str(profile["guest_memory_bytes"])
        if configuration["node_selector"]:
            vmi["nodeSelector"] = deepcopy(profile["selector"])
        if configuration["tolerations"]:
            vmi["tolerations"] = deepcopy(profile["tolerations"])
        if "resource_grant" in effect_intent:
            if reservation_hostname != effect_intent["resource_grant"]["node_name"]:
                raise ValueError
        if reservation_hostname is not None:
            vmi.setdefault("affinity", {}).setdefault("nodeAffinity", {})[
                "requiredDuringSchedulingIgnoredDuringExecution"
            ] = conjoin_required_hostname(
                profile["required_affinity"], reservation_hostname
            )
        vmi["volumes"] = [
            {"name": "rootdisk", "dataVolume": {"name": root_name}},
            {
                "name": "cloud-init",
                "cloudInitNoCloud": {
                    "secretRef": {
                        "name": "agent-vm-" + request["job_id"] + "-cloudinit"
                    }
                },
            },
        ]
        if "network_profile" in request:
            from shared.vm_network_profile import NETWORK_DATA

            vmi["volumes"][1]["cloudInitNoCloud"]["networkData"] = NETWORK_DATA
        expected = {
            "apiVersion": "kubevirt.io/v1",
            "kind": "VirtualMachine",
            "metadata": metadata,
            "spec": {
                "runStrategy": raw["spec"]["runStrategy"],
                "template": {"metadata": template_metadata, "spec": vmi},
            },
        }
        actual = deepcopy(manifest)
        memory = actual["spec"]["template"]["spec"]["domain"]["memory"]
        memory["guest"] = str(normalize_byte_quantity(memory["guest"]).normalized_value)
        if _encoded(actual) != _encoded(expected):
            raise ValueError
    except (
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        UnicodeError,
        RecursionError,
    ):
        raise ResourceAdmissionError("invalid_final_resource_manifest") from None


def validate_final_rootdisk_manifest(
    manifest: dict,
    *,
    template_text: str,
    request: dict,
    configuration: dict,
    effect_intent: dict,
) -> None:
    """Validate the separately issued DV; a retained root cannot create a new DV."""
    try:
        from shared.kubernetes_quantities import normalize_byte_quantity
        from shared.vm_workspace_storage import storage_labels

        _json_shape(manifest, static=True)
        raw, profile, source, binding, root_name = _contract(
            template_text, request, configuration, effect_intent, "rootdisk"
        )
        if (
            effect_intent["expected_pvc_uid"] is not None
            or source["kind"] == "retained"
            or source.get("mode") == "retained"
        ):
            raise ValueError
        if effect_intent["object_name"] != root_name:
            raise ValueError
        spec = deepcopy(raw["spec"]["dataVolumeTemplates"][0]["spec"])
        storage = spec["storage"]
        storage["storageClassName"] = profile["storage_class"]
        storage["resources"]["requests"]["storage"] = str(
            normalize_byte_quantity(request["disk_size"]).normalized_value
        )
        if source["kind"] in {"golden", "prepared"}:
            spec["source"] = {
                "pvc": {"name": source["name"], "namespace": source["namespace"]}
            }
        else:
            spec["source"] = {"registry": {"url": "docker://" + request["vm_image"]}}
        metadata = {
            "name": root_name,
            "namespace": configuration["namespace"],
            "labels": {
                "srw.io/rootdisk": "true",
                "job-id": request["job_id"],
                **(storage_labels(binding, request["job_id"]) if binding else {}),
            },
        }
        _stamp(metadata, request, effect_intent)
        expected = {
            "apiVersion": "cdi.kubevirt.io/v1beta1",
            "kind": "DataVolume",
            "metadata": metadata,
            "spec": spec,
        }
        actual = deepcopy(manifest)
        requests = actual["spec"]["storage"]["resources"]["requests"]
        requests["storage"] = str(
            normalize_byte_quantity(requests["storage"]).normalized_value
        )
        if _encoded(actual) != _encoded(expected):
            raise ValueError
    except (
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        UnicodeError,
        RecursionError,
    ):
        raise ResourceAdmissionError("invalid_final_resource_manifest") from None
