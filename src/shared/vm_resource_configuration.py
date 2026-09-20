"""Closed resource estimate embedded in an opt-in frozen creation configuration.

This validates reproducible estimates, not actual launcher resource requests or
admission authority. Runtime policy installation and waiter creation are separate.
"""

from copy import deepcopy
import json
import re

from shared.vm_resource_admission import (
    HOST_COST_ALGORITHM,
    ResourceAdmissionError,
    ResourceVector,
    _integer,
)
from shared.vm_resource_placement import _labels, _unmatched_taints, affinity_label_keys
from shared.vm_resource_policy import (
    parse_host_cost_policy,
    validate_resource_policy_snapshot,
)
from shared.vm_resource_template import (
    RESOURCE_TEMPLATE_ALGORITHM,
    _json_shape,
    inspect_resource_template,
)


def _object(value, fields):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ResourceAdmissionError("invalid_resource_configuration")
    return value


def validate_resource_configuration(value, configuration):
    """Reject unknown semantics and recompute the vector from frozen guest facts."""
    try:
        _json_shape(value, static=True)
        _object(
            value,
            {
                "version",
                "cluster_id",
                "policy_digest",
                "profile_algorithm",
                "template_profile",
                "host_mapping",
            },
        )
        if type(value["version"]) is not int or value["version"] != 1:
            raise ValueError
        cluster = value["cluster_id"]
        if (
            not isinstance(cluster, str)
            or len(cluster) > 253
            or re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", cluster) is None
        ):
            raise ValueError
        if (
            not isinstance(value["policy_digest"], str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", value["policy_digest"]) is None
            or value["profile_algorithm"] != RESOURCE_TEMPLATE_ALGORITHM
        ):
            raise ValueError
        profile = _object(
            value["template_profile"],
            {
                "version",
                "selector",
                "tolerations",
                "required_affinity",
                "storage_class",
                "guest_vcpus",
                "guest_memory_bytes",
            },
        )
        if type(profile["version"]) is not int or profile["version"] != 1:
            raise ValueError
        _integer(profile["guest_vcpus"], positive=True)
        _integer(profile["guest_memory_bytes"], positive=True)
        _labels(profile["selector"])
        _unmatched_taints({"spec": {}}, profile["tolerations"])
        affinity_label_keys(profile["required_affinity"])
        storage = profile["storage_class"]
        if (
            not isinstance(storage, str)
            or len(storage) > 253
            or re.fullmatch(r"[a-z0-9]([a-z0-9.-]*[a-z0-9])?", storage) is None
            or storage != configuration["storage_class"]
        ):
            raise ValueError
        # Nonempty controller settings override the source template. Empty
        # settings intentionally preserve static template constraints.
        for source, target in (
            ("node_selector", "selector"),
            ("tolerations", "tolerations"),
        ):
            if configuration[source] and configuration[source] != profile[target]:
                raise ValueError
        mapping = _object(value["host_mapping"], {"algorithm", "policy", "vector"})
        if mapping["algorithm"] != HOST_COST_ALGORITHM:
            raise ValueError
        policy = parse_host_cost_policy(mapping["policy"])
        vector = _object(
            mapping["vector"], {"cpu_millicores", "memory_bytes", "kvm_devices"}
        )
        actual = ResourceVector(**vector)
        expected = policy.cost(
            profile["guest_vcpus"], str(profile["guest_memory_bytes"])
        )
        if actual != expected:
            raise ValueError
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise ResourceAdmissionError("invalid_resource_configuration") from None


def build_resource_configuration(snapshot, *, template, request, configuration):
    """Only the private resolver supplies the complete operator policy snapshot."""
    snapshot = validate_resource_policy_snapshot(snapshot)
    if snapshot.inventory.namespace != configuration["namespace"]:
        raise ResourceAdmissionError("invalid_resource_configuration")
    profile = inspect_resource_template(
        template,
        request=request,
        node_selector=configuration["node_selector"],
        tolerations=configuration["tolerations"],
        storage_class=configuration["storage_class"],
    )
    result = {
        "version": 1,
        "cluster_id": snapshot.inventory.cluster_id,
        "policy_digest": snapshot.policy_digest,
        "profile_algorithm": RESOURCE_TEMPLATE_ALGORITHM,
        "template_profile": profile,
        "host_mapping": {
            "algorithm": HOST_COST_ALGORITHM,
            "policy": json.loads(snapshot.canonical_document)["policy"]["hostCost"],
            "vector": snapshot.host_cost.cost(
                profile["guest_vcpus"], str(profile["guest_memory_bytes"])
            ).to_dict(),
        },
    }
    validate_resource_configuration(result, configuration)
    return deepcopy(result)
