"""Full original clone provenance for retained prepared workspace inheritance.

Pure validation establishes structure/semantics only. The SQL authority must
select and compare the complete origin against its immutable observed effect.
"""

from copy import deepcopy
import json
from uuid import UUID


def retained_prepared_source(origin):
    root = origin["root"]
    return {
        **deepcopy(origin["source"]),
        "mode": "retained",
        "retained_root": {
            "name": root["name"],
            "dv_uid": root["uid"],
            "pvc_uid": root["pvc_uid"],
        },
        "inherited_origin": {
            key: origin[key] for key in ("request_id", "effect_nonce")
        },
    }


def validate_inherited_preparation(origin, *, request, configuration=None):
    from shared.vm_creation_issuance import validate_rootdisk_source
    from shared.vm_workspace_storage import storage_binding, storage_name
    from shared.workspace_preparation import normalized_image, validate_request

    if not isinstance(origin, dict) or set(origin) != {
        "request_id",
        "effect_nonce",
        "request",
        "configuration",
        "source",
        "root",
    }:
        raise ValueError("Inherited prepared origin is incomplete")
    for field in ("request_id", "effect_nonce"):
        if (
            not isinstance(origin[field], str)
            or str(UUID(origin[field])) != origin[field]
        ):
            raise ValueError("Inherited prepared origin identity changed")
    original = origin["request"]
    source, root = origin["source"], origin["root"]
    if not all(
        isinstance(value, dict)
        for value in (original, source, root, origin["configuration"])
    ):
        raise ValueError("Inherited prepared origin fields are incomplete")
    old = validate_request(original["preparation"])
    current = validate_request(request["preparation"])
    binding, initial = (
        storage_binding(request["workspace_storage"]),
        storage_binding(original["workspace_storage"]),
    )
    if (
        source.get("mode") != "clone"
        or "inherited_origin" in source
        or binding["owner_kind"] != "job"
        or binding["owner_id"] != original["job_id"]
        or binding["owner_id"] == request["job_id"]
        or binding["generation"] < 2
        or any(
            binding[key] != initial[key] for key in ("uid", "owner_kind", "owner_id")
        )
        or initial["generation"] != 1
        or initial["pvc_uid"] is not None
        or current["ownerKind"] != "job"
        or current["allocationId"] != request["job_id"]
        or old["allocationId"] != original["job_id"]
        or current["cache"] != "Reuse"
        or {k: v for k, v in current.items() if k not in {"allocationId", "revision"}}
        != {k: v for k, v in old.items() if k not in {"allocationId", "revision"}}
        or normalized_image(request["vm_image"])
        != normalized_image(original["vm_image"])
        or root.get("outcome") != "observed"
        or root.get("kind") != "rootdisk"
        or root.get("name") != storage_name(binding)
        or root.get("namespace") != origin["configuration"]["namespace"]
        or root.get("pvc_uid") != binding["pvc_uid"]
    ):
        raise ValueError("Inherited preparation semantics changed")
    for field in ("uid", "pvc_uid"):
        if not isinstance(root[field], str) or str(UUID(root[field])) != root[field]:
            raise ValueError("Inherited prepared target identity changed")
    if configuration is not None and (
        configuration["namespace"] != origin["configuration"]["namespace"]
        or json.dumps(configuration["preparation"], sort_keys=True)
        != json.dumps(origin["configuration"]["preparation"], sort_keys=True)
    ):
        raise ValueError("Inherited preparation configuration changed")
    validate_rootdisk_source(
        source,
        request=original,
        configuration=origin["configuration"],
        expected_pvc_uid=None,
    )


def validate_inherited_source(source, *, request, configuration, expected_pvc_uid):
    """Structural/semantic validation, not authority for the origin reference.

    The complete source remains on the carrier. SQL must separately compare it
    with retained_prepared_source(the full immutable original clone document).
    Do not copy original unrelated request fields into Kubernetes annotations.
    """
    reference = source["inherited_origin"]
    if not isinstance(reference, dict) or set(reference) != {
        "request_id",
        "effect_nonce",
    }:
        raise ValueError("Inherited prepared reference is incomplete")
    clone = {
        k: v
        for k, v in source.items()
        if k not in {"retained_root", "inherited_origin"}
    }
    clone = {**clone, "mode": "clone"}
    root = source["retained_root"]
    if (
        set(root) != {"name", "dv_uid", "pvc_uid"}
        or root["pvc_uid"] != expected_pvc_uid
        or source["mode"] != "retained"
    ):
        raise ValueError("Inherited prepared target changed")
    # Only the fields consumed by pure preparation validation are constructed;
    # this is not promoted to an original canonical request or a proven origin.
    original = {
        "job_id": clone["allocation"]["request"]["allocationId"],
        "preparation": clone["allocation"]["request"],
        "workspace_storage": clone["target"]["workspace_storage"],
        "vm_image": clone["allocation"]["request"]["image"],
    }
    validate_inherited_preparation(
        {
            **reference,
            "request": original,
            "configuration": configuration,
            "source": clone,
            "root": {
                "kind": "rootdisk",
                "outcome": "observed",
                "namespace": configuration["namespace"],
                "name": root["name"],
                "uid": root["dv_uid"],
                "pvc_uid": root["pvc_uid"],
            },
        },
        request=request,
        configuration=configuration,
    )


def validate_completed_target(origin, dv, pvc):
    """Reobserve the actual clone target, never substitute the artifact receipt."""
    from shared.vm_creation_issuance import public_effect_observation

    original, root = origin["request"], origin["root"]
    values = {
        "version": 2,
        "effect_kind": "rootdisk",
        "job_id": original["job_id"],
        "object_name": root["name"],
        "expected_pvc_uid": None,
        "effect_nonce": origin["effect_nonce"],
        "retry_request_id": origin["request_id"],
        "provision_generation": original["provision_generation"],
        "rootdisk_source": origin["source"],
    }
    observed = public_effect_observation(
        values,
        {"metadata": {"namespace": root["namespace"]}},
        {"outcome": "observed", "object": dv, "pvc": pvc},
    )
    if (
        observed != root
        or dv.get("status", {}).get("phase") != "Succeeded"
        or pvc.get("status", {}).get("phase") != "Bound"
        or pvc.get("spec", {}).get("volumeMode", "Filesystem") != "Filesystem"
        or not any(
            ref.get("kind") == "DataVolume"
            and ref.get("uid") == root["uid"]
            and ref.get("controller") is True
            for ref in pvc["metadata"].get("ownerReferences", [])
        )
    ):
        raise ValueError("Inherited prepared target completion is unproven")
