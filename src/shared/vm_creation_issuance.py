"""Authenticated source-specific evidence on existing workspace cleanup Leases.

This seal proves the trusted controller bound an observed Kubernetes Lease UID
and namespace to an exact create intent. It is not itself a permission to act;
only the database's successful begin-effect CAS grants one side effect.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import hmac
import json
import re
from uuid import UUID

CREATION_SOURCE = "controller_vm_create"
CREATION_INTENT_ANNOTATION = "srw.io/vm-create-intent"
CREATION_SIGNATURE_ANNOTATION = "srw.io/vm-create-carrier-signature"
EFFECT_NONCE_ANNOTATION = "srw.io/vm-create-effect-nonce"
REQUEST_ANNOTATION = "srw.io/vm-create-request-id"
EFFECT_KINDS = ("rootdisk", "cloud_init", "vm")
_FIELDS = frozenset(
    {
        "version",
        "source",
        "admission_id",
        "reservation_request_id",
        "intent_digest",
        "retry_request_id",
        "job_id",
        "provision_generation",
        "request_digest",
        "controller_configuration_digest",
        "expected_pvc_uid",
        "retained_dv_uid",
        "current_dv_uid",
        "current_pvc_uid",
        "current_secret_uid",
        "effect_kind",
        "effect_nonce",
        "object_name",
    }
)


def _uuid(value):
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError("Noncanonical creation identity")


def validate_rootdisk_source(source, *, request, configuration, expected_pvc_uid):
    """Validate complete typed clone input against immutable admitted semantics."""
    if not isinstance(source, Mapping):
        raise ValueError("Rootdisk source is unproven")
    if "network_profile" in request:
        from shared.vm_network_profile import NETWORK_PROFILE, compatible_image

        if (
            request["network_profile"] != NETWORK_PROFILE
            or configuration.get("network_profile_policy") != {
                "version": 1,
                "image": request["vm_image"],
                "profile": NETWORK_PROFILE,
            }
            or not compatible_image(request["vm_image"], allowlist=request["vm_image"])
            or source.get("kind") not in {"registry", "golden", "retained"}
            or request.get("preparation") is not None
        ):
            raise ValueError("Rootdisk network profile source is unproven")
    if request.get("preparation") is not None:
        if "inherited_origin" in source:
            from shared.vm_inherited_preparation import validate_inherited_source

            return validate_inherited_source(
                source,
                request=request,
                configuration=configuration,
                expected_pvc_uid=expected_pvc_uid,
            )
        return _validate_prepared_source(
            source,
            request=request,
            configuration=configuration,
            expected_pvc_uid=expected_pvc_uid,
        )
    if expected_pvc_uid is not None:
        if source != {"kind": "retained", "pvc_uid": expected_pvc_uid}:
            raise ValueError("Retained rootdisk source changed")
        _uuid(expected_pvc_uid)
        return
    image = request["vm_image"]
    if source.get("kind") == "registry":
        if configuration["golden_enabled"] or source != {
            "kind": "registry",
            "image": image,
        }:
            raise ValueError("Registry source changed")
        return
    expected = {
        "kind": "golden",
        "image": image,
        "namespace": configuration["namespace"],
        "name": "agent-vm-golden-" + hashlib.sha256(image.encode()).hexdigest()[:12],
        "dv_uid": source.get("dv_uid"),
        "pvc_uid": source.get("pvc_uid"),
        "pvc_owner_dv_uid": source.get("dv_uid"),
        "image_ref": image,
        "registry_source": {"registry": {"url": "docker://" + image}},
        "storage": {
            "accessModes": ["ReadWriteOnce"],
            "volumeMode": "Filesystem",
            "storageClassName": configuration["storage_class"],
            "resources": {"requests": {"storage": configuration["golden_disk_size"]}},
        },
        "pvc_volume_mode": "Filesystem",
    }
    if not configuration["golden_enabled"] or dict(source) != expected:
        raise ValueError("Golden rootdisk source changed")
    _uuid(source["dv_uid"])
    _uuid(source["pvc_uid"])


def _validate_prepared_source(source, *, request, configuration, expected_pvc_uid):
    from shared.workspace_preparation import (
        validate_request,
        normalized_image,
        image_reference,
        cache_key,
        revision,
    )
    from shared.workspace_preparation_settings import PreparationSettings, disk_bytes

    if source.get("kind") != "prepared":
        raise ValueError("Prepared rootdisk source is unproven")
    preparation = validate_request(request["preparation"])
    settings = PreparationSettings(**configuration["preparation"])
    mode = "retained" if expected_pvc_uid is not None else "clone"
    fields = {
        "kind",
        "mode",
        "namespace",
        "name",
        "dv_uid",
        "pvc_uid",
        "allocation",
        "artifact",
        "receipt",
    }
    if mode == "retained":
        fields.add("retained_root")
    binding = request.get("workspace_storage")
    if binding is not None:
        from shared.vm_preparation_target import validate_workspace_target

        fields.add("target")
        validate_workspace_target(
            source.get("target"),
            allocation_id=request["job_id"],
            namespace=configuration["namespace"],
            binding=binding,
        )
    if (
        set(source) != fields
        or source["mode"] != mode
        or not settings.enabled
        or source["namespace"] != configuration["namespace"]
        or preparation["ownerKind"] != "job"
        or preparation["allocationId"] != request["job_id"]
        or normalized_image(request["vm_image"]) != preparation["image"]
    ):
        raise ValueError("Prepared source request changed")
    allocation, artifact = source["allocation"], source["artifact"]
    if (
        set(allocation) != {"name", "uid", "request"}
        or set(artifact) != {"name", "uid", "request"}
        or allocation["request"] != preparation
        or allocation["name"]
        != "srw-prep-allocation-" + revision(["job", request["job_id"], None])[:32]
    ):
        raise ValueError("Prepared allocation identity changed")
    for uid in (
        allocation["uid"],
        artifact["uid"],
        source["dv_uid"],
        source["pvc_uid"],
    ):
        _uuid(uid)
    artifact_request = artifact["request"]
    base, builder = artifact_request["baseImage"], artifact_request["builderImage"]
    for resolved, requested in (
        (base, preparation["image"]),
        (builder, settings.builder_image),
    ):
        actual, requested = image_reference(resolved), image_reference(requested)
        if (
            not actual[2].startswith("sha256:")
            or actual[0] not in settings.registry_hosts
            or normalized_image(resolved) != resolved
            or actual[:2] != requested[:2]
            or requested[2].startswith("sha256:")
            and actual[2] != requested[2]
        ):
            raise ValueError("Prepared resolved image changed")
    network = (
        {"revision": settings.network_policy_revision, "podFirewall": settings.firewall}
        if settings.pod_firewall
        else settings.network_policy_revision
        if settings.network_enabled
        else "offline"
    )
    key = cache_key(
        preparation,
        base_image=base,
        builder_image=builder,
        disk_size=settings.disk_size,
        network_policy=network,
    )
    expected_artifact = {
        "scope": preparation["scope"],
        "baseImage": base,
        "builderImage": builder,
        "steps": preparation["steps"],
        "cacheKey": key,
        "networkEnabled": settings.network_enabled,
        "diskSize": settings.disk_size,
    }
    if settings.firewall is not None:
        expected_artifact["podFirewall"] = settings.firewall
    if (
        artifact_request != expected_artifact
        or artifact["name"] != "srw-prep-artifact-" + revision(key)[:32]
        or source["name"] != "srw-prepared-" + UUID(artifact["uid"]).hex
    ):
        raise ValueError("Prepared artifact semantics changed")
    receipt = source["receipt"]
    expected_receipt = {
        "version": 1,
        "buildUid": artifact["uid"],
        "pvcUid": source["pvc_uid"],
        "cacheKey": key,
        "phase": "Succeeded",
    }
    if (
        not isinstance(receipt, Mapping)
        or set(receipt) != {*expected_receipt, "diskSha256", "diskBytes"}
        or any(receipt.get(key) != value for key, value in expected_receipt.items())
        or type(receipt["version"]) is not int
        or type(receipt["diskBytes"]) is not int
        or not 0 < receipt["diskBytes"] <= disk_bytes(settings.disk_size)
        or not isinstance(receipt["diskSha256"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", receipt["diskSha256"])
    ):
        raise ValueError("Prepared artifact receipt changed")
    if mode == "retained":
        from shared.vm_workspace_storage import storage_name

        root = source["retained_root"]
        name = (
            storage_name(request["workspace_storage"])
            if request.get("workspace_storage")
            else "agent-vm-" + request["job_id"] + "-rootdisk"
        )
        if (
            set(root) != {"name", "dv_uid", "pvc_uid"}
            or root["name"] != name
            or root["pvc_uid"] != expected_pvc_uid
        ):
            raise ValueError("Prepared retained root changed")
        _uuid(root["dv_uid"])
        _uuid(root["pvc_uid"])


def _values(value):
    version = value.get("version") if isinstance(value, Mapping) else None
    fields = _FIELDS
    if version in (2, 3, 4, 5):
        fields |= {"rootdisk_source"}
    if version in (3, 5):
        fields |= {"workspace_attachment", "current_attachment_uid"}
    if version in (4, 5):
        fields |= {"resource_grant"}
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError("Incomplete creation carrier intent")
    value = dict(value)
    if (
        type(value["version"]) is not int
        or value["version"] not in (1, 2, 3, 4, 5)
        or value["source"] != CREATION_SOURCE
        or value["effect_kind"] not in (*EFFECT_KINDS, "workspace_attach")
    ):
        raise ValueError("Unsupported creation carrier source")
    attachment = value["effect_kind"] == "workspace_attach"
    if attachment and version not in (3, 5):
        raise ValueError("Attachment effects require their full carrier contract")
    if version in (3, 5):
        from shared.vm_creation_attachment import validate_attachment_intent
        from shared.vm_workspace_storage import storage_name

        intent = value["workspace_attachment"]
        if not isinstance(intent, Mapping):
            raise ValueError("Attachment intent is incomplete")
        validate_attachment_intent(
            intent,
            request={
                "workspace_storage": intent.get("binding"),
                "job_id": value["job_id"],
            },
            expected_pvc_uid=value["expected_pvc_uid"],
        )
        if attachment:
            if (
                value["rootdisk_source"] is not None
                or value["current_attachment_uid"] is not None
                or value["object_name"] != storage_name(intent["binding"])
            ):
                raise ValueError("Initial attachment carrier changed")
        elif value["current_attachment_uid"] is None or not isinstance(
            value["rootdisk_source"], Mapping
        ):
            raise ValueError("Observed attachment and root source are required")
        if value["current_attachment_uid"] is not None:
            _uuid(value["current_attachment_uid"])
    if version in (4, 5):
        from shared.vm_resource_admission import ResourceVector

        grant = value["resource_grant"]
        if not isinstance(grant, Mapping) or set(grant) != {
            "version", "id", "revision", "cluster_id", "policy_digest",
            "node_uid", "node_name", "vector", "snapshot_id", "snapshot_digest",
            "headroom",
        } or type(grant["version"]) is not int or grant["version"] != 1 or (
            type(grant["revision"]) is not int or not 1 <= grant["revision"] < 2**63
        ):
            raise ValueError("Resource creation grant is incomplete")
        for key in ("id", "node_uid", "snapshot_id"):
            _uuid(grant[key])
        for key in ("policy_digest", "snapshot_digest"):
            if not isinstance(grant[key], str) or not re.fullmatch(
                r"sha256:[0-9a-f]{64}", grant[key]
            ):
                raise ValueError("Resource creation grant digest is invalid")
        for key in ("cluster_id", "node_name"):
            if not isinstance(grant[key], str) or not 1 <= len(grant[key]) <= 253:
                raise ValueError("Resource creation grant node is invalid")
        ResourceVector.from_six_dict(grant["vector"])
        ResourceVector.from_six_dict(grant["headroom"])
    for key in (
        "admission_id",
        "reservation_request_id",
        "retry_request_id",
        "job_id",
        "provision_generation",
        "effect_nonce",
    ):
        _uuid(value[key])
    for key in ("intent_digest", "request_digest", "controller_configuration_digest"):
        if not isinstance(value[key], str) or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", value[key]
        ):
            raise ValueError("Invalid creation carrier digest")
    if (value["expected_pvc_uid"] is None) != (value["retained_dv_uid"] is None):
        raise ValueError("Incomplete retained disk identity")
    if (value["current_dv_uid"] is None) != (value["current_pvc_uid"] is None):
        raise ValueError("Incomplete current disk identity")
    if (
        value["effect_kind"] not in {"rootdisk", "workspace_attach"}
        and value["current_pvc_uid"] is None
    ):
        raise ValueError("Current disk identity required before later effects")
    if value["effect_kind"] == "vm" and value["current_secret_uid"] is None:
        raise ValueError("Current Secret identity required before VM creation")
    if value["effect_kind"] != "vm" and value["current_secret_uid"] is not None:
        raise ValueError("Unexpected Secret identity before its creation stage")
    for key in (
        "current_secret_uid",
        "expected_pvc_uid",
        "retained_dv_uid",
        "current_dv_uid",
        "current_pvc_uid",
    ):
        if value[key] is not None:
            _uuid(value[key])
    suffix = {
        "rootdisk": "-rootdisk",
        "cloud_init": "-cloudinit",
        "vm": "",
        "workspace_attach": "",
    }[value["effect_kind"]]
    # Reusable workspace bindings have their own rootdisk name. Such names must
    # be checked against the frozen binding by the store before granting.
    if (
        value["effect_kind"] not in {"rootdisk", "workspace_attach"}
        and value["object_name"] != f"agent-vm-{value['job_id']}{suffix}"
    ):
        raise ValueError("Foreign creation object name")
    if not isinstance(value["object_name"], str) or not re.fullmatch(
        r"[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?", value["object_name"]
    ):
        raise ValueError("Invalid creation object name")
    return value


def _signature(values, *, namespace, name, uid, secret):
    if not isinstance(secret, bytes) or not secret:
        raise ValueError("Creation carrier authentication unavailable")
    encoded = json.dumps(
        {
            "domain": "srw-vm-create-carrier-v1",
            "namespace": namespace,
            "name": name,
            "uid": uid,
            "values": values,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return hmac.new(secret, encoded, hashlib.sha256).hexdigest()


def seal_creation_carrier(values, *, namespace, uid, resource_version, secret):
    """Seal an already observed Lease UID; never invent a pre-admission UID."""
    values = _values(values)
    _uuid(uid)
    if (
        not isinstance(namespace, str)
        or not namespace
        or not isinstance(resource_version, str)
        or not resource_version
    ):
        raise ValueError("Creation carrier metadata is incomplete")
    name = f"srw-cleanup-{UUID(values['admission_id']).hex}"
    return {
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "namespace": namespace,
            "name": name,
            "uid": uid,
            "resourceVersion": resource_version,
            "labels": {"srw.io/vm-workspace-cleanup-carrier": "true"},
            "annotations": {
                CREATION_INTENT_ANNOTATION: json.dumps(
                    values, sort_keys=True, separators=(",", ":")
                ),
                CREATION_SIGNATURE_ANNOTATION: _signature(
                    values, namespace=namespace, name=name, uid=uid, secret=secret
                ),
            },
        },
        "spec": {"holderIdentity": values["admission_id"]},
    }


def verify_creation_carrier(carrier, *, secret):
    try:
        if (
            not isinstance(carrier, Mapping)
            or carrier["apiVersion"] != "coordination.k8s.io/v1"
            or carrier["kind"] != "Lease"
        ):
            raise ValueError("Invalid creation carrier kind")
        metadata = carrier["metadata"]
        values = _values(
            json.loads(metadata["annotations"][CREATION_INTENT_ANNOTATION])
        )
        expected = seal_creation_carrier(
            values,
            namespace=metadata["namespace"],
            uid=metadata["uid"],
            resource_version=metadata["resourceVersion"],
            secret=secret,
        )
        if (
            metadata.get("deletionTimestamp") is not None
            or metadata["name"] != expected["metadata"]["name"]
            or metadata["labels"].get("srw.io/vm-workspace-cleanup-carrier") != "true"
            or carrier["spec"].get("holderIdentity") != values["admission_id"]
            or not hmac.compare_digest(
                metadata["annotations"][CREATION_SIGNATURE_ANNOTATION],
                expected["metadata"]["annotations"][CREATION_SIGNATURE_ANNOTATION],
            )
        ):
            raise ValueError("Creation carrier authentication failed")
        return values
    except (KeyError, TypeError, AttributeError, json.JSONDecodeError) as exc:
        raise ValueError("Creation carrier evidence incomplete") from exc


def prepared_source_metadata(source):
    """Public receipt projection from an already validated prepared source."""
    return {
        "allocationId": source["allocation"]["request"]["allocationId"],
        "uid": source["artifact"]["uid"],
        "baseImage": source["artifact"]["request"]["baseImage"],
        **source["receipt"],
    }


def validate_prepared_vm_metadata(source, obj):
    from shared.workspace_preparation import PREPARATION_LABEL

    metadata = obj["metadata"]
    if metadata.get("labels", {}).get(PREPARATION_LABEL) != source["artifact"][
        "uid"
    ] or json.loads(
        metadata.get("annotations", {}).get("srw.io/prepared-artifact", "null")
    ) != prepared_source_metadata(source):
        raise ValueError("VM preparation receipt changed")


def public_effect_observation(
    values, carrier, observation, *, rootdisk=None, cloud_init=None,
    network_profile=None,
):
    """Extract exact public identities; never persist Secret data or API errors.

    The caller authenticates the controller transport and checks the carrier
    against its durable effect. Boolean assertions/absence are not evidence.
    """
    if not isinstance(observation, Mapping):
        raise ValueError("Invalid creation observation")
    if observation.get("outcome") == "rejected":
        status = observation.get("api_status")
        reasons = {
            400: "BadRequest",
            401: "Unauthorized",
            403: "Forbidden",
            404: "NotFound",
            405: "MethodNotAllowed",
            413: "RequestEntityTooLarge",
            415: "UnsupportedMediaType",
            422: "Invalid",
            429: "TooManyRequests",
        }
        if (
            not isinstance(status, Mapping)
            or status.get("apiVersion") != "v1"
            or status.get("kind") != "Status"
            or status.get("status") != "Failure"
            or type(status.get("code")) is not int
            or reasons.get(status["code"]) != status.get("reason")
        ):
            raise ValueError("API response does not prove rejection")
        return {
            "outcome": "rejected",
            "api_code": status["code"],
            "api_reason": status["reason"],
        }
    if observation.get("outcome") != "observed":
        raise ValueError("Unknown issuance cannot be settled from absence")
    try:
        kind = values["effect_kind"]
        if kind == "workspace_attach":
            from shared.vm_creation_attachment import attachment_observation

            return attachment_observation(
                values["workspace_attachment"],
                observation["object"],
                namespace=carrier["metadata"]["namespace"],
                request_id=values["retry_request_id"],
                effect_nonce=values["effect_nonce"],
                provision_generation=values["provision_generation"],
            )
        expected_kind = {
            "rootdisk": ("cdi.kubevirt.io/v1beta1", "DataVolume"),
            "cloud_init": ("v1", "Secret"),
            "vm": ("kubevirt.io/v1", "VirtualMachine"),
        }[kind]
        obj = observation["object"]
        metadata = obj["metadata"]
        uid = metadata["uid"]
        _uuid(uid)
        if (
            (obj["apiVersion"], obj["kind"]) != expected_kind
            or metadata["name"] != values["object_name"]
            or metadata["namespace"] != carrier["metadata"]["namespace"]
            or metadata.get("deletionTimestamp") is not None
        ):
            raise ValueError("Creation object identity changed")
        labels = metadata.get("labels") or {}
        annotations = metadata.get("annotations") or {}
        owner = values["job_id"]
        if kind == "rootdisk" and values["version"] in (3, 5):
            from shared.vm_creation_lineage import disk_owner

            owner = disk_owner(
                {
                    "job_id": values["job_id"],
                    "workspace_storage": values["workspace_attachment"]["binding"],
                }
            )
        if (
            labels.get("srw.io/owner-kind") != "job"
            or labels.get("srw.io/owner-id") != owner
        ):
            raise ValueError("Creation object owner changed")
        retained = kind == "rootdisk" and values["expected_pvc_uid"] is not None
        if retained:
            if uid != values["retained_dv_uid"]:
                raise ValueError("Retained DataVolume identity changed")
        elif (
            annotations.get(EFFECT_NONCE_ANNOTATION) != values["effect_nonce"]
            or annotations.get(REQUEST_ANNOTATION) != values["retry_request_id"]
            or annotations.get("srw.io/provision-generation")
            != values["provision_generation"]
        ):
            raise ValueError("Creation effect object provenance changed")
        result = {
            "outcome": "observed",
            "kind": kind,
            "name": metadata["name"],
            "uid": uid,
            "namespace": metadata["namespace"],
        }
        if kind == "rootdisk":
            if values["version"] in (2, 3, 4, 5) and not retained:
                source = values["rootdisk_source"]
                expected_source = (
                    {"pvc": {"namespace": source["namespace"], "name": source["name"]}}
                    if source["kind"] in {"golden", "prepared"}
                    else {"registry": {"url": "docker://" + source["image"]}}
                )
                if obj.get("spec", {}).get("source") != expected_source:
                    raise ValueError("Observed rootdisk source changed")
                if (
                    source["kind"] in {"golden", "prepared"}
                    and obj.get("spec", {}).get("storage", {}).get("volumeMode")
                    != "Filesystem"
                ):
                    raise ValueError("Observed clone volume mode changed")
            pvc = observation["pvc"]
            pvc_metadata = pvc["metadata"]
            _uuid(pvc_metadata["uid"])
            if (
                pvc["kind"] != "PersistentVolumeClaim"
                or pvc["apiVersion"] != "v1"
                or pvc_metadata["namespace"] != metadata["namespace"]
                or pvc_metadata["name"] != metadata["name"]
                or pvc_metadata.get("deletionTimestamp") is not None
            ):
                raise ValueError("Creation disk binding changed")
            if retained:
                if pvc_metadata["uid"] != values["expected_pvc_uid"]:
                    raise ValueError("Retained PVC identity changed")
            elif not any(
                ref.get("kind") == "DataVolume" and ref.get("uid") == uid
                for ref in pvc_metadata.get("ownerReferences", [])
            ):
                raise ValueError("New PVC does not belong to the admitted DataVolume")
            result["pvc_uid"] = pvc_metadata["uid"]
        elif kind == "cloud_init":
            fingerprint = annotations.get("srw.io/ssh-host-key-fingerprint")
            if not isinstance(fingerprint, str) or not re.fullmatch(
                r"SHA256:[A-Za-z0-9+/]{43}", fingerprint
            ):
                raise ValueError("Creation Secret host key identity missing")
            result["ssh_host_key_fingerprint"] = fingerprint
        else:
            if values["version"] in (4, 5):
                grant = values["resource_grant"]
                for meta in (
                    metadata,
                    obj["spec"]["template"]["metadata"],
                ):
                    stamped = meta.get("annotations") or {}
                    if (
                        stamped.get("srw.io/vm-resource-reservation") != grant["id"]
                        or stamped.get("srw.io/vm-resource-node-uid") != grant["node_uid"]
                        or stamped.get("srw.io/provision-generation")
                        != values["provision_generation"]
                    ):
                        raise ValueError("Observed VM resource identity changed")
            if values["version"] in (3, 5):
                from shared.vm_workspace_storage import storage_labels

                expected_labels = storage_labels(
                    values["workspace_attachment"]["binding"], values["job_id"]
                )
                for actual in (
                    labels,
                    obj["spec"]["template"]["metadata"].get("labels", {}),
                ):
                    if any(
                        actual.get(key) != value
                        for key, value in expected_labels.items()
                    ):
                        raise ValueError("VM attachment labels changed")
            source = values.get("rootdisk_source", {})
            if source.get("kind") == "prepared":
                validate_prepared_vm_metadata(source, obj)
            if (
                not isinstance(rootdisk, Mapping)
                or rootdisk.get("outcome") != "observed"
            ):
                raise ValueError("Admitted rootdisk evidence unavailable")
            volumes = obj["spec"]["template"]["spec"]["volumes"]
            roots = [volume for volume in volumes if volume.get("name") == "rootdisk"]
            if (
                len(roots) != 1
                or roots[0].get("dataVolume", {}).get("name") != rootdisk["name"]
            ):
                raise ValueError("VM does not reference the admitted rootdisk")
            if (
                not isinstance(cloud_init, Mapping)
                or cloud_init.get("outcome") != "observed"
            ):
                raise ValueError("Admitted cloud-init identity unavailable")
            cloud_volumes = [
                volume.get("cloudInitNoCloud")
                for volume in volumes
                if "cloudInitNoCloud" in volume
            ]
            if (
                len(cloud_volumes) != 1
                or cloud_volumes[0].get("secretRef", {}).get("name")
                != cloud_init["name"]
            ):
                raise ValueError("VM does not reference the admitted cloud-init Secret")
            if network_profile is not None:
                from shared.vm_network_profile import NETWORK_DATA, validate_network_profile

                validate_network_profile(network_profile)
                if cloud_volumes[0] != {
                    "secretRef": {"name": cloud_init["name"]},
                    "networkData": NETWORK_DATA,
                }:
                    raise ValueError("Observed VM network profile changed")
            result["cloud_init_uid"] = cloud_init["uid"]
            result["ssh_host_key_fingerprint"] = cloud_init["ssh_host_key_fingerprint"]
            result["pvc_uid"] = rootdisk["pvc_uid"]
        return result
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError("Creation object evidence incomplete") from exc


_CONFIGURATION_FIELDS = frozenset(
    {
        "version",
        "namespace",
        "vm_template_digest",
        "cloud_init_template_digest",
        "implementation_digest",
        "storage_class",
        "persistent_rootdisk",
        "node_selector",
        "tolerations",
        "nats_url",
        "orchestrator_id",
        "orchestrator_url",
        "headscale_url",
        "headscale_enabled",
        "headscale_api_url",
        "headscale_user",
        "headscale_key_expiry_minutes",
        "authorized_public_key_digest",
        "golden_enabled",
        "golden_disk_size",
        "preparation",
    }
)


def canonical_configuration_digest(configuration):
    """Validate/hash the entire versioned, nonsecret effective configuration."""
    from shared.vm_creation_retry import _validate_json

    if (
        not isinstance(configuration, dict)
        or type(configuration.get("version")) is not int
        or configuration["version"] not in (1, 2, 3)
        or set(configuration)
        != (
            _CONFIGURATION_FIELDS
            | ({"resource_admission"} if configuration["version"] in (2, 3) else set())
            | ({"network_profile_policy"} if "network_profile_policy" in configuration else set())
            | ({"disk_size_floor"} if "disk_size_floor" in configuration else set())
        )
    ):
        raise ValueError("Effective controller configuration is incomplete")
    if not isinstance(configuration["namespace"], str) or not re.fullmatch(
        r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?", configuration["namespace"]
    ):
        raise ValueError("Controller namespace identity is invalid")
    if "disk_size_floor" in configuration:
        from shared.vm_disk_size import quantity_bytes

        floor = configuration["disk_size_floor"]
        if not isinstance(floor, str) or not (quantity_bytes(floor) or 0) > 0:
            raise ValueError("Controller disk floor identity is invalid")
    for key, value in configuration.items():
        if key.endswith("_digest") and (
            not isinstance(value, str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", value)
        ):
            raise ValueError("Controller configuration identity is invalid")
    _validate_json(configuration)
    if "network_profile_policy" in configuration:
        from shared.vm_network_profile import NETWORK_PROFILE, compatible_image

        policy = configuration["network_profile_policy"]
        if (
            not isinstance(policy, dict)
            or set(policy) != {"version", "image", "profile"}
            or policy["version"] != 1
            or policy["profile"] != NETWORK_PROFILE
            or not compatible_image(policy["image"], allowlist=policy["image"])
        ):
            raise ValueError("Unsupported VM network profile policy")
    if configuration["version"] in (2, 3):
        from shared.vm_resource_configuration import validate_resource_configuration

        validate_resource_configuration(
            configuration["resource_admission"], configuration
        )
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                configuration,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode()
        ).hexdigest()
    )
