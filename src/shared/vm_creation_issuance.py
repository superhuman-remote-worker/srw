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


def _values(value):
    fields = (
        _FIELDS | {"rootdisk_source"}
        if isinstance(value, Mapping) and value.get("version") == 2
        else _FIELDS
    )
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError("Incomplete creation carrier intent")
    value = dict(value)
    if (
        type(value["version"]) is not int
        or value["version"] not in (1, 2)
        or value["source"] != CREATION_SOURCE
        or value["effect_kind"] not in EFFECT_KINDS
    ):
        raise ValueError("Unsupported creation carrier source")
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
    if value["effect_kind"] != "rootdisk" and value["current_pvc_uid"] is None:
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
    suffix = {"rootdisk": "-rootdisk", "cloud_init": "-cloudinit", "vm": ""}[
        value["effect_kind"]
    ]
    # Reusable workspace bindings have their own rootdisk name. Such names must
    # be checked against the frozen binding by the store before granting.
    if (
        value["effect_kind"] != "rootdisk"
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


def public_effect_observation(
    values, carrier, observation, *, rootdisk=None, cloud_init=None
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
        if (
            labels.get("srw.io/owner-kind") != "job"
            or labels.get("srw.io/owner-id") != values["job_id"]
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
            if values["version"] == 2 and not retained:
                source = values["rootdisk_source"]
                expected_source = (
                    {"pvc": {"namespace": source["namespace"], "name": source["name"]}}
                    if source["kind"] == "golden"
                    else {"registry": {"url": "docker://" + source["image"]}}
                )
                if obj.get("spec", {}).get("source") != expected_source:
                    raise ValueError("Observed rootdisk source changed")
                if (
                    source["kind"] == "golden"
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
        or set(configuration) != _CONFIGURATION_FIELDS
        or type(configuration["version"]) is not int
        or configuration["version"] != 1
    ):
        raise ValueError("Effective controller configuration is incomplete")
    if not isinstance(configuration["namespace"], str) or not re.fullmatch(
        r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?", configuration["namespace"]
    ):
        raise ValueError("Controller namespace identity is invalid")
    for key, value in configuration.items():
        if key.endswith("_digest") and (
            not isinstance(value, str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", value)
        ):
            raise ValueError("Controller configuration identity is invalid")
    _validate_json(configuration)
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
