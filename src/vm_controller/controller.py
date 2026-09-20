"""
VM Controller — KubeVirt VM Lifecycle Manager

Manages KubeVirt VirtualMachine resources on behalf of the orchestrator.
Two transports, selected by TRANSPORT env (nats|http|both):

  nats  — cross-cluster: subscribe to vm.lifecycle.{create,delete,get},
          publish results on vm.lifecycle.status. Default when TRANSPORT is
          unset (the parked external topology); the main chart's same-cluster
          deployment sets http explicitly.
  http  — same-cluster: serve POST /vms, DELETE /vms/{id}, GET /vms/{id}
          on LISTEN_PORT (default 8080). Returns the result synchronously
          so the orchestrator's HTTP client can update job context itself
          — no separate status channel needed for lifecycle events.
  both  — run both. Useful when migrating, or when the in-VM management
          daemon still uses NATS while the orchestrator dials HTTP.

SSH connectivity uses a Headscale mesh VPN (self-hosted Tailscale). The
controller generates short-lived auth keys via the Headscale API and injects
them into cloud-init so VMs join the tailnet on boot. Agent pods run a
Tailscale sidecar and route directly to VMs via 100.64.x.y addresses.

See knowledge-base/knowledge/features/headscale_mesh.md for the mesh VPN design.
See knowledge-base/knowledge/features/vm_backend.md (Phase 3) and knowledge-base/knowledge/features/nats.md.
"""

import asyncio
import base64
from collections import OrderedDict
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
import hashlib
import hmac
import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

import yaml
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from vm_controller.headscale_client import HeadscaleClient

from vm_controller.lifecycle_auth import (
    AUTH_FIELD,
    AUTH_VERSION,
    configured_secret,
    guest_token,
    sign_payload,
    unsigned_payload,
    verify_payload,
)

_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
if _log_level == "DEBUG" and not os.environ.get("DEBUG_ALL"):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logging.getLogger("vm-controller").setLevel(logging.DEBUG)
else:
    logging.basicConfig(
        level=_log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
log = logging.getLogger("vm-controller")

# Configuration from environment
NATS_URL = os.environ.get("NATS_URL", "nats://nats-leaf.nats.svc.cluster.local:4222")
# Per-orchestrator scope for vm.lifecycle.* subjects. Required when the
# controller and its orchestrator share a NATS hub with other SRW
# installations; without it the controller would receive every orchestrator's
# vm.lifecycle.create and provision duplicate VMs.
ORCHESTRATOR_ID = os.environ.get("ORCHESTRATOR_ID", "").strip()
VM_TEMPLATE_PATH = os.environ.get("VM_TEMPLATE_PATH", "/config/vm-template.yaml")
VM_CLOUD_INIT_PATH = os.environ.get("VM_CLOUD_INIT_PATH", "/config/cloud-init.yaml")
VM_NAMESPACE = os.environ.get("VM_NAMESPACE", "agent-vms")
ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "").strip()
VM_DEFAULT_NETWORK_TIER = os.environ.get("VM_DEFAULT_NETWORK_TIER", "").strip()
VM_MAX_CONCURRENT = max(0, int(os.environ.get("VM_MAX_CONCURRENT", "0")))
VM_NODE_SELECTOR = json.loads(os.environ.get("VM_NODE_SELECTOR", "{}") or "{}")
VM_TOLERATIONS = json.loads(os.environ.get("VM_TOLERATIONS", "[]") or "[]")
if not isinstance(VM_NODE_SELECTOR, dict) or not all(
    isinstance(key, str) and isinstance(value, str)
    for key, value in VM_NODE_SELECTOR.items()
):
    raise ValueError("VM_NODE_SELECTOR must be a JSON object with string values")
if not isinstance(VM_TOLERATIONS, list) or not all(
    isinstance(item, dict) for item in VM_TOLERATIONS
):
    raise ValueError("VM_TOLERATIONS must be a JSON array of objects")
DEFAULT_VM_IMAGE = os.environ.get(
    "DEFAULT_VM_IMAGE",
    "ghcr.io/superhuman-remote-worker/srw-agent-vm-base:latest",
)
DEFAULT_CPU = int(os.environ.get("DEFAULT_CPU", "2"))
DEFAULT_MEMORY = os.environ.get("DEFAULT_MEMORY", "4Gi")
VM_STORAGE_CLASS = os.environ.get("VM_STORAGE_CLASS", "local-path")
VM_DISK_SIZE = os.environ.get("VM_DISK_SIZE", "20Gi")

# Golden-image boot acceleration
# (knowledge-base/knowledge/features/vm_golden_image_boot_acceleration.md). When enabled, the base
# image is imported ONCE into a standalone "golden" DataVolume/PVC per image
# digest, and each VM's root disk is a CDI clone of it (host-assisted local copy
# on local-path) instead of a per-VM registry import. Off by default →
# byte-for-byte the legacy registry-per-VM behaviour.
VM_GOLDEN_IMAGE_ENABLED = os.environ.get(
    "VM_GOLDEN_IMAGE_ENABLED", "false"
).strip().lower() in ("1", "true", "yes")
# Golden PVC size; falls back to VM_DISK_SIZE so the clone target (also
# VM_DISK_SIZE) is never smaller than its source.
VM_GOLDEN_DISK_SIZE = os.environ.get("VM_GOLDEN_DISK_SIZE", "").strip() or VM_DISK_SIZE

_K8S_QUANTITY_RE = re.compile(r"^(\d+)(Ki|Mi|Gi|Ti|K|M|G|T)?$")
_QUANTITY_MULT = {
    None: 1,
    "K": 10**3,
    "M": 10**6,
    "G": 10**9,
    "T": 10**12,
    "Ki": 2**10,
    "Mi": 2**20,
    "Gi": 2**30,
    "Ti": 2**40,
}


def _quantity_bytes(value: object) -> int | None:
    """Parse a Kubernetes storage quantity (``120Gi``) into bytes; None if malformed."""
    m = _K8S_QUANTITY_RE.match(str(value).strip()) if value is not None else None
    if not m:
        return None
    return int(m.group(1)) * _QUANTITY_MULT[m.group(2)]


def effective_disk_size(job_config: Mapping[str, object]) -> str:
    """Per-job rootdisk size: ``job_config["disk_size"]`` when it is a valid
    quantity **not smaller than** ``VM_DISK_SIZE``; otherwise the controller
    default. Never shrink: the golden clone target must not be smaller than
    its source, and the default is that floor by construction (see
    ``VM_GOLDEN_DISK_SIZE`` above).
    """
    requested = job_config.get("disk_size")
    if requested in (None, ""):
        return VM_DISK_SIZE
    req_bytes = _quantity_bytes(requested)
    default_bytes = _quantity_bytes(VM_DISK_SIZE)
    if req_bytes is None:
        log.warning(
            "disk_size %r is not a k8s quantity; using %s", requested, VM_DISK_SIZE
        )
        return VM_DISK_SIZE
    if default_bytes is not None and req_bytes < default_bytes:
        log.warning(
            "disk_size %s is below the controller default %s; using the default",
            requested,
            VM_DISK_SIZE,
        )
        return VM_DISK_SIZE
    return str(requested).strip()


# Bounded wait for a golden import/clone to reach Succeeded (mirrors the agent's
# VM_UPGRADE_POLL_TIMEOUT=900 cold-import budget).
VM_GOLDEN_POLL_TIMEOUT = int(os.environ.get("VM_GOLDEN_POLL_TIMEOUT", "900"))
VM_GOLDEN_GC_ENABLED = os.environ.get(
    "VM_GOLDEN_GC_ENABLED", "true"
).strip().lower() in ("1", "true", "yes")
# Keep the N newest golden digests; GC older ones (mirrors CDI's importsToKeep).
VM_GOLDEN_KEEP = int(os.environ.get("VM_GOLDEN_KEEP", "3"))
VM_GOLDEN_GC_MIN_AGE_MINUTES = int(os.environ.get("VM_GOLDEN_GC_MIN_AGE_MINUTES", "30"))

# Persistent rootdisks (knowledge-base/knowledge/features/vm_persistent_rootdisk.md). When enabled,
# the VM's root disk is created as a STANDALONE DataVolume — same deterministic
# name the template already renders — instead of via spec.dataVolumeTemplates.
# Without an ownerRef it is not cascade-deleted with the VM, so a recreate
# reattaches it by name: files intact, and the clone skipped entirely. Off by
# default → byte-for-byte the legacy templated-disk behaviour.
VM_PERSISTENT_ROOTDISK = os.environ.get(
    "VM_PERSISTENT_ROOTDISK", "false"
).strip().lower() in ("1", "true", "yes")
# Orphan backstop for rootdisks whose entity the orchestrator no longer knows
# (a dev DB reset, a deleted row) — the orchestrator's own kept-disk sweep
# cannot see those. OFF by default on purpose: the controller has no DB, so it
# cannot tell a leaked disk from the workspace of a session that has been
# suspended for a long weekend. Enable it only where sessions are short-lived
# or capacity is tight.
VM_ROOTDISK_GC_ENABLED = os.environ.get(
    "VM_ROOTDISK_GC_ENABLED", "false"
).strip().lower() in ("1", "true", "yes")
# Generous: a kept disk is *supposed* to outlive its VM while a recovery is in
# flight. No VM for this long means nobody is coming back for it.
VM_ROOTDISK_ORPHAN_HOURS = int(os.environ.get("VM_ROOTDISK_ORPHAN_HOURS", "72"))
# CDI creates the PVC asynchronously after a DataVolume/VM is admitted.  The
# immutable PVC UID is the storage-metering ownership credential, so give the
# controller a short bounded window to observe it before returning the create
# result.  Failure is non-fatal for VM provisioning, but metering deliberately
# leaves that rootdisk unattributed until a later create can attest the UID.
VM_ROOTDISK_PVC_UID_ATTEMPTS = int(os.environ.get("VM_ROOTDISK_PVC_UID_ATTEMPTS", "20"))
VM_ROOTDISK_PVC_UID_RETRY_SECONDS = float(
    os.environ.get("VM_ROOTDISK_PVC_UID_RETRY_SECONDS", "0.25")
)
LIFECYCLE_LOCK_STRIPES = 256

# Transport selection: nats | http | both. Defaults to nats (the parked
# external topology); the main chart sets http for same-cluster.
TRANSPORT = os.environ.get("TRANSPORT", "nats").lower()
LISTEN_HOST = os.environ.get("LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8080"))
LIFECYCLE_HMAC_SECRET = configured_secret()
LIFECYCLE_REPLAY_CACHE_SIZE = int(
    os.environ.get("VM_LIFECYCLE_REPLAY_CACHE_SIZE", "10000")
)
LIFECYCLE_NONCE_TTL_SECONDS = max(
    120, int(os.environ.get("VM_LIFECYCLE_NONCE_TTL_SECONDS", "300"))
)
LIFECYCLE_NONCE_GC_INTERVAL = max(
    1, int(os.environ.get("VM_LIFECYCLE_NONCE_GC_INTERVAL", "100"))
)
LIFECYCLE_NONCE_GC_PAGE_LIMIT = min(
    500, max(1, int(os.environ.get("VM_LIFECYCLE_NONCE_GC_PAGE_LIMIT", "100")))
)
LIFECYCLE_NONCE_GC_DELETE_LIMIT = min(
    LIFECYCLE_NONCE_GC_PAGE_LIMIT,
    max(1, int(os.environ.get("VM_LIFECYCLE_NONCE_GC_DELETE_LIMIT", "100"))),
)
_LIFECYCLE_NONCE_LABEL = "srw.io/vm-lifecycle-nonce"

# KubeVirt API coordinates
KUBEVIRT_GROUP = "kubevirt.io"
KUBEVIRT_VERSION = "v1"
KUBEVIRT_PLURAL = "virtualmachines"
KUBEVIRT_VMI_PLURAL = "virtualmachineinstances"

# CDI (Containerized Data Importer) API coordinates — golden DataVolumes
CDI_GROUP = "cdi.kubevirt.io"
CDI_VERSION = "v1beta1"
CDI_PLURAL = "datavolumes"

WORKSPACE_RECOVERY_PIN_LABEL = "srw.io/vm-workspace-recovery-pin"
WORKSPACE_RECOVERY_ID_LABEL = "srw.io/recovery-id"
WORKSPACE_RECOVERY_PVC_LABEL = "srw.io/recovery-pvc-uid"
WORKSPACE_RECOVERY_GENERATION_LABEL = "srw.io/recovery-generation"
WORKSPACE_CLEANUP_CARRIER_LABEL = "srw.io/vm-workspace-cleanup-carrier"
_CLEANUP_ANNOTATIONS = {
    "admission_id": "srw.io/cleanup-admission-id",
    "request_id": "srw.io/cleanup-request-id",
    "intent_digest": "srw.io/cleanup-intent-digest",
    "owner_kind": "srw.io/cleanup-owner-kind",
    "owner_id": "srw.io/cleanup-owner-id",
    "source": "srw.io/cleanup-source",
    "outcome": "srw.io/cleanup-outcome",
    "name": "srw.io/cleanup-rootdisk-name",
    "old_dv_uid": "srw.io/cleanup-old-dv-uid",
    "old_pvc_uid": "srw.io/cleanup-old-pvc-uid",
    "provision_generation": "srw.io/cleanup-generation",
    "nonce": "srw.io/cleanup-nonce",
    "successor_dv_uid": "srw.io/cleanup-successor-dv-uid",
    "successor_pvc_uid": "srw.io/cleanup-successor-pvc-uid",
}
_CLEANUP_OUTCOMES = {
    "controller_creation_rootdisk_delete": "deleted",
    "controller_rootdisk_delete": "deleted",
    "controller_failed_dv_recreate": "recreated",
    "controller_rootdisk_adopt": "adopted",
}
WORKSPACE_RECOVERY_CONTROLLER_IDENTITY = os.environ.get(
    "POD_UID", os.environ.get("HOSTNAME", "vm-controller/unknown")
)


@dataclass(frozen=True, slots=True)
class WorkspaceRecoveryObservation:
    vm_uid: str
    vmi_uid: str | None
    launcher_uids: tuple[str, ...]
    node_uid: str | None
    root_pvc_uid: str
    migration_ambiguous: bool
    stop_evidence: Literal["proven", "unknown"]
    observed_at: datetime


# The job description is free text — typed by users, generated by the loop
# engine and automations — and lands in a JSON blob nested inside the VM
# template's cloud-init `userData: |` block scalar. Raw substitution there is
# unsafe twice over: a newline puts the continuation at column 1, dedenting out
# of the block scalar and destroying the manifest (job 4435994d), while a quote
# or backslash leaves valid YAML wrapping a corrupt job-config.json that
# management-daemon.py discards with only a log.warning.
#
# Capped because the whole userData block has a hard 2048-byte KubeVirt limit
# (inline cloudInitNoCloud) with only ~350 bytes of headroom. Nothing in the VM
# reads this field — management-daemon.py is job-config.json's only consumer and
# never looks at `description` — so truncating it costs nothing.
MAX_DESCRIPTION_LEN = 200

_OWNER_KINDS = frozenset({"job", "thread"})
_PROVISION_GENERATION_ANNOTATION = "srw.io/provision-generation"
_SSH_HOST_KEY_FINGERPRINT_ANNOTATION = "srw.io/ssh-host-key-fingerprint"
_NETWORK_TIER_PATTERN = re.compile(r"^[a-z0-9-]{1,63}$")


@dataclass(frozen=True, repr=False)
class _SSHHostKeyMaterial:
    """One ephemeral render-time host identity; never log or serialize it."""

    private_key: str
    public_key: str
    fingerprint: str


def _openssh_sha256_fingerprint(public_key: str) -> str:
    """Return the OpenSSH SHA256 fingerprint for an ed25519 public key."""

    fields = public_key.strip().split()
    if len(fields) < 2 or fields[0] != "ssh-ed25519":
        raise ValueError("SSH host public key must be OpenSSH ed25519")
    try:
        key_bytes = base64.b64decode(fields[1].encode("ascii"), validate=True)
    except (UnicodeEncodeError, ValueError) as exc:
        raise ValueError("SSH host public key has invalid base64") from exc
    digest = hashlib.sha256(key_bytes).digest()
    return "SHA256:" + base64.b64encode(digest).decode("ascii").rstrip("=")


def _ssh_host_key_fingerprint(value: object) -> str | None:
    """Accept only the canonical OpenSSH SHA256 fingerprint shape."""

    if not isinstance(value, str) or not value.startswith("SHA256:"):
        return None
    encoded = value.removeprefix("SHA256:")
    if len(encoded) != 43:
        return None
    try:
        digest = base64.b64decode((encoded + "=").encode("ascii"), validate=True)
    except (UnicodeEncodeError, ValueError):
        return None
    return value if len(digest) == hashlib.sha256().digest_size else None


def _generate_ssh_host_key() -> _SSHHostKeyMaterial:
    """Generate the controller-owned ed25519 identity for one VM provision."""

    private_key = Ed25519PrivateKey.generate()
    private_text = private_key.private_bytes(
        Encoding.PEM,
        PrivateFormat.OpenSSH,
        NoEncryption(),
    ).decode("ascii")
    public_text = (
        private_key.public_key()
        .public_bytes(
            Encoding.OpenSSH,
            PublicFormat.OpenSSH,
        )
        .decode("ascii")
    )
    return _SSHHostKeyMaterial(
        private_key=private_text,
        public_key=public_text,
        fingerprint=_openssh_sha256_fingerprint(public_text),
    )


def _inject_ssh_host_key(user_data: str) -> tuple[str, str]:
    """Inject one generated host identity into a cloud-config document."""

    cloud_config = yaml.safe_load(user_data)
    if not isinstance(cloud_config, dict):
        raise ValueError("Secret-backed cloud-init must be a cloud-config mapping")
    key = _generate_ssh_host_key()
    # Prevent cloud-init from adding a second, unpinned host identity. The
    # supplied pair is written by cc_ssh before sshd is restarted by runcmd.
    cloud_config["ssh_deletekeys"] = True
    cloud_config["ssh_genkeytypes"] = []
    cloud_config["ssh_keys"] = {
        "ed25519_private": key.private_key,
        "ed25519_public": key.public_key,
    }
    rendered = yaml.safe_dump(cloud_config, sort_keys=False)
    return f"#cloud-config\n{rendered}", key.fingerprint


def _owner_identity(job_config: dict) -> tuple[str, str]:
    """Return the validated, full application owner identity for one VM."""

    owner_kind = job_config.get("entity_type", "job")
    owner_id = job_config.get("job_id")
    if owner_kind not in _OWNER_KINDS:
        raise ValueError("entity_type must be 'job' or 'thread'")
    if (
        not isinstance(owner_id, str)
        or not owner_id
        or owner_id != owner_id.strip()
        or len(owner_id) > 63
        or any(character.isspace() for character in owner_id)
    ):
        raise ValueError("job_id is not a valid Kubernetes owner label")
    return owner_kind, owner_id


def _stamp_owner_identity(manifest: dict, owner_kind: str, owner_id: str) -> None:
    """Stamp VM, VMI-template, and DataVolume/PVC-propagated owner labels."""

    def stamp(metadata: dict) -> None:
        labels = metadata.setdefault("labels", {})
        labels["srw.io/owner-kind"] = owner_kind
        labels["srw.io/owner-id"] = owner_id

    metadata = manifest.setdefault("metadata", {})
    stamp(metadata)
    spec = manifest.setdefault("spec", {})
    template = spec.setdefault("template", {})
    stamp(template.setdefault("metadata", {}))
    data_volume_templates = spec.get("dataVolumeTemplates", [])
    if isinstance(data_volume_templates, list):
        for data_volume_template in data_volume_templates:
            if isinstance(data_volume_template, dict):
                stamp(data_volume_template.setdefault("metadata", {}))


def _provision_generation(value: object) -> str | None:
    """Return only the canonical opaque generation format we mint."""

    if not isinstance(value, str) or len(value) != 36:
        return None
    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError):
        return None
    return value if str(parsed) == value else None


def _stamp_provision_generation(manifest: dict, generation: str) -> None:
    """Bind a VM and its VMI template to one durable provision attempt."""

    metadata = manifest.setdefault("metadata", {})
    metadata.setdefault("annotations", {})[_PROVISION_GENERATION_ANNOTATION] = (
        generation
    )
    template_metadata = (
        manifest.setdefault("spec", {})
        .setdefault("template", {})
        .setdefault("metadata", {})
    )
    template_metadata.setdefault("annotations", {})[
        _PROVISION_GENERATION_ANNOTATION
    ] = generation


def _admitted_provision_generation(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    metadata = value.get("metadata")
    if not isinstance(metadata, Mapping):
        return None
    annotations = metadata.get("annotations")
    if not isinstance(annotations, Mapping):
        return None
    return _provision_generation(annotations.get(_PROVISION_GENERATION_ANNOTATION))


def _authenticated_http_payload(
    request, payload: Mapping[str, object], *, operation: str
) -> dict:
    """Attach the fixed HTTP query signature to a reconstructed payload."""

    result = dict(payload)
    signature_value = request.query.get("lifecycle_auth")
    if signature_value is not None:
        try:
            issued_at = int(request.query.get("lifecycle_auth_issued_at", ""))
        except (TypeError, ValueError):
            issued_at = None
        result[AUTH_FIELD] = {
            "version": AUTH_VERSION,
            "direction": "request",
            "operation": operation,
            "issued_at": issued_at,
            "request_id": request.query.get("lifecycle_auth_request_id"),
            "signature": signature_value,
        }
    return result


def _lifecycle_request_id(payload: Mapping[str, object]) -> str | None:
    auth = payload.get(AUTH_FIELD)
    if not isinstance(auth, Mapping):
        return None
    value = auth.get("request_id")
    return value if isinstance(value, str) else None


def _admitted_vm_uid(value: object, *, expected_name: str) -> str | None:
    """Extract one admitted VM UID without trusting a loose response shape."""

    if not isinstance(value, Mapping):
        return None
    metadata = value.get("metadata")
    if not isinstance(metadata, Mapping) or metadata.get("name") != expected_name:
        return None
    uid = metadata.get("uid")
    if (
        not isinstance(uid, str)
        or not uid
        or uid != uid.strip()
        or len(uid) > 256
        or any(character.isspace() for character in uid)
    ):
        return None
    return uid


def _safe_uid(value: object) -> str | None:
    """Validate one opaque Kubernetes UID without interpreting its format."""

    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 256
        or any(character.isspace() for character in value)
    ):
        return None
    return value


def _object_value(value: object, key: str, default=None):
    if isinstance(value, Mapping):
        return value.get(key, default)
    return getattr(value, key, default)


def _metadata(value: object) -> object:
    return _object_value(value, "metadata", {})


def _metadata_value(value: object, key: str, default=None):
    return _object_value(_metadata(value), key, default)


def _owner_references(value: object) -> list[object]:
    references = _metadata_value(value, "ownerReferences")
    if references is None:
        references = _metadata_value(value, "owner_references", [])
    return list(references or [])


def _owned_by(value: object, *, kind: str, uid: str) -> bool:
    return any(
        _object_value(reference, "kind") == kind
        and _object_value(reference, "uid") == uid
        and _object_value(reference, "controller") is True
        for reference in _owner_references(value)
    )


def _storage_volume_name(value: object, expected_name: str) -> str | None:
    """Return one volume name bound to the exact DV/PVC name."""

    volumes = _object_value(value, "volumes", [])
    if not isinstance(volumes, list):
        return None
    matches: list[str] = []
    for volume in volumes:
        data_volume = _object_value(volume, "dataVolume")
        if data_volume is None:
            data_volume = _object_value(volume, "data_volume")
        claim = _object_value(volume, "persistentVolumeClaim")
        if claim is None:
            claim = _object_value(volume, "persistent_volume_claim")
        source_name = (
            _object_value(data_volume, "name")
            if data_volume is not None
            else _object_value(claim, "claimName") or _object_value(claim, "claim_name")
        )
        if source_name == expected_name:
            name = _object_value(volume, "name")
            if not isinstance(name, str) or not name:
                return None
            matches.append(name)
    return matches[0] if len(matches) == 1 else None


def _container_mounts_volume(pod_spec: object, volume_name: str) -> bool:
    containers = _container_entries(pod_spec, "containers", "containers")
    if containers is None:
        return False
    compute = [item for item in containers if _object_value(item, "name") == "compute"]
    if len(compute) != 1:
        return False
    mounts = _object_value(compute[0], "volumeMounts")
    if mounts is None:
        mounts = _object_value(compute[0], "volume_mounts", [])
    return isinstance(mounts, list) and any(
        _object_value(mount, "name") == volume_name for mount in mounts
    )


def _container_entries(value: object, camel: str, snake: str) -> list[object] | None:
    entries = _object_value(value, camel)
    if entries is None:
        entries = _object_value(value, snake, [])
    return list(entries) if isinstance(entries, (list, tuple)) else None


def _empty_container_state(value: object) -> bool:
    """Recognize only an empty Kubernetes ContainerState representation."""

    fields = {"running", "waiting", "terminated"}
    if value is None:
        return True
    if isinstance(value, Mapping):
        return not (set(value) - fields) and all(
            value.get(field) is None for field in fields
        )
    attribute_map = getattr(value, "attribute_map", None)
    if not isinstance(attribute_map, Mapping) or set(attribute_map) != fields:
        return False
    if any(attribute_map.get(field) != field for field in fields):
        return False
    return all(getattr(value, field, None) is None for field in fields)


def _exact_terminal_container_evidence(pod: object) -> dict | None:
    """Return current kubelet termination evidence, never ``lastState``."""

    pod_status = _object_value(pod, "status", {})
    if _object_value(pod_status, "reason") in {
        "NodeLost",
        "ContainerStatusUnknown",
    }:
        return None
    if _object_value(pod_status, "phase") not in {"Succeeded", "Failed"}:
        return None
    grace = _metadata_value(pod, "deletionGracePeriodSeconds")
    if grace is None:
        grace = _metadata_value(pod, "deletion_grace_period_seconds")
    if grace == 0:
        return None
    spec = _object_value(pod, "spec", {})
    restart_policy = _object_value(spec, "restartPolicy")
    if restart_policy is None:
        restart_policy = _object_value(spec, "restart_policy")
    if restart_policy != "Never":
        return None
    declared: dict[str, list[str]] = {}
    status_groups: dict[str, list[object]] = {}
    for kind, spec_keys, status_keys in (
        (
            "init",
            ("initContainers", "init_containers"),
            ("initContainerStatuses", "init_container_statuses"),
        ),
        (
            "regular",
            ("containers", "containers"),
            ("containerStatuses", "container_statuses"),
        ),
    ):
        definitions = _container_entries(spec, *spec_keys)
        statuses = _container_entries(pod_status, *status_keys)
        if definitions is None or statuses is None:
            return None
        names = [_object_value(entry, "name") for entry in definitions]
        if any(not isinstance(name, str) or not name for name in names):
            return None
        if len(set(names)) != len(names):
            return None
        status_names = [_object_value(entry, "name") for entry in statuses]
        if (
            any(not isinstance(name, str) or not name for name in status_names)
            or len(set(status_names)) != len(status_names)
            or set(status_names) != set(names)
        ):
            return None
        declared[kind] = names
        status_groups[kind] = statuses
    if "compute" not in declared["regular"] or not declared["regular"]:
        return None
    evidence: list[dict] = []
    for kind in ("init", "regular"):
        for status in status_groups[kind]:
            container_id = _object_value(status, "containerID")
            if container_id is None:
                container_id = _object_value(status, "container_id")
            restart_count = _object_value(status, "restartCount")
            if restart_count is None:
                restart_count = _object_value(status, "restart_count")
            state = _object_value(status, "state", {})
            last_state = _object_value(status, "lastState")
            if last_state is None:
                last_state = _object_value(status, "last_state")
            if not _empty_container_state(last_state):
                return None
            terminated = _object_value(state, "terminated")
            if terminated is None:
                return None
            terminated_container_id = _object_value(terminated, "containerID")
            if terminated_container_id is None:
                terminated_container_id = _object_value(terminated, "container_id")
            finished_at = _object_value(terminated, "finishedAt")
            if finished_at is None:
                finished_at = _object_value(terminated, "finished_at")
            reason = _object_value(terminated, "reason")
            if (
                _safe_uid(container_id) is None
                or _safe_uid(terminated_container_id) is None
                or terminated_container_id != container_id
                or type(restart_count) is not int
                or restart_count != 0
                or finished_at is None
                or not isinstance(reason, str)
                or not reason
                or reason == "ContainerStatusUnknown"
            ):
                return None
            evidence.append(
                {
                    "name": str(_object_value(status, "name") or ""),
                    "kind": kind,
                    "container_id": container_id,
                    "terminated_container_id": terminated_container_id,
                    "restart_count": restart_count,
                    "state": "terminated",
                    "last_state": None,
                    "finished_at": (
                        finished_at.isoformat()
                        if isinstance(finished_at, datetime)
                        else str(finished_at)
                    ),
                    "reason": reason,
                }
            )
    return {
        "containers": evidence,
        "declared_containers": declared,
        "pod_terminal": {
            "phase": _object_value(pod_status, "phase"),
            "restart_policy": restart_policy,
        },
    }


def _admitted_pvc_uid(
    value: object,
    *,
    expected_name: str,
    expected_owner_id: str,
    expected_owner_kind: str | None,
) -> str | None:
    """Extract a PVC UID only from the exact controller-owned root claim."""

    if isinstance(value, Mapping):
        metadata = value.get("metadata")
    else:
        metadata = getattr(value, "metadata", None)
    if isinstance(metadata, Mapping):
        name = metadata.get("name")
        uid = metadata.get("uid")
        labels = metadata.get("labels")
    else:
        name = getattr(metadata, "name", None)
        uid = getattr(metadata, "uid", None)
        labels = getattr(metadata, "labels", None)
    if name != expected_name or not isinstance(labels, Mapping):
        return None
    if labels.get("srw.io/owner-id") != expected_owner_id:
        return None
    owner_kind = labels.get("srw.io/owner-kind")
    if owner_kind not in _OWNER_KINDS or (
        expected_owner_kind is not None and owner_kind != expected_owner_kind
    ):
        return None
    if (
        not isinstance(uid, str)
        or not uid
        or uid != uid.strip()
        or len(uid) > 256
        or any(character.isspace() for character in uid)
    ):
        return None
    return uid


class VMController:
    """Manages KubeVirt VM lifecycle via NATS commands."""

    def __init__(self):
        self.nc = None  # NATS client (when transport includes nats)
        self.http_runner = None  # aiohttp AppRunner (when transport includes http)
        self.k8s_client = None  # kubernetes CustomObjectsApi
        self.core_api = None  # kubernetes CoreV1Api (read-only PVC identity)
        self.coordination_api = None  # durable lifecycle replay claims
        self.template_text: str = ""  # Raw YAML template (for string substitution)
        self.cloud_init_text: str = ""  # Optional Secret-backed cloud-init payload
        self.headscale = HeadscaleClient()
        self._shutdown = asyncio.Event()
        self._seen_lifecycle_requests: OrderedDict[str, float] = OrderedDict()
        self._lifecycle_nonce_claim_count = 0
        self._lifecycle_nonce_gc_continue: str | None = None
        # The chart runs one controller replica. A fixed-size striped lock set
        # serializes create/delete for one reusable VM/rootdisk name without an
        # unbounded per-job lock registry. This closes the absent-VM -> rootdisk
        # purge race against a concurrent re-create in the same controller.
        self._lifecycle_locks = tuple(
            asyncio.Lock() for _ in range(LIFECYCLE_LOCK_STRIPES)
        )
        # Admission capacity is process-wide, not per entity. Holding this
        # lock from the live-VM count through VirtualMachine admission makes
        # VM_MAX_CONCURRENT a hard cap for concurrent creates.
        self._capacity_lock = asyncio.Lock()

    def _lifecycle_lock_for(self, entity_id: str) -> asyncio.Lock:
        locks = getattr(self, "_lifecycle_locks", None)
        if not locks:
            # A few unit-test fixtures intentionally construct via __new__.
            locks = tuple(asyncio.Lock() for _ in range(LIFECYCLE_LOCK_STRIPES))
            self._lifecycle_locks = locks
        digest = hashlib.sha256(entity_id.encode("utf-8")).digest()
        return locks[int.from_bytes(digest[:8], "big") % len(locks)]

    @asynccontextmanager
    async def _workspace_lifecycle(self, owner_id: str):
        """Serialize one workspace boundary, allowing same-task nesting."""

        lock = self._lifecycle_lock_for(str(owner_id))
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("workspace lifecycle requires an asyncio task")
        owners = getattr(self, "_lifecycle_lock_owners", None)
        if owners is None:
            owners = {}
            self._lifecycle_lock_owners = owners
        key = id(lock)
        if owners.get(key) is task:
            yield
            return
        await lock.acquire()
        owners[key] = task
        try:
            yield
        finally:
            if owners.get(key) is task:
                owners.pop(key, None)
            lock.release()

    @staticmethod
    def _recovery_pin_name(recovery_id: str) -> str:
        parsed = UUID(str(recovery_id))
        return f"srw-recovery-{parsed}"

    async def _workspace_cleanup_authority_request(
        self, path: str, payload: Mapping[str, object], *, operation: str
    ) -> dict[str, object]:
        """Call the database-backed cleanup authority over the lifecycle MAC."""

        import httpx

        if LIFECYCLE_HMAC_SECRET is None or not ORCHESTRATOR_URL:
            raise RuntimeError("workspace cleanup authority is unavailable")
        signed = sign_payload(
            payload,
            direction="request",
            operation=operation,
            secret=LIFECYCLE_HMAC_SECRET,
        )
        auth = signed.get(AUTH_FIELD)
        request_id = auth.get("request_id") if isinstance(auth, Mapping) else None
        if not isinstance(request_id, str):
            raise RuntimeError("workspace cleanup authority request is malformed")
        async with httpx.AsyncClient(base_url=ORCHESTRATOR_URL, timeout=10.0) as client:
            response = await client.post(path, json=signed)
        try:
            value = response.json()
        except ValueError as exc:
            raise RuntimeError(
                "workspace cleanup authority response is malformed"
            ) from exc
        if not isinstance(value, Mapping) or not verify_payload(
            value,
            direction="response",
            operation=operation,
            secret=LIFECYCLE_HMAC_SECRET,
            expected_correlation_id=request_id,
        ):
            raise RuntimeError(
                "workspace cleanup authority response is unauthenticated"
            )
        response.raise_for_status()
        return dict(unsigned_payload(value))

    @staticmethod
    def _workspace_cleanup_carrier_name(admission_id: str) -> str:
        return f"srw-cleanup-{UUID(str(admission_id)).hex}"

    @staticmethod
    def _workspace_cleanup_carrier_signature(
        *, name: str, uid: str, values: Mapping[str, object]
    ) -> str:
        """Authenticate durable intent without the transport's message expiry."""
        if LIFECYCLE_HMAC_SECRET is None:
            raise RuntimeError("workspace cleanup carrier authentication unavailable")
        payload = {
            "domain": "srw-workspace-cleanup-carrier-v1",
            "namespace": VM_NAMESPACE,
            "name": name,
            "uid": uid,
            "values": {key: str(values.get(key) or "") for key in _CLEANUP_ANNOTATIONS},
        }
        return hmac.new(
            LIFECYCLE_HMAC_SECRET,
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(),
            hashlib.sha256,
        ).hexdigest()

    def _parse_workspace_cleanup_carrier(self, lease: object) -> dict[str, object]:
        from shared.vm_creation_issuance import CREATION_INTENT_ANNOTATION

        if CREATION_INTENT_ANNOTATION in (
            _metadata_value(lease, "annotations", {}) or {}
        ):
            from vm_controller.creation_actuation import carrier_record

            return carrier_record(lease, secret=LIFECYCLE_HMAC_SECRET)
        metadata = _object_value(lease, "metadata")
        labels = _metadata_value(lease, "labels", {}) or {}
        annotations = _metadata_value(lease, "annotations", {}) or {}
        name = _metadata_value(lease, "name")
        uid = _metadata_value(lease, "uid")
        resource_version = _metadata_value(lease, "resourceVersion") or _metadata_value(
            lease, "resource_version"
        )
        if (
            metadata is None
            or not isinstance(labels, Mapping)
            or labels.get(WORKSPACE_CLEANUP_CARRIER_LABEL) != "true"
            or not isinstance(annotations, Mapping)
            or not isinstance(name, str)
            or not isinstance(uid, str)
            or not uid
            or not isinstance(resource_version, str)
            or not resource_version
        ):
            raise RuntimeError("workspace cleanup carrier metadata is incomplete")
        carrier = {
            key: str(annotations.get(annotation) or "")
            for key, annotation in _CLEANUP_ANNOTATIONS.items()
        }
        signature = annotations.get("srw.io/cleanup-carrier-signature")
        sealed = isinstance(signature, str) and bool(signature)
        if not sealed:
            # Initial creation is signed before Kubernetes assigns a UID. It
            # cannot carry a successor; refresh seals the observed Lease UID
            # before database revalidation or any disk effect.
            signature = annotations.get("srw.io/cleanup-creation-signature")
        if (
            _metadata_value(lease, "namespace") != VM_NAMESPACE
            or not isinstance(signature, str)
            or (
                not sealed
                and (carrier["successor_dv_uid"] or carrier["successor_pvc_uid"])
            )
            or not hmac.compare_digest(
                signature,
                self._workspace_cleanup_carrier_signature(
                    name=name, uid=uid if sealed else "", values=carrier
                ),
            )
        ):
            raise RuntimeError("workspace cleanup carrier authentication failed")
        required = (
            "admission_id",
            "request_id",
            "intent_digest",
            "owner_kind",
            "owner_id",
            "source",
            "outcome",
            "name",
            "old_dv_uid",
            "old_pvc_uid",
            "provision_generation",
            "nonce",
        )
        if (
            not all(carrier[key] for key in required)
            or carrier["owner_kind"] not in _OWNER_KINDS
            or _CLEANUP_OUTCOMES.get(carrier["source"]) != carrier["outcome"]
            or not carrier["intent_digest"].startswith("sha256:")
            or name != self._workspace_cleanup_carrier_name(carrier["admission_id"])
        ):
            raise RuntimeError("workspace cleanup carrier identity is malformed")
        try:
            UUID(carrier["admission_id"])
            UUID(carrier["request_id"])
            UUID(carrier["owner_id"])
            UUID(carrier["nonce"])
        except (TypeError, ValueError, AttributeError) as exc:
            raise RuntimeError(
                "workspace cleanup carrier identity is malformed"
            ) from exc
        if bool(carrier["successor_dv_uid"]) != bool(carrier["successor_pvc_uid"]):
            raise RuntimeError("workspace cleanup successor binding is incomplete")
        carrier.update(
            {
                "carrier_name": name,
                "carrier_uid": uid,
                "carrier_resource_version": resource_version,
                "carrier_sealed": sealed,
            }
        )
        return carrier

    async def _ensure_workspace_cleanup_carrier(
        self,
        reservation: Mapping[str, object],
        *,
        source: str,
        owner_kind: str,
        owner_id: str,
        pvc_uid: str,
        dv_uid: str,
        provision_generation: str,
    ) -> dict[str, object]:
        """Publish the durable Kubernetes half before returning DB authority."""

        from kubernetes.client.exceptions import ApiException

        admission_id = str(reservation.get("admission_id") or "")
        request_id = str(reservation.get("request_id") or "")
        intent_digest = str(reservation.get("intent_digest") or "")
        if (
            not admission_id
            or not request_id
            or not intent_digest.startswith("sha256:")
            or source not in _CLEANUP_OUTCOMES
        ):
            raise RuntimeError("workspace cleanup reservation identity is incomplete")
        carrier_name = self._workspace_cleanup_carrier_name(admission_id)
        nonce = str(
            uuid5(NAMESPACE_URL, f"srw-cleanup-carrier:{admission_id}:{request_id}")
        )
        values = {
            "admission_id": admission_id,
            "request_id": request_id,
            "intent_digest": intent_digest,
            "owner_kind": owner_kind,
            "owner_id": owner_id,
            "source": source,
            "outcome": _CLEANUP_OUTCOMES[source],
            "name": _rootdisk_name(owner_id),
            "old_dv_uid": dv_uid,
            "old_pvc_uid": pvc_uid,
            "provision_generation": provision_generation,
            "nonce": nonce,
        }
        body = {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": carrier_name,
                "namespace": VM_NAMESPACE,
                "labels": {WORKSPACE_CLEANUP_CARRIER_LABEL: "true"},
                "annotations": {
                    _CLEANUP_ANNOTATIONS[key]: value for key, value in values.items()
                },
            },
            "spec": {"holderIdentity": admission_id},
        }
        body["metadata"]["annotations"]["srw.io/cleanup-creation-signature"] = (
            self._workspace_cleanup_carrier_signature(
                name=carrier_name, uid="", values=values
            )
        )
        try:
            lease = await asyncio.to_thread(
                self.coordination_api.create_namespaced_lease,
                namespace=VM_NAMESPACE,
                body=body,
            )
        except ApiException as exc:
            if exc.status != 409:
                raise RuntimeError("workspace cleanup carrier is unavailable") from exc
            lease = await asyncio.to_thread(
                self.coordination_api.read_namespaced_lease,
                name=carrier_name,
                namespace=VM_NAMESPACE,
            )
        # Kubernetes assigns the UID. Seal it with the complete intent before
        # returning any destructive authority. A restart can authenticate the
        # creation signature, seal the assigned UID, then revalidate the exact
        # database reservation before any external effects.
        annotations = _metadata_value(lease, "annotations", {}) or {}
        if not annotations.get("srw.io/cleanup-carrier-signature"):
            self._parse_workspace_cleanup_carrier(lease)
            uid = _metadata_value(lease, "uid")
            version = _metadata_value(lease, "resourceVersion") or _metadata_value(
                lease, "resource_version"
            )
            if (
                not uid
                or not version
                or any(
                    annotations.get(_CLEANUP_ANNOTATIONS[key]) != value
                    for key, value in values.items()
                )
            ):
                raise RuntimeError("workspace cleanup carrier identity changed")
            body["metadata"].update({"uid": uid, "resourceVersion": version})
            body["metadata"]["annotations"].pop(
                "srw.io/cleanup-creation-signature", None
            )
            body["metadata"]["annotations"]["srw.io/cleanup-carrier-signature"] = (
                self._workspace_cleanup_carrier_signature(
                    name=carrier_name, uid=uid, values=values
                )
            )
            lease = await asyncio.to_thread(
                self.coordination_api.replace_namespaced_lease,
                name=carrier_name,
                namespace=VM_NAMESPACE,
                body=body,
            )
        carrier = self._parse_workspace_cleanup_carrier(lease)
        if any(str(carrier[key]) != value for key, value in values.items()):
            raise RuntimeError("workspace cleanup carrier identity changed")
        return carrier

    async def _list_workspace_cleanup_carriers(self) -> tuple[dict[str, object], ...]:
        from shared.vm_creation_issuance import (
            CREATION_INTENT_ANNOTATION,
            CREATION_SIGNATURE_ANNOTATION,
        )

        if self.coordination_api is None:
            raise RuntimeError("workspace cleanup carrier authority is unavailable")
        response = await asyncio.to_thread(
            self.coordination_api.list_namespaced_lease,
            namespace=VM_NAMESPACE,
            label_selector=f"{WORKSPACE_CLEANUP_CARRIER_LABEL}=true",
        )
        items = _object_value(response, "items")
        if items is None and isinstance(response, Mapping):
            items = response.get("items")
        if not isinstance(items, list):
            raise RuntimeError("workspace cleanup carrier authority is malformed")
        carriers = []
        for item in items:
            labels = _metadata_value(item, "labels", {}) or {}
            if (
                not isinstance(labels, Mapping)
                or labels.get(WORKSPACE_CLEANUP_CARRIER_LABEL) != "true"
            ):
                continue
            annotations = _metadata_value(item, "annotations", {}) or {}
            if CREATION_INTENT_ANNOTATION in annotations and not annotations.get(
                CREATION_SIGNATURE_ANNOTATION
            ):
                # A publication interrupted before UID sealing is not effect
                # evidence. Its durable DB reservation still excludes cleanup;
                # only the original authenticated create may finish sealing.
                continue
            carrier = self._parse_workspace_cleanup_carrier(item)
            if not carrier["carrier_sealed"]:
                carrier = await self._refresh_workspace_cleanup_carrier(carrier)
            carriers.append(carrier)
        return tuple(carriers)

    async def _find_workspace_cleanup_carrier(
        self,
        *,
        owner_kind: str,
        owner_id: str,
        source: str,
        name: str,
    ) -> dict[str, object] | None:
        matches = [
            carrier
            for carrier in await self._list_workspace_cleanup_carriers()
            if carrier["owner_kind"] == owner_kind
            and carrier["owner_id"] == owner_id
            and carrier["source"] == source
            and carrier["name"] == name
        ]
        if len(matches) > 1:
            raise RuntimeError("workspace cleanup carrier identity is ambiguous")
        return matches[0] if matches else None

    async def _refresh_workspace_cleanup_carrier(
        self, carrier: Mapping[str, object]
    ) -> dict[str, object]:
        try:
            lease = await asyncio.to_thread(
                self.coordination_api.read_namespaced_lease,
                name=str(carrier["carrier_name"]),
                namespace=VM_NAMESPACE,
            )
        except Exception as exc:
            raise RuntimeError("workspace cleanup carrier is unavailable") from exc
        current = self._parse_workspace_cleanup_carrier(lease)
        for key, value in carrier.items():
            if key in current and current[key] != value:
                raise RuntimeError("workspace cleanup carrier identity changed")
        if not current["carrier_sealed"]:
            annotations = {
                annotation: str(current[key])
                for key, annotation in _CLEANUP_ANNOTATIONS.items()
                if current.get(key)
            }
            annotations["srw.io/cleanup-carrier-signature"] = (
                self._workspace_cleanup_carrier_signature(
                    name=str(current["carrier_name"]),
                    uid=str(current["carrier_uid"]),
                    values=current,
                )
            )
            body = {
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "name": current["carrier_name"],
                    "namespace": VM_NAMESPACE,
                    "uid": current["carrier_uid"],
                    "resourceVersion": current["carrier_resource_version"],
                    "labels": {WORKSPACE_CLEANUP_CARRIER_LABEL: "true"},
                    "annotations": annotations,
                },
                "spec": {"holderIdentity": current["admission_id"]},
            }
            lease = await asyncio.to_thread(
                self.coordination_api.replace_namespaced_lease,
                name=str(current["carrier_name"]),
                namespace=VM_NAMESPACE,
                body=body,
            )
            current = self._parse_workspace_cleanup_carrier(lease)
        return current

    async def _delete_workspace_cleanup_carrier(
        self, carrier: Mapping[str, object]
    ) -> None:
        from kubernetes.client.exceptions import ApiException

        try:
            await asyncio.to_thread(
                self.coordination_api.delete_namespaced_lease,
                name=str(carrier["carrier_name"]),
                namespace=VM_NAMESPACE,
                body={
                    "apiVersion": "v1",
                    "kind": "DeleteOptions",
                    "preconditions": {"uid": str(carrier["carrier_uid"])},
                },
            )
        except ApiException as exc:
            if exc.status != 404:
                raise

    async def _acquire_workspace_cleanup_reservation(
        self,
        *,
        source: str,
        owner_kind: str,
        owner_id: str,
        pvc_uid: str,
        dv_uid: str,
        provision_generation: str,
        parent_cleanup: Mapping | None = None,
        parent_provision_generation: str | None = None,
        expected_vm_uid: str | None = None,
    ) -> dict[str, object]:
        identity = {
            "source": source,
            "owner_kind": owner_kind,
            "owner_id": owner_id,
            "pvc_uid": pvc_uid,
            "dv_uid": dv_uid,
            "provision_generation": provision_generation,
        }
        canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
        if parent_cleanup is not None:
            canonical += ":parent:" + str(parent_cleanup.get("admission_id"))
        request_id = str(uuid5(NAMESPACE_URL, f"srw-controller-cleanup:{canonical}"))
        intent_digest = (
            "sha256:"
            + hashlib.sha256(
                json.dumps(
                    identity,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    default=str,
                ).encode("utf-8")
            ).hexdigest()
        )
        result = await self._workspace_cleanup_authority_request(
            "/api/internal/vm-workspace-cleanup-authority/acquire",
            {
                **identity,
                "request_id": request_id,
                **(
                    {
                        "parent_cleanup": dict(parent_cleanup),
                        "parent_provision_generation": parent_provision_generation,
                        "expected_vm_uid": expected_vm_uid,
                    }
                    if parent_cleanup is not None
                    else {}
                ),
            },
            operation="recovery-cleanup-acquire",
        )
        if result.get("allowed") is not True:
            raise RuntimeError(
                "workspace cleanup is blocked by recovery authority: "
                f"{result.get('reason') or 'refused'}"
            )
        if (
            result.get("request_id") != request_id
            or result.get("intent_digest") != intent_digest
        ):
            raise RuntimeError("workspace cleanup reservation identity changed")
        admission_id = result.get("admission_id")
        if not isinstance(admission_id, str) or not admission_id:
            raise RuntimeError("workspace cleanup reservation was not acknowledged")
        if result.get("completed_outcome") is None:
            result["carrier"] = await self._ensure_workspace_cleanup_carrier(
                result,
                source=source,
                owner_kind=owner_kind,
                owner_id=owner_id,
                pvc_uid=pvc_uid,
                dv_uid=dv_uid,
                provision_generation=provision_generation,
            )
        return result

    async def _complete_workspace_cleanup_reservation(
        self, carrier: Mapping[str, object], *, outcome: str
    ) -> None:
        if carrier.get("outcome") != outcome:
            raise RuntimeError("workspace cleanup carrier outcome changed")
        carrier = await self._refresh_workspace_cleanup_carrier(carrier)
        result = await self._workspace_cleanup_authority_request(
            "/api/internal/vm-workspace-cleanup-authority/complete",
            {
                "admission_id": str(carrier["admission_id"]),
                "request_id": str(carrier["request_id"]),
                "intent_digest": str(carrier["intent_digest"]),
                "outcome": outcome,
            },
            operation="recovery-cleanup-complete",
        )
        if result.get("completed") is not True:
            raise RuntimeError("workspace cleanup completion was not acknowledged")
        await self._delete_workspace_cleanup_carrier(carrier)

    async def _resume_workspace_cleanup_reservation(
        self, carrier: Mapping[str, object]
    ) -> dict[str, object]:
        carrier = await self._refresh_workspace_cleanup_carrier(carrier)
        return await self._workspace_cleanup_authority_request(
            "/api/internal/vm-workspace-cleanup-authority/resume",
            {
                "admission_id": str(carrier["admission_id"]),
                "request_id": str(carrier["request_id"]),
                "intent_digest": str(carrier["intent_digest"]),
                "source": str(carrier["source"]),
                "owner_kind": str(carrier["owner_kind"]),
                "owner_id": str(carrier["owner_id"]),
            },
            operation="recovery-cleanup-resume",
        )

    async def _active_recovery_pins(self) -> tuple[dict[str, str], ...]:
        """Read the complete controller-side pin set or fail closed."""

        if self.coordination_api is None:
            raise RuntimeError("workspace recovery pin authority is unavailable")
        response = await asyncio.to_thread(
            self.coordination_api.list_namespaced_lease,
            namespace=VM_NAMESPACE,
            label_selector=f"{WORKSPACE_RECOVERY_PIN_LABEL}=true",
        )
        items = _object_value(response, "items")
        if items is None and isinstance(response, Mapping):
            items = response.get("items")
        if not isinstance(items, list):
            raise RuntimeError("workspace recovery pin authority is malformed")
        pins: list[dict[str, str]] = []
        for item in items:
            labels = _metadata_value(item, "labels", {}) or {}
            if not isinstance(labels, Mapping):
                raise RuntimeError("workspace recovery pin labels are malformed")
            if labels.get(WORKSPACE_CLEANUP_CARRIER_LABEL) == "true":
                continue
            if labels.get(WORKSPACE_RECOVERY_PIN_LABEL) != "true":
                continue
            pin = {
                "recovery_id": str(labels.get(WORKSPACE_RECOVERY_ID_LABEL) or ""),
                "pvc_uid": str(labels.get(WORKSPACE_RECOVERY_PVC_LABEL) or ""),
                "provision_generation": str(
                    labels.get(WORKSPACE_RECOVERY_GENERATION_LABEL) or ""
                ),
                "pin_uid": str(_metadata_value(item, "uid") or ""),
                "resource_version": str(
                    _metadata_value(item, "resourceVersion")
                    or _metadata_value(item, "resource_version")
                    or ""
                ),
            }
            if not all(pin.values()):
                raise RuntimeError("workspace recovery pin identity is incomplete")
            pins.append(pin)
        return tuple(pins)

    @staticmethod
    def _pvc_is_recovery_pinned(
        pins: tuple[dict[str, str], ...], pvc_uid: str | None
    ) -> bool:
        if pvc_uid is None:
            return False
        return any(pin["pvc_uid"] == pvc_uid for pin in pins)

    async def _do_reconcile_workspace_recovery_pin(
        self, payload: Mapping[str, object], *, _serialized: bool = False
    ) -> dict:
        """Idempotently project one PostgreSQL pin into a Kubernetes Lease."""

        from kubernetes.client.exceptions import ApiException

        try:
            recovery_id = str(UUID(str(payload.get("recovery_id"))))
            pvc_uid = str(UUID(str(payload.get("pvc_uid"))))
            generation = str(UUID(str(payload.get("provision_generation"))))
            owner_id = str(UUID(str(payload.get("owner_id"))))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("workspace recovery pin identity is malformed") from exc
        state = payload.get("state")
        owner_kind = payload.get("owner_kind")
        namespace = payload.get("namespace")
        if state not in {"active", "released"}:
            raise ValueError("workspace recovery pin state is invalid")
        if owner_kind not in {"job", "thread"} or namespace != VM_NAMESPACE:
            raise ValueError("workspace recovery pin owner is invalid")
        if not _serialized:
            async with self._workspace_lifecycle(owner_id):
                return await self._do_reconcile_workspace_recovery_pin(
                    payload, _serialized=True
                )
        name = self._recovery_pin_name(recovery_id)
        try:
            lease = await asyncio.to_thread(
                self.coordination_api.read_namespaced_lease,
                name=name,
                namespace=VM_NAMESPACE,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
            lease = None
        expected_labels = {
            WORKSPACE_RECOVERY_PIN_LABEL: "true",
            WORKSPACE_RECOVERY_ID_LABEL: recovery_id,
            WORKSPACE_RECOVERY_PVC_LABEL: pvc_uid,
            WORKSPACE_RECOVERY_GENERATION_LABEL: generation,
            "srw.io/owner-kind": str(owner_kind),
            "srw.io/owner-id": owner_id,
        }
        if lease is not None:
            labels = _metadata_value(lease, "labels", {}) or {}
            if not isinstance(labels, Mapping) or any(
                labels.get(key) != value for key, value in expected_labels.items()
            ):
                raise RuntimeError("workspace recovery pin identity changed")
            pin_uid = _safe_uid(_metadata_value(lease, "uid"))
            resource_version = str(
                _metadata_value(lease, "resourceVersion")
                or _metadata_value(lease, "resource_version")
                or ""
            )
            if pin_uid is None or not resource_version:
                raise RuntimeError("workspace recovery pin metadata is incomplete")
        else:
            pin_uid = None
            resource_version = ""
        if state == "released":
            expected_uid = payload.get("pin_uid")
            if not isinstance(expected_uid, str) or not expected_uid:
                raise ValueError("workspace recovery release pin UID is required")
            if lease is None:
                # A retry after a successful delete has exact intent in its
                # signed request and the durable PostgreSQL command.
                return {
                    "state": "released",
                    "recovery_id": recovery_id,
                    "pvc_uid": pvc_uid,
                    "provision_generation": generation,
                    "pin_uid": expected_uid,
                    "resource_version": str(
                        payload.get("resource_version") or "deleted"
                    ),
                }
            if pin_uid != expected_uid:
                raise RuntimeError("stale workspace recovery pin release refused")
            await asyncio.to_thread(
                self.coordination_api.delete_namespaced_lease,
                name=name,
                namespace=VM_NAMESPACE,
                body={"preconditions": {"uid": pin_uid}},
            )
            return {
                "state": "released",
                "recovery_id": recovery_id,
                "pvc_uid": pvc_uid,
                "provision_generation": generation,
                "pin_uid": pin_uid,
                "resource_version": resource_version,
            }
        if lease is None:
            known, pvc = await self._rootdisk_pvc_by_uid(
                pvc_uid, owner_id=owner_id, owner_kind=str(owner_kind)
            )
            pvc_name = _metadata_value(pvc, "name") if pvc is not None else None
            if (
                not known
                or pvc is None
                or _safe_uid(_metadata_value(pvc, "uid")) != pvc_uid
                or not isinstance(pvc_name, str)
                or not pvc_name
            ):
                raise RuntimeError("workspace recovery PVC identity is unavailable")
            dv = await self._get_dv(pvc_name)
            dv_metadata = (dv or {}).get("metadata") or {}
            dv_labels = dv_metadata.get("labels") or {}
            dv_uid = _safe_uid(dv_metadata.get("uid"))
            if (
                dv is None
                or dv_uid is None
                or dv_metadata.get("name") != pvc_name
                or dv_metadata.get("deletionTimestamp")
                or not isinstance(dv_labels, Mapping)
                or dv_labels.get("srw.io/owner-kind") != owner_kind
                or dv_labels.get("srw.io/owner-id") != owner_id
                or not _owned_by(pvc, kind="DataVolume", uid=dv_uid)
            ):
                raise RuntimeError(
                    "workspace recovery DataVolume identity is unavailable"
                )
            created = await asyncio.to_thread(
                self.coordination_api.create_namespaced_lease,
                namespace=VM_NAMESPACE,
                body={
                    "apiVersion": "coordination.k8s.io/v1",
                    "kind": "Lease",
                    "metadata": {
                        "name": name,
                        "namespace": VM_NAMESPACE,
                        "labels": expected_labels,
                    },
                    "spec": {},
                },
            )
            pin_uid = _safe_uid(_metadata_value(created, "uid"))
            resource_version = str(
                _metadata_value(created, "resourceVersion")
                or _metadata_value(created, "resource_version")
                or ""
            )
            if pin_uid is None or not resource_version:
                raise RuntimeError("workspace recovery pin create was not acknowledged")
        return {
            "state": "active",
            "recovery_id": recovery_id,
            "pvc_uid": pvc_uid,
            "provision_generation": generation,
            "pin_uid": pin_uid,
            "resource_version": resource_version,
        }

    async def _verify_lifecycle_request(
        self, payload: Mapping[str, object], operation: str, *, mutating: bool
    ) -> bool:
        """Verify freshness/MAC and durably claim mutating request nonces."""

        if not verify_payload(
            payload,
            direction="request",
            operation=operation,
            secret=LIFECYCLE_HMAC_SECRET,
        ):
            return False
        if LIFECYCLE_HMAC_SECRET is None or not mutating:
            return True
        auth = payload.get(AUTH_FIELD)
        if not isinstance(auth, Mapping):
            return False
        request_id = auth.get("request_id")
        if not isinstance(request_id, str):
            return False
        if not hasattr(self, "_seen_lifecycle_requests"):
            self._seen_lifecycle_requests = OrderedDict()
        now = time.monotonic()
        oldest_allowed = now - 120.0
        while self._seen_lifecycle_requests:
            first_id, first_seen = next(iter(self._seen_lifecycle_requests.items()))
            if first_seen >= oldest_allowed:
                break
            self._seen_lifecycle_requests.pop(first_id, None)
        if request_id in self._seen_lifecycle_requests:
            return False
        if not await self._claim_lifecycle_nonce(request_id, operation):
            return False
        self._seen_lifecycle_requests[request_id] = now
        while len(self._seen_lifecycle_requests) > max(1, LIFECYCLE_REPLAY_CACHE_SIZE):
            self._seen_lifecycle_requests.popitem(last=False)
        return True

    async def _claim_lifecycle_nonce(self, request_id: str, operation: str) -> bool:
        """Atomically consume one signed mutation nonce in Kubernetes.

        A namespaced Lease survives controller restarts and is shared across
        replicas. Kubernetes create is the compare-and-set: HTTP 409 means the
        request UUID was already consumed. Any other API/RBAC failure rejects
        the mutation instead of silently downgrading to the in-memory cache.
        """

        if self.coordination_api is None:
            log.error("Lifecycle nonce store is unavailable; rejecting %s", operation)
            return False
        nonce_name = f"srw-vm-lifecycle-{UUID(request_id).hex}"
        now = datetime.now(timezone.utc)
        body = {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": nonce_name,
                "namespace": VM_NAMESPACE,
                "labels": {_LIFECYCLE_NONCE_LABEL: "true"},
            },
            "spec": {
                "holderIdentity": f"{operation}:{request_id}",
                "acquireTime": now.isoformat().replace("+00:00", "Z"),
                "leaseDurationSeconds": LIFECYCLE_NONCE_TTL_SECONDS,
            },
        }
        try:
            await asyncio.to_thread(
                self.coordination_api.create_namespaced_lease,
                namespace=VM_NAMESPACE,
                body=body,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 409:
                log.warning("Rejecting replayed VM lifecycle request %s", request_id)
            else:
                log.error(
                    "Could not durably claim VM lifecycle request %s: %s",
                    request_id,
                    exc,
                )
            return False

        self._lifecycle_nonce_claim_count = (
            getattr(self, "_lifecycle_nonce_claim_count", 0) + 1
        )
        if self._lifecycle_nonce_claim_count % LIFECYCLE_NONCE_GC_INTERVAL == 0:
            if not await self._gc_expired_lifecycle_nonces(now=now):
                return False
        return True

    async def _gc_expired_lifecycle_nonces(
        self, *, now: datetime | None = None
    ) -> bool:
        """Best-effort bounded-TTL cleanup, failing closed when it is due."""

        if self.coordination_api is None:
            return False
        cutoff = (now or datetime.now(timezone.utc)).timestamp() - (
            LIFECYCLE_NONCE_TTL_SECONDS
        )
        cursor = getattr(self, "_lifecycle_nonce_gc_continue", None)
        list_kwargs = {
            "namespace": VM_NAMESPACE,
            "label_selector": f"{_LIFECYCLE_NONCE_LABEL}=true",
            "limit": LIFECYCLE_NONCE_GC_PAGE_LIMIT,
        }
        if cursor:
            list_kwargs["_continue"] = cursor
        try:
            response = await asyncio.to_thread(
                self.coordination_api.list_namespaced_lease,
                **list_kwargs,
            )
            items = (
                response.get("items", [])
                if isinstance(response, Mapping)
                else getattr(response, "items", [])
            )
            response_metadata = (
                response.get("metadata", {})
                if isinstance(response, Mapping)
                else getattr(response, "metadata", None)
            )
            if isinstance(response_metadata, Mapping):
                next_cursor = response_metadata.get("continue")
            else:
                next_cursor = getattr(response_metadata, "_continue", None)
            if not isinstance(next_cursor, str) or not next_cursor:
                next_cursor = None
            deleted = 0
            page_exhausted = True
            for lease in (items or [])[:LIFECYCLE_NONCE_GC_PAGE_LIMIT]:
                metadata = (
                    lease.get("metadata")
                    if isinstance(lease, Mapping)
                    else getattr(lease, "metadata", None)
                )
                if isinstance(metadata, Mapping):
                    name = metadata.get("name")
                    created_at = metadata.get("creationTimestamp")
                else:
                    name = getattr(metadata, "name", None)
                    created_at = getattr(metadata, "creation_timestamp", None)
                if not isinstance(name, str):
                    continue
                if isinstance(created_at, str):
                    try:
                        created_at = datetime.fromisoformat(
                            created_at.replace("Z", "+00:00")
                        )
                    except ValueError:
                        continue
                if not isinstance(created_at, datetime):
                    continue
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                if created_at.timestamp() > cutoff:
                    continue
                if deleted >= LIFECYCLE_NONCE_GC_DELETE_LIMIT:
                    page_exhausted = False
                    break
                deletion_succeeded = True
                try:
                    await asyncio.to_thread(
                        self.coordination_api.delete_namespaced_lease,
                        name=name,
                        namespace=VM_NAMESPACE,
                        body={"apiVersion": "v1", "kind": "DeleteOptions"},
                    )
                except Exception as exc:
                    if getattr(exc, "status", None) != 404:
                        raise
                    deletion_succeeded = False
                if deletion_succeeded:
                    deleted += 1
            if page_exhausted:
                self._lifecycle_nonce_gc_continue = next_cursor
            return True
        except Exception as exc:
            if getattr(exc, "status", None) == 410:
                self._lifecycle_nonce_gc_continue = None
                log.info("VM lifecycle nonce GC cursor expired; restarting scan")
                return True
            log.error("VM lifecycle nonce garbage collection failed: %s", exc)
            return False

    def load_template(self):
        """Load the VM template as raw text for placeholder substitution."""
        path = VM_TEMPLATE_PATH
        if not os.path.exists(path):
            log.error("VM template not found at %s", path)
            sys.exit(1)

        with open(path) as f:
            self.template_text = f.read()

        cloud_init_path = VM_CLOUD_INIT_PATH
        if os.path.exists(cloud_init_path):
            with open(cloud_init_path) as f:
                self.cloud_init_text = f.read()
            log.info("Loaded cloud-init template from %s", cloud_init_path)
        else:
            # The parked external chart embeds userData in its VM template.
            # Keeping this optional preserves that image/transport contract.
            self.cloud_init_text = ""

        log.info("Loaded VM template from %s", path)

    @staticmethod
    def _escape_for_job_config(description: str) -> str:
        """JSON-escape a description for the template's job-config.json blob.

        Returns the escaped body without json.dumps' surrounding quotes, since
        the template supplies those. Shrinks the raw text until its *escaped*
        form fits MAX_DESCRIPTION_LEN, so an escape sequence is never cut in
        half.

        Keep in sync with the copy in src/orchestrator/services/vm_provisioner.py —
        separate images, so the two cannot share code.
        """
        raw = description[:MAX_DESCRIPTION_LEN]
        while raw and len(json.dumps(raw)[1:-1]) > MAX_DESCRIPTION_LEN:
            raw = raw[:-1]
        return json.dumps(raw)[1:-1]

    def render_template(self, job_config: dict, tailscale_auth_key: str = "") -> dict:
        """Render the VM template with job-specific values.

        Performs string substitution on the raw YAML text, then parses
        the result. This handles placeholders in both string and numeric
        contexts (e.g., cores: ${CPU_CORES} becomes cores: 2).

        Args:
            job_config: Dict with keys: job_id, agent_config, vm_image,
                        cpu_cores, memory, nats_url, description.
            tailscale_auth_key: Headscale pre-auth key for the VM to join
                                the tailnet. Empty string if Headscale unavailable.

        Returns:
            Parsed YAML dict ready for the Kubernetes API.
        """
        headscale_url = os.environ.get("HEADSCALE_URL", "")
        owner_kind, owner_id = _owner_identity(job_config)
        # The orchestrator resolves the project's tier per job; the chart's
        # VM_DEFAULT_NETWORK_TIER is only the fallback for payloads that omit it.
        network_tier = (
            str(job_config.get("network_tier") or "").strip()
            or VM_DEFAULT_NETWORK_TIER
            or "internet-only"
        )
        if not _NETWORK_TIER_PATTERN.fullmatch(network_tier):
            raise ValueError("network_tier must match ^[a-z0-9-]{1,63}$")
        orchestrator_url = (
            ORCHESTRATOR_URL or str(job_config.get("orchestrator_url") or "").strip()
        )
        generation = _provision_generation(job_config.get("provision_generation"))
        initialization = job_config.get("initialization")
        if initialization is not None:
            from shared.workspace_initialization import validate_initialization_request

            initialization = validate_initialization_request(initialization)
            if not getattr(self, "cloud_init_text", ""):
                raise ValueError(
                    "VM initialization requires the same-cluster cloud-init template."
                )
        vm_auth_token = (
            guest_token(
                LIFECYCLE_HMAC_SECRET,
                owner_kind,
                owner_id,
                generation,
            )
            if LIFECYCLE_HMAC_SECRET is not None and generation is not None
            else ""
        )

        replacements = {
            "${JOB_ID}": job_config["job_id"],
            "${OWNER_KIND}": owner_kind,
            "${OWNER_ID}": owner_id,
            "${AGENT_CONFIG}": job_config.get("agent_config", "worker_base"),
            "${VM_IMAGE}": job_config.get("vm_image", DEFAULT_VM_IMAGE),
            "${CPU_CORES}": str(job_config.get("cpu_cores", DEFAULT_CPU)),
            "${MEMORY}": job_config.get("memory", DEFAULT_MEMORY),
            # Always use the local leaf node URL — the VM runs on this cluster,
            # not the orchestrator's cluster where the job's nats_url points.
            "${NATS_URL}": NATS_URL,
            # Per-orchestrator scope for the management-daemon + sudo-gated
            # NATS subjects inside the VM. Burned into /etc/default by
            # cloud-init so the in-VM publishers reach this orchestrator's
            # scoped subscribe wildcards.
            "${ORCHESTRATOR_ID}": ORCHESTRATOR_ID,
            "${DESCRIPTION}": self._escape_for_job_config(
                job_config.get("description", "")
            ),
            # CDI DataVolume storage
            "${VM_STORAGE_CLASS}": VM_STORAGE_CLASS,
            "${VM_DISK_SIZE}": effective_disk_size(job_config),
            # Headscale mesh VPN — VM joins tailnet on boot
            "${TAILSCALE_AUTH_KEY}": tailscale_auth_key,
            "${HEADSCALE_URL}": headscale_url,
            # The Vault-backed chart branch injects this from its synced
            # Secret. The inline-key branch contains no such placeholder, so
            # an absent environment value is harmless there.
            "${SSH_AUTHORIZED_KEY}": os.environ.get("SSH_AUTHORIZED_KEY", ""),
            "${VM_AUTH_TOKEN}": vm_auth_token,
            "${ORCHESTRATOR_URL}": orchestrator_url,
            "${NETWORK_TIER}": network_tier,
        }

        rendered = self.template_text
        for placeholder, value in replacements.items():
            rendered = rendered.replace(placeholder, value)

        manifest = yaml.safe_load(rendered)
        vmi_spec = manifest["spec"]["template"]["spec"]
        if VM_NODE_SELECTOR:
            vmi_spec["nodeSelector"] = dict(VM_NODE_SELECTOR)
        if VM_TOLERATIONS:
            vmi_spec["tolerations"] = list(VM_TOLERATIONS)
        if cloud_init_text := getattr(self, "cloud_init_text", ""):
            rendered_cloud_init = cloud_init_text
            for placeholder, value in replacements.items():
                rendered_cloud_init = rendered_cloud_init.replace(placeholder, value)
            if initialization is not None:
                from vm_controller.workspace_initialization import (
                    inject_workspace_initialization,
                )

                rendered_cloud_init = inject_workspace_initialization(
                    rendered_cloud_init,
                    owner_id=(job_config.get("workspace_storage") or {}).get(
                        "uid", owner_id
                    ),
                    request=initialization,
                )
            # Only the same-cluster chart mounts this Secret-backed template.
            # The parked external/direct template remains inline and therefore
            # keeps its existing guest-generated host-key behavior for now.
            rendered_cloud_init, host_key_fingerprint = _inject_ssh_host_key(
                rendered_cloud_init
            )
            # Internal hand-off only; both fields are removed before the VM is
            # sent to KubeVirt. The private key persists only in the Secret.
            manifest["_srwCloudInitUserData"] = rendered_cloud_init
            manifest["_srwSSHHostKeyFingerprint"] = host_key_fingerprint
        _stamp_owner_identity(manifest, owner_kind, owner_id)
        if generation:
            _stamp_provision_generation(manifest, generation)
        return manifest

    def init_k8s(self):
        """Initialize the Kubernetes client using in-cluster config."""
        from kubernetes import client, config

        config.load_incluster_config()
        self.k8s_client = client.CustomObjectsApi()
        self.core_api = client.CoreV1Api()
        self.coordination_api = client.CoordinationV1Api()
        log.info("Kubernetes client initialized (in-cluster)")

    async def connect_nats(self):
        """Connect to the NATS leaf node on the agent cluster."""
        import nats

        async def error_handler(e):
            log.error("NATS error: %s", e)

        async def disconnected_handler():
            log.warning("NATS disconnected")

        async def reconnected_handler():
            log.info("NATS reconnected")

        self.nc = await nats.connect(
            NATS_URL,
            error_cb=error_handler,
            disconnected_cb=disconnected_handler,
            reconnected_cb=reconnected_handler,
            max_reconnect_attempts=-1,  # Reconnect indefinitely
            reconnect_time_wait=2,
        )
        log.info("Connected to NATS at %s", NATS_URL)

    # =========================================================================
    # Transport-agnostic core
    #
    # Each `_do_*` method takes a plain dict, performs the K8s work, and
    # returns a result dict shaped the same as the historical NATS status
    # payload. Both NATS and HTTP transports wrap these.
    # =========================================================================

    async def _capacity_wait(self, vm_name: str) -> dict | None:
        """Return waiting_capacity when the live VM cap is already occupied."""

        if VM_MAX_CONCURRENT == 0:
            return None
        response = await asyncio.to_thread(
            self.k8s_client.list_namespaced_custom_object,
            group=KUBEVIRT_GROUP,
            version=KUBEVIRT_VERSION,
            namespace=VM_NAMESPACE,
            plural=KUBEVIRT_PLURAL,
        )
        live_names = []
        for item in response.get("items", []):
            metadata = item.get("metadata", {})
            name = metadata.get("name", "")
            if (
                name.startswith("agent-vm-")
                and not name.startswith("agent-vm-golden-")
                and not metadata.get("deletionTimestamp")
            ):
                live_names.append(name)
        # A retried create for an admitted VM remains idempotent even at cap.
        if vm_name in live_names or len(live_names) < VM_MAX_CONCURRENT:
            return None
        return {
            "status": "waiting_capacity",
            "running_vms": len(live_names),
            "max_concurrent_vms": VM_MAX_CONCURRENT,
        }

    async def _ensure_cloud_init_secret(
        self,
        *,
        job_id: str,
        owner_kind: str,
        generation: str | None,
        user_data: str,
        host_key_fingerprint: str,
    ) -> tuple[bool, str]:
        """Ensure the NoCloud Secret and return its durable public identity."""

        from kubernetes.client.exceptions import ApiException

        secret_name = f"agent-vm-{job_id}-cloudinit"
        metadata: dict[str, object] = {
            "name": secret_name,
            "namespace": VM_NAMESPACE,
            "labels": {
                "srw.io/owner-kind": owner_kind,
                "srw.io/owner-id": job_id,
            },
        }
        annotations = {_SSH_HOST_KEY_FINGERPRINT_ANNOTATION: host_key_fingerprint}
        if generation is not None:
            annotations[_PROVISION_GENERATION_ANNOTATION] = generation
        metadata["annotations"] = annotations
        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": metadata,
            "type": "Opaque",
            "stringData": {"userdata": user_data},
        }
        try:
            await asyncio.to_thread(
                self.core_api.create_namespaced_secret,
                namespace=VM_NAMESPACE,
                body=body,
            )
            return True, host_key_fingerprint
        except ApiException as exc:
            if exc.status != 409:
                raise
            existing = await asyncio.to_thread(
                self.core_api.read_namespaced_secret,
                name=secret_name,
                namespace=VM_NAMESPACE,
            )
            existing_metadata = getattr(existing, "metadata", None)
            labels = getattr(existing_metadata, "labels", None) or {}
            annotations = getattr(existing_metadata, "annotations", None) or {}
            if (
                labels.get("srw.io/owner-kind") != owner_kind
                or labels.get("srw.io/owner-id") != job_id
                or (
                    generation is not None
                    and annotations.get(_PROVISION_GENERATION_ANNOTATION) != generation
                )
            ):
                raise RuntimeError(
                    "existing cloud-init Secret belongs to another VM generation"
                ) from exc
            admitted_fingerprint = _ssh_host_key_fingerprint(
                annotations.get(_SSH_HOST_KEY_FINGERPRINT_ANNOTATION)
            )
            if admitted_fingerprint is None:
                raise RuntimeError(
                    "existing cloud-init Secret lacks a valid SSH host-key fingerprint"
                ) from exc
            # A lost create response may retry with the same generation after
            # the Secret already exists. Return that Secret's fingerprint, not
            # the newly generated but unused render, so the orchestrator pins
            # the identity the guest will actually receive.
            return False, admitted_fingerprint

    async def _patch_cloud_init_secret_owner(
        self, *, job_id: str, vm_name: str, vm_uid: str
    ) -> None:
        """Make the admitted VirtualMachine own its token-bearing Secret."""

        await asyncio.to_thread(
            self.core_api.patch_namespaced_secret,
            name=f"agent-vm-{job_id}-cloudinit",
            namespace=VM_NAMESPACE,
            body={
                "metadata": {
                    "ownerReferences": [
                        {
                            "apiVersion": "kubevirt.io/v1",
                            "kind": "VirtualMachine",
                            "name": vm_name,
                            "uid": vm_uid,
                            "controller": True,
                            "blockOwnerDeletion": False,
                        }
                    ]
                }
            },
        )

    async def _delete_cloud_init_secret(self, job_id: str) -> None:
        """Delete a VM's NoCloud Secret, tolerating an already-absent Secret."""

        from kubernetes.client.exceptions import ApiException

        try:
            await asyncio.to_thread(
                self.core_api.delete_namespaced_secret,
                name=f"agent-vm-{job_id}-cloudinit",
                namespace=VM_NAMESPACE,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise

    def _retained_storage(self):
        from vm_controller.retained_storage import RetainedStorage

        if not hasattr(self, "_retained_storage_service"):
            self._retained_storage_service = RetainedStorage(self, VM_NAMESPACE)
        return self._retained_storage_service

    def _workspace_preparation(self):
        from shared.workspace_preparation_settings import PreparationSettings
        from vm_controller.workspace_preparation import VMWorkspacePreparation

        if not hasattr(self, "_workspace_preparation_service"):
            self._workspace_preparation_service = VMWorkspacePreparation(
                self,
                namespace=VM_NAMESPACE,
                storage_class=VM_STORAGE_CLASS,
                settings=PreparationSettings.from_environment(),
            )
        return self._workspace_preparation_service

    async def _preparation_loop(self):
        while not self._shutdown.is_set():
            try:
                await self._workspace_preparation().reconcile()
            except Exception:
                log.exception("Workspace preparation reconciliation failed")
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass

    async def _do_create(self, job_config: dict) -> dict:
        """Create a KubeVirt VirtualMachine for a job."""
        job_id = job_config.get("job_id", "unknown")
        binding = job_config.get("workspace_storage")
        lifecycle_owner = str(job_id)
        if binding is not None:
            from shared.vm_workspace_storage import storage_binding

            binding = storage_binding(binding)
            lifecycle_owner = binding["owner_id"]
        async with self._workspace_lifecycle(lifecycle_owner):
            capacity_lock = getattr(self, "_capacity_lock", None)
            if capacity_lock is None:
                # A few unit-test fixtures intentionally construct via __new__.
                capacity_lock = asyncio.Lock()
                self._capacity_lock = capacity_lock
            async with capacity_lock:
                if binding is not None:
                    if (
                        not VM_PERSISTENT_ROOTDISK
                        or LIFECYCLE_HMAC_SECRET is None
                        or not getattr(self, "cloud_init_text", "")
                    ):
                        raise ValueError(
                            "Retained VM workspaces require persistent disks and authenticated lifecycle hosting."
                        )
                    return await self._do_create_serialized(job_config)
                return await self._do_create_serialized(job_config)

    async def _do_create_serialized(self, job_config: dict) -> dict:
        """Create while holding the reusable entity-name lifecycle lock."""
        if "creation_retry" in job_config:
            from vm_controller.creation_actuation import CreationActuator

            return await CreationActuator(self).run(job_config)
        from kubernetes.client.exceptions import ApiException

        job_id = job_config.get("job_id", "unknown")
        if job_config.get("initialization") is not None:
            from shared.workspace_initialization import validate_initialization_request

            validate_initialization_request(job_config["initialization"])
            if not getattr(self, "cloud_init_text", ""):
                raise ValueError(
                    "VM initialization requires the same-cluster cloud-init template."
                )
        owner_kind, _ = _owner_identity(job_config)
        generation = _provision_generation(job_config.get("provision_generation"))
        if LIFECYCLE_HMAC_SECRET is not None and generation is None:
            raise ValueError(
                "authenticated VM create requires a canonical provision_generation"
            )
        log.info("Creating VM for job %s", job_id)

        capacity = await self._capacity_wait(f"agent-vm-{job_id}")
        if capacity is not None:
            result = {"job_id": job_id, **capacity}
            if generation is not None:
                result["provision_generation"] = generation
            return result

        # Golden-image acceleration: import the base image once into a shared
        # golden PVC and clone the rootdisk from it, instead of a per-VM registry
        # import. Runs on EVERY create (incl. crash-recovery re-dispatch).
        #
        # NON-BLOCKING: if the golden is still importing (a cold import after an
        # agent-vm-base bump takes ~30 min — longer than every provisioning
        # budget), return ``waiting_golden`` WITHOUT creating the VM. Blocking
        # here (the old 900s wait) let the orchestrator recycle and re-create
        # while stale handlers were still parked in the wait; when the golden
        # finally succeeded, the racers collided in 409 AlreadyExists and failed
        # two loop jobs. The orchestrator polls create until the golden is
        # ready (see knowledge-history/done/golden_image_cold_import_fails_inflight_vm_jobs.md).
        # Checked FIRST so a poll doesn't mint a fresh Headscale auth key every
        # ~30s for the whole import window.
        #
        # Golden-infra *errors* (not in-progress imports) leave golden_name
        # None → the rendered manifest keeps its registry source (byte-for-byte
        # legacy behaviour + fallback).
        image = job_config.get("vm_image") or DEFAULT_VM_IMAGE
        golden_name = None
        prepared = None
        preparation = job_config.get("preparation")
        if preparation is not None:
            from shared.workspace_preparation import validate_request

            preparation = validate_request(preparation)
            if (
                preparation["allocationId"] != job_id
                or preparation["ownerKind"]
                != ("session" if owner_kind == "thread" else "job")
                or LIFECYCLE_HMAC_SECRET is None
                or not VM_PERSISTENT_ROOTDISK
                or not getattr(self, "cloud_init_text", "")
            ):
                raise ValueError(
                    "VM preparation requires authenticated, persistent same-cluster allocation."
                )
            binding = job_config.get("workspace_storage")
            from shared.vm_workspace_storage import storage_name

            root_name = storage_name(binding) if binding else _rootdisk_name(job_id)
            known, existing_root = await self._rootdisk_pvc_probe(
                root_name,
                owner_id=binding["owner_id"] if binding else job_id,
                owner_kind=binding["owner_kind"] if binding else owner_kind,
                wait=False,
            )
            if not known:
                raise RuntimeError("Existing workspace disk identity is unknown.")
            if existing_root is None:
                prepared, waiting = await self._workspace_preparation().prepare(
                    preparation
                )
                if waiting is not None:
                    return {
                        "job_id": job_id,
                        "provision_generation": generation,
                        **waiting,
                    }
                golden_name = prepared["name"]
        if (
            preparation is None
            and VM_GOLDEN_IMAGE_ENABLED
            and not (job_config.get("workspace_storage") or {}).get("pvc_uid")
        ):
            waiting = None
            try:
                golden_name, waiting = await self._golden_state_nowait(image)
            except Exception:
                log.exception(
                    "golden ensure failed for job %s — falling back to registry",
                    job_id,
                )
            if waiting is not None:
                log.info(
                    "golden %s not ready for job %s (%s) — deferring VM create",
                    waiting.get("golden"),
                    job_id,
                    waiting.get("golden_progress") or waiting.get("golden_phase"),
                )
                result = {"job_id": job_id, "status": "waiting_golden", **waiting}
                if generation is not None:
                    result["provision_generation"] = generation
                return result

        # Mesh VPN is how the orchestrator reaches the guest: a VM that boots
        # without a pre-auth key never joins the tailnet, so its daemon
        # registers with the QEMU-NAT address and ssh_ready=false forever. It
        # looks alive (it heartbeats) but is unreachable, and burns the full
        # provisioning budget — 3 × 10 min — before the job fails. Defer the
        # create instead, mirroring waiting_golden: no VM is built, so the
        # dispatcher polls without consuming a provision attempt. See
        # knowledge-base/knowledge/issues/vm_controller_headscale_latch_kills_provisioning.md.
        tailscale_auth_key = ""
        if self.headscale.is_available:
            tailscale_auth_key = await self.headscale.create_auth_key(job_id) or ""
            if not tailscale_auth_key:
                headscale_error = self.headscale.last_error or "Headscale unavailable"
                log.warning(
                    "No Headscale auth key for job %s (%s) — deferring VM create; "
                    "a keyless VM could never be reached",
                    job_id,
                    headscale_error,
                )
                result = {
                    "job_id": job_id,
                    "status": "waiting_headscale",
                    "headscale_error": headscale_error,
                }
                if generation is not None:
                    result["provision_generation"] = generation
                return result

        if (
            "${SSH_AUTHORIZED_KEY}" in getattr(self, "cloud_init_text", "")
            and not os.environ.get("SSH_AUTHORIZED_KEY", "").strip()
        ):
            raise ValueError(
                "SSH_AUTHORIZED_KEY must be non-empty for Secret-backed cloud-init"
            )

        manifest = self.render_template(job_config, tailscale_auth_key)
        cloud_init_user_data = manifest.pop("_srwCloudInitUserData", None)
        ssh_host_key_fingerprint = manifest.pop("_srwSSHHostKeyFingerprint", None)
        # Derive the name from the job id rather than reading it back out of
        # the rendered manifest: the manifest carries the Tailscale auth key,
        # the SSH key and the VM auth token, so nothing lifted out of it may
        # reach a log record. This is the same name the template renders.
        vm_name = f"agent-vm-{job_id}"
        if prepared is not None:
            from shared.workspace_preparation import PREPARATION_LABEL, canonical

            manifest["metadata"].setdefault("labels", {})[PREPARATION_LABEL] = prepared[
                "preparation"
            ]["uid"]
            manifest["metadata"].setdefault("annotations", {})[
                "srw.io/prepared-artifact"
            ] = canonical(prepared["preparation"])
        if golden_name:
            self._apply_clone_source(manifest, golden_name)

        # Detach the rootdisk from the VM object so it outlives it. Must run
        # AFTER the clone mutation above — it lifts the template's dataVolume
        # spec as-is, clone source included.
        workspace_storage = job_config.get("workspace_storage")
        rootdisk_reservation: dict[str, object] | None = None
        if workspace_storage is not None:
            await self._retained_storage().ensure(manifest, workspace_storage, job_id)
        elif VM_PERSISTENT_ROOTDISK:
            await self._ensure_rootdisk(
                manifest,
                job_id,
                owner_kind=owner_kind,
                provision_generation=generation or "legacy",
            )
            candidate = manifest.pop("_srwRootdiskReservation", None)
            if isinstance(candidate, dict):
                rootdisk_reservation = candidate

        cloud_init_secret_created = False
        if cloud_init_user_data is not None:
            if (
                fingerprint := _ssh_host_key_fingerprint(ssh_host_key_fingerprint)
            ) is None:
                raise RuntimeError(
                    "Secret-backed cloud-init lacks a valid SSH host-key fingerprint"
                )
            (
                cloud_init_secret_created,
                ssh_host_key_fingerprint,
            ) = await self._ensure_cloud_init_secret(
                job_id=job_id,
                owner_kind=owner_kind,
                generation=generation,
                user_data=cloud_init_user_data,
                host_key_fingerprint=fingerprint,
            )

        max_retries = 12  # ~60s total
        admitted_vm: object | None = None
        try:
            if rootdisk_reservation is not None:
                carrier = rootdisk_reservation.get("carrier")
                if rootdisk_reservation.get("completed") is not True:
                    if not isinstance(carrier, Mapping):
                        raise RuntimeError(
                            "workspace cleanup carrier was not published"
                        )
                    carrier = await self._refresh_workspace_cleanup_carrier(carrier)
                    if (
                        carrier["owner_kind"] != rootdisk_reservation["owner_kind"]
                        or carrier["owner_id"] != rootdisk_reservation["owner_id"]
                        or carrier["name"] != rootdisk_reservation["name"]
                        or carrier["provision_generation"] != (generation or "legacy")
                    ):
                        raise RuntimeError(
                            "workspace cleanup carrier generation or owner changed"
                        )
                    resumed = await self._resume_workspace_cleanup_reservation(carrier)
                    if resumed.get("allowed") is not True:
                        raise RuntimeError(
                            "workspace cleanup reservation is no longer active"
                        )
                    if rootdisk_reservation["outcome"] == "recreated" and (
                        carrier["successor_dv_uid"] != rootdisk_reservation["dv_uid"]
                        or carrier["successor_pvc_uid"]
                        != rootdisk_reservation["pvc_uid"]
                    ):
                        raise RuntimeError(
                            "workspace cleanup successor binding changed"
                        )
                    if rootdisk_reservation["outcome"] == "adopted" and (
                        carrier["old_dv_uid"] != rootdisk_reservation["dv_uid"]
                        or carrier["old_pvc_uid"] != rootdisk_reservation["pvc_uid"]
                    ):
                        raise RuntimeError("workspace cleanup adopted identity changed")
                    rootdisk_reservation["carrier"] = carrier
                _, _, _, current_pvc_uid = await self._exact_rootdisk_identity(
                    str(rootdisk_reservation["name"]),
                    owner_kind=str(rootdisk_reservation["owner_kind"]),
                    owner_id=str(rootdisk_reservation["owner_id"]),
                    expected_dv_uid=str(rootdisk_reservation["dv_uid"]),
                    expected_pvc_uid=str(rootdisk_reservation["pvc_uid"]),
                )
                if self._pvc_is_recovery_pinned(
                    await self._active_recovery_pins(), current_pvc_uid
                ):
                    raise RuntimeError("rootdisk is pinned for workspace recovery")
                if rootdisk_reservation.get("completed") is True:
                    # The only safe replay of a completed adoption is the VM
                    # admitted while that reservation was open. A later
                    # recovery may begin after completion, so never perform a
                    # new create from a completed permit.
                    admitted_vm = await asyncio.to_thread(
                        self.k8s_client.get_namespaced_custom_object,
                        group=KUBEVIRT_GROUP,
                        version=KUBEVIRT_VERSION,
                        namespace=VM_NAMESPACE,
                        plural=KUBEVIRT_PLURAL,
                        name=vm_name,
                    )
                    replay_generation = _admitted_provision_generation(admitted_vm)
                    if generation is not None and replay_generation != generation:
                        raise RuntimeError(
                            "completed rootdisk adoption has no exact admitted VM"
                        )
            for attempt in range(max_retries + 1):
                if admitted_vm is not None:
                    break
                try:
                    admitted_vm = await asyncio.to_thread(
                        self.k8s_client.create_namespaced_custom_object,
                        group=KUBEVIRT_GROUP,
                        version=KUBEVIRT_VERSION,
                        namespace=VM_NAMESPACE,
                        plural=KUBEVIRT_PLURAL,
                        body=manifest,
                    )
                    break
                except ApiException as e:
                    if e.status == 409 and "is being deleted" in (e.body or ""):
                        if attempt < max_retries:
                            log.info(
                                "VM %s still being deleted, waiting... (attempt %d/%d)",
                                vm_name,
                                attempt + 1,
                                max_retries,
                            )
                            await asyncio.sleep(5)
                            continue
                        log.error(
                            "VM %s still being deleted after %d retries, giving up",
                            vm_name,
                            max_retries,
                        )
                    elif e.status == 409:
                        # Plain AlreadyExists: the name is agent-vm-<job_id>, so a
                        # live VM with this name IS this job's VM — a duplicate or
                        # racing create lost to one that already succeeded. Treat
                        # as idempotent success; propagating the 409 as a 'failed'
                        # status parked two healthy loop jobs (see knowledge-base/knowledge/issues/
                        # golden_image_cold_import_fails_inflight_vm_jobs.md §B).
                        log.info(
                            "VM %s already exists (job %s) — idempotent create",
                            vm_name,
                            job_id,
                        )
                        # A 409 response has no admitted object. Read the exact
                        # existing VM so its immutable metadata.uid crosses the
                        # transport boundary just like a successful create result.
                        admitted_vm = await asyncio.to_thread(
                            self.k8s_client.get_namespaced_custom_object,
                            group=KUBEVIRT_GROUP,
                            version=KUBEVIRT_VERSION,
                            namespace=VM_NAMESPACE,
                            plural=KUBEVIRT_PLURAL,
                            name=vm_name,
                        )
                        admitted_generation = _admitted_provision_generation(
                            admitted_vm
                        )
                        if generation is not None and admitted_generation != generation:
                            raise RuntimeError(
                                "existing VM belongs to another provision generation"
                            )
                        break
                    raise
        except Exception:
            if cloud_init_secret_created:
                await self._delete_cloud_init_secret(job_id)
            raise

        vm_uid = _admitted_vm_uid(admitted_vm, expected_name=vm_name)
        admitted_generation = _admitted_provision_generation(admitted_vm)
        if vm_uid is None or (
            generation is not None and admitted_generation != generation
        ):
            # CustomObjectsApi normally returns the admitted object on create.
            # A defensive GET covers proxies/older clients that omit the body;
            # failure remains fail-closed instead of publishing name-only
            # ownership as exact.
            admitted_vm = await asyncio.to_thread(
                self.k8s_client.get_namespaced_custom_object,
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=VM_NAMESPACE,
                plural=KUBEVIRT_PLURAL,
                name=vm_name,
            )
            vm_uid = _admitted_vm_uid(admitted_vm, expected_name=vm_name)
            admitted_generation = _admitted_provision_generation(admitted_vm)
        if vm_uid is None:
            raise RuntimeError("Kubernetes admitted VM response lacks metadata.uid")
        if generation is not None and admitted_generation != generation:
            raise RuntimeError(
                "Kubernetes admitted VM response has another provision generation"
            )
        if (
            rootdisk_reservation is not None
            and rootdisk_reservation.get("completed") is True
            and isinstance(rootdisk_reservation.get("carrier"), Mapping)
        ):
            completed_carrier = await self._refresh_workspace_cleanup_carrier(
                rootdisk_reservation["carrier"]
            )
            await self._delete_workspace_cleanup_carrier(completed_carrier)
        if (
            rootdisk_reservation is not None
            and rootdisk_reservation.get("completed") is not True
        ):
            carrier = rootdisk_reservation.get("carrier")
            if not isinstance(carrier, Mapping):
                raise RuntimeError("workspace cleanup carrier was not published")
            await self._complete_workspace_cleanup_reservation(
                carrier,
                outcome=str(rootdisk_reservation["outcome"]),
            )

        if workspace_storage is not None:
            self._retained_storage().verify_vm(admitted_vm, workspace_storage, job_id)
        if prepared is not None:
            from shared.workspace_preparation import PREPARATION_LABEL

            labels = admitted_vm.get("metadata", {}).get("labels", {})
            if labels.get(PREPARATION_LABEL) != prepared["preparation"]["uid"]:
                raise RuntimeError("Existing VM did not select the prepared artifact.")
        if cloud_init_user_data is not None:
            await self._patch_cloud_init_secret_owner(
                job_id=job_id,
                vm_name=vm_name,
                vm_uid=vm_uid,
            )

        if workspace_storage is not None:
            from shared.vm_workspace_storage import storage_name

            rootdisk_pvc_uid = await self._rootdisk_pvc_uid(
                storage_name(workspace_storage),
                owner_id=workspace_storage["owner_id"],
                owner_kind=workspace_storage["owner_kind"],
                wait=True,
            )
            await self._retained_storage().probe(workspace_storage)
        else:
            rootdisk_pvc_uid = await self._rootdisk_pvc_uid(
                _rootdisk_name(job_id),
                owner_id=job_id,
                owner_kind=owner_kind,
                wait=True,
            )

        log.info("VM created: %s (job %s)", vm_name, job_id)

        if preparation is not None and rootdisk_pvc_uid is not None:
            await self._workspace_preparation().mark_allocated(
                preparation,
                rootdisk=root_name,
                pvc_uid=rootdisk_pvc_uid,
            )

        # Best-effort GC of stale goldens from previous image digests. Never the
        # current image's golden, one a live VM references (in-flight clone), or
        # one younger than the min age. Fire-and-forget so it can't delay create.
        if VM_GOLDEN_IMAGE_ENABLED and VM_GOLDEN_GC_ENABLED and golden_name:
            asyncio.create_task(self._gc_goldens_safe(image))

        # Same fire-and-forget hook for orphaned rootdisks (opt-in — see
        # VM_ROOTDISK_GC_ENABLED).
        if VM_PERSISTENT_ROOTDISK and VM_ROOTDISK_GC_ENABLED:
            asyncio.create_task(self._gc_rootdisks_safe())

        result = {
            "job_id": job_id,
            "status": "created",
            "vm_name": vm_name,
            "vm_uid": vm_uid,
            "namespace": VM_NAMESPACE,
            "entity_type": owner_kind,
        }
        if prepared is not None:
            result["preparation"] = prepared["preparation"]
        elif preparation is not None:
            result["preparation"] = {
                "phase": "ExistingWorkspace",
                "allocationId": job_id,
            }
        if admitted_generation is not None:
            result["provision_generation"] = admitted_generation
        if ssh_host_key_fingerprint is not None:
            # This public pin rides the same authenticated generation merge as
            # vm_uid. Although the VM object is now admitted, readiness cannot
            # pass before the orchestrator durably applies this response: the
            # same-cluster prober fails closed while the pin is absent.
            result["ssh_host_key_fingerprint"] = ssh_host_key_fingerprint
        if rootdisk_pvc_uid is not None:
            result["rootdisk_pvc_uid"] = rootdisk_pvc_uid
        if workspace_storage is not None:
            result["workspace_storage"] = {
                **workspace_storage,
                "pvc_uid": rootdisk_pvc_uid,
            }
        return result

    async def _do_delete(
        self,
        job_id: str,
        owner_kind: str = "job",
        purge_disk: bool = True,
        provision_generation: str | None = None,
        expected_vm_uid: str | None = None,
        expected_rootdisk_pvc_uid: str | None = None,
        workspace_storage: dict | None = None,
        parent_cleanup: Mapping | None = None,
    ) -> dict:
        """Delete a KubeVirt VirtualMachine for a job.

        Create and delete share a bounded striped lock keyed by entity ID. In
        particular, a delete that observes a missing old VM cannot purge the
        reusable rootdisk name after a concurrent create has attached it.
        """
        async with self._workspace_lifecycle(job_id):
            return await self._do_delete_serialized(
                job_id,
                owner_kind=owner_kind,
                purge_disk=purge_disk,
                provision_generation=provision_generation,
                expected_vm_uid=expected_vm_uid,
                expected_rootdisk_pvc_uid=expected_rootdisk_pvc_uid,
                **(
                    {"parent_cleanup": parent_cleanup}
                    if parent_cleanup is not None
                    else {}
                ),
                **(
                    {"workspace_storage": workspace_storage}
                    if workspace_storage is not None
                    else {}
                ),
            )

    async def _do_delete_serialized(
        self,
        job_id: str,
        owner_kind: str = "job",
        purge_disk: bool = True,
        provision_generation: str | None = None,
        expected_vm_uid: str | None = None,
        expected_rootdisk_pvc_uid: str | None = None,
        workspace_storage: dict | None = None,
        parent_cleanup: Mapping | None = None,
    ) -> dict:
        """Delete while holding the reusable entity-name lifecycle lock.

        ``purge_disk`` says whether this delete is terminal for the entity.
        It defaults to True so an orchestrator that never sends the field gets
        exactly today's semantics (VM gone, disk gone, tailnet node gone) —
        only now the disk goes by explicit delete rather than ownerRef cascade,
        which also cleans up disks left behind by a flag flip.

        ``purge_disk=False`` means a recreate is expected (crash recovery, the
        reconciler giving up on a dirty VM, a session suspending). Two things
        are kept:

        - the rootdisk DataVolume — the recovery artifact the next create
          reattaches;
        - the Headscale node — the kept disk still holds /var/lib/tailscale
          state for it, so deleting the node would leave the recovered VM
          reconnecting as a dead one (D3).
        """
        from kubernetes.client.exceptions import ApiException

        vm_name = f"agent-vm-{job_id}"
        generation = _provision_generation(provision_generation)
        admitted_generation = None
        if LIFECYCLE_HMAC_SECRET is not None and generation is None:
            raise ValueError(
                "authenticated VM delete requires a canonical provision_generation"
            )
        log.info(
            "Deleting VM %s (job %s, rootdisk=%s)",
            vm_name,
            job_id,
            "purge" if purge_disk else "keep",
        )

        vm_already_absent = False
        admitted_vm_uid = None
        if generation is not None:
            try:
                current_vm = await asyncio.to_thread(
                    self.k8s_client.get_namespaced_custom_object,
                    group=KUBEVIRT_GROUP,
                    version=KUBEVIRT_VERSION,
                    namespace=VM_NAMESPACE,
                    plural=KUBEVIRT_PLURAL,
                    name=vm_name,
                )
            except ApiException as e:
                if e.status == 404:
                    vm_already_absent = True
                else:
                    raise
            else:
                admitted_generation = _admitted_provision_generation(current_vm)
                if admitted_generation != generation:
                    raise RuntimeError(
                        "refusing to delete a VM from another provision generation"
                    )
                admitted_vm_uid = _admitted_vm_uid(current_vm, expected_name=vm_name)
                if admitted_vm_uid is None:
                    raise RuntimeError(
                        "refusing to delete a VM without its admitted immutable UID"
                    )
                if expected_vm_uid is not None and admitted_vm_uid != expected_vm_uid:
                    raise RuntimeError("refusing to delete a superseded VM UID")

        rootdisk = _rootdisk_name(job_id)
        rootdisk_owner = job_id
        if workspace_storage is not None:
            from shared.vm_workspace_storage import storage_binding, storage_name

            workspace_storage = storage_binding(workspace_storage)
            if not vm_already_absent:
                self._retained_storage().verify_vm(
                    current_vm, workspace_storage, job_id
                )
            await self._retained_storage().probe(workspace_storage)
            rootdisk = storage_name(workspace_storage)
            rootdisk_owner = workspace_storage["owner_id"]
            purge_disk = False
        elif not vm_already_absent and generation is not None:
            from shared.vm_workspace_storage import WORKSPACE_LABEL

            if current_vm.get("metadata", {}).get("labels", {}).get(WORKSPACE_LABEL):
                raise RuntimeError("Retained VM deletion requires its storage binding.")
        if purge_disk and expected_rootdisk_pvc_uid is None:
            # A reusable DataVolume name is not immutable authority. Leave the
            # disk behind unless the caller supplies the captured PVC UID.
            log.warning(
                "rootdisk purge refused for %s: captured PVC UID is unavailable",
                rootdisk,
            )
            purge_disk = False
        captured_rootdisk_absent = False
        if expected_rootdisk_pvc_uid is not None:
            rootdisk_known, observed_rootdisk_uid = await self._rootdisk_pvc_probe(
                rootdisk,
                owner_id=rootdisk_owner,
                owner_kind=None,
                wait=False,
            )
            if not rootdisk_known:
                raise RuntimeError(
                    "captured rootdisk PVC identity is temporarily unknown"
                )
            if observed_rootdisk_uid is None:
                # Response-loss replay: only the conjunction of absent VM,
                # absent PVC, and absent DataVolume is exact completion.
                captured_rootdisk_absent = bool(
                    vm_already_absent and await self._get_dv(rootdisk) is None
                )
                if not captured_rootdisk_absent:
                    raise RuntimeError(
                        "captured rootdisk PVC is absent but teardown is incomplete"
                    )
            if observed_rootdisk_uid != expected_rootdisk_pvc_uid:
                if not captured_rootdisk_absent:
                    raise RuntimeError(
                        "refusing to delete a superseded rootdisk PVC UID"
                    )

        try:
            if vm_already_absent:
                raise ApiException(status=404)
            await asyncio.to_thread(
                self.k8s_client.delete_namespaced_custom_object,
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=VM_NAMESPACE,
                plural=KUBEVIRT_PLURAL,
                name=vm_name,
                **(
                    {
                        "body": {
                            "apiVersion": "v1",
                            "kind": "DeleteOptions",
                            "preconditions": {"uid": admitted_vm_uid},
                        }
                    }
                    if admitted_vm_uid is not None
                    else {}
                ),
            )
        except ApiException as e:
            if e.status == 404:
                log.info("VM %s already gone (404), treating as deleted", vm_name)
            else:
                raise

        await self._delete_cloud_init_secret(job_id)

        if purge_disk:
            # Non-fatal: a disk we failed to delete is a leak the GC backstop
            # catches, whereas raising here would strand the VM delete itself.
            try:
                if (
                    expected_rootdisk_pvc_uid is not None
                    and not captured_rootdisk_absent
                ):
                    await self._delete_captured_rootdisk(
                        rootdisk,
                        owner_kind=owner_kind,
                        owner_id=rootdisk_owner,
                        expected_pvc_uid=expected_rootdisk_pvc_uid,
                        **(
                            {
                                "parent_cleanup": parent_cleanup,
                                "provision_generation": generation,
                                "expected_vm_uid": expected_vm_uid,
                            }
                            if parent_cleanup is not None
                            else {}
                        ),
                    )
            except Exception as e:
                if expected_rootdisk_pvc_uid is not None:
                    raise
                log.warning("rootdisk purge failed for %s: %s", rootdisk, e)
            if self.headscale.is_available:
                await self.headscale.delete_node(job_id)
        else:
            log.info(
                "rootdisk KEPT: %s (job %s) — Headscale node retained so the "
                "recreated VM rejoins the tailnet as the same node",
                rootdisk,
                job_id,
            )

        log.info("VM deleted: %s (job %s)", vm_name, job_id)
        result = {
            "job_id": job_id,
            "status": "deleted",
            "vm_name": vm_name,
            "rootdisk": "purged" if purge_disk else "kept",
        }
        if admitted_generation is not None:
            result["provision_generation"] = admitted_generation
            result["generation_evidence"] = "admitted-vm-metadata"
        elif generation is not None:
            # Idempotent already-absent delete: useful only as a CAS fence for
            # diagnostics. It never accompanies VM/PVC identity fields.
            result["provision_generation"] = generation
            result["generation_evidence"] = "request-echo-vm-absent"
        return result

    async def _do_list(self, *, include_teardown_identity: bool = False) -> dict:
        """Enumerate the agent VMs this controller manages.

        Inventory source for the orchestrator's VM orphan sweep
        (``VMInstanceManager.reap_orphans``): the orchestrator's own view is
        derived from jobs/threads rows, so a VM whose row was deleted is
        invisible to it — only the controller can still see it. Names encode
        the owning entity (``agent-vm-<job-or-thread-uuid>``); golden
        DataVolumes are a different plural and never appear here, but the
        prefix is excluded anyway as defense-in-depth.
        """
        vms = await asyncio.to_thread(
            self.k8s_client.list_namespaced_custom_object,
            group=KUBEVIRT_GROUP,
            version=KUBEVIRT_VERSION,
            namespace=VM_NAMESPACE,
            plural=KUBEVIRT_PLURAL,
        )
        out = []
        for item in vms.get("items", []):
            meta = item.get("metadata", {})
            name = meta.get("name", "")
            if not name.startswith("agent-vm-") or name.startswith("agent-vm-golden-"):
                continue
            entity_id = name[len("agent-vm-") :]
            inventory = {
                "vm_name": name,
                "entity_id": entity_id,
                "created_at": meta.get("creationTimestamp"),
                "phase": item.get("status", {}).get("printableStatus", "Unknown"),
            }
            if include_teardown_identity:
                generation = _admitted_provision_generation(item)
                vm_uid = _admitted_vm_uid(item, expected_name=name)
                rootdisk_uid = await self._rootdisk_pvc_uid(
                    _rootdisk_name(entity_id),
                    owner_id=entity_id,
                    owner_kind=None,
                    wait=False,
                )
                if generation is not None:
                    inventory["provision_generation"] = generation
                if vm_uid is not None:
                    inventory["vm_uid"] = vm_uid
                if rootdisk_uid is not None:
                    inventory["rootdisk_pvc_uid"] = rootdisk_uid
            out.append(inventory)
        return {"vms": out}

    async def _do_status(
        self,
        job_id: str,
        provision_generation: str | None = None,
        *,
        exact_absence: bool = False,
        workspace_storage: dict | None = None,
    ) -> dict:
        """Query KubeVirt for a VM's current status."""
        from kubernetes.client.exceptions import ApiException

        vm_name = f"agent-vm-{job_id}"
        rootdisk, rootdisk_owner = _rootdisk_name(job_id), job_id
        if workspace_storage is not None:
            from shared.vm_workspace_storage import storage_binding, storage_name

            workspace_storage = storage_binding(workspace_storage)
            await self._retained_storage().probe(workspace_storage)
            rootdisk, rootdisk_owner = (
                storage_name(workspace_storage),
                workspace_storage["owner_id"],
            )
        try:
            vm = await asyncio.to_thread(
                self.k8s_client.get_namespaced_custom_object,
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=VM_NAMESPACE,
                plural=KUBEVIRT_PLURAL,
                name=vm_name,
            )
        except ApiException as exc:
            if exc.status != 404 or not exact_absence:
                raise
            rootdisk_known, rootdisk_uid = await self._rootdisk_pvc_probe(
                rootdisk,
                owner_id=rootdisk_owner,
                owner_kind=None,
                wait=False,
            )
            if (
                workspace_storage is not None
                and not await self._retained_storage().unused(workspace_storage)
            ):
                rootdisk_known = False
            return {
                "job_id": job_id,
                "status": "not_found",
                "provision_generation": _provision_generation(provision_generation),
                "rootdisk_identity_known": rootdisk_known,
                **(
                    {"rootdisk_pvc_uid": rootdisk_uid}
                    if rootdisk_uid is not None
                    else {}
                ),
            }
        if workspace_storage is not None:
            self._retained_storage().verify_vm(vm, workspace_storage, job_id)
        status = vm.get("status", {})
        metadata = vm.get("metadata", {})
        labels = metadata.get("labels", {}) if isinstance(metadata, Mapping) else {}
        vm_uid = _admitted_vm_uid(vm, expected_name=vm_name)
        generation = _admitted_provision_generation(vm)
        entity_type = (
            labels.get("srw.io/owner-kind") if isinstance(labels, Mapping) else None
        )
        conditions = status.get("conditions", [])
        ready = any(
            c.get("type") == "Ready" and c.get("status") == "True" for c in conditions
        )
        result = {
            "job_id": job_id,
            "vm_name": vm_name,
            "namespace": VM_NAMESPACE,
            "ready": ready,
            "phase": status.get("printableStatus", "Unknown"),
            "created": status.get("created", False),
        }
        if vm_uid is not None:
            result["vm_uid"] = vm_uid
        if generation is not None:
            result["provision_generation"] = generation
        if entity_type in _OWNER_KINDS:
            result["entity_type"] = entity_type

        try:
            vmi = await asyncio.to_thread(
                self.k8s_client.get_namespaced_custom_object,
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=VM_NAMESPACE,
                plural=KUBEVIRT_VMI_PLURAL,
                name=vm_name,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
            vmi_observed = False
            vmi = None
        else:
            vmi_observed = True
            vmi_status = vmi.get("status", {})
            result["vmi_phase"] = vmi_status.get("phase")
            interfaces = vmi_status.get("interfaces") or []
            pod_ip = interfaces[0].get("ipAddress") if interfaces else None
            active_pods = vmi_status.get("activePods") or {}
            active_pod_uid = next(iter(active_pods), None)
            if active_pod_uid is None and self.core_api is not None:
                pods = await asyncio.to_thread(
                    self.core_api.list_namespaced_pod,
                    namespace=VM_NAMESPACE,
                    label_selector=f"vm.kubevirt.io/name={vm_name}",
                )
                items = getattr(pods, "items", None) or []
                if isinstance(items, list) and items:
                    active_pod_uid = getattr(items[0].metadata, "uid", None)
            result["pod_ip"] = pod_ip
            result["active_pod_uid"] = active_pod_uid
        # Authenticated teardown callers may skip guest SSH only when this VM
        # generation has never created a VMI and none exists now.  KubeVirt's
        # durable `status.created` bit prevents a stopped/restarting guest from
        # being misclassified as never credential-capable.
        result["credential_runtime_started"] = bool(
            status.get("created") is True or vmi_observed
        )
        if exact_absence:
            rootdisk_known, rootdisk_pvc_uid = await self._rootdisk_pvc_probe(
                rootdisk,
                owner_id=rootdisk_owner,
                owner_kind=None,
                wait=False,
            )
            result["rootdisk_identity_known"] = rootdisk_known
        else:
            # Preserve the ordinary status response/call shape.  Only the
            # explicit teardown probe may publish an authenticated absence bit.
            rootdisk_pvc_uid = await self._rootdisk_pvc_uid(
                rootdisk,
                owner_id=rootdisk_owner,
                owner_kind=None,
                wait=False,
            )
        if rootdisk_pvc_uid is not None:
            result["rootdisk_pvc_uid"] = rootdisk_pvc_uid
        if not exact_absence:
            result.update(
                await self._provisioning_status_evidence(
                    vm=vm,
                    vmi=vmi,
                    owner_id=job_id,
                    owner_kind=entity_type,
                    generation=generation,
                    rootdisk_name=rootdisk,
                    rootdisk_owner_id=rootdisk_owner,
                    rootdisk_owner_kind=(
                        workspace_storage["owner_kind"]
                        if workspace_storage is not None
                        else entity_type
                    ),
                    expected_pvc_uid=rootdisk_pvc_uid,
                )
            )
        # Completed clone retention is independent of guest readiness and of
        # the adoption carrier's lifetime. Status polling rereads the durable
        # source intent; failed observation simply retains the source pin.
        creation_request = (
            vm.get("metadata", {})
            .get("annotations", {})
            .get("srw.io/vm-create-request-id")
        )
        creation = None
        if creation_request and not exact_absence:
            try:
                from vm_controller.creation_actuation import CreationActuator
                from vm_controller.creation_sources import source_manager

                creation = await CreationActuator(self).authority(
                    "inspect", request_id=creation_request
                )
                await source_manager(self, creation).release_completed(creation)
            except Exception:
                log.warning(
                    "completed clone source retention remains pending for VM %s",
                    vm_name,
                )
        prepared_annotation = (
            vm.get("metadata", {})
            .get("annotations", {})
            .get("srw.io/prepared-artifact")
        )
        if prepared_annotation and not exact_absence:
            prepared_metadata = json.loads(prepared_annotation)
            if creation_request:
                # A protocol VM's receipt must agree with its immutable source;
                # failed authority reads must not promote raw annotations.
                prepared_metadata = None
                if creation is not None:
                    from vm_controller.creation_preparation import (
                        observed_preparation_metadata,
                    )

                    prepared_metadata = observed_preparation_metadata(creation, vm)
            if prepared_metadata is not None:
                result["preparation"] = prepared_metadata
            if rootdisk_pvc_uid is not None and not creation_request:
                await self._workspace_preparation().observe_workspace(
                    "session" if entity_type == "thread" else "job",
                    job_id,
                    rootdisk=rootdisk,
                    pvc_uid=rootdisk_pvc_uid,
                )
        return result

    async def _provisioning_status_evidence(
        self,
        *,
        vm,
        vmi,
        owner_id,
        owner_kind,
        generation,
        rootdisk_name,
        rootdisk_owner_id,
        rootdisk_owner_kind,
        expected_pvc_uid,
    ) -> dict:
        """Read bounded phase evidence; an unavailable read is never absence."""
        from kubernetes.client.exceptions import ApiException
        from vm_controller.provisioning_observation import (
            build_provisioning_observation,
        )

        unknown = {"provisioning_reason": "vm_phase_unproven"}
        try:
            # Legacy/malformed identity has no phase authority and needs no new
            # Kubernetes probes. Do not synthesize a boot clock from VM age.
            UUID(str(_metadata_value(vm, "uid")))
            UUID(str(generation))
            UUID(str(owner_id))
            if owner_kind not in _OWNER_KINDS or self.core_api is None:
                return unknown
            # RetainedStorage currently renders a dataVolume-backed volume too.
            # Its binding is not evidence that no DV needs observation. A real
            # direct-PVC volume remains valid when this exact read returns 404.
            dv = await self._get_dv(rootdisk_name)
            try:
                pvc = await asyncio.to_thread(
                    self.core_api.read_namespaced_persistent_volume_claim,
                    name=rootdisk_name,
                    namespace=VM_NAMESPACE,
                )
            except ApiException as exc:
                if exc.status != 404:
                    raise
                pvc = None
            observation = build_provisioning_observation(
                vm=vm,
                vmi=vmi,
                datavolume=dv,
                pvc=pvc,
                namespace=VM_NAMESPACE,
                owner_kind=owner_kind,
                owner_id=owner_id,
                generation=generation,
                rootdisk_name=rootdisk_name,
                rootdisk_owner_kind=rootdisk_owner_kind,
                rootdisk_owner_id=rootdisk_owner_id,
            )
            if observation["rootdisk_pvc_uid"] != expected_pvc_uid:
                return unknown
            return {"provisioning": observation}
        except Exception:
            # The signed status remains usable for established consumers; only
            # phase-based actions are withheld. Never expose API bodies here.
            log.debug("VM provisioning phase evidence unavailable for %s", owner_id)
            return unknown

    async def _do_observe_workspace_recovery(
        self, captured_identity: Mapping[str, object]
    ) -> dict:
        """Read exact VM/VMI/launcher provenance without persisting status."""

        from kubernetes.client.exceptions import ApiException

        observed_at = datetime.now(timezone.utc)
        required = (
            "owner_kind",
            "owner_id",
            "provision_generation",
            "namespace",
            "vm_uid",
            "prior_vmi_uid",
            "prior_launcher_uid",
            "root_pvc_uid",
        )
        if any(
            not isinstance(captured_identity.get(key), (str, UUID))
            or not str(captured_identity.get(key))
            for key in required
        ):
            raise ValueError("captured workspace recovery identity is incomplete")
        owner_kind = str(captured_identity["owner_kind"])
        owner_id = str(captured_identity["owner_id"])
        generation = str(captured_identity["provision_generation"])
        vm_uid = str(captured_identity["vm_uid"])
        old_vmi_uid = str(captured_identity["prior_vmi_uid"])
        old_launcher_uid = str(captured_identity["prior_launcher_uid"])
        pvc_uid = str(captured_identity["root_pvc_uid"])
        if (
            owner_kind not in {"job", "thread"}
            or str(captured_identity["namespace"]) != VM_NAMESPACE
        ):
            raise ValueError("captured workspace recovery owner is invalid")
        for value in (
            owner_id,
            generation,
            vm_uid,
            old_vmi_uid,
            old_launcher_uid,
            pvc_uid,
        ):
            UUID(value)

        base: dict[str, object] = {
            "ready": False,
            "authenticated": False,
            "ambiguous": True,
            "owner_kind": owner_kind,
            "owner_id": owner_id,
            "provision_generation": generation,
            "vm_uid": vm_uid,
            "root_pvc_uid": pvc_uid,
            "prior_runtime": "unknown",
            "stop_evidence": "unknown",
            "observed_at": observed_at.isoformat(),
            "controller_identity": WORKSPACE_RECOVERY_CONTROLLER_IDENTITY,
            "successor": {},
            "network_qualification": {
                "legacy_cloud_init_cache_cleaned": False,
                "qualified": False,
            },
        }
        vm_name = f"agent-vm-{owner_id}"
        try:
            vm = await asyncio.to_thread(
                self.k8s_client.get_namespaced_custom_object,
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=VM_NAMESPACE,
                plural=KUBEVIRT_PLURAL,
                name=vm_name,
            )
        except ApiException as exc:
            if exc.status == 404:
                return base
            raise
        labels = _metadata_value(vm, "labels", {}) or {}
        annotations = _metadata_value(vm, "annotations", {}) or {}
        if (
            _admitted_vm_uid(vm, expected_name=vm_name) != vm_uid
            or not isinstance(labels, Mapping)
            or labels.get("srw.io/owner-kind") != owner_kind
            or labels.get("srw.io/owner-id") != owner_id
            or not isinstance(annotations, Mapping)
            or annotations.get("srw.io/provision-generation") != generation
        ):
            return base
        pvc_known, pvc = await self._rootdisk_pvc_by_uid(
            pvc_uid, owner_id=owner_id, owner_kind=owner_kind
        )
        rootdisk_name = _metadata_value(pvc, "name") if pvc is not None else None
        if (
            not pvc_known
            or pvc is None
            or _safe_uid(_metadata_value(pvc, "uid")) != pvc_uid
            or not isinstance(rootdisk_name, str)
            or not rootdisk_name
        ):
            return base
        dv = await self._get_dv(rootdisk_name)
        dv_metadata = (dv or {}).get("metadata") or {}
        dv_labels = dv_metadata.get("labels") or {}
        dv_uid = _safe_uid(dv_metadata.get("uid"))
        vm_template_spec = _object_value(
            _object_value(_object_value(vm, "spec", {}), "template", {}), "spec", {}
        )
        if (
            dv is None
            or dv_uid is None
            or dv_metadata.get("name") != rootdisk_name
            or not isinstance(dv_labels, Mapping)
            or dv_labels.get("srw.io/owner-kind") != owner_kind
            or dv_labels.get("srw.io/owner-id") != owner_id
            or not _owned_by(pvc, kind="DataVolume", uid=dv_uid)
            or _storage_volume_name(vm_template_spec, rootdisk_name) is None
        ):
            return base
        try:
            vmi = await asyncio.to_thread(
                self.k8s_client.get_namespaced_custom_object,
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=VM_NAMESPACE,
                plural=KUBEVIRT_VMI_PLURAL,
                name=vm_name,
            )
        except ApiException as exc:
            if exc.status == 404:
                return base
            raise
        current_vmi_uid = _safe_uid(_metadata_value(vmi, "uid"))
        if current_vmi_uid is None or not _owned_by(
            vmi, kind="VirtualMachine", uid=vm_uid
        ):
            return base
        vmi_spec = _object_value(vmi, "spec", {})
        if _storage_volume_name(vmi_spec, rootdisk_name) is None:
            return base
        vmi_status = _object_value(vmi, "status", {})
        migration_state = _object_value(vmi_status, "migrationState")
        if migration_state is None:
            migration_state = _object_value(vmi_status, "migration_state")
        migration_ambiguous = bool(migration_state)
        try:
            pod_response = await asyncio.to_thread(
                self.core_api.list_namespaced_pod,
                namespace=VM_NAMESPACE,
                label_selector=f"vm.kubevirt.io/name={vm_name}",
            )
        except Exception:
            return base
        pods = _object_value(pod_response, "items")
        if not isinstance(pods, list):
            return base
        launchers = [
            pod
            for pod in pods
            if _owned_by(pod, kind="VirtualMachineInstance", uid=current_vmi_uid)
        ]
        # Any same-VM Pod with an unproven owner is an additional plausible writer.
        if len(launchers) != 1 or len(pods) != 1 or migration_ambiguous:
            return {
                **base,
                "migration_ambiguous": migration_ambiguous,
                "launcher_uids": tuple(
                    str(_metadata_value(pod, "uid") or "") for pod in launchers
                ),
            }
        launcher = launchers[0]
        launcher_uid = _safe_uid(_metadata_value(launcher, "uid"))
        if launcher_uid is None:
            return base
        pod_status = _object_value(launcher, "status", {})
        pod_spec = _object_value(launcher, "spec", {})
        launcher_volume = _storage_volume_name(pod_spec, rootdisk_name)
        if launcher_volume is None or not _container_mounts_volume(
            pod_spec, launcher_volume
        ):
            return base
        status_reason = _object_value(pod_status, "reason")
        status_message = str(_object_value(pod_status, "message") or "")
        if status_reason in {"NodeLost", "ContainerStatusUnknown"} or (
            "ContainerStatusUnknown" in status_message
        ):
            return base
        node_name = _object_value(pod_spec, "nodeName")
        if node_name is None:
            node_name = _object_value(pod_spec, "node_name")
        node_uid = None
        if isinstance(node_name, str) and node_name:
            try:
                node = await asyncio.to_thread(self.core_api.read_node, name=node_name)
            except Exception:
                return base
            node_uid = _safe_uid(_metadata_value(node, "uid"))
        if node_uid is None:
            return base
        interfaces = _object_value(vmi_status, "interfaces", []) or []
        interface = interfaces[0] if len(interfaces) == 1 else {}
        pod_ip = _object_value(interface, "ipAddress") or _object_value(
            pod_status, "podIP"
        )
        if pod_ip is None:
            pod_ip = _object_value(pod_status, "pod_ip")
        mac = _object_value(interface, "mac")
        vm_status = _object_value(vm, "status", {})
        conditions = _object_value(vm_status, "conditions", []) or []
        vm_ready = any(
            _object_value(condition, "type") == "Ready"
            and _object_value(condition, "status") == "True"
            for condition in conditions
        )
        vmi_phase = _object_value(vmi_status, "phase")
        pod_phase = _object_value(pod_status, "phase")
        ready = bool(
            vm_ready
            and vmi_phase == "Running"
            and pod_phase == "Running"
            and isinstance(pod_ip, str)
            and pod_ip
        )
        result = {
            **base,
            "ambiguous": False,
            "migration_ambiguous": False,
            "launcher_uids": (launcher_uid,),
            "vmi_uid": current_vmi_uid,
            "node_uid": node_uid,
            "ready": ready,
            "successor": {
                "vmi_uid": current_vmi_uid,
                "launcher_uid": launcher_uid,
                "node_uid": node_uid,
                "pod_ip": pod_ip,
                "interface_mac": mac,
            },
            "network_qualification": {
                "interface_mac": mac,
                "address": pod_ip,
                "route": "unknown",
                "dns": "unknown",
                "cloud_init_instance_id": "unknown",
                "cloud_init_cache": "untouched",
                "legacy_cloud_init_cache_cleaned": False,
                "qualified": False,
            },
        }
        if current_vmi_uid == old_vmi_uid and launcher_uid == old_launcher_uid:
            terminal = _exact_terminal_container_evidence(launcher)
            if terminal is None:
                result["prior_runtime"] = "same_runtime" if ready else "running"
            else:
                evidence = {
                    "protocol_version": 1,
                    "vm_uid": vm_uid,
                    "vmi_uid": old_vmi_uid,
                    "launcher_uid": old_launcher_uid,
                    "container_id": next(
                        item["container_id"]
                        for item in terminal["containers"]
                        if item["name"] == "compute" and item["kind"] == "regular"
                    ),
                    "root_pvc_uid": pvc_uid,
                    "controller_identity": WORKSPACE_RECOVERY_CONTROLLER_IDENTITY,
                    "observed_at": observed_at.isoformat(),
                    **terminal,
                    "node_uid": node_uid,
                    "migration_ambiguous": False,
                }
                evidence["evidence_digest"] = (
                    "sha256:"
                    + hashlib.sha256(
                        json.dumps(
                            evidence,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=True,
                        ).encode("utf-8")
                    ).hexdigest()
                )
                result.update(
                    {
                        "prior_runtime": "stopped",
                        "stop_evidence": evidence,
                        "ready": False,
                    }
                )
        return result

    # =========================================================================
    # Golden-image cloning
    # (knowledge-base/knowledge/features/vm_golden_image_boot_acceleration.md)
    #
    # All K8s calls go through asyncio.to_thread — the kubernetes client is
    # synchronous and a blocking poll here would stall every other NATS/HTTP
    # handler on the event loop.
    # =========================================================================

    async def _rootdisk_pvc_uid(
        self,
        name: str,
        *,
        owner_id: str,
        owner_kind: str | None,
        wait: bool,
    ) -> str | None:
        """Read the immutable UID of the exact rootdisk PVC, fail-closed.

        CDI materializes a PVC asynchronously.  Create waits for a bounded
        number of reads; status checks make one read.  A missing/malformed or
        owner-mismatched claim never crosses the controller boundary as an
        authenticated identity.
        """

        _known, uid = await self._rootdisk_pvc_probe(
            name,
            owner_id=owner_id,
            owner_kind=owner_kind,
            wait=wait,
        )
        return uid

    async def _rootdisk_pvc_probe(
        self,
        name: str,
        *,
        owner_id: str,
        owner_kind: str | None,
        wait: bool,
    ) -> tuple[bool, str | None]:
        """Return ``(known, uid)`` to distinguish 404 from API ambiguity."""

        from kubernetes.client.exceptions import ApiException

        if self.core_api is None:
            log.warning(
                "rootdisk PVC identity unavailable for %s: CoreV1Api is not initialized",
                name,
            )
            return False, None
        attempts = max(1, VM_ROOTDISK_PVC_UID_ATTEMPTS if wait else 1)
        exact_absence = False
        for attempt in range(attempts):
            try:
                pvc = await asyncio.to_thread(
                    self.core_api.read_namespaced_persistent_volume_claim,
                    name=name,
                    namespace=VM_NAMESPACE,
                )
            except ApiException as exc:
                if exc.status != 404:
                    log.warning(
                        "rootdisk PVC identity read failed for %s: %s", name, exc
                    )
                    return False, None
                exact_absence = True
            except Exception as exc:
                log.warning("rootdisk PVC identity read failed for %s: %s", name, exc)
                return False, None
            else:
                uid = _admitted_pvc_uid(
                    pvc,
                    expected_name=name,
                    expected_owner_id=owner_id,
                    expected_owner_kind=owner_kind,
                )
                if uid is not None:
                    return True, uid
                return False, None
            if attempt + 1 < attempts and VM_ROOTDISK_PVC_UID_RETRY_SECONDS > 0:
                await asyncio.sleep(VM_ROOTDISK_PVC_UID_RETRY_SECONDS)
        if exact_absence:
            return True, None
        log.warning(
            "rootdisk PVC %s was not admitted with the expected immutable identity; "
            "storage attribution will remain unknown",
            name,
        )
        return False, None

    async def _rootdisk_pvc_probe_by_uid(
        self, pvc_uid: str, *, owner_id: str, owner_kind: str
    ) -> tuple[bool, str | None]:
        """Find one exact owner-labelled root PVC without guessing its name."""

        known, pvc = await self._rootdisk_pvc_by_uid(
            pvc_uid, owner_id=owner_id, owner_kind=owner_kind
        )
        return known, _safe_uid(_metadata_value(pvc, "uid")) if pvc else None

    async def _rootdisk_pvc_by_uid(
        self, pvc_uid: str, *, owner_id: str, owner_kind: str
    ) -> tuple[bool, object | None]:
        """Read the one exact owner-labelled PVC object by immutable UID."""

        if self.core_api is None:
            return False, None
        try:
            response = await asyncio.to_thread(
                self.core_api.list_namespaced_persistent_volume_claim,
                namespace=VM_NAMESPACE,
                label_selector=(
                    f"srw.io/owner-kind={owner_kind},srw.io/owner-id={owner_id}"
                ),
            )
        except Exception:
            return False, None
        items = _object_value(response, "items")
        if not isinstance(items, list):
            return False, None
        matches = [
            item
            for item in items
            if _metadata_value(item, "uid") == pvc_uid
            and _metadata_value(item, "deletionTimestamp") is None
            and _metadata_value(item, "deletion_timestamp") is None
        ]
        if len(matches) != 1:
            return True, None
        labels = _metadata_value(matches[0], "labels", {}) or {}
        if not isinstance(labels, Mapping) or (
            labels.get("srw.io/owner-kind") != owner_kind
            or labels.get("srw.io/owner-id") != owner_id
        ):
            return False, None
        return True, matches[0]

    async def _get_dv(self, name: str) -> dict | None:
        """GET a CDI DataVolume by name; None on 404."""
        from kubernetes.client.exceptions import ApiException

        try:
            return await asyncio.to_thread(
                self.k8s_client.get_namespaced_custom_object,
                group=CDI_GROUP,
                version=CDI_VERSION,
                namespace=VM_NAMESPACE,
                plural=CDI_PLURAL,
                name=name,
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    async def _exact_rootdisk_identity(
        self,
        name: str,
        *,
        owner_kind: str,
        owner_id: str,
        expected_pvc_uid: str | None = None,
        expected_dv_uid: str | None = None,
    ) -> tuple[dict, object, str, str]:
        """Bind one reusable name to its exact DV/PVC ownership chain."""

        dv = await self._get_dv(name)
        metadata = dv.get("metadata") if isinstance(dv, Mapping) else None
        labels = metadata.get("labels") if isinstance(metadata, Mapping) else None
        dv_uid = (
            _safe_uid(metadata.get("uid")) if isinstance(metadata, Mapping) else None
        )
        if (
            not isinstance(metadata, Mapping)
            or metadata.get("name") != name
            or metadata.get("deletionTimestamp")
            or not isinstance(labels, Mapping)
            or labels.get("srw.io/owner-kind") != owner_kind
            or labels.get("srw.io/owner-id") != owner_id
            or dv_uid is None
            or (expected_dv_uid is not None and dv_uid != expected_dv_uid)
        ):
            raise RuntimeError("rootdisk DataVolume identity is not exact")
        known, pvc_uid = await self._rootdisk_pvc_probe(
            name,
            owner_id=owner_id,
            owner_kind=owner_kind,
            wait=False,
        )
        if (
            not known
            or pvc_uid is None
            or (expected_pvc_uid is not None and pvc_uid != expected_pvc_uid)
        ):
            raise RuntimeError("rootdisk PVC identity is unknown")
        try:
            pvc = await asyncio.to_thread(
                self.core_api.read_namespaced_persistent_volume_claim,
                name=name,
                namespace=VM_NAMESPACE,
            )
        except Exception as exc:
            raise RuntimeError("rootdisk PVC identity is unknown") from exc
        pvc_labels = _metadata_value(pvc, "labels", {}) or {}
        if (
            _metadata_value(pvc, "name") != name
            or _metadata_value(pvc, "deletionTimestamp") is not None
            or _metadata_value(pvc, "deletion_timestamp") is not None
            or not isinstance(pvc_labels, Mapping)
            or pvc_labels.get("srw.io/owner-kind") != owner_kind
            or pvc_labels.get("srw.io/owner-id") != owner_id
            or not _owned_by(pvc, kind="DataVolume", uid=dv_uid)
        ):
            raise RuntimeError("rootdisk PVC ownership is not exact")
        return dv, pvc, dv_uid, pvc_uid

    async def _cleanup_carrier_pvc(self, name: str) -> object | None:
        from kubernetes.client.exceptions import ApiException

        try:
            return await asyncio.to_thread(
                self.core_api.read_namespaced_persistent_volume_claim,
                name=name,
                namespace=VM_NAMESPACE,
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise

    def _validate_cleanup_carrier_pvc(
        self, pvc: object, carrier: Mapping[str, object]
    ) -> None:
        labels = _metadata_value(pvc, "labels", {}) or {}
        if (
            _metadata_value(pvc, "name") != carrier["name"]
            or _metadata_value(pvc, "uid") != carrier["old_pvc_uid"]
            or not isinstance(labels, Mapping)
            or labels.get("srw.io/owner-kind") != carrier["owner_kind"]
            or labels.get("srw.io/owner-id") != carrier["owner_id"]
            or not _owned_by(
                pvc,
                kind="DataVolume",
                uid=str(carrier["old_dv_uid"]),
            )
        ):
            raise RuntimeError("workspace cleanup carrier PVC identity changed")

    async def _reconcile_cleanup_carrier_old_identity(
        self,
        carrier: Mapping[str, object],
        *,
        require_failed_dv: bool,
        before_delete=None,
    ) -> bool:
        """Delete only the carrier's old immutable DV/PVC identities."""

        name = str(carrier["name"])
        dv = await self._get_dv(name)
        if dv is not None:
            metadata = dv.get("metadata") if isinstance(dv, Mapping) else None
            labels = metadata.get("labels") if isinstance(metadata, Mapping) else None
            if (
                not isinstance(metadata, Mapping)
                or metadata.get("name") != name
                or _safe_uid(metadata.get("uid")) != carrier["old_dv_uid"]
                or not isinstance(labels, Mapping)
                or labels.get("srw.io/owner-kind") != carrier["owner_kind"]
                or labels.get("srw.io/owner-id") != carrier["owner_id"]
            ):
                raise RuntimeError("workspace cleanup carrier DataVolume UID drifted")
            if require_failed_dv and (
                ((dv.get("status") or {}).get("phase") != "Failed")
                and not metadata.get("deletionTimestamp")
            ):
                raise RuntimeError(
                    "workspace cleanup old DataVolume is no longer Failed"
                )
        pvc = await self._cleanup_carrier_pvc(name)
        if pvc is not None:
            self._validate_cleanup_carrier_pvc(pvc, carrier)

        if dv is not None:
            if before_delete is not None:
                await before_delete()
            await self._delete_dv(name, expected_uid=str(carrier["old_dv_uid"]))
        if pvc is not None:
            from kubernetes.client.exceptions import ApiException

            if before_delete is not None:
                await before_delete()
            try:
                await asyncio.to_thread(
                    self.core_api.delete_namespaced_persistent_volume_claim,
                    name=name,
                    namespace=VM_NAMESPACE,
                    body={
                        "apiVersion": "v1",
                        "kind": "DeleteOptions",
                        "preconditions": {"uid": str(carrier["old_pvc_uid"])},
                    },
                )
            except ApiException as exc:
                if exc.status != 404:
                    raise

        remaining_dv = await self._get_dv(name)
        if remaining_dv is not None:
            remaining_uid = _safe_uid((remaining_dv.get("metadata") or {}).get("uid"))
            if remaining_uid != carrier["old_dv_uid"]:
                raise RuntimeError("workspace cleanup carrier DataVolume UID drifted")
            return False
        remaining_pvc = await self._cleanup_carrier_pvc(name)
        if remaining_pvc is not None:
            if _metadata_value(remaining_pvc, "uid") != carrier["old_pvc_uid"]:
                raise RuntimeError("workspace cleanup carrier PVC UID drifted")
            return False
        return True

    async def _reconcile_workspace_cleanup_carrier(
        self, carrier: Mapping[str, object]
    ) -> bool:
        if carrier["source"] == "controller_vm_create":
            from vm_controller.creation_actuation import reconcile_creation_carrier

            return await reconcile_creation_carrier(self, carrier)
        async with self._workspace_lifecycle(str(carrier["owner_id"])):
            carrier = await self._refresh_workspace_cleanup_carrier(carrier)
            resumed = await self._resume_workspace_cleanup_reservation(carrier)
            if resumed.get("creation_disposition") is not None:
                from vm_controller.creation_disposition import CreationDisposer

                # A child recovered independently must use the same consumer
                # fences as its parent, never the legacy unconditional delete.
                await CreationDisposer(self).run(resumed["creation_disposition"])
                return False
            if resumed.get("allowed") is not True:
                if resumed.get("completed_outcome") == carrier["outcome"]:
                    if carrier["source"] == "controller_failed_dv_recreate":
                        # Completion proves a VM was admitted, but the successor
                        # DV still carries this carrier UID/nonce. Let the create
                        # replay validate that exact DV and VM before removing
                        # the only durable bridge back to the completed intent.
                        return False
                    await self._delete_workspace_cleanup_carrier(carrier)
                    return True
                raise RuntimeError("workspace cleanup carrier DB intent changed")
            source = str(carrier["source"])
            if source == "controller_rootdisk_delete":
                absent = await self._reconcile_cleanup_carrier_old_identity(
                    carrier, require_failed_dv=False
                )
                if absent:
                    await self._complete_workspace_cleanup_reservation(
                        carrier, outcome="deleted"
                    )
                    return True
            elif source == "controller_failed_dv_recreate":
                if carrier["successor_dv_uid"] and carrier["successor_pvc_uid"]:
                    successor = await self._get_dv(str(carrier["name"]))
                    annotations = ((successor or {}).get("metadata") or {}).get(
                        "annotations"
                    ) or {}
                    if (
                        not isinstance(annotations, Mapping)
                        or annotations.get("srw.io/cleanup-carrier-uid")
                        != carrier["carrier_uid"]
                        or annotations.get(_CLEANUP_ANNOTATIONS["nonce"])
                        != carrier["nonce"]
                        or annotations.get(_PROVISION_GENERATION_ANNOTATION)
                        != carrier["provision_generation"]
                    ):
                        raise RuntimeError(
                            "workspace cleanup successor metadata changed"
                        )
                    await self._exact_rootdisk_identity(
                        str(carrier["name"]),
                        owner_kind=str(carrier["owner_kind"]),
                        owner_id=str(carrier["owner_id"]),
                        expected_dv_uid=str(carrier["successor_dv_uid"]),
                        expected_pvc_uid=str(carrier["successor_pvc_uid"]),
                    )
                    return False
                await self._reconcile_cleanup_carrier_old_identity(
                    carrier, require_failed_dv=True
                )
            return False

    async def _reconcile_workspace_cleanup_carriers(self) -> None:
        for carrier in await self._list_workspace_cleanup_carriers():
            try:
                await self._reconcile_workspace_cleanup_carrier(carrier)
            except Exception as exc:
                log.warning(
                    "workspace cleanup carrier %s reconciliation failed: %s",
                    carrier["carrier_name"],
                    exc,
                )

    async def _bind_workspace_cleanup_successor(
        self,
        carrier: Mapping[str, object],
        *,
        dv_uid: str,
        pvc_uid: str,
    ) -> dict[str, object]:
        carrier = await self._refresh_workspace_cleanup_carrier(carrier)
        if carrier["successor_dv_uid"] or carrier["successor_pvc_uid"]:
            if (
                carrier["successor_dv_uid"] != dv_uid
                or carrier["successor_pvc_uid"] != pvc_uid
            ):
                raise RuntimeError("workspace cleanup successor identity changed")
            return carrier
        annotations = {
            _CLEANUP_ANNOTATIONS[key]: str(carrier[key])
            for key in _CLEANUP_ANNOTATIONS
            if carrier.get(key)
        }
        annotations[_CLEANUP_ANNOTATIONS["successor_dv_uid"]] = dv_uid
        annotations[_CLEANUP_ANNOTATIONS["successor_pvc_uid"]] = pvc_uid
        annotations["srw.io/cleanup-carrier-signature"] = (
            self._workspace_cleanup_carrier_signature(
                name=str(carrier["carrier_name"]),
                uid=str(carrier["carrier_uid"]),
                values={
                    **carrier,
                    "successor_dv_uid": dv_uid,
                    "successor_pvc_uid": pvc_uid,
                },
            )
        )
        body = {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": str(carrier["carrier_name"]),
                "namespace": VM_NAMESPACE,
                "uid": str(carrier["carrier_uid"]),
                "resourceVersion": str(carrier["carrier_resource_version"]),
                "labels": {WORKSPACE_CLEANUP_CARRIER_LABEL: "true"},
                "annotations": annotations,
            },
            "spec": {"holderIdentity": str(carrier["admission_id"])},
        }
        lease = await asyncio.to_thread(
            self.coordination_api.replace_namespaced_lease,
            name=str(carrier["carrier_name"]),
            namespace=VM_NAMESPACE,
            body=body,
        )
        bound = self._parse_workspace_cleanup_carrier(lease)
        if (
            bound["carrier_uid"] != carrier["carrier_uid"]
            or bound["successor_dv_uid"] != dv_uid
            or bound["successor_pvc_uid"] != pvc_uid
        ):
            raise RuntimeError(
                "workspace cleanup successor binding was not acknowledged"
            )
        return bound

    async def _delete_captured_rootdisk(
        self,
        name: str,
        *,
        owner_kind: str,
        owner_id: str,
        expected_pvc_uid: str,
        parent_cleanup: Mapping | None = None,
        provision_generation: str | None = None,
        expected_vm_uid: str | None = None,
        _serialized: bool = False,
    ) -> None:
        """Purge only the rootdisk whose immutable PVC UID was captured."""

        if not _serialized:
            async with self._workspace_lifecycle(owner_id):
                return await self._delete_captured_rootdisk(
                    name,
                    owner_kind=owner_kind,
                    owner_id=owner_id,
                    expected_pvc_uid=expected_pvc_uid,
                    parent_cleanup=parent_cleanup,
                    provision_generation=provision_generation,
                    expected_vm_uid=expected_vm_uid,
                    _serialized=True,
                )

        if self.core_api is None:
            raise RuntimeError("CoreV1Api is unavailable for captured rootdisk delete")
        carried = await self._find_workspace_cleanup_carrier(
            owner_kind=owner_kind,
            owner_id=owner_id,
            source="controller_rootdisk_delete",
            name=name,
        )
        if carried is not None:
            if carried["old_pvc_uid"] != expected_pvc_uid:
                raise RuntimeError("captured rootdisk cleanup carrier identity changed")
            if await self._reconcile_workspace_cleanup_carrier(carried):
                return
            raise RuntimeError("captured rootdisk cleanup is still reconciling")
        dv, _pvc, dv_uid, observed_uid = await self._exact_rootdisk_identity(
            name,
            owner_kind=owner_kind,
            owner_id=owner_id,
            expected_pvc_uid=expected_pvc_uid,
        )
        if self._pvc_is_recovery_pinned(
            await self._active_recovery_pins(), observed_uid
        ):
            raise RuntimeError("captured rootdisk is pinned for workspace recovery")

        metadata = dv.get("metadata") or {}
        generation = str(
            (metadata.get("annotations") or {}).get(_PROVISION_GENERATION_ANNOTATION)
            or "unknown"
        )
        reservation = await self._acquire_workspace_cleanup_reservation(
            source="controller_rootdisk_delete",
            owner_kind=owner_kind,
            owner_id=owner_id,
            pvc_uid=observed_uid,
            dv_uid=dv_uid,
            provision_generation=generation,
            **(
                {
                    "parent_cleanup": parent_cleanup,
                    "parent_provision_generation": provision_generation,
                    "expected_vm_uid": expected_vm_uid,
                }
                if parent_cleanup is not None
                else {}
            ),
        )
        if reservation.get("completed_outcome") == "deleted":
            return
        carrier = reservation.get("carrier")
        if not isinstance(carrier, Mapping):
            raise RuntimeError("workspace cleanup carrier was not published")

        # The database reservation was acquired after the first Kubernetes
        # read. Rebind the exact chain before crossing the delete boundary.
        await self._exact_rootdisk_identity(
            name,
            owner_kind=owner_kind,
            owner_id=owner_id,
            expected_pvc_uid=expected_pvc_uid,
            expected_dv_uid=dv_uid,
        )
        await self._delete_dv(name, expected_uid=dv_uid)

        from kubernetes.client.exceptions import ApiException

        try:
            await asyncio.to_thread(
                self.core_api.delete_namespaced_persistent_volume_claim,
                name=name,
                namespace=VM_NAMESPACE,
                body={
                    "apiVersion": "v1",
                    "kind": "DeleteOptions",
                    "preconditions": {"uid": expected_pvc_uid},
                },
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
        if not await self._reconcile_cleanup_carrier_old_identity(
            carrier, require_failed_dv=False
        ):
            raise RuntimeError("captured rootdisk cleanup is still reconciling")
        await self._complete_workspace_cleanup_reservation(carrier, outcome="deleted")

    async def _delete_dv(self, name: str, *, expected_uid: str | None = None) -> None:
        """DELETE a CDI DataVolume (its PVC cascades); 404 is success."""
        if name.startswith("agent-vm-golden-"):
            from vm_controller.creation_sources import GoldenSources

            return await GoldenSources(self).delete(name, expected_uid=expected_uid)
        from kubernetes.client.exceptions import ApiException

        try:
            await asyncio.to_thread(
                self.k8s_client.delete_namespaced_custom_object,
                group=CDI_GROUP,
                version=CDI_VERSION,
                namespace=VM_NAMESPACE,
                plural=CDI_PLURAL,
                name=name,
                **(
                    {
                        "body": {
                            "apiVersion": "v1",
                            "kind": "DeleteOptions",
                            "preconditions": {"uid": expected_uid},
                        }
                    }
                    if expected_uid is not None
                    else {}
                ),
            )
        except ApiException as e:
            if e.status != 404:
                raise

    async def _wait_dv_succeeded(self, name: str) -> bool:
        """Poll a DataVolume until phase Succeeded (True); Failed/timeout → False."""
        deadline = asyncio.get_running_loop().time() + VM_GOLDEN_POLL_TIMEOUT
        while asyncio.get_running_loop().time() < deadline:
            dv = await self._get_dv(name)
            phase = ((dv or {}).get("status") or {}).get("phase", "")
            if phase == "Succeeded":
                return True
            if phase == "Failed":
                return False
            await asyncio.sleep(5)
        log.warning(
            "golden %s did not reach Succeeded within %ds", name, VM_GOLDEN_POLL_TIMEOUT
        )
        return False

    def _golden_dv_manifest(self, name: str, image: str) -> dict:
        """Build a standalone golden DataVolume that imports ``image`` once.

        Uses the ``spec.storage`` form WITH accessModes + volumeMode named, the
        same shape as the rootdisk template. The old ``spec.pvc`` form requested
        the literal size, which bypasses CDI's ``filesystemOverhead`` inflation:
        on any CSI that honours the requested size, a 20Gi *filesystem* holds
        ~19.5 GiB, so a 20 GiB virtual image failed with "DataVolume too small
        to contain image" and the golden never succeeded (single_cluster_vm_
        deployment.md §11.16). It only ever worked on local-path, whose importer
        writes into a host directory and reports the host's free space.
        ``spec.storage`` inflates the request by the profile's overhead (6 %
        here), exactly as §4.7 assumes; naming the modes explicitly keeps it
        valid on local-path's empty StorageProfile, where only the *size-only*
        inference form is rejected. ``bind.immediate`` forces populate on
        WaitForFirstConsumer storage (the golden is never VM-mounted, so nothing
        else would trigger it); ``deleteAfterCompletion:false`` keeps the DV
        object as our reuse handle after CDI would otherwise GC it post-import.
        """
        return {
            "apiVersion": f"{CDI_GROUP}/{CDI_VERSION}",
            "kind": "DataVolume",
            "metadata": {
                "name": name,
                "namespace": VM_NAMESPACE,
                "labels": {
                    "srw.io/golden-image": name.rsplit("-", 1)[-1],
                    "srw.io/vm-image": _label_safe(image),
                },
                "annotations": {
                    "cdi.kubevirt.io/storage.bind.immediate.requested": "true",
                    "cdi.kubevirt.io/storage.deleteAfterCompletion": "false",
                    "srw.io/vm-image-ref": image,
                },
            },
            "spec": {
                "source": {"registry": {"url": f"docker://{image}"}},
                "storage": {
                    "accessModes": ["ReadWriteOnce"],
                    "volumeMode": "Filesystem",
                    "storageClassName": VM_STORAGE_CLASS,
                    "resources": {"requests": {"storage": VM_GOLDEN_DISK_SIZE}},
                },
            },
        }

    async def _ensure_golden(self, image: str) -> str | None:
        """Ensure a Succeeded golden DataVolume for ``image``; return its name,
        or None so the caller falls back to the legacy registry source.

        Idempotent + concurrency-safe: the Kubernetes create-409 is the lock, so
        parallel creates converge on one import. Called on EVERY create path,
        including crash-recovery re-dispatch.
        """
        from kubernetes.client.exceptions import ApiException

        name = _golden_name(image)
        dv = await self._get_dv(name)
        phase = ((dv or {}).get("status") or {}).get("phase", "")

        if dv and phase == "Succeeded":
            return name
        if dv and phase == "Failed":
            log.warning("golden %s is Failed — recreating", name)
            await self._delete_dv(name)
            dv = None
        if dv is not None:
            # Importing / Pending / CloneScheduled / "" — already being built.
            return name if await self._wait_dv_succeeded(name) else None

        # Absent → create (409 = another create won the race; both then wait).
        try:
            await asyncio.to_thread(
                self.k8s_client.create_namespaced_custom_object,
                group=CDI_GROUP,
                version=CDI_VERSION,
                namespace=VM_NAMESPACE,
                plural=CDI_PLURAL,
                body=self._golden_dv_manifest(name, image),
            )
            log.info("golden %s: importing %s (once)", name, image)
        except ApiException as e:
            if e.status != 409:
                log.warning("golden %s create failed: %s", name, e)
                return None
        return name if await self._wait_dv_succeeded(name) else None

    async def _golden_state_nowait(self, image: str) -> tuple[str | None, dict | None]:
        """Non-blocking golden check for the VM create path.

        Unlike ``_ensure_golden`` (kept for the pre-warm background task), this
        never sleeps waiting for CDI: a create handler that blocks here for the
        duration of a cold import (~30 min) outlives every orchestrator
        provisioning budget and races later create attempts into 409
        AlreadyExists collisions.

        Returns ``(golden_name, waiting_payload)``:
          (name, None)    → golden Succeeded; clone the rootdisk from it.
          (None, payload) → golden import in flight (payload has ``golden`` /
                            ``golden_phase`` / ``golden_progress``); the caller
                            must NOT create the VM — the orchestrator polls
                            create until the golden is ready.
          (None, None)    → golden infra unusable (create rejected) → caller
                            falls back to the legacy registry source.
        """
        from kubernetes.client.exceptions import ApiException

        name = _golden_name(image)
        dv = await self._get_dv(name)
        status = (dv or {}).get("status") or {}
        phase = status.get("phase", "")

        if dv and phase == "Succeeded":
            return name, None
        if dv and phase == "Failed":
            log.warning("golden %s is Failed — recreating", name)
            await self._delete_dv(name)
            dv = None
        if dv is not None:
            # Importing / Pending / CloneScheduled / "" — being built.
            return None, {
                "golden": name,
                "golden_phase": phase or "Pending",
                "golden_progress": status.get("progress") or "",
            }

        # Absent → create (409 = another create won the race; both then poll).
        try:
            await asyncio.to_thread(
                self.k8s_client.create_namespaced_custom_object,
                group=CDI_GROUP,
                version=CDI_VERSION,
                namespace=VM_NAMESPACE,
                plural=CDI_PLURAL,
                body=self._golden_dv_manifest(name, image),
            )
            log.info("golden %s: importing %s (once)", name, image)
        except ApiException as e:
            if e.status != 409:
                log.warning("golden %s create failed: %s", name, e)
                return None, None
        return None, {
            "golden": name,
            "golden_phase": "Pending",
            "golden_progress": "",
        }

    def _apply_clone_source(self, manifest: dict, golden_name: str) -> None:
        """Mutate a rendered VM manifest so its rootdisk clones the golden PVC
        instead of importing from the registry. Same namespace → no ``namespace``
        key (avoids cross-namespace clone RBAC). Keeps the clone target
        WaitForFirstConsumer (NO bind.immediate) so it binds on the VM's node.
        """
        dv_spec = manifest["spec"]["dataVolumeTemplates"][0]["spec"]
        dv_spec["source"] = {"pvc": {"name": golden_name}}
        # Clone target must match the golden's Filesystem volumeMode.
        dv_spec.setdefault("storage", {})["volumeMode"] = "Filesystem"

    async def _ensure_rootdisk(
        self,
        manifest: dict,
        job_id: str,
        *,
        owner_kind: str = "job",
        provision_generation: str = "legacy",
        recovery_pins: tuple[dict[str, str], ...] | list[dict[str, str]] | None = None,
    ) -> str:
        """Detach the rootdisk from the VM object, creating it if absent.

        Pops ``spec.dataVolumeTemplates`` from the rendered manifest and
        ensures a standalone DataVolume with the same name and the same spec.
        ``volumes[].dataVolume.name`` refers to the disk *by name*, so that
        section needs no change: the VM binds to the standalone disk instead of
        a templated, owner-referenced one, and the disk survives VM deletion.

        Returns the rootdisk name. Raises if the disk cannot be ensured —
        there is deliberately no fallback to the templated form, which would
        silently reintroduce the cascade-delete this exists to remove.
        """
        from kubernetes.client.exceptions import ApiException

        dvts = manifest.get("spec", {}).pop("dataVolumeTemplates", None)
        if not dvts:
            raise RuntimeError(
                f"VM_PERSISTENT_ROOTDISK is on but the rendered manifest for "
                f"job {job_id} has no dataVolumeTemplates — refusing to create "
                f"a VM whose rootdisk is undefined"
            )
        dvt = dvts[0]
        name = (dvt.get("metadata") or {}).get("name") or _rootdisk_name(job_id)
        # The manifest carries the Tailscale auth key, the SSH key and the VM
        # auth token, so nothing read back out of it may reach a log record.
        # The template renders exactly this name, so it is the same string.
        log_name = _rootdisk_name(job_id)

        # A templated DataVolume may omit spec.source.pvc.namespace — CDI
        # defaults it from the owning VM. A standalone one may not: the webhook
        # rejects it with 422 "spec.source.pvc.namespace: Required value", which
        # failed every VM create the moment this flag was first flipped.
        # _apply_clone_source leaves it out deliberately (its docstring: same
        # namespace, no cross-namespace clone RBAC) and that stays true — the
        # value is simply now stated rather than inferred.
        source_pvc = ((dvt.get("spec") or {}).get("source") or {}).get("pvc")
        if isinstance(source_pvc, dict) and not source_pvc.get("namespace"):
            source_pvc["namespace"] = VM_NAMESPACE

        async def reserve_existing(
            expected_dv_uid: str | None = None,
            expected_pvc_uid: str | None = None,
            *,
            source: str,
            outcome: str,
        ) -> tuple[dict, str, str, dict[str, object]]:
            (
                exact_dv,
                _pvc,
                exact_dv_uid,
                exact_pvc_uid,
            ) = await self._exact_rootdisk_identity(
                name,
                owner_kind=owner_kind,
                owner_id=job_id,
                expected_dv_uid=expected_dv_uid,
                expected_pvc_uid=expected_pvc_uid,
            )
            pins = (
                tuple(recovery_pins)
                if recovery_pins is not None
                else await self._active_recovery_pins()
            )
            if self._pvc_is_recovery_pinned(pins, exact_pvc_uid):
                raise RuntimeError("rootdisk is pinned for workspace recovery")
            annotations = (exact_dv.get("metadata") or {}).get("annotations") or {}
            if isinstance(annotations, Mapping) and any(
                annotations.get(key)
                for key in (
                    _CLEANUP_ANNOTATIONS["admission_id"],
                    "srw.io/cleanup-carrier-uid",
                    _CLEANUP_ANNOTATIONS["nonce"],
                )
            ):
                raise RuntimeError(
                    "rootdisk carries cleanup metadata without an exact carrier"
                )
            reservation = await self._acquire_workspace_cleanup_reservation(
                source=source,
                owner_kind=owner_kind,
                owner_id=job_id,
                pvc_uid=exact_pvc_uid,
                dv_uid=exact_dv_uid,
                provision_generation=provision_generation,
            )
            if reservation.get("completed_outcome") not in {None, outcome}:
                raise RuntimeError("rootdisk reservation outcome changed")
            return exact_dv, exact_dv_uid, exact_pvc_uid, reservation

        # Settle an interrupted deletion before any same-name successor exists,
        # even when periodic rootdisk garbage collection is disabled.
        for carrier in await self._list_workspace_cleanup_carriers():
            if (
                carrier["owner_kind"] == owner_kind
                and carrier["owner_id"] == job_id
                and carrier["source"] == "controller_rootdisk_delete"
            ):
                if not await self._reconcile_workspace_cleanup_carrier(carrier):
                    raise RuntimeError("rootdisk deletion is still reconciling")
        dv = await self._get_dv(name)
        phase = ((dv or {}).get("status") or {}).get("phase", "")
        dv_annotations = ((dv or {}).get("metadata") or {}).get("annotations") or {}
        replacement_carrier: dict[str, object] | None = None
        if (
            dv is None
            or phase == "Failed"
            or (
                isinstance(dv_annotations, Mapping)
                and (
                    dv_annotations.get("srw.io/cleanup-carrier-uid")
                    or dv_annotations.get(_CLEANUP_ANNOTATIONS["admission_id"])
                )
            )
        ):
            replacement_carrier = await self._find_workspace_cleanup_carrier(
                owner_kind=owner_kind,
                owner_id=job_id,
                source="controller_failed_dv_recreate",
                name=name,
            )
        if replacement_carrier is not None:
            if replacement_carrier["provision_generation"] != provision_generation:
                raise RuntimeError(
                    "failed rootdisk recreation carrier generation changed"
                )
            resumed = await self._resume_workspace_cleanup_reservation(
                replacement_carrier
            )
            recreation_completed = resumed.get("completed_outcome") == "recreated"
            if resumed.get("allowed") is not True and not recreation_completed:
                raise RuntimeError("failed rootdisk recreation carrier is unavailable")
            successor_dv_uid = str(replacement_carrier.get("successor_dv_uid") or "")
            successor_pvc_uid = str(replacement_carrier.get("successor_pvc_uid") or "")
            if recreation_completed and (not successor_dv_uid or not successor_pvc_uid):
                raise RuntimeError(
                    "completed rootdisk recreation has no successor binding"
                )
            if dv is None:
                if successor_dv_uid or successor_pvc_uid:
                    raise RuntimeError(
                        "failed rootdisk recreation successor disappeared"
                    )
                old_identity_absent = (
                    await self._reconcile_cleanup_carrier_old_identity(
                        replacement_carrier, require_failed_dv=True
                    )
                )
                if not old_identity_absent:
                    raise RuntimeError(
                        "failed rootdisk recreation old identity is still deleting"
                    )
            else:
                observed_dv_uid = _safe_uid(((dv.get("metadata") or {}).get("uid")))
                if observed_dv_uid == replacement_carrier["old_dv_uid"]:
                    if recreation_completed:
                        raise RuntimeError(
                            "completed rootdisk recreation still exposes old identity"
                        )
                    if phase != "Failed":
                        raise RuntimeError(
                            "failed rootdisk recreation old identity changed phase"
                        )
                    old_identity_absent = (
                        await self._reconcile_cleanup_carrier_old_identity(
                            replacement_carrier, require_failed_dv=True
                        )
                    )
                    if not old_identity_absent:
                        raise RuntimeError(
                            "failed rootdisk recreation old identity is still deleting"
                        )
                    dv = None
                else:
                    annotations = (dv.get("metadata") or {}).get("annotations") or {}
                    if (
                        not isinstance(annotations, Mapping)
                        or annotations.get("srw.io/cleanup-carrier-uid")
                        != replacement_carrier["carrier_uid"]
                        or annotations.get(_CLEANUP_ANNOTATIONS["nonce"])
                        != replacement_carrier["nonce"]
                        or annotations.get(_PROVISION_GENERATION_ANNOTATION)
                        != provision_generation
                    ):
                        raise RuntimeError(
                            "failed rootdisk recreation successor metadata changed"
                        )
                    (
                        _,
                        _,
                        observed_dv_uid,
                        observed_pvc_uid,
                    ) = await self._exact_rootdisk_identity(
                        name,
                        owner_kind=owner_kind,
                        owner_id=job_id,
                        expected_dv_uid=(successor_dv_uid or None),
                        expected_pvc_uid=(successor_pvc_uid or None),
                    )
                    if self._pvc_is_recovery_pinned(
                        await self._active_recovery_pins(), observed_pvc_uid
                    ):
                        raise RuntimeError("rootdisk is pinned for workspace recovery")
                    replacement_carrier = await self._bind_workspace_cleanup_successor(
                        replacement_carrier,
                        dv_uid=observed_dv_uid,
                        pvc_uid=observed_pvc_uid,
                    )
                    manifest["_srwRootdiskReservation"] = {
                        "name": name,
                        "owner_kind": owner_kind,
                        "owner_id": job_id,
                        "dv_uid": observed_dv_uid,
                        "pvc_uid": observed_pvc_uid,
                        "admission_id": replacement_carrier["admission_id"],
                        "completed": recreation_completed,
                        "outcome": "recreated",
                        "carrier": replacement_carrier,
                    }
                    return name
            dv = None
            manifest["_srwRootdiskReservation"] = {
                "name": name,
                "owner_kind": owner_kind,
                "owner_id": job_id,
                "dv_uid": replacement_carrier["old_dv_uid"],
                "pvc_uid": replacement_carrier["old_pvc_uid"],
                "admission_id": replacement_carrier["admission_id"],
                "completed": False,
                "outcome": "recreated",
                "replacement": True,
                "carrier": replacement_carrier,
            }
        if dv and phase == "Succeeded":
            dv, dv_uid, pvc_uid, reservation = await reserve_existing(
                source="controller_rootdisk_adopt", outcome="adopted"
            )
            manifest["_srwRootdiskReservation"] = {
                "name": name,
                "owner_kind": owner_kind,
                "owner_id": job_id,
                "dv_uid": dv_uid,
                "pvc_uid": pvc_uid,
                "admission_id": reservation["admission_id"],
                "completed": reservation.get("completed_outcome") == "adopted",
                "outcome": reservation.get("_srw_outcome", "adopted"),
                "carrier": reservation.get("carrier"),
            }
            log.info("rootdisk reattach: %s (job %s)", log_name, job_id)
            return name
        if dv and phase == "Failed":
            async with self._workspace_lifecycle(job_id):
                # The phase read that selected this branch predates lifecycle
                # admission. Re-read every identity under the shared boundary.
                dv = await self._get_dv(name)
                phase = ((dv or {}).get("status") or {}).get("phase", "")
                if dv and phase == "Succeeded":
                    dv, dv_uid, pvc_uid, reservation = await reserve_existing(
                        source="controller_rootdisk_adopt", outcome="adopted"
                    )
                    manifest["_srwRootdiskReservation"] = {
                        "name": name,
                        "owner_kind": owner_kind,
                        "owner_id": job_id,
                        "dv_uid": dv_uid,
                        "pvc_uid": pvc_uid,
                        "admission_id": reservation["admission_id"],
                        "completed": (
                            reservation.get("completed_outcome") == "adopted"
                        ),
                        "outcome": reservation.get("_srw_outcome", "adopted"),
                        "carrier": reservation.get("carrier"),
                    }
                    return name
                if dv and phase == "Failed":
                    dv, dv_uid, failed_pvc_uid, reservation = await reserve_existing(
                        source="controller_failed_dv_recreate", outcome="recreated"
                    )
                    if reservation.get("completed_outcome") == "recreated":
                        raise RuntimeError(
                            "completed rootdisk recreation has not become observable"
                        )
                    carrier = reservation.get("carrier")
                    if not isinstance(carrier, Mapping):
                        raise RuntimeError(
                            "workspace cleanup carrier was not published"
                        )
                    log.warning("rootdisk %s is Failed — recreating", log_name)
                    await self._delete_dv(name, expected_uid=dv_uid)
                    dv = None
                    manifest["_srwRootdiskReservation"] = {
                        "name": name,
                        "owner_kind": owner_kind,
                        "owner_id": job_id,
                        "dv_uid": dv_uid,
                        "pvc_uid": failed_pvc_uid,
                        "admission_id": reservation["admission_id"],
                        "completed": False,
                        "outcome": "recreated",
                        "replacement": True,
                        "carrier": carrier,
                    }
        if dv is not None:
            dv, dv_uid, pvc_uid, reservation = await reserve_existing(
                source="controller_rootdisk_adopt", outcome="adopted"
            )
            manifest["_srwRootdiskReservation"] = {
                "name": name,
                "owner_kind": owner_kind,
                "owner_id": job_id,
                "dv_uid": dv_uid,
                "pvc_uid": pvc_uid,
                "admission_id": reservation["admission_id"],
                "completed": reservation.get("completed_outcome") == "adopted",
                "outcome": reservation.get("_srw_outcome", "adopted"),
                "carrier": reservation.get("carrier"),
            }
            log.info("rootdisk %s in progress (%s) — adopting", log_name, phase or "?")
            return name

        template_labels = (dvt.get("metadata") or {}).get("labels") or {}
        labels = dict(template_labels) if isinstance(template_labels, dict) else {}
        labels.update(
            {
                "srw.io/rootdisk": "true",
                "job-id": job_id,
                "srw.io/owner-kind": owner_kind,
                "srw.io/owner-id": job_id,
            }
        )
        body = {
            "apiVersion": f"{CDI_GROUP}/{CDI_VERSION}",
            "kind": "DataVolume",
            "metadata": {
                "name": name,
                "namespace": VM_NAMESPACE,
                # srw.io/rootdisk drives the GC listing; job-id ties the disk
                # back to its entity (job or thread — VM names are the same
                # shape for both).
                # CDI propagates these DataVolume labels to the generated root
                # PVC, giving the claim the same explicit ownership hint.
                "labels": labels,
            },
            # The template's own spec, clone mutation included. No
            # bind.immediate annotation: a clone target must stay
            # WaitForFirstConsumer so it binds on the VM's node.
            "spec": dvt.get("spec", {}),
        }
        replacement_reservation = manifest.get("_srwRootdiskReservation")
        if isinstance(replacement_reservation, Mapping) and replacement_reservation.get(
            "replacement"
        ):
            carrier = replacement_reservation.get("carrier")
            if not isinstance(carrier, Mapping):
                raise RuntimeError("workspace cleanup carrier was not published")
            body["metadata"]["annotations"] = {
                "srw.io/cleanup-admission-id": str(
                    replacement_reservation["admission_id"]
                ),
                "srw.io/cleanup-carrier-uid": str(carrier["carrier_uid"]),
                _CLEANUP_ANNOTATIONS["nonce"]: str(carrier["nonce"]),
                _PROVISION_GENERATION_ANNOTATION: provision_generation,
            }
        try:
            await asyncio.to_thread(
                self.k8s_client.create_namespaced_custom_object,
                group=CDI_GROUP,
                version=CDI_VERSION,
                namespace=VM_NAMESPACE,
                plural=CDI_PLURAL,
                body=body,
            )
            log.info("rootdisk created: %s (job %s)", log_name, job_id)
        except ApiException as e:
            if e.status != 409:
                raise
            log.info("rootdisk %s already exists — adopting", log_name)
        if isinstance(replacement_reservation, dict) and replacement_reservation.get(
            "replacement"
        ):
            (
                _,
                _,
                replacement_dv_uid,
                replacement_pvc_uid,
            ) = await self._exact_rootdisk_identity(
                name,
                owner_kind=owner_kind,
                owner_id=job_id,
            )
            if replacement_dv_uid == str(
                replacement_reservation["dv_uid"]
            ) or replacement_pvc_uid == str(replacement_reservation["pvc_uid"]):
                raise RuntimeError(
                    "failed rootdisk replacement identity did not advance"
                )
            carrier = await self._bind_workspace_cleanup_successor(
                replacement_reservation["carrier"],
                dv_uid=replacement_dv_uid,
                pvc_uid=replacement_pvc_uid,
            )
            replacement_reservation["dv_uid"] = replacement_dv_uid
            replacement_reservation["pvc_uid"] = replacement_pvc_uid
            replacement_reservation["carrier"] = carrier
        return name

    async def _gc_rootdisks_safe(self) -> None:
        """Non-fatal wrapper around _gc_rootdisks for fire-and-forget scheduling."""
        try:
            await self._gc_rootdisks()
        except Exception:
            log.exception("rootdisk GC pass failed")

    async def _gc_rootdisks(self) -> None:
        """Delete orphaned rootdisk DataVolumes — no VirtualMachine, older than
        VM_ROOTDISK_ORPHAN_HOURS.

        Layer 3 of the rootdisk GC (knowledge-base/knowledge/features/vm_persistent_rootdisk.md D4),
        and the only layer that can reach a disk whose entity row is gone from
        the orchestrator's DB entirely.

        Bails without deleting anything if the VM list fails: without it every
        disk looks orphaned, and this is a destructive sweep.
        """
        from kubernetes.client.exceptions import ApiException

        # A prior pass may have deleted the exact DV/PVC and then lost the DB
        # completion response. Carriers remain discoverable after the DV name
        # disappears, so settle them before relying on the DV list.
        await self._reconcile_workspace_cleanup_carriers()

        try:
            resp = await asyncio.to_thread(
                self.k8s_client.list_namespaced_custom_object,
                group=CDI_GROUP,
                version=CDI_VERSION,
                namespace=VM_NAMESPACE,
                plural=CDI_PLURAL,
                label_selector="srw.io/rootdisk",
            )
        except ApiException as e:
            log.debug("rootdisk GC list failed: %s", e)
            return
        disks = resp.get("items", [])
        if not disks:
            return

        try:
            vms = await asyncio.to_thread(
                self.k8s_client.list_namespaced_custom_object,
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=VM_NAMESPACE,
                plural=KUBEVIRT_PLURAL,
            )
        except ApiException as e:
            log.debug("rootdisk GC VM list failed — skipping GC this pass: %s", e)
            return
        live = {
            (vm.get("metadata") or {}).get("name")
            for vm in vms.get("items", [])
            if (vm.get("metadata") or {}).get("name")
        }

        max_age_minutes = VM_ROOTDISK_ORPHAN_HOURS * 60
        for dv in disks:
            name = (dv.get("metadata") or {}).get("name", "")
            if (
                (dv.get("metadata") or {})
                .get("labels", {})
                .get("srw.io/workspace-instance")
            ):
                continue
            if not name.endswith("-rootdisk"):
                continue
            if name[: -len("-rootdisk")] in live:
                continue  # its VM is back — a recovery in flight
            if _age_minutes(dv) < max_age_minutes:
                continue
            labels = (dv.get("metadata") or {}).get("labels", {})
            owner_id = labels.get("srw.io/owner-id") or labels.get("job-id")
            owner_kind = labels.get("srw.io/owner-kind") or "job"
            if not owner_id:
                owner_id = name[len("agent-vm-") : -len("-rootdisk")]
            async with self._workspace_lifecycle(str(owner_id)):
                # Re-read object existence and immutable identities after
                # acquiring the same boundary used by create and pin publish.
                fresh_disks = await asyncio.to_thread(
                    self.k8s_client.list_namespaced_custom_object,
                    group=CDI_GROUP,
                    version=CDI_VERSION,
                    namespace=VM_NAMESPACE,
                    plural=CDI_PLURAL,
                    label_selector="srw.io/rootdisk",
                )
                exact = [
                    item
                    for item in fresh_disks.get("items", [])
                    if (item.get("metadata") or {}).get("name") == name
                ]
                if len(exact) != 1:
                    continue
                current_dv = exact[0]
                current_metadata = current_dv.get("metadata") or {}
                current_labels = current_metadata.get("labels") or {}
                dv_uid = _safe_uid(current_metadata.get("uid"))
                candidate_uid = _safe_uid((dv.get("metadata") or {}).get("uid"))
                if (
                    dv_uid is None
                    or dv_uid != candidate_uid
                    or not isinstance(current_labels, Mapping)
                    or current_labels.get("srw.io/owner-kind") != str(owner_kind)
                    or current_labels.get("srw.io/owner-id") != str(owner_id)
                    or current_metadata.get("deletionTimestamp")
                    or _age_minutes(current_dv) < max_age_minutes
                ):
                    continue
                fresh_vms = await asyncio.to_thread(
                    self.k8s_client.list_namespaced_custom_object,
                    group=KUBEVIRT_GROUP,
                    version=KUBEVIRT_VERSION,
                    namespace=VM_NAMESPACE,
                    plural=KUBEVIRT_PLURAL,
                )
                if any(
                    (item.get("metadata") or {}).get("name")
                    == name[: -len("-rootdisk")]
                    for item in fresh_vms.get("items", [])
                ):
                    continue
                known, pvc_uid = await self._rootdisk_pvc_probe(
                    name,
                    owner_id=str(owner_id),
                    owner_kind=str(owner_kind),
                    wait=False,
                )
                if not known or pvc_uid is None:
                    log.warning(
                        "rootdisk GC: refusing %s with unknown PVC identity", name
                    )
                    continue
                recovery_pins = await self._active_recovery_pins()
                if self._pvc_is_recovery_pinned(recovery_pins, pvc_uid):
                    continue
                try:
                    reservation = await self._acquire_workspace_cleanup_reservation(
                        source="controller_rootdisk_delete",
                        owner_kind=str(owner_kind),
                        owner_id=str(owner_id),
                        pvc_uid=pvc_uid,
                        dv_uid=dv_uid,
                        provision_generation=str(
                            (current_metadata.get("annotations") or {}).get(
                                _PROVISION_GENERATION_ANNOTATION
                            )
                            or "unknown"
                        ),
                    )
                    if reservation.get("completed_outcome") == "deleted":
                        continue
                    carrier = reservation.get("carrier")
                    if not isinstance(carrier, Mapping):
                        raise RuntimeError(
                            "workspace cleanup carrier was not published"
                        )
                    # Reservation acquisition crossed the database boundary.
                    # Re-read both controller pin authority and the immutable
                    # DV UID before deletion.
                    if self._pvc_is_recovery_pinned(
                        await self._active_recovery_pins(), pvc_uid
                    ):
                        continue
                    after_reservation = await asyncio.to_thread(
                        self.k8s_client.list_namespaced_custom_object,
                        group=CDI_GROUP,
                        version=CDI_VERSION,
                        namespace=VM_NAMESPACE,
                        plural=CDI_PLURAL,
                        label_selector="srw.io/rootdisk",
                    )
                    rebound = [
                        item
                        for item in after_reservation.get("items", [])
                        if (item.get("metadata") or {}).get("name") == name
                        and _safe_uid((item.get("metadata") or {}).get("uid")) == dv_uid
                    ]
                    if len(rebound) != 1:
                        continue
                    await self._delete_dv(name, expected_uid=dv_uid)
                    absent = await self._reconcile_cleanup_carrier_old_identity(
                        carrier, require_failed_dv=False
                    )
                    if absent:
                        await self._complete_workspace_cleanup_reservation(
                            carrier, outcome="deleted"
                        )
                        log.warning(
                            "rootdisk GC: deleted orphan %s (no VM for >%dh)",
                            name,
                            VM_ROOTDISK_ORPHAN_HOURS,
                        )
                    else:
                        log.info(
                            "rootdisk GC: deletion of %s is still reconciling",
                            name,
                        )
                except Exception as e:
                    log.warning("rootdisk GC: delete %s failed: %s", name, e)

    async def _gc_goldens_safe(self, image: str) -> None:
        """Non-fatal wrapper around _gc_goldens for fire-and-forget scheduling."""
        try:
            await self._gc_goldens(image)
        except Exception:
            log.exception("golden GC pass failed")

    async def _gc_goldens(self, current_image: str) -> None:
        """Delete stale golden DataVolumes, keeping the newest N digests. Never
        touch the current image's golden, one a live VM still references (its
        in-flight clone reads the source pod), or one younger than the min age.
        Deletes the DataVolume only — its PVC cascades (the controller has no
        CoreV1Api / PVC permissions by design).
        """
        from kubernetes.client.exceptions import ApiException

        try:
            resp = await asyncio.to_thread(
                self.k8s_client.list_namespaced_custom_object,
                group=CDI_GROUP,
                version=CDI_VERSION,
                namespace=VM_NAMESPACE,
                plural=CDI_PLURAL,
                label_selector="srw.io/golden-image",
            )
        except ApiException as e:
            log.debug("golden GC list failed: %s", e)
            return
        goldens = resp.get("items", [])
        if len(goldens) <= VM_GOLDEN_KEEP:
            return

        # In-use = any golden a live VM's rootdisk was cloned from.
        in_use: set[str] = set()
        try:
            vms = await asyncio.to_thread(
                self.k8s_client.list_namespaced_custom_object,
                group=KUBEVIRT_GROUP,
                version=KUBEVIRT_VERSION,
                namespace=VM_NAMESPACE,
                plural=KUBEVIRT_PLURAL,
            )
        except ApiException as e:
            log.debug("golden GC VM list failed — skipping GC this pass: %s", e)
            return
        for vm in vms.get("items", []):
            for dvt in vm.get("spec", {}).get("dataVolumeTemplates", []):
                pvc = (dvt.get("spec", {}).get("source", {}) or {}).get("pvc")
                if pvc and pvc.get("name"):
                    in_use.add(pvc["name"])

        current = _golden_name(current_image)
        goldens.sort(
            key=lambda g: g.get("metadata", {}).get("creationTimestamp", ""),
            reverse=True,
        )
        for g in goldens[VM_GOLDEN_KEEP:]:
            name = g.get("metadata", {}).get("name", "")
            if not name or name == current or name in in_use:
                continue
            if _age_minutes(g) < VM_GOLDEN_GC_MIN_AGE_MINUTES:
                continue
            try:
                await self._delete_dv(name)
                log.info("golden GC: deleted stale %s", name)
            except Exception as e:
                log.warning("golden GC: delete %s failed: %s", name, e)

    async def _prewarm_golden(self) -> None:
        """Best-effort: import the default image's golden before the first job,
        so the first VM doesn't pay the one-time import on its critical path.
        """
        try:
            name = await self._ensure_golden(DEFAULT_VM_IMAGE)
            if name:
                log.info("golden pre-warm ready: %s", name)
            else:
                log.warning("golden pre-warm did not complete (non-fatal)")
        except Exception:
            log.exception("golden pre-warm failed (non-fatal)")

    # =========================================================================
    # NATS transport
    # =========================================================================

    async def handle_create(self, msg):
        """vm.lifecycle.create → _do_create + publish vm.lifecycle.status."""
        request_generation = None
        request_id = None
        try:
            job_config = json.loads(msg.data.decode())
            if not isinstance(
                job_config, Mapping
            ) or not await self._verify_lifecycle_request(
                job_config, "create", mutating=True
            ):
                raise PermissionError("invalid VM lifecycle create authentication")
            request_id = _lifecycle_request_id(job_config)
            job_config = unsigned_payload(job_config)
            request_generation = _provision_generation(
                job_config.get("provision_generation")
            )
            result = await self._do_create(job_config)
            await self._publish_status(
                result["job_id"],
                result,
                operation="create",
                correlation_id=request_id,
            )
        except PermissionError:
            log.warning("Dropping unauthenticated VM lifecycle create request")
        except Exception as e:
            job_id = _safe_job_id(msg.data)
            log.exception("Failed to create VM for job %s", job_id)
            error_result = {
                "job_id": job_id,
                "status": "failed",
                "error": str(e),
            }
            if request_generation is not None:
                error_result["provision_generation"] = request_generation
            await self._publish_status(
                job_id,
                error_result,
                operation="create",
                correlation_id=request_id,
            )

    async def handle_delete(self, msg):
        """vm.lifecycle.delete → _do_delete + publish vm.lifecycle.status."""
        request_generation = None
        request_id = None
        try:
            data = json.loads(msg.data.decode())
            if not isinstance(
                data, Mapping
            ) or not await self._verify_lifecycle_request(
                data, "delete", mutating=True
            ):
                raise PermissionError("invalid VM lifecycle delete authentication")
            request_id = _lifecycle_request_id(data)
            data = unsigned_payload(data)
            request_generation = _provision_generation(data.get("provision_generation"))
            # Absent field → purge, so an un-upgraded orchestrator keeps exact
            # current semantics.
            delete_kwargs = {
                "purge_disk": data.get("purge_disk", True) is not False,
                "provision_generation": data.get("provision_generation"),
            }
            if data.get("entity_type") is not None:
                delete_kwargs["owner_kind"] = data["entity_type"]
            if data.get("parent_cleanup") is not None:
                delete_kwargs["parent_cleanup"] = data["parent_cleanup"]
            if data.get("expected_vm_uid") is not None:
                delete_kwargs["expected_vm_uid"] = data["expected_vm_uid"]
            if data.get("expected_rootdisk_pvc_uid") is not None:
                delete_kwargs["expected_rootdisk_pvc_uid"] = data[
                    "expected_rootdisk_pvc_uid"
                ]
            result = await self._do_delete(data["job_id"], **delete_kwargs)
            await self._publish_status(
                result["job_id"],
                result,
                operation="delete",
                correlation_id=request_id,
            )
        except PermissionError:
            log.warning("Dropping unauthenticated VM lifecycle delete request")
        except Exception as e:
            job_id = _safe_job_id(msg.data)
            log.exception("Failed to delete VM for job %s", job_id)
            error_result = {
                "job_id": job_id,
                "status": "delete_failed",
                "error": str(e),
            }
            if request_generation is not None:
                error_result["provision_generation"] = request_generation
            await self._publish_status(
                job_id,
                error_result,
                operation="delete",
                correlation_id=request_id,
            )

    async def handle_status_query(self, msg):
        """vm.lifecycle.get → _do_status (request/reply or status publish)."""
        request_generation = None
        request_id = None
        try:
            data = json.loads(msg.data.decode())
            if not isinstance(
                data, Mapping
            ) or not await self._verify_lifecycle_request(
                data, "status", mutating=False
            ):
                raise PermissionError("invalid VM lifecycle status authentication")
            request_id = _lifecycle_request_id(data)
            data = unsigned_payload(data)
            request_generation = _provision_generation(data.get("provision_generation"))
            response = await self._do_status(
                data["job_id"],
                provision_generation=request_generation,
                exact_absence=data.get("exact_absence") is True,
            )
            response = sign_payload(
                response,
                direction="response",
                operation="status",
                secret=LIFECYCLE_HMAC_SECRET,
                correlation_id=request_id,
            )
            if msg.reply:
                await self.nc.publish(msg.reply, json.dumps(response).encode())
            else:
                await self._publish_status(
                    response["job_id"],
                    response,
                    operation="status",
                    correlation_id=request_id,
                )
        except PermissionError:
            log.warning("Dropping unauthenticated VM lifecycle status request")
        except Exception as e:
            job_id = _safe_job_id(msg.data)
            error_response = {
                "job_id": job_id,
                "status": "query_failed",
                "error": str(e),
            }
            if request_generation is not None:
                error_response["provision_generation"] = request_generation
            if msg.reply:
                signed_error = sign_payload(
                    error_response,
                    direction="response",
                    operation="status",
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                )
                await self.nc.publish(msg.reply, json.dumps(signed_error).encode())
            else:
                await self._publish_status(
                    job_id,
                    error_response,
                    operation="status",
                    correlation_id=request_id,
                )

    async def handle_list(self, msg):
        """vm.lifecycle.list → _do_list (request/reply only).

        A list is only meaningful as a reply to the asker; there is no
        status-publish fallback like the other handlers have.
        """
        try:
            data = json.loads(msg.data.decode())
            if not isinstance(
                data, Mapping
            ) or not await self._verify_lifecycle_request(data, "list", mutating=False):
                raise PermissionError("invalid VM lifecycle list authentication")
            request_id = _lifecycle_request_id(data)
            unsigned = unsigned_payload(data)
            response = sign_payload(
                await self._do_list(
                    include_teardown_identity=(
                        unsigned.get("include_teardown_identity") is True
                    )
                ),
                direction="response",
                operation="list",
                secret=LIFECYCLE_HMAC_SECRET,
                correlation_id=request_id,
            )
        except PermissionError:
            log.warning("Dropping unauthenticated VM lifecycle list request")
            return
        except Exception as e:
            log.exception("Failed to list VMs")
            response = sign_payload(
                {"status": "list_failed", "error": str(e)},
                direction="response",
                operation="list",
                secret=LIFECYCLE_HMAC_SECRET,
                correlation_id=(request_id if "request_id" in locals() else None),
            )
        if msg.reply:
            await self.nc.publish(msg.reply, json.dumps(response).encode())

    # =========================================================================
    # HTTP transport (aiohttp)
    # =========================================================================

    async def http_create(self, request):
        """POST /vms — body is the create payload, returns the result dict."""
        return await self._http_create(request, operation="create")

    async def http_creation_retry(self, request):
        """Dedicated protocol route: an older replica returns 404, never creates."""
        return await self._http_create(
            request, operation="creation_retry_create", require_creation_retry=True
        )

    async def http_creation_dispose(self, request):
        """Authenticated cancellation of an existing immutable create request."""
        from vm_controller.creation_disposition import http_dispose

        return await http_dispose(self, request)

    async def _http_create(self, request, *, operation, require_creation_retry=False):
        from aiohttp import web

        try:
            payload = await request.json()
        except Exception as e:
            return web.json_response({"error": f"invalid json: {e}"}, status=400)

        if not isinstance(payload, Mapping) or not payload.get("job_id"):
            return web.json_response({"error": "job_id required"}, status=400)
        if not isinstance(payload, Mapping) or not await self._verify_lifecycle_request(
            payload, operation, mutating=True
        ):
            return web.json_response({"error": "authentication failed"}, status=401)
        request_id = _lifecycle_request_id(payload)
        payload = unsigned_payload(payload)
        request_generation = _provision_generation(payload.get("provision_generation"))

        if require_creation_retry and "creation_retry" not in payload:
            return web.json_response(
                sign_payload(
                    {
                        "status": "creation_attention",
                        "reason": "creation_protocol_unproven",
                    },
                    direction="response",
                    operation=operation,
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                ),
                status=400,
            )

        try:
            result = await self._do_create(payload)
            return web.json_response(
                sign_payload(
                    result,
                    direction="response",
                    operation=operation,
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                ),
                status=200,
            )
        except Exception as e:
            log.exception("HTTP create failed for job %s", payload.get("job_id"))
            error_result = {
                "job_id": payload.get("job_id", "unknown"),
                "status": "failed",
                "error": str(e),
            }
            if request_generation is not None:
                error_result["provision_generation"] = request_generation
            return web.json_response(
                sign_payload(
                    error_result,
                    direction="response",
                    operation=operation,
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                ),
                status=500,
            )

    async def http_delete(self, request):
        """DELETE /vms/{job_id}[?purge_disk=false] — returns the result dict."""
        from aiohttp import web

        job_id = request.match_info.get("job_id")
        if not job_id:
            return web.json_response({"error": "job_id required"}, status=400)

        # Query param rather than a body: DELETE bodies are awkward for both
        # httpx and aiohttp, and the intent is a single boolean.
        purge_disk = str(request.query.get("purge_disk", "true")).lower() not in (
            "0",
            "false",
            "no",
        )
        request_payload = _authenticated_http_payload(
            request,
            {
                "job_id": job_id,
                "purge_disk": purge_disk,
                "provision_generation": request.query.get("provision_generation"),
                **(
                    {"parent_cleanup": request.query["parent_cleanup"]}
                    if "parent_cleanup" in request.query
                    else {}
                ),
                **(
                    {"entity_type": request.query["entity_type"]}
                    if "entity_type" in request.query
                    else {}
                ),
                **(
                    {"workspace_storage": request.query["workspace_storage"]}
                    if "workspace_storage" in request.query
                    else {}
                ),
                **(
                    {"expected_vm_uid": request.query.get("expected_vm_uid")}
                    if request.query.get("expected_vm_uid") is not None
                    else {}
                ),
                **(
                    {
                        "expected_rootdisk_pvc_uid": request.query.get(
                            "expected_rootdisk_pvc_uid"
                        )
                    }
                    if request.query.get("expected_rootdisk_pvc_uid") is not None
                    else {}
                ),
            },
            operation="delete",
        )
        if not await self._verify_lifecycle_request(
            request_payload, "delete", mutating=True
        ):
            return web.json_response({"error": "authentication failed"}, status=401)
        request_id = _lifecycle_request_id(request_payload)
        request_payload = unsigned_payload(request_payload)

        try:
            delete_kwargs = {
                "purge_disk": purge_disk,
                "provision_generation": request_payload.get("provision_generation"),
            }
            if request_payload.get("entity_type") is not None:
                delete_kwargs["owner_kind"] = request_payload["entity_type"]
            if request_payload.get("parent_cleanup") is not None:
                delete_kwargs["parent_cleanup"] = json.loads(
                    request_payload["parent_cleanup"]
                )
            if request_payload.get("expected_vm_uid") is not None:
                delete_kwargs["expected_vm_uid"] = request_payload["expected_vm_uid"]
            if request_payload.get("expected_rootdisk_pvc_uid") is not None:
                delete_kwargs["expected_rootdisk_pvc_uid"] = request_payload[
                    "expected_rootdisk_pvc_uid"
                ]
            if request_payload.get("workspace_storage") is not None:
                delete_kwargs["workspace_storage"] = json.loads(
                    request_payload["workspace_storage"]
                )
            result = await self._do_delete(job_id, **delete_kwargs)
            return web.json_response(
                sign_payload(
                    result,
                    direction="response",
                    operation="delete",
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                ),
                status=200,
            )
        except Exception as e:
            log.exception("HTTP delete failed for job %s", job_id)
            error_result = {
                "job_id": job_id,
                "status": "delete_failed",
                "error": str(e),
            }
            if generation := _provision_generation(
                request_payload.get("provision_generation")
            ):
                error_result["provision_generation"] = generation
            return web.json_response(
                sign_payload(
                    error_result,
                    direction="response",
                    operation="delete",
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                ),
                status=500,
            )

    async def http_status(self, request):
        """GET /vms/{job_id} — returns the result dict."""
        from aiohttp import web

        job_id = request.match_info.get("job_id")
        if not job_id:
            return web.json_response({"error": "job_id required"}, status=400)
        exact_absence = (
            str(request.query.get("exact_absence", "false")).lower() == "true"
        )
        request_payload = _authenticated_http_payload(
            request,
            {
                "job_id": job_id,
                "provision_generation": request.query.get("provision_generation"),
                **(
                    {"workspace_storage": request.query["workspace_storage"]}
                    if "workspace_storage" in request.query
                    else {}
                ),
                **({"exact_absence": True} if exact_absence else {}),
            },
            operation="status",
        )
        if not await self._verify_lifecycle_request(
            request_payload, "status", mutating=False
        ):
            return web.json_response({"error": "authentication failed"}, status=401)
        request_id = _lifecycle_request_id(request_payload)

        try:
            result = await self._do_status(
                job_id,
                provision_generation=request_payload.get("provision_generation"),
                exact_absence=request_payload.get("exact_absence") is True,
                **(
                    {
                        "workspace_storage": json.loads(
                            request_payload["workspace_storage"]
                        )
                    }
                    if request_payload.get("workspace_storage") is not None
                    else {}
                ),
            )
            return web.json_response(
                sign_payload(
                    result,
                    direction="response",
                    operation="status",
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                ),
                status=200,
            )
        except Exception as e:
            from kubernetes.client.exceptions import ApiException

            if isinstance(e, ApiException) and e.status == 404:
                not_found = {
                    "job_id": job_id,
                    "status": "not_found",
                }
                if generation := _provision_generation(
                    request_payload.get("provision_generation")
                ):
                    not_found["provision_generation"] = generation
                return web.json_response(
                    sign_payload(
                        not_found,
                        direction="response",
                        operation="status",
                        secret=LIFECYCLE_HMAC_SECRET,
                        correlation_id=request_id,
                    ),
                    status=404,
                )
            log.debug("HTTP status query failed for job %s: %s", job_id, e)
            error_result = {
                "job_id": job_id,
                "status": "query_failed",
                "error": str(e),
            }
            if generation := _provision_generation(
                request_payload.get("provision_generation")
            ):
                error_result["provision_generation"] = generation
            return web.json_response(
                sign_payload(
                    error_result,
                    direction="response",
                    operation="status",
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                ),
                status=500,
            )

    async def http_workspace_recovery_observation(self, request):
        """Authenticated read-only exact recovery observation."""

        from aiohttp import web

        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)
        if not isinstance(payload, Mapping) or not await self._verify_lifecycle_request(
            payload, "recovery-observe", mutating=False
        ):
            return web.json_response({"error": "authentication failed"}, status=401)
        request_id = _lifecycle_request_id(payload)
        try:
            result = await self._do_observe_workspace_recovery(
                unsigned_payload(payload)
            )
            return web.json_response(
                sign_payload(
                    result,
                    direction="response",
                    operation="recovery-observe",
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                )
            )
        except Exception as exc:
            log.warning("Workspace recovery observation refused: %s", exc)
            return web.json_response(
                sign_payload(
                    {"status": "observation_failed", "error": str(exc)},
                    direction="response",
                    operation="recovery-observe",
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                ),
                status=409,
            )

    async def http_workspace_recovery_pin(self, request):
        """Authenticated idempotent reconciliation of one controller pin."""

        from aiohttp import web

        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)
        if not isinstance(payload, Mapping) or not await self._verify_lifecycle_request(
            payload, "recovery-pin", mutating=True
        ):
            return web.json_response({"error": "authentication failed"}, status=401)
        request_id = _lifecycle_request_id(payload)
        try:
            result = await self._do_reconcile_workspace_recovery_pin(
                unsigned_payload(payload)
            )
            return web.json_response(
                sign_payload(
                    result,
                    direction="response",
                    operation="recovery-pin",
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                )
            )
        except Exception as exc:
            log.warning("Workspace recovery pin reconciliation refused: %s", exc)
            return web.json_response(
                sign_payload(
                    {"status": "pin_failed", "error": str(exc)},
                    direction="response",
                    operation="recovery-pin",
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                ),
                status=409,
            )

    async def http_list(self, request):
        """GET /vms — enumerate managed agent VMs (orphan-sweep inventory)."""
        from aiohttp import web

        include_teardown_identity = (
            str(request.query.get("include_teardown_identity", "false")).lower()
            == "true"
        )
        request_payload = _authenticated_http_payload(
            request,
            ({"include_teardown_identity": True} if include_teardown_identity else {}),
            operation="list",
        )
        if not await self._verify_lifecycle_request(
            request_payload, "list", mutating=False
        ):
            return web.json_response({"error": "authentication failed"}, status=401)
        request_id = _lifecycle_request_id(request_payload)
        try:
            result = await self._do_list(
                include_teardown_identity=include_teardown_identity
            )
            return web.json_response(
                sign_payload(
                    result,
                    direction="response",
                    operation="list",
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                ),
                status=200,
            )
        except Exception as e:
            log.exception("HTTP list failed")
            return web.json_response(
                sign_payload(
                    {"status": "list_failed", "error": str(e)},
                    direction="response",
                    operation="list",
                    secret=LIFECYCLE_HMAC_SECRET,
                    correlation_id=request_id,
                ),
                status=500,
            )

    async def http_release_workspace(self, request):
        return await self._http_workspace_storage_action(request, "release-workspace")

    async def http_detach_workspace(self, request):
        return await self._http_workspace_storage_action(request, "detach-workspace")

    async def _http_workspace_storage_action(self, request, operation):
        from aiohttp import web
        from kubernetes.client.exceptions import ApiException

        payload = await request.json()
        if LIFECYCLE_HMAC_SECRET is None or not await self._verify_lifecycle_request(
            payload, operation, mutating=True
        ):
            return web.json_response({"error": "authentication failed"}, status=401)
        request_id = _lifecycle_request_id(payload)
        try:
            action = (
                self._retained_storage().delete
                if operation == "release-workspace"
                else self._retained_storage().detach
            )
            complete = await action(payload.get("workspace_storage"))
            result, status = {"deleted": complete}, 200
        except (ValueError, RuntimeError, ApiException) as exc:
            result, status = {"error": str(exc)}, 409
        return web.json_response(
            sign_payload(
                result,
                direction="response",
                operation=operation,
                secret=LIFECYCLE_HMAC_SECRET,
                correlation_id=request_id,
            ),
            status=status,
        )

    async def http_resolve_creation_config(self, request):
        """Authenticate a read-only effective configuration snapshot, without grants."""
        from aiohttp import web
        from vm_controller.creation_configuration import resolve_creation_configuration

        operation = "creation_config_resolve"
        try:
            value = await request.json()
        except (ValueError, TypeError):
            return web.json_response({"error": "invalid_request"}, status=400)
        if (
            LIFECYCLE_HMAC_SECRET is None
            or not isinstance(value, Mapping)
            or not await self._verify_lifecycle_request(
                value, operation, mutating=False
            )
        ):
            return web.json_response({"error": "authentication_failed"}, status=401)
        try:
            payload = unsigned_payload(value)
            if set(payload) != {"request"}:
                raise ValueError("invalid resolution request")
            result = resolve_creation_configuration(self, payload["request"])
            status = 200
        except (ValueError, TypeError, KeyError, AttributeError):
            result, status = {"reason": "creation_configuration_unproven"}, 409
        return web.json_response(
            sign_payload(
                result,
                direction="response",
                operation=operation,
                secret=LIFECYCLE_HMAC_SECRET,
                correlation_id=_lifecycle_request_id(value),
            ),
            status=status,
        )

    async def http_health(self, _request):
        """GET /healthz — liveness probe target."""
        from aiohttp import web

        return web.json_response({"status": "ok"})

    async def _cancel_preparation(self, value):
        """Serialize non-issuance evidence with VM creation and runtime absence."""
        from kubernetes.client.exceptions import ApiException
        from shared.workspace_preparation import validate_request

        preparation = validate_request(value)
        entity_id = preparation["allocationId"]
        async with self._workspace_lifecycle(entity_id):
            result = await self._workspace_preparation().cancel_with_receipt(
                preparation
            )
            if result.get("workspaceNeverIssued") is not True:
                return result
            name = f"agent-vm-{entity_id}"
            for plural in (KUBEVIRT_PLURAL, KUBEVIRT_VMI_PLURAL):
                try:
                    await asyncio.to_thread(
                        self.k8s_client.get_namespaced_custom_object,
                        group=KUBEVIRT_GROUP,
                        version=KUBEVIRT_VERSION,
                        namespace=VM_NAMESPACE,
                        plural=plural,
                        name=name,
                    )
                except ApiException as exc:
                    if exc.status != 404:
                        raise
                else:
                    return {**result, "workspaceNeverIssued": False}
            if self.core_api is None:
                return {**result, "workspaceNeverIssued": False}
            pods = await asyncio.to_thread(
                self.core_api.list_namespaced_pod,
                namespace=VM_NAMESPACE,
                label_selector=f"vm.kubevirt.io/name={name}",
            )
            items = getattr(pods, "items", None)
            return {
                **result,
                "workspaceNeverIssued": isinstance(items, list) and not items,
            }

    async def http_preparation(self, request):
        from aiohttp import web

        action = request.match_info["action"]
        if action not in {"list", "delete", "cancel", "prepare"}:
            return web.json_response(
                {"error": "Unknown preparation operation"}, status=404
            )
        operation = "preparation-" + action
        payload = await request.json()
        if LIFECYCLE_HMAC_SECRET is None or not await self._verify_lifecycle_request(
            payload, operation, mutating=action != "list"
        ):
            return web.json_response({"error": "authentication failed"}, status=401)
        request_id = _lifecycle_request_id(payload)
        status = 200
        try:
            service = self._workspace_preparation()
            if action == "prepare":
                source, waiting = await service.prepare(payload["preparation"])
                result = {"source": source, "waiting": waiting}
            elif action == "cancel":
                result = await self._cancel_preparation(payload["preparation"])
            else:
                scope = payload["scope"]
                if (
                    not isinstance(scope, dict)
                    or set(scope) != {"kind", "uid"}
                    or scope["kind"] not in {"Account", "Project"}
                    or str(UUID(scope["uid"])) != scope["uid"]
                ):
                    raise ValueError("Invalid preparation scope")
                result = (
                    {"artifacts": await service.artifacts(scope)}
                    if action == "list"
                    else {
                        "deleted": await service.delete_artifact(payload["uid"], scope)
                    }
                )
                if action == "delete" and result["deleted"] is not True:
                    status = 409
        except (ValueError, KeyError, TypeError):
            result, status = {"error": "Invalid preparation operation"}, 400
        except Exception:
            log.exception("Workspace preparation %s failed", action)
            result, status = (
                {
                    "error": "Preparation operation could not establish current ownership"
                },
                409,
            )
        return web.json_response(
            sign_payload(
                result,
                direction="response",
                operation=operation,
                secret=LIFECYCLE_HMAC_SECRET,
                correlation_id=request_id,
            ),
            status=status,
        )

    async def _publish_status(
        self,
        job_id: str,
        payload: dict,
        *,
        operation: str = "status",
        correlation_id: str | None = None,
    ):
        """Publish a status message on vm.lifecycle.status.{ORCHESTRATOR_ID}
        (NATS only)."""
        if not self.nc:
            return
        try:
            await self.nc.publish(
                f"vm.lifecycle.status.{ORCHESTRATOR_ID}",
                json.dumps(
                    sign_payload(
                        payload,
                        direction="response",
                        operation=operation,
                        secret=LIFECYCLE_HMAC_SECRET,
                        correlation_id=correlation_id,
                    )
                ).encode(),
            )
        except Exception:
            log.exception("Failed to publish status for job %s", job_id)

    async def start_http_server(self) -> None:
        """Start the aiohttp HTTP server. Runs alongside other transports."""
        from aiohttp import web

        app = web.Application()
        app.router.add_post("/vms", self.http_create)
        app.router.add_post("/vm-creation/create", self.http_creation_retry)
        app.router.add_post("/vm-creation/dispose", self.http_creation_dispose)
        app.router.add_post(
            "/vm-creation/configuration", self.http_resolve_creation_config
        )
        app.router.add_post("/workspace-disks/release", self.http_release_workspace)
        app.router.add_post("/workspace-disks/detach", self.http_detach_workspace)
        app.router.add_post("/workspace-preparations/{action}", self.http_preparation)
        app.router.add_post(
            "/workspace-recovery/observe", self.http_workspace_recovery_observation
        )
        app.router.add_post(
            "/workspace-recovery/pins", self.http_workspace_recovery_pin
        )
        app.router.add_get("/vms", self.http_list)
        app.router.add_delete("/vms/{job_id}", self.http_delete)
        app.router.add_get("/vms/{job_id}", self.http_status)
        app.router.add_get("/healthz", self.http_health)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, LISTEN_HOST, LISTEN_PORT)
        await site.start()
        self.http_runner = runner
        log.info("HTTP server listening on %s:%d", LISTEN_HOST, LISTEN_PORT)

    async def run(self):
        """Main entry point — connect transports, wait for shutdown."""
        log.info("VM Controller starting (transport=%s)", TRANSPORT)

        if TRANSPORT not in ("nats", "http", "both"):
            log.error("Invalid TRANSPORT=%s (expected nats|http|both)", TRANSPORT)
            sys.exit(1)

        self.load_template()
        self.init_k8s()
        await self.headscale.init()
        from shared.vm_resource_inventory_settings import InventorySettings
        from vm_controller.resource_inventory_runtime import inventory_observer_context

        async with inventory_observer_context(
            InventorySettings.from_environment(),
            base_url=ORCHESTRATOR_URL,
            secret=LIFECYCLE_HMAC_SECRET,
            stop=self._shutdown,
        ):
            await self._run_transports()

    async def _run_transports(self):
        """Serve lifecycle requests within the inventory observer lifetime."""
        preparation_task = asyncio.create_task(self._preparation_loop())

        # Pre-warm the default image's golden so the first job doesn't pay the
        # one-time import on its critical path (best-effort, non-blocking).
        if VM_GOLDEN_IMAGE_ENABLED:
            asyncio.create_task(self._prewarm_golden())

        if TRANSPORT in ("nats", "both"):
            if not ORCHESTRATOR_ID:
                log.error(
                    "ORCHESTRATOR_ID is required for NATS transport — refusing to "
                    "subscribe to flat vm.lifecycle.* (would cross-talk on shared hub)"
                )
                sys.exit(1)
            await self.connect_nats()
            suffix = f".{ORCHESTRATOR_ID}"
            await self.nc.subscribe(
                f"vm.lifecycle.create{suffix}", cb=self.handle_create
            )
            await self.nc.subscribe(
                f"vm.lifecycle.delete{suffix}", cb=self.handle_delete
            )
            await self.nc.subscribe(
                f"vm.lifecycle.get{suffix}", cb=self.handle_status_query
            )
            await self.nc.subscribe(f"vm.lifecycle.list{suffix}", cb=self.handle_list)
            log.info(
                "Subscribed to vm.lifecycle.{create,delete,get,list}.%s — waiting for NATS requests",
                ORCHESTRATOR_ID,
            )

        if TRANSPORT in ("http", "both"):
            await self.start_http_server()

        # Wait for shutdown signal
        await self._shutdown.wait()
        await preparation_task

        log.info("Shutting down...")
        if self.nc and self.nc.is_connected:
            await self.nc.drain()
        if self.http_runner is not None:
            await self.http_runner.cleanup()
        await self.headscale.close()

        log.info("VM Controller stopped")

    def request_shutdown(self):
        """Signal the controller to shut down gracefully."""
        self._shutdown.set()


def _safe_job_id(data: bytes) -> str:
    """Extract job_id from a NATS payload without raising."""
    try:
        return json.loads(data.decode()).get("job_id", "unknown")
    except Exception:
        return "unknown"


def _rootdisk_name(job_id: str) -> str:
    """The rootdisk DataVolume name — identical to what the VM template renders,
    so ``volumes[].dataVolume.name`` never has to change. Entity-agnostic: the
    controller only sees an id, and VM names are ``agent-vm-<id>`` for both jobs
    and sessions.
    """
    return f"agent-vm-{job_id}-rootdisk"


def _golden_name(image: str) -> str:
    """Deterministic, DNS-safe golden PVC name from an image ref (content-keyed
    on the full ref so a new base-image sha yields a new golden)."""
    digest = hashlib.sha256(image.encode()).hexdigest()[:12]
    return f"agent-vm-golden-{digest}"


def _label_safe(image: str) -> str:
    """Sanitize an image ref into a <=63-char Kubernetes label value."""
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", image)
    return safe[-63:].strip("_.-") or "unknown"


def _age_minutes(obj: dict) -> float:
    """Age in minutes of a K8s object from its creationTimestamp; 0 if unknown."""
    ts = obj.get("metadata", {}).get("creationTimestamp")
    if not ts:
        return 0.0
    try:
        created = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except (ValueError, TypeError):
        return 0.0
    return (datetime.now(timezone.utc) - created).total_seconds() / 60.0


def main():
    controller = VMController()

    def signal_handler(sig, _frame):
        log.info("Received signal %d, requesting shutdown", sig)
        controller.request_shutdown()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    asyncio.run(controller.run())


if __name__ == "__main__":
    main()
