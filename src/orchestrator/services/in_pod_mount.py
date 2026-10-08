"""The in-pod plane's cloud mount sidecars (connector drivers D7, spike prototype).

Two native sidecars give a workspace Pod a FUSE mount while the workspace
container has no FUSE device, no capability for it and no credential:

- ``srw-fuse-opener`` is privileged and the only privileged code. It opens
  /dev/fuse, mounts it at its one target inside a memory emptyDir it shares
  with Bidirectional propagation, and passes the descriptor over a unix socket
  in a second emptyDir only the rclone sidecar mounts. It opens no network
  listener and never reads what the filesystem serves. It forces nosuid,
  nodev and, for a read-only mount, ro; on SIGTERM it detaches the mount so
  the kubelet can tear the emptyDir down.
- ``srw-cloud-mount`` runs rclone mount2 unprivileged (uid 65534, every
  capability dropped, read-only root filesystem, no /dev/fuse). It alone
  mounts the credential Secret and asks the opener through the fusermount3
  client its image carries. It opens no listener (never ``--rc``): containers
  of a Pod share localhost.

The workspace container mounts the shared emptyDir read-only with
HostToContainer propagation, so it sees the mount at ``/cloud/<name>`` and can
neither plant a symlink at the target nor remount it. The opener starts first
and stops last, so a mount whose rclone died is still detached at shutdown.

Off unless the chart's ``connectors.inPodPlane`` is on, and nothing asks for a
mount yet: D7 proper wires connectors and the main cloud. Design and the
spike's evidence: knowledge-base/knowledge/features/connector_drivers.md
("Three planes", D7); scripts/k3d-in-pod-plane-spike.py reproduces it.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

OPENER_CONTAINER = "srw-fuse-opener"
RCLONE_CONTAINER = "srw-cloud-mount"
CLOUD_VOLUME = "srw-cloud"
SOCKET_VOLUME = "srw-fuse-socket"
CREDENTIAL_VOLUME = "srw-cloud-credential"
RCLONE_TMP_VOLUME = "srw-cloud-tmp"

#: Where the workspace sees its cloud folders.
WORKSPACE_CLOUD_ROOT = "/cloud"
#: The same emptyDir in the sidecars.
SIDECAR_CLOUD_ROOT = "/srw/cloud"
#: The opener client's compiled-in default; go-fuse runs fusermount3 with
#: _FUSE_COMMFD as its only variable, so the path cannot come from env.
SOCKET_DIR = "/run/srw-fuse"
SOCKET_PATH = f"{SOCKET_DIR}/opener.sock"
CREDENTIAL_DIR = "/etc/srw-cloud"
#: The Secret key the rclone sidecar reads (an rclone config, its password
#: obscured as rclone requires).
CREDENTIAL_KEY = "rclone.conf"
RCLONE_UID = 65534
#: agent-host in the workspace image owns what the mount shows.
WORKSPACE_UID = 1000

_NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")
_REMOTE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_-]{0,63}:[^\x00-\x1f]{0,1024}")


@dataclass(frozen=True)
class InPodPlaneImages:
    """The two sidecar images, set by the chart only when the plane is on."""

    opener: str
    rclone: str

    @classmethod
    def from_env(cls) -> "InPodPlaneImages | None":
        opener = os.environ.get("CONNECTOR_IN_POD_OPENER_IMAGE", "").strip()
        rclone = os.environ.get("CONNECTOR_IN_POD_RCLONE_IMAGE", "").strip()
        if not opener or not rclone:
            return None
        return cls(opener=opener, rclone=rclone)


@dataclass(frozen=True)
class CloudMountSidecar:
    """One rclone remote mounted at ``/cloud/<name>`` in a workspace Pod."""

    name: str
    #: A Secret holding ``rclone.conf``; only the rclone sidecar mounts it.
    secret_name: str
    #: An rclone remote of that config, e.g. ``cloud:`` or ``cloud:Projects/x``.
    remote: str
    #: Enforced by the opener's mount flags, not only by rclone's flag.
    read_only: bool = True

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ValueError("cloud mount name must be a DNS label")
        if not _NAME.fullmatch(self.secret_name):
            raise ValueError("cloud mount Secret name is malformed")
        if not _REMOTE.fullmatch(self.remote):
            raise ValueError("cloud mount remote is malformed")

    @property
    def target(self) -> str:
        return f"{SIDECAR_CLOUD_ROOT}/{self.name}"


def _opener(mount: CloudMountSidecar, image: str) -> dict[str, Any]:
    args = [
        "serve",
        "--socket",
        SOCKET_PATH,
        "--target",
        mount.target,
        "--client-uid",
        str(RCLONE_UID),
    ]
    if mount.read_only:
        args.append("--read-only")
    return {
        "name": OPENER_CONTAINER,
        "image": image,
        # A native sidecar: it starts before, and stops after, everything else.
        "restartPolicy": "Always",
        "args": args,
        "resources": {
            "requests": {"cpu": "5m", "memory": "8Mi"},
            "limits": {"cpu": "100m", "memory": "32Mi"},
        },
        # Bidirectional propagation is admitted only for privileged
        # containers; SYS_ADMIN alone is refused by the API server.
        "securityContext": {"privileged": True},
        "volumeMounts": [
            {
                "name": CLOUD_VOLUME,
                "mountPath": SIDECAR_CLOUD_ROOT,
                "mountPropagation": "Bidirectional",
            },
            {"name": SOCKET_VOLUME, "mountPath": SOCKET_DIR},
        ],
        "startupProbe": {
            "exec": {"command": ["/srw-fuse-opener", "ping", "--socket", SOCKET_PATH]},
            "periodSeconds": 1,
            "failureThreshold": 30,
        },
    }


def _rclone(mount: CloudMountSidecar, image: str) -> dict[str, Any]:
    args = [
        "mount2",
        mount.remote,
        mount.target,
        "--config",
        f"{CREDENTIAL_DIR}/{CREDENTIAL_KEY}",
        "--cache-dir",
        "/tmp/rclone",
        "--allow-other",
        # After a restart the dead mount is still listed; the opener
        # detaches it before it mounts the new one.
        "--allow-non-empty",
        "--uid",
        str(WORKSPACE_UID),
        "--gid",
        str(WORKSPACE_UID),
        "--umask",
        "022",
    ]
    if mount.read_only:
        args.append("--read-only")
    return {
        "name": RCLONE_CONTAINER,
        "image": image,
        "restartPolicy": "Always",
        "command": ["rclone"],
        "args": args,
        "env": [{"name": "HOME", "value": "/tmp"}],
        "resources": {
            "requests": {"cpu": "10m", "memory": "32Mi"},
            "limits": {"cpu": "500m", "memory": "256Mi"},
        },
        "securityContext": {
            "runAsUser": RCLONE_UID,
            "runAsGroup": RCLONE_UID,
            "runAsNonRoot": True,
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "volumeMounts": [
            {
                "name": CLOUD_VOLUME,
                "mountPath": SIDECAR_CLOUD_ROOT,
                "readOnly": True,
                "mountPropagation": "HostToContainer",
            },
            {"name": SOCKET_VOLUME, "mountPath": SOCKET_DIR},
            {"name": CREDENTIAL_VOLUME, "mountPath": CREDENTIAL_DIR, "readOnly": True},
            {"name": RCLONE_TMP_VOLUME, "mountPath": "/tmp"},
        ],
        # The workspace starts only once the mount answers.
        "startupProbe": {
            "exec": {"command": ["srw-fuse-opener", "check", "--target", mount.target]},
            "periodSeconds": 1,
            "failureThreshold": 60,
        },
    }


def add_cloud_mount_sidecars(
    manifest: dict[str, Any],
    mount: CloudMountSidecar,
    images: InPodPlaneImages,
) -> None:
    """Add the sidecar pair and the workspace's view to a workspace Pod manifest."""

    spec = manifest["spec"]
    if spec.get("shareProcessNamespace"):
        # A shared PID namespace would show the workspace rclone's
        # environment and command line.
        raise ValueError(
            "the cloud mount sidecars need a Pod without a shared PID namespace"
        )
    if spec.get("hostPID") or spec.get("hostIPC"):
        raise ValueError("the cloud mount sidecars need a Pod without host namespaces")
    workspace = next(
        container
        for container in spec["containers"]
        if container["name"] == "workspace"
    )
    names = {volume["name"] for volume in spec.get("volumes", [])} | {
        container["name"] for container in spec.get("initContainers", [])
    }
    if names & {
        OPENER_CONTAINER,
        RCLONE_CONTAINER,
        CLOUD_VOLUME,
        SOCKET_VOLUME,
        CREDENTIAL_VOLUME,
        RCLONE_TMP_VOLUME,
    }:
        raise ValueError("the Pod already has cloud mount sidecars")
    spec.setdefault("initContainers", []).extend(
        [_opener(mount, images.opener), _rclone(mount, images.rclone)]
    )
    spec.setdefault("volumes", []).extend(
        [
            {
                "name": CLOUD_VOLUME,
                "emptyDir": {"medium": "Memory", "sizeLimit": "1Mi"},
            },
            {
                "name": SOCKET_VOLUME,
                "emptyDir": {"medium": "Memory", "sizeLimit": "1Mi"},
            },
            {
                "name": CREDENTIAL_VOLUME,
                "secret": {
                    "secretName": mount.secret_name,
                    "items": [{"key": CREDENTIAL_KEY, "path": CREDENTIAL_KEY}],
                    # Only the rclone sidecar mounts it; an fsGroup instead
                    # would add its group to the workspace container too.
                    "defaultMode": 0o444,
                },
            },
            {
                "name": RCLONE_TMP_VOLUME,
                "emptyDir": {"medium": "Memory", "sizeLimit": "64Mi"},
            },
        ]
    )
    workspace.setdefault("volumeMounts", []).append(
        {
            "name": CLOUD_VOLUME,
            "mountPath": WORKSPACE_CLOUD_ROOT,
            "readOnly": True,
            "mountPropagation": "HostToContainer",
        }
    )
