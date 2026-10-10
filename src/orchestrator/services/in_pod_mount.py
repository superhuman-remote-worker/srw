"""The in-pod plane's cloud mount sidecars (connector drivers D7).

Two native sidecars give a workspace Pod FUSE mounts whose daemons and
credentials live outside the workspace container:

- ``srw-fuse-opener`` is privileged and the only privileged code. It opens
  /dev/fuse, mounts it at each of its targets inside a memory emptyDir it
  shares with Bidirectional propagation, and passes the descriptor over a
  unix socket in a second emptyDir, which the rclone sidecar mounts
  read-only and the workspace not at all. It opens no network listener and
  never reads what the filesystem serves. It forces nosuid, nodev,
  default_permissions and, for a read-only target, ro; on SIGTERM it
  detaches every target so the kubelet can tear the emptyDir down. Its
  startup probe only waits for its own socket: the targets and directories
  exist before anything else in the Pod starts.
- ``srw-cloud-mount`` runs the supervisor (drivers/cloud-mount) unprivileged
  (uid 65534, every capability dropped, read-only root filesystem, no
  /dev/fuse): one rclone mount2 per mount, asking the opener through the
  fusermount3 client its image carries. It alone mounts the credential
  Secret (an rclone config file, never the environment) and the plan
  ConfigMap. It has no probe: a mount that does not come up never holds the
  workspace back, it is reported in a status file. Its remote controls
  listen on unix sockets in its private /tmp, never on a TCP port: the
  containers of a Pod share localhost.

The workspace container mounts the shared emptyDir read-only with
HostToContainer propagation and sees each mount at ``/cloud/<name>``, the
status files read-only at ``/srw/cloud-status``, and a small writable
``/srw/cloud-control`` where it may ask for a drain or a refresh. The opener
starts first and stops last, so a mount whose rclone died is still detached
at shutdown.

The workspace container must not be privileged or hold CAP_SYS_ADMIN, or
root in it could remount a read-only mount read-write, unmount it and reach
the node; the builder refuses such a workspace. The one exception is a
protected Pod: its capture overlay (fuse-overlayfs) still runs in the
workspace, so it keeps the FUSE profile, and its read-only lower layer
rests on the reader credential (SRW assists read-only, never guarantees
it). The overlay mounts on ``/cloud/merged``, a plain directory the opener
creates beside the lower and hands to agent-host, so a protected
workspace's view of the emptyDir is writable: a non-root fusermount3 needs
write access to its mountpoint, and a volume nested there would itself be a
mountpoint, which the overlay's scripts read as an overlay already up.
Design: knowledge-base/knowledge/features/connector_drivers.md ("Three
planes", D7) and connector_drivers_research/connector_drivers_d7_in_pod_plane_spike.md.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from typing import Any

OPENER_CONTAINER = "srw-fuse-opener"
RCLONE_CONTAINER = "srw-cloud-mount"
CLOUD_VOLUME = "srw-cloud"
SOCKET_VOLUME = "srw-fuse-socket"
STATUS_VOLUME = "srw-cloud-status"
CONTROL_VOLUME = "srw-cloud-control"
PLAN_VOLUME = "srw-cloud-plan"
CREDENTIAL_VOLUME = "srw-cloud-credential"
CACHE_VOLUME = "srw-cloud-cache"
RCLONE_TMP_VOLUME = "srw-cloud-tmp"
SIDECAR_VOLUMES = frozenset(
    {
        CLOUD_VOLUME,
        SOCKET_VOLUME,
        STATUS_VOLUME,
        CONTROL_VOLUME,
        PLAN_VOLUME,
        CREDENTIAL_VOLUME,
        CACHE_VOLUME,
        RCLONE_TMP_VOLUME,
    }
)

#: Where the workspace sees its cloud folders.
WORKSPACE_CLOUD_ROOT = "/cloud"
#: The same emptyDir in the sidecars.
SIDECAR_CLOUD_ROOT = "/srw/cloud"
#: Each mount's status file, ``<index>.json``; the workspace's view is
#: read-only.
STATUS_DIR = "/srw/cloud-status"
#: Where the workspace drops a ``drain`` or ``refresh`` request.
CONTROL_DIR = "/srw/cloud-control"
#: The opener client's compiled-in default; go-fuse runs fusermount3 with
#: _FUSE_COMMFD as its only variable, so the path cannot come from env.
SOCKET_DIR = "/run/srw-fuse"
SOCKET_PATH = f"{SOCKET_DIR}/opener.sock"
PLAN_DIR = "/etc/srw-cloud-plan"
PLAN_KEY = "plan.json"
CREDENTIAL_DIR = "/etc/srw-cloud"
#: The Secret key the supervisor reads: one rclone config, a section per
#: mount, passwords obscured as rclone requires.
CREDENTIAL_KEY = "rclone.conf"
CACHE_DIR = "/srw/cloud-cache"
RCLONE_UID = 65534
#: agent-host in the workspace image owns what the mounts show.
WORKSPACE_UID = 1000
#: The workspace Pod's terminationGracePeriodSeconds. The workspace stops
#: first, then the supervisor drains and stops its rclones, then the opener
#: detaches: the drain must leave room for the rest.
WORKSPACE_GRACE_SECONDS = 120
MAX_DRAIN_SECONDS = 90
#: The shortest grace a sidecar Pod is deleted with: the opener needs a
#: moment to detach every mount, or the kubelet cannot tear down the
#: emptyDir they sit in and the Pod hangs Terminating.
SIDECAR_DELETE_GRACE_SECONDS = 5
#: The supervisor's memory limit when the chart leaves it automatic: a base
#: and a share per rclone, capped.
SUPERVISOR_MEMORY_BASE_MI = 256
SUPERVISOR_MEMORY_PER_MOUNT_MI = 192
SUPERVISOR_MEMORY_CAP_MI = 2048

_QUANTITY = re.compile(r"[0-9]+(Ki|Mi|Gi|Ti|K|M|G|T)?")


@dataclass(frozen=True)
class InPodPlaneSettings:
    """The plane's chart settings; ``None`` from :meth:`from_env` when off."""

    opener_image: str
    rclone_image: str
    cache_size: str = "10Gi"
    drain_seconds: int = 60
    max_mounts: int = 8
    #: The supervisor's memory limit; ``None`` scales it with the mounts.
    supervisor_memory: str | None = None

    @classmethod
    def from_env(cls) -> "InPodPlaneSettings | None":
        opener = os.environ.get("CONNECTOR_IN_POD_OPENER_IMAGE", "").strip()
        rclone = os.environ.get("CONNECTOR_IN_POD_RCLONE_IMAGE", "").strip()
        if not opener or not rclone:
            return None
        cache_size = os.environ.get("CONNECTOR_IN_POD_CACHE_SIZE", "").strip() or "10Gi"
        if not _QUANTITY.fullmatch(cache_size):
            cache_size = "10Gi"
        memory = os.environ.get("CONNECTOR_IN_POD_SUPERVISOR_MEMORY", "").strip()
        return cls(
            opener_image=opener,
            rclone_image=rclone,
            cache_size=cache_size,
            drain_seconds=_bounded_int(
                "CONNECTOR_IN_POD_DRAIN_SECONDS", 60, 0, MAX_DRAIN_SECONDS
            ),
            max_mounts=_bounded_int("CONNECTOR_IN_POD_MAX_MOUNTS", 8, 1, 16),
            supervisor_memory=memory if _QUANTITY.fullmatch(memory) else None,
        )

    def supervisor_memory_for(self, mounts: int) -> str:
        """The supervisor's memory limit for a Pod with ``mounts`` rclones."""
        if self.supervisor_memory:
            return self.supervisor_memory
        mebibytes = SUPERVISOR_MEMORY_BASE_MI + SUPERVISOR_MEMORY_PER_MOUNT_MI * max(
            1, mounts
        )
        return f"{min(SUPERVISOR_MEMORY_CAP_MI, mebibytes)}Mi"


def _bounded_int(name: str, default: int, low: int, high: int) -> int:
    try:
        value = int(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default
    return max(low, min(high, value))


@dataclass(frozen=True)
class SidecarSpec:
    """What the builder needs from a cloud mount plan.

    ``targets`` are ``(path, read_only)`` under :data:`SIDECAR_CLOUD_ROOT`;
    ``dirs`` are plain directories the opener creates there for the
    workspace user (the protected overlay's merged mountpoint, so a protected
    spec only); ``objects_name`` names the plan ConfigMap and the credential
    Secret, which the Pod owns; ``protected`` keeps the workspace's FUSE
    profile for the overlay.
    """

    targets: tuple[tuple[str, bool], ...]
    objects_name: str
    dirs: tuple[str, ...] = ()
    protected: bool = False


def objects_name_for(pod_name: str, attempt: str | None) -> str:
    """The plan ConfigMap's and credential Secret's name for one creation
    attempt of a Pod.

    The Pod's name is the same for every creation of a session's workspace,
    and the previous Pod's objects can still await garbage collection when
    its successor is created, so the attempt names its own.
    """
    if not attempt:
        return f"{pod_name}-cloud"
    return f"{pod_name}-cloud-{hashlib.sha256(attempt.encode()).hexdigest()[:10]}"


def _field(obj: Any, snake: str, camel: str | None = None) -> Any:
    """A Kubernetes model attribute, from a client object or a plain dict."""
    if isinstance(obj, dict):
        return obj.get(camel or snake)
    return getattr(obj, snake, None)


def pod_has_cloud_mount_sidecars(pod: Any) -> bool:
    """Whether a Pod (client object or dict) runs the cloud mount opener."""
    containers = _field(_field(pod, "spec"), "init_containers", "initContainers")
    return any(_field(c, "name") == OPENER_CONTAINER for c in containers or ())


def objects_name_from_pod(pod: Any) -> str | None:
    """The credential Secret a created Pod references, or ``None``."""
    for volume in _field(_field(pod, "spec"), "volumes") or ():
        if _field(volume, "name") == CREDENTIAL_VOLUME:
            name = _field(_field(volume, "secret"), "secret_name", "secretName")
            return str(name) if name else None
    return None


def _opener(spec: SidecarSpec, image: str) -> dict[str, Any]:
    args = ["serve", "--socket", SOCKET_PATH, "--client-uid", str(RCLONE_UID)]
    for target, read_only in spec.targets:
        args += ["--target", f"{target}:ro" if read_only else target]
    for directory in spec.dirs:
        args += ["--dir", directory]
    if spec.dirs:
        args += ["--dir-uid", str(WORKSPACE_UID)]
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
        # Local only: its targets and directories exist and its socket
        # answers. Nothing remote is waited for here.
        "startupProbe": {
            "exec": {"command": ["/srw-fuse-opener", "ping", "--socket", SOCKET_PATH]},
            "periodSeconds": 1,
            "failureThreshold": 30,
        },
    }


def _supervisor(image: str, memory: str) -> dict[str, Any]:
    return {
        "name": RCLONE_CONTAINER,
        "image": image,
        "restartPolicy": "Always",
        "command": ["srw-cloud-mount"],
        "args": [
            "--plan",
            f"{PLAN_DIR}/{PLAN_KEY}",
            "--config",
            f"{CREDENTIAL_DIR}/{CREDENTIAL_KEY}",
            "--status-dir",
            STATUS_DIR,
            "--control-dir",
            CONTROL_DIR,
            "--cache-dir",
            CACHE_DIR,
            "--run-dir",
            "/tmp/srw-cloud-mount",
        ],
        "env": [{"name": "HOME", "value": "/tmp"}],
        # One rclone per mount, each with its VFS cache's directory tree in
        # memory: the limit scales with the mounts (connectors.inPodPlane.
        # supervisorMemory overrides it). An OOM kill restarts only the
        # supervisor, never the workspace.
        "resources": {
            "requests": {"cpu": "20m", "memory": "64Mi"},
            "limits": {"cpu": "1000m", "memory": memory},
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
            # connect(2) needs no write access to the directory; read-only
            # leaves only the opener able to place anything there.
            {"name": SOCKET_VOLUME, "mountPath": SOCKET_DIR, "readOnly": True},
            {"name": PLAN_VOLUME, "mountPath": PLAN_DIR, "readOnly": True},
            {"name": CREDENTIAL_VOLUME, "mountPath": CREDENTIAL_DIR, "readOnly": True},
            {"name": STATUS_VOLUME, "mountPath": STATUS_DIR},
            {"name": CONTROL_VOLUME, "mountPath": CONTROL_DIR, "readOnly": True},
            {"name": CACHE_VOLUME, "mountPath": CACHE_DIR},
            {"name": RCLONE_TMP_VOLUME, "mountPath": "/tmp"},
        ],
    }


def workspace_can_mount(container: dict[str, Any]) -> bool:
    """Whether a workspace container could remount or unmount a cloud mount."""
    context = container.get("securityContext") or {}
    added = (context.get("capabilities") or {}).get("add") or []
    return bool(context.get("privileged")) or any(
        str(capability).upper().removeprefix("CAP_") == "SYS_ADMIN"
        for capability in added
    )


def _memory_dir() -> dict[str, Any]:
    return {"emptyDir": {"medium": "Memory", "sizeLimit": "1Mi"}}


def add_cloud_mount_sidecars(
    manifest: dict[str, Any],
    spec: SidecarSpec,
    settings: InPodPlaneSettings,
) -> None:
    """Add the sidecar pair and the workspace's views to a workspace Pod."""

    pod_spec = manifest["spec"]
    if not spec.targets:
        raise ValueError("a cloud mount sidecar needs at least one mount")
    if len(spec.dirs) > 1:
        raise ValueError("the cloud mount sidecars create at most one directory")
    if spec.dirs and not spec.protected:
        # A directory makes the workspace's view writable, which only a
        # protected Pod (FUSE in the workspace already) may have.
        raise ValueError("only a protected Pod's sidecars create a directory")
    if pod_spec.get("shareProcessNamespace"):
        # A shared PID namespace would show the supervisor's environment and
        # command line, and its rclone children's.
        raise ValueError(
            "the cloud mount sidecars need a Pod without a shared PID namespace"
        )
    if pod_spec.get("hostPID") or pod_spec.get("hostIPC"):
        raise ValueError("the cloud mount sidecars need a Pod without host namespaces")
    workspace = next(
        container
        for container in pod_spec["containers"]
        if container["name"] == "workspace"
    )
    if workspace_can_mount(workspace) and not spec.protected:
        raise ValueError(
            "the cloud mount sidecars need a workspace without privilege or "
            "SYS_ADMIN unless the Pod is protected"
        )
    names = {volume["name"] for volume in pod_spec.get("volumes", [])} | {
        container["name"] for container in pod_spec.get("initContainers", [])
    }
    if names & (SIDECAR_VOLUMES | {OPENER_CONTAINER, RCLONE_CONTAINER}):
        raise ValueError("the Pod already has cloud mount sidecars")
    pod_spec.setdefault("initContainers", []).extend(
        [
            _opener(spec, settings.opener_image),
            _supervisor(
                settings.rclone_image,
                settings.supervisor_memory_for(len(spec.targets)),
            ),
        ]
    )
    volumes = [
        {"name": CLOUD_VOLUME, **_memory_dir()},
        {"name": SOCKET_VOLUME, **_memory_dir()},
        {"name": STATUS_VOLUME, **_memory_dir()},
        {"name": CONTROL_VOLUME, **_memory_dir()},
        {
            "name": PLAN_VOLUME,
            "configMap": {
                "name": spec.objects_name,
                "items": [{"key": PLAN_KEY, "path": PLAN_KEY}],
                "defaultMode": 0o444,
            },
        },
        {
            "name": CREDENTIAL_VOLUME,
            "secret": {
                "secretName": spec.objects_name,
                "items": [{"key": CREDENTIAL_KEY, "path": CREDENTIAL_KEY}],
                # Only the supervisor mounts it; an fsGroup instead would add
                # its group to the workspace container too.
                "defaultMode": 0o444,
            },
        },
        # No sizeLimit: the kubelet evicts the whole Pod when an emptyDir
        # passes its limit, and rclone's cache cap is soft (a file being
        # written or read past it stays). A cloud folder must never take the
        # workspace down; rclone keeps the caches under cacheSize together.
        {"name": CACHE_VOLUME, "emptyDir": {}},
        {
            "name": RCLONE_TMP_VOLUME,
            "emptyDir": {"medium": "Memory", "sizeLimit": "64Mi"},
        },
    ]
    cloud_view: dict[str, Any] = {
        "name": CLOUD_VOLUME,
        "mountPath": WORKSPACE_CLOUD_ROOT,
        "readOnly": True,
        "mountPropagation": "HostToContainer",
    }
    if spec.dirs:
        # The protected overlay mounts on the opener's directory itself
        # (see the module docstring); a privileged workspace was never held
        # back by a read-only view anyway.
        del cloud_view["readOnly"]
    workspace_mounts = [
        cloud_view,
        {"name": STATUS_VOLUME, "mountPath": STATUS_DIR, "readOnly": True},
        {"name": CONTROL_VOLUME, "mountPath": CONTROL_DIR},
    ]
    pod_spec.setdefault("volumes", []).extend(volumes)
    workspace.setdefault("volumeMounts", []).extend(workspace_mounts)
