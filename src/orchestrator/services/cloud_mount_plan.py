"""The cloud mounts a session's workspace Pod gets from the in-pod plane (D7).

A session container Pod's cloud folders are fixed when the Pod is created:
mounts are set up only when a session starts (owner, 2026-10-09). This
module resolves them for :mod:`container_provisioner`, as a plan with two
halves:

* the **non-secret plan** (:meth:`CloudMountPlan.recorded`): names, sources,
  access, flags, what was left out and why. It joins the creation digest and
  the pinned fingerprint, is annotated on the Pod (the one source of truth a
  re-attach reads back, see :func:`recorded_plan_from_annotations`), goes to
  the supervisor as its ConfigMap, and is what the agent and the cockpit see;
* the **credentials** (:meth:`CloudMountPlan.rclone_config`): one rclone
  config file, a section per mount, passwords obscured as rclone requires.
  It reaches only the Pod's Secret, which only the supervisor mounts; never
  an environment variable (rclone at debug level logs those), never the
  workspace, never a log line here.

The plan uses the in-pod plane wholesale or not at all. A Pod's ``/cloud``
is the sidecar volume, read-only to the workspace, so the old in-workspace
rclone cannot add a mount beside it. :func:`resolve_cloud_mount_plan`
returns ``None`` (the old path, unchanged) when the plane is off, for an
officer, for a malformed protected marker, and when any mount of the set
needs more than a static password: OpenCloud's bearer tokens are refreshed
by the agent into the workspace and stay there until the sidecar has a
token helper. VM, lite-tier and job workspaces never reach this module.

Which folders a session gets is still today's rule (``_resolve_live_mount_set``:
every ``thread_mounts`` row, or the session folder when one cannot be
built); main-cloud slice 3 replaces it with per-connector binding. What D7
adds is that the rows the rule left out are named, with a closed reason,
instead of disappearing.

Design: knowledge-base/knowledge/features/connector_drivers.md (D7, decisions
23 and 18) and main_cloud_as_connectors.md.
"""

from __future__ import annotations

import base64
import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from orchestrator.services import agent_cloud_mounts
from orchestrator.services.cloud_mount_sidecar import (
    PLAN_ANNOTATION,
    PLAN_VERSION,
    plan_fingerprint,
)
from orchestrator.services.in_pod_mount import (
    SIDECAR_CLOUD_ROOT,
    WORKSPACE_CLOUD_ROOT,
    WORKSPACE_UID,
    InPodPlaneSettings,
    SidecarSpec,
)
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object

# rclone's fixed obscuring key (lib/obscure). Obscuring is reversible by
# design: it only keeps a password from being read at a glance. The file it
# protects is the Secret, readable by the supervisor alone.
_OBSCURE_KEY = bytes(
    [
        0x9C, 0x93, 0x5B, 0x48, 0x73, 0x0A, 0x55, 0x4D,
        0x6B, 0xFD, 0x7C, 0x63, 0xC8, 0x86, 0xA9, 0x2B,
        0xD3, 0x90, 0x19, 0x8E, 0xB8, 0x12, 0x8A, 0xFB,
        0xF4, 0xDE, 0x16, 0x2B, 0x8B, 0x95, 0xF6, 0x38,
    ]
)  # fmt: skip


def rclone_obscure(password: str, *, iv: bytes | None = None) -> str:
    """What ``rclone obscure`` prints for ``password``."""
    iv = os.urandom(16) if iv is None else iv
    encryptor = Cipher(algorithms.AES(_OBSCURE_KEY), modes.CTR(iv)).encryptor()
    body = encryptor.update(password.encode("utf-8")) + encryptor.finalize()
    return base64.urlsafe_b64encode(iv + body).decode("ascii").rstrip("=")


def rclone_reveal(obscured: str) -> str:
    """What ``rclone reveal`` prints for ``obscured``."""
    raw = base64.urlsafe_b64decode(obscured + "=" * (-len(obscured) % 4))
    decryptor = Cipher(algorithms.AES(_OBSCURE_KEY), modes.CTR(raw[:16])).decryptor()
    return (decryptor.update(raw[16:]) + decryptor.finalize()).decode("utf-8")


# The flags the supervisor accepts from a plan (drivers/cloud-mount/plan.go,
# flagRE); a provider flag outside them keeps the Pod on the old path.
_FLAG = re.compile(
    r"--(vfs-[a-z-]+|dir-cache-time|poll-interval|buffer-size|attr-timeout"
    r"|transfers|checkers|timeout|contimeout|low-level-retries|webdav-[a-z-]+"
    r"|no-modtime|no-checksum)(=[^\x00-\x1f\x7f]*)?"
)
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")
#: The non-secret source keys a sidecar mount may carry.
_SOURCE_KEYS = ("url", "vendor", "user")
_CACHE_FLAGS = {
    "vfs_cache_mode": "--vfs-cache-mode",
    "vfs_cache_max_age": "--vfs-cache-max-age",
    "dir_cache_time": "--dir-cache-time",
    "poll_interval": "--poll-interval",
    "vfs_read_chunk_size": "--vfs-read-chunk-size",
    "vfs_read_chunk_size_limit": "--vfs-read-chunk-size-limit",
}
_DEFAULT_CACHE = {
    "vfs_cache_mode": "full",
    "vfs_cache_max_age": "24h",
    "dir_cache_time": "5m",
    "poll_interval": "1m",
    "vfs_read_chunk_size": "16M",
    "vfs_read_chunk_size_limit": "128M",
}
_QUANTITY_UNITS = {
    "": 1,
    "K": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
}


def _quantity_bytes(quantity: str) -> int:
    match = re.fullmatch(r"([0-9]+)(Ki|Mi|Gi|Ti|K|M|G|T)?", quantity)
    if not match:
        raise ValueError(f"not a size: {quantity!r}")
    return int(match.group(1)) * _QUANTITY_UNITS[match.group(2) or ""]


@dataclass(frozen=True)
class SidecarMount:
    """One mount of the plan, without its password."""

    index: int
    name: str
    mount_id: str
    mount_kind: str
    source_ref: str | None
    backend: str
    access: str
    source_type: str
    source_config: tuple[tuple[str, str], ...]
    root: str
    flags: tuple[str, ...]

    @property
    def target(self) -> str:
        return f"{SIDECAR_CLOUD_ROOT}/{self.name}"

    @property
    def target_path(self) -> str:
        return f"{WORKSPACE_CLOUD_ROOT}/{self.name}"

    @property
    def remote(self) -> str:
        return f"m{self.index}:{self.root}"

    def recorded(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "name": self.name,
            "mount_id": self.mount_id,
            "mount_kind": self.mount_kind,
            "source_ref": self.source_ref,
            "backend": self.backend,
            "access": self.access,
            "target_path": self.target_path,
            "remote": {
                "type": self.source_type,
                **dict(self.source_config),
                "root": self.root,
            },
            "flags": list(self.flags),
        }


@dataclass(frozen=True)
class CloudMountPlan:
    """A session Pod's cloud mounts; ``passwords`` never leave via repr/eq."""

    mounts: tuple[SidecarMount, ...]
    excluded: tuple[Mapping[str, str], ...]
    drain_seconds: int
    cache_size: str
    passwords: Mapping[int, str] = field(
        default_factory=dict, repr=False, compare=False
    )
    protected: bool = False
    overlay: Mapping[str, Any] | None = None

    def recorded(self) -> dict[str, Any]:
        """The non-secret plan, with its fingerprint."""
        body = {
            "version": PLAN_VERSION,
            "delivery": "sidecar",
            "mounts": [mount.recorded() for mount in self.mounts],
            "excluded": [dict(entry) for entry in self.excluded],
            "drain_seconds": self.drain_seconds,
            "cache_size": self.cache_size,
            "protected": self.protected,
            "overlay": dict(self.overlay) if self.overlay else None,
        }
        return {**body, "fingerprint": plan_fingerprint(body)}

    def digest_input(self, settings: InPodPlaneSettings) -> dict[str, Any]:
        """What the creation digest and the pinned fingerprint cover: the
        recorded plan and the two sidecar images (a new image is a new Pod)."""
        return {
            "plan": self.recorded(),
            "images": {
                "opener": settings.opener_image,
                "rclone": settings.rclone_image,
            },
        }

    def sidecar_spec(self, objects_name: str) -> SidecarSpec:
        dirs: tuple[str, ...] = ()
        if self.protected and self.overlay:
            merged = str(self.overlay.get("merged") or "")
            dirs = (SIDECAR_CLOUD_ROOT + merged.removeprefix(WORKSPACE_CLOUD_ROOT),)
        return SidecarSpec(
            targets=tuple(
                (mount.target, mount.access == "read_only") for mount in self.mounts
            ),
            objects_name=objects_name,
            dirs=dirs,
            protected=self.protected,
        )

    def supervisor_plan(self) -> dict[str, Any]:
        """The supervisor's plan.json (drivers/cloud-mount/plan.go)."""
        return {
            "version": 1,
            "uid": WORKSPACE_UID,
            "gid": WORKSPACE_UID,
            "drain_seconds": self.drain_seconds,
            "mounts": [
                {
                    "index": mount.index,
                    "name": mount.name,
                    "remote": mount.remote,
                    "target": mount.target,
                    "read_only": mount.access == "read_only",
                    "flags": list(mount.flags),
                    "ignore": [],
                    "cloudignore": True,
                }
                for mount in self.mounts
            ],
        }

    def rclone_config(self) -> str:
        """The credential file. The only place a password is written."""
        sections: list[str] = []
        for mount in self.mounts:
            password = self.passwords.get(mount.index)
            if not password:
                raise ValueError(f"cloud mount {mount.index} has no password")
            lines = [f"[m{mount.index}]", f"type = {mount.source_type}"]
            lines += [f"{key} = {value}" for key, value in mount.source_config]
            lines.append(f"pass = {rclone_obscure(password)}")
            sections.append("\n".join(lines))
        return "\n\n".join(sections) + "\n"


def _clean(value: Any) -> str | None:
    text = str(value)
    return None if _CONTROL.search(text) else text


def _cache_flags(cache: Mapping[str, Any], max_size_bytes: int) -> list[str] | None:
    merged = {**_DEFAULT_CACHE, **{k: v for k, v in cache.items() if k in _CACHE_FLAGS}}
    flags: list[str] = []
    for key, flag in _CACHE_FLAGS.items():
        value = _clean(merged[key])
        if value is None:
            return None
        flags += [flag, value]
    # Every mount's cache shares one emptyDir; its sizeLimit evicts the whole
    # Pod, so rclone's (soft) cap stays at half of it.
    flags += ["--vfs-cache-max-size", f"{max(1, max_size_bytes // 1024**2)}M"]
    return flags


def _provider_flags(flags: Any) -> list[str] | None:
    out: list[str] = []
    value_allowed = False
    for raw in flags or []:
        arg = _clean(raw)
        if arg is None:
            return None
        if arg.startswith("-"):
            if not _FLAG.fullmatch(arg):
                return None
            value_allowed = "=" not in arg
        elif not value_allowed:
            return None
        else:
            value_allowed = False
        out.append(arg)
    return out


def _sidecar_mount(
    index: int, entry: Mapping[str, Any], *, cache_bytes: int
) -> tuple[SidecarMount, str] | None:
    """A built rclone mount as a sidecar mount and its password, or ``None``
    when it needs more than a static password (a bearer token, say)."""
    source = entry.get("source") or {}
    auth = entry.get("auth") or {}
    config = source.get("config") or {}
    password = auth.get("password")
    if (
        source.get("type") != "webdav"
        or auth.get("type") != "basic"
        or not isinstance(password, str)
        or not password
        or _CONTROL.search(password)
        or entry.get("min_rclone_version")
        or not isinstance(config, Mapping)
        or set(config) - set(_SOURCE_KEYS)
        or not config.get("url")
    ):
        return None
    source_config: list[tuple[str, str]] = []
    for key in _SOURCE_KEYS:
        if key in config:
            value = _clean(config[key])
            if value is None or not value:
                return None
            source_config.append((key, value))
    root = _clean(source.get("root") or "")
    name = _clean(entry.get("workspace_name") or "")
    cache = _cache_flags(entry.get("cache") or {}, cache_bytes)
    provider = _provider_flags(entry.get("provider_flags"))
    if (
        root is None
        or not name
        or "/" in name
        or name.startswith(".")
        or cache is None
        or provider is None
    ):
        return None
    access = "read_only" if entry.get("access") == "read_only" else "read_write"
    mount = SidecarMount(
        index=index,
        name=name,
        mount_id=str(entry.get("mount_id") or name),
        mount_kind=str(entry.get("mount_kind") or "project"),
        source_ref=str(entry["source_ref"]) if entry.get("source_ref") else None,
        backend=str(entry.get("backend") or ""),
        access=access,
        source_type="webdav",
        source_config=tuple(source_config),
        root=root,
        flags=tuple(cache + provider),
    )
    return mount, password


def _container_rclone_allowed() -> bool:
    allow = os.getenv("CLOUD_RCLONE_ALLOW_CONTAINER", "true").lower()
    return allow not in {"0", "false", "no", "off"}


def _officer(metadata: Mapping[str, Any]) -> bool:
    override = metadata.get("config_override")
    if isinstance(override, str):
        try:
            override = json.loads(override)
        except ValueError:
            return False
    return isinstance(override, Mapping) and bool(override.get("officer"))


async def resolve_cloud_mount_plan(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    settings: InPodPlaneSettings | None,
    dependencies: agent_cloud_mounts.AgentCloudMountDependencies,
) -> CloudMountPlan | None:
    """The plan for a new session container Pod, or ``None`` for the old path.

    An empty plan (no mounts) still means the plane: nothing will ever mount
    in that Pod, so its workspace needs no FUSE either.
    """
    if settings is None or dependencies.cloud_workspace_driver() != "rclone_mount":
        return None
    if not _container_rclone_allowed():
        return None
    metadata = thread_metadata_object(thread)
    if _officer(metadata):
        return None
    if protected_cloud_marker_state(metadata) != "off":
        return None

    live = await agent_cloud_mounts._resolve_live_mount_set(
        thread,
        mount_rows=mount_rows,
        runtime_is_vm=False,
        dependencies=dependencies,
    )
    kept = live.mounts[: settings.max_mounts]
    excluded: list[dict[str, str]] = [
        {key: str(value) for key, value in entry.items()} for entry in live.excluded
    ]
    for entry in live.mounts[settings.max_mounts :]:
        excluded.append(
            {
                "source_ref": str(
                    entry.get("source_ref") or entry.get("mount_id") or ""
                ),
                "mount_kind": str(entry.get("mount_kind") or "project"),
                "reason": "too_many_mounts",
            }
        )
    cache_bytes = _quantity_bytes(settings.cache_size) // 2 // max(1, len(kept))
    mounts: list[SidecarMount] = []
    passwords: dict[int, str] = {}
    for index, entry in enumerate(kept):
        built = _sidecar_mount(index, entry, cache_bytes=cache_bytes)
        if built is None:
            return None
        mounts.append(built[0])
        passwords[index] = built[1]
    return CloudMountPlan(
        mounts=tuple(mounts),
        excluded=tuple(excluded),
        drain_seconds=settings.drain_seconds,
        cache_size=settings.cache_size,
        passwords=passwords,
    )


def plan_annotation(plan: CloudMountPlan) -> str:
    return json.dumps(plan.recorded(), sort_keys=True, separators=(",", ":"))


__all__ = [
    "PLAN_ANNOTATION",
    "CloudMountPlan",
    "SidecarMount",
    "plan_annotation",
    "rclone_obscure",
    "rclone_reveal",
    "resolve_cloud_mount_plan",
]
