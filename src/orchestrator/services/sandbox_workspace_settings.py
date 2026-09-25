"""Container workspace settings captured by an execution's WorkspaceTemplate.

The execution snapshot is the only source of these settings. ContainerProvisioner
resolves them for every pod creation, restore and stateless generation, so no
call site can drop them. See the Slice A1 spec in the knowledge base
(2026-09-25-slice-a1-container-workspace-templates-design).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import math
import os
from typing import Any, Literal
from uuid import UUID

from orchestrator.services.manifest_execution_snapshot import (
    SRW_ADAPTER,
    object_value,
    read_execution,
    srw_snapshot_config,
)

DEFAULT_WORKSPACE_IMAGE = "ghcr.io/superhuman-remote-worker/srw-workspace:latest"
DEFAULT_PULL_TIMEOUT_SECONDS = 600


def _env_flag(name: str, default: bool) -> bool:
    # Must stay identical to container_provisioner._env_flag (~:388): a
    # deny-list, so WORKSPACE_FUSE_ENABLED/WORKSPACE_FUSE_PRIVILEGED parse the
    # same way here as in the provisioner that actually builds the pod.
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _env_opt_in(name: str, default: bool) -> bool:
    # Fail-closed: unlike _env_flag, only a recognized truthy value enables
    # this. A garbage WORKSPACE_CUSTOM_IMAGES_PRIVILEGED must never grant
    # privilege by accident.
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class SandboxSettings:
    """What a WorkspaceTemplate set for a container; ``None`` means unset."""

    image: str | None = None
    pull_policy: str | None = None
    cpu: float | None = None
    memory: str | None = None
    storage: str | None = None

    @classmethod
    def from_policy(cls, policy: dict) -> "SandboxSettings":
        sandbox = object_value(object_value(policy.get("workspace")).get("sandbox"))
        return cls(
            image=sandbox.get("image"),
            pull_policy=sandbox.get("pull_policy"),
            cpu=sandbox.get("cpu"),
            memory=sandbox.get("memory"),
            storage=sandbox.get("storage"),
        )

    def is_empty(self) -> bool:
        return self == SandboxSettings()


async def resolve_sandbox_settings(
    store: Any, owner_kind: str, owner_id: str
) -> SandboxSettings:
    """Read the owner's frozen settings; historical work has none.

    A missing or non-SRW snapshot means the execution predates templates, so
    the installation defaults apply. Database errors propagate: callers must
    fail closed rather than silently fall back to the default image.
    """
    try:
        UUID(str(owner_id))
    except ValueError:
        return SandboxSettings()
    work_kind = "Session" if owner_kind == "session" else "Job"
    snapshot = await read_execution(store, work_kind, str(owner_id))
    if snapshot is None or snapshot.get("harness_adapter") != SRW_ADAPTER:
        return SandboxSettings()
    _, policy = srw_snapshot_config(snapshot)
    return SandboxSettings.from_policy(policy)


def image_repository(reference: str) -> str:
    """``host/path`` of an image reference, without its tag or digest."""
    name = reference.split("@", 1)[0]
    head, _, last = name.rpartition("/")
    last = last.split(":", 1)[0]
    return f"{head}/{last}" if head else last


@dataclass(frozen=True)
class SandboxImagePolicy:
    """Installation settings that decide a container's image and privilege."""

    default_image: str
    trusted_repositories: frozenset[str]
    custom_images_privileged: bool
    fuse_enabled: bool
    fuse_privileged: bool
    pull_timeout_seconds: int

    @classmethod
    def from_env(cls) -> "SandboxImagePolicy":
        default_image = os.environ.get("WORKSPACE_IMAGE", DEFAULT_WORKSPACE_IMAGE)
        configured = json.loads(
            os.environ.get("WORKSPACE_TRUSTED_IMAGE_REPOSITORIES") or "[]"
        )
        if not isinstance(configured, list) or not all(
            isinstance(item, str) and item for item in configured
        ):
            raise ValueError(
                "WORKSPACE_TRUSTED_IMAGE_REPOSITORIES must be a JSON list of "
                "image repositories."
            )
        fuse_enabled = _env_flag("WORKSPACE_FUSE_ENABLED", True)
        return cls(
            default_image=default_image,
            trusted_repositories=frozenset(
                [image_repository(default_image), *configured]
            ),
            custom_images_privileged=_env_opt_in(
                "WORKSPACE_CUSTOM_IMAGES_PRIVILEGED", False
            ),
            fuse_enabled=fuse_enabled,
            fuse_privileged=fuse_enabled
            and _env_flag("WORKSPACE_FUSE_PRIVILEGED", True),
            pull_timeout_seconds=int(
                os.environ.get(
                    "WORKSPACE_IMAGE_PULL_TIMEOUT_SECONDS",
                    str(DEFAULT_PULL_TIMEOUT_SECONDS),
                )
            ),
        )

    def trusts(self, image: str) -> bool:
        return image_repository(image) in self.trusted_repositories


@dataclass(frozen=True)
class SandboxPodProfile:
    """Physical container inputs. An empty template yields today's values."""

    image: str
    cpu: str
    memory: str
    cpu_limit: str
    memory_limit: str
    pull_policy: str | None
    storage: str | None
    fuse_enabled: bool
    fuse_privileged: bool
    templated: bool

    def plan_extension(self) -> dict | None:
        """Digest fields that exist only when a template set something.

        Keeping them absent otherwise keeps pre-A1 plan digests and pinned
        fingerprints byte-identical, so in-flight creations survive the upgrade.
        """
        if not self.templated:
            return None
        return {"pull_policy": self.pull_policy, "storage": self.storage}


def _millicores(cores: float) -> str:
    # Round away float noise (0.3 * 1000 must be 300, not 301) before ceil.
    return f"{max(1, math.ceil(round(cores * 1000, 6)))}m"


def sandbox_pod_profile(
    settings: SandboxSettings,
    policy: SandboxImagePolicy,
    *,
    cpu: str = "500m",
    memory: str = "1Gi",
    cpu_limit: str = "2000m",
    memory_limit: str = "4Gi",
    image: str | None = None,
) -> SandboxPodProfile:
    """Map a template's single allocation numbers onto requests and limits.

    Memory is reserved (request = limit). CPU is shared: the limit is the
    allocation and the request a quarter of it. Fields the template leaves out
    keep the caller's defaults, which are today's values. A custom image (one
    outside the trusted repositories) runs without FUSE or privilege unless the
    operator allows it, and never with more than the installation grants.
    """
    effective_image = settings.image or image or policy.default_image
    if settings.cpu is not None:
        cpu_limit = _millicores(settings.cpu)
        cpu = _millicores(settings.cpu / 4)
    if settings.memory is not None:
        memory = memory_limit = settings.memory
    full_profile = policy.trusts(effective_image) or policy.custom_images_privileged
    return SandboxPodProfile(
        image=effective_image,
        cpu=cpu,
        memory=memory,
        cpu_limit=cpu_limit,
        memory_limit=memory_limit,
        pull_policy=settings.pull_policy,
        storage=settings.storage,
        fuse_enabled=policy.fuse_enabled and full_profile,
        fuse_privileged=policy.fuse_privileged and full_profile,
        templated=not settings.is_empty(),
    )


async def container_denies_fuse(store: Any, thread: dict) -> bool:
    """Whether a Session's container is a custom image denied /dev/fuse.

    Mirrors the profile ContainerProvisioner renders from the same snapshot and
    installation policy. Threads without an execution snapshot, and trusted
    images, keep today's behaviour.
    """
    if thread.get("execution_harness_adapter") is None:
        return False
    settings = await resolve_sandbox_settings(store, "session", str(thread["id"]))
    if settings.image is None:
        return False
    policy = SandboxImagePolicy.from_env()
    return (
        policy.fuse_enabled and not sandbox_pod_profile(settings, policy).fuse_enabled
    )


_FAIL_AT_ONCE = frozenset({"InvalidImageName"})
_FAIL_AFTER_BUDGET = frozenset(
    {"ErrImagePull", "ImagePullBackOff", "CreateContainerConfigError"}
)
_STILL_PULLING = (
    frozenset({"ContainerCreating", "PodInitializing"}) | _FAIL_AFTER_BUDGET
)


@dataclass(frozen=True)
class PullVerdict:
    state: Literal["ok", "pulling", "failed"]
    message: str | None = None


def classify_image_pull(
    pod: Any, *, image: str, now: datetime, pull_timeout_seconds: float
) -> PullVerdict:
    """Classify the workspace container's waiting state for a custom image."""
    statuses = getattr(getattr(pod, "status", None), "container_statuses", None) or []
    workspace = next((s for s in statuses if s.name == "workspace"), None)
    state = getattr(workspace, "state", None)
    waiting = getattr(state, "waiting", None)
    if waiting is None:
        return PullVerdict("ok")
    reason = waiting.reason or ""
    message = f"Workspace image {image} could not be pulled: {reason}"
    if waiting.message:
        message += f" ({waiting.message})"
    if reason in _FAIL_AT_ONCE:
        return PullVerdict("failed", message)
    if reason not in _STILL_PULLING:
        return PullVerdict("ok")
    created = getattr(pod.metadata, "creation_timestamp", None)
    if (
        reason in _FAIL_AFTER_BUDGET
        and created is not None
        and (now - created).total_seconds() >= pull_timeout_seconds
    ):
        return PullVerdict("failed", message)
    return PullVerdict("pulling")


def pod_admission_rejection(exc: BaseException) -> str | None:
    """Describe a 403 from pod admission (ResourceQuota, LimitRange, policy)."""
    if getattr(exc, "status", None) != 403:
        return None
    body = getattr(exc, "body", "") or ""
    try:
        detail = json.loads(body).get("message") or str(exc)
    except (TypeError, ValueError, AttributeError):
        detail = str(exc)
    return f"Workspace pod was rejected by the cluster: {detail}"
