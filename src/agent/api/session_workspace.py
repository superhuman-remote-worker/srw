"""Workspace readiness and identity fences for session attachment.

Pure validation of the orchestrator's workspace responses -- the protected
cloud's three-way delivery state and its exact sandbox identity, the delivered
workspace generation/incarnation pair and the fences that bind every observed
response to it -- plus the readiness poll that waits for a container or a VM.
Attachment is the primary consumer; live tier upgrades reuse the poll.

This module holds no state, does not import the runtime that composes it, an
application factory, the loop or the worker graph (import contract).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

from agent.api.session_contract import ProtectedCloudUnavailable, WorkspaceNotReady
from agent.api.session_identity import (
    canonical_runtime_generation,
    pinned_runtime_generation_advertised,
)
from shared.session_subagent_batch import (
    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY,
    SESSION_SUBAGENT_FANOUT_KEY,
)

# Records keep the runtime's logger name (the moved code logged there).
logger = logging.getLogger("agent.api.persistent_app")

# How long an attach or live VM upgrade waits for a KubeVirt VM once the poll
# observes one in flight. A cold VM pays a fresh multi-GB DataVolume import,
# which routinely exceeds the sandbox-container budget.
VM_WORKSPACE_POLL_TIMEOUT_S: int = int(os.environ.get("VM_UPGRADE_POLL_TIMEOUT", "900"))


PROTECTED_CLOUD_SAFE_ERROR_CODES = frozenset(
    {
        "feature_disabled",
        "malformed_protected_marker",
        "unsupported_workspace_tier",
        "no_protected_mount",
        "engage_refused",
        "engage_failed",
    }
)


def protected_workspace_marker(payload: Dict[str, Any]) -> str:
    """Classify the additive workspace marker without truthiness coercion."""

    if "protected_cloud" not in payload or payload.get("protected_cloud") is False:
        return "off"
    if payload.get("protected_cloud") is True:
        return "on"
    raise ProtectedCloudUnavailable("malformed protected-cloud workspace marker")


def validate_protected_cloud_mount(payload: Any) -> Dict[str, Any]:
    """Return an exact protected lower+overlay payload or fail closed."""

    if not isinstance(payload, dict):
        raise ProtectedCloudUnavailable("protected-cloud mount payload is missing")
    overlay = payload.get("overlay")
    mounts = payload.get("mounts")
    if (
        type(payload.get("version")) is not int
        or payload.get("version") != 1
        or payload.get("driver") != "rclone"
        or payload.get("protected") is not True
        or payload.get("skip_workspace_links") is not True
        or payload.get("fallback") is not False
        or not isinstance(overlay, dict)
        or overlay.get("lower") != "/cloud/lower"
        or overlay.get("merged") != "/cloud/merged"
        or overlay.get("upper") != "/home/agent-host/.overlay/upper"
        or overlay.get("work") != "/home/agent-host/.overlay/work"
        or not isinstance(overlay.get("quota_bytes"), int)
        or isinstance(overlay.get("quota_bytes"), bool)
        or overlay.get("quota_bytes") <= 0
        or not isinstance(mounts, list)
        or len(mounts) != 1
    ):
        raise ProtectedCloudUnavailable("protected-cloud mount payload is malformed")

    lowers = [
        mount
        for mount in mounts
        if isinstance(mount, dict) and mount.get("mount_kind") == "protected_lower"
    ]
    if len(lowers) != 1:
        raise ProtectedCloudUnavailable(
            "protected-cloud payload must contain exactly one protected lower"
        )
    lower = lowers[0]
    source = lower.get("source")
    source_config = source.get("config") if isinstance(source, dict) else None
    auth = lower.get("auth")
    if (
        not isinstance(lower.get("mount_id"), str)
        or not lower.get("mount_id")
        or lower.get("backend") != "nextcloud"
        or lower.get("target_path") != "/cloud/lower"
        or lower.get("workspace_name") != "lower"
        or lower.get("access") != "read_only"
        or not isinstance(source, dict)
        or source.get("type") != "webdav"
        or not isinstance(source_config, dict)
        or source_config.get("vendor") != "nextcloud"
        or not isinstance(source_config.get("url"), str)
        or not source_config.get("url")
        or not isinstance(source_config.get("user"), str)
        or not source_config.get("user")
        or not isinstance(auth, dict)
        or auth.get("type") != "basic"
        or not isinstance(auth.get("password"), str)
        or not auth.get("password")
    ):
        raise ProtectedCloudUnavailable("protected-cloud lower mount is malformed")
    return payload


def protected_workspace_delivery(payload: Dict[str, Any]) -> str:
    """Return ``off``, ``engaging`` or ``ready`` for a workspace response."""

    if protected_workspace_marker(payload) == "off":
        mount = payload.get("cloud_mount")
        protected_mount_shape = False
        if isinstance(mount, dict):
            mounts = mount.get("mounts")
            protected_mount_shape = (
                ("protected" in mount and mount.get("protected") is not False)
                or "overlay" in mount
                or (
                    isinstance(mounts, list)
                    and any(
                        isinstance(candidate, dict)
                        and candidate.get("mount_kind") == "protected_lower"
                        for candidate in mounts
                    )
                )
            )
        if (
            payload.get("protected_cloud_state") is not None
            or payload.get("protected_cloud_error_code") is not None
            or protected_mount_shape
        ):
            raise ProtectedCloudUnavailable(
                "protected-cloud payload has no authoritative marker"
            )
        return "off"
    state = payload.get("protected_cloud_state")
    status = payload.get("status")
    if state in {"engaging", "failed"}:
        # Pending/refused responses are a deliberately tiny, coordinate-free
        # projection.  Use an allowlist rather than chasing aliases: attach
        # consumes ``remote.host`` directly, and one forgotten transport key
        # would otherwise bypass a blacklist without ever being inspected.
        allowed_non_ready = {
            "status",
            "protected_cloud",
            "protected_cloud_state",
            "protected_cloud_error_code",
            "pod_ip",
            "pod_name",
            "pod_port",
            "namespace",
            "vm_status",
            "vm_ssh_host",
            "vm_ssh_port",
            "vm_name",
            "ssh_key_path",
            "workspace_generation",
            "workspace_runtime_incarnation",
            "workspace_ssh_host_key_fingerprint",
            "git_remote_url",
            "managed_repository_credentials",
            "repositories",
            "resolved_config",
            "config_override",
            "project_ids",
            "datasources",
            "nc_session_folder",
            "cloud_sync",
            "cloud_mount",
            "cloud_sync_degraded",
            "canvas_presentation_available",
            "canvas_live_apps_available",
            "canvas_shared_browser_available",
        }
        neutral_none = allowed_non_ready - {
            "status",
            "protected_cloud",
            "protected_cloud_state",
            "protected_cloud_error_code",
            "project_ids",
            "cloud_sync_degraded",
            "canvas_presentation_available",
            "canvas_live_apps_available",
            "canvas_shared_browser_available",
        }
        if (
            any(key not in allowed_non_ready for key in payload)
            or any(payload.get(key) is not None for key in neutral_none)
            or ("project_ids" in payload and payload.get("project_ids") != [])
            or any(
                payload.get(key) is not False
                for key in (
                    "cloud_sync_degraded",
                    "canvas_presentation_available",
                    "canvas_live_apps_available",
                    "canvas_shared_browser_available",
                )
                if key in payload
            )
        ):
            raise ProtectedCloudUnavailable(
                "protected-cloud non-ready payload exposed runtime coordinates"
            )
    if state == "engaging" and status == "creating":
        return "engaging"
    if state == "failed" and status == "failed":
        code = payload.get("protected_cloud_error_code")
        safe_code = (
            code
            if isinstance(code, str) and code in PROTECTED_CLOUD_SAFE_ERROR_CODES
            else "engage_failed"
        )
        raise ProtectedCloudUnavailable(
            f"protected-cloud engage was refused ({safe_code})"
        )
    if state != "ready" or status != "ready":
        raise ProtectedCloudUnavailable(
            "protected-cloud workspace has no authoritative ready state"
        )
    declared_backends: list[str] = []
    if "backend" in payload:
        direct_backend = payload.get("backend")
        if not isinstance(direct_backend, str):
            raise ProtectedCloudUnavailable(
                "protected cloud workspace backend declaration is malformed"
            )
        declared_backends.append(direct_backend)
    override = payload.get("config_override")
    if override is not None:
        if not isinstance(override, dict):
            raise ProtectedCloudUnavailable(
                "protected cloud workspace override is malformed"
            )
        workspace = override.get("workspace")
        if workspace is not None:
            if not isinstance(workspace, dict) or not isinstance(
                workspace.get("backend"), str
            ):
                raise ProtectedCloudUnavailable(
                    "protected cloud workspace override is malformed"
                )
            declared_backends.append(workspace["backend"])
    resolved = payload.get("resolved_config")
    if resolved is not None:
        if not isinstance(resolved, dict):
            raise ProtectedCloudUnavailable(
                "protected cloud resolved config is malformed"
            )
        agent = resolved.get("agent")
        if agent is not None and not isinstance(agent, dict):
            raise ProtectedCloudUnavailable(
                "protected cloud resolved agent config is malformed"
            )
        workspace = agent.get("workspace") if isinstance(agent, dict) else None
        if workspace is not None:
            if not isinstance(workspace, dict) or not isinstance(
                workspace.get("backend"), str
            ):
                raise ProtectedCloudUnavailable(
                    "protected cloud resolved workspace config is malformed"
                )
            declared_backends.append(workspace["backend"])
    if (
        not declared_backends
        or any(backend != "sandbox" for backend in declared_backends)
        or not isinstance(payload.get("pod_ip"), str)
        or not payload.get("pod_ip")
        or (
            payload.get("pod_port") is not None
            and (
                type(payload.get("pod_port")) is not int
                or not 1 <= payload.get("pod_port") <= 65535
            )
        )
        or payload.get("vm_status") not in (None, "none")
        or payload.get("vm_ssh_host") is not None
        or payload.get("vm_ssh_port") is not None
        or payload.get("vm_name") is not None
        or canonical_runtime_generation(payload.get("workspace_generation")) is None
        or canonical_runtime_generation(payload.get("workspace_runtime_incarnation"))
        is None
        or not isinstance(payload.get("workspace_ssh_host_key_fingerprint"), str)
        or not payload.get("workspace_ssh_host_key_fingerprint")
        or not pinned_runtime_generation_advertised(payload)
        or canonical_runtime_generation(payload.get("session_runtime_generation"))
        is None
    ):
        raise ProtectedCloudUnavailable(
            "protected cloud requires an exact sandbox workspace"
        )
    remote = payload.get("remote")
    if remote is not None:
        expected_port = payload.get("pod_port") or 30022
        expected_key = payload.get("ssh_key_path") or "/run/secrets/vm-ssh-key"
        if (
            not isinstance(remote, dict)
            or set(remote)
            - {
                "host",
                "port",
                "username",
                "key_path",
                "workspace_path",
            }
            or remote.get("host") != payload.get("pod_ip")
            or remote.get("port") != expected_port
            or remote.get("username") != "agent-host"
            or remote.get("key_path") != expected_key
            or remote.get("workspace_path") != "/home/agent-host/workspace"
        ):
            raise ProtectedCloudUnavailable(
                "protected cloud remote endpoint does not match its attestation"
            )
    if (
        payload.get("cloud_sync") is not None
        or payload.get("nc_session_folder") is not None
    ):
        raise ProtectedCloudUnavailable(
            "protected-cloud payload exposed a legacy live-write surface"
        )
    validate_protected_cloud_mount(payload.get("cloud_mount"))
    return "ready"


@dataclass(frozen=True, slots=True)
class ProtectedWorkspaceIdentity:
    """Exact protected workspace + mount bytes authorized for one attach."""

    pod_ip: str
    pod_port: int
    workspace_generation: str
    runtime_incarnation: str
    host_fingerprint: str
    session_runtime_generation: str
    cloud_mount_json: str


def protected_workspace_identity(
    payload: Dict[str, Any],
) -> ProtectedWorkspaceIdentity:
    if protected_workspace_delivery(payload) != "ready":
        raise ProtectedCloudUnavailable("protected workspace is not ready")
    return ProtectedWorkspaceIdentity(
        pod_ip=payload["pod_ip"],
        pod_port=payload.get("pod_port") or 30022,
        workspace_generation=str(UUID(payload["workspace_generation"])),
        runtime_incarnation=str(UUID(payload["workspace_runtime_incarnation"])),
        host_fingerprint=payload["workspace_ssh_host_key_fingerprint"],
        session_runtime_generation=str(UUID(payload["session_runtime_generation"])),
        cloud_mount_json=json.dumps(
            payload["cloud_mount"],
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


ATTACH_WORKSPACE_IDENTITY_UNSET = object()


def canonical_attach_workspace_identity(
    workspace_generation: Any,
    workspace_runtime_incarnation: Any,
) -> Optional[Tuple[Optional[str], Optional[str]]]:
    """Canonicalize an explicitly delivered workspace-authority pair.

    ``None`` as the return value means the caller omitted the contract (the
    dedicated startup path). ``(None, None)`` means the claim bundle captured
    no physical identity yet; shared attach may still poll a workspace whose
    session-runtime generation is authoritative. Keeping those states distinct
    prevents a compatibility default from weakening a delivered exact pair.
    """

    generation_omitted = workspace_generation is ATTACH_WORKSPACE_IDENTITY_UNSET
    incarnation_omitted = (
        workspace_runtime_incarnation is ATTACH_WORKSPACE_IDENTITY_UNSET
    )
    if generation_omitted or incarnation_omitted:
        if generation_omitted and incarnation_omitted:
            return None
        raise WorkspaceNotReady(
            "Attach workspace identity must be delivered as an exact pair"
        )
    if workspace_generation is None and workspace_runtime_incarnation is None:
        return (None, None)

    canonical_generation = canonical_runtime_generation(workspace_generation)
    canonical_incarnation = canonical_runtime_generation(workspace_runtime_incarnation)
    if canonical_generation is None or canonical_incarnation is None:
        raise WorkspaceNotReady("Attach workspace identity is malformed or incomplete")
    return canonical_generation, canonical_incarnation


def assert_attach_workspace_tier(
    expected: Optional[Tuple[Optional[str], Optional[str]]],
    *,
    is_lite_session: bool,
) -> None:
    """Reject an exact physical claim for a resolved no-workspace tier."""

    if expected is None or expected == (None, None):
        return
    if is_lite_session:
        raise WorkspaceNotReady(
            "Attach workspace identity does not match the resolved workspace tier"
        )


def assert_attach_workspace_payload(
    expected: Optional[Tuple[Optional[str], Optional[str]]],
    payload: Any,
) -> None:
    """Fence an observed workspace response to the delivered claim identity."""

    if expected is None or expected == (None, None):
        return
    raw_generation = (
        payload.get("workspace_generation") if isinstance(payload, dict) else None
    )
    raw_incarnation = (
        payload.get("workspace_runtime_incarnation")
        if isinstance(payload, dict)
        else None
    )
    if raw_generation is None and raw_incarnation is None:
        observed: Tuple[Optional[str], Optional[str]] = (None, None)
    else:
        canonical_generation = canonical_runtime_generation(raw_generation)
        canonical_incarnation = canonical_runtime_generation(raw_incarnation)
        if canonical_generation is None or canonical_incarnation is None:
            raise WorkspaceNotReady(
                "Observed workspace identity is malformed or incomplete"
            )
        observed = canonical_generation, canonical_incarnation
    if observed != expected:
        raise WorkspaceNotReady("Workspace identity changed during attach")


async def poll_workspace_ready(
    client: Any,
    thread_id: str,
    timeout: int = 120,
    poll_interval: float = 2.0,
    *,
    raise_on_denied: bool = False,
    vm_timeout: int = VM_WORKSPACE_POLL_TIMEOUT_S,
    require_vm: bool = False,
    raise_on_ending: bool = False,
) -> Optional[Dict[str, Any]]:
    """Poll orchestrator for workspace container readiness.

    ``vm_timeout`` is the extended budget applied automatically once the poll
    observes a VM-backed thread waiting for capacity or being created:
    a cold KubeVirt boot (CDI import + guest boot) routinely runs minutes past
    the sandbox-container ``timeout`` default, so the deadline self-extends
    rather than declaring a still-booting VM "not ready"
    (knowledge-base/knowledge/features/session_create_on_vm.md).

    ``raise_on_ending`` makes a 409 ``session_ending`` terminal
    (:class:`~agent.api.orchestrator_client.SessionEnding`) instead of a
    transient "unavailable": an attaching life whose retirement began can
    never become ready.

    ``require_vm`` makes the VM the ONLY acceptable answer: a ready sandbox
    container is refused (and logged as a provisioning leak) instead of being
    returned. Checking ``vm_status`` first is not sufficient on its own — within
    a single iteration a not-yet-ready VM falls through to the container branch,
    and since a container is ready in ~8 s against a multi-minute VM boot it wins
    that race every time. Callers pass this when the thread's resolved tier is
    ``vm``; the sandbox-upgrade caller deliberately does not
    (knowledge-base/knowledge/issues/session_vm_backend_never_attaches.md Defect 2).

    Returns:
        Workspace config dict {"backend": "remote", "remote": {host, port, ...}}
        or None if timeout, unavailable, or no workspace provisioned.
    """
    import time

    start = time.monotonic()
    deadline = start + timeout
    _vm_budget_applied = False

    while time.monotonic() < deadline:
        fetch_options: Dict[str, Any] = {"raise_on_denied": raise_on_denied}
        if raise_on_ending:
            # An attach must stop once its life's retirement has begun.
            fetch_options["raise_on_ending"] = True
        ws = await client.get_thread_workspace(thread_id, **fetch_options)
        if not ws:
            # The client collapses every non-200 to None. For a vm-tier
            # session that includes a transient 5xx from the orchestrator
            # (a restart, or a repository authority that is briefly
            # unavailable) and bailing here mis-reports a booting VM as
            # "never became ready" while releasing the pinned agent — the
            # VM budget bounds the retry instead.
            if require_vm:
                logger.warning(
                    "Thread %s: workspace status unavailable — retrying within "
                    "the VM budget.",
                    thread_id,
                )
                await asyncio.sleep(poll_interval)
                continue
            return None

        protected_delivery = protected_workspace_delivery(ws)
        if protected_delivery == "engaging":
            await asyncio.sleep(poll_interval)
            continue

        # SSH key: orchestrator sends the path it resolved (dev compose
        # key or K8s secret mount); fall back to the K8s default.
        ssh_key = ws.get("ssh_key_path") or "/run/secrets/vm-ssh-key"

        # Check VM workspace first (takes precedence over container)
        vm_status = ws.get("vm_status")

        # A VM-backed thread pays a cold KubeVirt boot far beyond the
        # sandbox-container default. Extend the poll deadline ONCE the moment we
        # observe the VM is in flight so a legitimate cold boot isn't declared
        # "not ready" — self-adjusting, no caller signal needed.
        if not _vm_budget_applied and vm_status in (
            "waiting_capacity",
            "provisioning",
            "created",
        ):
            deadline = start + max(timeout, vm_timeout)
            _vm_budget_applied = True
            logger.info(
                "Thread %s: VM workspace provisioning detected — extending "
                "workspace readiness budget to %ss.",
                thread_id,
                max(timeout, vm_timeout),
            )
        if vm_status == "ready" and ws.get("vm_ssh_host"):
            return {
                "backend": "vm",
                # The attach verifier consumes the same server-issued runtime
                # contract for both backends; normalization must preserve it.
                "pinned_status_identity_contract": ws.get(
                    "pinned_status_identity_contract"
                ),
                "pinned_runtime_generation_contract": ws.get(
                    "pinned_runtime_generation_contract"
                ),
                # A self-attaching pinned pod takes its fan-out advertisement
                # from this ready payload (the VM wait payload has none).
                SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY: ws.get(
                    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY
                ),
                SESSION_SUBAGENT_FANOUT_KEY: ws.get(SESSION_SUBAGENT_FANOUT_KEY),
                "session_runtime_generation": ws.get("session_runtime_generation"),
                # Server-derived provisioner authority must survive this
                # normalization boundary. PersistentSession deliberately does
                # not trust the provisioner from agent config.
                "workspace_provisioner": ws.get("workspace_provisioner"),
                "workspace_generation": ws.get("workspace_generation"),
                "workspace_runtime_incarnation": ws.get(
                    "workspace_runtime_incarnation"
                ),
                # The VM controller and provisioner attest the same physical
                # tuple consumed by exact pinned-session SSH setup.
                "workspace_ssh_host_key_fingerprint": ws.get(
                    "workspace_ssh_host_key_fingerprint"
                ),
                # VM physical attestation does not grant Canvas presentation.
                "canvas_presentation_available": False,
                "canvas_live_apps_available": False,
                "canvas_shared_browser_available": False,
                "remote": {
                    "host": ws["vm_ssh_host"],
                    "port": ws.get("vm_ssh_port", 22),
                    "username": "agent-host",
                    "key_path": ssh_key,
                    "workspace_path": "/home/agent-host/workspace",
                },
                "git_remote_url": ws.get("git_remote_url"),
                "managed_repository_credentials": ws.get(
                    "managed_repository_credentials"
                ),
                "repositories": ws.get("repositories"),
                "config_override": ws.get("config_override"),
                "project_ids": ws.get("project_ids") or [],
                "datasources": ws.get("datasources"),
                "nc_session_folder": ws.get("nc_session_folder"),
                "cloud_sync": ws.get("cloud_sync"),
                "cloud_mount": ws.get("cloud_mount"),
                "cloud_sync_degraded": ws.get("cloud_sync_degraded"),
                # F-C1: carried through so the attach can fail-close the
                # legacy nc_session_folder sync shim for protected threads.
                "protected_cloud": ws.get("protected_cloud"),
                "protected_cloud_state": ws.get("protected_cloud_state"),
                "protected_cloud_error_code": ws.get("protected_cloud_error_code"),
            }

        # A vm-tier thread accepts no substitute. Bail on a terminal VM instead
        # of burning the full VM budget — the pre-existing 'failed' bail below
        # also requires the CONTAINER to have failed, which never happens on a
        # thread that (correctly) has no container.
        if require_vm:
            if not vm_status:
                # No VM context at all on a vm-tier thread — provisioning was
                # never requested (create_thread sets vm.status='provisioning'
                # synchronously before the agent can poll, and it persists across
                # resume). Terminal, so fail fast rather than sitting out the
                # budget; mirrors the container branch's status=='none' bail.
                logger.warning(
                    "Thread %s: vm-tier session has no VM context — no VM was "
                    "ever provisioned for it.",
                    thread_id,
                )
                return None
            if vm_status == "failed":
                logger.warning(
                    "Thread %s: VM provisioning failed — not falling back to a "
                    "container (vm-tier session).",
                    thread_id,
                )
                return None
            if ws.get("status") == "ready" and ws.get("pod_ip"):
                # A container exists for a vm-tier thread: a provisioning leak
                # (see Defect 1). Refuse it — attaching here is precisely the
                # silent wrong-tier downgrade this guard exists to prevent.
                logger.warning(
                    "Thread %s: ignoring a ready workspace container on a vm-tier "
                    "session (pod %s) — this container should not exist; waiting "
                    "for the VM instead.",
                    thread_id,
                    ws.get("pod_ip"),
                )
            await asyncio.sleep(poll_interval)
            continue

        # Check container workspace
        status = ws.get("status", "none")

        if status == "ready" and ws.get("pod_ip"):
            workspace_generation = ws.get("workspace_generation")
            workspace_runtime_incarnation = ws.get("workspace_runtime_incarnation")
            workspace_ssh_host_key_fingerprint = ws.get(
                "workspace_ssh_host_key_fingerprint"
            )
            if not workspace_generation or not workspace_runtime_incarnation:
                # Never let a detached fingerprint look like independently
                # usable authority. Stateless setup consumes one triplet.
                workspace_ssh_host_key_fingerprint = None
            return {
                "backend": "sandbox",
                # This is orchestrator authority, not an inference from the
                # normalized backend label. Dropping it makes every sandbox
                # attach fail closed before its first model call.
                "workspace_provisioner": ws.get("workspace_provisioner"),
                # Preserve the authoritative protected-ready tuple through
                # normalization.  the attach coordinator revalidates the
                # normalized response immediately before constructing
                # PersistentSession; dropping status/pod coordinates here
                # would turn a valid protected answer into an ambiguous one.
                "status": "ready",
                "pod_ip": ws["pod_ip"],
                "pod_port": ws.get("pod_port") or 30022,
                "ssh_key_path": ssh_key,
                "pinned_status_identity_contract": ws.get(
                    "pinned_status_identity_contract"
                ),
                "pinned_runtime_generation_contract": ws.get(
                    "pinned_runtime_generation_contract"
                ),
                # Same fan-out advertisement as the vm branch above.
                SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY: ws.get(
                    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY
                ),
                SESSION_SUBAGENT_FANOUT_KEY: ws.get(SESSION_SUBAGENT_FANOUT_KEY),
                "session_runtime_generation": ws.get("session_runtime_generation"),
                "workspace_generation": workspace_generation,
                "workspace_runtime_incarnation": workspace_runtime_incarnation,
                "workspace_ssh_host_key_fingerprint": (
                    workspace_ssh_host_key_fingerprint
                ),
                # This is an orchestrator-attested capability, not a property
                # inferred from the backend label or endpoint reachability.
                "canvas_presentation_available": (
                    ws.get("canvas_presentation_available") is True
                ),
                "canvas_live_apps_available": (
                    ws.get("canvas_live_apps_available") is True
                ),
                "canvas_shared_browser_available": (
                    ws.get("canvas_shared_browser_available") is True
                ),
                "remote": {
                    "host": ws["pod_ip"],
                    "port": ws.get("pod_port") or 30022,
                    "username": "agent-host",
                    "key_path": ssh_key,
                    "workspace_path": "/home/agent-host/workspace",
                },
                "git_remote_url": ws.get("git_remote_url"),
                "managed_repository_credentials": ws.get(
                    "managed_repository_credentials"
                ),
                "repositories": ws.get("repositories"),
                "config_override": ws.get("config_override"),
                "project_ids": ws.get("project_ids") or [],
                "datasources": ws.get("datasources"),
                "nc_session_folder": ws.get("nc_session_folder"),
                "cloud_sync": ws.get("cloud_sync"),
                "cloud_mount": ws.get("cloud_mount"),
                "cloud_sync_degraded": ws.get("cloud_sync_degraded"),
                # F-C1: see comment above (vm branch).
                "protected_cloud": ws.get("protected_cloud"),
                "protected_cloud_state": ws.get("protected_cloud_state"),
                "protected_cloud_error_code": ws.get("protected_cloud_error_code"),
            }
        if status == "failed" and (not vm_status or vm_status == "failed"):
            # The internal readiness response can carry the one-shot managed
            # repository authority bundle once a runtime is ready.  Never log
            # the response object on a terminal/error branch: a mixed-version
            # or racing response could otherwise put encrypted-handoff
            # plaintext in pod logs.  Status fields are sufficient to diagnose
            # the provisioning failure.
            logger.warning(
                "Workspace provisioning failed for thread %s "
                "(container_status=%s, vm_status=%s)",
                thread_id,
                status,
                vm_status or "none",
            )
            return None
        if status == "none" and not vm_status:
            # No workspace provisioned for this thread (no K8s)
            return None

        # Still creating — wait and poll again
        await asyncio.sleep(poll_interval)

    logger.warning(f"Workspace polling timed out after {timeout}s")
    return None
