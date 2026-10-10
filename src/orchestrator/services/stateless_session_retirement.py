"""Acknowledged terminal retirement for stateless sandbox session shells.

The serving Python session normally detaches after every queue claim, while its
tmux panes deliberately remain in the workspace for the next claimant.  Public
End therefore cannot rely on an in-process ``PersistentSession`` being around
to destroy that durable shell.  This service reconstructs only the existing
``RemoteBackend`` terminal protocol from orchestrator-owned workspace
authority and requires its acknowledgement before Kubernetes teardown begins.

No protocol is reimplemented here: ``RemoteBackend.shell_cleanup`` owns the
workspace-side flock, monotonic token check, incarnation check, retired record,
and tmux destruction.  This module is only the fail-closed authority adapter.
"""

from __future__ import annotations

import asyncio
import json
import shlex
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from orchestrator.services import resolve_ssh_key_path
from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.workspace_binding import (
    CANVAS_WORKSPACE_GENERATION_KEY,
    remote_canvas_presentation_available,
)
from shared.runtime.core.backends.remote import RemoteBackend
from shared.runtime.core.managed_repository import (
    managed_repository_agent_retirement_command,
    managed_repository_agent_zero_command,
)


SHELL_RETIREMENT_ACK_KEY = "_stateless_shell_retirement_ack"
RESIDENT_RETIREMENT_ACK_KEY = "_stateless_resident_retirement_ack"


class ShellRetirementUnavailable(RuntimeError):
    """The exact authoritative workspace cannot acknowledge retirement."""


@dataclass(frozen=True, slots=True)
class ShellRetirementAuthority:
    """Exact endpoint and incarnation authorized for terminal retirement."""

    thread_id: str
    terminal_token: int
    host: str
    port: int
    workspace_generation: str
    runtime_incarnation: str
    host_key_fingerprint: str


@dataclass(frozen=True, slots=True)
class ResidentRetirementProof:
    """Zero-resident proof bound to one exact terminal runtime authority."""

    authority: ShellRetirementAuthority
    browser_processes: int = 0
    code_server_processes: int = 0
    rclone_mounts: int = 0
    rclone_processes: int = 0
    overlay_mounts: int = 0
    overlay_processes: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "browser_processes": self.browser_processes,
            "code_server_processes": self.code_server_processes,
            "rclone_mounts": self.rclone_mounts,
            "rclone_processes": self.rclone_processes,
            "overlay_mounts": self.overlay_mounts,
            "overlay_processes": self.overlay_processes,
        }


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def resolve_shell_retirement_authority(
    thread: dict[str, Any], *, terminal_token: int
) -> ShellRetirementAuthority:
    """Resolve a complete, attested remote tuple or fail closed.

    A prior claim (positive queue token) proves that a durable shell record may
    exist.  Historical host/IP bytes alone are not authority: the endpoint must
    either still be Ready or carry the exact End-owned
    ``retiring_process_zero`` authority, paired with the backing generation,
    Pod UID, and host-key fingerprint accepted by the normal stateless attach
    boundary.  The retirement-only state never becomes Canvas/work admission.
    """

    token = int(terminal_token)
    if token <= 0:
        raise ValueError("terminal shell token must be positive")
    thread_id = str(thread.get("id") or "")
    if not thread_id:
        raise ShellRetirementUnavailable("thread identity is unavailable")

    metadata = _json_object(thread.get("metadata"))
    workspace = _json_object(metadata.get("workspace_container"))
    binding = _json_object(metadata.get("_workspace_binding"))
    endpoint_available = remote_canvas_presentation_available(metadata, workspace)
    if workspace.get("status") == "retiring_process_zero":
        from shared.session_retirement import stateless_retirement_authority

        try:
            retirement = stateless_retirement_authority(metadata)
        except RuntimeError:
            retirement = None
        endpoint_available = bool(
            thread.get("status") == "ended"
            and retirement is not None
            and retirement.get("terminal_token") == token
            and retirement.get("workspace_generation") == binding.get("generation")
            and retirement.get("endpoint_generation")
            == workspace.get(CANVAS_WORKSPACE_GENERATION_KEY)
            and retirement.get("runtime_incarnation")
            == workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
            and retirement.get("host_key_fingerprint")
            == binding.get("ssh_host_key_fingerprint")
            and isinstance(binding.get("backing_id"), str)
            and bool(binding.get("backing_id"))
        )
    if (
        workspace.get("provisioner") != "k8s"
        or binding.get("kind") != "remote"
        or not endpoint_available
    ):
        raise ShellRetirementUnavailable(
            "authoritative workspace retirement endpoint is unavailable"
        )

    try:
        generation = str(UUID(str(binding.get("generation"))))
        endpoint_generation = str(
            UUID(str(workspace.get(CANVAS_WORKSPACE_GENERATION_KEY)))
        )
        runtime_incarnation = str(
            UUID(str(workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY)))
        )
    except (TypeError, ValueError) as exc:
        raise ShellRetirementUnavailable(
            "workspace retirement incarnation is invalid"
        ) from exc
    if endpoint_generation != generation:
        raise ShellRetirementUnavailable(
            "workspace retirement generation is inconsistent"
        )

    host = workspace.get("host") or workspace.get("pod_ip")
    fingerprint = binding.get("ssh_host_key_fingerprint")
    if not isinstance(host, str) or not host.strip():
        raise ShellRetirementUnavailable("workspace retirement host is unavailable")
    if not isinstance(fingerprint, str) or not fingerprint:
        raise ShellRetirementUnavailable(
            "workspace retirement host-key authority is unavailable"
        )
    try:
        port = int(workspace.get("port") or workspace.get("pod_port") or 30022)
    except (TypeError, ValueError) as exc:
        raise ShellRetirementUnavailable(
            "workspace retirement port is invalid"
        ) from exc
    if not (1 <= port <= 65535):
        raise ShellRetirementUnavailable("workspace retirement port is invalid")

    return ShellRetirementAuthority(
        thread_id=thread_id,
        terminal_token=token,
        host=host.strip(),
        port=port,
        workspace_generation=generation,
        runtime_incarnation=runtime_incarnation,
        host_key_fingerprint=fingerprint,
    )


def shell_retirement_ack_matches(
    thread: dict[str, Any], *, terminal_token: int
) -> bool:
    """Whether DB state already durably records this exact remote ACK."""

    metadata = _json_object(thread.get("metadata"))
    ack = _json_object(metadata.get(SHELL_RETIREMENT_ACK_KEY))
    marker = _json_object(metadata.get("_stateless_claim_retirement"))
    binding = _json_object(metadata.get("_workspace_binding"))
    workspace = _json_object(metadata.get("workspace_container"))
    try:
        return (
            thread.get("status") == "ended"
            and metadata.get("_stateless_workspace_retirement_pending") is True
            and ack.get("kind") == "protocol"
            and int(ack.get("terminal_token")) == int(terminal_token)
            and int(marker.get("terminal_token")) == int(terminal_token)
            and str(UUID(str(ack.get("workspace_generation"))))
            == str(UUID(str(marker.get("workspace_generation"))))
            == str(UUID(str(binding.get("generation"))))
            and str(UUID(str(ack.get("endpoint_generation"))))
            == str(UUID(str(marker.get("endpoint_generation"))))
            == str(UUID(str(workspace.get(CANVAS_WORKSPACE_GENERATION_KEY))))
            == str(UUID(str(binding.get("generation"))))
            and str(UUID(str(ack.get("runtime_incarnation"))))
            == str(UUID(str(marker.get("runtime_incarnation"))))
            == str(UUID(str(workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY))))
            and ack.get("host_key_fingerprint")
            == marker.get("host_key_fingerprint")
            == binding.get("ssh_host_key_fingerprint")
            and marker.get("remote_retired") is True
        )
    except (TypeError, ValueError, AttributeError):
        return False


def _build_terminal_backend(authority: ShellRetirementAuthority) -> RemoteBackend:
    backend = RemoteBackend(
        host=authority.host,
        port=authority.port,
        username="agent-host",
        key_path=resolve_ssh_key_path(),
        workspace_path="/home/agent-host/workspace",
        job_id=authority.thread_id,
        workspace_generation=authority.workspace_generation,
        runtime_incarnation=authority.runtime_incarnation,
        expected_host_key_fingerprint=authority.host_key_fingerprint,
    )
    backend.set_shell_owner_token(authority.terminal_token)
    return backend


def _browser_ide_zero_command() -> str:
    return """
for _srw_cmdline in /proc/[0-9]*/cmdline; do
  _srw_pid=$(basename "$(dirname "$_srw_cmdline")")
  if [ ! -r "$_srw_cmdline" ]; then
    [ ! -e "/proc/$_srw_pid" ] && continue
    _srw_uid=$(awk '/^Uid:/ { print $2; exit }' "/proc/$_srw_pid/status" 2>/dev/null) || exit 86
    [ -n "$_srw_uid" ] || exit 86
    [ "$_srw_uid" != "$(id -u)" ] && continue
    exit 86
  fi
  _srw_exe=$(basename "$(readlink "/proc/$_srw_pid/exe" 2>/dev/null || true)")
  case "$_srw_exe" in
    code-server|node)
      _srw_args=$(tr '\\0' '\\n' < "$_srw_cmdline" 2>/dev/null || true)
      if printf '%s\n' "$_srw_args" | grep -Fx -- '0.0.0.0:38080' >/dev/null &&
         printf '%s\n' "$_srw_args" | grep -Fx -- '/var/lib/code-server' >/dev/null; then
        exit 85
      fi
    ;;
  esac
done
python3 -c 'import socket,sys; s=socket.socket(socket.AF_UNIX); s.settimeout(.2);\ntry: s.connect("/tmp/browser-exec.sock")\nexcept OSError: sys.exit(0)\nsys.exit(1)' || exit 86
if command -v ss >/dev/null 2>&1; then
  ! ss -ltn '( sport = :38080 or sport = :38801 )' | tail -n +2 | grep -q . || exit 86
fi
"""


def _resident_zero_command(thread_id: str) -> str:
    """Read-only process/socket/identity proof shared before and after shell kill."""

    thread = shlex.quote(str(UUID(str(thread_id))))
    return f"""
_srw_thread={thread}
_srw_rclone_base="$HOME/.cache/srw/rclone/$_srw_thread"
_srw_overlay_base="$HOME/.cache/srw/overlay/$_srw_thread"
if [ -d "$_srw_rclone_base" ] && find "$_srw_rclone_base" -name resident.identity -type f -print -quit | grep -q .; then exit 84; fi
if [ -d "$_srw_overlay_base" ] && find "$_srw_overlay_base" -name resident.identity -type f -print -quit | grep -q .; then exit 84; fi
for _srw_cmdline in /proc/[0-9]*/cmdline; do
  _srw_pid=$(basename "$(dirname "$_srw_cmdline")")
  if [ ! -r "$_srw_cmdline" ]; then
    [ ! -e "/proc/$_srw_pid" ] && continue
    _srw_uid=$(awk '/^Uid:/ {{ print $2; exit }}' "/proc/$_srw_pid/status" 2>/dev/null) || exit 86
    [ -n "$_srw_uid" ] || exit 86
    [ "$_srw_uid" != "$(id -u)" ] && continue
    exit 86
  fi
  _srw_args=$(tr '\\0' '\\n' < "$_srw_cmdline" 2>/dev/null || true)
  if printf '%s\n' "$_srw_args" | grep -F -- "$_srw_rclone_base/" >/dev/null; then exit 85; fi
  if printf '%s\n' "$_srw_args" | grep -F -- "$_srw_overlay_base/" >/dev/null; then exit 85; fi
done
{_browser_ide_zero_command()}
echo '__SRW_TERMINAL_RESIDENTS_ZERO__'
"""


def _resident_cleanup_command(thread_id: str) -> str:
    """Gracefully stop browser/IDE owners, then kill their exact process trees."""

    return f"""set -euo pipefail
_srw_browser=$(timeout 35 browser-exec shutdown --json '{{}}' 2>&1) || {{ printf '%s\n' "$_srw_browser" >&2; exit 91; }}
printf '%s\n' "$_srw_browser" | tail -n1 | python3 -c 'import json,sys; value=json.load(sys.stdin); raise SystemExit(0 if value.get("ok") is True and value.get("shutdown_complete") is True else 1)' || exit 91
_srw_roots=
for _srw_cmdline in /proc/[0-9]*/cmdline; do
  _srw_pid=$(basename "$(dirname "$_srw_cmdline")")
  if [ ! -r "$_srw_cmdline" ]; then
    [ ! -e "/proc/$_srw_pid" ] && continue
    _srw_uid=$(awk '/^Uid:/ {{ print $2; exit }}' "/proc/$_srw_pid/status" 2>/dev/null) || exit 86
    [ -n "$_srw_uid" ] || exit 86
    [ "$_srw_uid" != "$(id -u)" ] && continue
    exit 86
  fi
  _srw_exe=$(basename "$(readlink "/proc/$_srw_pid/exe" 2>/dev/null || true)")
  case "$_srw_exe" in
    code-server|node)
      _srw_args=$(tr '\\0' '\\n' < "$_srw_cmdline" 2>/dev/null || true)
      if printf '%s\n' "$_srw_args" | grep -Fx -- '0.0.0.0:38080' >/dev/null &&
         printf '%s\n' "$_srw_args" | grep -Fx -- '/var/lib/code-server' >/dev/null; then
        _srw_roots="$_srw_roots $_srw_pid"
      fi
    ;;
  esac
done
_srw_tree="$_srw_roots"
for _srw_round in $(seq 1 32); do
  _srw_added=
  while read -r _srw_pid _srw_ppid; do
    case " $_srw_tree " in *" $_srw_ppid "*)
      case " $_srw_tree " in *" $_srw_pid "*) ;; *) _srw_tree="$_srw_tree $_srw_pid"; _srw_added=yes ;; esac
    ;; esac
  done <<EOF
$(ps -eo pid=,ppid=)
EOF
  [ -n "$_srw_added" ] || break
done
for _srw_pid in $_srw_tree; do kill "$_srw_pid" 2>/dev/null || true; done
for _srw_i in $(seq 1 30); do
  _srw_alive=
  for _srw_pid in $_srw_tree; do [ ! -e "/proc/$_srw_pid" ] || _srw_alive="$_srw_alive $_srw_pid"; done
  [ -z "$_srw_alive" ] && break
  sleep 0.1
done
for _srw_pid in $_srw_alive; do kill -9 "$_srw_pid" 2>/dev/null || true; done
for _srw_i in $(seq 1 20); do
  _srw_alive=
  for _srw_pid in $_srw_tree; do [ ! -e "/proc/$_srw_pid" ] || _srw_alive="$_srw_alive $_srw_pid"; done
  [ -z "$_srw_alive" ] && break
  sleep 0.1
done
[ -z "$_srw_alive" ] || exit 92
{_browser_ide_zero_command()}
echo '__SRW_TERMINAL_BROWSER_IDE_ZERO__'
"""


async def retire_stateless_workspace_residents(
    thread: dict[str, Any],
    *,
    terminal_token: int,
    cloud_mount_cfg: dict[str, Any] | None,
) -> ResidentRetirementProof:
    """Drain every workspace-resident writer while the remote record is active T."""

    from shared.runtime.services.cloud_mount import RcloneMountManager
    from shared.runtime.services.cloud_mount.sidecar import SidecarMountWatcher
    from shared.runtime.services.cloud_overlay import OverlayMountManager

    authority = resolve_shell_retirement_authority(
        thread, terminal_token=terminal_token
    )
    backend = _build_terminal_backend(authority)
    counters: dict[str, int] = {}
    try:
        await asyncio.to_thread(
            backend.exec_terminal_claim_resource,
            managed_repository_agent_retirement_command(
                home_path="/home/agent-host",
                authority_ids=None,
                remove_configs=True,
            ),
            30,
            operation="managed repository credential-agent cleanup",
        )
        await asyncio.to_thread(
            backend.exec_terminal_claim_resource,
            _resident_cleanup_command(authority.thread_id),
            90,
            operation="browser and IDE resident cleanup",
        )
        if cloud_mount_cfg:
            if cloud_mount_cfg.get("protected") and cloud_mount_cfg.get("overlay"):
                overlay = OverlayMountManager(
                    thread_id=authority.thread_id,
                    overlay_cfg=dict(cloud_mount_cfg["overlay"]),
                    workspace_backend=backend,
                    workspace_root="/home/agent-host/workspace",
                )
                counters.update(await overlay.retire_existing())
            if cloud_mount_cfg.get("delivery") == "sidecar":
                # The Pod's sidecars own the folders (D7): nothing of the
                # session runs in the workspace, but its writes must still
                # reach the cloud while T is active, so ask for the flush
                # and require it when a mounted folder has uploads pending.
                watcher = SidecarMountWatcher(
                    thread_id=authority.thread_id,
                    cloud_cfg=dict(cloud_mount_cfg),
                    workspace_backend=backend,
                    workspace_root="/home/agent-host/workspace",
                    terminal=True,
                )
                counters.update(await watcher.retire_existing(drain=True))
            else:
                mount_manager = RcloneMountManager(
                    thread_id=authority.thread_id,
                    cloud_cfg=dict(cloud_mount_cfg),
                    workspace_backend=backend,
                    workspace_root="/home/agent-host/workspace",
                )
                counters.update(await mount_manager.retire_existing(drain=True))
        await asyncio.to_thread(
            backend.exec_terminal_claim_resource,
            _resident_zero_command(authority.thread_id)
            + "\n"
            + managed_repository_agent_zero_command(home_path="/home/agent-host"),
            30,
            operation="terminal resident zero proof",
        )
    except Exception as exc:
        raise ShellRetirementUnavailable(
            "workspace resident cleanup was not acknowledged"
        ) from exc
    finally:
        try:
            await asyncio.to_thread(backend.retire)
        except Exception:
            pass
    return ResidentRetirementProof(authority=authority, **counters)


async def verify_stateless_workspace_residents_retired(
    thread: dict[str, Any],
    *,
    terminal_token: int,
    cloud_mount_cfg: dict[str, Any] | None,
) -> ShellRetirementAuthority:
    """Re-prove zero writers under the exact retired-T record after tmux kill."""

    from shared.runtime.services.cloud_mount import RcloneMountManager
    from shared.runtime.services.cloud_overlay import OverlayMountManager

    authority = resolve_shell_retirement_authority(
        thread, terminal_token=terminal_token
    )
    backend = _build_terminal_backend(authority)
    try:
        await asyncio.to_thread(
            backend.verify_terminal_claim_resources_retired,
            _resident_zero_command(authority.thread_id)
            + "\n"
            + managed_repository_agent_zero_command(home_path="/home/agent-host"),
            30,
        )
        if cloud_mount_cfg:
            if cloud_mount_cfg.get("protected") and cloud_mount_cfg.get("overlay"):
                overlay = OverlayMountManager(
                    thread_id=authority.thread_id,
                    overlay_cfg=dict(cloud_mount_cfg["overlay"]),
                    workspace_backend=backend,
                    workspace_root="/home/agent-host/workspace",
                )
                await asyncio.to_thread(
                    backend.verify_terminal_claim_resources_retired,
                    overlay._terminal_zero_script(),
                    30,
                )
        if cloud_mount_cfg and cloud_mount_cfg.get("delivery") != "sidecar":
            # A sidecar Pod's folders have no workspace resident to re-prove.
            mount_manager = RcloneMountManager(
                thread_id=authority.thread_id,
                cloud_cfg=dict(cloud_mount_cfg),
                workspace_backend=backend,
                workspace_root="/home/agent-host/workspace",
            )
            await asyncio.to_thread(
                backend.verify_terminal_claim_resources_retired,
                mount_manager._terminal_zero_script(),
                30,
            )
    except Exception as exc:
        raise ShellRetirementUnavailable(
            "post-shell resident zero proof was not acknowledged"
        ) from exc
    finally:
        try:
            await asyncio.to_thread(backend.retire)
        except Exception:
            pass
    return authority


async def retire_stateless_session_shell(
    thread: dict[str, Any], *, terminal_token: int
) -> ShellRetirementAuthority:
    """Write and verify the existing remote terminal-retirement record."""

    authority = resolve_shell_retirement_authority(
        thread, terminal_token=terminal_token
    )
    backend = _build_terminal_backend(authority)
    try:
        await asyncio.to_thread(backend.shell_cleanup)
    except Exception as exc:
        # ``disconnect`` preserves the remote record and merely drops this
        # attempt's transport.  The durable DB marker remains pending, so a
        # later public End retry reconstructs a fresh backend and retries the
        # idempotent terminal protocol with the same token.
        try:
            await asyncio.to_thread(backend.disconnect)
        except Exception:
            pass
        raise ShellRetirementUnavailable(
            "remote shell retirement was not acknowledged"
        ) from exc
    await asyncio.to_thread(backend.retire)
    return authority


__all__ = [
    "RESIDENT_RETIREMENT_ACK_KEY",
    "SHELL_RETIREMENT_ACK_KEY",
    "ResidentRetirementProof",
    "ShellRetirementAuthority",
    "ShellRetirementUnavailable",
    "resolve_shell_retirement_authority",
    "retire_stateless_session_shell",
    "retire_stateless_workspace_residents",
    "shell_retirement_ack_matches",
    "verify_stateless_workspace_residents_retired",
]
