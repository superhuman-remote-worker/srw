"""Agent-side lazy cloud mounts backed by rclone.

The orchestrator owns provider decisions and sends a generic ``cloud_mount``
payload. This module runs inside the agent process but performs setup on the
remote workspace runtime, so shell commands and workspace tools see the same
mounted filesystem.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import secrets
import shlex
import threading
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

from shared.runtime.services.keycloak_token import KeycloakTokenClient

logger = logging.getLogger(__name__)

_OK = "__SRW_RCLONE_MOUNT_OK__"
_FAILED = "__SRW_RCLONE_MOUNT_FAILED__"
_CACHE_USAGE = "__SRW_RCLONE_CACHE_USAGE__"
_RC_CORE = "__SRW_RCLONE_RC_CORE__"
_RC_VFS = "__SRW_RCLONE_RC_VFS__"
_RESIDENT_ADOPTED = "__SRW_RCLONE_RESIDENT_ADOPTED__"
_RESIDENT_HEAL = "__SRW_RCLONE_RESIDENT_HEAL__"
_RESIDENT_IDENTITY_VERSION = "1"
_TERMINAL_DRAIN_STATUS_PY = (
    "import json,sys; c=json.loads(sys.argv[1]); v=json.loads(sys.argv[2]); "
    'd=v.get("diskCache") if isinstance(v,dict) else None; '
    't=c.get("transferring") if isinstance(c,dict) else None; '
    'core_ok=isinstance(c,dict) and "error" not in c and '
    'all(type(c.get(k)) is int and c[k] >= 0 for k in ("bytes","errors","transfers")) '
    'and ("transferring" not in c or (isinstance(t,list) and not t)); '
    'vfs_ok=isinstance(d,dict) and type(d.get("uploadsQueued")) is int and '
    'd["uploadsQueued"] == 0 and type(d.get("uploadsInProgress")) is int and '
    'd["uploadsInProgress"] == 0; ok=core_ok and vfs_ok; '
    "raise SystemExit(0 if ok else 1)"
)

_CACHE_FLAG_MAP = {
    "vfs_cache_mode": "--vfs-cache-mode",
    "vfs_cache_max_size": "--vfs-cache-max-size",
    "vfs_cache_max_age": "--vfs-cache-max-age",
    "vfs_cache_min_free_space": "--vfs-cache-min-free-space",
    "dir_cache_time": "--dir-cache-time",
    "poll_interval": "--poll-interval",
    "vfs_read_chunk_size": "--vfs-read-chunk-size",
    "vfs_read_chunk_size_limit": "--vfs-read-chunk-size-limit",
}

_DEFAULT_CACHE = {
    "vfs_cache_mode": "full",
    "vfs_cache_max_size": "10G",
    "vfs_cache_max_age": "24h",
    "dir_cache_time": "5m",
    "poll_interval": "1m",
    "vfs_read_chunk_size": "16M",
    "vfs_read_chunk_size_limit": "128M",
    "hard_cache_limit": "20G",
}

# Bearer-token auth types resolved through the shared Keycloak token client
# (OpenCloud). The orchestrator never sends a static credential for these —
# the agent process mints short-lived tokens and pushes them to the
# workspace runtime, where rclone reads them via ``bearer_token_command``.
_KEYCLOAK_AUTH_TYPES = {
    "keycloak_client_credentials",
    "keycloak_user_impersonation",
}
# Re-push tokens this long before expiry; never spin faster than the floor.
_TOKEN_REFRESH_SAFETY_SECONDS = 60.0
_TOKEN_REFRESH_MIN_INTERVAL_SECONDS = 30.0

_SIZE_UNITS = {
    "": 1,
    "B": 1,
    "K": 1024,
    "KB": 1024,
    "M": 1024**2,
    "MB": 1024**2,
    "G": 1024**3,
    "GB": 1024**3,
    "T": 1024**4,
    "TB": 1024**4,
}

_CLOUDIGNORE_HELPERS = r"""
compile_cloudignore() {
  local src="$1"
  local dest="$2"
  awk '
    function trim(s) {
      gsub(/^[ \t]+|[ \t]+$/, "", s)
      return s
    }
    {
      sub(/\r$/, "", $0)
      line = trim($0)
      if (line == "" || line ~ /^#/) next
      if (line ~ /^!/) next
      if (line ~ /(^|\/)\.\.($|\/)/) next
      gsub(/^\/+/, "", line)
      if (line == "") next
      if (line ~ /\/$/) {
        sub(/\/+$/, "", line)
        if (line == "") next
        print line "/**"
        if (line !~ /\//) print "**/" line "/**"
      } else {
        print line
        if (line !~ /\// && line !~ /[*?]/ && index(line, "[") == 0) print "**/" line
      }
    }
  ' "$src" >> "$dest"
}
"""


#: Non-secret source keys that may join a resident mount's identity
#: (``nextcloud_chunk_size`` is "0" for a Nextcloud Group Folder).
_IDENTITY_SOURCE_KEYS = frozenset(
    {"url", "vendor", "user", "encoding", "nextcloud_chunk_size"}
)


class RcloneMountError(RuntimeError):
    """Raised when a lazy cloud mount cannot be started."""


class RcloneMountCleanFailure(RcloneMountError):
    """Mount startup failed after exact stateless resident rollback.

    This is deliberately narrower than :class:`RcloneMountError`.  It may be
    used to degrade an *optional* stateless cloud mount because every mount
    candidate was newly owned by this claimant and strict cleanup proved its
    process and mount-table state absent.  Probe ambiguity, an adopted
    predecessor resident, or a failed rollback never produces this type.
    """


@dataclass(frozen=True)
class RcloneMountState:
    mount_id: str
    mount_kind: str
    target_path: str
    workspace_name: str
    remote_name: str
    state_dir: str
    cache_dir: str
    config_path: str
    filter_path: str
    pid_file: str
    identity_file: str
    resident_generation: str
    resident_spec_digest: str
    rc_addr: str
    rc_user: str
    rc_pass: str
    hard_cache_limit: str
    hard_cache_limit_bytes: int
    # Home-relative twin of state_dir, for write_home_file pushes.
    state_rel: str = ""
    # Bearer-token plumbing (Keycloak auth modes only).
    token_path: str = ""
    token_helper_path: str = ""
    uses_keycloak_auth: bool = False


@dataclass(frozen=True)
class RcloneCacheUsage:
    mount_id: str
    workspace_name: str
    target_path: str
    cache_dir: str
    cache_bytes: int
    hard_limit_bytes: int
    mounted: bool

    @property
    def over_hard_limit(self) -> bool:
        return self.hard_limit_bytes > 0 and self.cache_bytes >= self.hard_limit_bytes


class RcloneMountManager:
    """Start and stop rclone mounts in a remote workspace runtime."""

    def __init__(
        self,
        *,
        thread_id: str,
        cloud_cfg: dict[str, Any],
        workspace_backend: Any,
        workspace_root: Path,
    ) -> None:
        self.thread_id = thread_id
        self.cloud_cfg = cloud_cfg or {}
        self.workspace_backend = workspace_backend
        remote_root = getattr(workspace_backend, "root", None)
        self.workspace_root = str(remote_root or workspace_root)
        self._states: list[RcloneMountState] = []
        # Keycloak bearer plumbing, keyed by mount index in cloud_cfg order
        # (same enumerate order as _start_all_sync, which is all-or-nothing,
        # so self._states[i] corresponds to mount index i).
        self._token_clients: dict[int, KeycloakTokenClient] = {}
        # Latest bearer minted for each mount.  The same value seeds both the
        # first mount and any later ENOTCONN restart; keeping only the initial
        # token here would let a restart overwrite a refreshed workspace token
        # with an expired one.
        self._initial_tokens: dict[int, str] = {}
        self._token_state_lock = threading.Lock()
        self._refresh_task: asyncio.Task | None = None
        self._detached = False
        self._script_nonce = secrets.token_hex(8)
        # Original mount dicts + their index, retained so restart_mount() can
        # re-run the same generators (_unmount_script/_mount_script) that
        # _start_all_sync used, keyed by mount_id (design §11.6 #1 — Slice B
        # deferral: RcloneMountManager retained no mount dicts before this).
        self._mounts_by_id: dict[str, dict[str, Any]] = {}
        self._mount_index_by_id: dict[str, int] = {}

    @property
    def active(self) -> bool:
        return bool(self._states)

    @property
    def mounts(self) -> list[RcloneMountState]:
        return list(self._states)

    def status(self) -> str:
        """Return an agent-safe status summary for active cloud mounts."""
        if not self._states:
            return "No active rclone cloud mounts."
        try:
            rows = self._collect_status_sync()
        except Exception as exc:
            return f"Cloud mount status unavailable: {exc}"

        by_id = {row.get("mount_id"): row for row in rows}
        lines = ["Cloud mount status:"]
        for state in self._states:
            row = by_id.get(state.mount_id, {})
            cache_bytes = int(row.get("cache_bytes") or 0)
            mounted = row.get("mounted") == "yes"
            limit = state.hard_cache_limit_bytes
            usage = f"{_format_bytes(cache_bytes)}" + (
                f" / {_format_bytes(limit)} hard limit" if limit else ""
            )
            status = "mounted" if mounted else "not mounted"
            if limit and cache_bytes >= limit:
                status = "cache limit reached"
            lines.append(
                f"- {state.workspace_name} ({state.mount_kind}) at "
                f"{state.target_path}: {status}, cache {usage}"
            )
            core = str(row.get("rc_core") or "").strip()
            vfs = str(row.get("rc_vfs") or "").strip()
            if core:
                lines.append(f"  core/stats: {_compact_rc(core)}")
            if vfs:
                lines.append(f"  vfs/stats: {_compact_rc(vfs)}")
        return "\n".join(lines)

    def refresh_vfs(
        self, mount_id: str | None = None, *, recursive: bool = True
    ) -> None:
        """Flush the rclone VFS so a subsequent read sees the live cloud.

        Uses ``vfs/refresh`` (re-PROPFINDs, invalidates changed content) — NOT
        ``vfs/forget``, which does not flush already-read file content (design
        §11.2). Applies to every active mount when ``mount_id`` is None.
        """
        targets = [
            s for s in self._states if mount_id is None or s.mount_id == mount_id
        ]
        for state in targets:
            self._run_remote_script(
                f"vfs_refresh_{state.remote_name}.sh",
                self._vfs_refresh_script(state, recursive=recursive),
                timeout=120,
            )

    def _vfs_refresh_script(self, state: RcloneMountState, *, recursive: bool) -> str:
        rec = "true" if recursive else "false"
        return f"""#!/usr/bin/env bash
set +e
rclone rc --rc-addr {shlex.quote(state.rc_addr)} --rc-user {shlex.quote(state.rc_user)} --rc-pass {shlex.quote(state.rc_pass)} vfs/refresh recursive={rec} >/dev/null 2>&1
rc=$?
if [ "$rc" -ne 0 ]; then
  echo "vfs/refresh failed rc=$rc" >&2
  exit "$rc"
fi
echo "{_OK}"
"""

    def cache_usages(self) -> list[RcloneCacheUsage]:
        """Return current per-mount VFS cache usage."""
        if not self._states:
            return []
        return self._collect_cache_usage_sync()

    def cache_limit_message(self) -> str | None:
        """Return a blocking message when any mount is over its hard cache guard."""
        breaches = [usage for usage in self.cache_usages() if usage.over_hard_limit]
        if not breaches:
            return None

        lines = [
            "Cloud cache guard: this command was not run because the rclone "
            "VFS cache has reached its hard limit.",
            "",
        ]
        for usage in breaches:
            lines.append(
                f"- {usage.workspace_name} at {usage.target_path}: "
                f"{_format_bytes(usage.cache_bytes)} used, "
                f"limit {_format_bytes(usage.hard_limit_bytes)}"
            )
        lines.extend(
            [
                "",
                "Wait for rclone cache cleanup, narrow the cloud operation, "
                "or ask the operator to increase the one-time/cloud budget.",
            ]
        )
        return "\n".join(lines)

    async def start_all(self) -> None:
        if self._detached:
            raise RcloneMountError("cloud mount controller has been detached")
        await self._prepare_keycloak_tokens()
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._start_all_sync)
        except Exception:
            await self._aclose_token_clients()
            raise
        self._start_token_refresh()

    async def detach_for_handoff(self) -> None:
        """Retire only the agent-local controller, preserving remote mounts.

        This is the stateless claim-switch path.  It performs no remote
        mutation: the healthy rclone process and VFS cache remain owned by the
        workspace runtime for the successor to adopt.  Waiting for the refresh
        task and then retiring the backend's separate claim-resource admission
        makes return from this method the local quiescence acknowledgement.
        """

        self._detached = True
        await self._stop_local_controller()
        retire = getattr(self.workspace_backend, "retire_claim_resource_owner", None)
        if retire is not None:
            await asyncio.to_thread(retire)
        self._states = []
        self._mounts_by_id = {}
        self._mount_index_by_id = {}

    async def aclose(self, *, strict: bool = False) -> None:
        """Terminally stop the controller and its workspace-side mounts.

        ``strict`` is the stateless claimant-quiescence boundary: every exact
        resident must report its mount/process gone before the controller is
        detached. Pinned callers retain the historical best-effort cleanup.
        """

        if self._detached:
            return
        await self._stop_local_controller()
        loop = asyncio.get_running_loop()
        closed = False
        try:
            await loop.run_in_executor(None, self._close_sync, strict)
            closed = True
        except Exception:
            if strict:
                # Keep exact resident state and claim-resource admission so a
                # second teardown can retry. The queue claimant must not
                # publish completion/release after this exception.
                raise
            logger.debug("rclone terminal cleanup failed", exc_info=True)
        finally:
            if not strict or closed:
                retire = getattr(
                    self.workspace_backend, "retire_claim_resource_owner", None
                )
                if retire is not None:
                    await asyncio.to_thread(retire)
                self._detached = True

    async def _stop_local_controller(self) -> None:
        """Cancel/close only process-local refresh authority."""

        if self._refresh_task is not None:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.debug("cloud mount token refresh task exit", exc_info=True)
            self._refresh_task = None
        await self._aclose_token_clients()

    # ------------------------------------------------------- Keycloak bearers

    async def _prepare_keycloak_tokens(self) -> None:
        """Mint initial bearer tokens for Keycloak-auth mounts.

        Runs in the agent's event loop *before* the blocking mount setup so
        the generated mount scripts can seed the workspace-side token file.
        The client secret never leaves the agent process — only the
        short-lived access token is shipped to the workspace runtime.
        """
        for index, mount in enumerate(self.cloud_cfg.get("mounts") or []):
            auth = mount.get("auth") or {}
            if auth.get("type") not in _KEYCLOAK_AUTH_TYPES:
                continue
            issuer = str(auth.get("issuer") or "")
            client_id = str(auth.get("client_id") or "")
            client_secret = str(auth.get("client_secret") or "")
            if not (issuer and client_id and client_secret):
                raise RcloneMountError(
                    f"mount {mount.get('mount_id') or index}: keycloak auth "
                    "payload is missing issuer/client_id/client_secret"
                )
            client = KeycloakTokenClient(
                issuer=issuer,
                client_id=client_id,
                client_secret=client_secret,
                target_user_sub=str(auth.get("target_user_sub") or "") or None,
            )
            try:
                bearer = await client.get_bearer()
            except Exception as exc:
                await client.aclose()
                await self._aclose_token_clients()
                raise RcloneMountError(
                    f"mount {mount.get('mount_id') or index}: could not mint "
                    f"initial bearer token: {exc}"
                ) from exc
            self._token_clients[index] = client
            self._remember_token(index, bearer.token)

    async def _aclose_token_clients(self) -> None:
        for client in self._token_clients.values():
            try:
                await client.aclose()
            except Exception:
                pass
        self._token_clients.clear()
        with self._token_state_lock:
            self._initial_tokens.clear()

    def _remember_token(self, index: int, token: str) -> None:
        """Retain the newest bearer for both the live helper and remounts."""
        with self._token_state_lock:
            self._initial_tokens[index] = token

    def _token_for_index(self, index: int) -> str | None:
        with self._token_state_lock:
            return self._initial_tokens.get(index)

    def _start_token_refresh(self) -> None:
        if not self._token_clients or self._refresh_task is not None:
            return
        self._refresh_task = asyncio.create_task(
            self._token_refresh_loop(),
            name=f"cloud-mount-token-refresh-{self.thread_id[:8]}",
        )

    def _next_refresh_delay(self) -> float:
        ttls = []
        for client in self._token_clients.values():
            # Same monotonic clock the token client stamps expiry with.
            expires_at = getattr(client, "_token_expires_at", 0.0)
            ttls.append(max(0.0, expires_at - time.monotonic()))
        if not ttls:
            return _TOKEN_REFRESH_MIN_INTERVAL_SECONDS
        return max(
            _TOKEN_REFRESH_MIN_INTERVAL_SECONDS,
            min(ttls) - _TOKEN_REFRESH_SAFETY_SECONDS,
        )

    async def _token_refresh_loop(self) -> None:
        """Re-mint and push bearer tokens before they expire.

        rclone re-runs ``bearer_token_command`` when a request 401s, so a
        push that lands late self-heals on the next remote call. Failures
        here are logged and retried — never fatal to the session.
        """
        loop = asyncio.get_running_loop()
        delay = self._next_refresh_delay()
        while True:
            try:
                await asyncio.sleep(delay)
                await self._refresh_keycloak_tokens_once(loop=loop)
                delay = self._next_refresh_delay()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "cloud mount token refresh failed (thread=%s); retrying in %ss",
                    self.thread_id,
                    _TOKEN_REFRESH_MIN_INTERVAL_SECONDS,
                    exc_info=True,
                )
                delay = _TOKEN_REFRESH_MIN_INTERVAL_SECONDS

    async def _refresh_keycloak_tokens_once(
        self, *, loop: asyncio.AbstractEventLoop | None = None
    ) -> None:
        """Mint and publish one refresh cycle for every active bearer mount."""
        if loop is None:
            loop = asyncio.get_running_loop()
        for index, client in self._token_clients.items():
            state = self._state_for_index(index)
            if state is None or not state.uses_keycloak_auth:
                continue
            bearer = await client.get_bearer(force_refresh=True)
            # Store before the remote push.  If that push loses a race with an
            # ENOTCONN restart, the restart still seeds the newly-minted token
            # instead of resurrecting the expired attach-time bearer.
            self._remember_token(index, bearer.token)
            await loop.run_in_executor(None, self._push_token_sync, state, bearer.token)

    def _state_for_index(self, index: int) -> RcloneMountState | None:
        if 0 <= index < len(self._states):
            return self._states[index]
        return None

    def _push_token_sync(self, state: RcloneMountState, token: str) -> None:
        """Atomically replace the workspace-side token file.

        Written to a sibling tmp path first, then ``mv``-ed over the live
        file so rclone's helper never reads a half-written token. chmod is
        re-applied because the tmp file is created with default SFTP mode.
        """
        if not bool(getattr(self.workspace_backend, "claim_resource_fenced", False)):
            # Historical pinned path: SFTP keeps the bearer out of the remote
            # process command line, and a unique sibling avoids writer
            # collisions before the atomic rename.
            tmp_rel = f"{state.state_rel}/bearer.token.new.{secrets.token_hex(8)}"
            self.workspace_backend.write_home_file(tmp_rel, token + "\n")
            tmp_abs = self.workspace_backend.resolve_home_path(tmp_rel)
            quoted_tmp = shlex.quote(tmp_abs)
            self.workspace_backend.exec_command(
                f"chmod 600 -- {quoted_tmp} && "
                f"mv -f -- {quoted_tmp} {shlex.quote(state.token_path)}; "
                "rc=$?; "
                f"rm -f -- {quoted_tmp}; "
                'exit "$rc"',
                timeout=15,
            )
            return

        # Stage, chmod, and publish inside ONE claim-fenced remote command.
        # An SFTP upload followed by a fenced mv looks harmless, but it still
        # lets a cancelled N mutate the workspace after N+1 owns the lock.
        # Base64 keeps arbitrary bearer bytes out of shell syntax; this is
        # still a cooperative workload-user boundary, not secret isolation.
        encoded = base64.b64encode((token + "\n").encode("utf-8")).decode("ascii")
        quoted_dir = shlex.quote(state.state_dir)
        self._exec_resource(
            "set -e; umask 077; "
            f"mkdir -p -- {quoted_dir}; "
            f"tmp=$(mktemp {quoted_dir}/.bearer.token.XXXXXXXX) || exit 78; "
            "trap 'rm -f -- \"$tmp\"' EXIT HUP INT TERM; "
            f'printf %s {shlex.quote(encoded)} | base64 -d > "$tmp"; '
            'chmod 600 -- "$tmp"; '
            f'mv -f -- "$tmp" {shlex.quote(state.token_path)}; '
            "trap - EXIT HUP INT TERM",
            timeout=15,
            operation=f"publish bearer token for {state.mount_id}",
        )

    def _start_all_sync(self) -> None:
        if self.cloud_cfg.get("driver") != "rclone":
            raise RcloneMountError("cloud_mount driver must be 'rclone'")
        if not self.cloud_cfg.get("mounts"):
            return
        if not hasattr(self.workspace_backend, "exec_command"):
            raise RcloneMountError(
                "rclone cloud_mount requires a shell-capable workspace"
            )
        if not hasattr(self.workspace_backend, "write_home_file"):
            raise RcloneMountError("rclone cloud_mount requires remote home writes")

        states: list[RcloneMountState] = []
        attempted_states: list[RcloneMountState] = []
        adopted_states: list[RcloneMountState] = []
        mounts_by_id: dict[str, dict[str, Any]] = {}
        mount_index_by_id: dict[str, int] = {}
        try:
            for index, mount in enumerate(self.cloud_cfg.get("mounts") or []):
                state = self._state_for_mount(mount, index)
                started = time.perf_counter()
                state, disposition = self._start_or_adopt_mount(
                    mount,
                    state,
                    index=index,
                    record_owned=attempted_states.append,
                )
                states.append(state)
                if disposition == "adopted":
                    adopted_states.append(state)
                mounts_by_id[state.mount_id] = mount
                mount_index_by_id[state.mount_id] = index
                logger.info(
                    "rclone resident timing: thread=%s mount_id=%s "
                    "disposition=%s total=%.3fs target=%s",
                    self.thread_id,
                    state.mount_id,
                    disposition,
                    time.perf_counter() - started,
                    state.target_path,
                )

            if not self.cloud_cfg.get("skip_workspace_links"):
                self._install_workspace_links(states)
        except Exception as exc:
            strict_stateless = bool(
                getattr(self.workspace_backend, "claim_resource_fenced", False)
            )
            rollback_clean = self._rollback_start_sync(
                attempted_states,
                strict=strict_stateless,
            )
            self._states = []
            self._mounts_by_id = {}
            self._mount_index_by_id = {}
            # A typed degraded result is legal only when every requested mount
            # crossed the new-generation ownership boundary and every one was
            # then proven absent.  A failure before that point may hide an
            # unknown predecessor resident; an adopted resident must remain
            # available to the successor and therefore cannot be discarded as
            # a clean optional-mount failure.
            if (
                strict_stateless
                and rollback_clean
                and not adopted_states
                and len(attempted_states)
                == len(list(self.cloud_cfg.get("mounts") or []))
            ):
                raise RcloneMountCleanFailure(
                    "optional cloud mount is unavailable after exact resident cleanup"
                ) from exc
            raise

        self._states = states
        self._mounts_by_id = mounts_by_id
        self._mount_index_by_id = mount_index_by_id

    async def retire_existing(self, *, drain: bool = True) -> dict[str, int]:
        """Strictly drain and remove already-resident mounts without starting any.

        Public stateless End reconstructs this manager from the same
        orchestrator-resolved mount payload used at attach.  It must never run
        ``start_all``: an absent resident is success, while starting one would
        create new writeback work during snapshot retirement.  Every mutation
        runs through RemoteBackend's terminal-token adoption primitive.
        """

        if self.cloud_cfg.get("driver") != "rclone":
            raise RcloneMountError("terminal cloud cleanup requires rclone driver")
        execute = getattr(self.workspace_backend, "exec_terminal_claim_resource", None)
        if not callable(execute):
            raise RcloneMountError(
                "terminal cloud cleanup requires a terminal-fenced backend"
            )
        mounts = list(self.cloud_cfg.get("mounts") or [])
        expected = [
            self._state_for_mount(dict(mount), index)
            for index, mount in enumerate(mounts)
        ]

        def _retire() -> None:
            for state in reversed(expected):
                self._run_terminal_remote_script(
                    f"terminal_unmount_{state.remote_name}.sh",
                    self._terminal_unmount_script(state, drain=drain),
                    timeout=90,
                )
            self._run_terminal_remote_script(
                "terminal_verify_rclone_zero.sh",
                self._terminal_zero_script(),
                timeout=30,
            )

        await asyncio.to_thread(_retire)
        return {"rclone_mounts": len(expected), "rclone_processes": 0}

    def _start_or_adopt_mount(
        self,
        mount: dict[str, Any],
        state: RcloneMountState,
        *,
        index: int,
        record_owned: Callable[[RcloneMountState], None],
    ) -> tuple[RcloneMountState, str]:
        """Cold-mount pinned resources; adopt-or-heal stateless residents."""

        token = self._token_for_index(index)
        if not bool(getattr(self.workspace_backend, "claim_resource_fenced", False)):
            script = self._mount_script(mount, state, initial_token=token)
            record_owned(state)
            self._run_remote_script(f"mount_{state.remote_name}.sh", script, timeout=90)
            return state, "cold"

        # A successor always publishes its newly minted bearer before the
        # real directory probe.  An idle resident whose old token expired can
        # therefore recover on its first WebDAV request without an agent-side
        # daemon staying alive between turns.
        if state.uses_keycloak_auth:
            if not token:
                raise RcloneMountError(
                    f"mount {state.mount_id}: no fresh bearer available for adoption"
                )
            self._push_token_sync(state, token)

        probe = self._run_remote_script(
            f"probe_{state.remote_name}.sh",
            self._resident_probe_script(mount, state),
            timeout=30,
            require_ok=False,
        )
        parsed = self._parse_resident_probe(state, probe)
        if parsed is not None and self._resident_adoption_safe(mount):
            return parsed, "adopted"

        # Missing/dead residents reach here only after the probe proved there
        # is no unknown live process at the target.  A valid-but-unhealthy
        # resident carries its persisted RC identity in ``state`` and is
        # stopped by the exact PID/cmdline checks in _unmount_script.
        healed_state = parsed or self._resident_state_from_probe(state, probe) or state
        self._run_remote_script(
            f"unmount_{healed_state.remote_name}.sh",
            self._unmount_script(healed_state),
            timeout=45,
            # A remount must never proceed from best-effort cleanup. In
            # particular, stale ENOTCONN FUSE targets can make mountpoint(1)
            # lie; the exact script must prove mount-table absence and an
            # accessible backing directory before returning _OK.
            require_ok=True,
        )
        healed_state = replace(
            state,
            resident_generation=uuid.uuid4().hex,
            rc_pass=secrets.token_urlsafe(24),
        )
        script = self._mount_script(mount, healed_state, initial_token=token)
        record_owned(healed_state)
        self._run_remote_script(
            f"mount_{healed_state.remote_name}.sh", script, timeout=90
        )
        return healed_state, "healed"

    @staticmethod
    def _resident_adoption_safe(mount: dict[str, Any]) -> bool:
        """Whether every mount-affecting input can be checked without secrets.

        Unknown source keys and arbitrary provider flags can carry credentials
        or change process semantics. Rather than persist a digest oracle, those
        configurations conservatively remount on every claim.
        """

        source = mount.get("source") or {}
        source_keys = {str(key) for key in dict(source.get("config") or {})}
        if source_keys - _IDENTITY_SOURCE_KEYS:
            return False
        if mount.get("provider_flags"):
            return False
        auth_type = str((mount.get("auth") or {}).get("type") or "")
        return auth_type in {"", "basic", *_KEYCLOAK_AUTH_TYPES}

    def _resident_probe_script(
        self,
        mount: dict[str, Any],
        state: RcloneMountState,
    ) -> str:
        """Validate persisted identity and perform a real mounted readdir.

        The identity is workload-user writable and therefore cooperative.  We
        parse it as inert data, validate every field, and corroborate its PID
        against ``/proc`` before either adoption or an exact stop is allowed.
        """

        basic_check = ":"
        auth = mount.get("auth") or {}
        if auth.get("type") == "basic" and auth.get("password") is not None:
            encoded = base64.b64encode(str(auth["password"]).encode()).decode()
            basic_check = f"""
_srw_stored_pass=$(sed -n 's/^pass = //p' {shlex.quote(state.config_path)} | head -n1)
if [ -z "$_srw_stored_pass" ] || [ "$(rclone reveal "$_srw_stored_pass" 2>/dev/null)" != "$(printf %s {shlex.quote(encoded)} | base64 -d)" ]; then
  printf '%s\\t%s\\t%s\\t%s\\t%s\\n' {_RESIDENT_HEAL} "$_srw_spec" "$_srw_generation" "$_srw_pid" "$_srw_pass"
  exit 0
fi
"""
        return f"""#!/usr/bin/env bash
set -euo pipefail
identity={shlex.quote(state.identity_file)}
target={shlex.quote(state.target_path)}
if [ ! -f "$identity" ]; then
  mountpoint -q "$target" && exit 83
  echo "{_RESIDENT_HEAL}"
  exit 0
fi
[ "$(wc -l < "$identity")" -eq 1 ] || exit 84
IFS='|' read -r _srw_version _srw_spec _srw_status _srw_generation _srw_pid _srw_pass _srw_extra < "$identity"
[ "$_srw_version" = {_RESIDENT_IDENTITY_VERSION} ] || exit 84
printf %s "$_srw_spec" | grep -Eq '^[0-9a-f]{{64}}$' || exit 84
[ -z "$_srw_extra" ] || exit 84
case "$_srw_status" in active|creating) ;; *) exit 84 ;; esac
printf %s "$_srw_generation" | grep -Eq '^[0-9a-f]{{32}}$' || exit 84
printf %s "$_srw_pid" | grep -Eq '^[0-9]+$' || exit 84
printf %s "$_srw_pass" | grep -Eq '^[A-Za-z0-9_-]{{20,128}}$' || exit 84
if [ "$_srw_pid" = 0 ]; then
  for _srw_i in $(seq 1 20); do
    [ -s {shlex.quote(state.pid_file)} ] && break
    sleep 0.1
  done
  if [ -s {shlex.quote(state.pid_file)} ]; then
    _srw_pid=$(cat {shlex.quote(state.pid_file)})
    printf %s "$_srw_pid" | grep -Eq '^[1-9][0-9]*$' || exit 84
  else
    mountpoint -q "$target" && exit 85
    printf '%s\\t%s\\t%s\\t0\\t%s\\n' {_RESIDENT_HEAL} "$_srw_spec" "$_srw_generation" "$_srw_pass"
    exit 0
  fi
fi
_srw_pid_matches() {{
  [ -r "/proc/$_srw_pid/cmdline" ] || return 1
  _srw_args=$(tr '\\0' '\\n' < "/proc/$_srw_pid/cmdline") || return 1
  _srw_exe=$(printf '%s\\n' "$_srw_args" | head -n1)
  [ "$(basename "$_srw_exe")" = rclone ] || return 1
  for _srw_expected in mount {shlex.quote(state.config_path)} {shlex.quote(state.target_path)} {shlex.quote(state.rc_addr)}; do
    printf '%s\\n' "$_srw_args" | grep -Fqx -- "$_srw_expected" || return 1
  done
}}
if ! _srw_pid_matches; then
  mountpoint -q "$target" && exit 85
  printf '%s\\t%s\\t%s\\t%s\\t%s\\n' {_RESIDENT_HEAL} "$_srw_spec" "$_srw_generation" "$_srw_pid" "$_srw_pass"
  exit 0
fi
if [ "$_srw_status" = creating ]; then
  printf '%s\\t%s\\t%s\\t%s\\t%s\\n' {_RESIDENT_HEAL} "$_srw_spec" "$_srw_generation" "$_srw_pid" "$_srw_pass"
  exit 0
fi
if [ "$_srw_spec" != {shlex.quote(state.resident_spec_digest)} ]; then
  printf '%s\\t%s\\t%s\\t%s\\t%s\\n' {_RESIDENT_HEAL} "$_srw_spec" "$_srw_generation" "$_srw_pid" "$_srw_pass"
  exit 0
fi
{basic_check}
if ! timeout 10 rclone rc --rc-addr {shlex.quote(state.rc_addr)} --rc-user {shlex.quote(state.rc_user)} --rc-pass "$_srw_pass" vfs/refresh recursive=false >/dev/null 2>&1; then
  printf '%s\\t%s\\t%s\\t%s\\t%s\\n' {_RESIDENT_HEAL} "$_srw_spec" "$_srw_generation" "$_srw_pid" "$_srw_pass"
  exit 0
fi
if mountpoint -q "$target" && timeout 15 find "$target" -mindepth 1 -maxdepth 1 -print -quit >/dev/null; then
  printf '%s\\t%s\\t%s\\t%s\\t%s\\n' {_RESIDENT_ADOPTED} "$_srw_spec" "$_srw_generation" "$_srw_pid" "$_srw_pass"
else
  printf '%s\\t%s\\t%s\\t%s\\t%s\\n' {_RESIDENT_HEAL} "$_srw_spec" "$_srw_generation" "$_srw_pid" "$_srw_pass"
fi
"""

    @staticmethod
    def _resident_probe_fields(
        output: str, tag: str
    ) -> tuple[str, str, str, str] | None:
        for line in output.splitlines():
            parts = line.split("\t")
            if len(parts) == 5 and parts[0] == tag:
                spec_digest, generation, pid, rc_pass = parts[1:]
                if (
                    re.fullmatch(r"[0-9a-f]{64}", spec_digest)
                    and re.fullmatch(r"[0-9a-f]{32}", generation)
                    and re.fullmatch(r"(?:0|[1-9][0-9]*)", pid)
                    and re.fullmatch(r"[A-Za-z0-9_-]{20,128}", rc_pass)
                ):
                    return spec_digest, generation, pid, rc_pass
                raise RcloneMountError(
                    "resident rclone probe returned malformed identity"
                )
        return None

    def _parse_resident_probe(
        self,
        state: RcloneMountState,
        output: str,
    ) -> RcloneMountState | None:
        fields = self._resident_probe_fields(output, _RESIDENT_ADOPTED)
        if fields is None:
            return None
        spec_digest, generation, _pid, rc_pass = fields
        if _pid == "0":
            raise RcloneMountError("resident rclone adoption has no live PID")
        if spec_digest != state.resident_spec_digest:
            raise RcloneMountError("resident rclone adoption spec does not match")
        return replace(state, resident_generation=generation, rc_pass=rc_pass)

    def _resident_state_from_probe(
        self,
        state: RcloneMountState,
        output: str,
    ) -> RcloneMountState | None:
        fields = self._resident_probe_fields(output, _RESIDENT_HEAL)
        if fields is None:
            if _RESIDENT_HEAL in output:
                return None
            raise RcloneMountError("resident rclone probe returned no disposition")
        spec_digest, generation, _pid, rc_pass = fields
        return replace(
            state,
            resident_spec_digest=spec_digest,
            resident_generation=generation,
            rc_pass=rc_pass,
        )

    def _rollback_start_sync(
        self,
        states: list[RcloneMountState],
        *,
        strict: bool = False,
    ) -> bool:
        """Reverse partial startup and report whether exact cleanup succeeded.

        Pinned sessions retain best-effort cleanup.  Stateless callers request
        strict output validation because only that positive zero proof can
        authorize optional-cloud degradation.
        """

        clean = True
        for state in reversed(states):
            try:
                self._run_remote_script(
                    f"unmount_{state.remote_name}.sh",
                    self._unmount_script(state),
                    timeout=45,
                    require_ok=strict,
                )
            except Exception:
                clean = False
                logger.warning(
                    "rclone partial-start rollback failed: thread=%s "
                    "mount_id=%s target=%s",
                    self.thread_id,
                    state.mount_id,
                    state.target_path,
                    exc_info=True,
                )
        return clean

    def restart_mount(self, mount_id: str) -> None:
        """Unmount then remount ONE mount in place (ENOTCONN heal path).

        Synchronous like ``_start_all_sync`` — callers run this via
        ``asyncio.to_thread``/an executor, never from the event loop directly.
        Re-runs the same ``_unmount_script``/``_mount_script`` generators
        against the retained mount dict + state, so the remount is
        byte-identical to the original mount. Raises ``RcloneMountError`` for
        an unknown ``mount_id`` or when the remount script itself fails (the
        unmount step is best-effort — the target may already be wedged/dead,
        which is exactly why we're here).
        """
        index = self._mount_index_by_id.get(mount_id)
        mount = self._mounts_by_id.get(mount_id)
        state = self._state_for_index(index) if index is not None else None
        if index is None or mount is None or state is None:
            raise RcloneMountError(
                f"restart_mount: unknown or inactive mount_id {mount_id!r}"
            )

        started = time.perf_counter()
        self._run_remote_script(
            f"unmount_{state.remote_name}.sh",
            self._unmount_script(state),
            timeout=45,
            require_ok=True,
        )
        state = replace(
            state,
            resident_generation=uuid.uuid4().hex,
            rc_pass=secrets.token_urlsafe(24),
        )
        script = self._mount_script(
            mount, state, initial_token=self._token_for_index(index)
        )
        self._run_remote_script(f"mount_{state.remote_name}.sh", script, timeout=90)
        self._states[index] = state
        logger.info(
            "rclone resident timing: thread=%s mount_id=%s disposition=healed "
            "total=%.3fs target=%s",
            self.thread_id,
            state.mount_id,
            time.perf_counter() - started,
            state.target_path,
        )

    def _state_for_mount(self, mount: dict[str, Any], index: int) -> RcloneMountState:
        cache = self._cache_for_mount(mount)
        mount_id = str(mount.get("mount_id") or f"mount-{index}")
        workspace_name = str(mount.get("workspace_name") or f"mount-{index}")
        safe_name = "".join(
            ch.lower() if ch.isalnum() else "-" for ch in workspace_name
        ).strip("-")
        safe_name = safe_name or f"mount-{index}"
        remote_name = f"srw-{self.thread_id[:8]}-{safe_name}"
        state_rel = f".cache/srw/rclone/{self.thread_id}/{safe_name}"
        state_dir = self.workspace_backend.resolve_home_path(state_rel)
        cache_dir = f"{state_dir}/vfs-cache"
        config_path = f"{state_dir}/rclone.conf"
        filter_path = f"{state_dir}/rclone-excludes.txt"
        pid_file = f"{state_dir}/rclone.pid"
        identity_file = f"{state_dir}/resident.identity"
        hard_cache_limit = str(cache.get("hard_cache_limit") or "")
        hard_cache_limit_bytes = _parse_size_to_bytes(hard_cache_limit) or 0
        port = 43000 + (
            int(
                hashlib.sha256(f"{self.thread_id}:{index}".encode()).hexdigest()[:8],
                16,
            )
            % 10000
        )
        auth_type = str((mount.get("auth") or {}).get("type") or "")
        # Identity is intentionally NON-SECRET.  A digest of a password or
        # client_secret would be a durable offline oracle in the workload
        # user's home.  Static-basic equality is checked transiently inside
        # the fenced adoption probe; Keycloak residents consume the freshly
        # published bearer instead.
        source = mount.get("source") or {}
        source_config = {
            str(key): value
            for key, value in dict(source.get("config") or {}).items()
            if str(key) in _IDENTITY_SOURCE_KEYS
        }
        auth = mount.get("auth") or {}
        safe_spec = {
            "mount_id": mount_id,
            "mount_kind": str(mount.get("mount_kind") or "cloud"),
            "target_path": str(mount.get("target_path") or f"/cloud/{safe_name}"),
            "workspace_name": workspace_name,
            "backend": mount.get("backend"),
            "source": {
                "type": source.get("type"),
                "root": source.get("root"),
                "config": source_config,
            },
            "auth": {
                "type": auth.get("type"),
                "issuer": auth.get("issuer"),
                "client_id": auth.get("client_id"),
                "target_user_sub": auth.get("target_user_sub"),
            },
            "access": mount.get("access"),
            "cache": self._cache_for_mount(mount),
            "filters": mount.get("filters"),
            "effective_default_ignores": self._default_ignore_lines(mount),
            "adoption_safe": self._resident_adoption_safe(mount),
            "min_rclone_version": mount.get("min_rclone_version"),
            "remote_name": remote_name,
            "state_dir": state_dir,
        }
        resident_spec_digest = hashlib.sha256(
            json.dumps(safe_spec, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return RcloneMountState(
            mount_id=mount_id,
            mount_kind=str(mount.get("mount_kind") or "cloud"),
            target_path=str(mount.get("target_path") or f"/cloud/{safe_name}"),
            workspace_name=workspace_name,
            remote_name=remote_name,
            state_dir=state_dir,
            cache_dir=cache_dir,
            config_path=config_path,
            filter_path=filter_path,
            pid_file=pid_file,
            identity_file=identity_file,
            resident_generation=uuid.uuid4().hex,
            resident_spec_digest=resident_spec_digest,
            rc_addr=f"127.0.0.1:{port}",
            rc_user=f"srw-{self.thread_id[:8]}",
            rc_pass=secrets.token_urlsafe(24),
            hard_cache_limit=hard_cache_limit,
            hard_cache_limit_bytes=hard_cache_limit_bytes,
            state_rel=state_rel,
            token_path=f"{state_dir}/bearer.token",
            token_helper_path=f"{state_dir}/bearer-helper.sh",
            uses_keycloak_auth=auth_type in _KEYCLOAK_AUTH_TYPES,
        )

    def _mount_script(
        self,
        mount: dict[str, Any],
        state: RcloneMountState,
        *,
        initial_token: str | None = None,
    ) -> str:
        source = mount.get("source") or {}
        source_type = str(source.get("type") or "")
        source_config = dict(source.get("config") or {})
        auth = mount.get("auth") or {}
        token_setup_block = ":"
        if auth.get("type") == "basic" and auth.get("password"):
            source_config["pass"] = auth["password"]
        elif auth.get("type") in _KEYCLOAK_AUTH_TYPES:
            if not initial_token:
                raise RcloneMountError(
                    f"mount {state.mount_id}: no initial bearer token prepared "
                    "for keycloak auth"
                )
            # rclone executes this helper to obtain (and on 401, re-obtain)
            # the bearer; the agent-side refresh loop keeps the token file
            # it reads fresh.
            source_config["bearer_token_command"] = state.token_helper_path
            token_setup_block = self._token_setup_block(state, initial_token)
        if not source_type or not source_config:
            raise RcloneMountError(f"mount {state.mount_id} has no rclone source")
        min_rclone_version = str(mount.get("min_rclone_version") or "").strip()
        version_check_block = (
            self._version_check_block(min_rclone_version) if min_rclone_version else ":"
        )

        create_args = [
            "rclone",
            "--config",
            state.config_path,
            "config",
            "create",
            state.remote_name,
            source_type,
            "--obscure",
            "--non-interactive",
            # POSIX end-of-options: every following token is a positional
            # ``key value`` pair. Without this, rclone (cobra) parses any
            # config VALUE that begins with ``-`` as a flag — and ``pass`` is a
            # random Nextcloud reader credential that can legitimately start
            # with ``-`` (e.g. ``-Yi9OE…``), which aborts the mount with
            # "unknown shorthand flag". Flags must precede ``--``.
            "--",
        ]
        for key, value in source_config.items():
            create_args.extend([str(key), str(value)])

        cache = self._cache_for_mount(mount)
        claim_fenced = bool(
            getattr(self.workspace_backend, "claim_resource_fenced", False)
        )
        mount_args = [
            *([] if claim_fenced else ["nohup"]),
            "rclone",
            "--config",
            state.config_path,
            "--cache-dir",
            state.cache_dir,
            "mount",
            self._remote_path(state.remote_name, str(source.get("root") or "")),
            state.target_path,
            "--rc",
            "--rc-addr",
            state.rc_addr,
            "--rc-user",
            state.rc_user,
            "--rc-pass",
            state.rc_pass,
            "--daemon-timeout",
            "30s",
        ]
        cache_flag_lines = []
        for key, flag in _CACHE_FLAG_MAP.items():
            if cache.get(key):
                cache_flag_lines.append(
                    "append_mount_flag "
                    f"{shlex.quote(flag)} {shlex.quote(str(cache[key]))}"
                )
        cache_flag_block = "\n".join(cache_flag_lines) if cache_flag_lines else ":"
        if str(mount.get("access") or "").lower() == "read_only":
            mount_args.append("--read-only")
        for flag in mount.get("provider_flags") or []:
            if flag:
                mount_args.append(str(flag))

        log_file = f"{state.state_dir}/rclone.log"
        mount_array = self._bash_array("MOUNT_ARGS", mount_args)
        create_command = " ".join(shlex.quote(arg) for arg in create_args)
        cloudignore_ref = self._remote_child_path(
            state.remote_name, str(source.get("root") or ""), ".cloudignore"
        )
        target_parent = str(Path(state.target_path).parent)
        default_ignore_file = f"{state.state_dir}/default.cloudignore"
        default_ignore_content = "\n".join(self._default_ignore_lines(mount))
        default_ignore_block = self._write_text_file_block(
            default_ignore_file, default_ignore_content
        )
        if claim_fenced:
            existing_mount_block = (
                f"mountpoint -q {shlex.quote(state.target_path)} && exit 86"
            )
        else:
            existing_mount_block = f"""if mountpoint -q {shlex.quote(state.target_path)}; then
  fusermount3 -u {shlex.quote(state.target_path)} 2>/dev/null || fusermount -u {shlex.quote(state.target_path)} 2>/dev/null || true
fi
if mountpoint -q {shlex.quote(state.target_path)}; then
  fusermount3 -uz {shlex.quote(state.target_path)} 2>/dev/null || fusermount -uz {shlex.quote(state.target_path)} 2>/dev/null || true
fi"""
        if claim_fenced:
            identity_template = shlex.quote(
                f"{state.state_dir}/.resident.identity.XXXXXXXX"
            )
            identity_setup_block = f"""_srw_write_identity() {{
  _srw_status="$1"
  _srw_pid="$2"
  _srw_tmp=$(mktemp {identity_template})
  trap 'rm -f -- "$_srw_tmp"' EXIT HUP INT TERM
  printf '%s|%s|%s|%s|%s|%s\\n' {_RESIDENT_IDENTITY_VERSION} {shlex.quote(state.resident_spec_digest)} "$_srw_status" {shlex.quote(state.resident_generation)} "$_srw_pid" {shlex.quote(state.rc_pass)} > "$_srw_tmp"
  chmod 600 "$_srw_tmp"
  mv -f -- "$_srw_tmp" {shlex.quote(state.identity_file)}
  trap - EXIT HUP INT TERM
}}
_srw_write_identity creating 0"""
            launch_block = f"""nohup bash -c 'pid_file="$1"; shift; printf "%s\\n" "$$" > "$pid_file"; exec "$@"' bash {shlex.quote(state.pid_file)} "${{MOUNT_ARGS[@]}}" > {shlex.quote(log_file)} 2>&1 &
for _i in $(seq 1 50); do
  [ -s {shlex.quote(state.pid_file)} ] && break
  sleep 0.1
done
_srw_pid=$(cat {shlex.quote(state.pid_file)})
printf %s "$_srw_pid" | grep -Eq '^[1-9][0-9]*$'
_srw_write_identity creating "$_srw_pid"
"""
            active_identity_block = f"""_srw_pid=$(cat {shlex.quote(state.pid_file)})
    printf %s "$_srw_pid" | grep -Eq '^[1-9][0-9]*$'
    _srw_write_identity active "$_srw_pid"
"""
        else:
            identity_setup_block = ":"
            launch_block = f""""${{MOUNT_ARGS[@]}}" > {shlex.quote(log_file)} 2>&1 &
echo "$!" > {shlex.quote(state.pid_file)}"""
            active_identity_block = ":"

        return f"""#!/usr/bin/env bash
set -euo pipefail
umask 077
trap 'rc=$?; echo "{_FAILED} rc=${{rc}}"; exit "${{rc}}"' ERR

command -v rclone >/dev/null
{version_check_block}
mkdir -p {shlex.quote(state.state_dir)} {shlex.quote(state.cache_dir)}
if ! mkdir -p {shlex.quote(target_parent)} {shlex.quote(state.target_path)} 2>/dev/null; then
  sudo mkdir -p {shlex.quote(target_parent)} {shlex.quote(state.target_path)}
  sudo chown "$(id -u):$(id -g)" /cloud {shlex.quote(target_parent)} {shlex.quote(state.target_path)}
fi
{token_setup_block}

{existing_mount_block}

{identity_setup_block}

{create_command} >/dev/null
chmod 600 {shlex.quote(state.config_path)}

{_CLOUDIGNORE_HELPERS}
: > {shlex.quote(state.filter_path)}
{default_ignore_block}
if [ -s {shlex.quote(default_ignore_file)} ]; then
  compile_cloudignore {shlex.quote(default_ignore_file)} {shlex.quote(state.filter_path)}
fi
if timeout 15 rclone --config {shlex.quote(state.config_path)} cat {shlex.quote(cloudignore_ref)} > {shlex.quote(state.state_dir + "/cloudignore.raw")} 2>/dev/null; then
  compile_cloudignore {shlex.quote(state.state_dir + "/cloudignore.raw")} {shlex.quote(state.filter_path)}
fi
sort -u {shlex.quote(state.filter_path)} -o {shlex.quote(state.filter_path)}

{mount_array}
MOUNT_HELP="$(rclone mount --help 2>/dev/null || true)"
append_mount_flag() {{
  local flag="$1"
  local value="$2"
  if [ -z "${{MOUNT_HELP}}" ] || grep -Fq -- "${{flag}}" <<<"${{MOUNT_HELP}}"; then
    MOUNT_ARGS+=("${{flag}}" "${{value}}")
  else
    printf 'Skipping unsupported rclone mount flag: %s\\n' "${{flag}}" >&2
  fi
}}
{cache_flag_block}
if [ -s {shlex.quote(state.filter_path)} ]; then
  MOUNT_ARGS+=(--exclude-from {shlex.quote(state.filter_path)})
fi
{launch_block}

for _i in $(seq 1 30); do
  if mountpoint -q {shlex.quote(state.target_path)}; then
    {active_identity_block}
    echo "{_OK}"
    exit 0
  fi
  if ! kill -0 "$(cat {shlex.quote(state.pid_file)})" 2>/dev/null; then
    break
  fi
  sleep 1
done

cat {shlex.quote(log_file)} 2>/dev/null || true
exit 1
"""

    def _cache_for_mount(self, mount: dict[str, Any]) -> dict[str, Any]:
        return {**_DEFAULT_CACHE, **dict(mount.get("cache") or {})}

    def _default_ignore_lines(self, mount: dict[str, Any]) -> list[str]:
        lines: list[str] = []
        for value in (
            os.getenv("SRW_CLOUD_MOUNT_DEFAULT_IGNORES"),
            self.cloud_cfg.get("default_ignores"),
            (mount.get("filters") or {}).get("default_ignores"),
            (mount.get("filters") or {}).get("exclude"),
            (mount.get("filters") or {}).get("excludes"),
        ):
            lines.extend(_coerce_ignore_lines(value))
        return lines

    def _install_workspace_links(self, states: list[RcloneMountState]) -> None:
        script_lines = [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            f"workspace={shlex.quote(self.workspace_root)}",
            'mkdir -p "${workspace}/.srw"',
            'entry="${workspace}/cloud"',
            'if [ -L "${entry}" ]; then rm "${entry}"; fi',
            'if [ -e "${entry}" ] && [ ! -d "${entry}" ]; then '
            'mv "${entry}" "${workspace}/.srw/cloud.pre-rclone.$(date +%s)"; fi',
        ]
        if len(states) == 1:
            script_lines.extend(
                [
                    'if [ -d "${entry}" ] && [ ! -L "${entry}" ]; then '
                    'mv "${entry}" "${workspace}/.srw/cloud.pre-rclone.$(date +%s)"; fi',
                    f"ln -sfn {shlex.quote(states[0].target_path)} " + '"${entry}"',
                ]
            )
        else:
            script_lines.append('mkdir -p "${entry}"')
            for state in states:
                script_lines.extend(
                    [
                        f'link="${{entry}}/{state.workspace_name}"',
                        'if [ -L "${link}" ]; then rm "${link}"; fi',
                        f"ln -sfn {shlex.quote(state.target_path)} " + '"${link}"',
                    ]
                )
        script_lines.append(f'echo "{_OK}"')
        self._run_remote_script("install_cloud_links.sh", "\n".join(script_lines))

    def _close_sync(self, strict: bool = False) -> None:
        failures: list[tuple[str, Exception]] = []
        for state in reversed(self._states):
            try:
                self._run_remote_script(
                    f"unmount_{state.remote_name}.sh",
                    self._unmount_script(state),
                    timeout=45,
                    require_ok=strict,
                )
            except Exception as exc:
                failures.append((state.mount_id, exc))
                logger.debug(
                    "rclone cloud mount cleanup failed for %s",
                    state.mount_id,
                    exc_info=True,
                )
        if strict and failures:
            failed_ids = ", ".join(mount_id for mount_id, _exc in failures)
            raise RcloneMountError(
                f"rclone residents did not retire: {failed_ids}"
            ) from failures[0][1]
        self._states = []
        self._mounts_by_id = {}
        self._mount_index_by_id = {}

    def _unmount_script(self, state: RcloneMountState) -> str:
        if not bool(getattr(self.workspace_backend, "claim_resource_fenced", False)):
            # Preserve the historical pinned-lane teardown byte-for-byte in
            # substance. Pinned mounts predate resident identities and must
            # not become unkillable when that optional file is absent.
            return f"""#!/usr/bin/env bash
set +e
rclone rc --rc-addr {shlex.quote(state.rc_addr)} --rc-user {shlex.quote(state.rc_user)} --rc-pass {shlex.quote(state.rc_pass)} core/quit >/dev/null 2>&1
sleep 2
if mountpoint -q {shlex.quote(state.target_path)}; then
  fusermount3 -u {shlex.quote(state.target_path)} 2>/dev/null || fusermount -u {shlex.quote(state.target_path)} 2>/dev/null
fi
if mountpoint -q {shlex.quote(state.target_path)}; then
  fusermount3 -uz {shlex.quote(state.target_path)} 2>/dev/null || fusermount -uz {shlex.quote(state.target_path)} 2>/dev/null
fi
if [ -s {shlex.quote(state.pid_file)} ]; then
  pid="$(cat {shlex.quote(state.pid_file)})"
  if kill -0 "${{pid}}" 2>/dev/null; then
    kill "${{pid}}" 2>/dev/null
    sleep 2
    kill -9 "${{pid}}" 2>/dev/null
  fi
fi
rm -f {shlex.quote(state.token_path)} {shlex.quote(state.token_helper_path)} {shlex.quote(state.token_path + ".new.")}*
echo "{_OK}"
"""
        return f"""#!/usr/bin/env bash
set -u
identity={shlex.quote(state.identity_file)}
target={shlex.quote(state.target_path)}
_srw_mount_present() {{
  if command -v findmnt >/dev/null 2>&1; then
    # -C prevents canonicalization/stat of an ENOTCONN target. Read the kernel
    # mount table and compare the mountpoint string itself.
    findmnt -rn -C -M "$target" >/dev/null 2>&1
  else
    mountpoint -q "$target"
  fi
}}
_srw_any_exact_process() {{
  for _srw_cmdline in /proc/[0-9]*/cmdline; do
    if [ ! -r "$_srw_cmdline" ]; then
      _srw_proc=${{_srw_cmdline%/cmdline}}
      [ ! -e "$_srw_proc" ] && continue
      _srw_uid=$(awk '/^Uid:/{{print $2; exit}}' "$_srw_proc/status" 2>/dev/null) || exit 86
      [ "$_srw_uid" = "$(id -u)" ] && exit 86
      continue
    fi
    _srw_args=$(tr '\\0' '\\n' < "$_srw_cmdline" 2>/dev/null) || {{ [ ! -e "${{_srw_cmdline%/cmdline}}" ] && continue; exit 86; }}
    [ "$(basename "$(printf '%s\n' "$_srw_args" | head -n1)")" = rclone ] || continue
    printf '%s\n' "$_srw_args" | grep -Fqx -- mount || continue
    printf '%s\n' "$_srw_args" | grep -Fqx -- {shlex.quote(state.config_path)} || continue
    printf '%s\n' "$_srw_args" | grep -Fqx -- "$target" || continue
    printf '%s\n' "$_srw_args" | grep -Fqx -- {shlex.quote(state.rc_addr)} || continue
    return 0
  done
  return 1
}}
if [ ! -f "$identity" ]; then
  _srw_mount_present && exit 87
  ! _srw_any_exact_process || exit 85
  rm -f -- {shlex.quote(state.pid_file)} {shlex.quote(state.token_path)} {shlex.quote(state.token_helper_path)}
  echo "{_OK}"
  exit 0
fi
[ "$(wc -l < "$identity")" -eq 1 ] || exit 84
IFS='|' read -r _srw_version _srw_spec _srw_status _srw_generation _srw_pid _srw_pass _srw_extra < "$identity"
[ "$_srw_version" = {_RESIDENT_IDENTITY_VERSION} ] || exit 84
[ "$_srw_spec" = {shlex.quote(state.resident_spec_digest)} ] || exit 84
case "$_srw_status" in active|creating) ;; *) exit 84 ;; esac
[ "$_srw_generation" = {shlex.quote(state.resident_generation)} ] || exit 84
[ "$_srw_pass" = {shlex.quote(state.rc_pass)} ] || exit 84
[ -z "$_srw_extra" ] || exit 84
if [ "$_srw_pid" = 0 ] && [ -s {shlex.quote(state.pid_file)} ]; then
  _srw_pid=$(cat {shlex.quote(state.pid_file)})
fi
printf %s "$_srw_pid" | grep -Eq '^[1-9][0-9]*$' || {{
  _srw_mount_present && exit 85
  ! _srw_any_exact_process || exit 85
  rm -f -- "$identity" {shlex.quote(state.pid_file)}
  echo "{_OK}"
  exit 0
}}
_srw_pid_matches() {{
  [ -r "/proc/$_srw_pid/cmdline" ] || return 1
  _srw_args=$(tr '\\0' '\\n' < "/proc/$_srw_pid/cmdline") || return 1
  _srw_exe=$(printf '%s\\n' "$_srw_args" | head -n1)
  [ "$(basename "$_srw_exe")" = rclone ] || return 1
  for _srw_expected in mount {shlex.quote(state.config_path)} {shlex.quote(state.target_path)} {shlex.quote(state.rc_addr)}; do
    printf '%s\\n' "$_srw_args" | grep -Fqx -- "$_srw_expected" || return 1
  done
}}
if [ -e "/proc/$_srw_pid" ]; then
  # A live PID must still be the exact resident before either process or mount
  # mutation. A dead PID, in contrast, leaves a known stale FUSE/ENOTCONN
  # target that this exact spec+generation identity is allowed to unmount.
  _srw_pid_matches || exit 85
  timeout 10 rclone rc --rc-addr {shlex.quote(state.rc_addr)} --rc-user {shlex.quote(state.rc_user)} --rc-pass {shlex.quote(state.rc_pass)} core/quit >/dev/null 2>&1 || true
  sleep 2
  if _srw_pid_matches; then kill "$_srw_pid" 2>/dev/null || true; fi
  sleep 2
  if _srw_pid_matches; then kill -9 "$_srw_pid" 2>/dev/null || true; fi
  sleep 1
  # PID reuse after the stop is also fail-closed. Never unmount beside a live
  # process whose command line is no longer the attested rclone resident.
  if [ -e "/proc/$_srw_pid" ] && ! _srw_pid_matches; then exit 85; fi
fi
# Do not gate detach on mountpoint(1): on a genuinely stale FUSE target it may
# return "not mounted" because stat(2) itself fails with ENOTCONN. The exact
# dead/stopped resident identity above authorizes an unconditional detach.
timeout 10 fusermount3 -u "$target" 2>/dev/null || timeout 10 fusermount -u "$target" 2>/dev/null || true
if _srw_mount_present || ! [ -d "$target" ]; then
  timeout 10 fusermount3 -uz "$target" 2>/dev/null || timeout 10 fusermount -uz "$target" 2>/dev/null || true
fi
if _srw_mount_present || ! [ -d "$target" ]; then
  # Workspace images normally permit their agent user to manage its FUSE
  # mount. A bounded lazy umount is the final recovery for a dead transport.
  timeout 10 sudo -n umount -l -- "$target" >/dev/null 2>&1 || true
fi
_srw_target_ready=
for _srw_i in $(seq 1 20); do
  if ! _srw_mount_present && mkdir -p -- "$target" 2>/dev/null && [ -d "$target" ]; then
    _srw_target_ready=yes
    break
  fi
  sleep 0.1
done
[ "$_srw_target_ready" = yes ] || exit 88
! _srw_any_exact_process || exit 85
rm -f -- "$identity" {shlex.quote(state.pid_file)} {shlex.quote(state.token_path)} {shlex.quote(state.token_helper_path)}
echo "{_OK}"
"""

    def _terminal_unmount_script(self, state: RcloneMountState, *, drain: bool) -> str:
        """Exact resident adoption, VFS drain, teardown, and zero proof."""

        drain_flag = "yes" if drain else "no"
        return f"""#!/usr/bin/env bash
set -euo pipefail
identity={shlex.quote(state.identity_file)}
target={shlex.quote(state.target_path)}
expected_spec={shlex.quote(state.resident_spec_digest)}
expected_config={shlex.quote(state.config_path)}
expected_rc={shlex.quote(state.rc_addr)}
drain={shlex.quote(drain_flag)}
_srw_mount_present() {{
  if command -v findmnt >/dev/null 2>&1; then
    findmnt -rn -C -M "$target" >/dev/null 2>&1
  else
    mountpoint -q "$target"
  fi
}}
_srw_pid_matches() {{
  [ -r "/proc/$_srw_pid/cmdline" ] || return 1
  _srw_args=$(tr '\\0' '\\n' < "/proc/$_srw_pid/cmdline") || return 1
  [ "$(basename "$(printf '%s\n' "$_srw_args" | head -n1)")" = rclone ] || return 1
  for _srw_expected in mount "$expected_config" "$target" "$expected_rc"; do
    printf '%s\n' "$_srw_args" | grep -Fqx -- "$_srw_expected" || return 1
  done
}}
_srw_any_exact_process() {{
  for _srw_cmdline in /proc/[0-9]*/cmdline; do
    if [ ! -r "$_srw_cmdline" ]; then
      _srw_proc=${{_srw_cmdline%/cmdline}}
      [ ! -e "$_srw_proc" ] && continue
      _srw_uid=$(awk '/^Uid:/{{print $2; exit}}' "$_srw_proc/status" 2>/dev/null) || exit 86
      [ "$_srw_uid" = "$(id -u)" ] && exit 86
      continue
    fi
    _srw_args=$(tr '\\0' '\\n' < "$_srw_cmdline" 2>/dev/null) || {{ [ ! -e "${{_srw_cmdline%/cmdline}}" ] && continue; exit 86; }}
    [ "$(basename "$(printf '%s\n' "$_srw_args" | head -n1)")" = rclone ] || continue
    printf '%s\n' "$_srw_args" | grep -Fqx -- "$expected_config" || continue
    printf '%s\n' "$_srw_args" | grep -Fqx -- "$target" || continue
    printf '%s\n' "$_srw_args" | grep -Fqx -- "$expected_rc" || continue
    return 0
  done
  return 1
}}
if [ ! -f "$identity" ]; then
  ! _srw_mount_present || exit 87
  ! _srw_any_exact_process || exit 85
  echo "{_OK}"
  exit 0
fi
[ "$(wc -l < "$identity")" -eq 1 ] || exit 84
IFS='|' read -r _srw_version _srw_spec _srw_status _srw_generation _srw_pid _srw_pass _srw_extra < "$identity"
[ "$_srw_version" = {_RESIDENT_IDENTITY_VERSION} ] || exit 84
[ "$_srw_spec" = "$expected_spec" ] || exit 84
case "$_srw_status" in active|creating) ;; *) exit 84 ;; esac
printf '%s' "$_srw_generation" | grep -Eq '^[0-9a-f]{{32}}$' || exit 84
printf '%s' "$_srw_pid" | grep -Eq '^[1-9][0-9]*$' || exit 84
[ -n "$_srw_pass" ] && [ -z "$_srw_extra" ] || exit 84
if [ -e "/proc/$_srw_pid" ]; then
  _srw_pid_matches || exit 85
  if [ "$drain" = yes ]; then
    _srw_drained=
    for _srw_i in $(seq 1 60); do
      if _srw_core=$(timeout 5 rclone rc --rc-addr "$expected_rc" --rc-user {shlex.quote(state.rc_user)} --rc-pass "$_srw_pass" core/stats 2>/dev/null) &&
         _srw_vfs=$(timeout 5 rclone rc --rc-addr "$expected_rc" --rc-user {shlex.quote(state.rc_user)} --rc-pass "$_srw_pass" vfs/stats 2>/dev/null) &&
         python3 -c {shlex.quote(_TERMINAL_DRAIN_STATUS_PY)} "$_srw_core" "$_srw_vfs" 2>/dev/null; then
        _srw_drained=yes
        break
      fi
      sleep 0.5
    done
    [ "$_srw_drained" = yes ] || exit 89
  fi
  timeout 10 rclone rc --rc-addr "$expected_rc" --rc-user {shlex.quote(state.rc_user)} --rc-pass "$_srw_pass" core/quit >/dev/null 2>&1 || true
  for _srw_i in $(seq 1 20); do _srw_pid_matches || break; sleep 0.1; done
  if _srw_pid_matches; then kill "$_srw_pid" 2>/dev/null || true; fi
  for _srw_i in $(seq 1 20); do _srw_pid_matches || break; sleep 0.1; done
  if _srw_pid_matches; then kill -9 "$_srw_pid" 2>/dev/null || true; fi
  for _srw_i in $(seq 1 20); do _srw_pid_matches || break; sleep 0.1; done
  ! _srw_pid_matches || exit 85
fi
timeout 10 fusermount3 -u "$target" >/dev/null 2>&1 || timeout 10 fusermount -u "$target" >/dev/null 2>&1 || true
if _srw_mount_present; then
  timeout 10 fusermount3 -uz "$target" >/dev/null 2>&1 || timeout 10 fusermount -uz "$target" >/dev/null 2>&1 || true
fi
if _srw_mount_present; then
  timeout 10 sudo -n umount -l -- "$target" >/dev/null 2>&1 || true
fi
! _srw_mount_present || exit 88
! _srw_any_exact_process || exit 85
rm -f -- "$identity" {shlex.quote(state.pid_file)} {shlex.quote(state.token_path)} {shlex.quote(state.token_helper_path)}
echo "{_OK}"
"""

    def _terminal_zero_script(self) -> str:
        base = self.workspace_backend.resolve_home_path(
            f".cache/srw/rclone/{self.thread_id}"
        )
        expected = [
            self._state_for_mount(dict(mount), index)
            for index, mount in enumerate(self.cloud_cfg.get("mounts") or [])
        ]
        mount_checks = "\n".join(
            (
                "if command -v findmnt >/dev/null 2>&1; then "
                f"findmnt -rn -C -M {shlex.quote(state.target_path)} "
                ">/dev/null 2>&1 && exit 88; "
                f"else mountpoint -q {shlex.quote(state.target_path)} && exit 88; fi"
            )
            for state in expected
        )
        return f"""#!/usr/bin/env bash
set -euo pipefail
base={shlex.quote(base)}
{mount_checks}
if [ -d "$base" ] && find "$base" -name resident.identity -type f -print -quit | grep -q .; then
  exit 84
fi
for _srw_cmdline in /proc/[0-9]*/cmdline; do
  if [ ! -r "$_srw_cmdline" ]; then
    _srw_proc=${{_srw_cmdline%/cmdline}}
    [ ! -e "$_srw_proc" ] && continue
    _srw_uid=$(awk '/^Uid:/{{print $2; exit}}' "$_srw_proc/status" 2>/dev/null) || exit 86
    [ "$_srw_uid" = "$(id -u)" ] && exit 86
    continue
  fi
  _srw_args=$(tr '\\0' '\\n' < "$_srw_cmdline" 2>/dev/null) || {{ [ ! -e "${{_srw_cmdline%/cmdline}}" ] && continue; exit 86; }}
  [ "$(basename "$(printf '%s\n' "$_srw_args" | head -n1)")" = rclone ] || continue
  printf '%s\n' "$_srw_args" | grep -F -- "$base/" >/dev/null && exit 85
done
echo "{_OK}"
"""

    def _collect_cache_usage_sync(self) -> list[RcloneCacheUsage]:
        output = self._run_remote_script(
            "cloud_cache_usage.sh",
            self._cache_usage_script(include_rc=False),
            timeout=30,
        )
        usages: list[RcloneCacheUsage] = []
        for row in self._parse_status_rows(output):
            usages.append(
                RcloneCacheUsage(
                    mount_id=str(row.get("mount_id") or ""),
                    workspace_name=str(row.get("workspace_name") or ""),
                    target_path=str(row.get("target_path") or ""),
                    cache_dir=str(row.get("cache_dir") or ""),
                    cache_bytes=int(row.get("cache_bytes") or 0),
                    hard_limit_bytes=int(row.get("hard_limit_bytes") or 0),
                    mounted=row.get("mounted") == "yes",
                )
            )
        return usages

    def _collect_status_sync(self) -> list[dict[str, Any]]:
        output = self._run_remote_script(
            "cloud_mount_status.sh",
            self._cache_usage_script(include_rc=True),
            timeout=30,
        )
        return self._parse_status_rows(output)

    def _cache_usage_script(self, *, include_rc: bool) -> str:
        lines = [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
        ]
        for state in self._states:
            lines.extend(
                [
                    f"bytes=$(du -sb {shlex.quote(state.cache_dir)} 2>/dev/null | awk '{{print $1}}' || true)",
                    'bytes="${bytes:-0}"',
                    'mounted="no"',
                    f'if mountpoint -q {shlex.quote(state.target_path)}; then mounted="yes"; fi',
                    (
                        "printf '%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n' "
                        f"{shlex.quote(_CACHE_USAGE)} "
                        f"{shlex.quote(state.mount_id)} "
                        f"{shlex.quote(state.workspace_name)} "
                        f"{shlex.quote(state.target_path)} "
                        f"{shlex.quote(state.cache_dir)} "
                        '"${bytes}" '
                        f"{shlex.quote(str(state.hard_cache_limit_bytes))} "
                        f"{shlex.quote(state.hard_cache_limit)} "
                        '"${mounted}"'
                    ),
                ]
            )
            if include_rc:
                lines.extend(
                    [
                        "core_stats=$(rclone rc "
                        f"--rc-addr {shlex.quote(state.rc_addr)} "
                        f"--rc-user {shlex.quote(state.rc_user)} "
                        f"--rc-pass {shlex.quote(state.rc_pass)} "
                        "core/stats 2>/dev/null | tr '\\n' ' ' || true)",
                        (
                            "printf '%s\\t%s\\t%s\\n' "
                            f"{shlex.quote(_RC_CORE)} "
                            f"{shlex.quote(state.mount_id)} "
                            '"${core_stats}"'
                        ),
                        "vfs_stats=$(rclone rc "
                        f"--rc-addr {shlex.quote(state.rc_addr)} "
                        f"--rc-user {shlex.quote(state.rc_user)} "
                        f"--rc-pass {shlex.quote(state.rc_pass)} "
                        "vfs/stats 2>/dev/null | tr '\\n' ' ' || true)",
                        (
                            "printf '%s\\t%s\\t%s\\n' "
                            f"{shlex.quote(_RC_VFS)} "
                            f"{shlex.quote(state.mount_id)} "
                            '"${vfs_stats}"'
                        ),
                    ]
                )
        lines.append(f'echo "{_OK}"')
        return "\n".join(lines)

    def _parse_status_rows(self, output: str) -> list[dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for line in output.splitlines():
            if line.startswith(f"{_CACHE_USAGE}\t"):
                parts = line.split("\t", 8)
                if len(parts) != 9:
                    continue
                (
                    _marker,
                    mount_id,
                    workspace_name,
                    target_path,
                    cache_dir,
                    cache_bytes,
                    hard_limit_bytes,
                    hard_limit,
                    mounted,
                ) = parts
                row = rows.setdefault(mount_id, {"mount_id": mount_id})
                if mount_id not in order:
                    order.append(mount_id)
                row.update(
                    {
                        "workspace_name": workspace_name,
                        "target_path": target_path,
                        "cache_dir": cache_dir,
                        "cache_bytes": int(cache_bytes or 0),
                        "hard_limit_bytes": int(hard_limit_bytes or 0),
                        "hard_limit": hard_limit,
                        "mounted": mounted,
                    }
                )
            elif line.startswith(f"{_RC_CORE}\t"):
                parts = line.split("\t", 2)
                if len(parts) == 3:
                    rows.setdefault(parts[1], {"mount_id": parts[1]})["rc_core"] = (
                        parts[2]
                    )
            elif line.startswith(f"{_RC_VFS}\t"):
                parts = line.split("\t", 2)
                if len(parts) == 3:
                    rows.setdefault(parts[1], {"mount_id": parts[1]})["rc_vfs"] = parts[
                        2
                    ]
        return [rows[mount_id] for mount_id in order]

    def _run_remote_script(
        self,
        name: str,
        script: str,
        *,
        timeout: int = 30,
        require_ok: bool = True,
    ) -> str:
        # Each controller gets a unique inert staging directory.  An old
        # claimant may finish an SFTP upload after handoff, but it cannot
        # overwrite the successor's script and every chmod/execute/remove is
        # admitted atomically by exec_claim_resource below.
        rel_path = (
            f".cache/srw/rclone/{self.thread_id}/scripts/{self._script_nonce}/{name}"
        )
        self.workspace_backend.write_home_file(rel_path, script)
        script_path = self.workspace_backend.resolve_home_path(rel_path)
        command, outer_timeout = self._bounded_remote_script_command(
            script_path,
            timeout=timeout,
        )
        output = self._exec_resource(
            command,
            timeout=outer_timeout,
            operation=f"run rclone script {name}",
        )
        if require_ok and _OK not in output:
            raise RcloneMountError(output.strip() or "rclone mount failed")
        if _FAILED in output and require_ok:
            raise RcloneMountError(output.strip() or "rclone mount failed")
        return output

    def _run_terminal_remote_script(
        self,
        name: str,
        script: str,
        *,
        timeout: int,
    ) -> str:
        """Stage an inert script, then execute it under End's terminal token."""

        rel_path = (
            f".cache/srw/rclone/{self.thread_id}/scripts/{self._script_nonce}/{name}"
        )
        self.workspace_backend.write_home_file(rel_path, script)
        script_path = self.workspace_backend.resolve_home_path(rel_path)
        command, outer_timeout = self._bounded_remote_script_command(
            script_path,
            timeout=timeout,
        )
        execute = getattr(self.workspace_backend, "exec_terminal_claim_resource", None)
        if not callable(execute):
            raise RcloneMountError(
                "terminal rclone cleanup has no terminal resource fence"
            )
        output = execute(
            command,
            timeout=outer_timeout,
            operation=f"terminal rclone cleanup {name}",
        )
        if _OK not in output or _FAILED in output:
            raise RcloneMountError(output.strip() or "terminal rclone cleanup failed")
        return output

    @staticmethod
    def _bounded_remote_script_command(
        script_path: str,
        *,
        timeout: int,
    ) -> tuple[str, int]:
        """Build a remote kill boundary that expires before SSH does.

        Paramiko's channel timeout only closes the client-side channel; it
        does not prove that the remote shell (or a child holding the durable
        claim-resource flock) stopped. GNU ``timeout`` owns a remote process
        group, sends TERM and then KILL. The extra 40 seconds cover
        RemoteBackend's bounded 30-second flock acquisition plus cleanup and
        transport margin.
        """

        script_timeout = max(1, int(timeout))
        outer_timeout = script_timeout + 40
        command = (
            f"chmod 700 {shlex.quote(script_path)} && "
            "timeout --signal=TERM --kill-after=5s "
            f"{script_timeout}s bash {shlex.quote(script_path)}; "
            f"rc=$?; rm -f {shlex.quote(script_path)}; exit $rc"
        )
        return command, outer_timeout

    def _exec_resource(
        self,
        command: str,
        *,
        timeout: int,
        operation: str,
    ) -> str:
        execute = getattr(self.workspace_backend, "exec_claim_resource", None)
        if execute is None:
            # Compatibility for narrowly mocked/custom backends. Production
            # WorkspaceBackend implements the primitive; pinned semantics are
            # its ordinary exec_command path.
            return self.workspace_backend.exec_command(command, timeout=timeout)
        return execute(command, timeout=timeout, operation=operation)

    @staticmethod
    def _remote_path(remote_name: str, root: str) -> str:
        root = root.strip("/")
        return f"{remote_name}:{root}" if root else f"{remote_name}:"

    @staticmethod
    def _remote_child_path(remote_name: str, root: str, child: str) -> str:
        root = root.strip("/")
        child = child.lstrip("/")
        return f"{remote_name}:{root}/{child}" if root else f"{remote_name}:{child}"

    @staticmethod
    def _bash_array(name: str, args: list[str]) -> str:
        return f"{name}=(" + " ".join(shlex.quote(arg) for arg in args) + ")"

    def _token_setup_block(self, state: RcloneMountState, token: str) -> str:
        """Seed the workspace-side bearer token file + read-only helper.

        The helper only ever ``cat``s the token file; minting/refreshing
        stays in the agent process. Created before ``rclone config create``
        because the first remote call (the ``.cloudignore`` fetch) already
        needs the bearer.
        """
        helper_script = (
            "#!/usr/bin/env bash\n" + f"exec cat {shlex.quote(state.token_path)}\n"
        )
        return "\n".join(
            [
                self._write_text_file_block(state.token_path, token),
                f"chmod 600 {shlex.quote(state.token_path)}",
                self._write_text_file_block(state.token_helper_path, helper_script),
                f"chmod 700 {shlex.quote(state.token_helper_path)}",
            ]
        )

    @staticmethod
    def _version_check_block(min_version: str) -> str:
        """Fail fast with a clear message on too-old workspace rclone.

        A mount failure here does not renegotiate the orchestrator's
        fallback decision, so silent vendor misbehavior on an old rclone
        would be confusing — better to name the version gap outright.
        """
        return "\n".join(
            [
                f"required_ver={shlex.quote(min_version)}",
                'have_ver="$(rclone version 2>/dev/null'
                " | sed -nE '1s/^rclone v([0-9]+(\\.[0-9]+)*).*/\\1/p')\"",
                'if [ -z "${have_ver}" ]; then',
                '  echo "cannot determine rclone version on the workspace runtime" >&2',
                "  exit 1",
                "fi",
                'if [ "$(printf \'%s\\n%s\\n\' "${required_ver}" "${have_ver}"'
                ' | sort -V | head -n1)" != "${required_ver}" ]; then',
                '  echo "rclone ${have_ver} on the workspace runtime is older than'
                " ${required_ver} required by this cloud mount -"
                ' update the workspace image" >&2',
                "  exit 1",
                "fi",
            ]
        )

    @staticmethod
    def _write_text_file_block(path: str, content: str) -> str:
        if not content:
            return f": > {shlex.quote(path)}"
        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()[:12]
        delimiter = f"SRW_CLOUDIGNORE_{digest}"
        return (
            f"cat > {shlex.quote(path)} <<'{delimiter}'\n"
            f"{content.rstrip()}\n"
            f"{delimiter}"
        )


def _parse_size_to_bytes(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    text = str(value).strip()
    if not text:
        return None
    match = re.fullmatch(r"(?i)\s*(\d+(?:\.\d+)?)\s*([kmgt]?b?)?\s*", text)
    if not match:
        return None
    number = float(match.group(1))
    unit = (match.group(2) or "").upper()
    if unit not in _SIZE_UNITS:
        return None
    return int(number * _SIZE_UNITS[unit])


def _format_bytes(value: int) -> str:
    size = float(max(value, 0))
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            if unit == "B":
                return f"{int(size)} B"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{value} B"


def _compact_rc(value: str, max_chars: int = 240) -> str:
    compact = " ".join(value.split())
    if len(compact) <= max_chars:
        return compact
    return compact[: max_chars - 3] + "..."


def _coerce_ignore_lines(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        raw: list[str] = []
        for chunk in value.replace(",", "\n").splitlines():
            raw.append(chunk)
    elif isinstance(value, (list, tuple, set)):
        raw = [str(item) for item in value]
    else:
        raw = [str(value)]
    return [line.strip() for line in raw if line and line.strip()]


__all__ = [
    "RcloneCacheUsage",
    "RcloneMountError",
    "RcloneMountCleanFailure",
    "RcloneMountManager",
    "RcloneMountState",
]
