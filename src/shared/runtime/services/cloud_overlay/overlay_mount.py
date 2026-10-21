"""fuse-overlayfs capture overlay over a read-only rclone lower.

Sibling to ``shared.runtime.services.cloud_mount.RcloneMountManager``: pure Python that
generates a bash script and runs it on the WORKSPACE pod/VM over the workspace
backend's SSH channel (never the agent pod). The overlay stacks a local
scratch upperdir (the staged diff) over the read-only rclone mount so every
agent write — shell included — is captured locally and the cloud is untouched
until an operator applies the diff (design §3.1/§3.2).

Layout is fixed by the snapshot placement rule (design §11.3): upperdir/workdir
INSIDE /home/agent-host (captured by snapshots), merged mountpoint + raw rclone
lower OUTSIDE it (not captured). The agent sees the merged view at
``workspace/cloud`` via a symlink this manager owns.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
import shlex
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_OVERLAY_OK = "__SRW_OVERLAY_OK__"
_OVERLAY_FAILED = "__SRW_OVERLAY_FAILED__"
_OVERLAY_DEAD = "__SRW_OVERLAY_DEAD__"
_OVERLAY_ABSENT = "__SRW_OVERLAY_ABSENT__"
_OVERLAY_ADOPTED = "__SRW_OVERLAY_ADOPTED__"
_OVERLAY_MISMATCH = "__SRW_OVERLAY_MISMATCH__"


class OverlayMountError(RuntimeError):
    """The capture overlay could not be mounted/refreshed/healed."""


class OverlayMountManager:
    def __init__(
        self,
        *,
        thread_id: str,
        overlay_cfg: dict[str, Any],
        workspace_backend: Any,
        workspace_root: Path | str,
    ) -> None:
        self.thread_id = thread_id
        self.cfg = dict(overlay_cfg)
        self.workspace_backend = workspace_backend
        self.workspace_root = str(
            getattr(workspace_backend, "root", None) or workspace_root
        )
        # Uploading a script is inert: only ``exec_claim_resource`` may chmod
        # and run it.  Give every controller a private staging directory so a
        # retired claim cannot overwrite its successor's pending script before
        # the workspace-side lease fence rejects execution.
        self._controller_id = secrets.token_hex(8)
        self._active = False
        # Set only when this controller crossed the ABSENT -> cold-mount
        # boundary (or the historical pinned replacement boundary).  An adopt
        # mismatch is explicitly *not* ownership of the resident overlay and
        # must never authorize remote teardown during attach rollback.
        self._cold_mount_attempted = False

    @property
    def active(self) -> bool:
        return self._active

    @property
    def lower(self) -> str:
        return str(self.cfg["lower"])

    @property
    def merged(self) -> str:
        return str(self.cfg["merged"])

    @property
    def upper(self) -> str:
        return str(self.cfg["upper"])

    @property
    def work(self) -> str:
        return str(self.cfg["work"])

    @property
    def _identity_rel(self) -> str:
        return f".cache/srw/overlay/{self.thread_id}/resident.identity"

    @property
    def _identity_path(self) -> str:
        return self.workspace_backend.resolve_home_path(self._identity_rel)

    @property
    def _identity_digest(self) -> str:
        payload = "\0".join(
            (
                "srw-overlay-v1",
                self.thread_id,
                self.lower,
                self.upper,
                self.work,
                self.merged,
                self.workspace_root,
            )
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _identity_value(self, phase: str) -> str:
        if phase not in {"creating", "active"}:
            raise ValueError(f"invalid overlay identity phase: {phase}")
        return f"srw-overlay-v1 {phase} {self._identity_digest}"

    def mount(self, remount_lower: Callable[[], None] | None = None) -> str:
        """Cold-mount, adopt, or heal the workspace-resident overlay.

        A successor must not blindly unmount a healthy overlay left by its
        predecessor.  It adopts that mount in place.  A mounted-but-dead view
        is healed exactly once through the caller's exact-lower callback;
        ``heal`` preserves ``upper`` and gives fuse-overlayfs a fresh workdir.

        The claim marker and lock are in the workload user's writable home.
        They enforce ordering between honest agent owners, but remain
        cooperative against arbitrary code running as that same user.
        """
        started = time.perf_counter()
        disposition = "unknown"
        try:
            if (
                getattr(self.workspace_backend, "claim_resource_fenced", False)
                is not True
            ):
                # Pinned/custom backends keep the historical idempotent
                # remount behaviour.  Adoption is a stateless handoff
                # semantic, not a pinned-lane behaviour change.
                self._cold_mount_attempted = True
                self._run(
                    "overlay_mount.sh",
                    self._mount_script(replace_existing=True),
                    timeout=60,
                )
                disposition = "cold"
                self._active = True
                return disposition
            probe = self._run(
                "overlay_adopt_probe.sh",
                self._adopt_probe_script(),
                timeout=30,
                require_ok=False,
            )
            if _OVERLAY_MISMATCH in probe:
                raise OverlayMountError(
                    "existing capture overlay identity does not match this "
                    "claim's exact lower/upper/work/merged configuration"
                )
            if _OVERLAY_ADOPTED in probe:
                disposition = "adopted"
            elif _OVERLAY_DEAD in probe:
                if remount_lower is None:
                    raise OverlayMountError(
                        "existing capture overlay is unhealthy; an exact-lower "
                        "restart callback is required"
                    )
                self.heal(remount_lower)
                disposition = "healed"
            elif _OVERLAY_ABSENT in probe:
                self._cold_mount_attempted = True
                self._run("overlay_mount.sh", self._mount_script(), timeout=60)
                disposition = "cold"
            else:
                raise OverlayMountError(
                    f"overlay adopt probe returned no recognized disposition:\n{probe}"
                )
            self._active = True
            return disposition
        finally:
            logger.info(
                "cloud overlay claim attach timing: thread=%s disposition=%s "
                "total=%.3fs",
                self.thread_id,
                disposition,
                time.perf_counter() - started,
            )

    def rollback_failed_mount(self) -> bool:
        """Retire only a cold overlay this controller actually attempted.

        A stateless adopt mismatch/unrecognized probe describes somebody
        else's resident resource.  In that case rollback is local-only.  A
        cold mount can fail after FUSE and identity publication but before the
        workspace symlink, so the remote rollback script is also fenced by the
        exact creating/active identity before it unmounts anything.

        Returns whether a remote rollback was attempted.
        """

        if not self._cold_mount_attempted:
            self.detach_local()
            return False
        try:
            self._run(
                "overlay_failed_mount_rollback.sh",
                self._failed_mount_rollback_script(),
                timeout=45,
                require_ok=False,
            )
            return True
        finally:
            self._active = False

    def detach_local(self) -> None:
        """Forget this controller without touching workspace-side mounts.

        Claim handoff cancels the local watchdog and calls this method.  The
        lower, overlay, upperdir and workdir stay resident for the successor.
        Genuine terminal cleanup continues to call :meth:`unmount`.
        """
        self._active = False
        logger.info(
            "capture overlay controller detached locally: thread=%s merged=%s",
            self.thread_id,
            self.merged,
        )

    def unmount(self, *, strict: bool = False) -> None:
        """Unmount the resident overlay.

        Pinned teardown retains its historical best-effort behavior. A
        stateless physical claimant uses ``strict=True`` because End may treat
        return as permission to snapshot emptyDir bytes; the script must prove
        that the FUSE mount is actually gone.
        """

        try:
            self._run(
                "overlay_unmount.sh",
                self._unmount_script(strict=strict),
                timeout=45,
                require_ok=strict,
            )
        finally:
            self._active = False

    async def retire_existing(self) -> dict[str, int]:
        """Retire the exact resident overlay without mounting an absent one."""

        execute = getattr(self.workspace_backend, "exec_terminal_claim_resource", None)
        if not callable(execute):
            raise OverlayMountError(
                "terminal overlay cleanup requires a terminal-fenced backend"
            )
        loop = __import__("asyncio").get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: self._run_terminal(
                "terminal_overlay_unmount.sh",
                self._terminal_unmount_script(),
                timeout=60,
            ),
        )
        self._active = False
        return {"overlay_mounts": 0, "overlay_processes": 0}

    def refresh(self, refresh_lower: Callable[[], None]) -> None:
        """Refresh the frozen lower without losing the upperdir (design §11.2).

        CALLER MUST QUIESCE FIRST: held FDs across an unmount get silent stale
        reads. Sequence: PLAIN-unmount the overlay (``fusermount3 -u`` EBUSYs
        while an FD is held, so an un-quiesced agent surfaces as an
        OverlayMountError instead of silent staleness — never ``-uz`` here) →
        refresh the rclone lower (callback) → remount the overlay. The upperdir
        is untouched throughout; the workspace/cloud symlink already points at
        the merged path from the initial mount, so no symlink work is needed.
        """
        self._run(
            "overlay_pre_refresh_unmount.sh", self._plain_unmount_script(), timeout=60
        )
        refresh_lower()
        self._run("overlay_remount.sh", self._mount_body_only_script(), timeout=120)
        self._active = True

    def health_check(self) -> bool:
        """True when the merged view is readable; False on ENOTCONN (dead lower).

        A readdir over the merged view is the only reliable liveness signal —
        /proc/mounts and ``mountpoint -q`` both report "mounted" over a dead
        rclone endpoint (design §11.2)."""
        out = self._run(
            "overlay_probe.sh", self._probe_script(), timeout=30, require_ok=False
        )
        if _OVERLAY_DEAD in out:
            return False
        return _OVERLAY_OK in out

    def upperdir_usage_bytes(self) -> int:
        """Best-effort ``du -sb`` probe of the upperdir (design §7/§9.9).

        Runs with ``require_ok=False``: a probe failure (e.g. du missing, path
        gone) must never raise and block the caller — it degrades to 0 so
        ``over_quota``/``quota_guard_message`` fail open rather than wedging
        the shell preflight on an unrelated probe error.
        """
        out = self._run(
            "overlay_usage.sh", self._usage_script(), timeout=30, require_ok=False
        )
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("__SRW_"):
                continue
            head = line.split()[0]
            if head.isdigit():
                return int(head)
        return 0

    def over_quota(self) -> bool:
        quota = int(self.cfg.get("quota_bytes") or 0)
        return bool(quota) and self.upperdir_usage_bytes() >= quota

    def quota_guard_message(self) -> str | None:
        """Mirrors ``RcloneMountManager.cache_limit_message``: block message or None."""
        quota = int(self.cfg.get("quota_bytes") or 0)
        if not quota:
            return None
        used = self.upperdir_usage_bytes()
        if used < quota:
            return None
        return (
            "Cloud staging guard: this write was blocked because the protected-"
            "mode staging area (upperdir) has reached its cap.\n"
            f"Used {used} bytes of {quota} allowed.\n\n"
            "Staged changes are held locally until you review/apply them. Apply "
            "or reject the pending cloud diff to free space, or ask the operator "
            "to raise the cap."
        )

    def heal(self, remount_lower: Callable[[], None]) -> None:
        """Recover a dead rclone lower under a live overlay (design §11.2).

        Lazy-unmount the overlay FIRST (safe here: the dead lower makes every
        held-FD read fail loudly with ENOTCONN — no silent-staleness window),
        remount the lower via the callback, then remount the overlay."""
        self._run(
            "overlay_heal_unmount.sh",
            self._heal_unmount_script(),
            timeout=60,
            require_ok=False,
        )
        remount_lower()
        self._run("overlay_remount.sh", self._mount_body_only_script(), timeout=120)
        self._active = True

    def reset_upper(self, refresh_lower: Callable[[], None]) -> None:
        """Discard the staged upperdir after an apply/reject and remount with a
        FRESH workdir (a workdir must never be reused across overlay instances
        — design §11.2, fresh-workdir-per-epoch).

        Unmount (plain first, lazy fallback — the overlay is being torn down
        either way, so unlike ``refresh``'s FD-hold quiesce guard there is no
        reason to insist on a clean plain unmount here) → wipe AND recreate
        both upperdir and workdir → refresh the lower (callback) → remount.
        Never touches the merged mountpoint or the lower directory itself."""
        self._run(
            "overlay_reset_unmount.sh",
            self._reset_unmount_script(),
            timeout=60,
            require_ok=False,
        )
        self._run("overlay_wipe_upper.sh", self._wipe_upper_script(), timeout=60)
        refresh_lower()
        self._run("overlay_remount.sh", self._mount_body_only_script(), timeout=120)
        self._active = True

    # --------------------------------------------------------------- scripts

    def _adopt_probe_script(self) -> str:
        """Classify an existing overlay without ever unmounting it.

        Healthy adoption only reasserts the manager-owned workspace symlink.
        Missing and dead dispositions leave mount recovery to the caller.
        """
        lower = shlex.quote(self.lower)
        merged = shlex.quote(self.merged)
        workspace = shlex.quote(self.workspace_root)
        identity = shlex.quote(self._identity_path)
        active_identity = shlex.quote(self._identity_value("active"))
        creating_identity = shlex.quote(self._identity_value("creating"))
        publish_active = self._publish_identity_block("active")
        return f"""#!/usr/bin/env bash
set -euo pipefail
umask 077
trap 'rc=$?; echo "{_OVERLAY_FAILED} rc=${{rc}}"; exit "${{rc}}"' ERR

identity_value="$(cat {identity} 2>/dev/null || true)"
# A matching creating marker closes the crash gap between fuse mount success
# and active publication.  Any other layout is not ours to heal or replace.
# The marker is user-writable: cooperative correctness, not a security wall.
if [ -n "${{identity_value}}" ] && [ "${{identity_value}}" != {active_identity} ] && [ "${{identity_value}}" != {creating_identity} ]; then
  echo "{_OVERLAY_MISMATCH}"
  exit 0
fi
if ! mountpoint -q {merged}; then
  echo "{_OVERLAY_ABSENT}"
  exit 0
fi
if [ -z "${{identity_value}}" ]; then
  echo "{_OVERLAY_MISMATCH} missing-identity"
  exit 0
fi
if ! mountpoint -q {lower}; then
  echo "{_OVERLAY_DEAD} lower-not-mounted"
  exit 0
fi

probe_err="$(mktemp)"
trap 'rm -f "${{probe_err}}"' EXIT
if ! ls {merged} >/dev/null 2>"${{probe_err}}"; then
  cat "${{probe_err}}" >&2 || true
  echo "{_OVERLAY_DEAD} unreadable"
  exit 0
fi

if [ "${{identity_value}}" = {creating_identity} ]; then
  {publish_active}
fi

# Reassert only the manager-owned link; never unmount or touch upper/work.
workspace={workspace}
mkdir -p "${{workspace}}/.srw"
entry="${{workspace}}/cloud"
if [ -L "${{entry}}" ]; then rm "${{entry}}"; fi
if [ -e "${{entry}}" ] && [ ! -L "${{entry}}" ]; then
  mv "${{entry}}" "${{workspace}}/.srw/cloud.pre-overlay.$(date +%s)"
fi
ln -sfn {merged} "${{entry}}"
echo "{_OVERLAY_ADOPTED}"
echo "{_OVERLAY_OK}"
"""

    def _publish_identity_block(self, phase: str) -> str:
        identity = self._identity_path
        identity_parent = str(Path(identity).parent)
        return "\n".join(
            (
                f"mkdir -p {shlex.quote(identity_parent)}",
                f"identity_tmp={shlex.quote(identity + '.new')}.$$",
                f"printf '%s\\n' {shlex.quote(self._identity_value(phase))} "
                ' > "${identity_tmp}"',
                'chmod 600 "${identity_tmp}"',
                f'mv -f -- "${{identity_tmp}}" {shlex.quote(identity)}',
            )
        )

    def _mount_script(self, *, replace_existing: bool = False) -> str:
        lower = shlex.quote(self.lower)
        upper = shlex.quote(self.upper)
        work = shlex.quote(self.work)
        merged = shlex.quote(self.merged)
        workspace = shlex.quote(self.workspace_root)
        # Quote the WHOLE -o value: raw interpolation would word-split on paths
        # with spaces/metacharacters (B2 review finding).
        opts = shlex.quote(
            f"lowerdir={self.lower},upperdir={self.upper},workdir={self.work}"
        )
        if replace_existing:
            existing_mount_block = f"""# Historical pinned-lane idempotent remount.
if mountpoint -q {merged}; then
  fusermount3 -u {merged} 2>/dev/null || fusermount -u {merged} 2>/dev/null || true
fi
if mountpoint -q {merged}; then
  fusermount3 -uz {merged} 2>/dev/null || fusermount -uz {merged} 2>/dev/null || true
fi"""
            prepare_mount_dirs = (
                f"mkdir -p {upper} {work} {merged}\n\n{existing_mount_block}"
            )
        else:
            existing_mount_block = f"""# Existing mounts are handled by the adopt probe.  Never tear one down from
# the cold path: a retired controller must not destroy its successor's view.
identity_value="$(cat {shlex.quote(self._identity_path)} 2>/dev/null || true)"
if [ -n "${{identity_value}}" ] && [ "${{identity_value}}" != {shlex.quote(self._identity_value("active"))} ] && [ "${{identity_value}}" != {shlex.quote(self._identity_value("creating"))} ]; then
  echo "{_OVERLAY_FAILED} rc=5 (overlay identity changed during cold mount)"
  exit 5
fi
if mountpoint -q {merged}; then
  echo "{_OVERLAY_FAILED} rc=4 (overlay appeared during cold mount)"
  exit 4
fi"""
            # ABSENT also covers a predecessor that died after heal/refresh
            # unmounted the overlay. fuse-overlayfs workdirs are single-mount
            # scratch and cannot be reused, but upper contains the staged user
            # diff. Check for a concurrently appeared successor first, then
            # recreate only work immediately before this claim's cold mount.
            prepare_mount_dirs = f"""mkdir -p {upper} {merged}

{existing_mount_block}

rm -rf -- {work}
mkdir -p -- {work}"""
        publish_creating = self._publish_identity_block("creating")
        publish_active = self._publish_identity_block("active")
        return f"""#!/usr/bin/env bash
set -euo pipefail
umask 077
trap 'rc=$?; echo "{_OVERLAY_FAILED} rc=${{rc}}"; exit "${{rc}}"' ERR

# The RO rclone lower MUST already be mounted; refuse otherwise (fail-closed).
if ! mountpoint -q {lower}; then
  echo "{_OVERLAY_FAILED} rc=2 (lower {self.lower} not mounted)"
  exit 2
fi

{prepare_mount_dirs}

{publish_creating}
fuse-overlayfs -o {opts} {merged}

if ! mountpoint -q {merged}; then
  echo "{_OVERLAY_FAILED} rc=3 (overlay did not mount)"
  exit 3
fi
{publish_active}

# Point the agent's workspace/cloud at the MERGED view (not the raw lower).
workspace={workspace}
mkdir -p "${{workspace}}/.srw"
entry="${{workspace}}/cloud"
if [ -L "${{entry}}" ]; then rm "${{entry}}"; fi
if [ -e "${{entry}}" ] && [ ! -L "${{entry}}" ]; then
  mv "${{entry}}" "${{workspace}}/.srw/cloud.pre-overlay.$(date +%s)"
fi
ln -sfn {merged} "${{entry}}"

echo "{_OVERLAY_OK}"
"""

    def _unmount_script(self, *, strict: bool = False) -> str:
        merged = shlex.quote(self.merged)
        identity = shlex.quote(self._identity_path)
        terminal_check = (
            f'''if mountpoint -q {merged}; then
  echo "{_OVERLAY_FAILED} rc=6 (overlay remained mounted)"
  exit 6
fi'''
            if strict
            else ""
        )
        return f"""#!/usr/bin/env bash
set +e
if mountpoint -q {merged}; then
  fusermount3 -u {merged} 2>/dev/null || fusermount -u {merged} 2>/dev/null
fi
if mountpoint -q {merged}; then
  fusermount3 -uz {merged} 2>/dev/null || fusermount -uz {merged} 2>/dev/null
fi
if ! mountpoint -q {merged}; then
  rm -f -- {identity}
fi
{terminal_check}
echo "{_OVERLAY_OK}"
"""

    def _failed_mount_rollback_script(self) -> str:
        """Unmount only this config's partially published cold overlay."""

        merged = shlex.quote(self.merged)
        identity = shlex.quote(self._identity_path)
        creating = shlex.quote(self._identity_value("creating"))
        active = shlex.quote(self._identity_value("active"))
        return f"""#!/usr/bin/env bash
set +e
identity_value="$(cat {identity} 2>/dev/null || true)"
if [ "${{identity_value}}" != {creating} ] && [ "${{identity_value}}" != {active} ]; then
  echo "{_OVERLAY_MISMATCH} rollback-refused"
  exit 0
fi
if mountpoint -q {merged}; then
  fusermount3 -u {merged} 2>/dev/null || fusermount -u {merged} 2>/dev/null
fi
if mountpoint -q {merged}; then
  fusermount3 -uz {merged} 2>/dev/null || fusermount -uz {merged} 2>/dev/null
fi
if ! mountpoint -q {merged}; then
  rm -f -- {identity}
fi
echo "{_OVERLAY_OK}"
"""

    def _terminal_unmount_script(self) -> str:
        """Validate exact identity, retire FUSE, and prove mount/process zero."""

        identity = shlex.quote(self._identity_path)
        active_identity = shlex.quote(self._identity_value("active"))
        creating_identity = shlex.quote(self._identity_value("creating"))
        lower = shlex.quote(self.lower)
        upper = shlex.quote(self.upper)
        work = shlex.quote(self.work)
        merged = shlex.quote(self.merged)
        return f"""#!/usr/bin/env bash
set -euo pipefail
identity={identity}
lower={lower}
upper={upper}
work={work}
merged={merged}
_srw_mount_present() {{
  if command -v findmnt >/dev/null 2>&1; then
    findmnt -rn -C -M "$merged" >/dev/null 2>&1
  else
    mountpoint -q "$merged"
  fi
}}
_srw_exact_overlay_pids() {{
  for _srw_cmdline in /proc/[0-9]*/cmdline; do
    if [ ! -r "$_srw_cmdline" ]; then
      _srw_proc=${{_srw_cmdline%/cmdline}}
      [ ! -e "$_srw_proc" ] && continue
      _srw_uid=$(awk '/^Uid:/{{print $2; exit}}' "$_srw_proc/status" 2>/dev/null) || exit 86
      [ "$_srw_uid" = "$(id -u)" ] && exit 86
      continue
    fi
    _srw_args=$(tr '\\0' '\\n' < "$_srw_cmdline" 2>/dev/null) || {{ [ ! -e "${{_srw_cmdline%/cmdline}}" ] && continue; exit 86; }}
    printf '%s\n' "$_srw_args" | grep -Fqx -- fuse-overlayfs || continue
    printf '%s\n' "$_srw_args" | grep -F -- "lowerdir=$lower" >/dev/null || continue
    printf '%s\n' "$_srw_args" | grep -F -- "upperdir=$upper" >/dev/null || continue
    printf '%s\n' "$_srw_args" | grep -F -- "workdir=$work" >/dev/null || continue
    printf '%s\n' "$_srw_args" | grep -Fqx -- "$merged" || continue
    basename "$(dirname "$_srw_cmdline")"
  done
}}
_srw_any_merged_overlay() {{
  for _srw_cmdline in /proc/[0-9]*/cmdline; do
    if [ ! -r "$_srw_cmdline" ]; then
      _srw_proc=${{_srw_cmdline%/cmdline}}
      [ ! -e "$_srw_proc" ] && continue
      _srw_uid=$(awk '/^Uid:/{{print $2; exit}}' "$_srw_proc/status" 2>/dev/null) || exit 86
      [ "$_srw_uid" = "$(id -u)" ] && exit 86
      continue
    fi
    _srw_args=$(tr '\\0' '\\n' < "$_srw_cmdline" 2>/dev/null) || {{ [ ! -e "${{_srw_cmdline%/cmdline}}" ] && continue; exit 86; }}
    printf '%s\n' "$_srw_args" | grep -Fqx -- fuse-overlayfs || continue
    printf '%s\n' "$_srw_args" | grep -Fqx -- "$merged" && return 0
  done
  return 1
}}
if [ ! -f "$identity" ]; then
  ! _srw_mount_present || exit 87
  ! _srw_any_merged_overlay || exit 85
  echo "{_OVERLAY_OK}"
  exit 0
fi
[ "$(wc -l < "$identity")" -eq 1 ] || exit 84
identity_value=$(cat "$identity")
[ "$identity_value" = {active_identity} ] || [ "$identity_value" = {creating_identity} ] || exit 84
if _srw_any_merged_overlay && [ -z "$(_srw_exact_overlay_pids)" ]; then
  exit 85
fi
for _srw_pid in $(_srw_exact_overlay_pids); do kill "$_srw_pid" 2>/dev/null || true; done
for _srw_i in $(seq 1 20); do [ -z "$(_srw_exact_overlay_pids)" ] && break; sleep 0.1; done
for _srw_pid in $(_srw_exact_overlay_pids); do kill -9 "$_srw_pid" 2>/dev/null || true; done
for _srw_i in $(seq 1 20); do [ -z "$(_srw_exact_overlay_pids)" ] && break; sleep 0.1; done
[ -z "$(_srw_exact_overlay_pids)" ] || exit 85
timeout 10 fusermount3 -u "$merged" >/dev/null 2>&1 || timeout 10 fusermount -u "$merged" >/dev/null 2>&1 || true
if _srw_mount_present; then
  timeout 10 fusermount3 -uz "$merged" >/dev/null 2>&1 || timeout 10 fusermount -uz "$merged" >/dev/null 2>&1 || true
fi
if _srw_mount_present; then
  timeout 10 umount -l -- "$merged" >/dev/null 2>&1 || timeout 10 sudo -n umount -l -- "$merged" >/dev/null 2>&1 || true
fi
! _srw_mount_present || exit 88
! _srw_any_merged_overlay || exit 85
rm -f -- "$identity"
echo "{_OVERLAY_OK}"
"""

    def _terminal_zero_script(self) -> str:
        identity = shlex.quote(self._identity_path)
        merged = shlex.quote(self.merged)
        return f"""#!/usr/bin/env bash
set -euo pipefail
[ ! -e {identity} ] || exit 84
if command -v findmnt >/dev/null 2>&1; then
  ! findmnt -rn -C -M {merged} >/dev/null 2>&1 || exit 88
else
  ! mountpoint -q {merged} || exit 88
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
  printf '%s\n' "$_srw_args" | grep -Fqx -- fuse-overlayfs || continue
  printf '%s\n' "$_srw_args" | grep -Fqx -- {merged} && exit 85
done
echo "{_OVERLAY_OK}"
"""

    def _plain_unmount_script(self) -> str:
        merged = shlex.quote(self.merged)
        return f"""#!/usr/bin/env bash
set -euo pipefail
trap 'rc=$?; echo "{_OVERLAY_FAILED} rc=${{rc}}"; exit "${{rc}}"' ERR
# PLAIN unmount — EBUSYs while an FD is held (the quiesce guard, design §11.2).
fusermount3 -u {merged} || fusermount -u {merged}
echo "{_OVERLAY_OK}"
"""

    def _mount_body_only_script(self) -> str:
        """Remount the overlay over the (refreshed) lower; no symlink work —
        the symlink already points at the merged path from the initial mount.

        Freshens the WORKDIR (wipe + recreate) immediately before the
        fuse-overlayfs mount line — a fuse-overlayfs workdir must never be
        reused across mount instances (design §11.2/§11.6 #2). Deliberately
        NEVER touches ``upper``: staged data must survive a heal/refresh
        remount. ``reset_upper`` already wipes both upper and work via
        ``_wipe_upper_script`` before calling this method for its own
        remount — re-freshening work here is redundant-but-harmless for that
        caller and is exactly what's needed for the other caller, ``heal``,
        which never wipes work otherwise."""
        upper = shlex.quote(self.upper)
        work = shlex.quote(self.work)
        merged = shlex.quote(self.merged)
        lower = shlex.quote(self.lower)
        opts = shlex.quote(
            f"lowerdir={self.lower},upperdir={self.upper},workdir={self.work}"
        )
        identity = shlex.quote(self._identity_path)
        active_identity = shlex.quote(self._identity_value("active"))
        creating_identity = shlex.quote(self._identity_value("creating"))
        publish_creating = self._publish_identity_block("creating")
        publish_active = self._publish_identity_block("active")
        return f"""#!/usr/bin/env bash
set -euo pipefail
trap 'rc=$?; echo "{_OVERLAY_FAILED} rc=${{rc}}"; exit "${{rc}}"' ERR
identity_value="$(cat {identity} 2>/dev/null || true)"
if [ "${{identity_value}}" != {active_identity} ] && [ "${{identity_value}}" != {creating_identity} ]; then
  echo "{_OVERLAY_FAILED} rc=5 (overlay identity changed before remount)"
  exit 5
fi
mkdir -p {upper} {merged}
if ! mountpoint -q {lower}; then echo "{_OVERLAY_FAILED} rc=2 (lower not mounted)"; exit 2; fi
{publish_creating}
rm -rf {work} && mkdir -p {work}
fuse-overlayfs -o {opts} {merged}
mountpoint -q {merged}
{publish_active}
echo "{_OVERLAY_OK}"
"""

    def _probe_script(self) -> str:
        merged = shlex.quote(self.merged)
        return f"""#!/usr/bin/env bash
set +e
# A readdir returning ENOTCONN means the rclone lower died under us.
probe_out="$(mktemp)"
probe_err="$(mktemp)"
trap 'rm -f "${{probe_out}}" "${{probe_err}}"' EXIT
ls {merged} >"${{probe_out}}" 2>"${{probe_err}}"
rc=$?
if grep -qi 'not connected\\|ENOTCONN\\|Transport endpoint' "${{probe_err}}" 2>/dev/null; then
  echo "{_OVERLAY_DEAD} ENOTCONN"
  exit 0
fi
if [ "$rc" -ne 0 ]; then echo "{_OVERLAY_DEAD} rc=$rc"; exit 0; fi
echo "{_OVERLAY_OK}"
"""

    def _heal_unmount_script(self) -> str:
        merged = shlex.quote(self.merged)
        return f"""#!/usr/bin/env bash
set +e
# LAZY unmount is correct on heal (dead lower => held reads ENOTCONN loudly).
fusermount3 -uz {merged} 2>/dev/null || fusermount -uz {merged} 2>/dev/null
if mountpoint -q {merged}; then
  echo "{_OVERLAY_FAILED} rc=6 (dead overlay remained mounted)"
  exit 6
fi
echo "{_OVERLAY_OK}"
"""

    def _reset_unmount_script(self) -> str:
        """PLAIN unmount first; LAZY as fallback. No FD-hold quiesce guard is
        needed here (unlike ``refresh``'s plain-only unmount) — the overlay is
        being discarded and rebuilt either way, so a lazy fallback is safe."""
        merged = shlex.quote(self.merged)
        return f"""#!/usr/bin/env bash
set +e
fusermount3 -u {merged} 2>/dev/null || fusermount3 -uz {merged} 2>/dev/null || fusermount -u {merged} 2>/dev/null || fusermount -uz {merged} 2>/dev/null
if mountpoint -q {merged}; then
  echo "{_OVERLAY_FAILED} rc=6 (overlay remained mounted during reset)"
  exit 6
fi
echo "{_OVERLAY_OK}"
"""

    def _wipe_upper_script(self) -> str:
        """Wipe BOTH upperdir and workdir and recreate them fresh — a workdir
        must never be reused across overlay instances (design §11.2,
        fresh-workdir-per-epoch). Deliberately never references the merged
        mountpoint or the lower — only ``upper``/``work``."""
        upper = shlex.quote(self.upper)
        work = shlex.quote(self.work)
        return f"""#!/usr/bin/env bash
set -euo pipefail
trap 'rc=$?; echo "{_OVERLAY_FAILED} rc=${{rc}}"; exit "${{rc}}"' ERR
rm -rf {upper} {work}
mkdir -p {upper} {work}
echo "{_OVERLAY_OK}"
"""

    def _usage_script(self) -> str:
        upper = shlex.quote(self.upper)
        return f"""#!/usr/bin/env bash
set +e
du -sb {upper} 2>/dev/null
echo "{_OVERLAY_OK}"
"""

    # ----------------------------------------------------------- remote exec

    def _run(
        self, name: str, script: str, *, timeout: int = 30, require_ok: bool = True
    ) -> str:
        rel = (
            f".cache/srw/overlay/{self.thread_id}/scripts/{self._controller_id}/{name}"
        )
        self.workspace_backend.write_home_file(rel, script)
        script_path = self.workspace_backend.resolve_home_path(rel)
        command = (
            f"chmod 700 {shlex.quote(script_path)} && bash {shlex.quote(script_path)}; "
            f"rc=$?; rm -f {shlex.quote(script_path)}; exit $rc"
        )
        claim_exec = getattr(self.workspace_backend, "exec_claim_resource", None)
        if callable(claim_exec):
            output = claim_exec(
                command,
                timeout=timeout,
                operation=f"cloud overlay {name.removesuffix('.sh')}",
            )
        else:
            # Generic/custom backends and pinned-lane test doubles retain the
            # historical path.  RemoteBackend exposes exec_claim_resource for
            # both lanes; pinned mode forwards unchanged to exec_command.
            output = self.workspace_backend.exec_command(command, timeout=timeout)
        if require_ok and (_OVERLAY_OK not in output or _OVERLAY_FAILED in output):
            raise OverlayMountError(f"{name} did not report OK:\n{output}")
        return output

    def _run_terminal(self, name: str, script: str, *, timeout: int) -> str:
        rel = (
            f".cache/srw/overlay/{self.thread_id}/scripts/{self._controller_id}/{name}"
        )
        self.workspace_backend.write_home_file(rel, script)
        script_path = self.workspace_backend.resolve_home_path(rel)
        command = (
            f"chmod 700 {shlex.quote(script_path)} && bash {shlex.quote(script_path)}; "
            f"rc=$?; rm -f {shlex.quote(script_path)}; exit $rc"
        )
        execute = getattr(self.workspace_backend, "exec_terminal_claim_resource", None)
        if not callable(execute):
            raise OverlayMountError("terminal overlay cleanup has no resource fence")
        output = execute(
            command,
            timeout=timeout,
            operation="terminal cloud overlay cleanup",
        )
        if _OVERLAY_OK not in output or _OVERLAY_FAILED in output:
            raise OverlayMountError(f"{name} did not report OK:\n{output}")
        return output
