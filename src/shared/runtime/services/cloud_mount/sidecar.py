"""Cloud mounts a workspace Pod's sidecars own (connector drivers D7).

When the orchestrator created the Pod with the in-pod plane, its cloud
folders were mounted by the Pod's own sidecars before the agent arrived: the
FUSE daemons and their credentials live outside the workspace container, and
the workspace cannot reach either. The agent never starts or stops rclone
here. :class:`SidecarMountWatcher` stands in for ``RcloneMountManager`` in a
session (``active``, ``mounts``, ``status``, ``restart_mount``,
``refresh_vfs``, ``aclose``, ``detach_for_handoff``, ``retire_existing``)
and only:

* reads each mount's state from the supervisor's status files
  (``/srw/cloud-status/<index>.json``, read-only to the workspace) and the
  workspace's own mount table, with one ``cat`` that works on any image;
* waits, bounded, at attach for every mount to settle (mounted, or
  unavailable with a reason) - a mount that does not come up never holds the
  session back;
* links the mounted folders into ``workspace/cloud`` as the in-workspace
  manager does;
* asks the supervisor for a flush or a directory refresh by dropping a nonce
  into ``/srw/cloud-control``, and reads the answer back from the status
  files. That is all the workspace can ask for.

A reason is one of the supervisor's closed codes, never rclone's words.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import shlex
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

#: The reasons a status file may give; anything else reads as mount_failed.
SUPERVISOR_REASONS = frozenset(
    {
        "credential_rejected",
        "not_found",
        "unreachable",
        "timeout",
        "mount_failed",
        "config_missing",
    }
)
#: Plain words for every reason the user or the agent may see.
REASON_TEXT = {
    "credential_rejected": "the cloud refused its credential",
    "not_found": "the folder no longer exists in the cloud",
    "unreachable": "the cloud could not be reached",
    "timeout": "the cloud did not answer in time",
    "mount_failed": "the folder could not be mounted",
    "config_missing": "its credential never reached the cloud mount service",
    "sidecar_unavailable": "the workspace's cloud mount service is not running",
    "unbuildable": "it could not be prepared for this session",
    "set_fallback": "another folder of this session could not be prepared",
    "too_many_mounts": "the session has more cloud folders than a workspace mounts",
    "protected_unavailable": "the protected cloud could not be set up",
}

_MOUNTINFO = "==srw-mountinfo"


class SidecarMountError(RuntimeError):
    """A sidecar mount did not reach the state asked for in time."""


@dataclass(frozen=True)
class SidecarMountState:
    """One planned mount; the attributes the session's code reads."""

    index: int
    mount_id: str
    mount_kind: str
    target_path: str
    workspace_name: str
    access: str


def _unescape_mountinfo(field: str) -> str:
    if "\\" not in field:
        return field
    out: list[str] = []
    i = 0
    while i < len(field):
        if field[i] == "\\" and i + 4 <= len(field):
            try:
                out.append(chr(int(field[i + 1 : i + 4], 8)))
                i += 4
                continue
            except ValueError:
                pass
        out.append(field[i])
        i += 1
    return "".join(out)


def mounted_targets(mountinfo: str) -> dict[str, str]:
    """The filesystem type on top of the stack at each mountpoint."""
    tops: dict[str, str] = {}
    for line in mountinfo.splitlines():
        fields = line.split()
        if len(fields) < 7 or "-" not in fields[6:]:
            continue
        separator = fields.index("-", 6)
        if separator + 1 < len(fields):
            tops[_unescape_mountinfo(fields[4])] = fields[separator + 1]
    return tops


class SidecarMountWatcher:
    """The session's view of the mounts its Pod's sidecars own."""

    #: Tells session code apart from the in-workspace RcloneMountManager.
    delivery = "sidecar"

    def __init__(
        self,
        *,
        thread_id: str,
        cloud_cfg: dict[str, Any],
        workspace_backend: Any,
        workspace_root: Any,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        terminal: bool = False,
    ) -> None:
        self.thread_id = thread_id
        # End runs every remote command under its terminal token.
        self.terminal = terminal
        self.cloud_cfg = cloud_cfg or {}
        self.workspace_backend = workspace_backend
        remote_root = getattr(workspace_backend, "root", None)
        self.workspace_root = str(remote_root or workspace_root)
        self.fingerprint = str(self.cloud_cfg.get("fingerprint") or "")
        self.status_dir = str(self.cloud_cfg.get("status_dir") or "/srw/cloud-status")
        self.control_dir = str(
            self.cloud_cfg.get("control_dir") or "/srw/cloud-control"
        )
        self.wait_seconds = float(self.cloud_cfg.get("wait_seconds") or 30)
        # The Pod's UID: the orchestrator keeps a report only for this Pod.
        self.runtime_incarnation = str(self.cloud_cfg.get("runtime_incarnation") or "")
        # The orchestrator's record already saw every folder settle (a
        # re-attach, e.g. a stateless claim): read once, never wait again.
        self.settled_before = self.cloud_cfg.get("settled") is True
        self.drain_seconds = float(self.cloud_cfg.get("drain_seconds") or 60)
        self.skip_links = bool(self.cloud_cfg.get("skip_workspace_links"))
        self.excluded = [
            dict(entry)
            for entry in self.cloud_cfg.get("excluded") or []
            if isinstance(entry, dict)
        ]
        self.planned = [
            SidecarMountState(
                index=int(mount["index"]),
                mount_id=str(mount.get("mount_id") or mount.get("workspace_name")),
                mount_kind=str(mount.get("mount_kind") or "project"),
                target_path=str(mount["target_path"]),
                workspace_name=str(mount["workspace_name"]),
                access=str(mount.get("access") or "read_write"),
            )
            for mount in self.cloud_cfg.get("mounts") or []
        ]
        self._clock = clock
        self._sleep = sleep
        self._seen: dict[int, dict[str, Any]] = {}
        self._settled = False
        self._monitor: asyncio.Task | None = None
        # Set when a protected session runs without its cloud (decision 42):
        # every planned mount reports unavailable with this reason.
        self._unavailable_reason: str | None = None

    # ------------------------------------------------------------- the reads

    def _exec(self, command: str, *, timeout: int, operation: str) -> str:
        if self.terminal:
            execute = getattr(
                self.workspace_backend, "exec_terminal_claim_resource", None
            )
            if not callable(execute):
                raise SidecarMountError("End's cloud flush has no terminal fence")
            return execute(command, timeout, operation=operation)
        execute = getattr(self.workspace_backend, "exec_claim_resource", None)
        if execute is None:
            return self.workspace_backend.exec_command(command, timeout=timeout)
        return execute(command, timeout=timeout, operation=operation)

    def _read_command(self) -> str:
        files = " ".join(
            shlex.quote(f"{self.status_dir}/{mount.index}.json")
            for mount in self.planned
        )
        return (
            f"for f in {files}; do printf '==%s\\n' \"$f\"; "
            'cat "$f" 2>/dev/null; echo; done; '
            f"printf '{_MOUNTINFO}\\n'; cat /proc/self/mountinfo"
        )

    def read_states(self) -> dict[int, dict[str, Any]]:
        """Each planned mount's state now: ``state`` (pending, mounted,
        unavailable, or missing when its status file is absent), ``reason``,
        ``live`` (the workspace sees it mounted), and the last drain and
        refresh answers."""
        if not self.planned:
            return {}
        output = self._exec(
            self._read_command(), timeout=20, operation="read cloud mount status"
        )
        status_part, _, mountinfo = output.partition(f"{_MOUNTINFO}\n")
        tops = mounted_targets(mountinfo)
        raw_by_path: dict[str, str] = {}
        current: str | None = None
        for line in status_part.splitlines():
            if line.startswith("==") and line.endswith(".json"):
                current = line[2:]
                raw_by_path[current] = ""
            elif current is not None:
                raw_by_path[current] += line
        states: dict[int, dict[str, Any]] = {}
        for mount in self.planned:
            raw = raw_by_path.get(f"{self.status_dir}/{mount.index}.json", "").strip()
            live = tops.get(mount.target_path) == "fuse.rclone"
            entry: dict[str, Any] = {"state": "missing", "reason": None, "live": live}
            if raw:
                try:
                    status = json.loads(raw)
                except ValueError:
                    status = {}
                state = status.get("state")
                if state in {"pending", "mounted", "unavailable"}:
                    entry["state"] = state
                    if state == "unavailable":
                        reason = status.get("reason")
                        entry["reason"] = (
                            reason if reason in SUPERVISOR_REASONS else "mount_failed"
                        )
                for key in ("drain", "refresh"):
                    if isinstance(status.get(key), dict):
                        entry[key] = status[key]
            states[mount.index] = entry
        self._seen = states
        return states

    @staticmethod
    def _settled_entry(entry: dict[str, Any]) -> bool:
        return (entry["state"] == "mounted" and entry["live"]) or entry[
            "state"
        ] == "unavailable"

    # ---------------------------------------------------------- what is seen

    @property
    def active(self) -> bool:
        return bool(self.mounts)

    @property
    def mounts(self) -> list[SidecarMountState]:
        """The planned mounts that were mounted at the last read."""
        return [
            mount
            for mount in self.planned
            if self._seen.get(mount.index, {}).get("state") == "mounted"
            and self._seen.get(mount.index, {}).get("live")
        ]

    def mark_unavailable(self, reason: str) -> None:
        """Report every planned mount unavailable with ``reason`` from now on
        (a protected session that runs without its cloud, decision 42)."""
        self._unavailable_reason = reason

    def path_of(self, mount: SidecarMountState) -> str:
        """Where the agent finds a folder in its workspace."""
        if len(self.planned) == 1:
            return "workspace/cloud"
        return f"workspace/cloud/{mount.workspace_name}"

    def report(self) -> list[dict[str, Any]]:
        """Each mount's state for the orchestrator: mounted, pending, or
        unavailable with a closed reason (a missing status file after the
        wait means the sidecar is not running)."""
        rows: list[dict[str, Any]] = []
        for mount in self.planned:
            entry = self._seen.get(mount.index) or {"state": "missing", "live": False}
            if self._unavailable_reason is not None:
                rows.append(
                    {
                        "name": mount.workspace_name,
                        "state": "unavailable",
                        "reason": self._unavailable_reason,
                    }
                )
            elif entry["state"] == "mounted" and entry.get("live"):
                rows.append({"name": mount.workspace_name, "state": "mounted"})
            elif entry["state"] == "unavailable":
                rows.append(
                    {
                        "name": mount.workspace_name,
                        "state": "unavailable",
                        "reason": entry.get("reason") or "mount_failed",
                    }
                )
            elif entry["state"] == "missing" and self._settled:
                rows.append(
                    {
                        "name": mount.workspace_name,
                        "state": "unavailable",
                        "reason": "sidecar_unavailable",
                    }
                )
            else:
                rows.append({"name": mount.workspace_name, "state": "pending"})
        return rows

    def unavailable(self) -> list[dict[str, str]]:
        """What did not mount, in plain words (for the agent's prompt):
        ``name``, its ``path`` in the workspace (``workspace/cloud`` for a
        single folder), the closed ``reason`` and its ``text``; ``protected``
        when it is a protected session's whole cloud."""
        rows: list[dict[str, Any]] = []
        for row, mount in zip(self.report(), self.planned):
            if row["state"] != "unavailable":
                continue
            text = REASON_TEXT.get(row["reason"], REASON_TEXT["mount_failed"])
            entry: dict[str, Any] = {
                "name": row["name"],
                "path": self.path_of(mount),
                "reason": row["reason"],
                "text": text,
            }
            if self.cloud_cfg.get("protected"):
                entry["protected"] = True
            rows.append(entry)
        for excluded in self.excluded:
            reason = str(excluded.get("reason") or "")
            rows.append(
                {
                    "name": "",
                    "path": "",
                    "kind": str(excluded.get("mount_kind") or ""),
                    "reason": reason,
                    "text": REASON_TEXT.get(reason, REASON_TEXT["unbuildable"]),
                }
            )
        return rows

    def status(self) -> str:
        """An agent-safe summary of every planned mount, read now."""
        if not self.planned and not self.excluded:
            return "No cloud folders are attached to this session."
        try:
            self.read_states()
        except Exception as exc:
            return f"Cloud mount status is unavailable right now: {type(exc).__name__}."
        if self._unavailable_reason is not None:
            text = REASON_TEXT.get(
                self._unavailable_reason, REASON_TEXT["mount_failed"]
            )
            return (
                f"Protected cloud unavailable: {text}. This session has no cloud "
                "folder; nothing it writes reaches the cloud."
            )
        lines = ["Cloud folders (mounted by the workspace's cloud mount service):"]
        for row, mount in zip(self.report(), self.planned):
            where = self.path_of(mount)
            access = "read-only" if mount.access == "read_only" else "read-write"
            if row["state"] == "mounted":
                lines.append(f"- {where} ({mount.target_path}, {access}): mounted")
            elif row["state"] == "unavailable":
                text = REASON_TEXT.get(row["reason"], REASON_TEXT["mount_failed"])
                lines.append(f"- {where}: unavailable, {text} (read-only, empty)")
            else:
                lines.append(f"- {where}: still coming up (read-only until then)")
        for entry in self.excluded:
            text = REASON_TEXT.get(str(entry.get("reason")), REASON_TEXT["unbuildable"])
            lines.append(
                f"- a {entry.get('mount_kind') or 'cloud'} folder was not attached: {text}"
            )
        return "\n".join(lines)

    # --------------------------------------------------------------- attach

    async def start_all(self) -> None:
        """Wait (bounded) for every mount to settle, then link the mounted
        ones. Never raises for a mount that did not come up."""
        await asyncio.to_thread(self._start_sync)

    def _start_sync(self) -> None:
        # A re-attach whose folders already settled once reads them once.
        deadline = self._clock() + (0 if self.settled_before else self.wait_seconds)
        while True:
            states = self.read_states()
            if all(self._settled_entry(entry) for entry in states.values()):
                break
            if self._clock() >= deadline:
                break
            self._sleep(1.0)
        self._settled = True
        if self.planned and not self.skip_links:
            self._install_workspace_links()

    def _install_workspace_links(self) -> None:
        """Point ``workspace/cloud`` at the sidecars' folders, every planned
        one, mounted or not: one still coming up (or unavailable) is the
        opener's empty directory in a read-only view, so a write fails
        instead of landing in a plain local directory nobody syncs. A single
        folder is ``workspace/cloud`` itself; several are its entries, and
        ``workspace/cloud`` is the read-only root of the sidecars' volume."""
        lines = [
            "set -e",
            f"workspace={shlex.quote(self.workspace_root)}",
            'mkdir -p "${workspace}/.srw"',
            'entry="${workspace}/cloud"',
            'if [ -L "${entry}" ]; then rm "${entry}"; fi',
            'if [ -e "${entry}" ]; then '
            'mv "${entry}" "${workspace}/.srw/cloud.pre-rclone.$(date +%s)"; fi',
        ]
        roots = {mount.target_path.rsplit("/", 1)[0] for mount in self.planned}
        names_match = all(
            mount.target_path.rsplit("/", 1)[-1] == mount.workspace_name
            for mount in self.planned
        )
        if len(self.planned) == 1:
            lines.append(
                f"ln -sfn {shlex.quote(self.planned[0].target_path)} " + '"${entry}"'
            )
        elif len(roots) == 1 and names_match:
            lines.append(f"ln -sfn {shlex.quote(roots.pop())} " + '"${entry}"')
        else:
            lines.append('mkdir -p "${entry}"')
            for mount in self.planned:
                lines += [
                    'link="${entry}"/' + shlex.quote(mount.workspace_name),
                    'if [ -L "${link}" ]; then rm "${link}"; fi',
                    f"ln -sfn {shlex.quote(mount.target_path)} " + '"${link}"',
                ]
        lines.append("echo SRW_LINKS_OK")
        output = self._exec(
            "sh -c " + shlex.quote("\n".join(lines)),
            timeout=30,
            operation="link cloud folders",
        )
        if "SRW_LINKS_OK" not in output:
            raise SidecarMountError(
                "could not link the cloud folders into the workspace"
            )

    def remove_link_to(self, target: str) -> None:
        """Remove ``workspace/cloud`` when it is a link to ``target`` (a
        protected session that runs without its cloud must not keep a link
        to a capture overlay that is gone)."""
        script = "\n".join(
            [
                f"entry={shlex.quote(self.workspace_root)}/cloud",
                f"target={shlex.quote(target)}",
                'if [ -L "${entry}" ] && [ "$(readlink "${entry}")" = "${target}" ]; '
                'then rm -f -- "${entry}"; fi',
                "echo SRW_UNLINK_OK",
            ]
        )
        output = self._exec(
            "sh -c " + shlex.quote(script),
            timeout=30,
            operation="remove the capture overlay link",
        )
        if "SRW_UNLINK_OK" not in output:
            raise SidecarMountError("could not remove the workspace/cloud link")

    # ------------------------------------------------------------- requests

    def _request(self, command: str) -> str:
        nonce = secrets.token_hex(8)
        target = f"{self.control_dir}/{command}"
        self._exec(
            f"printf '%s' {nonce} > {shlex.quote(target)}.tmp && "
            f"mv {shlex.quote(target)}.tmp {shlex.quote(target)}",
            timeout=20,
            operation=f"request cloud {command}",
        )
        return nonce

    def _await_acks(
        self, kind: str, nonce: str, timeout: float, done: set[str]
    ) -> dict[int, dict[str, Any]]:
        deadline = self._clock() + timeout
        acks: dict[int, dict[str, Any]] = {}
        while True:
            states = self.read_states()
            acks = {
                index: entry[kind]
                for index, entry in states.items()
                if isinstance(entry.get(kind), dict)
                and entry[kind].get("nonce") == nonce
                and entry[kind].get("state") in done
            }
            if len(acks) == len(states) or self._clock() >= deadline:
                return acks
            self._sleep(1.0)

    def request_drain(self, timeout: float | None = None) -> tuple[bool, int]:
        """Ask the supervisor to flush every pending upload. Returns whether
        every planned folder answered ``drained`` (the supervisor answers so
        for a read-only or unavailable one), and how many uploads are known
        to be pending: -1 when that is unknown (a folder that did not
        answer, or could not say). Strict: anything else is not drained."""
        if not self.planned:
            return True, 0
        nonce = self._request("drain")
        acks = self._await_acks(
            "drain",
            nonce,
            (self.drain_seconds + 15) if timeout is None else timeout,
            {"drained", "incomplete"},
        )
        complete = len(acks) == len(self.planned) and all(
            ack.get("state") == "drained" for ack in acks.values()
        )
        lost = sum(
            ack["lost"]
            for ack in acks.values()
            if type(ack.get("lost")) is int and ack["lost"] > 0
        )
        if lost:
            # Drained, but a folder that is gone or refuses its credential
            # gave uploads up for good: retrying End cannot help.
            logger.warning(
                "Cloud folders drained, but %d upload(s) were lost: their folder "
                "is gone or refuses its credential",
                lost,
            )
        if complete:
            return True, 0
        counts = [ack.get("pending") for ack in acks.values()]
        known = len(acks) == len(self.planned) and all(
            type(count) is int and count >= 0 for count in counts
        )
        pending = sum(counts) if known else -1
        logger.warning(
            "Cloud folders not drained: %d of %d answered, %s upload(s) pending",
            len(acks),
            len(self.planned),
            pending if pending >= 0 else "an unknown number of",
        )
        return False, pending

    def refresh_vfs(self, *args: Any, **kwargs: Any) -> None:
        """Re-read every folder (after an applied review). Raises unless
        every planned folder answers ``done``: a protected review's upper
        reset relies on the lower showing what was just written."""
        if not self.planned:
            return
        nonce = self._request("refresh")
        acks = self._await_acks("refresh", nonce, 60, {"done", "failed"})
        if len(acks) != len(self.planned) or any(
            ack.get("state") != "done" for ack in acks.values()
        ):
            raise SidecarMountError("the cloud folders did not refresh")

    def restart_mount(self, mount_id: str) -> None:
        """The supervisor restarts a lost mount by itself: wait for it."""
        wanted = [m for m in self.planned if m.mount_id == mount_id]
        if not wanted:
            raise SidecarMountError(f"no planned mount {mount_id}")
        deadline = self._clock() + max(60.0, self.wait_seconds)
        while True:
            entry = self.read_states().get(wanted[0].index) or {}
            if entry.get("state") == "mounted" and entry.get("live"):
                return
            if self._clock() >= deadline:
                raise SidecarMountError(f"mount {mount_id} did not come back")
            self._sleep(1.0)

    # ------------------------------------------------------------ lifecycle

    def start_monitor(
        self,
        on_change: Callable[[list[dict[str, Any]]], Any],
        *,
        interval: float = 60.0,
    ) -> None:
        """Re-read every ``interval`` seconds and call ``on_change`` with the
        report when a mount's state changed (a pinned session's live view)."""
        if self._monitor is not None or not self.planned:
            return

        async def loop() -> None:
            last = self.report()
            while True:
                await asyncio.sleep(interval)
                try:
                    await asyncio.to_thread(self.read_states)
                    report = self.report()
                    if report != last:
                        last = report
                        result = on_change(report)
                        if asyncio.iscoroutine(result):
                            await result
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "Cloud mount status re-read failed: %s", type(exc).__name__
                    )

        self._monitor = asyncio.create_task(
            loop(), name=f"cloud-sidecar-monitor-{self.thread_id[:8]}"
        )

    async def _stop_monitor(self) -> None:
        task, self._monitor = self._monitor, None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async def detach_for_handoff(self) -> None:
        """Nothing of this session runs in the workspace for a successor."""
        await self._stop_monitor()

    async def aclose(self, *, strict: bool = False) -> None:
        """Best-effort flush on the way out (bounded); the sidecars stop with
        the Pod and drain again then. Never raises, ``strict`` included:
        nothing of the session runs in the workspace, so there is no resident
        whose retirement a failure here could leave unproven."""
        await self._stop_monitor()
        if not self.planned:
            return
        try:
            # request_drain logs a flush that did not complete.
            await asyncio.to_thread(self.request_drain, min(self.drain_seconds, 20.0))
        except Exception as exc:
            logger.warning("Cloud folder flush at close failed: %s", type(exc).__name__)

    async def retire_existing(self, *, drain: bool = True) -> dict[str, int]:
        """End's terminal step: ask for a flush and require it. Every planned
        read-write folder must answer drained (or be unavailable, which the
        supervisor answers as drained); an unknown count or no answer is not
        drained. Nothing of the session runs in the workspace, so there is
        nothing to stop or count there."""
        await self._stop_monitor()
        counters = {"rclone_mounts": 0, "rclone_processes": 0}
        if not drain or not self.planned:
            return counters
        complete, pending = await asyncio.to_thread(self.request_drain)
        if not complete:
            raise SidecarMountError(
                f"{pending} upload(s) still pending"
                if pending > 0
                else "the cloud folders did not confirm their flush"
            )
        return counters


__all__ = [
    "REASON_TEXT",
    "SUPERVISOR_REASONS",
    "SidecarMountError",
    "SidecarMountState",
    "SidecarMountWatcher",
    "mounted_targets",
]
