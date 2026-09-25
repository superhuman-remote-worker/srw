"""The generated process-zero programs against a scripted ``/proc``.

The real-process tests (``test_process_zero_scanner_real_processes.py``) cover
what this kernel does. These cover what other kernels and rare interleavings
do, deterministically and on any CI host:

* A task without an mm reads as ``ESRCH`` on Linux 6.16+ but as an empty
  environment on 6.15 and earlier.
* A thread can be created while a group is being inspected.
* ``status`` can be unreadable, or lack a field.

Each test extracts the exact Python program from the shell the backend
generates and executes it with ``open``/``os``/``time`` replaced by a
scripted process table. Nothing on the host is read or signalled.
"""

from __future__ import annotations

import builtins
import errno
import io
import shlex
import signal
import types
import uuid
from dataclasses import dataclass, field

import pytest

from shared.runtime.core.backends.remote import RemoteBackend

_TAG_ENV = "SRW_WORKSPACE_PROCESS_TAG"
_SCAN_PID = 100
_WORKSPACE_UID = 1000
_OTHER_UID = 2000
_ESRCH = ProcessLookupError(errno.ESRCH, "No such process")
_EACCES = PermissionError(errno.EACCES, "Permission denied")
_EIO = OSError(errno.EIO, "Input/output error")


@dataclass
class Proc:
    uid: int = _WORKSPACE_UID
    state: str = "S"
    threads: int = 1
    ppid: int = 1
    environ: bytes | BaseException = b""
    # tid -> environment (or the error reading it raises); defaults to the
    # leader alone, reading exactly like the process.
    tasks: dict[int, bytes | BaseException] | None = None
    # Successive answers to listdir(/proc/P/task); the last one repeats.
    listings: list[list[int]] | None = None
    status: str | BaseException | None = None
    # What SIGTERM / SIGKILL do: "exit" (gone), "zombie" (Z, Threads 1),
    # or "ignore".
    on_term: str = "exit"
    on_kill: str = "exit"


@dataclass
class Procfs:
    procs: dict[int, Proc]
    kills: list[tuple[int, int]] = field(default_factory=list)
    stderr: io.StringIO = field(default_factory=io.StringIO)
    now: float = 0.0

    def __post_init__(self) -> None:
        # The scan's own ancestry: python (100) -> bash (99) -> init (1).
        self.procs.setdefault(1, Proc(uid=0, ppid=0))
        self.procs.setdefault(99, Proc(ppid=1, environ=b"PATH=/usr/bin\0"))
        self.procs.setdefault(_SCAN_PID, Proc(ppid=99, environ=b"PATH=/usr/bin\0"))

    # -- /proc -------------------------------------------------------------
    def _proc(self, pid: str | int) -> Proc:
        try:
            return self.procs[int(pid)]
        except (KeyError, ValueError):
            raise FileNotFoundError(errno.ENOENT, "No such file or directory")

    def open(self, path: str, mode: str = "r", *args, **kwargs):
        parts = path.split("/")
        if parts[:2] != ["", "proc"]:
            return builtins.open(path, mode, *args, **kwargs)
        proc = self._proc(parts[2])
        leaf = parts[3:]
        if leaf == ["stat"]:
            return io.BytesIO(f"{parts[2]} (p) {proc.state} {proc.ppid} 0 0".encode())
        if leaf == ["status"]:
            if isinstance(proc.status, BaseException):
                raise proc.status
            text = proc.status
            if text is None:
                text = (
                    f"Name:\tp\nState:\t{proc.state} (x)\n"
                    f"Uid:\t{proc.uid}\t{proc.uid}\t{proc.uid}\t{proc.uid}\n"
                    f"Threads:\t{proc.threads}\n"
                )
            return io.StringIO(text)
        if leaf == ["environ"]:
            return self._read(proc.environ)
        if len(leaf) == 3 and leaf[0] == "task" and leaf[2] == "environ":
            tasks = (
                proc.tasks if proc.tasks is not None else {int(parts[2]): proc.environ}
            )
            if int(leaf[1]) not in tasks:
                raise FileNotFoundError(errno.ENOENT, "No such file or directory")
            return self._read(tasks[int(leaf[1])])
        raise AssertionError(f"unexpected procfs path {path}")

    @staticmethod
    def _read(value: bytes | BaseException):
        if isinstance(value, BaseException):
            raise value
        return io.BytesIO(value)

    def listdir(self, path: str) -> list[str]:
        if path == "/proc":
            return [str(pid) for pid in self.procs] + ["self", "sys"]
        parts = path.split("/")
        assert parts[3:] == ["task"], path
        pid = int(parts[2])
        proc = self._proc(pid)
        if proc.listings:
            listing = proc.listings[0]
            if len(proc.listings) > 1:
                proc.listings.pop(0)
            return [str(tid) for tid in listing]
        tasks = proc.tasks if proc.tasks is not None else {pid: proc.environ}
        return [str(tid) for tid in tasks]

    def exists(self, path: str) -> bool:
        parts = path.split("/")
        return len(parts) == 3 and parts[2].isdigit() and int(parts[2]) in self.procs

    def kill(self, pid: int, sig: int) -> None:
        if pid not in self.procs:
            raise ProcessLookupError(errno.ESRCH, "No such process")
        self.kills.append((pid, sig))
        outcome = (
            self.procs[pid].on_term
            if sig == signal.SIGTERM
            else self.procs[pid].on_kill
        )
        if outcome == "exit":
            del self.procs[pid]
        elif outcome == "zombie":
            self.procs[pid] = Proc(
                uid=self.procs[pid].uid, state="Z", threads=1, environ=_ESRCH
            )

    # -- execution ---------------------------------------------------------
    def run(self, command: str, marker: str) -> int:
        source, argv = _program(command, marker)
        fake_os = types.SimpleNamespace(
            getpid=lambda: _SCAN_PID,
            geteuid=lambda: _WORKSPACE_UID,
            listdir=self.listdir,
            kill=self.kill,
            path=types.SimpleNamespace(exists=self.exists),
        )

        def sleep(seconds: float) -> None:
            self.now += seconds

        fake_time = types.SimpleNamespace(monotonic=lambda: self.now, sleep=sleep)
        fake_sys = types.SimpleNamespace(argv=["-", *argv], stderr=self.stderr)
        modules = {"os": fake_os, "time": fake_time, "sys": fake_sys, "signal": signal}

        def fake_import(name, *args, **kwargs):
            if name in modules:
                return modules[name]
            return builtins.__import__(name, *args, **kwargs)

        scope = {
            "__name__": "__main__",
            "__builtins__": {
                **vars(builtins),
                "open": self.open,
                "__import__": fake_import,
            },
        }
        try:
            exec(compile(source, "<process-zero>", "exec"), scope)
        except SystemExit as exit_:
            return int(exit_.code or 0)
        return 0


def _program(command: str, marker: str) -> tuple[str, list[str]]:
    head, rest = command.split(f"<<'{marker}'", 1)
    argv = shlex.split(head.strip().splitlines()[-1])[2:]
    body = rest.split("\n", 1)[1]
    return body[: body.index(f"\n{marker}")], argv


def _tagged_backend() -> RemoteBackend:
    backend = RemoteBackend(
        host="workspace.test",
        workspace_path="/home/agent-host/workspace",
        job_id=str(uuid.uuid4()),
        workspace_generation=str(uuid.uuid4()),
        runtime_incarnation=str(uuid.uuid4()),
    )
    backend.set_shell_owner_token(21)
    return backend


_BACKEND = _tagged_backend()
_TAG = f"{_TAG_ENV}={_BACKEND._workspace_process_tag()}".encode()
_TAGGED = b"PATH=/usr/bin\0" + _TAG + b"\0"
_UNTAGGED = b"PATH=/usr/bin\0HOME=/home/agent-host\0"


def _tagged_scan(procs: dict[int, Proc], *, terminate: bool) -> tuple[int, Procfs]:
    procfs = Procfs(procs)
    command = _BACKEND._stateless_workspace_process_zero_shell(terminate=terminate)
    return procfs.run(command, "__SRW_PROCESS_ZERO_PY__"), procfs


def _uid_scan(procs: dict[int, Proc], *, terminate: bool) -> tuple[int, Procfs]:
    procfs = Procfs(procs)
    command = _BACKEND._dedicated_workspace_uid_process_zero_shell(terminate=terminate)
    return procfs.run(command, "__SRW_WORKSPACE_UID_ZERO_PY__"), procfs


class TestTaggedScanDeadLeaders:
    """A group is matched through any task that still holds its mm."""

    @pytest.mark.parametrize(
        "leader",
        [pytest.param(_ESRCH, id="linux-6.16+"), pytest.param(b"", id="linux-6.15")],
    )
    def test_a_dead_leader_is_matched_through_its_live_tagged_thread(self, leader):
        half = Proc(
            state="Z", threads=2, environ=leader, tasks={200: leader, 201: _TAGGED}
        )

        code, procfs = _tagged_scan({200: half}, terminate=False)

        assert code == 85
        assert procfs.kills == []

    @pytest.mark.parametrize(
        "leader",
        [pytest.param(_ESRCH, id="linux-6.16+"), pytest.param(b"", id="linux-6.15")],
    )
    def test_terminate_signals_the_dead_leader_group_by_tgid(self, leader):
        half = Proc(
            state="Z",
            threads=2,
            environ=leader,
            tasks={200: leader, 201: _TAGGED},
            on_term="zombie",
        )

        code, procfs = _tagged_scan({200: half}, terminate=True)

        assert code == 0
        assert procfs.kills == [(200, signal.SIGTERM)]

    @pytest.mark.parametrize(
        "leader",
        [pytest.param(_ESRCH, id="linux-6.16+"), pytest.param(b"", id="linux-6.15")],
    )
    def test_an_untagged_dead_leader_is_not_matched(self, leader):
        half = Proc(
            state="Z", threads=2, environ=leader, tasks={200: leader, 201: _UNTAGGED}
        )

        code, procfs = _tagged_scan({200: half}, terminate=True)

        assert code == 0
        assert procfs.kills == []

    def test_an_unreaped_zombie_is_not_a_target_and_does_not_abort(self):
        procs = {
            200: Proc(state="Z", threads=1, environ=_ESRCH),
            201: Proc(environ=_TAGGED, on_term="zombie"),
        }

        code, procfs = _tagged_scan(procs, terminate=True)

        assert code == 0
        assert procfs.kills == [(201, signal.SIGTERM)]

    def test_esrch_alone_does_not_dismiss_a_group_that_gains_a_thread(self):
        """Every listed task lost its mm, but a thread appeared meanwhile."""

        half = Proc(
            state="Z",
            threads=2,
            environ=_ESRCH,
            tasks={200: _ESRCH, 201: _ESRCH, 202: _TAGGED},
            listings=[[200, 201], [200, 202], [200, 202]],
        )

        code, _ = _tagged_scan({200: half}, terminate=False)

        assert code == 85

    def test_a_thread_list_that_never_settles_is_ambiguous(self):
        half = Proc(
            state="Z",
            threads=2,
            environ=_ESRCH,
            tasks={200: _ESRCH, **{tid: _ESRCH for tid in range(300, 320)}},
            listings=[[200, tid] for tid in range(300, 320)],
        )

        code, _ = _tagged_scan({200: half}, terminate=False)

        assert code == 86

    def test_a_group_whose_every_task_lost_its_mm_is_not_a_target(self):
        exiting = Proc(
            state="R", threads=2, environ=_ESRCH, tasks={200: _ESRCH, 201: _ESRCH}
        )

        code, procfs = _tagged_scan({200: exiting}, terminate=True)

        assert code == 0
        assert procfs.kills == []

    def test_an_empty_environment_is_simply_untagged(self):
        code, _ = _tagged_scan({200: Proc(environ=b"")}, terminate=False)

        assert code == 0

    def test_a_task_of_this_uid_that_cannot_be_read_is_ambiguous(self):
        half = Proc(
            state="Z", threads=2, environ=_ESRCH, tasks={200: _ESRCH, 201: _EACCES}
        )

        code, _ = _tagged_scan({200: half}, terminate=False)

        assert code == 86

    def test_an_unreadable_task_of_another_uid_is_not_this_workspace_s(self):
        half = Proc(
            uid=_OTHER_UID,
            state="Z",
            threads=2,
            environ=_ESRCH,
            tasks={200: _ESRCH, 201: _EACCES},
        )

        code, _ = _tagged_scan({200: half}, terminate=False)

        assert code == 0

    def test_any_other_procfs_error_still_refuses(self):
        code, procfs = _tagged_scan({200: Proc(environ=_EIO)}, terminate=True)

        assert code == 86
        assert procfs.kills == []

    def test_a_task_that_exits_mid_inspection_is_skipped(self):
        vanished = Proc(
            state="Z",
            threads=2,
            environ=_ESRCH,
            tasks={200: _ESRCH},
            listings=[[200, 201], [200]],
        )

        code, _ = _tagged_scan(
            {200: vanished, 201: Proc(environ=_UNTAGGED)}, terminate=False
        )

        assert code == 0

    def test_verify_never_signals(self):
        procs = {
            200: Proc(environ=_TAGGED),
            201: Proc(
                state="Z", threads=2, environ=_ESRCH, tasks={201: _ESRCH, 202: _TAGGED}
            ),
        }

        code, procfs = _tagged_scan(procs, terminate=False)

        assert code == 85
        assert procfs.kills == []

    def test_terminate_escalates_when_an_exiting_target_leaves_a_term_survivor(self):
        procs = {
            200: Proc(environ=_TAGGED, on_term="zombie"),
            201: Proc(environ=_TAGGED, on_term="ignore", on_kill="zombie"),
        }

        code, procfs = _tagged_scan(procs, terminate=True)

        assert code == 0
        assert (201, signal.SIGKILL) in procfs.kills
        assert procfs.now >= 5.0


class TestDedicatedUidScanDeadLeaders:
    """Only a zombie whose whole thread group has exited is not residual."""

    def test_a_zombie_leader_with_live_threads_is_residual(self):
        code, procfs = _uid_scan({200: Proc(state="Z", threads=2)}, terminate=False)

        assert code == 85
        assert "residual PIDs: 200" in procfs.stderr.getvalue()
        assert procfs.kills == []

    def test_an_exited_zombie_is_not_residual(self):
        code, _ = _uid_scan({200: Proc(state="Z", threads=1)}, terminate=False)

        assert code == 0

    def test_terminate_signals_a_dead_leader_group_until_it_has_exited(self):
        half = Proc(state="Z", threads=2, on_term="zombie")

        code, procfs = _uid_scan({200: half}, terminate=True)

        assert code == 0
        assert procfs.kills == [(200, signal.SIGTERM)]

    def test_terminate_escalates_a_term_ignoring_dead_leader_group(self):
        half = Proc(state="Z", threads=2, on_term="ignore", on_kill="zombie")

        code, procfs = _uid_scan({200: half}, terminate=True)

        assert code == 0
        assert (200, signal.SIGKILL) in procfs.kills

    def test_another_uid_is_never_counted(self):
        code, procfs = _uid_scan(
            {200: Proc(uid=_OTHER_UID, state="Z", threads=2)}, terminate=True
        )

        assert code == 0
        assert procfs.kills == []

    def test_a_status_without_a_thread_count_fails_closed(self):
        status = f"Name:\tp\nState:\tZ (zombie)\nUid:\t{_WORKSPACE_UID}\t0\t0\t0\n"

        code, procfs = _uid_scan({200: Proc(state="Z", status=status)}, terminate=True)

        assert code == 86
        assert procfs.kills == []

    def test_a_persistently_unreadable_status_fails_closed(self):
        code, _ = _uid_scan({200: Proc(status=_EIO)}, terminate=False)

        assert code == 86

    def test_the_scan_does_not_read_any_environment(self):
        """The UID boundary must not depend on a removable environment tag."""

        procs = {200: Proc(environ=_EIO, tasks={200: _EIO})}

        code, _ = _uid_scan(procs, terminate=False)

        assert code == 85
