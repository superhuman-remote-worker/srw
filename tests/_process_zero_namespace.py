"""PID-1 driver for process-zero scans against real, owned processes.

``tests/test_process_zero_scanner_real_processes.py`` runs this file as PID 1 of
a fresh PID namespace with a private ``/proc``. The namespace makes the scan
safe and its result exact:

* The scan only sees this driver, its fixtures and itself. A dedicated-UID
  termination scan can therefore never reach a desktop or CI process.
* Every fixture dies with the driver. When PID 1 of a namespace exits, the
  kernel kills everything else in it, so a failed assertion cannot leak a
  process.
* The fixtures and the scan run as a workspace UID that only they use, with
  every capability dropped, as they do in a workspace Pod.

The driver reads one JSON scenario from ``SRW_PZ_SCENARIO``. It starts the
fixtures, waits until each is observably in its intended state, runs the scan,
observes the fixtures again and prints one JSON result line. Standard library
only: it runs under ``sys.executable`` without the project on the path.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time

RESULT_MARKER = "__SRW_PZ_RESULT__ "
# The fixtures and the scan run as a UID that may not be able to reach the
# test's virtualenv, so they use the system interpreter, as a workspace does.
_PATH = "/usr/local/bin:/usr/bin:/bin"

# A process whose main thread exits while a sibling thread keeps executing.
# The sibling publishes a strictly increasing heartbeat, so "still executing"
# is observed rather than inferred from /proc.
_DEAD_LEADER = r"""
import ctypes, os, signal, sys, threading, time
beat = sys.argv[1]
if sys.argv[2] == "ignore-term":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
def spin():
    n = 0
    while True:
        n += 1
        with open(beat + ".tmp", "w") as fh:
            fh.write(str(n))
        os.replace(beat + ".tmp", beat)
        time.sleep(0.02)
threading.Thread(target=spin).start()
time.sleep(0.1)
ctypes.CDLL(None).pthread_exit(None)
"""

# A live process the scan's own UID cannot inspect: non-dumpable, so reading
# its environment needs CAP_SYS_PTRACE, which the scan does not have.
_NON_DUMPABLE = r"""
import ctypes, time
ctypes.CDLL(None).prctl(4, 0, 0, 0, 0)  # PR_SET_DUMPABLE = 0
while True:
    time.sleep(1)
"""

# A live process that keeps one exited, tagged child unreaped for its whole
# life, like a sandbox helper whose parent never calls wait().
_NON_REAPING_PARENT = r"""
import os, sys, time
tag = sys.argv[1]
env = {"PATH": "/usr/bin:/bin"}
if tag:
    env[sys.argv[2]] = tag
pid = os.fork()
if pid == 0:
    os.execve("/bin/sh", ["sh", "-c", "exit 0"], env)
with open(sys.argv[3], "w") as fh:
    fh.write(str(pid))
while True:
    time.sleep(1)
"""

# Continuously starts and reaps short-lived children, so the scan meets
# processes that exit while it is inspecting them.
_CHURN = r"""
import os, sys, time
tag = sys.argv[1]
env = {"PATH": "/usr/bin:/bin"}
if tag:
    env[sys.argv[2]] = tag
while True:
    pid = os.fork()
    if pid == 0:
        os.execve("/bin/sh", ["sh", "-c", "sleep 0.0$((RANDOM % 4 + 1))"], env)
    time.sleep(0.004)
    while True:
        try:
            done, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            break
        if done <= 0:
            break
"""

_SHELL_LOOPS = {
    "sleeper": "while :; do sleep 0.2; done",
    "term_ignorer": 'trap "" TERM; while :; do sleep 0.2; done',
    "term_logger": (
        "trap 'echo TERM >> \"$SRW_PZ_LOG\"; exit 0' TERM; while :; do sleep 0.1; done"
    ),
    "slow_exit": 'trap "sleep 0.3; exit 0" TERM; while :; do sleep 0.05; done',
}


def _drop(uid: int) -> list[str]:
    """Run as ``uid`` with no groups and no capabilities of any kind."""

    return [
        "setpriv",
        f"--reuid={uid}",
        f"--regid={uid}",
        "--clear-groups",
        "--inh-caps=-all",
        "--bounding-set=-all",
        "--",
    ]


def _status(pid: int) -> dict[str, str] | None:
    try:
        with open(f"/proc/{pid}/status") as fh:
            lines = fh.read().splitlines()
    except (FileNotFoundError, ProcessLookupError):
        return None
    fields = {}
    for line in lines:
        key, _, value = line.partition(":")
        fields[key] = value.strip()
    return fields


def _task_states(pid: int) -> list[str]:
    states = []
    try:
        tids = os.listdir(f"/proc/{pid}/task")
    except (FileNotFoundError, ProcessLookupError):
        return states
    for tid in tids:
        try:
            with open(f"/proc/{pid}/task/{tid}/stat", "rb") as fh:
                stat = fh.read()
        except (FileNotFoundError, ProcessLookupError):
            continue
        states.append(stat.rsplit(b") ", 1)[1].split()[0].decode())
    return states


def _executing(pid: int) -> bool:
    """True while any task of the group can still run userspace code."""

    return any(state not in {"Z", "X"} for state in _task_states(pid))


def _read_int(path: str) -> int | None:
    try:
        with open(path) as fh:
            return int(fh.read() or 0)
    except (FileNotFoundError, ValueError):
        return None


def _heartbeat_advancing(path: str, window: float = 0.4) -> bool:
    first = _read_int(path)
    time.sleep(window)
    second = _read_int(path)
    return first is not None and second is not None and second > first


def _wait(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


class Driver:
    def __init__(self, scenario: dict) -> None:
        self.scenario = scenario
        self.workspace_uid = int(scenario["workspace_uid"])
        self.other_uid = scenario.get("other_uid")
        self.tag_env = scenario.get("tag_env", "SRW_WORKSPACE_PROCESS_TAG")
        # The caller's directory, so a failed run can be inspected. The
        # fixtures write heartbeats there under the workspace UID.
        self.state = scenario["state_dir"]
        os.chmod(self.state, 0o777)
        self.fixtures: dict[str, dict] = {}
        self.unreaped: list[int] = []
        self.python = shutil.which("python3", path=_PATH) or "python3"

    def _env(self, spec: dict) -> dict[str, str]:
        env = {"PATH": _PATH, "SRW_PZ_LOG": self._path(spec, "log")}
        for name, value in spec.get("env", {}).items():
            env[name] = value
        return env

    def _path(self, spec: dict, suffix: str) -> str:
        return os.path.join(self.state, f"{spec['name']}.{suffix}")

    def _uid(self, spec: dict) -> int:
        if spec.get("uid") == "other":
            if self.other_uid is None:
                raise SystemExit("scenario needs a second UID")
            return int(self.other_uid)
        return self.workspace_uid

    def start(self, spec: dict) -> None:
        kind = spec["kind"]
        env = self._env(spec)
        record: dict = {"kind": kind}
        tag = spec.get("env", {}).get(self.tag_env, "")
        if kind in _SHELL_LOOPS:
            argv = ["sh", "-c", _SHELL_LOOPS[kind]]
        elif kind == "dead_leader":
            beat = self._path(spec, "beat")
            record["beat"] = beat
            mode = "ignore-term" if spec.get("ignore_term") else "default"
            argv = [self.python, "-c", _DEAD_LEADER, beat, mode]
        elif kind == "non_dumpable":
            argv = [self.python, "-c", _NON_DUMPABLE]
        elif kind == "non_reaping_parent":
            child = self._path(spec, "child")
            record["child_file"] = child
            argv = [self.python, "-c", _NON_REAPING_PARENT, tag, self.tag_env, child]
        elif kind == "churn":
            argv = [self.python, "-c", _CHURN, tag, self.tag_env]
        elif kind == "zombie":
            # A direct, never-reaped child of this driver: an exited process
            # of the workspace UID whose parent is outside the scan's scope.
            pid = os.fork()
            if pid == 0:
                os.setgroups([])
                os.setgid(self._uid(spec))
                os.setuid(self._uid(spec))
                os.execve("/bin/sh", ["sh", "-c", "exit 0"], env)
            self.unreaped.append(pid)
            record["pid"] = pid
            self.fixtures[spec["name"]] = record
            return
        else:
            raise SystemExit(f"unknown fixture kind {kind}")
        process = subprocess.Popen(
            _drop(self._uid(spec)) + argv,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        record["pid"] = process.pid
        record["process"] = process
        self.fixtures[spec["name"]] = record

    def ready(self, name: str) -> bool:
        record = self.fixtures[name]
        pid = record["pid"]
        kind = record["kind"]
        if kind == "zombie":
            status = _status(pid)
            return bool(status and status["State"].startswith("Z"))
        if kind == "dead_leader":
            status = _status(pid)
            return bool(
                status
                and status["State"].startswith("Z")
                and status.get("Threads") == "2"
                and _heartbeat_advancing(record["beat"], 0.1)
            )
        if kind == "non_reaping_parent":
            child = _read_int(record["child_file"])
            if child is None:
                return False
            record["child"] = child
            status = _status(child)
            return bool(status and status["State"].startswith("Z"))
        # Loops must have left setpriv and exec'd their real program.
        status = _status(pid)
        return bool(status and status.get("Name") not in {None, "setpriv"})

    def observe(self, name: str) -> dict:
        record = self.fixtures[name]
        pid = record["pid"]
        status = _status(pid)
        observed = {
            "pid": pid,
            "state": status["State"].split()[0] if status else "gone",
            "threads": status.get("Threads") if status else None,
            "executing": _executing(pid),
        }
        if "beat" in record:
            observed["heartbeat_advancing"] = _heartbeat_advancing(record["beat"])
        if record["kind"] == "non_reaping_parent":
            child = _status(record["child"])
            observed["child_state"] = child["State"].split()[0] if child else "gone"
        log = self._path({"name": name}, "log")
        if os.path.exists(log):
            with open(log) as fh:
                observed["log"] = fh.read().split()
        else:
            observed["log"] = []
        return observed

    def scan(self) -> dict:
        spec = self.scenario["scan"]
        env = {"PATH": _PATH, "HOME": self.state}
        env.update(spec.get("env", {}))
        started = time.monotonic()
        completed = subprocess.run(
            _drop(self.workspace_uid) + ["bash", "-c", spec["command"]],
            env=env,
            text=True,
            capture_output=True,
            timeout=float(spec.get("timeout", 30)),
            check=False,
        )
        return {
            "returncode": completed.returncode,
            "stderr": completed.stderr[-2000:],
            "stdout": completed.stdout[-2000:],
            "elapsed": round(time.monotonic() - started, 3),
        }

    def run(self) -> dict:
        for spec in self.scenario["fixtures"]:
            self.start(spec)
        not_ready = [
            name for name in self.fixtures if not _wait(lambda n=name: self.ready(n))
        ]
        if not_ready:
            return {"error": f"fixtures never became ready: {not_ready}"}
        before = {name: self.observe(name) for name in self.fixtures}
        scans = [self.scan() for _ in range(int(self.scenario.get("scans", 1)))]
        after = {name: self.observe(name) for name in self.fixtures}
        return {"before": before, "scans": scans, "after": after}


# ---------------------------------------------------------------------------
# Test-side launcher (imported by the tests, never run inside the namespace).


def _subordinate_range(path: str) -> tuple[int, int] | None:
    import getpass

    names = {str(os.getuid())}
    try:
        names.add(getpass.getuser())
    except Exception:
        pass
    try:
        with open(path) as fh:
            for line in fh:
                parts = line.strip().split(":")
                if len(parts) == 3 and parts[0] in names:
                    return int(parts[1]), int(parts[2])
    except (OSError, ValueError):
        return None
    return None


_LAUNCHER: tuple | str | None = None


def launcher() -> tuple[list[str], int, int | None] | str:
    """Return ``(prefix, workspace_uid, other_uid)`` or why none is usable.

    As root (a container), a plain PID namespace; the workspace and unrelated
    UIDs are simply 1000 and 2000. Unprivileged, a user namespace mapping this
    user to 0 and, when ``newuidmap`` and a subordinate range exist, 1..65535
    to that range, so the workspace UID is a subordinate UID no desktop
    process uses. Without a subordinate range the workspace UID is the
    mapped 0 with every capability dropped, and scenarios needing an
    unrelated UID skip.
    """

    global _LAUNCHER
    if _LAUNCHER is not None:
        return _LAUNCHER
    unshare = shutil.which("unshare")
    if unshare is None or shutil.which("setpriv") is None:
        _LAUNCHER = "unshare/setpriv are unavailable"
        return _LAUNCHER
    common = ["--pid", "--fork", "--mount", "--mount-proc"]
    candidates: list[tuple[list[str], int, int | None]] = []
    if os.geteuid() == 0:
        candidates.append(([unshare, *common], 1000, 2000))
    else:
        uids = _subordinate_range("/etc/subuid")
        gids = _subordinate_range("/etc/subgid")
        if (
            uids
            and gids
            and shutil.which("newuidmap")
            and shutil.which("newgidmap")
            and min(uids[1], gids[1]) > 2000
        ):
            count = min(uids[1], gids[1], 65535)
            candidates.append(
                (
                    [
                        unshare,
                        "--user",
                        "--map-user=0",
                        "--map-group=0",
                        f"--map-users=1:{uids[0]}:{count}",
                        f"--map-groups=1:{gids[0]}:{count}",
                        *common,
                    ],
                    1000,
                    2000,
                )
            )
        candidates.append(([unshare, "--user", "--map-root-user", *common], 0, None))
    reason = "private PID namespaces are unavailable"
    for prefix, workspace_uid, other_uid in candidates:
        probe = subprocess.run(
            [*prefix, sys.executable, "-c", "import os; assert os.getpid() == 1"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if probe.returncode == 0:
            _LAUNCHER = (prefix, workspace_uid, other_uid)
            return _LAUNCHER
        reason = (
            f"private PID namespaces are unavailable: {probe.stderr.strip()[-200:]}"
        )
    _LAUNCHER = reason
    return _LAUNCHER


def run_scenario(
    *,
    command: str,
    fixtures: list[dict],
    scan_env: dict[str, str] | None = None,
    tag_env: str = "SRW_WORKSPACE_PROCESS_TAG",
    scans: int = 1,
    timeout: float = 90,
) -> dict:
    """Run one scenario as PID 1 of a fresh namespace; raise ``LookupError``
    with the reason when this host cannot provide one."""

    import tempfile

    launch = launcher()
    if isinstance(launch, str):
        raise LookupError(launch)
    prefix, workspace_uid, other_uid = launch
    if other_uid is None and any(spec.get("uid") == "other" for spec in fixtures):
        raise LookupError("no subordinate UID range for an unrelated-UID fixture")
    # Under /tmp, not pytest's 0700 base: the workspace UID writes here.
    state = tempfile.mkdtemp(prefix="srw-pz-", dir="/tmp")
    scenario = {
        "workspace_uid": workspace_uid,
        "other_uid": other_uid,
        "tag_env": tag_env,
        "state_dir": state,
        "fixtures": fixtures,
        "scans": scans,
        "scan": {"command": command, "env": dict(scan_env or {})},
    }
    try:
        completed = subprocess.run(
            [*prefix, sys.executable, os.path.abspath(__file__)],
            env={**os.environ, "SRW_PZ_SCENARIO": json.dumps(scenario)},
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    finally:
        shutil.rmtree(state, ignore_errors=True)
    lines = [
        line[len(RESULT_MARKER) :]
        for line in completed.stdout.splitlines()
        if line.startswith(RESULT_MARKER)
    ]
    if not lines:
        raise AssertionError(
            f"driver exited {completed.returncode} without a result: "
            f"{completed.stderr[-2000:]}"
        )
    result = json.loads(lines[-1])
    if "error" in result:
        raise AssertionError(result["error"])
    return result


def main() -> None:
    if os.getpid() != 1:
        raise SystemExit("the process-zero driver must be PID 1 of its namespace")
    if shutil.which("setpriv") is None:
        raise SystemExit("setpriv is unavailable")
    result = Driver(json.loads(os.environ["SRW_PZ_SCENARIO"])).run()
    print(RESULT_MARKER + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
