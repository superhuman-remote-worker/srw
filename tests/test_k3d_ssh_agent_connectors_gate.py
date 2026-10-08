"""Safety contract for the local C1 ssh-agent connectors gate (never run here)."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[1] / "scripts" / "k3d-ssh-agent-connectors-gate.py"
)
_SPEC = importlib.util.spec_from_file_location("k3d_ssh_agent_connectors_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--turn-timeout", "5"],
        ["--job-timeout", "5"],
        ["--job-timeout", "7200"],
        ["--snapshot-timeout", "5"],
        ["--snapshot-timeout", "7200"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(monkeypatch, capsys):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in ("validation", "detach", "snapshot", "checkpoint", "wrong pin"):
        assert phase in out


def test_embedded_programs_compile():
    for program in (
        gate._API_PROGRAM,
        gate._GITEA_PROGRAM,
        gate._SNAPSHOT_PROGRAM,
        gate._HASH_PROGRAM,
    ):
        compile(program, "<gate program>", "exec")


def test_key_needles_identify_one_key_and_are_scrubbed():
    first, second = gate.make_key("x"), gate.make_key("y")
    assert first.needles and second.needles
    assert not set(first.needles) & set(second.needles)
    # The shared OpenSSH header line identifies nothing.
    assert first.private_key.splitlines()[1] not in first.needles
    assert "PRIVATE" not in gate._scrub(first.private_key).replace(
        "-----BEGIN OPENSSH PRIVATE KEY-----", ""
    ).replace("-----END OPENSSH PRIVATE KEY-----", "")
    for needle in first.needles:
        assert needle not in gate._scrub(first.private_key)


def test_workspace_scripts_are_valid_bash_and_carry_no_key(monkeypatch):
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION]
    )
    runner = gate.SshAgentConnectorsGate(args)
    runner.keys = {label: gate.make_key(label) for label in "abcd"}
    scripts: list[str] = []

    def record(pod, script, *, check=True):
        scripts.append(script)
        return 0, ""

    monkeypatch.setattr(runner, "ws", record)
    runner.workspace_checks("pod", labels="ABCD")

    assert scripts
    for script in scripts:
        assert "PRIVATE KEY-----" not in script
        assert subprocess.run(["bash", "-n"], input=script, text=True).returncode == 0
    assert runner.report.results[0][0] == "no-key (ABCD)"


def test_session_and_detach_scripts_are_valid_bash(monkeypatch):
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION]
    )
    runner = gate.SshAgentConnectorsGate(args)
    runner.keys = {label: gate.make_key(label) for label in "abcd"}
    runner.connectors = {label: f"id-{label}" for label in "ABCD"}
    runner.thread = "00000000-0000-4000-8000-000000000001"
    runner.gitea = {"owner": "srw", "ssh_host": "srw-gitea-ssh", "ssh_port": 2222}
    scripts: list[str] = []

    def record(pod, script, *, check=True):
        scripts.append(script)
        return 0, ""

    monkeypatch.setattr(runner, "ws", record)
    monkeypatch.setattr(runner, "workspace_pod", lambda selector: "pod")
    monkeypatch.setattr(runner, "turn", lambda text, step: None)
    monkeypatch.setattr(runner.api, "ok", lambda *a, **k: {})
    monkeypatch.setattr(gate, "sql", lambda query: "0")
    monkeypatch.setattr(gate, "in_orchestrator", lambda *a, **k: {})

    runner.session_checks()
    runner.detach()

    assert len(scripts) > 5
    for script in scripts:
        assert subprocess.run(["bash", "-n"], input=script, text=True).returncode == 0
    # Nothing was observed, so nothing may pass by default.
    assert not runner.report.passed


def _runner():
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION]
    )
    runner = gate.SshAgentConnectorsGate(args)
    runner.keys = {label: gate.make_key(label) for label in "abcd"}
    return runner


def test_a_regressed_201_on_a_refused_connector_is_still_cleaned_up(monkeypatch):
    runner = _runner()
    created = iter(range(100))

    def call(method, path, body=None):
        if method == "POST":
            return 201, {"id": f"stray-{next(created)}"}
        return 204, {}

    deleted: list[str] = []

    def cleanup_call(method, path, body=None):
        deleted.append(path)
        return 204, {}

    monkeypatch.setattr(runner.api, "call", call)
    runner.validation()

    assert not runner.report.passed
    assert sorted(runner.connectors.values()) == ["stray-0", "stray-1", "stray-2"]
    monkeypatch.setattr(runner.api, "call", cleanup_call)
    runner.cleanup()
    assert sorted(deleted) == [
        "/api/datasources/stray-0",
        "/api/datasources/stray-1",
        "/api/datasources/stray-2",
    ]


def test_the_alias_host_is_part_of_validation(monkeypatch):
    runner = _runner()
    bodies: list[dict] = []

    def call(method, path, body=None):
        bodies.append(body)
        return 400, {"detail": "refused passphrase"}

    monkeypatch.setattr(runner.api, "call", call)
    runner.validation()

    assert runner.report.passed
    assert not runner.connectors
    assert any(
        (body.get("config") or {}).get("host", "").lower().startswith("srw-repo-")
        for body in bodies
    )


@pytest.mark.parametrize(
    ("rc", "output", "count"),
    [
        (0, "ssh-agents=0", 0),
        (0, "noise\nssh-agents=2", 2),
        (0, "", None),
        (0, "0", None),
        (1, "ssh-agents=0", None),
        (126, "", None),
    ],
)
def test_the_end_count_needs_a_clean_exit_and_an_answer(rc, output, count):
    assert gate.ssh_agent_count(rc, output) == count


def test_the_end_count_script_counts_from_proc():
    result = subprocess.run(
        ["bash", "-s"], input=gate.COUNT_SSH_AGENTS, text=True, capture_output=True
    )
    assert result.returncode == 0
    assert gate.ssh_agent_count(result.returncode, result.stdout) is not None


@pytest.mark.parametrize("pods_after", [[], [{"metadata": {"name": "pod"}}]])
def test_end_fails_when_the_workspace_exec_fails(monkeypatch, pods_after):
    runner = _runner()
    runner.thread = "00000000-0000-4000-8000-000000000001"
    listings = iter([[{"metadata": {"name": "pod"}}], pods_after])
    monkeypatch.setattr(runner.api, "ok", lambda *a, **k: {})
    monkeypatch.setattr(gate, "sql", lambda query: "none")
    monkeypatch.setattr(gate.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        gate, "command", lambda args, **k: gate.json.dumps({"items": next(listings)})
    )
    monkeypatch.setattr(runner, "ws", lambda pod, script, check=True: (1, ""))

    runner.end()

    # A vanished workspace is a pass; a live one that did not answer is not.
    assert runner.report.passed is (not pods_after)


@pytest.mark.parametrize(
    ("origin", "alias"),
    [
        ("ssh://srw-repo-" + "a" * 32 + "/srw/r.git", "srw-repo-" + "a" * 32),
        ("srw-repo-" + "b" * 32 + ":srw/r.git", "srw-repo-" + "b" * 32),
        ("ssh://git@gitea:2222/srw/r.git", None),
        ("ssh://srw-repo-" + "a" * 32 + "/srw/other.git", None),
    ],
)
def test_origin_alias_accepts_both_url_forms(origin, alias):
    assert gate.origin_alias(origin, owner="srw", repo="r") == alias


# -- job snapshot by cancel, and snapshot ordering ------------------------------
# k3d runs of 2026-10-07/08: a stateless job's jobs/<id>/ snapshot is uploaded
# by a cancel of the running job, never by its completion or an approval, and
# cleanup's delete removes it. The gate cancels the running job itself, scans,
# and only then Ends the session and cleans up.


class _Clock:
    """A fake ``time`` for wait_for: every sleep advances the clock."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clocked(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(gate, "time", clock)
    return clock


def _gate_runner(*extra):
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION, *extra]
    )
    runner = gate.SshAgentConnectorsGate(args)
    runner.keys = {label: gate.make_key(label) for label in "abcd"}
    runner.thread = "00000000-0000-4000-8000-000000000001"
    runner.job = "00000000-0000-4000-8000-0000000000aa"
    return runner


def _statuses(monkeypatch, runner, *sequence):
    remaining = list(sequence)

    def status():
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    monkeypatch.setattr(runner, "job_status", status)


def _store(monkeypatch, *, configured=True, job=([],), thread=(), hits=()):
    """Fake the in-orchestrator listing and the local object scan.

    ``job`` is a sequence of listings; ``hits`` are keys whose scan finds a
    needle.
    """

    job_listings = list(job)
    log = []

    def program(source, request, timeout=180):
        assert source is gate._SNAPSHOT_PROGRAM
        assert request["mode"] == "list"
        (prefix,) = request["prefixes"]
        kind = "job" if prefix.startswith("jobs/") else "thread"
        log.append((kind, "list"))
        if not configured:
            return {"configured": False}
        if kind == "job":
            objects = job_listings.pop(0) if len(job_listings) > 1 else job_listings[0]
        else:
            objects = list(thread)
        return {"configured": True, "objects": list(objects)}

    def scan_object(key, needles, timeout=900):
        assert b"PRIVATE KEY" in needles
        log.append(("job" if key.startswith("jobs/") else "thread", "scan"))
        return gate.ObjectScan(complete=True, hit=key in hits, size=1)

    monkeypatch.setattr(gate, "in_orchestrator", program)
    monkeypatch.setattr(gate, "scan_snapshot_object", scan_object)
    return log


def _api(monkeypatch, runner):
    calls = []
    monkeypatch.setattr(runner.api, "ok", lambda *a, **k: calls.append(a) or {})
    return calls


def test_a_running_job_is_cancelled_and_its_snapshot_scanned(monkeypatch, clocked):
    runner = _gate_runner()
    _statuses(monkeypatch, runner, "processing", "processing", "cancelled")
    calls = _api(monkeypatch, runner)
    log = _store(
        monkeypatch,
        job=([], [], ["jobs/x/1"], ["jobs/x/1", "jobs/x/2"], ["jobs/x/1", "jobs/x/2"]),
    )

    runner.job_snapshot()

    assert calls == [("PUT", f"/api/jobs/{runner.job}/cancel")]
    assert runner.report.passed
    assert [name for name, _, _ in runner.report.results] == [
        "job-snap: the cancel finished",
        "snapshot (job): no key in snapshot objects",
    ]
    # Scanned only once the listing held still.
    assert log.index(("job", "scan")) > 4
    assert not runner.report.skipped


def test_a_key_in_the_cancel_snapshot_fails(monkeypatch, clocked):
    runner = _gate_runner()
    _statuses(monkeypatch, runner, "processing", "cancelled")
    _api(monkeypatch, runner)
    _store(monkeypatch, job=(["jobs/x/1"],), hits=["jobs/x/1"])

    runner.job_snapshot()

    assert not runner.report.passed


@pytest.mark.parametrize("allowed", [True, False])
def test_a_cancel_that_leaves_no_object_fails_with_a_store(
    monkeypatch, clocked, allowed
):
    flags = ["--snapshot-timeout", "60"] + (["--allow-no-snapshot"] if allowed else [])
    runner = _gate_runner(*flags)
    _statuses(monkeypatch, runner, "processing", "cancelled")
    _api(monkeypatch, runner)
    _store(monkeypatch, job=([],))

    runner.job_snapshot()

    assert not runner.report.passed
    assert ("snapshot (job): the cancel took a snapshot", False) in [
        (name, ok) for name, ok, _ in runner.report.results
    ]
    assert clocked.now <= 120


def test_a_cancel_that_never_finishes_fails(monkeypatch, clocked):
    runner = _gate_runner("--job-timeout", "60", "--snapshot-timeout", "30")
    _statuses(monkeypatch, runner, "processing")
    _api(monkeypatch, runner)
    _store(monkeypatch, job=(["jobs/x/1"],))

    runner.job_snapshot()

    assert ("job-snap: the cancel finished", False) in [
        (name, ok) for name, ok, _ in runner.report.results
    ]


@pytest.mark.parametrize("settled", ["completed", "pending_review", "failed"])
def test_a_job_that_settled_first_is_skipped_not_failed(
    monkeypatch, clocked, settled, capsys
):
    runner = _gate_runner()
    _statuses(monkeypatch, runner, settled)
    monkeypatch.setattr(runner.api, "ok", lambda *a, **k: pytest.fail("no cancel"))
    log = _store(monkeypatch, job=([],))

    runner.job_snapshot()
    runner.report.check("stand-in", True)

    assert runner.report.passed
    assert [name for name, _ in runner.report.skipped] == ["snapshot (job)"]
    assert "k3d completion takes no snapshot" in capsys.readouterr().out
    # One listing, no wait.
    assert log.count(("job", "list")) == 1
    assert clocked.now == 0


def test_a_settled_job_with_objects_is_still_scanned(monkeypatch, clocked):
    runner = _gate_runner()
    _statuses(monkeypatch, runner, "completed")
    monkeypatch.setattr(runner.api, "ok", lambda *a, **k: pytest.fail("no cancel"))
    _store(monkeypatch, job=(["jobs/x/1"],))

    runner.job_snapshot()

    assert runner.report.results[-1][:2] == (
        "snapshot (job): no key in snapshot objects",
        True,
    )


@pytest.mark.parametrize("allowed", [True, False])
def test_allow_no_snapshot_covers_only_a_missing_object_store(
    monkeypatch, clocked, allowed
):
    runner = _gate_runner(*(["--allow-no-snapshot"] if allowed else []))
    _statuses(monkeypatch, runner, "processing")
    monkeypatch.setattr(runner.api, "ok", lambda *a, **k: pytest.fail("no cancel"))
    _store(monkeypatch, configured=False)

    runner.job_snapshot()
    runner.snapshot()

    assert runner.report.passed is allowed
    # Reported once, not once per scan.
    assert len(runner.report.results) == 1


def test_an_absent_thread_snapshot_passes_as_absent(monkeypatch, clocked):
    runner = _gate_runner()
    _store(monkeypatch)

    runner.snapshot()

    assert runner.report.results == [
        (
            "snapshot (thread): none to scan",
            True,
            f"threads/{runner.thread}/ absent",
        )
    ]


def test_a_present_thread_snapshot_is_scanned(monkeypatch, clocked):
    runner = _gate_runner()
    runner.job = None
    _store(monkeypatch, thread=["threads/t/1"], hits=["threads/t/1"])

    runner.snapshot()

    assert runner.report.results[-1][:2] == (
        "snapshot (thread): no key in snapshot objects",
        False,
    )


def test_the_job_brief_holds_it_in_a_sleep(monkeypatch):
    runner = _gate_runner()
    runner.connectors = {label: f"id-{label}" for label in "ABCD"}
    bodies = []

    class Stop(Exception):
        pass

    def ok(method, path, body=None):
        bodies.append(body)
        raise Stop

    monkeypatch.setattr(runner.api, "ok", ok)
    with pytest.raises(Stop):
        runner.job_run()

    assert f"sleep {gate.JOB_SLEEP_SECONDS}" in bodies[0]["description"]


def test_run_scans_the_job_snapshot_before_end_and_cleanup(monkeypatch):
    runner = _gate_runner()
    runner.job = None
    order = []
    steps = (
        "preflight fixture validation connectors_setup session session_checks "
        "detach job_run job_snapshot end snapshot cleanup"
    ).split()
    for step in steps:
        monkeypatch.setattr(runner, step, lambda step=step: order.append(step) or None)
    runner.report.check("stand-in", True)

    assert runner.run() == 0
    assert order[-5:] == ["job_run", "job_snapshot", "end", "snapshot", "cleanup"]


def test_the_verdict_counts_skips_without_failing(monkeypatch, capsys):
    runner = _gate_runner("--skip-job")
    for step in (
        "preflight fixture validation connectors_setup session session_checks "
        "detach end snapshot cleanup"
    ).split():
        monkeypatch.setattr(runner, step, lambda: None)
    runner.report.check("stand-in", True)
    runner.report.skip("snapshot (job)", "job settled")

    assert runner.run() == 0
    last = capsys.readouterr().out.strip().splitlines()[-1]
    assert last.startswith(f"PASS {runner.gate_id}: 1 checks, 0 failed")
    assert "1 skipped" in last


# -- bounded memory -------------------------------------------------------------
# The k3d run of 2026-10-08 OOM-killed the orchestrator: the snapshot scan read
# a ~100 MB object whole and decompressed it whole inside the pod. Now the pod
# only passes bytes through and the gate scans the stream itself; both run
# here under the RLIMIT_DATA budget the pod programs set, on objects larger
# than that budget.

_BIG = gate.POD_MEMORY_BUDGET + (128 << 20)
_NEEDLE = b"c1-gate-needle-" + b"q" * 24


def _needs_zstd() -> None:
    import shutil

    if shutil.which("zstd") is None:
        pytest.skip("zstd not installed")


def _fake_store(root: Path) -> Path:
    """A fake ``orchestrator.services.snapshot_service`` for the pod program.

    FAKE_OBJECT (JSON) lists the object's segments, produced lazily:
    ``["skip", n]`` a zstd skippable frame of n bytes, ``["zeros", n]``,
    ``["file", path]`` and ``["hex", text]``. ``read()`` without a size
    returns it whole, the way a careless program would read it.
    """
    package = root / "fake" / "orchestrator" / "services"
    package.mkdir(parents=True)
    (root / "fake" / "orchestrator" / "__init__.py").write_text("")
    (package / "__init__.py").write_text("")
    (package / "snapshot_service.py").write_text(
        """
import json, os, struct

def _segments():
    for kind, value in json.loads(os.environ["FAKE_OBJECT"]):
        if kind == "skip":
            yield struct.pack("<II", 0x184D2A50, value)
            yield from _zeros(value)
        elif kind == "zeros":
            yield from _zeros(value)
        elif kind == "file":
            with open(value, "rb") as handle:
                while chunk := handle.read(1 << 20):
                    yield chunk
        else:
            yield bytes.fromhex(value)

def _zeros(count):
    while count:
        take = min(count, 1 << 20)
        count -= take
        yield bytes(take)

class _Body:
    def __init__(self):
        self._parts, self._pending = _segments(), b""

    def read(self, size=-1):
        if size is None or size < 0:
            return self._pending + b"".join(self._parts)
        while not self._pending:
            self._pending = next(self._parts, b"")
            if not self._pending:
                return b""
        part, self._pending = self._pending[:size], self._pending[size:]
        return part

class _S3:
    def get_object(self, Bucket, Key):
        return {"Body": _Body()}

    def list_objects_v2(self, **kwargs):
        return {"Contents": [{"Key": "jobs/x/env.tar.zst"}]}

class SnapshotService:
    def __init__(self):
        self._s3, self._bucket = None, "srw-snapshots"

    async def connect(self, db):
        self._s3 = _S3()
"""
    )
    return root / "fake"


def _compressed(tmp_path: Path, size: int, tail: bytes) -> Path:
    """``size`` zeros then ``tail``, zstd-compressed (small on disk)."""
    out = tmp_path / "payload.zst"
    zeros = subprocess.Popen(
        ["head", "-c", str(size), "/dev/zero"], stdout=subprocess.PIPE
    )
    with out.open("wb") as handle:
        compressor = subprocess.Popen(
            ["zstd", "-q", "-c"], stdin=subprocess.PIPE, stdout=handle
        )
        assert zeros.stdout is not None and compressor.stdin is not None
        while chunk := zeros.stdout.read(1 << 20):
            compressor.stdin.write(chunk)
        compressor.stdin.write(tail)
        compressor.stdin.close()
        assert compressor.wait() == 0 and zeros.wait() == 0
    return out


def _run_capped(script: str, env: dict) -> str:
    """Run ``script`` in a fresh python under the pod programs' memory cap."""
    import os
    import textwrap

    runner = (
        textwrap.dedent(
            f"""
            import importlib.util, json, sys
            spec = importlib.util.spec_from_file_location("gate", {str(_SCRIPT)!r})
            gate = importlib.util.module_from_spec(spec)
            sys.modules["gate"] = gate
            spec.loader.exec_module(gate)
            exec(gate._POD_MEMORY_CAP)
            cap_memory()
            """
        )
        + script
    )
    completed = subprocess.run(
        [sys.executable, "-"],
        input=runner,
        capture_output=True,
        text=True,
        env=dict(os.environ, **env),
        timeout=300,
    )
    assert completed.returncode == 0, completed.stderr[-2000:]
    return completed.stdout.strip().splitlines()[-1]


def test_scan_stream_finds_a_needle_split_across_reads():
    data = b"a" * 10 + _NEEDLE + b"b" * 10
    for chunk_size in (1, 7, 16, len(data)):
        stream = __import__("io").BytesIO(data)
        found, size, head = gate.scan_stream(
            stream.read, [b"absent", _NEEDLE], chunk_size=chunk_size
        )
        assert found == {1} and size == len(data) and head == b"aaaa"


def test_the_memory_cap_is_real():
    """Control: under the cap, a whole read this large fails."""
    completed = subprocess.run(
        [sys.executable, "-"],
        input=gate._POD_MEMORY_CAP + f"cap_memory()\nb = bytes({_BIG})\n",
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode != 0
    assert "MemoryError" in completed.stderr


def test_every_pod_program_caps_its_memory():
    for program in (
        gate._API_PROGRAM,
        gate._GITEA_PROGRAM,
        gate._SNAPSHOT_PROGRAM,
        gate._HASH_PROGRAM,
    ):
        assert program.startswith(gate._POD_MEMORY_CAP)
        assert "\ncap_memory()\n" in program


def test_the_pod_program_passes_an_object_larger_than_the_cap_through(tmp_path):
    """The pod side alone: it caps itself, so a whole read would fail."""
    import json
    import os

    fake = _fake_store(tmp_path)
    env = dict(
        os.environ,
        PYTHONPATH=str(fake),
        FAKE_OBJECT=json.dumps([["zeros", _BIG], ["hex", _NEEDLE.hex()]]),
    )
    producer = subprocess.Popen(
        [sys.executable, "-c", gate._SNAPSHOT_PROGRAM],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    assert producer.stdin is not None and producer.stdout is not None
    producer.stdin.write(b'{"mode": "stream", "key": "jobs/x/raw"}\n')
    producer.stdin.close()
    found, size, _head = gate.scan_stream(producer.stdout.read, [_NEEDLE])
    stderr = producer.stderr.read() if producer.stderr else b""

    assert producer.wait() == 0, stderr[-2000:]
    assert size == _BIG + len(_NEEDLE)
    assert found == {0}


def _local_scan(tmp_path, segments, key="jobs/x/env.tar.zst") -> dict:
    """scan_snapshot_object, capped, with the pod program run locally."""
    import json

    fake = _fake_store(tmp_path)
    script = (
        "gate.snapshot_stream_command = lambda: "
        "[sys.executable, '-c', gate._SNAPSHOT_PROGRAM]\n"
        f"scan = gate.scan_snapshot_object({key!r}, [{_NEEDLE!r}])\n"
        "print(json.dumps(scan.__dict__))\n"
    )
    return json.loads(
        _run_capped(
            script,
            {"PYTHONPATH": str(fake), "FAKE_OBJECT": json.dumps(segments)},
        )
    )


def test_the_local_scan_streams_an_object_larger_than_the_cap(tmp_path):
    """Both ends capped. The object is larger than the cap compressed (a
    skippable frame in front) and decompressed, so a whole read on either
    side fails with a MemoryError."""
    _needs_zstd()
    payload = _compressed(tmp_path, _BIG, _NEEDLE)

    scan = _local_scan(tmp_path, [["skip", _BIG], ["file", str(payload)]])

    assert scan == {
        "complete": True,
        "hit": True,
        "size": _BIG + len(_NEEDLE),
        "detail": "",
    }


def test_a_truncated_object_is_never_read_as_clean(tmp_path):
    _needs_zstd()
    payload = _compressed(tmp_path, 1 << 20, _NEEDLE)
    cut = tmp_path / "cut.zst"
    cut.write_bytes(payload.read_bytes()[:-8])

    scan = _local_scan(tmp_path, [["file", str(cut)]])

    assert scan["complete"] is False


def test_a_compressed_object_without_a_zst_name_is_not_complete(tmp_path):
    _needs_zstd()
    payload = _compressed(tmp_path, 1 << 20, _NEEDLE)

    scan = _local_scan(tmp_path, [["file", str(payload)]], key="jobs/x/odd-name")

    assert scan["complete"] is False
