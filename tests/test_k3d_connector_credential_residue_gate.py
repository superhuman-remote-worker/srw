"""Safety contract and scanners of the local connector credential residue gate."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from uuid import uuid4

import pytest


_SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "k3d-connector-credential-residue-gate.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "k3d_connector_credential_residue_gate", _SCRIPT
)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

GATE_ID = "srw-c0-0123456789ab"
MARKERS = {
    "token": "srwc0tok" + "a1" * 12,
    "env": "srwc0env" + "b2" * 12,
    "ssh_repo_key:1": "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo+/0123",
}


def _args(**overrides):
    values = {
        "context": "k3d-srw",
        "namespace": "srw",
        "lane": "pinned",
        "model": "muse-spark-1.3-contributor",
        "gate_id": GATE_ID,
        "timeout_seconds": 900,
        "settle_seconds": 600,
        "snapshot_wait_seconds": 600,
        "keep": False,
        "skip_code_check": False,
        "run": False,
        "confirm": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _needs(*tools):
    for tool in tools:
        if shutil.which(tool) is None:
            pytest.skip(f"{tool} not available")


def test_plan_is_local_and_non_mutating(capsys) -> None:
    assert gate.main(["--gate-id", GATE_ID]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "plan"
    assert payload["cleanup"] == "planned"
    names = [phase["name"] for phase in payload["phases"]]
    assert "pinned_sqlite_scan" in names
    assert {
        "agent_pod_code_matches",
        "snapshot_running_strict",
        "bash_history_scan",
        "snapshot_settled_strict",
        "snapshot_s3_product",
    } <= set(names)
    # The agent's own code is checked before the first scan.
    assert names.index("agent_pod_code_matches") < names.index(
        "checkpoint_scan_running"
    )
    assert all(phase["result"] == "planned" for phase in payload["phases"])


@pytest.mark.parametrize(
    ("field", "value"),
    [("context", "main-dev"), ("namespace", "default")],
)
def test_cannot_escape_the_local_cluster(field, value) -> None:
    with pytest.raises(gate.SafetyError, match="restricted"):
        gate.validate_config(_args(**{field: value}))


def test_run_requires_exact_confirmation() -> None:
    with pytest.raises(gate.SafetyError, match="requires"):
        gate.validate_config(_args(run=True))
    with pytest.raises(gate.SafetyError, match="requires"):
        gate.validate_config(_args(run=True, confirm="yes"))
    with pytest.raises(gate.SafetyError, match="only with --run"):
        gate.validate_config(_args(confirm="LOCAL-K3D-DISPOSABLE"))
    config = gate.validate_config(_args(run=True, confirm="LOCAL-K3D-DISPOSABLE"))
    assert config.run is True


@pytest.mark.parametrize("gate_id", ["srw-c0-xyz", "srw-mr-residue-0123456789ab"])
def test_gate_id_is_exact(gate_id) -> None:
    with pytest.raises(gate.SafetyError, match="gate id"):
        gate.validate_config(_args(gate_id=gate_id))


def test_markers_are_unique_per_key_and_labels_carry_no_secret() -> None:
    _needs("ssh-keygen")
    values = gate.generate_secrets(GATE_ID)
    markers = values.markers()

    repo = {k: v for k, v in markers.items() if k.startswith("ssh_repo_key")}
    file_key = {k: v for k, v in markers.items() if k.startswith("ssh_key_connector")}
    assert repo and file_key
    # The shared ed25519 header line is skipped, so the two keys' needles
    # never collide and each needle is really part of its own key.
    assert not set(repo.values()) & set(file_key.values())
    assert all(needle in values.ssh_repo_key for needle in repo.values())
    assert all(needle in values.ssh_file_key for needle in file_key.values())
    for label in markers:
        assert all(needle not in label for needle in markers.values())


def test_fingerprints_are_what_ssh_keygen_reports(tmp_path) -> None:
    _needs("ssh-keygen")
    values = gate.generate_secrets(GATE_ID)
    for private_key, fingerprint in (
        (values.ssh_repo_key, values.ssh_repo_fingerprint),
        (values.ssh_file_key, values.ssh_file_fingerprint),
    ):
        path = tmp_path / "key"
        path.write_text(private_key)
        path.chmod(0o600)
        listed = subprocess.run(
            ["ssh-keygen", "-l", "-f", str(path)],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
        assert listed[1] == fingerprint
    assert values.ssh_repo_fingerprint != values.ssh_file_fingerprint


def test_connector_secrets_live_only_in_credentials() -> None:
    _needs("ssh-keygen")
    values = gate.generate_secrets(GATE_ID)
    markers = values.markers()
    bodies = gate.connector_bodies(GATE_ID, values, ssh_host="srw-gitea", ssh_port=2222)

    assert [body["type"] for body in bodies] == [
        "repository",
        "repository",
        "ssh_key",
        "generic",
    ]
    for body in bodies:
        outside = json.dumps({k: v for k, v in body.items() if k != "credentials"})
        assert all(needle not in outside for needle in markers.values())
        assert body["name"].startswith(GATE_ID)
    # Since C1 a host-less ssh_key is not delivered; the gate's has a host.
    assert bodies[2]["config"] == {"host": "srw-gitea", "user": "git", "port": 2222}
    with pytest.raises(gate.GateFailure, match="endpoint"):
        gate.connector_bodies(GATE_ID, values, ssh_host="bad host", ssh_port=22)


class _FakeKube:
    """Records every kubectl invocation; answers like the orchestrator."""

    def __init__(self, rows=None) -> None:
        self.calls: list[tuple[list[str], str | None]] = []
        self.rows = list(rows or [])

    def run(self, arguments, *, operation, data=None, timeout=None, ok_codes=(0,)):
        self.calls.append((list(arguments), data))
        if operation == "keycloak login":
            return json.dumps({"id_token": "header.payload.signature"})
        if operation == "find agent pod":
            return "pod/" + arguments[2]
        return json.dumps({"id": str(uuid4())}) + "\n200"

    def sql(self, query, *, operation):
        self.calls.append((["sql"], query))
        row = self.rows.pop(0) if len(self.rows) > 1 else self.rows[0]
        return json.dumps(row)

    def python(self, target, container, script, *, operation):
        self.calls.append((["python", target, container], script))
        assert operation == "Gitea SSH endpoint"
        return {"host": "srw-gitea", "port": 2222}


def test_job_carries_the_model_override() -> None:
    _needs("ssh-keygen")
    kube = _FakeKube()
    runner = gate.Gate(gate.validate_config(_args()), kube, password="pw")

    runner.create(gate.Api(kube, runner.password), gate.generate_secrets(GATE_ID))

    bodies = [
        json.loads(json.loads(data.split("data-binary = ")[1]))
        for _arguments, data in kube.calls
        if data and "data-binary = " in data
    ]
    ssh_key = next(body for body in bodies if body.get("type") == "ssh_key")
    assert ssh_key["config"] == {"host": "srw-gitea", "user": "git", "port": 2222}
    job_body = bodies[-1]
    assert job_body["config_override"] == {
        "llm": {"model": "muse-spark-1.3-contributor"}
    }
    assert job_body["execution_lane"] == "pinned"
    other = gate.validate_config(_args(model="other-model"))
    assert other.model == "other-model"
    with pytest.raises(gate.SafetyError, match="model"):
        gate.validate_config(_args(model="bad model; rm"))


def test_secrets_and_bearer_never_appear_in_an_argument() -> None:
    _needs("ssh-keygen")
    kube = _FakeKube()
    config = gate.validate_config(_args())
    runner = gate.Gate(config, kube, password="pw-not-in-argv")
    values = gate.generate_secrets(GATE_ID)

    runner.create(gate.Api(kube, runner.password), values)

    assert len(runner.datasource_ids) == 4 and runner.job_id
    secrets = [*values.markers().values(), "pw-not-in-argv", "payload.signature"]
    for arguments, _data in kube.calls:
        joined = " ".join(arguments)
        assert all(secret not in joined for secret in secrets)
    stdin = "".join(data or "" for _arguments, data in kube.calls)
    assert values.token in stdin and values.env_value in stdin


@pytest.mark.parametrize(
    ("row", "message"),
    [
        ({"status": "failed", "llm_failure": True}, "LLM unavailable"),
        ({"status": "paused", "llm_failure": True}, "LLM unavailable"),
        ({"status": "failed", "llm_failure": False}, "job stopped"),
    ],
)
def test_early_stop_fails_clearly(row, message) -> None:
    runner = gate.Gate(gate.validate_config(_args()), _FakeKube([row]), "pw")
    runner.job_id = str(uuid4())
    with pytest.raises(gate.GateFailure, match=message) as raised:
        runner.workspace()
    assert "before the scans could run" in str(raised.value)
    assert "muse-spark-1.3-contributor" in str(raised.value)


def test_paused_on_llm_outage_after_the_scans_still_fails() -> None:
    row = {"status": "paused", "llm_failure": True}
    runner = gate.Gate(gate.validate_config(_args()), _FakeKube([row]), "pw")
    runner.job_id = str(uuid4())
    runner.scans_started = True
    with pytest.raises(gate.GateFailure, match="LLM unavailable"):
        runner.settle()
    assert runner.report.phases[-1] == {
        "name": "job_settled",
        "result": "fail",
        "status": "paused",
    }


@pytest.mark.parametrize(
    ("lane", "row"),
    [
        ("pinned", {"status": "processing", "agent": "srw-agent-j-0a1b2c3d"}),
        ("stateless", {"status": "processing", "leased_by": "srw-agent-s-1"}),
    ],
)
def test_agent_pod_is_resolved_per_lane_and_code_checked(lane, row) -> None:
    kube = _FakeKube([row])
    runner = gate.Gate(gate.validate_config(_args(lane=lane)), kube, "pw")
    runner.job_id = str(uuid4())
    checked = []
    runner.code_check = lambda target, container, paths: checked.append(
        (target, container, tuple(paths))
    )

    name = runner.agent_pod()

    expected = row.get("agent") or row.get("leased_by")
    assert name == expected
    assert checked == [(expected, "agent", gate.AGENT_FILES)]
    assert "src/shared/runtime/core/shell_protocol.py" in gate.AGENT_FILES


def test_pinned_lane_without_an_agent_pod_fails(monkeypatch) -> None:
    kube = _FakeKube([{"status": "processing", "agent": None}])
    runner = gate.Gate(gate.validate_config(_args()), kube, "pw")
    runner.job_id = str(uuid4())
    monkeypatch.setattr(
        gate,
        "wait_for",
        lambda check, **kwargs: check()
        or (_ for _ in ()).throw(gate.GateFailure("timed out")),
    )
    with pytest.raises(gate.GateFailure, match="no pinned agent pod"):
        runner.agent_pod()


class _WorkspaceKube(_FakeKube):
    """Answers the workspace probes: grep, the socket listing and find."""

    def __init__(self, *, env, sockets, keys_on_disk=(), legacy=()) -> None:
        super().__init__([{"status": "processing"}])
        self.env, self.sockets = env, sockets
        self.keys_on_disk, self.legacy = keys_on_disk, legacy

    def run(self, arguments, *, operation, data=None, timeout=None, ok_codes=(0,)):
        self.calls.append((list(arguments), data))
        if operation == "workspace shell":
            assert "agent-host" in arguments
            return "\n".join(self.sockets)
        if operation == "workspace grep":
            if arguments[-1] == "/home/agent-host":
                return "\n".join(self.keys_on_disk)
            return "\n".join(self.env)
        if operation == "legacy key probe":
            return "\n".join(self.legacy)
        raise AssertionError(operation)


def _keys():
    return gate.GateSecrets(
        token="t",
        env_name="C0_GATE_X",
        env_value="v",
        ssh_repo_key="",
        ssh_file_key="",
        ssh_repo_fingerprint="SHA256:repo",
        ssh_file_fingerprint="SHA256:file",
    )


def _materialize(kube, monkeypatch):
    runner = gate.Gate(gate.validate_config(_args()), kube, "pw")
    runner.job_id = str(uuid4())
    monkeypatch.setattr(
        gate,
        "wait_for",
        lambda check, **kwargs: check()
        or (_ for _ in ()).throw(gate.GateFailure("timed out: credentials")),
    )
    runner.runtime_received_credentials("workspace-1", MARKERS, _keys())
    return runner


def test_ssh_keys_are_proven_by_agent_socket_fingerprints(monkeypatch) -> None:
    kube = _WorkspaceKube(
        env=["/home/agent-host/.srw-credentials/abc.sh"],
        sockets=[
            "0123.sock SHA256:repo,",
            "4567.sock SHA256:file,",
            "89ab.sock SHA256:managed,",
        ],
    )
    runner = _materialize(kube, monkeypatch)

    phase = runner.report.phases[-1]
    assert phase["name"] == "runtime_received_credentials"
    assert phase["result"] == "pass"
    assert phase["agent_sockets"] == 3
    assert phase["env_files"] == ["~/.srw-credentials/abc.sh"]
    # The listing ran as agent-host with the script on stdin, never argv.
    shell = [c for c in kube.calls if c[0][-1] == "bash -s"]
    assert shell and "ssh-add -l" in shell[0][1]
    assert "agent-host" in shell[0][0]


@pytest.mark.parametrize(
    "sockets",
    [
        ["0123.sock SHA256:repo,"],  # the ssh_key connector was not delivered
        ["0123.sock SHA256:repo,SHA256:file,"],  # two keys behind one socket
        [],
    ],
)
def test_missing_or_shared_identities_never_pass(monkeypatch, sockets) -> None:
    kube = _WorkspaceKube(
        env=["/home/agent-host/.srw-credentials/abc.sh"], sockets=sockets
    )
    with pytest.raises(gate.GateFailure, match="timed out"):
        _materialize(kube, monkeypatch)


def test_a_socket_holding_two_keys_fails_once_both_are_held(monkeypatch) -> None:
    kube = _WorkspaceKube(
        env=["/home/agent-host/.srw-credentials/abc.sh"],
        sockets=[
            "0123.sock SHA256:repo,",
            "4567.sock SHA256:file,",
            "89ab.sock SHA256:a,SHA256:b,",
        ],
    )
    with pytest.raises(gate.GateFailure, match="exactly one key"):
        _materialize(kube, monkeypatch)


@pytest.mark.parametrize(
    ("found", "legacy"),
    [
        (["/home/agent-host/workspace/key.pem"], []),
        ([], ["/home/agent-host/.ssh/repo_c0"]),
        ([], []),
    ],
)
def test_no_private_key_or_legacy_key_file_on_the_workspace(found, legacy) -> None:
    kube = _WorkspaceKube(env=[], sockets=[], keys_on_disk=found, legacy=legacy)
    runner = gate.Gate(gate.validate_config(_args()), kube, "pw")
    runner.job_id = str(uuid4())
    if found or legacy:
        with pytest.raises(gate.GateFailure, match="private key"):
            runner.no_key_on_disk("workspace-1", MARKERS)
    else:
        runner.no_key_on_disk("workspace-1", MARKERS)
    phase = runner.report.phases[-1]
    assert phase["name"] == "workspace_holds_no_key_file"
    assert phase["paths"] == sorted(
        p.replace("/home/agent-host/", "~/") for p in found + legacy
    )
    grep = next(c for c in kube.calls if "grep" in c[0])
    assert grep[1].splitlines()[0] == "PRIVATE KEY"
    assert MARKERS["ssh_repo_key:1"] in grep[1]
    assert all(needle not in " ".join(grep[0]) for needle in MARKERS.values())


def test_the_socket_listing_reads_a_real_ssh_agent() -> None:
    """The listing script and parser against a real ssh-agent and key."""
    _needs("ssh-agent", "ssh-add", "ssh-keygen", "bash")
    import tempfile
    import time

    values = gate.generate_secrets(GATE_ID)
    # A short home: AF_UNIX socket paths are limited to ~108 bytes.
    with tempfile.TemporaryDirectory(prefix="c0a") as home:
        sockets = Path(home) / ".ssh" / "srw-managed" / "sockets"
        sockets.mkdir(parents=True)
        socket = sockets / ("a" * 32 + ".sock")
        env = dict(os.environ, HOME=home)
        agent = subprocess.Popen(
            ["ssh-agent", "-D", "-a", str(socket)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            for _ in range(200):
                if socket.exists():
                    break
                time.sleep(0.01)
            subprocess.run(
                ["ssh-add", "-"],
                input=values.ssh_repo_key,
                env=dict(env, SSH_AUTH_SOCK=str(socket)),
                capture_output=True,
                text=True,
                check=True,
            )
            listing = subprocess.run(
                ["bash", "-c", gate.AGENT_SOCKETS_SCRIPT],
                env=env,
                capture_output=True,
                text=True,
                check=True,
            ).stdout
        finally:
            agent.terminate()
            agent.wait(timeout=10)

    assert gate.parse_agent_sockets(listing) == {
        socket.name: [values.ssh_repo_fingerprint]
    }


def test_api_reports_http_status_without_the_body() -> None:
    class Refusing(_FakeKube):
        def run(self, arguments, *, operation, **kwargs):
            if operation == "keycloak login":
                return super().run(arguments, operation=operation, **kwargs)
            return json.dumps({"detail": "leaky detail"}) + "\n409"

    api = gate.Api(Refusing(), "pw")
    with pytest.raises(gate.GateFailure) as raised:
        api.call("POST", "/api/jobs", {"x": 1}, operation="create job")
    assert str(raised.value) == "create job: HTTP 409"


def test_checkpoint_sql_refuses_malformed_input() -> None:
    sql = gate.checkpoint_scan_sql(MARKERS, job_id=str(uuid4()))
    assert "('token', '" in sql and "checkpoint_writes" in sql
    with pytest.raises(gate.GateFailure):
        gate.checkpoint_scan_sql(
            {"token": "x'; DROP TABLE jobs;--aaaaaaaa"}, job_id=str(uuid4())
        )
    with pytest.raises(ValueError):
        gate.checkpoint_scan_sql(MARKERS, job_id="not-a-uuid")


def _run_script(script: str, *, env: dict | None = None) -> dict:
    completed = subprocess.run(
        [sys.executable, "-"],
        input=script,
        capture_output=True,
        text=True,
        check=False,
        env=env,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.splitlines()[-1])


def test_file_scanner_finds_markers_across_chunk_boundaries(tmp_path) -> None:
    clean = tmp_path / "checkpoints" / "job_a.db"
    leaky = tmp_path / "phase_snapshots" / "job_a" / "checkpoint.db"
    clean.parent.mkdir(parents=True)
    leaky.parent.mkdir(parents=True)
    clean.write_bytes(b"\0" * 4096)
    token = MARKERS["token"].encode()
    leaky.write_bytes(b"x" * ((1 << 20) - 5) + token + b"y" * 10)

    result = _run_script(
        gate.file_scan_script(
            MARKERS, [str(tmp_path / "checkpoints"), str(tmp_path / "phase_snapshots")]
        )
    )

    assert result == {"files": 2, "hits": {"token": [str(leaky)]}}


def _fake_snapshot_package(root: Path, home_root: Path) -> Path:
    package = root / "fake" / "orchestrator" / "services"
    package.mkdir(parents=True)
    (root / "fake" / "orchestrator" / "__init__.py").write_text("")
    (package / "__init__.py").write_text("")
    (package / "snapshot_service.py").write_text(
        textwrap.dedent(
            f"""
            import os, struct, subprocess, tempfile

            def _archive(path):
                with open(path, "wb") as out:
                    subprocess.run(
                        "tar -C {home_root} -cf - home | zstd -q",
                        shell=True, stdout=out, check=True,
                    )

            class _Missing(Exception):
                response = {{"Error": {{"Code": "NoSuchKey"}}}}

            class _Body:
                # FAKE_SKIPPABLE_BYTES puts a zstd skippable frame of that
                # many bytes in front: a compressed object that large, made
                # lazily, which every zstd decoder steps over.
                def __init__(self, path):
                    self._skip = int(os.environ.get("FAKE_SKIPPABLE_BYTES", "0"))
                    self._header = (
                        struct.pack("<II", 0x184D2A50, self._skip)
                        if self._skip else b""
                    )
                    self._file = open(path, "rb")

                def read(self, size=-1):
                    if size is None or size < 0:
                        whole = self._header + bytes(self._skip) + self._file.read()
                        self._header, self._skip = b"", 0
                        return whole
                    if self._header:
                        part, self._header = self._header[:size], self._header[size:]
                        return part
                    if self._skip:
                        take = min(size, self._skip)
                        self._skip -= take
                        return bytes(take)
                    return self._file.read(size)

            class _S3:
                def get_object(self, Bucket, Key):
                    if os.environ.get("FAKE_SNAPSHOT_MISSING"):
                        raise _Missing()
                    fd, path = tempfile.mkstemp(suffix=".tar.zst")
                    os.close(fd)
                    _archive(path)
                    return {{"Body": _Body(path)}}

            class SnapshotService:
                is_available = False
                _s3 = None
                _bucket = "srw-snapshots"

                async def connect(self, db):
                    self.is_available = True
                    self._s3 = _S3()

                async def capture_vm_snapshot(self, **kwargs):
                    fd, path = tempfile.mkstemp(suffix=".tar.zst")
                    os.close(fd)
                    _archive(path)
                    return await self.upload_snapshot(kwargs["job_id"], path, {{}})
            """
        )
    )
    return root / "fake"


def _fixture_home(tmp_path: Path, leak: bool) -> Path:
    home_root = tmp_path / "fixture"
    home = home_root / "home" / "agent-host"
    (home / ".ssh").mkdir(parents=True)
    (home / ".ssh" / "config").write_text(
        "Match all\nInclude /home/agent-host/.ssh/srw-managed/config.d/*.conf\n"
    )
    (home / "workspace").mkdir()
    (home / "workspace" / "notes.md").write_text("kept\n")
    if leak:
        (home / ".srw-credentials").mkdir()
        (home / ".srw-credentials" / "abc.sh").write_text(
            f"export C0_GATE_X={MARKERS['env']}\n"
        )
        (home / ".ssh" / "repo_c0").write_text(MARKERS["ssh_repo_key:1"] + "\n")
        (home / ".bash_history").write_text(
            f"git clone https://oauth2:{MARKERS['token']}@c0-gate.invalid/x\n"
        )
    return home_root


def _assert_archive(archive: dict, leak: bool) -> None:
    assert archive["ssh_config_kept"] is True
    assert archive["bash_history_present"] is leak
    if leak:
        assert archive["hits"] == {
            "env": ["home/agent-host/.srw-credentials/abc.sh"],
            "ssh_repo_key:1": ["home/agent-host/.ssh/repo_c0"],
            "token": ["home/agent-host/.bash_history"],
        }
        assert archive["excluded_paths"] == [
            "home/agent-host/.srw-credentials",
            "home/agent-host/.srw-credentials/abc.sh",
            "home/agent-host/.ssh/repo_c0",
        ]
    else:
        assert archive["hits"] == {}
        assert archive["excluded_paths"] == []
    assert all(needle not in json.dumps(archive) for needle in MARKERS.values())


@pytest.mark.parametrize("leak", [True, False])
def test_capture_scanner_reports_paths_not_secrets(tmp_path, leak) -> None:
    _needs("tar", "zstd")
    fake = _fake_snapshot_package(tmp_path, _fixture_home(tmp_path, leak))
    env = dict(os.environ, PYTHONPATH=str(fake))

    result = _run_script(
        gate.snapshot_capture_script(
            MARKERS, job_id=str(uuid4()), ssh_host="127.0.0.1", ssh_port=1
        ),
        env=env,
    )

    for mode in ("non_strict", "strict"):
        assert result[mode]["captured"] is True
        _assert_archive(result[mode], leak)


def _local_producer(monkeypatch, fake: Path) -> None:
    """Run the pod side of the S3 scan as a local python on the fake package."""
    monkeypatch.setattr(gate, "s3_snapshot_producer", lambda: [sys.executable, "-"])
    monkeypatch.setenv("PYTHONPATH", str(fake))


@pytest.mark.parametrize("leak", [True, False])
def test_s3_scanner_reads_the_real_snapshot(tmp_path, monkeypatch, leak) -> None:
    _needs("tar", "zstd")
    fake = _fake_snapshot_package(tmp_path, _fixture_home(tmp_path, leak))
    _local_producer(monkeypatch, fake)

    result = gate.scan_s3_snapshot_stream(MARKERS, job_id=str(uuid4()))

    assert result is not None
    _assert_archive(result, leak)


def test_s3_scanner_reports_no_snapshot_yet(tmp_path, monkeypatch) -> None:
    _needs("tar", "zstd")
    fake = _fake_snapshot_package(tmp_path, _fixture_home(tmp_path, False))
    _local_producer(monkeypatch, fake)
    monkeypatch.setenv("FAKE_SNAPSHOT_MISSING", "1")

    assert gate.scan_s3_snapshot_stream(MARKERS, job_id=str(uuid4())) is None


def test_the_s3_stream_program_carries_no_marker() -> None:
    script = gate.s3_snapshot_stream_script(job_id=str(uuid4()))
    assert all(needle not in script for needle in MARKERS.values())
    compile(script, "<s3 stream>", "exec")


def test_a_truncated_s3_snapshot_is_never_read_as_clean(tmp_path, monkeypatch) -> None:
    _needs("tar", "zstd")
    archive = tmp_path / "cut.tar.zst"
    subprocess.run(
        f"tar -C {_fixture_home(tmp_path, True)} -cf - home | zstd -q",
        shell=True,
        stdout=archive.open("wb"),
        check=True,
    )
    data = archive.read_bytes()
    archive.write_bytes(data[: len(data) // 2])
    monkeypatch.setattr(gate, "s3_snapshot_producer", lambda: ["cat", str(archive)])

    with pytest.raises(gate.GateFailure, match="incomplete"):
        gate.scan_s3_snapshot_stream(MARKERS, job_id=str(uuid4()))


def test_a_failed_producer_is_never_read_as_clean(tmp_path, monkeypatch) -> None:
    """The whole archive arrived, but the pod side did not exit cleanly."""
    _needs("tar", "zstd")
    archive = tmp_path / "whole.tar.zst"
    subprocess.run(
        f"tar -C {_fixture_home(tmp_path, False)} -cf - home | zstd -q",
        shell=True,
        stdout=archive.open("wb"),
        check=True,
    )
    monkeypatch.setattr(
        gate,
        "s3_snapshot_producer",
        lambda: ["sh", "-c", f"cat {archive}; exit 7"],
    )

    with pytest.raises(gate.GateFailure, match="incomplete"):
        gate.scan_s3_snapshot_stream(MARKERS, job_id=str(uuid4()))


# -- bounded memory -----------------------------------------------------------
# The k3d run of 2026-10-08 OOM-killed the orchestrator: the S3 scan read a
# ~100 MB snapshot whole and decompressed it whole inside the pod. Each scan
# below runs under the same RLIMIT_DATA budget the pod programs set, on an
# archive whose one member alone is larger than that budget.

_BIG_MEMBER = gate.POD_MEMORY_BUDGET + (128 << 20)


def _big_fixture_home(tmp_path: Path) -> Path:
    home_root = _fixture_home(tmp_path, True)
    big = home_root / "home" / "agent-host" / "workspace" / "big.bin"
    with big.open("wb") as handle:
        handle.truncate(_BIG_MEMBER)  # sparse; tar reads zeros
    with (home_root / "home" / "agent-host" / ".bash_history").open("a") as handle:
        handle.write("tail\n")
    return home_root


def test_the_memory_cap_is_real() -> None:
    """Control: the cap does stop a whole read this large."""
    completed = subprocess.run(
        [sys.executable, "-"],
        input=gate._POD_MEMORY_CAP + f"cap_memory()\nb = bytes({_BIG_MEMBER})\n",
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode != 0
    assert "MemoryError" in completed.stderr


def test_the_capture_scan_streams_a_member_larger_than_the_cap(tmp_path) -> None:
    _needs("tar", "zstd")
    fake = _fake_snapshot_package(tmp_path, _big_fixture_home(tmp_path))
    env = dict(os.environ, PYTHONPATH=str(fake))

    result = _run_script(
        gate.snapshot_capture_script(
            MARKERS, job_id=str(uuid4()), ssh_host="127.0.0.1", ssh_port=1
        ),
        env=env,
    )

    for mode in ("non_strict", "strict"):
        assert result[mode]["captured"] is True
        _assert_archive(result[mode], True)


def test_the_s3_scan_streams_a_member_larger_than_the_cap(tmp_path) -> None:
    """Both ends capped: the pod program by itself, the local scan here.

    The object is larger than the cap compressed (a skippable frame in front)
    and its tar has a member larger than the cap, so a whole read on either
    side fails with a MemoryError.
    """
    _needs("tar", "zstd")
    fake = _fake_snapshot_package(tmp_path, _big_fixture_home(tmp_path))
    runner = textwrap.dedent(
        f"""
        import importlib.util, json, sys
        spec = importlib.util.spec_from_file_location("gate", {str(_SCRIPT)!r})
        gate = importlib.util.module_from_spec(spec)
        sys.modules["gate"] = gate
        spec.loader.exec_module(gate)
        exec(gate._POD_MEMORY_CAP)
        cap_memory()
        gate.s3_snapshot_producer = lambda: [sys.executable, "-"]
        result = gate.scan_s3_snapshot_stream({MARKERS!r}, job_id={str(uuid4())!r})
        print(json.dumps(result))
        """
    )
    env = dict(os.environ, PYTHONPATH=str(fake), FAKE_SKIPPABLE_BYTES=str(_BIG_MEMBER))
    result = _run_script(runner, env=env)

    _assert_archive(result, True)
    assert result["members"] >= 8
