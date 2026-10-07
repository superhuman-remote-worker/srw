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
        "gate_id": GATE_ID,
        "timeout_seconds": 900,
        "settle_seconds": 600,
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
    assert {"snapshot_non_strict", "snapshot_strict"} <= set(names)
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


def test_connector_secrets_live_only_in_credentials() -> None:
    _needs("ssh-keygen")
    values = gate.generate_secrets(GATE_ID)
    markers = values.markers()
    bodies = gate.connector_bodies(GATE_ID, values)

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


class _FakeKube:
    """Records every kubectl invocation; answers like the orchestrator."""

    def __init__(self) -> None:
        self.calls: list[tuple[list[str], str | None]] = []

    def run(self, arguments, *, operation, data=None, timeout=None, ok_codes=(0,)):
        self.calls.append((list(arguments), data))
        if operation == "keycloak login":
            return json.dumps({"id_token": "header.payload.signature"})
        return json.dumps({"id": str(uuid4())}) + "\n200"


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
            import os, subprocess, tempfile

            class SnapshotService:
                is_available = False

                async def connect(self, db):
                    self.is_available = False

                async def capture_vm_snapshot(self, **kwargs):
                    fd, path = tempfile.mkstemp(suffix=".tar.zst")
                    os.close(fd)
                    with open(path, "wb") as out:
                        subprocess.run(
                            "tar -C {home_root} -cf - home | zstd -q",
                            shell=True, stdout=out, check=True,
                        )
                    return await self.upload_snapshot(kwargs["job_id"], path, {{}})
            """
        )
    )
    return root / "fake"


@pytest.mark.parametrize("leak", [True, False])
def test_snapshot_scanner_reports_paths_not_secrets(tmp_path, leak) -> None:
    _needs("tar", "zstd")
    home_root = tmp_path / "fixture"
    home = home_root / "home" / "agent-host"
    (home / ".ssh").mkdir(parents=True)
    (home / ".ssh" / "config").write_text("Host c0-gate.invalid\n")
    (home / "workspace").mkdir()
    (home / "workspace" / "notes.md").write_text("kept\n")
    if leak:
        (home / ".srw-credentials").mkdir()
        (home / ".srw-credentials" / "abc.sh").write_text(
            f"export C0_GATE_X={MARKERS['env']}\n"
        )
        (home / ".ssh" / "repo_c0").write_text(MARKERS["ssh_repo_key:1"] + "\n")
    fake = _fake_snapshot_package(tmp_path, home_root)
    env = dict(os.environ, PYTHONPATH=str(fake))

    result = _run_script(
        gate.snapshot_script(
            MARKERS,
            job_id=str(uuid4()),
            ssh_host="127.0.0.1",
            ssh_port=1,
            include_product=True,
        ),
        env=env,
    )

    assert result["s3_product"] == {"available": False}
    for mode in ("non_strict", "strict"):
        capture = result[mode]
        assert capture["captured"] is True
        assert capture["ssh_config_kept"] is True
        if leak:
            assert capture["hits"] == {
                "env": ["home/agent-host/.srw-credentials/abc.sh"],
                "ssh_repo_key:1": ["home/agent-host/.ssh/repo_c0"],
            }
            assert capture["excluded_paths"] == [
                "home/agent-host/.srw-credentials",
                "home/agent-host/.srw-credentials/abc.sh",
                "home/agent-host/.ssh/repo_c0",
            ]
        else:
            assert capture["hits"] == {}
            assert capture["excluded_paths"] == []
        assert all(needle not in json.dumps(capture) for needle in MARKERS.values())
