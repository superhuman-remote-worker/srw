"""Safety contract and evaluators of the local Virtual cloud folders gate
(features/cloud_folders_on_virtual_sessions.md § Acceptance; never run here)."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-virtual-cloud-folders-gate.py"
_SPEC = importlib.util.spec_from_file_location(
    "k3d_virtual_cloud_folders_gate", _SCRIPT
)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.cfc.LOCAL_CONFIRMATION],
        ["--gate-id", "cfc-0123456789"],
        ["--gate-id", "vcf-xyz"],
        ["--model", "bad model; rm -rf /"],
        ["--user", "Robert'); DROP"],
        ["--turn-timeout", "5"],
        ["--sweep"],
        ["--run", "--confirm", gate.cfc.LOCAL_CONFIRMATION, "--sweep"],
        ["--run", "--confirm", gate.cfc.LOCAL_CONFIRMATION, "--phases", "omitted"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    with pytest.raises(SystemExit) if "--phases" in argv else _no_raise():
        assert gate.main(argv) == 2


class _no_raise:
    def __enter__(self):
        return None

    def __exit__(self, *exc):
        return False


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in ("preflight", "attached", "empty", "readonly", "cleanup"):
        assert f"  - {phase}:" in out


def test_the_served_files_exist_and_carry_their_markers():
    for path in gate.SERVED:
        assert (ROOT / path).is_file(), path
    for path, marker in gate.AGENT_SERVED.items():
        assert marker in (ROOT / path).read_text(), (path, marker)


def test_the_prompts_name_the_tools_and_the_refusals_the_backend_raises():
    from agent.core.backends.virtual import (
        CloudRemote,
        VirtualWorkspaceBackend,
    )
    from shared.runtime.core.backends.object_store import InMemoryObjectStore

    backend = VirtualWorkspaceBackend(
        InMemoryObjectStore(),
        prefix="threads/t/",
        cloud_remotes=[
            CloudRemote(
                name="p",
                path="cloud/p",
                store=InMemoryObjectStore(),
                access="read_only",
            ),
        ],
    )
    with pytest.raises(ValueError) as excinfo:
        backend.search_files("ready", "")
    assert "search_files needs a path" in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        backend.write_file("cloud/p/x.txt", "ready")
    assert "read-only cloud folder (level read_only)" in str(excinfo.value)
    assert "write_file" in gate.WRITE_PROMPT and "list_files" in gate.LIST_PROMPT
    assert "search_files" in gate.ROOT_SEARCH_PROMPT


def test_the_gate_id_and_phases_are_its_own():
    assert gate._GATE_ID_RE.fullmatch("vcf-0123456789")
    assert not gate._GATE_ID_RE.fullmatch("cfc-0123456789")
    assert set(gate.PHASES) == {"attached", "empty", "readonly"}
    parser = gate.build_parser()
    args = parser.parse_args(
        ["--run", "--confirm", gate.cfc.LOCAL_CONFIRMATION, "--phases", "empty"]
    )
    assert args.phases == ["empty"]


def test_the_nextcloud_scripts_parse():
    for script in (
        gate._NEXTCLOUD_PUT_GROUP_FILE,
        gate._NEXTCLOUD_SESSION_DIR,
        gate._NEXTCLOUD_SESSION_LS,
    ):
        subprocess.run(["sh", "-n"], input=script, text=True, check=True)
