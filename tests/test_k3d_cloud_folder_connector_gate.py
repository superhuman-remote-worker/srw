"""Safety contract and evaluators of the local cloud folder connector gate
(main cloud slice 3.5; never run here)."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

from orchestrator.services.thread_mount_rows import slugify_mount_name

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-cloud-folder-connector-gate.py"
_SPEC = importlib.util.spec_from_file_location(
    "k3d_cloud_folder_connector_gate", _SCRIPT
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


def _args(*extra: str):
    return gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION, *extra]
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "d7-0123456789"],
        ["--gate-id", "cfc-xyz"],
        ["--model", "bad model; rm -rf /"],
        ["--user", "Robert'); DROP"],
        ["--pod-timeout", "5"],
        ["--sweep"],
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION, "--sweep"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in (
        "preflight",
        "backfill",
        "omitted",
        "empty",
        "nochip",
        "broken",
        "foreign",
        "protected",
        "cleanup",
    ):
        assert f"  - {phase}:" in out


def test_the_served_files_exist_and_carry_their_markers():
    for path in gate.SERVED:
        assert (ROOT / path).is_file(), path
    for path, marker in gate.AGENT_SERVED.items():
        assert marker in (ROOT / path).read_text(), path


def test_the_shell_script_parses():
    subprocess.run(["sh", "-n", "-c", gate._NEXTCLOUD_SESSION_FILE], check=True)


def test_the_mount_name_is_the_planners():
    for name in ("cfc-0123456789 main", "Acme Launch", "", "ÄÖ ü"):
        assert gate.slug(name) == slugify_mount_name(name)


def test_plans_are_read_as_names_kinds_ids_and_reasons():
    plan = {
        "mounts": [
            {
                "index": 1,
                "name": "outbox",
                "mount_kind": "cloud_outbox",
                "mount_id": "o",
            },
            {"index": 0, "name": "main", "mount_kind": "project", "mount_id": "f"},
        ],
        "excluded": [
            {"source_ref": "x", "mount_kind": "project", "reason": "config_invalid"}
        ],
    }
    assert gate.plan_mounts(plan) == [
        ("main", "project", "f"),
        ("outbox", "cloud_outbox", "o"),
    ]
    assert gate.plan_exclusions(plan) == [("x", "project", "config_invalid")]
    assert gate.plan_mounts({}) == [] and gate.plan_exclusions({}) == []
    instance = gate.CloudFolderConnectorGate.__new__(gate.CloudFolderConnectorGate)
    assert instance.index_of(plan, "cloud_outbox") == 1
    with pytest.raises(gate.GateError):
        instance.index_of(plan, "user_root")


def test_the_prompt_block_is_read_out_of_an_archived_request():
    request = json.dumps(
        {
            "messages": [
                {
                    "role": "system",
                    "content": "Hi\n\n<cloud_folders>\nCloud folders in this "
                    "session's workspace:\n- workspace/cloud/outbox: a folder SRW "
                    "created for this session\n</cloud_folders>\n",
                }
            ]
        }
    )
    block = gate.prompt_block(request)
    assert "- workspace/cloud/outbox: a folder SRW created" in block
    assert gate.prompt_block('{"messages": []}') == ""


def test_cleanup_restores_a_repointed_handle_before_anything_else(monkeypatch):
    statements: list[str] = []
    monkeypatch.setattr(gate, "sql", lambda query: statements.append(query) or "")
    instance = gate.CloudFolderConnectorGate(_args("--gate-id", "cfc-0123456789"))
    instance.restore = ["UPDATE a", "UPDATE b"]
    instance.titled_threads = lambda: []
    instance.named_projects = lambda: []
    assert instance.cleanup() == []
    assert statements[:2] == ["UPDATE b", "UPDATE a"]
    assert instance.restore == []


def test_residue_names_what_was_left(monkeypatch):
    instance = gate.CloudFolderConnectorGate(_args("--gate-id", "cfc-0123456789"))
    instance.projects = {"main": "00000000-0000-4000-8000-000000000001"}
    instance.restore = ["UPDATE a"]
    instance.titled_threads = lambda: ["t"]
    instance.named_projects = lambda: []
    queries: list[str] = []
    monkeypatch.setattr(gate, "sql", lambda query: queries.append(query) or "1")
    left = instance.residue()
    assert any("sessions titled" in item for item in left)
    assert any("project cloud folder rows" in item for item in left)
    assert "a repointed handle was not restored" in left
    assert "project-cloud:00000000-0000-4000-8000-000000000001" in queries[0]


def test_the_results_can_be_written_as_json(tmp_path):
    out = tmp_path / "gate.json"
    instance = gate.CloudFolderConnectorGate(
        _args("--gate-id", "cfc-0123456789", "--json", str(out))
    )
    instance.report.results.append(("omitted: x", True, ""))
    instance.write_json()
    written = json.loads(out.read_text())
    assert written["gate_id"] == "cfc-0123456789"
    assert written["passed"] is True
    assert written["checks"] == [{"name": "omitted: x", "ok": True, "detail": ""}]


@pytest.mark.parametrize("phase", ["broken", "foreign"])
def test_a_repointed_folder_is_put_back_even_when_its_session_fails(monkeypatch, phase):
    statements: list[str] = []
    monkeypatch.setattr(gate, "sql", lambda query: statements.append(query) or "{}")
    instance = gate.CloudFolderConnectorGate(_args("--gate-id", "cfc-0123456789"))
    instance.projects = {
        "main": "00000000-0000-4000-8000-000000000001",
        "broken": "00000000-0000-4000-8000-000000000002",
    }
    instance.folders = {"broken": "00000000-0000-4000-8000-0000000000f2"}

    def no_session(*args, **kwargs):
        raise gate.GateError("stop")

    instance.start_session = no_session
    with pytest.raises(gate.GateError):
        getattr(instance, phase)()
    changes = [s for s in statements if s.startswith("UPDATE")]
    if phase == "broken":
        assert "'{vendor_meta,mountpoint}'" in changes[0]
        assert "cfc-0123456789-missing" in changes[0]
        assert "'{folder_handle}'" in changes[1]
    else:
        assert "'{project_id}'" in changes[0]
        assert "00000000-0000-4000-8000-000000000001" in changes[0]
    # Every change is undone, the last first.
    assert instance.restore == []
    assert changes[-1].startswith("UPDATE projects") or phase == "foreign"


def test_the_gate_knows_every_closed_exclusion_reason():
    from orchestrator.services.cloud_connector_mounts import EXCLUSION_REASONS

    assert set(EXCLUSION_REASONS) <= gate.EXCLUSION_REASONS


def test_phases_can_be_picked_and_name_real_phases():
    assert _args("--phases", "broken", "foreign").phases == ["broken", "foreign"]
    with pytest.raises(SystemExit):
        _args("--phases", "teardown")
    for name in gate.PHASES:
        assert callable(getattr(gate.CloudFolderConnectorGate, name))
