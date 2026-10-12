"""Safety contract and expectations of the local job outbox / project folder
copy gate (main cloud slices 6.2 and 6.3; never run against a cluster here)."""

from __future__ import annotations

import importlib
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "k3d-cloud-outbox-checkout-gate.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "k3d_cloud_outbox_checkout_gate", _SCRIPT
)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--gate-id", "d4-0123456789"],
        ["--gate-id", "oc-xyz"],
        ["--sweep", "oc-0123"],
        ["--sweep", "oc-0123456789; DROP"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    assert "dry run" in out and gate.LOCAL_CONFIRMATION in out
    assert gate._GATE_ID_RE.fullmatch(out.split()[2].rstrip(";"))


def test_the_runner_compiles_and_caps_its_memory_before_working():
    compile(gate._RUNNER, "<runner>", "exec")
    prologue, _, body = gate._RUNNER.partition("cap_memory()\nrequest =")
    assert "def cap_memory" in prologue and body
    # Nothing reads the request or touches a store before the cap.
    assert "sys.stdin" not in prologue


def test_the_preflight_compares_files_this_checkout_has():
    for rel in gate.SERVED:
        assert (ROOT / rel).is_file(), rel


@pytest.mark.parametrize(
    ("module", "name"),
    [
        (
            "orchestrator.services.cloud.instance_registry",
            "reload_active_main_cloud_instance",
        ),
        ("orchestrator.services.job_cloud_checkout", "admission_stamp"),
        ("orchestrator.services.job_provisioning", "provision_job_repo"),
        ("orchestrator.services.datasource_policy", "default_datasource_selection"),
        (
            "orchestrator.services.cloud_outbox_connectors",
            "ensure_user_outbox_connector",
        ),
        ("orchestrator.services.legacy_job_completion", "complete_job_legacy"),
        ("orchestrator.application.completion", "legacy_completion_dependencies"),
        ("orchestrator.application", "build_application_resources"),
        ("orchestrator.schemas.job_runtime", "JobCompleteRequest"),
    ],
)
def test_the_runner_imports_what_this_checkout_has(module, name):
    assert name in gate._RUNNER
    assert hasattr(importlib.import_module(module), name)


def test_the_brief_is_rendered_by_this_checkouts_agent_code():
    brief = gate.render_brief(
        {
            "cloud_connectors": [
                {
                    "kind": "cloud_folder_checkout",
                    "path": "projects/oc_0123456789/",
                    "folder": "oc-0123456789",
                    "edits": "discarded",
                    "access": "read_only",
                },
                {"kind": "cloud_outbox"},
            ],
            "cloud_baseline": {
                "state": "ready",
                "project_slug": "oc_0123456789",
                "seeded": 2,
                "skipped_binary": 0,
                "failed": 0,
            },
        }
    )
    assert "`projects/oc_0123456789/` in this workspace is a copy" in brief
    assert "2 file(s) were copied." in brief
    assert "SRW never applies the copy's changes" in brief
    assert "## Cloud Outbox" in brief


def test_every_secret_is_scrubbed():
    token = gate.secret("srw_" + "x" * 40)
    assert token not in gate._scrub(f"Bearer {token} failed")


def test_only_idempotent_actions_survive_a_reload(monkeypatch):
    calls = []

    def flaky(args, *, data=None, timeout=240):
        calls.append(args)
        if len(calls) == 1:
            raise gate.GateError("exec failed (exit 1): container not found")
        return '{"ok": true}'

    monkeypatch.setattr(gate, "command", flaky)
    monkeypatch.setattr(gate, "_wait_for_orchestrator", lambda: None)

    assert gate.pod("job_state", job_id="j") == {"ok": True}
    assert len(calls) == 2

    calls.clear()
    with pytest.raises(gate.GateError, match="not idempotent"):
        gate.pod("create_job", gate_id="oc-0123456789")
    assert len(calls) == 1

    calls.clear()
    with pytest.raises(gate.GateError, match="not idempotent"):
        gate.pod("api", method="POST", path="/api/jobs/j/accept", token="t")
    calls.clear()
    assert gate.pod("api", method="GET", path="/api/jobs/j", token="t") == {"ok": True}


def test_a_failed_check_still_cleans_up_and_checks_residue(monkeypatch, capsys):
    order = []
    monkeypatch.setattr(gate, "command", lambda *a, **k: "k3d-srw")
    monkeypatch.setattr(gate, "served_matches", lambda: [])

    def boom(fx, results):
        order.append("setup")
        raise gate.GateError("the project never got its folder")

    monkeypatch.setattr(gate, "setup", boom)
    monkeypatch.setattr(gate, "cleanup", lambda fx: order.append("cleanup"))

    def residue(fx, results):
        order.append("residue")
        results.check("cleanup: nothing this run created is left", True)

    monkeypatch.setattr(gate, "residue", residue)

    assert gate.main(["--run", "--confirm", gate.LOCAL_CONFIRMATION]) == 1
    assert order == ["setup", "cleanup", "residue"]
    assert "FAIL  gate: the project never got its folder" in capsys.readouterr().out


def test_a_sweep_only_cleans_up_and_checks(monkeypatch):
    order = []
    monkeypatch.setattr(gate, "command", lambda *a, **k: "k3d-srw")
    monkeypatch.setattr(gate, "served_matches", lambda: pytest.fail("no preflight"))
    monkeypatch.setattr(gate, "setup", lambda *a: pytest.fail("no fixtures"))
    monkeypatch.setattr(
        gate, "cleanup", lambda fx: order.append(("cleanup", fx.gate_id))
    )
    monkeypatch.setattr(
        gate, "residue", lambda fx, results: order.append(("residue", fx.gate_id))
    )

    assert (
        gate.main(
            ["--sweep", "oc-0123456789", "--run", "--confirm", gate.LOCAL_CONFIRMATION]
        )
        == 0
    )
    assert order == [("cleanup", "oc-0123456789"), ("residue", "oc-0123456789")]


def test_nothing_observed_never_passes():
    results = gate.Results()
    assert not results.check("x", False, "nothing")
    assert results.failed == ["x"] and results.passed == []
