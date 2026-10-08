"""Safety contract and expectations of the local D4 main-cloud gate (never run here)."""

from __future__ import annotations

import importlib
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

from orchestrator.services.cloud import provider_matrix

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "k3d-main-cloud-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_main_cloud_gate", _SCRIPT)
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


def _runner(*extra):
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION, *extra]
    )
    return gate.MainCloudGate(args)


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "d3c-0123456789"],
        ["--gate-id", "d4-xyz"],
        ["--model", "bad model"],
        ["--user", "Robert'); DROP"],
        ["--base-url", "https://dev.example.com"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_only_a_bundled_provider_can_be_expected(no_cluster):
    with pytest.raises(SystemExit):
        gate.main(["--expect-provider", "ms365"])


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in (
        "preflight",
        "account",
        "page",
        "retired",
        "cockpit",
        "fixture",
        "scope",
        "thread_mounts",
        "cleanup",
    ):
        assert phase in out


def test_embedded_programs_compile_and_cap_their_memory():
    for name in (
        "_API_PROGRAM",
        "_LEAK_PROGRAM",
        "_SCOPE_PROGRAM",
        "_KEYCLOAK_PROGRAM",
        "_HASH_PROGRAM",
    ):
        program = getattr(gate, name)
        compile(program, name, "exec")
        assert "\ncap_memory()\n" in program, name


def test_the_preflight_compares_files_this_checkout_has():
    for path in (*gate.SERVED, *gate.AGENT_SERVED):
        assert (ROOT / path).is_file(), path


def test_the_scope_program_imports_what_this_checkout_has():
    for module, names in (
        ("orchestrator.services.thread_mount_rows", ("thread_project_ids",)),
        (
            "orchestrator.services.cloud",
            ("PROTECTED_PROJECT_FOLDER", "provider_offers"),
        ),
        ("orchestrator.services.cloud_staging", ("select_protected_mount",)),
        (
            "orchestrator.services.datasource_policy",
            ("classify_datasource_selection",),
        ),
        ("orchestrator.services.runtime_actor", ("_thread_project_ids",)),
        (
            "orchestrator.services.thread_datasource_authorization",
            (
                "ThreadDatasourceAuthorizationDependencies",
                "resolve_authorized_thread_datasources",
            ),
        ),
    ):
        loaded = importlib.import_module(module)
        for name in names:
            assert hasattr(loaded, name), f"{module}.{name}"
        assert f"from {module} import" in gate._SCOPE_PROGRAM or (
            f"import {module.rsplit('.', 1)[-1]}" in gate._SCOPE_PROGRAM
        )
    assert "ThreadMountDependencies(" in gate._SCOPE_PROGRAM
    fields = set(
        importlib.import_module(
            "orchestrator.services.thread_mount_rows"
        ).ThreadMountDependencies.__dataclass_fields__
    )
    for name in fields:
        assert f"{name}=" in gate._SCOPE_PROGRAM, name


def test_the_expected_table_is_this_checkouts_declaration():
    for active in gate.PROVIDERS:
        assert gate.matrix_problems(provider_matrix(active=active), active=active) == []


def test_a_changed_cell_or_a_wrong_active_provider_fails():
    matrix = provider_matrix(active="nextcloud")
    matrix["rows"][2]["cells"]["opencloud"]["status"] = "offered"
    problems = gate.matrix_problems(matrix, active="nextcloud")
    assert any("opencloud" in p for p in problems)
    assert gate.matrix_problems(provider_matrix(active="nextcloud"), active="opencloud")
    tiered = provider_matrix(active="nextcloud")
    tiered["rows"][2]["cells"]["nextcloud"]["workspace_backends"] = ["sandbox", "vm"]
    assert any(
        "container tier" in p for p in gate.matrix_problems(tiered, active="nextcloud")
    )
    assert gate.matrix_problems(None, active="nextcloud") == ["no matrix"]


def test_offered_reads_one_cell():
    matrix = provider_matrix(active="nextcloud")
    assert gate.offered(matrix, "nextcloud", gate.PROTECTED) is True
    assert gate.offered(matrix, "opencloud", gate.PROTECTED) is False


def test_scope_readings_compare_everything_the_slice_promises():
    reading = {
        "scope": ["p"],
        "actor_scope": ["p"],
        "verdicts": {"a": "eligible"},
        "resolved": ["a"],
        "mounts": 1,
    }
    assert gate.same_scope(reading, {**reading, "mounts": 0}) == []
    for key in ("scope", "actor_scope", "verdicts", "resolved"):
        assert gate.same_scope(reading, {**reading, key: None}), key


def test_the_retired_routes_are_the_removed_api():
    from orchestrator.routers import main_cloud_settings as routes

    assert routes.RETIRED_DETAIL.startswith(gate.RETIRED_PREFIX)
    retired = {(method, path) for method, path, _body in gate.RETIRED}
    declared = {
        (method, route.path)
        for route in routes.router.routes
        for method in route.methods
        if route.endpoint.__name__.endswith("main_cloud_settings")
    }
    assert retired == declared
    assert gate.PAGE in {route.path for route in routes.router.routes}


def test_every_secret_is_scrubbed():
    runner = _runner()
    assert runner.other.password not in gate._scrub(f"x {runner.other.password} y")
    assert runner.owner.password not in gate._scrub(f"{runner.owner.password}")


def test_run_cleans_up_and_checks_residue_after_a_failure(monkeypatch):
    runner = _runner()
    calls = []

    def boom():
        raise gate.GateError("preflight failed")

    monkeypatch.setattr(runner, "preflight", boom)
    monkeypatch.setattr(runner, "cleanup", lambda: calls.append("cleanup") or [])
    monkeypatch.setattr(runner, "residue", lambda: calls.append("residue") or [])

    assert runner.run() == 1
    assert calls == ["cleanup", "residue"]
    names = [name for name, _ok, _detail in runner.report.results]
    assert names == ["gate infrastructure", "cleanup: nothing this run created is left"]


def test_emptied_mount_rows_are_restored_first_in_cleanup(monkeypatch):
    runner = _runner()
    runner.mounts_deleted = True
    order = []
    monkeypatch.setattr(runner, "restore_mounts", lambda: order.append("restore"))
    monkeypatch.setattr(runner, "titled_threads", lambda: [])
    assert runner.cleanup() == []
    assert order == ["restore"]


def test_nothing_this_run_did_not_create_is_cleaned_up(monkeypatch):
    runner = _runner()
    monkeypatch.setattr(runner, "titled_threads", lambda: [])
    monkeypatch.setattr(
        gate, "sql", lambda *_a, **_k: pytest.fail("no database write expected")
    )
    monkeypatch.setattr(
        runner, "keycloak", lambda *_a: pytest.fail("no Keycloak action expected")
    )
    assert runner.cleanup() == []


def test_nothing_observed_never_passes():
    report = gate.Report("d4-0123456789")
    assert report.passed is False
