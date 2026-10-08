"""Safety contract for the local D1a connector drivers gate (never run here)."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[1] / "scripts" / "k3d-connector-drivers-gate.py"
)
_SPEC = importlib.util.spec_from_file_location("k3d_connector_drivers_gate", _SCRIPT)
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
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "c1-0123456789"],
        ["--gate-id", "d1a-xyz"],
        ["--model", "bad model"],
        ["--user", "Robert'); DROP"],
        ["--turn-timeout", "5"],
        ["--job-timeout", "7200"],
        ["--kb-timeout", "5"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in (
        "preflight",
        "lifecycle",
        "unsupported",
        "refusals",
        "MCP",
        "sql_execute",
        "webdav_list",
        "cockpit",
        "Neo4j",
        "cleanup",
        "live (D1b)",
        "pinned",
    ):
        assert phase in out


def test_embedded_programs_compile():
    for program in (
        gate._API_PROGRAM,
        gate._GITEA_PROGRAM,
        gate._DAV_PROGRAM,
        gate._PAYLOAD_PROGRAM,
        gate._HASH_PROGRAM,
        gate._LIVE_UPDATE_PROGRAM,
    ):
        compile(program, "<gate program>", "exec")


# -- the served-bytes preflight ------------------------------------------------


def test_every_served_set_compares_the_whole_connector_package():
    root = gate.ROOT
    shared = {
        str(path.relative_to(root))
        for path in (root / gate.SHARED_CONNECTORS).rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    }
    drivers = {
        str(path.relative_to(root))
        for path in (root / gate.DRIVERS).rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    }
    assert "src/shared/connectors/binding.schema.json" in shared
    assert "src/orchestrator/services/connector_drivers/registry.py" in drivers
    by_label = {served.label: served for served in gate.SERVED_SETS}
    assert set(by_label) == {"orchestrator", "stateless agent", "mcp"}
    for served in gate.SERVED_SETS:
        expected = gate.expected_bytes(served)
        assert shared <= set(expected)
        assert not any("__pycache__" in path for path in expected)
        for path in expected:
            assert (root / path).is_file(), path
    assert drivers <= set(gate.expected_bytes(by_label["orchestrator"]))
    for label in ("orchestrator", "stateless agent"):
        assert (gate.NEO4J_DB, "READ_ACCESS") in by_label[label].contains
    # D1b: both agent lanes serve the materializers and their entry points.
    materializers = {
        str(path.relative_to(root))
        for path in (root / gate.AGENT_CONNECTORS).rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    }
    assert "src/agent/connectors/registry.py" in materializers
    for served in (by_label["stateless agent"], gate.PINNED_AGENT):
        expected = set(gate.expected_bytes(served))
        assert materializers <= expected
        assert {
            "src/agent/agent.py",
            "src/agent/api/session_attach.py",
            "src/agent/api/persistent_session.py",
        } <= expected


def _hash(root: Path, request: dict) -> dict:
    result = subprocess.run(
        [sys.executable, "-c", gate._HASH_PROGRAM, str(root)],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_the_hash_program_reports_stale_extra_and_missing_text(tmp_path):
    package = tmp_path / "src" / "pkg"
    package.mkdir(parents=True)
    (package / "a.py").write_text("A = 1\n")
    (package / "b.py").write_text("B = 1\n")
    (package / "legacy.py").write_text("OLD = 1\n")
    (package / "__pycache__").mkdir()
    (package / "__pycache__" / "a.cpython-313.pyc").write_bytes(b"\0")
    digest = hashlib.sha256(b"A = 1\n").hexdigest()

    clean = _hash(
        tmp_path,
        {
            "files": {
                "src/pkg/a.py": digest,
                "src/pkg/b.py": hashlib.sha256(b"B = 1\n").hexdigest(),
            },
            "dirs": [],
            "contains": [["src/pkg/a.py", "A = 1"]],
        },
    )
    assert clean == {"stale": [], "extra": [], "missing_text": []}

    found = _hash(
        tmp_path,
        {
            "files": {
                "src/pkg/a.py": digest,
                "src/pkg/b.py": "0" * 64,
                "src/pkg/gone.py": digest,
            },
            "dirs": ["src/pkg"],
            "contains": [["src/pkg/a.py", "READ_ACCESS"]],
        },
    )
    assert found["stale"] == ["src/pkg/b.py", "src/pkg/gone.py"]
    assert found["extra"] == ["src/pkg/legacy.py"]
    assert found["missing_text"] == ["src/pkg/a.py:READ_ACCESS"]


# -- runner behaviour without a cluster -----------------------------------------


def _runner(*extra):
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION, *extra]
    )
    runner = gate.ConnectorDriversGate(args)
    runner.gitea = {
        "owner": "srw",
        "url": "http://srw-gitea:3000",
        "ssh_host": "srw-gitea-ssh",
        "ssh_port": 2222,
        "token": gate.secret("gitea-token-0123456789"),
    }
    return runner


def test_type_cases_expect_the_deliberate_unsupported_answers():
    cases = {case.label: case for case in _runner().type_cases()}
    assert set(cases) == {
        "postgresql",
        "webdav",
        "repository-token",
        "repository-ssh",
        "kb",
        "generic",
        "credentials",
        "kubeconfig",
        "generic_file",
        "ssh_key",
    }
    for label in ("kubeconfig", "generic_file", "ssh_key"):
        assert cases[label].expect_status == "unsupported"
        assert cases[label].expect_text == (gate.UNSUPPORTED[label],)
    assert "config" not in cases["ssh_key"].body  # host-less
    assert cases["webdav"].expect_status is None  # reported, not gated
    for label in (
        "postgresql",
        "repository-token",
        "repository-ssh",
        "kb",
        "generic",
        "credentials",
    ):
        assert cases[label].expect_status == "ok"


def test_every_secret_is_scrubbed():
    runner = _runner()
    cases = runner.type_cases()
    printed = gate._scrub(json.dumps([case.body for case in cases]) + runner.pg_url)
    for value in (
        runner.pg_password,
        runner.nc_password,
        runner.gitea["token"],
        runner.env["generic"][1],
        runner.env["credentials"][1],
    ):
        assert value not in printed
    by_label = {case.label: case.body for case in cases}
    keys = [
        by_label["repository-ssh"]["credentials"]["ssh_key"],
        by_label["ssh_key"]["credentials"]["files"][0]["contents"],
    ]
    for key in keys:
        for line in key.splitlines()[1:-1]:
            assert line not in printed


def test_a_refused_connector_answered_201_is_still_cleaned_up(monkeypatch):
    runner = _runner()
    created = iter(range(10))

    def call(method, path, body=None):
        if method == "POST":
            return 201, {"id": f"stray-{next(created)}"}
        return 200, {}

    monkeypatch.setattr(runner.api, "call", call)
    runner.refusals()

    assert not runner.report.passed
    assert sorted(runner.connectors.values()) == ["stray-0", "stray-1"]
    deleted: list[str] = []
    monkeypatch.setattr(
        runner.api,
        "call",
        lambda method, path, body=None: deleted.append(path) or (200, {}),
    )
    runner.cleanup()
    assert sorted(deleted) == ["/api/datasources/stray-0", "/api/datasources/stray-1"]


def test_cleanup_order_and_scope(monkeypatch):
    runner = _runner()
    runner.thread = "00000000-0000-4000-8000-000000000001"
    runner.job = "00000000-0000-4000-8000-0000000000aa"
    runner.project = "00000000-0000-4000-8000-0000000000bb"
    runner.connectors = {"attach-pg": "ds-1", "mcp-row": "ds-2"}
    runner.sql_rows = {"mcp-row": runner.name("mcp-row")}
    runner.repos = {runner.repo: "m1", runner.kb_repo: "m2"}
    runner.nc_started = runner.pg_started = True
    order: list[str] = []

    def call(method, path, body=None):
        order.append(f"{method} {path.split('?')[0]}")
        if path == "/api/datasources/ds-2":
            return 403, {}
        return 404 if "threads" in path else 200, {}

    statements: list[str] = []
    monkeypatch.setattr(runner.api, "call", call)
    monkeypatch.setattr(gate, "sql", lambda query, **k: statements.append(query) or "0")
    monkeypatch.setattr(
        gate, "sql_script", lambda script, **k: order.append("drop database") or ""
    )
    monkeypatch.setattr(
        gate,
        "in_orchestrator",
        lambda program, payload, **k: order.append(f"gitea {payload['action']}") or {},
    )
    monkeypatch.setattr(
        runner, "nextcloud_occ", lambda arguments, **k: order.append(arguments) or 0
    )

    assert runner.cleanup() == []
    assert order == [
        f"DELETE /api/persistent/threads/{runner.thread}",
        f"PUT /api/jobs/{runner.job}/cancel",
        f"DELETE /api/jobs/{runner.job}",
        "DELETE /api/datasources/ds-1",
        "DELETE /api/datasources/ds-2",
        f"DELETE /api/projects/{runner.project}",
        "gitea cleanup",
        f"user:delete {runner.nc_user}",
        "drop database",
    ]
    # The SQL fallback deletes only the row this run wrote, by id and name.
    assert statements == [
        f"DELETE FROM datasources WHERE id = 'ds-2' AND name = '{runner.name('mcp-row')}'"
    ]


def _payload(monkeypatch, runner, result):
    monkeypatch.setattr(gate, "in_orchestrator", lambda program, payload, **k: result)
    runner.job = "00000000-0000-4000-8000-0000000000aa"
    runner.project = "00000000-0000-4000-8000-0000000000bb"
    runner.payload_checks()


def test_payload_checks_pass_only_when_the_mcp_row_is_dropped(monkeypatch):
    runner = _runner()
    mcp = f"mcp:{runner.name('mcp-row')}"
    pg = f"postgresql:{runner.name('attach-pg')}"
    good = {
        "mcp_gate": False,
        "resolved": [mcp, pg],
        "read_only": [runner.name("attach-pg")],
        "payload": [pg],
        "tools": {"sql": ["sql_query", "sql_schema"]},
    }
    _payload(monkeypatch, runner, good)
    assert runner.report.passed

    for bad in (
        {**good, "payload": [mcp, pg]},
        {**good, "mcp_gate": True},
        {**good, "tools": {"sql": ["sql_query", "sql_schema", "sql_execute"]}},
        {**good, "tools": {**good["tools"], "mcp": ["*"]}},
        {**good, "read_only": []},
    ):
        runner = _runner()
        _payload(monkeypatch, runner, bad)
        assert not runner.report.passed, bad


def test_nothing_observed_never_passes():
    runner = _runner()
    assert not runner.report.passed
    runner.report.note("a note is not a check")
    assert not runner.report.passed


def test_run_cleans_up_and_checks_residue_after_a_failure(monkeypatch):
    runner = _runner()
    order: list[str] = []

    def fail():
        order.append("preflight")
        raise gate.GateError("cluster unreachable")

    monkeypatch.setattr(runner, "preflight", fail)
    monkeypatch.setattr(runner, "cleanup", lambda: order.append("cleanup") or [])
    monkeypatch.setattr(runner, "residue", lambda: order.append("residue") or [])

    assert runner.run() == 1
    assert order == ["preflight", "cleanup", "residue"]
    names = [name for name, ok, _ in runner.report.results if not ok]
    assert names == ["gate infrastructure"]


# -- the session checks ----------------------------------------------------------
# The 2026-10-08 run counted 'assistant' rows (thread_messages stores the
# model's turns as 'ai') and read a session's tools from audit rows that carry
# no tool list.


def test_session_tool_use_reads_the_ai_role_and_the_exact_tool_name(monkeypatch):
    runner = _runner()
    runner.thread = "00000000-0000-4000-8000-000000000001"
    queries: list[str] = []
    monkeypatch.setattr(gate, "sql", lambda query, **k: queries.append(query) or "2 1")

    assert runner.tool_use("webdav_list", runner.dav_file) == (2, 1)
    (query,) = queries
    assert "role IN ('ai', 'assistant')" in query
    assert "'\"webdav_list\"'" in query


def _session(monkeypatch, tools, probe):
    runner = _runner()
    runner.thread = "00000000-0000-4000-8000-000000000001"
    runner.project = "00000000-0000-4000-8000-0000000000bb"
    runner.connectors = {"attach-webdav": "ds-dav"}
    dav = runner.name("attach-webdav")
    requests: list[dict] = []

    def program(source, payload, **k):
        requests.append(payload)
        if source is gate._PAYLOAD_PROGRAM:
            return {"read_only": [dav], "tools": {"webdav": tools}}
        return {"status": probe}

    monkeypatch.setattr(gate, "in_orchestrator", program)
    monkeypatch.setattr(
        runner,
        "agent_log_lines",
        lambda needles: [f"INFO Connected to webdav datasource: {dav} (read-only)"],
    )
    monkeypatch.setattr(runner, "tool_use", lambda tool, text: (1, 1))
    turns: list[int] = []
    monkeypatch.setattr(runner, "turn", lambda text, step: turns.append(step))
    runner.session_checks()
    return runner, requests, turns


def test_session_checks_read_the_session_selection_and_probe_the_write(
    monkeypatch,
):
    runner, requests, turns = _session(
        monkeypatch, ["webdav_list", "webdav_read", "webdav_info"], 404
    )

    assert runner.report.passed
    assert turns == [2]
    assert requests[0] == {"datasource_ids": ["ds-dav"], "project": runner.project}
    assert requests[1]["method"] == "GET"
    assert requests[1]["url"].endswith(runner.dav_probe)


@pytest.mark.parametrize(
    ("tools", "probe"),
    [(["webdav_list", "webdav_write"], 404), (["webdav_list"], 200)],
)
def test_write_tools_or_a_written_probe_fail_the_session(monkeypatch, tools, probe):
    runner, _requests, _turns = _session(monkeypatch, tools, probe)
    assert not runner.report.passed


# -- the D1b live attach and detach on a pinned session ----------------------------


def _live(monkeypatch, **fault):
    runner = _runner()
    observed = {**_good_live(runner.name), **fault}
    readme_attached, readme_detached = (
        observed["readme_attached"],
        observed["readme_detached"],
    )
    held, fetches, logs = observed["held"], observed["fetches"], observed["logs"]
    runner.live_thread = "00000000-0000-4000-8000-0000000000cc"
    runner.live_fingerprint = "SHA256:" + "k" * 43
    phase = {"now": "attach"}

    def once(label, probe, *, timeout, interval=3.0):
        value = probe()
        if not value:
            raise gate.GateError(f"timed out: {label}")
        return value

    monkeypatch.setattr(gate, "wait_for", once)
    monkeypatch.setattr(runner, "live_workspace", lambda: "ws-pod")
    monkeypatch.setattr(
        runner,
        "workspace_grep",
        lambda pod, needle, path: [f"{gate.HOME}/.srw-credentials/env.sh"],
    )
    monkeypatch.setattr(runner, "held_fingerprints", lambda pod: held[phase["now"]])
    monkeypatch.setattr(runner, "live_fetches", lambda pod: fetches[phase["now"]])
    monkeypatch.setattr(
        runner,
        "live_readme",
        lambda pod: readme_attached if phase["now"] == "attach" else readme_detached,
    )
    monkeypatch.setattr(
        runner,
        "pinned_log_lines",
        lambda needles: [
            line for line in logs if any(needle in line for needle in needles)
        ],
    )
    names = sorted(runner.name(label) for label in gate.LIVE_LABELS)
    runner.live_attached({"outcome": "config.changed", "datasources": {"added": names}})
    phase["now"] = "detach"
    runner.live_detached(
        {"outcome": "config.changed", "datasources": {"removed": names}}
    )
    return runner


def _good_live(runner_name):
    pg = runner_name("attach-pg")
    return {
        "readme_attached": "\n".join(
            f"- **{runner_name(label)}**" for label in gate.LIVE_LABELS
        ),
        "readme_detached": "_No connectors attached._",
        "held": {"attach": {"SHA256:" + "k" * 43}, "detach": set()},
        "fetches": {"attach": True, "detach": False},
        "logs": [
            f"INFO Connected to postgresql datasource: {pg} (read-only)",
            "INFO Datasources re-set up live: 3 attached (3 added, 0 removed), "
            "1 connections",
            "INFO Datasources re-set up live: 0 attached (0 added, 3 removed), "
            "0 connections",
            "INFO Closed 1 replaced datasource connection(s) after turn end",
        ],
    }


def test_live_checks_pass_when_attach_and_detach_both_land(monkeypatch):
    runner = _live(monkeypatch)
    assert runner.report.passed, runner.report.results
    names = [name for name, _ok, _detail in runner.report.results]
    assert sum(name.startswith("live attach") for name in names) == 7
    assert sum(name.startswith("live detach") for name in names) == 6


@pytest.mark.parametrize(
    "fault",
    [
        {"held": {"attach": set(), "detach": set()}},
        {"held": {"attach": {"SHA256:" + "k" * 43}, "detach": {"SHA256:" + "k" * 43}}},
        {"fetches": {"attach": False, "detach": False}},
        {"fetches": {"attach": True, "detach": True}},
        {"readme_detached": "- **still listed**"},
        {"logs": []},
    ],
)
def test_a_live_step_that_did_not_land_fails(monkeypatch, fault):
    runner = _live(monkeypatch, **fault)
    assert not runner.report.passed


def test_a_live_update_error_fails_the_ack_check(monkeypatch):
    runner = _runner()
    runner.live_thread = "00000000-0000-4000-8000-0000000000cc"
    monkeypatch.setattr(runner, "pinned_log_lines", lambda needles: [])
    monkeypatch.setattr(runner, "live_workspace", lambda: "ws-pod")
    monkeypatch.setattr(runner, "workspace_grep", lambda *a: [])
    monkeypatch.setattr(runner, "held_fingerprints", lambda pod: set())
    monkeypatch.setattr(runner, "live_fetches", lambda pod: False)
    monkeypatch.setattr(runner, "live_readme", lambda pod: "")
    runner.live_attached({"outcome": "error", "message": "rejected"})
    failed = [name for name, ok, _ in runner.report.results if not ok]
    assert "live attach: config.changed lists the three connectors added" in failed


def test_skip_live_runs_the_d1a_gate_alone(monkeypatch):
    for argv, expected in (([], True), (["--skip-live"], False)):
        runner = _runner(*argv)
        ran: list[str] = []
        for phase in (
            "preflight",
            "fixture",
            "lifecycle",
            "refusals",
            "attach_setup",
            "job_run",
            "session",
            "session_checks",
            "job_settle",
            "job_agent_checks",
            "cockpit",
            "notes",
        ):
            monkeypatch.setattr(runner, phase, lambda: None)
        monkeypatch.setattr(runner, "end_session", lambda: True)
        monkeypatch.setattr(runner, "live", lambda: ran.append("live"))
        monkeypatch.setattr(runner, "cleanup", lambda: [])
        monkeypatch.setattr(runner, "residue", lambda: [])
        runner.run()
        assert (ran == ["live"]) is expected


def test_cleanup_deletes_the_live_session_after_the_first(monkeypatch):
    runner = _runner()
    runner.thread = "00000000-0000-4000-8000-000000000001"
    runner.live_thread = "00000000-0000-4000-8000-0000000000cc"
    order: list[str] = []

    def call(method, path, body=None):
        order.append(f"{method} {path.split('?')[0]}")
        return 404, {}

    monkeypatch.setattr(runner.api, "call", call)
    assert runner.cleanup() == []
    assert order == [
        f"DELETE /api/persistent/threads/{runner.thread}",
        f"DELETE /api/persistent/threads/{runner.live_thread}",
    ]
