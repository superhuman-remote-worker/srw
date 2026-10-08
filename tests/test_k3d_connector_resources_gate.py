"""Safety contract and expectations of the local D3a gate (never run here)."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[1] / "scripts" / "k3d-connector-resources-gate.py"
)
_SPEC = importlib.util.spec_from_file_location("k3d_connector_resources_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

OWNER = "00000000-0000-4000-8000-0000000000c1"
PROJECT = "00000000-0000-4000-8000-0000000000b1"
ROW_ID = "1a2b3c4d-5e6f-4000-8000-000000000001"


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


def _runner(*extra):
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION, *extra]
    )
    return gate.ConnectorResourcesGate(args)


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "d1a-0123456789"],
        ["--gate-id", "d3a-xyz"],
        ["--model", "bad model"],
        ["--user", "Robert'); DROP"],
        ["--other-user", "test"],
        ["--turn-timeout", "5"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in (
        "preflight",
        "fixture",
        "backfill",
        "policy_revision",
        "write",
        "native",
        "409",
        "shared",
        "--other-user",
        "cleanup",
    ):
        assert phase in out


def test_embedded_programs_compile_and_cap_their_memory():
    for program in (
        gate._API_PROGRAM,
        gate._BACKFILL_PROGRAM,
        gate._PAYLOAD_PROGRAM,
        gate._HASH_PROGRAM,
    ):
        compile(program, "<gate program>", "exec")
        assert program.startswith(gate._POD_MEMORY_CAP)
        assert "\ncap_memory()\n" in program


def test_the_memory_cap_is_real():
    """Control: under the cap, an allocation past the budget fails."""
    completed = subprocess.run(
        [sys.executable, "-"],
        input=gate._POD_MEMORY_CAP
        + f"cap_memory()\nb = bytes({gate.POD_MEMORY_BUDGET + (128 << 20)})\n",
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode != 0
    assert "MemoryError" in completed.stderr


def test_the_preflight_compares_files_this_checkout_has():
    assert all((gate.ROOT / path).is_file() for path in gate.SERVED)
    migrations = gate.ROOT / "src/orchestrator/database/migrations/app"
    assert all((migrations / name).is_file() for name in gate.MIGRATIONS)


def test_the_expected_details_are_the_products():
    from orchestrator.services import manifest_store

    assert gate.LINKED_CONNECTOR_MESSAGE == manifest_store.LINKED_CONNECTOR_MESSAGE
    assert gate.PLATFORM_MANAGED_MESSAGE == manifest_store.PLATFORM_MANAGED_MESSAGE
    sources = {
        path: (gate.ROOT / path).read_text()
        for path in (
            "src/orchestrator/services/datasources.py",
            "src/orchestrator/services/projects.py",
        )
    }
    text = "".join(sources.values())
    for detail in (
        gate.NATIVE_POLICY_DETAIL,
        gate.NATIVE_DELETE_DETAIL,
        gate.NATIVE_UNLINK_DETAIL,
    ):
        assert detail in text
    from shared.connectors.builtin import MCP_REMOTE_DRIVER

    assert gate.MCP_REMOTE_DRIVER == MCP_REMOTE_DRIVER


def test_every_secret_is_scrubbed():
    runner = _runner()
    printed = gate._scrub(runner.pg_url + " " + runner.owner.password)
    assert runner.pg_password not in printed
    assert runner.other.password not in gate._scrub(runner.other.password)


@pytest.mark.parametrize(
    ("capabilities", "allowed"),
    [
        ({"is_admin": True, "grants": None}, True),
        ({"is_admin": False, "grants": {"public_datasources": True}}, True),
        ({"is_admin": False, "grants": {"public_datasources": False}}, False),
        ({"is_admin": False, "grants": None}, False),
        (None, False),
    ],
)
def test_publishing_needs_an_administrator_or_the_grant(capabilities, allowed):
    assert gate.can_publish_connectors(capabilities) is allowed


# -- the mapping's expectation ---------------------------------------------------


def _row(**over):
    row = {
        "id": ROW_ID,
        "name": "d3a-0123456789 orders",
        "type": "postgresql",
        "created_by": OWNER,
        "read_only": None,
        "job_id": None,
        "managed_key": None,
        "manifest_resource_id": ROW_ID,
        "policy_revision": 1,
        "native": None,
        "native_exists": False,
        "links": [],
        "resource": {
            "kind": "Connector",
            "scope_kind": "Account",
            "scope_name": OWNER,
            "name": "d3a-0123456789-orders-1a2b3c4d5e6f",
            "linked_id": ROW_ID,
            "driver": "srw.postgresql/v1",
            "access": None,
            "transport": None,
            "has_credentials": False,
            "config": {"endpoint": "postgresql://db.internal:5432"},
            "platform_managed": None,
            "version": 1,
            "deleted": False,
        },
    }
    resource = over.pop("resource", {})
    row.update(over)
    row["resource"] = (
        None if resource is None else {**row["resource"], **(resource or {})}
    )
    return row


def test_a_row_that_matches_the_mapping_has_no_problem():
    assert gate.row_problems(_row(), exact_name=True) == []


@pytest.mark.parametrize(
    ("over", "problem"),
    [
        ({"resource": None}, "no live Connector"),
        ({"resource": {"deleted": True}}, "no live Connector"),
        ({"resource": {"scope_name": PROJECT}}, "scope"),
        ({"resource": {"driver": "srw.neo4j/v1"}}, "driver"),
        ({"read_only": True}, "access"),
        ({"resource": {"has_credentials": True}}, "credentials"),
        ({"resource": {"name": "orders"}}, "name"),
        ({"resource": {"linked_id": None}}, "linked"),
        ({"manifest_resource_id": None}, "manifest_resource_id"),
        ({"resource": {"platform_managed": f"project-kb:{PROJECT}"}}, "marker"),
    ],
)
def test_each_deviation_is_named(over, problem):
    problems = gate.row_problems(_row(**over))
    assert problems and problem in problems[0]


def test_an_exact_name_follows_the_row_name():
    renamed = _row(name="d3a-0123456789 orders eu")
    assert gate.row_problems(renamed) == []
    assert "slug" in gate.row_problems(renamed, exact_name=True)[0]


def test_an_ownerless_rows_connector_keeps_its_project():
    linked = _row(
        created_by=None,
        links=[PROJECT],
        resource={"scope_kind": "Project", "scope_name": PROJECT},
    )
    assert gate.row_problems(linked) == []
    for links in ([], [PROJECT, OWNER]):
        # A link change neither moves nor retires an existing Connector...
        kept = _row(
            created_by=None,
            links=links,
            resource={"scope_kind": "Project", "scope_name": PROJECT},
        )
        assert gate.row_problems(kept) == []
        # ...and without one, a row that has no one project stays legacy.
        legacy = _row(created_by=None, links=links, resource=None)
        assert gate.row_problems(legacy) == []
        # An ownerless row never lives in an Account.
        stray = _row(created_by=None, links=links)
        assert "legacy path" in gate.row_problems(stray)[0]


@pytest.mark.parametrize(
    ("config", "problem"),
    [
        ({"connection_url": "postgresql://h/db"}, "connection_url"),
        ({"cli_hint": "psql"}, "cli_hint"),
        ({"endpoint": "https://mcp.example/api/mcp/s/token/mcp"}, "endpoint"),
        ({"endpoint": "postgresql://user@db.internal"}, "endpoint"),
        ({"unknown": 1}, "config"),
    ],
)
def test_a_config_that_leaks_or_breaks_the_schema_is_named(config, problem):
    problems = gate.row_problems(_row(resource={"config": config}))
    assert problems and problem in problems[0]


def test_an_endpoint_may_carry_a_port_or_a_jdbc_prefix():
    for endpoint in ("postgresql://db.internal:5432", "jdbc:postgresql://db:5432"):
        assert gate.config_problems("srw.postgresql/v1", {"endpoint": endpoint}) == []


def test_a_native_kb_lives_in_its_project_with_the_platform_marker():
    key = f"project-kb:{PROJECT}"
    native = _row(
        type="kb",
        read_only=True,
        native=PROJECT,
        native_exists=True,
        managed_key=key,
        links=[PROJECT],
        resource={
            "scope_kind": "Project",
            "scope_name": PROJECT,
            "driver": "srw.kb/v1",
            "access": "ReadOnly",
            "platform_managed": key,
            "config": {"root_path": "knowledge"},
        },
    )
    assert gate.row_problems(native) == []
    assert gate.key_problems([native]) == []
    unstamped = {**native, "managed_key": None}
    assert gate.key_problems([unstamped]) == [
        f"{ROW_ID}: native KB without managed_key"
    ]
    # A second native row of the same project leaves the key to the first.
    assert gate.key_problems([native, unstamped]) == []
    gone = _row(type="kb", native=PROJECT, native_exists=False, resource=None)
    assert gate.row_problems(gone) == []


@pytest.mark.parametrize(
    ("transport", "driver"),
    [("stdio", "srw.mcp/v1"), ("http", "srw.mcp-remote/v1"), ("sse", None)],
)
def test_mcp_rows_name_the_driver_of_their_transport(transport, driver):
    expected = driver or "srw.mcp-remote/v1"
    row = _row(
        type="mcp",
        resource={
            "driver": expected,
            "transport": transport,
            "config": {"transport": transport},
        },
    )
    assert gate.row_problems(row) == []


def test_legacy_job_clones_never_have_a_connector():
    assert gate.row_problems(_row(job_id="j", resource=None)) == []
    assert "job clone" in gate.row_problems(_row(job_id="j"))[0]


# -- the run's envelope ----------------------------------------------------------


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
    assert [name for name, ok, _ in runner.report.results if not ok] == [
        "gate infrastructure"
    ]


def test_cleanup_order_and_scope(monkeypatch):
    runner = _runner()
    runner.thread = "00000000-0000-4000-8000-000000000001"
    runner.project = PROJECT
    runner.connectors = {"legacy": "ds-1", "public": "ds-2"}
    runner.pg_started = True
    order: list[str] = []

    def call(who):
        def record(method, path, body=None):
            order.append(f"{who} {method} {path.split('?')[0]}")
            return (404 if "threads" in path else 200), {}

        return record

    statements: list[str] = []
    monkeypatch.setattr(runner.owner, "call", call("owner"))
    monkeypatch.setattr(runner.other, "call", call("other"))
    monkeypatch.setattr(gate, "sql", lambda query, **k: statements.append(query) or "")
    monkeypatch.setattr(
        gate, "sql_script", lambda script, **k: order.append("drop database") or ""
    )

    assert runner.cleanup() == []
    assert order == [
        f"other DELETE /api/persistent/threads/{runner.thread}",
        "owner DELETE /api/datasources/ds-1",
        "owner DELETE /api/datasources/ds-2",
        f"owner DELETE /api/projects/{PROJECT}",
        "drop database",
    ]
    # Leftover sessions are found by the gate id in their title.
    assert statements[0].startswith("SELECT id FROM threads WHERE position(")
    assert runner.gate_id in statements[0]


def test_residue_is_looked_up_by_the_gate_id(monkeypatch):
    runner = _runner()
    runner.project = PROJECT
    runner.pg_started = True
    statements: list[str] = []
    answers = {"threads WHERE position": "", "count(*)": "0"}

    def sql(query, **_kwargs):
        statements.append(query)
        return next((value for key, value in answers.items() if key in query), "0")

    monkeypatch.setattr(gate, "sql", sql)
    assert runner.residue() == []
    joined = "\n".join(statements)
    assert f"name LIKE '{runner.gate_id} %'" in joined
    assert f"name LIKE '{runner.gate_id}-%'" in joined
    assert "kind = 'Connector' AND deleted_at IS NULL" in joined
    assert f"FROM projects WHERE id = '{PROJECT}'" in joined
    assert f"datname = '{runner.pg_name}'" in joined


def test_the_backfill_fails_only_on_this_runs_rows(monkeypatch):
    runner = _runner()
    runner.project = PROJECT
    runner.connectors = {"legacy": ROW_ID}
    legacy = _row()
    foreign = _row(id="ffffffff-0000-4000-8000-000000000001", resource=None)
    monkeypatch.setattr(gate, "rows", lambda where="": [legacy, foreign])
    monkeypatch.setattr(gate, "sql", lambda query, **k: "[]")
    monkeypatch.setattr(
        gate,
        "in_orchestrator",
        lambda program, payload, **k: {"created": 1, "deferred": 3},
    )
    notes: list[str] = []
    monkeypatch.setattr(runner.report, "note", notes.append)

    runner.backfill()

    assert runner.report.passed, runner.report.results
    assert any("deferred 3 rows" in note for note in notes)
    assert any("1 problems" in note and "no live Connector" in note for note in notes)
