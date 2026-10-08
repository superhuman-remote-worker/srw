"""Safety contract and expectations of the local D3b gate (never run here)."""

from __future__ import annotations

import ast
import importlib
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[1] / "scripts" / "k3d-connector-secrets-gate.py"
)
_SPEC = importlib.util.spec_from_file_location("k3d_connector_secrets_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

OWNER = "00000000-0000-4000-8000-0000000000c1"
PROJECT = "00000000-0000-4000-8000-0000000000b1"
ROW_ID = "1a2b3c4d-5e6f-4000-8000-000000000001"
NAME = "connector-1a2b3c4d5e6f40008000000000000001"
OWN = ["Account", OWNER]


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


def _runner(*extra):
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION, *extra]
    )
    return gate.ConnectorSecretsGate(args)


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "d3a-0123456789"],
        ["--gate-id", "d3b-xyz"],
        ["--model", "bad model"],
        ["--user", "Robert'); DROP"],
        ["--other-user", "test"],
        ["--stranger-user", "dev-user-1"],
        ["--stranger-user", "test"],
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
        "write",
        "merges",
        "replaces",
        "shared",
        "refused",
        "--stranger-user",
        "leaks",
        "cleanup",
    ):
        assert phase in out


PROGRAMS = (
    "_API_PROGRAM",
    "_BACKFILL_PROGRAM",
    "_SECRETS_PROGRAM",
    "_MARK_PROGRAM",
    "_DELIVERY_PROGRAM",
    "_HASH_PROGRAM",
)


@pytest.mark.parametrize("name", PROGRAMS)
def test_embedded_programs_compile_and_cap_their_memory(name):
    program = getattr(gate, name)
    compile(program, "<gate program>", "exec")
    assert program.startswith(gate._POD_MEMORY_CAP)
    assert "\ncap_memory()\n" in program


@pytest.mark.parametrize("name", PROGRAMS)
def test_embedded_programs_import_only_what_this_checkout_has(name):
    """A renamed helper fails here, not in the pod halfway through a run."""
    for node in ast.walk(ast.parse(getattr(gate, name))):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module
            and (node.module.split(".")[0] in ("orchestrator", "shared"))
        ):
            module = importlib.import_module(node.module)
            for alias in node.names:
                assert hasattr(module, alias.name), f"{node.module}.{alias.name}"


@pytest.mark.parametrize("name", PROGRAMS[1:5])
def test_programs_that_read_secrets_never_print_a_value(name):
    """They print names, scopes, versions and booleans only."""
    program = getattr(gate, name)
    printed = [
        node
        for node in ast.walk(ast.parse(program))
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "print"
    ]
    assert printed
    for call in printed:
        text = ast.unparse(call)
        assert "values" not in text and "ciphertext" not in text, text


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


def test_served_files_exist():
    for path in gate.SERVED:
        assert (gate.ROOT / path).is_file(), path


def test_the_gate_names_secrets_as_the_product_does():
    from orchestrator.services.connector_secrets import (
        CONNECTOR_SECRET_DETAIL,
        SHAPE_KEY,
        URL_KEY,
        connector_secret_name,
    )

    assert gate.secret_name(ROW_ID) == connector_secret_name(ROW_ID) == NAME
    assert (gate.URL_KEY, gate.SHAPE_KEY) == (URL_KEY, SHAPE_KEY)
    assert gate.CONNECTOR_SECRET_DETAIL == CONNECTOR_SECRET_DETAIL


# =============================================================================
# What the gate holds a secret to
# =============================================================================


def _entry(**over):
    entry = {
        "row": True,
        "resource": {
            "scope": list(OWN),
            "deleted": False,
            "refs": ["shape", "token", "url"],
            "refs_own": True,
        },
        "secrets": [
            {
                "scope": list(OWN),
                "keys": ["shape", "token", "url"],
                "version": 1,
                "owner": OWNER,
            }
        ],
        "derived_keys": ["shape", "token", "url"],
        "rebuilds_row": True,
        "matches_mapping": True,
    }
    resource = over.pop("resource", {})
    entry.update(over)
    if resource is None:
        entry["resource"] = None
    else:
        entry["resource"].update(resource)
    return entry


KEYS = {"shape", "token", "url"}


def test_a_secret_that_matches_d3b_has_no_problem():
    assert gate.secret_problems(_entry(), KEYS, scope=tuple(OWN)) == []
    # Keys the gate did not state: the deployed mapping's.
    assert gate.secret_problems(_entry(), None) == []


def test_a_connector_with_nothing_secret_has_no_secret():
    empty = _entry(
        resource={"refs": []}, secrets=[], derived_keys=[], rebuilds_row=None
    )
    assert gate.secret_problems(empty, set()) == []
    assert gate.secret_problems(empty, None) == []
    stray = _entry(resource={"refs": []}, derived_keys=[])
    assert "has a secret" in "; ".join(gate.secret_problems(stray, set()))


@pytest.mark.parametrize(
    ("entry", "problem"),
    [
        (None, "no row"),
        (_entry(row=False), "no row"),
        (_entry(resource=None), "no live Connector"),
        (_entry(resource={"deleted": True}), "no live Connector"),
        (_entry(resource={"scope": ["Project", PROJECT]}), "Connector in"),
        (_entry(resource={"refs": None}), "before D3b"),
        (_entry(resource={"refs_own": False}), "own secret"),
        (_entry(derived_keys=["url"]), "deployed mapping"),
        (
            _entry(
                secrets=[
                    _entry()["secrets"][0],
                    {**_entry()["secrets"][0], "scope": ["Project", PROJECT]},
                ]
            ),
            "outside the Connector's scope",
        ),
        (_entry(secrets=[]), "0 secrets"),
        (
            _entry(secrets=[{**_entry()["secrets"][0], "keys": ["url"]}]),
            "secret keys",
        ),
        (_entry(resource={"refs": ["url"]}), "names"),
        (_entry(rebuilds_row=False), "rebuild"),
        (_entry(matches_mapping=False), "mapping derives"),
    ],
)
def test_each_deviation_is_named(entry, problem):
    problems = gate.secret_problems(entry, KEYS, scope=tuple(OWN))
    assert problems and problem in "; ".join(problems)


def test_the_version_is_the_one_in_the_connectors_scope():
    entry = _entry(
        secrets=[
            {**_entry()["secrets"][0], "scope": ["Project", PROJECT], "version": 7},
            {**_entry()["secrets"][0], "version": 3},
        ]
    )
    assert gate.secret_version(entry) == 3
    assert gate.secret_version(_entry(secrets=[])) is None
    assert gate.secret_version(None) is None


def test_leaks_are_counted_and_scrubbed():
    marker = "d3b-test-" + "f" * 16
    gate.secret(marker)
    try:
        assert gate.leak_count(f"a {marker} b {marker}") == 2
        assert gate.leak_count("nothing here") == 0
        assert marker not in gate._scrub(f"x {marker}")
    finally:
        gate._SECRETS.remove(marker)


def test_api_calls_count_this_runs_secrets_in_the_raw_answer(monkeypatch):
    runner = _runner()
    sent = {}

    def in_orchestrator(program, payload, **_kwargs):
        sent.update(payload)
        return {"status": 200, "body": "{}", "leaks": 2}

    monkeypatch.setattr(gate, "in_orchestrator", in_orchestrator)
    status, body, leaks = runner.owner.call_counting("GET", "/api/datasources")
    assert (status, body, leaks) == (200, {}, 2)
    assert runner.pg_password in sent["needles"]
    assert set(runner.env_values.values()) <= set(sent["needles"])


# =============================================================================
# Run, cleanup and residue
# =============================================================================


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


def test_nothing_observed_never_passes():
    assert not gate.Report("d3b-0123456789").passed


def test_cleanup_order_and_scope(monkeypatch):
    runner = _runner()
    runner.threads = ["00000000-0000-4000-8000-000000000001"]
    runner.project = PROJECT
    runner.connectors = {"legacy": "ds-1", "public": "ds-2"}
    runner.pg_started = True
    runner.other_id = "other-id"
    order: list[str] = []

    def call(who):
        def record(method, path, body=None):
            order.append(f"{who} {method} {path.split('?')[0]}")
            return (404 if "threads" in path else 200), {}

        return record

    statements: list[str] = []

    def sql(query, **_kwargs):
        statements.append(query)
        return "other-id" if query.startswith("SELECT user_id") else ""

    monkeypatch.setattr(runner.owner, "call", call("owner"))
    monkeypatch.setattr(runner.other, "call", call("other"))
    monkeypatch.setattr(runner.stranger, "call", call("stranger"))
    monkeypatch.setattr(gate, "sql", sql)
    monkeypatch.setattr(
        gate, "sql_script", lambda script, **k: order.append("drop database") or ""
    )

    assert runner.cleanup() == []
    assert order == [
        f"other DELETE /api/persistent/threads/{runner.threads[0]}",
        "owner DELETE /api/datasources/ds-1",
        "owner DELETE /api/datasources/ds-2",
        f"owner DELETE /api/projects/{PROJECT}",
        "drop database",
    ]
    # Leftover sessions are found by the gate id in their title.
    assert any(
        runner.gate_id in query and query.startswith("SELECT id FROM threads")
        for query in statements
    )


def test_residue_is_looked_up_by_the_gate_id_and_the_secret_names(monkeypatch):
    runner = _runner()
    runner.project = PROJECT
    runner.pg_started = True
    runner.connectors = {"legacy": ROW_ID}
    runner.deleted = {"orders": "ffffffff-0000-4000-8000-000000000002"}
    statements: list[str] = []

    def sql(query, **_kwargs):
        statements.append(query)
        return "" if "threads WHERE position" in query else "0"

    monkeypatch.setattr(gate, "sql", sql)
    assert runner.residue() == []
    joined = "\n".join(statements)
    assert f"name LIKE '{runner.gate_id} %'" in joined
    assert f"name LIKE '{runner.gate_id}-%'" in joined
    assert "kind = 'Connector' AND deleted_at IS NULL" in joined
    assert f"'{NAME}'" in joined
    assert "'connector-ffffffff000040008000000000000002'" in joined
    assert f"FROM projects WHERE id = '{PROJECT}'" in joined
    assert f"datname = '{runner.pg_name}'" in joined


def test_a_secret_left_behind_is_residue(monkeypatch):
    runner = _runner()
    runner.connectors = {"legacy": ROW_ID}

    def sql(query, **_kwargs):
        if "threads WHERE position" in query:
            return ""
        return "1" if "srw_resource_secrets" in query else "0"

    monkeypatch.setattr(gate, "sql", sql)
    assert runner.residue() == ["1 connector secrets"]
