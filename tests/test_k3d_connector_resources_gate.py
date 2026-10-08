"""Safety contract and expectations of the local D3a gate (never run here)."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

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
OTHER = "00000000-0000-4000-8000-0000000000c2"
KEYCLOAK_ID = "00000000-0000-4000-8000-0000000000e1"
CLIENT_ID = "00000000-0000-4000-8000-0000000000e2"
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
        ["--other-user", "test", "--other-password", "x" * 16],
        ["--other-user", "dev-user-1"],
        ["--other-password", "x" * 16],
        ["--other-user", "Robert'); DROP", "--other-password", "x" * 16],
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
        gate._KEYCLOAK_PROGRAM,
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
    # The second account's session asks for no more than an ungranted user's
    # ceiling, so the gate needs no permission_mode grant for it.
    from shared.runtime.core.capability_grants import CATALOG

    assert gate.SESSION_PERMISSION_MODE == CATALOG["permission_mode"]["default"]


def test_every_secret_is_scrubbed():
    runner = _runner()
    printed = gate._scrub(runner.pg_url + " " + runner.owner.password)
    assert runner.pg_password not in printed
    assert runner.other.password not in gate._scrub(runner.other.password)


def test_the_second_account_is_disposable_unless_named():
    runner = _runner()
    assert runner.disposable
    assert runner.other.username == runner.gate_id
    assert gate._USER_RE.fullmatch(runner.other.username)
    assert runner.other_email == f"{runner.gate_id}@example.invalid"
    # The realm's password policy: length(16) and notUsername.
    assert len(runner.other.password) >= 16
    assert runner.other.password != runner.other.username
    assert runner.other.password != _runner().other.password

    named = _runner("--other-user", "dev-user-1", "--other-password", "p" * 16)
    assert not named.disposable
    assert (named.other.username, named.other.password) == ("dev-user-1", "p" * 16)


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
    runner.client_started = runner.account_started = runner.account_row = True
    runner.client_uuid, runner.account_keycloak_id = CLIENT_ID, KEYCLOAK_ID
    runner.other_id = OTHER
    order: list[str] = []

    def call(who):
        def record(method, path, body=None):
            order.append(f"{who} {method} {path.split('?')[0]}")
            return (404 if "threads" in path else 200), {}

        return record

    def in_orchestrator(program, payload, **_kwargs):
        assert program is gate._KEYCLOAK_PROGRAM
        order.append(f"keycloak {payload['action']}")
        assert payload == {
            "action": "delete",
            "client": f"{runner.gate_id}-oauth",
            "marker": runner.gate_id,
            "client_started": True,
            "client_uuid": CLIENT_ID,
            "username": runner.gate_id,
            "email": runner.other_email,
            "user_started": True,
            "user_id": KEYCLOAK_ID,
        }
        return {
            "deleted": [KEYCLOAK_ID, CLIENT_ID],
            "refused": [],
            "users": 0,
            "clients": 0,
        }

    statements: list[str] = []
    monkeypatch.setattr(runner.owner, "call", call("owner"))
    monkeypatch.setattr(runner.other, "call", call("other"))
    monkeypatch.setattr(gate, "sql", lambda query, **k: statements.append(query) or "")
    monkeypatch.setattr(gate, "in_orchestrator", in_orchestrator)
    monkeypatch.setattr(
        gate, "sql_script", lambda script, **k: order.append("drop database") or ""
    )

    assert runner.cleanup() == []
    assert order == [
        f"other DELETE /api/persistent/threads/{runner.thread}",
        "owner DELETE /api/datasources/ds-1",
        "owner DELETE /api/datasources/ds-2",
        f"owner DELETE /api/projects/{PROJECT}",
        f"owner DELETE /api/users/{OTHER}",
        "keycloak delete",
        "drop database",
    ]
    # Leftover sessions are found by the gate id in their title.
    assert statements[0].startswith("SELECT id FROM threads WHERE position(")
    assert runner.gate_id in statements[0]


def test_residue_is_looked_up_by_the_gate_id(monkeypatch):
    runner = _runner()
    runner.project = PROJECT
    runner.pg_started = True
    runner.client_started = runner.account_started = runner.account_row = True
    runner.client_uuid, runner.account_keycloak_id = CLIENT_ID, KEYCLOAK_ID
    runner.other_id = OTHER
    statements: list[str] = []
    answers = {"threads WHERE position": "", "count(*)": "0"}

    def sql(query, **_kwargs):
        statements.append(query)
        return next((value for key, value in answers.items() if key in query), "0")

    counted: list[dict] = []
    monkeypatch.setattr(gate, "sql", sql)
    monkeypatch.setattr(
        gate,
        "in_orchestrator",
        lambda program, payload, **k: (
            counted.append(payload) or {"users": 0, "clients": 0}
        ),
    )
    assert runner.residue() == []
    joined = "\n".join(statements)
    assert f"name LIKE '{runner.gate_id} %'" in joined
    assert f"name LIKE '{runner.gate_id}-%'" in joined
    assert "kind = 'Connector' AND deleted_at IS NULL" in joined
    assert f"FROM projects WHERE id = '{PROJECT}'" in joined
    assert f"datname = '{runner.pg_name}'" in joined
    assert (
        f"FROM users WHERE lower(email) = lower('{runner.other_email}') OR "
        f"id = '{OTHER}' OR keycloak_sub = '{KEYCLOAK_ID}'"
    ) in joined
    assert [
        (c["action"], c["username"], c["client"], c["user_started"]) for c in counted
    ] == [("count", runner.gate_id, f"{runner.gate_id}-oauth", True)]

    answers = {"FROM users": "1", "threads WHERE position": "", "count(*)": "0"}
    monkeypatch.setattr(
        gate,
        "in_orchestrator",
        lambda program, payload, **k: {"users": 1, "clients": 1},
    )
    assert runner.residue() == [
        f"the second account's app row {OTHER}",
        f"the Keycloak user {runner.gate_id}",
        f"the Keycloak client {runner.gate_id}-oauth",
    ]


def test_a_named_second_account_is_never_created_or_deleted(monkeypatch):
    runner = _runner("--other-user", "dev-user-1", "--other-password", "p" * 16)
    monkeypatch.setattr(
        gate, "in_orchestrator", lambda *a, **k: pytest.fail("no Keycloak call")
    )
    monkeypatch.setattr(
        gate, "sql", lambda query, **k: "" if "threads" in query else "0"
    )
    assert runner.cleanup() == []
    assert runner.residue() == []


# -- the disposable second account -------------------------------------------------


def _minting(monkeypatch, created):
    calls: list[tuple[str, object]] = []

    def in_orchestrator(program, payload, **_kwargs):
        assert program is gate._KEYCLOAK_PROGRAM
        calls.append(("keycloak", dict(payload)))
        return created

    def sql(query, **_kwargs):
        calls.append(("sql", query))
        return OTHER if "gen_random_uuid" in query else ""

    def sql_script(script, **_kwargs):
        calls.append(("sql_script", script))
        return ""

    monkeypatch.setattr(gate, "in_orchestrator", in_orchestrator)
    monkeypatch.setattr(gate, "sql", sql)
    monkeypatch.setattr(gate, "sql_script", sql_script)
    return calls


def test_the_account_is_admitted_before_its_first_login(monkeypatch):
    runner = _runner()
    runner.owner_id = OWNER
    calls = _minting(monkeypatch, {"id": KEYCLOAK_ID, "found": [KEYCLOAK_ID]})

    runner.mint_account()

    (_, create), (_, fresh), (_, insert) = calls
    assert create == {
        "action": "create-user",
        "client": f"{runner.gate_id}-oauth",
        "marker": runner.gate_id,
        "client_started": False,
        "username": runner.gate_id,
        "email": runner.other_email,
        "user_started": True,
        "password": runner.other.password,
    }
    assert "gen_random_uuid" in fresh
    assert runner.account_started and runner.account_row
    assert (runner.account_keycloak_id, runner.other_id) == (KEYCLOAK_ID, OTHER)
    # Admitted, linked to its Keycloak subject, approved by the owner: the
    # first login finds the row by sub and provisions nothing.
    assert insert.startswith("INSERT INTO users (id, display_name, email, keycloak_sub")
    for value in (OTHER, runner.gate_id, runner.other_email, KEYCLOAK_ID, OWNER):
        assert f"'{value}'" in insert
    assert "true, now()" in insert
    assert runner.other.password not in insert


def test_an_existing_keycloak_user_is_never_adopted(monkeypatch):
    runner = _runner()
    runner.owner_id = OWNER
    calls = _minting(monkeypatch, {"exists": True})

    with pytest.raises(gate.GateError, match="already exists"):
        runner.mint_account()
    assert not runner.account_started and not runner.account_row
    assert [kind for kind, _ in calls] == ["keycloak"]


@pytest.mark.parametrize(
    "created",
    [
        {"id": "", "found": []},
        {"id": KEYCLOAK_ID, "found": []},
        {"id": KEYCLOAK_ID, "found": [KEYCLOAK_ID, OTHER]},
        {"id": "not-a-uuid", "found": ["not-a-uuid"]},
    ],
)
def test_an_unproven_keycloak_user_is_still_cleaned_up(monkeypatch, created):
    runner = _runner()
    runner.owner_id = OWNER
    _minting(monkeypatch, created)

    with pytest.raises(gate.GateError, match="no Keycloak receipt"):
        runner.mint_account()
    # Recorded before it was created: cleanup finds it by username and email.
    assert runner.account_started and not runner.account_row


def test_both_accounts_log_in_with_the_runs_oauth_client(monkeypatch):
    runner = _runner()
    with pytest.raises(gate.GateError, match="no OAuth client"):
        runner.owner.call("GET", "/api/auth/me")
    calls = _minting(monkeypatch, {"id": CLIENT_ID, "found": [CLIENT_ID]})

    runner.mint_client()

    ((_, create),) = calls
    assert create["action"] == "create-client"
    assert (create["client"], create["marker"]) == (
        f"{runner.gate_id}-oauth",
        runner.gate_id,
    )
    assert "password" not in create
    assert runner.client_started and runner.client_uuid == CLIENT_ID
    assert runner.owner.client_id == runner.other.client_id == create["client"]
    payloads: list[dict] = []
    monkeypatch.setattr(
        gate,
        "in_orchestrator",
        lambda program, payload, **k: payloads.append(payload)
        or {"status": 200, "body": "{}"},
    )
    runner.owner.call("GET", "/api/auth/me")
    assert payloads[0]["client_id"] == f"{runner.gate_id}-oauth"


def test_an_existing_or_unproven_client_is_handled_like_the_account(monkeypatch):
    runner = _runner()
    _minting(monkeypatch, {"exists": True})
    with pytest.raises(gate.GateError, match="already exists"):
        runner.mint_client()
    assert not runner.client_started and runner.owner.client_id is None

    _minting(monkeypatch, {"id": CLIENT_ID, "found": []})
    with pytest.raises(gate.GateError, match="no Keycloak receipt"):
        runner.mint_client()
    assert runner.client_started and runner.owner.client_id is None


def _owner(monkeypatch, runner, *, admin=True):
    def owner_ok(method, path, body=None):
        if path == "/api/auth/me":
            return {"user": {"id": OWNER, "is_admin": admin, "is_approved": True}}
        return {
            "is_admin": admin,
            "grants": None if admin else {"public_datasources": True},
        }

    monkeypatch.setattr(runner.owner, "ok", owner_ok)


def test_the_second_login_must_be_the_admitted_row(monkeypatch):
    runner = _runner()
    monkeypatch.setattr(runner, "mint_client", lambda: None)
    monkeypatch.setattr(
        runner, "mint_account", lambda: setattr(runner, "other_id", OTHER)
    )
    _owner(monkeypatch, runner)
    stranger = "ffffffff-0000-4000-8000-0000000000d1"
    monkeypatch.setattr(
        runner.other,
        "ok",
        lambda m, p, b=None: {"user": {"id": stranger, "is_approved": True}},
    )
    with pytest.raises(gate.GateError, match="not the admitted row"):
        runner.accounts()

    monkeypatch.setattr(
        runner.other,
        "ok",
        lambda m, p, b=None: {
            "user": {"id": OTHER, "is_approved": True, "is_admin": False}
        },
    )
    runner.accounts()
    assert runner.report.passed, runner.report.results


def test_a_disposable_account_needs_an_administrator_owner(monkeypatch):
    runner = _runner()
    monkeypatch.setattr(runner, "mint_client", lambda: None)
    monkeypatch.setattr(
        runner, "mint_account", lambda: pytest.fail("must not mint an account")
    )
    _owner(monkeypatch, runner, admin=False)
    with pytest.raises(gate.GateError, match="administrator owner"):
        runner.accounts()


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


# -- the in-pod Keycloak program, against a fake Keycloak ---------------------------

ADMIN_PASSWORD = "admin-password-never-printed"


class _FakeKeycloak(BaseHTTPRequestHandler):
    users: list[dict] = []
    clients: list[dict] = []
    _ROOT = "/identity/admin/realms/srw"

    def _store(self, path):
        for name in ("users", "clients"):
            if path == f"{self._ROOT}/{name}":
                return name, getattr(self, name), None
            if path.startswith(f"{self._ROOT}/{name}/"):
                return name, getattr(self, name), path.rsplit("/", 1)[1]
        return None, None, None

    def log_message(self, *_args):
        pass

    def _send(self, status, body=None, headers=None):
        data = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _authorized(self):
        return self.headers.get("Authorization") == "Bearer admin-token"

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length).decode()
        path = urlsplit(self.path).path
        if path == "/identity/realms/master/protocol/openid-connect/token":
            form = {k: v[0] for k, v in parse_qs(raw).items()}
            if (form.get("username"), form.get("password")) != (
                "admin",
                ADMIN_PASSWORD,
            ):
                return self._send(401, {"error": "invalid_grant"})
            return self._send(200, {"access_token": "admin-token"})
        _name, store, item = self._store(path)
        if store is not None and item is None and self._authorized():
            created = {**json.loads(raw), "id": str(uuid.uuid4())}
            store.append(created)
            location = f"http://{self.headers['Host']}{path}/{created['id']}"
            return self._send(201, headers={"Location": location})
        return self._send(404, {"error": "not found"})

    def do_GET(self):
        split = urlsplit(self.path)
        query = {k: v[0] for k, v in parse_qs(split.query).items()}
        name, store, item = self._store(split.path)
        if store is not None and item is None and self._authorized():
            if name == "users":
                assert query.get("exact") == "true"
                key, value = "username", query["username"]
            else:
                key, value = "clientId", query["clientId"]
            found = [
                {k: v for k, v in entry.items() if k != "credentials"}
                for entry in store
                if entry[key] == value
            ]
            return self._send(200, found)
        return self._send(404, {"error": "not found"})

    def do_DELETE(self):
        _name, store, item = self._store(urlsplit(self.path).path)
        if store is not None and item is not None and self._authorized():
            before = len(store)
            store[:] = [entry for entry in store if entry["id"] != item]
            return self._send(204 if len(store) < before else 404)
        return self._send(404, {"error": "not found"})


@pytest.fixture
def fake_keycloak():
    _FakeKeycloak.users, _FakeKeycloak.clients = [], []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeKeycloak)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/identity/", _FakeKeycloak
    finally:
        server.shutdown()
        server.server_close()


def _keycloak(url, payload, *, admin_password=ADMIN_PASSWORD):
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("KEYCLOAK_") and "proxy" not in key.lower()
    }
    env.update(
        KEYCLOAK_URL=url,
        KEYCLOAK_REALM="srw",
        KEYCLOAK_ADMIN_USER="admin",
        KEYCLOAK_ADMIN_PASSWORD=admin_password,
    )
    completed = subprocess.run(
        [sys.executable, "-c", gate._KEYCLOAK_PROGRAM],
        input=json.dumps(payload) + "\n",
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    assert ADMIN_PASSWORD not in completed.stdout + completed.stderr
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.splitlines()[-1])


def test_the_keycloak_program_creates_counts_and_deletes_only_its_own(
    fake_keycloak,
):
    url, server = fake_keycloak
    runner = _runner()
    who = {
        "client": runner.oauth_client,
        "marker": runner.gate_id,
        "username": runner.gate_id,
        "email": runner.other_email,
    }

    made = _keycloak(url, {"action": "create-client", **who})
    (client,) = server.clients
    assert made == {"id": client["id"], "found": [client["id"]]}
    assert client["clientId"] == runner.oauth_client
    assert client["publicClient"] and client["directAccessGrantsEnabled"]
    assert not client["standardFlowEnabled"] and not client["serviceAccountsEnabled"]
    assert client["defaultClientScopes"] == ["profile", "email", "roles"]
    assert client["attributes"] == {"srw-gate": runner.gate_id}

    created = _keycloak(url, {"action": "create-user", **who, "password": "p" * 24})
    (user,) = server.users
    assert created == {"id": user["id"], "found": [user["id"]]}
    assert user["enabled"] and user["emailVerified"]
    assert user["requiredActions"] == []
    assert user["credentials"] == [
        {"type": "password", "value": "p" * 24, "temporary": False}
    ]
    assert "p" * 24 not in json.dumps(created)
    # Never adopted: a second create finds each and creates nothing.
    assert _keycloak(url, {"action": "create-client", **who}) == {"exists": True}
    assert _keycloak(url, {"action": "create-user", **who, "password": "q" * 24}) == {
        "exists": True
    }
    started = {**who, "user_started": True, "client_started": True}
    assert _keycloak(url, {"action": "count", **started}) == {"users": 1, "clients": 1}
    # Nothing this run did not start is counted (a named --other-user).
    assert _keycloak(url, {"action": "count", **who}) == {"users": 0, "clients": 0}

    # Someone else's user and client of the same names are refused.
    server.users.append(
        {"id": str(uuid.uuid4()), "username": runner.gate_id, "email": "x@y.z"}
    )
    server.clients.append(
        {"id": str(uuid.uuid4()), "clientId": runner.oauth_client, "attributes": {}}
    )
    result = _keycloak(
        url,
        {
            "action": "delete",
            **started,
            "user_id": user["id"],
            "client_uuid": client["id"],
        },
    )
    assert result["deleted"] == [user["id"], client["id"]]
    assert len(result["refused"]) == 2
    assert (result["users"], result["clients"]) == (1, 1)
    server.users.clear()
    server.clients.clear()
    assert _keycloak(url, {"action": "delete", **started}) == {
        "deleted": [],
        "refused": [],
        "users": 0,
        "clients": 0,
    }


def test_the_keycloak_program_needs_the_pods_admin_credentials(fake_keycloak):
    url, _server = fake_keycloak
    payload = {"action": "count", "username": "u", "email": "e", "client": "c"}
    assert _keycloak(url, payload, admin_password="") == {
        "error": "the orchestrator has no Keycloak admin credentials"
    }
