"""Safety contract and expectations of the local D3c gate (never run here)."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[1] / "scripts" / "k3d-connector-selection-gate.py"
)
_SPEC = importlib.util.spec_from_file_location("k3d_connector_selection_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

OWNER = "00000000-0000-4000-8000-0000000000c1"
PROJECT = "00000000-0000-4000-8000-0000000000b1"
LINKED = "1a2b3c4d-5e6f-4000-8000-000000000001"
KB = "1a2b3c4d-5e6f-4000-8000-000000000002"


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


def _runner(*extra):
    args = gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION, *extra]
    )
    return gate.ConnectorSelectionGate(args)


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "d3a-0123456789"],
        ["--gate-id", "d3c-xyz"],
        ["--model", "bad model"],
        ["--user", "Robert'); DROP"],
        ["--other-user", "test"],
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
        "manifest",
        "sessions",
        "execution.connectors",
        "datasource_ids",
        "jobs",
        "conflicts",
        "400",
        "refusals",
        "403",
        "defaults",
        "--other-user",
        "cleanup",
    ):
        assert phase in out


def test_embedded_programs_compile_and_cap_their_memory():
    for program in (
        gate._API_PROGRAM,
        gate._BINDINGS_PROGRAM,
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


def test_the_bindings_program_imports_what_this_checkout_has():
    """Every name the in-pod program imports exists here (it never runs
    against the cluster in this suite)."""
    from orchestrator.application.preparation import (  # noqa: F401
        datasource_payload_dependencies,
    )
    from orchestrator.services import job_datasource_selection as jds
    from orchestrator.services.thread_datasource_authorization import (  # noqa: F401
        ThreadDatasourceAuthorizationDependencies,
        authorize_thread_datasource_selection,
        resolve_authorized_thread_datasources,
    )
    from orchestrator.services.workspace_tier_policy import (  # noqa: F401
        backend_from_override,
    )

    assert callable(jds.resolve_authorized_job_datasources)
    assert callable(jds.revalidate_job_datasource_selection)


def test_the_expected_details_are_the_products():
    from orchestrator.services import connector_refs, datasource_policy
    from orchestrator.services.project_connectors import DATASOURCE_DRIVER

    assert gate.GENERIC_UNAVAILABLE_DETAIL == (
        datasource_policy.GENERIC_UNAVAILABLE_DETAIL
    )
    assert connector_refs.SELECTOR_CONFLICT_DETAIL.startswith(
        gate.SELECTOR_CONFLICT_PREFIX
    )
    assert gate.DATASOURCE_DRIVER == DATASOURCE_DRIVER


def test_the_refs_the_gate_sends_are_valid_requests():
    from orchestrator.schemas.execution_selection import ExecutionSelection

    execution = gate.connector_refs(
        {
            "own": {
                "name": "own-0123456789ab",
                "scope": {"kind": "Account", "name": "me"},
            },
            "public": {"uid": LINKED},
            "linked": {
                "name": "linked-0123456789ab",
                "scope": {"kind": "Account", "name": OWNER},
            },
            "knowledge": {"name": "knowledge-0123456789ab"},
        }
    )
    assert set(ExecutionSelection.model_validate(execution).connectors) == {
        "own",
        "public",
        "linked",
        "knowledge",
    }


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
        (None, False),
    ],
)
def test_publishing_needs_an_administrator_or_the_grant(capabilities, allowed):
    assert gate.can_publish_connectors(capabilities) is allowed


# -- what the slice says -----------------------------------------------------------


def _lookup(kind, scope, name):
    return {
        ("Account", OWNER, "linked-1a2b3c4d5e6f"): LINKED,
        ("Project", PROJECT, "knowledge-1a2b3c4d5e6f"): KB,
    }.get((kind, scope, name))


def _ref(name, kind="Account", scope=OWNER):
    return {"ref": {"name": name, "scope": {"kind": kind, "name": scope}}}


def test_a_manifest_of_refs_lists_its_connectors():
    entries = {
        "linked-1a2b3c4d5e6f": _ref("linked-1a2b3c4d5e6f"),
        "knowledge-1a2b3c4d5e6f": _ref("knowledge-1a2b3c4d5e6f", "Project", PROJECT),
    }
    assert gate.listed_ids(entries, _lookup, project=PROJECT) == ({LINKED, KB}, [])


@pytest.mark.parametrize(
    ("entries", "problem"),
    [
        (
            {
                "datasource-x": {
                    "inline": {
                        "driver": "srw.datasource/v1",
                        "config": {"datasourceId": LINKED},
                    }
                }
            },
            "inline srw.datasource/v1",
        ),
        ({"gone": _ref("gone-1a2b3c4d5e6f")}, "names no datasource Connector"),
        (
            {"knowledge-1a2b3c4d5e6f": {"ref": {"name": "knowledge-1a2b3c4d5e6f"}}},
            "without an explicit scope",
        ),
        ({"db": _ref("linked-1a2b3c4d5e6f")}, "alias is not the resource name"),
        ({"env": {"inline": {"driver": "srw.env/v1"}}}, "not a datasource entry"),
    ],
)
def test_each_deviation_from_the_d3c_form_is_named(entries, problem):
    _ids, problems = gate.listed_ids(entries, _lookup, project=PROJECT)
    assert problems and problem in problems[0]


def test_an_inline_entry_still_counts_as_the_link_it_names():
    ids, _problems = gate.listed_ids(
        {
            "datasource-x": {
                "inline": {
                    "driver": "srw.datasource/v1",
                    "config": {"datasourceId": LINKED},
                }
            }
        },
        _lookup,
        project=PROJECT,
    )
    assert ids == {LINKED}


def test_bindings_compare_everything_but_the_timestamp():
    record = {
        "ids": [LINKED, KB],
        "selection": {
            "origin": "explicit",
            "policy_revisions": {LINKED: 2, KB: 1},
            "materialized_at": "2026-10-08T10:00:00+00:00",
        },
        "resolved": sorted([LINKED, KB]),
        "payload": ["d3c-0123456789 linked"],
    }
    later = {
        **record,
        "selection": {**record["selection"], "materialized_at": "2026-10-08T10:00:09"},
    }
    assert gate.same_bindings(record, later) == []
    for key, value in (
        ("ids", [KB, LINKED]),
        ("selection", {**record["selection"], "policy_revisions": {LINKED: 3}}),
        ("resolved", [LINKED]),
        ("payload", []),
    ):
        assert gate.same_bindings(record, {**record, key: value})[0].startswith(key)


# -- the run's envelope ------------------------------------------------------------


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


def test_the_jobs_are_deleted_even_when_their_check_fails(monkeypatch):
    runner = _runner()
    runner.project = PROJECT
    runner.connectors = {"own": "ds-own", "public": "ds-pub", "linked": "ds-lnk"}
    monkeypatch.setattr(
        runner, "selection_refs", lambda: (["ds-own"], {"connectors": {}})
    )
    created = iter(("job-refs", "job-ids"))
    monkeypatch.setattr(
        runner.other,
        "call",
        lambda method, path, body=None: (200, {"job_id": next(created)}),
    )

    def broken(**_kwargs):
        raise gate.GateError("pod exec failed")

    deleted: list[str] = []
    monkeypatch.setattr(runner, "bindings", broken)
    monkeypatch.setattr(runner, "delete_job", lambda job: deleted.append(job) or True)
    with pytest.raises(gate.GateError):
        runner.job_phase()
    assert deleted == ["job-refs", "job-ids"]
    assert runner.jobs == {}


def test_cleanup_order_and_scope(monkeypatch):
    runner = _runner()
    runner.threads = {"refs": "00000000-0000-4000-8000-000000000001"}
    runner.jobs = {"refs": "00000000-0000-4000-8000-000000000002"}
    runner.project = PROJECT
    runner.connectors = {"public": "ds-1", "own": "ds-2"}
    runner.connector_api = {"public": "owner", "own": "other"}
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
    thread, job = runner.threads["refs"], runner.jobs["refs"]
    assert order == [
        f"other DELETE /api/persistent/threads/{thread}",
        f"other PUT /api/jobs/{job}/cancel",
        f"other DELETE /api/jobs/{job}",
        "owner DELETE /api/datasources/ds-1",
        "other DELETE /api/datasources/ds-2",
        f"owner DELETE /api/projects/{PROJECT}",
        "drop database",
    ]
    # Leftover sessions and jobs are found by the gate id.
    joined = "\n".join(statements)
    assert "FROM threads WHERE position(" in joined
    assert "FROM jobs WHERE position(" in joined
    assert runner.gate_id in joined


def test_residue_is_looked_up_by_the_gate_id(monkeypatch):
    runner = _runner()
    runner.project = PROJECT
    runner.pg_started = True
    statements: list[str] = []

    def sql(query, **_kwargs):
        statements.append(query)
        return "" if "WHERE position(" in query else "0"

    monkeypatch.setattr(gate, "sql", sql)
    assert runner.residue() == []
    joined = "\n".join(statements)
    assert f"name LIKE '{runner.gate_id} %'" in joined
    assert f"name LIKE '{runner.gate_id}-%'" in joined
    assert "kind = 'Connector' AND deleted_at IS NULL" in joined
    assert f"FROM projects WHERE id = '{PROJECT}'" in joined
    assert f"datname = '{runner.pg_name}'" in joined


# -- the phases read the product's answers -------------------------------------

PRIVATE = "1a2b3c4d-5e6f-4000-8000-000000000003"


def _phase_runner(monkeypatch):
    runner = _runner()
    runner.project = PROJECT
    runner.kb = KB
    runner.connectors = {"linked": LINKED, "private": PRIVATE}
    monkeypatch.setattr(
        runner,
        "resource_ref",
        lambda datasource_id: {
            "name": "private-1a2b3c4d5e6f",
            "scope": {"kind": "Account", "name": OWNER},
        },
    )
    monkeypatch.setattr(runner, "titled_threads", lambda: [])
    monkeypatch.setattr(runner, "described_jobs", lambda: [])
    return runner


def test_the_defaults_phase_passes_on_the_designed_answers(monkeypatch):
    runner = _phase_runner(monkeypatch)
    stored: list[str] = []

    def view():
        effective = [{"id": KB, "platform_owned": True}] + [
            {"id": value, "platform_owned": False} for value in stored
        ]
        return {"stored": list(stored), "effective": effective, "can_edit": False}

    def other(method, path, body=None):
        if method == "GET":
            return 200, view()
        if method == "PUT":
            return 403, {"detail": "Project owner role required"}
        return 200, {"datasource_ids": [KB, *stored]}

    def owner(method, path, body=None):
        wanted = [str(value) for value in body["connector_ids"]]
        if PRIVATE in wanted:
            return 422, {"detail": "link the connector first"}
        stored[:] = wanted
        return 200, view()

    monkeypatch.setattr(runner.other, "call", other)
    monkeypatch.setattr(runner.owner, "call", owner)
    runner.defaults()
    assert runner.report.passed, runner.report.results
    assert len(runner.report.results) == 6


def test_the_defaults_phase_fails_when_a_default_does_not_attach(monkeypatch):
    runner = _phase_runner(monkeypatch)

    def other(method, path, body=None):
        if method == "GET":
            return 200, {
                "stored": [],
                "effective": [{"id": KB, "platform_owned": True}],
                "can_edit": False,
            }
        if method == "PUT":
            return 403, {}
        return 200, {"datasource_ids": [KB]}

    def owner(method, path, body=None):
        wanted = [str(value) for value in body["connector_ids"]]
        if PRIVATE in wanted:
            return 422, {}
        return 200, {
            "stored": wanted,
            "effective": [{"id": KB}] + [{"id": value} for value in wanted],
        }

    monkeypatch.setattr(runner.other, "call", other)
    monkeypatch.setattr(runner.owner, "call", owner)
    runner.defaults()
    failed = [name for name, ok, _ in runner.report.results if not ok]
    assert failed == [
        "defaults: the owner's linked default is stored after the platform "
        "entries and reaches a member's defaults preview",
    ]


def test_refusals_pass_only_on_one_identical_403(monkeypatch):
    runner = _phase_runner(monkeypatch)
    calls: list[tuple[str, dict]] = []

    def same(method, path, body=None):
        calls.append((path, body))
        return 403, {"detail": gate.GENERIC_UNAVAILABLE_DETAIL}

    monkeypatch.setattr(runner.other, "call", same)
    runner.refusals()
    assert runner.report.passed, runner.report.results
    # Five preview variants and three job creates, each naming one selector.
    assert len(calls) == 8
    assert all(
        ("execution" in body) != ("datasource_ids" in body) for _path, body in calls
    )

    runner = _phase_runner(monkeypatch)
    answers = iter([(403, {"detail": gate.GENERIC_UNAVAILABLE_DETAIL})] * 7)
    monkeypatch.setattr(
        runner.other,
        "call",
        lambda method, path, body=None: next(
            answers, (404, {"detail": "Connector not found"})
        ),
    )
    runner.refusals()
    assert not runner.report.passed


def _world(monkeypatch, runner, *, refresh=True):
    """A project whose manifest lists its links as refs (``refresh``), or
    keeps the manifest of the fixture (no refresh on link and unlink)."""
    links = {KB}
    listed = {KB}
    names: dict[str, str] = {KB: "knowledge-1a2b3c4d5e6f"}

    def ref(datasource_id):
        return {
            "name": names[datasource_id],
            "scope": {"kind": "Account", "name": OWNER},
        }

    def changed():
        if refresh:
            listed.clear()
            listed.update(links)

    def create(label, body, *, who="owner"):
        datasource_id = f"00000000-0000-4000-8000-{len(names):012d}"
        names[datasource_id] = f"{label}-{datasource_id[-12:]}"
        runner.connectors[label] = datasource_id
        if body.get("project_ids"):
            links.add(datasource_id)
            changed()
        return datasource_id

    def ok(method, path, body=None):
        datasource_id = path.rsplit("/", 1)[-1]
        if method == "POST":
            links.add(datasource_id)
        else:
            links.discard(datasource_id)
        changed()
        return {}

    monkeypatch.setattr(runner, "create_connector", create)
    monkeypatch.setattr(runner.owner, "ok", ok)
    monkeypatch.setattr(runner, "resource_ref", ref)
    monkeypatch.setattr(
        runner,
        "manifest_entries",
        lambda: {names[value]: {"ref": ref(value)} for value in listed},
    )
    monkeypatch.setattr(runner, "links", lambda: set(links))
    monkeypatch.setattr(
        runner,
        "lookup",
        lambda kind, scope, name: next(
            (key for key, value in names.items() if value == name), None
        ),
    )
    monkeypatch.setattr(gate, "sql", lambda query, **k: "0")


@pytest.mark.parametrize("refresh", [True, False])
def test_the_manifest_phase_follows_every_link_write(monkeypatch, refresh):
    runner = _phase_runner(monkeypatch)
    _world(monkeypatch, runner, refresh=refresh)
    runner.manifest()
    failed = [name for name, ok, _ in runner.report.results if not ok]
    if refresh:
        assert failed == [], runner.report.results
    else:
        # A manifest that is not refreshed fails after a link, the entry
        # check, and after the connector created linked.
        assert failed and all("manifest:" in name for name in failed)
        assert any("after a link" in name for name in failed)
    assert "doomed" not in runner.connectors


@pytest.mark.parametrize("same", [True, False])
def test_the_sessions_phase_compares_the_two_creations(monkeypatch, same):
    runner = _phase_runner(monkeypatch)
    runner.connectors.update(public="ds-public", own="ds-own")
    ids = ["ds-own", "ds-public", LINKED, KB]
    monkeypatch.setattr(
        runner,
        "selection_refs",
        lambda: (ids, gate.connector_refs({"db": {"uid": LINKED}})),
    )
    created = iter(("thread-refs", "thread-ids"))

    def call(method, path, body=None):
        if path.endswith("/preview"):
            return 200, {"datasource_ids": ids}
        return 200, {"thread_id": next(created), "status": "created"}

    record = {
        "ids": ids,
        "selection": {"origin": "explicit", "policy_revisions": {LINKED: 1}},
        "resolved": sorted(ids),
        "payload": sorted(runner.name(label) for label in ("public", "linked")),
    }
    other = {**record, "ids": ids if same else ids[:-1]}
    monkeypatch.setattr(runner.other, "call", call)
    monkeypatch.setattr(
        runner,
        "bindings",
        lambda threads=None, jobs=None: {"refs": record, "ids": other},
    )
    runner.sessions()
    assert runner.threads == {"refs": "thread-refs", "ids": "thread-ids"}
    assert runner.report.passed is same, runner.report.results


def test_conflicts_need_the_selector_400_and_no_new_work(monkeypatch):
    for status, detail, passed in (
        (400, "execution.connectors and datasource_ids are mutually exclusive", True),
        (422, "validation error", False),
    ):
        runner = _phase_runner(monkeypatch)
        monkeypatch.setattr(
            runner,
            "selection_refs",
            lambda: ([LINKED], gate.connector_refs({"db": {"uid": LINKED}})),
        )
        monkeypatch.setattr(
            runner.other,
            "call",
            lambda method, path, body=None, status=status, detail=detail: (
                status,
                {"detail": detail},
            ),
        )
        runner.conflicts()
        assert runner.report.passed is passed, runner.report.results
