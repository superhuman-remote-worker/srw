"""Safety contract and evaluators of the local k3d gate for the srw client
(``scripts/k3d-cli-gate.py``; never run here)."""

from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-cli-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_cli_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

CONFIRMED = ["--run", "--confirm", gate.LOCAL_CONFIRMATION]


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--context", "main"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        [*CONFIRMED, "--server", "https://api.example.com"],
        [*CONFIRMED, "--server", "https://api.localhost.example.com"],
        [*CONFIRMED, "--server", "ftp://api.localhost"],
        [*CONFIRMED, "--model", "bad model; rm -rf /"],
        [*CONFIRMED, "--user", "Robert'); DROP TABLE users;--"],
        [*CONFIRMED, "--admin", "not-an-address"],
        [*CONFIRMED, "--gate-id", "clig-xyz"],
        [*CONFIRMED, "--gate-id", "other-01234567"],
        [*CONFIRMED, "--wait", "forever"],
        # A sweep removes things: never in a dry run, never without the id.
        ["--sweep"],
        ["--gate-id", "clig-0123abcd", "--sweep"],
        [*CONFIRMED, "--sweep"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in gate.PHASES:
        assert f"  - {phase}:" in out
    assert gate.LOCAL_CONFIRMATION in out


def test_every_phase_of_the_plan_is_a_method_and_the_run_calls_it():
    source = _SCRIPT.read_text()
    order = re.search(r"for name in \(([^)]*)\):", source).group(1)
    ran = re.findall(r'"([a-z-]+)"', order)
    # cleanup runs in ``finally``, after the phases.
    assert [*ran, "cleanup"] == list(gate.PHASES)
    for phase in gate.PHASES:
        assert callable(getattr(gate.Gate, phase.replace("-", "_")))


def test_only_hashes_reach_the_database():
    emails = {"user": "u@example.invalid", "admin": "a@example.invalid"}
    digests = {name: f"{index:064x}" for index, name in enumerate(gate.TOKENS)}
    sql = gate.mint_sql("clig-0123abcd", emails, digests)
    assert sql.count("INSERT INTO auth_tokens") == len(gate.TOKENS)
    # Named after the run, short-lived, and never the token itself.
    assert sql.count("'clig-0123abcd-") == len(gate.TOKENS)
    assert sql.count("interval '3 hours'") == len(gate.TOKENS)
    assert "ak_" not in sql.replace("'ak_gate'", "")
    assert "ARRAY['jobs:read','jobs:write','resources:write','projects:write']" in sql
    # The MCP token carries no action scopes.
    mcp = next(line for line in sql.splitlines() if "'mcp'" in line)
    assert "NULL" in mcp and "'srw_gate'" in mcp
    revoke = gate.revoke_sql("clig-0123abcd")
    assert "name LIKE 'clig-0123abcd-%'" in revoke
    assert "DELETE FROM experts WHERE name LIKE 'clig-0123abcd-helper-%'" in revoke


def test_a_runs_jobs_can_be_found_again_by_its_id():
    # Cleanup after an interrupted run finds the jobs by this tag.
    assert gate.job_tag("clig-0123abcd") == "(gate clig-0123abcd)"
    sql = gate.leftover_jobs_sql("clig-0123abcd")
    assert (
        sql == "SELECT id FROM jobs WHERE description LIKE '%(gate clig-0123abcd)%';\n"
    )
    source = _SCRIPT.read_text()
    assert 'f"{task} {job_tag(self.gate_id)}"' in source
    # A sweep skips the phases and still cleans up and revokes.
    assert source.index("if args.sweep:") < source.index("gate.cleanup()")


def test_the_token_set_covers_both_users_and_every_scope_under_test():
    users = {who for who, _, _ in gate.TOKENS.values()}
    assert users == {"user", "admin"}
    scopes = {scope for _, _, held in gate.TOKENS.values() for scope in held}
    assert {
        "jobs:read",
        "jobs:write",
        "resources:write",
        "projects:write",
        "admin",
    } == scopes
    # An admin's token without the admin scope: the admin flag must not leak.
    assert gate.TOKENS["a_scoped"] == ("admin", "api", ("jobs:read", "resources:write"))


def test_the_manifests_are_named_after_the_run_and_valid():
    from shared.manifests import ManifestError, parse_documents

    files = gate.manifests("clig-0123abcd", "some-model")
    for name in ("expert.yaml", "job.yaml", "project.yaml"):
        (document,) = parse_documents(files[name])
        assert document["metadata"]["name"].startswith("clig-0123abcd-")
    (template,) = parse_documents(files["template.json"], format="json")
    assert template["kind"] == "WorkspaceTemplate"
    # The broken file is broken on purpose: the gate checks that the error
    # names it.
    with pytest.raises(ManifestError):
        parse_documents(files["broken.yaml"])


@pytest.mark.parametrize(
    ("got", "text", "code", "needles", "ok"),
    [
        (0, "job/1 completed", 0, ("completed",), True),
        (10, "job/1 pending_review", 10, ("pending_review",), True),
        (0, "job/1 completed", 10, (), False),
        (0, "job/1 completed", 0, ("pending_review",), False),
        (3, "", 3, (), True),
    ],
)
def test_a_check_wants_the_exit_code_and_every_text(got, text, code, needles, ok):
    assert (gate.verdict(got, text, code, needles) == "") is ok


def test_approve_is_sent_again_only_for_a_conflict(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(gate.time, "sleep", lambda _: None)
    run = gate.Gate(gate.parser().parse_args([]), tmp_path / "srw", tmp_path)

    def answers(*codes):
        left = list(codes)
        calls = []

        def srw(token, *argv, **_):
            calls.append(argv)
            return left.pop(0), "", ""

        monkeypatch.setattr(run, "srw", srw)
        return calls

    calls = answers(5, 5, 0)
    assert run.approve("u_full", "job-id")[0] == 0 and len(calls) == 3
    assert (
        "NOTE  approve answered a conflict 2 time(s) first" in capsys.readouterr().out
    )
    # Another failure is the answer; nothing is sent again.
    calls = answers(3)
    assert run.approve("u_full", "job-id")[0] == 3 and len(calls) == 1
    # A conflict that stays is reported as one.
    calls = answers(5, 5, 5, 5)
    assert run.approve("u_full", "job-id")[0] == 5 and len(calls) == 4
    calls = answers(0)
    assert run.approve("u_full", "job-id")[0] == 0
    assert "NOTE" not in capsys.readouterr().out.split("first")[-1]


def test_tokens_are_scrubbed_from_what_is_printed(tmp_path):
    args = gate.parser().parse_args([])
    run = gate.Gate(args, tmp_path / "srw", tmp_path)
    token = run.tokens["a_admin"]
    assert token not in run.scrub(f"Authorization: Bearer {token} was refused")
    assert _GATE_ID.match(run.gate_id)


_GATE_ID = re.compile(r"^clig-[0-9a-f]{8}$")
