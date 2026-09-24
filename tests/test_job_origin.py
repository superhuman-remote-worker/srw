"""Job origin provenance — the stamp, its vocabulary, and its blind spot.

The failure mode this guards is silent by construction: ``origin`` defaults to
``'user'``, so a creation path that forgets to stamp does not error, it just
files unattended work as something a human asked for. Nothing at runtime
notices. So alongside the behavioural tests there is a structural one
(``TestEveryCreationPathStamps``) that walks the AST for ``create_job`` calls
and fails when a new one appears without a stamp.

See knowledge-base/knowledge/features/job_origin_provenance.md.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from orchestrator.database.postgres import KNOWN_JOB_ORIGINS
from orchestrator.application import jobs as jobs_composition

ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATOR = ROOT / "src" / "orchestrator"


class TestVocabulary:
    def test_the_eight_values_are_the_taxonomy(self):
        assert KNOWN_JOB_ORIGINS == {
            "user",
            "session",
            "automation",
            "loop",
            "officer",
            "subjob",
            "lifecycle",
            "bench",
        }

    def test_the_backfill_and_the_default_agree_on_user(self):
        """Migration 0172's ELSE arm and create_job's default must match, or a
        historic row and an identical new row get different origins."""
        migration = (
            ORCHESTRATOR / "database/migrations/app/0172_jobs_origin.sql"
        ).read_text()
        assert "ELSE 'user'" in migration
        assert "DEFAULT 'user'" in migration


class TestSubmittedJobOriginResolution:
    """POST /api/jobs is a funnel, not "the user path"."""

    @pytest.fixture(autouse=True)
    def _resolver(self):
        self.resolve = jobs_composition.resolve_submitted_job_origin

    def test_a_plain_submission_is_user(self):
        assert self.resolve(context=None, parent_job_id=None, thread_id=None) == "user"

    def test_a_thread_launch_is_a_session_job(self):
        assert (
            self.resolve(context=None, parent_job_id=None, thread_id="t-1") == "session"
        )

    def test_a_child_is_a_subjob_even_when_launched_from_a_session(self):
        """A subjob of a session-created job is a subjob first — same
        precedence the backfill uses."""
        assert (
            self.resolve(context=None, parent_job_id="j-1", thread_id="t-1") == "subjob"
        )

    def test_bench_is_recognised_by_its_context_marker(self):
        """Bench builds a request byte-identical to a normal internal
        submission, so without this it would land in every user's job list and
        in their spend attribution."""
        assert (
            self.resolve(
                context={"bench": {"run_id": "r-1", "arm": "baseline"}},
                parent_job_id=None,
                thread_id=None,
            )
            == "bench"
        )

    def test_bench_wins_over_the_thread_it_was_dispatched_from(self):
        assert (
            self.resolve(
                context={"bench": {"run_id": "r-1"}},
                parent_job_id=None,
                thread_id="t-1",
            )
            == "bench"
        )

    def test_every_resolution_is_in_the_vocabulary(self):
        for context, parent, thread in (
            (None, None, None),
            (None, None, "t"),
            (None, "j", None),
            ({"bench": {}}, None, None),
        ):
            assert (
                self.resolve(context=context, parent_job_id=parent, thread_id=thread)
                in KNOWN_JOB_ORIGINS
            )


class TestCreateJobValidatesOrigin:
    @pytest.mark.asyncio
    async def test_an_unknown_origin_raises_rather_than_writing(self):
        from orchestrator.database.postgres import PostgresDB

        db = PostgresDB.__new__(PostgresDB)
        with pytest.raises(ValueError, match="Unsupported job origin"):
            await db.create_job(description="x", origin="definitely-not-a-value")

    @pytest.mark.asyncio
    async def test_a_typo_in_a_known_value_still_raises(self):
        from orchestrator.database.postgres import PostgresDB

        db = PostgresDB.__new__(PostgresDB)
        with pytest.raises(ValueError, match="Unsupported job origin"):
            await db.create_job(description="x", origin="Subjob")


class TestAgentsCannotInsertJobs:
    @pytest.mark.asyncio
    async def test_the_agent_side_jobs_create_refuses(self):
        """It was a raw INSERT INTO jobs bypassing the stamp, the authority
        columns and execution-lane inheritance. It had no callers, but
        src/agent.py holds the object and calls siblings on it."""
        from agent.database.postgres_db import JobsNamespace

        namespace = JobsNamespace(db=object())
        with pytest.raises(NotImplementedError, match="must not insert jobs"):
            await namespace.create(description="x")


def _create_job_calls(tree: ast.AST) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "create_job"
    ]


def _enclosing_assigns_origin_key(tree: ast.AST, call: ast.Call) -> bool:
    """True when some ``<dict>["origin"] = ...`` exists in the same module.

    Two call sites pass ``**kwargs`` built earlier — the POST /api/jobs funnel
    and the officer admission helper — so the stamp is a subscript assignment
    rather than a keyword.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.slice, ast.Constant)
                    and target.slice.value == "origin"
                ):
                    return True
        if isinstance(node, ast.Dict):
            for key in node.keys:
                if isinstance(key, ast.Constant) and key.value == "origin":
                    return True
    return False


class TestEveryCreationPathStamps:
    """The structural guard. A new creation path that forgets to stamp is
    invisible at runtime — it just inherits the 'user' default."""

    def test_no_create_job_call_relies_on_the_default(self):
        unstamped: list[str] = []
        examined = 0
        for path in sorted(ORCHESTRATOR.rglob("*.py")):
            tree = ast.parse(path.read_text())
            calls = _create_job_calls(tree)
            if not calls:
                continue
            module_stamps = _enclosing_assigns_origin_key(tree, calls[0])
            for call in calls:
                examined += 1
                has_keyword = any(kw.arg == "origin" for kw in call.keywords)
                has_splat = any(kw.arg is None for kw in call.keywords)
                if has_keyword:
                    continue
                if has_splat and module_stamps:
                    continue
                unstamped.append(f"{path.relative_to(ROOT)}:{call.lineno}")
        assert not unstamped, (
            "create_job() call sites with no origin stamp — they will silently "
            "default to 'user' and file unattended work as human work: "
            + ", ".join(unstamped)
        )
        # A scan that finds nothing passes for the wrong reason. There are
        # seven direct call sites; if this ever drops the scan has broken, not
        # the codebase.
        assert examined >= 7, f"only {examined} create_job call sites examined"

    def test_no_raw_insert_into_jobs_outside_the_helper(self):
        """The helper is the only place the stamp can be applied, so anything
        writing the table directly bypasses it entirely."""
        offenders: list[str] = []
        for root in (ORCHESTRATOR, ROOT / "src" / "agent"):
            for path in sorted(root.rglob("*.py")):
                text = path.read_text()
                if "INSERT INTO jobs " not in text and "INSERT INTO jobs\n" not in text:
                    continue
                if path.name == "postgres.py" and root is ORCHESTRATOR:
                    continue  # create_job() itself
                offenders.append(str(path.relative_to(ROOT)))
        assert not offenders, (
            "raw INSERT INTO jobs bypasses create_job() and its origin stamp: "
            + ", ".join(offenders)
        )
