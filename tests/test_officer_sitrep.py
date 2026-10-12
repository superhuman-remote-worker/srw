"""Officer sitrep + wake-routing tests — S3/S4 of knowledge-base/knowledge/features/centurion.md.

Covers the computed-delta sitrep (fingerprint diff, no-progress flag,
per-section degradation, baseline preservation on failure), the officer
predicate on thread dicts, the job-transition choke point in
``maybe_wake_session`` (officer leg for terminal AND paused statuses), and
the jobs-outbox → officer-event conversion in ``_deliver`` (double-wake
suppression). Live delivery is k3d-smoke territory, as with the substrate
suite.
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import session_wake, sitrep

from orchestrator.database.postgres import JobQueryResult

THREAD_ID = str(uuid.uuid4())
PROJECT_ID = str(uuid.uuid4())
JOB_A = str(uuid.uuid4())
JOB_B = str(uuid.uuid4())
JOB_C = str(uuid.uuid4())


class _FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *args):
        return False


def _fake_conn():
    conn = SimpleNamespace()
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchval = AsyncMock(return_value=0)
    return conn


def _fake_db(jobs=None, conn=None):
    conn = conn or _fake_conn()
    db = SimpleNamespace()
    db.query_jobs = AsyncMock(return_value=JobQueryResult(jobs=list(jobs or [])))
    db.acquire = lambda: _FakeAcquire(conn)
    # officer_post.md §4: the capacity section resolves the post's thread
    # lineage before counting; a lone thread's lineage is itself.
    db.get_officer_capacity_lineage = AsyncMock(return_value=[THREAD_ID])
    return db


def _officer_thread(prior_sitrep=None, **officer):
    metadata = {"config_override": {"officer": {"enabled": True, **officer}}}
    if prior_sitrep is not None:
        metadata["officer_state"] = {"sitrep": prior_sitrep}
    return {"id": THREAD_ID, "project_id": PROJECT_ID, "metadata": metadata}


def _audit(counts=None, end=None):
    reader = SimpleNamespace()
    reader.get_audit_counts = AsyncMock(return_value=counts or {})
    reader.get_audit_time_range = AsyncMock(return_value={"end": end} if end else None)
    return reader


TIMER_ROW = {
    "id": 1,
    "source": "timer",
    "dedup_key": "timer",
    "payload": {"minutes": 30, "reason": "waiting on the migration job"},
}


class TestSitrepBuild:
    @pytest.mark.asyncio
    async def test_first_sitrep_lists_new_jobs_and_baselines(self):
        jobs = [
            {"id": JOB_A, "status": "processing", "description": "build the thing"},
            {"id": JOB_B, "status": "completed", "description": "old work"},
        ]
        db = _fake_db(jobs=jobs)
        text, patch = await sitrep.build_wake_message(
            db,
            _officer_thread(),
            [TIMER_ROW],
            audit_reader=_audit(counts={JOB_A: 12}),
            usage_ledger=None,
        )
        assert text is not None
        assert "first sitrep" in text
        assert "timer: slept ~30 min" in text
        assert f"NEW {JOB_A[:8]} processing" in text
        assert f"NEW {JOB_B[:8]} completed" in text
        assert "steps 12" in text
        prints = patch["sitrep"]["fingerprints"]
        assert prints[JOB_A] == {"status": "processing", "steps": 12}
        assert prints[JOB_B]["status"] == "completed"
        assert patch["sitrep"]["watermark"]

    @pytest.mark.asyncio
    async def test_workspace_contract_is_visible_without_transport_coordinates(self):
        jobs = [
            {
                "id": JOB_A,
                "status": "processing",
                "description": "VM work",
                "workspace_contract": {
                    "requested_backend": "vm",
                    "assigned_backend": "vm",
                    "effective_backend": None,
                    "state": "mismatch",
                    "failure": "sandbox_ready_for_vm_assignment",
                    "stale_backend": "sandbox",
                },
            }
        ]
        text, _patch = await sitrep.build_wake_message(
            _fake_db(jobs=jobs),
            _officer_thread(),
            [TIMER_ROW],
            audit_reader=_audit(counts={JOB_A: 1}),
            usage_ledger=None,
        )

        assert "workspace requested=vm, assigned=vm" in text
        assert (
            "effective=unavailable, state=mismatch, "
            "failure=sandbox_ready_for_vm_assignment, stale=sandbox"
        ) in text
        assert "ssh_host" not in text
        assert "pod_ip" not in text

    @pytest.mark.asyncio
    async def test_delta_transitions_stall_flag_and_steady(self):
        prior = {
            "watermark": (
                datetime.now(timezone.utc) - timedelta(minutes=30)
            ).isoformat(),
            "fingerprints": {
                JOB_A: {"status": "processing", "steps": 12},
                JOB_B: {"status": "processing", "steps": 4},
                JOB_C: {"status": "completed", "steps": None},
            },
        }
        jobs = [
            # A: still processing, same step count → NO PROGRESS flag.
            {"id": JOB_A, "status": "processing", "description": "stuck one"},
            # B: transitioned to failed → transition line with error.
            {
                "id": JOB_B,
                "status": "failed",
                "description": "flaky one",
                "error_message": "OOM in step 5",
            },
            # C: completed before and now → steady count only.
            {"id": JOB_C, "status": "completed", "description": "done one"},
        ]
        db = _fake_db(jobs=jobs)
        text, patch = await sitrep.build_wake_message(
            db,
            _officer_thread(prior_sitrep=prior),
            [TIMER_ROW],
            audit_reader=_audit(counts={JOB_A: 12}),
            usage_ledger=None,
        )
        assert "delta since" in text
        assert f"{JOB_B[:8]} processing → failed" in text
        assert "OOM in step 5" in text
        assert "steps 12→12 (NO PROGRESS since last sitrep)" in text
        assert "1 completed" in text  # steady tail
        assert patch["sitrep"]["fingerprints"][JOB_B]["status"] == "failed"

    @pytest.mark.asyncio
    async def test_a_job_held_for_review_says_why(self):
        """Officer backlog jobs run at full autonomy; a seal held because its
        delivery could not be proven must not reach the officer as a bare
        "processing → pending_review" (worker_git_versioning_stops_midjob_
        seal_pins_stale_revision.md)."""
        prior = {
            "watermark": (
                datetime.now(timezone.utc) - timedelta(minutes=30)
            ).isoformat(),
            "fingerprints": {
                JOB_A: {"status": "processing", "steps": 12},
                JOB_B: {"status": "processing", "steps": 4},
            },
        }
        jobs = [
            {
                "id": JOB_A,
                "status": "pending_review",
                "description": "probe the stack",
                "error_message": "Delivery unproven: the final commit did not "
                "land, so deliverable path(s) output/obstacles.md are not in "
                "the pushed revision.",
            },
            # An ordinary review item carries no reason, and gets none.
            {"id": JOB_B, "status": "pending_review", "description": "plain"},
        ]
        text, _patch = await sitrep.build_wake_message(
            _fake_db(jobs=jobs),
            _officer_thread(prior_sitrep=prior),
            [TIMER_ROW],
            audit_reader=_audit(),
            usage_ledger=None,
        )
        line_a = next(ln for ln in text.splitlines() if JOB_A[:8] in ln)
        line_b = next(ln for ln in text.splitlines() if JOB_B[:8] in ln)
        assert "processing → pending_review" in line_a
        assert "held: Delivery unproven" in line_a
        assert "held:" not in line_b

    @pytest.mark.asyncio
    async def test_jobs_failure_preserves_baseline_and_degrades(self):
        prior = {
            "watermark": datetime.now(timezone.utc).isoformat(),
            "fingerprints": {JOB_A: {"status": "processing", "steps": 3}},
        }
        db = _fake_db()
        db.query_jobs = AsyncMock(side_effect=RuntimeError("db down"))
        text, patch = await sitrep.build_wake_message(
            db,
            _officer_thread(prior_sitrep=prior),
            [TIMER_ROW],
            audit_reader=None,
            usage_ledger=None,
        )
        assert "Jobs: (section unavailable" in text
        # The failed section must NOT wipe the baseline.
        assert patch["sitrep"]["fingerprints"] == {
            JOB_A: {"status": "processing", "steps": 3}
        }

    @pytest.mark.asyncio
    async def test_sql_sections_degrade_without_killing_the_wake(self):
        db = _fake_db(jobs=[])

        def _broken_acquire():
            raise RuntimeError("pool gone")

        db.acquire = _broken_acquire
        text, patch = await sitrep.build_wake_message(
            db,
            _officer_thread(),
            [TIMER_ROW],
            audit_reader=None,
            usage_ledger=None,
        )
        assert text is not None
        assert "[SITREP]" in text
        assert "Capacity: (unavailable)" in text
        assert "Fleet: (unavailable)" in text
        assert "file a sleep" in text

    @staticmethod
    def _ledger():
        ledger = SimpleNamespace()
        ledger.query_usage = AsyncMock(
            return_value={
                "by_category": [
                    {"unit": "prompt-token", "quantity": 100000, "cost_usd": 1.5},
                    {"unit": "cached-prompt-token", "quantity": 20000, "cost_usd": 0.5},
                    {"unit": "completion-token", "quantity": 3456, "cost_usd": 0.5},
                    {"unit": "requests", "quantity": 42, "cost_usd": 0.0},
                ],
                "total_cost_usd": 2.5,
            }
        )
        return ledger

    @pytest.mark.asyncio
    async def test_budget_section_reads_ledger_when_the_legate_enables_it(self):
        ledger = self._ledger()
        text, _ = await sitrep.build_wake_message(
            _fake_db(jobs=[]),
            _officer_thread(show_budget=True),
            [TIMER_ROW],
            audit_reader=None,
            usage_ledger=ledger,
        )
        assert "Budget today (project): $2.50, 123,456 tokens." in text
        kwargs = ledger.query_usage.await_args.kwargs
        assert kwargs["scope_project_id"] == PROJECT_ID

    @pytest.mark.asyncio
    async def test_spend_is_invisible_until_the_legate_hands_it_over(self):
        """Default OFF — and the ledger is not even consulted.

        An officer shown a cost with no policy attached invents the policy.
        See ``sitrep._budget_visible``.
        """
        ledger = self._ledger()
        text, _ = await sitrep.build_wake_message(
            _fake_db(jobs=[]),
            _officer_thread(),
            [TIMER_ROW],
            audit_reader=None,
            usage_ledger=ledger,
        )
        assert "Budget" not in text
        ledger.query_usage.assert_not_awaited()


class TestOfficerPredicates:
    def test_thread_is_officer_variants(self):
        assert session_wake._thread_is_officer(_officer_thread()) is True
        # String-encoded metadata (some drivers return JSONB as text).
        import json

        assert (
            session_wake._thread_is_officer(
                {
                    "metadata": json.dumps(
                        {"config_override": {"officer": {"enabled": True}}}
                    )
                }
            )
            is True
        )
        assert (
            session_wake._thread_is_officer(
                {"metadata": {"config_override": {"officer": {"enabled": "true"}}}}
            )
            is True
        )
        assert session_wake._thread_is_officer({"metadata": {}}) is False
        assert (
            session_wake._thread_is_officer(
                {"metadata": {"config_override": {"officer": {"enabled": "yes"}}}}
            )
            is False
        )

    def test_dedup_key_shape(self):
        assert (
            session_wake._officer_job_dedup_key(JOB_A, "completed")
            == f"{JOB_A[:8]}:completed"
        )


def _routing_db(officer=True):
    db = SimpleNamespace()
    db.get_job = AsyncMock(
        return_value={
            "id": JOB_A,
            "project_id": PROJECT_ID,
            "description": "delegated work",
        }
    )
    # officer_post.md §O1: the officer leg routes through one post-locked
    # decision — the wake row is written inside that call, not by the caller.
    db.route_project_officer_job_transition = AsyncMock(
        return_value={
            "destination": "wake" if officer else "while_vacant",
            "thread_id": THREAD_ID if officer else None,
            "enqueued": bool(officer),
            "appended": not bool(officer),
        }
    )
    db.enqueue_session_wake_event = AsyncMock(return_value=True)
    db.mark_job_wake_pending = AsyncMock(return_value=True)
    # The kicked drain runs against this db as a background task; give it an
    # empty claim so it exits quietly.
    db.claim_pending_session_wake_events = AsyncMock(return_value=[])
    return db


class TestJobTransitionChokePoint:
    @pytest.mark.asyncio
    async def test_paused_notifies_officer_but_not_jobs_outbox(self):
        db = _routing_db()
        result = await session_wake.maybe_wake_session(db, JOB_A, "paused")
        assert result is False  # paused never enters the jobs outbox
        db.mark_job_wake_pending.assert_not_awaited()
        call = db.route_project_officer_job_transition.await_args
        assert call.args[0] == PROJECT_ID
        assert call.kwargs["job_id"] == JOB_A
        assert call.kwargs["status"] == "paused"
        assert call.kwargs["dedup_key"] == f"{JOB_A[:8]}:paused"
        await asyncio.sleep(0)  # let the kicked drain task settle

    @pytest.mark.asyncio
    async def test_completed_feeds_both_outboxes(self):
        db = _routing_db()
        result = await session_wake.maybe_wake_session(db, JOB_A, "completed")
        assert result is True
        db.mark_job_wake_pending.assert_awaited_once()
        assert (
            db.route_project_officer_job_transition.await_args.kwargs["dedup_key"]
            == f"{JOB_A[:8]}:completed"
        )
        await asyncio.sleep(0)

    @pytest.mark.asyncio
    async def test_vacant_post_routes_but_wakes_nobody(self):
        db = _routing_db(officer=False)
        await session_wake.maybe_wake_session(db, JOB_A, "completed")
        # The transition still reaches the post — it lands in the while-vacant
        # ledger instead of a wake queue — and the jobs outbox is untouched by
        # whether a project has an officer at all.
        db.route_project_officer_job_transition.assert_awaited_once()
        db.enqueue_session_wake_event.assert_not_awaited()
        db.mark_job_wake_pending.assert_awaited_once()
        await asyncio.sleep(0)

    @pytest.mark.asyncio
    async def test_processing_notifies_nobody(self):
        db = _routing_db()
        result = await session_wake.maybe_wake_session(db, JOB_A, "processing")
        assert result is False
        db.route_project_officer_job_transition.assert_not_awaited()
        db.enqueue_session_wake_event.assert_not_awaited()
        db.mark_job_wake_pending.assert_not_awaited()


class TestDeliverOfficerConversion:
    @pytest.mark.asyncio
    async def test_officer_thread_converts_instead_of_injecting(self):
        db = SimpleNamespace()
        db.get_thread = AsyncMock(return_value=_officer_thread())
        db.enqueue_session_wake_event = AsyncMock(return_value=True)
        db.claim_pending_session_wake_events = AsyncMock(return_value=[])
        row = {
            "id": JOB_A,
            "status": "completed",
            "created_by_thread_id": THREAD_ID,
            "description": "delegated work",
            "project_id": PROJECT_ID,
        }
        ok = await session_wake._deliver(db, row)
        assert ok is True
        kwargs = db.enqueue_session_wake_event.await_args.kwargs
        assert kwargs["source"] == "job_transition"
        assert kwargs["dedup_key"] == f"{JOB_A[:8]}:completed"
        assert kwargs["project_id"] == PROJECT_ID
        await asyncio.sleep(0)

    @pytest.mark.asyncio
    async def test_conversion_failure_keeps_the_claim(self):
        db = SimpleNamespace()
        db.get_thread = AsyncMock(return_value=_officer_thread())
        db.enqueue_session_wake_event = AsyncMock(side_effect=RuntimeError("down"))
        row = {
            "id": JOB_A,
            "status": "completed",
            "created_by_thread_id": THREAD_ID,
        }
        ok = await session_wake._deliver(db, row)
        assert ok is False  # released → retried, never dropped


class TestLegateNotes:
    """A directive is delivered whole or not at all (officer_legate_channel.md)."""

    NOTE = (
        "Stand down the theme work. Hotel Rheinland presents to their board on "
        "Thursday and nothing we have is demonstrable: no deployed URL, no seed "
        "data, no way for a non-developer to click through a booking. Reprioritise "
        "the backlog around one clickable path and tell me what you cut."
    )

    def _legate_row(self, message=None, key="ab12cd34"):
        return {
            "id": 2,
            "source": "legate",
            "dedup_key": key,
            "payload": {"message": message or self.NOTE},
        }

    @pytest.mark.asyncio
    async def test_a_queued_note_arrives_verbatim_and_leads_the_sitrep(self):
        """The generic reason renderer truncates at 160 chars — a directive must not."""
        text, _ = await sitrep.build_wake_message(
            _fake_db(),
            _officer_thread(),
            [self._legate_row(), TIMER_ROW],
            audit_reader=_audit(),
            usage_ledger=None,
        )
        assert self.NOTE in text
        assert text.index("Legate note") < text.index("Wake reasons")

    @pytest.mark.asyncio
    async def test_a_note_is_not_repeated_as_a_wake_reason(self):
        text, _ = await sitrep.build_wake_message(
            _fake_db(),
            _officer_thread(),
            [self._legate_row(key="deadbeef"), TIMER_ROW],
            audit_reader=_audit(),
            usage_ledger=None,
        )
        assert "deadbeef" not in text
        assert "Wake reasons (1)" in text

    @pytest.mark.asyncio
    async def test_a_sitrep_without_notes_has_no_legate_section(self):
        text, _ = await sitrep.build_wake_message(
            _fake_db(),
            _officer_thread(),
            [TIMER_ROW],
            audit_reader=_audit(),
            usage_ledger=None,
        )
        assert "Legate note" not in text
