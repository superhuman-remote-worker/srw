"""E3 — one liveness contract (officer_supervision_surface §5).

The gate: ONE fixture must produce the same state+reason on every surface —
the raw computation, the SITREP active-job line, the get_stuck_jobs row
rendering, and the get_job_progress rendering. ``jobs.updated_at`` is never
consulted, and a downed audit store yields ``unavailable`` — never a
fabricated 0% or a false stuck fact.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services.job_liveness import (
    STALL_THRESHOLD_MINUTES,
    compute_job_elapsed_seconds,
    compute_job_liveness,
    compute_jobs_liveness,
    get_liveness_policy,
    job_elapsed_seconds,
)
from orchestrator.services import sitrep
from shared.orch_surface import formatters as fmt

from orchestrator.database.postgres import JobQueryResult

JOB_ID = str(uuid.uuid4())
AGENT_ID = str(uuid.uuid4())
PROJECT_ID = str(uuid.uuid4())
NOW = datetime(2026, 8, 14, 12, 0, 0, tzinfo=timezone.utc)


def _audit_reader(*, end: datetime | None, available: bool = True):
    reader = SimpleNamespace()
    reader.is_available = available
    reader.get_audit_counts = AsyncMock(return_value={JOB_ID: 12})
    reader.get_audit_time_range = AsyncMock(
        return_value={"end": end.isoformat()} if end else None
    )
    return reader


def _db(*, heartbeat: datetime | None):
    db = SimpleNamespace()
    db.get_agent = AsyncMock(
        return_value={"id": AGENT_ID, "last_heartbeat": heartbeat}
        if heartbeat is not None
        else None
    )
    return db


def _stalled_job() -> dict:
    """THE fixture: processing, audit stalled 45m, fresh-ish updated_at.

    updated_at is deliberately RECENT — the poisoned column must not rescue
    the job from suspicion (that was the pre-E3 detect_stuck_jobs defect in
    reverse: it also must not be what convicts it).
    """
    return {
        "id": JOB_ID,
        "status": "processing",
        "description": "stalled worker",
        "assigned_agent_id": AGENT_ID,
        "created_at": NOW - timedelta(hours=2),
        "updated_at": NOW - timedelta(seconds=30),
    }


STALLED_AUDIT_END = NOW - timedelta(minutes=45)
EXPECTED_REASON = "no audit activity for 45m (threshold 30m)"


class TestSingleFixtureEverySurface:
    @pytest.mark.asyncio
    async def test_computation_is_suspected_stuck_with_the_reason(self):
        verdict = await compute_job_liveness(
            _stalled_job(),
            audit_reader=_audit_reader(end=STALLED_AUDIT_END),
            db=_db(heartbeat=NOW - timedelta(seconds=30)),
            threshold_minutes=30,
            now=NOW,
        )
        assert verdict["state"] == "suspected_stuck"
        assert EXPECTED_REASON in verdict["reasons"]
        assert verdict["last_activity_at"] == STALLED_AUDIT_END.isoformat()
        # E1 sources ride along for the envelope.
        assert {s["name"] for s in verdict["sources"]} == {
            "control_db",
            "audit_db",
            "agent_heartbeat",
        }

    @pytest.mark.asyncio
    async def test_sitrep_active_line_carries_the_same_state_and_reason(self):
        db = SimpleNamespace()
        db.query_jobs = AsyncMock(return_value=JobQueryResult(jobs=[_stalled_job()]))
        db.get_agent = AsyncMock(
            return_value={"id": AGENT_ID, "last_heartbeat": NOW - timedelta(seconds=30)}
        )
        lines, _prints = await sitrep._jobs_section(
            db,
            _audit_reader(end=STALLED_AUDIT_END),
            PROJECT_ID,
            {},
            None,
            NOW,
        )
        rendered = "\n".join(lines)
        assert "[suspected_stuck]" in rendered
        assert "no audit activity for 45m" in rendered

    @pytest.mark.asyncio
    async def test_sitrep_presents_blocked_undelivered_as_steady_terminal_state(self):
        blocked = {
            "id": JOB_ID,
            "status": "cancelled",
            "completion_outcome_kind": "blocked_undelivered",
            "description": "publication could not be delivered",
            "error_message": "pull request missing",
        }
        db = SimpleNamespace(
            query_jobs=AsyncMock(return_value=JobQueryResult(jobs=[blocked]))
        )
        previous = {JOB_ID: {"status": "blocked_undelivered", "steps": None}}

        lines, fingerprints = await sitrep._jobs_section(
            db, None, PROJECT_ID, previous, None, NOW
        )

        assert "Unchanged: 1 blocked_undelivered." in "\n".join(lines)
        assert fingerprints[JOB_ID]["status"] == "blocked_undelivered"

    @pytest.mark.asyncio
    async def test_stuck_row_and_progress_render_the_same_verdict(self):
        verdict = await compute_job_liveness(
            _stalled_job(),
            audit_reader=_audit_reader(end=STALLED_AUDIT_END),
            db=_db(heartbeat=NOW - timedelta(seconds=30)),
            threshold_minutes=30,
            now=NOW,
        )
        # get_stuck_jobs surface (route row = job + liveness merge).
        row = {**_stalled_job(), **verdict}
        row["created_at"] = row["created_at"].isoformat()
        row["updated_at"] = row["updated_at"].isoformat()
        stuck_text = fmt.format_stuck_jobs(
            {
                "jobs": [row],
                "threshold_minutes": 30,
                "threshold_source": "request_override",
            }
        )
        assert "Liveness: suspected_stuck" in stuck_text
        assert EXPECTED_REASON in stuck_text

        # get_job_progress surface (route payload = db basis + liveness merge).
        progress_payload = {
            "job_id": JOB_ID,
            "status": "processing",
            "progress_percent": None,
            "eta_seconds": None,
            "elapsed_seconds": 7200,
            **verdict,
        }
        progress_text = fmt.format_job_progress(JOB_ID, progress_payload)
        assert "Liveness: suspected_stuck" in progress_text
        assert EXPECTED_REASON in progress_text
        assert "%" not in progress_text  # no fabricated percentage, ever


class TestHonestUnavailability:
    @pytest.mark.asyncio
    async def test_audit_down_without_heartbeat_evidence_is_unavailable(self):
        """Acceptance §9.3: audit DB down → no 0%, no false-stuck fact."""
        verdict = await compute_job_liveness(
            _stalled_job(),
            audit_reader=_audit_reader(end=None, available=False),
            db=_db(heartbeat=None),
            now=NOW,
        )
        assert verdict["state"] == "unavailable"
        assert "audit store unavailable" in verdict["reasons"]
        assert "no agent heartbeat evidence" in verdict["reasons"]

    @pytest.mark.asyncio
    async def test_audit_down_but_fresh_heartbeat_is_active(self):
        verdict = await compute_job_liveness(
            _stalled_job(),
            audit_reader=_audit_reader(end=None, available=False),
            db=_db(heartbeat=NOW - timedelta(seconds=45)),
            now=NOW,
        )
        assert verdict["state"] == "active"
        assert "agent heartbeat fresh" in verdict["reasons"]

    @pytest.mark.asyncio
    async def test_audit_down_with_stale_heartbeat_is_suspected_stuck(self):
        verdict = await compute_job_liveness(
            _stalled_job(),
            audit_reader=_audit_reader(end=None, available=False),
            db=_db(heartbeat=NOW - timedelta(minutes=20)),
            now=NOW,
        )
        assert verdict["state"] == "suspected_stuck"
        assert any("heartbeat stale" in r for r in verdict["reasons"])
        heartbeat_source = next(
            source
            for source in verdict["sources"]
            if source["name"] == "agent_heartbeat"
        )
        assert heartbeat_source["status"] == "stale"


class TestControlPlaneAuthority:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "state", "reason_fragment"),
        [
            ("completed", "terminal", "terminal status 'completed'"),
            ("failed", "terminal", "terminal status 'failed'"),
            ("paused", "paused", "paused by control plane"),
            ("created", "waiting", "awaiting workspace provisioning"),
            ("pending_review", "waiting", "pending human review"),
            ("waiting_for_reply", "waiting", "waiting for a human reply"),
        ],
    )
    async def test_status_maps_without_consulting_audit(
        self, status, state, reason_fragment
    ):
        exploding_reader = SimpleNamespace()
        exploding_reader.is_available = True
        exploding_reader.get_audit_time_range = AsyncMock(
            side_effect=AssertionError("audit must not be consulted")
        )
        verdict = await compute_job_liveness(
            {**_stalled_job(), "status": status},
            audit_reader=exploding_reader,
            db=_db(heartbeat=None),
            now=NOW,
        )
        assert verdict["state"] == state
        assert any(reason_fragment in r for r in verdict["reasons"])

    @pytest.mark.asyncio
    async def test_paused_verdict_carries_the_freeze_reason(self):
        verdict = await compute_job_liveness(
            {
                **_stalled_job(),
                "status": "paused",
                "freeze_data": {"freeze_type": "budget_exceeded", "reason": "budget"},
            },
            now=NOW,
        )
        assert verdict["state"] == "paused"
        assert "budget" in verdict["reasons"]

    @pytest.mark.asyncio
    async def test_active_job_within_threshold(self):
        verdict = await compute_job_liveness(
            _stalled_job(),
            audit_reader=_audit_reader(end=NOW - timedelta(minutes=3)),
            db=_db(heartbeat=NOW - timedelta(seconds=30)),
            now=NOW,
        )
        assert verdict["state"] == "active"
        assert any("audit activity 3m ago" in r for r in verdict["reasons"])


class TestThresholdAndBatch:
    def test_one_threshold_module_default(self):
        # The single knob every surface shares (env-tunable, default 30).
        assert STALL_THRESHOLD_MINUTES == 30

    def test_deployment_default_and_override_report_their_authority(self, monkeypatch):
        monkeypatch.setenv("JOB_LIVENESS_STALL_MINUTES", "47")
        monkeypatch.setenv("JOB_LIVENESS_STALE_CLAIM_MINUTES", "305")
        deployment = get_liveness_policy()
        assert deployment.stall.as_dict() == {
            "threshold_minutes": 47,
            "threshold_source": "deployment_default",
        }
        assert deployment.stale_claim.as_dict() == {
            "threshold_minutes": 305,
            "threshold_source": "deployment_default",
        }
        assert get_liveness_policy(stall_override_minutes=60).stall.as_dict() == {
            "threshold_minutes": 60,
            "threshold_source": "request_override",
        }

    @pytest.mark.asyncio
    async def test_changed_deployment_default_reaches_computation_and_sitrep(
        self, monkeypatch
    ):
        monkeypatch.setenv("JOB_LIVENESS_STALL_MINUTES", "47")
        verdict = await compute_job_liveness(
            _stalled_job(),
            audit_reader=_audit_reader(end=NOW - timedelta(minutes=48)),
            db=_db(heartbeat=NOW - timedelta(seconds=30)),
            now=NOW,
        )
        assert verdict["threshold_minutes"] == 47
        assert verdict["threshold_source"] == "deployment_default"

        db = SimpleNamespace(
            query_jobs=AsyncMock(return_value=JobQueryResult(jobs=[_stalled_job()])),
            get_agent=AsyncMock(
                return_value={
                    "id": AGENT_ID,
                    "last_heartbeat": NOW - timedelta(seconds=30),
                }
            ),
        )
        lines, _ = await sitrep._jobs_section(
            db,
            _audit_reader(end=NOW - timedelta(minutes=48)),
            PROJECT_ID,
            {},
            None,
            NOW,
        )
        assert "stall threshold 47m, deployment_default" in "\n".join(lines)

    @pytest.mark.asyncio
    async def test_batch_shares_agent_lookups_and_matches_single(self):
        db = _db(heartbeat=NOW - timedelta(seconds=30))
        jobs = [
            _stalled_job(),
            {**_stalled_job(), "id": str(uuid.uuid4()), "status": "completed"},
        ]
        results = await compute_jobs_liveness(
            jobs,
            audit_reader=_audit_reader(end=STALLED_AUDIT_END),
            db=db,
            threshold_minutes=30,
            now=NOW,
        )
        assert results[JOB_ID]["state"] == "suspected_stuck"
        assert results[jobs[1]["id"]]["state"] == "terminal"
        # One distinct agent → exactly one lookup for the whole batch.
        assert db.get_agent.await_count == 1


# ---------------------------------------------------------------------------
# Duration: the /progress ``elapsed_seconds`` stops when the work does.
# ---------------------------------------------------------------------------

CREATED = datetime(2026, 11, 2, 9, 28, 25, tzinfo=timezone.utc)
LAST_STEP = datetime(2026, 11, 2, 12, 2, 15, tzinfo=timezone.utc)
LATER = datetime(2026, 11, 2, 15, 3, 0, tzinfo=timezone.utc)
RAN_FOR = (LAST_STEP - CREATED).total_seconds()  # 2h 33m 50s


def _stopped_job(status: str, **stamps) -> dict:
    return {"status": status, "created_at": CREATED.isoformat(), **stamps}


def _exploding_audit_reader():
    reader = SimpleNamespace()
    reader.is_available = True
    reader.get_audit_time_range = AsyncMock(
        side_effect=AssertionError("a running job must not read the audit store")
    )
    return reader


class TestJobElapsed:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status", ["created", "processing", "reviewing", "waiting"]
    )
    async def test_running_job_counts_to_now_without_reading_audit(self, status):
        elapsed = await compute_job_elapsed_seconds(
            JOB_ID,
            _stopped_job(status, updated_at=LAST_STEP.isoformat()),
            audit_reader=_exploding_audit_reader(),
            now=LATER,
        )
        assert elapsed == (LATER - CREATED).total_seconds()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status", ["pending_review", "paused", "waiting_for_reply"]
    )
    async def test_job_waiting_on_a_human_stops_at_its_last_step(self, status):
        """The reported bug: a pending_review job kept counting for hours."""
        elapsed = await compute_job_elapsed_seconds(
            JOB_ID,
            _stopped_job(status, updated_at=LATER.isoformat()),
            audit_reader=_audit_reader(end=LAST_STEP),
            now=LATER + timedelta(days=3),
        )
        assert elapsed == RAN_FOR

    @pytest.mark.asyncio
    async def test_reviewed_completion_does_not_count_the_review_wait(self):
        """completed_at is the approval, a day after the agent finished."""
        approved = LAST_STEP + timedelta(days=1)
        elapsed = await compute_job_elapsed_seconds(
            JOB_ID,
            _stopped_job(
                "completed",
                completed_at=approved.isoformat(),
                updated_at=approved.isoformat(),
            ),
            audit_reader=_audit_reader(end=LAST_STEP),
            now=approved + timedelta(hours=1),
        )
        assert elapsed == RAN_FOR

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "stamps"),
        [
            ("completed", {"completed_at": LAST_STEP, "updated_at": LATER}),
            ("failed", {"failed_at": LAST_STEP, "updated_at": LATER}),
            # A stale completed_at from an earlier run is not this stop's stamp.
            (
                "pending_review",
                {
                    "completed_at": CREATED + timedelta(minutes=5),
                    "updated_at": LAST_STEP,
                },
            ),
            ("cancelled", {"updated_at": LAST_STEP}),
        ],
    )
    async def test_audit_store_down_falls_back_to_the_status_stamp(
        self, status, stamps
    ):
        elapsed = await compute_job_elapsed_seconds(
            JOB_ID,
            _stopped_job(status, **{k: v.isoformat() for k, v in stamps.items()}),
            audit_reader=_audit_reader(end=None, available=False),
            now=LATER + timedelta(days=1),
        )
        assert elapsed == RAN_FOR

    def test_end_is_clamped_to_now_and_never_before_creation(self):
        future = _stopped_job(
            "completed", completed_at=(LATER + timedelta(hours=1)).isoformat()
        )
        assert (
            job_elapsed_seconds(future, last_activity=None, now=LATER)
            == (LATER - CREATED).total_seconds()
        )
        assert (
            job_elapsed_seconds(
                _stopped_job("failed"),
                last_activity=CREATED - timedelta(minutes=1),
                now=LATER,
            )
            == 0.0
        )

    def test_no_creation_stamp_is_unknown_not_zero(self):
        assert (
            job_elapsed_seconds({"status": "processing"}, last_activity=None, now=LATER)
            is None
        )

    @pytest.mark.asyncio
    async def test_progress_route_reports_the_stopped_duration(self):
        from orchestrator.services import job_inspection

        store = SimpleNamespace(
            get_job_progress=AsyncMock(
                return_value={
                    "job_id": JOB_ID,
                    "status": "pending_review",
                    "progress_percent": None,
                    "eta_seconds": None,
                    "created_at": CREATED.isoformat(),
                    "updated_at": LAST_STEP.isoformat(),
                    "completed_at": None,
                    "failed_at": None,
                }
            ),
            get_agent=AsyncMock(return_value=None),
        )
        payload = await job_inspection.get_job_progress(
            job_id=JOB_ID,
            authorized_job={"id": JOB_ID, "status": "pending_review"},
            dependencies=SimpleNamespace(
                store=store, audit_reader=_audit_reader(end=LAST_STEP)
            ),
        )
        assert payload["elapsed_seconds"] == RAN_FOR
        assert payload["state"] == "waiting"
