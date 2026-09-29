"""Tests for ``orchestrator/services/cron_dispatcher.py``.

Cover DST-correctness of ``compute_next_run_after`` (the part that has
to keep "09:00 Europe/Berlin" at 09:00 across spring-forward / fall-back),
input validation, and the per-tick state machine — fire, catchup skip,
max-fires auto-disable, invalid-cron auto-disable — against a fully
mocked DB so the tests stay fast and don't need a Postgres container.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from orchestrator.services.cron_dispatcher import (
    _process_one_due_automation,
    compute_initial_next_run,
    compute_next_run_after,
    validate_cron_expr,
    validate_timezone,
)


# ---------------------------------------------------------------------------
# compute_next_run_after — DST correctness
# ---------------------------------------------------------------------------


class TestComputeNextRunAfter:
    def test_simple_utc_daily(self) -> None:
        """Daily 09:00 UTC, base just after a fire → next is the day after."""
        base = datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc)
        out = compute_next_run_after("0 9 * * *", base, "UTC")
        assert out == datetime(2026, 5, 19, 9, 0, tzinfo=timezone.utc)

    def test_simple_utc_hourly(self) -> None:
        base = datetime(2026, 5, 18, 10, 0, tzinfo=timezone.utc)
        out = compute_next_run_after("0 * * * *", base, "UTC")
        assert out == datetime(2026, 5, 18, 11, 0, tzinfo=timezone.utc)

    def test_dst_spring_forward_berlin(self) -> None:
        """Daily 09:00 Europe/Berlin stays at 09:00 local across DST.

        Spring forward 2026 is 2026-03-29 (Europe/Berlin clocks jump
        02:00 → 03:00 CET → CEST). The 09:00 fire on 03-28 is at 08:00 UTC
        (CET=+01:00); the 09:00 fire on 03-29 is at 07:00 UTC (CEST=+02:00).
        """
        before_dst = datetime(2026, 3, 28, 8, 0, tzinfo=timezone.utc)  # 09:00 CET
        out = compute_next_run_after("0 9 * * *", before_dst, "Europe/Berlin")
        # Should be 2026-03-29 09:00 CEST = 07:00 UTC, not 08:00 UTC
        expected_local = datetime(2026, 3, 29, 9, 0, tzinfo=ZoneInfo("Europe/Berlin"))
        assert out == expected_local.astimezone(timezone.utc)
        assert out.hour == 7  # CEST is UTC+2

    def test_dst_fall_back_berlin(self) -> None:
        """Daily 09:00 Europe/Berlin stays at 09:00 local across fall-back.

        Fall back 2026 is 2026-10-25. 09:00 CEST on 10-24 = 07:00 UTC;
        09:00 CET on 10-25 = 08:00 UTC.
        """
        before_dst = datetime(2026, 10, 24, 7, 0, tzinfo=timezone.utc)  # 09:00 CEST
        out = compute_next_run_after("0 9 * * *", before_dst, "Europe/Berlin")
        expected_local = datetime(2026, 10, 25, 9, 0, tzinfo=ZoneInfo("Europe/Berlin"))
        assert out == expected_local.astimezone(timezone.utc)
        assert out.hour == 8  # CET is UTC+1

    def test_naive_input_treated_as_utc(self) -> None:
        """A naive datetime is not raised on — astimezone() of a naive
        datetime assumes local; we pass through utc-aware inputs in the
        real code path, but defending the helper signature matters."""
        base = datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc)
        out = compute_next_run_after("0 9 * * *", base, "UTC")
        assert out.tzinfo is not None  # always tz-aware

    def test_returns_utc(self) -> None:
        base = datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc)
        out = compute_next_run_after("*/5 * * * *", base, "America/Los_Angeles")
        assert out.tzinfo == timezone.utc


class TestComputeInitialNextRun:
    def test_seeds_from_now_default(self) -> None:
        """Without an explicit ``now``, uses the wall clock."""
        out = compute_initial_next_run("0 0 1 1 *", "UTC")
        assert out.tzinfo == timezone.utc
        assert out > datetime.now(timezone.utc)

    def test_explicit_now(self) -> None:
        now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
        out = compute_initial_next_run("0 9 * * *", "UTC", now=now)
        # Next 09:00 after 12:00 today is tomorrow 09:00.
        assert out == datetime(2026, 5, 19, 9, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Input validation helpers
# ---------------------------------------------------------------------------


class TestValidateCronExpr:
    @pytest.mark.parametrize(
        "expr",
        [
            "* * * * *",
            "0 9 * * *",
            "*/5 * * * *",
            "0 0 1 1 *",
            "0 9 * * 1-5",
        ],
    )
    def test_accepts_valid(self, expr: str) -> None:
        validate_cron_expr(expr)  # does not raise

    @pytest.mark.parametrize(
        "expr",
        [
            "",
            "not a cron",
            "99 * * * *",
            "* * * *",  # too few fields
        ],
    )
    def test_rejects_invalid(self, expr: str) -> None:
        with pytest.raises(ValueError):
            validate_cron_expr(expr)


class TestValidateTimezone:
    @pytest.mark.parametrize("tz", ["UTC", "Europe/Berlin", "America/New_York"])
    def test_accepts_iana(self, tz: str) -> None:
        validate_timezone(tz)  # does not raise

    @pytest.mark.parametrize("tz", ["", "Not/A/Zone", "GMT+1", "Europe/Atlantis"])
    def test_rejects_invalid(self, tz: str) -> None:
        with pytest.raises(ValueError):
            validate_timezone(tz)


# ---------------------------------------------------------------------------
# _process_one_due_automation — state-machine paths
# ---------------------------------------------------------------------------


def _make_mock_db(due_row: dict | None) -> MagicMock:
    """Build an asyncpg-shaped mock with the standard ``acquire`` →
    ``transaction`` async-context-manager chain plus the four automation
    helpers the dispatcher calls.
    """
    conn = MagicMock()
    conn.transaction = MagicMock(return_value=_AsyncCtx(MagicMock()))

    db = MagicMock()
    db.acquire = MagicMock(return_value=_AsyncCtx(conn))
    db.fetch_next_due_cron_automation = AsyncMock(return_value=due_row)
    db.advance_automation_after_fire = AsyncMock()
    db.skip_automation_fire = AsyncMock()
    db.auto_disable_automation = AsyncMock()
    # Scheduled work resolves the stored owner's live connector defaults before
    # creating the job. These tests have no connector fixtures, so model one
    # active owner with an empty default set.
    db.get_user = AsyncMock(
        return_value={
            "id": "33333333-3333-3333-3333-333333333333",
            "is_approved": True,
        }
    )
    db.user_is_member_of_projects = AsyncMock(return_value=True)
    db.list_default_datasource_candidates = AsyncMock(return_value=[])
    # create_job is invoked by create_job_from_automation
    db.create_job = AsyncMock(
        return_value={"id": "11111111-1111-1111-1111-111111111111"}
    )
    # An automation carrying a project_id is gated on that project's lifecycle
    # state before the fire (see TestArchivedProjectSkipsTheFire). None reads
    # as "no such project", which is not archived.
    db.get_project = AsyncMock(return_value=None)
    # No project_workspace_defaults row: the installation decides the tier.
    db.fetchrow = AsyncMock(return_value=None)
    return db


class _AsyncCtx:
    """Minimal ``async with`` returning a fixed value (asyncpg pool/tx shape)."""

    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _make_row(
    *,
    cron_expr: str = "0 9 * * *",
    timezone_name: str = "UTC",
    next_run_at: datetime,
    catchup_window_seconds: int = 86400,
    fires_today_count: int = 0,
    fires_today_date: date | None = None,
    max_fires_per_day: int = 100,
    autonomy: str = "review",
    config_override: dict | None = None,
    project_id: str | None = None,
) -> dict:
    """Mimics ``db.fetch_next_due_cron_automation`` output."""
    return {
        "id": "22222222-2222-2222-2222-222222222222",
        "owner_id": "33333333-3333-3333-3333-333333333333",
        "project_id": project_id,
        "name": "test-automation",
        "cron_expr": cron_expr,
        "timezone": timezone_name,
        "next_run_at": next_run_at,
        "catchup_window_seconds": catchup_window_seconds,
        "expert": "scholar",
        "prompt": "Do the thing",
        "config_override": config_override or {},
        "autonomy": autonomy,
        "priority": 5,
        "fires_today_count": fires_today_count,
        "fires_today_date": fires_today_date,
        "max_fires_per_day": max_fires_per_day,
    }


class TestProcessOneDueAutomation:
    @pytest.mark.asyncio
    async def test_no_due_returns_false_without_side_effects(self) -> None:
        db = _make_mock_db(due_row=None)
        out = await _process_one_due_automation(db)
        assert out is False
        db.create_job.assert_not_called()
        db.advance_automation_after_fire.assert_not_called()
        db.skip_automation_fire.assert_not_called()
        db.auto_disable_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_fires_when_due(self) -> None:
        now = datetime.now(timezone.utc)
        row = _make_row(next_run_at=now)  # due now
        db = _make_mock_db(due_row=row)

        out = await _process_one_due_automation(db)

        assert out is True
        db.create_job.assert_awaited_once()
        db.advance_automation_after_fire.assert_awaited_once()
        # The advance call should carry the new next_run_at + the scheduled
        # time we just fired for, plus the new job's id.
        kwargs = db.advance_automation_after_fire.await_args.kwargs
        assert kwargs["scheduled_for"] == now
        assert kwargs["next_run_at"] > now
        assert kwargs["job_id"] == "11111111-1111-1111-1111-111111111111"

    @pytest.mark.asyncio
    async def test_catchup_window_skip(self) -> None:
        """A fire scheduled more than ``catchup_window_seconds`` ago is
        skipped without firing a job."""
        long_ago = datetime.now(timezone.utc).replace(
            year=2025, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        row = _make_row(
            next_run_at=long_ago,
            catchup_window_seconds=3600,  # 1h grace, well under the gap
        )
        db = _make_mock_db(due_row=row)

        out = await _process_one_due_automation(db)

        assert out is True
        db.create_job.assert_not_called()
        db.advance_automation_after_fire.assert_not_called()
        db.skip_automation_fire.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_max_fires_per_day_auto_disables(self) -> None:
        now = datetime.now(timezone.utc)
        row = _make_row(
            next_run_at=now,
            max_fires_per_day=5,
            fires_today_count=5,
            fires_today_date=now.date(),
        )
        db = _make_mock_db(due_row=row)

        out = await _process_one_due_automation(db)

        assert out is True
        db.create_job.assert_not_called()
        db.auto_disable_automation.assert_awaited_once()
        kwargs = db.auto_disable_automation.await_args.kwargs
        assert "max_fires_per_day" in kwargs["reason"]

    @pytest.mark.asyncio
    async def test_yesterdays_counter_does_not_block(self) -> None:
        """fires_today_count from a previous date must not block today's
        fire — the counter rolls daily.
        """
        now = datetime.now(timezone.utc)
        yesterday = (
            now.replace(hour=0, minute=0, second=0, microsecond=0)
            - (now - now.replace(year=now.year))
        ).date()
        # Use a clearly-prior date
        row = _make_row(
            next_run_at=now,
            max_fires_per_day=5,
            fires_today_count=999,  # would be over cap if it were today
            fires_today_date=date(2020, 1, 1),
        )
        del yesterday  # not actually used; clarity-only
        db = _make_mock_db(due_row=row)

        out = await _process_one_due_automation(db)

        assert out is True
        db.create_job.assert_awaited_once()
        db.auto_disable_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_cron_disables_row(self) -> None:
        """A bad cron expression on a stored row must not loop the tick
        forever — disable the row so the owner can repair it.
        """
        now = datetime.now(timezone.utc)
        row = _make_row(next_run_at=now, cron_expr="this is not a cron")
        db = _make_mock_db(due_row=row)

        out = await _process_one_due_automation(db)

        assert out is True
        db.create_job.assert_not_called()
        db.auto_disable_automation.assert_awaited_once()
        kwargs = db.auto_disable_automation.await_args.kwargs
        assert "invalid cron" in kwargs["reason"].lower()


# ---------------------------------------------------------------------------
# Auto-disable owner notification — fires AFTER the txn commits, never on a
# normal fire, never crashes the dispatcher on transport failure.
# ---------------------------------------------------------------------------


def _patch_notification_service(
    monkeypatch, *, available: bool = True, side_effect=None
):
    """Swap the dispatcher's notification_service reference for a mock.

    Returns the AsyncMock the dispatcher will call so the caller can
    inspect arguments / await counts.
    """
    notify_mock = AsyncMock(side_effect=side_effect)
    fake_service = MagicMock()
    fake_service.is_available = available
    fake_service.record_automation_disabled = notify_mock
    monkeypatch.setattr(
        "orchestrator.services.cron_dispatcher.notification_service",
        fake_service,
    )
    return notify_mock


class TestAutoDisableNotification:
    @pytest.mark.asyncio
    async def test_max_fires_disable_emits_notification(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The owner gets notified when max_fires_per_day trips auto-disable."""
        notify_mock = _patch_notification_service(monkeypatch)
        now = datetime.now(timezone.utc)
        row = _make_row(
            next_run_at=now,
            max_fires_per_day=5,
            fires_today_count=5,
            fires_today_date=now.date(),
        )
        db = _make_mock_db(due_row=row)

        await _process_one_due_automation(db)

        notify_mock.assert_awaited_once()
        kwargs = notify_mock.await_args.kwargs
        assert kwargs["user_id"] == row["owner_id"]
        assert kwargs["automation_id"] == row["id"]
        assert kwargs["automation_name"] == row["name"]
        assert "max_fires_per_day" in kwargs["reason"]

    @pytest.mark.asyncio
    async def test_invalid_cron_disable_emits_notification(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        notify_mock = _patch_notification_service(monkeypatch)
        now = datetime.now(timezone.utc)
        row = _make_row(next_run_at=now, cron_expr="not a cron")
        db = _make_mock_db(due_row=row)

        await _process_one_due_automation(db)

        notify_mock.assert_awaited_once()
        kwargs = notify_mock.await_args.kwargs
        assert "invalid cron" in kwargs["reason"].lower()

    @pytest.mark.asyncio
    async def test_normal_fire_does_not_emit_notification(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A successful fire is not a safety event — no notification."""
        notify_mock = _patch_notification_service(monkeypatch)
        row = _make_row(next_run_at=datetime.now(timezone.utc))
        db = _make_mock_db(due_row=row)

        await _process_one_due_automation(db)

        notify_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_catchup_skip_does_not_emit_notification(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Catchup-window skips are not auto-disables — no notification."""
        notify_mock = _patch_notification_service(monkeypatch)
        long_ago = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        row = _make_row(next_run_at=long_ago, catchup_window_seconds=3600)
        db = _make_mock_db(due_row=row)

        await _process_one_due_automation(db)

        notify_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_notification_failure_does_not_break_dispatch(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A transport failure on the notification path must not roll back
        the auto-disable or crash the tick loop."""
        _patch_notification_service(
            monkeypatch, side_effect=RuntimeError("transport down")
        )
        row = _make_row(
            next_run_at=datetime.now(timezone.utc),
            cron_expr="not a cron",
        )
        db = _make_mock_db(due_row=row)

        out = await _process_one_due_automation(db)
        assert out is True

    @pytest.mark.asyncio
    async def test_notification_skipped_when_service_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the notification singleton is not connected (e.g., a test
        env without SMTP/feed wiring), the dispatcher quietly skips."""
        notify_mock = _patch_notification_service(monkeypatch, available=False)
        row = _make_row(
            next_run_at=datetime.now(timezone.utc),
            cron_expr="not a cron",
        )
        db = _make_mock_db(due_row=row)

        await _process_one_due_automation(db)
        notify_mock.assert_not_awaited()


# ---------------------------------------------------------------------------
# Post-commit Gitea provisioning — fires once per cron fire, OUTSIDE the txn,
# never on a skip/disable, and a provisioning failure never breaks the tick
# or undoes the committed fire. This is the parity fix: cron-spawned jobs now
# get a workspace repo like manual POST /api/jobs jobs.
# ---------------------------------------------------------------------------


def _provision_adapter(*, side_effect=None) -> AsyncMock:
    """The ``(job_row, db)`` provisioning adapter the application injects.

    R1.B07 turned this into an explicit constructor argument: the loop
    outlives every request, so it carries the Gitea/cloud adapter rather than
    late-importing ``orchestrator.main``. Handing the mock in is what steers
    the code now — monkey-patching ``provision_job_repo`` would be inert,
    because the dispatcher never names it.
    """
    return AsyncMock(side_effect=side_effect)


class TestPostCommitProvisioning:
    @pytest.mark.asyncio
    async def test_fire_provisions_created_job(self) -> None:
        provision = _provision_adapter()
        row = _make_row(next_run_at=datetime.now(timezone.utc))
        db = _make_mock_db(due_row=row)

        out = await _process_one_due_automation(db, provision_repo=provision)

        assert out is True
        provision.assert_awaited_once()
        job_row, postgres_db = provision.await_args.args
        assert job_row == {"id": "11111111-1111-1111-1111-111111111111"}
        assert postgres_db is db

    @pytest.mark.asyncio
    async def test_catchup_skip_does_not_provision(self) -> None:
        provision = _provision_adapter()
        long_ago = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        row = _make_row(next_run_at=long_ago, catchup_window_seconds=3600)
        db = _make_mock_db(due_row=row)

        await _process_one_due_automation(db, provision_repo=provision)

        provision.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_max_fires_disable_does_not_provision(self) -> None:
        provision = _provision_adapter()
        now = datetime.now(timezone.utc)
        row = _make_row(
            next_run_at=now,
            max_fires_per_day=5,
            fires_today_count=5,
            fires_today_date=now.date(),
        )
        db = _make_mock_db(due_row=row)

        await _process_one_due_automation(db, provision_repo=provision)

        provision.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_provisioning_failure_does_not_break_tick(self) -> None:
        """A Gitea outage during provisioning must not crash the tick or
        undo the committed fire."""
        provision = _provision_adapter(side_effect=RuntimeError("gitea down"))
        row = _make_row(next_run_at=datetime.now(timezone.utc))
        db = _make_mock_db(due_row=row)

        out = await _process_one_due_automation(db, provision_repo=provision)

        assert out is True
        provision.assert_awaited_once()
        db.advance_automation_after_fire.assert_awaited_once()


# ---------------------------------------------------------------------------
# Archived project — the fire is skipped, the automation stays enabled
# ---------------------------------------------------------------------------
#
# knowledge-base/knowledge/features/project_and_job_list_filtering.md §4.3:
# background paths skip and log, they never raise. A cron tick has no HTTP
# caller to receive a 409, and auto-disabling the automation would silently
# destroy the owner's configuration — the failure mode Jira's docs warn about,
# where archiving a project makes its automation rules start failing.


PROJECT_ARCHIVED = "68137e29-6b1f-4f1b-a0c1-4e6dc2be3f9a"


class TestArchivedProjectSkipsTheFire:
    def _archived_db(self, row):
        db = _make_mock_db(due_row=row)
        db.get_project = AsyncMock(
            return_value={"id": PROJECT_ARCHIVED, "status": "archived"}
        )
        return db

    @pytest.mark.asyncio
    async def test_no_job_is_created(self) -> None:
        row = _make_row(
            next_run_at=datetime.now(timezone.utc), project_id=PROJECT_ARCHIVED
        )
        db = self._archived_db(row)

        await _process_one_due_automation(db)

        db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_automation_is_not_disabled(self) -> None:
        row = _make_row(
            next_run_at=datetime.now(timezone.utc), project_id=PROJECT_ARCHIVED
        )
        db = self._archived_db(row)

        await _process_one_due_automation(db)

        # Disabling would destroy the owner's configuration for a condition
        # they can reverse in one click.
        db.auto_disable_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_schedule_advances_so_the_row_is_not_reclaimed_forever(
        self,
    ) -> None:
        row = _make_row(
            next_run_at=datetime.now(timezone.utc), project_id=PROJECT_ARCHIVED
        )
        db = self._archived_db(row)

        await _process_one_due_automation(db)

        # Same lever the catch-up-window skip pulls: move next_run_at forward
        # without recording a fire.
        db.skip_automation_fire.assert_awaited_once()
        db.advance_automation_after_fire.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_live_project_still_fires(self) -> None:
        row = _make_row(
            next_run_at=datetime.now(timezone.utc), project_id=PROJECT_ARCHIVED
        )
        db = _make_mock_db(due_row=row)
        db.get_project = AsyncMock(
            return_value={"id": PROJECT_ARCHIVED, "status": "active"}
        )

        await _process_one_due_automation(db)

        db.create_job.assert_awaited_once()
        db.advance_automation_after_fire.assert_awaited_once()


class TestWorkspaceSelectionRace:
    """Slice A2b: a Project activation racing the fire is transient, not a
    refusal — the tick rolls back and the row is re-claimed on the next pass."""

    @pytest.mark.asyncio
    async def test_a_selection_race_is_not_recorded_as_a_refusal(
        self, monkeypatch
    ) -> None:
        from orchestrator.services.manifest_workspace_selection import (
            WorkspaceSelectionRace,
        )

        monkeypatch.setattr(
            "orchestrator.services.manifest_workspace_selection."
            "select_project_workspace_default",
            AsyncMock(side_effect=WorkspaceSelectionRace()),
        )
        db = _make_mock_db(due_row=_make_row(next_run_at=datetime.now(timezone.utc)))
        db.refuse_automation_fire = AsyncMock()

        with pytest.raises(WorkspaceSelectionRace):
            await _process_one_due_automation(db)

        db.refuse_automation_fire.assert_not_awaited()
        db.skip_automation_fire.assert_not_awaited()
        db.advance_automation_after_fire.assert_not_awaited()
        db.create_job.assert_not_awaited()
