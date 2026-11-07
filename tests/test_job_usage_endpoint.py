"""``GET /api/jobs/{job_id}/usage`` — the per-job cost read.

The data this endpoint serves already existed: ``services/audit_usage.py`` has
been stamping ``ref_kind='job'`` / ``ref_id`` onto every metered LLM row since
the ledger shipped, and ``GET /api/usage?ref_id=`` reads it. What did not exist
was a *safe* read, and the three unsafe edges are what this file pins:

1. **The window.** ``usage_events`` is partitioned on ``ts``, so a per-job read
   must carry a range — and the shared endpoint takes that range from a ``days``
   parameter the caller picks. Verified live before this endpoint existed:
   ``/api/usage?ref_id=<job>&days=1`` returned ``total_cost_usd: 0`` with
   ``available: true`` for a job that had cost $0.94 the week before. A zero that
   confident is worse than an error, so the window is derived from the job here
   and the tail deliberately runs to *now* rather than ``completed_at``.
2. **Unpriced is not free.** ``UsageLedger.query_usage`` wraps the price in
   ``COALESCE(SUM(cost_usd), 0)``, so a model with no rate card and a workspace
   pod with no rate card both meter as $0.00. On the k3d ledger that is not a
   corner case — 960 of 23,716 job-attributed events carried a price, and *zero*
   of the 374 compute rows did.
3. **Three empty states.** The materializer's cursor is forward-only and anchored
   when it first ran, so jobs older than the ledger have no rows and never will
   (107 of 149 k3d jobs had usage). "Never measured" is a different claim from
   "measured zero", and only one of them is $0.00.

The DB half needs a real Postgres because every one of those behaviours is in the
SQL — ``SUM`` over all-NULL, ``COUNT(*) FILTER``, the partition-key range, and
the ``ref_kind`` discriminator. Mocking asyncpg would assert the mock.
"""

from __future__ import annotations

import inspect
import uuid
from datetime import datetime, timedelta, timezone
import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from orchestrator.routers import usage_reporting as usage_reporting_routes
from orchestrator.services import usage_reporting as m

UTC = timezone.utc
NOW = datetime(2026, 8, 21, 12, 0, 0, tzinfo=UTC)
SLACK = m.JOB_USAGE_WINDOW_SLACK


def _row(
    category: str = "llm",
    resource: str = "MiniMax-M3",
    unit: str = "prompt-token",
    quantity: float = 100.0,
    cost_usd: float | None = 0.5,
    events: int = 2,
    priced_events: int | None = None,
) -> dict[str, Any]:
    return {
        "category": category,
        "resource": resource,
        "unit": unit,
        "quantity": quantity,
        "cost_usd": cost_usd,
        "events": events,
        "priced_events": events if priced_events is None else priced_events,
    }


# ---------------------------------------------------------------------------
# _job_usage_window — the range that decides whether the answer is a lie.
# ---------------------------------------------------------------------------


class TestJobUsageWindow:
    def test_brackets_creation_with_slack_at_both_ends(self):
        created = NOW - timedelta(hours=3)
        from_ts, to_ts = m.job_usage_window(created, NOW, None)
        assert from_ts == created - SLACK
        assert to_ts == NOW + SLACK

    def test_tail_is_now_not_completion(self):
        """A window ending at completion drops teardown compute and async aux calls.

        The pod's vcpu-hour/gib-hour rows close when the workspace is torn down,
        and memory extraction / summarization fire after the graph stops. Both
        land after ``completed_at`` and both are the job's cost.
        """
        created = NOW - timedelta(days=30)
        _, to_ts = m.job_usage_window(created, NOW, None)
        assert to_ts > NOW
        # The job sealed 29 days ago; a completed_at-bounded window would end there.
        assert to_ts - (created + timedelta(days=1)) > timedelta(days=28)

    def test_missing_creation_stamp_widens_to_the_ledger_floor(self):
        floor = NOW - timedelta(days=200)
        from_ts, _ = m.job_usage_window(None, NOW, floor)
        assert from_ts == floor

    def test_missing_creation_stamp_and_no_floor_falls_back_to_a_year(self):
        from_ts, _ = m.job_usage_window(None, NOW, None)
        assert from_ts == NOW - timedelta(days=365)


# ---------------------------------------------------------------------------
# _job_usage_state — which kind of "nothing" an empty result is.
# ---------------------------------------------------------------------------


class TestJobUsageState:
    def test_rows_present_is_measured(self):
        assert m.job_usage_state([_row()], NOW, None) == "measured"

    def test_job_older_than_the_ledger_predates_it(self):
        floor = NOW - timedelta(days=10)
        created = NOW - timedelta(days=40)
        assert m.job_usage_state([], created, floor) == "predates_ledger"

    def test_job_inside_the_ledger_with_no_rows_really_spent_nothing(self):
        floor = NOW - timedelta(days=40)
        created = NOW - timedelta(days=10)
        assert m.job_usage_state([], created, floor) == "no_usage"

    def test_job_exactly_at_the_floor_is_not_predating(self):
        floor = NOW - timedelta(days=10)
        assert m.job_usage_state([], floor, floor) == "no_usage"

    def test_no_floor_never_claims_predates(self):
        """An empty ledger has no floor to compare against — don't invent one."""
        assert m.job_usage_state([], NOW - timedelta(days=999), None) == "no_usage"

    def test_rows_win_even_when_the_job_predates_the_floor(self):
        floor = NOW - timedelta(days=10)
        created = NOW - timedelta(days=40)
        assert m.job_usage_state([_row()], created, floor) == "measured"


# ---------------------------------------------------------------------------
# _fold_job_usage — the arithmetic that must not turn unknown into free.
# ---------------------------------------------------------------------------


class TestFoldJobUsage:
    def test_no_rows_reports_unknown_cost_not_zero(self):
        folded = m.fold_job_usage([])
        assert folded["cost"]["usd"] is None
        assert folded["cost"]["events"] == 0
        assert folded["llm"]["total_tokens"] == 0

    def test_entirely_unpriced_usage_reports_none_not_zero(self):
        """The compute case: 374 measured k3d rows, not one of them priced.

        Summing them to 0.0 would render real machine time as free, which is the
        single easiest way to make a cost feature untrustworthy.
        """
        rows = [
            _row(
                category="compute",
                resource="workspace_pod",
                unit="vcpu-hour",
                quantity=1.2,
                cost_usd=None,
                events=1,
                priced_events=0,
            ),
            _row(
                category="compute",
                resource="workspace_pod",
                unit="gib-hour",
                quantity=2.4,
                cost_usd=None,
                events=1,
                priced_events=0,
            ),
        ]
        folded = m.fold_job_usage(rows)
        assert folded["cost"]["usd"] is None
        assert folded["cost"]["complete"] is False
        assert folded["cost"]["priced_events"] == 0
        assert folded["cost"]["events"] == 2

    def test_partial_pricing_is_flagged_incomplete(self):
        """A partially priced job's real cost is strictly above what is shown."""
        rows = [
            _row(unit="prompt-token", cost_usd=0.25, events=4, priced_events=4),
            _row(
                category="compute",
                resource="workspace_pod",
                unit="vcpu-hour",
                quantity=1.0,
                cost_usd=None,
                events=1,
                priced_events=0,
            ),
        ]
        folded = m.fold_job_usage(rows)
        assert folded["cost"]["usd"] == 0.25
        assert folded["cost"]["complete"] is False
        assert folded["cost"]["priced_events"] == 4
        assert folded["cost"]["events"] == 5

    def test_fully_priced_usage_is_complete(self):
        rows = [_row(cost_usd=0.25, events=4, priced_events=4)]
        assert m.fold_job_usage(rows)["cost"]["complete"] is True

    def test_token_buckets_and_cache_ratio(self):
        rows = [
            _row(unit="prompt-token", quantity=1000.0),
            _row(unit="cached-prompt-token", quantity=9000.0),
            _row(unit="completion-token", quantity=500.0),
        ]
        llm = m.fold_job_usage(rows)["llm"]
        assert llm["prompt_tokens"] == 1000
        assert llm["cached_prompt_tokens"] == 9000
        assert llm["completion_tokens"] == 500
        assert llm["total_tokens"] == 10500
        assert llm["cache_hit_ratio"] == pytest.approx(0.9)

    def test_non_llm_quantities_never_enter_the_token_count(self):
        """gib-hours are not tokens; a naive sum over quantity would say they are."""
        rows = [
            _row(unit="prompt-token", quantity=100.0),
            _row(
                category="compute",
                resource="workspace_pod",
                unit="gib-hour",
                quantity=999.0,
                cost_usd=None,
                priced_events=0,
            ),
        ]
        assert m.fold_job_usage(rows)["llm"]["total_tokens"] == 100

    def test_unknown_llm_unit_is_not_counted_as_tokens(self):
        """`request` rows are real; reasoning tokens ride in details, not as a unit.

        Either one summed into the token total would inflate it — reasoning
        tokens are a *subset* of the completion count (see audit_usage.py).
        """
        rows = [
            _row(unit="prompt-token", quantity=100.0),
            _row(unit="request", quantity=59.0),
        ]
        assert m.fold_job_usage(rows)["llm"]["total_tokens"] == 100

    def test_by_category_keeps_unknown_separate_from_zero(self):
        rows = [
            _row(category="llm", cost_usd=0.5, events=2, priced_events=2),
            _row(
                category="compute",
                resource="workspace_pod",
                unit="vcpu-hour",
                cost_usd=None,
                events=1,
                priced_events=0,
            ),
        ]
        cats = {c["category"]: c for c in m.fold_job_usage(rows)["by_category"]}
        assert cats["llm"]["cost_usd"] == 0.5
        assert cats["compute"]["cost_usd"] is None
        assert cats["compute"]["events"] == 1


# ---------------------------------------------------------------------------
# The route itself: composition, degradation, and the subtree fan-out.
# ---------------------------------------------------------------------------


@pytest.fixture
def route_env():
    """Build the route's dependency bundle over fakes and hand back the knobs.

    The bundle is rebuilt per call so a test that swaps a collaborator on the
    returned handle is read at request time — exactly how the application's own
    factory resolves its late-bound metering singletons.
    """
    job_id = str(uuid.uuid4())
    job = {
        "id": job_id,
        "status": "completed",
        "created_at": NOW - timedelta(hours=2),
    }
    ledger = SimpleNamespace(
        is_available=True,
        earliest_event_ts=AsyncMock(return_value=NOW - timedelta(days=60)),
        query_ref_usage=AsyncMock(return_value=[]),
    )
    db = SimpleNamespace(get_job_descendant_ids=AsyncMock(return_value=[]))

    def dependencies():
        return usage_reporting_routes.UsageReportingDependencies(
            store=db,
            reports=m.UsageReportingDependencies(
                store=db,
                audit_reader=SimpleNamespace(is_available=False),
                logger=logging.getLogger("test-job-usage"),
                usage_ledger=ledger,
            ),
            require_admin=AsyncMock(),
            metering_settings=SimpleNamespace(v2_reads_enabled=False),
            require_job_access=AsyncMock(return_value=({"id": "u"}, job)),
        )

    async def call(**kwargs):
        # Called directly, so FastAPI never resolves the declared defaults and an
        # unpassed `include_subjobs` would arrive as a (truthy!) Query object.
        # The declared default is pinned separately, in test_default_scope_is_own.
        kwargs.setdefault("include_subjobs", False)
        return await usage_reporting_routes.get_job_usage(
            SimpleNamespace(), job_id, dependencies=dependencies(), **kwargs
        )

    return SimpleNamespace(job_id=job_id, job=job, ledger=ledger, db=db, call=call)


class TestRoute:
    def test_default_scope_is_own(self):
        """A parent's own spend, not its subtree — the caller opts into the sum.

        Pinned off the signature because a route called directly bypasses
        FastAPI's default resolution, and `Query(default=False)` is truthy.
        """
        default = (
            inspect.signature(usage_reporting_routes.get_job_usage)
            .parameters["include_subjobs"]
            .default
        )
        assert default.default is False

    @pytest.mark.asyncio
    async def test_reads_the_job_window_not_a_caller_supplied_one(self, route_env):
        await route_env.call()
        kwargs = route_env.ledger.query_ref_usage.await_args.kwargs
        assert kwargs["ref_kind"] == "job"
        assert kwargs["ref_ids"] == [route_env.job_id]
        # Brackets creation, and reaches past it rather than stopping at the seal.
        assert kwargs["from_ts"] < route_env.job["created_at"]
        assert kwargs["to_ts"] > route_env.job["created_at"]

    @pytest.mark.asyncio
    async def test_audit_tier_off_is_unavailable_not_zero(self, route_env):
        route_env.ledger.is_available = False
        out = await route_env.call()
        assert out["state"] == "unavailable"
        assert out["cost"]["usd"] is None
        # Same keys as a measured response — a UI must not have to branch on shape.
        assert {"llm", "by_category", "cost", "rows", "window"} <= set(out)
        # ...and the window is a real window, not a null that only *looks* like
        # the same shape. It comes from the job, so metering being off cannot
        # make it unanswerable. This endpoint's own live check indexed straight
        # into it and crashed when it was None.
        assert out["window"]["from"].endswith("Z")
        assert out["window"]["to"].endswith("Z")

    @pytest.mark.asyncio
    async def test_ledger_outage_does_not_fabricate_a_cost(self, route_env):
        """query_ref_usage swallows its own errors and returns []; that must not
        become "$0.00 measured" — the state carries the caveat instead."""
        route_env.ledger.query_ref_usage = AsyncMock(return_value=[])
        out = await route_env.call()
        assert out["cost"]["usd"] is None
        assert out["state"] == "no_usage"

    @pytest.mark.asyncio
    async def test_subtree_fans_out_to_descendants(self, route_env):
        kids = [str(uuid.uuid4()), str(uuid.uuid4())]
        route_env.db.get_job_descendant_ids = AsyncMock(return_value=kids)
        out = await route_env.call(include_subjobs=True)
        assert out["scope"] == "subtree"
        assert out["job_count"] == 3
        assert route_env.ledger.query_ref_usage.await_args.kwargs["ref_ids"] == [
            route_env.job_id,
            *kids,
        ]

    @pytest.mark.asyncio
    async def test_own_scope_never_pulls_children(self, route_env):
        route_env.db.get_job_descendant_ids = AsyncMock(
            side_effect=AssertionError("descendants fetched for scope=job")
        )
        out = await route_env.call()
        assert out["scope"] == "job"
        assert out["job_count"] == 1

    @pytest.mark.asyncio
    async def test_running_job_is_flagged_live_with_its_lag(self, route_env):
        route_env.job["status"] = "processing"
        out = await route_env.call()
        assert out["freshness"]["live"] is True
        # 120s materializer poll + 60s aging window: a live figure is behind.
        assert out["freshness"]["lag_seconds"] == 180

    @pytest.mark.asyncio
    async def test_sealed_job_is_not_live(self, route_env):
        out = await route_env.call()
        assert out["freshness"]["live"] is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status", ["pending_review", "paused", "waiting_for_reply"]
    )
    async def test_job_waiting_on_a_human_is_not_live(self, route_env, status):
        """Not terminal, but nothing runs: the panel must not say 'still running'."""
        route_env.job["status"] = status
        out = await route_env.call()
        assert out["freshness"]["live"] is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["created", "reviewing", "waiting"])
    async def test_job_the_system_still_works_is_live(self, route_env, status):
        route_env.job["status"] = status
        out = await route_env.call()
        assert out["freshness"]["live"] is True

    @pytest.mark.asyncio
    async def test_window_is_zulu_not_plus_offset(self, route_env):
        """A `+00:00` offset re-encodes as a space in a query string (422 on the
        round trip); the jobs list hit exactly that with its `as_of` watermark."""
        out = await route_env.call()
        assert out["window"]["from"].endswith("Z")
        assert out["window"]["to"].endswith("Z")
        assert out["freshness"]["as_of"].endswith("Z")
