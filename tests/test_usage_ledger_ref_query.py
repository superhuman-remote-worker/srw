"""``UsageLedger.query_ref_usage`` — the per-job/thread read behind job cost.

Every behaviour worth pinning here lives in the SQL, so this runs against a real
Postgres: ``SUM`` over an all-NULL column, ``COUNT(*) FILTER``, the partition-key
range, and the ``ref_kind`` discriminator. A mocked asyncpg would assert the mock.

The method exists because ``query_usage`` — which ``GET /api/usage?ref_id=``
already uses — answers a *different* question well and this one badly:

  * it wraps the price in ``COALESCE(SUM(cost_usd), 0)``, so "no rate card"
    and "free" arrive identical. On the k3d ledger that is the common case, not
    a corner: zero of 374 job-attributed compute rows carry a price, because
    ``UsageRates`` seeds LLM rates from OpenRouter and no compute rates at all.
  * it applies ``V1_USAGE_COMPAT_PREDICATE``, which freezes the v1 cards to
    LLM/TTS/STT plus the original workspace CPU/RAM tuple. The infrastructure
    materializer stamps ``ref_kind`` in {job, thread} on typed VM and volume rows
    too, so that scope would silently drop a VM-backed job's machine cost.

Both divergences are asserted against ``query_usage`` directly rather than
described, so that if the shared reader ever changes, this file says which of
the two moved.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

pytest.importorskip("testcontainers.postgres")

import asyncpg  # noqa: E402
import pytest_asyncio  # noqa: E402
from testcontainers.postgres import PostgresContainer  # noqa: E402

from orchestrator.services.usage_ledger import (  # noqa: E402
    UsageLedger,
    UsageRates,
)

UTC = timezone.utc
T0 = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
WIDE = (T0 - timedelta(days=30), T0 + timedelta(days=30))

JOB = uuid.UUID("11111111-0000-0000-0000-000000000001")
KID = uuid.UUID("22222222-0000-0000-0000-000000000002")
OTHER = uuid.UUID("33333333-0000-0000-0000-000000000003")

_PARTS = [
    ("2026_02", "2026-02-01", "2026-03-01"),
    ("2026_03", "2026-03-01", "2026-04-01"),
    ("2026_04", "2026-04-01", "2026-05-01"),
]


async def _setup_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS usage_events CASCADE")
        await conn.execute(
            """
            CREATE TABLE usage_events (
                id BIGSERIAL, ts TIMESTAMPTZ NOT NULL DEFAULT now(),
                user_id UUID, project_id UUID, ref_kind TEXT, ref_id UUID,
                category TEXT NOT NULL, resource TEXT NOT NULL,
                quantity NUMERIC NOT NULL, unit TEXT NOT NULL,
                rate_usd NUMERIC, cost_usd NUMERIC,
                source TEXT NOT NULL, source_id TEXT NOT NULL,
                details JSONB NOT NULL DEFAULT '{}'::jsonb,
                PRIMARY KEY (id, ts)
            ) PARTITION BY RANGE (ts)
            """
        )
        for suffix, lo, hi in _PARTS:
            await conn.execute(
                f"CREATE TABLE usage_events_p{suffix} PARTITION OF usage_events "
                f"FOR VALUES FROM ('{lo}') TO ('{hi}')"
            )


@pytest.fixture(scope="session")
def pg_dsn():
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")


@pytest_asyncio.fixture
async def db(pg_dsn):
    pool = await asyncpg.create_pool(pg_dsn)
    await _setup_schema(pool)
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def ledger(db):
    return UsageLedger(db, UsageRates(db))


async def _insert(
    pool: asyncpg.Pool,
    *,
    ref_kind: str | None = "job",
    ref_id: uuid.UUID | None = JOB,
    category: str = "llm",
    resource: str = "MiniMax-M3",
    unit: str = "prompt-token",
    quantity: float = 1000.0,
    cost_usd: float | None = None,
    ts: datetime = T0,
    source_id: str | None = None,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO usage_events
                (ts, ref_kind, ref_id, category, resource, quantity, unit,
                 cost_usd, source, source_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'audit',$9)
            """,
            ts,
            ref_kind,
            ref_id,
            category,
            resource,
            Decimal(str(quantity)),
            unit,
            None if cost_usd is None else Decimal(str(cost_usd)),
            source_id or str(uuid.uuid4()),
        )


class TestUnpricedIsNotFree:
    @pytest.mark.asyncio
    async def test_all_null_cost_stays_none(self, db, ledger):
        """The compute case. COALESCE-ing this to 0.0 renders machine time free."""
        await _insert(
            db,
            category="compute",
            resource="workspace_pod",
            unit="vcpu-hour",
            quantity=1.25,
            cost_usd=None,
        )
        (row,) = await ledger.query_ref_usage(
            ref_kind="job", ref_ids=[str(JOB)], from_ts=WIDE[0], to_ts=WIDE[1]
        )
        assert row["cost_usd"] is None
        assert row["quantity"] == pytest.approx(1.25)
        assert row["priced_events"] == 0
        assert row["events"] == 1

    @pytest.mark.asyncio
    async def test_query_usage_reports_the_same_row_as_zero(self, db, ledger):
        """Pins the divergence: the shared reader is why this method exists."""
        await _insert(
            db,
            category="compute",
            resource="workspace_pod",
            unit="vcpu-hour",
            quantity=1.25,
            cost_usd=None,
        )
        shared = await ledger.query_usage(
            from_ts=WIDE[0], to_ts=WIDE[1], ref_id=str(JOB)
        )
        assert shared["by_category"][0]["cost_usd"] == 0.0
        assert shared["total_cost_usd"] == 0.0

    @pytest.mark.asyncio
    async def test_partial_pricing_sums_what_it_knows_and_counts_the_rest(
        self, db, ledger
    ):
        await _insert(db, cost_usd=0.25)
        await _insert(db, cost_usd=None)
        (row,) = await ledger.query_ref_usage(
            ref_kind="job", ref_ids=[str(JOB)], from_ts=WIDE[0], to_ts=WIDE[1]
        )
        assert row["cost_usd"] == pytest.approx(0.25)
        assert row["priced_events"] == 1
        assert row["events"] == 2


class TestRefKindIsLoadBearing:
    @pytest.mark.asyncio
    async def test_a_thread_row_sharing_the_id_is_excluded(self, db, ledger):
        """``ref_id`` is polymorphic: ``llm_requests.job_id`` holds a job id for
        worker rows and a *thread* id for session rows (audit_usage.py:27). Ids
        are UUIDs so a real collision is vanishing, but the filter is the only
        thing making that a probability rather than a guarantee."""
        await _insert(db, ref_kind="job", quantity=100.0)
        await _insert(db, ref_kind="thread", quantity=999.0)
        (row,) = await ledger.query_ref_usage(
            ref_kind="job", ref_ids=[str(JOB)], from_ts=WIDE[0], to_ts=WIDE[1]
        )
        assert row["quantity"] == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_other_jobs_are_excluded(self, db, ledger):
        await _insert(db, ref_id=JOB, quantity=100.0)
        await _insert(db, ref_id=OTHER, quantity=999.0)
        (row,) = await ledger.query_ref_usage(
            ref_kind="job", ref_ids=[str(JOB)], from_ts=WIDE[0], to_ts=WIDE[1]
        )
        assert row["quantity"] == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_empty_ref_set_matches_nothing_not_everything(self, db, ledger):
        """A job with no descendants must not silently cost the whole fleet."""
        await _insert(db, ref_id=JOB)
        await _insert(db, ref_id=OTHER)
        assert (
            await ledger.query_ref_usage(
                ref_kind="job", ref_ids=[], from_ts=WIDE[0], to_ts=WIDE[1]
            )
            == []
        )


class TestSubtree:
    @pytest.mark.asyncio
    async def test_parent_and_children_sum_in_one_round_trip(self, db, ledger):
        await _insert(db, ref_id=JOB, quantity=100.0, cost_usd=0.1)
        await _insert(db, ref_id=KID, quantity=50.0, cost_usd=0.05)
        await _insert(db, ref_id=OTHER, quantity=999.0, cost_usd=9.99)
        (row,) = await ledger.query_ref_usage(
            ref_kind="job",
            ref_ids=[str(JOB), str(KID)],
            from_ts=WIDE[0],
            to_ts=WIDE[1],
        )
        assert row["quantity"] == pytest.approx(150.0)
        assert row["cost_usd"] == pytest.approx(0.15)
        assert row["events"] == 2


class TestWindow:
    @pytest.mark.asyncio
    async def test_upper_bound_is_exclusive_and_lower_inclusive(self, db, ledger):
        await _insert(db, ts=T0, quantity=1.0, source_id="lo")
        await _insert(db, ts=T0 + timedelta(hours=1), quantity=2.0, source_id="hi")
        (row,) = await ledger.query_ref_usage(
            ref_kind="job",
            ref_ids=[str(JOB)],
            from_ts=T0,
            to_ts=T0 + timedelta(hours=1),
        )
        assert row["quantity"] == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_a_too_narrow_window_hides_real_spend(self, db, ledger):
        """Why the endpoint derives the window instead of taking a `days` param:
        the query cannot tell "no spend" from "wrong range", so the caller that
        picks the range is the one that has to be right."""
        await _insert(db, ts=T0, cost_usd=0.94)
        assert (
            await ledger.query_ref_usage(
                ref_kind="job",
                ref_ids=[str(JOB)],
                from_ts=T0 + timedelta(days=1),
                to_ts=T0 + timedelta(days=2),
            )
            == []
        )

    @pytest.mark.asyncio
    async def test_spans_partitions(self, db, ledger):
        await _insert(db, ts=datetime(2026, 2, 20, tzinfo=UTC), quantity=1.0)
        await _insert(db, ts=datetime(2026, 3, 20, tzinfo=UTC), quantity=2.0)
        (row,) = await ledger.query_ref_usage(
            ref_kind="job",
            ref_ids=[str(JOB)],
            from_ts=datetime(2026, 2, 1, tzinfo=UTC),
            to_ts=datetime(2026, 4, 1, tzinfo=UTC),
        )
        assert row["quantity"] == pytest.approx(3.0)


class TestScopeIsNotFrozenToV1:
    @pytest.mark.asyncio
    async def test_typed_infrastructure_rows_are_counted(self, db, ledger):
        """A VM-backed job's machine time is its cost. The v1 compat scope keeps
        only workspace_pod vcpu/gib rows, so reusing it here would drop this."""
        await _insert(
            db,
            category="compute",
            resource="vm_instance",
            unit="vcpu-hour",
            quantity=4.0,
            cost_usd=None,
        )
        rows = await ledger.query_ref_usage(
            ref_kind="job", ref_ids=[str(JOB)], from_ts=WIDE[0], to_ts=WIDE[1]
        )
        assert [r["resource"] for r in rows] == ["vm_instance"]

    @pytest.mark.asyncio
    async def test_query_usage_drops_the_same_row(self, db, ledger):
        """Pins the second divergence, so a change to the compat scope shows up
        here as a failure rather than as a quietly shrinking job cost."""
        await _insert(
            db,
            category="compute",
            resource="vm_instance",
            unit="vcpu-hour",
            quantity=4.0,
            cost_usd=None,
        )
        shared = await ledger.query_usage(
            from_ts=WIDE[0], to_ts=WIDE[1], ref_id=str(JOB)
        )
        assert shared["by_category"] == []

    @pytest.mark.asyncio
    async def test_per_model_split_falls_out_of_the_grouping(self, db, ledger):
        await _insert(db, resource="MiniMax-M3", quantity=100.0)
        await _insert(db, resource="gemma-4-moe", quantity=200.0)
        rows = await ledger.query_ref_usage(
            ref_kind="job", ref_ids=[str(JOB)], from_ts=WIDE[0], to_ts=WIDE[1]
        )
        assert {r["resource"]: r["quantity"] for r in rows} == {
            "MiniMax-M3": pytest.approx(100.0),
            "gemma-4-moe": pytest.approx(200.0),
        }


class TestLedgerFloor:
    @pytest.mark.asyncio
    async def test_returns_the_earliest_ts_for_the_source(self, db, ledger):
        await _insert(db, ts=datetime(2026, 3, 20, tzinfo=UTC))
        await _insert(db, ts=datetime(2026, 2, 20, tzinfo=UTC))
        assert await ledger.earliest_event_ts() == datetime(2026, 2, 20, tzinfo=UTC)

    @pytest.mark.asyncio
    async def test_empty_ledger_is_not_cached(self, db, ledger):
        """A NULL floor must stay re-askable or a process that starts before the
        first metered row would call every later job "predates the ledger"."""
        assert await ledger.earliest_event_ts() is None
        await _insert(db, ts=datetime(2026, 3, 20, tzinfo=UTC))
        assert await ledger.earliest_event_ts() == datetime(2026, 3, 20, tzinfo=UTC)

    @pytest.mark.asyncio
    async def test_non_null_floor_is_memoized(self, db, ledger):
        """Memoized because the honest query seq-scans every partition — no index
        leads with `source` — and the value is a historical constant."""
        await _insert(db, ts=datetime(2026, 3, 20, tzinfo=UTC))
        assert await ledger.earliest_event_ts() == datetime(2026, 3, 20, tzinfo=UTC)
        await _insert(db, ts=datetime(2026, 2, 20, tzinfo=UTC))
        assert await ledger.earliest_event_ts() == datetime(2026, 3, 20, tzinfo=UTC)


def _load_backfill_script():
    """The manually run pricing backfill, imported from ``scripts/`` (no package)."""
    import importlib.util
    from pathlib import Path

    path = Path(__file__).parents[1] / "scripts" / "backfill_llm_pricing.py"
    spec = importlib.util.spec_from_file_location("backfill_llm_pricing", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestRepricedEventsCountOnce:
    """The backfill appends a reversal + a priced replacement per unpriced row.

    Quantity and cost net out under SUM; the event count must too, or a fully
    repriced job keeps its "partially priced" caveat: dev job ``90f68127``
    went from "priced 314 of 472" to "priced 472 of 788" after its backfill.
    Runs the script's own correction SQL so a change to either side shows up.
    """

    RATE = Decimal("0.000004")

    async def _reprice(self, pool: asyncpg.Pool, resource: str, unit: str) -> None:
        script = _load_backfill_script()
        async with pool.acquire() as conn:
            # The script's ON CONFLICT names the production dedupe index.
            await conn.execute(
                "CREATE UNIQUE INDEX usage_events_dedupe "
                "ON usage_events (source, source_id, unit, ts)"
            )
            await conn.execute(script._CORRECT_SQL, resource, unit, self.RATE)

    @pytest.mark.asyncio
    async def test_ref_query_counts_each_repriced_call_once(self, db, ledger):
        for _ in range(3):
            await _insert(db, resource="claude-opus-5-5", quantity=1000.0)
        await _insert(db, resource="claude-opus-5-5", quantity=500.0, cost_usd=0.002)

        await self._reprice(db, "claude-opus-5-5", "prompt-token")
        (row,) = await ledger.query_ref_usage(
            ref_kind="job", ref_ids=[str(JOB)], from_ts=WIDE[0], to_ts=WIDE[1]
        )

        assert row["events"] == 4
        assert row["priced_events"] == 4
        assert row["quantity"] == pytest.approx(3500.0)
        assert row["cost_usd"] == pytest.approx(3000 * 0.000004 + 0.002)

    @pytest.mark.asyncio
    async def test_rerunning_the_backfill_changes_nothing(self, db, ledger):
        await _insert(db, resource="claude-opus-5-5", quantity=1000.0)
        await self._reprice(db, "claude-opus-5-5", "prompt-token")
        script = _load_backfill_script()
        async with db.acquire() as conn:
            await conn.execute(
                script._CORRECT_SQL, "claude-opus-5-5", "prompt-token", self.RATE
            )
        (row,) = await ledger.query_ref_usage(
            ref_kind="job", ref_ids=[str(JOB)], from_ts=WIDE[0], to_ts=WIDE[1]
        )
        assert (row["events"], row["priced_events"]) == (1, 1)

    @pytest.mark.asyncio
    async def test_shared_usage_reader_counts_them_once_too(self, db, ledger):
        for _ in range(2):
            await _insert(db, resource="claude-opus-5-5", quantity=1000.0)
        await self._reprice(db, "claude-opus-5-5", "prompt-token")
        shared = await ledger.query_usage(
            from_ts=WIDE[0], to_ts=WIDE[1], ref_id=str(JOB)
        )
        (llm,) = shared["by_category"]
        assert llm["events"] == 2
        assert llm["cost_usd"] == pytest.approx(2000 * 0.000004)
