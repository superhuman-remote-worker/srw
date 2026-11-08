"""``UsageRates``: the per-process rate cache follows new rates quickly.

An event is priced once, when it is written, so a rate the pricing sync
inserts must not stay invisible for the cache's whole TTL: a miss reloads the
table when the cache is older than ``miss_reload_s``, a hit never does, and
``invalidate`` (called by the sync on its own replica) drops the cache.
Fake pool and clock; no database.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from orchestrator.services.usage_ledger import UsageRates

TS = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)
SEED = TS - timedelta(minutes=5)


def _rate(resource: str, rate: str, *, unit: str = "prompt-token") -> dict:
    return {
        "category": "llm",
        "resource": resource,
        "unit": unit,
        "rate_usd": Decimal(rate),
        "effective_from": SEED,
    }


class _Pool:
    """``usage_rates`` as a list; counts the full-table reads."""

    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.loads = 0

    async def fetch(self, _sql):
        self.loads += 1
        return [dict(r) for r in self.rows]


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _rates(pool: _Pool, clock: _Clock) -> UsageRates:
    return UsageRates(pool, ttl_s=300.0, miss_reload_s=30.0, clock=clock)


@pytest.mark.asyncio
async def test_a_rate_inserted_after_the_load_resolves_on_the_next_miss_after_30s():
    pool, clock = _Pool([_rate("openrouter/x/y", "0.000001")]), _Clock()
    rates = _rates(pool, clock)
    assert await rates.resolve("llm", "openrouter/a/b", "prompt-token", TS) is None
    assert pool.loads == 1

    pool.rows.append(_rate("openrouter/a/b", "0.000002"))  # the sync, elsewhere
    clock.now += 10
    assert await rates.resolve("llm", "openrouter/a/b", "prompt-token", TS) is None
    assert pool.loads == 1, "a miss within 30 s of the load does not reload"

    clock.now += 25  # 35 s after the load
    assert await rates.resolve("llm", "openrouter/a/b", "prompt-token", TS) == (
        Decimal("0.000002")
    )
    assert pool.loads == 2


@pytest.mark.asyncio
async def test_a_hit_does_not_reload():
    pool, clock = _Pool([_rate("openrouter/x/y", "0.000001")]), _Clock()
    rates = _rates(pool, clock)
    for _ in range(3):
        assert await rates.resolve("llm", "openrouter/x/y", "prompt-token", TS) == (
            Decimal("0.000001")
        )
        clock.now += 100  # past the miss window, inside the TTL
    assert pool.loads == 1


@pytest.mark.asyncio
async def test_misses_reload_at_most_once_per_window():
    pool, clock = _Pool(), _Clock()
    rates = _rates(pool, clock)
    clock.now += 31
    for _ in range(5):
        assert await rates.resolve("llm", "self-hosted", "prompt-token", TS) is None
    # The first lookup loads; the misses in this window reload nothing more.
    assert pool.loads == 1
    clock.now += 31
    assert await rates.resolve("llm", "self-hosted", "prompt-token", TS) is None
    assert pool.loads == 2


@pytest.mark.asyncio
async def test_the_ttl_still_reloads_a_hit():
    pool, clock = _Pool([_rate("openrouter/x/y", "0.000001")]), _Clock()
    rates = _rates(pool, clock)
    await rates.resolve("llm", "openrouter/x/y", "prompt-token", TS)
    pool.rows = [_rate("openrouter/x/y", "0.000003")]
    clock.now += 301
    assert await rates.resolve("llm", "openrouter/x/y", "prompt-token", TS) == (
        Decimal("0.000003")
    )
    assert pool.loads == 2


@pytest.mark.asyncio
async def test_invalidate_reloads_on_the_next_lookup():
    pool, clock = _Pool([_rate("openrouter/x/y", "0.000001")]), _Clock()
    rates = _rates(pool, clock)
    await rates.resolve("llm", "openrouter/x/y", "prompt-token", TS)
    pool.rows = [_rate("openrouter/x/y", "0.000004")]
    rates.invalidate()
    assert await rates.resolve("llm", "openrouter/x/y", "prompt-token", TS) == (
        Decimal("0.000004")
    )
    assert pool.loads == 2


@pytest.mark.asyncio
async def test_a_rate_dated_after_the_event_is_still_a_miss():
    pool, clock = _Pool([_rate("openrouter/x/y", "0.000001")]), _Clock()
    rates = _rates(pool, clock)
    assert (
        await rates.resolve(
            "llm", "openrouter/x/y", "prompt-token", SEED - timedelta(seconds=1)
        )
        is None
    )


@pytest.mark.asyncio
async def test_no_pool_is_unpriced_and_never_loads():
    rates = UsageRates(None)
    assert await rates.resolve("llm", "openrouter/x/y", "prompt-token", TS) is None
