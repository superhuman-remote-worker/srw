"""The share rate limit's bucket: blocking, bounded and non-blocking takes.

A caller that cannot block for a whole window (a session's outbox share
retry, a completion effect) bounds the wait of an adapter's own acquire
with ``bounded_wait``; past it the acquire raises ``BucketFull`` and
consumes nothing.
"""

from __future__ import annotations

import asyncio

import pytest

from orchestrator.services.cloud.retry import BucketFull, LeakyBucket, bounded_wait


@pytest.mark.asyncio
async def test_try_acquire_never_waits():
    bucket = LeakyBucket(max_events=1, window_seconds=60)
    assert await bucket.try_acquire() is True
    assert await bucket.try_acquire() is False


@pytest.mark.asyncio
async def test_a_bounded_acquire_raises_instead_of_waiting_and_consumes_nothing():
    bucket = LeakyBucket(max_events=1, window_seconds=60)
    await bucket.acquire()
    with pytest.raises(BucketFull) as caught:
        await bucket.acquire(timeout=0.01)
    assert 0 < caught.value.retry_after <= 60
    with bounded_wait(0.01), pytest.raises(BucketFull):
        await bucket.acquire()
    # Outside the block an acquire is unbounded again.
    roomy = LeakyBucket(max_events=2, window_seconds=60)
    with bounded_wait(0.0):
        await roomy.acquire()
    await roomy.acquire()
    assert await roomy.try_acquire() is False


@pytest.mark.asyncio
async def test_the_tighter_of_a_timeout_and_a_bounded_wait_applies():
    bucket = LeakyBucket(max_events=1, window_seconds=0.2)
    await bucket.acquire()
    with bounded_wait(5):
        with pytest.raises(BucketFull):
            await bucket.acquire(timeout=0.01)
        # Room comes back within the bound: the acquire waits for it.
        await asyncio.wait_for(bucket.acquire(timeout=1), timeout=2)


@pytest.mark.asyncio
async def test_an_unbounded_acquire_still_waits_for_room():
    bucket = LeakyBucket(max_events=1, window_seconds=0.05)
    await bucket.acquire()
    await asyncio.wait_for(bucket.acquire(), timeout=1)
