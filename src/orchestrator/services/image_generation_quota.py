"""Room on the subscription for another proxy image (image generation, D3).

The subscription proxy serves one ChatGPT/Codex quota to the whole
installation, and image generation drains it several times faster than chat.
The agent asks here before each proxy image call; the answer compares the
Codex usage windows (``subscriptions.first_codex_usage``) with the admin's
threshold (``shared.image_generation``, default 80%).

The usage read is cached briefly per process: one image job asks once per
call, and the upstream endpoint is not meant to be polled per request.
Design: knowledge-base/knowledge/features/image_generation.md §5 WP4, §6 FM1.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from shared.image_generation import (
    SYSTEM_SETTING_KEY,
    normalize_settings,
    proxy_quota_verdict,
)

#: Seconds a usage read is reused.
USAGE_CACHE_SECONDS = 60.0

_lock = asyncio.Lock()
_cache: dict[str, Any] = {"at": None, "usage": None}


async def _default_fetch() -> Mapping[str, Any] | None:
    from orchestrator.services.subscriptions import first_codex_usage

    return await first_codex_usage()


async def cached_codex_usage(
    *,
    fetch: Callable[[], Awaitable[Mapping[str, Any] | None]] | None = None,
    now: Callable[[], float] = time.monotonic,
) -> Mapping[str, Any] | None:
    """The Codex usage windows, read at most once per ``USAGE_CACHE_SECONDS``."""
    async with _lock:
        at = _cache["at"]
        if at is not None and now() - at < USAGE_CACHE_SECONDS:
            return _cache["usage"]
        try:
            usage = await (fetch or _default_fetch)()
        except Exception:
            usage = None
        _cache.update(at=now(), usage=usage)
        return usage


def reset_cache() -> None:
    """Forget the cached usage (tests)."""
    _cache.update(at=None, usage=None)


async def proxy_image_quota(
    *,
    store: Any,
    fetch: Callable[[], Awaitable[Mapping[str, Any] | None]] | None = None,
) -> dict[str, Any]:
    """Whether the proxy lane may make another image now, and why."""
    try:
        row = await store.get_system_setting(SYSTEM_SETTING_KEY)
    except Exception:
        row = None
    settings = normalize_settings((row or {}).get("value"))
    usage = await cached_codex_usage(fetch=fetch)
    return proxy_quota_verdict(
        usage, threshold_percent=settings["proxy_quota_threshold_percent"]
    )
