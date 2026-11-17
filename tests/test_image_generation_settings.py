"""Image generation limits (D1 cap, D3 proxy quota threshold) and the
agent-facing quota read (image generation WP4)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

from orchestrator.services import image_generation_quota, system_settings
from shared.image_generation import (
    DEFAULT_MAX_PAID_IMAGES,
    DEFAULT_PROXY_QUOTA_THRESHOLD_PERCENT,
    normalize_settings,
    proxy_quota_verdict,
    settings_problem,
)


def test_defaults_and_bounds():
    assert normalize_settings(None) == {
        "max_paid_images": DEFAULT_MAX_PAID_IMAGES,
        "proxy_quota_threshold_percent": DEFAULT_PROXY_QUOTA_THRESHOLD_PERCENT,
    }
    assert DEFAULT_MAX_PAID_IMAGES == 20
    assert DEFAULT_PROXY_QUOTA_THRESHOLD_PERCENT == 80
    assert normalize_settings({"max_paid_images": 0})["max_paid_images"] == 0
    # Out of range or the wrong type falls back to the default, never crashes.
    assert normalize_settings({"max_paid_images": -1})["max_paid_images"] == 20
    assert normalize_settings({"max_paid_images": True})["max_paid_images"] == 20
    assert (
        normalize_settings({"proxy_quota_threshold_percent": 0})[
            "proxy_quota_threshold_percent"
        ]
        == 80
    )


@pytest.mark.parametrize(
    "body, ok",
    [
        ({"max_paid_images": 5}, True),
        ({"proxy_quota_threshold_percent": 50}, True),
        ({"max_paid_images": "5"}, False),
        ({"max_paid_images": 10_001}, False),
        ({"proxy_quota_threshold_percent": 101}, False),
        ({"proxy_quota_threshold_percent": True}, False),
        ({"something_else": 1}, False),
    ],
)
def test_admin_body_validation(body, ok):
    assert (settings_problem(body) is None) is ok


def _usage(primary=10, secondary=None, limit_reached=False):
    return {
        "plan_type": "plus",
        "limit_reached": limit_reached,
        "primary": {"used_percent": primary, "reset_after_seconds": 100},
        "secondary": None
        if secondary is None
        else {"used_percent": secondary, "reset_after_seconds": 1000},
    }


def test_quota_verdict():
    assert proxy_quota_verdict(None, threshold_percent=80)["allowed"] is True
    assert proxy_quota_verdict(_usage(10), threshold_percent=80)["allowed"] is True
    at = proxy_quota_verdict(_usage(80), threshold_percent=80)
    assert at["allowed"] is False and at["reason"] == "window_at_threshold"
    weekly = proxy_quota_verdict(_usage(10, 95), threshold_percent=80)
    assert weekly["allowed"] is False
    reached = proxy_quota_verdict(_usage(10, limit_reached=True), threshold_percent=99)
    assert reached["allowed"] is False and reached["reason"] == "limit_reached"
    # The dev free plan seen on 2026-11-17: 100% used, limit reached.
    assert (
        proxy_quota_verdict(
            {
                "plan_type": "free",
                "limit_reached": True,
                "primary": {"used_percent": 100},
            },
            threshold_percent=80,
        )["allowed"]
        is False
    )


def test_quota_read_uses_the_admin_threshold_and_caches_usage():
    image_generation_quota.reset_cache()
    store = Mock()
    store.get_system_setting = AsyncMock(
        return_value={"value": {"proxy_quota_threshold_percent": 50}}
    )
    fetch = AsyncMock(return_value=_usage(60))
    first = asyncio.run(
        image_generation_quota.proxy_image_quota(store=store, fetch=fetch)
    )
    assert first["allowed"] is False and first["threshold_percent"] == 50
    asyncio.run(image_generation_quota.proxy_image_quota(store=store, fetch=fetch))
    assert fetch.await_count == 1
    image_generation_quota.reset_cache()


def test_a_failing_usage_read_allows_with_a_reason():
    image_generation_quota.reset_cache()
    store = Mock()
    store.get_system_setting = AsyncMock(return_value=None)
    fetch = AsyncMock(side_effect=RuntimeError("proxy down"))
    verdict = asyncio.run(
        image_generation_quota.proxy_image_quota(store=store, fetch=fetch)
    )
    assert verdict == {"allowed": True, "reason": "usage_unavailable"}
    image_generation_quota.reset_cache()


def test_settings_service_round_trip():
    stored: dict = {}

    async def get(key):
        return stored.get(key)

    async def upsert(key, value, *, updated_by):
        stored[key] = {"value": value, "updated_by": updated_by, "updated_at": None}
        return stored[key]

    store = Mock(get_system_setting=get, upsert_system_setting=upsert)
    deps = system_settings.SystemSettingsDependencies(store=store)
    got = asyncio.run(system_settings.get_image_generation_settings(dependencies=deps))
    assert got["max_paid_images"] == 20 and got["proxy_quota_threshold_percent"] == 80
    put = asyncio.run(
        system_settings.put_image_generation_settings(
            body={"proxy_quota_threshold_percent": 60},
            admin={"email": "admin@localhost"},
            dependencies=deps,
        )
    )
    assert put["proxy_quota_threshold_percent"] == 60
    assert put["max_paid_images"] == 20
    with pytest.raises(HTTPException):
        asyncio.run(
            system_settings.put_image_generation_settings(
                body={"max_paid_images": -3},
                admin={"email": "admin@localhost"},
                dependencies=deps,
            )
        )
