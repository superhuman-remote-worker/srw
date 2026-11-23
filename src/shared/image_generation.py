"""Image generation contracts shared by the orchestrator and the agent.

Framework-free on purpose: the orchestrator reads the installation's image
settings and injects them into the ``image`` dispatch section; the agent's
``generate_image`` tool enforces them. Design:
knowledge-base/knowledge/features/image_generation.md (§5 WP4, §9 D1/D3).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

#: ``system_settings`` key of the installation's image generation settings.
SYSTEM_SETTING_KEY = "image_generation"

#: Paid images one job or Session may generate (owner decision D1,
#: 2026-11-17). ``0`` switches the cap off. Subscription-proxy images are not
#: counted here; they are guarded by the quota threshold instead.
DEFAULT_MAX_PAID_IMAGES = 20
MAX_PAID_IMAGES_LIMIT = 10_000

#: Share of the subscription's 5-hour or weekly window above which the proxy
#: lane refuses new images (owner decision D3: configurable, default 80).
DEFAULT_PROXY_QUOTA_THRESHOLD_PERCENT = 80

#: Key under ``jobs.context`` / ``threads.metadata`` that holds the counter.
COUNTER_CONTEXT_KEY = "image_generation"
COUNTER_FIELD = "paid_images"

#: Billing labels of the ``image`` section.
BILLING_API = "api"
BILLING_SUBSCRIPTION = "subscription"


def _bounded_int(value: Any, *, default: int, low: int, high: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    if number < low or number > high:
        return default
    return number


def normalize_settings(value: Any) -> dict[str, int]:
    """The effective settings for a stored value (absent or invalid → defaults)."""
    stored = value if isinstance(value, Mapping) else {}
    return {
        "max_paid_images": _bounded_int(
            stored.get("max_paid_images"),
            default=DEFAULT_MAX_PAID_IMAGES,
            low=0,
            high=MAX_PAID_IMAGES_LIMIT,
        ),
        "proxy_quota_threshold_percent": _bounded_int(
            stored.get("proxy_quota_threshold_percent"),
            default=DEFAULT_PROXY_QUOTA_THRESHOLD_PERCENT,
            low=1,
            high=100,
        ),
    }


def settings_problem(body: Mapping[str, Any]) -> str | None:
    """Why an admin's settings body is invalid, or ``None``."""
    allowed = {"max_paid_images", "proxy_quota_threshold_percent"}
    unknown = sorted(set(body) - allowed)
    if unknown:
        return f"unknown setting(s): {', '.join(unknown)}"
    if "max_paid_images" in body:
        value = body["max_paid_images"]
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= MAX_PAID_IMAGES_LIMIT
        ):
            return (
                f"max_paid_images must be an integer from 0 to {MAX_PAID_IMAGES_LIMIT}"
            )
    if "proxy_quota_threshold_percent" in body:
        value = body["proxy_quota_threshold_percent"]
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 1 <= value <= 100
        ):
            return "proxy_quota_threshold_percent must be an integer from 1 to 100"
    return None


def proxy_quota_verdict(
    usage: Mapping[str, Any] | None, *, threshold_percent: int
) -> dict[str, Any]:
    """Whether the subscription has room for another proxy image.

    ``usage`` is the shape of ``orchestrator.services.subscriptions.fetch_codex_usage``.
    Unknown usage allows the call (the proxy itself still refuses an exhausted
    plan); a reached limit or a window at or above the threshold refuses it.
    """
    if not usage:
        return {"allowed": True, "reason": "usage_unavailable"}
    windows: list[dict[str, Any]] = []
    for name in ("primary", "secondary"):
        window = usage.get(name)
        if isinstance(window, Mapping) and window.get("used_percent") is not None:
            windows.append(
                {
                    "window": name,
                    "used_percent": window.get("used_percent"),
                    "reset_after_seconds": window.get("reset_after_seconds"),
                }
            )
    verdict: dict[str, Any] = {
        "allowed": True,
        "reason": "ok",
        "threshold_percent": threshold_percent,
        "windows": windows,
        "plan_type": usage.get("plan_type"),
    }
    if usage.get("limit_reached"):
        verdict.update(allowed=False, reason="limit_reached")
        return verdict
    for window in windows:
        try:
            used = float(window["used_percent"])
        except (TypeError, ValueError):
            continue
        if used >= threshold_percent:
            verdict.update(allowed=False, reason="window_at_threshold")
            return verdict
    return verdict


# --- list prices --------------------------------------------------------------
#
# USD per 1M tokens, read 2026-11-17 from the providers' own pricing pages
# (developers.openai.com/api/docs/pricing; aistudio.google.com/docs/pricing;
# knowledge-base research/image_generation reports reference_1 E15 and
# reference_2 E24). Image models bill tokens, not images: output image tokens
# scale with size and quality, edits add input image tokens, Gemini adds
# always-on thinking (billed as output text). Keys: input_text, input_image,
# output_image, output_text. Matched by id prefix, longest first.
IMAGE_TOKEN_LIST_PRICES: dict[str, dict[str, float]] = {
    "gpt-image-1.5": {
        "input_text": 5.0,
        "input_image": 8.0,
        "output_image": 32.0,
        "output_text": 10.0,
    },
    "gpt-image-2": {"input_text": 5.0, "input_image": 8.0, "output_image": 30.0},
    "gemini-nano-banana-2.1": {
        "input_text": 1.5,
        "input_image": 1.5,
        "output_image": 30.0,
        "output_text": 7.5,
    },
    "gemini-3-pro-image": {
        "input_text": 2.0,
        "input_image": 2.0,
        "output_image": 120.0,
        "output_text": 12.0,
    },
    "gemini-3.1-flash-lite-image": {
        "input_text": 0.25,
        "input_image": 0.25,
        "output_image": 30.0,
        "output_text": 1.5,
    },
    "gemini-3.1-flash-image": {
        "input_text": 0.5,
        "input_image": 0.5,
        "output_image": 60.0,
        "output_text": 3.0,
    },
    "gemini-2.5-flash-image": {
        "input_text": 0.3,
        "input_image": 0.3,
        "output_image": 30.0,
    },
}

_USAGE_TO_PRICE = {
    "input_text_tokens": "input_text",
    "input_image_tokens": "input_image",
    "output_image_tokens": "output_image",
    "output_text_tokens": "output_text",
}


def image_list_price_usd(model: str, usage: Mapping[str, Any] | None) -> float | None:
    """The list price of one image call from its token usage, or ``None``.

    ``None`` when the model has no list price here or the provider reported
    no usage: the row stays unpriced, so an admin's ``usage_rates`` entry
    (category ``llm``, unit ``image``) can still price it per image.
    """
    if not usage:
        return None
    name = model.strip().lower().rsplit("/", 1)[-1]
    prices = next(
        (
            IMAGE_TOKEN_LIST_PRICES[prefix]
            for prefix in sorted(IMAGE_TOKEN_LIST_PRICES, key=len, reverse=True)
            if name.startswith(prefix)
        ),
        None,
    )
    if prices is None:
        return None
    total = 0.0
    counted = False
    for usage_key, price_key in _USAGE_TO_PRICE.items():
        tokens = usage.get(usage_key)
        if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens <= 0:
            continue
        rate = prices.get(price_key)
        if rate is None:
            continue
        total += tokens * rate / 1_000_000
        counted = True
    return round(total, 6) if counted else None
