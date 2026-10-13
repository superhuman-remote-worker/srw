"""OpenRouter model-pricing sync → ``usage_rates`` (LLM $/token rates).

The in-process metering path materializes cost-free LLM token rows into
``usage_events`` from the ``llm_requests`` audit trail; this module seeds the
effective-dated ``usage_rates`` table so :meth:`UsageLedger.record_events` prices
those rows at write time.

Source: OpenRouter's public model catalog (``openrouter.ai/api/v1/models`` — no
auth), which publishes per-model ``pricing.prompt`` / ``pricing.completion`` /
``pricing.input_cache_read`` as USD-*per-token* decimal strings.

Mapping: each catalog model resolves to an OpenRouter price by its admin-set
``params_json.pricing_id`` (Admin → Models) when present, else by the model id
itself. Resolution (``_build_price_resolver``) tries an exact (case-insensitive)
match on the full OpenRouter id, then the once-stripped remainder as a full id
(the gateway case: ``openrouter/openai/gpt-oss-120b`` → ``openai/gpt-oss-120b``),
then as a bare suffix — so a bare ``gpt-5.6-terra`` auto-matches
``openai/gpt-5.6-terra`` with no admin mapping. A suffix shared by more than one
provider is treated as ambiguous and left unpriced (fail closed) rather than
mis-priced against the wrong provider.
``pricing_id = ""`` marks a model explicitly unpriced (self-hosted / free) → no
rate → ``cost_usd`` stays NULL.

Effective-dated + change-only: a sync inserts a NEW ``usage_rates`` row only when
the upstream price differs from the current newest rate for that (resource, unit),
so re-running every N minutes never bloats the effective-dated table and never
rewrites already-snapshotted history. Non-fatal throughout (network/DB errors log
and no-op) — same graceful-degradation posture as the rest of the metering tier.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Iterable, Optional

import asyncpg
import httpx

logger = logging.getLogger(__name__)

OPENROUTER_MODELS_URL = "https://openrouter.ai/api/v1/models"


@dataclass(frozen=True)
class LlmTokenPrices:
    """OpenRouter LLM token prices in USD/token."""

    prompt: Decimal
    completion: Decimal
    cached_prompt: Optional[Decimal] = None

    def rates(self) -> dict[str, Decimal]:
        """Usage-ledger unit → effective rate.

        Missing cache-read prices fall back to the full prompt rate. That keeps
        token accounting exact while conservatively avoiding understated costs.
        """
        return {
            "prompt-token": self.prompt,
            "completion-token": self.completion,
            "cached-prompt-token": (
                self.cached_prompt if self.cached_prompt is not None else self.prompt
            ),
        }


def _price(v: Any) -> Optional[Decimal]:
    """Parse an OpenRouter price value to a Decimal $/token.

    OpenRouter gives decimal strings in USD per token (e.g. ``"0.0000015"`` =
    $1.50/M). Returns ``None`` for absent / non-numeric / negative values (a
    negative such as ``"-1"`` signals variable/BYO pricing → treat as unpriced).
    ``"0"`` (free model) is a valid, priced rate of 0.
    """
    if v is None:
        return None
    try:
        d = Decimal(str(v))
    except (InvalidOperation, ValueError):
        return None
    return d if d >= 0 else None


def _pricing_id_for(resource: str, pricing_id: Optional[str]) -> Optional[str]:
    """Resolve the candidate id to price ``resource`` against.

    Prefers the admin-set ``params_json.pricing_id``. An explicit empty string
    means "unpriced on purpose" (self-hosted / free) → ``None``. When unset, fall
    back to the model id itself; the returned candidate is matched against the
    OpenRouter catalog by :func:`_build_price_resolver` (exact full-id first, then
    unique provider-prefix-stripped suffix), so a bare ``gpt-5.6-terra`` resolves
    to ``openai/gpt-5.6-terra`` without an admin ``pricing_id``. Set ``pricing_id``
    only for irregular names the suffix can't reach (e.g. ``gpt-5.3-codex-spark``
    → ``openai/gpt-5.3-codex``) or to force-unprice (``""``).
    """
    if pricing_id is not None:
        pid = pricing_id.strip()
        return pid or None  # "" → explicitly unpriced
    norm = resource.strip().lower()
    return norm or None


def _build_price_resolver(
    prices: dict[str, LlmTokenPrices],
) -> Callable[[Optional[str]], Optional[LlmTokenPrices]]:
    """Build a resolver ``candidate id → prices``: exact full-id, then unique suffix.

    Two indexes over the OpenRouter catalog:
    - ``by_full``: lowercased full id (``openai/gpt-5.5``) → prices, so an
      admin-set ``pricing_id`` that already carries the provider prefix — and any
      catalog ``model_id`` that is itself a full OpenRouter id — matches exactly.
    - ``by_suffix``: the provider-prefix-stripped suffix (``gpt-5.5``) → prices,
      but ONLY for suffixes that are unique across the catalog. A suffix shared by
      two providers is dropped, so an ambiguous bare name fails closed (unpriced)
      rather than being mis-priced against the wrong provider.

    Lookup order is full id, then the stripped remainder against ``by_full``, then
    against ``by_suffix``. The middle step handles GATEWAY-prefixed catalog ids —
    ``openrouter/openai/gpt-oss-120b`` is a routing prefix in front of a complete
    OpenRouter id, so one strip yields a full id rather than a bare suffix.

    This lets a bare catalog ``model_id`` (``gpt-5.6-terra``) auto-match
    ``openai/gpt-5.6-terra`` with no admin mapping, while every existing
    exact-match and force-unprice path is preserved unchanged.
    """
    by_full: dict[str, LlmTokenPrices] = {}
    suffix_counts: Counter = Counter()
    for full_id in prices:
        fl = full_id.lower()
        by_full[fl] = prices[full_id]
        suffix_counts[fl.split("/", 1)[-1]] += 1
    by_suffix: dict[str, LlmTokenPrices] = {}
    for full_id in prices:
        suffix = full_id.lower().split("/", 1)[-1]
        if suffix_counts[suffix] == 1:  # unique → safe to match by bare suffix
            by_suffix[suffix] = prices[full_id]

    def resolve(candidate: Optional[str]) -> Optional[LlmTokenPrices]:
        if not candidate:
            return None
        c = candidate.strip().lower()
        stripped = c.split("/", 1)[-1]
        # ``by_full`` on the stripped remainder is the gateway case: a catalog id
        # like ``openrouter/openai/gpt-oss-120b`` carries a ROUTING prefix in
        # front of a complete OpenRouter id. Dropping one segment leaves
        # ``openai/gpt-oss-120b``, which is a full id, not a bare suffix — so
        # without this it fell through to ``by_suffix`` (bare names only) and
        # missed, leaving OpenRouter's own models the ones auto-detection could
        # not price. Fail-closed is unaffected: ambiguity is a property of the
        # BARE suffix, and a full id is unambiguous by construction.
        return by_full.get(c) or by_full.get(stripped) or by_suffix.get(stripped)

    return resolve


async def fetch_openrouter_prices(
    *, client: Optional[httpx.AsyncClient] = None, timeout: float = 15.0
) -> dict[str, LlmTokenPrices]:
    """Fetch ``{openrouter_model_id: LlmTokenPrices}``.

    Public endpoint (no key). Models missing prompt or completion prices are
    skipped; missing cache-read price is allowed and handled conservatively by
    ``LlmTokenPrices.rates``. Non-fatal: any network/parse error logs and returns
    ``{}`` (→ sync no-ops, cost stays as last snapshotted).
    """
    owns = client is None
    client = client or httpx.AsyncClient(timeout=timeout)
    try:
        try:
            resp = await client.get(OPENROUTER_MODELS_URL)
            resp.raise_for_status()
            data = resp.json().get("data") or []
        except Exception:
            logger.warning("openrouter pricing fetch failed (non-fatal)", exc_info=True)
            return {}
    finally:
        if owns:
            await client.aclose()

    out: dict[str, LlmTokenPrices] = {}
    for m in data:
        mid = m.get("id")
        pricing = m.get("pricing") or {}
        prompt = _price(pricing.get("prompt"))
        completion = _price(pricing.get("completion"))
        cached_prompt = _price(pricing.get("input_cache_read"))
        if mid and prompt is not None and completion is not None:
            out[str(mid)] = LlmTokenPrices(prompt, completion, cached_prompt)
    return out


async def _rate_changed(
    conn: asyncpg.Connection, resource: str, unit: str, rate: Decimal
) -> bool:
    """True if ``rate`` differs from the newest ``llm`` rate for (resource, unit)."""
    cur = await conn.fetchval(
        "SELECT rate_usd FROM usage_rates "
        "WHERE category = 'llm' AND resource = $1 AND unit = $2 "
        "ORDER BY effective_from DESC LIMIT 1",
        resource,
        unit,
    )
    return cur is None or Decimal(cur) != rate


async def sync_llm_rates(
    app_pool: Optional[asyncpg.Pool],
    models: Iterable[tuple[str, Optional[str]]],
    *,
    prices: Optional[dict[str, LlmTokenPrices]] = None,
    now: Optional[datetime] = None,
) -> int:
    """Seed ``usage_rates`` LLM $/token rows for ``models`` from OpenRouter pricing.

    ``models`` = ``(resource, pricing_id)`` pairs, where ``resource`` is the exact
    string that appears in ``usage_events.resource`` (== the recorded
    ``llm_requests.model``) and ``pricing_id`` is the admin-set OpenRouter id (or
    ``None`` → heuristic). Inserts a new effective-dated row only when the price
    changed (see module docstring). Returns the count of rate rows inserted.
    Non-fatal: returns 0 when the app pool is unavailable.
    """
    if app_pool is None:
        return 0
    if prices is None:
        prices = await fetch_openrouter_prices()
    if not prices:
        return 0
    ts = now or datetime.now(timezone.utc)
    resolve = _build_price_resolver(prices)
    inserted = 0
    priced = 0
    unpriced = 0
    async with app_pool.acquire() as conn:
        for resource, pricing_id in models:
            pid = _pricing_id_for(resource, pricing_id)
            price = resolve(pid)
            if price is None:
                unpriced += 1
                continue  # unmatched / self-hosted → leave unpriced (cost = NULL)
            priced += 1
            for unit, rate in price.rates().items():
                if await _rate_changed(conn, resource, unit, rate):
                    await conn.execute(
                        "INSERT INTO usage_rates "
                        "(category, resource, unit, rate_usd, effective_from) "
                        "VALUES ('llm', $1, $2, $3, $4) "
                        "ON CONFLICT (category, resource, unit, effective_from) "
                        "DO NOTHING",
                        resource,
                        unit,
                        rate,
                        ts,
                    )
                    inserted += 1
    # Surface how many catalog resources matched a price vs. were left unpriced, so
    # a regression (e.g. a future suffix collision silently dropping a model to
    # unpriced) is visible in logs rather than invisible.
    logger.debug(
        "openrouter pricing: %d/%d catalog resource(s) matched a price; "
        "%d left unpriced (self-hosted, or ambiguous/absent OpenRouter id)",
        priced,
        priced + unpriced,
        unpriced,
    )
    if inserted:
        logger.info("openrouter pricing: inserted %d new usage_rates row(s)", inserted)
    return inserted


def _catalog_pricing_pairs(
    models: Iterable[dict],
) -> list[tuple[str, Optional[str]]]:
    """``(model_id, params_json.pricing_id)`` pairs from model-catalog rows.

    ``model_id`` is the string dispatched to the provider — exactly what lands in
    ``llm_requests.model`` / ``usage_events.resource`` — so a rate seeded under it
    is what ``record_events`` matches. ``pricing_id`` (admin-set in ``params_json``,
    Admin → Models) names the OpenRouter id to price against; absent → heuristic
    (see :func:`_pricing_id_for`), ``""`` → explicitly unpriced (self-hosted).
    """
    pairs: list[tuple[str, Optional[str]]] = []
    for m in models:
        mid = m.get("model_id")
        if not mid:
            continue
        if "live_voice" in (m.get("capabilities") or ()):
            # Billed per voice second (migration 0544), not per token:
            # OpenRouter has no price for it.
            continue
        params = m.get("params_json") or {}
        pairs.append((str(mid), params.get("pricing_id")))
    return pairs


async def sync_catalog_llm_rates(
    app_pool: Optional[asyncpg.Pool],
    list_models: Callable[[], Awaitable[list[dict]]],
    *,
    prices: Optional[dict[str, LlmTokenPrices]] = None,
    now: Optional[datetime] = None,
) -> int:
    """Seed ``usage_rates`` from the model catalog × OpenRouter. Rows inserted.

    ``list_models`` returns catalog rows (``PostgresDB.list_models``). ALL rows
    are enumerated (not just enabled) — a disabled model can still carry historical
    or ongoing recorded usage that needs a rate (e.g. a system-scoped provider row
    whose ``model_id`` matches the recorded string). Non-fatal.
    """
    if app_pool is None:
        return 0
    try:
        models = await list_models()
    except Exception:
        logger.warning("catalog list for pricing failed (non-fatal)", exc_info=True)
        return 0
    return await sync_llm_rates(
        app_pool, _catalog_pricing_pairs(models), prices=prices, now=now
    )


async def llm_pricing_sync_loop(
    shutdown_event: asyncio.Event,
    app_pool: Optional[asyncpg.Pool],
    list_models: Callable[[], Awaitable[list[dict]]],
    *,
    interval: float = 21600.0,
) -> None:
    """Background loop: refresh ``usage_rates`` from OpenRouter × catalog.

    Syncs once on entry then every ``interval`` s (default 6 h — public prices
    move slowly, and the sync is change-only so an unchanged run is nearly free).
    Non-fatal per tick; no-op when the app pool is absent.
    """
    if app_pool is None:
        logger.info("LLM pricing sync loop disabled (app pool unavailable)")
        return
    logger.info("LLM pricing sync loop starting (interval=%ss)", interval)
    try:
        while not shutdown_event.is_set():
            try:
                await sync_catalog_llm_rates(app_pool, list_models)
            except Exception:
                logger.exception("LLM pricing sync tick failed (non-fatal)")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
    finally:
        logger.info("LLM pricing sync loop stopped")


__all__ = [
    "LlmTokenPrices",
    "fetch_openrouter_prices",
    "sync_llm_rates",
    "sync_catalog_llm_rates",
    "llm_pricing_sync_loop",
    "OPENROUTER_MODELS_URL",
]
