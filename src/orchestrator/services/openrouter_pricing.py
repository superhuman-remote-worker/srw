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
rewrites already-snapshotted history. A network error or an empty answer prices
nothing and raises nothing; a database error propagates to the caller, and the
loop logs it and retries — the same graceful-degradation posture as the rest of
the metering tier.

Schedule (:func:`llm_pricing_sync_loop`, one leader per install): rates follow
the catalog within a minute of any write (seed Job, Admin, imports, manual SQL),
a failed fetch is retried with backoff, and prices are refreshed every 6 h.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Iterable,
    Optional,
)

import asyncpg
import httpx

if TYPE_CHECKING:
    from orchestrator.services.usage_ledger import UsageRates

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

    The last step compares version separators loosely: Anthropic's API ids write
    a minor version with a dash (``claude-opus-5-5``) where OpenRouter writes a
    dot (``anthropic/claude-opus-5.5``). Without it every ``x-y`` Claude model
    went unpriced and a 6.7M-token Opus job showed only its side model's $0.05.
    The loose index has its own uniqueness check, so a catalog listing both
    spellings stays unpriced rather than guessed.
    """
    by_full: dict[str, LlmTokenPrices] = {}
    suffix_counts: Counter = Counter()
    loose_counts: Counter = Counter()
    for full_id in prices:
        fl = full_id.lower()
        by_full[fl] = prices[full_id]
        suffix = fl.split("/", 1)[-1]
        suffix_counts[suffix] += 1
        loose_counts[_loose_version(suffix)] += 1
    by_suffix: dict[str, LlmTokenPrices] = {}
    by_loose: dict[str, LlmTokenPrices] = {}
    for full_id in prices:
        suffix = full_id.lower().split("/", 1)[-1]
        if suffix_counts[suffix] == 1:  # unique → safe to match by bare suffix
            by_suffix[suffix] = prices[full_id]
        loose = _loose_version(suffix)
        if loose_counts[loose] == 1:
            by_loose[loose] = prices[full_id]

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
        return (
            by_full.get(c)
            or by_full.get(stripped)
            or by_suffix.get(stripped)
            or by_loose.get(_loose_version(stripped))
        )

    return resolve


# A dot between two digits: the version separator OpenRouter uses where
# Anthropic's API ids use a dash (``claude-opus-5.5`` vs ``claude-opus-5-5``).
_VERSION_DOT = re.compile(r"(?<=\d)\.(?=\d)")


def _loose_version(model_suffix: str) -> str:
    """``model_suffix`` with every digit.digit separator written as a dash."""
    return _VERSION_DOT.sub("-", model_suffix)


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
        except Exception as exc:
            # One line: an air-gapped install retries this on a backoff, and a
            # traceback per attempt says nothing the exception text does not.
            logger.warning(
                "openrouter pricing fetch failed (non-fatal): %s: %s",
                type(exc).__name__,
                exc,
            )
            logger.debug("openrouter pricing fetch failure", exc_info=True)
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


# How often the loop compares the catalog's pricing pairs with the last sync.
CATALOG_CHECK_INTERVAL_S = 60.0
# First retry after a failed or empty price fetch; it doubles up to the loop's
# refresh interval.
FIRST_RETRY_S = 60.0


@dataclass
class _SyncSchedule:
    """When the pricing loop syncs next (times on the loop's monotonic clock).

    A sync is due when the catalog's pricing pairs differ from the set the
    last attempt saw, or once ``next_due`` has passed: ``interval`` after a
    sync that got prices (price drift), or a backoff after a failed or empty
    fetch — ``first_retry``, doubling up to ``interval``.

    Comparing with the last *attempt* keeps a failed fetch on its backoff
    rather than retrying at every check, while a catalog change no attempt
    has seen yet still syncs at the next check. After a sync that got prices
    the attempted set is the set of that sync, so a pair OpenRouter cannot
    price (a self-hosted model) never causes a fetch by itself.
    """

    interval: float
    first_retry: float
    attempted: Optional[frozenset] = None
    next_due: float = float("-inf")
    retry: Optional[float] = None

    def due(self, now: float, pairs: frozenset) -> bool:
        return pairs != self.attempted or now >= self.next_due

    def succeeded(self, now: float, pairs: frozenset) -> None:
        self.attempted = pairs
        self.retry = None
        self.next_due = now + self.interval

    def failed(self, now: float, pairs: frozenset) -> float:
        """Record a failed attempt; returns the delay until the next one."""
        self.attempted = pairs
        if self.retry is None:
            self.retry = min(self.first_retry, self.interval)
        else:
            self.retry = min(self.retry * 2, self.interval)
        self.next_due = now + self.retry
        return self.retry


async def _sync_pairs(
    app_pool: asyncpg.Pool,
    pairs: list[tuple[str, Optional[str]]],
    fetch_prices: Callable[[], Awaitable[dict[str, LlmTokenPrices]]],
) -> Optional[int]:
    """One sync of the catalog's ``pairs``.

    Returns the rate rows inserted (0 when the prices are unchanged), or
    ``None`` when the fetch failed or returned no prices, which the loop
    retries on its backoff. A catalog with no pair to price (empty, or every
    row unpriced on purpose) needs no fetch and counts as synced.
    """
    if not any(_pricing_id_for(resource, pid) for resource, pid in pairs):
        return 0
    prices = await fetch_prices()
    if not prices:
        return None
    return await sync_llm_rates(app_pool, pairs, prices=prices)


async def _wait_for_shutdown(shutdown_event: asyncio.Event, timeout: float) -> None:
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        pass


async def llm_pricing_sync_loop(
    shutdown_event: asyncio.Event,
    app_pool: Optional[asyncpg.Pool],
    list_models: Callable[[], Awaitable[list[dict]]],
    *,
    usage_rates: Optional[UsageRates] = None,
    interval: float = 21600.0,
    catalog_check_interval: float = CATALOG_CHECK_INTERVAL_S,
    first_retry: float = FIRST_RETRY_S,
    fetch_prices: Optional[Callable[[], Awaitable[dict[str, LlmTokenPrices]]]] = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Optional[Callable[[float], Awaitable[None]]] = None,
) -> None:
    """Background loop: keep ``usage_rates`` in step with the catalog × OpenRouter.

    Leader-gated, so one writer per install. Every ``catalog_check_interval``
    s it lists the catalog's pricing pairs (a DB read only) and syncs when
    they changed, so rates follow every catalog writer — the Helm seed Job,
    Admin create/update/delete, a ``pricing_id`` edit, the subscription
    import, the startup seeders, manual SQL — within about a minute. It
    also syncs on entry and ``interval`` s (6 h) after each sync that got
    prices, for price drift. A failed or empty fetch is retried after
    ``first_retry`` s, doubling up to ``interval``: an air-gapped install
    makes a few attempts, then one per ``interval`` (see
    :class:`_SyncSchedule`).

    After a sync that inserted rates, ``usage_rates`` (the ledger's resolver
    in this process) drops its cache; other replicas reload on a miss.

    Non-fatal per tick. Without the app pool the loop waits for shutdown
    rather than returning, because a leader-gated loop that returns is
    started again every second. ``fetch_prices``, ``clock`` and ``sleep``
    are for tests.
    """
    if app_pool is None:
        logger.info("LLM pricing sync loop disabled (app pool unavailable)")
        await shutdown_event.wait()
        return
    fetch = fetch_prices or fetch_openrouter_prices
    wait = sleep or (lambda seconds: _wait_for_shutdown(shutdown_event, seconds))
    schedule = _SyncSchedule(interval=interval, first_retry=first_retry)
    logger.info(
        "LLM pricing sync loop starting (catalog check every %ss, refresh every %ss)",
        catalog_check_interval,
        interval,
    )
    try:
        while not shutdown_event.is_set():
            try:
                models = await list_models()
            except Exception as exc:
                logger.warning(
                    "LLM pricing: catalog list failed (non-fatal): %s: %s",
                    type(exc).__name__,
                    exc,
                )
            else:
                pairs = _catalog_pricing_pairs(models)
                current = frozenset(pairs)
                if schedule.due(clock(), current):
                    try:
                        inserted = await _sync_pairs(app_pool, pairs, fetch)
                    except Exception:
                        logger.exception("LLM pricing sync failed (non-fatal)")
                        inserted = None
                    if inserted is None:
                        retry = schedule.failed(clock(), current)
                        logger.info(
                            "LLM pricing: no prices fetched; next attempt in %.0fs",
                            retry,
                        )
                    else:
                        schedule.succeeded(clock(), current)
                        if inserted and usage_rates is not None:
                            usage_rates.invalidate()
            until_due = schedule.next_due - clock()
            if until_due <= 0:
                until_due = catalog_check_interval
            await wait(min(catalog_check_interval, until_due))
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
