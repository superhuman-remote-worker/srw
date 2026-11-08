"""Unit tests for the OpenRouter → usage_rates pricing sync.

Covers price parsing, the model→OpenRouter-id mapping, the public-catalog fetch
(via httpx.MockTransport — no network), and the effective-dated *change-only*
upsert into usage_rates (via a fake asyncpg pool/conn).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import httpx
import pytest

from orchestrator.services.openrouter_pricing import (
    LlmTokenPrices,
    _build_price_resolver,
    _catalog_pricing_pairs,
    _price,
    _pricing_id_for,
    fetch_openrouter_prices,
    llm_pricing_sync_loop,
    sync_catalog_llm_rates,
    sync_llm_rates,
)


# --- fake asyncpg pool/conn -------------------------------------------------


class FakeConn:
    """Records inserts; answers _rate_changed's fetchval from in-memory state."""

    def __init__(self, existing=None):
        # {(resource, unit): Decimal}
        self.existing = dict(existing or {})
        self.inserts: list[tuple[str, str, Decimal]] = []

    async def fetchval(self, _sql, resource, unit):
        return self.existing.get((resource, unit))

    async def execute(self, _sql, resource, unit, rate, _ts):
        self.inserts.append((resource, unit, rate))
        self.existing[(resource, unit)] = rate  # reflect the insert for change-only


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *_a):
        return False


class FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# --- _price -----------------------------------------------------------------


class TestPrice:
    def test_valid_decimal_string(self):
        assert _price("0.0000015") == Decimal("0.0000015")

    def test_zero_is_priced(self):
        # A free model prices at 0 (not "unpriced").
        assert _price("0") == Decimal("0")

    def test_negative_is_variable_unpriced(self):
        assert _price("-1") is None

    def test_none_and_garbage(self):
        assert _price(None) is None
        assert _price("abc") is None


# --- _pricing_id_for --------------------------------------------------------


class TestPricingIdFor:
    def test_admin_pricing_id_wins(self):
        assert (
            _pricing_id_for("MiniMax-M3", "minimax/minimax-m3") == "minimax/minimax-m3"
        )

    def test_empty_string_is_explicitly_unpriced(self):
        assert _pricing_id_for("gemma-4-moe", "") is None
        assert _pricing_id_for("gemma-4-moe", "   ") is None

    def test_none_falls_back_to_normalized_id(self):
        assert _pricing_id_for("GPT-5.5", None) == "gpt-5.5"


# --- _build_price_resolver --------------------------------------------------


class TestBuildPriceResolver:
    _P = LlmTokenPrices(Decimal("0.0000025"), Decimal("0.00001"))
    _Q = LlmTokenPrices(Decimal("0.000005"), Decimal("0.00003"))

    def test_exact_full_id_match(self):
        resolve = _build_price_resolver({"openai/gpt-5.5": self._P})
        assert resolve("openai/gpt-5.5") is self._P

    def test_bare_suffix_match(self):
        # The whole point: a bare model id resolves to the prefixed OpenRouter id.
        resolve = _build_price_resolver({"openai/gpt-5.6-terra": self._P})
        assert resolve("gpt-5.6-terra") is self._P

    def test_case_insensitive(self):
        resolve = _build_price_resolver({"openai/gpt-5.5": self._P})
        assert resolve("OpenAI/GPT-5.5") is self._P
        assert resolve("GPT-5.5") is self._P

    def test_ambiguous_suffix_dropped_but_full_id_still_resolves(self):
        # Two providers share suffix 'foo' → bare 'foo' fails closed (unpriced),
        # but the unambiguous full ids still resolve.
        resolve = _build_price_resolver({"openai/foo": self._P, "bar/foo": self._Q})
        assert resolve("foo") is None
        assert resolve("openai/foo") is self._P
        assert resolve("bar/foo") is self._Q

    def test_gateway_prefixed_id_resolves_to_the_inner_full_id(self):
        """``openrouter/openai/gpt-oss-120b`` is a ROUTING prefix + a full id.

        Stripping one segment leaves ``openai/gpt-oss-120b``, which lives in the
        full-id index, not the bare-suffix one. Before this was handled, the
        models routed *through OpenRouter* were the ones auto-detection could not
        price — verified against the live catalog: both ``openrouter/…`` entries
        missed while the bare ``MiniMax-M3`` matched.
        """
        resolve = _build_price_resolver({"openai/gpt-oss-120b": self._P})
        assert resolve("openrouter/openai/gpt-oss-120b") is self._P

    def test_gateway_prefix_does_not_reopen_the_ambiguous_bare_name(self):
        """Fail-closed is a property of the BARE suffix and must survive the fix.

        A gateway-prefixed *full* id stays resolvable (it is unambiguous by
        construction), while the bare shared suffix stays unpriced.
        """
        resolve = _build_price_resolver({"openai/foo": self._P, "bar/foo": self._Q})
        assert resolve("gateway/openai/foo") is self._P
        assert resolve("gateway/foo") is None
        assert resolve("foo") is None

    def test_dashed_anthropic_version_matches_the_dotted_openrouter_id(self):
        """Anthropic's API id ``claude-opus-5-5`` is OpenRouter's
        ``anthropic/claude-opus-5.5``. Neither the exact nor the suffix index
        bridges the separator, so every ``x-y`` Claude model went unpriced: a
        6.7M-token Opus job on dev reported its side model's $0.05 as the cost.
        The ``:batch`` variant must not be picked up instead."""
        resolve = _build_price_resolver(
            {
                "anthropic/claude-opus-5.5": self._P,
                "anthropic/claude-opus-5.5:batch": self._Q,
                "anthropic/claude-haiku-4.5": self._Q,
            }
        )
        assert resolve("claude-opus-5-5") is self._P
        assert resolve("Claude-Opus-5-5") is self._P
        assert resolve("claude-haiku-4-5") is self._Q
        # A dotted catalog id still takes the exact-suffix path.
        assert resolve("claude-opus-5.5") is self._P

    def test_loose_version_match_is_digit_dot_digit_only(self):
        """Only a dot between two digits is a version separator; a name that
        differs elsewhere must not start matching."""
        resolve = _build_price_resolver({"openai/gpt-5.5": self._P})
        assert resolve("gpt-5-5") is self._P
        assert resolve("gpt-55") is None
        assert resolve("gpt5-5") is None

    def test_loose_version_collision_fails_closed(self):
        """Two catalog ids that differ only by separator are ambiguous for a
        spelling that matches neither exactly, so it stays unpriced."""
        resolve = _build_price_resolver(
            {"a/foo-1.5.2": self._P, "b/foo-1-5.2": self._Q}
        )
        assert resolve("foo-1-5-2") is None
        # Each exact spelling still resolves to its own provider.
        assert resolve("foo-1.5.2") is self._P
        assert resolve("foo-1-5.2") is self._Q

    def test_none_empty_and_absent(self):
        resolve = _build_price_resolver({"openai/gpt-5.5": self._P})
        assert resolve(None) is None
        assert resolve("") is None
        assert resolve("   ") is None
        assert resolve("nonexistent") is None


# --- fetch_openrouter_prices ------------------------------------------------


class TestFetch:
    @pytest.mark.asyncio
    async def test_parses_and_skips_unpriced(self):
        def handler(_req):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "openai/gpt-5.5",
                            "pricing": {
                                "prompt": "0.0000025",
                                "completion": "0.00001",
                                "input_cache_read": "0.00000025",
                            },
                        },
                        {
                            "id": "free/model",
                            "pricing": {"prompt": "0", "completion": "0"},
                        },
                        {  # variable/BYO pricing → skipped
                            "id": "variable/model",
                            "pricing": {"prompt": "-1", "completion": "-1"},
                        },
                        {"id": "nopricing"},  # no pricing block → skipped
                    ]
                },
            )

        prices = await fetch_openrouter_prices(client=_client(handler))
        assert prices["openai/gpt-5.5"] == LlmTokenPrices(
            Decimal("0.0000025"),
            Decimal("0.00001"),
            Decimal("0.00000025"),
        )
        assert prices["free/model"] == LlmTokenPrices(Decimal("0"), Decimal("0"))
        assert "variable/model" not in prices
        assert "nopricing" not in prices

    @pytest.mark.asyncio
    async def test_non_fatal_on_http_error(self):
        def handler(_req):
            return httpx.Response(500, json={})

        assert await fetch_openrouter_prices(client=_client(handler)) == {}


# --- sync_llm_rates ---------------------------------------------------------


class TestSyncLlmRates:
    _PRICES = {
        "openai/gpt-5.5": LlmTokenPrices(
            Decimal("0.0000025"),
            Decimal("0.00001"),
            Decimal("0.00000025"),
        )
    }
    _TS = datetime(2026, 7, 1, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_inserts_all_token_dimensions(self):
        conn = FakeConn()
        n = await sync_llm_rates(
            FakePool(conn),
            [("gpt-5.5", "openai/gpt-5.5")],
            prices=self._PRICES,
            now=self._TS,
        )
        assert n == 3
        assert ("gpt-5.5", "prompt-token", Decimal("0.0000025")) in conn.inserts
        assert ("gpt-5.5", "completion-token", Decimal("0.00001")) in conn.inserts
        assert (
            "gpt-5.5",
            "cached-prompt-token",
            Decimal("0.00000025"),
        ) in conn.inserts

    @pytest.mark.asyncio
    async def test_change_only_second_run_is_noop(self):
        conn = FakeConn()
        models = [("gpt-5.5", "openai/gpt-5.5")]
        await sync_llm_rates(FakePool(conn), models, prices=self._PRICES, now=self._TS)
        # Same prices again → no new rows.
        n2 = await sync_llm_rates(
            FakePool(conn), models, prices=self._PRICES, now=self._TS
        )
        assert n2 == 0

    @pytest.mark.asyncio
    async def test_price_change_inserts_new_row(self):
        conn = FakeConn()
        models = [("gpt-5.5", "openai/gpt-5.5")]
        await sync_llm_rates(FakePool(conn), models, prices=self._PRICES, now=self._TS)
        bumped = {
            "openai/gpt-5.5": LlmTokenPrices(
                Decimal("0.0000030"),
                Decimal("0.00001"),
                Decimal("0.00000025"),
            )
        }
        n2 = await sync_llm_rates(FakePool(conn), models, prices=bumped, now=self._TS)
        assert n2 == 1  # only the prompt-token rate changed
        assert ("gpt-5.5", "prompt-token", Decimal("0.0000030")) in conn.inserts

    @pytest.mark.asyncio
    async def test_missing_cache_read_price_falls_back_to_prompt_rate(self):
        conn = FakeConn()
        n = await sync_llm_rates(
            FakePool(conn),
            [("gpt-5.5", "openai/gpt-5.5")],
            prices={
                "openai/gpt-5.5": LlmTokenPrices(
                    Decimal("0.0000025"),
                    Decimal("0.00001"),
                )
            },
            now=self._TS,
        )
        assert n == 3
        assert (
            "gpt-5.5",
            "cached-prompt-token",
            Decimal("0.0000025"),
        ) in conn.inserts

    @pytest.mark.asyncio
    async def test_explicitly_unpriced_model_is_skipped(self):
        conn = FakeConn()
        n = await sync_llm_rates(
            FakePool(conn),
            [("gemma-4-moe", "")],  # self-hosted → unpriced
            prices=self._PRICES,
            now=self._TS,
        )
        assert n == 0
        assert conn.inserts == []

    @pytest.mark.asyncio
    async def test_unmatched_pricing_id_left_unpriced(self):
        conn = FakeConn()
        n = await sync_llm_rates(
            FakePool(conn),
            [("mystery", "openrouter/not-in-catalog")],
            prices=self._PRICES,
            now=self._TS,
        )
        assert n == 0

    @pytest.mark.asyncio
    async def test_bare_model_id_auto_matches_by_suffix(self):
        # No admin pricing_id: the bare model_id 'gpt-5.5' resolves to the
        # prefixed OpenRouter id 'openai/gpt-5.5' via the unique-suffix index.
        conn = FakeConn()
        n = await sync_llm_rates(
            FakePool(conn),
            [("gpt-5.5", None)],
            prices=self._PRICES,
            now=self._TS,
        )
        assert n == 3
        assert ("gpt-5.5", "prompt-token", Decimal("0.0000025")) in conn.inserts

    @pytest.mark.asyncio
    async def test_dashed_claude_catalog_id_is_seeded_under_its_own_name(self):
        """The rate lands under the catalog id the ledger records
        (``claude-opus-5-5``), not under OpenRouter's dotted spelling."""
        conn = FakeConn()
        n = await sync_llm_rates(
            FakePool(conn),
            [("claude-opus-5-5", None)],
            prices={
                "anthropic/claude-opus-5.5": LlmTokenPrices(
                    Decimal("0.000004"), Decimal("0.00002"), Decimal("0.0000002")
                )
            },
            now=self._TS,
        )
        assert n == 3
        assert ("claude-opus-5-5", "prompt-token", Decimal("0.000004")) in conn.inserts
        assert (
            "claude-opus-5-5",
            "cached-prompt-token",
            Decimal("0.0000002"),
        ) in conn.inserts

    @pytest.mark.asyncio
    async def test_bare_pricing_id_matches_by_suffix(self):
        # An admin pricing_id given without the provider prefix still resolves.
        conn = FakeConn()
        n = await sync_llm_rates(
            FakePool(conn),
            [("gpt-5.5", "gpt-5.5")],
            prices=self._PRICES,
            now=self._TS,
        )
        assert n == 3

    @pytest.mark.asyncio
    async def test_ambiguous_suffix_left_unpriced(self):
        # 'foo' is published by two providers → the bare name is ambiguous and
        # fails closed rather than pricing against the wrong provider.
        conn = FakeConn()
        prices = {
            "openai/foo": LlmTokenPrices(Decimal("0.0000025"), Decimal("0.00001")),
            "bar/foo": LlmTokenPrices(Decimal("0.000005"), Decimal("0.00003")),
        }
        n = await sync_llm_rates(
            FakePool(conn), [("foo", None)], prices=prices, now=self._TS
        )
        assert n == 0
        assert conn.inserts == []

    @pytest.mark.asyncio
    async def test_empty_pricing_id_force_unprices_despite_suffix_match(self):
        # pricing_id="" wins even though 'gpt-5.5' would otherwise suffix-match.
        conn = FakeConn()
        n = await sync_llm_rates(
            FakePool(conn),
            [("gpt-5.5", "")],
            prices=self._PRICES,
            now=self._TS,
        )
        assert n == 0
        assert conn.inserts == []

    @pytest.mark.asyncio
    async def test_no_pool_is_noop(self):
        assert await sync_llm_rates(None, [("gpt-5.5", "openai/gpt-5.5")]) == 0


# --- catalog wiring ---------------------------------------------------------


class TestCatalogPairs:
    def test_extracts_model_id_and_pricing_id(self):
        rows = [
            {"model_id": "gpt-5.5", "params_json": {"pricing_id": "openai/gpt-5.5"}},
            {"model_id": "gemma-4-moe", "params_json": {"pricing_id": ""}},
            {"model_id": "codex", "params_json": None},  # no params → pricing_id None
            {"model_id": "", "params_json": {}},  # empty model_id skipped
            {"params_json": {"pricing_id": "x"}},  # no model_id skipped
        ]
        assert _catalog_pricing_pairs(rows) == [
            ("gpt-5.5", "openai/gpt-5.5"),
            ("gemma-4-moe", ""),
            ("codex", None),
        ]


class TestSyncCatalogLlmRates:
    _TS = datetime(2026, 7, 1, tzinfo=timezone.utc)
    _PRICES = {
        "openai/gpt-5.5": LlmTokenPrices(
            Decimal("0.000005"),
            Decimal("0.00003"),
            Decimal("0.0000005"),
        )
    }

    @pytest.mark.asyncio
    async def test_enumerates_catalog_and_seeds(self):
        conn = FakeConn()

        async def list_models():
            return [
                {
                    "model_id": "gpt-5.5",
                    "params_json": {"pricing_id": "openai/gpt-5.5"},
                },
                {"model_id": "gemma-4-moe", "params_json": {"pricing_id": ""}},  # skip
            ]

        n = await sync_catalog_llm_rates(
            FakePool(conn), list_models, prices=self._PRICES, now=self._TS
        )
        assert n == 3  # gpt-5.5 prompt + cached prompt + completion; gemma unpriced
        assert ("gpt-5.5", "prompt-token", Decimal("0.000005")) in conn.inserts
        assert not any(r == "gemma-4-moe" for r, *_ in conn.inserts)

    @pytest.mark.asyncio
    async def test_no_pool_is_noop(self):
        async def list_models():
            raise AssertionError("should not be called when pool is None")

        assert await sync_catalog_llm_rates(None, list_models) == 0

    @pytest.mark.asyncio
    async def test_catalog_list_failure_is_nonfatal(self):
        async def list_models():
            raise RuntimeError("db down")

        assert await sync_catalog_llm_rates(FakePool(FakeConn()), list_models) == 0


# --- llm_pricing_sync_loop --------------------------------------------------

SIX_HOURS = 21600.0
_GPT = {"model_id": "gpt-5.5", "params_json": {"pricing_id": "openai/gpt-5.5"}}
_TERRA = {"model_id": "gpt-5.6-terra", "params_json": None}
_SELF_HOSTED = {"model_id": "gemma-4-local", "params_json": None}
_PRICED = {
    "openai/gpt-5.5": LlmTokenPrices(Decimal("0.0000025"), Decimal("0.00001")),
    "openai/gpt-5.6-terra": LlmTokenPrices(Decimal("0.000003"), Decimal("0.00002")),
}


class _Timeline:
    """A fake clock, catalog and price fetch for one loop run.

    ``sleep`` advances the clock by the requested wait; ``stop_when`` ends the
    run (sets shutdown) once it holds after a wait.
    """

    def __init__(self, catalog, prices=None):
        self.now = 0.0
        self.catalog = list(catalog)
        self.prices = _PRICED if prices is None else prices
        self.fetched_at: list[float] = []
        self.waits: list[float] = []
        self.shutdown = asyncio.Event()
        self.stop_when = lambda: len(self.waits) >= 10
        self.on_wait = lambda: None
        self.conn = FakeConn()

    def clock(self):
        return self.now

    async def sleep(self, seconds):
        self.waits.append(seconds)
        self.now += seconds
        self.on_wait()
        if self.stop_when():
            self.shutdown.set()

    async def list_models(self):
        return list(self.catalog)

    async def fetch(self):
        self.fetched_at.append(self.now)
        return dict(self.prices)

    async def run(self, **kwargs):
        await asyncio.wait_for(
            llm_pricing_sync_loop(
                self.shutdown,
                FakePool(self.conn),
                self.list_models,
                fetch_prices=self.fetch,
                clock=self.clock,
                sleep=self.sleep,
                **kwargs,
            ),
            timeout=5,
        )


class TestPricingSyncLoop:
    @pytest.mark.asyncio
    async def test_syncs_on_entry_then_an_unchanged_catalog_does_not_fetch(self):
        timeline = _Timeline([_GPT])
        timeline.stop_when = lambda: timeline.now >= 3600  # an hour of checks
        await timeline.run()

        assert timeline.fetched_at == [0.0]
        assert set(timeline.waits) == {60.0}, "the catalog is checked every minute"
        assert len(timeline.conn.inserts) == 3

    @pytest.mark.asyncio
    async def test_prices_are_refreshed_every_six_hours(self):
        timeline = _Timeline([_GPT])
        timeline.stop_when = lambda: len(timeline.fetched_at) >= 3
        await timeline.run()

        assert timeline.fetched_at == [0.0, SIX_HOURS, 2 * SIX_HOURS]

    @pytest.mark.asyncio
    async def test_a_new_catalog_pair_syncs_at_the_next_check(self):
        timeline = _Timeline([_GPT])

        def _admin_adds_a_model():
            if timeline.now == 300:
                timeline.catalog.append(_TERRA)

        timeline.on_wait = _admin_adds_a_model
        await timeline.run()

        assert timeline.fetched_at == [0.0, 300.0]
        assert ("gpt-5.6-terra", "prompt-token", Decimal("0.000003")) in (
            timeline.conn.inserts
        )

    @pytest.mark.asyncio
    async def test_a_pricing_id_edit_is_a_catalog_change(self):
        timeline = _Timeline([_SELF_HOSTED])
        timeline.prices = {
            **_PRICED,
            "google/gemma-4-31b-it": LlmTokenPrices(Decimal("1e-7"), Decimal("2e-7")),
        }

        def _admin_sets_a_pricing_id():
            if timeline.now == 120:
                timeline.catalog = [
                    {
                        "model_id": "gemma-4-local",
                        "params_json": {"pricing_id": "google/gemma-4-31b-it"},
                    }
                ]

        timeline.on_wait = _admin_sets_a_pricing_id
        await timeline.run()

        assert timeline.fetched_at == [0.0, 120.0]
        assert ("gemma-4-local", "prompt-token", Decimal("1e-7")) in (
            timeline.conn.inserts
        )

    @pytest.mark.asyncio
    async def test_a_pair_openrouter_cannot_price_does_not_refetch(self):
        timeline = _Timeline([_GPT, _SELF_HOSTED])
        timeline.stop_when = lambda: timeline.now >= 3600
        await timeline.run()

        assert timeline.fetched_at == [0.0]
        assert not any(r == "gemma-4-local" for r, *_ in timeline.conn.inserts)

    @pytest.mark.asyncio
    async def test_an_empty_catalog_fetches_nothing_until_the_seed_writes_it(self):
        timeline = _Timeline([])

        def _seed_job_runs():
            if timeline.now == 240:
                timeline.catalog = [_GPT]

        timeline.on_wait = _seed_job_runs
        await timeline.run()

        assert timeline.fetched_at == [240.0]

    @pytest.mark.asyncio
    async def test_a_failed_fetch_retries_on_a_doubling_backoff_capped_at_six_hours(
        self,
    ):
        timeline = _Timeline([_GPT], prices={})
        timeline.stop_when = lambda: len(timeline.fetched_at) >= 12
        await timeline.run()

        gaps = [b - a for a, b in zip(timeline.fetched_at, timeline.fetched_at[1:])]
        assert gaps == [60, 120, 240, 480, 960, 1920, 3840, 7680, 15360, 21600, 21600]
        assert max(timeline.waits) <= 60, "the catalog is still checked every minute"

    @pytest.mark.asyncio
    async def test_a_fetch_after_the_network_recovers_resets_the_backoff(self):
        timeline = _Timeline([_GPT], prices={})

        def _network_recovers():
            if timeline.now == 180:
                timeline.prices = _PRICED

        timeline.on_wait = _network_recovers
        timeline.stop_when = lambda: timeline.now > SIX_HOURS + 180
        await timeline.run()

        # 0 fails, 60 fails, 180 succeeds; then the 6 h refresh.
        assert timeline.fetched_at == [0.0, 60.0, 180.0, SIX_HOURS + 180]

    @pytest.mark.asyncio
    async def test_a_catalog_change_during_the_backoff_is_tried_at_once(self):
        timeline = _Timeline([_GPT], prices={})

        def _admin_adds_a_model():
            if timeline.now == 120:
                timeline.catalog.append(_TERRA)

        timeline.on_wait = _admin_adds_a_model
        timeline.stop_when = lambda: timeline.now >= 600
        await timeline.run()

        # 0 fails (retry at 60); 60 fails (retry at 180); the change at 120 is
        # tried at once and fails (retry 240 s later, at 360).
        assert timeline.fetched_at == [0.0, 60.0, 120.0, 360.0]

    @pytest.mark.asyncio
    async def test_a_database_error_counts_as_a_failed_sync(self):
        timeline = _Timeline([_GPT])

        class _BrokenConn(FakeConn):
            async def fetchval(self, *_args):
                raise RuntimeError("db down")

        timeline.conn = _BrokenConn()
        timeline.stop_when = lambda: len(timeline.fetched_at) >= 3
        await timeline.run()

        assert timeline.fetched_at == [0.0, 60.0, 180.0]

    @pytest.mark.asyncio
    async def test_a_catalog_list_failure_waits_for_the_next_check(self):
        timeline = _Timeline([_GPT])
        calls = []

        async def _flaky_list():
            calls.append(timeline.now)
            if len(calls) == 1:
                raise RuntimeError("db down")
            return [_GPT]

        timeline.list_models = _flaky_list
        timeline.stop_when = lambda: len(timeline.waits) >= 2
        await timeline.run()

        assert calls == [0.0, 60.0]
        assert timeline.fetched_at == [60.0]

    @pytest.mark.asyncio
    async def test_inserted_rates_clear_this_processes_rate_cache(self):
        rates = MagicMock()
        timeline = _Timeline([_GPT])
        timeline.stop_when = lambda: len(timeline.fetched_at) >= 2
        await timeline.run(usage_rates=rates)

        # The first sync inserted rows, the 6 h refresh found no change.
        assert rates.invalidate.call_count == 1

    @pytest.mark.asyncio
    async def test_shutdown_during_the_wait_returns(self):
        shutdown = asyncio.Event()
        listed = asyncio.Event()

        async def list_models():
            listed.set()
            return []

        task = asyncio.create_task(
            llm_pricing_sync_loop(shutdown, FakePool(FakeConn()), list_models)
        )
        await asyncio.wait_for(listed.wait(), timeout=5)
        await asyncio.sleep(0)
        assert not task.done(), "the loop is waiting for its next check"
        shutdown.set()
        await asyncio.wait_for(task, timeout=5)

    @pytest.mark.asyncio
    async def test_without_the_app_pool_the_loop_waits_for_shutdown(self):
        shutdown = asyncio.Event()

        async def list_models():
            raise AssertionError("no catalog read without the app pool")

        task = asyncio.create_task(llm_pricing_sync_loop(shutdown, None, list_models))
        await asyncio.sleep(0.05)
        assert not task.done(), "a leader-gated loop that returns is restarted"
        shutdown.set()
        await asyncio.wait_for(task, timeout=5)
