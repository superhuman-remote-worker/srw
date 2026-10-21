"""The ``live_voice`` catalog capability (live-session jobs §7)."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from orchestrator.database.postgres import PostgresDB
from orchestrator.schemas.model_catalog import (
    VALID_CATALOG_CAPABILITIES,
    CatalogModelCreate,
    CatalogModelUpdate,
    live_voice_capability_problem,
)
from orchestrator.schemas.provider_catalog import VALID_DEFAULT_MODEL_KINDS
from orchestrator.seed.llm_config import DEFAULT_PIN_CAPABILITY_BY_KIND
from orchestrator.services import discovery
from orchestrator.services.llm_endpoint_probe import _capability_hints
from orchestrator.services.openrouter_pricing import _catalog_pricing_pairs

MIGRATIONS = Path(__file__).parent.parent / "src/orchestrator/database/migrations/app"


def test_0536_swaps_the_capabilities_check_for_a_live_voice_superset():
    sql = " ".join((MIGRATIONS / "0536_live_voice_capability.sql").read_text().split())
    assert "DROP CONSTRAINT IF EXISTS models_capabilities_check" in sql
    assert "'search', 'fetch', 'rerank', 'live_voice'" in sql
    assert ") NOT VALID;" in sql
    sql = (MIGRATIONS / "0537_validate_live_voice_capability.sql").read_text()
    assert "VALIDATE CONSTRAINT models_capabilities_check" in sql


def test_the_capability_is_in_every_list():
    assert "live_voice" in VALID_CATALOG_CAPABILITIES
    assert "live_voice" in PostgresDB._CATALOG_CAPABILITIES
    assert "live_voice" in VALID_DEFAULT_MODEL_KINDS
    assert DEFAULT_PIN_CAPABILITY_BY_KIND["live_voice"] == "live_voice"
    chart = Path(__file__).parent.parent / (
        "helm/templates/orchestrator/llm-seed-configmap.yaml"
    )
    assert '"live_voice"' in chart.read_text()


def _create(**overrides):
    body = {
        "provider_kind": "endpoint",
        "provider_ref": "00000000-0000-0000-0000-000000000001",
        "model_id": "gpt-live-1",
        "display_label": "GPT-Live",
        "capabilities": ["live_voice"],
        "family": "default",
        "params_json": {"adapter": "openai-live", "voice": "marin"},
    }
    body.update(overrides)
    return CatalogModelCreate(**body)


def test_a_live_voice_row_is_accepted_alone():
    assert _create().capabilities == ["live_voice"]


@pytest.mark.parametrize("caps", [["live_voice", "chat"], ["auxiliary", "live_voice"]])
def test_live_voice_with_another_capability_contradicts_itself(caps):
    with pytest.raises(ValidationError, match="only capability"):
        _create(capabilities=caps)
    with pytest.raises(ValidationError, match="only capability"):
        CatalogModelUpdate(capabilities=caps)
    with pytest.raises(ValueError, match="only capability"):
        PostgresDB._canonicalize_capabilities(capabilities=caps)


def test_problem_helper():
    assert live_voice_capability_problem(None) is None
    assert live_voice_capability_problem(["chat", "auxiliary"]) is None
    assert live_voice_capability_problem(["live_voice", "live_voice"]) is None


@pytest.mark.parametrize(
    "model_id", ["gpt-live-1", "gpt-realtime", "gpt-4o-realtime-preview"]
)
def test_discovery_and_probe_classify_live_models_before_chat(model_id):
    assert discovery._capabilities_from_model_id(model_id) == ["live_voice"]
    assert _capability_hints(model_id) == ["live_voice"]


def test_pricing_sync_skips_live_voice_rows():
    pairs = _catalog_pricing_pairs(
        [
            {
                "model_id": "gpt-live-1",
                "capabilities": ["live_voice"],
                "params_json": {},
            },
            {"model_id": "gpt-6", "capabilities": ["chat"], "params_json": {}},
        ]
    )
    assert pairs == [("gpt-6", None)]


@pytest.mark.asyncio
async def test_the_ledger_prices_gpt_live_until_a_rate_is_configured():
    from datetime import datetime, timezone
    from decimal import Decimal
    from unittest.mock import AsyncMock, Mock

    from orchestrator.services.live.ledger import record_session_usage

    session = {
        "id": "00000000-0000-0000-0000-0000000000aa",
        "model": "gpt-live-1",
        "usage_seconds": 60,
        "ended_at": datetime(2026, 10, 13, tzinfo=timezone.utc),
        "job_id": None,
    }
    ledger = Mock(is_available=True)
    ledger.resolve_rate = AsyncMock(return_value=None)
    ledger.record_events = AsyncMock(return_value=1)
    assert await record_session_usage(ledger, Mock(), session) == 1
    event = ledger.record_events.await_args.args[0][0]
    assert event.unit == "live-voice-second" and event.cost_usd == Decimal("0.05")

    ledger.resolve_rate = AsyncMock(return_value=Decimal("0.001"))
    await record_session_usage(ledger, Mock(), session)
    event = ledger.record_events.await_args.args[0][0]
    assert event.rate_usd is None  # the configured rate is snapshotted


@pytest.mark.asyncio
async def test_the_ledger_prices_a_model_by_its_catalog_rows_rate():
    """A voice gateway profile prices at its row's rate (0, or an operator's
    rate the chart seeds); a configured rate still wins; a model with neither
    a row rate nor a list price stays unpriced."""
    from datetime import datetime, timezone
    from decimal import Decimal
    from unittest.mock import AsyncMock, Mock

    from orchestrator.services.live.ledger import record_session_usage

    def session(model: str) -> dict:
        return {
            "id": "00000000-0000-0000-0000-0000000000ab",
            "model": model,
            "usage_seconds": 120,
            "ended_at": datetime(2026, 10, 21, tzinfo=timezone.utc),
            "job_id": None,
        }

    def ledger(configured=None) -> Mock:
        found = Mock(is_available=True)
        found.resolve_rate = AsyncMock(return_value=configured)
        found.record_events = AsyncMock(return_value=1)
        return found

    def db(params) -> Mock:
        found = Mock()
        found.resolve_catalog_model = AsyncMock(
            return_value=None if params is None else {"params_json": params}
        )
        return found

    async def event(model, params, configured=None):
        books = ledger(configured)
        await record_session_usage(books, db(params), session(model))
        return books.record_events.await_args.args[0][0]

    zero = await event(
        "cascade-cpu", {"adapter": "openai-live", "rate_usd_per_minute": 0}
    )
    assert (zero.rate_usd, zero.cost_usd) == (Decimal(0), Decimal(0))
    rated = await event("cascade-gpu", '{"rate_usd_per_minute": "0.03"}')
    assert rated.cost_usd == Decimal("0.06")
    # The row's rate replaces the list price of a GPT-Live row too.
    assert (
        await event("gpt-live-1", {"rate_usd_per_minute": 0.06})
    ).cost_usd == Decimal("0.12")
    configured = await event("cascade-cpu", {"rate_usd_per_minute": 0}, Decimal("1"))
    assert configured.rate_usd is None and configured.cost_usd is None
    for unusable in (
        {},
        {"rate_usd_per_minute": -1},
        {"rate_usd_per_minute": True},
        {"rate_usd_per_minute": "x"},
        {"rate_usd_per_minute": "NaN"},
        None,
    ):
        unpriced = await event("cascade-cpu", unusable)
        assert unpriced.rate_usd is None and unpriced.cost_usd is None
    # Without a row rate, a GPT-Live row keeps its list price.
    assert (await event("gpt-live-1", {})).cost_usd == Decimal("0.10")
