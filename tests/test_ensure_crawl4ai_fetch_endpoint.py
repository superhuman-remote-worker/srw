"""Boot registration and default-slot behavior for bundled Crawl4AI."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest

from orchestrator.seed.llm_config import (
    CRAWL4AI_ENDPOINT_LABEL,
    CRAWL4AI_MODEL_ID,
    ensure_crawl4ai_fetch_endpoint,
)


def _db(*, endpoints=None, defaults=None, inserted=True):
    db = MagicMock()
    db.list_system_llm_endpoints = AsyncMock(return_value=list(endpoints or []))
    db.create_system_llm_endpoint = AsyncMock(
        return_value={"id": "33333333-3333-3333-3333-333333333333"}
    )
    db.create_model = AsyncMock(
        return_value={"model_id": CRAWL4AI_MODEL_ID} if inserted else None
    )
    defaults = defaults or {}
    db.get_default_llm_model = AsyncMock(
        side_effect=lambda capability: defaults.get(capability)
    )
    db.set_default_llm_model = AsyncMock()
    return db


@pytest.mark.asyncio
async def test_fresh_install_creates_fetch_row_and_claims_the_empty_slot(monkeypatch):
    monkeypatch.setenv("CRAWL4AI_BASE_URL", "http://srw-crawl4ai:11235/")
    monkeypatch.setenv("CRAWL4AI_API_TOKEN", "0123456789abcdef0123456789abcdef")
    db = _db()

    assert await ensure_crawl4ai_fetch_endpoint(db) is True

    endpoint = db.create_system_llm_endpoint.await_args.kwargs
    assert endpoint == {
        "label": CRAWL4AI_ENDPOINT_LABEL,
        "base_url": "http://srw-crawl4ai:11235",
        "api_key": "0123456789abcdef0123456789abcdef",
        "key_prefix": "01234567",
        "source": "default",
    }
    model = db.create_model.await_args.kwargs
    assert model["model_id"] == CRAWL4AI_MODEL_ID
    # Crawl4AI cannot search, so it must never advertise the search capability
    # or claim a slot the search tools resolve.
    assert model["capabilities"] == ["fetch"]
    assert model["params_json"] == {
        "provider": "crawl4ai",
        "ops": ["extract", "crawl", "map"],
    }
    assert db.set_default_llm_model.await_args_list == [
        call("fetch", CRAWL4AI_MODEL_ID, source="default")
    ]


@pytest.mark.asyncio
async def test_keyed_provider_keeps_the_fetch_slot():
    """Tavily seeds first and serves fetch; there is no fetch fallback slot, so
    Crawl4AI registers as a selectable row and writes no default."""
    db = _db(defaults={"fetch": "tavily"})

    assert (
        await ensure_crawl4ai_fetch_endpoint(
            db, base_url="http://srw-crawl4ai:11235", api_token="t" * 32
        )
        is True
    )

    db.create_model.assert_awaited_once()
    db.set_default_llm_model.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_service_url_is_a_no_op(monkeypatch):
    monkeypatch.delenv("CRAWL4AI_BASE_URL", raising=False)
    db = _db()

    assert await ensure_crawl4ai_fetch_endpoint(db) is False
    db.list_system_llm_endpoints.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_token_registers_nothing(monkeypatch):
    """The service rejects every unauthenticated request, so a tokenless row
    would be a provider that fails on first use."""
    monkeypatch.delenv("CRAWL4AI_API_TOKEN", raising=False)
    db = _db()

    assert (
        await ensure_crawl4ai_fetch_endpoint(db, base_url="http://srw-crawl4ai:11235")
        is False
    )
    db.list_system_llm_endpoints.assert_not_awaited()
    db.create_model.assert_not_awaited()


@pytest.mark.asyncio
async def test_second_boot_and_admin_deleted_model_are_not_recreated():
    db = _db(
        endpoints=[
            {
                "id": "33333333-3333-3333-3333-333333333333",
                "label": CRAWL4AI_ENDPOINT_LABEL,
            }
        ]
    )

    assert (
        await ensure_crawl4ai_fetch_endpoint(
            db, base_url="http://srw-crawl4ai:11235", api_token="t" * 32
        )
        is False
    )
    db.create_system_llm_endpoint.assert_not_awaited()
    db.create_model.assert_not_awaited()
    db.set_default_llm_model.assert_not_awaited()


@pytest.mark.asyncio
async def test_catalog_conflict_does_not_write_defaults():
    db = _db(inserted=False)

    assert (
        await ensure_crawl4ai_fetch_endpoint(
            db, base_url="http://srw-crawl4ai:11235", api_token="t" * 32
        )
        is False
    )
    db.set_default_llm_model.assert_not_awaited()
