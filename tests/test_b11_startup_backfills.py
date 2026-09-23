"""R1.B11: the startup data backfills moved out of the lifespan.

Each step is idempotent and non-fatal: a failure is logged and startup
continues with the next step.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services import startup_backfills


def _store(*, encrypt=None, strip=None):
    store = MagicMock()
    store.backfill_encrypt_datasource_credentials = AsyncMock(
        return_value={"encrypted": 0, "skipped": 0, "errors": 0}
        if encrypt is None
        else None,
        side_effect=encrypt,
    )
    store.backfill_strip_thread_config_secrets = AsyncMock(
        return_value={"stripped": 0, "skipped": 0, "errors": 0}
        if strip is None
        else None,
        side_effect=strip,
    )
    return store


@pytest.mark.asyncio
async def test_each_backfill_failure_is_logged_and_the_next_step_still_runs(
    monkeypatch, caplog
):
    monkeypatch.delenv("MCP_DEV_TOKEN", raising=False)
    store = _store(
        encrypt=RuntimeError("cipher unavailable"),
        strip=RuntimeError("row lock timeout"),
    )
    caplog.set_level(logging.ERROR)
    await startup_backfills.run_startup_backfills(store)

    store.backfill_encrypt_datasource_credentials.assert_awaited_once()
    store.backfill_strip_thread_config_secrets.assert_awaited_once()
    assert "Datasource credentials backfill failed" in caplog.text
    assert "Thread config_override strip backfill failed" in caplog.text


@pytest.mark.asyncio
async def test_dev_mcp_token_is_seeded_only_when_configured(monkeypatch):
    import orchestrator.init as init

    seed = AsyncMock()
    monkeypatch.setattr(init, "_seed_admin_mcp_token", seed)
    store = _store()

    monkeypatch.delenv("MCP_DEV_TOKEN", raising=False)
    await startup_backfills.run_startup_backfills(store)
    seed.assert_not_awaited()

    monkeypatch.setenv("MCP_DEV_TOKEN", "  ")
    await startup_backfills.run_startup_backfills(store)
    seed.assert_not_awaited()

    monkeypatch.setenv("MCP_DEV_TOKEN", "dev-token")
    await startup_backfills.run_startup_backfills(store)
    seed.assert_awaited_once_with(store)


@pytest.mark.asyncio
async def test_a_failing_dev_token_seed_does_not_stop_startup(monkeypatch, caplog):
    import orchestrator.init as init

    monkeypatch.setattr(
        init, "_seed_admin_mcp_token", AsyncMock(side_effect=RuntimeError("no admin"))
    )
    monkeypatch.setenv("MCP_DEV_TOKEN", "dev-token")
    caplog.set_level(logging.WARNING)
    await startup_backfills.run_startup_backfills(_store())
    assert "MCP dev token seed at startup failed" in caplog.text
