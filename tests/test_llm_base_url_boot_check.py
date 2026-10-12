"""Tests for the LLM_BASE_URL boot check (chunk 6 of models_yaml_removal).

The orchestrator hard-fails (sys.exit(1)) when ``LLM_BASE_URL`` is set
because the env-var-driven routing for self-hosted "Local" group models
was removed; leaving it set with no consumer is exactly the "active
misconfiguration that won't self-heal" path that produced the 401-against-
api.openai.com bug captured in knowledge-base/knowledge/llm_routing_issues.md.

The refusal runs in the application lifespan
(``orchestrator.application.lifecycle.lifespan``). Each case builds its own
application from the current environment and enters that lifespan.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.application import build_application_resources, lifecycle  # noqa: E402


def _application() -> FastAPI:
    """An application whose resources and settings ``create_app()`` would build
    from the current environment. Only its lifespan runs here, so the routers
    and the process-wide bindings ``create_app()`` also installs are left out
    (no other test sees this application)."""

    app = FastAPI(lifespan=lifecycle.lifespan)
    app.state.resources = build_application_resources()
    return app


@pytest.mark.asyncio
async def test_lifespan_exits_when_llm_base_url_set(monkeypatch):
    """When LLM_BASE_URL is set at boot, the lifespan handler logs an
    ERROR and calls sys.exit(1) before any DB connection happens."""
    monkeypatch.setenv("LLM_BASE_URL", "http://stale-vllm:8080/v1")
    app = _application()
    with patch("sys.exit", side_effect=SystemExit) as mock_exit:
        with patch.object(
            app.state.resources.postgres_db, "connect", AsyncMock()
        ) as connect:
            with pytest.raises(SystemExit):
                async with lifecycle.lifespan(app):
                    pass
    mock_exit.assert_called_once_with(1)
    connect.assert_not_awaited()


@pytest.mark.asyncio
async def test_lifespan_proceeds_when_llm_base_url_unset(monkeypatch):
    """Without LLM_BASE_URL, the boot check is a no-op and lifespan
    proceeds to its DB-connect step."""
    monkeypatch.delenv("LLM_BASE_URL", raising=False)
    app = _application()
    with patch("sys.exit") as mock_exit:
        # Stop at the first store connection so we can verify exit was not
        # called without actually starting the orchestrator.
        with patch.object(
            app.state.resources.postgres_db,
            "connect",
            AsyncMock(side_effect=RuntimeError("stop here")),
        ) as connect:
            with pytest.raises(RuntimeError, match="stop here"):
                async with lifecycle.lifespan(app):
                    pass
    mock_exit.assert_not_called()
    connect.assert_awaited_once()
