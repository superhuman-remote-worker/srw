"""Boot-time dependency checks for completion status reordering.

The two gates are read from the environment by
``DeploymentSettings.from_environment()`` when an application is built; the
refusal runs in ``orchestrator.application.lifecycle.lifespan``. Each case
builds its own application from the environment it sets and enters that
lifespan.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.application import build_application_resources, lifecycle  # noqa: E402


def _application(monkeypatch: pytest.MonkeyPatch, **gates: str) -> FastAPI:
    """An application built from an environment carrying ``gates``.

    The resources (and their ``DeploymentSettings``) are what ``create_app()``
    builds from that environment; only the lifespan runs here, so the routers
    and the process-wide bindings ``create_app()`` also installs are left out.
    """

    monkeypatch.delenv("LLM_BASE_URL", raising=False)
    for name, value in gates.items():
        monkeypatch.setenv(name, value)
    app = FastAPI(lifespan=lifecycle.lifespan)
    app.state.resources = build_application_resources()
    return app


@pytest.mark.asyncio
async def test_reorder_without_completion_commands_fails_before_db_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _application(
        monkeypatch,
        COMPLETION_COMMANDS_ENABLED="false",
        COMPLETION_STATUS_REORDER_ENABLED="true",
    )
    settings = app.state.resources.settings
    assert settings.completion_commands_enabled is False
    assert settings.completion_status_reorder_enabled is True
    connect = AsyncMock()

    with patch.object(app.state.resources.postgres_db, "connect", connect):
        with patch("sys.exit", side_effect=SystemExit) as exit_mock:
            with pytest.raises(SystemExit):
                async with lifecycle.lifespan(app):
                    pass

    exit_mock.assert_called_once_with(1)
    connect.assert_not_awaited()


@pytest.mark.asyncio
async def test_reorder_with_completion_commands_reaches_db_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _application(
        monkeypatch,
        COMPLETION_COMMANDS_ENABLED="true",
        COMPLETION_STATUS_REORDER_ENABLED="true",
    )
    settings = app.state.resources.settings
    assert settings.completion_commands_enabled is True
    assert settings.completion_status_reorder_enabled is True

    with patch.object(
        app.state.resources.postgres_db,
        "connect",
        AsyncMock(side_effect=RuntimeError("stop after dependency check")),
    ) as connect:
        with pytest.raises(RuntimeError, match="stop after dependency check"):
            async with lifecycle.lifespan(app):
                pass

    connect.assert_awaited_once()
