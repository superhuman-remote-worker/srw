"""Orchestrator wiring for the always-on session-memory outbox drain."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import config_resolver as config_resolver_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import grant_enforcement as grant_enforcement_module
from orchestrator.services import (
    session_config_resolution as session_config_resolution_module,
)
from orchestrator.services import (
    thread_project_authorization as thread_project_authorization_module,
)

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

import orchestrator.main  # noqa: E402

THREAD_ID = UUID("11111111-aaaa-4111-8111-111111111111")
PROJECT_ID = UUID("22222222-bbbb-4222-8222-222222222222")
USER_ID = UUID("33333333-cccc-4333-8333-333333333333")


def test_executor_import_does_not_require_agent_only_aiosqlite() -> None:
    """The always-on orchestrator drain must import in its production image."""

    script = """
import builtins
original = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'aiosqlite' or name.startswith('aiosqlite.'):
        raise ModuleNotFoundError('agent-only dependency blocked')
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
import orchestrator.services.session_memory_executor
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).parent.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_orchestrator_images_smoke_import_always_on_drain() -> None:
    root = Path(__file__).parent.parent
    import_probe = (
        'RUN python -c "import orchestrator.services.session_memory_executor"'
    )
    for relative in (
        "docker/Dockerfile.orchestrator",
        "docker/Dockerfile.orchestrator.dev",
    ):
        assert import_probe in (root / relative).read_text()


def test_drain_singleton_wires_app_vector_and_fresh_resolver() -> None:
    orchestrator.main.app.state.resources.session_memory_runtime.reset()
    executor_instance = object()
    drain_instance = object()
    with (
        patch(
            "orchestrator.services.session_memory_executor.SessionMemoryEffectExecutor",
            return_value=executor_instance,
        ) as executor_cls,
        patch(
            "orchestrator.services.session_memory_effects.SessionMemoryEffectDrain",
            return_value=drain_instance,
        ) as drain_cls,
    ):
        first = orchestrator.main.app.state.resources.session_memory_runtime.drain()
        second = orchestrator.main.app.state.resources.session_memory_runtime.drain()

    assert first is drain_instance
    assert second is drain_instance
    executor_cls.assert_called_once_with(
        orchestrator.main.app.state.resources.postgres_db,
        orchestrator.main.app.state.resources.vector_db,
        orchestrator.main.app.state.resources.session_memory_runtime.resolve_effect_config,
    )
    drain_cls.assert_called_once_with(
        orchestrator.main.app.state.resources.postgres_db, executor_instance
    )
    orchestrator.main.app.state.resources.session_memory_runtime.reset()


@pytest.mark.asyncio
async def test_config_resolver_reauthorizes_captured_project_without_redirect() -> None:
    thread = {
        "id": THREAD_ID,
        "user_id": USER_ID,
        "project_id": UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        "metadata": {"config_override": {"memory": {"enabled": True}}},
    }
    resolved = {"agent": {"agent_id": "session"}}
    owner = {"id": USER_ID, "is_admin": False}

    with (
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_project",
            AsyncMock(return_value={}),
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_user",
            AsyncMock(return_value=owner),
        ),
        patch.object(
            thread_project_authorization_module,
            "authorize_thread_project_ids",
            AsyncMock(return_value=[str(PROJECT_ID)]),
        ) as authorize,
        patch.object(
            session_config_resolution_module,
            "resolve_session_config",
            AsyncMock(return_value=resolved),
        ) as session_resolve,
    ):
        result = await orchestrator.main.app.state.resources.session_memory_runtime.resolve_effect_config(
            thread, "project", PROJECT_ID
        )

    assert result is resolved
    # The authorization is the owner's, bound by the composition to this
    # application's store.
    authorize.assert_awaited_once()
    assert authorize.await_args.args == (owner, [str(PROJECT_ID)])
    assert set(authorize.await_args.kwargs) == {"dependencies"}
    assert (
        authorize.await_args.kwargs["dependencies"].store
        is orchestrator.main.app.state.resources.postgres_db
    )
    resolved_thread = session_resolve.await_args.args[0]
    assert resolved_thread["project_id"] == PROJECT_ID
    assert thread["project_id"] != PROJECT_ID
    assert session_resolve.await_args.kwargs["resolve_base_when_experts_disabled"]


@pytest.mark.asyncio
async def test_config_resolver_missing_captured_project_is_retryable() -> None:
    with (
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_project",
            AsyncMock(return_value=None),
        ),
        patch.object(
            session_config_resolution_module, "resolve_session_config", AsyncMock()
        ) as resolve,
    ):
        with pytest.raises(RuntimeError, match="project no longer exists"):
            await orchestrator.main.app.state.resources.session_memory_runtime.resolve_effect_config(
                {"id": THREAD_ID, "metadata": {}}, "project", PROJECT_ID
            )

    resolve.assert_not_awaited()


@pytest.mark.asyncio
async def test_config_resolver_project_scope_requires_thread_owner() -> None:
    with (
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "get_project",
            AsyncMock(return_value={}),
        ),
        patch.object(
            orchestrator.main.app.state.resources.postgres_db, "get_user", AsyncMock()
        ) as get_user,
        patch.object(
            thread_project_authorization_module,
            "authorize_thread_project_ids",
            AsyncMock(),
        ) as authorize,
        patch.object(
            session_config_resolution_module, "resolve_session_config", AsyncMock()
        ) as resolve,
    ):
        with pytest.raises(RuntimeError, match="requires an owning user"):
            await orchestrator.main.app.state.resources.session_memory_runtime.resolve_effect_config(
                {"id": THREAD_ID, "user_id": None, "metadata": {}},
                "project",
                PROJECT_ID,
            )

    get_user.assert_not_awaited()
    authorize.assert_not_awaited()
    resolve.assert_not_awaited()


@pytest.mark.asyncio
async def test_experts_off_default_keeps_attach_fallback() -> None:
    status: dict[str, object] = {}
    with (
        patch.object(
            orchestrator.main.app.state.resources.postgres_db,
            "fetchrow",
            AsyncMock(return_value=None),
        ),
        patch.object(
            deployment_gates_module, "is_experts_db_enabled", return_value=False
        ),
        patch.object(
            grant_enforcement_module, "user_experts_enabled", AsyncMock()
        ) as user_gate,
        patch.object(config_resolver_module, "resolve_config", MagicMock()) as resolve,
    ):
        result = await session_config_resolution_module.resolve_session_config(
            {"id": THREAD_ID},
            {},
            status=status,
            dependencies=preparation_composition.session_config_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert result is None
    assert status == {"state": "disabled"}
    user_gate.assert_not_awaited()
    resolve.assert_not_called()
