"""App-local preference persistence, identity and default configuration."""

import asyncio
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI

from orchestrator.routers.preferences import (
    PreferencesDependencies,
    get_preferences_dependencies,
    router,
)
from orchestrator.security import auth
from orchestrator.routers import preferences as preferences_module


PATH = "/api/settings/preferences"


def dependencies_for(index):
    db = SimpleNamespace(
        get_user_settings=AsyncMock(return_value={"saved": index}),
        update_user_settings=AsyncMock(return_value=True),
        resolve_default_for_capability=AsyncMock(
            side_effect=lambda capability: f"registry-{index}-{capability}"
        ),
    )
    return PreferencesDependencies(
        db=db,
        role_base=lambda role: {
            "autonomy": f"autonomy-{index}",
            "llm": {
                "model": f"yaml-{index}-{role}",
                "reasoning_level": f"reasoning-{index}",
            },
        },
        environ={
            "VISION_MODEL": f"vision-{index}",
            "EMBEDDING_PROVIDER": f"embedding-provider-{index}",
        },
    )


def app_for(dependencies):
    app = FastAPI()
    app.state.preferences_dependencies = dependencies
    app.include_router(router)
    return app


def client_for(app):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://preferences.test"
    )


@pytest.mark.asyncio
async def test_two_concurrent_apps_keep_identity_settings_and_defaults_independent(
    monkeypatch,
):
    dependencies = [dependencies_for(index) for index in range(2)]
    users = [{"id": uuid4(), "is_approved": True, "is_admin": False} for _ in range(2)]
    entered = [asyncio.Event(), asyncio.Event()]

    async def identity(_request, db):
        index = next(
            index
            for index, dependency in enumerate(dependencies)
            if dependency.db is db
        )
        entered[index].set()
        await entered[1 - index].wait()
        return users[index]

    monkeypatch.setattr(auth, "get_current_user", identity)
    async with (
        client_for(app_for(dependencies[0])) as first,
        client_for(app_for(dependencies[1])) as second,
    ):
        clients = (first, second)
        responses = await asyncio.wait_for(
            asyncio.gather(*(client.get(PATH) for client in clients)), 3
        )
        for index, response in enumerate(responses):
            assert response.status_code == 200
            body = response.json()
            assert body["saved"] == index
            assert body["_resolved"]["default_model"] == f"registry-{index}-chat"
            assert (
                body["_resolved"]["default_auxiliary_model"]
                == f"registry-{index}-auxiliary"
            )
            assert body["_resolved"]["default_tts_model"] == f"registry-{index}-tts"
            assert body["_resolved"]["default_vision_model"] == f"vision-{index}"
            assert body["_resolved"]["default_autonomy"] == f"autonomy-{index}"
            assert body["_resolved"]["default_reasoning_level"] == f"reasoning-{index}"
            assert (
                body["_resolved"]["embedding_provider"] == f"embedding-provider-{index}"
            )
            dependencies[index].db.get_user_settings.assert_awaited_once_with(
                str(users[index]["id"])
            )
        for event in entered:
            event.clear()
        responses = await asyncio.wait_for(
            asyncio.gather(
                first.patch(PATH, json={"language": "en"}),
                second.patch(PATH, json={"language": "de-DE"}),
            ),
            3,
        )
    assert [response.json() for response in responses] == [
        {"status": "updated"},
        {"status": "updated"},
    ]
    for index, language in enumerate(("en", "de-DE")):
        dependencies[index].db.update_user_settings.assert_awaited_once_with(
            str(users[index]["id"]), {"language": language}
        )


@pytest.mark.asyncio
async def test_override_replaces_only_one_apps_identity_database_and_defaults(
    monkeypatch,
):
    original, replacement = (
        dependencies_for("original"),
        dependencies_for("replacement"),
    )
    first, second = app_for(original), app_for(original)
    first.dependency_overrides[get_preferences_dependencies] = lambda: replacement

    async def identity(_request, db):
        return {
            "id": "replacement-user" if db is replacement.db else "original-user",
            "is_approved": True,
            "is_admin": False,
        }

    monkeypatch.setattr(auth, "get_current_user", identity)
    async with client_for(first) as first_client, client_for(second) as second_client:
        replaced, untouched = await asyncio.gather(
            first_client.get(PATH), second_client.get(PATH)
        )
    assert replaced.json()["saved"] == "replacement"
    assert replaced.json()["_resolved"]["default_model"] == "registry-replacement-chat"
    assert untouched.json()["saved"] == "original"
    assert untouched.json()["_resolved"]["default_model"] == "registry-original-chat"
    replacement.db.get_user_settings.assert_awaited_once_with("replacement-user")
    original.db.get_user_settings.assert_awaited_once_with("original-user")


def test_canonical_router_import_is_inert_without_main_or_feature_providers(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            """
import importlib.abc
import sys

forbidden = (
    "orchestrator.main", "agent", "langgraph", "langchain",
    "langchain_core", "langchain_openai",
    "orchestrator.services.tts", "orchestrator.services.notification_catalog",
    "shared.runtime.core.loader",
)

def is_forbidden(fullname):
    return any(fullname == name or fullname.startswith(name + ".") for name in forbidden)

assert not [name for name in sys.modules if is_forbidden(name)]

class Blocker(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if is_forbidden(fullname):
            raise AssertionError("Unexpected startup/provider import: " + fullname)
        return None

sys.meta_path.insert(0, Blocker())
from orchestrator.routers.preferences import UserSettingsUpdate, router
from orchestrator.services.preference_defaults import resolve_preference_defaults
from orchestrator.services.session_workspace_policy import (
    SESSION_WORKSPACE_BACKENDS, SESSION_DEFAULT_WORKSPACE_BACKEND,
    SESSION_CREATE_WORKSPACE_BACKENDS,
)

assert UserSettingsUpdate(language="de-DE").language == "de-DE"
assert SESSION_WORKSPACE_BACKENDS == ("sandbox", "virtual", "none")
assert SESSION_DEFAULT_WORKSPACE_BACKEND == "virtual"
assert SESSION_CREATE_WORKSPACE_BACKENDS == ("sandbox", "virtual", "none", "vm")
assert len(router.routes) == 2
assert not [name for name in sys.modules if is_forbidden(name)]
""",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_workspace_policy_constants_are_consumed_from_their_owner():
    """R1.B12: ``orchestrator.main`` is only the entrypoint and re-exports
    nothing; the preferences router and the defaults resolver must read the
    tier constants from ``session_workspace_policy`` itself, not a copy."""
    from orchestrator import main
    from orchestrator.routers.preferences import UserSettingsUpdate
    from orchestrator.services import preference_defaults, session_workspace_policy

    assert preferences_module.UserSettingsUpdate is UserSettingsUpdate
    assert (
        preferences_module.SESSION_WORKSPACE_BACKENDS
        is session_workspace_policy.SESSION_WORKSPACE_BACKENDS
    )
    assert (
        preference_defaults.SESSION_DEFAULT_WORKSPACE_BACKEND
        is session_workspace_policy.SESSION_DEFAULT_WORKSPACE_BACKEND
    )
    for name in (
        "SESSION_WORKSPACE_BACKENDS",
        "SESSION_DEFAULT_WORKSPACE_BACKEND",
        "SESSION_CREATE_WORKSPACE_BACKENDS",
    ):
        assert not hasattr(main, name)
