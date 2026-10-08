"""Baseline HTTP/default contracts for the planned preferences extraction."""

from copy import deepcopy
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import httpx
import pytest
from fastapi import HTTPException
from fastapi.openapi.utils import get_openapi

from orchestrator.security import auth


PATH = "/api/settings/preferences"
ROLE_BASES = {
    "worker": {
        "llm": {"model": "yaml-worker", "reasoning_level": "medium"},
        "auxiliary": {"model": "yaml-aux"},
        "autonomy": "full",
    },
    "session": {"llm": {"model": "yaml-session"}},
}
EXPECTED_DEFAULTS = {
    "default_model": "registry-chat",
    "default_autonomy": "full",
    "default_reasoning_level": "medium",
    "default_auxiliary_model": "registry-aux",
    "default_vision_model": "registry-vision",
    "default_whisper_model": "env-whisper",
    "default_tts_model": "registry-tts",
    "default_embedding_model": "registry-embedding",
    "embedding_provider": "env-provider",
    "admin_view_mode": "all",
    "persistent_agent": {
        "model": "registry-chat",
        "permission_mode": "supervised",
        "idle_timeout_minutes": 30,
    },
}


@pytest.fixture
def preferences_api(monkeypatch, user_a):
    from orchestrator import main
    from orchestrator.routers.preferences import PreferencesDependencies

    db = SimpleNamespace(
        get_user_settings=AsyncMock(
            side_effect=lambda _user: {
                "language": "de-DE",
                "legacy": {"retained": True},
            }
        ),
        update_user_settings=AsyncMock(return_value=True),
        resolve_default_for_capability=AsyncMock(
            side_effect={
                "chat": "registry-chat",
                "auxiliary": "registry-aux",
                "vision": "registry-vision",
                "tts": "registry-tts",
                "embedding": "registry-embedding",
                # No whisper default: WHISPER_MODEL is the fallback.
            }.get
        ),
    )
    identity = AsyncMock(return_value=user_a)
    roles = Mock(side_effect=lambda role: deepcopy(ROLE_BASES[role]))
    monkeypatch.setattr(
        main.app.state,
        "preferences_dependencies",
        PreferencesDependencies(db=db, role_base=roles, environ=os.environ),
    )
    monkeypatch.setattr(auth, "get_current_user", identity)
    for key, value in {
        "VISION_MODEL": "env-vision",
        "WHISPER_MODEL": "env-whisper",
        "TTS_MODEL": "env-tts",
        "EMBEDDING_MODEL": "env-embedding",
        "EMBEDDING_PROVIDER": "env-provider",
    }.items():
        monkeypatch.setenv(key, value)
    return SimpleNamespace(
        main=main, app=main.app, db=db, identity=identity, roles=roles, user=user_a
    )


async def request(api, method, body=None):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api.app), base_url="http://preferences.test"
    ) as client:
        return await client.request(method, PATH, json=body)


def assert_no_settings_io(api):
    api.db.get_user_settings.assert_not_awaited()
    api.db.update_user_settings.assert_not_awaited()
    api.db.resolve_default_for_capability.assert_not_awaited()
    api.roles.assert_not_called()


@pytest.mark.parametrize(
    "method,body", [("GET", None), ("PATCH", {"language": "en"}), ("PATCH", {})]
)
@pytest.mark.asyncio
async def test_unauthenticated_precedes_settings_and_empty_patch_check(
    preferences_api, method, body
):
    api = preferences_api
    api.identity.side_effect = HTTPException(401, "Not authenticated")
    response = await request(api, method, body)
    assert response.status_code == 401
    assert response.json() == {"detail": "Not authenticated"}
    assert api.identity.await_args.args[1] is api.db
    assert_no_settings_io(api)


@pytest.mark.parametrize("method,body", [("GET", None), ("PATCH", {"language": "en"})])
@pytest.mark.asyncio
async def test_pending_approval_precedes_settings_io(preferences_api, method, body):
    api = preferences_api
    api.identity.return_value = {**api.user, "is_approved": False}
    response = await request(api, method, body)
    assert response.status_code == 403
    assert response.json() == {
        "detail": "Account pending approval. An administrator must approve your account."
    }
    assert_no_settings_io(api)


@pytest.mark.asyncio
async def test_get_preserves_saved_preferences_and_registry_defaults(preferences_api):
    api = preferences_api
    response = await request(api, "GET")
    assert response.status_code == 200
    assert response.json() == {
        "language": "de-DE",
        "legacy": {"retained": True},
        "_resolved": EXPECTED_DEFAULTS,
        "_unavailable": {},
    }
    api.db.get_user_settings.assert_awaited_once_with(str(api.user["id"]))
    assert api.roles.call_args_list == [call("worker"), call("session")]
    assert api.db.resolve_default_for_capability.await_args_list == [
        call("chat"),
        call("auxiliary"),
        call("vision"),
        call("whisper"),
        call("tts"),
        call("embedding"),
    ]
    api.db.update_user_settings.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_hides_the_retired_workspace_backend(preferences_api):
    api = preferences_api
    api.db.get_user_settings.side_effect = None
    api.db.get_user_settings.return_value = {
        "persistent_agent": {"model": "p", "workspace_backend": "sandbox"}
    }
    response = await request(api, "GET")
    assert response.json()["persistent_agent"] == {"model": "p"}
    assert "workspace_backend" not in response.json()["_resolved"]["persistent_agent"]


@pytest.mark.asyncio
async def test_get_overwrites_stored_resolved_but_does_not_persist_it(preferences_api):
    api = preferences_api
    api.db.get_user_settings.side_effect = None
    api.db.get_user_settings.return_value = {"_resolved": {"forged": True}}
    response = await request(api, "GET")
    assert response.json() == {"_resolved": EXPECTED_DEFAULTS, "_unavailable": {}}
    api.db.update_user_settings.assert_not_awaited()


@pytest.mark.asyncio
async def test_registry_miss_falls_back_to_role_and_helper_environment(preferences_api):
    api = preferences_api
    api.db.resolve_default_for_capability.side_effect = None
    api.db.resolve_default_for_capability.return_value = None
    response = await request(api, "GET")
    expected = deepcopy(EXPECTED_DEFAULTS)
    expected.update(
        default_model="yaml-worker",
        default_auxiliary_model="yaml-aux",
        default_vision_model="env-vision",
        default_tts_model="env-tts",
        default_embedding_model="env-embedding",
    )
    expected["persistent_agent"]["model"] = "yaml-session"
    assert response.json()["_resolved"] == expected


@pytest.mark.asyncio
async def test_empty_role_defaults_preserve_none_and_platform_fallbacks(
    preferences_api, monkeypatch
):
    api = preferences_api
    api.roles.side_effect = None
    api.roles.return_value = {}
    api.db.resolve_default_for_capability.side_effect = None
    api.db.resolve_default_for_capability.return_value = None
    for key in (
        "VISION_MODEL",
        "WHISPER_MODEL",
        "TTS_MODEL",
        "EMBEDDING_MODEL",
        "EMBEDDING_PROVIDER",
    ):
        monkeypatch.delenv(key)
    response = await request(api, "GET")
    expected = deepcopy(EXPECTED_DEFAULTS)
    expected.update(
        default_model=None,
        default_autonomy=None,
        default_reasoning_level=None,
        default_auxiliary_model=None,
        # No registry default and no env: no model name is invented.
        default_vision_model=None,
        default_whisper_model=None,
        default_tts_model=None,
        default_embedding_model=None,
        embedding_provider="local",
    )
    expected["persistent_agent"]["model"] = None
    assert response.json()["_resolved"] == expected


@pytest.mark.parametrize(
    "body,expected",
    [
        ({"language": "en"}, {"language": "en"}),
        ({"language": None}, {"language": None}),
        ({"persistent_agent": None}, {"persistent_agent": None}),
        ({"persistent_agent": {}}, {"persistent_agent": {}}),
        (
            {
                "persistent_agent": {
                    "workspace_backend": "none",
                    "greeting": "legacy",
                    "future": 7,
                }
            },
            {
                "persistent_agent": {
                    "greeting": "legacy",
                    "future": 7,
                }
            },
        ),
        (
            {"persistent_agent": {"model": "p", "workspace_backend": "sandbox"}},
            {"persistent_agent": {"model": "p"}},
        ),
        (
            {"communication": {"channels": {"email": False}, "future": 7}},
            {"communication": {"channels": {"email": False}, "future": 7}},
        ),
        (
            {
                "communication": {
                    "categories": {"custom": {"email": False}},
                    "escalation_minutes": 1440,
                }
            },
            {
                "communication": {
                    "categories": {"custom": {"email": False}},
                    "escalation_minutes": 1440,
                }
            },
        ),
        (
            {"read_aloud": {"reasoning_level": "HIGH", "custom_prompt": "read this"}},
            {"read_aloud": {"reasoning_level": "high", "custom_prompt": "read this"}},
        ),
        (
            {
                "language": "de-DE",
                "default_chat_model": "removed",
                "user_id": "other-user",
            },
            {"language": "de-DE"},
        ),
    ],
)
@pytest.mark.asyncio
async def test_patch_forwards_only_explicit_fields_to_current_user(
    preferences_api, body, expected
):
    api = preferences_api
    response = await request(api, "PATCH", body)
    assert response.status_code == 200
    assert response.json() == {"status": "updated"}
    api.db.update_user_settings.assert_awaited_once_with(str(api.user["id"]), expected)
    api.db.get_user_settings.assert_not_awaited()
    api.db.resolve_default_for_capability.assert_not_awaited()
    api.roles.assert_not_called()


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"unknown": 1},
        {"default_chat_model": "removed"},
        {"default_session_model": "removed"},
        {"default_strategic_model": "removed"},
    ],
)
@pytest.mark.asyncio
async def test_empty_or_only_unknown_patch_remains_400_after_auth(
    preferences_api, body
):
    response = await request(preferences_api, "PATCH", body)
    assert response.status_code == 400
    assert response.json() == {"detail": "No settings provided"}
    preferences_api.identity.assert_awaited_once()
    assert_no_settings_io(preferences_api)


@pytest.mark.parametrize(
    "body,field",
    [
        ({"read_aloud": {"reasoning_level": "ultra"}}, "read_aloud"),
        ({"read_aloud": {"custom_prompt": "x" * 1001}}, "read_aloud"),
        ({"communication": {"channels": {"email": "false"}}}, "communication"),
        ({"communication": {"categories": {"custom": {"email": 0}}}}, "communication"),
        ({"communication": {"escalation_minutes": True}}, "communication"),
        ({"communication": {"escalation_minutes": 1441}}, "communication"),
        ({"communication": {"quiet_hours": "not-object"}}, "communication"),
        ({"language": "de_DE"}, "language"),
        ({"admin_view_mode": "user"}, "admin_view_mode"),
    ],
)
@pytest.mark.asyncio
async def test_patch_model_validation_precedes_auth_and_writes(
    preferences_api, body, field
):
    api = preferences_api
    api.identity.side_effect = HTTPException(401, "Not authenticated")
    response = await request(api, "PATCH", body)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", field]
    api.identity.assert_not_awaited()
    assert_no_settings_io(api)


@pytest.mark.asyncio
async def test_update_false_result_does_not_add_new_not_found_policy(preferences_api):
    preferences_api.db.update_user_settings.return_value = False
    response = await request(preferences_api, "PATCH", {"language": "en"})
    assert response.status_code == 200
    assert response.json() == {"status": "updated"}


@pytest.mark.parametrize(
    "method,collaborator,body",
    [
        ("GET", "get_user_settings", None),
        ("GET", "resolve_default_for_capability", None),
        ("PATCH", "update_user_settings", {"language": "en"}),
    ],
)
@pytest.mark.asyncio
async def test_uncaught_database_errors_retain_composed_500_response(
    preferences_api, method, collaborator, body
):
    getattr(preferences_api.db, collaborator).side_effect = RuntimeError(
        "synthetic settings failure"
    )
    response = await request(preferences_api, method, body)
    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}


def preferences_openapi(api):
    # Let FastAPI resolve mounted routers before selecting the public contract.
    # Included routers need not expose leaf paths directly in app.routes.
    contract = get_openapi(
        title="Preferences contract", version="baseline", routes=api.app.routes
    )
    contract["paths"] = {
        path: operations
        for path, operations in contract["paths"].items()
        if path == PATH
    }
    return contract


def test_focused_openapi_contract(preferences_api):
    contract = preferences_openapi(preferences_api)
    assert set(contract["paths"]) == {PATH}
    operations = contract["paths"][PATH]
    assert set(operations) == {"get", "patch"}
    assert (
        operations["get"]["operationId"]
        == "get_user_preferences_api_settings_preferences_get"
    )
    assert (
        operations["patch"]["operationId"]
        == "update_user_preferences_api_settings_preferences_patch"
    )
    assert set(operations["get"]["responses"]) == {"200"}
    assert set(operations["patch"]["responses"]) == {"200", "422"}
    assert operations["patch"]["requestBody"] == {
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/UserSettingsUpdate"}
            }
        },
        "required": True,
    }
    schema = contract["components"]["schemas"]["UserSettingsUpdate"]
    assert "required" not in schema
    assert "default_chat_model" not in schema["properties"]
    assert "default_session_model" not in schema["properties"]
    assert (
        "communication" in schema["properties"] and "language" in schema["properties"]
    )


# ---------------------------------------------------------------------------
# Unavailable model preferences (unavailable_model_handling.md §7)
# ---------------------------------------------------------------------------


@pytest.fixture
def stale_registry(monkeypatch):
    """A live registry that knows only ``gpt-6-astra``; ``MiniMax-M3`` is a
    disabled catalog row."""
    from shared.runtime.core import model_registry
    from shared.runtime.core.model_registry import UnknownModelError

    async def resolve(model_id, user_id=None, capability="chat"):
        if model_id == "gpt-6-astra":
            return object()
        raise UnknownModelError(model_id)

    monkeypatch.setattr(model_registry, "lookups_registered", lambda: True)
    monkeypatch.setattr(model_registry, "resolve_model", AsyncMock(side_effect=resolve))


@pytest.mark.asyncio
async def test_patch_refuses_a_newly_chosen_unavailable_model(
    preferences_api, stale_registry
):
    api = preferences_api
    api.db.catalog_model_states = AsyncMock(
        return_value=[{"enabled": False, "capabilities": ["chat"]}]
    )
    response = await request(api, "PATCH", {"default_model": "MiniMax-M3"})
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "model.unavailable"
    assert "`MiniMax-M3` (main model) is no longer available" in detail["message"]
    assert "Settings → Preferences" in detail["message"]
    api.db.update_user_settings.assert_not_awaited()


@pytest.mark.asyncio
async def test_patch_lets_an_unchanged_stale_default_ride_along(
    preferences_api, stale_registry
):
    """The page saves every field at once: an already-stale stored default
    must not block an unrelated change."""
    api = preferences_api
    api.db.catalog_model_states = AsyncMock(
        return_value=[{"enabled": False, "capabilities": ["chat"]}]
    )
    api.db.get_user_settings.side_effect = None
    api.db.get_user_settings.return_value = {"default_model": "MiniMax-M3"}
    response = await request(
        api, "PATCH", {"default_model": "MiniMax-M3", "language": "en"}
    )
    assert response.status_code == 200
    api.db.update_user_settings.assert_awaited_once()


@pytest.mark.asyncio
async def test_patch_accepts_a_runnable_model_and_clearing(
    preferences_api, stale_registry
):
    api = preferences_api
    assert (
        await request(api, "PATCH", {"default_model": "gpt-6-astra"})
    ).status_code == 200
    assert (await request(api, "PATCH", {"default_model": None})).status_code == 200


@pytest.mark.asyncio
async def test_get_flags_a_stored_unavailable_model(preferences_api, stale_registry):
    api = preferences_api
    api.db.catalog_model_states = AsyncMock(
        return_value=[{"enabled": False, "capabilities": ["chat", "auxiliary"]}]
    )
    api.db.get_user_settings.side_effect = None
    api.db.get_user_settings.return_value = {
        "default_model": "MiniMax-M3",
        "default_auxiliary_model": "gpt-6-astra",
        "persistent_agent": {"model": "MiniMax-M3"},
    }
    response = await request(api, "GET")
    assert response.json()["_unavailable"] == {
        "default_model": "disabled",
        "persistent_agent.model": "disabled",
    }
