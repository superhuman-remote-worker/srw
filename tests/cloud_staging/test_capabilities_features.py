"""GET /api/users/me/capabilities — ``features.protected_cloud`` gate (Task 13).

The capabilities payload gains a ``"features": {"protected_cloud": bool}``
key mirroring ``_is_protected_cloud_mode_enabled()`` in BOTH the admin and
non-admin response branches — the Cockpit toggle gate for the protected
cloud session-create checkbox reads this.

The handler moved into ``orchestrator.routers.user_administration`` (R1.B02),
so the cases call it with ``main._user_administration_dependencies()``. That
factory resolves the feature-flag callables per invocation, which is why
patching them on ``orchestrator.main`` still reaches the endpoint.
"""

from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import pytest

import orchestrator.main
from orchestrator.routers import user_administration as user_administration_routes
from orchestrator.application import administration as administration_composition
from orchestrator.services import deployment_gates as deployment_gates_module


def _patch_capabilities(
    *,
    user: dict,
    flag: bool,
    datasource_scope_flag: bool = False,
    datasource_defaults_flag: bool = False,
    grants: dict | None = None,
) -> ExitStack:
    stack = ExitStack()
    stack.enter_context(
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            lambda: flag,
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.services.deployment_gates.datasource_scope_auto_attach_v1_enabled",
            lambda: datasource_scope_flag,
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.services.deployment_gates.datasource_defaults_on_omission",
            lambda: datasource_defaults_flag,
        )
    )
    # _grant_project_ids -> user_visible_project_ids -> postgres_db; short-
    # circuit it so the non-admin branch doesn't need a real db.
    stack.enter_context(
        patch(
            "orchestrator.services.grant_enforcement.grant_project_ids",
            AsyncMock(return_value=[]),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.services.grants_service.resolve_grants_for",
            AsyncMock(return_value=grants or {}),
        )
    )
    return stack


class TestCapabilitiesFeatures:
    @pytest.mark.asyncio
    async def test_admin_features_flag_on(self, user_admin, fake_request):
        stack = _patch_capabilities(user=user_admin, flag=True)
        with stack:
            result = await user_administration_routes.my_capabilities(
                fake_request,
                dependencies=administration_composition.user_administration_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        assert result["is_admin"] is True
        assert result["features"] == {
            "protected_cloud": True,
            "datasource_scope_auto_attach_v1": False,
            "datasource_defaults_on_omission": False,
        }

    @pytest.mark.asyncio
    async def test_admin_features_flag_off(self, user_admin, fake_request):
        stack = _patch_capabilities(user=user_admin, flag=False)
        with stack:
            result = await user_administration_routes.my_capabilities(
                fake_request,
                dependencies=administration_composition.user_administration_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        assert result["is_admin"] is True
        assert result["features"] == {
            "protected_cloud": False,
            "datasource_scope_auto_attach_v1": False,
            "datasource_defaults_on_omission": False,
        }

    @pytest.mark.asyncio
    async def test_non_admin_features_flag_on(self, user_a, fake_request):
        stack = _patch_capabilities(user=user_a, flag=True)
        with stack:
            result = await user_administration_routes.my_capabilities(
                fake_request,
                dependencies=administration_composition.user_administration_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        assert result["is_admin"] is False
        assert result["features"] == {
            "protected_cloud": True,
            "datasource_scope_auto_attach_v1": False,
            "datasource_defaults_on_omission": False,
        }

    @pytest.mark.asyncio
    async def test_non_admin_features_flag_off(self, user_a, fake_request):
        stack = _patch_capabilities(user=user_a, flag=False)
        with stack:
            result = await user_administration_routes.my_capabilities(
                fake_request,
                dependencies=administration_composition.user_administration_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        assert result["is_admin"] is False
        assert result["features"] == {
            "protected_cloud": False,
            "datasource_scope_auto_attach_v1": False,
            "datasource_defaults_on_omission": False,
        }

    @pytest.mark.asyncio
    async def test_datasource_scope_feature_reports_rollout_gate(
        self, user_admin, fake_request
    ):
        stack = _patch_capabilities(
            user=user_admin,
            flag=False,
            datasource_scope_flag=True,
        )
        with stack:
            result = await user_administration_routes.my_capabilities(
                fake_request,
                dependencies=administration_composition.user_administration_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )

        assert result["features"]["datasource_scope_auto_attach_v1"] is True


def test_datasource_scope_feature_gate_defaults_off(monkeypatch):
    monkeypatch.delenv("DATASOURCE_SCOPE_AUTO_ATTACH_V1_ENABLED", raising=False)
    assert deployment_gates_module.datasource_scope_auto_attach_v1_enabled() is False


def test_datasource_scope_feature_gate_accepts_true(monkeypatch):
    monkeypatch.setenv("DATASOURCE_SCOPE_AUTO_ATTACH_V1_ENABLED", "true")
    assert deployment_gates_module.datasource_scope_auto_attach_v1_enabled() is True
