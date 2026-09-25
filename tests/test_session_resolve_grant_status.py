"""_resolve_session_config records grant violations for the drift collector."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.services.grant_enforcement import GrantDenied
import orchestrator.main
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import (
    session_config_resolution as session_config_resolution_module,
)


THREAD = {"id": "11111111-1111-4111-8111-111111111111", "user_id": "u1"}


@pytest.mark.asyncio
async def test_grant_denied_records_violations_in_status():
    violations = ["shell_tools: tools.shell requires the shell_tools grant"]
    status: dict = {}

    with (
        patch(
            "orchestrator.services.deployment_gates.is_experts_db_enabled",
            return_value=True,
        ),
        patch(
            "orchestrator.services.grant_enforcement.user_experts_enabled",
            AsyncMock(return_value=True),
        ),
        # Real collaborators hit postgres_db for account-default / skills
        # lookups that are irrelevant to this test (it only cares about the
        # except-block wiring below the grant call). Stub them so the resolve
        # reaches _enforce_dispatch_grants without needing a live DB.
        # R1.B05 lane P: account defaults are a SAME-MODULE sibling of the
        # resolve, so once main delegates to
        # ``services.session_config_resolution`` only the second patch is
        # reached. Both are listed so the stub steers either way; the first
        # entry is the transitional one the integrator drops.
        patch(
            "orchestrator.services.session_config_resolution"
            ".resolve_session_account_defaults",
            AsyncMock(return_value={}),
        ),
        # R1.B12: the skill gatherer is the catalogue service's method, reached
        # through ``catalogue.expert_catalog_service(resources)`` per call.
        patch(
            "orchestrator.services.expert_catalog.ExpertCatalogService"
            ".gather_in_scope_skills",
            AsyncMock(return_value={}),
        ),
        patch(
            "orchestrator.services.grant_enforcement.enforce_dispatch_grants",
            AsyncMock(side_effect=GrantDenied(violations)),
        ),
        patch(
            "orchestrator.main.app.state.resources.postgres_db",
            fetchrow=AsyncMock(return_value=None),
        ),
    ):
        with pytest.raises(GrantDenied):
            await session_config_resolution_module.resolve_session_config(
                THREAD,
                {},
                status=status,
                dependencies=preparation_composition.session_config_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )

    assert status["state"] == "denied"
    assert status["grant_violations"] == violations
