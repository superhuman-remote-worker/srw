"""P4d — admin-only infrastructure endpoints.

Eight endpoints that expose either global state (sudo rules, expert
configs, all VMs) or raw postgres rows (table dumps) — must be admin
only. Pre-P4d the lint flagged all 8 as unscoped.

* `GET    /api/tables`                          → raw postgres tables list
* `GET    /api/tables/{name}`                   → raw postgres rows
* `GET    /api/tables/{name}/schema`            → raw postgres schema
* `GET    /api/sudo/rules`                      → global auto-approval rules
* `POST   /api/sudo/rules`                      → create rule
* `DELETE /api/sudo/rules/{rule_id}`            → delete rule
* `POST   /api/experts/reload`                  → reload expert YAML from disk
* `GET    /api/vms`                             → list all VMs across users

Tests share the 3-user fixture from ``conftest.py``. Non-admin caller
(user_a) gets 403; admin (user_admin) clears the gate. The downstream
service mocks are wired to fail loudly if the gate doesn't fire.
"""

from tests._expert_catalog import catalogue_route
from orchestrator.routers import expert_catalog as expert_routes
from orchestrator.services import expert_catalog as expert_catalog_module


from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException


def _patch_caller_and_db(user: dict, db):
    stack = ExitStack()
    stack.enter_context(
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    return stack


def _exploding(name: str):
    """A mock that errors on attribute access — proves the gate is what
    blocked the call rather than the downstream service erroring out."""
    return MagicMock(side_effect=AssertionError(f"{name} called past gate"))


class TestAdminInfraGates:
    # ----- /api/tables family -----
    @pytest.mark.asyncio
    async def test_list_tables_non_admin_403(self, user_a, fake_db, fake_request):
        from orchestrator.routers.tables import TablesDependencies, list_tables

        fake_db.get_tables = AsyncMock(
            side_effect=AssertionError("get_tables called past gate")
        )
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await list_tables(
                    fake_request, dependencies=TablesDependencies(db=fake_db)
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_table_data_non_admin_403(self, user_a, fake_db, fake_request):
        from orchestrator.routers.tables import TablesDependencies, get_table_data

        fake_db.get_table_data = AsyncMock(
            side_effect=AssertionError("get_table_data called past gate")
        )
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await get_table_data(
                    fake_request, "users", dependencies=TablesDependencies(db=fake_db)
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_table_schema_non_admin_403(self, user_a, fake_db, fake_request):
        from orchestrator.routers.tables import TablesDependencies, get_table_schema

        fake_db.get_table_schema = AsyncMock(
            side_effect=AssertionError("get_table_schema called past gate")
        )
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await get_table_schema(
                    fake_request, "users", dependencies=TablesDependencies(db=fake_db)
                )
        assert exc.value.status_code == 403

    # ----- /api/sudo/rules family -----
    @pytest.mark.asyncio
    async def test_list_sudo_rules_non_admin_403(self, user_a, fake_db, fake_request):
        from tests._b09_control_seams import list_sudo_rules

        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", _exploding("sudo_gate")),
        ):
            with pytest.raises(HTTPException) as exc:
                await list_sudo_rules(fake_request)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_create_sudo_rule_non_admin_403(self, user_a, fake_db, fake_request):
        from orchestrator.schemas.job_controls import SudoRuleCreateRequest
        from tests._b09_control_seams import create_sudo_rule

        body = SudoRuleCreateRequest(
            pattern="apt-get *", action="approve", priority=10, description="test"
        )
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", _exploding("sudo_gate")),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_sudo_rule(fake_request, body)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_sudo_rule_non_admin_403(self, user_a, fake_db, fake_request):
        from tests._b09_control_seams import delete_sudo_rule

        with (
            _patch_caller_and_db(user_a, fake_db),
            patch("orchestrator.services.sudo_gate.sudo_gate", _exploding("sudo_gate")),
        ):
            with pytest.raises(HTTPException) as exc:
                await delete_sudo_rule(fake_request, "rule-abc")
        assert exc.value.status_code == 403

    # ----- /api/experts/reload -----
    @pytest.mark.asyncio
    async def test_reload_experts_non_admin_403(self, user_a, fake_db, fake_request):
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch.object(
                expert_catalog_module.ExpertCatalogService,
                "scan_experts",
                _exploding("_scan_experts"),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await catalogue_route(expert_routes.reload_experts)(fake_request)
        assert exc.value.status_code == 403

    # ----- /api/vms -----
    @pytest.mark.asyncio
    async def test_list_vms_non_admin_403(self, user_a, fake_db, fake_request):
        from tests._b09_control_seams import list_vms

        # The handler uses postgres_db.acquire() as a context manager; the
        # gate fires before that so we don't need a deeper mock.
        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await list_vms(fake_request)
        assert exc.value.status_code == 403

    # ----- One representative admin-pass to prove we don't over-block -----
    @pytest.mark.asyncio
    async def test_list_tables_admin_passes_gate(
        self, user_admin, fake_db, fake_request
    ):
        from orchestrator.routers.tables import TablesDependencies, list_tables

        fake_db.get_tables = AsyncMock(return_value=[{"name": "users", "rows": 0}])
        with _patch_caller_and_db(user_admin, fake_db):
            result = await list_tables(
                fake_request, dependencies=TablesDependencies(db=fake_db)
            )
        assert result == [{"name": "users", "rows": 0}]
