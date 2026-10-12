"""Tests for the admin VM workspace controls.

Covers:
- Route registration of the new admin endpoints.
- Pydantic model shape for `AdminUserUpdate`.
- The `_check_vm_permission` gate — kill-switch + per-user grant + admin bypass.

Full TestClient integration with DB + Keycloak is out of scope; we mock
`postgres_db` directly to exercise the gate helper on its own. End-to-end
coverage of the submit/dispatch paths is verified manually per the plan.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import job_workspace_runtime as job_workspace_runtime_module
from orchestrator.services import vm_workspace_policy as vm_workspace_policy_module

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

import orchestrator.main as orch_main  # noqa: E402
from orchestrator.main import app  # noqa: E402
from orchestrator.schemas.users import AdminUserUpdate  # noqa: E402
from tests._route_inventory import mounted_routes  # noqa: E402


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------


ADMIN_VM_ROUTES = {
    ("GET", "/api/admin/system-settings/vm_workspaces"),
    ("PUT", "/api/admin/system-settings/vm_workspaces"),
    ("GET", "/api/admin/users"),
    ("PATCH", "/api/admin/users/{user_id}"),
}


def _registered_routes() -> set[tuple[str, str]]:
    return mounted_routes(app)


class TestAdminVmRoutesRegistered:
    def test_every_admin_vm_route_is_wired(self):
        registered = _registered_routes()
        missing = [r for r in ADMIN_VM_ROUTES if r not in registered]
        assert not missing, f"missing admin VM routes: {missing}"


# ---------------------------------------------------------------------------
# Pydantic body
# ---------------------------------------------------------------------------


class TestAdminUserUpdate:
    def test_is_admin_is_not_a_settable_field(self):
        # Admin is owned by the Keycloak `admin` realm role and reconciled onto
        # users.is_admin per request; the app must not offer a write path that
        # would be silently clobbered. So is_admin is intentionally not a field.
        assert "is_admin" not in AdminUserUpdate.model_fields

    def test_accepts_partial_can_use_vm_only(self):
        body = AdminUserUpdate(can_use_vm=True)
        assert body.can_use_vm is True
        assert body.is_approved is None

    def test_accepts_empty_body(self):
        body = AdminUserUpdate()
        assert body.can_use_vm is None
        assert body.is_approved is None


# ---------------------------------------------------------------------------
# _check_vm_permission gate
# ---------------------------------------------------------------------------


def _patch_db(*, setting=None, setting_error=None):
    """Patch the application's ``postgres_db`` resource for the gate tests.

    The gate makes two async DB calls: ``get_system_setting`` (the global
    kill-switch) and ``user_can_use_vm`` (the per-user grant). The latter
    resolves capability grants and falls back to ``bool(user['can_use_vm'])``
    when none exist, so the mock mirrors that fallback — the per-user
    expectations below read straight off the user dict, as before.
    """
    mock_db = MagicMock()
    mock_db.get_system_setting = AsyncMock(
        side_effect=setting_error, return_value=setting
    )
    mock_db.user_can_use_vm = AsyncMock(
        side_effect=lambda u: bool(u and u.get("can_use_vm"))
    )
    return patch.object(orch_main.app.state.resources, "postgres_db", mock_db)


class TestCheckVmPermission:
    @pytest.mark.asyncio
    async def test_no_op_when_job_does_not_need_vm(self):
        """The gate short-circuits for non-VM jobs — no DB call, no raise."""
        with _patch_db(setting_error=AssertionError("should not read kill-switch")):
            await vm_workspace_policy_module.check_vm_permission(
                user=None,
                job_needs_vm=False,
                dependencies=preparation_composition.vm_permission_dependencies(
                    orch_main.app.state.resources
                ),
            )

    @pytest.mark.asyncio
    async def test_kill_switch_blocks_admin(self):
        admin = {"id": "u1", "is_admin": True, "can_use_vm": True}
        with _patch_db(setting={"value": {"enabled": False}}):
            with pytest.raises(HTTPException) as exc:
                await vm_workspace_policy_module.check_vm_permission(
                    admin,
                    job_needs_vm=True,
                    dependencies=preparation_composition.vm_permission_dependencies(
                        orch_main.app.state.resources
                    ),
                )
            assert exc.value.status_code == 403
            assert "globally disabled" in exc.value.detail

    @pytest.mark.asyncio
    async def test_kill_switch_blocks_granted_non_admin(self):
        user = {"id": "u2", "is_admin": False, "can_use_vm": True}
        with _patch_db(setting={"value": {"enabled": False}}):
            with pytest.raises(HTTPException) as exc:
                await vm_workspace_policy_module.check_vm_permission(
                    user,
                    job_needs_vm=True,
                    dependencies=preparation_composition.vm_permission_dependencies(
                        orch_main.app.state.resources
                    ),
                )
            assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_bypasses_per_user_grant(self):
        """Admin without can_use_vm still gets through when kill-switch is on."""
        admin = {"id": "u3", "is_admin": True, "can_use_vm": False}
        with _patch_db(setting={"value": {"enabled": True}}):
            await vm_workspace_policy_module.check_vm_permission(
                admin,
                job_needs_vm=True,
                dependencies=preparation_composition.vm_permission_dependencies(
                    orch_main.app.state.resources
                ),
            )

    @pytest.mark.asyncio
    async def test_non_admin_without_grant_denied(self):
        user = {"id": "u4", "is_admin": False, "can_use_vm": False}
        with _patch_db(setting=None):
            with pytest.raises(HTTPException) as exc:
                await vm_workspace_policy_module.check_vm_permission(
                    user,
                    job_needs_vm=True,
                    dependencies=preparation_composition.vm_permission_dependencies(
                        orch_main.app.state.resources
                    ),
                )
            assert exc.value.status_code == 403
            assert "not permitted" in exc.value.detail

    @pytest.mark.asyncio
    async def test_non_admin_with_grant_allowed(self):
        user = {"id": "u5", "is_admin": False, "can_use_vm": True}
        with _patch_db(setting=None):
            await vm_workspace_policy_module.check_vm_permission(
                user,
                job_needs_vm=True,
                dependencies=preparation_composition.vm_permission_dependencies(
                    orch_main.app.state.resources
                ),
            )

    @pytest.mark.asyncio
    async def test_missing_user_denied(self):
        """No user record = treated as unauthenticated non-admin."""
        with _patch_db(setting=None):
            with pytest.raises(HTTPException) as exc:
                await vm_workspace_policy_module.check_vm_permission(
                    None,
                    job_needs_vm=True,
                    dependencies=preparation_composition.vm_permission_dependencies(
                        orch_main.app.state.resources
                    ),
                )
            assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_absent_setting_is_fail_open_enabled(self):
        """No vm_workspaces row = kill-switch not engaged, proceed to per-user."""
        user = {"id": "u6", "is_admin": False, "can_use_vm": True}
        with _patch_db(setting=None):
            await vm_workspace_policy_module.check_vm_permission(
                user,
                job_needs_vm=True,
                dependencies=preparation_composition.vm_permission_dependencies(
                    orch_main.app.state.resources
                ),
            )

    @pytest.mark.asyncio
    async def test_malformed_setting_is_fail_open(self):
        """Non-dict value = falls through to per-user check, no crash."""
        admin = {"id": "u7", "is_admin": True, "can_use_vm": False}
        with _patch_db(setting={"value": "garbage"}):
            await vm_workspace_policy_module.check_vm_permission(
                admin,
                job_needs_vm=True,
                dependencies=preparation_composition.vm_permission_dependencies(
                    orch_main.app.state.resources
                ),
            )

    @pytest.mark.asyncio
    async def test_enabled_true_is_noop_path(self):
        """Explicit enabled:true should defer to per-user check."""
        user = {"id": "u8", "is_admin": False, "can_use_vm": False}
        with _patch_db(setting={"value": {"enabled": True}}):
            with pytest.raises(HTTPException) as exc:
                await vm_workspace_policy_module.check_vm_permission(
                    user,
                    job_needs_vm=True,
                    dependencies=preparation_composition.vm_permission_dependencies(
                        orch_main.app.state.resources
                    ),
                )
            assert exc.value.status_code == 403
            assert "not permitted" in exc.value.detail

    @pytest.mark.asyncio
    async def test_db_read_error_is_non_fatal(self):
        """A DB read failure on the kill-switch shouldn't hard-fail the gate."""
        user = {"id": "u9", "is_admin": False, "can_use_vm": True}
        with _patch_db(setting_error=RuntimeError("db down")):
            await vm_workspace_policy_module.check_vm_permission(
                user,
                job_needs_vm=True,
                dependencies=preparation_composition.vm_permission_dependencies(
                    orch_main.app.state.resources
                ),
            )


# ---------------------------------------------------------------------------
# _job_needs_vm — the feeder for the gate
# ---------------------------------------------------------------------------


class TestJobNeedsVm:
    def test_config_override_vm_triggers(self):
        job = {"config_override": {"workspace": {"backend": "vm"}}}
        assert job_workspace_runtime_module.job_needs_vm(job) is True

    def test_config_override_legacy_remote_triggers(self):
        job = {"config_override": {"workspace": {"backend": "remote"}}}
        assert job_workspace_runtime_module.job_needs_vm(job) is True

    def test_config_override_sandbox_does_not_trigger(self):
        job = {"config_override": {"workspace": {"backend": "sandbox"}}}
        assert job_workspace_runtime_module.job_needs_vm(job) is False

    def test_context_vm_requested_with_provenance_triggers(self):
        """A legacy VM request counts only with provisioner-written provenance."""
        job = {
            "context": {
                "vm": {
                    "requested": True,
                    "provision_generation": "6f1d1e02-4d24-4d6e-9f7e-6a0d1c2b3a45",
                }
            }
        }
        assert job_workspace_runtime_module.job_needs_vm(job) is True

    def test_bare_context_vm_request_is_ambiguous_not_a_vm_tier(self):
        """A bare context.vm.requested flag no longer assigns the VM tier.

        The contract is the sole authority for the tier; a request with no
        authoritative provenance is refused by the resolver, and the gate
        feeder must fail closed rather than guess a tier from context.
        """
        job = {"context": {"vm": {"requested": True}}}
        assert job_workspace_runtime_module.job_needs_vm(job) is False

    def test_empty_job_does_not_trigger(self):
        assert job_workspace_runtime_module.job_needs_vm({}) is False
