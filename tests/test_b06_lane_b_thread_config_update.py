"""R1.B06 lane B — live session config edits and live workspace upgrades.

These five operations are credential and provisioning boundaries, so what is
asserted here is the refusal: its status code, its error ``code`` and the fact
that it happens BEFORE the effect it guards.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.database.postgres import (
    DatasourceMaterializationAuthorizationError,
)
from orchestrator.schemas.thread_config import (
    AgentThreadConfigUpdateRequest,
    ThreadConfigPatchRequest,
    ThreadWorkspaceUpgradeRequest,
)
from orchestrator.services import thread_config_update as tcu

THREAD = "11111111-1111-4111-8111-111111111111"
GENERATION = "22222222-2222-4222-8222-222222222222"
OWNER = "33333333-3333-4333-8333-333333333333"
CHAIN_SOURCES = {"tier": "upgrade", "template": "builtin", "template_name": None}


def _pinned_thread(**over: Any) -> dict[str, Any]:
    row = {
        "id": THREAD,
        "user_id": OWNER,
        "execution_lane": "pinned",
        "status": "created",
        "metadata": {},
        "agent_id": None,
        "runtime_generation": GENERATION,
        "runtime_attach_token": None,
        "runtime_retirement_token": None,
        "config_name": "session_base",
        "srw_runtime": True,
    }
    row.update(over)
    return row


def _transaction(conn: Any):
    @contextlib.asynccontextmanager
    async def _scope(_thread_id: str):
        yield conn

    return _scope


def _lock():
    @contextlib.asynccontextmanager
    async def _scope(_thread_id: str):
        yield None

    return _scope


def _deps(**over: Any) -> tcu.ThreadConfigUpdateDependencies:
    conn = over.pop("conn", None) or SimpleNamespace(
        fetchrow=AsyncMock(return_value=_pinned_thread())
    )
    store_over = over.pop("store", {})
    store = SimpleNamespace(
        get_thread=AsyncMock(return_value=_pinned_thread()),
        thread_configuration_transaction=_transaction(conn),
        thread_advisory_lock=_lock(),
        refresh_session_execution=AsyncMock(
            return_value={"delivery_override": {"llm": {"api_key": "secret"}}}
        ),
        merge_thread_vm_context=AsyncMock(),
        get_user=AsyncMock(return_value={"id": OWNER}),
    )
    for key, value in store_over.items():
        setattr(store, key, value)
    recovery_store = MagicMock()
    recovery_store.acquire_cleanup_permit = AsyncMock(
        return_value=SimpleNamespace(allowed=True, admission_id=None)
    )
    fields: dict[str, Any] = dict(
        store=store,
        vm_provisioner=SimpleNamespace(
            is_available=True,
            lifecycle_available=True,
            mode="same-cluster",
            create_thread_vm=AsyncMock(return_value=True),
            delete_thread_vm=AsyncMock(return_value=True),
            capture_vm_teardown_identity=AsyncMock(
                return_value=SimpleNamespace(
                    provision_generation="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    vm_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                    rootdisk_pvc_uid="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                )
            ),
            release_vm_captured=AsyncMock(
                return_value=SimpleNamespace(disposition="completed")
            ),
        ),
        container_provisioner=SimpleNamespace(
            is_available=True,
            in_cluster=True,
            create_pinned_thread_workspace=AsyncMock(return_value=True),
        ),
        recovery_store=recovery_store,
        enforce_workspace_upgrade_grants=AsyncMock(),
        require_internal=AsyncMock(),
        require_thread_owner=AsyncMock(return_value=({"id": "u-1"}, _pinned_thread())),
        thread_project_ids=AsyncMock(return_value=[]),
        authorize_thread_datasource_selection=AsyncMock(return_value=([], {})),
        build_datasource_tool_override=MagicMock(return_value={}),
        datasource_selection_provenance=AsyncMock(return_value={}),
        enforce_session_create_grants=AsyncMock(),
        inject_model_credentials=AsyncMock(),
        log_security_event=AsyncMock(),
    )
    fields.update(over)
    return tcu.ThreadConfigUpdateDependencies(**fields)


@pytest.fixture(autouse=True)
def shipped_chart(monkeypatch) -> None:
    """Upgrades resolve the shipped chain: no built-ins declared, so a tier's
    template is backend-only."""
    monkeypatch.delenv("WORKSPACE_DEFAULTS", raising=False)
    monkeypatch.delenv("WORKSPACE_BUILTIN_TEMPLATES", raising=False)


@pytest.fixture(autouse=True)
def locked_commit_core(monkeypatch) -> AsyncMock:
    """The commit core is characterized in test_b12_thread_config_update_policy;
    here it is stubbed at its owner so these tests pin only the routes and the
    transaction wrapper around it."""
    core = AsyncMock(return_value=({"llm": {"model": "m"}}, ["d1"]))
    monkeypatch.setattr(tcu, "apply_thread_config_update_locked", core)
    return core


class TestChangeSummary:
    def test_only_key_paths_are_recorded_never_values(self):
        summary = tcu.config_change_summary(
            {"llm": {"api_key": "sk-live-secret", "model": "m"}, "tools": {}}, ["a"]
        )
        assert summary == "keys=llm.api_key,llm.model,tools datasource_ids=1"
        assert "sk-live-secret" not in summary

    def test_no_change_reads_empty(self):
        assert tcu.config_change_summary({}, None) == "empty"


class TestProtectedMarker:
    def test_absent_thread_is_off(self):
        assert tcu.protected_cloud_mutation_marker(None) == "off"

    def test_unparseable_metadata_string_is_a_409_not_an_ordinary_row(self):
        with pytest.raises(HTTPException) as exc:
            tcu.protected_cloud_mutation_marker({"metadata": "{not json"})
        assert exc.value.status_code == 409
        assert exc.value.detail["code"] == "protected_cloud_malformed"

    def test_malformed_marker_is_a_409(self):
        with pytest.raises(HTTPException) as exc:
            tcu.protected_cloud_mutation_marker(
                {"metadata": {"protected_cloud": "yes-please"}}
            )
        assert exc.value.status_code == 409
        assert exc.value.detail["code"] == "protected_cloud_malformed"

    def test_live_marker_is_on(self):
        assert (
            tcu.protected_cloud_mutation_marker({"metadata": {"protected_cloud": True}})
            == "on"
        )

    def test_protected_workspace_upgrade_is_refused_before_any_effect(self):
        with pytest.raises(HTTPException) as exc:
            tcu.require_unprotected_workspace_upgrade(
                {"metadata": {"protected_cloud": True}}
            )
        assert exc.value.status_code == 409
        assert exc.value.detail["code"] == "protected_cloud_workspace_fixed"

    def test_unprotected_upgrade_returns_the_parsed_metadata(self):
        assert tcu.require_unprotected_workspace_upgrade(
            {"metadata": '{"workspace_container": {"status": "ready"}}'}
        ) == {"workspace_container": {"status": "ready"}}

    def test_absent_metadata_is_an_empty_mapping(self):
        assert tcu.require_unprotected_workspace_upgrade({"metadata": None}) == {}


class TestApplyConfigUpdate:
    @pytest.mark.asyncio
    async def test_ordered_scalars_must_use_the_control_inbox(self):
        with pytest.raises(HTTPException) as exc:
            await tcu.apply_thread_config_update(
                THREAD,
                None,
                {"interactive": {"permission_mode": "autonomous"}},
                None,
                request=MagicMock(),
                actor=None,
                dependencies=_deps(),
            )
        assert exc.value.status_code == 409
        assert "ordered session control endpoint" in exc.value.detail

    @pytest.mark.asyncio
    async def test_missing_row_under_the_lock_is_404(self):
        conn = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
        with pytest.raises(HTTPException) as exc:
            await tcu.apply_thread_config_update(
                THREAD,
                None,
                {"llm": {"model": "m"}},
                None,
                request=MagicMock(),
                actor=None,
                dependencies=_deps(conn=conn),
            )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_managed_runtime_without_the_exact_generation_is_409(
        self, monkeypatch
    ):
        from orchestrator.services import manifest_execution_snapshot

        monkeypatch.setattr(
            manifest_execution_snapshot,
            "read_execution",
            AsyncMock(return_value={"generation": 7}),
        )
        with pytest.raises(HTTPException) as exc:
            await tcu.apply_thread_config_update(
                THREAD,
                None,
                {"llm": {"model": "m"}},
                None,
                request=MagicMock(),
                actor=None,
                managed_runtime=True,
                snapshot_patch_protocol=1,
                snapshot_generation=6,
                dependencies=_deps(),
            )
        assert exc.value.status_code == 409
        assert "reattach" in exc.value.detail

    @pytest.mark.asyncio
    async def test_materialization_denial_is_a_403(self, monkeypatch):
        deps = _deps()
        deps.store.refresh_session_execution = AsyncMock(
            side_effect=DatasourceMaterializationAuthorizationError()
        )
        with pytest.raises(HTTPException) as exc:
            await tcu.apply_thread_config_update(
                THREAD,
                None,
                {"llm": {"model": "m"}},
                None,
                request=MagicMock(),
                actor=None,
                dependencies=deps,
            )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_commit_returns_the_delivery_override_and_selection(self):
        override, ids = await tcu.apply_thread_config_update(
            THREAD,
            None,
            {"llm": {"model": "m"}},
            ["d1"],
            request=MagicMock(),
            actor=None,
            dependencies=_deps(),
        )
        assert override == {"llm": {"api_key": "secret"}}
        assert ids == ["d1"]


class TestUpgradeToVm:
    @pytest.mark.asyncio
    async def test_unknown_thread_is_404(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(return_value=None)
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_vm(MagicMock(), THREAD, dependencies=deps)
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_stateless_lane_is_refused_before_the_grant_gate(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(execution_lane="stateless")
        )
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_vm(MagicMock(), THREAD, dependencies=deps)
        assert exc.value.status_code == 409
        deps.enforce_workspace_upgrade_grants.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_grants_run_before_the_provisioner_is_consulted(self):
        deps = _deps()
        deps.enforce_workspace_upgrade_grants.side_effect = HTTPException(
            status_code=403, detail="vm_workspace grant denied"
        )
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_vm(MagicMock(), THREAD, dependencies=deps)
        assert exc.value.status_code == 403
        deps.vm_provisioner.create_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_absent_provisioner_is_503(self):
        deps = _deps()
        deps.vm_provisioner.is_available = False
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_vm(MagicMock(), THREAD, dependencies=deps)
        assert exc.value.status_code == 503

    @pytest.mark.asyncio
    async def test_in_flight_vm_short_circuits_idempotently(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(metadata={"vm": {"status": "provisioning"}})
        )
        result = await tcu.agent_upgrade_thread_to_vm(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result["status"] == "provisioning"
        assert result["message"] == "VM already provisioned or in progress"
        deps.vm_provisioner.create_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_malformed_vm_authority_is_409(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(metadata={"vm": ["not", "a", "mapping"]})
        )
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_vm(MagicMock(), THREAD, dependencies=deps)
        assert exc.value.status_code == 409
        assert exc.value.detail == "VM authority is malformed"

    @pytest.mark.asyncio
    async def test_lost_generation_race_is_the_409_authority_code(self):
        deps = _deps()
        deps.vm_provisioner.create_thread_vm = AsyncMock(return_value=False)
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_vm(MagicMock(), THREAD, dependencies=deps)
        assert exc.value.status_code == 409
        assert exc.value.detail == {"code": "vm_provision_authority_changed"}

    @pytest.mark.asyncio
    async def test_the_chain_template_is_recorded_before_provisioning(self):
        deps = _deps()
        await tcu.agent_upgrade_thread_to_vm(MagicMock(), THREAD, dependencies=deps)
        record = {"upgrade_config": {}, "upgrade_sources": CHAIN_SOURCES}
        deps.store.get_user.assert_awaited_once_with(OWNER)
        deps.store.merge_thread_vm_context.assert_awaited_once_with(THREAD, record)
        call = deps.vm_provisioner.create_thread_vm.await_args
        assert call.kwargs["expected_vm_context"] == record

    @pytest.mark.asyncio
    async def test_a_missing_owner_fails_before_any_write(self):
        deps = _deps(store={"get_user": AsyncMock(return_value=None)})
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_vm(MagicMock(), THREAD, dependencies=deps)
        assert (exc.value.status_code, exc.value.detail) == (
            409,
            "The execution owner is unavailable.",
        )
        deps.store.merge_thread_vm_context.assert_not_awaited()
        deps.vm_provisioner.create_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_ownerless_session_upgrades_to_a_bare_vm(self):
        # Agent child threads are created without an owner.
        deps = _deps()
        deps.store.get_thread = AsyncMock(return_value=_pinned_thread(user_id=None))
        result = await tcu.agent_upgrade_thread_to_vm(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result["status"] == "provisioning"
        record = {"upgrade_config": {}, "upgrade_sources": None}
        deps.store.get_user.assert_not_awaited()
        deps.store.merge_thread_vm_context.assert_awaited_once_with(THREAD, record)
        call = deps.vm_provisioner.create_thread_vm.await_args
        assert call.kwargs["expected_vm_context"] == record

    @pytest.mark.asyncio
    async def test_a_refused_vm_option_writes_no_record(self, monkeypatch):
        monkeypatch.setattr(
            tcu,
            "vm_provisioning_options",
            AsyncMock(side_effect=HTTPException(422, "refused option")),
        )
        deps = _deps()
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_vm(MagicMock(), THREAD, dependencies=deps)
        assert exc.value.status_code == 422
        deps.store.merge_thread_vm_context.assert_not_awaited()
        deps.vm_provisioner.create_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_retry_after_an_aborted_vm_upgrade_still_provisions(self):
        # A VM upgrade stamps the vm tier before its VM exists; abort-vm-upgrade
        # leaves it there so the user can retry.
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(
                metadata={
                    "config_override": {"workspace": {"backend": "vm"}},
                    "vm": {"status": "aborted"},
                }
            )
        )
        result = await tcu.agent_upgrade_thread_to_vm(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result["status"] == "provisioning"
        deps.vm_provisioner.create_thread_vm.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_session_already_on_a_vm_is_not_upgraded_again(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(
                metadata={
                    "config_override": {"workspace": {"backend": "vm"}},
                    "vm": {"status": "suspended"},
                }
            )
        )
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_vm(MagicMock(), THREAD, dependencies=deps)
        assert (exc.value.status_code, exc.value.detail) == (
            400,
            "An upgrade must move to a higher tier than the current one.",
        )
        deps.vm_provisioner.create_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_success_reports_the_provisioner_mode(self):
        deps = _deps()
        result = await tcu.agent_upgrade_thread_to_vm(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result == {
            "status": "provisioning",
            "thread_id": THREAD,
            "vm_provisioner_mode": "same-cluster",
        }


class TestAbortVmUpgrade:
    @pytest.mark.asyncio
    async def test_workspace_recovery_blocks_abort_vm_external_delete(self):
        recovery_store = MagicMock()
        recovery_store.acquire_cleanup_permit = AsyncMock(
            return_value=SimpleNamespace(
                allowed=False,
                reason="workspace_recovery_unresolved",
            )
        )
        deps = _deps(recovery_store=recovery_store)
        deps.vm_provisioner.capture_vm_teardown_identity = AsyncMock(
            return_value=SimpleNamespace(
                provision_generation="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                vm_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                rootdisk_pvc_uid="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            )
        )
        deps.vm_provisioner.release_vm_captured = AsyncMock()

        with pytest.raises(HTTPException) as exc:
            await tcu.agent_abort_thread_vm_upgrade(
                MagicMock(), THREAD, dependencies=deps
            )

        assert exc.value.status_code == 503
        recovery_store.acquire_cleanup_permit.assert_awaited_once()
        deps.vm_provisioner.release_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unproven_teardown_is_503_and_never_stamps_aborted(self):
        deps = _deps()
        deps.vm_provisioner.release_vm_captured = AsyncMock(
            return_value=SimpleNamespace(disposition="process_zero_unproven")
        )
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_abort_thread_vm_upgrade(
                MagicMock(), THREAD, dependencies=deps
            )
        assert exc.value.status_code == 503
        assert exc.value.detail == {
            "code": "vm_process_zero_unproven",
            "retryable": True,
        }
        deps.store.merge_thread_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_raising_delete_is_also_unproven(self):
        deps = _deps()
        deps.vm_provisioner.release_vm_captured = AsyncMock(
            side_effect=RuntimeError("x")
        )
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_abort_thread_vm_upgrade(
                MagicMock(), THREAD, dependencies=deps
            )
        assert exc.value.status_code == 503

    @pytest.mark.asyncio
    async def test_proven_teardown_stamps_aborted(self):
        deps = _deps()
        result = await tcu.agent_abort_thread_vm_upgrade(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result == {
            "status": "aborted",
            "thread_id": THREAD,
            "vm_deleted": True,
        }
        deps.store.merge_thread_vm_context.assert_awaited_once_with(
            THREAD, {"status": "aborted"}
        )


class TestUpgradeToWorkspace:
    @pytest.mark.asyncio
    async def test_unknown_tier_is_400(self):
        deps = _deps()
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_workspace(
                MagicMock(),
                THREAD,
                ThreadWorkspaceUpgradeRequest(target_tier="desktop"),
                dependencies=deps,
            )
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_vm_target_delegates_to_the_operator_gated_path(self):
        deps = _deps()
        result = await tcu.agent_upgrade_thread_to_workspace(
            MagicMock(),
            THREAD,
            ThreadWorkspaceUpgradeRequest(target_tier="vm"),
            dependencies=deps,
        )
        assert result["vm_provisioner_mode"] == "same-cluster"
        assert result["target_tier"] == "vm"
        deps.enforce_workspace_upgrade_grants.assert_awaited_once()
        assert deps.enforce_workspace_upgrade_grants.await_args.kwargs == {
            "target_tier": "vm"
        }

    @pytest.mark.asyncio
    async def test_protected_session_cannot_replace_its_container(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(metadata={"protected_cloud": True})
        )
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_workspace(
                MagicMock(), THREAD, None, dependencies=deps
            )
        assert exc.value.status_code == 409
        assert exc.value.detail["code"] == "protected_cloud_workspace_fixed"

    @pytest.mark.asyncio
    async def test_no_in_cluster_provisioner_is_503(self):
        deps = _deps()
        deps.container_provisioner.in_cluster = False
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_upgrade_thread_to_workspace(
                MagicMock(), THREAD, None, dependencies=deps
            )
        assert exc.value.status_code == 503

    @pytest.mark.asyncio
    async def test_in_flight_container_is_idempotent(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(
                metadata={"workspace_container": {"status": "creating"}}
            )
        )
        result = await tcu.agent_upgrade_thread_to_workspace(
            MagicMock(), THREAD, None, dependencies=deps
        )
        assert result["status"] == "creating"
        assert result["target_tier"] == "sandbox"

    @pytest.mark.asyncio
    async def test_sandbox_upgrade_schedules_the_lifecycle_owner(self):
        import asyncio

        deps = _deps()
        result = await tcu.agent_upgrade_thread_to_workspace(
            MagicMock(), THREAD, None, dependencies=deps
        )
        assert result == {
            "status": "provisioning",
            "thread_id": THREAD,
            "target_tier": "sandbox",
        }
        await asyncio.sleep(0)
        deps.container_provisioner.create_pinned_thread_workspace.assert_awaited_once_with(
            THREAD
        )

    @pytest.mark.asyncio
    async def test_no_body_moves_a_container_session_to_a_vm(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(
                metadata={"config_override": {"workspace": {"backend": "sandbox"}}}
            )
        )
        result = await tcu.agent_upgrade_thread_to_workspace(
            MagicMock(), THREAD, None, dependencies=deps
        )
        assert (result["target_tier"], result["vm_provisioner_mode"]) == (
            "vm",
            "same-cluster",
        )
        assert deps.enforce_workspace_upgrade_grants.await_args.kwargs == {
            "target_tier": "vm"
        }
        deps.container_provisioner.create_pinned_thread_workspace.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("grant_refusal", "vms_available"),
        [(None, True), (HTTPException(403, "vm grant denied"), True), (None, False)],
        ids=["vm-rights", "no-vm-rights", "vms-off"],
    )
    async def test_a_container_template_upgrade_is_refused_for_sessions(
        self, monkeypatch, grant_refusal, vms_available
    ):
        monkeypatch.setattr(
            "orchestrator.services.workspace_defaults_resolution.find_readable_template",
            AsyncMock(return_value=({"ref": {"name": "site"}}, "container")),
        )
        deps = _deps()
        deps.enforce_workspace_upgrade_grants.side_effect = grant_refusal
        deps.vm_provisioner.is_available = vms_available
        with pytest.raises(HTTPException) as refused:
            await tcu.agent_upgrade_thread_to_workspace(
                MagicMock(),
                THREAD,
                ThreadWorkspaceUpgradeRequest(template="site"),
                dependencies=deps,
            )
        assert refused.value.status_code == 409
        assert refused.value.detail == (
            "Container upgrades of a running Session are unavailable; "
            "start a new Session with this template."
        )
        deps.container_provisioner.create_pinned_thread_workspace.assert_not_called()
        deps.store.merge_thread_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_session_on_a_vm_refuses_a_vm_template(self, monkeypatch):
        monkeypatch.setattr(
            "orchestrator.services.workspace_defaults_resolution.find_readable_template",
            AsyncMock(return_value=({"ref": {"name": "vm-big"}}, "vm")),
        )
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(
                metadata={
                    "config_override": {"workspace": {"backend": "vm"}},
                    "vm": {"status": "ready"},
                }
            )
        )
        with pytest.raises(HTTPException) as refused:
            await tcu.agent_upgrade_thread_to_workspace(
                MagicMock(),
                THREAD,
                ThreadWorkspaceUpgradeRequest(template="vm-big"),
                dependencies=deps,
            )
        assert (refused.value.status_code, refused.value.detail) == (
            400,
            "An upgrade must move to a higher tier than the current one.",
        )
        deps.store.merge_thread_vm_context.assert_not_awaited()
        deps.vm_provisioner.create_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_named_template_on_ownerless_work_is_refused(self, monkeypatch):
        find = AsyncMock()
        monkeypatch.setattr(
            "orchestrator.services.workspace_defaults_resolution.find_readable_template",
            find,
        )
        deps = _deps()
        deps.store.get_thread = AsyncMock(return_value=_pinned_thread(user_id=None))
        with pytest.raises(HTTPException) as refused:
            await tcu.agent_upgrade_thread_to_workspace(
                MagicMock(),
                THREAD,
                ThreadWorkspaceUpgradeRequest(template="vm-big"),
                dependencies=deps,
            )
        assert (refused.value.status_code, refused.value.detail) == (
            409,
            "The execution owner is unavailable.",
        )
        find.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("thread", "grant_refusal", "vms_available", "status"),
        [
            (_pinned_thread(execution_lane="stateless"), None, True, 409),
            (_pinned_thread(metadata={"protected_cloud": True}), None, True, 409),
            (_pinned_thread(), HTTPException(403, "vm grant denied"), True, 403),
            (_pinned_thread(), None, False, 503),
        ],
        ids=["stateless-lane", "protected-session", "vm-grant-denied", "vms-off"],
    )
    async def test_a_template_upgrade_runs_the_guards_before_resolving(
        self, monkeypatch, thread, grant_refusal, vms_available, status
    ):
        find = AsyncMock(return_value=({"ref": {"name": "vm-big"}}, "vm"))
        render = AsyncMock(return_value=("vm", {"backend": "vm"}, {}))
        monkeypatch.setattr(
            "orchestrator.services.workspace_defaults_resolution.find_readable_template",
            find,
        )
        monkeypatch.setattr(
            "orchestrator.services.workspace_defaults_resolution.render_upgrade_workspace",
            render,
        )
        deps = _deps()
        deps.store.get_thread = AsyncMock(return_value=thread)
        deps.enforce_workspace_upgrade_grants.side_effect = grant_refusal
        deps.vm_provisioner.is_available = vms_available
        with pytest.raises(HTTPException) as refused:
            await tcu.agent_upgrade_thread_to_workspace(
                MagicMock(),
                THREAD,
                ThreadWorkspaceUpgradeRequest(template="vm-big"),
                dependencies=deps,
            )
        assert refused.value.status_code == status
        # Only the read-only lookup may precede the VM guards; nothing is
        # rendered or written before every guard has passed.
        if status == 409:
            find.assert_not_awaited()
            deps.store.get_user.assert_not_awaited()
        render.assert_not_awaited()
        deps.store.merge_thread_vm_context.assert_not_awaited()
        deps.vm_provisioner.create_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_vm_template_upgrade_provisions_that_template(self, monkeypatch):
        sources = {
            "tier": "explicit",
            "template": "explicit",
            "template_name": "vm-big",
        }
        vm = {"image": "r.example/vm:2", "cpu_cores": 4}
        render = AsyncMock(return_value=("vm", {"backend": "vm", "vm": vm}, sources))
        monkeypatch.setattr(
            "orchestrator.services.workspace_defaults_resolution.find_readable_template",
            AsyncMock(return_value=({"ref": {"name": "vm-big"}}, "vm")),
        )
        monkeypatch.setattr(
            "orchestrator.services.workspace_defaults_resolution.render_upgrade_workspace",
            render,
        )
        deps = _deps()
        result = await tcu.agent_upgrade_thread_to_workspace(
            MagicMock(),
            THREAD,
            ThreadWorkspaceUpgradeRequest(template="vm-big"),
            dependencies=deps,
        )
        assert result["vm_provisioner_mode"] == "same-cluster"
        assert [call.kwargs["template_name"] for call in render.await_args_list] == [
            "vm-big"
        ]
        assert render.await_args.args[1] == {"id": OWNER}
        assert render.await_args.kwargs["current_backend"] == "virtual"
        record = {"upgrade_config": vm, "upgrade_sources": sources}
        deps.store.merge_thread_vm_context.assert_awaited_once_with(THREAD, record)
        call = deps.vm_provisioner.create_thread_vm.await_args
        assert (call.kwargs["vm_image"], call.kwargs["cpu_cores"]) == (
            "r.example/vm:2",
            4,
        )
        assert call.kwargs["expected_vm_context"] == record


class TestOwnerFacingPatch:
    @pytest.mark.asyncio
    async def test_a_connected_pinned_session_is_refused(self):
        deps = _deps(
            require_thread_owner=AsyncMock(
                return_value=({"id": "u-1"}, _pinned_thread(agent_id="a-1"))
            )
        )
        with pytest.raises(HTTPException) as exc:
            await tcu.update_thread_config(
                THREAD,
                ThreadConfigPatchRequest(config_override={"llm": {"model": "m"}}),
                MagicMock(),
                dependencies=deps,
            )
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_an_ended_thread_with_a_stale_agent_id_stays_editable(self):
        deps = _deps(
            require_thread_owner=AsyncMock(
                return_value=(
                    {"id": "u-1"},
                    _pinned_thread(agent_id="a-1", status="ended"),
                )
            )
        )
        result = await tcu.update_thread_config(
            THREAD,
            ThreadConfigPatchRequest(config_override={"llm": {"model": "m"}}),
            MagicMock(),
            dependencies=deps,
        )
        assert result["status"] == "updated"

    @pytest.mark.asyncio
    async def test_an_empty_patch_is_400(self):
        with pytest.raises(HTTPException) as exc:
            await tcu.update_thread_config(
                THREAD, ThreadConfigPatchRequest(), MagicMock(), dependencies=_deps()
            )
        assert exc.value.status_code == 400
        assert exc.value.detail == "No changes provided"

    @pytest.mark.asyncio
    async def test_the_browser_facing_body_is_redacted(self):
        deps = _deps()
        deps.store.refresh_session_execution = AsyncMock(
            return_value={
                "delivery_override": {"llm": {"model": "m", "api_key": "sk-live"}}
            }
        )
        result = await tcu.update_thread_config(
            THREAD,
            ThreadConfigPatchRequest(config_override={"llm": {"model": "m"}}),
            MagicMock(),
            dependencies=deps,
        )
        assert "sk-live" not in str(result["config_override"])
        assert result["effective"] == "next_attach"

    @pytest.mark.asyncio
    async def test_a_stateless_session_takes_effect_on_the_next_turn(self):
        deps = _deps(
            require_thread_owner=AsyncMock(
                return_value=({"id": "u-1"}, _pinned_thread(execution_lane="stateless"))
            )
        )
        result = await tcu.update_thread_config(
            THREAD,
            ThreadConfigPatchRequest(datasource_ids=[]),
            MagicMock(),
            dependencies=deps,
        )
        assert result["effective"] == "next_turn"


class TestInternalPatch:
    @pytest.mark.asyncio
    async def test_the_internal_body_keeps_its_plaintext_transport(self, monkeypatch):
        from orchestrator.services import manifest_execution_snapshot

        monkeypatch.setattr(
            manifest_execution_snapshot, "read_execution", AsyncMock(return_value=None)
        )
        deps = _deps()
        result = await tcu.agent_update_thread_config(
            MagicMock(),
            THREAD,
            AgentThreadConfigUpdateRequest(config_override={"llm": {"model": "m"}}),
            dependencies=deps,
        )
        assert result["config_override"] == {"llm": {"api_key": "secret"}}
        deps.require_internal.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_an_unexpected_failure_becomes_a_500_not_a_leak(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(side_effect=RuntimeError("boom"))
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_update_thread_config(
                MagicMock(),
                THREAD,
                AgentThreadConfigUpdateRequest(config_override={}),
                dependencies=deps,
            )
        assert exc.value.status_code == 500


class TestUpgradeAvailability:
    @pytest.mark.asyncio
    async def test_unknown_thread_is_404(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(return_value=None)
        with pytest.raises(HTTPException) as exc:
            await tcu.agent_thread_upgrade_availability(
                MagicMock(), THREAD, dependencies=deps
            )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_a_pinned_virtual_session_can_upgrade_to_a_vm(self):
        deps = _deps()
        result = await tcu.agent_thread_upgrade_availability(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result == {"vm": {"available": True, "reason": None}}
        deps.require_internal.assert_awaited_once()
        deps.store.merge_thread_vm_context.assert_not_awaited()
        deps.vm_provisioner.create_thread_vm.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stateless_lane_is_unavailable_before_the_grant_gate(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(execution_lane="stateless")
        )
        result = await tcu.agent_thread_upgrade_availability(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result == {
            "vm": {
                "available": False,
                "reason": "Workspace upgrades are not yet supported on the stateless lane",
            }
        }
        deps.enforce_workspace_upgrade_grants.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_protected_session_is_unavailable_with_its_refusal_text(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(metadata={"protected_cloud": True})
        )
        result = await tcu.agent_thread_upgrade_availability(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result["vm"]["available"] is False
        assert result["vm"]["reason"] == (
            "Protected cloud sessions cannot upgrade or replace their "
            "Container workspace."
        )

    @pytest.mark.asyncio
    async def test_a_refused_grant_is_unavailable_with_the_grant_text(self):
        deps = _deps()
        deps.enforce_workspace_upgrade_grants.side_effect = HTTPException(
            status_code=403, detail="vm_workspace grant denied"
        )
        result = await tcu.agent_thread_upgrade_availability(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result == {
            "vm": {"available": False, "reason": "vm_workspace grant denied"}
        }

    @pytest.mark.asyncio
    async def test_an_absent_provisioner_is_unavailable(self):
        deps = _deps()
        deps.vm_provisioner.is_available = False
        result = await tcu.agent_thread_upgrade_availability(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result == {
            "vm": {
                "available": False,
                "reason": "VM workspaces aren't available on this installation.",
            }
        }

    @pytest.mark.asyncio
    async def test_in_flight_vm_is_unavailable(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(metadata={"vm": {"status": "waiting_capacity"}})
        )
        result = await tcu.agent_thread_upgrade_availability(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result == {
            "vm": {"available": False, "reason": "A VM upgrade is already in progress."}
        }

    @pytest.mark.asyncio
    async def test_vm_session_is_unavailable_by_the_tier_rule(self):
        deps = _deps()
        deps.store.get_thread = AsyncMock(
            return_value=_pinned_thread(
                metadata={"config_override": {"workspace": {"backend": "vm"}}}
            )
        )
        result = await tcu.agent_thread_upgrade_availability(
            MagicMock(), THREAD, dependencies=deps
        )
        assert result["vm"]["available"] is False
        assert result["vm"]["reason"]  # the tier rule's own refusal text
