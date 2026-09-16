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


def _pinned_thread(**over: Any) -> dict[str, Any]:
    row = {
        "id": THREAD,
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
        apply_thread_config_update_locked=AsyncMock(
            return_value=({"llm": {"model": "m"}}, ["d1"])
        ),
        enforce_workspace_upgrade_grants=AsyncMock(),
        require_internal=AsyncMock(),
        require_thread_owner=AsyncMock(return_value=({"id": "u-1"}, _pinned_thread())),
    )
    fields.update(over)
    return tcu.ThreadConfigUpdateDependencies(**fields)


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
