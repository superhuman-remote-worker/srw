"""R1.B06 lane B — the session create funnel and the session list.

``create_thread`` is the only path that raises a session, so its refusals are
the contract this file characterizes: every branch that returns something other
than a created thread, in the order it is reached, plus the two ordering
properties an extraction could plausibly have broken (scope is authorized
before any account work; Gitea/cloud setup is awaited before an agent is
assigned).
"""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.database.postgres import (
    DatasourceMaterializationAuthorizationError,
    DatasourcePolicyConflictError,
)
from orchestrator.schemas.thread_admission import (
    ThreadCreateRequest,
    TrustedThreadSeed,
)
from orchestrator.services import thread_admission as ta

THREAD = "11111111-1111-4111-8111-111111111111"
GENERATION = "22222222-2222-4222-8222-222222222222"
PROJECT = "33333333-3333-4333-8333-333333333333"
OTHER_PROJECT = "44444444-4444-4444-8444-444444444444"
USER = {"id": "55555555-5555-4555-8555-555555555555", "is_admin": False}


def _created_thread(**over: Any) -> dict[str, Any]:
    row = {
        "id": THREAD,
        "runtime_generation": GENERATION,
        "runtime_retirement_token": None,
        "status": "created",
        "metadata": {},
        "agent_id": None,
        "runtime_attach_token": None,
    }
    row.update(over)
    return row


def _lock():
    @contextlib.asynccontextmanager
    async def _scope(_thread_id: str):
        yield None

    return _scope


def _unavailable_provisioner() -> SimpleNamespace:
    return SimpleNamespace(is_available=False, in_cluster=False)


def _deps(**over: Any) -> ta.ThreadAdmissionDependencies:
    store_over = over.pop("store", {})
    store = SimpleNamespace(
        fetchrow=AsyncMock(return_value=None),
        get_user_settings=AsyncMock(return_value={}),
        get_thread=AsyncMock(return_value=_created_thread()),
        create_thread=AsyncMock(return_value=THREAD),
        get_officer_thread_for_project=AsyncMock(return_value=None),
        register_project_officer_thread=AsyncMock(return_value={}),
        decommission_project_officer=AsyncMock(),
        replace_thread_mounts=AsyncMock(),
        list_thread_mounts=AsyncMock(return_value=[]),
        list_thread_mounts_bulk=AsyncMock(return_value={}),
        list_threads=AsyncMock(return_value=[]),
        merge_thread_workspace_context=AsyncMock(),
        bind_thread_managed_repository=AsyncMock(return_value=True),
        update_thread_main_cloud=AsyncMock(),
        thread_advisory_lock=_lock(),
    )
    for key, value in store_over.items():
        setattr(store, key, value)

    fields: dict[str, Any] = dict(
        store=store,
        gitea_client=SimpleNamespace(is_initialized=False, is_configured=False),
        main_cloud_router=SimpleNamespace(
            active_instance_id=None, for_owner=MagicMock()
        ),
        agent_provisioner=_unavailable_provisioner(),
        container_provisioner=_unavailable_provisioner(),
        docker_provisioner=_unavailable_provisioner(),
        persistent_provisioner=_unavailable_provisioner(),
        vm_provisioner=SimpleNamespace(is_available=True),
        enforce_readiness_gate=AsyncMock(),
        require_approved_user=AsyncMock(return_value=USER),
        is_experts_db_enabled=MagicMock(return_value=False),
        user_experts_enabled=AsyncMock(return_value=False),
        datasource_defaults_on_omission=MagicMock(return_value=False),
        is_protected_cloud_mode_enabled=MagicMock(return_value=True),
        authorize_thread_project_ids=AsyncMock(side_effect=lambda _u, ids: list(ids)),
        authorize_thread_datasource_selection=AsyncMock(
            side_effect=lambda _u, ids, **_kw: (list(ids), {})
        ),
        resolve_session_account_defaults=AsyncMock(
            return_value={"workspace": {"backend": "sandbox"}}
        ),
        prefetch_roster_refs=AsyncMock(return_value=None),
        resolve_thread_execution_lane=MagicMock(return_value="pinned"),
        build_thread_mount_rows=AsyncMock(return_value=[]),
        should_skip_session_folder=MagicMock(return_value=False),
        enforce_session_create_grants=AsyncMock(),
        check_vm_permission=AsyncMock(),
        resolve_cloud_session_url=MagicMock(return_value=None),
        validated_post_owned_officer_create_fragment=MagicMock(return_value=None),
        enforce_officer_auto_pull_release=MagicMock(),
        can_manage_project_officer=AsyncMock(return_value=True),
        find_open_conference_thread=AsyncMock(return_value=None),
        inherit_conference_brain=MagicMock(return_value=[]),
        hold_officer_for_conference=AsyncMock(),
        provision_commissioned_officer=AsyncMock(),
        end_thread_flow=AsyncMock(return_value={}),
        schedule_stateless_workspace_ensure=MagicMock(),
        schedule_protected_engage=MagicMock(),
        record_protected_error=AsyncMock(),
        find_idle_persistent_agent=AsyncMock(return_value=None),
        send_session_attach=AsyncMock(return_value=True),
        provision_or_assign=AsyncMock(),
        redact_thread_metadata=MagicMock(side_effect=lambda t: t),
    )
    fields.update(over)
    return ta.ThreadAdmissionDependencies(**fields)


async def _plan(body: ThreadCreateRequest, deps: ta.ThreadAdmissionDependencies):
    return await ta.resolve_thread_creation_plan(body, USER, dependencies=deps)


# =============================================================================
# The pure override rebuild
# =============================================================================


class TestConfigOverrideRebuild:
    def test_top_level_fields_land_in_the_llm_block(self):
        override, ignored = ta.build_session_config_override(
            ThreadCreateRequest(model="m", temperature=0.3, reasoning_level="high"),
            user_id=USER["id"],
            dependencies=_deps(),
        )
        assert override["llm"] == {
            "model": "m",
            "temperature": 0.3,
            "reasoning_level": "high",
        }
        assert ignored == []

    def test_a_bad_reasoning_level_is_a_400_not_a_silent_drop(self):
        with pytest.raises(HTTPException) as exc:
            ta.build_session_config_override(
                ThreadCreateRequest(reasoning_level="ludicrous"),
                user_id=USER["id"],
                dependencies=_deps(),
            )
        assert exc.value.status_code == 400

    def test_nested_llm_keys_are_bridged_and_top_level_still_wins(self):
        override, _ = ta.build_session_config_override(
            ThreadCreateRequest(
                model="top",
                config_override={"llm": {"model": "nested", "reasoning_level": "max"}},
            ),
            user_id=USER["id"],
            dependencies=_deps(),
        )
        assert override["llm"]["model"] == "top"
        assert override["llm"]["reasoning_level"] == "max"

    def test_the_delegation_gate_is_carried_with_the_tool_names(self):
        override, _ = ta.build_session_config_override(
            ThreadCreateRequest(
                config_override={
                    "delegation": {"enabled": True},
                    "tools": {"delegation": ["delegate_agent"]},
                }
            ),
            user_id=USER["id"],
            dependencies=_deps(),
        )
        assert override["delegation"]["enabled"] is True
        assert override["tools"]["delegation"] == ["delegate_agent"]

    def test_permission_mode_reaches_the_agent_through_the_override(self):
        override, _ = ta.build_session_config_override(
            ThreadCreateRequest(permission_mode="autonomous"),
            user_id=USER["id"],
            dependencies=_deps(),
        )
        assert override["interactive"]["permission_mode"] == "autonomous"

    def test_the_workspace_tier_is_honored_at_create(self):
        override, _ = ta.build_session_config_override(
            ThreadCreateRequest(config_override={"workspace": {"backend": "virtual"}}),
            user_id=USER["id"],
            dependencies=_deps(),
        )
        assert override["workspace"]["backend"] == "virtual"

    @pytest.mark.parametrize("key", ["container", "sandbox"])
    def test_workspace_container_or_sandbox_is_refused_at_create(self, key):
        """Container image/resources come only from the selected
        WorkspaceTemplate; a caller-authored ``workspace.container`` (the old
        side door) or ``workspace.sandbox`` (the template's rendered form) is
        refused here, at the New Session create boundary."""
        with pytest.raises(HTTPException) as exc:
            ta.build_session_config_override(
                ThreadCreateRequest(
                    config_override={
                        "workspace": {key: {"image": "registry.example/x:1"}}
                    }
                ),
                user_id=USER["id"],
                dependencies=_deps(),
            )
        assert exc.value.status_code == 422
        assert (
            "are no longer supported. Put the image and resources in a "
            "WorkspaceTemplate" in exc.value.detail
        )

    def test_an_unknown_nested_key_is_reported_rather_than_dropped_silently(self):
        override, ignored = ta.build_session_config_override(
            ThreadCreateRequest(config_override={"memory": {"enabled": False}}),
            user_id=USER["id"],
            dependencies=_deps(),
        )
        assert "memory" not in override
        assert ignored == ["memory.enabled"]

    def test_the_trusted_post_fragment_is_never_reported_as_an_ignored_key(self):
        body = ThreadCreateRequest(config_override={"officer": {"enabled": True}})
        object.__setattr__(body, "_officer_post_config_snapshot", {"auto_pull": True})
        deps = _deps(
            validated_post_owned_officer_create_fragment=MagicMock(
                return_value={"auto_pull": True}
            )
        )
        override, ignored = ta.build_session_config_override(
            body, user_id=USER["id"], dependencies=deps
        )
        assert override["officer"]["auto_pull"] is True
        assert ignored == []


# =============================================================================
# Connector selection origin
# =============================================================================


class TestDatasourceSelection:
    @pytest.mark.asyncio
    async def test_an_explicit_empty_array_is_authoritative(self):
        deps = _deps()
        ids, revisions, provenance = await ta.select_thread_datasources(
            ThreadCreateRequest(datasource_ids=[]),
            USER,
            thread_backend="sandbox",
            effective_project_ids=[],
            dependencies=deps,
        )
        assert ids == [] and revisions == {}
        assert provenance["origin"] == "explicit"
        deps.authorize_thread_datasource_selection.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_omission_without_the_gate_selects_nothing(self):
        deps = _deps()
        ids, _revisions, provenance = await ta.select_thread_datasources(
            ThreadCreateRequest(),
            USER,
            thread_backend="sandbox",
            effective_project_ids=[],
            dependencies=deps,
        )
        assert ids == []
        assert provenance["origin"] == "omitted_compat"
        deps.authorize_thread_datasource_selection.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_defaults_are_resolved_when_the_caller_asks(self, monkeypatch):
        from orchestrator.services import datasource_policy

        monkeypatch.setattr(
            datasource_policy,
            "default_datasource_selection",
            AsyncMock(return_value=(["d1"], {"d1": 2})),
        )
        ids, revisions, provenance = await ta.select_thread_datasources(
            ThreadCreateRequest(use_datasource_defaults=True),
            USER,
            thread_backend="sandbox",
            effective_project_ids=[PROJECT],
            dependencies=_deps(),
        )
        assert ids == ["d1"] and revisions == {"d1": 2}
        assert provenance["origin"] == "default"
        assert provenance["creation_path"] == "persistent_thread_rest"

    @pytest.mark.asyncio
    async def test_an_unavailable_default_is_the_generic_403(self, monkeypatch):
        from orchestrator.services import datasource_policy
        from orchestrator.services.datasource_policy import DatasourceUnavailableError

        async def _boom(*_a, **_kw):
            raise DatasourceUnavailableError()

        monkeypatch.setattr(datasource_policy, "default_datasource_selection", _boom)
        with pytest.raises(HTTPException) as exc:
            await ta.select_thread_datasources(
                ThreadCreateRequest(use_datasource_defaults=True),
                USER,
                thread_backend="sandbox",
                effective_project_ids=[],
                dependencies=_deps(),
            )
        assert exc.value.status_code == 403
        assert exc.value.detail == "One or more selected connectors are unavailable"


# =============================================================================
# Admission decisions and refusals
# =============================================================================


class TestCreationPlan:
    @pytest.mark.asyncio
    async def test_scope_is_authorized_before_any_account_work(self):
        deps = _deps(
            authorize_thread_project_ids=AsyncMock(
                side_effect=HTTPException(status_code=403, detail="nope")
            )
        )
        with pytest.raises(HTTPException) as exc:
            await _plan(ThreadCreateRequest(project_ids=[PROJECT]), deps)
        assert exc.value.status_code == 403
        deps.store.get_user_settings.assert_not_awaited()
        deps.resolve_session_account_defaults.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_expert_id_cannot_be_combined_with_a_bundled_config_name(self):
        with pytest.raises(HTTPException) as exc:
            await _plan(
                ThreadCreateRequest(expert_id="e-1", config_name="scholar"), _deps()
            )
        assert exc.value.status_code == 400
        assert "select one expert source" in exc.value.detail

    @pytest.mark.asyncio
    async def test_expert_selection_error_is_422(self, monkeypatch):
        from orchestrator.services.default_experts import ExpertSelectionError

        monkeypatch.setattr(
            ta, "resolve_root_expert", AsyncMock(side_effect=ExpertSelectionError("x"))
        )
        deps = _deps(
            is_experts_db_enabled=MagicMock(return_value=True),
            user_experts_enabled=AsyncMock(return_value=True),
        )
        with pytest.raises(HTTPException) as exc:
            await _plan(ThreadCreateRequest(), deps)
        assert exc.value.status_code == 422

    @pytest.mark.asyncio
    async def test_no_default_expert_is_503(self, monkeypatch):
        from orchestrator.services.default_experts import DefaultExpertUnavailable

        monkeypatch.setattr(
            ta,
            "resolve_root_expert",
            AsyncMock(side_effect=DefaultExpertUnavailable("none seeded")),
        )
        deps = _deps(
            is_experts_db_enabled=MagicMock(return_value=True),
            user_experts_enabled=AsyncMock(return_value=True),
        )
        with pytest.raises(HTTPException) as exc:
            await _plan(ThreadCreateRequest(), deps)
        assert exc.value.status_code == 503

    @pytest.mark.asyncio
    async def test_a_plain_session_resolves_to_sandbox_on_the_pinned_lane(self):
        plan = await _plan(ThreadCreateRequest(title="t"), _deps())
        assert plan.thread_backend == "sandbox"
        assert plan.execution_lane == "pinned"
        assert plan.lite_session is False and plan.vm_session is False
        assert plan.create_kwargs["config_name"] == "session_base"
        assert plan.create_kwargs["permission_mode"] == "supervised"

    @pytest.mark.asyncio
    async def test_protected_cloud_requires_the_container_tier(self):
        body = ThreadCreateRequest(
            protected_cloud=True, config_override={"workspace": {"backend": "virtual"}}
        )
        with pytest.raises(HTTPException) as exc:
            await _plan(body, _deps())
        assert exc.value.status_code == 422
        assert exc.value.detail["code"] == "protected_cloud_unsupported_workspace"

    @pytest.mark.asyncio
    async def test_protected_cloud_is_refused_for_the_officer_runtime(self):
        body = ThreadCreateRequest(
            protected_cloud=True, config_override={"officer": {"enabled": True}}
        )
        with pytest.raises(HTTPException) as exc:
            await _plan(body, _deps())
        assert exc.value.status_code == 422
        assert exc.value.detail["code"] == "protected_cloud_unsupported_session_class"

    @pytest.mark.asyncio
    async def test_protected_cloud_stays_pinned_even_when_stateless_is_admitted(self):
        deps = _deps(resolve_thread_execution_lane=MagicMock(return_value="stateless"))
        plan = await _plan(ThreadCreateRequest(protected_cloud=True), deps)
        assert plan.execution_lane == "pinned"
        assert plan.create_kwargs["initial_metadata"]["protected_cloud"] is True

    @pytest.mark.asyncio
    async def test_a_hand_rolled_post_owned_key_is_refused_at_the_request_boundary(
        self,
    ):
        body = ThreadCreateRequest(
            config_override={"officer": {"enabled": True, "auto_pull": True}}
        )
        with pytest.raises(HTTPException) as exc:
            await _plan(body, _deps())
        assert exc.value.status_code == 400
        assert "auto_pull" in exc.value.detail

    @pytest.mark.asyncio
    async def test_an_inherited_post_owned_key_is_refused_on_the_resolved_config(
        self, monkeypatch
    ):
        """An expert/account/project default must not bypass the Post either."""

        def _fake_resolve(*, capture, **_kw):
            capture["merged_fragment"] = {
                "officer": {"enabled": True, "auto_pull": True},
                "workspace": {"backend": "sandbox"},
            }

        monkeypatch.setattr(ta, "resolve_config", _fake_resolve)
        with pytest.raises(HTTPException) as exc:
            await _plan(ThreadCreateRequest(), _deps())
        assert exc.value.status_code == 400
        assert "durable Officer" in exc.value.detail

    @pytest.mark.asyncio
    async def test_an_inherited_auto_pull_is_release_fenced(self, monkeypatch):
        def _fake_resolve(*, capture, **_kw):
            capture["merged_fragment"] = {
                "officer": {"enabled": True, "auto_pull": True},
                "workspace": {"backend": "sandbox"},
            }

        monkeypatch.setattr(ta, "resolve_config", _fake_resolve)
        deps = _deps(
            enforce_officer_auto_pull_release=MagicMock(
                side_effect=HTTPException(status_code=409, detail="not released")
            )
        )
        with pytest.raises(HTTPException) as exc:
            await _plan(ThreadCreateRequest(), deps)
        assert exc.value.status_code == 409
        deps.enforce_officer_auto_pull_release.assert_called_once_with(True)

    @pytest.mark.asyncio
    async def test_auto_pull_is_materialized_off_for_an_ordinary_officer(self):
        body = ThreadCreateRequest(config_override={"officer": {"enabled": True}})
        plan = await _plan(body, _deps())
        assert plan.config_override["officer"]["auto_pull"] is False

    @pytest.mark.asyncio
    async def test_a_conference_needs_exactly_one_project(self):
        body = ThreadCreateRequest(config_override={"officer": {"conference": True}})
        with pytest.raises(HTTPException) as exc:
            await _plan(body, _deps())
        assert exc.value.status_code == 400
        assert "project-scoped" in exc.value.detail

    @pytest.mark.asyncio
    async def test_a_conference_re_checks_the_owner_role_at_the_server(self):
        body = ThreadCreateRequest(
            project_id=PROJECT, config_override={"officer": {"conference": True}}
        )
        deps = _deps(can_manage_project_officer=AsyncMock(return_value=False))
        with pytest.raises(HTTPException) as exc:
            await _plan(body, deps)
        assert exc.value.status_code == 403
        assert "conference" in exc.value.detail

    @pytest.mark.asyncio
    async def test_a_second_open_conference_is_409(self):
        body = ThreadCreateRequest(
            project_id=PROJECT, config_override={"officer": {"conference": True}}
        )
        deps = _deps(
            find_open_conference_thread=AsyncMock(return_value={"id": "other"})
        )
        with pytest.raises(HTTPException) as exc:
            await _plan(body, deps)
        assert exc.value.status_code == 409
        assert "conference_open" in exc.value.detail

    @pytest.mark.asyncio
    async def test_a_conference_inherits_the_standing_officers_brain(self):
        body = ThreadCreateRequest(
            project_id=PROJECT, config_override={"officer": {"conference": True}}
        )
        deps = _deps(inherit_conference_brain=MagicMock(return_value=["model"]))
        plan = await _plan(body, deps)
        deps.inherit_conference_brain.assert_called_once()
        assert plan.config_override["officer"]["conference"] is True

    @pytest.mark.asyncio
    async def test_an_officer_create_needs_the_owner_role(self):
        body = ThreadCreateRequest(
            project_id=PROJECT, config_override={"officer": {"enabled": True}}
        )
        deps = _deps(can_manage_project_officer=AsyncMock(return_value=False))
        with pytest.raises(HTTPException) as exc:
            await _plan(body, deps)
        assert exc.value.status_code == 403
        assert "commission an Officer" in exc.value.detail

    @pytest.mark.asyncio
    async def test_an_officer_create_must_come_through_the_post_endpoint(self):
        body = ThreadCreateRequest(
            project_id=PROJECT, config_override={"officer": {"enabled": True}}
        )
        with pytest.raises(HTTPException) as exc:
            await _plan(body, _deps())
        assert exc.value.status_code == 400
        assert "durable project Officer endpoint" in exc.value.detail

    @pytest.mark.asyncio
    async def test_a_held_post_refuses_a_second_officer(self):
        body = ThreadCreateRequest(
            project_id=PROJECT, config_override={"officer": {"enabled": True}}
        )
        object.__setattr__(body, "_officer_post_config_snapshot", {})
        deps = _deps(
            validated_post_owned_officer_create_fragment=MagicMock(return_value={}),
            store={
                "get_officer_thread_for_project": AsyncMock(return_value={"id": "x"})
            },
        )
        with pytest.raises(HTTPException) as exc:
            await _plan(body, deps)
        assert exc.value.status_code == 409
        assert "already commissioned" in exc.value.detail

    @pytest.mark.asyncio
    async def test_a_vm_session_without_a_provisioner_is_503(self):
        body = ThreadCreateRequest(config_override={"workspace": {"backend": "vm"}})
        deps = _deps(vm_provisioner=SimpleNamespace(is_available=False))
        with pytest.raises(HTTPException) as exc:
            await _plan(body, deps)
        assert exc.value.status_code == 503
        deps.check_vm_permission.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_grants_are_enforced_on_the_resolved_config_before_the_insert(self):
        deps = _deps(
            enforce_session_create_grants=AsyncMock(
                side_effect=HTTPException(status_code=422, detail="grant denied")
            )
        )
        with pytest.raises(HTTPException) as exc:
            await _plan(ThreadCreateRequest(), deps)
        assert exc.value.status_code == 422
        deps.store.create_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_stateless_k8s_thread_commits_its_create_nonce(self):
        deps = _deps(
            resolve_thread_execution_lane=MagicMock(return_value="stateless"),
            container_provisioner=SimpleNamespace(is_available=True, in_cluster=True),
        )
        plan = await _plan(ThreadCreateRequest(), deps)
        creation = plan.create_kwargs["initial_metadata"]["workspace_container"]
        assert creation["provisioner"] == "k8s"
        assert creation["_runtime_creation"]["mode"] == "create"
        assert creation["_runtime_creation"]["attempted"] is False

    @pytest.mark.asyncio
    async def test_an_unsupported_trusted_seed_is_refused(self):
        body = ThreadCreateRequest()
        object.__setattr__(
            body,
            "_trusted_seed",
            TrustedThreadSeed(metadata={"anything_else": 1}, opening_event="e"),
        )
        with pytest.raises(RuntimeError, match="Unsupported trusted thread seed"):
            await _plan(body, _deps())

    @pytest.mark.asyncio
    async def test_a_review_seed_is_committed_with_the_opening_event(self):
        body = ThreadCreateRequest()
        object.__setattr__(
            body,
            "_trusted_seed",
            TrustedThreadSeed(
                metadata={"review_delivery": {"job": "j"}}, opening_event="hello"
            ),
        )
        plan = await _plan(body, _deps())
        assert plan.create_kwargs["initial_event"] == "hello"
        assert plan.create_kwargs["initial_metadata"]["review_delivery"] == {"job": "j"}


# =============================================================================
# Commit
# =============================================================================


class TestCommit:
    @pytest.mark.asyncio
    async def test_a_row_without_a_readable_runtime_identity_is_409(self):
        deps = _deps(
            store={
                "get_thread": AsyncMock(
                    return_value=_created_thread(runtime_generation=None)
                )
            }
        )
        plan = await _plan(ThreadCreateRequest(), _deps())
        with pytest.raises(HTTPException) as exc:
            await ta.commit_thread_creation(
                plan, ThreadCreateRequest(), USER, dependencies=deps
            )
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_a_lost_officer_claim_stands_the_thread_down_through_end(self):
        body = ThreadCreateRequest(
            project_id=PROJECT, config_override={"officer": {"enabled": True}}
        )
        object.__setattr__(body, "_officer_post_config_snapshot", {})
        deps = _deps(
            validated_post_owned_officer_create_fragment=MagicMock(return_value={}),
            store={"register_project_officer_thread": AsyncMock(return_value=None)},
        )
        plan = await _plan(body, deps)
        with pytest.raises(HTTPException) as exc:
            await ta.commit_thread_creation(plan, body, USER, dependencies=deps)
        assert exc.value.status_code == 409
        assert "claimed by a concurrent officer create" in exc.value.detail
        deps.end_thread_flow.assert_awaited_once()
        assert deps.end_thread_flow.await_args.kwargs["force"] is True
        assert (
            deps.end_thread_flow.await_args.kwargs["officer_retire_reason"]
            == "commission_race_lost"
        )

    @pytest.mark.asyncio
    async def test_a_conference_holds_the_background_officer(self):
        body = ThreadCreateRequest(
            project_id=PROJECT, config_override={"officer": {"conference": True}}
        )
        deps = _deps()
        plan = await _plan(body, deps)
        await ta.commit_thread_creation(plan, body, USER, dependencies=deps)
        deps.hold_officer_for_conference.assert_awaited_once_with(PROJECT, THREAD)

    @pytest.mark.asyncio
    async def test_a_disabled_deployment_records_why_protected_cloud_is_absent(self):
        deps = _deps(is_protected_cloud_mode_enabled=MagicMock(return_value=False))
        plan = await _plan(ThreadCreateRequest(protected_cloud=True), deps)
        await ta.commit_thread_creation(
            plan, ThreadCreateRequest(protected_cloud=True), USER, dependencies=deps
        )
        deps.schedule_protected_engage.assert_not_called()
        deps.record_protected_error.assert_awaited_once()
        assert (
            deps.record_protected_error.await_args.kwargs["code"] == "feature_disabled"
        )

    @pytest.mark.asyncio
    async def test_a_mount_seeding_failure_never_fails_the_create(self):
        deps = _deps(
            authorize_thread_project_ids=AsyncMock(
                side_effect=lambda _u, ids: list(ids)
            ),
            build_thread_mount_rows=AsyncMock(side_effect=RuntimeError("gitea down")),
        )
        plan = await _plan(ThreadCreateRequest(project_id=PROJECT), deps)
        thread_id, authority = await ta.commit_thread_creation(
            plan, ThreadCreateRequest(project_id=PROJECT), USER, dependencies=deps
        )
        assert thread_id == THREAD
        assert authority.generation == GENERATION


# =============================================================================
# The whole operation
# =============================================================================


class TestCreateThreadOperation:
    @pytest.mark.asyncio
    async def test_the_readiness_gate_runs_before_authentication(self):
        deps = _deps(
            enforce_readiness_gate=AsyncMock(
                side_effect=HTTPException(status_code=503, detail={"ready": False})
            )
        )
        with pytest.raises(HTTPException) as exc:
            await ta.create_thread(
                ThreadCreateRequest(), MagicMock(), dependencies=deps
            )
        assert exc.value.status_code == 503
        deps.require_approved_user.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_plain_create_returns_the_thread_id(self):
        deps = _deps()
        result = await ta.create_thread(
            ThreadCreateRequest(title="t"), MagicMock(), dependencies=deps
        )
        assert result == {"thread_id": THREAD, "status": "created"}

    @pytest.mark.asyncio
    async def test_ignored_keys_are_echoed_on_the_response(self):
        deps = _deps()
        result = await ta.create_thread(
            ThreadCreateRequest(config_override={"memory": {"enabled": False}}),
            MagicMock(),
            dependencies=deps,
        )
        assert result["ignored_config_keys"] == ["memory.enabled"]

    @pytest.mark.asyncio
    async def test_cloud_and_git_setup_complete_before_the_agent_is_assigned(self):
        order: list[str] = []

        async def _bind(*_a, **_kw):
            order.append("gitea")
            return True

        deps = _deps(
            gitea_client=SimpleNamespace(
                is_initialized=True,
                is_configured=True,
                grant_user_repo_access=AsyncMock(),
            ),
            docker_provisioner=SimpleNamespace(is_available=True, in_cluster=False),
            store={"bind_thread_managed_repository": AsyncMock(side_effect=_bind)},
            find_idle_persistent_agent=AsyncMock(
                side_effect=lambda: order.append("agent") or None
            ),
        )
        from orchestrator.services import managed_repository_authority as mra

        original_create = ta.create_managed_repository
        ta.create_managed_repository = AsyncMock(
            return_value=("https://git/x.git", {"id": "intent"})
        )
        ta.ensure_managed_repository_authority = AsyncMock(
            return_value={"clean_repo_url": "https://git/x.git"}
        )
        try:
            await ta.create_thread(
                ThreadCreateRequest(), MagicMock(), dependencies=deps
            )
            await asyncio.sleep(0)
        finally:
            ta.create_managed_repository = original_create
            ta.ensure_managed_repository_authority = (
                mra.ensure_managed_repository_authority
            )
        assert order[0] == "gitea"
        assert "agent" in order

    @pytest.mark.asyncio
    async def test_a_materialization_denial_is_403(self):
        deps = _deps(
            require_approved_user=AsyncMock(
                side_effect=DatasourceMaterializationAuthorizationError()
            )
        )
        with pytest.raises(HTTPException) as exc:
            await ta.create_thread(
                ThreadCreateRequest(), MagicMock(), dependencies=deps
            )
        assert exc.value.status_code == 403
        assert exc.value.detail == "Work owner is no longer authorized"

    @pytest.mark.asyncio
    async def test_a_policy_conflict_asks_the_caller_to_retry(self):
        deps = _deps(
            require_approved_user=AsyncMock(side_effect=DatasourcePolicyConflictError())
        )
        with pytest.raises(HTTPException) as exc:
            await ta.create_thread(
                ThreadCreateRequest(), MagicMock(), dependencies=deps
            )
        assert exc.value.status_code == 409
        assert "retry the request" in exc.value.detail

    @pytest.mark.asyncio
    async def test_an_unexpected_failure_becomes_a_500(self):
        deps = _deps(require_approved_user=AsyncMock(side_effect=RuntimeError("boom")))
        with pytest.raises(HTTPException) as exc:
            await ta.create_thread(
                ThreadCreateRequest(), MagicMock(), dependencies=deps
            )
        assert exc.value.status_code == 500

    @pytest.mark.asyncio
    async def test_an_http_refusal_is_never_reshaped_into_a_500(self):
        deps = _deps(
            require_approved_user=AsyncMock(
                side_effect=HTTPException(status_code=401, detail="no token")
            )
        )
        with pytest.raises(HTTPException) as exc:
            await ta.create_thread(
                ThreadCreateRequest(), MagicMock(), dependencies=deps
            )
        assert exc.value.status_code == 401


class TestWorkspaceActuation:
    @pytest.mark.asyncio
    async def test_vm_create_forwards_the_selected_image_and_rootdisk(self):
        vm = {
            "image": "registry.example/dev-vm:v1",
            "cpu_cores": 12,
            "memory": "24Gi",
            "disk_size": "120Gi",
        }
        override = {"workspace": {"backend": "vm", "vm": vm}}
        deps = _deps(
            vm_provisioner=SimpleNamespace(
                is_available=True, create_thread_vm=AsyncMock(return_value=True)
            ),
            store={
                "get_thread": AsyncMock(
                    return_value=_created_thread(metadata={"config_override": override})
                )
            },
        )
        plan = await _plan(ThreadCreateRequest(config_override=override), deps)
        await ta.provision_thread_workspace(plan, THREAD, dependencies=deps)
        await asyncio.sleep(0)
        called = deps.vm_provisioner.create_thread_vm.await_args.kwargs
        assert called["vm_image"] == vm["image"]
        assert called["cpu_cores"] == 12
        assert called["memory"] == "24Gi"
        assert called["disk_size"] == "120Gi"
        assert called["expected_runtime_generation"] == GENERATION

    @pytest.mark.asyncio
    async def test_a_lite_session_provisions_no_workspace_pod(self):
        deps = _deps(
            container_provisioner=SimpleNamespace(
                is_available=True,
                in_cluster=True,
                create_pinned_thread_workspace=AsyncMock(),
            )
        )
        plan = await _plan(
            ThreadCreateRequest(config_override={"workspace": {"backend": "none"}}),
            deps,
        )
        await ta.provision_thread_workspace(plan, THREAD, dependencies=deps)
        await asyncio.sleep(0)
        deps.container_provisioner.create_pinned_thread_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_stateless_thread_goes_through_the_shared_lifecycle_owner(self):
        deps = _deps(
            resolve_thread_execution_lane=MagicMock(return_value="stateless"),
            container_provisioner=SimpleNamespace(
                is_available=True,
                in_cluster=True,
                create_pinned_thread_workspace=AsyncMock(),
            ),
        )
        plan = await _plan(ThreadCreateRequest(), deps)
        await ta.provision_thread_workspace(plan, THREAD, dependencies=deps)
        await asyncio.sleep(0)
        deps.schedule_stateless_workspace_ensure.assert_called_once_with(THREAD)
        deps.container_provisioner.create_pinned_thread_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_pinned_k8s_thread_provisions_its_own_container(self):
        deps = _deps(
            container_provisioner=SimpleNamespace(
                is_available=True,
                in_cluster=True,
                create_pinned_thread_workspace=AsyncMock(return_value=True),
            )
        )
        plan = await _plan(ThreadCreateRequest(), deps)
        await ta.provision_thread_workspace(plan, THREAD, dependencies=deps)
        await asyncio.sleep(0)
        deps.container_provisioner.create_pinned_thread_workspace.assert_awaited_once_with(
            THREAD
        )


class TestListThreads:
    @pytest.mark.asyncio
    async def test_mounts_are_fetched_in_one_query_and_metadata_is_redacted(self):
        rows = [{"id": THREAD}, {"id": OTHER_PROJECT}]
        deps = _deps(
            store={
                "list_threads": AsyncMock(return_value=rows),
                "list_thread_mounts_bulk": AsyncMock(
                    return_value={THREAD: [{"webdav_url": "https://c/x"}]}
                ),
            },
            resolve_cloud_session_url=MagicMock(return_value="https://c/x"),
            redact_thread_metadata=MagicMock(
                side_effect=lambda t: {**t, "redacted": True}
            ),
        )
        result = await ta.list_threads(MagicMock(), dependencies=deps)
        deps.store.list_thread_mounts_bulk.assert_awaited_once_with(
            [THREAD, OTHER_PROJECT]
        )
        assert all(t["redacted"] for t in result["threads"])
        assert result["threads"][0]["cloud_session_url"] == "https://c/x"

    @pytest.mark.asyncio
    async def test_a_store_failure_is_a_500(self):
        deps = _deps(store={"list_threads": AsyncMock(side_effect=RuntimeError("x"))})
        with pytest.raises(HTTPException) as exc:
            await ta.list_threads(MagicMock(), dependencies=deps)
        assert exc.value.status_code == 500
