"""R1.B06 lane B — wire-level behaviour of the seven moved routes.

Inspecting a model is not coverage of a route, so these mount the two extracted
routers on a bare ``FastAPI()`` and drive them over HTTP: status codes, error
bodies, request validation and the fact that each handler resolves its
collaborators from the application answering the request rather than from a
process-wide lookup.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from tests._route_inventory import iter_mounted_route_objects

from orchestrator.routers import thread_admission as admission_routes
from orchestrator.routers import thread_config as config_routes
from orchestrator.services import thread_admission as ta
from orchestrator.services import thread_config_update as tcu

THREAD = "11111111-1111-4111-8111-111111111111"
GENERATION = "22222222-2222-4222-8222-222222222222"
USER = {"id": "55555555-5555-4555-8555-555555555555", "is_admin": False}


def _thread(**over: Any) -> dict[str, Any]:
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
    }
    row.update(over)
    return row


def _lock():
    @contextlib.asynccontextmanager
    async def _scope(_thread_id: str):
        yield None

    return _scope


def _transaction(conn: Any):
    @contextlib.asynccontextmanager
    async def _scope(_thread_id: str):
        yield conn

    return _scope


def _admission_deps(**over: Any) -> ta.ThreadAdmissionDependencies:
    store_over = over.pop("store", {})
    store = SimpleNamespace(
        get_user_settings=AsyncMock(return_value={}),
        get_thread=AsyncMock(return_value=_thread()),
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
    absent = SimpleNamespace(is_available=False, in_cluster=False)
    fields: dict[str, Any] = dict(
        store=store,
        gitea_client=SimpleNamespace(is_initialized=False, is_configured=False),
        main_cloud_router=SimpleNamespace(
            active_instance_id=None, for_owner=MagicMock()
        ),
        agent_provisioner=absent,
        container_provisioner=absent,
        docker_provisioner=absent,
        persistent_provisioner=absent,
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
        resolve_session_account_defaults=AsyncMock(return_value={}),
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


def _config_deps(**over: Any) -> tcu.ThreadConfigUpdateDependencies:
    conn = over.pop("conn", None) or SimpleNamespace(
        fetchrow=AsyncMock(return_value=_thread())
    )
    store_over = over.pop("store", {})
    store = SimpleNamespace(
        get_thread=AsyncMock(return_value=_thread()),
        thread_configuration_transaction=_transaction(conn),
        thread_advisory_lock=_lock(),
        refresh_session_execution=AsyncMock(
            return_value={"delivery_override": {"llm": {"api_key": "sk-live"}}}
        ),
        merge_thread_vm_context=AsyncMock(),
    )
    for key, value in store_over.items():
        setattr(store, key, value)
    fields: dict[str, Any] = dict(
        store=store,
        vm_provisioner=SimpleNamespace(
            is_available=True,
            lifecycle_available=True,
            mode="same-cluster",
            create_thread_vm=AsyncMock(return_value=True),
            delete_thread_vm=AsyncMock(return_value=True),
        ),
        container_provisioner=SimpleNamespace(
            is_available=True,
            in_cluster=True,
            create_pinned_thread_workspace=AsyncMock(return_value=True),
        ),
        enforce_workspace_upgrade_grants=AsyncMock(),
        require_internal=AsyncMock(),
        require_thread_owner=AsyncMock(return_value=(USER, _thread())),
        thread_project_ids=AsyncMock(return_value=[]),
        authorize_thread_datasource_selection=AsyncMock(return_value=([], {})),
        build_datasource_tool_override=MagicMock(return_value={}),
        datasource_selection_provenance=AsyncMock(return_value={}),
        enforce_session_create_grants=AsyncMock(),
        inject_model_credentials=AsyncMock(),
        log_security_event=AsyncMock(),
    )
    fields.update(over)
    return tcu.ThreadConfigUpdateDependencies(
        recovery_store=SimpleNamespace(), **fields
    )


@pytest.fixture(autouse=True)
def locked_commit_core(monkeypatch) -> AsyncMock:
    """The commit core is characterized in test_b12_thread_config_update_policy;
    here it is stubbed at its owner so these tests pin only the wire."""
    core = AsyncMock(return_value=({"llm": {"model": "m"}}, ["d1"]))
    monkeypatch.setattr(tcu, "apply_thread_config_update_locked", core)
    return core


def _client(admission=None, config=None) -> TestClient:
    app = FastAPI()
    app.include_router(admission_routes.router)
    app.include_router(config_routes.router)
    app.state.thread_admission_dependencies_factory = lambda: (
        admission if admission is not None else _admission_deps()
    )
    app.state.thread_config_dependencies_factory = lambda: (
        config if config is not None else _config_deps()
    )
    return TestClient(app, raise_server_exceptions=False)


class TestRouteInventory:
    def test_the_seven_declarations_keep_their_identity(self):
        app = FastAPI()
        app.include_router(admission_routes.router)
        app.include_router(config_routes.router)
        moved = [
            route
            for route in iter_mounted_route_objects(app.routes)
            if str(route.path).startswith("/api/")
        ]
        seen = {
            (route.path, method, route.name)
            for route in moved
            for method in route.methods
        }
        assert seen == {
            ("/api/persistent/threads/preview", "POST", "preview_thread_creation"),
            ("/api/persistent/threads", "POST", "create_thread"),
            ("/api/persistent/threads", "GET", "list_threads"),
            (
                "/api/persistent/threads/{thread_id}/config",
                "PATCH",
                "update_thread_config",
            ),
            (
                "/api/agents/threads/{thread_id}/config",
                "PATCH",
                "agent_update_thread_config",
            ),
            (
                "/api/agents/threads/{thread_id}/upgrade-to-vm",
                "POST",
                "agent_upgrade_thread_to_vm",
            ),
            (
                "/api/agents/threads/{thread_id}/abort-vm-upgrade",
                "POST",
                "agent_abort_thread_vm_upgrade",
            ),
            (
                "/api/agents/threads/{thread_id}/upgrade-to-workspace",
                "POST",
                "agent_upgrade_thread_to_workspace",
            ),
        }
        for route in moved:
            assert route.tags == []
            assert route.status_code is None
            assert route.dependencies == []


class TestCreationPreview:
    @staticmethod
    def dependencies(backend="virtual"):
        repository = {
            "id": "66666666-6666-4666-8666-666666666666",
            "type": "repository",
            "created_by": USER["id"],
            "auto_attach": True,
            "scope_mode": "all",
            "policy_revision": 1,
            "project_ids": [],
        }
        compatible = {**repository, "id": GENERATION, "type": "postgresql"}
        foreign = {**compatible, "id": THREAD, "created_by": THREAD}
        out_of_scope = {
            **compatible,
            "id": "77777777-7777-4777-8777-777777777777",
            "scope_mode": "projects",
        }
        rows = [repository, compatible, foreign, out_of_scope]
        deps = _admission_deps(
            resolve_session_account_defaults=AsyncMock(
                return_value={"workspace": {"backend": backend}}
            ),
            store={
                "get_user": AsyncMock(return_value={**USER, "is_approved": True}),
                "user_is_member_of_projects": AsyncMock(return_value=True),
                "list_default_datasource_candidates": AsyncMock(return_value=rows),
                "get_datasource_policy_rows": AsyncMock(return_value=[repository]),
                "get_project": AsyncMock(return_value={"id": THREAD}),
                "get_user_role_in_project": AsyncMock(return_value="owner"),
            },
        )
        return deps, repository

    @pytest.mark.parametrize("backend", ["virtual", "none", "sandbox", "vm"])
    def test_defaults_follow_effective_workspace_and_scope_without_creating_work(
        self, backend
    ):
        deps, repository = self.dependencies(backend)
        response = _client(admission=deps).post(
            "/api/persistent/threads/preview", json={"use_datasource_defaults": True}
        )
        assert response.status_code == 200, response.text
        assert response.json() == {
            "project_ids": [],
            "workspace_backend": backend,
            "datasource_ids": (
                [repository["id"], GENERATION]
                if backend in ("sandbox", "vm")
                else [GENERATION]
            ),
        }
        deps.store.create_thread.assert_not_awaited()
        deps.store.replace_thread_mounts.assert_not_awaited()
        deps.provision_or_assign.assert_not_awaited()
        deps.send_session_attach.assert_not_awaited()
        deps.schedule_stateless_workspace_ensure.assert_not_called()

    def test_project_default_and_explicit_selection_use_the_create_resolver(
        self, monkeypatch
    ):
        from orchestrator.services import manifest_projects

        project = {
            "id": THREAD,
            "kind": "Project",
            "linked_id": THREAD,
            "revision": 1,
            "dependencies": [],
            "resolved": {
                "spec": {
                    "defaults": {"workspace": "code"},
                    "resources": {
                        "workspaces": {"code": {"inline": {"backend": "sandbox"}}}
                    },
                }
            },
        }
        monkeypatch.setattr(
            manifest_projects,
            "active_project_resource",
            AsyncMock(return_value=project),
        )
        deps, repository = self.dependencies()
        client = _client(admission=deps)
        body = {"project_ids": [THREAD], "use_datasource_defaults": True}
        response = client.post("/api/persistent/threads/preview", json=body)
        assert response.status_code == 200, response.text
        assert response.json()["workspace_backend"] == "sandbox"
        assert repository["id"] in response.json()["datasource_ids"]
        explicit = client.post(
            "/api/persistent/threads/preview", json={**body, "workspace": None}
        )
        assert explicit.status_code == 200, explicit.text
        assert explicit.json()["workspace_backend"] == "none"
        assert explicit.json()["datasource_ids"] == [GENERATION]
        deps.store.create_thread.assert_not_awaited()

    def test_explicit_repository_is_still_refused_and_opt_out_is_empty(self):
        from orchestrator.services.thread_datasource_authorization import (
            ThreadDatasourceAuthorizationDependencies,
            authorize_thread_datasource_selection,
        )

        deps, repository = self.dependencies()

        async def authorize(user, ids, **kwargs):
            return await authorize_thread_datasource_selection(
                user,
                ids,
                **kwargs,
                dependencies=ThreadDatasourceAuthorizationDependencies(
                    store=deps.store, thread_project_ids=AsyncMock(return_value=[])
                ),
            )

        from dataclasses import replace

        deps = replace(deps, authorize_thread_datasource_selection=authorize)
        client = _client(admission=deps)
        rejected = client.post(
            "/api/persistent/threads/preview",
            json={"datasource_ids": [repository["id"]]},
        )
        assert rejected.status_code == 400
        assert rejected.json() == {
            "detail": "Repository and credential connectors require a sandbox or VM workspace"
        }
        empty = client.post(
            "/api/persistent/threads/preview", json={"datasource_ids": []}
        )
        assert empty.status_code == 200, empty.text
        assert empty.json()["datasource_ids"] == []
        deps.store.list_default_datasource_candidates.assert_not_awaited()

    def test_auth_and_project_authority_precede_preview(self):
        deps, _ = self.dependencies()
        deps.require_approved_user.side_effect = HTTPException(401, "Not authenticated")
        response = _client(admission=deps).post(
            "/api/persistent/threads/preview", json={}
        )
        assert response.status_code == 401
        deps.store.get_user_settings.assert_not_awaited()
        deps.require_approved_user.side_effect = None
        deps.authorize_thread_project_ids.side_effect = HTTPException(
            403, "Project access denied"
        )
        response = _client(admission=deps).post(
            "/api/persistent/threads/preview",
            json={"project_ids": [THREAD], "use_datasource_defaults": True},
        )
        assert response.status_code == 403
        deps.store.get_user_settings.assert_not_awaited()
        deps.store.list_default_datasource_candidates.assert_not_awaited()


class TestCreate:
    def test_a_plain_create_answers_200(self):
        response = _client().post("/api/persistent/threads", json={"title": "t"})
        assert response.status_code == 200
        assert response.json() == {"thread_id": THREAD, "status": "created"}

    def test_the_public_boundary_refuses_an_execution_lane_selector(self):
        response = _client().post(
            "/api/persistent/threads", json={"execution_lane": "stateless"}
        )
        assert response.status_code == 422
        assert "execution_lane is orchestrator-managed" in response.text

    def test_a_null_connector_selection_is_refused_rather_than_coerced(self):
        response = _client().post(
            "/api/persistent/threads", json={"datasource_ids": None}
        )
        assert response.status_code == 422
        assert "may be omitted or an array, not null" in response.text

    def test_defaults_and_an_explicit_selection_are_mutually_exclusive(self):
        response = _client().post(
            "/api/persistent/threads",
            json={"datasource_ids": [], "use_datasource_defaults": True},
        )
        assert response.status_code == 422

    def test_an_unauthenticated_caller_gets_the_guards_own_status(self):
        deps = _admission_deps(
            require_approved_user=AsyncMock(
                side_effect=HTTPException(status_code=401, detail="Not authenticated")
            )
        )
        response = _client(admission=deps).post("/api/persistent/threads", json={})
        assert response.status_code == 401
        assert response.json() == {"detail": "Not authenticated"}

    def test_a_readiness_failure_carries_the_gate_body(self):
        deps = _admission_deps(
            enforce_readiness_gate=AsyncMock(
                side_effect=HTTPException(
                    status_code=503, detail={"ready": False, "missing_chat_model": True}
                )
            )
        )
        response = _client(admission=deps).post("/api/persistent/threads", json={})
        assert response.status_code == 503
        assert response.json()["detail"]["missing_chat_model"] is True

    def test_a_protected_cloud_tier_mismatch_keeps_its_error_code(self):
        response = _client().post(
            "/api/persistent/threads",
            json={
                "protected_cloud": True,
                "config_override": {"workspace": {"backend": "virtual"}},
            },
        )
        assert response.status_code == 422
        assert (
            response.json()["detail"]["code"] == "protected_cloud_unsupported_workspace"
        )


class TestList:
    def test_query_parameters_reach_the_store(self):
        deps = _admission_deps(
            store={"list_threads": AsyncMock(return_value=[])},
        )
        response = _client(admission=deps).get(
            "/api/persistent/threads", params={"project_id": "p-1", "status": "ended"}
        )
        assert response.status_code == 200
        assert response.json() == {"threads": []}
        deps.store.list_threads.assert_awaited_once_with(
            user_id=USER["id"], project_id="p-1", status="ended"
        )


class TestInternalConfigRoutes:
    def test_the_internal_guard_runs_first(self):
        deps = _config_deps(
            require_internal=AsyncMock(
                side_effect=HTTPException(status_code=403, detail="Internal only")
            )
        )
        response = _client(config=deps).patch(
            f"/api/agents/threads/{THREAD}/config", json={"config_override": {}}
        )
        assert response.status_code == 403
        deps.store.get_thread.assert_not_awaited()

    def test_the_internal_patch_returns_plaintext_transport(self, monkeypatch):
        from orchestrator.services import manifest_execution_snapshot

        monkeypatch.setattr(
            manifest_execution_snapshot, "read_execution", AsyncMock(return_value=None)
        )
        response = _client().patch(
            f"/api/agents/threads/{THREAD}/config",
            json={"config_override": {"llm": {"model": "m"}}},
        )
        assert response.status_code == 200
        assert response.json()["config_override"] == {"llm": {"api_key": "sk-live"}}

    def test_a_snapshot_generation_below_1_is_rejected_by_the_model(self):
        response = _client().patch(
            f"/api/agents/threads/{THREAD}/config",
            json={"config_override": {}, "snapshot_generation": 0},
        )
        assert response.status_code == 422

    def test_upgrade_to_vm_reports_the_provisioner_mode(self):
        response = _client().post(f"/api/agents/threads/{THREAD}/upgrade-to-vm")
        assert response.status_code == 200
        assert response.json()["vm_provisioner_mode"] == "same-cluster"

    def test_upgrade_to_vm_on_the_stateless_lane_is_409(self):
        deps = _config_deps(
            store={
                "get_thread": AsyncMock(
                    return_value=_thread(execution_lane="stateless")
                )
            }
        )
        response = _client(config=deps).post(
            f"/api/agents/threads/{THREAD}/upgrade-to-vm"
        )
        assert response.status_code == 409
        assert "stateless lane" in response.json()["detail"]

    def test_a_lost_vm_generation_race_keeps_its_code(self):
        deps = _config_deps()
        deps.vm_provisioner.create_thread_vm = AsyncMock(return_value=False)
        response = _client(config=deps).post(
            f"/api/agents/threads/{THREAD}/upgrade-to-vm"
        )
        assert response.status_code == 409
        assert response.json()["detail"] == {"code": "vm_provision_authority_changed"}

    def test_an_unproven_vm_teardown_is_a_retryable_503(self):
        deps = _config_deps()
        deps.vm_provisioner.delete_thread_vm = AsyncMock(return_value=False)
        response = _client(config=deps).post(
            f"/api/agents/threads/{THREAD}/abort-vm-upgrade"
        )
        assert response.status_code == 503
        assert response.json()["detail"] == {
            "code": "vm_process_zero_unproven",
            "retryable": True,
        }

    def test_upgrade_to_workspace_defaults_to_sandbox_without_a_body(self):
        response = _client().post(f"/api/agents/threads/{THREAD}/upgrade-to-workspace")
        assert response.status_code == 200
        assert response.json()["target_tier"] == "sandbox"

    def test_an_unsupported_tier_is_400(self):
        response = _client().post(
            f"/api/agents/threads/{THREAD}/upgrade-to-workspace",
            json={"target_tier": "desktop"},
        )
        assert response.status_code == 400
        assert "'sandbox' or 'vm'" in response.json()["detail"]

    def test_a_protected_session_cannot_replace_its_container(self):
        deps = _config_deps(
            store={
                "get_thread": AsyncMock(
                    return_value=_thread(metadata={"protected_cloud": True})
                )
            }
        )
        response = _client(config=deps).post(
            f"/api/agents/threads/{THREAD}/upgrade-to-workspace"
        )
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "protected_cloud_workspace_fixed"


class TestOwnerConfigRoute:
    def test_a_connected_session_is_refused(self):
        deps = _config_deps(
            require_thread_owner=AsyncMock(return_value=(USER, _thread(agent_id="a-1")))
        )
        response = _client(config=deps).patch(
            f"/api/persistent/threads/{THREAD}/config",
            json={"config_override": {"llm": {"model": "m"}}},
        )
        assert response.status_code == 409
        assert "settings pane" in response.json()["detail"]

    def test_an_empty_patch_is_400(self):
        response = _client().patch(f"/api/persistent/threads/{THREAD}/config", json={})
        assert response.status_code == 400
        assert response.json()["detail"] == "No changes provided"

    def test_the_browser_facing_body_never_carries_transport_secrets(self):
        response = _client().patch(
            f"/api/persistent/threads/{THREAD}/config",
            json={"config_override": {"llm": {"model": "m"}}},
        )
        assert response.status_code == 200
        body = response.json()
        assert "sk-live" not in response.text
        assert body["effective"] == "next_attach"


class TestApplicationScopedDependencies:
    def test_each_application_answers_with_its_own_collaborators(self):
        """Two apps in one process must not share one dependency object."""
        first = _admission_deps(store={"create_thread": AsyncMock(return_value=THREAD)})
        second = _admission_deps(
            store={
                "create_thread": AsyncMock(
                    return_value="99999999-9999-4999-8999-999999999999"
                )
            }
        )
        assert (
            _client(admission=first)
            .post("/api/persistent/threads", json={})
            .json()["thread_id"]
            == THREAD
        )
        assert (
            _client(admission=second)
            .post("/api/persistent/threads", json={})
            .json()["thread_id"]
            == "99999999-9999-4999-8999-999999999999"
        )
        first.store.create_thread.assert_awaited_once()
        second.store.create_thread.assert_awaited_once()
