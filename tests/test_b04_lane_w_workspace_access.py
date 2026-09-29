"""Wire contracts for the extracted frozen/snapshot/workspace-access routes.

R1.B04 lane W. The refusal ordering is the point: the internal-key gate fires
before any body-shaped 400, the owner-grant check fires before provisioning,
and ``ensure-workspace-access`` keeps its authentication *inside* the
try/except that turns an unexpected failure into a logged 500.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from orchestrator.routers.workspace_access import (
    WorkspaceAccessDependencies,
    get_workspace_access_dependencies,
    router,
)
from orchestrator.services import workspace_access as operations
from orchestrator.services.workspace_access import WorkspaceOperationDependencies

JOB = "55555555-5555-4555-8555-555555555555"
USER = {"id": "u-1", "email": "owner@example.test", "is_approved": True}


class _Contract:
    def __init__(self, assigned="virtual", requested="virtual"):
        self.assigned_backend = assigned
        self.requested_backend = requested


@pytest.fixture
def wire(monkeypatch):
    # The upgrade resolves the shipped chain: no built-ins declared.
    monkeypatch.delenv("WORKSPACE_DEFAULTS", raising=False)
    monkeypatch.delenv("WORKSPACE_BUILTIN_TEMPLATES", raising=False)
    events: list[str] = []

    async def require_job_access(request, store, job_id):
        events.append("job_access")
        if request.headers.get("x-test-user") != USER["id"]:
            raise HTTPException(status_code=401, detail="Authentication required")
        return USER, dict(state.job)

    async def require_approved_user(request, store):
        events.append("approved")
        if state.approve_error is not None:
            raise state.approve_error
        return USER

    async def require_admin(request):
        events.append("admin")
        if request.headers.get("x-test-admin") != "1":
            raise HTTPException(status_code=403, detail="Admin access required")
        return USER

    async def require_internal(request):
        events.append("internal")
        if request.headers.get("x-internal-key") != "secret":
            raise HTTPException(status_code=401, detail="Invalid internal key")

    async def enforce_grants(job, *, target_tier):
        events.append("grants")
        if state.grants_error is not None:
            raise state.grants_error

    async def resolve_job_repo(job_id):
        events.append("resolve_repo")
        return state.repo

    state = SimpleNamespace(
        job={"id": JOB, "status": "processing", "context": {}, "user_id": USER["id"]},
        repo=("srw-job", "main"),
        approve_error=None,
        grants_error=None,
        contract=_Contract(),
        transitioned=True,
        builds=0,
    )

    store = SimpleNamespace(
        get_job=AsyncMock(side_effect=lambda _id: dict(state.job)),
        begin_job_workspace_tier_transition=AsyncMock(
            side_effect=lambda *_a, **_kw: state.transitioned
        ),
        get_user=AsyncMock(return_value=USER),
    )
    forge = SimpleNamespace(
        is_initialized=True,
        get_file=AsyncMock(return_value=None),
        grant_user_repo_access=AsyncMock(return_value=True),
    )
    snapshots = SimpleNamespace(
        get_snapshot_status=AsyncMock(return_value={"status": "available"}),
        delete_snapshot=AsyncMock(return_value=True),
        toggle_pin=AsyncMock(return_value=True),
        get_storage_stats=AsyncMock(return_value={"count": 3}),
    )
    workspace = SimpleNamespace(base_path=SimpleNamespace())
    provisioner = SimpleNamespace(
        is_available=True,
        in_cluster=True,
        create_workspace=AsyncMock(return_value=None),
    )
    holder = SimpleNamespace(
        store=store,
        forge=forge,
        snapshots=snapshots,
        workspace=workspace,
        provisioner=provisioner,
    )

    def factory():
        state.builds += 1
        return WorkspaceAccessDependencies(
            store=holder.store,
            forge=holder.forge,
            workspace=holder.workspace,
            snapshots=holder.snapshots,
            operations=WorkspaceOperationDependencies(
                store=holder.store,
                forge=holder.forge,
                container_provisioner=holder.provisioner,
                enforce_job_workspace_upgrade_grants=enforce_grants,
            ),
            resolve_job_repo=resolve_job_repo,
            require_admin=require_admin,
            require_approved_user=require_approved_user,
            require_job_access=require_job_access,
            require_internal=require_internal,
        )

    monkeypatch.setattr(
        operations, "resolve_workspace_contract", lambda _job: state.contract
    )
    monkeypatch.setattr(
        operations,
        "WorkspaceOwner",
        SimpleNamespace(job=lambda job_id: ("job", job_id)),
    )

    app = FastAPI()
    app.state.workspace_access_dependencies_factory = factory
    app.include_router(router)
    return SimpleNamespace(
        app=app, state=state, holder=holder, events=events, factory=factory
    )


_DEFAULT = object()


async def call(wire, method, path, *, headers=_DEFAULT, **kwargs):
    if headers is _DEFAULT:
        headers = {"x-test-user": USER["id"]}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wire.app), base_url="http://ws.test"
    ) as client:
        return await client.request(method, path, headers=headers, **kwargs)


# =============================================================================
# Frozen job data
# =============================================================================


class TestFrozenJobData:
    @pytest.mark.asyncio
    async def test_the_gate_runs_before_any_source_is_read(self, wire):
        response = await call(wire, "GET", f"/api/jobs/{JOB}/frozen", headers={})

        assert response.status_code == 401
        assert wire.events == ["job_access"]
        wire.holder.forge.get_file.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_db_freeze_data_wins_and_json_strings_are_parsed(self, wire):
        wire.state.job = dict(wire.state.job, freeze_data='{"summary": "done"}')

        response = await call(wire, "GET", f"/api/jobs/{JOB}/frozen")

        assert response.status_code == 200
        assert response.json() == {"summary": "done"}
        assert "resolve_repo" not in wire.events

    @pytest.mark.asyncio
    async def test_gitea_is_the_first_fallback(self, wire):
        wire.holder.forge.get_file = AsyncMock(return_value={"summary": "from-gitea"})

        response = await call(wire, "GET", f"/api/jobs/{JOB}/frozen")

        assert response.json() == {"summary": "from-gitea"}
        assert wire.holder.forge.get_file.await_args.args[1] == "output/job_frozen.json"
        assert wire.holder.forge.get_file.await_args.kwargs == {"ref": "main"}

    @pytest.mark.asyncio
    async def test_an_uninitialized_forge_is_skipped_not_called(self, wire, tmp_path):
        wire.holder.forge.is_initialized = False
        path = tmp_path / "output" / "job_frozen.json"
        path.parent.mkdir(parents=True)
        path.write_text('{"summary": "local"}')
        wire.holder.workspace = SimpleNamespace(base_path=tmp_path)

        response = await call(wire, "GET", f"/api/jobs/{JOB}/frozen")

        assert response.json() == {"summary": "local"}
        wire.holder.forge.get_file.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_all_three_sources_missing_is_a_404(self, wire, tmp_path):
        wire.holder.workspace = SimpleNamespace(base_path=tmp_path)

        response = await call(wire, "GET", f"/api/jobs/{JOB}/frozen")

        assert response.status_code == 404
        assert response.json()["detail"] == f"No frozen job data found for job '{JOB}'"

    @pytest.mark.asyncio
    async def test_an_unexpected_failure_is_a_500_carrying_str(self, wire):
        wire.holder.forge.get_file = AsyncMock(side_effect=RuntimeError("forge down"))

        response = await call(wire, "GET", f"/api/jobs/{JOB}/frozen")

        assert response.status_code == 500
        assert response.json()["detail"] == "forge down"


# =============================================================================
# Snapshots
# =============================================================================


class TestSnapshotRoutes:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method,path",
        [
            ("GET", "/snapshot"),
            ("DELETE", "/snapshot"),
            ("PUT", "/snapshot/pin"),
        ],
    )
    async def test_each_snapshot_route_gates_first(self, wire, method, path):
        response = await call(wire, method, f"/api/jobs/{JOB}{path}", headers={})

        assert response.status_code == 401
        assert wire.events == ["job_access"]

    @pytest.mark.asyncio
    async def test_status_is_returned_verbatim(self, wire):
        response = await call(wire, "GET", f"/api/jobs/{JOB}/snapshot")

        assert response.status_code == 200
        assert response.json() == {"status": "available"}

    @pytest.mark.asyncio
    async def test_a_refused_delete_is_a_500_with_its_own_message(self, wire):
        wire.holder.snapshots.delete_snapshot = AsyncMock(return_value=False)

        response = await call(wire, "DELETE", f"/api/jobs/{JOB}/snapshot")

        assert response.status_code == 500
        assert response.json()["detail"] == "Failed to delete snapshot"

    @pytest.mark.asyncio
    async def test_a_successful_delete_reports_the_job(self, wire):
        response = await call(wire, "DELETE", f"/api/jobs/{JOB}/snapshot")

        assert response.json() == {"status": "deleted", "job_id": JOB}

    @pytest.mark.asyncio
    async def test_pin_returns_the_new_value(self, wire):
        response = await call(wire, "PUT", f"/api/jobs/{JOB}/snapshot/pin")

        assert response.json() == {"job_id": JOB, "pinned": True}

    @pytest.mark.asyncio
    async def test_stats_are_admin_only(self, wire):
        response = await call(wire, "GET", "/api/snapshots/stats")

        assert response.status_code == 403
        assert wire.events == ["admin"]
        wire.holder.snapshots.get_storage_stats.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stats_reach_an_admin(self, wire):
        response = await call(
            wire, "GET", "/api/snapshots/stats", headers={"x-test-admin": "1"}
        )

        assert response.status_code == 200
        assert response.json() == {"count": 3}

    @pytest.mark.asyncio
    async def test_stats_failure_is_a_500_carrying_str(self, wire):
        wire.holder.snapshots.get_storage_stats = AsyncMock(
            side_effect=RuntimeError("s3 down")
        )

        response = await call(
            wire, "GET", "/api/snapshots/stats", headers={"x-test-admin": "1"}
        )

        assert response.status_code == 500
        assert response.json()["detail"] == "s3 down"


# =============================================================================
# ensure-workspace-access
# =============================================================================


class TestEnsureWorkspaceAccess:
    PATH = f"/api/jobs/{JOB}/ensure-workspace-access"

    @pytest.mark.asyncio
    async def test_authentication_precedes_the_job_lookup(self, wire):
        wire.state.approve_error = HTTPException(status_code=403, detail="pending")

        response = await call(wire, "POST", self.PATH)

        assert response.status_code == 403
        assert response.json()["detail"] == "pending"
        wire.holder.store.get_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_non_http_gate_failure_becomes_a_logged_500(self, wire):
        wire.state.approve_error = RuntimeError("keycloak down")

        response = await call(wire, "POST", self.PATH)

        assert response.status_code == 500
        assert response.json()["detail"] == "keycloak down"

    @pytest.mark.asyncio
    async def test_a_missing_job_is_a_404(self, wire):
        wire.holder.store.get_job = AsyncMock(return_value=None)

        response = await call(wire, "POST", self.PATH)

        assert response.status_code == 404
        assert response.json()["detail"] == f"Job '{JOB}' not found"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "setup,reason",
        [
            ({}, "no_repo"),
            ({"repo_name": "srw-job"}, "gitea_unavailable"),
            ({"repo_name": "srw-job"}, "no_email"),
        ],
    )
    async def test_each_missing_precondition_is_a_200_with_a_reason(
        self, wire, setup, reason
    ):
        wire.state.job = dict(wire.state.job, **setup)
        if reason == "gitea_unavailable":
            wire.holder.forge.is_initialized = False
        if reason == "no_email":
            user_without_email = {k: v for k, v in USER.items() if k != "email"}
            wire.state.approve_error = None
            wire.holder.store.get_job = AsyncMock(
                side_effect=lambda _id: dict(wire.state.job)
            )

            async def approve(_request, _store):
                return user_without_email

            wire.app.state.workspace_access_dependencies_factory = lambda: _swap_gate(
                wire.factory(), approve
            )

        response = await call(wire, "POST", self.PATH)

        assert response.status_code == 200
        assert response.json() == {"granted": False, "reason": reason}

    @pytest.mark.asyncio
    async def test_a_granted_user_reports_ok(self, wire):
        wire.state.job = dict(wire.state.job, repo_name="srw-job")

        response = await call(wire, "POST", self.PATH)

        assert response.json() == {"granted": True, "reason": "ok"}
        wire.holder.forge.grant_user_repo_access.assert_awaited_once_with(
            USER["email"], "srw-job"
        )

    @pytest.mark.asyncio
    async def test_a_user_missing_from_gitea_is_reported_not_raised(self, wire):
        wire.state.job = dict(wire.state.job, repo_name="srw-job")
        wire.holder.forge.grant_user_repo_access = AsyncMock(return_value=False)

        response = await call(wire, "POST", self.PATH)

        assert response.json() == {"granted": False, "reason": "user_not_in_gitea"}


def _swap_gate(dependencies, approve):
    return WorkspaceAccessDependencies(
        store=dependencies.store,
        forge=dependencies.forge,
        workspace=dependencies.workspace,
        snapshots=dependencies.snapshots,
        operations=dependencies.operations,
        resolve_job_repo=dependencies.resolve_job_repo,
        require_admin=dependencies.require_admin,
        require_approved_user=approve,
        require_job_access=dependencies.require_job_access,
        require_internal=dependencies.require_internal,
    )


# =============================================================================
# provision-workspace
# =============================================================================


class TestProvisionJobWorkspace:
    PATH = f"/api/jobs/{JOB}/provision-workspace"
    INTERNAL = {"x-internal-key": "secret"}

    @pytest.mark.asyncio
    async def test_the_internal_gate_precedes_the_target_tier_refusal(self, wire):
        response = await call(
            wire, "POST", self.PATH, headers={}, json={"target_tier": "vm"}
        )

        assert response.status_code == 401
        assert wire.events == ["internal"]
        wire.holder.store.get_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_vm_is_refused_with_its_exact_message(self, wire):
        response = await call(
            wire, "POST", self.PATH, headers=self.INTERNAL, json={"target_tier": "vm"}
        )

        assert response.status_code == 400
        assert response.json()["detail"] == (
            "provision-workspace supports target_tier 'sandbox' only for a "
            "running job; vm upgrades go through /upgrade-to-vm "
            "(operator-gated). Got 'vm'"
        )

    @pytest.mark.asyncio
    async def test_an_absent_body_defaults_to_sandbox(self, wire):
        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 200
        assert response.json()["target_tier"] == "sandbox"
        await asyncio.sleep(0)

    @pytest.mark.asyncio
    async def test_a_missing_job_is_a_404(self, wire):
        wire.holder.store.get_job = AsyncMock(return_value=None)

        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 404
        assert response.json()["detail"] == "Job not found"

    @pytest.mark.asyncio
    async def test_a_contract_error_is_a_typed_409(self, wire, monkeypatch):
        from shared.workspace_contract import WorkspaceContractError

        error = WorkspaceContractError("workspace_contract_invalid", "bad row")
        monkeypatch.setattr(
            operations,
            "resolve_workspace_contract",
            MagicMock(side_effect=error),
        )

        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 409
        assert response.json()["detail"] == {
            "code": error.code,
            "message": error.detail,
        }

    @pytest.mark.asyncio
    async def test_a_vm_assignment_is_a_backend_conflict(self, wire):
        wire.state.contract = _Contract(assigned="vm", requested="vm")

        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 409
        assert response.json()["detail"] == {
            "code": "workspace_backend_conflict",
            "message": (
                "This job is assigned to the VM tier; an in-process "
                "sandbox upgrade would violate its workspace contract"
            ),
            "assigned_backend": "vm",
        }

    @pytest.mark.asyncio
    async def test_the_grant_check_precedes_the_provisioner_availability_check(
        self, wire
    ):
        wire.state.grants_error = HTTPException(status_code=403, detail="no shell")
        wire.holder.provisioner.is_available = False

        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 403
        assert wire.events == ["internal", "grants"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "available,in_cluster", [(False, True), (True, False), (False, False)]
    )
    async def test_no_in_cluster_provisioner_is_a_503(
        self, wire, available, in_cluster
    ):
        wire.holder.provisioner.is_available = available
        wire.holder.provisioner.in_cluster = in_cluster

        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 503
        assert response.json()["detail"] == (
            "Workspace container provisioning not available (no in-cluster K8s)"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["pending", "creating", "created", "ready"])
    async def test_a_durable_sandbox_assignment_short_circuits(self, wire, status):
        wire.state.contract = _Contract(assigned="sandbox", requested="virtual")
        wire.state.job = dict(
            wire.state.job, context={"workspace_container": {"status": status}}
        )

        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 200
        assert response.json() == {
            "status": status,
            "job_id": JOB,
            "target_tier": "sandbox",
            "message": "Workspace container already provisioned or in progress",
        }
        wire.holder.store.begin_job_workspace_tier_transition.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_json_string_context_is_parsed_for_the_short_circuit(self, wire):
        wire.state.contract = _Contract(assigned="sandbox", requested="virtual")
        wire.state.job = dict(
            wire.state.job, context='{"workspace_container": {"status": "ready"}}'
        )

        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.json()["status"] == "ready"

    @pytest.mark.asyncio
    async def test_sandbox_without_a_generation_is_a_typed_409(self, wire):
        wire.state.contract = _Contract(assigned="sandbox", requested="virtual")

        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 409
        assert response.json()["detail"] == {
            "code": "workspace_runtime_unavailable",
            "message": (
                "The sandbox assignment exists without a current "
                "provisioning generation; use normal workspace recovery"
            ),
        }

    @pytest.mark.asyncio
    async def test_a_won_transition_schedules_the_provisioner(self, wire):
        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 200
        assert response.json() == {
            "status": "provisioning",
            "job_id": JOB,
            "target_tier": "sandbox",
        }
        transition = wire.holder.store.begin_job_workspace_tier_transition.await_args
        assert transition.args == (JOB,)
        assert transition.kwargs == {
            "expected_backend": "virtual",
            "target_backend": "sandbox",
            "requested_backend": "virtual",
            "assignment_source": "runtime_workspace_upgrade",
            "expected_status": "processing",
            "upgrade_config": {
                "sources": {
                    "tier": "upgrade",
                    "template": "builtin",
                    "template_name": None,
                }
            },
        }
        wire.holder.store.get_user.assert_awaited_once_with(USER["id"])
        await asyncio.sleep(0)
        wire.holder.provisioner.create_workspace.assert_awaited_once_with(("job", JOB))

    @pytest.mark.asyncio
    async def test_a_lost_race_that_already_landed_reports_acceptance(self, wire):
        wire.state.transitioned = False
        calls = {"n": 0}

        def get_job(_id):
            calls["n"] += 1
            if calls["n"] == 1:
                return dict(wire.state.job)
            wire.state.contract = _Contract(assigned="sandbox", requested="virtual")
            return dict(
                wire.state.job,
                context={"workspace_container": {"status": "creating"}},
            )

        wire.holder.store.get_job = AsyncMock(side_effect=get_job)

        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 200
        assert response.json() == {
            "status": "creating",
            "job_id": JOB,
            "target_tier": "sandbox",
            "message": "Workspace transition already accepted",
        }
        wire.holder.provisioner.create_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_lost_race_that_did_not_land_is_a_typed_409(self, wire):
        wire.state.transitioned = False

        response = await call(wire, "POST", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 409
        assert response.json()["detail"] == {
            "code": "workspace_contract_changed",
            "message": "Job workspace assignment changed before provisioning",
        }


# =============================================================================
# workspace-status
# =============================================================================


class TestJobWorkspaceStatus:
    PATH = f"/api/jobs/{JOB}/workspace-status"
    INTERNAL = {"x-internal-key": "secret"}

    @pytest.mark.asyncio
    async def test_the_internal_gate_precedes_the_read(self, wire):
        response = await call(wire, "GET", self.PATH, headers={})

        assert response.status_code == 401
        assert wire.events == ["internal"]
        wire.holder.store.get_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_missing_job_is_a_404(self, wire):
        wire.holder.store.get_job = AsyncMock(return_value=None)

        response = await call(wire, "GET", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 404
        assert response.json()["detail"] == "Job not found"

    @pytest.mark.asyncio
    async def test_a_non_sandbox_assignment_is_a_typed_409(self, wire):
        response = await call(wire, "GET", self.PATH, headers=self.INTERNAL)

        assert response.status_code == 409
        assert response.json()["detail"] == {
            "code": "workspace_backend_conflict",
            "message": (
                "Sandbox workspace status is unavailable for this job's "
                "assigned workspace tier"
            ),
            "assigned_backend": "virtual",
        }

    @pytest.mark.asyncio
    async def test_the_payload_maps_port_to_pod_port(self, wire, monkeypatch):
        monkeypatch.setenv("SSH_KEY_PATH", "/keys/id")
        wire.state.contract = _Contract(assigned="sandbox", requested="virtual")
        wire.state.job = dict(
            wire.state.job,
            context={
                "workspace_container": {
                    "status": "ready",
                    "host": "10.42.0.9",
                    "port": 2222,
                    "pod_name": "srw-ws-1",
                    "namespace": "srw",
                    "git_remote_url": "http://gitea/srw/job.git",
                }
            },
        )

        response = await call(wire, "GET", self.PATH, headers=self.INTERNAL)

        assert response.json() == {
            "status": "ready",
            "pod_ip": "10.42.0.9",
            "pod_port": 2222,
            "pod_name": "srw-ws-1",
            "namespace": "srw",
            "ssh_key_path": "/keys/id",
            "git_remote_url": "http://gitea/srw/job.git",
        }

    @pytest.mark.asyncio
    async def test_an_absent_container_reports_none(self, wire, monkeypatch):
        monkeypatch.delenv("SSH_KEY_PATH", raising=False)
        wire.state.contract = _Contract(assigned="sandbox", requested="virtual")

        response = await call(wire, "GET", self.PATH, headers=self.INTERNAL)

        assert response.json() == {
            "status": "none",
            "pod_ip": None,
            "pod_port": None,
            "pod_name": None,
            "namespace": None,
            "ssh_key_path": None,
            "git_remote_url": None,
        }


# =============================================================================
# Dependency resolution
# =============================================================================


class TestDependencyResolution:
    @pytest.mark.asyncio
    async def test_each_request_rebuilds_and_observes_a_rebound_singleton(self, wire):
        await call(wire, "GET", f"/api/jobs/{JOB}/snapshot")
        first = wire.state.builds

        wire.holder.snapshots = SimpleNamespace(
            get_snapshot_status=AsyncMock(return_value={"status": "swapped"})
        )

        response = await call(wire, "GET", f"/api/jobs/{JOB}/snapshot")

        assert wire.state.builds > first
        assert response.json() == {"status": "swapped"}

    def test_the_provider_reads_the_per_app_factory(self, wire):
        first = get_workspace_access_dependencies(SimpleNamespace(app=wire.app))
        second = get_workspace_access_dependencies(SimpleNamespace(app=wire.app))

        assert first is not second
        assert first.store is second.store
