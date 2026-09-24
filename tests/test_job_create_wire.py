"""HTTP contract of the existing public/internal job creation funnel.

Only external identity/storage/provisioning collaborators are controlled. The
actual routes, Pydantic ingress, scope resolution and response redaction run.
No application startup, dispatch, provider or database connection is started.
"""

from tests._expert_catalog import catalogue_state, patch_service_method
from orchestrator.services import expert_catalog as expert_catalog_module


import copy
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from orchestrator import main
from orchestrator.routers import job_lifecycle as job_lifecycle_routes
from orchestrator.services.default_experts import ExpertSelection
from orchestrator import uploads as uploads_module
from orchestrator.application import access as access_composition
from orchestrator.application import jobs as jobs_composition
from orchestrator.routers import project_jobs as project_jobs_module
from orchestrator.security import access as access_module
from orchestrator.security import auth as auth_module
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import default_experts as default_experts_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import grant_enforcement as grant_enforcement_module
from orchestrator.services import job_dispatcher as job_dispatcher_module
from orchestrator.services import job_workspace_runtime as job_workspace_runtime_module
from orchestrator.services import subjob_completion as subjob_completion_module
from orchestrator.services import (
    thread_datasource_authorization as thread_datasource_authorization_module,
)
from orchestrator.services import thread_mount_rows as thread_mount_rows_module
from orchestrator.services import (
    thread_project_authorization as thread_project_authorization_module,
)
from orchestrator.services import vm_workspace_policy as vm_workspace_policy_module
import functools


USER = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
JOB = "33333333-3333-4333-8333-333333333333"
EXPERT = "44444444-4444-4444-8444-444444444444"
PARENT = "55555555-5555-4555-8555-555555555555"
CONNECTOR = "66666666-6666-4666-8666-666666666666"
STAMP = datetime(2026, 9, 6, 8, 0, tzinfo=timezone.utc)
THREAD = "77777777-7777-4777-8777-777777777777"
PATH = "/api/jobs"
PROJECT_PATH = f"/api/projects/{PROJECT}/jobs"


@pytest.fixture
def wire(monkeypatch):
    user = {"id": USER, "is_admin": False, "is_approved": True}
    # The real owner operation: the composition binds its dependencies per call.
    enforce_grants = grant_enforcement_module.enforce_job_create_grants

    async def approved(request, _db):
        if not request.headers.get("x-test-user"):
            raise HTTPException(401, "Authentication required")
        return user

    async def insert(**kwargs):
        return {
            "id": UUID(JOB),
            "description": kwargs["description"],
            "config_name": kwargs["config_name"],
            "status": "created",
            "created_at": STAMP,
            "assigned_agent_id": None,
            "user_id": UUID(kwargs["user_id"]),
            "project_id": UUID(kwargs["project_id"]),
            "context": copy.deepcopy(kwargs["context"]),
            "config_override": copy.deepcopy(kwargs["config_override"]),
            "workspace_contract": {"state": "unassigned"},
            # A real create returns its row, not a filtered ID acknowledgement.
            "existing_extension": {"nullable": None},
        }

    db = SimpleNamespace(
        create_job=AsyncMock(side_effect=insert),
        get_user=AsyncMock(return_value=user),
        get_project=AsyncMock(return_value={"id": PROJECT}),
        get_user_role_in_project=AsyncMock(return_value="editor"),
        get_job=AsyncMock(
            side_effect=lambda job_id: (
                {"id": PARENT, "user_id": USER, "project_id": PROJECT}
                if str(job_id) == PARENT
                else None
            )
        ),
        resolve_datasources_for_thread=AsyncMock(return_value=[]),
    )
    defaults = AsyncMock(return_value=([CONNECTOR], {}))
    authorize = AsyncMock(side_effect=lambda _actor, ids, **kw: (list(ids), {}))
    expert = AsyncMock(
        return_value=ExpertSelection(
            expert={"id": EXPERT, "expert_type": "worker", "owner_id": None},
            source="application",
        )
    )
    provision, dispatch = AsyncMock(), Mock()
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(auth_module, "require_approved_user", approved)
    monkeypatch.setattr(access_module, "require_project_member", AsyncMock())
    monkeypatch.setattr(
        access_module,
        "is_internal_call",
        lambda request: bool(request.headers.get("x-test-internal")),
    )
    monkeypatch.setattr(access_composition, "enforce_readiness_gate", AsyncMock())
    monkeypatch.setattr(deployment_gates_module, "is_experts_db_enabled", lambda: True)
    monkeypatch.setattr(
        grant_enforcement_module, "user_experts_enabled", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(default_experts_module, "resolve_root_expert", expert)
    monkeypatch.setattr(catalogue_state(), "experts", [SimpleNamespace(id="developer")])
    monkeypatch.setattr(
        thread_datasource_authorization_module,
        "authorize_thread_datasource_selection",
        authorize,
    )
    monkeypatch.setattr(
        deployment_gates_module, "datasource_defaults_on_omission", lambda: False
    )
    monkeypatch.setattr(
        grant_enforcement_module, "enforce_job_create_grants", AsyncMock()
    )
    monkeypatch.setattr(
        main.app.state.resources.settings, "stateless_worker_default_enabled", False
    )
    scholar = AsyncMock(return_value=None)
    monkeypatch.setattr(
        subjob_completion_module,
        "spawn_scholar_subjob",
        scholar,
    )
    monkeypatch.setattr(job_dispatcher_module, "trigger_dispatch", dispatch)
    monkeypatch.setattr(
        "orchestrator.services.datasource_policy.default_datasource_selection", defaults
    )
    monkeypatch.setattr(
        "orchestrator.services.job_provisioning.provision_job_repo", provision
    )
    app = FastAPI()
    app.state.job_lifecycle_route_dependencies_factory = functools.partial(
        jobs_composition.job_lifecycle_route_dependencies, main.app.state.resources
    )
    app.add_api_route(PATH, job_lifecycle_routes.create_job, methods=["POST"])
    app.add_api_route(
        "/api/projects/{project_id}/jobs",
        project_jobs_module.create_project_job,
        methods=["POST"],
    )
    return SimpleNamespace(
        app=app,
        db=db,
        defaults=defaults,
        authorize=authorize,
        expert=expert,
        provision=provision,
        scholar=scholar,
        dispatch=dispatch,
        enforce_grants=enforce_grants,
    )


async def submit(wire, payload, path=PATH, **headers):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wire.app), base_url="http://create.test"
    ) as client:
        return await client.post(
            path, json=payload, headers={"x-test-user": USER, **headers}
        )


def body(**fields):
    return {"description": "controlled HTTP fixture", "project_id": PROJECT, **fields}


@pytest.fixture
def workspace_wire(wire, monkeypatch):
    """Exercise real workspace/lane and VM policy with controlled capabilities."""
    monkeypatch.setattr(
        main.app.state.resources.settings, "stateless_worker_default_enabled", True
    )
    monkeypatch.setattr(
        main.app.state.resources.settings, "stateless_worker_enabled", True
    )
    monkeypatch.setattr(
        container_provisioner_module,
        "container_provisioner",
        SimpleNamespace(is_available=True, in_cluster=True),
    )
    monkeypatch.setattr(access_module, "vm_workspaces_on_pod_network", lambda: False)
    wire.db.get_system_setting = AsyncMock(return_value=None)
    wire.db.user_can_use_vm = AsyncMock(return_value=True)
    return wire


def assert_no_workspace_admission_effects(wire):
    wire.authorize.assert_not_awaited()
    wire.defaults.assert_not_awaited()
    wire.db.create_job.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


def test_workspace_dependency_factory_binds_without_reads_and_defers_flags(monkeypatch):
    class UnreadProvisioner:
        @property
        def is_available(self):
            pytest.fail("dependency construction must not read availability")

        @property
        def in_cluster(self):
            pytest.fail("dependency construction must not read cluster capabilities")

    first_store = SimpleNamespace(get_user=AsyncMock())
    second_store = SimpleNamespace(get_user=AsyncMock())
    first_provisioner, second_provisioner = UnreadProvisioner(), UnreadProvisioner()
    callbacks = {
        "needs_vm": ((job_workspace_runtime_module, "job_needs_vm"), Mock()),
        "needs_sandbox": ((job_workspace_runtime_module, "job_needs_sandbox"), Mock()),
        "check_vm_permission": (
            (vm_workspace_policy_module, "check_vm_permission"),
            AsyncMock(),
        ),
        "resolve_execution_lane": (
            (job_workspace_runtime_module, "resolve_requested_job_execution_lane"),
            Mock(),
        ),
        "vm_workspaces_on_pod_network": (
            (access_module, "vm_workspaces_on_pod_network"),
            Mock(),
        ),
        "enforce_grants": (
            (grant_enforcement_module, "enforce_job_create_grants"),
            AsyncMock(),
        ),
    }
    for (owner, attribute), callback in callbacks.values():
        monkeypatch.setattr(owner, attribute, callback)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", first_store)
    monkeypatch.setattr(
        container_provisioner_module, "container_provisioner", first_provisioner
    )
    monkeypatch.setattr(
        main.app.state.resources.settings, "stateless_worker_default_enabled", False
    )
    monkeypatch.setattr(
        main.app.state.resources.settings, "stateless_worker_enabled", False
    )
    first = jobs_composition.job_admission_workspace_dependencies(
        main.app.state.resources
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", second_store)
    monkeypatch.setattr(
        container_provisioner_module, "container_provisioner", second_provisioner
    )
    second = jobs_composition.job_admission_workspace_dependencies(
        main.app.state.resources
    )
    assert first.store is first_store and second.store is second_store
    assert first.provisioner is first_provisioner
    assert second.provisioner is second_provisioner
    first_store.get_user.assert_not_awaited()
    second_store.get_user.assert_not_awaited()
    for field, (_, callback) in callbacks.items():
        # Bound operations (``resources.bound``) wrap the owner and pass its
        # dependencies per call; the plain ones are the owner itself.
        bound_operation = getattr(first, field)
        assert getattr(bound_operation, "__wrapped__", bound_operation) is callback
        callback.assert_not_called()
    assert first.stateless_default_enabled() is False
    assert first.stateless_enabled() is False
    monkeypatch.setattr(
        main.app.state.resources.settings, "stateless_worker_default_enabled", True
    )
    monkeypatch.setattr(
        main.app.state.resources.settings, "stateless_worker_enabled", True
    )
    assert first.stateless_default_enabled() is True
    assert first.stateless_enabled() is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,detail",
    [
        ("creator_missing", "User is not permitted to use VM workspaces"),
        ("creator_read", "User is not permitted to use VM workspaces"),
        ("grant", "User is not permitted to use VM workspaces"),
        (
            "global",
            "VM workspaces are globally disabled by the administrator",
        ),
    ],
)
async def test_vm_refusal_precedes_lane_grants_and_later_effects(
    workspace_wire, monkeypatch, failure, detail
):
    wire = workspace_wire
    if failure == "creator_missing":
        wire.db.get_user.return_value = None
    elif failure == "creator_read":
        wire.db.get_user.side_effect = RuntimeError("unavailable")
    elif failure == "grant":
        wire.db.user_can_use_vm.return_value = False
    else:
        wire.db.get_system_setting.return_value = {"value": {"enabled": False}}
    sandbox = Mock(wraps=job_workspace_runtime_module.job_needs_sandbox)
    lane = Mock(wraps=job_workspace_runtime_module.resolve_requested_job_execution_lane)
    monkeypatch.setattr(job_workspace_runtime_module, "job_needs_sandbox", sandbox)
    monkeypatch.setattr(
        job_workspace_runtime_module, "resolve_requested_job_execution_lane", lane
    )
    response = await submit(
        wire,
        body(
            config_override={"workspace": {"backend": "vm"}},
            execution_lane="stateless",
        ),
    )
    assert response.status_code == 403, response.text
    assert response.json() == {"detail": detail}
    wire.db.get_user.assert_awaited_once_with(USER)
    sandbox.assert_not_called()
    lane.assert_not_called()
    grant_enforcement_module.enforce_job_create_grants.assert_not_awaited()
    assert_no_workspace_admission_effects(wire)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "child,requested,default_enabled,expected,logged_default",
    [
        (False, None, True, "stateless", True),
        (True, None, True, None, False),
        (False, None, False, None, False),
        (False, "pinned", True, "pinned", False),
    ],
)
async def test_create_lane_default_keeps_omission_and_parent_authority(
    workspace_wire,
    monkeypatch,
    caplog,
    child,
    requested,
    default_enabled,
    expected,
    logged_default,
):
    wire = workspace_wire
    monkeypatch.setattr(
        main.app.state.resources.settings,
        "stateless_worker_default_enabled",
        default_enabled,
    )
    caplog.set_level(logging.DEBUG)
    fields = {"config_override": {"workspace": {"backend": "sandbox"}}}
    if requested is not None:
        fields["execution_lane"] = requested
    if child:
        fields.update(parent_job_id=PARENT, datasource_ids=[])
    response = await submit(
        wire, body(**fields), **({"x-test-internal": "1"} if child else {})
    )
    assert response.status_code == 200, response.text
    assert wire.db.create_job.await_args.kwargs["execution_lane"] == expected
    assert (
        "Job create: worker execution lane defaulted to stateless for a capable root job"
        in caplog.messages
    ) is logged_default
    assert not any("default fell back" in message for message in caplog.messages)
    wire.db.user_can_use_vm.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,backend,expected,reason",
    [
        (
            "disabled",
            "sandbox",
            None,
            "stateless worker admission is disabled",
        ),
        (
            "unavailable",
            "sandbox",
            None,
            "the Kubernetes workspace provisioner is unavailable",
        ),
        (
            "outside_cluster",
            "sandbox",
            None,
            "the workspace provisioner is not in-cluster",
        ),
        (
            "no_sandbox",
            "none",
            None,
            "the job does not require a Kubernetes sandbox",
        ),
        (
            "external_vm",
            "vm",
            "pinned",
            "external VM jobs require pinned workers",
        ),
    ],
)
async def test_omitted_root_lane_keeps_fallback_diagnostic_and_insert_value(
    workspace_wire, monkeypatch, caplog, failure, backend, expected, reason
):
    wire = workspace_wire
    if failure == "disabled":
        monkeypatch.setattr(
            main.app.state.resources.settings, "stateless_worker_enabled", False
        )
    elif failure == "unavailable":
        container_provisioner_module.container_provisioner.is_available = False
    elif failure == "outside_cluster":
        container_provisioner_module.container_provisioner.in_cluster = False
    caplog.set_level(logging.DEBUG)
    response = await submit(
        wire, body(config_override={"workspace": {"backend": backend}})
    )
    assert response.status_code == 200, response.text
    assert wire.db.create_job.await_args.kwargs["execution_lane"] == expected
    assert (
        caplog.messages.count(
            f"Job create: stateless worker lane default fell back to pinned ({reason})"
        )
        == 1
    )
    grant_enforcement_module.enforce_job_create_grants.assert_awaited_once()


@pytest.mark.asyncio
async def test_fallback_diagnostic_rereads_provisioner_after_lane_resolution(
    workspace_wire, monkeypatch, caplog
):
    wire = workspace_wire

    class RecoveringProvisioner:
        is_available = True

        def __init__(self):
            self.cluster_reads = 0

        @property
        def in_cluster(self):
            self.cluster_reads += 1
            return self.cluster_reads > 1

    provisioner = RecoveringProvisioner()
    monkeypatch.setattr(
        container_provisioner_module, "container_provisioner", provisioner
    )
    caplog.set_level(logging.DEBUG)
    response = await submit(
        wire, body(config_override={"workspace": {"backend": "sandbox"}})
    )
    assert response.status_code == 200, response.text
    assert wire.db.create_job.await_args.kwargs["execution_lane"] is None
    assert provisioner.cluster_reads == 2
    assert (
        "Job create: stateless worker lane default fell back to pinned "
        "(the worker capability check declined stateless)"
    ) in caplog.messages


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "enabled,available,backend,status,detail",
    [
        (False, True, "sandbox", 409, "Stateless worker admission is disabled"),
        (
            True,
            False,
            "sandbox",
            503,
            "Stateless workers require an in-cluster Kubernetes workspace provisioner",
        ),
        (
            True,
            True,
            "none",
            422,
            "Stateless workers currently require a Kubernetes sandbox or same-cluster VM workspace",
        ),
    ],
)
async def test_explicit_stateless_refusals_keep_exact_http_and_no_later_effects(
    workspace_wire, monkeypatch, enabled, available, backend, status, detail
):
    wire = workspace_wire
    monkeypatch.setattr(
        main.app.state.resources.settings, "stateless_worker_enabled", enabled
    )
    container_provisioner_module.container_provisioner.is_available = available
    response = await submit(
        wire,
        body(
            execution_lane="stateless",
            config_override={"workspace": {"backend": backend}},
        ),
    )
    assert response.status_code == status, response.text
    assert response.json() == {"detail": detail}
    grant_enforcement_module.enforce_job_create_grants.assert_not_awaited()
    assert_no_workspace_admission_effects(wire)


@pytest.mark.asyncio
async def test_external_vm_lane_override_keeps_permission_and_merged_grant_order(
    workspace_wire, monkeypatch, caplog
):
    wire = workspace_wire
    wire.db.get_project.return_value["default_config_override"] = {
        "workspace": {"backend": "vm"},
        "autonomy": "review",
    }
    caplog.set_level(logging.DEBUG)
    order = Mock()
    permission = AsyncMock(wraps=vm_workspace_policy_module.check_vm_permission)
    sandbox = Mock(wraps=job_workspace_runtime_module.job_needs_sandbox)
    lane = Mock(wraps=job_workspace_runtime_module.resolve_requested_job_execution_lane)
    monkeypatch.setattr(vm_workspace_policy_module, "check_vm_permission", permission)
    monkeypatch.setattr(job_workspace_runtime_module, "job_needs_sandbox", sandbox)
    monkeypatch.setattr(
        job_workspace_runtime_module, "resolve_requested_job_execution_lane", lane
    )
    for name, collaborator in (
        ("creator", wire.db.get_user),
        ("permission", permission),
        ("sandbox", sandbox),
        ("lane", lane),
        (
            "grants",
            grant_enforcement_module.enforce_job_create_grants,
        ),
        ("datasources", wire.authorize),
        ("insert", wire.db.create_job),
        ("provision", wire.provision),
    ):
        order.attach_mock(collaborator, name)
    response = await submit(
        wire,
        body(
            execution_lane="stateless",
            config_override={"autonomy": "partial"},
            datasource_ids=[],
        ),
    )
    assert response.status_code == 200, response.text
    assert [call[0] for call in order.mock_calls] == [
        "creator",
        "permission",
        "sandbox",
        "lane",
        "grants",
        "datasources",
        "insert",
        "provision",
    ]
    # Both owners are bound by the composition, which passes ``dependencies=``.
    permission.assert_awaited_once_with(
        wire.db.get_user.return_value, job_needs_vm=True, dependencies=ANY
    )
    grant_enforcement_module.enforce_job_create_grants.assert_awaited_once_with(
        {"workspace": {"backend": "vm"}, "autonomy": "partial"},
        user_id=USER,
        project_ids=[PROJECT],
        dependencies=ANY,
    )
    assert wire.db.create_job.await_args.kwargs["execution_lane"] == "pinned"
    assert (
        "Job create: external VM request keeps job on pinned lane "
        "(stateless worker opt-in ignored)"
    ) in caplog.messages


@pytest.mark.asyncio
async def test_real_merged_capability_denial_is_refused_before_datasources_or_insert(
    workspace_wire, monkeypatch
):
    wire = workspace_wire
    monkeypatch.setattr(
        grant_enforcement_module, "enforce_job_create_grants", wire.enforce_grants
    )
    wire.db.get_project.return_value["default_config_override"] = {"autonomy": "full"}
    wire.db.list_grants_for_scopes = AsyncMock(
        return_value={"user": [], "project": [], "global": []}
    )
    response = await submit(
        wire,
        body(
            config_override={"workspace": {"backend": "none"}},
            datasource_ids=[CONNECTOR],
        ),
    )
    assert response.status_code == 422, response.text
    assert response.json() == {
        "detail": "config exceeds your capability grants: autonomy_ceiling: autonomy 'full' exceeds the ceiling"
    }
    assert_no_workspace_admission_effects(wire)


@pytest.fixture
def officer_wire(wire, monkeypatch):
    """Run real Officer snapshot/slot policy; control reads and the final write."""
    metadata = {"config_override": {"officer": {"enabled": True}}}
    thread = {
        "id": THREAD,
        "user_id": USER,
        "project_id": PROJECT,
        "status": "active",
        "metadata": metadata,
    }
    wire.db.get_thread = AsyncMock(return_value=thread)
    wire.db.get_project_officer_lineage = AsyncMock(return_value=[THREAD])
    snapshot = {
        "project_id": PROJECT,
        "thread_id": THREAD,
        "config_override": {},
        "incarnations": [],
        "post_updated_at": STAMP,
        "current_thread_id": THREAD,
        "thread_project_id": PROJECT,
        "thread_status": "active",
        "thread_metadata": metadata,
        "thread_user_id": USER,
        "thread_created_at": STAMP,
    }
    read_snapshot = AsyncMock(return_value=snapshot)

    @asynccontextmanager
    async def acquire():
        # No transaction or write methods: preparation must only read.
        yield SimpleNamespace(fetchrow=read_snapshot)

    wire.db.acquire = acquire
    monkeypatch.setattr(
        thread_mount_rows_module,
        "thread_project_ids",
        AsyncMock(return_value=[PROJECT]),
    )
    monkeypatch.setattr(
        thread_project_authorization_module,
        "revalidate_thread_project_ids",
        AsyncMock(return_value=[PROJECT]),
    )
    ticket = AsyncMock(
        return_value={
            "project_id": PROJECT,
            "status": "active",
            "note_type": "feature",
            "tags": ["ready", "category:researcher"],
            "ready_at": STAMP,
        }
    )
    monkeypatch.setattr(
        "orchestrator.services.project_backlog.fetch_ticket_state", ticket
    )

    async def insert(_db, *, job_kwargs, **kwargs):
        return await wire.db.create_job(**job_kwargs)

    admit = AsyncMock(side_effect=insert)
    monkeypatch.setattr(
        "orchestrator.services.officer_admission.admit_and_create_job", admit
    )
    preflight = AsyncMock(
        return_value=SimpleNamespace(
            activated=True, state="ready", retryable=False, phase="ready", error=None
        )
    )
    monkeypatch.setattr(
        "orchestrator.services.officer_preflight.ensure_officer_job_activated",
        preflight,
    )
    wire.officer = SimpleNamespace(
        thread=thread,
        snapshot=snapshot,
        read_snapshot=read_snapshot,
        ticket=ticket,
        admit=admit,
        preflight=preflight,
    )
    return wire


def assert_no_admission_effects(wire):
    wire.authorize.assert_not_awaited()
    grant_enforcement_module.enforce_job_create_grants.assert_not_awaited()
    wire.db.create_job.assert_not_awaited()
    wire.officer.admit.assert_not_awaited()
    wire.officer.preflight.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_officer_dependency_factory_captures_stores_without_reading_them(
    wire, monkeypatch
):
    from orchestrator.services import officer_admission, project_backlog

    snapshot = AsyncMock()
    ticket = AsyncMock()
    monkeypatch.setattr(officer_admission, "prepare_officer_admission", snapshot)
    monkeypatch.setattr(project_backlog, "fetch_ticket_state", ticket)
    first_store, first_vector = object(), object()
    second_store, second_vector = object(), object()
    monkeypatch.setattr(main.app.state.resources, "postgres_db", first_store)
    monkeypatch.setattr(main.app.state.resources, "vector_db", first_vector)
    first = jobs_composition.job_admission_officer_dependencies(
        main.app.state.resources
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", second_store)
    monkeypatch.setattr(main.app.state.resources, "vector_db", second_vector)
    second = jobs_composition.job_admission_officer_dependencies(
        main.app.state.resources
    )
    snapshot.assert_not_called()
    ticket.assert_not_called()
    assert first.store is first_store and second.store is second_store
    kwargs = dict(
        project_id=PROJECT,
        thread_id=THREAD,
        requested_slot=None,
        requested_config_override=None,
    )
    await first.prepare_officer(**kwargs)
    snapshot.assert_awaited_once_with(first_store, **kwargs)
    await first.fetch_ticket(PROJECT, "first")
    ticket.assert_awaited_once_with(first_vector, PROJECT, "first")
    await second.prepare_officer(**kwargs)
    assert snapshot.await_args.args == (second_store,)
    await second.fetch_ticket(PROJECT, "second")
    assert ticket.await_args.args == (second_vector, PROJECT, "second")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,code",
    [
        ("retired", "officer_disabled"),
        ("orphan", "post_missing"),
        ("duplicate", "stale_incarnation"),
        ("held", "officer_held"),
        ("wrong_project", "project_mismatch"),
    ],
)
async def test_officer_candidate_refusals_precede_ticket_and_all_effects(
    officer_wire, failure, code
):
    wire = officer_wire
    meta = wire.officer.thread["metadata"]["config_override"]["officer"]
    if failure == "retired":
        meta["enabled"] = False
    elif failure == "orphan":
        wire.db.get_project_officer_lineage.return_value = []
        wire.officer.read_snapshot.return_value = None
    elif failure == "duplicate":
        wire.db.get_project_officer_lineage.return_value = []
        wire.officer.snapshot["thread_id"] = PARENT
    elif failure == "held":
        meta["hold"] = {"reason": "conference"}
    else:
        wire.officer.snapshot["thread_project_id"] = PARENT
    response = await submit(
        wire, body(thread_id=THREAD, ticket="fixture"), **{"x-test-internal": "1"}
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == code
    wire.officer.ticket.assert_not_awaited()
    assert_no_admission_effects(wire)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["thread", "lineage", "both", "neither"])
async def test_ordinary_session_candidate_lookup_failures_keep_existing_behavior(
    officer_wire, failure
):
    wire = officer_wire
    thread = wire.officer.thread
    thread["metadata"]["config_override"]["officer"]["enabled"] = False
    # The first thread read is scope authorization; only the later Officer
    # candidate read has historical best-effort behavior.
    wire.db.get_thread.side_effect = [
        thread,
        RuntimeError("unavailable") if failure in {"thread", "both"} else thread,
        thread,  # the later datasource-inheritance read
    ]
    wire.db.get_project_officer_lineage.return_value = []
    if failure in {"lineage", "both"}:
        wire.db.get_project_officer_lineage.side_effect = RuntimeError("unavailable")
    response = await submit(wire, body(thread_id=THREAD), **{"x-test-internal": "1"})
    assert response.status_code == 200, response.text
    wire.officer.read_snapshot.assert_not_awaited()
    wire.officer.ticket.assert_not_awaited()
    wire.officer.admit.assert_not_awaited()
    wire.db.create_job.assert_awaited_once()
    wire.provision.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("lookup", ["thread", "lineage"])
async def test_either_candidate_signal_still_requires_authoritative_snapshot(
    officer_wire, lookup
):
    wire = officer_wire
    if lookup == "thread":
        wire.db.get_thread.side_effect = [
            wire.officer.thread,
            RuntimeError("unavailable"),
        ]
    else:
        wire.db.get_project_officer_lineage.side_effect = RuntimeError("unavailable")
    wire.officer.read_snapshot.return_value = None
    response = await submit(wire, body(thread_id=THREAD), **{"x-test-internal": "1"})
    assert response.status_code == 409, response.text
    assert response.json() == {
        "detail": {"code": "post_missing", "message": "Officer Post does not exist."}
    }
    wire.officer.read_snapshot.assert_awaited_once()
    assert_no_admission_effects(wire)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ticket_change,status,detail",
    [
        (
            "unavailable",
            503,
            "Backlog ticket authority is unavailable; no claim or job was created.",
        ),
        (
            None,
            409,
            "Backlog ticket 'fixture' does not exist in this Officer Post's project.",
        ),
        ({"project_id": PARENT}, 409, "Backlog ticket belongs to a different project."),
        (
            {"status": "archived"},
            409,
            "Backlog ticket 'fixture' is not an active backlog ticket.",
        ),
        (
            {"note_type": "reference"},
            409,
            "Backlog ticket 'fixture' is not an active backlog ticket.",
        ),
        (
            {"tags": ["category:researcher"]},
            409,
            "Backlog ticket 'fixture' is not ready with trusted Officer provenance.",
        ),
        (
            {"ready_at": STAMP.isoformat()},
            409,
            "Backlog ticket 'fixture' is not ready with trusted Officer provenance.",
        ),
    ],
)
async def test_officer_ticket_error_json_and_no_effects(
    officer_wire, ticket_change, status, detail
):
    wire = officer_wire
    if ticket_change == "unavailable":
        wire.officer.ticket.side_effect = RuntimeError("unavailable")
    elif ticket_change is None:
        wire.officer.ticket.return_value = None
    else:
        wire.officer.ticket.return_value.update(ticket_change)
    response = await submit(
        wire, body(thread_id=THREAD, ticket="fixture"), **{"x-test-internal": "1"}
    )
    assert response.status_code == status, response.text
    assert response.json() == {"detail": detail}
    assert_no_admission_effects(wire)


@pytest.mark.asyncio
async def test_non_officer_ticket_refused_without_vector_or_write(officer_wire):
    wire = officer_wire
    wire.officer.thread["metadata"]["config_override"]["officer"]["enabled"] = False
    wire.db.get_project_officer_lineage.return_value = []
    response = await submit(
        wire, body(thread_id=THREAD, ticket="fixture"), **{"x-test-internal": "1"}
    )
    assert response.status_code == 409, response.text
    assert response.json() == {
        "detail": "Backlog ticket claims require the exact current commissioned Officer Post incarnation. Ad-hoc jobs must omit ticket."
    }
    wire.officer.ticket.assert_not_awaited()
    assert_no_admission_effects(wire)


@pytest.mark.asyncio
@pytest.mark.parametrize("naive", [False, True])
async def test_officer_slot_ticket_preparation_reaches_final_admission_in_order(
    officer_wire, monkeypatch, naive
):
    wire = officer_wire
    meta = wire.officer.thread["metadata"]["config_override"]["officer"]
    meta["slots"] = {
        "researchers": {
            "count": 1,
            "model": "fixture-model",
            "backend": "none",
            "category": "researcher",
        }
    }
    wire.officer.ticket.return_value["ready_at"] = (
        STAMP.replace(tzinfo=None) if naive else STAMP
    )
    order = Mock()
    vm_check = Mock(wraps=job_workspace_runtime_module.job_needs_vm)
    monkeypatch.setattr(job_workspace_runtime_module, "job_needs_vm", vm_check)
    for name, collaborator in (
        ("snapshot", wire.officer.read_snapshot),
        ("ticket", wire.officer.ticket),
        ("vm", vm_check),
        ("datasources", wire.authorize),
        (
            "grants",
            grant_enforcement_module.enforce_job_create_grants,
        ),
        ("admit", wire.officer.admit),
        ("preflight", wire.officer.preflight),
    ):
        order.attach_mock(collaborator, name)
    response = await submit(
        wire,
        body(
            thread_id=THREAD,
            ticket="fixture",
            work_category="executor",
            context={
                "kickoff_message": "Existing brief",
                "instructions": "Keep instructions",
            },
        ),
        **{"x-test-internal": "1"},
    )
    assert response.status_code == 200, response.text
    assert [call[0] for call in order.mock_calls] == [
        "snapshot",
        "ticket",
        "vm",
        "grants",
        "datasources",
        "admit",
        "preflight",
    ]
    args = wire.officer.admit.await_args.kwargs
    assert args["ticket_ready_at"] == STAMP
    assert (
        args["ticket_claim_source"] == "manual" and args["strict_provisioning"] is True
    )
    assert args["preparation"].thread_id == THREAD
    assert args["preparation"].slot_name == "researchers"
    assert args["job_kwargs"]["config_override"]["llm"]["model"] == "fixture-model"
    context = args["job_kwargs"]["context"]
    assert (
        context["officer_slot"] == "researchers"
        and context["ticket_note_id"] == "fixture"
    )
    assert context["work_category"] == "researcher"
    assert context["instructions"] == "Keep instructions"
    assert context["kickoff_message"].startswith("Your deliverable is an ANSWER")
    assert (
        "dispatched this as executor work into the researchers slot"
        in context["kickoff_message"]
    )
    assert context["kickoff_message"].endswith("Existing brief")
    wire.officer.ticket.assert_awaited_once_with(
        main.app.state.resources.vector_db, PROJECT, "fixture"
    )
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [PATH, PROJECT_PATH])
async def test_actual_create_json_keeps_row_nulls_extensions_and_serialization(
    wire, path
):
    response = await submit(wire, body(), path)
    assert response.status_code == 200, response.text
    assert response.json() == {
        "id": JOB,
        "description": "controlled HTTP fixture",
        "config_name": "worker_base",
        "status": "created",
        "created_at": "2026-09-06T08:00:00Z",
        "assigned_agent_id": None,
        "user_id": USER,
        "project_id": PROJECT,
        "context": {"expert_selection": {"source": "application", "expert_id": EXPERT}},
        "config_override": None,
        "workspace_contract": {"state": "unassigned"},
        "existing_extension": {"nullable": None},
        "workspace_recovery": None,
        "vm_creation": None,
    }
    wire.db.create_job.assert_awaited_once()
    wire.provision.assert_awaited_once()
    wire.dispatch.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selection,gate,expected,origin",
    [
        ({}, False, [], "omitted_compat"),
        ({}, True, [CONNECTOR], "default"),
        ({"datasource_ids": []}, True, [], "explicit"),
        ({"datasource_ids": [CONNECTOR]}, False, [CONNECTOR], "explicit"),
        ({"use_datasource_defaults": True}, False, [CONNECTOR], "default"),
    ],
)
async def test_datasource_wire_presence_preserves_selection_intent(
    wire,
    monkeypatch,
    selection,
    gate,
    expected,
    origin,
):
    monkeypatch.setattr(
        deployment_gates_module, "datasource_defaults_on_omission", lambda: gate
    )
    response = await submit(wire, body(**selection))
    assert response.status_code == 200, response.text
    args = wire.db.create_job.await_args.kwargs
    assert args["datasource_ids"] == expected
    assert args["datasource_selection_provenance"]["origin"] == origin
    assert wire.defaults.await_count == (origin == "default")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fields",
    [
        {"datasource_ids": None},
        {"datasource_ids": [], "use_datasource_defaults": True},
        {"required_deliverables": ["../outside.txt"]},
        {"priority": 11},
    ],
)
async def test_validation_errors_are_json_arrays_and_never_insert(wire, fields):
    response = await submit(wire, body(**fields))
    assert response.status_code == 422, response.text
    assert isinstance(response.json()["detail"], list)
    assert response.json()["detail"][0]["loc"][0] == "body"
    wire.db.create_job.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selector,expected_config,expected_expert",
    [
        ({"expert": "developer"}, "developer", None),
        ({"expert": EXPERT}, "worker_base", EXPERT),
        ({"config_name": "developer"}, "developer", None),
        ({"expert_id": EXPERT}, "worker_base", EXPERT),
        ({"expert": "developer", "config_name": "developer"}, "developer", None),
    ],
)
async def test_expert_aliases_reach_the_same_create_command(
    wire, selector, expected_config, expected_expert
):
    response = await submit(wire, body(**selector))
    assert response.status_code == 200, response.text
    kwargs = wire.db.create_job.await_args.kwargs
    assert (kwargs["config_name"], kwargs["expert_id"]) == (
        expected_config,
        expected_expert,
    )


@pytest.mark.asyncio
async def test_conflicting_alias_is_a_string_error_before_any_insert(wire):
    response = await submit(wire, body(expert="developer", expert_id=EXPERT))
    assert response.status_code == 400
    assert isinstance(response.json()["detail"], str)
    wire.db.create_job.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [PATH, PROJECT_PATH])
async def test_public_identity_and_authority_injection_are_stripped_without_new_rejection(
    wire, path
):
    response = await submit(
        wire,
        body(
            user_id=PARENT,
            thread_id=PARENT,
            parent_job_id=PARENT,
            creation_order=2,
            worktree_path="foreign",
            delegation_context="foreign",
            builder_session_id=PARENT,
            context={
                "keep": "yes",
                "officer_admission": {"forged": True},
                "required_deliverables": ["forged"],
                "vm": {"host": "private"},
                "nested": [{"repository_credentials": "synthetic", "keep": 1}],
            },
            config_override={
                "lifecycle_marker": "forged",
                "extra": {"repository_auth": "synthetic"},
            },
        ),
        path,
    )
    assert response.status_code == 200, response.text
    kwargs = wire.db.create_job.await_args.kwargs
    assert kwargs["user_id"] == USER
    assert all(
        kwargs[key] is None
        for key in [
            "parent_job_id",
            "creation_order",
            "worktree_path",
            "delegation_context",
            "created_by_thread_id",
        ]
    )
    assert kwargs["context"] == {
        "keep": "yes",
        "nested": [{"keep": 1}],
        "expert_selection": {"source": "application", "expert_id": EXPERT},
    }
    assert kwargs["config_override"] == {"extra": {}}


@pytest.mark.asyncio
async def test_real_internal_scope_keeps_parent_and_rejects_forged_owner(wire):
    payload = body(parent_job_id=PARENT, creation_order=2, datasource_ids=[])
    response = await submit(wire, payload, **{"x-test-internal": "1"})
    assert response.status_code == 200, response.text
    kwargs = wire.db.create_job.await_args.kwargs
    assert kwargs["parent_job_id"] == PARENT and kwargs["creation_order"] == 2
    assert kwargs["user_id"] == USER and kwargs["project_id"] == PROJECT
    wire.db.create_job.reset_mock()
    response = await submit(
        wire, {**payload, "user_id": PARENT}, **{"x-test-internal": "1"}
    )
    assert response.status_code == 403
    wire.db.create_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_originless_internal_call_and_anonymous_public_call_do_not_write(wire):
    for headers in ({"x-test-internal": "1"}, {"x-test-user": ""}):
        response = await submit(wire, body(), **headers)
        assert response.status_code in (401, 403)
    wire.db.create_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_project_wrapper_overrides_body_project_after_editor_check(wire):
    response = await submit(wire, body(project_id=PARENT), PROJECT_PATH)
    assert response.status_code == 200, response.text
    assert wire.db.create_job.await_args.kwargs["project_id"] == PROJECT
    assert access_module.require_project_member.await_args_list[0].args[2] == PROJECT
    assert access_module.require_project_member.await_args_list[0].kwargs == {
        "min_role": "editor",
        "allow_archived": False,
    }


@pytest.mark.asyncio
async def test_project_denial_precedes_creation(wire):
    access_module.require_project_member.side_effect = HTTPException(
        403, "Project editor required"
    )
    response = await submit(wire, body(), PROJECT_PATH)
    assert response.status_code == 403
    wire.db.create_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_readiness_precedes_internal_authority_and_upload_checks(
    wire, monkeypatch
):
    access_composition.enforce_readiness_gate.side_effect = HTTPException(
        503, "Not ready"
    )
    upload = Mock()
    monkeypatch.setattr(uploads_module, "authorize_upload_reference", upload)
    response = await submit(
        wire,
        body(parent_job_id=PARENT, upload_id="unread"),
        **{"x-test-internal": "1"},
    )
    assert response.status_code == 503
    assert response.json() == {"detail": "Not ready"}
    wire.db.get_job.assert_not_awaited()
    wire.db.get_user.assert_not_awaited()
    upload.assert_not_called()
    wire.db.create_job.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_upload_refusal_precedes_project_expert_insert_and_provision(
    wire, monkeypatch
):
    upload = Mock(side_effect=HTTPException(403, "Upload belongs to another user"))
    monkeypatch.setattr(uploads_module, "authorize_upload_reference", upload)
    response = await submit(
        wire,
        body(parent_job_id=PARENT, upload_id="foreign"),
        **{"x-test-internal": "1"},
    )
    assert response.status_code == 403
    assert response.json() == {"detail": "Upload belongs to another user"}
    wire.db.get_job.assert_awaited_once_with(PARENT)
    wire.db.get_user.assert_awaited_once_with(USER)
    assert upload.call_args.args[0]["id"] == USER
    wire.db.get_user_role_in_project.assert_not_awaited()
    wire.db.get_project.assert_not_awaited()
    wire.expert.assert_not_awaited()
    wire.db.create_job.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal,status", [("role", 403), ("archived", 409)])
async def test_internal_project_refusal_never_reaches_expert_or_writes(
    wire, refusal, status
):
    if refusal == "role":
        wire.db.get_user_role_in_project.return_value = "viewer"
    else:
        wire.db.get_project.return_value = {"id": PROJECT, "status": "archived"}
    response = await submit(
        wire, body(parent_job_id=PARENT), **{"x-test-internal": "1"}
    )
    assert response.status_code == status, response.text
    wire.expert.assert_not_awaited()
    wire.authorize.assert_not_awaited()
    wire.db.create_job.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_catalogue_stays_application_owned_and_only_scans_for_explicit_slug(
    wire, monkeypatch
):
    scan = Mock(return_value=[SimpleNamespace(id="developer")])
    monkeypatch.setattr(catalogue_state(), "experts", None)
    patch_service_method(
        monkeypatch, expert_catalog_module.ExpertCatalogService, "scan_experts", scan
    )
    jobs_composition.job_admission_config_dependencies(main.app.state.resources)
    scan.assert_not_called()
    assert (
        await submit(wire, body(config_name="deployment/custom.yaml"))
    ).status_code == 200
    scan.assert_not_called()
    assert (await submit(wire, body(expert="developer"))).status_code == 200
    assert (await submit(wire, body(expert="developer"))).status_code == 200
    scan.assert_called_once_with()
    wire.provision.reset_mock()
    wire.db.create_job.reset_mock()
    response = await submit(wire, body(expert="absent"))
    assert response.status_code == 400 and "Unknown expert" in response.json()["detail"]
    scan.assert_called_once_with()
    wire.db.create_job.assert_not_awaited()
    wire.provision.assert_not_awaited()


@pytest.mark.asyncio
async def test_project_and_expert_overrides_merge_before_request_at_real_insert(wire):
    wire.db.get_project.return_value = {
        "id": PROJECT,
        "default_config_override": '{"llm":{"model":"project","temperature":0.2},"extra":{"keep":true,"remove":1}}',
    }
    wire.expert.return_value = ExpertSelection(
        expert={"id": EXPERT},
        source="project",
        project_override={"llm": {"model": "expert"}, "extra": {"remove": None}},
    )
    response = await submit(wire, body(config_override={"llm": {"model": "request"}}))
    assert response.status_code == 200, response.text
    args = wire.db.create_job.await_args.kwargs
    assert args["config_override"] == {
        "llm": {"model": "request", "temperature": 0.2},
        "extra": {"keep": True},
    }
    assert args["context"]["expert_selection"] == {
        "source": "project",
        "expert_id": EXPERT,
    }
    assert args["expert_id"] == EXPERT
    wire.provision.assert_awaited_once()


@pytest.mark.asyncio
async def test_bench_adapter_revalidates_creator_and_preserves_provenance(
    wire, monkeypatch
):
    from orchestrator.routers import bench
    from orchestrator.security import access

    # Application-owned submission still revalidates the persisted creator,
    # while admission no longer depends on an HTTP transport key or Request.
    monkeypatch.delenv("MCP_INTERNAL_KEY", raising=False)
    monkeypatch.setattr(access, "_INTERNAL_KEY", None)
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(side_effect=AssertionError("benchmark must not fabricate HTTP auth")),
    )
    run = {"id": JOB, "created_by": USER, "spec": {"project_id": PROJECT}}
    task = {"id": "scope-test", "description": "bench admission"}
    arm = {"name": "baseline", "model": "fixture-model"}
    result = await bench._create_job_through_admission(
        run,
        task,
        arm,
        1,
        create_job=functools.partial(
            jobs_composition.create_bench_job, main.app.state.resources
        ),
    )
    assert str(result["id"]) == JOB
    args = wire.db.create_job.await_args.kwargs
    assert args["user_id"] == USER and args["project_id"] == PROJECT
    assert args["datasource_ids"] == []
    assert args["context"]["bench"] == {
        "run_id": JOB,
        "task": "scope-test",
        "arm": "baseline",
        "replicate": 1,
    }
    assert args["datasource_selection_provenance"]["creation_path"] == "internal_rest"
    wire.db.create_job.reset_mock()
    wire.provision.reset_mock()
    wire.dispatch.reset_mock()
    wire.db.get_user.return_value["is_approved"] = False
    with pytest.raises(HTTPException) as exc:
        await bench._create_job_through_admission(
            run,
            task,
            arm,
            2,
            create_job=functools.partial(
                jobs_composition.create_bench_job, main.app.state.resources
            ),
        )
    assert exc.value.status_code == 403
    wire.db.get_user.return_value = None
    with pytest.raises(HTTPException) as exc:
        await bench._create_job_through_admission(
            run,
            task,
            arm,
            3,
            create_job=functools.partial(
                jobs_composition.create_bench_job, main.app.state.resources
            ),
        )
    assert exc.value.status_code == 401
    wire.db.create_job.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_deliverable_contract_survives_http_validation_and_is_bound_before_insert(
    wire,
):
    response = await submit(
        wire, body(required_deliverables=["output/report.txt", "output/report.txt"])
    )
    assert response.status_code == 200, response.text
    args = wire.db.create_job.await_args.kwargs
    assert args["context"]["required_deliverables"] == ["output/report.txt"]
    assert args["delivery_contract"] is not None
    wire.db.resolve_datasources_for_thread.assert_awaited_once()


@pytest.mark.asyncio
async def test_invalid_workspace_preserves_structured_error_without_writing(wire):
    response = await submit(
        wire, body(config_override={"workspace": {"backend": "unknown-tier"}})
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["code"] == "invalid_workspace_backend"
    assert isinstance(response.json()["detail"]["message"], str)
    wire.db.create_job.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("as_string", [False, True])
async def test_success_keeps_jsonb_shape_and_redacts_private_workspace_fields(
    wire, as_string
):
    import json

    context = {"safe": "yes", "vm": {"host": "synthetic-private-host"}}
    config = {
        "llm": {"model": "fixture"},
        "workspace": {"remote": {"host": "synthetic-private-host"}},
    }
    wire.db.create_job.side_effect = None
    wire.db.create_job.return_value = {
        "id": UUID(JOB),
        "status": "created",
        "workspace_contract": {"state": "unassigned"},
        "context": json.dumps(context) if as_string else context,
        "config_override": json.dumps(config) if as_string else config,
    }
    response = await submit(wire, body())
    assert response.status_code == 200
    result = response.json()
    assert isinstance(result["context"], str if as_string else dict)
    assert isinstance(result["config_override"], str if as_string else dict)
    assert (json.loads(result["context"]) if as_string else result["context"]) == {
        "safe": "yes"
    }
    projected = (
        json.loads(result["config_override"])
        if as_string
        else result["config_override"]
    )
    assert projected == {"llm": {"model": "fixture"}, "workspace": {}}
    assert "synthetic-private-host" not in response.text


def delivery_repository(**fields):
    return {
        "id": CONNECTOR,
        "name": "Widget",
        "type": "repository",
        "connection_url": "https://github.com/Acme/Widget.git",
        "read_only": False,
        "project_read_only": False,
        "config": {"forge": "github"},
        "policy_revision": 7,
        **fields,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "point,status,detail",
    [
        ("explicit", 403, "One or more selected connectors are unavailable"),
        ("tier", 400, "repository requires a shell workspace"),
        ("default", 403, "One or more selected connectors are unavailable"),
        ("default_store", 500, "fixture store unavailable"),
    ],
)
async def test_datasource_refusal_precedes_delivery_lookup_insert_and_provision(
    wire, point, status, detail
):
    from orchestrator.services.datasource_policy import DatasourceUnavailableError

    if point == "default":
        wire.defaults.side_effect = DatasourceUnavailableError()
    elif point == "default_store":
        wire.defaults.side_effect = RuntimeError(detail)
    else:
        wire.authorize.side_effect = HTTPException(status, detail)
    selection = (
        {"use_datasource_defaults": True}
        if point.startswith("default")
        else {"datasource_ids": [CONNECTOR]}
    )
    response = await submit(
        wire, body(required_deliverables=["output/report.txt"], **selection)
    )
    assert response.status_code == status, response.text
    assert response.json() == {"detail": detail}
    wire.db.resolve_datasources_for_thread.assert_not_awaited()
    wire.db.create_job.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("child", [False, True])
async def test_thread_dispatch_defaults_and_child_inheritance_reach_exact_insert(
    officer_wire, monkeypatch, child
):
    wire = officer_wire
    wire.officer.thread["metadata"]["config_override"]["officer"]["enabled"] = False
    wire.officer.thread["metadata"]["datasource_ids"] = [CONNECTOR]
    wire.db.get_project_officer_lineage.return_value = []
    wire.defaults.return_value = ([EXPERT], {EXPERT: 17})
    wire.authorize.side_effect = lambda _actor, ids, **_kw: (list(ids), {CONNECTOR: 7})
    response = await submit(
        wire,
        body(
            thread_id=THREAD,
            parent_job_id=PARENT if child else None,
            use_datasource_defaults=True,
        ),
        **{"x-test-internal": "1"},
    )
    assert response.status_code == 200, response.text
    kwargs = wire.db.create_job.await_args.kwargs
    expected_ids, expected_revisions = (
        ([CONNECTOR], {CONNECTOR: 7}) if child else ([EXPERT], {EXPERT: 17})
    )
    assert kwargs["datasource_ids"] == expected_ids
    assert kwargs["datasource_policy_revisions"] == expected_revisions
    provenance = kwargs["datasource_selection_provenance"]
    assert provenance["datasource_ids"] == expected_ids
    assert provenance["policy_revisions"] == expected_revisions
    assert provenance["origin"] == ("inherited" if child else "default")
    assert provenance["project_ids"] == [PROJECT]
    assert provenance["effective_work_owner_id"] == USER
    assert kwargs["created_by_thread_id"] == (None if child else THREAD)
    assert kwargs["wake_on_complete"] is (not child)
    assert wire.defaults.await_count == int(not child)
    assert wire.authorize.await_count == int(child)


@pytest.mark.asyncio
async def test_bound_pr_delivery_preserves_revision_and_provisions_after_insert(wire):
    wire.authorize.side_effect = lambda _actor, ids, **_kw: (list(ids), {CONNECTOR: 7})
    wire.db.resolve_datasources_for_thread.return_value = [delivery_repository()]
    order = Mock()
    for name, collaborator in (
        ("authorize", wire.authorize),
        ("resolve", wire.db.resolve_datasources_for_thread),
        ("insert", wire.db.create_job),
        ("provision", wire.provision),
        ("dispatch", wire.dispatch),
    ):
        order.attach_mock(collaborator, name)
    response = await submit(
        wire,
        body(
            datasource_ids=[CONNECTOR],
            required_deliverables=[" PR:Acme/Widget ", "pr:acme/widget"],
        ),
    )
    assert response.status_code == 200, response.text
    assert [event[0] for event in order.mock_calls] == [
        "authorize",
        "resolve",
        "insert",
        "provision",
        "dispatch",
    ]
    wire.db.resolve_datasources_for_thread.assert_awaited_once_with(
        [CONNECTOR], [PROJECT]
    )
    kwargs = wire.db.create_job.await_args.kwargs
    assert kwargs["datasource_policy_revisions"] == {CONNECTOR: 7}
    assert kwargs["context"]["required_deliverables"] == ["pr:acme/widget"]
    assert kwargs["delivery_contract"]["pr_bindings"] == [
        {
            "repository": "acme/widget",
            "datasource_id": CONNECTOR,
            "forge": "github",
            "policy_revision": 7,
        }
    ]
    assert kwargs["delivery_contract"]["deliverables"] == ["pr:acme/widget"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "requested,rows,code",
    [
        (["pr:acme/other"], [delivery_repository()], "pr_deliverable_not_attached"),
        (
            ["pr:acme/widget"],
            [delivery_repository(read_only=True)],
            "pr_deliverable_read_only",
        ),
        (
            ["repos/Widget/output/report.txt"],
            [delivery_repository()],
            "external_repository_requires_pr",
        ),
    ],
)
async def test_delivery_contract_refusals_keep_structured_http_without_inserting(
    wire, requested, rows, code
):
    wire.db.resolve_datasources_for_thread.return_value = rows
    response = await submit(
        wire, body(datasource_ids=[CONNECTOR], required_deliverables=requested)
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == code
    assert isinstance(response.json()["detail"]["message"], str)
    wire.authorize.assert_awaited_once()
    wire.db.resolve_datasources_for_thread.assert_awaited_once_with(
        [CONNECTOR], [PROJECT]
    )
    wire.db.create_job.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt_conflict", [False, True])
async def test_officer_delivery_refusal_records_requirement_before_refusing_creation(
    officer_wire, monkeypatch, receipt_conflict
):
    from orchestrator.services.officer_admission import OfficerAdmissionConflict

    wire = officer_wire
    wire.db.resolve_datasources_for_thread.return_value = [delivery_repository()]
    receipt = AsyncMock()
    if receipt_conflict:
        receipt.side_effect = OfficerAdmissionConflict("officer_held", "Post was held.")
    monkeypatch.setattr(
        "orchestrator.services.officer_admission.record_rejected_ticket_delivery_requirement",
        receipt,
    )
    response = await submit(
        wire,
        body(
            thread_id=THREAD,
            ticket="fixture",
            datasource_ids=[CONNECTOR],
            required_deliverables=["repos/Widget/output/report.txt"],
        ),
        **{"x-test-internal": "1"},
    )
    assert response.status_code == 409, response.text
    expected_code = (
        "officer_held" if receipt_conflict else "external_repository_requires_pr"
    )
    assert response.json()["detail"]["code"] == expected_code
    receipt.assert_awaited_once()
    assert receipt.await_args.args == (wire.db,)
    args = receipt.await_args.kwargs
    assert args["preparation"].thread_id == THREAD
    assert args["ticket_note_id"] == "fixture"
    assert args["ticket_ready_at"] == STAMP
    assert args["required_pr_repositories"] == ["acme/widget"]
    wire.db.create_job.assert_not_awaited()
    wire.officer.admit.assert_not_awaited()
    wire.officer.preflight.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_type,status,detail",
    [
        (
            "DatasourceMaterializationAuthorizationError",
            403,
            "Work owner is no longer authorized",
        ),
        (
            "DatasourcePolicyConflictError",
            409,
            "Connector policy changed while creating work; retry the request",
        ),
        ("RuntimeError", 500, "fixture persistence failure"),
    ],
)
async def test_insert_error_keeps_wire_mapping_and_never_provisions(
    wire, error_type, status, detail
):
    from orchestrator.database import postgres

    error_class = (
        RuntimeError if error_type == "RuntimeError" else getattr(postgres, error_type)
    )
    wire.db.create_job.side_effect = error_class("fixture persistence failure")
    response = await submit(wire, body())
    assert response.status_code == status, response.text
    assert response.json() == {"detail": detail}
    wire.db.create_job.assert_awaited_once()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_normal_provisioning_failure_leaves_insert_but_does_not_dispatch(wire):
    wire.provision.side_effect = RuntimeError("fixture provisioning failure")
    response = await submit(wire, body())
    assert response.status_code == 500, response.text
    assert response.json() == {"detail": "fixture provisioning failure"}
    wire.db.create_job.assert_awaited_once()
    wire.provision.assert_awaited_once()
    wire.scholar.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_inactive_officer_preflight_is_returned_without_scholar_or_normal_dispatch(
    officer_wire,
):
    wire = officer_wire
    wire.officer.preflight.return_value = SimpleNamespace(
        activated=False,
        state="retryable_failure",
        retryable=True,
        phase="repository",
        error="fixture provisioning unavailable",
    )
    response = await submit(wire, body(thread_id=THREAD), **{"x-test-internal": "1"})
    assert response.status_code == 200, response.text
    assert response.json()["provisioning_preflight"] == {
        "activated": False,
        "state": "retryable_failure",
        "retryable": True,
        "phase": "repository",
        "error": "fixture provisioning unavailable",
    }
    wire.officer.admit.assert_awaited_once()
    wire.officer.preflight.assert_awaited_once()
    wire.scholar.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_bench_readiness_refusal_precedes_fresh_creator_lookup(wire, monkeypatch):
    from orchestrator.routers import bench

    monkeypatch.setattr(
        access_composition,
        "enforce_readiness_gate",
        AsyncMock(side_effect=HTTPException(503, "not ready")),
    )
    run = {"id": JOB, "created_by": USER, "spec": {"project_id": PROJECT}}
    with pytest.raises(HTTPException) as exc:
        await bench._create_job_through_admission(
            run,
            {"id": "scope-test", "description": "bench admission"},
            {"name": "baseline", "model": "fixture-model"},
            1,
            create_job=functools.partial(
                jobs_composition.create_bench_job, main.app.state.resources
            ),
        )
    assert (exc.value.status_code, exc.value.detail) == (503, "not ready")
    wire.db.get_user.assert_not_awaited()
    wire.db.create_job.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.parametrize(
    "override, offending",
    [
        (
            {"llm": {"model": "m", "base_url": "https://evil.example/v1"}},
            "llm.base_url",
        ),
        ({"llm": {"model": "m", "api_key": "sk-caller"}}, "llm.api_key"),
        ({"env_keys": {"EMBEDDING_BASE_URL": "https://evil.example/v1"}}, "env_keys"),
    ],
)
@pytest.mark.asyncio
async def test_caller_transport_keys_are_refused_at_admission(
    wire, override, offending
):
    """A caller may not pin transport in ``config_override``: routing is server-
    resolved from the model ID and credentials are injected only at dispatch, so
    a pinned base_url/api_key/env_keys would otherwise have the deployment's
    stored key paired with a caller-chosen endpoint. Refuse loudly (422), name
    the offending path, and write nothing."""
    response = await submit(wire, body(config_override=override))
    assert response.status_code == 422, response.text
    assert offending in response.json()["detail"]
    wire.db.create_job.assert_not_awaited()
    wire.provision.assert_not_awaited()
    wire.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_transport_fence_also_covers_the_internal_rest_path(wire):
    """The internal (X-Internal-Key) create route holds the same line — the
    MCP create tool already strips these before POSTing, so a body that still
    carries one is refused rather than trusted."""
    response = await submit(
        wire,
        body(config_override={"llm": {"model": "m", "base_url": "https://evil/v1"}}),
        **{"x-test-internal": "1"},
    )
    assert response.status_code == 422, response.text
    wire.db.create_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_model_only_override_is_still_admitted(wire):
    """The fence rejects transport, never the routing selector itself."""
    response = await submit(wire, body(config_override={"llm": {"model": "fixture"}}))
    assert response.status_code == 200, response.text
    args = wire.db.create_job.await_args.kwargs
    assert args["config_override"]["llm"]["model"] == "fixture"
