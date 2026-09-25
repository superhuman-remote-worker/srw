"""Generic work cannot enter reference-harness control or orphan recovery."""

from tests import _b09_control_seams as control_seams

from tests import b08_completion_helpers as b08_helpers

from copy import deepcopy
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from fastapi import HTTPException
import pytest

# R1.B06: session admission moved to services/thread_admission, which
# imports the expert resolver and the config loader directly. Patching
# them on main would be green but inert.
from orchestrator.services import agent_child_threads  # noqa: E402
from orchestrator.services import thread_admission  # noqa: E402
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.job_completion_commands import (
    CompletionFenceRejected,
    accept_completion_command,
)
from orchestrator.services import session_config_resolution  # noqa: E402
from orchestrator.application import access as access_composition
from orchestrator.application import catalogue as catalogue_composition
from orchestrator.application import sessions as sessions_composition
from orchestrator.schemas import agent_child_threads as agent_child_threads_module
from orchestrator.schemas import thread_admission as schemas_thread_admission_module
from orchestrator.security import access as access_module
from orchestrator.security import auth as auth_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import grant_enforcement as grant_enforcement_module
from orchestrator.services import thread_admission as thread_admission_module
from orchestrator.services import (
    thread_project_authorization as thread_project_authorization_module,
)


WORK = "22222222-2222-4222-8222-222222222222"
USER = "11111111-1111-4111-8111-111111111111"


@pytest.mark.asyncio
@pytest.mark.parametrize("internal", [False, True])
async def test_generic_expert_is_refused_before_interactive_loader(
    monkeypatch, internal
):
    import orchestrator.main as main

    expert = {"id": WORK, "harness_adapter": None, "config": {}}
    db = SimpleNamespace(
        get_user_settings=AsyncMock(return_value={}),
        get_application_expert_default=AsyncMock(return_value=expert),
        create_thread=AsyncMock(side_effect=AssertionError("Generic session inserted")),
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(access_composition, "enforce_readiness_gate", AsyncMock())
    monkeypatch.setattr(access_module, "require_internal", AsyncMock())
    monkeypatch.setattr(
        auth_module, "require_approved_user", AsyncMock(return_value={"id": USER})
    )
    monkeypatch.setattr(
        thread_project_authorization_module,
        "authorize_thread_project_ids",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        session_config_resolution,
        "resolve_session_account_defaults",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(deployment_gates_module, "is_experts_db_enabled", lambda: True)
    monkeypatch.setattr(
        grant_enforcement_module, "user_experts_enabled", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        thread_admission,
        "resolve_root_expert",
        AsyncMock(return_value=SimpleNamespace(expert=expert, project_override=None)),
    )
    monkeypatch.setattr(
        thread_admission,
        "resolve_config",
        lambda **_: pytest.fail("SRW loader reached"),
    )
    with pytest.raises(HTTPException) as denied:
        if internal:
            await agent_child_threads.agent_create_thread(
                None,
                agent_child_threads_module.AgentThreadCreateRequest(),
                dependencies=sessions_composition.agent_child_threads_dependencies(
                    main.app.state.resources
                ),
            )
        else:
            await thread_admission_module.create_thread(
                schemas_thread_admission_module.ThreadCreateRequest(expert_id=WORK),
                None,
                dependencies=sessions_composition.thread_admission_dependencies(
                    main.app.state.resources
                ),
            )
    assert denied.value.status_code == 409
    assert "Interactive sessions require the SRW adapter" in denied.value.detail
    db.create_thread.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation",
    [
        "resume",
        "approve",
        "pause",
        "delete",
        "upgrade",
        "resume_without_vm",
        "complete",
        "session_resume",
        "assign",
    ],
)
async def test_legacy_mutations_refuse_generic_before_effects(monkeypatch, operation):
    import orchestrator.main as main
    from orchestrator.routers import job_assignment

    work = {
        "id": WORK,
        "user_id": USER,
        "status": "processing",
        "execution_lane": "pinned",
        "execution_harness_adapter": "generic",
    }
    db = SimpleNamespace(get_job=AsyncMock(return_value=deepcopy(work)))
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        access_module,
        "require_internal_or_job_access",
        AsyncMock(return_value=({"id": USER}, work)),
    )
    monkeypatch.setattr(
        access_module,
        "require_job_access",
        AsyncMock(return_value=({"id": USER}, work)),
    )
    monkeypatch.setattr(
        access_module,
        "require_thread_owner",
        AsyncMock(return_value=({"id": USER}, work)),
    )
    if operation == "resume":
        call = control_seams.resume_job_internal(WORK, user={"id": USER}, job=work)
    elif operation == "approve":
        call = control_seams.approve_job_internal(WORK, user={"id": USER}, job=work)
    elif operation == "pause":
        call = control_seams.pause_job(None, WORK)
    elif operation == "delete":
        call = control_seams.delete_job(None, WORK)
    elif operation == "upgrade":
        call = control_seams.upgrade_job_to_vm_internal(WORK)
    elif operation == "resume_without_vm":
        call = control_seams.resume_job_without_vm_internal(WORK)
    elif operation == "complete":
        call = b08_helpers.complete_job_legacy(None, WORK, None, _authorized=True)
    elif operation == "session_resume":
        call = control_seams.resume_thread(WORK, None)
    else:
        call = job_assignment.assign_job_to_agent(
            None,
            WORK,
            USER,
            dependencies=SimpleNamespace(
                store=db, logger=None, require_admin=AsyncMock()
            ),
        )
    with pytest.raises(HTTPException) as denied:
        await call
    assert denied.value.status_code == 409
    assert denied.value.detail["code"] == "manifest_runtime_owned"


@pytest.mark.asyncio
async def test_legacy_direct_delivery_and_internal_resume_leave_generic_alone(
    monkeypatch,
):
    import orchestrator.main as main

    job = {"id": WORK, "execution_harness_adapter": "generic"}
    db = SimpleNamespace(get_job=AsyncMock(return_value=job))
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    assert await control_seams.dispatch_job_to_agent(job, {"id": USER}) is False
    assert await control_seams.resume_job_on_agent(job, {"id": USER}) is False
    assert await control_seams.internal_resume_job(WORK, "feedback") is False
    assert await control_seams.initiate_pause(job) is None


@pytest.mark.asyncio
async def test_descendant_cancel_uses_generic_controller(monkeypatch):
    import orchestrator.main as main

    job = {"id": WORK, "execution_harness_adapter": "generic", "status": "processing"}
    db = SimpleNamespace(get_descendant_jobs=AsyncMock(return_value=[job]))
    cancel = AsyncMock(return_value=True)
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        catalogue_composition,
        "manifest_execution_service",
        lambda _resources: SimpleNamespace(cancel=cancel),
    )
    assert await control_seams.cascade_cancel_to_children(USER) is True
    cancel.assert_awaited_once_with(WORK)
    await control_seams.cascade_pause_to_children(USER)


@pytest.fixture(scope="module")
def pg_url():
    with PostgresContainer("postgres:16") as pg:
        yield re.sub(r"^postgresql\+\w+://", "postgresql://", pg.get_connection_url())


@pytest_asyncio.fixture
async def runtime_db(pg_url):
    db = PostgresDB(pg_url, min_connections=1, max_connections=2)
    await db.connect()
    await db.execute("""
        CREATE TABLE IF NOT EXISTS agents(id uuid PRIMARY KEY, status text);
        CREATE TABLE IF NOT EXISTS jobs(
            id uuid PRIMARY KEY, project_id uuid, user_id uuid, status text,
            execution_lane text DEFAULT 'pinned', assigned_agent_id uuid,
            parent_job_id uuid, resolved_config jsonb,
            workspace_idle_revision bigint NOT NULL DEFAULT 0,
            workspace_idle_episode jsonb,
            lease_expires_at timestamptz, updated_at timestamptz DEFAULT now(),
            context jsonb DEFAULT '{}', freeze_data jsonb, priority integer DEFAULT 0,
            config_override jsonb DEFAULT '{"workspace":{"backend":"none"}}',
            error_message text, error_details jsonb, completion_seq_hwm bigint DEFAULT 0,
            completion_outcome_kind text
        );
        CREATE TABLE IF NOT EXISTS srw_execution_specs(
            work_kind text, work_id uuid, harness_adapter text
        );
        CREATE TABLE IF NOT EXISTS run_queue(
            unit_id uuid, unit_kind text, state text, lease_token bigint, input_seq bigint
        );
        CREATE TABLE IF NOT EXISTS docker_workspace_leases(
            owner_kind text, owner_id uuid, status text, quarantine_reason text,
            updated_at timestamptz
        );
        -- Pinned resume paths fence on an open idle operation (0270).
        CREATE TABLE IF NOT EXISTS vm_idle_operations(
            owner_kind text NOT NULL, owner_id uuid NOT NULL, closed_at timestamptz
        );
        TRUNCATE jobs, agents, srw_execution_specs, run_queue, vm_idle_operations;
    """)
    yield db
    await db.disconnect()


@pytest.mark.asyncio
async def test_real_postgres_generic_job_is_excluded_from_orphan_claim_and_resume(
    runtime_db,
):
    db = runtime_db
    generic, reference, agent = uuid4(), uuid4(), uuid4()
    await db.execute(
        "INSERT INTO jobs(id,status) VALUES($1,'processing'),($2,'processing')",
        generic,
        reference,
    )
    await db.execute(
        "INSERT INTO srw_execution_specs VALUES('Job',$1,'generic')", generic
    )
    assert await db.pause_job(str(generic)) is False
    assert await db.resume_pinned_job_in_process(str(generic)) is False
    with pytest.raises(HTTPException) as deletion:
        await db.delete_job(str(generic))
    assert deletion.value.status_code == 409
    recovered = await db.recover_orphaned_jobs()
    assert recovered.count == 1
    assert (
        await db.fetchval("SELECT status FROM jobs WHERE id=$1", generic)
        == "processing"
    )
    assert (
        await db.fetchval("SELECT status FROM jobs WHERE id=$1", reference) == "paused"
    )

    await db.execute("UPDATE jobs SET status='created' WHERE id=$1", generic)
    assert await db.claim_job_for_agent(str(generic), str(agent)) is False
    assert (
        await db.queue_job_for_resume(
            str(generic), {"queued_feedback": "must not persist"}
        )
        is False
    )
    assert (
        await db.fetchval("SELECT status FROM jobs WHERE id=$1", generic) == "created"
    )
    assert await db.claim_job_for_agent(str(reference), str(agent)) is True


@pytest.mark.asyncio
async def test_real_postgres_srw_completion_refuses_generic_before_command_insert(
    runtime_db,
):
    db = runtime_db
    work = uuid4()
    await db.execute("INSERT INTO jobs(id,status) VALUES($1,'processing')", work)
    await db.execute("INSERT INTO srw_execution_specs VALUES('Job',$1,'generic')", work)
    with pytest.raises(CompletionFenceRejected, match="manifest harness"):
        await accept_completion_command(
            db,
            job_id=str(work),
            payload={"should_stop": True, "goal_achieved": True},
            lease_token=None,
            agent_id=USER,
            client_report_id=None,
            requested_by="unit-test",
        )
    assert (
        await db.fetchval("SELECT status FROM jobs WHERE id=$1", work) == "processing"
    )
