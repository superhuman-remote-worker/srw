"""Manual assignment must preserve the dispatcher's workspace preflight."""

from tests import _b09_control_seams as control_seams

from unittest.mock import ANY, AsyncMock, MagicMock
from types import SimpleNamespace

import pytest

import orchestrator.main as main
from orchestrator.application import access as access_composition
from orchestrator.application import controls as controls_composition
from orchestrator.services import job_dispatcher as job_dispatcher_module
from orchestrator.services import (
    job_workspace_authority as job_workspace_authority_module,
)
import fastapi as fastapi_module
import httpx


JOB_ID = "00000000-0000-0000-0000-000000000101"
AGENT_ID = "00000000-0000-0000-0000-000000000201"


def _job(status: str, *, workspace_status: str | None = None) -> dict:
    context = {
        "_workspace_contract": {
            "version": 1,
            "requested_backend": "sandbox",
            "assigned_backend": "sandbox",
            "assignment_source": "test",
        }
    }
    if workspace_status:
        context["workspace_container"] = {
            "status": workspace_status,
            **(
                {"host": "workspace-test.svc", "port": 30022}
                if workspace_status == "ready"
                else {}
            ),
        }
        if workspace_status == "ready":
            context["workspace_container"]["_runtime_incarnation"] = (
                "11111111-1111-4111-8111-111111111111"
            )
    return {
        "id": JOB_ID,
        "status": status,
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": context,
    }


def _agent() -> dict:
    return {
        "id": AGENT_ID,
        "status": "ready",
        "pod_ip": "10.42.0.9",
        "pod_port": 8080,
    }


@pytest.fixture
def collaborators(monkeypatch):
    monkeypatch.setattr(access_composition, "require_admin", AsyncMock())
    monkeypatch.setattr(main.app.state.resources.postgres_db, "get_job", AsyncMock())
    monkeypatch.setattr(main.app.state.resources.postgres_db, "get_agent", AsyncMock())
    monkeypatch.setattr(
        main.app.state.resources.postgres_db, "shed_workspace_context", AsyncMock()
    )
    monkeypatch.setattr(
        main.app.state.resources.postgres_db,
        "prepare_pinned_job_for_workspace_resume",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        main.app.state.resources.postgres_db,
        "claim_job_for_agent",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        main.app.state.resources.postgres_db,
        "queue_job_for_resume",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(job_dispatcher_module, "trigger_dispatch", MagicMock())
    delivery = SimpleNamespace(
        dispatch=AsyncMock(return_value=True),
        resume=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        controls_composition, "job_delivery_operations", lambda _resources: delivery
    )
    return delivery


@pytest.mark.asyncio
async def test_flag_on_manual_assign_guard_blocks_before_workspace_or_agent_io(
    collaborators, monkeypatch
):
    job = _job("paused", workspace_status="failed")
    main.app.state.resources.postgres_db.get_job.return_value = job
    monkeypatch.setattr(
        main.app.state.resources.settings, "completion_commands_enabled", True
    )
    blocked = fastapi_module.HTTPException(
        status_code=409, detail="completion finalizing"
    )
    guard = AsyncMock(side_effect=blocked)
    monkeypatch.setattr(
        main.app.state.resources.completion_control_boundary, "guard", guard
    )

    with pytest.raises(fastapi_module.HTTPException) as exc:
        await control_seams.assign_job_to_agent(MagicMock(), JOB_ID, AGENT_ID)

    assert exc.value.status_code == 409
    guard.assert_awaited_once_with(JOB_ID, source="manual_assign")
    main.app.state.resources.postgres_db.shed_workspace_context.assert_not_awaited()
    main.app.state.resources.postgres_db.prepare_pinned_job_for_workspace_resume.assert_not_awaited()
    main.app.state.resources.postgres_db.get_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_flag_on_missing_workspace_uses_claimed_atomic_preflight(
    collaborators, monkeypatch
):
    job = _job("failed", workspace_status="failed")
    main.app.state.resources.postgres_db.get_job.return_value = job
    monkeypatch.setattr(
        main.app.state.resources.settings, "completion_commands_enabled", True
    )
    monkeypatch.setattr(
        main.app.state.resources.completion_control_boundary, "guard", AsyncMock()
    )
    claim = SimpleNamespace(claim_id="00000000-0000-0000-0000-000000000301")
    claim_control = AsyncMock(return_value=claim)
    monkeypatch.setattr(
        main.app.state.resources.completion_control_boundary, "claim", claim_control
    )

    result = await control_seams.assign_job_to_agent(MagicMock(), JOB_ID, AGENT_ID)

    assert result["status"] == "queued"
    claim_control.assert_awaited_once_with(job, source="manual_assign_workspace")
    main.app.state.resources.postgres_db.prepare_pinned_job_for_workspace_resume.assert_awaited_once_with(
        JOB_ID,
        "workspace_container",
        expected_status="failed",
        completion_control_claim_id=claim.claim_id,
        lift_operator_pause_hold="",
    )
    main.app.state.resources.postgres_db.shed_workspace_context.assert_not_awaited()
    main.app.state.resources.postgres_db.queue_job_for_resume.assert_not_awaited()
    main.app.state.resources.postgres_db.get_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_flag_on_live_workspace_claims_before_agent_post(
    collaborators, monkeypatch
):
    job = _job("failed", workspace_status="ready")
    main.app.state.resources.postgres_db.get_job.return_value = job
    main.app.state.resources.postgres_db.get_agent.return_value = _agent()
    monkeypatch.setattr(
        main.app.state.resources.settings, "completion_commands_enabled", True
    )
    monkeypatch.setattr(
        main.app.state.resources.completion_control_boundary, "guard", AsyncMock()
    )
    order: list[str] = []
    main.app.state.resources.postgres_db.claim_job_for_agent.side_effect = (
        lambda *_args, **_kwargs: order.append("claim") or True
    )
    collaborators.dispatch.side_effect = (
        lambda *_args, **_kwargs: order.append("post") or True
    )

    result = await control_seams.assign_job_to_agent(MagicMock(), JOB_ID, AGENT_ID)

    assert result["status"] == "assigned"
    assert order == ["claim", "post"]
    main.app.state.resources.postgres_db.claim_job_for_agent.assert_awaited_once_with(
        JOB_ID,
        AGENT_ID,
        completion_commands_enabled=True,
        allow_failed=True,
        lift_operator_pause_hold="",
    )


@pytest.mark.asyncio
async def test_manual_assign_waits_for_legacy_runtime_adoption_before_claim(
    collaborators, monkeypatch
):
    job = _job("created", workspace_status="ready")
    job["context"]["workspace_container"].update(
        {"provisioner": "k8s", "pod_ip": "10.42.0.17"}
    )
    job["context"]["workspace_container"].pop("_runtime_incarnation")
    main.app.state.resources.postgres_db.get_job.return_value = job
    monkeypatch.setattr(
        main.app.state.resources.completion_control_boundary, "guard", AsyncMock()
    )
    prepare = AsyncMock(
        return_value=("wait", job, "kubernetes_attestation_unavailable")
    )
    monkeypatch.setattr(
        job_workspace_authority_module, "prepare_job_workspace_runtime", prepare
    )

    with pytest.raises(fastapi_module.HTTPException) as raised:
        await control_seams.assign_job_to_agent(MagicMock(), JOB_ID, AGENT_ID)

    assert raised.value.status_code == 409
    assert raised.value.detail["code"] == "workspace_runtime_adoption_pending"
    assert raised.value.detail["retryable"] is True
    prepare.assert_awaited_once_with(job, dependencies=ANY)
    main.app.state.resources.postgres_db.claim_job_for_agent.assert_not_awaited()
    main.app.state.resources.postgres_db.get_agent.assert_not_awaited()
    collaborators.dispatch.assert_not_awaited()


class TestManualAssignWorkspacePreflight:
    @pytest.mark.asyncio
    async def test_stateless_job_rejects_direct_assignment(self, collaborators):
        job = _job("created", workspace_status="ready")
        job["execution_lane"] = "stateless"
        main.app.state.resources.postgres_db.get_job.return_value = job

        with pytest.raises(fastapi_module.HTTPException) as exc:
            await control_seams.assign_job_to_agent(MagicMock(), JOB_ID, AGENT_ID)

        assert exc.value.status_code == 409
        main.app.state.resources.postgres_db.get_agent.assert_not_awaited()
        collaborators.dispatch.assert_not_awaited()
        collaborators.resume.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_created_job_without_workspace_is_queued(self, collaborators):
        main.app.state.resources.postgres_db.get_job.return_value = _job("created")

        result = await control_seams.assign_job_to_agent(MagicMock(), JOB_ID, AGENT_ID)

        assert result["status"] == "queued"
        assert "not reserved" in result["message"]
        main.app.state.resources.postgres_db.shed_workspace_context.assert_awaited_once_with(
            JOB_ID, "workspace_container"
        )
        main.app.state.resources.postgres_db.queue_job_for_resume.assert_not_awaited()
        main.app.state.resources.postgres_db.get_agent.assert_not_awaited()
        collaborators.dispatch.assert_not_awaited()
        job_dispatcher_module.trigger_dispatch.assert_called_once_with(dependencies=ANY)

    @pytest.mark.asyncio
    async def test_failed_job_without_workspace_is_made_dispatchable(
        self, collaborators
    ):
        main.app.state.resources.postgres_db.get_job.return_value = _job(
            "failed", workspace_status="failed"
        )

        result = await control_seams.assign_job_to_agent(MagicMock(), JOB_ID, AGENT_ID)

        assert result["status"] == "queued"
        main.app.state.resources.postgres_db.queue_job_for_resume.assert_awaited_once_with(
            JOB_ID, lift_operator_pause_hold=""
        )
        main.app.state.resources.postgres_db.get_agent.assert_not_awaited()
        job_dispatcher_module.trigger_dispatch.assert_called_once_with(dependencies=ANY)

    @pytest.mark.asyncio
    async def test_live_workspace_still_allows_direct_admin_override(
        self, collaborators
    ):
        main.app.state.resources.postgres_db.get_job.return_value = _job(
            "created", workspace_status="ready"
        )
        main.app.state.resources.postgres_db.get_agent.return_value = _agent()

        result = await control_seams.assign_job_to_agent(MagicMock(), JOB_ID, AGENT_ID)

        assert result == {
            "status": "assigned",
            "agent_id": AGENT_ID,
            "job_id": JOB_ID,
        }
        main.app.state.resources.postgres_db.shed_workspace_context.assert_not_awaited()
        main.app.state.resources.postgres_db.claim_job_for_agent.assert_awaited_once_with(
            JOB_ID,
            AGENT_ID,
            completion_commands_enabled=False,
            allow_failed=True,
            lift_operator_pause_hold="",
        )
        collaborators.dispatch.assert_awaited_once()
        job_dispatcher_module.trigger_dispatch.assert_not_called()


class TestAssignLaneChoice:
    """Paused jobs only take /job/resume when a checkpoint proves they ran.

    A paused-but-never-started job routed down /job/resume starts brief-less
    (knowledge-base/knowledge/issues/fresh_job_dispatched_as_resume_skips_seeding.md).
    """

    @pytest.mark.asyncio
    async def test_paused_with_checkpoint_uses_resume_lane(
        self, collaborators, monkeypatch
    ):
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "job_has_checkpoint",
            AsyncMock(return_value=True),
        )
        main.app.state.resources.postgres_db.get_job.return_value = _job(
            "paused", workspace_status="ready"
        )
        main.app.state.resources.postgres_db.get_agent.return_value = _agent()

        result = await control_seams.assign_job_to_agent(MagicMock(), JOB_ID, AGENT_ID)

        assert result["status"] == "assigned"
        collaborators.resume.assert_awaited_once()
        collaborators.dispatch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_paused_never_started_job_uses_the_fresh_lane(
        self, collaborators, monkeypatch
    ):
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "job_has_checkpoint",
            AsyncMock(return_value=False),
        )
        main.app.state.resources.postgres_db.get_job.return_value = _job(
            "paused", workspace_status="ready"
        )
        main.app.state.resources.postgres_db.get_agent.return_value = _agent()

        result = await control_seams.assign_job_to_agent(MagicMock(), JOB_ID, AGENT_ID)

        assert result["status"] == "assigned"
        collaborators.dispatch.assert_awaited_once()
        collaborators.resume.assert_not_awaited()


class TestPinnedDispatchDefenseInDepth:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "helper_name", ["dispatch_job_to_agent", "resume_job_on_agent"]
    )
    async def test_legacy_adoption_waits_before_agent_network(
        self, helper_name, monkeypatch
    ):
        job = _job("paused", workspace_status="ready")
        job["context"]["workspace_container"].update(
            {"provisioner": "k8s", "pod_ip": "10.42.0.17"}
        )
        job["context"]["workspace_container"].pop("_runtime_incarnation")
        prepare = AsyncMock(
            return_value=("wait", job, "kubernetes_attestation_unavailable")
        )
        monkeypatch.setattr(
            job_workspace_authority_module, "prepare_job_workspace_runtime", prepare
        )
        network = MagicMock(side_effect=AssertionError("agent I/O attempted"))
        monkeypatch.setattr(httpx, "AsyncClient", network)

        assert await getattr(control_seams, helper_name)(job, _agent()) is False

        prepare.assert_awaited_once_with(job, dependencies=ANY)
        network.assert_not_called()

    @pytest.mark.asyncio
    async def test_fresh_helper_refuses_stateless_job_before_network(self):
        job = _job("created", workspace_status="ready")
        job["execution_lane"] = "stateless"
        assert await control_seams.dispatch_job_to_agent(job, _agent()) is False

    @pytest.mark.asyncio
    async def test_resume_helper_refuses_stateless_job_before_network(self):
        job = _job("paused", workspace_status="ready")
        job["execution_lane"] = "stateless"
        assert await control_seams.resume_job_on_agent(job, _agent()) is False

    @pytest.mark.asyncio
    async def test_fresh_helper_refuses_redispatch_circuit_trip_before_network(self):
        job = _job("paused", workspace_status="ready")
        job["context"]["_lease_recovery"] = {
            "state": "tripped",
            "unchanged_recoveries": 3,
        }
        assert await control_seams.dispatch_job_to_agent(job, _agent()) is False

    @pytest.mark.asyncio
    async def test_resume_helper_refuses_redispatch_circuit_trip_before_network(self):
        job = _job("paused", workspace_status="ready")
        job["context"]["_lease_recovery"] = {
            "state": "tripped",
            "unchanged_recoveries": 3,
        }
        assert await control_seams.resume_job_on_agent(job, _agent()) is False
