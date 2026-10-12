"""Owner-visible shell output is bound to one exact pinned worker process."""

from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.routers import job_diagnostics as job_diagnostics_routes
from orchestrator.services import job_diagnostics as job_diagnostics_operations
from agent.api.models import PinnedJobRecipient
from orchestrator.services import job_mutation_target as job_mutation_target_module


AGENT_ID = "11111111-1111-4111-8111-111111111111"
JOB_ID = "22222222-2222-4222-8222-222222222222"
PROCESS_GENERATION = "33333333-3333-4333-8333-333333333333"


def _recipient(*, process_generation: str = PROCESS_GENERATION) -> PinnedJobRecipient:
    return PinnedJobRecipient(
        expected_agent_id=AGENT_ID,
        expected_pod_uid=None,
        expected_process_generation=process_generation,
        expected_job_id=JOB_ID,
    )


def _runtime_client() -> SimpleNamespace:
    return SimpleNamespace(
        agent_id=AGENT_ID,
        dispatch_process_generation=PROCESS_GENERATION,
    )


def _shell_agent() -> tuple[MagicMock, MagicMock]:
    shell = MagicMock()
    shell.list_tabs.return_value = [
        {"name": "work", "type": "shell", "created_at": "now"}
    ]
    shell.read_with_offset.return_value = {"output": "exact A output", "total_lines": 1}
    agent = MagicMock()
    agent._shell_manager = shell
    return agent, shell


@pytest.fixture(params=["app", "dual_app"])
def runtime_shell_endpoint(request):
    if request.param == "app":
        from agent.api import app as module

        app = module.create_app()
    else:
        from agent.api import dual_app as module

        app = module.create_dual_app()

    saved = {
        "_agent": module._agent,
        "_current_job_id": module._current_job_id,
        "_orchestrator_client": module._orchestrator_client,
    }
    agent, shell = _shell_agent()
    module._agent = agent
    module._current_job_id = JOB_ID
    module._orchestrator_client = _runtime_client()
    route = next(
        route
        for route in app.routes
        if getattr(route, "path", "") == "/system/shell-state"
    )
    yield route.endpoint, shell, route.methods
    for name, value in saved.items():
        setattr(module, name, value)


@pytest.mark.asyncio
async def test_exact_current_runtime_returns_shell_state(runtime_shell_endpoint):
    endpoint, shell, methods = runtime_shell_endpoint

    result = await endpoint(_recipient())

    assert methods == {"POST"}
    assert result == {
        "tabs": [
            {
                "name": "work",
                "type": "shell",
                "created_at": "now",
                "total_lines": 1,
                "recent_output": "exact A output",
            }
        ]
    }
    shell.list_tabs.assert_called_once_with()


@pytest.mark.asyncio
async def test_same_ip_replacement_returns_zero_shell_data(runtime_shell_endpoint):
    endpoint, shell, _methods = runtime_shell_endpoint

    with pytest.raises(HTTPException) as exc:
        await endpoint(_recipient(process_generation="replacement-process"))

    assert exc.value.status_code == 409
    assert exc.value.detail == {"code": "pinned_recipient_mismatch"}
    shell.list_tabs.assert_not_called()
    shell.read_with_offset.assert_not_called()


class _Response:
    status_code = 200
    text = ""

    @staticmethod
    def json():
        return {"tabs": [{"name": "work", "recent_output": "exact A output"}]}


class _Client:
    posts: list[tuple[str, dict]] = []

    def __init__(self, *_args, **_kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def post(self, url, json):
        self.posts.append((url, json))
        return _Response()


def _owner_route_dependencies(prepare):
    """The owner route's dependencies, composed as ``main`` composes them.

    The handler moved into ``orchestrator.routers.job_diagnostics`` (R1.B02);
    the recipient attestation it calls is still main's, injected here.
    """

    async def job_access(_request, _store, _job_id):
        return (
            {"id": "owner"},
            {
                "id": JOB_ID,
                "status": "processing",
                "assigned_agent_id": AGENT_ID,
            },
        )

    return job_diagnostics_routes.JobDiagnosticsDependencies(
        store=MagicMock(),
        operations=job_diagnostics_operations.JobDiagnosticsDependencies(
            workspace=MagicMock(),
            snapshots=MagicMock(),
            audit_reader=MagicMock(),
            prepare_pinned_job_mutation_target=prepare,
        ),
        require_job_access=job_access,
    )


@pytest.mark.asyncio
async def test_owner_route_uses_fresh_recipient_and_exact_agent_endpoint():
    recipient = _recipient()
    target = job_mutation_target_module.PinnedJobMutationTarget(
        agent={"pod_ip": "10.0.0.9", "pod_port": 8001},
        recipient=recipient,
    )
    _Client.posts = []
    prepare = AsyncMock(return_value=target)
    with ExitStack() as stack:
        stack.enter_context(
            patch.object(job_diagnostics_operations.httpx, "AsyncClient", _Client)
        )
        result = await job_diagnostics_routes.get_job_shell_state(
            MagicMock(),
            JOB_ID,
            dependencies=_owner_route_dependencies(prepare),
        )

    assert result["tabs"][0]["recent_output"] == "exact A output"
    prepare.assert_awaited_once_with(
        agent_id=AGENT_ID,
        job_id=JOB_ID,
        require_idle=False,
    )
    assert _Client.posts == [
        (
            "http://10.0.0.9:8001/system/shell-state",
            recipient.model_dump(mode="json"),
        )
    ]


@pytest.mark.asyncio
async def test_owner_route_never_dials_unattested_reused_ip():
    client = MagicMock()
    with (
        patch.object(job_diagnostics_operations.httpx, "AsyncClient", client),
        pytest.raises(HTTPException) as exc,
    ):
        await job_diagnostics_routes.get_job_shell_state(
            MagicMock(),
            JOB_ID,
            dependencies=_owner_route_dependencies(AsyncMock(return_value=None)),
        )

    assert exc.value.status_code == 409
    assert exc.value.detail == {"code": "pinned_recipient_unavailable"}
    client.assert_not_called()
