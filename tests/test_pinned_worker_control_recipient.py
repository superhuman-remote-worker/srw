"""Worker job mutations are bound to one exact registered runtime process.

``/system/shell-state`` already proves the read side of this fence
(``test_job_shell_state_recipient.py``). These are the *mutating* worker
endpoints: a same-IP successor that inherited the predecessor's Pod IP, and a
pre-contract orchestrator that sends no recipient at all, must both be refused
before the job is accepted or a cooperative stop is signalled.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException

from agent.api.models import (
    JobCancelByOrchestratorRequest,
    JobResumeRequest,
    JobStartRequest,
)
from shared.pinned_job_delivery import pinned_job_projection_digest


AGENT_ID = "11111111-1111-4111-8111-111111111111"
JOB_ID = "22222222-2222-4222-8222-222222222222"
PROCESS_GENERATION = "33333333-3333-4333-8333-333333333333"
SUCCESSOR_GENERATION = "44444444-4444-4444-8444-444444444444"
DELIVERY_ID = "55555555-5555-4555-8555-555555555555"
OTHER_DELIVERY_ID = "66666666-6666-4666-8666-666666666666"
PROJECTION_DIGEST = "sha256:" + "a" * 64
DELIVERY_PROOF = "b" * 64


def _recipient(*, process_generation: str = PROCESS_GENERATION) -> dict:
    return {
        "expected_agent_id": AGENT_ID,
        "expected_pod_uid": None,
        "expected_process_generation": process_generation,
        "expected_job_id": JOB_ID,
    }


@pytest.fixture(params=["app", "dual_app"])
def worker_runtime(request):
    """A registered worker process that already owns ``JOB_ID``."""

    if request.param == "app":
        from agent.api import app as module

        application = module.create_app()
    else:
        from agent.api import dual_app as module

        application = module.create_dual_app()

    saved = {
        name: getattr(module, name)
        for name in (
            "_agent",
            "_current_job_id",
            "_orchestrator_client",
            "_stop_requested",
            "_stop_completed",
        )
    }
    if request.param == "dual_app":
        saved["_pod_state"] = module._pod_state
        module._pod_state = module.PodState.WORKING
    module._agent = MagicMock()
    module._current_job_id = JOB_ID
    module._orchestrator_client = SimpleNamespace(
        agent_id=AGENT_ID,
        dispatch_process_generation=PROCESS_GENERATION,
    )
    # These are module-level asyncio.Events; a leftover binding from an earlier
    # test's loop would raise instead of exercising the fence.
    module._stop_requested = asyncio.Event()
    module._stop_completed = asyncio.Event()
    routes = {
        route.path: route.endpoint
        for route in application.routes
        if getattr(route, "path", "").startswith("/job/")
    }
    try:
        yield module, routes
    finally:
        for name, value in saved.items():
            setattr(module, name, value)


async def _call(module, routes, path, payload):
    endpoint = routes[path]
    if path in {"/job/start", "/job/resume"}:
        model = JobStartRequest if path == "/job/start" else JobResumeRequest
        kwargs = {"job_id": JOB_ID, "recipient": payload}
        if path == "/job/start":
            kwargs["description"] = "must not reach a foreign runtime"
        request = model(**kwargs)
        # ``app`` schedules background work, ``dual_app`` owns its own task.
        if module.__name__.endswith("dual_app"):
            return await endpoint(request)
        from fastapi import BackgroundTasks

        return await endpoint(request, BackgroundTasks())
    return await endpoint(JobCancelByOrchestratorRequest(recipient=payload))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path", ("/job/start", "/job/resume", "/job/cancel", "/job/pause")
)
@pytest.mark.parametrize("recipient", (None, "successor"))
async def test_worker_mutation_refuses_a_foreign_or_absent_recipient(
    worker_runtime, path, recipient
):
    module, routes = worker_runtime
    payload = (
        None
        if recipient is None
        else _recipient(process_generation=SUCCESSOR_GENERATION)
    )

    with pytest.raises(HTTPException) as refused:
        await _call(module, routes, path, payload)

    assert refused.value.status_code == 409
    assert refused.value.detail == {"code": "pinned_recipient_mismatch"}
    # No job accepted, no cooperative stop signalled, no task spawned.
    assert module._current_job_id == JOB_ID
    assert module._current_job_task is None
    assert not module._stop_requested.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ("/job/start", "/job/resume"))
async def test_exact_recipient_passes_the_fence(worker_runtime, path):
    """The fence must admit the registered process, not refuse everything.

    With the agent deliberately uninitialised, passing the fence surfaces as
    the downstream 503 — proof that control reached past the recipient check
    without the endpoint taking any job-accepting side effect.
    """

    module, routes = worker_runtime
    module._agent = None

    with pytest.raises(HTTPException) as outcome:
        await _call(module, routes, path, _recipient())

    assert outcome.value.status_code == 503
    assert module._current_job_task is None
    assert not module._stop_requested.is_set()


@pytest.mark.asyncio
async def test_same_job_start_retry_refuses_a_different_delivery(worker_runtime):
    """A busy runtime cannot echo acceptance of another VM projection."""
    module, routes = worker_runtime
    module._orchestrator_client.pinned_delivery_job_id = JOB_ID
    module._orchestrator_client.pinned_delivery_id = DELIVERY_ID
    baseline = JobStartRequest(
        job_id=JOB_ID,
        description="same job, changed delivery",
        recipient=_recipient(),
        pinned_delivery_id=DELIVERY_ID,
        pinned_delivery_proof=DELIVERY_PROOF,
    )
    digest = pinned_job_projection_digest(
        baseline.model_dump(mode="json", exclude_none=True)
    )
    module._orchestrator_client.pinned_projection_digest = digest
    module._orchestrator_client.pinned_delivery_proof = DELIVERY_PROOF
    request = baseline.model_copy(update={
        "pinned_delivery_id": UUID(OTHER_DELIVERY_ID),
        "pinned_projection_digest": digest,
    })

    with pytest.raises(HTTPException) as refused:
        if module.__name__.endswith("dual_app"):
            await routes["/job/start"](request)
        else:
            from fastapi import BackgroundTasks

            await routes["/job/start"](request, BackgroundTasks())

    assert refused.value.status_code == 409
    assert module._current_job_id == JOB_ID
    assert module._current_job_task is None


@pytest.mark.asyncio
async def test_same_job_start_retry_echoes_only_its_exact_projection(worker_runtime):
    module, routes = worker_runtime
    baseline = JobStartRequest(
        job_id=JOB_ID, description="exact accepted projection",
        recipient=_recipient(), pinned_delivery_id=DELIVERY_ID,
        pinned_delivery_proof=DELIVERY_PROOF,
    )
    digest = pinned_job_projection_digest(
        baseline.model_dump(mode="json", exclude_none=True)
    )
    module._orchestrator_client.pinned_delivery_job_id = JOB_ID
    module._orchestrator_client.pinned_delivery_id = DELIVERY_ID
    module._orchestrator_client.pinned_projection_digest = digest
    module._orchestrator_client.pinned_delivery_proof = DELIVERY_PROOF
    request = baseline.model_copy(update={"pinned_projection_digest": digest})

    if module.__name__.endswith("dual_app"):
        accepted = await routes["/job/start"](request)
    else:
        from fastapi import BackgroundTasks

        accepted = await routes["/job/start"](request, BackgroundTasks())

    assert str(accepted.pinned_delivery_id) == DELIVERY_ID
    assert accepted.pinned_projection_digest == digest
    changed = request.model_copy(update={"description": "different projection"})
    with pytest.raises(HTTPException) as refused:
        if module.__name__.endswith("dual_app"):
            await routes["/job/start"](changed)
        else:
            await routes["/job/start"](changed, BackgroundTasks())
    assert refused.value.status_code == 409
