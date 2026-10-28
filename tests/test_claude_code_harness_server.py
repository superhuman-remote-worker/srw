import asyncio
import copy

import httpx
import pytest
from srw_claude_code_harness.server import HarnessState, create_app

LLM = {
    "model": "m",
    "provider": "anthropic",
    "client_protocol": "anthropic-messages",
    "base_url": "https://x/v1",
    "api_key": "k",
}
REMOTE = {
    "host": "10.0.0.5",
    "port": 22,
    "username": "u",
    "key_path": "/k",
    "workspace_path": "/w",
}
RECIPIENT = {
    "expected_agent_id": "a",
    "expected_pod_uid": "pod",
    "expected_process_generation": "g1",
    "expected_job_id": "j1",
}
BODY = {
    "job_id": "j1",
    "description": "go",
    "resolved_config": {"agent": {"llm": LLM, "workspace": {"remote": REMOTE}}},
    "pinned_delivery_id": "d1",
    "pinned_projection_digest": "p1",
    "pinned_delivery_proof": "pr1",
    "recipient": RECIPIENT,
}


@pytest.fixture
def state():
    s = HarnessState(agent_id="a", process_generation="g1", pod_uid="pod")
    s.cancel_wait_seconds = 0.05
    s.runs = []

    async def run_job(spec):
        s.runs.append(spec)

    s.run_job = run_job
    return s


@pytest.fixture
def client(state):
    app = create_app(state)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://h"
    )


def body(**changes):
    b = copy.deepcopy(BODY)
    b.update(changes)
    return b


@pytest.mark.asyncio
async def test_health_and_status(client, state):
    r = await client.get("/health")
    assert r.status_code == 200 and r.json()["agent_id"] == "a"
    assert r.json()["status"] == "healthy"
    s = (await client.get("/status")).json()
    assert s["mode"] == "claude-code" and s["current_job_id"] is None


@pytest.mark.asyncio
async def test_ready_declares_the_harness_capabilities(client, state):
    r = await client.get("/ready")
    assert r.json()["ready"] is True
    assert r.json()["capabilities"] == {
        "pinned_recipient_binding": True,
        "jobs": True,
        "model_protocols": ["anthropic-messages"],
    }
    state.agent_id = None
    assert (await client.get("/ready")).json()["ready"] is False


@pytest.mark.asyncio
async def test_start_accepts_once_and_echoes_the_pinned_fields(client, state):
    r = await client.post("/job/start", json=body())
    assert r.status_code == 202
    assert r.json() == {
        "job_id": "j1",
        "status": "accepted",
        "message": "Job processing started",
        "pinned_delivery_id": "d1",
        "pinned_projection_digest": "p1",
    }
    await asyncio.sleep(0.01)
    assert [s.job_id for s in state.runs] == ["j1"]
    assert state.current.job_id == "j1"


@pytest.mark.asyncio
async def test_start_without_pinned_fields_omits_them(client, state):
    b = body()
    for k in (
        "pinned_delivery_id",
        "pinned_projection_digest",
        "pinned_delivery_proof",
    ):
        del b[k]
    r = await client.post("/job/start", json=b)
    assert r.status_code == 202 and "pinned_delivery_id" not in r.json()


@pytest.mark.asyncio
async def test_start_with_partial_pinned_fields_is_409(client, state):
    b = body()
    del b["pinned_delivery_proof"]
    r = await client.post("/job/start", json=b)
    assert r.status_code == 409 and state.current is None


@pytest.mark.asyncio
async def test_start_is_idempotent_for_the_same_job(client, state):
    first = await client.post("/job/start", json=body())
    second = await client.post("/job/start", json=body())
    assert second.status_code == 202
    assert second.json() == {
        **first.json(),
        "message": "Job was already accepted by this runtime",
    }
    await asyncio.sleep(0.01)
    assert len(state.runs) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "recipient",
    [
        None,
        {**RECIPIENT, "expected_agent_id": "x"},
        {**RECIPIENT, "expected_pod_uid": "other"},
        {**RECIPIENT, "expected_process_generation": "g2"},
        {**RECIPIENT, "expected_job_id": "j2"},
    ],
)
async def test_start_refuses_a_wrong_recipient(client, state, recipient):
    r = await client.post("/job/start", json=body(recipient=recipient))
    assert r.status_code == 409
    assert r.json() == {"detail": "pinned recipient mismatch"}
    assert state.current is None


@pytest.mark.asyncio
async def test_recipient_is_checked_before_the_job_is_judged(client, state):
    b = body(recipient={**RECIPIENT, "expected_agent_id": "x"}, resolved_config={})
    assert (await client.post("/job/start", json=b)).status_code == 409


@pytest.mark.asyncio
async def test_start_refuses_when_busy(client, state):
    assert (await client.post("/job/start", json=body())).status_code == 202
    other = body(job_id="j2", recipient={**RECIPIENT, "expected_job_id": "j2"})
    r = await client.post("/job/start", json=other)
    assert r.status_code == 409 and "busy" in r.json()["detail"]


@pytest.mark.asyncio
async def test_start_is_503_before_registration(client, state):
    state.agent_id = None
    assert (await client.post("/job/start", json=body())).status_code == 503


def _wire_reporter(state):
    state.reports = []

    async def report(job_id, pinned, message):
        state.reports.append((job_id, pinned, message))

    state.report_rejection = report


@pytest.mark.asyncio
async def test_start_accepts_a_non_anthropic_model_and_reports_the_rejection(
    client, state
):
    _wire_reporter(state)
    cfg = copy.deepcopy(BODY["resolved_config"])
    cfg["agent"]["llm"]["client_protocol"] = "openai-chat"
    r = await client.post("/job/start", json=body(resolved_config=cfg))
    assert r.status_code == 202
    assert r.json()["pinned_delivery_id"] == "d1"
    await asyncio.sleep(0.01)
    assert len(state.reports) == 1
    job_id, pinned, message = state.reports[0]
    assert job_id == "j1" and "anthropic-messages" in message
    assert (pinned.delivery_id, pinned.projection_digest, pinned.delivery_proof) == (
        "d1",
        "p1",
        "pr1",
    )
    assert state.runs == []
    again = await client.post("/job/start", json=body(resolved_config=cfg))
    assert again.json()["message"] == "Job was already accepted by this runtime"
    assert len(state.reports) == 1


@pytest.mark.asyncio
async def test_start_accepts_a_job_without_a_workspace_and_reports_it(client, state):
    _wire_reporter(state)
    r = await client.post("/job/start", json=body(resolved_config={}))
    assert r.status_code == 202
    await asyncio.sleep(0.01)
    assert [x[0] for x in state.reports] == ["j1"]
    assert "workspace" in state.reports[0][2] and state.runs == []


@pytest.mark.asyncio
async def test_start_without_a_job_id_stays_422(client, state):
    _wire_reporter(state)
    r = await client.post(
        "/job/start",
        json=body(
            job_id="",
            resolved_config={},
            recipient={**RECIPIENT, "expected_job_id": ""},
        ),
    )
    assert r.status_code == 422 and state.reports == []


@pytest.mark.asyncio
async def test_cancel_without_a_job_is_404_and_changes_nothing(client, state):
    r = await client.post("/job/cancel", json={"recipient": RECIPIENT})
    assert r.status_code == 404
    assert r.json() == {"detail": "No job currently running"}
    assert not state.stop_requested.is_set()


@pytest.mark.asyncio
async def test_cancel_with_wrong_recipient_is_409(client, state):
    await client.post("/job/start", json=body())
    r = await client.post("/job/cancel", json={"recipient": None})
    assert r.status_code == 409 and not state.stop_requested.is_set()


@pytest.mark.asyncio
async def test_cancel_sets_stop_and_reports_graceful(client, state):
    async def run_job(spec):
        await state.stop_requested.wait()
        state.job_finished.set()

    state.run_job = run_job
    await client.post("/job/start", json=body())
    r = await client.post("/job/cancel", json={"recipient": RECIPIENT, "reason": "r"})
    assert r.status_code == 200
    assert r.json() == {
        "job_id": "j1",
        "status": "cancelled",
        "reason": "r",
        "graceful": True,
    }


@pytest.mark.asyncio
async def test_cancel_reports_not_graceful_after_the_wait(client, state):
    async def run_job(spec):
        await asyncio.sleep(5)

    state.run_job = run_job
    await client.post("/job/start", json=body())
    r = await client.post("/job/cancel", json={"recipient": RECIPIENT})
    assert r.json()["graceful"] is False
    assert r.json()["reason"] == "Cancelled by orchestrator"


@pytest.mark.asyncio
async def test_unknown_routes_are_404(client):
    assert (await client.post("/job/pause", json={})).status_code == 404
