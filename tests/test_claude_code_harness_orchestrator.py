import json

import httpx
import pytest
from srw_claude_code_harness.orchestrator import (
    CompletionReport,
    OrchestratorClient,
    PinnedEcho,
    Registration,
)

IMAGE = "registry.example/harness@sha256:" + "a" * 64
KEY = "internal-key"
REG = dict(
    config_name="worker_base",
    pod_ip="10.0.0.2",
    pod_port=8001,
    hostname="p",
    build_sha="s",
    pod_uid="u",
    image=IMAGE,
)


class Orch:
    """MockTransport handler recording every request."""

    def __init__(self, responder=None):
        self.requests = []
        self.responder = responder
        self.registers = 0

    async def __call__(self, request):
        body = json.loads(request.content) if request.content else None
        self.requests.append((request.method, request.url.path, body, request.headers))
        if request.url.path == "/api/agents/register":
            self.registers += 1
            return httpx.Response(
                200,
                json={
                    "agent_id": f"agent-{self.registers}",
                    "heartbeat_interval_seconds": 60,
                    "dispatch_process_generation": f"gen-{self.registers}",
                },
            )
        if self.responder is not None:
            return self.responder(request, body)
        return httpx.Response(200, json={"intents": {"stop": True}})

    def client(self):
        http = httpx.AsyncClient(transport=httpx.MockTransport(self))
        return OrchestratorClient("http://orch:8000", KEY, http=http)


@pytest.mark.asyncio
async def test_register_sends_the_pod_identity_and_keeps_the_generation():
    orch = Orch()
    reg = await orch.client().register(**REG)
    assert reg == Registration("agent-1", 60, "gen-1")
    method, path, body, headers = orch.requests[0]
    assert (method, path) == ("POST", "/api/agents/register")
    assert headers["X-Internal-Key"] == KEY
    assert body["agent_mode"] == "worker"
    assert body["product_provenance"] == {"provenance_status": "unavailable"}
    assert body["image"] == IMAGE and body["pod_uid"] == "u"
    assert body["build_sha"] == "s" and body["pod_port"] == 8001
    assert body["config_name"] == "worker_base" and body["pod_ip"] == "10.0.0.2"
    assert "pid" in body and "thread_id" in body


@pytest.mark.asyncio
async def test_heartbeat_returns_intents_and_reregisters_on_404():
    state = {"n": 0}

    def responder(request, body):
        state["n"] += 1
        return (
            httpx.Response(404)
            if state["n"] == 1
            else httpx.Response(200, json={"intents": {"stop": True}})
        )

    orch = Orch(responder)
    client = orch.client()
    assert client.registration is None
    await client.register(**REG)
    assert client.registration == Registration("agent-1", 60, "gen-1")
    intents = await client.heartbeat("working", current_job_id="j")
    assert intents == {"stop": True}
    assert orch.registers == 2
    # The re-registration is the client's current one.
    assert client.registration == Registration("agent-2", 60, "gen-2")
    beats = [r for r in orch.requests if r[1].endswith("/heartbeat")]
    assert beats[0][1] == "/api/agents/agent-1/heartbeat"
    assert beats[1][1] == "/api/agents/agent-2/heartbeat"
    assert beats[1][2]["status"] == "working" and beats[1][2]["current_job_id"] == "j"
    assert beats[1][3]["X-Internal-Key"] == KEY


@pytest.mark.asyncio
async def test_heartbeat_non_200_returns_empty_and_network_error_is_swallowed():
    orch = Orch(lambda r, b: httpx.Response(500))
    client = orch.client()
    await client.register(**REG)
    assert await client.heartbeat("ready", current_job_id=None) == {}


@pytest.mark.asyncio
async def test_deregister_deletes_and_swallows_errors():
    def boom(request, body):
        raise httpx.ConnectError("down")

    orch = Orch(boom)
    client = orch.client()
    await client.register(**REG)
    await client.deregister()
    assert orch.requests[-1][:2] == ("DELETE", "/api/agents/agent-1")
    assert orch.requests[-1][3]["X-Internal-Key"] == KEY


@pytest.mark.asyncio
async def test_complete_echoes_the_pinned_fields_and_the_agent_id():
    orch = Orch(lambda r, b: httpx.Response(202))
    client = orch.client()
    await client.register(**REG)
    ok = await client.complete(
        "j",
        CompletionReport(True, True, None, {"summary": "s"}),
        pinned=PinnedEcho("d", "sha256:" + "0" * 64, "f" * 64),
        pod_uid="u",
    )
    method, path, posted, headers = orch.requests[-1]
    assert ok and (method, path) == ("POST", "/api/jobs/j/complete")
    assert headers["X-Internal-Key"] == KEY
    assert posted["pinned_process_generation"] == "gen-1"
    assert posted["pinned_pod_uid"] == "u" and posted["agent_id"] == "agent-1"
    assert posted["pinned_delivery_id"] == "d"
    assert posted["pinned_projection_digest"] == "sha256:" + "0" * 64
    assert posted["pinned_delivery_proof"] == "f" * 64
    assert posted["should_stop"] and posted["goal_achieved"]
    assert posted["freeze_data"] == {"summary": "s"} and posted["error"] is None


@pytest.mark.asyncio
async def test_complete_without_pinned_omits_pinned_fields():
    orch = Orch(lambda r, b: httpx.Response(200))
    client = orch.client()
    await client.register(**REG)
    ok = await client.complete(
        "j", CompletionReport(True, False, {"m": "x"}, None), pinned=None, pod_uid="u"
    )
    posted = orch.requests[-1][2]
    assert ok and not any(k.startswith("pinned_") for k in posted)
    assert posted["error"] == {"m": "x"}


@pytest.mark.asyncio
async def test_complete_returns_false_on_http_error_and_network_error():
    orch = Orch(lambda r, b: httpx.Response(409))
    client = orch.client()
    await client.register(**REG)
    report = CompletionReport(True, True, None, None)
    assert await client.complete("j", report, pinned=None, pod_uid="u") is False

    def boom(request, body):
        raise httpx.ConnectError("down")

    orch2 = Orch(boom)
    client2 = orch2.client()
    await client2.register(**REG)
    assert await client2.complete("j", report, pinned=None, pod_uid="u") is False


def _registering(*register_answers):
    """A handler whose heartbeats answer 404 once, then 200, and whose
    registrations answer ``register_answers`` in turn; records each request's
    timeout."""
    state = {"registers": 0, "beats": 0}
    seen = []

    def handler(request):
        seen.append((request.method, request.url.path, request.extensions["timeout"]))
        if request.url.path == "/api/agents/register":
            answer = register_answers[state["registers"]]
            state["registers"] += 1
            return answer
        if request.url.path.endswith("/heartbeat"):
            state["beats"] += 1
            if state["beats"] == 1:
                return httpx.Response(404)
        return httpx.Response(200, json={"intents": {}})

    return handler, seen


def _registered_answer(n):
    return httpx.Response(
        200,
        json={
            "agent_id": f"agent-{n}",
            "heartbeat_interval_seconds": 60,
            "dispatch_process_generation": f"gen-{n}",
        },
    )


@pytest.mark.asyncio
async def test_heartbeat_and_deregister_pass_a_given_timeout():
    """The SIGTERM path leaves with short calls; the re-registration a 404
    heartbeat triggers is held to the same limit."""
    handler, seen = _registering(_registered_answer(1), _registered_answer(2))
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = OrchestratorClient("http://orch:8000", KEY, http=http)
    await client.register(**REG)
    await client.heartbeat("draining", current_job_id=None, timeout=3.0)
    await client.deregister(timeout=3.0)
    short = {"connect": 3.0, "read": 3.0, "write": 3.0, "pool": 3.0}
    assert [(method, path) for method, path, _ in seen[1:]] == [
        ("POST", "/api/agents/agent-1/heartbeat"),
        ("POST", "/api/agents/register"),
        ("POST", "/api/agents/agent-2/heartbeat"),
        ("DELETE", "/api/agents/agent-2"),
    ]
    assert all(timeout == short for _, _, timeout in seen[1:])
    # Without one, the calls keep the default.
    await client.heartbeat("ready", current_job_id=None)
    assert seen[-1][2]["read"] == 30.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(200, json={"agent_id": "agent-2"}),
        httpx.Response(200, content=b"<html>not json</html>"),
        httpx.Response(200, json=["agent-2"]),
    ],
    ids=["missing-key", "not-json", "not-an-object"],
)
async def test_a_malformed_reregistration_is_a_warning_not_a_crash(answer):
    handler, _seen = _registering(_registered_answer(1), answer)
    warnings = []
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = OrchestratorClient(
        "http://orch:8000", KEY, http=http, warn=warnings.append
    )
    await client.register(**REG)
    assert await client.heartbeat("ready", current_job_id=None) == {}
    assert len(warnings) == 1 and "re-registration" in warnings[0]
    # Handled like an HTTP error: the registration it had stays current.
    assert client.registration == Registration("agent-1", 60, "gen-1")
