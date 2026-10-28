"""The entrypoint as SRW sees it: register, heartbeat, one job over HTTP, report, exit.

The orchestrator is an httpx ``MockTransport``; ``/job/start`` and ``/job/cancel``
go through the harness's own ASGI app; the workspace is the local sshd; Claude
Code is ``FakeClient``.
"""

import asyncio
import json
import os
import signal
import socket
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from srw_claude_code_harness import __main__ as entry
from srw_claude_code_harness.server import HarnessState, create_app
from tests._claude_code_harness_fakes import (
    AGENT_ID,
    API_KEY,
    GENERATION,
    INTERNAL_KEY,
    IMAGE,
    JOB_ID,
    POD_UID,
    PROOF,
    SECRETS,
    call_tool,
    connect_via,
    fake_client_factory,
    never_connect,
    recipient,
    remote_for,
    result_message,
    sdk_or_skip,
    start_body,
    until,
)
from tests._claude_code_harness_sshd import LocalSshd

SETTINGS = entry.Settings(
    orchestrator_url="http://orch:8000",
    internal_key=INTERNAL_KEY,
    port=8001,
    pod_uid=POD_UID,
    build_sha="abc1234",
    image=IMAGE,
    hostname="harness-pod",
    pod_ip="10.0.0.2",
    config_name="worker_base",
    path=os.environ.get("PATH", "/usr/bin:/bin"),
)


class Orch:
    """MockTransport handler standing in for the orchestrator; records every call."""

    def __init__(self, *, intents=None, complete_status=200, register_failures=0):
        self.requests = []
        self.intents = intents or (lambda body: {})
        self.complete_status = complete_status
        self.register_failures = register_failures
        self.ready = asyncio.Event()
        self.working = asyncio.Event()

    async def __call__(self, request):
        body = json.loads(request.content) if request.content else None
        path = request.url.path
        self.requests.append((request.method, path, body))
        assert request.headers["X-Internal-Key"] == INTERNAL_KEY
        if path == "/api/agents/register":
            if self.register_failures:
                self.register_failures -= 1
                return httpx.Response(503, json={"detail": "starting"})
            return httpx.Response(
                200,
                json={
                    "agent_id": AGENT_ID,
                    "heartbeat_interval_seconds": 60,
                    "dispatch_process_generation": GENERATION,
                },
            )
        if path.endswith("/heartbeat"):
            if body["status"] == "ready":
                self.ready.set()
            if body["status"] == "working":
                self.working.set()
            return httpx.Response(200, json={"intents": self.intents(body)})
        if path.endswith("/complete"):
            return httpx.Response(self.complete_status, json={})
        if request.method == "DELETE":
            return httpx.Response(200, json={"status": "deleted"})
        return httpx.Response(404)

    def calls(self):
        """``(method, path, status)`` per request; status for heartbeats only."""
        return [
            (method, path, (body or {}).get("status") if "heartbeat" in path else None)
            for method, path, body in self.requests
        ]

    def heartbeats(self):
        return [body for _, path, body in self.requests if path.endswith("/heartbeat")]

    def completions(self):
        return [body for _, path, body in self.requests if path.endswith("/complete")]


@pytest_asyncio.fixture
async def sshd(tmp_path):
    server = await LocalSshd.start(tmp_path / "workspace")
    yield server
    await server.close()


class Running:
    def __init__(self, task, app, http, terminate):
        self.task = task
        self.app = app
        self.http = http
        self.terminate = terminate
        self.harness = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://harness"
        )

    async def exit_code(self, timeout=30):
        try:
            return await asyncio.wait_for(asyncio.shield(self.task), timeout)
        finally:
            await self.harness.aclose()
            await self.http.aclose()


async def _launch(tmp_path, orch, *, wait_ready=True, **kwargs):
    apps = []

    async def start_server(app, port):
        assert port == SETTINGS.port
        apps.append(app)

        async def close():
            pass

        return close

    http = httpx.AsyncClient(transport=httpx.MockTransport(orch))
    terminate = asyncio.Event()
    kwargs.setdefault("connect", never_connect([]))
    task = asyncio.create_task(
        entry._serve(
            SETTINGS,
            terminate=terminate,
            run_dir=tmp_path / "run",
            http=http,
            start_server=start_server,
            **kwargs,
        )
    )
    await asyncio.wait_for(until(lambda: apps), 10)
    if wait_ready:
        await asyncio.wait_for(orch.ready.wait(), 10)
    return Running(task, apps[0], http, terminate)


def _assert_no_secrets(text):
    for secret in SECRETS:
        assert secret not in text


@pytest.mark.asyncio
async def test_a_job_runs_end_to_end_and_reports_completion(tmp_path, sshd, capsys):
    sdk_or_skip()
    orch, clients, connects = Orch(), [], []

    async def write_the_report(options):
        await call_tool(
            options, "Write", {"file_path": "out/report.md", "content": "# Done\n"}
        )

    running = await _launch(
        tmp_path,
        orch,
        connect=connect_via(sshd, connects),
        client_factory=fake_client_factory(
            clients, [result_message(result="All done.")], on_query=write_the_report
        ),
    )
    response = await running.harness.post(
        "/job/start", json=start_body(remote_for(sshd.access))
    )
    assert response.status_code == 202, response.text
    assert await running.exit_code() == entry.EXIT_OK

    # register, ready at once, working at once on accept, the report, draining, gone.
    assert orch.calls() == [
        ("POST", "/api/agents/register", None),
        ("POST", f"/api/agents/{AGENT_ID}/heartbeat", "ready"),
        ("POST", f"/api/agents/{AGENT_ID}/heartbeat", "working"),
        ("POST", f"/api/jobs/{JOB_ID}/complete", None),
        ("POST", f"/api/agents/{AGENT_ID}/heartbeat", "draining"),
        ("DELETE", f"/api/agents/{AGENT_ID}", None),
    ]
    register = orch.requests[0][2]
    assert register["image"] == IMAGE and register["pod_uid"] == POD_UID
    assert register["pod_ip"] == "10.0.0.2" and register["pod_port"] == 8001
    assert register["build_sha"] == "abc1234" and register["hostname"] == "harness-pod"
    assert register["config_name"] == "worker_base"
    assert orch.heartbeats()[1]["current_job_id"] == JOB_ID

    [complete] = orch.completions()
    root = sshd.access.root
    assert complete["should_stop"] is True and complete["goal_achieved"] is True
    assert complete["error"] is None and complete["agent_id"] == AGENT_ID
    freeze = complete["freeze_data"]
    assert freeze["summary"] == "All done."
    assert freeze["deliverables"] == [f"{root}/out/report.md"]
    assert freeze["status"] == "job_completed" and freeze["job_id"] == JOB_ID
    assert freeze["model"] == "claude-test"
    assert complete["pinned_delivery_id"] == "delivery-1"
    assert complete["pinned_projection_digest"] == "digest-1"
    assert complete["pinned_delivery_proof"] == PROOF
    assert complete["pinned_process_generation"] == GENERATION
    assert complete["pinned_pod_uid"] == POD_UID

    assert (Path(root) / "out/report.md").read_text() == "# Done\n"
    assert (Path(root) / ".srw/claude-code/transcript.jsonl").is_file()
    [access] = connects
    assert (access.host, access.port, access.user) == (
        sshd.access.host,
        sshd.access.port,
        sshd.access.user,
    )
    assert access.private_key_file == sshd.access.private_key_file
    [client] = clients
    assert client.prompts == ["Write the report."]
    assert client.options.env["ANTHROPIC_API_KEY"] == API_KEY
    assert client.options.env["ANTHROPIC_BASE_URL"] == "https://gateway.example"
    out = capsys.readouterr()
    assert "[srw-harness]" in out.out
    _assert_no_secrets(out.out + out.err)


@pytest.mark.asyncio
async def test_a_finished_pod_refuses_a_second_job(tmp_path, sshd):
    sdk_or_skip()
    orch, clients = Orch(), []
    running = await _launch(
        tmp_path,
        orch,
        connect=connect_via(sshd, []),
        client_factory=fake_client_factory(clients, [result_message()]),
    )
    first = await running.harness.post(
        "/job/start", json=start_body(remote_for(sshd.access))
    )
    assert first.status_code == 202
    assert await running.exit_code() == entry.EXIT_OK
    harness = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=running.app), base_url="http://harness"
    )
    async with harness:
        second = await harness.post(
            "/job/start", json=start_body(remote_for(sshd.access), job_id="job-2")
        )
        ready = (await harness.get("/ready")).json()
    assert second.status_code >= 400
    assert ready["ready"] is False
    assert len(clients) == 1


@pytest.mark.asyncio
async def test_a_non_anthropic_model_completes_with_a_clear_error(tmp_path, capsys):
    orch, connects = Orch(), []
    running = await _launch(tmp_path, orch, connect=never_connect(connects))
    response = await running.harness.post(
        "/job/start", json=start_body(protocol="openai-responses")
    )
    assert response.status_code == 202
    assert await running.exit_code() == entry.EXIT_OK
    [complete] = orch.completions()
    assert complete["goal_achieved"] is False and complete["freeze_data"] is None
    assert complete["error"]["type"] == "job_error"
    assert complete["error"]["recoverable"] is False
    assert "anthropic-messages" in complete["error"]["message"]
    assert "openai-responses" in complete["error"]["message"]
    assert complete["pinned_delivery_proof"] == PROOF
    assert connects == []
    assert orch.calls()[-2:] == [
        ("POST", f"/api/agents/{AGENT_ID}/heartbeat", "draining"),
        ("DELETE", f"/api/agents/{AGENT_ID}", None),
    ]
    out = capsys.readouterr()
    _assert_no_secrets(out.out + out.err)


@pytest.mark.asyncio
async def test_cancel_stops_the_session_and_sends_no_completion(tmp_path, sshd):
    sdk_or_skip()
    orch, clients = Orch(), []
    running = await _launch(
        tmp_path,
        orch,
        connect=connect_via(sshd, []),
        client_factory=fake_client_factory(clients, [], hang=True),
    )
    started = await running.harness.post(
        "/job/start", json=start_body(remote_for(sshd.access))
    )
    assert started.status_code == 202
    await asyncio.wait_for(until(lambda: clients and clients[0].queried.is_set()), 10)
    cancelled = await running.harness.post(
        "/job/cancel", json={"recipient": recipient(), "reason": "user"}
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"
    assert cancelled.json()["graceful"] is True
    assert await running.exit_code() == entry.EXIT_OK
    assert clients[0].interrupted
    assert orch.completions() == []
    assert orch.calls()[-2:] == [
        ("POST", f"/api/agents/{AGENT_ID}/heartbeat", "draining"),
        ("DELETE", f"/api/agents/{AGENT_ID}", None),
    ]


@pytest.mark.asyncio
async def test_should_drain_while_idle_deregisters_and_exits(tmp_path):
    orch = Orch(intents=lambda body: {"should_drain": True, "drain_reason": "x"})
    running = await _launch(tmp_path, orch)
    assert await running.exit_code() == entry.EXIT_OK
    assert orch.calls() == [
        ("POST", "/api/agents/register", None),
        ("POST", f"/api/agents/{AGENT_ID}/heartbeat", "ready"),
        ("POST", f"/api/agents/{AGENT_ID}/heartbeat", "draining"),
        ("DELETE", f"/api/agents/{AGENT_ID}", None),
    ]


@pytest.mark.asyncio
async def test_should_drain_while_a_job_runs_is_ignored(tmp_path, sshd):
    sdk_or_skip()
    clients = []
    orch = Orch(intents=lambda body: {"should_drain": body["status"] == "working"})

    async def wait_for_the_working_heartbeat(options):
        await asyncio.wait_for(orch.working.wait(), 10)
        await asyncio.sleep(0.05)  # its answer (the drain intent) is handled

    running = await _launch(
        tmp_path,
        orch,
        connect=connect_via(sshd, []),
        client_factory=fake_client_factory(
            clients, [result_message()], on_query=wait_for_the_working_heartbeat
        ),
    )
    response = await running.harness.post(
        "/job/start", json=start_body(remote_for(sshd.access))
    )
    assert response.status_code == 202
    assert await running.exit_code() == entry.EXIT_OK
    [complete] = orch.completions()
    assert complete["goal_achieved"] is True


@pytest.mark.asyncio
async def test_no_job_within_the_idle_limit_deregisters_and_exits(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(entry, "IDLE_EXIT_SECONDS", 0.2)
    orch = Orch()
    running = await _launch(tmp_path, orch)
    assert await running.exit_code(timeout=10) == entry.EXIT_OK
    assert orch.calls() == [
        ("POST", "/api/agents/register", None),
        ("POST", f"/api/agents/{AGENT_ID}/heartbeat", "ready"),
        ("POST", f"/api/agents/{AGENT_ID}/heartbeat", "draining"),
        ("DELETE", f"/api/agents/{AGENT_ID}", None),
    ]
    # The pod refuses a start once it has decided to go.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=running.app), base_url="http://h"
    ) as harness:
        response = await harness.post("/job/start", json=start_body())
    assert response.status_code >= 400


@pytest.mark.asyncio
async def test_registration_is_retried_until_it_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr(entry, "REGISTER_RETRY_SECONDS", 0.01)
    monkeypatch.setattr(entry, "IDLE_EXIT_SECONDS", 0.05)
    orch = Orch(register_failures=2)
    running = await _launch(tmp_path, orch)
    assert await running.exit_code(timeout=10) == entry.EXIT_OK
    registers = [c for c in orch.calls() if c[1] == "/api/agents/register"]
    assert len(registers) == 3


@pytest.mark.asyncio
async def test_a_signal_before_registration_exits_4(tmp_path, monkeypatch):
    monkeypatch.setattr(entry, "REGISTER_RETRY_SECONDS", 60)
    orch = Orch(register_failures=1)
    running = await _launch(tmp_path, orch, wait_ready=False)
    await asyncio.wait_for(until(lambda: orch.requests), 5)
    running.terminate.set()
    assert await running.exit_code(timeout=5) == entry.EXIT_INTERRUPTED
    assert [c[1] for c in orch.calls()] == ["/api/agents/register"]


@pytest.mark.asyncio
async def test_a_signal_during_a_job_stops_it_without_a_report_and_exits_4(
    tmp_path, sshd
):
    sdk_or_skip()
    orch, clients = Orch(), []
    running = await _launch(
        tmp_path,
        orch,
        connect=connect_via(sshd, []),
        client_factory=fake_client_factory(clients, [], hang=True),
    )
    response = await running.harness.post(
        "/job/start", json=start_body(remote_for(sshd.access))
    )
    assert response.status_code == 202
    await asyncio.wait_for(until(lambda: clients and clients[0].queried.is_set()), 10)
    running.terminate.set()
    assert await running.exit_code() == entry.EXIT_INTERRUPTED
    assert clients[0].interrupted
    assert orch.completions() == []
    assert orch.calls()[-2:] == [
        ("POST", f"/api/agents/{AGENT_ID}/heartbeat", "draining"),
        ("DELETE", f"/api/agents/{AGENT_ID}", None),
    ]


@pytest.mark.asyncio
async def test_a_runner_exception_is_reported_as_a_job_error_and_exits_1(
    tmp_path, capsys
):
    async def connect(access):
        raise RuntimeError(f"boom {API_KEY}")

    orch = Orch()
    running = await _launch(tmp_path, orch, connect=connect)
    response = await running.harness.post("/job/start", json=start_body())
    assert response.status_code == 202
    assert await running.exit_code() == entry.EXIT_HARNESS_ERROR
    [complete] = orch.completions()
    assert complete["goal_achieved"] is False
    assert complete["error"]["type"] == "job_error"
    assert "RuntimeError: boom" in complete["error"]["message"]
    assert API_KEY not in json.dumps(complete["error"])
    assert orch.calls()[-1] == ("DELETE", f"/api/agents/{AGENT_ID}", None)
    out = capsys.readouterr()
    _assert_no_secrets(out.out + out.err)


@pytest.mark.asyncio
async def test_a_refused_completion_report_exits_1(tmp_path):
    orch = Orch(complete_status=500)
    running = await _launch(tmp_path, orch)
    response = await running.harness.post(
        "/job/start", json=start_body(protocol="openai-chat")
    )
    assert response.status_code == 202
    assert await running.exit_code() == entry.EXIT_HARNESS_ERROR
    assert len(orch.completions()) == 1
    assert orch.calls()[-1] == ("DELETE", f"/api/agents/{AGENT_ID}", None)


@pytest.mark.asyncio
async def test_the_http_server_serves_the_app_and_stops():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    state = HarnessState(agent_id="a", pod_uid="p")
    close = await entry._start_uvicorn(create_app(state), port)
    try:
        async with httpx.AsyncClient() as client:
            health = await client.get(f"http://127.0.0.1:{port}/health")
        assert health.status_code == 200 and health.json()["agent_id"] == "a"
    finally:
        await close()
    with pytest.raises(httpx.ConnectError):
        async with httpx.AsyncClient() as client:
            await client.get(f"http://127.0.0.1:{port}/health")


def _no_scrub(monkeypatch, calls):
    def scrub(keep):
        calls.append(("scrub", list(keep)))
        return {}

    monkeypatch.setattr(entry, "scrub_process_environment", scrub)


@pytest.mark.parametrize("missing", ["ORCHESTRATOR_URL", "MCP_INTERNAL_KEY"])
def test_main_exits_2_without_the_orchestrator_settings(monkeypatch, capsys, missing):
    calls = []
    _no_scrub(monkeypatch, calls)
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://orch:8000")
    monkeypatch.setenv("MCP_INTERNAL_KEY", INTERNAL_KEY)
    monkeypatch.delenv(missing)
    assert entry.main([]) == entry.EXIT_CONFIG_ERROR == 2
    out = capsys.readouterr()
    assert missing in out.out + out.err
    assert calls == []


def test_main_exits_2_on_an_invalid_port(monkeypatch, capsys):
    calls = []
    _no_scrub(monkeypatch, calls)
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://orch:8000")
    monkeypatch.setenv("MCP_INTERNAL_KEY", INTERNAL_KEY)
    monkeypatch.setenv("AGENT_PORT", "eighty")
    assert entry.main([]) == 2
    out = capsys.readouterr()
    assert "AGENT_PORT" in out.out + out.err
    assert calls == []


def test_main_reads_the_pod_environment_scrubs_it_and_stops_on_sigterm(
    monkeypatch, tmp_path
):
    calls = []
    _no_scrub(monkeypatch, calls)
    for name, value in {
        "ORCHESTRATOR_URL": "http://orch:8000",
        "MCP_INTERNAL_KEY": INTERNAL_KEY,
        "AGENT_PORT": "8123",
        "POD_UID": POD_UID,
        "BUILD_SHA": "",
        "AGENT_IMAGE": IMAGE,
        "HOSTNAME": "harness-pod",
        "AGENT_POD_IP": "10.1.2.3",
        "AGENT_CONFIG": "worker_base",
    }.items():
        monkeypatch.setenv(name, value)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    monkeypatch.setattr(entry.tempfile, "mkdtemp", lambda **kwargs: str(run_dir))

    async def fake_serve(settings, *, terminate, run_dir):
        calls.append(("serve", settings, run_dir))
        os.kill(os.getpid(), signal.SIGTERM)
        try:
            await asyncio.wait_for(terminate.wait(), 5)
        except asyncio.TimeoutError:
            return 99
        return entry.EXIT_INTERRUPTED

    monkeypatch.setattr(entry, "_serve", fake_serve)
    # Guard: without the harness's handler, SIGTERM must not kill pytest.
    previous = signal.signal(signal.SIGTERM, lambda *args: None)
    try:
        assert entry.main([]) == entry.EXIT_INTERRUPTED == 4
    finally:
        signal.signal(signal.SIGTERM, previous)
    (scrub, keep), (name, settings, given_run_dir) = calls
    assert (scrub, keep, name) == ("scrub", ["PATH"], "serve")
    assert given_run_dir == run_dir
    assert settings.orchestrator_url == "http://orch:8000"
    assert settings.internal_key == INTERNAL_KEY
    assert settings.port == 8123 and settings.pod_uid == POD_UID
    assert settings.build_sha == "" and settings.image == IMAGE
    assert settings.hostname == "harness-pod" and settings.pod_ip == "10.1.2.3"
    assert settings.config_name == "worker_base"
    assert INTERNAL_KEY not in repr(settings)


def test_settings_default_the_port_and_detect_the_pod_ip():
    settings = entry._settings(
        {"ORCHESTRATOR_URL": "http://orch:8000", "MCP_INTERNAL_KEY": INTERNAL_KEY}
    )
    assert settings.port == 8001
    assert settings.config_name == "worker_base"
    assert settings.pod_ip.count(".") == 3
    assert settings.hostname
