"""Managed MCP for stdio images end to end, without a cluster (connector drivers D5b).

SRW's real front (drivers/mcp-front) runs in front of SRW's real stdio
bridge (drivers/mcp-bridge), which runs a stdio MCP server
(tests/_fake_stdio_mcp_server.py) once per binding, with a fake lease
exchange on loopback. The agent's own ``MCPManager`` is the client,
configured from a binding exactly as the orchestrator delivers it (the
endpoint URL and a lease token). This proves on one machine what the D5b
k3d gate proves in the cluster: two sessions get two processes in one pod,
each process gets its binding's credential in its environment and nothing
of SRW's, the agent holds only the lease token, ReadOnly hides and refuses
write tools before any process sees them, an execution without the
connector gets 401, a process ends with its binding and serves its
binding's next session with its state (initialized once), the pod runs at
most its cap of processes, and the D5a review's
parsing-differential corpus never reaches a process as a write call: every
line a process reads is in one form every reader agrees on.

The bridge serves the front on a unix socket, as in a pod; without root
every process runs as the test's own user here (``--uid-base 0``): the
bridge's isolation tests (as root) and the k3d gate prove that each binding's
process runs as a user of its own.

Skipped where no Go toolchain is installed (the Python CI); the drivers' own
Go tests run in their CI job.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import http.client
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from agent.tools.mcp import manager as manager_module
from shared.connectors.builtin import MCP_STDIO_TEST_SPEC
from shared.connectors.leases import mint_token
from shared.connectors.mcp import ManagedMcp
from tests.test_managed_mcp_front_integration import (
    CONNECTOR,
    CREDENTIAL,
    OTHER,
    Exchange,
    _connected,
    _free_port,
    _status,
    _text,
    _tool,
)

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "tests" / "_fake_stdio_mcp_server.py"
READ_TOOLS = ["whoami", "notes_read", "leak_credential"]

pytestmark = pytest.mark.skipif(shutil.which("go") is None, reason="no Go toolchain")


@pytest.fixture(scope="module")
def binaries(tmp_path_factory):
    out = tmp_path_factory.mktemp("bin")
    for name, directory in (("front", "mcp-front"), ("bridge", "mcp-bridge")):
        subprocess.run(
            ["go", "build", "-o", str(out / name), "."],
            cwd=ROOT / "drivers" / directory,
            check=True,
            capture_output=True,
            env={**os.environ, "CGO_ENABLED": "0"},
            timeout=600,
        )
    return out


class _SocketConnection(http.client.HTTPConnection):
    """HTTP over a unix socket (the bridge serves no port)."""

    def __init__(self, path: str) -> None:
        super().__init__("srw-mcp-bridge", timeout=5)
        self.path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(5)
        self.sock.connect(self.path)


def _socket_request(path: str, method: str, url: str) -> bytes:
    connection = _SocketConnection(path)
    try:
        connection.request(method, url)
        return connection.getresponse().read()
    finally:
        connection.close()


class StdioPod:
    """The front and the bridge, as one stdio pod runs them: the bridge is
    the server container's command, the stdio server's program follows it."""

    def __init__(
        self, binaries: Path, workdir: Path, exchange: Exchange, *, max_processes=4
    ) -> None:
        self.exchange = exchange
        self.front_port = _free_port()
        # The bridge's socket, in a directory only this user may enter (in a
        # pod: the front's group). Kept short: a socket path has 108 bytes.
        self.socket_dir = Path(tempfile.mkdtemp(prefix="srw-bridge-"))
        self.socket = str(self.socket_dir / "bridge.sock")
        self.processes: list[subprocess.Popen] = []
        self.mcp = ManagedMcp.parse(
            {
                "transport": "stdio",
                "tools": {"read": READ_TOOLS},
                "access": {"ReadOnly": ["read"], "ReadWrite": ["read", "write"]},
                "credential": {"env": "MCP_TOKEN"},
                "max_bindings_per_pod": max_processes,
                "idle_seconds": 600,
            },
            access_levels=["ReadOnly", "ReadWrite"],
            front_port=self.front_port,
        )
        request = {
            "protocol_version": "1.0",
            "plane": "service",
            "driver": MCP_STDIO_TEST_SPEC.name,
            "connector": {"id": CONNECTOR, "config": {}},
            "credentials": {},
            "service": {"port": self.front_port, "port_name": "srw-driver"},
            "exchange": {"url": exchange.url, "identity_file": "identity"},
            "mcp": self.mcp.front_config(socket=self.socket),
        }
        (workdir / "request.json").write_text(json.dumps(request))
        (workdir / "identity").write_text(exchange.identity + "\n")
        self.workdir = workdir
        self.lines = workdir / "lines"
        self.lines.mkdir()
        self.front_log = workdir / "front.log"
        self.bridge_log = workdir / "bridge.log"
        # Outside a pod there is no root: every process runs as this user
        # (--uid-base 0); the bridge's own tests prove the users.
        command = self.mcp.bridge_command(
            [sys.executable, str(SERVER)],
            socket=self.socket,
            socket_group=None,
            uid_base=0,
            home_root=str(workdir / "home"),
        )
        self.command = [str(binaries / "bridge"), *command[1:]]
        self.front = binaries / "front"

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.front_port}/mcp"

    def start(self) -> None:
        with self.bridge_log.open("ab") as log:
            self.processes.append(
                subprocess.Popen(
                    self.command,
                    # The container's environment: the image's, the spec's
                    # and what no process may see as its own.
                    env={
                        "PATH": os.environ.get("PATH", ""),
                        "FAKE_STDIO_LOG_DIR": str(self.lines),
                        "SRW_NEVER_IN_A_PROCESS": "x",
                        "MCP_TOKEN": "a-value-from-the-pod",
                    },
                    stdout=subprocess.DEVNULL,
                    stderr=log,
                )
            )
        with self.front_log.open("ab") as log:
            self.processes.append(
                subprocess.Popen(
                    [str(self.front), "serve"],
                    env={
                        "SRW_REQUEST_FILE": str(self.workdir / "request.json"),
                        "SRW_DRIVER_IDENTITY_FILE": str(self.workdir / "identity"),
                        "SRW_DRIVER_PORT": str(self.front_port),
                    },
                    stdout=log,
                    stderr=log,
                )
            )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{self.front_port}/readyz", timeout=2
                ) as response:
                    if response.status == 200:
                        return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError("the pod never became ready")

    def stop(self) -> None:
        for process in self.processes:
            process.terminate()
        for process in self.processes:
            process.wait(timeout=20)
        self.processes.clear()
        shutil.rmtree(self.socket_dir, ignore_errors=True)

    def status(self) -> dict:
        """GET /srw/status on the bridge's socket (the front's view)."""
        return json.loads(_socket_request(self.socket, "GET", "/srw/status"))

    def processes_by_binding(self) -> dict[str, int]:
        return {
            item["binding"]: item["pid"]
            for item in self.status()["processes"]
            if not item.get("probe")
        }

    def received(self) -> list[bytes]:
        """Every line any process of the server read, as it read it."""
        out: list[bytes] = []
        for log in sorted(self.lines.glob("*.log")):
            out += log.read_bytes().splitlines()
        return out


@pytest.fixture
def pod(binaries, tmp_path):
    exchange = Exchange(mint_token("sdi"))
    running = StdioPod(binaries, tmp_path, exchange)
    running.start()
    try:
        yield running
    finally:
        running.stop()


def _entry(pod: StdioPod, token: str) -> dict:
    """A managed MCP binding as the orchestrator delivers it."""
    return {
        "type": MCP_STDIO_TEST_SPEC.legacy_type,
        "name": "Memory",
        "description": None,
        "connection_url": pod.url,
        "credentials": {
            "lease": {"id": "lease", "connector_id": CONNECTOR, "token": token}
        },
        "project_read_only": False,
        "datasource_id": CONNECTOR,
        "config": {},
    }


def _lease_id(pod: StdioPod, token: str) -> str:
    return pod.exchange.leases[token]["lease_id"]


def _alive(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except OSError:
        return False
    return state != "Z"


async def _until(predicate, timeout=15.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(0.1)
    return True


@pytest.mark.asyncio
async def test_two_sessions_get_two_processes_in_one_pod(pod):
    tokens = [pod.exchange.issue("ReadWrite"), pod.exchange.issue("ReadWrite")]
    managers = [await _connected(_entry(pod, token)) for token in tokens]
    try:
        answers = []
        for manager in managers:
            assert manager.statuses == {"Memory": "connected"}
            answers.append(json.loads(await _text(_tool(manager, "whoami"))))
        first, second = answers
        assert first["pid"] != second["pid"]
        # Each process got the connector's credential from the front, in its
        # own environment; never the pod's value, never SRW's variables.
        digest = hashlib.sha256(CREDENTIAL.encode()).hexdigest()
        assert first["credential_sha256"] == second["credential_sha256"] == digest
        assert first["srw_env"] == second["srw_env"] == []
        # One process per binding, named by its lease, in the one pod.
        assert pod.processes_by_binding() == {
            _lease_id(pod, tokens[0]): first["pid"],
            _lease_id(pod, tokens[1]): second["pid"],
        }
        # Each process keeps its own state.
        again = json.loads(await _text(_tool(managers[0], "whoami")))
        assert again["pid"] == first["pid"] and again["calls"] == 2
        # The client held only its lease token.
        for manager, token in zip(managers, tokens, strict=True):
            assert CREDENTIAL not in json.dumps(manager._handles[0].ds)
            assert token in json.dumps(manager._handles[0].ds)
    finally:
        for manager in managers:
            await manager.aclose()


@pytest.mark.asyncio
async def test_read_only_hides_write_tools_before_any_process_sees_them(pod):
    manager = await _connected(_entry(pod, pod.exchange.issue("ReadOnly")))
    try:
        names = {t.metadata["mcp_tool_name"] for t in manager.get_langchain_tools()}
        assert names == set(READ_TOOLS)
        from mcp.shared.exceptions import McpError

        with pytest.raises(McpError) as refused:
            await manager._handles[0].session.call_tool("notes_write", {})
        assert "Unknown tool" in str(refused.value)
        assert manager.statuses == {"Memory": "connected"}
        assert not any(b"notes_write" in line for line in pod.received())
        assert ("/v1/leases/exchange", "write") not in pod.exchange.calls
    finally:
        await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["unknown", "other_connector"])
async def test_an_execution_without_the_connector_gets_401(pod, kind):
    if kind == "unknown":
        token = mint_token("scl")
    else:
        token = pod.exchange.issue("ReadWrite", connector=OTHER)
    assert _status(pod.url, token) == 401
    manager = await _connected(_entry(pod, token))
    try:
        assert manager.statuses["Memory"] == (
            f"unavailable: {manager_module.LEASE_REFUSED}"
        )
        # No process was started for it.
        assert pod.processes_by_binding() == {}
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_a_process_ends_with_its_binding(pod):
    # The front keeps a live decision for up to 30 s, never past the
    # lease's expiry: a lease expiring soon shows the revocation at once.
    soon = (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat()
    ending = pod.exchange.issue("ReadWrite", expires_at=soon)
    staying = pod.exchange.issue("ReadWrite")
    gone_manager = await _connected(_entry(pod, ending))
    kept_manager = await _connected(_entry(pod, staying))
    try:
        gone_pid = json.loads(await _text(_tool(gone_manager, "whoami")))["pid"]
        kept_pid = json.loads(await _text(_tool(kept_manager, "whoami")))["pid"]
        del pod.exchange.leases[ending]
        await asyncio.sleep(2.5)
        # The next call is refused, and the binding's process stops with it.
        answer = await _text(_tool(gone_manager, "whoami"))
        assert manager_module.LEASE_REFUSED in answer
        assert await _until(lambda: not _alive(gone_pid)), (
            "the process outlived its lease"
        )
        assert _lease_id(pod, staying) in pod.processes_by_binding()
        assert _alive(kept_pid)
        assert "its binding ended" in pod.bridge_log.read_text()
    finally:
        await gone_manager.aclose()
        await kept_manager.aclose()


@pytest.mark.asyncio
async def test_a_process_serves_its_bindings_next_session_with_its_state(pod):
    """The agent opens a session each time it attaches the connector (a
    stateless session's turns may run on different agent pods): the
    binding's process, and the state it keeps, serve the next one."""
    token = pod.exchange.issue("ReadWrite")
    first = await _connected(_entry(pod, token))
    before = json.loads(await _text(_tool(first, "whoami")))
    await first.aclose()
    assert _alive(before["pid"])
    second = await _connected(_entry(pod, token))
    try:
        after = json.loads(await _text(_tool(second, "whoami")))
        assert after["pid"] == before["pid"] and after["calls"] == before["calls"] + 1
        assert pod.processes_by_binding() == {_lease_id(pod, token): before["pid"]}
        # The server was initialized once.
        log = next(pod.lines.glob(f"{before['pid']}.log")).read_bytes()
        assert log.count(b'"method":"initialize"') == 1
    finally:
        await second.aclose()


@pytest.mark.asyncio
async def test_the_credential_reaches_neither_the_agent_nor_a_log(pod):
    token = pod.exchange.issue("ReadWrite")
    manager = await _connected(_entry(pod, token))
    try:
        leaked = await _text(_tool(manager, "leak_credential"))
        assert CREDENTIAL not in leaked and "[redacted]" in leaked
    finally:
        await manager.aclose()
    assert await _until(lambda: "my credential is" in pod.bridge_log.read_text())
    for log in (pod.front_log, pod.bridge_log):
        text = log.read_text()
        assert CREDENTIAL not in text and token not in text
    assert "my credential is [redacted]" in pod.bridge_log.read_text()


@pytest.mark.asyncio
async def test_a_crashed_process_is_replaced_when_the_client_reconnects(pod):
    manager = await _connected(_entry(pod, pod.exchange.issue("ReadWrite")))
    try:
        whoami, crash = _tool(manager, "whoami"), _tool(manager, "crash")
        first = json.loads(await _text(whoami))["pid"]
        began = time.monotonic()
        # The call the process died on is answered at once as a session
        # that ended: the client reconnects (and retries it once, on a
        # process that dies too) instead of waiting for its call timeout.
        assert "MCP tool error" in await _text(crash)
        assert time.monotonic() - began < manager_module.MCP_CALL_TIMEOUT / 2
        assert await _until(lambda: not _alive(first))
        answer = json.loads(await _text(whoami))
        assert answer["pid"] != first
        assert 1 <= len(manager._handles[0].reconnects) <= 3
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_the_pod_runs_at_most_its_cap_of_binding_processes(binaries, tmp_path):
    exchange = Exchange(mint_token("sdi"))
    pod = StdioPod(binaries, tmp_path, exchange, max_processes=2)
    pod.start()
    managers = []
    try:
        for _ in range(2):
            managers.append(await _connected(_entry(pod, exchange.issue("ReadWrite"))))
        refused = await _connected(_entry(pod, exchange.issue("ReadWrite")))
        managers.append(refused)
        assert refused.statuses["Memory"].startswith("unavailable")
        assert len(pod.processes_by_binding()) == 2
    finally:
        for manager in managers:
            await manager.aclose()
        pod.stop()


def _raw(pod: StdioPod, token: str, body: bytes, session: str | None = None):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "Authorization": f"Bearer {token}",
    }
    if session:
        headers["Mcp-Session-Id"] = session
    request = urllib.request.Request(pod.url, data=body, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return (
                response.status,
                response.headers.get("Mcp-Session-Id"),
                response.read().decode("utf-8", "replace"),
            )
    except urllib.error.HTTPError as error:
        return error.code, None, error.read().decode("utf-8", "replace")


def _corpus() -> list[bytes]:
    shared = json.loads(
        (ROOT / "drivers/mcp-front/testdata/bypass_corpus.json").read_text()
    )
    return [body.encode() for body in shared["bodies"]] + [
        base64.b64decode(body) for body in shared["bodies_base64"]
    ]


def _readings(line: bytes) -> set[tuple[str, str]]:
    """What a stdio server may read in a line: the last of a duplicate key
    (Python's json, a map), and the first (a streaming parser)."""
    last = json.loads(line)
    first = json.loads(line, object_pairs_hook=lambda pairs: dict(reversed(pairs)))

    def call(message):
        params = message.get("params") if isinstance(message, dict) else None
        name = params.get("name") if isinstance(params, dict) else None
        return (message.get("method"), name)

    return {call(last), call(first)}


def test_the_review_corpus_never_reaches_a_process_as_a_write_call(pod):
    """Every corpus body through the front and the bridge with a ReadOnly
    lease: none runs notes_write, and every line a process read is in one
    form every reader agrees on."""
    token = pod.exchange.issue("ReadOnly")
    status, session, _ = _raw(
        pod,
        token,
        b'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":'
        b'"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}',
    )
    assert status == 200 and session
    _raw(pod, token, b'{"jsonrpc":"2.0","method":"notifications/initialized"}', session)
    for body in _corpus():
        status, _, answer = _raw(pod, token, body, session)
        assert "called notes_write" not in answer, body
    lines = pod.received()
    assert lines, "nothing reached a process"
    for line in lines:
        readings = _readings(line)
        assert len(readings) == 1, line
        assert ("tools/call", "notes_write") not in readings, line
    # The well-formed read call of the corpus did reach the process.
    assert any(_readings(line) == {("tools/call", "whoami")} for line in lines)
    assert ("/v1/leases/exchange", "write") not in pod.exchange.calls


def _stdio_gate():
    import importlib.util

    name = "k3d_managed_mcp_stdio_gate"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "scripts" / "k3d-managed-mcp-stdio-gate.py"
    )
    gate = importlib.util.module_from_spec(spec)
    sys.modules[name] = gate
    spec.loader.exec_module(gate)
    return gate


def test_the_gates_corpus_program_and_verdict_hold_against_the_real_pod(pod):
    """The k3d gate's raw-body program and corpus verdict, here against the
    real front and bridge: the corpus (named for this server's tools) and
    the well-formed write control, with a ReadOnly lease."""
    gate = _stdio_gate()
    bodies = gate.corpus_bodies("notes_write", "whoami") + [
        gate.control_body("notes_write", "d5b-0123456789-bypass")
    ]
    payload = {
        "url": pod.url,
        "bearer": pod.exchange.issue("ReadOnly"),
        "bodies": [base64.b64encode(body).decode() for body in bodies],
    }
    done = subprocess.run(
        [sys.executable, "-c", gate._RAW_PROGRAM],
        input=json.dumps(payload) + "\n",
        capture_output=True,
        text=True,
        timeout=180,
    )
    result = json.loads(done.stdout.splitlines()[-1])
    ok, detail = gate.corpus_verdict(result, sent=len(bodies), write_tool="notes_write")
    assert ok, detail
    # (One body is a whoami call whose arguments name notes_write.)
    for line in pod.received():
        assert ("tools/call", "notes_write") not in _readings(line), line
