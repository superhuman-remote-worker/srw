"""The managed MCP front end to end, without a cluster (connector drivers D5a).

SRW's real front (drivers/mcp-front) runs in front of the real development MCP
server (drivers/mcp-test), with a fake lease exchange on loopback. The agent's
own ``MCPManager`` is the client, configured from a payload entry exactly as a
binding delivers it (the endpoint URL and a lease token). This proves on one
machine what the k3d gate proves in the cluster: the client holds only the
lease token, the server receives the upstream credential, ReadOnly hides and
refuses write tools, a lease of another connector gets 401, the credential is
scrubbed from answers, a pod replaced mid-session reconnects, and a lease
revoked mid-session is reported to the agent as such.

Skipped where no Go toolchain is installed (the Python CI); the drivers' own
Go tests run in their CI job.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from agent.tools.mcp import manager as manager_module
from agent.tools.mcp.manager import MCPManager
from shared.connectors.builtin import MCP_TEST_SPEC
from shared.connectors.leases import mint_token
from shared.connectors.mcp import managed_mcp

ROOT = Path(__file__).resolve().parents[1]
CONNECTOR = "66666666-7777-4888-8999-aaaaaaaaaaaa"
OTHER = "11111111-2222-4333-8444-555555555555"
CREDENTIAL = "upstream-token-" + "x" * 24

pytestmark = pytest.mark.skipif(shutil.which("go") is None, reason="no Go toolchain")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def binaries(tmp_path_factory):
    out = tmp_path_factory.mktemp("bin")
    for name, directory in (("front", "mcp-front"), ("server", "mcp-test")):
        subprocess.run(
            ["go", "build", "-o", str(out / name), "."],
            cwd=ROOT / "drivers" / directory,
            check=True,
            capture_output=True,
            env={**os.environ, "CGO_ENABLED": "0"},
            timeout=300,
        )
    return out


class Exchange:
    """The lease exchange's two routes, for leases the test issues."""

    def __init__(self, identity: str) -> None:
        self.identity = identity
        self.leases: dict[str, dict] = {}
        self.calls: list[tuple[str, str]] = []
        exchange = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # quiet
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                exchange.calls.append((self.path, body.get("operation", "")))
                if self.headers.get("Authorization") != f"Bearer {exchange.identity}":
                    return self._answer(401, {"error": "unknown_driver_identity"})
                found = exchange.leases.get(body.get("lease_token"))
                if self.path.endswith("/introspect"):
                    if found is None:
                        return self._answer(200, {"active": False})
                    if found["connector_id"] != CONNECTOR:
                        return self._answer(
                            403, {"error": "driver_identity_of_another_connector"}
                        )
                    return self._answer(200, {"active": True, **found})
                if found is None or found["connector_id"] != CONNECTOR:
                    return self._answer(403, {"error": "unknown_lease"})
                if body.get("operation") == "write" and found["access"] != "ReadWrite":
                    return self._answer(403, {"error": "operation_not_allowed"})
                return self._answer(
                    200,
                    {
                        "credential": CREDENTIAL,
                        "expires_at": found["expires_at"],
                        "access": found["access"],
                        "allowed_upstream": [],
                        "max_cache_seconds": 30,
                    },
                )

            def _answer(self, status, body):
                raw = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def issue(
        self,
        access: str,
        connector: str = CONNECTOR,
        expires_at: str = "2099-01-01T00:00:00+00:00",
    ) -> str:
        token = mint_token("scl")
        self.leases[token] = {
            "lease_id": f"lease-{len(self.leases)}",
            "connector_id": connector,
            "access": access,
            "expires_at": expires_at,
        }
        return token


class Pod:
    """The front and the server, as one managed MCP pod runs them."""

    def __init__(self, binaries: Path, workdir: Path, exchange: Exchange) -> None:
        self.binaries = binaries
        self.workdir = workdir
        self.exchange = exchange
        self.front_port = _free_port()
        self.server_port = _free_port()
        self.processes: list[subprocess.Popen] = []
        mcp = managed_mcp(MCP_TEST_SPEC)
        front = mcp.front_config()
        front["upstream"] = f"http://127.0.0.1:{self.server_port}/mcp"
        request = {
            "protocol_version": "1.0",
            "plane": "service",
            "driver": MCP_TEST_SPEC.name,
            "connector": {"id": CONNECTOR, "config": {}},
            "credentials": {},
            "service": {"port": self.front_port, "port_name": "srw-driver"},
            "exchange": {"url": exchange.url, "identity_file": "identity"},
            "mcp": front,
        }
        (workdir / "request.json").write_text(json.dumps(request))
        (workdir / "identity").write_text(exchange.identity + "\n")
        self.logs = workdir / "front.log"

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.front_port}/mcp"

    def start(self) -> None:
        self.processes.append(
            subprocess.Popen(
                [
                    str(self.binaries / "server"),
                    "-listen",
                    f"127.0.0.1:{self.server_port}",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )
        with self.logs.open("ab") as log:
            self.processes.append(
                subprocess.Popen(
                    [str(self.binaries / "front"), "serve"],
                    env={
                        "SRW_REQUEST_FILE": str(self.workdir / "request.json"),
                        "SRW_DRIVER_IDENTITY_FILE": str(self.workdir / "identity"),
                        "SRW_DRIVER_PORT": str(self.front_port),
                    },
                    stdout=log,
                    stderr=log,
                )
            )
        deadline = time.monotonic() + 30
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
            process.wait(timeout=10)
        self.processes.clear()


@pytest.fixture
def pod(binaries, tmp_path):
    exchange = Exchange(mint_token("sdi"))
    running = Pod(binaries, tmp_path, exchange)
    running.start()
    try:
        yield running
    finally:
        running.stop()


def _entry(pod: Pod, token: str) -> dict:
    """A managed MCP binding as the orchestrator delivers it."""
    return {
        "type": MCP_TEST_SPEC.legacy_type,
        "name": "Notes",
        "description": None,
        "connection_url": pod.url,
        "credentials": {
            "lease": {"id": "lease", "connector_id": CONNECTOR, "token": token}
        },
        "project_read_only": False,
        "datasource_id": CONNECTOR,
        "config": {},
    }


async def _connected(entry: dict) -> MCPManager:
    manager = MCPManager([entry])
    await manager.connect_all()
    return manager


def _tool(manager: MCPManager, suffix: str):
    return next(t for t in manager.get_langchain_tools() if t.name.endswith(suffix))


async def _text(tool, **arguments) -> str:
    """A tool call's text, as the model reads it."""
    result = await tool.coroutine(**arguments)
    if isinstance(result, tuple):  # the adapter's (content, artifact)
        result = result[0]
    if isinstance(result, list):
        result = "\n".join(
            block.get("text", "") if isinstance(block, dict) else str(block)
            for block in result
        )
    return str(result)


@pytest.mark.asyncio
async def test_a_session_serves_through_the_front_with_only_a_lease(pod):
    token = pod.exchange.issue("ReadWrite")
    entry = _entry(pod, token)
    manager = await _connected(entry)
    try:
        assert manager.statuses == {"Notes": "connected"}
        names = {t.metadata["mcp_tool_name"] for t in manager.get_langchain_tools()}
        assert names == {
            "whoami",
            "notes_list",
            "notes_read",
            "leak_credential",
            "notes_write",
            "notes_delete",
        }
        answer = json.loads(await _text(_tool(manager, "whoami")))
        # The server received the connector's credential; the client never
        # held it.
        assert (
            answer["credential_sha256"]
            == hashlib.sha256(CREDENTIAL.encode()).hexdigest()
        )
        assert CREDENTIAL not in json.dumps(entry)
        leaked = await _text(_tool(manager, "leak_credential"))
        assert CREDENTIAL not in leaked and "[redacted]" in leaked
        assert "written" in await _text(
            _tool(manager, "notes_write"), name="a", text="hello"
        )
        # Never the token or the credential in the front's log.
        await manager.aclose()
        logs = pod.logs.read_text()
        assert token not in logs and CREDENTIAL not in logs
        assert 'tool="notes_write" class=write status=200' in logs
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_read_only_hides_write_tools_and_refuses_them(pod):
    manager = await _connected(_entry(pod, pod.exchange.issue("ReadOnly")))
    try:
        names = {t.metadata["mcp_tool_name"] for t in manager.get_langchain_tools()}
        assert names == {"whoami", "notes_list", "notes_read", "leak_credential"}
        handle = manager._handles[0]
        # A client that asks anyway gets the server's "unknown tool" answer
        # from the front, and its session stays alive.
        from mcp.shared.exceptions import McpError

        with pytest.raises(McpError) as refused:
            await handle.session.call_tool("notes_write", {"name": "a", "text": "x"})
        assert "Unknown tool" in str(refused.value)
        assert manager_module._tool_level(refused.value)
        assert manager.statuses == {"Notes": "connected"}
        assert ("/v1/leases/exchange", "write") not in pod.exchange.calls
    finally:
        await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["unknown", "other_connector"])
async def test_an_execution_without_the_connector_gets_401(pod, kind):
    if kind == "unknown":
        token = mint_token("scl")  # never issued
    else:
        token = pod.exchange.issue("ReadWrite", connector=OTHER)
    assert _status(pod.url, token) == 401
    manager = await _connected(_entry(pod, token))
    try:
        assert manager.statuses["Notes"] == (
            f"unavailable: {manager_module.LEASE_REFUSED}"
        )
        assert manager.get_langchain_tools() == []
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_a_lease_revoked_mid_session_is_said_so_and_spends_no_reconnects(pod):
    # The front keeps a live decision for up to 30 s, never past the
    # lease's expiry: a lease expiring soon shows the revocation at once.
    soon = (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat()
    token = pod.exchange.issue("ReadWrite", expires_at=soon)
    entry = _entry(pod, token)
    manager = await _connected(entry)
    try:
        whoami = _tool(manager, "whoami")
        assert "credential_sha256" in await _text(whoami)
        del pod.exchange.leases[token]
        await asyncio.sleep(2.5)
        answer = await _text(whoami)
        assert manager_module.LEASE_REFUSED in answer, answer
        handle = manager._handles[0]
        assert handle.status == f"unavailable: {manager_module.LEASE_REFUSED}"
        assert len(handle.reconnects) == 0
        # Every further call is told the same, and nothing reconnects.
        assert manager_module.LEASE_REFUSED in await _text(whoami)
        assert len(handle.reconnects) == 0
        # The entry carries another lease (a redelivery): it reconnects.
        entry["credentials"]["lease"]["token"] = pod.exchange.issue("ReadWrite")
        assert "credential_sha256" in await _text(whoami)
        assert handle.status == "connected" and handle.refused_lease is None
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_the_gates_bypass_bodies_are_refused_by_the_real_front(pod):
    """The k3d gate's raw-body program and verdict, here against the real
    front: the review's four differential bodies are refused as malformed
    and the well-formed write call is 'Unknown tool' for a ReadOnly lease."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "k3d_managed_mcp_gate", ROOT / "scripts" / "k3d-managed-mcp-gate.py"
    )
    gate = sys.modules.get(spec.name) or importlib.util.module_from_spec(spec)
    # Its dataclasses look their module up while it loads.
    sys.modules[spec.name] = gate
    spec.loader.exec_module(gate)
    payload = {
        "url": pod.url,
        "bearer": pod.exchange.issue("ReadOnly"),
        "bodies": gate.bypass_bodies("d5a-0123456789"),
    }
    done = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", gate._RAW_PROGRAM],
        input=json.dumps(payload) + "\n",
        capture_output=True,
        text=True,
        timeout=60,
    )
    result = json.loads(done.stdout.splitlines()[-1])
    ok, detail = gate.bypass_verdict(result)
    assert ok, detail
    assert ("/v1/leases/exchange", "write") not in pod.exchange.calls
    assert "refused a malformed message" in pod.logs.read_text()


def _status(url: str, token: str) -> int:
    request = urllib.request.Request(
        url,
        data=json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        ).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["while_starting", "after_it_serves"])
async def test_a_pod_replaced_mid_session_reconnects(pod, when):
    """The pod is replaced: both containers go and new ones serve the same
    endpoint. A call made while the new pod starts finds nothing listening;
    one made after finds a server that does not know its session (404). Both
    reconnect within the budget and answer."""
    manager = await _connected(_entry(pod, pod.exchange.issue("ReadWrite")))
    try:
        whoami = _tool(manager, "whoami")
        assert json.loads(await _text(whoami))["pod"]
        pod.stop()
        if when == "while_starting":
            restarted = asyncio.get_running_loop().run_in_executor(None, pod.start)
            answer = await _text(whoami)
            await restarted
        else:
            pod.start()
            answer = await _text(whoami)
        assert "credential_sha256" in answer, answer
        handle = manager._handles[0]
        assert handle.status == "connected"
        assert len(handle.reconnects) == 1
        # And the next call needs no reconnect.
        assert "credential_sha256" in await _text(whoami)
        assert len(handle.reconnects) == 1
    finally:
        await manager.aclose()
