"""The wake fault injector must send UID and immediate deletion as one body."""

from __future__ import annotations

import argparse
import json
import threading
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from uuid import uuid4

import pytest
from kubernetes import client

from orchestrator.operator_cli.stateless_wake_acceptance import (
    GateError,
    StatelessWakeGate,
)


POD_NAME = "srw-agent-stateless-fixture"
NAMESPACE = "srw"
THREAD_ID = str(uuid4())
POD_UID = str(uuid4())


@pytest.fixture
def pod_api_server():
    """Serve the real generated Kubernetes client over local HTTP only."""

    observed = {"pod_uid": POD_UID, "requests": []}

    class Handler(BaseHTTPRequestHandler):
        def _send(self, payload: dict):
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            observed["requests"].append(("GET", self.path, b""))
            self._send(
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": POD_NAME,
                        "namespace": NAMESPACE,
                        "uid": observed["pod_uid"],
                    },
                }
            )

        def do_DELETE(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            observed["requests"].append(("DELETE", self.path, body))
            self._send({"apiVersion": "v1", "kind": "Status", "status": "Success"})

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    configuration = client.Configuration()
    configuration.host = f"http://127.0.0.1:{server.server_port}"
    api = client.CoreV1Api(client.ApiClient(configuration=configuration))
    try:
        yield api, observed
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class QuietQueue:
    def __init__(self, foreign: int = 0, fleet: int = 0):
        self.foreign = foreign
        self.fleet = fleet

    @asynccontextmanager
    async def acquire(self):
        yield self

    async def fetchval(self, query: str, *_args):
        if "AND leased_by=$1" in query:
            return self.foreign
        if "unit_id<>$1::uuid" in query:
            return self.fleet
        raise AssertionError("unexpected queue query")


def gate(api, *, foreign: int = 0, fleet: int = 0) -> StatelessWakeGate:
    result = StatelessWakeGate(
        argparse.Namespace(run_id="wake-test-001", owner_user_id=None)
    )
    result.state.thread_id = THREAD_ID
    result.namespace = NAMESPACE
    result.db = QuietQueue(foreign, fleet)
    result.provisioner = SimpleNamespace(_core_api=api)
    return result


@pytest.mark.asyncio
async def test_exact_executor_delete_serializes_uid_and_immediate_background_in_body(
    pod_api_server,
):
    api, observed = pod_api_server

    await gate(api)._delete_exact_executor(POD_NAME, POD_UID)

    requests = observed["requests"]
    assert [request[0] for request in requests] == ["GET", "DELETE"]
    assert (
        requests[1][1].split("?", 1)[0]
        == f"/api/v1/namespaces/{NAMESPACE}/pods/{POD_NAME}"
    )
    assert json.loads(requests[1][2]) == {
        "preconditions": {"uid": POD_UID},
        "gracePeriodSeconds": 0,
        "propagationPolicy": "Background",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "foreign,fleet,code",
    [
        (1, 0, "executor_not_disposable"),
        (0, 1, "stateless_pool_not_quiet"),
    ],
)
async def test_other_queue_work_prevents_any_pod_delete(
    pod_api_server,
    foreign,
    fleet,
    code,
):
    api, observed = pod_api_server

    with pytest.raises(GateError) as raised:
        await gate(api, foreign=foreign, fleet=fleet)._delete_exact_executor(
            POD_NAME, POD_UID
        )

    assert raised.value.code == code
    assert observed["requests"] == []


@pytest.mark.asyncio
async def test_changed_pod_uid_prevents_delete(pod_api_server):
    api, observed = pod_api_server
    observed["pod_uid"] = str(uuid4())

    with pytest.raises(GateError) as raised:
        await gate(api)._delete_exact_executor(POD_NAME, POD_UID)

    assert raised.value.code == "pod_authority_changed"
    assert [request[0] for request in observed["requests"]] == ["GET"]
