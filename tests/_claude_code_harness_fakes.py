"""Shared fakes for the Claude Code harness tests: Claude Code (``FakeClient``),
the start request SRW sends, and workspace connections to the local sshd."""

import asyncio
import dataclasses

import pytest
from srw_claude_code_harness.workspace import Workspace

AGENT_ID = "agent-1"
GENERATION = "gen-1"
POD_UID = "pod-uid-1"
JOB_ID = "job-1"
IMAGE = "registry.example/harness@sha256:" + "a" * 64
INTERNAL_KEY = "internal-key-0123456789"
API_KEY = "model-key-0123456789"
PROOF = "delivery-proof-0123456789"
SECRETS = (INTERNAL_KEY, API_KEY, PROOF)


def sdk_or_skip():
    return pytest.importorskip("claude_agent_sdk")


def result_message(subtype="success", is_error=False, result="done"):
    return sdk_or_skip().ResultMessage(
        subtype=subtype,
        duration_ms=10,
        duration_api_ms=5,
        is_error=is_error,
        num_turns=1,
        session_id="s-1",
        result=result,
    )


class FakeClient:
    """Stands in for ``ClaudeSDKClient``.

    ``on_query`` (async, given the options) runs when the prompt is sent;
    ``hang`` waits after the scripted messages until ``interrupt()`` answers.
    """

    def __init__(self, options, messages, *, hang=False, on_query=None):
        self.options = options
        self.messages = list(messages)
        self.hang = hang
        self.on_query = on_query
        self.prompts = []
        self.interrupted = False
        self.disconnected = False
        self.queried = asyncio.Event()
        self._answer = asyncio.Event()

    async def connect(self):
        pass

    async def query(self, prompt):
        self.prompts.append(prompt)
        if self.on_query is not None:
            await self.on_query(self.options)
        self.queried.set()

    async def receive_messages(self):
        for message in self.messages:
            yield message
        if self.hang:
            await self._answer.wait()
            yield result_message(result="interrupted")

    async def interrupt(self):
        self.interrupted = True
        self._answer.set()

    async def disconnect(self):
        self.disconnected = True


def fake_client_factory(clients, messages=(), **kwargs):
    def make(options):
        client = FakeClient(options, messages, **kwargs)
        clients.append(client)
        return client

    return make


async def call_tool(options, name, arguments):
    """Call one of the harness's tools the way the CLI does: through the SDK MCP
    server in the options."""
    from mcp.types import CallToolRequest, CallToolRequestParams

    server = options.mcp_servers["ws"]["instance"]
    handler = server.request_handlers[CallToolRequest]
    return await handler(
        CallToolRequest(
            method="tools/call",
            params=CallToolRequestParams(name=name, arguments=arguments),
        )
    )


def remote_for(access):
    return {
        "host": access.host,
        "port": access.port,
        "username": access.user,
        "key_path": access.private_key_file,
        "workspace_path": access.root,
    }


DUMMY_REMOTE = {
    "host": "10.0.0.5",
    "port": 22,
    "username": "agent-host",
    "key_path": "/nonexistent/id_ed25519",
    "workspace_path": "/home/agent-host/workspace",
}


def start_body(
    remote=None, *, protocol="anthropic-messages", job_id=JOB_ID, autonomy=None
):
    body = {
        "job_id": job_id,
        "description": "Write the report.",
        "instructions": "Be terse.",
        "resolved_config": {
            "agent": {
                "llm": {
                    "model": "claude-test",
                    "provider": "anthropic",
                    "client_protocol": protocol,
                    "base_url": "https://gateway.example/v1",
                    "api_key": API_KEY,
                },
                "workspace": {"remote": remote or DUMMY_REMOTE},
            }
        },
        "config_override": None,
        "pinned_delivery_id": "delivery-1",
        "pinned_projection_digest": "digest-1",
        "pinned_delivery_proof": PROOF,
        "recipient": recipient(job_id),
    }
    if autonomy is not None:
        body["resolved_config"]["agent"]["autonomy"] = autonomy
    return body


def recipient(job_id=JOB_ID):
    return {
        "expected_agent_id": AGENT_ID,
        "expected_pod_uid": POD_UID,
        "expected_process_generation": GENERATION,
        "expected_job_id": job_id,
    }


def connect_via(sshd, calls):
    """The start body carries no known_hosts file; the local sshd's stands in."""

    async def connect(access):
        calls.append(access)
        return await Workspace.connect(
            dataclasses.replace(access, known_hosts_file=sshd.access.known_hosts_file)
        )

    return connect


def never_connect(calls):
    async def connect(access):
        calls.append(access)
        raise AssertionError("connect must not be called")

    return connect


async def until(predicate):
    while not predicate():
        await asyncio.sleep(0.01)
