"""Real-protocol smoke test for the shipped MCP image.

Run inside ``docker/Dockerfile.mcp``'s image. The test starts a disposable
stdlib HTTP stub, launches the shipped stdio server twice (fresh MCP clients),
compares raw ``tools/list`` with the image-baked schema artifact, and invokes a
read, mutation, denied, and degraded-service path. It never touches a real SRW
deployment.
"""

from __future__ import annotations

import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import threading
from typing import Any
from urllib.parse import urlparse

from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client

from mcp_server.capabilities import TOOL_CAPABILITIES


class _StubState:
    create_job_calls = 0
    steer_job_calls = 0


class _StubHandler(BaseHTTPRequestHandler):
    server_version = "SRWMCPStub/1"

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def _json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        path = urlparse(self.path).path
        if path == "/api/jobs":
            # Must mirror the real envelope: a stub that keeps producing the
            # old bare array would let CI smoke go on validating a contract
            # the server no longer serves.
            self._json(
                200,
                {
                    "jobs": [
                        {
                            "id": "job-read-1",
                            "status": "paused",
                            "config_name": "worker_base",
                            "created_at": "2026-08-03T00:00:00Z",
                            "audit_count": 0,
                        }
                    ],
                    "total": 1,
                    "total_is_capped": False,
                    "has_more": False,
                    "limit": 100,
                    "offset": 0,
                    "filters": {"include_archived_projects": False},
                },
            )
            return
        if path == "/api/jobs/job-degraded/repo/file":
            self._json(503, {"detail": "Gitea unavailable in disposable smoke stub"})
            return
        if path == "/api/health":
            self._json(200, {"status": "ok"})
            return
        self._json(404, {"detail": f"unhandled smoke GET {path}"})

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        path = urlparse(self.path).path
        content_length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(content_length)) if content_length else {}
        if path == "/api/jobs":
            _StubState.create_job_calls += 1
            self._json(
                201,
                {
                    "id": "job-created-1",
                    "status": "created",
                    "description": "protocol smoke mutation",
                },
            )
            return
        if path == "/api/jobs/job-read-1/messages/officer/reply":
            assert body == {"message": "continue with the current plan", "urgent": True}
            _StubState.steer_job_calls += 1
            self._json(
                200,
                {"status": "ok", "delivery_strategy": "guidance_next_turn"},
            )
            return
        if path == "/api/skills/reload":
            self._json(403, {"detail": "administrator role required"})
            return
        self._json(404, {"detail": f"unhandled smoke POST {path}"})


def _tool_document(tools: list[Any]) -> list[dict[str, Any]]:
    document = [
        tool.model_dump(mode="json", by_alias=True, exclude_none=True) for tool in tools
    ]
    document.sort(key=lambda tool: tool["name"])
    return document


def _text(result: Any) -> str:
    return "\n".join(
        block.text for block in result.content if getattr(block, "text", None)
    )


async def _connect_and_list(api_url: str, *, exercise: bool) -> list[dict[str, Any]]:
    child_env = dict(os.environ)
    child_env.update(
        {
            "MCP_TRANSPORT": "stdio",
            "COCKPIT_API_URL": api_url,
            "PYTHONUNBUFFERED": "1",
        }
    )
    server_args = ["-m", "mcp_server"]
    server_cwd = Path(__file__).resolve().parents[2]
    parameters = StdioServerParameters(
        command=sys.executable,
        args=server_args,
        cwd=str(server_cwd),
        env=child_env,
    )
    with open(os.devnull, "w", encoding="utf-8") as child_stderr:
        transport_context = stdio_client(parameters, errlog=child_stderr)
        # The client this image's users have: 2026-07-28 where the server
        # speaks it, the initialize handshake otherwise.
        async with Client(transport_context, mode="auto") as session:
            result = await session.list_tools()
            tools = list(result.tools)
            cursor = result.next_cursor
            while cursor:
                result = await session.list_tools(cursor=cursor)
                tools.extend(result.tools)
                cursor = result.next_cursor

            if exercise:
                read = await session.call_tool("list_jobs", {})
                assert not read.is_error and "job-read-1" in _text(read)

                mutation = await session.call_tool(
                    "create_job", {"description": "protocol smoke mutation"}
                )
                mutation_text = _text(mutation)
                assert not mutation.is_error and "job-created-1" in mutation_text
                assert "automatic workspace provisioning" in mutation_text

                steer = await session.call_tool(
                    "steer_job",
                    {
                        "job_id": "job-read-1",
                        "message": "continue with the current plan",
                        "urgent": True,
                    },
                )
                assert not steer.is_error
                assert "guidance_next_turn" in _text(steer)

                denied = await session.call_tool("reload_skills", {})
                assert denied.is_error
                denied_text = _text(denied)
                assert (
                    "403" in denied_text or "administrator role required" in denied_text
                )

                degraded = await session.call_tool(
                    "get_workspace_file",
                    {"job_id": "job-degraded", "path": "report.md"},
                )
                assert not degraded.is_error
                assert "Gitea unavailable in disposable smoke stub" in _text(degraded)

            return _tool_document(tools)


async def _run() -> None:
    _StubState.create_job_calls = 0
    _StubState.steer_job_calls = 0
    stub = ThreadingHTTPServer(("127.0.0.1", 0), _StubHandler)
    thread = threading.Thread(target=stub.serve_forever, daemon=True)
    thread.start()
    api_url = f"http://127.0.0.1:{stub.server_address[1]}"
    try:
        first = await _connect_and_list(api_url, exercise=True)
        second = await _connect_and_list(api_url, exercise=False)
    finally:
        stub.shutdown()
        stub.server_close()
        thread.join(timeout=2)

    assert _StubState.create_job_calls == 1
    assert _StubState.steer_job_calls == 1
    assert first == second
    assert {tool["name"] for tool in first} == set(TOOL_CAPABILITIES)
    for tool in first:
        contract = TOOL_CAPABILITIES[tool["name"]]
        assert tool["annotations"] == contract.annotations
        assert tool["_meta"]["io.srw.capability"] == contract.metadata()

    artifact_path = Path(os.environ.get("MCP_SCHEMA_ARTIFACT", "/app/tool-schema.json"))
    if not artifact_path.is_file():
        raise AssertionError(f"image schema artifact missing: {artifact_path}")
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert artifact["tools"] == first
    print(
        json.dumps(
            {
                "status": "ok",
                "tool_count": len(first),
                "tool_schema_digest": artifact["digest"],
                "fresh_connections": 2,
                "mutation_calls": _StubState.create_job_calls,
                "steer_calls": _StubState.steer_job_calls,
            },
            sort_keys=True,
        )
    )


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
