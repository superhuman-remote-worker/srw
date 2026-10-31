"""Test-control client for the call gate; knowledge-base/knowledge/features/local_voice_gateway.md §4.1.

This adapter offers the fake engine's driver API to scripts. It does not
implement the protocol: webhooks, appends and engine events run in the gateway.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import aiohttp


class GatewayClient:
    def __init__(self, url: str, line: str, api_key: str):
        self.url, self.line, self.api_key = url.rstrip("/"), line, api_key
        self.http: aiohttp.ClientSession | None = None
        self.sessions: dict[str, SimpleNamespace] = {}
        self.webhook_responses: list[int] = []

    async def start(self):
        self.http = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=aiohttp.ClientTimeout(total=35),
        )
        return self

    async def stop(self):
        if self.http is not None:
            await self.http.close()

    async def request(self, method: str, path: str, **kwargs):
        async with self.http.request(method, self.url + path, **kwargs) as response:
            if response.status == 204:
                return None
            if response.status >= 400:
                # Do not echo a response body or headers into script logs.
                raise RuntimeError(
                    f"gateway test control refused request: HTTP {response.status}"
                )
            return await response.json()

    async def refresh(self, sid: str):
        body = await self.request("GET", f"/test/calls/{sid}")
        body["status"] = {"ringing": "pending", "active": "accepted"}.get(
            body["state"], "closed"
        )
        if body["decision"] == "reject":
            body["status"] = "rejected"
        body["sockets"] = [None] * body["sockets"]
        result = SimpleNamespace(**body)
        self.sessions[sid] = result
        return result

    def session(self, sid: str):
        return self.sessions[sid]

    async def incoming_call(self, *, to_number: str, from_number: str, **kwargs):
        result = await self.request(
            "POST",
            "/test/calls",
            json={
                "line": self.line,
                "to_number": to_number,
                "from_number": from_number,
            },
        )
        sid = result["session_id"]
        session = await self.wait_until(
            sid, lambda s: bool(s.webhook_responses), timeout=16
        )
        self.webhook_responses = session.webhook_responses
        return sid

    async def say(self, sid: str, speaker: str, text: str):
        await self.request(
            "POST", f"/test/calls/{sid}/say", json={"speaker": speaker, "text": text}
        )

    async def delegate(self, sid: str):
        result = await self.request("POST", f"/test/calls/{sid}/delegate", json={})
        return result["delegation_id"]

    async def remote_hangup(self, sid: str):
        await self.request("POST", f"/test/calls/{sid}/remote_hangup", json={})
        await self.refresh(sid)

    async def wait_until(self, sid: str, predicate, timeout: float = 5.0):
        deadline = time.monotonic() + timeout
        while True:
            session = await self.refresh(sid)
            if predicate(session):
                return session
            if time.monotonic() >= deadline:
                raise TimeoutError("gateway condition not met")
            await asyncio.sleep(0.05)

    async def wait_for_command(
        self,
        sid: str,
        command_type: str,
        predicate=None,
        timeout: float = 5.0,
        start_index: int = 0,
    ):
        deadline = time.monotonic() + timeout
        index = start_index
        while (remaining := deadline - time.monotonic()) > 0:
            result = await self.request(
                "GET",
                f"/test/calls/{sid}/commands",
                params={"after": index, "timeout": min(remaining, 20)},
            )
            if result is None:
                session = await self.refresh(sid)
                if session.state == "closed":
                    break
                continue
            index = result["next_index"]
            command = result["command"]
            if command.get("type") == command_type and (
                predicate is None or predicate(command)
            ):
                await self.refresh(sid)
                return command
        raise TimeoutError("gateway command not received")
