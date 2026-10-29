"""Outbound orchestrator client: registers, heartbeats and reports like SRW's agent.

Mirrors the contract of ``/api/agents/*`` and ``/api/jobs/{id}/complete`` with plain
httpx; the harness image contains no SRW code, so nothing is imported from it.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx

_COMPLETE_TIMEOUT = 60.0
_REQUEST_TIMEOUT = 30.0
# What a registration answer that is not the expected JSON object raises.
_MALFORMED_ANSWER = (KeyError, TypeError, ValueError)


@dataclass(frozen=True)
class Registration:
    agent_id: str
    heartbeat_interval_seconds: int
    dispatch_process_generation: str


@dataclass(frozen=True)
class PinnedEcho:
    """The pinned-delivery fields of the start request, echoed verbatim."""

    delivery_id: str | None
    projection_digest: str | None
    delivery_proof: str | None = field(repr=False)


@dataclass
class CompletionReport:
    should_stop: bool
    goal_achieved: bool
    error: dict | None
    freeze_data: dict | None


class OrchestratorClient:
    def __init__(
        self,
        base_url: str,
        internal_key: str,
        *,
        http: httpx.AsyncClient,
        warn: Callable[[str], None] = lambda message: None,
    ):
        self._base = base_url.rstrip("/")
        self._headers = {"X-Internal-Key": internal_key}
        self._http = http
        self._warn = warn
        self._registration: Registration | None = None
        self._register_args: dict[str, Any] | None = None

    async def register(
        self,
        *,
        config_name: str,
        pod_ip: str,
        pod_port: int,
        hostname: str,
        build_sha: str,
        pod_uid: str,
        image: str,
        timeout: float = _REQUEST_TIMEOUT,
    ) -> Registration:
        self._register_args = {
            "config_name": config_name,
            "pod_ip": pod_ip,
            "pod_port": pod_port,
            "hostname": hostname,
            "build_sha": build_sha,
            "pod_uid": pod_uid,
            "image": image,
        }
        response = await self._http.post(
            f"{self._base}/api/agents/register",
            json={
                **self._register_args,
                "pid": os.getpid(),
                "agent_mode": "worker",
                "thread_id": None,
                "product_provenance": {"provenance_status": "unavailable"},
            },
            headers=self._headers,
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json()
        self._registration = Registration(
            agent_id=data["agent_id"],
            heartbeat_interval_seconds=data["heartbeat_interval_seconds"],
            dispatch_process_generation=data["dispatch_process_generation"],
        )
        return self._registration

    @property
    def registration(self) -> Registration | None:
        """The current registration (the latest one after a heartbeat 404 made
        the client register again); ``None`` before the first."""
        return self._registration

    def _agent_id(self) -> str:
        if self._registration is None:
            raise RuntimeError("not registered")
        return self._registration.agent_id

    async def heartbeat(
        self,
        status: str,
        *,
        current_job_id: str | None,
        timeout: float = _REQUEST_TIMEOUT,
    ) -> dict:
        """Returns the ``intents`` object; ``{}`` on a non-200 or network error.

        A 404 means the orchestrator forgot this agent: re-register once and retry.
        A re-registration answered with something other than the registration
        JSON is handled like a network error, with a warning: the registration
        the client had stays current and the next heartbeat tries again.
        ``timeout`` bounds each request, the re-registration included.
        """
        for attempt in (0, 1):
            payload: dict[str, Any] = {"status": status}
            if current_job_id:
                payload["current_job_id"] = current_job_id
            try:
                response = await self._http.post(
                    f"{self._base}/api/agents/{self._agent_id()}/heartbeat",
                    json=payload,
                    headers=self._headers,
                    timeout=timeout,
                )
                if response.status_code == 404 and attempt == 0 and self._register_args:
                    await self.register(**self._register_args, timeout=timeout)
                    continue
            except httpx.HTTPError:
                return {}
            except _MALFORMED_ANSWER as exc:
                self._warn(
                    "re-registration after a heartbeat 404 got a malformed answer "
                    f"({type(exc).__name__}); trying again at the next heartbeat"
                )
                return {}
            if response.status_code != 200:
                return {}
            try:
                return (response.json() or {}).get("intents") or {}
            except ValueError:
                return {}
        return {}

    async def deregister(self, *, timeout: float = _REQUEST_TIMEOUT) -> None:
        if self._registration is None:
            return
        try:
            await self._http.delete(
                f"{self._base}/api/agents/{self._registration.agent_id}",
                headers=self._headers,
                timeout=timeout,
            )
        except httpx.HTTPError:
            pass

    async def complete(
        self,
        job_id: str,
        report: CompletionReport,
        *,
        pinned: PinnedEcho | None,
        pod_uid: str,
    ) -> bool:
        payload: dict[str, Any] = {
            "should_stop": report.should_stop,
            "goal_achieved": report.goal_achieved,
            "error": report.error,
            "freeze_data": report.freeze_data,
            "agent_id": self._agent_id(),
        }
        if pinned is not None:
            for key, value in (
                ("pinned_delivery_id", pinned.delivery_id),
                ("pinned_projection_digest", pinned.projection_digest),
                ("pinned_delivery_proof", pinned.delivery_proof),
            ):
                if value is not None:
                    payload[key] = value
            assert self._registration is not None
            payload["pinned_process_generation"] = (
                self._registration.dispatch_process_generation
            )
            payload["pinned_pod_uid"] = pod_uid
        try:
            response = await self._http.post(
                f"{self._base}/api/jobs/{job_id}/complete",
                json=payload,
                headers=self._headers,
                timeout=_COMPLETE_TIMEOUT,
            )
        except httpx.HTTPError:
            return False
        return response.status_code in (200, 202)
