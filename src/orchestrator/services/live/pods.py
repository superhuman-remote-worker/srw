"""Orchestrator → live connector pod; knowledge-base/knowledge/features/live_session_jobs.md §5.3–5.4.

The orchestrator holds no call: it hands each admitted session to the pod of
its connector (``POST /v1/sessions``), which accepts it at the engine inside
the provider's window, or asks the pod to reject one it will not take. It
reaches the pod on the connector's endpoint Service at the digest of its
newest ready pod, with the control token derived for the connector (never
stored; the pod reads the same token from its request file).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import httpx

from orchestrator.security.crypto import live_control_token
from shared.connectors.builtin import GPT_LIVE_SIP_SPEC

logger = logging.getLogger(__name__)

#: The pod answers a handoff within 13 s (its admission deadline is 12 s,
#: inside the provider's 15 s window).
HANDOFF_TIMEOUT_SECONDS = 14.0
REJECT_TIMEOUT_SECONDS = 5.0

_READY_POD = """
SELECT image_digest FROM connector_driver_identities
 WHERE connector_id = $1 AND revoked_at IS NULL AND removed_at IS NULL
   AND replaced_at IS NULL AND ready_at IS NOT NULL
   AND credential_generation IS NOT NULL AND image_digest IS NOT NULL
 ORDER BY ready_at DESC LIMIT 1
"""


class LivePodError(Exception):
    """The pod could not take the request (not ready, refused, timed out)."""

    def __init__(self, message: str, *, status: int = 0, code: str | None = None):
        super().__init__(message)
        self.status = status
        self.code = code


@dataclass(frozen=True)
class PodTarget:
    connector_id: str
    url: str


async def pod_endpoint(
    db: Any, connector_id: str, *, spec=GPT_LIVE_SIP_SPEC
) -> str | None:
    """The base URL of the connector's ready pod, or ``None`` (no pod is
    ready, or hosting is off)."""
    from orchestrator.services.connector_service_images import service_image_settings
    from orchestrator.services.connector_service_launch import endpoint_url

    namespace = service_image_settings().service_namespace
    if not namespace or spec.service is None:
        return None
    async with db.acquire() as conn:
        digest = await conn.fetchval(_READY_POD, UUID(str(connector_id)))
    if not digest:
        return None
    return endpoint_url(
        namespace=namespace,
        connector_id=str(connector_id),
        digest=str(digest),
        port=spec.service.port,
    )


class LivePodClient:
    """Calls a live connector's pod with its control token."""

    def __init__(self, db: Any, *, transport: Any = None):
        self.db = db
        self._transport = transport

    async def target(self, connector_id: str) -> PodTarget:
        url = await pod_endpoint(self.db, connector_id)
        if url is None:
            raise LivePodError("The line's connector pod is not ready")
        return PodTarget(connector_id=str(connector_id), url=url)

    async def _post(
        self, target: PodTarget, path: str, body: dict[str, Any], *, timeout: float
    ) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {live_control_token(target.connector_id)}"}
        try:
            async with httpx.AsyncClient(
                timeout=timeout, transport=self._transport
            ) as client:
                response = await client.post(
                    target.url + path, json=body, headers=headers
                )
        except httpx.HTTPError as exc:
            raise LivePodError(
                f"The line's connector pod did not answer ({type(exc).__name__})"
            ) from None
        try:
            answer = response.json()
        except ValueError:
            answer = {}
        if not isinstance(answer, dict):
            answer = {}
        if response.status_code >= 400:
            error = answer.get("error") if isinstance(answer.get("error"), dict) else {}
            status = error.get("status")
            code = error.get("code")
            raise LivePodError(
                str(error.get("message") or f"HTTP {response.status_code}")[:500],
                # A malformed answer must still be a LivePodError (the
                # handoff marks the session failed only for that).
                status=(
                    status
                    if isinstance(status, int) and not isinstance(status, bool)
                    else response.status_code
                ),
                code=code if isinstance(code, str) else None,
            )
        return answer

    async def handoff(self, connector_id: str, handoff: dict[str, Any]) -> dict:
        target = await self.target(connector_id)
        return await self._post(
            target, "/v1/sessions", handoff, timeout=HANDOFF_TIMEOUT_SECONDS
        )

    async def reject(
        self, connector_id: str, session_id: str, provider_session_id: str, code: int
    ) -> None:
        target = await self.target(connector_id)
        await self._post(
            target,
            f"/v1/sessions/{session_id}/reject",
            {"provider_session_id": provider_session_id, "status_code": int(code)},
            timeout=REJECT_TIMEOUT_SECONDS,
        )
