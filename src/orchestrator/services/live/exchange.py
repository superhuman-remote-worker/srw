"""What a live connector's pod asks the orchestrator; live_session_jobs.md §5.5.

The pod reaches these routes on the lease exchange port only, with its own
driver identity (``Authorization: Bearer sdi_…``). Every route checks that
the identity is a live service pod (not revoked, its driver a live driver,
hosting on in this process) and that a session it names belongs to the
pod's connector:

- ``credential``: the engine key for the connector's live voice model. A
  standing pod holds no lease, so it gets its upstream credential with its
  identity alone; nothing secret reaches the agent.
- ``state``: a monotonic session status update (``active``/``ended``/
  ``failed``); an ended session's voice seconds become its usage row.
- ``events``: journal rows, deduplicated on ``event_id``.
- ``lease``: the pod's session lease (only the holder acts).
- ``adopt``: the connector's active sessions no pod holds (a restart, a
  superseding generation).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from orchestrator.services.live.ledger import record_session_usage
from orchestrator.services.live.store import (
    MAX_EVENTS_PER_WRITE,
    LiveSessionStore,
    session_view,
)
from shared.connectors.builtin import spec_for_type
from shared.connectors.leases import (
    DRIVER_IDENTITY_PREFIX,
    token_digest,
    token_shape_valid,
)
from shared.runtime.live.contract import JOURNAL_KINDS

logger = logging.getLogger(__name__)

LIVE_PREFIX = "/v1/live/"
#: The journal batches and adoption answers are larger than a lease
#: exchange; the exchange's other routes keep their 4 KiB.
MAX_LIVE_BODY_BYTES = 1024 * 1024
#: How long the pod may cache the engine credential.
CREDENTIAL_CACHE_SECONDS = 300
#: The base URL of a system provider's live API (a catalog row on a
#: ``system`` key has no endpoint row of its own).
SYSTEM_API_BASES = {"openai": "https://api.openai.com/v1"}

_IDENTITY = """
SELECT id, connector_id, driver, revoked_at IS NOT NULL AS revoked,
       credential_generation IS NOT NULL AS service_pod
  FROM connector_driver_identities WHERE token_hash = $1
"""


@dataclass(frozen=True)
class LiveOutcome:
    status: int
    body: dict[str, Any] = field(repr=False)


@dataclass(frozen=True)
class LivePod:
    identity_id: str
    connector_id: str
    driver: str


class StateBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["active", "ended", "failed"]
    accepted_at: datetime | None = None
    attached_at: datetime | None = None
    ended_at: datetime | None = None
    close_reason: str | None = Field(None, max_length=200)
    usage_seconds: float | None = Field(None, ge=0)
    error: str | None = Field(None, max_length=2000)


class EventWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: str | None = Field(None, max_length=200)
    kind: str
    speaker: str | None = Field(None, max_length=40)
    text: str | None = None
    start_ms: int | None = None
    end_ms: int | None = None
    delegation_id: str | None = Field(None, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)


class EventsBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    events: list[EventWrite] = Field(max_length=MAX_EVENTS_PER_WRITE)


class LeaseBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    release: bool = False


def ws_base_for(api_base: str) -> str:
    parts = urlsplit(api_base)
    scheme = {"https": "wss", "http": "ws"}.get(parts.scheme, parts.scheme)
    return urlunsplit((scheme, parts.netloc, parts.path, "", ""))


async def live_engine(db: Any, model: str) -> dict[str, Any] | None:
    """``{api_key, api_base, ws_base, model}`` for a live voice catalog
    model, or ``None`` when the catalog has no usable row for it."""
    row = await db.resolve_catalog_model(model, capability="live_voice")
    if row is None or not row.get("api_key"):
        return None
    if row.get("provider_kind") == "system":
        api_base = SYSTEM_API_BASES.get(str(row.get("provider_ref") or ""))
    else:
        api_base = row.get("endpoint_base_url")
    if not api_base:
        return None
    api_base = str(api_base).rstrip("/")
    return {
        "api_key": row["api_key"],
        "api_base": api_base,
        "ws_base": ws_base_for(api_base),
        "model": row.get("model_id") or model,
    }


class LiveExchange:
    def __init__(
        self,
        *,
        db: Any,
        ledger: Any = None,
        service_hosting: bool = True,
    ) -> None:
        self.db = db
        self.sessions = LiveSessionStore(db)
        self.ledger = ledger
        self.service_hosting = service_hosting

    async def pod(self, identity_token: str) -> LivePod | None:
        """The live service pod an identity token names, or ``None``."""
        if not self.service_hosting:
            return None
        if not token_shape_valid(identity_token, DRIVER_IDENTITY_PREFIX):
            return None
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(_IDENTITY, token_digest(identity_token))
        if row is None or row["revoked"] or not row["service_pod"]:
            return None
        connector = await self.db.get_datasource(str(row["connector_id"]))
        spec = spec_for_type(connector.get("type")) if connector else None
        if spec is None or not spec.live or spec.name != row["driver"]:
            return None
        return LivePod(
            identity_id=str(row["id"]),
            connector_id=str(UUID(str(row["connector_id"]))),
            driver=str(row["driver"]),
        )

    async def _session(self, pod: LivePod, session_id: str) -> dict | None:
        try:
            UUID(session_id)
        except ValueError:
            return None
        row = await self.sessions.get(session_id)
        if row is None or str(row.get("connector_id")) != pod.connector_id:
            return None
        return row

    async def credential(self, pod: LivePod) -> LiveOutcome:
        connector = await self.db.get_datasource(pod.connector_id)
        model = (connector or {}).get("config", {}).get("model")
        engine = await live_engine(self.db, str(model)) if model else None
        if engine is None:
            logger.warning(
                "Live connector %s: no usable live_voice model for its pod",
                pod.connector_id,
            )
            return LiveOutcome(409, {"error": "no_live_voice_model"})
        return LiveOutcome(
            200,
            {
                "api_key": engine["api_key"],
                "api_base": engine["api_base"],
                "ws_base": engine["ws_base"],
                "headers": {},
                "max_cache_seconds": CREDENTIAL_CACHE_SECONDS,
            },
        )

    async def state(
        self, pod: LivePod, session_id: str, body: StateBody
    ) -> LiveOutcome:
        session = await self._session(pod, session_id)
        if session is None:
            return LiveOutcome(404, {"error": "unknown_session"})
        applied, row = await self.sessions.transition(
            session_id,
            body.status,
            accepted_at=body.accepted_at,
            attached_at=body.attached_at,
            ended_at=body.ended_at,
            close_reason=body.close_reason,
            usage_seconds=body.usage_seconds,
            error=body.error,
        )
        if not applied:
            return LiveOutcome(409, {"status": (row or {}).get("status")})
        if body.status in ("ended", "failed") and row is not None:
            # The engine bills the seconds a session ran, however it ended.
            await record_session_usage(self.ledger, self.db, row)
        return LiveOutcome(200, {"status": row["status"]})

    async def events(
        self, pod: LivePod, session_id: str, body: EventsBody
    ) -> LiveOutcome:
        session = await self._session(pod, session_id)
        if session is None:
            return LiveOutcome(404, {"error": "unknown_session"})
        events = [event.model_dump() for event in body.events]
        if any(event["kind"] not in JOURNAL_KINDS for event in events):
            return LiveOutcome(422, {"error": "unknown_event_kind"})
        inserted = await self.sessions.write_events(session_id, events)
        return LiveOutcome(200, {"inserted": inserted})

    async def lease(
        self, pod: LivePod, session_id: str, body: LeaseBody
    ) -> LiveOutcome:
        session = await self._session(pod, session_id)
        if session is None:
            return LiveOutcome(404, {"error": "unknown_session"})
        owned, until = await self.sessions.lease(
            session_id, pod.identity_id, release=body.release
        )
        return LiveOutcome(
            200,
            {"owned": owned, "lease_until": until.isoformat() if until else None},
        )

    async def adopt(self, pod: LivePod) -> LiveOutcome:
        sessions = await self.sessions.adoptable(pod.connector_id, pod.identity_id)
        return LiveOutcome(
            200,
            {
                "sessions": [
                    {
                        "session_id": str(row["id"]),
                        "provider_session_id": row["provider_session_id"],
                        "job_id": str(row["job_id"]) if row.get("job_id") else None,
                        "accepted_at": _iso(row.get("accepted_at")),
                        "attached_at": _iso(row.get("attached_at")),
                        "handoff": row.get("handoff") or {},
                        "sent_event_ids": row.get("sent_event_ids") or [],
                    }
                    for row in sessions
                    if row.get("handoff")
                ]
            },
        )


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


__all__ = [
    "LIVE_PREFIX",
    "MAX_LIVE_BODY_BYTES",
    "EventsBody",
    "LeaseBody",
    "LiveExchange",
    "LiveOutcome",
    "LivePod",
    "StateBody",
    "live_engine",
    "session_view",
    "ws_base_for",
]
