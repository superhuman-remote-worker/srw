"""A live-session job's dispatch injection; live_session_jobs.md §4.2.

``config.live_session`` tells the job which session it is in and where the
connector's pod is: ``{session_id, connector_id, provider_session_id,
endpoint_url}``. No secret: the voice subagent authenticates to the pod with
the job's lease for the connector, which normal lease delivery puts on the
connector's entry.
"""

from __future__ import annotations

import json
from typing import Any

from orchestrator.services.live.pods import pod_endpoint
from orchestrator.services.live.store import LiveSessionStore


async def live_session_config(db: Any, job: dict[str, Any]) -> dict[str, Any] | None:
    context = job.get("context") or {}
    if isinstance(context, str):
        context = json.loads(context)
    block = context.get("live_session") if isinstance(context, dict) else None
    session_id = block.get("session_id") if isinstance(block, dict) else None
    if not session_id:
        return None
    session = await LiveSessionStore(db).get(session_id, job_id=job["id"])
    if session is None:
        raise ValueError("The live session is not bound to this job")
    connector_id = str(session["connector_id"]) if session.get("connector_id") else ""
    endpoint = await pod_endpoint(db, connector_id) if connector_id else None
    return {
        "session_id": str(session["id"]),
        "connector_id": connector_id,
        "provider_session_id": session["provider_session_id"],
        "endpoint_url": endpoint or "",
    }
