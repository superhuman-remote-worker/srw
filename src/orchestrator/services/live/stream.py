"""A live session's cockpit stream; live_session_jobs.md §10.

Server-sent events: ``session`` when the session's view changes and
``session_event`` for each journal row, polled every second, a heartbeat
every 15 s that re-authorises the reader, until the session is over and its
journal drained.
"""

import asyncio
import json
import time

from fastapi.encoders import jsonable_encoder

from orchestrator.services.live.store import session_view
from shared.runtime.live.contract import TERMINAL_SESSION_STATUSES

POLL_SECONDS = 1.0
HEARTBEAT_SECONDS = 15.0


def frame(kind, value):
    return f"event: {kind}\ndata: {json.dumps(jsonable_encoder(value), ensure_ascii=False)}\n\n"


async def session_stream(store, session_id, job_id, after_id, request, authorize):
    previous = None
    heartbeat = time.monotonic()
    while not await request.is_disconnected():
        session = await store.get(session_id, job_id=job_id)
        if session is None:
            return
        view = session_view(session)
        if previous != view:
            yield frame("session", view)
            previous = view
        events = await store.events(session_id, after_id)
        for event in events:
            yield frame("session_event", event)
            after_id = event["id"]
        if len(events) == 500:
            continue
        if session["status"] in TERMINAL_SESSION_STATUSES:
            return
        if time.monotonic() - heartbeat >= HEARTBEAT_SECONDS:
            await authorize(request, store.db, str(job_id))
            yield ": heartbeat\n\n"
            heartbeat = time.monotonic()
        await asyncio.sleep(POLL_SECONDS)
