"""Durable call SSE; knowledge-base/knowledge/features/phone_calls_as_subagents.md."""

import asyncio
import json
import time

from fastapi.encoders import jsonable_encoder

from orchestrator.services.telephony.store import call_view
from shared.runtime.telephony.contract import TERMINAL_CALL_STATUSES

POLL_SECONDS = 1.0
HEARTBEAT_SECONDS = 15.0


def frame(kind, value):
    return f"event: {kind}\ndata: {json.dumps(jsonable_encoder(value), ensure_ascii=False)}\n\n"


async def call_stream(store, call_id, job_id, after_id, request, authorize):
    previous = None
    heartbeat = time.monotonic()
    while not await request.is_disconnected():
        call = await store.get_call(call_id, job_id=job_id)
        if call is None:
            return
        view = call_view(call)
        if previous != view:
            yield frame("call", view)
            previous = view
        events = await store.events(call_id, after_id)
        for event in events:
            yield frame("call_event", event)
            after_id = event["id"]
        if len(events) == 500:
            continue
        if call["status"] in TERMINAL_CALL_STATUSES:
            return
        if time.monotonic() - heartbeat >= HEARTBEAT_SECONDS:
            await authorize(request, store.db, str(job_id))
            yield ": heartbeat\n\n"
            heartbeat = time.monotonic()
        await asyncio.sleep(POLL_SECONDS)
