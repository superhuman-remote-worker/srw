"""Leased call recorders; knowledge-base/knowledge/features/phone_calls_as_subagents.md."""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from dataclasses import asdict
from datetime import datetime, timezone
from uuid import uuid4

import aiohttp

from orchestrator.services.application_tasks import drain_task_mapping
from orchestrator.services.telephony.instructions import FALLBACK, greeting_instruction
from orchestrator.services.telephony.store import TelephonyStore
from shared.runtime.telephony.contract import (
    AppendAcknowledged,
    AppendKind,
    DelegationCreated,
    DtmfReceived,
    EngineError,
    OtherEvent,
    SessionClosed,
    TranscriptDelta,
    UsageUpdated,
)
from shared.runtime.telephony.gpt_live import (
    GptLiveClient,
    GptLiveError,
    LiveEndpoints,
    SidebandClosedError,
)

logger = logging.getLogger(__name__)
RENEW_SECONDS = 10.0
SWEEP_SECONDS = 15.0
MONITOR_SECONDS = 0.5
LOST_RETRY_SECONDS = 30.0


def client_for(settings: dict, api_key: str) -> GptLiveClient:
    return GptLiveClient(
        api_key,
        LiveEndpoints(
            settings["api_base"], settings["ws_base"], settings["extra_headers"]
        ),
    )


class TelephonyTaskRegistry:
    """Identity-guarded per-app tasks, reusable after a call or sweep completes."""

    def __init__(self):
        self.owner = f"{socket.gethostname()}-{uuid4().hex}"
        self.tasks: dict[str, asyncio.Task] = {}
        self.stopping = False

    def start(self, key: str, coroutine) -> asyncio.Task | None:
        existing = self.tasks.get(key)
        if self.stopping or (existing is not None and not existing.done()):
            coroutine.close()
            return existing
        task = asyncio.create_task(coroutine, name=f"telephony-{key}")
        self.tasks[key] = task

        def done(completed):
            if self.tasks.get(key) is completed:
                self.tasks.pop(key, None)
            if not completed.cancelled() and completed.exception() is not None:
                logger.error(
                    "Phone task %s failed (%s)",
                    key,
                    type(completed.exception()).__name__,
                )

        task.add_done_callback(done)
        return task

    async def drain(self):
        self.stopping = True
        try:
            await drain_task_mapping(self.tasks)
        finally:
            self.stopping = False


class Recorder:
    def __init__(self, store: TelephonyStore, registry: TelephonyTaskRegistry):
        self.store = store
        self.registry = registry

    async def start(self, call_id):
        claimed = await self.store.claim(self.registry.owner, call_id)
        for call in claimed:
            self.registry.start(f"recorder-{call['id']}", self.run(call))

    async def sweep(self):
        for call in await self.store.claim(self.registry.owner):
            self.registry.start(f"recorder-{call['id']}", self.run(call))

    async def sweep_loop(self, shutdown_event: asyncio.Event):
        while not shutdown_event.is_set():
            try:
                await self.sweep()
            except Exception as exc:
                logger.error("Phone recorder sweep failed (%s)", type(exc).__name__)
            try:
                await asyncio.wait_for(shutdown_event.wait(), SWEEP_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def run(self, call: dict):
        call_id = call["id"]
        client = None
        tasks = []
        state = {"sideband": None}
        try:
            settings = await self.store.get_settings(call["settings_id"])
            line = await self.store.get_line(call["line_id"])
            if not settings or not line:
                raise ValueError("Recorder configuration is unavailable")
            client = client_for(settings, await self.store.api_key(settings))
            tasks = [
                asyncio.create_task(self._read(call, line, client, state)),
                asyncio.create_task(self._renew(call_id)),
                asyncio.create_task(self._monitor(call, line, client, state)),
            ]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if state["sideband"] is not None:
                await state["sideband"].detach()
            if client is not None:
                await client.close()
            await self.store.release(call_id, self.registry.owner)

    async def _renew(self, call_id):
        while True:
            await asyncio.sleep(RENEW_SECONDS)
            if not await self.store.renew(call_id, self.registry.owner):
                return

    async def _append(
        self, conn, call, sideband, kind, content, event_id, *, delegation_id=None
    ):
        if await self.store.has_event(conn, call["id"], event_id):
            return
        future = await sideband.send_append(
            kind, content, event_id=event_id, delegation_id=delegation_id
        )
        # The event reader persists engine rejection events. Retrieve the
        # future's exception even when the socket goes away before its ack.
        future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
        await self.store.append_event(
            call["id"],
            {
                "event_id": event_id,
                "kind": "instruction"
                if kind == AppendKind.INSTRUCTIONS
                else "fallback",
                "speaker": "recorder",
                "text": content,
                "delegation_id": delegation_id,
            },
            conn=conn,
        )

    async def _read(self, call, line, client, state):
        call_id = call["id"]
        outage_since = None
        delay = 0.25
        while await self.store.owns(call_id, self.registry.owner):
            sideband = None
            try:
                sideband = await asyncio.wait_for(
                    client.attach(call["session_id"]), 10.0
                )
                state["sideband"] = sideband
                async with self.store.recorder_action(
                    call_id, self.registry.owner
                ) as conn:
                    if conn is None:
                        return
                    await self._append(
                        conn,
                        call,
                        sideband,
                        AppendKind.INSTRUCTIONS,
                        greeting_instruction(line),
                        f"srw_greeting_{call_id}",
                    )
                outage_since = None
                delay = 0.25
                async for event in sideband:
                    if not await self._persist_retry(call_id, event):
                        return
                    if isinstance(event, SessionClosed):
                        return
            except (
                aiohttp.ClientError,
                GptLiveError,
                SidebandClosedError,
                TimeoutError,
            ) as exc:
                if sideband is not None and sideband.closed_event is not None:
                    await self._persist_retry(call_id, sideband.closed_event)
                    return
                outage_since = outage_since or time.monotonic()
                if (
                    getattr(exc, "status", None) in {404, 409}
                    and time.monotonic() - outage_since >= LOST_RETRY_SECONDS
                ):
                    async with self.store.recorder_action(
                        call_id, self.registry.owner
                    ) as conn:
                        if conn is not None:
                            await self.store.append_event(
                                call_id,
                                {
                                    "event_id": f"srw_lost_{call_id}",
                                    "kind": "closed",
                                    "payload": {"reason": "recorder_lost"},
                                },
                                conn=conn,
                            )
                            await self.store.end(conn, call_id, "recorder_lost")
                    return
            finally:
                state["sideband"] = None
                if sideband is not None:
                    await sideband.detach()
            outage_since = outage_since or time.monotonic()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 5.0)

    async def _persist_retry(self, call_id, event) -> bool:
        while True:
            try:
                return await self._persist(call_id, event)
            except Exception as exc:
                logger.error(
                    "Phone event persistence retry for %s (%s)",
                    call_id,
                    type(exc).__name__,
                )
                await asyncio.sleep(0.5)
                if not await self.store.owns(call_id, self.registry.owner):
                    return False

    async def _persist(self, call_id, event) -> bool:
        async with self.store.recorder_action(call_id, self.registry.owner) as conn:
            if conn is None:
                return False
            if isinstance(event, UsageUpdated):
                await self.store.usage(conn, call_id, event.seconds)
                return True
            data = {"event_id": event.event_id, "payload": asdict(event)}
            if isinstance(event, TranscriptDelta):
                data.update(
                    kind="transcript",
                    speaker=event.speaker.value,
                    text=event.text,
                    start_ms=event.start_ms,
                    end_ms=event.end_ms,
                )
            elif isinstance(event, DelegationCreated):
                data.update(
                    kind="delegation",
                    delegation_id=event.delegation_id,
                    start_ms=event.offset_ms,
                )
            elif isinstance(event, DtmfReceived):
                data.update(kind="dtmf", text=event.digit)
            elif isinstance(event, EngineError):
                data.update(kind="error", text=event.message)
            elif isinstance(event, SessionClosed):
                data.update(kind="closed")
                await self.store.append_event(call_id, data, conn=conn)
                await self.store.end(
                    conn,
                    call_id,
                    event.raw_reason or event.reason.value,
                    event.usage_seconds,
                )
                return True
            elif isinstance(event, AppendAcknowledged):
                return True
            else:
                data.update(kind="system")
                if isinstance(event, OtherEvent):
                    data["payload"] = dict(event.payload)
            await self.store.append_event(call_id, data, conn=conn)
            return True

    async def _monitor(self, call, line, client, state):
        deadline = call["accepted_at"].timestamp() + line["max_duration_s"]
        while await self.store.owns(call["id"], self.registry.owner):
            if datetime.now(timezone.utc).timestamp() >= deadline:
                async with self.store.recorder_action(
                    call["id"], self.registry.owner
                ) as conn:
                    if conn is None:
                        return
                    event_id = f"srw_max_duration_{call['id']}"
                    if not await self.store.has_event(conn, call["id"], event_id):
                        try:
                            await client.hangup(call["session_id"])
                        except (
                            aiohttp.ClientError,
                            GptLiveError,
                            SidebandClosedError,
                            TimeoutError,
                        ):
                            await asyncio.sleep(MONITOR_SECONDS)
                            continue
                        await self.store.append_event(
                            call["id"],
                            {
                                "event_id": event_id,
                                "kind": "hangup",
                                "speaker": "recorder",
                                "payload": {"reason": "max_duration"},
                            },
                            conn=conn,
                        )
            sideband = state["sideband"]
            if sideband is not None and not sideband.is_closed:
                for delegation in await self.store.pending_delegations(
                    call["id"], line["no_job_fallback_s"]
                ):
                    async with self.store.recorder_action(
                        call["id"], self.registry.owner, unattached=True
                    ) as conn:
                        if conn is not None:
                            try:
                                await self._append(
                                    conn,
                                    call,
                                    sideband,
                                    AppendKind.COMMENTARY,
                                    FALLBACK,
                                    f"srw_fallback_{call['id']}_{delegation['delegation_id']}",
                                    delegation_id=delegation["delegation_id"],
                                )
                            except (
                                aiohttp.ClientError,
                                SidebandClosedError,
                                TimeoutError,
                            ):
                                break
            await asyncio.sleep(MONITOR_SECONDS)
