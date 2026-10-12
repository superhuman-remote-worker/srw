"""Inbound phone admission; knowledge-base/knowledge/features/phone_calls_as_subagents.md."""

import logging
import re

from fastapi import HTTPException

from orchestrator.database.postgres import _decrypt_stored
from orchestrator.services.telephony.instructions import (
    greeting_instruction,
    session_instructions,
)
from orchestrator.services.telephony.jobs import create_phone_job
from orchestrator.services.telephony.recorder import (
    Recorder,
    TelephonyTaskRegistry,
    client_for,
)
from orchestrator.services.telephony.store import TelephonyStore
from shared.runtime.telephony.gpt_live import build_session_config
from shared.runtime.telephony.webhook import (
    WebhookVerificationError,
    parse_incoming_call,
    verify_webhook,
)

logger = logging.getLogger(__name__)


def error_summary(exc) -> str:
    # HTTP error bodies can echo request input. Persist only classifications,
    # never exception text from a credential-bearing transport request.
    status = getattr(exc, "status", None)
    return f"{type(exc).__name__}" + (f" (HTTP {status})" if status else "")


def resolve_line(lines: list[dict], to_number: str | None) -> dict | None:
    if to_number and re.fullmatch(r"\+[1-9][0-9]{1,14}", to_number):
        matched = next((line for line in lines if line["number"] == to_number), None)
        if matched is not None:
            return matched
    default = next((line for line in lines if line["is_default"]), None)
    return default or (lines[0] if len(lines) == 1 else None)


class TelephonyFront:
    def __init__(
        self, store: TelephonyStore, registry: TelephonyTaskRegistry, trigger_dispatch
    ):
        self.store, self.registry = store, registry
        self.recorder = Recorder(store, registry)
        self.trigger_dispatch = trigger_dispatch

    async def webhook(self, headers, body: bytes):
        settings = None
        payload = None
        for candidate in await self.store.settings(enabled_only=True):
            secret = _decrypt_stored(
                candidate["webhook_secret_encrypted"],
                field="telephony_settings.webhook_secret",
            )
            if not secret:
                continue
            try:
                payload = verify_webhook(secret, headers, body)
            except WebhookVerificationError:
                continue
            settings = candidate
            break
        if settings is None:
            raise HTTPException(401, "Invalid webhook signature")
        try:
            incoming = parse_incoming_call(payload, headers["webhook-id"])
        except ValueError as exc:
            raise HTTPException(400, "Malformed incoming call") from exc
        if incoming is None:
            return
        call = await self.store.insert_incoming(settings["id"], incoming)
        if call is not None:
            self.registry.start(f"incoming-{call['id']}", self.admit(call, settings))

    async def admit(self, call: dict, settings: dict):
        try:
            async with client_for(
                settings, await self.store.api_key(settings)
            ) as client:
                line = resolve_line(
                    await self.store.lines(settings["id"], enabled_only=True),
                    call["to_number"],
                )
                if line is None:
                    await client.reject(call["session_id"], 404)
                    await self.store.fail(
                        call["id"], "rejected", "No enabled phone line"
                    )
                    return
                greeting_instruction(line)
                await self.store.set_line(call["id"], line)
                await client.accept(
                    call["session_id"],
                    build_session_config(
                        instructions=session_instructions(line),
                        model=line["model"],
                        voice=line["voice"],
                        delegation="client",
                    ),
                )
                await self.store.activate(call["id"])
            await self.recorder.start(call["id"])
        except Exception as exc:
            error = error_summary(exc)
            await self.store.fail(call["id"], "failed", error)
            logger.error("Phone admission failed for %s (%s)", call["id"], error)
            return
        # Separate tracked work: accept and recorder attach never wait for the
        # expert snapshot, connector policy or the job's creation transaction.
        self.registry.start(f"job-{call['id']}", self._create_job(call, line))

    async def _create_job(self, call, line):
        try:
            await create_phone_job(self.store, call, line)
            self.trigger_dispatch()
        except Exception as exc:
            error = error_summary(exc)
            await self.store.append_event(
                call["id"],
                {
                    "event_id": f"srw_job_error_{call['id']}",
                    "kind": "error",
                    "payload": {"stage": "job_creation", "error": error},
                },
            )
            logger.error("Phone job creation failed for %s (%s)", call["id"], error)
