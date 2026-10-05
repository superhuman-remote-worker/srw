"""Agent boundary for a gateway-signed, exact pinned native first-use notice."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Callable
from uuid import UUID

import asyncssh
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from agent.api.models import PinnedSessionRecipient, pinned_session_recipient_matches
from shared.native_workspace_first_use import verify_native_first_use_proof
from shared.persistent_input_delivery import (
    InputDeliveryAuthorityLost,
    lock_runtime_authority,
)

logger = logging.getLogger(__name__)


class NativeFirstUseRefused(Exception):
    def __init__(self, reason: str, status_code: int = 409):
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code


def configured_public_keys() -> list[str]:
    """Read at most four configured public halves; partial config is unusable."""

    paths = os.environ.get("SSH_GATEWAY_PUBLIC_HOST_KEYS", "").split(",")
    if not 1 <= len(paths) <= 4 or any(not path.strip() for path in paths):
        return []
    result: list[str] = []
    try:
        for raw in paths:
            path = Path(raw.strip())
            if not path.is_absolute():
                return []
            exported = path.read_text(encoding="ascii")
            if (
                not exported.startswith("ssh-ed25519 ")
                or len(exported) > 1024
                or len(exported.splitlines()) != 1
            ):
                return []
            key = asyncssh.import_public_key(exported)
            if key.get_algorithm() != "ssh-ed25519":
                return []
            result.append(exported)
    except (OSError, UnicodeError, ValueError, asyncssh.KeyImportError):
        return []
    return result


async def apply_native_first_use(
    proof: Any,
    recipient: Any,
    *,
    session: Any,
    identity: Any,
    termination: Any,
    agent_id: str | None,
    pod_uid: str | None,
    process_generation: str | None,
    public_keys: list[str],
) -> dict[str, str]:
    """Hold the existing DB authority lock through the synchronous latch."""

    if not public_keys or not verify_native_first_use_proof(proof, public_keys):
        raise NativeFirstUseRefused("invalid_native_proof", 403)
    if not isinstance(proof, dict):  # verifier above ensures this
        raise NativeFirstUseRefused("invalid_native_proof", 403)
    try:
        exact = PinnedSessionRecipient.model_validate(recipient)
    except (TypeError, ValueError) as exc:
        raise NativeFirstUseRefused("recipient_authority_mismatch") from exc
    if not pinned_session_recipient_matches(
        exact,
        thread_id=proof["thread_id"],
        agent_id=agent_id,
        pod_uid=pod_uid,
        process_generation=process_generation,
    ):
        raise NativeFirstUseRefused("recipient_authority_mismatch")
    current = identity.snapshot()
    life = (
        proof["thread_id"],
        proof["runtime_generation"],
        proof["agent_id"],
        current.attach_token,
        proof["pod_uid"],
        proof["process_generation"],
    )
    if (
        session is None
        or session.postgres_conn is None
        or identity.runtime_contract is not True
        or identity.thread_id != life[0]
        or identity.session_generation != life[1]
        or current.agent_id != life[2]
        or current.attach_token != life[3]
        or current.pod_uid != life[4]
        or process_generation != life[5]
        or identity.fingerprint() != proof["session_identity_fingerprint"]
        or termination.runtime_admission_closed()
        or termination.terminating
    ):
        raise NativeFirstUseRefused("stale_native_recipient")
    try:
        async with session.postgres_conn.acquire() as conn:
            async with conn.transaction():
                await lock_runtime_authority(
                    conn,
                    thread_id=life[0],
                    agent_id=life[2],
                    pod_uid=life[4],
                    session_runtime_generation=life[1],
                    runtime_attach_token=life[3],
                )
                # The shared guard holds the agent row FOR SHARE. Read the
                # registration epoch under that lock before signaling.
                agent = await conn.fetchrow(
                    "SELECT metadata FROM agents WHERE id = $1::uuid FOR SHARE",
                    UUID(life[2]),
                )
                metadata = agent["metadata"] if agent is not None else None
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except ValueError:
                        metadata = None
                if (
                    not isinstance(metadata, dict)
                    or metadata.get("dispatch_process_generation") != life[5]
                ):
                    raise NativeFirstUseRefused("stale_native_process")
                # No await after this point: local timeout and retirement can
                # only win before or after this synchronous check/latch pair.
                if (
                    not verify_native_first_use_proof(proof, public_keys)
                    or identity.thread_id != life[0]
                    or identity.session_generation != life[1]
                    or identity.snapshot().agent_id != life[2]
                    or identity.attach_token != life[3]
                    or identity.snapshot().pod_uid != life[4]
                    or identity.fingerprint() != proof["session_identity_fingerprint"]
                    or process_generation != life[5]
                    or termination.runtime_admission_closed()
                    or termination.terminating
                ):
                    raise NativeFirstUseRefused("stale_native_recipient")
                outcome = termination.note_native_first_use(life)
                if outcome not in {"accepted", "already_observed"}:
                    raise NativeFirstUseRefused("native_first_use_closed")
    except NativeFirstUseRefused:
        raise
    except InputDeliveryAuthorityLost as exc:
        raise NativeFirstUseRefused("runtime_authority_lost") from exc
    except Exception as exc:
        logger.warning("Native first-use runtime authority unavailable", exc_info=True)
        raise NativeFirstUseRefused("native_authority_unavailable", 503) from exc
    return {
        "event_id": proof["event_id"],
        "session_identity_fingerprint": proof["session_identity_fingerprint"],
        "process_generation": life[5],
        "status": outcome,
    }


def register_native_first_use_route(
    app: FastAPI, context: Callable[[], dict[str, Any]]
) -> None:
    """Register the same exact handler on dedicated and dual Session apps."""

    @app.post("/session/native-first-use")
    async def native_first_use(request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except (ValueError, UnicodeError):
            body = None
        if not isinstance(body, dict) or set(body) != {"proof", "_recipient"}:
            return JSONResponse({"error": "invalid_native_request"}, status_code=400)
        try:
            receipt = await apply_native_first_use(
                body["proof"], body["_recipient"], **context()
            )
        except NativeFirstUseRefused as exc:
            return JSONResponse({"error": exc.reason}, status_code=exc.status_code)
        return JSONResponse(receipt)
