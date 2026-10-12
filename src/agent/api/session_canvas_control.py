"""Canvas control and live awareness over the session WebSocket.

A connected Cockpit sends four Canvas control verbs: two committed-edit
invalidations (``canvas.presentation_updated``, ``canvas.source_updated``)
and two live-only awareness verbs (``canvas.user_editing``,
``canvas.user_idle``). This channel validates each untrusted frame against
authoritative Canvas state, paces and deduplicates per connection, keeps one
expiring awareness lease per connection, and emits:

* committed invalidations through the runtime's ordered journal
  (``broadcast``), so they replay after reload; and
* awareness frames through live fan-out only (``fan_out_live``), never the
  journal.

Per-connection state belongs to this channel; the runtime owns the one
process-wide instance and calls :meth:`CanvasControlChannel.clear_all` across
attach/detach boundaries. A disconnect releases only its own connection's
lease.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from agent.api import session_transport

logger = logging.getLogger(__name__)

CANVAS_AWARENESS_TTL_S: float = max(
    15.0, min(60.0, float(os.environ.get("CANVAS_AWARENESS_TTL_S", "15")))
)
CANVAS_CONTROL_VALIDATION_MIN_INTERVAL_S = 0.5
CANVAS_AWARENESS_RENEW_MIN_INTERVAL_S = 1.0

CANVAS_CONTROL_METHODS = frozenset(
    {
        "canvas.presentation_updated",
        "canvas.source_updated",
        "canvas.user_editing",
        "canvas.user_idle",
    }
)


@dataclass(frozen=True)
class CanvasAwarenessLease:
    task: asyncio.Task
    params: Dict[str, Any]
    renewed_at: float
    validated_at: float


def validated_canvas_control_state(
    data: Dict[str, Any], state: dict[str, Any] | None
) -> dict[str, Any] | None:
    """Match an untrusted control frame to exact authoritative Canvas state."""

    if state is None or data.get("canvas_id") != "main":
        return None
    revision = data.get("presentation_revision")
    if (
        isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision != state.get("presentation_revision")
    ):
        return None
    if data.get("method") == "canvas.presentation_updated":
        return state

    source = state.get("source")
    if (
        not isinstance(source, dict)
        or source.get("type") != "workspace_file"
        or not isinstance(data.get("path"), str)
        or data["path"] != source.get("path")
        or not isinstance(data.get("source_version"), str)
        or data["source_version"] != state.get("source_version")
    ):
        return None
    return state


class CanvasControlChannel:
    """Per-connection Canvas control state for one session runtime."""

    def __init__(
        self,
        *,
        load_state: Callable[[], Awaitable[dict[str, Any] | None]],
        invalidate_recent_read: Callable[[str], None],
        identity_fingerprint: Callable[[], Optional[str]],
        broadcast: Callable[[str, Dict[str, Any]], None],
        fan_out_live: Callable[[Dict[str, Any]], None],
        awareness_ttl_s: float = CANVAS_AWARENESS_TTL_S,
        validation_min_interval_s: float = CANVAS_CONTROL_VALIDATION_MIN_INTERVAL_S,
        renew_min_interval_s: float = CANVAS_AWARENESS_RENEW_MIN_INTERVAL_S,
    ) -> None:
        self._load_state = load_state
        self._invalidate_recent_read = invalidate_recent_read
        self._identity_fingerprint = identity_fingerprint
        self._broadcast = broadcast
        self._fan_out_live = fan_out_live
        self.awareness_ttl_s = awareness_ttl_s
        self.validation_min_interval_s = validation_min_interval_s
        self.renew_min_interval_s = renew_min_interval_s
        self.awareness: Dict[str, CanvasAwarenessLease] = {}
        self._validation_at: Dict[tuple[str, str], float] = {}
        self._source_updates: Dict[str, tuple[str, int, str]] = {}
        self._presentation_updates: Dict[str, int] = {}

    def _cancel_awareness(self, client_id: str) -> CanvasAwarenessLease | None:
        lease = self.awareness.pop(client_id, None)
        if lease is not None and lease.task is not asyncio.current_task():
            lease.task.cancel()
        return lease

    def _fan_out_idle(self, client_id: str, params: Dict[str, Any]) -> None:
        self._fan_out_live(
            {
                "method": "canvas.user_idle",
                "params": {**params, "sender_id": client_id},
            }
        )

    async def _expire_awareness(
        self, client_id: str, editing_session_id: str, params: Dict[str, Any]
    ) -> None:
        try:
            await asyncio.sleep(self.awareness_ttl_s)
        except asyncio.CancelledError:
            return
        lease = self.awareness.get(client_id)
        if (
            lease is None
            or lease.task is not asyncio.current_task()
            or lease.params.get("editing_session_id") != editing_session_id
        ):
            return
        self.awareness.pop(client_id, None)
        self._fan_out_idle(client_id, params)

    def release(self, client_id: str) -> None:
        """Expire every courtesy lease owned by one disconnected connection."""

        lease = self._cancel_awareness(client_id)
        if lease is not None:
            self._fan_out_idle(client_id, lease.params)
        for key in [key for key in self._validation_at if key[0] == client_id]:
            self._validation_at.pop(key, None)
        self._source_updates.pop(client_id, None)
        self._presentation_updates.pop(client_id, None)

    def clear_all(self) -> None:
        """Cancel leases without emitting across a detach/reattach boundary."""

        leases = list(self.awareness.values())
        self.awareness.clear()
        self._validation_at.clear()
        self._source_updates.clear()
        self._presentation_updates.clear()
        for lease in leases:
            lease.task.cancel()

    def _start_awareness(
        self,
        client_id: str,
        params: Dict[str, Any],
        *,
        renewed_at: float,
        validated_at: float,
    ) -> None:
        editing_session_id = str(params["editing_session_id"])
        task = asyncio.create_task(
            self._expire_awareness(client_id, editing_session_id, params),
            name=f"canvas-awareness-{client_id[:8]}",
        )
        self.awareness[client_id] = CanvasAwarenessLease(
            task=task,
            params=params,
            renewed_at=renewed_at,
            validated_at=validated_at,
        )
        self._fan_out_live(
            {
                "method": "canvas.user_editing",
                "params": {
                    **params,
                    "sender_id": client_id,
                    "ttl_ms": int(self.awareness_ttl_s * 1000),
                },
            }
        )

    async def handle(
        self,
        ws: Any,
        data: Dict[str, Any],
        client_id: str,
        *,
        expected_session_identity_fingerprint: str | None = None,
    ) -> bool:
        """Handle validated edit invalidation and live-only awareness frames."""

        method = data.get("method")
        if method not in CANVAS_CONTROL_METHODS:
            return False
        if method == "canvas.presentation_updated":
            expected_fields = {"method", "canvas_id", "presentation_revision"}
        else:
            expected_fields = {
                "method",
                "canvas_id",
                "path",
                "presentation_revision",
                "source_version",
            }
            if method in {"canvas.user_editing", "canvas.user_idle"}:
                expected_fields.add("editing_session_id")
        if set(data) != expected_fields:
            await session_transport.send_message(
                ws,
                "error",
                {
                    "code": "invalid_canvas_control",
                    "message": "Canvas control message is invalid",
                },
            )
            return True
        editing_session_id = data.get("editing_session_id")
        if method in {"canvas.user_editing", "canvas.user_idle"} and (
            not isinstance(editing_session_id, str)
            or not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", editing_session_id)
        ):
            await session_transport.send_message(
                ws,
                "error",
                {
                    "code": "invalid_canvas_control",
                    "message": "Canvas editing session is invalid",
                },
            )
            return True
        path = data.get("path")
        revision = data.get("presentation_revision")
        source_version = data.get("source_version")
        invalid_identity = (
            data.get("canvas_id") != "main"
            or isinstance(revision, bool)
            or not isinstance(revision, int)
            or revision < 1
        )
        invalid_file_identity = method != "canvas.presentation_updated" and (
            not isinstance(path, str)
            or not 0 < len(path) <= 4096
            or not isinstance(source_version, str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", source_version)
        )
        if invalid_identity or invalid_file_identity:
            await session_transport.send_message(
                ws,
                "error",
                {
                    "code": "invalid_canvas_control",
                    "message": "Canvas control message is invalid",
                },
            )
            return True

        now = asyncio.get_running_loop().time()
        if method in {"canvas.user_editing", "canvas.user_idle"}:
            lease = self.awareness.get(client_id)
            if lease is not None and all(
                data.get(field) == lease.params.get(field)
                for field in (
                    "canvas_id",
                    "path",
                    "presentation_revision",
                    "source_version",
                    "editing_session_id",
                )
            ):
                if method == "canvas.user_idle":
                    self._cancel_awareness(client_id)
                    self._fan_out_idle(client_id, lease.params)
                    return True
                if now - lease.validated_at < self.awareness_ttl_s:
                    if now - lease.renewed_at < self.renew_min_interval_s:
                        # Exact duplicate/over-eager renewal: the current server
                        # TTL is still live, so avoid task churn and redundant
                        # fan-out.
                        return True
                    self._cancel_awareness(client_id)
                    self._start_awareness(
                        client_id,
                        lease.params,
                        renewed_at=now,
                        validated_at=lease.validated_at,
                    )
                    return True

        source_identity: tuple[str, int, str] | None = None
        if method == "canvas.source_updated":
            assert isinstance(path, str) and isinstance(revision, int)
            assert isinstance(source_version, str)
            source_identity = (path, revision, source_version)
            if self._source_updates.get(client_id) == source_identity:
                # The successful save response may be retried. A real
                # subsequent save advances the revision, so only the exact last
                # accepted identity is safe to deduplicate locally.
                return True

            # Do not drop a distinct committed revision. Pace authoritative
            # checks instead, bounding invalid/mismatched spam without losing a
            # real save.
            validation_key = (client_id, "source")
            last_validation = self._validation_at.get(validation_key)
            if last_validation is not None:
                remaining = self.validation_min_interval_s - (now - last_validation)
                if remaining > 0:
                    await asyncio.sleep(remaining)
                    now = asyncio.get_running_loop().time()
            self._validation_at[validation_key] = now
        elif method == "canvas.presentation_updated":
            assert isinstance(revision, int)
            if self._presentation_updates.get(client_id) == revision:
                return True
            validation_key = (client_id, "presentation")
            last_validation = self._validation_at.get(validation_key)
            if last_validation is not None:
                remaining = self.validation_min_interval_s - (now - last_validation)
                if remaining > 0:
                    await asyncio.sleep(remaining)
                    now = asyncio.get_running_loop().time()
            self._validation_at[validation_key] = now
        else:
            validation_key = (client_id, "awareness")
            last_validation = self._validation_at.get(validation_key)
            if (
                last_validation is not None
                and now - last_validation < self.validation_min_interval_s
            ):
                await session_transport.send_message(
                    ws,
                    "error",
                    {
                        "code": "canvas_control_rate_limited",
                        "message": "Canvas control messages are arriving too quickly",
                    },
                )
                return True
            self._validation_at[validation_key] = now

        try:
            state = await self._load_state()
        except Exception as exc:
            logger.warning("Canvas control state validation failed: %s", exc)
            await session_transport.send_message(
                ws,
                "error",
                {
                    "code": "canvas_control_unavailable",
                    "message": "Canvas state could not be validated",
                },
            )
            return True
        if (
            expected_session_identity_fingerprint is not None
            and self._identity_fingerprint() != expected_session_identity_fingerprint
        ):
            return True
        state = validated_canvas_control_state(data, state)
        if state is None:
            await session_transport.send_message(
                ws,
                "error",
                {
                    "code": "canvas_control_stale",
                    "message": "Canvas state changed; reload before continuing",
                },
            )
            return True

        if method == "canvas.presentation_updated":
            source = state.get("source")
            source_type = source.get("type") if isinstance(source, dict) else None
            self._broadcast(
                "canvas.updated",
                {
                    "canvas_id": "main",
                    "presentation_revision": state["presentation_revision"],
                    "source_type": source_type,
                    "updated_at": state.get("updated_at"),
                },
            )
            assert isinstance(revision, int)
            self._presentation_updates[client_id] = revision
            return True

        if method == "canvas.source_updated":
            self._invalidate_recent_read(path)
            self._broadcast(
                "canvas.source_updated",
                {
                    "canvas_id": "main",
                    "presentation_revision": state["presentation_revision"],
                    "source_type": "workspace_file",
                    "updated_at": state.get("updated_at"),
                },
            )
            assert source_identity is not None
            self._source_updates[client_id] = source_identity
            return True

        awareness_params = {
            "canvas_id": "main",
            "path": path,
            "presentation_revision": state["presentation_revision"],
            "source_version": state["source_version"],
            "editing_session_id": editing_session_id,
        }
        assert isinstance(editing_session_id, str)
        previous = self._cancel_awareness(client_id)
        if previous is not None:
            self._fan_out_idle(client_id, previous.params)
        if method == "canvas.user_idle":
            self._fan_out_idle(client_id, awareness_params)
            return True

        self._start_awareness(
            client_id,
            awareness_params,
            renewed_at=now,
            validated_at=now,
        )
        return True
