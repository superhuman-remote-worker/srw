"""Canvas control over the session socket, with the runtime's ports faked.

The channel owns per-connection validation, pacing, deduplication and live
awareness leases. Committed invalidations go through the runtime's ordered
journal (``broadcast``); awareness goes through live fan-out only.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock, call as mock_call

import pytest

from agent.api import session_transport
from agent.api.session_canvas_control import (
    CANVAS_AWARENESS_TTL_S,
    CanvasControlChannel,
)

FINGERPRINT = "sha256:" + ("c" * 64)


def _state():
    return {
        "canvas_id": "main",
        "source": {"type": "workspace_file", "path": "output/report.md"},
        "presentation_revision": 4,
        "source_version": "sha256:" + "a" * 64,
        "updated_at": "2026-07-13T12:00:00Z",
    }


def _frame(
    method: str,
    *,
    editing_session_id: str | None = None,
    revision: int = 4,
    version_char: str = "a",
):
    frame = {
        "method": method,
        "canvas_id": "main",
        "path": "output/report.md",
        "presentation_revision": revision,
        "source_version": "sha256:" + version_char * 64,
    }
    if editing_session_id is not None:
        frame["editing_session_id"] = editing_session_id
    return frame


def _channel(
    *,
    load_state=None,
    invalidate=None,
    broadcast=None,
    frames=None,
    fingerprint=FINGERPRINT,
    validation_min_interval_s: float = 0,
) -> CanvasControlChannel:
    return CanvasControlChannel(
        load_state=load_state or AsyncMock(return_value=_state()),
        invalidate_recent_read=invalidate or MagicMock(),
        identity_fingerprint=lambda: fingerprint,
        broadcast=broadcast or MagicMock(),
        fan_out_live=(frames.append if frames is not None else MagicMock()),
        validation_min_interval_s=validation_min_interval_s,
    )


@pytest.mark.asyncio
async def test_source_updated_invalidates_read_and_uses_distinct_event():
    invalidate = MagicMock()
    next_state = {
        **_state(),
        "presentation_revision": 5,
        "source_version": "sha256:" + "b" * 64,
        "updated_at": "2026-07-13T12:00:01Z",
    }
    state_loader = AsyncMock(side_effect=[_state(), next_state])
    broadcast = MagicMock()
    channel = _channel(
        load_state=state_loader, invalidate=invalidate, broadcast=broadcast
    )

    try:
        handled = await channel.handle(
            MagicMock(), _frame("canvas.source_updated"), "client-a"
        )
        # An exact retry is deduplicated, while a real subsequent save has a
        # new revision and must invalidate again.
        assert await channel.handle(
            MagicMock(), _frame("canvas.source_updated"), "client-a"
        )
        assert await channel.handle(
            MagicMock(),
            _frame("canvas.source_updated", revision=5, version_char="b"),
            "client-a",
        )
    finally:
        channel.clear_all()

    assert handled is True
    assert state_loader.await_count == 2
    assert invalidate.call_args_list == [
        mock_call("output/report.md"),
        mock_call("output/report.md"),
    ]
    assert broadcast.call_args_list == [
        mock_call(
            "canvas.source_updated",
            {
                "canvas_id": "main",
                "presentation_revision": 4,
                "source_type": "workspace_file",
                "updated_at": "2026-07-13T12:00:00Z",
            },
        ),
        mock_call(
            "canvas.source_updated",
            {
                "canvas_id": "main",
                "presentation_revision": 5,
                "source_type": "workspace_file",
                "updated_at": "2026-07-13T12:00:01Z",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_presentation_updated_reloads_authority_and_broadcasts_state():
    invalidate = MagicMock()
    state = {
        **_state(),
        "source": {"type": "workspace_app", "entry_path": "/demo"},
        "source_version": None,
    }
    state_loader = AsyncMock(return_value=state)
    broadcast = MagicMock()
    channel = _channel(
        load_state=state_loader, invalidate=invalidate, broadcast=broadcast
    )
    control = {
        "method": "canvas.presentation_updated",
        "canvas_id": "main",
        "presentation_revision": 4,
    }

    try:
        assert await channel.handle(MagicMock(), control, "client-p")
        assert await channel.handle(MagicMock(), control, "client-p")
    finally:
        channel.clear_all()

    state_loader.assert_awaited_once()
    invalidate.assert_not_called()
    broadcast.assert_called_once_with(
        "canvas.updated",
        {
            "canvas_id": "main",
            "presentation_revision": 4,
            "source_type": "workspace_app",
            "updated_at": "2026-07-13T12:00:00Z",
        },
    )


@pytest.mark.asyncio
async def test_presentation_updated_rejects_extra_file_identity():
    state_loader = AsyncMock()
    channel = _channel(load_state=state_loader)
    malformed = _frame("canvas.presentation_updated")
    ws = MagicMock(send_json=AsyncMock())

    assert await channel.handle(ws, malformed, "client-p")

    state_loader.assert_not_awaited()
    ws.send_json.assert_awaited_once_with(
        {
            "method": "error",
            "params": {
                "code": "invalid_canvas_control",
                "message": "Canvas control message is invalid",
            },
        }
    )


@pytest.mark.asyncio
async def test_malformed_source_update_is_rejected_before_validation():
    state_loader = AsyncMock()
    channel = _channel(load_state=state_loader)
    malformed = _frame("canvas.source_updated")
    malformed["presentation_revision"] = True
    ws = MagicMock(send_json=AsyncMock())

    assert await channel.handle(ws, malformed, "client-b")

    state_loader.assert_not_awaited()
    ws.send_json.assert_awaited_once_with(
        {
            "method": "error",
            "params": {
                "code": "invalid_canvas_control",
                "message": "Canvas control message is invalid",
            },
        }
    )


@pytest.mark.asyncio
async def test_awareness_is_one_live_only_lease_and_local_renew_idle():
    state_loader = AsyncMock(return_value=_state())
    frames = []
    broadcast = MagicMock()
    ws = MagicMock(send_json=AsyncMock())
    channel = _channel(load_state=state_loader, frames=frames, broadcast=broadcast)
    try:
        first = _frame("canvas.user_editing", editing_session_id="editor_session_a")
        assert await channel.handle(ws, first, "client-a")
        assert state_loader.await_count == 1
        assert list(channel.awareness) == ["client-a"]
        assert frames[-1]["method"] == "canvas.user_editing"
        assert frames[-1]["params"]["editing_session_id"] == "editor_session_a"
        assert frames[-1]["params"]["ttl_ms"] >= 15_000

        # Exact rapid renewal is deduplicated locally, without another
        # delegated orchestrator request or another task/lease.
        assert await channel.handle(ws, first, "client-a")
        assert state_loader.await_count == 1
        assert len(channel.awareness) == 1

        # Local renewals periodically revalidate ownership/current state;
        # they cannot keep a revoked lease alive forever.
        lease = channel.awareness["client-a"]
        channel.awareness["client-a"] = replace(
            lease,
            validated_at=(
                asyncio.get_running_loop().time() - channel.awareness_ttl_s - 1
            ),
        )
        assert await channel.handle(ws, first, "client-a")
        assert state_loader.await_count == 2
        assert len(channel.awareness) == 1

        replacement = _frame(
            "canvas.user_editing", editing_session_id="editor_session_b"
        )
        assert await channel.handle(ws, replacement, "client-a")
        assert state_loader.await_count == 3
        assert len(channel.awareness) == 1
        assert frames[-2]["method"] == "canvas.user_idle"
        assert frames[-2]["params"]["editing_session_id"] == "editor_session_a"
        assert "ttl_ms" not in frames[-2]["params"]
        assert frames[-1]["params"]["editing_session_id"] == "editor_session_b"

        idle = _frame("canvas.user_idle", editing_session_id="editor_session_b")
        assert await channel.handle(ws, idle, "client-a")
        assert state_loader.await_count == 3
        assert channel.awareness == {}
        assert frames[-1]["method"] == "canvas.user_idle"
        assert "ttl_ms" not in frames[-1]["params"]
        ws.send_json.assert_not_awaited()
        # Awareness is live-only: nothing entered the ordered journal.
        broadcast.assert_not_called()
    finally:
        channel.clear_all()


@pytest.mark.asyncio
async def test_release_expires_only_the_disconnected_connections_lease():
    frames = []
    channel = _channel(frames=frames)
    try:
        for client, session in (
            ("client-a", "editor_aaaaaaaa"),
            ("client-b", "editor_bbbbbbbb"),
        ):
            assert await channel.handle(
                MagicMock(),
                _frame("canvas.user_editing", editing_session_id=session),
                client,
            )
        lease_a = channel.awareness["client-a"]
        lease_b = channel.awareness["client-b"]
        frames.clear()

        channel.release("client-a")
        await asyncio.sleep(0)

        assert list(channel.awareness) == ["client-b"]
        assert lease_a.task.cancelled()
        assert not lease_b.task.done()
        assert frames == [
            {
                "method": "canvas.user_idle",
                "params": {**lease_a.params, "sender_id": "client-a"},
            }
        ]
        # Releasing an already-released connection emits nothing more.
        channel.release("client-a")
        assert len(frames) == 1
    finally:
        channel.clear_all()


@pytest.mark.asyncio
async def test_clear_all_cancels_leases_without_emitting_across_a_boundary():
    frames = []
    channel = _channel(frames=frames)
    assert await channel.handle(
        MagicMock(),
        _frame("canvas.user_editing", editing_session_id="editor_aaaaaaaa"),
        "client-a",
    )
    lease = channel.awareness["client-a"]
    frames.clear()

    channel.clear_all()
    await asyncio.sleep(0)

    assert channel.awareness == {}
    assert lease.task.cancelled()
    assert frames == []


@pytest.mark.asyncio
async def test_awareness_lease_expires_to_idle_after_its_ttl():
    frames = []
    channel = CanvasControlChannel(
        load_state=AsyncMock(return_value=_state()),
        invalidate_recent_read=MagicMock(),
        identity_fingerprint=lambda: FINGERPRINT,
        broadcast=MagicMock(),
        fan_out_live=frames.append,
        awareness_ttl_s=0.01,
        validation_min_interval_s=0,
    )
    assert await channel.handle(
        MagicMock(),
        _frame("canvas.user_editing", editing_session_id="editor_aaaaaaaa"),
        "client-a",
    )
    lease = channel.awareness["client-a"]
    await asyncio.wait_for(lease.task, timeout=2)

    assert channel.awareness == {}
    assert frames[-1] == {
        "method": "canvas.user_idle",
        "params": {**lease.params, "sender_id": "client-a"},
    }


@pytest.mark.asyncio
async def test_identity_change_during_validation_drops_the_frame():
    current = {"fingerprint": FINGERPRINT}
    broadcast = MagicMock()
    frames = []
    ws = MagicMock(send_json=AsyncMock())

    async def _load_then_replace():
        current["fingerprint"] = "sha256:" + ("d" * 64)
        return _state()

    channel = CanvasControlChannel(
        load_state=_load_then_replace,
        invalidate_recent_read=MagicMock(),
        identity_fingerprint=lambda: current["fingerprint"],
        broadcast=broadcast,
        fan_out_live=frames.append,
        validation_min_interval_s=0,
    )

    assert await channel.handle(
        ws,
        _frame("canvas.source_updated"),
        "client-a",
        expected_session_identity_fingerprint=FINGERPRINT,
    )

    broadcast.assert_not_called()
    assert frames == []
    ws.send_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_canvas_verbs_are_not_claimed(monkeypatch):
    send = AsyncMock()
    monkeypatch.setattr(session_transport, "send_message", send)
    channel = _channel()

    assert await channel.handle(MagicMock(), {"method": "message"}, "c") is False
    send.assert_not_awaited()


def test_default_awareness_ttl_is_bounded():
    assert 15.0 <= CANVAS_AWARENESS_TTL_S <= 60.0
