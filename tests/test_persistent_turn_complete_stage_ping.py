"""Agent-side turn-end cloud-stage ping (Slice C, Task 5).

Covers ``src/api/persistent_app.py``:
* ``_should_notify_cloud_stage()`` — the gate: only fires when the session's
  protected-cloud capture overlay is mounted AND active.
* ``_notify_cloud_stage()`` — the fire-and-forget POST itself. Never raises;
  mirrors the internal-call header pattern in
  ``src/tools/communication/messaging.py:197-226``.
* ``_loop_on_turn_complete()`` — wires the gate + ping at the end of the
  turn, after the existing workspace_sync push block.

No module in ``tests/`` previously covered ``_loop_on_turn_complete``
directly (checked via ``grep -rln "_loop_on_turn_complete" tests/``), so
this is a new file rather than an addition to an existing one.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import agent.api.persistent_app as papp


GENERATION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
ATTACH_TOKEN = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
AGENT_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"


# =============================================================================
# _should_notify_cloud_stage — the gate condition
# =============================================================================


class TestShouldNotifyCloudStage:
    def test_true_when_overlay_mounted_and_active(self):
        overlay = MagicMock()
        overlay.active = True
        session = MagicMock()
        session.overlay_mount_manager = overlay
        with patch.object(papp, "_session", session):
            assert papp._should_notify_cloud_stage() is True

    def test_false_when_overlay_inactive(self):
        overlay = MagicMock()
        overlay.active = False
        session = MagicMock()
        session.overlay_mount_manager = overlay
        with patch.object(papp, "_session", session):
            assert papp._should_notify_cloud_stage() is False

    def test_false_when_overlay_none(self):
        session = MagicMock()
        session.overlay_mount_manager = None
        with patch.object(papp, "_session", session):
            assert papp._should_notify_cloud_stage() is False

    def test_false_when_overlay_attr_missing(self):
        # A plain object with no overlay_mount_manager attribute at all
        # (getattr default path) must not raise.
        session = object()
        with patch.object(papp, "_session", session):
            assert papp._should_notify_cloud_stage() is False

    def test_false_when_session_none(self):
        with patch.object(papp, "_session", None):
            assert papp._should_notify_cloud_stage() is False


# =============================================================================
# _notify_cloud_stage — the fire-and-forget POST
# =============================================================================


def _mock_httpx_client(*, post_side_effect=None):
    """Build a ``httpx.AsyncClient`` replacement usable as ``async with``."""
    client = MagicMock()
    client.post = AsyncMock(
        side_effect=post_side_effect, return_value=MagicMock(status_code=200)
    )
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=client)
    ctx.__aexit__ = AsyncMock(return_value=False)
    client_cls = MagicMock(return_value=ctx)
    return client_cls, client


class TestNotifyCloudStage:
    @pytest.mark.asyncio
    async def test_posts_with_internal_key_header_and_thread_url(self, monkeypatch):
        monkeypatch.setenv("MCP_INTERNAL_KEY", "secret")
        monkeypatch.setenv("ORCHESTRATOR_URL", "http://orch:8085")
        client_cls, client = _mock_httpx_client()

        with (
            patch.object(papp._session_identity, "_thread_id", "tid-1"),
            patch("httpx.AsyncClient", client_cls),
        ):
            await papp._notify_cloud_stage(
                agent_id=AGENT_ID,
                session_runtime_generation=GENERATION,
                session_runtime_attach_token=ATTACH_TOKEN,
            )

        assert client_cls.call_args.kwargs["headers"] == {
            "X-Internal-Key": "secret",
            "X-Agent-ID": AGENT_ID,
            "X-Session-Runtime-Generation": GENERATION,
            "X-Session-Runtime-Attach-Token": ATTACH_TOKEN,
        }
        client.post.assert_awaited_once_with(
            "http://orch:8085/api/agents/threads/tid-1/cloud-stage"
        )

    @pytest.mark.asyncio
    async def test_defaults_orchestrator_url_when_env_unset(self, monkeypatch):
        monkeypatch.delenv("ORCHESTRATOR_URL", raising=False)
        monkeypatch.delenv("MCP_INTERNAL_KEY", raising=False)
        client_cls, client = _mock_httpx_client()

        with (
            patch.object(papp._session_identity, "_thread_id", "tid-2"),
            patch.object(papp._session_identity, "_attach_token", None),
            patch("httpx.AsyncClient", client_cls),
        ):
            await papp._notify_cloud_stage(
                agent_id=AGENT_ID,
                session_runtime_generation=GENERATION,
            )

        # No key configured -> no X-Internal-Key header at all.
        assert client_cls.call_args.kwargs["headers"] == {
            "X-Agent-ID": AGENT_ID,
            "X-Session-Runtime-Generation": GENERATION,
        }
        client.post.assert_awaited_once_with(
            "http://localhost:8085/api/agents/threads/tid-2/cloud-stage"
        )

    @pytest.mark.asyncio
    async def test_never_raises_on_post_failure(self, monkeypatch):
        monkeypatch.setenv("MCP_INTERNAL_KEY", "secret")
        client_cls, client = _mock_httpx_client(
            post_side_effect=RuntimeError("connection refused")
        )

        with (
            patch.object(papp._session_identity, "_thread_id", "tid-3"),
            patch.object(papp._session_identity, "_attach_token", None),
            patch("httpx.AsyncClient", client_cls),
        ):
            await papp._notify_cloud_stage(
                agent_id=AGENT_ID,
                session_runtime_generation=GENERATION,
            )  # must not raise

    @pytest.mark.asyncio
    async def test_never_raises_when_client_construction_fails(self, monkeypatch):
        monkeypatch.setenv("MCP_INTERNAL_KEY", "secret")
        with (
            patch.object(papp._session_identity, "_thread_id", "tid-4"),
            patch.object(papp._session_identity, "_attach_token", None),
            patch("httpx.AsyncClient", side_effect=RuntimeError("boom")),
        ):
            await papp._notify_cloud_stage(
                agent_id=AGENT_ID,
                session_runtime_generation=GENERATION,
            )  # must not raise


# =============================================================================
# _loop_on_turn_complete — wiring at turn end
# =============================================================================


class TestLoopOnTurnCompleteStagePing:
    def _mock_session(self, *, overlay_active):
        overlay = MagicMock()
        overlay.active = overlay_active
        session = MagicMock()
        session.auxiliary_llm = None  # short-circuits _wire_session_aux_archiver
        session.postgres_conn = None  # short-circuits the AI-message save + title
        session.workspace_sync = None  # short-circuits the cloud-sync push block
        session.tool_decisions = MagicMock()
        session.overlay_mount_manager = overlay
        return session

    @pytest.mark.asyncio
    async def test_schedules_ping_when_overlay_active(self):
        session = self._mock_session(overlay_active=True)
        mock_notify = AsyncMock()
        with (
            patch.object(papp, "_session", session),
            patch.object(papp._session_identity, "_thread_id", "tid"),
            patch.object(papp, "_broadcast", MagicMock()),
            patch.object(papp, "_notify_cloud_stage", mock_notify),
            patch.object(
                papp,
                "_orchestrator_client",
                MagicMock(agent_id=AGENT_ID),
            ),
            patch.object(papp._session_identity, "_session_generation", GENERATION),
            patch.object(papp._session_identity, "_attach_token", ATTACH_TOKEN),
        ):
            await papp._loop_on_turn_complete(turn_id=5, metrics={})
            await asyncio.sleep(0)  # let the fire-and-forget task run

        mock_notify.assert_awaited_once_with(
            "tid",
            agent_id=AGENT_ID,
            session_runtime_generation=GENERATION,
            session_runtime_attach_token=ATTACH_TOKEN,
        )

    @pytest.mark.asyncio
    async def test_does_not_schedule_ping_when_overlay_inactive(self):
        session = self._mock_session(overlay_active=False)
        mock_notify = AsyncMock()
        with (
            patch.object(papp, "_session", session),
            patch.object(papp._session_identity, "_thread_id", "tid"),
            patch.object(papp, "_broadcast", MagicMock()),
            patch.object(papp, "_notify_cloud_stage", mock_notify),
        ):
            await papp._loop_on_turn_complete(turn_id=5, metrics={})
            await asyncio.sleep(0)

        mock_notify.assert_not_called()

    @pytest.mark.asyncio
    async def test_does_not_schedule_ping_when_overlay_absent(self):
        session = self._mock_session(overlay_active=True)
        session.overlay_mount_manager = None
        mock_notify = AsyncMock()
        with (
            patch.object(papp, "_session", session),
            patch.object(papp._session_identity, "_thread_id", "tid"),
            patch.object(papp, "_broadcast", MagicMock()),
            patch.object(papp, "_notify_cloud_stage", mock_notify),
        ):
            await papp._loop_on_turn_complete(turn_id=5, metrics={})
            await asyncio.sleep(0)

        mock_notify.assert_not_called()
