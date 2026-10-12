"""Turn-boundary recovery from a cloud sync that failed to start at attach.

``_attach_session`` resolves cloud config once. An agent that lost the race
against session-folder provisioning — or hit a transient WebDAV failure —
left ``_session.workspace_sync = None``, and since every use site is guarded
by ``if _session.workspace_sync:`` and nothing ever rebuilt it, the session
ran unsynced for its WHOLE life. ``_retry_cloud_sync_start`` re-resolves once
per turn boundary so that becomes a late start instead of a total loss.

knowledge-history/done/session_resume_cloud_sync_race_late_provision.md
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import agent.api.persistent_app as papp


def _session_stub() -> MagicMock:
    session = MagicMock()
    session.workspace_sync = None
    session.workspace_manager.path = "/workspace"
    session.workspace_manager.backend = MagicMock()
    return session


def _client_stub(ws_info: dict | None) -> MagicMock:
    client = MagicMock()
    client.get_thread_workspace = AsyncMock(return_value=ws_info)
    return client


def _coordinator_stub() -> MagicMock:
    coordinator = MagicMock()
    coordinator.pull_all = AsyncMock()
    coordinator.__len__ = MagicMock(return_value=1)
    return coordinator


class TestRetryCloudSyncStart:
    @pytest.mark.asyncio
    async def test_recovers_once_the_sync_target_exists(self):
        """The provisioning that lost the race lands seconds later — the next
        turn boundary must pick it up and start syncing."""
        session = _session_stub()
        coordinator = _coordinator_stub()
        events: list[tuple] = []

        with (
            patch.object(papp, "_session", session),
            patch.object(papp._session_identity, "_thread_id", "t1"),
            patch.object(
                papp, "_orchestrator_client", _client_stub({"cloud_sync": {"v": 2}})
            ),
            patch.object(papp, "_cloud_sync_retry_pending", True),
            patch.object(
                papp, "_build_sync_coordinator", MagicMock(return_value=coordinator)
            ),
            patch.object(papp, "_broadcast", lambda k, p: events.append((k, p))),
        ):
            await papp._retry_cloud_sync_start(3)

            assert session.workspace_sync is coordinator
            assert papp._cloud_sync_retry_pending is False

        coordinator.pull_all.assert_awaited_once()
        assert events == [("workspace_sync.recovered", {"turn_id": 3})]

    @pytest.mark.asyncio
    async def test_accepts_the_legacy_nc_session_folder_shim(self):
        """An orchestrator still returning the flat pre-refactor field must
        recover too — same shim the attach path applies."""
        session = _session_stub()
        coordinator = _coordinator_stub()
        build = MagicMock(return_value=coordinator)

        with (
            patch.object(papp, "_session", session),
            patch.object(papp._session_identity, "_thread_id", "t1"),
            patch.object(
                papp,
                "_orchestrator_client",
                _client_stub({"nc_session_folder": "sessions/t1"}),
            ),
            patch.object(papp, "_cloud_sync_retry_pending", True),
            patch.object(papp, "_build_sync_coordinator", build),
            patch.object(papp, "_broadcast", lambda k, p: None),
        ):
            await papp._retry_cloud_sync_start(1)
            assert session.workspace_sync is coordinator

        assert build.call_args.kwargs["cloud_cfg"]["webdav_url"].endswith(
            "sessions/t1/"
        )

    @pytest.mark.asyncio
    async def test_keeps_retrying_while_no_target_resolves(self):
        """Provisioning may still be in flight (or the cloud down). Stay
        pending and stay quiet — the degraded toast already fired at attach,
        and re-broadcasting it every turn would be noise."""
        session = _session_stub()
        events: list[tuple] = []

        with (
            patch.object(papp, "_session", session),
            patch.object(papp._session_identity, "_thread_id", "t1"),
            patch.object(papp, "_orchestrator_client", _client_stub({})),
            patch.object(papp, "_cloud_sync_retry_pending", True),
            patch.object(papp, "_broadcast", lambda k, p: events.append((k, p))),
        ):
            await papp._retry_cloud_sync_start(2)

            assert session.workspace_sync is None
            assert papp._cloud_sync_retry_pending is True

        assert events == []

    @pytest.mark.asyncio
    async def test_protected_thread_never_gets_a_live_sync_and_stops_retrying(self):
        """F-C1 fail-closed: a protected thread's only sanctioned live-write
        surface is the capture overlay. A retry must not hand it a WebDAV
        sync, and must stop asking — that verdict can't change mid-session."""
        session = _session_stub()
        build = MagicMock()

        with (
            patch.object(papp, "_session", session),
            patch.object(papp._session_identity, "_thread_id", "t1"),
            patch.object(
                papp,
                "_orchestrator_client",
                _client_stub({"protected_cloud": True, "cloud_sync": {"v": 2}}),
            ),
            patch.object(papp, "_cloud_sync_retry_pending", True),
            patch.object(papp, "_build_sync_coordinator", build),
            patch.object(papp, "_broadcast", lambda k, p: None),
        ):
            await papp._retry_cloud_sync_start(1)

            assert session.workspace_sync is None
            assert papp._cloud_sync_retry_pending is False

        build.assert_not_called()

    @pytest.mark.asyncio
    async def test_failed_pull_leaves_no_half_built_coordinator(self):
        """If the seed pull raises, the coordinator must NOT be installed —
        otherwise the turn-end push would run against a mount we know is
        broken. Stay pending and try again next turn."""
        session = _session_stub()
        coordinator = _coordinator_stub()
        coordinator.pull_all = AsyncMock(side_effect=RuntimeError("webdav 502"))
        events: list[tuple] = []

        with (
            patch.object(papp, "_session", session),
            patch.object(papp._session_identity, "_thread_id", "t1"),
            patch.object(
                papp, "_orchestrator_client", _client_stub({"cloud_sync": {"v": 2}})
            ),
            patch.object(papp, "_cloud_sync_retry_pending", True),
            patch.object(
                papp, "_build_sync_coordinator", MagicMock(return_value=coordinator)
            ),
            patch.object(papp, "_broadcast", lambda k, p: events.append((k, p))),
        ):
            await papp._retry_cloud_sync_start(4)

            assert session.workspace_sync is None
            assert papp._cloud_sync_retry_pending is True

        assert events == []

    @pytest.mark.asyncio
    async def test_orchestrator_unreachable_does_not_break_the_turn(self):
        """The retry runs inside the turn-start hook — an exception here would
        kill the loop for a purely optional recovery attempt."""
        session = _session_stub()
        client = MagicMock()
        client.get_thread_workspace = AsyncMock(side_effect=RuntimeError("conn reset"))

        with (
            patch.object(papp, "_session", session),
            patch.object(papp._session_identity, "_thread_id", "t1"),
            patch.object(papp, "_orchestrator_client", client),
            patch.object(papp, "_cloud_sync_retry_pending", True),
            patch.object(papp, "_broadcast", lambda k, p: None),
        ):
            await papp._retry_cloud_sync_start(1)

            assert papp._cloud_sync_retry_pending is True


class TestTurnStartWiring:
    @pytest.mark.asyncio
    async def test_turn_start_retries_when_pending(self):
        session = _session_stub()
        retry = AsyncMock()

        with (
            patch.object(papp, "_session", session),
            patch.object(papp, "_cloud_sync_retry_pending", True),
            patch.object(papp, "_retry_cloud_sync_start", retry),
            patch.object(papp, "_broadcast", lambda k, p: None),
        ):
            await papp._loop_on_turn_start(5)

        retry.assert_awaited_once_with(5)

    @pytest.mark.asyncio
    async def test_turn_start_skips_retry_when_sync_already_works(self):
        """Nothing to recover — and rebuilding a live coordinator would drop
        the one the turn loop is about to push through."""
        session = _session_stub()
        session.workspace_sync = _coordinator_stub()
        retry = AsyncMock()

        with (
            patch.object(papp, "_session", session),
            patch.object(papp, "_cloud_sync_retry_pending", True),
            patch.object(papp, "_retry_cloud_sync_start", retry),
            patch.object(papp, "_resilient_cloud_sync", AsyncMock(return_value=False)),
            patch.object(papp, "_broadcast", lambda k, p: None),
        ):
            await papp._loop_on_turn_start(5)

        retry.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_turn_start_skips_retry_when_not_pending(self):
        session = _session_stub()
        retry = AsyncMock()

        with (
            patch.object(papp, "_session", session),
            patch.object(papp, "_cloud_sync_retry_pending", False),
            patch.object(papp, "_retry_cloud_sync_start", retry),
            patch.object(papp, "_broadcast", lambda k, p: None),
        ):
            await papp._loop_on_turn_start(5)

        retry.assert_not_awaited()
