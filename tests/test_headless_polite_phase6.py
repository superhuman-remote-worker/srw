"""Tests for Phase 6 of headless persistent sessions: polite/eager mode,
per-thread attention-sleep TTL, and the HeadlessConfig dataclass.

Live DB round-trips (JOIN users + COALESCE TTL resolution) are covered by
the smoke runbook. Here we exercise the contract surface of each new
code path with mocks.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.runtime.core.loader import (
    AgentConfig,
    HeadlessConfig,
    load_agent_config_from_dict,
)


# =============================================================================
# Section 1 — HeadlessConfig dataclass + parsing
# =============================================================================


class TestHeadlessConfigDefaults:
    def test_mode_defaults_to_eager(self):
        assert HeadlessConfig().mode == "eager"

    def test_attention_sleep_minutes_defaults_to_60(self):
        assert HeadlessConfig().attention_sleep_minutes == 60

    def test_agent_config_has_headless_field(self):
        ac = AgentConfig(agent_id="x", display_name="X")
        assert isinstance(ac.headless, HeadlessConfig)
        assert ac.headless.mode == "eager"


def _minimal(**overrides):
    data = {"agent_id": "t", "display_name": "T"}
    data.update(overrides)
    return data


class TestHeadlessConfigParsing:
    def test_missing_headless_uses_defaults(self):
        cfg = load_agent_config_from_dict(_minimal())
        assert cfg.headless.mode == "eager"
        assert cfg.headless.attention_sleep_minutes == 60

    def test_explicit_polite_mode(self):
        cfg = load_agent_config_from_dict(_minimal(headless={"mode": "polite"}))
        assert cfg.headless.mode == "polite"

    def test_attention_sleep_minutes_override(self):
        cfg = load_agent_config_from_dict(
            _minimal(headless={"attention_sleep_minutes": 15})
        )
        assert cfg.headless.attention_sleep_minutes == 15

    def test_headless_not_in_extra(self):
        """headless is a known field and must not leak into AgentConfig.extra."""
        cfg = load_agent_config_from_dict(_minimal(headless={"mode": "polite"}))
        assert "headless" not in cfg.extra

    def test_string_int_coerces(self):
        """YAML sometimes hands us "60" not 60 — must not crash."""
        cfg = load_agent_config_from_dict(
            _minimal(headless={"attention_sleep_minutes": "90"})
        )
        assert cfg.headless.attention_sleep_minutes == 90

    def test_none_attention_sleep_falls_back_to_default(self):
        cfg = load_agent_config_from_dict(
            _minimal(headless={"attention_sleep_minutes": None})
        )
        assert cfg.headless.attention_sleep_minutes == 60


# =============================================================================
# Section 2 — Polite-mode flip in _loop_get_user_input
# =============================================================================


def _reset_agent_globals():
    import agent.api.persistent_app as mod

    mod._session = None
    mod._thread_id = None
    mod._pinned_status_identity_enabled = False
    mod._pinned_runtime_generation_enabled = False
    mod._session_runtime_generation = None
    mod._session_runtime_attach_token = None
    mod._orchestrator_client = None
    mod._subscribers.clear()
    mod._loop_user_queue = None


def _install_session(*, turn_count: int, headless_mode: str = "eager"):
    """Install a fake _session with a HeadlessConfig-shaped config.headless."""
    import agent.api.persistent_app as mod

    session = MagicMock()
    session.turn_count = turn_count
    session.permission_mode = "supervised"
    session.tool_decisions = {}
    session.postgres_conn = None
    session.config = MagicMock()
    session.config.interactive.idle_timeout_minutes = 0
    session.config.headless = HeadlessConfig(mode=headless_mode)
    mod._session = session
    mod._thread_id = "thread-phase6"
    client = AsyncMock()
    client.update_thread_status = AsyncMock(return_value=True)
    mod._orchestrator_client = client
    mod._loop_user_queue = asyncio.Queue()
    return session, client


def test_reset_agent_globals_clears_pinned_identity_protocol() -> None:
    import agent.api.persistent_app as mod

    mod._pinned_status_identity_enabled = True
    mod._pinned_runtime_generation_enabled = True
    mod._session_runtime_generation = "leaked-generation"
    mod._session_runtime_attach_token = "leaked-attach"

    _reset_agent_globals()

    assert mod._pinned_status_identity_enabled is False
    assert mod._pinned_runtime_generation_enabled is False
    assert mod._session_runtime_generation is None
    assert mod._session_runtime_attach_token is None


class TestPoliteModeFlip:
    def setup_method(self):
        _reset_agent_globals()

    def teardown_method(self):
        _reset_agent_globals()

    @pytest.mark.asyncio
    async def test_polite_flips_with_subscribers_attached(self):
        """Polite mode flips awaiting_user even when a subscriber is present —
        this is the core Phase 6 behavioral change."""
        import agent.api.persistent_app as mod

        _, client = _install_session(turn_count=2, headless_mode="polite")
        mod._subscribers["ws-1"] = asyncio.Queue()
        await asyncio.sleep(0)
        client.update_thread_status.reset_mock()

        mod._loop_user_queue.put_nowait("hi")
        await mod._loop_get_user_input()
        await asyncio.sleep(0)

        client.update_thread_status.assert_awaited_with(
            "thread-phase6", "awaiting_user"
        )

    @pytest.mark.asyncio
    async def test_polite_still_skips_when_turn_count_is_zero(self):
        """Polite still respects the first-boot guard — flipping on the boot
        wait would email the user before they've sent a single message."""
        import agent.api.persistent_app as mod

        _, client = _install_session(turn_count=0, headless_mode="polite")
        mod._loop_user_queue.put_nowait("hi")

        await mod._loop_get_user_input()
        await asyncio.sleep(0)

        client.update_thread_status.assert_not_called()

    @pytest.mark.asyncio
    async def test_eager_with_subscribers_does_not_flip(self):
        """Phase 5 regression: eager mode + subscribers attached → no flip."""
        import agent.api.persistent_app as mod

        _, client = _install_session(turn_count=2, headless_mode="eager")
        mod._subscribers["ws-1"] = asyncio.Queue()
        await asyncio.sleep(0)
        client.update_thread_status.reset_mock()

        mod._loop_user_queue.put_nowait("hi")
        await mod._loop_get_user_input()
        await asyncio.sleep(0)

        for call in client.update_thread_status.await_args_list:
            assert call.args[1] != "awaiting_user"

    @pytest.mark.asyncio
    async def test_eager_untethered_still_flips(self):
        """Phase 5 regression: eager + no subs → flip awaiting_user."""
        import agent.api.persistent_app as mod

        _, client = _install_session(turn_count=2, headless_mode="eager")
        mod._loop_user_queue.put_nowait("hi")

        await mod._loop_get_user_input()
        await asyncio.sleep(0)

        client.update_thread_status.assert_awaited_with(
            "thread-phase6", "awaiting_user"
        )

    @pytest.mark.asyncio
    async def test_missing_headless_attr_defaults_to_eager(self):
        """Defensive: a session whose config has no headless attribute (legacy
        config or test mock) must not crash the loop — fall back to eager."""
        import agent.api.persistent_app as mod

        _, client = _install_session(turn_count=2, headless_mode="eager")
        # Strip only the headless attribute; keep interactive intact so
        # the downstream idle-timeout read works.
        del mod._session.config.headless

        mod._loop_user_queue.put_nowait("hi")
        await mod._loop_get_user_input()
        await asyncio.sleep(0)

        # Eager fallback + no subscribers → should flip.
        client.update_thread_status.assert_awaited_with(
            "thread-phase6", "awaiting_user"
        )


# =============================================================================
# Section 3 — Sweeper SQL hygiene: per-thread TTL resolution
# =============================================================================


class TestSweeperPerThreadTTL:
    """Contract test on the sweeper's SQL — captures the actual query and asserts
    it now JOINs users and COALESCEs three TTL sources. Doesn't run real SQL."""

    @pytest.mark.asyncio
    async def test_sweeper_sql_joins_users_and_coalesces(self, monkeypatch):
        from orchestrator.services import session_attention

        captured: dict = {}
        evt = asyncio.Event()

        async def _fake_fetch(query: str, *args):
            captured["query"] = query
            captured["args"] = args
            # Set the shutdown event after the first SELECT so the sweeper
            # loop exits on its next wait_for.
            evt.set()
            return []

        fake_conn = MagicMock()
        fake_conn.fetch = _fake_fetch

        class _Acquire:
            async def __aenter__(self_inner):
                return fake_conn

            async def __aexit__(self_inner, exc_type, exc, tb):
                return None

        # The store and suspension service are the sweeper's dependencies; the
        # presence promotion is a module-level name looked up in
        # ``session_attention`` at call time (monkeypatch restores it).
        fake_db = MagicMock()
        fake_db.acquire = lambda: _Acquire()
        monkeypatch.setattr(
            session_attention,
            "promote_expired_stateless_pauses",
            AsyncMock(return_value=[]),
        )
        # The default TTL is a module global read per sweep; a non-default
        # value proves the bound parameter comes from it, not a literal 60.
        monkeypatch.setattr(session_attention, "ATTENTION_SLEEP_MINUTES", 17)

        suspension = MagicMock()
        suspension.is_enabled = True
        suspension.suspend_thread_workspace = AsyncMock(return_value=False)
        dependencies = session_attention.SessionAttentionDependencies(
            store=fake_db,
            container_provisioner=MagicMock(),
            workspace_suspension=suspension,
            persistent_provisioner=None,
            persistent_thread_recycler=lambda: None,
            emit_session_provisioning_failure=AsyncMock(),
            thread_retirement_operations=MagicMock(),
            notification_service=MagicMock(),
            cockpit_url=lambda: "http://localhost:4200",
        )

        await session_attention.attention_sleep_sweeper(evt, dependencies=dependencies)

        q = captured.get("query", "")
        assert "LEFT JOIN users" in q
        assert "COALESCE(" in q
        assert "t.metadata->'config_override'->'headless'" in q
        assert "u.settings->'persistent_agent'" in q
        # Global default is the only bound parameter, passed as int.
        assert captured.get("args") == (int(session_attention.ATTENTION_SLEEP_MINUTES),)
