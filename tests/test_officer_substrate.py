"""Officer (centurion) substrate tests — S1+S2 of knowledge-base/knowledge/features/centurion.md.

Covers the config parse (shared helper, both loader paths), the ToolContext
sleep slot, the sleep tool, the TurnResult flag, the officer branch of
``_loop_get_user_input`` (durable-timer filing, backstop wake, never
``IdleTimeoutError``, no awaiting_user flip), the ready-mirror suppression,
and the session_wake officer helpers. Live wiring (drain → pod inject,
watchdog respawn) is covered by the k3d smoke runbook, not unit tests —
same split the attention-sleep suite uses.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.runtime.core.loader import OfficerConfig, _parse_officer_config
from agent.persistent_graph import TurnResult
from agent.tools.context import ToolContext
from agent.tools.core.officer import OFFICER_TOOLS_METADATA, create_officer_tools


def _watchdog_deps(db, provisioner, recycler, *, reconciliation):
    """What the watchdog task carries. The recycler is a callable because the
    application assigns that global during startup and the tick re-reads it;
    the rollout fence is a callable for the same reason a rebind on
    ``orchestrator.main`` used to work."""
    from orchestrator.services.officer_watchdog import OfficerWatchdogDependencies

    return OfficerWatchdogDependencies(
        store=db,
        persistent_provisioner=provisioner,
        persistent_thread_recycler=lambda: recycler,
        kick_officer_event_drain=MagicMock(),
        dispatch_officer_page=AsyncMock(return_value=None),
        conclude_conference_if_any=AsyncMock(),
        officer_runtime_verification_enabled=lambda: False,
        persistent_agent_reconciliation_enabled=lambda: reconciliation,
    )


@pytest.mark.asyncio
async def test_officer_missing_pod_delegates_to_lifecycle_owner(monkeypatch):
    """The watchdog observes; one shared recycler owns missing-pod repair."""
    from orchestrator.services import officer_watchdog

    thread = {
        "id": "11111111-1111-4111-8111-111111111111",
        "status": "suspended",
        "execution_lane": "pinned",
        "config_name": "session_base",
        "metadata": {"config_override": {"officer": {"enabled": True}}},
    }
    db = MagicMock()
    db.get_thread = AsyncMock(return_value=thread)
    db.get_pending_officer_timer = AsyncMock(return_value=None)
    wake = MagicMock()
    wake._resolve_live_agent = AsyncMock(return_value=None)
    provisioner = MagicMock()
    provisioner.is_available = True
    provisioner.expected_build_sha = "current"
    recycler = MagicMock()
    recycler.observe = AsyncMock(return_value=None)
    recycler.request_and_reconcile = AsyncMock()
    maintenance = AsyncMock(
        return_value=SimpleNamespace(
            authorized=False,
            state="lifecycle_pending",
            notification_due=False,
        )
    )
    # The watchdog calls the liveness check as its own module-level name, so
    # the stub goes there; everything else is a dependency field.
    monkeypatch.setattr(
        officer_watchdog, "maintain_officer_runtime_authorization", maintenance
    )
    dependencies = _watchdog_deps(db, provisioner, recycler, reconciliation=True)

    await officer_watchdog.officer_watchdog_check_one(
        thread, wake, dependencies=dependencies
    )

    maintenance.assert_awaited_once_with(thread, dependencies=dependencies)
    recycler.request_and_reconcile.assert_awaited_once_with(
        thread_id=thread["id"],
        reason="missing_pod",
        expected_build_sha="current",
        observation=None,
        expected_project_id="",
    )


@pytest.mark.asyncio
async def test_officer_watchdog_probe_miss_does_not_recycle_existing_pod(monkeypatch):
    from orchestrator.services import officer_watchdog
    from orchestrator.services.persistent_recycler import PersistentPodObservation

    thread = {
        "id": "11111111-1111-4111-8111-111111111111",
        "project_id": "22222222-2222-4222-8222-222222222222",
        "status": "active",
        "execution_lane": "pinned",
        "metadata": {"config_override": {"officer": {"enabled": True}}},
    }
    db = MagicMock()
    db.get_thread = AsyncMock(return_value=thread)
    db.get_pending_officer_timer = AsyncMock(return_value=None)
    wake = MagicMock()
    wake._resolve_live_agent = AsyncMock(return_value=None)
    provisioner = MagicMock(is_available=True, expected_build_sha="current")
    observation = PersistentPodObservation(
        thread_id=thread["id"],
        pod_name=f"persistent-{thread['id'][:12]}",
        pod_uid="pod-still-live",
        build_sha="current",
        phase="Running",
        ready=True,
        terminating=False,
        labels={
            "srw/component": "persistent-agent",
            "srw/thread-id": thread["id"],
        },
    )
    recycler = MagicMock()
    recycler.observe = AsyncMock(return_value=observation)
    recycler.request_and_reconcile = AsyncMock()
    maintenance = AsyncMock(
        return_value=SimpleNamespace(
            authorized=True,
            state="authorized",
            notification_due=False,
        )
    )
    # The watchdog calls the liveness check as its own module-level name, so
    # the stub goes there; everything else is a dependency field.
    monkeypatch.setattr(
        officer_watchdog, "maintain_officer_runtime_authorization", maintenance
    )
    dependencies = _watchdog_deps(db, provisioner, recycler, reconciliation=True)

    await officer_watchdog.officer_watchdog_check_one(
        thread, wake, dependencies=dependencies
    )

    recycler.observe.assert_awaited_once_with(thread["id"])
    recycler.request_and_reconcile.assert_not_awaited()


@pytest.mark.asyncio
async def test_officer_missing_pod_is_observation_only_while_rollout_fence_is_off(
    monkeypatch,
):
    from orchestrator.services import officer_watchdog

    thread = {
        "id": "11111111-1111-4111-8111-111111111111",
        "project_id": "22222222-2222-4222-8222-222222222222",
        "status": "suspended",
        "execution_lane": "pinned",
        "metadata": {"config_override": {"officer": {"enabled": True}}},
    }
    db = MagicMock()
    db.get_thread = AsyncMock(return_value=thread)
    db.get_pending_officer_timer = AsyncMock(return_value=None)
    wake = MagicMock()
    wake._resolve_live_agent = AsyncMock(return_value=None)
    provisioner = MagicMock(is_available=True, expected_build_sha="current")
    recycler = MagicMock()
    recycler.observe = AsyncMock(return_value=None)
    recycler.request_and_reconcile = AsyncMock()
    maintenance = AsyncMock(
        return_value=SimpleNamespace(
            authorized=False,
            state="lifecycle_pending",
            notification_due=False,
        )
    )
    # The watchdog calls the liveness check as its own module-level name, so
    # the stub goes there; everything else is a dependency field.
    monkeypatch.setattr(
        officer_watchdog, "maintain_officer_runtime_authorization", maintenance
    )
    dependencies = _watchdog_deps(db, provisioner, recycler, reconciliation=False)

    await officer_watchdog.officer_watchdog_check_one(
        thread, wake, dependencies=dependencies
    )

    recycler.observe.assert_not_awaited()
    recycler.request_and_reconcile.assert_not_awaited()


# ---------------------------------------------------------------------------
# Config parse
# ---------------------------------------------------------------------------


class TestOfficerConfigParse:
    def test_defaults_disabled(self):
        cfg = _parse_officer_config({})
        assert cfg.enabled is False
        assert cfg.sleep_min_minutes == 5
        assert cfg.sleep_max_minutes == 60
        assert cfg.max_concurrent_workers == 3

    def test_enabled_with_overrides(self):
        cfg = _parse_officer_config(
            {
                "officer": {
                    "enabled": True,
                    "sleep_min_minutes": 2,
                    "sleep_max_minutes": 30,
                    "max_concurrent_workers": 5,
                }
            }
        )
        assert cfg.enabled is True
        assert (cfg.sleep_min_minutes, cfg.sleep_max_minutes) == (2, 30)
        assert cfg.max_concurrent_workers == 5

    def test_min_clamped_to_max(self):
        cfg = _parse_officer_config(
            {"officer": {"sleep_min_minutes": 90, "sleep_max_minutes": 30}}
        )
        assert cfg.sleep_min_minutes == 30
        assert cfg.sleep_max_minutes == 30

    def test_backstop_floor_two_hours(self):
        # Backstop is insurance, not the wake mechanism — never below 2h.
        assert OfficerConfig(sleep_max_minutes=5).backstop_seconds == 120 * 60
        assert OfficerConfig(sleep_max_minutes=90).backstop_seconds == 180 * 60

    def test_dict_loader_path_parses_officer_and_keeps_extra_clean(self):
        from shared.runtime.core.loader import load_agent_config_from_dict

        config = load_agent_config_from_dict(
            {
                "agent_id": "officer-test",
                "display_name": "Officer Test",
                "officer": {"enabled": True, "sleep_max_minutes": 45},
            }
        )
        assert config.officer.enabled is True
        assert config.officer.sleep_max_minutes == 45
        # 'officer' is a known field — it must not leak into config.extra.
        assert "officer" not in (config.extra or {})


# ---------------------------------------------------------------------------
# ToolContext slot + sleep tool + TurnResult flag
# ---------------------------------------------------------------------------


def _bare_tool_context() -> ToolContext:
    """A ToolContext with only class-level defaults — the sleep slot needs
    nothing else, and constructing the full dataclass drags in managers."""
    return ToolContext.__new__(ToolContext)


class TestSleepSlotAndTool:
    def test_request_peek_consume_roundtrip(self):
        ctx = _bare_tool_context()
        assert ctx.peek_officer_sleep() is None
        ctx.request_officer_sleep({"minutes": 10, "reason": "r"})
        assert ctx.peek_officer_sleep() == {"minutes": 10, "reason": "r"}
        # Peek is non-destructive; consume clears.
        assert ctx.peek_officer_sleep() is not None
        assert ctx.consume_officer_sleep() == {"minutes": 10, "reason": "r"}
        assert ctx.peek_officer_sleep() is None
        assert ctx.consume_officer_sleep() is None

    @pytest.mark.asyncio
    async def test_sleep_tool_parks_request(self):
        ctx = _bare_tool_context()
        tools = create_officer_tools(ctx)
        by_name = {t.name: t for t in tools}
        sleep_tool = by_name["sleep"]
        assert set(by_name) == {"sleep", "notify_user"}
        assert "sleep" in OFFICER_TOOLS_METADATA
        assert "notify_user" in OFFICER_TOOLS_METADATA
        result = await sleep_tool.ainvoke({"minutes": 30, "reason": "3 jobs healthy"})
        assert "Wake-up call filed" in result
        parked = ctx.peek_officer_sleep()
        assert parked is not None and parked["minutes"] == 30
        assert parked["reason"] == "3 jobs healthy"

    def test_turn_result_flag_defaults_false(self):
        r = TurnResult(turn_id=0, messages_added=0, tool_calls_made=0)
        assert r.ended_by_sleep is False


# ---------------------------------------------------------------------------
# _loop_get_user_input — officer branch
# ---------------------------------------------------------------------------


def _reset_agent_globals():
    import agent.api.persistent_app as mod

    mod._session = None
    mod._session_identity._thread_id = None
    mod._session_identity._status_contract = False
    mod._session_identity._runtime_contract = False
    mod._session_identity._session_generation = None
    mod._session_identity._attach_token = None
    mod._orchestrator_client = None
    mod._subscribers.clear()
    mod._session_input._queue = None


def _install_officer_session(*, officer_cfg, turn_count: int = 2):
    import agent.api.persistent_app as mod

    session = MagicMock()
    session.turn_count = turn_count
    session.config = MagicMock()
    session.config.interactive.idle_timeout_minutes = 0
    session.config.headless.mode = "eager"
    session.config.officer = officer_cfg
    session.tool_context = None
    mod._session = session
    mod._session_identity._thread_id = "thread-test-uuid"
    client = AsyncMock()
    client.update_thread_status = AsyncMock(return_value=True)
    client.file_officer_wake = AsyncMock(return_value=True)
    mod._orchestrator_client = client
    mod._session_input._queue = asyncio.Queue()
    return session, client


class TestOfficerInputWait:
    def setup_method(self):
        _reset_agent_globals()

    def teardown_method(self):
        _reset_agent_globals()

    @pytest.mark.asyncio
    async def test_sleep_request_files_clamped_wake_and_queue_wins(self):
        import agent.api.persistent_app as mod

        session, client = _install_officer_session(
            officer_cfg=OfficerConfig(
                enabled=True, sleep_min_minutes=5, sleep_max_minutes=60
            )
        )
        session.tool_context = MagicMock()
        session.tool_context.consume_officer_sleep = MagicMock(
            return_value={"minutes": 500, "reason": "long haul"}
        )
        await mod._session_input.queue.put({"content": "hi", "role": "human"})

        item = await mod._session_input.get_user_input()

        assert item["content"] == "hi"
        # Filing runs as a task — let it complete.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        client.file_officer_wake.assert_awaited_once_with(
            "thread-test-uuid", 60, "long haul"
        )

    @pytest.mark.asyncio
    async def test_backstop_returns_labeled_wake_never_raises(self):
        import agent.api.persistent_app as mod
        from agent.persistent_graph import IdleTimeoutError

        _install_officer_session(
            officer_cfg=SimpleNamespace(
                enabled=True,
                sleep_min_minutes=1,
                sleep_max_minutes=2,
                backstop_seconds=0.05,
            )
        )

        try:
            item = await mod._session_input.get_user_input()
        except IdleTimeoutError:  # pragma: no cover - the regression this pins
            pytest.fail("officer session must never raise IdleTimeoutError")

        assert item["role"] == "event"
        assert item["content"].startswith("[backstop wake]")

    @pytest.mark.asyncio
    async def test_officer_never_flips_awaiting_user(self):
        import agent.api.persistent_app as mod

        _, client = _install_officer_session(
            officer_cfg=OfficerConfig(enabled=True), turn_count=3
        )
        assert not mod._subscribers  # untethered — the flip condition
        await mod._session_input.queue.put({"content": "x", "role": "human"})

        await mod._session_input.get_user_input()
        await asyncio.sleep(0)

        client.update_thread_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_sleep_request_files_nothing(self):
        import agent.api.persistent_app as mod

        session, client = _install_officer_session(
            officer_cfg=OfficerConfig(enabled=True)
        )
        session.tool_context = MagicMock()
        session.tool_context.consume_officer_sleep = MagicMock(return_value=None)
        await mod._session_input.queue.put({"content": "x", "role": "human"})

        await mod._session_input.get_user_input()
        await asyncio.sleep(0)

        # The watchdog files implicit sleep_max — the transport must not.
        client.file_officer_wake.assert_not_awaited()


class TestReadyMirrorSuppression:
    def setup_method(self):
        _reset_agent_globals()

    def teardown_method(self):
        _reset_agent_globals()

    @pytest.mark.asyncio
    async def test_officer_ready_skips_nats(self, monkeypatch):
        import agent.api.persistent_app as mod

        _install_officer_session(officer_cfg=OfficerConfig(enabled=True))
        monkeypatch.setenv("SESSION_BOUND_THREAD_ID", "thread-test-uuid")
        nats_probe = AsyncMock(return_value=None)
        monkeypatch.setattr(mod, "_ensure_nats_client", nats_probe)

        await mod.emit_session_event("ready", {})

        nats_probe.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_officer_ready_still_mirrors(self, monkeypatch):
        import agent.api.persistent_app as mod

        # MagicMock config: officer gate must NOT trip on truthy mocks.
        session = MagicMock()
        mod._session = session
        monkeypatch.setenv("SESSION_BOUND_THREAD_ID", "thread-test-uuid")
        nats_probe = AsyncMock(return_value=None)
        monkeypatch.setattr(mod, "_ensure_nats_client", nats_probe)

        await mod.emit_session_event("ready", {})

        nats_probe.assert_awaited()


# ---------------------------------------------------------------------------
# session_wake officer helpers (orchestrator side)
# ---------------------------------------------------------------------------


class TestSessionWakeOfficerHelpers:
    @pytest.mark.asyncio
    async def test_file_officer_timer_enqueues_future_fire_at(self):
        from orchestrator.services import session_wake

        db = MagicMock()
        db.enqueue_session_wake_event = AsyncMock(return_value=True)

        before = datetime.now(timezone.utc)
        ok = await session_wake.file_officer_timer(db, "t-1", 30, "waiting")

        assert ok is True
        kwargs = db.enqueue_session_wake_event.await_args.kwargs
        assert kwargs["source"] == "timer"
        assert kwargs["dedup_key"] == "timer"
        assert kwargs["payload"]["minutes"] == 30
        fire_at = kwargs["fire_at"]
        assert timedelta(minutes=29) < (fire_at - before) < timedelta(minutes=31)

    @pytest.mark.asyncio
    async def test_file_officer_timer_never_raises(self):
        from orchestrator.services import session_wake

        db = MagicMock()
        db.enqueue_session_wake_event = AsyncMock(side_effect=RuntimeError("boom"))
        assert await session_wake.file_officer_timer(db, "t-1", 30, "") is False

    @pytest.mark.asyncio
    async def test_notify_officer_routes_to_project_officer(self):
        from orchestrator.services import session_wake

        db = MagicMock()
        db.get_officer_thread_for_project = AsyncMock(
            return_value={"id": "officer-thread"}
        )
        db.enqueue_session_wake_event = AsyncMock(return_value=True)

        ok = await session_wake.notify_officer(
            db, "proj-1", source="job_transition", dedup_key="ab12:completed"
        )

        assert ok is True
        args = db.enqueue_session_wake_event.await_args
        assert args.args[0] == "officer-thread"
        assert args.kwargs["source"] == "job_transition"

    @pytest.mark.asyncio
    async def test_notify_officer_noop_without_officer(self):
        from orchestrator.services import session_wake

        db = MagicMock()
        db.get_officer_thread_for_project = AsyncMock(return_value=None)
        db.enqueue_session_wake_event = AsyncMock()

        ok = await session_wake.notify_officer(
            db, "proj-1", source="job_transition", dedup_key="k"
        )

        assert ok is False
        db.enqueue_session_wake_event.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_notify_owning_officers_scopes_to_each_projects_officer(self):
        """Two projects with a job-derived fleet event: only the project with
        a commissioned officer is woken, and only with its own payload."""
        from orchestrator.services import session_wake

        officers = {"proj-a": {"id": "thread-a"}}
        db = MagicMock()
        db.get_officer_thread_for_project = AsyncMock(
            side_effect=lambda project_id: officers.get(project_id)
        )
        db.enqueue_session_wake_event = AsyncMock(return_value=True)

        enqueued = await session_wake.notify_owning_officers(
            db,
            {
                "proj-a": {"summary": "1 job(s) recovered: aaaa1111"},
                "proj-b": {"summary": "1 job(s) recovered: bbbb2222"},
            },
            source="fleet",
            dedup_key="fleet:lease_recovered",
        )

        assert enqueued == 1
        db.enqueue_session_wake_event.assert_awaited_once()
        args = db.enqueue_session_wake_event.await_args
        assert args.args[0] == "thread-a"
        assert args.kwargs["source"] == "fleet"
        assert args.kwargs["dedup_key"] == "fleet:lease_recovered"
        assert args.kwargs["payload"] == {"summary": "1 job(s) recovered: aaaa1111"}
        assert args.kwargs["project_id"] == "proj-a"

    @pytest.mark.asyncio
    async def test_notify_owning_officers_without_an_officer_notifies_nobody(self):
        """No commissioned officer means no notification — the event is not
        rerouted to other projects' officers."""
        from orchestrator.services import session_wake

        db = MagicMock()
        db.get_officer_thread_for_project = AsyncMock(return_value=None)
        db.enqueue_session_wake_event = AsyncMock()

        enqueued = await session_wake.notify_owning_officers(
            db,
            {"proj-a": {"summary": "x"}},
            source="fleet",
            dedup_key="fleet:orphans_recovered",
        )

        assert enqueued == 0
        db.enqueue_session_wake_event.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_notify_owning_officers_never_raises_and_skips_blank_keys(self):
        from orchestrator.services import session_wake

        db = MagicMock()
        db.get_officer_thread_for_project = AsyncMock(side_effect=RuntimeError("db"))
        db.enqueue_session_wake_event = AsyncMock()

        enqueued = await session_wake.notify_owning_officers(
            db,
            {"": {"summary": "skipped"}, "proj-a": {"summary": "swallowed"}},
            source="fleet",
            dedup_key="fleet:agents_offline",
        )

        assert enqueued == 0
        db.enqueue_session_wake_event.assert_not_awaited()

    def test_format_officer_wake_renders_sitrep_bracket(self):
        from orchestrator.services import session_wake

        text = session_wake._format_officer_wake(
            [
                {
                    "source": "timer",
                    "dedup_key": "timer",
                    "payload": {"minutes": 30, "reason": "quiet"},
                },
                {
                    "source": "job_transition",
                    "dedup_key": "ab12cd34:completed",
                    "payload": {"status": "completed"},
                },
            ]
        )
        assert text.startswith("[SITREP]")
        assert "2 reason(s)" in text
        assert "timer: slept ~30 min" in text
        assert "ab12cd34:completed" in text

    def test_timer_debounce_is_zero(self):
        from orchestrator.services import session_wake

        assert session_wake.OFFICER_DEBOUNCE_BY_SOURCE["timer"] == 0


# ---------------------------------------------------------------------------
# Boot-WS watchdog exemption (S3 k3d smoke defect 7)
# ---------------------------------------------------------------------------


class TestBootWsWatchdogOfficerExemption:
    @pytest.mark.asyncio
    async def test_officer_session_is_never_boot_ws_killed(self):
        """Officers are headless by design — the abandoned-during-creation
        watchdog must stand down entirely, or every officer dies 600s after
        boot (observed live: exiting (likely abandoned during creation))."""
        import agent.api.persistent_app as mod

        _reset_agent_globals()
        _install_officer_session(officer_cfg=OfficerConfig(enabled=True))
        mod._ws_connected_event = asyncio.Event()  # never set
        terminated = []

        async def _fake_terminate(reason):
            terminated.append(reason)

        original = mod._terminate_session
        mod._terminate_session = _fake_terminate
        try:
            # Watchdog must return immediately — a 0-second timeout would
            # otherwise terminate on the spot.
            await asyncio.wait_for(mod._boot_ws_watchdog(0), timeout=1.0)
        finally:
            mod._terminate_session = original
            _reset_agent_globals()
        assert terminated == []

    @pytest.mark.asyncio
    async def test_normal_session_still_boot_ws_killed(self):
        import agent.api.persistent_app as mod

        _reset_agent_globals()
        _install_officer_session(officer_cfg=OfficerConfig(enabled=False))
        mod._ws_connected_event = asyncio.Event()
        terminated = []

        async def _fake_terminate(reason):
            terminated.append(reason)

        original_terminate = mod._terminate_session
        original_exit = mod._schedule_exit
        mod._terminate_session = _fake_terminate
        mod._schedule_exit = lambda delay=0: None
        try:
            await asyncio.wait_for(mod._boot_ws_watchdog(0), timeout=5.0)
        finally:
            mod._terminate_session = original_terminate
            mod._schedule_exit = original_exit
            _reset_agent_globals()
        assert terminated == ["boot_ws_timeout"]
