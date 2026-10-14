"""Host-side unit tests for docker/browser-exec streaming additions.

The daemon file has no .py extension and must stay stdlib-importable
(browser-use imports are lazy), so we load it via SourceFileLoader.
"""

import asyncio
import importlib.machinery
import importlib.util
import json
import sys
import time
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO = Path(__file__).resolve().parents[3]


def _load():
    loader = importlib.machinery.SourceFileLoader(
        "browser_exec_mod", str(REPO / "docker" / "browser-exec")
    )
    spec = importlib.util.spec_from_loader("browser_exec_mod", loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


BE = _load()


class TestFramingCodec:
    def test_roundtrip(self):
        async def run():
            wire = BE.encode_stream_frame(BE.T_STATE, b'{"baton":"agent"}')
            reader = asyncio.StreamReader()
            reader.feed_data(wire)
            reader.feed_eof()
            return await BE.read_stream_frame(reader)

        ftype, payload = asyncio.run(run())
        assert ftype == BE.T_STATE
        assert payload == b'{"baton":"agent"}'

    def test_length_covers_type_byte(self):
        wire = BE.encode_stream_frame(BE.T_HELLO, b"abc")
        assert wire[:4] == (4).to_bytes(4, "big")
        assert wire[4] == BE.T_HELLO

    def test_oversize_encode_rejected(self):
        with pytest.raises(ValueError):
            BE.encode_stream_frame(BE.T_FRAME, b"x" * (BE.MAX_STREAM_FRAME + 1))

    def test_oversize_read_rejected(self):
        async def run():
            reader = asyncio.StreamReader()
            reader.feed_data((BE.MAX_STREAM_FRAME + 100).to_bytes(4, "big") + b"\x02")
            reader.feed_eof()
            await BE.read_stream_frame(reader)

        with pytest.raises(ValueError):
            asyncio.run(run())

    def test_frame_payload_roundtrip(self):
        header = {"generation": "g1", "w": 1280, "h": 720, "ts": 1.5}
        jpeg = b"\xff\xd8fakejpeg"
        header2, jpeg2 = BE.decode_frame_payload(BE.encode_frame_payload(header, jpeg))
        assert header2 == header
        assert jpeg2 == jpeg


class _StubFiles:
    def url_for(self, url):
        if url.startswith("file://"):
            return "http://127.0.0.1:45678/mock.html"
        return url


class _FakePageRegister:
    def __init__(self):
        self.callbacks = {}
        self.counts = {}

    def __getattr__(self, name):
        def register(callback):
            self.callbacks[name] = callback
            self.counts[name] = self.counts.get(name, 0) + 1

        return register


class _FakePageSend:
    def __init__(self, client):
        self.client = client

    async def getLayoutMetrics(self, *, session_id):
        self.client.calls.append(("metrics", session_id))
        return {"cssVisualViewport": {"clientWidth": 1024, "clientHeight": 640}}

    async def getFrameTree(self, *, session_id):
        self.client.calls.append(("frame_tree", session_id))
        target = session_id.removeprefix("session-")
        return {
            "frameTree": {
                "frame": {
                    "id": f"frame-{target}",
                    "url": f"https://{target}.example.test/",
                }
            }
        }

    async def startScreencast(self, *, params, session_id):
        self.client.calls.append(("start", session_id, params))

    async def stopScreencast(self, *, session_id):
        self.client.calls.append(("stop", session_id))

    async def screencastFrameAck(self, *, params, session_id):
        self.client.calls.append(("ack", session_id, params["sessionId"]))


class _FakeInputSend:
    def __init__(self):
        self.calls = []

    async def dispatchMouseEvent(self, **kwargs):
        self.calls.append(("mouse", kwargs))

    async def dispatchKeyEvent(self, **kwargs):
        self.calls.append(("key", kwargs))

    async def insertText(self, *, params, session_id):
        self.calls.append(("insertText", params, session_id))


class _FakeCdpClient:
    def __init__(self):
        page_register = _FakePageRegister()
        self.register = SimpleNamespace(Page=page_register)
        self.calls = []
        self.send = SimpleNamespace(
            Page=_FakePageSend(self),
            Input=_FakeInputSend(),
        )


class _FakeBrowserSession:
    def __init__(self, client=None):
        self.client = client or _FakeCdpClient()
        self.agent_focus_target_id = "A"
        self.cdp_calls = []
        self.url = "https://A.example.test/"
        self.title = "Target A"

    async def get_or_create_cdp_session(self, target_id=None):
        target_id = target_id or self.agent_focus_target_id
        self.cdp_calls.append(target_id)
        return SimpleNamespace(
            target_id=target_id,
            session_id=f"session-{target_id}",
            cdp_client=self.client,
        )

    async def get_current_page_url(self):
        return self.url

    async def get_current_page_title(self):
        return self.title


class TestValidateUserNav:
    def test_https_passes(self):
        assert (
            BE.validate_user_nav("https://example.com/x", _StubFiles())
            == "https://example.com/x"
        )

    def test_schemeless_gets_https(self):
        assert (
            BE.validate_user_nav("example.com", _StubFiles()) == "https://example.com"
        )

    def test_javascript_blocked(self):
        with pytest.raises(ValueError):
            BE.validate_user_nav("javascript:alert(1)", _StubFiles())

    def test_data_blocked(self):
        with pytest.raises(ValueError):
            BE.validate_user_nav("data:text/html,<b>x</b>", _StubFiles())

    def test_metadata_host_blocked(self):
        with pytest.raises(ValueError):
            BE.validate_user_nav("http://metadata.google.internal/", _StubFiles())

    def test_k8s_internal_blocked(self):
        with pytest.raises(ValueError):
            BE.validate_user_nav(
                "http://orchestrator.default.svc.cluster.local/",
                _StubFiles(),
            )

    def test_file_translated_then_allowed(self):
        assert (
            BE.validate_user_nav(
                "file:///home/agent-host/workspace/mock.html", _StubFiles()
            )
            == "http://127.0.0.1:45678/mock.html"
        )

    def test_empty_rejected(self):
        with pytest.raises(ValueError):
            BE.validate_user_nav("   ", _StubFiles())


class TestStreamHub:
    def test_mint_generation_sets_identity_and_initial_baton(self):
        hub = BE.StreamHub()
        assert hub.baton == "agent"
        hub.mint_generation("user")
        assert hub.baton == "user"
        assert hub.generation and hub.token and len(hub.token) == 64

    def test_state_payload_shape(self):
        hub = BE.StreamHub()
        hub.mint_generation()
        state = hub.state_payload()
        assert set(state) >= {
            "generation",
            "baton",
            "viewport",
            "url",
            "title",
            "loading",
        }

    def test_page_state_change_notifies_viewers_once(self):
        hub = BE.StreamHub()
        notifications = []
        hub.on_state_change = lambda: notifications.append(hub.state_payload())

        hub.update_page_state(
            url="https://example.com/",
            title="Example",
        )
        hub.update_page_state(
            url="https://example.com/",
            title="Example",
        )

        assert len(notifications) == 1
        assert notifications[0]["url"] == "https://example.com/"
        assert notifications[0]["title"] == "Example"

    def test_broadcast_drops_oldest_for_laggards(self):
        async def run():
            hub = BE.StreamHub()
            queue = asyncio.Queue(maxsize=2)
            hub.add_viewer(queue, 3)
            for frame in (b"f1", b"f2", b"f3"):
                hub.broadcast(frame)
            assert queue.qsize() == 2
            assert await queue.get() == b"f2"
            assert await queue.get() == b"f3"

        asyncio.run(run())

    def test_frame_broadcast_supersedes_queued_frame(self):
        async def run():
            hub = BE.StreamHub()
            queue = BE.ViewerQueue(maxsize=4)
            hub.add_viewer(queue, 3)
            state = BE.encode_stream_frame(BE.T_STATE, b'{"s":1}')
            frames = [
                BE.encode_stream_frame(BE.T_FRAME, b"jpeg-%d" % i) for i in range(3)
            ]
            hub.broadcast(state)
            for frame in frames:
                hub.broadcast_frame(frame)

            # The two older frames were superseded in place: the viewer sees
            # ordered state plus only the newest frame, never a backlog.
            assert queue.qsize() == 2
            assert queue.get_nowait() == state
            assert queue.get_nowait() == frames[-1]

        asyncio.run(run())

    def test_frame_broadcast_drop_oldest_fallback_for_plain_queue(self):
        async def run():
            hub = BE.StreamHub()
            queue = asyncio.Queue(maxsize=2)
            hub.add_viewer(queue, 3)
            frames = [
                BE.encode_stream_frame(BE.T_FRAME, b"jpeg-%d" % i) for i in range(3)
            ]
            for frame in frames:
                hub.broadcast_frame(frame)
            assert queue.get_nowait() == frames[1]
            assert queue.get_nowait() == frames[2]

        asyncio.run(run())

    def test_mixed_replica_limits_only_tighten_until_zero_viewers(self):
        hub = BE.StreamHub()
        first = hub.add_viewer(asyncio.Queue(), 5)
        second = hub.add_viewer(asyncio.Queue(), 6)

        assert first is not None and second is not None
        assert hub.viewer_limit == 5
        assert hub.add_viewer(asyncio.Queue(), 1) is None
        assert hub.viewer_limit == 1

        hub.remove_viewer(first)
        hub.remove_viewer(second)
        assert hub.viewer_limit is None
        assert hub.add_viewer(asyncio.Queue(), 6) is not None
        assert hub.viewer_limit == 6

    def test_auto_release_after_last_viewer_leaves(self, monkeypatch):
        monkeypatch.setattr(BE, "BATON_GRACE_S", 0.05)

        async def run():
            hub = BE.StreamHub()
            hub.mint_generation("user")
            viewer_id = hub.add_viewer(asyncio.Queue(maxsize=4), 3)
            hub.remove_viewer(viewer_id)
            assert hub.baton == "user"
            await asyncio.sleep(0.15)
            assert hub.baton == "agent"

        asyncio.run(run())

    def test_reconnect_cancels_auto_release(self, monkeypatch):
        monkeypatch.setattr(BE, "BATON_GRACE_S", 0.05)

        async def run():
            hub = BE.StreamHub()
            hub.mint_generation("user")
            viewer_id = hub.add_viewer(asyncio.Queue(maxsize=4), 3)
            hub.remove_viewer(viewer_id)
            hub.add_viewer(asyncio.Queue(maxsize=4), 3)
            await asyncio.sleep(0.15)
            assert hub.baton == "user"

        asyncio.run(run())


class TestScreencastCdp:
    @staticmethod
    def _adapter():
        session = _FakeBrowserSession()
        hub = BE.StreamHub()
        hub.mint_generation()
        return BE.ScreencastCdp(session, hub), session, hub

    def test_main_frame_loading_and_subframes(self):
        async def run():
            adapter, session, hub = self._adapter()
            await adapter.start("A")

            adapter._on_frame_started_loading_event(
                {"frameId": "subframe"}, "session-A"
            )
            assert hub.state_extra["loading"] is False
            adapter._on_frame_started_loading_event({"frameId": "frame-A"}, "session-A")
            assert hub.state_extra["loading"] is True
            adapter._on_frame_stopped_loading_event(
                {"frameId": "subframe"}, "session-A"
            )
            assert hub.state_extra["loading"] is True

            session.url = "https://A.example.test/loaded"
            session.title = "Loaded A"
            adapter._on_frame_stopped_loading_event({"frameId": "frame-A"}, "session-A")
            await asyncio.sleep(0)

            assert hub.state_extra == {
                "url": "https://A.example.test/loaded",
                "title": "Loaded A",
                "loading": False,
            }

        asyncio.run(run())

    def test_target_switch_rejects_stale_events_and_acks_old_frame(self):
        async def run():
            adapter, session, hub = self._adapter()
            queue = asyncio.Queue()
            hub.add_viewer(queue, 3)
            await adapter.start("A")
            session.agent_focus_target_id = "B"
            session.url = "https://B.example.test/"
            session.title = "Target B"
            await adapter.start("B")
            state = dict(hub.state_extra)

            adapter._on_navigated_event(
                {"frame": {"id": "old", "url": "https://stale.test/"}},
                "session-A",
            )
            adapter._on_frame_started_loading_event({"frameId": "frame-A"}, "session-A")
            await adapter._handle_frame(
                {
                    "data": BE.base64.b64encode(b"jpeg").decode(),
                    "metadata": {},
                    "sessionId": 91,
                },
                "session-A",
            )

            assert hub.state_extra == state
            assert queue.empty()
            assert ("stop", "session-A") in session.client.calls
            assert any(
                call[:2] == ("start", "session-B") for call in session.client.calls
            )
            assert ("ack", "session-A", 91) in session.client.calls

        asyncio.run(run())

    def test_callbacks_are_not_duplicated_across_stop_start_cycles(self):
        async def run():
            adapter, session, _ = self._adapter()
            for _ in range(3):
                await adapter.start("A")
                await adapter.stop()
            await adapter.start("A")

            assert session.client.register.Page.counts == {
                "screencastFrame": 1,
                "frameNavigated": 1,
                "frameStartedLoading": 1,
                "frameStoppedLoading": 1,
            }

        asyncio.run(run())


class TestInsertTextDispatch:
    def test_insert_text_dispatches_capped_and_skips_empty(self):
        async def run():
            adapter, session, hub = TestScreencastCdp._adapter()
            await adapter.start("A")

            await adapter.dispatch_input(
                {"kind": "insertText", "params": {"text": "hunter2"}}
            )
            oversized = "x" * (BE.INSERT_TEXT_MAX_CHARS + 5)
            await adapter.dispatch_input(
                {"kind": "insertText", "params": {"text": oversized}}
            )
            await adapter.dispatch_input({"kind": "insertText", "params": {}})

            calls = session.client.send.Input.calls
            assert calls[0] == ("insertText", {"text": "hunter2"}, "session-A")
            assert calls[1][1] == {"text": "x" * BE.INSERT_TEXT_MAX_CHARS}
            assert len(calls) == 2

        asyncio.run(run())


class TestActiveTargetLifecycle:
    class _Adapter:
        def __init__(self):
            self.targets = []
            self.stops = 0

        async def start(self, target_id=None):
            self.targets.append(target_id)

        async def stop(self):
            self.stops += 1

    def test_focus_changes_coalesce_to_newest_target(self):
        async def run():
            daemon = BE.BrowserDaemon()
            session = _FakeBrowserSession()
            adapter = self._Adapter()
            daemon._session = session
            daemon._screencast = adapter
            daemon.hub.add_viewer(asyncio.Queue(), 3)
            daemon._focus_change_revision = 1
            task = asyncio.create_task(daemon._follow_agent_focus())
            daemon._focus_change_task = task
            session.agent_focus_target_id = "B"
            daemon._focus_change_revision = 2

            await task

            assert adapter.targets == ["B"]

        asyncio.run(run())

    def test_focus_change_does_not_switch_without_viewers(self):
        async def run():
            daemon = BE.BrowserDaemon()
            daemon._session = _FakeBrowserSession()
            adapter = self._Adapter()
            daemon._screencast = adapter
            daemon._focus_change_revision = 1

            await daemon._follow_agent_focus()

            assert adapter.targets == []

        asyncio.run(run())

    def test_browser_close_stops_and_discards_adapter(self):
        async def run():
            daemon = BE.BrowserDaemon()
            adapter = self._Adapter()

            class Session:
                stopped = False

                async def stop(self):
                    self.stopped = True

            session = Session()
            daemon._session = session
            daemon._screencast = adapter
            daemon.hub.mint_generation()

            await daemon._close_session()

            assert adapter.stops == 1
            assert daemon._screencast is None
            assert daemon._session is None
            assert session.stopped is True
            assert daemon.hub.generation is None

        asyncio.run(run())

    def test_target_blank_click_explicitly_switches_browser_use_focus(
        self, monkeypatch
    ):
        # `_switch_to_new_click_target` is the one exercised path that trips the
        # daemon's lazy `browser_use` import (see module docstring). browser-use
        # is a workspace-image dependency, not a CI/test one, so stand in the
        # single event class it imports. monkeypatch.setitem unwinds the
        # sys.modules entries afterwards so the stub can't leak into other tests.
        class SwitchTabEvent:
            def __init__(self, target_id):
                self.target_id = target_id

        events = types.ModuleType("browser_use.browser.events")
        events.SwitchTabEvent = SwitchTabEvent
        for name, mod in (
            ("browser_use", types.ModuleType("browser_use")),
            ("browser_use.browser", types.ModuleType("browser_use.browser")),
            ("browser_use.browser.events", events),
        ):
            monkeypatch.setitem(sys.modules, name, mod)

        async def run():
            daemon = BE.BrowserDaemon()
            dispatched = []

            class DispatchResult:
                def __await__(self):
                    return self.wait().__await__()

                async def wait(self):
                    return None

                async def event_result(self, **kwargs):
                    assert kwargs == {"raise_if_any": True}

            class EventBus:
                def dispatch(self, event):
                    dispatched.append(event)
                    return DispatchResult()

            class Session:
                event_bus = EventBus()

                async def get_tabs(self):
                    return [
                        SimpleNamespace(target_id="A"),
                        SimpleNamespace(target_id="B"),
                    ]

            await daemon._switch_to_new_click_target(Session(), {"A"})

            assert len(dispatched) == 1
            assert dispatched[0].target_id == "B"

        asyncio.run(run())


class TestBatonRefusal:
    def test_mutating_action_refused_while_user_drives(self):
        daemon = BE.BrowserDaemon()
        daemon.hub.mint_generation("user")
        daemon.hub.state_extra["url"] = "https://example.com/form"
        response = asyncio.run(daemon.handle({"action": "click", "args": {"ref": 1}}))
        assert response["error"] == "user_is_driving"
        assert response["url"] == "https://example.com/form"
        assert "release control" in response["message"]

    def test_refusal_never_touches_the_browser(self):
        daemon = BE.BrowserDaemon()
        daemon.hub.mint_generation("user")

        async def boom():
            raise AssertionError("session must not be touched for a refusal")

        daemon._get_session = boom
        response = asyncio.run(
            daemon.handle(
                {
                    "action": "navigate",
                    "args": {"url": "https://x.dev"},
                }
            )
        )
        assert response["error"] == "user_is_driving"

    def test_unknown_action_beats_baton_check(self):
        daemon = BE.BrowserDaemon()
        daemon.hub.mint_generation("user")
        response = asyncio.run(daemon.handle({"action": "bogus"}))
        assert "unknown action" in response["error"]

    def test_take_baton_waits_for_inflight_agent_action(self):
        async def run():
            daemon = BE.BrowserDaemon()
            daemon.hub.mint_generation("agent")
            action_started = asyncio.Event()
            allow_action_to_finish = asyncio.Event()

            class Session:
                async def navigate_to(self, url):
                    action_started.set()
                    await allow_action_to_finish.wait()

            async def get_session():
                return Session()

            async def page_state(session, args):
                return {"url": args["url"]}

            daemon._get_session = get_session
            daemon._page_state = page_state
            action = asyncio.create_task(
                daemon.handle(
                    {
                        "action": "navigate",
                        "args": {"url": "https://example.com/"},
                    }
                )
            )
            await action_started.wait()

            takeover = asyncio.create_task(daemon.take_user_baton())
            await asyncio.sleep(0)
            assert daemon.hub.baton == "agent"
            assert not takeover.done()

            allow_action_to_finish.set()
            await action
            await takeover
            assert daemon.hub.baton == "user"

        asyncio.run(run())

    def test_agent_action_rechecks_baton_after_waiting_for_lock(self):
        async def run():
            daemon = BE.BrowserDaemon()
            daemon.hub.mint_generation("agent")
            session_touched = False

            async def get_session():
                nonlocal session_touched
                session_touched = True
                raise AssertionError("stale agent action must not touch the browser")

            daemon._get_session = get_session
            await daemon._lock.acquire()
            action = asyncio.create_task(
                daemon.handle(
                    {
                        "action": "navigate",
                        "args": {"url": "https://example.com/"},
                    }
                )
            )
            await asyncio.sleep(0)
            daemon.hub.take_baton()
            daemon._lock.release()

            response = await action
            assert response["error"] == "user_is_driving"
            assert session_touched is False

        asyncio.run(run())


class TestLaunchHygiene:
    def test_parse_chromium_version(self):
        assert (
            BE._parse_chromium_version("Chromium 138.0.7204.15 \n") == "138.0.7204.15"
        )
        assert BE._parse_chromium_version("") is None
        assert BE._parse_chromium_version("garbage") is None

    def test_clean_user_agent_has_no_headless_marker(self, monkeypatch):
        monkeypatch.setattr(BE, "_UA_CACHE", [])
        monkeypatch.setattr(
            BE.subprocess,
            "run",
            lambda *a, **k: SimpleNamespace(stdout="Chromium 138.0.7204.15"),
        )
        ua = BE._clean_user_agent()
        assert "Headless" not in ua
        assert "Chrome/138.0.7204.15" in ua
        # Memoized: a second call must not re-probe the binary.
        monkeypatch.setattr(
            BE.subprocess,
            "run",
            lambda *a, **k: (_ for _ in ()).throw(AssertionError("re-probed")),
        )
        assert BE._clean_user_agent() == ua

    def test_clean_user_agent_none_when_probe_fails(self, monkeypatch):
        monkeypatch.setattr(BE, "_UA_CACHE", [])

        def boom(*a, **k):
            raise OSError("no binary")

        monkeypatch.setattr(BE.subprocess, "run", boom)
        assert BE._clean_user_agent() is None


class TestShutdownProtocol:
    def test_request_connects_unix_socket_once(self, monkeypatch):
        class FakeSocket:
            def __init__(self):
                self.connects = []
                self.sent = []
                self.reads = [b'{"ok":true}\n']

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def settimeout(self, timeout):
                assert timeout == 2.0

            def connect(self, path):
                self.connects.append(path)

            def sendall(self, data):
                self.sent.append(data)

            def shutdown(self, how):
                assert how == BE.socket.SHUT_WR

            def recv(self, _size):
                return self.reads.pop(0) if self.reads else b""

        sock = FakeSocket()
        monkeypatch.setattr(BE.socket, "socket", lambda *_args: sock)
        monkeypatch.setattr(BE, "SOCKET_PATH", "/tmp/test-browser-exec.sock")

        result = BE._request(b'{"action":"snapshot"}\n', 2.0)

        assert result == b'{"ok":true}\n'
        assert sock.connects == ["/tmp/test-browser-exec.sock"]

    def test_shutdown_when_absent_never_spawns(self, monkeypatch, capsys):
        monkeypatch.setattr(BE, "_ping", lambda _path: False)
        monkeypatch.setattr(BE, "_browser_resident_pids", lambda: {})
        monkeypatch.setattr(
            BE,
            "_spawn_daemon",
            lambda: (_ for _ in ()).throw(AssertionError("shutdown must not spawn")),
        )

        rc = BE.run_client("shutdown", {}, 1.0)

        assert rc == 0
        assert json.loads(capsys.readouterr().out) == {
            "ok": True,
            "shutdown_complete": True,
            "already_stopped": True,
            "forced": False,
        }

    def test_shutdown_discards_daemon_early_reply_for_physical_ack(
        self, monkeypatch, capsys
    ):
        ack = {
            "ok": True,
            "shutdown_complete": True,
            "already_stopped": False,
            "forced": False,
        }
        monkeypatch.setattr(BE, "_ping", lambda _path: True)
        monkeypatch.setattr(BE, "_request", lambda *_args: b'{"ok":true,"pid":7}\n')
        monkeypatch.setattr(BE, "_complete_shutdown", lambda: dict(ack))

        rc = BE.run_client("shutdown", {}, 1.0)

        assert rc == 0
        assert json.loads(capsys.readouterr().out) == ack

    def test_shutdown_ack_follows_exact_resident_absence(self, monkeypatch):
        waits = iter([{123: "chromium-profile"}, {}])
        signals = []
        monkeypatch.setattr(
            BE, "_browser_resident_pids", lambda: {123: "chromium-profile"}
        )
        monkeypatch.setattr(
            BE, "_wait_for_browser_shutdown", lambda _timeout: next(waits)
        )
        monkeypatch.setattr(
            BE,
            "_signal_browser_residents",
            lambda residents, sig: signals.append((dict(residents), sig)),
        )
        monkeypatch.setattr(BE, "_ping", lambda _path: False)
        monkeypatch.setattr(BE.os, "unlink", lambda _path: None)

        result = BE._complete_shutdown()

        assert result == {
            "ok": True,
            "shutdown_complete": True,
            "already_stopped": False,
            "forced": True,
        }
        assert len(signals) == 1
        assert signals[0][0] == {123: "chromium-profile"}

    def test_session_stop_failure_is_not_silently_acknowledged(self):
        async def run():
            daemon = BE.BrowserDaemon()

            class BrokenSession:
                async def stop(self):
                    raise RuntimeError("stuck chromium")

            daemon._session = BrokenSession()
            response = await daemon.handle({"action": "shutdown", "args": {}})

            assert response == {"error": "browser shutdown required forced cleanup"}
            assert daemon.should_exit is True
            assert daemon._session is None

        asyncio.run(run())


class TestColdOpenStartPage:
    def test_cold_stream_info_lands_on_start_page(self):
        async def run():
            daemon = BE.BrowserDaemon()
            navigated = []

            class Session:
                url = "about:blank"

                async def get_current_page_url(self):
                    return self.url

                async def navigate_to(self, url):
                    navigated.append(url)
                    self.url = url

            session = Session()

            async def get_session():
                return session

            daemon._get_session = get_session
            response = await daemon.handle(
                {"action": "stream_info", "args": {"initial_baton": "user"}}
            )
            assert response["generation"]
            assert daemon._start_page_task is not None
            await daemon._start_page_task
            assert navigated == [BE.STREAM_START_URL]

            # Warm stream_info: same generation, no second navigation.
            again = await daemon.handle({"action": "stream_info", "args": {}})
            assert again["generation"] == response["generation"]
            assert navigated == [BE.STREAM_START_URL]

        asyncio.run(run())

    def test_agent_driven_page_is_never_clobbered(self):
        async def run():
            daemon = BE.BrowserDaemon()
            navigated = []

            class Session:
                async def get_current_page_url(self):
                    return "https://example.test/agent-was-here"

                async def navigate_to(self, url):
                    navigated.append(url)

            async def get_session():
                return Session()

            daemon._get_session = get_session
            await daemon.handle({"action": "stream_info", "args": {}})
            await daemon._start_page_task
            assert navigated == []

        asyncio.run(run())


class TestStreamListener:
    @staticmethod
    async def _client(port):
        return await asyncio.open_connection("127.0.0.1", port)

    @staticmethod
    async def _send(writer, frame_type, obj):
        writer.write(BE.encode_stream_frame(frame_type, json.dumps(obj).encode()))
        await writer.drain()

    @staticmethod
    async def _recv(reader):
        frame_type, payload = await asyncio.wait_for(
            BE.read_stream_frame(reader), timeout=2
        )
        return frame_type, json.loads(payload.decode())

    def _daemon(self, monkeypatch, port):
        monkeypatch.setattr(BE, "STREAM_PORT", port)
        daemon = BE.BrowserDaemon()
        daemon.hub.mint_generation("user")

        async def no_op(*_args, **_kwargs):
            return None

        # Listener protocol tests stay host-only. The real adapter is exercised
        # by docker/check-browser-stream.py inside the workspace image.
        daemon.ensure_screencast = no_op
        daemon.stop_screencast = no_op
        daemon.dispatch_user_input = no_op
        return daemon

    def test_bad_token_gets_error_and_close(self, monkeypatch):
        async def run():
            daemon = self._daemon(monkeypatch, 38899)
            server = await BE.start_stream_server(daemon)
            try:
                reader, writer = await self._client(38899)
                await self._send(
                    writer,
                    BE.T_HELLO,
                    {"token": "wrong", "min_protocol": 1},
                )
                frame_type, error = await self._recv(reader)
                assert frame_type == BE.T_ERROR
                assert error["code"] == "unauthorized"
                assert await reader.read(1) == b""
                writer.close()
                await writer.wait_closed()
            finally:
                server.close()
                await server.wait_closed()

        asyncio.run(run())

    @pytest.mark.parametrize("limit", [None, True, 0, 17, "3"])
    def test_invalid_hello_viewer_limit_gets_error_and_close(self, monkeypatch, limit):
        async def run():
            daemon = self._daemon(monkeypatch, 38896)
            server = await BE.start_stream_server(daemon)
            try:
                reader, writer = await self._client(38896)
                await self._send(
                    writer,
                    BE.T_HELLO,
                    {
                        "token": daemon.hub.token,
                        "min_protocol": 1,
                        "max_viewers": limit,
                    },
                )
                frame_type, error = await self._recv(reader)
                assert frame_type == BE.T_ERROR
                assert error["code"] == "invalid_hello"
                assert await reader.read(1) == b""
                assert daemon.hub.viewers == {}
                writer.close()
                await writer.wait_closed()
            finally:
                server.close()
                await server.wait_closed()

        asyncio.run(run())

    def test_daemon_refuses_global_viewer_over_effective_limit(self, monkeypatch):
        async def run():
            daemon = self._daemon(monkeypatch, 38895)
            starts = 0

            async def count_start():
                nonlocal starts
                starts += 1

            daemon.ensure_screencast = count_start
            server = await BE.start_stream_server(daemon)
            first_writer = None
            second_writer = None
            try:
                first_reader, first_writer = await self._client(38895)
                await self._send(
                    first_writer,
                    BE.T_HELLO,
                    {
                        "token": daemon.hub.token,
                        "min_protocol": 1,
                        "max_viewers": 1,
                    },
                )
                frame_type, _ = await self._recv(first_reader)
                assert frame_type == BE.T_STATE

                second_reader, second_writer = await self._client(38895)
                await self._send(
                    second_writer,
                    BE.T_HELLO,
                    {
                        "token": daemon.hub.token,
                        "min_protocol": 1,
                        "max_viewers": 16,
                    },
                )
                frame_type, error = await self._recv(second_reader)
                assert frame_type == BE.T_ERROR
                assert error["code"] == "viewer_limit"
                assert await second_reader.read(1) == b""
                assert len(daemon.hub.viewers) == 1
                assert daemon.hub.viewer_limit == 1
                assert starts == 1
            finally:
                if second_writer is not None:
                    second_writer.close()
                    await second_writer.wait_closed()
                if first_writer is not None:
                    first_writer.close()
                    await first_writer.wait_closed()
                await asyncio.sleep(0.05)
                server.close()
                await server.wait_closed()

        asyncio.run(run())

    def test_good_token_gets_state_then_baton_flip_broadcast(self, monkeypatch):
        async def run():
            daemon = self._daemon(monkeypatch, 38898)
            server = await BE.start_stream_server(daemon)
            try:
                reader, writer = await self._client(38898)
                await self._send(
                    writer,
                    BE.T_HELLO,
                    {
                        "token": daemon.hub.token,
                        "min_protocol": 1,
                        "max_viewers": 3,
                    },
                )
                frame_type, state = await self._recv(reader)
                assert (frame_type, state["baton"]) == (BE.T_STATE, "user")
                assert state["generation"] == daemon.hub.generation
                await self._send(writer, BE.T_CONTROL, {"op": "release_baton"})
                frame_type, state = await self._recv(reader)
                assert (frame_type, state["baton"]) == (BE.T_STATE, "agent")
                response = await daemon.handle({"action": "bogus"})
                assert "unknown action" in response["error"]
                writer.close()
                await writer.wait_closed()
            finally:
                server.close()
                await server.wait_closed()

        asyncio.run(run())

    def test_disconnect_removes_viewer(self, monkeypatch):
        async def run():
            daemon = self._daemon(monkeypatch, 38897)
            server = await BE.start_stream_server(daemon)
            try:
                reader, writer = await self._client(38897)
                await self._send(
                    writer,
                    BE.T_HELLO,
                    {
                        "token": daemon.hub.token,
                        "min_protocol": 1,
                        "max_viewers": 3,
                    },
                )
                await self._recv(reader)
                assert len(daemon.hub.viewers) == 1
                writer.close()
                await writer.wait_closed()
                await asyncio.sleep(0.1)
                assert len(daemon.hub.viewers) == 0
            finally:
                server.close()
                await server.wait_closed()

        asyncio.run(run())


_HANG = object()


class _FakeRuntime:
    def __init__(self, outcome):
        self._outcome = outcome
        self.calls = []

    async def evaluate(self, params=None, session_id=None):
        self.calls.append((params, session_id))
        if self._outcome is _HANG:
            await asyncio.sleep(30)
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


class _FakeTitleSession:
    def __init__(self, outcome, *, cdp_error=None, hang_lookup=False):
        self.lookup_calls = []
        self._hang_lookup = hang_lookup
        self.runtime = _FakeRuntime(outcome)
        self.agent_focus_target_id = "T1"
        self._cdp_error = cdp_error
        cdp_client = SimpleNamespace(send=SimpleNamespace(Runtime=self.runtime))
        self._cdp = SimpleNamespace(cdp_client=cdp_client, session_id="S1")

    async def get_or_create_cdp_session(self, target_id=None, focus=True):
        self.lookup_calls.append({"target_id": target_id, "focus": focus})
        if self._hang_lookup:
            await asyncio.sleep(30)
        if self._cdp_error is not None:
            raise self._cdp_error
        return self._cdp


class TestDocumentTitle:
    def _run(self, session, fallback="example.com"):
        return asyncio.run(BE._document_title(session, fallback))

    def test_document_title_wins_over_cached_hostname(self):
        s = _FakeTitleSession({"result": {"type": "string", "value": "Example Domain"}})
        assert self._run(s) == "Example Domain"
        params, sid = s.runtime.calls[0]
        assert "document.title" in params["expression"]
        assert sid == "S1"

    def test_empty_document_title_falls_back(self):
        s = _FakeTitleSession({"result": {"type": "string", "value": "   "}})
        assert self._run(s) == "example.com"

    def test_evaluate_error_falls_back(self):
        s = _FakeTitleSession(RuntimeError("target closed"))
        assert self._run(s) == "example.com"

    def test_cdp_session_error_falls_back(self):
        s = _FakeTitleSession({}, cdp_error=RuntimeError("no session"))
        assert self._run(s) == "example.com"

    def test_exception_details_or_non_string_falls_back(self):
        s = _FakeTitleSession({"result": {"type": "undefined"}})
        assert self._run(s) == "example.com"

    def test_lookup_uses_focus_false(self):
        s = _FakeTitleSession({"result": {"value": "T"}})
        assert self._run(s) == "T"
        assert s.lookup_calls == [{"target_id": "T1", "focus": False}]

    def test_hanging_lookup_falls_back_within_timeout(self, monkeypatch):
        monkeypatch.setattr(BE, "DOCUMENT_TITLE_TIMEOUT", 0.05)
        s = _FakeTitleSession({}, hang_lookup=True)
        start = time.monotonic()
        assert self._run(s) == "example.com"
        assert time.monotonic() - start < 1.0

    def test_hanging_evaluate_falls_back_within_timeout(self, monkeypatch):
        monkeypatch.setattr(BE, "DOCUMENT_TITLE_TIMEOUT", 0.05)
        s = _FakeTitleSession(_HANG)
        start = time.monotonic()
        assert self._run(s) == "example.com"
        assert time.monotonic() - start < 1.0
