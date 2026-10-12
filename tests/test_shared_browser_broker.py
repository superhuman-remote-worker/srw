"""Broker helper tests: codec mirror, readiness, and SSH resolution."""

import asyncio
from contextlib import asynccontextmanager
import json
import logging
import threading
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient
from starlette.datastructures import Headers
from starlette.websockets import WebSocketDisconnect

from orchestrator.services import browser_stream_broker as broker
from orchestrator.services.canvas_ssh import CanvasSSHError, PinnedSSHCommandResult


_WORKSPACE_GENERATION = UUID("11111111-aaaa-4aaa-8aaa-111111111111")
_RUNTIME_INCARNATION = UUID("22222222-bbbb-4bbb-8bbb-222222222222")
_THREAD_ID = UUID("33333333-cccc-4ccc-8ccc-333333333333")
_VALID_ORIGIN = "http://localhost:4200"


@pytest.fixture(autouse=True)
def _clear_active_viewers():
    broker._ACTIVE_VIEWERS.clear()
    yield
    broker._ACTIVE_VIEWERS.clear()


def _bound_thread(*, generation: UUID = _WORKSPACE_GENERATION) -> dict:
    return {
        "id": "t1",
        "user_id": "u1",
        "metadata": {
            "_workspace_binding": {
                "generation": str(generation),
                "kind": "remote",
                "backing_id": "workspace-a",
                "ssh_host_key_fingerprint": "SHA256:test",
            },
            "workspace_container": {
                "status": "ready",
                "ssh_host": "workspace.test",
                "ssh_port": 30022,
                "_canvas_workspace_generation": str(generation),
            },
        },
    }


def _stateless_thread(*, status: str = "active") -> dict:
    thread = _bound_thread()
    thread["id"] = str(_THREAD_ID)
    thread["execution_lane"] = "stateless"
    thread["status"] = status
    thread["metadata"].update(
        {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "_workspace_binding": {
                "generation": str(_WORKSPACE_GENERATION),
                "kind": "remote",
                "backing_id": "k8s-pvc:srw:test",
                "ssh_host_key_fingerprint": "SHA256:test",
            },
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "workspace.test",
                "pod_port": 30022,
                "pod_name": "workspace-test",
                "namespace": "srw",
                "_canvas_workspace_generation": str(_WORKSPACE_GENERATION),
                "_runtime_incarnation": str(_RUNTIME_INCARNATION),
            },
        }
    )
    return thread


class TestCodecMirror:
    def test_roundtrip(self):
        async def run():
            wire = broker.encode_stream_frame(broker.T_STATE, b'{"a":1}')
            reader = asyncio.StreamReader()
            reader.feed_data(wire)
            reader.feed_eof()
            return await broker.read_stream_frame(reader)

        frame_type, payload = asyncio.run(run())
        assert (frame_type, payload) == (broker.T_STATE, b'{"a":1}')

    def test_length_covers_type_byte(self):
        wire = broker.encode_stream_frame(broker.T_HELLO, b"abc")
        assert wire[:4] == (4).to_bytes(4, "big")
        assert wire[4] == broker.T_HELLO


class TestWorkspaceResolution:
    def test_ready_container(self):
        thread = {
            "metadata": {
                "workspace_container": {
                    "status": "ready",
                    "ssh_host": "10.1.2.3",
                    "ssh_port": 2222,
                }
            }
        }
        assert broker.workspace_ready(thread) is True

    def test_not_ready(self):
        assert broker.workspace_ready({"metadata": {}}) is False


class TestExecStreamInfo:
    def test_vm_runtime_is_contained_before_ssh(self, monkeypatch):
        async def forbidden(**kwargs):
            raise AssertionError(kwargs)

        monkeypatch.setattr(broker.PINNED_SSH_TRANSPORT_POOL, "run_command", forbidden)
        thread = _bound_thread()
        thread["metadata"]["vm"] = {
            "status": "ready",
            "ssh_host": "192.0.2.8",
            "ssh_port": 22,
        }

        with pytest.raises(broker.BrowserStreamUnavailable) as error:
            asyncio.run(
                broker.exec_stream_info(
                    thread,
                    generation_resolver=lambda: asyncio.sleep(0, result=thread),
                )
            )

        assert error.value.status == 503
        assert "continuous runtime attestation" in error.value.detail

    def test_uses_pinned_pool_and_parses_last_stdout_line(self, monkeypatch):
        captured = {}

        async def run_command(**kwargs):
            captured.update(kwargs)
            return PinnedSSHCommandResult(
                exit_status=0,
                stdout=(
                    b'[browser-exec] noise\n{"generation": "g1", '
                    b'"token": "t", "port": 38801, "baton": "user"}\n'
                ),
                stderr=b"",
            )

        async def forbidden_exec(*args, **kwargs):
            raise AssertionError((args, kwargs))

        monkeypatch.setattr(
            broker.PINNED_SSH_TRANSPORT_POOL, "run_command", run_command
        )
        monkeypatch.setattr(
            broker, "build_agent_ssh_cmd", forbidden_exec, raising=False
        )
        monkeypatch.setattr(broker.asyncio, "create_subprocess_exec", forbidden_exec)
        monkeypatch.setattr(broker, "resolve_ssh_key_path", lambda: "/tmp/key")
        monkeypatch.setattr(broker, "orchestrator_can_reach", lambda host: True)
        thread = _bound_thread()

        async def current():
            return thread

        info = asyncio.run(
            broker.exec_stream_info(
                thread,
                initial_baton="user",
                generation_resolver=current,
            )
        )

        assert info["generation"] == "g1"
        assert captured["target"].generation == _WORKSPACE_GENERATION
        assert captured["key_path"] == "/tmp/key"
        assert captured["command"] == (
            'browser-exec stream_info --json \'{"initial_baton":"user"}\''
        )
        assert captured["generation_resolver"] is current
        assert captured["max_output_bytes"] == 64 * 1024

    def test_stateless_cold_start_injects_exact_workspace_runtime_tag(
        self, monkeypatch
    ):
        captured = {}

        async def run_command(**kwargs):
            captured.update(kwargs)
            return PinnedSSHCommandResult(
                exit_status=0,
                stdout=(
                    b'{"generation":"g1","token":"t","port":38801,"baton":"agent"}\n'
                ),
                stderr=b"",
            )

        monkeypatch.setattr(
            broker.PINNED_SSH_TRANSPORT_POOL, "run_command", run_command
        )
        monkeypatch.setattr(broker, "resolve_ssh_key_path", lambda: "/tmp/key")
        monkeypatch.setattr(broker, "orchestrator_can_reach", lambda host: True)
        thread = _stateless_thread()

        info = asyncio.run(
            broker.exec_stream_info(
                thread,
                generation_resolver=lambda: asyncio.sleep(0, result=thread),
            )
        )

        assert info["generation"] == "g1"
        assert captured["command"] == (
            "env SRW_WORKSPACE_PROCESS_TAG="
            f"v1:session:{_THREAD_ID}:{_RUNTIME_INCARNATION} "
            "browser-exec stream_info --json '{}'"
        )

    def test_stateless_cold_start_refuses_missing_runtime_tag_authority(
        self, monkeypatch
    ):
        async def forbidden(**kwargs):
            raise AssertionError(kwargs)

        monkeypatch.setattr(broker.PINNED_SSH_TRANSPORT_POOL, "run_command", forbidden)
        thread = _stateless_thread()
        thread["metadata"]["workspace_container"].pop("_runtime_incarnation")

        with pytest.raises(broker.BrowserStreamUnavailable) as error:
            asyncio.run(
                broker.exec_stream_info(
                    thread,
                    generation_resolver=lambda: asyncio.sleep(0, result=thread),
                )
            )

        assert error.value.status == 503
        assert "runtime authority" in error.value.detail

    def test_generation_change_maps_to_typed_unavailable(self, monkeypatch):
        async def run_command(**kwargs):
            await kwargs["generation_resolver"]()
            raise CanvasSSHError(
                409,
                "workspace_generation_changed",
                "sentinel private endpoint",
            )

        monkeypatch.setattr(
            broker.PINNED_SSH_TRANSPORT_POOL, "run_command", run_command
        )
        monkeypatch.setattr(broker, "resolve_ssh_key_path", lambda: "/tmp/key")
        monkeypatch.setattr(broker, "orchestrator_can_reach", lambda host: True)

        with pytest.raises(broker.BrowserStreamUnavailable) as error:
            asyncio.run(
                broker.exec_stream_info(
                    _bound_thread(),
                    generation_resolver=lambda: asyncio.sleep(
                        0, result=_bound_thread(generation=UUID(int=2))
                    ),
                )
            )

        assert error.value.status == 409
        assert "sentinel" not in error.value.detail

    def test_nonzero_result_redacts_remote_output(self, monkeypatch, caplog):
        async def run_command(**kwargs):
            del kwargs
            return PinnedSSHCommandResult(
                exit_status=7,
                stdout=b"stdout-secret-sentinel",
                stderr=b"stderr-secret-sentinel",
            )

        monkeypatch.setattr(
            broker.PINNED_SSH_TRANSPORT_POOL, "run_command", run_command
        )
        monkeypatch.setattr(broker, "resolve_ssh_key_path", lambda: "/tmp/key")
        monkeypatch.setattr(broker, "orchestrator_can_reach", lambda host: True)
        caplog.set_level(logging.DEBUG)

        with pytest.raises(broker.BrowserStreamUnavailable) as error:
            asyncio.run(
                broker.exec_stream_info(
                    _bound_thread(),
                    generation_resolver=lambda: asyncio.sleep(
                        0, result=_bound_thread()
                    ),
                )
            )

        captured = error.value.detail + caplog.text
        assert "stdout-secret-sentinel" not in captured
        assert "stderr-secret-sentinel" not in captured


class TestStatelessColdStartLifecycle:
    def test_cold_start_is_serialized_and_rechecked(self, monkeypatch):
        thread = _stateless_thread()
        events = []

        class DB:
            locked = False

            @asynccontextmanager
            async def stateless_session_workspace_ensure_lock(self, thread_id, *, wait):
                assert (thread_id, wait) == ("t1", True)
                self.locked = True
                events.append("lock-enter")
                try:
                    yield True
                finally:
                    events.append("lock-exit")
                    self.locked = False

            async def get_thread(self, thread_id):
                assert thread_id == "t1"
                assert self.locked is True
                events.append("read")
                return dict(thread)

        db = DB()

        async def fake_exec(current, **kwargs):
            assert current == thread
            assert db.locked is True
            assert callable(kwargs["generation_resolver"])
            events.append("spawn")
            return {"generation": "g1"}

        monkeypatch.setattr(broker, "exec_stream_info", fake_exec)

        result = asyncio.run(
            broker._exec_stream_info_with_lifecycle(
                thread,
                thread_id="t1",
                db=db,
                generation_resolver=lambda: asyncio.sleep(0, result=thread),
            )
        )

        assert result == {"generation": "g1"}
        assert events == ["lock-enter", "read", "spawn", "read", "lock-exit"]

    @pytest.mark.parametrize(
        "mutation",
        [
            lambda thread: {**thread, "status": "ended"},
            lambda thread: {**thread, "status": "suspended"},
            lambda thread: {
                **thread,
                "metadata": {
                    **thread["metadata"],
                    "_stateless_workspace_retirement_pending": True,
                },
            },
            lambda thread: {
                **thread,
                "metadata": {
                    **thread["metadata"],
                    "_stateless_claim_losses": {
                        "7": {"pod": "agent-a", "quiesced": False}
                    },
                },
            },
        ],
        ids=["ended", "suspended", "retirement-pending", "claim-loss"],
    )
    def test_fresh_terminal_or_loss_state_blocks_spawn(self, monkeypatch, mutation):
        thread = _stateless_thread()

        class DB:
            @asynccontextmanager
            async def stateless_session_workspace_ensure_lock(self, *_args, **_kwargs):
                yield True

            async def get_thread(self, _thread_id):
                return mutation(thread)

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("terminal stateless thread spawned browser-exec")

        monkeypatch.setattr(broker, "exec_stream_info", forbidden)

        with pytest.raises(broker.BrowserStreamUnavailable) as error:
            asyncio.run(
                broker._exec_stream_info_with_lifecycle(
                    thread,
                    thread_id="t1",
                    db=DB(),
                    generation_resolver=lambda: asyncio.sleep(0, result=thread),
                )
            )

        assert error.value.status == 409

    @pytest.mark.parametrize(
        "marker",
        [
            "_stateless_workspace_retirement_pending",
            "_stateless_claim_retirement",
            "_stateless_claim_loss_hold",
            "_stateless_claim_losses",
        ],
        ids=["workspace-retirement", "claim-retirement", "loss-hold", "losses"],
    )
    @pytest.mark.parametrize(
        "marker_value",
        [None, False, 0, "", [], {}],
        ids=["null", "false", "zero", "empty-string", "empty-list", "empty-map"],
    )
    def test_present_falsey_lifecycle_marker_blocks_spawn(
        self, monkeypatch, marker, marker_value
    ):
        thread = _stateless_thread()
        thread["metadata"] = {
            **thread["metadata"],
            marker: marker_value,
        }

        class DB:
            @asynccontextmanager
            async def stateless_session_workspace_ensure_lock(self, *_args, **_kwargs):
                yield True

            async def get_thread(self, _thread_id):
                return thread

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("malformed lifecycle marker spawned browser-exec")

        monkeypatch.setattr(broker, "exec_stream_info", forbidden)

        with pytest.raises(broker.BrowserStreamUnavailable) as error:
            asyncio.run(
                broker._exec_stream_info_with_lifecycle(
                    thread,
                    thread_id="t1",
                    db=DB(),
                    generation_resolver=lambda: asyncio.sleep(0, result=thread),
                )
            )

        assert error.value.status == 409

    @pytest.mark.parametrize(
        "protected_cloud",
        [None, True, 0, "", [], {}],
        ids=["null", "true", "zero", "empty-string", "empty-list", "empty-map"],
    )
    def test_present_nonfalse_protected_cloud_blocks_spawn(
        self, monkeypatch, protected_cloud
    ):
        thread = _stateless_thread()
        thread["metadata"] = {
            **thread["metadata"],
            "protected_cloud": protected_cloud,
        }

        class DB:
            @asynccontextmanager
            async def stateless_session_workspace_ensure_lock(self, *_args, **_kwargs):
                yield True

            async def get_thread(self, _thread_id):
                return thread

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("protected stateless thread spawned browser-exec")

        monkeypatch.setattr(broker, "exec_stream_info", forbidden)

        with pytest.raises(broker.BrowserStreamUnavailable) as error:
            asyncio.run(
                broker._exec_stream_info_with_lifecycle(
                    thread,
                    thread_id="t1",
                    db=DB(),
                    generation_resolver=lambda: asyncio.sleep(0, result=thread),
                )
            )

        assert error.value.status == 409

    @pytest.mark.parametrize("tier", ["vm", "unknown"], ids=["vm", "unknown"])
    def test_non_sandbox_physical_or_unknown_tier_blocks_spawn(self, monkeypatch, tier):
        thread = _stateless_thread()
        if tier == "vm":
            thread["metadata"]["vm"] = {
                "status": "ready",
                "host": "vm.test",
            }
        else:
            thread["metadata"]["config_override"]["workspace"]["backend"] = (
                "future-tier"
            )

        class DB:
            @asynccontextmanager
            async def stateless_session_workspace_ensure_lock(self, *_args, **_kwargs):
                yield True

            async def get_thread(self, _thread_id):
                return thread

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("non-sandbox stateless tier spawned browser-exec")

        monkeypatch.setattr(broker, "exec_stream_info", forbidden)

        with pytest.raises(broker.BrowserStreamUnavailable) as error:
            asyncio.run(
                broker._exec_stream_info_with_lifecycle(
                    thread,
                    thread_id=str(_THREAD_ID),
                    db=DB(),
                    generation_resolver=lambda: asyncio.sleep(0, result=thread),
                )
            )

        assert error.value.status == 409

    def test_lifecycle_change_during_startup_is_rejected_under_lock(self, monkeypatch):
        thread = _stateless_thread()
        reads = 0

        class DB:
            @asynccontextmanager
            async def stateless_session_workspace_ensure_lock(self, *_args, **_kwargs):
                yield True

            async def get_thread(self, _thread_id):
                nonlocal reads
                reads += 1
                if reads == 1:
                    return dict(thread)
                changed = dict(thread)
                changed["metadata"] = {
                    **thread["metadata"],
                    "_stateless_workspace_retirement_pending": True,
                }
                return changed

        async def fake_exec(*_args, **_kwargs):
            return {"generation": "g1"}

        monkeypatch.setattr(broker, "exec_stream_info", fake_exec)

        with pytest.raises(broker.BrowserStreamUnavailable) as error:
            asyncio.run(
                broker._exec_stream_info_with_lifecycle(
                    thread,
                    thread_id="t1",
                    db=DB(),
                    generation_resolver=lambda: asyncio.sleep(0, result=thread),
                )
            )

        assert error.value.status == 409
        assert reads == 2

    def test_pinned_start_does_not_require_lifecycle_lock(self, monkeypatch):
        thread = _bound_thread()

        async def fake_exec(current, **_kwargs):
            assert current is thread
            return {"generation": "g1"}

        monkeypatch.setattr(broker, "exec_stream_info", fake_exec)

        result = asyncio.run(
            broker._exec_stream_info_with_lifecycle(
                thread,
                thread_id="t1",
                db=object(),
                generation_resolver=lambda: asyncio.sleep(0, result=thread),
            )
        )

        assert result == {"generation": "g1"}


def _ws_app(db):
    """A minimal app whose stream route relays with this application's store.

    The production route (``routers/ide.py``) passes its per-request
    dependencies' ``store``; the harness publishes ``db`` the same way, on its
    own ``app.state``, and the route reads it per connection.
    """

    app = FastAPI()
    app.state.store = db

    @app.websocket("/stream/{thread_id}")
    async def stream(ws: WebSocket, thread_id: str):
        await broker.relay_browser_stream(ws, thread_id, db=ws.app.state.store)

    return app


def _connect_and_capture_close(client, url, *, headers=None) -> int:
    if headers is None:
        headers = {"origin": _VALID_ORIGIN}
    try:
        with client.websocket_connect(url, headers=headers) as ws:
            ws.receive_bytes()
        return 1000
    except WebSocketDisconnect as exc:
        return exc.code


def _patch_ready_transport(monkeypatch) -> None:
    monkeypatch.setattr(broker, "resolve_ssh_key_path", lambda: "/tmp/key")
    monkeypatch.setattr(broker, "orchestrator_can_reach", lambda host: True)


class _RelayDB:
    def __init__(self, thread: dict | None = None):
        self.thread = thread or _bound_thread()
        self.get_calls = 0
        self.activity_calls = 0

    async def get_thread(self, thread_id):
        assert thread_id == "t1"
        self.get_calls += 1
        return dict(self.thread) if self.thread is not None else None

    async def merge_thread_workspace_context(self, thread_id, updates):
        assert thread_id == "t1"
        assert updates == {}
        self.activity_calls += 1
        return True


class _HangingSSHReader:
    async def readexactly(self, size):
        del size
        await asyncio.sleep(3600)


class _RecordingSSHWriter:
    def __init__(self):
        self.frames: list[bytes] = []

    def write(self, data):
        self.frames.append(bytes(data))

    async def drain(self):
        pass


class _FakeWebSocket:
    def __init__(self, receive_message=None):
        self.headers = Headers({"origin": _VALID_ORIGIN})
        self.receive_message = receive_message
        self.accepted = False
        self.closes: list[tuple[int, str | None]] = []

    async def accept(self):
        self.accepted = True

    async def close(self, *, code=1000, reason=None):
        self.closes.append((code, reason))

    async def receive(self):
        return self.receive_message

    async def send_bytes(self, data):
        del data


def _install_hanging_relay(monkeypatch, *, db: _RelayDB | None = None):
    monkeypatch.setenv("CANVAS_SHARED_BROWSER_ENABLED", "true")
    _patch_ready_transport(monkeypatch)
    db = db or _RelayDB()
    writer = _RecordingSSHWriter()

    async def fake_user(ws, database):
        assert database is db
        return {"id": "u1", "is_approved": True}

    async def fake_info(thread, **kwargs):
        del thread, kwargs
        return {"generation": "g1", "token": "tok", "port": 38801}

    async def fake_record(database, thread_id):
        assert database is db
        assert thread_id == "t1"
        return SimpleNamespace(source=SimpleNamespace(browser_generation="g1"))

    @asynccontextmanager
    async def fake_open(**kwargs):
        del kwargs
        yield _HangingSSHReader(), writer

    monkeypatch.setattr(broker, "resolve_ws_user", fake_user)
    monkeypatch.setattr(broker, "exec_stream_info", fake_info)
    monkeypatch.setattr(broker, "_get_canvas_record", fake_record)
    monkeypatch.setattr(broker, "_open_loopback", fake_open)
    app = _ws_app(db)
    return app, db, writer


class TestRelayAuthGates:
    def test_vm_runtime_is_rejected_before_browser_start(self, monkeypatch):
        thread = _bound_thread()
        thread["metadata"]["vm"] = {
            "status": "ready",
            "ssh_host": "192.0.2.8",
            "ssh_port": 22,
        }
        app, _, _ = _install_hanging_relay(
            monkeypatch,
            db=_RelayDB(thread),
        )

        async def forbidden(*args, **kwargs):
            raise AssertionError((args, kwargs))

        monkeypatch.setattr(broker, "exec_stream_info", forbidden)

        assert _connect_and_capture_close(TestClient(app), "/stream/t1") == 4503

    def test_missing_origin_closes_4403_before_feature_gate(self, monkeypatch):
        monkeypatch.delenv("CANVAS_SHARED_BROWSER_ENABLED", raising=False)
        app = _ws_app(object())

        assert (
            _connect_and_capture_close(TestClient(app), "/stream/t1", headers={})
            == 4403
        )

    @pytest.mark.parametrize(
        "origin",
        [
            "https://cross-site.example.test",
            "null",
            "https://example.test/path",
        ],
    )
    def test_bad_origin_closes_4403_before_feature_gate(self, monkeypatch, origin):
        monkeypatch.delenv("CANVAS_SHARED_BROWSER_ENABLED", raising=False)
        app = _ws_app(object())

        assert (
            _connect_and_capture_close(
                TestClient(app),
                "/stream/t1",
                headers={"origin": origin},
            )
            == 4403
        )

    def test_duplicate_origin_closes_4403_before_feature_gate(self, monkeypatch):
        monkeypatch.delenv("CANVAS_SHARED_BROWSER_ENABLED", raising=False)
        headers = httpx.Headers([("origin", _VALID_ORIGIN), ("origin", _VALID_ORIGIN)])
        app = _ws_app(object())

        assert (
            _connect_and_capture_close(
                TestClient(app),
                "/stream/t1",
                headers=headers,
            )
            == 4403
        )

    def test_disabled_closes_4404(self, monkeypatch):
        monkeypatch.delenv("CANVAS_SHARED_BROWSER_ENABLED", raising=False)
        app = _ws_app(object())

        assert _connect_and_capture_close(TestClient(app), "/stream/t1") == 4404

    def test_allowed_environment_origin_reaches_feature_gate(self, monkeypatch):
        monkeypatch.delenv("CANVAS_SHARED_BROWSER_ENABLED", raising=False)
        monkeypatch.setenv("CORS_ORIGINS", "https://cockpit.example.test")
        app = _ws_app(object())

        assert (
            _connect_and_capture_close(
                TestClient(app),
                "/stream/t1",
                headers={"origin": "HTTPS://COCKPIT.EXAMPLE.TEST:443"},
            )
            == 4404
        )

    def test_unauthenticated_closes_4401(self, monkeypatch):
        monkeypatch.setenv("CANVAS_SHARED_BROWSER_ENABLED", "true")

        async def no_user(ws, db):
            return None

        monkeypatch.setattr(broker, "resolve_ws_user", no_user)
        app = _ws_app(object())

        assert _connect_and_capture_close(TestClient(app), "/stream/t1") == 4401

    def test_stale_generation_closes_4409(self, monkeypatch):
        monkeypatch.setenv("CANVAS_SHARED_BROWSER_ENABLED", "true")
        _patch_ready_transport(monkeypatch)

        async def fake_user(ws, db):
            return {"id": "u1", "is_approved": True}

        async def fake_info(thread, **kwargs):
            return {"generation": "g-new", "token": "tok", "port": 38801}

        async def fake_record(db, thread_id):
            return SimpleNamespace(source=SimpleNamespace(browser_generation="g-old"))

        thread = _bound_thread()

        class FakeDB:
            async def get_thread(self, thread_id):
                return dict(thread)

        monkeypatch.setattr(broker, "resolve_ws_user", fake_user)
        monkeypatch.setattr(broker, "exec_stream_info", fake_info)
        monkeypatch.setattr(broker, "_get_canvas_record", fake_record)
        app = _ws_app(FakeDB())

        assert _connect_and_capture_close(TestClient(app), "/stream/t1") == 4409
        assert broker._ACTIVE_VIEWERS == {}


class TestRelayReadmission:
    def test_feature_is_rechecked_after_startup(self, monkeypatch):
        app, _, _ = _install_hanging_relay(monkeypatch)

        async def disable_during_start(thread, **kwargs):
            del thread, kwargs
            monkeypatch.setenv("CANVAS_SHARED_BROWSER_ENABLED", "false")
            return {"generation": "g1", "token": "tok", "port": 38801}

        monkeypatch.setattr(broker, "exec_stream_info", disable_during_start)

        assert _connect_and_capture_close(TestClient(app), "/stream/t1") == 4404
        assert broker._ACTIVE_VIEWERS == {}

    def test_approval_is_rechecked_after_startup(self, monkeypatch):
        app, db, _ = _install_hanging_relay(monkeypatch)
        calls = 0

        async def approval_changes(ws, database):
            nonlocal calls
            del ws
            assert database is db
            calls += 1
            return {"id": "u1", "is_approved": calls == 1}

        monkeypatch.setattr(broker, "resolve_ws_user", approval_changes)

        assert _connect_and_capture_close(TestClient(app), "/stream/t1") == 4403
        assert calls == 2
        assert broker._ACTIVE_VIEWERS == {}

    def test_owner_is_rechecked_after_startup(self, monkeypatch):
        db = _RelayDB()
        app, _, _ = _install_hanging_relay(monkeypatch, db=db)

        async def transfer_during_start(thread, **kwargs):
            del thread, kwargs
            db.thread = {**db.thread, "user_id": "u2"}
            return {"generation": "g1", "token": "tok", "port": 38801}

        monkeypatch.setattr(broker, "exec_stream_info", transfer_during_start)

        assert _connect_and_capture_close(TestClient(app), "/stream/t1") == 4403
        assert broker._ACTIVE_VIEWERS == {}

    def test_workspace_binding_is_rechecked_after_startup(self, monkeypatch):
        db = _RelayDB()
        app, _, _ = _install_hanging_relay(monkeypatch, db=db)

        async def revoke_binding_during_start(thread, **kwargs):
            del thread, kwargs
            changed = _bound_thread()
            changed["metadata"].pop("_workspace_binding")
            db.thread = changed
            return {"generation": "g1", "token": "tok", "port": 38801}

        monkeypatch.setattr(broker, "exec_stream_info", revoke_binding_during_start)

        assert _connect_and_capture_close(TestClient(app), "/stream/t1") == 4503
        assert broker._ACTIVE_VIEWERS == {}

    @pytest.mark.parametrize("transition", ["stop", "class", "tier"])
    def test_stateless_lifecycle_is_rechecked_after_startup(
        self, monkeypatch, transition
    ):
        db = _RelayDB(_stateless_thread())
        app, _, writer = _install_hanging_relay(monkeypatch, db=db)

        async def transition_after_startup(thread, **kwargs):
            del thread, kwargs
            changed = _stateless_thread()
            metadata = dict(changed["metadata"])
            if transition == "stop":
                metadata["_stateless_workspace_retirement_pending"] = True
            elif transition == "class":
                metadata["config_override"] = {
                    "workspace": {"backend": "sandbox"},
                    "officer": {"enabled": "yes"},
                }
            else:
                metadata["config_override"] = {"workspace": {"backend": "virtual"}}
            changed["metadata"] = metadata
            db.thread = changed
            return {"generation": "g1", "token": "tok", "port": 38801}

        monkeypatch.setattr(
            broker,
            "_exec_stream_info_with_lifecycle",
            transition_after_startup,
        )

        assert _connect_and_capture_close(TestClient(app), "/stream/t1") == 4409
        assert writer.frames == []
        assert broker._ACTIVE_VIEWERS == {}


class TestRelayClientProtocol:
    @pytest.mark.parametrize(
        ("kind", "payload"),
        [
            ("text", "not-binary"),
            ("bytes", b""),
            ("bytes", b"\x99unknown"),
            (
                "bytes",
                bytes([broker.T_INPUT]) + b"x" * broker.MAX_BROWSER_CLIENT_MESSAGE,
            ),
        ],
        ids=["text", "empty", "unknown-type", "oversized"],
    )
    def test_invalid_client_message_closes_4400(self, monkeypatch, kind, payload):
        app, _, _ = _install_hanging_relay(monkeypatch)

        with TestClient(app).websocket_connect(
            "/stream/t1",
            headers={"origin": _VALID_ORIGIN},
        ) as ws:
            if kind == "text":
                ws.send_text(payload)
            else:
                ws.send_bytes(payload)
            with pytest.raises(WebSocketDisconnect) as closed:
                ws.receive_bytes()

        assert closed.value.code == 4400
        assert broker._ACTIVE_VIEWERS == {}

    def test_ssh_open_failure_releases_reserved_viewer(self, monkeypatch):
        app, _, _ = _install_hanging_relay(monkeypatch)

        @asynccontextmanager
        async def broken_open(**kwargs):
            del kwargs
            raise RuntimeError("loopback unavailable")
            yield  # pragma: no cover

        monkeypatch.setattr(broker, "_open_loopback", broken_open)

        with TestClient(app).websocket_connect(
            "/stream/t1",
            headers={"origin": _VALID_ORIGIN},
        ) as ws:
            with pytest.raises(WebSocketDisconnect) as closed:
                ws.receive_bytes()

        assert closed.value.code == 4502
        assert broker._ACTIVE_VIEWERS == {}

    def test_malformed_asgi_receive_shape_closes_4400(self, monkeypatch):
        _, db, _ = _install_hanging_relay(monkeypatch)
        ws = _FakeWebSocket({"type": "websocket.receive"})

        asyncio.run(broker.relay_browser_stream(ws, "t1", db=db))

        assert ws.accepted is True
        assert ws.closes == [(4400, "Invalid browser protocol message")]
        assert broker._ACTIVE_VIEWERS == {}


class TestViewerAccounting:
    def test_startup_failure_before_accept_releases_viewer(self, monkeypatch):
        app, _, _ = _install_hanging_relay(monkeypatch)

        async def unavailable(thread, **kwargs):
            del thread, kwargs
            raise broker.BrowserStreamUnavailable(503, "private detail")

        monkeypatch.setattr(broker, "exec_stream_info", unavailable)

        assert _connect_and_capture_close(TestClient(app), "/stream/t1") == 4502
        assert broker._ACTIVE_VIEWERS == {}

    def test_startup_reservation_caps_handshakes_and_releases_on_cancel(
        self, monkeypatch
    ):
        _, db, _ = _install_hanging_relay(monkeypatch)
        monkeypatch.setenv("CANVAS_BROWSER_MAX_VIEWERS", "1")

        async def run():
            startup_entered = asyncio.Event()

            async def blocked_info(thread, **kwargs):
                del thread, kwargs
                startup_entered.set()
                await asyncio.sleep(3600)

            monkeypatch.setattr(broker, "exec_stream_info", blocked_info)
            first_ws = _FakeWebSocket()
            first = asyncio.create_task(
                broker.relay_browser_stream(first_ws, "t1", db=db)
            )
            await startup_entered.wait()
            assert broker._ACTIVE_VIEWERS == {"t1": 1}

            second_ws = _FakeWebSocket()
            await broker.relay_browser_stream(second_ws, "t1", db=db)
            # A pre-accept ASGI close becomes an HTTP denial in a real server,
            # which browsers surface as abnormal code 1006. Complete the
            # handshake first so Cockpit receives the contractual 4429.
            assert second_ws.accepted is True
            assert second_ws.closes == [(4429, "Viewer limit reached")]
            assert broker._ACTIVE_VIEWERS == {"t1": 1}

            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            assert broker._ACTIVE_VIEWERS == {}

        asyncio.run(run())


class TestRelayHappyPath:
    def test_relays_state_frame_and_sends_hello(self, monkeypatch):
        monkeypatch.setenv("CANVAS_SHARED_BROWSER_ENABLED", "true")
        _patch_ready_transport(monkeypatch)
        written = []
        activity_touched = threading.Event()
        client_frames_forwarded = threading.Event()

        class FakeSSHReader:
            def __init__(self):
                state = broker.encode_stream_frame(
                    broker.T_STATE,
                    b'{"baton":"user"}',
                )
                self._chunks = [state[:4], state[4:]]

            async def readexactly(self, size):
                if not self._chunks:
                    await asyncio.sleep(3600)
                chunk = self._chunks.pop(0)
                assert len(chunk) == size
                return chunk

        class FakeSSHWriter:
            def write(self, data):
                written.append(bytes(data))
                if len(written) >= 3:
                    client_frames_forwarded.set()

            async def drain(self):
                pass

        @asynccontextmanager
        async def fake_open(**kwargs):
            yield FakeSSHReader(), FakeSSHWriter()

        async def fake_user(ws, db):
            return {"id": "u1", "is_approved": True}

        async def fake_info(thread, **kwargs):
            return {"generation": "g1", "token": "tok", "port": 38801}

        async def fake_record(db, thread_id):
            return SimpleNamespace(source=SimpleNamespace(browser_generation="g1"))

        thread = _bound_thread()

        class FakeDB:
            async def get_thread(self, thread_id):
                return dict(thread)

            async def merge_thread_workspace_context(self, thread_id, updates):
                activity_touched.set()
                return True

        monkeypatch.setattr(broker, "resolve_ws_user", fake_user)
        monkeypatch.setattr(broker, "exec_stream_info", fake_info)
        monkeypatch.setattr(broker, "_get_canvas_record", fake_record)
        monkeypatch.setattr(broker, "_open_loopback", fake_open)
        app = _ws_app(FakeDB())

        with TestClient(app).websocket_connect(
            "/stream/t1",
            headers={"origin": _VALID_ORIGIN},
        ) as ws:
            first = ws.receive_bytes()
            assert first[0] == broker.T_STATE
            assert first[1:] == b'{"baton":"user"}'
            assert activity_touched.wait(timeout=1)
            ws.send_bytes(bytes([broker.T_INPUT]) + b"opaque-input")
            ws.send_bytes(bytes([broker.T_CONTROL]) + b'{"op":"take_baton"}')
            assert client_frames_forwarded.wait(timeout=1)

        assert written[0][4] == broker.T_HELLO
        assert b'"token": "tok"' in written[0] or b'"token":"tok"' in written[0]
        assert json.loads(written[0][5:]) == {
            "token": "tok",
            "min_protocol": 1,
            "max_viewers": 3,
        }
        assert any(
            frame[4] == broker.T_INPUT and frame[5:] == b"opaque-input"
            for frame in written[1:]
        )
        assert any(frame[4] == broker.T_CONTROL for frame in written[1:])
        assert broker._ACTIVE_VIEWERS == {}
