"""The VM hot swap's core runs behind an injected emitter (C2, spec §4.4).

The WebSocket verb's frames stay byte-identical; the durable path journals
them instead. One upgrade runs per agent process.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.api import persistent_app as pa
from agent.core.backends.virtual import VirtualWorkspaceBackend
from shared.runtime.core.backends.object_store import InMemoryObjectStore
from tests._fs_backend import FilesystemTestBackend


def _session(order=None, *, metadata=None):
    sess = MagicMock()
    sess.protected_cloud_required = False
    sess.shell_owner_token = None
    sess.config.extra = {"shell": {}}
    sess.workspace_manager.backend = SimpleNamespace(supports_shell=False)
    # The WebSocket swap reads the thread for a pending lane-switch marker.
    sess.postgres_conn.get_thread = AsyncMock(
        return_value={"metadata": metadata if metadata is not None else {}}
    )
    sess.cloud_mount_manager = None
    sess.cloud_mount_error = None
    sess._live_connector_lock = None
    sess._setup_cloud_mount = AsyncMock()
    sess.resetup_tools_for_backend = MagicMock()
    sess.swap_backend = MagicMock(
        side_effect=lambda backend, **_kw: (
            order.append("swap") if order is not None else None
        )
    )
    return sess


def _client():
    client = AsyncMock()
    client.request_thread_workspace_upgrade.return_value = (True, None)
    client.get_thread_workspace.return_value = {}
    return client


def _frames(ws):
    return [
        (call.args[0]["method"], call.args[0]["params"])
        for call in ws.send_json.call_args_list
    ]


def _patches(sess, client, *, vm_cfg, seed=None, remote_module=None, poll=None):
    return (
        patch("agent.api.persistent_app._session", sess),
        patch("agent.api.persistent_app._orchestrator_client", client),
        patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
        (
            patch("agent.api.persistent_app._poll_vm_ready", new=poll)
            if poll is not None
            else patch(
                "agent.api.persistent_app._poll_vm_ready",
                new_callable=AsyncMock,
                return_value=vm_cfg,
            )
        ),
        patch.dict(
            sys.modules,
            {"shared.runtime.core.backends.remote": remote_module or MagicMock()},
        ),
        patch(
            "agent.core.backends.seed.seed_workspace",
            side_effect=seed or (lambda src, dst, **_kw: 7),
        ),
    )


@pytest.mark.asyncio
async def test_the_websocket_frames_are_unchanged():
    ws, sess, client = AsyncMock(), _session(), _client()
    p = _patches(sess, client, vm_cfg={"ssh_host": "100.64.0.9", "ssh_port": 22})
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        await pa._handle_workspace_upgrade(ws, target_tier="vm")
    assert _frames(ws) == [
        ("workspace_upgrade.started", {"thread_id": "tid", "target_tier": "vm"}),
        (
            "workspace_upgrade.complete",
            {"thread_id": "tid", "target_tier": "vm", "seeded_files": 7},
        ),
    ]
    assert pa._workspace_upgrade_in_flight is False


@pytest.mark.asyncio
async def test_the_failure_frames_are_unchanged_when_the_vm_never_readies():
    ws, sess, client = AsyncMock(), _session(), _client()
    p = _patches(sess, client, vm_cfg=None)
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        await pa._handle_workspace_upgrade(ws, target_tier="vm")
    assert _frames(ws) == [
        ("workspace_upgrade.started", {"thread_id": "tid", "target_tier": "vm"}),
        (
            "workspace_upgrade.failed",
            {"reason": "vm workspace did not become ready in time"},
        ),
    ]
    client.abort_thread_vm_upgrade.assert_awaited_once_with("tid")


@pytest.mark.asyncio
async def test_the_core_emits_through_the_injected_emitter_and_records_the_seed_first():
    order: list = []
    emitted: list = []

    async def emit(method, params):
        emitted.append((method, params))

    async def before_swap(seeded):
        order.append(f"before_swap:{seeded}")

    def seed(src, dst, **_kw):
        order.append("seed")
        return 3

    sess, client = _session(order), _client()
    p = _patches(sess, client, vm_cfg={"ssh_host": "100.64.0.9"}, seed=seed)
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        await pa._run_workspace_upgrade_swap(
            emit,
            target_tier="vm",
            src_backend=SimpleNamespace(),
            before_swap=before_swap,
        )
    assert order == ["seed", "before_swap:3", "swap"]
    assert emitted[-1] == (
        "workspace_upgrade.complete",
        {"thread_id": "tid", "target_tier": "vm", "seeded_files": 3},
    )


@pytest.mark.asyncio
async def test_the_swap_hands_the_session_the_vm_tier_and_identity():
    """End picks its drain by the tier and proves this VM (E4 part 0)."""

    async def emit(_method, _params):
        return None

    remote_module = MagicMock()
    sess, client = _session(), _client()
    p = _patches(
        sess,
        client,
        vm_cfg={
            "ssh_host": "100.64.0.9",
            "ssh_port": 22,
            "workspace_generation": "generation-1",
            "workspace_runtime_incarnation": "launcher-pod-1",
            "workspace_ssh_host_key_fingerprint": "SHA256:abc",
        },
        remote_module=remote_module,
    )
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        await pa._run_workspace_upgrade_swap(
            emit, target_tier="vm", src_backend=SimpleNamespace()
        )
    sess.swap_backend.assert_called_once_with(
        remote_module.RemoteBackend.return_value,
        workspace_tier="vm",
        workspace_generation="generation-1",
        workspace_runtime_incarnation="launcher-pod-1",
    )


@pytest.mark.asyncio
async def test_a_failed_seed_record_tears_the_vm_down_and_never_swaps():
    emitted: list = []

    async def emit(method, params):
        emitted.append((method, params))

    async def before_swap(_seeded):
        raise RuntimeError("seeded route refused")

    sess, client = _session(), _client()
    p = _patches(sess, client, vm_cfg={"ssh_host": "100.64.0.9"})
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        await pa._run_workspace_upgrade_swap(
            emit,
            target_tier="vm",
            src_backend=SimpleNamespace(),
            before_swap=before_swap,
        )
    sess.swap_backend.assert_not_called()
    client.abort_thread_vm_upgrade.assert_awaited_once_with("tid")
    assert emitted == [("workspace_upgrade.failed", {"reason": "seeded route refused"})]


@pytest.mark.asyncio
async def test_a_second_websocket_upgrade_while_one_runs_is_refused():
    ws, sess, client = AsyncMock(), _session(), _client()
    with (
        patch("agent.api.persistent_app._session", sess),
        patch("agent.api.persistent_app._orchestrator_client", client),
        patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
        patch("agent.api.persistent_app._workspace_upgrade_in_flight", True),
    ):
        await pa._handle_workspace_upgrade(ws, target_tier="vm")
    assert _frames(ws) == [
        (
            "workspace_upgrade.failed",
            {
                "reason": "A workspace upgrade is already running.",
                "error_code": "upgrade_already_running",
            },
        )
    ]
    client.request_thread_workspace_upgrade.assert_not_called()


@pytest.mark.asyncio
async def test_the_flag_is_released_after_a_refused_request():
    ws, sess, client = AsyncMock(), _session(), _client()
    client.request_thread_workspace_upgrade.return_value = (False, "VMs are off")
    with (
        patch("agent.api.persistent_app._session", sess),
        patch("agent.api.persistent_app._orchestrator_client", client),
        patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
    ):
        await pa._handle_workspace_upgrade(ws, target_tier="vm")
    assert _frames(ws)[-1] == ("workspace_upgrade.failed", {"reason": "VMs are off"})
    assert pa._workspace_upgrade_in_flight is False


@pytest.mark.asyncio
async def test_a_websocket_upgrade_holds_the_single_flight_until_it_finishes(
    monkeypatch,
):
    monkeypatch.setattr(pa, "_workspace_upgrade_in_flight", False)
    polling, ready = asyncio.Event(), asyncio.Event()

    async def poll(*_args, **_kwargs):
        polling.set()
        await ready.wait()
        return {"ssh_host": "100.64.0.9", "ssh_port": 22}

    first_ws, second_ws = AsyncMock(), AsyncMock()
    sess, client = _session(), _client()
    p = _patches(sess, client, vm_cfg=None, poll=poll)
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        first = asyncio.create_task(
            pa._handle_workspace_upgrade(first_ws, target_tier="vm")
        )
        try:
            await asyncio.wait_for(polling.wait(), 5)
            # The first upgrade is waiting for its VM and holds the flight.
            assert pa._workspace_upgrade_in_flight is True

            await pa._handle_workspace_upgrade(second_ws, target_tier="vm")
            assert _frames(second_ws) == [
                (
                    "workspace_upgrade.failed",
                    {
                        "reason": "A workspace upgrade is already running.",
                        "error_code": "upgrade_already_running",
                    },
                )
            ]
            client.request_thread_workspace_upgrade.assert_awaited_once()

            ready.set()
            await asyncio.wait_for(first, 5)
        finally:
            if not first.done():
                first.cancel()
    assert _frames(first_ws) == [
        ("workspace_upgrade.started", {"thread_id": "tid", "target_tier": "vm"}),
        (
            "workspace_upgrade.complete",
            {"thread_id": "tid", "target_tier": "vm", "seeded_files": 7},
        ),
    ]
    assert pa._workspace_upgrade_in_flight is False


@pytest.mark.asyncio
async def test_cancelling_a_blocked_websocket_upgrade_releases_the_single_flight(
    monkeypatch,
):
    monkeypatch.setattr(pa, "_workspace_upgrade_in_flight", False)
    polling = asyncio.Event()

    async def poll(*_args, **_kwargs):
        polling.set()
        await asyncio.Event().wait()  # the VM never answers

    ws, sess, client = AsyncMock(), _session(), _client()
    p = _patches(sess, client, vm_cfg=None, poll=poll)
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        upgrade = asyncio.create_task(
            pa._handle_workspace_upgrade(ws, target_tier="vm")
        )
        await asyncio.wait_for(polling.wait(), 5)
        assert pa._workspace_upgrade_in_flight is True

        upgrade.cancel()
        with pytest.raises(asyncio.CancelledError):
            await upgrade
    assert pa._workspace_upgrade_in_flight is False


def _upgrade_with_real_seed(tmp_path, src):
    """A WebSocket VM upgrade whose seed really copies ``src`` (wrapped in the
    workspace's virtual overlay, like a live session) into ``tmp_path``."""
    from agent.core.backends.overlay import VirtualOverlayBackend

    dst = FilesystemTestBackend(tmp_path)
    remote_module = MagicMock()
    remote_module.RemoteBackend.return_value = dst
    ws, sess, client = AsyncMock(), _session(), _client()
    sess.workspace_manager.backend = VirtualOverlayBackend(src)
    p = _patches(
        sess,
        client,
        vm_cfg={"ssh_host": "100.64.0.9", "ssh_port": 22},
        remote_module=remote_module,
    )
    return ws, sess, client, dst, p


@pytest.mark.asyncio
async def test_the_hot_swap_reads_up_to_the_upload_ceiling_and_reports_what_it_skips(
    tmp_path, monkeypatch, caplog
):
    """Final review Important 3: the live virtual backend reads at most 50 MiB
    a file while uploads allow 100 MiB. The seed reads through a source built
    with the upload ceiling, so a file between the two is copied; a file over
    the upload ceiling is skipped (the old prefix keeps it) and named in the
    ``.complete`` frame. Both ceilings are shrunk here; the live one is the
    backend's own ``max_read_bytes``."""
    monkeypatch.setattr(
        "agent.core.backends.seed.VIRTUAL_UPLOAD_MAX_FILE_BYTES", 64, raising=True
    )
    src = VirtualWorkspaceBackend(
        InMemoryObjectStore(), prefix="threads/tid/", max_read_bytes=16
    )
    src.write_file("plan.md", "p")
    src.write_file("mid.bin", b"m" * 32)  # over the live ceiling, under uploads
    src.write_file("big.bin", b"x" * 100)  # over the upload ceiling
    with pytest.raises(ValueError):
        src.read_file("mid.bin", binary=True)  # the live backend refuses it

    ws, sess, client, dst, p = _upgrade_with_real_seed(tmp_path, src)
    # p[5] (the seed_workspace stub) is left out: the real copy runs.
    with (
        p[0],
        p[1],
        p[2],
        p[3],
        p[4],
        caplog.at_level(logging.WARNING, logger="agent.core.backends.seed"),
    ):
        await pa._handle_workspace_upgrade(ws, target_tier="vm")

    assert _frames(ws) == [
        ("workspace_upgrade.started", {"thread_id": "tid", "target_tier": "vm"}),
        (
            "workspace_upgrade.complete",
            {
                "thread_id": "tid",
                "target_tier": "vm",
                "seeded_files": 2,
                "skipped_files": ["big.bin"],
            },
        ),
    ]
    assert dst.read_file("plan.md") == "p"
    assert dst.read_file("mid.bin", binary=True) == b"m" * 32
    assert not dst.exists("big.bin")
    assert "'big.bin'" in caplog.text and "copy limit" in caplog.text
    sess.swap_backend.assert_called_once()
    assert sess.swap_backend.call_args.args == (dst,)
    client.abort_thread_vm_upgrade.assert_not_called()


@pytest.mark.asyncio
async def test_the_skipped_files_the_frame_names_are_capped(tmp_path, monkeypatch):
    monkeypatch.setattr("agent.core.backends.seed.VIRTUAL_UPLOAD_MAX_FILE_BYTES", 4)
    src = VirtualWorkspaceBackend(InMemoryObjectStore(), prefix="threads/tid/")
    names = [f"big-{n:02d}.bin" for n in range(25)]
    for name in names:
        src.write_file(name, b"x" * 8)

    ws, _sess, _client_, _dst, p = _upgrade_with_real_seed(tmp_path, src)
    with p[0], p[1], p[2], p[3], p[4]:
        await pa._handle_workspace_upgrade(ws, target_tier="vm")

    complete = _frames(ws)[-1]
    assert complete[0] == "workspace_upgrade.complete"
    assert complete[1]["seeded_files"] == 0
    assert complete[1]["skipped_files"] == names[:20]


# --- a WebSocket VM upgrade over a pending lane-switch marker (final review) --

LANE_SWITCH_REQUEST = "22222222-2222-4222-8222-222222222222"
PROVISION = "77777777-7777-4777-8777-777777777777"
OLD_PREFIX = {"backend": "virtual", "prefix": "threads/tid/", "binding": None}


def _marker_metadata(state):
    return {
        "vm": {
            "status": "ready",
            "provision_generation": PROVISION,
            "upgrade_seed": {
                "control_request_id": LANE_SWITCH_REQUEST,
                "state": state,
                "source": OLD_PREFIX,
            },
        }
    }


@pytest.mark.asyncio
async def test_a_websocket_upgrade_over_a_pending_marker_records_it_before_the_swap():
    """Final review Important 2 (data safety): the orchestrator answers a
    WebSocket retry "already in progress" and keeps a pending C2 marker. This
    swap seeds the same old prefix, so it records the marker ``seeded`` first;
    the next VM attach then copies nothing over newer VM files."""
    from agent.api import vm_upgrade_handoff as handoff

    order: list = []
    metadata = _marker_metadata("pending")

    async def mark_seeded(_thread_id, *, provision_generation, control_request_id):
        order.append("seeded")
        metadata["vm"]["upgrade_seed"]["state"] = "seeded"

    ws, sess, client = AsyncMock(), _session(order, metadata=metadata), _client()
    client.mark_workspace_upgrade_seeded = AsyncMock(side_effect=mark_seeded)

    def seed(src, dst, **_kw):
        order.append("seed")
        return 7

    p = _patches(sess, client, vm_cfg={"ssh_host": "100.64.0.9"}, seed=seed)
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        await pa._handle_workspace_upgrade(ws, target_tier="vm")

    assert order == ["seed", "seeded", "swap"]
    client.mark_workspace_upgrade_seeded.assert_awaited_once_with(
        "tid",
        provision_generation=PROVISION,
        control_request_id=LANE_SWITCH_REQUEST,
    )
    assert _frames(ws) == [
        ("workspace_upgrade.started", {"thread_id": "tid", "target_tier": "vm"}),
        (
            "workspace_upgrade.complete",
            {"thread_id": "tid", "target_tier": "vm", "seeded_files": 7},
        ),
    ]

    # A later VM attach finds nothing owed: no source read, no copy.
    attach = SimpleNamespace(
        postgres_conn=SimpleNamespace(
            get_thread=AsyncMock(return_value={"metadata": metadata})
        ),
        workspace_manager=SimpleNamespace(backend=SimpleNamespace(supports_shell=True)),
    )
    client.get_workspace_upgrade_seed_source = AsyncMock()
    assert (
        await handoff.recover_vm_upgrade_seed(
            session=attach, client=client, thread_id="tid"
        )
        is None
    )
    client.get_workspace_upgrade_seed_source.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata",
    [{}, _marker_metadata("seeded"), _marker_metadata("interrupted")],
    ids=["no-marker", "seeded", "interrupted"],
)
async def test_a_websocket_upgrade_without_a_pending_marker_records_nothing(metadata):
    ws, sess, client = AsyncMock(), _session(metadata=metadata), _client()
    client.mark_workspace_upgrade_seeded = AsyncMock()
    p = _patches(sess, client, vm_cfg={"ssh_host": "100.64.0.9", "ssh_port": 22})
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        await pa._handle_workspace_upgrade(ws, target_tier="vm")
    client.mark_workspace_upgrade_seeded.assert_not_awaited()
    assert _frames(ws) == [
        ("workspace_upgrade.started", {"thread_id": "tid", "target_tier": "vm"}),
        (
            "workspace_upgrade.complete",
            {"thread_id": "tid", "target_tier": "vm", "seeded_files": 7},
        ),
    ]


@pytest.mark.asyncio
async def test_a_websocket_upgrade_that_cannot_record_the_marker_never_swaps():
    from agent.api.orchestrator_client import WorkspaceUpgradeRouteRefused

    ws = AsyncMock()
    sess, client = _session(metadata=_marker_metadata("pending")), _client()
    client.mark_workspace_upgrade_seeded = AsyncMock(
        side_effect=WorkspaceUpgradeRouteRefused(409, "upgrade_seed_fence_lost", "lost")
    )
    p = _patches(sess, client, vm_cfg={"ssh_host": "100.64.0.9"})
    with p[0], p[1], p[2], p[3], p[4], p[5]:
        await pa._handle_workspace_upgrade(ws, target_tier="vm")
    sess.swap_backend.assert_not_called()
    client.abort_thread_vm_upgrade.assert_awaited_once_with("tid")
    assert _frames(ws)[-1] == ("workspace_upgrade.failed", {"reason": "lost"})
