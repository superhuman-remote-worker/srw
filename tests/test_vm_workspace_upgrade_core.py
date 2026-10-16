"""The VM hot swap's core runs behind an injected emitter (C2, spec §4.4).

The WebSocket verb's frames stay byte-identical; the durable path journals
them instead. One upgrade runs per agent process.
"""

from __future__ import annotations

import logging
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.api import persistent_app as pa
from agent.core.backends.virtual import DEFAULT_MAX_READ_BYTES, VirtualWorkspaceBackend
from shared.runtime.core.backends.object_store import InMemoryObjectStore
from tests._fs_backend import FilesystemTestBackend


def _session(order=None):
    sess = MagicMock()
    sess.protected_cloud_required = False
    sess.shell_owner_token = None
    sess.config.extra = {"shell": {}}
    sess.workspace_manager.backend = SimpleNamespace(supports_shell=False)
    sess.cloud_mount_manager = None
    sess.cloud_mount_error = None
    sess._live_connector_lock = None
    sess._setup_cloud_mount = AsyncMock()
    sess.resetup_tools_for_backend = MagicMock()
    sess.swap_backend = MagicMock(
        side_effect=lambda backend: order.append("swap") if order is not None else None
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


def _patches(sess, client, *, vm_cfg, seed=None, remote_module=None):
    return (
        patch("agent.api.persistent_app._session", sess),
        patch("agent.api.persistent_app._orchestrator_client", client),
        patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
        patch(
            "agent.api.persistent_app._poll_vm_ready",
            new_callable=AsyncMock,
            return_value=vm_cfg,
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
            {"reason": "A workspace upgrade is already running."},
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
async def test_a_virtual_file_over_the_read_ceiling_is_skipped_and_the_upgrade_completes(
    tmp_path, caplog
):
    # The live virtual backend refuses to read an object over its ceiling. The
    # hot swap skips that one file with a warning (the old prefix keeps it)
    # instead of failing every VM upgrade after the VM is Ready (C2).
    src = VirtualWorkspaceBackend(InMemoryObjectStore(), prefix="threads/tid/")
    src.write_file("plan.md", "p")
    src.write_file("big.bin", b"x" * (DEFAULT_MAX_READ_BYTES + 1))
    dst = FilesystemTestBackend(tmp_path)
    remote_module = MagicMock()
    remote_module.RemoteBackend.return_value = dst

    ws, sess, client = AsyncMock(), _session(), _client()
    sess.workspace_manager.backend = src
    p = _patches(
        sess,
        client,
        vm_cfg={"ssh_host": "100.64.0.9", "ssh_port": 22},
        remote_module=remote_module,
    )
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
            {"thread_id": "tid", "target_tier": "vm", "seeded_files": 1},
        ),
    ]
    assert dst.read_file("plan.md") == "p"
    assert not dst.exists("big.bin")
    assert "'big.bin'" in caplog.text and "copy limit" in caplog.text
    sess.swap_backend.assert_called_once_with(dst)
    client.abort_thread_vm_upgrade.assert_not_called()
