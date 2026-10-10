"""The agent's side of the in-pod plane's cloud folders (connector drivers D7).

The agent never starts rclone for a sidecar Pod. It reads each mount's state
(the supervisor's status files and the workspace's mount table), waits
bounded at attach, links what mounted, keeps what did not with a plain
reason for the prompt and srw_cloud_status, asks for a flush with a nonce,
and never fails an attach for a folder that did not come up. An agent image
from before the plane, given the payload such a Pod's poll returns it, runs
its turns unrefused.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.api.persistent_session import PersistentSession
from agent.api.session_attach import cloud_mount_payload
from shared.runtime.core.loader import cloud_mount_system_floor
from shared.runtime.services.cloud_mount.sidecar import (
    SidecarMountError,
    SidecarMountWatcher,
    mounted_targets,
)

FINGERPRINT = "f" * 64


def _cfg(*names: str, excluded=()) -> dict:
    return {
        "version": 1,
        "delivery": "sidecar",
        "fingerprint": FINGERPRINT,
        "status_dir": "/srw/cloud-status",
        "control_dir": "/srw/cloud-control",
        "wait_seconds": 3,
        "drain_seconds": 5,
        "mounts": [
            {
                "index": i,
                "mount_id": f"row-{name}",
                "mount_kind": "project",
                "target_path": f"/cloud/{name}",
                "workspace_name": name,
                "access": "read_write",
            }
            for i, name in enumerate(names)
        ],
        "excluded": list(excluded),
    }


def _mountinfo(*live: str) -> str:
    lines = ["30 25 0:40 / /cloud ro,relatime master:9 - tmpfs tmpfs ro"]
    for n, name in enumerate(live):
        lines.append(
            f"{40 + n} 30 0:{50 + n} / /cloud/{name} rw,nosuid,nodev master:1{n} "
            "- fuse.rclone srw-cloud rw,user_id=65534"
        )
    return "\n".join(lines)


class _Workspace:
    """A workspace backend whose status files and mount table a test sets."""

    def __init__(self, names: list[str]) -> None:
        self.names = names
        self.status: dict[int, dict] = {}
        self.live: set[str] = set()
        self.commands: list[str] = []
        self.terminal: list[str] = []
        self.root = "/home/agent-host/workspace"
        self.drain_answer: dict | None = None

    def _answer(self, command: str) -> str:
        if "/srw/cloud-control/" in command:
            nonce = command.split("printf '%s' ", 1)[1].split(" ", 1)[0]
            kind = "drain" if "/drain" in command else "refresh"
            for index in self.status:
                answer = (
                    (self.drain_answer or {"state": "drained", "pending": 0})
                    if (kind == "drain")
                    else {"state": "done"}
                )
                self.status[index][kind] = {"nonce": nonce, **answer}
            return ""
        if "ln -sfn" in command:
            return "SRW_LINKS_OK\n"
        parts = []
        for index, _name in enumerate(self.names):
            parts.append(f"==/srw/cloud-status/{index}.json")
            if index in self.status:
                parts.append(json.dumps(self.status[index]))
            parts.append("")
        parts.append("==srw-mountinfo")
        parts.append(_mountinfo(*sorted(self.live)))
        return "\n".join(parts)

    def exec_claim_resource(self, command, timeout, operation):
        self.commands.append(command)
        return self._answer(command)

    def exec_terminal_claim_resource(self, command, timeout, operation):
        self.terminal.append(command)
        return self._answer(command)


def _runs_rclone(command: str) -> bool:
    """Whether a command starts, configures or unmounts rclone itself."""
    return any(
        word in command
        for word in ("rclone mount", "rclone config", "rclone rc", "fusermount")
    )


def _watcher(workspace: _Workspace, cfg: dict, **kw) -> SidecarMountWatcher:
    clock = [0.0]
    return SidecarMountWatcher(
        thread_id="t1",
        cloud_cfg=cfg,
        workspace_backend=workspace,
        workspace_root="/home/agent-host/workspace",
        clock=lambda: clock[0],
        sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        **kw,
    )


def test_the_mount_table_parser_reads_the_top_of_each_stack():
    tops = mounted_targets(
        _mountinfo("project") + "\n50 40 0:9 / /cloud/project rw - tmpfs x rw"
    )
    assert tops["/cloud/project"] == "tmpfs"
    assert mounted_targets(r"31 30 0:41 / /cloud/a\040b rw - fuse.rclone s rw") == {
        "/cloud/a b": "fuse.rclone"
    }


@pytest.mark.asyncio
async def test_mounted_folders_are_linked_and_reported():
    workspace = _Workspace(["project"])
    workspace.status[0] = {"state": "mounted"}
    workspace.live = {"project"}
    watcher = _watcher(workspace, _cfg("project"))
    await watcher.start_all()
    assert watcher.active and [m.workspace_name for m in watcher.mounts] == ["project"]
    assert watcher.report() == [{"name": "project", "state": "mounted"}]
    links = next(c for c in workspace.commands if "ln -sfn" in c)
    assert "/cloud/project" in links and "SRW_LINKS_OK" in links
    # Nothing here ever starts, stops or configures rclone.
    assert not any(_runs_rclone(c) for c in workspace.commands)


@pytest.mark.asyncio
async def test_a_folder_that_did_not_mount_never_fails_the_attach():
    workspace = _Workspace(["project", "home", "gone"])
    workspace.status[0] = {"state": "unavailable", "reason": "credential_rejected"}
    workspace.status[1] = {"state": "unavailable", "reason": "401 Unauthorized"}
    watcher = _watcher(
        workspace,
        _cfg("project", "home", "gone"),
    )
    await watcher.start_all()
    assert not watcher.active
    assert watcher.report() == [
        {"name": "project", "state": "unavailable", "reason": "credential_rejected"},
        {"name": "home", "state": "unavailable", "reason": "mount_failed"},
        {"name": "gone", "state": "unavailable", "reason": "sidecar_unavailable"},
    ]
    assert not any("ln -sfn" in c for c in workspace.commands)
    text = watcher.status()
    assert "refused its credential" in text and "401" not in text


@pytest.mark.asyncio
async def test_a_mount_still_coming_up_stays_pending_and_unlinked():
    workspace = _Workspace(["project", "slow"])
    workspace.status[0] = {"state": "mounted"}
    workspace.status[1] = {"state": "mounted"}  # but not yet in the mount table
    workspace.live = {"project"}
    watcher = _watcher(workspace, _cfg("project", "slow"))
    await watcher.start_all()
    assert watcher.report()[1] == {"name": "slow", "state": "pending"}
    links = next(c for c in workspace.commands if "ln -sfn" in c)
    assert "/cloud/project" in links and "/cloud/slow" not in links


def test_the_prompt_rows_include_what_was_left_out():
    workspace = _Workspace(["project"])
    workspace.status[0] = {"state": "unavailable", "reason": "not_found"}
    watcher = _watcher(
        workspace,
        _cfg(
            "project",
            excluded=[
                {"source_ref": "r", "mount_kind": "project", "reason": "set_fallback"}
            ],
        ),
    )
    watcher._start_sync()
    rows = watcher.unavailable()
    assert rows[0] == {
        "name": "project",
        "reason": "not_found",
        "text": "the folder no longer exists in the cloud",
    }
    assert rows[1]["name"] == "" and rows[1]["reason"] == "set_fallback"
    floor = cloud_mount_system_floor(rows)
    assert "workspace/cloud/project: the folder no longer exists" in floor
    assert "was not attached" in floor and floor.startswith("<cloud_folders>")
    assert cloud_mount_system_floor([]) == "" and cloud_mount_system_floor(None) == ""


def test_the_session_prompt_names_folders_that_did_not_mount():
    from shared.runtime.core.loader import AgentConfig, get_phase_system_prompt

    def prompt(extra: dict) -> str:
        config = AgentConfig(
            agent_id="a",
            display_name="A",
            extra={
                "_resolved_prompts": {
                    "systemprompt_interactive": "Hi {agent_display_name}"
                },
                **extra,
            },
        )
        return get_phase_system_prompt(
            config=config, is_strategic=False, prompt_type="interactive", model="gpt-4o"
        )

    rows = [
        {
            "name": "project",
            "reason": "timeout",
            "text": "the cloud did not answer in time",
        }
    ]
    assert "<cloud_folders>" in prompt({"_cloud_mounts_unavailable": rows})
    assert "workspace/cloud/project" in prompt({"_cloud_mounts_unavailable": rows})
    assert "<cloud_folders>" not in prompt({})


def test_a_drain_is_asked_with_a_nonce_and_answered():
    workspace = _Workspace(["project"])
    workspace.status[0] = {"state": "mounted"}
    workspace.live = {"project"}
    watcher = _watcher(workspace, _cfg("project"))
    assert watcher.request_drain() == (True, 0)
    request = next(c for c in workspace.commands if "/srw/cloud-control/drain" in c)
    assert "mv " in request
    workspace.drain_answer = {"state": "incomplete", "pending": 2}
    assert watcher.request_drain() == (False, 2)


@pytest.mark.asyncio
async def test_end_requires_the_flush_only_when_uploads_are_known_pending():
    workspace = _Workspace(["project"])
    workspace.status[0] = {"state": "mounted"}
    workspace.live = {"project"}
    watcher = _watcher(workspace, _cfg("project"), terminal=True)
    assert await watcher.retire_existing(drain=True) == {
        "rclone_mounts": 0,
        "rclone_processes": 0,
    }
    assert workspace.terminal and not workspace.commands
    workspace.drain_answer = {"state": "incomplete", "pending": 3}
    with pytest.raises(SidecarMountError, match="3 upload"):
        await watcher.retire_existing(drain=True)
    # Unknown (rclone did not answer) is reported, not a reason to wedge End.
    workspace.drain_answer = {"state": "incomplete", "pending": -1}
    await watcher.retire_existing(drain=True)


@pytest.mark.asyncio
async def test_closing_never_raises_and_a_handoff_leaves_nothing_behind():
    workspace = _Workspace(["project"])
    workspace.status[0] = {"state": "mounted"}

    def broken(*args, **kwargs):
        raise RuntimeError("ssh gone")

    workspace.exec_claim_resource = broken
    watcher = _watcher(workspace, _cfg("project"))
    await watcher.aclose(strict=True)
    await watcher.detach_for_handoff()


def test_a_lost_mount_is_waited_for_never_restarted():
    workspace = _Workspace(["lower"])
    workspace.status[0] = {"state": "pending"}
    watcher = _watcher(workspace, _cfg("lower"))
    with pytest.raises(SidecarMountError, match="did not come back"):
        watcher.restart_mount("row-lower")
    workspace.status[0] = {"state": "mounted"}
    workspace.live = {"lower"}
    watcher.restart_mount("row-lower")
    assert not any(_runs_rclone(c) for c in workspace.commands)


@pytest.mark.asyncio
async def test_a_pinned_session_hears_of_a_change():
    workspace = _Workspace(["project"])
    workspace.status[0] = {"state": "mounted"}
    workspace.live = {"project"}
    watcher = _watcher(workspace, _cfg("project"))
    await watcher.start_all()
    seen: list = []
    watcher.start_monitor(seen.append, interval=0.01)
    workspace.status[0] = {"state": "unavailable", "reason": "unreachable"}
    workspace.live = set()
    for _ in range(100):
        if seen:
            break
        await asyncio.sleep(0.01)
    await watcher.detach_for_handoff()
    assert seen[0] == [
        {"name": "project", "state": "unavailable", "reason": "unreachable"}
    ]


@pytest.mark.asyncio
async def test_the_session_attaches_to_sidecar_folders_and_keeps_why_for_the_prompt():
    workspace = _Workspace(["project"])
    workspace.status[0] = {"state": "unavailable", "reason": "timeout"}
    session = SimpleNamespace(
        thread_id="t1",
        workspace_manager=SimpleNamespace(
            backend=workspace, path="/home/agent-host/workspace"
        ),
        shell_owner_token=None,
        config=SimpleNamespace(extra={}),
        cloud_mount_manager=None,
        cloud_mount_error=None,
    )
    with patch(
        "shared.runtime.services.cloud_mount.sidecar.SidecarMountWatcher._start_sync",
        lambda self: (self.read_states(), setattr(self, "_settled", True)),
    ):
        await PersistentSession._setup_sidecar_cloud_mount(session, _cfg("project"))
    assert session.cloud_mount_manager.delivery == "sidecar"
    assert "did not answer in time" in session.cloud_mount_error


def test_the_workspace_payload_names_either_delivery():
    sidecar = _cfg("project")
    assert (
        cloud_mount_payload({"cloud_mount": None, "cloud_mount_sidecar": sidecar})
        is sidecar
    )
    assert cloud_mount_payload({"cloud_mount": {"driver": "rclone"}}) == {
        "driver": "rclone"
    }
    assert (
        cloud_mount_payload({"cloud_mount": None, "cloud_mount_sidecar": None}) is None
    )


@pytest.mark.asyncio
async def test_stateless_end_flushes_sidecar_folders_and_proves_no_workspace_rclone():
    from orchestrator.services import stateless_session_retirement as retirement
    from tests.test_managed_repository_agent_lifecycle import _terminal_thread

    workspace = _Workspace(["project"])
    workspace.status[0] = {"state": "mounted"}
    workspace.live = {"project"}
    workspace.verify_terminal_claim_resources_retired = MagicMock(return_value="")
    workspace.retire = MagicMock()
    with patch.object(retirement, "_build_terminal_backend", return_value=workspace):
        proof = await retirement.retire_stateless_workspace_residents(
            _terminal_thread(), terminal_token=9, cloud_mount_cfg=_cfg("project")
        )
        await retirement.verify_stateless_workspace_residents_retired(
            _terminal_thread(), terminal_token=9, cloud_mount_cfg=_cfg("project")
        )
    assert proof.rclone_mounts == 0 and proof.rclone_processes == 0
    assert any("/srw/cloud-control/drain" in c for c in workspace.terminal)
    assert not any(_runs_rclone(c) for c in workspace.terminal)
    # Only the general resident zero proof: no per-mount rclone re-proof.
    assert workspace.verify_terminal_claim_resources_retired.call_count == 1
    workspace.drain_answer = {"state": "incomplete", "pending": 4}
    with patch.object(retirement, "_build_terminal_backend", return_value=workspace):
        with pytest.raises(retirement.ShellRetirementUnavailable):
            await retirement.retire_stateless_workspace_residents(
                _terminal_thread(), terminal_token=9, cloud_mount_cfg=_cfg("project")
            )


@pytest.mark.asyncio
async def test_an_older_agents_turn_is_not_refused_on_a_sidecar_pod():
    """An agent image from before D7 reads only cloud_mount, cloud_sync,
    nc_session_folder and cloud_sync_degraded. The poll of a sidecar Pod
    gives it none of them (tests/test_cloud_mount_sidecar_delivery.py): at
    the stateless turn start it then records an authoritative "no mirror"
    and runs, instead of refusing tool work as it would for a degraded
    sync. This code path is unchanged by D7."""
    import agent.api.persistent_app as papp

    session = MagicMock()
    session.workspace_sync = None
    outdated_poll = {
        "cloud_mount": None,
        "cloud_mount_sidecar": _cfg("project"),
        "cloud_mount_agent_outdated": True,
        "cloud_sync": None,
        "nc_session_folder": None,
        "cloud_sync_degraded": False,
    }
    client = MagicMock()
    client.get_thread_workspace = AsyncMock(return_value=outdated_poll)
    build = MagicMock()
    with (
        patch.object(papp, "_session", session),
        patch.object(papp._session_identity, "_thread_id", "t1"),
        patch.object(papp, "_orchestrator_client", client),
        patch.object(papp, "_cloud_sync_retry_pending", True),
        patch.object(papp, "_stateless_mode", lambda: True),
        patch.object(papp, "_build_sync_coordinator", build),
        patch.object(papp, "_broadcast", lambda *a: None),
    ):
        await papp._retry_cloud_sync_start(1)
        assert papp._cloud_sync_retry_pending is False
    build.assert_not_called()
    assert session.workspace_sync is None
