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
from dataclasses import dataclass
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
POD_UID = "99999999-9999-4999-8999-999999999999"


def _cfg(*names: str, excluded=()) -> dict:
    return {
        "version": 1,
        "delivery": "sidecar",
        "fingerprint": FINGERPRINT,
        "runtime_incarnation": POD_UID,
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
        if "__SRW_SIDECAR_RCLONE_ZERO__" in command:
            return "__SRW_SIDECAR_RCLONE_ZERO__\n"
        if "SRW_UNLINK_OK" in command:
            return "SRW_UNLINK_OK\n"
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
    # A single folder is workspace/cloud itself.
    assert "ln -sfn /cloud/project" in links and "SRW_LINKS_OK" in links
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
    # Linked all the same: a write into a folder that did not mount fails in
    # the read-only view instead of landing in a plain local directory.
    links = next(c for c in workspace.commands if "ln -sfn" in c)
    assert "ln -sfn /cloud " in links
    text = watcher.status()
    assert "refused its credential" in text and "401" not in text
    assert "workspace/cloud/project: unavailable" in text


@pytest.mark.asyncio
async def test_a_mount_still_coming_up_stays_pending_and_unlinked():
    workspace = _Workspace(["project", "slow"])
    workspace.status[0] = {"state": "mounted"}
    workspace.status[1] = {"state": "mounted"}  # but not yet in the mount table
    workspace.live = {"project"}
    watcher = _watcher(workspace, _cfg("project", "slow"))
    await watcher.start_all()
    assert watcher.report()[1] == {"name": "slow", "state": "pending"}
    # workspace/cloud is the sidecars' read-only root: the pending folder is
    # there already, empty and read-only until it mounts.
    links = next(c for c in workspace.commands if "ln -sfn" in c)
    assert "ln -sfn /cloud " in links and "mkdir" not in links.split("SRW")[0][-80:]
    assert "workspace/cloud/slow: still coming up" in watcher.status()


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
        "path": "workspace/cloud",
        "reason": "not_found",
        "text": "the folder no longer exists in the cloud",
    }
    assert rows[1]["name"] == "" and rows[1]["reason"] == "set_fallback"
    assert rows[1]["kind"] == "project"
    floor = cloud_mount_system_floor(rows)
    # One folder: it is workspace/cloud itself.
    assert "- workspace/cloud: the folder no longer exists" in floor
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
    # Unknown is not a count: no clamp to zero.
    workspace.drain_answer = {"state": "incomplete", "pending": -1}
    assert watcher.request_drain() == (False, -1)


@pytest.mark.asyncio
async def test_end_requires_every_folder_to_confirm_its_flush():
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
    # Unknown (rclone did not answer, a folder still coming up) is not
    # drained: End retries until the supervisor can say.
    workspace.drain_answer = {"state": "incomplete", "pending": -1}
    with pytest.raises(SidecarMountError, match="did not confirm"):
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
        sidecar_mount_watcher=None,
        protected_cloud_unavailable=None,
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
    # A session from before the deploy may keep in-workspace rclone
    # state in its home: End proves no rclone runs and removes its directory.
    cleanup = next(c for c in workspace.terminal if "__SRW_SIDECAR_RCLONE_ZERO__" in c)
    assert 'rm -rf -- "$_srw_rclone_base"' in cleanup and "exit 85" in cleanup
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


# --------------------------------------------------------------------------- #
# A protected sidecar Pod (Phase B): the lower from the sidecars, the overlay
# in the workspace
# --------------------------------------------------------------------------- #


def _protected_cfg() -> dict:
    """What the orchestrator gives a protected sidecar Pod's agent."""
    from orchestrator.services.cloud_mount_sidecar import agent_payload
    from tests.test_cloud_mount_sidecar_delivery import _protected_plan

    return agent_payload(_protected_plan().recorded())


def _protected_ready(sidecar: dict | None = None) -> dict:
    from tests.test_protected_workspace_delivery import _ready_payload

    payload = _ready_payload()
    payload["cloud_mount"] = None
    payload["cloud_mount_sidecar"] = sidecar or _protected_cfg()
    return payload


def test_the_agent_accepts_the_orchestrators_protected_sidecar_payload():
    from agent.api import session_workspace

    payload = _protected_ready()
    assert session_workspace.protected_workspace_delivery(payload) == "ready"
    sidecar = payload["cloud_mount_sidecar"]
    assert session_workspace.protected_mount_payload(payload) is sidecar
    assert PersistentSession._protected_cloud_config_valid(_protected_cfg()) is True
    identity = session_workspace.protected_workspace_identity(payload)
    assert json.loads(identity.cloud_mount_json) == payload["cloud_mount_sidecar"]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda cfg: cfg.update(protected=False),
        lambda cfg: cfg.update(skip_workspace_links=False),
        lambda cfg: cfg["overlay"].update(merged="/tmp/merged"),
        lambda cfg: cfg["overlay"].update(lower="/cloud/project"),
        lambda cfg: cfg["overlay"].update(quota_bytes=True),
        lambda cfg: cfg["mounts"][0].update(access="read_write"),
        lambda cfg: cfg["mounts"][0].update(target_path="/cloud/merged"),
        lambda cfg: cfg["mounts"][0].update(mount_kind="project"),
        # A credential or a remote never crosses to the agent.
        lambda cfg: cfg["mounts"][0].update(auth={"password": "x"}),
        lambda cfg: cfg["mounts"][0].update(source={"type": "webdav"}),
        lambda cfg: cfg["mounts"].append(dict(cfg["mounts"][0], index=1)),
    ],
)
def test_a_protected_sidecar_payload_that_is_not_exact_fails_closed(mutate):
    from agent.api import session_contract, session_workspace

    cfg = _protected_cfg()
    mutate(cfg)
    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(_protected_ready(cfg))
    assert PersistentSession._protected_cloud_config_valid(cfg) is False


def test_a_protected_response_with_both_deliveries_fails_closed():
    from agent.api import session_contract, session_workspace
    from tests.test_protected_workspace_delivery import _protected_mount

    payload = _protected_ready()
    payload["cloud_mount"] = _protected_mount()
    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_mount_payload(payload)


@dataclass(frozen=True)
class _Config:
    extra: dict


def _protected_session(workspace: _Workspace, *, required: bool = True):
    session = SimpleNamespace(
        thread_id="t1",
        workspace_manager=SimpleNamespace(
            backend=workspace, path="/home/agent-host/workspace"
        ),
        shell_owner_token=None,
        protected_cloud_required=required,
        protected_cloud_unavailable=None,
        config=_Config(extra={}),
        cloud_mount_manager=None,
        sidecar_mount_watcher=None,
        overlay_mount_manager=None,
        _protected_mount_id=None,
        _protected_cloud_health_ready=False,
        cloud_mount_error=None,
        _finish_protected_cloud=AsyncMock(),
    )
    session._run_protected_without_cloud = (
        lambda watcher, reason, overlay: PersistentSession._run_protected_without_cloud(
            session, watcher, reason, overlay
        )
    )
    return session


@pytest.mark.asyncio
async def test_a_protected_session_mounts_its_overlay_on_the_sidecars_lower():
    workspace = _Workspace(["lower"])
    workspace.status[0] = {"state": "mounted"}
    workspace.live = {"lower"}
    session = _protected_session(workspace)
    cfg = _protected_cfg()
    with patch(
        "shared.runtime.services.cloud_mount.sidecar.SidecarMountWatcher._start_sync",
        lambda self: (self.read_states(), setattr(self, "_settled", True)),
    ):
        await PersistentSession._setup_sidecar_cloud_mount(session, cfg)
    watcher = session.cloud_mount_manager
    assert watcher.delivery == "sidecar" and watcher.active
    (lower,) = watcher.mounts
    assert (lower.mount_kind, lower.target_path) == ("protected_lower", "/cloud/lower")
    session._finish_protected_cloud.assert_awaited_once_with(cfg)
    # The overlay owns workspace/cloud: the watcher links nothing.
    assert not any("ln -sfn" in command for command in workspace.commands)
    assert not any(_runs_rclone(command) for command in workspace.commands)


@pytest.mark.asyncio
async def test_a_protected_session_whose_lower_did_not_mount_starts_without_cloud():
    """Decision 42: no cloud folder at all, so nothing reaches the cloud
    unreviewed; the state, the prompt and the tool say why; the attach does
    not fail."""
    workspace = _Workspace(["lower"])
    workspace.status[0] = {"state": "unavailable", "reason": "credential_rejected"}
    session = _protected_session(workspace)
    with patch(
        "shared.runtime.services.cloud_mount.sidecar.SidecarMountWatcher._start_sync",
        lambda self: (self.read_states(), setattr(self, "_settled", True)),
    ):
        await PersistentSession._setup_sidecar_cloud_mount(session, _protected_cfg())
    session._finish_protected_cloud.assert_not_awaited()
    assert session.cloud_mount_manager is None
    assert session.protected_cloud_unavailable == "credential_rejected"
    watcher = session.sidecar_mount_watcher
    assert watcher.report() == [
        {"name": "lower", "state": "unavailable", "reason": "credential_rejected"}
    ]
    assert "Protected cloud unavailable" in watcher.status()
    rows = session.config.extra["_cloud_mounts_unavailable"]
    floor = cloud_mount_system_floor(rows)
    assert "Protected cloud unavailable: the cloud refused its credential" in floor
    assert "no cloud folder at all" in floor
    # Ready, on purpose: no cloud manager and no overlay.
    ready = PersistentSession.protected_cloud_ready(
        SimpleNamespace(
            protected_cloud_required=True,
            protected_cloud_unavailable="credential_rejected",
            cloud_mount_manager=None,
            overlay_mount_manager=None,
        )
    )
    assert ready is True


@pytest.mark.asyncio
async def test_a_protected_session_whose_overlay_failed_starts_without_cloud():
    workspace = _Workspace(["lower"])
    workspace.status[0] = {"state": "mounted"}
    workspace.live = {"lower"}
    session = _protected_session(workspace)
    session._finish_protected_cloud = AsyncMock(side_effect=RuntimeError("overlay"))
    with patch(
        "shared.runtime.services.cloud_mount.sidecar.SidecarMountWatcher._start_sync",
        lambda self: (self.read_states(), setattr(self, "_settled", True)),
    ):
        await PersistentSession._setup_sidecar_cloud_mount(session, _protected_cfg())
    assert session.protected_cloud_unavailable == "protected_unavailable"
    assert session.cloud_mount_manager is None
    assert session.sidecar_mount_watcher.report()[0]["reason"] == (
        "protected_unavailable"
    )


@pytest.mark.asyncio
async def test_stateless_end_retires_the_overlay_then_asks_the_sidecars_to_flush():
    from orchestrator.services import stateless_session_retirement as retirement
    from shared.runtime.services.cloud_overlay import OverlayMountManager
    from tests.test_managed_repository_agent_lifecycle import _terminal_thread

    workspace = _Workspace(["lower"])
    workspace.status[0] = {"state": "mounted"}
    workspace.live = {"lower"}
    workspace.verify_terminal_claim_resources_retired = MagicMock(return_value="")
    workspace.retire = MagicMock()
    workspace.resolve_home_path = lambda rel: f"/home/agent-host/{rel}"
    order: list[str] = []
    original = SidecarMountWatcher.retire_existing

    async def overlay_retired(self):
        order.append("overlay")
        return {"overlay_mounts": 0, "overlay_processes": 0}

    async def watcher_retired(self, **kwargs):
        order.append("sidecars")
        return await original(self, **kwargs)

    with (
        patch.object(retirement, "_build_terminal_backend", return_value=workspace),
        patch.object(OverlayMountManager, "retire_existing", overlay_retired),
        patch.object(SidecarMountWatcher, "retire_existing", watcher_retired),
    ):
        await retirement.retire_stateless_workspace_residents(
            _terminal_thread(), terminal_token=9, cloud_mount_cfg=_protected_cfg()
        )
        await retirement.verify_stateless_workspace_residents_retired(
            _terminal_thread(), terminal_token=9, cloud_mount_cfg=_protected_cfg()
        )
    assert order == ["overlay", "sidecars"]
    assert any("/srw/cloud-control/drain" in c for c in workspace.terminal)
    # The resident zero proof and the overlay's, never an rclone re-proof.
    assert workspace.verify_terminal_claim_resources_retired.call_count == 2


@pytest.mark.asyncio
async def test_a_reattach_whose_folders_settled_once_does_not_wait_again():
    workspace = _Workspace(["project"])
    workspace.status[0] = {"state": "pending"}
    clock = [0.0]
    watcher = SidecarMountWatcher(
        thread_id="t1",
        cloud_cfg={**_cfg("project"), "settled": True, "wait_seconds": 30},
        workspace_backend=workspace,
        workspace_root="/home/agent-host/workspace",
        clock=lambda: clock[0],
        sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )
    await watcher.start_all()
    assert clock[0] == 0.0
    # Without the record's word it waits its 30 s.
    waiting = _watcher(workspace, {**_cfg("project"), "wait_seconds": 30})
    await waiting.start_all()
    assert waiting.report() == [{"name": "project", "state": "pending"}]


def test_a_refresh_must_reach_every_folder():
    workspace = _Workspace(["project", "home"])
    workspace.status[0] = {"state": "mounted"}
    workspace.status[1] = {"state": "mounted"}
    watcher = _watcher(workspace, _cfg("project", "home"))
    watcher.refresh_vfs()
    original = workspace._answer

    def one_failed(command: str) -> str:
        out = original(command)
        if "/srw/cloud-control/refresh" in command:
            workspace.status[1]["refresh"]["state"] = "failed"
        return out

    workspace._answer = one_failed
    with pytest.raises(SidecarMountError, match="did not refresh"):
        watcher.refresh_vfs()


def test_several_folders_named_oddly_are_linked_one_by_one_and_quoted():
    workspace = _Workspace(["a b", "c"])
    cfg = _cfg("a b", "c")
    cfg["mounts"][0]["target_path"] = "/cloud/a-b"
    watcher = _watcher(workspace, cfg)
    watcher._start_sync()
    links = next(c for c in workspace.commands if "ln -sfn" in c)
    assert "'a b'" in links and "mkdir -p" in links


@pytest.mark.asyncio
async def test_the_state_report_names_its_pod_and_never_holds_the_attach():
    from agent.api.session_attach import SessionAttachCoordinator

    events: list = []
    calls: list[dict] = []
    gate = asyncio.Event()

    async def report_cloud_mount_status(thread_id, **kwargs):
        calls.append(kwargs)
        await gate.wait()
        return True

    ports = MagicMock()
    ports.broadcast = lambda name, payload: events.append((name, payload))
    ports.identity.return_value = SimpleNamespace(thread_id="t1")
    ports.orchestrator_client.return_value = SimpleNamespace(
        report_cloud_mount_status=report_cloud_mount_status
    )
    coordinator = SessionAttachCoordinator(ports)
    watcher = _watcher(_Workspace(["project"]), _cfg("project"))
    await asyncio.wait_for(
        coordinator._report_sidecar_mounts(watcher, [{"name": "project"}]), 1
    )
    assert events[0][0] == "cloud_mount.status"
    await asyncio.sleep(0)
    assert calls == [
        {
            "fingerprint": FINGERPRINT,
            "pod_uid": POD_UID,
            "mounts": [{"name": "project"}],
        }
    ]
    gate.set()
    await asyncio.gather(*coordinator._report_tasks)


def test_a_protected_sidecar_shape_needs_the_marker():
    from agent.api import session_contract, session_workspace

    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(
            {"protected_cloud": False, "cloud_mount_sidecar": _protected_cfg()}
        )
    assert (
        session_workspace.protected_workspace_delivery(
            {"protected_cloud": False, "cloud_mount_sidecar": _cfg("project")}
        )
        == "off"
    )


def test_the_stale_rclone_cleanup_removes_the_directory_and_refuses_a_live_rclone(
    tmp_path,
):
    import os
    import subprocess

    from orchestrator.services import stateless_session_retirement as retirement

    thread = "33333333-3333-4333-8333-333333333333"
    base = tmp_path / ".cache/srw/rclone" / thread / "m0"
    base.mkdir(parents=True)
    (base / "resident.identity").write_text("old\n")
    (base / "rclone.conf").write_text("[m0]\npass = obscured\n")
    (base / "vfs-cache").mkdir()
    script = retirement._sidecar_stale_rclone_cleanup_command(thread)
    env = {**os.environ, "HOME": str(tmp_path)}
    done = subprocess.run(
        ["sh", "-c", script], env=env, capture_output=True, text=True, timeout=60
    )
    assert done.returncode == 0, done.stderr
    assert retirement.SIDECAR_RCLONE_ZERO_MARKER in done.stdout
    # Identities and the old rclone.conf (an obscured password) alike.
    assert not (tmp_path / ".cache/srw/rclone" / thread).exists()
    assert (tmp_path / ".cache/srw/rclone").is_dir()
    base.mkdir(parents=True)
    # A process of the thread's rclone is a refusal, never cleaned up.
    import sys

    live = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)", str(base) + "/"],
        env=env,
    )
    try:
        refused = subprocess.run(
            ["sh", "-c", script], env=env, capture_output=True, text=True, timeout=60
        )
    finally:
        live.kill()
        live.wait()
    assert refused.returncode == 85


@pytest.mark.asyncio
@pytest.mark.parametrize("stateless", [False, True])
async def test_without_its_cloud_a_protected_session_keeps_no_link_to_a_dead_overlay(
    stateless,
):
    """A workspace/cloud link to /cloud/merged from an earlier protected
    attach would send writes into the sidecars' 1 MiB memory volume, lost
    silently: decision 42 unmounts a leftover overlay (strictly on a
    stateless claim) and removes that link."""
    from shared.runtime.services.cloud_overlay import OverlayMountManager

    workspace = _Workspace(["lower"])
    workspace.status[0] = {"state": "unavailable", "reason": "unreachable"}
    session = _protected_session(workspace)
    session.shell_owner_token = 7 if stateless else None
    unmounts: list[dict] = []

    def unmount(self, **kwargs):
        unmounts.append({"merged": self.merged, **kwargs})

    with (
        patch(
            "shared.runtime.services.cloud_mount.sidecar.SidecarMountWatcher._start_sync",
            lambda self: (self.read_states(), setattr(self, "_settled", True)),
        ),
        patch.object(OverlayMountManager, "unmount", unmount),
    ):
        await PersistentSession._setup_sidecar_cloud_mount(session, _protected_cfg())
    assert unmounts == [{"merged": "/cloud/merged", "strict": stateless}]
    unlink = next(c for c in workspace.commands if "SRW_UNLINK_OK" in c)
    assert "/cloud/merged" in unlink and "readlink" in unlink and "rm -f" in unlink
    # The error says what the user sees, never a workspace/cloud path.
    assert session.cloud_mount_error.startswith("Protected cloud unavailable")
    assert "workspace/cloud:" not in session.cloud_mount_error


def test_a_drain_that_gave_uploads_up_for_good_says_so(caplog):
    import logging

    workspace = _Workspace(["project"])
    workspace.status[0] = {"state": "unavailable", "reason": "not_found"}
    workspace.drain_answer = {"state": "drained", "pending": 0, "lost": 2}
    watcher = _watcher(workspace, _cfg("project"), terminal=True)
    with caplog.at_level(logging.WARNING):
        assert watcher.request_drain() == (True, 0)
    assert "2 upload(s) were lost" in caplog.text


def test_a_control_request_is_readable_by_the_supervisor(tmp_path):
    """The supervisor runs as another user: a request written under SRW's
    umask 077 would never be read, and End would wait for it forever."""
    import os
    import stat
    import subprocess

    class _Local:
        root = str(tmp_path)

        def exec_claim_resource(self, command, *, timeout, operation):
            done = subprocess.run(
                ["sh", "-c", "umask 077; " + command],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            assert done.returncode == 0, done.stderr
            return done.stdout

    control = tmp_path / "control"
    control.mkdir()
    cfg = {**_cfg("project"), "control_dir": str(control)}
    watcher = _watcher(_Local(), cfg)
    nonce = watcher._request("drain")
    request = control / "drain"
    assert request.read_text() == nonce
    assert stat.S_IMODE(os.stat(request).st_mode) == 0o644
    assert not (control / "drain.tmp").exists()
