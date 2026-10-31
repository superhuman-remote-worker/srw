"""The agent side of cloud folders on a Virtual Session, above the backend
(features/cloud_folders_on_virtual_sessions.md § Agent, § Status, prompt):

* the prompt rows a Virtual Session builds from its backend's folders, and the
  ``<cloud_folders>`` floor without the shell-only status tool;
* the attach's cockpit event rows and the probe-and-report flow;
* the file tools over a backend with cloud folders: the "changed since you
  read it" refusal, the refusals the backend raises as tool errors, and the
  search tool's truncation note.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any


from agent.api import persistent_session as ps_module
from agent.api import session_attach
from agent.core.backends.virtual import CloudRemote, VirtualWorkspaceBackend
from agent.core.workspace import WorkspaceManager
from agent.tools.context import ToolContext
from agent.tools.workspace import create_workspace_tools
from shared.runtime.core.backends.object_store import InMemoryObjectStore
from shared.runtime.core.loader import cloud_mount_system_floor


def _backend(**kwargs):
    outbox, proj = InMemoryObjectStore(), InMemoryObjectStore()
    backend = VirtualWorkspaceBackend(
        InMemoryObjectStore(),
        prefix="threads/t/",
        cloud_remotes=[
            CloudRemote(
                name="outbox",
                path="cloud/outbox",
                store=outbox,
                access="read_write",
                kind="cloud_outbox",
                mount_id="c-1",
                shared=False,
            ),
            CloudRemote(
                name="proj",
                path="cloud/proj",
                store=proj,
                access="read_only",
                kind="project",
                mount_id="c-2",
            ),
        ],
        **kwargs,
    )
    return backend, outbox, proj


# ---------------------------------------------------------------------------
# Prompt rows and the floor
# ---------------------------------------------------------------------------


def test_prompt_rows_name_each_folder_at_its_path():
    backend, *_ = _backend()
    mounted, unavailable = ps_module._remote_cloud_folders(backend.cloud_folders())
    assert unavailable == []
    assert [(r["name"], r["path"], r["state"]) for r in mounted] == [
        ("outbox", "cloud/outbox", "mounted"),
        ("proj", "cloud/proj", "mounted"),
    ]
    assert "sign in to the cloud once" in mounted[0]["text"]  # shared=False
    assert "read-only" in mounted[1]["text"]
    floor = cloud_mount_system_floor(unavailable, mounted, status_tool=False)
    assert "- cloud/outbox: a folder SRW created for this session" in floor
    assert "- cloud/proj: the project's folder" in floor
    assert "srw_cloud_status" not in floor
    assert "srw_cloud_status" in cloud_mount_system_floor(unavailable, mounted)


def test_a_failed_folder_is_named_with_its_reason():
    backend, _, proj = _backend()
    backend.cloud_remotes[1].state = "failed"
    backend.cloud_remotes[1].reason = "mount_failed"
    mounted, unavailable = ps_module._remote_cloud_folders(backend.cloud_folders())
    assert [r["name"] for r in mounted] == ["outbox"]
    assert unavailable == [
        {
            "name": "proj",
            "path": "cloud/proj",
            "kind": "project",
            "reason": "mount_failed",
            "text": "the folder could not be mounted",
        }
    ]
    floor = cloud_mount_system_floor(unavailable, mounted, status_tool=False)
    assert "- cloud/proj: the folder could not be mounted." in floor


@dataclass
class _Config:
    extra: dict = field(default_factory=dict)


def _fake_session(backend):
    return SimpleNamespace(
        config=_Config(extra={"keep": 1}),
        workspace_manager=SimpleNamespace(backend=backend),
    )


def test_the_session_keeps_the_rows_in_its_config_and_probes_off_the_turn():
    backend, _, _ = _backend()
    fake = _fake_session(backend)
    ps_module.PersistentSession._refresh_cloud_remote_prompt(fake, backend)
    assert fake.config.extra["keep"] == 1
    assert [r["path"] for r in fake.config.extra["_cloud_mounts_mounted"]] == [
        "cloud/outbox",
        "cloud/proj",
    ]
    assert fake.config.extra["_cloud_status_tool"] is False
    assert [r["name"] for r in ps_module.PersistentSession.cloud_remote_rows(fake)] == [
        "outbox",
        "proj",
    ]

    class Broken(InMemoryObjectStore):
        def list_dir(self, prefix):
            raise RuntimeError("no route to host")

    backend.cloud_remotes[1].store = Broken()
    fake._refresh_cloud_remote_prompt = (
        lambda b: ps_module.PersistentSession._refresh_cloud_remote_prompt(fake, b)
    )
    rows = asyncio.run(ps_module.PersistentSession.probe_cloud_remotes(fake))
    assert [(r["name"], r["state"]) for r in rows] == [
        ("outbox", "ready"),
        ("proj", "failed"),
    ]
    assert [r["name"] for r in fake.config.extra["_cloud_mounts_unavailable"]] == [
        "proj"
    ]
    assert [r["name"] for r in fake.config.extra["_cloud_mounts_mounted"]] == ["outbox"]


def test_a_session_without_cloud_folders_has_no_rows():
    plain = VirtualWorkspaceBackend(InMemoryObjectStore(), prefix="threads/t/")
    fake = _fake_session(plain)
    ps_module.PersistentSession._refresh_cloud_remote_prompt(fake, plain)
    assert fake.config.extra == {"keep": 1}
    assert ps_module.PersistentSession.cloud_remote_rows(fake) == []
    assert asyncio.run(ps_module.PersistentSession.probe_cloud_remotes(fake)) == []
    assert (
        ps_module.PersistentSession.cloud_remote_rows(
            SimpleNamespace(workspace_manager=None)
        )
        == []
    )


# ---------------------------------------------------------------------------
# The attach: cockpit event rows and the report
# ---------------------------------------------------------------------------


def _attach_class():
    return next(
        c
        for c in vars(session_attach).values()
        if isinstance(c, type) and hasattr(c, "_remote_cloud_event_rows")
    )


def test_event_rows_read_like_the_sidecars():
    backend, *_ = _backend()
    backend.cloud_remotes[1].state = "failed"
    backend.cloud_remotes[1].reason = "mount_failed"
    rows = _attach_class()._remote_cloud_event_rows(backend.cloud_folders())
    assert rows == [
        {
            "name": "outbox",
            "state": "mounted",
            "mount_kind": "cloud_outbox",
            "level": "read_write",
            "connector_id": "c-1",
            "delivery": "remote",
        },
        {
            "name": "proj",
            "state": "unavailable",
            "mount_kind": "project",
            "level": "read_only",
            "connector_id": "c-2",
            "delivery": "remote",
            "reason": "mount_failed",
        },
    ]


def test_the_probe_reports_to_the_cockpit_and_the_orchestrator():
    backend, *_ = _backend()
    events: list[tuple[str, dict]] = []
    reports: list[dict] = []

    async def report_status(thread_id, *, fingerprint, pod_uid, mounts):
        reports.append(
            {
                "thread": thread_id,
                "fingerprint": fingerprint,
                "pod_uid": pod_uid,
                "mounts": mounts,
            }
        )
        return True

    fake_session = SimpleNamespace(
        cloud_remote_plan={"fingerprint": "fp-1", "incarnation": "claim-9"},
        probe_cloud_remotes=lambda: asyncio.to_thread(backend.probe_cloud_folders),
    )
    fake = SimpleNamespace(
        _session=fake_session,
        _ports=SimpleNamespace(
            broadcast=lambda kind, payload: events.append((kind, payload))
        ),
        _identity=SimpleNamespace(thread_id="thread-1"),
        _client=SimpleNamespace(report_cloud_mount_status=report_status),
        _logger=SimpleNamespace(warning=lambda *a, **k: None),
    )
    cls = _attach_class()
    fake._remote_cloud_event_rows = cls._remote_cloud_event_rows
    fake._broadcast_remote_cloud_folders = (
        lambda rows: cls._broadcast_remote_cloud_folders(fake, rows)
    )
    asyncio.run(cls._probe_and_report_remote_cloud_folders(fake))

    assert [kind for kind, _ in events] == ["cloud_mount.status"]
    payload = events[0][1]
    assert payload["delivery"] == "remote" and payload["excluded"] == []
    assert [(m["name"], m["state"]) for m in payload["mounts"]] == [
        ("outbox", "mounted"),
        ("proj", "mounted"),
    ]
    assert reports == [
        {
            "thread": "thread-1",
            "fingerprint": "fp-1",
            "pod_uid": "claim-9",
            # The record's closed states: a folder that answered is mounted.
            "mounts": [
                {"name": "outbox", "state": "mounted"},
                {"name": "proj", "state": "mounted"},
            ],
        }
    ]


def test_without_a_plan_the_probe_only_tells_the_cockpit():
    backend, *_ = _backend()
    events: list[str] = []
    calls: list[Any] = []

    async def report_status(*a, **k):
        calls.append(a)
        return True

    fake = SimpleNamespace(
        _session=SimpleNamespace(
            cloud_remote_plan=None,
            probe_cloud_remotes=lambda: asyncio.to_thread(backend.probe_cloud_folders),
        ),
        _ports=SimpleNamespace(broadcast=lambda kind, payload: events.append(kind)),
        _identity=SimpleNamespace(thread_id="thread-1"),
        _client=SimpleNamespace(report_cloud_mount_status=report_status),
        _logger=SimpleNamespace(warning=lambda *a, **k: None),
    )
    cls = _attach_class()
    fake._remote_cloud_event_rows = cls._remote_cloud_event_rows
    fake._broadcast_remote_cloud_folders = (
        lambda rows: cls._broadcast_remote_cloud_folders(fake, rows)
    )
    asyncio.run(cls._probe_and_report_remote_cloud_folders(fake))
    assert events == ["cloud_mount.status"] and calls == []


# ---------------------------------------------------------------------------
# The file tools over a backend with cloud folders
# ---------------------------------------------------------------------------


def _tools(tmp_path, **kwargs):
    backend, outbox, proj = _backend(**kwargs)
    ws = WorkspaceManager(job_id="cloud-tools", base_path=tmp_path, backend=backend)
    ws.initialize()
    ctx = ToolContext(workspace_manager=ws, config={"max_search_results": 50})
    tools = {t.name: t for t in create_workspace_tools(ctx)}
    return tools, ctx, outbox, proj


def test_a_cloud_file_the_human_changed_is_refused_until_reread(tmp_path):
    tools, ctx, outbox, _ = _tools(tmp_path)
    outbox.put("report.md", b"draft by the agent\n")
    assert "draft" in tools["read_file"].invoke({"path": "cloud/outbox/report.md"})
    outbox.put("report.md", b"edited by a human\n")

    out = tools["edit_file"].invoke(
        {"path": "cloud/outbox/report.md", "old_string": "draft", "new_string": "final"}
    )
    assert out.startswith("Error: 'cloud/outbox/report.md' changed since you read it")
    assert outbox.get("report.md") == b"edited by a human\n"

    out = tools["write_file"].invoke(
        {"path": "cloud/outbox/report.md", "content": "whole\n"}
    )
    assert "changed since you read it" in out or "read_file" in out
    assert outbox.get("report.md") == b"edited by a human\n"

    # After a fresh read the edit goes through, and the write is whole-file.
    tools["read_file"].invoke({"path": "cloud/outbox/report.md"})
    out = tools["edit_file"].invoke(
        {"path": "cloud/outbox/report.md", "old_string": "human", "new_string": "robot"}
    )
    assert not out.startswith("Error")
    assert outbox.get("report.md") == b"edited by a robot\n"


def test_a_file_never_read_gets_the_read_first_nudge(tmp_path):
    tools, ctx, outbox, _ = _tools(tmp_path)
    outbox.put("notes.md", b"something\n")
    out = tools["write_file"].invoke(
        {"path": "cloud/outbox/notes.md", "content": "x\n"}
    )
    assert "changed since you read it" not in out
    assert "read" in out.lower() and outbox.get("notes.md") == b"something\n"


def test_backend_refusals_arrive_as_tool_errors(tmp_path):
    tools, _, outbox, proj = _tools(tmp_path)
    proj.put("a.txt", b"a")
    out = tools["write_file"].invoke({"path": "cloud/proj/b.txt", "content": "x"})
    assert out.startswith("Error:") and "cloud/proj" in out and "read_only" in out
    out = tools["search_files"].invoke({"query": "a"})
    assert out.startswith("Error:") and "cloud/outbox" in out
    out = tools["read_file"].invoke({"path": "cloud/nothere/a.txt"})
    assert "not found" in out.lower()


def test_the_search_tool_says_when_it_stopped_early(tmp_path):
    tools, _, outbox, _ = _tools(tmp_path, cloud_search_files=2)
    for i in range(3):
        outbox.put(f"f{i}.md", b"needle\n")
    out = tools["search_files"].invoke({"query": "needle", "path": "cloud/outbox"})
    assert "cloud/outbox/f0.md" in out and "cloud/outbox/f1.md" in out
    assert (
        "[truncated: only the first 2 files under cloud/outbox were searched (limit 2)"
        in out
    )
    outbox.put("f0.md", b"nothing\n")
    outbox.put("f1.md", b"nothing\n")
    out = tools["search_files"].invoke({"query": "needle", "path": "cloud/outbox"})
    assert out.startswith("No matches found for: needle") and "[truncated" in out


def test_listing_through_the_tools_shows_the_folders(tmp_path):
    tools, _, outbox, _ = _tools(tmp_path)
    outbox.put("r.md", b"r")
    out = tools["list_files"].invoke({"path": "cloud"})
    assert "outbox/" in out and "proj/" in out
    out = tools["list_files"].invoke({"path": "cloud/outbox"})
    assert "r.md" in out
    assert "Written: cloud/outbox/new.md" in tools["write_file"].invoke(
        {"path": "cloud/outbox/new.md", "content": "n"}
    )


def test_remote_mounts_are_read_off_a_workspace_payload():
    """The attach and the turn-start retry both ask the same question of a
    workspace payload: does it hand this session remote descriptors? Only a
    well-formed ``cloud_remote.mounts`` list counts, and only its dict rows."""
    from agent.api.session_workspace import remote_cloud_mounts

    assert remote_cloud_mounts(None) == []
    assert remote_cloud_mounts({}) == []
    assert remote_cloud_mounts({"cloud_remote": "junk"}) == []
    assert remote_cloud_mounts({"cloud_remote": {"mounts": []}}) == []
    assert remote_cloud_mounts({"cloud_remote": {"mounts": "cloud/outbox"}}) == []
    rows = remote_cloud_mounts(
        {
            "cloud_remote": {
                "mounts": [{"name": "outbox", "path": "cloud/outbox"}, "junk"],
                "plan": {"fingerprint": "f" * 64, "incarnation": "c" * 32},
            }
        }
    )
    assert rows == [{"name": "outbox", "path": "cloud/outbox"}]
