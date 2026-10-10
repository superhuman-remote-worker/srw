"""A session Pod's cloud mounts from the in-pod plane (connector drivers D7).

What the plan must hold to: it is the in-pod plane wholesale or not at all,
it never carries a password outside the credential file, the file is what
rclone itself would write, every row the set rule left out is named with a
closed reason, and what the supervisor and the agent receive matches their
contracts (drivers/cloud-mount, the agent's sidecar payload).
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import re
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import agent_cloud_mounts, cloud_mount_plan
from orchestrator.services.cloud_mount_plan import (
    CloudMountPlan,
    rclone_obscure,
    rclone_reveal,
    resolve_cloud_mount_plan,
)
from orchestrator.services.cloud_mount_sidecar import (
    PLAN_ANNOTATION,
    PLAN_CONTEXT_KEY,
    agent_payload,
    recorded_plan_from_annotations,
    recorded_sidecar_plan,
)
from orchestrator.services.in_pod_mount import InPodPlaneSettings
from orchestrator.services.session_class_policy import (
    materialized_session_class_override,
)

ROOT = Path(__file__).resolve().parents[1]
THREAD_ID = "33333333-3333-4333-8333-333333333333"
POD_UID = "44444444-4444-4444-8444-444444444444"
SETTINGS = InPodPlaneSettings(
    opener_image="registry.example/srw-fuse-opener:1@sha256:" + "1" * 64,
    rclone_image="registry.example/srw-cloud-mount:1@sha256:" + "2" * 64,
    cache_size="10Gi",
    drain_seconds=60,
    max_mounts=8,
)
PASSWORD = "-Yi9OE p@ss/wörd"


def _deps(
    driver: str = "rclone_mount",
) -> agent_cloud_mounts.AgentCloudMountDependencies:
    return agent_cloud_mounts.AgentCloudMountDependencies(
        store=SimpleNamespace(),
        cloud_router=SimpleNamespace(),
        cloud_tasks=SimpleNamespace(),
        is_protected_cloud_mode_enabled=lambda: True,
        cloud_workspace_driver=lambda: driver,
        slugify_mount_name=lambda name: name.lower(),
    )


def _entry(name: str = "project", **over) -> dict:
    """A built rclone mount, as _build_rclone_mount_from_row returns it."""
    entry = {
        "mount_id": f"row-{name}",
        "mount_kind": "project",
        "backend": "nextcloud",
        "target_path": f"/cloud/{name}",
        "workspace_name": name,
        "access": "read_write",
        "source_ref": f"project-{name}",
        "source": {
            "type": "webdav",
            "config": {
                "url": f"http://srw-nextcloud/remote.php/dav/files/agent/{name}/",
                "vendor": "nextcloud",
                "user": "agent-service",
            },
        },
        "auth": {"type": "basic", "password": PASSWORD},
        "provider_flags": [],
        "cache": {"vfs_cache_mode": "full", "vfs_cache_max_size": "10G"},
        "required_capabilities": ["rclone", "fuse", "rc"],
    }
    entry.update(over)
    return entry


def _thread(**metadata) -> dict:
    return {"id": THREAD_ID, "metadata": metadata}


async def _resolve(monkeypatch, entries, excluded=(), thread=None, **kw):
    live = agent_cloud_mounts.LiveMountSet(
        mounts=list(entries), fallback=bool(excluded), excluded=list(excluded)
    )
    monkeypatch.setattr(
        agent_cloud_mounts,
        "_resolve_live_mount_set",
        AsyncMock(return_value=live),
    )
    return await resolve_cloud_mount_plan(
        thread or _thread(),
        mount_rows=[],
        settings=kw.pop("settings", SETTINGS),
        dependencies=kw.pop("dependencies", _deps()),
    )


# --------------------------------------------------------------------------- #
# Obscuring: what rclone itself writes
# --------------------------------------------------------------------------- #


def test_obscure_matches_rclone_both_ways():
    # Produced by `rclone obscure -- '-Yi9OE p@ss/wörd'` (rclone 1.74.3).
    assert rclone_reveal("AZBCWD7iYB3yNtHVaQfCeNBas-2YsKX13CDfQ8OlK2q4") == PASSWORD
    # `rclone reveal` of this (IV 00..0f) printed the password back.
    assert (
        rclone_obscure(PASSWORD, iv=bytes(range(16)))
        == "AAECAwQFBgcICQoLDA0ODyzGDhxIl2D6jVJ7SnMjeVBU"
    )
    first, second = rclone_obscure(PASSWORD), rclone_obscure(PASSWORD)
    assert first != second and rclone_reveal(first) == rclone_reveal(second) == PASSWORD


# --------------------------------------------------------------------------- #
# Who gets the plane
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_static_password_mounts_get_the_plane(monkeypatch):
    plan = await _resolve(monkeypatch, [_entry("project"), _entry("home")])
    assert [m.name for m in plan.mounts] == ["project", "home"]
    assert [m.remote for m in plan.mounts] == ["m0:", "m1:"]
    assert [m.target for m in plan.mounts] == ["/srw/cloud/project", "/srw/cloud/home"]
    assert plan.passwords == {0: PASSWORD, 1: PASSWORD}
    assert plan.protected is False and plan.excluded == ()


def _with_cloud(**backend) -> agent_cloud_mounts.AgentCloudMountDependencies:
    """Dependencies whose thread resolves to a main-cloud backend so."""
    return dataclasses.replace(
        _deps(),
        cloud_router=SimpleNamespace(
            for_thread_optional=lambda thread: SimpleNamespace(**backend)
        ),
    )


@pytest.mark.asyncio
async def test_nothing_to_mount_keeps_todays_path(monkeypatch):
    """Decision 43: no mount and nothing left out (the main cloud off) is
    no plan at all: the Pod keeps its FUSE profile and sends no status."""
    assert await _resolve(monkeypatch, []) is None
    off = _with_cloud(is_initialized=False, static_mount_credentials=True)
    assert await _resolve(monkeypatch, [], dependencies=off) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("thread", "detail"),
    [
        (_thread(), "no_session_folder"),
        ({**_thread(), "nc_session_folder": "nextcloud:x"}, "spec_failed"),
    ],
)
async def test_a_session_folder_the_main_cloud_failed_to_make_is_named(
    monkeypatch, thread, detail
):
    up = _with_cloud(is_initialized=True, static_mount_credentials=True)
    plan = await _resolve(monkeypatch, [], thread=thread, dependencies=up)
    assert plan.mounts == ()
    assert plan.excluded == (
        {
            "source_ref": "session-folder",
            "mount_kind": "session_folder",
            "reason": "unbuildable",
            "detail": detail,
        },
    )
    # A provider whose mounts need bearer tokens keeps the old path anyway.
    bearer = _with_cloud(is_initialized=True, static_mount_credentials=False)
    assert await _resolve(monkeypatch, [], thread=thread, dependencies=bearer) is None


@pytest.mark.asyncio
async def test_a_plan_rebuilds_from_what_its_pod_recorded(monkeypatch):
    plan = await _resolve(monkeypatch, [_entry("project"), _entry("home")])
    recorded = json.loads(cloud_mount_plan.plan_annotation(plan))
    rebuilt = CloudMountPlan.from_recorded(recorded, plan.passwords_by_mount_id())
    assert rebuilt == plan
    assert rebuilt.passwords == plan.passwords
    # A password gone, or a record that is not intact, rebuilds nothing.
    assert CloudMountPlan.from_recorded(recorded, {"row-project": PASSWORD}) is None
    tampered = {**recorded, "drain_seconds": 5}
    assert CloudMountPlan.from_recorded(tampered, plan.passwords_by_mount_id()) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("why", "kwargs"),
    [
        ("plane off", {"settings": None}),
        ("sync driver", {"dependencies": _deps(driver="sync")}),
        (
            "officer",
            {
                "thread": _thread(
                    config_override={
                        "officer": materialized_session_class_override(
                            {"officer": {"enabled": True}}
                        )
                    }
                )
            },
        ),
        # No runtime authority, so no grant of this runtime (Phase B below).
        ("protected without a grant", {"thread": _thread(protected_cloud=True)}),
        ("malformed protected marker", {"thread": _thread(protected_cloud="yes")}),
    ],
)
async def test_opted_out_sessions_keep_the_old_path(monkeypatch, why, kwargs):
    assert await _resolve(monkeypatch, [_entry()], **kwargs) is None, why


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "officer",
    [
        # What creation materializes on every session's override.
        materialized_session_class_override({}),
        materialized_session_class_override({"officer": {"conference": True}}),
        {"enabled": False},
        {},
    ],
)
async def test_a_session_that_is_no_officer_gets_the_plane(monkeypatch, officer):
    """Every session's override carries an officer block; only an enabled one
    keeps the old path (every k3d gate session fell back before)."""
    thread = _thread(config_override={"officer": officer})
    plan = await _resolve(monkeypatch, [_entry()], thread=thread)
    assert plan is not None and len(plan.mounts) == 1


@pytest.mark.asyncio
async def test_containers_without_rclone_keep_the_old_path(monkeypatch):
    monkeypatch.setenv("CLOUD_RCLONE_ALLOW_CONTAINER", "false")
    assert await _resolve(monkeypatch, [_entry()]) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "broken",
    [
        # OpenCloud: a bearer token the agent refreshes, not a password.
        {"auth": {"type": "keycloak_client_credentials", "client_secret": "x"}},
        {"auth": {"type": "basic", "password": ""}},
        {"auth": {"type": "basic", "password": "line\nbreak"}},
        {"source": {"type": "s3", "config": {"url": "http://x"}}},
        {
            "source": {
                "type": "webdav",
                "config": {"url": "http://x", "bearer_token": "t"},
            }
        },
        {"source": {"type": "webdav", "config": {"url": "http://x\n[evil]"}}},
        {"min_rclone_version": "1.70.0"},
        {"provider_flags": ["--rc-addr", ":5572"]},
        {"provider_flags": ["--config=/tmp/x"]},
        {"workspace_name": ".hidden"},
        # The opener reads a target as PATH or PATH:ro.
        {"workspace_name": "a:ro"},
    ],
)
async def test_a_mount_needing_more_than_a_password_keeps_the_whole_pod_on_the_old_path(
    monkeypatch, broken
):
    assert await _resolve(monkeypatch, [_entry("ok"), _entry("bad", **broken)]) is None


@pytest.mark.asyncio
async def test_left_out_rows_are_named_and_extra_mounts_capped(monkeypatch):
    excluded = [
        {
            "source_ref": "row-a",
            "mount_kind": "project",
            "reason": "unbuildable",
            "detail": "not_supported",
        },
        {"source_ref": "row-b", "mount_kind": "project", "reason": "set_fallback"},
    ]
    settings = InPodPlaneSettings(
        opener_image=SETTINGS.opener_image,
        rclone_image=SETTINGS.rclone_image,
        max_mounts=2,
    )
    plan = await _resolve(
        monkeypatch,
        [_entry("a"), _entry("b"), _entry("c")],
        excluded=excluded,
        settings=settings,
    )
    assert [m.name for m in plan.mounts] == ["a", "b"]
    assert [dict(e) for e in plan.excluded] == [
        *excluded,
        {
            "source_ref": "project-c",
            "mount_kind": "project",
            "reason": "too_many_mounts",
        },
    ]


@pytest.mark.asyncio
async def test_the_shared_cache_keeps_every_mount_under_its_share(monkeypatch):
    plan = await _resolve(monkeypatch, [_entry("a"), _entry("b")])
    flags = list(plan.mounts[0].flags)
    # 10Gi / 2 mounts = 5120 MiB each, whatever the spec asked for.
    assert flags[flags.index("--vfs-cache-max-size") + 1] == "5120M"
    # And never at the node's last free disk.
    assert flags[flags.index("--vfs-cache-min-free-space") + 1] == "2G"
    assert flags[flags.index("--vfs-cache-mode") + 1] == "full"
    assert "10G" not in flags


# --------------------------------------------------------------------------- #
# The two halves: recorded plan and credential file
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_no_password_leaves_the_credential_file(monkeypatch):
    plan = await _resolve(monkeypatch, [_entry("project")])
    for public in (
        json.dumps(plan.recorded()),
        json.dumps(plan.supervisor_plan()),
        json.dumps(plan.digest_input()),
        repr(plan),
        cloud_mount_plan.plan_annotation(plan),
    ):
        assert PASSWORD not in public
    config = plan.rclone_config()
    assert PASSWORD not in config
    section = dict(
        line.split(" = ", 1) for line in config.splitlines() if " = " in line
    )
    assert config.startswith("[m0]\ntype = webdav\n")
    assert section["user"] == "agent-service" and section["vendor"] == "nextcloud"
    assert rclone_reveal(section["pass"]) == PASSWORD


@pytest.mark.asyncio
async def test_the_recorded_plan_is_deterministic_and_fingerprinted(monkeypatch):
    first = await _resolve(monkeypatch, [_entry("project")])
    second = await _resolve(
        monkeypatch, [_entry("project", auth={"type": "basic", "password": "rotated"})]
    )
    # A rotated credential is not a different Pod; a changed source is.
    assert first.recorded() == second.recorded()
    third = await _resolve(monkeypatch, [_entry("other")])
    assert third.recorded()["fingerprint"] != first.recorded()["fingerprint"]
    # The sidecar images are not part of it: a deploy that bumps them must
    # not hold a creation admitted before it.
    assert first.digest_input() == {"plan": first.recorded()}


@pytest.mark.asyncio
async def test_the_supervisor_plan_matches_its_go_contract(monkeypatch):
    plan = await _resolve(monkeypatch, [_entry("project", access="read_only")])
    supervisor = plan.supervisor_plan()
    assert set(supervisor) == {"version", "uid", "gid", "drain_seconds", "mounts"}
    (mount,) = supervisor["mounts"]
    assert set(mount) == {
        "index",
        "name",
        "remote",
        "target",
        "read_only",
        "flags",
        "ignore",
        "cloudignore",
    }
    assert mount["read_only"] is True and mount["remote"] == "m0:"
    # The flag allowlist is the supervisor's own (drivers/cloud-mount/plan.go).
    go = (ROOT / "drivers/cloud-mount/plan.go").read_text()
    go_flags = re.search(r"flagRE = regexp.MustCompile\(`\^(.*)\$`\)", go).group(1)
    assert cloud_mount_plan._FLAG.pattern == go_flags


def test_the_sidecar_spec_marks_read_only_targets():
    mount = cloud_mount_plan.SidecarMount(
        index=0,
        name="lower",
        mount_id="m",
        mount_kind="protected_lower",
        source_ref=None,
        backend="nextcloud",
        access="read_only",
        source_type="webdav",
        source_config=(("url", "http://x"),),
        root="",
        flags=(),
    )
    plan = CloudMountPlan(
        mounts=(mount,),
        excluded=(),
        drain_seconds=60,
        cache_size="10Gi",
        passwords={0: "p"},
        protected=True,
        overlay={"merged": "/cloud/merged", "lower": "/cloud/lower"},
    )
    spec = plan.sidecar_spec("ws-cloud-x")
    assert spec.targets == (("/srw/cloud/lower", True),)
    assert spec.dirs == ("/srw/cloud/merged",) and spec.protected is True


# --------------------------------------------------------------------------- #
# A protected session's lower layer (Phase B)
# --------------------------------------------------------------------------- #

GENERATION = "66666666-6666-4666-8666-666666666666"
EARLIER_GENERATION = "77777777-7777-4777-8777-777777777777"
READER_SECRET = "reader-app-password"


def _grant(**over) -> dict:
    """A cloud_ro_mounts row as the store returns it."""
    row = {
        "id": "aaaaaaaa-0000-4000-8000-000000000001",
        "status": "active",
        "backend": "nextcloud",
        "webdav_url": "https://nc.internal/remote.php/dav/files/srw-reader-u/Proj/",
        "reader_id": "srw-reader-u",
        "credentials": READER_SECRET,
        "runtime_generation": GENERATION,
        "engage_attempt": "bbbbbbbb-0000-4000-8000-000000000002",
    }
    row.update(over)
    return row


def _protected_thread() -> dict:
    return {
        "id": THREAD_ID,
        "status": "active",
        "user_id": "cccccccc-0000-4000-8000-000000000003",
        "runtime_generation": GENERATION,
        "runtime_retirement_token": None,
        "pinned_idle_terminal_intent_at": None,
        "metadata": {"protected_cloud": True},
    }


def _protected_deps(store=None, tasks=None):
    return agent_cloud_mounts.AgentCloudMountDependencies(
        store=store or SimpleNamespace(),
        cloud_router=SimpleNamespace(),
        cloud_tasks=tasks or SimpleNamespace(protected_engage_get=lambda key: None),
        is_protected_cloud_mode_enabled=lambda: True,
        cloud_workspace_driver=lambda: "rclone_mount",
        slugify_mount_name=lambda name: name,
    )


@pytest.fixture
def selection(monkeypatch):
    """The engage's own selection check, reduced to what the planner relies
    on: an active row of this runtime."""
    calls: list[str] = []

    def matches(row, mount_rows, *, thread_id, user_id, runtime_generation):
        calls.append(runtime_generation)
        return bool(
            row
            and row.get("status") == "active"
            and row.get("runtime_generation") == runtime_generation
            and thread_id == THREAD_ID
        )

    monkeypatch.setattr(
        cloud_mount_plan, "_ro_mount_matches_protected_selection", matches
    )
    return calls


async def _protected(monkeypatch, grant, *, store=None, tasks=None):
    monkeypatch.setattr(
        agent_cloud_mounts, "_resolve_protected_grant", AsyncMock(return_value=grant)
    )
    return await resolve_cloud_mount_plan(
        _protected_thread(),
        mount_rows=[{"id": "row-1"}],
        settings=SETTINGS,
        dependencies=_protected_deps(store, tasks),
    )


@pytest.mark.asyncio
async def test_a_protected_session_gets_the_plane_for_its_lower_with_this_runtimes_grant(
    monkeypatch, selection
):
    plan = await _protected(monkeypatch, _grant())
    assert plan.protected is True and selection == [GENERATION]
    (lower,) = plan.mounts
    assert (lower.mount_kind, lower.access, lower.target) == (
        "protected_lower",
        "read_only",
        "/srw/cloud/lower",
    )
    # The reader credential, in the credential file only.
    assert plan.passwords == {0: READER_SECRET}
    assert "srw-reader-u" in plan.rclone_config()
    assert plan.overlay["merged"] == "/cloud/merged"
    assert plan.overlay["lower"] == "/cloud/lower"
    recorded = json.dumps(plan.recorded())
    assert READER_SECRET not in recorded
    assert plan.recorded()["protected_grant"] == {
        "id": "aaaaaaaa-0000-4000-8000-000000000001",
        "runtime_generation": GENERATION,
        "engage_attempt": "bbbbbbbb-0000-4000-8000-000000000002",
        "reader_id": "srw-reader-u",
    }
    spec = plan.sidecar_spec("ws-cloud-x")
    assert spec.targets == (("/srw/cloud/lower", True),)
    assert spec.dirs == ("/srw/cloud/merged",) and spec.protected is True
    payload = agent_payload(plan.recorded())
    assert payload["protected"] is True and payload["skip_workspace_links"] is True
    assert READER_SECRET not in json.dumps(payload)


@pytest.mark.asyncio
async def test_the_planner_awaits_this_runtimes_engage_when_an_earlier_grant_stands(
    monkeypatch, selection
):
    rows = {"row": _grant(status="engaging", runtime_generation=EARLIER_GENERATION)}

    async def engage():
        await asyncio.sleep(0)
        rows["row"] = _grant(engage_attempt="dddddddd-0000-4000-8000-000000000004")

    task = asyncio.ensure_future(engage())
    store = SimpleNamespace(
        get_ro_mount_by_thread=AsyncMock(side_effect=lambda tid: rows["row"])
    )
    keys: list = []
    tasks = SimpleNamespace(
        protected_engage_get=lambda key: (keys.append(key), task)[1]
    )
    plan = await _protected(monkeypatch, rows["row"], store=store, tasks=tasks)
    assert keys == [(THREAD_ID, GENERATION)]
    assert plan.protected_grant["engage_attempt"] == (
        "dddddddd-0000-4000-8000-000000000004"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "grant",
    [
        None,
        _grant(status="engaging"),
        _grant(status="revoked"),
        _grant(runtime_generation=EARLIER_GENERATION),
        # OpenCloud's bearer reader: more than a static password.
        _grant(credentials=None),
    ],
)
async def test_without_this_runtimes_active_grant_the_session_keeps_the_old_path(
    monkeypatch, selection, grant
):
    store = SimpleNamespace(get_ro_mount_by_thread=AsyncMock(return_value=grant))
    assert await _protected(monkeypatch, grant, store=store) is None


@pytest.mark.asyncio
async def test_a_protected_session_without_runtime_authority_keeps_the_old_path(
    monkeypatch, selection
):
    resolver = AsyncMock(return_value=_grant())
    monkeypatch.setattr(agent_cloud_mounts, "_resolve_protected_grant", resolver)
    thread = {**_protected_thread(), "runtime_retirement_token": 3}
    assert (
        await resolve_cloud_mount_plan(
            thread, mount_rows=[], settings=SETTINGS, dependencies=_protected_deps()
        )
        is None
    )
    resolver.assert_not_awaited()


# --------------------------------------------------------------------------- #
# Reading it back
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_pods_annotation_reads_back_only_when_intact(monkeypatch):
    plan = await _resolve(monkeypatch, [_entry("project")])
    annotations = {PLAN_ANNOTATION: cloud_mount_plan.plan_annotation(plan)}
    assert recorded_plan_from_annotations(annotations) == plan.recorded()
    tampered = json.loads(annotations[PLAN_ANNOTATION])
    tampered["mounts"][0]["access"] = "read_only"
    for broken in (
        {PLAN_ANNOTATION: json.dumps(tampered)},
        {PLAN_ANNOTATION: "not json"},
        {},
        None,
    ):
        assert recorded_plan_from_annotations(broken) is None


@pytest.mark.asyncio
async def test_a_recorded_plan_belongs_to_its_pod_only(monkeypatch):
    plan = await _resolve(monkeypatch, [_entry("project")])
    recorded = {**plan.recorded(), "runtime_incarnation": POD_UID}
    metadata = {
        "workspace_container": {
            "_runtime_incarnation": POD_UID,
            PLAN_CONTEXT_KEY: recorded,
        }
    }
    assert recorded_sidecar_plan(metadata) == recorded
    metadata["workspace_container"]["_runtime_incarnation"] = (
        "55555555-5555-4555-8555-555555555555"
    )
    assert recorded_sidecar_plan(metadata) is None
    assert recorded_sidecar_plan({"workspace_container": {}}) is None


@pytest.mark.asyncio
async def test_the_agent_payload_carries_no_credential_and_no_remote(monkeypatch):
    plan = await _resolve(
        monkeypatch,
        [_entry("project")],
        excluded=[
            {"source_ref": "row-x", "mount_kind": "project", "reason": "set_fallback"}
        ],
    )
    payload = agent_payload(plan.recorded())
    text = json.dumps(payload)
    assert (
        PASSWORD not in text
        and "agent-service" not in text
        and "remote.php" not in text
    )
    assert payload["delivery"] == "sidecar"
    assert payload["status_dir"] == "/srw/cloud-status"
    assert payload["control_dir"] == "/srw/cloud-control"
    assert payload["mounts"] == [
        {
            "index": 0,
            "mount_id": "row-project",
            "mount_kind": "project",
            "target_path": "/cloud/project",
            "workspace_name": "project",
            "access": "read_write",
        }
    ]
    assert payload["excluded"][0]["reason"] == "set_fallback"


# --------------------------------------------------------------------------- #
# .cloudignore parity with the workspace manager
# --------------------------------------------------------------------------- #


def test_the_awk_compiler_matches_the_supervisors_vectors(tmp_path):
    from shared.runtime.services.cloud_mount import _CLOUDIGNORE_HELPERS

    lines = [
        "# a comment",
        "",
        "!keep.txt",
        "../escape",
        "a/../b",
        "/build/",
        "node_modules",
        "*.log",
        "docs/tmp",
        "/",
        "dir//",
        "  spaced  \r",
        "\tcache/",
        "/root.txt",
        "x[0-9]",
        "sub/dir/",
    ]
    source, dest = tmp_path / "in", tmp_path / "out"
    source.write_text("\n".join(lines) + "\n")
    dest.write_text("")
    subprocess.run(
        [
            "bash",
            "-c",
            _CLOUDIGNORE_HELPERS + f'\ncompile_cloudignore "{source}" "{dest}"\n',
        ],
        check=True,
    )
    # The same expectation as drivers/cloud-mount's TestCloudignoreCompilesLikeTheWorkspaceManager.
    assert dest.read_text().splitlines() == [
        "build/**",
        "**/build/**",
        "node_modules",
        "**/node_modules",
        "*.log",
        "docs/tmp",
        "dir/**",
        "**/dir/**",
        "spaced",
        "**/spaced",
        "cache/**",
        "**/cache/**",
        "root.txt",
        "**/root.txt",
        "x[0-9]",
        "sub/dir/**",
    ]


# --------------------------------------------------------------------------- #
# The set rule names what it leaves out
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_set_rule_names_every_row_it_left_out(monkeypatch):
    rows = [
        {
            "id": f"row-{x}",
            "backend_id": "nextcloud",
            "cloud_handle": "h",
            "mount_kind": "project",
            "target_path": x,
        }
        for x in "abc"
    ]

    async def build(row, *, workspace_name, runtime_is_vm, dependencies, failures=None):
        if row["id"] == "row-b":
            failures.append("not_supported")
            return None
        return _entry(workspace_name)

    monkeypatch.setattr(agent_cloud_mounts, "_build_rclone_mount_from_row", build)
    monkeypatch.setattr(
        agent_cloud_mounts,
        "_build_rclone_session_mount",
        AsyncMock(return_value=_entry("home", mount_kind="session_folder")),
    )
    live = await agent_cloud_mounts._resolve_live_mount_set(
        {"id": THREAD_ID}, mount_rows=rows, runtime_is_vm=False, dependencies=_deps()
    )
    assert [m["workspace_name"] for m in live.mounts] == ["home"] and live.fallback
    assert live.excluded == [
        {"source_ref": "row-a", "mount_kind": "project", "reason": "set_fallback"},
        {
            "source_ref": "row-b",
            "mount_kind": "project",
            "reason": "unbuildable",
            "detail": "not_supported",
        },
        {"source_ref": "row-c", "mount_kind": "project", "reason": "set_fallback"},
    ]


@pytest.mark.asyncio
async def test_a_row_that_cannot_build_says_why_in_a_closed_code():
    failures: list[str] = []
    assert (
        await agent_cloud_mounts._build_rclone_mount_from_row(
            {"id": "r"}, workspace_name="x", dependencies=_deps(), failures=failures
        )
        is None
    )

    def refuse(*args, **kwargs):
        raise RuntimeError("secret detail that must not be recorded")

    deps = agent_cloud_mounts.AgentCloudMountDependencies(
        store=SimpleNamespace(),
        cloud_router=SimpleNamespace(for_backend_instance=refuse),
        cloud_tasks=SimpleNamespace(),
        is_protected_cloud_mode_enabled=lambda: True,
        cloud_workspace_driver=lambda: "rclone_mount",
        slugify_mount_name=lambda name: name,
    )
    assert (
        await agent_cloud_mounts._build_rclone_mount_from_row(
            {"id": "r", "backend_id": "nextcloud", "cloud_handle": "h"},
            workspace_name="x",
            dependencies=deps,
            failures=failures,
        )
        is None
    )
    assert failures == ["no_transport", "backend_unavailable"]


@pytest.mark.asyncio
async def test_under_the_advisory_lock_the_planner_only_reads_the_grant(
    monkeypatch, selection
):
    """The engage that mints a grant takes the thread's advisory lock: the
    planner under that lock must not wait for it (nor poll), only read."""
    resolver = AsyncMock(side_effect=AssertionError("waited under the lock"))
    monkeypatch.setattr(agent_cloud_mounts, "_resolve_protected_grant", resolver)
    tasks = SimpleNamespace(
        protected_engage_get=lambda key: pytest.fail("awaited the engage task")
    )
    store = SimpleNamespace(get_ro_mount_by_thread=AsyncMock(return_value=_grant()))
    plan = await resolve_cloud_mount_plan(
        _protected_thread(),
        mount_rows=[{"id": "row-1"}],
        settings=SETTINGS,
        dependencies=_protected_deps(store, tasks),
        wait_for_grant=False,
    )
    assert plan.protected is True
    store.get_ro_mount_by_thread.return_value = _grant(status="engaging")
    assert (
        await resolve_cloud_mount_plan(
            _protected_thread(),
            mount_rows=[{"id": "row-1"}],
            settings=SETTINGS,
            dependencies=_protected_deps(store, tasks),
            wait_for_grant=False,
        )
        is None
    )
    resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_grant_is_awaited_before_the_lock(monkeypatch):
    finished: list[str] = []

    async def engage():
        await asyncio.sleep(0)
        finished.append("engage")

    task = asyncio.ensure_future(engage())
    keys: list = []
    tasks = SimpleNamespace(
        protected_engage_get=lambda key: (keys.append(key), task)[1]
    )
    monkeypatch.setattr(
        agent_cloud_mounts,
        "_resolve_protected_grant",
        AsyncMock(return_value=_grant(runtime_generation=EARLIER_GENERATION)),
    )
    await cloud_mount_plan.await_protected_grant(
        _protected_thread(), dependencies=_protected_deps(tasks=tasks)
    )
    assert finished == ["engage"] and keys == [(THREAD_ID, GENERATION)]
    # An unprotected thread waits for nothing.
    resolver = AsyncMock()
    monkeypatch.setattr(agent_cloud_mounts, "_resolve_protected_grant", resolver)
    await cloud_mount_plan.await_protected_grant(
        {**_protected_thread(), "metadata": {}}, dependencies=_protected_deps()
    )
    resolver.assert_not_awaited()
