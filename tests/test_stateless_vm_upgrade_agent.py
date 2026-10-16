"""The pinned agent's half of a stateless Session's VM upgrade (C2)."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import httpx
import pytest

import agent.api.persistent_app as pa
from agent.api import vm_upgrade_handoff as handoff
from agent.api.orchestrator_client import (
    OrchestratorClient,
    WorkspaceUpgradeRouteMissing,
    WorkspaceUpgradeRouteRefused,
)
from agent.api.workspace_upgrade_seed import cloud_synced_path
from agent.core.backends.virtual import VirtualWorkspaceBackend
from shared.runtime.core.backends.object_store import InMemoryObjectStore
from shared.thread_controls import ControlRequest
from tests._fs_backend import FilesystemTestBackend

THREAD_ID = UUID("11111111-1111-4111-8111-111111111111")
REQUEST_ID = UUID("22222222-2222-4222-8222-222222222222")
CLIENT_REQUEST_ID = UUID(int=1)
AGENT_ID = "33333333-3333-4333-8333-333333333333"
G0 = "55555555-5555-4555-8555-555555555555"
G1 = "66666666-6666-4666-8666-666666666666"
PROVISION = "77777777-7777-4777-8777-777777777777"
ATTACH = "88888888-8888-4888-8888-888888888888"
ENVELOPE = {
    "request_id": str(REQUEST_ID),
    "client_request_id": str(CLIENT_REQUEST_ID),
    "request_seq": 8,
    "method": "upgrade-to-workspace",
}
SOURCE = {"backend": "virtual", "prefix": f"threads/{THREAD_ID}/", "binding": None}


def _request(payload=None, *, generation=G0):
    return ControlRequest(
        id=REQUEST_ID,
        thread_id=THREAD_ID,
        request_seq=8,
        client_request_id=CLIENT_REQUEST_ID,
        verb="upgrade-to-workspace",
        payload=(
            {"runtime_generation": G0, "target_tier": "vm"}
            if payload is None
            else payload
        ),
        accepted_agent_id=UUID(AGENT_ID),
        runtime_generation=UUID(generation),
    )


# --- pure helpers -----------------------------------------------------------


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({"runtime_generation": G0, "target_tier": "vm"}, (G0, None)),
        ({"runtime_generation": G0, "template": "vm-big"}, (G0, "vm-big")),
        ({"runtime_generation": G0, "target_tier": "sandbox"}, None),
        ({"target_tier": "vm"}, None),
        ({"runtime_generation": G0, "focus": "x"}, None),
        ({"runtime_generation": G0, "template": ""}, None),
        ("not a dict", None),
    ],
)
def test_vm_upgrade_payload(payload, expected):
    assert handoff.vm_upgrade_payload(payload) == expected


@pytest.mark.parametrize(
    "tier,payload_generation,marker,recovered,in_flight,row",
    [
        ("virtual", G1, None, None, False, handoff.ROW_REQUEST),
        ("none", G1, None, None, False, handoff.ROW_REQUEST),
        ("virtual", G0, None, None, False, handoff.ROW_CANCEL),
        ("virtual", G1, None, None, True, handoff.ROW_BUSY),
        (
            "vm",
            G0,
            None,
            {"control_request_id": str(REQUEST_ID), "seeded_files": 2},
            False,
            handoff.ROW_COMPLETE,
        ),
        (
            "vm",
            G0,
            {"control_request_id": str(REQUEST_ID), "state": "seeded"},
            None,
            False,
            handoff.ROW_COMPLETE,
        ),
        (
            "vm",
            G0,
            {"control_request_id": str(UUID(int=9)), "state": "seeded"},
            None,
            False,
            handoff.ROW_INTERRUPTED,
        ),
        ("vm", G0, None, None, False, handoff.ROW_INTERRUPTED),
        ("sandbox", G1, None, None, False, handoff.ROW_REFUSE_TIER),
    ],
    ids=[
        "lite-same-life",
        "none-same-life",
        "ended-and-resumed",
        "already-running",
        "vm-recovered-here",
        "vm-seeded-before-loss",
        "vm-marker-for-another-request",
        "vm-without-marker",
        "unexpected-tier",
    ],
)
def test_each_row_of_the_consumer_table(
    tier, payload_generation, marker, recovered, in_flight, row
):
    assert (
        handoff.vm_upgrade_consumer_row(
            attached_tier=tier,
            payload_generation=payload_generation,
            current_generation=G1,
            marker=marker,
            request_id=str(REQUEST_ID),
            recovered=recovered,
            in_flight=in_flight,
        )
        == row
    )


def test_cloud_mount_targets_are_workspace_relative():
    session = SimpleNamespace(
        workspace_manager=SimpleNamespace(path="/home/agent-host/workspace"),
        workspace_sync=SimpleNamespace(
            mounts=[SimpleNamespace(target_path="projects/x")]
        ),
        cloud_mount_manager=SimpleNamespace(
            workspace_root="/home/agent-host/workspace",
            mounts=[SimpleNamespace(target_path="/home/agent-host/workspace/cloud")],
        ),
    )
    assert handoff.cloud_mount_targets(session) == ["projects/x", "cloud"]


def test_a_cloud_mount_outside_the_workspace_is_excluded_at_its_cloud_link():
    """A VM's rclone mounts live at ``/cloud/<name>``; the mount manager links
    them into the workspace under ``cloud`` (``_install_workspace_links``)."""
    session = SimpleNamespace(
        workspace_manager=SimpleNamespace(path="/w"),
        workspace_sync=None,
        cloud_mount_manager=SimpleNamespace(
            workspace_root="/w",
            mounts=[
                SimpleNamespace(target_path="/cloud/home"),
                SimpleNamespace(target_path="/cloud/project-a"),
            ],
        ),
    )
    assert handoff.cloud_mount_targets(session) == ["cloud"]


@pytest.mark.parametrize(
    "sync_target,mount_target",
    [("../elsewhere", "/w/cloud"), ("notes", "/w/../cloud"), ("/elsewhere", "/w")],
    ids=["sync-escapes", "mount-escapes", "sync-outside"],
)
def test_a_cloud_root_the_copy_cannot_place_fails_closed(sync_target, mount_target):
    session = SimpleNamespace(
        workspace_manager=SimpleNamespace(path="/w"),
        workspace_sync=SimpleNamespace(
            mounts=[SimpleNamespace(target_path=sync_target)]
        ),
        cloud_mount_manager=SimpleNamespace(
            workspace_root="/w", mounts=[SimpleNamespace(target_path=mount_target)]
        ),
    )
    with pytest.raises(handoff.VmUpgradeRecoveryError):
        handoff.cloud_mount_targets(session)


def _sidecar(*, planned=("/cloud/home",), live=(), skip_links=False):
    """``SidecarMountWatcher``: ``mounts`` lists only live folders, while its
    ``workspace/cloud`` link covers every planned one."""
    return SimpleNamespace(
        workspace_root="/w",
        planned=[SimpleNamespace(target_path=t) for t in planned],
        mounts=[SimpleNamespace(target_path=t) for t in live],
        skip_links=skip_links,
        cloud_cfg={"skip_workspace_links": skip_links},
    )


def _rclone(cloud_cfg, *, live=()):
    """``RcloneMountManager``: its plan is ``cloud_cfg["mounts"]``."""
    return SimpleNamespace(
        workspace_root="/w",
        cloud_cfg=cloud_cfg,
        mounts=[SimpleNamespace(target_path=t) for t in live],
    )


def _with_manager(manager):
    return SimpleNamespace(
        workspace_manager=SimpleNamespace(path="/w"),
        workspace_sync=None,
        cloud_mount_manager=manager,
    )


@pytest.mark.parametrize(
    "manager",
    [_sidecar(), _rclone({"mounts": [{"workspace_name": "home"}]})],
    ids=["sidecar-planned-not-live", "rclone-planned-not-live"],
)
def test_a_planned_mount_that_is_not_live_still_excludes_cloud(manager):
    """Review: with no live folder at owner start, ``workspace/cloud`` is
    still the link (a read-only view until the folder mounts)."""
    assert handoff.cloud_mount_targets(_with_manager(manager)) == ["cloud"]


@pytest.mark.parametrize(
    "manager,expected",
    [
        (_sidecar(skip_links=True), []),
        (
            _rclone(
                {"skip_workspace_links": True, "mounts": [{}]}, live=["/cloud/home"]
            ),
            [],
        ),
        (_rclone({"skip_workspace_links": True}, live=["/w/data"]), ["data"]),
        (_rclone({}), []),
    ],
    ids=[
        "sidecar-no-links",
        "rclone-no-links",
        "inside-the-workspace",
        "nothing-planned",
    ],
)
def test_without_its_link_an_outside_mount_is_not_in_the_workspace(manager, expected):
    assert handoff.cloud_mount_targets(_with_manager(manager)) == expected


def test_a_cloud_mount_that_failed_to_start_still_excludes_cloud():
    """No manager, but the attach asked for a cloud mount: a link an earlier
    attach installed may still point into the cloud."""
    session = _with_manager(None)
    assert handoff.cloud_mount_targets(session) == []
    session.cloud_mount_error = "rclone failed to start"
    assert handoff.cloud_mount_targets(session) == ["cloud"]


SYNC_IGNORED_CLOUD_FILES = [
    "cloud/plan.md",
    "cloud/projects/x.txt",
    "cloud/docs/requirements/r.md",
    "cloud/repo/.git/config",
    "cloud/home/archive/old.pdf",
]


class _ShellBackend(FilesystemTestBackend):
    supports_shell = True


@pytest.mark.asyncio
async def test_the_recovery_copy_never_writes_under_a_cloud_root(tmp_path, monkeypatch):
    """Review: ``workspace/cloud`` links into the live mount, so a path cloud
    sync ignores must not be copied there either. C1's claimant predicate
    keeps those paths (``cloud_synced_path``); the recovery uses a plain
    prefix."""
    store = InMemoryObjectStore()
    src = VirtualWorkspaceBackend(store, prefix=f"threads/{THREAD_ID}/")
    for path in ["notes/a.md", "cloud/b.md", *SYNC_IGNORED_CLOUD_FILES]:
        src.write_file(path, "old copy")
    monkeypatch.setattr(
        "agent.core.backends.factory.create_lite_backend", lambda *_a, **_k: src
    )
    (tmp_path / "ws").mkdir()
    dst = _ShellBackend(tmp_path / "ws")
    order: list = []
    session = _vm_session(tmp_path, _marker_metadata(), order=order)
    session.workspace_manager.backend = dst
    session.cloud_mount_manager = _sidecar()

    out = await handoff.recover_vm_upgrade_seed(
        session=session, client=_seed_client(order), thread_id=str(THREAD_ID)
    )

    assert out == {"control_request_id": str(REQUEST_ID), "seeded_files": 1}
    assert dst.read_file("notes/a.md") == "old copy"
    assert not dst.exists("cloud")
    assert order == ["source", "commit", "seeded"]
    # C1's predicate copies them: the first cloud pull won't bring them back.
    assert not any(cloud_synced_path(p, ["cloud"]) for p in SYNC_IGNORED_CLOUD_FILES)


@pytest.mark.asyncio
async def test_a_missing_route_is_retried_within_a_bounded_window(monkeypatch):
    monkeypatch.setattr(handoff.asyncio, "sleep", AsyncMock())
    flaky = AsyncMock(side_effect=[WorkspaceUpgradeRouteMissing("404"), "ok"])
    assert await handoff.call_with_route_retry(flaky) == "ok"
    missing = AsyncMock(side_effect=WorkspaceUpgradeRouteMissing("404"))
    with pytest.raises(WorkspaceUpgradeRouteMissing):
        await handoff.call_with_route_retry(missing, attempts=3)
    assert missing.await_count == 3


# --- the recovery copy ------------------------------------------------------


def _marker_metadata(state="pending", source=SOURCE, provision=PROVISION):
    vm = {
        "status": "ready",
        "upgrade_seed": {
            "control_request_id": str(REQUEST_ID),
            "state": state,
            "source": source,
        },
    }
    if provision is not None:
        vm["provision_generation"] = provision
    return {"vm": vm}


def _vm_session(tmp_path, metadata, *, mounts=(), order=None):
    def commit(_message):
        if order is not None:
            order.append("commit")
        return True

    git = MagicMock(is_active=True)
    git.commit = MagicMock(side_effect=commit)
    git.get_current_commit = MagicMock(return_value="b" * 40)
    return SimpleNamespace(
        workspace_manager=SimpleNamespace(
            backend=SimpleNamespace(supports_shell=True),
            git_manager=git,
            path=str(tmp_path),
        ),
        workspace_sync=None,
        cloud_mount_manager=SimpleNamespace(
            workspace_root=str(tmp_path),
            mounts=[SimpleNamespace(target_path=f"{tmp_path}/{m}") for m in mounts],
        ),
        postgres_conn=SimpleNamespace(
            get_thread=AsyncMock(return_value={"metadata": metadata}),
            record_turn_commit=AsyncMock(),
        ),
    )


def _seed_client(order, *, spec_overrides=None):
    spec = {
        "version": 1,
        "seed_from": SOURCE,
        "workspace": {"backend": "virtual", "mounts": []},
        "control_request_id": str(REQUEST_ID),
        "provision_generation": PROVISION,
        **(spec_overrides or {}),
    }
    return SimpleNamespace(
        get_workspace_upgrade_seed_source=AsyncMock(
            side_effect=lambda _tid: order.append("source") or spec
        ),
        mark_workspace_upgrade_seeded=AsyncMock(
            side_effect=lambda *_a, **_k: order.append("seeded")
        ),
        mark_workspace_upgrade_interrupted=AsyncMock(
            side_effect=lambda *_a, **_k: order.append("interrupted")
        ),
    )


@pytest.fixture
def copy(monkeypatch):
    """The shared copy step (C1's ``copy_lite_workspace_and_commit``) resolves
    these two names in its own module."""
    calls = []

    def fake_seed(workspace, *, owner_id, destination, exclude):
        calls.append(
            (
                workspace,
                owner_id,
                exclude("cloud/a.md") and exclude("cloud/plan.md"),
                exclude("notes/a.md"),
            )
        )
        return 4

    monkeypatch.setattr(
        "agent.api.workspace_upgrade_seed.seed_from_lite_workspace", fake_seed
    )
    monkeypatch.setattr("agent.api.workspace_upgrade_seed.unwrap_backend", lambda b: b)
    return calls


@pytest.mark.asyncio
async def test_recovery_copies_commits_records_then_marks_seeded(tmp_path, copy):
    order: list = []
    session = _vm_session(tmp_path, _marker_metadata(), mounts=["cloud"], order=order)
    client = _seed_client(order)

    out = await handoff.recover_vm_upgrade_seed(
        session=session, client=client, thread_id=str(THREAD_ID)
    )

    assert out == {"control_request_id": str(REQUEST_ID), "seeded_files": 4}
    assert order == ["source", "commit", "seeded"]
    [(workspace, owner_id, cloud_excluded, notes_excluded)] = copy
    assert workspace == {"backend": "virtual", "mounts": []}
    assert (owner_id, cloud_excluded, notes_excluded) == (str(THREAD_ID), True, False)
    session.postgres_conn.record_turn_commit.assert_awaited_once_with(
        str(THREAD_ID), "b" * 40
    )
    assert client.mark_workspace_upgrade_seeded.await_args.kwargs == {
        "provision_generation": PROVISION,
        "control_request_id": str(REQUEST_ID),
    }


@pytest.mark.asyncio
async def test_a_none_source_marks_seeded_without_a_source_call(tmp_path, copy):
    order: list = []
    session = _vm_session(tmp_path, _marker_metadata(source=None))
    client = _seed_client(order)
    out = await handoff.recover_vm_upgrade_seed(
        session=session, client=client, thread_id=str(THREAD_ID)
    )
    assert out == {"control_request_id": str(REQUEST_ID), "seeded_files": 0}
    assert order == ["seeded"] and copy == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata", [_marker_metadata(state="seeded"), {"vm": {"status": "ready"}}, {}]
)
async def test_nothing_to_recover_without_a_pending_marker(tmp_path, copy, metadata):
    order: list = []
    out = await handoff.recover_vm_upgrade_seed(
        session=_vm_session(tmp_path, metadata),
        client=_seed_client(order),
        thread_id=str(THREAD_ID),
    )
    assert out is None and order == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata,spec_overrides",
    [
        (_marker_metadata(), {"provision_generation": G0}),
        (_marker_metadata(), {"control_request_id": str(UUID(int=9))}),
        (_marker_metadata(), {"version": 2}),
        (_marker_metadata(), {"workspace": None}),
        (_marker_metadata(provision=None), None),
    ],
    ids=[
        "stale-provision",
        "other-request",
        "unknown-version",
        "no-workspace",
        "no-provision",
    ],
)
async def test_a_mismatched_recovery_is_refused_and_keeps_the_marker(
    tmp_path, copy, metadata, spec_overrides
):
    order: list = []
    with pytest.raises(handoff.VmUpgradeRecoveryError):
        await handoff.recover_vm_upgrade_seed(
            session=_vm_session(tmp_path, metadata),
            client=_seed_client(order, spec_overrides=spec_overrides),
            thread_id=str(THREAD_ID),
        )
    assert "seeded" not in order and copy == []


@pytest.mark.asyncio
async def test_a_failed_commit_keeps_the_marker(tmp_path, copy):
    order: list = []
    session = _vm_session(tmp_path, _marker_metadata())
    session.workspace_manager.git_manager.commit = MagicMock(return_value=False)
    with pytest.raises(handoff.VmUpgradeRecoveryError):
        await handoff.recover_vm_upgrade_seed(
            session=session, client=_seed_client(order), thread_id=str(THREAD_ID)
        )
    assert "seeded" not in order
    session.postgres_conn.record_turn_commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_lite_attach_never_marks_a_pending_copy_seeded(tmp_path, copy):
    order: list = []
    session = _vm_session(tmp_path, _marker_metadata(source=None))
    session.workspace_manager.backend.supports_shell = False
    with pytest.raises(handoff.VmUpgradeRecoveryError):
        await handoff.recover_vm_upgrade_seed(
            session=session, client=_seed_client(order), thread_id=str(THREAD_ID)
        )
    assert order == []


SOURCE_CHANGED = WorkspaceUpgradeRouteRefused(
    409, "upgrade_seed_source_changed", "workspace upgrade seed route answered 409"
)


@pytest.mark.asyncio
async def test_a_changed_seed_source_ends_the_copy_as_interrupted(tmp_path, copy):
    """Review: a permanent refusal must not abort every attach. The marker
    leaves ``pending`` (no later attach retries) and the request ends as
    ``upgrade_interrupted``; the files stay in the old storage."""
    order: list = []
    client = _seed_client(order)
    client.get_workspace_upgrade_seed_source = AsyncMock(side_effect=SOURCE_CHANGED)

    out = await handoff.recover_vm_upgrade_seed(
        session=_vm_session(tmp_path, _marker_metadata()),
        client=client,
        thread_id=str(THREAD_ID),
    )

    assert out == {"control_request_id": str(REQUEST_ID), "interrupted": True}
    assert order == ["interrupted"] and copy == []
    client.mark_workspace_upgrade_interrupted.assert_awaited_once_with(
        str(THREAD_ID),
        provision_generation=PROVISION,
        control_request_id=str(REQUEST_ID),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        WorkspaceUpgradeRouteRefused(409, "no_pending_upgrade_seed", "race"),
        WorkspaceUpgradeRouteRefused(409, "pinned_runtime_identity_mismatch", "race"),
        WorkspaceUpgradeRouteRefused(503, None, "unavailable"),
        WorkspaceUpgradeRouteMissing("404"),
        httpx.ConnectError("refused"),
    ],
    ids=["not-pending", "identity-race", "5xx", "old-replica", "unreachable"],
)
async def test_a_transient_seed_route_failure_still_aborts_the_attach(
    tmp_path, copy, monkeypatch, error
):
    monkeypatch.setattr(handoff.asyncio, "sleep", AsyncMock())
    order: list = []
    client = _seed_client(order)
    client.get_workspace_upgrade_seed_source = AsyncMock(side_effect=error)
    with pytest.raises(type(error)):
        await handoff.recover_vm_upgrade_seed(
            session=_vm_session(tmp_path, _marker_metadata()),
            client=client,
            thread_id=str(THREAD_ID),
        )
    assert order == [] and copy == []


@pytest.mark.asyncio
async def test_an_unrecorded_interruption_aborts_the_attach(tmp_path, copy):
    """If the marker can't leave ``pending``, the next attach must try again."""
    order: list = []
    client = _seed_client(order)
    client.get_workspace_upgrade_seed_source = AsyncMock(side_effect=SOURCE_CHANGED)
    client.mark_workspace_upgrade_interrupted = AsyncMock(
        side_effect=WorkspaceUpgradeRouteRefused(409, "upgrade_seed_fence_lost", "x")
    )
    with pytest.raises(WorkspaceUpgradeRouteRefused):
        await handoff.recover_vm_upgrade_seed(
            session=_vm_session(tmp_path, _marker_metadata()),
            client=client,
            thread_id=str(THREAD_ID),
        )
    assert order == [] and copy == []


@pytest.mark.asyncio
async def test_an_interrupted_marker_is_not_owed_a_copy(tmp_path, copy):
    order: list = []
    out = await handoff.recover_vm_upgrade_seed(
        session=_vm_session(tmp_path, _marker_metadata(state="interrupted")),
        client=_seed_client(order),
        thread_id=str(THREAD_ID),
    )
    assert out is None and order == []


# --- the drain on the pinned owner -----------------------------------------


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *exc):
        return False


@pytest.fixture
def pinned(monkeypatch):
    pool = SimpleNamespace(
        acquire=lambda: _Acquire(MagicMock()),
        get_thread=AsyncMock(
            return_value={
                "metadata": {
                    "vm": {"status": "provisioning", "provision_generation": PROVISION}
                }
            }
        ),
    )
    session = SimpleNamespace(
        postgres_conn=pool,
        workspace_backend_tier="virtual",
        workspace_manager=SimpleNamespace(
            backend=SimpleNamespace(supports_shell=False)
        ),
    )
    client = SimpleNamespace(
        request_vm_upgrade_for_control=AsyncMock(return_value=(True, 200, None))
    )
    monkeypatch.setattr(pa, "_session", session)
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa._session_identity, "_session_generation", G0)
    monkeypatch.setattr(pa._session_identity, "_attach_token", ATTACH)
    monkeypatch.setattr(pa, "_workspace_upgrade_in_flight", False)
    monkeypatch.setattr(pa, "_recovered_vm_upgrade", None)
    monkeypatch.setattr(pa, "_interrupted_vm_upgrade", None)
    return SimpleNamespace(session=session, client=client, pool=pool)


async def _drain(request, *, receipt=None):
    order: list = []

    async def journal(kind, params, **_kwargs):
        order.append(("journal", kind, params))
        return (6, 44)

    async def finalize(_request, **kwargs):
        order.append(("finalize", kwargs["outcome"], kwargs.get("error_code")))
        return kwargs["outcome"]

    with (
        patch(
            "shared.thread_controls.owner_fence_current", AsyncMock(return_value=True)
        ),
        patch(
            "shared.thread_controls.adopt_next_pinned_control_request",
            AsyncMock(return_value=False),
        ),
        patch(
            "shared.thread_controls.fetch_next_control_request",
            AsyncMock(side_effect=[request, None]),
        ),
        patch(
            "shared.thread_controls.fetch_control_receipt",
            AsyncMock(return_value=receipt),
        ),
        patch.object(pa, "_broadcast_durable", journal),
        patch.object(pa, "_finalize_durable_control", finalize),
        patch.object(
            pa,
            "_start_durable_vm_upgrade",
            MagicMock(
                side_effect=lambda _r, **kw: order.append(
                    ("start", kw["provision_generation"])
                )
            ),
        ),
    ):
        await pa._drain_thread_controls(agent_id=AGENT_ID)
    return order


@pytest.mark.asyncio
async def test_a_lite_attach_requests_the_vm_then_receipts_started_then_starts_the_swap(
    pinned,
):
    order = await _drain(_request())
    assert order == [
        (
            "journal",
            "workspace_upgrade.started",
            {**ENVELOPE, "target_tier": "vm", "provision_generation": PROVISION},
        ),
        ("finalize", "applied", None),
        ("start", PROVISION),
    ]
    pinned.client.request_vm_upgrade_for_control.assert_awaited_once_with(
        str(THREAD_ID), template=None, control_request_id=str(REQUEST_ID)
    )


@pytest.mark.asyncio
async def test_a_template_request_names_its_template(pinned):
    await _drain(_request({"runtime_generation": G0, "template": "vm-big"}))
    pinned.client.request_vm_upgrade_for_control.assert_awaited_once_with(
        str(THREAD_ID), template="vm-big", control_request_id=str(REQUEST_ID)
    )


@pytest.mark.asyncio
async def test_a_request_from_an_ended_life_is_cancelled_without_a_vm_request(
    pinned, monkeypatch
):
    """Review Focus 2 (D3): 0185 re-stamped the row; the payload kept G0."""
    monkeypatch.setattr(pa._session_identity, "_session_generation", G1)
    order = await _drain(_request(generation=G1))
    assert order == [
        (
            "journal",
            "workspace_upgrade.failed",
            {
                **ENVELOPE,
                "target_tier": "vm",
                "reason": "The VM upgrade was cancelled when this Session ended.",
                "error_code": "upgrade_cancelled",
            },
        ),
        ("finalize", "rejected", "upgrade_cancelled"),
    ]
    pinned.client.request_vm_upgrade_for_control.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_second_upgrade_while_one_runs_is_a_typed_rejection(
    pinned, monkeypatch
):
    monkeypatch.setattr(pa, "_workspace_upgrade_in_flight", True)
    order = await _drain(_request())
    assert order == [
        (
            "journal",
            "workspace_upgrade.failed",
            {
                **ENVELOPE,
                "target_tier": "vm",
                "reason": "A workspace upgrade is already running.",
                "error_code": "upgrade_already_running",
            },
        ),
        ("finalize", "rejected", "upgrade_already_running"),
    ]
    pinned.client.request_vm_upgrade_for_control.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_refused_vm_request_is_a_typed_rejection(pinned):
    pinned.client.request_vm_upgrade_for_control.return_value = (
        False,
        409,
        "VM workspaces aren't available on this installation.",
    )
    order = await _drain(_request())
    assert order[0][1] == "workspace_upgrade.failed"
    assert order[0][2]["error_code"] == "upgrade_refused"
    assert (
        order[0][2]["reason"] == "VM workspaces aren't available on this installation."
    )
    assert order[1] == ("finalize", "rejected", "upgrade_refused")
    assert len(order) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,detail",
    [
        (503, "VM provisioning not available (no NATS or K8s)"),
        (403, "vm_workspace grant denied"),
    ],
    ids=["typed-503", "403"],
)
async def test_the_typed_503_and_a_4xx_are_typed_rejections(pinned, status, detail):
    pinned.client.request_vm_upgrade_for_control.return_value = (False, status, detail)
    order = await _drain(_request())
    assert order == [
        (
            "journal",
            "workspace_upgrade.failed",
            {
                **ENVELOPE,
                "target_tier": "vm",
                "reason": detail,
                "error_code": "upgrade_refused",
            },
        ),
        ("finalize", "rejected", "upgrade_refused"),
    ]


@pytest.mark.asyncio
async def test_a_vm_request_that_never_reached_the_orchestrator_stays_pending(pinned):
    pinned.client.request_vm_upgrade_for_control.return_value = (None, None, None)
    with pytest.raises(pa.ControlInboxBlocked):
        await _drain(_request())


def _committed_metadata(*, request_id=REQUEST_ID, provision=PROVISION):
    """``metadata.vm`` once the request's pre-CAS merge (and maybe the CAS) ran."""
    vm = {
        "upgrade_seed": {
            "control_request_id": str(request_id),
            "state": "pending",
            "source": SOURCE,
        }
    }
    if provision is not None:
        vm.update(status="provisioning", provision_generation=provision)
    return {"vm": vm}


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [500, 502, 504, None], ids=str)
async def test_a_5xx_after_the_provision_cas_is_accepted_and_starts_the_swap(
    pinned, status
):
    """Final review Important 1: the CAS committed before the error, so the VM
    boots for this request; the marker naming it is the proof."""
    pinned.client.request_vm_upgrade_for_control.return_value = (None, status, None)
    pinned.pool.get_thread.return_value = {"metadata": _committed_metadata()}
    order = await _drain(_request())
    assert order == [
        (
            "journal",
            "workspace_upgrade.started",
            {**ENVELOPE, "target_tier": "vm", "provision_generation": PROVISION},
        ),
        ("finalize", "applied", None),
        ("start", PROVISION),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"vm": {"status": "provisioning", "provision_generation": PROVISION}},
        _committed_metadata(provision=None),
        _committed_metadata(request_id=UUID(int=9)),
    ],
    ids=["nothing-recorded", "no-marker", "marker-before-the-cas", "another-request"],
)
async def test_a_5xx_before_the_provision_cas_leaves_the_request_pending(
    pinned, metadata
):
    pinned.client.request_vm_upgrade_for_control.return_value = (None, 502, None)
    pinned.pool.get_thread.return_value = {"metadata": metadata}
    with pytest.raises(pa.ControlInboxBlocked):
        await _drain(_request())


@pytest.mark.asyncio
async def test_an_invalid_pinned_payload_is_not_served(pinned):
    order = await _drain(_request({"runtime_generation": G0, "target_tier": "sandbox"}))
    assert order[0][1] == "control.rejected"
    assert order[0][2]["error_code"] == "unsupported_control"
    pinned.client.request_vm_upgrade_for_control.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_vm_attach_completes_the_request_its_recovery_copied(
    pinned, monkeypatch
):
    pinned.session.workspace_backend_tier = "vm"
    monkeypatch.setattr(
        pa,
        "_recovered_vm_upgrade",
        {"control_request_id": str(REQUEST_ID), "seeded_files": 2},
    )
    order = await _drain(_request())
    assert order == [
        (
            "journal",
            "workspace_upgrade.complete",
            {**ENVELOPE, "target_tier": "vm", "seeded_files": 2},
        ),
        ("finalize", "applied", None),
    ]
    assert pa._recovered_vm_upgrade is None


@pytest.mark.asyncio
async def test_a_vm_attach_without_a_marker_reports_the_interruption(pinned):
    pinned.session.workspace_backend_tier = "vm"
    order = await _drain(_request())
    assert order[0][2]["error_code"] == "upgrade_interrupted"
    assert order[0][2]["reason"] == (
        "The VM upgrade was interrupted before your files were copied. "
        "End this Session and start a new one."
    )
    assert order[1] == ("finalize", "rejected", "upgrade_interrupted")


@pytest.mark.asyncio
async def test_a_vm_attach_receipts_the_interruption_its_recovery_recorded(
    pinned, monkeypatch
):
    pinned.session.workspace_backend_tier = "vm"
    pinned.pool.get_thread.return_value = {
        "metadata": _marker_metadata(state="interrupted")
    }
    monkeypatch.setattr(pa, "_interrupted_vm_upgrade", str(REQUEST_ID))
    order = await _drain(_request())
    assert order == [
        (
            "journal",
            "workspace_upgrade.failed",
            {
                **ENVELOPE,
                "target_tier": "vm",
                "reason": (
                    "The VM upgrade was interrupted before your files were "
                    "copied. End this Session and start a new one."
                ),
                "error_code": "upgrade_interrupted",
            },
        ),
        ("finalize", "rejected", "upgrade_interrupted"),
    ]
    assert pa._interrupted_vm_upgrade is None
    pinned.client.request_vm_upgrade_for_control.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_recovered_started_receipt_restarts_the_swap(pinned):
    receipt = SimpleNamespace(
        kind="workspace_upgrade.started",
        payload={**ENVELOPE, "target_tier": "vm", "provision_generation": PROVISION},
    )
    order = await _drain(_request(), receipt=receipt)
    assert order == [("finalize", "applied", None), ("start", PROVISION)]


# --- owner start, the emitter and the durable task --------------------------


@pytest.mark.asyncio
async def test_recovery_copy_runs_before_the_first_drain(monkeypatch):
    """Review Focus 3: the copy lands before any control, input or /ready."""
    order: list = []
    monkeypatch.setattr(pa, "_session", SimpleNamespace(postgres_conn=MagicMock()))
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa, "_events_epoch", 0)
    monkeypatch.setattr(pa, "_next_seq", 0)
    monkeypatch.setattr(pa, "_event_writer", None)
    monkeypatch.setattr(
        pa, "_resolve_event_journal_epoch", AsyncMock(return_value=(3, 10))
    )
    monkeypatch.setattr(pa, "_OrderedPersistentEventWriter", MagicMock())
    monkeypatch.setattr(pa, "_registered_pinned_agent_id", lambda: AGENT_ID)
    monkeypatch.setattr(
        pa,
        "_recover_pinned_vm_upgrade_seed",
        AsyncMock(side_effect=lambda: order.append("recover")),
    )
    monkeypatch.setattr(
        pa,
        "_start_thread_control_watcher",
        AsyncMock(side_effect=lambda **_k: order.append("drain")),
    )
    monkeypatch.setattr(
        pa,
        "_announce_recovered_vm_upgrade",
        MagicMock(side_effect=lambda: order.append("announce")),
    )
    await pa._open_event_journal()
    assert order == ["recover", "drain", "announce"]


@pytest.mark.asyncio
async def test_a_failed_recovery_fails_the_attach_before_any_drain(monkeypatch):
    drained = AsyncMock()
    monkeypatch.setattr(pa, "_session", SimpleNamespace(postgres_conn=MagicMock()))
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa, "_events_epoch", 0)
    monkeypatch.setattr(pa, "_next_seq", 0)
    monkeypatch.setattr(pa, "_event_writer", None)
    monkeypatch.setattr(
        pa, "_resolve_event_journal_epoch", AsyncMock(return_value=(3, 10))
    )
    monkeypatch.setattr(pa, "_OrderedPersistentEventWriter", MagicMock())
    monkeypatch.setattr(pa, "_registered_pinned_agent_id", lambda: AGENT_ID)
    monkeypatch.setattr(
        pa,
        "_recover_pinned_vm_upgrade_seed",
        AsyncMock(side_effect=handoff.VmUpgradeRecoveryError("no copy")),
    )
    monkeypatch.setattr(pa, "_start_thread_control_watcher", drained)
    with pytest.raises(handoff.VmUpgradeRecoveryError):
        await pa._open_event_journal()
    drained.assert_not_awaited()


@pytest.mark.asyncio
async def test_owner_start_recovers_only_on_a_vm_attach(monkeypatch):
    recover = AsyncMock(
        return_value={"control_request_id": str(REQUEST_ID), "seeded_files": 1}
    )
    session = SimpleNamespace(
        postgres_conn=MagicMock(), workspace_backend_tier="virtual"
    )
    monkeypatch.setattr(handoff, "recover_vm_upgrade_seed", recover)
    monkeypatch.setattr(pa, "_session", session)
    monkeypatch.setattr(pa, "_orchestrator_client", SimpleNamespace())
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa, "_recovered_vm_upgrade", {"stale": True})

    await pa._recover_pinned_vm_upgrade_seed()
    assert pa._recovered_vm_upgrade is None
    recover.assert_not_awaited()

    session.workspace_backend_tier = "vm"
    await pa._recover_pinned_vm_upgrade_seed()
    assert pa._recovered_vm_upgrade == {
        "control_request_id": str(REQUEST_ID),
        "seeded_files": 1,
    }
    assert recover.await_args.kwargs["thread_id"] == str(THREAD_ID)


@pytest.mark.asyncio
async def test_owner_start_holds_an_interrupted_copy_for_the_drain(monkeypatch):
    recover = AsyncMock(
        return_value={"control_request_id": str(REQUEST_ID), "interrupted": True}
    )
    monkeypatch.setattr(handoff, "recover_vm_upgrade_seed", recover)
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(postgres_conn=MagicMock(), workspace_backend_tier="vm"),
    )
    monkeypatch.setattr(pa, "_orchestrator_client", SimpleNamespace())
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa, "_recovered_vm_upgrade", None)
    monkeypatch.setattr(pa, "_interrupted_vm_upgrade", "stale")

    await pa._recover_pinned_vm_upgrade_seed()

    assert pa._recovered_vm_upgrade is None
    assert pa._interrupted_vm_upgrade == str(REQUEST_ID)


def test_an_interruption_the_drain_did_not_receipt_is_announced_once(monkeypatch):
    """The request was receipted ``.started`` before the predecessor was lost:
    no request is left to reject, so every tab is told once."""
    sent: list = []
    monkeypatch.setattr(
        pa, "_broadcast", lambda method, params: sent.append((method, params))
    )
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa, "_recovered_vm_upgrade", None)
    monkeypatch.setattr(pa, "_interrupted_vm_upgrade", str(REQUEST_ID))
    pa._announce_recovered_vm_upgrade()
    pa._announce_recovered_vm_upgrade()
    assert sent == [
        (
            "workspace_upgrade.failed",
            {
                "thread_id": str(THREAD_ID),
                "target_tier": "vm",
                "reason": (
                    "The VM upgrade was interrupted before your files were "
                    "copied. End this Session and start a new one."
                ),
                "error_code": "upgrade_interrupted",
                "request_id": str(REQUEST_ID),
            },
        )
    ]


def test_a_recovery_the_drain_did_not_receipt_is_announced_once(monkeypatch):
    sent: list = []
    monkeypatch.setattr(
        pa, "_broadcast", lambda method, params: sent.append((method, params))
    )
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(
        pa,
        "_recovered_vm_upgrade",
        {"control_request_id": str(REQUEST_ID), "seeded_files": 5},
    )
    monkeypatch.setattr(pa, "_interrupted_vm_upgrade", None)
    pa._announce_recovered_vm_upgrade()
    pa._announce_recovered_vm_upgrade()
    assert sent == [
        (
            "workspace_upgrade.complete",
            {
                "thread_id": str(THREAD_ID),
                "target_tier": "vm",
                "seeded_files": 5,
                "request_id": str(REQUEST_ID),
            },
        )
    ]


@pytest.mark.asyncio
async def test_the_durable_emitter_journals_with_the_envelope_and_a_typed_failure(
    monkeypatch,
):
    sent: list = []
    monkeypatch.setattr(
        pa, "_broadcast", lambda method, params: sent.append((method, params))
    )
    emit = pa._journaled_upgrade_emitter({"request_id": "r", "client_request_id": "c"})
    await emit("workspace_upgrade.progress", {"elapsed_s": 60})
    await emit(
        "workspace_upgrade.failed",
        {"reason": "vm workspace did not become ready in time"},
    )
    assert sent == [
        (
            "workspace_upgrade.progress",
            {"elapsed_s": 60, "request_id": "r", "client_request_id": "c"},
        ),
        (
            "workspace_upgrade.failed",
            {
                "reason": "vm workspace did not become ready in time",
                "error_code": "upgrade_failed",
                "request_id": "r",
                "client_request_id": "c",
            },
        ),
    ]


@pytest.fixture
def durable(monkeypatch):
    client = SimpleNamespace(mark_workspace_upgrade_seeded=AsyncMock())
    tasks: list = []
    sent: list = []
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(workspace_manager=SimpleNamespace(backend="lite")),
    )
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(THREAD_ID))
    monkeypatch.setattr(pa, "_workspace_upgrade_in_flight", False)
    monkeypatch.setattr(
        pa._session_termination,
        "track_session_side_task",
        lambda task: tasks.append(task) or task,
    )
    monkeypatch.setattr(
        pa, "_broadcast", lambda method, params: sent.append((method, params))
    )
    return SimpleNamespace(client=client, tasks=tasks, sent=sent)


@pytest.mark.asyncio
async def test_the_durable_swap_marks_the_seed_with_the_receipted_provision(
    monkeypatch, durable
):
    seen: dict = {}

    async def core(emit, *, target_tier, src_backend, before_swap):
        seen["target_tier"] = target_tier
        seen["src_backend"] = src_backend
        seen["in_flight"] = pa._workspace_upgrade_in_flight
        await before_swap(3)

    monkeypatch.setattr(pa, "_run_workspace_upgrade_swap", core)

    pa._start_durable_vm_upgrade(_request(), provision_generation=PROVISION)
    await durable.tasks[0]

    assert seen == {"target_tier": "vm", "src_backend": "lite", "in_flight": True}
    durable.client.mark_workspace_upgrade_seeded.assert_awaited_once_with(
        str(THREAD_ID),
        provision_generation=PROVISION,
        control_request_id=str(REQUEST_ID),
    )
    assert pa._workspace_upgrade_in_flight is False


@pytest.mark.asyncio
async def test_a_durable_swap_cancelled_before_it_starts_releases_the_single_flight(
    monkeypatch, durable
):
    """Task 7 review: a task cancelled before its first step never runs its
    ``finally``; the flag must not stay set for the process lifetime."""
    core = AsyncMock()
    monkeypatch.setattr(pa, "_run_workspace_upgrade_swap", core)

    pa._start_durable_vm_upgrade(_request(), provision_generation=PROVISION)
    assert pa._workspace_upgrade_in_flight is True
    durable.tasks[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        await durable.tasks[0]
    await asyncio.sleep(0)

    core.assert_not_awaited()
    assert pa._workspace_upgrade_in_flight is False


@pytest.mark.asyncio
async def test_a_durable_swap_cancelled_while_running_releases_the_single_flight(
    monkeypatch, durable
):
    entered = asyncio.Event()

    async def core(emit, *, target_tier, src_backend, before_swap):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(pa, "_run_workspace_upgrade_swap", core)

    pa._start_durable_vm_upgrade(_request(), provision_generation=PROVISION)
    await entered.wait()
    durable.tasks[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        await durable.tasks[0]
    await asyncio.sleep(0)

    assert pa._workspace_upgrade_in_flight is False


@pytest.mark.asyncio
async def test_a_durable_swap_never_starts_twice(monkeypatch, durable):
    core = AsyncMock()
    monkeypatch.setattr(pa, "_run_workspace_upgrade_swap", core)
    monkeypatch.setattr(pa, "_workspace_upgrade_in_flight", True)

    pa._start_durable_vm_upgrade(_request(), provision_generation=PROVISION)

    assert durable.tasks == []
    assert pa._workspace_upgrade_in_flight is True


@pytest.mark.asyncio
async def test_a_durable_start_while_another_upgrade_runs_is_a_typed_failure(
    monkeypatch, durable, caplog
):
    """Final review Important 2 (b): the request was receipted ``.started``,
    so a start the single flight blocks must say so to every tab and log."""
    monkeypatch.setattr(pa, "_run_workspace_upgrade_swap", AsyncMock())
    monkeypatch.setattr(pa, "_workspace_upgrade_in_flight", True)

    with caplog.at_level("WARNING", logger=pa.logger.name):
        pa._start_durable_vm_upgrade(_request(), provision_generation=PROVISION)

    assert durable.sent == [
        (
            "workspace_upgrade.failed",
            {
                "thread_id": str(THREAD_ID),
                "target_tier": "vm",
                "reason": "A workspace upgrade is already running.",
                "error_code": "upgrade_already_running",
                "request_id": str(REQUEST_ID),
                "client_request_id": str(CLIENT_REQUEST_ID),
            },
        )
    ]
    assert str(REQUEST_ID) in caplog.text
    assert durable.tasks == []


def test_a_recovered_started_receipt_under_a_held_flight_says_so(monkeypatch, durable):
    monkeypatch.setattr(pa, "_workspace_upgrade_in_flight", True)
    pa._session.workspace_backend_tier = "virtual"

    pa._resume_vm_upgrade_from_receipt(
        _request(), {**ENVELOPE, "provision_generation": PROVISION}
    )

    assert [(method, params["error_code"]) for method, params in durable.sent] == [
        ("workspace_upgrade.failed", "upgrade_already_running")
    ]
    assert durable.tasks == []


@pytest.mark.asyncio
async def test_a_durable_start_without_a_session_is_a_typed_failure(
    monkeypatch, durable, caplog
):
    monkeypatch.setattr(pa, "_run_workspace_upgrade_swap", AsyncMock())
    monkeypatch.setattr(pa, "_session", None)

    with caplog.at_level("WARNING", logger=pa.logger.name):
        pa._start_durable_vm_upgrade(_request(), provision_generation=PROVISION)

    assert [(method, params["error_code"]) for method, params in durable.sent] == [
        ("workspace_upgrade.failed", "upgrade_failed")
    ]
    assert durable.sent[0][1]["request_id"] == str(REQUEST_ID)
    assert str(REQUEST_ID) in caplog.text
    assert durable.tasks == []
    assert pa._workspace_upgrade_in_flight is False


# --- the client -------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_client_sends_the_request_id_and_the_pinned_identity():
    seen: list = []

    def handler(request):
        seen.append(
            (request.method, request.url.path, request.headers, request.content)
        )
        if request.url.path.endswith("/upgrade-to-workspace"):
            return httpx.Response(200, json={"status": "provisioning"})
        if request.url.path.endswith("/seed-source"):
            return httpx.Response(200, json={"version": 1})
        if request.url.path.endswith("/interrupted"):
            return httpx.Response(200, json={"state": "interrupted"})
        return httpx.Response(200, json={"state": "seeded"})

    client = OrchestratorClient("http://test", "127.0.0.1", 8000, "worker", "default")
    client.agent_id = AGENT_ID
    assert client.adopt_session_runtime_identity(G0, ATTACH)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
        client._client = transport
        assert await client.request_vm_upgrade_for_control(
            str(THREAD_ID), template="vm-big", control_request_id=str(REQUEST_ID)
        ) == (True, 200, None)
        assert await client.get_workspace_upgrade_seed_source(str(THREAD_ID)) == {
            "version": 1
        }
        await client.mark_workspace_upgrade_seeded(
            str(THREAD_ID),
            provision_generation=PROVISION,
            control_request_id=str(REQUEST_ID),
        )
        await client.mark_workspace_upgrade_interrupted(
            str(THREAD_ID),
            provision_generation=PROVISION,
            control_request_id=str(REQUEST_ID),
        )

    upgrade, source, seeded, interrupted = seen
    assert upgrade[:2] == (
        "POST",
        f"/api/agents/threads/{THREAD_ID}/upgrade-to-workspace",
    )
    assert source[:2] == (
        "GET",
        f"/api/agents/threads/{THREAD_ID}/workspace-upgrade/seed-source",
    )
    assert seeded[:2] == (
        "POST",
        f"/api/agents/threads/{THREAD_ID}/workspace-upgrade/seeded",
    )
    assert json.loads(upgrade[3]) == {
        "target_tier": "vm",
        "template": "vm-big",
        "control_request_id": str(REQUEST_ID),
    }
    assert interrupted[:2] == (
        "POST",
        f"/api/agents/threads/{THREAD_ID}/workspace-upgrade/interrupted",
    )
    for _method, _path, headers, _body in (source, seeded, interrupted):
        assert headers["X-Agent-ID"] == AGENT_ID
        assert headers["X-Session-Runtime-Generation"] == G0
        assert headers["X-Session-Runtime-Attach-Token"] == ATTACH
    for marked in (seeded, interrupted):
        assert json.loads(marked[3]) == {
            "provision_generation": PROVISION,
            "control_request_id": str(REQUEST_ID),
        }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,body,raised,code",
    [
        (404, {"detail": "Not Found"}, WorkspaceUpgradeRouteMissing, None),
        (404, {"detail": "Thread not found"}, WorkspaceUpgradeRouteRefused, None),
        (
            409,
            {"detail": {"code": "upgrade_seed_source_changed"}},
            WorkspaceUpgradeRouteRefused,
            "upgrade_seed_source_changed",
        ),
        (502, ["not", "an", "object"], WorkspaceUpgradeRouteRefused, None),
    ],
)
async def test_an_old_replica_is_told_apart_from_a_refusal(status, body, raised, code):
    client = OrchestratorClient("http://test", "127.0.0.1", 8000, "worker", "default")
    transport = httpx.MockTransport(lambda request: httpx.Response(status, json=body))
    async with httpx.AsyncClient(transport=transport) as http:
        client._client = http
        calls = [
            lambda: client.get_workspace_upgrade_seed_source(str(THREAD_ID)),
            lambda: client.mark_workspace_upgrade_seeded(
                str(THREAD_ID),
                provision_generation=PROVISION,
                control_request_id=str(REQUEST_ID),
            ),
            lambda: client.mark_workspace_upgrade_interrupted(
                str(THREAD_ID),
                provision_generation=PROVISION,
                control_request_id=str(REQUEST_ID),
            ),
        ]
        for call in calls:
            with pytest.raises(RuntimeError) as caught:
                await call()
            assert type(caught.value) is raised
            if raised is WorkspaceUpgradeRouteRefused:
                assert (caught.value.status_code, caught.value.code) == (status, code)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response,expected",
    [
        (
            httpx.Response(
                409,
                json={
                    "detail": {
                        "code": "protected_cloud_workspace_fixed",
                        "message": "Protected.",
                    }
                },
            ),
            (False, 409, "Protected."),
        ),
        (
            httpx.Response(
                503, json={"detail": "VM provisioning not available (no NATS or K8s)"}
            ),
            (False, 503, "VM provisioning not available (no NATS or K8s)"),
        ),
        (
            httpx.Response(403, json={"detail": "vm_workspace grant denied"}),
            (False, 403, "vm_workspace grant denied"),
        ),
    ],
    ids=["typed-409", "typed-503-provisioning-off", "403"],
)
async def test_a_4xx_or_the_typed_503_is_a_refusal_with_its_text(response, expected):
    client = OrchestratorClient("http://test", "127.0.0.1", 8000, "worker", "default")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _r: response)
    ) as http:
        client._client = http
        assert (
            await client.request_vm_upgrade_for_control(
                str(THREAD_ID), template=None, control_request_id=str(REQUEST_ID)
            )
            == expected
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response,expected",
    [
        (httpx.Response(500, text="boom"), (None, 500, None)),
        (httpx.Response(502, json=["not", "an", "object"]), (None, 502, None)),
        (httpx.Response(504, text="gateway timeout"), (None, 504, None)),
        (
            httpx.Response(503, json={"detail": "Service Unavailable"}),
            (None, 503, "Service Unavailable"),
        ),
    ],
    ids=["500", "502", "504", "untyped-503"],
)
async def test_a_5xx_is_an_unknown_outcome_not_a_refusal(response, expected):
    """Final review Important 1: a 5xx can arrive after the provision CAS
    committed, so it is neither an acceptance nor a refusal."""
    client = OrchestratorClient("http://test", "127.0.0.1", 8000, "worker", "default")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _r: response)
    ) as http:
        client._client = http
        assert (
            await client.request_vm_upgrade_for_control(
                str(THREAD_ID), template=None, control_request_id=str(REQUEST_ID)
            )
            == expected
        )


@pytest.mark.asyncio
async def test_a_transport_failure_is_no_answer():
    def handler(_request):
        raise httpx.ConnectError("refused")

    client = OrchestratorClient("http://test", "127.0.0.1", 8000, "worker", "default")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client._client = http
        assert await client.request_vm_upgrade_for_control(
            str(THREAD_ID), template=None, control_request_id=str(REQUEST_ID)
        ) == (None, None, None)
