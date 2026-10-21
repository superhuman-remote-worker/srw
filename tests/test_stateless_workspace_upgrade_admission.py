"""Pre-admission for stateless workspace upgrades (C1): order, targets, copy."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from orchestrator.services import stateless_workspace_upgrade as swu

THREAD_ID = "11111111-2222-4333-8444-555555555555"
RECORD = {"upgrade_config": {"image": "r.example/c:1"}, "upgrade_sources": None}


def _thread(*, lane="stateless", backend="virtual", **metadata):
    return {
        "id": THREAD_ID,
        "user_id": "u-1",
        "project_id": None,
        "execution_lane": lane,
        "metadata": {
            "config_override": {"workspace": {"backend": backend}},
            **metadata,
        },
    }


def _provisioner(*, available=True, in_cluster=True):
    return SimpleNamespace(is_available=available, in_cluster=in_cluster)


@pytest.fixture
def tracking(monkeypatch):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")


@pytest.fixture
def record(monkeypatch):
    upgrade_record = AsyncMock(return_value=("container", RECORD))
    monkeypatch.setattr(
        "orchestrator.services.workspace_defaults_resolution.upgrade_record",
        upgrade_record,
    )
    return upgrade_record


async def _prepare(thread, *, grants=None, provisioner=None, template=None):
    return await swu.prepare_stateless_container_upgrade(
        object(),
        thread,
        container_provisioner=provisioner or _provisioner(),
        enforce_workspace_upgrade_grants=grants or AsyncMock(),
        template_name=template,
    )


@pytest.mark.asyncio
async def test_happy_path_returns_the_record_for_a_container(tracking, record):
    grants = AsyncMock()
    assert await _prepare(_thread(), grants=grants) == RECORD
    grants.assert_awaited_once()
    assert grants.await_args.kwargs == {"target_tier": "sandbox"}
    kwargs = record.await_args.kwargs
    assert kwargs["role"] == "session"
    assert kwargs["requested_backend"] == "sandbox"
    assert kwargs["current_backend"] == "virtual"
    assert kwargs["template_name"] is None


@pytest.mark.asyncio
async def test_tracking_off_is_refused_before_grants(record, monkeypatch):
    monkeypatch.delenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", raising=False)
    grants = AsyncMock()
    with pytest.raises(HTTPException) as refused:
        await _prepare(_thread(), grants=grants)
    assert (refused.value.status_code, refused.value.detail) == (
        409,
        swu.STARTUP_TRACKING_REQUIRED,
    )
    grants.assert_not_awaited()
    record.assert_not_awaited()


@pytest.mark.asyncio
async def test_protected_session_is_refused_before_grants(tracking, record):
    grants = AsyncMock()
    with pytest.raises(HTTPException) as refused:
        await _prepare(_thread(protected_cloud=True), grants=grants)
    assert refused.value.status_code == 409
    grants.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_refused_grant_propagates(tracking, record):
    grants = AsyncMock(side_effect=HTTPException(403, "shell grant denied"))
    with pytest.raises(HTTPException) as refused:
        await _prepare(_thread(), grants=grants)
    assert refused.value.status_code == 403
    record.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("available,in_cluster", [(False, True), (True, False)])
async def test_no_in_cluster_provisioner_is_a_409_never_a_5xx(
    tracking, record, available, in_cluster
):
    with pytest.raises(HTTPException) as refused:
        await _prepare(
            _thread(),
            provisioner=_provisioner(available=available, in_cluster=in_cluster),
        )
    assert (refused.value.status_code, refused.value.detail) == (
        409,
        swu.CONTAINER_WORKSPACES_UNAVAILABLE,
    )


@pytest.mark.asyncio
async def test_a_container_session_is_refused_by_the_tier_rule(tracking, record):
    from shared.workspace_defaults import UPGRADE_REFUSED

    with pytest.raises(HTTPException) as refused:
        await _prepare(_thread(backend="sandbox"))
    assert (refused.value.status_code, refused.value.detail) == (400, UPGRADE_REFUSED)
    record.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_record_that_resolves_to_a_vm_is_a_changed_target(
    tracking, monkeypatch
):
    """C2: a container template edited to a VM after the route resolved it."""
    monkeypatch.setattr(
        "orchestrator.services.workspace_defaults_resolution.upgrade_record",
        AsyncMock(return_value=("vm", RECORD)),
    )
    with pytest.raises(HTTPException) as refused:
        await _prepare(_thread(), template="vm-full")
    assert (refused.value.status_code, refused.value.detail) == (
        409,
        swu.UPGRADE_TARGET_CHANGED,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target,template,mode,expected",
    [
        ("sandbox", None, None, "container"),
        ("container", None, None, "container"),
        ("vm", None, None, "vm"),
        (None, None, None, "container"),
        (None, "container-minimal", "container", "container"),
        (None, "vm-full", "vm", "vm"),
    ],
)
async def test_resolve_upgrade_target(monkeypatch, target, template, mode, expected):
    monkeypatch.setattr(
        "orchestrator.services.workspace_defaults_resolution.work_owner",
        AsyncMock(return_value={"id": "u-1"}),
    )
    find = AsyncMock(return_value=({"ref": {"name": template}}, mode))
    monkeypatch.setattr(
        "orchestrator.services.workspace_defaults_resolution.find_readable_template",
        find,
    )
    assert (
        await swu.resolve_upgrade_target(
            object(), _thread(), target_tier=target, template_name=template
        )
        == expected
    )
    assert find.await_count == (1 if template and target is None else 0)


@pytest.mark.asyncio
async def test_prepare_refuses_the_pinned_lane_first(tracking, record):
    deps = swu.StatelessWorkspaceUpgradeDependencies(
        container_provisioner=_provisioner(),
        enforce_workspace_upgrade_grants=AsyncMock(),
        schedule_stateless_workspace_ensure=lambda _tid: None,
    )
    with pytest.raises(HTTPException) as refused:
        await swu.prepare_stateless_workspace_upgrade(
            object(),
            _thread(lane="pinned"),
            target_tier=None,
            template_name=None,
            dependencies=deps,
        )
    assert (refused.value.status_code, refused.value.detail) == (
        409,
        swu.PINNED_TRANSPORT_REFUSAL,
    )


@pytest.mark.asyncio
async def test_prepare_refuses_a_vm_target_it_is_not_given(tracking, record):
    """C2: the route sends a VM target to the lane switch before this runs."""
    deps = swu.StatelessWorkspaceUpgradeDependencies(
        container_provisioner=_provisioner(),
        enforce_workspace_upgrade_grants=AsyncMock(),
        schedule_stateless_workspace_ensure=lambda _tid: None,
    )
    with pytest.raises(HTTPException) as refused:
        await swu.prepare_stateless_workspace_upgrade(
            object(), _thread(), target_tier="vm", template_name=None, dependencies=deps
        )
    assert refused.value.detail == swu.UPGRADE_TARGET_CHANGED
    deps.enforce_workspace_upgrade_grants.assert_not_awaited()


@pytest.mark.parametrize(
    "thread,tracking_on,provisioner,expected",
    [
        (_thread(), True, _provisioner(), ["container"]),
        (_thread(backend="none"), True, _provisioner(), ["container"]),
        (_thread(), False, _provisioner(), []),
        (_thread(backend="sandbox"), True, _provisioner(), []),
        (_thread(lane="pinned"), True, _provisioner(), []),
        (_thread(), True, _provisioner(in_cluster=False), []),
        # What the transition refuses is never offered (E4-era bug: a resumed
        # Session was offered the container, then refused).
        (
            _thread(workspace_container={"volume_reclaimed": False}),
            True,
            _provisioner(),
            [],
        ),
        (_thread(workspace_container={"repo_name": "r"}), True, _provisioner(), []),
        (_thread(workspace_container={"pod_name": "p"}), True, _provisioner(), []),
        (_thread(vm={"status": "failed"}), True, _provisioner(), []),
        (_thread(_stateless_claim_retirement={}), True, _provisioner(), []),
    ],
)
def test_workspace_upgrade_targets(
    monkeypatch, thread, tracking_on, provisioner, expected
):
    if tracking_on:
        monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    else:
        monkeypatch.delenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", raising=False)
    assert (
        swu.workspace_upgrade_targets(thread, container_provisioner=provisioner)
        == expected
    )


SEED = {"backend": "virtual", "prefix": f"threads/{THREAD_ID}/", "binding": None}
ATTENTION = {
    "stage": "readiness",
    "state": "attention",
    "reason_code": "invalid_image",
    "readiness_deadline_at": None,
}


@pytest.mark.parametrize(
    "container,creation,expected",
    [
        (None, None, None),
        ({"status": "ready"}, None, None),
        (
            {"status": "pending", "seed_from": SEED},
            None,
            {"target_tier": "sandbox", "phase": "provisioning"},
        ),
        (
            {"status": "ready", "seed_from": SEED},
            None,
            {"target_tier": "sandbox", "phase": "copying"},
        ),
        (
            {"status": "pending", "seed_from": SEED},
            ATTENTION,
            {
                "target_tier": "sandbox",
                "phase": "failed",
                "reason_code": "invalid_image",
            },
        ),
    ],
)
def test_workspace_upgrade_view(container, creation, expected):
    thread = _thread(backend="sandbox")
    if container is not None:
        thread["metadata"]["workspace_container"] = container
    assert swu.workspace_upgrade_view(thread, creation) == expected


def test_seed_spec_for_a_virtual_source_carries_the_lite_mount(monkeypatch):
    spec = {"type": "s3", "root": "srw-workspaces", "config": {"endpoint": "e"}}
    monkeypatch.setattr(
        "orchestrator.services.virtual_workspace.virtual_workspace_rclone_spec",
        lambda: spec,
    )
    out = swu.workspace_upgrade_seed_spec(THREAD_ID, SEED)
    assert out["version"] == 1
    assert out["seed_from"] == SEED
    assert out["workspace"]["backend"] == "virtual"
    assert out["workspace"]["mounts"] == [
        {
            "name": "workspace",
            "rclone_spec": spec,
            "prefix": f"threads/{THREAD_ID}/",
            "access": "read_write",
        }
    ]


def test_seed_spec_for_a_none_source_has_nothing_to_mount():
    out = swu.workspace_upgrade_seed_spec(
        THREAD_ID,
        {"backend": "none", "prefix": f"threads/{THREAD_ID}/", "binding": None},
    )
    assert out["workspace"] == {"backend": "none"}


@pytest.mark.parametrize("bound_to", ["current", "another"])
def test_seed_spec_proves_the_binding_against_the_current_store(monkeypatch, bound_to):
    """R10: a binding minted against another object store would copy 0 files
    and clear the marker silently, so the spec refuses it instead."""
    from orchestrator.services.workspace_binding import virtual_thread_backing_id

    spec = {"type": "s3", "root": "srw-workspaces", "config": {"endpoint": "e"}}
    bound_store = spec if bound_to == "current" else {**spec, "root": "old"}
    seed = {
        **SEED,
        "binding": {
            "kind": "virtual",
            "backing_id": virtual_thread_backing_id(THREAD_ID, bound_store),
        },
    }
    monkeypatch.setattr(
        "orchestrator.services.virtual_workspace.virtual_workspace_rclone_spec",
        lambda: spec,
    )

    if bound_to == "current":
        assert swu.workspace_upgrade_seed_spec(THREAD_ID, seed)["seed_from"] == seed
    else:
        with pytest.raises(ValueError, match="another object store"):
            swu.workspace_upgrade_seed_spec(THREAD_ID, seed)


@pytest.mark.asyncio
async def test_a_guard_refusing_the_transition_is_the_typed_refusal(caplog):
    """R6: a guard trigger's CheckViolationError on the transition CAS becomes
    the typed "can't be upgraded in place" refusal (a 409), never a 500 the
    cockpit would retry with the same request id forever."""
    import asyncpg

    from orchestrator.database.stateless_workspace_upgrade import (
        UPGRADE_NOT_POSSIBLE,
        StatelessUpgradeRefused,
        begin_stateless_session_workspace_upgrade,
    )

    guard = asyncpg.exceptions.CheckViolationError("guard")
    guard.constraint_name = "managed_repository_workspace_cleanup_in_progress"
    statements = []

    async def fetchval(sql, *_args):
        statements.append(sql)
        if "UPDATE threads" in sql:
            raise guard
        return False  # no earlier container history

    thread = {
        "id": THREAD_ID,
        "runtime_generation": "66666666-7777-4888-8999-aaaaaaaaaaaa",
        "metadata": {"config_override": {"workspace": {"backend": "virtual"}}},
    }

    with pytest.raises(StatelessUpgradeRefused) as refused:
        await begin_stateless_session_workspace_upgrade(
            SimpleNamespace(fetchval=fetchval),
            thread,
            upgrade_config=RECORD["upgrade_config"],
            upgrade_sources=None,
        )

    assert str(refused.value) == UPGRADE_NOT_POSSIBLE
    assert refused.value.__cause__ is guard
    assert len(statements) == 2
    assert "managed_repository_workspace_cleanup_in_progress" in caplog.text
