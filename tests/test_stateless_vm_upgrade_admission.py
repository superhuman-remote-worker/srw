"""Pre-admission for stateless VM upgrades (C2): order, targets, copy."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from orchestrator.services import stateless_workspace_upgrade as swu
from orchestrator.services.thread_config_update import VM_PROVISIONING_UNAVAILABLE
from orchestrator.services.workspace_defaults_resolution import (
    RETAINED_UPGRADE_UNAVAILABLE,
)

THREAD_ID = "11111111-2222-4333-8444-555555555555"
RECORD = {"upgrade_config": {"image": "r.example/vm:1"}, "upgrade_sources": None}


def _thread(*, lane="stateless", backend="virtual", kind="session", **metadata):
    return {
        "id": THREAD_ID,
        "user_id": "u-1",
        "project_id": None,
        "kind": kind,
        "execution_lane": lane,
        "metadata": {
            "config_override": {"workspace": {"backend": backend}},
            **metadata,
        },
    }


def _vm(available=True):
    return SimpleNamespace(is_available=available)


@pytest.fixture
def record(monkeypatch):
    upgrade_record = AsyncMock(return_value=("vm", RECORD))
    monkeypatch.setattr(
        "orchestrator.services.workspace_defaults_resolution.upgrade_record",
        upgrade_record,
    )
    return upgrade_record


async def _check(thread, *, grants=None, vm="default", template=None):
    return await swu.check_stateless_vm_upgrade(
        object(),
        thread,
        template_name=template,
        vm_provisioner=_vm() if vm == "default" else vm,
        enforce_workspace_upgrade_grants=grants or AsyncMock(),
    )


@pytest.mark.asyncio
async def test_a_root_lite_stateless_session_passes_in_the_spec_order(record):
    order = []
    grants = AsyncMock(side_effect=lambda *_a, **_k: order.append("grants"))
    record.side_effect = lambda *_a, **_k: order.append("record") or ("vm", RECORD)

    assert await _check(_thread(), grants=grants, template="vm-big") is None

    assert order == ["grants", "record"]
    assert grants.await_args.kwargs == {"target_tier": "vm"}
    kwargs = record.await_args.kwargs
    assert (
        kwargs["role"],
        kwargs["requested_backend"],
        kwargs["current_backend"],
        kwargs["template_name"],
    ) == ("session", "vm", "virtual", "vm-big")


@pytest.mark.asyncio
async def test_a_none_session_can_move_to_a_vm(record):
    assert await _check(_thread(backend="none")) is None
    assert record.await_args.kwargs["current_backend"] == "none"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "thread",
    [
        _thread(lane="pinned"),
        _thread(backend="sandbox"),
        _thread(kind="subagent"),
        _thread(vm={"status": "failed"}),
        _thread(
            config_override={
                "workspace": {"backend": "virtual"},
                "officer": {"enabled": True},
            }
        ),
    ],
    ids=["pinned", "sandbox", "subagent", "vm-evidence", "officer"],
)
async def test_only_a_root_lite_stateless_session_can_switch(record, thread):
    grants = AsyncMock()
    with pytest.raises(HTTPException) as refused:
        await _check(thread, grants=grants)
    assert (refused.value.status_code, refused.value.detail) == (
        409,
        swu.VM_UPGRADE_NOT_POSSIBLE,
    )
    grants.assert_not_awaited()
    record.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_refused_grant_propagates_before_the_provisioner_check(record):
    with pytest.raises(HTTPException) as refused:
        await _check(
            _thread(),
            grants=AsyncMock(
                side_effect=HTTPException(403, "vm_workspace grant denied")
            ),
            vm=_vm(False),
        )
    assert refused.value.status_code == 403
    record.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("vm", [_vm(False), None], ids=["off", "absent"])
async def test_vm_provisioning_off_is_a_409_never_a_5xx(record, vm):
    with pytest.raises(HTTPException) as refused:
        await _check(_thread(), vm=vm)
    assert (refused.value.status_code, refused.value.detail) == (
        409,
        VM_PROVISIONING_UNAVAILABLE,
    )
    record.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_template_that_resolves_to_a_container_is_a_changed_target(record):
    record.return_value = ("container", RECORD)
    with pytest.raises(HTTPException) as refused:
        await _check(_thread(), template="container-minimal")
    assert (refused.value.status_code, refused.value.detail) == (
        409,
        swu.UPGRADE_TARGET_CHANGED,
    )


@pytest.mark.asyncio
async def test_a_kept_template_is_refused_by_the_record(record):
    record.side_effect = HTTPException(409, RETAINED_UPGRADE_UNAVAILABLE)
    with pytest.raises(HTTPException) as refused:
        await _check(_thread(), template="vm-kept")
    assert refused.value.detail == RETAINED_UPGRADE_UNAVAILABLE


@pytest.mark.parametrize(
    "thread,tracking_on,vm,expected",
    [
        (_thread(), True, _vm(), ["container", "vm"]),
        (_thread(), False, _vm(), ["vm"]),
        (_thread(), True, _vm(False), ["container"]),
        (_thread(), True, None, ["container"]),
        (_thread(backend="none"), False, _vm(), ["vm"]),
        (_thread(kind="subagent"), False, _vm(), []),
        (_thread(lane="pinned"), True, _vm(), []),
        (_thread(backend="sandbox"), True, _vm(), []),
    ],
)
def test_workspace_upgrade_targets_declares_vm_where_it_can_run(
    monkeypatch, thread, tracking_on, vm, expected
):
    if tracking_on:
        monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    else:
        monkeypatch.delenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", raising=False)
    provisioner = SimpleNamespace(is_available=True, in_cluster=True)
    assert (
        swu.workspace_upgrade_targets(
            thread, container_provisioner=provisioner, vm_provisioner=vm
        )
        == expected
    )


REQUEST = "55555555-5555-4555-8555-555555555555"
PENDING_MARKER = {"control_request_id": REQUEST, "state": "pending", "source": None}
SEEDED_MARKER = {**PENDING_MARKER, "state": "seeded"}


def _request(outcome=None, error_code=None):
    return {"id": REQUEST, "outcome": outcome, "error_code": error_code, "payload": {}}


@pytest.mark.parametrize(
    "lane,vm,latest,expected",
    [
        ("pinned", None, _request(), {"target_tier": "vm", "phase": "requested"}),
        (
            "pinned",
            {"status": "provisioning", "upgrade_seed": PENDING_MARKER},
            _request("applied"),
            {"target_tier": "vm", "phase": "provisioning"},
        ),
        (
            "pinned",
            {"status": "ready", "upgrade_seed": PENDING_MARKER},
            _request("applied"),
            {"target_tier": "vm", "phase": "copying"},
        ),
        (
            "pinned",
            {"status": "aborted", "upgrade_seed": SEEDED_MARKER},
            _request("applied"),
            {"target_tier": "vm", "phase": "failed", "reason_code": "upgrade_failed"},
        ),
        (
            "pinned",
            {"status": "ready", "upgrade_seed": SEEDED_MARKER},
            _request("applied"),
            None,
        ),
        (
            "pinned",
            None,
            _request("rejected", "upgrade_cancelled"),
            {
                "target_tier": "vm",
                "phase": "failed",
                "reason_code": "upgrade_cancelled",
            },
        ),
        ("pinned", None, _request("rejected", "unsupported_control"), None),
        (
            "pinned",
            {"status": "ready", "upgrade_seed": None},
            _request("rejected", "upgrade_refused"),
            None,
        ),
        ("pinned", {"status": "failed"}, None, None),
        ("stateless", None, _request(), None),
    ],
    ids=[
        "requested",
        "provisioning",
        "copying",
        "vm-aborted",
        "done",
        "cancelled",
        "old-agent-rejection",
        "a-later-websocket-retry-succeeded",
        "websocket-path-failure",
        "not-pinned",
    ],
)
def test_workspace_upgrade_view_reports_the_vm_phases(lane, vm, latest, expected):
    thread = _thread(lane=lane)
    if vm is not None:
        thread["metadata"]["vm"] = vm
    assert swu.workspace_upgrade_view(thread, None, latest_request=latest) == expected
