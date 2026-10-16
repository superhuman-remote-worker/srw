"""C2's durable VM upgrade seed marker and its internal routes."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from orchestrator.schemas.thread_config import VmUpgradeSeededRequest
from orchestrator.services import vm_upgrade_seed as vus
from shared.workspace_upgrade import (
    UPGRADE_ALREADY_RUNNING,
    UPGRADE_CANCELLED,
    UPGRADE_FAILED,
    UPGRADE_INTERRUPTED,
    UPGRADE_REFUSED,
    WORKSPACE_UPGRADE_FAILURE_CODES,
    vm_provision_generation,
    vm_upgrade_seed,
)

THREAD_ID = "11111111-1111-4111-8111-111111111111"
REQUEST_ID = "55555555-5555-4555-8555-555555555555"
GENERATION = "66666666-6666-4666-8666-666666666666"
SOURCE = {
    "backend": "virtual",
    "prefix": f"threads/{THREAD_ID}/",
    "binding": {"kind": "virtual", "backing_id": "rclone:" + "a" * 64},
}


def _metadata(marker, **vm):
    return {"vm": {"upgrade_seed": marker, **vm}}


def test_the_failure_codes_are_the_five_typed_reasons():
    assert WORKSPACE_UPGRADE_FAILURE_CODES == {
        UPGRADE_CANCELLED,
        UPGRADE_REFUSED,
        UPGRADE_FAILED,
        UPGRADE_INTERRUPTED,
        UPGRADE_ALREADY_RUNNING,
    }
    assert UPGRADE_CANCELLED == "upgrade_cancelled"
    assert UPGRADE_INTERRUPTED == "upgrade_interrupted"


@pytest.mark.parametrize("state", ["pending", "seeded"])
def test_a_well_formed_marker_is_read_back(state):
    marker = {
        "control_request_id": REQUEST_ID.upper(),
        "state": state,
        "source": SOURCE,
    }
    assert vm_upgrade_seed(_metadata(marker)) == {
        "control_request_id": REQUEST_ID,
        "state": state,
        "source": SOURCE,
    }


def test_json_text_metadata_and_a_none_source_are_accepted():
    marker = {"control_request_id": REQUEST_ID, "state": "pending", "source": None}
    assert vm_upgrade_seed(json.dumps(_metadata(marker)))["source"] is None


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        {},
        {"vm": None},
        {"vm": {"status": "ready"}},
        _metadata(None),
        _metadata(
            {"control_request_id": "not-a-uuid", "state": "pending", "source": None}
        ),
        _metadata({"control_request_id": REQUEST_ID, "state": "done", "source": None}),
        _metadata(
            {
                "control_request_id": REQUEST_ID,
                "state": "pending",
                "source": {"backend": "sandbox"},
            }
        ),
        _metadata(
            {
                "control_request_id": REQUEST_ID,
                "state": "pending",
                "source": "threads/x/",
            }
        ),
    ],
)
def test_anything_else_is_no_marker(metadata):
    assert vm_upgrade_seed(metadata) is None


def test_the_provision_generation_is_canonical_or_absent():
    assert (
        vm_provision_generation({"vm": {"provision_generation": GENERATION.upper()}})
        == GENERATION
    )
    assert vm_provision_generation({"vm": {"provision_generation": "x"}}) is None
    assert vm_provision_generation({"vm": {}}) is None
    assert vm_provision_generation({}) is None


AGENT_ID = "77777777-7777-4777-8777-777777777777"
RUNTIME = "88888888-8888-4888-8888-888888888888"
ATTACH = "99999999-9999-4999-8999-999999999999"
HEADERS = {
    "X-Agent-ID": AGENT_ID,
    "X-Session-Runtime-Generation": RUNTIME,
    "X-Session-Runtime-Attach-Token": ATTACH,
}


def _thread(marker=None, *, status="ready", **over):
    vm = {"status": status, "provision_generation": GENERATION}
    if marker is not None:
        vm["upgrade_seed"] = marker
    row = {
        "id": THREAD_ID,
        "execution_lane": "pinned",
        "agent_id": AGENT_ID,
        "runtime_generation": RUNTIME,
        "runtime_attach_token": ATTACH,
        "runtime_retirement_token": None,
        "metadata": {"vm": vm},
    }
    row.update(over)
    return row


def _deps(thread, *, request_row=None):
    return SimpleNamespace(
        require_internal=AsyncMock(),
        store=SimpleNamespace(
            get_thread=AsyncMock(return_value=thread),
            fetchrow=AsyncMock(
                return_value=request_row
                if request_row is not None
                else {"verb": "upgrade-to-workspace", "outcome": "applied"}
            ),
        ),
    )


def _request(headers=HEADERS):
    return SimpleNamespace(headers=headers)


PENDING = {"control_request_id": REQUEST_ID, "state": "pending", "source": SOURCE}


@pytest.mark.parametrize(
    "headers,over",
    [
        ({}, {}),
        ({**HEADERS, "X-Agent-ID": "00000000-0000-4000-8000-000000000000"}, {}),
        ({**HEADERS, "X-Session-Runtime-Attach-Token": ""}, {}),
        (HEADERS, {"execution_lane": "stateless"}),
        (HEADERS, {"runtime_retirement_token": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}),
    ],
)
def test_the_routes_require_the_exact_pinned_runtime(headers, over):
    with pytest.raises(HTTPException) as refused:
        vus.require_pinned_runtime_identity(_thread(**over), headers)
    assert refused.value.status_code == 409
    assert refused.value.detail == {"code": "pinned_runtime_identity_mismatch"}


def test_the_exact_runtime_passes_and_names_its_agent():
    assert vus.require_pinned_runtime_identity(_thread(), HEADERS) == AGENT_ID


@pytest.mark.asyncio
async def test_seed_source_returns_the_lite_mount_for_a_pending_marker(monkeypatch):
    spec = {
        "version": 1,
        "seed_from": SOURCE,
        "workspace": {"backend": "virtual", "mounts": []},
    }
    monkeypatch.setattr(
        "orchestrator.services.stateless_workspace_upgrade.workspace_upgrade_seed_spec",
        lambda thread_id, seed_from: spec,
    )
    deps = _deps(_thread(PENDING))
    out = await vus.agent_vm_upgrade_seed_source(
        _request(), THREAD_ID, dependencies=deps
    )
    assert out == {
        **spec,
        "control_request_id": REQUEST_ID,
        "provision_generation": GENERATION,
    }
    deps.require_internal.assert_awaited_once()


@pytest.mark.asyncio
async def test_seed_source_for_a_none_session_has_nothing_to_mount():
    deps = _deps(_thread({**PENDING, "source": None}))
    out = await vus.agent_vm_upgrade_seed_source(
        _request(), THREAD_ID, dependencies=deps
    )
    assert out["seed_from"] is None
    assert out["workspace"] == {"backend": "none"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "marker,generation,request_row",
    [
        (None, GENERATION, None),
        ({**PENDING, "state": "seeded"}, GENERATION, None),
        (PENDING, None, None),
        (PENDING, GENERATION, {"verb": "compact", "outcome": None}),
    ],
    ids=["no-marker", "already-seeded", "no-provision", "not-an-upgrade-request"],
)
async def test_seed_source_answers_only_for_a_pending_c2_marker(
    marker, generation, request_row
):
    thread = _thread(marker)
    if generation is None:
        thread["metadata"]["vm"].pop("provision_generation")
    deps = _deps(thread, request_row=request_row)
    with pytest.raises(HTTPException) as refused:
        await vus.agent_vm_upgrade_seed_source(_request(), THREAD_ID, dependencies=deps)
    assert refused.value.detail == {"code": "no_pending_upgrade_seed"}


@pytest.mark.asyncio
async def test_seed_source_refuses_a_changed_object_store(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.virtual_workspace.virtual_workspace_rclone_spec",
        lambda: {"type": "s3", "root": "other-bucket", "config": {"endpoint": "e"}},
    )
    deps = _deps(_thread(PENDING))
    with pytest.raises(HTTPException) as refused:
        await vus.agent_vm_upgrade_seed_source(_request(), THREAD_ID, dependencies=deps)
    assert refused.value.detail == {"code": "upgrade_seed_source_changed"}


@pytest.mark.asyncio
async def test_seed_source_refuses_when_no_object_store_is_configured(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.virtual_workspace.virtual_workspace_rclone_spec",
        lambda: None,
    )
    deps = _deps(_thread(PENDING))
    with pytest.raises(HTTPException) as refused:
        await vus.agent_vm_upgrade_seed_source(_request(), THREAD_ID, dependencies=deps)
    assert refused.value.status_code == 409
    assert refused.value.detail == {"code": "upgrade_seed_source_changed"}


@pytest.mark.asyncio
@pytest.mark.parametrize("marked", [True, False])
async def test_seeded_is_the_fenced_cas_or_a_409(monkeypatch, marked):
    mark = AsyncMock(return_value=marked)
    monkeypatch.setattr(vus, "mark_vm_upgrade_seeded", mark)
    body = VmUpgradeSeededRequest(
        provision_generation=GENERATION, control_request_id=REQUEST_ID
    )
    deps = _deps(_thread(PENDING))
    if marked:
        out = await vus.agent_vm_upgrade_seeded(
            _request(), THREAD_ID, body, dependencies=deps
        )
        assert out == {"state": "seeded"}
    else:
        with pytest.raises(HTTPException) as refused:
            await vus.agent_vm_upgrade_seeded(
                _request(), THREAD_ID, body, dependencies=deps
            )
        assert refused.value.detail == {"code": "upgrade_seed_fence_lost"}
    assert mark.await_args.kwargs == {
        "agent_id": AGENT_ID,
        "provision_generation": GENERATION,
        "control_request_id": REQUEST_ID,
    }
