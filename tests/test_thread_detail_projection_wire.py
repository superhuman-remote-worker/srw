"""``GET /api/persistent/threads/{thread_id}`` returns the owner projection.

``redact_thread_metadata`` itself is covered elsewhere; what no case asserted
before R1.B10 is that the detail route actually runs every row through it.
This drives the mounted router with an owner gate that hands back a raw
``SELECT *``-shaped row and checks the wire: metadata leaves as a parsed object
with binding and process-zero evidence dropped, credential-shaped overrides
redacted, runtime/retirement internals removed, the public retirement state
derived, and mounts projected.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from fastapi import HTTPException
from fastapi.testclient import TestClient

from orchestrator.routers import thread_session

from ._mounted_router import mount_router

THREAD_ID = "11111111-2222-4333-8444-555555555555"
PROJECT_ID = "22222222-3333-4444-8555-666666666666"


class _Store:
    def __init__(self) -> None:
        self.container_workspace_creation_views = AsyncMock(return_value={})
        self.ensure_thread_ssh_handle = AsyncMock(return_value="s-minted")
        self.list_thread_mounts = AsyncMock(
            return_value=[
                {
                    "id": 1,
                    "mount_kind": "project",
                    "target_path": "/workspace/project",
                    "source_kind": "project",
                    "source_ref": PROJECT_ID,
                    "backend_id": None,
                }
            ]
        )


def _raw_row() -> dict:
    return {
        "id": THREAD_ID,
        "user_id": "owner-1",
        "ssh_handle": None,
        "metadata": json.dumps(
            {
                "config_override": {"llm": {"api_key": "sk-live-secret"}},
                "_workspace_binding": {"generation": "g", "kind": "remote"},
                "_stateless_workspace_process_zero_observation": {"pid": 1},
                "expert_id": "e-1",
            }
        ),
        "runtime_generation": "33333333-4444-4555-8666-777777777777",
        "runtime_attach_token": "tok",
        "runtime_retirement_token": "retire",
        "runtime_retirement_authorized_at": "2026-09-23T10:00:00Z",
        "runtime_retirement_context": json.dumps({"settle_status": "suspended"}),
        "runtime_retirement_stage_receipt": {"stage": "x"},
    }


def _client(
    store: _Store, row: dict | None = None, *, denied: bool = False
) -> TestClient:
    async def gate(request, gate_store, thread_id):
        del request
        assert gate_store is store and thread_id == THREAD_ID
        if denied:
            raise HTTPException(status_code=403, detail="not owner")
        return {
            "id": "owner-1",
            "is_admin": False,
        }, row if row is not None else _raw_row()

    app = mount_router(
        thread_session.router,
        factories={
            "thread_session_dependencies_factory": (
                lambda: thread_session.ThreadSessionDependencies(
                    store=store,
                    require_thread_owner=gate,
                    require_approved_user=AsyncMock(),
                    resolve_cloud_session_url=lambda thread, mounts: "https://cloud/x",
                    resolve_session_config=AsyncMock(),
                    enforce_session_create_grants=AsyncMock(),
                    tool_view=None,
                )
            )
        },
    )
    return TestClient(app)


def test_detail_route_returns_the_redacted_owner_projection() -> None:
    store = _Store()
    body = _client(store).get(f"/api/persistent/threads/{THREAD_ID}").json()

    metadata = body["metadata"]
    assert isinstance(metadata, dict)
    assert "_workspace_binding" not in metadata
    assert "_stateless_workspace_process_zero_observation" not in metadata
    assert "sk-live-secret" not in json.dumps(body)
    assert metadata["expert_id"] == "e-1"
    for internal in (
        "runtime_generation",
        "runtime_attach_token",
        "runtime_retirement_token",
        "runtime_retirement_authorized_at",
        "runtime_retirement_context",
        "runtime_retirement_stage_receipt",
    ):
        assert internal not in body
    assert body["runtime_retirement_pending"] is True
    assert body["retirement_disposition"] == "suspended"
    assert body["session_runtime_generation"] == _raw_row()["runtime_generation"]


def test_detail_route_mints_a_missing_handle_and_projects_mounts() -> None:
    store = _Store()
    body = _client(store).get(f"/api/persistent/threads/{THREAD_ID}").json()

    store.ensure_thread_ssh_handle.assert_awaited_once_with(THREAD_ID)
    assert body["ssh_handle"] == "s-minted"
    assert body["cloud_session_url"] == "https://cloud/x"
    assert body["project_ids"] == [PROJECT_ID]
    assert body["mounts"] == [
        {
            "id": "1",
            "mount_kind": "project",
            "target_path": "/workspace/project",
            "source_kind": "project",
            "source_ref": PROJECT_ID,
            "backend_id": None,
        }
    ]


def test_detail_route_still_reads_a_legacy_two_project_session() -> None:
    """New Sessions take one project at most, but one created before that
    rule keeps both project mounts and still opens."""
    other_project = "33333333-4444-4555-8666-777777777777"
    store = _Store()
    store.list_thread_mounts.return_value = [
        *store.list_thread_mounts.return_value,
        {
            "id": 2,
            "mount_kind": "project",
            "target_path": "/workspace/projects/other",
            "source_kind": "project",
            "source_ref": other_project,
            "backend_id": None,
        },
    ]
    response = _client(store).get(f"/api/persistent/threads/{THREAD_ID}")
    assert response.status_code == 200
    assert response.json()["project_ids"] == [PROJECT_ID, other_project]


def test_detail_route_reads_one_allowlisted_startup_view_after_owner_gate() -> None:
    store = _Store()
    view = {
        "stage": "scheduling",
        "state": "observing",
        "reason_code": "observation_pending",
        "readiness_deadline_at": None,
    }
    store.container_workspace_creation_views.return_value = {UUID(THREAD_ID): view}
    body = _client(store).get(f"/api/persistent/threads/{THREAD_ID}").json()
    assert body["workspace_creation"] == view
    store.container_workspace_creation_views.assert_awaited_once_with(
        "thread", [THREAD_ID]
    )


def test_detail_denial_precedes_startup_receipt_read() -> None:
    store = _Store()
    response = _client(store, denied=True).get(f"/api/persistent/threads/{THREAD_ID}")
    assert response.status_code == 403
    store.container_workspace_creation_views.assert_not_awaited()


@pytest.mark.parametrize("permanent", [False, True])
@pytest.mark.parametrize("authorized", [False, True])
def test_detail_preserves_raw_status_but_hides_ready_during_authorized_end(
    permanent, authorized
):
    from .test_vm_idle_public import _thread_owner

    row = {**_raw_row(), **_thread_owner(), "id": THREAD_ID, "ssh_handle": "s-owner"}
    row.update(
        runtime_retirement_token="retire",
        runtime_retirement_authorized_at="2026-09-27T10:00:00Z" if authorized else None,
        runtime_retirement_context={"settle_status": "ended"},
        runtime_retirement_permanent=permanent,
        workspace_idle_episode=None,
    )

    class Store(_Store):
        @asynccontextmanager
        async def acquire(self):
            yield self

        async def fetch(self, query, owner_kind, ids):
            assert owner_kind == "thread" and [str(i) for i in ids] == [THREAD_ID]
            return [row]

    body = _client(Store(), row).get(f"/api/persistent/threads/{THREAD_ID}").json()
    assert body["status"] == "active"
    assert body["runtime_retirement_pending"] is authorized
    assert body["retirement_disposition"] == ("ended" if authorized else None)
    assert body["retirement_permanent"] is (authorized and permanent)
    assert body["workspace_lifecycle"] == (None if authorized else {"state": "ready"})
    assert "ended_at" not in body
