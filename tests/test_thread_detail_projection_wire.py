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
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from orchestrator.routers import thread_session

from ._mounted_router import mount_router

THREAD_ID = "11111111-2222-4333-8444-555555555555"
PROJECT_ID = "22222222-3333-4444-8555-666666666666"


class _Store:
    def __init__(self) -> None:
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


def _client(store: _Store) -> TestClient:
    async def gate(request, gate_store, thread_id):
        del request
        assert gate_store is store and thread_id == THREAD_ID
        return {"id": "owner-1", "is_admin": False}, _raw_row()

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
