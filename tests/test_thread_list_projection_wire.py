"""The Sessions list returns the same owner lifecycle projection as detail."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from orchestrator.routers import thread_admission as admission_router
from orchestrator.services.thread_projection import redact_thread_metadata

from ._mounted_router import mount_router


THREAD_ID = "11111111-2222-4333-8444-555555555555"


def test_list_projects_settled_permanent_cleanup_without_failing_other_rows(
    monkeypatch,
) -> None:
    owner = {
        "id": THREAD_ID,
        "status": "ended",
        "execution_lane": "stateless",
        "metadata": json.dumps(
            {
                "_stateless_workspace_retirement_settled": {
                    "terminal_token": 2,
                    "cleanup_complete": True,
                    "permanent": True,
                    "snapshot_restore_required": False,
                    "runtime_incarnation": "6a5b7eb8-ba34-4179-884d-7da1bee7bcc8",
                    "backing_id": None,
                }
            }
        ),
    }
    malformed = {
        "id": "22222222-3333-4444-8555-666666666666",
        "status": "ended",
        "execution_lane": "stateless",
        "metadata": json.dumps(
            {"_stateless_workspace_retirement_settled": {"permanent": True}}
        ),
    }
    store = SimpleNamespace(
        list_threads=AsyncMock(return_value=[owner, malformed]),
        list_thread_mounts_bulk=AsyncMock(return_value={}),
        container_workspace_creation_views=AsyncMock(return_value={}),
    )
    dependencies = SimpleNamespace(
        store=store,
        require_approved_user=AsyncMock(return_value={"id": "owner-1"}),
        resolve_cloud_session_url=lambda _thread, _mounts: None,
        redact_thread_metadata=redact_thread_metadata,
    )
    monkeypatch.setattr(
        admission_router, "read_vm_idle_states", AsyncMock(return_value={})
    )
    app = mount_router(
        admission_router.router,
        factories={"thread_admission_dependencies_factory": lambda: dependencies},
    )

    response = TestClient(app).get("/api/persistent/threads")

    assert response.status_code == 200
    projected, refused = response.json()["threads"]
    assert projected["runtime_retirement_pending"] is True
    assert projected["retirement_disposition"] == "ended"
    assert projected["retirement_permanent"] is True
    assert refused["runtime_retirement_pending"] is False
    assert refused["retirement_permanent"] is False
    store.list_threads.assert_awaited_once_with(
        user_id="owner-1", project_id=None, status=None
    )
