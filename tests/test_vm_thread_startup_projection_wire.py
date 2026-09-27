"""List/detail/IDE compose existing creation progress without granting access."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.routers import thread_admission, thread_files, thread_session
from orchestrator.services.thread_projection import redact_thread_metadata


class ProjectionStore:
    supports_vm_creation_retry = True

    def __init__(self, status, state):
        self.thread_id, self.user_id, self.request_id = uuid4(), uuid4(), uuid4()
        self.row = {
            "id": self.thread_id,
            "user_id": self.user_id,
            "status": "created",
            "kind": "session",
            "execution_lane": "pinned",
            "ssh_handle": "s-test",
            "metadata": {
                "vm": {"status": status, "creation_request_id": str(self.request_id)},
                "config_override": {"workspace": {"backend": "vm"}},
            },
            "workspace_idle_revision": 2,
            "workspace_idle_episode": None,
            "idle_phase": None,
            "idle_episode_id": None,
        }
        self.creation = {
            "request_id": str(self.request_id),
            "stage": "creation",
            "state": state,
            "reason": None,
            "ready_at": None,
            "pending": True,
            "resume_blocked": True,
        }
        self.list_thread_mounts = AsyncMock(return_value=[])
        self.list_thread_mounts_bulk = AsyncMock(return_value={})
        self.list_threads = AsyncMock(side_effect=lambda **_: [dict(self.row)])

    @asynccontextmanager
    async def acquire(self):
        yield self

    async def fetch(self, query, *args):
        if "AS creation" in query:
            assert args == ([self.thread_id], self.user_id, False)
            return [
                {"id": self.thread_id, "status": "created", "creation": self.creation}
            ]
        assert "op.phase AS idle_phase" in query
        assert args == ("thread", [self.thread_id])
        return [self.row]

    async def fetchrow(self, query, *args):
        assert "vm_idle_operations" in query
        return None


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["list", "detail", "ide"])
@pytest.mark.parametrize("status", ["created", "ssh_pending"])
@pytest.mark.parametrize(
    "state",
    ["queued", "reconciling", "succeeded", "attention", "cancel_requested", "settled"],
)
async def test_current_creation_progress_is_composed_on_every_thread_read(
    monkeypatch,
    surface,
    status,
    state,
):
    store = ProjectionStore(status, state)
    user = {"id": str(store.user_id), "is_admin": False}
    dependencies = SimpleNamespace(
        store=store,
        require_approved_user=AsyncMock(return_value=user),
        require_thread_owner=AsyncMock(return_value=(user, store.row)),
        resolve_cloud_session_url=lambda *_: None,
        redact_thread_metadata=redact_thread_metadata,
    )
    if surface == "list":
        monkeypatch.setattr(
            thread_admission,
            "get_thread_admission_dependencies",
            lambda _: dependencies,
        )
        result = await thread_admission.list_threads(SimpleNamespace())
        payload = result["threads"][0]
    elif surface == "detail":
        payload = await thread_session.get_thread(
            str(store.thread_id),
            SimpleNamespace(),
            dependencies=dependencies,
        )
    else:
        payload = await thread_files.get_thread_ide_status(
            str(store.thread_id),
            SimpleNamespace(),
            dependencies=dependencies,
        )
    starting = state in {"queued", "reconciling", "succeeded"}
    assert payload["workspace_lifecycle"] == (
        None
        if starting
        else {"state": "release_held", "reason_code": "identity_unverified"}
    )
    if surface == "ide":
        assert payload["status"] == ("restoring" if starting else "unavailable")
        assert payload["code_server_url"] is None
    else:
        assert payload["status"] == "created"
        if state == "settled":
            assert payload["vm_creation"] is None
        else:
            assert payload["vm_creation"]["state"] == state
            assert payload["vm_creation"]["request_id"] == str(store.request_id)
    assert "runtime_attach_token" not in payload
