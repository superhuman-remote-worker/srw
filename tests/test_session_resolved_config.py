"""``GET /api/persistent/threads/{id}/resolved-config`` serves the frozen session.

The live settings pane shows the real values of settings that can no longer
change. They come from the session's execution snapshot, never from a fresh
resolve, and the read is owner-gated like its sibling thread routes. Nothing
that addresses an endpoint or authenticates to one may cross it.
"""

from __future__ import annotations

import json
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from orchestrator.routers import thread_session
from orchestrator.security import access
from orchestrator.services import manifest_execution_snapshot as snapshots

from ._mounted_router import mount_router

THREAD_ID = "11111111-2222-4333-8444-555555555555"
OWNER = {"id": "owner-1", "is_admin": False}
STRANGER = {"id": "someone-else", "is_admin": False}
ADMIN = {"id": "admin-1", "is_admin": True}
SECRETS = (
    "sk-llm-secret",
    "sk-aux-secret",
    "sk-env-secret",
    "sk-citation-secret",
    "https://llm.internal.example",
    "https://embed.internal.example",
    "workspace-host.internal",
)


def _frozen_blob() -> dict:
    """A resolved blob carrying every credential/transport shape it could hold.

    A current snapshot is already rendered without them; a historical or
    tampered row must still never leak one through this read.
    """
    return {
        "agent": {
            "llm": {
                "model": "gpt-4o",
                "temperature": 0.2,
                "api_key": "sk-llm-secret",
                "base_url": "https://llm.internal.example",
            },
            "auxiliary": {"model": "aux", "api_key": "sk-aux-secret"},
            "env_keys": {
                "EMBEDDING_API_KEY": "sk-env-secret",
                "EMBEDDING_BASE_URL": "https://embed.internal.example",
            },
            "citation_llm_api_key": "sk-citation-secret",
            "workspace": {
                "backend": "sandbox",
                "remote": {"host": "workspace-host.internal", "port": 22},
                "mounts": [{"source": "workspace-host.internal"}],
            },
            "tools": {"research": ["web_search"], "shell": []},
            "interactive": {"permission_mode": "supervised"},
        },
        "prompts": {"persona": "A terse helper."},
        "instructions": {},
        "model_family": "gpt",
        "resolved_at": "2026-10-06T12:00:00+00:00",
    }


def _execution_row(blob: dict, *, adapter: str = "srw/v1") -> dict:
    prepared = snapshots.rendered_srw_snapshot(
        {"agent": {}},
        {"llm": {"model": "gpt-4o"}},
        work_kind="Session",
        work_id=THREAD_ID,
        owner_id=OWNER["id"],
        project_ids=[],
        config_name="session_base",
        description="Session",
        datasource_ids=[],
        policy_revisions={},
        image="installed:1",
        dependencies=[],
    )
    # Bypass the render-time redaction on purpose (see _frozen_blob).
    runtime = prepared["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"]
    runtime["config"]["resolved"] = deepcopy(blob)
    return {
        "id": "99999999-2222-4333-8444-555555555555",
        "generation": 2,
        "harness_adapter": adapter,
        "document": json.dumps(prepared["document"]),
        "resolved": json.dumps(prepared["resolved"]),
        "revision": prepared["revision"],
        "dependencies": "[]",
    }


class _Store:
    def __init__(self, thread: dict | None, execution: dict | None) -> None:
        self.thread = thread
        self.execution = execution
        self.queries: list[str] = []

    async def get_thread(self, thread_id: str):
        assert thread_id == THREAD_ID
        return deepcopy(self.thread)

    async def fetchrow(self, query: str, *args):
        self.queries.append(query)
        assert "FROM srw_execution_specs" in query
        assert args[0] == "Session" and str(args[1]) == THREAD_ID
        return deepcopy(self.execution)


def _thread(**over) -> dict:
    row = {
        "id": THREAD_ID,
        "user_id": OWNER["id"],
        "permission_mode": "auto_accept",
        "narration_mode": "verbose",
        "metadata": json.dumps(
            {"expert_selection_source": "inline", "expert_based_on": "assistant"}
        ),
    }
    row.update(over)
    return row


def _client(monkeypatch, store: _Store, caller: dict) -> TestClient:
    async def approved(request, db):
        assert db is store
        return dict(caller)

    monkeypatch.setattr(access, "require_approved_user", approved)
    monkeypatch.setattr(access, "log_security_event", AsyncMock())
    resolve = AsyncMock(side_effect=AssertionError("the read must not re-resolve"))
    app = mount_router(
        thread_session.router,
        factories={
            "thread_session_dependencies_factory": (
                lambda: thread_session.ThreadSessionDependencies(
                    store=store,
                    require_thread_owner=access.require_thread_owner,
                    require_approved_user=AsyncMock(),
                    resolve_cloud_session_url=lambda *_: None,
                    resolve_session_config=resolve,
                    enforce_session_create_grants=AsyncMock(),
                    tool_view=None,
                )
            )
        },
    )
    return TestClient(app)


URL = f"/api/persistent/threads/{THREAD_ID}/resolved-config"


@pytest.mark.parametrize("caller", [OWNER, ADMIN], ids=["owner", "admin"])
def test_the_owner_reads_the_frozen_configuration_without_secrets(monkeypatch, caller):
    store = _Store(_thread(), _execution_row(_frozen_blob()))
    response = _client(monkeypatch, store, caller).get(URL)

    assert response.status_code == 200, response.text
    assert response.headers["Cache-Control"] == "private, no-store"
    body = response.json()
    assert set(body) == {"thread_id", "source", "config", "expert_based_on"}
    assert body["thread_id"] == THREAD_ID
    assert body["source"] == "snapshot"
    assert body["expert_based_on"] == "assistant"
    agent = body["config"]["agent"]
    assert agent["llm"] == {"model": "gpt-4o", "temperature": 0.2}
    assert agent["tools"] == {"research": ["web_search"], "shell": []}
    assert agent["workspace"] == {"backend": "sandbox"}
    assert "env_keys" not in agent and "citation_llm_api_key" not in agent
    assert agent["auxiliary"] == {"model": "aux"}
    assert body["config"]["prompts"] == {"persona": "A terse helper."}
    # The materialized controls the next attach delivers win over the frozen
    # copy, as they do in resolve_session_config.
    assert agent["interactive"] == {
        "permission_mode": "auto_accept",
        "narration_mode": "verbose",
    }
    encoded = response.text
    for secret in SECRETS:
        assert secret not in encoded


def test_a_stranger_is_refused_like_every_sibling_thread_route(monkeypatch):
    store = _Store(_thread(), _execution_row(_frozen_blob()))
    response = _client(monkeypatch, store, STRANGER).get(URL)
    assert response.status_code == 403
    assert store.queries == []
    for secret in SECRETS:
        assert secret not in response.text


def test_a_missing_thread_is_404(monkeypatch):
    store = _Store(None, None)
    response = _client(monkeypatch, store, OWNER).get(URL)
    assert response.status_code == 404
    assert store.queries == []


def test_a_session_from_before_snapshots_is_legacy_and_is_not_re_resolved(
    monkeypatch,
):
    store = _Store(_thread(metadata={"expert_id": "e-1"}), None)
    response = _client(monkeypatch, store, OWNER).get(URL)
    assert response.status_code == 200
    assert response.json() == {
        "thread_id": THREAD_ID,
        "source": "legacy",
        "config": None,
        "expert_based_on": None,
    }


def test_a_snapshot_this_harness_cannot_read_is_unavailable(monkeypatch):
    store = _Store(_thread(), _execution_row(_frozen_blob(), adapter="generic/v1"))
    response = _client(monkeypatch, store, OWNER).get(URL)
    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "unavailable" and body["config"] is None
    assert body["expert_based_on"] == "assistant"
