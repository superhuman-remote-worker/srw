"""Delegated-agent boundary for presenting the current shared browser."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from orchestrator.routers import canvases
from orchestrator.services.canvas import (
    BrowserSource,
    CanvasCapabilities,
    CanvasMutation,
    CanvasRecord,
    build_public_canvas_representation,
)
from orchestrator.services.canvas_ssh import RemoteWorkspaceTarget
from orchestrator.services.shared_browser_canvas import (
    BrowserCapabilityResponse,
    PreparedBrowser,
)

THREAD_ID = "a3333333-3333-3333-8333-333333333333"
USER_ID = "b4444444-4444-4444-8444-444444444444"
WORKSPACE_GENERATION = UUID("11111111-aaaa-4aaa-8aaa-111111111111")
BROWSER_GENERATION = UUID("5f0a9f5e-0000-4000-8000-000000000001")
NOW = datetime(2026, 7, 22, tzinfo=timezone.utc)


def _thread() -> dict:
    return {
        "id": THREAD_ID,
        "user_id": USER_ID,
        "metadata": {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "_workspace_binding": {
                "generation": str(WORKSPACE_GENERATION),
                "kind": "remote",
                "backing_id": "workspace-a",
                "ssh_host_key_fingerprint": "SHA256:test",
            },
            "workspace_container": {
                "status": "ready",
                "ssh_host": "workspace.test",
                "ssh_port": 30022,
                "_canvas_workspace_generation": str(WORKSPACE_GENERATION),
            },
        },
    }


def _target() -> RemoteWorkspaceTarget:
    return RemoteWorkspaceTarget(
        thread_id=THREAD_ID,
        generation=WORKSPACE_GENERATION,
        host="workspace.test",
        port=30022,
        fingerprint="SHA256:test",
    )


def _record(*, revision: int = 1, title: str = "Shared browser") -> CanvasRecord:
    return CanvasRecord(
        thread_id=THREAD_ID,
        canvas_id="main",
        source=BrowserSource(browser_generation=BROWSER_GENERATION),
        title=title,
        renderer="auto",
        editable=False,
        alt_text=None,
        presentation_revision=revision,
        source_fingerprint=None,
        source_version=None,
        origin_generation=None,
        created_at=NOW,
        updated_at=NOW,
    )


def _capability(*, enabled: bool = True) -> BrowserCapabilityResponse:
    return BrowserCapabilityResponse(
        feature_enabled=enabled,
        can_open_browser=enabled,
        workspace_ready=enabled,
        reason=None if enabled else "feature_disabled",
    )


class _RouteDB:
    def __init__(self) -> None:
        self.thread = _thread()

    async def get_thread(self, thread_id: str):
        assert thread_id == THREAD_ID
        return dict(self.thread)


@pytest.fixture()
def route(monkeypatch):
    db = _RouteDB()
    state = SimpleNamespace(
        mutation=CanvasMutation(changed=True, record=_record()),
        owner_calls=[],
        prepare_calls=[],
        commit_calls=[],
    )

    async def delegated_owner(request, received_db, thread_id):
        del request
        assert received_db is db
        state.owner_calls.append(thread_id)
        return {"id": USER_ID}, dict(db.thread)

    async def prepare(thread, *, initial_baton, generation_resolver):
        state.prepare_calls.append(
            {
                "thread": thread,
                "initial_baton": initial_baton,
                "resolved": await generation_resolver(),
            }
        )
        return PreparedBrowser(BROWSER_GENERATION, _target())

    async def commit(received_db, thread_id, prepared, *, title):
        state.commit_calls.append(
            {
                "db": received_db,
                "thread_id": thread_id,
                "prepared": prepared,
                "title": title,
            }
        )
        return state.mutation

    async def represent(thread_id, thread, record, **kwargs):
        del thread_id, thread, kwargs
        return build_public_canvas_representation(
            record,
            status="ready",
            capabilities=CanvasCapabilities(
                can_pop_out=True,
                can_take_control=True,
                can_stream_browser=True,
            ),
        )

    monkeypatch.setattr(canvases, "_require_delegated_owner", delegated_owner)
    monkeypatch.setattr(canvases, "browser_capability", lambda thread: _capability())
    monkeypatch.setattr(canvases, "prepare_browser_canvas", prepare)
    monkeypatch.setattr(canvases, "commit_browser_canvas", commit)
    monkeypatch.setattr(canvases, "_represent", represent)
    app = FastAPI()
    app.state.store = db
    app.include_router(canvases.internal_router)
    state.client = TestClient(app)
    state.db = db
    return state


def _set(route, **payload):
    return route.client.post(
        f"/api/internal/persistent/threads/{THREAD_ID}/canvases/main/set",
        headers={"X-Internal-Key": "test", "X-MCP-User-Id": USER_ID},
        json={
            "source_type": "browser",
            "browser_id": "current",
            **payload,
        },
    )


def test_internal_browser_set_uses_agent_baton_and_returns_redacted_state(route):
    route.mutation = CanvasMutation(
        changed=True,
        record=_record(revision=3, title="Research browser"),
    )

    response = _set(route, title="  Research browser  ")

    assert response.status_code == 200
    assert response.headers["etag"].startswith('"canvas:3:')
    assert response.headers["x-canvas-mutation-changed"] == "true"
    assert response.json()["source"] == {"type": "browser"}
    assert response.json()["capabilities"]["can_stream_browser"] is True
    assert str(BROWSER_GENERATION) not in response.text
    assert "workspace.test" not in response.text
    assert route.prepare_calls == [
        {
            "thread": _thread(),
            "initial_baton": "agent",
            "resolved": _thread(),
        }
    ]
    assert route.commit_calls[0]["title"] == "Research browser"
    assert route.owner_calls == [THREAD_ID, THREAD_ID, THREAD_ID]


def test_internal_browser_set_reports_idempotent_repeat_without_revision(route):
    route.mutation = CanvasMutation(changed=False, record=_record(revision=1))

    response = _set(route)

    assert response.status_code == 200
    assert response.headers["x-canvas-mutation-changed"] == "false"
    assert response.json()["presentation_revision"] == 1


def test_internal_browser_set_rechecks_capability_before_commit(route, monkeypatch):
    monkeypatch.setattr(
        canvases,
        "browser_capability",
        lambda thread: _capability(enabled=False),
    )

    response = _set(route)

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "feature_disabled"
    assert route.prepare_calls[0]["initial_baton"] == "agent"
    assert route.commit_calls == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"browser_id": None},
        {"path": "report.md"},
        {"port": 8501},
        {"entry_path": "/"},
        {"alt_text": "browser"},
        {"renderer": "html"},
        {"editable": True},
        {"new_app": True},
    ],
)
def test_internal_browser_request_rejects_missing_or_cross_kind_fields(
    route, overrides
):
    response = _set(route, **overrides)

    assert response.status_code == 422
    assert route.prepare_calls == []


def test_file_and_port_requests_reject_browser_selector(route):
    url = f"/api/internal/persistent/threads/{THREAD_ID}/canvases/main/set"
    headers = {"X-Internal-Key": "test", "X-MCP-User-Id": USER_ID}

    file_response = route.client.post(
        url,
        headers=headers,
        json={
            "source_type": "workspace_file",
            "path": "report.md",
            "browser_id": "current",
        },
    )
    port_response = route.client.post(
        url,
        headers=headers,
        json={
            "source_type": "workspace_port",
            "port": 8501,
            "browser_id": "current",
        },
    )

    assert file_response.status_code == 422
    assert port_response.status_code == 422
    assert route.prepare_calls == []


# ---------------------------------------------------------------------------
# Same-cluster VM Sessions (WP7): the agent's set_canvas(browser) end to end
# through the real capability, prepare and commit.
# ---------------------------------------------------------------------------

from orchestrator.services import shared_browser_canvas as browser_canvas  # noqa: E402
from tests._canvas_vm_fixtures import vm_context, vm_thread  # noqa: E402


@pytest.mark.parametrize(
    ("mode", "vm", "status", "code"),
    [
        ("same-cluster", vm_context(), 200, None),
        ("external", vm_context(ssh_host="100.64.1.2"), 503, "workspace_unroutable"),
        (
            "same-cluster",
            vm_context(identity_authenticated=False),
            409,
            "workspace_unattested",
        ),
    ],
)
def test_agent_presents_the_vm_browser(monkeypatch, mode, vm, status, code):
    monkeypatch.setenv("VM_MODE", mode)
    monkeypatch.setenv("CANVAS_SHARED_BROWSER_ENABLED", "true")
    monkeypatch.setattr(browser_canvas, "resolve_ssh_key_path", lambda: "/tmp/key")
    monkeypatch.setattr(browser_canvas, "asyncssh", object())
    thread = vm_thread(vm, user_id=USER_ID, thread_id=THREAD_ID)
    started: list = []
    proofs: list = []
    writes: list = []

    class DB:
        async def get_thread(self, thread_id):
            assert thread_id == THREAD_ID
            return dict(thread)

    db = DB()

    async def delegated_owner(request, received_db, thread_id):
        del request, received_db, thread_id
        return {"id": USER_ID}, dict(thread)

    async def exec_info(received, *, initial_baton, generation_resolver):
        started.append((received["id"], initial_baton))
        assert (await generation_resolver())["id"] == THREAD_ID
        return {
            "generation": str(BROWSER_GENERATION),
            "token": "private",
            "port": 38801,
            "baton": "agent",
        }

    async def attest(target):
        proofs.append(target)
        return "current"

    class Service:
        def __init__(self, received_db):
            assert received_db is db

        async def set_if_changed(
            self, thread_id, presentation, *, expected_presentation_revision
        ):
            del expected_presentation_revision
            writes.append((thread_id, presentation.source))
            return CanvasMutation(changed=True, record=_record())

    monkeypatch.setattr(canvases, "_require_delegated_owner", delegated_owner)
    monkeypatch.setattr(browser_canvas, "exec_stream_info", exec_info)
    monkeypatch.setattr(browser_canvas, "attest_vm_workspace_target", attest)
    monkeypatch.setattr(browser_canvas, "CanvasService", Service)
    app = FastAPI()
    app.state.store = db
    app.include_router(canvases.internal_router)

    response = TestClient(app).post(
        f"/api/internal/persistent/threads/{THREAD_ID}/canvases/main/set",
        headers={"X-Internal-Key": "test", "X-MCP-User-Id": USER_ID},
        json={"source_type": "browser", "browser_id": "current"},
    )

    assert response.status_code == status
    if code is None:
        assert started == [(THREAD_ID, "agent")]
        assert len(proofs) == 1 and proofs[0].vm_runtime is not None
        assert writes == [
            (THREAD_ID, BrowserSource(browser_generation=BROWSER_GENERATION))
        ]
        body = response.json()
        assert body["status"] == "ready"
        assert body["capabilities"]["can_stream_browser"] is True
        for private in ("10.42.1.23", "SHA256:", "private"):
            assert private not in response.text
    else:
        assert response.json()["detail"]["code"] == code
        assert started == [] and writes == []
