"""Authoritative shared-browser open and prepare/commit contracts."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from orchestrator.routers import shared_browser as routes
from orchestrator.services import shared_browser_canvas as browser_canvas
from orchestrator.services.canvas import (
    BrowserSource,
    CanvasCapabilities,
    CanvasMutation,
    CanvasRecord,
    build_public_canvas_representation,
)
from orchestrator.services.canvas_ssh import CanvasSSHError, RemoteWorkspaceTarget
from orchestrator.services.shared_browser_canvas import (
    BrowserCapabilityResponse,
    PreparedBrowser,
)

_THREAD_ID = "a3333333-3333-3333-3333-333333333333"
_WORKSPACE_GENERATION = UUID("11111111-aaaa-4aaa-8aaa-111111111111")
_BROWSER_GENERATION = UUID("5f0a9f5e-0000-4000-8000-000000000001")
_NEXT_BROWSER_GENERATION = UUID("6f0a9f5e-0000-4000-8000-000000000002")
_NOW = datetime(2026, 7, 22, tzinfo=timezone.utc)


def _thread(*, ready: bool = True) -> dict:
    metadata: dict = {
        "config_override": {"workspace": {"backend": "sandbox"}},
    }
    if ready:
        metadata.update(
            {
                "_workspace_binding": {
                    "generation": str(_WORKSPACE_GENERATION),
                    "kind": "remote",
                    "backing_id": "workspace-a",
                    "ssh_host_key_fingerprint": "SHA256:test",
                },
                "workspace_container": {
                    "status": "ready",
                    "ssh_host": "workspace.test",
                    "ssh_port": 30022,
                    "_canvas_workspace_generation": str(_WORKSPACE_GENERATION),
                },
            }
        )
    return {"id": _THREAD_ID, "user_id": "user-1", "metadata": metadata}


def _target() -> RemoteWorkspaceTarget:
    return RemoteWorkspaceTarget(
        thread_id=_THREAD_ID,
        generation=_WORKSPACE_GENERATION,
        host="workspace.test",
        port=30022,
        fingerprint="SHA256:test",
    )


def _record(
    *,
    revision: int = 1,
    generation: UUID = _BROWSER_GENERATION,
    title: str = "Shared browser",
) -> CanvasRecord:
    return CanvasRecord(
        thread_id=_THREAD_ID,
        canvas_id="main",
        source=BrowserSource(browser_generation=generation),
        title=title,
        renderer="auto",
        editable=False,
        alt_text=None,
        presentation_revision=revision,
        source_fingerprint=None,
        source_version=None,
        origin_generation=None,
        created_at=_NOW,
        updated_at=_NOW,
    )


def _capability(*, ready: bool = True) -> BrowserCapabilityResponse:
    return BrowserCapabilityResponse(
        feature_enabled=True,
        can_open_browser=True,
        workspace_ready=ready,
        reason=None,
    )


class _RouteDB:
    def __init__(self, thread: dict) -> None:
        self.thread = thread

    async def get_thread(self, thread_id: str):
        assert thread_id == _THREAD_ID
        return dict(self.thread)


@pytest.fixture()
def route_client(monkeypatch):
    db = _RouteDB(_thread())
    owner_calls: list[str] = []
    prepared_calls: list[dict] = []
    committed_calls: list[dict] = []
    mutation = CanvasMutation(changed=True, record=_record())

    async def owner(request, received_db, thread_id):
        del request
        assert received_db is db
        owner_calls.append(thread_id)
        return {"id": "user-1"}, dict(db.thread)

    async def prepare(thread, *, initial_baton, generation_resolver):
        prepared_calls.append(
            {
                "thread": thread,
                "initial_baton": initial_baton,
                "resolved": await generation_resolver(),
            }
        )
        return PreparedBrowser(_BROWSER_GENERATION, _target())

    async def commit(
        received_db,
        thread_id,
        prepared,
        *,
        title,
        expected_presentation_revision,
    ):
        committed_calls.append(
            {
                "db": received_db,
                "thread_id": thread_id,
                "prepared": prepared,
                "title": title,
                "expected_presentation_revision": expected_presentation_revision,
            }
        )
        return mutation

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

    monkeypatch.setattr(routes, "require_thread_owner", owner)
    monkeypatch.setattr(routes, "browser_capability", lambda thread: _capability())
    monkeypatch.setattr(routes, "prepare_browser_canvas", prepare)
    monkeypatch.setattr(routes, "commit_browser_canvas", commit)
    monkeypatch.setattr(routes, "_represent", represent)
    app = FastAPI()
    app.state.store = db
    kicked: list[tuple[str, object]] = []
    # The application's one collaborator for this router (R1.B12): the
    # workspace reconcile the cold ``open`` path kicks without waiting.
    app.state.shared_browser_dependencies_factory = lambda: (
        routes.SharedBrowserDependencies(
            kick_workspace_provisioning=lambda thread_id, received_db: kicked.append(
                (thread_id, received_db)
            )
        )
    )
    app.include_router(routes.router)
    return SimpleNamespace(
        kicked=kicked,
        client=TestClient(app),
        db=db,
        owner_calls=owner_calls,
        prepared_calls=prepared_calls,
        committed_calls=committed_calls,
    )


def test_ready_open_returns_redacted_canvas_state_and_headers(
    route_client, monkeypatch
):
    mutation = CanvasMutation(
        changed=True,
        record=_record(revision=3, title="Research browser"),
    )

    async def commit(
        received_db,
        thread_id,
        prepared,
        *,
        title,
        expected_presentation_revision,
    ):
        route_client.committed_calls.append(
            {
                "db": received_db,
                "thread_id": thread_id,
                "prepared": prepared,
                "title": title,
                "expected_presentation_revision": expected_presentation_revision,
            }
        )
        return mutation

    monkeypatch.setattr(routes, "commit_browser_canvas", commit)

    response = route_client.client.post(
        f"/api/persistent/threads/{_THREAD_ID}/browser/open",
        json={
            "title": "  Research browser  ",
            "expected_presentation_revision": 2,
        },
    )

    assert response.status_code == 200
    assert response.headers["etag"].startswith('"canvas:3:')
    assert response.headers["x-canvas-mutation-changed"] == "true"
    assert response.headers["cache-control"] == "private, no-cache"
    body = response.json()
    assert body["source"] == {"type": "browser"}
    assert body["status"] == "ready"
    assert body["presentation_revision"] == 3
    assert body["capabilities"] == {
        "can_edit": False,
        "can_pop_out": True,
        "can_take_control": True,
        "can_create_viewer_session": False,
        "can_stream_browser": True,
        "can_view_office": False,
    }
    serialized = response.text
    assert str(_BROWSER_GENERATION) not in serialized
    assert "38801" not in serialized
    assert route_client.prepared_calls[0]["initial_baton"] == "user"
    assert route_client.committed_calls[-1]["title"] == "Research browser"
    assert route_client.committed_calls[-1]["expected_presentation_revision"] == 2
    assert route_client.owner_calls == [_THREAD_ID, _THREAD_ID, _THREAD_ID]


def test_idempotent_open_reports_false_without_a_new_revision(
    route_client, monkeypatch
):
    async def unchanged(*args, **kwargs):
        del args, kwargs
        return CanvasMutation(changed=False, record=_record(revision=1))

    monkeypatch.setattr(routes, "commit_browser_canvas", unchanged)

    response = route_client.client.post(
        f"/api/persistent/threads/{_THREAD_ID}/browser/open",
        json={},
    )

    assert response.status_code == 200
    assert response.headers["x-canvas-mutation-changed"] == "false"
    assert response.json()["presentation_revision"] == 1


def test_public_open_rejects_caller_selected_baton(route_client):
    response = route_client.client.post(
        f"/api/persistent/threads/{_THREAD_ID}/browser/open",
        json={"opened_by": "agent"},
    )

    assert response.status_code == 422
    assert route_client.prepared_calls == []


def test_cold_open_kicks_provisioning_with_retry_hint(route_client, monkeypatch):
    route_client.db.thread = _thread(ready=False)
    monkeypatch.setattr(
        routes,
        "browser_capability",
        lambda thread: _capability(ready=False),
    )

    response = route_client.client.post(
        f"/api/persistent/threads/{_THREAD_ID}/browser/open",
        json={},
    )

    assert response.status_code == 202
    assert response.json() == {"status": "provisioning"}
    assert response.headers["retry-after"] == "1"
    assert route_client.kicked == [(_THREAD_ID, route_client.db)]
    assert route_client.prepared_calls == []


def test_post_prepare_capability_change_prevents_commit(route_client, monkeypatch):
    calls = 0

    def changing_capability(thread):
        nonlocal calls
        del thread
        calls += 1
        if calls == 1:
            return _capability()
        return BrowserCapabilityResponse(
            feature_enabled=False,
            can_open_browser=False,
            workspace_ready=False,
            reason="feature_disabled",
        )

    monkeypatch.setattr(routes, "browser_capability", changing_capability)

    response = route_client.client.post(
        f"/api/persistent/threads/{_THREAD_ID}/browser/open",
        json={},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "feature_disabled"
    assert route_client.committed_calls == []


def test_post_prepare_owner_change_prevents_commit(route_client, monkeypatch):
    calls = 0

    async def changing_owner(request, db, thread_id):
        nonlocal calls
        del request
        assert db is route_client.db
        assert thread_id == _THREAD_ID
        calls += 1
        if calls > 1:
            raise HTTPException(status_code=404, detail="Thread not found")
        return {"id": "user-1"}, dict(route_client.db.thread)

    monkeypatch.setattr(routes, "require_thread_owner", changing_owner)

    response = route_client.client.post(
        f"/api/persistent/threads/{_THREAD_ID}/browser/open",
        json={},
    )

    assert response.status_code == 404
    assert route_client.committed_calls == []


@pytest.mark.asyncio
async def test_prepare_validates_uuid_and_forwards_creation_baton(monkeypatch):
    captured = []

    async def exec_info(thread, *, initial_baton, generation_resolver):
        captured.append((thread, initial_baton, await generation_resolver()))
        return {
            "generation": str(_BROWSER_GENERATION),
            "token": "private",
            "port": 38801,
            "baton": "agent",
        }

    monkeypatch.setattr(
        browser_canvas, "browser_capability", lambda thread: _capability()
    )
    monkeypatch.setattr(browser_canvas, "exec_stream_info", exec_info)
    current = _thread()

    async def resolver():
        return current

    prepared = await browser_canvas.prepare_browser_canvas(
        current,
        initial_baton="agent",
        generation_resolver=resolver,
    )

    assert prepared.browser_generation == _BROWSER_GENERATION
    assert prepared.workspace_target == _target()
    assert captured == [(current, "agent", current)]


@pytest.mark.asyncio
async def test_prepare_rejects_noncanonical_browser_generation(monkeypatch):
    async def exec_info(thread, *, initial_baton, generation_resolver):
        del thread, initial_baton
        await generation_resolver()
        return {
            "generation": str(_BROWSER_GENERATION).upper(),
            "token": "private",
            "port": 38801,
            "baton": "user",
        }

    monkeypatch.setattr(
        browser_canvas, "browser_capability", lambda thread: _capability()
    )
    monkeypatch.setattr(browser_canvas, "exec_stream_info", exec_info)
    current = _thread()

    async def resolver():
        return current

    with pytest.raises(browser_canvas.BrowserCanvasError) as error:
        await browser_canvas.prepare_browser_canvas(
            current,
            initial_baton="user",
            generation_resolver=resolver,
        )

    assert error.value.code == "browser_identity_invalid"


@pytest.mark.asyncio
async def test_reopen_does_not_assume_the_requested_baton_was_applied(monkeypatch):
    daemon = {"generation": None, "baton": None}

    async def exec_info(thread, *, initial_baton, generation_resolver):
        del thread
        await generation_resolver()
        if daemon["generation"] is None:
            daemon["generation"] = str(_BROWSER_GENERATION)
            daemon["baton"] = initial_baton
        return {
            "generation": daemon["generation"],
            "token": "private",
            "port": 38801,
            "baton": daemon["baton"],
        }

    monkeypatch.setattr(
        browser_canvas, "browser_capability", lambda thread: _capability()
    )
    monkeypatch.setattr(browser_canvas, "exec_stream_info", exec_info)
    current = _thread()

    async def resolver():
        return current

    first = await browser_canvas.prepare_browser_canvas(
        current,
        initial_baton="agent",
        generation_resolver=resolver,
    )
    repeated = await browser_canvas.prepare_browser_canvas(
        current,
        initial_baton="user",
        generation_resolver=resolver,
    )

    assert first.browser_generation == repeated.browser_generation
    assert daemon["baton"] == "agent"


@pytest.mark.asyncio
async def test_commit_revalidates_target_before_writing(monkeypatch):
    db = _RouteDB(_thread())
    calls = []

    class FakeCanvasService:
        def __init__(self, received_db):
            assert received_db is db

        async def set_if_changed(
            self, thread_id, presentation, *, expected_presentation_revision
        ):
            calls.append((thread_id, presentation, expected_presentation_revision))
            return CanvasMutation(changed=True, record=_record())

    monkeypatch.setattr(
        browser_canvas, "browser_capability", lambda thread: _capability()
    )
    monkeypatch.setattr(browser_canvas, "CanvasService", FakeCanvasService)

    mutation = await browser_canvas.commit_browser_canvas(
        db,
        _THREAD_ID,
        PreparedBrowser(_BROWSER_GENERATION, _target()),
        title="  Shared browser  ",
    )

    assert mutation.changed is True
    assert calls[0][0] == _THREAD_ID
    presentation = calls[0][1]
    assert presentation.source == BrowserSource(browser_generation=_BROWSER_GENERATION)
    assert presentation.title == "Shared browser"
    assert presentation.renderer == "auto"
    assert presentation.editable is False
    assert presentation.alt_text is None
    assert presentation.source_version is None
    assert presentation.new_app is False
    assert calls[0][2] is None


@pytest.mark.asyncio
async def test_commit_maps_a_stale_canvas_revision_to_a_typed_conflict(monkeypatch):
    db = _RouteDB(_thread())

    class FakeCanvasService:
        def __init__(self, received_db):
            assert received_db is db

        async def set_if_changed(self, *args, **kwargs):
            del args, kwargs
            raise browser_canvas.CanvasPreconditionFailed("stale")

    monkeypatch.setattr(
        browser_canvas, "browser_capability", lambda thread: _capability()
    )
    monkeypatch.setattr(browser_canvas, "CanvasService", FakeCanvasService)

    with pytest.raises(browser_canvas.BrowserCanvasError) as error:
        await browser_canvas.commit_browser_canvas(
            db,
            _THREAD_ID,
            PreparedBrowser(_BROWSER_GENERATION, _target()),
            title="Shared browser",
            expected_presentation_revision=7,
        )

    assert error.value.status_code == 409
    assert error.value.code == "canvas_presentation_changed"


@pytest.mark.asyncio
async def test_commit_rejects_workspace_change_without_writing(monkeypatch):
    db = _RouteDB(_thread())
    written = False

    class FakeCanvasService:
        def __init__(self, received_db):
            del received_db

        async def set_if_changed(
            self, thread_id, presentation, *, expected_presentation_revision
        ):
            nonlocal written
            del thread_id, presentation, expected_presentation_revision
            written = True

    def changed(thread, target):
        del thread, target
        raise CanvasSSHError(
            409,
            "workspace_generation_changed",
            "changed",
        )

    monkeypatch.setattr(
        browser_canvas, "browser_capability", lambda thread: _capability()
    )
    monkeypatch.setattr(browser_canvas, "require_same_remote_workspace", changed)
    monkeypatch.setattr(browser_canvas, "CanvasService", FakeCanvasService)

    with pytest.raises(browser_canvas.BrowserCanvasError) as error:
        await browser_canvas.commit_browser_canvas(
            db,
            _THREAD_ID,
            PreparedBrowser(_NEXT_BROWSER_GENERATION, _target()),
            title="Shared browser",
        )

    assert error.value.code == "workspace_generation_changed"
    assert written is False
