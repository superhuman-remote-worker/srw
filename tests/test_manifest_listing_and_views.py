"""Listing stored resources by name, and what a resource view says of itself.

``GET /api/resources`` takes ``name``, refuses an unknown ``kind`` and reports
``truncated``. A resource view carries ``redacted`` (whether the reader got
less than the stored document) and the row facts a table needs: ``linkedId``,
``managedBy``, ``createdAt``, ``updatedAt``.

The store's SQL is exercised on real PostgreSQL in
``tests/test_manifest_resources.py``; here the store is faked.
"""

from datetime import UTC, datetime
import json
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

from httpx import ASGITransport, AsyncClient
import pytest

from orchestrator.services import manifest_resources
from orchestrator.services.manifest_resources import _for_reader
from orchestrator.services.manifest_store import LIST_LIMIT, resource_view
from tests.test_manifest_http import (
    RESOURCE_SECRETS,
    TestResourceReadRedaction,
    make_app,
    manifest,
)

PROJECT_ID = TestResourceReadRedaction.PROJECT_ID
OWNER_ID = TestResourceReadRedaction.OWNER_ID
VIEWER_ID = TestResourceReadRedaction.VIEWER_ID
PROJECT_SCOPE = f"/api/resources?scope_kind=Project&scope_name={PROJECT_ID}"


async def _get(app, path):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.get(path)


def _project_app(caller_id, role):
    """A Project scope holding a Project and its child Expert, both with
    secrets in their documents, read by ``caller_id`` in ``role``."""
    return TestResourceReadRedaction()._app(caller_id, role)


class TestListRoute:
    @pytest.mark.asyncio
    async def test_kind_and_name_reach_the_service(self):
        resources = Mock()
        resources.list = AsyncMock(return_value={"resources": [], "truncated": False})
        response = await _get(
            make_app(resources=resources),
            "/api/resources?kind=WorkspaceTemplate&name=python-dev",
        )
        assert response.status_code == 200, response.text
        called = resources.list.await_args.kwargs
        assert (called["kind"], called["name"]) == ("WorkspaceTemplate", "python-dev")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "query", ["kind=Workspace", "kind=expert", "name=", "name=" + "x" * 254]
    )
    async def test_a_filter_that_can_match_nothing_is_refused(self, query):
        # An empty list would read as "none stored".
        resources = Mock()
        response = await _get(make_app(resources=resources), f"/api/resources?{query}")
        assert response.status_code == 422
        assert not resources.mock_calls

    @pytest.mark.asyncio
    async def test_the_refusal_names_the_kinds(self):
        response = await _get(make_app(resources=Mock()), "/api/resources?kind=Nope")
        assert response.json()["detail"] == (
            "Unknown resource kind. Choose one of: Expert, WorkspaceTemplate, "
            "Connector, Project, Job."
        )


class TestTruncation:
    @pytest.mark.asyncio
    async def test_a_complete_list_says_so(self):
        app, _, _ = _project_app(OWNER_ID, "owner")
        store = app.state.manifest_dependencies_factory().resources.store
        listing = await _get(app, PROJECT_SCOPE + "&kind=Expert&name=worker")
        assert listing.status_code == 200, listing.text
        assert listing.json()["truncated"] is False
        # One row past the limit, so the count is the store's, not the
        # visibility filter's.
        called = store.list_scope.await_args.kwargs
        assert called == {"kind": "Expert", "name": "worker", "limit": LIST_LIMIT + 1}

    @pytest.mark.asyncio
    async def test_a_scope_with_more_rows_than_one_call_returns(self, monkeypatch):
        monkeypatch.setattr(manifest_resources, "LIST_LIMIT", 1)
        app, project_row, _ = _project_app(OWNER_ID, "owner")
        listing = await _get(app, PROJECT_SCOPE)
        body = listing.json()
        assert body["truncated"] is True
        assert [item["uid"] for item in body["resources"]] == [str(project_row["id"])]


class TestRedactedFlag:
    @pytest.mark.asyncio
    async def test_a_viewer_is_told_the_view_is_redacted(self):
        app, project_row, child_row = _project_app(VIEWER_ID, "viewer")
        for row in (project_row, child_row):
            response = await _get(app, f"/api/resources/{row['id']}")
            assert response.json()["redacted"] is True
            for secret in RESOURCE_SECRETS:
                assert secret not in response.text
        listing = await _get(app, PROJECT_SCOPE)
        assert [item["redacted"] for item in listing.json()["resources"]] == [
            True,
            True,
        ]

    @pytest.mark.asyncio
    async def test_the_owner_reads_the_stored_document(self):
        app, project_row, child_row = _project_app(OWNER_ID, "owner")
        for row in (project_row, child_row):
            body = (await _get(app, f"/api/resources/{row['id']}")).json()
            assert body["redacted"] is False
            assert body["resource"] == row["document"]

    def test_nothing_hidden_is_not_redacted(self):
        # A reader below owner whose document holds nothing to hide got the
        # stored document, and may export it.
        view = _for_reader({"resource": manifest()}, False)
        assert view == {"resource": manifest(), "redacted": False}

    def test_the_stored_document_is_not_changed_by_redaction(self):
        _, project_row, _ = _project_app(VIEWER_ID, "viewer")
        before = json.dumps(project_row["document"], sort_keys=True)
        view = _for_reader({"resource": project_row["document"]}, False)
        assert view["redacted"] is True
        assert json.dumps(project_row["document"], sort_keys=True) == before


def test_a_view_carries_the_row_facts_as_json():
    linked, manager = uuid4(), uuid4()
    stamp = datetime(2026, 11, 17, 9, 30, tzinfo=UTC)
    row = {
        "id": uuid4(),
        "document": manifest(),
        "resource_version": 3,
        "revision": "sha256:" + "a" * 64,
        "linked_id": linked,
        "managed_by": manager,
        "created_at": stamp,
        "updated_at": stamp,
    }
    view = resource_view(row)
    assert view["linkedId"] == str(linked)
    assert view["managedBy"] == str(manager)
    assert view["createdAt"] == view["updatedAt"] == "2026-11-17T09:30:00+00:00"
    # An apply stores its views as the operation's result.
    json.dumps(view)
    bare = resource_view({**row, "linked_id": None, "managed_by": None})
    assert bare["linkedId"] is None and bare["managedBy"] is None
