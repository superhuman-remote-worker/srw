"""What a personal access token needs for the content of a manifest request.

The route table asks ``resources:write`` for ``POST /api/manifests/apply`` and
``DELETE /api/resources/{resource_id}`` (``tests/test_pat_scope_policy.py``,
``tests/test_pat_scope_enforcement.py``). Two kinds of document do more than
change configuration, and only the request's content shows them, so the
service checks a second scope:

* a ``kind: Job`` document starts work: ``jobs:write``;
* a ``kind: Project`` document, and the delete of a Project resource, create,
  change or remove a project: ``projects:write``.

The apply check runs before the transaction opens. The fake database below
raises when a transaction is opened, which is how a test sees that the check
let a request through. Only the database and the role check are faked; the
parser and the scope check are the real ones.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.services import manifest_resources
from orchestrator.services.manifest_resources import ManifestResourceService

API_VERSION = "srw/v1alpha1"
EXPERT = {
    "apiVersion": API_VERSION,
    "kind": "Expert",
    "metadata": {"name": "worker"},
    "spec": {"runtime": {"image": "example/worker@sha256:" + "c" * 64}},
}
JOB = {
    "apiVersion": API_VERSION,
    "kind": "Job",
    "metadata": {"name": "assignment"},
    "spec": {
        "task": {"text": "Do the work"},
        "execution": {"expert": {"ref": {"name": "worker"}}},
    },
}
PROJECT = {
    "apiVersion": API_VERSION,
    "kind": "Project",
    "metadata": {"name": "team"},
    "spec": {"resources": {}},
}


class _TransactionOpened(Exception):
    """The request passed the scope check and reached the database."""


class _Database:
    def __init__(self) -> None:
        self.security_events: list[dict[str, Any]] = []

    async def record_security_event(self, **event: Any) -> None:
        self.security_events.append(event)

    def transaction_scope(self):
        raise _TransactionOpened


def _token(*scopes: str) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "is_admin": "admin" in scopes,
        "auth_method": "pat",
        "scopes": list(scopes),
    }


async def _apply(service: ManifestResourceService, documents, user):
    return await service.apply(json.dumps(documents), user, format="json")


class TestApply:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("documents", "scopes"),
        [
            ([EXPERT], ["resources:write"]),
            ([EXPERT, JOB], ["resources:write", "jobs:write"]),
            ([PROJECT], ["resources:write", "projects:write"]),
            ([PROJECT, EXPERT, JOB], ["jobs:write", "projects:write"]),
            # `admin` satisfies every scope check.
            ([PROJECT, JOB], ["admin"]),
        ],
    )
    async def test_a_token_with_the_scopes_reaches_the_apply(self, documents, scopes):
        database = _Database()
        with pytest.raises(_TransactionOpened):
            await _apply(ManifestResourceService(database), documents, _token(*scopes))
        assert database.security_events == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("documents", "scopes", "missing"),
        [
            ([JOB], ["resources:write"], "jobs:write"),
            ([EXPERT, JOB], ["resources:write", "projects:write"], "jobs:write"),
            ([PROJECT], ["resources:write"], "projects:write"),
            ([PROJECT], ["resources:write", "jobs:write"], "projects:write"),
            ([PROJECT, JOB], ["resources:write"], "jobs:write, projects:write"),
        ],
    )
    async def test_a_token_without_them_is_refused_before_anything_is_written(
        self, documents, scopes, missing
    ):
        database = _Database()
        with pytest.raises(HTTPException) as refused:
            await _apply(ManifestResourceService(database), documents, _token(*scopes))
        assert refused.value.status_code == 403
        assert refused.value.detail == f"Insufficient token scope: requires {missing}"
        # A refusal raised in a service leaves no trace unless the service
        # writes one; the route-level refusal is written by the resolver.
        (event,) = database.security_events
        assert event["event_type"] == "token_scope_denied"
        assert event["resource_type"] == "manifest"
        assert event["auth_method"] == "pat"
        assert f"token scopes: {sorted(scopes)}" in event["detail"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "user",
        [
            # A Cockpit session.
            {"id": uuid4(), "is_admin": False},
            # An MCP token, restricted to one project or not: governed by
            # role and row visibility, never by action scopes.
            {"id": uuid4(), "is_admin": False, "auth_method": "mcp"},
            {
                "id": uuid4(),
                "is_admin": False,
                "auth_method": "mcp",
                "scopes": [f"project:{uuid4()}"],
            },
        ],
    )
    async def test_other_credentials_are_not_checked(self, user):
        database = _Database()
        with pytest.raises(_TransactionOpened):
            await _apply(ManifestResourceService(database), [PROJECT, JOB], user)
        assert database.security_events == []


def _stored(kind: str) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "kind": kind,
        "linked_id": None,
        "document": {"kind": kind, "metadata": {"name": "x"}},
    }


@pytest.fixture
def role_check(monkeypatch):
    """The role check (``ManifestAuthority.resource``), admitting by default."""
    check = AsyncMock()
    monkeypatch.setattr(manifest_resources.ManifestAuthority, "resource", check)
    return check


def _service_holding(row: dict[str, Any]) -> tuple[ManifestResourceService, _Database]:
    database = _Database()
    service = ManifestResourceService(database)
    service.store.by_id = AsyncMock(return_value=row)
    service.store.delete = AsyncMock()
    return service, database


class TestDelete:
    @pytest.mark.asyncio
    async def test_a_project_needs_the_project_scope(self, role_check):
        row = _stored("Project")
        service, database = _service_holding(row)
        with pytest.raises(HTTPException) as refused:
            await service.delete(
                row["id"], _token("resources:write"), expected_version=1
            )
        assert refused.value.status_code == 403
        assert refused.value.detail == (
            "Insufficient token scope: requires projects:write"
        )
        service.store.delete.assert_not_awaited()
        (event,) = database.security_events
        assert event["event_type"] == "token_scope_denied"
        assert event["resource_id"] == str(row["id"])

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kind", "scopes"),
        [
            ("Project", ["resources:write", "projects:write"]),
            ("Project", ["admin"]),
            ("Expert", ["resources:write"]),
            # A stored Job's delete removes its definition only.
            ("Job", ["resources:write"]),
        ],
    )
    async def test_the_delete_proceeds(self, role_check, kind, scopes):
        row = _stored(kind)
        service, database = _service_holding(row)
        result = await service.delete(row["id"], _token(*scopes), expected_version=3)
        assert result == {"deleted": True, "uid": str(row["id"])}
        service.store.delete.assert_awaited_once_with(row, expected_version=3)
        assert database.security_events == []

    @pytest.mark.asyncio
    async def test_a_session_deletes_a_project_without_any_scope(self, role_check):
        row = _stored("Project")
        service, _ = _service_holding(row)
        await service.delete(
            row["id"], {"id": uuid4(), "is_admin": False}, expected_version=1
        )
        service.store.delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_role_check_answers_first(self, role_check):
        # A resource the caller may not write answers as it did before the
        # scope existed, so the scope check tells a token nothing about it.
        role_check.side_effect = HTTPException(404, "Resource does not exist.")
        row = _stored("Project")
        service, database = _service_holding(row)
        with pytest.raises(HTTPException) as refused:
            await service.delete(
                row["id"], _token("resources:write"), expected_version=1
            )
        assert refused.value.status_code == 404
        assert database.security_events == []
