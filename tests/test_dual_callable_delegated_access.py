"""The dual-callable job routes authorize a delegated MCP user (N13).

``approve``, ``resume``, ``cancel`` and ``pause`` are dual-callable: an
approved user with job access, or the agent runtime with ``X-Internal-Key``.
The MCP server, and the session and officer tool lanes, send that key
*together with* ``X-MCP-User-Id`` on every call, and the gate let any such
call act on any job by id. A delegated call now passes ``require_job_access``
as that user; only the bare key (the agent runtime) skips user auth.

Both routers are mounted with the real gates and real MCP-header auth; only
the database and the operations behind the gate are fakes.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.routers import job_controls as control_routes
from orchestrator.routers import job_lifecycle as lifecycle_routes
from orchestrator.security import access
from tests._mounted_router import mount_router

INTERNAL_KEY = "test-internal-key"
JOB_ID = "00000000-0000-0000-0000-00000000a001"
PROJECT_ID = "00000000-0000-0000-0000-00000000b001"
OTHER_PROJECT_ID = "00000000-0000-0000-0000-00000000b002"
OWNER_ID = "00000000-0000-0000-0000-00000000c001"
MEMBER_ID = "00000000-0000-0000-0000-00000000c002"
STRANGER_ID = "00000000-0000-0000-0000-00000000c003"
ADMIN_ID = "00000000-0000-0000-0000-00000000c004"

USERS = {
    OWNER_ID: {"id": OWNER_ID, "is_admin": False, "is_approved": True},
    MEMBER_ID: {"id": MEMBER_ID, "is_admin": False, "is_approved": True},
    STRANGER_ID: {"id": STRANGER_ID, "is_admin": False, "is_approved": True},
    ADMIN_ID: {"id": ADMIN_ID, "is_admin": True, "is_approved": True},
}

#: (method, path, operation the route calls past the gate)
ROUTES = {
    "approve": ("post", f"/api/jobs/{JOB_ID}/approve", "approve_job"),
    "resume": ("post", f"/api/jobs/{JOB_ID}/resume", "resume_job"),
    "cancel": ("put", f"/api/jobs/{JOB_ID}/cancel", "cancel"),
    "pause": ("put", f"/api/jobs/{JOB_ID}/pause", "pause"),
}


def _store():
    job = {"id": JOB_ID, "user_id": OWNER_ID, "project_id": PROJECT_ID}

    async def get_user(user_id):
        row = USERS.get(str(user_id))
        return dict(row) if row else None

    async def role(project_id, user_id):
        if str(project_id) == PROJECT_ID and str(user_id) == MEMBER_ID:
            return "viewer"
        return None

    return SimpleNamespace(
        get_job=AsyncMock(
            side_effect=lambda job_id: dict(job) if job_id == JOB_ID else None
        ),
        get_user=AsyncMock(side_effect=get_user),
        get_user_role_in_project=AsyncMock(side_effect=role),
        record_security_event=AsyncMock(),
    )


@pytest.fixture
def wire(monkeypatch):
    monkeypatch.setattr(access, "_INTERNAL_KEY", INTERNAL_KEY)
    monkeypatch.setenv("MCP_INTERNAL_KEY", INTERNAL_KEY)
    store = _store()
    operations = SimpleNamespace(
        approve_job=AsyncMock(return_value={"status": "completed"}),
        resume_job=AsyncMock(return_value={"status": "resumed"}),
        cancel=AsyncMock(return_value={"status": "cancelled"}),
        pause=AsyncMock(return_value={"status": "paused"}),
    )
    controls = control_routes.JobControlRouteDependencies(
        operations=operations,
        store=store,
        require_admin=AsyncMock(side_effect=AssertionError("admin gate")),
        require_job_access=access.require_job_access,
        require_internal_or_job_access=access.require_internal_or_job_access,
        require_approved_user=AsyncMock(side_effect=AssertionError("user gate")),
        require_sudo_request_authority=AsyncMock(),
        user_can_access_job_or_thread=AsyncMock(),
        mcp_scope_project_id=access.mcp_scope_project_id,
    )
    mutations = lifecycle_routes.JobControlRouteDependencies(
        operations=operations,
        store=store,
        require_job_access=access.require_job_access,
        require_internal_or_job_access=access.require_internal_or_job_access,
        require_internal=access.require_internal,
    )
    app = mount_router(
        control_routes.router,
        lifecycle_routes.router,
        factories={
            "job_control_dependencies_factory": lambda: controls,
            "job_control_route_dependencies_factory": lambda: mutations,
        },
    )
    return SimpleNamespace(
        client=TestClient(app, raise_server_exceptions=False),
        store=store,
        operations=operations,
    )


def _call(wire, route, headers):
    method, path, operation = ROUTES[route]
    response = getattr(wire.client, method)(path, headers=headers)
    return response, getattr(wire.operations, operation)


def _delegated(user_id, **extra):
    return {"X-Internal-Key": INTERNAL_KEY, "X-MCP-User-Id": user_id, **extra}


@pytest.mark.parametrize("route", sorted(ROUTES))
def test_the_bare_internal_key_is_the_agent_runtime(wire, route):
    response, operation = _call(wire, route, {"X-Internal-Key": INTERNAL_KEY})

    assert response.status_code == 200
    wire.store.get_user.assert_not_awaited()
    assert operation.await_args.kwargs["job"]["id"] == JOB_ID


@pytest.mark.parametrize("route", sorted(ROUTES))
def test_a_delegated_user_without_access_is_refused(wire, route):
    """N13: key + X-MCP-User-Id reached any job by id."""
    response, operation = _call(wire, route, _delegated(STRANGER_ID))

    assert response.status_code == 403
    assert response.json()["detail"] == "Not authorized to access this job"
    operation.assert_not_awaited()
    wire.store.record_security_event.assert_awaited_once()


@pytest.mark.parametrize("route", sorted(ROUTES))
def test_the_refusal_matches_the_no_key_user_path(wire, route):
    """The delegated path answers exactly what require_job_access answers."""
    stranger = {**USERS[STRANGER_ID], "real_is_admin": False, "scopes": []}
    with patch.object(
        access, "require_approved_user", AsyncMock(return_value=stranger)
    ):
        plain, _ = _call(wire, route, {})
    delegated, _ = _call(wire, route, _delegated(STRANGER_ID))

    assert (delegated.status_code, delegated.json()) == (
        plain.status_code,
        plain.json(),
    )


@pytest.mark.parametrize("route", sorted(ROUTES))
@pytest.mark.parametrize("user_id", [OWNER_ID, MEMBER_ID, ADMIN_ID])
def test_a_delegated_user_with_access_is_allowed(wire, route, user_id):
    response, operation = _call(wire, route, _delegated(user_id))

    assert response.status_code == 200
    wire.store.get_user.assert_awaited_with(user_id)
    operation.assert_awaited_once()


@pytest.mark.parametrize("route", ["approve", "resume", "pause"])
def test_a_delegated_call_keeps_the_internal_actor_semantics(wire, route):
    """The handler still sees ``user=None``: a resume's livelock ack stays the
    officer's runtime-actor check, and attribution stays internal."""
    _response, operation = _call(wire, route, _delegated(OWNER_ID))

    kwargs = operation.await_args.kwargs
    if route == "pause":
        assert kwargs["paused_by"] is None
    else:
        assert kwargs["user"] is None


@pytest.mark.parametrize("route", ["approve", "cancel"])
def test_the_mcp_project_scope_still_fences_a_delegated_call(wire, route):
    """An officer or project-scoped token cannot reach another project's job."""
    response, operation = _call(
        wire,
        route,
        _delegated(OWNER_ID, **{"X-MCP-Scope": f"project:{OTHER_PROJECT_ID}"}),
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Access denied by MCP token scope"
    operation.assert_not_awaited()


@pytest.mark.parametrize("route", ["approve", "cancel"])
def test_the_matching_project_scope_is_allowed(wire, route):
    response, operation = _call(
        wire, route, _delegated(OWNER_ID, **{"X-MCP-Scope": f"project:{PROJECT_ID}"})
    )

    assert response.status_code == 200
    operation.assert_awaited_once()


@pytest.mark.parametrize("route", ["approve", "cancel"])
def test_an_unknown_delegated_user_is_unauthenticated(wire, route):
    response, operation = _call(wire, route, _delegated(OTHER_PROJECT_ID))

    assert response.status_code == 401
    operation.assert_not_awaited()


@pytest.mark.parametrize("route", ["approve", "cancel"])
def test_a_missing_job_is_404_on_every_path(wire, route):
    method, path, _ = ROUTES[route]
    path = path.replace(JOB_ID, "00000000-0000-0000-0000-00000000dead")
    for headers in ({"X-Internal-Key": INTERNAL_KEY}, _delegated(OWNER_ID)):
        response = getattr(wire.client, method)(path, headers=headers)
        assert response.status_code == 404
