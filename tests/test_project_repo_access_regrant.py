"""A project's managed repos are re-granted to its members, not shared once.

Issue: knowledge-base/knowledge/issues/project_knowledge_repo_grant_never_retried.md.
The creator's grant on ``project-<id8>-knowledge`` ran once at creation; when
it failed (their Gitea account could not be created) nothing tried again, and
the owner got a 404 on their own vault. Three things are pinned here:

* one helper decides a member's Gitea access, and adding a member, opening the
  project in Cockpit and logging in all go through it;
* the Cockpit route grants the caller, and only a real member;
* the login re-grant runs in the background, is capped, and never fails login;
* a failed grant, and Gitea refusing an account, are WARNINGs that carry
  Gitea's answer.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from orchestrator.routers.workspace_access import (
    WorkspaceAccessDependencies,
    router,
)
from orchestrator.security import auth
from orchestrator.services import gitea as gitea_mod
from orchestrator.services import projects as projects_mod
from orchestrator.services import workspace_access as operations
from orchestrator.services.workspace_access import WorkspaceOperationDependencies

PROJECT = str(uuid4())
OTHER_PROJECT = str(uuid4())
USER = {"id": str(uuid4()), "email": "riel@example.test", "is_approved": True}
VAULT = f"project-{PROJECT[:8]}-knowledge"


def _repos(project_id: str = PROJECT) -> list[dict[str, Any]]:
    return [
        {"name": f"project-{project_id[:8]}-knowledge", "is_managed": True},
        {"name": f"project-{project_id[:8]}", "is_managed": True},
        {"name": "someone-elses-github-repo", "is_managed": False},
    ]


class _Forge:
    """Records each grant; answers ``granted`` for every repo."""

    def __init__(self, granted: bool = True) -> None:
        self.is_initialized = True
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.granted = granted

    async def grant_user_repo_access(self, *args: Any, **kwargs: Any) -> bool:
        self.calls.append((args, kwargs))
        return self.granted


# =============================================================================
# The one member grant
# =============================================================================


class TestTheMemberGrant:
    @pytest.mark.asyncio
    async def test_every_managed_repo_is_granted_and_attached_ones_are_not(self):
        store = SimpleNamespace(
            get_project_repositories=AsyncMock(return_value=_repos())
        )
        forge = _Forge()

        results = await operations.grant_project_repo_access(
            store=store, forge=forge, project_id=PROJECT, email=USER["email"]
        )

        assert results == {VAULT: True, f"project-{PROJECT[:8]}": True}
        store.get_project_repositories.assert_awaited_once_with(PROJECT)
        # The add-member call shape: no permission named here, so the forge's
        # one default decides it for every path.
        assert forge.calls == [
            ((USER["email"], VAULT), {}),
            ((USER["email"], f"project-{PROJECT[:8]}"), {}),
        ]

    @pytest.mark.asyncio
    async def test_adding_a_member_grants_through_the_shared_helper(self, monkeypatch):
        helper = AsyncMock(return_value={VAULT: True})
        monkeypatch.setattr(
            projects_mod, "grant_project_repo_access", helper, raising=False
        )
        member = {"id": USER["id"], "email": USER["email"]}
        store = SimpleNamespace(
            add_project_member=AsyncMock(return_value={"role": "editor"}),
            get_user=AsyncMock(return_value=member),
            get_project_repositories=AsyncMock(return_value=_repos()),
        )
        forge = _Forge()
        backend = SimpleNamespace(is_initialized=False)
        deps = SimpleNamespace(
            store=store,
            logger=MagicMock(),
            forge=forge,
            main_cloud_router=SimpleNamespace(for_project_optional=lambda _p: backend),
            provisioning=SimpleNamespace(
                keycloak_groups=SimpleNamespace(is_initialized=False)
            ),
        )
        monkeypatch.setattr(
            projects_mod.project_provisioning,
            "sync_project_member_to_groups",
            AsyncMock(),
        )

        await projects_mod.add_project_member(
            PROJECT,
            SimpleNamespace(user_id=USER["id"], role="editor"),
            project={"id": PROJECT},
            dependencies=deps,
        )

        helper.assert_awaited_once_with(
            store=store, forge=forge, project_id=PROJECT, email=USER["email"]
        )
        assert forge.calls == []  # nothing granted beside the helper


# =============================================================================
# POST /api/projects/{project_id}/ensure-repo-access (Cockpit opens a project)
# =============================================================================


@pytest.fixture
def wire():
    events: list[str] = []
    state = SimpleNamespace(gate_error=None, user=dict(USER), role="owner")

    async def require_project_member(request, store, project_id):
        events.append("project_member")
        assert project_id == PROJECT
        if state.gate_error is not None:
            raise state.gate_error
        return dict(state.user), {"id": PROJECT}

    async def unexpected(*_args, **_kwargs):
        raise AssertionError("not this route's gate")

    store = SimpleNamespace(
        get_project_repositories=AsyncMock(side_effect=lambda _pid: _repos()),
        get_user_role_in_project=AsyncMock(side_effect=lambda *_a: state.role),
    )
    forge = _Forge()
    holder = SimpleNamespace(store=store, forge=forge)

    def factory():
        return WorkspaceAccessDependencies(
            store=holder.store,
            forge=holder.forge,
            workspace=SimpleNamespace(),
            snapshots=SimpleNamespace(),
            operations=WorkspaceOperationDependencies(
                store=holder.store,
                forge=holder.forge,
                container_provisioner=SimpleNamespace(),
                enforce_job_workspace_upgrade_grants=unexpected,
            ),
            resolve_job_repo=unexpected,
            require_admin=unexpected,
            require_approved_user=unexpected,
            require_job_access=unexpected,
            require_internal=unexpected,
            require_project_member=require_project_member,
        )

    app = FastAPI()
    app.state.workspace_access_dependencies_factory = factory
    app.include_router(router)
    return SimpleNamespace(app=app, state=state, holder=holder, events=events)


async def _post(wire) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wire.app), base_url="http://ws.test"
    ) as client:
        return await client.post(f"/api/projects/{PROJECT}/ensure-repo-access")


class TestEnsureProjectRepoAccess:
    @pytest.mark.asyncio
    async def test_a_member_is_granted_every_managed_repo(self, wire):
        response = await _post(wire)

        assert response.status_code == 200
        assert response.json() == {
            "granted": True,
            "reason": "ok",
            "repos": [VAULT, f"project-{PROJECT[:8]}"],
        }
        assert [args for args, _ in wire.holder.forge.calls] == [
            (USER["email"], VAULT),
            (USER["email"], f"project-{PROJECT[:8]}"),
        ]

    @pytest.mark.asyncio
    async def test_the_member_gate_runs_before_anything_is_read(self, wire):
        wire.state.gate_error = HTTPException(status_code=403, detail="Not a member")

        response = await _post(wire)

        assert response.status_code == 403
        wire.holder.store.get_project_repositories.assert_not_awaited()
        assert wire.holder.forge.calls == []

    @pytest.mark.asyncio
    async def test_an_admin_who_is_not_a_member_is_granted_nothing(self, wire):
        # The gate lets an admin through; the grant is for owners and members.
        wire.state.role = None

        response = await _post(wire)

        assert response.json() == {"granted": False, "reason": "not_a_member"}
        assert wire.holder.forge.calls == []

    @pytest.mark.asyncio
    async def test_gitea_down_is_a_200_with_a_reason(self, wire):
        wire.holder.forge.is_initialized = False

        response = await _post(wire)

        assert response.status_code == 200
        assert response.json() == {"granted": False, "reason": "gitea_unavailable"}

    @pytest.mark.asyncio
    async def test_a_caller_without_email_is_a_200_with_a_reason(self, wire):
        wire.state.user = {"id": USER["id"], "is_approved": True}

        response = await _post(wire)

        assert response.json() == {"granted": False, "reason": "no_email"}

    @pytest.mark.asyncio
    async def test_a_project_without_managed_repos_is_a_200_with_a_reason(self, wire):
        wire.holder.store.get_project_repositories = AsyncMock(return_value=[])

        response = await _post(wire)

        assert response.json() == {"granted": False, "reason": "no_repo"}

    @pytest.mark.asyncio
    async def test_a_user_missing_from_gitea_is_reported_not_raised(self, wire):
        wire.holder.forge.granted = False

        response = await _post(wire)

        assert response.status_code == 200
        assert response.json() == {
            "granted": False,
            "reason": "user_not_in_gitea",
            "repos": [],
        }

    @pytest.mark.asyncio
    async def test_a_non_http_gate_failure_becomes_a_logged_500(self, wire):
        wire.state.gate_error = RuntimeError("keycloak down")

        response = await _post(wire)

        assert response.status_code == 500
        assert response.json()["detail"] == "keycloak down"


# =============================================================================
# The login re-grant
# =============================================================================


def _login_store(projects: list[str]) -> SimpleNamespace:
    return SimpleNamespace(
        get_projects_for_user=AsyncMock(
            return_value=[{"id": project_id} for project_id in projects]
        ),
        get_project_repositories=AsyncMock(side_effect=_repos),
    )


class TestTheLoginRegrant:
    @pytest.mark.asyncio
    async def test_each_project_of_the_user_is_regranted_up_to_the_cap(self):
        store = _login_store([PROJECT, OTHER_PROJECT])
        forge = _Forge()

        granted = await operations.regrant_user_project_repos(
            store=store, forge=forge, user_id=USER["id"], email=USER["email"]
        )

        assert granted == 4
        store.get_projects_for_user.assert_awaited_once_with(
            USER["id"], limit=operations.LOGIN_REGRANT_PROJECT_LIMIT
        )
        assert operations.LOGIN_REGRANT_PROJECT_LIMIT <= 50
        assert {args[1] for args, _ in forge.calls} == {
            VAULT,
            f"project-{PROJECT[:8]}",
            f"project-{OTHER_PROJECT[:8]}-knowledge",
            f"project-{OTHER_PROJECT[:8]}",
        }

    @pytest.mark.asyncio
    async def test_one_failing_project_does_not_stop_the_others(self, caplog):
        store = _login_store([PROJECT, OTHER_PROJECT])
        store.get_project_repositories = AsyncMock(
            side_effect=[RuntimeError("db hiccup"), _repos(OTHER_PROJECT)]
        )
        forge = _Forge()

        with caplog.at_level(logging.WARNING, logger=operations.logger.name):
            granted = await operations.regrant_user_project_repos(
                store=store, forge=forge, user_id=USER["id"], email=USER["email"]
            )

        assert granted == 2
        assert any(
            r.levelno == logging.WARNING and PROJECT in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_login_regrants_after_the_gitea_account_is_ensured(self, monkeypatch):
        order: list[str] = []
        forge = _Forge()
        store = _login_store([PROJECT])

        async def ensure_gitea(**_kwargs):
            order.append("gitea_user")

        original = forge.grant_user_repo_access

        async def grant(*args, **kwargs):
            order.append("grant")
            return await original(*args, **kwargs)

        forge.grant_user_repo_access = grant
        monkeypatch.setattr(auth, "_ensure_gitea_user", ensure_gitea)
        monkeypatch.setattr(auth, "_default_project_vault_provider", MagicMock())
        monkeypatch.setattr(auth, "_forge_provider", lambda: forge)

        await auth._ensure_gitea_user_then_vault(
            dict(USER),
            sub="kc-riel",
            email=USER["email"],
            preferred_username="riel",
            display_name="Riel",
            db=store,
        )

        assert order[0] == "gitea_user"
        assert order[1:] == ["grant", "grant"]

    @pytest.mark.asyncio
    async def test_a_gitea_failure_never_fails_the_login(self, monkeypatch, caplog):
        forge = _Forge()
        forge.grant_user_repo_access = AsyncMock(side_effect=RuntimeError("502"))
        monkeypatch.setattr(auth, "_ensure_gitea_user", AsyncMock())
        monkeypatch.setattr(auth, "_default_project_vault_provider", MagicMock())
        monkeypatch.setattr(auth, "_forge_provider", lambda: forge)

        with caplog.at_level(logging.WARNING):
            await auth._ensure_gitea_user_then_vault(
                dict(USER),
                sub="kc-riel",
                email=USER["email"],
                preferred_username="riel",
                display_name="Riel",
                db=_login_store([PROJECT]),
            )

        assert any(r.levelno == logging.WARNING for r in caplog.records)

    @pytest.mark.asyncio
    async def test_first_login_hands_the_store_to_the_background_regrant(
        self, monkeypatch
    ):
        monkeypatch.delenv("MCP_DEV_TOKEN", raising=False)
        seen: dict[str, Any] = {}

        async def ensure_then_vault(user, **kwargs):
            seen.update(kwargs)

        monkeypatch.setattr(auth, "_ensure_gitea_user_then_vault", ensure_then_vault)
        monkeypatch.setattr(auth, "_ensure_cloud_user", AsyncMock())
        monkeypatch.setattr(auth, "_ensure_user_cloud_connectors", AsyncMock())
        row = {**USER, "is_admin": False, "default_project_id": PROJECT}
        db = MagicMock()
        db.get_user_by_keycloak_sub = AsyncMock(return_value=None)
        db.upsert_user_from_oidc = AsyncMock(return_value=dict(row))
        db.seed_admin_grants = AsyncMock()

        await auth._resolve_user_from_claims(
            {
                "sub": "kc-riel",
                "email": USER["email"],
                "preferred_username": "riel",
                "realm_access": {"roles": ["user"]},
            },
            db,
        )
        for _ in range(5):
            await asyncio.sleep(0)

        assert seen.get("db") is db


# =============================================================================
# A failed grant is visible
# =============================================================================


def _gitea(handler) -> gitea_mod.GiteaClient:
    gc = gitea_mod.GiteaClient.__new__(gitea_mod.GiteaClient)
    gc._initialized = True
    gc._url = "http://gitea"
    gc._user = "srw-admin"
    gc._password = ""
    gc._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return gc


class TestFailedGrantsAreWarnings:
    @pytest.mark.asyncio
    async def test_gitea_refusing_a_reserved_username_is_a_warning_with_its_message(
        self, caplog
    ):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "POST"
            return httpx.Response(
                422, json={"message": "name is reserved [name: admin]"}
            )

        gc = _gitea(handler)
        gc._get_oidc_source_id = AsyncMock(return_value=1)
        gc.find_user_by_email = AsyncMock(return_value=None)

        with caplog.at_level(logging.DEBUG, logger=gitea_mod.logger.name):
            assert await gc.ensure_user("admin@example.test", "admin") is None

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings, "Gitea's 422 must be visible at WARNING"
        message = warnings[0].getMessage()
        assert "422" in message
        assert "name is reserved" in message

    @pytest.mark.asyncio
    async def test_a_grant_without_a_gitea_account_is_a_warning(self, caplog):
        gc = _gitea(lambda _request: httpx.Response(500))
        gc.find_user_by_email = AsyncMock(return_value=None)

        with caplog.at_level(logging.DEBUG, logger=gitea_mod.logger.name):
            assert await gc.grant_user_repo_access(USER["email"], VAULT) is False

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert VAULT in warnings[0].getMessage()
        assert USER["email"] in warnings[0].getMessage()

    @pytest.mark.asyncio
    async def test_a_refused_collaborator_add_names_giteas_status_and_message(
        self, caplog
    ):
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(403, json={"message": "user is blocked"})

        gc = _gitea(handler)
        gc.find_user_by_email = AsyncMock(return_value="riel")

        with caplog.at_level(logging.WARNING, logger=gitea_mod.logger.name):
            assert await gc.grant_user_repo_access(USER["email"], VAULT) is False

        message = " ".join(r.getMessage() for r in caplog.records)
        assert "403" in message and "user is blocked" in message
