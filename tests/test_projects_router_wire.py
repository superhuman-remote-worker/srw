"""Wire contracts for the extracted projects router (R1.B03 lane P).

Before the extraction these 22 handlers lived in ``main.py`` and their gates
were only reachable by calling the handler function directly
(``tests/test_project_access.py`` does exactly that). Mounting the real router
over injected gates pins the things a handler move can quietly change and a
direct call cannot see:

* **which gate tier each route depends on** — the four tiers refuse
  independently, and ``scripts/check_endpoint_auth.py`` reads the same names
  statically, so a swapped field would show up here as a wrong status code
  rather than only in the inventory diff;
* **self-removal**, the one deliberately-not-owner-gated mutation: a member may
  leave a project they do not own, which is why
  ``DELETE /api/projects/{id}/members/{user_id}`` authenticates instead of
  demanding ownership, and why the owner check runs *inside* it for a
  cross-user removal;
* **the archived refusal**, which is a body-level check (the gate flag fires
  before the body is parsed) and refuses the WHOLE PATCH rather than applying
  its status half;
* **redaction on the way out** — repository and connector rows leave the
  orchestrator without credentials and without userinfo in a URL. That boundary
  is the reason those two list routes exist in this shape at all.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from tests._mounted_router import mount_router


PROJECT_ID = str(uuid4())
DATASOURCE_ID = str(uuid4())
REPO_ID = str(uuid4())
JOB_ID = str(uuid4())
OWNER_ID = str(uuid4())
MEMBER_ID = str(uuid4())

OWNER = {"id": OWNER_ID, "email": "owner@test", "is_admin": False}
MEMBER = {"id": MEMBER_ID, "email": "member@test", "is_admin": False}
PROJECT = {"id": PROJECT_ID, "name": "Wire", "is_default": False, "status": "active"}
ARCHIVED = {**PROJECT, "status": "archived"}


def _store(**over: Any) -> SimpleNamespace:
    """Only the store methods these routes actually reach."""
    store = SimpleNamespace(
        get_project=AsyncMock(return_value=dict(PROJECT)),
        get_project_members=AsyncMock(
            return_value=[{"user_id": OWNER_ID, "role": "owner"}]
        ),
        get_user_role_in_project=AsyncMock(return_value="editor"),
        remove_project_member=AsyncMock(return_value=True),
        get_user=AsyncMock(return_value=dict(MEMBER)),
        get_project_repositories=AsyncMock(
            return_value=[
                {
                    "id": REPO_ID,
                    "name": "repo",
                    "repo_url": "http://bot:hunter2@srw-gitea:3000/srw/repo.git",
                    "credentials": {"token": "leak-me"},
                    "is_managed": True,
                }
            ]
        ),
        list_project_datasources=AsyncMock(
            return_value=[
                {
                    "id": DATASOURCE_ID,
                    "name": "connector",
                    "type": "postgres",
                    "connection_url": "postgresql://u:p@db.internal:5432/x",
                    "credentials": {"password": "leak-me"},
                }
            ]
        ),
        update_project=AsyncMock(return_value=True),
        list_project_job_change_records=AsyncMock(return_value=[{"job_id": JOB_ID}]),
        get_job_change_record=AsyncMock(return_value={"job_id": JOB_ID}),
    )
    for key, value in over.items():
        setattr(store, key, value)
    return store


def _deny(status: int, detail: str):
    async def gate(*_args, **_kwargs):
        raise HTTPException(status_code=status, detail=detail)

    return gate


def _wire(
    *,
    store=None,
    approved_user=None,
    project_member=None,
    project_owner=None,
    job_access=None,
    admin=None,
    provisioning_over: dict[str, Any] | None = None,
):
    from orchestrator.routers.projects import ProjectsDependencies, router
    from orchestrator.services import project_provisioning, projects

    db = store or _store()

    async def _approved(_request, _store):
        return OWNER

    async def _member(_request, _store, _project_id, **_kwargs):
        return OWNER, dict(PROJECT)

    async def _owner(_request, _store, _project_id, **_kwargs):
        return OWNER, dict(PROJECT)

    async def _job(_request, _store, _job_id):
        return OWNER, {"id": JOB_ID}

    async def _admin(_request):
        return {**OWNER, "is_admin": True}

    provisioning = project_provisioning.ProjectProvisioningDependencies(
        store=db,
        forge=SimpleNamespace(is_initialized=False),
        keycloak_groups=SimpleNamespace(is_initialized=False),
        main_cloud_router=SimpleNamespace(
            for_project_optional=MagicMock(return_value=None)
        ),
        logger=MagicMock(),
        repair=project_provisioning.ProjectRepairState(),
        knowledge_index=MagicMock(),
        **(provisioning_over or {}),
    )
    operations = projects.ProjectDependencies(
        store=db,
        vector_db=MagicMock(),
        forge=SimpleNamespace(is_initialized=False),
        keycloak_groups=SimpleNamespace(is_initialized=False),
        main_cloud_router=SimpleNamespace(
            for_project_optional=MagicMock(return_value=None)
        ),
        logger=MagicMock(),
        provisioning=provisioning,
        with_validated_tool_overrides=lambda value: value,
    )
    deps = ProjectsDependencies(
        store=db,
        operations=operations,
        require_admin=admin or _admin,
        require_approved_user=approved_user or _approved,
        require_project_member=project_member or _member,
        require_project_owner=project_owner or _owner,
        require_job_access=job_access or _job,
    )
    app = mount_router(
        router, factories={"projects_dependencies_factory": lambda: deps}
    )
    return SimpleNamespace(client=TestClient(app), store=db, operations=operations)


# =============================================================================
# One refusal per gate tier
# =============================================================================


class TestGateTiers:
    """Each tier refuses on its own routes and reaches no store method."""

    def test_approved_user_tier_refuses(self):
        wired = _wire(approved_user=_deny(401, "Authentication required"))
        for method, path, body in (
            ("post", "/api/projects", {"name": "x", "user_id": OWNER_ID}),
            ("get", "/api/projects", None),
            ("delete", f"/api/projects/{PROJECT_ID}/members/{MEMBER_ID}", None),
        ):
            kwargs = {"json": body} if body is not None else {}
            response = getattr(wired.client, method)(path, **kwargs)
            assert response.status_code == 401, (method, path)
        wired.store.remove_project_member.assert_not_awaited()

    def test_project_member_tier_refuses(self):
        wired = _wire(project_member=_deny(403, "Not a project member"))
        for path in (
            f"/api/projects/{PROJECT_ID}",
            f"/api/projects/{PROJECT_ID}/members",
            f"/api/projects/{PROJECT_ID}/repositories",
            f"/api/projects/{PROJECT_ID}/datasources",
            f"/api/projects/{PROJECT_ID}/job-records",
        ):
            assert wired.client.get(path).status_code == 403, path
        wired.store.get_project_members.assert_not_awaited()
        wired.store.get_project_repositories.assert_not_awaited()
        wired.store.list_project_datasources.assert_not_awaited()

    def test_project_owner_tier_refuses(self):
        wired = _wire(project_owner=_deny(403, "Project owner role required"))
        calls = [
            ("patch", f"/api/projects/{PROJECT_ID}", {"name": "n"}),
            ("delete", f"/api/projects/{PROJECT_ID}", None),
            ("post", f"/api/projects/{PROJECT_ID}/members", {"user_id": MEMBER_ID}),
            (
                "patch",
                f"/api/projects/{PROJECT_ID}/members/{MEMBER_ID}",
                {"role": "editor"},
            ),
            ("post", f"/api/projects/{PROJECT_ID}/repositories", {"name": "r"}),
            ("patch", f"/api/projects/{PROJECT_ID}/repositories/{REPO_ID}", {}),
            ("delete", f"/api/projects/{PROJECT_ID}/repositories/{REPO_ID}", None),
            ("get", f"/api/projects/{PROJECT_ID}/linkable-datasources", None),
            (
                "post",
                f"/api/projects/{PROJECT_ID}/datasources/{DATASOURCE_ID}",
                {},
            ),
            (
                "patch",
                f"/api/projects/{PROJECT_ID}/datasources/{DATASOURCE_ID}",
                {},
            ),
            (
                "post",
                f"/api/projects/{PROJECT_ID}/knowledge/repository",
                {"repo_url": "https://github.com/o/r", "token": "t"},
            ),
        ]
        for method, path, body in calls:
            kwargs = {"json": body} if body is not None else {}
            response = getattr(wired.client, method)(path, **kwargs)
            assert response.status_code == 403, (method, path, response.text)

    def test_job_access_tier_refuses(self):
        wired = _wire(job_access=_deny(404, f"Job '{JOB_ID}' not found"))
        assert wired.client.get(f"/api/jobs/{JOB_ID}/change-record").status_code == 404
        assert (
            wired.client.post(
                f"/api/jobs/{JOB_ID}/promote",
                json={"name": "p", "user_id": OWNER_ID},
            ).status_code
            == 404
        )
        wired.store.get_job_change_record.assert_not_awaited()


# =============================================================================
# Self-removal — the deliberately non-owner-gated mutation
# =============================================================================


class TestSelfRemoval:
    def test_a_member_may_remove_themselves_without_owning_the_project(self):
        """No project-owner gate is consulted, and no owner role is required.

        This is the reason the route is labelled ``require_approved_user``. If
        someone "tightens" it to ``require_project_owner``, a plain member can
        no longer leave a project and this case fails.
        """
        role_lookup = AsyncMock(return_value="editor")
        store = _store(get_user_role_in_project=role_lookup)
        owner_gate = _deny(403, "Project owner role required")
        wired = _wire(
            store=store,
            approved_user=lambda _r, _s: _self(),
            project_owner=owner_gate,
        )

        response = wired.client.delete(
            f"/api/projects/{PROJECT_ID}/members/{MEMBER_ID}"
        )

        assert response.status_code == 200
        assert response.json() == {"status": "removed"}
        store.remove_project_member.assert_awaited_once_with(PROJECT_ID, MEMBER_ID)
        # Only the target's role is looked up; the caller's is not, because the
        # caller is the target.
        assert role_lookup.await_args_list == [((PROJECT_ID, MEMBER_ID), {})]

    def test_removing_someone_else_still_needs_the_owner_role(self):
        store = _store(get_user_role_in_project=AsyncMock(return_value="editor"))
        wired = _wire(store=store)

        response = wired.client.delete(
            f"/api/projects/{PROJECT_ID}/members/{MEMBER_ID}"
        )

        assert response.status_code == 403
        assert response.json()["detail"] == "Project owner role required"
        store.remove_project_member.assert_not_awaited()

    def test_the_last_owner_cannot_leave(self):
        """Self-removal is allowed; emptying a project of owners is not."""
        store = _store(
            get_user_role_in_project=AsyncMock(return_value="owner"),
            get_project_members=AsyncMock(
                return_value=[{"user_id": MEMBER_ID, "role": "owner"}]
            ),
        )
        wired = _wire(store=store, approved_user=lambda _r, _s: _self())

        response = wired.client.delete(
            f"/api/projects/{PROJECT_ID}/members/{MEMBER_ID}"
        )

        assert response.status_code == 400
        assert response.json()["detail"] == "Cannot remove the last owner of a project"
        store.remove_project_member.assert_not_awaited()


async def _self() -> dict[str, Any]:
    """The caller is the member being removed."""
    return MEMBER


# =============================================================================
# Archived projects are read-only apart from their status
# =============================================================================


class TestArchivedProject:
    def _archived_owner(self):
        async def gate(_request, _store, _project_id, **_kwargs):
            return OWNER, dict(ARCHIVED)

        return gate

    def test_a_non_status_field_is_refused_whole(self):
        store = _store()
        wired = _wire(store=store, project_owner=self._archived_owner())

        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}", json={"name": "renamed", "status": "active"}
        )

        assert response.status_code == 409
        assert "archived" in response.json()["detail"]
        # The status half must NOT have been applied on its own.
        store.update_project.assert_not_awaited()

    def test_the_unarchive_patch_still_works(self):
        store = _store()
        wired = _wire(store=store, project_owner=self._archived_owner())

        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}", json={"status": "active"}
        )

        assert response.status_code == 200
        assert response.json() == {"status": "updated"}
        store.update_project.assert_awaited_once_with(PROJECT_ID, status="active")

    def test_archiving_an_active_project_reports_what_it_quiesced(self):
        store = _store(
            get_active_project_loop=AsyncMock(
                return_value={"id": str(uuid4()), "status": "running"}
            ),
            update_project_loop=AsyncMock(return_value=True),
            get_officer_thread_for_project=AsyncMock(return_value=None),
            park_project_jobs_for_archive=AsyncMock(return_value=3),
        )
        wired = _wire(store=store)

        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}", json={"status": "archived"}
        )

        assert response.status_code == 200
        assert response.json() == {
            "status": "updated",
            "archived": True,
            "loop_paused": True,
            "officer_held": False,
            "jobs_parked": 3,
        }

    def test_an_empty_patch_is_a_400_before_anything_is_read(self):
        store = _store()
        wired = _wire(store=store)

        response = wired.client.patch(f"/api/projects/{PROJECT_ID}", json={})

        assert response.status_code == 400
        assert response.json()["detail"] == "No fields to update"
        store.update_project.assert_not_awaited()

    def test_network_tier_escalates_to_admin_after_the_archived_check(self):
        """Order matters: an archived project answers 409, not the admin 403.

        The admin gate is reached only once the body has survived every
        body-level rule. Hoisting it would turn this request's answer from
        "unarchive it first" into "you are not an admin", which is the less
        actionable of the two.
        """
        store = _store()
        wired = _wire(
            store=store,
            project_owner=self._archived_owner(),
            admin=_deny(403, "Admin access required"),
        )

        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}",
            json={"network_tier": "home-allowed", "status": "active"},
        )

        assert response.status_code == 409
        store.update_project.assert_not_awaited()

    def test_network_tier_alone_is_admin_gated(self):
        store = _store()
        wired = _wire(store=store, admin=_deny(403, "Admin access required"))

        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}", json={"network_tier": "home-allowed"}
        )

        assert response.status_code == 403
        store.update_project.assert_not_awaited()


# =============================================================================
# Redaction on the way out
# =============================================================================


class TestResponseRedaction:
    def test_repository_rows_lose_credentials_and_url_userinfo(self):
        wired = _wire()

        rows = wired.client.get(f"/api/projects/{PROJECT_ID}/repositories").json()

        assert len(rows) == 1
        assert "credentials" not in rows[0]
        assert "hunter2" not in rows[0]["repo_url"]
        assert "bot:" not in rows[0]["repo_url"]

    def test_connector_rows_lose_credentials_and_url_password(self):
        wired = _wire()

        rows = wired.client.get(f"/api/projects/{PROJECT_ID}/datasources").json()

        assert len(rows) == 1
        assert "credentials" not in rows[0]
        assert ":p@" not in rows[0]["connection_url"]
        assert rows[0]["connection_url_redacted"] is True

    def test_linkable_connectors_are_redacted_inside_the_page_envelope(self):
        store = _store(
            list_project_linkable_datasources=AsyncMock(
                return_value={
                    "items": [
                        {
                            "id": DATASOURCE_ID,
                            "name": "c",
                            "credentials": {"password": "leak-me"},
                        }
                    ],
                    "next_cursor": None,
                }
            )
        )
        wired = _wire(store=store)

        page = wired.client.get(
            f"/api/projects/{PROJECT_ID}/linkable-datasources"
        ).json()

        assert page["next_cursor"] is None
        assert "credentials" not in page["items"][0]


# The shapes the loader honours in a project override (merged under every job
# in the project): a BYO provider key, a capability key in ``env_keys``, an
# rclone remote with its token, and the SSH transport block.
SECRET_OVERRIDE = {
    "memory": {"project_scoped": True},
    "llm": {
        "model": "openrouter/some-model",
        "base_url": "https://openrouter.ai/api/v1",
        "api_key": "sk-project-secret",
    },
    "env_keys": {"EMBEDDING_API_KEY": "sk-embed-secret", "EMBEDDING_MODEL": "e5"},
    "workspace": {
        "backend": "sandbox",
        "remote": {"host": "10.0.0.7", "port": 2222, "user": "ws"},
        "mounts": [
            {"name": "drive", "path": "/mnt/d", "rclone_spec": "drive:token=rc-secret"}
        ],
    },
}
PUBLIC_OVERRIDE = {
    "memory": {"project_scoped": True},
    "llm": {
        "model": "openrouter/some-model",
        "base_url": "https://openrouter.ai/api/v1",
    },
    "env_keys": {"EMBEDDING_MODEL": "e5"},
    "workspace": {
        "backend": "sandbox",
        "mounts": [{"name": "drive", "path": "/mnt/d"}],
    },
}
SECRETS = ("sk-project-secret", "sk-embed-secret", "rc-secret", "10.0.0.7")


def _stored_project_gate(override):
    """Member/owner gate returning the project as asyncpg does: JSONB as text."""

    async def gate(_request, _store, _project_id, **_kwargs):
        return OWNER, {**PROJECT, "default_config_override": json.dumps(override)}

    return gate


class TestDefaultConfigOverrideRedaction:
    """``default_config_override`` leaves by the job API's policy
    (``redact_config_override`` + ``workspace.remote``): every project MEMBER
    reads it, and before this they read the stored keys verbatim."""

    def test_project_detail_hides_secrets_and_keeps_the_rest(self):
        wired = _wire(project_member=_stored_project_gate(SECRET_OVERRIDE))

        response = wired.client.get(f"/api/projects/{PROJECT_ID}")

        assert response.status_code == 200
        assert response.json()["default_config_override"] == PUBLIC_OVERRIDE
        for secret in SECRETS:
            assert secret not in response.text

    def test_project_list_hides_secrets(self):
        store = _store(
            get_projects_for_user=AsyncMock(
                return_value=[{**PROJECT, "default_config_override": SECRET_OVERRIDE}]
            )
        )
        wired = _wire(store=store)

        response = wired.client.get("/api/projects")

        assert response.status_code == 200
        assert response.json()[0]["default_config_override"] == PUBLIC_OVERRIDE
        for secret in SECRETS:
            assert secret not in response.text

    def test_cockpit_memory_toggle_round_trip_keeps_the_stored_secrets(self):
        """project-detail ``toggleProjectMemory`` spreads the override it READ
        and PATCHes the whole thing back. Redacting the read without restoring
        on write would make that click delete every stored key."""
        gate = _stored_project_gate(SECRET_OVERRIDE)
        wired = _wire(project_member=gate, project_owner=gate)
        existing = wired.client.get(f"/api/projects/{PROJECT_ID}").json()[
            "default_config_override"
        ]
        override = {
            **existing,
            "memory": {**existing["memory"], "project_scoped": False},
        }

        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}", json={"default_config_override": override}
        )

        assert response.status_code == 200
        written = wired.store.update_project.await_args.kwargs[
            "default_config_override"
        ]
        assert written == {**SECRET_OVERRIDE, "memory": {"project_scoped": False}}
        # Nothing was lost, so nothing is reported.
        assert response.json() == {"status": "updated"}

    def test_a_redirected_endpoint_restores_nothing_anywhere(self):
        """An override has several writers (any co-owner), and one who cannot
        read ``llm.api_key`` must not be able to re-point ``llm.base_url`` at a
        host they control and have every job in the project send it there —
        nor any other stored key, which might ride the same request."""
        gate = _stored_project_gate(SECRET_OVERRIDE)
        wired = _wire(project_owner=gate)
        override = json.loads(json.dumps(PUBLIC_OVERRIDE))
        override["llm"]["base_url"] = "https://attacker.example/v1"

        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}", json={"default_config_override": override}
        )

        assert response.status_code == 200
        written = wired.store.update_project.await_args.kwargs[
            "default_config_override"
        ]
        assert written == override
        # Not silent: a read-modify-write client learns which stored values
        # the write discarded and must re-enter.
        assert sorted(response.json()["dropped_hidden_keys"]) == [
            "env_keys.EMBEDDING_API_KEY",
            "llm.api_key",
            "workspace.mounts[0].rclone_spec",
            "workspace.remote",
        ]

    def test_an_explicitly_resent_secret_is_not_reported_dropped(self):
        gate = _stored_project_gate(SECRET_OVERRIDE)
        wired = _wire(project_owner=gate)
        override = json.loads(json.dumps(PUBLIC_OVERRIDE))
        override["llm"]["base_url"] = "https://other.example/v1"
        override["llm"]["api_key"] = "sk-new"

        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}", json={"default_config_override": override}
        )

        assert "llm.api_key" not in response.json()["dropped_hidden_keys"]

    def test_editing_a_section_drops_the_hidden_values_in_it(self):
        """Without an endpoint change, only the sections the write touched lose
        their hidden values."""
        gate = _stored_project_gate(SECRET_OVERRIDE)
        wired = _wire(project_owner=gate)
        override = json.loads(json.dumps(PUBLIC_OVERRIDE))
        override["llm"]["model"] = "openrouter/another-model"
        override["workspace"]["mounts"][0]["path"] = "/mnt/elsewhere"

        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}", json={"default_config_override": override}
        )

        assert response.status_code == 200
        written = wired.store.update_project.await_args.kwargs[
            "default_config_override"
        ]
        assert written["llm"] == override["llm"]
        # The workspace section changed (a mount moved), so its transport block
        # and the moved mount's rclone_spec go too.
        assert written["workspace"] == override["workspace"]
        # The untouched section keeps its key.
        assert written["env_keys"] == SECRET_OVERRIDE["env_keys"]

    def test_dropping_a_section_drops_its_hidden_values(self):
        gate = _stored_project_gate(SECRET_OVERRIDE)
        wired = _wire(project_owner=gate)
        override = {"memory": {"project_scoped": True}}

        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}", json={"default_config_override": override}
        )

        assert response.status_code == 200
        written = wired.store.update_project.await_args.kwargs[
            "default_config_override"
        ]
        assert written == override

    def test_an_explicit_value_always_wins_over_the_stored_one(self):
        gate = _stored_project_gate(SECRET_OVERRIDE)
        wired = _wire(project_owner=gate)
        override = json.loads(json.dumps(PUBLIC_OVERRIDE))
        override["llm"]["api_key"] = None
        override["env_keys"]["EMBEDDING_API_KEY"] = "sk-rotated"

        response = wired.client.patch(
            f"/api/projects/{PROJECT_ID}", json={"default_config_override": override}
        )

        assert response.status_code == 200
        written = wired.store.update_project.await_args.kwargs[
            "default_config_override"
        ]
        assert written["llm"]["api_key"] is None
        assert written["env_keys"]["EMBEDDING_API_KEY"] == "sk-rotated"


def _round_trip(stored: dict[str, Any], edit) -> dict[str, Any]:
    """Serve ``stored`` redacted, apply ``edit`` to that view the way a client
    would, and return what the write path would persist."""
    from orchestrator.security.access import (
        redact_public_config_override,
        restore_hidden_config_values,
    )

    view = redact_public_config_override(json.dumps(stored))
    return restore_hidden_config_values(
        edit(json.loads(json.dumps(view))), json.dumps(stored)
    )


class TestRestoreEndpointGuard:
    """No hidden value is restored anywhere once any endpoint-shaped key in the
    override differs from the stored one. The per-section rule alone restored a
    nested or neighbouring section's key under a redirected endpoint."""

    def test_nested_summarization_key_is_not_restored_under_a_new_base_url(self):
        stored = {
            "llm": {
                "model": "m",
                "base_url": "https://good",
                "api_key": "K",
                "summarization": {"model": "s", "api_key": "S"},
            }
        }

        saved = _round_trip(
            stored, lambda v: {**v, "llm": {**v["llm"], "base_url": "https://evil"}}
        )

        assert "api_key" not in saved["llm"]["summarization"]
        assert "api_key" not in saved["llm"]

    def test_legacy_tier_and_roster_keys_are_not_restored(self):
        stored = {
            "llm": {
                "base_url": "https://good",
                "strategic": {"model": "m", "api_key": "K"},
            },
            "subagents": {"roster": {"r": {"llm": {"model": "x", "api_key": "R"}}}},
        }

        saved = _round_trip(
            stored, lambda v: {**v, "llm": {**v["llm"], "base_url": "https://evil"}}
        )

        assert "api_key" not in saved["llm"]["strategic"]
        assert "api_key" not in saved["subagents"]["roster"]["r"]["llm"]

    def test_a_new_endpoint_in_another_section_restores_nothing(self):
        """The memory reranker rides the embedding transport: a new
        ``memory.reranker.base_url`` would receive ``EMBEDDING_API_KEY``."""
        stored = {
            "env_keys": {
                "EMBEDDING_API_KEY": "E",
                "EMBEDDING_BASE_URL": "https://good",
            }
        }

        saved = _round_trip(
            stored,
            lambda v: {**v, "memory": {"reranker": {"base_url": "https://evil"}}},
        )

        assert saved["env_keys"] == {"EMBEDDING_BASE_URL": "https://good"}

    @pytest.mark.parametrize(
        "key",
        [
            "EMBEDDING_BASE_URL",
            "provider",
            "endpoint",
            "endpoint_id",
            "SEARXNG_URL",
            "baseUrl",
            "http_proxy",
            "api_base",
        ],
    )
    def test_every_endpoint_shape_counts(self, key):
        stored = {
            "llm": {"model": "m", "api_key": "K"},
            "env_keys": {"EMBEDDING_API_KEY": "E"},
        }

        saved = _round_trip(stored, lambda v: {**v, "search": {key: "https://evil"}})

        assert "api_key" not in saved["llm"]
        assert "EMBEDDING_API_KEY" not in saved["env_keys"]

    def test_an_explicit_transport_block_counts_as_an_endpoint_change(self):
        stored = {
            "llm": {"model": "m", "api_key": "K"},
            "workspace": {"backend": "sandbox", "remote": {"host": "good"}},
        }

        saved = _round_trip(
            stored,
            lambda v: {
                **v,
                "workspace": {**v["workspace"], "remote": {"host": "evil"}},
            },
        )

        assert "api_key" not in saved["llm"]

    def test_a_case_variant_of_a_hidden_key_counts_as_sent(self):
        stored = {"llm": {"model": "m", "api_key": "K"}}

        saved = _round_trip(
            stored, lambda v: {**v, "llm": {**v["llm"], "API_KEY": None}}
        )

        assert saved["llm"] == {"model": "m", "API_KEY": None}


def _composed_project() -> dict[str, Any]:
    """A project the way ``hydrate_project_row`` serves it after
    ``migrate_projects``: the whole Project manifest rides on the row, and each
    linked Expert's inline spec carries the shared override (and the link
    override) VERBATIM as ``runtime.config.layers``. The officer's config rides
    at ``spec.team.controller.config.config``."""
    from orchestrator.services.manifest_experts import (
        expert_manifest,
        project_expert_resource,
    )
    from orchestrator.services.manifest_projects import project_document
    from shared.manifests.resolution import content_revision

    expert_id = str(uuid4())
    raw = {
        "id": expert_id,
        "name": "builder",
        "display_name": "Builder",
        "expert_type": "worker",
        "owner_id": OWNER_ID,
        "is_global": False,
        "icon": "build",
        "color": "#123456",
        "tags": ["worker"],
        "config": {"settings": {"expert": True}},
        "prompts": {"persona": "Keep this persona."},
        "default_for": "worker",
        "config_override": {"llm": {"api_key": "sk-link-secret"}},
    }
    document = expert_manifest(raw, image="trusted/srw:v1")
    source = project_expert_resource(
        raw,
        {
            "id": uuid4(),
            "document": document,
            "revision": content_revision(document["spec"]),
            "resource_version": 1,
        },
    )
    project = {**PROJECT, "default_config_override": SECRET_OVERRIDE}
    post = {
        "config_override": {
            "officer": {"enabled": True},
            "llm": {"api_key": "sk-officer-secret"},
        },
        "communication_policy": {},
        "is_active": True,
    }
    manifest, recipe = project_document(
        project, owner_id=OWNER_ID, experts=[source], post=post
    )
    return {
        **PROJECT,
        "manifest": manifest,
        "manifest_uid": str(uuid4()),
        "manifest_revision": "r1",
        "manifest_composed": True,
        "default_config_override": recipe["sharedConfig"],
    }


COMPOSED_SECRETS = (*SECRETS, "sk-link-secret", "sk-officer-secret")


class TestManifestComposedProjectRedaction:
    """``migrate_projects`` makes every project manifest-composed, so the row
    both reads serve carries the Project manifest — and the manifest holds the
    override verbatim in each Expert's ``layers``. Redacting only the
    ``default_config_override`` column projection left all of it readable."""

    def test_project_detail_redacts_every_layer_of_the_manifest(self):
        row = _composed_project()

        async def member(_request, _store, _project_id, **_kwargs):
            return OWNER, row

        wired = _wire(project_member=member)

        response = wired.client.get(f"/api/projects/{PROJECT_ID}")

        assert response.status_code == 200
        for secret in COMPOSED_SECRETS:
            assert secret not in response.text
        body = response.json()
        assert body["default_config_override"] == PUBLIC_OVERRIDE
        # The document keeps its shape: consumers compare it to the resource.
        experts = body["manifest"]["spec"]["resources"]["experts"]
        worker = next(v for k, v in experts.items() if k.endswith("-worker"))
        layers = worker["inline"]["runtime"]["config"]["layers"]
        assert layers[0] == PUBLIC_OVERRIDE
        assert layers[1] == {"llm": {}}
        assert body["manifest"]["spec"]["team"]["controller"]["config"]["config"] == {
            "officer": {"enabled": True},
            "llm": {},
        }

    def test_project_list_redacts_the_manifest(self):
        store = _store(
            get_projects_for_user=AsyncMock(return_value=[_composed_project()])
        )
        wired = _wire(store=store)

        response = wired.client.get("/api/projects")

        assert response.status_code == 200
        for secret in COMPOSED_SECRETS:
            assert secret not in response.text
        assert response.json()[0]["manifest"]["kind"] == "Project"

    def test_a_manifest_without_secrets_is_served_unchanged(self):
        """The k3d cutover gate asserts the list's ``manifest`` equals the
        resource document; redaction must be the identity when there is
        nothing to hide."""
        row = _composed_project()
        clean = json.loads(
            json.dumps(row["manifest"])
            .replace('"api_key": "sk-link-secret"', '"model": "m"')
            .replace('"api_key": "sk-officer-secret"', '"model": "m"')
        )
        clean_row = {**row, "manifest": clean, "default_config_override": {}}
        for layer_holder in clean["spec"]["resources"]["experts"].values():
            config = layer_holder["inline"]["runtime"]["config"]
            config["layers"] = [
                layer for layer in config.get("layers", []) if "workspace" not in layer
            ]
        store = _store(get_projects_for_user=AsyncMock(return_value=[clean_row]))
        wired = _wire(store=store)

        response = wired.client.get("/api/projects")

        assert response.json()[0]["manifest"] == clean


# =============================================================================
# The conditional owner escalation on unlink
# =============================================================================


class TestUnlinkEscalation:
    def _connector(self, **over):
        return {
            "id": DATASOURCE_ID,
            "type": "postgres",
            "created_by": OWNER_ID,
            "config": {},
            **over,
        }

    def test_the_connector_owner_never_reaches_the_project_owner_gate(self):
        store = _store(
            get_datasource=AsyncMock(return_value=self._connector()),
            unlink_datasource_from_project=AsyncMock(return_value=True),
        )
        wired = _wire(store=store, project_owner=_deny(403, "Project owner required"))

        response = wired.client.delete(
            f"/api/projects/{PROJECT_ID}/datasources/{DATASOURCE_ID}"
        )

        assert response.status_code == 200
        assert response.json() == {"status": "unlinked"}

    def test_a_stranger_is_escalated_to_the_project_owner_gate(self):
        store = _store(
            get_datasource=AsyncMock(
                return_value=self._connector(created_by=str(uuid4()))
            ),
            unlink_datasource_from_project=AsyncMock(return_value=True),
        )
        wired = _wire(store=store, project_owner=_deny(403, "Project owner required"))

        response = wired.client.delete(
            f"/api/projects/{PROJECT_ID}/datasources/{DATASOURCE_ID}"
        )

        assert response.status_code == 403
        store.unlink_datasource_from_project.assert_not_awaited()

    def test_a_missing_connector_is_404_before_any_escalation(self):
        store = _store(get_datasource=AsyncMock(return_value=None))
        wired = _wire(store=store, project_owner=_deny(403, "Project owner required"))

        response = wired.client.delete(
            f"/api/projects/{PROJECT_ID}/datasources/{DATASOURCE_ID}"
        )

        assert response.status_code == 404
        assert response.json()["detail"] == "Connector not found"


# =============================================================================
# Route identity
# =============================================================================


def test_the_router_carries_exactly_the_expected_route_identities():
    """The move must not add, drop or rename a mounted identity."""
    from orchestrator.routers.projects import router

    expected = {
        ("POST", "/api/projects"),
        ("POST", "/api/projects/{project_id}/knowledge/repository"),
        ("GET", "/api/projects"),
        ("GET", "/api/projects/{project_id}"),
        ("PATCH", "/api/projects/{project_id}"),
        ("DELETE", "/api/projects/{project_id}"),
        ("GET", "/api/projects/{project_id}/members"),
        ("POST", "/api/projects/{project_id}/members"),
        ("PATCH", "/api/projects/{project_id}/members/{user_id}"),
        ("DELETE", "/api/projects/{project_id}/members/{user_id}"),
        ("GET", "/api/projects/{project_id}/repositories"),
        ("POST", "/api/projects/{project_id}/repositories"),
        ("PATCH", "/api/projects/{project_id}/repositories/{repo_id}"),
        ("DELETE", "/api/projects/{project_id}/repositories/{repo_id}"),
        ("GET", "/api/projects/{project_id}/linkable-datasources"),
        ("GET", "/api/projects/{project_id}/datasources"),
        ("POST", "/api/projects/{project_id}/datasources/{datasource_id}"),
        ("PATCH", "/api/projects/{project_id}/datasources/{datasource_id}"),
        ("DELETE", "/api/projects/{project_id}/datasources/{datasource_id}"),
        ("GET", "/api/projects/{project_id}/job-records"),
        ("GET", "/api/jobs/{job_id}/change-record"),
        ("POST", "/api/jobs/{job_id}/promote"),
    }
    actual = {
        (method, route.path)
        for route in router.routes
        for method in route.methods
        if method != "HEAD"
    }
    assert actual == expected


@pytest.mark.parametrize(
    "field",
    [
        "require_approved_user",
        "require_project_member",
        "require_project_owner",
        "require_job_access",
        "require_admin",
    ],
)
def test_every_gate_is_an_injectable_field(field):
    """``check_endpoint_auth.py`` matches ``deps.<name>(...)`` on ``<name>``."""
    from orchestrator.routers.projects import ProjectsDependencies

    assert field in ProjectsDependencies.__dataclass_fields__
