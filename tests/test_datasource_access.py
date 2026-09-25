"""F3 — multi-tenancy gates + credential redaction on datasource endpoints.

Covers:
    GET    /api/datasources                                — list (scoped + redacted)
    GET    /api/datasources/{id}                           — get (gated + redacted)
    POST   /api/datasources                                — create (auth required)
    PUT    /api/datasources/{id}                           — update (creator/admin; null creds preserve)
    DELETE /api/datasources/{id}                           — delete (creator/admin)
    POST   /api/datasources/{id}/test                      — test (creator/admin)
    GET    /api/jobs/{id}/datasources                      — job-resolved (job access + redacted)
    GET    /api/projects/{id}/datasources                  — project-linked (member + redacted)
    POST   /api/projects/{pid}/datasources/{dsid}          — link (project owner + ds access)
    PATCH  /api/projects/{pid}/datasources/{dsid}          — patch link (project owner)
    DELETE /api/projects/{pid}/datasources/{dsid}          — unlink (project owner)

Topology lives in conftest.py (`datasource_a` belongs to user_a / project_a;
`datasource_b` belongs to user_b / project_b; `datasource_global` is
admin-only with no project link).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.database.postgres import DatasourceProjectAuthorizationError
from orchestrator.routers.datasources import (
    create_datasource,
    delete_datasource,
    get_datasource,
    get_job_datasources,
    list_datasources,
    list_eligible_datasources,
    list_linkable_datasource_targets,
    update_datasource,
)
from orchestrator.routers.projects import (
    link_datasource_to_project,
    list_project_datasources,
    list_project_linkable_datasources,
    unlink_datasource_from_project,
    update_project_datasource,
)
from orchestrator.schemas.datasources import (
    DatasourceCreate,
    DatasourceUpdate,
    ProjectDatasourceSettings,
)
from orchestrator.security import access
from orchestrator.services import datasource_config as datasource_config_module


# =============================================================================
# Helpers
# =============================================================================


def _patch_caller(user: dict):
    """Patch the caller resolution that the *real* gates read.

    Every gate under test here (``require_datasource_access``,
    ``require_datasource_owner``, ``require_project_member``,
    ``require_project_owner``, ``require_job_access``) resolves its caller
    through ``orchestrator.security.access.require_approved_user`` as a module
    global, so this patch still intercepts it. Each route's *own* approved-user
    gate, and the store, now arrive through the dependency dataclass instead of
    ``orchestrator.main`` globals — see ``_ds_deps`` / ``_proj_deps``.
    """
    return patch(
        "orchestrator.security.access.require_approved_user",
        AsyncMock(return_value=user),
    )


def _ds_deps(user: dict, db):
    """The connector router's collaborators, composed as main's
    ``_datasources_dependencies`` factory composes them.

    Only ``store`` and ``require_approved_user`` are substituted — exactly what
    this file used to patch on ``orchestrator.main``. Every other gate keeps
    the dataclass default, i.e. the real function from
    ``orchestrator.security.access``.
    """
    from orchestrator.services.deployment_gates import (
        mcp_datasources_enabled as _mcp_datasources_enabled,
    )
    from orchestrator.routers.datasources import DatasourcesDependencies
    from orchestrator.services.datasources import DatasourceDependencies
    from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
    from orchestrator.services.knowledge_index import KnowledgeIndexDependencies

    return DatasourcesDependencies(
        store=db,
        operations=DatasourceDependencies(
            store=db,
            vector_db=MagicMock(),
            knowledge_index=KnowledgeIndexDependencies(
                store=db,
                vector_db=MagicMock(),
                gitea_client=MagicMock(),
                logger=MagicMock(),
                tasks=KbDatasourceTaskRegistry(),
                inject_system_kb_embedding_profile=AsyncMock(return_value=None),
            ),
            mcp_datasources_enabled=_mcp_datasources_enabled,
            validate_mcp_datasource=datasource_config_module.validate_mcp_datasource,
        ),
        require_approved_user=AsyncMock(return_value=user),
    )


def _proj_deps(user: dict, db):
    """The projects router's collaborators, composed as main's
    ``_projects_dependencies`` factory composes them.

    Same substitution rule as ``_ds_deps``: store and approved-user gate only,
    so ``require_project_owner`` / ``require_project_member`` stay real.
    ``require_admin`` has no dataclass default; none of the routes exercised
    here reach it, so it is wired to refuse loudly if one ever does.
    """
    from orchestrator.routers.projects import ProjectsDependencies
    from orchestrator.services import project_provisioning, projects

    async def _no_admin(*_args, **_kwargs):
        raise AssertionError("no route in this file may escalate to admin")

    return ProjectsDependencies(
        store=db,
        operations=projects.ProjectDependencies(
            store=db,
            vector_db=MagicMock(),
            forge=MagicMock(is_initialized=False),
            keycloak_groups=MagicMock(is_initialized=False),
            main_cloud_router=MagicMock(
                for_project_optional=MagicMock(return_value=None)
            ),
            logger=MagicMock(),
            provisioning=project_provisioning.ProjectProvisioningDependencies(
                store=db,
                forge=MagicMock(is_initialized=False),
                keycloak_groups=MagicMock(is_initialized=False),
                main_cloud_router=MagicMock(
                    for_project_optional=MagicMock(return_value=None)
                ),
                logger=MagicMock(),
                repair=project_provisioning.ProjectRepairState(),
                knowledge_index=MagicMock(),
            ),
            with_validated_tool_overrides=lambda value: value,
        ),
        require_admin=_no_admin,
        require_approved_user=AsyncMock(return_value=user),
    )


# =============================================================================
# redact helpers — pure functions, no DB
# =============================================================================


class TestRedactDatasource:
    def test_strips_credentials(self, datasource_a):
        out = access.redact_datasource(datasource_a)
        assert "credentials" not in out
        # Other fields preserved.
        assert out["id"] == datasource_a["id"]
        assert out["name"] == datasource_a["name"]

    def test_does_not_mutate_input(self, datasource_a):
        before = dict(datasource_a)
        access.redact_datasource(datasource_a)
        assert datasource_a == before

    def test_redact_datasources_list(self, datasource_a, datasource_b):
        rows = [datasource_a, datasource_b]
        out = access.redact_datasources(rows)
        assert all("credentials" not in row for row in out)
        assert len(out) == 2

    def test_strips_connection_url_userinfo_and_marks_redaction(self):
        marker = "password-must-not-leave-rest"
        out = access.redact_datasource(
            {
                "id": "ds-1",
                "connection_url": f"postgresql://alice:{marker}@db.example/app",
            }
        )

        assert out["connection_url"] == "postgresql://db.example/app"
        assert out["connection_url_redacted"] is True
        assert marker not in str(out)

    def test_strips_secret_query_values_but_preserves_safe_options(self):
        marker = "query-secret-must-not-leave-rest"
        out = access.redact_datasource(
            {
                "connection_url": (
                    "https://api.example/data?sslmode=require&api_key=" + marker
                )
            }
        )

        assert out["connection_url"] == ("https://api.example/data?sslmode=require")
        assert out["connection_url_redacted"] is True
        assert marker not in str(out)

    def test_strips_secret_properties_from_opaque_jdbc_dsn(self):
        marker = "jdbc-secret-must-not-leave-rest"
        out = access.redact_datasource(
            {
                "connection_url": (
                    "jdbc:sqlserver://db.example;database=app;password=" + marker
                )
            }
        )

        assert marker not in str(out)
        assert "database=app" in out["connection_url"]
        assert out["connection_url_redacted"] is True

    def test_safe_connection_url_is_preserved_without_redaction_marker(self):
        value = "postgresql://db.example/app?sslmode=require"
        out = access.redact_datasource({"connection_url": value})

        assert out["connection_url"] == value
        assert "connection_url_redacted" not in out


# =============================================================================
# user_can_access_datasource — admin / creator / project member
# =============================================================================


class TestUserCanAccessDatasource:
    @pytest.mark.asyncio
    async def test_creator_yes(self, user_a, datasource_a, fake_db):
        assert await access.user_can_access_datasource(user_a, fake_db, datasource_a)

    @pytest.mark.asyncio
    async def test_other_user_no(self, user_b, datasource_a, fake_db):
        assert not await access.user_can_access_datasource(
            user_b, fake_db, datasource_a
        )

    @pytest.mark.asyncio
    async def test_admin_yes(self, user_admin, datasource_a, fake_db):
        assert await access.user_can_access_datasource(
            user_admin, fake_db, datasource_a
        )

    @pytest.mark.asyncio
    async def test_global_unowned_no_for_normal_user(
        self, user_a, datasource_global, fake_db
    ):
        """`datasource_global` is created_by admin with no project link →
        user_a has neither creator nor membership path."""
        assert not await access.user_can_access_datasource(
            user_a, fake_db, datasource_global
        )

    @pytest.mark.asyncio
    async def test_project_member_yes(self, user_a, datasource_a, fake_db):
        """user_a is a member of project_a, and project_a is linked to
        datasource_a → access via project membership path."""
        # Override creator so the creator-shortcut doesn't fire.
        ds = dict(datasource_a)
        ds["created_by"] = None
        assert await access.user_can_access_datasource(user_a, fake_db, ds)


# =============================================================================
# require_datasource_access — 404 / 403 shape
# =============================================================================


class TestRequireDatasourceAccess:
    @pytest.mark.asyncio
    async def test_owner_passes(self, user_a, datasource_a, fake_db, fake_request):
        with _patch_caller(user_a):
            user, ds = await access.require_datasource_access(
                fake_request, fake_db, str(datasource_a["id"])
            )
        assert user is user_a
        assert ds is datasource_a

    @pytest.mark.asyncio
    async def test_cross_user_403(self, user_b, datasource_a, fake_db, fake_request):
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await access.require_datasource_access(
                    fake_request, fake_db, str(datasource_a["id"])
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_404(self, user_a, fake_db, fake_request):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await access.require_datasource_access(
                    fake_request, fake_db, "ffffffff-ffff-ffff-ffff-ffffffffffff"
                )
        assert exc.value.status_code == 404


# =============================================================================
# require_datasource_owner — creator/admin only (no project-member bypass)
# =============================================================================


class TestRequireDatasourceOwner:
    @pytest.mark.asyncio
    async def test_creator_passes(self, user_a, datasource_a, fake_db, fake_request):
        with _patch_caller(user_a):
            user, ds = await access.require_datasource_owner(
                fake_request, fake_db, str(datasource_a["id"])
            )
        assert user is user_a

    @pytest.mark.asyncio
    async def test_admin_passes(self, user_admin, datasource_a, fake_db, fake_request):
        with _patch_caller(user_admin):
            await access.require_datasource_owner(
                fake_request, fake_db, str(datasource_a["id"])
            )

    @pytest.mark.asyncio
    async def test_project_member_who_isnt_creator_403(
        self, user_a, datasource_b, fake_db, fake_request
    ):
        """user_a can SEE datasource_b only if it's linked to a shared
        project (it isn't here), but they shouldn't be able to MUTATE it
        either way unless they're the creator/admin."""
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await access.require_datasource_owner(
                    fake_request, fake_db, str(datasource_b["id"])
                )
        assert exc.value.status_code == 403


# =============================================================================
# list_datasources — credentials stripped from every row + scoped to caller
# =============================================================================


class TestListDatasourcesEndpoint:
    @pytest.mark.asyncio
    async def test_user_sees_only_own(
        self, user_a, datasource_a, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            rows = await list_datasources(
                fake_request, dependencies=_ds_deps(user_a, fake_db)
            )
        assert {row["id"] for row in rows} == {datasource_a["id"]}
        assert all("credentials" not in row for row in rows)

    @pytest.mark.asyncio
    async def test_admin_sees_all(self, user_admin, fake_db, fake_request):
        with _patch_caller(user_admin):
            rows = await list_datasources(
                fake_request, dependencies=_ds_deps(user_admin, fake_db)
            )
        assert len(rows) == 3
        assert all("credentials" not in row for row in rows)


# =============================================================================
# get_datasource endpoint
# =============================================================================


class TestGetDatasourceEndpoint:
    @pytest.mark.asyncio
    async def test_creator_gets_redacted(
        self, user_a, datasource_a, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            result = await get_datasource(
                fake_request,
                str(datasource_a["id"]),
                dependencies=_ds_deps(user_a, fake_db),
            )
        assert result["id"] == datasource_a["id"]
        assert "credentials" not in result

    @pytest.mark.asyncio
    async def test_cross_user_403(self, user_b, datasource_a, fake_db, fake_request):
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await get_datasource(
                    fake_request,
                    str(datasource_a["id"]),
                    dependencies=_ds_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_project_scoped_token_get_only_returns_its_project_link(
        self,
        user_a,
        project_a,
        project_b,
        datasource_a,
        fake_db,
        fake_request,
    ):
        scoped = {
            **user_a,
            "auth_method": "mcp",
            "scopes": [f"project:{project_a['id']}"],
        }
        fake_db.list_datasource_projects = AsyncMock(
            return_value=[str(project_a["id"]), str(project_b["id"])]
        )
        with _patch_caller(scoped):
            result = await get_datasource(
                fake_request,
                str(datasource_a["id"]),
                dependencies=_ds_deps(scoped, fake_db),
            )

        assert result["project_ids"] == [str(project_a["id"])]


# =============================================================================
# create_datasource — project-scope boundary and transactional authority
# =============================================================================


class TestCreateDatasourceEndpoint:
    @pytest.mark.asyncio
    async def test_project_scoped_token_must_create_inside_its_project(
        self, user_a, project_a, fake_db, fake_request
    ):
        scoped = {
            **user_a,
            "auth_method": "mcp",
            "scopes": [f"project:{project_a['id']}"],
        }
        fake_db.create_datasource = AsyncMock()
        with _patch_caller(scoped):
            with pytest.raises(HTTPException) as exc:
                await create_datasource(
                    DatasourceCreate(name="Database", type="generic"),
                    fake_request,
                    dependencies=_ds_deps(scoped, fake_db),
                )

        assert exc.value.status_code == 403
        assert exc.value.detail == "Access denied by MCP token scope"
        fake_db.create_datasource.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_passes_actor_to_transactional_owner_recheck(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.create_datasource = AsyncMock(
            return_value={**datasource_a, "type": "generic"}
        )
        with _patch_caller(user_a):
            with patch(
                "orchestrator.services.knowledge_projection.sync_datasource_knowledge",
                AsyncMock(),
            ):
                await create_datasource(
                    DatasourceCreate(
                        name="Database",
                        type="generic",
                        scope_mode="projects",
                        project_ids=[str(project_a["id"])],
                    ),
                    fake_request,
                    dependencies=_ds_deps(user_a, fake_db),
                )

        call = fake_db.create_datasource.await_args
        assert call.kwargs["authority_user_id"] == str(user_a["id"])
        assert call.kwargs["authority_is_admin"] is False


# =============================================================================
# update_datasource — empty credentials preserve stored value
# =============================================================================


class TestUpdateDatasourceEndpoint:
    @pytest.mark.asyncio
    async def test_creator_passes_gate(
        self, user_a, datasource_a, fake_db, fake_request
    ):
        fake_db.update_datasource = AsyncMock(return_value=True)
        fake_db.list_datasource_projects = AsyncMock(return_value=[])
        with _patch_caller(user_a):
            result = await update_datasource(
                fake_request,
                str(datasource_a["id"]),
                DatasourceUpdate(name="renamed"),
                dependencies=_ds_deps(user_a, fake_db),
            )
        assert result["id"] == datasource_a["id"]
        assert result["project_ids"] == []
        assert "credentials" not in result

    @pytest.mark.asyncio
    async def test_non_owner_403(self, user_b, datasource_a, fake_db, fake_request):
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await update_datasource(
                    fake_request,
                    str(datasource_a["id"]),
                    DatasourceUpdate(name="renamed"),
                    dependencies=_ds_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_null_credentials_preserved(
        self, user_a, datasource_a, fake_db, fake_request
    ):
        """Body without credentials → DB call gets credentials=None → stored
        secret stays intact (the real DB layer skips the column when None)."""
        fake_db.update_datasource = AsyncMock(return_value=True)
        fake_db.list_datasource_projects = AsyncMock(return_value=[])
        with _patch_caller(user_a):
            await update_datasource(
                fake_request,
                str(datasource_a["id"]),
                DatasourceUpdate(name="renamed", credentials=None),
                dependencies=_ds_deps(user_a, fake_db),
            )
        fake_db.update_datasource.assert_awaited_once()
        assert fake_db.update_datasource.await_args.kwargs["credentials"] is None

    @pytest.mark.asyncio
    async def test_empty_credentials_preserved(
        self, user_a, datasource_a, fake_db, fake_request
    ):
        """Body with empty dict credentials → same preservation as None."""
        fake_db.update_datasource = AsyncMock(return_value=True)
        fake_db.list_datasource_projects = AsyncMock(return_value=[])
        with _patch_caller(user_a):
            await update_datasource(
                fake_request,
                str(datasource_a["id"]),
                DatasourceUpdate(name="renamed", credentials={}),
                dependencies=_ds_deps(user_a, fake_db),
            )
        fake_db.update_datasource.assert_awaited_once()
        assert fake_db.update_datasource.await_args.kwargs["credentials"] is None

    @pytest.mark.asyncio
    async def test_explicit_credentials_pass_through(
        self, user_a, datasource_a, fake_db, fake_request
    ):
        fake_db.update_datasource = AsyncMock(return_value=True)
        fake_db.list_datasource_projects = AsyncMock(return_value=[])
        with _patch_caller(user_a):
            await update_datasource(
                fake_request,
                str(datasource_a["id"]),
                DatasourceUpdate(
                    name="renamed",
                    credentials={"username": "u", "password": "p"},
                ),
                dependencies=_ds_deps(user_a, fake_db),
            )
        sent = fake_db.update_datasource.await_args.kwargs["credentials"]
        assert sent is not None
        assert sent.get("username") == "u"

    @pytest.mark.asyncio
    async def test_project_scoped_token_cannot_remove_a_hidden_project_link(
        self,
        user_a,
        project_a,
        project_b,
        datasource_a,
        fake_db,
        fake_request,
    ):
        scoped = {
            **user_a,
            "auth_method": "mcp",
            "scopes": [f"project:{project_a['id']}"],
        }
        fake_db.list_datasource_projects = AsyncMock(
            return_value=[str(project_a["id"]), str(project_b["id"])]
        )
        fake_db.update_datasource_with_policy = AsyncMock()
        with _patch_caller(scoped):
            with pytest.raises(HTTPException) as exc:
                await update_datasource(
                    fake_request,
                    str(datasource_a["id"]),
                    DatasourceUpdate(
                        project_ids=[str(project_a["id"])],
                        policy_revision=1,
                    ),
                    dependencies=_ds_deps(scoped, fake_db),
                )

        assert exc.value.status_code == 403
        assert exc.value.detail == "Access denied by MCP token scope"
        fake_db.update_datasource_with_policy.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_project_token_cannot_edit_content_on_all_scope_connector(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        scoped = {
            **user_a,
            "auth_method": "mcp",
            "scopes": [f"project:{project_a['id']}"],
        }
        fake_db.get_datasource = AsyncMock(
            return_value={**datasource_a, "scope_mode": "all"}
        )
        fake_db.list_datasource_projects = AsyncMock(
            return_value=[str(project_a["id"])]
        )
        fake_db.update_datasource = AsyncMock()
        with _patch_caller(scoped):
            with pytest.raises(HTTPException) as exc:
                await update_datasource(
                    fake_request,
                    str(datasource_a["id"]),
                    DatasourceUpdate(name="renamed"),
                    dependencies=_ds_deps(scoped, fake_db),
                )

        assert exc.value.status_code == 403
        assert exc.value.detail == "Access denied by MCP token scope"
        fake_db.update_datasource.assert_not_awaited()


# =============================================================================
# delete_datasource — creator/admin only
# =============================================================================


class TestDeleteDatasourceEndpoint:
    @pytest.mark.asyncio
    async def test_creator_passes(self, user_a, datasource_a, fake_db, fake_request):
        fake_db.delete_datasource = AsyncMock(return_value=True)
        fake_db.list_datasource_projects = AsyncMock(return_value=[])
        with _patch_caller(user_a):
            result = await delete_datasource(
                fake_request,
                str(datasource_a["id"]),
                dependencies=_ds_deps(user_a, fake_db),
            )
        assert result == {"status": "deleted"}

    @pytest.mark.asyncio
    async def test_non_owner_403(self, user_b, datasource_a, fake_db, fake_request):
        fake_db.delete_datasource = AsyncMock()
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await delete_datasource(
                    fake_request,
                    str(datasource_a["id"]),
                    dependencies=_ds_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403
        fake_db.delete_datasource.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_native_project_kb_cannot_be_deleted_directly(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        native = {
            **datasource_a,
            "type": "kb",
            "config": {"native_project_id": str(project_a["id"])},
        }
        fake_db.get_datasource = AsyncMock(return_value=native)
        fake_db.delete_datasource = AsyncMock()

        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await delete_datasource(
                    fake_request,
                    str(datasource_a["id"]),
                    dependencies=_ds_deps(user_a, fake_db),
                )

        assert exc.value.status_code == 409
        assert exc.value.detail == (
            "The project knowledge connector is managed by its project"
        )
        fake_db.delete_datasource.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_failed_db_delete_does_not_remove_project_knowledge(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.list_datasource_projects = AsyncMock(
            return_value=[str(project_a["id"])]
        )
        fake_db.delete_datasource = AsyncMock(return_value=False)
        with _patch_caller(user_a):
            with patch(
                "orchestrator.services.knowledge_projection.delete_datasource_knowledge",
                AsyncMock(),
            ) as delete_note:
                with pytest.raises(HTTPException) as exc:
                    await delete_datasource(
                        fake_request,
                        str(datasource_a["id"]),
                        dependencies=_ds_deps(user_a, fake_db),
                    )

        assert exc.value.status_code == 404
        delete_note.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_project_token_cannot_delete_cross_scope_connector(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        scoped = {
            **user_a,
            "auth_method": "mcp",
            "scopes": [f"project:{project_a['id']}"],
        }
        fake_db.get_datasource = AsyncMock(
            return_value={**datasource_a, "scope_mode": "all"}
        )
        fake_db.list_datasource_projects = AsyncMock(
            return_value=[str(project_a["id"])]
        )
        fake_db.delete_datasource = AsyncMock()
        with _patch_caller(scoped):
            with pytest.raises(HTTPException) as exc:
                await delete_datasource(
                    fake_request,
                    str(datasource_a["id"]),
                    dependencies=_ds_deps(scoped, fake_db),
                )

        assert exc.value.status_code == 403
        assert exc.value.detail == "Access denied by MCP token scope"
        fake_db.delete_datasource.assert_not_awaited()


# =============================================================================
# get_job_datasources — credentials redacted in resolved-for-job response
# =============================================================================


class TestGetJobDatasourcesEndpoint:
    @pytest.mark.asyncio
    async def test_owner_gets_redacted_list(
        self, user_a, job_a, datasource_a, fake_db, fake_request
    ):
        fake_db.resolve_datasources_for_job = AsyncMock(return_value=[datasource_a])
        with _patch_caller(user_a):
            rows = await get_job_datasources(
                fake_request, str(job_a["id"]), dependencies=_ds_deps(user_a, fake_db)
            )
        assert all("credentials" not in row for row in rows)
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_cross_user_403(self, user_b, job_a, fake_db, fake_request):
        fake_db.resolve_datasources_for_job = AsyncMock()
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await get_job_datasources(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=_ds_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403
        fake_db.resolve_datasources_for_job.assert_not_awaited()


# =============================================================================
# list_project_datasources — project membership required + redacted
# =============================================================================


class TestListProjectDatasourcesEndpoint:
    @pytest.mark.asyncio
    async def test_member_gets_redacted_list(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.list_project_datasources = AsyncMock(return_value=[datasource_a])
        with _patch_caller(user_a):
            rows = await list_project_datasources(
                fake_request,
                str(project_a["id"]),
                dependencies=_proj_deps(user_a, fake_db),
            )
        assert all("credentials" not in row for row in rows)

    @pytest.mark.asyncio
    async def test_non_member_403(self, user_b, project_a, fake_db, fake_request):
        fake_db.list_project_datasources = AsyncMock()
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await list_project_datasources(
                    fake_request,
                    str(project_a["id"]),
                    dependencies=_proj_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403
        fake_db.list_project_datasources.assert_not_awaited()


# =============================================================================
# list_eligible_datasources — picker eligibility (owned + global + project)
# =============================================================================


class TestEligibleDatasourcesEndpoint:
    @pytest.mark.asyncio
    async def test_member_gets_redacted_union(
        self, user_a, project_a, datasource_a, datasource_global, fake_db, fake_request
    ):
        fake_db.list_eligible_datasources = AsyncMock(
            return_value=[datasource_a, datasource_global]
        )
        with _patch_caller(user_a):
            rows = await list_eligible_datasources(
                fake_request,
                project_id=[str(project_a["id"])],
                dependencies=_ds_deps(user_a, fake_db),
            )
        assert all("credentials" not in row for row in rows)
        assert {r["id"] for r in rows} == {datasource_a["id"], datasource_global["id"]}
        # Project ids + admin flag are forwarded to the DB layer.
        call = fake_db.list_eligible_datasources.call_args
        assert call.args[1] == [str(project_a["id"])]
        assert call.kwargs.get("is_admin") is False

    @pytest.mark.asyncio
    async def test_non_member_project_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        fake_db.list_eligible_datasources = AsyncMock()
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await list_eligible_datasources(
                    fake_request,
                    project_id=[str(project_a["id"])],
                    dependencies=_ds_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403
        fake_db.list_eligible_datasources.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_project_owned_and_global(
        self, user_a, datasource_a, datasource_global, fake_db, fake_request
    ):
        fake_db.list_eligible_datasources = AsyncMock(
            return_value=[datasource_a, datasource_global]
        )
        with _patch_caller(user_a):
            rows = await list_eligible_datasources(
                fake_request, project_id=None, dependencies=_ds_deps(user_a, fake_db)
            )
        assert len(rows) == 2
        fake_db.list_eligible_datasources.assert_awaited_once()
        assert fake_db.list_eligible_datasources.call_args.args[1] == []

    @pytest.mark.asyncio
    async def test_project_scoped_token_omission_binds_to_token_project(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        scoped = {
            **user_a,
            "auth_method": "mcp",
            "scopes": [f"project:{project_a['id']}"],
        }
        fake_db.list_eligible_datasources = AsyncMock(return_value=[datasource_a])

        with _patch_caller(scoped):
            await list_eligible_datasources(
                fake_request, project_id=None, dependencies=_ds_deps(scoped, fake_db)
            )

        assert fake_db.list_eligible_datasources.await_args.args[1] == [
            str(project_a["id"])
        ]

    @pytest.mark.asyncio
    async def test_project_scoped_token_rejects_different_or_added_context(
        self, user_a, project_a, project_b, fake_db, fake_request
    ):
        scoped = {
            **user_a,
            "auth_method": "mcp",
            "scopes": [f"project:{project_a['id']}"],
        }
        fake_db.list_eligible_datasources = AsyncMock()

        with _patch_caller(scoped):
            for requested in (
                [str(project_b["id"])],
                [str(project_a["id"]), str(project_b["id"])],
            ):
                with pytest.raises(HTTPException) as exc:
                    await list_eligible_datasources(
                        fake_request,
                        project_id=requested,
                        dependencies=_ds_deps(scoped, fake_db),
                    )
                assert exc.value.status_code == 403

        fake_db.list_eligible_datasources.assert_not_awaited()


# =============================================================================
# linkable datasource targets — paged additions + complete current selections
# =============================================================================


class TestLinkableDatasourceTargetsEndpoint:
    @pytest.mark.asyncio
    async def test_edit_response_keeps_unpaginated_selected_items(
        self, user_a, datasource_a, project_a, fake_db, fake_request
    ):
        selected = {
            "id": project_a["id"],
            "name": project_a["name"],
            "user_role": "viewer",
            "linked": True,
            "addable": False,
            "retained_only": True,
        }
        fake_db.list_linkable_datasource_targets = AsyncMock(
            return_value={
                "items": [],
                "selected_items": [selected],
                "next_cursor": None,
            }
        )

        with _patch_caller(user_a):
            result = await list_linkable_datasource_targets(
                fake_request,
                datasource_id=str(datasource_a["id"]),
                q="does-not-match",
                limit=10,
                cursor=None,
                dependencies=_ds_deps(user_a, fake_db),
            )

        assert result["items"] == []
        assert result["selected_items"] == [selected]
        call = fake_db.list_linkable_datasource_targets.await_args
        assert call.kwargs["datasource_id"] == str(datasource_a["id"])
        assert call.kwargs["q"] == "does-not-match"
        assert call.kwargs["limit"] == 10


# =============================================================================
# link / patch / unlink — project-owner gate
# =============================================================================


class TestProjectLinkableDatasourcesEndpoint:
    @pytest.mark.asyncio
    async def test_owner_gets_redacted_target_aware_page(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.list_project_linkable_datasources = AsyncMock(
            return_value={
                "items": [datasource_a],
                "next_cursor": "next-page",
            }
        )
        with _patch_caller(user_a):
            result = await list_project_linkable_datasources(
                fake_request,
                str(project_a["id"]),
                q="data",
                limit=25,
                cursor=None,
                dependencies=_proj_deps(user_a, fake_db),
            )

        assert result["next_cursor"] == "next-page"
        assert "credentials" not in result["items"][0]
        call = fake_db.list_project_linkable_datasources.await_args
        assert call.args == (str(user_a["id"]), str(project_a["id"]))
        assert call.kwargs == {
            "is_admin": False,
            "q": "data",
            "limit": 25,
            "cursor": None,
        }

    @pytest.mark.asyncio
    async def test_project_token_cannot_list_candidates_for_another_project(
        self, user_a, project_a, project_b, fake_db, fake_request
    ):
        scoped = {
            **user_a,
            "auth_method": "mcp",
            "scopes": [f"project:{project_b['id']}"],
        }
        fake_db.list_project_linkable_datasources = AsyncMock()
        with _patch_caller(scoped):
            with pytest.raises(HTTPException) as exc:
                await list_project_linkable_datasources(
                    fake_request,
                    str(project_a["id"]),
                    dependencies=_proj_deps(scoped, fake_db),
                )

        assert exc.value.status_code == 403
        fake_db.list_project_linkable_datasources.assert_not_awaited()


class TestLinkDatasourceToProjectEndpoint:
    @pytest.mark.asyncio
    async def test_kb_link_is_forced_read_only(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        kb = {**datasource_a, "type": "kb"}
        fake_db.get_datasource = AsyncMock(return_value=kb)
        fake_db.link_datasource_to_project = AsyncMock(return_value=None)
        with _patch_caller(user_a):
            with patch(
                "orchestrator.services.knowledge_projection.sync_datasource_knowledge",
                AsyncMock(),
            ):
                await link_datasource_to_project(
                    fake_request,
                    str(project_a["id"]),
                    str(datasource_a["id"]),
                    ProjectDatasourceSettings(read_only=False),
                    dependencies=_proj_deps(user_a, fake_db),
                )

        assert fake_db.link_datasource_to_project.await_args.kwargs["read_only"] is True

    @pytest.mark.asyncio
    async def test_owner_links_their_own_datasource(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.link_datasource_to_project = AsyncMock(return_value=None)
        with _patch_caller(user_a):
            with patch(
                "orchestrator.services.knowledge_projection.sync_datasource_knowledge",
                AsyncMock(),
            ):
                result = await link_datasource_to_project(
                    fake_request,
                    str(project_a["id"]),
                    str(datasource_a["id"]),
                    dependencies=_proj_deps(user_a, fake_db),
                )
        assert result == {"status": "linked"}
        call = fake_db.link_datasource_to_project.await_args
        assert call.kwargs["authority_user_id"] == str(user_a["id"])
        assert call.kwargs["authority_is_admin"] is False

    @pytest.mark.asyncio
    async def test_transactional_owner_recheck_failure_maps_to_generic_403(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.link_datasource_to_project = AsyncMock(
            side_effect=DatasourceProjectAuthorizationError("race lost")
        )
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await link_datasource_to_project(
                    fake_request,
                    str(project_a["id"]),
                    str(datasource_a["id"]),
                    dependencies=_proj_deps(user_a, fake_db),
                )

        assert exc.value.status_code == 403
        assert exc.value.detail == "Not authorized to add one or more project links"

    @pytest.mark.asyncio
    async def test_non_owner_403(
        self, user_b, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.link_datasource_to_project = AsyncMock()
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await link_datasource_to_project(
                    fake_request,
                    str(project_a["id"]),
                    str(datasource_a["id"]),
                    dependencies=_proj_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403
        fake_db.link_datasource_to_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_owner_cant_link_stranger_datasource(
        self, user_a, project_a, datasource_b, fake_db, fake_request
    ):
        """user_a owns project_a, but datasource_b is user_b's. Even though
        user_a is the project owner, they shouldn't be able to link a
        datasource they can't see — that would be a UUID-enumeration probe."""
        fake_db.link_datasource_to_project = AsyncMock()
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await link_datasource_to_project(
                    fake_request,
                    str(project_a["id"]),
                    str(datasource_b["id"]),
                    dependencies=_proj_deps(user_a, fake_db),
                )
        assert exc.value.status_code == 403
        fake_db.link_datasource_to_project.assert_not_awaited()


class TestUpdateProjectDatasourceEndpoint:
    @pytest.mark.asyncio
    async def test_owner_passes(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.update_project_datasource = AsyncMock(return_value=True)
        with _patch_caller(user_a):
            with patch(
                "orchestrator.services.knowledge_projection.sync_datasource_knowledge",
                AsyncMock(),
            ):
                result = await update_project_datasource(
                    fake_request,
                    str(project_a["id"]),
                    str(datasource_a["id"]),
                    ProjectDatasourceSettings(read_only=True),
                    dependencies=_proj_deps(user_a, fake_db),
                )
        assert result == {"status": "updated"}
        call = fake_db.update_project_datasource.await_args
        assert call.kwargs["authority_user_id"] == str(user_a["id"])
        assert call.kwargs["authority_is_admin"] is False

    @pytest.mark.asyncio
    async def test_transactional_owner_demotion_maps_to_403(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.update_project_datasource = AsyncMock(
            side_effect=DatasourceProjectAuthorizationError("race lost")
        )
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await update_project_datasource(
                    fake_request,
                    str(project_a["id"]),
                    str(datasource_a["id"]),
                    ProjectDatasourceSettings(read_only=True),
                    dependencies=_proj_deps(user_a, fake_db),
                )

        assert exc.value.status_code == 403
        assert exc.value.detail == (
            "Not authorized to modify this project connector link"
        )

    @pytest.mark.asyncio
    async def test_non_owner_403(
        self, user_b, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.update_project_datasource = AsyncMock()
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await update_project_datasource(
                    fake_request,
                    str(project_a["id"]),
                    str(datasource_a["id"]),
                    ProjectDatasourceSettings(read_only=True),
                    dependencies=_proj_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403
        fake_db.update_project_datasource.assert_not_awaited()


class TestUnlinkDatasourceFromProjectEndpoint:
    @pytest.mark.asyncio
    async def test_owner_unlinks(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.unlink_datasource_from_project = AsyncMock(return_value=True)
        with _patch_caller(user_a):
            with patch(
                "orchestrator.services.knowledge_projection.delete_datasource_knowledge",
                AsyncMock(),
            ):
                result = await unlink_datasource_from_project(
                    fake_request,
                    str(project_a["id"]),
                    str(datasource_a["id"]),
                    dependencies=_proj_deps(user_a, fake_db),
                )
        assert result == {"status": "unlinked"}
        call = fake_db.unlink_datasource_from_project.await_args
        assert call.kwargs["authority_user_id"] == str(user_a["id"])
        assert call.kwargs["authority_is_admin"] is False

    @pytest.mark.asyncio
    async def test_transactional_authority_loss_maps_to_403(
        self, user_a, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.unlink_datasource_from_project = AsyncMock(
            side_effect=DatasourceProjectAuthorizationError("race lost")
        )
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await unlink_datasource_from_project(
                    fake_request,
                    str(project_a["id"]),
                    str(datasource_a["id"]),
                    dependencies=_proj_deps(user_a, fake_db),
                )

        assert exc.value.status_code == 403
        assert exc.value.detail == (
            "Not authorized to modify this project connector link"
        )

    @pytest.mark.asyncio
    async def test_non_owner_403(
        self, user_b, project_a, datasource_a, fake_db, fake_request
    ):
        fake_db.unlink_datasource_from_project = AsyncMock()
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await unlink_datasource_from_project(
                    fake_request,
                    str(project_a["id"]),
                    str(datasource_a["id"]),
                    dependencies=_proj_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403
        fake_db.unlink_datasource_from_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_connector_owner_project_token_cannot_unlink_another_project(
        self,
        user_a,
        project_a,
        project_b,
        datasource_a,
        fake_db,
        fake_request,
    ):
        scoped = {
            **user_a,
            "auth_method": "mcp",
            "scopes": [f"project:{project_a['id']}"],
        }
        fake_db.unlink_datasource_from_project = AsyncMock()
        with _patch_caller(scoped):
            with pytest.raises(HTTPException) as exc:
                await unlink_datasource_from_project(
                    fake_request,
                    str(project_b["id"]),
                    str(datasource_a["id"]),
                    dependencies=_proj_deps(scoped, fake_db),
                )

        assert exc.value.status_code == 403
        assert exc.value.detail == "Access denied by MCP token scope"
        fake_db.unlink_datasource_from_project.assert_not_awaited()
