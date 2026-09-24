"""G2 — multi-tenancy gates on the projects family.

Covers 9 reads + 5 mutations under ``/api/projects`` and
``/api/projects/{id}/...``:

Reads (gate: ``require_project_member`` at viewer):
    GET    /api/projects                          (list_projects)
    GET    /api/projects/{id}                     (get_project)
    GET    /api/projects/{id}/members
    GET    /api/projects/{id}/contacts             (routers/contacts.py)
    GET    /api/projects/{id}/memory/stats
    GET    /api/projects/{id}/experts
    GET    /api/projects/{id}/experts/{name}
    GET    /api/projects/{id}/repositories
    GET    /api/projects/{id}/jobs

Mutations:
    POST   /api/projects/{id}/contacts            (editor, routers/contacts.py)
    POST   /api/projects/{id}/repositories        (owner)
    PATCH  /api/projects/{id}/repositories/{id}   (owner)
    DELETE /api/projects/{id}/repositories/{id}   (owner)
    POST   /api/projects/{id}/jobs                (editor)

``list_projects`` semantics:
    * Admin sees full list (or `?user_id=` cross-user)
    * Non-admin auto-restricted to caller's memberships
    * Non-admin `?user_id=<other>` → 403
    * MCP `project:<uuid>` scope filters the result post-fetch
"""

from tests._expert_catalog import catalogue_route
from orchestrator.routers import expert_catalog as expert_routes


from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from fastapi import HTTPException

from orchestrator.routers.citations import get_project_memory_stats
from orchestrator.routers.projects import (
    add_project_repository,
    get_project,
    list_project_members,
    list_project_repositories,
    list_projects,
    remove_project_repository,
    update_project,
    update_project_repository,
)
from orchestrator.schemas.projects import (
    ProjectRepositoryCreate,
    ProjectRepositoryUpdate,
    ProjectUpdate,
)
from orchestrator.application import jobs as jobs_composition
from orchestrator.routers import job_reads as job_reads_module


# =============================================================================
# Patch helpers
# =============================================================================


def _patch_caller_and_db(user: dict, db):
    """Stack the patches every endpoint test needs.

    Still the right shape for handlers whose dependencies the application
    builds from its resources at call time (``list_project_jobs``,
    ``create_project_job`` via ``_with_job_lifecycle_factory``, the
    expert-catalogue routes). Routes extracted in R1.B03 read their caller gate
    and their store off a dependency dataclass instead — those tests use
    ``_patch_caller`` plus ``_proj_deps`` / ``_citation_deps``.
    """
    stack = ExitStack()
    stack.enter_context(
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    return stack


def _with_job_lifecycle_factory(fake_request):
    """Serve the application's per-request job lifecycle dependency factory.

    ``create_project_job`` reads ``request.app.state``; the application's
    factory builds the dependencies from its resources at call time, so it
    sees the patched ``postgres_db``.
    """
    import orchestrator.main

    fake_request.app.state.job_lifecycle_route_dependencies_factory = (
        orchestrator.main.app.state.job_lifecycle_route_dependencies_factory
    )
    return fake_request


def _patch_caller(user: dict):
    """Patch the caller resolution the *real* gates read.

    ``require_project_member`` / ``require_project_owner`` / ``require_job_access``
    stay real in these tests — they are the subject — and each resolves its
    caller through ``orchestrator.security.access.require_approved_user`` as a
    module global, so this patch still intercepts it.
    """
    return patch(
        "orchestrator.security.access.require_approved_user",
        AsyncMock(return_value=user),
    )


def _proj_deps(
    user: dict,
    db,
    *,
    forge=None,
    main_cloud_router=None,
    require_approved_user=None,
    require_project_owner=None,
):
    """The projects router's collaborators, composed as main's
    ``_projects_dependencies`` factory composes them.

    Substitutes exactly what this file used to patch on ``orchestrator.main``:
    the store, ``gitea_client`` (``forge``), ``main_cloud_router``, and the
    caller/owner gates where a test stubbed them. Every other gate keeps its
    dataclass default — the real function from ``orchestrator.security.access``.
    """
    from orchestrator.routers.projects import ProjectsDependencies
    from orchestrator.services import project_provisioning, projects

    async def _no_admin(*_args, **_kwargs):
        raise AssertionError("no route in this file may escalate to admin")

    forge = MagicMock(is_initialized=False) if forge is None else forge
    if main_cloud_router is None:
        main_cloud_router = MagicMock(for_project_optional=MagicMock(return_value=None))
    gates: dict = {}
    if require_approved_user is not None:
        gates["require_approved_user"] = require_approved_user
    else:
        gates["require_approved_user"] = AsyncMock(return_value=user)
    if require_project_owner is not None:
        gates["require_project_owner"] = require_project_owner

    return ProjectsDependencies(
        store=db,
        operations=projects.ProjectDependencies(
            store=db,
            vector_db=MagicMock(),
            forge=forge,
            keycloak_groups=MagicMock(is_initialized=False),
            main_cloud_router=main_cloud_router,
            logger=MagicMock(),
            provisioning=project_provisioning.ProjectProvisioningDependencies(
                store=db,
                forge=forge,
                keycloak_groups=MagicMock(is_initialized=False),
                main_cloud_router=main_cloud_router,
                logger=MagicMock(),
                repair=project_provisioning.ProjectRepairState(),
                knowledge_index=MagicMock(),
            ),
            with_validated_tool_overrides=lambda value: value,
        ),
        require_admin=_no_admin,
        **gates,
    )


def _citation_deps(user: dict, db, *, vector_db=None):
    """The citations router's collaborators, composed as main's
    ``_citations_dependencies`` factory composes them.

    ``vector_db`` is a parameter because this file uses it as a tripwire: the
    memory-stats gate must refuse before anything touches the vector pool.
    """
    from orchestrator.routers.citations import CitationsDependencies
    from orchestrator.services import citations

    return CitationsDependencies(
        store=db,
        operations=citations.CitationDependencies(
            store=db,
            vector_db=MagicMock() if vector_db is None else vector_db,
            snapshot_service=MagicMock(),
            main_cloud_router=MagicMock(),
            logger=MagicMock(),
        ),
        require_approved_user=AsyncMock(return_value=user),
    )


def _scoped(user: dict, scope: str) -> dict:
    out = dict(user)
    out["scopes"] = [scope]
    out["auth_method"] = "mcp"
    return out


def _set_role(db, project_id, user_id, role):
    """Override ``get_user_role_in_project`` to assign a specific role."""
    pid = UUID(str(project_id))
    uid = UUID(str(user_id))

    async def lookup(p, u):
        if UUID(str(p)) == pid and UUID(str(u)) == uid:
            return role
        return None

    db.get_user_role_in_project = AsyncMock(side_effect=lookup)


# Fake acquire() for the admin list_projects path.
def _patch_admin_list_fetch(projects):
    """Patch `postgres_db.acquire()` to return rows for the admin path."""
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=projects)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=None)
    return ctx


# =============================================================================
# list_projects — visibility model
# =============================================================================


class TestListProjects:
    @pytest.mark.asyncio
    async def test_non_admin_uses_get_projects_for_user(
        self, user_a, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            result = await list_projects(
                fake_request, user_id=None, dependencies=_proj_deps(user_a, fake_db)
            )

        fake_db.get_projects_for_user.assert_awaited_with(
            str(user_a["id"]), statuses=["active"]
        )
        assert len(result) == 1  # user_a is owner of project_a only

    @pytest.mark.asyncio
    async def test_non_admin_cross_user_query_403(
        self, user_a, user_b, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await list_projects(
                    fake_request,
                    user_id=str(user_b["id"]),
                    dependencies=_proj_deps(user_a, fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_non_admin_self_query_allowed(self, user_a, fake_db, fake_request):
        with _patch_caller(user_a):
            result = await list_projects(
                fake_request,
                user_id=str(user_a["id"]),
                dependencies=_proj_deps(user_a, fake_db),
            )
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_admin_no_user_id_uses_admin_view(
        self, user_admin, project_a, project_b, fake_db, fake_request
    ):
        fake_db.fetchrow = AsyncMock(return_value=None)
        fake_db.acquire = MagicMock(
            return_value=_patch_admin_list_fetch([project_a, project_b])
        )
        with _patch_caller(user_admin):
            result = await list_projects(
                fake_request, user_id=None, dependencies=_proj_deps(user_admin, fake_db)
            )
        assert len(result) == 2
        # The non-admin helper must NOT be called for admin
        fake_db.get_projects_for_user.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_admin_cross_user_query_uses_helper(
        self, user_admin, user_a, fake_db, fake_request
    ):
        with _patch_caller(user_admin):
            await list_projects(
                fake_request,
                user_id=str(user_a["id"]),
                dependencies=_proj_deps(user_admin, fake_db),
            )
        fake_db.get_projects_for_user.assert_awaited_with(
            str(user_a["id"]), statuses=["active"]
        )

    @pytest.mark.asyncio
    async def test_mcp_project_scope_narrows_admin(
        self, user_admin, project_a, project_b, fake_db, fake_request
    ):
        fake_db.fetchrow = AsyncMock(return_value=None)
        scoped = _scoped(user_admin, f"project:{project_a['id']}")
        fake_db.acquire = MagicMock(
            return_value=_patch_admin_list_fetch([project_a, project_b])
        )
        with _patch_caller(scoped):
            result = await list_projects(
                fake_request, user_id=None, dependencies=_proj_deps(scoped, fake_db)
            )
        # Only project_a should remain after scope filter.
        assert len(result) == 1
        assert result[0]["id"] == project_a["id"]

    @pytest.mark.asyncio
    async def test_mcp_project_scope_narrows_non_admin(
        self, user_a, project_a, project_b, fake_db, fake_request
    ):
        scoped = _scoped(user_a, f"project:{project_b['id']}")
        with _patch_caller(scoped):
            result = await list_projects(
                fake_request, user_id=None, dependencies=_proj_deps(scoped, fake_db)
            )
        # user_a's memberships filtered against project_b → empty.
        assert result == []

    @pytest.mark.asyncio
    async def test_unauthenticated_baseline(self, fake_db, fake_request):
        with pytest.raises(HTTPException) as exc:
            await list_projects(
                fake_request,
                user_id=None,
                dependencies=_proj_deps(
                    {},
                    fake_db,
                    require_approved_user=AsyncMock(
                        side_effect=HTTPException(status_code=401)
                    ),
                ),
            )
        assert exc.value.status_code == 401


# =============================================================================
# get_project — viewer-minimum gate, no over-fetch
# =============================================================================


class TestGetProject:
    @pytest.mark.asyncio
    async def test_owner_passes(self, user_a, project_a, fake_db, fake_request):
        router = MagicMock()
        router.for_project_optional.return_value.is_initialized = False
        with (
            _patch_caller(user_a),
            patch(
                "orchestrator.services.project_provisioning."
                "ensure_project_cloud_resources",
                AsyncMock(side_effect=lambda p, **_: p),
            ),
        ):
            result = await get_project(
                fake_request,
                str(project_a["id"]),
                dependencies=_proj_deps(user_a, fake_db, main_cloud_router=router),
            )
        assert result["id"] == project_a["id"]

    @pytest.mark.asyncio
    async def test_cross_user_403(self, user_b, project_a, fake_db, fake_request):
        # _ensure_project_cloud_resources must NOT fire — gate first.
        sentinel = AsyncMock(side_effect=AssertionError("side effect ran"))
        with (
            _patch_caller(user_b),
            patch(
                "orchestrator.services.project_provisioning."
                "ensure_project_cloud_resources",
                sentinel,
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_project(
                    fake_request,
                    str(project_a["id"]),
                    dependencies=_proj_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403
        sentinel.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_404(self, user_a, fake_db, fake_request):
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await get_project(
                    fake_request,
                    "00000000-0000-0000-0000-000000000999",
                    dependencies=_proj_deps(user_a, fake_db),
                )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_admin_bypass(self, user_admin, project_a, fake_db, fake_request):
        router = MagicMock()
        router.for_project_optional.return_value.is_initialized = False
        with (
            _patch_caller(user_admin),
            patch(
                "orchestrator.services.project_provisioning."
                "ensure_project_cloud_resources",
                AsyncMock(side_effect=lambda p, **_: p),
            ),
        ):
            result = await get_project(
                fake_request,
                str(project_a["id"]),
                dependencies=_proj_deps(user_admin, fake_db, main_cloud_router=router),
            )
        assert result["id"] == project_a["id"]

    @pytest.mark.asyncio
    async def test_viewer_passes(self, user_b, project_a, fake_db, fake_request):
        """user_b granted viewer on project_a → can read it."""
        _set_role(fake_db, project_a["id"], user_b["id"], "viewer")
        router = MagicMock()
        router.for_project_optional.return_value.is_initialized = False
        with (
            _patch_caller(user_b),
            patch(
                "orchestrator.services.project_provisioning."
                "ensure_project_cloud_resources",
                AsyncMock(side_effect=lambda p, **_: p),
            ),
        ):
            result = await get_project(
                fake_request,
                str(project_a["id"]),
                dependencies=_proj_deps(user_b, fake_db, main_cloud_router=router),
            )
        assert result["id"] == project_a["id"]


# =============================================================================
# Read endpoints — gate fires before downstream service
# =============================================================================


def _explode(name: str):
    return MagicMock(side_effect=AssertionError(f"{name} called past the gate"))


class TestProjectReadGates:
    @pytest.mark.asyncio
    async def test_list_members_blocked_cross_user(
        self, user_b, project_a, fake_db, fake_request
    ):
        fake_db.get_project_members = AsyncMock(
            side_effect=AssertionError("get_project_members called past the gate")
        )
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await list_project_members(
                    fake_request,
                    str(project_a["id"]),
                    dependencies=_proj_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_contacts_blocked_cross_user(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.contacts import (
            ContactsDependencies,
            get_project_contacts,
        )

        gate = AsyncMock(side_effect=HTTPException(status_code=403, detail="denied"))
        fake_db.get_project_contacts = AsyncMock(
            side_effect=AssertionError("get_project_contacts called past the gate")
        )
        dependencies = ContactsDependencies(db=fake_db, require_project_member=gate)
        with pytest.raises(HTTPException) as exc:
            await get_project_contacts(
                fake_request, str(project_a["id"]), dependencies=dependencies
            )
        assert exc.value.status_code == 403
        gate.assert_awaited_once_with(fake_request, fake_db, str(project_a["id"]))

    @pytest.mark.asyncio
    async def test_memory_stats_blocked_cross_user(
        self, user_b, project_a, fake_db, fake_request
    ):
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await get_project_memory_stats(
                    fake_request,
                    str(project_a["id"]),
                    dependencies=_citation_deps(
                        user_b, fake_db, vector_db=_explode("vector_db")
                    ),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_experts_blocked_cross_user(
        self, user_b, project_a, fake_db, fake_request
    ):
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch(
                "orchestrator.main.app.state.resources.gitea_client",
                _explode("gitea_client"),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await catalogue_route(expert_routes.list_project_experts)(
                    fake_request, str(project_a["id"])
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_get_expert_blocked_cross_user(
        self, user_b, project_a, fake_db, fake_request
    ):
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch(
                "orchestrator.main.app.state.resources.gitea_client",
                _explode("gitea_client"),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await catalogue_route(expert_routes.get_project_expert)(
                    fake_request, str(project_a["id"]), "scholar"
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_repositories_blocked_cross_user(
        self, user_b, project_a, fake_db, fake_request
    ):
        fake_db.get_project_repositories = AsyncMock(
            side_effect=AssertionError("get_project_repositories called past the gate")
        )
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await list_project_repositories(
                    fake_request,
                    str(project_a["id"]),
                    role=None,
                    dependencies=_proj_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_list_project_jobs_blocked_cross_user(
        self, user_b, project_a, fake_db, fake_request
    ):
        import orchestrator.main

        # acquire shouldn't be reached
        fake_db.acquire = MagicMock(
            side_effect=AssertionError("acquire called past the gate")
        )
        with _patch_caller_and_db(user_b, fake_db):
            with pytest.raises(HTTPException) as exc:
                await job_reads_module.list_project_jobs(
                    fake_request,
                    str(project_a["id"]),
                    status=None,
                    limit=100,
                    dependencies=jobs_composition.job_reads_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 403


# =============================================================================
# Read endpoints — happy paths (owner / viewer / admin reach the service)
# =============================================================================


class TestProjectReadHappyPaths:
    @pytest.mark.asyncio
    async def test_list_members_owner_reaches_db(
        self, user_a, project_a, fake_db, fake_request
    ):
        fake_db.get_project_members = AsyncMock(
            return_value=[{"user_id": str(user_a["id"]), "role": "owner"}]
        )
        with _patch_caller(user_a):
            result = await list_project_members(
                fake_request,
                str(project_a["id"]),
                dependencies=_proj_deps(user_a, fake_db),
            )
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_list_members_admin_bypass(
        self, user_admin, project_a, fake_db, fake_request
    ):
        fake_db.get_project_members = AsyncMock(return_value=[])
        with _patch_caller(user_admin):
            await list_project_members(
                fake_request,
                str(project_a["id"]),
                dependencies=_proj_deps(user_admin, fake_db),
            )
        fake_db.get_project_members.assert_awaited_once()


# =============================================================================
# Mutations — role enforcement (editor vs owner)
# =============================================================================


class TestProjectMutationRoles:
    @pytest.mark.asyncio
    async def test_add_contact_viewer_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.contacts import (
            ContactCreate,
            ContactsDependencies,
            add_project_contact,
        )

        gate = AsyncMock(
            side_effect=HTTPException(
                status_code=403, detail="Project role 'editor' or higher required"
            )
        )
        fake_db.create_contact = AsyncMock(
            side_effect=AssertionError("create_contact called past the gate")
        )
        body = ContactCreate(display_name="X", addresses=[])
        dependencies = ContactsDependencies(db=fake_db, require_project_member=gate)
        with pytest.raises(HTTPException) as exc:
            await add_project_contact(
                fake_request, str(project_a["id"]), body, dependencies=dependencies
            )
        assert exc.value.status_code == 403
        gate.assert_awaited_once_with(
            fake_request,
            fake_db,
            str(project_a["id"]),
            min_role="editor",
            allow_archived=False,
        )

    @pytest.mark.asyncio
    async def test_add_contact_editor_passes(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.contacts import (
            ContactCreate,
            ContactsDependencies,
            add_project_contact,
        )

        gate = AsyncMock(return_value=(user_b, project_a))
        fake_db.list_contacts_for_user = AsyncMock(return_value=[])
        fake_db.create_contact = AsyncMock(return_value={"id": "new-contact"})
        fake_db.add_contact_address = AsyncMock(return_value={"id": "addr-1"})
        fake_db.get_contact = AsyncMock(
            return_value={"id": "new-contact", "display_name": "X", "addresses": []}
        )
        fake_db.link_contact_to_project = AsyncMock(return_value=True)
        body = ContactCreate(display_name="X", addresses=[])
        dependencies = ContactsDependencies(db=fake_db, require_project_member=gate)
        result = await add_project_contact(
            fake_request, str(project_a["id"]), body, dependencies=dependencies
        )
        assert result["contact"]["id"] == "new-contact"
        fake_db.link_contact_to_project.assert_awaited_once_with(
            str(project_a["id"]), "new-contact", user_b["id"]
        )

    @pytest.mark.asyncio
    async def test_add_repository_editor_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        _set_role(fake_db, project_a["id"], user_b["id"], "editor")
        fake_db.add_project_repository = AsyncMock(
            side_effect=AssertionError("add_project_repository called past the gate")
        )
        body = ProjectRepositoryCreate(name="r", repo_url="https://example.test/r.git")
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await add_project_repository(
                    fake_request,
                    str(project_a["id"]),
                    body,
                    dependencies=_proj_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_add_repository_owner_passes(
        self, user_a, project_a, fake_db, fake_request
    ):
        fake_db.add_project_repository = AsyncMock(return_value={"id": "new"})
        body = ProjectRepositoryCreate(name="r", repo_url="https://example.test/r.git")
        gitea = MagicMock()
        gitea.is_initialized = False
        with _patch_caller(user_a):
            result = await add_project_repository(
                fake_request,
                str(project_a["id"]),
                body,
                dependencies=_proj_deps(user_a, fake_db, forge=gitea),
            )
        assert result == {"id": "new"}

    @pytest.mark.asyncio
    async def test_update_repository_editor_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        _set_role(fake_db, project_a["id"], user_b["id"], "editor")
        fake_db.update_project_repository = AsyncMock(
            side_effect=AssertionError("update called past the gate")
        )
        body = ProjectRepositoryUpdate(description="x")
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await update_project_repository(
                    fake_request,
                    str(project_a["id"]),
                    "repo-id",
                    body,
                    dependencies=_proj_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_managed_repository_read_only_patch_rotates_before_row_update(
        self, user_a, project_a, fake_db, fake_request
    ):
        repository = {
            "id": "repo-id",
            "project_id": project_a["id"],
            "name": "project-source",
            "role": "source",
            "read_only": False,
            "is_managed": True,
        }
        fake_db.get_project_repository = AsyncMock(return_value=repository)
        fake_db.update_project_repository = AsyncMock(return_value=True)
        events: list[str] = []

        async def rotate(*_args, **kwargs):
            events.append("rotate")
            assert "force" not in kwargs
            assert _args[2]["read_only"] is True
            return {"access_mode": "read"}

        async def update(*_args, **_kwargs):
            assert events == ["rotate"]
            events.append("update")
            return True

        fake_db.update_project_repository.side_effect = update
        with (
            _patch_caller(user_a),
            patch(
                "orchestrator.services.projects.rotate_project_repository_authority",
                AsyncMock(side_effect=rotate),
            ) as rotation,
        ):
            result = await update_project_repository(
                fake_request,
                str(project_a["id"]),
                "repo-id",
                ProjectRepositoryUpdate(read_only=True),
                dependencies=_proj_deps(user_a, fake_db),
            )

        assert result == {"status": "updated"}
        assert events == ["rotate", "update"]
        rotation.assert_awaited_once()
        fake_db.update_project_repository.assert_awaited_once_with(
            "repo-id", read_only=True
        )

    @pytest.mark.asyncio
    async def test_remove_repository_editor_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        _set_role(fake_db, project_a["id"], user_b["id"], "editor")
        fake_db.get_project_repository = AsyncMock(
            side_effect=AssertionError("get_project_repository called past the gate")
        )
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await remove_project_repository(
                    fake_request,
                    str(project_a["id"]),
                    "repo-id",
                    dependencies=_proj_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_remove_repository_owner_passes(
        self, user_a, project_a, fake_db, fake_request
    ):
        fake_db.get_project_repository = AsyncMock(
            return_value={
                "id": "r",
                "project_id": project_a["id"],
                "role": "doc",
                "is_managed": False,
                "name": "r",
            }
        )
        fake_db.remove_project_repository = AsyncMock(
            return_value={"is_managed": False, "name": "r"}
        )
        gitea = MagicMock()
        gitea.is_initialized = False
        with _patch_caller(user_a):
            result = await remove_project_repository(
                fake_request,
                str(project_a["id"]),
                "repo-id",
                dependencies=_proj_deps(user_a, fake_db, forge=gitea),
            )
        assert result == {"status": "removed"}

    @pytest.mark.asyncio
    async def test_create_project_job_viewer_403(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.schemas.job_create import JobCreate
        from orchestrator.routers.project_jobs import create_project_job

        _set_role(fake_db, project_a["id"], user_b["id"], "viewer")
        body = JobCreate(description="x")
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch(
                "orchestrator.routers.job_lifecycle.admit_job_request",
                AsyncMock(
                    side_effect=AssertionError("create_job called past the gate")
                ),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_project_job(
                    _with_job_lifecycle_factory(fake_request),
                    str(project_a["id"]),
                    body,
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_create_project_job_editor_passes(
        self, user_b, project_a, fake_db, fake_request
    ):
        from orchestrator.schemas.job_create import JobCreate
        from orchestrator.routers.project_jobs import create_project_job

        _set_role(fake_db, project_a["id"], user_b["id"], "editor")
        body = JobCreate(description="x")
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch(
                "orchestrator.routers.job_lifecycle.admit_job_request",
                AsyncMock(return_value={"id": "new-job"}),
            ),
        ):
            result = await create_project_job(
                _with_job_lifecycle_factory(fake_request),
                str(project_a["id"]),
                body,
            )
        assert result == {"id": "new-job"}
        assert body.project_id == str(project_a["id"])


# =============================================================================
# Archived projects — server-side lifecycle
# =============================================================================
#
# knowledge-base/knowledge/features/project_and_job_list_filtering.md §4.2,
# §4.3a, §4.5. The guard-flag unit behaviour lives in
# tests/test_security_access.py; the five internal creation seams live in
# tests/test_archived_projects_refuse_new_work.py. This section is the
# per-endpoint matrix.


@pytest.fixture
def archived_project(project_b):
    """``project_b`` archived — user_b owns it, user_a is not a member."""
    project_b["status"] = "archived"
    return project_b


class TestListProjectsStatusFilter:
    @pytest.mark.asyncio
    async def test_default_excludes_archived(
        self, user_b, archived_project, fake_db, fake_request
    ):
        with _patch_caller(user_b):
            result = await list_projects(
                fake_request, user_id=None, dependencies=_proj_deps(user_b, fake_db)
            )

        # user_b owns exactly one project, and it is archived.
        assert result == []
        assert fake_db.get_projects_for_user.await_args.kwargs["statuses"] == ["active"]

    @pytest.mark.asyncio
    async def test_status_archived_returns_the_archive(
        self, user_b, archived_project, fake_db, fake_request
    ):
        with _patch_caller(user_b):
            result = await list_projects(
                fake_request,
                user_id=None,
                status=["archived"],
                dependencies=_proj_deps(user_b, fake_db),
            )

        assert [p["id"] for p in result] == [archived_project["id"]]

    @pytest.mark.asyncio
    async def test_both_statuses_return_everything(
        self, user_b, archived_project, fake_db, fake_request
    ):
        with _patch_caller(user_b):
            result = await list_projects(
                fake_request,
                user_id=None,
                status=["active", "archived"],
                dependencies=_proj_deps(user_b, fake_db),
            )

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_a_null_status_project_stays_visible(
        self, user_b, project_b, fake_db, fake_request
    ):
        """Fail toward showing (§4.2): the column is nullable and the CHECK
        passes on NULL, so a NULL row must behave as active rather than
        vanishing from every listing."""
        project_b["status"] = None
        with _patch_caller(user_b):
            result = await list_projects(
                fake_request, user_id=None, dependencies=_proj_deps(user_b, fake_db)
            )

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_admin_branch_binds_the_statuses_and_drops_the_dead_filter(
        self, user_admin, project_a, project_b, fake_db, fake_request
    ):
        fake_db.fetchrow = AsyncMock(return_value=None)
        ctx = _patch_admin_list_fetch([project_a, project_b])
        fake_db.acquire = MagicMock(return_value=ctx)
        with _patch_caller(user_admin):
            await list_projects(
                fake_request,
                user_id=None,
                status=["archived"],
                dependencies=_proj_deps(user_admin, fake_db),
            )

        conn = await ctx.__aenter__()
        sql, statuses = conn.fetch.await_args.args
        assert statuses == ["archived"]
        # `status != 'deleted'` was dead code: valid_project_status has no
        # such value, so it never excluded a single row.
        assert "deleted" not in sql
        assert "COALESCE(status, 'active')" in sql


class TestUpdateProjectOnAnArchivedProject:
    """§4.3a — status-only while archived, and never a partial apply."""

    def _patched(self, user, db, project):
        """Caller patch only — the owner gate is stubbed through ``_deps``,
        which is where the extracted route now reads it from."""
        return _patch_caller(user)

    def _deps(self, user, db, project):
        return _proj_deps(
            user,
            db,
            require_project_owner=AsyncMock(return_value=(user, project)),
        )

    @pytest.mark.asyncio
    async def test_unarchive_is_allowed(
        self, user_b, archived_project, fake_db, fake_request
    ):
        fake_db.update_project = AsyncMock(return_value=True)
        with self._patched(user_b, fake_db, archived_project):
            result = await update_project(
                str(archived_project["id"]),
                ProjectUpdate(status="active"),
                fake_request,
                dependencies=self._deps(user_b, fake_db, archived_project),
            )

        assert result["status"] == "updated"
        assert fake_db.update_project.await_args.kwargs == {"status": "active"}
        # Unarchive does NOT auto-resume the children it quiesced.
        fake_db.update_project_loop.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_renaming_an_archived_project_is_refused(
        self, user_b, archived_project, fake_db, fake_request
    ):
        fake_db.update_project = AsyncMock(return_value=True)
        with self._patched(user_b, fake_db, archived_project):
            with pytest.raises(HTTPException) as exc:
                await update_project(
                    str(archived_project["id"]),
                    ProjectUpdate(name="renamed"),
                    fake_request,
                    dependencies=self._deps(user_b, fake_db, archived_project),
                )

        assert exc.value.status_code == 409
        fake_db.update_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_status_plus_another_field_applies_neither(
        self, user_b, archived_project, fake_db, fake_request
    ):
        """The worst outcome is a half-applied PATCH: the caller has no way
        to tell which half landed."""
        fake_db.update_project = AsyncMock(return_value=True)
        with self._patched(user_b, fake_db, archived_project):
            with pytest.raises(HTTPException) as exc:
                await update_project(
                    str(archived_project["id"]),
                    ProjectUpdate(status="active", name="renamed"),
                    fake_request,
                    dependencies=self._deps(user_b, fake_db, archived_project),
                )

        assert exc.value.status_code == 409
        fake_db.update_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_active_project_still_takes_any_field(
        self, user_b, project_b, fake_db, fake_request
    ):
        fake_db.update_project = AsyncMock(return_value=True)
        with self._patched(user_b, fake_db, project_b):
            result = await update_project(
                str(project_b["id"]),
                ProjectUpdate(name="renamed"),
                fake_request,
                dependencies=self._deps(user_b, fake_db, project_b),
            )

        assert result == {"status": "updated"}
        assert fake_db.update_project.await_args.kwargs == {"name": "renamed"}

    @pytest.mark.asyncio
    async def test_default_config_override_is_locked_while_archived(
        self, user_b, archived_project, fake_db, fake_request
    ):
        """This layer is merged under EVERY job in the project — leaving it
        editable would stop "archived" meaning read-only."""
        fake_db.update_project = AsyncMock(return_value=True)
        with self._patched(user_b, fake_db, archived_project):
            with pytest.raises(HTTPException) as exc:
                await update_project(
                    str(archived_project["id"]),
                    ProjectUpdate(default_config_override={"memory": {"x": True}}),
                    fake_request,
                    dependencies=self._deps(user_b, fake_db, archived_project),
                )

        assert exc.value.status_code == 409
        fake_db.update_project.assert_not_awaited()


class TestArchivingQuiescesChildren:
    """§4.5 — quiesce, never refuse; then report what happened."""

    def _patched(self, user, db, project):
        """Caller patch only — the owner gate is stubbed through ``_deps``,
        which is where the extracted route now reads it from."""
        return _patch_caller(user)

    def _deps(self, user, db, project):
        return _proj_deps(
            user,
            db,
            require_project_owner=AsyncMock(return_value=(user, project)),
        )

    @pytest.mark.asyncio
    async def test_archiving_pauses_the_loop_holds_the_officer_and_parks_jobs(
        self, user_b, project_b, fake_db, fake_request
    ):
        loop_id = "1111aaaa-1111-4111-8111-111111111111"
        officer_id = "2222bbbb-2222-4222-8222-222222222222"
        fake_db.update_project = AsyncMock(return_value=True)
        fake_db.get_active_project_loop = AsyncMock(
            return_value={"id": loop_id, "status": "running"}
        )
        fake_db.get_officer_thread_for_project = AsyncMock(
            return_value={"id": officer_id, "metadata": {}}
        )
        fake_db.park_project_jobs_for_archive = AsyncMock(return_value=3)

        with self._patched(user_b, fake_db, project_b):
            result = await update_project(
                str(project_b["id"]),
                ProjectUpdate(status="archived"),
                fake_request,
                dependencies=self._deps(user_b, fake_db, project_b),
            )

        assert result == {
            "status": "updated",
            "archived": True,
            "loop_paused": True,
            "officer_held": True,
            "jobs_parked": 3,
        }
        fake_db.update_project_loop.assert_awaited_once_with(loop_id, status="paused")
        hold = fake_db.set_project_officer_hold.await_args.kwargs["hold"]
        assert hold["kind"] == "project_archived"
        # The hold carries NO thread_id — that absence is what stops the
        # watchdog's stale-hold self-heal from releasing it.
        assert "thread_id" not in hold

    @pytest.mark.asyncio
    async def test_a_paused_loop_and_an_already_held_officer_are_left_alone(
        self, user_b, project_b, fake_db, fake_request
    ):
        fake_db.update_project = AsyncMock(return_value=True)
        fake_db.get_active_project_loop = AsyncMock(
            return_value={"id": "l1", "status": "paused"}
        )
        fake_db.get_officer_thread_for_project = AsyncMock(
            return_value={
                "id": "o1",
                "metadata": {"config_override": {"officer": {"hold": {"kind": "m"}}}},
            }
        )
        fake_db.park_project_jobs_for_archive = AsyncMock(return_value=0)

        with self._patched(user_b, fake_db, project_b):
            result = await update_project(
                str(project_b["id"]),
                ProjectUpdate(status="archived"),
                fake_request,
                dependencies=self._deps(user_b, fake_db, project_b),
            )

        assert result["loop_paused"] is False
        assert result["officer_held"] is False
        fake_db.update_project_loop.assert_not_awaited()
        fake_db.set_project_officer_hold.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failing_child_never_refuses_the_archive(
        self, user_b, project_b, fake_db, fake_request
    ):
        """Refusing makes archive un-completable exactly when you most want
        it — when something is wedged and you want it to stop mattering."""
        fake_db.update_project = AsyncMock(return_value=True)
        fake_db.get_active_project_loop = AsyncMock(side_effect=RuntimeError("boom"))
        fake_db.get_officer_thread_for_project = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        fake_db.park_project_jobs_for_archive = AsyncMock(
            side_effect=RuntimeError("boom")
        )

        with self._patched(user_b, fake_db, project_b):
            result = await update_project(
                str(project_b["id"]),
                ProjectUpdate(status="archived"),
                fake_request,
                dependencies=self._deps(user_b, fake_db, project_b),
            )

        assert result["archived"] is True
        assert result["jobs_parked"] == 0

    @pytest.mark.asyncio
    async def test_re_archiving_does_not_re_hold_a_released_officer(
        self, user_b, archived_project, fake_db, fake_request
    ):
        """Only the transition INTO archived quiesces. A PATCH re-asserting
        the current status must not undo a hold the owner released."""
        fake_db.update_project = AsyncMock(return_value=True)

        with self._patched(user_b, fake_db, archived_project):
            result = await update_project(
                str(archived_project["id"]),
                ProjectUpdate(status="archived"),
                fake_request,
                dependencies=self._deps(user_b, fake_db, archived_project),
            )

        assert result == {"status": "updated"}
        fake_db.set_project_officer_hold.assert_not_awaited()


class TestCreateProjectJobOnAnArchivedProject:
    @pytest.mark.asyncio
    async def test_editor_is_refused(
        self, user_b, archived_project, fake_db, fake_request
    ):
        from orchestrator.schemas.job_create import JobCreate
        from orchestrator.routers.project_jobs import create_project_job

        _set_role(fake_db, archived_project["id"], user_b["id"], "editor")
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch(
                "orchestrator.routers.job_lifecycle.admit_job_request",
                AsyncMock(
                    side_effect=AssertionError("create_job called past the gate")
                ),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_project_job(
                    _with_job_lifecycle_factory(fake_request),
                    str(archived_project["id"]),
                    JobCreate(description="x"),
                )
        assert exc.value.status_code == 409


class TestArchivedProjectStaysReadableAndTearableDown:
    """§4.3's other half — an archive you cannot open or empty is a trap.

    Every ALLOW-listed endpoint rides the guards' ``allow_archived=True``
    default; these pin that the default is what they actually get.
    """

    @pytest.mark.asyncio
    async def test_listing_its_jobs_still_works(
        self, user_b, archived_project, fake_db, fake_request
    ):
        import orchestrator.main

        fake_db.acquire = MagicMock(return_value=_patch_admin_list_fetch([]))
        fake_db.get_project_members = AsyncMock(return_value=[])
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch("orchestrator.main.app.state.resources.audit_reader") as audit,
        ):
            audit.is_available = False
            result = await job_reads_module.list_project_jobs(
                fake_request,
                str(archived_project["id"]),
                dependencies=jobs_composition.job_reads_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
        assert result == []

    @pytest.mark.asyncio
    async def test_reading_its_members_still_works(
        self, user_b, archived_project, fake_db, fake_request
    ):
        fake_db.get_project_members = AsyncMock(return_value=[{"user_id": "u"}])
        with _patch_caller(user_b):
            result = await list_project_members(
                fake_request,
                str(archived_project["id"]),
                dependencies=_proj_deps(user_b, fake_db),
            )
        assert result == [{"user_id": "u"}]

    @pytest.mark.asyncio
    async def test_detaching_a_repository_still_works(
        self, user_b, archived_project, fake_db, fake_request
    ):
        fake_db.get_project_repository = AsyncMock(
            return_value={
                "id": "r",
                "project_id": archived_project["id"],
                "role": "doc",
                "is_managed": False,
                "name": "r",
            }
        )
        fake_db.remove_project_repository = AsyncMock(
            return_value={"is_managed": False, "name": "r"}
        )
        gitea = MagicMock()
        gitea.is_initialized = False
        with _patch_caller(user_b):
            result = await remove_project_repository(
                fake_request,
                str(archived_project["id"]),
                "repo-id",
                dependencies=_proj_deps(user_b, fake_db, forge=gitea),
            )
        assert result == {"status": "removed"}

    @pytest.mark.asyncio
    async def test_attaching_a_repository_is_refused(
        self, user_b, archived_project, fake_db, fake_request
    ):
        """The mirror image, so the pair is unambiguous: detach yes, attach no."""
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await add_project_repository(
                    fake_request,
                    str(archived_project["id"]),
                    ProjectRepositoryCreate(name="new-repo"),
                    dependencies=_proj_deps(user_b, fake_db),
                )
        assert exc.value.status_code == 409
