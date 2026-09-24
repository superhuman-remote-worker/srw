"""The project's own knowledge base is indexed exactly once.

New projects get one managed repo (``project-<id8>-knowledge``) and
auto-attach it as a ``kb`` datasource. That datasource is a *management
surface* — visible, listable, unlinkable — over notes that are already indexed
under the project's own id.

If it were mistaken for an ordinary external KB, the sweep would index the same
vault a second time under the datasource UUID and every note would appear twice
in search: acceptance criterion 5, and the one failure in that design that
corrupts search rather than merely failing it. The marker that prevents it is
``config.native_project_id``; these tests pin every place that reads it.
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from orchestrator.routers.datasources import (
    DatasourcesDependencies,
    reindex_datasource_knowledge,
    update_datasource,
)
from orchestrator.routers.projects import (
    ProjectsDependencies,
    attach_project_knowledge_repository,
    create_project,
)
from orchestrator.schemas.datasources import DatasourceUpdate
from orchestrator.schemas.projects import ExternalKnowledgeBase, ProjectCreate
from orchestrator.services.datasource_config import normalize_kb_config
from orchestrator.services.datasources import DatasourceDependencies
from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
from orchestrator.services.knowledge_index import KnowledgeIndexDependencies
from orchestrator.services.project_provisioning import (
    ProjectProvisioningDependencies,
    ProjectRepairState,
    plan_external_kb_vault,
    provision_external_project_knowledge_repo,
    provision_project_knowledge_repo,
)
from orchestrator.services.projects import ProjectDependencies
from orchestrator.services.kb_datasources import (
    NATIVE_PROJECT_CONFIG_KEY,
    native_kb_project_id,
    reindex_kb_datasource,
)
from orchestrator.services.kb_reindex import kb_sweep_tick
from agent.core.datasource_setup import inject_workspace_facts
from agent.core.workspace import WorkspaceManager, WorkspaceManagerConfig
from agent.managers.git_manager import GitManager
from agent.services.knowledge.bindings import (
    NATIVE_PROJECT_CONFIG_KEY as AGENT_NATIVE_KEY,
    build_knowledge_bindings,
)
from tests._fs_backend import FilesystemTestBackend
from orchestrator.services import datasource_config as datasource_config_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import session_tool_policy as session_tool_policy_module


def _index_deps(store) -> KnowledgeIndexDependencies:
    """The KB-index collaborators main's ``_knowledge_index_dependencies``
    supplies; every KB path here is either stubbed or purely marker logic."""
    return KnowledgeIndexDependencies(
        store=store,
        vector_db=MagicMock(),
        gitea_client=MagicMock(),
        logger=MagicMock(),
        tasks=KbDatasourceTaskRegistry(),
        inject_system_kb_embedding_profile=AsyncMock(return_value=None),
    )


def _provisioning_deps(store, gitea=None) -> ProjectProvisioningDependencies:
    """Provisioning ports, composed the way main's factory composes them."""
    return ProjectProvisioningDependencies(
        store=store,
        forge=gitea if gitea is not None else MagicMock(is_initialized=False),
        keycloak_groups=MagicMock(),
        main_cloud_router=MagicMock(),
        logger=logging.getLogger("test.kb_native"),
        repair=ProjectRepairState(),
        knowledge_index=_index_deps(store),
    )


def _projects_deps(store, gitea=None, **gates) -> ProjectsDependencies:
    """Project router dependencies, composed the way main's factory does.

    ``with_validated_tool_overrides`` is main's real write-boundary gate, and
    every gate not named in ``gates`` keeps the real
    ``orchestrator.security.access`` implementation the dataclass defaults to —
    which is what an unpatched ``main`` global used to resolve to.
    """

    forge = gitea if gitea is not None else MagicMock(is_initialized=False)
    gates.setdefault("require_admin", AsyncMock())
    return ProjectsDependencies(
        store=store,
        operations=ProjectDependencies(
            store=store,
            vector_db=MagicMock(),
            forge=forge,
            keycloak_groups=MagicMock(),
            main_cloud_router=MagicMock(),
            logger=logging.getLogger("test.kb_native"),
            provisioning=_provisioning_deps(store, forge),
            with_validated_tool_overrides=session_tool_policy_module.with_validated_tool_overrides,
        ),
        **gates,
    )


def _attach_deps(store, gitea) -> ProjectsDependencies:
    """``_projects_deps`` with the project-owner gate these attach tests stub."""
    return _projects_deps(
        store,
        gitea,
        require_project_owner=AsyncMock(
            return_value=(
                {"id": OWNER_ID, "is_admin": False},
                {"id": PROJECT_ID, "name": "Better Resavio"},
            )
        ),
    )


def _ds_deps(store=None, **gates) -> DatasourcesDependencies:
    """Connector router dependencies, composed the way main's factory does."""

    db = MagicMock() if store is None else store
    return DatasourcesDependencies(
        store=db,
        operations=DatasourceDependencies(
            store=db,
            vector_db=MagicMock(),
            knowledge_index=_index_deps(db),
            mcp_datasources_enabled=deployment_gates_module.mcp_datasources_enabled,
            validate_mcp_datasource=datasource_config_module.validate_mcp_datasource,
        ),
        **gates,
    )


PROJECT_ID = "1a387b4d-1111-2222-3333-444444444444"
ID8 = PROJECT_ID[:8]
OWNER_ID = "9c9c9c9c-0000-0000-0000-000000000001"
DATASOURCE_ID = uuid.UUID("55555555-6666-7777-8888-999999999999")


def native_kb_row(project_id: str = PROJECT_ID, **overrides) -> dict:
    """A project's own KB datasource, as ``create_project`` writes it."""
    row = {
        "id": DATASOURCE_ID,
        "type": "kb",
        "name": f"Better Resavio Knowledge ({project_id[:8]})",
        "connection_url": None,
        "credentials": {},
        "default_branch": None,
        "config": {
            "root_path": "knowledge",
            NATIVE_PROJECT_CONFIG_KEY: project_id,
        },
    }
    row.update(overrides)
    return row


def external_kb_row(**overrides) -> dict:
    """A genuinely external OKF KB — the kind that must still be swept."""
    row = {
        "id": uuid.uuid4(),
        "type": "kb",
        "name": "Team Docs",
        "connection_url": "https://example.test/team-docs.git",
        "credentials": {"token": "orchestrator-only"},
        "default_branch": "main",
        "config": {"root_path": "vault"},
    }
    row.update(overrides)
    return row


# =============================================================================
# Project creation — criterion 2
# =============================================================================


class TestProjectCreationProvisioning:
    def _db(self) -> MagicMock:
        db = MagicMock()
        db.create_project = AsyncMock(
            return_value={"id": PROJECT_ID, "name": "Better Resavio"}
        )
        db.add_project_member = AsyncMock()
        db.add_project_repository = AsyncMock(
            side_effect=lambda **kwargs: {"id": uuid.uuid4(), **kwargs}
        )
        db.get_native_project_kb_datasource_ref = AsyncMock(return_value=None)
        db.get_user = AsyncMock(return_value={"email": "owner@example.test"})
        db.create_datasource = AsyncMock(
            return_value={"id": DATASOURCE_ID, "name": "Better Resavio Knowledge"}
        )
        db.link_datasource_to_project = AsyncMock(return_value=True)
        creation_intent_id = uuid.uuid4()
        creation_marker = uuid.uuid4()
        db.reserve_managed_repository_creation_intent = AsyncMock(
            return_value={
                "id": creation_intent_id,
                "intent_marker": creation_marker,
                "status": "pending",
            }
        )
        db.mark_managed_repository_created = AsyncMock(
            return_value={
                "id": creation_intent_id,
                "intent_marker": creation_marker,
                "status": "created",
            }
        )
        db.fail_managed_repository_creation_intent = AsyncMock(return_value=True)
        db.conflict_managed_repository_creation_intent = AsyncMock(return_value=True)

        async def _reserve_authority(**kwargs):
            return {
                "id": uuid.uuid4(),
                "repository_owner": kwargs["repository_owner"],
                "repo_name": kwargs["repo_name"],
                "authority_kind": kwargs["authority_kind"],
                "authority_id": uuid.UUID(kwargs["authority_id"]),
                "project_id": uuid.UUID(kwargs["project_id"]),
                "generation": 1,
                "clean_repo_url": kwargs["clean_repo_url"],
                "private_key": kwargs["private_key"],
                "status": "active",
            }

        db.reserve_managed_repository_authority = AsyncMock(
            side_effect=_reserve_authority
        )
        db.managed_repository_scope_is_unambiguous = AsyncMock(return_value=True)
        return db

    def _gitea(self, **kwargs) -> MagicMock:
        gitea = MagicMock()
        gitea.is_initialized = True
        gitea.repository_owner = "srw"
        gitea.create_repo = AsyncMock(
            side_effect=lambda name, **_kwargs: f"http://gitea/srw/{name}.git",
            **kwargs,
        )
        gitea.repository_creation_intent_status = AsyncMock(return_value="missing")
        gitea.clean_repo_url = MagicMock(
            side_effect=lambda name: f"http://gitea/srw/{name}.git"
        )
        gitea.grant_user_repo_access = AsyncMock()
        gitea.delete_repo = AsyncMock()
        return gitea

    async def _create(self, db, gitea, *, external_kb=None):
        deps = _projects_deps(
            db,
            gitea,
            require_approved_user=AsyncMock(
                return_value={"id": OWNER_ID, "is_admin": False}
            ),
        )
        # Kept so a caller can assert on the exact dependency value the
        # provisioning layer was handed.
        self.deps = deps
        with patch(
            "orchestrator.services.project_provisioning.ensure_project_cloud_resources",
            AsyncMock(side_effect=lambda project, **_kwargs: project),
        ):
            return await create_project(
                ProjectCreate(
                    name="Better Resavio",
                    user_id=OWNER_ID,
                    external_kb=external_kb,
                ),
                object(),
                dependencies=deps,
            )

    @pytest.mark.asyncio
    async def test_creates_only_the_managed_knowledge_repo(self):
        db, gitea = self._db(), self._gitea()

        await self._create(db, gitea)

        by_role = {
            call.kwargs["role"]: call.kwargs
            for call in db.add_project_repository.await_args_list
        }
        assert set(by_role) == {"knowledge"}
        assert db.add_project_repository.await_count == 1
        assert by_role["knowledge"]["name"] == f"project-{ID8}-knowledge"
        assert all(kwargs["is_managed"] for kwargs in by_role.values())
        assert [c.args[0] for c in gitea.create_repo.await_args_list] == [
            f"project-{ID8}-knowledge",
        ]

    @pytest.mark.asyncio
    async def test_attaches_exactly_one_kb_datasource_marked_native(self):
        db, gitea = self._db(), self._gitea()

        await self._create(db, gitea)

        db.create_datasource.assert_awaited_once()
        kwargs = db.create_datasource.await_args.kwargs
        assert kwargs["ds_type"] == "kb"
        assert kwargs["config"] == {
            "root_path": "knowledge",
            NATIVE_PROJECT_CONFIG_KEY: PROJECT_ID,
        }
        # The vault's location lives in project_repositories and nowhere else —
        # a second copy here is how the reader and the writer drift apart, and
        # the authenticated Gitea URL would leak credentials through the API.
        assert kwargs["connection_url"] is None
        assert kwargs["created_by"] == OWNER_ID
        assert kwargs["scope_mode"] == "projects"
        assert kwargs["project_ids"] == [PROJECT_ID]
        assert kwargs["auto_attach"] is True
        db.link_datasource_to_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_datasource_name_is_unique_per_project(self):
        """(name, type, owner) is unique, so two projects sharing a title must
        not collide on their KB connectors."""
        db, gitea = self._db(), self._gitea()

        await self._create(db, gitea)

        assert ID8 in db.create_datasource.await_args.kwargs["name"]

    @pytest.mark.asyncio
    async def test_creator_gets_access_to_knowledge_repo(self):
        db, gitea = self._db(), self._gitea()

        await self._create(db, gitea)

        granted = {c.args[1] for c in gitea.grant_user_repo_access.await_args_list}
        assert granted == {f"project-{ID8}-knowledge"}

    @pytest.mark.asyncio
    async def test_gitea_hiccup_does_not_fail_project_creation(self):
        """The knowledge repo remains optional when Gitea is unavailable."""
        db = self._db()
        gitea = self._gitea()
        gitea.create_repo = AsyncMock(side_effect=RuntimeError("502"))

        project = await self._create(db, gitea)

        assert project["id"] == PROJECT_ID
        roles = [c.kwargs["role"] for c in db.add_project_repository.await_args_list]
        assert roles == []
        db.create_datasource.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_refused_knowledge_repo_attaches_no_datasource(self):
        """No repo means no vault to manage — a connector pointing at nothing
        would be worse than its absence."""
        db = self._db()
        gitea = self._gitea()
        gitea.create_repo = AsyncMock(return_value=None)

        project = await self._create(db, gitea)

        assert project["id"] == PROJECT_ID
        db.create_datasource.assert_not_awaited()
        db.link_datasource_to_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_provisioning_is_skipped_without_gitea(self):
        db = self._db()
        gitea = self._gitea()
        gitea.is_initialized = False

        await self._create(db, gitea)

        db.add_project_repository.assert_not_awaited()
        db.create_datasource.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_helper_returns_the_created_row(self):
        db, gitea = self._db(), self._gitea()
        created = await provision_project_knowledge_repo(
            {"id": PROJECT_ID, "name": "Better Resavio"},
            OWNER_ID,
            dependencies=_provisioning_deps(db, gitea),
        )
        assert created["id"] == DATASOURCE_ID


# =============================================================================
# External GitHub live vault — knowledge-base/knowledge/features/external_forge_knowledge_base.md
# =============================================================================


class TestExternalProjectKnowledgeProvisioning(TestProjectCreationProvisioning):
    EXTERNAL = ExternalKnowledgeBase(
        repo_url="https://github.com/acme/design-vault.git",
        branch="vault/main",
        token="github-pat-never-return",
    )

    @pytest.mark.asyncio
    async def test_create_uses_external_repo_without_creating_gitea_repo(self):
        db, gitea = self._db(), self._gitea()

        project = await self._create(db, gitea, external_kb=self.EXTERNAL)

        assert project["id"] == PROJECT_ID
        gitea.create_repo.assert_not_awaited()
        db.add_project_repository.assert_awaited_once()
        repo_kwargs = db.add_project_repository.await_args.kwargs
        assert repo_kwargs["name"] == "design-vault"
        assert repo_kwargs["repo_url"] == self.EXTERNAL.repo_url
        assert repo_kwargs["role"] == "knowledge"
        assert repo_kwargs["branch"] == "vault/main"
        assert repo_kwargs["is_managed"] is False
        assert "credentials" not in repo_kwargs

    @pytest.mark.asyncio
    async def test_pat_is_written_only_to_native_datasource_credentials(self):
        db, gitea = self._db(), self._gitea()

        project = await self._create(db, gitea, external_kb=self.EXTERNAL)

        kwargs = db.create_datasource.await_args.kwargs
        assert kwargs["connection_url"] is None
        assert kwargs["credentials"] == {
            "auth_method": "token",
            "token": "github-pat-never-return",
        }
        assert kwargs["config"] == {
            "root_path": "knowledge",
            NATIVE_PROJECT_CONFIG_KEY: PROJECT_ID,
        }
        assert "github-pat-never-return" not in repr(project)
        assert "github-pat-never-return" not in repr(
            db.add_project_repository.await_args.kwargs
        )

    @pytest.mark.asyncio
    async def test_github_enterprise_persists_explicit_forge_override(
        self, monkeypatch
    ):
        db, gitea = self._db(), self._gitea()
        monkeypatch.setenv("KB_GIT_ALLOWED_HOSTS", "github.corp.example")
        external = ExternalKnowledgeBase(
            repo_url="https://github.corp.example/acme/design-vault.git",
            branch="main",
            forge="github",
            token="enterprise-pat",
        )

        await self._create(db, gitea, external_kb=external)

        assert db.create_datasource.await_args.kwargs["config"]["forge"] == "github"

    @pytest.mark.asyncio
    async def test_external_helper_compensates_repo_row_if_datasource_write_fails(self):
        db = self._db()
        db.create_datasource.side_effect = RuntimeError("db failure")
        db.remove_project_repository = AsyncMock()

        provisioning = _provisioning_deps(db)
        with pytest.raises(RuntimeError):
            await provision_external_project_knowledge_repo(
                {"id": PROJECT_ID, "name": "Better Resavio"},
                OWNER_ID,
                await plan_external_kb_vault(self.EXTERNAL, dependencies=provisioning),
                dependencies=provisioning,
            )

        db.remove_project_repository.assert_awaited_once()
        assert db.remove_project_repository.await_args.args[0]


class TestAttachExternalProjectKnowledgeRepo(TestProjectCreationProvisioning):
    EXTERNAL = ExternalKnowledgeBase(
        repo_url="https://github.com/acme/design-vault.git",
        branch="main",
        token="attach-pat-never-return",
    )

    @pytest.mark.asyncio
    async def test_repo_less_existing_project_can_attach_external_vault(self):
        db, gitea = self._db(), self._gitea()
        db.get_project_repositories = AsyncMock(return_value=[])
        request = object()

        result = await attach_project_knowledge_repository(
            request, PROJECT_ID, self.EXTERNAL, dependencies=_attach_deps(db, gitea)
        )

        assert result["status"] == "attached"
        assert "credentials" not in result["repository"]
        assert "credentials" not in result["datasource"]
        assert "attach-pat-never-return" not in repr(result)
        gitea.create_repo.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_existing_knowledge_repo_is_not_silently_replaced(self):
        db, gitea = self._db(), self._gitea()
        db.get_project_repositories = AsyncMock(
            return_value=[{"id": uuid.uuid4(), "role": "knowledge"}]
        )

        with pytest.raises(HTTPException) as exc:
            await attach_project_knowledge_repository(
                object(),
                PROJECT_ID,
                self.EXTERNAL,
                dependencies=_attach_deps(db, gitea),
            )

        assert exc.value.status_code == 409
        db.add_project_repository.assert_not_awaited()
        db.create_datasource.assert_not_awaited()
        gitea.delete_repo.assert_not_awaited()


# =============================================================================
# Adopting an existing connector as the vault —
# knowledge-base/knowledge/features/external_forge_knowledge_base.md §4.4
# =============================================================================


CONNECTOR_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
OTHER_PROJECT_ID = "7d7d7d7d-1111-2222-3333-444444444444"


def connector_row(**overrides) -> dict:
    """A user-created external ``kb`` connector, eligible for adoption."""
    row = {
        "id": CONNECTOR_ID,
        "type": "kb",
        "name": "Design Vault",
        "connection_url": "https://github.com/acme/design-vault.git",
        "credentials": {
            "auth_method": "token",
            "token": "connector-pat-never-return",
        },
        "default_branch": "vault/main",
        "config": {"root_path": "knowledge"},
        "created_by": OWNER_ID,
        "scope_mode": "all",
        "auto_attach": False,
        "policy_revision": 3,
    }
    row.update(overrides)
    return row


class TestAdoptConnectorAsProjectVault(TestProjectCreationProvisioning):
    """A project may point its writable vault at a connector created earlier.

    The connector is converted *in place* rather than copied: a second row for
    the same repository would be swept as an ordinary external source and index
    every note a second time under its own UUID.
    """

    ADOPT = ExternalKnowledgeBase(datasource_id=str(CONNECTOR_ID))

    def _db(self, connector: dict | None = None) -> MagicMock:
        db = super()._db()
        db.get_datasource = AsyncMock(
            return_value=connector if connector is not None else connector_row()
        )
        db.list_datasource_projects = AsyncMock(return_value=[])
        db.update_datasource_with_policy = AsyncMock(
            side_effect=lambda datasource_id, **kwargs: {
                "id": CONNECTOR_ID,
                "name": "Design Vault",
                "type": "kb",
                "config": kwargs.get("config"),
            }
        )
        return db

    async def _create_adopting(self, db, gitea, external_kb=None):
        with patch(
            "orchestrator.services.project_provisioning.purge_kb_datasource_index",
            AsyncMock(),
        ):
            return await self._create(db, gitea, external_kb=external_kb or self.ADOPT)

    @pytest.mark.asyncio
    async def test_no_second_connector_row_is_created_for_the_same_repo(self):
        db, gitea = self._db(), self._gitea()

        await self._create_adopting(db, gitea)

        db.create_datasource.assert_not_awaited()
        gitea.create_repo.assert_not_awaited()
        db.update_datasource_with_policy.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_adopted_connector_is_marked_native_to_the_new_project(self):
        db, gitea = self._db(), self._gitea()

        await self._create_adopting(db, gitea)

        kwargs = db.update_datasource_with_policy.await_args.kwargs
        assert kwargs["config"][NATIVE_PROJECT_CONFIG_KEY] == PROJECT_ID
        assert kwargs["config"]["root_path"] == "knowledge"
        assert kwargs["expected_policy_revision"] == 3

    @pytest.mark.asyncio
    async def test_adopted_connector_is_scoped_to_the_project_it_serves(self):
        """Left available to other projects it would be bound as a read-only
        source whose index no longer exists — a vault that silently reads
        empty."""
        db, gitea = self._db(), self._gitea()

        await self._create_adopting(db, gitea)

        kwargs = db.update_datasource_with_policy.await_args.kwargs
        assert kwargs["scope_mode"] == "projects"
        assert kwargs["auto_attach"] is True
        assert kwargs["project_ids"] == [PROJECT_ID]

    @pytest.mark.asyncio
    async def test_vault_repository_row_takes_the_connectors_url_and_branch(self):
        db, gitea = self._db(), self._gitea()

        await self._create_adopting(db, gitea)

        kwargs = db.add_project_repository.await_args.kwargs
        assert kwargs["repo_url"] == "https://github.com/acme/design-vault.git"
        assert kwargs["branch"] == "vault/main"
        assert kwargs["role"] == "knowledge"
        assert kwargs["is_managed"] is False

    @pytest.mark.asyncio
    async def test_stale_external_index_is_purged_after_the_marker_lands(self):
        """Its notes are about to be indexed under the project id. Whatever it
        accumulated under its own UUID would double every search hit."""
        db, gitea = self._db(), self._gitea()

        with patch(
            "orchestrator.services.project_provisioning.purge_kb_datasource_index",
            AsyncMock(),
        ) as purge:
            await self._create(db, gitea, external_kb=self.ADOPT)

        purge.assert_awaited_once_with(
            str(CONNECTOR_ID),
            dependencies=self.deps.operations.provisioning.knowledge_index,
        )
        assert db.update_datasource_with_policy.await_count == 1

    @pytest.mark.asyncio
    async def test_purge_failure_does_not_fail_project_creation(self):
        """The marker is already stored, so the row is out of the sweep; a
        leftover index is disposable and the next sweep is not blocked."""
        db, gitea = self._db(), self._gitea()

        with patch(
            "orchestrator.services.project_provisioning.purge_kb_datasource_index",
            AsyncMock(side_effect=RuntimeError("vector db down")),
        ):
            project = await self._create(db, gitea, external_kb=self.ADOPT)

        assert project["id"] == PROJECT_ID

    @pytest.mark.asyncio
    async def test_connector_of_another_type_is_refused(self):
        db, gitea = self._db(connector_row(type="repository")), self._gitea()

        with pytest.raises(HTTPException) as exc:
            await self._create_adopting(db, gitea)

        assert exc.value.status_code == 400
        db.create_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_connector_is_refused(self):
        db, gitea = self._db(), self._gitea()
        db.get_datasource = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc:
            await self._create_adopting(db, gitea)

        assert exc.value.status_code == 404
        db.create_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_connector_already_owned_by_a_project_is_refused(self):
        db, gitea = (
            self._db(
                connector_row(
                    config={
                        "root_path": "knowledge",
                        NATIVE_PROJECT_CONFIG_KEY: OTHER_PROJECT_ID,
                    }
                )
            ),
            self._gitea(),
        )

        with pytest.raises(HTTPException) as exc:
            await self._create_adopting(db, gitea)

        assert exc.value.status_code == 409
        db.create_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_connector_linked_to_another_project_is_refused(self):
        """Adoption narrows the connector to one project. Doing that silently
        would revoke another project's reader access without telling anyone."""
        db, gitea = self._db(), self._gitea()
        db.list_datasource_projects = AsyncMock(return_value=[OTHER_PROJECT_ID])

        with pytest.raises(HTTPException) as exc:
            await self._create_adopting(db, gitea)

        assert exc.value.status_code == 409
        assert "unlink" in str(exc.value.detail).lower()

    @pytest.mark.asyncio
    async def test_published_connector_is_refused(self):
        """Same reasoning as the shared-project check: adoption takes the row
        private and drops the index everyone else was reading."""
        db, gitea = self._db(connector_row(is_global=True)), self._gitea()

        with pytest.raises(HTTPException) as exc:
            await self._create_adopting(db, gitea)

        assert exc.value.status_code == 409
        db.create_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ssh_only_connector_is_refused(self):
        """The vault is written through the GitHub contents API, which has no
        SSH equivalent: it would clone fine and fail on every note write."""
        db, gitea = (
            self._db(
                connector_row(
                    credentials={"auth_method": "ssh", "ssh_key": "PRIVATE KEY"}
                )
            ),
            self._gitea(),
        )

        with pytest.raises(HTTPException) as exc:
            await self._create_adopting(db, gitea)

        assert exc.value.status_code == 400
        assert "token" in str(exc.value.detail).lower()

    @pytest.mark.asyncio
    async def test_non_github_connector_is_refused(self):
        db, gitea = (
            self._db(connector_row(connection_url="https://gitlab.com/acme/vault.git")),
            self._gitea(),
        )

        with pytest.raises(HTTPException) as exc:
            await self._create_adopting(db, gitea)

        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_connector_with_a_custom_note_root_is_refused(self):
        """The write path commits to ``knowledge/<slug>.md`` unconditionally,
        so any other root reads a different folder than it writes."""
        db, gitea = (
            self._db(connector_row(config={"root_path": "docs/vault"})),
            self._gitea(),
        )

        with pytest.raises(HTTPException) as exc:
            await self._create_adopting(db, gitea)

        assert exc.value.status_code == 400
        assert "knowledge" in str(exc.value.detail)

    @pytest.mark.asyncio
    async def test_non_owner_cannot_adopt_someone_elses_connector(self):
        db, gitea = (
            self._db(connector_row(created_by=str(uuid.uuid4()))),
            self._gitea(),
        )

        with pytest.raises(HTTPException) as exc:
            await self._create_adopting(db, gitea)

        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_repo_row_is_rolled_back_when_the_marker_write_fails(self):
        db = self._db()
        db.update_datasource_with_policy.side_effect = RuntimeError("db failure")
        db.remove_project_repository = AsyncMock()

        provisioning = _provisioning_deps(db)
        with (
            patch(
                "orchestrator.services.project_provisioning.purge_kb_datasource_index",
                AsyncMock(),
            ) as purge,
            pytest.raises(RuntimeError),
        ):
            await provision_external_project_knowledge_repo(
                {"id": PROJECT_ID, "name": "Better Resavio"},
                OWNER_ID,
                await plan_external_kb_vault(
                    self.ADOPT,
                    caller={"id": OWNER_ID, "is_admin": False},
                    dependencies=provisioning,
                ),
                dependencies=provisioning,
            )

        db.remove_project_repository.assert_awaited_once()
        purge.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_connector_credentials_never_reach_the_response(self):
        db, gitea = self._db(), self._gitea()
        db.get_project_repositories = AsyncMock(return_value=[])

        with patch(
            "orchestrator.services.project_provisioning.purge_kb_datasource_index",
            AsyncMock(),
        ):
            result = await attach_project_knowledge_repository(
                object(), PROJECT_ID, self.ADOPT, dependencies=_attach_deps(db, gitea)
            )

        assert result["status"] == "attached"
        assert "connector-pat-never-return" not in repr(result)

    @pytest.mark.asyncio
    async def test_project_that_already_has_a_kb_connector_is_refused(self):
        """Two rows marked native to one project make
        ``get_native_project_kb_datasource_ref`` pick the older one, so the
        vault would read credentials that belong to a different repository."""
        db, gitea = self._db(), self._gitea()
        db.get_project_repositories = AsyncMock(return_value=[])
        db.get_native_project_kb_datasource_ref = AsyncMock(
            return_value=native_kb_row()
        )

        with pytest.raises(HTTPException) as exc:
            await attach_project_knowledge_repository(
                object(), PROJECT_ID, self.ADOPT, dependencies=_attach_deps(db, gitea)
            )

        assert exc.value.status_code == 409
        db.add_project_repository.assert_not_awaited()
        db.update_datasource_with_policy.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_attach_allows_a_connector_already_linked_to_that_project(self):
        """Its only link is the project doing the adopting — nobody loses
        access."""
        db, gitea = self._db(), self._gitea()
        db.get_project_repositories = AsyncMock(return_value=[])
        db.list_datasource_projects = AsyncMock(return_value=[PROJECT_ID])

        with patch(
            "orchestrator.services.project_provisioning.purge_kb_datasource_index",
            AsyncMock(),
        ):
            result = await attach_project_knowledge_repository(
                object(), PROJECT_ID, self.ADOPT, dependencies=_attach_deps(db, gitea)
            )

        assert result["status"] == "attached"


class TestExternalKnowledgeBaseRequestShape:
    """One vault, named exactly one way."""

    def test_connector_and_inline_credentials_are_mutually_exclusive(self):
        with pytest.raises(ValidationError):
            ExternalKnowledgeBase(
                datasource_id=str(CONNECTOR_ID),
                repo_url="https://github.com/acme/design-vault.git",
                token="pat",
            )

    def test_a_branch_cannot_override_the_connectors_own(self):
        with pytest.raises(ValidationError):
            ExternalKnowledgeBase(
                datasource_id=str(CONNECTOR_ID), branch="somewhere-else"
            )

    def test_inline_mode_still_requires_a_repo_url_and_token(self):
        with pytest.raises(ValidationError):
            ExternalKnowledgeBase(repo_url="https://github.com/acme/vault.git")
        with pytest.raises(ValidationError):
            ExternalKnowledgeBase()

    def test_inline_mode_still_defaults_the_branch(self):
        body = ExternalKnowledgeBase(
            repo_url="https://github.com/acme/vault.git", token="pat"
        )
        assert body.branch == "main"


# =============================================================================
# The external sweep — criterion 5
# =============================================================================


class TestExternalSweepSkipsNativeKb:
    def _sweep(self, rows, reindex_fn):
        postgres_db = AsyncMock()
        postgres_db.fetch.return_value = []  # no native project repos this tick
        postgres_db.list_datasources.return_value = rows
        return kb_sweep_tick(
            postgres_db=postgres_db,
            store=MagicMock(),
            gitea_client=MagicMock(),
            embedding_service=MagicMock(),
            reindex_fn=reindex_fn,
        )

    @pytest.mark.asyncio
    async def test_project_own_kb_is_not_swept_as_an_external_source(self):
        """Patching the external indexer itself, not the reindex callable it
        wraps: the sweep must not *attempt* the row. Asserting only on the
        inner callable would also pass if the attempt happened and was caught
        by the ValueError backstop, which is a swallowed error every tick, not
        a skip."""
        indexer = AsyncMock(return_value={"status": "completed"})

        with patch(
            "orchestrator.services.kb_datasources.reindex_kb_datasource", indexer
        ):
            worked = await self._sweep([native_kb_row()], AsyncMock())

        assert worked == 0
        indexer.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_external_kbs_are_still_swept_alongside_a_native_one(
        self, monkeypatch
    ):
        """The skip must be surgical: a genuinely external KB in the same
        result set keeps its own index under its own datasource id."""
        monkeypatch.setenv("KB_GIT_ALLOWED_HOSTS", "example.test")
        external = external_kb_row()
        reindex_fn = AsyncMock(return_value={"status": "completed"})

        worked = await self._sweep([native_kb_row(), external], reindex_fn)

        assert worked == 1
        reindex_fn.assert_awaited_once()
        assert reindex_fn.await_args.kwargs["kb_id"] == uuid.UUID(str(external["id"]))

    @pytest.mark.asyncio
    async def test_liveness_check_fails_a_row_adopted_mid_sweep(self):
        """A sweep that read the row before it became a project's vault must
        not be allowed to commit its chunks afterwards — that is the double
        index, arrived at through a race."""
        captured = {}
        adopted = native_kb_row(id=external_kb_row()["id"])

        async def capture(datasource, **kwargs):
            captured["is_active"] = kwargs["is_active"]
            return {"status": "completed"}

        postgres_db = AsyncMock()
        postgres_db.fetch.return_value = []
        postgres_db.list_datasources.return_value = [external_kb_row(id=adopted["id"])]
        postgres_db.get_datasource.return_value = adopted

        with patch(
            "orchestrator.services.kb_datasources.reindex_kb_datasource",
            AsyncMock(side_effect=capture),
        ):
            await kb_sweep_tick(
                postgres_db=postgres_db,
                store=MagicMock(),
                gitea_client=MagicMock(),
                embedding_service=MagicMock(),
                reindex_fn=AsyncMock(),
            )

        assert await captured["is_active"]() is False

    @pytest.mark.asyncio
    async def test_reindexing_a_native_row_directly_is_refused(self):
        """The backstop: every external index path funnels through here, so a
        caller that forgets the skip fails loudly instead of silently
        duplicating a whole vault."""
        with pytest.raises(ValueError, match="project's own knowledge base"):
            await reindex_kb_datasource(
                native_kb_row(),
                store=MagicMock(),
                embedding_service=MagicMock(),
                is_active=AsyncMock(return_value=True),
                reindex_fn=AsyncMock(),
            )

    @pytest.mark.asyncio
    async def test_manual_reindex_endpoint_refuses_a_native_row(self):
        with (
            patch(
                "orchestrator.services.knowledge_index.reindex_kb_datasource_now",
                AsyncMock(),
            ) as now,
            pytest.raises(HTTPException) as exc,
        ):
            await reindex_datasource_knowledge(
                object(),
                str(DATASOURCE_ID),
                dependencies=_ds_deps(
                    require_datasource_owner=AsyncMock(
                        return_value=({}, native_kb_row())
                    )
                ),
            )

        assert exc.value.status_code == 400
        assert "own knowledge base" in str(exc.value.detail)
        now.assert_not_awaited()

    def test_marker_reader_ignores_rows_without_it(self):
        assert native_kb_project_id(external_kb_row()) is None
        assert native_kb_project_id({"type": "kb"}) is None
        assert native_kb_project_id({"type": "kb", "config": None}) is None
        assert native_kb_project_id(None) is None
        assert native_kb_project_id(native_kb_row()) == PROJECT_ID


# =============================================================================
# The marker survives round-trips — a stripped marker is a double index
# =============================================================================


class TestMarkerDurability:
    def test_stored_config_keeps_the_marker(self):
        stored = {"root_path": "knowledge", NATIVE_PROJECT_CONFIG_KEY: PROJECT_ID}
        assert normalize_kb_config(stored, stored=True) == stored

    def test_user_supplied_marker_is_rejected(self):
        """Nobody can hand-forge a row out of the sweep from the outside."""
        with pytest.raises(HTTPException) as exc:
            normalize_kb_config({NATIVE_PROJECT_CONFIG_KEY: PROJECT_ID})
        assert exc.value.status_code == 400
        assert NATIVE_PROJECT_CONFIG_KEY in str(exc.value.detail)

    def test_forge_override_survives_native_config_round_trip(self):
        stored = {
            "root_path": "knowledge",
            NATIVE_PROJECT_CONFIG_KEY: PROJECT_ID,
            "forge": "github",
        }
        assert normalize_kb_config(stored, stored=True) == stored

    @pytest.mark.asyncio
    async def test_editing_the_root_path_does_not_strip_the_marker(self):
        """Losing the marker on an ordinary edit would drop the vault back
        into the external sweep — the double index, arrived at by accident."""
        db = MagicMock()
        db.update_datasource = AsyncMock(return_value=True)
        db.list_datasource_projects = AsyncMock(return_value=[])
        db.get_datasource = AsyncMock(return_value=native_kb_row())
        schedule = MagicMock()

        with (
            patch(
                "orchestrator.services.knowledge_index.mark_kb_datasource_pending",
                AsyncMock(),
            ) as pending,
            patch(
                "orchestrator.services.knowledge_index.schedule_kb_datasource_reindex",
                schedule,
            ),
        ):
            await update_datasource(
                object(),
                str(DATASOURCE_ID),
                DatasourceUpdate(name="Renamed", config={"root_path": "notes"}),
                dependencies=_ds_deps(
                    db,
                    require_datasource_owner=AsyncMock(
                        return_value=({}, native_kb_row())
                    ),
                ),
            )

        written = db.update_datasource.await_args.kwargs["config"]
        assert written[NATIVE_PROJECT_CONFIG_KEY] == PROJECT_ID
        assert written["root_path"] == "notes"
        # ...and no external rebuild is ever scheduled for it
        schedule.assert_not_called()
        pending.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_native_row_update_does_not_demand_a_repository_url(self):
        """It has no remote to validate; requiring one would make renaming the
        project's own connector a 400."""
        db = MagicMock()
        db.update_datasource = AsyncMock(return_value=True)
        db.list_datasource_projects = AsyncMock(return_value=[])
        db.get_datasource = AsyncMock(return_value=native_kb_row())

        with patch(
            "orchestrator.services.knowledge_index.schedule_kb_datasource_reindex",
            MagicMock(),
        ):
            result = await update_datasource(
                object(),
                str(DATASOURCE_ID),
                DatasourceUpdate(name="Renamed"),
                dependencies=_ds_deps(
                    db,
                    require_datasource_owner=AsyncMock(
                        return_value=({}, native_kb_row())
                    ),
                ),
            )

        assert str(result["id"]) == str(DATASOURCE_ID)
        assert db.update_datasource.await_args.kwargs["connection_url"] is None


# =============================================================================
# Runtime bindings — one KB, one binding, still writable
# =============================================================================


class TestBindingsDoNotDuplicateTheProjectKb:
    def test_own_kb_datasource_collapses_into_the_writable_native_binding(self):
        bindings = build_knowledge_bindings(
            project_ids=[PROJECT_ID],
            datasources=[native_kb_row()],
        )

        assert len(bindings) == 1
        binding = bindings[0]
        assert binding.kb_id == uuid.UUID(PROJECT_ID)
        assert binding.kind == "native"
        assert binding.writable is True
        assert binding.alias == "project"

    def test_no_second_alias_for_the_same_kb_id(self):
        bindings = build_knowledge_bindings(
            project_ids=[PROJECT_ID],
            datasources=[native_kb_row(), external_kb_row()],
        )

        assert len(bindings) == 2
        assert len({b.kb_id for b in bindings}) == 2
        assert [b.kind for b in bindings] == ["native", "datasource"]
        assert uuid.UUID(PROJECT_ID) not in {
            b.kb_id for b in bindings if b.kind == "datasource"
        }

    def test_external_kbs_stay_bound_and_read_only(self):
        external = external_kb_row()

        bindings = build_knowledge_bindings(
            project_ids=[PROJECT_ID], datasources=[external]
        )

        docs = next(b for b in bindings if b.kind == "datasource")
        assert docs.kb_id == uuid.UUID(str(external["id"]))
        assert docs.writable is False
        assert docs.root_path == "vault"

    def test_native_row_without_its_project_binds_by_project_id(self):
        """Selected for a job in another project: still one KB, still keyed by
        the id its notes are actually indexed under, and read-only there."""
        bindings = build_knowledge_bindings(
            project_ids=[], datasources=[native_kb_row()]
        )

        assert len(bindings) == 1
        assert bindings[0].kb_id == uuid.UUID(PROJECT_ID)
        assert bindings[0].writable is False

    def test_existing_modules_export_the_shared_marker(self):
        """Existing callers retain their imports from both application modules."""
        from shared.native_kb import NATIVE_PROJECT_CONFIG_KEY as SHARED_NATIVE_KEY

        assert AGENT_NATIVE_KEY == NATIVE_PROJECT_CONFIG_KEY == SHARED_NATIVE_KEY

    def test_connector_index_does_not_advertise_the_project_kb_as_read_only(self):
        ws = MagicMock()
        ws.read_file.return_value = ""

        inject_workspace_facts([native_kb_row(), external_kb_row()], ws)

        written = ws.write_file.call_args.args[1]
        assert "Team Docs" in written
        assert "Better Resavio Knowledge" not in written


# =============================================================================
# The vault is never checked out — criterion 6
# =============================================================================


class TestKnowledgeRepoIsNotCloned:
    def _workspace(self, tmp_path: Path) -> WorkspaceManager:
        return WorkspaceManager(
            job_id="test-job",
            config=WorkspaceManagerConfig(
                structure=["archive/"],
                git_versioning=True,
                repositories=[
                    {
                        "role": "jobs",
                        "name": f"project-{ID8}-jobs",
                        "repo_url": "http://gitea/srw/jobs.git",
                    },
                    {
                        "role": "knowledge",
                        "name": f"project-{ID8}-knowledge",
                        "repo_url": "http://gitea/srw/knowledge.git",
                    },
                    {
                        "role": "source",
                        "name": "kurort-engine",
                        "repo_url": "http://gitea/srw/kurort.git",
                    },
                ],
            ),
            base_path=tmp_path,
            backend=FilesystemTestBackend(tmp_path),
        )

    def test_only_source_repos_land_in_repos(self, tmp_path, monkeypatch):
        cloned: list[str] = []

        def _record(repo_url, target, **kwargs):
            cloned.append(Path(target).name)
            return None  # clone "failed" — the call is what this test is about

        monkeypatch.setattr(GitManager, "clone", staticmethod(_record))

        self._workspace(tmp_path)._clone_auxiliary_repos()

        assert cloned == ["kurort-engine"]
        assert f"project-{ID8}-knowledge" not in cloned
        assert not (tmp_path / "repos" / f"project-{ID8}-knowledge").exists()
