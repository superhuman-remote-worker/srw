"""Project provisioning and drift repair (R1.B03 lane P).

Four behaviours, each of which is a correctness ruling rather than an
implementation detail:

* **the live-vault planner refuses before anything is created.** Every reason a
  repository or an existing connector may not become a project's writable vault
  is checked in one place, so a rejected request cannot leave a half-created
  project behind. The 409s in particular are data-integrity rulings, not
  politeness: adopting a shared or published connector silently revokes reader
  access and strands an index other projects are still listing.
* **adoption converts the row in place.** Copying it would leave two connectors
  on one repository — the copy indexed under the project id, the original still
  swept under its own UUID — and every note would answer a search twice.
* **the per-project heal lock is what stops a second Space.**
  ``ensure_project_folder`` is not idempotent: each call makes a new drive, so a
  concurrent heal without the lock silently duplicates a project's storage.
* **member sync degrades.** Keycloak and the cloud backend are optional tiers;
  neither being down may fail the caller that triggered the repair.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.schemas.projects import ExternalKnowledgeBase
from orchestrator.services import project_provisioning as provisioning


PROJECT_ID = str(uuid4())
DATASOURCE_ID = str(uuid4())
OWNER_ID = str(uuid4())
STRANGER_ID = str(uuid4())

OWNER = {"id": OWNER_ID, "is_admin": False}
STRANGER = {"id": STRANGER_ID, "is_admin": False}
ADMIN = {"id": STRANGER_ID, "is_admin": True}

REPO_URL = "https://github.com/acme/vault.git"


def _deps(**over: Any) -> provisioning.ProjectProvisioningDependencies:
    base: dict[str, Any] = dict(
        store=MagicMock(),
        forge=SimpleNamespace(is_initialized=False),
        keycloak_groups=SimpleNamespace(is_initialized=False),
        main_cloud_router=SimpleNamespace(
            for_project_optional=MagicMock(return_value=None),
            for_owner=MagicMock(),
        ),
        logger=MagicMock(),
        repair=provisioning.ProjectRepairState(),
        knowledge_index=MagicMock(),
    )
    base.update(over)
    return provisioning.ProjectProvisioningDependencies(**base)


def _connector(**over: Any) -> dict[str, Any]:
    row = {
        "id": DATASOURCE_ID,
        "type": "kb",
        "created_by": OWNER_ID,
        "connection_url": REPO_URL,
        "default_branch": "main",
        "credentials": {"token": "ghp_live"},
        "config": {"root_path": "knowledge"},
        "policy_revision": 7,
        "is_global": False,
    }
    row.update(over)
    return row


# =============================================================================
# The vault planner's refusals
# =============================================================================


class TestKbVaultPlan:
    def test_a_missing_url_is_refused(self):
        with pytest.raises(HTTPException) as raised:
            provisioning.kb_vault_plan(
                None, "main", {"auth_method": "token", "token": "t"}, None
            )
        assert raised.value.status_code == 400

    def test_a_non_github_host_is_refused_without_an_explicit_forge(self):
        with pytest.raises(HTTPException) as raised:
            provisioning.kb_vault_plan(
                "https://gitlab.com/acme/vault.git",
                "main",
                {"auth_method": "token", "token": "t"},
                None,
            )
        assert raised.value.status_code == 400
        assert "GitHub only" in raised.value.detail

    def test_embedded_credentials_are_refused(self):
        with pytest.raises(HTTPException) as raised:
            provisioning.kb_vault_plan(
                "https://bot:pat@github.com/acme/vault.git",
                "main",
                {"auth_method": "token", "token": "t"},
                None,
            )
        assert raised.value.status_code == 400

    def test_github_com_is_inferred_and_not_marked_explicit(self):
        plan = provisioning.kb_vault_plan(
            REPO_URL, "main", {"auth_method": "token", "token": "t"}, None
        )
        assert (plan.forge, plan.owner, plan.repo) == ("github", "acme", "vault")
        assert plan.explicit_forge is False

    def test_an_explicit_forge_is_remembered(self):
        """Only an explicitly-set forge is written into the connector config.

        The field exists for GitHub Enterprise, whose host cannot be inferred;
        sending it for github.com is the same code path and needs no trusted
        enterprise endpoint to exercise.
        """
        plan = provisioning.kb_vault_plan(
            REPO_URL, "main", {"auth_method": "token", "token": "t"}, "github"
        )
        assert plan.explicit_forge is True

    def test_the_plan_never_renders_its_pat(self):
        """It carries the token, so its repr is suppressed on purpose."""
        plan = provisioning.kb_vault_plan(
            REPO_URL, "main", {"auth_method": "token", "token": "ghp_secret"}, None
        )
        assert "ghp_secret" not in repr(plan)


class TestPlanFromConnector:
    async def _plan(self, row, caller=OWNER, project_id=None):
        store = MagicMock()
        store.get_datasource = AsyncMock(return_value=row)
        store.list_datasource_projects = AsyncMock(return_value=[])
        return await provisioning.plan_kb_vault_from_connector(
            DATASOURCE_ID, caller, project_id, dependencies=_deps(store=store)
        )

    @pytest.mark.asyncio
    async def test_a_missing_connector_is_404(self):
        with pytest.raises(HTTPException) as raised:
            await self._plan(None)
        assert raised.value.status_code == 404

    @pytest.mark.asyncio
    async def test_a_non_kb_connector_is_refused(self):
        with pytest.raises(HTTPException) as raised:
            await self._plan(_connector(type="postgres"))
        assert raised.value.status_code == 400

    @pytest.mark.asyncio
    async def test_a_stranger_may_not_adopt_someone_elses_connector(self):
        with pytest.raises(HTTPException) as raised:
            await self._plan(_connector(), caller=STRANGER)
        assert raised.value.status_code == 403

    @pytest.mark.asyncio
    async def test_an_admin_may(self):
        plan = await self._plan(_connector(), caller=ADMIN)
        assert plan.datasource_id == DATASOURCE_ID

    @pytest.mark.asyncio
    async def test_a_connector_that_is_already_a_project_vault_is_409(self):
        from orchestrator.services.kb_datasources import NATIVE_PROJECT_CONFIG_KEY

        row = _connector(
            config={"root_path": "knowledge", NATIVE_PROJECT_CONFIG_KEY: PROJECT_ID}
        )
        with pytest.raises(HTTPException) as raised:
            await self._plan(row)
        assert raised.value.status_code == 409

    @pytest.mark.asyncio
    async def test_a_published_connector_is_409(self):
        """Adoption takes the row private and drops everyone else's index."""
        with pytest.raises(HTTPException) as raised:
            await self._plan(_connector(is_global=True))
        assert raised.value.status_code == 409
        assert "published to everyone" in raised.value.detail

    @pytest.mark.asyncio
    async def test_a_connector_shared_with_other_projects_is_409(self):
        other = str(uuid4())
        store = MagicMock()
        store.get_datasource = AsyncMock(return_value=_connector())
        store.list_datasource_projects = AsyncMock(return_value=[other])
        with pytest.raises(HTTPException) as raised:
            await provisioning.plan_kb_vault_from_connector(
                DATASOURCE_ID, OWNER, PROJECT_ID, dependencies=_deps(store=store)
            )
        assert raised.value.status_code == 409
        assert "shared with other projects" in raised.value.detail

    @pytest.mark.asyncio
    async def test_the_adopting_project_itself_does_not_count_as_a_share(self):
        store = MagicMock()
        store.get_datasource = AsyncMock(return_value=_connector())
        store.list_datasource_projects = AsyncMock(return_value=[PROJECT_ID])
        plan = await provisioning.plan_kb_vault_from_connector(
            DATASOURCE_ID, OWNER, PROJECT_ID, dependencies=_deps(store=store)
        )
        assert plan.datasource_id == DATASOURCE_ID

    @pytest.mark.asyncio
    async def test_a_foreign_note_root_is_refused(self):
        """kb_materialize writes ``knowledge/``; any other root reads elsewhere."""
        with pytest.raises(HTTPException) as raised:
            await self._plan(_connector(config={"root_path": "docs"}))
        assert raised.value.status_code == 400
        assert "'docs/'" in raised.value.detail

    @pytest.mark.asyncio
    async def test_an_empty_note_root_is_accepted(self):
        plan = await self._plan(_connector(config={}))
        assert plan.datasource_id == DATASOURCE_ID

    @pytest.mark.asyncio
    async def test_an_ssh_only_connector_is_refused(self):
        """Writes go through the GitHub contents API; SSH has no equivalent."""
        with pytest.raises(HTTPException) as raised:
            await self._plan(_connector(credentials={"ssh_key": "-----BEGIN..."}))
        assert raised.value.status_code == 400
        assert "token credential" in raised.value.detail

    @pytest.mark.asyncio
    async def test_json_encoded_credentials_are_decoded(self):
        plan = await self._plan(_connector(credentials='{"token": "ghp_live"}'))
        assert plan.credentials == {"auth_method": "token", "token": "ghp_live"}

    @pytest.mark.asyncio
    async def test_the_policy_revision_is_carried_for_the_cas_write(self):
        plan = await self._plan(_connector())
        assert plan.policy_revision == 7

    @pytest.mark.asyncio
    async def test_the_inline_form_bypasses_the_connector_checks(self):
        body = ExternalKnowledgeBase(repo_url=REPO_URL, token="ghp_inline")
        store = MagicMock()
        store.get_datasource = AsyncMock(
            side_effect=AssertionError("connector lookup on the inline path")
        )
        plan = await provisioning.plan_external_kb_vault(
            body, caller=OWNER, dependencies=_deps(store=store)
        )
        assert plan.datasource_id is None
        assert plan.credentials["token"] == "ghp_inline"


# =============================================================================
# Adoption converts the row in place
# =============================================================================


class TestConnectorAdoption:
    def _plan(self) -> provisioning.KbVaultPlan:
        return provisioning.kb_vault_plan(
            REPO_URL,
            "main",
            {"auth_method": "token", "token": "ghp_live"},
            None,
            datasource_id=DATASOURCE_ID,
            policy_revision=7,
        )

    def _store(self, **over: Any) -> MagicMock:
        store = MagicMock()
        store.get_native_project_kb_datasource_ref = AsyncMock(return_value=None)
        store.add_project_repository = AsyncMock(return_value={"id": str(uuid4())})
        store.update_datasource_with_policy = AsyncMock(
            return_value={"id": DATASOURCE_ID}
        )
        store.remove_project_repository = AsyncMock(return_value=True)
        store.create_datasource = AsyncMock(
            side_effect=AssertionError("adoption must not create a second row")
        )
        for key, value in over.items():
            setattr(store, key, value)
        return store

    @pytest.mark.asyncio
    async def test_the_existing_row_is_narrowed_under_its_expected_revision(self):
        store = self._store()
        purge = AsyncMock()
        deps = _deps(store=store)
        (
            _repo,
            datasource,
        ) = await self._call(deps, store, purge)

        assert datasource["id"] == DATASOURCE_ID
        kwargs = store.update_datasource_with_policy.await_args.kwargs
        assert kwargs["expected_policy_revision"] == 7
        assert kwargs["scope_mode"] == "projects"
        assert kwargs["project_ids"] == [PROJECT_ID]
        assert kwargs["config"]["root_path"] == "knowledge"

    @pytest.mark.asyncio
    async def test_the_stale_external_index_is_purged(self):
        store = self._store()
        purge = AsyncMock()
        await self._call(_deps(store=store), store, purge)
        assert purge.await_args.args[0] == DATASOURCE_ID

    @pytest.mark.asyncio
    async def test_a_failed_purge_never_fails_the_provisioned_project(self):
        """Disposable cleanup: the marker is already stored, the sweep let go."""
        store = self._store()
        purge = AsyncMock(side_effect=RuntimeError("vector db down"))
        _repo, datasource = await self._call(_deps(store=store), store, purge)
        assert datasource["id"] == DATASOURCE_ID

    @pytest.mark.asyncio
    async def test_a_second_native_row_refuses_before_any_write(self):
        store = self._store(
            get_native_project_kb_datasource_ref=AsyncMock(
                return_value={"id": str(uuid4())}
            )
        )
        with pytest.raises(HTTPException) as raised:
            await self._call(_deps(store=store), store, AsyncMock())
        assert raised.value.status_code == 409
        store.add_project_repository.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_lost_cas_race_rolls_the_repository_row_back(self):
        """The repo row is written first, so it must be undone on failure."""
        repo_id = str(uuid4())
        store = self._store(
            add_project_repository=AsyncMock(return_value={"id": repo_id}),
            update_datasource_with_policy=AsyncMock(return_value=None),
        )
        with pytest.raises(RuntimeError):
            await self._call(_deps(store=store), store, AsyncMock())
        store.remove_project_repository.assert_awaited_once_with(repo_id)

    async def _call(self, deps, store, purge):
        import orchestrator.services.project_provisioning as module

        original = module.purge_kb_datasource_index
        module.purge_kb_datasource_index = purge
        try:
            return await provisioning.provision_external_project_knowledge_repo(
                {"id": PROJECT_ID, "name": "Vault"},
                OWNER_ID,
                self._plan(),
                dependencies=deps,
            )
        finally:
            module.purge_kb_datasource_index = original


# =============================================================================
# The per-project heal lock
# =============================================================================


class _HealStore:
    """A store whose project row is visible to whoever reads it next."""

    def __init__(self) -> None:
        self.row: dict[str, Any] = {
            "id": PROJECT_ID,
            "name": "Locked",
            "is_default": False,
            "main_cloud_backend": None,
            "main_cloud_folder_handle": None,
            "nextcloud_folder_id": None,
        }

    async def get_project(self, _project_id: str) -> dict[str, Any]:
        return dict(self.row)

    async def update_project(self, _project_id: str, **kwargs: Any) -> bool:
        self.row.update(kwargs)
        return True

    async def get_project_members(self, _project_id: str) -> list[dict[str, Any]]:
        return []

    # The project's own cloud folder connector, unique by its managed key.
    connectors: dict[str, dict[str, Any]]

    async def get_datasource_by_managed_key(self, key: str) -> dict[str, Any] | None:
        return getattr(self, "connectors", {}).get(key)

    async def create_datasource(self, **row: Any) -> dict[str, Any] | None:
        connectors = self.__dict__.setdefault("connectors", {})
        await asyncio.sleep(0)
        if row["managed_key"] in connectors:
            return None
        connectors[row["managed_key"]] = {"id": str(uuid4()), **row}
        return connectors[row["managed_key"]]


def _folder_backend() -> SimpleNamespace:
    handle = SimpleNamespace(native_id="drive-1", to_db=lambda: "opencloud:drive-1")

    async def ensure_project_folder(**_kwargs: Any):
        # Yield, so a caller without the lock would interleave here and make a
        # second drive — which is exactly the failure the lock prevents.
        await asyncio.sleep(0)
        return handle

    return SimpleNamespace(
        is_initialized=True,
        backend_id="opencloud",
        backend_instance_id=str(uuid4()),
        ensure_group=AsyncMock(),
        ensure_project_folder=AsyncMock(side_effect=ensure_project_folder),
        legacy_folder_id=lambda _handle: None,
    )


class TestHealLock:
    @pytest.mark.asyncio
    async def test_two_concurrent_heals_create_exactly_one_space(self):
        store = _HealStore()
        backend = _folder_backend()
        deps = _deps(
            store=store,
            main_cloud_router=SimpleNamespace(
                for_project_optional=MagicMock(return_value=backend),
                for_owner=MagicMock(return_value=backend),
                active_instance_id=backend.backend_instance_id,
            ),
        )

        await asyncio.gather(
            provisioning.ensure_project_cloud_resources(
                dict(store.row), dependencies=deps
            ),
            provisioning.ensure_project_cloud_resources(
                dict(store.row), dependencies=deps
            ),
        )

        assert backend.ensure_project_folder.await_count == 1
        assert store.row["main_cloud_folder_handle"] == "opencloud:drive-1"
        # The new folder got its own connectors, one of each kind, once: the
        # folder's and its copy into the project's jobs.
        by_kind = {
            row["managed_key"].split(":")[0]: row for row in store.connectors.values()
        }
        assert sorted(by_kind) == ["project-cloud", "project-cloud-checkout"]
        assert len(store.connectors) == 2
        copy = by_kind["project-cloud-checkout"]
        assert copy["managed_key"] == f"project-cloud-checkout:{PROJECT_ID}"
        assert copy["ds_type"] == "cloud_folder_checkout"
        assert copy["config"] == {"project_id": PROJECT_ID}
        connector = by_kind["project-cloud"]
        assert connector["managed_key"] == f"project-cloud:{PROJECT_ID}"
        assert connector["ds_type"] == "cloud_folder"
        assert connector["project_ids"] == [PROJECT_ID]
        assert connector["config"] == {
            "folder_kind": "project",
            "project_id": PROJECT_ID,
            "backend_id": "opencloud",
            "backend_instance_id": backend.backend_instance_id,
            "folder_handle": "opencloud:drive-1",
        }

    @pytest.mark.asyncio
    async def test_the_lock_map_is_owned_by_the_dependencies_not_the_module(self):
        """Two applications must not share a project's heal lock."""
        store = _HealStore()
        backend = _folder_backend()
        router = SimpleNamespace(
            for_project_optional=MagicMock(return_value=backend),
            for_owner=MagicMock(return_value=backend),
            active_instance_id=backend.backend_instance_id,
        )
        first = _deps(store=store, main_cloud_router=router)
        second = _deps(store=_HealStore(), main_cloud_router=router)

        await provisioning.ensure_project_cloud_resources(
            dict(store.row), dependencies=first
        )

        assert PROJECT_ID in first.repair.heal_locks
        assert second.repair.heal_locks == {}

    @pytest.mark.asyncio
    async def test_a_default_project_provisions_nothing(self):
        """Defaults piggyback on the owner's home Space; a second one is dead state."""
        router = SimpleNamespace(
            for_project_optional=MagicMock(
                side_effect=AssertionError("resolved a backend for a default project")
            ),
            for_owner=MagicMock(
                side_effect=AssertionError("resolved a backend for a default project")
            ),
        )
        project = {"id": PROJECT_ID, "name": "Default", "is_default": True}

        result = await provisioning.ensure_project_cloud_resources(
            project, dependencies=_deps(main_cloud_router=router)
        )

        assert result is project


# =============================================================================
# A refused folder create reaches the project page
# =============================================================================


class _FolderErrorStore(_HealStore):
    """``_HealStore`` that records the folder error, as the row column does."""

    def __init__(self) -> None:
        super().__init__()
        self.error_writes: list[str | None] = []

    async def set_project_main_cloud_folder_error(
        self, _project_id: str, error: str | None
    ) -> None:
        self.error_writes.append(error)
        self.row["main_cloud_folder_error"] = error


class TestRefusedFolderReachesTheProjectPage:
    """Groupfolders 20+ refuses a taken mount point; a refusal is neither
    swallowed nor fatal: the project stands without a folder and its row says
    why (a closed error kind), until a create succeeds."""

    @staticmethod
    def _deps_for(store: _HealStore, backend: SimpleNamespace):
        return _deps(
            store=store,
            main_cloud_router=SimpleNamespace(
                for_project_optional=MagicMock(return_value=backend),
                for_owner=MagicMock(return_value=backend),
                active_instance_id=backend.backend_instance_id,
            ),
        )

    @pytest.mark.asyncio
    async def test_a_refusal_is_recorded_then_cleared_by_a_success(self):
        from orchestrator.services.cloud import (
            CloudBackendError,
            CloudBackendErrorKind,
        )

        store = _FolderErrorStore()
        backend = _folder_backend()
        handle = SimpleNamespace(native_id="drive-1", to_db=lambda: "opencloud:drive-1")
        backend.ensure_project_folder = AsyncMock(
            side_effect=[
                CloudBackendError(
                    CloudBackendErrorKind.ALREADY_EXISTS,
                    "Group Folders exist at both names",
                    backend="nextcloud",
                ),
                handle,
            ]
        )
        deps = self._deps_for(store, backend)

        result = await provisioning.ensure_project_cloud_resources(
            dict(store.row), dependencies=deps
        )

        assert store.row["main_cloud_folder_error"] == "already_exists"
        assert result["main_cloud_folder_error"] == "already_exists"
        assert store.row["main_cloud_folder_handle"] is None
        assert getattr(store, "connectors", {}) == {}
        backend.ensure_project_folder.assert_awaited_with(
            project_name="Locked",
            group_id=f"project-{PROJECT_ID}",
            project_id=PROJECT_ID,
        )

        # The same refusal again is not rewritten; a success clears it.
        await provisioning.ensure_project_cloud_resources(
            dict(store.row), dependencies=deps
        )
        assert store.row["main_cloud_folder_error"] is None
        assert store.row["main_cloud_folder_handle"] == "opencloud:drive-1"
        assert store.error_writes == ["already_exists", None]

    @pytest.mark.asyncio
    async def test_any_other_failure_is_unknown(self):
        store = _FolderErrorStore()
        backend = _folder_backend()
        backend.ensure_project_folder = AsyncMock(side_effect=AttributeError("x"))

        await provisioning.ensure_project_cloud_resources(
            dict(store.row), dependencies=self._deps_for(store, backend)
        )

        assert store.error_writes == ["unknown"]

    @pytest.mark.asyncio
    async def test_a_failing_record_never_fails_provisioning(self):
        store = _HealStore()  # no set_project_main_cloud_folder_error
        backend = _folder_backend()
        backend.ensure_project_folder = AsyncMock(side_effect=AttributeError("x"))
        deps = self._deps_for(store, backend)

        result = await provisioning.ensure_project_cloud_resources(
            dict(store.row), dependencies=deps
        )

        assert result.get("main_cloud_folder_error") is None
        deps.logger.warning.assert_called()


# =============================================================================
# A removed member leaves the main cloud's own project group
# =============================================================================


class TestRemovedMemberLeavesTheCloudGroup:
    """The group carries the folder's permissions in the cloud itself, so a
    member left in it keeps the folder through their own cloud account ("My
    cloud"). Removal is best effort: the SRW removal stands either way."""

    PROJECT = {
        "id": PROJECT_ID,
        "is_default": False,
        "main_cloud_backend": "nextcloud",
    }

    def _deps_with(self, backend):
        return _deps(
            main_cloud_router=SimpleNamespace(
                for_project_optional=MagicMock(return_value=backend)
            )
        )

    @pytest.mark.asyncio
    async def test_the_member_is_removed_from_the_project_group(self, monkeypatch):
        backend = SimpleNamespace(
            is_initialized=True, remove_user_from_group=AsyncMock()
        )
        monkeypatch.setattr(
            provisioning, "resolve_user_identity_cached", AsyncMock(return_value="u-1")
        )
        deps = self._deps_with(backend)

        removed = await provisioning.remove_project_member_from_backend_group(
            dict(self.PROJECT), {"id": STRANGER_ID}, dependencies=deps
        )

        assert removed is True
        backend.remove_user_from_group.assert_awaited_once_with(
            "u-1", f"project-{PROJECT_ID}"
        )
        deps.main_cloud_router.for_project_optional.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_refusing_cloud_is_logged_not_raised(self, monkeypatch):
        backend = SimpleNamespace(
            is_initialized=True,
            remove_user_from_group=AsyncMock(side_effect=RuntimeError("ocs 500")),
        )
        monkeypatch.setattr(
            provisioning, "resolve_user_identity_cached", AsyncMock(return_value="u-1")
        )
        deps = self._deps_with(backend)

        removed = await provisioning.remove_project_member_from_backend_group(
            dict(self.PROJECT), {"id": STRANGER_ID}, dependencies=deps
        )

        assert removed is False
        deps.logger.warning.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "project",
        [
            {**PROJECT, "is_default": True},
            {**PROJECT, "main_cloud_backend": None},
        ],
        ids=["default-project", "no-cloud"],
    )
    async def test_a_project_without_a_cloud_group_has_nothing_to_leave(self, project):
        backend = SimpleNamespace(
            is_initialized=True,
            remove_user_from_group=AsyncMock(side_effect=AssertionError("touched")),
        )

        assert (
            await provisioning.remove_project_member_from_backend_group(
                project, {"id": STRANGER_ID}, dependencies=self._deps_with(backend)
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_an_unresolvable_installation_is_never_guessed(self):
        deps = self._deps_with(None)

        removed = await provisioning.remove_project_member_from_backend_group(
            dict(self.PROJECT), {"id": STRANGER_ID}, dependencies=deps
        )

        assert removed is False
        deps.logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_removing_a_member_removes_them_from_the_cloud_group(
        self, monkeypatch
    ):
        from orchestrator.services import projects
        from orchestrator.services.cloud import folder_grants

        monkeypatch.setattr(
            folder_grants, "revoke_folder_grants", AsyncMock(return_value=0)
        )
        leave = AsyncMock(return_value=True)
        monkeypatch.setattr(
            provisioning, "remove_project_member_from_backend_group", leave
        )
        member = {"id": STRANGER_ID}
        store = SimpleNamespace(
            get_user_role_in_project=AsyncMock(return_value="viewer"),
            remove_project_member=AsyncMock(return_value=True),
            get_user=AsyncMock(return_value=member),
            get_project=AsyncMock(return_value=dict(self.PROJECT)),
        )
        provisioning_deps = _deps()
        deps = projects.ProjectDependencies(
            store=store,
            vector_db=MagicMock(),
            forge=SimpleNamespace(is_initialized=False),
            keycloak_groups=SimpleNamespace(is_initialized=False),
            main_cloud_router=MagicMock(),
            logger=MagicMock(),
            provisioning=provisioning_deps,
            with_validated_tool_overrides=lambda value: value,
        )

        result = await projects.remove_project_member(
            PROJECT_ID, STRANGER_ID, caller={"id": STRANGER_ID}, dependencies=deps
        )

        assert result == {"status": "removed"}
        leave.assert_awaited_once_with(
            dict(self.PROJECT), member, dependencies=provisioning_deps
        )


# =============================================================================
# Member sync degrades when its optional tiers are down
# =============================================================================


class TestMemberSyncDegradation:
    @pytest.mark.asyncio
    async def test_keycloak_being_down_still_writes_the_backend_group(self):
        backend = SimpleNamespace(
            is_initialized=True, add_user_to_group=AsyncMock(return_value=None)
        )
        deps = _deps(keycloak_groups=SimpleNamespace(is_initialized=False))

        import orchestrator.services.project_provisioning as module

        original = module.resolve_user_identity_cached
        module.resolve_user_identity_cached = AsyncMock(return_value="u-1")
        try:
            await provisioning.sync_project_member_to_groups(
                PROJECT_ID,
                f"project-{PROJECT_ID}",
                {"id": OWNER_ID, "keycloak_sub": "sub-1"},
                backend,
                dependencies=deps,
            )
        finally:
            module.resolve_user_identity_cached = original

        backend.add_user_to_group.assert_awaited_once_with(
            "u-1", f"project-{PROJECT_ID}"
        )

    @pytest.mark.asyncio
    async def test_a_failing_backend_add_is_logged_not_raised(self):
        backend = SimpleNamespace(
            is_initialized=True,
            add_user_to_group=AsyncMock(side_effect=RuntimeError("libregraph 503")),
        )
        keycloak = SimpleNamespace(
            is_initialized=True, add_user_to_project_group=AsyncMock()
        )
        deps = _deps(keycloak_groups=keycloak)

        import orchestrator.services.project_provisioning as module

        original = module.resolve_user_identity_cached
        module.resolve_user_identity_cached = AsyncMock(return_value="u-1")
        try:
            await provisioning.sync_project_member_to_groups(
                PROJECT_ID,
                f"project-{PROJECT_ID}",
                {"id": OWNER_ID, "keycloak_sub": "sub-1"},
                backend,
                dependencies=deps,
            )
        finally:
            module.resolve_user_identity_cached = original

        # The durable half still landed.
        keycloak.add_user_to_project_group.assert_awaited_once()
        deps.logger.debug.assert_called_once()

    @pytest.mark.asyncio
    async def test_an_uninitialised_backend_skips_identity_resolution_entirely(self):
        backend = SimpleNamespace(
            is_initialized=False,
            add_user_to_group=AsyncMock(
                side_effect=AssertionError("wrote to an uninitialised backend")
            ),
        )
        await provisioning.sync_project_member_to_groups(
            PROJECT_ID,
            f"project-{PROJECT_ID}",
            {"id": OWNER_ID},
            backend,
            dependencies=_deps(),
        )

    @pytest.mark.asyncio
    async def test_an_unresolvable_installation_skips_the_heal_loudly(self):
        """A pre-0186 row: we must not guess which installation holds the Space."""
        project = {
            "id": PROJECT_ID,
            "name": "Legacy",
            "is_default": False,
            "main_cloud_backend": "nextcloud",
        }
        deps = _deps(
            main_cloud_router=SimpleNamespace(
                for_project_optional=MagicMock(return_value=None),
                for_owner=MagicMock(
                    side_effect=AssertionError("fell through to for_owner")
                ),
            )
        )

        result = await provisioning.ensure_project_cloud_resources(
            project, dependencies=deps
        )

        assert result is project
        deps.logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_member_sweep_failure_never_propagates(self):
        store = MagicMock()
        store.get_project_members = AsyncMock(side_effect=RuntimeError("db gone"))
        backend = SimpleNamespace(
            is_initialized=True,
            backend_id="opencloud",
            backend_instance_id=str(uuid4()),
        )
        deps = _deps(
            store=store,
            keycloak_groups=SimpleNamespace(
                is_initialized=True, ensure_project_group=AsyncMock()
            ),
            main_cloud_router=SimpleNamespace(
                for_project_optional=MagicMock(return_value=backend),
                for_owner=MagicMock(return_value=backend),
                active_instance_id=backend.backend_instance_id,
            ),
        )
        project = {
            "id": PROJECT_ID,
            "name": "Has a folder",
            "is_default": False,
            "main_cloud_folder_handle": "opencloud:drive-9",
        }

        result = await provisioning.ensure_project_cloud_resources(
            project, dependencies=deps
        )

        assert result is project
        deps.logger.debug.assert_called_once()


# =============================================================================
# Background repair throttling
# =============================================================================


class TestBackgroundRepair:
    @pytest.mark.asyncio
    async def test_a_throttled_coroutine_is_closed_not_leaked(self):
        deps = _deps()
        ran: list[str] = []

        async def work(tag: str) -> None:
            ran.append(tag)

        assert provisioning.fire_background_repair(
            "k", work("first"), dependencies=deps
        )
        assert not provisioning.fire_background_repair(
            "k", work("second"), dependencies=deps
        )
        await asyncio.gather(*list(deps.repair.bg_repair_tasks))
        assert ran == ["first"]

    @pytest.mark.asyncio
    async def test_the_cooldown_map_is_per_application(self):
        first, second = _deps(), _deps()

        async def work() -> None:
            return None

        assert provisioning.fire_background_repair("k", work(), dependencies=first)
        assert provisioning.fire_background_repair("k", work(), dependencies=second)
        await asyncio.gather(
            *list(first.repair.bg_repair_tasks), *list(second.repair.bg_repair_tasks)
        )


# =============================================================================
# The default-project knowledge vault
# =============================================================================


class TestDefaultProjectKnowledge:
    @pytest.mark.asyncio
    async def test_a_disabled_forge_provisions_nothing(self):
        deps = _deps(forge=SimpleNamespace(is_initialized=False))
        await provisioning.provision_default_project_knowledge(
            {"id": OWNER_ID}, {"id": PROJECT_ID}, dependencies=deps
        )
        deps.logger.warning.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_provisioning_failure_is_logged_not_raised(self):
        store = MagicMock()
        store.add_project_repository = AsyncMock(side_effect=RuntimeError("gitea 500"))
        deps = _deps(
            store=store,
            forge=SimpleNamespace(is_initialized=True),
        )

        import orchestrator.services.project_provisioning as module

        original = module.create_managed_repository
        module.create_managed_repository = AsyncMock(
            return_value=("http://gitea/x.git", {"id": "intent"})
        )
        try:
            await provisioning.provision_default_project_knowledge(
                {"id": OWNER_ID}, {"id": PROJECT_ID, "name": "P"}, dependencies=deps
            )
        finally:
            module.create_managed_repository = original

        deps.logger.warning.assert_called_once()
