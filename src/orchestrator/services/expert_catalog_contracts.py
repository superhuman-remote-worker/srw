"""Explicit collaborators and application-owned state for expert catalogues."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

from orchestrator.schemas.expert_catalog import ExpertInfo, SkillInfo


class ExpertCatalogStore(Protocol):
    """The existing catalogue/default storage operations, without pool creation."""

    async def get_projects_for_user(
        self, user_id: str, limit: int = 100, statuses: list[str] | None = None
    ) -> list[dict[str, Any]]: ...

    async def list_grants_for_scopes(
        self, *, user_id: str | None, project_ids: list[str]
    ) -> dict[str, list[dict]]: ...

    async def create_expert(
        self,
        *,
        name: str,
        display_name: str,
        expert_type: str,
        owner_id: str,
        description: str | None = None,
        icon: str = "smart_toy",
        color: str = "#6B7280",
        tags: list[str] | None = None,
        config: dict[str, Any] | None = None,
        prompts: dict[str, Any] | None = None,
        is_global: bool = False,
        srw_layers: list[dict[str, Any]] | None = None,
        srw_config_name: str | None = None,
        srw_asset_name: str | None = None,
    ) -> dict[str, Any]: ...

    async def get_expert_by_id(self, expert_id: str) -> dict[str, Any] | None: ...

    async def get_expert_visible_by_id(
        self,
        expert_id: str,
        *,
        user_id: str,
        project_ids: list[str] | None = None,
        is_admin: bool = False,
    ) -> dict[str, Any] | None: ...

    async def list_experts_visible(
        self, *, user_id: str, project_ids: list[str], expert_type: str | None = None
    ) -> list[dict[str, Any]]: ...

    async def get_application_expert_default(
        self, expert_type: str
    ) -> dict[str, Any] | None: ...

    async def list_application_expert_defaults(self) -> list[dict[str, Any]]: ...

    async def set_application_expert_default(
        self, *, expert_type: str, expert_id: str, actor_user_id: str
    ) -> dict[str, Any]: ...

    async def get_user_expert_default(
        self, *, user_id: str, expert_type: str
    ) -> dict[str, Any] | None: ...

    async def set_user_expert_default(
        self, *, user_id: str, expert_type: str, expert_id: str
    ) -> dict[str, Any]: ...

    async def clear_user_expert_default(
        self, *, user_id: str, expert_type: str
    ) -> bool: ...

    async def fork_and_set_user_expert_default(
        self, *, user_id: str, expert_type: str, source: dict[str, Any]
    ) -> dict[str, Any]: ...

    async def get_project_default_expert(
        self, *, project_id: str, expert_type: str
    ) -> dict[str, Any] | None: ...

    async def list_project_linked_experts(
        self, project_id: str
    ) -> list[dict[str, Any]]: ...

    async def get_project_linked_expert(
        self, project_id: str, expert_ref: str
    ) -> dict[str, Any] | None: ...

    async def set_project_default_expert(
        self,
        *,
        project_id: str,
        expert_type: str,
        expert_id: str,
        actor_user_id: str,
        config_override: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    async def clear_project_default_expert(
        self, *, project_id: str, expert_type: str, actor_user_id: str
    ) -> bool: ...

    async def record_managed_expert_update(
        self, *, expert_id: str, expert_type: str, actor_user_id: str
    ) -> None: ...

    async def update_expert(
        self, expert_id: str, *, updated_by: str, **fields: Any
    ) -> dict[str, Any] | None: ...

    async def expert_delete_blockers(self, expert_id: str) -> list[dict[str, Any]]: ...

    async def delete_expert(self, expert_id: str) -> bool: ...

    async def create_skill(
        self,
        *,
        name: str,
        display_name: str,
        owner_id: str,
        files: dict[str, str],
        description: str | None = None,
        icon: str = "extension",
        color: str = "#6B7280",
        tags: list[str] | None = None,
        is_global: bool = False,
    ) -> dict[str, Any]: ...

    async def get_skill_by_id(self, skill_id: str) -> dict[str, Any] | None: ...

    async def get_skill_files(self, skill_id: str) -> dict[str, str]: ...

    async def list_skills_visible(self, *, user_id: str) -> list[dict[str, Any]]: ...

    async def update_skill(
        self,
        skill_id: str,
        *,
        updated_by: str,
        files: dict[str, str] | None = None,
        **fields: Any,
    ) -> dict[str, Any] | None: ...

    async def delete_skill(self, skill_id: str) -> bool: ...

    async def resolve_default_for_capability(self, capability: str) -> str | None: ...

    async def get_user_settings(self, user_id: str) -> dict[str, Any]: ...

    async def get_project_repositories(
        self, project_id: str, role: str | None = None
    ) -> list[dict[str, Any]]: ...


class ProjectExpertForge(Protocol):
    @property
    def is_initialized(self) -> bool: ...

    async def list_contents(
        self, repo_name: str, path: str
    ) -> list[dict[str, Any]] | None: ...
    async def get_file_content(self, repo_name: str, path: str) -> str | None: ...


class VisibleProjectIds(Protocol):
    async def __call__(
        self, user: dict[str, Any], db: ExpertCatalogStore
    ) -> set[UUID] | Literal["all"]: ...


class SaveExpert(Protocol):
    async def __call__(
        self, config: dict[str, Any], *, user: dict[str, Any]
    ) -> None: ...


class StripSaveGrants(Protocol):
    async def __call__(
        self, config: dict[str, Any], *, user: dict[str, Any]
    ) -> tuple[dict[str, Any], list[str]]: ...


class PrefetchRosterRefs(Protocol):
    async def __call__(
        self,
        *,
        expert_row: dict[str, Any] | None = None,
        overrides: Iterable[dict[str, Any] | None] = (),
        user_id: str | None = None,
        project_ids: list[str] | None = None,
    ) -> dict[str, Any]: ...


DefaultModels = Callable[[str | None], Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class ExpertWritePolicy:
    """Per-call canonical gates; the HTTP adapter binds the original request."""

    enforce_save: SaveExpert
    enforce_save_prelude: Callable[[], Awaitable[None]]
    strip_save_grants: StripSaveGrants


@dataclass(frozen=True)
class SkillArchive:
    name: str
    content: bytes


@dataclass
class ExpertCatalogState:
    """One app's reloadable bundled catalogue; no module-global caches."""

    experts: list[ExpertInfo] | None = None
    library: list[ExpertInfo] | None = None
    skills: list[SkillInfo] | None = None


@dataclass(frozen=True)
class ExpertCatalogDependencies:
    store: ExpertCatalogStore
    state: ExpertCatalogState
    get_config_dir: Callable[[], Path]
    load_settings_matrix: Callable[[Path], dict[str, Any]]
    experts_enabled: Callable[[], bool]
    account_defaults_layer: Callable[[str | None, str], Awaitable[dict[str, Any]]]
    visible_project_ids: VisibleProjectIds
    with_validated_tool_overrides: Callable[
        [dict[str, Any] | None], dict[str, Any] | None
    ]
    looks_like_uuid: Callable[[str], bool]
    forge: ProjectExpertForge
    # Production binds the canonical resource store. Pure catalogue fixtures
    # may leave it absent when exercising only private harness formatting.
    manifests: Any | None = None
