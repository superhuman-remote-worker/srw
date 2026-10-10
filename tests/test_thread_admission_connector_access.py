"""Choosing a connector's level when a session is created (main cloud 5.4).

Every attached connector with access levels (a cloud folder) is resolved
once at admission: the level the request asks for (``datasource_access``,
``execution.connectors.<alias>.access``, or the deprecated
``protected_cloud`` alias), else its project link's default lowered to what
the tier, the deployment and the owner's role allow. The levels are stored in
``metadata.datasource_access`` with their origins in the selection's
provenance; the protected marker and the pinned lane follow the folder bound
at ``protected``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.services import thread_admission as ta
from orchestrator.services.cloud.capabilities import (
    CloudCapability,
    ProviderCapabilities,
)
from orchestrator.services.connector_refs import resolve_execution_connectors
from tests.test_b06_lane_b_thread_admission import (
    FOLDER,
    PROJECT,
    USER,
    _deps,
    _folder_row,
)

OTHER_FOLDER = "77777777-7777-4777-8777-777777777777"
OTHER_PROJECT = "88888888-8888-4888-8888-888888888888"

#: A provider that offers every level of a project folder on the Container
#: tier and read-only on a VM, so the rule, not today's matrix, is tested.
_EVERY_LEVEL = ProviderCapabilities(
    backend_id="nextcloud",
    title="Nextcloud",
    capabilities=(
        CloudCapability(
            "cloud_folder",
            "project",
            "read_only",
            "offered",
            "a reader",
            frozenset({"sandbox", "vm"}),
        ),
        CloudCapability(
            "cloud_folder",
            "project",
            "protected",
            "offered",
            "reviewed",
            frozenset({"sandbox"}),
        ),
        CloudCapability(
            "cloud_folder",
            "project",
            "read_write",
            "offered",
            "a writer",
            frozenset({"sandbox"}),
        ),
    ),
)


@pytest.fixture(autouse=True)
def every_level(monkeypatch):
    # Container sessions by default (the defaults chain, slice A2b).
    monkeypatch.setenv("WORKSPACE_DEFAULTS", '{"sessions": "container"}')
    monkeypatch.delenv("WORKSPACE_BUILTIN_TEMPLATES", raising=False)
    monkeypatch.setattr(
        "orchestrator.services.cloud.provider_capabilities",
        lambda backend_id: _EVERY_LEVEL if backend_id == "nextcloud" else None,
    )


def _foreign_row() -> dict[str, Any]:
    row = _folder_row()
    row.update(
        id=OTHER_FOLDER,
        managed_key=f"project-cloud:{OTHER_PROJECT}",
        config={**row["config"], "project_id": OTHER_PROJECT},
    )
    return row


def _deps_with(
    rows: list[dict[str, Any]] | None = None,
    *,
    roles: dict[str, str] | None = None,
    **over: Any,
):
    return _deps(
        store={
            "resolve_datasources_for_thread": AsyncMock(
                return_value=rows if rows is not None else [_folder_row()]
            ),
            "get_user_project_roles": AsyncMock(return_value=roles or {}),
        },
        **over,
    )


def _body(**fields: Any) -> ThreadCreateRequest:
    fields.setdefault("datasource_ids", [FOLDER])
    return ThreadCreateRequest(project_id=PROJECT, **fields)


async def _plan(body: ThreadCreateRequest, deps):
    return await ta.resolve_thread_creation_plan(body, USER, dependencies=deps)


async def _refused(body: ThreadCreateRequest, deps) -> dict[str, Any]:
    with pytest.raises(HTTPException) as exc:
        await _plan(body, deps)
    assert exc.value.status_code == 422, exc.value.detail
    return exc.value.detail


def _metadata(plan) -> dict[str, Any]:
    return plan.create_kwargs["initial_metadata"]


def _origins(plan) -> dict[str, Any]:
    return plan.create_kwargs["datasource_selection_provenance"]["access"]


class TestLevels:
    @pytest.mark.asyncio
    async def test_an_explicit_level_is_stored_with_its_origin(self):
        plan = await _plan(_body(datasource_access={FOLDER: "read_only"}), _deps_with())
        assert _metadata(plan)["datasource_access"] == {FOLDER: "read_only"}
        assert _origins(plan)[FOLDER] == {"access": "read_only", "origin": "explicit"}
        assert plan.protected is False
        assert "protected_cloud" not in _metadata(plan)

    @pytest.mark.asyncio
    async def test_without_a_level_the_driver_default_is_stored(self):
        plan = await _plan(_body(), _deps_with())
        assert _metadata(plan)["datasource_access"] == {FOLDER: "read_write"}
        assert _origins(plan)[FOLDER]["origin"] == "driver_default"

    @pytest.mark.asyncio
    async def test_a_protected_link_default_makes_the_session_protected(self):
        row = {**_folder_row(), "project_access": "protected"}
        deps = _deps_with(
            [row], resolve_thread_execution_lane=MagicMock(return_value="stateless")
        )
        plan = await _plan(_body(), deps)
        assert plan.protected is True
        assert plan.execution_lane == "pinned"
        assert _metadata(plan)["protected_cloud"] is True
        assert _origins(plan)[FOLDER]["origin"] == "link_default"
        # An explicit level wins over the link's default.
        plan = await _plan(_body(datasource_access={FOLDER: "read_write"}), deps)
        assert plan.protected is False
        assert "protected_cloud" not in _metadata(plan)

    @pytest.mark.asyncio
    async def test_a_protected_default_is_lowered_where_the_switch_is_off(self):
        row = {**_folder_row(), "project_access": "protected"}
        deps = _deps_with(
            [row], is_protected_cloud_mode_enabled=MagicMock(return_value=False)
        )
        plan = await _plan(_body(), deps)
        assert plan.protected is False
        assert _metadata(plan)["datasource_access"] == {FOLDER: "read_only"}
        origin = _origins(plan)[FOLDER]
        assert origin["origin"] == "clamped"
        assert "lowered to 'read_only'" in origin["reason"]

    @pytest.mark.asyncio
    async def test_a_viewers_default_is_lowered_to_read_only(self):
        plan = await _plan(_body(), _deps_with(roles={PROJECT: "viewer"}))
        assert _metadata(plan)["datasource_access"] == {FOLDER: "read_only"}
        assert _origins(plan)[FOLDER]["origin"] == "clamped"

    @pytest.mark.asyncio
    async def test_an_empty_selection_stores_no_levels(self):
        plan = await _plan(ThreadCreateRequest(datasource_ids=[]), _deps_with([]))
        assert "datasource_access" not in _metadata(plan)
        assert plan.datasource_access == {}


class TestRefusals:
    @pytest.mark.asyncio
    async def test_a_level_off_the_tier_is_refused_never_changed(self):
        detail = await _refused(
            _body(
                datasource_access={FOLDER: "read_write"},
                config_override={"workspace": {"backend": "vm"}},
            ),
            _deps_with(),
        )
        assert detail["code"] == "connector_access_unavailable"
        assert (detail["datasource_id"], detail["access"]) == (FOLDER, "read_write")
        assert detail["reason"] == "level_not_offered"

    @pytest.mark.asyncio
    async def test_a_level_above_the_viewers_ceiling_is_refused(self):
        detail = await _refused(
            _body(datasource_access={FOLDER: "read_write"}),
            _deps_with(roles={PROJECT: "viewer"}),
        )
        assert detail["code"] == "connector_access_unavailable"
        assert detail["reason"] == "above_ceiling"

    @pytest.mark.asyncio
    async def test_protected_needs_the_container_tier(self):
        detail = await _refused(
            _body(
                datasource_access={FOLDER: "protected"},
                config_override={"workspace": {"backend": "virtual"}},
            ),
            _deps_with(),
        )
        assert detail["code"] == "protected_cloud_unsupported_workspace"

    @pytest.mark.asyncio
    async def test_protected_is_never_an_officers(self):
        body = _body(
            datasource_access={FOLDER: "protected"},
            config_override={"officer": {"enabled": True}},
        )
        body._officer_post_config_snapshot = {}
        detail = await _refused(body, _deps_with())
        assert detail["code"] == "protected_cloud_unsupported_session_class"

    @pytest.mark.asyncio
    async def test_protected_with_the_switch_off_is_refused(self):
        detail = await _refused(
            _body(datasource_access={FOLDER: "protected"}),
            _deps_with(is_protected_cloud_mode_enabled=MagicMock(return_value=False)),
        )
        assert detail["code"] == "connector_access_unavailable"

    @pytest.mark.asyncio
    async def test_a_level_for_a_connector_not_selected_is_refused(self):
        detail = await _refused(
            _body(datasource_access={OTHER_FOLDER: "read_only"}), _deps_with()
        )
        assert detail["code"] == "connector_access_not_selected"
        detail = await _refused(
            _body(datasource_ids=[], datasource_access={FOLDER: "read_only"}),
            _deps_with([]),
        )
        assert detail["code"] == "connector_access_not_selected"

    @pytest.mark.asyncio
    async def test_the_access_map_takes_connector_ids_and_level_ids(self):
        with pytest.raises(ValueError):
            ThreadCreateRequest(datasource_access={"not-a-uuid": "read_only"})
        with pytest.raises(ValueError):
            ThreadCreateRequest(datasource_access={FOLDER: "read only"})
        upper = ThreadCreateRequest(datasource_access={FOLDER.upper(): "read_only"})
        assert upper.datasource_access == {FOLDER: "read_only"}


class TestTheProtectedAlias:
    @pytest.mark.asyncio
    async def test_it_protects_the_projects_own_folder(self):
        plan = await _plan(_body(protected_cloud=True), _deps_with())
        assert plan.protected is True
        assert _metadata(plan)["protected_cloud"] is True
        assert _metadata(plan)["datasource_access"] == {FOLDER: "protected"}
        assert plan.execution_lane == "pinned"

    @pytest.mark.asyncio
    async def test_it_is_refused_without_that_folder_never_ignored(self):
        detail = await _refused(
            _body(protected_cloud=True, datasource_ids=[]), _deps_with([])
        )
        assert detail["code"] == "protected_cloud_requires_folder"
        detail = await _refused(
            ThreadCreateRequest(protected_cloud=True, datasource_ids=[FOLDER]),
            _deps_with(),
        )
        assert detail["code"] == "protected_cloud_requires_folder"

    @pytest.mark.asyncio
    async def test_it_is_refused_when_the_access_map_disagrees(self):
        detail = await _refused(
            _body(protected_cloud=True, datasource_access={FOLDER: "read_write"}),
            _deps_with(),
        )
        assert detail["code"] == "protected_cloud_conflict"
        plan = await _plan(
            _body(protected_cloud=True, datasource_access={FOLDER: "protected"}),
            _deps_with(),
        )
        assert plan.protected is True


class TestExecutionRefs:
    @pytest.mark.asyncio
    async def test_a_refs_level_is_a_sessions_choice(self, monkeypatch):
        resolver = AsyncMock(return_value=[FOLDER])
        monkeypatch.setattr(ta, "resolve_execution_connectors", resolver)
        body = ThreadCreateRequest(
            project_id=PROJECT,
            execution={
                "connectors": {
                    "folder": {"ref": {"uid": FOLDER}, "access": "read_only"}
                }
            },
        )
        plan = await _plan(body, _deps_with())
        assert _metadata(plan)["datasource_access"] == {FOLDER: "read_only"}
        assert resolver.await_args.kwargs["accepts_access"] is True

    @pytest.mark.asyncio
    async def test_a_ref_and_the_access_map_must_agree(self, monkeypatch):
        monkeypatch.setattr(
            ta, "resolve_execution_connectors", AsyncMock(return_value=[FOLDER])
        )
        body = ThreadCreateRequest(
            project_id=PROJECT,
            datasource_access={FOLDER: "read_write"},
            execution={
                "connectors": {
                    "folder": {"ref": {"uid": FOLDER}, "access": "read_only"}
                }
            },
        )
        detail = await _refused(body, _deps_with())
        assert detail["code"] == "connector_access_conflict"

    @pytest.mark.asyncio
    async def test_a_job_refuses_a_level_rather_than_ignore_it(self):
        from orchestrator.schemas.execution_selection import ExecutionSelection

        execution = ExecutionSelection(
            connectors={"folder": {"ref": {"uid": FOLDER}, "access": "read_only"}}
        )
        with pytest.raises(HTTPException) as exc:
            await resolve_execution_connectors(
                SimpleNamespace(), execution, owner_id=USER["id"], project_id=None
            )
        assert exc.value.status_code == 422
        assert exc.value.detail["code"] == "connector_access_unsupported"


class TestCommit:
    @pytest.mark.asyncio
    async def test_the_engage_follows_the_folders_level(self):
        deps = _deps_with()
        body = _body(datasource_access={FOLDER: "protected"})
        plan = await _plan(body, deps)
        await ta.commit_thread_creation(plan, body, USER, dependencies=deps)
        deps.schedule_protected_engage.assert_called_once()
        deps = _deps_with()
        body = _body(datasource_access={FOLDER: "read_write"})
        plan = await _plan(body, deps)
        await ta.commit_thread_creation(plan, body, USER, dependencies=deps)
        deps.schedule_protected_engage.assert_not_called()


class TestLiveEdits:
    """A live edit keeps the recorded levels, resolves an added connector's
    default, and never sets, changes or detaches protected."""

    @staticmethod
    async def _live(metadata, selection, requested=None, rows=None):
        from orchestrator.services.thread_config_update import _live_selection_access

        thread = {
            "id": "t",
            "user_id": USER["id"],
            "metadata": metadata,
            "config_override": {"workspace": {"backend": "sandbox"}},
        }
        store = SimpleNamespace(
            resolve_datasources_for_thread=AsyncMock(
                return_value=rows if rows is not None else [_folder_row()]
            ),
            get_user_project_roles=AsyncMock(return_value={}),
            get_datasource=AsyncMock(return_value=None),
        )
        return await _live_selection_access(
            thread,
            selection,
            [PROJECT],
            requested=requested,
            dependencies=SimpleNamespace(store=store),
        )

    @pytest.mark.asyncio
    async def test_a_session_without_levels_stays_without(self):
        assert await self._live({}, [FOLDER]) == (None, {})

    @pytest.mark.asyncio
    async def test_a_recorded_level_is_kept_and_a_change_is_stored(self):
        levels, _ = await self._live(
            {"datasource_access": {FOLDER: "read_only"}}, [FOLDER]
        )
        assert levels == {FOLDER: "read_only"}
        levels, origins = await self._live(
            {"datasource_access": {FOLDER: "read_only"}},
            [FOLDER],
            requested={FOLDER: "read_write"},
        )
        assert levels == {FOLDER: "read_write"}
        assert origins[FOLDER]["origin"] == "explicit"

    @pytest.mark.asyncio
    async def test_an_added_folder_gets_its_default_never_protected(self):
        row = {**_foreign_row(), "project_access": "protected"}
        levels, _ = await self._live(
            {"datasource_access": {FOLDER: "read_only"}},
            [FOLDER, OTHER_FOLDER],
            rows=[_folder_row(), row],
        )
        assert levels[FOLDER] == "read_only"
        assert levels[OTHER_FOLDER] == "read_only"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("stored", "selection", "requested"),
        [
            ({FOLDER: "protected"}, [], None),
            ({FOLDER: "protected"}, [FOLDER], {FOLDER: "read_write"}),
            ({FOLDER: "read_only"}, [FOLDER], {FOLDER: "protected"}),
        ],
    )
    async def test_protected_is_fixed_for_the_sessions_life(
        self, stored, selection, requested
    ):
        metadata = {"datasource_access": stored}
        if "protected" in stored.values():
            metadata["protected_cloud"] = True
        with pytest.raises(HTTPException) as exc:
            await self._live(metadata, selection, requested)
        assert exc.value.status_code == 409
        assert exc.value.detail["code"] == "protected_cloud_fixed"

    @pytest.mark.asyncio
    async def test_an_older_protected_session_refuses_a_level(self):
        with pytest.raises(HTTPException) as exc:
            await self._live({"protected_cloud": True}, [FOLDER], {FOLDER: "read_only"})
        assert exc.value.detail["code"] == "protected_cloud_fixed"
