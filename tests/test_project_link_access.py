"""A project link's default access level (main cloud slice 5, 5.2).

A connector whose driver has strict levels (a cloud folder) takes a default
level per project link (``project_datasources.access``) instead of the
``read_only`` boolean; any other driver keeps the boolean and takes no
level. The level must be one the driver offers the connector on some tier.
Only the project's owner sets it (the router's gate); natives can be set
too, only their delete, unlink and re-point are locked.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.schemas.datasources import ProjectDatasourceSettings
from orchestrator.services.cloud.nextcloud import NEXTCLOUD_CAPABILITIES
from orchestrator.services.projects import (
    link_datasource_to_project,
    update_project_datasource,
)
from shared.connectors.platform import project_cloud_key

PROJECT = str(uuid4())
OWNER = {"id": str(uuid4()), "is_admin": False}


def _folder(backend: str = "nextcloud", **changes) -> dict:
    row = {
        "id": str(uuid4()),
        "name": "Atlas cloud folder",
        "type": "cloud_folder",
        "created_by": OWNER["id"],
        "is_global": False,
        "read_only": None,
        "scope_mode": "projects",
        "managed_key": project_cloud_key(PROJECT),
        "config": {
            "folder_kind": "project",
            "project_id": PROJECT,
            "backend_id": backend,
            "backend_instance_id": str(uuid4()),
            "folder_handle": f"{backend}:41",
        },
    }
    row.update(changes)
    return row


def _postgres() -> dict:
    return {
        "id": str(uuid4()),
        "name": "Warehouse",
        "type": "postgresql",
        "created_by": OWNER["id"],
        "is_global": False,
        "read_only": None,
        "scope_mode": "projects",
        "config": {},
    }


def _deps(row: dict) -> SimpleNamespace:
    return SimpleNamespace(
        store=SimpleNamespace(
            get_datasource=AsyncMock(return_value=row),
            update_project_datasource=AsyncMock(return_value=True),
            link_datasource_to_project=AsyncMock(return_value=True),
            list_datasource_projects=AsyncMock(return_value=[PROJECT]),
        )
    )


async def _patch(row: dict, **body):
    deps = _deps(row)
    result = await update_project_datasource(
        PROJECT,
        str(row["id"]),
        ProjectDatasourceSettings(**body),
        user=OWNER,
        dependencies=deps,
    )
    return result, deps.store.update_project_datasource


class TestStrictLinks:
    @pytest.mark.asyncio
    async def test_a_level_is_stored_and_read_only_kept_null(self):
        result, update = await _patch(_folder(), access="protected")
        assert result == {"status": "updated"}
        kwargs = update.await_args.kwargs
        assert kwargs["access"] == "protected"
        assert kwargs["read_only"] is None

    @pytest.mark.asyncio
    async def test_null_clears_the_level_and_omission_keeps_it(self):
        _result, update = await _patch(_folder(), access=None)
        assert update.await_args.kwargs["access"] is None
        _result, update = await _patch(_folder(), description="Shared drafts")
        assert update.await_args.kwargs["access"] is ...
        assert update.await_args.kwargs["description"] == "Shared drafts"

    @pytest.mark.asyncio
    async def test_read_only_is_refused_for_a_level(self):
        with pytest.raises(HTTPException) as refused:
            await _patch(_folder(), read_only=True)
        assert refused.value.status_code == 422
        assert refused.value.detail["code"] == "link_access_is_a_level"
        # Turning it off asks for nothing: the link has no boolean.
        _result, update = await _patch(_folder(), read_only=False)
        assert update.await_args.kwargs["read_only"] is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("level", ["admin", "ReadWrite", "reviewed_write_back"])
    async def test_a_level_the_driver_does_not_have_is_refused(self, level):
        with pytest.raises(HTTPException) as refused:
            await _patch(_folder(), access=level)
        assert refused.value.status_code == 422
        detail = refused.value.detail
        assert detail["code"] == "link_access_not_offered"
        assert detail["access"] == level
        assert "read_write" in detail["offered"]

    @pytest.mark.asyncio
    async def test_the_offered_set_is_the_providers_on_any_tier(self):
        # OpenCloud offers neither read_only nor protected for a project
        # folder; Nextcloud offers what its matrix says on some tier.
        with pytest.raises(HTTPException) as refused:
            await _patch(_folder("opencloud"), access="protected")
        assert refused.value.detail["offered"] == ["read_write"]
        for level in ("read_only", "protected", "read_write"):
            offered = NEXTCLOUD_CAPABILITIES.offers("cloud_folder", "project", level)
            if offered:
                _result, update = await _patch(_folder(), access=level)
                assert update.await_args.kwargs["access"] == level
            else:
                with pytest.raises(HTTPException):
                    await _patch(_folder(), access=level)

    @pytest.mark.asyncio
    async def test_a_native_link_is_never_added_elsewhere(self):
        deps = _deps(_folder())
        with pytest.raises(HTTPException) as refused:
            await link_datasource_to_project(
                str(uuid4()),
                str(uuid4()),
                ProjectDatasourceSettings(access="protected"),
                user={**OWNER, "is_admin": True},
                dependencies=deps,
            )
        assert refused.value.status_code == 409
        deps.store.link_datasource_to_project.assert_not_awaited()


class TestOtherLinks:
    @pytest.mark.asyncio
    async def test_a_level_is_refused(self):
        with pytest.raises(HTTPException) as refused:
            await _patch(_postgres(), access="read_only")
        assert refused.value.status_code == 422
        assert refused.value.detail["code"] == "link_access_not_a_level"

    @pytest.mark.asyncio
    async def test_read_only_keeps_working_and_the_level_is_left_alone(self):
        _result, update = await _patch(_postgres(), read_only=True)
        kwargs = update.await_args.kwargs
        assert kwargs["read_only"] is True
        assert kwargs["access"] is ...

    @pytest.mark.asyncio
    async def test_linking_stores_no_level(self):
        row = _postgres()
        deps = _deps(row)
        await link_datasource_to_project(
            PROJECT,
            str(row["id"]),
            ProjectDatasourceSettings(read_only=True),
            user=OWNER,
            dependencies=deps,
        )
        kwargs = deps.store.link_datasource_to_project.await_args.kwargs
        assert kwargs["read_only"] is True
        assert kwargs["access"] is None


@pytest.mark.asyncio
async def test_only_the_project_owner_reaches_the_service():
    from orchestrator.routers.projects import (
        update_project_datasource as patch_route,
    )

    row = _folder()
    deps = _deps(row)
    route_deps = SimpleNamespace(
        store=deps.store,
        operations=deps,
        require_project_owner=AsyncMock(
            side_effect=HTTPException(status_code=403, detail="Project owner required")
        ),
    )
    with pytest.raises(HTTPException) as refused:
        await patch_route(
            SimpleNamespace(),
            PROJECT,
            str(row["id"]),
            ProjectDatasourceSettings(access="protected"),
            dependencies=route_deps,
        )
    assert refused.value.status_code == 403
    deps.store.update_project_datasource.assert_not_awaited()


def test_the_level_field_takes_a_level_id_only():
    with pytest.raises(ValueError):
        ProjectDatasourceSettings(access="read only")
    assert ProjectDatasourceSettings(access="read_only").access == "read_only"
