"""A project delete over a real store, with every external service stubbed.

The manifest API deletes a linked Project through the project delete
(``ManifestResourceService(delete_project=...)``); a service built without
that hook answers 503. Tests that delete Projects build it with
:func:`delete_project_hook`.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

from orchestrator.services import projects


def stub_project_dependencies(store: Any, **overrides: Any) -> Any:
    """``ProjectDependencies`` over ``store``; no forge, Keycloak, cloud,
    vector DB or graph unless ``overrides`` supplies one."""

    @asynccontextmanager
    async def vector_acquire():
        async def execute(*_args):
            return "DELETE 0"

        yield SimpleNamespace(execute=execute)

    values = dict(
        store=store,
        vector_db=SimpleNamespace(acquire=vector_acquire),
        forge=SimpleNamespace(is_initialized=False, repository_owner="srw-admin"),
        keycloak_groups=SimpleNamespace(is_configured=False, is_initialized=False),
        main_cloud_router=SimpleNamespace(
            for_project=MagicMock(side_effect=AssertionError("no cloud folder")),
            for_project_optional=MagicMock(return_value=None),
        ),
        logger=MagicMock(),
        provisioning=MagicMock(),
        graph=SimpleNamespace(get=lambda: None),
        with_validated_tool_overrides=lambda value: value,
    )
    values.update(overrides)
    return projects.ProjectDependencies(**values)


def delete_project_hook(store: Any, **overrides: Any):
    """The ``delete_project`` hook ``catalogue.manifest_dependencies`` binds,
    over :func:`stub_project_dependencies`."""

    async def delete_project(project_id, *, user, expected_resource):
        from fastapi import HTTPException

        project = await store.get_project(project_id)
        if not project:
            raise HTTPException(404, "Resource does not exist.")
        return await projects.delete_project(
            project_id,
            project=project,
            dependencies=stub_project_dependencies(store, **overrides),
            requested_by=str(user["id"]),
            expected_resource=expected_resource,
        )

    return delete_project


def fake_deletion_store(
    *, delete_project: Any, begin: Any = None, repositories: Any = (), **extra: Any
) -> Any:
    """A store double with the project deletion interface and nothing else.

    ``delete_project`` is the row delete; ``begin`` (optional) runs inside
    ``begin_project_deletion``, e.g. to refuse it. The deletion record is
    kept in memory and follows the row as the real store's does.
    """
    from unittest.mock import AsyncMock

    from orchestrator.services import project_deletion

    state: dict[str, Any] = {
        "row_deleted_at": None,
        "pending_steps": [],
        "targets": {},
        "last_error": None,
    }

    async def begin_project_deletion(project_id, **_kwargs):
        if begin is not None:
            await begin(project_id)
        return dict(state)

    async def get_project_deletion(_project_id):
        return dict(state)

    async def claim_project_deletion(_project_id):
        return None

    async def settle_project_deletion(_project_id, *, pending_steps, error):
        state["last_error"] = error
        if pending_steps is not None:
            state["pending_steps"] = list(pending_steps)

    async def delete(project_id):
        deleted = await delete_project(project_id)
        if deleted:
            state["row_deleted_at"] = "now"
            state["pending_steps"] = list(project_deletion.STEPS_AFTER_ROW)
        return deleted

    @asynccontextmanager
    async def acquire():
        yield SimpleNamespace(execute=AsyncMock())

    return SimpleNamespace(
        begin_project_deletion=begin_project_deletion,
        get_project_deletion=get_project_deletion,
        claim_project_deletion=claim_project_deletion,
        settle_project_deletion=settle_project_deletion,
        get_project_repositories=AsyncMock(return_value=list(repositories)),
        delete_project=delete,
        acquire=acquire,
        deletion=state,
        **extra,
    )
