"""``POST /api/projects`` on an installation with no cloud, and no forge.

R1.B01 recorded that a minimal profile — no Gitea, no main cloud — answered
``POST /api/projects`` with a **500 after the project and owner-membership rows
were already written**, and explicitly refused to count that as a passing
project-create gate. R1.B03 owns the endpoint, so it owned the defect.

The failure was never the forge. With Gitea disabled the endpoint skips
knowledge-repo provisioning through a guard that is already there
(``forge.is_initialized``);
``test_disabled_forge_alone_does_not_fail_project_create``
isolates that and is the control for everything else here. The 500 came from
the *cloud* step: ``ensure_project_cloud_resources`` called
``main_cloud_router.for_owner()`` outside its own ``try``, and that raises
``FeatureNotAvailable`` when no active backend instance has been bound — while
every other remote effect in the helper is wrapped and logs, and the sibling
``for_project_optional`` branch returns the project unchanged.

**Intended degraded behaviour**, now implemented: a cloudless installation
creates the project and simply gets no cloud resources. That is what the
optional tier means everywhere else in this codebase, and it is the only
outcome that does not leave a row the caller was told did not exist. The
refusal inside ``for_owner`` is *kept* — guessing an installation is the
failure that refusal exists to prevent; only the caller's handling changed.

A real failure must still be a real failure, so
``test_a_genuine_store_failure_still_surfaces`` pins that the blanket 500 has
not been turned into a blanket swallow.

Re-pointed by R1.B03 lane P at the extracted owner
(``orchestrator.services.projects.create_project`` over
``orchestrator.services.project_provisioning``), which is where the endpoint's
body now lives. Every assertion is the one written against ``orchestrator.main``
and means the same thing: the collaborators that used to be patched module
globals are now injected values, so the fakes are handed in rather than
monkeypatched, and the caller's identity is supplied directly instead of
through the gate the router owns.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.schemas.projects import ProjectCreate
from orchestrator.services import project_provisioning, projects
from orchestrator.services.cloud.errors import FeatureNotAvailable


OWNER_ID = str(uuid4())
PROJECT_ID = str(uuid4())


def _owner() -> dict[str, Any]:
    return {"id": OWNER_ID, "email": "owner@example.test", "is_admin": False}


def _store() -> MagicMock:
    """A store that records the writes ``create_project`` performs."""
    db = MagicMock()
    row = {
        "id": PROJECT_ID,
        "name": "Minimal profile project",
        "description": None,
        "goal": None,
        "is_default": False,
        "main_cloud_backend": None,
        "main_cloud_folder_handle": None,
        "nextcloud_folder_id": None,
    }
    db.create_project = AsyncMock(return_value=dict(row))
    db.add_project_member = AsyncMock(return_value={"role": "owner"})
    db.get_project = AsyncMock(return_value=dict(row))
    db.get_project_members = AsyncMock(return_value=[{"user_id": OWNER_ID}])
    db.get_user = AsyncMock(return_value=_owner())
    db.update_project = AsyncMock(return_value=dict(row))
    return db


def _uninitialised_cloud_router() -> MagicMock:
    """A router with no bound active backend instance.

    This is the exact production shape when no cloud is configured: the active
    backend is never bound, so ``for_owner`` refuses rather than guessing an
    installation. Refusing is correct; the caller's handling of the refusal is
    what this file is about.
    """
    router = MagicMock()
    router.for_owner.side_effect = FeatureNotAvailable(
        "durable active backend-instance authority", backend="nextcloud"
    )
    router.for_project_optional.return_value = None
    return router


def _with_validated_tool_overrides(value: Any) -> Any:
    """The application injects its own; this body sends no ``tools`` block."""
    return value


def _dependencies(db: MagicMock, cloud: MagicMock) -> projects.ProjectDependencies:
    """The minimal profile, wired as values instead of patched globals.

    The forge is off exactly as `values-e2e.yaml` ships it: `is_initialized` is
    False, so no knowledge repo is provisioned. Keycloak is off for the same
    reason.
    """
    forge = SimpleNamespace(is_initialized=False)
    keycloak = SimpleNamespace(is_initialized=False)
    return projects.ProjectDependencies(
        graph=MagicMock(get=MagicMock(return_value=None)),
        store=db,
        vector_db=MagicMock(),
        forge=forge,
        keycloak_groups=keycloak,
        main_cloud_router=cloud,
        logger=MagicMock(),
        provisioning=project_provisioning.ProjectProvisioningDependencies(
            store=db,
            forge=forge,
            keycloak_groups=keycloak,
            main_cloud_router=cloud,
            logger=MagicMock(),
            repair=project_provisioning.ProjectRepairState(),
            knowledge_index=MagicMock(),
        ),
        with_validated_tool_overrides=_with_validated_tool_overrides,
    )


def _body() -> ProjectCreate:
    return ProjectCreate(name="Minimal profile project", user_id=OWNER_ID)


@pytest.mark.asyncio
async def test_create_project_succeeds_without_cloud_and_leaves_no_orphan_rows() -> (
    None
):
    """The corrected behaviour: created, no cloud, nothing orphaned.

    Before the fix this raised 500 *after* both writes, so the caller was told
    the project did not exist while the project and its owner membership did.
    The two writes are separate statements, not one transaction, so nothing
    rolled them back — which is why degrading is the fix and a rollback is not.
    """
    db = _store()
    cloud = _uninitialised_cloud_router()

    project = await projects.create_project(
        _body(), user=_owner(), dependencies=_dependencies(db, cloud)
    )

    assert project["id"] == PROJECT_ID
    assert project["name"] == "Minimal profile project"
    # No cloud was configured, so no cloud state is claimed on the row.
    assert project["main_cloud_backend"] is None
    assert project["main_cloud_folder_handle"] is None

    # The rows the caller can now legitimately see.
    db.create_project.assert_awaited_once()
    db.add_project_member.assert_awaited_once()
    assert db.add_project_member.await_args.kwargs["role"] == "owner"
    assert db.add_project_member.await_args.kwargs["project_id"] == PROJECT_ID

    # The refusal itself is preserved: `for_owner` was asked and did refuse.
    # The fix is in how the caller handles it, not in weakening the router.
    cloud.for_owner.assert_called_once()
    # Nothing was written to the project row on the way out.
    db.update_project.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_router_refusal_is_not_weakened() -> None:
    """``for_owner`` still raises rather than guessing an installation.

    This is the half of the behaviour that must NOT change. Acting on a guessed
    installation is unrecoverable; refusing an effect is not.
    """
    from orchestrator.services.cloud import MainCloudRouter

    backend = MagicMock()
    backend.backend_id = "nextcloud"
    backend.backend_instance_id = None
    router = MainCloudRouter(backend)

    with pytest.raises(FeatureNotAvailable):
        router.for_owner()


@pytest.mark.asyncio
async def test_a_genuine_store_failure_still_surfaces() -> None:
    """Degrading the cloud step did not turn the handler into a blanket swallow."""
    db = _store()
    db.add_project_member = AsyncMock(side_effect=RuntimeError("membership write lost"))
    cloud = _uninitialised_cloud_router()

    with pytest.raises(HTTPException) as raised:
        await projects.create_project(
            _body(), user=_owner(), dependencies=_dependencies(db, cloud)
        )

    assert raised.value.status_code == 500
    assert "membership write lost" in str(raised.value.detail)


@pytest.mark.asyncio
async def test_a_configured_but_unreachable_cloud_still_degrades() -> None:
    """A bound-but-uninitialised backend was already tolerated; keep it that way.

    Distinct from the cloudless case: here an installation *is* resolvable, it
    just is not up. The folder branch is gated on ``backend.is_initialized`` and
    the member sync on the same flag, so the project is created either way.
    """
    db = _store()
    cloud = MagicMock()
    backend = MagicMock()
    backend.is_initialized = False
    cloud.for_owner.return_value = backend

    project = await projects.create_project(
        _body(), user=_owner(), dependencies=_dependencies(db, cloud)
    )

    assert project["id"] == PROJECT_ID
    db.update_project.assert_not_awaited()


@pytest.mark.asyncio
async def test_disabled_forge_alone_does_not_fail_project_create() -> None:
    """Isolate the forge: with a working cloud, a disabled Gitea is fine.

    This is the control for the test above. It is what makes "the 500 is the
    cloud step, not the forge" a measured claim rather than a reading of the
    source.
    """
    db = _store()
    cloud = MagicMock()
    backend = MagicMock()
    backend.is_initialized = False
    cloud.for_owner.return_value = backend
    cloud.for_project_optional.return_value = backend

    project = await projects.create_project(
        _body(), user=_owner(), dependencies=_dependencies(db, cloud)
    )

    assert project["id"] == PROJECT_ID
    db.create_project.assert_awaited_once()
    db.add_project_member.assert_awaited_once()
