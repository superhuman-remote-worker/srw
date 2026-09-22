"""A request cannot pin an expert only its owner's unrestricted admin flag sees.

Automations and bench runs persist a DB expert id that every later fire runs
as the owner. The request that stores it validates visibility — and used to
re-read the owner's admin flag from the database, so an admin-owned personal
access token *without* the ``admin`` scope (whose resolver drops ``is_admin``
for the request) could still pin another user's private expert. ``POST
/api/jobs`` already validates against the narrowed caller; these pin the same
bound on the three request-time validators. The deferred fire paths keep
running as the owner.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from orchestrator.routers import automations as automations_router
from orchestrator.routers import bench as bench_router
from orchestrator.services.bench import BenchStore

OWNER_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
PRIVATE_EXPERT = "dddddddd-dddd-dddd-dddd-dddddddddddd"


def _store() -> MagicMock:
    """An admin owner, and an expert only the admin bypass can see."""
    db = MagicMock()
    db.get_user = AsyncMock(return_value={"id": OWNER_ID, "is_admin": True})

    async def visible_by_id(expert_id, *, is_admin=False, **_):
        if is_admin:
            return {"id": expert_id, "expert_type": "worker", "owner_id": "other"}
        return None

    db.get_expert_visible_by_id = AsyncMock(side_effect=visible_by_id)
    db.get_project_expert_link = AsyncMock(return_value=None)
    db.create_automation = AsyncMock(return_value={"id": "a1"})
    db.update_automation = AsyncMock(return_value={"id": "a1"})
    return db


def _narrowed_caller() -> dict:
    # What _resolve_pat hands a route for an admin's jobs:write token.
    return {"id": OWNER_ID, "is_admin": False, "scopes": ["jobs:write"]}


@pytest.fixture(autouse=True)
def _experts_db(monkeypatch):
    monkeypatch.setenv("EXPERTS_DB_ENABLED", "true")


def _automation_deps(db):
    return automations_router.AutomationsDependencies(
        store=db,
        gitea_client=MagicMock(),
        main_cloud_router=MagicMock(),
        trigger_dispatch=MagicMock(),
    )


def _visibility_flags(db) -> list[bool]:
    return [
        call.kwargs.get("is_admin")
        for call in db.get_expert_visible_by_id.await_args_list
    ]


@pytest.mark.asyncio
async def test_create_automation_pins_only_what_the_caller_may_see(monkeypatch):
    db = _store()
    monkeypatch.setattr(
        automations_router,
        "require_approved_user",
        AsyncMock(return_value=_narrowed_caller()),
    )
    body = automations_router.AutomationCreate(
        name="nightly",
        cron_expr="0 * * * *",
        expert="worker_base",
        expert_id=PRIVATE_EXPERT,
        prompt="p",
    )

    with pytest.raises(HTTPException) as exc:
        await automations_router.create_automation(
            MagicMock(), body, dependencies=_automation_deps(db)
        )

    assert exc.value.status_code == 422
    assert _visibility_flags(db) == [False]
    db.create_automation.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_automation_pins_only_what_the_caller_may_see(monkeypatch):
    db = _store()
    monkeypatch.setattr(
        automations_router,
        "require_approved_user",
        AsyncMock(return_value=_narrowed_caller()),
    )
    monkeypatch.setattr(
        automations_router,
        "_resolve_automation_or_404",
        AsyncMock(
            return_value={
                "id": "a1",
                "owner_id": OWNER_ID,
                "project_id": None,
                "expert": "worker_base",
                "expert_id": None,
                "enabled": True,
                "cron_expr": "0 * * * *",
                "timezone": "UTC",
            }
        ),
    )

    with pytest.raises(HTTPException) as exc:
        await automations_router.update_automation(
            MagicMock(),
            "a1",
            automations_router.AutomationUpdate(expert_id=PRIVATE_EXPERT),
            dependencies=_automation_deps(db),
        )

    assert exc.value.status_code == 422
    assert _visibility_flags(db) == [False]
    db.update_automation.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_bench_run_pins_only_what_the_caller_may_see(monkeypatch):
    db = _store()
    monkeypatch.setattr(
        bench_router,
        "require_approved_user",
        AsyncMock(return_value={**_narrowed_caller(), "default_project_id": None}),
    )
    create = AsyncMock(return_value={"id": "r1"})
    monkeypatch.setattr(bench_router, "create_bench_run", create)
    dependencies = bench_router.BenchDependencies(
        store=BenchStore(db),
        create_job=AsyncMock(),
        validate_tool_overrides=lambda value: value,
    )
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/bench/runs",
            "headers": [],
            "app": SimpleNamespace(
                state=SimpleNamespace(bench_dependencies_factory=lambda: dependencies)
            ),
        }
    )
    body = bench_router.BenchRunCreate(
        name="paired",
        tasks=[{"id": "t1", "description": "task"}],
        replicates=1,
        max_in_flight=1,
        arms=[{"name": "pinned", "model": "m", "expert_id": PRIVATE_EXPERT}],
    )

    with pytest.raises(HTTPException) as exc:
        await bench_router.create_run(request, body)

    assert exc.value.status_code == 422
    assert _visibility_flags(db) == [False]
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_unrestricted_admin_caller_still_sees_everything(monkeypatch):
    db = _store()
    monkeypatch.setattr(
        automations_router,
        "require_approved_user",
        AsyncMock(return_value={"id": OWNER_ID, "is_admin": True}),
    )
    body = automations_router.AutomationCreate(
        name="nightly",
        cron_expr="0 * * * *",
        expert="worker_base",
        expert_id=PRIVATE_EXPERT,
        prompt="p",
    )

    await automations_router.create_automation(
        MagicMock(), body, dependencies=_automation_deps(db)
    )

    assert _visibility_flags(db) == [True]
    db.create_automation.assert_awaited_once()
