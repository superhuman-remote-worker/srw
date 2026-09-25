"""``GET /api/jobs/{job_id}/subjobs`` — the roster that explains a parent's status.

The bug this endpoint exists to fix is a *reading* bug, not a data bug. A parent
parked in ``waiting`` is by definition blocked on a child — ``_spawn_scholar_job``
holds it there while the scholar runs — but the jobs list pages over display
roots and rides a child along only when the child *also* matches the caller's
filter. The default filter is ``origin IN ('user','session')`` and every subjob
is stamped ``origin='subjob'``, so on the k3d cluster all 33 children were
excluded from every page: the parent rendered as a stalled row with no children
whatsoever, while its scholar was running normally one level down.

So the property under test is **filter independence**. This endpoint must never
grow a filter parameter and must never share the list's matched-set: the roster
of a job is a property of the job, not of the view someone is looking at it
through. `TestFilterIndependence` pins that as a signature contract, because it
is the kind of thing a later "just reuse query_jobs" refactor would quietly undo.
"""

from __future__ import annotations

import inspect
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

import orchestrator.main as m  # noqa: E402
from orchestrator.routers import job_inspection as job_inspection_routes
from orchestrator.application import jobs as jobs_composition
from orchestrator.security import access as access_module
import fastapi as fastapi_module

UTC = timezone.utc
NOW = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)


def _child(
    *,
    job_id: str | None = None,
    parent_job_id: str | None = None,
    depth: int = 0,
    status: str = "processing",
    config_name: str | None = "scholar",
    description: str = "Research phase for: build a calculator",
    origin: str = "subjob",
    error_message: str | None = None,
    completed_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "id": uuid.UUID(job_id) if job_id else uuid.uuid4(),
        "parent_job_id": uuid.UUID(parent_job_id) if parent_job_id else None,
        "depth": depth,
        "description": description,
        "status": status,
        "config_name": config_name,
        "origin": origin,
        "error_message": error_message,
        "created_at": NOW - timedelta(hours=1),
        "completed_at": completed_at,
        "updated_at": NOW,
    }


@pytest.fixture
def route_env(monkeypatch):
    job_id = str(uuid.uuid4())
    job = {"id": job_id, "status": "waiting", "created_at": NOW - timedelta(hours=2)}
    db = SimpleNamespace(get_job_subjob_roster=AsyncMock(return_value=[]))
    guard = AsyncMock(return_value=({"id": "u"}, job))
    monkeypatch.setattr(m.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(access_module, "require_job_access", guard)

    async def call():
        return await job_inspection_routes.get_job_subjobs(
            SimpleNamespace(),
            job_id,
            dependencies=jobs_composition.job_inspection_dependencies(
                m.app.state.resources
            ),
        )

    return SimpleNamespace(job_id=job_id, job=job, db=db, guard=guard, call=call)


class TestFilterIndependence:
    def test_the_route_takes_no_filter_parameters(self):
        """The roster is a property of the job, not of the caller's view.

        Pinned on the signature rather than on behaviour because the failure
        mode is a future refactor *adding* a parameter — an `origin=` or a
        `status=` here would reintroduce exactly the blind spot the endpoint was
        built to remove, and no behavioural test would notice a parameter that
        merely exists and defaults to "everything".
        """
        params = set(
            inspect.signature(job_inspection_routes.get_job_subjobs).parameters
        )
        assert params == {"request", "job_id", "dependencies"}

    @pytest.mark.asyncio
    async def test_it_does_not_go_through_the_list_query(self, route_env):
        """`query_jobs` is the thing that cannot answer this question."""
        assert not hasattr(route_env.db, "query_jobs")
        await route_env.call()
        route_env.db.get_job_subjob_roster.assert_awaited_once_with(route_env.job_id)


class TestAuthorization:
    @pytest.mark.asyncio
    async def test_the_parent_is_the_gate(self, route_env):
        """A subjob inherits its parent's project and owner at creation.

        Verified against the cluster before relying on it: all 33 parent/child
        pairs on k3d share both `project_id` and `user_id`. Seeing the parent is
        therefore seeing the family, and every field returned is one the list
        already publishes for a child the filter happened to let through.
        """
        await route_env.call()
        route_env.guard.assert_awaited_once()
        assert route_env.guard.await_args.args[2] == route_env.job_id

    @pytest.mark.asyncio
    async def test_a_denied_caller_never_reaches_the_walk(self, route_env, monkeypatch):
        boom = AsyncMock(
            side_effect=fastapi_module.HTTPException(status_code=403, detail="no")
        )
        monkeypatch.setattr(access_module, "require_job_access", boom)
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await route_env.call()
        assert excinfo.value.status_code == 403
        route_env.db.get_job_subjob_roster.assert_not_awaited()


class TestPayload:
    @pytest.mark.asyncio
    async def test_a_childless_job_answers_zero_rather_than_erroring(self, route_env):
        """An empty roster is a positive answer, not a failure.

        The panel renders nothing either way, but the caller must be able to
        tell "this job spawned nothing" from "the roster failed to load" — the
        cockpit keys those to different states.
        """
        payload = await route_env.call()
        assert payload == {
            "job_id": route_env.job_id,
            "count": 0,
            "subjobs": [],
        }

    @pytest.mark.asyncio
    async def test_ids_are_serialized_as_strings(self, route_env):
        """asyncpg hands back `UUID` objects; the wire contract is strings.

        Not cosmetic: the cockpit matches roster ids against list-row ids with
        `===`, and a UUID that JSON-encodes differently would silently never
        match, so "open this subjob" would do nothing on rows that are present.
        """
        parent = route_env.job_id
        child_id = str(uuid.uuid4())
        route_env.db.get_job_subjob_roster.return_value = [
            _child(job_id=child_id, parent_job_id=parent)
        ]
        payload = await route_env.call()
        row = payload["subjobs"][0]
        assert row["id"] == child_id
        assert isinstance(row["id"], str)
        assert row["parent_job_id"] == parent
        assert isinstance(row["parent_job_id"], str)

    @pytest.mark.asyncio
    async def test_a_null_parent_stays_null_rather_than_becoming_none_the_string(
        self, route_env
    ):
        route_env.db.get_job_subjob_roster.return_value = [_child(parent_job_id=None)]
        payload = await route_env.call()
        assert payload["subjobs"][0]["parent_job_id"] is None

    @pytest.mark.asyncio
    async def test_count_matches_the_rows(self, route_env):
        route_env.db.get_job_subjob_roster.return_value = [
            _child(depth=0),
            _child(depth=0, config_name="critic"),
            _child(depth=1, config_name="critic"),
        ]
        payload = await route_env.call()
        assert payload["count"] == 3
        assert len(payload["subjobs"]) == 3

    @pytest.mark.asyncio
    async def test_the_role_label_survives(self, route_env):
        """`config_name` is what makes a roster row readable at a glance.

        'scholar' and 'critic' are the whole reason a reader can tell why the
        parent is parked; an id and a status alone would not explain anything.
        """
        route_env.db.get_job_subjob_roster.return_value = [
            _child(config_name="scholar")
        ]
        payload = await route_env.call()
        assert payload["subjobs"][0]["config_name"] == "scholar"

    @pytest.mark.asyncio
    async def test_terminal_children_are_not_dropped(self, route_env):
        """A finished critic is exactly what explains why its parent moved on."""
        route_env.db.get_job_subjob_roster.return_value = [
            _child(status="completed", completed_at=NOW),
            _child(status="failed", error_message="boom"),
        ]
        payload = await route_env.call()
        assert [r["status"] for r in payload["subjobs"]] == ["completed", "failed"]
        assert payload["subjobs"][1]["error_message"] == "boom"

    @pytest.mark.asyncio
    async def test_depth_is_published_so_the_client_need_not_rebuild_the_tree(
        self, route_env
    ):
        route_env.db.get_job_subjob_roster.return_value = [
            _child(depth=0),
            _child(depth=1),
        ]
        payload = await route_env.call()
        assert [r["depth"] for r in payload["subjobs"]] == [0, 1]


class TestFailure:
    @pytest.mark.asyncio
    async def test_a_broken_walk_is_a_500_not_a_silent_empty_roster(self, route_env):
        """An empty roster means "no children" everywhere else in this feature.

        Returning one on a database error would make a `waiting` parent look
        childless again — the precise misreading the endpoint exists to prevent,
        reintroduced through the error path.
        """
        route_env.db.get_job_subjob_roster.side_effect = RuntimeError("connection lost")
        with pytest.raises(fastapi_module.HTTPException) as excinfo:
            await route_env.call()
        assert excinfo.value.status_code == 500
