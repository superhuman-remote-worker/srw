"""Live automations through the automations router (live_session_jobs.md §4.4).

The handlers are called directly with a fake store, as the other automation
boundary suites do. A live automation fires on an incoming session on its
primary connector (an alias in ``job_shape.execution.connectors`` that
resolves to a live connector), has no schedule, cannot be run now, and at
most one enabled live automation listens on one connector: a pre-check plus
the ``uq_automations_live_primary`` backstop, both a 409 naming the holder.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from orchestrator.routers import automations as mod
from orchestrator.services import connector_refs

OWNER_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
AUTOMATION_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
HOLDER_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
LINE_ID = "11111111-1111-4111-8111-111111111111"
OTHER_LINE_ID = "22222222-2222-4222-8222-222222222222"
GENERIC_ID = "33333333-3333-4333-8333-333333333333"

LINE_REF = {"ref": {"name": "reception"}}
OTHER_REF = {"ref": {"name": "night-line"}}
CONNECTORS = {"telefon": LINE_REF, "notes": {"ref": {"name": "notes"}}}
WORKSPACE = {"template": {"ref": {"name": "build-env"}}}

_DATASOURCES = {
    LINE_ID: {"id": LINE_ID, "type": "gpt_live_sip", "name": "reception"},
    OTHER_LINE_ID: {"id": OTHER_LINE_ID, "type": "gpt_live_sip", "name": "night"},
    GENERIC_ID: {"id": GENERIC_ID, "type": "generic", "name": "notes"},
}
_BY_REF = {"reception": LINE_ID, "night-line": OTHER_LINE_ID, "notes": GENERIC_ID}


def _store(*, holder: dict[str, Any] | None = None) -> MagicMock:
    db = MagicMock()
    db.create_automation = AsyncMock(return_value={"id": AUTOMATION_ID})
    db.update_automation = AsyncMock(return_value={"id": AUTOMATION_ID})
    db.live_automation_for_connector = AsyncMock(return_value=holder)

    async def get_datasource(datasource_id):
        return _DATASOURCES.get(str(datasource_id))

    db.get_datasource = AsyncMock(side_effect=get_datasource)
    return db


def _deps(db) -> mod.AutomationsDependencies:
    return mod.AutomationsDependencies(
        store=db,
        gitea_client=MagicMock(),
        main_cloud_router=MagicMock(),
        trigger_dispatch=MagicMock(),
    )


def _live_row(**overrides) -> dict[str, Any]:
    row = {
        "id": AUTOMATION_ID,
        "owner_id": OWNER_ID,
        "project_id": None,
        "name": "Reception",
        "trigger_type": "live",
        "trigger_connector_id": LINE_ID,
        "job_shape": {
            "execution": {"connectors": dict(CONNECTORS)},
            "primary_connector": "telefon",
        },
        "expert": "phone-desk",
        "expert_id": None,
        "enabled": False,
        "cron_expr": None,
        "timezone": "UTC",
    }
    row.update(overrides)
    return row


def _cron_row(**overrides) -> dict[str, Any]:
    row = {
        "id": AUTOMATION_ID,
        "owner_id": OWNER_ID,
        "project_id": None,
        "name": "Nightly",
        "trigger_type": "cron",
        "trigger_connector_id": None,
        "job_shape": {},
        "expert": "developer",
        "expert_id": None,
        "enabled": False,
        "cron_expr": "0 * * * *",
        "timezone": "UTC",
    }
    row.update(overrides)
    return row


@pytest.fixture
def env(monkeypatch):
    """The router's collaborators: an approved owner, an accepted expert, a
    connector resolver over the fixed refs, and a scheduler that must not be
    asked for a live automation."""
    caller = {"id": OWNER_ID, "is_admin": False}
    monkeypatch.setattr(mod, "require_approved_user", AsyncMock(return_value=caller))
    monkeypatch.setattr(
        mod,
        "validate_automation_expert_selection",
        AsyncMock(return_value="phone-desk"),
    )
    monkeypatch.setattr(mod, "require_project_member", AsyncMock())
    resolved: list[tuple[Any, dict[str, Any]]] = []

    async def resolve(db, selection, *, owner_id, project_id, **_):
        resolved.append((selection, {"owner_id": owner_id, "project_id": project_id}))
        return [
            _BY_REF[entry.ref.name]
            for entry in selection.connectors.values()
            if entry.ref is not None and entry.ref.name in _BY_REF
        ]

    monkeypatch.setattr(connector_refs, "resolve_execution_connectors", resolve)
    next_run = MagicMock(side_effect=AssertionError("no schedule for a live row"))
    monkeypatch.setattr(mod, "compute_initial_next_run", next_run)
    return {"resolved": resolved, "next_run": next_run}


def _row(monkeypatch, row: dict[str, Any]) -> None:
    monkeypatch.setattr(mod, "_resolve_automation_or_404", AsyncMock(return_value=row))


def _live_body(**overrides) -> mod.AutomationCreate:
    body = {
        "name": "Reception",
        "trigger_type": "live",
        "expert": "phone-desk",
        "prompt": "Answer the line",
        "primary_connector": "telefon",
        "job_shape": {"execution": {"connectors": dict(CONNECTORS)}},
    }
    body.update(overrides)
    return mod.AutomationCreate(**body)


def _unique_violation(constraint: str) -> asyncpg.UniqueViolationError:
    exc = asyncpg.UniqueViolationError("duplicate key value")
    exc.constraint_name = constraint
    return exc


# =============================================================================
# Create
# =============================================================================


class TestCreateTriggerShape:
    @pytest.mark.asyncio
    async def test_a_live_automation_has_no_cron_expr(self, env):
        db = _store()
        with pytest.raises(HTTPException) as caught:
            await mod.create_automation(
                MagicMock(), _live_body(cron_expr="0 * * * *"), dependencies=_deps(db)
            )
        assert caught.value.status_code == 422
        assert "cron_expr" in caught.value.detail
        db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_scheduled_automation_needs_a_cron_expr(self, env):
        db = _store()
        body = mod.AutomationCreate(name="n", expert="developer", prompt="p")
        with pytest.raises(HTTPException) as caught:
            await mod.create_automation(MagicMock(), body, dependencies=_deps(db))
        assert caught.value.status_code == 422
        assert "cron_expr" in caught.value.detail
        db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "extra",
        [
            {"primary_connector": "telefon"},
            {"job_shape": {"execution": {"connectors": {"telefon": LINE_REF}}}},
        ],
    )
    async def test_a_scheduled_automation_takes_no_live_shape(self, env, extra):
        db = _store()
        body = mod.AutomationCreate(
            name="n", expert="developer", prompt="p", cron_expr="0 * * * *", **extra
        )
        with pytest.raises(HTTPException) as caught:
            await mod.create_automation(MagicMock(), body, dependencies=_deps(db))
        assert caught.value.status_code == 422
        assert "live automation" in caught.value.detail
        db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_live_automation_names_its_primary_connector(self, env):
        db = _store()
        with pytest.raises(HTTPException) as caught:
            await mod.create_automation(
                MagicMock(), _live_body(primary_connector=None), dependencies=_deps(db)
            )
        assert caught.value.status_code == 422
        assert "primary_connector" in caught.value.detail
        db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_primary_connector_is_a_key_of_the_shape(self, env):
        db = _store()
        with pytest.raises(HTTPException) as caught:
            await mod.create_automation(
                MagicMock(),
                _live_body(primary_connector="fax"),
                dependencies=_deps(db),
            )
        assert caught.value.status_code == 422
        assert "'fax' is not a key of job_shape.execution.connectors" in (
            caught.value.detail
        )
        db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_shapeless_live_automation_has_no_primary_to_name(self, env):
        db = _store()
        with pytest.raises(HTTPException) as caught:
            await mod.create_automation(
                MagicMock(), _live_body(job_shape=None), dependencies=_deps(db)
            )
        assert caught.value.status_code == 422
        db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_primary_must_be_a_live_connector(self, env):
        db = _store()
        with pytest.raises(HTTPException) as caught:
            await mod.create_automation(
                MagicMock(),
                _live_body(primary_connector="notes"),
                dependencies=_deps(db),
            )
        assert caught.value.status_code == 422
        assert "not a live connector" in caught.value.detail
        db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_unresolvable_primary_is_refused(self, env):
        db = _store()
        shape = {"execution": {"connectors": {"telefon": {"ref": {"name": "gone"}}}}}
        with pytest.raises(HTTPException) as caught:
            await mod.create_automation(
                MagicMock(), _live_body(job_shape=shape), dependencies=_deps(db)
            )
        assert caught.value.status_code == 422
        db.create_automation.assert_not_called()


class TestCreateLive:
    @pytest.mark.asyncio
    async def test_a_live_automation_is_stored_on_its_connector(self, env):
        db = _store()
        result = await mod.create_automation(
            MagicMock(), _live_body(), dependencies=_deps(db)
        )
        assert result == {"id": AUTOMATION_ID}
        kwargs = db.create_automation.await_args.kwargs
        assert kwargs["trigger_type"] == "live"
        assert kwargs["trigger_connector_id"] == LINE_ID
        assert kwargs["job_shape"] == {
            "execution": {"connectors": CONNECTORS},
            "primary_connector": "telefon",
        }
        assert kwargs["cron_expr"] is None
        assert kwargs["next_run_at"] is None
        assert kwargs["owner_id"] == OWNER_ID
        # Only the primary alias is resolved, in the owner's scope.
        (selection, scope), *_ = env["resolved"]
        assert list(selection.connectors) == ["telefon"]
        assert scope == {"owner_id": OWNER_ID, "project_id": None}
        env["next_run"].assert_not_called()
        db.live_automation_for_connector.assert_awaited_once_with(
            LINE_ID, exclude_id=None
        )

    @pytest.mark.asyncio
    async def test_the_workspace_tri_state_is_kept(self, env):
        db = _store()
        await mod.create_automation(
            MagicMock(),
            _live_body(
                job_shape={
                    "execution": {"connectors": CONNECTORS},
                    "workspace": None,
                }
            ),
            dependencies=_deps(db),
        )
        shape = db.create_automation.await_args.kwargs["job_shape"]
        assert "workspace" in shape and shape["workspace"] is None

        await mod.create_automation(
            MagicMock(),
            _live_body(
                job_shape={
                    "execution": {"connectors": CONNECTORS},
                    "workspace": WORKSPACE,
                }
            ),
            dependencies=_deps(db),
        )
        assert db.create_automation.await_args.kwargs["job_shape"]["workspace"] == (
            WORKSPACE
        )

        await mod.create_automation(MagicMock(), _live_body(), dependencies=_deps(db))
        assert "workspace" not in db.create_automation.await_args.kwargs["job_shape"]

    @pytest.mark.asyncio
    async def test_a_second_enabled_live_automation_is_409_naming_the_holder(self, env):
        db = _store(holder={"id": HOLDER_ID, "name": "Front desk"})
        with pytest.raises(HTTPException) as caught:
            await mod.create_automation(
                MagicMock(), _live_body(), dependencies=_deps(db)
            )
        assert caught.value.status_code == 409
        detail = caught.value.detail
        assert detail["code"] == "live_connector_taken"
        assert detail["automation_id"] == HOLDER_ID
        assert detail["automation_name"] == "Front desk"
        assert "Front desk" in detail["message"]
        db.create_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_disabled_live_automation_skips_the_pre_check(self, env):
        db = _store(holder={"id": HOLDER_ID, "name": "Front desk"})
        await mod.create_automation(
            MagicMock(), _live_body(enabled=False), dependencies=_deps(db)
        )
        db.live_automation_for_connector.assert_not_called()
        kwargs = db.create_automation.await_args.kwargs
        assert kwargs["enabled"] is False and kwargs["trigger_connector_id"] == LINE_ID

    @pytest.mark.asyncio
    async def test_the_unique_index_backstop_is_409_naming_the_holder(self, env):
        """Two saves racing past the pre-check: the partial unique index
        refuses the second, and the answer names the one that won."""
        db = _store()
        db.live_automation_for_connector = AsyncMock(
            side_effect=[None, {"id": HOLDER_ID, "name": "Front desk"}]
        )
        db.create_automation = AsyncMock(
            side_effect=_unique_violation("uq_automations_live_primary")
        )
        with pytest.raises(HTTPException) as caught:
            await mod.create_automation(
                MagicMock(), _live_body(), dependencies=_deps(db)
            )
        assert caught.value.status_code == 409
        assert caught.value.detail["code"] == "live_connector_taken"
        assert caught.value.detail["automation_id"] == HOLDER_ID
        assert isinstance(caught.value.__cause__, asyncpg.UniqueViolationError)

    @pytest.mark.asyncio
    async def test_another_unique_violation_is_not_mapped(self, env):
        db = _store()
        db.create_automation = AsyncMock(
            side_effect=_unique_violation("automations_pkey")
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await mod.create_automation(
                MagicMock(), _live_body(), dependencies=_deps(db)
            )


class TestJobShapeModel:
    def test_execution_holds_connectors_only(self):
        with pytest.raises(ValidationError, match="connectors only"):
            mod.AutomationJobShape(
                execution={"connectors": {}, "session": {"connector": "x", "id": "y"}}
            )

    def test_execution_connectors_are_validated_selections(self):
        with pytest.raises(ValidationError):
            mod.AutomationJobShape(execution={"connectors": {"telefon": {"bogus": 1}}})

    def test_the_workspace_is_a_manifest_binding_or_null(self):
        assert mod.AutomationJobShape(workspace=WORKSPACE).workspace == WORKSPACE
        assert mod.AutomationJobShape(workspace=None).workspace is None
        with pytest.raises(ValidationError, match="workspace must be null"):
            mod.AutomationJobShape(workspace={"bogus": True})

    def test_stored_keeps_workspace_only_when_sent(self):
        assert mod.AutomationJobShape().stored(None) == {}
        assert mod.AutomationJobShape(workspace=None).stored("telefon") == {
            "workspace": None,
            "primary_connector": "telefon",
        }


# =============================================================================
# Run now and resume
# =============================================================================


class TestRunNowAndResume:
    @pytest.mark.asyncio
    async def test_a_live_automation_cannot_run_now(self, env, monkeypatch):
        _row(monkeypatch, _live_row(enabled=True))
        create_job = AsyncMock()
        monkeypatch.setattr(mod, "create_job_from_automation", create_job)
        db = _store()
        with pytest.raises(HTTPException) as caught:
            await mod.run_now(MagicMock(), AUTOMATION_ID, dependencies=_deps(db))
        assert caught.value.status_code == 409
        create_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_resume_of_a_live_automation_has_no_schedule(self, env, monkeypatch):
        _row(monkeypatch, _live_row())
        db = _store()
        result = await mod.resume_automation(
            MagicMock(), AUTOMATION_ID, dependencies=_deps(db)
        )
        assert result == {"id": AUTOMATION_ID}
        db.update_automation.assert_awaited_once_with(AUTOMATION_ID, enabled=True)
        db.live_automation_for_connector.assert_awaited_once_with(
            LINE_ID, exclude_id=AUTOMATION_ID
        )
        env["next_run"].assert_not_called()

    @pytest.mark.asyncio
    async def test_resume_with_another_holder_is_409(self, env, monkeypatch):
        _row(monkeypatch, _live_row())
        db = _store(holder={"id": HOLDER_ID, "name": "Front desk"})
        with pytest.raises(HTTPException) as caught:
            await mod.resume_automation(
                MagicMock(), AUTOMATION_ID, dependencies=_deps(db)
            )
        assert caught.value.status_code == 409
        assert caught.value.detail["automation_id"] == HOLDER_ID
        db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_resume_backstop_maps_the_unique_index(self, env, monkeypatch):
        _row(monkeypatch, _live_row())
        db = _store()
        db.live_automation_for_connector = AsyncMock(
            side_effect=[None, {"id": HOLDER_ID, "name": "Front desk"}]
        )
        db.update_automation = AsyncMock(
            side_effect=_unique_violation("uq_automations_live_primary")
        )
        with pytest.raises(HTTPException) as caught:
            await mod.resume_automation(
                MagicMock(), AUTOMATION_ID, dependencies=_deps(db)
            )
        assert caught.value.status_code == 409
        assert caught.value.detail["automation_name"] == "Front desk"

    @pytest.mark.asyncio
    async def test_resume_of_a_cron_automation_still_computes_next_run(
        self, env, monkeypatch
    ):
        _row(monkeypatch, _cron_row())
        next_run = MagicMock(return_value="next")
        monkeypatch.setattr(mod, "compute_initial_next_run", next_run)
        db = _store()
        await mod.resume_automation(MagicMock(), AUTOMATION_ID, dependencies=_deps(db))
        next_run.assert_called_once_with("0 * * * *", "UTC")
        db.update_automation.assert_awaited_once_with(
            AUTOMATION_ID, enabled=True, next_run_at="next"
        )
        db.live_automation_for_connector.assert_not_called()


# =============================================================================
# PATCH
# =============================================================================


class TestPatch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "change",
        [
            {"cron_expr": "0 * * * *"},
            {"timezone": "Europe/Berlin"},
            {"catchup_window_seconds": 60},
        ],
    )
    async def test_a_live_automation_has_no_schedule_fields(
        self, env, monkeypatch, change
    ):
        _row(monkeypatch, _live_row())
        db = _store()
        with pytest.raises(HTTPException) as caught:
            await mod.update_automation(
                MagicMock(),
                AUTOMATION_ID,
                mod.AutomationUpdate(**change),
                dependencies=_deps(db),
            )
        assert caught.value.status_code == 422
        db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "change",
        [
            {"primary_connector": "telefon"},
            {"job_shape": {"execution": {"connectors": CONNECTORS}}},
        ],
    )
    async def test_a_cron_automation_takes_no_live_shape(
        self, env, monkeypatch, change
    ):
        _row(monkeypatch, _cron_row())
        db = _store()
        with pytest.raises(HTTPException) as caught:
            await mod.update_automation(
                MagicMock(),
                AUTOMATION_ID,
                mod.AutomationUpdate(**change),
                dependencies=_deps(db),
            )
        assert caught.value.status_code == 422
        db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_enabling_with_another_holder_is_409(self, env, monkeypatch):
        _row(monkeypatch, _live_row())
        db = _store(holder={"id": HOLDER_ID, "name": "Front desk"})
        with pytest.raises(HTTPException) as caught:
            await mod.update_automation(
                MagicMock(),
                AUTOMATION_ID,
                mod.AutomationUpdate(enabled=True),
                dependencies=_deps(db),
            )
        assert caught.value.status_code == 409
        assert caught.value.detail["code"] == "live_connector_taken"
        assert "Front desk" in caught.value.detail["message"]
        db.live_automation_for_connector.assert_awaited_once_with(
            LINE_ID, exclude_id=AUTOMATION_ID
        )
        db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_enabling_alone_computes_no_schedule(self, env, monkeypatch):
        _row(monkeypatch, _live_row())
        db = _store()
        await mod.update_automation(
            MagicMock(),
            AUTOMATION_ID,
            mod.AutomationUpdate(enabled=True),
            dependencies=_deps(db),
        )
        db.update_automation.assert_awaited_once_with(AUTOMATION_ID, enabled=True)
        env["next_run"].assert_not_called()

    @pytest.mark.asyncio
    async def test_pausing_a_live_automation_skips_the_holder_check(
        self, env, monkeypatch
    ):
        _row(monkeypatch, _live_row(enabled=True))
        db = _store(holder={"id": HOLDER_ID, "name": "Front desk"})
        await mod.update_automation(
            MagicMock(),
            AUTOMATION_ID,
            mod.AutomationUpdate(enabled=False),
            dependencies=_deps(db),
        )
        db.live_automation_for_connector.assert_not_called()
        db.update_automation.assert_awaited_once_with(AUTOMATION_ID, enabled=False)
        env["next_run"].assert_not_called()

    @pytest.mark.asyncio
    async def test_patching_the_primary_re_resolves_its_connector(
        self, env, monkeypatch
    ):
        row = _live_row(
            job_shape={
                "execution": {
                    "connectors": {"telefon": LINE_REF, "nacht": OTHER_REF},
                },
                "workspace": None,
                "primary_connector": "telefon",
            }
        )
        _row(monkeypatch, row)
        db = _store()
        await mod.update_automation(
            MagicMock(),
            AUTOMATION_ID,
            mod.AutomationUpdate(primary_connector="nacht"),
            dependencies=_deps(db),
        )
        args, kwargs = db.update_automation.await_args
        assert args == (AUTOMATION_ID,)
        assert kwargs["trigger_connector_id"] == OTHER_LINE_ID
        assert kwargs["job_shape"] == {
            "execution": {"connectors": {"telefon": LINE_REF, "nacht": OTHER_REF}},
            "workspace": None,
            "primary_connector": "nacht",
        }
        assert "primary_connector" not in kwargs
        env["next_run"].assert_not_called()

    @pytest.mark.asyncio
    async def test_patching_the_shape_keeps_the_stored_primary(self, env, monkeypatch):
        _row(monkeypatch, _live_row())
        db = _store()
        await mod.update_automation(
            MagicMock(),
            AUTOMATION_ID,
            mod.AutomationUpdate(
                job_shape={
                    "execution": {"connectors": {"telefon": LINE_REF}},
                    "workspace": WORKSPACE,
                }
            ),
            dependencies=_deps(db),
        )
        kwargs = db.update_automation.await_args.kwargs
        assert kwargs["job_shape"] == {
            "execution": {"connectors": {"telefon": LINE_REF}},
            "workspace": WORKSPACE,
            "primary_connector": "telefon",
        }
        assert kwargs["trigger_connector_id"] == LINE_ID

    @pytest.mark.asyncio
    async def test_a_shape_that_drops_the_primary_alias_is_refused(
        self, env, monkeypatch
    ):
        _row(monkeypatch, _live_row())
        db = _store()
        with pytest.raises(HTTPException) as caught:
            await mod.update_automation(
                MagicMock(),
                AUTOMATION_ID,
                mod.AutomationUpdate(
                    job_shape={"execution": {"connectors": {"nacht": OTHER_REF}}}
                ),
                dependencies=_deps(db),
            )
        assert caught.value.status_code == 422
        db.update_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_moving_an_enabled_automation_onto_a_held_line_is_409(
        self, env, monkeypatch
    ):
        _row(
            monkeypatch,
            _live_row(
                enabled=True,
                job_shape={
                    "execution": {
                        "connectors": {"telefon": LINE_REF, "nacht": OTHER_REF}
                    },
                    "primary_connector": "telefon",
                },
            ),
        )
        db = _store(holder={"id": HOLDER_ID, "name": "Night desk"})
        with pytest.raises(HTTPException) as caught:
            await mod.update_automation(
                MagicMock(),
                AUTOMATION_ID,
                mod.AutomationUpdate(primary_connector="nacht"),
                dependencies=_deps(db),
            )
        assert caught.value.status_code == 409
        db.live_automation_for_connector.assert_awaited_once_with(
            OTHER_LINE_ID, exclude_id=AUTOMATION_ID
        )
        db.update_automation.assert_not_called()
