"""A job's work category staffs it when nobody named a worker.

``CATEGORY_DEFAULT_EXPERT`` maps the kind of WORK onto a worker that can do it
(an executor gets a shell). The backlog tick always applied it; a job created
directly — Officer hand-dispatch, the only mode in use while auto-pull is off —
never did, and fell through to the application default (``general-worker``,
``tools.shell: []``). Eight Better Resavio dispatches, three of them stamped
``executor``, all landed there. See
knowledge-base/knowledge/issues/category_expert_default_skipped_on_direct_dispatch.md.

These tests run the real config, Officer and work-default stages inside the
real coordinator and read what reaches creation. Every candidate default is
previewed through the real ``prepare_srw_snapshot`` — the renderer and grant
decision ``postgres.create_job`` runs on each insert — against a fake grant
store, so "staffed" also means "creation would admit it".
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fastapi import HTTPException
import pytest

from orchestrator.schemas.job_create import JobCreate
from orchestrator.services import job_admission as admission
from orchestrator.services import manifest_experts, manifest_projects
from orchestrator.services.config_resolver import resolve_config
from orchestrator.services.default_experts import ExpertSelection
from orchestrator.services.job_admission_config import JobAdmissionConfigDependencies
from orchestrator.services.job_admission_datasources import JobAdmissionDatasources
from orchestrator.services.job_admission_officer import (
    JobAdmissionOfficerDependencies,
)
from orchestrator.services.job_admission_scope import (
    JobAdmissionActor,
    JobAdmissionScope,
)
from orchestrator.services.job_admission_work_expert import preview_expert_refusals
from orchestrator.services.officer_admission import OfficerAdmissionPreparation
from orchestrator.services.officer_slots import validate_slots_spec
from orchestrator.services.work_categories import EXECUTOR, default_expert

USER = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
THREAD = "33333333-3333-4333-8333-333333333333"
APPLICATION_EXPERT = "44444444-4444-4444-8444-444444444444"
PARENT = "55555555-5555-4555-8555-555555555555"
DB_EXPERT = "66666666-6666-4666-8666-666666666666"
GRANTED = ("shell_tools", "delegation")


def _selection(
    source: str = "application",
    expert_id: str = APPLICATION_EXPERT,
    project_override: dict | None = None,
) -> ExpertSelection:
    return ExpertSelection(
        {"id": expert_id, "expert_type": "worker"},
        source,
        project_override=project_override,
    )


def _preparation(
    category: str | None, slot_patch: dict | None = None
) -> OfficerAdmissionPreparation:
    return OfficerAdmissionPreparation(
        project_id=PROJECT,
        thread_id=THREAD,
        requested_slot="build",
        slot_name="build",
        slot_patch=slot_patch or {},
        category=category,
        config_fingerprint="snapshot",
        incarnation=1,
        owner_user_id=USER,
        require_auto_pull=False,
    )


def _grant_store(grants) -> SimpleNamespace:
    """What prepare_srw_snapshot reads for a non-admin owner."""
    return SimpleNamespace(
        manifest_runtime_image="installed:1",
        get_user_settings=AsyncMock(return_value={"default_model": "gpt-4o"}),
        resolve_default_for_capability=AsyncMock(return_value=None),
        get_system_setting=AsyncMock(return_value={"value": {"enabled": True}}),
        get_user=AsyncMock(return_value={"id": USER, "is_admin": False}),
        list_grants_for_scopes=AsyncMock(
            return_value={
                "user": [{"key": key, "value_json": True} for key in grants],
                "project": [],
                "global": [],
            }
        ),
    )


@pytest.fixture
def harness(monkeypatch):
    """The coordinator with real config/Officer/work-default stages."""
    # The shipped Catalog rows live in Postgres; the definitions on disk are
    # what those rows were seeded from. The fixture project is not
    # manifest-composed, so it binds no project Expert.
    monkeypatch.setattr(
        manifest_experts, "bundled_expert_for_execution", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        manifest_projects, "project_expert_for_execution", AsyncMock(return_value=None)
    )
    state = SimpleNamespace(
        scope_context={},
        selection=_selection(),
        officer=False,
        slot_category=None,
        slot_patch=None,
        ticket_tags=None,
        grants=GRANTED,
        preview=None,
        bundled=Mock(return_value=True),
    )
    creation = AsyncMock(return_value={"id": "job"})

    async def scope_stage(**_kwargs):
        return JobAdmissionScope(
            dict(state.scope_context), {"id": USER}, USER, PROJECT, False
        )

    monkeypatch.setattr(admission, "prepare_job_admission_scope", scope_stage)
    monkeypatch.setattr(
        admission, "prepare_job_admission_workspace", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        admission,
        "prepare_job_admission_datasources",
        AsyncMock(return_value=JobAdmissionDatasources([], {}, {}, [PROJECT])),
    )
    monkeypatch.setattr(
        admission, "prepare_job_admission_delivery", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(admission, "create_admitted_job", creation)

    def preview(**kwargs):
        if state.preview is not None:
            return state.preview(**kwargs)
        return preview_expert_refusals(_grant_store(state.grants), **kwargs)

    def config_deps():
        return JobAdmissionConfigDependencies(
            store=SimpleNamespace(
                get_user=AsyncMock(return_value={}),
                get_project=AsyncMock(return_value={"id": PROJECT}),
            ),
            require_project_access=AsyncMock(),
            bundled_expert_exists=state.bundled,
            experts_db_enabled=Mock(return_value=True),
            user_experts_enabled=AsyncMock(return_value=True),
            resolve_worker_expert=AsyncMock(return_value=state.selection),
            preview_expert_refusals=preview,
        )

    def officer_deps():
        metadata = {"config_override": {"officer": {"enabled": state.officer}}}
        return JobAdmissionOfficerDependencies(
            store=SimpleNamespace(
                get_thread=AsyncMock(return_value={"metadata": metadata}),
                get_project_officer_lineage=AsyncMock(
                    return_value=[THREAD] if state.officer else []
                ),
            ),
            prepare_officer=AsyncMock(
                return_value=_preparation(state.slot_category, state.slot_patch)
            ),
            fetch_ticket=AsyncMock(
                return_value={
                    "project_id": PROJECT,
                    "status": "active",
                    "note_type": "feature",
                    "tags": state.ticket_tags,
                    "ready_at": datetime(2026, 9, 22, tzinfo=timezone.utc),
                }
            ),
        )

    deps = admission.JobAdmissionDependencies(
        validate_tool_overrides=lambda override: override,
        enforce_readiness=AsyncMock(),
        scope=Mock(),
        config=config_deps,
        officer=officer_deps,
        workspace=Mock(),
        datasources=Mock(),
        delivery=Mock(),
        creation=Mock(),
        redact_result=lambda row: row,
    )

    async def admit(**fields):
        await admission.admit_job(
            command=JobCreate(description="category fixture", **fields),
            actor=JobAdmissionActor(principal={"id": USER}),
            origin="internal_rest",
            dependencies=deps,
        )
        return creation.await_args.kwargs["inputs"]

    state.admit = admit
    return state


def _binds_a_shell(inputs) -> bool:
    """Resolve the persisted selection exactly as dispatch does."""
    assert inputs.expert_id is None, "a DB overlay would be layered on top"
    blob = resolve_config(base_config_name=inputs.config_name, expert_type="worker")
    return "run_command" in (blob["agent"]["tools"].get("shell") or [])


def _denied(inputs) -> list[dict]:
    return inputs.context["expert_selection"].get("denied_defaults", [])


# ── the category default ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_an_explicit_executor_category_is_staffed_by_its_default_expert(
    harness,
):
    inputs = await harness.admit(work_category="executor")

    assert inputs.config_name == default_expert(EXECUTOR)
    assert inputs.expert_id is None
    assert inputs.context["expert_selection"] == {
        "source": "category",
        "category": EXECUTOR,
        "expert": default_expert(EXECUTOR),
    }
    assert _binds_a_shell(inputs)


@pytest.mark.asyncio
async def test_the_application_default_it_replaces_binds_no_shell(harness):
    """The control: naming nobody and no category is today's application
    default, and the bare worker base it overlays is what ships
    ``shell: []``."""
    inputs = await harness.admit()

    assert (inputs.config_name, inputs.expert_id) == (
        "worker_base",
        APPLICATION_EXPERT,
    )
    assert inputs.context["expert_selection"]["source"] == "application"
    blob = resolve_config(base_config_name="worker_base", expert_type="worker")
    assert not blob["agent"]["tools"].get("shell")


@pytest.mark.asyncio
async def test_a_slot_category_is_staffed_the_same_way(harness):
    harness.officer = True
    harness.slot_category = EXECUTOR
    harness.scope_context = {"officer_slot": "build"}

    inputs = await harness.admit(thread_id=THREAD)

    assert inputs.officer_preparation.category == EXECUTOR
    assert inputs.context["work_category"] == EXECUTOR
    assert inputs.config_name == default_expert(EXECUTOR)
    assert inputs.context["expert_selection"]["source"] == "category"
    assert _binds_a_shell(inputs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "slot,ticket,requested,expected",
    [
        # The slot's category is the contract (§6) and wins over both.
        ("researcher", "tester", "executor", "researcher"),
        # An uncategorized slot: the claimed ticket's category, as on the tick.
        (None, "researcher", "executor", "researcher"),
        # No slot or ticket category: the request's stated intent.
        (None, None, "tester", "tester"),
    ],
)
async def test_category_order_is_slot_then_ticket_then_request(
    harness, slot, ticket, requested, expected
):
    harness.officer = True
    harness.slot_category = slot
    harness.ticket_tags = ["ready", *([f"category:{ticket}"] if ticket else [])]

    inputs = await harness.admit(
        thread_id=THREAD, ticket="the-work", work_category=requested
    )

    assert inputs.config_name == default_expert(expected)
    assert inputs.context["expert_selection"]["category"] == expected


@pytest.mark.asyncio
async def test_a_contradicting_request_is_still_named_in_the_kickoff(harness):
    """Warn-not-forbid survives: the default follows the slot, and the
    officer's differing request is stated rather than refused."""
    harness.officer = True
    harness.slot_category = "researcher"

    inputs = await harness.admit(thread_id=THREAD, work_category="executor")

    assert inputs.config_name == default_expert("researcher")
    assert "dispatched this as executor work" in inputs.context["kickoff_message"]


# ── precedence ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fields,config_name,expert_id",
    [
        ({"expert": "scholar"}, "scholar", None),
        ({"config_name": "critic"}, "critic", None),
        ({"expert": DB_EXPERT}, "worker_base", DB_EXPERT),
    ],
)
async def test_a_named_expert_wins_over_every_default(
    harness, fields, config_name, expert_id
):
    harness.officer = True
    harness.slot_category = EXECUTOR
    harness.ticket_tags = ["ready", "category:executor", "expert:designer"]
    harness.selection = _selection("explicit", DB_EXPERT)

    inputs = await harness.admit(
        thread_id=THREAD, ticket="the-work", work_category="executor", **fields
    )

    assert (inputs.config_name, inputs.expert_id) == (config_name, expert_id)
    assert inputs.context["expert_selection"]["source"] not in ("category", "ticket")


@pytest.mark.asyncio
@pytest.mark.parametrize("pre_selected", ["application", "project", "user"])
async def test_a_hand_claimed_ticket_pin_beats_every_default(harness, pre_selected):
    """The pin is an explicit choice about this work; the tick stores
    ``resolve_expert(classification)`` and never consults project or personal
    defaults, so hand-claiming the same ticket must not either."""
    harness.officer = True
    harness.slot_category = EXECUTOR
    harness.ticket_tags = ["ready", "category:executor", "expert:designer"]
    harness.selection = _selection(pre_selected)

    inputs = await harness.admit(thread_id=THREAD, ticket="build-the-ui")

    assert inputs.context["ticket_note_id"] == "build-the-ui"
    assert (inputs.config_name, inputs.expert_id) == ("designer", None)
    assert inputs.context["expert_selection"] == {
        "source": "ticket",
        "expert": "designer",
        "category": EXECUTOR,
    }


@pytest.mark.asyncio
async def test_a_displaced_project_default_takes_its_project_overlay_with_it(
    harness,
):
    """The project_experts override tunes the project's default expert; the
    pinned worker must not inherit it. The request and the slot's pins stay."""
    harness.officer = True
    harness.slot_category = EXECUTOR
    harness.slot_patch = {"workspace": {"backend": "sandbox"}}
    harness.ticket_tags = ["ready", "category:executor", "expert:designer"]
    harness.selection = _selection(
        "project", project_override={"llm": {"temperature": 0.9}}
    )

    inputs = await harness.admit(
        thread_id=THREAD,
        ticket="build-the-ui",
        config_override={"llm": {"reasoning_level": "low"}},
    )

    assert inputs.config_name == "designer"
    assert inputs.config_override["llm"] == {"reasoning_level": "low"}
    assert inputs.config_override["workspace"]["backend"] == "sandbox"


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["project", "user"])
async def test_a_scoped_default_still_beats_the_category(harness, source):
    """The category default outranks only the deployment-wide application
    fallback; a project or personal default is a person's choice."""
    harness.selection = _selection(source)

    inputs = await harness.admit(work_category="executor")

    assert (inputs.config_name, inputs.expert_id) == (
        "worker_base",
        APPLICATION_EXPERT,
    )
    assert inputs.context["expert_selection"]["source"] == source


@pytest.mark.asyncio
async def test_child_jobs_keep_their_inherited_selector(harness):
    inputs = await harness.admit(work_category="executor", parent_job_id=PARENT)

    assert (inputs.config_name, inputs.expert_id) == ("worker_base", None)
    assert "expert_selection" not in inputs.context


@pytest.mark.asyncio
@pytest.mark.parametrize("category", [None, "", "designer", "EXECUTORS"])
async def test_no_recognized_category_leaves_the_fallback_alone(harness, category):
    inputs = await harness.admit(work_category=category)

    assert inputs.expert_id == APPLICATION_EXPERT
    assert inputs.context["expert_selection"] == {
        "source": "application",
        "expert_id": APPLICATION_EXPERT,
    }


# ── a default must never turn an admissible create into a refused one ────


@pytest.mark.asyncio
async def test_an_owner_without_shell_grants_keeps_the_worker_that_runs(
    harness, caplog
):
    """engineer, scholar and product-qa all carry a shell and delegation; a
    non-admin owner without those grants would have the create refused (422)
    by the snapshot renderer. The fallback is kept, and it is admissible."""
    harness.grants = ()

    with caplog.at_level(logging.WARNING):
        inputs = await harness.admit(work_category="executor")

    assert (inputs.config_name, inputs.expert_id) == (
        "worker_base",
        APPLICATION_EXPERT,
    )
    selection = inputs.context["expert_selection"]
    assert selection["source"] == "application"
    [denied] = selection["denied_defaults"]
    assert (denied["source"], denied["expert"]) == ("category", "engineer")
    assert any(r.startswith("shell_tools:") for r in denied["reasons"])
    assert any(r.startswith("delegation:") for r in denied["reasons"])
    assert "delegation, shell_tools" in caplog.text
    assert (
        await preview_expert_refusals(
            _grant_store(()),
            config_name=inputs.config_name,
            owner_id=USER,
            project_id=PROJECT,
            config_override=inputs.config_override,
        )
        == []
    )


@pytest.mark.asyncio
async def test_a_refused_pin_falls_through_to_the_category_default(harness):
    """Each candidate is previewed in precedence order; the first admissible
    one staffs the job and the refusals ride along for the operator."""

    async def refuse_designer(**kwargs):
        return ["shell_tools: nope"] if kwargs["config_name"] == "designer" else []

    harness.preview = refuse_designer
    harness.officer = True
    harness.slot_category = EXECUTOR
    harness.ticket_tags = ["ready", "category:executor", "expert:designer"]

    inputs = await harness.admit(thread_id=THREAD, ticket="build-the-ui")

    assert inputs.config_name == default_expert(EXECUTOR)
    assert inputs.context["expert_selection"]["source"] == "category"
    assert [d["expert"] for d in _denied(inputs)] == ["designer"]


@pytest.mark.asyncio
async def test_a_retired_bundle_keeps_the_fallback(harness, monkeypatch):
    monkeypatch.setattr(
        manifest_experts,
        "bundled_expert_for_execution",
        AsyncMock(side_effect=HTTPException(409, "retired")),
    )

    inputs = await harness.admit(work_category="executor")

    assert inputs.expert_id == APPLICATION_EXPERT
    assert _denied(inputs)[0]["reasons"] == ["refused (409): retired"]


@pytest.mark.asyncio
async def test_a_preview_outage_keeps_the_fallback_instead_of_a_500(harness, caplog):
    harness.preview = AsyncMock(side_effect=RuntimeError("store down"))

    with caplog.at_level(logging.WARNING):
        inputs = await harness.admit(work_category="executor")

    assert inputs.expert_id == APPLICATION_EXPERT
    assert _denied(inputs)[0]["reasons"] == ["preview unavailable: RuntimeError"]
    assert "could not be previewed" in caplog.text


@pytest.mark.asyncio
async def test_the_disk_catalogue_is_not_this_stages_authority(harness):
    """Availability is decided by the same Catalog lookup creation uses, so a
    failing disk scan (``_bundled_job_expert_exists``) is never consulted."""
    harness.bundled.side_effect = RuntimeError("catalog down")

    inputs = await harness.admit(work_category="executor")

    assert inputs.config_name == default_expert(EXECUTOR)
    harness.bundled.assert_not_called()


def test_officer_slots_still_cannot_name_an_expert():
    """Category is a property of the work, expert of the worker; a slot pins
    the first and must never pin the second (many-to-many on purpose)."""
    with pytest.raises(ValueError, match="unknown keys"):
        validate_slots_spec(
            {"build": {"count": 1, "category": "executor", "expert": "developer"}}
        )


def test_the_structured_grant_refusal_is_wire_identical_to_the_old_422():
    from orchestrator.services.grant_enforcement import grant_violations_detail
    from orchestrator.services.manifest_execution_snapshot import ExecutionGrantDenied

    denial = ExecutionGrantDenied(["shell_tools: tools.shell requires it"])

    assert isinstance(denial, HTTPException)
    assert denial.status_code == 422
    assert denial.detail == grant_violations_detail(denial.violations)
