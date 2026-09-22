"""A job's work category staffs it when nobody named a worker.

``CATEGORY_DEFAULT_EXPERT`` maps the kind of WORK onto a worker that can do it
(an executor gets a shell). The backlog tick always applied it; a job created
directly — Officer hand-dispatch, the only mode in use while auto-pull is off —
never did, and fell through to the application default (``general-worker``,
``tools.shell: []``). Eight Better Resavio dispatches, three of them stamped
``executor``, all landed there. See
knowledge-base/knowledge/issues/category_expert_default_skipped_on_direct_dispatch.md.

These tests run the real config and Officer preparation stages inside the real
coordinator and read what reaches creation, then resolve that selection the
way dispatch does — the claim is "the worker binds a shell", not "a variable
changed".
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from orchestrator.schemas.job_create import JobCreate
from orchestrator.services import job_admission as admission
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
from orchestrator.services.officer_admission import OfficerAdmissionPreparation
from orchestrator.services.officer_slots import validate_slots_spec
from orchestrator.services.work_categories import EXECUTOR, default_expert

USER = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
THREAD = "33333333-3333-4333-8333-333333333333"
APPLICATION_EXPERT = "44444444-4444-4444-8444-444444444444"
PARENT = "55555555-5555-4555-8555-555555555555"
DB_EXPERT = "66666666-6666-4666-8666-666666666666"


def _selection(
    source: str = "application", expert_id: str = APPLICATION_EXPERT
) -> ExpertSelection:
    return ExpertSelection({"id": expert_id, "expert_type": "worker"}, source)


def _preparation(category: str | None) -> OfficerAdmissionPreparation:
    return OfficerAdmissionPreparation(
        project_id=PROJECT,
        thread_id=THREAD,
        requested_slot="build",
        slot_name="build",
        slot_patch={},
        category=category,
        config_fingerprint="snapshot",
        incarnation=1,
        owner_user_id=USER,
        require_auto_pull=False,
    )


@pytest.fixture
def harness(monkeypatch):
    """The coordinator with real config/Officer stages and fake stores."""
    state = SimpleNamespace(
        scope_context={},
        selection=_selection(),
        officer=False,
        slot_category=None,
        ticket_tags=None,
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
            prepare_officer=AsyncMock(return_value=_preparation(state.slot_category)),
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
async def test_the_slot_contract_decides_over_a_contradicting_request(harness):
    """The slot's category is the contract the worker is held to (§6), so the
    default expert follows it; the officer's differing request stays
    warn-not-forbid and is only named in the kickoff."""
    harness.officer = True
    harness.slot_category = "researcher"

    inputs = await harness.admit(thread_id=THREAD, work_category="executor")

    assert inputs.config_name == default_expert("researcher")
    assert inputs.context["expert_selection"]["category"] == "researcher"
    assert "dispatched this as executor work" in inputs.context["kickoff_message"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fields,config_name,expert_id",
    [
        ({"expert": "scholar"}, "scholar", None),
        ({"config_name": "critic"}, "critic", None),
        ({"expert": DB_EXPERT}, "worker_base", DB_EXPERT),
    ],
)
async def test_a_named_expert_wins_over_the_category_default(
    harness, fields, config_name, expert_id
):
    harness.officer = True
    harness.slot_category = EXECUTOR
    harness.selection = _selection("explicit", DB_EXPERT)

    inputs = await harness.admit(thread_id=THREAD, work_category="executor", **fields)

    assert (inputs.config_name, inputs.expert_id) == (config_name, expert_id)
    assert inputs.context["expert_selection"]["source"] != "category"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tags,expected,source",
    [
        (["ready", "category:executor", "expert:designer"], "designer", "ticket"),
        (["ready", "category:executor"], default_expert(EXECUTOR), "category"),
    ],
)
async def test_a_hand_claimed_ticket_pin_outranks_the_category_default(
    harness, tags, expected, source
):
    """``resolve_expert`` is "the ticket's validated pin wins; otherwise the
    category default". Working a ticket by hand must not invert that."""
    harness.officer = True
    harness.slot_category = EXECUTOR
    harness.ticket_tags = tags

    inputs = await harness.admit(thread_id=THREAD, ticket="build-the-ui")

    assert inputs.context["ticket_note_id"] == "build-the-ui"
    assert (inputs.config_name, inputs.expert_id) == (expected, None)
    assert inputs.context["expert_selection"]["source"] == source


@pytest.mark.asyncio
async def test_a_named_expert_outranks_a_ticket_pin(harness):
    harness.officer = True
    harness.slot_category = EXECUTOR
    harness.ticket_tags = ["ready", "category:executor", "expert:designer"]

    inputs = await harness.admit(
        thread_id=THREAD, ticket="build-the-ui", expert="scholar"
    )

    assert (inputs.config_name, inputs.expert_id) == ("scholar", None)


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["project", "user"])
async def test_a_scoped_default_someone_chose_still_beats_the_category(harness, source):
    """Precedence decision: the category default outranks the deployment-wide
    application fallback only. A project or personal default is a person's
    deliberate choice and keeps winning."""
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
    assert inputs.context["expert_selection"]["source"] == "application"


@pytest.mark.asyncio
async def test_a_missing_bundled_default_degrades_to_the_fallback(harness, caplog):
    """A deployment that dropped the bundle must not buy a job that only fails
    when the agent cannot load its config."""
    harness.bundled.return_value = False

    inputs = await harness.admit(work_category="executor")

    assert inputs.expert_id == APPLICATION_EXPERT
    assert inputs.context["expert_selection"]["source"] == "application"
    assert "category default" in caplog.text


def test_officer_slots_still_cannot_name_an_expert():
    """Category is a property of the work, expert of the worker; a slot pins
    the first and must never pin the second (many-to-many on purpose)."""
    with pytest.raises(ValueError, match="unknown keys"):
        validate_slots_spec(
            {"build": {"count": 1, "category": "executor", "expert": "developer"}}
        )
