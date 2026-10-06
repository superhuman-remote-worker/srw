"""Prepared creation preserves authoritative calls, ordering and partial success."""

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

from fastapi import HTTPException
import pytest

from orchestrator.schemas.job_create import JobCreate
from orchestrator.services.job_admission_creation import (
    JobAdmissionCreationDependencies,
    JobAdmissionCreationInputs,
    create_admitted_job,
)
from orchestrator.services.officer_admission import (
    OfficerAdmissionConflict,
    OfficerAdmissionPreparation,
    SlotAdmissionError,
)
from orchestrator.services.officer_preflight import OfficerPreflightOutcome

USER = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
THREAD = "33333333-3333-4333-8333-333333333333"
JOB = "44444444-4444-4444-8444-444444444444"
SCHOLAR = "55555555-5555-4555-8555-555555555555"
STAMP = datetime(2026, 9, 7, 9, tzinfo=timezone.utc)
LOGGER = "orchestrator.services.job_admission_creation"


@pytest.fixture
def inputs():
    return JobAdmissionCreationInputs(
        context={"instructions": "Prepared instructions"},
        config_name="prepared-profile",
        expert_id="prepared-expert",
        config_override={"llm": {"model": "prepared-model"}},
        requested_workspace_backend="sandbox",
        root_creation=True,
        effective_user_id=USER,
        project_id=PROJECT,
        datasource_ids=["selected-connector"],
        policy_revisions={"selected-connector": 7},
        provenance={"origin": "explicit", "creation_path": "internal_rest"},
        target_project_ids=[PROJECT],
        execution_lane="stateless",
        delivery_contract={"deliverables": ["output/result.txt"]},
        officer_preparation=None,
        ticket_ready_at=None,
    )


@pytest.fixture
def deps():
    return JobAdmissionCreationDependencies(
        store=SimpleNamespace(
            create_job=AsyncMock(
                return_value={
                    "id": UUID(JOB),
                    "config_override": {
                        "server_private_marker": "preserved for outer redaction"
                    },
                }
            ),
            get_job=AsyncMock(
                return_value={"id": UUID(JOB), "repo_name": "fresh-repo"}
            ),
        ),
        admit_officer=AsyncMock(return_value={"id": UUID(JOB), "status": "paused"}),
        activate_officer=AsyncMock(
            return_value=OfficerPreflightOutcome(JOB, "activated", True, True)
        ),
        provision_officer=AsyncMock(),
        provision_repo=AsyncMock(return_value={"ignored": "provision return value"}),
        spawn_scholar=AsyncMock(return_value=None),
        resolve_origin=Mock(return_value="session"),
        trigger_dispatch=Mock(),
    )


@pytest.fixture
def officer(inputs):
    return replace(
        inputs,
        officer_preparation=OfficerAdmissionPreparation(
            project_id=PROJECT,
            thread_id=THREAD,
            requested_slot="researchers",
            slot_name="researchers",
            slot_patch={"llm": {"model": "prepared-model"}},
            category="researcher",
            config_fingerprint="exact-preparation-snapshot",
            incarnation=9,
            owner_user_id=USER,
            require_auto_pull=False,
        ),
        ticket_ready_at=STAMP,
    )


async def create(inputs, deps, command=None):
    return await create_admitted_job(
        command=command or JobCreate(description="Creation fixture", thread_id=THREAD),
        inputs=inputs,
        dependencies=deps,
    )


def order_for(deps):
    order = Mock()
    for name, target in (
        ("origin", deps.resolve_origin),
        ("create", deps.store.create_job),
        ("admit", deps.admit_officer),
        ("activate", deps.activate_officer),
        ("provision_officer", deps.provision_officer),
        ("provision_repo", deps.provision_repo),
        ("get", deps.store.get_job),
        ("scholar", deps.spawn_scholar),
        ("dispatch", deps.trigger_dispatch),
    ):
        order.attach_mock(target, name)
    return order


def test_canonical_import_has_no_runtime_or_application_startup(tmp_path):
    source = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/services/job_admission_creation.py"
    )
    probe = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            """
import importlib.abc
from pathlib import Path
import sys
blocked = ('orchestrator.main', 'orchestrator.database', 'orchestrator.security',
           'orchestrator.services.job_provisioning', 'orchestrator.services.officer_preflight',
           'agent', 'shared.runtime', 'langgraph', 'langchain', 'langchain_core', 'langchain_openai')
def forbidden(name):
    return any(name == prefix or name.startswith(prefix + '.') for prefix in blocked)
class ImportFence(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        assert not forbidden(fullname), fullname
sys.meta_path.insert(0, ImportFence())
def audit(event, args):
    assert event not in {'socket.connect', 'subprocess.Popen'}, event
sys.addaudithook(audit)
import orchestrator.services.job_admission_creation as creation
assert Path(creation.__file__).resolve() == Path(sys.argv[1]).resolve()
assert not any(forbidden(name) for name in sys.modules)
assert callable(creation.create_admitted_job)
""",
            str(source),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert probe.returncode == 0, probe.stderr


@pytest.mark.asyncio
async def test_normal_insert_gets_exact_prepared_authority_and_original_references(
    inputs, deps
):
    command = JobCreate(
        description="Exact description",
        document_path="document.pdf",
        document_dir="documents",
        config_name="ignored-request-profile",
        expert_id="ignored-request-expert",
        config_override={"llm": {"model": "ignored-request-model"}},
        context={"instructions": "ignored unprepared context"},
        user_id="ignored-request-owner",
        project_id="ignored-request-project",
        thread_id=THREAD,
        priority=8,
        creation_order=2,
        worktree_path="worktrees/task",
        delegation_context="delegation context",
    )
    result = await create(inputs, deps, command)
    assert deps.store.create_job.await_args.kwargs == {
        "description": "Exact description",
        "document_path": "document.pdf",
        "document_dir": "documents",
        "config_name": inputs.config_name,
        "expert_id": inputs.expert_id,
        "config_override": inputs.config_override,
        "context": inputs.context,
        "user_id": USER,
        "project_id": PROJECT,
        "parent_job_id": None,
        "priority": 8,
        "creation_order": 2,
        "worktree_path": "worktrees/task",
        "delegation_context": "delegation context",
        "created_by_thread_id": THREAD,
        "wake_on_complete": True,
        "datasource_ids": inputs.datasource_ids,
        "datasource_selection_provenance": inputs.provenance,
        "datasource_policy_revisions": inputs.policy_revisions,
        "authority_user_id": USER,
        "authority_project_ids": inputs.target_project_ids,
        "execution_lane": "stateless",
        "origin": "session",
        "requested_workspace_backend": "sandbox",
        "workspace_assignment_source": "request",
        "delivery_contract": inputs.delivery_contract,
    }
    for key, value in {
        "context": inputs.context,
        "config_override": inputs.config_override,
        "datasource_ids": inputs.datasource_ids,
        "datasource_policy_revisions": inputs.policy_revisions,
        "datasource_selection_provenance": inputs.provenance,
        "authority_project_ids": inputs.target_project_ids,
        "delivery_contract": inputs.delivery_contract,
    }.items():
        assert deps.store.create_job.await_args.kwargs[key] is value
    deps.resolve_origin.assert_called_once_with(
        context=inputs.context, parent_job_id=None, thread_id=THREAD
    )
    deps.admit_officer.assert_not_awaited()
    deps.activate_officer.assert_not_awaited()
    assert result is deps.store.create_job.return_value
    assert (
        result["config_override"]["server_private_marker"]
        == "preserved for outer redaction"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "root,parent,thread,expected",
    [
        (True, None, THREAD, THREAD),
        (False, JOB, THREAD, None),
        (True, None, None, None),
    ],
)
async def test_only_root_session_creation_carries_wake_backref(
    inputs, deps, root, parent, thread, expected
):
    inputs = replace(inputs, root_creation=root)
    await create(
        inputs,
        deps,
        JobCreate(description="Backref", parent_job_id=parent, thread_id=thread),
    )
    kwargs = deps.store.create_job.await_args.kwargs
    assert kwargs["created_by_thread_id"] == expected
    assert kwargs["wake_on_complete"] is bool(expected)
    assert kwargs["parent_job_id"] == parent
    assert deps.resolve_origin.call_args.kwargs["thread_id"] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend,source",
    [(None, "resolved_config"), ("", "request"), ("virtual", "request")],
)
async def test_unset_prepared_values_and_ownerless_authority_are_not_replaced(
    inputs, deps, backend, source
):
    inputs = replace(
        inputs,
        context={},
        config_override=None,
        effective_user_id=None,
        requested_workspace_backend=backend,
        execution_lane=None,
        delivery_contract=None,
    )
    await create(inputs, deps)
    kwargs = deps.store.create_job.await_args.kwargs
    assert kwargs["context"] is None
    assert kwargs["config_override"] is None
    assert kwargs["user_id"] is None and kwargs["authority_user_id"] is None
    assert kwargs["authority_project_ids"] is None
    assert kwargs["execution_lane"] is None
    assert kwargs["delivery_contract"] is None
    assert kwargs["requested_workspace_backend"] == backend
    assert kwargs["workspace_assignment_source"] == source
    assert deps.resolve_origin.call_args.kwargs["context"] is inputs.context
    assert deps.spawn_scholar.await_args.args[3] is inputs.context


@pytest.mark.asyncio
async def test_only_authority_owner_is_stringified_for_persistence(inputs, deps):
    owner = UUID(USER)
    inputs = replace(inputs, effective_user_id=owner)
    await create(inputs, deps)
    kwargs = deps.store.create_job.await_args.kwargs
    assert kwargs["user_id"] is owner
    assert kwargs["authority_user_id"] == USER


@pytest.mark.asyncio
async def test_normal_provision_and_fresh_scholar_finish_before_dispatch(inputs, deps):
    order = order_for(deps)
    inserted = deps.store.create_job.return_value

    async def provision(*, job_row):
        assert job_row is inserted
        job_row["repo_name"] = "provision-mutated-repo"
        deps.store.get_job.assert_not_awaited()
        deps.trigger_dispatch.assert_not_called()

    deps.provision_repo.side_effect = provision
    deps.spawn_scholar.return_value = {"id": UUID(SCHOLAR)}
    result = await create(inputs, deps)
    assert [call[0] for call in order.mock_calls] == [
        "origin",
        "create",
        "provision_repo",
        "get",
        "scholar",
        "dispatch",
    ]
    deps.store.get_job.assert_awaited_once_with(JOB)
    args = deps.spawn_scholar.await_args.args
    assert args[0] is deps.store.get_job.return_value
    assert args[1] == inputs.config_name
    assert args[2] is inputs.config_override and args[3] is inputs.context
    assert result is inserted
    assert result["repo_name"] == "provision-mutated-repo"
    assert result["scholar_job_id"] == SCHOLAR


@pytest.mark.asyncio
async def test_scholar_eligibility_uses_parent_field_not_root_flag(inputs, deps):
    await create(replace(inputs, root_creation=False), deps)
    deps.spawn_scholar.assert_awaited_once()


@pytest.mark.asyncio
async def test_child_is_provisioned_and_dispatched_without_scholar_or_extra_lookup(
    inputs, deps
):
    order = order_for(deps)
    await create(
        replace(inputs, root_creation=False),
        deps,
        JobCreate(description="Child", parent_job_id=JOB, thread_id=THREAD),
    )
    assert [call[0] for call in order.mock_calls] == [
        "origin",
        "create",
        "provision_repo",
        "dispatch",
    ]
    deps.store.get_job.assert_not_awaited()
    deps.spawn_scholar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [None, {}])
async def test_missing_fresh_job_skips_scholar_but_keeps_created_result(
    inputs, deps, missing
):
    deps.store.get_job.return_value = missing
    result = await create(inputs, deps)
    assert result is deps.store.create_job.return_value
    deps.spawn_scholar.assert_not_awaited()
    deps.trigger_dispatch.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["lookup", "spawn", "malformed_result"])
async def test_scholar_failures_remain_best_effort_and_dispatch_continues(
    inputs, deps, caplog, failure
):
    if failure == "lookup":
        deps.store.get_job.side_effect = RuntimeError("fresh row unavailable")
    elif failure == "spawn":
        deps.spawn_scholar.side_effect = RuntimeError("scholar unavailable")
    else:
        deps.spawn_scholar.return_value = {"missing_id": True}
    with caplog.at_level("WARNING", logger=LOGGER):
        result = await create(inputs, deps)
    assert result is deps.store.create_job.return_value
    assert "scholar_job_id" not in result
    assert len(caplog.records) == 1
    assert (
        caplog.records[0]
        .getMessage()
        .startswith(f"Failed to spawn scholar for job {JOB}:")
    )
    deps.trigger_dispatch.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["origin", "insert", "provision", "dispatch"])
async def test_non_scholar_failures_escape_unchanged_without_later_effects(
    inputs, deps, stage
):
    order = order_for(deps)
    error = RuntimeError("exact outer-mapped failure")
    target = {
        "origin": deps.resolve_origin,
        "insert": deps.store.create_job,
        "provision": deps.provision_repo,
        "dispatch": deps.trigger_dispatch,
    }[stage]
    target.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        await create(inputs, deps)
    assert caught.value is error
    expected = {
        "origin": ["origin"],
        "insert": ["origin", "create"],
        "provision": ["origin", "create", "provision_repo"],
        "dispatch": [
            "origin",
            "create",
            "provision_repo",
            "get",
            "scholar",
            "dispatch",
        ],
    }
    assert [call[0] for call in order.mock_calls] == expected[stage]


@pytest.mark.asyncio
@pytest.mark.parametrize("ticket", [None, "", "ticket-note"])
async def test_officer_calls_existing_locked_admission_and_activation_ports_exactly(
    officer, deps, ticket
):
    command = JobCreate(description="Officer", thread_id=THREAD, ticket=ticket)
    inserted = deps.admit_officer.return_value
    refreshed = {"id": UUID(JOB), "status": "created"}
    scholar_fresh = {"id": UUID(JOB), "repo_name": "second-fresh-repo"}
    deps.store.get_job.side_effect = [refreshed, scholar_fresh]
    order = order_for(deps)

    async def activate(job_row, *, provision, category, trigger_dispatch):
        assert job_row is inserted
        assert provision is deps.provision_officer
        assert category == "researcher"
        assert trigger_dispatch is deps.trigger_dispatch
        await provision(job_row, category=category)
        trigger_dispatch()
        return OfficerPreflightOutcome(JOB, "activated", True, True)

    deps.activate_officer.side_effect = activate
    result = await create(officer, deps, command)
    kwargs = deps.admit_officer.await_args.kwargs
    assert set(kwargs) == {
        "preparation",
        "job_kwargs",
        "ticket_note_id",
        "ticket_ready_at",
        "ticket_claim_source",
        "strict_provisioning",
    }
    assert kwargs["preparation"] is officer.officer_preparation
    assert kwargs["ticket_note_id"] == (ticket or None)
    assert kwargs["ticket_ready_at"] is STAMP
    assert kwargs["ticket_claim_source"] == "manual"
    assert kwargs["strict_provisioning"] is True
    assert kwargs["job_kwargs"]["context"] is officer.context
    assert (
        kwargs["job_kwargs"]["datasource_policy_revisions"] is officer.policy_revisions
    )
    assert kwargs["job_kwargs"]["delivery_contract"] is officer.delivery_contract
    assert [call[0] for call in order.mock_calls] == [
        "origin",
        "admit",
        "activate",
        "provision_officer",
        "dispatch",
        "get",
        "get",
        "scholar",
    ]
    deps.store.create_job.assert_not_awaited()
    deps.provision_repo.assert_not_awaited()
    deps.trigger_dispatch.assert_called_once_with()
    assert deps.spawn_scholar.await_args.args[0] is scholar_fresh
    assert result is refreshed
    assert result["provisioning_preflight"] == {
        "state": "activated",
        "activated": True,
        "retryable": None,
        "phase": None,
        "error": None,
    }
    assert "provisioning_preflight" not in inserted


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [None, {}])
async def test_officer_refresh_falls_back_to_original_row(officer, deps, replacement):
    deps.store.get_job.return_value = replacement
    result = await create(officer, deps)
    assert result is deps.admit_officer.return_value
    assert result["provisioning_preflight"]["activated"] is True
    assert deps.store.get_job.await_count == 2
    deps.spawn_scholar.assert_not_awaited()
    deps.trigger_dispatch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state,retryable",
    [("in-progress", None), ("retryable-failed", True), ("permanent-failed", False)],
)
async def test_unactivated_officer_retains_preflight_result_without_scholar_or_extra_dispatch(
    officer, deps, state, retryable
):
    deps.activate_officer.return_value = OfficerPreflightOutcome(
        JOB,
        state,
        False,
        True,
        retryable,
        "repository",
        "classified provisioning error",
    )
    result = await create(officer, deps)
    assert result["provisioning_preflight"] == {
        "state": state,
        "activated": False,
        "retryable": retryable,
        "phase": "repository",
        "error": "classified provisioning error",
    }
    deps.store.get_job.assert_awaited_once_with(JOB)
    deps.spawn_scholar.assert_not_awaited()
    deps.trigger_dispatch.assert_not_called()
    deps.provision_repo.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["officer", "slot"])
async def test_only_locked_officer_admission_conflicts_map_to_exact_409(
    officer, deps, kind
):
    error = (
        OfficerAdmissionConflict(
            "ticket_claimed",
            "Already claimed",
            existing_job_id=JOB,
            message="field precedence preserved",
        )
        if kind == "officer"
        else SlotAdmissionError("No remaining slot")
    )
    deps.admit_officer.side_effect = error
    with pytest.raises(HTTPException) as caught:
        await create(officer, deps)
    assert caught.value.status_code == 409
    assert caught.value.detail == (
        {
            "code": "ticket_claimed",
            "message": "field precedence preserved",
            "existing_job_id": JOB,
        }
        if kind == "officer"
        else "No remaining slot"
    )
    assert caught.value.__cause__ is error
    deps.activate_officer.assert_not_awaited()
    deps.store.get_job.assert_not_awaited()
    deps.provision_repo.assert_not_awaited()
    deps.spawn_scholar.assert_not_awaited()
    deps.trigger_dispatch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["ordinary_insert", "activation", "refresh"])
async def test_officer_shaped_errors_outside_admission_are_not_remapped(
    inputs, officer, deps, stage
):
    error = OfficerAdmissionConflict("exact-original", "Outer caller maps this failure")
    if stage == "ordinary_insert":
        deps.store.create_job.side_effect = error
    elif stage == "activation":
        inputs = officer
        deps.activate_officer.side_effect = error
    else:
        inputs = officer
        deps.store.get_job.side_effect = error
    with pytest.raises(OfficerAdmissionConflict) as caught:
        await create(inputs, deps)
    assert caught.value is error
    deps.spawn_scholar.assert_not_awaited()
    deps.trigger_dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_concurrent_creations_keep_bound_stores_and_callbacks_isolated(
    inputs, deps
):
    other_row = {"id": UUID(SCHOLAR), "source": "other-instance"}
    other = replace(
        deps,
        store=SimpleNamespace(
            create_job=AsyncMock(return_value=other_row),
            get_job=AsyncMock(return_value=None),
        ),
        provision_repo=AsyncMock(),
        spawn_scholar=AsyncMock(),
        trigger_dispatch=Mock(),
        resolve_origin=Mock(return_value="other-origin"),
    )
    entered, release = asyncio.Event(), asyncio.Event()

    async def paused_insert(**kwargs):
        entered.set()
        await release.wait()
        return deps.store.create_job.return_value

    deps.store.create_job.side_effect = paused_insert
    pending = asyncio.create_task(create(inputs, deps))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        result_other = await create(replace(inputs, context={"other": True}), other)
        deps.provision_repo.assert_not_awaited()
        deps.trigger_dispatch.assert_not_called()
    finally:
        release.set()
        result_first = await pending
    assert result_first is deps.store.create_job.return_value
    assert result_other is other_row
    deps.provision_repo.assert_awaited_once_with(job_row=result_first)
    other.provision_repo.assert_awaited_once_with(job_row=other_row)
    deps.trigger_dispatch.assert_called_once_with()
    other.trigger_dispatch.assert_called_once_with()
    assert other.store.create_job.await_args.kwargs["origin"] == "other-origin"
    assert deps.store.create_job.await_args.kwargs["origin"] == "session"


@pytest.mark.asyncio
async def test_an_inline_expert_row_reaches_the_insert_on_both_funnels(
    inputs, officer, deps
):
    row = {
        "expert_type": "worker",
        "config": {"llm": {"model": "inline-model"}},
        "prompts": {},
        "harness_config_layers": [],
        "harness_asset_name": None,
        "harness_config_name": None,
    }
    await create(replace(inputs, expert_id=None, expert_row=row), deps)
    assert deps.store.create_job.await_args.kwargs["expert_row"] is row
    assert deps.store.create_job.await_args.kwargs["expert_id"] is None

    await create(replace(officer, expert_id=None, expert_row=row), deps)
    assert deps.admit_officer.await_args.kwargs["job_kwargs"]["expert_row"] is row
