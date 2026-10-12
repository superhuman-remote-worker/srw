"""The shared admission coordinator preserves stage authority and failure order."""

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fastapi import HTTPException
import pytest

from orchestrator.schemas.job_create import JobCreate
from orchestrator.services import job_admission as admission
from orchestrator.services.datasource_policy_errors import (
    DatasourceMaterializationAuthorizationError,
    DatasourcePolicyConflictError,
)
from orchestrator.services.job_admission_config import JobAdmissionConfig
from orchestrator.services.job_admission_datasources import JobAdmissionDatasources
from orchestrator.services.job_admission_officer import JobAdmissionOfficer
from orchestrator.services.job_admission_scope import (
    JobAdmissionActor,
    JobAdmissionScope,
)


STAGES = (
    "scope",
    "config",
    "officer",
    "workspace",
    "datasources",
    "delivery",
    "creation",
)
STAMP = datetime(2026, 9, 7, 12, tzinfo=timezone.utc)


@pytest.fixture
def flow(monkeypatch):
    command = JobCreate(
        description="coordinator fixture",
        config_override={"source": "request"},
        context={"source": "request"},
        user_id="raw-user",
        project_id="raw-project",
        ticket="trusted-ticket",
        thread_id="thread",
        execution_lane="stateless",
    )
    actor = JobAdmissionActor(principal={"id": "initiator"})
    scope = JobAdmissionScope(
        context={"source": "scope"},
        principal={"id": "resolved-owner"},
        user_id="resolved-user",
        project_id="scope-project",
        origin_bound=True,
    )
    config = JobAdmissionConfig(
        context={"source": "config"},
        project_id="resolved-project",
        config_name="resolved-expert",
        config_override={"source": "project"},
        expert_id="resolved-expert-id",
        request_config_override=command.config_override,
        requested_workspace_backend="virtual",
        root_creation=True,
    )
    officer = JobAdmissionOfficer(
        context={"source": "officer"},
        config_override={"source": "slot"},
        preparation=object(),
        ticket_ready_at=STAMP,
    )
    datasources = JobAdmissionDatasources(
        datasource_ids=["authorized-connector"],
        policy_revisions={"authorized-connector": 19},
        provenance={"origin": "inherited", "materialized_at": STAMP.isoformat()},
        target_project_ids=["resolved-project"],
    )
    delivery = {"version": 1, "deliverables": ["output/report.txt"]}
    raw_row = {"id": "job", "context": {"vm": {"host": "fixture-private"}}}
    redacted_row = {"id": "job", "context": {}}
    values = dict(
        scope=scope,
        config=config,
        officer=officer,
        workspace="pinned",
        datasources=datasources,
        delivery=delivery,
        creation=raw_row,
    )
    stages = {}
    events = Mock()
    for name, value in values.items():
        stage = AsyncMock(return_value=value)
        symbol = (
            "create_admitted_job"
            if name == "creation"
            else f"prepare_job_admission_{name}"
        )
        monkeypatch.setattr(admission, symbol, stage)
        stages[name] = stage
        events.attach_mock(stage, name)
    factories = {name: Mock(return_value=object()) for name in STAGES}
    for name, factory in factories.items():
        events.attach_mock(factory, f"bind_{name}")
    dependencies = admission.JobAdmissionDependencies(
        validate_tool_overrides=Mock(return_value={"source": "validated"}),
        enforce_readiness=AsyncMock(),
        redact_result=Mock(return_value=redacted_row),
        **factories,
    )
    events.attach_mock(dependencies.validate_tool_overrides, "validate")
    events.attach_mock(dependencies.enforce_readiness, "readiness")
    events.attach_mock(dependencies.redact_result, "redact")
    return SimpleNamespace(
        command=command,
        actor=actor,
        deps=dependencies,
        events=events,
        stages=stages,
        factories=factories,
        scope=scope,
        config=config,
        officer=officer,
        datasources=datasources,
        delivery=delivery,
        raw_row=raw_row,
        redacted_row=redacted_row,
    )


async def admit(flow, origin="internal_rest"):
    return await admission.admit_job(
        command=flow.command,
        actor=flow.actor,
        origin=origin,
        dependencies=flow.deps,
    )


def test_import_does_not_load_database_auth_or_application_startup():
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from orchestrator.services.job_admission import admit_job
for prefix in ('orchestrator.main', 'orchestrator.database', 'orchestrator.security.auth', 'agent'):
    assert not any(n == prefix or n.startswith(prefix + '.') for n in sys.modules), prefix
""",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert probe.returncode == 0, probe.stderr


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["user_rest", "internal_rest"])
async def test_pipeline_passes_prepared_authority_and_snapshot_references(flow, origin):
    original_override = flow.command.config_override

    async def bind_delivery(**kwargs):
        # The delivery owner intentionally normalizes the prepared context in place.
        kwargs["context"]["required_deliverables"] = ["output/report.txt"]
        return flow.delivery

    flow.stages["delivery"].side_effect = bind_delivery
    assert await admit(flow, origin) is flow.redacted_row
    flow.deps.validate_tool_overrides.assert_called_once_with(original_override)
    assert flow.command.config_override == {"source": "validated"}
    flow.stages["scope"].assert_awaited_once_with(
        command=flow.command,
        actor=flow.actor,
        origin=origin,
        dependencies=flow.factories["scope"].return_value,
    )
    flow.stages["config"].assert_awaited_once_with(
        command=flow.command,
        scope=flow.scope,
        origin=origin,
        dependencies=flow.factories["config"].return_value,
    )
    flow.stages["officer"].assert_awaited_once_with(
        command=flow.command,
        config=flow.config,
        dependencies=flow.factories["officer"].return_value,
    )
    flow.stages["workspace"].assert_awaited_once_with(
        context=flow.officer.context,
        config_override=flow.officer.config_override,
        effective_user_id=flow.scope.user_id,
        project_id=flow.config.project_id,
        requested_lane="stateless",
        root_creation=True,
        dependencies=flow.factories["workspace"].return_value,
    )
    flow.stages["datasources"].assert_awaited_once_with(
        command=flow.command,
        config_override=flow.officer.config_override,
        selection_actor=(
            flow.scope.principal if origin == "internal_rest" else flow.actor.principal
        ),
        effective_user_id=flow.scope.user_id,
        project_id=flow.config.project_id,
        internal_call=origin == "internal_rest",
        internal_origin_bound=True,
        dependencies=flow.factories["datasources"].return_value,
        unattended=True,
    )
    flow.stages["delivery"].assert_awaited_once_with(
        command=flow.command,
        context=flow.officer.context,
        datasource_ids=flow.datasources.datasource_ids,
        target_project_ids=flow.datasources.target_project_ids,
        officer_preparation=flow.officer.preparation,
        ticket_ready_at=STAMP,
        dependencies=flow.factories["delivery"].return_value,
    )
    creation_args = flow.stages["creation"].await_args.kwargs
    assert creation_args["command"] is flow.command
    assert creation_args["dependencies"] is flow.factories["creation"].return_value
    inputs = creation_args["inputs"]
    for field, expected in {
        "context": flow.officer.context,
        "config_override": flow.officer.config_override,
        "datasource_ids": flow.datasources.datasource_ids,
        "policy_revisions": flow.datasources.policy_revisions,
        "provenance": flow.datasources.provenance,
        "target_project_ids": flow.datasources.target_project_ids,
        "officer_preparation": flow.officer.preparation,
        "ticket_ready_at": STAMP,
        "delivery_contract": flow.delivery,
    }.items():
        assert getattr(inputs, field) is expected
    assert inputs.context["required_deliverables"] == ["output/report.txt"]
    assert (
        inputs.config_name,
        inputs.expert_id,
        inputs.requested_workspace_backend,
        inputs.root_creation,
        inputs.effective_user_id,
        inputs.project_id,
        inputs.execution_lane,
    ) == (
        "resolved-expert",
        "resolved-expert-id",
        "virtual",
        True,
        "resolved-user",
        "resolved-project",
        "pinned",
    )
    flow.deps.redact_result.assert_called_once_with(flow.raw_row)
    assert [event[0] for event in flow.events.mock_calls] == [
        "validate",
        "readiness",
        *(step for name in STAGES for step in (f"bind_{name}", name)),
        "redact",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("preparation", [None, object()])
async def test_only_an_officer_ticket_selects_as_unattended(flow, preparation):
    # Work a user starts gets their outbox among its defaults; an officer's
    # ticket (prepared by the officer stage) does not.
    flow.stages["officer"].return_value = replace(flow.officer, preparation=preparation)
    await admit(flow)
    kwargs = flow.stages["datasources"].await_args.kwargs
    assert kwargs["unattended"] is (preparation is not None)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", STAGES)
@pytest.mark.parametrize("during_binding", [False, True])
async def test_refusal_stops_before_any_later_stage_is_bound(
    flow, stage, during_binding
):
    refusal = HTTPException(409, {"code": "fixture_denial", "stage": stage})
    target = flow.factories[stage] if during_binding else flow.stages[stage]
    target.side_effect = refusal
    with pytest.raises(HTTPException) as exc:
        await admit(flow)
    assert exc.value is refusal
    reached = STAGES.index(stage)
    for index, name in enumerate(STAGES):
        assert flow.factories[name].call_count == int(index <= reached)
        assert flow.stages[name].await_count == int(
            index < reached or (index == reached and not during_binding)
        )
    flow.deps.redact_result.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("point", ["validate_tool_overrides", "enforce_readiness"])
@pytest.mark.parametrize(
    "error", [RuntimeError("early fixture"), HTTPException(503, "gate")]
)
async def test_tool_and_readiness_errors_stay_outside_creation_error_translation(
    flow, point, error
):
    getattr(flow.deps, point).side_effect = error
    with pytest.raises(type(error)) as exc:
        await admit(flow)
    assert exc.value is error
    for factory in flow.factories.values():
        factory.assert_not_called()
    flow.deps.redact_result.assert_not_called()
    if point == "validate_tool_overrides":
        flow.deps.enforce_readiness.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("point", ["stage", "factory", "redaction"])
async def test_unexpected_error_keeps_legacy_500_and_cause(flow, point, caplog):
    failure = RuntimeError("controlled fixture failure")
    target = {
        "stage": flow.stages["creation"],
        "factory": flow.factories["creation"],
        "redaction": flow.deps.redact_result,
    }[point]
    target.side_effect = failure
    with caplog.at_level("ERROR", logger="orchestrator.services.job_admission"):
        with pytest.raises(HTTPException) as exc:
            await admit(flow)
    assert (exc.value.status_code, exc.value.detail) == (500, str(failure))
    assert exc.value.__cause__ is failure
    assert "Failed to create job: controlled fixture failure" in caplog.text
    if point != "redaction":
        flow.deps.redact_result.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,status,detail",
    [
        (
            "DatasourceMaterializationAuthorizationError",
            403,
            "Work owner is no longer authorized",
        ),
        (
            "DatasourcePolicyConflictError",
            409,
            "Connector policy changed while creating work; retry the request",
        ),
    ],
)
@pytest.mark.parametrize("source", ["service", "postgres"])
async def test_persistence_error_exports_keep_identity_and_http_translation(
    flow, name, status, detail, source
):
    from orchestrator.database import postgres
    from orchestrator.services import datasource_policy_errors

    error_type = getattr(datasource_policy_errors, name)
    assert getattr(postgres, name) is error_type
    failure = getattr(
        postgres if source == "postgres" else datasource_policy_errors, name
    )("fixture policy failure")
    flow.stages["creation"].side_effect = failure
    with pytest.raises(HTTPException) as exc:
        await admit(flow)
    assert (exc.value.status_code, exc.value.detail) == (status, detail)
    assert exc.value.__cause__ is failure
    flow.deps.redact_result.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [DatasourceMaterializationAuthorizationError(), DatasourcePolicyConflictError()],
)
async def test_early_persistence_policy_failure_never_reaches_delivery_or_creation(
    flow, failure
):
    flow.stages["datasources"].side_effect = failure
    with pytest.raises(HTTPException) as exc:
        await admit(flow)
    assert exc.value.__cause__ is failure
    flow.factories["delivery"].assert_not_called()
    flow.factories["creation"].assert_not_called()


@pytest.mark.asyncio
async def test_cancellation_propagates_without_redaction_or_later_stage_binding(flow):
    started = asyncio.Event()

    async def suspended(**_kwargs):
        started.set()
        await asyncio.Future()

    flow.stages["datasources"].side_effect = suspended
    task = asyncio.create_task(admit(flow))
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        if not task.done():
            task.cancel()
    flow.factories["delivery"].assert_not_called()
    flow.factories["creation"].assert_not_called()
    flow.deps.redact_result.assert_not_called()


@pytest.mark.asyncio
async def test_redaction_waits_for_creation_owner_to_complete(flow):
    started, release = asyncio.Event(), asyncio.Event()

    async def creating(**_kwargs):
        started.set()
        await release.wait()
        flow.raw_row["provisioning_preflight"] = {"state": "ready"}
        return flow.raw_row

    flow.stages["creation"].side_effect = creating
    task = asyncio.create_task(admit(flow))
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        flow.deps.redact_result.assert_not_called()
    finally:
        release.set()
        result = await asyncio.wait_for(task, timeout=5)
    assert result is flow.redacted_row
    flow.deps.redact_result.assert_called_once_with(flow.raw_row)
    assert flow.deps.redact_result.call_args.args[0]["provisioning_preflight"] == {
        "state": "ready"
    }
