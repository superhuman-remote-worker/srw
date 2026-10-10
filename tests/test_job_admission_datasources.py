"""Datasource admission preserves source precedence, scope and policy snapshots."""

import asyncio
from dataclasses import replace
import subprocess
import sys
from unittest.mock import AsyncMock, Mock
from uuid import UUID

from fastapi import HTTPException
import pytest

from orchestrator.schemas.job_create import JobCreate
from orchestrator.services.datasource_policy import DatasourceUnavailableError
from orchestrator.services.job_admission_datasources import (
    JobAdmissionDatasourcesDependencies,
    prepare_job_admission_datasources,
)


USER = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
THREAD = "33333333-3333-4333-8333-333333333333"
PARENT = "44444444-4444-4444-8444-444444444444"
CONNECTOR = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
OTHER = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
UNAVAILABLE = "One or more selected connectors are unavailable"


@pytest.fixture
def deps():
    async def authorize(_actor, ids, **_kwargs):
        return ids, {value: 7 for value in ids}

    async def provenance(**values):
        return {**values, "materialized_at": "fixed-test-timestamp"}

    return JobAdmissionDatasourcesDependencies(
        backend_from_override=Mock(return_value="virtual"),
        inherit_parent_ids=AsyncMock(return_value=[CONNECTOR, OTHER]),
        filter_implicit_lite_ids=AsyncMock(return_value=[CONNECTOR]),
        authorize_selection=AsyncMock(side_effect=authorize),
        default_selection=AsyncMock(return_value=([OTHER], {OTHER: 11})),
        defaults_on_omission=Mock(return_value=False),
        selection_provenance=AsyncMock(side_effect=provenance),
    )


async def prepare(deps, *, fields=None, **scope):
    return await prepare_job_admission_datasources(
        command=JobCreate(description="Datasource fixture", **(fields or {})),
        dependencies=deps,
        **{
            "config_override": {"workspace": {"backend": "virtual"}},
            "selection_actor": {"id": USER},
            "effective_user_id": USER,
            "project_id": PROJECT,
            "internal_call": False,
            "internal_origin_bound": False,
            **scope,
        },
    )


def test_import_does_not_load_application_or_runtime_collaborators():
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from orchestrator.services.job_admission_datasources import prepare_job_admission_datasources
for prefix in ('orchestrator.main', 'orchestrator.security.auth',
               'orchestrator.database', 'agent', 'shared.runtime'):
    assert not any(n == prefix or n.startswith(prefix + '.') for n in sys.modules), prefix
""",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert probe.returncode == 0, probe.stderr


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", [[], [CONNECTOR]])
@pytest.mark.parametrize("parent", [None, PARENT])
async def test_explicit_presence_wins_over_thread_inheritance_and_defaults(
    deps, selection, parent
):
    deps.defaults_on_omission.return_value = True
    result = await prepare(
        deps,
        fields={
            "datasource_ids": selection,
            "thread_id": THREAD,
            "parent_job_id": parent,
        },
    )
    assert result.datasource_ids == (selection or [])
    assert result.provenance["origin"] == "explicit"
    deps.authorize_selection.assert_awaited_once_with(
        {"id": USER},
        selection or [],
        workspace_backend="virtual",
        target_project_ids=[PROJECT],
        effective_work_owner_id=USER,
        trusted_system_inheritance=False,
        legacy_job_id=parent,
        execution_kind="job",
    )
    deps.inherit_parent_ids.assert_not_awaited()
    deps.filter_implicit_lite_ids.assert_not_awaited()
    deps.default_selection.assert_not_awaited()
    deps.defaults_on_omission.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "thread,parent,requested_defaults,omission_defaults,origin,flag_reads",
    [
        (None, None, False, False, "native_default", 1),
        (None, None, False, True, "default", 1),
        (None, None, True, False, "default", 0),
        (THREAD, None, False, False, "inherited", 0),
        (THREAD, None, False, True, "inherited", 0),
        (THREAD, None, True, False, "default", 0),
        (THREAD, None, True, True, "default", 0),
        (None, PARENT, False, False, "inherited", 0),
        (None, PARENT, True, True, "inherited", 0),
        (THREAD, PARENT, True, True, "inherited", 0),
    ],
)
async def test_dispatch_defaults_do_not_widen_parented_delegation(
    deps, thread, parent, requested_defaults, omission_defaults, origin, flag_reads
):
    deps.defaults_on_omission.return_value = omission_defaults
    result = await prepare(
        deps,
        fields={
            "thread_id": thread,
            "parent_job_id": parent,
            "use_datasource_defaults": requested_defaults,
        },
    )
    assert result.provenance["origin"] == origin
    assert deps.defaults_on_omission.call_count == flag_reads
    if origin == "inherited":
        assert result.datasource_ids == [CONNECTOR]
        deps.inherit_parent_ids.assert_awaited_once_with(
            thread_id=thread, parent_job_id=parent
        )
        deps.filter_implicit_lite_ids.assert_awaited_once_with(
            [CONNECTOR, OTHER], "virtual"
        )
        deps.authorize_selection.assert_awaited_once_with(
            {"id": USER},
            [CONNECTOR],
            workspace_backend=None,
            target_project_ids=[PROJECT],
            effective_work_owner_id=USER,
            trusted_system_inheritance=False,
            legacy_job_id=parent,
        )
        deps.default_selection.assert_not_awaited()
    else:
        # Omission: the job's own connectors, and every other default when
        # asked for or attached on omission.
        assert (result.datasource_ids, result.policy_revisions) == (
            [OTHER],
            {OTHER: 11},
        )
        deps.default_selection.assert_awaited_once_with(
            USER,
            [PROJECT],
            "virtual",
            execution_kind="job",
            native_only=origin == "native_default",
        )
        deps.inherit_parent_ids.assert_not_awaited()
        deps.authorize_selection.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_inherited_selection_drops_what_a_job_cannot_attach(deps):
    # The session's cloud folder (OTHER) mounts into sessions only.
    drop = AsyncMock(side_effect=lambda ids, kind: [v for v in ids if v != OTHER])
    deps = replace(
        deps,
        filter_implicit_lite_ids=AsyncMock(side_effect=lambda ids, _b: ids),
        filter_execution_kind_ids=drop,
    )
    result = await prepare(deps, fields={"thread_id": THREAD})
    assert result.provenance["origin"] == "inherited"
    assert result.datasource_ids == [CONNECTOR]
    drop.assert_awaited_once_with([CONNECTOR, OTHER], "job")


@pytest.mark.asyncio
@pytest.mark.parametrize("requested_defaults", [False, True])
@pytest.mark.parametrize("thread", [None, THREAD])
async def test_ownerless_work_never_acquires_ambient_defaults(
    deps, requested_defaults, thread
):
    deps.defaults_on_omission.return_value = True
    result = await prepare(
        deps,
        fields={"thread_id": thread, "use_datasource_defaults": requested_defaults},
        effective_user_id=None,
        selection_actor=None,
        internal_call=True,
        internal_origin_bound=True,
    )
    assert result.provenance["origin"] == ("inherited" if thread else "system_empty")
    if thread:
        assert (
            deps.authorize_selection.await_args.kwargs["trusted_system_inheritance"]
            is True
        )
    else:
        assert result.datasource_ids == []
        deps.authorize_selection.assert_not_awaited()
    deps.default_selection.assert_not_awaited()
    deps.defaults_on_omission.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "internal,bound,actor,owner,expected_trust",
    [
        (True, True, None, None, True),
        (False, True, None, None, False),
        (True, False, None, None, False),
        (True, True, {"id": USER}, None, False),
        (True, True, None, USER, False),
    ],
)
async def test_explicit_reuse_only_trusts_an_ownerless_bound_internal_origin(
    deps, internal, bound, actor, owner, expected_trust
):
    await prepare(
        deps,
        fields={"datasource_ids": [CONNECTOR], "thread_id": THREAD},
        internal_call=internal,
        internal_origin_bound=bound,
        selection_actor=actor,
        effective_user_id=owner,
    )
    assert deps.inherit_parent_ids.await_count == int(expected_trust)
    assert (
        deps.authorize_selection.await_args.kwargs["trusted_system_inheritance"]
        is expected_trust
    )
    deps.filter_implicit_lite_ids.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("requested", [[], [CONNECTOR.upper()], [CONNECTOR, CONNECTOR]])
async def test_ownerless_explicit_narrowing_compares_canonical_ids_but_preserves_input(
    deps, requested
):
    result = await prepare(
        deps,
        fields={"datasource_ids": requested, "parent_job_id": PARENT},
        internal_call=True,
        internal_origin_bound=True,
        selection_actor=None,
        effective_user_id=None,
    )
    assert deps.authorize_selection.await_args.args == (None, requested)
    assert result.datasource_ids == requested
    deps.inherit_parent_ids.assert_awaited_once_with(
        thread_id=None, parent_job_id=PARENT
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "requested,inherited",
    [
        ([OTHER], [CONNECTOR]),
        ([CONNECTOR], []),
        (["malformed"], [CONNECTOR]),
        ([CONNECTOR], ["malformed"]),
        ([], ["malformed"]),
        ([CONNECTOR], [None]),
    ],
)
async def test_ownerless_narrowing_refuses_widening_and_invalid_authority_before_policy(
    deps, requested, inherited
):
    deps.inherit_parent_ids.return_value = inherited
    with pytest.raises(HTTPException) as exc:
        await prepare(
            deps,
            fields={"datasource_ids": requested, "thread_id": THREAD},
            internal_call=True,
            internal_origin_bound=True,
            selection_actor=None,
            effective_user_id=None,
        )
    assert (exc.value.status_code, exc.value.detail) == (403, UNAVAILABLE)
    deps.authorize_selection.assert_not_awaited()
    deps.selection_provenance.assert_not_awaited()
    deps.default_selection.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("internal", [False, True])
@pytest.mark.parametrize("project", [None, PROJECT])
async def test_provenance_uses_authorized_snapshot_and_resolved_scope(
    deps, internal, project
):
    ids, revisions = [OTHER], {OTHER: 19}
    actor = {"id": "initiator", "is_admin": True}
    config = {"workspace": {"backend": "none"}}
    deps.authorize_selection.side_effect = None
    deps.authorize_selection.return_value = ids, revisions
    result = await prepare(
        deps,
        fields={"datasource_ids": [CONNECTOR], "project_id": "raw-project"},
        config_override=config,
        effective_user_id=UUID(USER),
        project_id=project,
        selection_actor=actor,
        internal_call=internal,
    )
    deps.backend_from_override.assert_called_once_with(config)
    deps.selection_provenance.assert_awaited_once_with(
        datasource_ids=ids,
        policy_revisions=revisions,
        origin="explicit",
        effective_work_owner_id=USER,
        actor=actor,
        project_ids=[project] if project else [],
        creation_path="internal_rest" if internal else "user_rest",
    )
    assert result.datasource_ids is ids and result.policy_revisions is revisions
    assert result.provenance["datasource_ids"] is ids
    assert result.provenance["policy_revisions"] is revisions
    assert result.provenance["project_ids"] is result.target_project_ids
    assert deps.authorize_selection.await_args.args[0] is actor


@pytest.mark.asyncio
async def test_inherited_selection_filters_before_authorizing_and_stamping(deps):
    events = Mock()
    for name in (
        "backend_from_override",
        "inherit_parent_ids",
        "filter_implicit_lite_ids",
        "authorize_selection",
        "selection_provenance",
    ):
        events.attach_mock(getattr(deps, name), name)
    await prepare(deps, fields={"thread_id": THREAD})
    assert [event[0] for event in events.mock_calls] == [
        "backend_from_override",
        "inherit_parent_ids",
        "filter_implicit_lite_ids",
        "authorize_selection",
        "selection_provenance",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_stage,completed",
    [
        ("backend_from_override", []),
        ("inherit_parent_ids", []),
        ("filter_implicit_lite_ids", ["inherit_parent_ids"]),
        ("authorize_selection", ["inherit_parent_ids", "filter_implicit_lite_ids"]),
    ],
)
async def test_inherited_failure_stops_later_collaborators_without_remapping(
    deps, failure_stage, completed
):
    failure = RuntimeError("fixture read failure")
    getattr(deps, failure_stage).side_effect = failure
    with pytest.raises(RuntimeError) as exc:
        await prepare(deps, fields={"thread_id": THREAD})
    assert exc.value is failure
    for name in (
        "inherit_parent_ids",
        "filter_implicit_lite_ids",
        "authorize_selection",
    ):
        assert getattr(deps, name).await_count == int(
            name in completed or name == failure_stage
        )
    deps.selection_provenance.assert_not_awaited()
    deps.default_selection.assert_not_awaited()
    deps.defaults_on_omission.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        DatasourceUnavailableError(),
        RuntimeError("store unavailable"),
        HTTPException(400),
    ],
)
async def test_default_only_maps_unavailable_policy_errors(deps, failure):
    deps.default_selection.side_effect = failure
    with pytest.raises(Exception) as exc:
        await prepare(deps, fields={"use_datasource_defaults": True})
    if isinstance(failure, DatasourceUnavailableError):
        assert (exc.value.status_code, exc.value.detail) == (403, UNAVAILABLE)
        assert exc.value.__cause__ is failure
    else:
        assert exc.value is failure
    deps.selection_provenance.assert_not_awaited()
    deps.authorize_selection.assert_not_awaited()


@pytest.mark.asyncio
async def test_provenance_failure_does_not_retry_or_fall_back_to_defaults(deps):
    failure = RuntimeError("audit unavailable")
    deps.selection_provenance.side_effect = failure
    with pytest.raises(RuntimeError) as exc:
        await prepare(deps, fields={"datasource_ids": [CONNECTOR]})
    assert exc.value is failure
    deps.authorize_selection.assert_awaited_once()
    deps.selection_provenance.assert_awaited_once()
    deps.default_selection.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_preparations_keep_their_dependencies_and_scope(deps):
    waiting = asyncio.Event()
    release = asyncio.Event()

    async def suspended_default(owner, projects, backend, **kinds):
        assert (owner, projects, backend) == (USER, [PROJECT], "virtual")
        assert kinds == {"execution_kind": "job", "native_only": False}
        waiting.set()
        await release.wait()
        return [CONNECTOR], {CONNECTOR: 23}

    first_deps = replace(deps, default_selection=suspended_default)
    first = asyncio.create_task(
        prepare(first_deps, fields={"use_datasource_defaults": True})
    )
    try:
        await asyncio.wait_for(waiting.wait(), timeout=5)
        second = await prepare(
            deps,
            fields={"datasource_ids": []},
            effective_user_id=OTHER,
            selection_actor={"id": OTHER},
            project_id=None,
        )
    finally:
        release.set()
        first_result = await asyncio.wait_for(first, timeout=5)
    assert second.datasource_ids == []
    assert second.provenance["effective_work_owner_id"] == OTHER
    assert second.provenance["project_ids"] == []
    assert first_result.datasource_ids == [CONNECTOR]
    assert first_result.policy_revisions == {CONNECTOR: 23}
    assert first_result.provenance["effective_work_owner_id"] == USER
    assert first_result.provenance["project_ids"] == [PROJECT]
