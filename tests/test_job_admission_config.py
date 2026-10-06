"""Project/expert preparation preserves authority, ordering and override policy."""

import asyncio
import copy
from dataclasses import replace
from functools import partial
import json
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fastapi import HTTPException
import pytest

from orchestrator.schemas.job_create import JobCreate
from orchestrator.services.config_overrides import deep_merge_dicts
from orchestrator.services.default_experts import (
    DefaultExpertUnavailable,
    ExpertSelection,
    ExpertSelectionError,
    resolve_root_expert,
)
from orchestrator.services.job_admission_config import (
    JobAdmissionConfigDependencies,
    prepare_job_admission_config,
)
from orchestrator.services.job_admission_scope import JobAdmissionScope
from orchestrator.services.project_status import PROJECT_ARCHIVED_DETAIL


USER = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
EXPERT = "33333333-3333-4333-8333-333333333333"
PARENT = "44444444-4444-4444-8444-444444444444"


@pytest.fixture
def scope():
    return JobAdmissionScope({}, {"id": USER}, USER, PROJECT, False)


@pytest.fixture
def deps():
    return JobAdmissionConfigDependencies(
        store=SimpleNamespace(
            get_user=AsyncMock(return_value={"default_project_id": PROJECT}),
            get_project=AsyncMock(return_value={"id": PROJECT}),
            # No project_workspace_defaults row: the installation decides.
            fetchrow=AsyncMock(return_value=None),
        ),
        require_project_access=AsyncMock(),
        bundled_expert_exists=Mock(return_value=True),
        experts_db_enabled=Mock(return_value=True),
        user_experts_enabled=AsyncMock(return_value=True),
        resolve_worker_expert=AsyncMock(
            return_value=ExpertSelection({"id": EXPERT}, "application")
        ),
        preview_expert_refusals=AsyncMock(return_value=[]),
        srw_image=Mock(return_value="installed:1"),
    )


async def prepare(deps, scope, *, origin="user_rest", **fields):
    return await prepare_job_admission_config(
        command=JobCreate(description="config fixture", **fields),
        scope=scope,
        origin=origin,
        dependencies=deps,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["user_rest", "internal_rest"])
async def test_generic_expert_requires_native_job_before_private_merging(
    deps, scope, origin
):
    deps.resolve_worker_expert.return_value = ExpertSelection(
        {"id": EXPERT, "harness_adapter": None, "config": {}}, "application"
    )
    with pytest.raises(HTTPException) as denied:
        await prepare(deps, scope, origin=origin)
    assert denied.value.status_code == 409
    assert "native manifest Job admission" in denied.value.detail


def test_config_import_does_not_load_auth_database_or_application_startup():
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from orchestrator.services.job_admission_config import prepare_job_admission_config
for prefix in ('orchestrator.main', 'orchestrator.security.auth',
               'orchestrator.database', 'agent'):
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
async def test_default_project_precedes_access_and_expert_reads(deps, scope, origin):
    order = Mock()
    for name, method in (
        ("user", deps.store.get_user),
        ("access", deps.require_project_access),
        ("project", deps.store.get_project),
        ("enabled", deps.experts_db_enabled),
        ("preference", deps.user_experts_enabled),
        ("expert", deps.resolve_worker_expert),
    ):
        order.attach_mock(method, name)
    result = await prepare(deps, replace(scope, project_id=None), origin=origin)
    assert result.project_id == PROJECT
    assert [call[0] for call in order.mock_calls] == [
        "user",
        "access",
        "project",
        "enabled",
        "preference",
        "expert",
    ]
    deps.require_project_access.assert_awaited_once_with(
        scope.principal,
        PROJECT,
        denial_detail=(
            "Internal job origin scope is unavailable"
            if origin == "internal_rest"
            else "Project role 'editor' or higher required"
        ),
    )
    deps.bundled_expert_exists.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", [USER, None])
async def test_origin_bound_child_never_inherits_owner_defaults(deps, scope, owner):
    scope = replace(
        scope,
        project_id=None,
        user_id=owner,
        principal={"id": owner} if owner else None,
        origin_bound=True,
    )
    result = await prepare(deps, scope, origin="internal_rest", parent_job_id=PARENT)
    assert result.project_id is None and result.expert_id is None
    assert result.root_creation is False
    deps.store.get_user.assert_not_awaited()
    deps.store.get_project.assert_not_awaited()
    deps.experts_db_enabled.assert_not_called()
    deps.user_experts_enabled.assert_not_awaited()
    deps.resolve_worker_expert.assert_not_awaited()


@pytest.mark.asyncio
async def test_thread_without_worker_parent_still_resolves_root_expert(deps, scope):
    result = await prepare(
        deps,
        replace(scope, origin_bound=True),
        origin="internal_rest",
        thread_id=PARENT,
    )
    assert result.root_creation and result.expert_id == EXPERT
    deps.resolve_worker_expert.assert_awaited_once()


@pytest.mark.asyncio
async def test_default_lookup_failure_keeps_existing_fail_open_behavior(
    deps, scope, caplog
):
    deps.store.get_user.side_effect = RuntimeError("unavailable")
    result = await prepare(deps, replace(scope, project_id=None))
    assert result.project_id is None
    assert "Failed to resolve default project" in caplog.text
    deps.require_project_access.assert_awaited_once()
    deps.resolve_worker_expert.assert_awaited_once()


@pytest.mark.asyncio
async def test_access_refusal_precedes_even_conflicting_expert_selectors(deps, scope):
    deps.require_project_access.side_effect = HTTPException(403, "denied")
    with pytest.raises(HTTPException) as exc:
        await prepare(deps, scope, expert="developer", expert_id=EXPERT)
    assert (exc.value.status_code, exc.value.detail) == (403, "denied")
    deps.store.get_project.assert_not_awaited()
    deps.bundled_expert_exists.assert_not_called()
    deps.resolve_worker_expert.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fields,status,detail",
    [
        ({"expert": "developer", "expert_id": EXPERT}, 400, "a job runs one expert"),
        ({"expert": "missing"}, 400, "Unknown expert"),
        ({"config_name": "../escape"}, 422, "config_name"),
        (
            {"config_override": {"workspace": {"backend": "unknown-tier"}}},
            422,
            "invalid_workspace_backend",
        ),
    ],
)
async def test_selector_and_workspace_refusals_precede_project_load(
    deps, scope, fields, status, detail
):
    deps.bundled_expert_exists.return_value = False
    with pytest.raises(HTTPException) as exc:
        await prepare(deps, scope, **fields)
    assert exc.value.status_code == status
    assert detail.lower() in str(exc.value.detail).lower()
    deps.store.get_project.assert_not_awaited()
    deps.user_experts_enabled.assert_not_awaited()
    deps.resolve_worker_expert.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_deployment_selector_does_not_consult_catalogue(deps, scope):
    deps.bundled_expert_exists.side_effect = AssertionError("must stay lazy")
    result = await prepare(deps, scope, config_name="deployment/custom.yaml")
    assert result.config_name == "deployment/custom.yaml"
    assert result.expert_id is None
    deps.experts_db_enabled.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["user_rest", "internal_rest"])
@pytest.mark.parametrize(
    "project,status,detail",
    [
        (None, 404, f"Project '{PROJECT}' not found"),
        (
            {"status": " Archived ", "default_config_override": "invalid json"},
            409,
            PROJECT_ARCHIVED_DETAIL,
        ),
    ],
)
async def test_missing_or_archived_project_precedes_json_and_expert_gates(
    deps, scope, origin, project, status, detail
):
    deps.store.get_project.return_value = project
    with pytest.raises(HTTPException) as exc:
        await prepare(deps, scope, origin=origin)
    assert (exc.value.status_code, exc.value.detail) == (status, detail)
    deps.experts_db_enabled.assert_not_called()
    deps.user_experts_enabled.assert_not_awaited()
    deps.resolve_worker_expert.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parent,explicit,db_enabled,preference,expected,preference_calls,db_calls",
    [
        (None, None, True, False, False, 1, 1),
        (None, EXPERT, True, False, True, 1, 2),
        (PARENT, EXPERT, True, False, True, 0, 1),
        (PARENT, None, True, True, False, 0, 0),
        (None, EXPERT, False, True, False, 0, 2),
    ],
)
async def test_explicit_experts_keep_distinct_gates_and_short_circuit_order(
    deps,
    scope,
    parent,
    explicit,
    db_enabled,
    preference,
    expected,
    preference_calls,
    db_calls,
):
    deps.experts_db_enabled.return_value = db_enabled
    deps.user_experts_enabled.return_value = preference
    result = await prepare(deps, scope, parent_job_id=parent, expert_id=explicit)
    assert deps.resolve_worker_expert.await_count == int(expected)
    assert deps.user_experts_enabled.await_count == preference_calls
    assert deps.experts_db_enabled.call_count == db_calls
    assert result.expert_id == (EXPERT if expected or explicit else None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parent,db_enabled,expected",
    [
        (None, False, "developer"),
        (PARENT, False, "worker_base"),
        (None, True, "worker_base"),
    ],
)
async def test_legacy_project_default_only_applies_in_disabled_db_root_mode(
    deps, scope, parent, db_enabled, expected
):
    deps.store.get_project.return_value = {"default_config_name": "developer"}
    deps.experts_db_enabled.return_value = db_enabled
    deps.user_experts_enabled.return_value = False
    result = await prepare(deps, scope, parent_job_id=parent)
    assert result.config_name == expected
    deps.resolve_worker_expert.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("as_json", [False, True])
async def test_override_precedence_and_null_removal_are_preserved(deps, scope, as_json):
    project_override = {
        "llm": {"model": "project", "temperature": 0.2},
        "extra": {"keep": True, "remove": 1},
        # Slice A2b: a legacy Project's stored workspace backend no longer
        # leaks through this merge; the workspace defaults chain resolves it
        # instead (here: the shipped installation default, since this
        # fixture's store never returns a project_workspace_defaults row).
        "workspace": {"backend": "vm"},
        "items": [1, 2],
    }
    expert_override = {
        "llm": {"model": "expert"},
        "extra": {"remove": None},
        "items": [3],
    }
    requested = {"llm": {"model": "request"}, "items": [4], "extra": {"request": True}}
    before = copy.deepcopy(
        (project_override, expert_override, requested, scope.context)
    )
    deps.store.get_project.return_value = {
        "default_config_override": json.dumps(project_override)
        if as_json
        else project_override
    }
    deps.resolve_worker_expert.return_value = ExpertSelection(
        {"id": EXPERT}, "project", expert_override
    )
    result = await prepare(deps, scope, config_override=requested)
    assert result.config_override == {
        "llm": {"model": "request", "temperature": 0.2},
        "extra": {"keep": True, "request": True},
        "workspace": {"backend": "sandbox"},
        "items": [4],
    }
    assert result.requested_workspace_backend is None
    assert result.request_config_override == requested
    assert result.context == {
        "expert_selection": {"source": "project", "expert_id": EXPERT}
    }
    assert (project_override, expert_override, requested, scope.context) == before


def test_existing_shallow_merge_identity_is_retained():
    untouched, replacement = {"nested": []}, ["replacement"]
    base = {"untouched": untouched, "changed": {"old": 1, "keep": 2}}
    result = deep_merge_dicts(base, {"changed": {"old": None}, "items": replacement})
    assert result == {
        "untouched": untouched,
        "changed": {"keep": 2},
        "items": replacement,
    }
    assert result["untouched"] is untouched and result["items"] is replacement
    assert result["changed"] is not base["changed"]
    assert base["changed"] == {"old": 1, "keep": 2}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,status",
    [
        (ExpertSelectionError("denied expert"), 422),
        (DefaultExpertUnavailable("no default"), 503),
    ],
)
async def test_existing_expert_errors_keep_http_mapping(deps, scope, error, status):
    deps.resolve_worker_expert.side_effect = error
    with pytest.raises(HTTPException) as exc:
        await prepare(deps, scope)
    assert (exc.value.status_code, exc.value.detail) == (status, str(error))


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["explicit", "project", "user", "application"])
async def test_real_default_resolver_remains_precedence_owner(deps, scope, source):
    row = {"id": EXPERT, "expert_type": "worker", "owner_id": USER}
    db = SimpleNamespace(
        get_expert_visible_by_id=AsyncMock(return_value=row),
        get_project_expert_link=AsyncMock(return_value=None),
        get_project_default_expert=AsyncMock(
            return_value=row if source == "project" else None
        ),
        list_grants_for_scopes=AsyncMock(
            return_value={"user": [], "project": [], "global": []}
        ),
        get_user_expert_default=AsyncMock(
            return_value=row if source == "user" else None
        ),
        get_application_expert_default=AsyncMock(return_value=row),
    )
    deps = replace(
        deps,
        resolve_worker_expert=partial(resolve_root_expert, db, expert_type="worker"),
    )
    result = await prepare(
        deps, scope, expert_id=EXPERT if source == "explicit" else None
    )
    assert result.context["expert_selection"] == {"source": source, "expert_id": EXPERT}
    assert db.get_project_default_expert.await_count == (source != "explicit")
    assert db.get_application_expert_default.await_count == (source == "application")


@pytest.mark.asyncio
async def test_interleaved_preparations_keep_dependency_and_context_isolation(
    deps, scope
):
    entered, release = asyncio.Event(), asyncio.Event()

    async def first_expert(**kwargs):
        entered.set()
        await release.wait()
        return ExpertSelection({"id": EXPERT}, "application")

    first_deps = replace(deps, resolve_worker_expert=first_expert)
    second_deps = replace(
        deps,
        store=SimpleNamespace(
            get_user=AsyncMock(),
            get_project=AsyncMock(return_value={"id": PARENT}),
            fetchrow=AsyncMock(return_value=None),
        ),
        require_project_access=AsyncMock(),
        resolve_worker_expert=AsyncMock(
            return_value=ExpertSelection({"id": USER}, "user")
        ),
    )
    first = asyncio.create_task(prepare(first_deps, scope))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        second = await prepare(
            second_deps,
            replace(scope, project_id=PARENT, principal={"id": EXPERT}, user_id=EXPERT),
        )
    finally:
        release.set()
    first = await first
    assert (first.project_id, first.expert_id) == (PROJECT, EXPERT)
    assert (second.project_id, second.expert_id) == (PARENT, USER)
    assert scope.context == {}
    assert second_deps.require_project_access.await_args.args == (
        {"id": EXPERT},
        PARENT,
    )
    assert second_deps.resolve_worker_expert.await_args.kwargs["user_id"] == EXPERT


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["user_rest", "internal_rest"])
async def test_manifest_workspace_overrides_inherited_legacy_project_backend(
    deps, scope, origin
):
    deps.store.get_project.return_value = {
        "id": PROJECT,
        "default_config_override": {"workspace": {"backend": "vm"}, "autonomy": "full"},
    }
    result = await prepare(deps, scope, origin=origin, workspace=None)
    assert result.config_override["workspace"]["backend"] == "none"
    assert result.config_override["autonomy"] == "full"
    assert result.workspace_selection["document"] is None


@pytest.mark.asyncio
async def test_two_authored_workspace_selections_remain_an_error(deps, scope):
    with pytest.raises(HTTPException) as denied:
        await prepare(
            deps,
            scope,
            workspace=None,
            config_override={"workspace": {"backend": "sandbox"}},
        )
    assert denied.value.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["user_rest", "internal_rest"])
async def test_a_root_job_in_a_plain_project_reaches_the_resolver(
    deps, scope, origin, monkeypatch
):
    deps.store.get_project.return_value = {"id": PROJECT, "manifest_composed": False}
    calls = []

    async def fake_select(db, user, **kwargs):
        calls.append(kwargs)
        return {"backend": "sandbox"}, {
            "sources": {"tier": "installation", "template": "builtin"}
        }

    monkeypatch.setattr(
        "orchestrator.services.manifest_workspace_selection.select_execution_workspace",
        fake_select,
    )
    result = await prepare(deps, scope, origin=origin)
    assert calls and calls[0]["supplied"] is False
    assert result.workspace_selection["sources"]["tier"] == "installation"


# --------------------------------------------------------------------------- #
# Inline experts (creation UI slice S2)
# --------------------------------------------------------------------------- #


def inline_expert(**runtime):
    return {
        "inline": {
            "runtime": {
                "adapter": "srw/v1",
                "config": {
                    "config": {"llm": {"model": "gpt-4o"}},
                    "prompts": {"persona": "A plain helper."},
                },
                **runtime,
            }
        }
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["user_rest", "internal_rest"])
async def test_inline_expert_is_the_callers_choice_without_catalogue_identity(
    deps, scope, origin
):
    result = await prepare(
        deps,
        scope,
        origin=origin,
        expert=inline_expert(),
        expert_based_on="developer",
    )
    assert result.expert_row == {
        "expert_type": "worker",
        "config": {"llm": {"model": "gpt-4o"}},
        "prompts": {"persona": "A plain helper."},
        "harness_config_layers": [],
        "harness_asset_name": None,
        "harness_config_name": None,
    }
    assert "manifest_uid" not in result.expert_row and "id" not in result.expert_row
    assert result.expert_id is None
    assert result.config_name == "worker_base"
    # A caller choice: no project/personal/application default displaces it.
    assert result.expert_source == "caller"
    deps.resolve_worker_expert.assert_not_awaited()
    deps.bundled_expert_exists.assert_not_called()
    assert result.context["expert_selection"] == {"source": "inline"}
    assert result.context["expert_based_on"] == "developer"


@pytest.mark.asyncio
async def test_expert_based_on_is_recorded_only_for_an_inline_expert(deps, scope):
    result = await prepare(
        deps,
        scope,
        expert="developer",
        expert_based_on="scholar",
        context={"expert_based_on": "forged-by-context"},
    )
    assert result.expert_row is None
    assert result.config_name == "developer"
    assert "expert_based_on" not in result.context


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selector",
    [{"expert_id": EXPERT}, {"config_name": "developer"}],
    ids=["expert_id", "config_name"],
)
async def test_inline_expert_cannot_be_combined_with_a_catalogue_selector(
    deps, scope, selector
):
    with pytest.raises(HTTPException) as refused:
        await prepare(deps, scope, expert=inline_expert(), **selector)
    assert refused.value.status_code == 400
    assert "inline expert" in refused.value.detail
    deps.resolve_worker_expert.assert_not_awaited()


@pytest.mark.asyncio
async def test_inline_base_alias_config_name_is_not_a_conflict(deps, scope):
    result = await prepare(deps, scope, expert=inline_expert(), config_name="defaults")
    assert result.expert_row is not None and result.config_name == "worker_base"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "image,accepted",
    [("installed:1", True), ("example.invalid/other:1", False), (None, False)],
)
async def test_inline_image_must_be_the_installed_harness(deps, scope, image, accepted):
    expert = inline_expert(image=image)
    if accepted:
        result = await prepare(deps, scope, expert=expert)
        assert result.expert_row is not None
        return
    with pytest.raises(HTTPException) as refused:
        await prepare(deps, scope, expert=expert)
    assert refused.value.status_code == 422
    assert "installed harness" in refused.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "private,fragment",
    [
        ({"prompts": {"system": "Replace the scaffold."}}, "Unknown prompt keys"),
        (
            {"prompts": {"persona": "Hi {prompt_content}"}},
            "reserved prompt placeholders",
        ),
        ({"config": {"llm": {"api_key": "sk-caller"}}}, "credential"),
        ({"config": {"workspace": {"remote": {"host": "x"}}}}, "runtime authority"),
        ({"layers": [{"env_keys": {"K": "v"}}]}, "credential"),
        ({"asset_name": "../../etc"}, "asset"),
        ({"asset_name": "no-such-installed-asset"}, "installed assets"),
        ({"config_name": "bad name; rm -rf /"}, None),
    ],
)
async def test_inline_content_passes_the_expert_write_gates(
    deps, scope, private, fragment
):
    expert = inline_expert()
    expert["inline"]["runtime"]["config"].update(private)
    with pytest.raises(HTTPException) as refused:
        await prepare(deps, scope, expert=expert)
    assert 400 <= refused.value.status_code < 500
    if fragment:
        assert fragment in str(refused.value.detail)
    deps.resolve_worker_expert.assert_not_awaited()


@pytest.mark.asyncio
async def test_inline_tool_vocabulary_smuggling_is_refused(deps, scope):
    expert = inline_expert()
    expert["inline"]["runtime"]["config"]["config"] = {
        "tools": {"canvas": ["run_command"]}
    }
    with pytest.raises(HTTPException) as refused:
        await prepare(deps, scope, expert=expert)
    assert 400 <= refused.value.status_code < 500
