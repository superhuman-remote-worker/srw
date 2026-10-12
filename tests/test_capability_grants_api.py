"""Contract tests for the orchestrator capability-grants glue (Slice 2): the
save-time and dispatch PEP helpers in orchestrator/main.py, exercised against a
mocked postgres_db. The pure PDP is covered in test_capability_grants.py.

Route-level ``duplicate_expert`` and ``fork_my_expert_default`` tests live
here too — a deliberate exception to this file's usual grain, because the
routing/wiring details they pin (whether the save-time gate runs at all, and
now which *policy* it runs) can't be observed from a bare helper call. They
are not in tests/test_tool_override_boundary.py::TestExpertWriteBoundary
alongside the kill-switch half of the same fix: that module's docstring is
explicit that its validator is "NOT an authorization gate" and that
capability grants are a separate PDP pinned elsewhere, and its
``expert_env`` fixture stubs ``_enforce_expert_save_prelude`` and
``_strip_save_grants`` out specifically so those tests see the shape gate in
isolation. ``kill_switch_env`` stubs **neither** — the real prelude is the
only thing ``test_kill_switch_403s_every_write_route`` asserts, so adding the
stubs there to "restore consistency" would silently delete that test's entire
subject and leave it passing with the kill switch removed. This file, not that
one, is the PDP's home.

2026-08-04 (knowledge-base/knowledge/superpowers/plans/2026-08-04-expert-write-gate-holes.md,
task 3): ``duplicate_expert``'s grants half changed from refuse-outright to
strip-and-report — measured against the real PDP with default grants, refusing
blocked 7 of the 11 shipped experts, including ``scholar``, the route's own
advertised use ("start from scholar"). ``fork_my_expert_default`` followed in
task 4 for the same reason, so the current inventory is **two routes strip,
three refuse** (create, update, import); ``_enforce_save_grants``'s own tests
below assert the refusing three.

Task 4 of the same plan: ``fork_my_expert_default`` (``duplicate`` plus
"select the copy as my default") carried the identical exposure and is fixed
the same way, reusing the exact same two helpers rather than a second
implementation. It has an extra, EARLIER, and INDEPENDENT 403 gate
(``personal_defaults_allowed`` — a different switch from ``user_experts``),
which must still fire, in order, before any stripping — see the tests below
that pin each gate closing without the other.
"""

from tests._expert_catalog import catalogue_service, catalogue_route
from orchestrator.routers import expert_catalog as expert_routes
from orchestrator.schemas import expert_catalog as expert_schemas
from orchestrator.services import expert_authoring as expert_authoring_module


from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

import orchestrator.main as m
from orchestrator.services import grant_enforcement
from orchestrator.application import preparation as preparation_composition
from orchestrator.security import access as access_module
from orchestrator.security import auth as auth_module
from orchestrator.services import grant_enforcement as grant_enforcement_module

_UID = "11111111-1111-1111-1111-111111111111"


def _patch_grants_db(monkeypatch, *, user_rows=None, is_admin=False):
    fake = AsyncMock()
    fake.get_user = AsyncMock(return_value={"id": _UID, "is_admin": is_admin})
    fake.list_grants_for_scopes = AsyncMock(
        return_value={"user": user_rows or [], "project": [], "global": []}
    )
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    return fake


def test_violations_detail_lists_keys():
    assert "shell_tools" in grant_enforcement_module.grant_violations_detail(
        ["shell_tools: tools.shell requires the shell_tools grant"]
    )


@pytest.mark.asyncio
async def test_enforce_save_grants_admin_bypasses():
    # Admin short-circuits before any DB call (no mock needed).
    await grant_enforcement.enforce_save_grants(
        {"tools": {"shell": ["ls"]}},
        user={"id": _UID, "is_admin": True},
        dependencies=preparation_composition.grant_enforcement_dependencies(
            m.app.state.resources
        ),
    )


@pytest.mark.asyncio
async def test_enforce_save_grants_raises_422_for_ungranted(monkeypatch):
    fake = AsyncMock()
    fake.list_grants_for_scopes = AsyncMock(
        return_value={"user": [], "project": [], "global": []}
    )
    fake.get_projects_for_user = AsyncMock(return_value=[])
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    with pytest.raises(HTTPException) as ei:
        await grant_enforcement.enforce_save_grants(
            {"tools": {"shell": ["ls"]}},
            user={"id": _UID, "is_admin": False},
            # Built after the store is swapped, the way the composition root
            # builds it per call.
            dependencies=preparation_composition.grant_enforcement_dependencies(
                m.app.state.resources
            ),
        )
    assert ei.value.status_code == 422 and "shell_tools" in str(ei.value.detail)


@pytest.mark.asyncio
async def test_enforce_dispatch_grants_raises_grantdenied(monkeypatch):
    _patch_grants_db(monkeypatch)
    with pytest.raises(grant_enforcement_module.GrantDenied) as ei:
        await grant_enforcement_module.enforce_dispatch_grants(
            {"tools": {"shell": ["ls"]}},
            runner_user_id=_UID,
            project_ids=[],
            dependencies=preparation_composition.grant_enforcement_dependencies(
                m.app.state.resources
            ),
        )
    assert "shell_tools" in str(ei.value)


@pytest.mark.asyncio
async def test_enforce_dispatch_grants_admin_bypass(monkeypatch):
    _patch_grants_db(monkeypatch, is_admin=True)
    # No violation despite ungranted shell — admin runner bypasses.
    await grant_enforcement_module.enforce_dispatch_grants(
        {"tools": {"shell": ["ls"]}},
        runner_user_id=_UID,
        project_ids=[],
        dependencies=preparation_composition.grant_enforcement_dependencies(
            m.app.state.resources
        ),
    )


@pytest.mark.asyncio
async def test_enforce_dispatch_grants_allows_when_granted(monkeypatch):
    _patch_grants_db(
        monkeypatch,
        user_rows=[
            {"key": "shell_tools", "value_json": True},
            {"key": "delegation", "value_json": True},
        ],
    )
    # Granted shell + delegation -> the worker-base-shaped fragment passes.
    await grant_enforcement_module.enforce_dispatch_grants(
        {"tools": {"shell": ["ls"], "delegation": ["delegate_agent"]}},
        runner_user_id=_UID,
        project_ids=[],
        dependencies=preparation_composition.grant_enforcement_dependencies(
            m.app.state.resources
        ),
    )


@pytest.mark.asyncio
async def test_lifecycle_runner_allows_full_autonomy(monkeypatch):
    _patch_grants_db(monkeypatch)
    await grant_enforcement_module.enforce_dispatch_grants(
        {"autonomy": "full"},
        runner_user_id=_UID,
        project_ids=[],
        runner_kind="lifecycle",
        dependencies=preparation_composition.grant_enforcement_dependencies(
            m.app.state.resources
        ),
    )


@pytest.mark.asyncio
async def test_user_runner_still_denies_full_autonomy(monkeypatch):
    _patch_grants_db(monkeypatch)
    with pytest.raises(grant_enforcement_module.GrantDenied) as ei:
        await grant_enforcement_module.enforce_dispatch_grants(
            {"autonomy": "full"},
            runner_user_id=_UID,
            project_ids=[],
            runner_kind="user",
            dependencies=preparation_composition.grant_enforcement_dependencies(
                m.app.state.resources
            ),
        )
    assert "autonomy_ceiling" in str(ei.value)


@pytest.mark.asyncio
async def test_lifecycle_runner_keeps_owner_capability_limits(monkeypatch):
    _patch_grants_db(monkeypatch)
    with pytest.raises(grant_enforcement_module.GrantDenied) as ei:
        await grant_enforcement_module.enforce_dispatch_grants(
            {"autonomy": "full", "workspace": {"backend": "vm"}},
            runner_user_id=_UID,
            project_ids=[],
            runner_kind="lifecycle",
            dependencies=preparation_composition.grant_enforcement_dependencies(
                m.app.state.resources
            ),
        )
    assert "vm_workspace" in str(ei.value)
    assert "autonomy_ceiling" not in str(ei.value)


@pytest.mark.asyncio
async def test_enforce_session_create_grants_raises_422_for_denied_mode(monkeypatch):
    # Session create/update PEP (Layer 2): a permission_mode above the owner's
    # ceiling (default 'supervised', no grant) → 422 with the violation, so a
    # never-startable session is rejected at the API instead of timing out at
    # provisioning. knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md
    fake = AsyncMock()
    fake.get_user = AsyncMock(return_value={"id": _UID, "is_admin": False})
    fake.list_grants_for_scopes = AsyncMock(
        return_value={"user": [], "project": [], "global": []}
    )
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    with pytest.raises(HTTPException) as ei:
        await grant_enforcement_module.enforce_session_create_grants(
            {"interactive": {"permission_mode": "autonomous"}},
            user_id=_UID,
            project_ids=[],
            dependencies=preparation_composition.grant_enforcement_dependencies(
                m.app.state.resources
            ),
        )
    assert ei.value.status_code == 422
    assert "permission_mode" in str(ei.value.detail)
    assert "autonomous" in str(ei.value.detail)


@pytest.mark.asyncio
async def test_enforce_session_create_grants_admin_bypass(monkeypatch):
    fake = AsyncMock()
    fake.get_user = AsyncMock(return_value={"id": _UID, "is_admin": True})
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    # Admin owner bypasses — autonomous is fine.
    await grant_enforcement_module.enforce_session_create_grants(
        {"interactive": {"permission_mode": "autonomous"}},
        user_id=_UID,
        project_ids=[],
        dependencies=preparation_composition.grant_enforcement_dependencies(
            m.app.state.resources
        ),
    )


@pytest.mark.asyncio
async def test_enforce_session_create_grants_allows_within_ceiling(monkeypatch):
    fake = AsyncMock()
    fake.get_user = AsyncMock(return_value={"id": _UID, "is_admin": False})
    fake.list_grants_for_scopes = AsyncMock(
        return_value={"user": [], "project": [], "global": []}
    )
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    # 'supervised' is the default ceiling → within grants → no raise.
    await grant_enforcement_module.enforce_session_create_grants(
        {"interactive": {"permission_mode": "supervised"}},
        user_id=_UID,
        project_ids=[],
        dependencies=preparation_composition.grant_enforcement_dependencies(
            m.app.state.resources
        ),
    )


@pytest.mark.asyncio
async def test_user_experts_kill_switch_default_enabled(monkeypatch):
    fake = AsyncMock()
    fake.get_system_setting = AsyncMock(return_value=None)  # absent row
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    assert (
        await grant_enforcement_module.user_experts_enabled(
            dependencies=preparation_composition.grant_enforcement_dependencies(
                m.app.state.resources
            )
        )
        is True
    )


@pytest.mark.asyncio
async def test_user_experts_kill_switch_disabled(monkeypatch):
    fake = AsyncMock()
    fake.get_system_setting = AsyncMock(return_value={"value": {"enabled": False}})
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    assert (
        await grant_enforcement_module.user_experts_enabled(
            dependencies=preparation_composition.grant_enforcement_dependencies(
                m.app.state.resources
            )
        )
        is False
    )


@pytest.mark.asyncio
async def test_enforce_job_create_grants_raises_422_for_ungranted(monkeypatch):
    # Job submit-time PEP: an ungranted override is refused at create. Without
    # it the job is accepted and only denied at dispatch, which marks it failed
    # — so a session agent that created it has already reported success.
    fake = AsyncMock()
    fake.get_user = AsyncMock(return_value={"id": _UID, "is_admin": False})
    fake.list_grants_for_scopes = AsyncMock(
        return_value={"user": [], "project": [], "global": []}
    )
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    with pytest.raises(HTTPException) as ei:
        await grant_enforcement_module.enforce_job_create_grants(
            {"workspace": {"backend": "vm"}},
            user_id=_UID,
            project_ids=[],
            dependencies=preparation_composition.grant_enforcement_dependencies(
                m.app.state.resources
            ),
        )
    assert ei.value.status_code == 422
    assert "vm_workspace" in str(ei.value.detail)


@pytest.mark.asyncio
async def test_enforce_job_create_grants_allows_granted_override(monkeypatch):
    _patch_grants_db(
        monkeypatch,
        user_rows=[{"key": "vm_workspace", "value_json": True}],
    )
    await grant_enforcement_module.enforce_job_create_grants(
        {"workspace": {"backend": "vm"}},
        user_id=_UID,
        project_ids=[],
        dependencies=preparation_composition.grant_enforcement_dependencies(
            m.app.state.resources
        ),
    )


@pytest.mark.asyncio
async def test_enforce_job_create_grants_admin_bypass(monkeypatch):
    fake = AsyncMock()
    fake.get_user = AsyncMock(return_value={"id": _UID, "is_admin": True})
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    await grant_enforcement_module.enforce_job_create_grants(
        {"autonomy": "full"},
        user_id=_UID,
        project_ids=[],
        dependencies=preparation_composition.grant_enforcement_dependencies(
            m.app.state.resources
        ),
    )


@pytest.mark.asyncio
async def test_enforce_job_create_grants_skips_userless_and_empty(monkeypatch):
    """Userless system children have no principal whose grants to resolve, and
    an empty override has nothing to check — neither may touch the DB."""
    fake = AsyncMock()
    fake.get_user = AsyncMock(side_effect=AssertionError("must not hit the DB"))
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    await grant_enforcement_module.enforce_job_create_grants(
        {"workspace": {"backend": "vm"}},
        user_id=None,
        project_ids=[],
        dependencies=preparation_composition.grant_enforcement_dependencies(
            m.app.state.resources
        ),
    )
    await grant_enforcement_module.enforce_job_create_grants(
        None,
        user_id=_UID,
        project_ids=[],
        dependencies=preparation_composition.grant_enforcement_dependencies(
            m.app.state.resources
        ),
    )
    await grant_enforcement_module.enforce_job_create_grants(
        {},
        user_id=_UID,
        project_ids=[],
        dependencies=preparation_composition.grant_enforcement_dependencies(
            m.app.state.resources
        ),
    )


@pytest.mark.asyncio
async def test_duplicate_expert_strips_an_ungranted_tool_and_reports_it(monkeypatch):
    """The grants half of the fifth expert-write route
    (knowledge-base/knowledge/issues/duplicate_expert_bypasses_user_experts_kill_switch.md), now
    strip-and-report rather than refuse (task 3 of the 2026-08-04 plan above):
    `duplicate` is the one route that forks a config someone else authored —
    visibility, not ownership, is the read check, so the source row's
    `tools.shell` grant requirement is never the copier's own to have
    satisfied, and refusing blocked the fork instead of just the shell tool.

    This was `test_duplicate_expert_denies_a_grant_the_source_config_requires`
    before task 3 — same source row, same missing grant, opposite outcome by
    design: 200 now, with the offending grant named in `dropped` and gone
    from the stored config, instead of a 422 with nothing stored.
    """
    source_row = {
        "id": str(uuid4()),
        "owner_id": str(uuid4()),  # someone else's row; visible, not owned
        "expert_type": "session",
        "name": "shared",
        "display_name": "Shared",
        "icon": "smart_toy",
        "color": "#6B7280",
        # A real, correctly-categorised (shape-valid) shell tool list — not a
        # smuggle — that the copier's own grants do not cover: shell_tools
        # defaults to deny in the CATALOG.
        "config": {"tools": {"shell": ["run_command"], "git": ["git_status"]}},
    }
    fake = AsyncMock()
    fake.get_expert_visible_by_id = AsyncMock(return_value=source_row)
    fake.list_grants_for_scopes = AsyncMock(
        return_value={"user": [], "project": [], "global": []}
    )
    fake.create_expert = AsyncMock(return_value={"id": "forked-id"})
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)

    non_admin_user = {"id": _UID, "is_admin": False}
    monkeypatch.setattr(
        auth_module, "require_approved_user", AsyncMock(return_value=non_admin_user)
    )
    monkeypatch.setattr(
        access_module, "user_visible_project_ids", AsyncMock(return_value=[])
    )
    # Kill switch ON: this test isolates the grants half, not the switch.
    monkeypatch.setattr(
        grant_enforcement_module, "user_experts_enabled", AsyncMock(return_value=True)
    )

    result = await catalogue_route(expert_routes.duplicate_expert)(
        MagicMock(), str(uuid4())
    )

    assert result == {"id": "forked-id", "dropped": ["shell_tools"]}
    fake.create_expert.assert_awaited_once()
    assert fake.create_expert.await_args.kwargs["config"] == {
        "tools": {"git": ["git_status"]}
    }
    # Non-admin path proof (the free-admin-bypass trap this task's plan calls
    # out, tool_configuration_deferred_findings §4.1): is_admin is explicit
    # False above, and the grants resolver returns immediately for an admin
    # WITHOUT touching the DB — so an awaited grants lookup is direct evidence
    # this ran the non-admin branch rather than a Mock truthy-bypass quietly
    # making the assertions above vacuous.
    assert non_admin_user["is_admin"] is False
    fake.list_grants_for_scopes.assert_awaited_once()


@pytest.mark.asyncio
async def test_duplicate_expert_still_forks_when_the_gate_allows(monkeypatch):
    """The new call is a gate, not a detour. Neither denial test above can
    show the added line leaves the working case alone — one exercises the
    switch, the other the grants 422 — so this pins the third outcome: a
    copier who clears the kill switch and holds every grant the source needs
    still gets the forked row back. Also guards the specific instruction to
    pass `src["config"]` (not `src.get("config") or {}`): a typo'd name here
    would raise before `_create_forked_expert` ever ran.

    `dropped` must be present and empty: nothing in this config is
    grant-gated, so the strip-and-report path (task 3) has nothing to strip.
    """
    source_row = {
        "id": str(uuid4()),
        "owner_id": str(uuid4()),  # visible, still not the copier's own row
        "expert_type": "session",
        "name": "shared",
        "display_name": "Shared",
        "icon": "smart_toy",
        "color": "#6B7280",
        "config": {"llm": {"model": "gemma-4-moe"}},  # nothing grant-gated
    }
    forked_row = {"id": str(uuid4()), "name": "shared-copy"}
    fake = AsyncMock()
    fake.get_expert_visible_by_id = AsyncMock(return_value=source_row)
    fake.list_grants_for_scopes = AsyncMock(
        return_value={"user": [], "project": [], "global": []}
    )
    fake.create_expert = AsyncMock(return_value=forked_row)
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": _UID, "is_admin": False}),
    )
    monkeypatch.setattr(
        access_module, "user_visible_project_ids", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        grant_enforcement_module, "user_experts_enabled", AsyncMock(return_value=True)
    )

    result = await catalogue_route(expert_routes.duplicate_expert)(
        MagicMock(), str(uuid4())
    )

    assert result == {**forked_row, "dropped": []}
    fake.create_expert.assert_awaited_once()
    assert fake.create_expert.await_args.kwargs["config"] == {
        "llm": {"model": "gemma-4-moe"}
    }


def _duplicate_env(monkeypatch, *, source_row, grant_rows=(), is_admin=False):
    """Shared scaffold for the strip-and-report tests below: a visible source
    row, a grants lookup returning `grant_rows` for the user scope, and the
    kill switch held open (each of these tests isolates the grants half, same
    as the two above)."""
    fake = AsyncMock()
    fake.get_expert_visible_by_id = AsyncMock(return_value=source_row)
    fake.list_grants_for_scopes = AsyncMock(
        return_value={"user": list(grant_rows), "project": [], "global": []}
    )
    fake.create_expert = AsyncMock(side_effect=lambda **kw: {"id": "forked-id", **kw})
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": _UID, "is_admin": is_admin}),
    )
    monkeypatch.setattr(
        access_module, "user_visible_project_ids", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        grant_enforcement_module, "user_experts_enabled", AsyncMock(return_value=True)
    )
    return fake


@pytest.mark.asyncio
async def test_duplicate_expert_holder_of_the_grant_gets_an_unmodified_copy(
    monkeypatch,
):
    """Test group 5 of task 3: stripping must not fire for a copier who
    already holds what the source needs — otherwise it could be firing for
    everyone and test_duplicate_expert_strips_an_ungranted_tool_and_reports_it
    above would not be able to tell the difference."""
    config = {"tools": {"shell": ["run_command"], "git": ["git_status"]}}
    fake = _duplicate_env(
        monkeypatch,
        source_row={
            "id": str(uuid4()),
            "owner_id": str(uuid4()),
            "expert_type": "session",
            "name": "shared",
            "display_name": "Shared",
            "icon": "smart_toy",
            "color": "#6B7280",
            "config": config,
        },
        grant_rows=[{"key": "shell_tools", "value_json": True}],
    )

    result = await catalogue_route(expert_routes.duplicate_expert)(
        MagicMock(), str(uuid4())
    )

    assert result["dropped"] == []
    assert fake.create_expert.await_args.kwargs["config"] == config


@pytest.mark.asyncio
async def test_duplicate_expert_admin_copier_bypasses_and_reports_nothing_dropped(
    monkeypatch,
):
    """Admins bypass grants entirely (same as `_enforce_save_grants`) — the
    strip-and-report path must short-circuit the same way, not merely happen
    to strip nothing because an admin's resolved grants are permissive."""
    config = {"tools": {"shell": ["run_command"]}}
    fake = _duplicate_env(
        monkeypatch,
        source_row={
            "id": str(uuid4()),
            "owner_id": str(uuid4()),
            "expert_type": "session",
            "name": "shared",
            "display_name": "Shared",
            "icon": "smart_toy",
            "color": "#6B7280",
            "config": config,
        },
        is_admin=True,
    )

    result = await catalogue_route(expert_routes.duplicate_expert)(
        MagicMock(), str(uuid4())
    )

    assert result["dropped"] == []
    assert fake.create_expert.await_args.kwargs["config"] == config
    fake.list_grants_for_scopes.assert_not_awaited()  # admin bypass, no DB grants lookup


@pytest.mark.asyncio
async def test_duplicate_expert_safety_recheck_fires_when_the_strip_map_misses(
    monkeypatch,
):
    """Test group 3 of task 3 — THE SAFETY PROPERTY. `strip_to_grants` is
    forced to a no-op (as if a rule were missing from its map); the route
    must still 422 rather than create a row, because `_strip_save_grants`
    re-runs `evaluate` on whatever comes back and refuses on any residual
    violation. This is what makes an incomplete strip map merely a false
    refusal, never a permitted escape.
    """
    import shared.runtime.core.capability_grants as capability_grants

    monkeypatch.setattr(
        capability_grants,
        "strip_to_grants",
        lambda fragment, grants: (fragment, []),  # claims nothing to drop
    )
    fake = _duplicate_env(
        monkeypatch,
        source_row={
            "id": str(uuid4()),
            "owner_id": str(uuid4()),
            "expert_type": "session",
            "name": "shared",
            "display_name": "Shared",
            "icon": "smart_toy",
            "color": "#6B7280",
            "config": {"tools": {"shell": ["run_command"]}},
        },
    )

    with pytest.raises(HTTPException) as ei:
        await catalogue_route(expert_routes.duplicate_expert)(MagicMock(), str(uuid4()))

    assert ei.value.status_code == 422
    assert "shell_tools" in ei.value.detail
    fake.create_expert.assert_not_awaited()


@pytest.mark.asyncio
async def test_duplicate_expert_scholar_end_to_end_for_a_default_grants_user(
    monkeypatch,
):
    """Test group 4 of task 3, and the one that proves the reported defect is
    actually fixed: `scholar` is the expert the route's own docstring names
    ("start from scholar"), a bundled (disk) expert, not a DB row — so this
    goes through `_bundled_expert_bundle`, the real config.yaml, unlike every
    other test in this file. A default-grants non-admin gets a 200, the
    stored row has no `tools.shell` and no `tools.delegation`, and the
    response names both as dropped.
    """
    fake = AsyncMock()
    fake.list_grants_for_scopes = AsyncMock(
        return_value={"user": [], "project": [], "global": []}
    )
    fake.create_expert = AsyncMock(side_effect=lambda **kw: {"id": "forked-id", **kw})
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": _UID, "is_admin": False}),
    )
    monkeypatch.setattr(
        grant_enforcement_module, "user_experts_enabled", AsyncMock(return_value=True)
    )
    # A bundled (disk) expert_id skips the visibility lookup entirely (it's
    # only for DB rows) — this is the copier's OWN project scope, resolved
    # while computing THEIR grants (_grant_project_ids), and is exercised
    # regardless of the source expert's origin. Explicit here (rather than
    # relying on AsyncMock's default empty-iterable return) so the "no
    # project-level grant" premise is visible, not incidental.
    monkeypatch.setattr(
        access_module, "user_visible_project_ids", AsyncMock(return_value=[])
    )

    result = await catalogue_route(expert_routes.duplicate_expert)(
        MagicMock(), "scholar"
    )

    assert set(result["dropped"]) == {"shell_tools", "delegation"}
    stored = fake.create_expert.await_args.kwargs["config"]
    assert "shell" not in stored.get("tools", {})
    assert "delegation" not in stored.get("tools", {})
    assert "enabled" not in stored.get("delegation", {})
    # Scholar's delegation.max_concurrent is not the violation (only .enabled
    # is) and must survive — proof the strip did not take the whole settings
    # dict.
    assert stored["delegation"]["max_concurrent"] == 4


# The brief's own measurement: default grants (shell_tools=False,
# delegation=False) refuse 7 of the 11 shipped experts before task 3 —
# scholar, developer, critic, bughunter and product-qa (shell + delegation),
# plus designer and designer-interactive (shell). This is the after: every one
# forks (200) for the same default-grants non-admin, and drops exactly the
# grants the brief measured, no more and no less.
_PREVIOUSLY_REFUSED_SHIPPED_EXPERTS = {
    "scholar": {"shell_tools", "delegation"},
    "developer": {"shell_tools", "delegation"},
    "critic": {"shell_tools", "delegation"},
    "bughunter": {"shell_tools", "delegation"},
    "designer": {"shell_tools"},
    "designer-interactive": {"shell_tools"},
    "product-qa": {"shell_tools", "delegation"},
}


@pytest.mark.asyncio
@pytest.mark.parametrize("expert_id", sorted(_PREVIOUSLY_REFUSED_SHIPPED_EXPERTS))
async def test_previously_refused_shipped_experts_now_fork_for_default_grants(
    monkeypatch, expert_id
):
    """Measured before/after for every one of the 7 shipped experts the brief
    names as blocked by Task 1's refuse-outright grants check. Before: 422,
    nothing stored (measured directly against this repo's real bundled
    configs while writing this fix — see the task report). After: 200,
    dropped is exactly the grant set named in the brief, never more (an
    over-broad strip would silently degrade an expert further than its own
    config warrants) and never less (that would be the escape the safety
    re-check exists to close).
    """
    fake = AsyncMock()
    fake.list_grants_for_scopes = AsyncMock(
        return_value={"user": [], "project": [], "global": []}
    )
    fake.create_expert = AsyncMock(side_effect=lambda **kw: {"id": "forked-id", **kw})
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": _UID, "is_admin": False}),
    )
    monkeypatch.setattr(
        grant_enforcement_module, "user_experts_enabled", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(
        access_module, "user_visible_project_ids", AsyncMock(return_value=[])
    )

    result = await catalogue_route(expert_routes.duplicate_expert)(
        MagicMock(), expert_id
    )

    assert set(result["dropped"]) == _PREVIOUSLY_REFUSED_SHIPPED_EXPERTS[expert_id]
    stored = fake.create_expert.await_args.kwargs["config"]
    # The stored config must actually be clean against the same default
    # grants, not merely have a `dropped` list that claims so.
    from shared.runtime.core.capability_grants import CATALOG, evaluate

    default_grants = {k: v["default"] for k, v in CATALOG.items()}
    assert evaluate(stored, default_grants) == []


# =============================================================================
# fork_my_expert_default (task 4 of the 2026-08-04 plan): `duplicate` plus
# "select the copy as my default", reusing duplicate_expert's exact two
# helpers (`_enforce_expert_save_prelude` + `_strip_save_grants`) rather than
# a second implementation. Its extra wrinkle: an earlier, INDEPENDENT 403 gate
# (`personal_defaults_allowed` — a different switch from `user_experts`) that
# must still fire, in order, before any stripping.
# =============================================================================


def _fake_fork_row(*, user_id, expert_type, source):
    """What a real `fork_and_set_user_expert_default` would return: a fresh
    experts row shaped for `_default_expert_summary`. A fresh id per call so
    a test can tell "the new row" apart from the source or anything else."""
    return {
        "id": str(uuid4()),
        "name": f"{source['name']}-default",
        "display_name": f"{source['display_name']} (My Default)",
        "expert_type": expert_type,
        "owner_id": user_id,
        "description": source.get("description"),
        "icon": source.get("icon", "smart_toy"),
        "color": source.get("color", "#6B7280"),
        "tags": source.get("tags") or [],
        "is_global": False,
        "managed_key": None,
        "config": source.get("config") or {},
    }


def _fork_env(
    monkeypatch,
    *,
    grant_rows=(),
    is_admin=False,
    personal_defaults_ok: bool | None = None,
    kill_switch_on=True,
):
    """Shared scaffold, the fork_my_expert_default mirror of `_duplicate_env`
    above. `personal_defaults_ok=None` (the default) leaves
    `personal_defaults_allowed` UNSTUBBED — it runs for real against this
    same fake grants DB, which is a stronger test than stubbing it whenever a
    test doesn't need to isolate that gate specifically (empty/default grants
    resolve `personal_default_experts` to its catalog default, True, and an
    admin short-circuits it without touching the DB either way). Pass an
    explicit bool only to isolate that gate from the kill switch and the
    strip, the way the two 403 tests below do.
    """
    fake = AsyncMock()
    fake.list_grants_for_scopes = AsyncMock(
        return_value={"user": list(grant_rows), "project": [], "global": []}
    )
    fake.fork_and_set_user_expert_default = AsyncMock(
        side_effect=lambda **kw: _fake_fork_row(**kw)
    )
    monkeypatch.setattr(m.app.state.resources, "postgres_db", fake)
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": _UID, "is_admin": is_admin}),
    )
    monkeypatch.setattr(
        access_module, "user_visible_project_ids", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        grant_enforcement_module,
        "user_experts_enabled",
        AsyncMock(return_value=kill_switch_on),
    )
    if personal_defaults_ok is not None:
        monkeypatch.setattr(
            expert_authoring_module,
            "personal_defaults_allowed",
            AsyncMock(return_value=personal_defaults_ok),
        )
    return fake


def _db_selection(config: dict) -> SimpleNamespace:
    """An `ExpertSelection`-shaped double for the `resolve_root_expert`
    branch of `source` — the route only ever reads `.expert`."""
    return SimpleNamespace(
        expert={
            "id": str(uuid4()),
            "owner_id": str(uuid4()),
            "expert_type": "session",
            "name": "shared",
            "display_name": "Shared",
            "icon": "smart_toy",
            "color": "#6B7280",
            "config": config,
        }
    )


@pytest.mark.asyncio
async def test_fork_my_expert_default_strips_a_bundled_source_and_reports_it(
    monkeypatch,
):
    """Test group 1: a default-grants non-admin forks `scholar` — a BUNDLED
    (disk) source, reached via `_bundled_expert_bundle`, not a DB row — and
    gets 200 with `dropped` naming both grants scholar's real config.yaml
    needs (shell_tools, delegation) that default grants withhold. The stored
    row is read back from the mock's own call args, not the response, and
    must actually lack both: a route that reports `dropped` while persisting
    the original would be worse than the refusal it replaced.
    """
    fake = _fork_env(monkeypatch, grant_rows=[])

    result = await catalogue_route(expert_routes.fork_my_expert_default)(
        MagicMock(),
        "worker",
        expert_schemas.ExpertDefaultForkRequest(expert_id="scholar"),
    )

    assert set(result["dropped"]) == {"shell_tools", "delegation"}
    assert result["source"] == "user"
    stored = fake.fork_and_set_user_expert_default.await_args.kwargs["source"]["config"]
    assert "shell" not in stored.get("tools", {})
    assert "delegation" not in stored.get("tools", {})
    assert "enabled" not in stored.get("delegation", {})
    # delegation.max_concurrent is not the violation (only .enabled is) and
    # must survive — proof the strip did not take the whole settings dict.
    assert stored["delegation"]["max_concurrent"] == 4


@pytest.mark.asyncio
async def test_fork_my_expert_default_strips_a_db_source_and_reports_it(monkeypatch):
    """Test group 2: the OTHER path to `source` — `resolve_root_expert`,
    a DB row (no explicit `expert_id` posted, so this resolves someone else's
    personal or the application default), not a bundled one. Same
    strip-and-report outcome, proving the fix covers both branches."""
    fake = _fork_env(monkeypatch, grant_rows=[])
    monkeypatch.setattr(
        expert_authoring_module,
        "resolve_root_expert",
        AsyncMock(
            return_value=_db_selection(
                {"tools": {"shell": ["run_command"], "git": ["git_status"]}}
            )
        ),
    )

    result = await catalogue_route(expert_routes.fork_my_expert_default)(
        MagicMock(), "session", expert_schemas.ExpertDefaultForkRequest()
    )

    assert result["dropped"] == ["shell_tools"]
    stored = fake.fork_and_set_user_expert_default.await_args.kwargs["source"]["config"]
    assert stored == {"tools": {"git": ["git_status"]}}


@pytest.mark.asyncio
async def test_fork_my_expert_default_holder_of_the_grant_gets_an_unmodified_fork(
    monkeypatch,
):
    """Test group 3: a user who already holds what the source needs must get
    it back byte-for-byte, with `dropped` empty — otherwise stripping could be
    firing unconditionally and the two tests above would not be able to tell
    the difference."""
    config = {"tools": {"shell": ["run_command"], "git": ["git_status"]}}
    fake = _fork_env(
        monkeypatch, grant_rows=[{"key": "shell_tools", "value_json": True}]
    )
    monkeypatch.setattr(
        expert_authoring_module,
        "resolve_root_expert",
        AsyncMock(return_value=_db_selection(config)),
    )

    result = await catalogue_route(expert_routes.fork_my_expert_default)(
        MagicMock(), "session", expert_schemas.ExpertDefaultForkRequest()
    )

    assert result["dropped"] == []
    assert (
        fake.fork_and_set_user_expert_default.await_args.kwargs["source"]["config"]
        == config
    )


@pytest.mark.asyncio
async def test_fork_my_expert_default_403s_when_personal_defaults_are_disabled(
    monkeypatch,
):
    """Test group 4, gate 1: `personal_defaults_allowed` is this route's OWN
    switch, independent of the shared `user_experts` kill switch (next test).
    Isolated here (`personal_defaults_ok=False`, stubbed directly) so the
    assertions below are unambiguous: neither the source (`resolve_root_expert`)
    nor the grants DB (which stripping would need) are ever touched, proving
    the gate returns before either — "before any strip" the brief asks for,
    not just "eventually a 403 somewhere".
    """
    fake = _fork_env(monkeypatch, personal_defaults_ok=False)
    resolve_mock = AsyncMock(
        return_value=_db_selection({"tools": {"shell": ["run_command"]}})
    )
    monkeypatch.setattr(expert_authoring_module, "resolve_root_expert", resolve_mock)

    with pytest.raises(HTTPException) as ei:
        await catalogue_route(expert_routes.fork_my_expert_default)(
            MagicMock(), "worker", expert_schemas.ExpertDefaultForkRequest()
        )

    assert ei.value.status_code == 403
    assert ei.value.detail == "Your administrator has disabled personal default experts"
    fake.fork_and_set_user_expert_default.assert_not_awaited()
    fake.list_grants_for_scopes.assert_not_awaited()
    resolve_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_fork_my_expert_default_403s_when_the_kill_switch_is_off(monkeypatch):
    """Test group 5, gate 2 — the shared switch, not this route's own.
    Already parametrised across all five expert-write routes in
    tests/test_tool_override_boundary.py::test_kill_switch_403s_every_write_route
    (which must keep passing unchanged); this pins the same property from the
    strip-and-report side of this file, so a change that reordered the two
    gates or ran the strip before either would be caught from both angles.
    `personal_defaults_ok=True` is stubbed directly (not left real) so the
    grants DB is untouched by gate 1 here, and the "never awaited" assertion
    below unambiguously means the strip itself never ran — the source
    (`resolve_root_expert`) DOES run first (unchanged pre-existing order: a
    bad/missing source is a 404/422 regardless of the kill switch), so unlike
    the gate-1 test above, this one does not assert that call away.
    """
    fake = _fork_env(monkeypatch, personal_defaults_ok=True, kill_switch_on=False)
    monkeypatch.setattr(
        expert_authoring_module,
        "resolve_root_expert",
        AsyncMock(return_value=_db_selection({"tools": {"shell": ["run_command"]}})),
    )

    with pytest.raises(HTTPException) as ei:
        await catalogue_route(expert_routes.fork_my_expert_default)(
            MagicMock(), "worker", expert_schemas.ExpertDefaultForkRequest()
        )

    assert ei.value.status_code == 403
    assert ei.value.detail == "User-defined experts are disabled by the administrator"
    fake.fork_and_set_user_expert_default.assert_not_awaited()
    fake.list_grants_for_scopes.assert_not_awaited()


@pytest.mark.asyncio
async def test_fork_my_expert_default_safety_recheck_fires_when_the_strip_map_misses(
    monkeypatch,
):
    """Test group 6 — THE SAFETY PROPERTY, same as duplicate_expert's version
    of this test above. `strip_to_grants` is forced to a no-op (as if a rule
    were missing from its map); `_strip_save_grants` must still re-run
    `evaluate` on whatever comes back and refuse (422) rather than let the
    route persist a config its own grants forbid.
    """
    import shared.runtime.core.capability_grants as capability_grants

    monkeypatch.setattr(
        capability_grants,
        "strip_to_grants",
        lambda fragment, grants: (fragment, []),  # claims nothing to drop
    )
    fake = _fork_env(monkeypatch, grant_rows=[])

    with pytest.raises(HTTPException) as ei:
        await catalogue_route(expert_routes.fork_my_expert_default)(
            MagicMock(),
            "worker",
            expert_schemas.ExpertDefaultForkRequest(expert_id="scholar"),
        )

    assert ei.value.status_code == 422
    assert "shell_tools" in ei.value.detail
    fake.fork_and_set_user_expert_default.assert_not_awaited()


@pytest.mark.asyncio
async def test_fork_my_expert_default_actually_sets_the_personal_default(
    monkeypatch,
):
    """Test group 7 — the fork's whole point, not merely a side effect of it.
    A tiny stateful double (not just an AsyncMock recording call args) so an
    independent read AFTER the route returns proves the write actually took:
    the personal-default pointer now names the SAME id the response reports,
    and resolves to the STRIPPED config, not the source's original.
    """
    fake = _fork_env(monkeypatch, grant_rows=[])
    pointer: dict[str, Any] = {"row": None}

    async def _fork_and_set(*, user_id, expert_type, source):
        row = _fake_fork_row(user_id=user_id, expert_type=expert_type, source=source)
        pointer["row"] = row
        return row

    async def _get_user_expert_default(*, user_id, expert_type):
        return pointer["row"]

    fake.fork_and_set_user_expert_default = AsyncMock(side_effect=_fork_and_set)
    fake.get_user_expert_default = AsyncMock(side_effect=_get_user_expert_default)

    before = await fake.get_user_expert_default(user_id=_UID, expert_type="worker")
    assert before is None

    result = await catalogue_route(expert_routes.fork_my_expert_default)(
        MagicMock(),
        "worker",
        expert_schemas.ExpertDefaultForkRequest(expert_id="scholar"),
    )

    after = await fake.get_user_expert_default(user_id=_UID, expert_type="worker")
    assert after is not None
    assert after["id"] == result["default"]["id"]
    assert "shell" not in after["config"].get("tools", {})
    assert "delegation" not in after["config"].get("tools", {})


@pytest.mark.asyncio
async def test_fork_my_expert_default_admin_bypasses_and_reports_nothing_dropped(
    monkeypatch,
):
    """`_resolve_user_save_grants` returns `None` for an admin — the bypass
    sentinel both `_enforce_save_grants` and `_strip_save_grants` share (see
    that helper's own docstring in orchestrator/main.py). Admins must get an
    unstripped fork of `scholar` with `dropped: []`, not merely happen to
    pass because an admin's resolved grants are permissive. The grants DB is
    never touched — the same non-vacuous-admin-path proof
    test_duplicate_expert_admin_copier_bypasses_and_reports_nothing_dropped
    above uses, and here it also covers `personal_defaults_allowed`'s own
    `is_admin` short-circuit, left real (not stubbed) for this one test.
    """
    fake = _fork_env(monkeypatch, is_admin=True)
    # The untouched fragment to compare against — fetched the same way the
    # route does, rather than a hand-copied literal that could drift from the
    # real config.yaml (as `tools.shell` already has once: `cancel_command`
    # joined `run_command` there since this file's other admin-bypass test
    # was last touched).
    expected = catalogue_service().validate_expert_fragment(
        catalogue_service().bundled_expert_bundle("scholar")["config"]
    )

    result = await catalogue_route(expert_routes.fork_my_expert_default)(
        MagicMock(),
        "worker",
        expert_schemas.ExpertDefaultForkRequest(expert_id="scholar"),
    )

    assert result["dropped"] == []
    stored = fake.fork_and_set_user_expert_default.await_args.kwargs["source"]["config"]
    assert stored == expected
    fake.list_grants_for_scopes.assert_not_awaited()


#: Mirrors _PREVIOUSLY_REFUSED_SHIPPED_EXPERTS's expert_type for each shipped
#: expert (derived from each config.yaml's own $extends — worker_base vs
#: session_base), since fork_my_expert_default takes expert_type as a path
#: param the bundled branch cross-checks against the source.
_SHIPPED_EXPERT_TYPE = {
    "scholar": "worker",
    "developer": "worker",
    "critic": "worker",
    "bughunter": "worker",
    "designer": "worker",
    "designer-interactive": "session",
    "product-qa": "worker",
}


@pytest.mark.asyncio
@pytest.mark.parametrize("expert_id", sorted(_PREVIOUSLY_REFUSED_SHIPPED_EXPERTS))
async def test_previously_refused_shipped_experts_now_fork_as_default(
    monkeypatch, expert_id
):
    """The measured before/after for forking each of the 7 shipped experts as
    a personal default, same measurement as duplicate_expert's version of
    this test above (this route shared the exact same refuse-outright call
    before this fix, so the "before" — 422, nothing stored — is identical).
    After: 200, `dropped` is exactly the grant set the brief measured, and
    the stored config is actually clean against the same default grants, not
    merely reported as such.
    """
    fake = _fork_env(monkeypatch, grant_rows=[])

    result = await catalogue_route(expert_routes.fork_my_expert_default)(
        MagicMock(),
        _SHIPPED_EXPERT_TYPE[expert_id],
        expert_schemas.ExpertDefaultForkRequest(expert_id=expert_id),
    )

    assert set(result["dropped"]) == _PREVIOUSLY_REFUSED_SHIPPED_EXPERTS[expert_id]
    stored = fake.fork_and_set_user_expert_default.await_args.kwargs["source"]["config"]
    from shared.runtime.core.capability_grants import CATALOG, evaluate

    default_grants = {k: v["default"] for k, v in CATALOG.items()}
    assert evaluate(stored, default_grants) == []
