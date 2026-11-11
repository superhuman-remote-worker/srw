"""The shipped session default must start for an approved default-grants user."""

from pathlib import Path
from unittest.mock import AsyncMock

from fastapi import HTTPException
import pytest

from orchestrator import main
from orchestrator.services.config_resolver import resolve_config
from orchestrator.services.default_experts import load_seed_bundle
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import grant_enforcement as grant_enforcement_module


ROOT = Path(__file__).resolve().parents[1]
USER_ID = "8243be24-e054-4543-8f48-178e9e99da8c"


def _resolved_assistant(override):
    assistant = load_seed_bundle(
        ROOT / "config", directory="assistant", expert_type="session"
    )
    capture = {}
    resolve_config(
        base_config_name="session_base",
        expert_row=assistant,
        expert_type="session",
        request_override=override,
        capture=capture,
    )
    return capture["merged_fragment"]


@pytest.fixture
def default_grants_user(monkeypatch):
    db = AsyncMock()
    db.get_user.return_value = {"id": USER_ID, "is_admin": False}
    db.list_grants_for_scopes.return_value = {
        "user": [],
        "project": [],
        "global": [],
    }
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    return db


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["virtual", "sandbox"])
async def test_seeded_default_passes_session_create_grants(
    default_grants_user, backend
):
    fragment = _resolved_assistant({"workspace": {"backend": backend}})

    await grant_enforcement_module.enforce_session_create_grants(
        fragment,
        user_id=USER_ID,
        project_ids=[],
        dependencies=preparation_composition.grant_enforcement_dependencies(
            main.app.state.resources
        ),
    )

    # Since 2026-11-11 (seed 4) the assistant carries the shell tools; on the
    # virtual tier the session's backend filter strips them at runtime.
    assert fragment["tools"]["shell"] == ["run_command", "cancel_command", "shell_read"]
    roster = fragment["subagents"]["roster"]
    assert set(roster) == {"explorer", "reader", "implementer"}
    assert all(entry["tools"]["workspace"] for entry in roster.values())
    # Seed 3's `shell: []` pin is lifted: the implementer keeps its library shell.
    assert roster["implementer"]["tools"]["shell"]
    assert "write_file" in roster["implementer"]["tools"]["workspace"]


@pytest.mark.asyncio
async def test_seeded_default_is_refused_where_shell_is_restricted(
    default_grants_user,
):
    """The assistant carries the shell tools since seed 4, so an install that
    restricts shell_tools refuses a Session on the unmodified default (as for
    any expert with a shell); its administrator gives users a copy without
    the shell group as their default."""
    default_grants_user.list_grants_for_scopes.return_value = {
        "user": [],
        "project": [],
        "global": [{"key": "shell_tools", "value_json": False}],
    }

    with pytest.raises(HTTPException) as exc:
        await grant_enforcement_module.enforce_session_create_grants(
            _resolved_assistant({"workspace": {"backend": "virtual"}}),
            user_id=USER_ID,
            project_ids=[],
            dependencies=preparation_composition.grant_enforcement_dependencies(
                main.app.state.resources
            ),
        )

    assert exc.value.status_code == 422
    assert "shell_tools" in exc.value.detail


def _shell_override(surface):
    tools = {"shell": ["run_command"]}
    return (
        {"tools": tools}
        if surface == "parent"
        else {"subagents": {"roster": {"implementer": {"tools": tools}}}}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["parent", "child"])
async def test_explicit_shell_request_passes_for_default_grants(
    default_grants_user, surface
):
    """shell_tools is granted by default (2026-11-11)."""
    await grant_enforcement_module.enforce_session_create_grants(
        _resolved_assistant(_shell_override(surface)),
        user_id=USER_ID,
        project_ids=[],
        dependencies=preparation_composition.grant_enforcement_dependencies(
            main.app.state.resources
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["parent", "child"])
async def test_explicit_shell_request_fails_where_shell_is_restricted(
    default_grants_user, surface
):
    default_grants_user.list_grants_for_scopes.return_value = {
        "user": [],
        "project": [],
        "global": [{"key": "shell_tools", "value_json": False}],
    }
    fragment = _resolved_assistant(_shell_override(surface))

    with pytest.raises(HTTPException) as exc:
        await grant_enforcement_module.enforce_session_create_grants(
            fragment,
            user_id=USER_ID,
            project_ids=[],
            dependencies=preparation_composition.grant_enforcement_dependencies(
                main.app.state.resources
            ),
        )

    assert exc.value.status_code == 422
    assert "shell_tools" in exc.value.detail
    if surface == "child":
        assert "subagents.roster.implementer.tools.shell" in exc.value.detail
