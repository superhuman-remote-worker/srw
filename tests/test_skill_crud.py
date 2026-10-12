"""Unit tests for DB-backed skill authoring (Slice 1).

Guards the request models + the SKILL.md parse/deny gate + path validation.
Full endpoint integration is verified live on k3d (see the plan's Task 12)
because the auth dependency + asyncpg store are not hermetically mockable here.
Local env may be noisy (Py3.14); CI (Py3.12) is the authoritative gate.
"""

from tests._expert_catalog import catalogue_service, catalogue_state
from orchestrator.schemas import expert_catalog as expert_schemas
from orchestrator.services import expert_catalog as expert_catalog_module


import pytest
from fastapi import HTTPException
from unittest.mock import AsyncMock

import orchestrator.main as orchestrator_main
from orchestrator.services import deployment_gates as deployment_gates_module


GOOD = "---\nname: my-helper\ndescription: Use when X.\n---\n# My Helper\n"


def test_skill_create_minimal_ok():
    s = expert_schemas.SkillCreate(files={"SKILL.md": GOOD})
    assert s.icon == "extension" and s.color == "#6B7280" and s.tags == []


def test_skill_create_rejects_bad_color():
    with pytest.raises(Exception):
        expert_schemas.SkillCreate(files={"SKILL.md": GOOD}, color="red")


def test_skill_update_excludes_name():
    assert "name" not in expert_schemas.SkillUpdate.model_fields


def test_parse_bundle_returns_name_and_description():
    name, desc, files = expert_catalog_module.parse_skill_bundle({"SKILL.md": GOOD})
    assert name == "my-helper"
    assert desc == "Use when X."
    assert "SKILL.md" in files


def test_parse_bundle_rejects_missing_skill_md():
    with pytest.raises(HTTPException) as ei:
        expert_catalog_module.parse_skill_bundle({"references/x.md": "y"})
    assert ei.value.status_code == 422


def test_parse_bundle_rejects_path_traversal():
    with pytest.raises(HTTPException) as ei:
        expert_catalog_module.parse_skill_bundle({"SKILL.md": GOOD, "../evil": "x"})
    assert ei.value.status_code == 422


def test_parse_bundle_rejects_bad_slug():
    bad = "---\nname: Bad Name\ndescription: y\n---\nbody\n"
    with pytest.raises(HTTPException) as ei:
        expert_catalog_module.parse_skill_bundle({"SKILL.md": bad})
    assert ei.value.status_code == 422


def test_parse_bundle_rejects_managed_app_guide_name():
    reserved = (
        "---\nname: app-guide\ndescription: replacement\n---\n"
        "USER CONTROLLED PRODUCT CLAIMS\n"
    )

    with pytest.raises(HTTPException) as ei:
        expert_catalog_module.parse_skill_bundle({"SKILL.md": reserved})

    assert ei.value.status_code == 422
    assert "reserved" in str(ei.value.detail).lower()
    assert "distinct name" in str(ei.value.detail).lower()


def test_validate_frontmatter_blocks_credentials():
    with pytest.raises(HTTPException) as ei:
        expert_catalog_module.validate_skill_frontmatter(
            {"connections": {"token": "secret"}}
        )
    assert ei.value.status_code == 422


def test_validate_frontmatter_allows_clean():
    expert_catalog_module.validate_skill_frontmatter({"name": "x", "description": "y"})


@pytest.mark.asyncio
async def test_ordinary_resolved_catalog_excludes_managed_app_guide(monkeypatch):
    monkeypatch.setattr(deployment_gates_module, "is_skills_db_enabled", lambda: True)
    monkeypatch.setattr(catalogue_state(), "skills", catalogue_service().scan_skills())
    monkeypatch.setattr(
        orchestrator_main.app.state.resources.postgres_db,
        "list_skills_visible",
        AsyncMock(return_value=[]),
    )

    payload = await catalogue_service().gather_in_scope_skills("user-1")

    names = {item["name"] for item in payload["menu"]}
    assert "app-guide" not in names
    assert names, "ordinary bundled skills should still resolve"


def test_scan_skills_hides_catalog_hidden_skills():
    """``catalog: hidden`` (the worker's phase skills, U2) keeps a bundled skill
    out of the scanned catalog — the model-invoked menu, the session list and
    the cockpit's bundled list all read this cache — while the binding channel
    still freezes it straight from disk."""
    from shared.runtime.core.loader import (
        PHASE_SKILL_NAMES,
        load_agent_config,
        resolve_config_path,
    )
    from shared.runtime.core.loader import serialize_resolved_config

    names = {s.name for s in catalogue_service().scan_skills()}
    assert names, "bundled skills should still scan"
    assert not (names & PHASE_SKILL_NAMES), names & PHASE_SKILL_NAMES
    assert "todo-guide" in names  # an ordinary bound skill is still listed
    path, dep = resolve_config_path("worker_base")
    cfg = load_agent_config(path, dep)
    frozen = serialize_resolved_config(cfg, model=cfg.llm.model)["instructions"]
    assert PHASE_SKILL_NAMES <= set(frozen)


@pytest.mark.asyncio
async def test_resolved_catalog_never_offers_the_phase_skills(monkeypatch):
    from shared.runtime.core.loader import PHASE_SKILL_NAMES

    monkeypatch.setattr(deployment_gates_module, "is_skills_db_enabled", lambda: True)
    monkeypatch.setattr(catalogue_state(), "skills", catalogue_service().scan_skills())
    monkeypatch.setattr(
        orchestrator_main.app.state.resources.postgres_db,
        "list_skills_visible",
        AsyncMock(return_value=[]),
    )
    payload = await catalogue_service().gather_in_scope_skills("user-1")
    names = {item["name"] for item in payload["menu"]}
    assert not (names & PHASE_SKILL_NAMES)
    assert not (set(payload["files"]) & PHASE_SKILL_NAMES)
