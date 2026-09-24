"""Unit tests for DB-backed expert write-CRUD (restored Slice-1 surface).

These guard the request models + the save-time hard-deny gate + the bundle
mapping. Full endpoint integration is verified live on k3d (see the plan's
Task 10) because the auth dependency + asyncpg store are not hermetically
mockable here. Local env may be noisy (Py3.14, missing optional deps); CI
(Py3.12) is the authoritative gate.
"""

from shared.runtime.core.srw_manifest_config import (
    srw_config_fragment as _srw_config_fragment,
)

from tests._expert_catalog import catalogue_service, catalogue_state, catalogue_route
from orchestrator.routers import expert_catalog as expert_routes
from orchestrator.schemas import expert_catalog as expert_schemas
from orchestrator.services import expert_catalog as expert_catalog_module


import pytest
from fastapi import HTTPException
from orchestrator.security import access as access_module
from orchestrator.security import auth as auth_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import grant_enforcement as grant_enforcement_module


# --- T1: request models + save-time hard-deny gate ---


def test_expert_create_rejects_bad_slug():
    with pytest.raises(Exception):
        expert_schemas.ExpertCreate(
            name="Bad Name", display_name="X", expert_type="worker"
        )


def test_expert_create_rejects_bad_type():
    with pytest.raises(Exception):
        expert_schemas.ExpertCreate(
            name="ok", display_name="X", expert_type="orchestrator"
        )


def test_expert_create_rejects_bad_color():
    with pytest.raises(Exception):
        expert_schemas.ExpertCreate(
            name="ok", display_name="X", expert_type="worker", color="red"
        )


def test_expert_create_minimal_ok():
    e = expert_schemas.ExpertCreate(
        name="my-helper", display_name="My Helper", expert_type="session"
    )
    assert e.config == {} and e.prompts == {} and e.color == "#6B7280"
    assert e.icon == "smart_toy"


def test_validate_fragment_blocks_credentials():
    with pytest.raises(HTTPException) as ei:
        catalogue_service().validate_expert_fragment({"llm": {"api_key": "secret"}})
    assert ei.value.status_code == 422


def test_validate_fragment_blocks_connections():
    with pytest.raises(HTTPException) as ei:
        catalogue_service().validate_expert_fragment({"connections": {"db": "x"}})
    assert ei.value.status_code == 422


def test_validate_fragment_allows_clean_config_and_canonicalises_tools():
    """Should not raise, and returns the fragment with `tools` normalised — the
    caller persists what comes back, so a row never holds a policy value the
    save-time PDP would read backwards."""
    assert catalogue_service().validate_expert_fragment(
        {
            "llm": {"model": "gemma-4-moe"},
            "tools": {"shell": ["run_command"], "git": []},
        }
    ) == {
        "llm": {"model": "gemma-4-moe"},
        "tools": {"shell": ["run_command"], "git": []},
    }


def test_validate_fragment_runs_the_shared_tool_gate():
    """The fragment gate is now the same one every other write boundary runs
    (400, not 422): `shell` must enumerate, and a cross-category smuggle is
    refused. See tests/test_tool_override_boundary.py::TestExpertWriteBoundary."""
    with pytest.raises(HTTPException) as ei:
        catalogue_service().validate_expert_fragment({"tools": {"shell": True}})
    assert ei.value.status_code == 400

    with pytest.raises(HTTPException) as ei:
        catalogue_service().validate_expert_fragment(
            {"tools": {"canvas": ["run_command"]}}
        )
    assert ei.value.status_code == 400


# --- T2: update contract (immutable name/type; unset fields dropped) ---


def test_update_excludes_immutable_fields():
    assert "name" not in expert_schemas.ExpertUpdate.model_fields
    assert "expert_type" not in expert_schemas.ExpertUpdate.model_fields


# --- Part 2: prompt-key allow-list + fork fidelity ---


def test_expert_create_accepts_known_prompt_keys():
    e = expert_schemas.ExpertCreate(
        name="coder",
        display_name="Coder",
        expert_type="worker",
        prompts={
            "persona": "p",
            "strategic": "s",
            "tactical": "t",
            "summarization": "z",
        },
    )
    assert e.prompts["strategic"] == "s"


def test_expert_create_rejects_unknown_prompt_key():
    with pytest.raises(Exception):
        expert_schemas.ExpertCreate(
            name="coder",
            display_name="Coder",
            expert_type="worker",
            prompts={"bogus": "x"},
        )


def test_expert_update_rejects_unknown_prompt_key():
    with pytest.raises(Exception):
        expert_schemas.ExpertUpdate(prompts={"systemprompt": "x"})


def test_bundled_expert_bundle_captures_all_prompt_segments():
    """Fork fidelity: bundling a worker captures strategic/tactical/summarization,
    not just persona+instructions (critic ships all five)."""
    bundle = catalogue_service().bundled_expert_bundle("critic")
    if bundle is None:
        pytest.skip("critic bundled expert not found in this env")
    prompts = bundle["prompts"]
    assert prompts.get("strategic", "").strip()
    assert prompts.get("tactical", "").strip()
    assert prompts.get("summarization", "").strip()
    assert "persona" in prompts  # still captured


def test_update_payload_drops_unset_fields():
    body = expert_schemas.ExpertUpdate(display_name="New Name")
    assert body.model_dump(exclude_unset=True) == {"display_name": "New Name"}


# --- T3: bundle-source mapping (JSONB str-tolerant) ---


def test_db_row_to_bundle_src_shape():
    row = {
        "name": "scholar",
        "display_name": "Scholar",
        "expert_type": "worker",
        "description": None,
        "icon": "school",
        "color": "#111111",
        "tags": ["research"],
        "config": {"llm": {"model": "x"}},
        "prompts": {"persona": "p"},
    }
    src = expert_catalog_module.db_expert_to_bundle_src(row)
    assert src["name"] == "scholar"
    assert src["expert_type"] == "worker"
    assert src["config"] == {"llm": {"model": "x"}}
    assert src["prompts"] == {"persona": "p"}


def test_db_row_to_bundle_src_parses_json_strings():
    row = {
        "name": "x",
        "display_name": "X",
        "expert_type": "session",
        "icon": "smart_toy",
        "color": "#6B7280",
        "config": '{"tools": {"shell": false}}',
        "prompts": '{"persona": "hi"}',
    }
    src = expert_catalog_module.db_expert_to_bundle_src(row)
    assert src["config"] == {"tools": {"shell": False}}
    assert src["prompts"] == {"persona": "hi"}


# --- U1 WP4: the roster at save, role tags on write, the tag-aware list -----

import uuid  # noqa: E402
from pathlib import Path  # noqa: E402
from unittest.mock import AsyncMock, MagicMock  # noqa: E402

import yaml  # noqa: E402

import orchestrator.main as main_module  # noqa: E402


def test_validate_fragment_accepts_a_resolvable_roster_and_canonicalises_entry_tools():
    """Bundled, library and DB `$ref`s pass the sync gate (a DB ref's visibility
    is the async half below); each entry's `tools` comes back canonical."""
    out = catalogue_service().validate_expert_fragment(
        {
            "subagents": {
                "default": "explorer",
                "roster": {
                    "explorer": {"$ref": "subagents/explorer"},
                    "reviewer": {"$ref": "critic", "tools": {"git": []}},
                    "helper": {"$ref": str(uuid.uuid4())},
                    "inline": {"description": "no ref at all"},
                },
            }
        }
    )
    roster = out["subagents"]["roster"]
    assert roster["reviewer"] == {"$ref": "critic", "tools": {"git": []}}
    assert roster["inline"] == {"description": "no ref at all"}
    assert out["subagents"]["default"] == "explorer"


def test_validate_fragment_422s_an_unknown_ref():
    with pytest.raises(HTTPException) as ei:
        catalogue_service().validate_expert_fragment(
            {"subagents": {"roster": {"x": {"$ref": "no-such-expert"}}}}
        )
    assert ei.value.status_code == 422
    assert "subagents.roster.x" in str(ei.value.detail)
    assert "no-such-expert" in str(ei.value.detail)
    # A path is never a ref; a non-mapping entry or roster is refused too.
    for bad in (
        {"roster": {"x": {"$ref": "../secrets"}}},
        {"roster": {"x": {"$ref": ""}}},
        {"roster": {"x": "critic"}},
        {"roster": ["critic"]},
        {"llm": "not-a-mapping"},
    ):
        with pytest.raises(HTTPException) as ei:
            catalogue_service().validate_expert_fragment({"subagents": bad})
        assert ei.value.status_code == 422, bad


def test_validate_fragment_runs_the_tool_gate_on_roster_entries():
    """The same vocabulary gate as the top level (400), naming the entry: a
    cross-category smuggle inside a child is refused like one on the parent."""
    with pytest.raises(HTTPException) as ei:
        catalogue_service().validate_expert_fragment(
            {"subagents": {"roster": {"x": {"tools": {"canvas": ["run_command"]}}}}}
        )
    assert ei.value.status_code == 400
    assert "subagents.roster.x" in str(ei.value.detail)


@pytest.mark.asyncio
async def test_db_refs_must_be_visible_to_the_author(monkeypatch):
    visible, hidden = str(uuid.uuid4()), str(uuid.uuid4())
    monkeypatch.setattr(
        access_module, "user_visible_project_ids", AsyncMock(return_value="all")
    )
    monkeypatch.setattr(
        main_module.app.state.resources.postgres_db,
        "get_expert_visible_by_id",
        AsyncMock(
            side_effect=lambda ref, **kw: {"id": ref} if ref == visible else None
        ),
    )
    user = {"id": str(uuid.uuid4()), "is_admin": False}

    # Visible DB ref + a disk ref: fine (disk refs are the sync gate's job).
    await catalogue_service().require_visible_roster_refs(
        {"subagents": {"roster": {"a": {"$ref": visible}, "b": {"$ref": "critic"}}}},
        user=user,
    )
    await catalogue_service().require_visible_roster_refs({}, user=user)
    await catalogue_service().require_visible_roster_refs(None, user=user)

    with pytest.raises(HTTPException) as ei:
        await catalogue_service().require_visible_roster_refs(
            {"subagents": {"roster": {"a": {"$ref": hidden}}}}, user=user
        )
    assert ei.value.status_code == 422
    assert hidden in str(ei.value.detail)


@pytest.mark.asyncio
async def test_create_adds_role_tag(monkeypatch):
    """`tags ∪ {expert_type}` on the way in: the role tag is appended once,
    duplicates and blanks dropped, authored order kept."""

    monkeypatch.setattr(
        deployment_gates_module, "is_experts_db_enabled", MagicMock(return_value=True)
    )
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": str(uuid.uuid4()), "is_admin": False}),
    )
    monkeypatch.setattr(
        grant_enforcement_module, "enforce_expert_save", AsyncMock(return_value=None)
    )
    created = AsyncMock(side_effect=lambda **kw: {"id": "new", **kw})
    monkeypatch.setattr(
        main_module.app.state.resources.postgres_db, "create_expert", created
    )

    row = await catalogue_route(expert_routes.create_expert)(
        MagicMock(),
        expert_schemas.ExpertCreate(
            name="tagged",
            display_name="Tagged",
            expert_type="session",
            tags=["research", " session ", "research", ""],
        ),
    )
    assert created.await_args.kwargs["tags"] == ["research", "session"]
    assert row["tags"] == ["research", "session"]

    await catalogue_route(expert_routes.create_expert)(
        MagicMock(),
        expert_schemas.ExpertCreate(
            name="plain", display_name="Plain", expert_type="worker"
        ),
    )
    assert created.await_args.kwargs["tags"] == ["worker"]


@pytest.mark.asyncio
async def test_update_keeps_the_role_tag(monkeypatch):
    expert_id, owner = str(uuid.uuid4()), str(uuid.uuid4())
    monkeypatch.setattr(
        deployment_gates_module, "is_experts_db_enabled", MagicMock(return_value=True)
    )
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": owner, "is_admin": False}),
    )
    monkeypatch.setattr(
        grant_enforcement_module, "enforce_expert_save", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        main_module.app.state.resources.postgres_db,
        "get_expert_by_id",
        AsyncMock(
            return_value={
                "id": expert_id,
                "owner_id": owner,
                "expert_type": "worker",
                "managed_key": None,
            }
        ),
    )
    updated = AsyncMock(side_effect=lambda expert_id, **kw: {"id": expert_id, **kw})
    monkeypatch.setattr(
        main_module.app.state.resources.postgres_db, "update_expert", updated
    )

    await catalogue_route(expert_routes.update_expert)(
        MagicMock(), expert_id, expert_schemas.ExpertUpdate(tags=["coding"])
    )
    assert updated.await_args.kwargs["tags"] == ["coding", "worker"]

    # An update that does not touch tags leaves the column alone.
    await catalogue_route(expert_routes.update_expert)(
        MagicMock(), expert_id, expert_schemas.ExpertUpdate(display_name="Renamed")
    )
    assert "tags" not in updated.await_args.kwargs


@pytest.mark.asyncio
async def test_list_type_filter_matches_tag(monkeypatch):
    """`?type=X` lists a row when its role is X OR it carries the tag X (U1
    B.4). The subagent library lists by tag only (`?type=subagent`), never in
    the default listing; the DB rows are fetched without the SQL role filter."""

    monkeypatch.setattr(catalogue_state(), "experts", None)
    monkeypatch.setattr(catalogue_state(), "library", None)
    monkeypatch.setattr(
        deployment_gates_module, "is_experts_db_enabled", MagicMock(return_value=True)
    )
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": str(uuid.uuid4()), "is_admin": False}),
    )
    monkeypatch.setattr(
        access_module, "user_visible_project_ids", AsyncMock(return_value="all")
    )

    def row(name, expert_type, tags):
        return {
            "id": str(uuid.uuid4()),
            "name": name,
            "display_name": name,
            "description": "",
            "icon": "smart_toy",
            "color": "#6B7280",
            "tags": tags,
            "expert_type": expert_type,
            "is_global": True,
            "owner_id": None,
            "managed_key": None,
        }

    listed = AsyncMock(
        return_value=[
            row("tagged-worker", "worker", ["worker", "subagent"]),
            row("plain-session", "session", ["session"]),
            row("dual", "session", ["session", "worker"]),
        ]
    )
    monkeypatch.setattr(
        main_module.app.state.resources.postgres_db, "list_experts_visible", listed
    )

    def by_name(result):
        return {r["name"]: r for r in result}

    subagent = by_name(
        await catalogue_route(expert_routes.list_experts)(MagicMock(), type="subagent")
    )
    explorer = subagent["subagents/explorer"]
    assert explorer["id"] == "explorer"
    assert explorer["source"] == "library" and explorer["storage_kind"] == "library"
    assert "subagent" in explorer["tags"]
    assert "tagged-worker" in subagent
    assert "plain-session" not in subagent and "developer" not in subagent
    assert listed.await_args.kwargs["expert_type"] is None

    worker = by_name(
        await catalogue_route(expert_routes.list_experts)(MagicMock(), type="worker")
    )
    assert {"developer", "tagged-worker", "dual"} <= set(worker)  # dual: tagged
    assert "plain-session" not in worker and "subagents/explorer" not in worker
    authored = _srw_config_fragment(
        yaml.safe_load(
            (
                Path(main_module.__file__).resolve().parents[2]
                / "config/experts/developer/config.yaml"
            ).read_text(encoding="utf-8")
        )
    )["tags"]
    assert worker["developer"]["tags"] == [*authored, "worker"]

    session = by_name(
        await catalogue_route(expert_routes.list_experts)(MagicMock(), type="session")
    )
    assert {"assistant", "plain-session", "dual"} <= set(session)
    assert "tagged-worker" not in session

    default = by_name(await catalogue_route(expert_routes.list_experts)(MagicMock()))
    assert "subagents/explorer" not in default
    assert {"developer", "assistant", "tagged-worker", "plain-session", "dual"} <= set(
        default
    )


def test_bundled_expert_bundle_reads_the_phase_skill_bodies():
    """U2: a fork's prompts.strategic/tactical are the expert-local phase skill
    BODIES (frontmatter stripped) — the DB shape is unchanged and the text is
    what the phase block's <expert_workflow> addendum will carry."""
    from pathlib import Path

    from shared.runtime.core.skill_format import parse_skill_md

    bundle = catalogue_service().bundled_expert_bundle("developer")
    if bundle is None:
        pytest.skip("developer bundled expert not found in this env")
    prompts = bundle["prompts"]
    dev = Path("config/experts/developer")
    for phase in ("strategic", "tactical"):
        _fm, body = parse_skill_md(
            (dev / "skills" / f"{phase}-phase" / "SKILL.md").read_text(encoding="utf-8")
        )
        assert prompts[phase] == body.lstrip("\n")
        assert not prompts[phase].startswith("---")
        assert f"name: {phase}-phase" not in prompts[phase]
        assert f"You are in {phase.upper()} mode." in prompts[phase]
    assert "tdd_phase" in prompts["strategic"]
    # An expert with neither a local phase skill nor the legacy .txt has no key.
    writer = catalogue_service().bundled_expert_bundle("writer")
    if writer is not None:
        assert "strategic" not in writer["prompts"]
        assert "tactical" not in writer["prompts"]
