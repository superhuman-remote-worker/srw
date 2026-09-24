"""Managed seed and authoritative root-expert selection contracts."""

from copy import deepcopy
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from orchestrator.services.default_experts import (
    load_seed_bundle,
    resolve_root_expert,
    seed_managed_default_experts,
)
from shared.runtime.core.loader import canonical_config_name, resolve_config_path
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import (
    session_config_resolution as session_config_resolution_module,
)
from orchestrator.services import session_tool_policy as session_tool_policy_module


ROOT = Path(__file__).resolve().parents[1]


class SelectionDB:
    def __init__(self):
        self.application = {"id": "app", "expert_type": "worker", "owner_id": None}
        self.personal = {"id": "mine", "expert_type": "worker", "owner_id": "u1"}
        self.project = None
        self.explicit = {"id": "explicit", "expert_type": "worker", "owner_id": "u1"}
        self.link = None
        self.global_grants = []

    async def list_grants_for_scopes(self, *, user_id, project_ids):
        return {"user": [], "project": [], "global": self.global_grants}

    async def get_expert_visible_by_id(self, expert_id, **_kwargs):
        return self.explicit if expert_id == self.explicit["id"] else None

    async def get_project_expert_link(self, **_kwargs):
        return self.link

    async def get_project_default_expert(self, **_kwargs):
        return self.project

    async def get_user_expert_default(self, **_kwargs):
        return self.personal

    async def get_application_expert_default(self, _expert_type):
        return self.application


@pytest.mark.asyncio
async def test_selection_precedence_and_project_override():
    db = SelectionDB()
    db.project = {
        "id": "project",
        "expert_type": "worker",
        "owner_id": "other",
        "config_override": {"llm": {"temperature": 0.2}},
    }
    selected = await resolve_root_expert(
        db, expert_type="worker", user_id="u1", project_id="p1"
    )
    assert selected.source == "project"
    assert selected.expert["id"] == "project"
    assert selected.project_override == {"llm": {"temperature": 0.2}}

    db.link = {"config_override": {"tools": {"shell": []}}}
    explicit = await resolve_root_expert(
        db,
        expert_type="worker",
        user_id="u1",
        project_id="p1",
        explicit_expert_id="explicit",
    )
    assert explicit.source == "explicit"
    assert explicit.project_override == {"tools": {"shell": []}}


@pytest.mark.asyncio
async def test_personal_default_is_dormant_when_grant_is_revoked():
    db = SelectionDB()
    chosen = await resolve_root_expert(db, expert_type="worker", user_id="u1")
    assert chosen.source == "user"

    db.global_grants = [{"key": "personal_default_experts", "value_json": False}]
    chosen = await resolve_root_expert(db, expert_type="worker", user_id="u1")
    assert chosen.source == "application"


def test_legacy_base_names_resolve_to_canonical_files():
    """The public root names survive the U1 split: aliases canonicalise to
    ``worker_base``/``session_base`` (never to the overlay files), and the
    names resolve to the role overlays that replaced the old base files."""
    assert canonical_config_name("defaults") == "worker_base"
    assert canonical_config_name("persistent_defaults") == "session_base"
    assert canonical_config_name("worker_base") == "worker_base"
    assert canonical_config_name("overlays/session") == "session_base"
    worker, _ = resolve_config_path("defaults")
    session, _ = resolve_config_path("persistent_defaults")
    assert Path(worker).parts[-2:] == ("overlays", "worker.yaml")
    assert Path(session).parts[-2:] == ("overlays", "session.yaml")
    assert resolve_config_path("worker_base")[0] == worker
    assert resolve_config_path("session_base")[0] == session


def test_managed_seed_bundles_are_raw_typed_overlays():
    worker = load_seed_bundle(
        ROOT / "config", directory="general-worker", expert_type="worker"
    )
    session = load_seed_bundle(
        ROOT / "config", directory="assistant", expert_type="session"
    )
    assert worker["name"] == "general-worker"
    assert session["name"] == "assistant"
    assert "$extends" not in worker["config"]
    assert "$extends" not in session["config"]
    assert session["prompts"]["persona"].strip()


class SeedDB:
    def __init__(self):
        self.rows = {}
        self.defaults = {}

    async def upsert_managed_expert(self, *, managed_key, **bundle):
        if managed_key in self.rows:
            return self.rows[managed_key], False
        row = {"id": f"id-{len(self.rows)}", "managed_key": managed_key, **bundle}
        self.rows[managed_key] = row
        return row, True

    async def upgrade_managed_expert_seed(
        self,
        *,
        managed_key,
        seed_version,
        config_additions,
        expected_seed_version=None,
        expected_subagents=None,
        replacement_subagents=None,
    ):
        # Models PostgresDB.upgrade_managed_expert_seed: `$additions || config`
        # (the row wins every key it has) under a `seed_version < $3` guard.
        row = self.rows[managed_key]
        if int(row.get("seed_version") or 0) >= seed_version:
            return None
        if expected_seed_version is not None:
            if (
                row["seed_version"] != expected_seed_version
                or row["config"].get("subagents") != expected_subagents
            ):
                return None
            row["config"] = {**row["config"], "subagents": replacement_subagents}
        row["config"] = {**config_additions, **row["config"]}
        row["seed_version"] = seed_version
        return row

    async def ensure_application_expert_default(self, *, expert_type, expert_id):
        self.defaults.setdefault(expert_type, expert_id)
        return {"expert_type": expert_type, "expert_id": self.defaults[expert_type]}


@pytest.mark.asyncio
async def test_managed_seed_is_idempotent_and_insert_only():
    db = SeedDB()
    first = await seed_managed_default_experts(db, ROOT / "config")
    db.rows["application-default-session-seed"]["display_name"] = "Operator Assistant"
    second = await seed_managed_default_experts(db, ROOT / "config")
    assert first == second
    assert (
        db.rows["application-default-session-seed"]["display_name"]
        == "Operator Assistant"
    )
    assert set(db.defaults) == {"worker", "session"}


@pytest.mark.asyncio
async def test_seed_upgrade_adds_only_the_keys_the_row_lacks():
    """A row seeded before the assistant grew its roster gains `subagents`
    on the next start; everything the operator touched stays theirs."""
    db = SeedDB()
    await seed_managed_default_experts(db, ROOT / "config")
    row = db.rows["application-default-session-seed"]
    assert "subagents" in row["config"], "the bundle ships the roster"
    # Rewind the row to what a pre-roster deployment holds: seed 1, no
    # `subagents`, plus two operator edits (a renamed expert, a tools tweak).
    row["seed_version"] = 1
    row["config"] = {k: v for k, v in row["config"].items() if k != "subagents"}
    row["config"]["tools"] = {"shell": ["run_command"]}
    row["display_name"] = "Operator Assistant"

    await seed_managed_default_experts(db, ROOT / "config")

    upgraded = db.rows["application-default-session-seed"]
    assert upgraded["seed_version"] == 3
    assert upgraded["config"]["subagents"]["default"] == "explorer"
    assert set(upgraded["config"]["subagents"]["roster"]) == {
        "explorer",
        "reader",
        "implementer",
    }
    assert upgraded["config"]["tools"] == {"shell": ["run_command"]}
    assert upgraded["display_name"] == "Operator Assistant"


@pytest.mark.asyncio
async def test_seed_upgrade_never_replaces_an_operators_roster():
    db = SeedDB()
    await seed_managed_default_experts(db, ROOT / "config")
    row = db.rows["application-default-session-seed"]
    row["seed_version"] = 1
    theirs = {"default": "critic", "roster": {"critic": {"$ref": "critic"}}}
    row["config"]["subagents"] = theirs

    await seed_managed_default_experts(db, ROOT / "config")

    assert row["seed_version"] == 3
    assert row["config"]["subagents"] == theirs


@pytest.mark.asyncio
async def test_seed_upgrade_is_a_no_op_at_the_current_version():
    from orchestrator.services.default_experts import (
        MANAGED_SEEDS,
        upgrade_managed_seed,
    )

    db = SeedDB()
    await seed_managed_default_experts(db, ROOT / "config")
    spec = next(s for s in MANAGED_SEEDS if s["expert_type"] == "session")
    row = db.rows[spec["managed_key"]]
    bundle = load_seed_bundle(
        ROOT / "config", directory=spec["directory"], expert_type="session"
    )
    assert await upgrade_managed_seed(db, spec=spec, bundle=bundle, row=row) is None


@pytest.mark.asyncio
async def test_future_seed_target_still_repairs_v2_with_historical_safe_roster():
    from orchestrator.services.default_experts import (
        MANAGED_SEEDS,
        upgrade_managed_seed,
    )

    db = SeedDB()
    await seed_managed_default_experts(db, ROOT / "config")
    spec = next(s for s in MANAGED_SEEDS if s["expert_type"] == "session")
    row = db.rows[spec["managed_key"]]
    safe_v3_roster = deepcopy(row["config"]["subagents"])
    row["seed_version"] = 2
    row["config"]["subagents"]["roster"]["implementer"].pop("tools")
    future_bundle = load_seed_bundle(
        ROOT / "config", directory="assistant", expert_type="session"
    )
    future_bundle["config"]["future_top_level_key"] = True
    future_bundle["config"]["subagents"]["default"] = "reader"

    await upgrade_managed_seed(
        db, spec={**spec, "seed_version": 4}, bundle=future_bundle, row=row
    )

    assert row["seed_version"] == 4
    assert row["config"]["subagents"] == safe_v3_roster
    assert row["config"]["future_top_level_key"] is True


def test_default_expert_migration_shape():
    migration_dir = ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
    sql = "\n".join(
        path.read_text() for path in sorted(migration_dir.glob("006[4-8]_*.sql"))
    )
    assert "application_expert_defaults" in sql
    assert "user_expert_defaults" in sql
    assert "expert_default_audit" in sql
    assert "managed_key" in sql
    assert "FOREIGN KEY (expert_id, expert_type)" in sql
    assert "CREATE UNIQUE INDEX CONCURRENTLY" in sql
    assert "BIGINT GENERATED BY DEFAULT AS IDENTITY" in sql


def test_default_assistant_runtime_control_groups_are_really_off():
    """The resolved policy must reach the runtime gates, not stop at YAML."""
    from orchestrator.services.config_resolver import resolve_config
    from agent.api.persistent_session import (
        _agent_catalog_enabled,
        _canvas_enabled,
        _fleet_management_enabled,
        _workflows_enabled,
    )
    from shared.runtime.core.loader import load_config_from_resolved

    assistant = load_seed_bundle(
        ROOT / "config", directory="assistant", expert_type="session"
    )
    capture: dict = {}
    blob = resolve_config(
        base_config_name="session_base",
        expert_row=assistant,
        expert_type="session",
        capture=capture,
    )
    blob["agent"].update(
        session_tool_policy_module.session_tool_group_disabled_markers(
            capture["merged_fragment"]
        )
    )
    hydrated = load_config_from_resolved(blob)

    assert _fleet_management_enabled(hydrated) is False
    assert _agent_catalog_enabled(hydrated) is False
    assert _workflows_enabled(hydrated) is False
    assert _canvas_enabled(hydrated) is True


@pytest.mark.asyncio
async def test_account_reasoning_is_a_floor_below_the_expert(monkeypatch):
    from orchestrator import main as orchestrator_main
    from orchestrator.services.config_resolver import resolve_config

    monkeypatch.setattr(
        orchestrator_main.app.state.resources.postgres_db,
        "get_user_settings",
        AsyncMock(
            return_value={
                "default_model": "account-model",
                "default_reasoning_level": "low",
            }
        ),
    )
    monkeypatch.setattr(
        orchestrator_main.app.state.resources.postgres_db,
        "resolve_default_for_capability",
        AsyncMock(return_value=None),
    )
    account_floor = await session_config_resolution_module.resolve_default_models(
        "user-1",
        dependencies=preparation_composition.session_config_dependencies(
            orchestrator_main.app.state.resources
        ),
    )

    assert account_floor["llm"] == {
        "model": "account-model",
        "reasoning_level": "low",
    }

    expert = {
        "expert_type": "session",
        "name": "reasoning-expert",
        "config": {"llm": {"reasoning_level": "high"}},
        "prompts": {},
    }
    resolved = resolve_config(
        base_config_name="session_base",
        base_defaults=account_floor,
        expert_row=expert,
        expert_type="session",
    )
    assert resolved["agent"]["llm"]["model"] == "account-model"
    assert resolved["agent"]["llm"]["reasoning_level"] == "high"


# --- U1 WP4: universal experts (D4) and role tags on the seeds ---------------


@pytest.mark.asyncio
async def test_explicit_cross_role_selection_is_allowed(caplog):
    """Every expert is usable in every role: an explicit session expert picked
    for a worker root is accepted (`resolve_config` re-roots it onto the worker
    overlay at dispatch) and logged, never refused. Invisible stays refused;
    the default SLOTS stay per role (checked at their endpoints)."""
    import logging

    from orchestrator.services.default_experts import ExpertSelectionError

    db = SelectionDB()
    db.explicit = {"id": "explicit", "expert_type": "session", "owner_id": "u1"}
    with caplog.at_level(logging.INFO, logger="orchestrator.services.default_experts"):
        chosen = await resolve_root_expert(
            db, expert_type="worker", user_id="u1", explicit_expert_id="explicit"
        )
    assert chosen.source == "explicit"
    assert chosen.expert["expert_type"] == "session"
    assert any(
        "session" in r.getMessage() and "worker" in r.getMessage()
        for r in caplog.records
    )
    with pytest.raises(ExpertSelectionError):
        await resolve_root_expert(
            db, expert_type="worker", user_id="u1", explicit_expert_id="missing"
        )


def test_seed_bundles_carry_the_role_tag():
    worker = load_seed_bundle(
        ROOT / "config", directory="general-worker", expert_type="worker"
    )
    session = load_seed_bundle(
        ROOT / "config", directory="assistant", expert_type="session"
    )
    # general-worker authors its role tag already: kept in place, not doubled.
    assert worker["tags"] == ["general", "worker", "safe-default"]
    assert session["tags"][-1] == "session" and session["tags"].count("session") == 1


def test_seed_bundle_reads_expert_local_phase_skill_bodies(tmp_path):
    """U2: the managed seed's prompts.strategic/tactical come from the
    expert-local phase skills (body only)."""
    expert_dir = tmp_path / "experts" / "seeded"
    (expert_dir / "skills" / "strategic-phase").mkdir(parents=True)
    (expert_dir / "skills" / "tactical-phase").mkdir(parents=True)
    (expert_dir / "config.yaml").write_text(
        "$extends: worker_base\nagent_id: seeded\ndisplay_name: Seeded\n"
    )
    (expert_dir / "persona.txt").write_text("I am seeded.\n")
    (expert_dir / "skills" / "strategic-phase" / "SKILL.md").write_text(
        "---\nname: strategic-phase\ndescription: d\ncatalog: hidden\n---\n\n"
        "# Strategic phase\n\nSEEDED STRATEGIC BODY\n"
    )
    (expert_dir / "skills" / "tactical-phase" / "SKILL.md").write_text(
        "---\nname: tactical-phase\ndescription: d\ncatalog: hidden\n---\n\n"
        "# Tactical phase\n\nSEEDED TACTICAL BODY\n"
    )

    bundle = load_seed_bundle(tmp_path, directory="seeded", expert_type="worker")

    assert bundle["prompts"]["persona"] == "I am seeded.\n"
    assert (
        bundle["prompts"]["strategic"] == "# Strategic phase\n\nSEEDED STRATEGIC BODY\n"
    )
    assert "catalog: hidden" not in bundle["prompts"]["strategic"]
    assert bundle["prompts"]["tactical"] == "# Tactical phase\n\nSEEDED TACTICAL BODY\n"
    # The bundled worker seed itself ships no phase prompt of its own.
    seed = load_seed_bundle(
        ROOT / "config", directory="general-worker", expert_type="worker"
    )
    assert "strategic" not in seed["prompts"] and "tactical" not in seed["prompts"]
