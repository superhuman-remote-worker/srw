"""Roster resolution contract (U1 WP3, ``src/core/subagent_roster.py``).

An expert's ``subagents.roster`` names the children it may delegate to —
inline small experts or ``$ref``s to a bundled expert, a library entry
(``config/subagents/<name>``) or a DB expert row. Every entry is materialised
into a fully merged subagent-role config dict::

    expert_base <- overlays/subagent <- [$ref target's own $extends chain]
                <- subagents.llm (roster-wide) <- the entry's sibling keys
                (+ job/thread override) -> `inherit` -> settings matrix per
                entry -> the subagent overlay's `$ignore_keys` pruned

Tests are named after u1_plan.md WP3. The failure policy is pinned twice:
``on_missing="raise"`` (the disk path — a bundled typo fails loudly) and
``on_missing="drop"`` (dispatch — the entry is dropped and recorded in
``_roster_warnings``, the job never fails).
"""

from __future__ import annotations

from shared.runtime.core.srw_manifest_config import (
    srw_config_fragment as _srw_config_fragment,
)

import json
from pathlib import Path

import pytest
import yaml

from orchestrator.services.config_resolver import resolve_config
from shared.runtime.core import subagent_roster
from shared.runtime.core.loader import (
    IGNORE_KEYS_DIRECTIVE,
    INHERIT_MODEL,
    ROSTER_INHERIT_MARKER,
    SubagentsConfig,
    get_all_tool_names,
    load_agent_config,
    load_agent_config_from_dict,
    load_config_from_resolved,
    load_role_base,
    resolve_config_path,
    serialize_resolved_config,
)
from shared.runtime.core.subagent_roster import (
    MAX_REF_HOPS,
    ROSTER_WARNINGS_KEY,
    RosterResolutionError,
    resolve_subagent_roster,
)
from agent.subagents.budgets import BUILTIN_DEFAULTS, ChildBudgets
from agent.subagents.child import (
    CONTROL_PLANE_CATEGORIES,
    DELEGATION_TOOL_NAMES,
    entry_tool_names,
    select_child_tool_names,
)
from agent.tools.registry import TOOL_REGISTRY

_REPO = Path(__file__).resolve().parents[1]
_CONFIG = _REPO / "config"
_UUID = "3f2a9c1e-4b6d-4e8f-9a0b-1c2d3e4f5a6b"
_PARENT_LLM = {
    "model": "claude-opus-4-1",
    "provider": "anthropic",
    "base_url": "https://router.example/v1",
    "model_max_context_tokens": 400000,
}
_SUBAGENT_IGNORED: list[str] = yaml.safe_load(
    (_CONFIG / "overlays" / "subagent.yaml").read_text(encoding="utf-8")
)[IGNORE_KEYS_DIRECTIVE]


def _present(data: dict, dotted: str) -> bool:
    node = data
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return False
        node = node[part]
    return True


def _resolve(subagents, *, db_refs=None, on_missing="raise", llm=None, **extra):
    """Run the resolver on a minimal parent; returns the parent dict."""
    data = {
        "agent_id": "parent",
        "display_name": "Parent",
        "llm": dict(llm if llm is not None else _PARENT_LLM),
        "subagents": subagents,
        **extra,
    }
    return resolve_subagent_roster(data, db_refs=db_refs or {}, on_missing=on_missing)


def _entry(subagents, name, **kw) -> dict:
    return _resolve(subagents, **kw)["subagents"]["roster"][name]


def _db_row(**over) -> dict:
    row = {
        "id": _UUID,
        "name": "sonnet-reader",
        "display_name": "Sonnet Reader",
        "description": "Reads sources for the parent.",
        "expert_type": "session",
        "tags": ["reader"],
        "config": {
            "llm": {"model": "claude-sonnet-4-5"},
            "tools": {"workspace": ["read_file"]},
            "workspace": {"backend": "kubernetes"},
            "autonomy": "full",
        },
        "prompts": {"persona": "READER-PERSONA", "instructions": ""},
    }
    row.update(over)
    return row


# ---------------------------------------------------------------------------
# Inline entries and the layer order
# ---------------------------------------------------------------------------


def test_inline_entry_resolves_on_subagent_overlay():
    entry = _entry(
        {
            "roster": {
                "implementer": {
                    "description": "Implements ONE bounded change.",
                    "tools": {"workspace": ["read_file", "write_file"]},
                    "isolation": "worktree",
                    "write_policy": "owned_paths",
                    "return": "diff",
                    "limits": {"max_turns": 150},
                }
            }
        },
        "implementer",
    )
    base = load_role_base("subagent")
    assert entry["agent_id"] == "implementer"
    assert entry["display_name"] == "implementer"
    assert entry["description"] == "Implements ONE bounded change."
    # The entry's own group replaces the floor's; untouched groups keep the floor.
    assert entry["tools"]["workspace"] == ["read_file", "write_file"]
    assert entry["tools"]["research"] == base["tools"]["research"]
    assert entry["tools"]["browser_direct"] == []
    # The overlay's values sit under the entry ...
    assert entry["interactive"]["permission_mode"] == "autonomous"
    assert entry["memory"]["enabled"] is False
    assert all(t["enabled"] is False for t in entry["auxiliary"]["tasks"].values())
    # ... and its ignored keys are gone, directive included.
    for dotted in _SUBAGENT_IGNORED:
        assert not _present(entry, dotted), dotted
    assert IGNORE_KEYS_DIRECTIVE not in entry
    assert "$ref" not in entry and "_ref" not in entry
    # U3-only keys ride verbatim.
    assert entry["isolation"] == "worktree"
    assert entry["write_policy"] == "owned_paths"
    assert entry["return"] == "diff"
    assert entry["limits"]["max_turns"] == 150
    # No model authored above the base: the overlay's `inherit` -> the parent's.
    assert entry["llm"]["model"] == "claude-opus-4-1"
    assert entry["llm"][ROSTER_INHERIT_MARKER] is True
    # A child parses like any config.
    child = load_agent_config_from_dict(entry)
    assert child.agent_id == "implementer"
    assert child.tools.workspace == ["read_file", "write_file"]
    assert child.interactive.permission_mode == "autonomous"
    assert child.memory.enabled is False


def test_inline_entry_deployment_dir_is_the_parents():
    """An inline entry is part of its parent expert: its `prompts` file
    names resolve against the parent's directory, recorded repo-relative."""
    data = {
        "agent_id": "scholar",
        "display_name": "S",
        "llm": dict(_PARENT_LLM),
        "subagents": {"roster": {"reader": {"prompts": {"system": "reader.txt"}}}},
    }
    out = resolve_subagent_roster(
        data, db_refs={}, deployment_dir=str(_CONFIG / "experts" / "scholar")
    )
    entry = out["subagents"]["roster"]["reader"]
    assert entry["_deployment_dir"] == "config/experts/scholar"
    assert entry["prompts"] == {"system": "reader.txt"}
    # No parent dir -> no key at all (never a bogus path).
    assert "_deployment_dir" not in _entry({"roster": {"r": {}}}, "r")


def test_roster_wide_llm_below_entry_llm():
    out = _resolve(
        {
            "llm": {"model": "claude-haiku-4-5", "temperature": 0.3},
            "roster": {
                "a": {},
                "b": {"llm": {"model": "claude-sonnet-4-5"}},
                "c": {"$ref": "subagents/explorer"},
            },
        }
    )
    roster = out["subagents"]["roster"]
    assert out["subagents"]["llm"] == {"model": "claude-haiku-4-5", "temperature": 0.3}
    # no own llm -> the roster-wide model AND params
    assert roster["a"]["llm"]["model"] == "claude-haiku-4-5"
    assert roster["a"]["llm"]["temperature"] == 0.3
    # own model wins; the rest of the roster-wide partial still applies
    assert roster["b"]["llm"]["model"] == "claude-sonnet-4-5"
    assert roster["b"]["llm"]["temperature"] == 0.3
    # the roster-wide default sits ABOVE a $ref target's own `inherit`
    assert roster["c"]["llm"]["model"] == "claude-haiku-4-5"
    for entry in roster.values():
        assert ROSTER_INHERIT_MARKER not in entry["llm"]
    # and the roster-wide model family drives each entry's window
    assert roster["a"]["limits"]["model_max_context_tokens"] == 200000
    assert roster["b"]["limits"]["model_max_context_tokens"] == 1000000


def test_inherit_model_sentinel():
    entry = _entry(
        {"roster": {"twin": {"llm": {"model": INHERIT_MODEL, "temperature": 0.9}}}},
        "twin",
    )
    llm = entry["llm"]
    assert llm["model"] == "claude-opus-4-1"
    assert llm["provider"] == "anthropic"
    assert llm["base_url"] == "https://router.example/v1"
    assert llm["model_max_context_tokens"] == 400000
    assert llm["temperature"] == 0.9  # the entry's own params survive
    assert llm[ROSTER_INHERIT_MARKER] is True
    # The parent's admin-pinned window drives the child's limits, not the
    # family max (1M for opus).
    assert entry["limits"]["model_max_context_tokens"] == 400000


def test_inherit_re_syncs_when_the_parsed_parent_runs_another_model():
    """A materialised roster parsed under a parent that changed model (the
    agent's fallback-path job override, a live session model switch)
    follows the parent — and drops the old model's transport."""
    out = _resolve({"roster": {"twin": {}, "pinned": {"llm": {"model": "gpt-5.4"}}}})
    data = {
        "agent_id": "p",
        "display_name": "P",
        "llm": {"model": "gemini-3-pro", "provider": "google"},
        "subagents": out["subagents"],
    }
    cfg = load_agent_config_from_dict(data)
    twin = cfg.subagents.roster["twin"]["llm"]
    assert twin["model"] == "gemini-3-pro"
    assert twin["provider"] == "google"
    assert "base_url" not in twin  # the anthropic router does not route gemini
    assert twin[ROSTER_INHERIT_MARKER] is True
    assert cfg.subagents.roster["pinned"]["llm"]["model"] == "gpt-5.4"  # untouched
    # Same model on re-parse: None leaves never clear what the entry carries.
    same = {
        "agent_id": "p",
        "display_name": "P",
        "llm": {"model": "claude-opus-4-1", "base_url": None, "api_key": "k"},
        "subagents": out["subagents"],
    }
    cfg = load_agent_config_from_dict(same)
    twin = cfg.subagents.roster["twin"]["llm"]
    assert twin["base_url"] == "https://router.example/v1"
    assert twin["api_key"] == "k"


def test_inherit_with_a_modelless_parent_is_left_and_recorded():
    out = _resolve({"roster": {"twin": {}}}, llm={"temperature": 0.1})
    assert out["subagents"]["roster"]["twin"]["llm"]["model"] == INHERIT_MODEL
    assert any("left unresolved" in w for w in out[ROSTER_WARNINGS_KEY])


# ---------------------------------------------------------------------------
# $ref targets
# ---------------------------------------------------------------------------


def test_ref_bundled_expert_drops_parent_only_keys():
    critic = _srw_config_fragment(
        yaml.safe_load(
            (_CONFIG / "experts" / "critic" / "config.yaml").read_text(encoding="utf-8")
        )
    )
    entry = _entry({"roster": {"reviewer": {"$ref": "critic"}}}, "reviewer")
    # the critic's tools survive ...
    assert entry["tools"]["shell"] == critic["tools"]["shell"]
    assert entry["tools"]["workspace"] == critic["tools"]["workspace"]
    # ... except the two groups a child never gets, however explicitly the
    # expert authored them: the phase loop (D3: a headless session) and
    # delegation (D7: children do not delegate).
    assert critic["tools"]["core"] and "core" not in entry["tools"]
    assert critic["tools"]["delegation"] and "delegation" not in entry["tools"]
    # its parent-only keys do not
    assert "backend" not in entry["workspace"]
    assert entry["workspace"]["max_read_words"] == 25000
    for key in ("verification", "autonomy", "delegation", "phase_settings"):
        assert key not in entry
    # identity: the roster name is the child; the target is recorded
    assert entry["agent_id"] == "reviewer"
    assert entry["display_name"] == "Critic"
    assert entry["tags"] == critic["tags"]
    assert entry["_ref"] == "critic"
    assert entry["_ref_kind"] == "bundled"
    assert entry["_deployment_dir"] == "config/experts/critic"
    # the critic pins no model -> the parent's
    assert entry["llm"]["model"] == "claude-opus-4-1"
    assert entry["llm"][ROSTER_INHERIT_MARKER] is True
    # sibling keys deep-merge over the target
    over = _entry(
        {"roster": {"reviewer": {"$ref": "experts/critic", "description": "D"}}},
        "reviewer",
    )
    assert over["description"] == "D"
    assert over["tools"]["shell"] == critic["tools"]["shell"]


def test_ref_library_entry():
    explorer = _srw_config_fragment(
        yaml.safe_load(
            (_CONFIG / "subagents" / "explorer" / "config.yaml").read_text(
                encoding="utf-8"
            )
        )
    )
    for ref in ("subagents/explorer", "explorer"):
        entry = _entry({"roster": {"explorer": {"$ref": ref}}}, "explorer")
        assert entry["_ref"] == ref
        assert entry["_ref_kind"] == "library"
        assert entry["_deployment_dir"] == "config/subagents/explorer"
        assert entry["tags"] == ["subagent"]
        assert entry["description"].strip()
        assert entry["display_name"] == "Explorer"
        for group, names in explorer["tools"].items():
            assert entry["tools"][group] == names, group
        assert "write_file" not in entry["tools"]["workspace"]
        assert entry["tools"]["browser_direct"] == []
        assert entry["llm"]["model"] == "claude-opus-4-1"  # its own `inherit`
        for dotted in _SUBAGENT_IGNORED:
            assert not _present(entry, dotted), dotted


def test_bound_catalog_uuid_keeps_bundled_harness_asset_directory():
    fragment = _srw_config_fragment(
        yaml.safe_load((_CONFIG / "experts/developer/config.yaml").read_text())
    )
    row = _db_row(
        name="developer",
        config=fragment,
        prompts={},
        harness_adapter="srw/v1",
        harness_config_name="worker_base",
        harness_asset_name="developer",
    )
    by_name = _entry({"roster": {"builder": {"$ref": "developer"}}}, "builder")
    by_id = _entry(
        {"roster": {"builder": {"$ref": _UUID}}}, "builder", db_refs={_UUID: row}
    )
    assert by_id["_deployment_dir"] == by_name["_deployment_dir"]
    # Optional unset fields can be represented by either absence or None after
    # the private SRW merge; the routed/model-family values must agree.
    assert {key: value for key, value in by_id["llm"].items() if value is not None} == {
        key: value for key, value in by_name["llm"].items() if value is not None
    }
    assert by_id.get("prompts") == by_name.get("prompts")


def test_ref_db_row_via_prefetched_map():
    row = _db_row()
    entry = _entry(
        {"roster": {"reader": {"$ref": _UUID}}}, "reader", db_refs={_UUID: row}
    )
    assert entry["llm"]["model"] == "claude-sonnet-4-5"
    assert ROSTER_INHERIT_MARKER not in entry["llm"]
    assert entry["limits"]["model_max_context_tokens"] == 1000000  # sonnet family
    assert entry["_ref"] == _UUID
    assert entry["_ref_kind"] == "db"
    assert entry["_ref_name"] == "sonnet-reader"
    assert "_deployment_dir" not in entry
    # row columns fill what the fragment does not author
    assert entry["display_name"] == "Sonnet Reader"
    assert entry["description"] == "Reads sources for the parent."
    assert entry["tags"] == ["reader"]
    assert entry["agent_id"] == "reader"
    # the row's prompt text is inlined and marked DB-authored
    assert entry["prompts"] == {"persona": "READER-PERSONA", "instructions": ""}
    assert entry["_persona_source"] == "db"
    assert entry["_db_prompt_keys"] == ["persona"]
    # parent-only keys the row authored are pruned; its tools kept
    assert "backend" not in entry["workspace"]
    assert "autonomy" not in entry
    assert entry["tools"]["workspace"] == ["read_file"]
    # the map is matched by uuid string regardless of case / key type
    upper = _entry(
        {"roster": {"reader": {"$ref": _UUID}}}, "reader", db_refs={_UUID.upper(): row}
    )
    assert upper["_ref_name"] == "sonnet-reader"
    # JSON-string config/prompts (asyncpg without a codec) parse too
    as_text = _db_row(
        config=json.dumps(row["config"]), prompts=json.dumps(row["prompts"])
    )
    text_entry = _entry(
        {"roster": {"reader": {"$ref": _UUID}}}, "reader", db_refs={_UUID: as_text}
    )
    assert text_entry["llm"]["model"] == "claude-sonnet-4-5"
    assert text_entry["prompts"]["persona"] == "READER-PERSONA"


@pytest.mark.parametrize(
    "config_name",
    [
        "expert_base",
        "worker_base",
        "session_base",
        "subagent_base",
        "default",
        "persistent_default",
        "overlays/worker",
    ],
)
def test_stored_roster_target_root_bases_re_root_onto_subagent(config_name):
    row = _db_row(harness_adapter="srw/v1", harness_config_name=config_name)
    entry = _entry(
        {"roster": {"reader": {"$ref": _UUID}}}, "reader", db_refs={_UUID: row}
    )
    assert entry["llm"]["model"] == "claude-sonnet-4-5"
    assert entry["tools"]["workspace"] == ["read_file"]
    assert entry["memory"]["enabled"] is False
    assert "backend" not in entry["workspace"]


@pytest.mark.parametrize("config_name", ["developer", "custom-team-base"])
def test_stored_roster_target_cannot_silently_ignore_non_root_base(config_name):
    row = _db_row(harness_adapter="srw/v1", harness_config_name=config_name)
    roster = {"roster": {"reader": {"$ref": _UUID}}}
    with pytest.raises(
        RosterResolutionError,
        match=rf"subagents\.roster\.reader: unsupported SRW config_name '{config_name}'",
    ):
        _resolve(roster, db_refs={_UUID: row})

    dropped = _resolve(roster, db_refs={_UUID: row}, on_missing="drop")
    assert dropped["subagents"]["roster"] == {}
    assert any(
        "unsupported SRW config_name" in warning and config_name in warning
        for warning in dropped[ROSTER_WARNINGS_KEY]
    )


def test_nested_roster_dropped():
    nested = {
        "default": "inner",
        "llm": {"model": "claude-haiku-4-5"},
        "roster": {"inner": {"$ref": "critic"}},
    }
    row = _db_row(config={**_db_row()["config"], "subagents": nested})
    entry = _entry(
        {"roster": {"reader": {"$ref": _UUID}}}, "reader", db_refs={_UUID: row}
    )
    assert "subagents" not in entry
    # and the target's roster-wide llm does not leak into the entry's model
    assert entry["llm"]["model"] == "claude-sonnet-4-5"
    inline = _entry({"roster": {"x": {"subagents": nested}}}, "x")
    assert "subagents" not in inline


# ---------------------------------------------------------------------------
# Failure policy
# ---------------------------------------------------------------------------


def test_unknown_ref_raises():
    with pytest.raises(
        RosterResolutionError, match=r"subagents\.roster\.x: .*not found"
    ):
        _resolve({"roster": {"x": {"$ref": "no-such-expert"}}})
    # never a path, never a chain root, never a single-file config
    for bad in ("../experts/critic", "/etc/passwd", "worker_base", "interactive", ""):
        with pytest.raises(RosterResolutionError, match=r"subagents\.roster\.x"):
            _resolve({"roster": {"x": {"$ref": bad}}})
    with pytest.raises(RosterResolutionError, match="must be a mapping"):
        _resolve({"roster": {"x": "critic"}})


def test_unknown_ref_is_dropped_and_recorded_at_dispatch():
    out = _resolve(
        {
            "default": "x",
            "roster": {"x": {"$ref": "no-such-expert"}, "ok": {"$ref": "explorer"}},
        },
        on_missing="drop",
    )
    assert list(out["subagents"]["roster"]) == ["ok"]
    assert out["subagents"]["default"] == "x"  # kept verbatim, but flagged
    warnings = out[ROSTER_WARNINGS_KEY]
    assert any("subagents.roster.x" in w and "entry dropped" in w for w in warnings)
    assert any("subagents.default 'x' names no roster entry" in w for w in warnings)


def test_db_ref_without_a_prefetched_row_is_dropped_under_both_policies():
    for policy in ("raise", "drop"):
        out = _resolve({"roster": {"ghost": {"$ref": _UUID}}}, on_missing=policy)
        assert out["subagents"]["roster"] == {}
        assert any(
            "subagents.roster.ghost" in w and "no prefetched row" in w
            for w in out[ROSTER_WARNINGS_KEY]
        )


def test_bad_policy_and_malformed_block_shapes():
    with pytest.raises(ValueError, match="on_missing"):
        _resolve({"roster": {}}, on_missing="ignore")
    out = _resolve({"llm": "haiku", "roster": ["explorer"]})
    assert out["subagents"] == {"default": None, "llm": {}, "roster": {}}
    assert len(out[ROSTER_WARNINGS_KEY]) == 2
    out = _resolve("explorer")
    assert out["subagents"] == {"default": None, "llm": {}, "roster": {}}


def test_missing_block_is_left_alone():
    data = {"agent_id": "p", "display_name": "P", "llm": dict(_PARENT_LLM)}
    assert resolve_subagent_roster(dict(data), db_refs={}) == data
    empty = _resolve({})
    assert empty["subagents"] == {"default": None, "llm": {}, "roster": {}}
    assert ROSTER_WARNINGS_KEY not in empty


@pytest.fixture
def library(tmp_path, monkeypatch):
    """A throwaway config root with a `subagents/` library the resolver
    locates through the same seam as the shipped one."""
    root = tmp_path / "config"
    (root / "subagents").mkdir(parents=True)
    (root / "experts").mkdir()
    monkeypatch.setattr(subagent_roster, "_config_root", lambda: root)

    def add(name: str, kind: str = "subagents", **body) -> Path:
        directory = root / kind / name
        directory.mkdir(exist_ok=True)
        path = directory / "config.yaml"
        path.write_text(yaml.safe_dump(body, sort_keys=False), encoding="utf-8")
        return path

    return add


def test_ref_cycle_raises(library):
    a = library("a", agent_id="a", display_name="A")
    b = library("b", agent_id="b", display_name="B")
    a.write_text(
        yaml.safe_dump({"$extends": str(b), "agent_id": "a"}), encoding="utf-8"
    )
    b.write_text(
        yaml.safe_dump({"$extends": str(a), "agent_id": "b"}), encoding="utf-8"
    )
    with pytest.raises(RosterResolutionError, match=r"subagents\.roster\.x: .*cycle"):
        _resolve({"roster": {"x": {"$ref": "a"}}})
    # dispatch: dropped, not fatal
    out = _resolve({"roster": {"x": {"$ref": "a"}}}, on_missing="drop")
    assert out["subagents"]["roster"] == {}
    assert any("cycle" in w for w in out[ROSTER_WARNINGS_KEY])


def test_ref_chain_too_deep_raises(library):
    """`MAX_REF_HOPS` expert-to-expert links are fine; one more is refused."""

    def chain(depth: int) -> str:
        links = [library(f"e{i}", agent_id=f"e{i}") for i in range(depth + 1)]
        for i, path in enumerate(links):
            parent = str(links[i + 1]) if i + 1 < len(links) else "expert_base"
            path.write_text(
                yaml.safe_dump({"$extends": parent, "agent_id": f"e{i}"}),
                encoding="utf-8",
            )
        return "e0"

    ok = _resolve({"roster": {"x": {"$ref": chain(MAX_REF_HOPS)}}})
    assert ok["subagents"]["roster"]["x"]["agent_id"] == "x"
    with pytest.raises(RosterResolutionError, match=r"deeper than"):
        _resolve({"roster": {"x": {"$ref": chain(MAX_REF_HOPS + 1)}}})


def test_library_target_without_extends_still_lands_on_the_subagent_base(library):
    library(
        "bare", agent_id="bare", display_name="Bare", tools={"workspace": ["read_file"]}
    )
    entry = _entry({"roster": {"x": {"$ref": "bare"}}}, "x")
    assert entry["tools"]["workspace"] == ["read_file"]
    assert entry["interactive"]["permission_mode"] == "autonomous"
    assert entry["memory"]["enabled"] is False
    assert entry["_ref_kind"] == "library"


def test_bundled_target_wins_over_a_library_entry_of_the_same_name(library):
    library("twin", kind="experts", agent_id="twin", display_name="Bundled")
    library("twin", agent_id="twin", display_name="Library")
    bare = _entry({"roster": {"x": {"$ref": "twin"}}}, "x")
    assert bare["_ref_kind"] == "bundled" and bare["display_name"] == "Bundled"
    explicit = _entry({"roster": {"x": {"$ref": "subagents/twin"}}}, "x")
    assert explicit["_ref_kind"] == "library" and explicit["display_name"] == "Library"
    assert (
        _entry({"roster": {"x": {"$ref": "experts/twin"}}}, "x")["display_name"]
        == "Bundled"
    )


# ---------------------------------------------------------------------------
# Settings matrix per entry, legacy tier, hooks
# ---------------------------------------------------------------------------


def test_matrix_applied_per_entry_family():
    gemma = "RedHatAI/gemma-4-31B-it-FP8-Dynamic"
    out = _resolve(
        {
            "roster": {
                "sonnet": {"llm": {"model": "claude-sonnet-4-5"}},
                "haiku": {"llm": {"model": "claude-haiku-4-5"}},
                "twin": {},
            }
        },
        llm={"model": gemma},
    )
    roster = out["subagents"]["roster"]
    assert roster["sonnet"]["limits"]["model_max_context_tokens"] == 1000000
    assert roster["sonnet"]["llm"]["multimodal"] is True
    assert roster["haiku"]["limits"]["model_max_context_tokens"] == 200000
    assert roster["twin"]["llm"]["model"] == gemma
    assert roster["twin"]["limits"]["model_max_context_tokens"] == 131072
    # the parent's own matrix pass is the caller's business, not the roster's
    assert "limits" not in out


def test_legacy_llm_subagent_becomes_roster_wide_llm():
    blob = resolve_config(
        base_config_name="session_base",
        request_override={
            "llm": {"subagent": {"model": "claude-haiku-4-5"}},
            "subagents": {"roster": {"reader": {}, "explorer": {"$ref": "explorer"}}},
        },
        expert_type="session",
    )
    agent = blob["agent"]
    assert "subagent" not in agent["llm"]
    assert agent["subagents"]["llm"] == {"model": "claude-haiku-4-5"}
    assert agent["subagents"]["roster"]["reader"]["llm"]["model"] == "claude-haiku-4-5"
    assert (
        agent["subagents"]["roster"]["explorer"]["llm"]["model"] == "claude-haiku-4-5"
    )
    cfg = load_config_from_resolved(blob)
    assert cfg.subagents.llm == {"model": "claude-haiku-4-5"}
    assert set(cfg.subagents.roster) == {"reader", "explorer"}


def test_job_override_deep_merges_into_entry():
    """``config_override.subagents.roster.explorer.llm.model`` lands on the
    entry BEFORE it is materialised: the pin wins over the library entry's
    `inherit`, the rest of the entry is the library entry."""
    blob = resolve_config(
        base_config_name="worker_base",
        expert_row={
            "expert_type": "worker",
            "name": "lead",
            "config": {
                "llm": {"model": "claude-opus-4-1"},
                "subagents": {"roster": {"explorer": {"$ref": "subagents/explorer"}}},
            },
            "prompts": {},
        },
        request_override={
            "subagents": {
                "roster": {
                    "explorer": {
                        "llm": {"model": "claude-haiku-4-5"},
                        "limits": {"max_turns": 5},
                    }
                }
            }
        },
        expert_type="worker",
    )
    entry = blob["agent"]["subagents"]["roster"]["explorer"]
    assert entry["llm"]["model"] == "claude-haiku-4-5"
    assert ROSTER_INHERIT_MARKER not in entry["llm"]
    assert entry["limits"]["max_turns"] == 5
    assert entry["limits"]["model_max_context_tokens"] == 200000  # haiku's window
    assert entry["_ref_kind"] == "library"
    assert "write_file" not in entry["tools"]["workspace"]
    assert blob["agent"]["llm"]["model"] == "claude-opus-4-1"


def test_resolve_config_materialises_the_roster_into_the_blob():
    """The blob path end to end: three entries (bundled + library + inline),
    a DB ref no one prefetched, the PDP capture, hydration, secrets, size."""
    parent = {
        "expert_type": "worker",
        "name": "lead",
        "config": {"llm": {"model": "claude-opus-4-1"}},
        "prompts": {},
    }
    roster = {
        "default": "explorer",
        "roster": {
            "explorer": {"$ref": "subagents/explorer"},
            "reviewer": {"$ref": "critic"},
            "implementer": {
                "description": "Implements one change.",
                "tools": {"workspace": ["read_file", "write_file"]},
            },
            "ghost": {"$ref": _UUID},
        },
    }
    capture: dict = {}
    plain = resolve_config(
        base_config_name="worker_base", expert_row=parent, expert_type="worker"
    )
    blob = resolve_config(
        base_config_name="worker_base",
        expert_row={**parent, "config": {**parent["config"], "subagents": roster}},
        expert_type="worker",
        capture=capture,
    )
    agent = blob["agent"]
    assert set(agent["subagents"]["roster"]) == {"explorer", "reviewer", "implementer"}
    assert agent["subagents"]["default"] == "explorer"
    assert any("subagents.roster.ghost" in w for w in agent[ROSTER_WARNINGS_KEY])
    # the PDP sees resolved entries (the critic's shell), not `{$ref: critic}`
    fragment = capture["merged_fragment"]["subagents"]["roster"]["reviewer"]
    assert fragment["tools"]["shell"] == ["run_command", "cancel_command"]
    assert "$ref" not in fragment
    # hydration: the parsed field, never extra
    cfg = load_config_from_resolved(blob)
    assert isinstance(cfg.subagents, SubagentsConfig)
    assert set(cfg.subagents.roster) == {"explorer", "reviewer", "implementer"}
    assert "subagents" not in cfg.extra
    assert cfg.extra[ROSTER_WARNINGS_KEY]
    # size: a soft ceiling on blob growth (u1_plan.md D.12)
    growth = len(json.dumps(blob)) - len(json.dumps(plain))
    assert 0 < growth <= 40_000, growth


def test_serialize_strips_roster_api_keys():
    out = _resolve(
        {
            "llm": {"model": "claude-haiku-4-5", "api_key": "roster-secret"},
            "roster": {"twin": {"llm": {"model": INHERIT_MODEL}}, "own": {}},
        },
        llm={**_PARENT_LLM, "api_key": "parent-secret"},
    )
    assert out["subagents"]["roster"]["twin"]["llm"]["api_key"] == "parent-secret"
    assert out["subagents"]["roster"]["own"]["llm"]["api_key"] == "roster-secret"
    cfg = load_agent_config_from_dict(out)
    blob = serialize_resolved_config(cfg, model=cfg.llm.model)
    agent = blob["agent"]
    assert "api_key" not in agent["llm"]
    assert "api_key" not in agent["subagents"]["llm"]
    for entry in agent["subagents"]["roster"].values():
        assert "api_key" not in entry["llm"]
    assert "parent-secret" not in json.dumps(blob)
    assert "roster-secret" not in json.dumps(blob)


def test_load_agent_config_resolves_a_bundled_roster_from_disk(tmp_path):
    """The agent's disk path: bundled / library refs resolve, a DB ref drops
    with a warning, and an unknown disk ref is an authoring error."""
    leaf = tmp_path / "lead.yaml"
    leaf.write_text(
        yaml.safe_dump(
            {
                "$extends": "worker_base",
                "agent_id": "lead",
                "display_name": "Lead",
                "tags": ["worker", "lead"],
                "subagents": {
                    "default": "explorer",
                    "roster": {
                        "explorer": {"$ref": "subagents/explorer"},
                        "reviewer": {"$ref": "critic"},
                        "ghost": {"$ref": _UUID},
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    cfg = load_agent_config(str(leaf))
    assert cfg.tags == ["worker", "lead"]
    assert set(cfg.subagents.roster) == {"explorer", "reviewer"}
    assert cfg.subagents.default == "explorer"
    assert cfg.subagents.roster["reviewer"]["tools"]["shell"] == [
        "run_command",
        "cancel_command",
    ]
    assert cfg.subagents.roster["explorer"]["llm"]["model"] == cfg.llm.model
    assert any("ghost" in w for w in cfg.extra[ROSTER_WARNINGS_KEY])
    assert "subagents" not in cfg.extra and "tags" not in cfg.extra
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        yaml.safe_dump(
            {
                "$extends": "worker_base",
                "agent_id": "bad",
                "display_name": "Bad",
                "subagents": {"roster": {"x": {"$ref": "no-such-expert"}}},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RosterResolutionError):
        load_agent_config(str(bad))


def test_public_bases_carry_no_roster_and_the_rostered_experts_resolve_the_library():
    """Public bases stay neutral; the five U3 parents and the seeded session
    assistant resolve usable rosters (the assistant gained its roster
    2026-09-07 — before that, a ticked Delegation on the default session expert
    bound ``delegate_agent`` with nothing to delegate to)."""
    for name in ("worker_base", "session_base", "subagent_base"):
        path, deployment_dir = resolve_config_path(name)
        cfg = load_agent_config(path, deployment_dir)
        assert cfg.subagents == SubagentsConfig(), name
        assert "subagents" not in cfg.extra, name
        assert cfg.delegation.max_concurrent == 4, name
        assert cfg.delegation.run_in_background_default is False, name

    expected = {
        "developer": (
            "explorer",
            {"explorer", "implementer", "tester", "reviewer"},
        ),
        "critic": ("verifier", {"explorer", "verifier"}),
        "scholar": ("reader", {"explorer", "reader"}),
        "bughunter": ("probe", {"explorer", "probe"}),
        "product-qa": ("probe", {"explorer", "probe"}),
        "assistant": ("explorer", {"explorer", "reader", "implementer"}),
    }
    for expert, (default, names) in expected.items():
        cfg = load_agent_config(*resolve_config_path(expert))
        assert cfg.subagents.default == default
        assert set(cfg.subagents.roster) == names
        parent_names = get_all_tool_names(cfg)
        for name, entry in cfg.subagents.roster.items():
            library_leaf = _CONFIG / "subagents" / name / "config.yaml"
            assert entry["_ref_kind"] == "library"
            assert entry["_ref"] == f"subagents/{name}"
            assert entry["_deployment_dir"] == f"config/subagents/{name}"
            assert resolve_config_path(entry["_ref"])[0] == str(library_leaf)

            raw = _srw_config_fragment(
                yaml.safe_load(library_leaf.read_text(encoding="utf-8"))
            )
            budgets = ChildBudgets.from_entry(entry, name)
            declared = raw.get("limits") or dict(
                zip(
                    (
                        "max_turns",
                        "max_tokens",
                        "return_budget_tokens",
                        "stale_idle_s",
                        "stale_in_tool_s",
                    ),
                    BUILTIN_DEFAULTS[name],
                    strict=True,
                )
            )
            for key, value in declared.items():
                assert getattr(budgets, key) == value

            selected, _dropped = select_child_tool_names(
                entry_tool_names(entry),
                parent_names,
                write_policy=str(entry.get("write_policy") or "none"),
            )
            assert selected, f"{expert}/{name} resolved no child tools"
            assert not (set(selected) & DELEGATION_TOOL_NAMES)
            for tool_name in selected:
                assert TOOL_REGISTRY[tool_name]["category"] not in (
                    CONTROL_PLANE_CATEGORIES
                )


@pytest.mark.parametrize("shell_mode", ["stateless", "persistent"])
def test_a_shell_granting_library_entry_gets_a_command_executor(shell_mode):
    """A library child that is granted a shell can run a command in either
    shell mode of its parent.

    The child's tools are ``entry ∩ parent``. ``get_all_tool_names`` keeps one
    executor name per parent: ``run_command`` when stateless, ``shell_execute``
    when persistent — what every capable model family's ``shell_mode`` sets.
    Tester, implementer, reviewer and verifier named only ``run_command``, so on
    a persistent parent they got ``shell_read`` and ``cancel_command`` and
    nothing to run a command with. The test above resolves its parents without
    the matrix, i.e. stateless, and passed.
    """
    executors = {"run_command", "shell_execute"}
    checked: set[str] = set()
    for expert in ("developer", "critic", "bughunter", "product-qa", "assistant"):
        cfg = load_agent_config(*resolve_config_path(expert))
        shell_cfg = cfg.extra.get("shell")
        cfg.extra["shell"] = {
            **(shell_cfg if isinstance(shell_cfg, dict) else {}),
            "mode": shell_mode,
        }
        parent_names = get_all_tool_names(cfg)
        if not executors & set(parent_names):
            continue  # a parent without a shell cannot lend one
        for name, entry in cfg.subagents.roster.items():
            declared = set(entry_tool_names(entry))
            if not any(
                (TOOL_REGISTRY.get(n) or {}).get("category") == "shell"
                for n in declared
            ):
                continue
            selected, dropped = select_child_tool_names(
                entry_tool_names(entry),
                parent_names,
                write_policy=str(entry.get("write_policy") or "none"),
            )
            assert executors & set(selected), (
                f"{expert}/{name} on a {shell_mode} parent binds no command "
                f"executor: selected={selected}, dropped={dropped}"
            )
            checked.add(name)
    assert {"implementer", "tester", "reviewer", "verifier", "probe"} <= checked


# --- the live voice entry (live_session_jobs §6.3) ---------------------------


def _voice(**entry):
    return {"roster": {"voice": {"description": "The live session.", **entry}}}


def test_a_live_voice_entry_passes_the_authoring_check():
    from shared.runtime.core.subagent_roster import validate_roster_fragment

    assert (
        validate_roster_fragment(
            _voice(
                driver="live",
                live={
                    "persona": "A calm voice.",
                    "facts_file": "facts.md",
                    "greeting": "Hello, you are speaking with an AI assistant.",
                    "voice": "marin",
                    "answer_timeout_s": 25,
                    "texts": {"follow_up": "Say it is noted.", "goodbye": "Bye."},
                },
            )
        )
        == set()
    )
    # The bundled receptionist's own entry.
    raw = yaml.safe_load(
        (
            Path(__file__).resolve().parents[1]
            / "config/experts/receptionist/config.yaml"
        ).read_text()
    )
    validate_roster_fragment(raw["spec"]["runtime"]["config"]["config"]["subagents"])


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        ({"driver": "phone"}, "is not a driver"),
        ({"driver": "phone", "live": {"greeting": "Hi."}}, "is not a driver"),
        ({"driver": "live", "live": "x"}, "expected a mapping"),
        # Without a driver the fragment overrides an inherited entry: the
        # live keys it sets are still checked.
        ({"live": "x"}, "expected a mapping"),
        ({"live": {"greeting": 3}}, "expected a string"),
        ({"live": {"facts": "Remain {expert_identity}."}}, "reserved prompt"),
        ({"live": {"persona": "x", "persona_file": "p.txt"}}, "not both"),
        (
            {"driver": "live", "live": {"persona": "x", "persona_file": "p.txt"}},
            "not both",
        ),
        ({"driver": "live", "live": {"facts_file": "../secrets"}}, "a file name"),
        ({"driver": "live", "live": {"greeting": 3}}, "expected a string"),
        ({"driver": "live", "live": {"answer_timeout_s": 0}}, "between 1 and 600"),
        ({"driver": "live", "live": {"texts": {"hello": "x"}}}, "texts"),
        (
            {"driver": "live", "live": {"facts": "Remain {expert_identity}."}},
            "reserved prompt placeholders",
        ),
    ],
)
def test_a_malformed_live_voice_entry_is_refused(entry, message):
    from shared.runtime.core.subagent_roster import (
        RosterResolutionError,
        validate_roster_fragment,
    )

    with pytest.raises(RosterResolutionError, match=message):
        validate_roster_fragment(_voice(**entry))


# --- a partial override of an inherited live entry ---------------------------
#
# A DB expert extending the receptionist overrides only
# `subagents.roster.voice.live.{greeting, facts}` (receptionist_expert.md). Its
# fragment is checked as one layer (it inherits `driver: live`); the merged
# entry is checked where the layers merge, under both failure policies.

_GREETING = "Hello, this is the Example desk's AI assistant."
_FACTS = "The Example desk is open Monday to Friday."
_PARTIAL_VOICE = {
    "subagents": {
        "roster": {"voice": {"live": {"greeting": _GREETING, "facts": _FACTS}}}
    }
}


def _receptionist_row(config: dict) -> dict:
    """A DB expert built on the bundled receptionist, as a manifest saves it
    (`config_name: receptionist`, `asset_name: receptionist`)."""
    return {
        "name": "example-desk",
        "expert_type": "worker",
        "harness_config_name": "receptionist",
        "harness_asset_name": "receptionist",
        "config": config,
        "prompts": {},
    }


def test_a_fragment_overriding_part_of_an_inherited_live_entry_passes():
    from shared.runtime.core.subagent_roster import validate_roster_fragment

    assert validate_roster_fragment(_PARTIAL_VOICE["subagents"]) == set()


def test_a_partial_live_override_resolves_onto_the_receptionist_voice():
    blob = resolve_config(
        base_config_name="worker_base",
        expert_row=_receptionist_row(_PARTIAL_VOICE),
        expert_type="worker",
    )
    voice = blob["agent"]["subagents"]["roster"]["voice"]
    assert voice["driver"] == "live"
    assert voice["live"]["greeting"] == _GREETING
    assert voice["live"]["facts"] == _FACTS
    # Everything the fragment did not set is the receptionist's.
    assert voice["live"]["persona"].startswith("You are the receptionist")
    assert voice["live"]["voice"] == "marin"
    assert ROSTER_WARNINGS_KEY not in blob["agent"]


@pytest.mark.parametrize("on_missing", ["raise", "drop"])
def test_a_live_block_on_an_entry_that_is_not_live_is_refused_at_resolution(
    on_missing,
):
    from shared.runtime.core.subagent_roster import (
        LiveEntryError,
        validate_roster_fragment,
    )

    subagents = {
        "roster": {
            "helper": {
                "description": "An LLM helper.",
                "live": {"greeting": _GREETING},
            }
        }
    }
    # One layer cannot tell: the entry might inherit `driver: live`.
    assert validate_roster_fragment(subagents) == set()
    # The merged entry can, and the error is never a silent drop.
    with pytest.raises(LiveEntryError, match=r"helper\.live: requires driver: live"):
        _resolve(subagents, on_missing=on_missing)


def test_a_live_block_added_to_a_non_live_entry_fails_resolve_config():
    from shared.runtime.core.subagent_roster import LiveEntryError

    row = {
        "name": "lead",
        "expert_type": "worker",
        "config": {
            "subagents": {
                "roster": {
                    "explorer": {
                        "$ref": "subagents/explorer",
                        "live": {"greeting": _GREETING},
                    }
                }
            }
        },
        "prompts": {},
    }
    with pytest.raises(LiveEntryError, match=r"explorer\.live: requires driver: live"):
        resolve_config(base_config_name="worker_base", expert_row=row)


def test_clearing_the_inherited_driver_is_refused_at_resolution():
    from shared.runtime.core.subagent_roster import LiveEntryError

    config = {"subagents": {"roster": {"voice": {"driver": None}}}}
    with pytest.raises(LiveEntryError, match=r"voice\.live: requires driver: live"):
        resolve_config(
            base_config_name="worker_base",
            expert_row=_receptionist_row(config),
            expert_type="worker",
        )


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        ({"driver": "phone"}, "is not a driver"),
        # Each layer is well formed; the merge gives both persona sources.
        ({"live": {"persona_file": "persona.txt"}}, "not both"),
    ],
)
def test_the_merged_live_entry_is_checked_whole(entry, message):
    from shared.runtime.core.subagent_roster import LiveEntryError

    config = {"subagents": {"roster": {"voice": entry}}}
    with pytest.raises(LiveEntryError, match=message):
        resolve_config(
            base_config_name="worker_base",
            expert_row=_receptionist_row(config),
            expert_type="worker",
        )
