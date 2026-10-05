"""Golden contract for the U1 root split (universal experts, WP2).

Before WP2 every expert extended one of two self-contained bases,
``config/worker_base.yaml`` or ``config/session_base.yaml``. WP2 split them
into ONE shared root (``config/expert_base.yaml``) plus a role overlay each
(``config/overlays/{worker,session,subagent}.yaml``), kept the public names as
aliases of the overlays, and taught the loader to re-root any chain onto a
requested role. The contract this file pins:

1. **Neutrality.** For every bundled expert, in the role it was authored for,
   the merged dict AND the effective (matrix-applied, serialized) config are
   identical to what the pre-split base produced. The pre-split bases are
   frozen under ``tests/fixtures/pre_split/`` (verbatim copies of commit
   ``a8950251``) and resolved through the real loader by pointing a copy of
   each leaf's ``$extends`` at the fixture — so the comparison exercises the
   same code path, not a re-implementation.
2. **Universality.** Every bundled expert resolves in every role; the
   subagent overlay's ``$ignore_keys`` are honoured (pruned, never errors); a
   session expert re-rooted as a worker gains the worker keys and vice versa.
3. **Aliases.** ``worker_base`` / ``session_base`` (and the legacy
   ``default(s)`` / ``persistent_default(s)``) stay valid names and path
   forms, canonicalise to the PUBLIC names, and resolve to the overlay files.

If neutrality fails, the split leaked a value — fix the YAML, never the
fixture. The fixtures only change when a new pre-split baseline is
deliberately frozen.
"""

from __future__ import annotations

from shared.runtime.core.srw_manifest_config import (
    srw_config_fragment as _srw_config_fragment,
)

import copy
import json
from pathlib import Path

import pytest
import yaml

from shared.runtime.core.loader import (
    EXPERT_BASE,
    IGNORE_KEYS_DIRECTIVE,
    ROLE_ROOTS,
    ROOT_NAMES,
    authored_llm_keys,
    canonical_config_name,
    chain_root,
    deep_merge,
    get_all_tool_names,
    load_agent_config,
    load_agent_config_from_dict,
    load_and_merge_config,
    load_role_base,
    prune_ignored_keys,
    reroot_extends,
    resolve_config_path,
    role_of_root,
    serialize_resolved_config,
)

_REPO = Path(__file__).resolve().parents[1]
_CONFIG = _REPO / "config"
_FIXTURES = Path(__file__).parent / "fixtures" / "pre_split"
#: role -> the frozen pre-split base for that role.
_PRE_SPLIT = {
    "worker": _FIXTURES / "worker_base.a8950251.yaml",
    "session": _FIXTURES / "session_base.a8950251.yaml",
}
#: The keys the design note names as parent-only (§1.1) — the subagent overlay
#: must ignore at least these; it may ignore more.
_NOTE_IGNORED = {
    "workspace.backend",
    "autonomy",
    "verification",
    "phase_settings",
    "delegation",
    "communication",
    "scholar",
    "curator",
}
#: Keys later work packages added to an overlay ON TOP of the frozen
#: baseline (role -> dotted paths, with the WP that added them). They are
#: deleted from the post-split dict before the neutrality comparison, so the
#: split's proof stays exact while the overlays keep evolving — and asserted
#: PRESENT first, so a stale entry here cannot hide a real leak.
_POST_SPLIT_OVERLAY_ADDITIONS: dict[str, tuple[str, ...]] = {
    # WP3: the roster runtime knobs on DelegationConfig (schema + overlay
    # values; the dataclass defaults are identical, so the effective-config
    # identity below holds without an exclusion).
    # Summarizer thinking off: the shared root states
    # auxiliary.summarization_reasoning_level (both roles; same value as the
    # dataclass default, so the effective compare needs no exclusion).
    "worker": (
        "delegation.max_concurrent",
        "delegation.run_in_background_default",
        "auxiliary.summarization_reasoning_level",
    ),
    # U3 WP1: the persistent loop reads limits.llm_inproc_retries (it was a
    # hard-coded 3); the overlay pins the historical 3 against the dataclass
    # default of 5, so the effective compare strips it too (see
    # ``_without_effective_additions``).
    "session": (
        "limits.llm_inproc_retries",
        "auxiliary.summarization_reasoning_level",
    ),
}
#: Keys later work packages DELETED from an overlay after the baseline was
#: frozen (role -> dotted paths, with the WP that removed them) — the mirror
#: of the additions above: asserted ABSENT in the post-split dict and PRESENT
#: in the frozen baseline first (a stale entry cannot hide a leak either way),
#: then removed from the pre-split side before the comparison.
_POST_SPLIT_OVERLAY_REMOVALS: dict[str, tuple[str, ...]] = {
    # U3 WP4: a delegation batch runs without the tool-batch watchdog (B.6),
    # so the per-category ceiling for `delegation` left the worker overlay
    # with the light reader it was sized for.
    # Manifest workspace ownership: infrastructure now comes from the
    # execution resolver, so the shared Expert root no longer selects it.
    # rerank catalog capability (migration 0240): the shared root no longer
    # names the reranker model — the `rerank` pin arrives as RERANK_MODEL at
    # dispatch and the loader default is None, so the key left both overlays.
    # Per-turn tool-result clearing was retired (it broke the prompt cache
    # every turn), so its window key left the shared root.
    "worker": (
        "limits.tool_category_timeouts.delegation",
        "workspace.backend",
        "memory.reranker.model",
        "context_management.keep_recent_tool_results",
    ),
    "session": (
        "workspace.backend",
        "memory.reranker.model",
        "context_management.keep_recent_tool_results",
    ),
}
#: Tool names later work packages added to a tool CATEGORY on top of the
#: frozen baseline (role -> category -> names, with the work that added them).
#: A category list replaces wholesale on merge, so a dotted path cannot name
#: one entry and an expert that restates the category carries the addition in
#: its own leaf: they are matched by name and stripped from BOTH sides of the
#: comparison. Presence is guarded once, on the role base, by
#: ``test_post_split_tool_additions_are_actually_there`` — a stale entry here
#: cannot hide a real leak either.
_POST_SPLIT_TOOL_ADDITIONS: dict[str, dict[str, tuple[str, ...]]] = {
    # KB gardening slice 1: kb_delete (the retire tombstone) joined the shared
    # root's knowledge category, so every expert that inherits it gained it.
    # kb_retrieval_hardening T7: kb_grep (literal/regex enumeration) joined
    # the shared root's knowledge category alongside kb_search.
    "worker": {"knowledge": ("kb_delete", "kb_grep")},
    "session": {"knowledge": ("kb_delete", "kb_grep")},
}
#: Bindings later work added to an overlay's ``instruction_files`` (role ->
#: skill names). The list replaces wholesale on merge, so a dotted path cannot
#: name one entry: they are matched by ``skill``, asserted PRESENT first (in
#: the merged dict AND in the frozen blob, where the freeze also keys the
#: skill body under the same name), then removed before the comparison.
_POST_SPLIT_OVERLAY_BINDINGS: dict[str, tuple[str, ...]] = {
    # U2 WP2: the phase skills replace the per-phase system-prompt swap
    # (config/skills/{strategic,tactical}-phase). scholar, product-qa and
    # designer restate them because their own lists replace the overlay's.
    "worker": ("strategic-phase", "tactical-phase"),
    "session": (),
}


def _bundled_configs() -> list[tuple[str, Path, str | None]]:
    """``(name, leaf path, deployment_dir)`` for every bundled expert and the
    standalone ``interactive`` session profile — discovered, so a new expert is
    covered on arrival."""
    out = [
        (p.parent.name, p, str(p.parent))
        for p in sorted(_CONFIG.glob("experts/*/config.yaml"))
        if p.parent.name != "__pycache__"
    ]
    out.append(("interactive", _CONFIG / "interactive.yaml", None))
    return out


_BUNDLED = _bundled_configs()
_IDS = [name for name, _, _ in _BUNDLED]


def _native_role(leaf: Path) -> str:
    root = chain_root(str(leaf))
    role = role_of_root(root)
    assert role in _PRE_SPLIT, f"{leaf} is rooted on {root!r}, not a pre-split role"
    return role


def _dotted_present(data: dict, dotted: str) -> bool:
    node = data
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return False
        node = node[part]
    return True


def _delete_dotted(data: dict, dotted: str) -> None:
    *parents, leaf = dotted.split(".")
    node = data
    for part in parents:
        node = node[part]
    del node[leaf]


def _without_post_split_additions(
    data: dict, role: str, *, require: bool = True
) -> dict:
    """``data`` minus the overlay keys added after the baseline was frozen
    (each one must actually be present — see ``_POST_SPLIT_OVERLAY_ADDITIONS``),
    and asserted free of the keys removed since (``_POST_SPLIT_OVERLAY_REMOVALS``).
    ``require=False`` is the PRE-split side of an expert whose own leaf restates
    an added key (stripped if present, never asserted)."""
    out = copy.deepcopy(data)
    for dotted in _POST_SPLIT_OVERLAY_ADDITIONS.get(role, ()):
        present = _dotted_present(out, dotted)
        assert present or not require, f"{dotted} is listed as added but absent"
        if present:
            _delete_dotted(out, dotted)
    if require:
        for dotted in _POST_SPLIT_OVERLAY_REMOVALS.get(role, ()):
            assert not _dotted_present(out, dotted), (
                f"{dotted} is listed as removed but present"
            )
    out = _without_post_split_tools(out, role)
    return _without_post_split_bindings(out, role, require=require)


def _without_post_split_tools(data: dict, role: str) -> dict:
    """``data`` (a merged dict or the effective ``agent`` blob) minus the tool
    names added to a category after the baseline was frozen
    (``_POST_SPLIT_TOOL_ADDITIONS``). Stripped wherever they appear, on both
    sides and without a presence assertion: an expert that restates the
    category carries the addition in its own leaf, and one that empties the
    category (``interactive``) never had it."""
    additions = _POST_SPLIT_TOOL_ADDITIONS.get(role, {})
    tools = data.get("tools")
    if not additions or not isinstance(tools, dict):
        return data
    out = copy.deepcopy(data)
    for category, names in additions.items():
        entries = out["tools"].get(category)
        if isinstance(entries, list):
            out["tools"][category] = [t for t in entries if t not in names]
    return out


def _without_post_split_removals(data: dict, role: str, *, require: bool) -> dict:
    """The PRE-split side minus the overlay keys deleted after the baseline was
    frozen. ``require`` asserts each one is present in the frozen base (the
    role-base compare); an expert leaf on the frozen base is stripped without
    the assertion."""
    out = copy.deepcopy(data)
    for dotted in _POST_SPLIT_OVERLAY_REMOVALS.get(role, ()):
        present = _dotted_present(out, dotted)
        assert present or not require, f"{dotted} is listed as removed but absent"
        if present:
            _delete_dotted(out, dotted)
    return out


def _without_post_split_bindings(data: dict, role: str, *, require: bool) -> dict:
    """``data`` minus the ``instruction_files`` bindings added after the
    baseline was frozen (``_POST_SPLIT_OVERLAY_BINDINGS``). ``require`` asserts
    each one is present first (the post-split side); the pre-split side of an
    expert that RESTATES the bindings in its own leaf carries them too and is
    stripped without the assertion."""
    out = copy.deepcopy(data)
    bindings = _POST_SPLIT_OVERLAY_BINDINGS.get(role, ())
    if not bindings:
        return out
    entries = out.get("instruction_files")
    if not isinstance(entries, list):
        assert not require, "instruction_files is listed as extended but absent"
        return out
    if require:
        for skill in bindings:
            assert any(e.get("skill") == skill for e in entries), (
                f"binding to {skill} is listed as added but absent"
            )
    out["instruction_files"] = [e for e in entries if e.get("skill") not in bindings]
    return out


def _without_effective_additions(agent: dict, role: str) -> dict:
    """``agent`` (the asdict blob) minus the dotted post-split additions whose
    overlay value differs from the dataclass default — present on both sides
    of the effective compare (the dataclass fills the pre-split side), so they
    are stripped from both without a presence assertion."""
    out = copy.deepcopy(agent)
    for dotted in (
        *_POST_SPLIT_OVERLAY_ADDITIONS.get(role, ()),
        *_POST_SPLIT_OVERLAY_REMOVALS.get(role, ()),
    ):
        *parents, leaf = dotted.split(".")
        node = out
        for part in parents:
            node = node.get(part) if isinstance(node, dict) else None
            if node is None:
                break
        if isinstance(node, dict):
            node.pop(leaf, None)
    return _without_post_split_tools(out, role)


def _effective(
    config_path: str, deployment_dir: str | None, role: str | None = None
) -> dict:
    """The deterministic part of the frozen blob for a config loaded from disk
    (minus the effective-side post-split additions of ``role`` when given)."""
    cfg = load_agent_config(config_path, deployment_dir)
    blob = serialize_resolved_config(cfg, model=cfg.llm.model)
    out = {k: blob[k] for k in ("agent", "prompts", "instructions")}
    if role is not None:
        out["agent"] = _without_effective_additions(out["agent"], role)
    return out


def _effective_without_post_split_bindings(
    config_path: str, deployment_dir: str | None, role: str, *, require: bool = True
) -> dict:
    """``_effective`` minus the post-split bindings: in ``agent.instruction_files``
    (asdict shape) and the skill bodies the freeze keys under the same names in
    ``instructions``. Dotted additions whose dataclass default equals the
    overlay value need no exclusion; the others are stripped on both sides by
    ``_without_effective_additions``."""
    blob = _effective(config_path, deployment_dir, role)
    blob["agent"] = _without_post_split_bindings(blob["agent"], role, require=require)
    for skill in _POST_SPLIT_OVERLAY_BINDINGS.get(role, ()):
        if require:
            assert blob["instructions"].get(skill), f"{skill} body is not frozen"
        blob["instructions"].pop(skill, None)
    return blob


@pytest.fixture(scope="module")
def pre_split_leaves(tmp_path_factory) -> dict[str, tuple[Path, str | None, str]]:
    """A copy of every bundled leaf whose ``$extends`` points at the frozen
    pre-split base of its role: ``name -> (tmp leaf, deployment_dir, role)``."""
    tmp = tmp_path_factory.mktemp("pre_split_leaves")
    out = {}
    for name, leaf, deployment_dir in _BUNDLED:
        role = _native_role(leaf)
        raw = _srw_config_fragment(yaml.safe_load(leaf.read_text(encoding="utf-8")))
        raw["$extends"] = str(_PRE_SPLIT[role].resolve())
        copy = tmp / f"{name}.yaml"
        copy.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
        out[name] = (copy, deployment_dir, role)
    return out


# ---------------------------------------------------------------------------
# The frozen baseline itself
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("role", sorted(_PRE_SPLIT))
def test_pre_split_fixture_is_the_a8950251_base(role):
    """Guard the guard: the fixture must still be the self-contained base it
    was frozen as (the alias machinery must not have been pointed at it)."""
    fixture = _PRE_SPLIT[role]
    assert fixture.is_file()
    assert canonical_config_name(str(fixture)) == str(fixture)
    raw = _srw_config_fragment(yaml.safe_load(fixture.read_text(encoding="utf-8")))
    assert raw["agent_id"] == ROLE_ROOTS[role]
    assert "$extends" not in raw


# ---------------------------------------------------------------------------
# 1. Neutrality
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("role", sorted(_PRE_SPLIT))
def test_post_split_tool_additions_are_actually_there(role):
    """The ledger the neutrality compare strips by must stay honest: every
    declared name is in the role base today and was NOT in the frozen
    baseline. A stale entry would silently excuse a real leak."""
    post = load_role_base(role).get("tools", {})
    pre = load_and_merge_config(str(_PRE_SPLIT[role])).get("tools", {})
    for category, names in _POST_SPLIT_TOOL_ADDITIONS.get(role, {}).items():
        for name in names:
            assert name in post.get(category, ()), (
                f"{category}.{name} is listed as added but absent"
            )
            assert name not in pre.get(category, ()), (
                f"{category}.{name} is listed as added but is in the baseline"
            )


@pytest.mark.parametrize("role", sorted(_PRE_SPLIT))
def test_role_base_merged_dict_is_identical_to_the_pre_split_base(role):
    pre = _without_post_split_removals(
        load_and_merge_config(str(_PRE_SPLIT[role])), role, require=True
    )
    post = _without_post_split_additions(load_role_base(role), role)
    assert post == pre
    assert json.dumps(post, sort_keys=True) == json.dumps(pre, sort_keys=True)
    # and by public name / re-rooting, the same dict again
    root_path, _ = resolve_config_path(ROLE_ROOTS[role])
    assert _without_post_split_additions(load_and_merge_config(root_path), role) == pre
    assert (
        _without_post_split_additions(load_and_merge_config(root_path, role=role), role)
        == pre
    )


@pytest.mark.parametrize("role", sorted(_PRE_SPLIT))
def test_role_base_effective_config_is_identical_to_the_pre_split_base(role):
    """``agent.py --config worker_base|session_base`` boots on the same
    effective config as before: matrix applied with the same explicit keys,
    same prompts, same instructions."""
    root_path, deployment_dir = resolve_config_path(ROLE_ROOTS[role])
    assert _effective_without_post_split_bindings(
        root_path, deployment_dir, role
    ) == _effective(str(_PRE_SPLIT[role]), None, role)


@pytest.mark.parametrize("role", sorted(_PRE_SPLIT))
def test_authored_llm_keys_of_a_root_match_the_pre_split_base(role):
    """The settings matrix must skip exactly the llm keys the framework base
    authored — now split over overlay + expert_base, so the union counts."""
    root_path, _ = resolve_config_path(ROLE_ROOTS[role])
    raw = _srw_config_fragment(
        yaml.safe_load(_PRE_SPLIT[role].read_text(encoding="utf-8"))
    )
    assert authored_llm_keys(root_path) == set(raw["llm"])
    assert authored_llm_keys(str(_PRE_SPLIT[role])) == set(raw["llm"])


@pytest.mark.parametrize("name", _IDS)
def test_bundled_expert_merged_dict_is_identical(name, pre_split_leaves):
    tmp_leaf, _, role = pre_split_leaves[name]
    leaf = next(p for n, p, _ in _BUNDLED if n == name)
    # The pre-split side is the CURRENT leaf on the frozen base: an expert that
    # restates the post-split bindings in its own leaf carries them here too.
    pre = _without_post_split_removals(
        _without_post_split_additions(
            load_and_merge_config(str(tmp_leaf)), role, require=False
        ),
        role,
        require=False,
    )
    post = _without_post_split_additions(load_and_merge_config(str(leaf)), role)
    post_role = _without_post_split_additions(
        load_and_merge_config(str(leaf), role=role), role
    )
    assert post == pre, name
    assert post_role == pre, name
    assert json.dumps(post, sort_keys=True) == json.dumps(pre, sort_keys=True)


@pytest.mark.parametrize("name", _IDS)
def test_bundled_expert_effective_config_is_identical(name, pre_split_leaves):
    tmp_leaf, deployment_dir, role = pre_split_leaves[name]
    leaf = next(p for n, p, _ in _BUNDLED if n == name)
    assert _effective_without_post_split_bindings(
        str(leaf), deployment_dir, role
    ) == _effective_without_post_split_bindings(
        str(tmp_leaf), deployment_dir, role, require=False
    ), name


# ---------------------------------------------------------------------------
# 2. Universality
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def subagent_ignored() -> list[str]:
    raw = _srw_config_fragment(
        yaml.safe_load((_CONFIG / "overlays" / "subagent.yaml").read_text())
    )
    declared = raw[IGNORE_KEYS_DIRECTIVE]
    assert _NOTE_IGNORED <= set(declared), "the note's parent-only keys are the floor"
    return declared


@pytest.mark.parametrize("name", _IDS)
def test_every_bundled_expert_resolves_in_the_subagent_role(name, subagent_ignored):
    leaf, deployment_dir = next((p, d) for n, p, d in _BUNDLED if n == name)
    data = load_and_merge_config(str(leaf), role="subagent")
    assert (
        data["agent_id"]
        == _srw_config_fragment(yaml.safe_load(leaf.read_text()))["agent_id"]
    )
    assert data[IGNORE_KEYS_DIRECTIVE] == subagent_ignored
    for dotted in subagent_ignored:
        assert not _dotted_present(data, dotted), f"{name}: {dotted} survived"
    assert data["tools"] and data["llm"]["model"]
    # The subagent overlay's own values sit under the expert
    assert data["interactive"]["permission_mode"] in {"autonomous", "supervised"}
    # and the whole thing parses — with the matrix applied — like any config.
    cfg = load_agent_config(str(leaf), deployment_dir, role="subagent")
    assert cfg.agent_id == data["agent_id"]
    assert IGNORE_KEYS_DIRECTIVE not in cfg.extra
    blob = serialize_resolved_config(cfg, model=cfg.llm.model)
    assert IGNORE_KEYS_DIRECTIVE not in blob["agent"]


def test_subagent_role_keeps_the_experts_tools_and_drops_the_parent_only_keys():
    """``$ref: critic`` semantics: the critic's tools survive, its
    workspace.backend / verification / autonomy do not (D4)."""
    critic = _CONFIG / "experts" / "critic" / "config.yaml"
    own = _srw_config_fragment(yaml.safe_load(critic.read_text()))
    data = load_and_merge_config(str(critic), role="subagent")
    assert data["tools"]["shell"] == own["tools"]["shell"]
    assert "backend" not in data["workspace"]
    assert data["workspace"]["max_read_words"] == 25000  # per-agent cap kept
    for key in ("verification", "autonomy", "delegation", "phase_settings"):
        assert key not in data
    assert data["memory"]["enabled"] is False
    assert all(t["enabled"] is False for t in data["auxiliary"]["tasks"].values())


def test_session_expert_as_worker_carries_the_worker_keys():
    assistant = _CONFIG / "experts" / "assistant" / "config.yaml"
    assert _native_role(assistant) == "session"
    data = load_and_merge_config(str(assistant), role="worker")
    worker = load_role_base("worker")
    assert data["agent_id"] == "assistant"
    assert data["phase_settings"] == worker["phase_settings"]
    assert data["autonomy"] == worker["autonomy"]
    assert data["tools"]["core"] == worker["tools"]["core"]
    assert data["llm"]["max_retries"] == 0
    assert (
        data["memory"]["pipeline"]["writers"] == worker["memory"]["pipeline"]["writers"]
    )


def test_worker_expert_as_session_uses_the_session_overlay_and_drops_nothing():
    developer = _CONFIG / "experts" / "developer" / "config.yaml"
    assert _native_role(developer) == "worker"
    own = _srw_config_fragment(yaml.safe_load(developer.read_text()))
    data = load_and_merge_config(str(developer), role="session")
    session = load_role_base("session")
    assert data["agent_id"] == "developer"
    assert data["llm"]["max_retries"] == 3
    assert data["tools"]["canvas"] == session["tools"]["canvas"]
    assert (
        data["memory"]["pipeline"]["writers"]
        == session["memory"]["pipeline"]["writers"]
    )
    # Expert wins: everything the developer authored is still there, including
    # keys the session runtime never reads.
    assert data["tools"]["shell"] == own["tools"]["shell"]
    assert data["delegation"]["enabled"] == own["delegation"]["enabled"]
    assert IGNORE_KEYS_DIRECTIVE not in data


@pytest.mark.parametrize("role", sorted(ROLE_ROOTS))
def test_loading_any_root_with_a_role_yields_that_roles_base(role):
    """The roots are one thing in different roles: naming one root with
    another role's re-rooting gives the requested role's base."""
    for root_name in ROOT_NAMES:
        root_path, _ = resolve_config_path(root_name)
        assert load_and_merge_config(root_path, role=role) == load_role_base(role)


# ---------------------------------------------------------------------------
# 3. Aliases and public names
# ---------------------------------------------------------------------------


def test_public_names_canonicalise_to_themselves_and_resolve_to_the_overlays():
    for alias, public in (
        ("default", "worker_base"),
        ("defaults", "worker_base"),
        ("worker_base", "worker_base"),
        ("overlays/worker", "worker_base"),
        ("persistent_default", "session_base"),
        ("persistent_defaults", "session_base"),
        ("session_base", "session_base"),
        ("overlays/session", "session_base"),
        ("subagent_base", "subagent_base"),
        ("overlays/subagent", "subagent_base"),
        ("expert_base", "expert_base"),
    ):
        assert canonical_config_name(alias) == public, alias
    assert resolve_config_path("worker_base")[0].endswith("config/overlays/worker.yaml")
    assert resolve_config_path("default")[0] == resolve_config_path("worker_base")[0]
    assert resolve_config_path("session_base")[0].endswith("overlays/session.yaml")
    assert resolve_config_path("subagent_base")[0].endswith("overlays/subagent.yaml")
    assert resolve_config_path("expert_base")[0].endswith("config/expert_base.yaml")
    # never a deployment dir for a root, and the directory form is never probed
    for name in ROOT_NAMES:
        path, deployment_dir = resolve_config_path(name)
        assert deployment_dir is None and Path(path).is_file(), name


def test_path_forms_of_the_old_base_files_still_load():
    """``config/worker_base.yaml`` no longer exists on disk, but the path form
    (create_persistent_app("config/session_base.yaml"), old CLI invocations)
    lands on the overlay next to where the file used to be."""
    assert (
        canonical_config_name("config/worker_base.yaml")
        == "config/overlays/worker.yaml"
    )
    assert (
        canonical_config_name("config/persistent_defaults.yaml")
        == "config/overlays/session.yaml"
    )
    assert canonical_config_name("worker_base.yaml") == "overlays/worker.yaml"
    assert load_and_merge_config("config/worker_base.yaml") == load_role_base("worker")
    assert load_and_merge_config("config/session_base.yaml") == load_role_base(
        "session"
    )
    assert load_agent_config("config/defaults.yaml").agent_id == "worker_base"
    # A name that merely starts with a base name is not an alias.
    assert (
        canonical_config_name("worker_base.a8950251.yaml")
        == "worker_base.a8950251.yaml"
    )


@pytest.mark.parametrize(
    ("public", "agent_id", "probe"),
    [
        ("worker_base", "worker_base", lambda c: c.phase_settings.min_todos == 2),
        ("session_base", "session_base", lambda c: "get_canvas" in c.tools.canvas),
    ],
)
def test_agent_boot_path_loads_both_public_names(public, agent_id, probe):
    """What ``agent.py --config worker_base|session_base`` does
    (src/api/app.py: resolve_config_path -> load_agent_config)."""
    path, deployment_dir = resolve_config_path(public)
    cfg = load_agent_config(path, deployment_dir)
    assert cfg.agent_id == agent_id
    assert probe(cfg)


def test_chain_root_names_the_first_role_overlay_met():
    assert (
        chain_root(str(_CONFIG / "experts" / "assistant" / "config.yaml"))
        == "session_base"
    )
    assert (
        chain_root(str(_CONFIG / "experts" / "developer" / "config.yaml"))
        == "worker_base"
    )
    assert chain_root(str(_CONFIG / "interactive.yaml")) == "session_base"
    assert chain_root(str(_CONFIG / "overlays" / "session.yaml")) == "session_base"
    assert chain_root(str(_CONFIG / "expert_base.yaml")) == EXPERT_BASE
    assert chain_root(str(_PRE_SPLIT["worker"])) is None  # standalone
    assert chain_root(str(_CONFIG / "does-not-exist.yaml")) is None


def test_chain_root_follows_an_expert_to_expert_chain(tmp_path):
    child = tmp_path / "child.yaml"
    child.write_text(
        "$extends: " + str(_CONFIG / "experts" / "assistant" / "config.yaml") + "\n"
        "agent_id: child\ndisplay_name: Child\n"
    )
    assert chain_root(str(child)) == "session_base"
    data = load_and_merge_config(str(child), role="worker")
    assert data["agent_id"] == "child" and data["autonomy"] == "review"


# ---------------------------------------------------------------------------
# The mechanics: $ignore_keys pruning and re-rooting
# ---------------------------------------------------------------------------


def test_prune_ignored_keys_removes_nested_paths_and_keeps_the_directive():
    data = {
        IGNORE_KEYS_DIRECTIVE: ["workspace.backend", "autonomy", "nope.missing"],
        "workspace": {"backend": "vm", "max_read_words": 1},
        "autonomy": "full",
        "tools": {"shell": []},
    }
    out = prune_ignored_keys(data)
    assert out is data
    assert data["workspace"] == {"max_read_words": 1}
    assert "autonomy" not in data
    assert data["tools"] == {"shell": []}
    assert data[IGNORE_KEYS_DIRECTIVE] == [
        "workspace.backend",
        "autonomy",
        "nope.missing",
    ]


def test_prune_ignored_keys_is_a_no_op_without_the_directive():
    assert prune_ignored_keys({"a": 1}) == {"a": 1}
    assert prune_ignored_keys({IGNORE_KEYS_DIRECTIVE: [], "a": 1}) == {
        IGNORE_KEYS_DIRECTIVE: [],
        "a": 1,
    }
    assert prune_ignored_keys(None) is None
    assert prune_ignored_keys(["not", "a", "dict"]) == ["not", "a", "dict"]


def test_prune_ignored_keys_rejects_a_malformed_directive():
    with pytest.raises(ValueError):
        prune_ignored_keys({IGNORE_KEYS_DIRECTIVE: "autonomy"})
    with pytest.raises(ValueError):
        prune_ignored_keys({IGNORE_KEYS_DIRECTIVE: [{"k": "v"}]})


def test_ignore_keys_ride_the_merge_as_a_list_that_a_child_may_replace():
    """deep_merge's lists-replace rule applies to the directive too: an expert
    that declares its own list wins wholesale (and an empty list disables
    pruning) — a later layer cannot re-add a key by merging, only by
    redeclaring the directive."""
    base = {IGNORE_KEYS_DIRECTIVE: ["autonomy"], "autonomy": "review"}
    child = {"autonomy": "full"}
    assert "autonomy" not in prune_ignored_keys(deep_merge(base, child))
    override = {IGNORE_KEYS_DIRECTIVE: [], "autonomy": "full"}
    assert prune_ignored_keys(deep_merge(base, override))["autonomy"] == "full"


def test_ignore_keys_never_reach_extra_or_the_blob():
    data = load_and_merge_config(
        str(_CONFIG / "experts" / "critic" / "config.yaml"), role="subagent"
    )
    cfg = load_agent_config_from_dict(dict(data))
    assert IGNORE_KEYS_DIRECTIVE not in cfg.extra
    assert "verification" not in cfg.extra
    blob = serialize_resolved_config(cfg, model=cfg.llm.model)
    assert IGNORE_KEYS_DIRECTIVE not in blob["agent"]


def test_reroot_extends_rules():
    assert reroot_extends("worker_base", None) == ("worker_base", None)
    assert reroot_extends("worker_base", "session") == ("session_base", None)
    assert reroot_extends("persistent_defaults", "worker") == ("worker_base", None)
    assert reroot_extends("overlays/subagent", "worker") == ("worker_base", None)
    assert reroot_extends("expert_base", "subagent") == ("subagent_base", None)
    # a link to another expert passes the role down
    assert reroot_extends("critic", "subagent") == ("critic", "subagent")
    with pytest.raises(ValueError):
        reroot_extends("worker_base", "officer")


def test_unknown_role_is_refused_everywhere():
    with pytest.raises(ValueError):
        load_role_base("officer")
    with pytest.raises(ValueError):
        load_and_merge_config(
            str(_CONFIG / "experts" / "critic" / "config.yaml"), role="officer"
        )


# ---------------------------------------------------------------------------
# 4. The subagent library (WP3) — config/subagents/<name>/config.yaml
# ---------------------------------------------------------------------------

_LIBRARY = sorted(
    p for p in _CONFIG.glob("subagents/*/config.yaml") if p.parent.name != "__pycache__"
)
_LIBRARY_IDS = [p.parent.name for p in _LIBRARY]


def test_the_library_is_discovered():
    """Guard the guard: all seven shipped child types are in the sweep."""
    assert set(_LIBRARY_IDS) == {
        "explorer",
        "implementer",
        "probe",
        "reader",
        "reviewer",
        "tester",
        "verifier",
    }


@pytest.mark.parametrize("name", _LIBRARY_IDS)
def test_library_entry_resolves_in_the_subagent_role(name, subagent_ignored):
    """A library entry is a roster target: on the subagent overlay it keeps
    exactly its declared tools, carries the `subagent` tag, inherits the
    parent's model by default, and parses like any config."""
    leaf = next(p for p in _LIBRARY if p.parent.name == name)
    own = _srw_config_fragment(yaml.safe_load(leaf.read_text(encoding="utf-8")))
    assert canonical_config_name(str(own["$extends"])) == EXPERT_BASE
    assert chain_root(str(leaf)) == EXPERT_BASE
    assert "subagent" in own["tags"]
    assert own["description"].strip()
    data = load_and_merge_config(str(leaf), role="subagent")
    assert data["agent_id"] == own["agent_id"]
    assert data[IGNORE_KEYS_DIRECTIVE] == subagent_ignored
    for dotted in subagent_ignored:
        assert not _dotted_present(data, dotted), f"{name}: {dotted} survived"
    assert data["llm"]["model"] == "inherit"
    assert data["interactive"]["permission_mode"] == "autonomous"
    assert data["memory"]["enabled"] is False
    cfg = load_agent_config(str(leaf), str(leaf.parent), role="subagent")
    assert cfg.agent_id == own["agent_id"]
    assert "subagent" in cfg.tags
    assert IGNORE_KEYS_DIRECTIVE not in cfg.extra and "tags" not in cfg.extra
    declared = {group: list(names) for group, names in own["tools"].items()}
    actual = {group: list(names) for group, names in vars(cfg.tools).items() if names}
    assert actual == {group: names for group, names in declared.items() if names}


@pytest.mark.parametrize("name", _LIBRARY_IDS)
def test_library_entry_is_read_only_when_loaded_standalone(name):
    """Standalone loading cannot leak expert_base capabilities.

    Read-only entries may add shell, git reads and job-inspection reads to the
    subagent floor. Implementer is the sole workspace writer; probe and reader
    never acquire a workspace file mutator.
    """
    from agent.subagents.child import WRITE_TOOLS
    from agent.tools.registry import TOOL_REGISTRY

    leaf = next(p for p in _LIBRARY if p.parent.name == name)
    own = _srw_config_fragment(yaml.safe_load(leaf.read_text(encoding="utf-8")))
    floor_path, floor_dir = resolve_config_path(ROLE_ROOTS["subagent"])
    floor = set(get_all_tool_names(load_agent_config(floor_path, floor_dir)))
    cfg = load_agent_config(str(leaf), str(leaf.parent))
    standalone = set(get_all_tool_names(cfg))
    assert standalone, "an empty toolset would make this vacuous"

    # Every non-empty group is authored on the leaf: nothing from expert_base
    # may leak through a group the entry forgot to restate.
    declared = {group: list(names) for group, names in own["tools"].items() if names}
    actual = {group: list(names) for group, names in vars(cfg.tools).items() if names}
    assert actual == declared

    if own.get("write_policy", "none") == "none":
        allowed_categories = {"shell", "git", "job_inspection"}
        allowed = floor | {
            tool
            for tool, metadata in TOOL_REGISTRY.items()
            if metadata.get("category") in allowed_categories
        }
        if name == "reader":
            # The scholar's fan-out unit deliberately extends the floor with
            # paper download and the shared citation-library write surface.
            allowed |= {"download_paper", "cite_web", "cite_document"}
        assert standalone <= allowed, sorted(standalone - allowed)
        assert not (standalone & WRITE_TOOLS)

    workspace_mutators = standalone & WRITE_TOOLS
    assert bool(workspace_mutators) is (name == "implementer")
    if name in {"probe", "reader"}:
        assert "write_file" not in standalone

    # And by the two `$ref` spellings, the same file.
    assert resolve_config_path(f"subagents/{name}")[0] == str(leaf)
    assert resolve_config_path(name)[0] == str(leaf)
