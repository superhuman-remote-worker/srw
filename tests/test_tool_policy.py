"""``normalize_tool_policy`` — the authoring vocabulary that can say "on".

``tools.<category>`` is a list of tool names, and that one field carries two
unrelated facts: **membership** (which tools) and **policy** (whether the group
is on).  Every registry category has at least one tool, so no group is ever
legitimately empty — every ``[]`` in every config is a disable marker.  "Off"
is therefore self-describing and "on" is not, which is why the New Session form
can disable a tool group and can never enable one.

This module tests the front-end that fixes it: ``true`` / ``false`` /
``{only: [...]}`` / ``{except: [...]}`` normalise down to the exact
``list[str]`` the whole stack already speaks, at four call sites, before
anything downstream sees them.

The load-bearing property is that **normalisation is the identity function on
every declaration that exists today** — see ``TestIdentityProperty``.  Both
legacy spellings are already canonical (``[names]`` is
``{only: [names]}``; ``[]`` is ``false``), so the enabling commit changes no
resolved tool set anywhere and the eight ``== []`` consumers keep working
untouched.

Design: ``knowledge-base/knowledge/features/tool_config_policy_vs_membership.md``.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest
import yaml

from shared.runtime.core.loader import (
    ToolsConfig,
    get_all_tool_names,
    load_agent_config_from_dict,
    load_and_merge_config,
    resolve_config_path,
)
from shared.runtime.core.session_tool_overrides import SESSION_TOOL_OVERRIDE_NAMES
from shared.runtime.core.srw_manifest_config import srw_config_fragment
from shared.runtime.core.tool_policy import (
    ENUMERATE_ONLY_CATEGORIES,
    MCP_WILDCARD,
    ToolPolicyError,
    assert_tool_policy_canonical,
    config_tool_categories,
    enumerate_only_members,
    expand_category_true,
    expand_tool_policy,
    normalize_tool_policy,
)
from agent.tools.registry import TOOL_REGISTRY, get_categories, get_tools_by_category

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONFIG_DIR = _REPO_ROOT / "config"


# ---------------------------------------------------------------------------
# Independent re-derivation of the expansion, NOT importing the production one.
# Two implementations that must agree is the whole point of ``test_expansion_*``.
# ---------------------------------------------------------------------------
def _reference_expand_true(category: str) -> set[str]:
    names = {
        name
        for name, meta in TOOL_REGISTRY.items()
        if meta.get("category") == category and "grant" not in meta
    }
    if category == "orchestrator":
        # ``orchestrator: true`` predates the 0156 job-group split and 0156's
        # non-array guard skipped boolean rows whole-row, so the boolean keeps
        # meaning the PRE-SPLIT category: today's members plus the grantable
        # job tools that used to live in it (see expand_category_true).
        names |= {
            name
            for name, meta in TOOL_REGISTRY.items()
            if meta.get("category") in ("job_control", "job_inspection")
            and "grant" not in meta
        }
    return names


def _config_files() -> list[Path]:
    return sorted(
        p
        for p in _CONFIG_DIR.rglob("*.y*ml")
        if p.suffix in (".yaml", ".yml") and "__pycache__" not in p.parts
    )


def _raw_declarations() -> list[tuple[str, str, object]]:
    """Every raw ``tools.<category>`` declaration in every bundled config.

    Raw = pre-``$extends``, exactly as written on disk.  This is the population
    the identity property has to hold over.
    """
    out: list[tuple[str, str, object]] = []
    for path in _config_files():
        data = yaml.safe_load(path.read_text())
        if not isinstance(data, dict):
            continue
        data = srw_config_fragment(data)
        tools = data.get("tools")
        if not isinstance(tools, dict):
            continue
        rel = str(path.relative_to(_REPO_ROOT))
        for category, value in tools.items():
            out.append((rel, category, value))
    return out


_DECLARATIONS = _raw_declarations()

# The chain roots by their public names (expert_base + the three role
# overlays) plus the one standalone session profile.
_STANDALONE_CONFIGS = [
    "expert_base",
    "session_base",
    "subagent_base",
    "worker_base",
    "interactive",
]


def _all_config_names() -> list[str]:
    experts = sorted(
        p.parent.name
        for p in _CONFIG_DIR.glob("experts/*/config.yaml")
        if p.parent.name != "__pycache__"
    )
    library = sorted(
        f"subagents/{p.parent.name}"
        for p in _CONFIG_DIR.glob("subagents/*/config.yaml")
        if p.parent.name != "__pycache__"
    )
    return _STANDALONE_CONFIGS + experts + library


def _minimal(**extra) -> dict:
    return {"agent_id": "t", "display_name": "T", **extra}


# ===========================================================================
# The population itself — a guard against a vacuous identity test
# ===========================================================================
class TestPopulation:
    def test_the_declaration_scan_actually_found_the_configs(self):
        """A scan that silently found nothing would make every test below pass."""
        files = {rel for rel, _, _ in _DECLARATIONS}
        assert len(files) == 22, f"expected 22 configs declaring tools:, got {files}"
        assert "config/expert_base.yaml" in files
        assert "config/overlays/worker.yaml" in files
        assert "config/overlays/session.yaml" in files
        assert "config/overlays/subagent.yaml" in files
        assert "config/subagents/explorer/config.yaml" in files
        assert "config/subagents/implementer/config.yaml" in files
        assert "config/subagents/probe/config.yaml" in files
        assert "config/subagents/reader/config.yaml" in files
        assert "config/subagents/reviewer/config.yaml" in files
        assert "config/subagents/tester/config.yaml" in files
        assert "config/subagents/verifier/config.yaml" in files
        assert any("experts/centurion" in f for f in files)
        # 148 after job_control/job_inspection became descriptor-owned groups;
        # 149 with the Centurion's explicit knowledge grant
        # (officer_knowledge_plane.md §3, K2); 141 after the eight shell-having
        # configs dropped their `git:` blocks — ToolsConfig.__post_init__
        # suppresses that group whenever shell tools are present, so declaring
        # it there advertised tools the pod never binds; 137 after the U1 root
        # split: the two self-contained bases (21 + 23 declarations) became
        # expert_base (17 shared groups) + the worker / session / subagent
        # overlays (4 + 6 + 13 role-owned groups); 143 with the subagent
        # library's explorer entry (6 read-only groups restated so it is
        # read-only standalone too); 187 with U3 WP5's six entries (44 explicit
        # group declarations, including the reviewer/verifier inspection group);
        # 192 with the engineer expert (shell, research, delegation, citation,
        # graph — engineer_expert.md); 201 with the memory group
        # (memory_search, append-only context injection WP5a): expert_base
        # grants it, the subagent overlay and the seven library entries
        # restate it empty (a child has no memory of its own).
        assert len(_DECLARATIONS) == 201, (
            f"expected 201 raw declarations, got {len(_DECLARATIONS)}"
        )

    def test_every_shipped_declaration_is_already_a_list(self):
        """The premise of the identity property: today's vocabulary is lists only."""
        non_lists = [
            (rel, cat, value)
            for rel, cat, value in _DECLARATIONS
            if not isinstance(value, list)
        ]
        assert not non_lists, non_lists


# ===========================================================================
# expand — the table from the design doc's "Normalisation is layer-local"
# ===========================================================================
class TestExpandTable:
    def test_true_expands_to_the_config_grantable_members(self):
        assert set(expand_category_true("workspace")) == _reference_expand_true(
            "workspace"
        )
        assert expand_tool_policy(True, "canvas") == sorted(
            _reference_expand_true("canvas")
        )

    def test_false_expands_to_empty(self):
        assert expand_tool_policy(False, "workspace") == []

    def test_bare_list_is_returned_as_written(self):
        assert expand_tool_policy(["read_file", "write_file"], "workspace") == [
            "read_file",
            "write_file",
        ]

    def test_empty_list_is_the_legacy_spelling_of_false(self):
        assert expand_tool_policy([], "workspace") == []

    def test_only_is_returned_as_written(self):
        assert expand_tool_policy({"only": ["read_file"]}, "workspace") == ["read_file"]

    def test_except_is_true_minus_the_named(self):
        full = sorted(_reference_expand_true("workspace"))
        got = expand_tool_policy({"except": ["read_file"]}, "workspace")
        assert got == [n for n in full if n != "read_file"]

    def test_expansion_result_is_a_fresh_list(self):
        """Callers merge these into shared dicts; aliasing the input is a trap."""
        src = ["read_file"]
        out = expand_tool_policy(src, "workspace")
        assert out == src and out is not src

    def test_true_is_sorted_and_deterministic(self):
        got = expand_category_true("workspace")
        assert got == sorted(got)


class TestExpandRejections:
    @pytest.mark.parametrize(
        "value,fragment",
        [
            ({}, "write false"),
            ({"only": []}, "write false"),
            ({"except": []}, "write true"),
            ({"only": ["a"], "except": ["b"]}, "one mapping"),
            ({"onlyy": ["a"]}, "unknown policy key"),
            ("read_file", "unsupported value"),
            (3, "unsupported value"),
            (None, "unsupported value"),
            ({"only": "read_file"}, "expected a list"),
            ({"only": [1]}, "tool-name strings"),
        ],
    )
    def test_rejected_forms_raise_with_a_useful_message(self, value, fragment):
        with pytest.raises(ToolPolicyError) as exc:
            expand_tool_policy(value, "workspace")
        assert fragment in str(exc.value).lower()

    def test_except_rejects_a_name_from_another_category(self):
        with pytest.raises(ToolPolicyError) as exc:
            expand_tool_policy({"except": ["run_command"]}, "workspace")
        assert "run_command" in str(exc.value)

    def test_unknown_category_raises(self):
        with pytest.raises(ToolPolicyError):
            expand_tool_policy(True, "workspaces")


class TestOnlyIsNeverIntersected:
    """Requirement inherited from Task 4 §4a — the sharpest trap in the contract.

    ``grant: "explicit"`` restricts category-level *policy*, never an explicit
    name.  If ``only`` were intersected against the ``true`` expansion,
    ``config/experts/centurion`` would silently lose ``steer_job`` and
    ``get_stuck_jobs`` — the only two ``explicit`` tools any shipped config
    names — and the grants snapshot would move on a commit whose entire
    acceptance criterion is that it does not.

    Task 4 pinned the *config* side (``test_explicit_tools_stay_nameable``).
    This is the *resolver* side, which did not exist then.
    """

    _EXPLICIT_IN_CENTURION = ("steer_job", "get_stuck_jobs")

    def test_the_premise_still_holds(self):
        for name in self._EXPLICIT_IN_CENTURION:
            assert TOOL_REGISTRY[name].get("grant") == "explicit"
            category = TOOL_REGISTRY[name]["category"]
            assert name not in expand_category_true(category)

    def test_only_keeps_explicit_tools(self):
        for name in self._EXPLICIT_IN_CENTURION:
            category = TOOL_REGISTRY[name]["category"]
            assert expand_tool_policy({"only": [name]}, category) == [name]

    def test_bare_list_keeps_explicit_tools(self):
        for name in self._EXPLICIT_IN_CENTURION:
            category = TOOL_REGISTRY[name]["category"]
            assert expand_tool_policy([name], category) == [name]

    def test_only_keeps_code_granted_tools_too(self):
        """``grant: "code"`` has the same asymmetry — excluded from ``true``,
        still nameable.  Nothing that works today stops working."""
        assert TOOL_REGISTRY["srw_cloud_status"].get("grant") == "code"
        assert "srw_cloud_status" not in expand_tool_policy(
            {"except": ["cypher_query"]}, "graph"
        )
        assert expand_tool_policy({"only": ["srw_cloud_status"]}, "shell") == [
            "srw_cloud_status"
        ]

    def test_centurions_declaration_survives_normalisation_intact(self):
        path = _CONFIG_DIR / "experts" / "centurion" / "config.yaml"
        tools = srw_config_fragment(yaml.safe_load(path.read_text()))["tools"]
        for name in self._EXPLICIT_IN_CENTURION:
            category = TOOL_REGISTRY[name]["category"]
            raw = tools[category]
            assert name in raw
            out = normalize_tool_policy({"tools": {category: raw}})
            assert out["tools"][category] == raw


# ===========================================================================
# The two hazards Task 4 handed over
# ===========================================================================
class TestShellMustEnumerate:
    """``shell`` accepts ``only`` and ``false``. Nothing else.

    The rule is about **auto-tracking**, and only ``only`` avoids it. ``true``
    means "this category and whatever is added to it later"; ``{except: [...]}``
    means exactly the same thing minus a fixed subtraction, recomputed from the
    registry on every resolution. For a code-execution category both are the
    wrong default: a tool added to ``shell`` in the registry would land in
    every config that used either form, with no diff to review anywhere.

    A rule that forbade ``true`` while blessing ``except`` would be
    spelling-based, not semantic — the two expand to the identical four names
    today, which is what ``test_true_and_except_are_the_same_thing`` pins.

    Not to be confused with the mode-alias argument: ``run_command`` /
    ``shell_execute`` being rewritten from ``extra.shell.mode`` makes naming
    both halves redundant, not dangerous, and ``bughunter`` already names both.
    """

    def test_shell_true_is_refused(self):
        with pytest.raises(ToolPolicyError) as exc:
            expand_tool_policy(True, "shell")
        msg = str(exc.value)
        assert "shell" in msg and "auto-track" in msg and "only" in msg

    @pytest.mark.parametrize(
        "value", [{"except": ["srw_cloud_status"]}, {"except": ["run_command"]}]
    )
    def test_shell_except_is_refused(self, value):
        with pytest.raises(ToolPolicyError) as exc:
            expand_tool_policy(value, "shell")
        assert "auto-track" in str(exc.value)

    def test_shell_empty_except_is_refused_with_the_shell_rule(self):
        """``{except: []}`` must not fall through to the generic "write true"
        message, because ``true`` is refused here too."""
        with pytest.raises(ToolPolicyError) as exc:
            expand_tool_policy({"except": []}, "shell")
        assert "auto-track" in str(exc.value)

    def test_true_and_except_are_the_same_thing(self):
        """Why blessing ``except`` while refusing ``true`` made no sense: on
        every other category the two forms differ only by the subtraction, and
        both are recomputed from the live registry."""
        full = sorted(
            n
            for n, meta in TOOL_REGISTRY.items()
            if meta.get("category") == "shell" and "grant" not in meta
        )
        assert expand_tool_policy({"except": ["git_log"]}, "git") == sorted(
            set(expand_category_true("git")) - {"git_log"}
        )
        assert full == ["cancel_command", "run_command", "shell_execute", "shell_read"]

    def test_the_accepted_forms_still_work(self):
        assert expand_tool_policy(False, "shell") == []
        assert expand_tool_policy([], "shell") == []
        assert expand_tool_policy(["run_command"], "shell") == ["run_command"]
        assert expand_tool_policy({"only": ["run_command", "shell_read"]}, "shell") == [
            "run_command",
            "shell_read",
        ]

    def test_every_shipped_shell_declaration_is_still_accepted(self):
        """Seventeen configs declare ``tools.shell`` (the prior ten, all six
        U3 WP5 library entries including reader's explicit empty override, and
        the engineer expert); all are bare lists or ``[]``, the legacy
        spellings of ``only`` and ``false``. None is affected."""
        decls = [(rel, v) for rel, cat, v in _DECLARATIONS if cat == "shell"]
        assert len(decls) == 17, decls
        for rel, value in decls:
            assert (
                normalize_tool_policy({"tools": {"shell": value}})["tools"]["shell"]
                == value
            ), rel

    def test_the_rule_is_scoped_to_shell_and_delegation(self):
        """``delegation`` joined in U3 WP4: its one tool is ``grant: explicit``,
        so ``true`` would silently expand to ``[]`` — refusing it keeps the
        settings toggle honest (the cockpit sends the served enumeration). A
        STORED ``true`` is compat, not vocabulary: ``normalize_tool_policy``
        maps it to ``[delegate_agent]`` (tests/test_delegation_config_compat)."""
        assert ENUMERATE_ONLY_CATEGORIES == {"shell", "delegation"}
        assert expand_category_true("git")
        assert expand_tool_policy({"except": ["git_log"]}, "git")
        with pytest.raises(ToolPolicyError, match="must enumerate"):
            expand_category_true("delegation")

    def test_the_legacy_coding_alias_obeys_the_shell_rule(self):
        for value in (True, {"except": ["srw_cloud_status"]}):
            with pytest.raises(ToolPolicyError):
                normalize_tool_policy({"tools": {"coding": value}})

    @pytest.mark.parametrize("category", sorted(ENUMERATE_ONLY_CATEGORIES))
    def test_schema_json_refuses_true_and_except_on_enumerated_categories(
        self, category
    ):
        schema = json.loads((_REPO_ROOT / "config" / "schema.json").read_text())
        block = schema["properties"]["tools"]["properties"][category]
        boolean, array, mapping = block["oneOf"]
        assert boolean["const"] is False, f"{category} must not accept `true`"
        assert array["type"] == "array", "bare lists stay legal"
        assert set(mapping["properties"]) == {"only"}, (
            f"{category} must not accept `except`"
        )
        assert mapping["required"] == ["only"]


class TestMcp:
    """``get_tools_by_category("mcp")`` is PROCESS-LOCAL.

    ``register_mcp_tools`` mutates the global registry per job/session and is
    never called in the orchestrator process, where resolution happens.  A
    registry-derived expansion would therefore be ``[]`` in the orchestrator
    and a different, session-dependent set in each agent.  ``mcp: true`` is a
    hard special case for the existing ``"*"`` sentinel, never a category
    expansion.
    """

    def test_mcp_true_is_the_wildcard_sentinel(self):
        assert expand_tool_policy(True, "mcp") == [MCP_WILDCARD]

    def test_mcp_true_ignores_whatever_the_local_registry_happens_to_hold(self):
        TOOL_REGISTRY["_fake_mcp_tool"] = {"category": "mcp"}
        try:
            assert expand_tool_policy(True, "mcp") == [MCP_WILDCARD]
        finally:
            del TOOL_REGISTRY["_fake_mcp_tool"]

    def test_mcp_except_is_refused(self):
        with pytest.raises(ToolPolicyError):
            expand_tool_policy({"except": ["anything"]}, "mcp")

    def test_the_datasource_written_wildcard_list_round_trips(self):
        assert expand_tool_policy([MCP_WILDCARD], "mcp") == [MCP_WILDCARD]


class TestMachineOwnedCategories:
    """A category whose every tool is ``grant: "code"`` expands ``true`` to ``[]``.

    That is the design's own position: config-grantability is expressed
    per-tool with ``grant``, which is checkable, rather than by absence from a
    dataclass, which is not.  It is also a footgun (``sql: true`` reading as
    "off"), so it warns.
    """

    _MACHINE_OWNED = (
        "graph",
        "sql",
        "mongodb",
        "webdav",
        "repo",
        "email",
        "product_help",
        "session_task",
    )

    @pytest.mark.parametrize("category", _MACHINE_OWNED)
    def test_true_expands_to_empty(self, category):
        assert get_tools_by_category(category), category
        assert expand_category_true(category) == []

    def test_it_warns(self, caplog):
        with caplog.at_level("WARNING", logger="shared.runtime.core.tool_policy"):
            expand_category_true("sql")
        assert any(
            "tools.sql: true expands to []" in r.getMessage() for r in caplog.records
        )

    def test_no_other_category_expands_to_empty(self):
        empties = {
            c
            for c in get_categories()
            if c != "mcp"
            and c not in ENUMERATE_ONLY_CATEGORIES  # `true` is refused there
            and not expand_category_true(c)
        }
        assert empties == set(self._MACHINE_OWNED)


# ===========================================================================
# THE EXPANSION TABLE — re-derived, and compared to the closed vocabulary
# ===========================================================================
class TestExpansionAgainstTheClosedVocabulary:
    """The safety gate for the whole migration.

    ``true`` is structurally wider than the hand-curated session vocabulary:
    nine registry entries sit in a selectable category and are absent from it,
    six of them control-plane *writes* such as ``set_expert_bundle``.  Task 4's
    classification is what holds the delta at zero.  If this ever fails, a user
    ticking "Experts & Skills" is about to gain the ability to rewrite the
    expert catalogue.
    """

    @pytest.mark.parametrize("group", sorted(SESSION_TOOL_OVERRIDE_NAMES))
    def test_true_expands_to_exactly_the_curated_set(self, group):
        expected = set(SESSION_TOOL_OVERRIDE_NAMES[group])
        if group == "orchestrator":
            # Legacy-compat exception: ``orchestrator: true`` predates the
            # 0156 job-group split (0156 skipped boolean rows whole-row), so
            # its expansion is the pre-split union — the curated orchestrator
            # set plus the curated job groups. No wider: the explicit-tier
            # job tools (delete/assign/promote/steer/...) stay out.
            expected |= set(SESSION_TOOL_OVERRIDE_NAMES["job_control"])
            expected |= set(SESSION_TOOL_OVERRIDE_NAMES["job_inspection"])
        assert set(expand_category_true(group)) == expected

    @pytest.mark.parametrize("category", sorted(get_categories()))
    def test_production_expansion_matches_an_independent_derivation(self, category):
        if category == "mcp" or category in ENUMERATE_ONLY_CATEGORIES:
            pytest.skip(
                "special-cased; covered by TestMcp / TestShell* / "
                "test_the_rule_is_scoped_to_shell_and_delegation"
            )
        assert set(expand_category_true(category)) == _reference_expand_true(category)


# ===========================================================================
# normalize — identity, idempotence, wiring
# ===========================================================================
class TestIdentityProperty:
    """``normalize`` must not change what a single shipped config grants.

    This is the executable form of the backward-compatibility guarantee and
    the acceptance criterion for the commit.
    """

    @pytest.mark.parametrize(
        "rel,category,value",
        _DECLARATIONS,
        ids=[
            f"{rel.split('/')[-2] if '/' in rel else rel}:{cat}"
            for rel, cat, _ in _DECLARATIONS
        ],
    )
    def test_normalize_is_the_identity_on_every_shipped_declaration(
        self, rel, category, value
    ):
        out = normalize_tool_policy({"tools": {category: value}})
        assert out["tools"][category] == value, rel

    def test_normalize_does_not_mutate_its_input(self):
        fragment = {"tools": {"canvas": True}}
        normalize_tool_policy(fragment)
        assert fragment == {"tools": {"canvas": True}}

    def test_a_fragment_without_tools_is_returned_untouched(self):
        fragment = {"llm": {"model": "x"}}
        assert normalize_tool_policy(fragment) is fragment

    def test_unknown_category_keys_pass_through_verbatim(self):
        """Today a typo'd key is silently ignored end to end.  Preserve that
        here — ``schema.json``'s ``additionalProperties: false`` is what
        catches it, at authoring time, without changing runtime behaviour."""
        out = normalize_tool_policy({"tools": {"workspaces": True}})
        assert out["tools"]["workspaces"] is True

    def test_the_legacy_coding_alias_normalises_against_shell(self):
        assert normalize_tool_policy({"tools": {"coding": ["run_command"]}})["tools"][
            "coding"
        ] == ["run_command"]
        with pytest.raises(ToolPolicyError):
            normalize_tool_policy({"tools": {"coding": True}})


class TestIdempotence:
    """What makes the belt-and-braces four-call-site placement safe."""

    @pytest.mark.parametrize(
        "value",
        [
            True,
            False,
            [],
            ["read_file"],
            {"only": ["read_file"]},
            {"except": ["read_file"]},
        ],
    )
    def test_normalize_twice_is_normalize_once(self, value):
        once = normalize_tool_policy({"tools": {"workspace": value}})
        twice = normalize_tool_policy(once)
        assert twice == once


class TestAssertCanonical:
    """The last call site raises rather than coerces.

    ``ToolsConfig`` fields are typed ``List[str]`` and
    ``get_all_tool_names`` silently returns ``[]`` for a non-list, so a ``true``
    leaking this far would **silently disable** the group — the exact failure
    mode under repair.  A bool arriving here is a missed call site, not input.
    """

    def test_a_list_passes(self):
        assert_tool_policy_canonical({"workspace": ["read_file"]}, where="test")

    @pytest.mark.parametrize("value", [True, False, {"only": ["read_file"]}, "x", None])
    def test_a_policy_value_raises(self, value):
        with pytest.raises(ToolPolicyError) as exc:
            assert_tool_policy_canonical({"workspace": value}, where="test")
        assert "workspace" in str(exc.value) and "test" in str(exc.value)

    def test_unknown_keys_are_not_policed(self):
        assert_tool_policy_canonical({"workspaces": True}, where="test")

    def test_load_agent_config_from_dict_refuses_a_bool(self):
        with pytest.raises(ToolPolicyError):
            load_agent_config_from_dict(_minimal(tools={"canvas": True}))

    def test_load_agent_config_from_dict_accepts_a_normalised_fragment(self):
        cfg = load_agent_config_from_dict(
            normalize_tool_policy(_minimal(tools={"canvas": True}))
        )
        assert sorted(cfg.tools.canvas) == sorted(expand_category_true("canvas"))


# ===========================================================================
# The category vocabulary: four lists that must agree
# ===========================================================================
class TestCategoryVocabularyAgreement:
    """``TOOL_REGISTRY`` is the authority; three other lists mirror it.

    ``ToolsConfig`` cannot derive its fields at runtime — ``src/tools/registry``
    imports ``src/core/loader`` (via the tool packages), so a module-level
    import the other way is a cycle.  ``get_all_tool_names`` *is* derived from
    ``ToolsConfig``, which removes one of the four lists outright; the
    remaining two are pinned here so adding a registry category fails loudly at
    the places it must be reflected instead of silently at none.
    """

    _EXPECTED = None

    @property
    def expected(self) -> set[str]:
        return set(get_categories()) | {"mcp"}

    def test_tools_config_fields_match_the_registry(self):
        fields = {f.name for f in dataclasses.fields(ToolsConfig)}
        assert fields == self.expected

    def test_config_tool_categories_matches_the_registry(self):
        assert set(config_tool_categories()) == self.expected

    def test_get_all_tool_names_reads_every_field(self):
        """Derived, not transcribed — a field that no longer feeds
        ``get_all_tool_names`` would be silently discarded."""
        names = {f.name: [f"probe_{f.name}"] for f in dataclasses.fields(ToolsConfig)}
        cfg = load_agent_config_from_dict(_minimal(tools=names))
        got = set(get_all_tool_names(cfg))
        # run_command/shell_execute aliasing rewrites the shell probe; `git` is
        # suppressed by ToolsConfig.__post_init__ because this probe config also
        # carries shell tools (asserted separately below, so the field keeps its
        # plumbing coverage); `memory` binds only names a bound memory
        # extension serves (memory_tool_names_bindable, also asserted below).
        # Every other probe must survive verbatim.
        assert {n for n in names if n not in ("shell", "git", "memory")} <= {
            g.removeprefix("probe_") for g in got
        }
        assert "probe_memory" not in got

        # git still has to reach get_all_tool_names on the path where it is
        # bound at all — otherwise this exemption would hide a broken field.
        without_shell = dict(names)
        without_shell["shell"] = []
        cfg = load_agent_config_from_dict(_minimal(tools=without_shell))
        assert "probe_git" in set(get_all_tool_names(cfg))

        # Same for memory: with memory, the manager seam and a same-named
        # extension on, the field reaches the list.
        cfg = load_agent_config_from_dict(
            _minimal(
                tools=names,
                memory={
                    "enabled": True,
                    "manager": {"enabled": True},
                    "pipeline": {"extensions": ["probe_memory"]},
                },
            )
        )
        assert "probe_memory" in set(get_all_tool_names(cfg))

    def test_schema_json_tools_properties_match_the_registry(self):
        schema = json.loads((_REPO_ROOT / "config" / "schema.json").read_text())
        tools = schema["properties"]["tools"]
        assert set(tools["properties"]) == self.expected
        assert tools["additionalProperties"] is False

    @pytest.mark.parametrize("category", sorted(set(get_categories()) | {"mcp"}))
    def test_each_schema_block_accepts_the_five_forms(self, category):
        schema = json.loads((_REPO_ROOT / "config" / "schema.json").read_text())
        block = schema["properties"]["tools"]["properties"][category]
        kinds = {b.get("type") for b in block["oneOf"]}
        assert kinds == {"boolean", "array", "object"}, category

    def test_product_help_and_session_task_are_newly_addressable(self):
        """Deliberate: both were registry categories with no ``ToolsConfig``
        field, so ``tools.product_help: [...]`` was silently discarded.  Both
        are wholly ``grant: "code"``, so ``true`` grants nothing new, and
        ``load_tools`` already binds their tools when named under *any* key
        (it groups by registry metadata, not by the key) — so this adds no
        reachable capability, it only makes the natural key work."""
        fields = {f.name for f in dataclasses.fields(ToolsConfig)}
        assert {"product_help", "session_task"} <= fields
        assert expand_category_true("product_help") == []
        assert expand_category_true("session_task") == []

    def test_no_shipped_config_declares_the_two_new_keys(self):
        declared = {cat for _, cat, _ in _DECLARATIONS}
        assert not declared & {"product_help", "session_task"}


# ===========================================================================
# Wiring: the resolved grant of every shipped config is unchanged
# ===========================================================================
class TestShippedConfigsResolveUnchanged:
    @pytest.mark.parametrize("config_name", _all_config_names())
    def test_merged_config_carries_only_lists(self, config_name):
        path, _ = resolve_config_path(config_name)
        tools = (load_and_merge_config(path) or {}).get("tools") or {}
        assert tools, config_name
        assert all(isinstance(v, list) for v in tools.values()), config_name

    @pytest.mark.parametrize("config_name", _all_config_names())
    def test_merged_config_matches_the_raw_yaml_chain(self, config_name):
        """Normalising inside ``load_and_merge_config`` must not perturb the
        ``$extends`` merge: the result has to equal a merge of the raw files
        (with the merged chain's ``$ignore_keys`` honoured — the subagent
        overlay prunes ``tools.delegation``, and that pruning is the loader's
        documented step, not a perturbation)."""
        from shared.runtime.core.loader import deep_merge, prune_ignored_keys

        path, _ = resolve_config_path(config_name)

        def raw_chain(p: str) -> dict:
            data = srw_config_fragment(yaml.safe_load(Path(p).read_text()))
            parent = data.pop("$extends", None)
            data.pop("$comment", None)
            if parent:
                ppath, _ = resolve_config_path(str(parent))
                data = deep_merge(raw_chain(ppath), data)
            return data

        assert (load_and_merge_config(path) or {}).get("tools") == (
            prune_ignored_keys(raw_chain(path)).get("tools")
        )


class TestMergeOrder:
    """Layers merge by the existing, unmodified ``deep_merge``: lists REPLACE."""

    def _resolve(self, *layers: dict) -> dict:
        from shared.runtime.core.loader import deep_merge

        out: dict = {}
        for layer in layers:
            out = deep_merge(out, normalize_tool_policy(layer)) if layer else out
        return out

    def test_child_false_beats_parent_true(self):
        got = self._resolve({"tools": {"canvas": True}}, {"tools": {"canvas": False}})
        assert got["tools"]["canvas"] == []

    def test_child_true_beats_a_parent_empty_list(self):
        """The motivating bug, at the layer level: ``[]`` above, ``true``
        below, and the group comes back ON."""
        got = self._resolve({"tools": {"canvas": []}}, {"tools": {"canvas": True}})
        assert set(got["tools"]["canvas"]) == set(expand_category_true("canvas"))

    def test_absent_child_inherits(self):
        got = self._resolve({"tools": {"canvas": True}}, {"tools": {"git": False}})
        assert set(got["tools"]["canvas"]) == set(expand_category_true("canvas"))

    def test_parent_only_child_except_WIDENS_and_that_is_intended(self):
        """``except`` is relative to the REGISTRY CATEGORY, never to the
        parent's selection, because expansion is layer-local — which is what
        keeps ``deep_merge`` untouched.  A child ``except`` under a parent
        ``only`` is therefore *wider* than its parent.  This is deliberate: the
        parent-relative alternative forces post-merge normalisation, at which
        point parent ``{only: [x]}`` and child ``{except: [x]}`` dict-merge
        into ``{only: [x], except: [x]}`` and the child that explicitly asked
        for ``x`` gets nothing — a second silent-empty, in the feature whose
        job is to remove the first one.  Do not "fix" this."""
        got = self._resolve(
            {"tools": {"workspace": {"only": ["read_file"]}}},
            {"tools": {"workspace": {"except": ["write_file"]}}},
        )
        resolved = set(got["tools"]["workspace"])
        assert "write_file" not in resolved
        assert len(resolved) > 1


class TestResolveConfigSeam:
    """``capture["merged_fragment"]`` feeds the single policy decision point.

    ``_truthy`` in ``capability_grants`` is wrong on ``{}`` (grant violation
    missed) and on ``{only: []}`` (violation fabricated), so the sweep has to
    land before the capture, not inside ``load_agent_config_from_dict``.  Both
    of those shapes are also refused outright, which closes the rows.
    """

    def _resolve(self, **kw):
        from orchestrator.services.config_resolver import resolve_config

        capture: dict = {}
        resolve_config(base_config_name="session_base", capture=capture, **kw)
        return capture["merged_fragment"]

    def test_merged_fragment_carries_no_policy_values(self):
        frag = self._resolve(
            request_override={"tools": {"canvas": True, "orchestrator": False}}
        )
        tools = frag["tools"]
        assert all(isinstance(v, list) for v in tools.values()), tools
        assert set(tools["canvas"]) == set(expand_category_true("canvas"))
        assert tools["orchestrator"] == []

    def test_a_db_expert_fragment_is_normalised(self):
        frag = self._resolve(
            expert_row={"config": json.dumps({"tools": {"canvas": True}})}
        )
        assert isinstance(frag["tools"]["canvas"], list)
        assert set(frag["tools"]["canvas"]) == set(expand_category_true("canvas"))

    def test_layer_dicts_are_not_mutated(self):
        layer = {"tools": {"canvas": True}}
        self._resolve(request_override=layer)
        assert layer == {"tools": {"canvas": True}}

    def test_the_pdp_sees_a_shell_grant_request(self):
        from shared.runtime.core.capability_grants import evaluate

        frag = self._resolve(request_override={"tools": {"shell": ["run_command"]}})
        assert evaluate(frag, {"shell_tools": False}) != []
        off = self._resolve(request_override={"tools": {"shell": False}})
        assert evaluate(off, {"shell_tools": False}) == []


class TestSessionToolGroupMarkers:
    """``false`` must reach the marker map as ``[]``, or the closed groups
    stay on when a request turned them off."""

    def test_false_sets_the_disable_marker(self):
        from agent.api.persistent_app import (
            _apply_session_tool_group_markers,
            _CANVAS_DISABLED_KEY,
        )

        override = normalize_tool_policy({"tools": {"canvas": False}})
        merged: dict = {}
        _apply_session_tool_group_markers(merged, override)
        assert merged.get(_CANVAS_DISABLED_KEY) is True

    def test_true_clears_the_disable_marker(self):
        from agent.api.persistent_app import (
            _apply_session_tool_group_markers,
            _CANVAS_DISABLED_KEY,
        )

        override = normalize_tool_policy({"tools": {"canvas": True}})
        merged: dict = {_CANVAS_DISABLED_KEY: True}
        _apply_session_tool_group_markers(merged, override)
        assert _CANVAS_DISABLED_KEY not in merged


class TestEnumerateOnlyMembersAreServable:
    """The enumeration a UI needs in order to turn an enumerate-only category ON.

    ``ENUMERATE_ONLY_CATEGORIES`` refuses ``true`` so a tool added to a
    code-execution category cannot land in live configs with no diff. Correct —
    and it leaves a client unable to *ask* for the category at all unless
    something hands it the names. Serving them from the registry is what keeps
    that from becoming a hand-maintained tool-name list in the cockpit, which
    is the species of defect this whole change is removing.
    """

    def test_it_covers_exactly_the_categories_that_refuse_true(self):
        assert set(enumerate_only_members()) == set(ENUMERATE_ONLY_CATEGORIES)

    def test_every_served_enumeration_round_trips_through_the_write_boundary(self):
        """The payload it prescribes must be one the boundary accepts."""
        from shared.runtime.core.tool_policy import validate_tool_override_fragment

        for category, names in enumerate_only_members().items():
            assert names, f"{category} would be unenablable"
            accepted = validate_tool_override_fragment(
                {"tools": {category: {"only": names}}}
            )
            assert accepted[category] == names

    def test_true_is_still_refused_for_the_same_categories(self):
        """The served list is a workaround for the rule, not a repeal of it."""
        from shared.runtime.core.tool_policy import validate_tool_override_fragment

        for category in enumerate_only_members():
            with pytest.raises(ToolPolicyError, match="must enumerate"):
                validate_tool_override_fragment({"tools": {category: True}})

    def test_it_names_no_code_granted_tool(self):
        """`only` carrying a code-granted name would assert config manages it.
        An `explicit` name is the opposite case — naming it is how config
        grants it (`delegation`'s one member)."""
        from agent.tools.registry import TOOL_REGISTRY

        for names in enumerate_only_members().values():
            for name in names:
                assert TOOL_REGISTRY[name].get("grant") != "code", name
        assert enumerate_only_members()["delegation"] == [
            "delegate_agent",
            "list_agents",
            "message_agent",
            "stop_agent",
            "wait_agent",
        ]

    def test_shell_is_the_current_membership(self):
        assert enumerate_only_members()["shell"] == [
            "cancel_command",
            "run_command",
            "shell_execute",
            "shell_read",
        ]


class TestGitToolsSuppressedWhenShellPresent:
    """A shell can run git; the git_* tools cannot leave the job's own repo.

    Granting both gives the agent two ways to ask the same question, one of
    which silently answers about a different repository — the c4849fa1 failure.
    Shell wins, and the resolved config must SAY so rather than claiming git
    tools the pod will not bind.
    """

    GIT = ["git_log", "git_show", "git_diff", "git_status", "git_tags"]

    def test_shell_suppresses_git(self):
        cfg = ToolsConfig(git=list(self.GIT), shell=["run_command", "shell_read"])
        assert cfg.git == []
        assert cfg.shell == ["run_command", "shell_read"]

    def test_any_shell_tool_suppresses_git(self):
        assert ToolsConfig(git=list(self.GIT), shell=["shell_read"]).git == []
        assert ToolsConfig(git=list(self.GIT), shell=["shell_execute"]).git == []

    def test_git_kept_without_shell(self):
        cfg = ToolsConfig(git=list(self.GIT), shell=[])
        assert cfg.git == self.GIT

    def test_no_shell_no_git_is_untouched(self):
        assert ToolsConfig(git=[], shell=[]).git == []

    def test_repo_tools_are_never_suppressed(self):
        """repo_* carries the read_only enforcement — the shell cannot replace it."""
        cfg = ToolsConfig(
            git=list(self.GIT),
            repo=["repo_commit", "repo_push"],
            shell=["run_command"],
        )
        assert cfg.repo == ["repo_commit", "repo_push"]

    def test_resolved_config_reports_the_suppression(self):
        """The stored blob must match the pod, or the creation forms lie again."""
        config = load_agent_config_from_dict(
            {
                "agent_id": "test",
                "display_name": "Test",
                "tools": {"git": list(self.GIT), "shell": ["run_command"]},
            }
        )
        assert config.tools.git == []
