"""Pure capability-grants logic: catalog, resolution, PDP, and the adversarial
matrix (User-Defined Experts, Slice 2). No DB/framework — hermetic.

Spec: knowledge-history/done/global_expert_management.md (decisions 8, 9, 19, 21-23).
"""

import copy

import pytest

from shared.runtime.core.capability_grants import (
    CATALOG,
    evaluate,
    meet,
    resolve_grants,
    strip_to_grants,
)

DEFAULTS = {k: v["default"] for k, v in CATALOG.items()}


def _rows(**kv):
    return [{"key": k, "value_json": v} for k, v in kv.items()]


# --- Task 2: catalog + meet --------------------------------------------------


def test_catalog_keys_and_defaults():
    assert set(CATALOG) == {
        "personal_default_experts",
        "vm_workspace",
        "shell_tools",
        "delegation",
        "datasource_tools",
        "browser",
        "model_selection",
        "autonomy_ceiling",
        "permission_mode",
        "public_datasources",
        "email_autonomous_send",
        "catalog_authoring",
        "complete_unmerged_pr",
        "unattended_operations",
    }
    assert all(spec["restrict_only"] for spec in CATALOG.values())
    assert CATALOG["personal_default_experts"]["default"] is True
    assert CATALOG["vm_workspace"]["default"] is False
    assert CATALOG["shell_tools"]["default"] is False  # deny-by-default
    assert CATALOG["delegation"]["default"] is False
    assert CATALOG["public_datasources"]["default"] is False  # deny-by-default
    assert CATALOG["email_autonomous_send"]["default"] is False  # deny-by-default
    # Deny-by-default AND not backfilled: nobody held it before, so unlike
    # 0030's shell_tools/delegation there is nothing to grandfather.
    assert CATALOG["catalog_authoring"]["default"] is False
    assert CATALOG["browser"]["default"] is True  # spec-deferred allow
    assert CATALOG["datasource_tools"]["default"] is True
    assert CATALOG["model_selection"]["default"] is None
    assert CATALOG["autonomy_ceiling"]["default"] == "review"
    assert CATALOG["permission_mode"]["default"] == "auto_accept"
    # Deny-by-default and NOT backfilled: the gate can only fire on a job that
    # carries a recorded context.pull_request, and no such job existed when the
    # key was introduced, so there is nothing to grandfather.
    assert CATALOG["complete_unmerged_pr"]["default"] is False
    # Deny-by-default and NOT backfilled: loops and officers are unbounded
    # unattended spend, and nobody held a key for them before.
    assert CATALOG["unattended_operations"]["default"] is False


def test_meet_bool_enum_list():
    assert meet(CATALOG["vm_workspace"], True, False) is False
    assert meet(CATALOG["autonomy_ceiling"], "full", "review") == "review"
    assert meet(CATALOG["permission_mode"], "autonomous", "read_only") == "read_only"
    assert meet(CATALOG["model_selection"], None, ["a", "b"]) == ["a", "b"]
    assert meet(CATALOG["model_selection"], ["a", "b"], ["b", "c"]) == ["b"]


# --- Task 3: resolve_grants (restrict-only, escalation-safe) ------------------


def test_lone_user_grant_widens_past_default():
    g = resolve_grants(
        user_rows=_rows(shell_tools=True), project_rows=[], global_rows=[]
    )
    assert g["shell_tools"] is True
    assert g["vm_workspace"] is False and g["model_selection"] is None
    assert g["autonomy_ceiling"] == "review" and g["permission_mode"] == "auto_accept"


def test_project_cap_clamps_user_grant():
    g = resolve_grants(
        user_rows=_rows(shell_tools=True),
        project_rows=_rows(shell_tools=False),
        global_rows=[],
    )
    assert g["shell_tools"] is False


def test_autonomy_and_permission_clamped_by_global():
    g = resolve_grants(
        user_rows=_rows(autonomy_ceiling="full", permission_mode="autonomous"),
        project_rows=[],
        global_rows=_rows(autonomy_ceiling="review", permission_mode="auto_accept"),
    )
    assert g["autonomy_ceiling"] == "review" and g["permission_mode"] == "auto_accept"


def test_model_selection_intersects():
    g = resolve_grants(
        user_rows=_rows(model_selection=["a", "b"]),
        project_rows=_rows(model_selection=["b", "c"]),
        global_rows=[],
    )
    assert g["model_selection"] == ["b"]


# --- Task 4: the PDP evaluate() ----------------------------------------------


def test_allows_within_grants():
    assert (
        evaluate(
            {"tools": {"shell": ["ls"]}, "autonomy": "review"},
            {**DEFAULTS, "shell_tools": True},
        )
        == []
    )


def test_flags_ungranted_shell_and_vm_and_autonomy():
    v = evaluate(
        {
            "workspace": {"backend": "vm"},
            "tools": {"shell": ["ls"]},
            "autonomy": "full",
        },
        DEFAULTS,
    )
    j = " ".join(v)
    assert "shell_tools" in j and "vm_workspace" in j and "autonomy_ceiling" in j


def test_datasource_grant_covers_every_datasource_tool_category():
    for category in ("sql", "mongodb", "graph", "webdav", "email", "mcp", "repo"):
        violations = evaluate(
            {"tools": {category: ["tool"]}},
            {**DEFAULTS, "datasource_tools": False},
        )
        assert any("datasource_tools" in violation for violation in violations), (
            category
        )


def test_workspace_upgrade_gate_vm_vs_sandbox():
    """Sec-1 (workspace_tier_upgrade.md §4.4): the upgrade gate re-runs this PDP
    on the post-upgrade fragment {workspace:{backend:target_tier}}.

      * sandbox is the ungated default tier — clean for a default principal.
      * vm trips vm_workspace unless granted.
    """
    # sandbox upgrade — passes by default (no backend gate, no declared tools).
    assert evaluate({"workspace": {"backend": "sandbox"}}, DEFAULTS) == []
    # vm upgrade without the grant — refused.
    v = evaluate({"workspace": {"backend": "vm"}}, DEFAULTS)
    assert len(v) == 1 and "vm_workspace" in v[0]
    # vm upgrade with the grant — clean.
    assert (
        evaluate({"workspace": {"backend": "vm"}}, {**DEFAULTS, "vm_workspace": True})
        == []
    )


def test_workspace_upgrade_gate_refuses_shell_restricted_principal():
    """A principal whose config declares tools.shell but lacks the shell_tools
    grant is refused even for a sandbox upgrade — the upgrade re-runs dispatch
    enforcement on the post-upgrade config, so a shell restriction still bites."""
    post_upgrade = {"workspace": {"backend": "sandbox"}, "tools": {"shell": ["ls"]}}
    v = evaluate(post_upgrade, {**DEFAULTS, "shell_tools": False})
    assert len(v) == 1 and "shell_tools" in v[0]
    # Same config, owner holds the grant → clean.
    assert evaluate(post_upgrade, {**DEFAULTS, "shell_tools": True}) == []


def test_delegation_reads_enabled_not_dict_presence():
    # A disabled delegation settings-dict must NOT trip the gate.
    assert evaluate({"delegation": {"enabled": False, "max_depth": 3}}, DEFAULTS) == []
    assert evaluate(
        {"delegation": {"enabled": True}}, DEFAULTS
    )  # flagged (deny default)


def test_session_permission_mode_gated():
    # sessions use interactive.permission_mode, NOT autonomy. Default ceiling is
    # auto_accept (Phase 5): supervised + auto_accept pass without a grant;
    # autonomous (fully unattended) is gated.
    v = evaluate({"interactive": {"permission_mode": "autonomous"}}, DEFAULTS)
    assert len(v) == 1 and "permission_mode" in v[0]
    assert evaluate({"interactive": {"permission_mode": "read_only"}}, DEFAULTS) == []
    assert evaluate({"interactive": {"permission_mode": "auto_accept"}}, DEFAULTS) == []


def test_model_not_in_selection():
    v = evaluate(
        {"llm": {"strategic": {"model": "x"}, "tactical": {"model": "y"}}},
        {**DEFAULTS, "model_selection": ["y"]},
    )
    assert len(v) == 1 and "x" in v[0]


def test_admin_short_circuits_and_empty_is_clean():
    assert evaluate({"tools": {"shell": ["ls"]}}, DEFAULTS, is_admin=True) == []
    assert evaluate({}, DEFAULTS) == []


# --- Task 5: canonicalize-before-scan + reject non-ASCII keys -----------------


def test_rejects_duplicate_keys():
    from shared.runtime.core.expert_resolution import scan_fragment_text

    assert scan_fragment_text('{"llm": {"api_key": null, "api_key": "x"}}')


def test_rejects_non_ascii_key():
    # fullwidth 'api_key' — reject the non-ASCII key outright, don't try to normalize.
    from shared.runtime.core.expert_resolution import scan_fragment_text

    assert scan_fragment_text('{"llm": {"ａｐｉ＿ｋｅｙ": "x"}}')


def test_allows_clean_fragment_text():
    from shared.runtime.core.expert_resolution import scan_fragment_text

    assert (
        scan_fragment_text('{"llm": {"model": "gemma-4-moe"}, "tools": {"shell": []}}')
        == []
    )


# --- Task 9: resolve_config capture out-param --------------------------------


def test_resolve_config_capture_exposes_merged_fragment():
    from orchestrator.services.config_resolver import resolve_config

    cap: dict = {}
    resolve_config(base_config_name="defaults", capture=cap, expert_type="worker")
    assert "merged_fragment" in cap and isinstance(cap["merged_fragment"], dict)
    assert (
        "tools" in cap["merged_fragment"]
    )  # base tools present (the deny-by-default subject)


# --- Task 15: adversarial matrix --------------------------------------------


def test_user_grant_cannot_exceed_project_ceiling():
    g = resolve_grants(
        user_rows=[{"key": "autonomy_ceiling", "value_json": "full"}],
        project_rows=[{"key": "autonomy_ceiling", "value_json": "guided"}],
        global_rows=[],
    )
    assert g["autonomy_ceiling"] == "guided"


def test_null_deletion_of_guardrail_caught_in_merged():
    d = {k: v["default"] for k, v in CATALOG.items()}
    assert evaluate({"autonomy": "full"}, d)  # ceiling review -> flagged


def test_cross_layer_credential_assembly_denied():
    from shared.runtime.core.expert_resolution import hard_deny_scan

    assert hard_deny_scan({"llm": {"model": "x", "api_key": "leaked"}})


def test_worker_base_is_safe_for_a_new_principal():
    """The framework fallback itself must not require privileged grants."""
    from orchestrator.services.config_resolver import resolve_config

    cap: dict = {}
    resolve_config(base_config_name="defaults", capture=cap, expert_type="worker")
    frag = cap["merged_fragment"]
    deny = {k: v["default"] for k, v in CATALOG.items()}
    flagged = {v.split(":")[0] for v in evaluate(frag, deny)}
    assert flagged == set()


def test_session_base_is_safe_for_a_new_principal():
    from orchestrator.services.config_resolver import resolve_config

    cap: dict = {}
    resolve_config(
        base_config_name="persistent_defaults", capture=cap, expert_type="session"
    )
    frag = cap["merged_fragment"]
    deny = {k: v["default"] for k, v in CATALOG.items()}
    flagged = {v.split(":")[0] for v in evaluate(frag, deny)}
    assert flagged == set()


def test_specialists_explicitly_opt_in_to_delegation():
    """The five delegating profiles explicitly grant spawn and controls."""
    from orchestrator.services.config_resolver import resolve_config

    for name in ("bughunter", "critic", "developer", "product-qa", "scholar"):
        cap: dict = {}
        resolve_config(base_config_name=name, capture=cap, expert_type="worker")
        fragment = cap["merged_fragment"]
        assert fragment["delegation"]["enabled"] is True
        assert fragment["tools"]["delegation"] == [
            "delegate_agent",
            "wait_agent",
            "message_agent",
            "stop_agent",
            "list_agents",
        ]


# --- strip_to_grants (2026-08-04 plan, expert-write-gate-holes, task 3) ------
#
# `duplicate_expert` strips a source config down to what the copier's grants
# allow instead of refusing outright (the other four expert-write routes are
# untouched and still refuse via `evaluate` alone). This is the pure map;
# the route-level safety re-check (`evaluate` re-run on the stripped result,
# 422 on any residual violation) lives in
# orchestrator/main.py::_strip_save_grants and is exercised in
# tests/test_capability_grants_api.py, not here.


def _most_permissive(spec: dict) -> object:
    """The value for this CATALOG spec that never trips a violation on its
    own — bool True (granted), the least-restrictive enum step, or `None`
    (list: allow-all). Type-derived, not default-derived: `browser` and
    `datasource_tools` default True already, so a helper that assumed every
    grant defaults False would get those two backwards."""
    if spec["type"] == "bool":
        return True
    if spec["type"] == "enum":
        return spec["order"][-1]
    if spec["type"] == "list":
        return None
    raise AssertionError(f"unknown catalog type {spec['type']!r}")


def _most_restrictive(spec: dict) -> object:
    """The complement of `_most_permissive`: the value most likely to trip a
    violation — bool False (denied), the most-restrictive enum step, or `[]`
    (list: allow-nothing)."""
    if spec["type"] == "bool":
        return False
    if spec["type"] == "enum":
        return spec["order"][0]
    if spec["type"] == "list":
        return []
    raise AssertionError(f"unknown catalog type {spec['type']!r}")


def _deny_only(key: str) -> dict:
    """Every CATALOG key at its most permissive value except `key`, which is
    pinned to its most restrictive. Isolates one rule: a fragment that could
    trip several rules at once only shows the one under test, because every
    other gate's condition is `not <permissive-grant> and ...` (bool) or
    `value_index > ceiling_index` with the ceiling at its top step (enum) —
    both structurally False regardless of the fragment."""
    grants = {k: _most_permissive(spec) for k, spec in CATALOG.items()}
    grants[key] = _most_restrictive(CATALOG[key])
    return grants


# One fragment per rule. Each also carries an untouched "git" tool entry (or
# equivalent) so a too-broad strip — the failure mode `strip_to_grants` exists
# to avoid, since deleting the wrong thing is silent until this test catches
# it — has something to be caught clobbering.
_RULE_FRAGMENTS: dict[str, dict] = {
    "shell_tools": {"tools": {"shell": ["run_command"], "git": ["git_status"]}},
    "delegation": {
        "tools": {"delegation": ["delegate_agent"], "git": ["git_status"]},
        "delegation": {
            "enabled": True,
            "max_concurrent": 3,
            "run_in_background_default": True,
        },
    },
    "datasource_tools": {
        "tools": {
            "sql": ["run_query"],
            "mongodb": ["run_query"],
            "graph": ["run_query"],
            "webdav": ["list_files"],
            "email": ["send_email"],
            "mcp": ["call_tool"],
            "repo": ["clone_repo"],
            "git": ["git_status"],
        }
    },
    "browser": {
        "tools": {"browser_direct": ["browser_navigate"], "git": ["git_status"]}
    },
    "catalog_authoring": {
        "tools": {"catalog_authoring": ["create_expert"], "git": ["git_status"]}
    },
    "unattended_operations": {
        "officer": {"enabled": True, "sleep_seconds": 900, "max_daily_spend": 20}
    },
    "vm_workspace": {"workspace": {"backend": "vm", "structure": ["notes/"]}},
    "model_selection": {"llm": {"model": "gpt-5.6-sol"}},
    "autonomy_ceiling": {"autonomy": "full"},
    "permission_mode": {"interactive": {"permission_mode": "autonomous"}},
}


@pytest.mark.parametrize("grant_key", sorted(_RULE_FRAGMENTS))
def test_each_rule_strips_and_reports_exactly_its_own_grant(grant_key):
    """Test group 1: each of the ten rules, in isolation, comes back clean
    with that grant (and only that grant) reported."""
    fragment = _RULE_FRAGMENTS[grant_key]
    grants = _deny_only(grant_key)

    stripped, dropped = strip_to_grants(fragment, grants)

    assert dropped == [grant_key]
    assert evaluate(stripped, grants) == []


def test_shell_strip_leaves_other_tool_categories_alone():
    stripped, _ = strip_to_grants(
        _RULE_FRAGMENTS["shell_tools"], _deny_only("shell_tools")
    )
    assert "shell" not in stripped["tools"]
    assert stripped["tools"]["git"] == ["git_status"]


def test_delegation_strip_keeps_settings_that_were_never_the_violation():
    """The comment in `evaluate` is explicit that delegation gates on
    `.enabled is True` or a non-empty `tools.delegation`, not on the settings
    dict merely existing — so the strip must take `tools.delegation` and only
    the `.enabled` flag, never the whole `delegation` dict (that would also
    drop `max_concurrent`/`run_in_background_default`, which were never the
    problem)."""
    stripped, dropped = strip_to_grants(
        _RULE_FRAGMENTS["delegation"], _deny_only("delegation")
    )
    assert dropped == ["delegation"]
    assert "delegation" not in stripped["tools"]
    assert stripped["tools"]["git"] == ["git_status"]
    assert "enabled" not in stripped["delegation"]
    assert stripped["delegation"]["max_concurrent"] == 3
    assert stripped["delegation"]["run_in_background_default"] is True


def test_unattended_operations_gates_officer_enabled_only():
    """The rule fires on `officer.enabled` and nothing else: the rest of the
    kit (slots, sleep bounds, pools) is inert configuration on a post nobody
    holds, so a user without the grant may still edit it — he just cannot raise
    anyone onto it. Mirrors delegation's `.enabled`-not-dict-presence rule."""
    denied = _deny_only("unattended_operations")

    assert evaluate({"officer": {"sleep_seconds": 900}}, denied) == []
    assert evaluate({"officer": {"enabled": False}}, denied) == []
    assert evaluate({}, denied) == []

    flagged = evaluate({"officer": {"enabled": True}}, denied)
    assert len(flagged) == 1
    assert flagged[0].startswith("unattended_operations:")


def test_unattended_operations_does_not_read_the_string_false_as_enabled():
    """`_truthy` would call the string "false" enabled; this rule uses the same
    explicit truthy set as main._officer_meta_enabled, which is what actually
    decides whether a thread boots as an officer. A mismatch either way is a
    real defect: too loose refuses a config that would never boot an officer,
    too tight lets one boot ungated."""
    denied = _deny_only("unattended_operations")

    assert evaluate({"officer": {"enabled": "false"}}, denied) == []
    for truthy in (True, "true", "True", 1):
        assert evaluate({"officer": {"enabled": truthy}}, denied), truthy


def test_unattended_operations_strip_keeps_the_rest_of_the_kit():
    """Like delegation: drop only `.enabled`, never the whole officer dict —
    sleep bounds and spend ceilings were never the violation."""
    stripped, dropped = strip_to_grants(
        _RULE_FRAGMENTS["unattended_operations"], _deny_only("unattended_operations")
    )

    assert dropped == ["unattended_operations"]
    assert "enabled" not in stripped["officer"]
    assert stripped["officer"]["sleep_seconds"] == 900
    assert stripped["officer"]["max_daily_spend"] == 20


def test_unattended_operations_denied_by_default():
    """The whole point of the key: a brand-new principal cannot start a loop or
    commission an officer until an administrator grants it."""
    assert DEFAULTS["unattended_operations"] is False
    assert evaluate({"officer": {"enabled": True}}, DEFAULTS)


def test_datasource_strip_removes_every_connector_category():
    stripped, dropped = strip_to_grants(
        _RULE_FRAGMENTS["datasource_tools"], _deny_only("datasource_tools")
    )
    assert dropped == ["datasource_tools"]
    for category in ("sql", "mongodb", "graph", "webdav", "email", "mcp", "repo"):
        assert category not in stripped["tools"]
    assert stripped["tools"]["git"] == ["git_status"]


def test_model_selection_strip_drops_only_the_offending_pin():
    """'the offending model pins' (plural, task-3 brief) — a pin already
    inside the permitted set must survive; only the ones outside it go."""
    fragment = {
        "llm": {
            "model": "allowed-model",
            "strategic": {"model": "blocked-model"},
            "tactical": {"model": "allowed-model"},
        }
    }
    grants = {**_deny_only("model_selection"), "model_selection": ["allowed-model"]}

    stripped, dropped = strip_to_grants(fragment, grants)

    assert dropped == ["model_selection"]
    assert stripped["llm"]["model"] == "allowed-model"
    assert stripped["llm"]["tactical"]["model"] == "allowed-model"
    assert "model" not in stripped["llm"]["strategic"]


def test_strip_does_not_mutate_the_input_fragment():
    fragment = _RULE_FRAGMENTS["shell_tools"]
    before = copy.deepcopy(fragment)

    strip_to_grants(fragment, _deny_only("shell_tools"))

    assert fragment == before


def test_strip_does_not_delete_an_emptied_parent_dict():
    """`tools` becomes `{}`, not absent, once its only key is gone — the brief
    is explicit that removing an emptied parent could change meaning."""
    fragment = {"tools": {"shell": ["run_command"]}}

    stripped, dropped = strip_to_grants(fragment, _deny_only("shell_tools"))

    assert dropped == ["shell_tools"]
    assert stripped["tools"] == {}


def test_a_user_with_every_grant_gets_an_unmodified_copy():
    """Test group 5: nothing is dropped, and the config is unchanged, for a
    copier whose grants already cover a fragment that would otherwise trip
    every one of the nine rules at once. If stripping fired regardless of
    grants held, this would catch it."""
    grants = {k: _most_permissive(spec) for k, spec in CATALOG.items()}
    fragment = {
        "tools": {
            "shell": ["run_command"],
            "delegation": ["delegate_agent"],
            "sql": ["run_query"],
            "browser_direct": ["browser_navigate"],
            "catalog_authoring": ["create_expert"],
        },
        "delegation": {"enabled": True},
        "workspace": {"backend": "vm"},
        "autonomy": "full",
        "interactive": {"permission_mode": "autonomous"},
    }
    before = copy.deepcopy(fragment)

    stripped, dropped = strip_to_grants(fragment, grants)

    assert dropped == []
    assert stripped == before
    assert fragment == before  # still not mutated


def test_defaults_grant_browser_and_datasource_but_deny_shell_and_delegation():
    """Sanity check against the REAL catalog defaults (not the permissive-
    except-one construction above): browser/datasource_tools default True
    (granted, so untouched), shell_tools/delegation default False (denied, so
    stripped). A completeness check that assumed every grant defaults False
    would get browser/datasource_tools backwards."""
    fragment = {
        "tools": {
            "shell": ["run_command"],
            "delegation": ["delegate_agent"],
            "browser_direct": ["browser_navigate"],
            "sql": ["run_query"],
        },
        "delegation": {"enabled": True},
    }

    stripped, dropped = strip_to_grants(fragment, DEFAULTS)

    assert sorted(dropped) == ["delegation", "shell_tools"]
    assert stripped["tools"]["browser_direct"] == ["browser_navigate"]
    assert stripped["tools"]["sql"] == ["run_query"]


def test_kitchen_sink_fragment_strips_every_current_violation_at_once():
    """Regression/integration check, NOT the completeness mechanism (see
    `test_strip_map_covers_every_catalog_key_or_is_explicitly_excluded`
    below for that): a fragment that violates all nine rules `evaluate`
    currently enforces, stripped in one pass against fully-denied grants,
    comes back fully clean under the REAL (unmocked) `evaluate`. This is
    still useful — it is the only test exercising all nine branches against
    one shared fragment/`out` dict together, catching e.g. one branch's
    deletion clobbering another's key in the same `tools` dict — but it
    proves nothing about a grant this fragment doesn't happen to violate,
    which is exactly why it cannot stand in for completeness (see below).
    """
    kitchen_sink = {
        "tools": {
            "shell": ["run_command"],
            "delegation": ["delegate_agent"],
            "sql": ["run_query"],
            "mongodb": ["run_query"],
            "graph": ["run_query"],
            "webdav": ["list_files"],
            "email": ["send_email"],
            "mcp": ["call_tool"],
            "repo": ["clone_repo"],
            "browser_direct": ["browser_navigate"],
            "catalog_authoring": ["create_expert"],
        },
        "delegation": {"enabled": True},
        "officer": {"enabled": True},
        "workspace": {"backend": "vm"},
        "llm": {"model": "some-model"},
        "autonomy": "full",
        "interactive": {"permission_mode": "autonomous"},
    }
    grants = {k: _most_restrictive(spec) for k, spec in CATALOG.items()}

    stripped, dropped = strip_to_grants(kitchen_sink, grants)

    assert sorted(dropped) == sorted(
        [
            "shell_tools",
            "delegation",
            "datasource_tools",
            "browser",
            "catalog_authoring",
            "unattended_operations",
            "vm_workspace",
            "model_selection",
            "autonomy_ceiling",
            "permission_mode",
        ]
    )
    assert evaluate(stripped, grants) == []


#: Grant keys `evaluate` never reads at all — not part of the fragment PDP
#: (they gate other endpoints: personal-default forking, datasource
#: publishing, email autonomous-send), so `strip_to_grants` correctly has no
#: branch for them. Explicit, reviewed, and commented, rather than "whatever
#: the probe didn't happen to trigger" — see the test below for why that
#: distinction is load-bearing.
_NOT_ENFORCED_BY_EVALUATE_FRAGMENT_PDP = {
    "personal_default_experts": (
        "gates POST /api/expert-defaults/{type}/fork directly "
        "(fork_my_expert_default's own check) — never read by evaluate()"
    ),
    "public_datasources": (
        "gates is_global at the datasource routes, not a field evaluate() "
        "reads on an expert fragment"
    ),
    "email_autonomous_send": (
        "gates datasource.unattended_send at the datasource routes, not a "
        "field evaluate() reads on an expert fragment"
    ),
    "complete_unmerged_pr": (
        "gates the terminal job transition against LIVE forge state at "
        "approve_job and the autonomous seal — there is no config fragment "
        "to strip, so evaluate() never reads it"
    ),
}


def test_strip_map_covers_every_catalog_key_or_is_explicitly_excluded(monkeypatch):
    """Test group 2: completeness, derived from the PDP — not a hand-written
    list, and NOT dependent on any probe fragment triggering anything.

    **Why the previous version of this test was wrong, and how a reviewer
    proved it**: it iterated `CATALOG` correctly, but asked "does `evaluate`
    flag this key" by running the real `evaluate` against one hand-written
    probe fragment (`kitchen_sink`) and checking whether THAT fragment
    happened to trip the rule. A CATALOG key can gain a real `evaluate` rule
    that the probe never touches — reviewer added `network_egress` (bool) to
    CATALOG plus a matching `evaluate` rule gating `network.egress`, left
    `strip_to_grants` untouched, and the old test still passed (`checked`
    stayed at 9; `>= 9` cannot notice a rule it never saw). Driving the route
    then 422'd a default-grants non-admin instead of stripping — the exact
    defect this task exists to remove, silently reintroduced.

    **The fix removes the probe dependency entirely**: `evaluate` is
    monkeypatched (module-level, so `strip_to_grants`'s own unqualified call
    picks it up) to report a violation for exactly one `key` at a time,
    regardless of fragment content — so whether `strip_to_grants` handles
    that key is no longer gated by whether any fragment "happens to" trigger
    it. A CATALOG key with no matching branch fails immediately, UNLESS it is
    named in `_NOT_ENFORCED_BY_EVALUATE_FRAGMENT_PDP` with a reason. There is
    no third, silent outcome: every key is either handled here or excluded
    there, and the test enumerates `CATALOG` itself, so a key added later
    (like the reviewer's `network_egress`) is automatically in-scope with no
    edit to this test.
    """
    import shared.runtime.core.capability_grants as capability_grants

    for key in CATALOG:
        if key in _NOT_ENFORCED_BY_EVALUATE_FRAGMENT_PDP:
            continue

        monkeypatch.setattr(
            capability_grants,
            "evaluate",
            lambda fragment, grants, _key=key: [
                f"{_key}: forced for completeness test"
            ],
        )

        _, dropped = strip_to_grants({}, {})

        assert dropped == [key], (
            f"CATALOG key {key!r} has no strip_to_grants branch that reacts "
            "to it (or a branch reported a different key) — add one, or if "
            "evaluate() genuinely never flags this key on an expert "
            "fragment, add it to _NOT_ENFORCED_BY_EVALUATE_FRAGMENT_PDP with "
            "a reason"
        )


def test_every_catalog_key_is_handled_or_excluded_not_both():
    """A key cannot silently satisfy the completeness test by sitting in
    both categories — the exclusion set is for keys evaluate() truly never
    reads, not an escape hatch for a key someone forgot to wire up."""
    assert set(_NOT_ENFORCED_BY_EVALUATE_FRAGMENT_PDP) <= set(CATALOG)
    for key, reason in _NOT_ENFORCED_BY_EVALUATE_FRAGMENT_PDP.items():
        assert reason, f"{key!r} needs a real reason, not a placeholder"


# --- U1 WP4: roster entries are a tool / model surface of their own ----------
#
# A child runs with the tools its `subagents.roster.<name>` entry names, so the
# tool gates and model_selection apply per entry — at save on the raw entries,
# at dispatch on the materialised ones (`resolve_config` resolves the roster
# BEFORE the PDP capture). Parent-only keys (`workspace.backend`, `autonomy`,
# `delegation`, `tools.core`, `officer`) are pruned from every child by the
# subagent overlay and `interactive.permission_mode` is `autonomous` on every
# child by design, so those rules are NOT applied to entries.


def test_roster_shell_tools_require_grant():
    fragment = {
        "tools": {"git": ["git_status"]},
        "subagents": {
            "roster": {
                "fixer": {"tools": {"shell": ["run_command"], "git": ["git_status"]}},
                "reader": {"tools": {"workspace": ["read_file"]}},
            }
        },
    }

    v = evaluate(fragment, DEFAULTS)

    assert len(v) == 1
    assert v[0].startswith("shell_tools:") and "subagents.roster.fixer" in v[0]
    assert evaluate(fragment, {**DEFAULTS, "shell_tools": True}) == []

    stripped, dropped = strip_to_grants(fragment, DEFAULTS)

    assert dropped == ["shell_tools"]
    fixer = stripped["subagents"]["roster"]["fixer"]["tools"]
    assert "shell" not in fixer and fixer["git"] == ["git_status"]
    assert (
        stripped["subagents"]["roster"]["reader"]
        == fragment["subagents"]["roster"]["reader"]
    )
    assert stripped["tools"] == {"git": ["git_status"]}
    assert evaluate(stripped, DEFAULTS) == []


@pytest.mark.parametrize(
    ("grant_key", "tools"),
    [
        ("datasource_tools", {"sql": ["run_query"], "repo": ["clone_repo"]}),
        ("browser", {"browser_direct": ["browser_navigate"]}),
        ("catalog_authoring", {"catalog_authoring": ["create_expert"]}),
    ],
)
def test_roster_tool_gates_apply_per_entry(grant_key, tools):
    fragment = {
        "subagents": {"roster": {"x": {"tools": {**tools, "git": ["git_log"]}}}}
    }
    grants = _deny_only(grant_key)

    v = evaluate(fragment, grants)

    assert len(v) == 1 and v[0].startswith(f"{grant_key}:")
    assert "subagents.roster.x" in v[0]

    stripped, dropped = strip_to_grants(fragment, grants)

    assert dropped == [grant_key]
    assert stripped["subagents"]["roster"]["x"]["tools"]["git"] == ["git_log"]
    assert evaluate(stripped, grants) == []


def test_roster_model_selection():
    """The roster-wide `subagents.llm.model` and each entry's own pin are model
    selections; `inherit` is the parent's (already gated) model, not one."""
    fragment = {
        "llm": {"model": "allowed"},
        "subagents": {
            "llm": {"model": "roster-wide-blocked"},
            "roster": {
                "pinned": {"llm": {"model": "entry-blocked"}},
                "ok": {"llm": {"model": "allowed"}},
                "twin": {"llm": {"model": "inherit"}},
            },
        },
    }
    grants = {**_deny_only("model_selection"), "model_selection": ["allowed"]}

    v = evaluate(fragment, grants)

    assert sorted(v) == [
        "model_selection: model 'entry-blocked' is not in the permitted set",
        "model_selection: model 'roster-wide-blocked' is not in the permitted set",
    ]

    stripped, dropped = strip_to_grants(fragment, grants)

    assert dropped == ["model_selection"]
    assert "model" not in stripped["subagents"]["llm"]
    assert "model" not in stripped["subagents"]["roster"]["pinned"]["llm"]
    assert stripped["subagents"]["roster"]["ok"]["llm"]["model"] == "allowed"
    assert stripped["subagents"]["roster"]["twin"]["llm"]["model"] == "inherit"
    assert stripped["llm"]["model"] == "allowed"
    assert evaluate(stripped, grants) == []


def test_resolved_roster_children_are_safe_for_a_new_principal():
    """The dispatch shape: a materialised roster (the read-only library entry
    and an inline child) under the safe worker base trips nothing for a
    default-grants principal — in particular NOT `permission_mode`, which
    every child pins to `autonomous` by design."""
    from orchestrator.services.config_resolver import resolve_config

    parent = {
        "expert_type": "worker",
        "name": "lead",
        "config": {
            "llm": {"model": "lead-model"},
            "subagents": {
                "roster": {
                    "explorer": {"$ref": "subagents/explorer"},
                    "inline": {"tools": {"workspace": ["read_file"]}},
                }
            },
        },
        "prompts": {},
    }
    cap: dict = {}
    resolve_config(
        base_config_name="worker_base",
        expert_row=parent,
        expert_type="worker",
        capture=cap,
    )
    roster = cap["merged_fragment"]["subagents"]["roster"]
    assert set(roster) == {"explorer", "inline"}
    assert all(
        e["interactive"]["permission_mode"] == "autonomous" for e in roster.values()
    )
    assert evaluate(cap["merged_fragment"], DEFAULTS) == []
    # …and a child that names a shell IS caught on the materialised entry.
    parent["config"]["subagents"]["roster"]["inline"]["tools"]["shell"] = [
        "run_command"
    ]
    cap = {}
    resolve_config(
        base_config_name="worker_base",
        expert_row=parent,
        expert_type="worker",
        capture=cap,
    )
    flagged = evaluate(cap["merged_fragment"], DEFAULTS)
    assert len(flagged) == 1 and "subagents.roster.inline" in flagged[0]
