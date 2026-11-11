"""src/core/tool_report.py — measured vs predicted, and the three states.

The property that matters most here is the one D6 exists to protect: when the
agent has reported, the agent's answer wins. ``compose_tool_view`` may use the
merged config to EXPLAIN a measurement and must never use it to correct one —
a "fix-up" is a second implementation of the same fact, which is the original
bug in this series.
"""

import pytest

from shared.runtime.core.capability_grants import CATALOG, evaluate
from shared.runtime.core.session_tool_overrides import SESSION_TOOL_OVERRIDE_NAMES
from shared.runtime.core.tool_report import (
    MEASURED_ORIGINS,
    ORIGIN_AGENT,
    ORIGIN_AGENT_PARTIAL,
    ORIGIN_PREDICTION,
    DECIDED_BY_BACKEND,
    DECIDED_BY_GRANT,
    DECIDED_BY_REGISTRY,
    DECIDED_BY_RUNTIME,
    DECIDED_BY_UNSET,
    GRANT_GATED_CATEGORIES,
    REPORT_VERSION,
    _GRANT_REASONS,
    STATE_OFF,
    STATE_ON,
    STATE_UNAVAILABLE,
    UNCLASSIFIED_CATEGORY,
    ToolReportError,
    backend_blocked_categories,
    backend_capabilities,
    build_agent_toolset_report,
    categorize_tool_names,
    code_granted_categories,
    code_granted_tools,
    compose_tool_view,
    grant_blocked_categories,
    layer_provenance,
    read_agent_toolset_report,
    report_categories,
    tool_groups_from_view,
)


class _Backend:
    def __init__(self, shell=True, files=True, canvas=True):
        self.supports_shell = shell
        self.supports_file_tools = files
        self.supports_canvas_presentation = canvas


# =============================================================================
# Vocabulary — every category a config can name is a category we answer for
# =============================================================================
class TestVocabulary:
    def test_covers_every_config_addressable_category(self):
        from shared.runtime.core.tool_policy import config_tool_categories

        assert set(report_categories()) == set(config_tool_categories())

    def test_covers_far_more_than_the_four_closed_groups(self):
        """The shipped endpoint answered for 4 of 24; that was the defect."""
        assert set(SESSION_TOOL_OVERRIDE_NAMES) < set(report_categories())
        assert len(report_categories()) >= 24

    def test_mcp_is_in_the_vocabulary_despite_having_no_static_membership(self):
        assert "mcp" in report_categories()


class TestCategorizeToolNames:
    def test_groups_by_registry_category(self):
        grouped = categorize_tool_names(["read_file", "web_search", "read_file"])
        assert grouped["workspace"] == ["read_file", "read_file"]
        assert grouped["research"] == ["web_search"]

    def test_preserves_author_order(self):
        grouped = categorize_tool_names(["write_file", "read_file"])
        assert grouped["workspace"] == ["write_file", "read_file"]

    def test_unknown_names_are_named_not_dropped(self):
        """An MCP tool categorised outside the agent must not vanish."""
        grouped = categorize_tool_names(["definitely_not_a_registered_tool"])
        assert grouped[UNCLASSIFIED_CATEGORY] == ["definitely_not_a_registered_tool"]


# =============================================================================
# The agent's half — measurement
# =============================================================================
class TestAgentReport:
    def test_reports_the_names_it_was_given_verbatim(self):
        report = build_agent_toolset_report(
            thread_id="t1", tool_names=["read_file", "web_search"]
        )
        assert report["tools"] == ["read_file", "web_search"]
        assert report["categories"]["workspace"] == ["read_file"]
        assert report["version"] == REPORT_VERSION
        assert report["observed_at"]

    def test_backend_is_none_when_there_is_no_backend(self):
        report = build_agent_toolset_report(thread_id="t1", tool_names=[])
        assert report["backend"] is None

    def test_backend_capabilities_are_carried(self):
        report = build_agent_toolset_report(
            thread_id="t1", tool_names=[], backend=_Backend(shell=False)
        )
        assert report["backend"] == {
            "supports_shell": False,
            "supports_file_tools": True,
            "supports_canvas_presentation": True,
        }

    def test_empty_toolset_is_a_legitimate_measurement(self):
        report = build_agent_toolset_report(thread_id="t1", tool_names=[])
        assert report["tools"] == []
        assert report["categories"] == {}

    def test_round_trips_through_the_reader(self):
        report = build_agent_toolset_report(
            thread_id="t1", tool_names=["read_file", "web_search"]
        )
        assert read_agent_toolset_report(report) == {
            "workspace": ["read_file"],
            "research": ["web_search"],
        }


class TestReaderRefusesRatherThanGuesses:
    """A mis-parsed report would be served as a MEASUREMENT. Refuse instead."""

    def test_unknown_version_is_refused(self):
        with pytest.raises(ToolReportError, match="version"):
            read_agent_toolset_report({"version": 99, "categories": {}})

    def test_missing_version_is_refused(self):
        with pytest.raises(ToolReportError, match="version"):
            read_agent_toolset_report({"categories": {}})

    def test_missing_categories_is_refused(self):
        with pytest.raises(ToolReportError, match="categories"):
            read_agent_toolset_report({"version": REPORT_VERSION})

    def test_non_dict_is_refused(self):
        with pytest.raises(ToolReportError):
            read_agent_toolset_report(["read_file"])

    def test_malformed_category_value_is_refused(self):
        with pytest.raises(ToolReportError, match="malformed"):
            read_agent_toolset_report(
                {"version": REPORT_VERSION, "categories": {"workspace": "read_file"}}
            )


# =============================================================================
# Gates
# =============================================================================
class TestBackendGate:
    def test_no_backend_gates_nothing(self):
        assert backend_capabilities(None) is None
        assert backend_blocked_categories(None) == {}

    def test_no_shell_blocks_exactly_the_execution_categories(self):
        from agent.tools.registry import _EXECUTION_CATEGORIES

        blocked = backend_blocked_categories(
            backend_capabilities(_Backend(shell=False))
        )
        assert set(blocked) == set(_EXECUTION_CATEGORIES)

    def test_no_file_tools_blocks_workspace_and_canvas(self):
        blocked = backend_blocked_categories(
            backend_capabilities(_Backend(files=False))
        )
        assert "workspace" in blocked and "canvas" in blocked

    def test_no_canvas_presentation_blocks_only_canvas(self):
        blocked = backend_blocked_categories(
            backend_capabilities(_Backend(canvas=False))
        )
        assert set(blocked) == {"canvas"}

    def test_a_fully_capable_backend_blocks_nothing(self):
        assert backend_blocked_categories(backend_capabilities(_Backend())) == {}


class TestGrantGate:
    def test_admin_bypass_blocks_nothing(self):
        assert grant_blocked_categories(None) == {}

    def test_missing_shell_tools_blocks_shell_with_a_reason(self):
        blocked = grant_blocked_categories({"shell_tools": False})
        assert "shell_tools" in blocked["shell"]

    def test_absent_key_falls_back_to_the_catalog_default(self):
        """`{}` reads the catalog defaults: shell, delegation, catalog
        authoring (granted by default since 2026-11-11) and browser are not
        blocked; each is once its key is set to false."""
        blocked = grant_blocked_categories({})
        for category in ("shell", "delegation", "catalog_authoring", "browser_direct"):
            assert category not in blocked
        assert "shell" in grant_blocked_categories({"shell_tools": False})
        assert "delegation" in grant_blocked_categories({"delegation": False})
        assert "catalog_authoring" in grant_blocked_categories(
            {"catalog_authoring": False}
        )

    def test_datasource_tools_blocks_every_connector_category(self):
        blocked = grant_blocked_categories({"datasource_tools": False})
        assert GRANT_GATED_CATEGORIES["datasource_tools"] <= set(blocked)


class TestGrantMapMatchesThePDP:
    """Re-derive GRANT_GATED_CATEGORIES by running the real PDP.

    This module explains a denial; ``capability_grants.evaluate`` enforces it.
    Two statements of one rule is exactly the shape of bug this series exists
    to kill, so the literal is checked against the PDP rather than trusted.
    """

    @pytest.mark.parametrize("grant_key", sorted(GRANT_GATED_CATEGORIES))
    def test_each_gate_denies_exactly_the_categories_claimed(self, grant_key):
        denying = {
            k: CATALOG[k]["default"] for k in CATALOG if CATALOG[k]["type"] != "list"
        }
        denying[grant_key] = False
        actually_denied = set()
        for category in report_categories():
            fragment = {"tools": {category: ["_probe_"]}}
            violations = evaluate(fragment, denying)
            if any(v.startswith(f"{grant_key}:") for v in violations):
                actually_denied.add(category)
        assert actually_denied == GRANT_GATED_CATEGORIES[grant_key]

    def test_no_gate_the_pdp_enforces_is_missing_from_the_map(self):
        """The inverse direction, which the parametrised test cannot see.

        Parametrising over ``GRANT_GATED_CATEGORIES`` checks every claim in the
        map but never notices a grant the PDP enforces and the map omits — and
        an omission is the failure that matters: the denial still happens, the
        explanation just goes missing, so the pane says "off" where it should say
        "needs a grant".

        Filed in advance as
        ``knowledge-base/knowledge/issues/tool_configuration_deferred_findings.md`` §4.4, and adding
        ``catalog_authoring`` walked straight into it — the map entry was missing
        and every test passed.
        """
        bool_grants = [k for k, spec in CATALOG.items() if spec["type"] == "bool"]
        for grant_key in bool_grants:
            denying = {
                k: CATALOG[k]["default"]
                for k in CATALOG
                if CATALOG[k]["type"] != "list"
            }
            denying[grant_key] = False
            denies_something = any(
                v.startswith(f"{grant_key}:")
                for category in report_categories()
                for v in evaluate({"tools": {category: ["_probe_"]}}, denying)
            )
            if denies_something:
                assert grant_key in GRANT_GATED_CATEGORIES, (
                    f"{grant_key} gates a tool category in the PDP but has no "
                    f"entry in GRANT_GATED_CATEGORIES, so a blocked user is "
                    f"shown a plain 'off' with no reason"
                )
                assert grant_key in _GRANT_REASONS, (
                    f"{grant_key} is mapped but has no human-readable reason"
                )


class TestCodeGrantedCategories:
    def test_matches_the_registry(self):
        from agent.tools.registry import CODE_GRANTED_CATEGORIES

        assert set(code_granted_categories()) == set(CODE_GRANTED_CATEGORIES)


# =============================================================================
# Provenance
# =============================================================================
class TestLayerProvenance:
    def test_the_last_layer_naming_a_category_owns_it(self):
        prov = layer_provenance(
            [
                ("base", {"tools": {"workspace": ["read_file"], "shell": []}}),
                ("expert", {"tools": {"shell": ["run_command"]}}),
                ("request", {"tools": {"canvas": []}}),
            ]
        )
        assert prov == {"workspace": "base", "shell": "expert", "canvas": "request"}

    def test_layers_without_tools_are_skipped(self):
        prov = layer_provenance([("base", {"llm": {}}), ("expert", None)])
        assert prov == {}

    def test_legacy_coding_alias_maps_to_shell(self):
        prov = layer_provenance([("expert", {"tools": {"coding": ["run_command"]}})])
        assert prov == {"shell": "expert"}


# =============================================================================
# Composition — the heart of D6
# =============================================================================
class TestMeasurementWins:
    def test_agent_answer_overrides_a_disagreeing_config(self):
        """The config says two app groups are empty; the agent bound one in each.

        This is the runtime-injection layer, and the whole reason D6 says ask
        the agent. A view that reported ``off`` here would be the original bug
        wearing a new endpoint.

        These two names are deliberately NOT ``grant: "code"``: they are
        re-appended by ``_load_tools_for_backend`` and unticking each category
        really does drop it. These stay ordinary settable rows, and
        ``decided_by`` is ``runtime`` —
        contrast ``TestOnIsRevocable``, where an all-code-granted bind reads as
        ``registry`` and cannot be unticked at all.
        """
        view = compose_tool_view(
            measured={
                "orchestrator": ["get_session_context"],
                "job_control": ["create_job"],
            },
            configured={"orchestrator": [], "job_control": []},
        )
        assert view["orchestrator"]["state"] == STATE_ON
        assert view["orchestrator"]["tools"] == ["get_session_context"]
        assert view["job_control"]["tools"] == ["create_job"]
        assert view["orchestrator"]["decided_by"] == DECIDED_BY_RUNTIME
        assert view["job_control"]["decided_by"] == DECIDED_BY_RUNTIME
        assert view["orchestrator"]["settable"] is True
        assert view["job_control"]["settable"] is True

    def test_agent_answer_overrides_a_config_that_promised_tools(self):
        """And the shortfall reads UNAVAILABLE, not off — see TestOffIsAPromise."""
        view = compose_tool_view(
            measured={},
            configured={"research": ["web_search"]},
        )
        assert view["research"]["state"] == STATE_UNAVAILABLE
        assert view["research"]["tools"] == []
        assert view["research"]["configured"] == ["web_search"]

    def test_measured_entries_carry_the_config_alongside_for_comparison(self):
        view = compose_tool_view(
            measured={"research": ["web_search"]},
            configured={"research": ["web_search", "web_fetch"]},
        )
        assert view["research"]["tools"] == ["web_search"]
        assert view["research"]["configured"] == ["web_search", "web_fetch"]

    def test_prediction_has_no_configured_key(self):
        """No measurement means nothing to compare against — say so by shape."""
        view = compose_tool_view(measured=None, configured={"research": ["web_search"]})
        assert "configured" not in view["research"]
        assert view["research"]["tools"] == ["web_search"]


class TestOffIsAPromise:
    """``off`` means "ticking would work". It is only said when it can be kept.

    The live gate produced the counterexample: ``knowledge`` measured
    ``configured=10 kb_*, tools=0`` and rendered ``off, settable: true`` — a
    checkbox offering an "on" that writes config and changes no binding.
    """

    def test_config_granted_but_agent_bound_none_is_unavailable(self):
        view = compose_tool_view(
            measured={"knowledge": []},
            configured={"knowledge": ["kb_read", "kb_write"]},
        )
        entry = view["knowledge"]
        assert entry["state"] == STATE_UNAVAILABLE
        assert entry["settable"] is False
        assert "bound none" in entry["reason"]
        assert "2 tool(s)" in entry["reason"]

    def test_it_fires_without_any_legible_cause(self):
        """The point: no backend, no grant, no code mark — still honest.

        A degraded agent report carries no ``backend``, so the tier gate is
        invisible. The tools are measurably absent regardless, and that is
        enough to refuse the promise.
        """
        view = compose_tool_view(
            measured={"git": []},
            configured={"git": ["git_log", "git_status"]},
            backend_caps=None,
            grants=None,
        )
        assert view["git"]["state"] == STATE_UNAVAILABLE
        assert view["git"]["settable"] is False

    def test_a_legible_cause_still_wins_the_message(self):
        view = compose_tool_view(
            measured={"git": []},
            configured={"git": ["git_log"]},
            backend_caps={"supports_shell": False},
        )
        assert view["git"]["reason"].startswith("this workspace tier")

    def test_config_empty_and_agent_empty_stays_off(self):
        """Nothing asked, nothing refused — the control is genuinely offerable."""
        view = compose_tool_view(measured={"citation": []}, configured={"citation": []})
        assert view["citation"]["state"] == STATE_OFF
        assert view["citation"]["settable"] is True
        assert view["citation"]["reason"] is None

    def test_a_prediction_never_claims_the_shortfall(self):
        """No measurement means no evidence the binding failed."""
        view = compose_tool_view(measured=None, configured={"knowledge": ["kb_read"]})
        assert view["knowledge"]["state"] == STATE_ON

    def test_partial_bind_is_on_not_unavailable(self):
        view = compose_tool_view(
            measured={"research": ["web_search"]},
            configured={"research": ["web_search", "crawl_website"]},
        )
        assert view["research"]["state"] == STATE_ON


class TestOnIsRevocable:
    """The mirror of "off is a promise": a TICKED box promises unticking works.

    ``code_granted_categories`` only knows the eight *category*-level grants, so
    the per-TOOL tier was invisible here — and the agent re-appends exactly that
    tier after the merge (``persistent_session.py``: ``srw_cloud_status`` on a
    cloud mount, ``sleep``/``notify_user`` for an officer,
    ``request_workspace_upgrade`` on a lite tier,
    ``checkout_project_repository`` with fleet management). Unticking any of them
    writes ``tools.<c>: []``, the runtime re-appends, the next read says ``on``:
    a control that cannot lose, forever, with no explanation.

    The composed row this used to produce, verbatim from a session with a cloud
    mount — the DEFAULT project topology::

        {'state': 'on', 'settable': True, 'reason': None,
         'decided_by': 'runtime', 'tools': ['srw_cloud_status'],
         'configured': []}
    """

    def test_a_runtime_held_category_is_locked_on_not_a_live_checkbox(self):
        view = compose_tool_view(
            measured={"shell": ["srw_cloud_status"]},
            configured={"shell": []},
            grants={"shell_tools": True},
        )
        entry = view["shell"]
        assert entry["state"] == STATE_ON, "the agent holds a tool here"
        assert entry["settable"] is False
        assert entry["reason"] is not None
        # The reason names the tool AND its gate, so the user learns why.
        assert "srw_cloud_status" in entry["reason"]
        assert "cloud_mount_manager.active" in entry["reason"]
        assert entry["decided_by"] == DECIDED_BY_REGISTRY

    def test_the_officer_floor_is_locked_too(self):
        view = compose_tool_view(
            measured={"core": ["sleep", "notify_user"]}, configured={"core": []}
        )
        entry = view["core"]
        assert (entry["state"], entry["settable"]) == (STATE_ON, False)
        assert "sleep" in entry["reason"] and "notify_user" in entry["reason"]
        assert "officer.enabled" in entry["reason"]

    def test_the_lite_tier_upgrade_tool_is_locked_too(self):
        view = compose_tool_view(
            measured={"core": ["request_workspace_upgrade"]}, configured={"core": []}
        )
        assert view["core"]["settable"] is False

    def test_a_ticked_core_that_bound_nothing_it_asked_for_is_still_honest(self):
        """The sibling of the `core` fast-follow: `core: true` expands to exactly
        the six phase tools a session strips, so config asks for six and holds
        two runtime ones. Unticking still cannot release those two."""
        view = compose_tool_view(
            measured={"core": ["sleep", "notify_user"]},
            configured={"core": ["todo_list", "todo_complete", "mark_complete"]},
        )
        assert view["core"]["settable"] is False
        assert view["core"]["state"] == STATE_ON

    def test_a_category_config_can_still_shrink_stays_settable(self):
        """The guard fires only when unticking would change NOTHING. Here it
        would drop `run_command`, which is a real effect, so the control is
        honest as an ordinary ticked box."""
        view = compose_tool_view(
            measured={"shell": ["run_command", "srw_cloud_status"]},
            configured={"shell": ["run_command"]},
            grants={"shell_tools": True},
        )
        entry = view["shell"]
        assert (entry["state"], entry["settable"], entry["reason"]) == (
            STATE_ON,
            True,
            None,
        )

    def test_an_ordinary_config_granted_category_is_untouched(self):
        view = compose_tool_view(
            measured={"research": ["web_search"]},
            configured={"research": ["web_search"]},
        )
        assert view["research"]["settable"] is True
        assert view["research"]["reason"] is None

    def test_a_prediction_never_locks(self):
        """No measurement means no bound set to reason about — and the creation
        form is where turning shell ON still has to work."""
        view = compose_tool_view(measured=None, configured={"shell": []})
        assert view["shell"]["settable"] is True
        assert view["shell"]["state"] == STATE_OFF

    def test_a_grant_denial_still_wins_the_message(self):
        """Precedence unchanged: the strongest statement is the one shown."""
        view = compose_tool_view(
            measured={"shell": ["srw_cloud_status"]},
            configured={"shell": []},
            grants={"shell_tools": False},
        )
        assert view["shell"]["decided_by"] == DECIDED_BY_GRANT
        assert "shell_tools" in view["shell"]["reason"]
        assert view["shell"]["settable"] is False

    def test_an_mcp_or_unclassified_bind_is_not_mistaken_for_a_code_grant(self):
        """Names the reader's registry does not know are not code-granted; they
        are unknown, and inventing a lock for them would be a guess."""
        view = compose_tool_view(
            measured={"mcp": ["some_mcp_tool"], UNCLASSIFIED_CATEGORY: ["whatever"]},
            configured={"mcp": []},
        )
        assert view["mcp"]["settable"] is True
        assert view[UNCLASSIFIED_CATEGORY]["settable"] is True

    def test_the_per_tool_tier_is_read_off_the_registry_not_a_literal(self):
        from agent.tools.registry import TOOL_REGISTRY

        expected = {n for n, m in TOOL_REGISTRY.items() if m.get("grant") == "code"}
        assert set(code_granted_tools()) == expected
        # The four the category-level map cannot see, and the whole point of it.
        assert {
            "srw_cloud_status",
            "sleep",
            "notify_user",
            "request_workspace_upgrade",
            "checkout_project_repository",
        } <= set(code_granted_tools())
        assert all(gate for gate in code_granted_tools().values())

    @pytest.mark.parametrize(
        ("category", "names"),
        [
            ("shell", ["srw_cloud_status"]),
            ("core", ["sleep", "notify_user"]),
            ("core", ["request_workspace_upgrade"]),
            ("orchestrator", ["checkout_project_repository"]),
        ],
    )
    def test_every_post_merge_re_append_site_is_covered(self, category, names):
        """One case per injection site in `_load_tools_for_backend`, so a new
        re-append with a `grant: "code"` mark is covered by construction and one
        WITHOUT the mark shows up as a missing lock here."""
        entry = compose_tool_view(
            measured={category: names}, configured={category: []}
        )[category]
        assert entry["settable"] is False
        assert entry["reason"]

    def test_unsettable_never_means_off(self):
        """The invariant Task 8's client depends on: it renders `state` verbatim
        for a locked row, so an unsettable row must be `on` or `unavailable` —
        never `off`, which would draw an unticked box nobody can tick."""
        for measured, configured in (
            ({"shell": ["srw_cloud_status"]}, {"shell": []}),
            ({"core": ["sleep"]}, {"core": []}),
            ({"sql": []}, {"sql": []}),
            ({"knowledge": []}, {"knowledge": ["kb_read"]}),
        ):
            for entry in compose_tool_view(
                measured=measured, configured=configured
            ).values():
                if entry["settable"] is False:
                    assert entry["state"] != STATE_OFF
                    assert entry["reason"]


class TestThreeStates:
    def test_on_off_and_unavailable_are_all_reachable(self):
        view = compose_tool_view(
            measured={"research": ["web_search"]},
            configured={"research": ["web_search"], "citation": []},
            grants={"shell_tools": False},
        )
        assert view["research"]["state"] == STATE_ON
        assert view["citation"]["state"] == STATE_OFF
        assert view["shell"]["state"] == STATE_UNAVAILABLE

    def test_unavailable_always_carries_a_reason(self):
        view = compose_tool_view(
            measured={},
            configured={},
            grants={"shell_tools": False},
            backend_caps={"supports_shell": False},
        )
        for name, entry in view.items():
            if entry["state"] == STATE_UNAVAILABLE:
                assert entry["reason"], f"{name} unavailable with no reason"

    def test_off_never_carries_a_reason(self):
        view = compose_tool_view(measured={}, configured={"citation": []})
        assert view["citation"]["state"] == STATE_OFF
        assert view["citation"]["reason"] is None
        assert view["citation"]["settable"] is True

    def test_grant_denial_beats_a_plain_off(self):
        """D4/1.4: a user without shell_tools must see a REASON, not a blank box."""
        view = compose_tool_view(
            measured={}, configured={"shell": []}, grants={"shell_tools": False}
        )
        assert view["shell"]["state"] == STATE_UNAVAILABLE
        assert view["shell"]["settable"] is False
        assert "shell_tools" in view["shell"]["reason"]
        assert view["shell"]["decided_by"] == DECIDED_BY_GRANT

    def test_backend_denial_reads_as_the_tier_not_the_config(self):
        view = compose_tool_view(
            measured={},
            configured={"shell": ["run_command"]},
            grants={"shell_tools": True},
            backend_caps={"supports_shell": False, "supports_file_tools": True},
        )
        assert view["shell"]["state"] == STATE_UNAVAILABLE
        assert view["shell"]["decided_by"] == DECIDED_BY_BACKEND
        assert "workspace tier" in view["shell"]["reason"]

    def test_grant_denial_outranks_the_backend(self):
        view = compose_tool_view(
            measured={},
            configured={},
            grants={"shell_tools": False},
            backend_caps={"supports_shell": False},
        )
        assert view["shell"]["decided_by"] == DECIDED_BY_GRANT
        assert "shell_tools" in view["shell"]["reason"]

    def test_code_granted_category_is_unavailable_when_empty(self):
        view = compose_tool_view(measured={}, configured={"sql": []})
        assert view["sql"]["state"] == STATE_UNAVAILABLE
        assert view["sql"]["settable"] is False
        assert "not by config" in view["sql"]["reason"]
        assert view["sql"]["decided_by"] == DECIDED_BY_REGISTRY

    def test_code_granted_category_can_still_be_on(self):
        """A datasource attach turns `sql` on without config saying anything."""
        view = compose_tool_view(
            measured={"sql": ["sql_query"]}, configured={"sql": []}
        )
        assert view["sql"]["state"] == STATE_ON
        assert view["sql"]["settable"] is False
        assert view["sql"]["reason"]


class TestProvenanceInTheView:
    def test_falls_through_to_the_authored_layer_when_nothing_intervened(self):
        view = compose_tool_view(
            measured={"research": ["web_search"]},
            configured={"research": ["web_search"]},
            provenance={"research": "expert"},
        )
        assert view["research"]["decided_by"] == "expert"

    def test_unset_when_no_layer_named_it(self):
        view = compose_tool_view(measured={}, configured={})
        assert view["evaluation"]["decided_by"] == DECIDED_BY_UNSET

    def test_runtime_wins_over_the_authored_layer_when_they_disagree(self):
        view = compose_tool_view(
            measured={"research": ["web_search", "extra_injected"]},
            configured={"research": ["web_search"]},
            provenance={"research": "expert"},
        )
        assert view["research"]["decided_by"] == DECIDED_BY_RUNTIME


class TestToolGroupsProjection:
    def test_derives_the_closed_group_booleans_from_the_view(self):
        view = compose_tool_view(
            measured={"job_control": ["create_job"], "canvas": []},
            configured={},
        )
        groups = tool_groups_from_view(view)
        assert set(groups) == set(SESSION_TOOL_OVERRIDE_NAMES)
        assert groups["job_control"] is True
        assert groups["orchestrator"] is False
        assert groups["canvas"] is False

    def test_a_group_absent_from_the_view_reads_false(self):
        assert tool_groups_from_view({}) == {
            g: False for g in SESSION_TOOL_OVERRIDE_NAMES
        }


class TestCategoriesOutsideTheVocabulary:
    def test_an_unclassified_measured_bucket_is_still_reported(self):
        view = compose_tool_view(
            measured={UNCLASSIFIED_CATEGORY: ["some_mcp_tool"]}, configured={}
        )
        assert view[UNCLASSIFIED_CATEGORY]["state"] == STATE_ON
        assert view[UNCLASSIFIED_CATEGORY]["tools"] == ["some_mcp_tool"]

    def test_every_known_category_appears_even_with_no_data(self):
        view = compose_tool_view(measured={}, configured={})
        assert set(report_categories()) <= set(view)


class TestOriginVocabulary:
    def test_both_agent_origins_count_as_measured(self):
        assert MEASURED_ORIGINS == {ORIGIN_AGENT, ORIGIN_AGENT_PARTIAL}

    def test_prediction_is_not_measured(self):
        assert ORIGIN_PREDICTION not in MEASURED_ORIGINS

    def test_the_three_origins_are_distinct(self):
        assert len({ORIGIN_AGENT, ORIGIN_AGENT_PARTIAL, ORIGIN_PREDICTION}) == 3


@pytest.mark.parametrize("measured", [None, {"shell": ["run_command"]}])
def test_selected_backend_filters_predictions_but_never_agent_measurements(measured):
    entry = compose_tool_view(
        measured=measured,
        configured={"shell": ["run_command"]},
        backend_caps={"supports_shell": False},
    )["shell"]
    assert entry["state"] == (STATE_UNAVAILABLE if measured is None else STATE_ON)
    assert entry["tools"] == ([] if measured is None else ["run_command"])
