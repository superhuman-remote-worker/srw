"""One tool vocabulary at every write boundary.

Two defects, one root cause: ``tools.<category>`` was validated by a
hand-curated name list covering four of the registry's twenty-four categories,
and only on the session surface.

**Defect 2 — the fail-open half.** The New Session form renders twelve tool
categories and writes ``tools[cat] = []`` for every one the user unticks
(``cockpit/.../tools-group.component.ts``). The create boundary copied across
only the four allowlisted groups and the runtime-update boundary replaced
``tools`` wholesale with the accepted subset, so unticking ``research``,
``browser_direct``, ``citation``, ``shell``, ``communication``, ``delegation``,
``knowledge`` or ``git`` was **accepted and discarded**. The user was shown a
restriction that was never applied.

**Defect 8 — the security half.** ``POST /api/jobs`` had no tool allowlist at
all: it stripped four lifecycle markers and accepted
``tools.<anything>: [<any registered name>]``. ``load_tools`` resolves a name
against the global registry rather than the key it arrived under, so
``tools.canvas: ["run_command"]`` binds a shell tool — and the PDP reads
``_truthy(tools.get("shell"))``, a category key, so it never sees it.

The fix is one generic validator
(``src.core.tool_policy.validate_tool_override_fragment``) at every boundary
that accepts a caller-supplied ``tools`` fragment. The rule: the category must
be real and every name in it must belong to that category *per the registry*.
Anything the boundary will not honour is **rejected with a 400** — never
silently dropped, which would only replace one silent drop with another.

Seven boundaries, and the coverage below is deliberately split between the
validator and each **call site**, because the defect was a filter at the call
site and a validator-level test cannot see one being re-narrowed:

===============================  ==========================================
Boundary                         Call-site class
===============================  ==========================================
``POST /persistent/threads``     ``TestSessionCreateBoundary``
``PATCH .../config`` (runtime)   ``TestSessionRuntimeUpdateBoundary``
``POST /api/jobs``               ``TestJobCreateBoundary``
``POST /sessions/{id}/prepare``  ``TestPrepareBoundary``
``POST``/``PATCH /automations``  ``TestAutomationBoundary``
``POST``/``PATCH /projects``     ``TestProjectDefaultOverrideBoundary``
``/api/experts`` (five writes)   ``TestExpertWriteBoundary``
===============================  ==========================================

What this validator is NOT: an authorization gate. Capability grants
(``src/core/capability_grants.py``) remain the single PDP, and
``TestTheGateIsNotThePDP`` pins that separation.

Design: knowledge-base/knowledge/features/tool_config_policy_vs_membership.md.
"""

from __future__ import annotations

from shared.runtime.core.srw_manifest_config import (
    srw_config_fragment as _srw_config_fragment,
)

from tests._expert_catalog import catalogue_route
from orchestrator.services import expert_authoring as expert_authoring_module
from orchestrator.routers import expert_catalog as expert_routes
from orchestrator.schemas import expert_catalog as expert_schemas


import uuid
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services import thread_admission  # noqa: E402

# R1.B06: these handlers moved to services/thread_config_update with their
# routes in routers/thread_config. main's dependency factory still reads
# main's attributes at call time, so the patches below keep steering what
# they steered before.
from orchestrator.services import thread_config_update  # noqa: E402

from shared.runtime.core.session_tool_overrides import SESSION_TOOL_OVERRIDE_NAMES
from shared.runtime.core.tool_policy import (
    MCP_WILDCARD,
    ToolPolicyError,
    validate_tool_override_fragment,
)
from agent.tools.registry import TOOL_REGISTRY, get_tools_by_category
from orchestrator.services import session_config_resolution  # noqa: E402

#: The categories the cockpit's New Session form renders as checkboxes, in
#: order. Mirror of SESSION_TOOL_CATEGORIES in
#: cockpit/src/app/views/agent-settings/agent-settings.types.ts — the payloads
#: the SHIPPED cockpit sends are the compatibility surface for this change.
COCKPIT_SESSION_CATEGORIES = (
    "research",
    "browser_direct",
    "citation",
    "shell",
    "communication",
    "delegation",
    "canvas",
    "orchestrator",
    "agent_catalog",
    "workflows",
    "knowledge",
    "git",
)

#: The four the boundary used to honour. The other eight were the defect.
PREVIOUSLY_HONOURED = frozenset(SESSION_TOOL_OVERRIDE_NAMES)
PREVIOUSLY_DISCARDED = tuple(
    c for c in COCKPIT_SESSION_CATEGORIES if c not in PREVIOUSLY_HONOURED
)


# =============================================================================
# Defect 2 — the eight discarded categories
# =============================================================================


class TestEveryRenderedCategoryIsHonoured:
    def test_the_defect_covered_eight_of_the_twelve(self):
        """Guard the premise: if the form's category list moves, this test
        tells you before the coverage below quietly narrows."""
        assert len(COCKPIT_SESSION_CATEGORIES) == 12
        assert len(PREVIOUSLY_DISCARDED) == 8
        assert set(PREVIOUSLY_DISCARDED) == {
            "research",
            "browser_direct",
            "citation",
            "shell",
            "communication",
            "delegation",
            "knowledge",
            "git",
        }

    @pytest.mark.parametrize("category", PREVIOUSLY_DISCARDED)
    def test_unticking_a_previously_discarded_category_is_honoured(self, category):
        """THE motivating defect: the payload the cockpit sends for an
        unticked box now survives the boundary."""
        assert validate_tool_override_fragment({"tools": {category: []}}) == {
            category: []
        }

    @pytest.mark.parametrize("category", COCKPIT_SESSION_CATEGORIES)
    def test_every_rendered_category_round_trips(self, category):
        """Whole-form deselect: all twelve, all honoured, none dropped."""
        fragment = {"tools": {c: [] for c in COCKPIT_SESSION_CATEGORIES}}
        assert validate_tool_override_fragment(fragment)[category] == []

    def test_re_enable_payloads_from_the_shipped_bases_validate(self):
        """The other half of what the shipped cockpit sends.

        Re-ticking a category the expert disabled posts
        ``tools[cat] = [...defaults_tools[cat]]`` — the mode base's own list,
        served by ``GET /api/experts/{id}``. Honouring those is only safe if
        they are in-category, so assert it against the registry rather than
        trusting it.
        """
        from shared.runtime.core.loader import (
            load_and_merge_config,
            resolve_config_path,
        )

        for base in ("session_base", "worker_base"):
            path, _ = resolve_config_path(base)
            tools = (load_and_merge_config(path) or {}).get("tools") or {}
            populated = {k: v for k, v in tools.items() if v}
            assert populated, f"{base} declares no non-empty tool list"
            assert validate_tool_override_fragment({"tools": populated}) == populated


# =============================================================================
# Defect 8 — the smuggle, and it is generic now
# =============================================================================


class TestCrossCategorySmuggling:
    @pytest.mark.parametrize(
        ("category", "smuggled"),
        [
            ("canvas", "run_command"),
            ("canvas", "shell_execute"),
            ("agent_catalog", "create_job"),
            ("workflows", "get_skill"),
            ("orchestrator", "run_command"),
            # The eight that were not validated at all before.
            ("citation", "run_command"),
            ("knowledge", "browser_navigate"),
            ("git", "kb_write"),
            ("research", "delegate_agent"),
            # And the ones no surface ever checked.
            ("workspace", "run_command"),
            ("core", "web_search"),
            ("loop", "approve_job"),
        ],
    )
    def test_a_foreign_name_is_rejected(self, category, smuggled):
        with pytest.raises(ToolPolicyError) as exc:
            validate_tool_override_fragment({"tools": {category: [smuggled]}})
        message = str(exc.value)
        assert f"tools.{category}" in message
        assert smuggled in message
        # The message names where the tool actually lives, so the caller can
        # fix the fragment instead of guessing.
        assert f"tools.{TOOL_REGISTRY[smuggled]['category']}" in message

    def test_a_smuggle_hidden_in_a_policy_mapping_is_rejected(self):
        """``{only: [...]}`` is the same list in a different coat."""
        with pytest.raises(ToolPolicyError, match="run_command"):
            validate_tool_override_fragment(
                {"tools": {"canvas": {"only": ["run_command"]}}}
            )

    def test_an_unregistered_name_is_rejected_rather_than_swallowed(self):
        """``load_tools`` falls back to per-name loading and *logs* an unknown
        name away (src/agent.py). A typo that silently binds nothing is the
        same class of lie as an ignored restriction — say it here."""
        with pytest.raises(ToolPolicyError) as exc:
            validate_tool_override_fragment({"tools": {"git": ["git_lug"]}})
        assert "not a registered tool" in str(exc.value)


class TestMcpHasNoStaticMembership:
    """``register_mcp_tools`` populates the category per session and is never
    called in the orchestrator process, so membership cannot be checked
    positively. The rule inverts: a name the registry knows under a different
    category is the smuggle; an unrecognised one is a presumed runtime tool."""

    def test_a_registered_foreign_name_is_still_rejected(self):
        with pytest.raises(ToolPolicyError, match="run_command"):
            validate_tool_override_fragment({"tools": {"mcp": ["run_command"]}})

    def test_an_unrecognised_name_passes_as_a_runtime_discovered_tool(self):
        assert validate_tool_override_fragment(
            {"tools": {"mcp": ["notion__search_pages"]}}
        ) == {"mcp": ["notion__search_pages"]}

    def test_true_resolves_to_the_wildcard_sentinel(self):
        assert validate_tool_override_fragment({"tools": {"mcp": True}}) == {
            "mcp": [MCP_WILDCARD]
        }


# =============================================================================
# Rejection, not silence — the shape of every refusal
# =============================================================================


class TestRejectDoNotDrop:
    def test_an_unknown_category_is_rejected_not_ignored(self):
        """``normalize_tool_policy`` passes an unknown key through verbatim
        because a config file's typo is caught by schema.json at authoring
        time. A request has no schema pass, so this is where it is caught."""
        with pytest.raises(ToolPolicyError) as exc:
            validate_tool_override_fragment({"tools": {"workspaces": []}})
        assert "unknown tool category" in str(exc.value)
        assert "workspace" in str(exc.value)  # the known-categories listing

    def test_a_non_object_tools_value_is_rejected(self):
        with pytest.raises(ToolPolicyError, match="keyed by tool category"):
            validate_tool_override_fragment({"tools": ["research"]})

    def test_a_non_string_category_key_is_rejected(self):
        with pytest.raises(ToolPolicyError, match="must be strings"):
            validate_tool_override_fragment({"tools": {3: []}})

    def test_a_non_list_category_value_is_rejected(self):
        with pytest.raises(ToolPolicyError, match="tools.agent_catalog"):
            validate_tool_override_fragment({"tools": {"agent_catalog": "disabled"}})

    def test_a_non_string_tool_name_is_rejected(self):
        with pytest.raises(ToolPolicyError, match="tool-name strings"):
            validate_tool_override_fragment({"tools": {"git": ["git_log", 7]}})

    def test_an_absent_tools_key_is_not_an_error(self):
        assert validate_tool_override_fragment({"llm": {"model": "x"}}) == {}
        assert validate_tool_override_fragment({}) == {}
        assert validate_tool_override_fragment(None) == {}

    def test_an_empty_tools_object_asks_for_nothing(self):
        assert validate_tool_override_fragment({"tools": {}}) == {}

    def test_the_coding_alias_normalises_to_shell(self):
        assert validate_tool_override_fragment(
            {"tools": {"coding": ["run_command"]}}
        ) == {"shell": ["run_command"]}

    def test_an_alias_collision_is_rejected_rather_than_resolved(self):
        """Two values for one category: picking one is the silent drop."""
        with pytest.raises(ToolPolicyError, match="both address the shell category"):
            validate_tool_override_fragment(
                {"tools": {"coding": ["run_command"], "shell": []}}
            )


# =============================================================================
# Constraints inherited from tasks 4 and 5
# =============================================================================


class TestInheritedConstraints:
    def test_only_is_returned_as_written_never_intersected(self):
        """``config/experts/centurion`` names two ``grant: "explicit"`` tools;
        intersecting with ``true``'s expansion would silently revoke them."""
        assert validate_tool_override_fragment(
            {
                "tools": {
                    "job_control": {"only": ["steer_job"]},
                    "job_inspection": {"only": ["get_stuck_jobs"]},
                }
            }
        ) == {
            "job_control": ["steer_job"],
            "job_inspection": ["get_stuck_jobs"],
        }

    @pytest.mark.parametrize(
        "name",
        sorted(
            n
            for n, m in TOOL_REGISTRY.items()
            if m.get("grant") in ("code", "explicit")
        ),
    )
    def test_a_classified_tool_stays_nameable_by_name(self, name):
        """``grant`` restricts category-level POLICY, not an explicit name.
        Confusing "not in ``true``'s expansion" with "not allowed" would make
        this validator a second, stricter grant system."""
        category = TOOL_REGISTRY[name]["category"]
        assert validate_tool_override_fragment({"tools": {category: [name]}}) == {
            category: [name]
        }

    @pytest.mark.parametrize("group", sorted(SESSION_TOOL_OVERRIDE_NAMES))
    def test_a_closed_group_true_still_expands_to_the_curated_vocabulary(self, group):
        """The registry's ``explicit`` tier is what holds this, not a name
        list — so a user ticking "Experts & Skills" cannot acquire
        ``set_expert_bundle`` through a category-level ``true``.

        ``orchestrator`` carries a legacy-compat exception: its boolean
        predates the 0156 job-group split (0156 skipped boolean rows
        whole-row), so ``true`` expands to the pre-split union — the curated
        orchestrator set plus the curated job groups, never the explicit-tier
        job tools (delete/assign/promote/steer/...)."""
        expected = set(SESSION_TOOL_OVERRIDE_NAMES[group])
        if group == "orchestrator":
            expected |= set(SESSION_TOOL_OVERRIDE_NAMES["job_control"])
            expected |= set(SESSION_TOOL_OVERRIDE_NAMES["job_inspection"])
        assert validate_tool_override_fragment({"tools": {group: True}})[
            group
        ] == sorted(expected)

    def test_shell_accepts_only_and_false_and_nothing_else(self):
        assert validate_tool_override_fragment({"tools": {"shell": []}}) == {
            "shell": []
        }
        assert validate_tool_override_fragment({"tools": {"shell": False}}) == {
            "shell": []
        }
        assert validate_tool_override_fragment(
            {"tools": {"shell": {"only": ["run_command", "shell_read"]}}}
        ) == {"shell": ["run_command", "shell_read"]}
        for refused in (True, {"except": ["shell_read"]}, {"except": []}):
            with pytest.raises(ToolPolicyError, match="must enumerate"):
                validate_tool_override_fragment({"tools": {"shell": refused}})

    def test_bughunters_bare_shell_list_is_legal(self):
        names = ["run_command", "shell_execute", "shell_read", "cancel_command"]
        assert validate_tool_override_fragment({"tools": {"shell": names}}) == {
            "shell": names
        }


class TestOutputIsCanonical:
    """The PDP reads ``_truthy(tools.get("shell"))``. Handing it a raw policy
    value gets two rows wrong (``{}`` reads false, ``{only: []}`` reads true),
    which is why the boundary normalises before the grant check runs."""

    @pytest.mark.parametrize(
        "value",
        [True, False, [], ["git_log"], {"only": ["git_log"]}, {"except": ["git_log"]}],
    )
    def test_every_accepted_form_leaves_as_a_list_of_strings(self, value):
        result = validate_tool_override_fragment({"tools": {"git": value}})["git"]
        assert isinstance(result, list)
        assert all(isinstance(n, str) for n in result)

    def test_the_two_truthy_hazards_never_reach_the_pdp(self):
        for hazard in ({}, {"only": []}):
            with pytest.raises(ToolPolicyError):
                validate_tool_override_fragment({"tools": {"git": hazard}})


class TestTheGateIsNotThePDP:
    """A shape-and-vocabulary gate accepts things the PDP will refuse. If this
    validator started refusing them too, it would be a second authorization
    system — the root cause the whole series is removing."""

    def test_a_grant_gated_category_validates_fine_here(self):
        assert validate_tool_override_fragment(
            {"tools": {"shell": ["run_command"], "browser_direct": ["browser_click"]}}
        ) == {"shell": ["run_command"], "browser_direct": ["browser_click"]}

    def test_the_pdp_still_sees_what_this_lets_through(self):
        from shared.runtime.core.capability_grants import evaluate

        fragment = {
            "tools": validate_tool_override_fragment(
                {"tools": {"shell": ["run_command"]}}
            )
        }
        assert evaluate(fragment, {"shell_tools": False}) == [
            "shell_tools: tools.shell requires the shell_tools grant"
        ]


# =============================================================================
# The orchestrator wrappers
# =============================================================================


class TestOrchestratorWrapper:
    def test_a_rejection_becomes_a_400(self):
        import orchestrator.main as orch_main
        from orchestrator.services import session_tool_policy

        with pytest.raises(orch_main.HTTPException) as exc:
            session_tool_policy.validated_tool_overrides(
                {"tools": {"canvas": ["run_command"]}}
            )
        assert exc.value.status_code == 400
        assert "run_command" in exc.value.detail

    def test_the_fragment_helper_replaces_tools_without_mutating_the_input(self):
        import orchestrator.main as orch_main

        original = {"tools": {"git": True}, "autonomy": "full"}
        result = orch_main._with_validated_tool_overrides(original)

        assert result["tools"] == sorted_git_expansion()
        assert result["autonomy"] == "full"
        assert original["tools"] == {"git": True}, "caller-owned fragment mutated"

    def test_a_fragment_without_tools_is_returned_untouched(self):
        import orchestrator.main as orch_main

        fragment = {"llm": {"model": "x"}}
        assert orch_main._with_validated_tool_overrides(fragment) is fragment
        assert orch_main._with_validated_tool_overrides(None) is None


def sorted_git_expansion() -> dict[str, list[str]]:
    return {
        "git": sorted(
            n for n in get_tools_by_category("git") if "grant" not in TOOL_REGISTRY[n]
        )
    }


# =============================================================================
# The job boundary — Defect 8 end to end
# =============================================================================

USER_ID = str(uuid.uuid4())
JOB_ID = str(uuid.uuid4())


@pytest.fixture
def job_request():
    """A cockpit/public caller. ``_INTERNAL_KEY`` is patched to something else
    in ``_create_job``, so this header is not a key — the internal-path tests
    set it explicitly."""
    return SimpleNamespace(headers={}, query_params={})


@pytest.fixture
def job_db():
    db = MagicMock()
    db.get_user = AsyncMock(return_value={"id": USER_ID, "is_admin": False})
    db.get_project = AsyncMock(return_value=None)
    db.get_job = AsyncMock(return_value=None)
    db.get_thread = AsyncMock(return_value=None)
    db.get_datasource = AsyncMock(return_value=None)
    db.link_datasource_to_job = AsyncMock()
    db.create_job = AsyncMock(return_value={"id": JOB_ID, "status": "created"})
    return db


async def _create_job(db, request, body):
    import orchestrator.security.access as access_module
    from tests._b09_control_seams import create_job

    user = {"id": USER_ID, "is_admin": False}
    patches = [
        patch.object(access_module, "_INTERNAL_KEY", "secret"),
        patch("orchestrator.main.require_approved_user", AsyncMock(return_value=user)),
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        ),
        patch("orchestrator.main.postgres_db", db),
        patch(
            "orchestrator.main._enforce_readiness_gate", AsyncMock(return_value=None)
        ),
        patch("orchestrator.main._thread_project_ids", AsyncMock(return_value=[])),
        patch(
            "orchestrator.main._revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.main._require_job_project_access",
            AsyncMock(return_value=None),
        ),
        patch(
            "orchestrator.main._is_experts_db_enabled", MagicMock(return_value=False)
        ),
        patch(
            "orchestrator.main._inherit_parent_datasource_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.main._enforce_job_create_grants", AsyncMock(return_value=None)
        ),
        patch("orchestrator.services.job_provisioning.provision_job_repo", AsyncMock()),
        patch(
            "orchestrator.main.subjob_completion_operations.spawn_scholar_subjob",
            AsyncMock(return_value=None),
        ),
        patch("orchestrator.main._trigger_dispatch", MagicMock()),
    ]
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        return await create_job(request, body)


class TestJobCreateBoundary:
    @pytest.mark.asyncio
    async def test_the_classic_smuggle_is_a_400_on_the_job_surface(
        self, job_db, job_request
    ):
        """Was accepted outright: four lifecycle markers stripped, then
        ``tools.<anything>: [<any registered name>]`` straight through, with
        only a dispatch PDP that keys off the CATEGORY behind it."""
        from fastapi import HTTPException
        from orchestrator.main import JobCreate

        with pytest.raises(HTTPException) as exc:
            await _create_job(
                job_db,
                job_request,
                JobCreate(
                    description="d",
                    config_override={"tools": {"canvas": ["run_command"]}},
                ),
            )
        assert exc.value.status_code == 400
        assert "run_command" in exc.value.detail
        job_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_internal_path_is_validated_too(self, job_db, job_request):
        """``X-Internal-Key`` is transport authentication, not authorization,
        and ``create_job`` forwards a MODEL-AUTHORED config_override
        verbatim — which is precisely a caller that can write this fragment."""
        from fastapi import HTTPException
        from orchestrator.main import JobCreate

        parent = str(uuid.uuid4())
        job_db.get_job = AsyncMock(
            return_value={"id": parent, "user_id": USER_ID, "project_id": None}
        )
        internal_request = SimpleNamespace(
            headers={"X-Internal-Key": "secret"}, query_params={}
        )
        with pytest.raises(HTTPException) as exc:
            await _create_job(
                job_db,
                internal_request,
                JobCreate(
                    description="d",
                    parent_job_id=parent,
                    config_override={"tools": {"citation": ["shell_execute"]}},
                ),
            )
        assert exc.value.status_code == 400
        assert "shell_execute" in exc.value.detail
        job_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_legitimate_fragment_is_persisted_normalised(
        self, job_db, job_request
    ):
        from orchestrator.main import JobCreate

        await _create_job(
            job_db,
            job_request,
            JobCreate(
                description="d",
                config_override={"tools": {"evaluation": True, "communication": []}},
            ),
        )
        persisted = job_db.create_job.await_args.kwargs["config_override"]
        assert persisted["tools"]["communication"] == []
        assert persisted["tools"]["evaluation"] == [
            "approve_job_verdict",
            "return_job_with_feedback",
        ]

    @pytest.mark.asyncio
    async def test_the_agents_verification_job_fragment_still_validates(
        self, job_db, job_request
    ):
        """``src/api/orchestrator_client.py`` POSTs this exact fragment for
        every self-verifying job — the one real internal caller with a
        ``tools`` block on this endpoint."""
        from orchestrator.main import JobCreate

        await _create_job(
            job_db,
            job_request,
            JobCreate(
                description="d",
                config_override={
                    "autonomy": "full",
                    "tools": {
                        "evaluation": [
                            "approve_job_verdict",
                            "return_job_with_feedback",
                        ]
                    },
                },
            ),
        )
        job_db.create_job.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_job_without_tools_is_untouched(self, job_db, job_request):
        from orchestrator.main import JobCreate

        await _create_job(
            job_db,
            job_request,
            JobCreate(description="d", config_override={"autonomy": "full"}),
        )
        persisted = job_db.create_job.await_args.kwargs["config_override"]
        assert persisted == {"autonomy": "full"}


# =============================================================================
# The runtime-update boundary
# =============================================================================


class TestLiveSessionSanitiser:
    def test_every_category_survives_the_agent_side_sanitiser(self):
        from agent.api.persistent_app import _sanitize_live_session_config_override

        result = _sanitize_live_session_config_override(
            {"tools": {"canvas": [], "research": [], "shell": ["run_command"]}}
        )
        assert result["tools"] == {
            "canvas": [],
            "research": [],
            "shell": ["run_command"],
        }

    def test_a_smuggle_raises_rather_than_being_filtered_out(self):
        from agent.api.persistent_app import _sanitize_live_session_config_override

        with pytest.raises(ValueError, match="run_command"):
            _sanitize_live_session_config_override(
                {"tools": {"canvas": ["run_command"]}}
            )

    def test_the_caller_owned_payload_is_not_mutated(self):
        from agent.api.persistent_app import _sanitize_live_session_config_override

        payload = {"tools": {"canvas": True}}
        _sanitize_live_session_config_override(payload)
        assert payload == {"tools": {"canvas": True}}


# =============================================================================
# The session call sites — I2
# =============================================================================
#
# Everything above this line tests the shared validator. That is not enough,
# and the gap was found by review: re-narrowing `create_thread` or
# `_apply_thread_config_update` back to the closed groups — Defect 2,
# verbatim — left the whole suite green, because both sites delegate to a
# helper the tests exercised directly. A filter at the call site is invisible
# to a validator-level test. These two classes drive the endpoints.

SESSION_THREAD_ID = "11111111-2222-3333-4444-555555555555"
SESSION_RUNTIME_GENERATION = "66666666-7777-4888-8999-aaaaaaaaaaaa"
SESSION_USER_ID = str(uuid.uuid4())
SESSION_DATASOURCE_ID = str(uuid.uuid4())


@pytest.fixture
def session_create_env(monkeypatch):
    """`POST /api/persistent/threads` with its collaborators stubbed.

    `resolve_config` is left REAL — it reads the shipped YAML, so the
    assertions below prove the fragment survives an actual resolution rather
    than a mock's idea of one.
    """
    import orchestrator.main as main

    conn = AsyncMock()
    conn.execute = AsyncMock(return_value="UPDATE 1")
    acquire_cm = AsyncMock()
    acquire_cm.__aenter__.return_value = conn
    acquire_cm.__aexit__.return_value = False

    db = SimpleNamespace(
        # Session create now asks the manifest store for a Project workspace
        # default before it authorizes anything. None means this project
        # authored none, so selection falls back to the account/role default.
        fetchrow=AsyncMock(return_value=None),
        get_user_settings=AsyncMock(return_value={}),
        create_thread=AsyncMock(return_value=SESSION_THREAD_ID),
        get_thread=AsyncMock(
            return_value={
                "id": SESSION_THREAD_ID,
                "status": "created",
                "runtime_generation": SESSION_RUNTIME_GENERATION,
                "runtime_retirement_token": None,
            }
        ),
        acquire=MagicMock(return_value=acquire_cm),
        merge_thread_workspace_context=AsyncMock(return_value=True),
        list_thread_mounts=AsyncMock(return_value=[]),
        replace_thread_mounts=AsyncMock(),
    )
    user = {"id": SESSION_USER_ID, "is_admin": False}

    monkeypatch.setattr(main, "postgres_db", db)
    monkeypatch.setattr(main, "require_approved_user", AsyncMock(return_value=user))
    monkeypatch.setattr(main, "_enforce_readiness_gate", AsyncMock())
    monkeypatch.setattr(
        main, "_authorize_thread_project_ids", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        session_config_resolution,
        "resolve_session_account_defaults",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(main, "_is_experts_db_enabled", MagicMock(return_value=False))
    monkeypatch.setattr(main, "_user_experts_enabled", AsyncMock(return_value=False))
    # Keep this endpoint-boundary fixture hermetic. The real create path also
    # initializes cloud/Git services and schedules workspace/agent provisioning;
    # those integrations have their own tests and can otherwise wait on local
    # developer services here.
    monkeypatch.setattr(
        main,
        "gitea_client",
        SimpleNamespace(is_initialized=False, is_configured=False),
    )
    monkeypatch.setattr(
        main.main_cloud_router,
        "for_owner",
        MagicMock(
            return_value=SimpleNamespace(
                is_initialized=False,
                is_configured=False,
            )
        ),
    )
    monkeypatch.setattr(
        main, "container_provisioner", SimpleNamespace(is_available=False)
    )
    monkeypatch.setattr(main, "docker_provisioner", SimpleNamespace(is_available=False))
    monkeypatch.setattr(main, "agent_provisioner", SimpleNamespace(is_available=False))
    monkeypatch.setattr(main, "STATELESS_SESSION_ENABLED", False)
    grants = AsyncMock()
    monkeypatch.setattr(main, "_enforce_session_create_grants", grants)
    return main, db, conn, grants


def _thread_create_body(main, config_override):
    return main.ThreadCreateRequest(title="t", config_override=config_override)


def _persisted_thread_override(db) -> dict:
    """The `tools` block as it lands in threads.metadata — the durable
    artifact, and the thing the user's untick has to survive into."""
    return db.create_thread.await_args.kwargs["initial_metadata"]["config_override"]


class TestSessionCreateBoundary:
    @pytest.mark.asyncio
    async def test_create_skips_optional_cloud_without_active_instance_authority(
        self, session_create_env, monkeypatch
    ):
        main, _, _, _ = session_create_env
        for_owner = MagicMock(
            side_effect=AssertionError(
                "an unavailable optional cloud must not be resolved"
            )
        )
        monkeypatch.setattr(
            main,
            "main_cloud_router",
            SimpleNamespace(active_instance_id=None, for_owner=for_owner),
        )

        result = await main.create_thread(
            main.ThreadCreateRequest(title="session without main cloud"),
            MagicMock(),
        )

        assert result == {"thread_id": SESSION_THREAD_ID, "status": "created"}
        for_owner.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "officer",
        [
            {"enabled": True, "auto_pull": False},
            {"enabled": True, "worker_spend_ceiling_daily": 5.0},
            {
                "enabled": True,
                "slots": {"line": {"count": 1, "spend_ceiling_daily": 2.0}},
            },
        ],
    )
    async def test_generic_create_rejects_post_owned_authority_even_with_capability(
        self, session_create_env, officer
    ):
        main, db, _, grants = session_create_env

        with pytest.raises(main.HTTPException) as exc:
            await main.create_thread(
                main.ThreadCreateRequest(
                    title="hand-rolled officer",
                    project_id=SESSION_THREAD_ID,
                    config_override={"officer": officer},
                ),
                MagicMock(),
            )

        assert exc.value.status_code == 400
        db.create_thread.assert_not_awaited()
        grants.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_hand_rolled_officer_requires_owner_not_only_capability(
        self, session_create_env
    ):
        main, db, _, grants = session_create_env
        db.get_user_role_in_project = AsyncMock(return_value="editor")

        with pytest.raises(main.HTTPException) as exc:
            await main.create_thread(
                main.ThreadCreateRequest(
                    title="hand-rolled officer",
                    project_id=SESSION_THREAD_ID,
                    config_override={"officer": {"enabled": True}},
                ),
                MagicMock(),
            )

        assert exc.value.status_code == 403
        assert "Project owner" in exc.value.detail
        db.create_thread.assert_not_awaited()
        grants.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "release_enabled,expected_status", [(False, 409), (True, 400)]
    )
    async def test_effective_account_auto_pull_cannot_bypass_release_or_post_owner(
        self, session_create_env, monkeypatch, release_enabled, expected_status
    ):
        main, db, _, _ = session_create_env
        monkeypatch.setattr(main, "OFFICER_AUTO_PULL_RELEASE_ENABLED", release_enabled)
        monkeypatch.setattr(
            session_config_resolution,
            "resolve_session_account_defaults",
            AsyncMock(return_value={"officer": {"enabled": True, "auto_pull": True}}),
        )

        with pytest.raises(main.HTTPException) as exc:
            await main.create_thread(
                main.ThreadCreateRequest(
                    title="default-derived officer", project_id=SESSION_THREAD_ID
                ),
                MagicMock(),
            )

        assert exc.value.status_code == expected_status
        db.create_thread.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "post_owned",
        [
            {"worker_spend_ceiling_daily": 7.0},
            {"slots": {"line": {"count": 1, "spend_ceiling_daily": 3.0}}},
        ],
    )
    async def test_effective_account_spend_cannot_rehydrate_into_an_officer(
        self, session_create_env, monkeypatch, post_owned
    ):
        main, db, _, _ = session_create_env
        monkeypatch.setattr(
            session_config_resolution,
            "resolve_session_account_defaults",
            AsyncMock(return_value={"officer": {"enabled": True, **post_owned}}),
        )

        with pytest.raises(
            main.HTTPException, match="owned by the durable Officer Post"
        ):
            await main.create_thread(
                main.ThreadCreateRequest(
                    title="default-derived officer", project_id=SESSION_THREAD_ID
                ),
                MagicMock(),
            )

        db.create_thread.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("role", ["editor", "viewer"])
    async def test_officer_conference_requires_current_management_authority(
        self, session_create_env, monkeypatch, role
    ):
        main, db, _, _ = session_create_env
        db.get_user_role_in_project = AsyncMock(return_value=role)
        find_open = AsyncMock(return_value=None)
        monkeypatch.setattr(main, "_find_open_conference_thread", find_open)

        with pytest.raises(main.HTTPException) as exc:
            await main.create_thread(
                main.ThreadCreateRequest(
                    title="Officer conference",
                    project_id=SESSION_THREAD_ID,
                    project_ids=[SESSION_THREAD_ID],
                    config_override={"officer": {"conference": True}},
                ),
                MagicMock(),
            )

        assert exc.value.status_code == 403
        assert "Project owner" in exc.value.detail
        db.create_thread.assert_not_awaited()
        find_open.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_enabled_pool_auto_admits_sandbox_without_dedicated_agent(
        self, session_create_env, monkeypatch
    ):
        import asyncio

        main, db, conn, _ = session_create_env
        workspace_create = AsyncMock(return_value=True)
        schedule_workspace = MagicMock()
        monkeypatch.setattr(main, "STATELESS_SESSION_ENABLED", True)
        monkeypatch.setattr(
            main, "_schedule_stateless_workspace_ensure", schedule_workspace
        )
        monkeypatch.setattr(
            main,
            "container_provisioner",
            SimpleNamespace(
                is_available=True,
                in_cluster=True,
                create_workspace=workspace_create,
            ),
        )
        monkeypatch.setattr(
            main,
            "agent_provisioner",
            SimpleNamespace(is_available=True, in_cluster=True),
        )
        pinned_provision = AsyncMock()

        with patch(
            "orchestrator.services.provision_or_assign.provision_or_assign",
            pinned_provision,
        ):
            result = await main.create_thread(
                main.ThreadCreateRequest(
                    title="stateless sandbox",
                    config_override={"workspace": {"backend": "sandbox"}},
                ),
                MagicMock(),
            )
            await asyncio.sleep(0)

        assert result == {"thread_id": SESSION_THREAD_ID, "status": "created"}
        assert db.create_thread.await_args.kwargs["execution_lane"] == "stateless"
        initial = db.create_thread.await_args.kwargs["initial_metadata"]
        assert initial["config_override"]["officer"] == {
            "enabled": False,
            "conference": False,
        }
        creation = initial["workspace_container"]["_runtime_creation"]
        assert creation["mode"] == "create"
        assert creation["attempted"] is False
        assert creation["replaces_uid"] is None
        assert str(uuid.UUID(creation["generation"])) == creation["generation"]
        assert initial["workspace_container"]["status"] == "pending"
        assert initial["workspace_container"]["provisioner"] == "k8s"
        conn.execute.assert_not_awaited()
        db.merge_thread_workspace_context.assert_not_awaited()
        schedule_workspace.assert_called_once_with(SESSION_THREAD_ID)
        workspace_create.assert_not_awaited()
        pinned_provision.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stateless_sandbox_insert_failure_never_schedules_workspace(
        self, session_create_env, monkeypatch
    ):
        from fastapi import HTTPException

        main, db, _, _ = session_create_env
        schedule_workspace = MagicMock()
        db.create_thread.side_effect = RuntimeError("insert failed")
        monkeypatch.setattr(main, "STATELESS_SESSION_ENABLED", True)
        monkeypatch.setattr(
            main, "_schedule_stateless_workspace_ensure", schedule_workspace
        )
        monkeypatch.setattr(
            main,
            "container_provisioner",
            SimpleNamespace(
                is_available=True,
                in_cluster=True,
                create_workspace=AsyncMock(return_value=True),
            ),
        )

        with pytest.raises(HTTPException, match="insert failed") as exc:
            await main.create_thread(
                main.ThreadCreateRequest(
                    title="stateless sandbox",
                    config_override={"workspace": {"backend": "sandbox"}},
                ),
                MagicMock(),
            )

        assert exc.value.status_code == 500
        schedule_workspace.assert_not_called()

    @pytest.mark.asyncio
    async def test_disabled_pool_preserves_pinned_sandbox_agent_path(
        self, session_create_env, monkeypatch
    ):
        import asyncio

        main, db, _, _ = session_create_env
        monkeypatch.setattr(main, "STATELESS_SESSION_ENABLED", False)
        monkeypatch.setattr(
            main,
            "container_provisioner",
            SimpleNamespace(
                is_available=True,
                in_cluster=True,
                create_workspace=AsyncMock(return_value=True),
            ),
        )
        monkeypatch.setattr(
            main,
            "agent_provisioner",
            SimpleNamespace(is_available=True, in_cluster=True),
        )
        pinned_provision = AsyncMock()

        with patch(
            "orchestrator.services.provision_or_assign.provision_or_assign",
            pinned_provision,
        ):
            await main.create_thread(
                main.ThreadCreateRequest(
                    title="pinned sandbox",
                    config_override={"workspace": {"backend": "sandbox"}},
                ),
                MagicMock(),
            )
            await asyncio.sleep(0)

        assert db.create_thread.await_args.kwargs["execution_lane"] == "pinned"
        pinned_provision.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("class_source", ["account", "expert"])
    async def test_resolved_pinned_only_class_is_materialized_before_admission(
        self, session_create_env, monkeypatch, class_source
    ):
        import asyncio

        main, db, conn, _ = session_create_env
        monkeypatch.setattr(main, "STATELESS_SESSION_ENABLED", True)
        monkeypatch.setattr(
            main,
            "container_provisioner",
            SimpleNamespace(
                is_available=True,
                in_cluster=True,
                create_workspace=AsyncMock(return_value=True),
            ),
        )
        monkeypatch.setattr(
            main,
            "agent_provisioner",
            SimpleNamespace(is_available=True, in_cluster=True),
        )
        if class_source == "account":
            monkeypatch.setattr(
                session_config_resolution,
                "resolve_session_account_defaults",
                AsyncMock(return_value={"officer": {"enabled": True}}),
            )
        else:
            expert_id = str(uuid.uuid4())
            monkeypatch.setattr(
                main, "_is_experts_db_enabled", MagicMock(return_value=True)
            )
            monkeypatch.setattr(
                main, "_user_experts_enabled", AsyncMock(return_value=True)
            )
            # R1.B06: session admission moved to services/thread_admission, which
            # imports the resolver directly; patching main would be inert.
            monkeypatch.setattr(
                thread_admission,
                "resolve_root_expert",
                AsyncMock(
                    return_value=SimpleNamespace(
                        expert={
                            "id": expert_id,
                            "expert_type": "session",
                            "config": {"officer": {"enabled": True}},
                            "prompts": {},
                        },
                        source="application",
                        project_override=None,
                    )
                ),
            )

        pinned_provision = AsyncMock()
        with patch(
            "orchestrator.services.provision_or_assign.provision_or_assign",
            pinned_provision,
        ):
            await main.create_thread(
                main.ThreadCreateRequest(title=f"{class_source} officer"),
                MagicMock(),
            )
            await asyncio.sleep(0)

        assert db.create_thread.await_args.kwargs["execution_lane"] == "pinned"
        persisted = _persisted_thread_override(db)
        assert persisted["officer"] == {
            "enabled": True,
            "conference": False,
            "auto_pull": False,
        }
        pinned_provision.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_explicit_post_commission_uses_dedicated_lifecycle_substrate(
        self, session_create_env, monkeypatch
    ):
        """A background Officer must be born on the Pod its recycler owns.

        Ordinary pinned sessions may use the warm-pool fast path. A durable
        Post commission may not: the Officer lifecycle scanner intentionally
        owns only finalizer-protected ``persistent-agent`` Pods, so a warm
        binding would be held as ``unsupported_pod_authority`` on its first
        reconciliation pass.
        """
        import asyncio

        main, db, _, _ = session_create_env
        db.get_officer_thread_for_project = AsyncMock(return_value=None)
        db.get_user_role_in_project = AsyncMock(return_value="owner")
        db.register_project_officer_thread = AsyncMock(
            return_value={
                "commission_continuity": {"brief_enqueued": True},
            }
        )
        dedicated_create = AsyncMock(
            return_value=SimpleNamespace(
                usable=True,
                status=SimpleNamespace(value="created"),
                failure_class=None,
            )
        )
        monkeypatch.setattr(
            main,
            "persistent_provisioner",
            SimpleNamespace(is_available=True, create_agent_pod=dedicated_create),
        )
        monkeypatch.setattr(
            main,
            "agent_provisioner",
            SimpleNamespace(is_available=True, in_cluster=True),
        )
        monkeypatch.setattr(main, "OFFICER_AUTO_PULL_RELEASE_ENABLED", True)
        monkeypatch.setattr(
            thread_admission, "ensure_virtual_thread_workspace_binding", AsyncMock()
        )
        warm_provision = AsyncMock()
        request = main.ThreadCreateRequest(
            title="commissioned officer",
            project_id=SESSION_THREAD_ID,
            config_name="centurion",
            config_override={"workspace": {"backend": "virtual"}},
        )
        request._officer_post_config_snapshot = {
            "officer": {
                "enabled": True,
                "auto_pull": True,
                "slots": {
                    "test": {
                        "count": 0,
                        "category": "tester",
                        "model": "MiniMax-M3",
                        "backend": "sandbox",
                    }
                },
            },
            "workspace": {"backend": "virtual"},
        }

        with patch(
            "orchestrator.services.provision_or_assign.provision_or_assign",
            warm_provision,
        ):
            await main.create_thread(request, MagicMock())
            await asyncio.sleep(0)

        dedicated_create.assert_awaited_once_with(
            SESSION_THREAD_ID,
            config_name="centurion",
            expected_runtime_generation=SESSION_RUNTIME_GENERATION,
        )
        warm_provision.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_explicit_default_request_materializes_policy_selection(
        self, session_create_env
    ):
        main, db, _, _ = session_create_env
        resolver = AsyncMock(
            return_value=(
                [SESSION_DATASOURCE_ID],
                {SESSION_DATASOURCE_ID: 7},
            )
        )

        with patch(
            "orchestrator.services.datasource_policy.default_datasource_selection",
            resolver,
        ):
            await main.create_thread(
                main.ThreadCreateRequest(
                    title="t",
                    use_datasource_defaults=True,
                ),
                MagicMock(),
            )

        # Execution now owns workspace selection, and the documented account/role
        # default is sandbox for a worker but virtual for a session
        # (config/README.md, "Workspace ownership").
        resolver.assert_awaited_once_with(
            db,
            SESSION_USER_ID,
            [],
            "virtual",
        )
        kwargs = db.create_thread.await_args.kwargs
        assert kwargs["datasource_ids"] == [SESSION_DATASOURCE_ID]
        assert kwargs["datasource_policy_revisions"] == {SESSION_DATASOURCE_ID: 7}
        assert kwargs["datasource_selection_provenance"]["origin"] == "default"

    @pytest.mark.asyncio
    async def test_unticking_a_previously_discarded_category_reaches_the_thread(
        self, session_create_env
    ):
        """THE motivating defect, at the endpoint. Before: `tools.research` was
        copied across only if it was one of the original groups, so this key never
        reached `threads.metadata.config_override` and the agent bound research
        tools anyway."""
        main, db, conn, _ = session_create_env

        await main.create_thread(
            _thread_create_body(main, {"tools": {"research": [], "git": []}}),
            MagicMock(),
        )

        persisted = _persisted_thread_override(db)
        assert persisted["tools"]["research"] == []
        assert persisted["tools"]["git"] == []

    @pytest.mark.asyncio
    async def test_the_whole_form_deselect_survives(self, session_create_env):
        main, db, conn, _ = session_create_env

        await main.create_thread(
            _thread_create_body(
                main, {"tools": {c: [] for c in COCKPIT_SESSION_CATEGORIES}}
            ),
            MagicMock(),
        )

        persisted = _persisted_thread_override(db)
        assert set(persisted["tools"]) == set(COCKPIT_SESSION_CATEGORIES)
        assert all(v == [] for v in persisted["tools"].values())

    @pytest.mark.asyncio
    async def test_the_grant_check_sees_the_widened_fragment(self, session_create_env):
        """A category the boundary now honours must also reach the PDP — the
        fix must not hand the agent tools the grant check never saw."""
        main, _, _, grants = session_create_env

        await main.create_thread(
            _thread_create_body(main, {"tools": {"shell": ["run_command"]}}),
            MagicMock(),
        )

        fragment = grants.await_args.args[0]
        assert fragment["tools"]["shell"] == ["run_command"]

    @pytest.mark.asyncio
    async def test_a_smuggle_is_a_400_and_nothing_is_created(self, session_create_env):
        main, db, _, _ = session_create_env

        with pytest.raises(main.HTTPException) as exc:
            await main.create_thread(
                _thread_create_body(main, {"tools": {"canvas": ["run_command"]}}),
                MagicMock(),
            )
        assert exc.value.status_code == 400
        assert "run_command" in exc.value.detail
        db.create_thread.assert_not_awaited()


@pytest.fixture
def session_patch_env(monkeypatch):
    """The internal runtime PATCH (`agent_update_thread_config`) — the live
    `config.update` boundary, driven at the endpoint."""
    import orchestrator.main as main

    thread_row = {
        "id": SESSION_THREAD_ID,
        "user_id": SESSION_USER_ID,
        "project_id": None,
        "metadata": {"config_override": {"workspace": {"backend": "sandbox"}}},
    }
    db = SimpleNamespace(
        get_thread=AsyncMock(return_value=thread_row),
        get_user=AsyncMock(return_value={"id": SESSION_USER_ID, "is_admin": False}),
        get_datasource_policy_rows=AsyncMock(
            return_value=[
                {
                    "id": SESSION_DATASOURCE_ID,
                    "type": "postgresql",
                    "is_global": True,
                    "scope_mode": "all",
                    "policy_revision": 1,
                    "project_ids": [],
                }
            ]
        ),
        merge_thread_config_override=AsyncMock(return_value=True),
        set_thread_datasource_ids=AsyncMock(return_value=True),
        record_security_event=AsyncMock(),
        resolve_api_keys_for_job=AsyncMock(return_value={}),
        resolve_datasources_for_thread=AsyncMock(return_value=[]),
    )
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def transaction_scope(_thread_id):
        yield SimpleNamespace(
            fetchrow=AsyncMock(
                side_effect=lambda query, *_: None
                if "srw_execution_specs" in query
                else thread_row
            )
        )

    db.thread_configuration_transaction = transaction_scope
    db.refresh_session_execution = AsyncMock(
        side_effect=lambda _thread_id, *, conn, config_override: {
            "delivery_override": config_override
        }
    )
    monkeypatch.setattr(main, "postgres_db", db)
    monkeypatch.setattr(main, "require_internal", AsyncMock())
    monkeypatch.setattr(main, "_thread_project_ids", AsyncMock(return_value=[]))
    grants = AsyncMock()
    monkeypatch.setattr(main, "_enforce_session_create_grants", grants)
    return main, db, grants


class TestSessionRuntimeUpdateBoundary:
    @pytest.mark.asyncio
    async def test_a_live_untick_outside_the_four_groups_is_persisted(
        self, session_patch_env
    ):
        """Before: this boundary REPLACED `tools` with the accepted four-group
        subset, so a live "turn research off" was acknowledged and dropped."""
        main, db, _ = session_patch_env

        await thread_config_update.agent_update_thread_config(
            MagicMock(),
            SESSION_THREAD_ID,
            main.AgentThreadConfigUpdateRequest(
                config_override={"tools": {"research": [], "canvas": []}}
            ),
            dependencies=main._thread_config_update_dependencies(),
        )

        merged = db.merge_thread_config_override.await_args.args[1]
        assert merged["tools"] == {"research": [], "canvas": []}

    @pytest.mark.asyncio
    async def test_a_live_smuggle_is_a_400_and_nothing_persists(
        self, session_patch_env
    ):
        main, db, _ = session_patch_env

        with pytest.raises(main.HTTPException) as exc:
            await thread_config_update.agent_update_thread_config(
                MagicMock(),
                SESSION_THREAD_ID,
                main.AgentThreadConfigUpdateRequest(
                    config_override={"tools": {"citation": ["run_command"]}}
                ),
                dependencies=main._thread_config_update_dependencies(),
            )
        assert exc.value.status_code == 400
        db.merge_thread_config_override.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_datasource_flip_wins_over_the_requests_own_tools(
        self, session_patch_env
    ):
        """The order in the grant fragment is load-bearing. Now that a request
        may name a connector category, `tools.sql: []` would mask a
        datasource_tools violation — while attach applies the flip LAST, so the
        session would get the tools anyway."""
        main, db, grants = session_patch_env
        db.resolve_datasources_for_thread = AsyncMock(
            return_value=[
                {"type": "postgresql", "name": "PG", "project_read_only": False}
            ]
        )
        db.get_datasource = AsyncMock(
            return_value={
                "id": SESSION_DATASOURCE_ID,
                "type": "postgresql",
                "is_global": True,
            }
        )
        # ``user_can_access_datasource`` moved to ``services.projects`` with
        # ``link_datasource_to_project`` (R1.B03), so it is no longer a name on
        # ``main``. Patch it where it is defined. NOTE: this stub is inert on
        # this path — the thread-config selection authorizes through
        # ``services.datasource_policy``, which reads ``is_global`` inline (set
        # True on the row above); it was equally inert before the extraction,
        # when the name on ``main`` was only reachable from the project-link
        # endpoint.
        with patch(
            "orchestrator.security.access.user_can_access_datasource",
            AsyncMock(return_value=True),
        ):
            await thread_config_update.agent_update_thread_config(
                MagicMock(),
                SESSION_THREAD_ID,
                main.AgentThreadConfigUpdateRequest(
                    config_override={"tools": {"sql": []}},
                    datasource_ids=[SESSION_DATASOURCE_ID],
                ),
                dependencies=main._thread_config_update_dependencies(),
            )

        fragment = grants.await_args.args[0]
        assert fragment["tools"]["sql"], (
            "the request's `sql: []` masked the datasource flip from the PDP"
        )


# =============================================================================
# The three boundaries found by review — I1 and I3
# =============================================================================


class TestPrepareBoundary:
    """`POST /api/sessions/{id}/prepare` takes a `config_override` that flows
    to `_resolve_session_config`, where a non-None value **replaces** the
    thread's persisted override outright — so it is a write boundary, not a
    hint. Owner-only and API-direct (the cockpit posts `{}`)."""

    @pytest.fixture
    def prepare_env(self, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from orchestrator.routers import sessions as sessions_mod

        async def _fake_auth(request, db):
            return {"id": "u1", "is_approved": True}

        monkeypatch.setattr(sessions_mod, "require_approved_user", _fake_auth)
        store = SimpleNamespace(
            get_thread=AsyncMock(
                return_value={
                    "id": SESSION_THREAD_ID,
                    "user_id": "u1",
                    "agent_id": None,
                    "config_name": "session_base",
                    "execution_lane": "pinned",
                    "status": "created",
                    "runtime_generation": SESSION_RUNTIME_GENERATION,
                    "runtime_retirement_token": None,
                }
            )
        )
        do_prepare = MagicMock()
        monkeypatch.setattr(sessions_mod, "_do_prepare", do_prepare)
        monkeypatch.setattr(
            sessions_mod, "_schedule_prepare_task", lambda coro: MagicMock()
        )
        # A pinned prepare first asks the durable idle ledger whether it must
        # wake a released VM instead. No idle operation or access continuation
        # is open here, so the request takes the ordinary provisioning path.
        from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

        monkeypatch.setattr(
            VMIdleLifecycleStore, "get_open_for_thread", AsyncMock(return_value=None)
        )
        monkeypatch.setattr(
            VMIdleLifecycleStore,
            "get_pending_access_continuation",
            AsyncMock(return_value=None),
        )

        app = FastAPI()
        # The router reads its collaborators off the application answering the
        # request; only the store matters for this write boundary.
        app.state.sessions_dependencies_factory = lambda: SimpleNamespace(store=store)
        app.include_router(sessions_mod.router)
        return TestClient(app, raise_server_exceptions=False), do_prepare

    def test_a_smuggle_is_a_400_and_nothing_is_provisioned(self, prepare_env):
        client, do_prepare = prepare_env

        resp = client.post(
            f"/api/sessions/{SESSION_THREAD_ID}/prepare",
            json={"config_override": {"tools": {"canvas": ["run_command"]}}},
        )

        assert resp.status_code == 400
        assert "run_command" in resp.text
        do_prepare.assert_not_called()

    def test_a_valid_override_reaches_provisioning_normalised(self, prepare_env):
        client, do_prepare = prepare_env

        resp = client.post(
            f"/api/sessions/{SESSION_THREAD_ID}/prepare",
            json={"config_override": {"tools": {"canvas": True, "research": []}}},
        )

        assert resp.status_code == 202
        passed = do_prepare.call_args.kwargs["config_override"]
        assert passed["tools"] == {
            "canvas": ["clear_canvas", "get_canvas", "set_canvas"],
            "research": [],
        }

    def test_the_cockpits_empty_body_is_unaffected(self, prepare_env):
        """The shipped cockpit posts `{}` — no behaviour change for it."""
        client, do_prepare = prepare_env

        assert (
            client.post(
                f"/api/sessions/{SESSION_THREAD_ID}/prepare", json={}
            ).status_code
            == 202
        )
        assert do_prepare.call_args.kwargs["config_override"] is None


class TestAutomationBoundary:
    """An automation's `config_override` is stored raw and handed STRAIGHT to
    `db.create_job` by `create_job_from_automation` — it never crosses
    `POST /api/jobs`, so every cron fire re-plants whatever is stored."""

    @pytest.fixture
    def automations_env(self, monkeypatch):
        from orchestrator.routers import automations as mod

        caller = {"id": SESSION_USER_ID, "is_admin": False}
        db = SimpleNamespace(
            create_automation=AsyncMock(return_value={"id": "a1"}),
            update_automation=AsyncMock(return_value={"id": "a1"}),
        )
        # R1.B07 closed this router's late `from main import postgres_db`.
        # The store now arrives on `AutomationsDependencies`, which the cases
        # below hand in explicitly — calling a declaration directly never
        # resolves its `Depends(...)` default.
        monkeypatch.setattr("orchestrator.main.postgres_db", db)
        monkeypatch.setattr(
            mod, "require_approved_user", AsyncMock(return_value=caller)
        )
        monkeypatch.setattr(
            mod,
            "validate_automation_expert_selection",
            AsyncMock(return_value="developer"),
        )
        monkeypatch.setattr(
            mod,
            "_resolve_automation_or_404",
            AsyncMock(
                return_value={
                    "id": "a1",
                    "owner_id": SESSION_USER_ID,
                    "project_id": None,
                    "expert": "developer",
                    "enabled": True,
                    "cron_expr": "0 * * * *",
                    "timezone": "UTC",
                }
            ),
        )
        return mod, db

    @staticmethod
    def _deps(mod, db):
        """What `main._automations_dependencies()` binds, with this suite's
        store. The forge/cloud clients and the dispatch nudge are inert here:
        the boundary under test refuses before any job is created."""
        return mod.AutomationsDependencies(
            store=db,
            gitea_client=MagicMock(),
            main_cloud_router=MagicMock(),
            trigger_dispatch=MagicMock(),
        )

    def _create_body(self, mod, config_override):
        return mod.AutomationCreate(
            name="a",
            cron_expr="0 * * * *",
            expert="developer",
            prompt="p",
            config_override=config_override,
        )

    @pytest.mark.asyncio
    async def test_a_smuggle_is_a_400_and_nothing_is_stored(self, automations_env):
        from fastapi import HTTPException

        mod, db = automations_env

        with pytest.raises(HTTPException) as exc:
            await mod.create_automation(
                MagicMock(),
                self._create_body(mod, {"tools": {"canvas": ["run_command"]}}),
                dependencies=self._deps(mod, db),
            )
        assert exc.value.status_code == 400
        db.create_automation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_valid_override_is_stored_normalised(self, automations_env):
        mod, db = automations_env

        await mod.create_automation(
            MagicMock(),
            self._create_body(mod, {"tools": {"research": []}}),
            dependencies=self._deps(mod, db),
        )

        stored = db.create_automation.await_args.kwargs["config_override"]
        assert stored["tools"] == {"research": []}

    @pytest.mark.asyncio
    async def test_the_patch_path_is_validated_too(self, automations_env):
        """Every fire replays the stored override, so an update is the same
        boundary as a create."""
        from fastapi import HTTPException

        mod, db = automations_env

        with pytest.raises(HTTPException) as exc:
            await mod.update_automation(
                MagicMock(),
                "a1",
                mod.AutomationUpdate(
                    config_override={"tools": {"citation": ["shell_execute"]}}
                ),
                dependencies=self._deps(mod, db),
            )
        assert exc.value.status_code == 400
        db.update_automation.assert_not_awaited()


class TestProjectDefaultOverrideBoundary:
    """`projects.default_config_override` is merged UNDER every job created in
    the project, so an unvalidated tools block here is a cross-principal
    escalation: the planter is not the runner, and the PDP keys off the
    category name. Validated on WRITE only — no existing row is touched."""

    @pytest.fixture
    def project_env(self, monkeypatch):
        """The handlers live in ``routers.projects`` now; the gates, store and
        the ``with_validated_tool_overrides`` gate they run still come from
        ``main`` — through ``main._projects_dependencies()``, which reads these
        very module globals at call time, exactly as production does."""
        import orchestrator.main as main

        db = SimpleNamespace(
            create_project=AsyncMock(return_value={"id": "p1"}),
            add_project_member=AsyncMock(),
            update_project=AsyncMock(return_value=True),
        )
        monkeypatch.setattr(main, "postgres_db", db)
        monkeypatch.setattr(
            main,
            "require_approved_user",
            AsyncMock(return_value={"id": SESSION_USER_ID, "is_admin": False}),
        )
        monkeypatch.setattr(main, "require_project_owner", AsyncMock())
        return main, db

    @pytest.mark.asyncio
    async def test_create_rejects_a_smuggle(self, project_env):
        from orchestrator.routers.projects import create_project
        from orchestrator.schemas.projects import ProjectCreate

        main, db = project_env

        with pytest.raises(main.HTTPException) as exc:
            await create_project(
                ProjectCreate(
                    name="p",
                    user_id=SESSION_USER_ID,
                    default_config_override={"tools": {"canvas": ["run_command"]}},
                ),
                MagicMock(),
                dependencies=main._projects_dependencies(),
            )
        assert exc.value.status_code == 400
        db.create_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_patch_rejects_a_smuggle(self, project_env):
        from orchestrator.routers.projects import update_project
        from orchestrator.schemas.projects import ProjectUpdate

        main, db = project_env

        with pytest.raises(main.HTTPException) as exc:
            await update_project(
                "p1",
                ProjectUpdate(
                    default_config_override={"tools": {"knowledge": ["run_command"]}}
                ),
                MagicMock(),
                dependencies=main._projects_dependencies(),
            )
        assert exc.value.status_code == 400
        db.update_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_patch_that_does_not_send_the_field_is_untouched(self, project_env):
        """Write-only is what makes this back-compatible: an existing row with
        a bad override is never read, let alone rejected, until someone
        rewrites it."""
        from orchestrator.routers.projects import update_project
        from orchestrator.schemas.projects import ProjectUpdate

        main, db = project_env

        await update_project(
            "p1",
            ProjectUpdate(name="renamed"),
            MagicMock(),
            dependencies=main._projects_dependencies(),
        )

        assert db.update_project.await_args.args[0] == "p1"
        assert "default_config_override" not in db.update_project.await_args.kwargs

    @pytest.mark.asyncio
    async def test_the_cockpits_memory_toggle_payload_still_works(self, project_env):
        """`project-detail.component.ts::toggleProjectMemory` re-submits the
        WHOLE stored override plus a memory key. It carries no tools block, so
        it is unaffected — this is the payload the write-path check has to keep
        working."""
        from orchestrator.routers.projects import update_project
        from orchestrator.schemas.projects import ProjectUpdate

        main, db = project_env

        await update_project(
            "p1",
            ProjectUpdate(default_config_override={"memory": {"project_scoped": True}}),
            MagicMock(),
            dependencies=main._projects_dependencies(),
        )

        assert db.update_project.await_args.kwargs["default_config_override"] == {
            "memory": {"project_scoped": True}
        }


# =============================================================================
# An affirmative policy that turns nothing on — M-b
# =============================================================================


class TestAffirmativeThatExpandsToNothing:
    """`{"tools": {"sql": true}}` used to return `{"sql": []}` with a 200 and a
    server-side log: the caller asked for ON and got OFF. That is "accepted and
    discarded" at a request boundary — this task's own defect in miniature. A
    warning is right for a config layer and wrong for a request."""

    @pytest.mark.parametrize(
        "category",
        [
            "sql",
            "graph",
            "mongodb",
            "webdav",
            "email",
            "repo",
            "product_help",
            "session_task",
        ],
    )
    def test_true_on_a_wholly_code_granted_category_is_a_400(self, category):
        with pytest.raises(ToolPolicyError) as exc:
            validate_tool_override_fragment({"tools": {category: True}})
        assert "turn nothing on" in str(exc.value)
        # The message names the real gate, so the caller learns why.
        assert "runtime code" in str(exc.value) or "attached" in str(exc.value)

    def test_except_that_expands_to_nothing_is_a_400_too(self):
        with pytest.raises(ToolPolicyError, match="turn nothing on"):
            validate_tool_override_fragment(
                {"tools": {"sql": {"except": ["sql_query"]}}}
            )

    @pytest.mark.parametrize("off", [False, []])
    def test_turning_such_a_category_off_stays_legal(self, off):
        """Asking to turn off a category nobody manages is a harmless no-op,
        and both bases ship exactly this."""
        assert validate_tool_override_fragment({"tools": {"sql": off}}) == {"sql": []}

    def test_a_category_config_does_manage_is_unaffected(self):
        assert validate_tool_override_fragment({"tools": {"canvas": True}})["canvas"]


# =============================================================================
# What actually makes the `explicit`-tier collapse safe — C1
# =============================================================================


class TestNoModelAuthoredPathReachesSessionCreate:
    """Membership is the whole registry category, so a session-create request
    may NAME a `grant: "explicit"` control-plane write (`set_expert_bundle`)
    that the old four-name list refused. `true` still cannot reach them, and
    the tool acts as the session's own user — but neither of those answers
    prompt injection.

    The property that does is that **no model-authored path reaches session
    create's `config_override`**: the MCP `create_persistent_thread` tool
    exposes no such parameter, and `delegate_agent` builds its child from the
    expert's roster, never from a model-authored config.
    That is load-bearing and otherwise invisible — adding the parameter would
    silently dissolve the mitigation. So it is pinned here.
    """

    def test_the_mcp_session_create_tool_exposes_no_config_override(self):
        # Parsed, not imported: `orchestrator.mcp.server` needs `fastmcp`,
        # which the orchestrator image ships and this test environment does
        # not. The signature is what matters and the AST has it.
        import ast
        from pathlib import Path

        tree = ast.parse(Path("src/mcp_server/server.py").read_text())
        fns = [
            n
            for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            and n.name == "create_persistent_thread"
        ]
        assert fns, "create_persistent_thread vanished — re-point this test"
        for fn in fns:
            args = fn.args
            params = {a.arg for a in [*args.posonlyargs, *args.args, *args.kwonlyargs]}
            assert "config_override" not in params, (
                "Adding config_override to the MCP session-create tool opens a "
                "MODEL-AUTHORED path to a boundary that accepts every registry "
                "name in its own category, including the *_bundle "
                "control-plane writes. Decide what a model may name before "
                "adding it — see TestNoModelAuthoredPathReachesSessionCreate."
            )

    def test_the_job_create_tool_does_expose_one_which_is_why_it_is_validated(self):
        """The contrast that makes the point: `create_job` DOES forward
        a model-authored fragment verbatim, which is exactly why the job
        boundary had to be closed rather than trusted."""
        from pathlib import Path

        src = Path("src/shared/orch_surface/jobs/control.py").read_text()
        assert "async def create_job(" in src
        assert "config_override: dict[str, Any] | None = None," in src


# =============================================================================
# The expert boundary — the one the "every write boundary" pass missed
# =============================================================================

BUNDLED_EXPERT_CONFIGS = sorted(
    p.parent.name for p in Path("config/experts").glob("*/config.yaml")
)


class TestExpertWriteBoundary:
    """An expert's `config` is an authored config layer, and until now the only
    thing standing in front of it was a *credential* deny-scan plus the grants
    PDP. Neither can see a cross-category smuggle: the PDP keys off the category
    (`_truthy(tools.get("shell"))`), and `load_tools` regroups by REGISTRY
    category — so `tools.canvas: ["run_command"]` stored here binds a shell tool
    with no `shell_tools` grant, for every job and session the expert drives.

    Five writes, not four. `POST /api/experts/{id}/duplicate` had neither gate:
    it forks a *visible* row, which may be another principal's, so it is the one
    path that can propagate a stored smuggle into a new row.

    Second, quieter failure this closes: a stored fragment is normalised on the
    way OUT (`expert_resolution.build_expert_config`), so a shape
    `normalize_tool_policy` refuses — `tools.shell: true` — was storable and
    then made the expert unresolvable. Refusing at write turns a later 500 into
    an immediate 400.
    """

    @pytest.fixture
    def expert_env(self, monkeypatch):
        import orchestrator.main as main

        db = SimpleNamespace(
            create_expert=AsyncMock(return_value={"id": "e1"}),
            update_expert=AsyncMock(return_value={"id": "e1"}),
            get_expert_by_id=AsyncMock(
                return_value={
                    "id": "e1",
                    "owner_id": SESSION_USER_ID,
                    "expert_type": "session",
                    "name": "helper",
                    "display_name": "Helper",
                    "icon": "smart_toy",
                    "color": "#6B7280",
                    "managed_key": None,
                }
            ),
            get_expert_visible_by_id=AsyncMock(
                return_value={
                    "id": "e2",
                    "owner_id": str(uuid.uuid4()),
                    "expert_type": "session",
                    "name": "shared",
                    "display_name": "Shared",
                    "icon": "smart_toy",
                    "color": "#6B7280",
                    "config": {"tools": {"canvas": ["run_command"]}},
                }
            ),
            fork_and_set_user_expert_default=AsyncMock(return_value={"id": "e3"}),
        )
        monkeypatch.setattr(main, "postgres_db", db)
        monkeypatch.setattr(
            main,
            "require_approved_user",
            AsyncMock(return_value={"id": SESSION_USER_ID, "is_admin": False}),
        )
        # The save-time PDP is stubbed so these tests see the SHAPE gate alone;
        # `test_the_pdp_reads_the_canonical_fragment` asserts what it is handed.
        # `duplicate_expert` no longer calls `_enforce_expert_save` (task 3,
        # 2026-08-04 plan): it runs `_enforce_expert_save_prelude` (kill switch
        # + raw-request scan) and then `_strip_save_grants` (strip-and-report)
        # instead, so both are stubbed here too. Without this, a shape-valid
        # `duplicate` test added to this fixture would skip the now-dead
        # `_enforce_expert_save` stub, fall through to the REAL prelude/grants
        # resolution, and hit `AttributeError` on this fixture's bare
        # `SimpleNamespace` db (no `get_system_setting`/`list_grants_for_scopes`)
        # instead of seeing the shape gate in isolation like every other route
        # here does. `_strip_save_grants`'s stub echoes the config back
        # unchanged with nothing dropped — the pass-through analog of
        # `_enforce_expert_save`'s no-op for the other four routes.
        monkeypatch.setattr(main, "_enforce_expert_save", AsyncMock())
        monkeypatch.setattr(main, "_enforce_expert_save_prelude", AsyncMock())
        monkeypatch.setattr(
            main,
            "_strip_save_grants",
            AsyncMock(side_effect=lambda config, **_kwargs: (config, [])),
        )
        monkeypatch.setattr(
            main, "user_visible_project_ids", AsyncMock(return_value=[])
        )
        monkeypatch.setattr(
            expert_authoring_module,
            "personal_defaults_allowed",
            AsyncMock(return_value=True),
        )
        return main, db

    def _create_body(self, main, config):
        return expert_schemas.ExpertCreate(
            name="helper",
            display_name="Helper",
            expert_type="session",
            config=config,
        )

    SMUGGLE = {"tools": {"canvas": ["run_command"]}}

    @pytest.mark.asyncio
    async def test_create_rejects_a_smuggle_and_stores_nothing(self, expert_env):
        main, db = expert_env

        with pytest.raises(main.HTTPException) as exc:
            await catalogue_route(expert_routes.create_expert)(
                MagicMock(), self._create_body(main, self.SMUGGLE)
            )

        assert exc.value.status_code == 400
        assert "run_command" in exc.value.detail
        db.create_expert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_update_rejects_a_smuggle_and_stores_nothing(self, expert_env):
        main, db = expert_env

        with pytest.raises(main.HTTPException) as exc:
            await catalogue_route(expert_routes.update_expert)(
                MagicMock(),
                str(uuid.uuid4()),
                expert_schemas.ExpertUpdate(config=self.SMUGGLE),
            )

        assert exc.value.status_code == 400
        db.update_expert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_import_rejects_a_smuggle(self, expert_env):
        """A posted bundle is caller-supplied JSON — the same trust level as a
        create body, and the export/import pair is how a fragment travels
        between deployments."""
        main, db = expert_env

        with pytest.raises(main.HTTPException) as exc:
            await catalogue_route(expert_routes.import_expert)(
                MagicMock(), self._create_body(main, self.SMUGGLE)
            )

        assert exc.value.status_code == 400
        db.create_expert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fork_of_a_personal_default_rejects_a_smuggle(self, expert_env):
        main, db = expert_env
        selection = SimpleNamespace(
            expert={
                "name": "shared",
                "display_name": "Shared",
                "expert_type": "session",
                "icon": "smart_toy",
                "color": "#6B7280",
                "config": self.SMUGGLE,
            }
        )
        with patch.object(
            expert_authoring_module,
            "resolve_root_expert",
            AsyncMock(return_value=selection),
        ):
            with pytest.raises(main.HTTPException) as exc:
                await catalogue_route(expert_routes.fork_my_expert_default)(
                    MagicMock(), "session", expert_schemas.ExpertDefaultForkRequest()
                )

        assert exc.value.status_code == 400
        db.fork_and_set_user_expert_default.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_duplicate_rejects_a_smuggle_carried_by_the_source_row(
        self, expert_env
    ):
        """The fifth site, and the only one that had NO save-time gate at all.
        A legacy row is never validated where it sits (write-only, as at every
        other boundary), but copying it is a new write by a new principal."""
        main, db = expert_env

        with pytest.raises(main.HTTPException) as exc:
            await catalogue_route(expert_routes.duplicate_expert)(
                MagicMock(), str(uuid.uuid4())
            )

        assert exc.value.status_code == 400
        db.create_expert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_duplicate_of_a_shape_valid_config_reaches_create(self, expert_env):
        """Guards the `expert_env` fixture itself (fix round 1, task 3): the
        smuggle test above never reaches the save-time gate at all (it 400s
        at shape validation, before either the old or new gate call), so it
        cannot prove the fixture's `_enforce_expert_save_prelude`/
        `_strip_save_grants` stubs are wired correctly. A shape-VALID
        duplicate does reach them — without the stubs added in this fix, this
        hits the real prelude (`_user_experts_enabled` reads
        `postgres_db.get_system_setting`, which this fixture's bare
        `SimpleNamespace` db does not have) and fails with `AttributeError`
        instead of returning the forked row.
        """
        main, db = expert_env
        db.get_expert_visible_by_id = AsyncMock(
            return_value={
                "id": "e2",
                "owner_id": str(uuid.uuid4()),
                "expert_type": "session",
                "name": "shared",
                "display_name": "Shared",
                "icon": "smart_toy",
                "color": "#6B7280",
                "config": {"llm": {"model": "gemma-4-moe"}},  # nothing grant-gated
            }
        )

        result = await catalogue_route(expert_routes.duplicate_expert)(
            MagicMock(), str(uuid.uuid4())
        )

        assert result == {"id": "e1", "dropped": []}
        db.create_expert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_an_unresolvable_shape_is_refused_at_write_not_at_read(
        self, expert_env
    ):
        """`tools.shell: true` is refused by `normalize_tool_policy`, which runs
        when the row is READ — so storing it produced an expert that could not
        resolve. Same 400 as every other boundary gives it."""
        main, db = expert_env

        with pytest.raises(main.HTTPException) as exc:
            await catalogue_route(expert_routes.create_expert)(
                MagicMock(), self._create_body(main, {"tools": {"shell": True}})
            )

        assert exc.value.status_code == 400
        assert "must enumerate" in exc.value.detail
        db.create_expert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_stored_fragment_is_canonical(self, expert_env):
        """Assignment, not filtering: what is stored is the normalised list the
        loader and the PDP both speak, so `true` never sits in a row waiting to
        be re-interpreted."""
        main, db = expert_env

        await catalogue_route(expert_routes.create_expert)(
            MagicMock(), self._create_body(main, {"tools": {"git": True}})
        )

        assert db.create_expert.await_args.kwargs["config"] == {
            "tools": sorted_git_expansion()
        }

    @pytest.mark.asyncio
    async def test_the_pdp_reads_the_canonical_fragment(self, expert_env, monkeypatch):
        """Order is load-bearing. The PDP reads `_truthy(tools.get(...))`, which
        gets `{}` and `{only: []}` backwards; normalising first means it can
        only ever see a list."""
        main, _db = expert_env
        seen = AsyncMock()
        monkeypatch.setattr(main, "_enforce_expert_save", seen)

        await catalogue_route(expert_routes.create_expert)(
            MagicMock(), self._create_body(main, {"tools": {"shell": False}})
        )

        assert seen.await_args.args[1] == {"tools": {"shell": []}}

    @pytest.mark.asyncio
    async def test_a_config_without_tools_is_untouched(self, expert_env):
        main, db = expert_env

        await catalogue_route(expert_routes.create_expert)(
            MagicMock(), self._create_body(main, {"llm": {"model": "gemma-4-moe"}})
        )

        assert db.create_expert.await_args.kwargs["config"] == {
            "llm": {"model": "gemma-4-moe"}
        }

    @pytest.mark.asyncio
    async def test_the_expert_editors_own_payload_still_saves(self, expert_env):
        """Back-compat that matters most: `tools-group.component.ts` is shared
        by the expert editor, and it emits `true` for on, `[]` for off and the
        `enumerate_only` enumeration for a category that refuses `true`. All
        three have to survive this gate."""
        main, db = expert_env
        from shared.runtime.core.tool_policy import enumerate_only_members

        payload = {
            "tools": {
                "shell": enumerate_only_members()["shell"],
                "research": True,
                "citation": [],
            },
            "delegation": {"enabled": False},
        }
        await catalogue_route(expert_routes.create_expert)(
            MagicMock(), self._create_body(main, payload)
        )

        stored = db.create_expert.await_args.kwargs["config"]
        assert stored["tools"]["shell"] == enumerate_only_members()["shell"]
        assert stored["tools"]["citation"] == []
        assert stored["tools"]["research"] == sorted(
            n
            for n in get_tools_by_category("research")
            if "grant" not in TOOL_REGISTRY[n]
        ), "`true` did not expand against the registry"
        assert stored["delegation"] == {"enabled": False}

    @pytest.mark.parametrize("expert", BUNDLED_EXPERT_CONFIGS)
    def test_every_bundled_expert_still_duplicates(self, expert):
        """ "Start from scholar" is the primary way an expert gets created, and
        it now runs the source through the gate. All eleven shipped configs must
        pass or the gate has broken onboarding."""
        import yaml

        config = _srw_config_fragment(
            yaml.safe_load(Path(f"config/experts/{expert}/config.yaml").read_text())
        )
        config.pop("$extends", None)
        validate_tool_override_fragment(config)

    # -------------------------------------------------------------------------
    # The kill switch, pinned across the class rather than the instance
    # -------------------------------------------------------------------------
    #
    # knowledge-base/knowledge/issues/duplicate_expert_bypasses_user_experts_kill_switch.md: five
    # routes write an expert, and until now `duplicate` was the only one with
    # no save-time gate at all — an administrator's `user_experts` switch did
    # not hold there. But the issue doc's own "Verification owed" section is
    # explicit that no test asserted a 403 from ANY of the five while the
    # switch was off, so the four that already called `_enforce_expert_save`
    # were unpinned too. `kill_switch_env` is deliberately the mirror image of
    # `expert_env` above: that fixture stubs `_enforce_expert_save` out so its
    # tests see the shape gate alone; this one leaves it REAL, because the
    # kill switch inside it is exactly what is under test here.

    @pytest.fixture
    def kill_switch_env(self, monkeypatch):
        import orchestrator.main as main

        db = SimpleNamespace(
            create_expert=AsyncMock(return_value={"id": "e1"}),
            update_expert=AsyncMock(return_value={"id": "e1"}),
            get_expert_by_id=AsyncMock(
                return_value={
                    "id": "e1",
                    "owner_id": SESSION_USER_ID,
                    "expert_type": "session",
                    "name": "helper",
                    "display_name": "Helper",
                    "icon": "smart_toy",
                    "color": "#6B7280",
                    "managed_key": None,
                }
            ),
            get_expert_visible_by_id=AsyncMock(
                return_value={
                    "id": "e2",
                    "owner_id": str(uuid.uuid4()),
                    "expert_type": "session",
                    "name": "shared",
                    "display_name": "Shared",
                    "icon": "smart_toy",
                    "color": "#6B7280",
                    "config": {},  # clean: a shape 400 must not mask the 403
                }
            ),
            fork_and_set_user_expert_default=AsyncMock(return_value={"id": "e3"}),
        )
        monkeypatch.setattr(main, "postgres_db", db)
        monkeypatch.setattr(
            main,
            "require_approved_user",
            AsyncMock(return_value={"id": SESSION_USER_ID, "is_admin": False}),
        )
        monkeypatch.setattr(
            main, "user_visible_project_ids", AsyncMock(return_value=[])
        )
        monkeypatch.setattr(
            expert_authoring_module,
            "personal_defaults_allowed",
            AsyncMock(return_value=True),
        )
        return main, db

    #: The exact detail `_enforce_expert_save` raises on the kill-switch
    #: branch. Asserting this (not just the status code) matters because
    #: `fork_my_expert_default` has its OWN unrelated 403
    #: ("...disabled personal default experts") a step earlier in that same
    #: route — a status-code-only assertion could not tell the two apart.
    KILL_SWITCH_DETAIL = "User-defined experts are disabled by the administrator"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "route_id",
        [
            "POST /api/experts",
            "PUT /api/experts/{id}",
            "POST /api/experts/import",
            "POST /api/expert-defaults/{expert_type}/fork",
            "POST /api/experts/{id}/duplicate",
        ],
    )
    async def test_kill_switch_403s_every_write_route(
        self, kill_switch_env, monkeypatch, route_id
    ):
        """One parametrised test over all five expert-write routes. Note the
        fourth id: the issue doc and the plan both call this route
        `POST /api/experts/{id}/fork-default`, but the actual decorator is
        `@app.post("/api/expert-defaults/{expert_type}/fork")`
        (`fork_my_expert_default`) — the id below names the real route so a
        failure points somewhere that greps."""
        main, db = kill_switch_env
        monkeypatch.setattr(
            main, "_user_experts_enabled", AsyncMock(return_value=False)
        )
        existing_id = str(uuid.uuid4())

        if route_id == "POST /api/expert-defaults/{expert_type}/fork":
            # Bypass the real resolve_root_expert (it needs a fuller DB double
            # than this fixture provides) the same way
            # test_fork_of_a_personal_default_rejects_a_smuggle does above.
            selection = SimpleNamespace(
                expert={
                    "name": "shared",
                    "display_name": "Shared",
                    "expert_type": "session",
                    "icon": "smart_toy",
                    "color": "#6B7280",
                    "config": {},
                }
            )
            monkeypatch.setattr(
                expert_authoring_module,
                "resolve_root_expert",
                AsyncMock(return_value=selection),
            )

        calls = {
            "POST /api/experts": lambda: catalogue_route(expert_routes.create_expert)(
                MagicMock(), self._create_body(main, {})
            ),
            "PUT /api/experts/{id}": lambda: catalogue_route(
                expert_routes.update_expert
            )(MagicMock(), existing_id, expert_schemas.ExpertUpdate()),
            "POST /api/experts/import": lambda: catalogue_route(
                expert_routes.import_expert
            )(MagicMock(), self._create_body(main, {})),
            "POST /api/expert-defaults/{expert_type}/fork": (
                lambda: catalogue_route(expert_routes.fork_my_expert_default)(
                    MagicMock(), "session", expert_schemas.ExpertDefaultForkRequest()
                )
            ),
            "POST /api/experts/{id}/duplicate": lambda: catalogue_route(
                expert_routes.duplicate_expert
            )(MagicMock(), existing_id),
        }

        with pytest.raises(main.HTTPException) as exc:
            await calls[route_id]()

        assert exc.value.status_code == 403
        assert exc.value.detail == self.KILL_SWITCH_DETAIL
        db.create_expert.assert_not_awaited()
        db.update_expert.assert_not_awaited()
        db.fork_and_set_user_expert_default.assert_not_awaited()
