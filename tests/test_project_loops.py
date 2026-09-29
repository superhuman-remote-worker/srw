"""Unit tests for the project self-improvement loop service.

Focus: ``create_loop_job`` builds the right per-job ``config_override`` — the
"bare" lifecycle flags plus the per-loop ``model`` and ``workspace_backend``
overrides (the latter mirrors the former; it pins every spawned job's workspace
tier so e.g. a loop can run all of its roles on a VM).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
import orchestrator.services.project_loops as project_loops_service

from orchestrator.services.project_loops import (
    extract_cooldown_reset_at,
    build_loop_description,
    build_loop_kickoff,
    create_loop_job,
    job_loop_id,
    normalize_stage,
    validate_role_sequence,
)


def _loop(**over):
    base = {
        "id": "11111111-1111-1111-1111-111111111111",
        "project_id": None,  # None → skip datasource linking in create_loop_job
        "owner_id": None,
        "goal": "Build a thing",
        "acceptance_criteria": None,
        "user_prompt": None,
        "model": None,
        "remaining_iterations": 5,
        "run_until": None,
        "workspace_backend": None,
    }
    base.update(over)
    return base


def _db():
    db = AsyncMock()
    db.get_project.return_value = None
    db.create_job = AsyncMock(return_value={"id": "job-1"})
    # Keep the optional resolution paths inert regardless of EXPERTS_DB_ENABLED.
    db.list_experts_visible = AsyncMock(return_value=[])
    db.list_project_datasources = AsyncMock(return_value=[])
    # No project_workspace_defaults row: the installation decides the tier.
    db.fetchrow = AsyncMock(return_value=None)
    return db


@pytest.fixture(autouse=True)
def _empty_datasource_selection(monkeypatch):
    """Keep the broad loop protocol suite independent of connector fixtures."""
    resolver = AsyncMock(return_value=([], {}))
    monkeypatch.setattr(project_loops_service, "default_datasource_selection", resolver)
    return resolver


def _config_override(db):
    return db.create_job.call_args.kwargs["config_override"]


def _context(db):
    return db.create_job.call_args.kwargs["context"]


@pytest.mark.asyncio
async def test_workspace_backend_injected_when_set():
    db = _db()
    await create_loop_job(
        db, _loop(workspace_backend="vm"), role="developer", iteration=2
    )
    assert _config_override(db)["workspace"] == {"backend": "vm"}


@pytest.mark.asyncio
async def test_no_workspace_key_when_backend_unset():
    db = _db()
    await create_loop_job(db, _loop(), role="scholar", iteration=1)
    assert "workspace" not in _config_override(db)


@pytest.mark.asyncio
async def test_model_and_backend_coexist_with_bare_invariants():
    db = _db()
    await create_loop_job(
        db,
        _loop(model="openrouter/minimax/minimax-m3", workspace_backend="vm"),
        role="developer",
        iteration=3,
    )
    co = _config_override(db)
    assert co["llm"] == {"model": "openrouter/minimax/minimax-m3"}
    assert co["workspace"] == {"backend": "vm"}
    # The override must not clobber the loop's "bare" lifecycle invariants.
    assert co["verification"] == {"enabled": False}
    assert co["scholar"] == {"enabled": False}
    assert co["autonomy"] == "full"
    assert co["memory"] == {"required": True}


@pytest.mark.asyncio
async def test_loop_materializes_only_live_owner_defaults(_empty_datasource_selection):
    db = _db()
    db.link_datasource_to_job = AsyncMock()
    project_id = "22222222-2222-2222-2222-222222222222"
    owner_id = "33333333-3333-3333-3333-333333333333"
    selected = ["44444444-4444-4444-4444-444444444444"]
    revisions = {selected[0]: 9}
    _empty_datasource_selection.return_value = (selected, revisions)

    await create_loop_job(
        db,
        _loop(
            project_id=project_id,
            owner_id=owner_id,
            workspace_backend="virtual",
        ),
        role="scholar",
        iteration=1,
    )

    _empty_datasource_selection.assert_awaited_once_with(
        db,
        owner_id,
        [project_id],
        "virtual",
    )
    kwargs = db.create_job.await_args.kwargs
    assert kwargs["datasource_ids"] == selected
    assert kwargs["datasource_policy_revisions"] == revisions
    assert kwargs["authority_user_id"] == owner_id
    assert kwargs["authority_project_ids"] == [project_id]
    provenance = kwargs["datasource_selection_provenance"]
    assert provenance["origin"] == "default"
    assert provenance["creation_path"] == "project_loop"
    assert provenance["effective_work_owner_id"] == owner_id
    assert provenance["target_project_ids"] == [project_id]
    assert provenance["datasource_ids"] == selected
    assert provenance["policy_revisions"] == revisions
    assert datetime.fromisoformat(provenance["resolved_at"]).tzinfo is not None
    assert "credentials" not in provenance
    db.list_project_datasources.assert_not_awaited()
    db.link_datasource_to_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_loop_default_policy_denial_creates_no_job(_empty_datasource_selection):
    db = _db()
    _empty_datasource_selection.side_effect = RuntimeError("project access revoked")

    with pytest.raises(RuntimeError, match="project access revoked"):
        await create_loop_job(
            db,
            _loop(
                project_id="22222222-2222-2222-2222-222222222222",
                owner_id="33333333-3333-3333-3333-333333333333",
            ),
            role="scholar",
            iteration=1,
        )

    db.create_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_ownerless_loop_materializes_authoritative_empty_selection(
    _empty_datasource_selection,
):
    db = _db()

    await create_loop_job(db, _loop(), role="scholar", iteration=1)

    _empty_datasource_selection.assert_not_awaited()
    kwargs = db.create_job.await_args.kwargs
    assert kwargs["datasource_ids"] == []
    assert kwargs["datasource_policy_revisions"] == {}
    assert kwargs["authority_user_id"] is None
    assert kwargs["authority_project_ids"] is None
    assert kwargs["datasource_selection_provenance"]["origin"] == "explicit"


class TestJobLoopId:
    """job_loop_id detects loop membership from context.loop_id — the guard the
    completion handler uses to exempt loop jobs from the Mode-A diff-review gate
    (which would wedge an unattended loop). knowledge-base/knowledge/features/loop_parallel_stages.md."""

    def test_loop_job_returns_id(self) -> None:
        assert job_loop_id({"context": {"loop_id": "abc-123"}}) == "abc-123"

    def test_json_string_context(self) -> None:
        # The DB layer sometimes hands back context as a JSON string.
        assert job_loop_id({"context": '{"loop_id": "abc-123"}'}) == "abc-123"

    def test_non_loop_job_returns_none(self) -> None:
        assert job_loop_id({"context": {"kickoff_message": "hi"}}) is None

    def test_missing_context_returns_none(self) -> None:
        assert job_loop_id({}) is None

    def test_none_context_returns_none(self) -> None:
        assert job_loop_id({"context": None}) is None

    def test_malformed_json_context_returns_none(self) -> None:
        assert job_loop_id({"context": "{not json"}) is None

    def test_empty_loop_id_returns_none(self) -> None:
        assert job_loop_id({"context": {"loop_id": ""}}) is None


class TestFanOutMemoryAssembler:
    """Fan-out stage members disable the TTL-curation assembler so concurrent
    analysis roles don't race the shared project RecallStore's curation slot
    (knowledge-base/knowledge/features/loop_parallel_stages.md). The lever is the existing
    ``auxiliary.tasks.assemble_memories.enabled`` gate — a scalar deep-merge,
    NOT a rewrite of ``memory.pipeline.writers``."""

    @pytest.mark.asyncio
    async def test_disabled_when_flag_set(self) -> None:
        db = _db()
        await create_loop_job(
            db, _loop(), role="scholar", iteration=2, disable_memory_assembler=True
        )
        co = _config_override(db)
        assert co["auxiliary"]["tasks"]["assemble_memories"]["enabled"] is False

    @pytest.mark.asyncio
    async def test_no_auxiliary_override_by_default(self) -> None:
        # Single-role stages (the default) leave the resolved config untouched —
        # the assembler keeps compounding the KB across sequential iterations.
        db = _db()
        await create_loop_job(db, _loop(), role="scholar", iteration=1)
        assert "auxiliary" not in _config_override(db)

    @pytest.mark.asyncio
    async def test_override_is_scalar_not_writers_list(self) -> None:
        # The disable must NOT clobber the memory.pipeline.writers list (that
        # would drop the append-only extractors too, and drift from defaults).
        db = _db()
        await create_loop_job(
            db, _loop(), role="product-qa", iteration=2, disable_memory_assembler=True
        )
        co = _config_override(db)
        assert "memory" in co and "pipeline" not in co["memory"]

    @pytest.mark.asyncio
    async def test_coexists_with_bare_invariants(self) -> None:
        db = _db()
        await create_loop_job(
            db,
            _loop(model="openrouter/minimax/minimax-m3"),
            role="scholar",
            iteration=2,
            disable_memory_assembler=True,
        )
        co = _config_override(db)
        # The bare lifecycle invariants and the model override survive alongside.
        assert co["auxiliary"]["tasks"]["assemble_memories"]["enabled"] is False
        assert co["memory"] == {"required": True}
        assert co["curator"] == {"enabled": True}
        assert co["llm"] == {"model": "openrouter/minimax/minimax-m3"}


class TestProductQaLoopWiring:
    """Phase 0 wiring for the product-qa role (knowledge-base/knowledge/features/loop_parallel_stages.md):
    the loop must give it a QA-specific kickoff (not the generic default) and the
    Critic must be told to triage QA findings alongside Scholar proposals."""

    def test_product_qa_gets_specific_role_block_not_default(self) -> None:
        kick = build_loop_kickoff(_loop(), role="product-qa", iteration=4)
        assert "YOUR ROLE THIS ITERATION — PRODUCT-QA:" in kick
        # QA-specific, not the "advance the goal acting as 'product-qa'" default.
        assert "advance the goal acting as" not in kick
        # Core QA behaviors: audits shipped product, files findings, doesn't fix.
        # `issue`, not the old `qa-finding` tag (B2: the ticket model reconcile
        # — a `qa-finding`-tagged `plan` note is never in the backlog pool).
        assert "`issue` note" in kick
        assert "qa-finding" not in kick
        low = kick.lower()
        assert "do not fix" in low or "do not fix anything" in low
        assert "scholar" in low  # counterpart framing / be-fair-to-scholar

    def test_product_qa_description_is_specific(self) -> None:
        desc = build_loop_description(_loop(), role="product-qa", iteration=4)
        assert "PRODUCT-QA" in desc
        assert "audit" in desc.lower()

    def test_critic_block_reads_the_injected_pool_not_old_note_types(self) -> None:
        """B1 fix: the critic's candidate set is the injected PROJECT BACKLOG
        pool, not Scholar's old `proposal` notes / Product-QA's old
        `qa-finding` notes — neither type was ever in the pool, so selecting
        one made close_backlog_ticket's index mirror match zero rows
        silently and left the whole second half of the pipeline inert."""
        kick = build_loop_kickoff(_loop(), role="critic", iteration=5)
        low = kick.lower()
        assert "backlog pool" in low
        assert "first-class" in low  # fix-vs-build is still a first-class choice
        # The old, now-inert candidate universe must not survive the rewrite.
        assert "qa-finding" not in kick
        assert "proposal" not in low

    def test_critic_block_initiative_is_the_ticket_not_the_verdict_note(self) -> None:
        """The other half of B1: the verdict/decision note is a rationale
        record, never the thing selected — the note_id that becomes a
        campaign's initiative must be the chosen ticket's own id."""
        kick = build_loop_kickoff(_loop(), role="critic", iteration=5)
        assert "verdict" in kick
        assert (
            "NOT the ticket itself" in kick or "not the ticket itself" in kick.lower()
        )

    def test_critic_block_does_not_mass_supersede_tickets(self) -> None:
        """B2's second contradiction: 'mark every non-selected candidate...
        superseded' would drain the entire pool every turn once feature/
        issue/idea notes ARE the pool (superseded != active). The critic
        must be told NOT to do that merely for not being picked this turn."""
        kick = build_loop_kickoff(_loop(), role="critic", iteration=5)
        low = kick.lower()
        assert "do not flip it to `superseded`" in low or (
            "not flip it to `superseded`" in low
        )
        assert "genuine duplicate" in low


class TestCategoryContractComposition:
    """B5 of knowledge-base/knowledge/features/officer_backlog_pools.md §7.

    The loop's kickoff now leads with the work-category contract and follows
    with the role's identity. This is the half of the pools feature that
    reaches EVERY loop, officer or not — it is what stops the next month of
    iterations from picking whatever is easiest to prove.
    """

    def test_scholar_leads_with_the_researcher_contract(self):
        kick = build_loop_kickoff(_loop(), role="scholar", iteration=3)
        role_section = kick.split("YOUR ROLE THIS ITERATION — SCHOLAR:")[1]
        assert role_section.lstrip().startswith("Your deliverable is an ANSWER")
        # Spike discipline and design anchors now reach a plain loop.
        assert "SPIKE" in role_section
        assert "Material 3" in role_section and "WCAG" in role_section
        # ...and the scholar identity still follows it.
        assert "Do NOT self-filter" in role_section

    def test_product_qa_leads_with_the_tester_contract(self):
        kick = build_loop_kickoff(_loop(), role="product-qa", iteration=4)
        role_section = kick.split("YOUR ROLE THIS ITERATION — PRODUCT-QA:")[1]
        assert role_section.lstrip().startswith("Your output is ISSUE TICKETS")
        # The anchored-critique upgrade, previously officer-only.
        assert "Nielsen" in role_section and "SC 2.5.8" in role_section
        assert "Scholar's counterpart" in role_section

    def test_developer_leads_with_the_executor_contract(self):
        kick = build_loop_kickoff(_loop(), role="developer", iteration=6)
        role_section = kick.split("YOUR ROLE THIS ITERATION — DEVELOPER:")[1]
        assert role_section.lstrip().startswith("Ship it, prove it appropriately")
        assert "regression rails, never the score" in role_section
        assert "Implement the Critic's chosen action" in role_section

    def test_the_critic_gets_no_category_contract(self):
        # role_to_category("critic") is `tester` — right for the expert, wrong
        # here. The loop's critic SELECTS; prepending the tester contract would
        # tell it to file 3-7 issue tickets and contradict its duty on the same
        # screen. Categories describe work; selection is orchestration.
        kick = build_loop_kickoff(_loop(), role="critic", iteration=5)
        role_section = kick.split("YOUR ROLE THIS ITERATION — CRITIC:")[1]
        assert role_section.lstrip().startswith("You are the OVERSEER")
        assert "3-7 findings MAXIMUM" not in role_section

    def test_an_unknown_execution_role_still_gets_the_executor_contract(self):
        # The execution slot is swappable (writer / default / anything).
        kick = build_loop_kickoff(_loop(), role="writer", iteration=2)
        assert "Ship it, prove it appropriately" in kick

    def test_the_contract_text_is_context_free(self):
        # The blocks are shared with the officer's ticket dispatches. A loop
        # iteration has no ticket, so wording that says "this ticket" would be
        # a small lie on every loop kickoff.
        kick = build_loop_kickoff(_loop(), role="scholar", iteration=3)
        assert "This ticket is a SPIKE" not in kick
        assert "deliver THIS ticket" not in kick


class TestEvidenceIncentiveWording:
    """The two edits that repair the loop's own incentives (§7).

    Neither phrase was pinned before, because neither existed — the loop asked
    for a "verifiable" increment and scored tickets on "evidence quality",
    which is exactly how a month of iterations produced tested Python and no
    UI. These pins exist so the wording cannot quietly regress.
    """

    def test_the_increment_asks_for_evidence_appropriate_to_the_work(self):
        kick = build_loop_kickoff(_loop(), role="developer", iteration=1)
        assert "EVIDENCE APPROPRIATE TO THE WORK" in kick
        assert "screenshots for UI" in kick
        assert "citations for research" in kick
        # The old wording made unverifiable work look like bad work.
        assert "ONE solid, verifiable increment" not in kick

    def test_the_increment_names_the_bias_it_is_correcting(self):
        kick = build_loop_kickoff(_loop(), role="scholar", iteration=1)
        assert "Do not pick the work whose evidence is easiest to produce" in kick

    def test_the_critic_rubric_prices_evidence_by_the_claim(self):
        kick = build_loop_kickoff(_loop(), role="critic", iteration=5)
        assert "evidence APPROPRIATE TO THE CLAIM" in kick
        assert (
            "A ticket is not weaker because its evidence would be a screenshot" in kick
        )
        assert "and evidence quality." not in kick


class TestNormalizeStage:
    """The parallel-stage grammar: a role_sequence entry is a single role name
    (one-job stage) or a list of role names (fan-out). knowledge-base/knowledge/features/loop_parallel_stages.md."""

    def test_single_role_string(self) -> None:
        assert normalize_stage("scholar") == ["scholar"]

    def test_strips_whitespace(self) -> None:
        assert normalize_stage("  scholar  ") == ["scholar"]

    def test_list_of_roles(self) -> None:
        assert normalize_stage(["scholar", "product-qa"]) == ["scholar", "product-qa"]

    def test_dedupes_within_stage_preserving_order(self) -> None:
        # Two jobs of the same role would collide on branch name / KB slot.
        assert normalize_stage(["scholar", "scholar", "product-qa"]) == [
            "scholar",
            "product-qa",
        ]

    def test_tuple_accepted(self) -> None:
        assert normalize_stage(("scholar", "product-qa")) == ["scholar", "product-qa"]

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError):
            normalize_stage("   ")

    def test_empty_list_raises(self) -> None:
        with pytest.raises(ValueError):
            normalize_stage([])

    def test_list_of_blanks_raises(self) -> None:
        with pytest.raises(ValueError):
            normalize_stage(["", "  "])

    def test_wrong_type_raises(self) -> None:
        with pytest.raises(ValueError):
            normalize_stage(42)


class TestValidateRoleSequence:
    """Grammar validation at the start boundary: fan-out stages are analysis-only."""

    def test_single_role_sequence_ok(self) -> None:
        validate_role_sequence(["scholar", "critic", "developer"])

    def test_parallel_analysis_stage_ok(self) -> None:
        validate_role_sequence([["scholar", "product-qa"], "critic", "developer"])

    def test_single_execution_role_ok(self) -> None:
        # A lone execution role is fine — only a *fan-out* may not contain one.
        validate_role_sequence(["developer"])

    def test_empty_sequence_raises(self) -> None:
        with pytest.raises(ValueError):
            validate_role_sequence([])

    def test_execution_role_in_fan_out_raises(self) -> None:
        # developer commits to `main`; two concurrent committers race the merge.
        with pytest.raises(ValueError, match="analysis roles"):
            validate_role_sequence([["scholar", "developer"], "critic"])

    def test_malformed_entry_raises(self) -> None:
        with pytest.raises(ValueError):
            validate_role_sequence(["scholar", []])


class TestLoopCounterStamps:
    """create_loop_job stamps loop_seq_index / loop_remaining into a spawned
    job's context (spawn-time truth the torn-advance sweeper reads back)."""

    @pytest.mark.asyncio
    async def test_stamps_present_when_seq_index_given(self) -> None:
        db = _db()
        await create_loop_job(
            db,
            _loop(),
            role="scholar",
            iteration=2,
            seq_index=0,
            remaining_iterations=7,
        )
        ctx = _context(db)
        assert ctx["loop_seq_index"] == 0
        assert ctx["loop_remaining"] == 7
        assert ctx["loop_iteration"] == 2
        assert ctx["loop_role"] == "scholar"

    @pytest.mark.asyncio
    async def test_remaining_none_is_stamped(self) -> None:
        # Deadline-only loop: remaining is genuinely None, and that's recorded
        # (the sweeper uses loop_seq_index's presence as the "stamped" sentinel).
        db = _db()
        await create_loop_job(
            db,
            _loop(),
            role="critic",
            iteration=3,
            seq_index=1,
            remaining_iterations=None,
        )
        ctx = _context(db)
        assert ctx["loop_seq_index"] == 1
        assert ctx["loop_remaining"] is None

    @pytest.mark.asyncio
    async def test_no_stamps_when_seq_index_absent(self) -> None:
        # Legacy call path (no seq_index) stays stamp-free → sweeper falls back
        # to modulo derivation.
        db = _db()
        await create_loop_job(db, _loop(), role="scholar", iteration=1)
        ctx = _context(db)
        assert "loop_seq_index" not in ctx
        assert "loop_remaining" not in ctx


# =============================================================================
# extract_cooldown_reset_at (knowledge-base/knowledge/issues/loop_advances_into_active_model_cooldown.md)
# =============================================================================


class TestExtractCooldownResetAt:
    def test_in_flight_dict_error_wins(self):
        result = {"error": {"classification": "cooldown", "reset_at": 1785412444.0}}
        job = {"error_details": {"classification": "cooldown", "reset_at": 1.0}}
        assert extract_cooldown_reset_at(job, result) == 1785412444.0

    def test_non_cooldown_in_flight_falls_back_to_row(self):
        result = {"error": {"message": "boom", "type": "llm_error"}}
        job = {"error_details": {"classification": "cooldown", "reset_at": 42.0}}
        assert extract_cooldown_reset_at(job, result) == 42.0

    def test_row_error_details_as_json_string(self):
        job = {
            "error_details": '{"classification": "cooldown", "reset_at": 1785412444}'
        }
        assert extract_cooldown_reset_at(job, {}) == 1785412444.0

    def test_heal_shape_empty_result_reads_row(self):
        job = {"error_details": {"classification": "cooldown", "reset_at": 7.5}}
        assert extract_cooldown_reset_at(job, {}) == 7.5

    def test_both_absent_returns_none(self):
        assert extract_cooldown_reset_at({}, {}) is None
        assert extract_cooldown_reset_at(None, None) is None

    def test_non_cooldown_classification_returns_none(self):
        job = {"error_details": {"classification": "transient", "reset_at": 5.0}}
        assert extract_cooldown_reset_at(job, {}) is None

    def test_missing_or_bad_reset_at_returns_none(self):
        assert (
            extract_cooldown_reset_at(
                {"error_details": {"classification": "cooldown"}}, {}
            )
            is None
        )
        job = {"error_details": {"classification": "cooldown", "reset_at": "soon"}}
        assert extract_cooldown_reset_at(job, {}) is None

    def test_malformed_json_string_returns_none(self):
        assert extract_cooldown_reset_at({"error_details": "not json{"}, {}) is None


# =============================================================================
# Born-parked creation (knowledge-base/knowledge/issues/loop_advances_into_active_model_cooldown.md)
# =============================================================================


PARK_UNTIL = datetime(2026, 7, 30, 11, 54, 4, tzinfo=timezone.utc)


class TestBornParkedCreation:
    @pytest.mark.asyncio
    async def test_park_until_creates_born_parked_row(self):
        db = _db()
        await create_loop_job(
            db,
            _loop(model="gpt-5.3-codex-spark"),
            role="critic",
            iteration=4,
            park_until=PARK_UNTIL,
        )
        kwargs = db.create_job.call_args.kwargs
        assert kwargs["status"] == "paused"
        fd = kwargs["freeze_data"]
        assert fd["freeze_type"] == "llm_unavailable"
        assert fd["classification"] == "cooldown"
        assert fd["next_retry_at"] == PARK_UNTIL.isoformat()
        assert fd["attempt"] == 0
        assert fd["model"] == "gpt-5.3-codex-spark"
        ctx = kwargs["context"]
        assert ctx["llm_outage"] == {
            "attempt": 0,
            "next_retry_at": PARK_UNTIL.isoformat(),
        }
        # The ceiling landmine: first_failed_at at creation time would make
        # evaluate_llm_outage read the whole park as elapsed outage and kill
        # the member AT WAKE. It must be absent.
        assert "first_failed_at" not in ctx["llm_outage"]

    @pytest.mark.asyncio
    async def test_without_park_is_unchanged(self):
        db = _db()
        await create_loop_job(db, _loop(), role="scholar", iteration=1)
        kwargs = db.create_job.call_args.kwargs
        assert kwargs["status"] == "created"
        assert kwargs["freeze_data"] is None
        assert "llm_outage" not in kwargs["context"]

    @pytest.mark.asyncio
    async def test_park_context_wins_over_extra_context(self):
        db = _db()
        await create_loop_job(
            db,
            _loop(),
            role="critic",
            iteration=2,
            extra_context={"llm_outage": {"attempt": 9}},
            park_until=PARK_UNTIL,
        )
        ctx = db.create_job.call_args.kwargs["context"]
        assert ctx["llm_outage"] == {
            "attempt": 0,
            "next_retry_at": PARK_UNTIL.isoformat(),
        }


class TestHistoryNamesTheDelivery:
    """What the *next* iteration is told about the last one.

    The kickoff history is the loop's memory. When code compounds into a
    source repository, every record legitimately reads
    ``delivery=no-changes``; if that is all the next developer sees, the
    loop's own history testifies that nothing has ever been delivered — the
    exact false signal that made the 08-06 run compound nothing.
    """

    def _record(self, changes: object) -> dict:
        return {
            "iteration": 3,
            "role": "developer",
            "job_id": "29c28492-1111-2222-3333-444444444444",
            "status": "completed",
            "delivery_status": "no-changes",
            "completion_notes": "Shipped the theme studies.",
            "changes": changes,
        }

    _PR_CHANGE = {
        "datasource": "Knaeckebrothero/KurortEngine",
        "kind": "pull_request",
        "action": "open",
        "ref": "https://github.com/Knaeckebrothero/KurortEngine/pull/1",
        "summary": "github PR #1: design/hotel-rheinland-theme → main",
        "verified": True,
    }

    def test_pull_request_is_named_in_the_history_line(self) -> None:
        rendered = project_loops_service.render_loop_job_history(
            [self._record([self._PR_CHANGE])]
        )
        assert "#1" in rendered
        assert "design/hotel-rheinland-theme" in rendered

    def test_jsonb_string_changes_are_parsed(self) -> None:
        import json

        rendered = project_loops_service.render_loop_job_history(
            [self._record(json.dumps([self._PR_CHANGE]))]
        )
        assert "#1" in rendered

    def test_unverified_claim_is_not_rendered_as_delivery(self) -> None:
        claim = {**self._PR_CHANGE, "verified": False}
        rendered = project_loops_service.render_loop_job_history(
            [self._record([claim])]
        )
        assert "#1" not in rendered

    def test_record_without_a_pull_request_is_unchanged(self) -> None:
        rendered = project_loops_service.render_loop_job_history([self._record([])])
        assert rendered.rstrip().endswith("Shipped the theme studies.")


class TestNextStageIndex:
    """Rotation must not step past a turn that produced nothing.

    knowledge-base/knowledge/features/better_resavio_restart_status.md §6c. A turn in which every
    member failed leaves the next role nothing new to work from. The canonical
    case is a failed critic: the developer runs anyway and builds on the
    previous iteration's verdict as though it were fresh, because the engine
    deliberately cannot read verdicts (they live in KB notes).

    Re-running the stage is bounded by the loop's existing
    consecutive-failure stop, so this cannot spin.
    """

    def test_a_successful_turn_advances(self) -> None:
        assert project_loops_service.next_stage_index(
            seq_index_completed=0, stage_count=3, turn_all_failed=False
        ) == (1, False)

    def test_the_last_stage_wraps_the_cycle(self) -> None:
        assert project_loops_service.next_stage_index(
            seq_index_completed=2, stage_count=3, turn_all_failed=False
        ) == (0, True)

    def test_a_fully_failed_turn_reruns_its_own_stage(self) -> None:
        assert project_loops_service.next_stage_index(
            seq_index_completed=1, stage_count=3, turn_all_failed=True
        ) == (1, False)

    def test_a_failed_last_stage_does_not_wrap(self) -> None:
        """Retrying stage 2 is not a completed cycle.

        Reporting a wrap here would tick the KB convergence TTL a second
        time for a cycle that never finished, ageing notes toward
        re-verification on the strength of a failure.
        """
        assert project_loops_service.next_stage_index(
            seq_index_completed=2, stage_count=3, turn_all_failed=True
        ) == (2, False)

    def test_single_stage_loop_wraps_on_success(self) -> None:
        assert project_loops_service.next_stage_index(
            seq_index_completed=0, stage_count=1, turn_all_failed=False
        ) == (0, True)

    def test_single_stage_loop_failure_is_a_retry_not_a_wrap(self) -> None:
        assert project_loops_service.next_stage_index(
            seq_index_completed=0, stage_count=1, turn_all_failed=True
        ) == (0, False)

    def test_degenerate_stage_count_does_not_raise(self) -> None:
        """A malformed loop row must not take the advance down with a
        ZeroDivisionError — the rotate is the only thing that keeps a
        running loop moving."""
        assert project_loops_service.next_stage_index(
            seq_index_completed=0, stage_count=0, turn_all_failed=False
        ) == (0, False)


@pytest.mark.asyncio
async def test_a_refused_workspace_default_stops_the_loop_with_its_message(
    monkeypatch,
):
    """Slice A2b, spec §7: the refusal text reaches the loop's last_error."""
    from unittest.mock import MagicMock

    from fastapi import HTTPException

    from orchestrator.services.project_loop_advance import rotate_loop_to_next_stage
    from orchestrator.services.project_loop_spawn import ProjectLoopDependencies

    message = "This Project's container template 'gone' no longer exists."
    monkeypatch.setattr(
        "orchestrator.services.manifest_workspace_selection."
        "select_project_workspace_default",
        AsyncMock(side_effect=HTTPException(409, message)),
    )
    owner = "33333333-3333-3333-3333-333333333333"
    db = _db()
    db.get_user = AsyncMock(return_value={"id": owner})
    db.user_can_run_unattended_operations = AsyncMock(return_value=True)
    dependencies = ProjectLoopDependencies(
        store=db,
        vector_store=None,
        notifier=AsyncMock(),
        gitea_client=MagicMock(),
        main_cloud_router=MagicMock(),
        trigger_dispatch=MagicMock(),
        kick_officer_event_drain=MagicMock(),
        enforce_dispatch_grants=AsyncMock(),
        reindex_project_kb=AsyncMock(),
        completion_commands_enabled=lambda: False,
        completion_sweep_router=MagicMock(),
    )
    actions: list[str] = []
    await rotate_loop_to_next_stage(
        _loop(
            project_id="22222222-2222-2222-2222-222222222222",
            owner_id=owner,
            role_sequence=["scholar", "developer"],
        ),
        seq_index_completed=0,
        base_total=1,
        next_remaining=4,
        consecutive=0,
        last_error=None,
        actions=actions,
        dependencies=dependencies,
    )
    db.create_job.assert_not_awaited()
    stopped = db.update_project_loop.call_args.kwargs
    assert stopped["status"] == "failed"
    assert stopped["last_error"] == f"spawn failed: {message}"
