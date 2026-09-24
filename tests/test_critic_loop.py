"""Unit tests for the critic feedback loop redesign.

Tests:
- Deferred verdict storage (_verdict_data accessors)
- _finalize_with_verdict produces correct freeze_data and status
- handle_transition uses freeze_data.status
- Round limit enforcement in _handle_critic_verdict
- Evaluation tools store verdict + final_phase_data
"""

import json
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

# Add project root to path
project_root = Path(__file__).parent.parent

from agent.tools.evaluation.evaluation_tools import (  # noqa: E402
    _verdict_data,
    get_verdict_data,
    clear_verdict_data,
    EVALUATION_TOOLS_METADATA,
    create_evaluation_tools,
)
from agent.core.phase import (  # noqa: E402
    _finalize_with_verdict,
    TransitionResult,
)


# =============================================================================
# Fixtures
# =============================================================================


def make_state(job_id: str = "critic-job-1", phase_number: int = 2) -> dict:
    """Create a minimal agent state dict for a critic job."""
    return {
        "job_id": job_id,
        "phase_number": phase_number,
        "is_strategic_phase": True,
        "is_final_phase": False,
        "messages": [],
        "metadata": {
            "verification_target": "target-job-1",
            "original_config": "developer",
        },
    }


def make_workspace():
    """Create a mock WorkspaceManager."""
    ws = MagicMock()
    ws.write_file = MagicMock()
    ws.git_manager = None
    # An unconfigured MagicMock() return value breaks json.dumps in
    # finalize_job's normal (non-verdict) completion path, which always
    # reads these for freeze_data["head_commit"] / ["content_tree"] — a real
    # WorkspaceManager returns str | None.
    ws.get_head_commit = MagicMock(return_value=None)
    ws.get_content_tree = MagicMock(return_value=None)
    return ws


def make_todo_manager():
    """Create a mock TodoManager."""
    tm = MagicMock()
    tm.archive = MagicMock()
    return tm


# =============================================================================
# Deferred verdict storage tests
# =============================================================================


class TestVerdictData:
    """Tests for _verdict_data accessors."""

    def setup_method(self):
        """Clear verdict data before each test."""
        _verdict_data.clear()

    def test_get_verdict_data_empty(self):
        assert get_verdict_data("nonexistent") is None

    def test_set_and_get_verdict_data(self):
        _verdict_data["job-1"] = {"_verdict": "approved", "_target_job_id": "target-1"}
        result = get_verdict_data("job-1")
        assert result is not None
        assert result["_verdict"] == "approved"
        assert result["_target_job_id"] == "target-1"

    def test_clear_verdict_data(self):
        _verdict_data["job-1"] = {"_verdict": "returned"}
        clear_verdict_data("job-1")
        assert get_verdict_data("job-1") is None

    def test_clear_nonexistent_is_safe(self):
        clear_verdict_data("nonexistent")  # Should not raise

    def test_multiple_jobs(self):
        _verdict_data["job-1"] = {"_verdict": "approved"}
        _verdict_data["job-2"] = {"_verdict": "returned"}
        assert get_verdict_data("job-1")["_verdict"] == "approved"
        assert get_verdict_data("job-2")["_verdict"] == "returned"
        clear_verdict_data("job-1")
        assert get_verdict_data("job-1") is None
        assert get_verdict_data("job-2") is not None


# =============================================================================
# _finalize_with_verdict tests
# =============================================================================


class TestFinalizeWithVerdict:
    """Tests for _finalize_with_verdict.

    ``verdict`` here is shaped exactly like the module-level mirror
    ``evaluation_tools._verdict_data`` actually populates (Task 5):
    ``_verdict``, ``_target_job_id``, ``round``, ``open_findings`` — NOT the
    older ``report``/``strengths``/``minor_notes``/``_feedback``/``issues``/
    ``severity`` shape, which nothing writes any more (verified by reading
    ``_submit_verdict`` in src/tools/evaluation/evaluation_tools.py). Task 8
    reconciled ``_finalize_with_verdict`` to read the real shape so the
    freeze no longer renders those fields blank.
    """

    def test_approved_verdict(self):
        state = make_state()
        ws = make_workspace()
        tm = make_todo_manager()

        verdict = {
            "_verdict": "approved",
            "_target_job_id": "target-job-1",
            "round": 2,
            "open_findings": [],
        }

        result = _finalize_with_verdict(state, ws, tm, verdict, "critic-job-1")

        assert isinstance(result, TransitionResult)
        assert result.success is True
        assert result.state_updates["should_stop"] is True
        assert result.state_updates["goal_achieved"] is True
        assert result.freeze_data["status"] == "completed"
        assert result.freeze_data["freeze_type"] == "verdict"
        assert result.freeze_data["verdict"] == "approved"
        assert result.freeze_data["target_job_id"] == "target-job-1"
        assert result.freeze_data["round"] == 2
        assert result.freeze_data["open_findings"] == []

        # Should write critic_verdict.json
        ws.write_file.assert_called_once()
        call_args = ws.write_file.call_args
        assert call_args[0][0] == "output/critic_verdict.json"

        # Should archive todos
        tm.archive.assert_called_once_with("final")

    def test_returned_verdict(self):
        state = make_state()
        ws = make_workspace()
        tm = make_todo_manager()

        verdict = {
            "_verdict": "returned",
            "_target_job_id": "target-job-1",
            "round": 1,
            "open_findings": [
                {
                    "id": "F1",
                    "severity": "high",
                    "claim": "Test coverage too low",
                    "opened_round": 1,
                },
                {
                    "id": "F2",
                    "severity": "medium",
                    "claim": "Missing edge cases",
                    "opened_round": 1,
                },
            ],
        }

        result = _finalize_with_verdict(state, ws, tm, verdict, "critic-job-1")

        assert result.success is True
        assert result.state_updates["should_stop"] is True
        assert result.state_updates["goal_achieved"] is False
        # Critics no longer park in 'waiting' between rounds (Task 8): a
        # fresh critic is spawned every round from the ledger
        # (_trigger_verification_on_complete), so a 'returned' critic has
        # nothing left to wait FOR. determine_job_status reads freeze_data
        # ["status"] directly as the CRITIC's own DB status — this is
        # unrelated to the target job's status, which
        # _handle_critic_verdict_on_complete resolves separately from the
        # ledger in orchestrator/main.py.
        assert result.freeze_data["status"] == "completed"
        assert result.freeze_data["freeze_type"] == "verdict"
        assert result.freeze_data["verdict"] == "returned"
        assert result.freeze_data["target_job_id"] == "target-job-1"
        assert result.freeze_data["round"] == 1
        assert len(result.freeze_data["open_findings"]) == 2
        assert result.freeze_data["open_findings"][0]["id"] == "F1"

    def test_unknown_verdict_defaults_to_completed(self):
        state = make_state()
        ws = make_workspace()
        tm = make_todo_manager()

        verdict = {
            "_verdict": "unknown_type",
            "_target_job_id": "target-job-1",
        }

        result = _finalize_with_verdict(state, ws, tm, verdict, "critic-job-1")

        assert result.success is True
        assert result.state_updates["goal_achieved"] is True
        assert result.freeze_data["status"] == "completed"


# =============================================================================
# finalize_job: no implicit approval when a critic reaches finalize_job
# without ever calling approve_job_verdict/return_job_with_feedback (Task 8).
# =============================================================================


class TestFinalizeJobNoImplicitApproval:
    """A critic that calls job_complete instead of a verdict tool must NOT be
    read as an implicit approval (CWE-636: a missing verdict is not consent).

    Previously this synthesized ``{"_verdict": "approved", ...}`` and froze
    the critic exactly as if it had called approve_job_verdict. The fix logs a
    refusal and falls through to the ordinary (non-verdict) completion path
    — no 'verdict' key anywhere in freeze_data — so the orchestrator's
    ``_resolve_critic_outcome`` (a fresh ledger lookup on the TARGET, keyed
    by this critic_job_id) finds no round and escalates instead of
    advancing the target.
    """

    def setup_method(self):
        _verdict_data.clear()

    def test_critic_without_verdict_does_not_synthesize_approval(self, caplog):
        import logging

        from agent.core.phase import finalize_job

        state = make_state()  # metadata.verification_target == "target-job-1"
        ws = make_workspace()
        tm = make_todo_manager()
        config = MagicMock(autonomy="full")  # critics always run autonomy=full

        with caplog.at_level(logging.ERROR):
            result = finalize_job(state, ws, tm, config=config)

        # The crux of the fix: no verdict anywhere in the freeze.
        assert result.freeze_data.get("verdict") is None
        assert result.freeze_data.get("status") != "waiting"
        assert "approved" not in json.dumps(result.freeze_data).lower()

        # A human operator can see WHY nothing was recorded.
        messages = [rec.message for rec in caplog.records]
        assert any("WITHOUT a recorded verdict" in m for m in messages)
        assert any("target-job-1" in m for m in messages)

    def test_critic_without_verdict_still_completes_its_own_job(self):
        """The critic's own job must still resolve (not hang) — only the
        TARGET's advancement is refused, not the critic's own completion."""
        from agent.core.phase import finalize_job

        state = make_state()
        ws = make_workspace()
        tm = make_todo_manager()
        config = MagicMock(autonomy="full")

        result = finalize_job(state, ws, tm, config=config)

        assert result.success is True
        assert result.state_updates["should_stop"] is True


# =============================================================================
# handle_transition freeze_data.status tests
# =============================================================================


class TestHandleTransitionFreezeDataStatus:
    """Tests that handle_transition respects freeze_data.status."""

    @pytest.mark.asyncio
    async def test_freeze_data_status_completed(self):
        """When freeze_data has status='completed', DB should get 'completed'."""
        from agent.core.phase import TransitionResult

        mock_db = MagicMock()
        mock_db.jobs = MagicMock()
        mock_db.jobs.update_status = AsyncMock()
        mock_db.execute = AsyncMock()

        result = TransitionResult(
            success=True,
            state_updates={"should_stop": True, "goal_achieved": True},
            freeze_data={"status": "completed", "freeze_type": "verdict"},
        )

        # Simulate what handle_transition does
        if result.freeze_data and result.freeze_data.get("status"):
            db_status = result.freeze_data["status"]
            if db_status == "job_completed":
                db_status = "completed"
        elif result.state_updates.get("goal_achieved"):
            db_status = "completed"
        else:
            db_status = "pending_review"

        assert db_status == "completed"

    @pytest.mark.asyncio
    async def test_freeze_data_status_waiting(self):
        """When freeze_data has status='waiting', DB should get 'waiting'."""
        result = TransitionResult(
            success=True,
            state_updates={"should_stop": True, "goal_achieved": False},
            freeze_data={"status": "waiting", "freeze_type": "verdict"},
        )

        if result.freeze_data and result.freeze_data.get("status"):
            db_status = result.freeze_data["status"]
        elif result.state_updates.get("goal_achieved"):
            db_status = "completed"
        else:
            db_status = "pending_review"

        assert db_status == "waiting"

    @pytest.mark.asyncio
    async def test_freeze_data_status_job_completed_normalized(self):
        """Synonym 'job_completed' should normalize to 'completed'."""
        result = TransitionResult(
            success=True,
            state_updates={"should_stop": True, "goal_achieved": True},
            freeze_data={"status": "job_completed"},
        )

        if result.freeze_data and result.freeze_data.get("status"):
            db_status = result.freeze_data["status"]
            if db_status == "job_completed":
                db_status = "completed"
        else:
            db_status = "pending_review"

        assert db_status == "completed"

    @pytest.mark.asyncio
    async def test_no_freeze_data_falls_through(self):
        """Without freeze_data.status, use goal_achieved logic."""
        # goal_achieved=True → completed
        result = TransitionResult(
            success=True,
            state_updates={"should_stop": True, "goal_achieved": True},
            freeze_data=None,
        )

        if result.freeze_data and result.freeze_data.get("status"):
            db_status = result.freeze_data["status"]
        elif result.state_updates.get("goal_achieved"):
            db_status = "completed"
        else:
            db_status = "pending_review"

        assert db_status == "completed"

        # goal_achieved=False → pending_review
        result2 = TransitionResult(
            success=True,
            state_updates={"should_stop": True, "goal_achieved": False},
            freeze_data={"freeze_type": "phase_boundary"},  # No status key
        )

        if result2.freeze_data and result2.freeze_data.get("status"):
            db_status2 = result2.freeze_data["status"]
        elif result2.state_updates.get("goal_achieved"):
            db_status2 = "completed"
        else:
            db_status2 = "pending_review"

        assert db_status2 == "pending_review"


# =============================================================================
# Evaluation tool metadata tests
# =============================================================================


class TestEvaluationToolMetadata:
    """Tests for evaluation tool registry metadata."""

    def test_approve_job_verdict_is_strategic_only(self):
        assert EVALUATION_TOOLS_METADATA["approve_job_verdict"]["phases"] == [
            "strategic"
        ]

    def test_return_job_is_strategic_only(self):
        assert EVALUATION_TOOLS_METADATA["return_job_with_feedback"]["phases"] == [
            "strategic"
        ]


# =============================================================================
# Critic strategic-prompt verdict-timing regression
# =============================================================================
#
# Guards against the 2026-06-03 deadlock (job 8a3fc7d1): the verdict tools
# approve_job_verdict / return_job_with_feedback are strategic-only (asserted above),
# but the critic strategic prompt told the agent "Do NOT render verdicts during
# strategic phases" — leaving no phase where the agent could both call the tool
# AND believe it was allowed to. The agent looped forever via todo_rewind.
# See knowledge-base/knowledge/tests/critic_verdict_deadlock_verification.md.

CRITIC_PROMPT_DIR = project_root / "config" / "experts" / "critic"


class TestCriticStrategicPromptVerdictTiming:
    """The critic strategic prompt must agree with the strategic-only verdict tools."""

    @pytest.mark.parametrize("fname", ["skills/strategic-phase/SKILL.md"])
    def test_prompt_does_not_forbid_strategic_verdict(self, fname):
        text = (CRITIC_PROMPT_DIR / fname).read_text(encoding="utf-8")
        # The exact sentence that caused the 8a3fc7d1 deadlock.
        assert "Do NOT render verdicts during strategic phases" not in text, (
            f"{fname} forbids strategic verdicts, but approve_job_verdict/"
            "return_job_with_feedback are strategic-only — this deadlocks the critic."
        )

    @pytest.mark.parametrize("fname", ["skills/strategic-phase/SKILL.md"])
    def test_prompt_directs_verdict_into_strategic(self, fname):
        text = (CRITIC_PROMPT_DIR / fname).read_text(encoding="utf-8")
        assert "approve_job_verdict" in text
        # The fix tells the agent these tools are strategic-only and must not be
        # deferred to a tactical todo.
        assert "strategic-phase-only" in text, (
            f"{fname} lost the directive that the verdict tools are strategic-only."
        )


# =============================================================================
# Critic verdict-tool / template agreement (Task 7)
# =============================================================================
#
# Same failure MODE as the deadlock above, different mechanism: instead of a
# phase-timing contradiction, the §4 "Render Your Verdict" section in
# verification_instructions.md kept documenting the RETIRED
# `return_job_with_feedback(..., issues=[...], severity="...")` call shape
# after Task 5 replaced it with `findings=[...]` / `dispositions=[...]`. A
# stale doc doesn't crash the tool call the way the phase contradiction did,
# but it reliably mis-teaches the model the wrong verdict-tool shape. These
# assertions are tied to the LIVE tool signatures (not a hardcoded string),
# so a future signature change that forgets to update the prompt fails here
# instead of drifting silently again.

VERIFICATION_INSTRUCTIONS_PATH = CRITIC_PROMPT_DIR / "verification_instructions.md"


class TestCriticVerdictInstructionsToolAgreement:
    """§4 of verification_instructions.md must match the REAL verdict tools."""

    def test_stale_verdict_shape_is_gone(self):
        text = VERIFICATION_INSTRUCTIONS_PATH.read_text(encoding="utf-8")
        assert "issues=" not in text, (
            "verification_instructions.md still documents the retired "
            'issues=[...] / severity="..." call shape that Task 5 replaced.'
        )

    def test_verdict_examples_use_real_tool_params(self):
        text = VERIFICATION_INSTRUCTIONS_PATH.read_text(encoding="utf-8")
        for tool_fn in create_evaluation_tools(MagicMock()):
            assert f"{tool_fn.name}(" in text, (
                f"verification_instructions.md never calls `{tool_fn.name}`."
            )
            for param in tool_fn.args:
                assert f"{param}=" in text, (
                    f"verification_instructions.md never shows "
                    f"`{tool_fn.name}`'s real `{param}=` parameter — the "
                    "prompt has drifted from the tool surface."
                )

    def test_verification_requires_independent_verifier_evidence(self):
        text = VERIFICATION_INSTRUCTIONS_PATH.read_text(encoding="utf-8")
        assert "MUST delegate one independent evidence pass" in text
        assert '`subagent_type="verifier"`' in text
        assert '`isolation="shared"`' in text
        assert "`run_in_background=false`" in text
        assert "Call `delegate_agent` in a turn by itself" in text
        assert "The verifier gathers evidence only" in text
        assert "YOU still inspect the work" in text


# =============================================================================
# Round limit enforcement tests
# =============================================================================
#
# The round cap used to be enforced agent-side, in `_handle_critic_verdict`
# (src/api/app.py) — see complete_job's docstring in orchestrator/main.py:
# "This replaces the agent-side `_update_job_status_from_result`,
# `_handle_critic_verdict`, and `_maybe_trigger_verification` functions."
# That function no longer exists (confirmed: it's absent from the whole
# tree, only mentioned in that historical docstring). A round-cap branch
# briefly lived on the orchestrator side too, in
# `_handle_critic_verdict_on_complete`, but Task 8 removed it entirely — the
# cap is now enforced exactly once, in `_verification_gate_decision`
# (orchestrator/main.py), read fresh from the target's resolved_config on
# every round. These tests were skipped with a note that the logic "moved to
# orchestrator" and never re-targeted there; this rewrites them against the
# real function.


def _rounds_with_open_blocking(n: int) -> list:
    """``n`` prior rounds, each returned with a distinct open high-severity
    finding and a distinct content_tree.

    Distinct per-round content hashes matter: ``_verification_gate_decision``
    checks no-progress (same content as the immediately preceding round)
    BEFORE the round cap, and the two are mutually exclusive branches. A
    decision made against yet another new content_tree (see call sites below)
    exercises the CAP branch specifically, not the no-progress branch already
    covered by ``TestVerificationGateDecision`` in
    tests/test_verification_flow.py.
    """
    return [
        {
            "round": i,
            "critic_job_id": f"c{i}",
            "content_tree": f"h{i}",
            "verdict": "returned",
            "asserted_verdict": "returned",
            "opened": [{"id": f"F{i}", "severity": "high", "claim": "still broken"}],
            "dispositions": [],
            "ts": "t",
        }
        for i in range(1, n + 1)
    ]


def _make_loop_target_job(
    *, freeze_content_tree: str, verification_rounds: list, max_rounds: int = 3
) -> dict:
    """A job row that clears every guard in
    ``_trigger_verification_on_complete`` for a PROJECT-LOOP job (has
    ``context.loop_id``) — so hitting the cap must resolve 'completed', not
    'pending_review': the loop advance hook fires only on terminal statuses,
    and a parked loop job wedges the whole loop."""
    return {
        "id": "loop-target-1",
        "parent_job_id": None,
        "config_override": None,
        "resolved_config": {
            "verification": {"enabled": True, "max_rounds": max_rounds}
        },
        "freeze_data": {
            "freeze_type": "job_complete",
            "summary": "done",
            "deliverables": [],
            "confidence": 0.9,
            "content_tree": freeze_content_tree,
        },
        "context": {
            "verification_rounds": verification_rounds,
            "loop_id": "loop-1",
        },
        "status": "reviewing",
        "description": "loop work",
        "config_name": "developer",
        "project_id": None,
        "user_id": None,
        "repo_name": None,
        "branch_name": None,
    }


class TestRoundLimitEnforcement:
    """The round cap must ESCALATE to a human, never auto-accept.

    The retired agent-side branch this replaces did the opposite at the cap:
    fail the critic and call ``_set_target_to_autonomy_status`` directly — an
    auto-approval of whatever was on disk, regardless of open findings. The
    orchestrator's ``_verification_gate_decision`` has no such branch; its
    only two outcomes are "spawn" (another round) and "escalate" (a human
    decides). These tests pin that down, including the project-loop variant
    that must resolve 'completed' rather than park on 'pending_review'.
    """

    def test_round_under_limit_spawns(self):
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = _rounds_with_open_blocking(1)
        action, _ = _verification_gate_decision(rounds, content_tree="h9", max_rounds=3)
        assert action == "spawn"

    def test_round_at_limit_escalates(self):
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = _rounds_with_open_blocking(3)
        action, reason = _verification_gate_decision(
            rounds, content_tree="h9", max_rounds=3
        )
        assert action == "escalate"
        assert "round limit" in reason.lower()

    def test_round_over_limit_escalates(self):
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = _rounds_with_open_blocking(5)
        action, reason = _verification_gate_decision(
            rounds, content_tree="h9", max_rounds=3
        )
        assert action == "escalate"
        assert "round limit" in reason.lower()

    def test_first_round_always_spawns(self):
        """Round zero (no prior rounds at all) is always under the cap."""
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        action, _ = _verification_gate_decision([], content_tree="h1", max_rounds=3)
        assert action == "spawn"

    def test_cap_reached_outcome_is_never_approval(self):
        """The defect class this replaces: the retired branch auto-accepted
        the target at the cap. Confirm the decision function's outcome is
        always one of exactly two values, and approval is never one of
        them — however many rounds pile up."""
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = _rounds_with_open_blocking(10)
        action, _ = _verification_gate_decision(
            rounds, content_tree="h999", max_rounds=3
        )
        assert action in ("spawn", "escalate")

    @pytest.mark.asyncio
    async def test_loop_job_at_round_cap_resolves_completed_not_pending_review(
        self, monkeypatch
    ):
        """Wired end to end through the real trigger function (not just the
        pure decision function): a project-loop job that hits the round cap
        must resolve 'completed', never 'pending_review'."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )

        rounds = _rounds_with_open_blocking(3)
        # A content_tree distinct from every prior round's — proves this
        # escalation is the CAP branch, not the no-progress branch (already
        # covered by test_loop_job_no_progress_escalates_to_completed in
        # tests/test_verification_flow.py).
        job = _make_loop_target_job(
            freeze_content_tree="h999", verification_rounds=rounds
        )
        actions: list[str] = []

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, actions
        )

        update_mock.assert_awaited_once()
        assert update_mock.call_args.kwargs["status"] == "completed"
        assert "round limit" in update_mock.call_args.kwargs["error_message"].lower()


# =============================================================================
# Integration: on_strategic_phase_complete with verdict
# =============================================================================


class TestStrategicPhaseWithVerdict:
    """Tests that on_strategic_phase_complete detects verdict data."""

    def setup_method(self):
        """Clear state before each test."""
        _verdict_data.clear()

    def test_verdict_triggers_finalize(self):
        """When verdict data exists, on_strategic_phase_complete should finalize."""
        from agent.core.phase import on_strategic_phase_complete

        state = make_state()
        ws = make_workspace()
        tm = make_todo_manager()
        tm.has_staged_todos = MagicMock(return_value=False)
        tm.list_all = MagicMock(return_value=[])

        # Set verdict data
        _verdict_data["critic-job-1"] = {
            "_verdict": "approved",
            "_target_job_id": "target-job-1",
            "report": "Looks good",
        }

        result = on_strategic_phase_complete(state, ws, tm)

        assert result.success is True
        assert result.state_updates["should_stop"] is True
        assert result.freeze_data["verdict"] == "approved"

        # Verdict data should be consumed
        assert get_verdict_data("critic-job-1") is None


# =============================================================================
# Verdict durability: tools must persist through the orchestrator BEFORE
# returning to the model (journal-before-observe). Task 5 of the fail-closed
# verification plan — see knowledge-base/knowledge/superpowers/plans/2026-07-27-verification-fail-closed.md.
# =============================================================================


class TestVerdictDurability:
    @pytest.mark.asyncio
    async def test_return_tool_fails_loudly_without_orchestrator_client(self):
        """A verdict that cannot be persisted must NOT report success.

        The house convention for orchestrator_client is best-effort
        (`if client is None: return None`). For a verdict that is exactly
        backwards — a silently-unrecorded rejection becomes an approval.
        """
        from agent.tools.context import ToolContext
        from agent.tools.evaluation.evaluation_tools import create_evaluation_tools

        ctx = ToolContext(_job_id="c1", config={})
        ctx.orchestrator_client = None
        approve, return_with_feedback = create_evaluation_tools(ctx)

        result = await return_with_feedback.ainvoke(
            {
                "job_id": "t1",
                "feedback": "bad",
                "findings": [{"claim": "x", "severity": "high"}],
            }
        )

        assert "error" in result.lower()
        from agent.tools.evaluation.evaluation_tools import get_verdict_data

        assert get_verdict_data("c1") is None

    @pytest.mark.asyncio
    async def test_tool_stores_server_computed_verdict_not_asserted(self):
        """The server's computed verdict wins over what the model claimed."""
        from unittest.mock import AsyncMock

        from agent.tools.context import ToolContext
        from agent.tools.evaluation.evaluation_tools import (
            create_evaluation_tools,
            get_verdict_data,
        )

        ctx = ToolContext(_job_id="c2", config={})
        ctx.orchestrator_client = AsyncMock()
        ctx.orchestrator_client.record_verification_round = AsyncMock(
            return_value={
                "verdict": "returned",
                "round": 3,
                "assigned": [],
                "open_findings": [{"id": "F1"}],
            }
        )
        approve, _ = create_evaluation_tools(ctx)

        await approve.ainvoke(
            {
                "job_id": "t1",
                "report": "looks good",
                "dispositions": [{"id": "F1", "disposition": "STILL_OPEN"}],
            }
        )

        assert get_verdict_data("c2")["_verdict"] == "returned"

    @pytest.mark.asyncio
    async def test_workspace_write_failure_degrades_gracefully_not_raises(self):
        """Round 1 fix — Finding 1.

        The round is durably recorded by the orchestrator BEFORE any local
        bookkeeping runs. If the subsequent workspace write then fails (I/O
        error, remote-backend hiccup), the tool must report success with a
        degradation note, not crash — the model must not be told to correct
        and resubmit a round that has already landed durably.
        """
        from unittest.mock import AsyncMock, MagicMock

        from agent.tools.context import ToolContext
        from agent.tools.evaluation.evaluation_tools import (
            create_evaluation_tools,
            get_verdict_data,
        )

        ctx = ToolContext(_job_id="c3", config={})
        ctx.orchestrator_client = AsyncMock()
        ctx.orchestrator_client.record_verification_round = AsyncMock(
            return_value={
                "verdict": "approved",
                "round": 1,
                "assigned": [],
                "open_findings": [],
            }
        )
        ctx.workspace_manager = MagicMock()
        ctx.workspace_manager.get_head_commit = MagicMock(return_value=None)
        ctx.workspace_manager.write_file = MagicMock(side_effect=OSError("disk full"))
        approve, _ = create_evaluation_tools(ctx)

        result = await approve.ainvoke({"job_id": "t1", "report": "looks good"})

        # Must NOT read like the "not recorded" failure path...
        assert "error" not in result.lower()
        # ...and must say plainly that the round IS recorded.
        assert "recorded" in result.lower()
        assert "disk full" in result

        # Bonus (stricter than the brief's minimal ask): the mirror is
        # populated before the workspace write is attempted, so a write
        # failure alone doesn't also cost finalize_job the verdict.
        assert get_verdict_data("c3")["_verdict"] == "approved"

    @pytest.mark.asyncio
    async def test_approve_tool_fails_loudly_when_client_raises(self):
        """Round 1 fix — Finding 2 (approve_job_verdict half).

        Complements test_return_tool_fails_loudly_without_orchestrator_client
        (client is None — an impossible write) with the other half of the
        property: a client that IS present but whose
        record_verification_round raises must also produce a failure-
        signaling string and leave no local trace.
        """
        from unittest.mock import AsyncMock

        from agent.api.orchestrator_client import VerdictRecordingError
        from agent.tools.context import ToolContext
        from agent.tools.evaluation.evaluation_tools import (
            create_evaluation_tools,
            get_verdict_data,
        )

        ctx = ToolContext(_job_id="c4", config={})
        ctx.orchestrator_client = AsyncMock()
        ctx.orchestrator_client.record_verification_round = AsyncMock(
            side_effect=VerdictRecordingError(
                "verdict rejected:\n- F1: no disposition supplied."
            )
        )
        approve, _ = create_evaluation_tools(ctx)

        result = await approve.ainvoke({"job_id": "t1", "report": "looks good"})

        assert "error" in result.lower()
        assert "not recorded" in result.lower()
        assert get_verdict_data("c4") is None

    @pytest.mark.asyncio
    async def test_return_tool_fails_loudly_when_client_raises(self):
        """Round 1 fix — Finding 2 (return_job_with_feedback half)."""
        from unittest.mock import AsyncMock

        from agent.api.orchestrator_client import VerdictRecordingError
        from agent.tools.context import ToolContext
        from agent.tools.evaluation.evaluation_tools import (
            create_evaluation_tools,
            get_verdict_data,
        )

        ctx = ToolContext(_job_id="c5", config={})
        ctx.orchestrator_client = AsyncMock()
        ctx.orchestrator_client.record_verification_round = AsyncMock(
            side_effect=VerdictRecordingError("network error: connection refused")
        )
        _, return_with_feedback = create_evaluation_tools(ctx)

        result = await return_with_feedback.ainvoke(
            {
                "job_id": "t1",
                "feedback": "bad",
                "findings": [{"claim": "x", "severity": "high"}],
            }
        )

        assert "error" in result.lower()
        assert "not recorded" in result.lower()
        assert get_verdict_data("c5") is None

    @pytest.mark.asyncio
    async def test_escalated_rejection_returns_stop_order_not_resubmit(self):
        """Rejection-cap escalation: the tool must STOP the resubmit loop.

        When the orchestrator flags the 409 as escalated (per-critic rejection
        cap reached, target already sent to manual review), returning the
        usual "must be corrected and resubmitted" string is the livelock —
        the model obeys it forever (189 iterations / 105 min live).
        knowledge-history/done/rejected_verdict_livelocks_critic_and_wedges_parent.md
        """
        from unittest.mock import AsyncMock

        from agent.api.orchestrator_client import VerdictRecordingError
        from agent.tools.context import ToolContext
        from agent.tools.evaluation.evaluation_tools import (
            create_evaluation_tools,
            get_verdict_data,
        )

        err = VerdictRecordingError(
            "verdict rejected and the review has been escalated to a human "
            "after repeated invalid submissions:\n- Cannot return a job with "
            "no findings"
        )
        err.escalated = True
        ctx = ToolContext(_job_id="c6", config={})
        ctx.orchestrator_client = AsyncMock()
        ctx.orchestrator_client.record_verification_round = AsyncMock(side_effect=err)
        _, return_with_feedback = create_evaluation_tools(ctx)

        result = await return_with_feedback.ainvoke(
            {"job_id": "t1", "feedback": "bad", "findings": []}
        )

        assert "escalated to a human" in result
        assert "Do NOT resubmit" in result
        assert "must be corrected and resubmitted" not in result
        assert get_verdict_data("c6") is None
