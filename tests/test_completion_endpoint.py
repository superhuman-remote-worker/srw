"""Unit tests for orchestrator-side job completion handling.

Tests the pure logic functions in orchestrator/services/completion.py
and the orchestrator endpoint helpers.
"""

import json
from pathlib import Path
from unittest.mock import patch

from orchestrator.services.completion import (  # noqa: E402
    determine_job_status,
    get_verification_config,
    is_verification_enabled,
    get_curation_config,
    is_curation_enabled,
    get_scholar_config,
    is_scholar_enabled,
    resolve_scholar_config_from_disk,
    get_autonomy_level,
    is_job_completion_freeze,
    format_verification_instructions,
    format_scholar_instructions,
)


# =============================================================================
# Fixtures — job dicts with resolved_config
# =============================================================================


def make_job(
    *,
    status: str = "processing",
    parent_job_id: str | None = None,
    freeze_data: dict | None = None,
    verification_enabled: bool = True,
    curation_enabled: bool = False,
    scholar_enabled: bool = True,
    autonomy: str = "review",
    config_name: str = "defaults",
) -> dict:
    """Create a minimal job dict for testing."""
    resolved_config = {
        "agent": {
            "agent_id": config_name,
            "autonomy": autonomy,
            "verification": {
                "enabled": verification_enabled,
                "critic_config": "critic",
                "max_rounds": 3,
            },
            "curator": {
                "enabled": curation_enabled,
                "curator_config": "curator",
            },
            "scholar": {
                "enabled": scholar_enabled,
                "scholar_config": "scholar",
            },
        },
        "model_family": "openai",
    }

    job = {
        "id": "test-job-1",
        "status": status,
        "config_name": config_name,
        "parent_job_id": parent_job_id,
        "resolved_config": resolved_config,
        "description": "Test job description",
        "context": {},
        "project_id": None,
    }
    if freeze_data is not None:
        job["freeze_data"] = freeze_data
    return job


# =============================================================================
# Config resolution tests
# =============================================================================


class TestGetVerificationConfig:
    """Test reading verification config from resolved_config."""

    def test_standard_path(self):
        job = make_job(verification_enabled=True)
        vc = get_verification_config(job)
        assert vc["enabled"] is True
        assert vc["critic_config"] == "critic"
        assert vc["max_rounds"] == 3

    def test_missing_resolved_config(self):
        job = {"id": "x", "resolved_config": None}
        assert get_verification_config(job) == {}

    def test_string_resolved_config(self):
        rc = json.dumps(
            {
                "agent": {"verification": {"enabled": True, "critic_config": "critic"}},
            }
        )
        job = {"id": "x", "resolved_config": rc}
        vc = get_verification_config(job)
        assert vc["enabled"] is True

    def test_top_level_fallback(self):
        """When verification is at top level (not nested in agent)."""
        job = {
            "id": "x",
            "resolved_config": {
                "verification": {"enabled": True, "max_rounds": 5},
            },
        }
        vc = get_verification_config(job)
        assert vc["enabled"] is True
        assert vc["max_rounds"] == 5

    def test_no_verification_key(self):
        job = {"id": "x", "resolved_config": {"agent": {}}}
        assert get_verification_config(job) == {}

    def test_extra_fallback(self):
        """When verification is nested inside agent.extra (un-flattened)."""
        job = {
            "id": "x",
            "resolved_config": {
                "agent": {
                    "extra": {
                        "verification": {
                            "enabled": True,
                            "critic_config": "critic",
                            "max_rounds": 3,
                        },
                    },
                },
            },
        }
        vc = get_verification_config(job)
        assert vc["enabled"] is True
        assert vc["critic_config"] == "critic"

    def test_is_verification_enabled(self):
        assert is_verification_enabled(make_job(verification_enabled=True)) is True
        assert is_verification_enabled(make_job(verification_enabled=False)) is False
        # The conservative worker base does not start a critic round implicitly.
        assert is_verification_enabled({"id": "x", "resolved_config": None}) is False

    def test_is_verification_disabled_by_override(self):
        """Config override disables verification even without resolved_config."""
        job = {
            "id": "x",
            "resolved_config": None,
            "config_name": "default",
            "config_override": {"verification": {"enabled": False}},
        }
        assert is_verification_enabled(job) is False


class TestGetCurationConfig:
    """Test reading curation config from resolved_config."""

    def test_standard_path(self):
        job = make_job(curation_enabled=True)
        cc = get_curation_config(job)
        assert cc["enabled"] is True

    def test_is_curation_enabled(self):
        assert is_curation_enabled(make_job(curation_enabled=True)) is True
        assert is_curation_enabled(make_job(curation_enabled=False)) is False


class TestGetAutonomyLevel:
    """Test reading autonomy from resolved_config."""

    def test_standard_path(self):
        assert get_autonomy_level(make_job(autonomy="full")) == "full"
        assert get_autonomy_level(make_job(autonomy="review")) == "review"
        assert get_autonomy_level(make_job(autonomy="partial")) == "partial"

    def test_default(self):
        job = {"id": "x", "resolved_config": {"agent": {}}}
        assert get_autonomy_level(job) == "review"


# =============================================================================
# Freeze data tests
# =============================================================================


class TestIsJobCompletionFreeze:
    """Test detecting job completion vs phase boundary freezes."""

    def test_job_complete(self):
        job = make_job(freeze_data={"freeze_type": "job_complete", "summary": "done"})
        assert is_job_completion_freeze(job) is True

    def test_job_completed_status(self):
        """Full autonomy format: status='job_completed' instead of freeze_type."""
        job = make_job(freeze_data={"status": "job_completed"})
        assert is_job_completion_freeze(job) is True

    def test_phase_boundary(self):
        job = make_job(freeze_data={"freeze_type": "phase_boundary"})
        assert is_job_completion_freeze(job) is False

    def test_no_freeze_data(self):
        job = make_job()
        assert is_job_completion_freeze(job) is False

    def test_string_freeze_data(self):
        job = make_job()
        job["freeze_data"] = json.dumps({"freeze_type": "job_complete"})
        assert is_job_completion_freeze(job) is True


# =============================================================================
# Status determination tests
# =============================================================================


class TestDetermineJobStatus:
    """Test the status decision tree."""

    def test_error_returns_failed(self):
        job = make_job()
        result = {"error": {"message": "something broke"}}
        status, err = determine_job_status(job, result)
        assert status == "failed"
        assert err == "something broke"

    def test_error_string(self):
        job = make_job()
        result = {"error": "plain string error"}
        status, err = determine_job_status(job, result)
        assert status == "failed"
        assert err == "plain string error"

    def test_not_stopped_returns_none(self):
        job = make_job()
        result = {"should_stop": False}
        status, err = determine_job_status(job, result)
        assert status is None
        assert err is None

    def test_harness_completion_without_goal_achieved_waits_in_review(self):
        """A custom harness's job_complete report (goal_achieved false, no
        verification) lands in pending_review like SRW's own agent's."""
        job = make_job(
            verification_enabled=False,
            freeze_data={
                "status": "job_completed",
                "freeze_type": "job_complete",
                "summary": "done",
                "deliverables": ["hello.txt"],
            },
        )
        result = {"should_stop": True, "goal_achieved": False, "error": None}
        assert determine_job_status(job, result) == ("pending_review", None)

    def test_critic_job_approved(self):
        """Critic jobs with approved verdict get status from freeze_data."""
        job = make_job(
            parent_job_id="parent-1",
            freeze_data={
                "status": "completed",
                "freeze_type": "verdict",
                "verdict": "approved",
            },
        )
        result = {"should_stop": True, "goal_achieved": True}
        status, err = determine_job_status(job, result)
        assert status == "completed"
        assert err is None

    def test_critic_job_returned(self):
        """Critic jobs with returned verdict get 'waiting' status from freeze_data."""
        job = make_job(
            parent_job_id="parent-1",
            freeze_data={
                "status": "waiting",
                "freeze_type": "verdict",
                "verdict": "returned",
            },
        )
        result = {"should_stop": True, "goal_achieved": False}
        status, err = determine_job_status(job, result)
        assert status == "waiting"
        assert err is None

    def test_critic_job_no_freeze_data(self):
        """Critic jobs without freeze_data infer from goal_achieved."""
        job = make_job(parent_job_id="parent-1")
        result = {"should_stop": True, "goal_achieved": True}
        status, err = determine_job_status(job, result)
        assert status == "completed"
        assert err is None

    def test_goal_achieved_verification_enabled(self):
        job = make_job(
            verification_enabled=True,
            freeze_data={"freeze_type": "job_complete"},
        )
        result = {"should_stop": True, "goal_achieved": True}
        status, err = determine_job_status(job, result)
        assert status == "reviewing"

    def test_goal_achieved_verification_disabled(self):
        """Full autonomy, no verification — orchestrator sets completed."""
        job = make_job(verification_enabled=False)
        result = {"should_stop": True, "goal_achieved": True}
        status, err = determine_job_status(job, result)
        assert status == "completed"

    def test_job_completion_freeze_verification_enabled(self):
        """Non-full autonomy, freeze_type=job_complete, verification enabled."""
        job = make_job(
            verification_enabled=True,
            freeze_data={"freeze_type": "job_complete"},
        )
        result = {"should_stop": True, "goal_achieved": False}
        status, err = determine_job_status(job, result)
        assert status == "reviewing"

    def test_job_completion_freeze_no_verification(self):
        """Non-full autonomy, freeze_type=job_complete, no verification."""
        job = make_job(
            verification_enabled=False,
            freeze_data={"freeze_type": "job_complete"},
        )
        result = {"should_stop": True, "goal_achieved": False}
        status, err = determine_job_status(job, result)
        assert status == "pending_review"

    def test_phase_boundary(self):
        """Phase boundary freeze — always pending_review."""
        job = make_job(freeze_data={"freeze_type": "phase_boundary"})
        result = {"should_stop": True, "goal_achieved": False}
        status, err = determine_job_status(job, result)
        assert status == "pending_review"

    def test_workspace_upgrade_required_paused(self):
        """A worker in-process sandbox upgrade that FAILED surfaces the freeze;
        the orchestrator pauses for re-attempt (workspace_tier_upgrade.md §4.3
        W1). On the happy path this freeze never reaches completion — the agent
        swaps in place and the job continues."""
        job = make_job(
            freeze_data={
                "freeze_type": "workspace_upgrade_required",
                "target_tier": "sandbox",
            }
        )
        result = {"should_stop": True, "goal_achieved": False}
        status, err = determine_job_status(job, result)
        assert status == "paused"
        assert err is None

    def test_no_freeze_data_not_goal_achieved(self):
        """Stopped without goal or freeze data — pending_review."""
        job = make_job(verification_enabled=True)
        result = {"should_stop": True, "goal_achieved": False}
        status, err = determine_job_status(job, result)
        assert status == "pending_review"


# =============================================================================
# Template formatting tests
# =============================================================================


class TestFormatVerificationInstructions:
    """Test verification instructions template formatting."""

    def test_basic_formatting(self):
        freeze_data = {
            "deliverables": ["output/report.md", "output/data.json"],
            "summary": "Completed the analysis",
            "confidence": 0.85,
        }
        result = format_verification_instructions(
            job_id="test-123",
            description="Analyze the dataset",
            freeze_data=freeze_data,
            config_name="developer",
        )
        assert result is not None
        assert "test-123" in result
        assert "developer" in result
        assert "Analyze the dataset" in result
        assert "output/report.md" in result
        assert "output/data.json" in result
        assert "Completed the analysis" in result
        assert "85%" in result
        assert "MUST delegate one independent evidence pass" in result
        assert '`subagent_type="verifier"`' in result
        assert '`isolation="shared"`' in result
        assert "`run_in_background=false`" in result

    def test_no_deliverables(self):
        freeze_data = {"summary": "Done", "confidence": 0.5}
        result = format_verification_instructions(
            job_id="test-456",
            description="Do something",
            freeze_data=freeze_data,
            config_name="defaults",
        )
        assert result is not None
        assert "no deliverables listed" in result

    def test_template_not_found(self):
        with patch(
            "orchestrator.services.completion._REPO_ROOT",
            Path("/nonexistent"),
        ):
            result = format_verification_instructions(
                job_id="x",
                description="x",
                freeze_data={},
                config_name="x",
            )
            assert result is None


# =============================================================================
# Scholar config tests
# =============================================================================


class TestGetScholarConfig:
    """Test reading scholar config from resolved_config."""

    def test_standard_path(self):
        job = make_job(scholar_enabled=True)
        sc = get_scholar_config(job)
        assert sc["enabled"] is True
        assert sc["scholar_config"] == "scholar"

    def test_disabled(self):
        job = make_job(scholar_enabled=False)
        assert is_scholar_enabled(job) is False

    def test_enabled(self):
        assert is_scholar_enabled(make_job(scholar_enabled=True)) is True

    def test_missing_resolved_config(self):
        job = {"id": "x", "resolved_config": None}
        assert get_scholar_config(job) == {}
        assert is_scholar_enabled(job) is False

    def test_top_level_fallback(self):
        """When scholar is at top level (not nested in agent)."""
        job = {
            "id": "x",
            "resolved_config": {
                "scholar": {"enabled": True, "scholar_config": "custom_scholar"},
            },
        }
        sc = get_scholar_config(job)
        assert sc["enabled"] is True
        assert sc["scholar_config"] == "custom_scholar"

    def test_string_resolved_config(self):
        rc = json.dumps(
            {
                "agent": {"scholar": {"enabled": True, "scholar_config": "scholar"}},
            }
        )
        job = {"id": "x", "resolved_config": rc}
        sc = get_scholar_config(job)
        assert sc["enabled"] is True


class TestResolveScholarConfigFromDisk:
    """Test the lightweight YAML reader for creation-time config checks."""

    def test_worker_base_has_scholar_disabled(self):
        """The framework base must not start a research subjob implicitly."""
        sc = resolve_scholar_config_from_disk("default")
        assert sc.get("enabled") is False

    def test_config_override_disables(self):
        sc = resolve_scholar_config_from_disk(
            "default",
            config_override={"scholar": {"enabled": False}},
        )
        assert sc.get("enabled") is False

    def test_config_override_custom_config(self):
        sc = resolve_scholar_config_from_disk(
            "default",
            config_override={"scholar": {"scholar_config": "custom_scholar"}},
        )
        assert sc.get("scholar_config") == "custom_scholar"

    def test_nonexistent_config_falls_back_to_defaults(self):
        sc = resolve_scholar_config_from_disk("nonexistent_config_xyz")
        assert sc.get("enabled") is False  # From worker_base.yaml

    def test_scholar_config_reads_scholar_expert(self):
        """The scholar expert config disables scholar spawning to prevent
        recursive scholar-of-scholar jobs."""
        sc = resolve_scholar_config_from_disk("scholar")
        assert sc.get("enabled") is False


class TestFormatScholarInstructions:
    """Test scholar instructions template formatting."""

    def test_basic_formatting(self):
        result = format_scholar_instructions(
            parent_job_id="test-123",
            description="Analyze microservice patterns",
            config_name="developer",
        )
        assert result is not None
        assert "test-123" in result
        assert "developer" in result
        assert "Analyze microservice patterns" in result
        assert "research/" in result

    def test_with_instructions(self):
        result = format_scholar_instructions(
            parent_job_id="test-456",
            description="Build REST API",
            config_name="developer",
            instructions="Focus on authentication patterns",
        )
        assert result is not None
        assert "Focus on authentication patterns" in result

    def test_custom_output_dir(self):
        result = format_scholar_instructions(
            parent_job_id="test-789",
            description="Research task",
            config_name="default",
            output_dir="custom_research",
        )
        assert result is not None
        assert "custom_research/" in result

    def test_template_not_found(self):
        with patch(
            "orchestrator.services.completion._REPO_ROOT",
            Path("/nonexistent"),
        ):
            result = format_scholar_instructions(
                parent_job_id="x",
                description="x",
                config_name="x",
            )
            assert result is None


class TestVerificationInstructionsDelivery:
    """Task 7: the rendered instructions must actually reach the critic.

    Before this fix, ``format_verification_instructions`` was called in
    ``orchestrator/main.py`` and its result was only null-checked, never
    passed to ``create_job`` (which has no ``instructions`` parameter) — so
    every critic since the orchestrator migration ran on a generic
    description instead of the rendered brief. See
    knowledge-base/knowledge/superpowers/plans/2026-07-27-verification-fail-closed.md Task 7.
    """

    def test_prior_findings_rendered_into_template(self):
        from orchestrator.services.completion import format_verification_instructions

        text = format_verification_instructions(
            job_id="t1",
            description="d",
            freeze_data={},
            config_name="worker_base",
            prior_findings="- **F1** [high, opened round 1]: missing source",
        )
        assert text is not None
        assert "F1" in text
        assert "missing source" in text

    def test_missing_prior_findings_does_not_abort(self):
        """A KeyError here returns None, which aborts critic spawn entirely."""
        from orchestrator.services.completion import format_verification_instructions

        assert (
            format_verification_instructions(
                job_id="t1", description="d", freeze_data={}, config_name="worker_base"
            )
            is not None
        )
