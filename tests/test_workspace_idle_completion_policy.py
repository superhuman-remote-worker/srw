"""Only proven semantic human waits may start a completion idle episode."""

from copy import deepcopy
from uuid import uuid4

import pytest


def inputs(*, autonomy="guided", verification=True, kind="phase_boundary"):
    job_id = str(uuid4())
    return {
        "job": {
            "id": job_id,
            "parent_job_id": None,
            "context": {},
            "resolved_config": {
                "agent": {
                    "autonomy": autonomy,
                    "verification": {"enabled": verification},
                }
            },
        },
        "report": {
            "should_stop": True,
            "goal_achieved": False,
            "error": None,
            "freeze_data": {
                "job_id": job_id,
                "status": "pending_review",
                "freeze_type": kind,
                "phase_type": "strategic",
                "phase_number": 1,
            },
        },
        "decision_tool_call_id": "decision-1",
    }


def classify(data):
    from shared.workspace_idle_completion import classify_completion_wait

    return classify_completion_wait(**data)


@pytest.mark.parametrize(
    "autonomy,phase,number,allowed",
    [
        ("partial", "strategic", 1, True),
        ("partial", "strategic", 2, False),
        ("partial", "tactical", 1, False),
        ("guided", "strategic", 4, True),
        ("guided", "tactical", 4, False),
        ("dependent", "strategic", 2, True),
        ("dependent", "tactical", 2, True),
        ("review", "strategic", 1, False),
        ("full", "strategic", 1, False),
        ("unknown", "strategic", 1, False),
    ],
)
@pytest.mark.parametrize("verification", [True, False])
def test_phase_policy_matches_actual_agent_human_boundary(
    autonomy, phase, number, allowed, verification
):
    data = inputs(autonomy=autonomy, verification=verification)
    data["report"]["freeze_data"].update(phase_type=phase, phase_number=number)
    original = deepcopy(data)
    result = classify(data)
    assert data == original
    assert (result is not None) is allowed
    if allowed:
        assert result == {
            "version": 1,
            "branch": "phase_approval",
            "wait_kind": "human_approval",
            "job_id": data["job"]["id"],
            "freeze": {**data["report"]["freeze_data"]},
            "policy": {"autonomy": autonomy, "verification_enabled": verification},
        }


@pytest.mark.parametrize(
    "autonomy", ["review", "partial", "guided", "dependent", "full"]
)
@pytest.mark.parametrize("verification", [True, False])
def test_final_human_review_excludes_automatic_critic_and_full_autonomy(
    autonomy, verification
):
    data = inputs(autonomy=autonomy, verification=verification, kind="job_complete")
    result = classify(data)
    assert (result is not None) is (autonomy != "full" and not verification)
    if result:
        assert result["wait_kind"] == "human_review"
        assert result["branch"] == "final_human_review"
        assert result["decision_tool_call_id"] == "decision-1"
        assert result["freeze"] == {
            "job_id": data["job"]["id"],
            "status": "pending_review",
            "freeze_type": "job_complete",
        }


@pytest.mark.parametrize(
    "key,value",
    [
        ("should_stop", False),
        ("should_stop", 1),
        ("should_stop", "true"),
        ("goal_achieved", True),
        ("goal_achieved", 0),
        ("goal_achieved", None),
        ("error", "delivery failure"),
        ("error", {}),
        ("error", False),
        ("freeze_data", None),
        ("freeze_data", []),
        ("freeze_data", "pending_review"),
    ],
)
def test_unproven_or_nonhuman_report_skips(key, value):
    data = inputs()
    data["report"][key] = value
    assert classify(data) is None


@pytest.mark.parametrize(
    "key,value",
    [
        ("job_id", str(uuid4())),
        ("job_id", "not-a-uuid"),
        ("status", "reviewing"),
        ("freeze_type", "capacity"),
        ("freeze_type", "sudo"),
        ("freeze_type", "unknown"),
        ("phase_type", "unknown"),
        ("phase_number", 0),
        ("phase_number", True),
        ("phase_number", 1.0),
        ("phase_number", "1"),
        ("phase_number", 2**63),
    ],
)
def test_freeze_must_identify_same_exact_human_question(key, value):
    data = inputs()
    data["report"]["freeze_data"][key] = value
    assert classify(data) is None


@pytest.mark.parametrize(
    "key,value",
    [
        ("parent_job_id", str(uuid4())),
        ("context", {"loop_id": str(uuid4())}),
        ("context", None),
        ("context", []),
        ("context", "[]"),
        ("resolved_config", None),
        ("resolved_config", []),
        ("resolved_config", {}),
        ("resolved_config", {"agent": {"autonomy": "guided"}}),
        ("resolved_config", {"agent": {"verification": {"enabled": False}}}),
        (
            "resolved_config",
            {"agent": {"autonomy": "guided", "verification": {"enabled": 0}}},
        ),
        (
            "resolved_config",
            {"agent": {"autonomy": [], "verification": {"enabled": False}}},
        ),
    ],
)
def test_no_policy_defaults_or_legacy_owner_guessing(key, value):
    data = inputs()
    data["job"][key] = value
    assert classify(data) is None


@pytest.mark.parametrize("key", ["id", "parent_job_id", "context", "resolved_config"])
def test_incomplete_job_projection_is_unsupported(key):
    data = inputs()
    del data["job"][key]
    assert classify(data) is None


@pytest.mark.parametrize("decision", [None, "", " ", {}, "x" * 513])
def test_final_review_requires_bounded_accepted_decision(decision):
    data = inputs(verification=False, kind="job_complete")
    data["decision_tool_call_id"] = decision
    assert classify(data) is None


def test_report_policy_and_timestamps_are_not_authority():
    data = inputs(autonomy="full", verification=True, kind="job_complete")
    data["report"].update(autonomy="review", verification={"enabled": False})
    assert classify(data) is None
    data = inputs()
    data["report"]["freeze_data"].update(
        timestamp="yesterday", summary="private narrative"
    )
    result = classify(data)
    assert (
        result
        and "timestamp" not in result["freeze"]
        and "summary" not in result["freeze"]
    )


@pytest.mark.parametrize("shape", ["top", "extra"])
def test_explicit_existing_resolved_policy_shapes(shape):
    data = inputs()
    agent = data["job"]["resolved_config"]["agent"]
    if shape == "top":
        data["job"]["resolved_config"] = agent
    else:
        agent["extra"] = {"verification": agent.pop("verification")}
    assert classify(data) is not None


def test_top_level_extra_is_not_a_resolved_verification_policy():
    from orchestrator.services.completion import get_verification_config

    data = inputs(autonomy="review", verification=False, kind="job_complete")
    data["job"]["resolved_config"] = {
        "autonomy": "review",
        "extra": {"verification": {"enabled": False}},
    }
    assert get_verification_config(data["job"]) == {}
    assert classify(data) is None
