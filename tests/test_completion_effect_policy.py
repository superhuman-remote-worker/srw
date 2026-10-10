from __future__ import annotations

import pytest

from orchestrator.services.completion_effect_policy import (
    COMPLETION_EFFECT_GROUPS,
    COMPLETION_EFFECT_INDEX,
    COMPLETION_EFFECT_PLAN,
    DELIVERY_GATED_GROUPS,
    PRODUCT_DELIVERY_EFFECTS,
    REORDERED_TERMINAL_STATUSES,
    completion_status_order,
)


def test_stable_effect_plan_has_unique_names_and_group_pairs() -> None:
    names = [effect.name for effect in COMPLETION_EFFECT_PLAN]

    assert len(names) == len(set(names))
    assert len(COMPLETION_EFFECT_INDEX) == len(COMPLETION_EFFECT_PLAN)
    assert set(COMPLETION_EFFECT_GROUPS) == {
        effect.group for effect in COMPLETION_EFFECT_PLAN
    }


def test_product_delivery_set_and_gated_groups_are_the_step_four_decision() -> None:
    assert PRODUCT_DELIVERY_EFFECTS == (
        "loop_project_cloud_delivery",
        "subjob_output_graft",
        "terminal_merge_change_record",
    )
    assert DELIVERY_GATED_GROUPS == (
        "delivery",
        "subjob_graft",
        "terminal_delivery",
    )
    assert {
        name
        for name, policy in COMPLETION_EFFECT_GROUPS.items()
        if policy.gates_terminal_status
    } == set(DELIVERY_GATED_GROUPS)

    mode_a = next(
        effect
        for effect in COMPLETION_EFFECT_PLAN
        if effect.name == "mode_a_diff_capture"
    )
    assert mode_a.group == "delivery"
    assert mode_a.product_delivery is False

    # The job outbox (S16b) runs right after the capture, in its group, and is
    # best-effort: no product delivery, so the step-four set is unchanged.
    names = [effect.name for effect in COMPLETION_EFFECT_PLAN]
    outbox = COMPLETION_EFFECT_PLAN[names.index("job_outbox_delivery")]
    assert names.index("job_outbox_delivery") == names.index("mode_a_diff_capture") + 1
    assert (outbox.group, outbox.inventory_id) == ("delivery", "S16b")
    assert outbox.product_delivery is False


@pytest.mark.parametrize("status", sorted(REORDERED_TERMINAL_STATUSES))
def test_persisted_true_reorders_only_the_exact_terminal_set(status: str) -> None:
    plan = completion_status_order({"status_reorder_enabled": True}, status)

    assert plan.reordered is True
    assert plan.pre_status_class_b_effects == ("critic_verdict",)
    assert plan.pre_status_delivery_effects == PRODUCT_DELIVERY_EFFECTS
    assert plan.gated_groups == DELIVERY_GATED_GROUPS


@pytest.mark.parametrize(
    "status",
    [None, "", "created", "processing", "paused", "pending_review", "reviewing"],
)
def test_nonterminal_status_retains_legacy_order(status: str | None) -> None:
    assert not completion_status_order(
        {"status_reorder_enabled": True}, status
    ).reordered


@pytest.mark.parametrize(
    "command",
    [
        None,
        {},
        {"status_reorder_enabled": False},
        {"status_reorder_enabled": 1},
        {"status_reorder_enabled": "true"},
    ],
)
def test_missing_false_or_malformed_persisted_value_fails_dark(command) -> None:
    assert not completion_status_order(command, "completed").reordered
