"""Pure ordering policy for the durable job-completion effect journal.

The stable effect name/group pairs are part of the resumability contract: an
in-flight command must never silently reinterpret either value after a deploy.
Step 4 adds one independently persisted policy bit, but deliberately keeps the
effect vocabulary unchanged so commands admitted under the old order continue
to replay byte-for-byte.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class CompletionEffectSpec:
    """One stable journal identity and its inventory classification."""

    name: str
    group: str
    inventory_id: str
    product_delivery: bool = False


@dataclass(frozen=True, slots=True)
class CompletionEffectGroupPolicy:
    """Ordering metadata shared by every member of one effect group."""

    name: str
    gates_terminal_status: bool = False


@dataclass(frozen=True, slots=True)
class CompletionStatusOrder:
    """The immutable ordering decision for one command/workflow pass."""

    reordered: bool
    pre_status_class_b_effects: tuple[str, ...] = ()
    pre_status_delivery_effects: tuple[str, ...] = ()
    gated_groups: tuple[str, ...] = ()


# Keep this list in the historical execution order.  Reordering is selected by
# ``completion_status_order``; it never mutates this versioned vocabulary.
COMPLETION_EFFECT_PLAN: tuple[CompletionEffectSpec, ...] = (
    CompletionEffectSpec("late_callback_guard", "entry", "S1"),
    CompletionEffectSpec("clear_stale_failure", "entry", "S2"),
    CompletionEffectSpec("persist_reported_freeze", "entry", "S3"),
    CompletionEffectSpec("drop_queued_replies", "entry", "S4"),
    CompletionEffectSpec("infra_transient_give_up", "recovery", "S5"),
    CompletionEffectSpec("infra_transient_pause", "recovery", "S6"),
    CompletionEffectSpec("pod_workspace_recovery", "recovery", "S7"),
    CompletionEffectSpec("vm_workspace_recovery", "recovery", "S8"),
    CompletionEffectSpec("reset_recovery_strikes", "recovery", "S9"),
    CompletionEffectSpec("memory_kb_retry_pause", "recovery", "S11"),
    CompletionEffectSpec("llm_outage_retry_pause", "recovery", "S12"),
    CompletionEffectSpec("llm_give_up_operator_alert", "llm_give_up_alert", "S13"),
    CompletionEffectSpec("deliverable_contract_gate", "delivery_gate", "S14"),
    # E4 (officer_supervision_surface §3.3): record the typed evidence
    # manifest right after the gate so the deliverable-check stamp exists.
    # Additive vocabulary: in-flight pre-E4 commands simply run it on resume.
    CompletionEffectSpec("evidence_manifest_record", "delivery_gate", "S14b"),
    CompletionEffectSpec(
        "loop_project_cloud_delivery",
        "delivery",
        "S15",
        product_delivery=True,
    ),
    # S16 keeps its shipped group identity for in-flight compatibility.  It is
    # diagnostic diff capture, not one of step 4's three product deliveries.
    CompletionEffectSpec("mode_a_diff_capture", "delivery", "S16"),
    # The job outbox (main cloud slice 6.2): a best-effort copy of a root
    # job's deliverables into its owner's shared folder. Additive vocabulary:
    # an in-flight command without it simply runs it on resume.
    CompletionEffectSpec("job_outbox_delivery", "delivery", "S16b"),
    CompletionEffectSpec("main_status_write", "job_disposition", "S17"),
    CompletionEffectSpec("clear_assigned_agent_on_pause", "job_disposition", "S18"),
    CompletionEffectSpec("stash_and_clear_freeze", "job_disposition", "S19"),
    CompletionEffectSpec("drain_stall_counter_alert", "drain_stall_alert", "S20"),
    CompletionEffectSpec(
        "drain_stall_operator_alert", "drain_stall_notification", "S20"
    ),
    CompletionEffectSpec("completed_at", "job_disposition", "S21"),
    CompletionEffectSpec("sudo_approval_request", "sudo_request", "S22"),
    CompletionEffectSpec("auto_deny_resume", "auto_deny_resume", "S23"),
    CompletionEffectSpec("freeze_workspace_snapshot", "workspace_snapshot", "S24"),
    CompletionEffectSpec("freeze_notification", "freeze_notification", "S25"),
    CompletionEffectSpec(
        "subjob_output_graft",
        "subjob_graft",
        "S26",
        product_delivery=True,
    ),
    CompletionEffectSpec("critic_verdict", "critic_verdict", "S27"),
    CompletionEffectSpec(
        "critic_verdict_followup", "critic_verdict_followup", "S27-tail"
    ),
    CompletionEffectSpec("scholar_parent_unblock", "scholar_unblock", "S28"),
    CompletionEffectSpec("delegation_parent_unblock", "delegation_unblock", "S29"),
    CompletionEffectSpec("verification_critic_spawn", "verification", "S30"),
    CompletionEffectSpec(
        "verification_critic_handoff", "verification_handoff", "S30-tail"
    ),
    CompletionEffectSpec("curation_final_pass", "curation", "S31"),
    CompletionEffectSpec("project_loop_advance", "project_loop", "S32"),
    CompletionEffectSpec(
        "project_loop_advance_handoff", "project_loop_handoff", "S32-tail"
    ),
    CompletionEffectSpec(
        "terminal_merge_change_record",
        "terminal_delivery",
        "S33",
        product_delivery=True,
    ),
    CompletionEffectSpec("session_wake_enqueue", "session_wake_enqueue", "S34"),
    CompletionEffectSpec("dispatch_trigger", "dispatch", "S35"),
    CompletionEffectSpec("workspace_archive_teardown", "workspace_teardown", "S36"),
    CompletionEffectSpec("session_wake_drain_kick", "session_wake_kick", "S37"),
)

COMPLETION_EFFECT_INDEX = frozenset(
    (effect.name, effect.group) for effect in COMPLETION_EFFECT_PLAN
)
if len(COMPLETION_EFFECT_INDEX) != len(COMPLETION_EFFECT_PLAN):
    raise RuntimeError("completion effect stable names must be unique")


_GATED_GROUP_NAMES = frozenset({"delivery", "subjob_graft", "terminal_delivery"})
_GROUP_NAMES = tuple(dict.fromkeys(effect.group for effect in COMPLETION_EFFECT_PLAN))
COMPLETION_EFFECT_GROUPS: Mapping[str, CompletionEffectGroupPolicy] = MappingProxyType(
    {
        name: CompletionEffectGroupPolicy(
            name=name,
            gates_terminal_status=name in _GATED_GROUP_NAMES,
        )
        for name in _GROUP_NAMES
    }
)

PRODUCT_DELIVERY_EFFECTS = tuple(
    effect.name for effect in COMPLETION_EFFECT_PLAN if effect.product_delivery
)
DELIVERY_GATED_GROUPS = tuple(
    policy.name
    for policy in COMPLETION_EFFECT_GROUPS.values()
    if policy.gates_terminal_status
)
if {
    effect.group for effect in COMPLETION_EFFECT_PLAN if effect.product_delivery
} != set(DELIVERY_GATED_GROUPS):
    raise RuntimeError("every product-delivery effect group must gate terminal status")


# Cancel is a terminal disposition too.  Review/pause/waiting states preserve
# the old ordering even when the persisted rollout bit is true.
REORDERED_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})


def completion_status_order(
    command: Mapping[str, Any] | None,
    new_status: str | None,
) -> CompletionStatusOrder:
    """Return the command-persisted status order, with a fail-dark default.

    Only a real boolean ``True`` on a durable command can select the new order.
    A missing/malformed value, a direct legacy call, and every non-terminal
    disposition retain the historical status-first execution exactly.
    """

    enabled = bool(
        command is not None and command.get("status_reorder_enabled") is True
    )
    if not enabled or new_status not in REORDERED_TERMINAL_STATUSES:
        return CompletionStatusOrder(reordered=False)
    return CompletionStatusOrder(
        reordered=True,
        pre_status_class_b_effects=("critic_verdict",),
        pre_status_delivery_effects=PRODUCT_DELIVERY_EFFECTS,
        gated_groups=DELIVERY_GATED_GROUPS,
    )


__all__ = [
    "COMPLETION_EFFECT_GROUPS",
    "COMPLETION_EFFECT_INDEX",
    "COMPLETION_EFFECT_PLAN",
    "DELIVERY_GATED_GROUPS",
    "PRODUCT_DELIVERY_EFFECTS",
    "REORDERED_TERMINAL_STATUSES",
    "CompletionEffectGroupPolicy",
    "CompletionEffectSpec",
    "CompletionStatusOrder",
    "completion_status_order",
]
