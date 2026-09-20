"""Unit tests for the dispatcher preemption placeability guard (D1).

Covers the pure predicate ``preemption_blocked_reason``. The dispatcher itself
(``_try_dispatch_pending_jobs`` in orchestrator/main.py) is untested, so the
decision logic is extracted here to be verified in isolation. See
knowledge-history/done/preemption_before_first_checkpoint_replays_job_opening.md.
"""

from __future__ import annotations

import pytest

from orchestrator.services.dispatch_guards import (
    VM_ATTENTION,
    VM_CAPACITY_POLL,
    VM_GOLDEN_POLL,
    VM_HEADSCALE_POLL,
    VM_PARK_EXHAUSTED,
    VM_PARK_GOLDEN,
    VM_PARK_HEADSCALE,
    VM_PARKED,
    VM_PROVISION,
    VM_READY,
    VM_RECYCLE,
    VM_WAIT,
    preemption_blocked_reason,
    resume_lane_applies,
    vm_provisioning_decision,
)
from shared.vm_provisioning_phases import observe_provisioning
from tests.test_vm_provisioning_phases import evidence, running


def phase_context(observation, *, now=100, status="created"):
    return {
        "status": status,
        "provision_generation": observation["provision_generation"],
        "vm_uid": observation["vm_uid"],
        "rootdisk_pvc_uid": observation["rootdisk_pvc_uid"],
        "namespace": observation["namespace"],
        "provisioning": observe_provisioning(None, observation, now=now),
    }


class TestResumeLaneApplies:
    """Lane choice: /job/resume only for paused jobs that actually ran.

    A paused-but-never-started job dispatched as a resume reaches the agent
    brief-less (JobResumeRequest has no description/deliverables/kickoff) and
    strands. See knowledge-base/knowledge/issues/fresh_job_dispatched_as_resume_skips_seeding.md.
    """

    def test_paused_with_checkpoint_resumes(self):
        assert resume_lane_applies({"status": "paused"}, has_checkpoint=True)

    def test_paused_without_checkpoint_takes_fresh_lane(self):
        assert not resume_lane_applies({"status": "paused"}, has_checkpoint=False)

    def test_non_paused_statuses_never_resume(self):
        for status in ("created", "failed", "processing", "", None):
            assert not resume_lane_applies({"status": status}, has_checkpoint=True), (
                status
            )


class TestPreemptionBlockedReason:
    def test_root_job_never_blocked(self):
        # No parent_job_id → a normal job; never blocked regardless of the
        # (irrelevant) parent_status argument.
        assert preemption_blocked_reason({"id": "j1"}, None) is None
        assert preemption_blocked_reason({"id": "j1"}, "completed") is None

    def test_subjob_with_live_parent_not_blocked(self):
        job = {"id": "critic1", "parent_job_id": "p1"}
        for parent_status in (
            "processing",
            "reviewing",
            "waiting",
            "created",
            "paused",
        ):
            assert preemption_blocked_reason(job, parent_status) is None

    def test_subjob_with_terminal_parent_blocked(self):
        job = {"id": "critic1", "parent_job_id": "p1"}
        for parent_status in ("completed", "failed", "cancelled"):
            reason = preemption_blocked_reason(job, parent_status)
            assert reason is not None
            assert parent_status in reason

    def test_subjob_with_missing_parent_not_blocked(self):
        # Parent not found (None) → cannot confirm terminal; let other guards /
        # the sweeper handle it rather than blocking on uncertainty.
        job = {"id": "critic1", "parent_job_id": "p1"}
        assert preemption_blocked_reason(job, None) is None


class TestVmProvisioningDecision:
    """The VM provisioning state machine (timeout + bounded-retry park)."""

    def _decide(self, vm_ctx, *, attempts=0, cap=3, now=1000.0, timeout_s=600.0):
        return vm_provisioning_decision(
            vm_ctx,
            provision_attempts=attempts,
            max_provision_attempts=cap,
            now=now,
            timeout_s=timeout_s,
        )

    def test_absent_provisions(self):
        assert self._decide({}) == VM_PROVISION

    def test_deleted_re_provisions(self):
        # The drain/crash-recovery release leaves 'deleted' — must re-provision.
        assert self._decide({"status": "deleted"}) == VM_PROVISION

    def test_absent_parks_when_attempts_exhausted(self):
        assert self._decide({}, attempts=3, cap=3) == VM_PARK_EXHAUSTED
        assert self._decide({"status": "deleted"}, attempts=5, cap=3) == (
            VM_PARK_EXHAUSTED
        )

    def test_failed_stays_parked(self):
        # Never hot-retry a failed VM against the shared cluster.
        assert self._decide({"status": "failed"}) == VM_PARKED

    def test_deleting_waits(self):
        assert self._decide({"status": "deleting"}) == VM_WAIT

    def test_ready_dispatches(self):
        assert self._decide({"status": "ready"}) == VM_READY

    def test_unproven_retirement_waits_until_its_durable_retry_time(self):
        ctx = {
            "status": "retiring_process_zero",
            "provisioned_at": 1.0,
            "retirement_retry_after": 1100.0,
        }
        assert self._decide(ctx, now=1000.0) == VM_WAIT
        assert self._decide(ctx, now=1100.0) == VM_RECYCLE

    def test_stale_retry_metadata_cannot_block_ready_or_new_provision(self):
        for status, expected in [("ready", VM_READY), ("deleted", VM_PROVISION)]:
            assert (
                self._decide(
                    {
                        "status": status,
                        "retirement_retry_after": 1100.0,
                    }
                )
                == expected
            )

    def test_legacy_retirement_without_retry_metadata_gets_an_attempt(self):
        assert (
            self._decide(
                {
                    "status": "retiring_process_zero",
                    "provisioned_at": 1.0,
                }
            )
            == VM_RECYCLE
        )

    def test_provisioning_within_budget_waits(self):
        ctx = phase_context(running(), now=900)
        assert self._decide(ctx, now=1000.0, timeout_s=600.0) == VM_WAIT

    def test_ssh_pending_within_budget_waits(self):
        # Daemon registered, but the orchestrator has not proved SSH yet.
        ctx = phase_context(running(), now=900, status="ssh_pending")
        assert self._decide(ctx, now=1000.0, timeout_s=600.0) == VM_WAIT

    def test_provisioning_past_budget_recycles(self):
        ctx = phase_context(running(), now=300)
        assert self._decide(ctx, now=1000.0, timeout_s=600.0) == VM_RECYCLE

    def test_ssh_pending_past_budget_recycles(self):
        ctx = phase_context(running(), now=300, status="ssh_pending")
        assert self._decide(ctx, now=1000.0, timeout_s=600.0) == VM_RECYCLE

    def test_missing_phase_evidence_is_visible_attention_without_recycling(self):
        assert self._decide({"status": "creating"}) == VM_ATTENTION
        assert self._decide({"status": "created", "provisioned_at": 1}) == VM_ATTENTION

    def test_recycle_needs_both_status_and_stale_timestamp(self):
        # 'ready' is never recycled even with an old timestamp.
        ctx = {"status": "ready", "provisioned_at": 1.0}
        assert self._decide(ctx, now=10_000.0, timeout_s=600.0) == VM_READY

    def test_slow_clone_and_long_placement_never_recycle(self):
        clone = phase_context(evidence())
        assert self._decide(clone, now=701) == VM_WAIT
        assert self._decide(clone, now=2801) == VM_ATTENTION
        placement = phase_context(evidence(disk_phase="waiting_for_consumer"))
        assert self._decide(placement, now=100000) == VM_WAIT

    def test_phase_identity_must_still_match_current_vm_context(self):
        context = phase_context(running())
        context["provision_generation"] = "00000000-0000-4000-8000-000000000099"
        assert self._decide(context, now=701) == VM_ATTENTION

    def test_new_unknown_observation_cannot_reuse_old_boot_timeout(self):
        context = phase_context(running())
        context["provisioning_attention_reason"] = "vm_phase_unproven"
        assert self._decide(context, now=701) == VM_ATTENTION

    def test_cleanup_and_initialization_keep_precedence_over_phase_attention(self):
        context = phase_context(evidence(disk_phase="unknown"))
        context["retirement_cleanup_pending"] = True
        assert self._decide(context) == VM_RECYCLE
        context["retirement_cleanup_pending"] = False
        context["initialization_started_at"] = 900
        assert self._decide(context) == VM_WAIT


class TestVmGoldenWaitDecision:
    """waiting_golden — a cold golden-image import must not burn boot budget.

    The controller has NOT created a VM: it is waiting on a shared golden
    DataVolume import (~30 min after an agent-vm-base bump — longer than
    timeout_s). Polling must not consume provision attempts, and recycling is
    meaningless (nothing exists to tear down). See
    knowledge-history/done/golden_image_cold_import_fails_inflight_vm_jobs.md.
    """

    def _decide(
        self,
        vm_ctx,
        *,
        attempts=0,
        cap=3,
        now=1000.0,
        timeout_s=600.0,
        golden_timeout_s=2700.0,
    ):
        return vm_provisioning_decision(
            vm_ctx,
            provision_attempts=attempts,
            max_provision_attempts=cap,
            now=now,
            timeout_s=timeout_s,
            golden_timeout_s=golden_timeout_s,
        )

    def test_waiting_golden_polls_within_budget(self):
        ctx = {"status": "waiting_golden", "golden_wait_started_at": 900.0}
        assert self._decide(ctx, now=1000.0) == VM_GOLDEN_POLL

    def test_waiting_golden_polls_before_anchor_stamped(self):
        # First sighting: dispatcher hasn't stamped golden_wait_started_at
        # yet → poll (the dispatcher stamps it in the poll branch).
        assert self._decide({"status": "waiting_golden"}) == VM_GOLDEN_POLL

    def test_waiting_golden_outlives_boot_budget(self):
        # 700s in with a 600s boot budget — must POLL, not RECYCLE: the boot
        # budget does not apply while no VM is booting. This is the exact
        # misalignment that burned loop iteration 22.
        ctx = {
            "status": "waiting_golden",
            "golden_wait_started_at": 300.0,
            "provisioned_at": 300.0,
        }
        assert self._decide(ctx, now=1000.0, timeout_s=600.0) == VM_GOLDEN_POLL

    def test_waiting_golden_ignores_attempt_exhaustion(self):
        # Attempts bound VM boots; polling is not a boot.
        ctx = {"status": "waiting_golden", "golden_wait_started_at": 900.0}
        assert self._decide(ctx, attempts=3, cap=3, now=1000.0) == VM_GOLDEN_POLL

    def test_waiting_golden_parks_past_golden_budget(self):
        # CDI wedged / registry unreachable — fail decisively with the truth.
        # started=100, budget=2700 → poll at/below 2800, park beyond it.
        # (A non-zero anchor: like provisioned_at, the anchor is truthiness-
        # gated, and epoch-zero never occurs in practice.)
        ctx = {"status": "waiting_golden", "golden_wait_started_at": 100.0}
        assert self._decide(ctx, now=2800.0, golden_timeout_s=2700.0) == VM_GOLDEN_POLL
        assert self._decide(ctx, now=2800.1, golden_timeout_s=2700.0) == VM_PARK_GOLDEN


class TestVmCapacityWaitDecision:
    def _decide(self, vm_ctx, *, now=1000.0, capacity_timeout_s=2700.0):
        return vm_provisioning_decision(
            vm_ctx,
            provision_attempts=3,
            max_provision_attempts=3,
            now=now,
            timeout_s=600.0,
            capacity_timeout_s=capacity_timeout_s,
        )

    def test_waiting_capacity_polls_without_consuming_attempts(self):
        ctx = {"status": "waiting_capacity", "capacity_wait_started_at": 900.0}
        assert self._decide(ctx) == VM_CAPACITY_POLL

    def test_waiting_capacity_polls_before_anchor(self):
        assert self._decide({"status": "waiting_capacity"}) == VM_CAPACITY_POLL

    @pytest.mark.parametrize("elapsed", [2700.1, 26 * 3600, 7 * 86400])
    def test_capacity_wait_has_no_elapsed_failure_or_boot_attempt(self, elapsed):
        ctx = {"status": "waiting_capacity", "capacity_wait_started_at": 100.0}
        before = dict(ctx)
        assert self._decide(ctx, now=100 + elapsed) == VM_CAPACITY_POLL
        assert ctx == before

    def test_old_capacity_timeout_argument_cannot_restore_terminal_policy(self):
        ctx = {"status": "waiting_capacity", "capacity_wait_started_at": 100.0}
        assert self._decide(ctx, now=100000.0, capacity_timeout_s=1) == VM_CAPACITY_POLL

    def test_capacity_wait_preserves_cleanup_precedence(self):
        ctx = {
            "status": "waiting_capacity",
            "capacity_wait_started_at": 100.0,
            "retirement_cleanup_pending": True,
        }
        assert self._decide(ctx, now=100000.0) == VM_RECYCLE


class TestVmHeadscaleWaitDecision:
    """waiting_headscale — a mesh-VPN outage must not burn boot budget.

    The controller has NOT created a VM: it refuses to build one it cannot
    hand a tailnet pre-auth key to, because such a VM boots and heartbeats
    but is unreachable over SSH forever. Same shape as waiting_golden —
    polling must not consume provision attempts, and recycling is meaningless
    (nothing exists to tear down). Regression for knowledge-base/knowledge/issues/
    vm_controller_headscale_latch_kills_provisioning.md.
    """

    def _decide(
        self,
        vm_ctx,
        *,
        attempts=0,
        cap=3,
        now=1000.0,
        timeout_s=600.0,
        headscale_timeout_s=900.0,
    ):
        return vm_provisioning_decision(
            vm_ctx,
            provision_attempts=attempts,
            max_provision_attempts=cap,
            now=now,
            timeout_s=timeout_s,
            headscale_timeout_s=headscale_timeout_s,
        )

    def test_waiting_headscale_polls_within_budget(self):
        ctx = {"status": "waiting_headscale", "headscale_wait_started_at": 900.0}
        assert self._decide(ctx, now=1000.0) == VM_HEADSCALE_POLL

    def test_waiting_headscale_polls_before_anchor_stamped(self):
        # First sighting: the dispatcher stamps the anchor in the poll branch.
        assert self._decide({"status": "waiting_headscale"}) == VM_HEADSCALE_POLL

    def test_waiting_headscale_outlives_boot_budget(self):
        # 700s in with a 600s boot budget — POLL, not RECYCLE: no VM is
        # booting, so the boot budget does not apply.
        ctx = {
            "status": "waiting_headscale",
            "headscale_wait_started_at": 300.0,
            "provisioned_at": 300.0,
        }
        assert self._decide(ctx, now=1000.0, timeout_s=600.0) == VM_HEADSCALE_POLL

    def test_waiting_headscale_ignores_attempt_exhaustion(self):
        # Attempts bound VM boots; polling is not a boot. Without this, a job
        # dispatched into a Headscale outage parks instantly on retry cap.
        ctx = {"status": "waiting_headscale", "headscale_wait_started_at": 900.0}
        assert self._decide(ctx, attempts=3, cap=3, now=1000.0) == VM_HEADSCALE_POLL

    def test_waiting_headscale_parks_past_budget(self):
        # Mesh genuinely dead — fail decisively with the real cause rather
        # than the misleading "provisioning exhausted after N attempts".
        ctx = {"status": "waiting_headscale", "headscale_wait_started_at": 100.0}
        assert (
            self._decide(ctx, now=1000.0, headscale_timeout_s=900.0)
            == VM_HEADSCALE_POLL
        )
        assert (
            self._decide(ctx, now=1000.1, headscale_timeout_s=900.0)
            == VM_PARK_HEADSCALE
        )


class TestVmTeardownAndSuspendDecision:
    """Teardown must be bounded, and a suspended VM must never be recycled.

    Three defects from the VM reliability audit, all in this one pure function
    (knowledge-base/knowledge/issues/vm_reliability_assessment.md P1-4/P1-5/P1-6):

    * ``deleting`` returned WAIT unconditionally, *before* the timeout branch.
      Both the delete request and the controller's answer are fire-and-forget
      core NATS, so one lost message wedged the job forever with no signal.
    * ``delete_failed``/``query_failed`` matched no branch and fell through to
      the generic not-ready arm, which RECYCLEs forever: PARK_EXHAUSTED only
      triggers on absent-or-``deleted``, so the job could never reach a
      terminal state.
    * ``suspended``/``suspending``/``restoring`` also matched no branch, so a
      deliberately-suspended VM read as "stuck short of ready" and was torn
      down — the disk was kept on purpose and the recycle purges it.
    """

    def _decide(self, vm_ctx, *, attempts=0, cap=3, now=1000.0, timeout_s=600.0):
        return vm_provisioning_decision(
            vm_ctx,
            provision_attempts=attempts,
            max_provision_attempts=cap,
            now=now,
            timeout_s=timeout_s,
        )

    # --- P1-4: a teardown in flight must not wait forever -------------------

    def test_deleting_in_flight_waits(self):
        ctx = {"status": "deleting", "deleting_started_at": 990.0}
        assert self._decide(ctx, now=1000.0) == VM_WAIT

    def test_deleting_stuck_past_budget_recycles(self):
        # The delete request or its answer was lost — re-issue the teardown.
        ctx = {"status": "deleting", "deleting_started_at": 100.0}
        assert self._decide(ctx, now=1000.0) == VM_RECYCLE

    def test_deleting_stuck_parks_once_attempts_are_exhausted(self):
        ctx = {"status": "deleting", "deleting_started_at": 100.0}
        assert self._decide(ctx, now=1000.0, attempts=3, cap=3) == VM_PARK_EXHAUSTED

    def test_deleting_without_a_stamp_waits(self):
        # Rows written before the stamp existed carry no start time; staleness
        # is unknowable, so stay with the old non-destructive behaviour.
        assert self._decide({"status": "deleting"}) == VM_WAIT

    # --- P1-5: a failed teardown must reach a terminal state ----------------

    def test_delete_failed_retries_within_budget(self):
        assert self._decide({"status": "delete_failed"}) == VM_RECYCLE

    def test_delete_failed_parks_once_attempts_are_exhausted(self):
        assert self._decide({"status": "delete_failed"}, attempts=3, cap=3) == (
            VM_PARK_EXHAUSTED
        )

    def test_query_failed_cannot_authorize_cleanup_or_exhaust_boot_attempts(self):
        assert self._decide({"status": "query_failed"}, attempts=3, cap=3) == (
            VM_ATTENTION
        )
        context = phase_context(running(), status="query_failed")
        assert self._decide(context, now=10000) == VM_ATTENTION

    def test_ready_cannot_hide_recorded_identity_attention(self):
        context = phase_context(running(), status="ready")
        context["provisioning_attention_reason"] = "vm_phase_identity_conflict"
        assert self._decide(context) == VM_ATTENTION

    def test_ready_cannot_hide_attention_in_authenticated_phase(self):
        context = phase_context(evidence(disk_phase="unknown"), status="ready")
        assert self._decide(context) == VM_ATTENTION

    # --- P1-6: never tear down a deliberately-suspended VM ------------------

    def test_suspended_long_past_budget_is_not_recycled(self):
        # A suspended VM keeps its rootdisk on purpose; recycling purges it.
        ctx = {"status": "suspended", "provisioned_at": 100.0}
        assert self._decide(ctx, now=1000.0) == VM_WAIT

    def test_suspending_is_not_recycled(self):
        ctx = {"status": "suspending", "provisioned_at": 100.0}
        assert self._decide(ctx, now=1000.0) == VM_WAIT

    def test_restoring_is_not_recycled(self):
        ctx = {"status": "restoring", "provisioned_at": 100.0}
        assert self._decide(ctx, now=1000.0) == VM_WAIT
