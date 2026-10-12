"""Pure predicates guarding the dispatcher's preemption decisions.

Extracted from ``_try_dispatch_pending_jobs`` (src/orchestrator/main.py) so the
decision logic is unit-testable without standing up the whole dispatcher (which
is otherwise untested). The dispatcher performs the DB I/O and passes the
resolved values in.

See knowledge-history/done/preemption_before_first_checkpoint_replays_job_opening.md.
"""

from __future__ import annotations

from typing import Any, Optional

# Statuses from which a job can never make progress again.
_TERMINAL_STATUSES = ("completed", "failed", "cancelled")


def preemption_blocked_reason(
    pending_job: dict[str, Any], parent_status: Optional[str]
) -> Optional[str]:
    """Return why ``pending_job`` must NOT preempt a running job, or ``None``.

    A verification/critic subjob whose parent is already terminal can never make
    progress, so it must not pause healthy work to "make room" for itself — yet
    the dispatcher would still treat it as a high-priority pending job. Root jobs
    (no parent) and subjobs whose parent is still live are unaffected.

    Args:
        pending_job: the candidate preemptor job row.
        parent_status: status of ``pending_job``'s parent, or ``None`` if it has
            no parent or the parent could not be found.

    Returns:
        A short human-readable reason string if preemption must be blocked, else
        ``None`` (preemption may proceed).
    """
    if pending_job.get("parent_job_id") and parent_status in _TERMINAL_STATUSES:
        return f"subjob parent is terminal ({parent_status})"
    return None


def resume_lane_applies(job: dict[str, Any], *, has_checkpoint: bool) -> bool:
    """True → dispatch via ``/job/resume``; False → fresh ``/job/start``.

    Only a 'paused' job that actually RAN may take the resume lane. A
    paused-but-never-started job re-dispatched as a resume reaches the agent
    with no description/deliverables/kickoff — ``JobResumeRequest`` carries
    none of them — so it starts brief-less and strands. The caller resolves
    ``has_checkpoint`` (``PostgresDB.job_has_checkpoint``); when checkpoint
    presence is unknowable (sqlite backend) that probe fails open to True,
    preserving today's behavior — the agent-side brief hydration + tripwire
    cover that half. See
    knowledge-base/knowledge/issues/fresh_job_dispatched_as_resume_skips_seeding.md.
    """
    if str(job.get("status") or "") != "paused":
        return False
    return has_checkpoint


# VM provisioning decision outcomes — the dispatcher performs the I/O each names.
VM_PROVISION = "provision"  # (re)create the VM (retries remain)
VM_PARK_EXHAUSTED = "park_exhausted"  # retries used up → mark failed + park
VM_PARKED = "parked"  # already 'failed' → leave parked (no hot-retry)
VM_WAIT = "wait"  # provisioning/creating/deleting in flight → wait
VM_ATTENTION = "attention"  # retain workspace; no destructive phase decision
VM_RECYCLE = "recycle"  # stuck past budget → tear down so it re-provisions
VM_READY = "ready"  # VM booted → proceed to claim/dispatch
VM_GOLDEN_POLL = "golden_poll"  # golden image importing → re-poll create, free
VM_PARK_GOLDEN = "park_golden"  # golden import never finished → fail + park
VM_PARK_INITIALIZATION = (
    "park_initialization"  # setup did not finish → fail without retry
)
VM_CAPACITY_POLL = "capacity_poll"  # controller at capacity → re-poll create
VM_HEADSCALE_POLL = "headscale_poll"  # mesh VPN down → re-poll create, free
VM_PARK_HEADSCALE = "park_headscale"  # mesh VPN never recovered → fail + park
VM_PREPARATION_POLL = "preparation_poll"
VM_PARK_PREPARATION = "park_preparation"

# Teardown states the controller reports when a delete does not complete. Both
# used to match no branch and fall through to the generic not-ready arm, which
# RECYCLEs forever — PARK_EXHAUSTED only triggers on absent-or-'deleted', so the
# job could never reach a terminal state.
TEARDOWN_FAILED_STATUSES = ("delete_failed",)

# Suspend/restore states. The suspension subsystem owns these transitions and
# keeps the rootdisk on purpose; the dispatcher must wait rather than treat them
# as "stuck short of ready" and tear the VM down (which purges that disk).
SUSPEND_STATUSES = ("suspending", "suspended", "restoring")

# Bounded patience for a cold golden-image import (an agent-vm-base bump).
# Observed cold import on the shared VM cluster: ~30 min — deliberately above
# both VM_PROVISION_TIMEOUT_S (600) and the controller's VM_GOLDEN_POLL_TIMEOUT
# (900), which is the misalignment that burned a loop iteration (see
# knowledge-history/done/golden_image_cold_import_fails_inflight_vm_jobs.md).
DEFAULT_GOLDEN_WAIT_TIMEOUT_S = 2700.0
DEFAULT_ROOTDISK_STALL_TIMEOUT_S = 2700.0

# Bounded patience for a Headscale outage. The controller refuses to build a
# VM it cannot hand a tailnet key to, so this budget covers "how long might
# Headscale plausibly be down". Observed worst case: a full homelab reboot,
# where Headscale trailed the VM controller by ~8 min. 15 min leaves margin
# without stalling a loop iteration on a genuinely dead mesh.
DEFAULT_HEADSCALE_WAIT_TIMEOUT_S = 900.0


def _bounded_teardown_retry(
    provision_attempts: int, max_provision_attempts: int
) -> str:
    """Retry a failed/stuck teardown, but let the attempt budget end it.

    RECYCLE re-issues the delete; without this bound the controller's
    delete_failed → RECYCLE → delete_failed cycle never terminates.
    """
    if provision_attempts >= max_provision_attempts:
        return VM_PARK_EXHAUSTED
    return VM_RECYCLE


def vm_phase_decision(
    vm_ctx: dict[str, Any],
    *,
    now: float,
    timeout_s: float,
    rootdisk_stall_timeout_s: float = DEFAULT_ROOTDISK_STALL_TIMEOUT_S,
):
    """Use phase clocks only while their exact identity is still current."""
    from shared.vm_provisioning_phases import (
        ProvisioningDecision,
        provisioning_decision,
    )

    reason = vm_ctx.get("provisioning_attention_reason")
    if reason is not None:
        if reason not in (
            "vm_phase_unproven",
            "vm_phase_identity_conflict",
            "vm_phase_conflict",
            "vm_runtime_changed",
            "vm_rootdisk_stalled",
        ):
            reason = "vm_phase_unproven"
        return ProvisioningDecision("attention", reason)
    state = vm_ctx.get("provisioning")
    identity = state.get("identity") if isinstance(state, dict) else None
    if not isinstance(identity, dict) or any(
        identity.get(key) != vm_ctx.get(key)
        for key in ("provision_generation", "vm_uid", "rootdisk_pvc_uid", "namespace")
    ):
        return ProvisioningDecision("attention", "vm_phase_unproven")
    return provisioning_decision(
        state,
        now=now,
        boot_timeout_s=timeout_s,
        rootdisk_stall_timeout_s=rootdisk_stall_timeout_s,
    )


def vm_provisioning_decision(
    vm_ctx: dict[str, Any],
    *,
    provision_attempts: int,
    max_provision_attempts: int,
    now: float,
    timeout_s: float,
    golden_timeout_s: float = DEFAULT_GOLDEN_WAIT_TIMEOUT_S,
    capacity_timeout_s: float | None = None,
    headscale_timeout_s: float = DEFAULT_HEADSCALE_WAIT_TIMEOUT_S,
    rootdisk_stall_timeout_s: float = DEFAULT_ROOTDISK_STALL_TIMEOUT_S,
) -> str:
    """Decide what the dispatcher should do with a VM-backed job's VM.

    Pure branch logic extracted from ``_try_dispatch_pending_jobs`` so the VM
    provisioning state machine is testable without the whole dispatcher. The
    dispatcher resolves ``vm_ctx`` / the attempt counter / the clock and performs
    the resulting I/O (create, delete, park, or dispatch).

    The attempt counter is the durable park signal: a status-based park ('failed')
    can be clobbered by the external VM controller's async status callbacks, but
    ``provision_attempts`` is monotonic in ``context.vm`` (reset to 0 only once the
    VM reaches 'ready'), so retries stay bounded regardless of callback races.

    States:
      absent / 'deleted' → PROVISION, or PARK_EXHAUSTED once retries are used up
      'failed'           → PARKED (don't hot-retry the shared VM cluster)
      initialization    → WAIT within the separate setup budget; otherwise
                           PARK_INITIALIZATION, preserving partial setup for
                           diagnosis instead of recycling it as a boot failure
      'suspending'/'suspended'/'restoring'
                         → WAIT (the suspension subsystem owns these and keeps
                           the rootdisk deliberately; recycling would purge it)
      'deleting'         → WAIT while in flight; once stuck past ``timeout_s``
                           from ``deleting_started_at``, RECYCLE to re-issue the
                           teardown, or PARK_EXHAUSTED once retries are gone
      'delete_failed'
                         → RECYCLE (re-issue), PARK_EXHAUSTED once retries are
                           gone — previously these reached no terminal state
      'waiting_golden'   → GOLDEN_POLL within ``golden_timeout_s`` of
                           ``golden_wait_started_at``, else PARK_GOLDEN. The
                           controller has NOT created a VM yet — it is waiting
                           on a shared golden-image import (a cold import after
                           an agent-vm-base bump takes ~30 min, longer than
                           ``timeout_s``). Polling re-issues create WITHOUT
                           consuming a provision attempt: the attempt budget
                           bounds VM boots, and no boot is happening. RECYCLE
                           would be meaningless here (nothing to tear down) and
                           counting attempts would park every job dispatched
                           into the import window — the exact failure this
                           branch removes.
      'waiting_capacity' → CAPACITY_POLL regardless of elapsed time. Explicit
                           immutable Job deadlines and cancellation are enforced
                           by their existing admission/control owners. The legacy
                           ``capacity_timeout_s`` keyword is ignored for call
                           compatibility; it cannot restore terminal waiting.
      'waiting_headscale'→ HEADSCALE_POLL within ``headscale_timeout_s`` of
                           ``headscale_wait_started_at``, else PARK_HEADSCALE.
                           Same shape as waiting_golden and for the same
                           reason: no VM exists, so polling re-issues create
                           WITHOUT consuming a provision attempt. The
                           controller refuses to build a VM while Headscale
                           is down, because a VM with no tailnet pre-auth key
                           boots fine but is unreachable forever — it would
                           silently burn the whole attempt budget.
      not-yet-'ready'    → exact Running evidence starts immutable boot budget;
                           disk preparation and placement do not. Missing or
                           conflicting evidence/stalled disk retains attention.
      'ready'            → READY (proceed to claim)
    """
    status = vm_ctx.get("status")
    retry_after = vm_ctx.get("retirement_retry_after")
    cleanup_backoff = (
        type(retry_after) in (int, float) and now < retry_after <= now + 300
    )
    # An HTTP delete response can arrive before the VM disappears and before
    # the cleanup admission is completed. Never start a successor in that gap.
    if vm_ctx.get("retirement_cleanup_pending") is True:
        return VM_WAIT if cleanup_backoff else VM_RECYCLE
    if not status or status == "deleted":
        if provision_attempts >= max_provision_attempts:
            return VM_PARK_EXHAUSTED
        return VM_PROVISION
    if status == "failed":
        return VM_PARKED
    if status != "ready" and cleanup_backoff:
        return VM_WAIT
    if status in SUSPEND_STATUSES:
        return VM_WAIT
    if status == "ready" and (
        vm_ctx.get("provisioning_attention_reason") is not None
        or vm_ctx.get("provisioning") is not None
    ):
        if (
            vm_phase_decision(
                vm_ctx,
                now=now,
                timeout_s=timeout_s,
                rootdisk_stall_timeout_s=rootdisk_stall_timeout_s,
            ).action
            == "attention"
        ):
            return VM_ATTENTION
    if status == "retiring_process_zero":
        return VM_RECYCLE
    if status == "query_failed":
        # A failed observation is not evidence that the guest failed to boot.
        # Pending cleanup above retains its own authority and retry policy.
        return VM_ATTENTION
    if status == "deleting":
        # Both the delete request and the controller's answer are fire-and-forget
        # core NATS (at-most-once, no JetStream), so a dropped message strands the
        # job here. Re-issue the teardown once it is provably stuck. Rows written
        # before this stamp existed carry no start time — staleness is unknowable,
        # so they keep the old non-destructive behaviour.
        started = vm_ctx.get("deleting_started_at")
        if started and (now - float(started)) > timeout_s:
            return _bounded_teardown_retry(provision_attempts, max_provision_attempts)
        return VM_WAIT
    if status in TEARDOWN_FAILED_STATUSES:
        return _bounded_teardown_retry(provision_attempts, max_provision_attempts)
    if status == "waiting_golden":
        started = vm_ctx.get("golden_wait_started_at")
        if started and (now - float(started)) > golden_timeout_s:
            return VM_PARK_GOLDEN
        return VM_GOLDEN_POLL
    if status == "waiting_preparation":
        from shared.workspace_preparation_settings import PreparationSettings

        settings = PreparationSettings.from_environment()
        budget = settings.wait_budget
        started = vm_ctx.get("preparation_wait_started_at")
        if (
            type(started) not in (int, float)
            or not 0 < started <= now
            or now - started > budget
        ):
            return VM_PARK_PREPARATION
        return VM_PREPARATION_POLL
    if status == "waiting_capacity":
        return VM_CAPACITY_POLL
    if status == "waiting_headscale":
        started = vm_ctx.get("headscale_wait_started_at")
        if started and (now - float(started)) > headscale_timeout_s:
            return VM_PARK_HEADSCALE
        return VM_HEADSCALE_POLL
    if status != "ready" and vm_ctx.get("initialization_started_at") is not None:
        from shared.workspace_initialization import TIMEOUT_SECONDS

        started = vm_ctx["initialization_started_at"]
        if type(started) not in (int, float) or not 0 < started <= now:
            return VM_PARK_INITIALIZATION
        if now - started > TIMEOUT_SECONDS + 60:
            return VM_PARK_INITIALIZATION
        # SSH has been verified and guest setup is running. Do not recycle
        # the disk under a live initializer using the shorter VM boot budget.
        return VM_WAIT
    if status != "ready":
        phase = vm_phase_decision(
            vm_ctx,
            now=now,
            timeout_s=timeout_s,
            rootdisk_stall_timeout_s=rootdisk_stall_timeout_s,
        )
        if phase.action == "boot_timeout":
            return VM_RECYCLE
        if phase.action == "attention":
            return VM_ATTENTION
        return VM_WAIT
    return VM_READY
