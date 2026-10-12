"""Closed semantic classifier for completion-owned human waits.

This pure projection grants no idle episode, runtime authority, or physical
release permission. The acceptance adapter must bind it to the delivered runtime;
finalization must recheck that identity and the successful disposition branch.
Only explicit server-resolved policy is supported. Report timestamps and display
status alone never establish a human-wait clock.
"""

import json
from uuid import UUID


_AUTONOMY = {"review", "partial", "guided", "dependent", "full"}


def _object(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return None
    return value if isinstance(value, dict) else None


def _policy(value):
    resolved = _object(value)
    if not resolved:
        return None
    agent = resolved.get("agent", resolved)
    if not isinstance(agent, dict):
        return None
    autonomy = agent.get("autonomy")
    if not isinstance(autonomy, str) or autonomy not in _AUTONOMY:
        return None
    verification = agent.get("verification")
    if verification is None and isinstance(resolved.get("agent"), dict):
        extra = agent.get("extra")
        if isinstance(extra, dict):
            verification = extra.get("verification")
    if verification is None:
        verification = resolved.get("verification")
    if (
        not isinstance(verification, dict)
        or type(verification.get("enabled")) is not bool
    ):
        return None
    return {"autonomy": autonomy, "verification_enabled": verification["enabled"]}


def classify_completion_wait(*, job, report, decision_tool_call_id=None):
    """Return bounded versioned semantics or None for unsupported evidence.

    ``job`` and ``decision_tool_call_id`` come from the locked server admission
    snapshot; never construct them from caller policy or a later mutable Job.
    The returned branch must survive the finalizer's outcome-changing gates.
    """
    if not isinstance(job, dict) or not isinstance(report, dict):
        return None
    if "parent_job_id" not in job or job["parent_job_id"] is not None:
        return None
    context = _object(job.get("context"))
    if context is None or context.get("loop_id") is not None:
        return None
    try:
        job_id = str(UUID(str(job["id"])))
    except (KeyError, ValueError, TypeError, AttributeError):
        return None
    if (
        report.get("should_stop") is not True
        or report.get("goal_achieved") is not False
    ):
        return None
    error = report.get("error")
    if error is not None and not (isinstance(error, str) and error == ""):
        return None
    freeze = report.get("freeze_data")
    if not isinstance(freeze, dict):
        return None
    if freeze.get("job_id") != job_id or freeze.get("status") != "pending_review":
        return None
    policy = _policy(job.get("resolved_config"))
    if policy is None:
        return None
    kind = freeze.get("freeze_type")
    normalized = {"job_id": job_id, "status": "pending_review", "freeze_type": kind}
    result = {"version": 1, "job_id": job_id, "freeze": normalized, "policy": policy}
    if kind == "phase_boundary":
        phase = freeze.get("phase_type")
        number = freeze.get("phase_number")
        if (
            not isinstance(phase, str)
            or phase not in {"strategic", "tactical"}
            or type(number) is not int
            or not 1 <= number < 2**63
        ):
            return None
        autonomy = policy["autonomy"]
        if not (
            autonomy == "dependent"
            or autonomy == "guided"
            and phase == "strategic"
            or autonomy == "partial"
            and phase == "strategic"
            and number == 1
        ):
            return None
        normalized.update(phase_type=phase, phase_number=number)
        result.update(branch="phase_approval", wait_kind="human_approval")
    elif kind == "job_complete":
        if policy["autonomy"] == "full" or policy["verification_enabled"]:
            return None
        if (
            not isinstance(decision_tool_call_id, str)
            or not decision_tool_call_id.strip()
            or len(decision_tool_call_id) > 512
        ):
            return None
        result.update(
            branch="final_human_review",
            wait_kind="human_review",
            decision_tool_call_id=decision_tool_call_id.strip(),
        )
    else:
        return None
    return result
