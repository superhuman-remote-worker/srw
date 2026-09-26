"""Structured worker budget diagnostics; never infer causes from error text."""

from typing import Any

from shared.workspace_recovery import WorkspaceRecoveryCode

_BUDGET_ERRORS = frozenset({"worker_retry_exhausted", "worker_retry_budget_exhausted"})
_WORKSPACE_ERRORS = frozenset(
    {"workspace_unavailable", *(c.value for c in WorkspaceRecoveryCode)}
)


def worker_error_cause(error: Any) -> dict[str, Any] | None:
    """Keep a factual cause through either of the two budget envelopes."""
    if not isinstance(error, dict):
        return None
    if not isinstance(error.get("type"), str):
        return None
    if error.get("type") in _BUDGET_ERRORS:
        error = error.get("cause")
    if not isinstance(error, dict) or not isinstance(error.get("type"), str):
        return None
    if error["type"] in _BUDGET_ERRORS:
        return None
    return dict(error)


def worker_workspace_exhaustion_cause(error: Any) -> dict[str, Any] | None:
    """Recognize only an exhausted worker's explicit typed workspace cause."""
    if not isinstance(error, dict) or error.get("type") != "worker_retry_exhausted":
        return None
    cause = worker_error_cause(error)
    return cause if cause is not None and cause["type"] in _WORKSPACE_ERRORS else None
