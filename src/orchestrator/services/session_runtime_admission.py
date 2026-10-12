"""Shared fail-closed predicates for persistent-session runtime admission.

Execution lane answers *which* runtime serves a thread.  It is deliberately
not a lifecycle predicate: an ``ended`` row stays on its old lane but must not
be prepared, rebound, credential-served, or forwarded until the owner uses the
explicit resume transition.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Literal, Mapping
from uuid import UUID


PREPARABLE_THREAD_STATUSES = frozenset(
    {"created", "active", "awaiting_user", "suspended"}
)

SESSION_ENDED_DETAIL = {
    "code": "session_ended",
    "message": "This session has ended. Resume it before reconnecting.",
}

SESSION_NOT_PREPARABLE_DETAIL = {
    "code": "session_not_preparable",
    "message": "This session is not in a state that can start a runtime.",
}

SESSION_ENDING_DETAIL = {
    "code": "session_ending",
    "message": "This session is finishing its exact runtime cleanup.",
}


def thread_runtime_status(thread: Mapping[str, Any] | None) -> str:
    """Return the normalized lifecycle status; missing/malformed is unknown."""

    if not thread:
        return ""
    value = thread.get("status")
    return str(value or "").strip()


def thread_runtime_is_preparable(thread: Mapping[str, Any] | None) -> bool:
    """Whether a row may own or receive a live session runtime."""

    return (
        thread_runtime_status(thread) in PREPARABLE_THREAD_STATUSES
        and bool(thread)
        and thread.get("runtime_retirement_token") is None
        and thread.get("pinned_idle_terminal_intent_at") is None
    )


@dataclass(frozen=True, slots=True)
class ThreadRuntimeAuthority:
    """Immutable authority for one live persistent-session incarnation."""

    thread_id: str
    generation: str


def pinned_binding_invalid_detail(
    runtime_authority: ThreadRuntimeAuthority,
) -> dict[str, Any]:
    """Typed non-retryable refusal scoped to one exact pinned generation."""

    return {
        "code": "session_binding_invalid",
        "message": "This session binding is no longer authoritative.",
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": runtime_authority.generation,
    }


def thread_runtime_authority(
    thread: Mapping[str, Any] | None,
) -> ThreadRuntimeAuthority | None:
    """Return the exact admitted runtime authority, or fail closed.

    This is intentionally stricter than :func:`thread_runtime_is_preparable`.
    The latter remains safe during the reader-first migration rollout because
    old application tests/rows may not project the additive column; any code
    that crosses an asynchronous side-effect boundary must capture this exact
    identity and therefore refuses a missing/malformed generation.
    """

    if not thread_runtime_is_preparable(thread):
        return None
    try:
        thread_id = str(UUID(str(thread.get("id"))))
        generation = str(UUID(str(thread.get("runtime_generation"))))
    except (TypeError, ValueError, AttributeError):
        return None
    return ThreadRuntimeAuthority(thread_id=thread_id, generation=generation)


def same_thread_runtime_authority(
    thread: Mapping[str, Any] | None,
    expected: ThreadRuntimeAuthority,
) -> bool:
    """Whether ``thread`` is the same still-open runtime incarnation."""

    return thread_runtime_authority(thread) == expected


def thread_runtime_refusal_detail(
    thread: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Stable public refusal shape, distinguishing only the terminal state."""

    def _bind_exact_generation(detail: dict[str, Any]) -> dict[str, Any]:
        try:
            generation = str(UUID(str((thread or {}).get("runtime_generation"))))
        except (TypeError, ValueError, AttributeError):
            return detail
        detail["pinned_runtime_generation_contract"] = 1
        detail["session_runtime_generation"] = generation
        return detail

    if (
        thread
        and thread.get("runtime_retirement_token") is not None
        and thread.get("runtime_retirement_authorized_at") is not None
    ):
        detail: dict[str, Any] = dict(SESSION_ENDING_DETAIL)
        context = thread.get("runtime_retirement_context") or {}
        if isinstance(context, str):
            try:
                context = json.loads(context)
            except (TypeError, ValueError):
                context = {}
        if isinstance(context, Mapping):
            disposition = str(context.get("settle_status") or "")
            if disposition in {"ended", "suspended"}:
                detail["retirement_disposition"] = disposition
        return _bind_exact_generation(detail)
    if thread_runtime_status(thread) == "ended":
        return _bind_exact_generation(dict(SESSION_ENDED_DETAIL))
    return dict(SESSION_NOT_PREPARABLE_DETAIL)


def thread_requests_protected_cloud(thread: Mapping[str, Any] | None) -> bool:
    """Whether a row must take the protected (including malformed) path."""

    if not thread:
        return False
    metadata = thread.get("metadata")
    if metadata is None:
        metadata = {}
    # asyncpg returns JSONB as text, and PostgresDB.get_thread preserves that
    # storage shape. Decode the object before classifying its strict marker;
    # malformed JSON and non-object values must still require protected gates.
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            return True
    return protected_cloud_marker_state(metadata) != "off"


def protected_cloud_marker_state(
    metadata: Mapping[str, Any] | None,
) -> Literal["off", "on", "malformed"]:
    """Classify the marker without Python truthiness or coercion.

    Historical rows without the key and the explicit JSON boolean ``false``
    are ordinary sessions.  Exact JSON ``true`` selects protected mode.  Every
    other present value is unsafe legacy/corrupt state and must fail closed.
    """

    if not isinstance(metadata, Mapping):
        return "malformed"
    if "protected_cloud" not in metadata or metadata["protected_cloud"] is False:
        return "off"
    if metadata["protected_cloud"] is True:
        return "on"
    return "malformed"
