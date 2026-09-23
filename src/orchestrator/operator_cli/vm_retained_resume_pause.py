"""Exact public owner Pause receipt for the disposable A1 fixture."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from uuid import UUID


class OwnerPauseHoldError(ValueError):
    """The fixture's durable Pause hold cannot be attributed to its owner."""


def owned_pause_hold_id(
    context: Mapping | None, *, owner_id: str,
    expected_hold_id: str | None = None,
) -> str | None:
    """Return a real public Pause hold ID, optionally bound to its prior output."""
    present = isinstance(context, Mapping) and "_operator_pause_hold" in context
    marker = context.get("_operator_pause_hold") if present else None
    if not present:
        if expected_hold_id is not None:
            raise OwnerPauseHoldError("A1 owner Pause hold is absent")
        return None
    if not isinstance(marker, Mapping) or set(marker) != {
        "version", "hold_id", "source", "paused_by", "paused_at",
    }:
        raise OwnerPauseHoldError("A1 owner Pause hold has an unknown shape")
    try:
        hold_id = str(UUID(marker["hold_id"]))
        owner = str(UUID(owner_id))
        paused_at = datetime.fromisoformat(marker["paused_at"])
    except (AttributeError, TypeError, ValueError) as exc:
        raise OwnerPauseHoldError("A1 owner Pause hold identity is malformed") from exc
    if (
        marker["version"] != 1
        or type(marker["version"]) is not int
        or marker["source"] != "public_pause"
        or marker["paused_by"] != owner_id
        or owner != owner_id
        or hold_id != marker["hold_id"]
        or paused_at.tzinfo is None
        or paused_at.utcoffset() is None
        or (expected_hold_id is not None and hold_id != expected_hold_id)
    ):
        raise OwnerPauseHoldError("A1 owner Pause hold does not match")
    return hold_id
