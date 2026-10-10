"""What a session's cloud mounts are doing, for the user and the agent (D7).

A mount that does not come up never blocks the workspace: the workspace
starts, the mount is marked unavailable, and the reason reaches the user
(owner, 2026-10-09). The state lives in ``threads.metadata.cloud_mount_status``::

    {
      "version": 1,
      "fingerprint": <the Pod's recorded plan>,
      "runtime_incarnation": <the Pod's UID>,
      "updated_at": <ISO time>,
      "notice": null | "agent_outdated",
      "mounts": {<name>: {mount_kind, target_path, access, state, reason,
                          reported_by, updated_at}},
      "excluded": [{source_ref, mount_kind, reason, detail?}]
    }

Two writers, never more:

* the **orchestrator**, when it publishes a Pod: every planned mount
  ``pending``, every row the plan left out under ``excluded`` (a Pod on the
  in-workspace path clears the record); and ``notice: agent_outdated`` when
  an agent image from before the in-pod plane attaches;
* the **agent**, after it reads the supervisor's status files: ``mounted``
  or ``unavailable`` with a reason (:func:`report_entries` checks every
  field against the plan and the closed reason set).

A report is merged only into the record of the same plan fingerprint, so an
agent of an earlier Pod cannot overwrite its successor's state. Nothing here
carries a remote's own words: states and reasons are closed sets.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from orchestrator.services.cloud_mount_sidecar import UNAVAILABLE_REASONS

logger = logging.getLogger(__name__)

STATUS_KEY = "cloud_mount_status"
MOUNT_STATES = frozenset({"pending", "mounted", "unavailable"})
NOTICES = frozenset({"agent_outdated"})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def initial_status(
    recorded: Mapping[str, Any], runtime_incarnation: str, *, now: str | None = None
) -> dict[str, Any]:
    """The orchestrator's record for a freshly created Pod."""
    stamp = now or _now()
    return {
        "version": 1,
        "fingerprint": recorded.get("fingerprint"),
        "runtime_incarnation": runtime_incarnation,
        "updated_at": stamp,
        "notice": None,
        "mounts": {
            str(mount.get("name")): {
                "mount_kind": mount.get("mount_kind"),
                "target_path": mount.get("target_path"),
                "access": mount.get("access"),
                "state": "pending",
                "reason": None,
                "reported_by": "orchestrator",
                "updated_at": stamp,
            }
            for mount in recorded.get("mounts") or []
        },
        "excluded": [
            {
                key: entry.get(key)
                for key in ("source_ref", "mount_kind", "reason", "detail")
                if entry.get(key) is not None
            }
            for entry in recorded.get("excluded") or []
        ],
    }


def report_entries(
    recorded: Mapping[str, Any],
    reports: Any,
    *,
    now: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Mount entries from an agent's report, checked against the plan.

    Raises ``ValueError`` for a mount the plan does not name, a state or a
    reason outside the closed sets, or a reason on a mounted mount.
    """
    if not isinstance(reports, list) or len(reports) > 64:
        raise ValueError("mounts must be a list")
    planned = {str(mount.get("name")): mount for mount in recorded.get("mounts") or []}
    stamp = now or _now()
    entries: dict[str, dict[str, Any]] = {}
    for report in reports:
        if not isinstance(report, Mapping):
            raise ValueError("a mount report must be an object")
        name = report.get("name")
        state = report.get("state")
        reason = report.get("reason")
        if not isinstance(name, str) or name not in planned:
            raise ValueError("the report names a mount the plan does not")
        if state not in MOUNT_STATES:
            raise ValueError("unknown mount state")
        if state == "unavailable":
            if reason not in UNAVAILABLE_REASONS:
                raise ValueError("unknown unavailable reason")
        elif reason is not None:
            raise ValueError("only an unavailable mount has a reason")
        mount = planned[name]
        entries[name] = {
            "mount_kind": mount.get("mount_kind"),
            "target_path": mount.get("target_path"),
            "access": mount.get("access"),
            "state": state,
            "reason": reason,
            "reported_by": "agent",
            "updated_at": stamp,
        }
    return entries


async def record_pod(
    store: Any,
    thread_id: str,
    recorded: Mapping[str, Any] | None,
    runtime_incarnation: str,
) -> bool:
    """Record a new Pod's plan as its initial state (``None`` clears it)."""
    setter = getattr(type(store), "set_thread_cloud_mount_status", None)
    if not callable(setter):
        return True
    status = (
        initial_status(recorded, runtime_incarnation) if recorded is not None else None
    )
    return bool(await setter(store, thread_id, status))


async def record_report(
    store: Any,
    thread_id: str,
    *,
    fingerprint: str,
    entries: Mapping[str, Mapping[str, Any]],
) -> bool:
    merge = getattr(type(store), "merge_thread_cloud_mount_status", None)
    if not callable(merge):
        return False
    return bool(
        await merge(
            store,
            thread_id,
            fingerprint=fingerprint,
            mounts=dict(entries),
            notice=None,
            updated_at=_now(),
        )
    )


async def record_agent_outdated(
    store: Any, thread_id: str, sidecar_payload: Mapping[str, Any]
) -> None:
    """Best effort, never raises: the poll it rides on must not fail."""
    fingerprint = sidecar_payload.get("fingerprint")
    merge = getattr(type(store), "merge_thread_cloud_mount_status", None)
    if not isinstance(fingerprint, str) or not callable(merge):
        return
    try:
        await merge(
            store,
            thread_id,
            fingerprint=fingerprint,
            mounts={},
            notice="agent_outdated",
            updated_at=_now(),
        )
    except Exception:
        logger.warning(
            "Thread %s: could not record an outdated agent's cloud mounts",
            thread_id,
            exc_info=True,
        )
    logger.warning(
        "Thread %s: an agent without in-pod plane support attached to a Pod "
        "whose cloud mounts come from its sidecars; it gets no cloud payload",
        thread_id,
    )


__all__ = [
    "MOUNT_STATES",
    "NOTICES",
    "STATUS_KEY",
    "initial_status",
    "record_agent_outdated",
    "record_pod",
    "record_report",
    "report_entries",
]
