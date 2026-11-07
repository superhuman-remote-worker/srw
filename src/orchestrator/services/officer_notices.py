"""Telling the Officer, and telling the user a route fell back.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane O, census group
``S_OFFICER``). Two one-way deliveries the Post lifecycle and the conference
machinery both need:

* :func:`inject_officer_notice` — a best-effort one-liner into the live
  runtime's durable input inbox. It **bypasses holds by design**: Legate input
  always reaches him. Never raises.
* :func:`deliver_staged_officer_routes` — durable route fallback intents,
  delivered only *after* their transaction committed. A failed dispatch leaves
  ``user_delivery_at`` null, which is the routing reconciler's retry contract,
  so delivery can never roll back or falsify a hold/decommission transition.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass
class OfficerNoticeDependencies:
    """The store both deliveries read and write through."""

    store: Any


async def inject_officer_notice(
    officer_thread: dict[str, Any],
    text: str,
    *,
    dependencies: OfficerNoticeDependencies,
) -> bool:
    """Best-effort one-liner to a live officer (bypasses holds by design —
    Legate input always reaches him). Never raises.

    The notice enters the same durable input ledger as every server-injected
    officer wake (``persist_thread_input_delivery``); the exact live runtime
    claims it, so a recyclable Pod IP is never treated as the recipient. It is
    sent only while a ready runtime serves the thread: a notice for a down
    officer would replay as a stale turn on his next attach, and his sitrep
    already carries the durable state. Returns True when the ledger accepted
    it for that thread.

    Until 2026-11 this called ``session_wake._inject_live``, which the
    runtime-authority tranche removed on 2026-08-27; the AttributeError was
    swallowed here, so every hold, release, post-update and conference notice
    was silently dropped.
    """
    try:
        from orchestrator.services import session_wake as _sw

        thread_id = str(officer_thread["id"])
        # Callers hold the Post's thread projection, which carries no runtime
        # authority; the live probe needs the full row (as the watchdog reads).
        thread = await dependencies.store.get_thread(thread_id)
        if thread is None:
            return False
        agent = await _sw._resolve_live_agent(dependencies.store, thread)
        if agent is None:
            return False
        result = await dependencies.store.persist_thread_input_delivery(
            thread_id=thread_id,
            delivery_id=str(uuid4()),
            role="event",
            content=text,
            # One source contract for every server-injected persistent input
            # (see session_wake._deliver_durable).
            source="officer_wake",
        )
        return _sw._delivery_state_for_thread(result, thread_id) is not None
    except Exception:
        logger.warning("officer notice: delivery failed (non-fatal)", exc_info=True)
        return False


async def deliver_staged_officer_routes(
    routes: list[dict[str, Any]],
    *,
    reason: str,
    dependencies: OfficerNoticeDependencies,
) -> int:
    """Deliver durable route fallback intents after their transaction commits.

    A failed dispatch leaves ``user_delivery_at`` null, which is the routing
    reconciler's retry contract. Delivery can therefore never roll back or
    falsify a hold/decommission transition.
    """
    if not routes:
        return 0
    from orchestrator.services import message_routing as _routing_svc

    delivered = 0
    for route in routes:
        try:
            if await _routing_svc.deliver_route_to_user(
                dependencies.store, route, reason=reason
            ):
                delivered += 1
        except Exception:
            logger.warning(
                "Officer route %s delivery failed after durable %s transition; "
                "leaving it retryable",
                str(route.get("route_id") or "")[:8],
                reason,
                exc_info=True,
            )
    return delivered


__all__ = [
    "OfficerNoticeDependencies",
    "deliver_staged_officer_routes",
    "inject_officer_notice",
]
