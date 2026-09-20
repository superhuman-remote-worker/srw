"""Pure nomination only; SQL commits authority, fairness and reservations together."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from shared.vm_resource_admission import ResourceAdmissionError


def _integer(value, *, minimum=0, maximum=2**63 - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ResourceAdmissionError("invalid_fairness")


def _uuid(value):
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except ValueError:
        raise ResourceAdmissionError("invalid_fairness") from None


def _time(value):
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ResourceAdmissionError("invalid_fairness")


@dataclass(frozen=True)
class Waiter:
    request_id: str
    owner_key: str
    enqueued_at: datetime
    priority: int
    bypasses: int
    protected_order: int | None

    def __post_init__(self):
        _uuid(self.request_id)
        _time(self.enqueued_at)
        _integer(self.priority, minimum=-(2**31), maximum=2**31 - 1)
        _integer(self.bypasses)
        if self.protected_order is not None:
            _integer(self.protected_order)
        if not isinstance(self.owner_key, str) or not 1 <= len(self.owner_key) <= 253:
            raise ResourceAdmissionError("invalid_fairness")


@dataclass(frozen=True)
class Nomination:
    action: str
    request_id: str | None = None
    node_uid: str | None = None
    bypassed: tuple[str, ...] = ()


def choose_waiter(
    waiters,
    *,
    fit_nodes,
    nonfit_ids,
    owner_last_admitted,
    now,
    aging_seconds,
    max_bypasses,
):
    """Return an own-authority nomination, without mutating counters or demand.

    Inputs describe live waiters under the policy lock. Caller must nominate
    invalid/cancelled heads for maintenance under their own Job authority first.
    Missing fit is a temporary wait; only caller-proven size nonfit may be passed
    in nonfit_ids. Node lists are already ordered by validated placement policy.
    """
    _time(now)
    _integer(aging_seconds, minimum=1)
    _integer(max_bypasses)
    ids, groups, scores = set(), {}, {}
    for waiter in waiters:
        if not isinstance(waiter, Waiter) or waiter.request_id in ids:
            raise ResourceAdmissionError("invalid_fairness")
        ids.add(waiter.request_id)
        if now < waiter.enqueued_at:
            raise ResourceAdmissionError("fairness_clock_unproven")
        age = now - waiter.enqueued_at
        age_seconds = age.days * 86400 + age.seconds
        scores[waiter.request_id] = waiter.priority + age_seconds // aging_seconds
        groups.setdefault(waiter.owner_key, []).append(waiter)
    if not set(fit_nodes) <= ids or not set(nonfit_ids) <= ids:
        raise ResourceAdmissionError("invalid_fairness")
    for request_id, nodes in fit_nodes.items():
        if not isinstance(nodes, (list, tuple)) or len(set(nodes)) != len(nodes):
            raise ResourceAdmissionError("invalid_fairness")
        for node in nodes:
            _uuid(node)
        if nodes and request_id in nonfit_ids:
            raise ResourceAdmissionError("invalid_fairness")
    for sequence in owner_last_admitted.values():
        _integer(sequence)

    # The SQL admitting transaction must assign protection atomically when a
    # committed bypass reaches the limit. Do not let an inconsistent/imported
    # threshold row disappear behind a newer high-priority owner head: nominate
    # its own-authority repair before any admission. Zero-bypass policy protects
    # only the selected temporarily blocked head, not every arriving waiter.
    missing_protection = [
        waiter
        for group in groups.values()
        for waiter in group
        if max_bypasses > 0
        and waiter.bypasses >= max_bypasses
        and waiter.protected_order is None
    ]
    if missing_protection:
        repair = min(missing_protection, key=lambda w: (w.enqueued_at, w.request_id))
        return Nomination("protect", repair.request_id)

    def within_owner(waiter):
        if waiter.protected_order is not None:
            return (0, waiter.protected_order, waiter.enqueued_at, waiter.request_id)
        return (1, -scores[waiter.request_id], waiter.enqueued_at, waiter.request_id)

    heads = [min(group, key=within_owner) for group in groups.values()]

    def across_owners(waiter):
        if waiter.protected_order is not None:
            return (0, waiter.protected_order, 0, waiter.enqueued_at, waiter.request_id)
        return (
            1,
            -scores[waiter.request_id],
            owner_last_admitted.get(waiter.owner_key, 0),
            waiter.enqueued_at,
            waiter.request_id,
        )

    blocked = []
    for head in sorted(heads, key=across_owners):
        if head.request_id in nonfit_ids:
            return Nomination("nonfit", head.request_id)
        nodes = fit_nodes.get(head.request_id, ())
        if nodes:
            # Only the eventual committed reservation may increment these rows.
            return Nomination("admit", head.request_id, nodes[0], tuple(blocked))
        if head.protected_order is not None:
            return Nomination("wait", head.request_id)
        if head.bypasses >= max_bypasses:
            return Nomination("protect", head.request_id)
        blocked.append(head.request_id)
    return Nomination("wait", blocked[0] if blocked else None)
