from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from shared.vm_resource_fairness import Waiter, choose_waiter


NOW = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
NODE = str(uuid4())


def waiter(owner="owner-a", **kwargs):
    return Waiter(
        request_id=str(uuid4()),
        owner_key=owner,
        enqueued_at=kwargs.pop("enqueued_at", NOW - timedelta(seconds=10)),
        priority=kwargs.pop("priority", 5),
        bypasses=kwargs.pop("bypasses", 0),
        protected_order=kwargs.pop("protected_order", None),
        **kwargs,
    )


def choose(waiters, *, fits=None, nonfit=(), last=None, max_bypasses=2, now=NOW):
    return choose_waiter(
        waiters,
        fit_nodes=fits or {},
        nonfit_ids=set(nonfit),
        owner_last_admitted=last or {},
        now=now,
        aging_seconds=60,
        max_bypasses=max_bypasses,
    )


def test_owner_round_robin_within_equal_effective_priority():
    a, b = waiter(), waiter("owner-b")
    assert (
        choose(
            [a, b],
            fits={a.request_id: [NODE], b.request_id: [NODE]},
            last={a.owner_key: 8, b.owner_key: 7},
        ).request_id
        == b.request_id
    )


def test_priority_and_oldest_wait_select_owner_head():
    low, high = waiter(priority=1), waiter(priority=9)
    assert (
        choose(
            [low, high], fits={low.request_id: [NODE], high.request_id: [NODE]}
        ).request_id
        == high.request_id
    )
    old = replace(low, enqueued_at=NOW - timedelta(seconds=600))
    assert (
        choose(
            [old, high], fits={old.request_id: [NODE], high.request_id: [NODE]}
        ).request_id
        == old.request_id
    )


def test_wait_aging_eventually_beats_continually_new_higher_priority():
    older = waiter(priority=1)
    future = NOW + timedelta(seconds=600)
    newer = waiter("owner-b", priority=9, enqueued_at=future)
    assert (
        choose(
            [older, newer],
            fits={older.request_id: [NODE], newer.request_id: [NODE]},
            now=future,
        ).request_id
        == older.request_id
    )


def test_skipped_head_is_counted_only_in_proposed_committed_admission():
    blocked, fitting = waiter(priority=10), waiter("owner-b")
    decision = choose([blocked, fitting], fits={fitting.request_id: [NODE]})
    assert decision.action == "admit" and decision.request_id == fitting.request_id
    assert decision.bypassed == (blocked.request_id,)
    assert blocked.bypasses == 0
    assert choose([blocked, fitting]).bypassed == ()


@pytest.mark.parametrize("max_bypasses,bypasses", [(0, 0), (2, 2)])
def test_blocked_head_at_limit_prevents_backfill(max_bypasses, bypasses):
    blocked, fitting = waiter(priority=10, bypasses=bypasses), waiter("owner-b")
    decision = choose(
        [blocked, fitting], fits={fitting.request_id: [NODE]}, max_bypasses=max_bypasses
    )
    assert decision.action == "protect" and decision.request_id == blocked.request_id
    assert decision.bypassed == ()


def test_protection_survives_new_priority_and_owner_head_changes():
    protected = waiter(priority=1, protected_order=10)
    same_owner = waiter(priority=100)
    other = waiter("owner-b", priority=200)
    decision = choose(
        [protected, same_owner, other],
        fits={same_owner.request_id: [NODE], other.request_id: [NODE]},
    )
    assert decision.action == "wait" and decision.request_id == protected.request_id
    assert (
        choose(
            [protected, same_owner, other],
            fits={protected.request_id: [NODE], other.request_id: [NODE]},
        ).request_id
        == protected.request_id
    )


def test_multiple_protected_heads_keep_persisted_protection_order():
    first, second = (
        waiter(protected_order=5),
        waiter("owner-b", protected_order=6, priority=100),
    )
    decision = choose(
        [second, first], fits={first.request_id: [NODE], second.request_id: [NODE]}
    )
    assert decision.request_id == first.request_id


def test_nonfit_is_nominated_for_own_authority_transition_without_bypass():
    impossible, fitting = waiter(protected_order=1), waiter("owner-b")
    decision = choose(
        [impossible, fitting],
        fits={fitting.request_id: [NODE]},
        nonfit={impossible.request_id},
    )
    assert decision.action == "nonfit" and decision.request_id == impossible.request_id
    assert decision.bypassed == ()
    assert (
        choose([fitting], fits={fitting.request_id: [NODE]}).request_id
        == fitting.request_id
    )


def test_input_order_does_not_change_nomination_or_node_choice():
    a, b = waiter(), waiter("owner-b")
    fits = {a.request_id: [NODE], b.request_id: [NODE]}
    assert choose([a, b], fits=fits) == choose([b, a], fits=fits)


def test_missing_placement_evidence_is_wait_not_nonfit():
    blocked = waiter()
    decision = choose([blocked])
    assert decision.action == "wait" and decision.bypassed == ()


def test_threshold_waiter_without_marker_is_protected_before_new_owner_head():
    threshold = waiter(priority=1, bypasses=2)
    incoming = waiter(priority=100)
    result = choose([threshold, incoming], fits={incoming.request_id: [NODE]})
    assert result.action == "protect" and result.request_id == threshold.request_id
    assert result.bypassed == ()
