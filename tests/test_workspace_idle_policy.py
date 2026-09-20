"""Idle eligibility is a clock/hold decision, never physical release authority."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest


def setup():
    from shared.workspace_idle_policy import RuntimeIdentity, IdleEpisode

    entered = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
    runtime = RuntimeIdentity("job", str(uuid4()), "vm", str(uuid4()), str(uuid4()))
    episode = IdleEpisode(
        str(uuid4()), 7, "human_message", str(uuid4()), entered, None, 0, runtime
    )
    return entered, runtime, episode


def evaluate(episode, runtime, now, **kwargs):
    from shared.workspace_idle_policy import evaluate_idle

    values = dict(enabled=True, supported=True, human_wait_current=True)
    values.update(kwargs)
    return evaluate_idle(episode, now=now, runtime=runtime, **values)


@pytest.mark.parametrize(
    "seconds,state", [(899, "warm"), (900, "eligible"), (93600, "eligible")]
)
def test_default_human_wait_boundary(seconds, state):
    entered, runtime, episode = setup()
    decision = evaluate(episode, runtime, entered + timedelta(seconds=seconds))
    assert decision.state == state
    assert decision.due_at == entered + timedelta(seconds=900)
    assert (decision.episode_id, decision.revision) == (episode.episode_id, 7)


@pytest.mark.parametrize(
    "field", ["control_hold", "recovery_hold", "cleanup_hold", "restore_hold"]
)
def test_existing_lifecycle_hold_vetoes_elapsed_wait(field):
    entered, runtime, episode = setup()
    result = evaluate(episode, runtime, entered + timedelta(hours=26), **{field: True})
    assert result.state == "held" and result.reason == field


@pytest.mark.parametrize(
    "settings,reason",
    [
        ({"enabled": False}, "policy_disabled"),
        ({"supported": False}, "unsupported_runtime"),
        ({"human_wait_current": False}, "not_human_wait"),
    ],
)
def test_elapsed_capacity_or_unsupported_state_does_not_become_idle(settings, reason):
    entered, runtime, episode = setup()
    result = evaluate(episode, runtime, entered + timedelta(hours=26), **settings)
    assert result.state == "blocked" and result.reason == reason


def test_expiring_activity_removes_only_policy_veto_and_never_resets_clock():
    from shared.workspace_idle_policy import ActivityLeaseView

    entered, runtime, episode = setup()
    expires = entered + timedelta(hours=1)
    lease = ActivityLeaseView("execution", runtime, expires, True)
    result = evaluate(
        episode, runtime, expires - timedelta(microseconds=1), leases=(lease,)
    )
    assert result.state == "held" and result.reason == "activity_lease"
    expired = evaluate(episode, runtime, expires, leases=(lease,))
    assert expired.state == "eligible" and expired.due_at == entered + timedelta(
        seconds=900
    )
    assert episode.entered_at == entered and episode.revision == 7


def test_unknown_lease_authority_and_wrong_runtime_fail_closed():
    from shared.workspace_idle_policy import ActivityLeaseView

    entered, runtime, episode = setup()
    now = entered + timedelta(hours=1)
    for lease in (
        ActivityLeaseView("access", runtime, entered, False),
        ActivityLeaseView(
            "access", replace(runtime, runtime_uid=str(uuid4())), now, True
        ),
    ):
        assert evaluate(episode, runtime, now, leases=(lease,)).state == "blocked"


@pytest.mark.parametrize(
    "change",
    [
        "future",
        "naive",
        "bad_uid",
        "boolean_revision",
        "integer_generation",
        "wait_kind",
        "backend",
        "owner_kind",
    ],
)
def test_malformed_or_future_episode_cannot_authorize_eligibility(change):
    entered, runtime, episode = setup()
    now = entered + timedelta(minutes=20)
    if change == "future":
        episode = replace(episode, entered_at=now + timedelta(seconds=1))
    elif change == "naive":
        episode = replace(episode, entered_at=entered.replace(tzinfo=None))
    elif change == "bad_uid":
        episode = replace(
            episode, runtime_identity=replace(runtime, runtime_uid="node-name")
        )
    elif change == "boolean_revision":
        episode = replace(episode, revision=True)
    elif change == "integer_generation":
        episode = replace(
            episode, runtime_identity=replace(runtime, runtime_generation=3)
        )
    elif change == "wait_kind":
        episode = replace(episode, wait_kind=[])
    else:
        episode = replace(episode, runtime_identity=replace(runtime, **{change: []}))
    assert evaluate(episode, runtime, now).state == "blocked"


def test_explicit_never_and_extension_override_preserve_original_entry():
    entered, runtime, episode = setup()
    now = entered + timedelta(hours=3)
    assert evaluate(episode, runtime, now, warm_seconds=None).reason == "never_suspend"
    extended = replace(
        episode, override_until=now + timedelta(seconds=1), extend_count=1
    )
    assert evaluate(extended, runtime, now).state == "warm"
    assert extended.entered_at == entered


def transition(prior, event, runtime, now, **kwargs):
    from shared.workspace_idle_policy import transition_idle_episode

    values = dict(
        revision=prior.revision if prior else 0,
        expected_episode_id=prior.episode_id if prior else None,
    )
    values.update(kwargs)
    return transition_idle_episode(
        prior, event=event, runtime=runtime, now=now, **values
    )


def test_duplicate_wait_preserves_id_time_and_revision():
    entered, runtime, episode = setup()
    result = transition(
        episode,
        "enter",
        runtime,
        entered + timedelta(minutes=2),
        wait_kind=episode.wait_kind,
        wait_key=episode.wait_key,
    )
    assert result.episode == episode and result.revision == 7


def test_different_question_and_exit_reentry_advance_fence():
    entered, runtime, episode = setup()
    question = str(uuid4())
    result = transition(
        episode,
        "enter",
        runtime,
        entered + timedelta(minutes=2),
        wait_kind="human_approval",
        wait_key=question,
    )
    assert result.episode.episode_id != episode.episode_id
    assert result.episode.entered_at == entered + timedelta(minutes=2)
    assert result.revision == 8
    ended = transition(result.episode, "exit", runtime, entered + timedelta(minutes=3))
    assert ended.episode is None and ended.revision == 9
    restarted = transition(
        None,
        "enter",
        runtime,
        entered + timedelta(minutes=4),
        revision=9,
        wait_kind="human_message",
        wait_key=str(uuid4()),
    )
    assert restarted.revision == 10


def test_access_only_wake_rebind_preserves_question_age_and_extensions():
    entered, runtime, episode = setup()
    episode = replace(
        episode, override_until=entered + timedelta(hours=1), extend_count=2
    )
    successor = replace(
        runtime, runtime_uid=str(uuid4()), runtime_generation=str(uuid4())
    )
    result = transition(episode, "rebind", successor, entered + timedelta(minutes=30))
    assert result.episode.runtime_identity == successor and result.revision == 8
    assert result.episode.episode_id == episode.episode_id
    assert result.episode.entered_at == entered
    assert (
        result.episode.override_until == episode.override_until
        and result.episode.extend_count == 2
    )
    assert (
        evaluate(episode, successor, entered + timedelta(hours=2)).reason
        == "runtime_changed"
    )


def test_presence_event_is_noop_and_cannot_reset_extension_cap():
    entered, runtime, episode = setup()
    episode = replace(
        episode, extend_count=4, override_until=entered + timedelta(hours=1)
    )
    result = transition(episode, "presence", runtime, entered + timedelta(hours=2))
    assert result.episode == episode and result.revision == 7


def test_extend_rearms_effective_interval_without_changing_entry_and_caps_clicks():
    from shared.workspace_idle_policy import IdlePolicyError

    entered, runtime, episode = setup()
    for count in range(1, 5):
        now = entered + timedelta(minutes=10 * count)
        result = transition(
            episode, "extend", runtime, now, warm_seconds=900, extension_cap=4
        )
        episode = result.episode
        assert episode.entered_at == entered
        assert episode.override_until == now + timedelta(seconds=900)
        assert episode.extend_count == count
    with pytest.raises(IdlePolicyError, match="extension_limit"):
        transition(episode, "extend", runtime, now, warm_seconds=900, extension_cap=4)


def test_stale_reply_or_revision_cannot_close_new_episode():
    from shared.workspace_idle_policy import IdlePolicyError

    entered, runtime, episode = setup()
    for kwargs in ({"expected_episode_id": str(uuid4())}, {"revision": 6}):
        with pytest.raises(IdlePolicyError, match="episode_changed"):
            transition(
                episode, "exit", runtime, entered + timedelta(minutes=1), **kwargs
            )


def test_episode_serialization_excludes_revision_and_roundtrips_exact_identity():
    from shared.workspace_idle_policy import episode_document, read_episode

    entered, runtime, episode = setup()
    document = episode_document(episode)
    assert "revision" not in document and document["version"] == 1
    assert document["entered_at"] == "2026-09-20T12:00:00+00:00"
    assert read_episode(document, revision=7) == episode


@pytest.mark.parametrize("event,kind", [([], "human_message"), ("enter", {})])
def test_malformed_transition_tags_have_bounded_failure(event, kind):
    from shared.workspace_idle_policy import IdlePolicyError

    entered, runtime, episode = setup()
    with pytest.raises(IdlePolicyError, match="invalid_idle_event"):
        transition(
            episode, event, runtime, entered, wait_kind=kind, wait_key=str(uuid4())
        )


def test_malformed_lease_kind_is_blocked():
    from shared.workspace_idle_policy import ActivityLeaseView

    entered, runtime, episode = setup()
    lease = ActivityLeaseView([], runtime, entered + timedelta(hours=1), True)
    assert (
        evaluate(episode, runtime, entered + timedelta(hours=1), leases=(lease,)).state
        == "blocked"
    )


@pytest.mark.parametrize("count,override", [(4, False), (0, True)])
def test_partial_extension_state_cannot_erase_or_invent_a_hold(count, override):
    from shared.workspace_idle_policy import (
        episode_document,
        read_episode,
        IdlePolicyError,
    )

    entered, runtime, episode = setup()
    changed = replace(
        episode,
        extend_count=count,
        override_until=entered + timedelta(hours=2) if override else None,
    )
    assert evaluate(changed, runtime, entered + timedelta(hours=1)).state == "blocked"
    document = episode_document(episode)
    document.update(
        extend_count=count,
        override_until=(entered + timedelta(hours=2)).isoformat() if override else None,
    )
    with pytest.raises(IdlePolicyError):
        read_episode(document, revision=7)
