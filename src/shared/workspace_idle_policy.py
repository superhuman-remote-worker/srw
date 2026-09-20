"""Human-wait clocks and vetoes, with no physical release or access authority.

Callers supply authenticated runtime/claim facts and post-lock database time.
Eligibility only admits consideration by the existing retirement/capture funnel;
it never proves process zero, checkpoint durability, or permission to delete.
The initial identity view supports VM and remote Kubernetes UUID generations.
Manifest integer attachment generations and static Docker require other adapters.
"""

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
import json
from uuid import UUID, uuid4


DEFAULT_WARM_SECONDS = 900
_WAIT_KINDS = {
    "human_message",
    "human_approval",
    "human_review",
    "human_pause",
    "natural_pause",
}
_LEASE_KINDS = {"execution", "dependent", "access", "operation"}
_MAX_INTEGER = 2**63 - 1


class IdlePolicyError(ValueError):
    """Only bounded reason codes cross this pure boundary."""


@dataclass(frozen=True, slots=True)
class RuntimeIdentity:
    owner_kind: str
    owner_id: str
    backend: str
    runtime_generation: str
    runtime_uid: str


@dataclass(frozen=True, slots=True)
class IdleEpisode:
    episode_id: str
    revision: int
    wait_kind: str
    wait_key: str
    entered_at: datetime
    override_until: datetime | None
    extend_count: int
    runtime_identity: RuntimeIdentity


@dataclass(frozen=True, slots=True)
class ActivityLeaseView:
    kind: str
    runtime_identity: RuntimeIdentity
    expires_at: datetime
    proven_live: bool


@dataclass(frozen=True, slots=True)
class IdleDecision:
    state: str
    reason: str
    due_at: datetime | None = None
    episode_id: str | None = None
    revision: int | None = None


@dataclass(frozen=True, slots=True)
class IdleTransition:
    episode: IdleEpisode | None
    revision: int


def _integer(value, *, minimum=0):
    if type(value) is not int or not minimum <= value <= _MAX_INTEGER:
        raise IdlePolicyError("invalid_idle_state")
    return value


def _member(value, choices):
    return isinstance(value, str) and value in choices


def _uuid(value):
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except ValueError:
        raise IdlePolicyError("invalid_idle_identity") from None


def _time(value):
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise IdlePolicyError("invalid_idle_time")


def _runtime(value):
    if (
        not isinstance(value, RuntimeIdentity)
        or not _member(value.owner_kind, {"job", "thread"})
        or not _member(value.backend, {"vm", "kubernetes"})
    ):
        raise IdlePolicyError("invalid_idle_identity")
    for identity in (value.owner_id, value.runtime_generation, value.runtime_uid):
        _uuid(identity)


def _episode(value):
    if not isinstance(value, IdleEpisode) or not _member(value.wait_kind, _WAIT_KINDS):
        raise IdlePolicyError("invalid_idle_state")
    _uuid(value.episode_id)
    _uuid(value.wait_key)
    _integer(value.revision, minimum=1)
    _integer(value.extend_count)
    if (value.extend_count > 0) != (value.override_until is not None):
        raise IdlePolicyError("invalid_idle_state")
    _time(value.entered_at)
    _runtime(value.runtime_identity)
    if value.override_until is not None:
        _time(value.override_until)
        if value.override_until < value.entered_at:
            raise IdlePolicyError("invalid_idle_time")


def _due(episode, warm_seconds):
    if warm_seconds is None:
        return None
    _integer(warm_seconds, minimum=1)
    try:
        due = episode.entered_at + timedelta(seconds=warm_seconds)
    except OverflowError:
        raise IdlePolicyError("invalid_idle_time") from None
    return max(due, episode.override_until or due)


def evaluate_idle(
    episode,
    *,
    now,
    runtime,
    human_wait_current,
    supported,
    enabled=False,
    warm_seconds=DEFAULT_WARM_SECONDS,
    leases=(),
    control_hold=False,
    recovery_hold=False,
    cleanup_hold=False,
    restore_hold=False,
):
    """Expiry removes a policy veto; subsequent physical proof is still required."""
    try:
        _time(now)
        _runtime(runtime)
        if any(
            type(flag) is not bool
            for flag in (
                human_wait_current,
                supported,
                enabled,
                control_hold,
                recovery_hold,
                cleanup_hold,
                restore_hold,
            )
        ):
            raise IdlePolicyError("invalid_idle_state")
        if episode is None:
            return IdleDecision("blocked", "not_human_wait")
        _episode(episode)
        if episode.entered_at > now:
            raise IdlePolicyError("invalid_idle_time")
        due = _due(episode, warm_seconds)

        def result(state, reason):
            return IdleDecision(
                state, reason, due, episode.episode_id, episode.revision
            )

        if episode.runtime_identity != runtime:
            return result("blocked", "runtime_changed")
        if not enabled:
            return result("blocked", "policy_disabled")
        if not supported:
            return result("blocked", "unsupported_runtime")
        if not human_wait_current:
            return result("blocked", "not_human_wait")
        if not isinstance(leases, (list, tuple)) or len(leases) > 1024:
            raise IdlePolicyError("invalid_activity_lease")
        activity = False
        for lease in leases:
            if (
                not isinstance(lease, ActivityLeaseView)
                or not _member(lease.kind, _LEASE_KINDS)
                or type(lease.proven_live) is not bool
            ):
                raise IdlePolicyError("invalid_activity_lease")
            _runtime(lease.runtime_identity)
            _time(lease.expires_at)
            if not lease.proven_live:
                return result("blocked", "activity_authority_unproven")
            if lease.runtime_identity != runtime:
                return result("blocked", "activity_runtime_changed")
            activity |= lease.expires_at > now
        for reason, held in (
            ("control_hold", control_hold),
            ("recovery_hold", recovery_hold),
            ("cleanup_hold", cleanup_hold),
            ("restore_hold", restore_hold),
        ):
            if held:
                return result("held", reason)
        if warm_seconds is None:
            return result("held", "never_suspend")
        if activity:
            return result("held", "activity_lease")
        return (
            result("warm", "human_wait_warm")
            if now < due
            else result("eligible", "human_wait_elapsed")
        )
    except IdlePolicyError as exc:
        return IdleDecision("blocked", str(exc))


def transition_idle_episode(
    prior,
    *,
    revision,
    expected_episode_id,
    event,
    runtime,
    now,
    wait_kind=None,
    wait_key=None,
    warm_seconds=DEFAULT_WARM_SECONDS,
    extension_cap=4,
):
    """Reduce an already authorized semantic event under an owner revision fence.

    The DB adapter must commit this result with the successful source transition.
    Presence is deliberately separate from actual input/turn/decision events.
    """
    _integer(revision)
    _time(now)
    _runtime(runtime)
    if prior is not None:
        _episode(prior)
        if prior.revision != revision or prior.episode_id != expected_episode_id:
            raise IdlePolicyError("episode_changed")
        if prior.entered_at > now:
            raise IdlePolicyError("invalid_idle_time")
        if (prior.runtime_identity.owner_kind, prior.runtime_identity.owner_id) != (
            runtime.owner_kind,
            runtime.owner_id,
        ):
            raise IdlePolicyError("runtime_owner_changed")
    elif expected_episode_id is not None:
        raise IdlePolicyError("episode_changed")
    if not _member(event, {"enter", "exit", "rebind", "extend", "presence"}):
        raise IdlePolicyError("invalid_idle_event")
    if event == "presence" or (event == "exit" and prior is None):
        return IdleTransition(prior, revision)
    if event == "enter":
        if not _member(wait_kind, _WAIT_KINDS):
            raise IdlePolicyError("invalid_idle_event")
        _uuid(wait_key)
        if (
            prior is not None
            and prior.wait_kind == wait_kind
            and prior.wait_key == wait_key
        ):
            if prior.runtime_identity == runtime:
                return IdleTransition(prior, revision)
            # Same question across an authenticated access-only wake.
            event = "rebind"
        else:
            next_revision = _integer(revision + 1)
            return IdleTransition(
                IdleEpisode(
                    str(uuid4()),
                    next_revision,
                    wait_kind,
                    wait_key,
                    now,
                    None,
                    0,
                    runtime,
                ),
                next_revision,
            )
    if event != "rebind" and prior is not None and prior.runtime_identity != runtime:
        raise IdlePolicyError("runtime_changed")
    if event == "exit":
        return IdleTransition(None, _integer(revision + 1))
    if prior is None:
        raise IdlePolicyError("episode_missing")
    if event == "rebind":
        if prior.runtime_identity == runtime:
            return IdleTransition(prior, revision)
        changed = replace(prior, runtime_identity=runtime)
    else:
        _integer(extension_cap)
        due = _due(prior, warm_seconds)
        if due is None:
            return IdleTransition(prior, revision)
        if prior.extend_count >= extension_cap:
            raise IdlePolicyError("extension_limit")
        try:
            until = max(due, now + timedelta(seconds=warm_seconds))
        except OverflowError:
            raise IdlePolicyError("invalid_idle_time") from None
        changed = replace(
            prior, override_until=until, extend_count=_integer(prior.extend_count + 1)
        )
    next_revision = _integer(revision + 1)
    return IdleTransition(replace(changed, revision=next_revision), next_revision)


def episode_document(episode):
    """Revision belongs to the native owner column, never a second JSON copy."""
    if episode is None:
        return None
    _episode(episode)
    result = asdict(episode)
    result.pop("revision")
    result.update(
        version=1,
        entered_at=episode.entered_at.isoformat(),
        override_until=episode.override_until.isoformat()
        if episode.override_until
        else None,
    )
    return result


def read_episode(document, *, revision):
    _integer(revision)
    if document is None:
        return None
    try:
        if (
            not isinstance(document, dict)
            or set(document)
            != {
                "version",
                "episode_id",
                "wait_kind",
                "wait_key",
                "entered_at",
                "override_until",
                "extend_count",
                "runtime_identity",
            }
            or type(document["version"]) is not int
            or document["version"] != 1
        ):
            raise ValueError
        if len(json.dumps(document, allow_nan=False).encode()) > 4096:
            raise ValueError
        runtime = document["runtime_identity"]
        if not isinstance(runtime, dict) or set(runtime) != {
            "owner_kind",
            "owner_id",
            "backend",
            "runtime_generation",
            "runtime_uid",
        }:
            raise ValueError
        result = IdleEpisode(
            document["episode_id"],
            revision,
            document["wait_kind"],
            document["wait_key"],
            datetime.fromisoformat(document["entered_at"]),
            datetime.fromisoformat(document["override_until"])
            if document["override_until"] is not None
            else None,
            document["extend_count"],
            RuntimeIdentity(**runtime),
        )
        _episode(result)
        return result
    except (ValueError, TypeError, OverflowError):
        raise IdlePolicyError("invalid_idle_state") from None
