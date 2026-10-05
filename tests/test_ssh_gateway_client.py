"""Tests for the SSH gateway's target-resolution client.

Builds ``GatewayConfig`` directly rather than going through ``load_config``:
``load_config`` now does real file I/O -- it opens and parses every
configured host key (rejecting anything that isn't Ed25519) and loads the
user CA key from disk (Task 1's ``load_user_ca``). ``resolve_target`` never
reads ``host_key_paths``, ``user_ca_path``, or ``allowed_origins`` -- only
``config.orchestrator_url``, ``config.internal_key`` and
``config.orchestrator_request_timeout`` -- so routing every test in this
file through real key generation and parsing would make them about
``ssh_gateway_config``'s file I/O, not about this module's HTTP behavior.
Mirrors the ``_gateway_config_kwargs`` pattern at
``tests/test_ssh_gateway_config.py:300``, which exists for the exact same
reason one field over (``GatewayConfig`` construction tests that don't want
``load_config``'s validation).

Deliberately does NOT import ``BASE_ENV`` from ``test_ssh_gateway_config``:
that fixture's real key material comes from a ``scope="session", autouse``
fixture defined in that module, which pytest only runs for tests collected
from that module.
"""

import json

import pytest

from orchestrator.services import ssh_gateway_targets
from orchestrator.services.ssh_gateway_client import (
    KEY_USE_BUMP_TIMEOUT_SECONDS,
    REFUSAL_MESSAGES,
    SshTarget,
    TargetDenied,
    TargetUnavailable,
    resolve_target,
)
from orchestrator.services.ssh_gateway_config import GatewayConfig

_UNSET = object()


def _gateway_config_kwargs(**overrides):
    base = dict(
        host_key_paths=("/k",),
        user_ca_path="/ca",
        orchestrator_url="http://orchestrator:8085",
        internal_key="internal",
        allowed_origins=("https://cockpit.example",),
    )
    base.update(overrides)
    return base


def _config(**overrides) -> GatewayConfig:
    return GatewayConfig(**_gateway_config_kwargs(**overrides))


def _live_payload(**overrides):
    payload = {
        "thread_id": "t1",
        "user_id": "u1",
        "pod_ip": "10.1.2.3",
        "pod_port": 30022,
        "host_key_fingerprint": "SHA256:xyz",
        "state": "live",
        "execution_lane": "stateless",
        "no_boot_watchdog": True,
    }
    payload.update(overrides)
    return payload


class FakeResponse:
    def __init__(self, status, payload=_UNSET):
        self.status_code = status
        # `payload if payload is not _UNSET else {}` rather than the more
        # obvious `payload or {}`: a JSON list/string/null test payload
        # (`[]`, `""`, `None`) is falsy, so `or {}` would silently replace an
        # intentional non-dict payload with an empty dict and the review
        # 3.1 tests below would never actually exercise a non-dict body.
        self._payload = {} if payload is _UNSET else payload

    def json(self):
        return self._payload


class NonJsonResponse:
    """A 200 whose body isn't JSON at all -- e.g. an ingress or error page.

    ``httpx.Response.json()`` is ``json.loads(self.content)``, which raises
    ``json.JSONDecodeError`` (a ``ValueError`` subclass) on a non-JSON body.
    """

    status_code = 200

    def json(self):
        raise json.JSONDecodeError("Expecting value", "", 0)


@pytest.mark.asyncio
async def test_live_target_is_returned(monkeypatch):
    calls = []

    async def _get(url, headers=None, params=None, timeout=None):
        calls.append(
            {"url": url, "headers": headers, "params": params, "timeout": timeout}
        )
        return FakeResponse(200, _live_payload())

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    target = await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")

    assert isinstance(target, SshTarget)
    assert target.pod_port == 30022
    assert target.execution_lane == "stateless"

    # Review 4.3: these used to be assertions *inside* the fake, which
    # resolve_target calls inside its own `except Exception` -- a failure
    # there was swallowed and resurfaced as a misleading
    # `TargetUnavailable: unreachable` instead of the real AssertionError.
    # Recording the call and asserting after resolve_target returns points a
    # future failure at the real cause.
    assert len(calls) == 1
    assert calls[0]["headers"]["X-Internal-Key"] == "internal"
    assert calls[0]["params"]["fingerprint"].startswith("SHA256:")
    # Review 4.5: this was a hardcoded `timeout=10.0` literal with no config
    # lever; it must come from GatewayConfig, matching every sibling budget.
    assert calls[0]["timeout"] == 10.0


@pytest.mark.asyncio
async def test_orchestrator_request_timeout_is_configurable(monkeypatch):
    """The literal is gone: a non-default config value actually reaches
    _http_get, not just a field that exists but is never read."""
    calls = []

    async def _get(url, headers=None, params=None, timeout=None):
        calls.append(timeout)
        return FakeResponse(200, _live_payload())

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    await resolve_target(
        _config(orchestrator_request_timeout=2.5), "s-7f3a91c2", "SHA256:abc"
    )
    assert calls == [2.5]


@pytest.mark.asyncio
async def test_legacy_live_target_cannot_be_mistaken_for_stateless(monkeypatch):
    async def _get(*_args, **_kwargs):
        return FakeResponse(200, _live_payload(execution_lane=None, no_boot_watchdog=None))

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetUnavailable) as exc:
        await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert exc.value.state == "stale_binding"


@pytest.mark.asyncio
async def test_404_is_denial_not_unavailability(monkeypatch):
    """Unknown handle, unknown key and unauthorized are one opaque case."""

    async def _get(url, headers=None, params=None, timeout=None):
        return FakeResponse(404, {"detail": "No such workspace"})

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetDenied):
        await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")


@pytest.mark.asyncio
async def test_non_live_state_raises_with_the_state(monkeypatch):
    async def _get(url, headers=None, params=None, timeout=None):
        return FakeResponse(
            200,
            _live_payload(
                pod_ip=None,
                pod_port=None,
                host_key_fingerprint=None,
                state="suspended",
            ),
        )

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetUnavailable) as excinfo:
        await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert excinfo.value.state == "suspended"


@pytest.mark.asyncio
async def test_orchestrator_failure_fails_closed(monkeypatch):
    async def _get(url, headers=None, params=None, timeout=None):
        raise OSError("connection refused")

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetUnavailable) as excinfo:
        await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert excinfo.value.state == "unreachable"


# ---------------------------------------------------------------------------
# Review Critical 3.0: handle is the SSH username and is fully
# attacker-controlled. Unvalidated, it is interpolated straight into the URL
# path, and httpx normalises ".." path segments -- an authenticated
# GET-request-forgery primitive into arbitrary orchestrator internal routes,
# carrying config.internal_key with it.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_path_traversal_is_denied_without_an_http_call(monkeypatch):
    """One '../' is the working payload -- verified independently by the
    coordinator; the review's own two-segment example
    ('../../api/internal/agent-token') is off by one segment and actually
    misses:

        '../../api/internal/agent-token'
            -> /api/internal/ssh-targets/../../api/internal/agent-token
            -> normalises to /api/api/internal/agent-token        (misses)
        '../agent-token'
            -> /api/internal/ssh-targets/../agent-token
            -> normalises to /api/internal/agent-token             (HITS)

    Asserts _http_get is never even called for an invalid handle -- a
    reverted fix would let this fake "hit" (return a live-shaped 200), which
    is exactly what an attacker's forged request would look like from the
    gateway's side.
    """
    calls = []

    async def _get(url, headers=None, params=None, timeout=None):
        calls.append(url)
        return FakeResponse(200, _live_payload())

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetDenied):
        await resolve_target(_config(), "../agent-token", "SHA256:abc")
    assert calls == []


@pytest.mark.asyncio
async def test_ordinary_handle_still_reaches_the_http_call(monkeypatch):
    """Negative control for the handle-validation gate itself: a
    well-formed handle must still work, proving the gate discriminates
    rather than just refusing everything."""
    calls = []

    async def _get(url, headers=None, params=None, timeout=None):
        calls.append(url)
        return FakeResponse(200, _live_payload())

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    target = await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert isinstance(target, SshTarget)
    assert len(calls) == 1


# ---------------------------------------------------------------------------
# Review Important 3.1: the malformed-payload guard (Ruling G13) started one
# line too late. response.json() and the state read were outside the try,
# so a non-JSON body, a non-dict payload, or an unhashable state all still
# escaped as unhandled exceptions.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_json_response_body_refuses_instead_of_crashing(monkeypatch):
    async def _get(url, headers=None, params=None, timeout=None):
        return NonJsonResponse()

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetUnavailable) as excinfo:
        await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert excinfo.value.state == "unreachable"


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [[], "oops", None], ids=["list", "string", "null"])
async def test_non_dict_payload_refuses_instead_of_crashing(monkeypatch, payload):
    async def _get(url, headers=None, params=None, timeout=None):
        return FakeResponse(200, payload)

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetUnavailable) as excinfo:
        await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert excinfo.value.state == "unreachable"


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [["a", "list"], {"a": "dict"}], ids=["list", "dict"])
async def test_unhashable_state_refuses_instead_of_crashing(monkeypatch, state):
    """`state in REFUSAL_MESSAGES` raises `TypeError: unhashable type` for a
    list/dict state -- the report's own claim that this branch "handles
    absent/unexpected values without crashing" was false for this case
    (review §1/§4.6)."""

    async def _get(url, headers=None, params=None, timeout=None):
        return FakeResponse(200, {"state": state})

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetUnavailable) as excinfo:
        await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert excinfo.value.state == "unreachable"


@pytest.mark.asyncio
async def test_live_payload_with_null_port_refuses_instead_of_crashing(monkeypatch):
    """Ruling G13: ``state: "live"`` is not proof the rest of the payload is
    well-formed. ``int(None)`` raises ``TypeError`` -- that must become a
    readable refusal, not an unhandled exception surfacing mid-authentication
    in the SSH server."""

    async def _get(url, headers=None, params=None, timeout=None):
        return FakeResponse(200, _live_payload(pod_port=None))

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetUnavailable) as excinfo:
        await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert excinfo.value.state == "unreachable"


@pytest.mark.asyncio
async def test_live_payload_missing_a_field_refuses_instead_of_crashing(monkeypatch):
    """Same defect, the ``KeyError`` half: a "live" payload missing a
    required field must not surface as an unhandled ``KeyError`` either."""

    async def _get(url, headers=None, params=None, timeout=None):
        payload = _live_payload()
        del payload["host_key_fingerprint"]
        return FakeResponse(200, payload)

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetUnavailable) as excinfo:
        await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert excinfo.value.state == "unreachable"


# ---------------------------------------------------------------------------
# Review Important 3.2: the live guard is type-blind. A frozen dataclass
# performs no runtime validation, so a "live" state with a null pod_ip, or a
# bool/float/out-of-range pod_port, still produced a usable-looking
# SshTarget. thread_id joined this guard once connect_upstream started
# minting each certificate's principal from it directly (Task 9 review):
# SshUserCa.mint raising on an empty principal is a fail-closed backstop,
# not the readable refusal this module otherwise promises.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"pod_ip": None},
        {"pod_ip": ""},
        {"pod_port": True},
        {"pod_port": 3.9},
        {"pod_port": 0},
        {"pod_port": 70000},
        {"pod_port": "30022"},
        {"host_key_fingerprint": None},
        {"host_key_fingerprint": ""},
        {"thread_id": None},
        {"thread_id": ""},
    ],
    ids=[
        "pod_ip-null",
        "pod_ip-empty",
        "pod_port-bool-true",
        "pod_port-float",
        "pod_port-zero",
        "pod_port-too-large",
        "pod_port-numeric-string",
        "fingerprint-null",
        "fingerprint-empty",
        "thread_id-null",
        "thread_id-empty",
    ],
)
async def test_live_payload_with_an_invalid_field_refuses_instead_of_returning_a_target(
    monkeypatch, overrides
):
    """isinstance(True, int) is True and int(3.9) == 3 -- both must be
    excluded explicitly, not discovered by a bare isinstance/int() pair."""

    async def _get(url, headers=None, params=None, timeout=None):
        return FakeResponse(200, _live_payload(**overrides))

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetUnavailable) as excinfo:
        await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert excinfo.value.state == "unreachable"


@pytest.mark.asyncio
async def test_live_payload_with_valid_fields_is_still_accepted(monkeypatch):
    """Negative control for the validation above: an ordinary well-formed
    live payload must still come back as a usable SshTarget."""

    async def _get(url, headers=None, params=None, timeout=None):
        return FakeResponse(200, _live_payload())

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    target = await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert target.pod_ip == "10.1.2.3"
    assert target.pod_port == 30022
    assert target.host_key_fingerprint == "SHA256:xyz"


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["failed", "deleted", "stale_binding"])
async def test_the_three_added_states_are_not_collapsed_to_unreachable(
    monkeypatch, state
):
    """Ruling G10: before this fix, any state absent from REFUSAL_MESSAGES
    (which "failed"/"deleted"/"stale_binding" all were) fell back to
    "unreachable" -- telling whoever reads the refusal that the gateway
    couldn't reach the control plane, when it actually reached it and got a
    specific, actionable answer. See ssh_gateway_targets.py's own comments on
    STATE_FAILED/STATE_DELETED/STATE_STALE_BINDING for why collapsing these
    "sends an operator after the wrong problem"."""

    async def _get(url, headers=None, params=None, timeout=None):
        return FakeResponse(
            200,
            _live_payload(
                pod_ip=None, pod_port=None, host_key_fingerprint=None, state=state
            ),
        )

    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_get", _get)
    with pytest.raises(TargetUnavailable) as excinfo:
        await resolve_target(_config(), "s-7f3a91c2", "SHA256:abc")
    assert excinfo.value.state == state


def _non_live_states() -> list[str]:
    """Every ``STATE_*`` constant ``ssh_gateway_targets.py`` defines, except
    "live".

    Introspected rather than hardcoded (Ruling G10): a state added upstream
    to ``ssh_gateway_targets.py`` is picked up here automatically, so if
    ``REFUSAL_MESSAGES`` is not updated to match, this test fails instead of
    ``resolve_target`` silently degrading the new state to "unreachable".

    Review 4.4: this depends on one convention holding upstream -- every
    state is a module-level ``STATE_*`` string constant. A new state added
    under a different shape (an enum member, a differently-prefixed
    constant, a bare string literal inside ``resolve_workspace_state``) is
    not something this introspection can see; the
    ``{"failed", "deleted", "stale_binding"} <= set(states)`` floor below
    only catches *total* breakage of that convention, not the loss of one
    member under it.
    """
    return [
        value
        for name, value in vars(ssh_gateway_targets).items()
        if name.startswith("STATE_")
        and isinstance(value, str)
        and value != ssh_gateway_targets.STATE_LIVE
    ]


def test_every_state_has_a_message_and_exit_code():
    states = _non_live_states()
    # Sanity floor: if a future rename stopped the introspection above from
    # matching anything, the loop below would vacuously pass. Pin the three
    # states this ruling specifically added so that can't happen silently.
    assert {"failed", "deleted", "stale_binding"} <= set(states)
    for state in states:
        message, code = REFUSAL_MESSAGES[state]
        assert message and not message[0].isupper()
        assert code in (69, 75, 77)


def test_client_side_states_are_covered_too():
    """Review 4.1: "unreachable" and "denied" are synthesized by
    resolve_target itself (five call sites: the outer except, the non-200
    branch, the non-live fallback, and the payload-validation/malformed-
    payload except), not STATE_* constants from ssh_gateway_targets --
    _non_live_states() cannot see them by construction, so deleting either
    entry from REFUSAL_MESSAGES went completely unnoticed before this test
    existed. A downstream REFUSAL_MESSAGES[exc.state] lookup would KeyError
    at the exact moment you least want an exception: while printing a
    refusal."""
    assert {"unreachable", "denied"} <= REFUSAL_MESSAGES.keys()


def test_every_message_is_distinct():
    """Review 4.2: two hardcoded pairs (suspended/reclaimed,
    failed/deleted/stale_binding/never_provisioned) only spot-checked
    distinctness -- making `restoring` silently reuse
    `never_provisioned`'s text still passed 12/12. This is the structural
    form of the property: every message in the table must be unique, since
    a repeated message hides which of two different problems the user
    actually has (e.g. a reclaimed volume is not the same promise as a
    merely suspended one, and none of "failed"/"deleted"/"stale_binding"
    should read the same as the "never_provisioned" state they used to be
    misreported as, inside ssh_gateway_targets.py itself, before that
    module's own fix)."""
    messages = [message for message, _ in REFUSAL_MESSAGES.values()]
    assert len(set(messages)) == len(messages)


_EXPECTED_EXIT_CODES = {
    # 75 EX_TEMPFAIL -- genuinely retryable without anything else changing.
    # stale_binding lives here, not in the 69 group below: a resume/
    # re-provision plausibly rewrites the binding, so it is
    # user-recoverable the same way suspended/reclaimed are -- not the
    # same class as failed/deleted, where retrying is pointless. (Fix
    # round 2: this was originally mis-slotted at 69; the message text
    # was always right, only the code was wrong.)
    "suspended": 75,
    "reclaimed": 75,
    "ending": 75,
    "restoring": 75,
    "stale_binding": 75,
    # 69 EX_UNAVAILABLE -- broken or gone, not fixed by retrying alone.
    "failed": 69,
    "deleted": 69,
    "ended": 69,
    "never_provisioned": 69,
    "unreachable": 69,
    # 77 EX_NOPERM -- policy refusals.
    "denied": 77,
    "vm_unsupported": 77,
}


def test_exit_codes_are_pinned_per_state_not_just_in_the_legal_set():
    """Review 3.3: `assert code in (69, 75, 77)` alone still passes with
    every state set to the same code -- verified: setting every entry's
    code to 69 left the old suite at 12/12 green. An automated SSH retry
    wrapper keying on the exit code would then loop forever against a
    target that can never become live (vm_unsupported, failed, deleted,
    ended) or, conversely, give up immediately on one genuinely worth
    retrying (suspended, restoring). Pin each state's code individually.

    The set-equality assertion (not just iterating _EXPECTED_EXIT_CODES)
    also catches a state present in one table but not the other in either
    direction.
    """
    assert set(_EXPECTED_EXIT_CODES) == set(REFUSAL_MESSAGES)
    for state, expected_code in _EXPECTED_EXIT_CODES.items():
        _, code = REFUSAL_MESSAGES[state]
        assert code == expected_code, f"{state}: expected {expected_code}, got {code}"


# =====================================================================
# Task 6A's audit endpoints
#
# These three are WRITES, and they fail differently from resolve_target
# above: an audit failure raises AuditWriteFailed and every caller
# swallows it, because a bookkeeping write must never tear down a session
# that already authenticated. The tests below therefore care about two
# things a refusal test would not: that a bad response can never be
# mistaken for a good one, and that nothing here can manufacture a
# TargetDenied/TargetUnavailable the gateway would turn into a refusal.
# =====================================================================


class _PostRecorder:
    """Stands in for ``_http_post``, recording exactly what went on the wire."""

    def __init__(self, response=None, raises=None):
        self.calls = []
        self._response = response
        self._raises = raises

    async def __call__(self, url, headers=None, json=None, timeout=None):
        self.calls.append(
            {"url": url, "headers": headers, "json": json, "timeout": timeout}
        )
        if self._raises is not None:
            raise self._raises
        return self._response


def _patch_post(monkeypatch, recorder):
    import orchestrator.services.ssh_gateway_client as mod

    monkeypatch.setattr(mod, "_http_post", recorder)
    return recorder


@pytest.mark.asyncio
async def test_mark_key_used_posts_only_the_fingerprint(monkeypatch):
    """The gateway holds no key id at auth_completed time -- target
    resolution is lazy -- so this endpoint is keyed by fingerprint. Sending
    anything more would also be asserting an identity, which this module's
    docstring rules out."""
    from orchestrator.services.ssh_gateway_client import mark_key_used

    post = _patch_post(monkeypatch, _PostRecorder(FakeResponse(200, {"status": "ok"})))
    await mark_key_used(_config(), "SHA256:abc")

    assert len(post.calls) == 1
    call = post.calls[0]
    assert call["url"] == "http://orchestrator:8085/api/internal/ssh-keys/used"
    assert call["json"] == {"fingerprint": "SHA256:abc"}
    assert call["headers"] == {"X-Internal-Key": "internal"}
    assert call["timeout"] == KEY_USE_BUMP_TIMEOUT_SECONDS


@pytest.mark.asyncio
async def test_the_bump_gets_a_shorter_budget_than_the_other_audit_calls(monkeypatch):
    """asyncssh awaits auth_completed from inside packet processing and
    buffers the connection's further inbound packets while it runs
    (connection.py:1719-1725), so a slow bump hangs the user's first channel
    open. The other two audit calls run off the packet path and keep the
    full budget."""
    from orchestrator.services.ssh_gateway_client import record_attachment

    post = _patch_post(
        monkeypatch, _PostRecorder(FakeResponse(200, {"attachment_id": "att-1"}))
    )
    await record_attachment(_config(), "SHA256:abc", "s-7f3a91c2")

    assert post.calls[0]["timeout"] == 10.0
    assert KEY_USE_BUMP_TIMEOUT_SECONDS < 10.0


@pytest.mark.asyncio
async def test_a_tightened_global_budget_is_never_widened_by_the_override(monkeypatch):
    """An operator who lowers orchestrator_request_timeout is stating a
    ceiling. A per-call override takes the SHORTER of the two, so the bump's
    own constant can only ever tighten, never relax, that ceiling."""
    from orchestrator.services.ssh_gateway_client import mark_key_used

    post = _patch_post(monkeypatch, _PostRecorder(FakeResponse(200, {"status": "ok"})))
    await mark_key_used(_config(orchestrator_request_timeout=0.5), "SHA256:abc")

    assert post.calls[0]["timeout"] == 0.5


@pytest.mark.asyncio
async def test_an_audit_write_never_raises_a_refusal(monkeypatch):
    """AuditWriteFailed must not be a TargetDenied/TargetUnavailable: those
    two are authorization outcomes the gateway turns into a readable
    refusal, and a control-plane hiccup on a bookkeeping write must never be
    able to manufacture one."""
    from orchestrator.services.ssh_gateway_client import AuditWriteFailed, mark_key_used

    _patch_post(monkeypatch, _PostRecorder(raises=RuntimeError("connection reset")))

    with pytest.raises(AuditWriteFailed) as excinfo:
        await mark_key_used(_config(), "SHA256:abc")

    assert not isinstance(excinfo.value, (TargetDenied, TargetUnavailable))
    assert isinstance(excinfo.value.__cause__, RuntimeError)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 404, 422, 500, 503])
async def test_a_non_200_audit_response_is_a_failure(monkeypatch, status):
    from orchestrator.services.ssh_gateway_client import AuditWriteFailed, mark_key_used

    _patch_post(monkeypatch, _PostRecorder(FakeResponse(status, {"status": "ok"})))

    with pytest.raises(AuditWriteFailed):
        await mark_key_used(_config(), "SHA256:abc")


@pytest.mark.asyncio
async def test_record_attachment_asserts_no_identity(monkeypatch):
    """thread_id/user_id/ssh_key_id are resolved server-side. An earlier
    draft of the endpoint took them as asserted fields, which let any
    internal-key holder attribute an SSH attach to any user."""
    from orchestrator.services.ssh_gateway_client import record_attachment

    post = _patch_post(
        monkeypatch, _PostRecorder(FakeResponse(200, {"attachment_id": "att-1"}))
    )
    attachment_id = await record_attachment(
        _config(), "SHA256:abc", "s-7f3a91c2", "203.0.113.9"
    )

    assert attachment_id == "att-1"
    assert (
        post.calls[0]["url"] == "http://orchestrator:8085/api/internal/ssh-attachments"
    )
    assert post.calls[0]["json"] == {
        "fingerprint": "SHA256:abc",
        "handle": "s-7f3a91c2",
        "client_ip": "203.0.113.9",
    }


@pytest.mark.asyncio
async def test_record_attachment_refuses_an_invalid_handle_before_the_request(
    monkeypatch,
):
    """The handle is the SSH username and is fully attacker-controlled. A
    handle this module would refuse to resolve has no business riding along
    with config.internal_key to open an audit row either."""
    from orchestrator.services.ssh_gateway_client import (
        AuditWriteFailed,
        record_attachment,
    )

    post = _patch_post(
        monkeypatch, _PostRecorder(FakeResponse(200, {"attachment_id": "att-1"}))
    )

    with pytest.raises(AuditWriteFailed):
        await record_attachment(_config(), "SHA256:abc", "../../admin")

    assert post.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [{}, {"attachment_id": None}, {"attachment_id": ""}, {"attachment_id": 7}, []],
)
async def test_record_attachment_rejects_an_unusable_id(monkeypatch, payload):
    """Without this the gateway would hold a None/int "id" and later POST it
    into a URL path, turning one bad response into a second bad request."""
    from orchestrator.services.ssh_gateway_client import (
        AuditWriteFailed,
        record_attachment,
    )

    _patch_post(monkeypatch, _PostRecorder(FakeResponse(200, payload)))

    with pytest.raises(AuditWriteFailed):
        await record_attachment(_config(), "SHA256:abc", "s-7f3a91c2")


@pytest.mark.asyncio
async def test_close_attachment_sends_the_fingerprint_and_channels(monkeypatch):
    """The endpoint authorizes the close against the named attachment's own
    thread, so the fingerprint is not optional decoration -- without it any
    internal-key holder could close (and fabricate channels on) an audit row
    belonging to someone else."""
    from orchestrator.services.ssh_gateway_client import close_attachment

    post = _patch_post(monkeypatch, _PostRecorder(FakeResponse(200, {"closed": 1})))
    closed = await close_attachment(
        _config(), "att-1", "SHA256:abc", ("session", "sftp")
    )

    assert closed == 1
    assert post.calls[0]["url"] == (
        "http://orchestrator:8085/api/internal/ssh-attachments/att-1/close"
    )
    assert post.calls[0]["json"] == {
        "fingerprint": "SHA256:abc",
        "channels": ["session", "sftp"],
    }


@pytest.mark.asyncio
async def test_close_attachment_percent_encodes_the_id(monkeypatch):
    """The one value here that lands in a URL path. resolve_target encodes
    its handle at the point the path is built rather than trusting where it
    came from; this follows that precedent."""
    from orchestrator.services.ssh_gateway_client import close_attachment

    post = _patch_post(monkeypatch, _PostRecorder(FakeResponse(200, {"closed": 0})))
    await close_attachment(_config(), "../../admin", "SHA256:abc")

    assert post.calls[0]["url"] == (
        "http://orchestrator:8085/api/internal/ssh-attachments/..%2F..%2Fadmin/close"
    )


@pytest.mark.asyncio
async def test_close_attachment_treats_zero_as_a_normal_outcome(monkeypatch):
    """{"closed": 0} is the endpoint's opaque answer for unknown, already
    closed, and not-yours alike. The gateway's close is best effort and must
    not log a failure for the ordinary already-closed case."""
    from orchestrator.services.ssh_gateway_client import close_attachment

    _patch_post(monkeypatch, _PostRecorder(FakeResponse(200, {"closed": 0})))
    assert await close_attachment(_config(), "att-1", "SHA256:abc") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload", [{}, {"closed": None}, {"closed": True}, {"closed": "1"}]
)
async def test_close_attachment_rejects_an_unusable_count(monkeypatch, payload):
    """`isinstance(True, int)` is True, so a JSON boolean would otherwise
    read back as "1 row closed" -- the same bool-is-an-int trap _is_valid_port
    guards against one field over."""
    from orchestrator.services.ssh_gateway_client import (
        AuditWriteFailed,
        close_attachment,
    )

    _patch_post(monkeypatch, _PostRecorder(FakeResponse(200, payload)))

    with pytest.raises(AuditWriteFailed):
        await close_attachment(_config(), "att-1", "SHA256:abc")


@pytest.mark.asyncio
async def test_a_non_json_audit_body_is_a_failure(monkeypatch):
    from orchestrator.services.ssh_gateway_client import AuditWriteFailed, mark_key_used

    _patch_post(monkeypatch, _PostRecorder(NonJsonResponse()))

    with pytest.raises(AuditWriteFailed):
        await mark_key_used(_config(), "SHA256:abc")


@pytest.mark.asyncio
async def test_client_ip_is_omitted_rather_than_sent_as_null(monkeypatch):
    """The gateway does not always know a peer address (Task 8's websocket
    bridge behind a proxy that strips it). Omitting the optional field is
    the documented shape; sending an explicit null is the same to Pydantic
    but reads in a request log as "we knew it was nothing"."""
    from orchestrator.services.ssh_gateway_client import record_attachment

    post = _patch_post(
        monkeypatch, _PostRecorder(FakeResponse(200, {"attachment_id": "att-1"}))
    )
    await record_attachment(_config(), "SHA256:abc", "s-7f3a91c2")

    assert "client_ip" not in post.calls[0]["json"]
