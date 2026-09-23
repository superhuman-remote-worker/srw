"""Tests for the internal SSH-gateway target resolution endpoint.

``GET /api/internal/ssh-targets/{handle}`` is how the SSH gateway (which
holds no database credentials) turns a presented handle + key fingerprint
into a routable workspace target. It is the seam plan 1's design rests on:

* Anti-enumeration: unknown handle, unknown/unapproved key, and "your key is
  fine but you don't own this thread" must be indistinguishable (identical
  404, same detail) — see ``PostgresDB.get_thread_id_by_ssh_handle`` and
  ``PostgresDB.resolve_user_by_ssh_fingerprint``'s docstrings, which both
  name this endpoint as the reason.
* No wake-on-connect (D5): resolving a handle must never restore a suspended
  workspace as a side effect, so this deliberately does NOT reuse
  ``resolve_thread_for_forwarding`` (``services/pinned_forwarding``, formerly
  main's ``_resolve_thread_for_forwarding``; it does exactly that) or
  ``thread_runtime_is_preparable`` (which folds 'suspended' into "OK to
  prepare" and says by its own docstring that it is not a lifecycle
  predicate).
* Liveness is three independent axes — session status, lane +
  its retirement marker, and workspace_container/vm status — collapsed by
  ``resolve_workspace_state``.  The idle sweeper's own query keys on exactly
  "session ended, workspace still ready", so that combination is the common
  case this must get right, not an edge case.

These cases first passed against the original handler in ``main``. The
identity resolution now lives in ``orchestrator.routers.ssh_access`` (where
its opaque 404 is readable next to the lookups it protects) and the target
projection in ``orchestrator.services.ssh_access``; the source-scan cases at
the bottom therefore read BOTH.
"""

import inspect
import json
from uuid import UUID

import pytest
from fastapi import HTTPException

import orchestrator.main
from orchestrator.routers import ssh_access as ssh_access_routes
from orchestrator.services import ssh_access as ssh_access_operations
from orchestrator.services.canvas_ssh import RemoteWorkspaceTarget
from orchestrator.services.ssh_gateway_targets import resolve_workspace_state
from orchestrator.services.workspace_suspension import _thread_is_vm_tier
from tests._ssh_access_harness import SshAccessHarness
from tests._route_inventory import mounted_routes

USER = "00000000-0000-0000-0000-000000000001"
THREAD = "00000000-0000-0000-0000-000000000002"
FINGERPRINT = "SHA256:" + "A" * 43


def _thread(**over):
    base = {
        "id": THREAD,
        "user_id": USER,
        "status": "active",
        "execution_lane": "pinned",
        "runtime_retirement_token": None,
    }
    base.update(over)
    return base


def test_live_when_workspace_ready():
    assert (
        resolve_workspace_state(_thread(), {"workspace_container": {"status": "ready"}})
        == "live"
    )


def test_never_provisioned_is_absence_of_status_not_absence_of_key():
    """workspace_container exists on every thread carrying only git fields."""
    assert (
        resolve_workspace_state(
            _thread(),
            {"workspace_container": {"git_remote_url": "x", "repo_name": "y"}},
        )
        == "never_provisioned"
    )
    assert resolve_workspace_state(_thread(), {}) == "never_provisioned"


def test_failed_is_not_reported_as_never_provisioned():
    """`failed` is a real status container_provisioner writes (PVC creation
    failure, provisioning error) and is in the spec's §7.1 set. The gateway
    prints this state as the user-facing reason, so collapsing it into
    "never provisioned" sends the operator after the wrong problem — the same
    defect ``stale_binding`` was added to fix."""
    assert (
        resolve_workspace_state(
            _thread(), {"workspace_container": {"status": "failed"}}
        )
        == "failed"
    )


def test_deleted_is_not_reported_as_never_provisioned():
    """`deleted` means the workspace existed and was torn down. "Never
    provisioned" is a materially different message."""
    assert (
        resolve_workspace_state(
            _thread(), {"workspace_container": {"status": "deleted"}}
        )
        == "deleted"
    )


def test_an_unknown_status_still_falls_through():
    """The fallthrough survives, now covering only statuses this module has
    never heard of — a guess, where the branches above are facts."""
    assert (
        resolve_workspace_state(
            _thread(), {"workspace_container": {"status": "quantum"}}
        )
        == "never_provisioned"
    )


def test_reclaimed_is_distinct_from_suspended():
    assert (
        resolve_workspace_state(
            _thread(), {"workspace_container": {"status": "suspended"}}
        )
        == "suspended"
    )
    assert (
        resolve_workspace_state(
            _thread(),
            {"workspace_container": {"status": "suspended", "volume_reclaimed": True}},
        )
        == "reclaimed"
    )


def test_ended_session_reported_even_when_workspace_still_ready():
    """The idle sweeper keys on exactly this combination, so it is the common
    case, not an edge case."""
    assert (
        resolve_workspace_state(
            _thread(status="ended"), {"workspace_container": {"status": "ready"}}
        )
        == "ended"
    )


def test_pinned_retirement_token_means_ending():
    assert (
        resolve_workspace_state(
            _thread(runtime_retirement_token="tok"),
            {"workspace_container": {"status": "ready"}},
        )
        == "ending"
    )


def test_stateless_retirement_flags_mean_ending():
    assert (
        resolve_workspace_state(
            _thread(execution_lane="stateless"),
            {
                "workspace_container": {"status": "ready"},
                "_stateless_workspace_retirement_pending": True,
            },
        )
        == "ending"
    )


def test_session_suspended_is_not_workspace_suspended():
    """threads.status='suspended' means awaiting-user, not a stopped workspace."""
    assert (
        resolve_workspace_state(
            _thread(status="suspended"), {"workspace_container": {"status": "ready"}}
        )
        == "live"
    )


@pytest.fixture
def harness():
    """The application's own composition, with the real VM-tier predicate."""
    built = SshAccessHarness()
    built.thread_is_vm_tier = _thread_is_vm_tier
    return built


async def _get_target(harness, *, handle="s-7f3a91c2", fingerprint=FINGERPRINT):
    return await ssh_access_routes.get_ssh_target(
        request=object(),
        handle=handle,
        fingerprint=fingerprint,
        dependencies=harness.dependencies,
    )


@pytest.mark.asyncio
async def test_requires_internal_key(harness):
    async def _deny(request):
        raise HTTPException(status_code=401, detail="Invalid internal key")

    harness.require_internal = _deny
    with pytest.raises(HTTPException) as excinfo:
        await _get_target(harness)
    assert excinfo.value.status_code == 401


@pytest.mark.asyncio
async def test_unknown_handle_and_unauthorized_are_identical(harness):
    """Anti-enumeration: an attacker must not learn which handles exist.

    ``resolve_user_by_ssh_fingerprint`` is wired up front and reused for
    both calls (fix round 1 / Important 1): the handler calls it
    unconditionally, even on an unknown handle, so leaving it unwired for
    the first call would trip the harness's unexpected-call tripwire instead
    of exercising the anti-enumeration property this test is named for.
    """

    async def _no_thread(handle):
        return None

    async def _user(fp):
        return {"id": "00000000-0000-0000-0000-000000000009"}

    harness.store.set("get_thread_id_by_ssh_handle", _no_thread)
    harness.store.set("resolve_user_by_ssh_fingerprint", _user)
    with pytest.raises(HTTPException) as unknown:
        await _get_target(harness, handle="s-aaaaaaaa")

    async def _thread_ok(handle):
        return THREAD

    async def _no_access(user, db, entity_id):
        return False

    harness.store.set("get_thread_id_by_ssh_handle", _thread_ok)
    harness.user_can_access_ide_entity = _no_access
    with pytest.raises(HTTPException) as denied:
        await _get_target(harness)

    assert unknown.value.status_code == denied.value.status_code == 404
    assert unknown.value.detail == denied.value.detail


@pytest.mark.asyncio
async def test_unknown_key_is_the_same_404_as_the_other_two(harness):
    """The third state this module's docstring says must be indistinguishable.

    ``test_unknown_handle_and_unauthorized_are_identical`` covers unknown
    handle and not-yours; an unknown or unapproved KEY was named as a case
    that must match byte for byte and was never asserted. It is the one an
    attacker probes first — the fingerprint is the field they control most
    freely, they can enumerate published keys, and it is the branch that
    would most plausibly grow its own message ("key not registered") in a
    future edit while the handle branches stayed opaque.

    ``resolve_user_by_ssh_fingerprint`` returns None for all of: no such
    fingerprint, a key whose ``disabled_at`` is set, and a key on an
    unapproved/deactivated account.
    """

    async def _thread_ok(handle):
        return THREAD

    async def _no_user(fp):
        return None

    async def _access(user, db, entity_id):
        raise AssertionError("authorization must not run without an identity")

    harness.store.set("get_thread_id_by_ssh_handle", _thread_ok)
    harness.store.set("resolve_user_by_ssh_fingerprint", _no_user)
    harness.user_can_access_ide_entity = _access

    with pytest.raises(HTTPException) as unknown_key:
        await _get_target(harness)

    # Compare against a genuinely different failure resolved the same request,
    # rather than against a hardcoded string that could drift with the handler.
    async def _no_thread(handle):
        return None

    harness.store.set("get_thread_id_by_ssh_handle", _no_thread)
    with pytest.raises(HTTPException) as unknown_handle:
        await _get_target(harness, handle="s-aaaaaaaa")

    assert unknown_key.value.status_code == unknown_handle.value.status_code == 404
    assert unknown_key.value.detail == unknown_handle.value.detail


@pytest.mark.asyncio
async def test_unknown_handle_still_reaches_the_fingerprint_resolver(harness):
    """The resolver must run even for a handle already known to be unknown.

    HISTORY: this pin was written when resolve_user_by_ssh_fingerprint was a
    ``WITH bumped AS (UPDATE ... RETURNING)`` CTE. Short-circuiting before it
    then leaked handle existence, because a probing user could read their own
    last_used_at back through GET /api/ssh-keys and watch whether it moved —
    telling "exists, not mine" from "does not exist" through a side channel,
    while the HTTP responses stayed byte-for-byte identical.

    That CTE is GONE: the final review found that a write on the resolution
    path is reachable by any X-Internal-Key holder and, in the gateway plan,
    fires before key.verify. Resolution is now a pure read and the bump lives
    in mark_ssh_key_used. Do not restore the old rationale from git history.

    The assertion still earns its place for two reasons that outlive the CTE:
    both failure paths must issue the same round trips, and calling the
    resolver unconditionally is what makes re-introducing a write on this
    path a visible change rather than a silent regression.
    """

    called_with = []

    async def _no_thread(handle):
        return None

    async def _user(fp):
        called_with.append(fp)
        return {"id": USER}

    harness.store.set("get_thread_id_by_ssh_handle", _no_thread)
    harness.store.set("resolve_user_by_ssh_fingerprint", _user)

    with pytest.raises(HTTPException):
        await _get_target(harness, handle="s-aaaaaaaa")

    assert called_with == [FINGERPRINT]


@pytest.mark.asyncio
async def test_rejects_a_malformed_handle_before_touching_the_database(harness):
    called = False

    async def _tripwire(handle):
        nonlocal called
        called = True
        return None

    harness.store.set("get_thread_id_by_ssh_handle", _tripwire)
    with pytest.raises(HTTPException):
        await _get_target(harness, handle="s-abc\nProxyCommand x")
    assert called is False


@pytest.mark.asyncio
async def test_missing_fingerprint_is_opaque_404_not_a_422(harness):
    """Fix round 1 / Minor 6: ``fingerprint`` became an Optional query param
    so a request omitting it still reaches ``require_internal`` first — a
    required param would make FastAPI 422 before any auth check runs,
    disclosing the route and its parameter name to an unauthenticated
    caller. A missing fingerprint must fail the same opaque way as any other
    malformed input, before touching the database.
    """
    called = False

    async def _tripwire(handle):
        nonlocal called
        called = True
        return None

    harness.store.set("get_thread_id_by_ssh_handle", _tripwire)
    with pytest.raises(HTTPException) as excinfo:
        await _get_target(harness, fingerprint=None)
    assert excinfo.value.status_code == 404
    assert called is False


def _resolve_to(harness, *, thread, user=None):
    """Wire the three lookups the handler performs, then hand back the thread."""

    async def _thread_id(handle):
        return THREAD

    async def _user(fp):
        return user if user is not None else {"id": USER}

    async def _access(u, db, entity_id):
        return True

    async def _get_thread(tid):
        return thread

    harness.store.set("get_thread_id_by_ssh_handle", _thread_id)
    harness.store.set("resolve_user_by_ssh_fingerprint", _user)
    harness.store.set("get_thread", _get_thread)
    harness.user_can_access_ide_entity = _access


@pytest.mark.asyncio
async def test_non_live_state_returns_200_with_no_pod_ip(harness, monkeypatch):
    """The gateway needs a readable reason, so this is not an error response."""
    _resolve_to(harness, thread=_thread(status="active"))
    monkeypatch.setattr(
        ssh_access_operations,
        "thread_metadata_object",
        lambda t: {"workspace_container": {"status": "suspended"}},
    )

    result = await _get_target(harness)
    assert result["state"] == "suspended"
    assert result["pod_ip"] is None


@pytest.mark.asyncio
async def test_live_state_maps_target_fields(harness, monkeypatch):
    """Important 2 (fix round 1): the one branch that hands out a routable
    target had zero coverage. Stubs the canvas_ssh resolver rather than
    constructing a real ``_workspace_binding`` — that resolver's own
    behavior is covered by test_canvas_ssh_transport.py; this pins the
    endpoint's field mapping (``pod_ip``/``pod_port``/``host_key_fingerprint``
    <- target.host/.port/.fingerprint), which nothing else exercises.
    """
    _resolve_to(
        harness,
        thread=_thread(
            status="active", metadata={"workspace_container": {"status": "ready"}}
        ),
    )

    generation = UUID("11111111-1111-1111-1111-111111111111")

    def _generation(thread):
        return generation

    def _target(thread, expected_generation):
        return RemoteWorkspaceTarget(
            thread_id=THREAD,
            generation=expected_generation,
            host="10.0.0.5",
            port=2222,
            fingerprint="SHA256:" + "B" * 43,
        )

    monkeypatch.setattr(
        ssh_access_operations, "bound_workspace_generation", _generation
    )
    monkeypatch.setattr(
        ssh_access_operations, "resolve_remote_workspace_target", _target
    )

    result = await _get_target(harness)
    assert result["state"] == "live"
    assert result["pod_ip"] == "10.0.0.5"
    assert result["pod_port"] == 2222
    assert result["host_key_fingerprint"] == "SHA256:" + "B" * 43


@pytest.mark.asyncio
async def test_vm_tier_state_from_json_string_metadata(harness):
    """Important 2 (fix round 1): exercises the real JSONB-as-str seam end
    to end. asyncpg returns ``threads.metadata`` as a JSON string, not a
    dict; this passes one, does NOT stub ``thread_metadata_object``
    (unlike the other tests here), and lets the real parser and the real
    ``_thread_is_vm_tier`` run against the result — both were previously
    only exercised through a mocked ``thread_metadata_object``.
    """
    _resolve_to(
        harness,
        thread=_thread(
            status="active", metadata=json.dumps({"vm": {"status": "ready"}})
        ),
    )

    result = await _get_target(harness)
    assert result["state"] == "vm_unsupported"
    assert result["pod_ip"] is None


def _both_ready_stale_backend_metadata():
    """The shape where the tier guard and the endpoint resolver disagree.

    ``vm.status`` and ``workspace_container.status`` are BOTH "ready" and the
    declared backend is still a non-VM one. That is not contrived:
    upgrade-to-VM provisions ``metadata.vm`` without rewriting
    ``config_override.workspace.backend``, which
    ``_thread_is_vm_tier``'s own docstring records, so an upgraded session
    declares ``sandbox`` forever. ``_thread_is_vm_tier`` reads the declared
    backend once a container status is present and answers "pod tier", while
    ``resolve_remote_workspace_target`` prefers the VM context whenever its
    status is ready and answers with the VM's endpoint.
    """
    generation = "11111111-1111-1111-1111-111111111111"
    endpoint = {
        "_canvas_workspace_generation": generation,
        "status": "ready",
    }
    return {
        "config_override": {"workspace": {"backend": "sandbox"}},
        "_workspace_binding": {
            "kind": "remote",
            "generation": generation,
            "backing_id": "k8s-pvc:ns:abc",
            "ssh_host_key_fingerprint": "SHA256:" + "C" * 43,
        },
        "workspace_container": dict(endpoint, ssh_host="pod.svc", ssh_port=22),
        "vm": dict(endpoint, ssh_host="10.9.9.9", ssh_port=2200),
    }


def test_the_resolver_really_would_hand_back_the_vm_endpoint():
    """Negative control for the test below. Without this, that test could pass
    because the metadata never resolved at all rather than because the
    endpoint refused a VM target it genuinely could have returned."""
    from orchestrator.services.canvas_ssh import (
        bound_workspace_generation,
        resolve_remote_workspace_target,
    )

    metadata = _both_ready_stale_backend_metadata()
    thread = _thread(status="active", metadata=metadata)

    # The v1 guard says "not VM tier" ...
    assert not _thread_is_vm_tier(
        metadata, metadata["workspace_container"], metadata["vm"]
    )
    # ... while the resolver hands back the VM's host and port.
    target = resolve_remote_workspace_target(thread, bound_workspace_generation(thread))
    assert (target.host, target.port) == ("10.9.9.9", 2200)


@pytest.mark.asyncio
async def test_vm_endpoint_is_refused_even_when_the_guard_passes(harness):
    """Final review, Important 4: v1 does not support VM workspaces, so a
    thread that slips past ``_thread_is_vm_tier`` must still not be handed the
    VM's host and port. Nothing is stubbed below ``get_thread`` — the real
    parser, the real guard and the real resolver all run."""
    _resolve_to(
        harness,
        thread=_thread(
            status="active", metadata=json.dumps(_both_ready_stale_backend_metadata())
        ),
    )

    result = await _get_target(harness)
    assert result["state"] == "vm_unsupported"
    assert result["pod_ip"] is None
    assert result["pod_port"] is None


@pytest.mark.asyncio
async def test_stale_binding_state_on_canvas_ssh_error(harness):
    """Important 2 + Minor 3 (fix round 1): a workspace_container that IS
    ready but carries no provisioner-attested ``_workspace_binding`` must
    report ``stale_binding``, not ``never_provisioned`` — the workspace
    really is provisioned, just not SSH-attested. Runs the real
    ``bound_workspace_generation``/``resolve_remote_workspace_target``
    (neither stubbed): with no ``_workspace_binding`` in metadata,
    ``bound_workspace_generation`` itself raises ``CanvasSSHError`` while
    being evaluated as that call's argument, which the service's ``try``
    still catches.
    """
    _resolve_to(
        harness,
        thread=_thread(
            status="active", metadata={"workspace_container": {"status": "ready"}}
        ),
    )

    result = await _get_target(harness)
    assert result["state"] == "stale_binding"
    assert result["pod_ip"] is None


@pytest.mark.asyncio
async def test_a_thread_that_vanished_between_lookups_is_the_same_opaque_404(harness):
    """The resolution above proved the handle maps to a thread id; a row
    deleted between that lookup and ``get_thread`` must not become a
    distinguishable failure — it reuses the identical opaque 404.
    """
    _resolve_to(harness, thread=None)
    with pytest.raises(HTTPException) as vanished:
        await _get_target(harness)

    async def _no_thread(handle):
        return None

    harness.store.set("get_thread_id_by_ssh_handle", _no_thread)
    with pytest.raises(HTTPException) as unknown:
        await _get_target(harness, handle="s-aaaaaaaa")

    assert vanished.value.status_code == unknown.value.status_code == 404
    assert vanished.value.detail == unknown.value.detail


def _target_resolution_source() -> str:
    """Both halves of what used to be one handler, scanned as one body."""
    return inspect.getsource(ssh_access_routes.get_ssh_target) + inspect.getsource(
        ssh_access_operations.resolve_target
    )


def test_endpoint_does_not_use_the_restoring_resolver():
    """resolve_thread_for_forwarding restores suspended workspaces as a side
    effect. Using it here would silently implement wake-on-connect, which the
    design rules out. R1.B10 moved it out of ``main`` and dropped the leading
    underscore, so the forbidden token is the bare name — it also matches the
    old spelling and any ``pinned_forwarding.`` qualified use.

    Scope note (fix round 1 / Minor 7): this inspects only the endpoint's own
    two source bodies, not transitively through the helpers they call
    (``resolve_workspace_state``, ``_thread_is_vm_tier``,
    ``resolve_remote_workspace_target``, ...). A prohibited helper
    introduced one level down would not trip this guard — it was checked by
    hand against current source when this endpoint was written and is clean
    today, but a future change to those helpers is outside what this test
    can see.
    """
    source = _target_resolution_source()
    for forbidden in (
        "resolve_thread_for_forwarding",
        "pinned_forwarding",
        "thread_runtime_is_preparable",
        "resolve_pod_ip",
    ):
        assert forbidden not in source, f"{forbidden} must not be used here"


def test_endpoint_does_not_mark_a_key_used():
    """Resolution is not proof of possession.

    ``resolve_user_by_ssh_fingerprint`` was split into a pure read plus
    ``mark_ssh_key_used`` (final review, Important 1) precisely so that
    offering a fingerprint stops being a write. Calling the bump from here
    would put it straight back: every agent pod holds ``X-Internal-Key``, and
    fingerprints come off ``github.com/<user>.keys``, so the write would be
    attacker-controlled and ``last_used_at`` — the field a user checks to
    notice a stolen key — would stop meaning anything. The gateway calls the
    bump itself, after ``key.verify``.

    Matches the CALL form, not the bare name: the endpoint's docstring names
    the method in order to prohibit it.
    """
    assert "mark_ssh_key_used(" not in _target_resolution_source()


def test_ssh_target_route_is_mounted():
    """Every test above calls the handler directly, so none of them prove
    FastAPI actually serves this path at this method — a typo'd decorator
    path or a route registered under the wrong verb would pass every other
    test here and still 404 in production. Same rationale as
    ``test_ssh_key_routes_are_mounted`` in test_ssh_key_endpoints.py."""
    routes = mounted_routes(orchestrator.main.app)
    assert ("GET", "/api/internal/ssh-targets/{handle}") in routes
