"""G3 — multi-tenancy gate on the persistent-thread family.

Pre-G3, all 9 thread endpoints carried this inline check:

    if thread.get("user_id") and str(thread["user_id"]) != str(user["id"]):
        raise HTTPException(status_code=403, detail="Not your thread")

It allowed any caller through when ``user_id IS NULL`` (orphan threads
left behind by user deletion) — letting attackers enumerate them by
UUID. It also didn't bypass for admins or enforce MCP project-scope
refusal. G3 replaces every occurrence with a single
``require_thread_owner`` call (new helper in ``security/access.py``).
The forwarding resolver that takes ``user`` but not ``request`` (now
``services/pinned_forwarding.resolve_thread_for_forwarding``) got an inline
fail-closed + admin-bypass fix.

This file covers the helper directly (the cross-cutting properties) and
samples a few endpoints to prove the gate is wired. The endpoints now live in
their R1.B10 routers and receive the real ``require_thread_owner`` through
their application's dependencies, so only the caller resolver the gate itself
consults (``security.access.require_approved_user``) is patched.
"""

import dataclasses
import inspect
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.routers import thread_history, thread_session, thread_transport
from orchestrator.security.access import require_thread_owner
from orchestrator.services import pinned_forwarding
from orchestrator.services.thread_turn_locks import ThreadTurnLocks
from shared.pinned_session_identity import PinnedSessionBinding


FORWARD_THREAD_ID = "11111111-1111-4111-8111-111111111111"
FORWARD_RUNTIME_ID = "22222222-2222-4222-8222-222222222222"
FORWARD_GENERATION = "33333333-3333-4333-8333-333333333333"


def _forwarding_stateless_thread(*, ready: bool) -> dict:
    workspace = {
        "status": "ready" if ready else "created",
        "provisioner": "k8s",
        "pod_name": f"ws-thread-{FORWARD_THREAD_ID[:12]}",
        "namespace": "agent-workspaces",
        "pod_ip": "10.42.0.9",
        "port": 30022,
        "_runtime_incarnation": FORWARD_RUNTIME_ID,
    }
    metadata = {
        "config_override": {"workspace": {"backend": "sandbox"}},
        "workspace_container": workspace,
    }
    if ready:
        workspace["_canvas_workspace_generation"] = FORWARD_GENERATION
        metadata["_workspace_binding"] = {
            "generation": FORWARD_GENERATION,
            "kind": "remote",
            "backing_id": f"k8s-pod:agent-workspaces:{FORWARD_RUNTIME_ID}",
            "ssh_host_key_fingerprint": "SHA256:" + ("A" * 43),
        }
    else:
        workspace["_runtime_creation"] = {
            "generation": "44444444-4444-4444-8444-444444444444",
            "mode": "create",
            "attempted": True,
            "replaces_uid": None,
        }
    return {
        "id": FORWARD_THREAD_ID,
        "user_id": "user-1",
        "agent_id": "agent-1",
        "execution_lane": "stateless",
        "status": "active",
        "runtime_generation": "55555555-5555-4555-8555-555555555555",
        "runtime_retirement_token": None,
        "metadata": metadata,
    }


# =============================================================================
# Patch helpers
# =============================================================================


def _patch_caller(user: dict):
    """Resolve ``user`` as the caller of the real owner gate.

    The store is handed to the gate (or the route's dependencies) explicitly
    by each case; nothing reads an application-module store any more.
    """
    stack = ExitStack()
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    return stack


def _forwarding_deps(
    db, suspension=None
) -> pinned_forwarding.PinnedForwardingDependencies:
    return pinned_forwarding.PinnedForwardingDependencies(
        store=db,
        workspace_suspension=(
            suspension
            if suspension is not None
            else SimpleNamespace(
                is_enabled=True,
                restore_thread_workspace=AsyncMock(
                    side_effect=AssertionError("unexpected workspace restore")
                ),
            )
        ),
        protected_cloud_delivery_state=AsyncMock(
            side_effect=AssertionError("unexpected protected-cloud read")
        ),
    )


def _assert_forwarding_has_no_reconciliation_path() -> None:
    """The resolver can refuse "before reconciliation" only if it has no way to
    reconcile: its collaborators are the store, the suspension service and
    the protected-cloud probe, and it never names the workspace ensure."""
    assert {
        field.name
        for field in dataclasses.fields(pinned_forwarding.PinnedForwardingDependencies)
    } == {"store", "workspace_suspension", "protected_cloud_delivery_state"}
    assert "ensure_session_workspace" not in inspect.getsource(pinned_forwarding)


def _session_deps(db, resolve_cloud_session_url):
    return thread_session.ThreadSessionDependencies(
        store=db,
        require_thread_owner=require_thread_owner,
        require_approved_user=AsyncMock(
            side_effect=AssertionError("thread reads gate on the owner check")
        ),
        resolve_cloud_session_url=resolve_cloud_session_url,
        resolve_session_config=AsyncMock(side_effect=AssertionError("not reached")),
        enforce_session_create_grants=AsyncMock(
            side_effect=AssertionError("not reached")
        ),
        tool_view=SimpleNamespace(),
    )


def _history_deps(db):
    return thread_history.ThreadHistoryDependencies(
        store=db,
        vector_db=SimpleNamespace(),
        require_thread_owner=require_thread_owner,
    )


def _transport_deps(db):
    return thread_transport.ThreadTransportDependencies(
        store=db,
        require_thread_owner=require_thread_owner,
        require_approved_user=AsyncMock(
            side_effect=AssertionError("the stream gates on the owner check")
        ),
        forwarding=_forwarding_deps(db),
        stateless_input=SimpleNamespace(),
        turn_locks=ThreadTurnLocks(),
    )


def _scoped(user: dict, scope: str) -> dict:
    out = dict(user)
    out["scopes"] = [scope]
    out["auth_method"] = "mcp"
    return out


# =============================================================================
# require_thread_owner — the helper itself
# =============================================================================


class TestRequireThreadOwner:
    @pytest.mark.asyncio
    async def test_owner_passes(self, user_a, thread_a, fake_db, fake_request):
        from orchestrator.security.access import require_thread_owner

        with _patch_caller(user_a):
            user, thread = await require_thread_owner(
                fake_request, fake_db, str(thread_a["id"])
            )
        assert thread is thread_a
        assert user["id"] == user_a["id"]

    @pytest.mark.asyncio
    async def test_cross_user_403(self, user_b, thread_a, fake_db, fake_request):
        from orchestrator.security.access import require_thread_owner

        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await require_thread_owner(fake_request, fake_db, str(thread_a["id"]))
        assert exc.value.status_code == 403


class TestForwardingWorkspaceAuthority:
    @pytest.mark.asyncio
    async def test_pinned_forwarding_uses_one_joined_binding_snapshot(self):
        thread = _forwarding_stateless_thread(ready=True)
        thread["execution_lane"] = "pinned"
        binding = PinnedSessionBinding(
            thread_id=FORWARD_THREAD_ID,
            runtime_generation=thread["runtime_generation"],
            agent_id="66666666-6666-4666-8666-666666666666",
            runtime_attach_token="77777777-7777-4777-8777-777777777777",
            agent_hostname="persistent-111111111111",
            pod_namespace="srw",
            pod_uid="pod-uid-1",
            pod_ip="10.42.0.8",
            pod_port=8001,
            agent_status="session",
        )
        joined = AsyncMock(return_value=binding)
        db = SimpleNamespace(
            get_thread=AsyncMock(return_value=thread),
            get_pinned_session_binding=joined,
        )
        suspension = SimpleNamespace(
            is_enabled=True,
            restore_thread_workspace=AsyncMock(),
        )

        resolved, selected = await pinned_forwarding.resolve_thread_for_forwarding(
            FORWARD_THREAD_ID,
            {"id": "user-1", "is_admin": False},
            dependencies=_forwarding_deps(db, suspension),
        )

        assert resolved is thread
        assert selected is binding
        joined.assert_awaited_once_with(
            FORWARD_THREAD_ID,
            expected_runtime_generation=thread["runtime_generation"],
        )
        suspension.restore_thread_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_direct_stateless_forwarding_is_refused_before_reconciliation(self):
        in_progress = _forwarding_stateless_thread(ready=False)
        ready = _forwarding_stateless_thread(ready=True)
        db = SimpleNamespace(
            get_thread=AsyncMock(side_effect=[in_progress, ready]),
            get_agent=AsyncMock(return_value={"id": "agent-1", "pod_ip": "10.0.0.2"}),
        )
        suspension = SimpleNamespace(
            is_enabled=True,
            restore_thread_workspace=AsyncMock(),
        )

        with pytest.raises(HTTPException) as exc:
            await pinned_forwarding.resolve_thread_for_forwarding(
                FORWARD_THREAD_ID,
                {"id": "user-1", "is_admin": False},
                dependencies=_forwarding_deps(db, suspension),
            )

        assert exc.value.status_code == 409
        _assert_forwarding_has_no_reconciliation_path()
        suspension.restore_thread_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stateless_forwarding_refuses_fresh_lane_flip(self):
        initial = _forwarding_stateless_thread(ready=False)
        flipped = _forwarding_stateless_thread(ready=True)
        flipped["execution_lane"] = "pinned"
        db = SimpleNamespace(
            get_thread=AsyncMock(side_effect=[initial, flipped]),
            get_agent=AsyncMock(),
        )
        suspension = SimpleNamespace(
            is_enabled=True,
            restore_thread_workspace=AsyncMock(),
        )

        with pytest.raises(HTTPException) as exc:
            await pinned_forwarding.resolve_thread_for_forwarding(
                FORWARD_THREAD_ID,
                {"id": "user-1", "is_admin": False},
                dependencies=_forwarding_deps(db, suspension),
            )

        assert exc.value.status_code == 409
        db.get_agent.assert_not_awaited()
        suspension.restore_thread_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stateless_suspended_forwarding_refuses_legacy_restore(self):
        suspended = _forwarding_stateless_thread(ready=False)
        workspace = suspended["metadata"]["workspace_container"]
        workspace["status"] = "suspended"
        workspace["_snapshot_restore_required"] = True
        workspace["_runtime_creation"]["mode"] = "restore"
        ready = _forwarding_stateless_thread(ready=True)
        db = SimpleNamespace(
            get_thread=AsyncMock(side_effect=[suspended, ready]),
            get_agent=AsyncMock(return_value={"id": "agent-1", "pod_ip": "10.0.0.2"}),
        )
        suspension = SimpleNamespace(
            is_enabled=True,
            restore_thread_workspace=AsyncMock(),
        )

        with pytest.raises(HTTPException) as exc:
            await pinned_forwarding.resolve_thread_for_forwarding(
                FORWARD_THREAD_ID,
                {"id": "user-1", "is_admin": False},
                dependencies=_forwarding_deps(db, suspension),
            )

        assert exc.value.status_code == 409
        suspension.restore_thread_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stateless_forwarding_refuses_malformed_physical_class_first(self):
        thread = _forwarding_stateless_thread(ready=True)
        thread["metadata"]["vm"] = {"status": "ready"}
        db = SimpleNamespace(get_thread=AsyncMock(return_value=thread))

        with pytest.raises(HTTPException) as exc:
            await pinned_forwarding.resolve_thread_for_forwarding(
                FORWARD_THREAD_ID,
                {"id": "user-1", "is_admin": False},
                dependencies=_forwarding_deps(db),
            )

        assert exc.value.status_code == 409
        _assert_forwarding_has_no_reconciliation_path()

    @pytest.mark.asyncio
    async def test_orphan_thread_fail_closed(self, user_a, fake_db, fake_request):
        """G3 core fix: thread with user_id=NULL is no longer publicly readable."""
        from orchestrator.security.access import require_thread_owner

        orphan_id = "ccc55555-5555-5555-5555-555555555555"
        fake_db.get_thread = AsyncMock(
            return_value={"id": orphan_id, "user_id": None, "title": "ophan"}
        )
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await require_thread_owner(fake_request, fake_db, orphan_id)
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_orphan_thread_admin_bypass(self, user_admin, fake_db, fake_request):
        """Admins still see orphans (they may need to clean them up)."""
        from orchestrator.security.access import require_thread_owner

        orphan_id = "ccc55555-5555-5555-5555-555555555555"
        orphan = {"id": orphan_id, "user_id": None, "title": "orphan"}
        fake_db.get_thread = AsyncMock(return_value=orphan)
        with _patch_caller(user_admin):
            user, thread = await require_thread_owner(fake_request, fake_db, orphan_id)
        assert thread is orphan

    @pytest.mark.asyncio
    async def test_admin_bypass(self, user_admin, thread_a, fake_db, fake_request):
        from orchestrator.security.access import require_thread_owner

        with _patch_caller(user_admin):
            user, thread = await require_thread_owner(
                fake_request, fake_db, str(thread_a["id"])
            )
        assert thread is thread_a

    @pytest.mark.asyncio
    async def test_missing_404(self, user_a, fake_db, fake_request):
        from orchestrator.security.access import require_thread_owner

        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await require_thread_owner(
                    fake_request, fake_db, "00000000-0000-0000-0000-000000000999"
                )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_mcp_project_scoped_token_rejected(
        self, user_a, thread_a, project_a, fake_db, fake_request
    ):
        """Threads have no project, so a project:<uuid> token can't read them."""
        from orchestrator.security.access import require_thread_owner

        scoped = _scoped(user_a, f"project:{project_a['id']}")
        with _patch_caller(scoped):
            with pytest.raises(HTTPException) as exc:
                await require_thread_owner(fake_request, fake_db, str(thread_a["id"]))
        assert exc.value.status_code == 403


# =============================================================================
# Endpoint samples — gate is wired
# =============================================================================


class TestThreadEndpointGates:
    @pytest.mark.asyncio
    async def test_get_thread_orphan_blocked(self, user_a, fake_db, fake_request):
        orphan_id = "ccc55555-5555-5555-5555-555555555555"
        fake_db.get_thread = AsyncMock(
            return_value={"id": orphan_id, "user_id": None, "title": "orphan"}
        )
        # resolve_cloud_session_url should NOT be reached.
        sentinel = MagicMock(side_effect=AssertionError("called past gate"))
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await thread_session.get_thread(
                    orphan_id,
                    fake_request,
                    dependencies=_session_deps(fake_db, sentinel),
                )
        assert exc.value.status_code == 403
        sentinel.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_thread_cross_user_blocked(
        self, user_b, thread_a, fake_db, fake_request
    ):
        sentinel = MagicMock(side_effect=AssertionError("called past gate"))
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await thread_session.get_thread(
                    str(thread_a["id"]),
                    fake_request,
                    dependencies=_session_deps(fake_db, sentinel),
                )
        assert exc.value.status_code == 403
        sentinel.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_thread_owner_passes(
        self, user_a, thread_a, fake_db, fake_request
    ):
        with _patch_caller(user_a):
            result = await thread_session.get_thread(
                str(thread_a["id"]),
                fake_request,
                dependencies=_session_deps(fake_db, MagicMock(return_value=None)),
            )
        assert result["id"] == thread_a["id"]

    @pytest.mark.asyncio
    async def test_get_thread_messages_history_cross_user_blocked(
        self, user_b, thread_a, fake_db, fake_request
    ):
        fake_db.get_thread_messages_history = AsyncMock(
            side_effect=AssertionError("called past gate")
        )
        with _patch_caller(user_b):
            with pytest.raises(HTTPException) as exc:
                await thread_history.get_thread_messages_history(
                    str(thread_a["id"]),
                    fake_request,
                    limit=200,
                    offset=0,
                    dependencies=_history_deps(fake_db),
                )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_thread_event_stream_orphan_blocked(
        self, user_a, fake_db, fake_request
    ):
        orphan_id = "ccc55555-5555-5555-5555-555555555555"
        fake_db.get_thread = AsyncMock(
            return_value={
                "id": orphan_id,
                "user_id": None,
                "events_epoch": 0,
                "title": "x",
            }
        )
        with _patch_caller(user_a):
            with pytest.raises(HTTPException) as exc:
                await thread_transport.thread_event_stream(
                    orphan_id, fake_request, dependencies=_transport_deps(fake_db)
                )
        assert exc.value.status_code == 403
