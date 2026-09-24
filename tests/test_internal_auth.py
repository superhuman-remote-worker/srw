"""P4b — Track B internal-auth helpers + endpoint coverage.

The agent ↔ orchestrator boundary is now defended at two layers:

  1. ``helm/templates/ingress.yaml`` strips ``/api/agents/`` and
     ``/api/internal/`` from the public API ingress (a Traefik
     ``IPAllowList`` middleware on a high-priority Ingress).
  2. Every agent-internal endpoint calls ``require_internal`` (or, for
     the dual-callable job mutations, ``require_internal_or_job_access``).

This file tests layer 2. Layer 1 lives in the Helm chart and is
out-of-scope for unit tests — it's exercised in cluster.
"""

from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

# R1.B06: agent registration moved to services/agent_registration with its
# route in routers/agent_registration. main's dependency factory still reads
# main's attributes at call time, so the patches below keep steering what
# they steered before.
from orchestrator.services import agent_registration  # noqa: E402
from fastapi import HTTPException
from pydantic import ValidationError

import orchestrator.security.access as access_module
from orchestrator.application import completion as completion_composition
from orchestrator.application import preparation as preparation_composition
from orchestrator.application import sessions as sessions_composition
from orchestrator.schemas import agent_runtime as agent_runtime_module
from orchestrator.security import access as _access_module
from orchestrator.services import (
    job_datasource_selection as job_datasource_selection_module,
)
from orchestrator.services import (
    session_attach_binding as session_attach_binding_module,
)
import functools


_PINNED_THREAD_ID = "11111111-1111-4111-8111-111111111111"
_PINNED_GENERATION = "22222222-2222-4222-8222-222222222222"
_PINNED_ATTACH_TOKEN = "33333333-3333-4333-8333-333333333333"


def _pinned_thread(
    *, agent_id: str | None, runtime_attach_token: str | None = None
) -> dict:
    return {
        "id": _PINNED_THREAD_ID,
        "execution_lane": "pinned",
        "status": "created",
        "runtime_generation": _PINNED_GENERATION,
        "runtime_retirement_token": None,
        "agent_id": agent_id,
        "runtime_attach_token": runtime_attach_token,
        "metadata": {},
    }


def _registration_thread_reads(
    *,
    initial_agent_id: str | None,
    final_agent_id: str,
    initial_reads: int = 2,
) -> list[dict]:
    initial = _pinned_thread(agent_id=initial_agent_id)
    final = _pinned_thread(
        agent_id=final_agent_id,
        runtime_attach_token=_PINNED_ATTACH_TOKEN,
    )
    return [initial] * initial_reads + [final] * 4


def _assert_bound_await(mock, *args, store) -> None:
    """``mock`` (a patched owner operation) was awaited once with exactly
    ``args`` plus the dependencies the application's composition built for it
    from ``store`` (the store the application was patched with)."""

    mock.assert_awaited_once()
    assert mock.await_args.args == args
    assert set(mock.await_args.kwargs) == {"dependencies"}
    assert mock.await_args.kwargs["dependencies"].store is store


def _make_request(headers: dict[str, str] | None = None) -> MagicMock:
    """Build a stub FastAPI Request with the given headers dict."""
    req = MagicMock()
    req.headers = headers or {}
    req.cookies = {}
    return req


def _patch_caller_and_db(user: dict, db):
    stack = ExitStack()
    stack.enter_context(
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    return stack


# =============================================================================
# is_internal_call / require_internal — the helpers themselves
# =============================================================================


class TestIsInternalCall:
    def test_returns_false_when_key_unset(self):
        with patch.object(access_module, "_INTERNAL_KEY", ""):
            req = _make_request({"X-Internal-Key": "anything"})
            assert access_module.is_internal_call(req) is False

    def test_returns_false_when_header_missing(self):
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            req = _make_request({})
            assert access_module.is_internal_call(req) is False

    def test_returns_false_when_header_wrong(self):
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            req = _make_request({"X-Internal-Key": "wrong"})
            assert access_module.is_internal_call(req) is False

    def test_returns_true_when_header_matches(self):
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            req = _make_request({"X-Internal-Key": "secret"})
            assert access_module.is_internal_call(req) is True


class TestRequireInternal:
    @pytest.mark.asyncio
    async def test_raises_401_without_key(self):
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            req = _make_request({})
            with pytest.raises(HTTPException) as exc:
                await access_module.require_internal(req)
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_raises_401_when_key_unset(self):
        """Fail-closed: an unconfigured cluster denies all internal calls
        loudly rather than silently letting traffic through."""
        with patch.object(access_module, "_INTERNAL_KEY", ""):
            req = _make_request({"X-Internal-Key": "anything"})
            with pytest.raises(HTTPException) as exc:
                await access_module.require_internal(req)
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_passes_with_valid_key(self):
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            req = _make_request({"X-Internal-Key": "secret"})
            await access_module.require_internal(req)  # no raise


class TestRequireInternalOrJobAccess:
    @pytest.mark.asyncio
    async def test_internal_call_skips_user_auth(self, job_a, fake_db):
        """Agent path: valid X-Internal-Key bypasses the user auth chain."""
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            req = _make_request({"X-Internal-Key": "secret"})
            with patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(side_effect=AssertionError("user auth called")),
            ):
                caller, job = await access_module.require_internal_or_job_access(
                    req, fake_db, str(job_a["id"])
                )
        assert caller is None
        assert str(job["id"]) == str(job_a["id"])

    @pytest.mark.asyncio
    async def test_internal_call_with_missing_job_404(self, fake_db):
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            req = _make_request({"X-Internal-Key": "secret"})
            with pytest.raises(HTTPException) as exc:
                await access_module.require_internal_or_job_access(
                    req, fake_db, "00000000-0000-0000-0000-000000000000"
                )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_user_path_runs_require_job_access(self, user_a, job_a, fake_db):
        """No internal key → falls through to require_job_access. user_a
        is the owner of job_a → call succeeds."""
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            req = _make_request({})
            with patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(return_value=user_a),
            ):
                caller, job = await access_module.require_internal_or_job_access(
                    req, fake_db, str(job_a["id"])
                )
        assert str(caller["id"]) == str(user_a["id"])
        assert str(job["id"]) == str(job_a["id"])

    @pytest.mark.asyncio
    async def test_user_path_blocks_cross_user(self, user_b, job_a, fake_db):
        """No internal key + cross-user → 403 from require_job_access."""
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            req = _make_request({})
            with patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(return_value=user_b),
            ):
                with pytest.raises(HTTPException) as exc:
                    await access_module.require_internal_or_job_access(
                        req, fake_db, str(job_a["id"])
                    )
        assert exc.value.status_code == 403


# =============================================================================
# Endpoint integration — pure-internal endpoints reject without the key
# =============================================================================


class TestPureInternalEndpoints:
    def test_agent_registration_rejects_self_verified_provenance(self):
        from orchestrator.schemas.agent_runtime import AgentRegistration

        with pytest.raises(ValidationError, match="self-assert verified"):
            AgentRegistration(
                config_name="scholar",
                pod_ip="10.0.0.1",
                product_provenance={
                    "source_revision": "a" * 40,
                    "artifact_digest": f"sha256:{'b' * 64}",
                    "provenance_status": "verified",
                },
            )

    @pytest.mark.asyncio
    async def test_agent_register_without_key_401(self, fake_request):
        from orchestrator.schemas.agent_runtime import AgentRegistration
        import orchestrator.main as orch_main
        from orchestrator.routers.agent_registration import register_agent

        reg = AgentRegistration(
            config_name="scholar", pod_ip="10.0.0.1", hostname="agent-1"
        )
        # R1.B06: the transport gate is the router's — the endpoint-auth
        # scanner reads it from the route declaration — so the 401 case
        # drives the route, not the service.
        fake_request.app.state.agent_registration_dependencies_factory = (
            functools.partial(
                sessions_composition.agent_registration_dependencies,
                orch_main.app.state.resources,
            )
        )
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            with pytest.raises(HTTPException) as exc:
                await register_agent(fake_request, reg)
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "thread",
        [
            None,
            {"id": "thread-1", "execution_lane": "stateless", "agent_id": None},
            {"id": "thread-1", "execution_lane": "future-lane", "agent_id": None},
        ],
    )
    async def test_persistent_registration_refuses_non_pinned_thread(self, thread):
        """The lane fence runs before a hostname upsert can mutate any row."""
        import orchestrator.main as orch_main

        reg = agent_runtime_module.AgentRegistration(
            config_name="session_base",
            pod_ip="10.0.0.1",
            hostname="agent-1",
            agent_mode="persistent",
            thread_id=_PINNED_THREAD_ID,
            session_runtime_generation=UUID(_PINNED_GENERATION),
        )
        db = MagicMock()
        db.register_agent = AsyncMock(
            return_value={"agent_id": "agent-new", "heartbeat_interval_seconds": 20}
        )
        db.get_thread = AsyncMock(return_value=thread)
        db.delete_agent = AsyncMock()
        db.update_thread_agent = AsyncMock()
        lock_cm = AsyncMock()
        lock_cm.__aenter__.return_value = None
        lock_cm.__aexit__.return_value = False
        db.thread_advisory_lock = MagicMock(return_value=lock_cm)

        with (
            patch.object(_access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            pytest.raises(HTTPException) as exc,
        ):
            await agent_registration.register_agent(
                MagicMock(),
                reg,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert exc.value.status_code == 409
        db.register_agent.assert_not_awaited()
        db.delete_agent.assert_not_awaited()
        db.update_thread_agent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_persistent_registration_still_binds_pinned_thread(self):
        import orchestrator.main as orch_main

        reg = agent_runtime_module.AgentRegistration(
            config_name="session_base",
            pod_ip="10.0.0.1",
            hostname="agent-1",
            agent_mode="persistent",
            thread_id=_PINNED_THREAD_ID,
            session_runtime_generation=UUID(_PINNED_GENERATION),
        )
        db = MagicMock()
        db.register_agent = AsyncMock(
            return_value={
                "agent_id": "agent-new",
                "heartbeat_interval_seconds": 20,
                "dispatch_process_generation": "process-new",
            }
        )
        db.fetchrow = AsyncMock(return_value=None)
        db.get_thread = AsyncMock(
            side_effect=_registration_thread_reads(
                initial_agent_id=None, final_agent_id="agent-new"
            )
        )
        db.delete_agent = AsyncMock()
        db.update_thread_agent = AsyncMock()
        lock_cm = AsyncMock()
        lock_cm.__aenter__.return_value = None
        lock_cm.__aexit__.return_value = False
        db.thread_advisory_lock = MagicMock(return_value=lock_cm)

        with (
            patch.object(_access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(
                session_attach_binding_module,
                "bind_registered_persistent_agent",
                AsyncMock(return_value=_PINNED_ATTACH_TOKEN),
            ) as bind,
        ):
            response = await agent_registration.register_agent(
                MagicMock(),
                reg,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert response.agent_id == "agent-new"
        assert response.dispatch_process_generation == "process-new"
        assert response.model_dump()["dispatch_process_generation"] == "process-new"
        assert response.runtime_actor is None
        assert (
            db.register_agent.await_args.kwargs["completion_commands_enabled"]
            is orch_main.app.state.resources.settings.completion_commands_enabled
        )
        assert db.register_agent.await_args.kwargs["insert_only"] is True
        assert db.register_agent.await_args.kwargs["expected_agent_id"] is None
        _assert_bound_await(
            bind,
            _PINNED_THREAD_ID,
            "agent-new",
            None,
            _PINNED_GENERATION,
            store=db,
        )
        db.delete_agent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_live_persistent_owner_rejects_other_hostname_before_upsert(self):
        import orchestrator.main as orch_main

        reg = agent_runtime_module.AgentRegistration(
            config_name="session_base",
            pod_ip="10.0.0.2",
            hostname="agent-loser",
            agent_mode="persistent",
            thread_id=_PINNED_THREAD_ID,
            session_runtime_generation=UUID(_PINNED_GENERATION),
        )
        db = MagicMock()
        db.get_thread = AsyncMock(
            side_effect=_registration_thread_reads(
                initial_agent_id="agent-winner",
                final_agent_id="agent-winner",
                initial_reads=4,
            )
        )
        db.get_agent = AsyncMock(
            return_value={
                "id": "agent-winner",
                "hostname": "agent-winner-host",
                "status": "session",
            }
        )
        db.register_agent = AsyncMock()
        db.delete_agent = AsyncMock()
        lock_cm = AsyncMock()
        lock_cm.__aenter__.return_value = None
        lock_cm.__aexit__.return_value = False
        db.thread_advisory_lock = MagicMock(return_value=lock_cm)

        with (
            patch.object(_access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            pytest.raises(HTTPException) as exc,
        ):
            await agent_registration.register_agent(
                MagicMock(),
                reg,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert exc.value.status_code == 409
        db.register_agent.assert_not_awaited()
        db.delete_agent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_same_hostname_restart_targets_exact_live_owner(self):
        import orchestrator.main as orch_main

        reg = agent_runtime_module.AgentRegistration(
            config_name="session_base",
            pod_ip="10.0.0.2",
            hostname="agent-winner-host",
            agent_mode="persistent",
            thread_id=_PINNED_THREAD_ID,
            session_runtime_generation=UUID(_PINNED_GENERATION),
        )
        db = MagicMock()
        db.get_thread = AsyncMock(
            side_effect=_registration_thread_reads(
                initial_agent_id="agent-winner",
                final_agent_id="agent-winner",
                initial_reads=4,
            )
        )
        db.get_agent = AsyncMock(
            return_value={
                "id": "agent-winner",
                "hostname": "agent-winner-host",
                "status": "session",
            }
        )
        db.register_agent = AsyncMock(
            return_value={
                "agent_id": "agent-winner",
                "heartbeat_interval_seconds": 20,
                "dispatch_process_generation": "process-winner",
            }
        )
        db.fetchrow = AsyncMock(return_value=None)
        db.delete_agent = AsyncMock()
        lock_cm = AsyncMock()
        lock_cm.__aenter__.return_value = None
        lock_cm.__aexit__.return_value = False
        db.thread_advisory_lock = MagicMock(return_value=lock_cm)

        with (
            patch.object(_access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(
                session_attach_binding_module,
                "bind_registered_persistent_agent",
                AsyncMock(return_value=_PINNED_ATTACH_TOKEN),
            ) as bind,
        ):
            response = await agent_registration.register_agent(
                MagicMock(),
                reg,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert response.agent_id == "agent-winner"
        assert (
            db.register_agent.await_args.kwargs["expected_agent_id"] == "agent-winner"
        )
        assert db.register_agent.await_args.kwargs["insert_only"] is False
        _assert_bound_await(
            bind,
            _PINNED_THREAD_ID,
            "agent-winner",
            "agent-winner",
            _PINNED_GENERATION,
            store=db,
        )
        db.delete_agent.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("owner_status", ["offline", "failed"])
    async def test_same_hostname_stale_owner_still_targets_exact_row(
        self, owner_status
    ):
        import orchestrator.main as orch_main

        reg = agent_runtime_module.AgentRegistration(
            config_name="session_base",
            pod_ip="10.0.0.2",
            hostname="agent-owner-host",
            agent_mode="persistent",
            thread_id=_PINNED_THREAD_ID,
            session_runtime_generation=UUID(_PINNED_GENERATION),
        )
        db = MagicMock()
        db.get_thread = AsyncMock(
            side_effect=_registration_thread_reads(
                initial_agent_id="agent-owner",
                final_agent_id="agent-owner",
                initial_reads=4,
            )
        )
        db.get_agent = AsyncMock(
            return_value={
                "id": "agent-owner",
                "hostname": "agent-owner-host",
                "status": owner_status,
            }
        )
        db.register_agent = AsyncMock(
            return_value={
                "agent_id": "agent-owner",
                "heartbeat_interval_seconds": 20,
                "dispatch_process_generation": "process-owner",
            }
        )
        db.fetchrow = AsyncMock(return_value=None)
        lock_cm = AsyncMock()
        lock_cm.__aenter__.return_value = None
        lock_cm.__aexit__.return_value = False
        db.thread_advisory_lock = MagicMock(return_value=lock_cm)

        with (
            patch.object(_access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(
                session_attach_binding_module,
                "bind_registered_persistent_agent",
                AsyncMock(return_value=_PINNED_ATTACH_TOKEN),
            ),
        ):
            await agent_registration.register_agent(
                MagicMock(),
                reg,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert db.register_agent.await_args.kwargs["expected_agent_id"] == "agent-owner"
        assert db.register_agent.await_args.kwargs["insert_only"] is False

    @pytest.mark.asyncio
    async def test_different_hostname_replacement_of_stale_owner_inserts_new_row(self):
        import orchestrator.main as orch_main

        reg = agent_runtime_module.AgentRegistration(
            config_name="session_base",
            pod_ip="10.0.0.2",
            hostname="agent-replacement-host",
            agent_mode="persistent",
            thread_id=_PINNED_THREAD_ID,
            session_runtime_generation=UUID(_PINNED_GENERATION),
        )
        db = MagicMock()
        db.get_thread = AsyncMock(
            side_effect=_registration_thread_reads(
                initial_agent_id="agent-offline",
                final_agent_id="agent-fresh",
                initial_reads=4,
            )
        )
        db.get_agent = AsyncMock(
            return_value={
                "id": "agent-offline",
                "hostname": "old-host",
                "status": "offline",
            }
        )
        db.register_agent = AsyncMock(
            return_value={
                "agent_id": "agent-fresh",
                "heartbeat_interval_seconds": 20,
                "dispatch_process_generation": "process-fresh",
            }
        )
        db.fetchrow = AsyncMock(return_value=None)
        lock_cm = AsyncMock()
        lock_cm.__aenter__.return_value = None
        lock_cm.__aexit__.return_value = False
        db.thread_advisory_lock = MagicMock(return_value=lock_cm)

        with (
            patch.object(_access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(
                session_attach_binding_module,
                "bind_registered_persistent_agent",
                AsyncMock(return_value=_PINNED_ATTACH_TOKEN),
            ) as bind,
        ):
            await agent_registration.register_agent(
                MagicMock(),
                reg,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert db.register_agent.await_args.kwargs["expected_agent_id"] is None
        assert db.register_agent.await_args.kwargs["insert_only"] is True
        _assert_bound_await(
            bind,
            _PINNED_THREAD_ID,
            "agent-fresh",
            "agent-offline",
            _PINNED_GENERATION,
            store=db,
        )

    @pytest.mark.asyncio
    async def test_missing_snapshotted_owner_refuses_before_upsert(self):
        import orchestrator.main as orch_main

        reg = agent_runtime_module.AgentRegistration(
            config_name="session_base",
            pod_ip="10.0.0.2",
            hostname="agent-replacement",
            agent_mode="persistent",
            thread_id=_PINNED_THREAD_ID,
            session_runtime_generation=UUID(_PINNED_GENERATION),
        )
        db = MagicMock()
        db.get_thread = AsyncMock(return_value=_pinned_thread(agent_id="agent-missing"))
        db.get_agent = AsyncMock(return_value=None)
        db.register_agent = AsyncMock()
        lock_cm = AsyncMock()
        lock_cm.__aenter__.return_value = None
        lock_cm.__aexit__.return_value = False
        db.thread_advisory_lock = MagicMock(return_value=lock_cm)

        with (
            patch.object(_access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            pytest.raises(HTTPException) as exc,
        ):
            await agent_registration.register_agent(
                MagicMock(),
                reg,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert exc.value.status_code == 409
        db.register_agent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_persistent_registration_refuses_a_lost_final_lane_bind(self):
        import orchestrator.main as orch_main

        reg = agent_runtime_module.AgentRegistration(
            config_name="session_base",
            pod_ip="10.0.0.1",
            hostname="agent-1",
            agent_mode="persistent",
            thread_id=_PINNED_THREAD_ID,
            session_runtime_generation=UUID(_PINNED_GENERATION),
        )
        db = MagicMock()
        db.register_agent = AsyncMock(
            return_value={"agent_id": "agent-new", "heartbeat_interval_seconds": 20}
        )
        db.fetchrow = AsyncMock(return_value=None)
        db.get_thread = AsyncMock(return_value=_pinned_thread(agent_id=None))
        lock_cm = AsyncMock()
        lock_cm.__aenter__.return_value = None
        lock_cm.__aexit__.return_value = False
        db.thread_advisory_lock = MagicMock(return_value=lock_cm)

        with (
            patch.object(_access_module, "require_internal", AsyncMock()),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(
                session_attach_binding_module,
                "bind_registered_persistent_agent",
                AsyncMock(return_value=None),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await agent_registration.register_agent(
                MagicMock(),
                reg,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert exc.value.status_code == 409
        db.register_agent.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_final_persistent_bind_is_lane_qualified_and_clears_exact_agent(self):
        import orchestrator.main as orch_main
        from orchestrator.services import session_attach_binding

        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 0")
        transaction = AsyncMock()
        transaction.__aenter__.return_value = None
        transaction.__aexit__.return_value = False
        conn.transaction = MagicMock(return_value=transaction)
        acquire = AsyncMock()
        acquire.__aenter__.return_value = conn
        acquire.__aexit__.return_value = False
        db = MagicMock()
        db.acquire = MagicMock(return_value=acquire)

        with (
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            # R1.B06: the bind moved to services/session_attach_binding, which
            # has its own ``uuid4``. Patching main's would be green but inert.
            patch.object(
                session_attach_binding,
                "uuid4",
                return_value=UUID(_PINNED_ATTACH_TOKEN),
            ),
        ):
            bound = await session_attach_binding_module.bind_registered_persistent_agent(
                _PINNED_THREAD_ID,
                "agent-new",
                None,
                _PINNED_GENERATION,
                dependencies=sessions_composition.session_attach_binding_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert bound is None
        assert "execution_lane = $3" in conn.execute.await_args_list[0].args[0]
        assert "agent_id IS NULL" in conn.execute.await_args_list[0].args[0]
        assert conn.execute.await_args_list[0].args[1:] == (
            _PINNED_THREAD_ID,
            "agent-new",
            "pinned",
            _PINNED_GENERATION,
            _PINNED_ATTACH_TOKEN,
        )
        assert conn.execute.await_count == 1

    @pytest.mark.asyncio
    async def test_final_persistent_bind_cas_matches_the_snapshotted_owner(self):
        import orchestrator.main as orch_main
        from orchestrator.services import session_attach_binding

        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        transaction = AsyncMock()
        transaction.__aenter__.return_value = None
        transaction.__aexit__.return_value = False
        conn.transaction = MagicMock(return_value=transaction)
        acquire = AsyncMock()
        acquire.__aenter__.return_value = conn
        acquire.__aexit__.return_value = False
        db = MagicMock()
        db.acquire = MagicMock(return_value=acquire)

        with (
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            # R1.B06: the bind moved to services/session_attach_binding, which
            # has its own ``uuid4``. Patching main's would be green but inert.
            patch.object(
                session_attach_binding,
                "uuid4",
                return_value=UUID(_PINNED_ATTACH_TOKEN),
            ),
        ):
            bound = await session_attach_binding_module.bind_registered_persistent_agent(
                _PINNED_THREAD_ID,
                "agent-new",
                "agent-offline-snapshot",
                _PINNED_GENERATION,
                dependencies=sessions_composition.session_attach_binding_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert bound == _PINNED_ATTACH_TOKEN
        thread_update = conn.execute.await_args_list[0]
        assert "agent_id = $4" in thread_update.args[0]
        assert thread_update.args[1:] == (
            _PINNED_THREAD_ID,
            "agent-new",
            "pinned",
            "agent-offline-snapshot",
            _PINNED_GENERATION,
            _PINNED_ATTACH_TOKEN,
        )
        reciprocal = conn.execute.await_args_list[1]
        assert "UPDATE agents SET thread_id=$2::uuid" in reciprocal.args[0]
        assert reciprocal.args[1:] == ("agent-new", _PINNED_THREAD_ID)

    @pytest.mark.asyncio
    async def test_agent_heartbeat_without_key_401(self, fake_request):
        from orchestrator.schemas.agent_runtime import AgentHeartbeat
        import orchestrator.main as orch_main
        from orchestrator.routers.agent_registration import agent_heartbeat

        hb = AgentHeartbeat(status="ready", current_job_id=None, metrics=None)
        # R1.B06: the transport gate is the router's — the endpoint-auth scanner
        # reads it from the route declaration — so the 401 case drives the route.
        fake_request.app.state.agent_registration_dependencies_factory = (
            functools.partial(
                sessions_composition.agent_registration_dependencies,
                orch_main.app.state.resources,
            )
        )
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            with pytest.raises(HTTPException) as exc:
                await agent_heartbeat(fake_request, "agent-1", hb)
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_agent_heartbeat_merges_graph_progress_into_metrics(
        self, fake_request
    ):
        from orchestrator.schemas.agent_runtime import AgentHeartbeat
        import orchestrator.main as orch_main
        from orchestrator.services.agent_registration import agent_heartbeat

        hb = AgentHeartbeat(
            status="working",
            current_job_id="11111111-1111-1111-1111-111111111111",
            metrics={"memory_mb": 128},
            graph_progress=9,
        )
        fake_db = MagicMock()
        fake_db.heartbeat = AsyncMock(
            return_value={"previous_status": "working", "effective_status": "working"}
        )

        fake_request.headers = {"X-Internal-Key": "secret"}
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        ):
            result = await agent_heartbeat(
                fake_request,
                "agent-1",
                hb,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        fake_db.heartbeat.assert_awaited_once_with(
            agent_id="agent-1",
            status="working",
            current_job_id="11111111-1111-1111-1111-111111111111",
            metrics={"memory_mb": 128, "graph_progress": 9},
            aux_degraded=None,
            session_runtime_generation=None,
            session_runtime_attach_token=None,
            require_pinned_identity=True,
        )
        # The legacy keys are the back-compat contract for older agent builds.
        # `job_status` was added alongside them (Defect 3 of
        # knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md) and
        # is additive, so assert on the contract rather than exact equality —
        # here it degrades to None because the fake db has no get_job.
        assert result["status"] == "ok"
        assert result["intents"] == {}
        assert result["job_status"] is None

    @pytest.mark.asyncio
    async def test_agent_heartbeat_reports_current_job_status(self, fake_request):
        """The backstop that tells a running agent its job was taken away.

        Job c6dd288d streamed for 21 more minutes after being failed
        out-of-band because the heartbeat carried nothing back.
        knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md (Defect 3)
        """
        from orchestrator.schemas.agent_runtime import AgentHeartbeat
        import orchestrator.main as orch_main
        from orchestrator.services.agent_registration import agent_heartbeat

        job_id = "11111111-1111-1111-1111-111111111111"
        hb = AgentHeartbeat(status="working", current_job_id=job_id)
        fake_db = MagicMock()
        fake_db.heartbeat = AsyncMock(
            return_value={"previous_status": "working", "effective_status": "working"}
        )
        fake_db.get_job = AsyncMock(return_value={"id": job_id, "status": "failed"})

        fake_request.headers = {"X-Internal-Key": "secret"}
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        ):
            result = await agent_heartbeat(
                fake_request,
                "agent-1",
                hb,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert result["job_status"] == "failed"

    @pytest.mark.asyncio
    async def test_agent_heartbeat_survives_a_job_lookup_failure(self, fake_request):
        """A heartbeat must never fail over the status lookup — it degrades to
        the previous push-only behaviour instead."""
        from orchestrator.schemas.agent_runtime import AgentHeartbeat
        import orchestrator.main as orch_main
        from orchestrator.services.agent_registration import agent_heartbeat

        hb = AgentHeartbeat(
            status="working", current_job_id="11111111-1111-1111-1111-111111111111"
        )
        fake_db = MagicMock()
        fake_db.heartbeat = AsyncMock(
            return_value={"previous_status": "working", "effective_status": "working"}
        )
        fake_db.get_job = AsyncMock(side_effect=Exception("db down"))

        fake_request.headers = {"X-Internal-Key": "secret"}
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        ):
            result = await agent_heartbeat(
                fake_request,
                "agent-1",
                hb,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert result["status"] == "ok"
        assert result["job_status"] is None

    @pytest.mark.asyncio
    async def test_agent_heartbeat_graph_progress_overrides_metric_field(
        self, fake_request
    ):
        from orchestrator.schemas.agent_runtime import AgentHeartbeat
        import orchestrator.main as orch_main
        from orchestrator.services.agent_registration import agent_heartbeat

        hb = AgentHeartbeat(
            status="working",
            current_job_id="11111111-1111-1111-1111-111111111111",
            metrics={"graph_progress": 1, "memory_mb": 256},
            graph_progress=2,
        )
        fake_db = MagicMock()
        fake_db.heartbeat = AsyncMock(
            return_value={"previous_status": "working", "effective_status": "working"}
        )

        fake_request.headers = {"X-Internal-Key": "secret"}
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        ):
            await agent_heartbeat(
                fake_request,
                "agent-1",
                hb,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        fake_db.heartbeat.assert_awaited_once_with(
            agent_id="agent-1",
            status="working",
            current_job_id="11111111-1111-1111-1111-111111111111",
            metrics={"memory_mb": 256, "graph_progress": 2},
            aux_degraded=None,
            session_runtime_generation=None,
            session_runtime_attach_token=None,
            require_pinned_identity=True,
        )

    # -- runtime-actor liveness slide ------------------------------------
    # A heartbeat from a thread-bound runtime IS the liveness signal that
    # licenses extending its actor grant. Before this, the grant only slid
    # inside a refresh, and the runtime only refreshes for a PRIVILEGED
    # call — so officer 6ce5bc4c, awake every 10 minutes for 24h reading
    # SITREPs, hit the 24h wall while running.
    # knowledge/issues/officer_runtime_grant_expires_after_24h_and_dies_silently.md

    @pytest.mark.asyncio
    async def test_agent_heartbeat_slides_the_bound_threads_grant(self, fake_request):
        from orchestrator.schemas.agent_runtime import AgentHeartbeat
        import orchestrator.main as orch_main
        from orchestrator.services.agent_registration import agent_heartbeat

        thread_id = "22222222-2222-2222-2222-222222222222"
        hb = AgentHeartbeat(status="ready")
        fake_db = MagicMock()
        fake_db.heartbeat = AsyncMock(
            return_value={
                "previous_status": "ready",
                "effective_status": "ready",
                "thread_id": thread_id,
            }
        )
        slide = AsyncMock(return_value=True)

        fake_request.headers = {"X-Internal-Key": "secret"}
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.services.runtime_actor.slide_thread_grant_on_liveness",
                slide,
            ),
        ):
            result = await agent_heartbeat(
                fake_request,
                "agent-1",
                hb,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert result["status"] == "ok"
        slide.assert_awaited_once_with(
            fake_db,
            thread_id,
            agent_id="agent-1",
            session_runtime_generation=None,
            session_runtime_attach_token=None,
        )

    @pytest.mark.asyncio
    async def test_agent_heartbeat_without_a_bound_thread_slides_nothing(
        self, fake_request
    ):
        """A stateless worker agent has no thread and so no liveness claim."""
        from orchestrator.schemas.agent_runtime import AgentHeartbeat
        import orchestrator.main as orch_main
        from orchestrator.services.agent_registration import agent_heartbeat

        hb = AgentHeartbeat(status="ready")
        fake_db = MagicMock()
        fake_db.heartbeat = AsyncMock(
            return_value={
                "previous_status": "ready",
                "effective_status": "ready",
                "thread_id": None,
            }
        )
        slide = AsyncMock(return_value=False)

        fake_request.headers = {"X-Internal-Key": "secret"}
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.services.runtime_actor.slide_thread_grant_on_liveness",
                slide,
            ),
        ):
            result = await agent_heartbeat(
                fake_request,
                "agent-1",
                hb,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert result["status"] == "ok"
        slide.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_agent_heartbeat_survives_a_failing_liveness_slide(
        self, fake_request
    ):
        """Best-effort by construction: the credential window is never worth
        failing the liveness channel itself over."""
        from orchestrator.schemas.agent_runtime import AgentHeartbeat
        import orchestrator.main as orch_main
        from orchestrator.services.agent_registration import agent_heartbeat

        hb = AgentHeartbeat(status="ready")
        fake_db = MagicMock()
        fake_db.heartbeat = AsyncMock(
            return_value={
                "previous_status": "ready",
                "effective_status": "ready",
                "thread_id": "22222222-2222-2222-2222-222222222222",
            }
        )
        slide = AsyncMock(side_effect=Exception("db down"))

        fake_request.headers = {"X-Internal-Key": "secret"}
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.services.runtime_actor.slide_thread_grant_on_liveness",
                slide,
            ),
        ):
            result = await agent_heartbeat(
                fake_request,
                "agent-1",
                hb,
                dependencies=sessions_composition.agent_registration_dependencies(
                    orch_main.app.state.resources
                ),
            )

        assert result["status"] == "ok"
        slide.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_complete_job_without_key_401(self, fake_request, job_a):
        import orchestrator.main as orch_main
        from orchestrator.routers.job_completion import complete_job
        from orchestrator.schemas.job_runtime import JobCompleteRequest

        body = JobCompleteRequest(
            should_stop=True, goal_achieved=False, error=None, freeze_data=None
        )
        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            with pytest.raises(HTTPException) as exc:
                await complete_job(
                    fake_request,
                    str(job_a["id"]),
                    body,
                    dependencies=completion_composition.job_completion_dependencies(
                        orch_main.app.state.resources
                    ),
                )
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_subjob_merge_without_key_401(self, fake_request, job_a):
        from tests._b09_control_seams import subjob_merge

        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            with pytest.raises(HTTPException) as exc:
                await subjob_merge(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_agent_release_job_without_key_401(self, fake_request, job_a):
        from tests._b09_control_seams import agent_release_job

        with patch.object(access_module, "_INTERNAL_KEY", "secret"):
            with pytest.raises(HTTPException) as exc:
                await agent_release_job(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 401


# =============================================================================
# Endpoint integration — dual-callable endpoints
# =============================================================================


class TestDualCallableEndpoints:
    @pytest.mark.asyncio
    async def test_cancel_job_internal_bypasses_user_auth(
        self, fake_request, job_a, fake_db
    ):
        """Agent path: valid X-Internal-Key + load job → skip user check."""
        from tests._b09_control_seams import cancel_job

        fake_request.headers = {"X-Internal-Key": "secret"}
        # Job_a status is "created" — cancel handler will reach the next
        # check (status != processing) and not error out early.
        fake_db.cancel_job = AsyncMock(return_value=True)
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        ):
            # We patch require_approved_user to explode — if the gate ran
            # the user path, this would fire. It must not.
            with patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(side_effect=AssertionError("user auth ran for agent call")),
            ):
                # The handler may still fail after the gate (DB shape) — what
                # matters is that the AssertionError never fires.
                try:
                    await cancel_job(fake_request, str(job_a["id"]))
                except AssertionError:
                    raise
                except Exception:
                    pass  # any other error past the gate is fine for this test

    @pytest.mark.asyncio
    async def test_pause_job_user_path_blocked_cross_user(
        self, user_b, job_a, fake_db, fake_request
    ):
        """Cockpit path: no key, cross-user → 403 from require_job_access."""
        from tests._b09_control_seams import pause_job

        fake_request.headers = {}
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            _patch_caller_and_db(user_b, fake_db),
        ):
            with pytest.raises(HTTPException) as exc:
                await pause_job(fake_request, str(job_a["id"]))
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_create_job_internal_rejects_unbound_body_user(
        self, fake_request, fake_db
    ):
        """An internal key authenticates transport, not a claimed body user.

        Agent jobs must bind identity through a thread/parent (or an
        authenticated MCP-forwarded user); a bare body user_id is rejected.
        """
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {"X-Internal-Key": "secret"}
        body = JobCreate(
            description="delegation child",
            config_name="scholar",
            user_id="11111111-1111-1111-1111-111111111111",
        )
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.security.auth.require_approved_user",
                AsyncMock(side_effect=AssertionError("user auth ran")),
            ),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await create_job(fake_request, body)

        assert exc.value.status_code == 403
        assert exc.value.detail == "Internal job origin scope is unavailable"
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_job_rejects_originless_internal_request(
        self, fake_request, fake_db
    ):
        """The shared internal key is present in agent pods and therefore can
        never grant an originless HTTP "system job" bypass."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {"X-Internal-Key": "secret"}
        body = JobCreate(description="originless internal attempt")

        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await create_job(fake_request, body)

        assert exc.value.status_code == 403
        assert exc.value.detail == "Internal job origin scope is unavailable"
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_job_internal_thread_rejects_cross_project_native_scope(
        self,
        user_a,
        project_a,
        project_b,
        thread_a,
        fake_request,
        fake_db,
    ):
        """A session agent cannot point a child at another project's native KB
        or repositories by submitting a different project_id."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {"X-Internal-Key": "secret"}
        scoped_thread = {**thread_a, "project_id": project_a["id"]}
        fake_db.get_thread = AsyncMock(return_value=scoped_thread)
        fake_db.get_user = AsyncMock(return_value=user_a)
        body = JobCreate(
            description="cross-project attempt",
            thread_id=str(thread_a["id"]),
            project_id=str(project_b["id"]),
        )

        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.services.thread_mount_rows.thread_project_ids",
                AsyncMock(return_value=[str(project_a["id"])]),
            ),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await create_job(fake_request, body)

        assert exc.value.status_code == 403
        assert exc.value.detail == "Internal job origin scope is unavailable"
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_job_internal_thread_rejects_private_datasource_uuid(
        self,
        user_a,
        project_a,
        datasource_b,
        thread_a,
        fake_request,
        fake_db,
    ):
        """A valid thread principal still cannot attach another user's private
        datasource (including an external OKF KB) by guessing its UUID."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {"X-Internal-Key": "secret"}
        scoped_thread = {**thread_a, "project_id": project_a["id"]}
        fake_db.get_thread = AsyncMock(return_value=scoped_thread)
        fake_db.get_user = AsyncMock(return_value=user_a)
        body = JobCreate(
            description="private datasource attempt",
            thread_id=str(thread_a["id"]),
            datasource_ids=[str(datasource_b["id"])],
        )

        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.services.thread_mount_rows.thread_project_ids",
                AsyncMock(return_value=[str(project_a["id"])]),
            ),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await create_job(fake_request, body)

        assert exc.value.status_code == 403
        assert exc.value.detail == "One or more selected connectors are unavailable"
        assert str(datasource_b["id"]) not in exc.value.detail
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ownerless_internal_thread_cannot_add_arbitrary_datasource_uuid(
        self,
        thread_a,
        datasource_b,
        fake_request,
        fake_db,
    ):
        """Transport trust permits reuse, not ambient connector selection."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {"X-Internal-Key": "secret"}
        ownerless_thread = {
            **thread_a,
            "user_id": None,
            "project_id": None,
            "metadata": {"datasource_ids": []},
        }
        fake_db.get_thread = AsyncMock(return_value=ownerless_thread)
        body = JobCreate(
            description="ownerless datasource attempt",
            thread_id=str(thread_a["id"]),
            datasource_ids=[str(datasource_b["id"])],
        )

        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.services.thread_mount_rows.thread_project_ids",
                AsyncMock(return_value=[]),
            ),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await create_job(fake_request, body)

        assert exc.value.status_code == 403
        assert exc.value.detail == "One or more selected connectors are unavailable"
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_job_reauthorizes_inherited_parent_datasources(
        self,
        user_a,
        job_a,
        datasource_b,
        fake_request,
        fake_db,
    ):
        """A child cannot retain a datasource after the parent's owner's
        current access was revoked; inheritance is selection, not authority."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {"X-Internal-Key": "secret"}
        fake_db.get_user = AsyncMock(return_value=user_a)
        fake_db.list_job_datasource_ids = AsyncMock(
            return_value=[str(datasource_b["id"])]
        )
        body = JobCreate(
            description="inherit revoked datasource",
            parent_job_id=str(job_a["id"]),
            use_datasource_defaults=True,
        )

        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            patch(
                "orchestrator.services.datasource_policy.default_datasource_selection",
                AsyncMock(
                    side_effect=AssertionError(
                        "defaults must not override parent inheritance"
                    )
                ),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await create_job(fake_request, body)

        assert exc.value.status_code == 403
        assert exc.value.detail == "One or more selected connectors are unavailable"
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_job_user_path_forces_user_id(
        self, user_a, fake_db, fake_request
    ):
        """Cockpit path: body.user_id is overwritten with caller.id (F2 pattern).
        A malicious body trying to attribute the job to user_b is sanitized."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {}
        body = JobCreate(
            description="hijack attempt",
            config_name="scholar",
            user_id="22222222-2222-2222-2222-222222222222",  # not user_a
        )
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            _patch_caller_and_db(user_a, fake_db),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
        ):
            try:
                await create_job(fake_request, body)
            except Exception:
                pass
        # Body's user_id must now match the caller (user_a).
        assert str(body.user_id) == str(user_a["id"])

    @pytest.mark.asyncio
    async def test_create_job_revalidates_stale_public_default_project(
        self, user_a, project_a, fake_db, fake_request
    ):
        """A stale users.default_project_id cannot restore native KB/project
        scope after the user loses editor access."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {}
        fake_db.get_user = AsyncMock(
            return_value={**user_a, "default_project_id": project_a["id"]}
        )
        fake_db.get_user_role_in_project = AsyncMock(return_value="viewer")
        body = JobCreate(description="stale default project")

        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            _patch_caller_and_db(user_a, fake_db),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await create_job(fake_request, body)

        assert exc.value.status_code == 403
        assert exc.value.detail == "Project role 'editor' or higher required"
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_create_job_user_path_strips_system_markers(
        self, user_a, fake_db, fake_request
    ):
        """Public callers cannot self-declare subjobs or lifecycle runners."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {}
        fake_db.get_user = AsyncMock(
            return_value={**user_a, "default_project_id": None}
        )
        fake_db.create_job = AsyncMock(
            return_value={
                "id": "aaaaaaaa-1111-1111-1111-111111111111",
                "status": "created",
                "user_id": str(user_a["id"]),
                "project_id": None,
                "parent_job_id": None,
            }
        )
        body = JobCreate.model_validate(
            {
                "description": "forged lifecycle job",
                "config_name": "critic",
                "user_id": "22222222-2222-2222-2222-222222222222",
                "parent_job_id": "bbbbbbbb-2222-2222-2222-222222222222",
                "creation_order": 0,
                "worktree_path": "/tmp/forged",
                "delegation_context": "forged",
                "runner_kind": "lifecycle",
                "context": {
                    "verification_target": "target",
                    "runner_kind": "lifecycle",
                    "workspace_container": {"status": "ready"},
                    "instructions": "keep me",
                },
                "config_override": {
                    "runner_kind": "lifecycle",
                    "parent_job_id": "target",
                    "autonomy": "full",
                },
            }
        )
        with (
            patch.object(access_module, "_INTERNAL_KEY", "secret"),
            _patch_caller_and_db(user_a, fake_db),
            patch(
                "orchestrator.application.access.enforce_readiness_gate",
                AsyncMock(return_value=None),
            ),
            # Orthogonal gate: this test is about marker stripping, and the
            # submit-time capability PEP would 422 on the `autonomy: full`
            # override for a mocked user with no grant rows.
            patch(
                "orchestrator.services.grant_enforcement.enforce_job_create_grants",
                AsyncMock(return_value=None),
            ),
            patch(
                "orchestrator.services.job_provisioning.provision_job_repo", AsyncMock()
            ),
        ):
            await create_job(fake_request, body)

        kwargs = fake_db.create_job.await_args.kwargs
        assert kwargs["user_id"] == str(user_a["id"])
        assert kwargs["parent_job_id"] is None
        assert kwargs["creation_order"] is None
        assert kwargs["worktree_path"] is None
        assert kwargs["delegation_context"] is None
        assert kwargs["context"] == {
            "instructions": "keep me",
            # Server-stamped provenance, not caller-supplied: this body names
            # a bundled expert (via the deprecated config_name alias), and an
            # explicit selection is recorded in the same field the DB-expert
            # path already writes. See tests/test_unified_expert_selection.py.
            "expert_selection": {"source": "bundled", "expert": "critic"},
        }
        assert kwargs["config_override"] == {"autonomy": "full"}


@pytest.mark.asyncio
async def test_job_revalidation_scopes_legacy_policy_read_to_same_job(
    job_a, user_a, fake_db
):
    import orchestrator.main

    datasource_id = "99999999-9999-4999-8999-999999999999"
    fake_db.list_job_datasource_ids = AsyncMock(return_value=[datasource_id])
    fake_db.get_user = AsyncMock(return_value=user_a)
    authorize = AsyncMock(return_value=([datasource_id], {datasource_id: 1}))
    job_a = {
        **job_a,
        "context": {
            "datasource_selection": {"datasource_ids": [datasource_id]},
        },
    }

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch(
            "orchestrator.services.thread_datasource_authorization.authorize_thread_datasource_selection",
            authorize,
        ),
    ):
        (
            selected,
            revisions,
        ) = await job_datasource_selection_module.revalidate_job_datasource_selection(
            job_a,
            dependencies=preparation_composition.job_datasource_selection_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert selected == [datasource_id]
    assert revisions == {datasource_id: 1}
    assert authorize.await_args.kwargs["legacy_job_id"] == str(job_a["id"])


@pytest.mark.asyncio
async def test_job_revalidation_rejects_connector_deleted_from_live_junction(
    job_a, user_a, fake_db
):
    """The immutable context snapshot survives datasource FK cascade deletion."""
    import orchestrator.main

    datasource_id = "99999999-9999-4999-8999-999999999999"
    fake_db.list_job_datasource_ids = AsyncMock(return_value=[])
    fake_db.get_user = AsyncMock(return_value=user_a)
    authorize = AsyncMock()
    job_a = {
        **job_a,
        "context": {
            "datasource_selection": {"datasource_ids": [datasource_id]},
        },
    }

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch(
            "orchestrator.services.thread_datasource_authorization.authorize_thread_datasource_selection",
            authorize,
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await job_datasource_selection_module.revalidate_job_datasource_selection(
                job_a,
                dependencies=preparation_composition.job_datasource_selection_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )

    assert exc.value.status_code == 403
    assert exc.value.detail == "One or more selected connectors are unavailable"
    authorize.assert_not_awaited()
