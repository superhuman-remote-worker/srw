"""Tests for ``services.provision_or_assign`` lifecycle emission.

This is the create-thread binding path. It must emit the same
``session.lifecycle`` sequence (``provisioning`` → ``booting`` →
``ready``) that ``routers/sessions._do_prepare`` does, so the cockpit's
startup card renders live counters regardless of which path bound the
agent. See the warm-pool regression on dev cluster thread ``68acde8d``
(2026-05-23) and the unified-signal-source plan.
"""

from __future__ import annotations

import sys
import types
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from orchestrator.services.provision_or_assign import (
    ProvisionOrAssignDependencies,
)
from shared.pinned_session_identity import PinnedSessionBinding


THREAD_ID = "11111111-1111-4111-8111-111111111111"
RUNTIME_GENERATION = "22222222-2222-4222-8222-222222222222"
AGENT_ID = "33333333-3333-4333-8333-333333333333"
ATTACH_TOKEN = "44444444-4444-4444-8444-444444444444"
POD_UID = "55555555-5555-4555-8555-555555555555"


def _thread_row(
    *,
    lane: str = "pinned",
    status: str = "created",
    agent_id: str | None = None,
    metadata: dict | None = None,
) -> dict:
    return {
        "id": THREAD_ID,
        "execution_lane": lane,
        "status": status,
        "agent_id": agent_id,
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_attach_token": ATTACH_TOKEN if agent_id else None,
        "runtime_retirement_token": None,
        "metadata": metadata or {},
    }


def _binding(
    *,
    status: str = "session",
    hostname: str = "srw-agent-pool-1",
    pod_uid: str = POD_UID,
    pod_ip: str = "10.0.0.5",
    pod_port: int = 8001,
) -> PinnedSessionBinding:
    return PinnedSessionBinding(
        thread_id=THREAD_ID,
        runtime_generation=RUNTIME_GENERATION,
        agent_id=AGENT_ID,
        runtime_attach_token=ATTACH_TOKEN,
        agent_hostname=hostname,
        pod_namespace="srw",
        pod_uid=pod_uid,
        pod_ip=pod_ip,
        pod_port=pod_port,
        agent_status=status,
    )


def _sequence_then_repeat(*rows):
    iterator = iter(rows)
    last = rows[-1]

    def _next(*_args, **_kwargs):
        nonlocal last
        try:
            last = next(iterator)
        except StopIteration:
            pass
        return last

    return _next


class _Ports:
    """Mutable stand-ins for ``provision_or_assign``'s explicit ports.

    R1.B06 replaced this path's late ``from orchestrator.main import ...`` with
    a constructed ``ProvisionOrAssignDependencies``. Tests reassign a
    collaborator after construction (the grant-denied case swaps in its own
    violation source, say), so ``dependencies`` forwards through this holder at
    call time instead of capturing the callables when it is built.
    """

    def __init__(self) -> None:
        self.store = MagicMock()
        self.store.get_thread = AsyncMock(return_value=_thread_row())
        self.store.resolve_datasources_for_thread = AsyncMock(return_value=[])
        self.store.get_agent = AsyncMock(
            return_value={"id": AGENT_ID, "pod_ip": "10.0.0.5", "pod_port": 8001}
        )
        self.store.get_pinned_session_binding = AsyncMock(return_value=_binding())
        lock_cm = AsyncMock()
        lock_cm.__aenter__.return_value = None
        lock_cm.__aexit__.return_value = False
        self.store.thread_advisory_lock = MagicMock(return_value=lock_cm)

        self.agent_provisioner = MagicMock()
        self.agent_provisioner.provision_agent = AsyncMock(
            return_value="srw-agent-s-new"
        )

        async def _no_idle():
            return None

        self.find_idle_persistent_agent = _no_idle

        async def _attach(*args, **kwargs):
            return True

        self.send_session_attach = _attach

        # Pre-flight checks default to "no violations" so the happy-path cases
        # proceed; the denial cases override them.
        self.session_grant_violations = AsyncMock(return_value=[])
        self.session_endpoint_violations = AsyncMock(return_value=[])
        self.await_protected_cloud_runtime_ready = AsyncMock(return_value=True)

    @property
    def dependencies(self) -> ProvisionOrAssignDependencies:
        holder = self
        return ProvisionOrAssignDependencies(
            store=holder.store,
            agent_provisioner=holder.agent_provisioner,
            await_protected_cloud_runtime_ready=(
                lambda *a, **k: holder.await_protected_cloud_runtime_ready(*a, **k)
            ),
            session_grant_violations=(
                lambda *a, **k: holder.session_grant_violations(*a, **k)
            ),
            session_endpoint_violations=(
                lambda *a, **k: holder.session_endpoint_violations(*a, **k)
            ),
            find_idle_persistent_agent=(
                lambda *a, **k: holder.find_idle_persistent_agent(*a, **k)
            ),
            send_session_attach=(lambda *a, **k: holder.send_session_attach(*a, **k)),
        )


def _ports(monkeypatch, **overrides) -> _Ports:
    """Build this path's ports without standing up the composition root."""
    ports = _Ports()
    for k, v in overrides.items():
        setattr(ports, k, v)
    return ports


def _install_fake_lifecycle_module(monkeypatch, emit_calls: list[dict]):
    """Stub ``services.session_lifecycle`` so the function-under-test's late
    import resolves to our capture helpers."""
    stub = types.ModuleType("orchestrator.services.session_lifecycle")

    def _capture_emit(user_id, thread_id, state, **extra):
        emit_calls.append(
            {"user_id": user_id, "thread_id": thread_id, "state": state, **extra}
        )

    async def _bound(*a, **k):
        return True

    stub.emit = _capture_emit
    stub.wait_for_ready = AsyncMock(return_value=True)
    stub.wait_for_binding = _bound
    monkeypatch.setitem(sys.modules, "orchestrator.services.session_lifecycle", stub)
    return stub


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delayed", [False, True], ids=["already-held", "changed-during-await"]
)
async def test_old_initial_vm_marker_holds_common_binding(monkeypatch, delayed):
    old_vm = {
        "status": "waiting_capacity",
        "initial_runtime": {
            "runtime_generation": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        },
        "creation_request_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    }
    held = _thread_row(metadata={"vm": old_vm})
    ports = _ports(monkeypatch)
    ports.store.get_thread = AsyncMock(
        side_effect=_sequence_then_repeat(_thread_row(), held) if delayed else None,
        return_value=held,
    )
    ports.find_idle_persistent_agent = AsyncMock(return_value=None)
    ports.send_session_attach = AsyncMock()
    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        "u1",
        THREAD_ID,
        "session_base",
        {"workspace": {"backend": "vm"}},
        [],
        None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    assert emit_calls == []
    ports.find_idle_persistent_agent.assert_not_awaited()
    ports.send_session_attach.assert_not_awaited()
    ports.agent_provisioner.provision_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_path_refetch_treats_stateless_as_ready_without_lifecycle_error(
    monkeypatch,
):
    """A legitimate lane change neither provisions nor emits a false failure."""
    ports = _ports(monkeypatch)
    ports.store.get_thread = AsyncMock(return_value=_thread_row(lane="stateless"))
    ports.find_idle_persistent_agent = AsyncMock()
    ports.send_session_attach = AsyncMock()
    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        "u1",
        THREAD_ID,
        "session_base",
        {},
        [],
        None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    assert emit_calls == []
    ports.session_grant_violations.assert_not_awaited()
    ports.session_endpoint_violations.assert_not_awaited()
    ports.find_idle_persistent_agent.assert_not_awaited()
    ports.send_session_attach.assert_not_awaited()
    ports.agent_provisioner.provision_agent.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "thread_row",
    [
        None,
        _thread_row(lane="future-lane"),
    ],
)
async def test_create_path_refetch_fails_closed_for_missing_or_unknown_lane(
    monkeypatch, thread_row
):
    ports = _ports(monkeypatch)
    ports.store.get_thread = AsyncMock(return_value=thread_row)
    ports.find_idle_persistent_agent = AsyncMock()
    ports.send_session_attach = AsyncMock()
    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        "u1",
        THREAD_ID,
        "session_base",
        {},
        [],
        None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    if thread_row is None:
        # Unknown/missing lifecycle is not an authority for a new SSE frame.
        assert emit_calls == []
    else:
        assert [call["state"] for call in emit_calls] == ["failed"]
        assert "pinned provisioning" in emit_calls[0]["reason"]
    ports.session_grant_violations.assert_not_awaited()
    ports.session_endpoint_violations.assert_not_awaited()
    ports.find_idle_persistent_agent.assert_not_awaited()
    ports.send_session_attach.assert_not_awaited()
    ports.agent_provisioner.provision_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_pool_reservation_refetches_lane_before_pod_fallback(monkeypatch):
    idle_agent = {
        "id": AGENT_ID,
        "hostname": "srw-agent-pool-1",
        "pod_ip": "10.0.0.5",
        "pod_port": 8001,
    }
    ports = _ports(monkeypatch)
    ports.store.get_thread = AsyncMock(
        side_effect=_sequence_then_repeat(
            _thread_row(),
            _thread_row(),
            _thread_row(),
            _thread_row(),
            _thread_row(),
            _thread_row(lane="stateless"),
        )
    )
    ports.find_idle_persistent_agent = AsyncMock(return_value=idle_agent)
    ports.send_session_attach = AsyncMock(return_value=False)
    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        "u1",
        THREAD_ID,
        "session_base",
        {},
        [],
        None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    assert [call["state"] for call in emit_calls] == ["provisioning"]
    ports.send_session_attach.assert_awaited_once()
    ports.agent_provisioner.provision_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_idle_pool_attach_emits_provisioning_booting_ready(monkeypatch):
    """Warm pool fast-path: agent attaches instantly, /ready flips fast."""
    idle_agent = {
        "id": AGENT_ID,
        "hostname": "srw-agent-pool-1",
        "pod_ip": "10.0.0.5",
        "pod_port": 8001,
    }

    async def _find_idle():
        return idle_agent

    ports = _ports(monkeypatch, _find_idle_persistent_agent=_find_idle)
    # First get_thread inside the lock — no prior binding.
    ports.store.get_thread = AsyncMock(return_value=_thread_row())

    emit_calls: list[dict] = []
    lifecycle = _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        uid="u1",
        tid=THREAD_ID,
        cfg="persistent_defaults",
        co={},
        pids=[],
        ds_ids=None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    states = [c["state"] for c in emit_calls]
    assert states == ["provisioning", "booting", "ready"], (
        f"expected provisioning→booting→ready, got {states}"
    )
    assert all(c["user_id"] == "u1" and c["thread_id"] == THREAD_ID for c in emit_calls)
    lifecycle.wait_for_ready.assert_awaited_once_with(
        "10.0.0.5",
        8001,
        180,
        require_protected_cloud=False,
        expected_session_identity_fingerprint=_binding().session_identity_fingerprint,
    )


@pytest.mark.parametrize(
    "changed_binding",
    [
        replace(_binding(), agent_hostname="successor-agent"),
        replace(_binding(), pod_uid="successor-pod-uid"),
        replace(_binding(), pod_ip="10.0.0.99"),
        replace(_binding(), pod_port=9001),
        replace(_binding(), agent_id="66666666-6666-4666-8666-666666666666"),
        replace(
            _binding(),
            runtime_attach_token="77777777-7777-4777-8777-777777777777",
        ),
    ],
    ids=["hostname", "pod_uid", "pod_ip", "pod_port", "agent_id", "attach"],
)
@pytest.mark.asyncio
async def test_create_path_never_emits_ready_for_a_changed_binding(
    monkeypatch,
    changed_binding,
):
    """A readiness result for physical target A cannot label B ready."""

    idle_agent = {
        "id": AGENT_ID,
        "hostname": "srw-agent-pool-1",
        "pod_ip": "10.0.0.5",
        "pod_port": 8001,
    }
    ports = _ports(
        monkeypatch,
        _find_idle_persistent_agent=AsyncMock(return_value=idle_agent),
    )
    ports.store.get_thread = AsyncMock(return_value=_thread_row())
    original = _binding()
    ports.store.get_pinned_session_binding.side_effect = [
        original,
        changed_binding,
    ]
    emit_calls: list[dict] = []
    lifecycle = _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        uid="u1",
        tid=THREAD_ID,
        cfg="persistent_defaults",
        co={},
        pids=[],
        ds_ids=None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    assert [call["state"] for call in emit_calls] == ["provisioning", "booting"]
    lifecycle.wait_for_ready.assert_awaited_once_with(
        original.pod_ip,
        original.pod_port,
        180,
        require_protected_cloud=False,
        expected_session_identity_fingerprint=(original.session_identity_fingerprint),
    )


@pytest.mark.asyncio
async def test_create_path_allows_booting_status_lag_after_exact_ready_probe(
    monkeypatch,
):
    """The DB heartbeat state may lag the exact identity-bound /ready result."""

    idle_agent = {
        "id": AGENT_ID,
        "hostname": "srw-agent-pool-1",
        "pod_ip": "10.0.0.5",
        "pod_port": 8001,
    }
    ports = _ports(
        monkeypatch,
        _find_idle_persistent_agent=AsyncMock(return_value=idle_agent),
    )
    ports.store.get_thread = AsyncMock(return_value=_thread_row())
    original = _binding(status="ready")
    ports.store.get_pinned_session_binding.side_effect = [
        original,
        replace(original, agent_status="booting"),
    ]
    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        uid="u1",
        tid=THREAD_ID,
        cfg="persistent_defaults",
        co={},
        pids=[],
        ds_ids=None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    assert [call["state"] for call in emit_calls] == [
        "provisioning",
        "booting",
        "ready",
    ]


@pytest.mark.asyncio
async def test_create_path_rejects_offline_status_after_exact_ready_probe(monkeypatch):
    """The final joined status gate may not publish an offline target ready."""

    idle_agent = {
        "id": AGENT_ID,
        "hostname": "srw-agent-pool-1",
        "pod_ip": "10.0.0.5",
        "pod_port": 8001,
    }
    ports = _ports(
        monkeypatch,
        _find_idle_persistent_agent=AsyncMock(return_value=idle_agent),
    )
    ports.store.get_thread = AsyncMock(return_value=_thread_row())
    original = _binding(status="ready")
    ports.store.get_pinned_session_binding.side_effect = [
        original,
        replace(original, agent_status="offline"),
    ]
    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        uid="u1",
        tid=THREAD_ID,
        cfg="persistent_defaults",
        co={},
        pids=[],
        ds_ids=None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    assert [call["state"] for call in emit_calls] == ["provisioning", "booting"]


@pytest.mark.asyncio
async def test_fresh_pod_path_emits_full_sequence(monkeypatch):
    """No idle agent — provision a fresh pod, wait for binding, emit phases."""
    ports = _ports(monkeypatch)
    # No idle agent (default already None). After fresh-pod, the second
    # get_thread sees the binding.
    ports.store.get_thread = AsyncMock(
        side_effect=_sequence_then_repeat(
            _thread_row(),
            _thread_row(),
            _thread_row(),
            _thread_row(),
            _thread_row(),
            _thread_row(),
            _thread_row(agent_id=AGENT_ID),
        )
    )
    ports.store.get_pinned_session_binding = AsyncMock(
        return_value=_binding(pod_ip="10.0.0.9")
    )

    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        uid="u1",
        tid=THREAD_ID,
        cfg="persistent_defaults",
        co={},
        pids=[],
        ds_ids=None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    states = [c["state"] for c in emit_calls]
    assert states == ["provisioning", "booting", "ready"]


@pytest.mark.asyncio
async def test_fresh_pod_path_waits_when_agent_pod_marker_in_flight(monkeypatch):
    """A sibling prepare/create path may already have created the pod but not
    yet received the agent registration. Do not create a duplicate pod."""
    ports = _ports(monkeypatch)
    marker = {
        "status": "created",
        "pod_name": "srw-agent-s-existing",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    ports.store.get_thread = AsyncMock(
        side_effect=_sequence_then_repeat(
            _thread_row(metadata={"agent_pod": marker}),
            _thread_row(metadata={"agent_pod": marker}),
            _thread_row(metadata={"agent_pod": marker}),
            _thread_row(agent_id=AGENT_ID),
        )
    )
    ports.store.get_pinned_session_binding = AsyncMock(
        return_value=_binding(pod_ip="10.0.0.9")
    )
    ports.find_idle_persistent_agent = AsyncMock(return_value=None)

    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        uid="u1",
        tid=THREAD_ID,
        cfg="persistent_defaults",
        co={},
        pids=[],
        ds_ids=None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    ports.find_idle_persistent_agent.assert_not_awaited()
    ports.agent_provisioner.provision_agent.assert_not_awaited()
    states = [c["state"] for c in emit_calls]
    assert states == ["provisioning", "booting", "ready"]


@pytest.mark.asyncio
async def test_no_idle_and_provision_fails_emits_failed(monkeypatch):
    """No idle pool agent, fresh-pod creation also fails — emit ``failed``."""
    ports = _ports(monkeypatch)
    ports.agent_provisioner.provision_agent = AsyncMock(return_value=None)

    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        uid="u1",
        tid=THREAD_ID,
        cfg="persistent_defaults",
        co={},
        pids=[],
        ds_ids=None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    states = [c["state"] for c in emit_calls]
    assert states[0] == "provisioning"
    assert "failed" in states
    failed_emit = next(c for c in emit_calls if c["state"] == "failed")
    assert "reason" in failed_emit and failed_emit["reason"]


@pytest.mark.asyncio
async def test_grant_denied_fails_fast_without_pool_or_pod(monkeypatch):
    """A session whose resolved config exceeds the user's capability grants must
    fail fast: emit provisioning→failed carrying the violation, and attach NO
    pool agent / spawn NO dedicated pod. Otherwise a doomed pod boots, 403s at
    the workspace endpoint, exits, and the cockpit polls /connection until its
    ~5m40s ready timeout.
    knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md
    """
    ports = _ports(monkeypatch)
    ports.store.get_thread = AsyncMock(return_value=_thread_row())
    ports.session_grant_violations = AsyncMock(
        return_value=["permission_mode: 'autonomous' exceeds the ceiling"]
    )
    # Spy that neither provisioning path is taken.
    ports.find_idle_persistent_agent = AsyncMock(return_value=None)

    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        uid="u1",
        tid=THREAD_ID,
        cfg="persistent_defaults",
        co={},
        pids=[],
        ds_ids=None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    states = [c["state"] for c in emit_calls]
    assert states == ["provisioning", "failed"], states
    failed = next(c for c in emit_calls if c["state"] == "failed")
    assert "capability grants" in failed["reason"]
    assert "autonomous" in failed["reason"]
    ports.find_idle_persistent_agent.assert_not_awaited()
    ports.agent_provisioner.provision_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_endpoint_denied_fails_fast_without_pool_or_pod(monkeypatch):
    """A session whose resolved config has an unusable model transport (e.g. the
    memory reranker riding an unreachable embedding endpoint) must fail fast:
    emit provisioning→failed with the real reason, spawn NO pod. Otherwise the
    agent crashes at startup, the workspace is released, and the cockpit hangs
    on /connection.
    knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md
    """
    ports = _ports(monkeypatch)
    ports.store.get_thread = AsyncMock(return_value=_thread_row())
    ports.session_endpoint_violations = AsyncMock(
        return_value=[
            "embedding model 'qwen3-embedding-8b' (local) resolved but no "
            "EMBEDDING_BASE_URL — memory, KB, and the reranker cannot reach the "
            "embedding endpoint"
        ]
    )
    ports.find_idle_persistent_agent = AsyncMock(return_value=None)

    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        uid="u1",
        tid=THREAD_ID,
        cfg="persistent_defaults",
        co={},
        pids=[],
        ds_ids=None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    states = [c["state"] for c in emit_calls]
    assert states == ["provisioning", "failed"], states
    failed = next(c for c in emit_calls if c["state"] == "failed")
    assert "unusable model transport" in failed["reason"]
    assert "reranker" in failed["reason"]
    ports.find_idle_persistent_agent.assert_not_awaited()
    ports.agent_provisioner.provision_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_grant_ok_but_endpoint_check_runs(monkeypatch):
    """The endpoint pre-flight runs even when grants pass (it's a second gate)."""
    ports = _ports(monkeypatch)
    ports.store.get_thread = AsyncMock(return_value=_thread_row())
    ports.find_idle_persistent_agent = AsyncMock(return_value=None)

    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        uid="u1",
        tid=THREAD_ID,
        cfg="persistent_defaults",
        co={},
        pids=[],
        ds_ids=None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    ports.session_endpoint_violations.assert_awaited_once()


@pytest.mark.asyncio
async def test_already_bound_exits_without_emitting_booting_or_ready(monkeypatch):
    """Race with /prepare or /resume: the other path owns the rest of the
    lifecycle, so this path emits only the up-front ``provisioning`` and
    returns without ``booting``/``ready`` duplicates."""
    ports = _ports(monkeypatch)
    # Already bound — duplicate-provision guard fires.
    ports.store.get_thread = AsyncMock(return_value=_thread_row(agent_id=AGENT_ID))

    emit_calls: list[dict] = []
    _install_fake_lifecycle_module(monkeypatch, emit_calls)

    from orchestrator.services.provision_or_assign import provision_or_assign

    await provision_or_assign(
        uid="u1",
        tid=THREAD_ID,
        cfg="persistent_defaults",
        co={},
        pids=[],
        ds_ids=None,
        runtime_generation=RUNTIME_GENERATION,
        dependencies=ports.dependencies,
    )

    states = [c["state"] for c in emit_calls]
    assert states == ["provisioning"]
