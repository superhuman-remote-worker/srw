"""R1.B11 lane A: the job dispatcher owner module, driven by explicit fakes.

Every collaborator arrives through ``JobDispatchDependencies``; nothing here
imports ``orchestrator.main``. The dispatcher swallows exceptions into a
``Dispatcher error`` log, so each pass is checked for that record — a fake that
raised an assertion would otherwise pass silently.
"""

from __future__ import annotations

import ast
import asyncio
import dataclasses
import functools
import inspect
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.database.dispatch_discovery import discovery_order
from orchestrator.security.access import vm_workspaces_on_pod_network
from orchestrator.services import job_dispatcher, job_workspace_authority
from orchestrator.services.job_dispatcher import (
    JobDispatchDependencies,
    JobDispatchState,
    auto_assign_dispatcher,
    dispatch_pending_jobs as schedule_pending_jobs,
    trigger_dispatch,
)
from orchestrator.services.model_availability import ModelUnavailable, UnavailableModel

GUARD_KWARGS = {"completion_generation_guard": "fenced"}
LOGGER_NAME = "orchestrator.services.job_dispatcher"


def _job(job_id: str, **overrides: Any) -> dict[str, Any]:
    """A job whose workspace contract resolves 'ready' with no remote tier."""

    job = {
        "id": job_id,
        "status": "created",
        "execution_lane": "pinned",
        "assigned_agent_id": None,
        "priority": 5,
        "user_id": None,
        "parent_job_id": None,
        "config_override": {"workspace": {"backend": "none"}},
        "context": {},
    }
    job.update(overrides)
    return job


def _stateless_job(job_id: str, **overrides: Any) -> dict[str, Any]:
    return _job(
        job_id,
        execution_lane="stateless",
        context={
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.8",
            }
        },
        **overrides,
    )


class FakeStore:
    """The store surface one dispatch pass touches, recording every call."""

    def __init__(
        self,
        *,
        pinned: list[dict] | None = None,
        stateless: list[dict] | None = None,
        agents: list[dict] | None = None,
        claims: dict[str, bool] | None = None,
        checkpoints: dict[str, bool] | None = None,
        candidates: list[dict] | None = None,
        admissions: dict[str, tuple[bool, Any]] | None = None,
        jobs: dict[str, dict] | None = None,
    ) -> None:
        self.pinned = pinned or []
        self.stateless = stateless or []
        self.agents = agents or []
        self.claims = claims or {}
        self.checkpoints = checkpoints or {}
        self.candidates = candidates or []
        self.admissions = admissions or {}
        self.jobs = {str(job["id"]): job for job in self.pinned + self.stateless}
        self.jobs.update(jobs or {})
        self.calls: list[tuple[str, tuple, dict]] = []

    def _record(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append((name, args, kwargs))

    def called(self, name: str) -> list[tuple[tuple, dict]]:
        return [(a, k) for n, a, k in self.calls if n == name]

    async def get_job_discovery_cutoff(self):
        return datetime.now(timezone.utc)

    def _page(self, jobs, *, limit, discovery_after=None, discovery_cutoff=None, **_):
        rows = sorted(jobs, key=discovery_order)
        if discovery_cutoff is not None:
            rows = [
                row
                for row in rows
                if row.get("created_at") is None
                or row["created_at"] <= discovery_cutoff
            ]
        if discovery_after is not None:
            key = discovery_order(
                {
                    "id": discovery_after.job_id,
                    "priority": discovery_after.priority,
                    "created_at": discovery_after.created_at,
                }
            )
            rows = [row for row in rows if discovery_order(row) > key]
        return rows[:limit]

    async def get_dispatchable_jobs(self, **kwargs):
        self._record("get_dispatchable_jobs", **kwargs)
        return self._page(self.pinned, **kwargs)

    async def get_admittable_stateless_jobs(self, **kwargs):
        self._record("get_admittable_stateless_jobs", **kwargs)
        return self._page(self.stateless, **kwargs)

    async def update_job_status(self, *args, **kwargs):
        self._record("update_job_status", *args, **kwargs)
        return True

    async def get_available_agents(self, **kwargs):
        self._record("get_available_agents", **kwargs)
        return list(self.agents)

    async def claim_job_for_agent(self, job_id, agent_id, **kwargs):
        self._record("claim_job_for_agent", job_id, agent_id, **kwargs)
        return self.claims.get(job_id, True)

    async def job_has_checkpoint(self, job_id):
        self._record("job_has_checkpoint", job_id)
        return self.checkpoints.get(job_id, False)

    async def get_preemption_candidates(self):
        self._record("get_preemption_candidates")
        return list(self.candidates)

    async def get_job(self, job_id):
        self._record("get_job", job_id)
        return self.jobs.get(job_id)

    async def admit_stateless_worker_job(self, job_id, **kwargs):
        self._record("admit_stateless_worker_job", job_id, **kwargs)
        return self.admissions.get(job_id, (True, "inserted"))


class FakeDelivery:
    def __init__(self) -> None:
        self.resume = AsyncMock(return_value=True)
        self.dispatch = AsyncMock(return_value=True)
        self.initiate_pause = AsyncMock(return_value=None)


def _deps(
    store: FakeStore,
    *,
    state: JobDispatchState | None = None,
    delivery: FakeDelivery | None = None,
    auto_assign_enabled: bool = True,
    stateless_worker_enabled: bool = False,
    agent_provisioner: Any = None,
    manifest_service: Any = None,
    prepare_repository: Any = None,
    bind_gate: Any = None,
    mint_gate: Any = None,
    unavailable_models: Any = None,
    fail_subjob: Any = None,
) -> JobDispatchDependencies:
    delivery = delivery or FakeDelivery()

    async def prepare_job_workspace_runtime(job):
        return ("proceed", job, None)

    async def unexpected(*args, **kwargs):
        raise AssertionError(f"unexpected collaborator call {args} {kwargs}")

    return JobDispatchDependencies(
        state=state or JobDispatchState(),
        store=store,
        completion_control_boundary=SimpleNamespace(
            dispatch_guard_kwargs=lambda: dict(GUARD_KWARGS)
        ),
        agent_provisioner=agent_provisioner
        or SimpleNamespace(_k8s_available=False, is_available=False),
        vm_provisioner=SimpleNamespace(mode="external", is_available=False),
        container_provisioner=SimpleNamespace(is_available=True, in_cluster=True),
        docker_provisioner=SimpleNamespace(is_available=False),
        workspace_suspension=object(),
        auto_assign_enabled=auto_assign_enabled,
        stateless_worker_enabled=stateless_worker_enabled,
        manifest_execution_service=manifest_service
        or MagicMock(side_effect=AssertionError("manifest reconcile not gated")),
        prepare_job_workspace_runtime=prepare_job_workspace_runtime,
        fail_subjob_and_unblock_parent=fail_subjob or unexpected,
        check_vm_permission=unexpected,
        fail_vm_parked_job=unexpected,
        job_needs_sandbox=lambda job: False,
        provision_parent_workspace_for_scholar=unexpected,
        prepare_job_repository_before_claim=prepare_repository
        or AsyncMock(return_value=True),
        job_delivery_operations=lambda: delivery,
        **({"job_bind_gate": bind_gate} if bind_gate is not None else {}),
        **({"job_mint_gate": mint_gate} if mint_gate is not None else {}),
        **(
            {"unavailable_job_models": unavailable_models}
            if unavailable_models is not None
            else {}
        ),
    )


@pytest.fixture(autouse=True)
def _deterministic_env(monkeypatch):
    # Stale-SHA filtering and the VM pod-network probe both read env.
    for var in ("AGENT_IMAGE", "PERSISTENT_AGENT_IMAGE", "VM_MODE"):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def no_dispatcher_error(caplog):
    caplog.set_level(logging.DEBUG, logger=LOGGER_NAME)
    yield caplog
    errors = [
        r.getMessage()
        for r in caplog.records
        if r.name == LOGGER_NAME and r.getMessage().startswith("Dispatcher error")
    ]
    assert errors == []


async def _drain_tasks() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def dispatch_pending_jobs(*, dependencies):
    """Existing decision tests await the explicitly owned preflight work."""
    await schedule_pending_jobs(dependencies=dependencies)
    while dependencies.state.tasks:
        await asyncio.wait_for(
            asyncio.gather(*list(dependencies.state.tasks)), timeout=2
        )
        await asyncio.sleep(0)


# =============================================================================
# Module shape
# =============================================================================


class TestModuleShape:
    def test_does_not_import_the_application_module(self):
        tree = ast.parse(inspect.getsource(job_dispatcher))
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")
            elif isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
        assert not {m for m in imported if m.startswith("orchestrator.main")}
        assert "orchestrator" not in imported

    def test_state_is_per_application_and_dependencies_are_frozen(self):
        first, second = JobDispatchState(), JobDispatchState()
        assert first.lock is not second.lock
        assert first.pause_pending_job_ids is not second.pause_pending_job_ids
        deps = _deps(FakeStore())
        with pytest.raises(dataclasses.FrozenInstanceError):
            deps.store = FakeStore()  # type: ignore[misc]


# =============================================================================
# Early gates
# =============================================================================


class TestEarlyGates:
    @pytest.mark.asyncio
    async def test_reconciles_manifests_when_ready_and_k8s_available(
        self, no_dispatcher_error
    ):
        store = FakeStore()
        store.manifests_ready = True
        service = SimpleNamespace(reconcile=AsyncMock())
        provider = MagicMock(return_value=service)
        deps = _deps(
            store,
            auto_assign_enabled=False,
            stateless_worker_enabled=False,
            agent_provisioner=SimpleNamespace(_k8s_available=True, is_available=False),
            manifest_service=provider,
        )

        await dispatch_pending_jobs(dependencies=deps)

        provider.assert_called_once_with()
        service.reconcile.assert_awaited_once_with()
        # Both lanes disabled: the reconcile still ran, but nothing was scanned.
        assert store.calls == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("manifests_ready", "k8s_available"),
        [
            (None, True),  # attribute absent
            (1, True),  # truthy is not enough: the gate is ``is True``
            (True, False),
        ],
    )
    async def test_skips_reconcile_unless_both_gates_hold(
        self, manifests_ready, k8s_available, no_dispatcher_error
    ):
        store = FakeStore()
        if manifests_ready is not None:
            store.manifests_ready = manifests_ready
        provider = MagicMock(side_effect=AssertionError("reconcile not gated"))
        deps = _deps(
            store,
            auto_assign_enabled=False,
            agent_provisioner=SimpleNamespace(
                _k8s_available=k8s_available, is_available=False
            ),
            manifest_service=provider,
        )

        await dispatch_pending_jobs(dependencies=deps)

        provider.assert_not_called()

    @pytest.mark.asyncio
    async def test_both_lanes_disabled_returns_without_taking_the_lock(self):
        store = FakeStore(pinned=[_job("j1")])
        deps = _deps(store, auto_assign_enabled=False, stateless_worker_enabled=False)

        async with deps.state.lock:
            # Would deadlock if the early return came after the lock.
            await asyncio.wait_for(dispatch_pending_jobs(dependencies=deps), 1.0)

        assert store.calls == []

    @pytest.mark.asyncio
    async def test_each_lane_scans_only_its_own_source(self, no_dispatcher_error):
        pinned_only = FakeStore()
        await dispatch_pending_jobs(
            dependencies=_deps(pinned_only, auto_assign_enabled=True)
        )
        assert [n for n, *_ in pinned_only.calls] == ["get_dispatchable_jobs"] * 2
        pages = pinned_only.called("get_dispatchable_jobs")
        assert [kwargs["limit"] for _, kwargs in pages] == [10, 40]
        assert all(
            kwargs["completion_generation_guard"] == "fenced" for _, kwargs in pages
        )

        stateless_only = FakeStore()
        await dispatch_pending_jobs(
            dependencies=_deps(
                stateless_only,
                auto_assign_enabled=False,
                stateless_worker_enabled=True,
            )
        )
        assert [n for n, *_ in stateless_only.calls] == [
            "get_admittable_stateless_jobs"
        ] * 2
        pages = stateless_only.called("get_admittable_stateless_jobs")
        assert [kwargs["limit"] for _, kwargs in pages] == [10, 40]
        assert all(
            kwargs["completion_generation_guard"] == "fenced" for _, kwargs in pages
        )


# =============================================================================
# Serialization
# =============================================================================


class TestLockSerialization:
    @pytest.mark.asyncio
    async def test_concurrent_passes_never_overlap_inside_the_lock(
        self, no_dispatcher_error
    ):
        active = 0
        peak = 0
        entered = 0

        class SlowStore(FakeStore):
            async def get_dispatchable_jobs(self, **kwargs):
                nonlocal active, peak, entered
                entered += 1
                active += 1
                peak = max(peak, active)
                for _ in range(10):
                    await asyncio.sleep(0)
                active -= 1
                return []

        deps = _deps(SlowStore())
        await asyncio.gather(
            dispatch_pending_jobs(dependencies=deps),
            dispatch_pending_jobs(dependencies=deps),
        )

        assert entered == 4
        assert peak == 1

    @pytest.mark.asyncio
    async def test_pass_waits_on_the_application_owned_lock(self):
        store = FakeStore()
        deps = _deps(store)

        await deps.state.lock.acquire()
        task = asyncio.create_task(dispatch_pending_jobs(dependencies=deps))
        await _drain_tasks()
        assert not task.done()
        assert store.calls == []

        deps.state.lock.release()
        await asyncio.wait_for(task, 1.0)
        assert [n for n, *_ in store.calls] == ["get_dispatchable_jobs"] * 2


# =============================================================================
# Claim + delivery lane selection
# =============================================================================


class TestClaimAndDelivery:
    @pytest.mark.asyncio
    async def test_lost_claim_skips_delivery_and_consumes_the_agent(
        self, no_dispatcher_error
    ):
        j1, j2 = _job("j1"), _job("j2")
        a1, a2 = {"id": "a1", "metadata": {}}, {"id": "a2", "metadata": "{}"}
        store = FakeStore(pinned=[j1, j2], agents=[a1, a2], claims={"j1": False})
        delivery = FakeDelivery()

        await dispatch_pending_jobs(dependencies=_deps(store, delivery=delivery))

        assert store.called("claim_job_for_agent") == [
            (("j1", "a1"), GUARD_KWARGS),
            (("j2", "a2"), GUARD_KWARGS),
        ]
        # The loser is not delivered; its checkpoint is not even probed.
        assert store.called("job_has_checkpoint") == [(("j2",), {})]
        delivery.dispatch.assert_awaited_once_with(j2, a2)
        delivery.resume.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_paused_job_with_checkpoint_takes_the_resume_lane(
        self, no_dispatcher_error
    ):
        job = _job("j1", status="paused")
        agent = {"id": "a1", "metadata": None}
        store = FakeStore(pinned=[job], agents=[agent], checkpoints={"j1": True})
        delivery = FakeDelivery()

        await dispatch_pending_jobs(dependencies=_deps(store, delivery=delivery))

        delivery.resume.assert_awaited_once_with(job, agent)
        delivery.dispatch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_paused_job_without_checkpoint_dispatches_fresh(
        self, no_dispatcher_error
    ):
        job = _job("j1", status="paused")
        agent = {"id": "a1", "metadata": {}}
        store = FakeStore(pinned=[job], agents=[agent], checkpoints={"j1": False})
        delivery = FakeDelivery()

        await dispatch_pending_jobs(dependencies=_deps(store, delivery=delivery))

        delivery.dispatch.assert_awaited_once_with(job, agent)
        delivery.resume.assert_not_awaited()
        assert any(
            "is paused with no checkpoint" in r.getMessage()
            for r in no_dispatcher_error.records
        )

    @pytest.mark.asyncio
    async def test_created_job_dispatches_fresh_even_with_a_checkpoint(
        self, no_dispatcher_error
    ):
        job = _job("j1", status="created")
        agent = {"id": "a1", "metadata": {}}
        store = FakeStore(pinned=[job], agents=[agent], checkpoints={"j1": True})
        delivery = FakeDelivery()

        await dispatch_pending_jobs(dependencies=_deps(store, delivery=delivery))

        delivery.dispatch.assert_awaited_once_with(job, agent)
        delivery.resume.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_repository_preflight_failure_holds_the_job_before_claim(
        self, no_dispatcher_error
    ):
        job = _job("j1")
        store = FakeStore(pinned=[job], agents=[{"id": "a1", "metadata": {}}])
        prepare = AsyncMock(return_value=False)

        await dispatch_pending_jobs(
            dependencies=_deps(store, prepare_repository=prepare)
        )

        prepare.assert_awaited_once_with(job)
        assert store.called("claim_job_for_agent") == []
        assert store.called("get_available_agents") == []

    @pytest.mark.asyncio
    async def test_a_bind_still_running_holds_the_job_before_claim(
        self, no_dispatcher_error
    ):
        """A registered driver's connector binds first (D6): the preflight
        starts it without waiting and retries at later ticks."""
        job = _job("j1")
        store = FakeStore(pinned=[job], agents=[{"id": "a1", "metadata": {}}])
        gate = AsyncMock(return_value=("wait", None))

        await dispatch_pending_jobs(dependencies=_deps(store, bind_gate=gate))

        gate.assert_awaited_once_with(job)
        assert store.called("claim_job_for_agent") == []
        assert store.called("update_job_status") == []

    @pytest.mark.asyncio
    async def test_a_mint_still_running_holds_the_job_before_claim(
        self, no_dispatcher_error
    ):
        """A provider-minted credential is minted first (C5): the preflight
        starts it without waiting, and the job is claimed only once its
        delivery can hand it out."""
        job = _job("j1")
        store = FakeStore(pinned=[job], agents=[{"id": "a1", "metadata": {}}])
        gate = AsyncMock(return_value=("wait", None))

        await dispatch_pending_jobs(dependencies=_deps(store, mint_gate=gate))

        gate.assert_awaited_once_with(job)
        assert store.called("claim_job_for_agent") == []
        assert store.called("update_job_status") == []

    @pytest.mark.asyncio
    async def test_a_mint_the_provider_refused_fails_the_job_with_its_reason(
        self, no_dispatcher_error
    ):
        job = _job("j1", status="created")
        store = FakeStore(pinned=[job], agents=[{"id": "a1", "metadata": {}}])
        gate = AsyncMock(
            return_value=("fail", "Connector gh: GitHub refused (HTTP 403)")
        )

        await dispatch_pending_jobs(dependencies=_deps(store, mint_gate=gate))

        assert store.called("claim_job_for_agent") == []
        ((args, kwargs),) = store.called("update_job_status")
        assert args == ("j1",)
        assert kwargs["status"] == "failed"
        assert kwargs["error_message"] == "Connector gh: GitHub refused (HTTP 403)"

    @pytest.mark.asyncio
    async def test_the_mint_gate_runs_once_the_binds_are_done(
        self, no_dispatcher_error
    ):
        job = _job("j1")
        store = FakeStore(pinned=[job], agents=[{"id": "a1", "metadata": {}}])
        bind = AsyncMock(return_value=("wait", None))
        mint = AsyncMock(return_value=("dispatch", None))

        await dispatch_pending_jobs(
            dependencies=_deps(store, bind_gate=bind, mint_gate=mint)
        )

        mint.assert_not_awaited()
        assert store.called("claim_job_for_agent") == []

    @pytest.mark.asyncio
    async def test_a_minted_job_is_claimed_once_its_credentials_are_live(
        self, no_dispatcher_error
    ):
        job = _job("j1")
        store = FakeStore(pinned=[job], agents=[{"id": "a1", "metadata": {}}])
        mint = AsyncMock(return_value=("dispatch", None))

        await dispatch_pending_jobs(dependencies=_deps(store, mint_gate=mint))

        mint.assert_awaited_once_with(job)
        assert [args for args, _ in store.called("claim_job_for_agent")] == [
            ("j1", "a1")
        ]

    @pytest.mark.asyncio
    async def test_a_bind_that_failed_for_good_fails_the_job_with_its_reason(
        self, no_dispatcher_error
    ):
        job = _job("j1", status="created")
        store = FakeStore(pinned=[job], agents=[{"id": "a1", "metadata": {}}])
        gate = AsyncMock(return_value=("fail", "Connector acme: No such tenant"))

        await dispatch_pending_jobs(dependencies=_deps(store, bind_gate=gate))

        assert store.called("claim_job_for_agent") == []
        ((args, kwargs),) = store.called("update_job_status")
        assert args == ("j1",)
        assert kwargs["status"] == "failed"
        assert kwargs["error_message"] == "Connector acme: No such tenant"
        assert kwargs["expected_status"] == "created"


# =============================================================================
# Unavailable models (unavailable_model_handling.md S4)
# =============================================================================

DISABLED_MESSAGE = (
    "The model `MiniMax-M3` (main model) is no longer available. Choose "
    "another model in the job's configuration, or ask your administrator."
)


def _unavailable(model: str = "MiniMax-M3", reason: str = "disabled"):
    return ModelUnavailable([UnavailableModel(slot="llm", model=model, reason=reason)])


class _AgentPool:
    """Phase 1.5's pod provisioner with free capacity, recording each pod."""

    def __init__(self) -> None:
        self._k8s_available = False
        self.is_available = True
        self.max_agents = 5
        self.active_count = AsyncMock(return_value=0)
        self.provision_agent = AsyncMock(return_value="srw-agent-j-1")


class TestUnavailableModelRefusal:
    """A job whose model cannot run is failed before its claim: after the
    claim the start bundle's refusal cannot write the status while completion
    commands are on, and the job was claimed again at every lease expiry, each
    time on a fresh agent pod (k3d gate G6, 2026-10-09)."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "guard",
        [{"completion_commands_enabled": True}, {}],
        ids=["completion-commands-on", "completion-commands-off"],
    )
    @pytest.mark.parametrize(
        ("refusal", "message"),
        [
            (_unavailable(), DISABLED_MESSAGE),
            (
                _unavailable("gpt-9-preview", "unknown"),
                "The model `gpt-9-preview` (main model) is not configured on "
                "this installation. Choose another model in the job's "
                "configuration.",
            ),
        ],
        ids=["disabled", "unknown"],
    )
    async def test_the_job_fails_before_any_claim_or_provision(
        self, guard, refusal, message, no_dispatcher_error
    ):
        job = _job("j1", status="created")
        # No ready agent: an accepted job would get a fresh pod.
        store = FakeStore(pinned=[job])
        pool = _AgentPool()
        gate = AsyncMock(return_value=refusal)
        prepare = AsyncMock(return_value=True)
        bind = AsyncMock(return_value=("dispatch", None))
        deps = dataclasses.replace(
            _deps(
                store,
                agent_provisioner=pool,
                prepare_repository=prepare,
                bind_gate=bind,
                unavailable_models=gate,
            ),
            completion_control_boundary=SimpleNamespace(
                dispatch_guard_kwargs=lambda: dict(guard)
            ),
        )

        await dispatch_pending_jobs(dependencies=deps)

        gate.assert_awaited_once_with(job)
        assert store.called("update_job_status") == [
            (
                ("j1",),
                {
                    "status": "failed",
                    "error_message": message,
                    "expected_status": "created",
                },
            )
        ]
        assert store.called("get_available_agents") == []
        assert store.called("claim_job_for_agent") == []
        pool.provision_agent.assert_not_awaited()
        prepare.assert_not_awaited()
        bind.assert_not_awaited()
        assert any(
            "refusing job j1, unavailable model slot(s)" in r.getMessage()
            for r in no_dispatcher_error.records
        )

    @pytest.mark.asyncio
    async def test_a_resumed_job_is_refused_before_its_resume(
        self, no_dispatcher_error
    ):
        job = _job("j1", status="paused")
        store = FakeStore(
            pinned=[job],
            agents=[{"id": "a1", "metadata": {}}],
            checkpoints={"j1": True},
        )
        delivery = FakeDelivery()

        await dispatch_pending_jobs(
            dependencies=_deps(
                store,
                delivery=delivery,
                unavailable_models=AsyncMock(return_value=_unavailable()),
            )
        )

        assert store.called("update_job_status") == [
            (
                ("j1",),
                {
                    "status": "failed",
                    "error_message": DISABLED_MESSAGE,
                    "expected_status": "paused",
                },
            )
        ]
        assert store.called("claim_job_for_agent") == []
        delivery.resume.assert_not_awaited()
        delivery.dispatch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_stateless_job_is_refused_before_its_admission(
        self, no_dispatcher_error
    ):
        job = _stateless_job("s1")
        store = FakeStore(stateless=[job])

        await dispatch_pending_jobs(
            dependencies=_deps(
                store,
                auto_assign_enabled=False,
                stateless_worker_enabled=True,
                unavailable_models=AsyncMock(return_value=_unavailable()),
            )
        )

        ((args, kwargs),) = store.called("update_job_status")
        assert args == ("s1",)
        assert kwargs["error_message"] == DISABLED_MESSAGE
        assert kwargs["expected_status"] == "created"
        assert store.called("admit_stateless_worker_job") == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("lane", ["pinned", "stateless"])
    async def test_a_refused_subjob_unblocks_its_parent(
        self, lane, no_dispatcher_error
    ):
        """A scholar holds its parent in 'waiting', which only the
        completion-side unblock releases; a plain status write would strand
        the parent there for good."""
        if lane == "pinned":
            job = _job("child", parent_job_id="parent")
            store = FakeStore(pinned=[job])
            lanes: dict[str, bool] = {}
        else:
            job = _stateless_job("child", parent_job_id="parent")
            store = FakeStore(stateless=[job])
            lanes = {"auto_assign_enabled": False, "stateless_worker_enabled": True}
        scholar, delegation = AsyncMock(), AsyncMock()
        fail_subjob = functools.partial(
            job_workspace_authority.fail_subjob_and_unblock_parent,
            dependencies=SimpleNamespace(
                store=store,
                logger=logging.getLogger("test.unavailable_model"),
                handle_scholar_completion=scholar,
                handle_delegation_child_completion=delegation,
            ),
        )

        await dispatch_pending_jobs(
            dependencies=_deps(
                store,
                fail_subjob=fail_subjob,
                unavailable_models=AsyncMock(return_value=_unavailable()),
                **lanes,
            )
        )

        # The failure keeps the dispatcher's status CAS on both lanes.
        assert store.called("update_job_status") == [
            (
                ("child",),
                {
                    "status": "failed",
                    "error_message": DISABLED_MESSAGE,
                    "expected_status": "created",
                },
            )
        ]
        scholar.assert_awaited_once()
        assert scholar.await_args.args[0]["status"] == "failed"
        delegation.assert_awaited_once()
        assert store.called("claim_job_for_agent") == []
        assert store.called("admit_stateless_worker_job") == []

    @pytest.mark.asyncio
    async def test_a_top_level_job_is_failed_directly(self, no_dispatcher_error):
        store = FakeStore(pinned=[_job("j1")])
        fail_subjob = AsyncMock()

        await dispatch_pending_jobs(
            dependencies=_deps(
                store,
                fail_subjob=fail_subjob,
                unavailable_models=AsyncMock(return_value=_unavailable()),
            )
        )

        fail_subjob.assert_not_awaited()
        ((args, kwargs),) = store.called("update_job_status")
        assert args == ("j1",)
        assert kwargs["expected_status"] == "created"

    @pytest.mark.asyncio
    async def test_a_job_whose_models_can_run_is_claimed_as_before(
        self, no_dispatcher_error
    ):
        job = _job("j1")
        agent = {"id": "a1", "metadata": {}}
        store = FakeStore(pinned=[job], agents=[agent])
        delivery = FakeDelivery()
        gate = AsyncMock(return_value=None)

        await dispatch_pending_jobs(
            dependencies=_deps(store, delivery=delivery, unavailable_models=gate)
        )

        gate.assert_awaited_once_with(job)
        assert store.called("update_job_status") == []
        assert [args for args, _ in store.called("claim_job_for_agent")] == [
            ("j1", "a1")
        ]
        delivery.dispatch.assert_awaited_once_with(job, agent)

    @pytest.mark.asyncio
    async def test_a_job_whose_models_can_run_still_gets_a_pod(
        self, no_dispatcher_error
    ):
        """The control for the refusal above: the same job, accepted, makes
        Phase 1.5 provision an agent."""
        store = FakeStore(pinned=[_job("j1")])
        pool = _AgentPool()

        await dispatch_pending_jobs(
            dependencies=_deps(
                store,
                agent_provisioner=pool,
                unavailable_models=AsyncMock(return_value=None),
            )
        )

        assert store.called("update_job_status") == []
        pool.provision_agent.assert_awaited_once_with(purpose="job")


class TestClaimedJobsAreNotPending:
    """A job this pass claimed is processing even when its delivery returned
    False (a refused start fails it through the completion ledger, connector
    drivers decision 34): it provisions no agent pod and preempts nothing."""

    @pytest.mark.asyncio
    async def test_a_refused_start_provisions_no_pod(self, no_dispatcher_error):
        job = _job("j1")
        store = FakeStore(pinned=[job], agents=[{"id": "a1", "metadata": {}}])
        delivery = FakeDelivery()
        delivery.dispatch = AsyncMock(return_value=False)
        pool = _AgentPool()

        await dispatch_pending_jobs(
            dependencies=_deps(store, delivery=delivery, agent_provisioner=pool)
        )

        delivery.dispatch.assert_awaited_once()
        pool.provision_agent.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_refused_start_preempts_nothing(self, no_dispatcher_error):
        candidate = {"id": "running", "priority": 1, "assigned_agent_id": "a9"}
        store = FakeStore(
            pinned=[_job("j1", priority=9)],
            agents=[{"id": "a1", "metadata": {}}],
            candidates=[candidate],
        )
        delivery = FakeDelivery()
        delivery.dispatch = AsyncMock(return_value=False)

        await dispatch_pending_jobs(dependencies=_deps(store, delivery=delivery))
        await _drain_tasks()

        delivery.initiate_pause.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_job_another_replica_claimed_is_still_pending_here(
        self, no_dispatcher_error
    ):
        store = FakeStore(
            pinned=[_job("j1")],
            agents=[{"id": "a1", "metadata": {}}],
            claims={"j1": False},
        )
        pool = _AgentPool()

        await dispatch_pending_jobs(dependencies=_deps(store, agent_provisioner=pool))

        pool.provision_agent.assert_awaited_once_with(purpose="job")


# =============================================================================
# Stateless admission
# =============================================================================


class TestStatelessAdmission:
    @pytest.mark.asyncio
    async def test_lost_admission_cas_is_never_appended_for_agent_matching(
        self, no_dispatcher_error
    ):
        pinned = _job("pinned-1")
        stateless = _stateless_job("stateless-1", priority=7, user_id="u-9")
        store = FakeStore(
            pinned=[pinned],
            stateless=[stateless],
            agents=[{"id": "a1", "metadata": {}}, {"id": "a2", "metadata": {}}],
            admissions={"stateless-1": (False, None)},
        )
        delivery = FakeDelivery()

        await dispatch_pending_jobs(
            dependencies=_deps(
                store,
                delivery=delivery,
                auto_assign_enabled=True,
                stateless_worker_enabled=True,
            )
        )

        assert store.called("admit_stateless_worker_job") == [
            (
                ("stateless-1",),
                {
                    "fair_key": "u-9",
                    "priority": 7,
                    "allow_vm_workspace": vm_workspaces_on_pod_network(),
                    **GUARD_KWARGS,
                },
            )
        ]
        # Two free agents, yet only the pinned job is claimed and delivered.
        assert [a for a, _ in store.called("claim_job_for_agent")] == [
            ("pinned-1", "a1")
        ]
        delivery.dispatch.assert_awaited_once_with(pinned, store.agents[0])
        messages = [r.getMessage() for r in no_dispatcher_error.records]
        assert any("stateless admission CAS lost" in m for m in messages)
        assert not any("admitted stateless worker job" in m for m in messages)

    @pytest.mark.asyncio
    async def test_admitted_stateless_job_stays_off_the_agent_plane(
        self, no_dispatcher_error
    ):
        stateless = _stateless_job("stateless-1")
        store = FakeStore(
            stateless=[stateless],
            agents=[{"id": "a1", "metadata": {}}],
            admissions={"stateless-1": (True, "inserted")},
        )

        await dispatch_pending_jobs(
            dependencies=_deps(
                store, auto_assign_enabled=False, stateless_worker_enabled=True
            )
        )

        assert len(store.called("admit_stateless_worker_job")) == 1
        assert store.called("get_available_agents") == []
        assert store.called("claim_job_for_agent") == []
        assert any(
            "admitted stateless worker job" in r.getMessage()
            for r in no_dispatcher_error.records
        )


# =============================================================================
# Preemption
# =============================================================================


class TestPreemption:
    @staticmethod
    def _unplaced(pending_priority: int, candidates: list[dict]) -> FakeStore:
        # No free agents and no agent provisioner: the job reaches Phase 2.
        return FakeStore(
            pinned=[_job("pending", priority=pending_priority)],
            agents=[],
            candidates=candidates,
        )

    @pytest.mark.asyncio
    async def test_strictly_higher_priority_pauses_into_the_shared_set(
        self, no_dispatcher_error
    ):
        candidate = {"id": "running", "priority": 3, "assigned_agent_id": "a9"}
        store = self._unplaced(8, [candidate])
        shared = {"already-pausing-elsewhere"}
        state = JobDispatchState(pause_pending_job_ids=shared)
        delivery = FakeDelivery()

        await dispatch_pending_jobs(
            dependencies=_deps(store, state=state, delivery=delivery)
        )
        await _drain_tasks()

        delivery.initiate_pause.assert_awaited_once_with(candidate)
        assert state.pause_pending_job_ids is shared
        assert shared == {"already-pausing-elsewhere", "running"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("candidate_priority", [8, 9])
    async def test_equal_or_lower_priority_never_preempts(
        self, candidate_priority, no_dispatcher_error
    ):
        candidate = {"id": "running", "priority": candidate_priority}
        store = self._unplaced(8, [candidate])
        state = JobDispatchState()
        delivery = FakeDelivery()

        await dispatch_pending_jobs(
            dependencies=_deps(store, state=state, delivery=delivery)
        )
        await _drain_tasks()

        delivery.initiate_pause.assert_not_awaited()
        assert state.pause_pending_job_ids == set()

    @pytest.mark.asyncio
    async def test_candidate_already_pause_pending_is_skipped(
        self, no_dispatcher_error
    ):
        pausing = {"id": "pausing", "priority": 1}
        next_best = {"id": "next", "priority": 2}
        store = self._unplaced(8, [pausing, next_best])
        state = JobDispatchState(pause_pending_job_ids={"pausing"})
        delivery = FakeDelivery()

        await dispatch_pending_jobs(
            dependencies=_deps(store, state=state, delivery=delivery)
        )
        await _drain_tasks()

        delivery.initiate_pause.assert_awaited_once_with(next_best)
        assert state.pause_pending_job_ids == {"pausing", "next"}

    @pytest.mark.asyncio
    async def test_one_candidate_is_preempted_once_per_cycle(self, no_dispatcher_error):
        candidate = {"id": "running", "priority": 1}
        store = FakeStore(
            pinned=[_job("p1", priority=8), _job("p2", priority=9)],
            candidates=[candidate],
        )
        delivery = FakeDelivery()

        await dispatch_pending_jobs(dependencies=_deps(store, delivery=delivery))
        await _drain_tasks()

        delivery.initiate_pause.assert_awaited_once_with(candidate)

    @pytest.mark.asyncio
    async def test_subjob_with_terminal_parent_does_not_preempt(
        self, no_dispatcher_error
    ):
        candidate = {"id": "running", "priority": 1}
        store = FakeStore(
            pinned=[_job("child", priority=9, parent_job_id="parent")],
            candidates=[candidate],
            jobs={"parent": {"id": "parent", "status": "completed"}},
        )
        delivery = FakeDelivery()

        await dispatch_pending_jobs(dependencies=_deps(store, delivery=delivery))
        await _drain_tasks()

        assert (("parent",), {}) in store.called("get_job")
        delivery.initiate_pause.assert_not_awaited()


# =============================================================================
# trigger_dispatch
# =============================================================================


class TestTriggerDispatch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("auto_assign", "stateless", "leader", "expected_tasks"),
        [
            (False, False, True, 0),
            (True, False, False, 0),
            (False, True, False, 0),
            (True, False, True, 1),
            (False, True, True, 1),
            (True, True, True, 1),
        ],
    )
    async def test_schedules_one_pass_only_for_an_enabled_leader(
        self, monkeypatch, auto_assign, stateless, leader, expected_tasks
    ):
        from orchestrator.services import leader_election

        is_leader = asyncio.Event()
        if leader:
            is_leader.set()
        monkeypatch.setattr(leader_election, "is_leader", is_leader)

        store = FakeStore()
        deps = _deps(
            store,
            auto_assign_enabled=auto_assign,
            stateless_worker_enabled=stateless,
        )
        created: list[asyncio.Task] = []
        real_create_task = asyncio.create_task

        def spy(coro, *args, **kwargs):
            task = real_create_task(coro, *args, **kwargs)
            created.append(task)
            return task

        with monkeypatch.context() as patch:
            patch.setattr(job_dispatcher.asyncio, "create_task", spy)
            assert trigger_dispatch(dependencies=deps) is None

        assert len(created) == expected_tasks
        if created:
            assert created[0].get_coro().__qualname__ == "_run_requested_dispatches"
            # Owned by the application's dispatch state until it finishes.
            assert created[0] in deps.state.tasks
            await asyncio.wait_for(created[0], 1.0)
            # The scheduled pass ran against exactly these dependencies.
            assert store.calls
        else:
            assert store.calls == []


# =============================================================================
# auto_assign_dispatcher
# =============================================================================


class TestAutoAssignDispatcher:
    @pytest.mark.asyncio
    async def test_shutdown_before_start_runs_no_tick(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO, logger=LOGGER_NAME)
        tick = AsyncMock()
        monkeypatch.setattr(job_dispatcher, "dispatch_pending_jobs", tick)
        shutdown = asyncio.Event()
        shutdown.set()
        deps = _deps(
            FakeStore(), auto_assign_enabled=True, stateless_worker_enabled=False
        )

        await asyncio.wait_for(auto_assign_dispatcher(shutdown, dependencies=deps), 1.0)

        tick.assert_not_awaited()
        messages = [r.getMessage() for r in caplog.records]
        assert (
            "Auto-assign dispatcher started (pinned=True, stateless_workers=False)"
            in messages
        )
        assert "Auto-assign dispatcher stopped" in messages

    @pytest.mark.asyncio
    async def test_shutdown_during_a_tick_exits_without_another_tick(self, monkeypatch):
        shutdown = asyncio.Event()
        seen: list[JobDispatchDependencies] = []

        async def tick(*, dependencies):
            seen.append(dependencies)
            shutdown.set()

        monkeypatch.setattr(job_dispatcher, "dispatch_pending_jobs", tick)
        deps = _deps(FakeStore())

        # Well under the 30 s cadence: the wait wakes on the event and breaks.
        await asyncio.wait_for(auto_assign_dispatcher(shutdown, dependencies=deps), 1.0)

        assert seen == [deps]
        assert seen[0] is deps

    @pytest.mark.asyncio
    async def test_a_failing_tick_is_logged_and_the_loop_still_stops(
        self, monkeypatch, caplog
    ):
        caplog.set_level(logging.INFO, logger=LOGGER_NAME)
        shutdown = asyncio.Event()
        ticks = 0

        async def tick(*, dependencies):
            nonlocal ticks
            ticks += 1
            shutdown.set()
            raise RuntimeError("store down")

        monkeypatch.setattr(job_dispatcher, "dispatch_pending_jobs", tick)

        await asyncio.wait_for(
            auto_assign_dispatcher(shutdown, dependencies=_deps(FakeStore())), 1.0
        )

        assert ticks == 1
        assert any(
            r.levelno == logging.ERROR
            and r.getMessage() == "Error in auto-assign dispatcher: store down"
            for r in caplog.records
        )


# --------------------------------------------------------------------------- #
# R1.B11 correction: dispatch passes and preemptions are the application's
# tasks (strong reference while running, drained before the pools close).
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_spawned_tasks_are_tracked_until_they_finish():
    state = job_dispatcher.JobDispatchState()
    release = asyncio.Event()

    async def work():
        await release.wait()

    task = state.spawn(work())
    assert state.tasks == {task}
    release.set()
    await task
    await asyncio.sleep(0)
    assert state.tasks == set()


@pytest.mark.asyncio
async def test_drain_cancels_and_awaits_in_flight_passes():
    state = job_dispatcher.JobDispatchState()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def long_pass():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    task = state.spawn(long_pass())
    await started.wait()
    await asyncio.wait_for(state.drain(), timeout=2)
    assert cancelled.is_set()
    assert task.cancelled()
    assert state.tasks == set()
    # Draining an idle dispatcher is a no-op.
    await asyncio.wait_for(state.drain(), timeout=1)
