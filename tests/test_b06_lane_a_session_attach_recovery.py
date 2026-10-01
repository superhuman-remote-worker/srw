"""The abort-successor recovery path, moved out of ``orchestrator.main``.

R1.B06 lane A. ``current_attach_abort_successor`` and the Docker/protocol
branches of ``prepare_attach_abort_successor_workspace`` had no direct
coverage before the extraction; they are characterized here. The exactly-once
property of ``schedule_attach_abort_successor`` and its stale-edge no-op are
re-proved against the extracted module.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.session_attach_recovery import (
    SessionAttachRecoveryDependencies,
    current_attach_abort_successor,
    prepare_attach_abort_successor_workspace,
    reconcile_attach_abort_successor,
    schedule_attach_abort_successor,
)
from orchestrator.services.workspace_binding import CANVAS_WORKSPACE_GENERATION_KEY
from orchestrator.services.workspace_lifecycle import EnsureOutcome

THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
AGENT_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
RUNTIME_GENERATION = "11111111-1111-4111-8111-111111111111"
ATTACH_TOKEN = "22222222-2222-4222-8222-222222222222"
WORKSPACE_GENERATION = "33333333-3333-4333-8333-333333333333"
WORKSPACE_RUNTIME = "44444444-4444-4444-8444-444444444444"
SUCCESSOR_GENERATION = "55555555-5555-4555-8555-555555555555"
FRESH_WORKSPACE_RUNTIME = "66666666-6666-4666-8666-666666666666"
FINGERPRINT = "SHA256:" + ("A" * 43)


def _successor_thread(*, workspace=None, binding=None, **updates):
    row = {
        "id": THREAD_ID,
        "user_id": "user-a",
        "status": "created",
        "execution_lane": "pinned",
        "agent_id": None,
        "runtime_generation": SUCCESSOR_GENERATION,
        "runtime_attach_token": None,
        "runtime_retirement_token": None,
        "config_name": "session_base",
        "metadata": {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "datasource_ids": ["datasource-a"],
            "workspace_container": (
                workspace
                if workspace is not None
                else {
                    "status": "ready",
                    "provisioner": "k8s",
                    CANVAS_WORKSPACE_GENERATION_KEY: WORKSPACE_GENERATION,
                    WORKSPACE_RUNTIME_INCARNATION_KEY: WORKSPACE_RUNTIME,
                }
            ),
            "_workspace_binding": (
                binding
                if binding is not None
                else {
                    "generation": WORKSPACE_GENERATION,
                    "kind": "remote",
                    "backing_id": "k8s-pvc:workspace-pvc-a",
                    "ssh_host_key_fingerprint": FINGERPRINT,
                }
            ),
        },
    }
    row.update(updates)
    return row


def _candidate(**updates):
    payload = {
        "thread_id": THREAD_ID,
        "retired_runtime_generation": RUNTIME_GENERATION,
        "retired_attach_token": ATTACH_TOKEN,
        "retired_agent_id": AGENT_ID,
        "successor_generation": SUCCESSOR_GENERATION,
        "quiescence_protocol": "workspace_process_zero_v1",
        "workspace_generation": WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
    }
    payload.update(updates)
    return payload


def _deps(
    *,
    store=None,
    container=None,
    docker=None,
    ensure=None,
    project_ids=None,
    reconcile=None,
    tasks=None,
    provision=None,
):
    return SessionAttachRecoveryDependencies(
        store=store or MagicMock(),
        container_provisioner=container or MagicMock(),
        docker_provisioner=docker or MagicMock(),
        workspace_suspension_service=MagicMock(),
        ensure_session_workspace=ensure or AsyncMock(),
        thread_project_ids=project_ids or AsyncMock(return_value=[]),
        reconcile_attach_abort_successor=reconcile or AsyncMock(return_value=True),
        provision_or_assign=provision or AsyncMock(),
        successor_tasks=tasks if tasks is not None else {},
    )


class TestCurrentAttachAbortSuccessor:
    def _call(self, thread):
        return current_attach_abort_successor(
            thread,
            thread_id=THREAD_ID,
            successor_generation=SUCCESSOR_GENERATION,
        )

    def test_the_exact_open_unbound_g2_is_current(self):
        assert self._call(_successor_thread()) is True

    @pytest.mark.parametrize(
        "updates",
        [
            {"agent_id": AGENT_ID},
            {"runtime_attach_token": ATTACH_TOKEN},
            {"runtime_retirement_token": "t"},
            {"status": "active"},
            {"status": "ended"},
            {"execution_lane": "stateless"},
            {"runtime_generation": RUNTIME_GENERATION},
            {"id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"},
            {"runtime_generation": "not-a-uuid"},
        ],
        ids=[
            "rebound",
            "reissued-token",
            "retiring",
            "already-active",
            "ended",
            "lane-flipped",
            "still-g1",
            "other-thread",
            "malformed-generation",
        ],
    )
    def test_anything_that_moved_is_not_the_successor(self, updates):
        assert self._call(_successor_thread(**updates)) is False

    @pytest.mark.parametrize("thread", [None, {}])
    def test_a_missing_row_is_not_the_successor(self, thread):
        assert self._call(thread) is False

    @pytest.mark.parametrize(
        "vm,eligible",
        [
            (None, True),
            ({"status": "waiting_capacity"}, True),
            ({"status": "waiting_capacity", "initial_runtime": None}, False),
            ({"status": "waiting_capacity", "initial_runtime": "broken"}, False),
            ({"status": "waiting_capacity", "initial_runtime": {}}, False),
            ({"status": "ready", "initial_runtime": "broken"}, False),
            (
                {
                    "status": "waiting_capacity",
                    "initial_runtime": {
                        "runtime_generation": SUCCESSOR_GENERATION,
                        "agent_id": AGENT_ID,
                        "runtime_attach_token": ATTACH_TOKEN,
                    },
                },
                True,
            ),
            (
                {
                    "status": "waiting_capacity",
                    "initial_runtime": {
                        "runtime_generation": RUNTIME_GENERATION,
                        "agent_id": AGENT_ID,
                        "runtime_attach_token": ATTACH_TOKEN,
                    },
                },
                False,
            ),
            (
                {
                    "status": "failed",
                    "initial_runtime": {
                        "runtime_generation": RUNTIME_GENERATION,
                        "agent_id": AGENT_ID,
                        "runtime_attach_token": ATTACH_TOKEN,
                    },
                },
                False,
            ),
            (
                {
                    "status": "ready",
                    "initial_runtime": {
                        "runtime_generation": RUNTIME_GENERATION,
                        "agent_id": AGENT_ID,
                        "runtime_attach_token": ATTACH_TOKEN,
                    },
                },
                True,
            ),
            (
                {
                    "status": "waiting_capacity",
                    "rootdisk": "kept",
                    "initial_runtime": {
                        "runtime_generation": RUNTIME_GENERATION,
                    },
                },
                True,
            ),
            (
                {
                    "status": "waiting_capacity",
                    "idle_wake_operation_id": AGENT_ID,
                    "initial_runtime": {"runtime_generation": RUNTIME_GENERATION},
                },
                True,
            ),
        ],
        ids=[
            "no-vm",
            "no-marker",
            "null-marker",
            "malformed-marker",
            "incomplete-marker",
            "ready-malformed",
            "same-runtime",
            "old-waiting",
            "old-failed",
            "ready",
            "retained",
            "wake",
        ],
    )
    def test_initial_vm_marker_controls_successor_eligibility(self, vm, eligible):
        current = _successor_thread()
        if vm is not None:
            current["metadata"]["vm"] = vm
        assert self._call(current) is eligible


@pytest.mark.asyncio
async def test_abort_successor_holds_old_nonready_initial_vm_source():
    old_vm = {
        "status": "waiting_capacity",
        "initial_runtime": {
            "runtime_generation": RUNTIME_GENERATION,
            "agent_id": AGENT_ID,
            "runtime_attach_token": ATTACH_TOKEN,
        },
        "creation_request_id": "77777777-7777-4777-8777-777777777777",
    }
    current = _successor_thread()
    current["metadata"]["vm"] = old_vm
    store = MagicMock()
    store.get_thread = AsyncMock(return_value=current)
    store.try_thread_advisory_lock = MagicMock()

    @asynccontextmanager
    async def lock(_thread_id):
        yield True

    store.try_thread_advisory_lock.side_effect = lock
    provision = AsyncMock()
    deps = _deps(store=store, provision=provision)
    candidate = _candidate(quiescence_protocol="agent_attach_not_started_v1")

    assert await reconcile_attach_abort_successor(candidate, dependencies=deps) is False
    assert current["runtime_generation"] == SUCCESSOR_GENERATION
    assert current["agent_id"] is None
    assert current["metadata"]["vm"] is old_vm
    deps.thread_project_ids.assert_not_awaited()
    provision.assert_not_awaited()


@pytest.mark.asyncio
async def test_abort_successor_rechecks_initial_vm_marker_after_project_lookup():
    initial = _successor_thread()
    held = _successor_thread()
    held["metadata"]["vm"] = {
        "status": "waiting_capacity",
        "initial_runtime": {
            "runtime_generation": RUNTIME_GENERATION,
            "agent_id": AGENT_ID,
            "runtime_attach_token": ATTACH_TOKEN,
        },
    }
    store = MagicMock()
    store.get_thread = AsyncMock(side_effect=[initial, held])

    @asynccontextmanager
    async def lock(_thread_id):
        yield True

    store.try_thread_advisory_lock = lock
    provision = AsyncMock()
    project_ids = AsyncMock(return_value=[])
    deps = _deps(store=store, provision=provision, project_ids=project_ids)
    assert (
        await reconcile_attach_abort_successor(
            _candidate(quiescence_protocol="agent_attach_not_started_v1"),
            dependencies=deps,
        )
        is False
    )
    project_ids.assert_awaited_once_with(THREAD_ID)
    provision.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_abort_workspace_preparation_holds_old_initial_vm_marker():
    current = _successor_thread()
    current["metadata"]["vm"] = {
        "status": "waiting_capacity",
        "initial_runtime": {
            "runtime_generation": RUNTIME_GENERATION,
            "agent_id": AGENT_ID,
            "runtime_attach_token": ATTACH_TOKEN,
        },
    }
    assert (
        await prepare_attach_abort_successor_workspace(
            _candidate(quiescence_protocol="agent_attach_not_started_v1"),
            current,
            dependencies=_deps(),
        )
        is None
    )


class TestPrepareWorkspaceProtocolDispatch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "protocol",
        [
            "pre_delivery_no_payload_v1",
            "agent_attach_not_started_v1",
            "agent_runtime_zero_v1",
        ],
    )
    async def test_protocols_that_left_the_workspace_alive_pass_straight_through(
        self, protocol
    ):
        current = _successor_thread()
        deps = _deps()
        result = await prepare_attach_abort_successor_workspace(
            _candidate(quiescence_protocol=protocol), current, dependencies=deps
        )
        assert result is current
        deps.container_provisioner.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("protocol", ["", "future_protocol_v9", None])
    async def test_an_unrecognised_protocol_fails_closed(self, protocol):
        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(quiescence_protocol=protocol),
                _successor_thread(),
                dependencies=_deps(),
            )
            is None
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "field",
        [
            "thread_id",
            "retired_runtime_generation",
            "retired_attach_token",
            "retired_agent_id",
            "successor_generation",
            "workspace_generation",
            "workspace_runtime_incarnation",
        ],
    )
    async def test_a_malformed_identity_field_refuses_before_any_actuator(self, field):
        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(**{field: "not-a-uuid"}),
                _successor_thread(),
                dependencies=_deps(),
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_a_thread_that_is_no_longer_the_successor_refuses(self):
        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(),
                _successor_thread(agent_id=AGENT_ID),
                dependencies=_deps(),
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_a_static_workspace_provisioner_has_no_restart_actuator(self):
        current = _successor_thread(
            workspace={"status": "ready", "provisioner": "static"}
        )
        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(), current, dependencies=_deps()
            )
            is None
        )


class TestPrepareWorkspaceDockerLease:
    def _docker_thread(self, *, lease=WORKSPACE_RUNTIME, status="ready"):
        return _successor_thread(
            workspace={
                "status": status,
                "provisioner": "docker",
                "_docker_workspace_lease_id": lease,
            }
        )

    @pytest.mark.asyncio
    async def test_the_captured_lease_is_quarantined_then_the_endpoint_cleared(self):
        docker = MagicMock()
        docker.release_thread_workspace = AsyncMock(return_value=True)
        refreshed = self._docker_thread(lease="", status="deleted")
        store = SimpleNamespace(
            clear_pinned_attach_abort_docker_workspace_endpoint=AsyncMock(
                return_value=True
            ),
            get_thread=AsyncMock(return_value=refreshed),
        )

        result = await prepare_attach_abort_successor_workspace(
            _candidate(),
            self._docker_thread(),
            dependencies=_deps(store=store, docker=docker),
        )

        assert result is refreshed
        docker.release_thread_workspace.assert_awaited_once_with(
            THREAD_ID,
            expected_lease_id=WORKSPACE_RUNTIME,
            force_quarantine=True,
        )
        store.clear_pinned_attach_abort_docker_workspace_endpoint.assert_awaited_once_with(
            THREAD_ID,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
            successor_generation=SUCCESSOR_GENERATION,
            workspace_generation=WORKSPACE_GENERATION,
            docker_lease_id=WORKSPACE_RUNTIME,
        )

    @pytest.mark.asyncio
    async def test_a_refused_quarantine_stops_before_the_endpoint_cas(self):
        docker = MagicMock()
        docker.release_thread_workspace = AsyncMock(return_value=False)
        store = SimpleNamespace(
            clear_pinned_attach_abort_docker_workspace_endpoint=AsyncMock(
                return_value=True
            )
        )
        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(),
                self._docker_thread(),
                dependencies=_deps(store=store, docker=docker),
            )
            is None
        )
        store.clear_pinned_attach_abort_docker_workspace_endpoint.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_lost_endpoint_cas_refuses(self):
        docker = MagicMock()
        docker.release_thread_workspace = AsyncMock(return_value=True)
        store = SimpleNamespace(
            clear_pinned_attach_abort_docker_workspace_endpoint=AsyncMock(
                return_value=False
            ),
            get_thread=AsyncMock(),
        )
        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(),
                self._docker_thread(),
                dependencies=_deps(store=store, docker=docker),
            )
            is None
        )
        store.get_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_retry_that_finds_a_distinct_ready_lease_never_rewrites_it(self):
        docker = MagicMock()
        docker.release_thread_workspace = AsyncMock()
        current = self._docker_thread(lease=FRESH_WORKSPACE_RUNTIME)

        result = await prepare_attach_abort_successor_workspace(
            _candidate(), current, dependencies=_deps(docker=docker)
        )

        assert result is current
        docker.release_thread_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_distinct_lease_that_is_not_ready_refuses(self):
        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(),
                self._docker_thread(lease=FRESH_WORKSPACE_RUNTIME, status="creating"),
                dependencies=_deps(),
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_an_already_cleared_deleted_mirror_lets_the_caller_provision(self):
        current = self._docker_thread(lease="", status="deleted")
        result = await prepare_attach_abort_successor_workspace(
            _candidate(), current, dependencies=_deps()
        )
        assert result is current

    @pytest.mark.asyncio
    async def test_an_empty_lease_that_is_not_deleted_refuses(self):
        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(),
                self._docker_thread(lease="", status="ready"),
                dependencies=_deps(),
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_a_workspace_generation_mismatch_refuses(self):
        current = _successor_thread(
            workspace={
                "status": "ready",
                "provisioner": "docker",
                "_docker_workspace_lease_id": WORKSPACE_RUNTIME,
            },
            binding={"generation": FRESH_WORKSPACE_RUNTIME},
        )
        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(), current, dependencies=_deps()
            )
            is None
        )


class TestPrepareWorkspaceKubernetes:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "authority", ["same_name_replacement", "ambiguous", "unknown"]
    )
    async def test_an_ambiguous_pod_authority_is_not_deletion_authority(
        self, authority
    ):
        container = MagicMock()
        container.workspace_pod_authority = AsyncMock(return_value=authority)
        container.delete_workspace = AsyncMock()

        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(),
                _successor_thread(),
                dependencies=_deps(container=container),
            )
            is None
        )
        container.delete_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_refused_delete_stops_before_the_endpoint_cas(self):
        container = MagicMock()
        container.workspace_pod_authority = AsyncMock(return_value="exact_live")
        container.delete_workspace = AsyncMock(return_value=False)
        store = SimpleNamespace(
            clear_pinned_attach_abort_workspace_endpoint=AsyncMock(return_value=True)
        )

        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(),
                _successor_thread(),
                dependencies=_deps(store=store, container=container),
            )
            is None
        )
        store.clear_pinned_attach_abort_workspace_endpoint.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_fresh_incarnation_is_ide_health_checked_and_re_attested(self):
        fresh_workspace = {
            "status": "ready",
            "provisioner": "k8s",
            CANVAS_WORKSPACE_GENERATION_KEY: WORKSPACE_GENERATION,
            WORKSPACE_RUNTIME_INCARNATION_KEY: FRESH_WORKSPACE_RUNTIME,
        }
        fresh = _successor_thread(workspace=fresh_workspace)
        container = MagicMock()
        container.wait_for_workspace_code_server = AsyncMock(return_value=True)
        store = SimpleNamespace(get_thread=AsyncMock(return_value=fresh))

        result = await prepare_attach_abort_successor_workspace(
            _candidate(workspace_runtime_incarnation=WORKSPACE_RUNTIME),
            fresh,
            dependencies=_deps(store=store, container=container),
        )

        assert result is fresh
        container.wait_for_workspace_code_server.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_dead_ide_refuses_the_successor(self):
        fresh = _successor_thread(
            workspace={
                "status": "ready",
                "provisioner": "k8s",
                CANVAS_WORKSPACE_GENERATION_KEY: WORKSPACE_GENERATION,
                WORKSPACE_RUNTIME_INCARNATION_KEY: FRESH_WORKSPACE_RUNTIME,
            }
        )
        container = MagicMock()
        container.wait_for_workspace_code_server = AsyncMock(return_value=False)

        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(), fresh, dependencies=_deps(container=container)
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_the_retired_incarnation_can_never_be_re_advertised(self):
        """A U1 that survived the clear is refused even if it looks Ready."""
        container = MagicMock()
        container.workspace_pod_authority = AsyncMock(return_value="exact_absent")
        container.wait_for_workspace_code_server = AsyncMock(return_value=True)
        store = SimpleNamespace(
            clear_pinned_attach_abort_workspace_endpoint=AsyncMock(return_value=True),
            get_thread=AsyncMock(return_value=_successor_thread()),
        )

        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(),
                _successor_thread(),
                dependencies=_deps(store=store, container=container),
            )
            is None
        )
        container.wait_for_workspace_code_server.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_ensure_refuses_rather_than_advertising_a_stub(self):
        cleared = _successor_thread(
            workspace={
                "status": "creating",
                "provisioner": "k8s",
                WORKSPACE_RUNTIME_INCARNATION_KEY: None,
            }
        )
        container = MagicMock()
        container.workspace_pod_authority = AsyncMock(return_value="exact_absent")
        store = SimpleNamespace(
            clear_pinned_attach_abort_workspace_endpoint=AsyncMock(return_value=True),
            get_thread=AsyncMock(return_value=cleared),
        )
        ensure = AsyncMock(return_value=SimpleNamespace(outcome=EnsureOutcome.FAILED))

        assert (
            await prepare_attach_abort_successor_workspace(
                _candidate(),
                _successor_thread(),
                dependencies=_deps(store=store, container=container, ensure=ensure),
            )
            is None
        )
        ensure.assert_awaited_once()
        assert ensure.await_args.kwargs["expected_runtime_generation"] == (
            SUCCESSOR_GENERATION
        )
        assert ensure.await_args.kwargs["_pinned_runtime_lock_held"] is True


class TestScheduleAttachAbortSuccessor:
    def _store_with_outcome(self, outcome):
        conn = MagicMock()
        conn.fetchrow = AsyncMock(return_value=outcome)

        @asynccontextmanager
        async def acquire():
            yield conn

        return SimpleNamespace(acquire=acquire), conn

    @pytest.mark.asyncio
    async def test_the_exact_retired_edge_is_read_and_reconciled_once(self):
        outcome = {"thread_id": THREAD_ID, "successor_generation": SUCCESSOR_GENERATION}
        store, conn = self._store_with_outcome(outcome)
        reconcile = AsyncMock(return_value=True)
        tasks: dict = {}
        deps = _deps(store=store, reconcile=reconcile, tasks=tasks)

        task = schedule_attach_abort_successor(
            THREAD_ID,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
            dependencies=deps,
        )
        duplicate = schedule_attach_abort_successor(
            THREAD_ID,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
            dependencies=deps,
        )
        assert duplicate is task
        await task

        reconcile.assert_awaited_once_with(dict(outcome))
        assert conn.fetchrow.await_args.args[1:] == (
            THREAD_ID,
            RUNTIME_GENERATION,
            ATTACH_TOKEN,
            AGENT_ID,
        )
        assert tasks == {}

    @pytest.mark.asyncio
    async def test_a_different_retired_edge_gets_its_own_task(self):
        store, _ = self._store_with_outcome(None)
        tasks: dict = {}
        deps = _deps(store=store, tasks=tasks)

        first = schedule_attach_abort_successor(
            THREAD_ID,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
            dependencies=deps,
        )
        second = schedule_attach_abort_successor(
            THREAD_ID,
            retired_runtime_generation=SUCCESSOR_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
            dependencies=deps,
        )
        assert first is not second
        assert len(tasks) == 2
        await first
        await second

    @pytest.mark.asyncio
    async def test_a_missing_outcome_row_is_a_silent_no_op(self):
        store, _ = self._store_with_outcome(None)
        reconcile = AsyncMock()
        await schedule_attach_abort_successor(
            THREAD_ID,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
            dependencies=_deps(store=store, reconcile=reconcile),
        )
        reconcile.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_raising_reconcile_is_swallowed_and_the_slot_freed(self):
        store, _ = self._store_with_outcome({"thread_id": THREAD_ID})
        tasks: dict = {}
        await schedule_attach_abort_successor(
            THREAD_ID,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
            dependencies=_deps(
                store=store,
                reconcile=AsyncMock(side_effect=RuntimeError("boom")),
                tasks=tasks,
            ),
        )
        assert tasks == {}

    @pytest.mark.asyncio
    async def test_the_registry_is_the_injected_dict_not_a_module_global(self):
        store, _ = self._store_with_outcome(None)
        mine: dict = {}
        task = schedule_attach_abort_successor(
            THREAD_ID,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
            dependencies=_deps(store=store, tasks=mine),
        )
        assert list(mine) == [(THREAD_ID, RUNTIME_GENERATION, ATTACH_TOKEN, AGENT_ID)]
        await task
