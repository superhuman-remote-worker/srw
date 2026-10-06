"""Reservation, delivery, abort and acknowledgement for one pinned attach.

R1.B06 lane A. Pins the atomicity properties the port contract calls out
(§P8) against the extracted module: the reciprocal bind lives in one
transaction, the release keeps its two ``FOR UPDATE`` reads and its three
writes in that same transaction, and every refusal keeps its exact
``SessionAttachReleaseOutcome`` string. ``bind_registered_persistent_agent``
and ``find_idle_persistent_agent``'s stale-SHA skip had no direct coverage
before the extraction.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
)
from orchestrator.services.session_attach_binding import (
    SessionAttachBindingDependencies,
    WarmBindingReservationPending,
    acknowledge_retiring_failed_attach,
    bind_registered_persistent_agent,
    find_idle_persistent_agent,
    release_session_attach_binding,
    reserve_session_attach_binding,
    send_session_attach,
    send_session_attach_locked,
)
from orchestrator.services import session_attach_binding as binding_module

THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
AGENT_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
RUNTIME_GENERATION = "11111111-1111-4111-8111-111111111111"
ATTACH_TOKEN = "22222222-2222-4222-8222-222222222222"
WORKSPACE_GENERATION = "33333333-3333-4333-8333-333333333333"
WORKSPACE_RUNTIME = "44444444-4444-4444-8444-444444444444"
PROTECTION_ID = "77777777-7777-4777-8777-777777777777"
POD_UID = "pod-uid-a"


def _deps(**overrides):
    fields = {
        "store": MagicMock(),
        "gitea_client": MagicMock(),
        "agent_provisioner": MagicMock(),
        "persistent_provisioner": MagicMock(),
        "reserve_pinned_warm_agent_binding": AsyncMock(),
        "release_pinned_warm_binding_protection": AsyncMock(),
        "await_protected_cloud_runtime_ready": AsyncMock(return_value=True),
        "prepare_thread_repository_authority": AsyncMock(return_value=None),
        "assemble_session_attach_payload": AsyncMock(
            return_value={"session_runtime_generation": RUNTIME_GENERATION}
        ),
        "schedule_attach_abort_successor": MagicMock(),
        "prepare_pinned_session_mutation_target": AsyncMock(),
        "pinned_session_mutation_target_is_current": AsyncMock(return_value=True),
        "reserve_session_attach_binding": AsyncMock(return_value=ATTACH_TOKEN),
        "release_session_attach_binding": AsyncMock(return_value="released"),
        "send_session_attach_locked": AsyncMock(return_value=True),
    }
    fields.update(overrides)
    return SessionAttachBindingDependencies(**fields)


# --------------------------------------------------------------------------
# bind_registered_persistent_agent
# --------------------------------------------------------------------------


def _bind_conn(results):
    conn = MagicMock()
    conn.execute = AsyncMock(side_effect=list(results))
    transaction = AsyncMock()
    transaction.__aenter__.return_value = None
    transaction.__aexit__.return_value = False
    conn.transaction = MagicMock(return_value=transaction)
    return conn, transaction


def _store_for(conn):
    @asynccontextmanager
    async def acquire():
        yield conn

    return SimpleNamespace(acquire=acquire)


class TestBindRegisteredPersistentAgent:
    @pytest.mark.asyncio
    async def test_an_unbound_insert_binds_both_sides_in_one_transaction(self):
        conn, transaction = _bind_conn(["UPDATE 1", "UPDATE 1"])
        token = await bind_registered_persistent_agent(
            THREAD_ID,
            AGENT_ID,
            None,
            RUNTIME_GENERATION,
            dependencies=_deps(store=_store_for(conn)),
        )

        assert token and token != ATTACH_TOKEN
        thread_sql = " ".join(conn.execute.await_args_list[0].args[0].split())
        assert "agent_id IS NULL" in thread_sql
        assert "execution_lane = $3" in thread_sql
        assert "runtime_generation = $4::uuid" in thread_sql
        assert "runtime_retirement_token IS NULL" in thread_sql
        assert (
            "status IN ('created','active','awaiting_user','suspended')" in thread_sql
        )
        agent_sql = " ".join(conn.execute.await_args_list[1].args[0].split())
        assert "UPDATE agents SET thread_id=$2::uuid" in agent_sql
        assert "current_job_id IS NULL" in agent_sql
        transaction.__aexit__.assert_awaited_once_with(None, None, None)

    @pytest.mark.asyncio
    async def test_an_exact_rebind_asserts_the_expected_prior_owner(self):
        conn, _ = _bind_conn(["UPDATE 1", "UPDATE 1"])
        await bind_registered_persistent_agent(
            THREAD_ID,
            AGENT_ID,
            AGENT_ID,
            RUNTIME_GENERATION,
            dependencies=_deps(store=_store_for(conn)),
        )
        thread_sql = " ".join(conn.execute.await_args_list[0].args[0].split())
        assert "agent_id = $4" in thread_sql
        assert conn.execute.await_args_list[0].args[4] == AGENT_ID

    @pytest.mark.asyncio
    async def test_a_lost_thread_cas_returns_none_and_never_touches_the_agent(self):
        conn, _ = _bind_conn(["UPDATE 0"])
        assert (
            await bind_registered_persistent_agent(
                THREAD_ID,
                AGENT_ID,
                None,
                RUNTIME_GENERATION,
                dependencies=_deps(store=_store_for(conn)),
            )
            is None
        )
        assert conn.execute.await_count == 1

    @pytest.mark.asyncio
    async def test_a_lost_reciprocal_cas_raises_rather_than_half_binding(self):
        conn, _ = _bind_conn(["UPDATE 1", "UPDATE 0"])
        with pytest.raises(RuntimeError, match="no longer available"):
            await bind_registered_persistent_agent(
                THREAD_ID,
                AGENT_ID,
                None,
                RUNTIME_GENERATION,
                dependencies=_deps(store=_store_for(conn)),
            )


# --------------------------------------------------------------------------
# find_idle_persistent_agent
# --------------------------------------------------------------------------


class TestFindIdlePersistentAgent:
    @pytest.mark.asyncio
    async def test_the_first_current_agent_in_heartbeat_order_wins(self, monkeypatch):
        monkeypatch.setenv("AGENT_IMAGE", "ghcr.io/x/agent:sha-good")
        monkeypatch.delenv("PERSISTENT_AGENT_IMAGE", raising=False)
        store = SimpleNamespace(
            fetch=AsyncMock(
                return_value=[
                    {"id": "stale", "metadata": {"build_sha": "old"}},
                    {"id": "fresh", "metadata": {"build_sha": "good"}},
                ]
            )
        )
        agent = await find_idle_persistent_agent(dependencies=_deps(store=store))
        assert agent["id"] == "fresh"
        sql = " ".join(store.fetch.await_args.args[0].split())
        assert "agent_mode IN ('persistent', 'dual')" in sql
        assert "thread_id IS NULL" in sql
        assert "ORDER BY last_heartbeat DESC" in sql

    @pytest.mark.asyncio
    async def test_a_json_string_metadata_column_is_decoded(self, monkeypatch):
        monkeypatch.setenv("AGENT_IMAGE", "ghcr.io/x/agent:sha-good")
        store = SimpleNamespace(
            fetch=AsyncMock(
                return_value=[
                    {"id": "a", "metadata": json.dumps({"build_sha": "good"})}
                ]
            )
        )
        assert (await find_idle_persistent_agent(dependencies=_deps(store=store)))[
            "id"
        ] == "a"

    @pytest.mark.asyncio
    async def test_unparseable_metadata_is_treated_as_stale_not_current(
        self, monkeypatch
    ):
        monkeypatch.setenv("AGENT_IMAGE", "ghcr.io/x/agent:sha-good")
        store = SimpleNamespace(
            fetch=AsyncMock(return_value=[{"id": "a", "metadata": "{not json"}])
        )
        assert await find_idle_persistent_agent(dependencies=_deps(store=store)) is None

    @pytest.mark.asyncio
    async def test_no_sha_expectation_admits_every_pool_agent(self, monkeypatch):
        monkeypatch.delenv("AGENT_IMAGE", raising=False)
        monkeypatch.delenv("PERSISTENT_AGENT_IMAGE", raising=False)
        store = SimpleNamespace(
            fetch=AsyncMock(return_value=[{"id": "a", "metadata": None}])
        )
        assert (await find_idle_persistent_agent(dependencies=_deps(store=store)))[
            "id"
        ] == "a"

    @pytest.mark.asyncio
    async def test_a_query_failure_is_no_idle_agent_not_an_exception(self):
        store = SimpleNamespace(fetch=AsyncMock(side_effect=RuntimeError("db down")))
        assert await find_idle_persistent_agent(dependencies=_deps(store=store)) is None


# --------------------------------------------------------------------------
# send_session_attach / reserve
# --------------------------------------------------------------------------


class TestSendSessionAttach:
    @pytest.mark.asyncio
    async def test_the_datasource_lock_wraps_the_whole_delivery(self):
        events: list[str] = []

        @asynccontextmanager
        async def lock(thread_id):
            events.append(f"lock:{thread_id}")
            yield
            events.append("unlock")

        async def locked(*args, **kwargs):
            events.append("deliver")
            return True

        deps = _deps(
            store=SimpleNamespace(thread_datasource_lock=lock),
            send_session_attach_locked=AsyncMock(side_effect=locked),
        )
        assert await send_session_attach({"id": AGENT_ID}, THREAD_ID, dependencies=deps)
        assert events == [f"lock:{THREAD_ID}", "deliver", "unlock"]
        assert deps.send_session_attach_locked.await_args.kwargs == {
            "config_override": None,
            "project_ids": None,
            "datasources": None,
            "config_name": None,
            "expected_runtime_generation": None,
        }


class TestReserveSessionAttachBinding:
    async def _reserve(self, result_or_exc):
        reserve = (
            AsyncMock(side_effect=result_or_exc)
            if isinstance(result_or_exc, BaseException)
            else AsyncMock(return_value=result_or_exc)
        )
        deps = _deps(reserve_pinned_warm_agent_binding=reserve)
        return (
            await reserve_session_attach_binding(
                AGENT_ID,
                THREAD_ID,
                expected_runtime_generation=RUNTIME_GENERATION,
                dependencies=deps,
            ),
            reserve,
        )

    @pytest.mark.asyncio
    async def test_a_bound_plan_returns_its_exact_attach_token(self):
        token, reserve = await self._reserve(
            SimpleNamespace(bound=True, state="bound", attach_token=ATTACH_TOKEN)
        )
        assert token == ATTACH_TOKEN
        assert reserve.await_args.kwargs["thread_id"] == THREAD_ID
        assert reserve.await_args.kwargs["agent_id"] == AGENT_ID
        assert (
            reserve.await_args.kwargs["expected_runtime_generation"]
            == RUNTIME_GENERATION
        )

    @pytest.mark.asyncio
    async def test_a_refusal_is_no_reservation_not_an_exception(self):
        token, _ = await self._reserve(
            SimpleNamespace(bound=False, state="refused", attach_token=None)
        )
        assert token is None

    @pytest.mark.asyncio
    async def test_a_pending_plan_fences_every_fallback_runtime(self):
        with pytest.raises(WarmBindingReservationPending):
            await self._reserve(
                SimpleNamespace(bound=False, state="pending", attach_token=None)
            )

    @pytest.mark.asyncio
    async def test_a_post_plan_error_is_ambiguous_not_refused(self):
        with pytest.raises(WarmBindingReservationPending):
            await self._reserve(RuntimeError("ambiguous"))

    @pytest.mark.asyncio
    async def test_cancellation_is_never_swallowed_into_pending(self):
        with pytest.raises(asyncio.CancelledError):
            await self._reserve(asyncio.CancelledError())


# --------------------------------------------------------------------------
# release_session_attach_binding
# --------------------------------------------------------------------------


def _thread_row(*, workspace=False, warm=False, **updates):
    metadata = {"config_override": {"workspace": {"backend": "sandbox"}}}
    if workspace:
        metadata["workspace_container"] = {
            WORKSPACE_RUNTIME_INCARNATION_KEY: WORKSPACE_RUNTIME
        }
        metadata["_workspace_binding"] = {"generation": WORKSPACE_GENERATION}
    if warm:
        metadata["agent_pod"] = {
            "pod_name": "agent-a",
            "pod_uid": POD_UID,
            "namespace": "srw",
            "warm_binding_protection": PROTECTION_ID,
        }
    row = {
        "agent_id": AGENT_ID,
        "status": "created",
        "metadata": metadata,
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_attach_token": ATTACH_TOKEN,
        "runtime_retirement_token": None,
        "runtime_authority_exposed": True,
    }
    row.update(updates)
    return row


def _agent_row(**updates):
    row = {
        "thread_id": THREAD_ID,
        "current_job_id": None,
        "status": "session",
        "hostname": "agent-a",
        "pod_uid": POD_UID,
    }
    row.update(updates)
    return row


async def _release(
    thread,
    agent,
    *,
    prior=None,
    warm_binding=None,
    fetchvals=(False, False, False),
    execute_results=("UPDATE 1", "UPDATE 1", "INSERT 0 1"),
    deps_overrides=None,
    **kwargs,
):
    rows = [thread, agent, prior]
    if warm_binding is not None:
        rows.append(warm_binding)
    conn = MagicMock()
    conn.fetchrow = AsyncMock(side_effect=rows)
    conn.fetchval = AsyncMock(side_effect=list(fetchvals))
    conn.execute = AsyncMock(side_effect=list(execute_results))
    transaction = AsyncMock()
    transaction.__aenter__.return_value = None
    transaction.__aexit__.return_value = False
    conn.transaction = MagicMock(return_value=transaction)

    dependencies = _deps(store=_store_for(conn), **(deps_overrides or {}))
    outcome = await release_session_attach_binding(
        AGENT_ID,
        THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        expected_attach_token=ATTACH_TOKEN,
        dependencies=dependencies,
        **kwargs,
    )
    return outcome, conn, transaction, dependencies


class TestReleaseSessionAttachBinding:
    @pytest.mark.asyncio
    async def test_a_pre_delivery_abort_rotates_the_pair_in_one_transaction(self):
        outcome, conn, transaction, _ = await _release(
            _thread_row(), _agent_row(), pre_delivery=True
        )
        assert outcome == "released"
        assert conn.execute.await_count == 3
        thread_sql = " ".join(conn.execute.await_args_list[0].args[0].split())
        assert "runtime_generation=$5::uuid" in thread_sql
        assert "runtime_authority_exposed=false" in thread_sql
        assert "runtime_retirement_token IS NULL" in thread_sql
        outcome_sql = " ".join(conn.execute.await_args_list[2].args[0].split())
        assert "thread_runtime_attach_abort_outcomes" in outcome_sql
        assert "ON CONFLICT DO NOTHING" in outcome_sql
        # both authoritative reads are FOR UPDATE, in the same transaction
        assert "FOR UPDATE" in conn.fetchrow.await_args_list[0].args[0]
        assert "FOR UPDATE" in conn.fetchrow.await_args_list[1].args[0]
        transaction.__aexit__.assert_awaited_once_with(None, None, None)

    @pytest.mark.asyncio
    async def test_the_receipt_names_the_successor_it_rotated_to(self):
        outcome, conn, _, _ = await _release(
            _thread_row(), _agent_row(), pre_delivery=True
        )
        assert outcome == "released"
        receipt = json.loads(conn.execute.await_args_list[0].args[6])
        assert receipt["release_kind"] == "server_pre_delivery"
        assert receipt["quiescence_protocol"] == "pre_delivery_no_payload_v1"
        assert receipt["runtime_generation"] == RUNTIME_GENERATION
        assert (
            receipt["successor_generation"] == conn.execute.await_args_list[0].args[5]
        )
        assert receipt["agent_pod_uid"] == POD_UID

    @pytest.mark.asyncio
    async def test_a_replayed_abort_reports_already_detached_without_rewriting(self):
        outcome, conn, _, _ = await _release(
            _thread_row(),
            _agent_row(),
            prior={"successor_generation": WORKSPACE_GENERATION},
            pre_delivery=True,
        )
        assert outcome == "already_detached"
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "thread_updates",
        [
            {"agent_id": "other-agent"},
            {"runtime_generation": WORKSPACE_GENERATION},
            {"runtime_attach_token": WORKSPACE_RUNTIME},
            {"runtime_retirement_token": "t"},
            {"status": "active"},
            {"runtime_authority_exposed": False},
        ],
    )
    async def test_a_moved_thread_authority_is_unsafe(self, thread_updates):
        outcome, conn, _, _ = await _release(
            _thread_row(**thread_updates), _agent_row(), pre_delivery=True
        )
        assert outcome == "unsafe"
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "agent_updates",
        [
            {"thread_id": "other-thread"},
            {"current_job_id": "job-1"},
            {"status": "ready"},
            {"pod_uid": None},
        ],
    )
    async def test_a_moved_agent_is_unsafe(self, agent_updates):
        outcome, conn, _, _ = await _release(
            _thread_row(), _agent_row(**agent_updates), pre_delivery=True
        )
        assert outcome == "unsafe"
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_delivered_abort_must_present_the_captured_pod_uid(self):
        outcome, _, _, _ = await _release(
            _thread_row(workspace=True),
            _agent_row(),
            expected_agent_pod_uid="a-different-pod",
            local_runtime_quiesced=True,
            local_quiescence_protocol="workspace_process_zero_v1",
            workspace_generation=WORKSPACE_GENERATION,
            workspace_runtime_incarnation=WORKSPACE_RUNTIME,
        )
        assert outcome == "unsafe"

    @pytest.mark.asyncio
    async def test_an_unparseable_metadata_blob_is_unsafe(self):
        outcome, _, _, _ = await _release(
            _thread_row(metadata="{not json"), _agent_row(), pre_delivery=True
        )
        assert outcome == "unsafe"

    @pytest.mark.asyncio
    async def test_an_agent_pod_marker_that_names_another_pod_is_unsafe(self):
        thread = _thread_row()
        thread["metadata"]["agent_pod"] = {"pod_name": "agent-b", "pod_uid": POD_UID}
        outcome, _, _, _ = await _release(thread, _agent_row(), pre_delivery=True)
        assert outcome == "unsafe"

    @pytest.mark.asyncio
    async def test_the_proven_agent_pod_marker_is_cleared_never_uid_deleted(self):
        thread = _thread_row()
        thread["metadata"]["agent_pod"] = {"pod_name": "agent-a", "pod_uid": POD_UID}
        outcome, conn, _, _ = await _release(thread, _agent_row(), pre_delivery=True)
        assert outcome == "released"
        written = json.loads(conn.execute.await_args_list[0].args[7])
        assert "agent_pod" not in written
        assert "config_override" in written

    @pytest.mark.asyncio
    async def test_a_half_present_workspace_tuple_is_unsafe(self):
        thread = _thread_row()
        thread["metadata"]["workspace_container"] = {
            WORKSPACE_RUNTIME_INCARNATION_KEY: WORKSPACE_RUNTIME
        }
        thread["metadata"]["_workspace_binding"] = {}
        outcome, _, _, _ = await _release(thread, _agent_row(), pre_delivery=True)
        assert outcome == "unsafe"

    @pytest.mark.asyncio
    async def test_a_delivered_abort_without_quiescence_is_unsafe(self):
        outcome, conn, _, _ = await _release(
            _thread_row(workspace=True),
            _agent_row(),
            expected_agent_pod_uid=POD_UID,
        )
        assert outcome == "unsafe"
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_vm_backend_needs_the_orchestrators_own_actuator_proof(self):
        thread = _thread_row(workspace=True)
        thread["metadata"]["config_override"] = {"workspace": {"backend": "vm"}}
        outcome, _, _, _ = await _release(
            thread,
            _agent_row(),
            expected_agent_pod_uid=POD_UID,
            local_runtime_quiesced=True,
            local_quiescence_protocol="workspace_process_zero_v1",
            workspace_generation=WORKSPACE_GENERATION,
            workspace_runtime_incarnation=WORKSPACE_RUNTIME,
        )
        assert outcome == "unsafe"

    @pytest.mark.asyncio
    async def test_a_claimed_protocol_that_disagrees_with_the_derived_one_is_unsafe(
        self,
    ):
        outcome, _, _, _ = await _release(
            _thread_row(workspace=True),
            _agent_row(),
            expected_agent_pod_uid=POD_UID,
            local_runtime_quiesced=True,
            local_quiescence_protocol="agent_runtime_zero_v1",
            workspace_generation=WORKSPACE_GENERATION,
            workspace_runtime_incarnation=WORKSPACE_RUNTIME,
        )
        assert outcome == "unsafe"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("ws_generation", "ws_runtime"),
        [(None, WORKSPACE_RUNTIME), (WORKSPACE_GENERATION, None)],
    )
    async def test_a_workspace_tuple_that_is_not_the_captured_one_is_unsafe(
        self, ws_generation, ws_runtime
    ):
        outcome, _, _, _ = await _release(
            _thread_row(workspace=True),
            _agent_row(),
            expected_agent_pod_uid=POD_UID,
            local_runtime_quiesced=True,
            local_quiescence_protocol="workspace_process_zero_v1",
            workspace_generation=ws_generation,
            workspace_runtime_incarnation=ws_runtime,
        )
        assert outcome == "unsafe"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("fetchvals", [(True, False), (False, True)])
    async def test_admitted_input_or_control_forbids_a_generation_rollback(
        self, fetchvals
    ):
        outcome, conn, _, _ = await _release(
            _thread_row(), _agent_row(), pre_delivery=True, fetchvals=fetchvals
        )
        assert outcome == "unsafe"
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "execute_results",
        [
            ("UPDATE 0", "UPDATE 1", "INSERT 0 1"),
            ("UPDATE 1", "UPDATE 0", "INSERT 0 1"),
            ("UPDATE 1", "UPDATE 1", "INSERT 0 0"),
        ],
        ids=["thread-cas-lost", "agent-cas-lost", "outcome-conflict"],
    )
    async def test_any_lost_cas_rolls_the_whole_rotation_back(self, execute_results):
        outcome, _, transaction, _ = await _release(
            _thread_row(),
            _agent_row(),
            pre_delivery=True,
            execute_results=execute_results,
        )
        assert outcome == "unsafe"
        # the CAS-lost exception escapes the transaction body, so the context
        # manager sees it and rolls back
        assert transaction.__aexit__.await_args.args[0] is not None

    @pytest.mark.asyncio
    async def test_a_warm_protection_row_must_match_on_every_identity_column(self):
        warm = {
            "status": "bound",
            "source": "attach",
            "thread_id": THREAD_ID,
            "runtime_generation": RUNTIME_GENERATION,
            "runtime_attach_token": ATTACH_TOKEN,
            "agent_id": AGENT_ID,
            "pod_name": "agent-b",  # the mismatch
            "pod_uid": POD_UID,
            "namespace": "srw",
        }
        outcome, conn, _, _ = await _release(
            _thread_row(warm=True), _agent_row(), warm_binding=warm, pre_delivery=True
        )
        assert outcome == "unsafe"
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_matched_warm_protection_drains_the_pod_and_releases_after_commit(
        self,
    ):
        warm = {
            "status": "bound",
            "source": "attach",
            "thread_id": THREAD_ID,
            "runtime_generation": RUNTIME_GENERATION,
            "runtime_attach_token": ATTACH_TOKEN,
            "agent_id": AGENT_ID,
            "pod_name": "agent-a",
            "pod_uid": POD_UID,
            "namespace": "srw",
        }
        outcome, conn, _, deps = await _release(
            _thread_row(warm=True),
            _agent_row(),
            warm_binding=warm,
            pre_delivery=True,
            execute_results=("UPDATE 1", "UPDATE 1", "UPDATE 1", "INSERT 0 1"),
        )
        assert outcome == "released"
        assert (
            conn.execute.await_args_list[0]
            .args[0]
            .startswith("UPDATE thread_agent_warm_binding_protections")
        )
        assert conn.execute.await_args_list[2].args[4] == "draining"
        deps.release_pinned_warm_binding_protection.assert_awaited_once()
        assert (
            deps.release_pinned_warm_binding_protection.await_args.kwargs[
                "protection_id"
            ]
            == PROTECTION_ID
        )

    @pytest.mark.asyncio
    async def test_a_failed_finalizer_release_still_reports_released(self):
        warm = {
            "status": "bound",
            "source": "attach",
            "thread_id": THREAD_ID,
            "runtime_generation": RUNTIME_GENERATION,
            "runtime_attach_token": ATTACH_TOKEN,
            "agent_id": AGENT_ID,
            "pod_name": "agent-a",
            "pod_uid": POD_UID,
            "namespace": "srw",
        }
        outcome, _, _, deps = await _release(
            _thread_row(warm=True),
            _agent_row(),
            warm_binding=warm,
            pre_delivery=True,
            execute_results=("UPDATE 1", "UPDATE 1", "UPDATE 1", "INSERT 0 1"),
            deps_overrides={
                "release_pinned_warm_binding_protection": AsyncMock(
                    side_effect=RuntimeError("finalizer down")
                )
            },
        )
        assert outcome == "released"
        deps.release_pinned_warm_binding_protection.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_an_agent_without_a_warm_protection_returns_to_ready(self):
        outcome, conn, _, _ = await _release(
            _thread_row(), _agent_row(), pre_delivery=True
        )
        assert outcome == "released"
        assert conn.execute.await_args_list[1].args[4] == "ready"


# --------------------------------------------------------------------------
# acknowledge_retiring_failed_attach
# --------------------------------------------------------------------------


class TestAcknowledgeRetiringFailedAttach:
    async def _ack(self, store, **kwargs):
        params = {
            "expected_runtime_generation": RUNTIME_GENERATION,
            "expected_attach_token": ATTACH_TOKEN,
            "expected_agent_pod_uid": POD_UID,
            "local_quiescence_protocol": "workspace_process_zero_v1",
            "workspace_generation": WORKSPACE_GENERATION,
            "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
        }
        params.update(kwargs)
        return await acknowledge_retiring_failed_attach(
            AGENT_ID, THREAD_ID, dependencies=_deps(store=store), **params
        )

    def _store(self, thread, *, receipt=None, settled=False):
        return SimpleNamespace(
            get_thread=AsyncMock(return_value=thread),
            acknowledge_pinned_thread_local_quiescence=AsyncMock(return_value=receipt),
            has_exact_pinned_runtime_retirement_outcome=AsyncMock(return_value=settled),
        )

    @pytest.mark.asyncio
    async def test_a_settled_retirement_accepts_the_process_zero_proof(self):
        store = self._store(
            {
                "runtime_retirement_token": "t",
                "runtime_retirement_context": {"settle_status": "ended"},
            },
            receipt={"ok": True},
        )
        assert await self._ack(store) is True
        kwargs = store.acknowledge_pinned_thread_local_quiescence.await_args.kwargs
        assert kwargs["expected_retirement_token"] == "t"
        assert kwargs["expected_settle_status"] == "ended"
        assert kwargs["expected_quiescence_protocol"] == "workspace_process_zero_v1"
        assert kwargs["require_zero_admission"] is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("ws_generation", "ws_runtime", "derived"),
        [
            (WORKSPACE_GENERATION, WORKSPACE_RUNTIME, "workspace_process_zero_v1"),
            (None, None, "agent_runtime_zero_v1"),
            (WORKSPACE_GENERATION, None, "agent_runtime_zero_v1"),
        ],
    )
    async def test_the_pre_setup_latch_derives_the_ordinary_retirement_protocol(
        self, ws_generation, ws_runtime, derived
    ):
        store = self._store(
            {
                "runtime_retirement_token": "t",
                "runtime_retirement_context": {"settle_status": "suspended"},
            },
            receipt={"ok": True},
        )
        assert (
            await self._ack(
                store,
                local_quiescence_protocol="agent_attach_not_started_v1",
                workspace_generation=ws_generation,
                workspace_runtime_incarnation=ws_runtime,
            )
            is True
        )
        assert (
            store.acknowledge_pinned_thread_local_quiescence.await_args.kwargs[
                "expected_quiescence_protocol"
            ]
            == derived
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "thread",
        [
            None,
            {"runtime_retirement_token": None},
            {"runtime_retirement_token": "t", "runtime_retirement_context": {}},
            {
                "runtime_retirement_token": "t",
                "runtime_retirement_context": {"settle_status": "authorizing"},
            },
        ],
        ids=["no-row", "no-retirement", "no-settle-status", "not-yet-settled"],
    )
    async def test_a_row_without_a_settled_retirement_falls_back_to_the_readback(
        self, thread
    ):
        store = self._store(thread, settled=True)
        assert await self._ack(store) is True
        store.acknowledge_pinned_thread_local_quiescence.assert_not_awaited()
        store.has_exact_pinned_runtime_retirement_outcome.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_an_unparseable_retirement_context_refuses_outright(self):
        store = self._store(
            {
                "runtime_retirement_token": "t",
                "runtime_retirement_context": "{not json",
            },
            settled=True,
        )
        assert await self._ack(store) is False
        store.has_exact_pinned_runtime_retirement_outcome.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_refused_receipt_is_idempotent_against_an_existing_outcome(self):
        store = self._store(
            {
                "runtime_retirement_token": "t",
                "runtime_retirement_context": {"settle_status": "ended"},
            },
            receipt=None,
            settled=True,
        )
        assert await self._ack(store) is True

    @pytest.mark.asyncio
    async def test_a_refused_receipt_with_no_outcome_is_a_refusal(self):
        store = self._store(
            {
                "runtime_retirement_token": "t",
                "runtime_retirement_context": {"settle_status": "ended"},
            },
            receipt=None,
            settled=False,
        )
        assert await self._ack(store) is False


# --------------------------------------------------------------------------
# send_session_attach_locked
# --------------------------------------------------------------------------


def _pinned_thread(**updates):
    row = {
        "id": THREAD_ID,
        "execution_lane": "pinned",
        "status": "created",
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_retirement_token": None,
        "runtime_attach_token": ATTACH_TOKEN,
        "agent_id": AGENT_ID,
    }
    row.update(updates)
    return row


class _AttachClient:
    status_code = 200
    posts: list[tuple[str, dict]] = []
    error: Exception | None = None

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def post(self, url, *, json):
        type(self).posts.append((url, json))
        if type(self).error is not None:
            raise type(self).error
        return SimpleNamespace(status_code=type(self).status_code)


@pytest.fixture
def attach_client(monkeypatch):
    _AttachClient.posts = []
    _AttachClient.status_code = 200
    _AttachClient.error = None
    monkeypatch.setattr(binding_module.httpx, "AsyncClient", _AttachClient)
    return _AttachClient


def _target():
    return SimpleNamespace(
        agent={"id": AGENT_ID, "pod_ip": "10.0.0.1", "pod_port": 8001},
        recipient={"expected_thread_id": THREAD_ID},
    )


class TestSendSessionAttachLocked:
    async def _send(self, *, rows=None, deps_overrides=None, **kwargs):
        rows = rows if rows is not None else [_pinned_thread()] * 6
        overrides = {
            "prepare_pinned_session_mutation_target": AsyncMock(return_value=_target())
        }
        overrides.update(deps_overrides or {})
        dependencies = _deps(
            store=SimpleNamespace(get_thread=AsyncMock(side_effect=rows)), **overrides
        )
        result = await send_session_attach_locked(
            {"id": AGENT_ID, "pod_ip": "10.0.0.1", "pod_port": 8001},
            THREAD_ID,
            dependencies=dependencies,
            **kwargs,
        )
        return result, dependencies

    @pytest.mark.asyncio
    async def test_a_full_delivery_posts_the_recipient_envelope_and_token(
        self, attach_client
    ):
        accepted, deps = await self._send()
        assert accepted is True
        url, payload = attach_client.posts[0]
        assert url == "http://10.0.0.1:8001/session/attach"
        assert payload["session_runtime_attach_token"] == ATTACH_TOKEN
        assert payload["_recipient"] == {"expected_thread_id": THREAD_ID}
        deps.pinned_session_mutation_target_is_current.assert_awaited_once()
        deps.schedule_attach_abort_successor.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_non_pinned_lane_refuses_before_any_reservation(
        self, attach_client
    ):
        accepted, deps = await self._send(
            rows=[_pinned_thread(execution_lane="stateless")]
        )
        assert accepted is False
        deps.reserve_session_attach_binding.assert_not_awaited()
        assert attach_client.posts == []

    @pytest.mark.asyncio
    async def test_a_generation_the_caller_did_not_expect_refuses(self):
        accepted, deps = await self._send(
            expected_runtime_generation=WORKSPACE_GENERATION
        )
        assert accepted is False
        deps.reserve_session_attach_binding.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unready_protected_reader_refuses_before_the_reservation(self):
        accepted, deps = await self._send(
            deps_overrides={
                "await_protected_cloud_runtime_ready": AsyncMock(return_value=False)
            }
        )
        assert accepted is False
        deps.reserve_session_attach_binding.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unavailable_repository_authority_refuses(self):
        accepted, deps = await self._send(
            deps_overrides={
                "prepare_thread_repository_authority": AsyncMock(
                    side_effect=ManagedRepositoryAuthorityError("repo_unavailable")
                )
            }
        )
        assert accepted is False
        deps.reserve_session_attach_binding.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_pending_warm_protection_reports_ambiguous_delivery(self):
        accepted, deps = await self._send(
            deps_overrides={
                "reserve_session_attach_binding": AsyncMock(
                    side_effect=WarmBindingReservationPending
                )
            }
        )
        assert accepted is True
        deps.assemble_session_attach_payload.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_refused_reservation_leaves_no_reservation_behind(self):
        accepted, deps = await self._send(
            deps_overrides={
                "reserve_session_attach_binding": AsyncMock(return_value=None)
            }
        )
        assert accepted is False
        deps.release_session_attach_binding.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("payload", "label"),
        [
            (None, "assembly-refused"),
            ({"session_runtime_generation": WORKSPACE_GENERATION}, "wrong-generation"),
        ],
    )
    async def test_a_bad_payload_releases_and_schedules_the_successor(
        self, payload, label
    ):
        accepted, deps = await self._send(
            deps_overrides={
                "assemble_session_attach_payload": AsyncMock(return_value=payload)
            }
        )
        assert accepted is False
        deps.release_session_attach_binding.assert_awaited_once()
        assert deps.release_session_attach_binding.await_args.kwargs[
            "pre_delivery"
        ] is (True)
        deps.schedule_attach_abort_successor.assert_called_once_with(
            THREAD_ID,
            retired_runtime_generation=RUNTIME_GENERATION,
            retired_attach_token=ATTACH_TOKEN,
            retired_agent_id=AGENT_ID,
        )

    @pytest.mark.asyncio
    async def test_an_unsafe_release_reports_ambiguous_and_schedules_nothing(self):
        accepted, deps = await self._send(
            deps_overrides={
                "assemble_session_attach_payload": AsyncMock(return_value=None),
                "release_session_attach_binding": AsyncMock(return_value="unsafe"),
            }
        )
        assert accepted is True
        deps.schedule_attach_abort_successor.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_raising_release_reports_ambiguous_ownership(self):
        accepted, deps = await self._send(
            deps_overrides={
                "assemble_session_attach_payload": AsyncMock(return_value=None),
                "release_session_attach_binding": AsyncMock(
                    side_effect=RuntimeError("down")
                ),
            }
        )
        assert accepted is True
        deps.schedule_attach_abort_successor.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_thread_rebound_under_us_aborts_before_delivery(
        self, attach_client
    ):
        accepted, deps = await self._send(
            rows=[_pinned_thread()] * 4 + [_pinned_thread(agent_id="other-agent")]
        )
        assert accepted is False
        assert attach_client.posts == []
        deps.schedule_attach_abort_successor.assert_called_once()

    @pytest.mark.asyncio
    async def test_an_unprovable_recipient_aborts_before_delivery(self, attach_client):
        accepted, deps = await self._send(
            deps_overrides={
                "prepare_pinned_session_mutation_target": AsyncMock(return_value=None)
            }
        )
        assert accepted is False
        assert attach_client.posts == []
        deps.schedule_attach_abort_successor.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_lost_recipient_after_the_post_retains_the_reservation(
        self, attach_client
    ):
        accepted, deps = await self._send(
            deps_overrides={
                "pinned_session_mutation_target_is_current": AsyncMock(
                    return_value=False
                )
            }
        )
        assert accepted is True
        assert len(attach_client.posts) == 1
        deps.release_session_attach_binding.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [409, 500, 503])
    async def test_a_non_200_response_is_ambiguous_not_a_failure(
        self, status, attach_client
    ):
        attach_client.status_code = status
        accepted, deps = await self._send()
        assert accepted is True
        deps.release_session_attach_binding.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_transport_failure_retains_the_reservation(self, attach_client):
        attach_client.error = RuntimeError("connection reset")
        accepted, deps = await self._send()
        assert accepted is True
        deps.release_session_attach_binding.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_project_ids_and_datasources_are_ignored_not_forwarded(
        self, attach_client
    ):
        accepted, deps = await self._send(
            project_ids=["project-a"], datasources=[{"id": "ds-a"}]
        )
        assert accepted is True
        assert deps.assemble_session_attach_payload.await_args.kwargs == {
            "config_override": None,
            "config_name": None,
            "runtime_agent_id": AGENT_ID,
        }
