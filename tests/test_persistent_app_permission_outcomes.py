"""Transport-side supervised gate semantics in src/api/persistent_app.py.

Companion to tests/test_persistent_graph_permission_outcomes.py, covering
knowledge-history/done/supervised_parallel_gates_timeout_fabricates_denial.md:

* Fix 1 — ``_loop_permission_check`` must report a *three-state* outcome.
  An expired/unanswered gate is NO_ANSWER, never DECLINED: collapsing the
  two is what told the model "User denied this tool call." for calls the
  user never saw.
* Fix 2 — a still-pending gate must be re-surfaced to a (re)attaching
  client, so a reload recovers the approval card instead of leaving it
  stranded (the live repro froze on a stale card while the stream was
  down).
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import agent.api.persistent_app as pa
from agent.persistent_graph import PermissionOutcome
from shared.run_queue import heartbeat_unit
from shared.thread_presence import PermissionExpiryResult


# =============================================================================
# Helpers
# =============================================================================


def _mock_session(permission_mode: str = "supervised"):
    session = MagicMock()
    session.permission_mode = permission_mode
    session.tool_decisions = {}
    session.postgres_conn = None  # skip the wake-path SELECT by default
    return session


# =============================================================================
# Fix 1 — three-state outcome from the transport
# =============================================================================


class TestPermissionCheckOutcomeMapping:
    @pytest.mark.asyncio
    async def test_expired_gate_reports_no_answer_not_denied(self):
        """The core bug: a gate that timed out must NOT look like a refusal."""
        session = _mock_session()
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(
                pa, "_insert_permission_request", AsyncMock(return_value="req-1")
            ),
            patch.object(
                pa,
                "_wait_for_permission_resolution",
                AsyncMock(return_value="expired"),
            ),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            outcome = await pa._loop_permission_check("web_search", {}, "tc_1")

        assert outcome is PermissionOutcome.NO_ANSWER
        # Audit must keep the distinction too.
        assert session.tool_decisions["tc_1"] == "expired"

    @pytest.mark.asyncio
    async def test_wait_failure_reports_no_answer_not_denied(self):
        """Same bug, other trigger: when the wait itself fails (unreachable DB,
        unexpected error) the user still said nothing. Reporting a refusal
        would be doubly wrong — the row is left pending, so the transcript's
        'declined' is contradicted by the DB it came from."""
        session = _mock_session()
        broadcast = MagicMock()
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(
                pa, "_insert_permission_request", AsyncMock(return_value="req-9")
            ),
            patch.object(
                pa,
                "_wait_for_permission_resolution",
                AsyncMock(return_value="unavailable"),
            ),
            patch.object(pa, "_broadcast", broadcast),
        ):
            outcome = await pa._loop_permission_check("web_search", {}, "tc_9")

        assert outcome is PermissionOutcome.NO_ANSWER
        # Audit distinguishes an infrastructure failure from a user's refusal
        # and from an unanswered TTL.
        assert session.tool_decisions["tc_9"] == "unavailable"
        resolved = [
            c for c in broadcast.call_args_list if c.args[0] == "permission.resolved"
        ]
        assert resolved and resolved[-1].args[1]["decision"] == "unavailable"

    @pytest.mark.asyncio
    async def test_explicit_deny_reports_declined(self):
        session = _mock_session()
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(
                pa, "_insert_permission_request", AsyncMock(return_value="req-2")
            ),
            patch.object(
                pa, "_wait_for_permission_resolution", AsyncMock(return_value="denied")
            ),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            outcome = await pa._loop_permission_check("web_search", {}, "tc_2")

        assert outcome is PermissionOutcome.DECLINED
        assert session.tool_decisions["tc_2"] == "denied"

    @pytest.mark.asyncio
    async def test_approved_reports_approved(self):
        session = _mock_session()
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(
                pa, "_insert_permission_request", AsyncMock(return_value="req-3")
            ),
            patch.object(
                pa,
                "_wait_for_permission_resolution",
                AsyncMock(return_value="approved"),
            ),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            outcome = await pa._loop_permission_check("web_search", {}, "tc_3")

        assert outcome is PermissionOutcome.APPROVED

    @pytest.mark.asyncio
    async def test_stateless_sudo_gate_uses_durable_presence_oracle(self):
        session = _mock_session()
        durable_pause = AsyncMock(return_value=True)
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_orchestrator_client", MagicMock()),
            patch.object(pa, "_stateless_mode", return_value=True),
            patch.object(pa, "_officer_cfg", return_value=None),
            patch.object(pa, "_safe_mark_stateless_natural_pause", durable_pause),
            patch.object(
                pa, "_insert_permission_request", AsyncMock(return_value="req-sudo")
            ),
            patch.object(
                pa,
                "_wait_for_permission_resolution",
                AsyncMock(return_value="approved"),
            ),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            outcome = await pa._loop_permission_check("run_command", {}, "tc-sudo")

        assert outcome is PermissionOutcome.APPROVED
        durable_pause.assert_awaited_once_with(require_untethered=True)

    @pytest.mark.asyncio
    async def test_autonomous_mode_approves(self):
        with patch.object(pa, "_session", _mock_session("autonomous")):
            outcome = await pa._loop_permission_check("run_command", {}, "tc_4")
        assert outcome is PermissionOutcome.APPROVED

    @pytest.mark.asyncio
    async def test_dead_session_declines_rather_than_parking(self):
        """A gone session can't ever be answered — that's a real stop, not a
        pending question, so it must not park the turn forever."""
        with patch.object(pa, "_session", None):
            outcome = await pa._loop_permission_check("web_search", {}, "tc_5")
        assert outcome is PermissionOutcome.DECLINED

    @pytest.mark.asyncio
    async def test_db_unavailable_declines(self):
        """No durable row means no gate can ever resolve — conservative deny,
        never silent auto-approval."""
        session = _mock_session()
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(
                pa, "_insert_permission_request", AsyncMock(return_value=None)
            ),
        ):
            outcome = await pa._loop_permission_check("web_search", {}, "tc_6")

        assert outcome is PermissionOutcome.DECLINED


# =============================================================================
# Fix 1b — the TTL must not cut off a user who is still there
# =============================================================================


def _conn_returning(statuses):
    """A mock connection whose fetchval yields ``statuses`` in order."""
    conn = AsyncMock()
    conn.fetchval = AsyncMock(side_effect=list(statuses))
    conn.execute = AsyncMock()
    conn.add_listener = AsyncMock()
    conn.remove_listener = AsyncMock()
    return conn


def _session_with_conn(conn):
    acquire_ctx = MagicMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
    acquire_ctx.__aexit__ = AsyncMock(return_value=False)
    session = _mock_session()
    session.postgres_conn = MagicMock()
    session.postgres_conn.acquire = MagicMock(return_value=acquire_ctx)
    return session


class TestTetheredWaitDoesNotExpire:
    @pytest.mark.asyncio
    async def test_tethered_gate_is_not_expired_on_ttl(self):
        """A user watching the session may simply be slow. Expiring the gate
        under them means their later click 404s and the tool never runs."""
        # pending at first check, still pending after the slice, then the user
        # finally approves.
        conn = _conn_returning(["pending", "pending", "approved", "approved"])
        session = _session_with_conn(conn)

        with (
            patch.object(pa, "_session", session),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),  # tethered
            patch.object(pa._session_input, "_hard_interrupt_event", None),
        ):
            status = await pa._wait_for_permission_resolution("req-1", timeout=0.01)

        assert status == "approved"
        # Crucially: no CAS-expire UPDATE was issued while the user was there.
        expire_calls = [
            c for c in conn.execute.await_args_list if "expired" in str(c).lower()
        ]
        assert expire_calls == [], f"gate expired under a watching user: {expire_calls}"

    @pytest.mark.asyncio
    async def test_untethered_gate_still_expires(self):
        """Nobody attached: fall back to the bounded wait so the loop doesn't
        hang forever waiting for a client that isn't there."""
        conn = _conn_returning(["pending", "expired"])
        session = _session_with_conn(conn)

        with (
            patch.object(pa, "_session", session),
            patch.object(pa, "_subscribers", {}),  # untethered
            patch.object(pa._session_input, "_hard_interrupt_event", None),
        ):
            status = await pa._wait_for_permission_resolution("req-2", timeout=0.01)

        assert status == "expired"
        expire_calls = [
            c for c in conn.execute.await_args_list if "expired" in str(c).lower()
        ]
        assert expire_calls, "untethered gate should CAS-expire"

    @pytest.mark.asyncio
    async def test_hard_interrupt_breaks_the_wait(self):
        """Stop must not be held hostage by an open gate."""
        import asyncio as _asyncio

        conn = _conn_returning(["pending", "pending"])
        session = _session_with_conn(conn)
        interrupt = _asyncio.Event()
        interrupt.set()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),  # tethered
            patch.object(pa._session_input, "_hard_interrupt_event", interrupt),
        ):
            status = await _asyncio.wait_for(
                pa._wait_for_permission_resolution("req-3", timeout=30),
                timeout=5,
            )

        # Interrupted, not answered — must not read as approval.
        assert status != "approved"


class TestStatelessDurablePresenceWait:
    @pytest.mark.asyncio
    async def test_absent_client_expires_under_exact_owner(self):
        conn = _conn_returning(["pending"])
        session = _session_with_conn(conn)
        expiry = AsyncMock(
            return_value=PermissionExpiryResult(
                status="expired", owner_live=True, live_for_seconds=None
            )
        )
        with (
            patch.object(pa, "_session", session),
            patch.object(
                pa._session_identity,
                "_thread_id",
                "00000000-0000-0000-0000-000000000001",
            ),
            patch.object(pa, "_stateless_mode", return_value=True),
            patch.object(pa._session_identity, "stateless_lease_token", return_value=9),
            patch.object(pa, "expire_permission_if_untethered", expiry),
            patch.object(pa._session_input, "_hard_interrupt_event", None),
        ):
            status = await pa._wait_for_permission_resolution("req-1", timeout=0.01)

        assert status == "expired"
        expiry.assert_awaited_once_with(
            conn,
            thread_id="00000000-0000-0000-0000-000000000001",
            request_id="req-1",
            lease_token=9,
        )

    @pytest.mark.asyncio
    async def test_fast_status_polls_do_not_move_presence_expiry_boundary(self):
        conn = _conn_returning(["pending"] * 20)
        session = _session_with_conn(conn)
        expiry = AsyncMock(
            return_value=PermissionExpiryResult(
                status="expired", owner_live=True, live_for_seconds=None
            )
        )
        with (
            patch.object(pa, "_session", session),
            patch.object(
                pa._session_identity,
                "_thread_id",
                "00000000-0000-0000-0000-000000000001",
            ),
            patch.object(pa, "_stateless_mode", return_value=True),
            patch.object(pa._session_identity, "stateless_lease_token", return_value=9),
            patch.object(pa, "expire_permission_if_untethered", expiry),
            patch.object(pa._session_input, "_hard_interrupt_event", None),
            patch.object(pa, "_PERMISSION_POLL_SECONDS", 0.005),
        ):
            waiter = asyncio.create_task(
                pa._wait_for_permission_resolution("req-boundary", timeout=0.05)
            )
            await asyncio.sleep(0.025)

            assert conn.fetchval.await_count >= 3
            expiry.assert_not_awaited()
            assert waiter.done() is False
            assert await waiter == "expired"

        expiry.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_live_client_rechecks_at_presence_expiry(self):
        # One status read opens each bounded presence slice; the durable
        # oracle itself supplies the terminal result at the second boundary.
        conn = _conn_returning(["pending", "pending"])
        session = _session_with_conn(conn)
        expiry = AsyncMock(
            side_effect=[
                PermissionExpiryResult(
                    status="pending", owner_live=True, live_for_seconds=0.01
                ),
                PermissionExpiryResult(
                    status="approved", owner_live=True, live_for_seconds=None
                ),
            ]
        )
        with (
            patch.object(pa, "_session", session),
            patch.object(
                pa._session_identity,
                "_thread_id",
                "00000000-0000-0000-0000-000000000001",
            ),
            patch.object(pa, "_stateless_mode", return_value=True),
            patch.object(pa._session_identity, "stateless_lease_token", return_value=9),
            patch.object(pa, "expire_permission_if_untethered", expiry),
            patch.object(pa._session_input, "_hard_interrupt_event", None),
        ):
            status = await pa._wait_for_permission_resolution("req-2", timeout=0.01)

        assert status == "approved"
        assert expiry.await_count == 2

    @pytest.mark.asyncio
    async def test_lost_owner_leaves_permission_pending(self):
        conn = _conn_returning(["pending"])
        session = _session_with_conn(conn)
        with (
            patch.object(pa, "_session", session),
            patch.object(
                pa._session_identity,
                "_thread_id",
                "00000000-0000-0000-0000-000000000001",
            ),
            patch.object(pa, "_stateless_mode", return_value=True),
            patch.object(
                pa._session_identity, "stateless_lease_token", return_value=None
            ),
            patch.object(pa._session_input, "_hard_interrupt_event", None),
        ):
            status = await pa._wait_for_permission_resolution("req-3", timeout=0.01)

        assert status == "interrupted"
        assert not any(
            "expired" in str(call).lower() for call in conn.execute.await_args_list
        )

    @pytest.mark.asyncio
    async def test_interrupt_does_not_cancel_ambiguous_expiry_mutation(self):
        """Once the exact-owner CAS starts, its durable outcome is authoritative."""

        conn = _conn_returning(["pending"])
        session = _session_with_conn(conn)
        hard_interrupt = asyncio.Event()
        expiry_started = asyncio.Event()
        allow_expiry = asyncio.Event()

        async def _expire(*_args, **_kwargs):
            expiry_started.set()
            await allow_expiry.wait()
            return PermissionExpiryResult(
                status="expired", owner_live=True, live_for_seconds=None
            )

        with (
            patch.object(pa, "_session", session),
            patch.object(
                pa._session_identity,
                "_thread_id",
                "00000000-0000-0000-0000-000000000001",
            ),
            patch.object(pa, "_stateless_mode", return_value=True),
            patch.object(pa._session_identity, "stateless_lease_token", return_value=9),
            patch.object(pa, "expire_permission_if_untethered", _expire),
            patch.object(pa._session_input, "_hard_interrupt_event", hard_interrupt),
        ):
            waiter = asyncio.create_task(
                pa._wait_for_permission_resolution("req-cas", timeout=0.01)
            )
            await asyncio.wait_for(expiry_started.wait(), timeout=0.5)
            hard_interrupt.set()
            await asyncio.sleep(0)

            assert waiter.done() is False
            allow_expiry.set()
            assert await waiter == "expired"


class _ThreeConnectionPool:
    """Small asyncpg-like pool used to reproduce the stateless listener budget."""

    class _Connection:
        def __init__(self, pool):
            self._pool = pool

        async def add_listener(self, _channel, _callback):
            self._pool.listener_count += 1
            if self._pool.listener_count == 2:
                self._pool.listeners_ready.set()

        async def remove_listener(self, _channel, _callback):
            self._pool.listener_count -= 1

        async def fetchval(self, sql, *_args):
            if "thread_permission_requests" in sql:
                self._pool.permission_reads += 1
                self._pool.permission_read.set()
                return "pending"
            self._pool.heartbeat_reads += 1
            return datetime.now(timezone.utc)

        async def execute(self, _sql, *_args):
            return "UPDATE 0"

    class _Acquire:
        def __init__(self, pool):
            self._pool = pool

        async def __aenter__(self):
            await self._pool._slots.acquire()
            self._pool.in_use += 1
            self._pool.peak_in_use = max(self._pool.peak_in_use, self._pool.in_use)
            return _ThreeConnectionPool._Connection(self._pool)

        async def __aexit__(self, _exc_type, _exc, _tb):
            self._pool.in_use -= 1
            self._pool._slots.release()

    def __init__(self):
        self._slots = asyncio.Semaphore(3)
        self.in_use = 0
        self.peak_in_use = 0
        self.listener_count = 0
        self.permission_reads = 0
        self.heartbeat_reads = 0
        self.listeners_ready = asyncio.Event()
        self.permission_read = asyncio.Event()

    def acquire(self):
        return self._Acquire(self)

    async def fetchval(self, sql, *args):
        async with self.acquire() as conn:
            return await conn.fetchval(sql, *args)


class TestStatelessPermissionPoolBudget:
    @pytest.mark.asyncio
    async def test_pending_gate_leaves_max_three_pool_slot_for_lease_heartbeat(self):
        """Control + interrupt listeners must not make permission starve renewal.

        This is the production max=3 shape: two long-lived watcher LISTEN
        connections, one pending supervised gate, and the independent exact
        lease heartbeat. The gate may use the third connection briefly, but it
        must return it while waiting for the human decision.
        """

        pool = _ThreeConnectionPool()
        thread_id = "00000000-0000-0000-0000-000000000001"
        control_stop = asyncio.Event()
        interrupt_stop = asyncio.Event()
        control_task = None
        interrupt_task = None
        permission_task = None

        with (
            patch.object(pa, "_session", SimpleNamespace(postgres_conn=pool)),
            patch.object(pa._session_identity, "_thread_id", thread_id),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa._session_input, "_hard_interrupt_event", None),
            patch.object(pa, "_stateless_mode", return_value=True),
            patch.object(pa._session_identity, "stateless_lease_token", return_value=7),
            patch.object(pa, "_PERMISSION_POLL_SECONDS", 0.05),
            patch.object(pa, "_drain_thread_controls", AsyncMock(return_value=0)),
            patch.object(pa, "_drain_thread_interrupts", AsyncMock(return_value=0)),
        ):
            try:
                control_task = asyncio.create_task(
                    pa._control_watcher_loop(
                        postgres_conn=pool,
                        thread_id=thread_id,
                        stop=control_stop,
                        lease_token=7,
                        agent_id=None,
                    )
                )
                interrupt_task = asyncio.create_task(
                    pa._interrupt_watcher_loop(
                        postgres_conn=pool,
                        thread_id=thread_id,
                        stop=interrupt_stop,
                        lease_token=7,
                        target_turn_id=1,
                    )
                )
                await asyncio.wait_for(pool.listeners_ready.wait(), timeout=0.5)

                permission_task = asyncio.create_task(
                    pa._wait_for_permission_resolution("permission-1", timeout=30)
                )
                await asyncio.wait_for(pool.permission_read.wait(), timeout=0.5)

                renewed = await asyncio.wait_for(
                    heartbeat_unit(pool, unit_id=thread_id, lease_token=7),
                    timeout=0.5,
                )

                assert renewed is not None
                assert pool.heartbeat_reads == 1
                assert pool.listener_count == 2
                assert pool.peak_in_use == 3
                assert permission_task.done() is False
            finally:
                if permission_task is not None:
                    permission_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await permission_task
                control_stop.set()
                interrupt_stop.set()
                if control_task is not None and interrupt_task is not None:
                    await asyncio.gather(control_task, interrupt_task)

    @pytest.mark.asyncio
    async def test_hard_interrupt_cancels_permission_acquire_on_saturated_pool(self):
        """Stop remains prompt even if a transient third borrower fills the pool."""

        pool = _ThreeConnectionPool()
        thread_id = "00000000-0000-0000-0000-000000000001"
        hard_interrupt = asyncio.Event()
        control_stop = asyncio.Event()
        interrupt_stop = asyncio.Event()
        control_task = None
        interrupt_task = None
        permission_task = None

        with (
            patch.object(pa, "_session", SimpleNamespace(postgres_conn=pool)),
            patch.object(pa._session_identity, "_thread_id", thread_id),
            patch.object(pa._session_input, "_hard_interrupt_event", hard_interrupt),
            patch.object(pa, "_drain_thread_controls", AsyncMock(return_value=0)),
            patch.object(pa, "_drain_thread_interrupts", AsyncMock(return_value=0)),
        ):
            try:
                control_task = asyncio.create_task(
                    pa._control_watcher_loop(
                        postgres_conn=pool,
                        thread_id=thread_id,
                        stop=control_stop,
                        lease_token=7,
                        agent_id=None,
                    )
                )
                interrupt_task = asyncio.create_task(
                    pa._interrupt_watcher_loop(
                        postgres_conn=pool,
                        thread_id=thread_id,
                        stop=interrupt_stop,
                        lease_token=7,
                        target_turn_id=1,
                    )
                )
                await asyncio.wait_for(pool.listeners_ready.wait(), timeout=0.5)

                async with pool.acquire():
                    permission_task = asyncio.create_task(
                        pa._wait_for_permission_resolution(
                            "permission-blocked", timeout=30
                        )
                    )
                    await asyncio.sleep(0)
                    hard_interrupt.set()
                    assert (
                        await asyncio.wait_for(permission_task, timeout=0.5)
                        == "interrupted"
                    )

                assert pool.permission_reads == 0
            finally:
                if permission_task is not None and not permission_task.done():
                    permission_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await permission_task
                control_stop.set()
                interrupt_stop.set()
                if control_task is not None and interrupt_task is not None:
                    await asyncio.gather(control_task, interrupt_task)


# =============================================================================
# Fix 2 — pending gates re-surface on (re)attach
# =============================================================================


class TestPendingGatesResurfaceOnAttach:
    @pytest.mark.asyncio
    async def test_pending_requests_are_fetched_for_welcome_frame(self):
        """A reconnecting client must be able to learn about the gate that is
        still waiting, so a reload re-renders the approval card."""
        row = {
            "id": "req-9",
            "tool_call_id": "tc_9",
            "tool_name": "web_search",
            "tool_args": '{"query": "capital of Japan"}',
        }

        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[row])
        acquire_ctx = MagicMock()
        acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
        acquire_ctx.__aexit__ = AsyncMock(return_value=False)

        session = _mock_session()
        session.postgres_conn = MagicMock()
        session.postgres_conn.acquire = MagicMock(return_value=acquire_ctx)

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            pending = await pa._pending_permission_requests()

        assert len(pending) == 1
        assert pending[0]["id"] == "tc_9"
        assert pending[0]["approval_id"] == "req-9"
        assert pending[0]["tool"] == "web_search"
        # args are decoded for the client, not handed over as a raw JSON blob
        assert pending[0]["args"] == {"query": "capital of Japan"}

    @pytest.mark.asyncio
    async def test_no_pending_requests_returns_empty(self):
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])
        acquire_ctx = MagicMock()
        acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
        acquire_ctx.__aexit__ = AsyncMock(return_value=False)

        session = _mock_session()
        session.postgres_conn = MagicMock()
        session.postgres_conn.acquire = MagicMock(return_value=acquire_ctx)

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            pending = await pa._pending_permission_requests()

        assert pending == []

    @pytest.mark.asyncio
    async def test_db_failure_is_soft(self):
        """A welcome frame must still be sent if the lookup fails."""
        session = _mock_session()
        session.postgres_conn = MagicMock()
        session.postgres_conn.acquire = MagicMock(side_effect=RuntimeError("boom"))

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            pending = await pa._pending_permission_requests()

        assert pending == []
