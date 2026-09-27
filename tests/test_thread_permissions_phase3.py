"""Tests for Phase 3 of headless persistent sessions: DB-backed permission
gates. The real INSERT → UPDATE → polling round-trip needs a live Postgres
and lives in integration tests; here we cover the data-flow and contract
surface with mocks.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.persistent_graph import PermissionOutcome


# ---------------------------------------------------------------------------
# Helper: a fake postgres_conn that returns a context-managed asyncpg-like
# connection object with the methods _loop_permission_check uses.
# ---------------------------------------------------------------------------


def _make_postgres_conn(
    *,
    insert_returns: str = "11111111-1111-1111-1111-111111111111",
    select_status_sequence: list[str] = None,
    update_returns: dict | None = None,
    add_listener_callback=None,
):
    """Build a MagicMock postgres_conn whose .acquire() yields a connection
    with stubbed fetchval/fetchrow/execute/add_listener/remove_listener."""

    select_status_sequence = list(select_status_sequence or ["pending"])

    fake_conn = MagicMock()
    fake_conn.fetchval = AsyncMock()
    # Default to None (asyncpg's "no row" return) so callers without an
    # explicit update_returns don't accidentally trip Phase 5's
    # SELECT-first-by-tool_call_id guard in _loop_permission_check.
    fake_conn.fetchrow = AsyncMock(return_value=None)
    fake_conn.execute = AsyncMock()
    fake_conn.add_listener = AsyncMock()
    fake_conn.remove_listener = AsyncMock()

    # fetchval routes:
    #   - INSERT ... RETURNING id   -> insert_returns
    #   - SELECT status ... WHERE id = $1  -> next item from sequence
    async def _fetchval(sql, *args, **kwargs):
        if "INSERT" in sql:
            return insert_returns
        if "SELECT id FROM threads" in sql:
            return args[0]
        if "SELECT unit_id FROM run_queue" in sql:
            return args[0]
        if "SELECT status" in sql:
            return (
                select_status_sequence.pop(0) if select_status_sequence else "pending"
            )
        if "MAX(seq)" in sql or "COALESCE(MAX" in sql:
            return 0
        if "events_epoch" in sql:
            return 0
        return None

    fake_conn.fetchval.side_effect = _fetchval

    if update_returns is not None:
        fake_conn.fetchrow.return_value = update_returns

    if add_listener_callback is not None:
        fake_conn.add_listener.side_effect = add_listener_callback

    class _Acquire:
        async def __aenter__(self):
            return fake_conn

        async def __aexit__(self, exc_type, exc, tb):
            return None

    postgres_conn = MagicMock()
    postgres_conn.acquire = lambda: _Acquire()
    postgres_conn._fake_conn = fake_conn
    return postgres_conn


def _install_session(postgres_conn=None, permission_mode="supervised"):
    """Install a minimal _session into the persistent_app module globals.

    Note: call AFTER create_persistent_app() if you also need _thread_id
    set — create_persistent_app overwrites _thread_id from its arg.
    """
    import agent.api.persistent_app as mod

    session = MagicMock()
    session.permission_mode = permission_mode
    session.tool_decisions = {}
    session.postgres_conn = postgres_conn
    mod._session = session
    mod._thread_id = "thread-test"
    return session


# ---------------------------------------------------------------------------
# Section 1 — Early returns: autonomous / auto_accept / no session
# ---------------------------------------------------------------------------


class TestPermissionCheckEarlyReturns:
    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._thread_id = None

    def teardown_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._thread_id = None

    @pytest.mark.asyncio
    async def test_no_session_denies(self):
        import agent.api.persistent_app as mod

        result = await mod._loop_permission_check("read_file", {}, "tc1")
        # A gone session can never be answered — a real stop, not a pending
        # question, so DECLINED (not NO_ANSWER).
        assert result is PermissionOutcome.DECLINED

    @pytest.mark.asyncio
    async def test_autonomous_approves_without_db(self):
        import agent.api.persistent_app as mod

        _install_session(postgres_conn=None, permission_mode="autonomous")
        result = await mod._loop_permission_check("run_command", {}, "tc1")
        assert result is PermissionOutcome.APPROVED

    @pytest.mark.asyncio
    async def test_auto_accept_approves_non_shell(self):
        import agent.api.persistent_app as mod

        _install_session(postgres_conn=None, permission_mode="auto_accept")
        result = await mod._loop_permission_check("read_file", {}, "tc1")
        assert result is PermissionOutcome.APPROVED

    @pytest.mark.asyncio
    async def test_auto_accept_falls_through_for_shell(self):
        """Shell tools under auto_accept go through the DB path; with no
        postgres_conn the INSERT fails and we deny conservatively."""
        import agent.api.persistent_app as mod

        session = _install_session(postgres_conn=None, permission_mode="auto_accept")
        result = await mod._loop_permission_check("run_command", {}, "tc1")
        # No durable row can exist, so no later answer is possible: DECLINED.
        assert result is PermissionOutcome.DECLINED
        assert session.tool_decisions["tc1"] == "denied"


# ---------------------------------------------------------------------------
# Section 2 — _insert_permission_request behavior
# ---------------------------------------------------------------------------


class TestInsertPermissionRequest:
    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._thread_id = None

    def teardown_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._thread_id = None

    @pytest.mark.asyncio
    async def test_returns_uuid_string(self):
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn(insert_returns="aaaaaaaa-1111")
        _install_session(postgres_conn=postgres)
        rid = await mod._insert_permission_request("tc1", "read_file", {"path": "/x"})
        assert rid == "aaaaaaaa-1111"
        # INSERT was called via fetchval.
        sql = postgres._fake_conn.fetchval.await_args_list[0].args[0]
        assert "INSERT INTO thread_permission_requests" in sql

    @pytest.mark.asyncio
    async def test_returns_none_when_session_missing(self):
        import agent.api.persistent_app as mod

        # No _session set.
        rid = await mod._insert_permission_request("tc1", "x", {})
        assert rid is None

    @pytest.mark.asyncio
    async def test_returns_none_on_db_error(self):
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn()
        postgres._fake_conn.fetchval.side_effect = RuntimeError("db down")
        _install_session(postgres_conn=postgres)
        rid = await mod._insert_permission_request("tc1", "x", {})
        assert rid is None

    @pytest.mark.asyncio
    async def test_stateless_insert_stamps_exact_accepted_lease_token(
        self, monkeypatch
    ):
        import agent.api.persistent_app as mod
        from agent.api.lease_context import LeaseHandle

        postgres = _make_postgres_conn(insert_returns="aaaaaaaa-1111")
        _install_session(postgres_conn=postgres)
        mod._thread_id = "11111111-1111-4111-8111-111111111111"
        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        handle = LeaseHandle()
        handle.update(mod._thread_id, 17)
        context_token = mod._current_lease_var.set(handle)
        try:
            rid = await mod._insert_permission_request("tc1", "read_file", {})
        finally:
            mod._current_lease_var.reset(context_token)

        assert rid == "aaaaaaaa-1111"
        calls = postgres._fake_conn.fetchval.await_args_list
        assert "FOR UPDATE" in calls[0].args[0]
        assert "FOR SHARE" in calls[1].args[0]
        insert = calls[2]
        assert "accepted_lease_token" in insert.args[0]
        assert insert.args[-1] == 17


# ---------------------------------------------------------------------------
# Section 3 — _resolve_pending_permission UPDATEs the right row
# ---------------------------------------------------------------------------


class TestResolvePendingPermission:
    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._thread_id = None

    def teardown_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._thread_id = None

    @pytest.mark.asyncio
    async def test_resolves_by_explicit_id(self):
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn(
            update_returns={
                "id": "abc-123",
                "status": "approved",
                "tool_call_id": "tc1",
                "thread_id": "thread-test",
            }
        )
        _install_session(postgres_conn=postgres)
        result = await mod._resolve_pending_permission(
            "approved", approval_id="abc-123"
        )
        assert result is not None
        assert result["status"] == "approved"
        # UPDATE issued via fetchrow with the explicit id.
        sql = postgres._fake_conn.fetchrow.await_args.args[0]
        assert "UPDATE thread_permission_requests" in sql
        assert "WHERE id = $1" in sql

    @pytest.mark.asyncio
    async def test_resolves_most_recent_when_no_id(self):
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn(
            update_returns={
                "id": "abc-456",
                "status": "denied",
                "tool_call_id": "tc2",
                "thread_id": "thread-test",
            }
        )
        _install_session(postgres_conn=postgres)
        result = await mod._resolve_pending_permission("denied")
        assert result is not None
        assert result["id"] == "abc-456"
        sql = postgres._fake_conn.fetchrow.await_args.args[0]
        # The fallback selects the most-recent pending by thread.
        assert "ORDER BY requested_at DESC LIMIT 1" in sql

    @pytest.mark.asyncio
    async def test_rejects_invalid_decision(self):
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn()
        _install_session(postgres_conn=postgres)
        # Garbage decision → return None, no UPDATE.
        result = await mod._resolve_pending_permission("maybe?")
        assert result is None
        postgres._fake_conn.fetchrow.assert_not_called()


# ---------------------------------------------------------------------------
# Section 4 — _wait_for_permission_resolution: short-acquisition polling
# ---------------------------------------------------------------------------


class TestWaitForPermissionResolution:
    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._thread_id = None
        # The wait races the status read against this module global. A stale
        # Event left behind by an earlier test file is bound to that file's
        # (now closed) event loop, so awaiting it here raises and the wait
        # returns its conservative 'denied' default instead of the real
        # status. Own the precondition rather than inherit it.
        mod._session_input._hard_interrupt_event = None

    def teardown_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._thread_id = None
        mod._session_input._hard_interrupt_event = None

    @pytest.mark.asyncio
    async def test_returns_immediately_if_already_approved(self):
        """The first short acquisition observes a decision without waiting."""
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn(
            select_status_sequence=["approved"],
        )
        _install_session(postgres_conn=postgres)
        result = await mod._wait_for_permission_resolution("rid-1", timeout=1.0)
        assert result == "approved"
        postgres._fake_conn.add_listener.assert_not_awaited()
        postgres._fake_conn.remove_listener.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_denied_when_already_denied(self):
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn(
            select_status_sequence=["denied"],
        )
        _install_session(postgres_conn=postgres)
        result = await mod._wait_for_permission_resolution("rid-2", timeout=1.0)
        assert result == "denied"

    @pytest.mark.asyncio
    async def test_timeout_marks_expired_and_returns_expired(self):
        """When the listener never fires and the wait times out, the
        function UPDATEs the row to expired (CAS-style) and re-reads."""
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn(
            # First SELECT: still pending → enter wait.
            # Second SELECT (post-timeout, post-UPDATE): expired.
            select_status_sequence=["pending", "expired"],
        )
        _install_session(postgres_conn=postgres)
        result = await mod._wait_for_permission_resolution("rid-3", timeout=0.1)
        assert result == "expired"
        # The expire-UPDATE was issued.
        update_sql = postgres._fake_conn.execute.await_args.args[0]
        assert "UPDATE thread_permission_requests" in update_sql
        assert "SET status = 'expired'" in update_sql

    @pytest.mark.asyncio
    async def test_db_error_is_a_non_decision_not_a_denial(self):
        """A read that blows up says nothing about what the user wanted, so it
        must not come back as a refusal — the model would be told the user
        declined a call they were never asked about."""
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn()
        postgres._fake_conn.fetchval.side_effect = RuntimeError("status read failed")
        _install_session(postgres_conn=postgres)
        result = await mod._wait_for_permission_resolution("rid-4", timeout=0.1)
        assert result == "unavailable"
        assert result != "denied"
        # The row is left pending: no decision was made, so none is recorded.
        assert postgres._fake_conn.execute.await_args_list == []

    @pytest.mark.asyncio
    async def test_dead_session_is_a_non_decision_not_a_denial(self):
        import agent.api.persistent_app as mod

        mod._session = None
        result = await mod._wait_for_permission_resolution("rid-6", timeout=0.1)
        assert result == "unavailable"

    @pytest.mark.asyncio
    async def test_retired_row_reads_as_expired_not_denied(self):
        """The untethered CAS found no row to re-read — the question is gone
        unanswered, which is an expiry, not a refusal."""
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn(select_status_sequence=["pending", None])
        _install_session(postgres_conn=postgres)
        mod._subscribers.clear()
        result = await mod._wait_for_permission_resolution("rid-7", timeout=0.1)
        assert result == "expired"

    @pytest.mark.asyncio
    async def test_short_poll_observes_resolution(self, monkeypatch):
        """A decision committed during the slice is observed on a fresh poll."""
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn(
            # First short acquisition: pending. Next acquisition: approved.
            select_status_sequence=["pending", "approved"],
        )
        _install_session(postgres_conn=postgres)
        monkeypatch.setattr(mod, "_PERMISSION_POLL_SECONDS", 0.01)

        result = await mod._wait_for_permission_resolution("rid-5", timeout=2.0)
        assert result == "approved"
        postgres._fake_conn.add_listener.assert_not_awaited()


# ---------------------------------------------------------------------------
# Section 5 — _loop_permission_check end-to-end (mocked)
# ---------------------------------------------------------------------------


class TestLoopPermissionCheckDBPath:
    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._thread_id = None
        mod._subscribers.clear()
        mod._events_epoch = 0
        mod._next_seq = 0
        # Reaches _wait_for_permission_resolution — same cross-loop hazard.
        mod._session_input._hard_interrupt_event = None

    def teardown_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._thread_id = None
        mod._subscribers.clear()
        mod._session_input._hard_interrupt_event = None

    @pytest.mark.asyncio
    async def test_denies_when_insert_fails(self):
        import agent.api.persistent_app as mod

        session = _install_session(postgres_conn=None, permission_mode="supervised")
        result = await mod._loop_permission_check("run_command", {}, "tc1")
        assert result is PermissionOutcome.DECLINED
        assert session.tool_decisions["tc1"] == "denied"

    @pytest.mark.asyncio
    async def test_approved_path_returns_true_and_records_decision(self):
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn(
            insert_returns="rid-approve",
            select_status_sequence=["approved"],  # SELECT-first picks it up
        )
        session = _install_session(postgres_conn=postgres, permission_mode="supervised")
        result = await mod._loop_permission_check("run_command", {"cmd": "ls"}, "tc1")
        assert result is PermissionOutcome.APPROVED
        assert session.tool_decisions["tc1"] == "approved"

    @pytest.mark.asyncio
    async def test_denied_path_returns_false_and_records_decision(self):
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn(
            insert_returns="rid-deny",
            select_status_sequence=["denied"],
        )
        session = _install_session(postgres_conn=postgres, permission_mode="supervised")
        result = await mod._loop_permission_check(
            "run_command", {"cmd": "rm -rf /"}, "tc1"
        )
        # An explicit user "no" — DECLINED, distinct from an unanswered gate.
        assert result is PermissionOutcome.DECLINED
        assert session.tool_decisions["tc1"] == "denied"

    @pytest.mark.asyncio
    async def test_broadcast_carries_approval_id(self):
        """The permission.request frame must include approval_id so
        clients can refer back via either tool_call_id or approval_id."""
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn(
            insert_returns="rid-broadcast",
            select_status_sequence=["approved"],
        )
        _install_session(postgres_conn=postgres, permission_mode="supervised")
        q = mod._subscribe("client-X")
        await mod._loop_permission_check("run_command", {"cmd": "ls"}, "tc1")
        # First broadcast on the queue should be permission.request.
        frame = q.get_nowait()
        assert frame["method"] == "permission.request"
        assert frame["params"]["approval_id"] == "rid-broadcast"
        assert frame["params"]["id"] == "tc1"
        assert frame["params"]["tool"] == "run_command"


# ---------------------------------------------------------------------------
# Section 6 — Agent REST /api/approve hits the DB path
# ---------------------------------------------------------------------------


class TestApiApproveRestEndpoint:
    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._session = None

    def teardown_method(self):
        import agent.api.persistent_app as mod

        mod._session = None

    def test_resolves_via_db_helper_with_explicit_id(self):
        from fastapi.testclient import TestClient

        import agent.api.persistent_app as mod
        from agent.api.persistent_app import create_persistent_app

        postgres = _make_postgres_conn(
            update_returns={
                "id": "rid-X",
                "status": "approved",
                "tool_call_id": "tc-abc",
                "thread_id": "thread-test",
            }
        )
        # create_persistent_app overwrites _thread_id — install AFTER.
        app = create_persistent_app("interactive")
        _install_session(postgres_conn=postgres)
        try:
            client = TestClient(app)
            resp = client.post(
                "/api/approve",
                json={"decision": "approve", "approval_id": "rid-X"},
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["accepted"] is True
            assert body["approval_id"] == "rid-X"
            assert body["tool_call_id"] == "tc-abc"
        finally:
            mod._session = None

    def test_returns_404_when_no_pending(self):
        from fastapi.testclient import TestClient

        import agent.api.persistent_app as mod
        from agent.api.persistent_app import create_persistent_app

        postgres = _make_postgres_conn(update_returns=None)
        app = create_persistent_app("interactive")
        _install_session(postgres_conn=postgres)
        try:
            client = TestClient(app)
            resp = client.post(
                "/api/approve",
                json={"decision": "approve"},  # no id, no pending
            )
            assert resp.status_code == 404
        finally:
            mod._session = None

    def test_rejects_invalid_decision(self):
        from fastapi.testclient import TestClient

        import agent.api.persistent_app as mod
        from agent.api.persistent_app import create_persistent_app

        postgres = _make_postgres_conn()
        app = create_persistent_app("interactive")
        _install_session(postgres_conn=postgres)
        try:
            client = TestClient(app)
            resp = client.post(
                "/api/approve",
                json={"decision": "maybe"},
            )
            assert resp.status_code == 400
        finally:
            mod._session = None


# ---------------------------------------------------------------------------
# Section 7 — Imports of removed APPROVE/DENY sentinels stay clean
# ---------------------------------------------------------------------------


def test_persistent_app_no_longer_imports_sentinels():
    """Phase 3 removed the APPROVE_SENTINEL/DENY_SENTINEL imports because
    the queue-based approval path was replaced with the DB-backed one.
    They still live in persistent_graph for dual_app's worker-mode flow."""
    import agent.api.persistent_app as mod

    assert not hasattr(mod, "APPROVE_SENTINEL")
    assert not hasattr(mod, "DENY_SENTINEL")
